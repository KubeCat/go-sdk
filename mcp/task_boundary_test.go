// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
)

// The tasks extension lives in the task files of this package: tasks.go and
// task*.go. The other, shared files are the code every program runs, and they
// reach the extension only at a few fixed points, each keyed on state that
// only an installed TaskServer or an opted-in client call creates.
//
// TestTaskBoundary pins those points. It lists every place where a shared
// file mentions the extension, in two ways, and compares each list with the
// one below:
//
//   - tokens: every identifier and string literal in a shared file that
//     contains "task", in any case;
//   - references: every identifier in a shared file that refers to something
//     declared in a task file, whatever its name.
//
// An entry is "file: enclosing top-level declaration: name", followed by
// "(xN)" when the name occurs N times in the declaration. A change that
// adds or removes one fails the test. Before adding an entry, check that the
// new code does nothing unless a TaskServer is installed on that server or
// that client call opted in, and that it is not keyed on what a peer sent.
//
// The other direction is pinned too. A task file can change what every
// program runs without a shared file naming anything of the extension: by
// assigning or filling a package-level variable of a shared file (a hook, a
// method table), by writing to a field of a shared type or to a map shared
// code handed out, by declaring a method on a shared type (a JSON codec), in
// an init function, or in the initializer of a package-level variable. So
// the test also lists, for the task files:
//
//   - variables: every use of a package-level variable declared in a shared
//     file;
//   - fields: every use of an unexported field of a type of a shared file,
//     by the declaration that uses it;
//   - map writes: every write to a map that is in such a field or variable,
//     or that a function returned;
//   - methods: every method declared on a type of a shared file;
//   - package-level variables: every one a task file declares;
//   - init functions, and assignments to a package-level variable declared
//     outside the task files (in this or another package), of which there
//     are none.
//
// Every non-test Go file of the directory is looked at; one that a build
// constraint leaves out of this build is a failure.
//
// The test pins where the extension is reached from, and where it reaches
// into shared state, not what the code there does. It does not see a change
// of behaviour inside a listed place, a shared file that handles a task
// method under a name it builds at run time, a shared map written through a
// local variable, or a method of a task type called through an interface a
// shared file declares. Those change what a program that does not use the
// extension sees, and the compat_nonuser tests and the task tests named
// "...Unchanged" are what catch them.
var (
	taskBoundaryTokens = []string{
		"client.go: ClientSession.CallTool: callUsesTasks",
		"client.go: ClientSession.CallTool: useTasks",
		"client.go: ClientSession.receivingMethodInfos: taskReceive",
		"client.go: ClientSession: newTaskTimer",
		"client.go: ClientSession: taskFollower",
		"client.go: ClientSession: taskFollowers",
		"client.go: ClientSession: taskFollowersMu",
		"client.go: ClientSession: taskReceive",
		"client.go: ClientSession: taskTimer",
		"protocol.go: CallToolParams: UseTasks",
		"protocol.go: CallToolResult.MarshalJSON: marshalCreateTaskResult",
		"protocol.go: CallToolResult.MarshalJSON: taskCreated",
		"protocol.go: CallToolResult: Task",
		"protocol.go: CallToolResult: task",
		"protocol.go: CallToolResult: taskCreated",
		"protocol.go: NotificationSubscriptions: TaskIDs",
		`server.go: AddReceivingCustomMethod: "mcp: AddReceivingCustomMethod: %q is reserved by the installed tasks extension"`,
		"server.go: AddReceivingCustomMethod: tasks",
		"server.go: Server.discover: tasks",
		"server.go: Server.subscriptionsListen: TaskIDs (x2)",
		"server.go: Server.subscriptionsListen: taskSub (x8)",
		"server.go: Server.subscriptionsListen: taskSubscription",
		"server.go: Server.subscriptionsListen: tasks",
		"server.go: Server: TaskServer",
		"server.go: Server: tasks",
		"shared.go: defaultSendingMethodHandler: taskSendFromContext",
		"streamable.go: streamableServerConn.servePOST: tasks",
		"streamable_headers.go: setStandardHeaders: taskNameFromContext",
	}
	taskBoundaryReferences = []string{
		"client.go: ClientSession.CallTool: ClientSession.callUsesTasks",
		"client.go: ClientSession.CallTool: ClientSession.useTasks",
		"client.go: ClientSession: taskFollower",
		"client.go: ClientSession: taskTimer",
		"protocol.go: CallToolResult.MarshalJSON: marshalCreateTaskResult",
		"protocol.go: CallToolResult: Task",
		"server.go: AddReceivingCustomMethod: TaskServer.ownsMethod",
		"server.go: Server.discover: TaskServer.advertise",
		"server.go: Server.subscriptionsListen: TaskServer.subscribe",
		"server.go: Server.subscriptionsListen: ids",
		"server.go: Server.subscriptionsListen: taskSubscription",
		"server.go: Server.subscriptionsListen: taskSubscription.close",
		"server.go: Server.subscriptionsListen: taskSubscription.context",
		"server.go: Server.subscriptionsListen: taskSubscription.deliver",
		"server.go: Server: TaskServer",
		"shared.go: defaultSendingMethodHandler: taskSendFromContext",
		"streamable.go: streamableServerConn.servePOST: TaskServer.validateHeaders",
		"streamable_headers.go: setStandardHeaders: taskNameFromContext",
	}
)

// taskBoundaryVariables are the uses, in task files, of package-level
// variables declared in shared files. Every one of them must be a read.
var taskBoundaryVariables = []string{
	// Read, to copy: the follow table of a session starts from a clone of
	// the table the session uses without a follower, which the debug setting
	// chooses.
	"task_client.go: taskFollowMethodInfos: allowmissingclientparams",
	"task_client.go: taskFollowMethodInfos: clientMethodInfos",
	"task_client.go: taskFollowMethodInfos: clientMethodInfosLegacyParams",
	// Read: a notification of the extension reports a closed connection with
	// the error every other notification reports it with.
	"task_send.go: taskNotify: ErrConnectionClosed (x2)",
	// Read, to tell a standard request from a method the program registered.
	"task_send.go: requestDeclaresTasks: serverMethodInfos",
}

// taskBoundaryFields are the unexported fields of shared types that task
// files use, by the declaration that uses them. A task file that starts to
// use another one reaches into state every program has: check that it only
// reads it, or that only an installed or opted-in extension gets there.
var taskBoundaryFields = []string{
	"task_client.go: ClientSession.FollowTask: newTaskTimer",
	"task_client.go: ClientSession.callUsesTasks: client",
	"task_client.go: ClientSession.registerTaskFollower: taskFollowers",
	"task_client.go: ClientSession.registerTaskFollower: taskFollowersMu",
	"task_client.go: ClientSession.registerTaskFollower: taskReceive",
	"task_client.go: ClientSession.serverAdvertisesTasks: state",
	"task_client.go: ClientSession.taskFollowersFor: taskFollowers",
	"task_client.go: ClientSession.taskFollowersFor: taskFollowersMu",
	"task_client.go: ClientSession.unregisterTaskFollower: taskFollowers",
	"task_client.go: ClientSession.unregisterTaskFollower: taskFollowersMu",
	"task_client.go: ClientSession.unregisterTaskFollower: taskReceive",
	"task_client.go: clientDeclaresTasks: opts",
	"task_client.go: sendTaskCallTool: resultType",
	"task_client.go: sendTaskCallTool: task",
	"task_client.go: taskFollowMethodInfos: handleMethod",
	"task_client.go: taskFollowMethodInfos: unmarshalParams",
	"task_client.go: withTasksCapability: client",
	"task_client.go: withTasksCapability: state",
	"task_input.go: TaskContext.RequireInput: opts",
	"task_methods.go: taskMethodInfo: unmarshalParams",
	"task_notifications.go: taskSubscription.deliver: opts",
	"task_send.go: requestDeclaresTasks: client",
	"task_send.go: sendDeclaredRequest: newResult",
	"task_server.go: AddTaskTool: opts",
	"task_server.go: TaskServer.finishTask: opts",
	"task_server.go: TaskServer.startTask: task",
	"task_server.go: TaskServer.startTask: taskCreated",
	"tasks.go: CallToolResult.Task: task",
	"tasks.go: NewTaskServer: mu",
	"tasks.go: NewTaskServer: receiveMethods",
	"tasks.go: NewTaskServer: tasks",
	"tasks.go: marshalCreateTaskResult: task",
	"tasks.go: taskListenMethodInfo: unmarshalParams",
}

// taskBoundaryMapWrites are the writes, in task files, to maps the task
// files do not own: a map in a field or a variable declared elsewhere, or
// one a function returned. A method table or a registry of the shared code
// is such a map.
var taskBoundaryMapWrites = []string{
	"task_client.go: ClientSession.registerTaskFollower: cs.taskFollowers",
	"task_client.go: ClientSession.registerTaskFollower: cs.taskFollowers[id]",
	"task_client.go: ClientSession.unregisterTaskFollower: cs.taskFollowers",
	"task_client.go: ClientSession.unregisterTaskFollower: cs.taskFollowers[id]",
	"task_client.go: withTasksCapability: caps.Extensions",
	"task_notifications.go: TaskServer.subscribe: s.subscriptions[key]",
	"task_notifications.go: taskSubscription.unfollowLocked: s.subscriptions[key]",
}

// taskBoundaryDeclared are the package-level variables task files declare.
// Their initializers run in every program: each is a plain value, and none
// calls into or changes anything outside the task files.
var taskBoundaryDeclared = []string{
	"task_client.go: errTaskListenEnded",
	"task_input.go: errTaskUnchanged",
	"task_server.go: errTaskTerminal",
	"tasks.go: ErrTaskNotFound",
	"tasks.go: taskMethods",
}

// taskBoundaryMethods are the methods task files declare on types of shared
// files. None of them is a method something else calls by its name (a JSON
// codec, a Stringer, an interface of the shared code): each is either new
// API or called from a place listed above.
var taskBoundaryMethods = []string{
	"task_client.go: ClientSession.CancelTask",
	"task_client.go: ClientSession.FollowTask",
	"task_client.go: ClientSession.GetTask",
	"task_client.go: ClientSession.UpdateTask",
	"task_client.go: ClientSession.acknowledgeTasks",
	"task_client.go: ClientSession.answerTaskInput",
	"task_client.go: ClientSession.callTaskNotificationHandler",
	"task_client.go: ClientSession.callUsesTasks",
	"task_client.go: ClientSession.deliverTaskNotification",
	"task_client.go: ClientSession.listenForTask",
	"task_client.go: ClientSession.registerTaskFollower",
	"task_client.go: ClientSession.serverAdvertisesTasks",
	"task_client.go: ClientSession.taskFollowersFor",
	"task_client.go: ClientSession.unregisterTaskFollower",
	"task_client.go: ClientSession.useTasks",
	"tasks.go: CallToolResult.Task",
}

func isTaskFile(name string) bool {
	name = filepath.Base(name)
	return name == "tasks.go" || strings.HasPrefix(name, "task")
}

// enclosingTaskBoundaryDecl names the top-level declaration of file that
// contains pos.
func enclosingTaskBoundaryDecl(file *ast.File, pos token.Pos) string {
	for _, decl := range file.Decls {
		if pos < decl.Pos() || pos >= decl.End() {
			continue
		}
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if decl.Recv != nil && len(decl.Recv.List) == 1 {
				recv := decl.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if index, ok := recv.(*ast.IndexExpr); ok {
					recv = index.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					return id.Name + "." + decl.Name.Name
				}
			}
			return decl.Name.Name
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				if pos < spec.Pos() || pos >= spec.End() {
					continue
				}
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					return spec.Name.Name
				case *ast.ValueSpec:
					return spec.Names[0].Name
				}
			}
			return decl.Tok.String()
		}
	}
	return "file"
}

// taskBoundaryPackage is this package's non-test files, parsed and type
// checked from source. It is loaded once per test process.
type taskBoundaryPackage struct {
	fset  *token.FileSet
	files []*ast.File
	pkg   *types.Package
	info  *types.Info
}

var loadTaskBoundaryPackage = sync.OnceValues(func() (*taskBoundaryPackage, error) {
	dir, err := build.ImportDir(".", 0)
	if err != nil {
		return nil, err
	}
	// Every non-test Go file of the directory is in this build: a file that
	// a build constraint leaves out would not be looked at.
	if left := slices.Concat(dir.IgnoredGoFiles, dir.InvalidGoFiles, dir.CgoFiles); len(left) != 0 {
		return nil, fmt.Errorf("Go files that are not part of this build: %v", left)
	}
	p := &taskBoundaryPackage{
		fset: token.NewFileSet(),
		info: &types.Info{
			Uses:  make(map[*ast.Ident]types.Object),
			Defs:  make(map[*ast.Ident]types.Object),
			Types: make(map[ast.Expr]types.TypeAndValue),
		},
	}
	for _, name := range dir.GoFiles {
		file, err := parser.ParseFile(p.fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		p.files = append(p.files, file)
	}
	config := &types.Config{Importer: importer.ForCompiler(p.fset, "source", nil)}
	p.pkg, err = config.Check(dir.ImportPath, p.fset, p.files, p.info)
	if err != nil {
		return nil, err
	}
	return p, nil
})

func TestTaskBoundary(t *testing.T) {
	pkg, err := loadTaskBoundaryPackage()
	if err != nil {
		t.Fatalf("loading the package: %v", err)
	}
	taskWord := regexp.MustCompile(`(?i)task`)
	var tokens, references, variables, methods, fields, writes, declared []string
	shared, taskFiles := 0, 0
	for _, file := range pkg.files {
		name := filepath.Base(pkg.fset.Position(file.Pos()).Filename)
		entry := func(pos token.Pos, what string) string {
			return fmt.Sprintf("%s: %s: %s", name, enclosingTaskBoundaryDecl(file, pos), what)
		}
		if isTaskFile(name) {
			taskFiles++
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				if fn.Recv == nil {
					if fn.Name.Name == "init" {
						t.Errorf("%s declares an init function: the extension does nothing until it is installed or a call opts in", name)
					}
					continue
				}
				obj, _ := pkg.info.Defs[fn.Name].(*types.Func)
				if obj == nil {
					t.Fatalf("%s: no type information for method %s", name, fn.Name.Name)
				}
				recv := obj.Type().(*types.Signature).Recv().Type()
				if ptr, ok := recv.(*types.Pointer); ok {
					recv = ptr.Elem()
				}
				if named, ok := recv.(*types.Named); ok && !isTaskFile(pkg.fset.Position(named.Obj().Pos()).Filename) {
					methods = append(methods, fmt.Sprintf("%s: %s.%s", name, named.Obj().Name(), fn.Name.Name))
				}
			}
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.VAR {
					continue
				}
				for _, spec := range gen.Specs {
					for _, id := range spec.(*ast.ValueSpec).Names {
						declared = append(declared, name+": "+id.Name)
					}
				}
			}
			// outside reports whether obj is declared outside the task
			// files: in a shared file or in another package.
			outside := func(obj types.Object) bool {
				return obj.Pkg() != pkg.pkg || !isTaskFile(pkg.fset.Position(obj.Pos()).Filename)
			}
			// packageVar returns the package-level variable expr names.
			packageVar := func(expr ast.Expr) *types.Var {
				var id *ast.Ident
				switch expr := ast.Unparen(expr).(type) {
				case *ast.Ident:
					id = expr
				case *ast.SelectorExpr:
					id = expr.Sel
				default:
					return nil
				}
				obj, ok := pkg.info.Uses[id].(*types.Var)
				if !ok || obj.IsField() || obj.Pkg() == nil || obj.Parent() != obj.Pkg().Scope() {
					return nil
				}
				return obj
			}
			assigned := func(target ast.Expr) {
				if obj := packageVar(target); obj != nil && outside(obj) {
					t.Errorf("%s assigns to %s.%s, a package-level variable declared outside the task files", entry(target.Pos(), ""), obj.Pkg().Name(), obj.Name())
				}
			}
			// written records a write to a map: one a task file does not
			// own, or cannot be seen to own, is listed.
			written := func(target ast.Expr) {
				target = ast.Unparen(target)
				if _, ok := pkg.info.TypeOf(target).Underlying().(*types.Map); !ok {
					return
				}
				switch target := target.(type) {
				case *ast.Ident:
					if obj := packageVar(target); obj != nil && outside(obj) {
						writes = append(writes, entry(target.Pos(), obj.Name()))
					}
				case *ast.SelectorExpr:
					if obj, ok := pkg.info.Uses[target.Sel].(*types.Var); ok && outside(obj) {
						writes = append(writes, entry(target.Pos(), types.ExprString(target)))
					}
				default:
					writes = append(writes, entry(target.Pos(), types.ExprString(target)))
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.AssignStmt:
					for _, target := range node.Lhs {
						assigned(target)
						if index, ok := ast.Unparen(target).(*ast.IndexExpr); ok {
							written(index.X)
						}
					}
				case *ast.IncDecStmt:
					assigned(node.X)
					if index, ok := ast.Unparen(node.X).(*ast.IndexExpr); ok {
						written(index.X)
					}
				case *ast.CallExpr:
					switch types.ExprString(node.Fun) {
					case "delete", "clear", "maps.Copy", "maps.Insert", "maps.DeleteFunc":
						if len(node.Args) > 0 {
							written(node.Args[0])
						}
					}
				case *ast.Ident:
					obj, ok := pkg.info.Uses[node].(*types.Var)
					if !ok || obj.Pkg() != pkg.pkg || !outside(obj) {
						return true
					}
					switch {
					case obj.IsField() && !obj.Exported():
						fields = append(fields, entry(node.Pos(), obj.Name()))
					case obj.Parent() == pkg.pkg.Scope():
						variables = append(variables, entry(node.Pos(), obj.Name()))
					}
				}
				return true
			})
			continue
		}
		shared++
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.BasicLit:
				if node.Kind == token.STRING && taskWord.MatchString(node.Value) {
					tokens = append(tokens, entry(node.Pos(), node.Value))
				}
			case *ast.Ident:
				if taskWord.MatchString(node.Name) {
					tokens = append(tokens, entry(node.Pos(), node.Name))
				}
				obj := pkg.info.Uses[node]
				if obj == nil || obj.Pkg() != pkg.pkg || !obj.Pos().IsValid() {
					break
				}
				if !isTaskFile(pkg.fset.Position(obj.Pos()).Filename) {
					break
				}
				what := obj.Name()
				if fn, ok := obj.(*types.Func); ok {
					if recv := fn.Type().(*types.Signature).Recv(); recv != nil {
						recvType := recv.Type()
						if ptr, ok := recvType.(*types.Pointer); ok {
							recvType = ptr.Elem()
						}
						if named, ok := recvType.(*types.Named); ok {
							what = named.Obj().Name() + "." + what
						}
					}
				}
				references = append(references, entry(node.Pos(), what))
			}
			return true
		})
	}
	// The package has shared files and task files; otherwise the lists
	// below would be compared with nothing.
	if shared == 0 || taskFiles == 0 {
		t.Fatalf("found %d shared files and %d task files", shared, taskFiles)
	}
	compareTaskBoundary(t, "tokens", tokens, taskBoundaryTokens)
	compareTaskBoundary(t, "references", references, taskBoundaryReferences)
	compareTaskBoundary(t, "variables", variables, taskBoundaryVariables)
	compareTaskBoundary(t, "methods", methods, taskBoundaryMethods)
	// A field is listed once for each declaration that uses it.
	slices.Sort(fields)
	compareTaskBoundary(t, "fields", slices.Compact(fields), taskBoundaryFields)
	compareTaskBoundary(t, "map writes", writes, taskBoundaryMapWrites)
	compareTaskBoundary(t, "package-level variables", declared, taskBoundaryDeclared)
}

// The reserved-method check of AddReceivingCustomMethod and the install of
// NewTaskServer exclude each other because both run under the server's
// mutex. A check made before the lock is taken would let both succeed in
// some interleaving, which a test can only produce by chance
// (TestNewTaskServerRacesCustomMethod); so the order is pinned here, in the
// source: the function reads the server's tasks field once, after it has
// locked the mutex.
func TestTaskBoundaryReservedMethodCheckIsLocked(t *testing.T) {
	pkg, err := loadTaskBoundaryPackage()
	if err != nil {
		t.Fatalf("loading the package: %v", err)
	}
	var lock, reads []token.Pos
	found := false
	for _, file := range pkg.files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name.Name != "AddReceivingCustomMethod" {
				continue
			}
			found = true
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.CallExpr:
					if types.ExprString(node.Fun) == "s.mu.Lock" {
						lock = append(lock, node.Pos())
					}
				case *ast.SelectorExpr:
					if types.ExprString(node) == "s.tasks" {
						reads = append(reads, node.Pos())
					}
				}
				return true
			})
		}
	}
	if !found {
		t.Fatal("AddReceivingCustomMethod not found")
	}
	if len(lock) != 1 || len(reads) != 1 {
		t.Fatalf("AddReceivingCustomMethod locks s.mu %d times and reads s.tasks %d times, want once each", len(lock), len(reads))
	}
	if reads[0] < lock[0] {
		t.Fatal("AddReceivingCustomMethod reads s.tasks before it locks s.mu: a concurrent NewTaskServer can install between the check and the registration")
	}
}

func compareTaskBoundary(t *testing.T, what string, found, want []string) {
	t.Helper()
	// A place that occurs more than once is listed once, with its count: a
	// second use of a listed name in the same declaration is a change too.
	counts := make(map[string]int)
	for _, entry := range found {
		counts[entry]++
	}
	var got []string
	for entry, n := range counts {
		if n > 1 {
			entry = fmt.Sprintf("%s (x%d)", entry, n)
		}
		got = append(got, entry)
	}
	slices.Sort(got)
	for _, entry := range got {
		if !slices.Contains(want, entry) {
			t.Errorf("%s: shared code and the tasks extension meet at a place that is not listed: %q", what, entry)
		}
	}
	for _, entry := range want {
		if !slices.Contains(got, entry) {
			t.Errorf("%s: listed but not found: %q", what, entry)
		}
	}
}
