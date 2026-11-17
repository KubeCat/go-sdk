// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// taskPost is one HTTP POST a client made, as the server saw it.
type taskPost struct {
	method   string // JSON-RPC method in the body
	taskID   string // params.taskId in the body
	mcpName  []string
	declares bool // the request's _meta declares the tasks extension
	body     string
}

type taskPostLog struct {
	base http.RoundTripper

	mu    sync.Mutex
	posts []taskPost
}

func (l *taskPostLog) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost && req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(body))
		var message struct {
			Method string `json:"method"`
			Params struct {
				Meta   Meta `json:"_meta"`
				TaskID any  `json:"taskId"`
			} `json:"params"`
		}
		if err := json.Unmarshal(body, &message); err == nil && message.Method != "" {
			id, _ := message.Params.TaskID.(string)
			l.mu.Lock()
			l.posts = append(l.posts, taskPost{
				method:   message.Method,
				taskID:   id,
				mcpName:  req.Header.Values(nameHeader),
				declares: metaUsesTasks(message.Params.Meta),
				body:     string(body),
			})
			l.mu.Unlock()
		}
	}
	return l.base.RoundTrip(req)
}

// take returns the posts for method (every post if method is empty) and
// forgets all posts recorded so far.
func (l *taskPostLog) take(method string) []taskPost {
	l.mu.Lock()
	defer l.mu.Unlock()
	var posts []taskPost
	for _, post := range l.posts {
		if method == "" || post.method == method {
			posts = append(posts, post)
		}
	}
	l.posts = nil
	return posts
}

// connectTaskHTTP connects client to server over stateless Streamable HTTP
// and records the client's POSTs.
func connectTaskHTTP(t *testing.T, server *Server, client *Client) (*ClientSession, *taskPostLog) {
	t.Helper()
	handler := NewStreamableHTTPHandler(func(*http.Request) *Server { return server }, &StreamableHTTPOptions{Stateless: true})
	httpServer := httptest.NewServer(handler)
	closeTaskHTTPServer(t, httpServer)
	log := &taskPostLog{base: http.DefaultTransport}
	session, err := client.Connect(taskTestContext(t), &StreamableClientTransport{
		Endpoint:             httpServer.URL,
		HTTPClient:           &http.Client{Transport: log},
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "session", session.Close)
	return session, log
}

// callOwnTaskMethod makes the program's own custom call and reports the
// outcome as text, a panic included.
func callOwnTaskMethod(ctx context.Context, session *ClientSession, method, id string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("panic: %v", r)
		}
	}()
	result, err := CallCustomMethod[*installTestParams, *installTestResult](ctx, session, method, &installTestParams{TaskID: id})
	if err != nil {
		return "error: " + err.Error()
	}
	return "value: " + result.Value
}

func notRegistered(method string) string {
	return fmt.Sprintf("error: mcp: CallCustomMethod: %q is not registered; call AddSendingCustomMethod first", method)
}

// newTaskServerWithTask returns a server with the extension installed and one
// stored task.
func newTaskServerWithTask(t *testing.T, id string) *Server {
	t.Helper()
	server := NewServer(testImpl, nil)
	store := NewMemoryTaskStore()
	if _, err := NewTaskServer(server, &TaskServerOptions{Store: store}); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(taskTestContext(t), "", &DetailedTask{Task: Task{TaskID: id, Status: TaskStatusWorking}}); err != nil {
		t.Fatal(err)
	}
	return server
}

// newOwnTaskMethodServer returns a server without the extension whose program
// registered its own tasks/get, tasks/update and tasks/cancel.
func newOwnTaskMethodServer(t *testing.T) *Server {
	t.Helper()
	server := NewServer(testImpl, nil)
	for _, method := range taskMethods {
		if err := addInstallTestMethod(server, method); err != nil {
			t.Fatal(err)
		}
	}
	return server
}

func connectTaskMemory(t *testing.T, server *Server, client *Client) *ClientSession {
	t.Helper()
	ctx := taskTestContext(t)
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "session", session.Close)
	return session
}

// The task helpers register nothing on the Client: the program's own custom
// tasks/* calls answer as they do without the extension before and after every
// helper call, on the helper's session and on another session of the Client.
func TestTaskHelpersLeaveCustomMethodsUnchanged(t *testing.T) {
	ctx := taskTestContext(t)
	client := NewClient(&Implementation{Name: "task-send-client", Version: "v1.0.0"}, nil)
	tasks := connectTaskMemory(t, newTaskServerWithTask(t, "task-1"), client)
	other := connectTaskMemory(t, newOwnTaskMethodServer(t), client)

	helpers := func() {
		t.Helper()
		if task, err := tasks.GetTask(ctx, &GetTaskParams{TaskID: "task-1"}); err != nil || task.TaskID != "task-1" {
			t.Fatalf("GetTask = %+v, %v", task, err)
		}
		if err := tasks.UpdateTask(ctx, &UpdateTaskParams{TaskID: "task-1"}); err != nil {
			t.Fatalf("UpdateTask: %v", err)
		}
		if err := tasks.CancelTask(ctx, &CancelTaskParams{TaskID: "task-1"}); err != nil {
			t.Fatalf("CancelTask: %v", err)
		}
	}
	unregistered := func(when string) {
		t.Helper()
		for _, method := range taskMethods {
			for name, session := range map[string]*ClientSession{"the helpers' session": tasks, "another session": other} {
				if got := callOwnTaskMethod(ctx, session, method, "own-1"); got != notRegistered(method) {
					t.Fatalf("%s, %s on %s: %s; want %s", when, method, name, got, notRegistered(method))
				}
			}
		}
	}
	unregistered("before any helper call")
	helpers()
	unregistered("after the helper calls")

	for _, method := range taskMethods {
		if err := AddSendingCustomMethod[*installTestParams, *installTestResult](client, method); err != nil {
			t.Fatal(err)
		}
	}
	registered := func(when string) {
		t.Helper()
		for _, method := range taskMethods {
			if got := callOwnTaskMethod(ctx, other, method, "own-1"); got != "value: own:own-1" {
				t.Fatalf("%s, the program's %s: %s", when, method, got)
			}
			// On the task server the program's call does not declare the
			// extension: the server refuses it, and the client reports that
			// in the program's own types.
			if got := callOwnTaskMethod(ctx, tasks, method, "task-1"); !strings.Contains(got, "missing required tasks extension capability") {
				t.Fatalf("%s, the program's %s on the task server: %s", when, method, got)
			}
		}
	}
	registered("with the program's registration")
	helpers()
	registered("after helper calls beside the program's registration")
}

// Sending middleware gets the helper's context. A call the program makes with
// that context is not the helper's request: it is sent as the program's own
// method table says, and carries no Mcp-Name.
func TestTaskHelperContextDoesNotChangeOtherCalls(t *testing.T) {
	for _, register := range []bool{true, false} {
		name := "unregistered"
		if register {
			name = "registered"
		}
		t.Run(name, func(t *testing.T) {
			ctx := taskTestContext(t)
			client := NewClient(&Implementation{Name: "task-send-client", Version: "v1.0.0"}, nil)
			if register {
				for _, method := range taskMethods {
					if err := AddSendingCustomMethod[*installTestParams, *installTestResult](client, method); err != nil {
						t.Fatal(err)
					}
				}
			}
			// The server has the program's own methods, so that a nested
			// call can succeed; the helpers decode its answers leniently.
			session, posts := connectTaskHTTP(t, newOwnTaskMethodServer(t), client)
			second := connectTaskMemory(t, newOwnTaskMethodServer(t), client)
			nested, nest := taskEvents[string](t, "nested calls")
			client.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
				return func(ctx context.Context, method string, req Request) (Result, error) {
					switch req.GetParams().(type) {
					case *GetTaskParams, *UpdateTaskParams, *CancelTaskParams:
						nest(callOwnTaskMethod(ctx, session, method, "nested-1"))
						nest(callOwnTaskMethod(ctx, second, method, "nested-2"))
					}
					return next(ctx, method, req)
				}
			})
			posts.take("")
			helpers := map[string]func() error{
				methodGetTask: func() error {
					_, err := session.GetTask(ctx, &GetTaskParams{TaskID: "task-1"})
					return err
				},
				methodUpdateTask: func() error { return session.UpdateTask(ctx, &UpdateTaskParams{TaskID: "task-1"}) },
				methodCancelTask: func() error { return session.CancelTask(ctx, &CancelTaskParams{TaskID: "task-1"}) },
			}
			for _, method := range taskMethods {
				if err := helpers[method](); err != nil {
					t.Fatalf("%s helper: %v", method, err)
				}
				wantSame, wantSecond := notRegistered(method), notRegistered(method)
				if register {
					wantSame, wantSecond = "value: own:nested-1", "value: own:nested-2"
				}
				if got := waitTask(t, nested, "the nested call"); got != wantSame {
					t.Fatalf("%s: nested call on the helper's session: %s; want %s", method, got, wantSame)
				}
				if got := waitTask(t, nested, "the nested call on a second session"); got != wantSecond {
					t.Fatalf("%s: nested call on a second session: %s; want %s", method, got, wantSecond)
				}
				var sawHelper, sawNested bool
				for _, post := range posts.take(method) {
					switch post.taskID {
					case "task-1":
						sawHelper = true
						if len(post.mcpName) != 1 || post.mcpName[0] != "task-1" || !post.declares {
							t.Fatalf("%s helper request: Mcp-Name %q, declares %v", method, post.mcpName, post.declares)
						}
					case "nested-1":
						sawNested = true
						if len(post.mcpName) != 0 || post.declares {
							t.Fatalf("%s nested request: Mcp-Name %q, declares %v; want neither", method, post.mcpName, post.declares)
						}
					default:
						t.Fatalf("unexpected %s request for %q", method, post.taskID)
					}
				}
				if !sawHelper || sawNested != register {
					t.Fatalf("%s: helper request sent = %v, nested request sent = %v", method, sawHelper, sawNested)
				}
			}
		})
	}
}

// Over Streamable HTTP each helper names the task in Mcp-Name, and a server
// with the extension accepts the request.
func TestTaskHelpersSendMcpName(t *testing.T) {
	ctx := taskTestContext(t)
	client := NewClient(&Implementation{Name: "task-send-client", Version: "v1.0.0"}, nil)
	session, posts := connectTaskHTTP(t, newTaskServerWithTask(t, "task-1"), client)
	posts.take("")
	if task, err := session.GetTask(ctx, &GetTaskParams{TaskID: "task-1"}); err != nil || task.TaskID != "task-1" {
		t.Fatalf("GetTask = %+v, %v", task, err)
	}
	if err := session.UpdateTask(ctx, &UpdateTaskParams{TaskID: "task-1"}); err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}
	if err := session.CancelTask(ctx, &CancelTaskParams{TaskID: "task-1"}); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	all := posts.take("")
	for _, method := range taskMethods {
		var names [][]string
		for _, post := range all {
			if post.method == method {
				names = append(names, post.mcpName)
			}
		}
		if len(names) != 1 || len(names[0]) != 1 || names[0][0] != "task-1" {
			t.Errorf("%s: Mcp-Name per request = %q, want one request naming task-1", method, names)
		}
	}
}

// A task id that is not header-safe is sent Base64-wrapped in Mcp-Name, as
// the name of a tool is, and the server reads it back.
func TestTaskHelpersEncodeMcpName(t *testing.T) {
	const id = "tâche 1 "
	ctx := taskTestContext(t)
	client := NewClient(&Implementation{Name: "task-send-client", Version: "v1.0.0"}, nil)
	session, posts := connectTaskHTTP(t, newTaskServerWithTask(t, id), client)
	posts.take("")
	if task, err := session.GetTask(ctx, &GetTaskParams{TaskID: id}); err != nil || task.TaskID != id {
		t.Fatalf("GetTask = %+v, %v", task, err)
	}
	if err := session.CancelTask(ctx, &CancelTaskParams{TaskID: id}); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	all := posts.take("")
	if len(all) != 2 {
		t.Fatalf("the client sent %d requests, want 2", len(all))
	}
	for _, post := range all {
		if len(post.mcpName) != 1 || post.mcpName[0] != encodeBase64(id) {
			t.Errorf("%s: Mcp-Name = %q, want the wrapped id %q", post.method, post.mcpName, encodeBase64(id))
		}
	}
}

// A request that opts in declares the capabilities the client declares on
// every other request, plus the extension; capabilities the caller wrote into
// _meta are kept instead.
func TestTaskRequestsDeclareClientCapabilities(t *testing.T) {
	ctx := taskTestContext(t)
	server := newTaskServerWithTask(t, "task-1")
	server.AddTool(&Tool{Name: "plain", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest) (*CallToolResult, error) { return &CallToolResult{}, nil })
	type declared struct {
		method string
		caps   *clientCapabilitiesV2
	}
	seen, see := taskEvents[declared](t, "declared capabilities")
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			switch method {
			case methodGetTask, methodUpdateTask, methodCancelTask, methodCallTool:
				caps, _ := decodeMetaValue[*clientCapabilitiesV2](req.GetParams().GetMeta(), MetaKeyClientCapabilities)
				see(declared{method, caps})
			}
			return next(ctx, method, req)
		}
	})
	client := NewClient(&Implementation{Name: "task-send-client", Version: "v1.0.0"}, &ClientOptions{
		ElicitationHandler: func(context.Context, *ElicitRequest) (*ElicitResult, error) {
			return &ElicitResult{Action: "decline"}, nil
		},
	})
	session := connectTaskMemory(t, server, client)
	own := Meta{MetaKeyClientCapabilities: &ClientCapabilities{Experimental: map[string]any{"fixture": map[string]any{}}}}
	calls := []struct {
		name string
		call func(Meta) error
	}{
		{methodGetTask, func(meta Meta) error {
			_, err := session.GetTask(ctx, &GetTaskParams{TaskID: "task-1", ParamsBase: ParamsBase{Meta: meta}})
			return err
		}},
		{methodUpdateTask, func(meta Meta) error {
			return session.UpdateTask(ctx, &UpdateTaskParams{TaskID: "task-1", ParamsBase: ParamsBase{Meta: meta}})
		}},
		{methodCancelTask, func(meta Meta) error {
			return session.CancelTask(ctx, &CancelTaskParams{TaskID: "task-1", ParamsBase: ParamsBase{Meta: meta}})
		}},
		{methodCallTool, func(meta Meta) error {
			_, err := session.CallTool(ctx, &CallToolParams{Name: "plain", UseTasks: true, Meta: meta})
			return err
		}},
	}
	for _, call := range calls {
		if err := call.call(nil); err != nil {
			t.Fatalf("%s: %v", call.name, err)
		}
		got := waitTask(t, seen, call.name)
		if got.method != call.name || got.caps == nil {
			t.Fatalf("%s: server saw %+v", call.name, got)
		}
		if _, ok := got.caps.Extensions[ExtensionTasks]; !ok {
			t.Errorf("%s does not declare the tasks extension: %+v", call.name, got.caps)
		}
		if got.caps.Elicitation == nil {
			t.Errorf("%s hides the client's elicitation capability: %+v", call.name, got.caps)
		}

		if err := call.call(own); err != nil {
			t.Fatalf("%s with the caller's capabilities: %v", call.name, err)
		}
		got = waitTask(t, seen, call.name)
		if _, ok := got.caps.Extensions[ExtensionTasks]; !ok {
			t.Errorf("%s with the caller's capabilities does not declare the tasks extension", call.name)
		}
		if _, ok := got.caps.Experimental["fixture"]; !ok || got.caps.Elicitation != nil {
			t.Errorf("%s replaced the capabilities the caller declared: %+v", call.name, got.caps)
		}
	}
	if caps := own[MetaKeyClientCapabilities].(*ClientCapabilities); len(caps.Extensions) != 0 {
		t.Errorf("the caller's capabilities were changed: %+v", caps)
	}
}

// The listen request FollowTask sends declares the client's capabilities and
// the extension, like every other request that opts in.
func TestTaskListenDeclaresClientCapabilities(t *testing.T) {
	server := newTaskServerWithTask(t, "task-1")
	seen, see := taskEvents[*clientCapabilitiesV2](t, "declared capabilities")
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if listen, ok := req.GetParams().(*SubscriptionsListenParams); ok && listen.Notifications != nil && len(listen.Notifications.TaskIDs) > 0 {
				caps, _ := decodeMetaValue[*clientCapabilitiesV2](listen.Meta, MetaKeyClientCapabilities)
				see(caps)
			}
			return next(ctx, method, req)
		}
	})
	client := NewClient(&Implementation{Name: "task-send-client", Version: "v1.0.0"}, &ClientOptions{
		ElicitationHandler: func(context.Context, *ElicitRequest) (*ElicitResult, error) {
			return &ElicitResult{Action: "decline"}, nil
		},
	})
	session := connectTaskMemory(t, server, client)
	ctx, cancel := context.WithCancel(taskTestContext(t))
	defer cancel()
	session.listenForTask(ctx, "task-1")
	caps := waitTask(t, seen, "the task listen")
	if caps == nil || caps.Elicitation == nil {
		t.Errorf("the task listen hides the client's elicitation capability: %+v", caps)
	}
	if caps == nil || caps.Extensions[ExtensionTasks] == nil {
		t.Errorf("the task listen does not declare the tasks extension: %+v", caps)
	}
}

// taskCountingValue counts how often it is encoded.
type taskCountingValue struct{ encoded *atomic.Int32 }

func (v taskCountingValue) MarshalJSON() ([]byte, error) {
	v.encoded.Add(1)
	return []byte(`{}`), nil
}

// Client capabilities in a request's _meta that show as they are that they
// do not name the extension are not encoded to find that out: a call that
// does not use the extension encodes its values as often as without it. And
// such a shape says what encoding the value says.
func TestTaskMetaCapabilitiesNotEncodedToBeRead(t *testing.T) {
	var encoded atomic.Int32
	other := map[string]any{"example.test/other": taskCountingValue{&encoded}}
	tasks := map[string]any{ExtensionTasks: map[string]any{}}
	slow := func(v any) bool {
		caps, ok := decodeMetaValue[*clientCapabilitiesV2](Meta{MetaKeyClientCapabilities: v}, MetaKeyClientCapabilities)
		if !ok || caps == nil {
			return false
		}
		_, named := caps.Extensions[ExtensionTasks]
		return named
	}
	for _, test := range []struct {
		name  string
		value any
		plain bool // the value shows without encoding that it names no tasks
		uses  bool
	}{
		{"a decoded object naming another extension", map[string]any{"extensions": other}, true, false},
		{"a decoded object without extensions", map[string]any{"sampling": map[string]any{}}, true, false},
		{"a decoded object with null extensions", map[string]any{"extensions": nil}, true, false},
		{"a decoded object naming extensions in another case", map[string]any{"Extensions": tasks}, true, false},
		{"a decoded object that does not decode as capabilities", map[string]any{"extensions": other, "roots": 7}, true, false},
		{"capabilities naming another extension", ClientCapabilities{Extensions: other}, true, false},
		{"a capabilities pointer naming another extension", &ClientCapabilities{Extensions: other}, true, false},
		{"a nil capabilities pointer", (*ClientCapabilities)(nil), true, false},
		{"a decoded object naming tasks", map[string]any{"extensions": tasks}, false, true},
		{"a decoded object naming tasks that does not decode as capabilities", map[string]any{"extensions": tasks, "roots": 7}, false, false},
		{"a decoded object with extensions of another type", map[string]any{"extensions": json.RawMessage(`{"` + ExtensionTasks + `":{}}`)}, false, true},
		{"capabilities naming tasks", ClientCapabilities{Extensions: tasks}, false, true},
		{"a capabilities pointer naming tasks", &ClientCapabilities{Extensions: tasks}, false, true},
		{"raw JSON naming tasks", json.RawMessage(`{"extensions":{"` + ExtensionTasks + `":{}}}`), false, true},
		{"raw JSON naming another extension", json.RawMessage(`{"extensions":{"example.test/other":{}}}`), false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := slow(test.value); got != test.uses {
				t.Fatalf("encoded and decoded, the value names tasks = %v, want %v", got, test.uses)
			}
			encoded.Store(0)
			meta := Meta{MetaKeyClientCapabilities: test.value}
			if got := capabilitiesOmitTasks(test.value); got != test.plain {
				t.Errorf("capabilitiesOmitTasks = %v, want %v", got, test.plain)
			}
			if got := metaUsesTasks(meta); got != test.uses {
				t.Errorf("metaUsesTasks = %v, want %v", got, test.uses)
			}
			session := &ClientSession{client: NewClient(testImpl, nil)}
			session.state.InitializeResult = &InitializeResult{ProtocolVersion: protocolVersion20260728}
			if got := session.callUsesTasks(&CallToolParams{Meta: meta}); got != test.uses {
				t.Errorf("callUsesTasks = %v, want %v", got, test.uses)
			}
			if got := encoded.Load(); test.plain && got != 0 {
				t.Errorf("the value was encoded %d times to be read, want 0", got)
			}
		})
	}
}
