// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// scriptedResult is a tools/call answer written byte for byte.
type scriptedResult struct {
	ResultBase
	body string
}

func (r *scriptedResult) MarshalJSON() ([]byte, error) { return []byte(r.body), nil }

// newScriptedToolServer returns a server that answers tools/call for a tool
// named in answers with those bytes, whatever the request declares.
func newScriptedToolServer(answers map[string]string) *Server {
	server := NewServer(testImpl, nil)
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if params, ok := req.GetParams().(*CallToolParamsRaw); ok && method == methodCallTool {
				if body, ok := answers[params.Name]; ok {
					return &scriptedResult{body: body}, nil
				}
			}
			return next(ctx, method, req)
		}
	})
	return server
}

func marshalForTaskTest(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

const (
	taskShapedAnswer  = `{"resultType":"task","taskId":"t-1","status":"working","createdAt":"2026-07-28T10:30:00Z","lastUpdatedAt":"2026-07-28T10:30:00Z","ttlMs":null,"content":[{"type":"text","text":"hi"}]}`
	taskShapedMarshal = `{"content":[{"type":"text","text":"hi"}],"resultType":"task"}`
)

// Only a CallTool that set UseTasks reads a task from the answer. It returns
// the handle through Task, refuses a malformed task, and its result marshals
// as the unchanged SDK's does. A plain call decodes the same answers as the unchanged SDK.
func TestCallToolUseTasksDecodesHandle(t *testing.T) {
	ctx := taskTestContext(t)
	server := newScriptedToolServer(map[string]string{
		"task":       taskShapedAnswer,
		"numeric-id": `{"resultType":"task","taskId":7,"status":"working"}`,
		"no-id":      `{"resultType":"task","status":"working"}`,
		"bad-status": `{"resultType":"task","taskId":"t-1","status":"napping"}`,
		"bad-ttl":    `{"resultType":"task","taskId":"t-1","status":"working","ttlMs":"soon"}`,
		"complete":   `{"resultType":"complete","taskId":"t-1","status":"working","content":[{"type":"text","text":"hi"}]}`,
	})
	client := NewClient(&Implementation{Name: "task-call-client", Version: "v1.0.0"}, nil)
	session := connectTaskMemory(t, server, client)
	tests := []struct {
		tool      string
		plain     string // what a plain call's result marshals as
		wantTask  bool
		wantError bool
	}{
		{tool: "task", plain: taskShapedMarshal, wantTask: true},
		{tool: "numeric-id", plain: `{"content":[],"resultType":"task"}`, wantError: true},
		{tool: "no-id", plain: `{"content":[],"resultType":"task"}`, wantError: true},
		{tool: "bad-status", plain: `{"content":[],"resultType":"task"}`, wantError: true},
		{tool: "bad-ttl", plain: `{"content":[],"resultType":"task"}`, wantError: true},
		{tool: "complete", plain: `{"content":[{"type":"text","text":"hi"}],"resultType":"complete"}`},
	}
	for _, test := range tests {
		t.Run(test.tool, func(t *testing.T) {
			plain, err := session.CallTool(ctx, &CallToolParams{Name: test.tool})
			if err != nil {
				t.Fatalf("plain call: %v", err)
			}
			if task, ok := plain.Task(); ok || task != nil {
				t.Errorf("plain call returned a task: %+v", task)
			}
			if got := marshalForTaskTest(t, plain); got != test.plain {
				t.Errorf("plain call result marshals as %s, want %s", got, test.plain)
			}

			opted, err := session.CallTool(ctx, &CallToolParams{Name: test.tool, UseTasks: true})
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "invalid task result") {
					t.Fatalf("opted-in call = %+v, %v; want an invalid task result error", opted, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("opted-in call: %v", err)
			}
			task, ok := opted.Task()
			if ok != test.wantTask {
				t.Fatalf("opted-in call: Task() = %+v, %v; want a task: %v", task, ok, test.wantTask)
			}
			if ok && (task.TaskID != "t-1" || task.Status != TaskStatusWorking || task.CreatedAt != "2026-07-28T10:30:00Z") {
				t.Errorf("opted-in call: task = %+v", task)
			}
			if got := marshalForTaskTest(t, opted); got != test.plain {
				t.Errorf("opted-in call result marshals as %s, want %s", got, test.plain)
			}
		})
	}
}

// A program that declares the extension itself, in the client's capabilities
// or in the client capabilities of a call's _meta, uses it: CallTool returns
// the handle of the task the server created. Everything such a program could
// see before UseTasks existed is unchanged: the result marshals as it did, and
// an answer that is not a valid task is returned as it was, without a handle
// and without an error. (The requests are unchanged too; the non-user wire
// gate compares them with the unchanged SDK's.)
func TestCallToolDeclaredExtensionReturnsHandle(t *testing.T) {
	if metaUsesTasks(Meta{MetaKeyClientCapabilities: (*ClientCapabilities)(nil)}) {
		t.Error("nil client capabilities in _meta declare the extension")
	}
	answers := map[string]string{
		"task":       taskShapedAnswer,
		"numeric-id": `{"resultType":"task","taskId":7,"status":"working"}`,
		"bad-status": `{"resultType":"task","taskId":"t-1","status":"napping"}`,
		"complete":   `{"resultType":"complete","taskId":"t-1","status":"working","content":[{"type":"text","text":"hi"}]}`,
	}
	declaring := &ClientCapabilities{Extensions: map[string]any{ExtensionTasks: map[string]any{}}}
	declaringMeta := func() Meta { return Meta{MetaKeyClientCapabilities: declaring} }
	tests := []struct {
		name     string
		caps     *ClientCapabilities
		meta     func() Meta
		version  string
		wantTask bool
	}{
		{name: "client capabilities", caps: declaring, wantTask: true},
		{name: "call _meta", meta: declaringMeta, wantTask: true},
		{name: "neither"},
		{name: "another extension", caps: &ClientCapabilities{Extensions: map[string]any{"example.com/other": map[string]any{}}}},
		// A protocol version without the extension: the declaration means
		// nothing there.
		{name: "client capabilities, legacy session", caps: declaring, version: protocolVersion20251125},
		{name: "call _meta, legacy session", meta: declaringMeta, version: protocolVersion20251125},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := taskTestContext(t)
			client := NewClient(&Implementation{Name: "task-call-client", Version: "v1.0.0"}, &ClientOptions{Capabilities: test.caps})
			clientTransport, serverTransport := NewInMemoryTransports()
			serverSession, err := newScriptedToolServer(answers).Connect(ctx, serverTransport, nil)
			if err != nil {
				t.Fatal(err)
			}
			closeTaskSession(t, "serverSession", serverSession.Close)
			var options *ClientSessionOptions
			if test.version != "" {
				options = &ClientSessionOptions{ProtocolVersion: test.version}
			}
			session, err := client.Connect(ctx, clientTransport, options)
			if err != nil {
				t.Fatal(err)
			}
			closeTaskSession(t, "session", session.Close)
			call := func(tool string) *CallToolResult {
				t.Helper()
				params := &CallToolParams{Name: tool}
				if test.meta != nil {
					params.Meta = test.meta()
				}
				result, err := session.CallTool(ctx, params)
				if err != nil {
					t.Fatalf("%s: %v", tool, err)
				}
				return result
			}
			result := call("task")
			task, ok := result.Task()
			if ok != test.wantTask {
				t.Fatalf("Task() = %+v, %v; want a task: %v", task, ok, test.wantTask)
			}
			if ok && (task.TaskID != "t-1" || task.Status != TaskStatusWorking) {
				t.Errorf("task = %+v", task)
			}
			if test.version == "" {
				if got := marshalForTaskTest(t, result); got != taskShapedMarshal {
					t.Errorf("result marshals as %s, want %s", got, taskShapedMarshal)
				}
			}
			for _, tool := range []string{"numeric-id", "bad-status", "complete"} {
				result := call(tool)
				if task, ok := result.Task(); ok {
					t.Errorf("%s: Task() = %+v, want none", tool, task)
				}
			}
			if test.version == "" {
				if got, want := marshalForTaskTest(t, call("numeric-id")), `{"content":[],"resultType":"task"}`; got != want {
					t.Errorf("numeric-id: result marshals as %s, want %s", got, want)
				}
			}
		})
	}
}

// nestedCall is one plain CallTool made from user code that runs inside an
// opted-in call, with the context that code was given.
type nestedCall struct {
	where   string // which session the plain call was made on
	server  string // the server whose tool ran
	task    bool   // the plain call reported a task handle
	marshal string
	err     error
}

// newNestedTaskServer returns a server with the extension that answers a
// call of "inner" with a task, whatever the request declares, after reporting
// the server's name through reach.
func newNestedTaskServer(t *testing.T, name string, reach func(string)) (*Server, *TaskServer) {
	t.Helper()
	server := NewServer(&Implementation{Name: name, Version: "v1.0.0"}, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if params, ok := req.GetParams().(*CallToolParamsRaw); ok && method == methodCallTool && params.Name == "inner" {
				reach(name)
				return &scriptedResult{body: taskShapedAnswer}, nil
			}
			return next(ctx, method, req)
		}
	})
	return server, tasks
}

// An opted-in CallTool hands its context to the program: to sending
// middleware, and to the elicitation and sampling handlers of a multi
// round-trip. A plain CallTool the program makes with that context, on the
// same session, on another session of the same Client or on another Client,
// is not the opted-in request: it reaches its own server and is decoded as
// the unchanged SDK decodes it, even when the answer is a task. The opted-in call still
// gets its handle after the retry.
func TestCallToolUseTasksLeavesNestedPlainCallsUnchanged(t *testing.T) {
	for _, source := range []string{"elicitation", "sampling", "middleware"} {
		t.Run(source, func(t *testing.T) {
			ctx := taskTestContext(t)
			reached, reach := taskEvents[string](t, "tool calls")
			outerServer, outerTasks := newNestedTaskServer(t, "outer-server", reach)
			outerTasks.AddTool(&Tool{Name: "outer", InputSchema: json.RawMessage(`{"type":"object"}`)},
				func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
					return &CallToolResult{Content: []Content{&TextContent{Text: "outer done"}}}, nil
				})
			// The first round of "outer" asks for input; the retry reaches the
			// task tool. Both rounds declare the extension.
			var roundsMu sync.Mutex
			var rounds []bool
			outerServer.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
				return func(ctx context.Context, method string, req Request) (Result, error) {
					params, ok := req.GetParams().(*CallToolParamsRaw)
					if !ok || method != methodCallTool || params.Name != "outer" {
						return next(ctx, method, req)
					}
					roundsMu.Lock()
					rounds = append(rounds, metaUsesTasks(params.Meta))
					roundsMu.Unlock()
					if len(params.InputResponses) > 0 {
						return next(ctx, method, req)
					}
					var ask InputRequest = &ElicitParams{Message: "go on?"}
					if source == "sampling" {
						ask = &CreateMessageParams{MaxTokens: 1, Messages: []*SamplingMessage{{Role: "user", Content: &TextContent{Text: "go on?"}}}}
					}
					return &CallToolResult{InputRequests: InputRequestMap{"ask": ask}, resultType: resultTypeInputRequired}, nil
				}
			})
			secondServer, _ := newNestedTaskServer(t, "second-server", reach)
			thirdServer, _ := newNestedTaskServer(t, "third-server", reach)

			nested, nest := taskEvents[nestedCall](t, "nested calls")
			sessions := make(map[string]*ClientSession)
			// plainCalls makes the three plain calls. None declares the
			// extension, and each is answered with a task all the same: the
			// answer a plain call must not read a handle from.
			plainCalls := func(ctx context.Context) {
				for _, where := range []string{"same session", "another session of the client", "another client"} {
					result, err := sessions[where].CallTool(ctx, &CallToolParams{Name: "inner"})
					call := nestedCall{where: where, err: err}
					if err == nil {
						_, call.task = result.Task()
						data, _ := json.Marshal(result)
						call.marshal = string(data)
						call.server = waitTask(t, reached, "the nested call's tool")
					}
					nest(call)
				}
			}
			client := NewClient(&Implementation{Name: "task-call-client", Version: "v1.0.0"}, &ClientOptions{
				ElicitationHandler: func(ctx context.Context, _ *ElicitRequest) (*ElicitResult, error) {
					if source == "elicitation" {
						plainCalls(ctx)
					}
					return &ElicitResult{Action: "accept"}, nil
				},
				CreateMessageHandler: func(ctx context.Context, _ *CreateMessageRequest) (*CreateMessageResult, error) {
					plainCalls(ctx)
					return &CreateMessageResult{Model: "fixture", Role: "assistant", Content: &TextContent{Text: "yes"}}, nil
				},
			})
			if source == "middleware" {
				client.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
					return func(ctx context.Context, method string, req Request) (Result, error) {
						if params, ok := req.GetParams().(*CallToolParams); ok && params.Name == "outer" {
							plainCalls(ctx)
						}
						return next(ctx, method, req)
					}
				})
			}
			sessions["same session"] = connectTaskMemory(t, outerServer, client)
			sessions["another session of the client"] = connectTaskMemory(t, secondServer, client)
			sessions["another client"] = connectTaskMemory(t, thirdServer,
				NewClient(&Implementation{Name: "task-call-other-client", Version: "v1.0.0"}, nil))

			outer, err := sessions["same session"].CallTool(ctx, &CallToolParams{Name: "outer", UseTasks: true})
			if err != nil {
				t.Fatalf("opted-in call: %v", err)
			}
			if task, ok := outer.Task(); !ok || task.TaskID == "" {
				t.Fatalf("opted-in call returned no handle after the retry: %+v", outer)
			}
			roundsMu.Lock()
			if fmt.Sprint(rounds) != "[true true]" {
				t.Errorf("rounds of the opted-in call declaring the extension = %v, want two declaring rounds", rounds)
			}
			roundsMu.Unlock()
			want := map[string]string{
				"same session":                  "outer-server",
				"another session of the client": "second-server",
				"another client":                "third-server",
			}
			for range want {
				call := waitTask(t, nested, "a nested plain call")
				if call.err != nil {
					t.Errorf("plain call on %s: %v", call.where, call.err)
					continue
				}
				if call.server != want[call.where] {
					t.Errorf("plain call on %s reached %s, want %s", call.where, call.server, want[call.where])
				}
				if call.task {
					t.Errorf("plain call on %s returned a task handle", call.where)
				}
				if call.marshal != taskShapedMarshal {
					t.Errorf("plain call on %s marshals as %s, want the unchanged SDK's form %s", call.where, call.marshal, taskShapedMarshal)
				}
			}
		})
	}
}

// A result reaches the wire as a task only when the TaskServer created it.
// On a server with the extension, an ordinary tool that returns a result it
// decoded from JSON, or one an upstream CallTool gave it (opted in or not),
// answers with resultType complete, to a caller that declares the extension
// and to one that does not.
func TestTaskServerPlainToolNeverSendsTask(t *testing.T) {
	ctx := taskTestContext(t)
	upstreamClient := NewClient(&Implementation{Name: "task-call-upstream-client", Version: "v1.0.0"}, nil)
	upstream := connectTaskMemory(t, newScriptedToolServer(map[string]string{"task": taskShapedAnswer}), upstreamClient)

	server := NewServer(testImpl, nil)
	if _, err := NewTaskServer(server, nil); err != nil {
		t.Fatal(err)
	}
	schema := json.RawMessage(`{"type":"object"}`)
	server.AddTool(&Tool{Name: "stored", InputSchema: schema}, func(context.Context, *CallToolRequest) (*CallToolResult, error) {
		var result CallToolResult
		if err := json.Unmarshal([]byte(taskShapedAnswer), &result); err != nil {
			return nil, err
		}
		return &result, nil
	})
	server.AddTool(&Tool{Name: "forward", InputSchema: schema}, func(ctx context.Context, _ *CallToolRequest) (*CallToolResult, error) {
		return upstream.CallTool(ctx, &CallToolParams{Name: "task"})
	})
	server.AddTool(&Tool{Name: "forward-opted", InputSchema: schema}, func(ctx context.Context, _ *CallToolRequest) (*CallToolResult, error) {
		result, err := upstream.CallTool(ctx, &CallToolParams{Name: "task", UseTasks: true})
		if err == nil {
			if _, ok := result.Task(); !ok {
				return nil, fmt.Errorf("upstream opted-in call returned no handle")
			}
		}
		return result, err
	})
	session := connectTaskMemory(t, server, NewClient(&Implementation{Name: "task-call-client", Version: "v1.0.0"}, nil))
	// What the server puts on the wire, read by hand before any client
	// decoding. The server labels a result after its receiving middleware has
	// run, so middleware cannot show this.
	peer := newRawTaskPeer(t, server)
	for _, tool := range []string{"stored", "forward", "forward-opted"} {
		for _, declares := range []bool{false, true} {
			meta := rawTaskMeta
			if declares {
				meta = rawTaskMetaDeclaring
			}
			peer.request(methodCallTool, fmt.Sprintf(`{"name":%q,%s}`, tool, meta))
			answer := decodeRawTaskMessage(t, peer.read("the server's answer"))
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(answer.Result, &fields); err != nil {
				t.Fatalf("%s (declares %v): answer %+v: %v", tool, declares, answer, err)
			}
			delete(fields, "_meta") // the server's name and version
			got, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if want := `{"content":[{"type":"text","text":"hi"}],"resultType":"complete"}`; string(got) != want {
				t.Errorf("%s (declares %v): server answered %s, want %s", tool, declares, got, want)
			}
			result, err := session.CallTool(ctx, &CallToolParams{Name: tool, UseTasks: declares})
			if err != nil {
				t.Fatalf("%s (UseTasks %v): %v", tool, declares, err)
			}
			if task, ok := result.Task(); ok {
				t.Errorf("%s (UseTasks %v): caller got a task handle %+v", tool, declares, task)
			}
		}
	}
}
