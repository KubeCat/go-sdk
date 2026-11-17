// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// exchange sends one request and returns its answer.
func (p *rawTaskPeer) exchange(method, params string) rawTaskMessage {
	p.t.Helper()
	p.request(method, params)
	return decodeRawTaskMessage(p.t, p.read("the answer to "+method))
}

// The missing-capability error names the extension, and only the extension,
// as what the request has to declare, wherever it comes from.
func TestTaskMissingCapabilityData(t *testing.T) {
	const want = `{"requiredCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}`
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, &TaskServerOptions{RequireTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
			return &CallToolResult{}, nil
		})
	peer := newRawTaskPeer(t, server)
	for _, test := range []struct{ method, params string }{
		{methodCallTool, `"name":"work","arguments":{}`},
		{methodGetTask, `"taskId":"task-1"`},
		{methodUpdateTask, `"taskId":"task-1","inputResponses":{}`},
		{methodCancelTask, `"taskId":"task-1"`},
		{methodSubscriptionsListen, `"notifications":{"taskIds":["task-1"]}`},
		{methodSubscriptionsListen, `"notifications":{"taskIds":7}`},
	} {
		answer := peer.exchange(test.method, "{"+test.params+","+rawTaskMeta+"}")
		if answer.Error == nil || answer.Error.Code != CodeMissingRequiredClientCapabilities || string(answer.Error.Data) != want {
			t.Errorf("%s {%s} without the declaration: %+v; want error -32021 with data %s", test.method, test.params, answer.Error, want)
		}
	}
}

// A failed task reports the error the call would have been answered with had
// it not run as a task.
func TestTaskFailedErrorIsTheSynchronousError(t *testing.T) {
	failures := map[string]error{
		"plain":   errors.New("the disk is full"),
		"wrapped": fmt.Errorf("while fetching: %w", &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "rate limited", Data: json.RawMessage(`{"retry":3}`)}),
		"bare":    &jsonrpc.Error{Code: -32001, Message: "upstream refused", Data: json.RawMessage(`{"retry":3}`)},
	}
	server := NewServer(testImpl, nil)
	store := newObservedTaskStore()
	tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	for name, failure := range failures {
		tasks.AddTool(&Tool{Name: name, InputSchema: json.RawMessage(`{"type":"object"}`)},
			func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) { return nil, failure })
	}
	peer := newRawTaskPeer(t, server)
	for name := range failures {
		params := fmt.Sprintf(`"name":%q,"arguments":{}`, name)
		synchronous := peer.exchange(methodCallTool, "{"+params+","+rawTaskMeta+"}")
		if synchronous.Error == nil {
			t.Fatalf("%s: the synchronous call answered %+v, want an error", name, synchronous)
		}
		created := peer.exchange(methodCallTool, "{"+params+","+rawTaskMetaDeclaring+"}")
		var task Task
		if err := json.Unmarshal(created.Result, &task); err != nil || task.TaskID == "" {
			t.Fatalf("%s: the declaring call answered %+v, want a task", name, created)
		}
		waitTask(t, store.done, "the task to fail")
		got := peer.exchange(methodGetTask, fmt.Sprintf(`{"taskId":%q,%s}`, task.TaskID, rawTaskMetaDeclaring))
		var failed struct {
			Status        TaskStatus      `json:"status"`
			StatusMessage string          `json:"statusMessage"`
			Error         json.RawMessage `json:"error"`
			Result        json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(got.Result, &failed); err != nil {
			t.Fatal(err)
		}
		if failed.Status != TaskStatusFailed || failed.StatusMessage == "" || failed.Result != nil {
			t.Errorf("%s: tasks/get = %s, want a failed task with a status message and no result", name, got.Result)
		}
		var taskError, wireError any
		if err := json.Unmarshal(failed.Error, &taskError); err != nil {
			t.Fatalf("%s: tasks/get = %s: %v", name, got.Result, err)
		}
		data, err := json.Marshal(synchronous.Error)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &wireError); err != nil {
			t.Fatal(err)
		}
		if wire := wireError.(map[string]any); wire["data"] == nil {
			delete(wire, "data") // absent on the wire
		}
		if !reflect.DeepEqual(taskError, wireError) {
			t.Errorf("%s: the failed task's error is %s, the synchronous call's is %s", name, failed.Error, data)
		}
	}
}

// A handler that returns input requests starts a multi round-trip exchange
// when the call is not a task. As a task it cannot: the task fails, and says
// what to use instead.
func TestTaskHandlerReturningInputRequests(t *testing.T) {
	server := NewServer(testImpl, nil)
	store := newObservedTaskStore()
	tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	tasks.AddTool(&Tool{Name: "ask", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(_ context.Context, req *CallToolRequest, _ *TaskContext) (*CallToolResult, error) {
			if len(req.Params.InputResponses) == 0 {
				return &CallToolResult{InputRequests: elicitInput("question")}, nil
			}
			return &CallToolResult{Content: []Content{&TextContent{Text: "answered"}}}, nil
		})
	// A result with only a request state asks the client to continue too.
	tasks.AddTool(&Tool{Name: "state", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
			return &CallToolResult{RequestState: "round-1"}, nil
		})
	peer := newRawTaskPeer(t, server)
	const params = `"name":"ask","arguments":{}`
	const elicitation = `"io.modelcontextprotocol/clientCapabilities":{"elicitation":{}`

	plain := peer.exchange(methodCallTool, "{"+params+","+strings.Replace(rawTaskMeta, `"io.modelcontextprotocol/clientCapabilities":{`, elicitation, 1)+"}")
	if !strings.Contains(string(plain.Result), `"resultType":"input_required"`) || !strings.Contains(string(plain.Result), `"question"`) {
		t.Fatalf("the call that is not a task answered %s %+v, want input_required with the request", plain.Result, plain.Error)
	}

	declaring := strings.Replace(rawTaskMetaDeclaring, `"io.modelcontextprotocol/clientCapabilities":{`, elicitation+",", 1)
	created := peer.exchange(methodCallTool, "{"+params+","+declaring+"}")
	var task Task
	if err := json.Unmarshal(created.Result, &task); err != nil || task.TaskID == "" {
		t.Fatalf("the declaring call answered %s %+v, want a task", created.Result, created.Error)
	}
	waitTask(t, store.done, "the task to end")
	got := peer.exchange(methodGetTask, fmt.Sprintf(`{"taskId":%q,%s}`, task.TaskID, rawTaskMetaDeclaring))
	var ended DetailedTask
	if err := json.Unmarshal(got.Result, &ended); err != nil {
		t.Fatal(err)
	}
	if ended.Status != TaskStatusFailed || ended.Error == nil || ended.Error.Code != jsonrpc.CodeInternalError ||
		!strings.Contains(ended.Error.Message, "RequireInput") || ended.Result != nil || len(ended.InputRequests) != 0 {
		t.Errorf("tasks/get = %s, want a failed task whose -32603 error names RequireInput, with no result and no input request", got.Result)
	}

	const stateParams = `"name":"state","arguments":{}`
	plain = peer.exchange(methodCallTool, "{"+stateParams+","+rawTaskMeta+"}")
	if !strings.Contains(string(plain.Result), `"resultType":"input_required"`) || !strings.Contains(string(plain.Result), `"requestState":"round-1"`) {
		t.Fatalf("the call that is not a task answered %s %+v, want input_required with the request state", plain.Result, plain.Error)
	}
	created = peer.exchange(methodCallTool, "{"+stateParams+","+rawTaskMetaDeclaring+"}")
	if err := json.Unmarshal(created.Result, &task); err != nil || task.TaskID == "" {
		t.Fatalf("the declaring call answered %s %+v, want a task", created.Result, created.Error)
	}
	waitTask(t, store.done, "the task to end")
	got = peer.exchange(methodGetTask, fmt.Sprintf(`{"taskId":%q,%s}`, task.TaskID, rawTaskMetaDeclaring))
	ended = DetailedTask{}
	if err := json.Unmarshal(got.Result, &ended); err != nil {
		t.Fatal(err)
	}
	if ended.Status != TaskStatusFailed || ended.Error == nil || !strings.Contains(ended.Error.Message, "RequireInput") || ended.Result != nil {
		t.Errorf("tasks/get = %s, want a failed task whose error names RequireInput, with no result", got.Result)
	}
}

// legacyTaskPeer connects a raw peer and initializes a 2025-11-25 session
// that declares the extension, as far as that version can.
func legacyTaskPeer(t *testing.T, server *Server) *rawTaskPeer {
	t.Helper()
	peer := newRawTaskPeer(t, server)
	initialized := peer.exchange("initialize", `{"protocolVersion":"2025-11-25","clientInfo":{"name":"legacy-peer","version":"v1"},"capabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}`)
	if initialized.Error != nil || !strings.Contains(string(initialized.Result), `"protocolVersion":"2025-11-25"`) {
		t.Fatalf("initialize answered %s %+v", initialized.Result, initialized.Error)
	}
	if err := peer.conn.Write(peer.ctx, &jsonrpc.Request{Method: "notifications/initialized", Params: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	return peer
}

// On a session of a protocol version that has no tasks extension, the three
// task methods are answered as methods that version does not have: exactly
// as a server without the extension answers them.
func TestTaskMethodsOnLegacySession(t *testing.T) {
	with := NewServer(testImpl, nil)
	if _, err := NewTaskServer(with, nil); err != nil {
		t.Fatal(err)
	}
	withPeer, withoutPeer := legacyTaskPeer(t, with), legacyTaskPeer(t, NewServer(testImpl, nil))
	const declaring = `"_meta":{"io.modelcontextprotocol/clientCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}`
	for _, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
		for _, params := range []string{`{"taskId":"task-1"}`, `{"taskId":"task-1",` + declaring + `}`, `{"taskId":7}`, `{}`} {
			withPeer.request(method, params)
			withoutPeer.request(method, params)
			got, want := withPeer.read("the answer with the extension"), withoutPeer.read("the answer without it")
			if got != want || !strings.Contains(got, `"code":-32601`) {
				t.Errorf("%s %s on a 2025-11-25 session:\n with the extension: %s\n without it:         %s\nwant the same method-not-found answer", method, params, got, want)
			}
		}
	}
}

func TestNewTaskServerRejectsShortDurations(t *testing.T) {
	for _, test := range []struct {
		name    string
		options TaskServerOptions
		wantErr string
	}{
		{"retention under a millisecond", TaskServerOptions{Retention: 500 * time.Microsecond}, "Retention"},
		{"retention of a nanosecond", TaskServerOptions{Retention: time.Nanosecond}, "Retention"},
		{"poll interval under a millisecond", TaskServerOptions{DefaultPollInterval: 999 * time.Microsecond}, "DefaultPollInterval"},
		{"negative retention", TaskServerOptions{Retention: -time.Second}, "negative"},
		{"a millisecond each", TaskServerOptions{Retention: time.Millisecond, DefaultPollInterval: time.Millisecond}, ""},
		{"unset", TaskServerOptions{}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(testImpl, nil)
			tasks, err := NewTaskServer(server, &test.options)
			if test.wantErr == "" {
				if err != nil || tasks == nil {
					t.Fatalf("NewTaskServer = %v, %v", tasks, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("NewTaskServer = %v, want an error naming %s", err, test.wantErr)
			}
			// A refused install leaves the server without the extension.
			if server.tasks.Load() != nil {
				t.Error("the refused TaskServer is installed")
			}
			if _, err := NewTaskServer(server, nil); err != nil {
				t.Errorf("NewTaskServer after a refused one = %v", err)
			}
		})
	}
}
