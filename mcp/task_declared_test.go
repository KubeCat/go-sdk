// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// scriptTaskAnswers makes server answer the named methods with the given
// bytes, whatever the request is.
func scriptTaskAnswers(server *Server, answers map[string]string) {
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if body, ok := answers[method]; ok {
				return &scriptedResult{body: body}, nil
			}
			return next(ctx, method, req)
		}
	})
}

// Only tools/call can be answered with a task. A client that uses the
// extension takes a task in answer to any other request of the protocol as an
// invalid response: the call returns an error. What makes a request one of a
// client that uses the extension is the program's own declaration, in the
// client's capabilities, or that it is a request of the extension.
//
// A request of a client that does not declare the extension is decoded as it
// is without the extension, whatever the server answers and whatever client
// capabilities the request's _meta carries. (The same, for a relay that hands
// its caller's _meta on, is pinned against the unchanged SDK's transcripts by
// TestCompatNonUserRelayMeta; a session of an earlier protocol version by
// TestCompatNonUserClientDecoding.)
func TestTaskAnswerToRequestThatCannotReturnOne(t *testing.T) {
	const (
		task = `{"resultType":"task","taskId":"task-1","status":"working","createdAt":"2026-07-28T10:30:00Z","lastUpdatedAt":"2026-07-28T10:30:00Z","ttlMs":null}`
		done = `{"resultType":"task","taskId":"task-1","status":"completed","createdAt":"2026-07-28T10:30:00Z","lastUpdatedAt":"2026-07-28T10:30:00Z","ttlMs":null,"result":{"content":[]}}`
	)
	schema := json.RawMessage(`{"type":"object"}`)
	declaring := &ClientOptions{
		Capabilities: &ClientCapabilities{Extensions: map[string]any{ExtensionTasks: map[string]any{}}},
	}
	declaredMeta := func() Meta {
		return Meta{MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{ExtensionTasks: map[string]any{}}}}
	}
	newServer := func(answers map[string]string) *Server {
		server := NewServer(testImpl, nil)
		server.AddTool(&Tool{Name: "any", InputSchema: schema}, func(context.Context, *CallToolRequest) (*CallToolResult, error) {
			return &CallToolResult{}, nil
		})
		server.AddPrompt(&Prompt{Name: "any"}, func(context.Context, *GetPromptRequest) (*GetPromptResult, error) {
			return &GetPromptResult{}, nil
		})
		scriptTaskAnswers(server, answers)
		return server
	}
	newTaskServer := func(answers map[string]string) *Server {
		server := newServer(answers)
		if _, err := NewTaskServer(server, nil); err != nil {
			t.Fatal(err)
		}
		return server
	}
	invalid := func(t *testing.T, what string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "invalid response") {
			t.Fatalf("%s answered with a task: error %v, want an invalid-response error", what, err)
		}
	}

	t.Run("tools/list, declared in the client's capabilities", func(t *testing.T) {
		session := connectTaskMemory(t, newServer(map[string]string{methodListTools: task}), NewClient(testImpl, declaring))
		_, err := session.ListTools(taskTestContext(t), nil)
		invalid(t, "tools/list", err)
	})
	t.Run("prompts/get, declared in the client's capabilities", func(t *testing.T) {
		session := connectTaskMemory(t, newServer(map[string]string{methodGetPrompt: task}), NewClient(testImpl, declaring))
		_, err := session.GetPrompt(taskTestContext(t), &GetPromptParams{Name: "any"})
		invalid(t, "prompts/get", err)
	})
	t.Run("prompts/get, declared only in the request's _meta", func(t *testing.T) {
		// The program did not name the extension. The client capabilities
		// in the _meta may be another program's, handed on by a relay: the
		// answer is decoded as it is without the extension.
		session := connectTaskMemory(t, newServer(map[string]string{methodGetPrompt: task}), NewClient(testImpl, nil))
		result, err := session.GetPrompt(taskTestContext(t), &GetPromptParams{Name: "any", Meta: declaredMeta()})
		if err != nil {
			t.Fatalf("prompts/get of a client that does not declare the extension: %v", err)
		}
		if data := marshalForTaskTest(t, result); data != `{"messages":null,"resultType":"task"}` {
			t.Fatalf("prompts/get of a client that does not declare the extension = %s", data)
		}
	})
	t.Run("prompts/get, declared in the client's capabilities, not in the request's own", func(t *testing.T) {
		// The caller gave this request client capabilities of its own,
		// which leave the extension out: the request does not declare it.
		session := connectTaskMemory(t, newServer(map[string]string{methodGetPrompt: task}), NewClient(testImpl, declaring))
		meta := Meta{MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{"example.test/other": map[string]any{}}}}
		result, err := session.GetPrompt(taskTestContext(t), &GetPromptParams{Name: "any", Meta: meta})
		if err != nil {
			t.Fatalf("prompts/get that does not declare the extension: %v", err)
		}
		if data := marshalForTaskTest(t, result); data != `{"messages":null,"resultType":"task"}` {
			t.Fatalf("prompts/get that does not declare the extension = %s", data)
		}
	})
	t.Run("a method of the program's own, declared in the client's capabilities", func(t *testing.T) {
		// The program defined this request and its result: the answer is
		// its to read, as it is without the extension.
		const method = "example/echo"
		server := newServer(map[string]string{method: task})
		if err := AddReceivingCustomMethod(server, method, func(context.Context, *ServerSession, *PingParams) (*emptyResult, error) {
			return &emptyResult{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		client := NewClient(testImpl, declaring)
		if err := AddSendingCustomMethod[*PingParams, *ListToolsResult](client, method); err != nil {
			t.Fatal(err)
		}
		session := connectTaskMemory(t, server, client)
		result, err := CallCustomMethod[*PingParams, *ListToolsResult](taskTestContext(t), session, method, &PingParams{})
		if err != nil || result.ResultType != resultTypeTask {
			t.Fatalf("%s = %+v, %v; want the answer as the program's result type", method, result, err)
		}
	})
	t.Run("an ordinary answer, declared in the client's capabilities", func(t *testing.T) {
		session := connectTaskMemory(t, newServer(nil), NewClient(testImpl, declaring))
		result, err := session.ListTools(taskTestContext(t), nil)
		if err != nil || len(result.Tools) != 1 {
			t.Fatalf("tools/list = %+v, %v; want the server's tool", result, err)
		}
	})
	t.Run("tools/list, not declared", func(t *testing.T) {
		session := connectTaskMemory(t, newServer(map[string]string{methodListTools: task}), NewClient(testImpl, nil))
		result, err := session.ListTools(taskTestContext(t), nil)
		if err != nil {
			t.Fatalf("tools/list that does not declare the extension: %v", err)
		}
		if result.ResultType != resultTypeTask || len(result.Tools) != 0 {
			t.Fatalf("tools/list that does not declare the extension = %s", marshalForTaskTest(t, result))
		}
	})
	t.Run("tasks/get", func(t *testing.T) {
		session := connectTaskMemory(t, newTaskServer(map[string]string{methodGetTask: task}), NewClient(testImpl, nil))
		_, err := session.GetTask(taskTestContext(t), &GetTaskParams{TaskID: "task-1"})
		invalid(t, "tasks/get", err)
	})
	t.Run("tasks/update and tasks/cancel", func(t *testing.T) {
		session := connectTaskMemory(t, newTaskServer(map[string]string{methodUpdateTask: task, methodCancelTask: task}), NewClient(testImpl, nil))
		ctx := taskTestContext(t)
		invalid(t, "tasks/update", session.UpdateTask(ctx, &UpdateTaskParams{TaskID: "task-1"}))
		invalid(t, "tasks/cancel", session.CancelTask(ctx, &CancelTaskParams{TaskID: "task-1"}))
	})
	t.Run("FollowTask", func(t *testing.T) {
		session := connectTaskMemory(t, newTaskServer(map[string]string{methodGetTask: done}), NewClient(testImpl, nil))
		_, err := session.FollowTask(taskTestContext(t), "task-1", nil)
		invalid(t, "FollowTask's tasks/get", err)
	})
}

// The answers the extension reads are decoded with the decoder every other
// result is decoded with, which matches field names by their exact case: a
// key in another case is not the field. (For the standard requests of a
// client that declares the extension, the same is pinned against the unchanged SDK's
// transcripts by TestCompatNonUserClientDecoding.)
func TestTaskAnswersAreDecodedCaseSensitively(t *testing.T) {
	schema := json.RawMessage(`{"type":"object"}`)
	newServer := func(answers map[string]string) *Server {
		server := NewServer(testImpl, nil)
		server.AddTool(&Tool{Name: "any", InputSchema: schema}, func(context.Context, *CallToolRequest) (*CallToolResult, error) {
			return &CallToolResult{}, nil
		})
		scriptTaskAnswers(server, answers)
		if _, err := NewTaskServer(server, nil); err != nil {
			t.Fatal(err)
		}
		return server
	}
	t.Run("tasks/get, task fields in another case", func(t *testing.T) {
		const answer = `{"TASKID":"task-1","STATUS":"completed","StatusMessage":"done","RESULT":{"content":[]}}`
		session := connectTaskMemory(t, newServer(map[string]string{methodGetTask: answer}), NewClient(testImpl, nil))
		task, err := session.GetTask(taskTestContext(t), &GetTaskParams{TaskID: "task-1"})
		if err != nil {
			t.Fatalf("tasks/get: %v", err)
		}
		if task.TaskID != "" || task.Status != "" || task.StatusMessage != "" || task.Result != nil {
			t.Fatalf("tasks/get read keys in another case as the task's fields: %s", marshalForTaskTest(t, task))
		}
	})
	t.Run("tasks/get, resultType in another case", func(t *testing.T) {
		// Not a task as the answer: ResultType is not resultType.
		const answer = `{"ResultType":"task","taskId":"task-1","status":"working","createdAt":"2026-07-28T10:30:00Z","lastUpdatedAt":"2026-07-28T10:30:00Z","ttlMs":null}`
		session := connectTaskMemory(t, newServer(map[string]string{methodGetTask: answer}), NewClient(testImpl, nil))
		task, err := session.GetTask(taskTestContext(t), &GetTaskParams{TaskID: "task-1"})
		if err != nil {
			t.Fatalf("tasks/get: %v", err)
		}
		if task.TaskID != "task-1" || task.Status != TaskStatusWorking {
			t.Fatalf("tasks/get = %s", marshalForTaskTest(t, task))
		}
	})
	t.Run("tools/call, task fields in another case", func(t *testing.T) {
		const answer = `{"resultType":"task","TASKID":"task-1","STATUS":"working","createdAt":"2026-07-28T10:30:00Z","lastUpdatedAt":"2026-07-28T10:30:00Z","ttlMs":null}`
		session := connectTaskMemory(t, newServer(map[string]string{methodCallTool: answer}), NewClient(testImpl, nil))
		result, err := session.CallTool(taskTestContext(t), &CallToolParams{Name: "any", UseTasks: true})
		if err == nil || !strings.Contains(err.Error(), "invalid task result") {
			task, _ := result.Task()
			t.Fatalf("tools/call answered with a task without taskId and status: task %+v, error %v; want an invalid-task-result error", task, err)
		}
	})
}
