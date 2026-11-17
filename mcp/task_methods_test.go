// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func getTaskForTest(t *testing.T, client *ClientSession, id string, meta Meta) (*DetailedTask, error) {
	t.Helper()
	return CallCustomMethod[*GetTaskParams, *DetailedTask](taskTestContext(t), client, methodGetTask, &GetTaskParams{
		TaskID:     id,
		ParamsBase: ParamsBase{Meta: meta},
	})
}

func updateTaskForTest(t *testing.T, client *ClientSession, params *UpdateTaskParams) (*taskAckResult, error) {
	t.Helper()
	return CallCustomMethod[*UpdateTaskParams, *taskAckResult](taskTestContext(t), client, methodUpdateTask, params)
}

func cancelTaskForTest(t *testing.T, client *ClientSession, id string) (*taskAckResult, error) {
	t.Helper()
	return CallCustomMethod[*CancelTaskParams, *taskAckResult](taskTestContext(t), client, methodCancelTask, &CancelTaskParams{
		TaskID:     id,
		ParamsBase: ParamsBase{Meta: tasksRequestMeta()},
	})
}

func TestTaskMethodsRequireCapabilityAndKnownID(t *testing.T) {
	server := NewServer(testImpl, nil)
	if _, err := NewTaskServer(server, nil); err != nil {
		t.Fatal(err)
	}
	client := connectTaskClient(t, server, nil)
	if _, err := getTaskForTest(t, client, "unknown", nil); errorCodeForTaskTest(err) != CodeMissingRequiredClientCapabilities {
		t.Fatalf("get without capability error = %v", err)
	}
	if _, err := updateTaskForTest(t, client, &UpdateTaskParams{
		TaskID:         "unknown",
		InputResponses: InputResponseMap{},
	}); errorCodeForTaskTest(err) != CodeMissingRequiredClientCapabilities {
		t.Fatalf("update without capability error = %v", err)
	}
	if _, err := CallCustomMethod[*CancelTaskParams, *taskAckResult](taskTestContext(t), client, methodCancelTask, &CancelTaskParams{
		TaskID: "unknown",
	}); errorCodeForTaskTest(err) != CodeMissingRequiredClientCapabilities {
		t.Fatalf("cancel without capability error = %v", err)
	}
	if _, err := getTaskForTest(t, client, "unknown", tasksRequestMeta()); errorCodeForTaskTest(err) != jsonrpc.CodeInvalidParams {
		t.Fatalf("get with unknown ID error = %v", err)
	}
	if _, err := updateTaskForTest(t, client, &UpdateTaskParams{
		TaskID:         "unknown",
		InputResponses: InputResponseMap{},
		ParamsBase:     ParamsBase{Meta: tasksRequestMeta()},
	}); errorCodeForTaskTest(err) != jsonrpc.CodeInvalidParams {
		t.Fatalf("update with unknown ID error = %v", err)
	}
	if _, err := cancelTaskForTest(t, client, "unknown"); errorCodeForTaskTest(err) != jsonrpc.CodeInvalidParams {
		t.Fatalf("cancel with unknown ID error = %v", err)
	}
}

func errorCodeForTaskTest(err error) int64 {
	var wireErr *jsonrpc.Error
	if errors.As(err, &wireErr) {
		return wireErr.Code
	}
	return 0
}

func TestTaskUpdateInputResponses(t *testing.T) {
	server := NewServer(testImpl, nil)
	store := newObservedTaskStore()
	tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	handlerContext, stopHandlers := taskTestStop()
	tasks.AddTool(&Tool{Name: "input", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
			ctx = handlerContext(ctx)
			responses, err := taskContext.RequireInput(ctx, InputRequestMap{
				"first":  &ElicitParams{Message: "first"},
				"second": &ElicitParams{Message: "second"},
			})
			if err != nil {
				return nil, err
			}
			if _, err := taskContext.RequireInput(ctx, InputRequestMap{
				"first": &ElicitParams{Message: "duplicate"},
			}); err == nil {
				t.Error("reusing an input request key succeeded")
			}
			first := responses["first"].(*ElicitResult).Content["first"]
			second := responses["second"].(*ElicitResult).Content["second"]
			return &CallToolResult{Content: []Content{&TextContent{Text: fmt.Sprint(first, second)}}}, nil
		})
	client := connectTaskClient(t, server, nil)
	t.Cleanup(stopHandlers)
	result, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "input", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task := taskHandle(t, result)
	snapshot := waitTask(t, store.snapshots, "input-required snapshot")
	if snapshot.Status != TaskStatusInputRequired || len(snapshot.InputRequests) != 2 {
		t.Fatalf("initial input snapshot = %+v", snapshot)
	}
	current, err := getTaskForTest(t, client, task.TaskID, tasksRequestMeta())
	if err != nil {
		t.Fatal(err)
	}
	if current.ResultType != resultTypeComplete || len(current.InputRequests) != 2 {
		t.Fatalf("tasks/get result = %+v", current)
	}
	ack, err := updateTaskForTest(t, client, &UpdateTaskParams{
		TaskID: task.TaskID,
		InputResponses: InputResponseMap{
			"first": &ElicitResult{Action: "accept", Content: map[string]any{"first": "A"}},
		},
		ParamsBase: ParamsBase{Meta: tasksRequestMeta()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.ResultType != resultTypeComplete {
		t.Fatalf("update resultType = %q", ack.ResultType)
	}
	snapshot = waitTask(t, store.snapshots, "partial input snapshot")
	if snapshot.Status != TaskStatusInputRequired || len(snapshot.InputRequests) != 1 || snapshot.InputRequests["second"] == nil {
		t.Fatalf("partial input snapshot = %+v", snapshot)
	}
	if _, err := updateTaskForTest(t, client, &UpdateTaskParams{
		TaskID: task.TaskID,
		InputResponses: InputResponseMap{
			"unknown": &ElicitResult{Action: "accept"},
		},
		ParamsBase: ParamsBase{Meta: tasksRequestMeta()},
	}); err != nil {
		t.Fatal(err)
	}
	current, err = getTaskForTest(t, client, task.TaskID, tasksRequestMeta())
	if err != nil {
		t.Fatal(err)
	}
	if len(current.InputRequests) != 1 || current.InputRequests["second"] == nil {
		t.Fatalf("unknown response changed outstanding requests: %+v", current.InputRequests)
	}
	if _, err := updateTaskForTest(t, client, &UpdateTaskParams{
		TaskID: task.TaskID,
		InputResponses: InputResponseMap{
			"second": &ElicitResult{Action: "accept", Content: map[string]any{"second": "B"}},
		},
		ParamsBase: ParamsBase{Meta: tasksRequestMeta()},
	}); err != nil {
		t.Fatal(err)
	}
	waitTask(t, store.done, "terminal task store update")
	current, err = getTaskForTest(t, client, task.TaskID, tasksRequestMeta())
	if err != nil {
		t.Fatal(err)
	}
	toolResult, err := current.ToolResult()
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResult.Content[0].(*TextContent).Text; got != "AB" {
		t.Fatalf("final result text = %q, want AB", got)
	}
}

func TestTaskCancel(t *testing.T) {
	server := NewServer(testImpl, nil)
	store := newObservedTaskStore()
	tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	stoppable, stopHandlers := taskTestStop()
	handlerContexts, handlerContext := taskEvents[context.Context](t, "handler contexts")
	tasks.AddTool(&Tool{Name: "wait", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
			ctx = stoppable(ctx)
			handlerContext(ctx)
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	client := connectTaskClient(t, server, nil)
	t.Cleanup(stopHandlers)
	result, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "wait", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task := taskHandle(t, result)
	waitTask(t, started, "task handler start")
	ack, err := cancelTaskForTest(t, client, task.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if ack.ResultType != resultTypeComplete {
		t.Fatalf("cancel resultType = %q", ack.ResultType)
	}
	handlerCtx := waitTask(t, handlerContexts, "task handler context")
	select {
	case <-handlerCtx.Done():
	default:
		t.Fatal("tasks/cancel returned without cancelling the handler context")
	}
	waitTask(t, store.done, "terminal task store update")
	current, err := getTaskForTest(t, client, task.TaskID, tasksRequestMeta())
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != TaskStatusCancelled {
		t.Fatalf("task status = %q, want cancelled", current.Status)
	}
}

func TestTaskMethodsEnforceScope(t *testing.T) {
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, &TaskServerOptions{
		Scope: func(_ context.Context, session *ServerSession) string {
			return fmt.Sprintf("%p", session)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	release, releaseTask := taskTestGate()
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
			<-release
			return &CallToolResult{Content: []Content{}}, nil
		})
	owner := connectTaskClient(t, server, nil)
	other := connectTaskClient(t, server, nil)
	t.Cleanup(releaseTask)
	result, err := owner.CallTool(taskTestContext(t), &CallToolParams{Name: "work", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task := taskHandle(t, result)
	if _, err := getTaskForTest(t, other, task.TaskID, tasksRequestMeta()); errorCodeForTaskTest(err) != jsonrpc.CodeInvalidParams {
		t.Fatalf("foreign task Get error = %v, want invalid params", err)
	}
	update := &UpdateTaskParams{TaskID: task.TaskID, ParamsBase: ParamsBase{Meta: tasksRequestMeta()}}
	if _, err := updateTaskForTest(t, other, update); errorCodeForTaskTest(err) != jsonrpc.CodeInvalidParams {
		t.Fatalf("foreign task Update error = %v, want invalid params", err)
	}
	if _, err := cancelTaskForTest(t, other, task.TaskID); errorCodeForTaskTest(err) != jsonrpc.CodeInvalidParams {
		t.Fatalf("foreign task Cancel error = %v, want invalid params", err)
	}
	current, err := getTaskForTest(t, owner, task.TaskID, tasksRequestMeta())
	if err != nil {
		t.Fatalf("owning session could not read task: %v", err)
	}
	if current.Status != TaskStatusWorking {
		t.Fatalf("after another scope's cancel the task is %s, want working", current.Status)
	}
	if _, err := updateTaskForTest(t, owner, update); err != nil {
		t.Fatalf("owning session could not update task: %v", err)
	}
	if _, err := cancelTaskForTest(t, owner, task.TaskID); err != nil {
		t.Fatalf("owning session could not cancel task: %v", err)
	}
	if current, err := getTaskForTest(t, owner, task.TaskID, tasksRequestMeta()); err != nil || current.Status != TaskStatusCancelled {
		t.Fatalf("after its owner's cancel the task is %+v, %v; want cancelled", current, err)
	}
	releaseTask()
}
