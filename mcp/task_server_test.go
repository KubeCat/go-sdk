// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func connectTaskClient(t *testing.T, server *Server, options *ClientSessionOptions) *ClientSession {
	t.Helper()
	return connectTaskClientWith(t, server, nil, options)
}

// connectTaskClientWith is connectTaskClient for a client with options.
func connectTaskClientWith(t *testing.T, server *Server, clientOptions *ClientOptions, options *ClientSessionOptions) *ClientSession {
	t.Helper()
	ctx := taskTestContext(t)
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	client := NewClient(&Implementation{Name: "task-test-client", Version: "v1.0.0"}, clientOptions)
	if err := AddSendingCustomMethod[*GetTaskParams, *DetailedTask](client, methodGetTask); err != nil {
		t.Fatal(err)
	}
	if err := AddSendingCustomMethod[*UpdateTaskParams, *taskAckResult](client, methodUpdateTask); err != nil {
		t.Fatal(err)
	}
	if err := AddSendingCustomMethod[*CancelTaskParams, *taskAckResult](client, methodCancelTask); err != nil {
		t.Fatal(err)
	}
	clientSession, err := client.Connect(ctx, clientTransport, options)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "clientSession", clientSession.Close)
	return clientSession
}

type observedTaskStore struct {
	TaskStore
	done      chan struct{}
	snapshots chan *DetailedTask
}

func newObservedTaskStore() *observedTaskStore {
	return &observedTaskStore{
		TaskStore: NewMemoryTaskStore(),
		done:      make(chan struct{}, 1),
		snapshots: make(chan *DetailedTask, 16),
	}
}

func (s *observedTaskStore) Update(ctx context.Context, scope, id string, update func(*DetailedTask) error) error {
	if err := s.TaskStore.Update(ctx, scope, id, update); err != nil {
		return err
	}
	task, err := s.TaskStore.Get(ctx, scope, id)
	if err == nil {
		select {
		case s.snapshots <- task:
		default:
		}
		if task.Status.Terminal() {
			select {
			case s.done <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

func tasksRequestMeta() Meta {
	return Meta{
		MetaKeyClientCapabilities: &ClientCapabilities{
			Extensions: map[string]any{ExtensionTasks: map[string]any{}},
		},
	}
}

func taskRuntimeForTest(t *testing.T, server *TaskServer, id string) *taskRuntime {
	t.Helper()
	server.mu.Lock()
	defer server.mu.Unlock()
	runtime := server.runtimes[id]
	if runtime == nil {
		t.Fatalf("no runtime for task %q", id)
	}
	return runtime
}

func TestTaskServerToolCallCreatesTask(t *testing.T) {
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release, releaseTask := taskTestGate()
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
			close(started)
			<-release
			return &CallToolResult{Content: []Content{&TextContent{Text: "done"}}}, nil
		})
	client := connectTaskClient(t, server, nil)
	t.Cleanup(releaseTask)
	result, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "work", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.Task()
	if !ok {
		t.Fatalf("opted-in call returned an ordinary result: %+v", result)
	}
	if task.TaskID == "" || task.Status != TaskStatusWorking {
		t.Fatalf("task handle = %+v", task)
	}
	stored, err := tasks.opts.Store.Get(taskTestContext(t), "", task.TaskID)
	if err != nil {
		t.Fatalf("task was not durable when its handle returned: %v", err)
	}
	if stored.Status != TaskStatusWorking {
		t.Fatalf("stored status = %q, want working", stored.Status)
	}
	runtime := taskRuntimeForTest(t, tasks, task.TaskID)
	waitTask(t, started, "task handler start")
	releaseTask()
	waitTask(t, runtime.done, "task runtime completion")
	stored, err = tasks.opts.Store.Get(taskTestContext(t), "", task.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != TaskStatusCompleted {
		t.Fatalf("stored status = %q, want completed", stored.Status)
	}
	toolResult, err := stored.ToolResult()
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResult.Content[0].(*TextContent).Text; got != "done" {
		t.Fatalf("final text = %q, want done", got)
	}
}

func TestTaskServerToolCallWithoutOptInIsSynchronous(t *testing.T) {
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
			if err := taskContext.SetStatusMessage("not a task"); err == nil {
				t.Error("SetStatusMessage succeeded without a task")
			}
			return &CallToolResult{Content: []Content{&TextContent{Text: "immediate"}}}, nil
		})
	client := connectTaskClient(t, server, nil)
	result, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if task, ok := result.Task(); ok || task != nil {
		t.Fatalf("non-opted-in client received task %+v", task)
	}
	if got := result.Content[0].(*TextContent).Text; got != "immediate" {
		t.Fatalf("result text = %q, want immediate", got)
	}
}

func TestTaskServerRequireTasks(t *testing.T) {
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, &TaskServerOptions{RequireTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
			t.Error("handler ran without the required capability")
			return nil, nil
		})
	client := connectTaskClient(t, server, nil)
	_, err = client.CallTool(taskTestContext(t), &CallToolParams{Name: "work"})
	var wireErr *jsonrpc.Error
	if !errors.As(err, &wireErr) || wireErr.Code != CodeMissingRequiredClientCapabilities {
		t.Fatalf("CallTool error = %v, want %d", err, CodeMissingRequiredClientCapabilities)
	}
	var data MissingRequiredClientCapabilityData
	if err := json.Unmarshal(wireErr.Data, &data); err != nil {
		t.Fatal(err)
	}
	if _, ok := data.RequiredCapabilities.Extensions[ExtensionTasks]; !ok {
		t.Fatalf("required capabilities = %+v", data.RequiredCapabilities)
	}
}

func TestTaskServerExecutionError(t *testing.T) {
	server := NewServer(testImpl, nil)
	store := newObservedTaskStore()
	tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	tasks.AddTool(&Tool{Name: "fail", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "execution failed"}
		})
	client := connectTaskClient(t, server, nil)
	result, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "fail", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task := taskHandle(t, result)
	waitTask(t, store.done, "terminal task store update")
	stored, err := tasks.opts.Store.Get(taskTestContext(t), "", task.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != TaskStatusFailed || stored.Error == nil || stored.Error.Code != jsonrpc.CodeInternalError {
		t.Fatalf("failed task = %+v", stored)
	}
}

func TestAddTaskTool(t *testing.T) {
	type input struct {
		Value string `json:"value"`
	}
	type output struct {
		Echo string `json:"echo"`
	}
	server := NewServer(testImpl, nil)
	store := newObservedTaskStore()
	tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	AddTaskTool(tasks, &Tool{Name: "echo"}, func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext, in input) (*CallToolResult, output, error) {
		return nil, output{Echo: in.Value}, nil
	})
	client := connectTaskClient(t, server, nil)
	result, err := client.CallTool(taskTestContext(t), &CallToolParams{
		Name:      "echo",
		Arguments: input{Value: "hello"},
		UseTasks:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	task := taskHandle(t, result)
	waitTask(t, store.done, "terminal task store update")
	stored, err := tasks.opts.Store.Get(taskTestContext(t), "", task.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	toolResult, err := stored.ToolResult()
	if err != nil {
		t.Fatal(err)
	}
	structured, ok := toolResult.StructuredContent.(map[string]any)
	if !ok || structured["echo"] != "hello" {
		t.Fatalf("structured content = %#v", toolResult.StructuredContent)
	}
}

func TestTaskServerLegacyProtocolIgnoresExtension(t *testing.T) {
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
			return &CallToolResult{Content: []Content{&TextContent{Text: "immediate"}}}, nil
		})
	client := connectTaskClient(t, server, &ClientSessionOptions{ProtocolVersion: protocolVersion20251125})
	result, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "work", Meta: tasksRequestMeta()})
	if err != nil {
		t.Fatal(err)
	}
	if task, ok := result.Task(); ok || task != nil {
		t.Fatalf("legacy client received task %+v", task)
	}
	// The handler ran in the request: the answer is its result, not a task.
	if len(result.Content) != 1 || result.Content[0].(*TextContent).Text != "immediate" {
		t.Fatalf("legacy call answered %s, want the handler's result", marshalForTaskTest(t, result))
	}
}

func TestTaskServerResultMarshalError(t *testing.T) {
	server := NewServer(testImpl, nil)
	store := newObservedTaskStore()
	tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	tasks.AddTool(&Tool{Name: "bad-result", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
			return &CallToolResult{StructuredContent: make(chan int)}, nil
		})
	client := connectTaskClient(t, server, nil)
	result, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "bad-result", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task := taskHandle(t, result)
	waitTask(t, store.done, "terminal task store update")
	stored, err := tasks.opts.Store.Get(taskTestContext(t), "", task.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != TaskStatusFailed || stored.Error == nil {
		t.Fatalf("task after result marshal error = %+v", stored)
	}
}
