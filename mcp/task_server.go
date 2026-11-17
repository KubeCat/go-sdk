// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// TaskToolHandler handles a tool call that may execute as a task.
type TaskToolHandler func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error)

// TaskToolHandlerFor handles a typed tool call that may execute as a task.
type TaskToolHandlerFor[In, Out any] func(context.Context, *CallToolRequest, *TaskContext, In) (*CallToolResult, Out, error)

type taskRuntime struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu            sync.Mutex
	pending       InputRequestMap
	responses     InputResponseMap
	usedInputKeys map[string]struct{}
	changed       chan struct{}
}

// TaskContext provides operations on the currently executing task.
type TaskContext struct {
	server  *TaskServer
	scope   string
	taskID  string
	runtime *taskRuntime
}

// SetStatusMessage updates the task's status message.
func (c *TaskContext) SetStatusMessage(message string) error {
	if c.server == nil {
		return errors.New("synchronous tool call has no task")
	}
	return c.server.updateTask(context.Background(), c.scope, c.taskID, func(task *DetailedTask) error {
		task.StatusMessage = message
		return nil
	})
}

// AddTool adds a task-capable tool to the server.
//
// A call whose request declares the tasks extension is answered with a task
// and the handler runs in the background; any other call runs the handler
// synchronously, with a TaskContext that has no task.
//
// A handler that needs input from the client while it runs as a task must
// call [TaskContext.RequireInput]. Returning a result with InputRequests or
// a RequestState works only for a call that does not run as a task, where it
// starts a multi round-trip exchange; a task whose handler returns one fails.
//
// The task is created before the handler runs, so a handler cannot gather
// input with a multi round-trip exchange first and become a task afterwards.
// The tasks extension recommends that order for an implementation that uses
// both; this one asks for input as part of the task instead.
func (s *TaskServer) AddTool(tool *Tool, handler TaskToolHandler) {
	s.server.AddTool(tool, func(ctx context.Context, req *CallToolRequest) (*CallToolResult, error) {
		taskContext := &TaskContext{}
		if !requestUsesTasks(req) {
			if s.opts.RequireTasks {
				return nil, missingTasksCapabilityError()
			}
			return handler(ctx, req, taskContext)
		}
		return s.startTask(ctx, req, handler)
	})
}

// AddTaskTool adds a typed task-capable tool to the server. See
// [TaskServer.AddTool] for how the handler runs and how it asks for input.
func AddTaskTool[In, Out any](s *TaskServer, tool *Tool, handler TaskToolHandlerFor[In, Out]) {
	adaptedTool, adapted, err := toolForErr(tool, func(ctx context.Context, req *CallToolRequest, input In) (*CallToolResult, Out, error) {
		taskContext, _ := ctx.Value(taskContextKey{}).(*TaskContext)
		return handler(ctx, req, taskContext, input)
	}, s.server.opts.SchemaCache)
	if err != nil {
		panic(fmt.Sprintf("AddTaskTool: tool %q: %v", tool.Name, err))
	}
	s.AddTool(adaptedTool, func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
		return adapted(context.WithValue(ctx, taskContextKey{}, taskContext), req)
	})
}

type taskContextKey struct{}

func requestUsesTasks[P Params](req *ServerRequest[P]) bool {
	return req.ProtocolVersion() >= protocolVersion20260728 && metaUsesTasks(getRequestMeta(req))
}

// metaUsesTasks reports whether the client capabilities of a request's _meta
// name the extension. The value can be of any type that encodes as client
// capabilities, so in general it is encoded and decoded to find out.
func metaUsesTasks(meta Meta) bool {
	if capabilitiesOmitTasks(meta[MetaKeyClientCapabilities]) {
		return false
	}
	caps, ok := decodeMetaValue[*clientCapabilitiesV2](meta, MetaKeyClientCapabilities)
	if !ok || caps == nil {
		return false
	}
	_, ok = caps.Extensions[ExtensionTasks]
	return ok
}

// capabilitiesOmitTasks reports whether v, the client capabilities value of
// a request's _meta, is of a shape that shows as it is that it does not name
// the extension: a decoded JSON object or the capabilities type, without the
// extension's key. Such a value is not encoded to be looked at, so a request
// that does not name the extension encodes its values as often as it does
// without the extension. False says nothing about v.
func capabilitiesOmitTasks(v any) bool {
	var extensions map[string]any
	switch caps := v.(type) {
	case map[string]any:
		switch ext := caps["extensions"].(type) {
		case nil:
		case map[string]any:
			extensions = ext
		default:
			return false
		}
	case ClientCapabilities:
		extensions = caps.Extensions
	case *ClientCapabilities:
		if caps != nil {
			extensions = caps.Extensions
		}
	default:
		return false
	}
	_, named := extensions[ExtensionTasks]
	return !named
}

// missingTasksCapabilityData is the data of the missing-capability error: the
// extension, and nothing else, is what the request has to declare.
const missingTasksCapabilityData = `{"requiredCapabilities":{"extensions":{"` + ExtensionTasks + `":{}}}}`

func missingTasksCapabilityError() *jsonrpc.Error {
	return &jsonrpc.Error{
		Code:    CodeMissingRequiredClientCapabilities,
		Message: "missing required tasks extension capability",
		Data:    json.RawMessage(missingTasksCapabilityData),
	}
}

func (s *TaskServer) scope(ctx context.Context, session *ServerSession) string {
	if s.opts.Scope != nil {
		return s.opts.Scope(ctx, session)
	}
	return ""
}

func (s *TaskServer) startTask(ctx context.Context, req *CallToolRequest, handler TaskToolHandler) (*CallToolResult, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	detail := &DetailedTask{Task: Task{
		TaskID:        rand.Text(),
		Status:        TaskStatusWorking,
		CreatedAt:     now,
		LastUpdatedAt: now,
	}}
	detail.ResultType = resultTypeComplete
	if s.opts.Retention > 0 {
		ttl := s.opts.Retention.Milliseconds()
		detail.TTLMs = &ttl
	}
	if s.opts.DefaultPollInterval > 0 {
		interval := s.opts.DefaultPollInterval.Milliseconds()
		detail.PollIntervalMs = &interval
	}
	scope := s.scope(ctx, req.Session)
	if err := s.opts.Store.Create(ctx, scope, detail); err != nil {
		return nil, fmt.Errorf("creating task: %w", err)
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	runtime := &taskRuntime{
		cancel:        cancel,
		done:          make(chan struct{}),
		usedInputKeys: make(map[string]struct{}),
		changed:       make(chan struct{}),
	}
	s.mu.Lock()
	s.runtimes[detail.TaskID] = runtime
	s.mu.Unlock()
	taskContext := &TaskContext{server: s, scope: scope, taskID: detail.TaskID, runtime: runtime}
	go s.runTask(runCtx, req, handler, taskContext, runtime)
	task := detail.Task
	return &CallToolResult{task: &task, taskCreated: true}, nil
}

func (s *TaskServer) runTask(ctx context.Context, req *CallToolRequest, handler TaskToolHandler, taskContext *TaskContext, runtime *taskRuntime) {
	defer func() {
		s.mu.Lock()
		delete(s.runtimes, taskContext.taskID)
		s.mu.Unlock()
		close(runtime.done)
	}()
	result, err := handler(ctx, req, taskContext)
	if err == nil && result != nil && asksForInput(result) {
		// The result asks the client to continue (input requests or a
		// request state, as for a call that is not a task). The call was
		// answered with the task long ago: there is no request left to
		// answer with input_required, and a completed task must not carry
		// an unanswered request.
		err = &jsonrpc.Error{
			Code:    jsonrpc.CodeInternalError,
			Message: "task tool handler returned input requests; use TaskContext.RequireInput",
		}
	}
	if err != nil {
		s.finishTask(taskContext, nil, err)
		return
	}
	if result == nil {
		result = &CallToolResult{}
	}
	if result.Content == nil {
		copy := *result
		copy.Content = []Content{}
		result = &copy
	}
	result.setResultType(resultTypeComplete)
	s.finishTask(taskContext, result, nil)
}

func (s *TaskServer) finishTask(taskContext *TaskContext, result *CallToolResult, executionErr error) {
	var data []byte
	if executionErr == nil {
		var err error
		data, err = json.Marshal(result)
		if err != nil {
			executionErr = fmt.Errorf("marshaling task result: %w", err)
		}
	}
	err := s.updateTask(context.Background(), taskContext.scope, taskContext.taskID, func(task *DetailedTask) error {
		if executionErr != nil {
			task.Status = TaskStatusFailed
			task.StatusMessage = executionErr.Error()
			task.Error = taskExecutionError(executionErr)
			task.Result = nil
			task.InputRequests = nil
			return nil
		}
		task.Status = TaskStatusCompleted
		task.Result = data
		task.Error = nil
		task.InputRequests = nil
		return nil
	})
	if err != nil && !errors.Is(err, errTaskTerminal) {
		s.server.opts.Logger.Error("finishing task", "task_id", taskContext.taskID, "error", err)
	}
}

// taskExecutionError returns the JSON-RPC error a failed task reports: the
// error the request would have been answered with had it not run as a task.
// It applies the rules the response path applies to a handler's error. An
// error that carries no JSON-RPC code is an internal error
// ([withInternalErrorCode]). Then, as the unexported toWireError of
// internal/jsonrpc2 has it, a JSON-RPC error is used as it is; any other
// error gives its own message, with the code of the JSON-RPC error it wraps,
// and no data.
func taskExecutionError(err error) *jsonrpc.Error {
	err = withInternalErrorCode(err)
	if wireErr, ok := err.(*jsonrpc.Error); ok {
		return wireErr
	}
	result := &jsonrpc.Error{Message: err.Error()}
	var wrapped *jsonrpc.Error
	if errors.As(err, &wrapped) {
		result.Code = wrapped.Code
	}
	return result
}

var errTaskTerminal = errors.New("task is terminal")

func (s *TaskServer) updateTask(ctx context.Context, scope, id string, update func(*DetailedTask) error) error {
	err := s.opts.Store.Update(ctx, scope, id, func(task *DetailedTask) error {
		if task.Status.Terminal() {
			return errTaskTerminal
		}
		if err := update(task); err != nil {
			return err
		}
		task.LastUpdatedAt = nextTaskTimestamp(task.LastUpdatedAt, time.Now())
		task.ResultType = resultTypeComplete
		return nil
	})
	if errors.Is(err, errTaskUnchanged) {
		return nil
	}
	if err == nil {
		s.notifyTask(scope, id)
	}
	return err
}

// nextTaskTimestamp returns the lastUpdatedAt of an update made at now to a
// task last updated at previous. It is always later than previous, so that
// the snapshots of one task are ordered by it even when the clock is coarse
// or steps back.
func nextTaskTimestamp(previous string, now time.Time) string {
	now = now.UTC()
	if last, err := time.Parse(time.RFC3339Nano, previous); err == nil && !now.After(last) {
		now = last.Add(time.Nanosecond)
	}
	return now.Format(time.RFC3339Nano)
}
