// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"

	internaljson "github.com/modelcontextprotocol/go-sdk/internal/json"
	"github.com/modelcontextprotocol/go-sdk/internal/jsonrpc2"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func (s *TaskServer) getTask(ctx context.Context, req *ServerRequest[*GetTaskParams]) (*DetailedTask, error) {
	if req.ProtocolVersion() < protocolVersion20260728 {
		return nil, taskMethodUnsupported(methodGetTask)
	}
	if !requestUsesTasks(req) {
		return nil, missingTasksCapabilityError()
	}
	if req.Params != nil && req.Params.decodeErr != nil {
		return nil, req.Params.decodeErr
	}
	if req.Params == nil || req.Params.TaskID == "" {
		return nil, invalidTaskIDError("")
	}
	task, err := s.opts.Store.Get(ctx, s.scope(ctx, req.Session), req.Params.TaskID)
	if err != nil {
		return nil, taskRequestError("retrieve", req.Params.TaskID, err)
	}
	task.ResultType = resultTypeComplete
	return task, nil
}

func (s *TaskServer) updateTaskResponses(ctx context.Context, req *ServerRequest[*UpdateTaskParams]) (*taskAckResult, error) {
	if req.ProtocolVersion() < protocolVersion20260728 {
		return nil, taskMethodUnsupported(methodUpdateTask)
	}
	if !requestUsesTasks(req) {
		return nil, missingTasksCapabilityError()
	}
	if req.Params != nil && req.Params.decodeErr != nil {
		return nil, req.Params.decodeErr
	}
	if req.Params == nil || req.Params.TaskID == "" {
		return nil, invalidTaskIDError("")
	}
	scope := s.scope(ctx, req.Session)
	id := req.Params.TaskID
	task, err := s.opts.Store.Get(ctx, scope, id)
	if err != nil {
		return nil, taskRequestError("update", id, err)
	}
	if task.Status == TaskStatusInputRequired {
		if runtime := s.taskRuntime(id); runtime != nil {
			answered := runtime.answer(req.Params.InputResponses)
			if len(answered) > 0 {
				err := s.updateTask(ctx, scope, id, resolveTaskInput(maps.Keys(answered)))
				if err != nil && !errors.Is(err, errTaskTerminal) {
					return nil, taskRequestError("update", id, err)
				}
			}
		}
	}
	return &taskAckResult{}, nil
}

func (s *TaskServer) cancelTask(ctx context.Context, req *ServerRequest[*CancelTaskParams]) (*taskAckResult, error) {
	if req.ProtocolVersion() < protocolVersion20260728 {
		return nil, taskMethodUnsupported(methodCancelTask)
	}
	if !requestUsesTasks(req) {
		return nil, missingTasksCapabilityError()
	}
	if req.Params != nil && req.Params.decodeErr != nil {
		return nil, req.Params.decodeErr
	}
	if req.Params == nil || req.Params.TaskID == "" {
		return nil, invalidTaskIDError("")
	}
	scope := s.scope(ctx, req.Session)
	id := req.Params.TaskID
	if _, err := s.opts.Store.Get(ctx, scope, id); err != nil {
		return nil, taskRequestError("cancel", id, err)
	}
	err := s.updateTask(ctx, scope, id, func(task *DetailedTask) error {
		task.Status = TaskStatusCancelled
		task.InputRequests = nil
		return nil
	})
	if err != nil && !errors.Is(err, errTaskTerminal) {
		return nil, taskRequestError("cancel", id, err)
	}
	if runtime := s.taskRuntime(id); runtime != nil {
		runtime.cancel()
	}
	return &taskAckResult{}, nil
}

func (s *TaskServer) taskRuntime(id string) *taskRuntime {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runtimes[id]
}

func invalidTaskIDError(id string) *jsonrpc.Error {
	return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: fmt.Sprintf("invalid task ID %q", id)}
}

func taskRequestError(operation, id string, err error) error {
	if errors.Is(err, ErrTaskNotFound) {
		return invalidTaskIDError(id)
	}
	return &jsonrpc.Error{
		Code:    jsonrpc.CodeInternalError,
		Message: fmt.Sprintf("failed to %s task %q: %v", operation, id, err),
	}
}

// taskRequestParams is implemented by the params of the three task request
// methods.
type taskRequestParams interface {
	setDecodeError(Meta, error)
}

func (p *GetTaskParams) setDecodeError(meta Meta, err error)    { p.Meta, p.decodeErr = meta, err }
func (p *UpdateTaskParams) setDecodeError(meta Meta, err error) { p.Meta, p.decodeErr = meta, err }
func (p *CancelTaskParams) setDecodeError(meta Meta, err error) { p.Meta, p.decodeErr = meta, err }

// taskMethodInfo returns the method table entry of one of the three task
// request methods.
//
// A request that does not declare the extension is answered with the
// missing-capability error whatever the rest of its params look like, so
// malformed params must reach the handler: when the standard decode fails,
// the entry hands the handler params that carry only the request's _meta and
// the decode error, and the handler reports that error once it has checked
// the declaration.
func taskMethodInfo[P interface {
	paramsPtr[T]
	taskRequestParams
}, R Result, T any](handler typedServerMethodHandler[P, R]) methodInfo {
	info := newServerMethodInfo(handler, missingParamsOK)
	decode := info.unmarshalParams
	info.unmarshalParams = func(raw json.RawMessage) (Params, error) {
		params, err := decode(raw)
		if err == nil {
			return params, nil
		}
		var wire struct {
			Meta Meta `json:"_meta"`
		}
		if internaljson.Unmarshal(raw, &wire) != nil {
			wire.Meta = nil
		}
		p := P(new(T))
		p.setDecodeError(wire.Meta, err)
		return p, nil
	}
	return info
}

// validateHeaders checks the Mcp-Name header of a tasks/get, tasks/update or
// tasks/cancel request received over Streamable HTTP: a request that declares
// the extension must carry its taskId there. It is reached only through an
// installed TaskServer, after the standard header validation, and follows the
// same protocol version rule.
//
// A request that does not declare the extension is left to the handler, which
// answers it with the missing-capability error; so is one whose taskId is not
// a non-empty string, which the handler answers with invalid params.
func (s *TaskServer) validateHeaders(header http.Header, msg jsonrpc.Message) error {
	if version := header.Get(protocolVersionHeader); version == "" || version < minVersionForStandardHeaders {
		return nil
	}
	req, ok := msg.(*jsonrpc.Request)
	if !ok || !s.ownsMethod(req.Method) {
		return nil
	}
	var wire struct {
		Meta   Meta            `json:"_meta"`
		TaskID json.RawMessage `json:"taskId"`
	}
	if err := internaljson.Unmarshal(req.Params, &wire); err != nil || !metaUsesTasks(wire.Meta) {
		return nil
	}
	var id string
	if err := internaljson.Unmarshal(wire.TaskID, &id); err != nil || id == "" {
		return nil
	}
	// As the standard validation reads the Mcp-Name of a tool, a prompt or a
	// resource: sent once, and Base64-wrapped if it is not header-safe.
	nameInHeader, present, err := singletonHeader(header, nameHeader)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("missing required Mcp-Name header for method %q", req.Method)
	}
	name, ok := decodeHeaderValue(nameInHeader)
	if !ok {
		return errors.New("header mismatch: Mcp-Name header contains invalid Base64 encoding")
	}
	if name != id {
		return fmt.Errorf("header mismatch: Mcp-Name header value '%s' does not match body value '%s'", name, id)
	}
	return nil
}

// taskMethodUnsupported is the answer to a task request of a protocol
// version that has no tasks extension: the answer that version gives for a
// method it does not have.
func taskMethodUnsupported(method string) error {
	return fmt.Errorf("%w: %q unsupported", jsonrpc2.ErrNotHandled, method)
}
