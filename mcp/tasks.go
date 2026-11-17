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
	"slices"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/internal/jsonrpc2"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// ExtensionTasks is the MCP tasks extension identifier.
//
// A server uses the extension when a [TaskServer] is installed on it. A
// client uses it on a CallTool with [CallToolParams.UseTasks], or with
// client capabilities in its _meta that name the extension; on a call of
// GetTask, UpdateTask, CancelTask or FollowTask; and on every request of a
// client that names the extension in ClientOptions.Capabilities.
//
// Only tools/call can be answered with a task. For the task calls, and for
// any other request of the protocol made by a client whose
// ClientOptions.Capabilities name the extension, a task as the answer is an
// invalid response and the call returns an error. Every other request is
// sent and decoded as it is without the extension, whatever client
// capabilities its _meta carries: a program that hands another program's
// _meta on has not named the extension.
const ExtensionTasks = "io.modelcontextprotocol/tasks"

const (
	methodGetTask          = "tasks/get"
	methodUpdateTask       = "tasks/update"
	methodCancelTask       = "tasks/cancel"
	notificationTaskStatus = "notifications/tasks"

	resultTypeTask resultType = "task"
)

// TaskStatus is the execution state of a task.
type TaskStatus string

const (
	TaskStatusWorking       TaskStatus = "working"
	TaskStatusInputRequired TaskStatus = "input_required"
	TaskStatusCompleted     TaskStatus = "completed"
	TaskStatusCancelled     TaskStatus = "cancelled"
	TaskStatusFailed        TaskStatus = "failed"
)

// Terminal reports whether the task status is final.
func (s TaskStatus) Terminal() bool {
	switch s {
	case TaskStatusCompleted, TaskStatusCancelled, TaskStatusFailed:
		return true
	default:
		return false
	}
}

// Task carries the operational metadata for an asynchronous request.
type Task struct {
	Meta           `json:"_meta,omitempty"`
	TaskID         string     `json:"taskId"`
	Status         TaskStatus `json:"status"`
	StatusMessage  string     `json:"statusMessage,omitempty"`
	CreatedAt      string     `json:"createdAt"`
	LastUpdatedAt  string     `json:"lastUpdatedAt"`
	TTLMs          *int64     `json:"ttlMs"`
	PollIntervalMs *int64     `json:"pollIntervalMs,omitempty"`
}

// DetailedTask is a complete task snapshot, including its status-specific data.
type DetailedTask struct {
	completeResultWithType
	Task
	InputRequests InputRequestMap `json:"inputRequests,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         *jsonrpc.Error  `json:"error,omitempty"`
}

func (*DetailedTask) isResult()     {}
func (*DetailedTask) isParams()     {}
func (t *DetailedTask) isNil() bool { return t == nil }

// ToolResult decodes the final result of a completed tool task.
func (t *DetailedTask) ToolResult() (*CallToolResult, error) {
	if t == nil || t.Status != TaskStatusCompleted || len(t.Result) == 0 {
		return nil, fmt.Errorf("task does not contain a completed tool result")
	}
	var result CallToolResult
	if err := json.Unmarshal(t.Result, &result); err != nil {
		return nil, fmt.Errorf("decoding task tool result: %w", err)
	}
	return &result, nil
}

// Task returns a copy of the task handle, if this is a task creation result.
func (r *CallToolResult) Task() (*Task, bool) {
	if r == nil || r.task == nil {
		return nil, false
	}
	t := *r.task
	t.Meta = maps.Clone(t.Meta)
	if t.TTLMs != nil {
		ttl := *t.TTLMs
		t.TTLMs = &ttl
	}
	if t.PollIntervalMs != nil {
		interval := *t.PollIntervalMs
		t.PollIntervalMs = &interval
	}
	return &t, true
}

// TaskStore persists task snapshots within an authorization scope.
//
// Update applies the callback to the stored task atomically. When the
// callback returns an error, Update stores nothing and returns that error,
// or an error that wraps it.
type TaskStore interface {
	Create(context.Context, string, *DetailedTask) error
	Get(context.Context, string, string) (*DetailedTask, error)
	Update(context.Context, string, string, func(*DetailedTask) error) error
	Delete(context.Context, string, string) error
}

// ErrTaskNotFound reports that a task does not exist in the requested scope.
var ErrTaskNotFound = errors.New("task not found")

// TaskServerOptions configures a TaskServer.
type TaskServerOptions struct {
	// Store holds the tasks. The default is a new [MemoryTaskStore].
	Store TaskStore
	// Scope returns the authorization scope of a request: a task is visible
	// only to requests whose scope equals the scope of the request that
	// created it.
	//
	// With a nil Scope every request has the same scope, so that the task ID
	// alone gives access to a task: IDs are unguessable, and act as bearer
	// tokens. A server that authenticates its callers should return
	// something that identifies the caller, such as the user ID of the
	// request's token.
	Scope func(context.Context, *ServerSession) string
	// RequireTasks makes task tools refuse calls that do not declare the
	// tasks extension, instead of running them synchronously.
	RequireTasks bool
	// DefaultPollInterval is the polling interval suggested to clients. If
	// set, it must be at least a millisecond.
	DefaultPollInterval time.Duration
	// Retention is how long a task is kept after its creation. Zero keeps
	// tasks without limit. If set, it must be at least a millisecond.
	Retention time.Duration
}

// TaskServer adds task-augmented tool execution to a Server.
type TaskServer struct {
	server *Server
	opts   TaskServerOptions

	mu            sync.Mutex
	runtimes      map[string]*taskRuntime
	subscriptions map[taskKey]map[*taskSubscription]struct{}
}

// NewTaskServer installs the tasks extension on server.
func NewTaskServer(server *Server, options *TaskServerOptions) (*TaskServer, error) {
	if server == nil {
		return nil, errors.New("nil Server")
	}
	var opts TaskServerOptions
	if options != nil {
		opts = *options
	}
	if opts.DefaultPollInterval < 0 || opts.Retention < 0 {
		return nil, errors.New("task durations must not be negative")
	}
	// Both are sent in whole milliseconds. A shorter Retention would be sent
	// as a ttlMs of zero and the task would be gone before the client could
	// ask for it; a shorter DefaultPollInterval would not be sent at all.
	if opts.Retention > 0 && opts.Retention < time.Millisecond {
		return nil, fmt.Errorf("mcp: NewTaskServer: Retention %v is shorter than a millisecond", opts.Retention)
	}
	if opts.DefaultPollInterval > 0 && opts.DefaultPollInterval < time.Millisecond {
		return nil, fmt.Errorf("mcp: NewTaskServer: DefaultPollInterval %v is shorter than a millisecond", opts.DefaultPollInterval)
	}
	if opts.Store == nil {
		opts.Store = NewMemoryTaskStore()
	}
	ts := &TaskServer{
		server:        server,
		opts:          opts,
		runtimes:      make(map[string]*taskRuntime),
		subscriptions: make(map[taskKey]map[*taskSubscription]struct{}),
	}
	// Install under server.mu, the lock AddReceivingCustomMethod writes the
	// method table under: whichever of the two runs second sees the first and
	// is refused. Nothing is changed when the install fails.
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.tasks.Load() != nil {
		return nil, errors.New("tasks extension is already installed")
	}
	for _, method := range taskMethods {
		if _, ok := server.receiveMethods[method]; ok {
			return nil, fmt.Errorf("mcp: NewTaskServer: %q is already registered as a custom method", method)
		}
	}
	// Sessions index the method tables after releasing server.mu, so the
	// table is replaced, never written in place.
	receiveMethods := maps.Clone(server.receiveMethods)
	receiveMethods[methodGetTask] = taskMethodInfo(ts.getTask)
	receiveMethods[methodUpdateTask] = taskMethodInfo(ts.updateTaskResponses)
	receiveMethods[methodCancelTask] = taskMethodInfo(ts.cancelTask)
	receiveMethods[methodSubscriptionsListen] = taskListenMethodInfo(receiveMethods[methodSubscriptionsListen])
	server.receiveMethods = receiveMethods
	server.tasks.Store(ts)
	return ts, nil
}

// taskMethods are the request methods an installed TaskServer owns.
var taskMethods = [...]string{methodGetTask, methodUpdateTask, methodCancelTask}

// ownsMethod reports whether method is one of the installed extension's
// request methods.
func (s *TaskServer) ownsMethod(method string) bool {
	return slices.Contains(taskMethods[:], method)
}

// advertise adds the extension to a server/discover result. It is reached
// only through an installed TaskServer.
//
// The extension is advertised where it can be used: to a request of a
// protocol version that has it, on a connection that serves such a version.
// A connection that does not (a Streamable HTTP server with sessions) names
// no such version in the result, and refuses every request the extension
// needs.
func (s *TaskServer) advertise(res *DiscoverResult, protocolVersion string) {
	if res.Capabilities == nil || protocolVersion < protocolVersion20260728 {
		return
	}
	if !slices.ContainsFunc(res.SupportedVersions, func(v string) bool { return v >= protocolVersion20260728 }) {
		return
	}
	res.Capabilities.AddExtension(ExtensionTasks, nil)
}

// GetTaskParams are the parameters for tasks/get.
type GetTaskParams struct {
	ParamsBase
	TaskID string `json:"taskId"`

	// decodeErr is set, with only Meta, when the params of a received request
	// did not decode. See taskMethodInfo.
	decodeErr error
}

// UpdateTaskParams are the parameters for tasks/update.
type UpdateTaskParams struct {
	ParamsBase
	TaskID         string           `json:"taskId"`
	InputResponses InputResponseMap `json:"inputResponses"`

	decodeErr error // see GetTaskParams.decodeErr
}

// CancelTaskParams are the parameters for tasks/cancel.
type CancelTaskParams struct {
	ParamsBase
	TaskID string `json:"taskId"`

	// decodeErr is set, with only Meta, when the params of a received request
	// did not decode. See taskMethodInfo.
	decodeErr error
}

type taskAckResult struct {
	completeResultWithType
	Meta `json:"_meta,omitempty"`
}

func (*taskAckResult) isResult()     {}
func (r *taskAckResult) isNil() bool { return r == nil }

// marshalCreateTaskResult writes the result an installed TaskServer made for
// a task it started (see CallToolResult.taskCreated) as a CreateTaskResult:
// the task's fields, resultType "task" and the result's _meta. It is the only
// place the SDK writes resultType "task".
func marshalCreateTaskResult(result *CallToolResult) ([]byte, error) {
	data, err := json.Marshal(result.task)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	fields["resultType"] = json.RawMessage(`"task"`)
	if result.Meta != nil {
		meta, err := json.Marshal(result.Meta)
		if err != nil {
			return nil, err
		}
		fields["_meta"] = meta
	}
	return json.Marshal(fields)
}

// subscriptionTaskIDs reads notifications.taskIds from the raw params of a
// subscriptions/listen request or of its acknowledgement. The shared
// NotificationSubscriptions type neither reads nor writes that key, so that
// its JSON is the same with and without the extension; the extension reads
// the key here, and only where it is installed or opted in.
//
// present reports whether the key has a non-null value, wellFormed whether
// that value is an array of strings.
func subscriptionTaskIDs(raw json.RawMessage) (ids []string, present, wellFormed bool) {
	var wire struct {
		Notifications struct {
			TaskIDs json.RawMessage `json:"taskIds"`
		} `json:"notifications"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, false, false
	}
	value := wire.Notifications.TaskIDs
	if len(value) == 0 || string(value) == "null" {
		return nil, false, false
	}
	if err := json.Unmarshal(value, &ids); err != nil {
		return nil, true, false
	}
	return ids, true, true
}

// taskSubscriptionsWire is the wire form of NotificationSubscriptions with
// taskIds, which the shared type's JSON leaves out.
type taskSubscriptionsWire struct {
	NotificationSubscriptions
	TaskIDs []string `json:"taskIds,omitempty"`
}

func newTaskSubscriptionsWire(subscriptions NotificationSubscriptions) taskSubscriptionsWire {
	return taskSubscriptionsWire{NotificationSubscriptions: subscriptions, TaskIDs: subscriptions.TaskIDs}
}

// taskListenMethodInfo returns the subscriptions/listen entry of a server
// with the extension installed: the standard entry, whose params decode also
// reads notifications.taskIds.
func taskListenMethodInfo(standard methodInfo) methodInfo {
	decode := standard.unmarshalParams
	standard.unmarshalParams = func(raw json.RawMessage) (Params, error) {
		params, err := decode(raw)
		if err != nil {
			return params, err
		}
		listen, ok := params.(*SubscriptionsListenParams)
		if !ok || listen == nil || listen.Notifications == nil {
			return params, nil
		}
		ids, present, wellFormed := subscriptionTaskIDs(raw)
		switch {
		case !present:
		case wellFormed:
			listen.Notifications.TaskIDs = ids
		case metaUsesTasks(listen.Meta):
			return nil, fmt.Errorf("%w: notifications.taskIds must be an array of strings", jsonrpc2.ErrInvalidParams)
		default:
			// A request that does not declare the extension and names tasks
			// is answered with the missing-capability error, well formed or
			// not. That error carries data, which an error returned from
			// here cannot (handleReceive wraps it). So hand the handler a
			// list that names tasks; the empty id is never a task's id and
			// is never acknowledged.
			listen.Notifications.TaskIDs = []string{""}
		}
		return params, nil
	}
	return standard
}
