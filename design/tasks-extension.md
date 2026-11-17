## Context

A proposal for implementing the tasks extension, `io.modelcontextprotocol/tasks`,
as defined in [SEP-2663](https://modelcontextprotocol.io/seps/2663-tasks-extension)
for protocol version 2026-07-28. The wire contract is the
[2026-07-28 extension specification](https://tasks.extensions.modelcontextprotocol.io/specification/2026-07-28/tasks).

With the extension a server may answer `tools/call` with a task handle instead
of a result, and run the call in the background. The client reads the task's
state, supplies input it asks for, and cancels it, with three new requests.

## Goals

**Must have:**

* Backward compatibility: a program that does not use the extension sends,
  receives and observes exactly what it does today, and the exported API only
  grows.
* Correct representation on the wire.

**Nice to have:**

* Minimal changes to the shared code paths.
* Consistency with multi round-trip requests (`design/mrtr.md`).

## Wire format

The extension has these methods and this notification, and no others:

- `tasks/get`
- `tasks/update`
- `tasks/cancel`
- `notifications/tasks`

There is no `tasks/list`, `tasks/result`, `task` request parameter or per-tool
capability. `notifications/cancelled` does not cancel a task. Only `tools/call`
can be answered with a task.

A task answer to `tools/call` is the `Task` flattened into the result object
with `resultType: "task"`:

```json
{
  "resultType": "task",
  "taskId": "786512e2-9e0d-44bd-8f29-789f320fe840",
  "status": "working",
  "statusMessage": "optional",
  "createdAt": "2026-07-28T10:30:00Z",
  "lastUpdatedAt": "2026-07-28T10:30:00Z",
  "ttlMs": 60000,
  "pollIntervalMs": 5000
}
```

`status` is `working`, `input_required`, `completed`, `cancelled` or `failed`.
`ttlMs` is required and may be null, meaning no limit. `pollIntervalMs` is
optional.

`tasks/get` takes `{ "taskId": "..." }` and returns the task with
`resultType: "complete"` and, depending on its status:

- `inputRequests` when `input_required`;
- `result`, the tool result, when `completed` (a tool result with
  `isError: true` included);
- `error`, a JSON-RPC error object, when `failed`.

`tasks/update` takes a task ID and `inputResponses`. `tasks/cancel` takes a
task ID. Both return an empty acknowledgement. Cancellation is cooperative.

An unknown task ID is answered with `-32602`. A task request, or a task
subscription, whose request does not declare the extension is answered with
`-32021` and `requiredCapabilities.extensions["io.modelcontextprotocol/tasks"]`.

Over Streamable HTTP the three task requests carry the task ID in `Mcp-Name`,
encoded as other names in that header are when it is not header-safe.

## Negotiation

A client declares the extension **per request**, in that request's `_meta`:

```json
"io.modelcontextprotocol/clientCapabilities": {
  "extensions": { "io.modelcontextprotocol/tasks": {} }
}
```

The server advertises the extension under `capabilities.extensions` in its
`server/discover` result, and decides for each declaring call whether to answer
with a task. Only the request's own metadata counts. The extension is neither
advertised nor acted on for an older protocol version, or on a connection that
does not serve 2026-07-28.

## Status delivery

Polling `tasks/get` is the baseline. A client may also open
`subscriptions/listen` with `notifications.taskIds`. The acknowledgement names
the task IDs the server accepted, and each `notifications/tasks` on that stream
carries the complete task, final result or error included. A subscriber is told
nothing before its acknowledgement is written.

## Proposal

All new code is in new files in `mcp`. The shared files gain a few hooks, each
of which does nothing unless the extension is in use: on a server, unless a
`TaskServer` is installed; on a client, unless the call opted in.

### Types

```go
const ExtensionTasks = "io.modelcontextprotocol/tasks"

type TaskStatus string
const (
    TaskStatusWorking       TaskStatus = "working"
    TaskStatusInputRequired TaskStatus = "input_required"
    TaskStatusCompleted     TaskStatus = "completed"
    TaskStatusCancelled     TaskStatus = "cancelled"
    TaskStatusFailed        TaskStatus = "failed"
)
func (s TaskStatus) Terminal() bool

type Task struct {
    Meta
    TaskID         string     `json:"taskId"`
    Status         TaskStatus `json:"status"`
    StatusMessage  string     `json:"statusMessage,omitempty"`
    CreatedAt      string     `json:"createdAt"`
    LastUpdatedAt  string     `json:"lastUpdatedAt"`
    TTLMs          *int64     `json:"ttlMs"`
    PollIntervalMs *int64     `json:"pollIntervalMs,omitempty"`
}

type DetailedTask struct {
    Task
    InputRequests InputRequestMap `json:"inputRequests,omitempty"`
    Result        json.RawMessage `json:"result,omitempty"`
    Error         *jsonrpc.Error  `json:"error,omitempty"`
}
func (t *DetailedTask) ToolResult() (*CallToolResult, error)

// Task returns a copy of the task handle, if the result created a task.
func (r *CallToolResult) Task() (*Task, bool)
```

`ExtensionTasks`, `Task` and `TaskStatus` keep the names of draft PR #1335.

`CallToolResult` holds its task privately, as it holds `input_required` data,
and exposes it with an accessor. Its JSON is unchanged for every result the
extension did not make: only a result that an installed `TaskServer` created
for a task is written as a task, and only a call that opted in decodes one.

### Server

```go
type TaskServerOptions struct {
    Store               TaskStore // default: a new MemoryTaskStore
    Scope               func(context.Context, *ServerSession) string
    RequireTasks        bool
    DefaultPollInterval time.Duration
    Retention           time.Duration
}
func NewTaskServer(*Server, *TaskServerOptions) (*TaskServer, error)

type TaskToolHandler func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error)
type TaskToolHandlerFor[In, Out any] func(context.Context, *CallToolRequest, *TaskContext, In) (*CallToolResult, Out, error)
func (s *TaskServer) AddTool(*Tool, TaskToolHandler)
func AddTaskTool[In, Out any](*TaskServer, *Tool, TaskToolHandlerFor[In, Out])

func (c *TaskContext) TaskID() string
func (c *TaskContext) SetStatusMessage(string) error
func (c *TaskContext) RequireInput(context.Context, InputRequestMap) (InputResponseMap, error)

type TaskStore interface {
    Create(ctx context.Context, scope string, task *DetailedTask) error
    Get(ctx context.Context, scope, id string) (*DetailedTask, error)
    Update(ctx context.Context, scope, id string, update func(*DetailedTask) error) error
    Delete(ctx context.Context, scope, id string) error
}
var ErrTaskNotFound error
func NewMemoryTaskStore() *MemoryTaskStore
```

`NewTaskServer` installs the extension on a server: the three methods, the
advertisement and the task subscriptions. A server without one behaves as it
does today, and may register its own `tasks/*` custom methods. `NewTaskServer`
fails if such a method is already registered, and registering one afterwards
is refused.

A tool added through the `TaskServer` runs as a task when the call declares the
extension: the task is stored, with a random ID, before its handle is returned,
and the handler then runs in the background with a context that `tasks/cancel`
cancels. A call that does not declare the extension runs the same handler
synchronously, or is refused with `-32021` when `RequireTasks` is set. Tools
added with `Server.AddTool` are never answered with a task.
`TaskContext.TaskID` returns the task's ID to a handler that runs as a task,
and the empty string to one that runs synchronously.

A handler's result and error map to the task as they map to a synchronous
answer: a tool error completes the task with `isError: true`, and an error that
would be a JSON-RPC error fails the task with that same error object.

A task handler asks the client for input with `TaskContext.RequireInput`, which
sets the task to `input_required` and returns when `tasks/update` has supplied
every answer. Input request keys are not reused within a task.

Every task belongs to a scope, and a request sees only tasks of its own scope;
a task of another scope is reported as unknown. With no `Scope` function all
requests share one scope and the ID alone gives access to a task.

`Retention` is the task's `ttlMs`; zero means no limit. `MemoryTaskStore`
discards expired tasks during its operations.

### Client

```go
type CallToolParams struct {
    // Existing fields unchanged.
    UseTasks bool `json:"-"`
}

func (cs *ClientSession) GetTask(context.Context, *GetTaskParams) (*DetailedTask, error)
func (cs *ClientSession) UpdateTask(context.Context, *UpdateTaskParams) error
func (cs *ClientSession) CancelTask(context.Context, *CancelTaskParams) error
func (cs *ClientSession) FollowTask(
    ctx context.Context, id string,
    update func(context.Context, *DetailedTask) error,
) (*DetailedTask, error)
```

A call uses the extension in one of three ways:

- `CallToolParams.UseTasks`, for one call. `CallTool` adds the declaration to a
  copy of the request's metadata. The field is not sent.
- The extension named in `ClientOptions.Capabilities`, for every request.
- A declaration the program puts in a call's `_meta` itself.

When the server answers such a call with a task, `CallToolResult.Task` returns
its handle. `CallTool` does not follow the task: the caller chooses between the
handle, `GetTask` and `FollowTask`. The task helpers declare the extension on
their own requests. All of them return an error on an older protocol version.

A task as the answer to a request other than `tools/call` is an invalid
response. It is refused for the task helpers, and for every request of a client
whose `ClientOptions.Capabilities` name the extension. Other requests are
decoded as they are today.

`FollowTask` returns the task's terminal state. It polls at the interval the
server suggests, and also opens a task subscription if the server advertises
the extension, so that a lost notification is recovered. The callback sees each
new state once, in order. `FollowTask` answers the task's input requests with
the client's elicitation, sampling and roots handlers, as a multi round-trip
call does, and sends the answers with `tasks/update`.

A client handles `notifications/tasks` only while a `FollowTask` call is
running. At any other time the client's method tables are the ones it has
today, custom methods included.

### Subscriptions

`NotificationSubscriptions` gains `TaskIDs []string`, tagged `json:"-"`. The
type's own JSON is unchanged. Only the extension reads and writes `taskIds`, in
a listen request received by a server with a `TaskServer` and in an
acknowledgement received while `FollowTask` runs.

## Compatibility

The exported API only gains names; `apidiff` reports additions and nothing
else. The new fields' zero values keep today's JSON and behaviour. A server
without a `TaskServer` registers no method and advertises nothing. A client
that does not opt in declares nothing and decodes every answer as it does
today, a `resultType: "task"` answer included.

Tests sit beside the code. They cover the wire shapes, the declaration being
present only on opted-in requests, the synchronous path for calls that do not
declare, the task methods and their errors, input, notification and polling,
cancellation racing completion, and the Streamable HTTP headers. Orderings are
held with channels and a fake poll timer, never with sleeps.

## Not implemented

The extension recommends that a server using both multi round-trip requests
and tasks gathers its input with the former before creating the task. Here the
task is created before the handler runs, so a task handler asks for input as
part of the task, and a task handler that returns `InputRequests` or a
`RequestState` fails the task. Supporting the recommended order needs API
beyond this proposal.

## Provenance

The voocel/mcp-sdk-go `ext/tasks` implementation was read as a behavioural
reference only; no code or comments are copied or adapted. Draft PR #1335
contributes only the type names noted above.
