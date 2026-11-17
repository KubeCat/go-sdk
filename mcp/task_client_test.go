// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func TestClientTaskOptIn(t *testing.T) {
	server := NewServer(testImpl, nil)
	store := newObservedTaskStore()
	tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release, releaseTask := taskTestGate()
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
			if requestUsesTasks(req) {
				close(started)
				<-release
				return &CallToolResult{Content: []Content{&TextContent{Text: "task result"}}}, nil
			}
			return &CallToolResult{Content: []Content{&TextContent{Text: "immediate"}}}, nil
		})
	client := connectTaskClient(t, server, nil)
	t.Cleanup(releaseTask)

	ordinary, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if task, ok := ordinary.Task(); ok || task != nil {
		t.Fatalf("call without UseTasks returned task %+v", task)
	}
	if got := ordinary.Content[0].(*TextContent).Text; got != "immediate" {
		t.Fatalf("ordinary result = %q, want immediate", got)
	}

	originalCaps := &ClientCapabilities{Extensions: map[string]any{"example.com/other": map[string]any{"setting": true}}}
	params := &CallToolParams{
		Name:     "work",
		UseTasks: true,
		Meta:     Meta{MetaKeyClientCapabilities: originalCaps, "example.com/metadata": "preserved"},
	}
	originalMeta := Meta{MetaKeyClientCapabilities: originalCaps, "example.com/metadata": "preserved"}
	result, err := client.CallTool(taskTestContext(t), params)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.Task()
	if !ok || task.TaskID == "" {
		t.Fatalf("UseTasks call did not return a task: %+v", result)
	}
	if !reflect.DeepEqual(params.Meta, originalMeta) {
		t.Fatalf("CallTool mutated caller metadata: got %#v, want %#v", params.Meta, originalMeta)
	}
	if len(originalCaps.Extensions) != 1 || originalCaps.Extensions[ExtensionTasks] != nil {
		t.Fatalf("CallTool mutated caller capabilities: %#v", originalCaps.Extensions)
	}
	wire, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "UseTasks") || strings.Contains(string(wire), "useTasks") {
		t.Fatalf("UseTasks leaked into tool parameters: %s", wire)
	}
	waitTask(t, started, "task handler start")
	releaseTask()
	waitTask(t, store.done, "terminal task store update")
	final, err := client.GetTask(taskTestContext(t), &GetTaskParams{TaskID: task.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	toolResult, err := final.ToolResult()
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResult.Content[0].(*TextContent).Text; got != "task result" {
		t.Fatalf("task result = %q, want task result", got)
	}
}

func TestClientTaskMethodHelpers(t *testing.T) {
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
			responses, err := taskContext.RequireInput(ctx, InputRequestMap{"answer": &ElicitParams{Message: "answer?"}})
			if err != nil {
				return nil, err
			}
			answer := responses["answer"].(*ElicitResult).Content["answer"]
			return &CallToolResult{Content: []Content{&TextContent{Text: answer.(string)}}}, nil
		})
	cancelStarted := make(chan struct{})
	tasks.AddTool(&Tool{Name: "cancel", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
			ctx = handlerContext(ctx)
			close(cancelStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	client := connectTaskClient(t, server, nil)
	t.Cleanup(stopHandlers)

	result, err := client.CallTool(taskTestContext(t), &CallToolParams{Name: "input", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task := taskHandle(t, result)
	if snapshot := waitTask(t, store.snapshots, "input-required snapshot"); snapshot.Status != TaskStatusInputRequired {
		t.Fatalf("status = %q, want input_required", snapshot.Status)
	}
	getParams := &GetTaskParams{TaskID: task.TaskID, ParamsBase: ParamsBase{Meta: Meta{"example.com/metadata": "get"}}}
	current, err := client.GetTask(taskTestContext(t), getParams)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.InputRequests) != 1 || len(getParams.Meta) != 1 {
		t.Fatalf("GetTask result or caller metadata changed: task=%+v meta=%v", current, getParams.Meta)
	}
	updateParams := &UpdateTaskParams{
		TaskID:         task.TaskID,
		InputResponses: InputResponseMap{"answer": &ElicitResult{Action: "accept", Content: map[string]any{"answer": "updated"}}},
		ParamsBase:     ParamsBase{Meta: Meta{"example.com/metadata": "update"}},
	}
	if err := client.UpdateTask(taskTestContext(t), updateParams); err != nil {
		t.Fatal(err)
	}
	if len(updateParams.Meta) != 1 {
		t.Fatalf("UpdateTask mutated caller metadata: %v", updateParams.Meta)
	}
	waitTask(t, store.done, "terminal task store update")
	current, err = client.GetTask(taskTestContext(t), &GetTaskParams{TaskID: task.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	toolResult, err := current.ToolResult()
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResult.Content[0].(*TextContent).Text; got != "updated" {
		t.Fatalf("updated result = %q, want updated", got)
	}

	result, err = client.CallTool(taskTestContext(t), &CallToolParams{Name: "cancel", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task, _ = result.Task()
	waitTask(t, cancelStarted, "cancellation handler start")
	cancelParams := &CancelTaskParams{TaskID: task.TaskID, ParamsBase: ParamsBase{Meta: Meta{"example.com/metadata": "cancel"}}}
	if err := client.CancelTask(taskTestContext(t), cancelParams); err != nil {
		t.Fatal(err)
	}
	if len(cancelParams.Meta) != 1 {
		t.Fatalf("CancelTask mutated caller metadata: %v", cancelParams.Meta)
	}
	waitTask(t, store.done, "terminal task store update")
	current, err = client.GetTask(taskTestContext(t), &GetTaskParams{TaskID: task.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != TaskStatusCancelled {
		t.Fatalf("cancelled task status = %q", current.Status)
	}
}

func TestClientTasksRejectLegacyProtocol(t *testing.T) {
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
		return &CallToolResult{}, nil
	})
	// Everything the server receives after the handshake.
	received, receive := taskEvents[string](t, "requests at the server")
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			receive(method)
			return next(ctx, method, req)
		}
	})
	client := connectTaskClient(t, server, &ClientSessionOptions{ProtocolVersion: protocolVersion20251125})
	ctx := taskTestContext(t)
	if _, err := client.ListTools(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for len(received) > 0 {
		<-received
	}
	// Each call is refused by the client, with the same error, and sends
	// nothing: the server would refuse it too, but with its own error.
	want := tasksProtocolError().Error()
	calls := map[string]func() error{
		"CallTool with UseTasks": func() error {
			_, err := client.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
			return err
		},
		"GetTask": func() error {
			_, err := client.GetTask(ctx, &GetTaskParams{TaskID: "task"})
			return err
		},
		"UpdateTask": func() error { return client.UpdateTask(ctx, &UpdateTaskParams{TaskID: "task"}) },
		"CancelTask": func() error { return client.CancelTask(ctx, &CancelTaskParams{TaskID: "task"}) },
		"FollowTask": func() error {
			_, err := client.FollowTask(ctx, "task", nil)
			return err
		},
	}
	for name, call := range calls {
		if err := call(); err == nil || err.Error() != want {
			t.Errorf("%s on a legacy session: %v, want %q", name, err, want)
		}
	}
	// The ping is answered after everything the client sent before it.
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := waitTask(t, received, "the ping at the server"); got != "ping" {
		t.Errorf("a refused call reached the server: %s", got)
	}
}

// FollowTask on a session of an earlier protocol version is refused before
// it does anything: also when the server's capabilities name the extension.
// (With a TaskServer the extension is not advertised to such a session.)
func TestFollowTaskOnLegacySessionSendsNothing(t *testing.T) {
	server := NewServer(testImpl, &ServerOptions{
		Capabilities: &ServerCapabilities{Extensions: map[string]any{ExtensionTasks: map[string]any{}}},
	})
	received, receive := taskEvents[string](t, "requests at the server")
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			receive(method)
			return next(ctx, method, req)
		}
	})
	client := connectTaskClient(t, server, &ClientSessionOptions{ProtocolVersion: protocolVersion20251125})
	ctx := taskTestContext(t)
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for got := ""; got != "ping"; {
		got = waitTask(t, received, "the handshake and the first ping at the server")
	}
	if _, err := client.FollowTask(ctx, "task", nil); err == nil || err.Error() != tasksProtocolError().Error() {
		t.Errorf("FollowTask on a legacy session: %v, want %q", err, tasksProtocolError())
	}
	// The ping is answered after everything the client sent before it.
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := waitTask(t, received, "the ping at the server"); got != "ping" {
		t.Errorf("the refused FollowTask reached the server: %s", got)
	}
	if client.taskReceive.Load() != nil {
		t.Error("the refused FollowTask left the session's follow table in place")
	}
}

// dropTaskNotifications makes server lose every notifications/tasks it would
// send, as if the peer never received them.
func dropTaskNotifications(server *Server) {
	server.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if _, ok := req.GetParams().(*DetailedTask); ok {
				return nil, nil
			}
			return next(ctx, method, req)
		}
	})
}

// newStoredTaskServer returns a server with the extension whose store holds
// one working task that no handler runs: the test changes it in the store,
// behind the TaskServer's back, so no notification is sent for a change.
func newStoredTaskServer(t *testing.T, id string) (*Server, *MemoryTaskStore) {
	t.Helper()
	server := NewServer(testImpl, nil)
	store := NewMemoryTaskStore()
	if err := store.Create(t.Context(), "", &DetailedTask{Task: Task{
		TaskID:        id,
		Status:        TaskStatusWorking,
		StatusMessage: "initial",
		CreatedAt:     "2026-07-28T10:30:00Z",
		LastUpdatedAt: "2026-07-28T10:30:00Z",
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewTaskServer(server, &TaskServerOptions{Store: store}); err != nil {
		t.Fatal(err)
	}
	return server, store
}

// changeStoredTask changes the task in the store directly.
func changeStoredTask(t *testing.T, store *MemoryTaskStore, id string, change func(*DetailedTask)) {
	t.Helper()
	if err := store.Update(t.Context(), "", id, func(task *DetailedTask) error {
		change(task)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func completeStoredTask(text string) func(*DetailedTask) {
	return func(task *DetailedTask) {
		task.Status = TaskStatusCompleted
		task.LastUpdatedAt = "2026-07-28T10:30:09Z"
		task.Result = json.RawMessage(`{"content":[{"type":"text","text":"` + text + `"}]}`)
	}
}

// endFollow checks that FollowTask ends with the completed task whose result
// is text, and that the callback is given nothing more.
func endFollow(t *testing.T, f *followFixture, text string) {
	t.Helper()
	outcome := f.end("FollowTask")
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if outcome.task.Status != TaskStatusCompleted {
		t.Fatalf("final status = %q, want completed", outcome.task.Status)
	}
	toolResult, err := outcome.task.ToolResult()
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResult.Content[0].(*TextContent).Text; got != text {
		t.Fatalf("final result = %q, want %q", got, text)
	}
}

// A task's changes reach FollowTask as notifications: the poll timer is never
// fired. FollowTask answers the task's input request through the client's
// elicitation handler, which the test holds until it has seen the request.
//
// In the second variant the follower lags: while FollowTask is held in its
// callback, with the snapshot that asks for input, another caller answers the
// request and the task completes. FollowTask then still asks the handler and
// sends its answer, which the completed task ignores.
func TestClientFollowTaskNotifications(t *testing.T) {
	for _, lag := range []bool{false, true} {
		name := "follower waiting"
		if lag {
			name = "follower held in its callback"
		}
		t.Run(name, func(t *testing.T) {
			ctx := taskTestContext(t)
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
					responses, err := taskContext.RequireInput(ctx, InputRequestMap{"answer": &ElicitParams{Message: "answer?"}})
					if err != nil {
						return nil, err
					}
					answer := responses["answer"].(*ElicitResult).Content["answer"]
					return &CallToolResult{Content: []Content{&TextContent{Text: answer.(string)}}}, nil
				})
			asked, ask := taskEvents[string](t, "elicitation requests")
			answer, giveAnswer := taskTestGate()
			client := connectTaskClientWith(t, server, &ClientOptions{
				ElicitationHandler: func(ctx context.Context, req *ElicitRequest) (*ElicitResult, error) {
					ask(req.Params.Message)
					select {
					case <-answer:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return &ElicitResult{Action: "accept", Content: map[string]any{"answer": "from the handler"}}, nil
				},
			}, nil)
			t.Cleanup(stopHandlers)
			t.Cleanup(giveAnswer)
			result, err := client.CallTool(ctx, &CallToolParams{Name: "input", UseTasks: true})
			if err != nil {
				t.Fatal(err)
			}
			task := taskHandle(t, result)
			if snapshot := waitTask(t, store.snapshots, "input-required snapshot"); snapshot.Status != TaskStatusInputRequired {
				t.Fatalf("status = %q, want input_required", snapshot.Status)
			}
			f := startFollow(t, client, task.TaskID, lag)
			if first := f.next("the first snapshot"); first.Status != TaskStatusInputRequired {
				t.Fatalf("first snapshot status = %q, want input_required", first.Status)
			}
			f.polls(1, "the first poll")
			want := "from the handler"
			if lag {
				// The callback is held: FollowTask has not asked the handler.
				want = "from another caller"
				if err := client.UpdateTask(ctx, &UpdateTaskParams{
					TaskID:         task.TaskID,
					InputResponses: InputResponseMap{"answer": &ElicitResult{Action: "accept", Content: map[string]any{"answer": want}}},
				}); err != nil {
					t.Fatal(err)
				}
				waitTask(t, store.done, "terminal task store update")
				if len(asked) != 0 {
					t.Fatal("FollowTask asked the elicitation handler while its callback was held")
				}
				f.release()
			}
			if got := beforeNextPoll(f, asked, "the elicitation handler to be asked"); got != "answer?" {
				t.Fatalf("the elicitation handler was asked %q, want the task's request", got)
			}
			giveAnswer()
			// The working state between the answer and the result may be
			// skipped.
			snapshot := f.next("the task after its answer")
			if snapshot.Status == TaskStatusWorking {
				snapshot = f.next("the completed task")
			}
			if snapshot.Status != TaskStatusCompleted {
				t.Fatalf("snapshot after the answer = %q, want completed", snapshot.Status)
			}
			endFollow(t, f, want)
			if len(asked) != 0 {
				t.Fatalf("the elicitation handler was asked again: %q", <-asked)
			}
			// No timer fired, so FollowTask stopped each one it made.
			if made, stopped := f.timers(); made == 0 || made != stopped {
				t.Fatalf("FollowTask made %d poll timers and stopped %d", made, stopped)
			}
		})
	}
}

// beforeNextPoll waits for an event that FollowTask must cause before it
// polls again: with a snapshot that asks for input in hand, FollowTask asks
// the client's handler, sends the answer, or fails, before it does anything
// else. A further tasks/get with no such event before it is a failure,
// reported at once: the acknowledgement of its listen makes a FollowTask that
// skipped the step poll.
//
// When the test lags, the event and the poll may both be there. The event is
// reported before FollowTask can poll, so it came first: it is returned and
// the poll is left for polls to count.
func beforeNextPoll[T any](f *followFixture, ch <-chan T, waiting string) T {
	f.t.Helper()
	polledFirst := func() T {
		f.t.Helper()
		select {
		case value := <-ch:
			return value
		default:
		}
		f.t.Fatalf("waiting for %s: FollowTask polled again instead", waiting)
		panic("unreachable")
	}
	if f.sparePolls > 0 {
		return polledFirst()
	}
	guard := taskGuard(f.t)
	defer guard.Stop()
	select {
	case value := <-ch:
		return value
	case <-f.polled:
		f.sparePolls++
		return polledFirst()
	case <-guard.C:
		taskGuardExpired(f.t, "timed out waiting for %s", waiting)
	}
	panic("unreachable")
}

// inputStoredTask makes the stored task ask for input with the given keys.
func inputStoredTask(updated string, keys ...string) func(*DetailedTask) {
	return func(task *DetailedTask) {
		task.Status = TaskStatusInputRequired
		task.LastUpdatedAt = updated
		task.InputRequests = elicitInput(keys...)
	}
}

// FollowTask answers each input request key once. A request stays in the
// task's snapshots until the server has seen its answer, and a later snapshot
// may show it next to a new one: only the new one goes to the handler. The
// answers are sent with tasks/update, one request per snapshot that has
// something new, before FollowTask polls again.
//
// In the second variant the test lags: FollowTask has answered and polled
// again before the test looks for the request and the answer.
func TestFollowTaskAnswersEachInputRequestOnce(t *testing.T) {
	for _, lag := range []bool{false, true} {
		name := "test waiting"
		if lag {
			name = "test behind FollowTask"
		}
		t.Run(name, func(t *testing.T) {
			const id = "asks-twice"
			server, store := newStoredTaskServer(t, id)
			dropTaskNotifications(server)
			changeStoredTask(t, store, id, inputStoredTask("2026-07-28T10:30:01Z", "first"))
			updates, updated := taskEvents[string](t, "tasks/update requests")
			server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
				return func(ctx context.Context, method string, req Request) (Result, error) {
					if params, ok := req.GetParams().(*UpdateTaskParams); ok && method == methodUpdateTask {
						keys := slices.Sorted(maps.Keys(params.InputResponses))
						updated(params.TaskID + ": " + strings.Join(keys, ","))
					}
					return next(ctx, method, req)
				}
			})
			asked, ask := taskEvents[string](t, "elicitation requests")
			client := connectTaskClientWith(t, server, &ClientOptions{
				ElicitationHandler: func(_ context.Context, req *ElicitRequest) (*ElicitResult, error) {
					ask(req.Params.Message)
					return &ElicitResult{Action: "accept"}, nil
				},
			}, nil)
			f := startFollow(t, client, id, false)
			if first := f.next("the first snapshot"); first.Status != TaskStatusInputRequired {
				t.Fatalf("first snapshot status = %q, want input_required", first.Status)
			}
			f.polls(1, "the first poll")
			if lag {
				// FollowTask has asked, answered and polled again before the test
				// looks.
				f.lagUntilPolled()
			}
			if got := beforeNextPoll(f, asked, "the first request at the handler"); got != "first" {
				t.Fatalf("the handler was asked %q, want first", got)
			}
			if got, want := beforeNextPoll(f, updates, "the first tasks/update"), id+": first"; got != want {
				t.Fatalf("tasks/update = %q, want %q", got, want)
			}
			f.polls(1, "the poll after the acknowledgement")
			// The server has not taken the first answer yet, and asks a second
			// question.
			changeStoredTask(t, store, id, inputStoredTask("2026-07-28T10:30:02Z", "first", "second"))
			f.fire()
			if snapshot := f.next("the snapshot with both requests"); len(snapshot.InputRequests) != 2 {
				t.Fatalf("snapshot has requests %v, want both", inputKeys(snapshot))
			}
			if got := followAwait(f, asked, "the second request at the handler"); got != "second" {
				t.Fatalf("the handler was asked %q, want second", got)
			}
			if got, want := followAwait(f, updates, "the second tasks/update"), id+": second"; got != want {
				t.Fatalf("tasks/update = %q, want %q", got, want)
			}
			// The same two requests again, in a newer snapshot: nothing to answer.
			changeStoredTask(t, store, id, func(task *DetailedTask) {
				task.StatusMessage = "still waiting"
				task.LastUpdatedAt = "2026-07-28T10:30:03Z"
			})
			f.fire()
			if snapshot := f.next("the snapshot with a new message"); snapshot.StatusMessage != "still waiting" {
				t.Fatalf("snapshot message = %q", snapshot.StatusMessage)
			}
			changeStoredTask(t, store, id, func(task *DetailedTask) {
				task.InputRequests = nil
				completeStoredTask("answered")(task)
			})
			f.fire()
			if snapshot := f.next("the completed task"); snapshot.Status != TaskStatusCompleted {
				t.Fatalf("snapshot after the poll = %q, want completed", snapshot.Status)
			}
			endFollow(t, f, "answered")
			if len(asked) != 0 || len(updates) != 0 {
				t.Fatalf("after its two answers FollowTask asked the handler %d more times and sent %d more updates", len(asked), len(updates))
			}
		})
	}
}

// A request the client cannot answer ends FollowTask with an error, as it
// ends a multi round-trip tool call.
func TestFollowTaskInputWithoutHandler(t *testing.T) {
	const id = "asks-nobody"
	server, store := newStoredTaskServer(t, id)
	changeStoredTask(t, store, id, inputStoredTask("2026-07-28T10:30:01Z", "first"))
	client := connectTaskClient(t, server, nil)
	other := otherTaskFollower(t, client, id)
	f := startFollow(t, client, id, false)
	if first := f.next("the first snapshot"); first.Status != TaskStatusInputRequired {
		t.Fatalf("first snapshot status = %q, want input_required", first.Status)
	}
	f.polls(1, "the first poll")
	outcome := beforeNextPoll(f, f.followed, "FollowTask's error")
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "does not support elicitation") {
		t.Fatalf("FollowTask = %+v, %v; want an error saying the client does not support elicitation", outcome.task, outcome.err)
	}
	other.mayAnswer(t, "first")
}

// An otherFollower is a second follower of a task on a session.
type otherFollower struct{ follower *taskFollower }

// otherTaskFollower registers a second follower of the task on the session,
// as a second FollowTask call would.
func otherTaskFollower(t *testing.T, session *ClientSession, id string) otherFollower {
	t.Helper()
	follower := session.registerTaskFollower(id)
	t.Cleanup(func() { session.unregisterTaskFollower(id, follower) })
	return otherFollower{follower}
}

// mayAnswer checks that the request key is free for the other follower: a
// FollowTask call that did not get a request answered leaves it to be
// answered.
func (o otherFollower) mayAnswer(t *testing.T, key string) {
	t.Helper()
	if free := o.follower.inputKeys.take(elicitInput(key)); len(free) != 1 {
		t.Fatalf("after a FollowTask call failed to answer request %q, the key is still taken", key)
	}
}

// FollowTask keeps polling after its listen was acknowledged: a change whose
// notification is lost after the poll that follows the acknowledgement is
// found by the poll timer. (TestClientFollowTaskGetsAfterSubscriptionAck is
// the variant in which the change comes before that poll is answered.)
func TestClientFollowTaskPollsAfterSubscription(t *testing.T) {
	const id = "lost-notification"
	server, store := newStoredTaskServer(t, id)
	dropTaskNotifications(server)
	client := connectTaskClient(t, server, nil)
	f := startFollow(t, client, id, false)
	if first := f.next("the first snapshot"); first.Status != TaskStatusWorking {
		t.Fatalf("first snapshot status = %q, want working", first.Status)
	}
	f.subscribed()
	changeStoredTask(t, store, id, completeStoredTask("recovered by polling"))
	f.fire()
	if snapshot := f.next("the completed task"); snapshot.Status != TaskStatusCompleted {
		t.Fatalf("snapshot after the poll = %q, want completed", snapshot.Status)
	}
	endFollow(t, f, "recovered by polling")
}

// A change made after FollowTask's first poll and before its listen is
// acknowledged, whose notification is lost, is found by the poll that follows
// the acknowledgement: the test holds that poll at the server until the
// change is made, and never fires the poll timer.
func TestClientFollowTaskGetsAfterSubscriptionAck(t *testing.T) {
	const id = "ack-snapshot"
	server, store := newStoredTaskServer(t, id)
	dropTaskNotifications(server)
	hold, release := taskTestGate()
	reached, reach := taskEvents[int32](t, "tasks/get requests")
	var gets atomic.Int32
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if method == methodGetTask {
				n := gets.Add(1)
				reach(n)
				if n == 2 {
					<-hold
				}
			}
			return next(ctx, method, req)
		}
	})
	client := connectTaskClient(t, server, nil)
	t.Cleanup(release)
	f := startFollow(t, client, id, false)
	if first := f.next("the first snapshot"); first.Status != TaskStatusWorking {
		t.Fatalf("first snapshot status = %q, want working", first.Status)
	}
	for want := int32(1); want <= 2; want++ {
		if got := followAwait(f, reached, "a tasks/get at the server"); got != want {
			t.Fatalf("tasks/get number %d at the server, want %d", got, want)
		}
	}
	changeStoredTask(t, store, id, completeStoredTask("from acknowledgement snapshot"))
	release()
	if snapshot := f.next("the completed task"); snapshot.Status != TaskStatusCompleted {
		t.Fatalf("snapshot after the acknowledgement = %q, want completed", snapshot.Status)
	}
	endFollow(t, f, "from acknowledgement snapshot")
}

// Against a server that never acknowledges a task subscription FollowTask
// polls: one tick, one more tasks/get.
func TestClientFollowTaskPollingFallback(t *testing.T) {
	server := NewServer(testImpl, nil)
	var calls atomic.Int32
	if err := AddReceivingCustomMethod(server, methodGetTask, func(ctx context.Context, session *ServerSession, params *GetTaskParams) (*DetailedTask, error) {
		req := &ServerRequest[*GetTaskParams]{Session: session, Params: params}
		if !requestUsesTasks(req) {
			return nil, missingTasksCapabilityError()
		}
		task := &DetailedTask{Task: Task{
			TaskID:        params.TaskID,
			Status:        TaskStatusWorking,
			CreatedAt:     "2026-07-28T10:30:00Z",
			LastUpdatedAt: "2026-07-28T10:30:00Z",
		}}
		if calls.Add(1) > 1 {
			completeStoredTask("from polling")(task)
		}
		return task, nil
	}); err != nil {
		t.Fatal(err)
	}
	client := connectTaskClient(t, server, nil)
	f := startFollow(t, client, "poll-task", false)
	if first := f.next("the first snapshot"); first.Status != TaskStatusWorking {
		t.Fatalf("first snapshot status = %q, want working", first.Status)
	}
	f.fire()
	if snapshot := f.next("the completed task"); snapshot.Status != TaskStatusCompleted {
		t.Fatalf("snapshot after the poll = %q, want completed", snapshot.Status)
	}
	endFollow(t, f, "from polling")
	if got := calls.Load(); got != 2 {
		t.Fatalf("tasks/get calls = %d, want the first one and one poll", got)
	}
	// FollowTask has returned, so it makes no more timers: the one it made
	// is the one that fired.
	if made, _ := f.timers(); made != 1 {
		t.Fatalf("FollowTask made %d poll timers, want 1", made)
	}
}

// A notification older than the state FollowTask already has is dropped. In
// the second variant the follower lags: a newer and then a stale notification
// arrive while FollowTask is held in its callback, and it goes on with the
// newer one.
func TestClientFollowTaskDropsStaleNotifications(t *testing.T) {
	const id = "stale-notification"
	notification := func(status TaskStatus, message, updated string) *DetailedTask {
		return &DetailedTask{Task: Task{TaskID: id, Status: status, StatusMessage: message, LastUpdatedAt: updated}}
	}
	newer := notification(TaskStatusWorking, "newer", "2026-07-28T10:30:02Z")
	stale := notification(TaskStatusWorking, "stale", "2026-07-28T10:30:01Z")
	final := notification(TaskStatusCompleted, "final", "2026-07-28T10:30:03Z")
	for _, lag := range []bool{false, true} {
		name := "follower waiting"
		if lag {
			name = "follower held in its callback"
		}
		t.Run(name, func(t *testing.T) {
			ctx := taskTestContext(t)
			server, _ := newStoredTaskServer(t, id)
			client := connectTaskClient(t, server, nil)
			f := startFollow(t, client, id, lag)
			if first := f.next("the first snapshot"); first.StatusMessage != "initial" {
				t.Fatalf("first snapshot = %q, want initial", first.StatusMessage)
			}
			if lag {
				client.deliverTaskNotification(ctx, newer)
				client.deliverTaskNotification(ctx, stale)
				f.release()
			} else {
				f.subscribed()
				client.deliverTaskNotification(ctx, newer)
			}
			if got := f.next("the newer snapshot"); got.StatusMessage != "newer" {
				t.Fatalf("second snapshot = %q, want newer", got.StatusMessage)
			}
			if !lag {
				client.deliverTaskNotification(ctx, stale)
			}
			client.deliverTaskNotification(ctx, final)
			if got := f.next("the terminal snapshot"); got.StatusMessage != "final" {
				t.Fatalf("snapshot = %q, want final", got.StatusMessage)
			}
			outcome := f.end("FollowTask")
			if outcome.err != nil {
				t.Fatal(outcome.err)
			}
			if outcome.task.StatusMessage != "final" {
				t.Fatalf("final snapshot = %+v", outcome.task)
			}
		})
	}
}

// A poll that finds the task still running restarts the poll timer: two
// changes whose notifications never arrive are both found by polling. In the
// second variant the follower lags: the second change is made while
// FollowTask is held in its callback with the first.
func TestClientFollowTaskRestartsPolling(t *testing.T) {
	for _, lag := range []bool{false, true} {
		name := "follower waiting"
		if lag {
			name = "follower held in its callback"
		}
		t.Run(name, func(t *testing.T) {
			const id = "two-lost-notifications"
			server, store := newStoredTaskServer(t, id)
			dropTaskNotifications(server)
			client := connectTaskClient(t, server, nil)
			f := startFollow(t, client, id, false)
			if first := f.next("the first snapshot"); first.StatusMessage != "initial" {
				t.Fatalf("first snapshot = %q, want initial", first.StatusMessage)
			}
			f.subscribed()
			changeStoredTask(t, store, id, func(task *DetailedTask) {
				task.StatusMessage = "first notification lost"
				task.LastUpdatedAt = "2026-07-28T10:30:01Z"
			})
			if lag {
				f.park()
			}
			f.fire()
			if second := f.next("the first change"); second.StatusMessage != "first notification lost" {
				t.Fatalf("second snapshot = %q, want the first change", second.StatusMessage)
			}
			changeStoredTask(t, store, id, completeStoredTask("second notification lost"))
			if lag {
				f.release()
			}
			f.fire()
			if third := f.next("the completed task"); third.Status != TaskStatusCompleted {
				t.Fatalf("third snapshot = %q, want completed", third.Status)
			}
			endFollow(t, f, "second notification lost")
		})
	}
}

// An answer the server does not take ends FollowTask with the error of the
// tasks/update, before it polls again.
func TestFollowTaskUpdateRefused(t *testing.T) {
	const id = "refuses-answers"
	server, store := newStoredTaskServer(t, id)
	dropTaskNotifications(server)
	changeStoredTask(t, store, id, inputStoredTask("2026-07-28T10:30:01Z", "first"))
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if method == methodUpdateTask {
				return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "answers refused"}
			}
			return next(ctx, method, req)
		}
	})
	client := connectTaskClientWith(t, server, &ClientOptions{
		ElicitationHandler: func(context.Context, *ElicitRequest) (*ElicitResult, error) {
			return &ElicitResult{Action: "accept"}, nil
		},
	}, nil)
	other := otherTaskFollower(t, client, id)
	f := startFollow(t, client, id, false)
	if first := f.next("the first snapshot"); first.Status != TaskStatusInputRequired {
		t.Fatalf("first snapshot status = %q, want input_required", first.Status)
	}
	f.polls(1, "the first poll")
	outcome := beforeNextPoll(f, f.followed, "FollowTask's error")
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "answers refused") {
		t.Fatalf("FollowTask = %+v, %v; want the error of the tasks/update", outcome.task, outcome.err)
	}
	other.mayAnswer(t, "first")
}

// Two FollowTask calls that follow one task on one session ask the client's
// handler for each input request once, and send one answer: the second call
// sees the request while the first is still at the handler, and goes on
// without asking.
func TestFollowTaskTwoFollowersAskOnce(t *testing.T) {
	const id = "asked-once"
	server, store := newStoredTaskServer(t, id)
	dropTaskNotifications(server)
	changeStoredTask(t, store, id, inputStoredTask("2026-07-28T10:30:01Z", "first"))
	updates, updated := taskEvents[string](t, "tasks/update requests")
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if params, ok := req.GetParams().(*UpdateTaskParams); ok && method == methodUpdateTask {
				updated(strings.Join(slices.Sorted(maps.Keys(params.InputResponses)), ","))
			}
			return next(ctx, method, req)
		}
	})
	asked, ask := taskEvents[string](t, "elicitation requests")
	answer, giveAnswer := taskTestGate()
	client := connectTaskClientWith(t, server, &ClientOptions{
		ElicitationHandler: func(ctx context.Context, req *ElicitRequest) (*ElicitResult, error) {
			ask(req.Params.Message)
			select {
			case <-answer:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &ElicitResult{Action: "accept"}, nil
		},
	}, nil)
	t.Cleanup(giveAnswer)

	first := startFollow(t, client, id, false)
	if snapshot := first.next("the first follower's first snapshot"); snapshot.Status != TaskStatusInputRequired {
		t.Fatalf("first snapshot status = %q, want input_required", snapshot.Status)
	}
	first.polls(1, "the first follower's first poll")
	if got := beforeNextPoll(first, asked, "the request at the handler"); got != "first" {
		t.Fatalf("the handler was asked %q, want first", got)
	}
	// The first follower is held at the handler. The second sees the same
	// request, and polls again when its listen is acknowledged: a handler
	// asked before that poll was asked twice.
	second := startFollow(t, client, id, false)
	if snapshot := second.next("the second follower's first snapshot"); snapshot.Status != TaskStatusInputRequired {
		t.Fatalf("second follower's first snapshot status = %q, want input_required", snapshot.Status)
	}
	second.polls(1, "the second follower's first poll")
	guard := taskGuard(t)
	defer guard.Stop()
	select {
	case <-second.polled:
	case again := <-asked:
		t.Fatalf("the handler was asked %q a second time, by the second FollowTask call", again)
	case outcome := <-second.followed:
		t.Fatalf("the second FollowTask returned %+v, %v", outcome.task, outcome.err)
	case <-guard.C:
		taskGuardExpired(t, "timed out waiting for the second follower's poll after its acknowledgement")
	}
	giveAnswer()
	if got := waitTask(t, updates, "the answer"); got != "first" {
		t.Fatalf("tasks/update answered %q, want first", got)
	}
	// Both go on following; neither asks or answers again.
	first.cancel()
	second.cancel()
	for _, f := range []*followFixture{first, second} {
		if outcome := waitTask(t, f.followed, "FollowTask to return"); !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("FollowTask = %+v, %v; want its context's error", outcome.task, outcome.err)
		}
	}
	if len(asked) != 0 || len(updates) != 0 {
		t.Fatalf("the handler was asked %d more times and %d more answers were sent", len(asked), len(updates))
	}
}

// Two FollowTask calls follow one task on one session, and the one that took
// an input request does not get it answered: its handler fails, FollowTask
// returns the error and gives the key back. The other call saw the request
// while the key was taken, and every later poll shows it the same snapshot.
// It asks for the request again at its next poll, and then answers it or
// ends with the handler's error: it does not go on polling a task that
// waits for an answer nobody will give.
func TestFollowTaskTwoFollowersKeyGivenBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		// secondFails makes the handler fail the second time too.
		secondFails bool
		// lag makes the test look only after FollowTask has gone on.
		lag bool
	}{
		{name: "second answers"},
		{name: "second answers, test behind FollowTask", lag: true},
		{name: "second cannot answer either", secondFails: true},
		{name: "second cannot answer either, test behind FollowTask", secondFails: true, lag: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id = "given-back"
			server, store := newStoredTaskServer(t, id)
			dropTaskNotifications(server)
			changeStoredTask(t, store, id, inputStoredTask("2026-07-28T10:30:01Z", "first"))
			updates, updated := taskEvents[string](t, "tasks/update requests")
			server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
				return func(ctx context.Context, method string, req Request) (Result, error) {
					if params, ok := req.GetParams().(*UpdateTaskParams); ok && method == methodUpdateTask {
						updated(strings.Join(slices.Sorted(maps.Keys(params.InputResponses)), ","))
					}
					return next(ctx, method, req)
				}
			})
			asked, ask := taskEvents[string](t, "elicitation requests")
			answer, giveAnswer := taskTestGate()
			var calls atomic.Int64
			client := connectTaskClientWith(t, server, &ClientOptions{
				ElicitationHandler: func(ctx context.Context, req *ElicitRequest) (*ElicitResult, error) {
					n := calls.Add(1)
					ask(req.Params.Message)
					if n == 1 {
						select {
						case <-answer:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
						return nil, errors.New("the user was away")
					}
					if tc.secondFails {
						return nil, errors.New("the user is still away")
					}
					return &ElicitResult{Action: "accept"}, nil
				},
			}, nil)
			t.Cleanup(giveAnswer)

			first := startFollow(t, client, id, false)
			first.next("the first follower's first snapshot")
			first.polls(1, "the first follower's first poll")
			if got := beforeNextPoll(first, asked, "the request at the handler"); got != "first" {
				t.Fatalf("the handler was asked %q, want first", got)
			}
			// The first follower is held at the handler. The second sees
			// the request, whose key is taken, and goes on: it has its
			// first poll timer and has polled after its acknowledgement.
			second := startFollow(t, client, id, false)
			second.next("the second follower's first snapshot")
			second.polls(2, "the second follower's first poll and the poll after its acknowledgement")
			followAwait(second, second.delays, "the second follower's first poll timer")
			if n := calls.Load(); n != 1 {
				t.Fatalf("the handler was asked %d times while the first follower held the request, want once", n)
			}
			// The first follower's handler fails: its FollowTask returns
			// the error, and the key is free.
			giveAnswer()
			if outcome := waitTask(t, first.followed, "the first FollowTask"); outcome.err == nil || !strings.Contains(outcome.err.Error(), "the user was away") {
				t.Fatalf("the first FollowTask = %+v, %v; want the handler's error", outcome.task, outcome.err)
			}

			// The second follower's next poll shows the snapshot it has.
			// What it does with it is done before it makes its next poll
			// timer, or returns: so once either has happened, a handler
			// that was not asked again was passed over.
			if !tc.secondFails {
				second.fire()
			}
			if tc.secondFails {
				// The second follower may have had its poll's answer in hand
				// when the key came back: it then asks at once and returns
				// the handler's error without another poll. Both orders are
				// right, so a return before the tick is not a failure here.
				second.fireOrReturned()
				guard := taskGuard(t)
				defer guard.Stop()
				wentOn := func() {
					t.Helper()
					t.Fatalf("the second FollowTask polled and went on polling; the handler was asked %d times, want twice: the request the first call gave back has nobody to answer it", calls.Load())
				}
				if tc.lag {
					// Look only once FollowTask has returned.
					select {
					case <-second.returned:
					case <-second.delays:
						wentOn()
					case <-guard.C:
						taskGuardExpired(t, "timed out waiting for the second FollowTask to return")
					}
				}
				select {
				case outcome := <-second.followed:
					if outcome.err == nil || !strings.Contains(outcome.err.Error(), "the user is still away") {
						t.Fatalf("the second FollowTask = %+v, %v; want the handler's error", outcome.task, outcome.err)
					}
				case <-second.delays:
					wentOn()
				case <-guard.C:
					taskGuardExpired(t, "timed out waiting for the second FollowTask to return")
				}
				if n := calls.Load(); n != 2 {
					t.Fatalf("the handler was asked %d times, want twice", n)
				}
				if len(updates) != 0 {
					t.Fatalf("%d answers were sent, want none", len(updates))
				}
				return
			}
			followAwait(second, second.delays, "the second follower's poll timer after the poll")
			if tc.lag {
				// One more poll: the second follower is two polls on.
				second.fire()
				followAwait(second, second.delays, "the second follower's poll timer after a further poll")
			}
			if n := calls.Load(); n != 2 {
				t.Fatalf("the second FollowTask polled and went on polling; the handler was asked %d times, want twice: the request the first call gave back has nobody to answer it", n)
			}
			if got := waitTask(t, asked, "the second request at the handler"); got != "first" {
				t.Fatalf("the handler was asked %q the second time, want first", got)
			}
			if got := waitTask(t, updates, "the answer"); got != "first" {
				t.Fatalf("tasks/update answered %q, want first", got)
			}
			second.cancel()
			if outcome := waitTask(t, second.followed, "the second FollowTask to return"); !errors.Is(outcome.err, context.Canceled) {
				t.Fatalf("the second FollowTask = %+v, %v; want its context's error", outcome.task, outcome.err)
			}
			if len(asked) != 0 || len(updates) != 0 {
				t.Fatalf("the handler was asked %d more times and %d more answers were sent", len(asked), len(updates))
			}
		})
	}
}

// A task that is terminal at FollowTask's first poll ends it there: with one
// snapshot, no poll timer and no further poll.
func TestFollowTaskOfTerminalTask(t *testing.T) {
	const id = "already-done"
	server, store := newStoredTaskServer(t, id)
	changeStoredTask(t, store, id, completeStoredTask("done before"))
	f := startFollow(t, connectTaskClient(t, server, nil), id, false)
	if snapshot := f.next("the terminal snapshot"); snapshot.Status != TaskStatusCompleted {
		t.Fatalf("first snapshot = %q, want completed", snapshot.Status)
	}
	endFollow(t, f, "done before")
	if made, _ := f.timers(); made != 0 || len(f.polled) != 1 {
		t.Fatalf("FollowTask made %d poll timers and %d tasks/get calls; want none and one", made, len(f.polled))
	}
}

// Each FollowTask call that follows a task gets its own copy of a snapshot a
// notification brought.
func TestTaskNotificationCopiedForEachFollower(t *testing.T) {
	session := connectTaskMemory(t, newTaskServerWithTask(t, "task-1"), NewClient(testImpl, nil))
	a, b := session.registerTaskFollower("task-1"), session.registerTaskFollower("task-1")
	defer session.unregisterTaskFollower("task-1", a)
	defer session.unregisterTaskFollower("task-1", b)
	sent := snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:01Z", "one")
	session.deliverTaskNotification(taskTestContext(t), sent)
	gotA, errA := a.take()
	gotB, errB := b.take()
	if errA != nil || errB != nil || gotA == nil || gotB == nil {
		t.Fatalf("followers took %+v, %v and %+v, %v; want the snapshot twice", gotA, errA, gotB, errB)
	}
	if gotA == gotB || gotA == sent || gotB == sent {
		t.Fatal("two followers were given the same DetailedTask value")
	}
	if gotA.StatusMessage != "one" || gotB.StatusMessage != "one" {
		t.Fatalf("followers took %q and %q", gotA.StatusMessage, gotB.StatusMessage)
	}
}
