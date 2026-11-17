// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// A taskSend applies to one request: its method, its session and its params.
// Each of the three alone keeps another request, made with the same context,
// on the standard path.
func TestTaskSendMatchesOneRequest(t *testing.T) {
	session, other := &ClientSession{}, &ClientSession{}
	params, otherParams := &GetTaskParams{TaskID: "task-1"}, &GetTaskParams{TaskID: "task-1"}
	ctx := withTaskSend(context.Background(), taskSend{
		session: session,
		method:  methodGetTask,
		owns:    ownsParams(params),
		send:    func(context.Context, Request) (Result, error) { return nil, nil },
	})
	tests := []struct {
		name    string
		ctx     context.Context
		method  string
		session *ClientSession
		params  *GetTaskParams
		want    bool
	}{
		{"the request", ctx, methodGetTask, session, params, true},
		{"no value in the context", context.Background(), methodGetTask, session, params, false},
		{"another method", ctx, methodCancelTask, session, params, false},
		{"another session", ctx, methodGetTask, other, params, false},
		{"other params", ctx, methodGetTask, session, otherParams, false},
	}
	for _, test := range tests {
		got := taskSendFromContext(test.ctx, test.method, newClientRequest(test.session, Params(test.params))) != nil
		if got != test.want {
			t.Errorf("%s: the task terminal applies = %v, want %v", test.name, got, test.want)
		}
	}
}

// The Mcp-Name of a task request is written for that request alone. The
// context a request is written with is also the context its cancellation
// notification is written with; that notification gets no Mcp-Name.
func TestTaskNameOnlyOnItsRequest(t *testing.T) {
	ctx := withTaskName(context.Background(), methodGetTask, "task-1")
	for _, test := range []struct {
		method string
		want   string
	}{
		{methodGetTask, "task-1"},
		{notificationCancelled, ""},
		{methodCancelTask, ""},
	} {
		header := http.Header{protocolVersionHeader: {protocolVersion20260728}}
		setStandardHeaders(ctx, header, &jsonrpc.Request{Method: test.method, Params: json.RawMessage(`{}`)})
		if got := header.Get(nameHeader); got != test.want {
			t.Errorf("%s written with the context of tasks/get: Mcp-Name %q, want %q", test.method, got, test.want)
		}
	}
}

// An opted-in request declares the extension next to the capabilities the
// caller declared, in a copy: the caller's value is not written to.
func TestWithTasksCapabilityCopies(t *testing.T) {
	declared := &clientCapabilitiesV2{ClientCapabilities: ClientCapabilities{Extensions: map[string]any{"example.com/other": map[string]any{}}}}
	meta := Meta{MetaKeyClientCapabilities: declared, "example.com/key": "v"}
	got := withTasksCapability(&ClientSession{}, meta)
	caps, ok := got[MetaKeyClientCapabilities].(*clientCapabilitiesV2)
	if !ok || len(caps.Extensions) != 2 || caps.Extensions[ExtensionTasks] == nil || got["example.com/key"] != "v" {
		t.Fatalf("meta = %v, capabilities = %+v; want the caller's extension and the tasks extension", got, caps)
	}
	if len(declared.Extensions) != 1 || meta[MetaKeyClientCapabilities] != any(declared) || len(meta) != 2 {
		t.Fatalf("the caller's capabilities were changed: %+v, meta %v", declared, meta)
	}
}

// While FollowTask runs, an acknowledgement whose taskIds is not a list of
// strings is delivered as it is without the extension: it decodes without an
// error and acknowledges no task.
func TestTaskFollowAckWithMalformedTaskIDs(t *testing.T) {
	ack := taskFollowMethodInfos()[notificationSubscriptionsAck]
	for _, raw := range []string{
		`{"notifications":{"toolsListChanged":true,"taskIds":7}}`,
		`{"notifications":{"toolsListChanged":true,"taskIds":[7]}}`,
		`{"notifications":{"toolsListChanged":true,"taskIds":"task-1"}}`,
	} {
		params, err := ack.unmarshalParams(json.RawMessage(raw))
		if err != nil {
			t.Errorf("%s: %v", raw, err)
			continue
		}
		acked := params.(*SubscriptionsAcknowledgedParams)
		if !acked.Notifications.ToolsListChanged || acked.Notifications.TaskIDs != nil {
			t.Errorf("%s decoded as %+v, want the standard fields and no task ids", raw, acked.Notifications)
		}
	}
	params, err := ack.unmarshalParams(json.RawMessage(`{"notifications":{"taskIds":["task-1"]}}`))
	if err != nil || !slices.Equal(params.(*SubscriptionsAcknowledgedParams).Notifications.TaskIDs, []string{"task-1"}) {
		t.Errorf("a well-formed list decoded as %+v, %v", params, err)
	}
}

// Sending middleware that answers a task request with a result of another
// type makes the helper return an error.
func TestTaskHelperWrongResultTypeFromMiddleware(t *testing.T) {
	client := NewClient(testImpl, nil)
	client.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if method == methodGetTask {
				return &ListToolsResult{}, nil
			}
			return next(ctx, method, req)
		}
	})
	session := connectTaskMemory(t, newTaskServerWithTask(t, "task-1"), client)
	task, err := session.GetTask(taskTestContext(t), &GetTaskParams{TaskID: "task-1"})
	if err == nil || !strings.Contains(err.Error(), "sending middleware returned") {
		t.Fatalf("GetTask = %+v, %v; want an error about the middleware's result", task, err)
	}
}

// The Task accessor returns a copy, _meta included.
func TestCallToolResultTaskCopiesMeta(t *testing.T) {
	result := &CallToolResult{task: &Task{TaskID: "task-1", Meta: Meta{"example.com/key": "v"}}}
	task, ok := result.Task()
	if !ok {
		t.Fatal("no task")
	}
	task.Meta["example.com/key"] = "changed"
	task.Meta["example.com/new"] = true
	if again, _ := result.Task(); len(again.Meta) != 1 || again.Meta["example.com/key"] != "v" {
		t.Fatalf("writing to the returned _meta changed the result's task: %v", again.Meta)
	}
}

// A task carries the retention and the poll interval of its TaskServer, and
// neither when the server has none.
func TestTaskCarriesRetentionAndPollInterval(t *testing.T) {
	hour, interval := int64(3600000), int64(1500)
	for _, test := range []struct {
		name     string
		options  *TaskServerOptions
		ttl      *int64
		interval *int64
	}{
		{"none", nil, nil, nil},
		{"both", &TaskServerOptions{Retention: time.Hour, DefaultPollInterval: 1500 * time.Millisecond}, &hour, &interval},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := taskTestContext(t)
			server := NewServer(testImpl, nil)
			tasks, err := NewTaskServer(server, test.options)
			if err != nil {
				t.Fatal(err)
			}
			tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
				func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
					return &CallToolResult{}, nil
				})
			session := connectTaskMemory(t, server, NewClient(testImpl, nil))
			result, err := session.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
			if err != nil {
				t.Fatal(err)
			}
			handle := taskHandle(t, result)
			snapshot, err := session.GetTask(ctx, &GetTaskParams{TaskID: handle.TaskID})
			if err != nil {
				t.Fatal(err)
			}
			show := func(p *int64) string {
				if p == nil {
					return "none"
				}
				return fmt.Sprint(*p)
			}
			for where, task := range map[string]*Task{"the handle": handle, "tasks/get": &snapshot.Task} {
				if show(task.TTLMs) != show(test.ttl) || show(task.PollIntervalMs) != show(test.interval) {
					t.Errorf("%s: ttlMs %s, pollIntervalMs %s; want %s and %s", where, show(task.TTLMs), show(task.PollIntervalMs), show(test.ttl), show(test.interval))
				}
			}
		})
	}
}

// A task that ends while it waits for input ends without input requests,
// whether its handler returned a result or an error.
func TestTaskEndsWithoutInputRequests(t *testing.T) {
	for _, test := range []struct {
		name   string
		fail   bool
		status TaskStatus
	}{
		{"completed", false, TaskStatusCompleted},
		{"failed", true, TaskStatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := taskTestContext(t)
			server := NewServer(testImpl, nil)
			store := newObservedTaskStore()
			tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
			if err != nil {
				t.Fatal(err)
			}
			handlerContext, stopHandlers := taskTestStop()
			finish, finishTask := taskTestGate()
			tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
				func(ctx context.Context, _ *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
					ctx = handlerContext(ctx)
					// The handler asks for input on the side and ends without
					// waiting for the answer.
					go taskContext.RequireInput(ctx, elicitInput("unanswered"))
					select {
					case <-finish:
					case <-ctx.Done():
					}
					if test.fail {
						return nil, errors.New("gave up")
					}
					return &CallToolResult{}, nil
				})
			session := connectTaskMemory(t, server, NewClient(testImpl, nil))
			t.Cleanup(stopHandlers)
			t.Cleanup(finishTask)
			result, err := session.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
			if err != nil {
				t.Fatal(err)
			}
			id := taskHandle(t, result).TaskID
			if snapshot := waitTask(t, store.snapshots, "the task asking for input"); snapshot.Status != TaskStatusInputRequired || len(snapshot.InputRequests) != 1 {
				t.Fatalf("snapshot = %s with %d requests, want input_required with one", snapshot.Status, len(snapshot.InputRequests))
			}
			finishTask()
			waitTask(t, store.done, "the end of the task")
			final, err := session.GetTask(ctx, &GetTaskParams{TaskID: id})
			if err != nil {
				t.Fatal(err)
			}
			if final.Status != test.status || len(final.InputRequests) != 0 {
				t.Fatalf("the task ended as %s with input requests %v, want %s with none", final.Status, inputKeys(final), test.status)
			}
		})
	}
}

// RequireInput refuses a call that has no task, a call without requests, and
// a second call while the first is waiting.
func TestTaskRequireInputRefusals(t *testing.T) {
	ctx := taskTestContext(t)
	if _, err := (&TaskContext{}).RequireInput(ctx, elicitInput("only")); err == nil || !strings.Contains(err.Error(), "has no task") {
		t.Errorf("RequireInput on a synchronous call = %v, want an error saying there is no task", err)
	}
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	taskContext := &TaskContext{server: tasks, taskID: "no-such-task", runtime: &taskRuntime{usedInputKeys: make(map[string]struct{}), changed: make(chan struct{})}}
	if _, err := taskContext.RequireInput(ctx, InputRequestMap{}); err == nil || !strings.Contains(err.Error(), "at least one request") {
		t.Errorf("RequireInput without requests = %v, want an error asking for a request", err)
	}
	runtime := &taskRuntime{usedInputKeys: make(map[string]struct{})}
	if err := runtime.beginInput(elicitInput("first")); err != nil {
		t.Fatal(err)
	}
	if err := runtime.beginInput(elicitInput("second")); err == nil || !strings.Contains(err.Error(), "outstanding") {
		t.Errorf("a second RequireInput while the first waits = %v, want an error", err)
	}
}

// FollowTask ends with the error of its callback. The task has ended, so a
// FollowTask that dropped the error would return the task at once.
func TestFollowTaskCallbackError(t *testing.T) {
	server, store := newStoredTaskServer(t, "task-1")
	changeStoredTask(t, store, "task-1", completeStoredTask("done"))
	session := connectTaskMemory(t, server, NewClient(testImpl, nil))
	stop := errors.New("the callback has seen enough")
	calls := 0
	task, err := session.FollowTask(taskTestContext(t), "task-1", func(context.Context, *DetailedTask) error {
		calls++
		return stop
	})
	if !errors.Is(err, stop) || task != nil || calls != 1 {
		t.Fatalf("FollowTask = %+v, %v after %d callback calls; want the callback's error after one", task, err, calls)
	}
}

// FollowTask ends when its context ends, without waiting for a notification
// or for the next poll: the poll timer is never fired.
func TestFollowTaskContextCancelled(t *testing.T) {
	const id = "never-ends"
	server, _ := newStoredTaskServer(t, id)
	session := connectTaskClient(t, server, nil)
	f := startFollow(t, session, id, false)
	f.next("the first snapshot")
	// FollowTask has polled after the acknowledgement: it now waits.
	f.subscribed()
	f.cancel()
	if outcome := f.end("FollowTask's return"); !errors.Is(outcome.err, context.Canceled) || outcome.task != nil {
		t.Fatalf("FollowTask = %+v, %v; want the context's error", outcome.task, outcome.err)
	}
}

// When FollowTask returns it ends its listen: the server's handler of that
// request returns, over an in-memory connection and over Streamable HTTP.
func TestFollowTaskEndsItsListen(t *testing.T) {
	for _, transport := range []string{"memory", "http"} {
		t.Run(transport, func(t *testing.T) {
			ctx := taskTestContext(t)
			server := NewServer(testImpl, nil)
			tasks, err := NewTaskServer(server, nil)
			if err != nil {
				t.Fatal(err)
			}
			tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
				func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
					return &CallToolResult{}, nil
				})
			returned := taskListenReturns(t, server)
			client := NewClient(testImpl, nil)
			var session *ClientSession
			if transport == "memory" {
				session = connectTaskMemory(t, server, client)
			} else {
				session, _ = connectTaskHTTP(t, server, client)
			}
			result, err := session.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
			if err != nil {
				t.Fatal(err)
			}
			final, err := session.FollowTask(ctx, taskHandle(t, result).TaskID, nil)
			if err != nil || !final.Status.Terminal() {
				t.Fatalf("FollowTask = %+v, %v", final, err)
			}
			waitTaskListenReturned(t, returned)
		})
	}
}

// MemoryTaskStore refuses to create a task twice and reports a task it cannot
// delete.
func TestMemoryTaskStoreCreateAndDelete(t *testing.T) {
	ctx := taskTestContext(t)
	store := NewMemoryTaskStore()
	first := &DetailedTask{Task: Task{TaskID: "task-1", Status: TaskStatusWorking, StatusMessage: "first"}}
	if err := store.Create(ctx, "scope", first); err != nil {
		t.Fatal(err)
	}
	second := &DetailedTask{Task: Task{TaskID: "task-1", Status: TaskStatusWorking, StatusMessage: "second"}}
	if err := store.Create(ctx, "scope", second); err == nil {
		t.Error("creating a task with an id in use succeeded")
	}
	if got, err := store.Get(ctx, "scope", "task-1"); err != nil || got.StatusMessage != "first" {
		t.Errorf("after the refused create the store has %+v, %v; want the first task", got, err)
	}
	if err := store.Create(ctx, "other-scope", second); err != nil {
		t.Errorf("the same id in another scope: %v", err)
	}
	if err := store.Delete(ctx, "scope", "no-such-task"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("deleting a task that does not exist = %v, want ErrTaskNotFound", err)
	}
	if err := store.Delete(ctx, "scope", "task-1"); err != nil {
		t.Errorf("deleting a task: %v", err)
	}
	if err := store.Delete(ctx, "scope", "task-1"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("deleting a task twice = %v, want ErrTaskNotFound", err)
	}
}

// A notification that cannot be sent is dropped; the subscription goes on
// with the other tasks and with later changes of the same task.
func TestTaskDeliveryGoesOnAfterFailedSend(t *testing.T) {
	f := newDeliveryFixture(t)
	failed, fail := taskEvents[string](t, "failed sends")
	failOnce := true
	f.server.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			// Only the listen's goroutine sends task notifications.
			if task, ok := req.GetParams().(*DetailedTask); ok && failOnce {
				failOnce = false
				fail(task.TaskID)
				return nil, errors.New("the fixture refuses this send")
			}
			return next(ctx, method, req)
		}
	})
	peer := f.peer()
	first, second := f.startTask(peer, "a"), f.startTask(peer, "b")
	listen := f.listen(peer, taskIDsJSON(first, second))
	f.readAck(peer)
	// The first task's state is refused; the second task's state follows.
	if task := f.readTask(peer, "the second task's state"); task.TaskID != second {
		t.Fatalf("the first notification is about %s, want the second task %s", task.TaskID, second)
	}
	if got := await(f, failed, "the refused send"); got != first {
		t.Fatalf("the refused send was about %s, want the first task %s", got, first)
	}
	f.step("a", setTaskMessage("changed"))
	if task := f.readTask(peer, "the first task's change"); task.TaskID != first || task.StatusMessage != "changed" {
		t.Fatalf("notification = %s %q, want the first task's change", task.TaskID, task.StatusMessage)
	}
	f.endListen(peer, listen)
	f.noViolation()
}

// A task the store no longer has when its state is to be sent is no longer
// followed; the rest of the subscription goes on.
func TestTaskDeliveryDropsMissingTask(t *testing.T) {
	f := newDeliveryFixture(t)
	peer := f.peer()
	gone, kept := f.startTask(peer, "a"), f.startTask(peer, "b")
	// Both tasks exist when the listen subscribes; the first is deleted
	// while the acknowledgement is held, before its state is read.
	f.ackGate.arm()
	listen := f.listen(peer, taskIDsJSON(gone, kept))
	await(f, f.ackGate.held, "the held acknowledgement")
	if got := f.subscribedTasks(); len(got) != 2 {
		t.Fatalf("subscribed tasks = %v, want both", got)
	}
	if err := f.tasks.opts.Store.Delete(taskTestContext(t), "", gone); err != nil {
		t.Fatal(err)
	}
	f.ackGate.open()
	if ids := f.readAck(peer); len(ids) != 2 {
		t.Fatalf("acknowledged tasks = %v, want both", ids)
	}
	// The subscription handles its tasks in order, in one goroutine: when
	// the second task's state is here, the first has been dealt with.
	if task := f.readTask(peer, "the remaining task's state"); task.TaskID != kept {
		t.Fatalf("notification about %s, want %s", task.TaskID, kept)
	}
	if got := f.subscribedTasks(); !slices.Equal(got, []string{kept}) {
		t.Fatalf("subscribed tasks after one was deleted = %v, want only %s", got, kept)
	}
	f.endListen(peer, listen)
	f.noViolation()
}

// A subscription sends nothing once its listen has ended. The lag is held:
// the subscription has read a task's state and is parked before it sends it;
// the listen is cancelled, and its context has ended, before the read
// returns. The state is not sent, and the server's sending middleware is not
// handed it: the peer is sent nothing more.
func TestTaskDeliveryStopsWhenListenEnds(t *testing.T) {
	f := newDeliveryFixture(t)
	var ended atomic.Bool
	late, sendLate := taskEvents[string](t, "notifications after the listen ended")
	f.server.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if task, ok := req.GetParams().(*DetailedTask); ok && ended.Load() {
				sendLate(task.TaskID)
			}
			return next(ctx, method, req)
		}
	})
	peer := f.peer()
	id := f.startTask(peer, "a")
	f.armReadOnAck.Store(true)
	listen := f.listen(peer, taskIDsJSON(id))
	f.readAck(peer)
	await(f, f.readGate.held, "the subscription's read of the task")
	listenContext := *f.listenReadContext.Load()
	peer.writeCancel(listen)
	await(f, listenContext.Done(), "the end of the listen's context")
	ended.Store(true)
	f.readGate.open()
	await(f, f.listenReturned, "the listen handler's return")
	f.readProbe(peer)
	if len(late) != 0 {
		t.Fatalf("the subscription tried to send the state of %s after its listen ended", <-late)
	}
	f.noViolation()
}
