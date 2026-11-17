// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// These tests hold state with gates and read the wire in order; nothing in
// them waits for time to pass. Timeouts are hang guards.

// taskGate parks one goroutine: once armed, the next pass reports what it
// holds and waits until the gate is opened. An opened gate never holds again.
type taskGate struct {
	armed   atomic.Bool
	held    chan string
	release chan struct{}
	open    func()
}

func newTaskGate(t *testing.T) *taskGate {
	g := &taskGate{held: make(chan string, 1), release: make(chan struct{})}
	g.open = sync.OnceFunc(func() { close(g.release) })
	// A gate is also opened by the fixture before its sessions close: a
	// failing test never leaves a connection-owned goroutine parked.
	t.Cleanup(g.open)
	return g
}

func (g *taskGate) arm() { g.armed.Store(true) }

func (g *taskGate) pass(what string) {
	if g.armed.CompareAndSwap(true, false) {
		g.held <- what
		<-g.release
	}
}

// taskInputMark marks the context a test's tool handler calls RequireInput
// with.
type taskInputMark struct{}

// taskListenMark marks the context of a subscriptions/listen request.
type taskListenMark struct{}

// taskLane drives one call of the fixture's tool: the handler runs each step
// it is sent, reports the step's error, and finishes when the lane is closed.
type taskLane struct {
	steps   chan func(context.Context, *TaskContext) error
	stepped chan error
	finish  func()
}

// deliveryFixture is a server with the extension, a tool whose task the test
// drives step by step, a store that sees every read, and middleware that
// records what the server does on whose behalf.
type deliveryFixture struct {
	t      *testing.T
	server *Server
	tasks  *TaskServer
	lanes  map[string]*taskLane

	violations chan string
	// ackGate parks the acknowledgement in the server's sending middleware.
	ackGate *taskGate
	ackHeld atomic.Bool
	// readGate parks a listen request's store read, after the read. When
	// armReadOnAck is set, the acknowledgement arms it on its way out, so
	// that it holds the first read the subscription makes to send a state.
	readGate     *taskGate
	armReadOnAck atomic.Bool
	// listenReadContext is the context of the last store read a listen
	// request made.
	listenReadContext atomic.Pointer[context.Context]
	// forbidOtherReads makes a store read outside a listen request a
	// violation.
	forbidOtherReads atomic.Bool
	// updateGate parks a store update made with a context that carries
	// taskInputMark, before the update.
	updateGate     *taskGate
	terminal       chan string // ids of tasks that reached a terminal status
	listenReturned chan struct{}
}

type deliveryStore struct {
	TaskStore
	f *deliveryFixture
}

func (s *deliveryStore) Get(ctx context.Context, scope, id string) (*DetailedTask, error) {
	marked := ctx.Value(taskListenMark{}) != nil
	if !marked && s.f.forbidOtherReads.Load() {
		s.f.violation("the store was read outside a subscriptions/listen request")
	}
	task, err := s.TaskStore.Get(ctx, scope, id)
	if marked && err == nil {
		s.f.listenReadContext.Store(&ctx)
		s.f.readGate.pass(string(task.Status))
	}
	return task, err
}

func (s *deliveryStore) Update(ctx context.Context, scope, id string, update func(*DetailedTask) error) error {
	if ctx.Value(taskInputMark{}) != nil {
		s.f.updateGate.pass("a marked update")
	}
	var status TaskStatus
	err := s.TaskStore.Update(ctx, scope, id, func(task *DetailedTask) error {
		if err := update(task); err != nil {
			return err
		}
		status = task.Status
		return nil
	})
	if err == nil && status.Terminal() {
		s.f.terminal <- id
	}
	return err
}

func newDeliveryFixture(t *testing.T) *deliveryFixture {
	t.Helper()
	f := &deliveryFixture{
		t:              t,
		lanes:          make(map[string]*taskLane),
		violations:     make(chan string, 64),
		terminal:       make(chan string, 64),
		listenReturned: make(chan struct{}, 64),
	}
	f.server = NewServer(testImpl, nil)
	var err error
	f.tasks, err = NewTaskServer(f.server, &TaskServerOptions{Store: &deliveryStore{TaskStore: NewMemoryTaskStore(), f: f}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		lane := &taskLane{steps: make(chan func(context.Context, *TaskContext) error), stepped: make(chan error, 1)}
		lane.finish = sync.OnceFunc(func() { close(lane.steps) })
		t.Cleanup(lane.finish)
		f.lanes[name] = lane
	}
	f.tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
			var args struct {
				Lane string `json:"lane"`
			}
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				return nil, err
			}
			lane := f.lanes[args.Lane]
			for step := range lane.steps {
				lane.stepped <- step(ctx, taskContext)
			}
			return &CallToolResult{Content: []Content{&TextContent{Text: "finished " + args.Lane}}}, nil
		})
	f.server.AddTool(&Tool{Name: "plain", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest) (*CallToolResult, error) { return &CallToolResult{}, nil })
	f.server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if method != methodSubscriptionsListen {
				return next(ctx, method, req)
			}
			result, err := next(context.WithValue(ctx, taskListenMark{}, true), method, req)
			f.listenReturned <- struct{}{}
			return result, err
		}
	})
	f.server.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			switch method {
			case notificationSubscriptionsAck:
				f.ackHeld.Store(true)
				f.ackGate.pass("acknowledgement")
				f.ackHeld.Store(false)
				if f.armReadOnAck.Load() {
					f.readGate.arm()
				}
			case notificationTaskStatus:
				if ctx.Value(taskListenMark{}) == nil {
					f.violation("notifications/tasks was sent outside its subscriptions/listen request")
				}
				if f.ackHeld.Load() {
					f.violation("notifications/tasks was sent while the acknowledgement was held")
				}
			}
			return next(ctx, method, req)
		}
	})

	f.ackGate = newTaskGate(t)
	f.readGate = newTaskGate(t)
	f.updateGate = newTaskGate(t)
	return f
}

// release opens every gate and finishes every lane, so that nothing the
// fixture parked outlives the test.
func (f *deliveryFixture) release() {
	f.ackGate.open()
	f.readGate.open()
	f.updateGate.open()
	for _, lane := range f.lanes {
		lane.finish()
	}
}

// peer connects a raw peer. The release is registered after the peer's
// session cleanups, so it runs before them: a failing test never closes a
// session while one of its goroutines is parked in a gate.
func (f *deliveryFixture) peer() *rawTaskPeer {
	f.t.Helper()
	peer := newRawTaskPeer(f.t, f.server)
	f.t.Cleanup(f.release)
	return peer
}

// http serves the fixture's server over Streamable HTTP; see peer.
func (f *deliveryFixture) http(options *StreamableHTTPOptions) *rawTaskHTTP {
	f.t.Helper()
	peer := newRawTaskHTTP(f.t, f.server, options)
	f.t.Cleanup(f.release)
	return peer
}

func (f *deliveryFixture) violation(what string) {
	select {
	case f.violations <- what:
	default:
	}
}

// await waits for ch, and fails at once on a violation.
func await[T any](f *deliveryFixture, ch <-chan T, waiting string) T {
	f.t.Helper()
	timer := taskGuard(f.t)
	defer timer.Stop()
	select {
	case value := <-ch:
		f.noViolation()
		return value
	case what := <-f.violations:
		f.t.Fatalf("while waiting for %s: %s", waiting, what)
	case <-timer.C:
		taskGuardExpired(f.t, "timed out waiting for %s", waiting)
	}
	panic("unreachable")
}

func (f *deliveryFixture) noViolation() {
	f.t.Helper()
	select {
	case what := <-f.violations:
		f.t.Fatal(what)
	default:
	}
}

// step runs one step in a lane's handler and waits for it to report back.
func (f *deliveryFixture) step(lane string, step func(context.Context, *TaskContext) error) {
	f.t.Helper()
	l := f.lanes[lane]
	select {
	case l.steps <- step:
	case what := <-f.violations:
		f.t.Fatal(what)
	case <-taskGuard(f.t).C:
		taskGuardExpired(f.t, "timed out handing a step to lane %s", lane)
	}
	if err := await(f, l.stepped, "the step in lane "+lane); err != nil {
		f.t.Fatalf("step in lane %s: %v", lane, err)
	}
}

// begin hands one step to a lane's handler without waiting for it to end;
// stepDone waits for it.
func (f *deliveryFixture) begin(lane string, step func(context.Context, *TaskContext) error) {
	f.t.Helper()
	select {
	case f.lanes[lane].steps <- step:
	case what := <-f.violations:
		f.t.Fatal(what)
	case <-taskGuard(f.t).C:
		taskGuardExpired(f.t, "timed out handing a step to lane %s", lane)
	}
}

func (f *deliveryFixture) stepDone(lane string) error {
	f.t.Helper()
	return await(f, f.lanes[lane].stepped, "the step in lane "+lane)
}

func setTaskMessage(message string) func(context.Context, *TaskContext) error {
	return func(_ context.Context, taskContext *TaskContext) error { return taskContext.SetStatusMessage(message) }
}

// read returns the next message a peer receives, and fails at once on a
// violation.
func (f *deliveryFixture) read(peer *rawTaskPeer, waiting string) rawTaskMessage {
	f.t.Helper()
	type read struct {
		message jsonrpc.Message
		err     error
	}
	done := make(chan read, 1)
	go func() {
		message, err := peer.conn.Read(peer.ctx)
		done <- read{message, err}
	}()
	got := await(f, done, waiting)
	if got.err != nil {
		f.t.Fatalf("reading %s: %v", waiting, got.err)
	}
	data, err := jsonrpc.EncodeMessage(got.message)
	if err != nil {
		f.t.Fatal(err)
	}
	return decodeRawTaskMessage(f.t, string(data))
}

// startTask calls the tool in a lane and returns the id of its task.
func (f *deliveryFixture) startTask(peer *rawTaskPeer, lane string) string {
	f.t.Helper()
	peer.request(methodCallTool, fmt.Sprintf(`{"name":"work","arguments":{"lane":%q},%s}`, lane, rawTaskMetaDeclaring))
	answer := f.read(peer, "the task of lane "+lane)
	var task Task
	if err := json.Unmarshal(answer.Result, &task); err != nil || task.TaskID == "" {
		f.t.Fatalf("tools/call answered %+v (%v), want a task", answer, err)
	}
	return task.TaskID
}

func (f *deliveryFixture) listen(peer *rawTaskPeer, notifications string) jsonrpc.ID {
	f.t.Helper()
	return peer.request(methodSubscriptionsListen, fmt.Sprintf(`{"notifications":%s,%s}`, notifications, rawTaskMetaDeclaring))
}

func taskIDsJSON(ids ...string) string {
	data, _ := json.Marshal(map[string]any{"taskIds": ids})
	return string(data)
}

// readAck reads the acknowledgement of a listen and returns the task ids in
// it.
func (f *deliveryFixture) readAck(peer *rawTaskPeer) []string {
	f.t.Helper()
	message := f.read(peer, "the subscription acknowledgement")
	if message.Method != notificationSubscriptionsAck {
		f.t.Fatalf("the first message of the listen is %+v, want the acknowledgement", message)
	}
	ids, _, _ := subscriptionTaskIDs(message.Params)
	return ids
}

// readTask reads one notifications/tasks.
func (f *deliveryFixture) readTask(peer *rawTaskPeer, waiting string) *DetailedTask {
	f.t.Helper()
	message := f.read(peer, waiting)
	if message.Method != notificationTaskStatus {
		f.t.Fatalf("waiting for %s: got %s %s %s, want notifications/tasks", waiting, message.Method, message.Params, message.Result)
	}
	var task DetailedTask
	if err := json.Unmarshal(message.Params, &task); err != nil {
		f.t.Fatal(err)
	}
	if (task.Status == TaskStatusInputRequired) != (len(task.InputRequests) > 0) {
		f.t.Fatalf("notification %s: input_required and inputRequests must come together", message.Params)
	}
	if task.ResultType != "" || task.Meta[MetaKeySubscriptionID] == nil {
		f.t.Fatalf("notification %s: want no resultType and a subscription id", message.Params)
	}
	return &task
}

// endListen cancels a listen and waits until its handler has returned.
// Everything a subscription sends, it sends from its listen request, so the
// answer to a probe written after that is the next message unless the
// subscription sent something more: a notification here is a failure.
func (f *deliveryFixture) endListen(peer *rawTaskPeer, id jsonrpc.ID) {
	f.t.Helper()
	peer.writeCancel(id)
	await(f, f.listenReturned, "the listen handler's return")
	f.readProbe(peer)
}

// readProbe checks that a peer has been sent nothing the test did not read;
// see [rawTaskPeer.probe].
func (f *deliveryFixture) readProbe(peer *rawTaskPeer) {
	f.t.Helper()
	probe := peer.probe()
	if message := f.read(peer, "the answer to the probe after the listen"); string(message.ID) != fmt.Sprint(probe.Raw()) {
		f.t.Fatalf("after the last expected notification the listen sent %s %s, want nothing", message.Method, message.Params)
	}
}

// subscriptionState reports, for the one subscription of a task, whether the
// task is marked changed and whether the subscription was woken.
func (f *deliveryFixture) subscriptionState(id string) (subscribed, dirty, woken bool) {
	f.tasks.mu.Lock()
	defer f.tasks.mu.Unlock()
	for sub := range f.tasks.subscriptions[taskKey{"", id}] {
		return true, sub.dirty[id], len(sub.wake) == 1
	}
	return false, false, false
}

func (f *deliveryFixture) subscribedTasks() []string {
	f.tasks.mu.Lock()
	defer f.tasks.mu.Unlock()
	var ids []string
	for key := range f.tasks.subscriptions {
		ids = append(ids, key.id)
	}
	slices.Sort(ids)
	return ids
}

// The acknowledgement of a listen precedes every notifications/tasks of it,
// however long the acknowledgement takes and whatever happens to the task
// meanwhile.
func TestTaskAckPrecedesNotifications(t *testing.T) {
	for _, lag := range []bool{false, true} {
		t.Run(fmt.Sprintf("lag=%t", lag), func(t *testing.T) {
			f := newDeliveryFixture(t)
			peer := f.peer()
			id := f.startTask(peer, "a")
			f.ackGate.arm()
			listen := f.listen(peer, taskIDsJSON(id))
			await(f, f.ackGate.held, "the acknowledgement to be on its way")

			// With the acknowledgement held, the task changes. A server that
			// notified from here would hand notifications/tasks to the
			// sending middleware now: await fails on that at once.
			f.step("a", setTaskMessage("one"))
			want := TaskStatusWorking
			if lag {
				f.step("a", setTaskMessage("two"))
				f.lanes["a"].finish()
				await(f, f.terminal, "the task to complete")
				want = TaskStatusCompleted
			}
			f.noViolation()
			f.ackGate.open()

			if ids := f.readAck(peer); !slices.Equal(ids, []string{id}) {
				t.Fatalf("acknowledged task ids = %v, want [%s]", ids, id)
			}
			last := f.readTask(peer, "the task's state after the acknowledgement")
			if last.TaskID != id || last.Status != want {
				t.Fatalf("first notification = %s %s %q, want the current state: %s", last.TaskID, last.Status, last.StatusMessage, want)
			}
			if !lag {
				if last.StatusMessage != "one" {
					t.Fatalf("first notification has message %q, want the current one", last.StatusMessage)
				}
				f.lanes["a"].finish()
				await(f, f.terminal, "the task to complete")
				last = f.readTask(peer, "the completed task")
			}
			if last.Status != TaskStatusCompleted || len(last.Result) == 0 {
				t.Fatalf("last notification = %s with result %s, want the completed task", last.Status, last.Result)
			}
			// Nothing follows the terminal state.
			f.endListen(peer, listen)
		})
	}
}

// An update only marks the task changed: it does not read the store, send or
// wait on behalf of a subscriber, whatever the subscriber is doing.
func TestTaskUpdatesDoNoSubscriberWork(t *testing.T) {
	t.Run("acknowledgement held", func(t *testing.T) {
		f := newDeliveryFixture(t)
		peer := f.peer()
		id := f.startTask(peer, "a")
		f.ackGate.arm()
		listen := f.listen(peer, taskIDsJSON(id))
		await(f, f.ackGate.held, "the acknowledgement to be on its way")

		f.forbidOtherReads.Store(true)
		f.step("a", setTaskMessage("changed"))
		f.forbidOtherReads.Store(false)
		f.noViolation()

		f.ackGate.open()
		f.readAck(peer)
		if task := f.readTask(peer, "the changed task"); task.StatusMessage != "changed" {
			t.Fatalf("notification has message %q, want the change", task.StatusMessage)
		}
		f.noViolation()
		f.lanes["a"].finish()
		if task := f.readTask(peer, "the completed task"); task.Status != TaskStatusCompleted {
			t.Fatalf("notification has status %s, want completed", task.Status)
		}
		f.endListen(peer, listen)
	})

	// Lag: the subscriber is parked in the store read for a notification
	// while another session cancels the task. The cancel is answered
	// meanwhile; the subscriber then sends what it read, and after it the
	// cancelled state, and nothing after that.
	t.Run("subscriber parked in a read", func(t *testing.T) {
		f := newDeliveryFixture(t)
		peer := f.peer()
		other := f.peer()
		id := f.startTask(peer, "a")
		f.armReadOnAck.Store(true)
		listen := f.listen(peer, taskIDsJSON(id))
		f.readAck(peer)
		if held := await(f, f.readGate.held, "the subscriber's read"); held != string(TaskStatusWorking) {
			t.Fatalf("the subscriber read a %s task, want working", held)
		}

		f.forbidOtherReads.Store(true)
		f.step("a", setTaskMessage("changed"))
		f.forbidOtherReads.Store(false)
		// The subscriber is parked, so its state is stable: the update left
		// a mark and a wake-up, and nothing else.
		if subscribed, dirty, woken := f.subscriptionState(id); !subscribed || !dirty || !woken {
			t.Fatalf("after an update: subscribed %t, marked changed %t, woken %t; want all three", subscribed, dirty, woken)
		}
		other.request(methodCancelTask, fmt.Sprintf(`{"taskId":%q,%s}`, id, rawTaskMetaDeclaring))
		if answer := f.read(other, "the answer to tasks/cancel"); answer.Error != nil || len(answer.Result) == 0 {
			t.Fatalf("tasks/cancel answered %+v", answer)
		}
		await(f, f.terminal, "the task to be cancelled")

		f.readGate.open()
		if task := f.readTask(peer, "the state the subscriber had read"); task.Status != TaskStatusWorking || task.StatusMessage != "" {
			t.Fatalf("first notification = %s %q, want the working state that was read", task.Status, task.StatusMessage)
		}
		if task := f.readTask(peer, "the cancelled task"); task.Status != TaskStatusCancelled {
			t.Fatalf("second notification = %s %q, want cancelled", task.Status, task.StatusMessage)
		}
		f.endListen(peer, listen)
	})
}

// A listen is told the current state of each task right after the
// acknowledgement, so a task that finished before the listen is not waited
// for; ids the server does not know are left out of the acknowledgement and
// the rest of the listen is honoured.
func TestTaskListenSnapshotAndUnknownIDs(t *testing.T) {
	f := newDeliveryFixture(t)
	peer := f.peer()
	id := f.startTask(peer, "a")
	f.lanes["a"].finish()
	await(f, f.terminal, "the task to complete")

	listen := f.listen(peer, fmt.Sprintf(`{"toolsListChanged":true,"taskIds":["unknown",%q,"",%q]}`, id, id))
	ack := f.read(peer, "the subscription acknowledgement")
	if ack.Method != notificationSubscriptionsAck {
		t.Fatalf("got %+v, want the acknowledgement", ack)
	}
	if !strings.Contains(string(ack.Params), `"toolsListChanged":true`) {
		t.Errorf("acknowledgement %s does not honour toolsListChanged", ack.Params)
	}
	if ids, _, _ := subscriptionTaskIDs(ack.Params); !slices.Equal(ids, []string{id}) {
		t.Errorf("acknowledged task ids = %v, want [%s]", ids, id)
	}
	task := f.readTask(peer, "the finished task's state")
	if task.TaskID != id || task.Status != TaskStatusCompleted {
		t.Fatalf("notification = %s %s, want the completed task", task.TaskID, task.Status)
	}
	result, err := task.ToolResult()
	if err != nil || len(result.Content) != 1 || result.Content[0].(*TextContent).Text != "finished a" {
		t.Fatalf("notification result = %s (%v), want the tool's result", task.Result, err)
	}
	f.endListen(peer, listen)

	// Only unknown ids: acknowledged without tasks, and the listen ends as a
	// listen with no subscription does.
	only := f.listen(peer, taskIDsJSON("unknown"))
	if ids := f.readAck(peer); len(ids) != 0 {
		t.Errorf("acknowledged task ids = %v, want none", ids)
	}
	if message := f.read(peer, "the answer to a listen with no subscription"); string(message.ID) != fmt.Sprint(only.Raw()) || message.Error != nil {
		t.Fatalf("got %+v, want the listen's answer", message)
	}
}

// subscribe leaves every accepted task marked changed and the subscription
// woken, which is what sends the first state after the acknowledgement.
func TestTaskSubscribeState(t *testing.T) {
	ctx := taskTestContext(t)
	server := NewServer(testImpl, nil)
	owner, stranger := &ServerSession{}, &ServerSession{}
	tasks, err := NewTaskServer(server, &TaskServerOptions{
		Scope: func(_ context.Context, session *ServerSession) string {
			if session == owner {
				return "owner"
			}
			return "stranger"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		if err := tasks.opts.Store.Create(ctx, "owner", &DetailedTask{Task: Task{TaskID: id, Status: TaskStatusWorking}}); err != nil {
			t.Fatal(err)
		}
	}
	requestID, err := jsonrpc.MakeID("listen-1")
	if err != nil {
		t.Fatal(err)
	}
	meta := tasksRequestMeta()
	meta[MetaKeyProtocolVersion] = protocolVersion20260728
	request := func(session *ServerSession, meta Meta) *SubscriptionsListenRequest {
		return &SubscriptionsListenRequest{Session: session, Params: &SubscriptionsListenParams{
			Meta:          meta,
			Notifications: &NotificationSubscriptions{TaskIDs: []string{"second", "unknown", "first", "second", ""}},
		}}
	}

	if _, err := tasks.subscribe(ctx, request(owner, nil), requestID); errorCodeForTaskTest(err) != CodeMissingRequiredClientCapabilities {
		t.Errorf("subscribe without the capability = %v, want the missing-capability error", err)
	}
	// Another scope's tasks are unknown tasks.
	if sub, err := tasks.subscribe(ctx, request(stranger, meta), requestID); sub != nil || err != nil {
		t.Errorf("subscribe from another scope = %v, %v; want no subscription and no error", sub, err)
	}
	if len(tasks.subscriptions) != 0 {
		t.Errorf("subscriptions after two refused listens: %v", tasks.subscriptions)
	}

	sub, err := tasks.subscribe(ctx, request(owner, meta), requestID)
	if err != nil || sub == nil {
		t.Fatalf("subscribe = %v, %v", sub, err)
	}
	if want := []string{"second", "first"}; !slices.Equal(sub.ids, want) {
		t.Errorf("accepted ids = %v, want %v", sub.ids, want)
	}
	if !sub.dirty["first"] || !sub.dirty["second"] || len(sub.wake) != 1 {
		t.Errorf("after subscribe: changed %v, woken %t; want both tasks and a wake-up", sub.dirty, len(sub.wake) == 1)
	}
	if got := sub.takeDirty(); !slices.Equal(got, []string{"second", "first"}) {
		t.Errorf("takeDirty = %v, want the tasks in the order of the request", got)
	}
	select {
	case <-sub.wake:
	default:
	}
	tasks.notifyTask("stranger", "first")
	tasks.notifyTask("owner", "unknown")
	if len(sub.dirty) != 0 || len(sub.wake) != 0 {
		t.Errorf("an update to another scope's or another task marked the subscription: %v", sub.dirty)
	}
	tasks.notifyTask("owner", "first")
	tasks.notifyTask("owner", "first")
	if !sub.dirty["first"] || sub.dirty["second"] || len(sub.wake) != 1 {
		t.Errorf("after an update: changed %v, woken %t; want only the updated task and a wake-up", sub.dirty, len(sub.wake) == 1)
	}
	sub.close()
	if len(tasks.subscriptions) != 0 || len(sub.following) != 0 {
		t.Errorf("after close: %v, following %v", tasks.subscriptions, sub.following)
	}
	tasks.notifyTask("owner", "first") // must not reach a closed subscription
	if len(sub.dirty) != 0 {
		t.Errorf("an update marked a closed subscription: %v", sub.dirty)
	}
}

// A subscription leaves the server when its listen is cancelled and when its
// session ends, and a task leaves it once its terminal state was sent.
func TestTaskSubscriptionRemoved(t *testing.T) {
	start := func(t *testing.T) (*deliveryFixture, *rawTaskPeer, string, jsonrpc.ID) {
		f := newDeliveryFixture(t)
		peer := f.peer()
		id := f.startTask(peer, "a")
		listen := f.listen(peer, taskIDsJSON(id))
		f.readAck(peer)
		f.readTask(peer, "the task's state after the acknowledgement")
		if got := f.subscribedTasks(); !slices.Equal(got, []string{id}) {
			t.Fatalf("subscribed tasks = %v, want [%s]", got, id)
		}
		return f, peer, id, listen
	}
	t.Run("listen cancelled", func(t *testing.T) {
		f, peer, _, listen := start(t)
		f.endListen(peer, listen)
		if got := f.subscribedTasks(); len(got) != 0 {
			t.Errorf("after the listen ended the server still has subscriptions to %v", got)
		}
	})
	t.Run("session closed", func(t *testing.T) {
		f, peer, _, _ := start(t)
		peer.conn.Close()
		await(f, f.listenReturned, "the listen handler's return")
		if got := f.subscribedTasks(); len(got) != 0 {
			t.Errorf("after the session ended the server still has subscriptions to %v", got)
		}
	})
	t.Run("task terminal", func(t *testing.T) {
		f := newDeliveryFixture(t)
		peer := f.peer()
		first, second := f.startTask(peer, "a"), f.startTask(peer, "b")
		listen := f.listen(peer, taskIDsJSON(first, second))
		f.readAck(peer)
		f.readTask(peer, "the first task's state")
		f.readTask(peer, "the second task's state")
		f.lanes["a"].finish()
		if task := f.readTask(peer, "the first task completed"); task.TaskID != first || task.Status != TaskStatusCompleted {
			t.Fatalf("notification = %s %s, want the first task completed", task.TaskID, task.Status)
		}
		// The subscription sends from one goroutine: once it has sent the
		// second task's next state, it is done with the first.
		f.step("b", setTaskMessage("still working"))
		if task := f.readTask(peer, "the second task's change"); task.TaskID != second || task.StatusMessage != "still working" {
			t.Fatalf("notification = %s %q, want the second task's change", task.TaskID, task.StatusMessage)
		}
		if got := f.subscribedTasks(); !slices.Equal(got, []string{second}) {
			t.Errorf("subscribed tasks = %v, want only the running one, %s", got, second)
		}
		f.endListen(peer, listen)
	})
}

func TestNextTaskTimestamp(t *testing.T) {
	at := time.Date(2026, 7, 28, 10, 0, 0, 500, time.UTC)
	for _, test := range []struct {
		name     string
		previous string
		now      time.Time
		want     string
	}{
		{"later clock", "2026-07-28T10:00:00Z", at, "2026-07-28T10:00:00.0000005Z"},
		{"same instant", "2026-07-28T10:00:00.0000005Z", at, "2026-07-28T10:00:00.000000501Z"},
		{"clock stepped back", "2026-07-28T11:00:00Z", at, "2026-07-28T11:00:00.000000001Z"},
		{"another zone", "2026-07-28T12:00:00+02:00", at.In(time.FixedZone("east", 7200)), "2026-07-28T10:00:00.0000005Z"},
		{"unreadable previous", "yesterday", at, "2026-07-28T10:00:00.0000005Z"},
		{"no previous", "", at, "2026-07-28T10:00:00.0000005Z"},
	} {
		if got := nextTaskTimestamp(test.previous, test.now); got != test.want {
			t.Errorf("%s: nextTaskTimestamp(%q) = %q, want %q", test.name, test.previous, got, test.want)
		}
	}
}

// Over stateless Streamable HTTP the acknowledgement and every
// notifications/tasks arrive on the response stream of the listen request,
// though the task runs on behalf of another request.
func TestTaskNotificationsOnListenStream(t *testing.T) {
	f := newDeliveryFixture(t)
	peer := f.http(&StreamableHTTPOptions{Stateless: true})
	status, messages := peer.post(rawTaskRequest(1, methodCallTool, `"name":"work","arguments":{"lane":"a"}`, true), map[string]string{nameHeader: "work"})
	if status != http.StatusOK || len(messages) != 1 {
		t.Fatalf("tools/call: HTTP %d %v", status, messages)
	}
	var task Task
	if err := json.Unmarshal(decodeRawTaskMessage(t, messages[0]).Result, &task); err != nil || task.TaskID == "" {
		t.Fatalf("tools/call answered %s, want a task", messages[0])
	}

	stream := peer.open(rawTaskRequest(2, methodSubscriptionsListen, `"notifications":`+taskIDsJSON(task.TaskID), true), nil)
	if stream.status != http.StatusOK {
		t.Fatalf("listen: HTTP %d %v", stream.status, stream.rest())
	}
	ack := decodeRawTaskMessage(t, stream.next("the acknowledgement on the listen stream"))
	if ids, _, _ := subscriptionTaskIDs(ack.Params); ack.Method != notificationSubscriptionsAck || !slices.Equal(ids, []string{task.TaskID}) {
		t.Fatalf("first message on the listen stream = %+v, want the acknowledgement of the task", ack)
	}
	next := func(waiting string) *DetailedTask {
		t.Helper()
		message := decodeRawTaskMessage(t, stream.next(waiting))
		var task DetailedTask
		if err := json.Unmarshal(message.Params, &task); err != nil || message.Method != notificationTaskStatus {
			t.Fatalf("waiting for %s on the listen stream: got %+v", waiting, message)
		}
		return &task
	}
	if got := next("the task's state"); got.TaskID != task.TaskID || got.Status != TaskStatusWorking {
		t.Fatalf("first notification = %s %s, want the working task", got.TaskID, got.Status)
	}
	f.step("a", setTaskMessage("halfway"))
	if got := next("the task's change"); got.StatusMessage != "halfway" {
		t.Fatalf("second notification has message %q, want the change", got.StatusMessage)
	}
	f.lanes["a"].finish()
	if got := next("the completed task"); got.Status != TaskStatusCompleted || len(got.Result) == 0 {
		t.Fatalf("third notification = %s %s, want the completed task with its result", got.Status, got.Result)
	}
	f.noViolation()

	// Ending the request ends the listen and removes the subscription.
	stream.cancel()
	await(f, f.listenReturned, "the listen handler's return")
	if got := f.subscribedTasks(); len(got) != 0 {
		t.Errorf("after the listen stream closed the server still has subscriptions to %v", got)
	}
}

// A Streamable HTTP server with sessions does not serve 2026-07-28 requests
// at all, so the extension, which exists only from that version on, has no
// stream of that kind to deliver on: every task request is refused before it
// reaches the server.
func TestTaskRequestsRefusedWithSessions(t *testing.T) {
	f := newDeliveryFixture(t)
	peer := f.http(nil)
	for _, test := range []struct {
		method, params string
		headers        map[string]string
	}{
		{methodCallTool, `"name":"work","arguments":{"lane":"a"}`, map[string]string{nameHeader: "work"}},
		{methodSubscriptionsListen, `"notifications":` + taskIDsJSON("task-1"), nil},
		{methodGetTask, `"taskId":"task-1"`, map[string]string{nameHeader: "task-1"}},
	} {
		status, messages := peer.post(rawTaskRequest(1, test.method, test.params, true), test.headers)
		if status != http.StatusBadRequest {
			t.Errorf("%s on a server with sessions: HTTP %d %v, want 400", test.method, status, messages)
		}
	}
	if got := f.subscribedTasks(); len(got) != 0 {
		t.Errorf("subscriptions: %v", got)
	}
}

// A client that only polls: it finds the extension with server/discover,
// opts in on each request through _meta, calls the tool, and asks tasks/get
// until the task is completed; the tool's result is in the answer.
func TestTaskPollingOverStreamableHTTP(t *testing.T) {
	f := newDeliveryFixture(t)
	peer := f.http(&StreamableHTTPOptions{Stateless: true})
	result := func(waiting string, status int, messages []string) json.RawMessage {
		t.Helper()
		if status != http.StatusOK || len(messages) != 1 {
			t.Fatalf("%s: HTTP %d %v", waiting, status, messages)
		}
		message := decodeRawTaskMessage(t, messages[0])
		if message.Error != nil || len(message.Result) == 0 {
			t.Fatalf("%s: %s", waiting, messages[0])
		}
		return message.Result
	}

	status, messages := peer.post(rawTaskRequest(1, "server/discover", "", false), nil)
	var discovered struct {
		Capabilities struct {
			Extensions map[string]json.RawMessage `json:"extensions"`
		} `json:"capabilities"`
	}
	if err := json.Unmarshal(result("server/discover", status, messages), &discovered); err != nil {
		t.Fatal(err)
	}
	if got := string(discovered.Capabilities.Extensions[ExtensionTasks]); got != "{}" {
		t.Fatalf("server/discover advertises the extension as %q, want {}", got)
	}

	status, messages = peer.post(rawTaskRequest(2, methodCallTool, `"name":"work","arguments":{"lane":"a"}`, true), map[string]string{nameHeader: "work"})
	var created struct {
		Task
		ResultType string          `json:"resultType"`
		Content    json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(result("tools/call", status, messages), &created); err != nil {
		t.Fatal(err)
	}
	if created.ResultType != "task" || created.TaskID == "" || created.Status != TaskStatusWorking || created.Content != nil {
		t.Fatalf("tools/call answered %s, want a working task and no content", messages[0])
	}

	get := func(id int) (task DetailedTask, raw string) {
		t.Helper()
		status, messages := peer.post(rawTaskRequest(id, methodGetTask, fmt.Sprintf(`"taskId":%q`, created.TaskID), true), map[string]string{nameHeader: created.TaskID})
		if err := json.Unmarshal(result("tasks/get", status, messages), &task); err != nil {
			t.Fatal(err)
		}
		if task.TaskID != created.TaskID || task.ResultType != resultTypeComplete {
			t.Fatalf("tasks/get answered %s, want the task with resultType complete", messages[0])
		}
		return task, messages[0]
	}
	if task, raw := get(3); task.Status != TaskStatusWorking || task.Result != nil {
		t.Fatalf("tasks/get while the tool runs: %s", raw)
	}
	f.step("a", setTaskMessage("halfway"))
	if task, raw := get(4); task.Status != TaskStatusWorking || task.StatusMessage != "halfway" {
		t.Fatalf("tasks/get after a status message: %s", raw)
	}
	f.lanes["a"].finish()
	await(f, f.terminal, "the task to complete")
	task, raw := get(5)
	if task.Status != TaskStatusCompleted {
		t.Fatalf("tasks/get after the tool returned: %s", raw)
	}
	var inlined struct {
		ResultType string `json:"resultType"`
		Content    []struct {
			Type, Text string
		} `json:"content"`
	}
	if err := json.Unmarshal(task.Result, &inlined); err != nil {
		t.Fatal(err)
	}
	if inlined.ResultType != "complete" || len(inlined.Content) != 1 || inlined.Content[0].Text != "finished a" {
		t.Fatalf("tasks/get result = %s, want the tool's result", task.Result)
	}
	// The same request without the declaration is refused, not served.
	status, messages = peer.post(rawTaskRequest(6, methodGetTask, fmt.Sprintf(`"taskId":%q`, created.TaskID), false), map[string]string{nameHeader: created.TaskID})
	if code := rawTaskErrorCode(t, messages); status != http.StatusBadRequest || code != CodeMissingRequiredClientCapabilities {
		t.Fatalf("tasks/get without the declaration: HTTP %d %v, want -32021", status, messages)
	}
	f.noViolation()
}
