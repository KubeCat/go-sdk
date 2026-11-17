// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func TestNotificationSubscriptionsJSONHasNoTaskIDs(t *testing.T) {
	data, err := json.Marshal(&SubscriptionsListenParams{Notifications: &NotificationSubscriptions{
		ToolsListChanged: true,
		TaskIDs:          []string{"task-1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"notifications":{"toolsListChanged":true}}`; string(data) != want {
		t.Errorf("listen params marshal as %s, want %s", data, want)
	}
	for _, input := range []string{`{"taskIds":["task-1"]}`, `{"taskIds":7}`, `{"taskIds":"task-1"}`, `{"taskIds":[7]}`} {
		var subscriptions NotificationSubscriptions
		if err := json.Unmarshal([]byte(input), &subscriptions); err != nil {
			t.Errorf("unmarshal %s: %v", input, err)
		}
		if subscriptions.TaskIDs != nil {
			t.Errorf("unmarshal %s read task ids %v", input, subscriptions.TaskIDs)
		}
	}
}

// rawTaskPeer speaks JSON-RPC to a server by hand. Every read is bounded by
// the test context.
type rawTaskPeer struct {
	t    *testing.T
	ctx  context.Context
	conn Connection
	next int64
	// ended receives the id of each subscriptions/listen request of this
	// peer whose handler has returned.
	ended <-chan jsonrpc.ID
}

func newRawTaskPeer(t *testing.T, server *Server) *rawTaskPeer {
	t.Helper()
	ctx := taskTestContext(t)
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	ended, end := taskEvents[jsonrpc.ID](t, "listen handlers that returned")
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			result, err := next(ctx, method, req)
			if method == methodSubscriptionsListen && req.GetSession() == Session(serverSession) {
				end(req.GetID())
			}
			return result, err
		}
	})
	conn, err := clientTransport.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "conn", conn.Close)
	return &rawTaskPeer{t: t, ctx: ctx, conn: conn, ended: ended}
}

const (
	rawTaskMeta          = `"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"raw-peer","version":"v1"},"io.modelcontextprotocol/clientCapabilities":{}}`
	rawTaskMetaDeclaring = `"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"raw-peer","version":"v1"},"io.modelcontextprotocol/clientCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}`
)

// request writes a request and returns its id.
func (p *rawTaskPeer) request(method, params string) jsonrpc.ID {
	p.t.Helper()
	p.next++
	id, err := jsonrpc.MakeID(float64(p.next))
	if err != nil {
		p.t.Fatal(err)
	}
	if err := p.conn.Write(p.ctx, &jsonrpc.Request{ID: id, Method: method, Params: json.RawMessage(params)}); err != nil {
		p.t.Fatalf("writing %s: %v", method, err)
	}
	return id
}

// read returns the next message as JSON.
func (p *rawTaskPeer) read(waiting string) string {
	p.t.Helper()
	message, err := p.conn.Read(p.ctx)
	if err != nil {
		p.t.Fatalf("reading %s: %v", waiting, err)
	}
	data, err := jsonrpc.EncodeMessage(message)
	if err != nil {
		p.t.Fatal(err)
	}
	return string(data)
}

// writeCancel cancels a request. A cancelled request of protocol version
// 2026-07-28 is not answered.
func (p *rawTaskPeer) writeCancel(id jsonrpc.ID) {
	p.t.Helper()
	params := fmt.Sprintf(`{"requestId":%v}`, id.Raw())
	if err := p.conn.Write(p.ctx, &jsonrpc.Request{Method: notificationCancelled, Params: json.RawMessage(params)}); err != nil {
		p.t.Fatal(err)
	}
}

// probe writes a request the server refuses without running any handler.
// The server answers what it reads in order when nothing holds a request, and
// everything a listen sends it sends before its handler returns: once that
// handler has returned, the answer to the probe is the peer's next message
// unless the listen sent something more.
func (p *rawTaskPeer) probe() jsonrpc.ID {
	p.t.Helper()
	return p.request("example/probe", `{`+rawTaskMeta+`}`)
}

// cancel ends a listen request, waits until its handler has returned, and
// checks that the listen has sent nothing the test did not read.
func (p *rawTaskPeer) cancel(id jsonrpc.ID) {
	p.t.Helper()
	p.writeCancel(id)
	for waitTask(p.t, p.ended, "the cancelled listen's handler to return") != id {
	}
	probe := p.probe()
	if got := p.read("the answer to the probe after the listen"); !strings.Contains(got, fmt.Sprintf(`"id":%v`, probe.Raw())) {
		p.t.Fatalf("after cancelling listen %v: %s, want only the answer to the probe", id.Raw(), got)
	}
}

// A server with the extension reads notifications.taskIds from the raw
// listen request and writes the ids it agreed to into the acknowledgement,
// though the shared type's JSON has no such key.
func TestTaskListenTaskIDsOnTheWire(t *testing.T) {
	server := newTaskServerWithTask(t, "task-1")
	server.AddTool(&Tool{Name: "plain", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest) (*CallToolResult, error) { return &CallToolResult{}, nil })
	peer := newRawTaskPeer(t, server)

	t.Run("agreed ids are acknowledged", func(t *testing.T) {
		id := peer.request(methodSubscriptionsListen, `{`+rawTaskMetaDeclaring+`,"notifications":{"toolsListChanged":true,"taskIds":["task-1"]}}`)
		ack := peer.read("the acknowledgement")
		want := fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":%v},"notifications":{"toolsListChanged":true,"taskIds":["task-1"]}}}`, id.Raw())
		if ack != want {
			t.Errorf("acknowledgement = %s, want %s", ack, want)
		}
		// The task's state follows the acknowledgement; only then is the
		// listen cancelled, so that its answer is the next message.
		if state := peer.read("the task's state"); !strings.Contains(state, `"method":"notifications/tasks"`) || !strings.Contains(state, `"taskId":"task-1"`) {
			t.Errorf("after the acknowledgement: %s, want the state of task-1", state)
		}
		peer.cancel(id)
	})
	t.Run("no ids, standard acknowledgement", func(t *testing.T) {
		for _, notifications := range []string{`{"toolsListChanged":true}`, `{"toolsListChanged":true,"taskIds":null}`, `{"toolsListChanged":true,"taskIds":[]}`} {
			id := peer.request(methodSubscriptionsListen, `{`+rawTaskMeta+`,"notifications":`+notifications+`}`)
			ack := peer.read("the acknowledgement")
			want := fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":%v},"notifications":{"toolsListChanged":true}}}`, id.Raw())
			if ack != want {
				t.Errorf("%s: acknowledgement = %s, want %s", notifications, ack, want)
			}
			peer.cancel(id)
		}
	})
	t.Run("malformed ids", func(t *testing.T) {
		for _, taskIDs := range []string{`"task-1"`, `[7]`, `{"task-1":true}`, `7`} {
			peer.request(methodSubscriptionsListen, `{`+rawTaskMetaDeclaring+`,"notifications":{"toolsListChanged":true,"taskIds":`+taskIDs+`}}`)
			if got := peer.read("the answer to a declaring listen"); !strings.Contains(got, `"error":{"code":-32602,`) {
				t.Errorf("declaring listen with taskIds %s: %s; want invalid params", taskIDs, got)
			}
			peer.request(methodSubscriptionsListen, `{`+rawTaskMeta+`,"notifications":{"toolsListChanged":true,"taskIds":`+taskIDs+`}}`)
			if got := peer.read("the answer to a non-declaring listen"); !strings.Contains(got, `"error":{"code":-32021,`) {
				t.Errorf("non-declaring listen with taskIds %s: %s; want the missing-capability error", taskIDs, got)
			}
		}
	})
	t.Run("well-formed ids without declaring", func(t *testing.T) {
		peer.request(methodSubscriptionsListen, `{`+rawTaskMeta+`,"notifications":{"taskIds":["task-1"]}}`)
		if got := peer.read("the answer"); !strings.Contains(got, `"error":{"code":-32021,`) {
			t.Errorf("non-declaring listen naming a task: %s; want the missing-capability error", got)
		}
	})
}

// A server without the extension treats taskIds as the unchanged SDK does: it never
// reads the key.
func TestListenIgnoresTaskIDsWithoutTaskServer(t *testing.T) {
	peer := newRawTaskPeer(t, NewServer(testImpl, nil))
	for _, taskIDs := range []string{`["task-1"]`, `"task-1"`, `[7]`, `{"task-1":true}`} {
		for _, meta := range []string{rawTaskMeta, rawTaskMetaDeclaring} {
			id := peer.request(methodSubscriptionsListen, `{`+meta+`,"notifications":{"taskIds":`+taskIDs+`}}`)
			ack := peer.read("the acknowledgement")
			want := fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":%v},"notifications":{}}}`, id.Raw())
			if ack != want {
				t.Errorf("taskIds %s: acknowledgement = %s, want %s", taskIDs, ack, want)
			}
			if got := peer.read("the listen's result"); !strings.Contains(got, `"result":`) {
				t.Errorf("taskIds %s: %s; want the listen's result", taskIDs, got)
			}
		}
	}
}

// methodRecorder is receiving middleware that records the methods a client
// was handed, with the task ids of each acknowledgement.
type methodRecorder struct {
	mu      sync.Mutex
	methods []string
}

func (r *methodRecorder) middleware(next MethodHandler) MethodHandler {
	return func(ctx context.Context, method string, req Request) (Result, error) {
		entry := method
		if ack, ok := req.GetParams().(*SubscriptionsAcknowledgedParams); ok && ack != nil {
			entry = fmt.Sprintf("%s%v", method, ack.Notifications.TaskIDs)
		}
		if task, ok := req.GetParams().(*DetailedTask); ok && task != nil {
			entry = method + "[" + task.TaskID + "]"
		}
		r.mu.Lock()
		r.methods = append(r.methods, entry)
		r.mu.Unlock()
		return next(ctx, method, req)
	}
}

func (r *methodRecorder) take() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	methods := strings.Join(r.methods, " ")
	r.methods = nil
	return methods
}

// sendTaskMessages has the server send, unasked, a notifications/tasks and
// an acknowledgement naming tasks, and then a ping, which returns once the
// client has answered it: the two notifications were handled before that.
func sendTaskMessages(t *testing.T, serverSession *ServerSession, taskID string) {
	t.Helper()
	ctx := taskTestContext(t)
	conn := serverSession.getConn()
	if err := conn.Notify(ctx, notificationTaskStatus, &DetailedTask{Task: Task{TaskID: taskID, Status: TaskStatusWorking}}); err != nil {
		t.Fatal(err)
	}
	ack := &taskAckWire{Meta: Meta{MetaKeySubscriptionID: "unasked"}, Notifications: taskSubscriptionsWire{TaskIDs: []string{taskID}}}
	if err := conn.Notify(ctx, notificationSubscriptionsAck, ack); err != nil {
		t.Fatal(err)
	}
	if err := serverSession.Ping(ctx, nil); err != nil {
		t.Fatalf("ping after unasked task messages: %v", err)
	}
}

// The session's receiving table knows notifications/tasks, and reads task
// ids from acknowledgements, only while FollowTask runs on that session.
// Before and after, both messages are handled as without the extension, no
// caller sees an error and the session keeps working. While it runs, messages
// about a task nobody follows change nothing.
func TestTaskFollowTableOnlyDuringFollowTask(t *testing.T) {
	ctx := taskTestContext(t)
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	releaseTask := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseTask)
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, _ *CallToolRequest, _ *TaskContext) (*CallToolResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &CallToolResult{}, nil
		})
	listenReturned := taskListenReturns(t, server)
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	client := NewClient(&Implementation{Name: "task-follow-client", Version: "v1.0.0"}, nil)
	var recorder methodRecorder
	client.AddReceivingMiddleware(recorder.middleware)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "session", session.Close)
	other, err := client.Connect(ctx, connectServerForTaskTest(t, server), nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "other", other.Close)

	result, err := session.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.Task()
	if !ok {
		t.Fatal("no task handle")
	}
	standard := func(when string) {
		t.Helper()
		if session.taskReceive.Load() != nil {
			t.Fatalf("%s: the session has a follow table", when)
		}
		if _, ok := session.receivingMethodInfos()[notificationTaskStatus]; ok {
			t.Fatalf("%s: the session receives notifications/tasks", when)
		}
		recorder.take()
		sendTaskMessages(t, serverSession, task.TaskID)
		// The task notification is an unknown method, as in the unchanged SDK; the
		// acknowledgement carries no task ids.
		if got, want := recorder.take(), "notifications/subscriptions/acknowledged[] ping"; got != want {
			t.Fatalf("%s: the client was handed %q, want %q", when, got, want)
		}
		if _, err := session.ListTools(ctx, nil); err != nil {
			t.Fatalf("%s: the session stopped working: %v", when, err)
		}
	}
	standard("before FollowTask")

	var during string
	var once sync.Once
	final, err := session.FollowTask(ctx, task.TaskID, func(context.Context, *DetailedTask) error {
		once.Do(func() {
			if session.taskReceive.Load() == nil {
				during = "no follow table while FollowTask runs"
				return
			}
			if other.taskReceive.Load() != nil {
				during = "another session of the client got a follow table"
				return
			}
			recorder.take()
			sendTaskMessages(t, serverSession, "nobody-follows-this")
			// The followed task's own acknowledgement and notifications may
			// arrive at any point; they are not what this looks at.
			var handed []string
			for _, entry := range strings.Fields(recorder.take()) {
				if !strings.Contains(entry, task.TaskID) {
					handed = append(handed, entry)
				}
			}
			during = strings.Join(handed, " ")
			releaseTask()
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != TaskStatusCompleted {
		t.Fatalf("final status = %q", final.Status)
	}
	if want := "notifications/tasks[nobody-follows-this] notifications/subscriptions/acknowledged[nobody-follows-this] ping"; during != want {
		t.Fatalf("while FollowTask ran the client was handed %q, want %q", during, want)
	}
	// FollowTask does not wait for its listen to end. What the server sent on
	// that listen may still be on its way; such a message is the subject of
	// TestTaskFollowTableLateNotification. Here, let all of it arrive first:
	// the listen has ended on the server, and the client has answered a ping
	// the server sent after that.
	waitTaskListenReturned(t, listenReturned)
	if err := serverSession.Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	standard("after FollowTask")
}

// FollowTask returns without waiting for its listen to end, so a
// notifications/tasks the server sent on that listen can reach the client
// when FollowTask has returned and the session has its standard table again.
// The lag is held here: the task's terminal notification is decoded while
// FollowTask runs and parked in receiving middleware until FollowTask, which
// learned the same from a poll, has returned. The notification then ends as
// one the session does not handle; nobody sees an error and the session
// keeps working.
func TestTaskFollowTableLateNotification(t *testing.T) {
	ctx := taskTestContext(t)
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	release, releaseTask := taskTestGate()
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, _ *CallToolRequest, _ *TaskContext) (*CallToolResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &CallToolResult{}, nil
		})
	listenReturned := taskListenReturns(t, server)
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	client := NewClient(&Implementation{Name: "task-follow-client", Version: "v1.0.0"}, nil)
	held, hold := taskEvents[struct{}](t, "held notifications")
	late, deliverLate := taskTestGate()
	var recorder methodRecorder
	client.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if task, ok := req.GetParams().(*DetailedTask); ok && task != nil && task.Status.Terminal() {
				hold(struct{}{})
				<-late
			}
			return next(ctx, method, req)
		}
	}, recorder.middleware)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "session", session.Close)
	// Registered after the sessions' cleanups, so they run before them.
	t.Cleanup(deliverLate)
	t.Cleanup(releaseTask)

	result, err := session.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.Task()
	if !ok {
		t.Fatal("no task handle")
	}
	f := startFollow(t, session, task.TaskID, false)
	if first := f.next("the first snapshot"); first.Status != TaskStatusWorking {
		t.Fatalf("first snapshot: %s", first.Status)
	}
	f.subscribed()
	releaseTask()
	followAwait(f, held, "the terminal notification to reach the client")
	// The notification is parked. FollowTask learns of the end from a poll.
	f.fire()
	if last := f.next("the snapshot of the poll"); last.Status != TaskStatusCompleted {
		t.Fatalf("snapshot of the poll: %s", last.Status)
	}
	if outcome := f.end("FollowTask's return"); outcome.err != nil || outcome.task.Status != TaskStatusCompleted {
		t.Fatalf("FollowTask = %+v, %v", outcome.task, outcome.err)
	}
	if session.taskReceive.Load() != nil {
		t.Fatal("the session has a follow table after FollowTask")
	}
	recorder.take()
	deliverLate()
	waitTaskListenReturned(t, listenReturned)
	if err := serverSession.Ping(ctx, nil); err != nil {
		t.Fatalf("ping after the late notification: %v", err)
	}
	// It was decoded while FollowTask ran, so receiving middleware is handed
	// it; the standard table then has no handler for it.
	if got, want := recorder.take(), "notifications/tasks["+task.TaskID+"] ping"; got != want {
		t.Fatalf("after FollowTask the client was handed %q, want %q", got, want)
	}
	if _, err := session.ListTools(ctx, nil); err != nil {
		t.Fatalf("the session stopped working: %v", err)
	}
}

func connectServerForTaskTest(t *testing.T, server *Server) Transport {
	t.Helper()
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(taskTestContext(t), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	return clientTransport
}

// The follow table is counted per session: it stays while any FollowTask
// runs and goes with the last one.
func TestTaskFollowTableIsRefcounted(t *testing.T) {
	session := connectTaskMemory(t, NewServer(testImpl, nil), NewClient(&Implementation{Name: "task-follow-client", Version: "v1.0.0"}, nil))
	first := session.registerTaskFollower("task-1")
	table := session.taskReceive.Load()
	if table == nil {
		t.Fatal("no follow table after the first follower")
	}
	second := session.registerTaskFollower("task-1")
	third := session.registerTaskFollower("task-2")
	if session.taskReceive.Load() != table {
		t.Fatal("the follow table was replaced while followers were registered")
	}
	session.unregisterTaskFollower("task-1", first)
	session.unregisterTaskFollower("task-2", third)
	if session.taskReceive.Load() != table {
		t.Fatal("the follow table went away with a follower still registered")
	}
	session.unregisterTaskFollower("task-1", second)
	if session.taskReceive.Load() != nil {
		t.Fatal("the follow table outlived the last follower")
	}
}

// A message reads the receiving table twice, and the table can change in
// between. Each mix of the standard and the follow entry for an
// acknowledgement is harmless: an acknowledgement decoded by the standard
// entry names no tasks and acknowledges none.
func TestTaskFollowTableSwapBetweenReads(t *testing.T) {
	ctx := taskTestContext(t)
	session := connectTaskMemory(t, NewServer(testImpl, nil), NewClient(&Implementation{Name: "task-follow-client", Version: "v1.0.0"}, nil))
	follower := session.registerTaskFollower("task-1")
	raw := json.RawMessage(`{"notifications":{"taskIds":["task-1"]}}`)
	standard := clientMethodInfos[notificationSubscriptionsAck]
	follow := taskFollowMethodInfos()[notificationSubscriptionsAck]
	handle := func(decode, handler methodInfo) {
		t.Helper()
		params, err := decode.unmarshalParams(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := handler.handleMethod(ctx, notificationSubscriptionsAck, handler.newRequest(session, params, nil, jsonrpc.ID{})); err != nil {
			t.Fatal(err)
		}
	}
	handle(standard, follow)
	handle(follow, standard)
	handle(standard, standard)
	if len(follower.subscribed) != 0 {
		t.Fatal("an acknowledgement not decoded and handled by the follow entry acknowledged a task")
	}
	handle(follow, follow)
	if len(follower.subscribed) != 1 {
		t.Fatal("the follow entry did not acknowledge the task")
	}
	// A task notification for a task nobody follows, or whose follower has
	// left, does nothing.
	session.unregisterTaskFollower("task-1", follower)
	follower = session.registerTaskFollower("task-2")
	defer session.unregisterTaskFollower("task-2", follower)
	notify := taskFollowMethodInfos()[notificationTaskStatus]
	for _, id := range []string{"nobody-follows-this", "task-1"} {
		params, err := notify.unmarshalParams(json.RawMessage(fmt.Sprintf(`{"taskId":%q,"status":"working"}`, id)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := notify.handleMethod(ctx, notificationTaskStatus, notify.newRequest(session, params, nil, jsonrpc.ID{})); err != nil {
			t.Fatal(err)
		}
	}
	if len(follower.ready) != 0 {
		t.Fatal("a notification for another task reached a follower")
	}
}

// tickTaskTimer is a poll timer the test fires. Every timer FollowTask makes
// shares one channel, so a tick reaches whichever timer is current.
type tickTaskTimer struct {
	ticks   chan time.Time
	stopped func() // called by stop, when set
}

func (t tickTaskTimer) channel() <-chan time.Time { return t.ticks }
func (t tickTaskTimer) stop() {
	if t.stopped != nil {
		t.stopped()
	}
}

type followOutcome struct {
	task *DetailedTask
	err  error
}

// fireTaskTimer delivers one poll tick to the follower. It fails at once if
// FollowTask returns instead of taking the tick.
func fireTaskTimer(t *testing.T, timer tickTaskTimer, followed <-chan followOutcome) {
	t.Helper()
	guard := taskGuard(t)
	defer guard.Stop()
	select {
	case timer.ticks <- time.Time{}:
	case outcome := <-followed:
		t.Fatalf("FollowTask returned before the poll: %+v, %v", outcome.task, outcome.err)
	case <-guard.C:
		taskGuardExpired(t, "timed out firing the poll timer")
	}
}

// A server that agrees to no task (its acknowledgement names none) and whose
// task notifications never arrive leaves the follower polling: the
// acknowledgement acknowledges nothing, and FollowTask ends by the poll.
func TestFollowTaskPollsWhenNoTaskIsAcknowledged(t *testing.T) {
	ctx := taskTestContext(t)
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	releaseTask := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseTask)
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, _ *CallToolRequest, _ *TaskContext) (*CallToolResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &CallToolResult{Content: []Content{&TextContent{Text: "polled"}}}, nil
		})
	// The server's acknowledgement loses its task ids and its task
	// notifications are dropped, as if the peer never sent either.
	dropped, drop := taskEvents[TaskStatus](t, "dropped notifications")
	server.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			switch params := req.GetParams().(type) {
			case *SubscriptionsAcknowledgedParams:
				params.Notifications.TaskIDs = nil
			case *DetailedTask:
				drop(params.Status)
				return nil, nil
			}
			return next(ctx, method, req)
		}
	})
	var getsMu sync.Mutex
	gets := 0
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if method == methodGetTask {
				getsMu.Lock()
				gets++
				getsMu.Unlock()
			}
			return next(ctx, method, req)
		}
	})
	client := NewClient(&Implementation{Name: "task-follow-client", Version: "v1.0.0"}, nil)
	acknowledged, acked := taskEvents[[]string](t, "acknowledgements")
	client.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			result, err := next(ctx, method, req)
			if ack, ok := req.GetParams().(*SubscriptionsAcknowledgedParams); ok && ack != nil {
				acked(ack.Notifications.TaskIDs)
			}
			return result, err
		}
	})
	session := connectTaskMemory(t, server, client)
	timer := tickTaskTimer{ticks: make(chan time.Time)}
	session.newTaskTimer = func(time.Duration) taskTimer { return timer }

	result, err := session.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.Task()
	if !ok {
		t.Fatal("no task handle")
	}
	followed := make(chan followOutcome, 1)
	snapshots, sawSnapshot := taskEvents[TaskStatus](t, "snapshots")
	go func() {
		task, err := session.FollowTask(ctx, task.TaskID, func(_ context.Context, snapshot *DetailedTask) error {
			sawSnapshot(snapshot.Status)
			return nil
		})
		followed <- followOutcome{task, err}
	}()
	// FollowTask has its first snapshot, and the acknowledgement has been
	// handled, naming no task. The two arrive in either order.
	if status := waitTask(t, snapshots, "the first snapshot"); status != TaskStatusWorking {
		t.Fatalf("first snapshot status = %q, want working", status)
	}
	if ids := waitTask(t, acknowledged, "the acknowledgement"); len(ids) != 0 {
		t.Fatalf("acknowledged task ids = %v, want none", ids)
	}
	releaseTask()
	// The state sent after the acknowledgement is dropped too.
	status := waitTask(t, dropped, "a dropped notification")
	for status == TaskStatusWorking {
		status = waitTask(t, dropped, "the dropped terminal notification")
	}
	if status != TaskStatusCompleted {
		t.Fatalf("dropped notification status = %q, want completed", status)
	}
	fireTaskTimer(t, timer, followed)
	outcome := waitTask(t, followed, "FollowTask")
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if outcome.task.Status != TaskStatusCompleted {
		t.Fatalf("final status = %q, want completed", outcome.task.Status)
	}
	getsMu.Lock()
	defer getsMu.Unlock()
	if gets != 2 {
		t.Fatalf("tasks/get requests = %d, want the first snapshot and one poll", gets)
	}
}

// Over stateless Streamable HTTP the listen request names the task on the
// wire, and the acknowledgement and the task's notifications arrive on the
// listen request's stream.
func TestTaskListenOverStreamableHTTP(t *testing.T) {
	ctx := taskTestContext(t)
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	releaseTask := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseTask)
	tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, _ *CallToolRequest, _ *TaskContext) (*CallToolResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &CallToolResult{Content: []Content{&TextContent{Text: "over http"}}}, nil
		})
	client := NewClient(&Implementation{Name: "task-follow-client", Version: "v1.0.0"}, nil)
	type acknowledgement struct {
		subscription string
		taskIDs      []string
	}
	acknowledged, acked := taskEvents[acknowledgement](t, "acknowledgements")
	client.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if ack, ok := req.GetParams().(*SubscriptionsAcknowledgedParams); ok && ack != nil {
				acked(acknowledgement{fmt.Sprint(ack.Meta[MetaKeySubscriptionID]), ack.Notifications.TaskIDs})
			}
			return next(ctx, method, req)
		}
	})
	session, posts := connectTaskHTTP(t, server, client)
	notifications, notified := taskEvents[*DetailedTask](t, "task notifications")
	observeTaskNotifications(session, notified)

	result, err := session.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.Task()
	if !ok {
		t.Fatal("no task handle")
	}
	posts.take("")
	listenCtx, cancelListen := context.WithCancel(ctx)
	defer cancelListen()
	session.listenForTask(listenCtx, task.TaskID)
	// Find the acknowledgement of this listen request by its subscription id
	// (an earlier listen of the client may be acknowledged too). The only
	// listen request sent from here on is this one, and it was recorded
	// before it was sent, so it is known when its acknowledgement arrives.
	// The request must name the task, and so must its acknowledgement.
	for listenID := ""; ; {
		ack := waitTask(t, acknowledged, "the acknowledgement of the task listen")
		if listenID == "" {
			for _, post := range posts.take(methodSubscriptionsListen) {
				if !strings.Contains(post.body, fmt.Sprintf(`"notifications":{"taskIds":[%q]}`, task.TaskID)) {
					t.Fatalf("the listen request does not name the task: %s", post.body)
				}
				if !post.declares {
					t.Fatalf("the task listen does not declare the extension: %s", post.body)
				}
				var request struct {
					ID any `json:"id"`
				}
				if err := json.Unmarshal([]byte(post.body), &request); err != nil {
					t.Fatal(err)
				}
				listenID = fmt.Sprint(request.ID)
			}
		}
		if listenID == "" || ack.subscription != listenID {
			continue
		}
		if len(ack.taskIDs) != 1 || ack.taskIDs[0] != task.TaskID {
			t.Fatalf("acknowledged task ids = %v, want %s", ack.taskIDs, task.TaskID)
		}
		break
	}
	releaseTask()
	for {
		notification := waitTask(t, notifications, "the terminal task notification")
		if notification.TaskID != task.TaskID {
			t.Fatalf("notification for task %q, want %q", notification.TaskID, task.TaskID)
		}
		if notification.Status.Terminal() {
			if notification.Status != TaskStatusCompleted {
				t.Fatalf("terminal notification status = %q", notification.Status)
			}
			break
		}
	}
}
