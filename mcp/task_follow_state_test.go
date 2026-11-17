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
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func snapshotForTest(status TaskStatus, updated, message string) *DetailedTask {
	return &DetailedTask{Task: Task{TaskID: "task-1", Status: status, LastUpdatedAt: updated, StatusMessage: message}}
}

func TestTaskSnapshotSupersedes(t *testing.T) {
	const early, late = "2026-07-28T10:30:00Z", "2026-07-28T10:30:01Z"
	working := snapshotForTest(TaskStatusWorking, late, "halfway")
	with := func(change func(*DetailedTask)) *DetailedTask {
		task := *working
		change(&task)
		return &task
	}
	for _, test := range []struct {
		name          string
		current, next *DetailedTask
		want          bool
	}{
		{"newer", working, snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:02Z", "halfway"), true},
		{"older", working, snapshotForTest(TaskStatusWorking, early, "start"), false},
		{"older, by a nanosecond", working, snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:00.999999999Z", "start"), false},
		{"same state", working, with(func(*DetailedTask) {}), false},
		{"same state, another zone", working, snapshotForTest(TaskStatusWorking, "2026-07-28T12:30:01+02:00", "halfway"), false},
		{"same state from a poll and a notification", working, with(func(task *DetailedTask) {
			task.ResultType = resultTypeComplete
			task.Meta = Meta{MetaKeySubscriptionID: 3}
		}), false},
		{"same time, another message", working, with(func(task *DetailedTask) { task.StatusMessage = "nearly" }), true},
		{"same time, another status", working, with(func(task *DetailedTask) { task.Status = TaskStatusInputRequired }), true},
		{"same time, input requests", working, with(func(task *DetailedTask) { task.InputRequests = elicitInput("question") }), true},
		{"same time, an error", working, with(func(task *DetailedTask) { task.Error = &jsonrpc.Error{Code: 1} }), true},
		{"same time, a result", working, with(func(task *DetailedTask) { task.Result = json.RawMessage(`{}`) }), true},
		{"terminal after working, older", working, snapshotForTest(TaskStatusCompleted, early, ""), true},
		{"working after terminal, newer", snapshotForTest(TaskStatusCancelled, early, ""), snapshotForTest(TaskStatusWorking, late, ""), false},
		{"terminal after terminal, newer", snapshotForTest(TaskStatusCancelled, early, ""), snapshotForTest(TaskStatusCompleted, late, ""), false},
		{"times that are not timestamps, later", snapshotForTest(TaskStatusWorking, "5", ""), snapshotForTest(TaskStatusWorking, "6", ""), true},
		{"times that are not timestamps, earlier", snapshotForTest(TaskStatusWorking, "5", ""), snapshotForTest(TaskStatusWorking, "4", ""), false},
	} {
		if got := taskSnapshotSupersedes(test.current, test.next); got != test.want {
			t.Errorf("%s: taskSnapshotSupersedes = %t, want %t", test.name, got, test.want)
		}
	}
}

// A follower holds one snapshot from notifications, the newest: a stale
// arrival never overwrites a newer or a terminal one.
func TestTaskFollowerKeepsNewest(t *testing.T) {
	newFollower := func() *taskFollower { return &taskFollower{ready: make(chan struct{}, 1)} }
	take := func(f *taskFollower) string {
		task, err := f.take()
		if err != nil {
			return "error: " + err.Error()
		}
		if task == nil {
			return "nothing"
		}
		return string(task.Status) + " " + task.StatusMessage
	}

	f := newFollower()
	f.offer("task-1", snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:01Z", "one"))
	f.offer("task-1", snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:03Z", "three"))
	f.offer("task-1", snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:02Z", "two"))
	if got := take(f); got != "working three" || len(f.ready) != 1 {
		t.Errorf("after three notifications, the newest second: took %q, woken %t; want the newest", got, len(f.ready) == 1)
	}
	if got := take(f); got != "nothing" {
		t.Errorf("a second take returned %q", got)
	}

	f = newFollower()
	f.offer("task-1", snapshotForTest(TaskStatusCompleted, "2026-07-28T10:30:01Z", "done"))
	f.offer("task-1", snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:05Z", "stale"))
	if got := take(f); got != "completed done" {
		t.Errorf("after a terminal notification and a later working one: took %q, want the terminal one", got)
	}

	f = newFollower()
	f.offer("task-1", snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:01Z", "one"))
	f.offer("task-1", snapshotForTest("paused", "2026-07-28T10:30:02Z", ""))
	if got := take(f); !strings.Contains(got, "error: invalid task snapshot") {
		t.Errorf("after a notification with an unknown status: took %q, want an error", got)
	}
}

// followFixture runs FollowTask on a session whose poll timer the test
// fires and whose update callback the test can hold.
type followFixture struct {
	t        *testing.T
	session  *ClientSession
	timer    tickTaskTimer
	delays   <-chan time.Duration // the delay each poll timer was asked for
	updates  <-chan *DetailedTask
	polled   <-chan struct{} // one for each tasks/get whose answer FollowTask has
	followed chan followOutcome
	returned chan struct{}      // closed when FollowTask's outcome is in followed
	cancel   context.CancelFunc // ends the context FollowTask was called with
	// sparePolls counts the tasks/get reports a wait took from polled while
	// it waited for something else: polls counts them first.
	sparePolls int

	made         atomic.Int64    // poll timers FollowTask made
	stops        atomic.Int64    // poll timers FollowTask stopped
	stopped      <-chan struct{} // one for each
	settled      bool            // subscribed returned: no timer is stopped before the end
	settledStops int64           // the timers stopped when subscribed returned

	mu    sync.Mutex
	parks []chan struct{} // gates the callback has not reached yet
	gates []func()
}

func startFollow(t *testing.T, session *ClientSession, id string, hold bool) *followFixture {
	t.Helper()
	f := &followFixture{
		t:        t,
		session:  session,
		timer:    tickTaskTimer{ticks: make(chan time.Time)},
		followed: make(chan followOutcome, 1),
		returned: make(chan struct{}),
	}
	delays, delay := taskEvents[time.Duration](t, "poll timers")
	updates, updated := taskEvents[*DetailedTask](t, "snapshots given to the callback")
	polled, poll := taskEvents[struct{}](t, "tasks/get calls")
	stopped, stop := taskEvents[struct{}](t, "stopped poll timers")
	f.delays, f.updates, f.polled, f.stopped = delays, updates, polled, stopped
	f.timer.stopped = func() {
		f.stops.Add(1)
		stop(struct{}{})
	}
	if hold {
		f.park()
	}
	// Registered after the session's cleanups, so it runs before them.
	t.Cleanup(f.release)
	session.newTaskTimer = func(d time.Duration) taskTimer {
		f.made.Add(1)
		delay(d)
		return f.timer
	}
	// The middleware runs on the goroutine that called GetTask: when it
	// reports, that goroutine has the answer.
	session.client.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			result, err := next(ctx, method, req)
			if method == methodGetTask && req.GetSession() == Session(session) {
				poll(struct{}{})
			}
			return result, err
		}
	})
	ctx, cancel := context.WithCancel(taskTestContext(t))
	t.Cleanup(cancel)
	f.cancel = cancel
	go func() {
		task, err := session.FollowTask(ctx, id, func(_ context.Context, task *DetailedTask) error {
			// Whether this snapshot parks is decided before the test sees it,
			// so a park the test asks for after seeing it is for a later one.
			gate := f.nextPark()
			updated(task)
			if gate != nil {
				<-gate
			}
			return nil
		})
		f.followed <- followOutcome{task, err}
		close(f.returned)
	}()
	return f
}

// lagUntilReturned makes the test lag behind FollowTask: it waits until
// FollowTask has returned, without taking its outcome. The waits of a test
// must give the same answer after it as before.
func (f *followFixture) lagUntilReturned() {
	f.t.Helper()
	waitTask(f.t, f.returned, "FollowTask to return")
}

// lagUntilPolled makes the test lag behind FollowTask: it waits until
// FollowTask has the answer of one more tasks/get, and leaves that poll to be
// counted by the next call of polls.
func (f *followFixture) lagUntilPolled() {
	f.t.Helper()
	followAwait(f, f.polled, "a further poll")
	f.sparePolls++
}

// park makes the callback wait, after it reported the next snapshot it is
// given, until release.
func (f *followFixture) park() {
	gate := make(chan struct{})
	f.mu.Lock()
	defer f.mu.Unlock()
	f.parks = append(f.parks, gate)
	f.gates = append(f.gates, sync.OnceFunc(func() { close(gate) }))
}

func (f *followFixture) nextPark() chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.parks) == 0 {
		return nil
	}
	gate := f.parks[0]
	f.parks = f.parks[1:]
	return gate
}

// release lets a parked callback go on, and cancels the parks it has not
// reached.
func (f *followFixture) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, open := range f.gates {
		open()
	}
	f.parks = nil
}

// fire delivers one poll tick. It fails at once if FollowTask returns instead
// of taking it, or, after subscribed, if FollowTask stopped its poll timer.
func (f *followFixture) fire() {
	f.t.Helper()
	guard := taskGuard(f.t)
	defer guard.Stop()
	for {
		if f.settled && f.stops.Load() != f.settledStops {
			f.t.Fatal("FollowTask stopped its poll timer after its listen was acknowledged: it no longer polls")
		}
		select {
		case f.timer.ticks <- time.Time{}:
			return
		case <-f.stopped:
			// A timer was stopped, now or earlier: look again.
		case outcome := <-f.followed:
			f.t.Fatalf("FollowTask returned before the poll: %+v, %v", outcome.task, outcome.err)
		case <-guard.C:
			taskGuardExpired(f.t, "timed out firing the poll timer")
		}
	}
}

// fireOrReturned is fire for a test in which FollowTask may rightly have
// returned before the tick is taken. It reports whether the tick was taken;
// if not, FollowTask has returned and its outcome is still in followed.
func (f *followFixture) fireOrReturned() bool {
	f.t.Helper()
	guard := taskGuard(f.t)
	defer guard.Stop()
	for {
		select {
		case f.timer.ticks <- time.Time{}:
			return true
		case <-f.stopped:
			// A timer was stopped, now or earlier: look again.
		case <-f.returned:
			return false
		case <-guard.C:
			taskGuardExpired(f.t, "timed out firing the poll timer")
		}
	}
}

// polls waits until FollowTask has the answers of n more tasks/get calls.
// FollowTask returning instead is a failure, reported at once.
func (f *followFixture) polls(n int, waiting string) {
	f.t.Helper()
	for range n {
		if f.sparePolls > 0 {
			f.sparePolls--
			continue
		}
		followAwait(f, f.polled, waiting)
	}
}

// subscribed waits until FollowTask has its first snapshot and has polled
// again after the acknowledgement of its listen. No tick was fired, so the
// acknowledgement is the only reason for the second poll. From here
// FollowTask learns of a change from a notification or from the poll timer.
//
// FollowTask makes one poll timer when it starts, and after that only when
// one has fired; a notification does not restart it. So here exactly one
// was made and none was stopped, and from here a timer is stopped only when
// FollowTask ends: one stopped earlier is a failure, which fire reports.
func (f *followFixture) subscribed() {
	f.t.Helper()
	f.polls(2, "the first poll and the poll after the acknowledgement")
	f.settledStops = f.stops.Load()
	if made := f.made.Load(); made != 1 || f.settledStops != 0 {
		f.t.Fatalf("after the acknowledgement FollowTask has made %d poll timers and stopped %d; want one made, none stopped", made, f.settledStops)
	}
	f.settled = true
}

// timers reports how many poll timers FollowTask made and how many of them
// it stopped.
func (f *followFixture) timers() (made, stopped int64) { return f.made.Load(), f.stops.Load() }

// next returns the next snapshot the callback is given. FollowTask returning
// instead is a failure, reported at once.
func (f *followFixture) next(waiting string) *DetailedTask {
	f.t.Helper()
	return followAwait(f, f.updates, waiting)
}

// end returns FollowTask's outcome. A further snapshot handed to the callback
// instead is a failure, reported at once.
func (f *followFixture) end(waiting string) followOutcome {
	f.t.Helper()
	guard := taskGuard(f.t)
	defer guard.Stop()
	select {
	case outcome := <-f.followed:
		if len(f.updates) != 0 {
			f.t.Fatalf("waiting for %s: the callback was given %d more snapshots, first %+v", waiting, len(f.updates), (<-f.updates).Task)
		}
		return outcome
	case task := <-f.updates:
		f.t.Fatalf("waiting for %s: the callback was given %s %q (%s)", waiting, task.Status, task.StatusMessage, task.LastUpdatedAt)
	case <-guard.C:
		taskGuardExpired(f.t, "timed out waiting for %s", waiting)
	}
	panic("unreachable")
}

// Two notifications that arrive in the wrong order, the terminal one first
// and a working one after it, while FollowTask is busy: FollowTask ends with
// the terminal one and the callback never sees the other.
func TestFollowTaskOutOfOrderNotifications(t *testing.T) {
	for _, lag := range []bool{false, true} {
		name := "test waiting"
		if lag {
			name = "test behind FollowTask"
		}
		t.Run(name, func(t *testing.T) {
			session := connectTaskMemory(t, newTaskServerWithTask(t, "task-1"), NewClient(testImpl, nil))
			f := startFollow(t, session, "task-1", true)
			if first := f.next("the first snapshot"); first.Status != TaskStatusWorking {
				t.Fatalf("first snapshot = %s, want working", first.Status)
			}
			// FollowTask is parked in the callback.
			ctx := taskTestContext(t)
			session.deliverTaskNotification(ctx, snapshotForTest(TaskStatusCompleted, "2026-07-28T10:30:02Z", "done"))
			session.deliverTaskNotification(ctx, snapshotForTest(TaskStatusWorking, "2026-07-28T10:30:01Z", "stale"))
			f.release()
			if lag {
				f.lagUntilReturned()
			}
			if second := f.next("the terminal snapshot"); second.Status != TaskStatusCompleted || second.StatusMessage != "done" {
				t.Fatalf("second snapshot = %s %q, want the completed one", second.Status, second.StatusMessage)
			}
			outcome := f.end("FollowTask")
			if outcome.err != nil || outcome.task.Status != TaskStatusCompleted {
				t.Fatalf("FollowTask = %+v, %v; want the completed task", outcome.task, outcome.err)
			}
		})
	}
}

// The callback is given each state of the task once, whether a notification
// or a poll brought it, and however often it arrives.
func TestFollowTaskOneUpdatePerState(t *testing.T) {
	fixture := newDeliveryFixture(t)
	session := connectTaskMemory(t, fixture.server, NewClient(testImpl, nil))
	t.Cleanup(fixture.release)
	ctx := taskTestContext(t)
	result, err := session.CallTool(ctx, &CallToolParams{Name: "work", Arguments: map[string]any{"lane": "a"}, UseTasks: true})
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.Task()
	if !ok {
		t.Fatal("no task handle")
	}
	f := startFollow(t, session, task.TaskID, false)
	first := f.next("the first snapshot")
	if first.Status != TaskStatusWorking || first.StatusMessage != "" {
		t.Fatalf("first snapshot = %s %q", first.Status, first.StatusMessage)
	}
	// The same state again: as the server's notification after the
	// acknowledgement, as a notification of the test's own, and as a poll.
	again := *first
	again.ResultType = ""
	again.Meta = Meta{MetaKeySubscriptionID: 7}
	session.deliverTaskNotification(ctx, &again)
	f.fire()

	fixture.step("a", setTaskMessage("halfway"))
	if second := f.next("the changed task"); second.StatusMessage != "halfway" {
		t.Fatalf("second snapshot = %s %q, want the change", second.Status, second.StatusMessage)
	}
	f.fire()
	fixture.lanes["a"].finish()
	if third := f.next("the completed task"); third.Status != TaskStatusCompleted {
		t.Fatalf("third snapshot = %s %q, want completed", third.Status, third.StatusMessage)
	}
	if outcome := f.end("FollowTask"); outcome.err != nil || outcome.task.Status != TaskStatusCompleted {
		t.Fatalf("FollowTask = %+v, %v", outcome.task, outcome.err)
	}
	fixture.noViolation()
}

// A snapshot that is not about the followed task, or has an unknown status,
// ends FollowTask with an error; the callback never sees it.
func TestFollowTaskInvalidSnapshot(t *testing.T) {
	for _, test := range []struct {
		name   string
		answer Task
	}{
		{"poll about another task", Task{TaskID: "another", Status: TaskStatusWorking}},
		{"poll with an unknown status", Task{TaskID: "task-1", Status: "paused"}},
		{"poll without a status", Task{TaskID: "task-1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(testImpl, nil)
			if err := AddReceivingCustomMethod(server, methodGetTask, func(context.Context, *ServerSession, *GetTaskParams) (*DetailedTask, error) {
				return &DetailedTask{Task: test.answer}, nil
			}); err != nil {
				t.Fatal(err)
			}
			f := startFollow(t, connectTaskMemory(t, server, NewClient(testImpl, nil)), "task-1", false)
			if outcome := f.end("FollowTask"); outcome.err == nil || !strings.Contains(outcome.err.Error(), "invalid task snapshot") {
				t.Fatalf("FollowTask = %+v, %v; want an invalid-snapshot error", outcome.task, outcome.err)
			}
		})
	}
	t.Run("notification with an unknown status", func(t *testing.T) {
		session := connectTaskMemory(t, newTaskServerWithTask(t, "task-1"), NewClient(testImpl, nil))
		f := startFollow(t, session, "task-1", true)
		f.next("the first snapshot")
		session.deliverTaskNotification(taskTestContext(t), snapshotForTest("paused", "2026-07-28T10:30:02Z", ""))
		f.release()
		if outcome := f.end("FollowTask"); outcome.err == nil || !strings.Contains(outcome.err.Error(), "invalid task snapshot") {
			t.Fatalf("FollowTask = %+v, %v; want an invalid-snapshot error", outcome.task, outcome.err)
		}
	})
}

// FollowTask polls at the interval the server suggests, and at a second when
// it suggests none.
func TestFollowTaskPollInterval(t *testing.T) {
	for _, test := range []struct {
		interval time.Duration
		want     time.Duration
	}{
		{7 * time.Second, 7 * time.Second},
		{0, time.Second},
	} {
		t.Run(fmt.Sprint(test.interval), func(t *testing.T) {
			server := NewServer(testImpl, nil)
			tasks, err := NewTaskServer(server, &TaskServerOptions{DefaultPollInterval: test.interval})
			if err != nil {
				t.Fatal(err)
			}
			release, releaseTask := taskTestGate()
			tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
				func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
					<-release
					return &CallToolResult{}, nil
				})
			session := connectTaskMemory(t, server, NewClient(testImpl, nil))
			t.Cleanup(releaseTask)
			result, err := session.CallTool(taskTestContext(t), &CallToolParams{Name: "work", UseTasks: true})
			if err != nil {
				t.Fatal(err)
			}
			task, ok := result.Task()
			if !ok {
				t.Fatal("no task handle")
			}
			f := startFollow(t, session, task.TaskID, false)
			f.next("the first snapshot")
			if got := waitTask(t, f.delays, "the poll timer"); got != test.want {
				t.Errorf("the poll timer was asked for %v, want %v", got, test.want)
			}
			// A poll that finds the task running asks for the same interval.
			f.fire()
			for {
				if got := waitTask(t, f.delays, "the next poll timer"); got != test.want {
					t.Errorf("the next poll timer was asked for %v, want %v", got, test.want)
				}
				if len(f.delays) == 0 {
					break
				}
			}
		})
	}
}

// followAwait waits for an event FollowTask causes before it returns.
// FollowTask returning instead is a failure, reported at once.
//
// When the test lags, the event and FollowTask's outcome may both be there.
// Every event of this fixture is reported before FollowTask can go on, so the
// event came first: it is returned, and the outcome is left for end. Only an
// outcome with no event before it is the failure.
func followAwait[T any](f *followFixture, ch <-chan T, waiting string) T {
	f.t.Helper()
	guard := taskGuard(f.t)
	defer guard.Stop()
	select {
	case value := <-ch:
		return value
	case outcome := <-f.followed:
		select {
		case value := <-ch:
			// FollowTask sends one outcome, and it was just taken: this
			// does not block.
			f.followed <- outcome
			return value
		default:
		}
		f.t.Fatalf("waiting for %s: FollowTask returned %+v, %v", waiting, outcome.task, outcome.err)
	case <-guard.C:
		taskGuardExpired(f.t, "timed out waiting for %s", waiting)
	}
	panic("unreachable")
}
