// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The task tests use time only as a hang guard. A wait in a test gives up after
// taskTestWait; the context of a client call ends later, after
// taskTestCallTimeout, so that when a test is stuck its wait reports the
// failure, not a call that happens to be in flight.
//
// A guard that expires means the subject is broken in a way that makes
// nothing happen, and then most of the tasks tests would each wait as long,
// one after another, past any timeout of the test binary. So the first expiry
// is recorded (taskTestHung), and from then on every wait and every call
// context of these tests fails its test at once, naming the first: the run
// ends with the failure that matters and a list of the tests not run.
const (
	taskTestWait        = 30 * time.Second
	taskTestCallTimeout = time.Minute
	// taskTestHungWait is how long a test's cleanup waits for a session to
	// close once a guard has expired: the run has failed, and this only
	// bounds how long the report takes.
	taskTestHungWait = time.Second
)

// taskTestHung describes the first hang guard that expired in this run of the
// test binary, or is nil.
var taskTestHung atomic.Pointer[string]

// taskGuardRecord records an expired hang guard.
func taskGuardRecord(t testing.TB, what string) {
	what = t.Name() + ": " + what
	taskTestHung.CompareAndSwap(nil, &what)
}

// taskGuardExpired fails the test whose hang guard expired, and records the
// expiry.
func taskGuardExpired(t testing.TB, format string, args ...any) {
	t.Helper()
	what := fmt.Sprintf(format, args...)
	taskGuardRecord(t, what)
	t.Fatal(what)
}

// taskGuardCheck fails the test at once if a hang guard expired earlier in
// the run.
func taskGuardCheck(t testing.TB) {
	t.Helper()
	if first := taskTestHung.Load(); first != nil {
		t.Fatalf("not run to its end: a hang guard expired earlier in this run (%s)", *first)
	}
}

// taskGuard returns the hang guard of one wait. The caller selects on its
// channel and calls taskGuardExpired when it fires.
func taskGuard(t testing.TB) *time.Timer {
	t.Helper()
	taskGuardCheck(t)
	return time.NewTimer(taskTestWait)
}

// taskCleanupWait returns how long a cleanup waits for a Close to return.
func taskCleanupWait() time.Duration {
	if taskTestHung.Load() != nil {
		return taskTestHungWait
	}
	return taskTestWait
}

// taskTestContext returns the context for the client calls of a test: a lost
// reply ends the call instead of hanging the test. A call that ends that way
// counts as an expired hang guard.
func taskTestContext(t *testing.T) context.Context {
	t.Helper()
	taskGuardCheck(t)
	ctx, cancel := context.WithTimeout(t.Context(), taskTestCallTimeout)
	t.Cleanup(cancel)
	context.AfterFunc(ctx, func() {
		if ctx.Err() == context.DeadlineExceeded {
			taskGuardRecord(t, "the context of the test's client calls ended after "+taskTestCallTimeout.String())
		}
	})
	return ctx
}

func waitTask[T any](t *testing.T, ch <-chan T, waiting string) T {
	t.Helper()
	timer := taskGuard(t)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		taskGuardExpired(t, "timed out waiting for %s", waiting)
		var zero T
		return zero
	}
}

// taskEvents returns a channel of events and the function that reports one.
// Handlers and middleware report with it: it never blocks, so an event the
// test did not expect cannot park a goroutine its connection waits for. The
// channel holds far more events than a test expects; one more is a failure.
func taskEvents[T any](t *testing.T, what string) (<-chan T, func(T)) {
	t.Helper()
	events := make(chan T, 256)
	var overflow atomic.Bool
	t.Cleanup(func() {
		if overflow.Load() {
			t.Errorf("more than %d events: %s", cap(events), what)
		}
	})
	return events, func(event T) {
		select {
		case events <- event:
		default:
			overflow.Store(true)
		}
	}
}

// taskTestGate returns a channel that parks whatever waits on it and the
// function that closes it, which may be called more than once. The test hands
// open to t.Cleanup after it has connected its sessions: cleanups run last in
// first out, so the gate then opens before the sessions close, and a failing
// test does not leave a goroutine parked that a session's Close waits for.
func taskTestGate() (<-chan struct{}, func()) {
	gate := make(chan struct{})
	return gate, sync.OnceFunc(func() { close(gate) })
}

// taskTestStop returns a function that gives a tool handler a context which
// also ends when stop is called, and stop. A task's handler runs with a
// context its session does not end, so a handler a test leaves waiting, for
// input or for its context, is released by stop: the test hands stop to
// t.Cleanup after it has connected its sessions (see taskTestGate).
func taskTestStop() (handlerContext func(context.Context) context.Context, stop func()) {
	stopped, stop := context.WithCancel(context.Background())
	return func(ctx context.Context) context.Context {
		ctx, cancel := context.WithCancel(ctx)
		context.AfterFunc(stopped, cancel)
		return ctx
	}, stop
}

// closeTaskSession closes a session, or a connection, when the test ends. A
// Close that does not return is reported as a failure after the hang guard:
// with a broken subject a session may wait forever for one of its requests,
// and that must fail the test, not hang the test binary.
func closeTaskSession(t *testing.T, what string, closeSession func() error) {
	t.Helper()
	t.Cleanup(func() {
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			closeSession()
		}()
		timer := time.NewTimer(taskCleanupWait())
		defer timer.Stop()
		select {
		case <-closed:
		case <-timer.C:
			taskGuardRecord(t, fmt.Sprintf("closing %s did not return", what))
			t.Errorf("closing %s did not return", what)
		}
	})
}

// closeTaskHTTPServer closes a test HTTP server when the test ends. Close
// waits for the server's open requests; with a broken subject a stream may
// stay open forever. That is reported as a failure after the hang guard, and
// the connections are then cut so that the test binary ends.
func closeTaskHTTPServer(t *testing.T, server *httptest.Server) {
	t.Helper()
	t.Cleanup(func() {
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			server.Close()
		}()
		timer := time.NewTimer(taskCleanupWait())
		defer timer.Stop()
		select {
		case <-closed:
		case <-timer.C:
			taskGuardRecord(t, "closing the HTTP server did not return")
			t.Errorf("closing the HTTP server did not return: a request is still open")
			server.CloseClientConnections()
		}
	})
}

// taskHandle returns the task handle of the result of an opted-in call.
func taskHandle(t *testing.T, result *CallToolResult) *Task {
	t.Helper()
	task, ok := result.Task()
	if !ok {
		t.Fatalf("the call returned no task handle: %+v", result)
	}
	return task
}

func TestTaskStatusTerminal(t *testing.T) {
	tests := []struct {
		status TaskStatus
		want   bool
	}{
		{TaskStatusWorking, false},
		{TaskStatusInputRequired, false},
		{TaskStatusCompleted, true},
		{TaskStatusCancelled, true},
		{TaskStatusFailed, true},
		{TaskStatus("other"), false},
	}
	for _, test := range tests {
		if got := test.status.Terminal(); got != test.want {
			t.Errorf("%q.Terminal() = %v, want %v", test.status, got, test.want)
		}
	}
}

func TestCallToolResultTaskJSON(t *testing.T) {
	ttl := int64(60000)
	poll := int64(5000)
	task := &Task{
		TaskID:         "task-1",
		Status:         TaskStatusWorking,
		StatusMessage:  "waiting",
		CreatedAt:      "2026-07-28T10:30:00Z",
		LastUpdatedAt:  "2026-07-28T10:30:00Z",
		TTLMs:          &ttl,
		PollIntervalMs: &poll,
	}
	const created = `{"createdAt":"2026-07-28T10:30:00Z","lastUpdatedAt":"2026-07-28T10:30:00Z","pollIntervalMs":5000,"resultType":"task","status":"working","statusMessage":"waiting","taskId":"task-1","ttlMs":60000}`

	// A result the TaskServer created is written as a CreateTaskResult,
	// whatever result type the server stamped on it on the way out.
	result := &CallToolResult{task: task, taskCreated: true}
	result.setResultType(resultTypeComplete)
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != created {
		t.Fatalf("created task JSON = %s, want %s", data, created)
	}
	if got, ok := result.Task(); !ok || got.TaskID != "task-1" {
		t.Fatalf("Task() = %+v, %v", got, ok)
	} else {
		*got.TTLMs = 1
		if *result.task.TTLMs != ttl {
			t.Fatal("Task accessor did not return a copy")
		}
	}

	// The result's _meta is part of the CreateTaskResult.
	withMeta := &CallToolResult{Meta: Meta{"example/trace": "abc"}, task: task, taskCreated: true}
	if got, want := marshalForTaskTest(t, withMeta), `{"_meta":{"example/trace":"abc"},`+created[1:]; got != want {
		t.Fatalf("created task JSON with _meta = %s, want %s", got, want)
	}

	// Decoding is the unchanged SDK's: no task, and the value marshals as the unchanged SDK did.
	var decoded CallToolResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if got, ok := decoded.Task(); ok || got != nil || decoded.taskCreated {
		t.Fatalf("decoded result carries a task: %+v", decoded)
	}
	again, err := json.Marshal(&decoded)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"content":[],"resultType":"task"}`; string(again) != want {
		t.Fatalf("decoded task-shaped result marshals as %s, want %s", again, want)
	}
	decoded.setResultType(resultTypeComplete)
	if decoded.resultType != resultTypeComplete {
		t.Fatalf("setResultType left %q", decoded.resultType)
	}

	// A handle alone, as an opted-in CallTool returns it, changes no JSON.
	handle := &CallToolResult{Content: []Content{&TextContent{Text: "kept"}}, task: task}
	handle.setResultType(resultTypeComplete)
	data, err = json.Marshal(handle)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"content":[{"type":"text","text":"kept"}],"resultType":"complete"}`; string(data) != want {
		t.Fatalf("result with a handle marshals as %s, want %s", data, want)
	}
}

func TestCallToolResultOrdinaryJSONUnchanged(t *testing.T) {
	result := &CallToolResult{Content: []Content{}}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"content":[]}`; string(data) != want {
		t.Fatalf("ordinary result JSON = %s, want %s", data, want)
	}
	if task, ok := result.Task(); ok || task != nil {
		t.Fatalf("ordinary result has task %+v", task)
	}
}

func TestDetailedTaskToolResult(t *testing.T) {
	task := &DetailedTask{
		Task:   Task{Status: TaskStatusCompleted},
		Result: json.RawMessage(`{"content":[{"type":"text","text":"done"}],"resultType":"complete"}`),
	}
	result, err := task.ToolResult()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Content) != 1 || result.Content[0].(*TextContent).Text != "done" {
		t.Fatalf("tool result = %+v", result)
	}
	task.Status = TaskStatusFailed
	if _, err := task.ToolResult(); err == nil {
		t.Fatal("ToolResult succeeded for a failed task")
	}
}

func TestTaskServerCapability(t *testing.T) {
	server := NewServer(testImpl, nil)
	if discoverAdvertisesTasks(t, installTestSession(t, server)) {
		t.Fatal("plain server advertises tasks")
	}
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := server.capabilities().Extensions[ExtensionTasks]; ok {
		t.Fatal("legacy handshake advertises tasks")
	}
	// Advertised only to a request of a version that has the extension, on a
	// connection that serves such a version.
	for _, test := range []struct {
		name      string
		request   string
		supported []string
		want      bool
	}{
		{"new request, new connection", protocolVersion20260728, []string{protocolVersion20260728, protocolVersion20251125}, true},
		{"legacy request", protocolVersion20251125, []string{protocolVersion20260728, protocolVersion20251125}, false},
		{"connection without the version", protocolVersion20260728, []string{protocolVersion20251125}, false},
	} {
		res := &DiscoverResult{SupportedVersions: test.supported, Capabilities: &ServerCapabilities{}}
		tasks.advertise(res, test.request)
		if _, got := res.Capabilities.Extensions[ExtensionTasks]; got != test.want {
			t.Errorf("%s: advertised = %v, want %v", test.name, got, test.want)
		}
	}
	session := installTestSession(t, server)
	if !discoverAdvertisesTasks(t, session) {
		t.Fatal("task server does not advertise tasks")
	}
	settings := session.InitializeResult().Capabilities.Extensions[ExtensionTasks]
	if settings == nil || len(settings.(map[string]any)) != 0 {
		t.Fatalf("task settings = %#v, want an empty object", settings)
	}
	if _, err := NewTaskServer(server, nil); err == nil {
		t.Fatal("second task server installation succeeded")
	}
}

// waitTaskOK is waitTask for a channel that may be closed: ok is false when
// it was.
func waitTaskOK[T any](t *testing.T, ch <-chan T, waiting string) (value T, ok bool) {
	t.Helper()
	timer := taskGuard(t)
	defer timer.Stop()
	select {
	case value, ok = <-ch:
		return value, ok
	case <-timer.C:
		taskGuardExpired(t, "timed out waiting for %s", waiting)
		return value, false
	}
}
