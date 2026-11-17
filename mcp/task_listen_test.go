// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

// taskListenReturns makes server report, for each subscriptions/listen
// request, when its handler has returned: true for a listen that named tasks.
func taskListenReturns(t *testing.T, server *Server) <-chan bool {
	t.Helper()
	returned, report := taskEvents[bool](t, "listen handlers that returned")
	server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			result, err := next(ctx, method, req)
			if listen, ok := req.GetParams().(*SubscriptionsListenParams); ok && listen != nil && listen.Notifications != nil {
				report(len(listen.Notifications.TaskIDs) > 0)
			}
			return result, err
		}
	})
	return returned
}

// waitTaskListenReturned waits until the handler of a listen that named tasks
// has returned.
func waitTaskListenReturned(t *testing.T, returned <-chan bool) {
	t.Helper()
	for !waitTask(t, returned, "the handler of the task listen to return") {
	}
}

// listChangedSubscriptions returns how many sessions the server holds a
// tools, a prompts and a resources list-changed subscription for.
func listChangedSubscriptions(server *Server) [3]int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return [3]int{len(server.toolChangeSubscriptions), len(server.promptChangeSubscriptions), len(server.resourceChangeSubscriptions)}
}

// taskListenAcks makes server report each subscriptions acknowledgement it
// sends. The server registers a listen's list-changed subscriptions before it
// acknowledges the listen.
func taskListenAcks(t *testing.T, server *Server) <-chan struct{} {
	t.Helper()
	acks, report := taskEvents[struct{}](t, "subscription acknowledgements")
	server.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if method == notificationSubscriptionsAck {
				report(struct{}{})
			}
			return next(ctx, method, req)
		}
	})
	return acks
}

// A client's list-changed subscriptions belong to the listen request Connect
// opened. FollowTask opens a second listen on the same session, for its task,
// and ends it when it returns: the first listen's subscriptions stay.
//
// Connect does not wait for its listen, and the server handles the session's
// requests concurrently: the test waits for the listen's acknowledgement. In
// the second variant the listen lags: the server holds it until a later
// request of the session was answered.
func TestFollowTaskKeepsListChangedSubscriptions(t *testing.T) {
	for _, lag := range []bool{false, true} {
		name := "listen handled at once"
		if lag {
			name = "listen handled after a later request"
		}
		t.Run(name, func(t *testing.T) {
			ctx := taskTestContext(t)
			server := NewServer(testImpl, nil)
			schema := json.RawMessage(`{"type":"object"}`)
			plain := func(context.Context, *CallToolRequest) (*CallToolResult, error) { return &CallToolResult{}, nil }
			server.AddTool(&Tool{Name: "first", InputSchema: schema}, plain)
			server.AddPrompt(&Prompt{Name: "first"}, func(context.Context, *GetPromptRequest) (*GetPromptResult, error) {
				return &GetPromptResult{}, nil
			})
			server.AddResource(&Resource{Name: "first", URI: "file:///first"}, func(context.Context, *ReadResourceRequest) (*ReadResourceResult, error) {
				return &ReadResourceResult{}, nil
			})
			tasks, err := NewTaskServer(server, nil)
			if err != nil {
				t.Fatal(err)
			}
			tasks.AddTool(&Tool{Name: "work", InputSchema: schema},
				func(context.Context, *CallToolRequest, *TaskContext) (*CallToolResult, error) {
					return &CallToolResult{}, nil
				})
			returned := taskListenReturns(t, server)
			acks := taskListenAcks(t, server)
			hold, release := taskTestGate()
			if !lag {
				release()
			}
			server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
				return func(ctx context.Context, method string, req Request) (Result, error) {
					if method == methodSubscriptionsListen {
						select {
						case <-hold:
						case <-ctx.Done():
						}
					}
					return next(ctx, method, req)
				}
			})
			changed, report := taskEvents[string](t, "list-changed notifications")
			client := NewClient(testImpl, &ClientOptions{
				ToolListChangedHandler:     func(context.Context, *ToolListChangedRequest) { report("tools") },
				PromptListChangedHandler:   func(context.Context, *PromptListChangedRequest) { report("prompts") },
				ResourceListChangedHandler: func(context.Context, *ResourceListChangedRequest) { report("resources") },
			})
			session := connectTaskMemory(t, server, client)
			t.Cleanup(release)
			if _, err := session.ListTools(ctx, nil); err != nil {
				t.Fatal(err)
			}
			release()
			waitTask(t, acks, "the acknowledgement of the listen Connect opened")
			if got := listChangedSubscriptions(server); got != [3]int{1, 1, 1} {
				t.Fatalf("list-changed subscriptions after Connect = %v, want one of each", got)
			}

			result, err := session.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
			if err != nil {
				t.Fatal(err)
			}
			task, ok := result.Task()
			if !ok {
				t.Fatal("no task handle")
			}
			if _, err := session.FollowTask(ctx, task.TaskID, nil); err != nil {
				t.Fatal(err)
			}
			waitTaskListenReturned(t, returned)
			if got := listChangedSubscriptions(server); got != [3]int{1, 1, 1} {
				t.Fatalf("list-changed subscriptions after FollowTask's listen ended = %v, want one of each", got)
			}
			// And the client is still told.
			server.AddTool(&Tool{Name: "second", InputSchema: schema}, plain)
			if got := waitTask(t, changed, "tools/list_changed after FollowTask"); got != "tools" {
				t.Fatalf("notification = %q, want tools", got)
			}
		})
	}
}

// A listen that names tasks and asks for list-changed notifications too
// removes its own subscriptions when it ends.
func TestTaskListenEndsItsOwnListChangedSubscriptions(t *testing.T) {
	f := newDeliveryFixture(t)
	f.server.AddTool(&Tool{Name: "first", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest) (*CallToolResult, error) { return &CallToolResult{}, nil })
	peer := f.peer()
	id := f.startTask(peer, "a")
	listen := f.listen(peer, fmt.Sprintf(`{"toolsListChanged":true,"taskIds":[%q]}`, id))
	f.readAck(peer)
	f.readTask(peer, "the task's state after the acknowledgement")
	subscriptions := func() int {
		f.server.mu.Lock()
		defer f.server.mu.Unlock()
		return len(f.server.toolChangeSubscriptions)
	}
	if got := subscriptions(); got != 1 {
		t.Fatalf("tool list-changed subscriptions during the listen = %d, want 1", got)
	}
	f.endListen(peer, listen)
	if got := subscriptions(); got != 0 {
		t.Fatalf("tool list-changed subscriptions after the listen = %d, want 0", got)
	}
}

// server/discover names the extension only where it can be used. A Streamable
// HTTP server with sessions serves no protocol version that has the
// extension, and refuses every request of one.
func TestDiscoverAdvertisesTasksOnlyWhereUsable(t *testing.T) {
	for _, test := range []struct {
		name    string
		options *StreamableHTTPOptions
		want    bool
	}{
		{"stateless", &StreamableHTTPOptions{Stateless: true}, true},
		{"with sessions", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(testImpl, nil)
			if _, err := NewTaskServer(server, nil); err != nil {
				t.Fatal(err)
			}
			status, messages := newRawTaskHTTP(t, server, test.options).post(rawTaskRequest(1, "server/discover", "", false), nil)
			if status != 200 || len(messages) != 1 {
				t.Fatalf("server/discover: status %d, answer %v", status, messages)
			}
			var result struct {
				SupportedVersions []string `json:"supportedVersions"`
				Capabilities      struct {
					Extensions map[string]any `json:"extensions"`
				} `json:"capabilities"`
			}
			if err := json.Unmarshal(decodeRawTaskMessage(t, messages[0]).Result, &result); err != nil {
				t.Fatal(err)
			}
			if _, got := result.Capabilities.Extensions[ExtensionTasks]; got != test.want {
				t.Fatalf("extension advertised = %v, want %v (supported versions %v)", got, test.want, result.SupportedVersions)
			}
		})
	}
}

// FollowTask opens its listen only on a server that advertises the
// extension. On a server without it FollowTask sends no subscriptions/listen:
// a server that does not yet keep subscriptions per listen ends the session's
// list-changed subscriptions when any listen ends, and the client would no
// longer be told of changes. On a server with it the listen ends when
// FollowTask returns, and the subscriptions of the listen Connect opened stay.
func TestFollowTaskListensOnlyWhereAdvertised(t *testing.T) {
	schema := json.RawMessage(`{"type":"object"}`)
	plain := func(context.Context, *CallToolRequest) (*CallToolResult, error) { return &CallToolResult{}, nil }
	for _, installed := range []bool{false, true} {
		name := "server without the extension"
		if installed {
			name = "server with the extension"
		}
		t.Run(name, func(t *testing.T) {
			ctx := taskTestContext(t)
			server := NewServer(testImpl, nil)
			server.AddTool(&Tool{Name: "first", InputSchema: schema}, plain)
			if installed {
				store := NewMemoryTaskStore()
				if _, err := NewTaskServer(server, &TaskServerOptions{Store: store}); err != nil {
					t.Fatal(err)
				}
				if err := store.Create(ctx, "", &DetailedTask{Task: Task{TaskID: "some-task", Status: TaskStatusCompleted}}); err != nil {
					t.Fatal(err)
				}
			}
			returned := taskListenReturns(t, server)
			acks := taskListenAcks(t, server)
			changed, report := taskEvents[string](t, "list-changed notifications")
			client := NewClient(testImpl, &ClientOptions{
				ToolListChangedHandler: func(context.Context, *ToolListChangedRequest) { report("tools") },
			})
			// The middleware counts a listen before it is sent. Connect sends
			// its listen from a goroutine of its own; the acknowledgement
			// waited for below shows that it was counted.
			var listens atomic.Int32
			client.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
				return func(ctx context.Context, method string, req Request) (Result, error) {
					if method == methodSubscriptionsListen {
						listens.Add(1)
					}
					return next(ctx, method, req)
				}
			})
			session := connectTaskMemory(t, server, client)
			waitTask(t, acks, "the acknowledgement of the listen Connect opened")
			if got := listChangedSubscriptions(server)[0]; got != 1 || listens.Load() != 1 {
				t.Fatalf("after Connect: %d tool list-changed subscriptions, %d listens sent; want one of each", got, listens.Load())
			}
			// FollowTask sends its listen on its own goroutine, before its
			// first tasks/get.
			_, err := session.FollowTask(ctx, "some-task", nil)
			want := int32(1)
			if installed {
				want = 2
				if err != nil {
					t.Fatalf("FollowTask: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "method not found") {
				t.Fatalf("FollowTask on a server without the extension: %v; want method not found", err)
			}
			if got := listens.Load(); got != want {
				t.Fatalf("the client has sent %d subscriptions/listen requests, want %d", got, want)
			}
			if installed {
				// The only listen that ends is FollowTask's.
				if named := waitTask(t, returned, "the handler of FollowTask's listen to return"); !named {
					t.Fatal("the listen that ended named no task")
				}
			}
			// Without the extension no listen ended. With it, the one that
			// ended took only its own subscriptions with it.
			if got := listChangedSubscriptions(server)[0]; got != 1 {
				t.Fatalf("tool list-changed subscriptions after FollowTask = %d, want 1", got)
			}
			// And the client is still told.
			server.AddTool(&Tool{Name: "second", InputSchema: schema}, plain)
			if got := waitTask(t, changed, "tools/list_changed after FollowTask"); got != "tools" {
				t.Fatalf("notification = %q, want tools", got)
			}
		})
	}
}

// Each listen of a session ends its own list-changed subscriptions and no
// other listen's, whether or not it names tasks, with and without the
// extension installed.
func TestListenWithoutTasksEndsOnlyItsOwnListChangedSubscriptions(t *testing.T) {
	for _, installed := range []bool{false, true} {
		name := "server without the extension"
		if installed {
			name = "server with the extension"
		}
		t.Run(name, func(t *testing.T) {
			server := NewServer(testImpl, nil)
			server.AddTool(&Tool{Name: "first", InputSchema: json.RawMessage(`{"type":"object"}`)},
				func(context.Context, *CallToolRequest) (*CallToolResult, error) { return &CallToolResult{}, nil })
			if installed {
				if _, err := NewTaskServer(server, nil); err != nil {
					t.Fatal(err)
				}
			}
			peer := newRawTaskPeer(t, server)
			const listen = `{"notifications":{"toolsListChanged":true},` + rawTaskMetaDeclaring + `}`
			readAck := func(waiting string) {
				t.Helper()
				if message := decodeRawTaskMessage(t, peer.read(waiting)); message.Method != notificationSubscriptionsAck {
					t.Fatalf("%s: got %s %s", waiting, message.Method, message.Params)
				}
			}
			listens := func() int {
				server.mu.Lock()
				defer server.mu.Unlock()
				n := 0
				for _, ids := range server.toolChangeSubscriptions {
					n += len(ids)
				}
				return n
			}
			first := peer.request(methodSubscriptionsListen, listen)
			readAck("the first listen's acknowledgement")
			second := peer.request(methodSubscriptionsListen, listen)
			readAck("the second listen's acknowledgement")
			if got := listens(); got != 2 {
				t.Fatalf("tool list-changed subscriptions during the listens = %d, want 2", got)
			}
			peer.cancel(first)
			if got := listens(); got != 1 {
				t.Fatalf("tool list-changed subscriptions after the first listen ended = %d, want 1", got)
			}
			peer.cancel(second)
			if got := listens(); got != 0 {
				t.Fatalf("tool list-changed subscriptions after both listens ended = %d, want 0", got)
			}
		})
	}
}

// taskWireLine is one message a [LoggingTransport] logged: "write" or "read",
// and the message's fields.
type taskWireLine struct {
	direction string
	message   map[string]json.RawMessage
}

// taskWireLog is the writer of a [LoggingTransport] that reports each message.
type taskWireLog struct{ report func(taskWireLine) }

func (w taskWireLog) Write(p []byte) (int, error) {
	if direction, body, ok := strings.Cut(strings.TrimSpace(string(p)), ": "); ok {
		var message map[string]json.RawMessage
		if json.Unmarshal([]byte(body), &message) == nil {
			w.report(taskWireLine{direction, message})
		}
	}
	return len(p), nil
}

// The listen FollowTask sends is cancelled when FollowTask ends only if the
// server has not answered it: a cancellation may name only a request that is
// still in progress, and a server ends a listen itself when it agrees to
// nothing in it. The answered half holds the state the other way round from
// a follower that lags: the answer is there, and the context is done, before
// the wait for either begins.
func TestTaskListenCancelledOnlyWhileUnanswered(t *testing.T) {
	ctx := taskTestContext(t)
	server := NewServer(testImpl, nil)
	entered, reportEntered := taskEvents[struct{}](t, "calls of the held tool")
	server.AddTool(&Tool{Name: "held", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, _ *CallToolRequest) (*CallToolResult, error) {
			reportEntered(struct{}{})
			<-ctx.Done()
			return nil, ctx.Err()
		})
	lines, report := taskEvents[taskWireLine](t, "wire messages")
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	session, err := NewClient(testImpl, nil).Connect(ctx, &LoggingTransport{Transport: clientTransport, Writer: taskWireLog{report}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "session", session.Close)
	conn := session.getConn()

	// Answered, then ended. Which of the two the wait reports first is
	// random, so it is tried several times.
	for range 10 {
		callCtx, cancel := context.WithCancel(ctx)
		call := conn.Call(callCtx, methodListTools, injectRequestMeta(session, &ListToolsParams{}))
		if err := call.Await(ctx, nil); err != nil {
			t.Fatalf("tools/list: %v", err)
		}
		cancel()
		awaitTaskListen(callCtx, conn, call)
	}

	// Ended while the server still works on it. Each wait above had
	// returned before the next message was written, so the first
	// cancellation on the wire is this call's, or one of an answered call.
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	call := conn.Call(callCtx, methodCallTool, injectRequestMeta(session, &CallToolParams{Name: "held", Arguments: map[string]any{}}))
	waitTask(t, entered, "the held tool to be called")
	cancel()
	awaitTaskListen(callCtx, conn, call)
	for {
		line := waitTask(t, lines, "the cancellation of the unanswered call")
		if line.direction != "write" || string(line.message["method"]) != fmt.Sprintf("%q", notificationCancelled) {
			continue
		}
		var params CancelledParams
		if err := json.Unmarshal(line.message["params"], &params); err != nil {
			t.Fatal(err)
		}
		if got, want := fmt.Sprint(params.RequestID), fmt.Sprint(call.ID().Raw()); got != want {
			t.Fatalf("the client cancelled request %s, which was answered; the unanswered one is %s", got, want)
		}
		break
	}
}
