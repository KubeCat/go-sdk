// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/internal/jsonrpc2"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// A subscriber is sent the completed task, with its result and the
// subscription's id. In the second variant the subscriber lags: the task has
// completed before the listen is sent.
func TestTaskStatusNotification(t *testing.T) {
	for _, lag := range []bool{false, true} {
		name := "listen before completion"
		if lag {
			name = "listen after completion"
		}
		t.Run(name, func(t *testing.T) {
			ctx := taskTestContext(t)
			server := NewServer(testImpl, nil)
			store := newObservedTaskStore()
			tasks, err := NewTaskServer(server, &TaskServerOptions{Store: store})
			if err != nil {
				t.Fatal(err)
			}
			release, releaseTask := taskTestGate()
			tasks.AddTool(&Tool{Name: "work", InputSchema: json.RawMessage(`{"type":"object"}`)},
				func(ctx context.Context, req *CallToolRequest, taskContext *TaskContext) (*CallToolResult, error) {
					<-release
					return &CallToolResult{Content: []Content{&TextContent{Text: "finished"}}}, nil
				})

			client := NewClient(&Implementation{Name: "task-notification-client", Version: "v1.0.0"}, nil)
			notifications, notified := taskEvents[*DetailedTask](t, "task notifications")
			acknowledged, acked := taskEvents[*SubscriptionsAcknowledgedParams](t, "acknowledgements")
			client.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
				return func(ctx context.Context, method string, req Request) (Result, error) {
					if method == notificationSubscriptionsAck {
						acked(req.GetParams().(*SubscriptionsAcknowledgedParams))
					}
					return next(ctx, method, req)
				}
			})
			clientSession := connectTaskMemory(t, server, client)
			t.Cleanup(releaseTask)
			observeTaskNotifications(clientSession, notified)

			result, err := clientSession.CallTool(ctx, &CallToolParams{Name: "work", UseTasks: true})
			if err != nil {
				t.Fatal(err)
			}
			task := taskHandle(t, result)
			if lag {
				releaseTask()
				waitTask(t, store.done, "terminal task store update")
			}
			listenCtx, cancelListen := context.WithCancel(ctx)
			t.Cleanup(cancelListen)
			clientSession.listenForTask(listenCtx, task.TaskID)
			ack := waitTask(t, acknowledged, "task subscription acknowledgement")
			if len(ack.Notifications.TaskIDs) != 1 || ack.Notifications.TaskIDs[0] != task.TaskID {
				t.Fatalf("acknowledged tasks = %v", ack.Notifications.TaskIDs)
			}
			releaseTask()
			// The working state sent after the acknowledgement may come first.
			notification := waitTask(t, notifications, "task status notification")
			if notification.Status == TaskStatusWorking {
				notification = waitTask(t, notifications, "the completed task notification")
			}
			if notification.TaskID != task.TaskID || notification.Status != TaskStatusCompleted {
				t.Fatalf("notification = %+v", notification)
			}
			if notification.ResultType != "" {
				t.Fatalf("notification resultType = %q, want empty", notification.ResultType)
			}
			if notification.Meta[MetaKeySubscriptionID] != ack.Meta[MetaKeySubscriptionID] {
				t.Fatalf("subscription ID = %v, want %v", notification.Meta[MetaKeySubscriptionID], ack.Meta[MetaKeySubscriptionID])
			}
			toolResult, err := notification.ToolResult()
			if err != nil {
				t.Fatal(err)
			}
			if got := toolResult.Content[0].(*TextContent).Text; got != "finished" {
				t.Fatalf("notification result text = %q, want finished", got)
			}
		})
	}
}

func TestTaskSubscriptionWithoutTaskServerIsIgnored(t *testing.T) {
	server := NewServer(testImpl, nil)
	handler := NewStreamableHTTPHandler(func(*http.Request) *Server { return server }, &StreamableHTTPOptions{Stateless: true})
	defer handler.closeAll()
	params := map[string]any{
		"_meta": Meta{
			MetaKeyProtocolVersion:    protocolVersion20260728,
			MetaKeyClientInfo:         map[string]any{"name": "legacy-client", "version": "v1.0.0"},
			MetaKeyClientCapabilities: map[string]any{},
		},
		"notifications": map[string]any{"taskIds": []string{"legacy-custom-task"}},
	}
	testStreamableHandler(t, handler, []streamableRequest{{
		method: http.MethodPost,
		headers: http.Header{
			protocolVersionHeader: {protocolVersion20260728},
			methodHeader:          {methodSubscriptionsListen},
		},
		messages:           []jsonrpc.Message{req(1, methodSubscriptionsListen, params)},
		wantStatusCode:     http.StatusOK,
		wantBodyContaining: `"result"`,
	}})
}

func TestTaskSubscriptionErrors(t *testing.T) {
	server := NewServer(testImpl, nil)
	if _, err := NewTaskServer(server, nil); err != nil {
		t.Fatal(err)
	}
	requestID, err := jsonrpc.MakeID("1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(taskTestContext(t), idContextKey{}, requestID)
	req := &SubscriptionsListenRequest{Params: &SubscriptionsListenParams{
		Notifications: &NotificationSubscriptions{TaskIDs: []string{"unknown"}},
	}}
	if _, err := server.subscriptionsListen(ctx, req); errorCodeForTaskTest(err) != CodeMissingRequiredClientCapabilities {
		t.Fatalf("subscription without capability error = %v", err)
	}
}

func TestCustomTaskMethodsWithoutTaskServerDoNotRequireTaskHeaders(t *testing.T) {
	server := NewServer(testImpl, nil)
	called, call := taskEvents[string](t, "custom method calls")
	for _, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
		if err := AddReceivingCustomMethod(server, method, func(context.Context, *ServerSession, *GetTaskParams) (*emptyResult, error) {
			call(method)
			return &emptyResult{}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	handler := NewStreamableHTTPHandler(func(*http.Request) *Server { return server }, &StreamableHTTPOptions{Stateless: true})
	defer handler.closeAll()
	meta := Meta{
		MetaKeyProtocolVersion:    protocolVersion20260728,
		MetaKeyClientInfo:         map[string]any{"name": "legacy-client", "version": "v1.0.0"},
		MetaKeyClientCapabilities: map[string]any{},
	}
	var requests []streamableRequest
	for i, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
		requests = append(requests, streamableRequest{
			method: http.MethodPost,
			headers: http.Header{
				protocolVersionHeader: {protocolVersion20260728},
				methodHeader:          {method},
			},
			messages:           []jsonrpc.Message{req(int64(i+1), method, &GetTaskParams{TaskID: "custom", ParamsBase: ParamsBase{Meta: meta}})},
			wantStatusCode:     http.StatusOK,
			wantBodyContaining: `"jsonrpc"`,
		})
	}
	testStreamableHandler(t, handler, requests)
	if got := len(called); got != 3 {
		t.Fatalf("custom task methods called %d times, want all three", got)
	}
}

func TestTaskStreamableMissingCapabilityError(t *testing.T) {
	server := NewServer(testImpl, nil)
	if _, err := NewTaskServer(server, nil); err != nil {
		t.Fatal(err)
	}
	handler := NewStreamableHTTPHandler(func(*http.Request) *Server { return server }, &StreamableHTTPOptions{Stateless: true})
	defer handler.closeAll()
	meta := Meta{
		MetaKeyProtocolVersion:    protocolVersion20260728,
		MetaKeyClientInfo:         map[string]any{"name": "task-client", "version": "v1.0.0"},
		MetaKeyClientCapabilities: map[string]any{},
	}
	for _, withName := range []bool{false, true} {
		var requests []streamableRequest
		for i, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
			headers := http.Header{
				protocolVersionHeader: {protocolVersion20260728},
				methodHeader:          {method},
			}
			if withName {
				headers.Set(nameHeader, "task-1")
			}
			requests = append(requests, streamableRequest{
				method:             http.MethodPost,
				headers:            headers,
				messages:           []jsonrpc.Message{req(int64(i+1), method, &GetTaskParams{TaskID: "task-1", ParamsBase: ParamsBase{Meta: meta}})},
				wantStatusCode:     http.StatusBadRequest,
				wantBodyContaining: `-32021`,
			})
		}
		testStreamableHandler(t, handler, requests)
	}
}

// observeTaskNotifications makes session receive as it does while FollowTask
// runs, and hands every notifications/tasks to handler instead of to a
// follower.
func observeTaskNotifications(session *ClientSession, handler func(*DetailedTask)) {
	infos := taskFollowMethodInfos()
	infos[notificationTaskStatus] = newClientMethodInfo(func(_ context.Context, req *ClientRequest[*DetailedTask]) (*emptyResult, error) {
		handler(req.Params)
		return nil, nil
	}, notification)
	session.taskReceive.Store(&infos)
}

// The follow table starts from the table the session uses without a
// follower, which MCPGODEBUG=allowmissingclientparams=1 changes.
func TestTaskFollowTableKeepsParamsSetting(t *testing.T) {
	for _, legacy := range []string{"", "1"} {
		func() {
			defer func(old string) { allowmissingclientparams = old }(allowmissingclientparams)
			allowmissingclientparams = legacy
			standard := (&ClientSession{}).receivingMethodInfos()
			follow := taskFollowMethodInfos()
			for _, method := range []string{methodElicit, notificationResourceUpdated, notificationElicitationComplete, methodPing} {
				if got, want := follow[method].flags, standard[method].flags; got != want {
					t.Errorf("allowmissingclientparams=%q: the follow table's %s has flags %b, the standard table's %b", legacy, method, got, want)
				}
			}
			if (follow[methodElicit].flags&missingParamsOK != 0) != (legacy == "1") {
				t.Errorf("allowmissingclientparams=%q: the follow table's %s has flags %b", legacy, methodElicit, follow[methodElicit].flags)
			}
		}()
	}
}

// A session that has a follower receives with the follow table, also under
// MCPGODEBUG=allowmissingclientparams=1, which selects the table of a session
// without one.
func TestTaskFollowerReceivesWithFollowTable(t *testing.T) {
	for _, legacy := range []string{"", "1"} {
		func() {
			defer func(old string) { allowmissingclientparams = old }(allowmissingclientparams)
			allowmissingclientparams = legacy
			session := &ClientSession{}
			if _, ok := session.receivingMethodInfos()[notificationTaskStatus]; ok {
				t.Errorf("allowmissingclientparams=%q: a session without a follower receives %s", legacy, notificationTaskStatus)
			}
			follow := taskFollowMethodInfos()
			session.taskReceive.Store(&follow)
			if _, ok := session.receivingMethodInfos()[notificationTaskStatus]; !ok {
				t.Errorf("allowmissingclientparams=%q: a session with a follower does not receive %s", legacy, notificationTaskStatus)
			}
		}()
	}
}

// A task notification, and an acknowledgement that names tasks, sent on a
// closed connection fail with the error every other notification fails with.
func TestTaskNotificationsOnClosedConnection(t *testing.T) {
	ctx := taskTestContext(t)
	server := NewServer(testImpl, nil)
	if _, err := NewTaskServer(server, nil); err != nil {
		t.Fatal(err)
	}
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	session, err := NewClient(testImpl, nil).Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- serverSession.Wait() }()
	waitTask(t, waited, "the server's session to end")

	want := handleNotify(ctx, notificationToolListChanged, newServerRequest(serverSession, &ToolListChangedParams{}))
	if !errors.Is(want, ErrConnectionClosed) {
		t.Fatalf("a standard notification on the closed connection: %v; want ErrConnectionClosed", want)
	}
	sub := &taskSubscription{session: serverSession, requestID: jsonrpc2.Int64ID(7)}
	sent := map[string]error{
		notificationTaskStatus: sub.notify(ctx, &DetailedTask{Task: Task{TaskID: "some-task", Status: TaskStatusWorking}}),
		notificationSubscriptionsAck: handleNotify(sub.context(ctx), notificationSubscriptionsAck, newServerRequest(serverSession, &SubscriptionsAcknowledgedParams{
			Meta:          Meta{MetaKeySubscriptionID: sub.requestID.Raw()},
			Notifications: NotificationSubscriptions{TaskIDs: []string{"some-task"}},
		})),
	}
	for method, err := range sent {
		if !errors.Is(err, ErrConnectionClosed) || errors.Is(err, jsonrpc2.ErrConnClosing) || err.Error() != want.Error() {
			t.Errorf("%s on the closed connection: %v; want %v", method, err, want)
		}
	}
}
