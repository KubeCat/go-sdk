// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"time"

	internaljson "github.com/modelcontextprotocol/go-sdk/internal/json"
	"github.com/modelcontextprotocol/go-sdk/internal/jsonrpc2"
)

func tasksProtocolError() error {
	return fmt.Errorf("tasks extension requires protocol version %s or later", protocolVersion20260728)
}

// withTasksCapability returns a copy of meta whose client capabilities
// declare the tasks extension. It starts from the capabilities the caller put
// in meta, or else from the capabilities the client declares on every other
// request, so that an opted-in request does not hide the client's elicitation,
// sampling and roots support from the server.
func withTasksCapability(cs *ClientSession, meta Meta) Meta {
	meta = maps.Clone(meta)
	if meta == nil {
		meta = make(Meta)
	}
	var caps clientCapabilitiesV2
	if declared, ok := decodeMetaValue[*clientCapabilitiesV2](meta, MetaKeyClientCapabilities); ok && declared != nil {
		caps = *declared
	} else {
		caps = *cs.client.capabilities(cs.state.InitializeResult.ProtocolVersion).toV2()
	}
	caps.Extensions = maps.Clone(caps.Extensions)
	if caps.Extensions == nil {
		caps.Extensions = make(map[string]any)
	}
	if _, ok := caps.Extensions[ExtensionTasks]; !ok {
		caps.Extensions[ExtensionTasks] = map[string]any{}
	}
	meta[MetaKeyClientCapabilities] = &caps
	return meta
}

// taskFollowMethodInfos returns the receiving method table of a session on
// which FollowTask is running: the standard table, plus notifications/tasks,
// with an acknowledgement entry that also reads the task ids the server
// agreed to. The acknowledgement keeps its standard params type and its
// standard handler.
//
// The standard table is the one the session uses without a follower, which
// MCPGODEBUG=allowmissingclientparams=1 changes.
func taskFollowMethodInfos() map[string]methodInfo {
	standard := clientMethodInfos
	if allowmissingclientparams == "1" {
		standard = clientMethodInfosLegacyParams
	}
	infos := maps.Clone(standard)
	infos[notificationTaskStatus] = newClientMethodInfo(clientSessionMethod((*ClientSession).callTaskNotificationHandler), notification)
	ack := infos[notificationSubscriptionsAck]
	decode, handle := ack.unmarshalParams, ack.handleMethod
	ack.unmarshalParams = func(raw json.RawMessage) (Params, error) {
		params, err := decode(raw)
		if err != nil {
			return params, err
		}
		if acked, ok := params.(*SubscriptionsAcknowledgedParams); ok && acked != nil {
			// Lenient: a malformed list acknowledges no task, and the
			// acknowledgement is delivered as it is without the extension.
			if ids, _, wellFormed := subscriptionTaskIDs(raw); wellFormed {
				acked.Notifications.TaskIDs = ids
			}
		}
		return params, nil
	}
	ack.handleMethod = func(ctx context.Context, method string, req Request) (Result, error) {
		if acked, ok := req.(*ClientRequest[*SubscriptionsAcknowledgedParams]); ok && acked.Params != nil && acked.Session != nil {
			acked.Session.acknowledgeTasks(acked.Params.Notifications.TaskIDs)
		}
		return handle(ctx, method, req)
	}
	infos[notificationSubscriptionsAck] = ack
	return infos
}

// callTaskNotificationHandler handles notifications/tasks. It does nothing
// when no FollowTask is following the task.
func (cs *ClientSession) callTaskNotificationHandler(ctx context.Context, params *DetailedTask) (Result, error) {
	cs.deliverTaskNotification(ctx, params)
	return nil, nil
}

// taskListenWire is the wire form of the subscriptions/listen request
// FollowTask sends.
type taskListenWire struct {
	ParamsBase
	Notifications taskSubscriptionsWire `json:"notifications"`
}

// listenForTask asks the server for notifications/tasks about one task. The
// request stays open until ctx is done.
//
// The request goes through the session's sending middleware as a standard
// subscriptions/listen request; a [taskSend] replaces only the terminal, to
// write notifications.taskIds, which the shared type's JSON leaves out.
func (cs *ClientSession) listenForTask(ctx context.Context, id string) {
	params := injectRequestMeta(cs, &SubscriptionsListenParams{
		Meta:          withTasksCapability(cs, nil),
		Notifications: &NotificationSubscriptions{TaskIDs: []string{id}},
	})
	ctx = withTaskSend(ctx, taskSend{
		session: cs,
		method:  methodSubscriptionsListen,
		owns:    ownsParams(params),
		send: func(ctx context.Context, req Request) (Result, error) {
			listen := req.GetParams().(*SubscriptionsListenParams)
			wire := &taskListenWire{ParamsBase: ParamsBase{Meta: listen.Meta}}
			if listen.Notifications != nil {
				wire.Notifications = newTaskSubscriptionsWire(*listen.Notifications)
			}
			// The request is written before this returns, so the server has
			// it before the follower's first tasks/get. Its answer comes
			// when the listen ends, and is awaited off the follower's path.
			conn := req.GetSession().getConn()
			go awaitTaskListen(ctx, conn, conn.Call(ctx, methodSubscriptionsListen, wire))
			return &SubscriptionsListenResult{}, nil
		},
	})
	// A refusal leaves the follower without notifications, and it polls.
	_, _ = cs.sendingMethodHandler()(ctx, methodSubscriptionsListen, newClientRequest(cs, orZero[Params](params)))
}

// errTaskListenEnded is the error awaitTaskListen retires a listen with that
// the server has not answered. No call gets it any other way.
var errTaskListenEnded = errors.New("task listen ended")

// awaitTaskListen waits for the answer to the listen FollowTask sent, or for
// ctx to be done, and then cancels the listen if the server has not answered
// it. Only a request still in progress may be cancelled, and a server that
// agrees to nothing in a listen ends it itself.
func awaitTaskListen(ctx context.Context, conn *jsonrpc2.Connection, listen *jsonrpc2.AsyncCall) {
	if err := listen.Await(ctx, nil); err == nil || ctx.Err() == nil {
		return
	}
	// Await may report ctx although the answer is there as well. Retire does
	// nothing to a call that was answered, so the error the call holds
	// afterwards tells the two apart.
	conn.Retire(listen, errTaskListenEnded)
	if err := listen.Await(context.WithoutCancel(ctx), nil); err != errTaskListenEnded {
		return
	}
	notifyCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), notifyCancellationTimeout)
	defer stop()
	_ = conn.Notify(notifyCtx, notificationCancelled, &CancelledParams{
		Reason:    ctx.Err().Error(),
		RequestID: listen.ID().Raw(),
	})
}

// sendTaskRequest sends one tasks/get, tasks/update or tasks/cancel request.
//
// The method is not registered in the client's method table: the request goes
// through the session's sending middleware like any other and a [taskSend]
// replaces only the terminal, for this request alone. A program's own
// registration of the same method name, and [CallCustomMethod] for a name it
// did not register, behave as they do without the extension, before, during
// and after this call.
//
// If sending middleware replaces the params or does not pass on the context
// it was given, the override does not apply and the request is sent (or
// refused) as the program's method table says.
func sendTaskRequest[P Params, R any, PR interface {
	*R
	Result
}](ctx context.Context, cs *ClientSession, method, taskID string, params P) (PR, error) {
	ctx = withTaskSend(ctx, taskSend{
		session: cs,
		method:  method,
		owns:    ownsParams(params),
		send: func(ctx context.Context, req Request) (Result, error) {
			result := PR(new(R))
			checked := &taskCheckedResult{Result: result}
			if err := call(withTaskName(ctx, method, taskID), req.GetSession().getConn(), method, req.GetParams(), checked); err != nil {
				return nil, err
			}
			if checked.task {
				return nil, errTaskForRequest(method)
			}
			return result, nil
		},
	})
	result, err := cs.sendingMethodHandler()(ctx, method, newClientRequest(cs, params))
	if err != nil {
		return nil, err
	}
	typed, ok := result.(PR)
	if !ok {
		return nil, fmt.Errorf("calling %q: sending middleware returned a %T, want a %T", method, result, typed)
	}
	return typed, nil
}

// GetTask retrieves a complete snapshot of a task.
func (cs *ClientSession) GetTask(ctx context.Context, params *GetTaskParams) (*DetailedTask, error) {
	if !cs.usesNewProtocol() {
		return nil, tasksProtocolError()
	}
	if params == nil {
		params = new(GetTaskParams)
	} else {
		copy := *params
		params = &copy
	}
	params.Meta = withTasksCapability(cs, params.Meta)
	params = injectRequestMeta(cs, params)
	return sendTaskRequest[*GetTaskParams, DetailedTask](ctx, cs, methodGetTask, params.TaskID, params)
}

// UpdateTask supplies responses to a task's outstanding input requests.
func (cs *ClientSession) UpdateTask(ctx context.Context, params *UpdateTaskParams) error {
	if !cs.usesNewProtocol() {
		return tasksProtocolError()
	}
	if params == nil {
		params = new(UpdateTaskParams)
	} else {
		copy := *params
		params = &copy
	}
	params.Meta = withTasksCapability(cs, params.Meta)
	params = injectRequestMeta(cs, params)
	_, err := sendTaskRequest[*UpdateTaskParams, taskAckResult](ctx, cs, methodUpdateTask, params.TaskID, params)
	return err
}

// CancelTask cooperatively cancels a task.
func (cs *ClientSession) CancelTask(ctx context.Context, params *CancelTaskParams) error {
	if !cs.usesNewProtocol() {
		return tasksProtocolError()
	}
	if params == nil {
		params = new(CancelTaskParams)
	} else {
		copy := *params
		params = &copy
	}
	params.Meta = withTasksCapability(cs, params.Meta)
	params = injectRequestMeta(cs, params)
	_, err := sendTaskRequest[*CancelTaskParams, taskAckResult](ctx, cs, methodCancelTask, params.TaskID, params)
	return err
}

// A taskFollower is one FollowTask call's view of what the session receives
// about its task.
type taskFollower struct {
	ready      chan struct{} // holds a token while latest or invalid is set
	subscribed chan struct{}
	// inputKeys is shared by the FollowTask calls that follow the same task
	// on the session at the same time.
	inputKeys *taskInputKeys

	mu sync.Mutex
	// latest is the newest snapshot a notification brought that FollowTask
	// has not taken yet. A notification replaces it only with a snapshot
	// that supersedes it (taskSnapshotSupersedes), so a stale arrival never
	// overwrites a newer or a terminal one, and the follower holds one
	// snapshot however many arrive.
	latest *DetailedTask
	// invalid is the first notification that is not a valid snapshot.
	invalid error
}

// registerTaskFollower registers a FollowTask call for a task.
//
// While at least one FollowTask runs on the session, cs.taskReceive holds
// the follow table ([taskFollowMethodInfos]); otherwise it is nil and the
// session receives with clientMethodInfos, as it does without the extension.
// The switch happens here and in unregisterTaskFollower, under
// taskFollowersMu.
//
// A received message reads the table twice, without a lock: once to decode
// the params (handleReceive) and once to find the handler
// (defaultReceivingMethodHandler). The switch can land between the two
// reads, and both orders are harmless: the two follow handlers do nothing
// when no follower is registered for the task; an acknowledgement decoded by
// the standard entry names no tasks, so it acknowledges none and the
// follower keeps polling; the acknowledgement entry keeps the standard params
// type, so either handler takes either decode; and a notifications/tasks
// decoded under the follow table and then not found in the standard one
// ends as an unhandled notification, which is what it is without the
// extension.
func (cs *ClientSession) registerTaskFollower(id string) *taskFollower {
	follower := &taskFollower{
		ready:      make(chan struct{}, 1),
		subscribed: make(chan struct{}, 1),
	}
	cs.taskFollowersMu.Lock()
	if len(cs.taskFollowers) == 0 {
		cs.taskFollowers = make(map[string]map[*taskFollower]struct{})
		infos := taskFollowMethodInfos()
		cs.taskReceive.Store(&infos)
	}
	if cs.taskFollowers[id] == nil {
		cs.taskFollowers[id] = make(map[*taskFollower]struct{})
	}
	for other := range cs.taskFollowers[id] {
		follower.inputKeys = other.inputKeys
		break
	}
	if follower.inputKeys == nil {
		follower.inputKeys = &taskInputKeys{taken: make(map[string]struct{})}
	}
	cs.taskFollowers[id][follower] = struct{}{}
	cs.taskFollowersMu.Unlock()
	return follower
}

func (cs *ClientSession) unregisterTaskFollower(id string, follower *taskFollower) {
	cs.taskFollowersMu.Lock()
	delete(cs.taskFollowers[id], follower)
	if len(cs.taskFollowers[id]) == 0 {
		delete(cs.taskFollowers, id)
	}
	if len(cs.taskFollowers) == 0 {
		cs.taskReceive.Store(nil)
	}
	cs.taskFollowersMu.Unlock()
}

func (cs *ClientSession) taskFollowersFor(id string) []*taskFollower {
	cs.taskFollowersMu.Lock()
	defer cs.taskFollowersMu.Unlock()
	followers := make([]*taskFollower, 0, len(cs.taskFollowers[id]))
	for follower := range cs.taskFollowers[id] {
		followers = append(followers, follower)
	}
	return followers
}

func (cs *ClientSession) acknowledgeTasks(ids []string) {
	for _, id := range ids {
		for _, follower := range cs.taskFollowersFor(id) {
			select {
			case follower.subscribed <- struct{}{}:
			default:
			}
		}
	}
}

func (cs *ClientSession) deliverTaskNotification(_ context.Context, task *DetailedTask) {
	if task == nil {
		return
	}
	for _, follower := range cs.taskFollowersFor(task.TaskID) {
		clone, err := cloneDetailedTask(task)
		if err != nil {
			continue
		}
		follower.offer(task.TaskID, clone)
	}
}

// offer hands the follower a snapshot from a notification.
func (f *taskFollower) offer(id string, task *DetailedTask) {
	f.mu.Lock()
	if err := validateTaskSnapshot(id, task); err != nil {
		if f.invalid == nil {
			f.invalid = err
		}
	} else if f.latest == nil || taskSnapshotSupersedes(f.latest, task) {
		f.latest = task
	}
	f.mu.Unlock()
	select {
	case f.ready <- struct{}{}:
	default:
	}
}

// take returns what notifications brought since the last call: the newest
// snapshot, if any, or the error of an invalid one.
func (f *taskFollower) take() (*DetailedTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, err := f.latest, f.invalid
	f.latest, f.invalid = nil, nil
	return task, err
}

// taskInputKeys are the input request keys of one task that the FollowTask
// calls following it on one session have taken to answer. A request stays in
// the task's snapshots until the server has seen its answer, and every
// follower of the task sees every snapshot: a key is taken once, so the
// client's handler is asked once and one answer is sent.
type taskInputKeys struct {
	mu    sync.Mutex
	taken map[string]struct{}
}

// take returns the requests whose keys nobody has taken, and takes them.
func (k *taskInputKeys) take(requests InputRequestMap) InputRequestMap {
	k.mu.Lock()
	defer k.mu.Unlock()
	pending := make(InputRequestMap)
	for key, request := range requests {
		if _, ok := k.taken[key]; !ok {
			k.taken[key] = struct{}{}
			pending[key] = request
		}
	}
	return pending
}

// giveBack makes the keys of requests free again: they were not answered.
func (k *taskInputKeys) giveBack(requests InputRequestMap) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for key := range requests {
		delete(k.taken, key)
	}
}

type taskTimer interface {
	channel() <-chan time.Time
	stop()
}

type realTaskTimer struct {
	timer *time.Timer
}

func (t realTaskTimer) channel() <-chan time.Time { return t.timer.C }
func (t realTaskTimer) stop()                     { t.timer.Stop() }

const defaultTaskPollInterval = time.Second

// FollowTask follows a task until it reaches a terminal status and returns
// its final snapshot. It polls, at the interval the server suggests, and, if
// the server advertises the tasks extension, also asks it for task
// notifications; the polling recovers a lost notification.
//
// The update callback, which may be nil, is called once for each new state of
// the task, in order: a snapshot that shows nothing newer than one already
// delivered is dropped, whether it came from a notification or a poll, and
// nothing is delivered after a terminal snapshot. A snapshot of another task
// or with an unknown status ends FollowTask with an error.
//
// FollowTask answers the task's input requests. Each request of a snapshot
// with status input_required is handled as the same elicitation, sampling or
// roots request received from the server directly is: by the client's
// handler for it, as in a multi round-trip tool call. FollowTask sends the
// answers with tasks/update, after the callback has seen the snapshot, and
// answers each request key once, however many snapshots show it and however
// many FollowTask calls follow the task on the session at the same time. A
// request the client cannot answer, or an answer the server does not take,
// ends FollowTask with an error. Its requests are then unanswered: another
// FollowTask call that is still following the task asks for them again, when
// it next hears of the task, and answers them or ends with an error too.
func (cs *ClientSession) FollowTask(ctx context.Context, id string, update func(context.Context, *DetailedTask) error) (*DetailedTask, error) {
	if !cs.usesNewProtocol() {
		return nil, tasksProtocolError()
	}
	follower := cs.registerTaskFollower(id)
	defer cs.unregisterTaskFollower(id, follower)
	// The listen is one of its own, next to the one Connect opened for the
	// session's other subscriptions, and ends when FollowTask returns. It is
	// opened only on a server that advertises the extension. A server
	// without it knows nothing of task notifications, and one that does not
	// yet keep subscriptions per listen ends the session's other
	// subscriptions when any listen ends. The follower then polls.
	if cs.serverAdvertisesTasks() {
		listenCtx, cancelListen := context.WithCancel(ctx)
		defer cancelListen()
		cs.listenForTask(listenCtx, id)
	}

	var current *DetailedTask
	deliver := func(next *DetailedTask) (bool, error) {
		if next == nil {
			return false, nil
		}
		if err := validateTaskSnapshot(id, next); err != nil {
			return false, err
		}
		if current == nil || taskSnapshotSupersedes(current, next) {
			current = next
			if update != nil {
				if err := update(ctx, current); err != nil {
					return false, err
				}
			}
		}
		// Also when next shows nothing new: a request of the current
		// snapshot that another FollowTask call took, and gave back
		// unanswered, is free again, and nobody else will ask for it.
		if current.Status == TaskStatusInputRequired {
			if err := cs.answerTaskInput(ctx, id, current.InputRequests, follower.inputKeys); err != nil {
				return false, err
			}
		}
		return current.Status.Terminal(), nil
	}

	next, err := cs.GetTask(ctx, &GetTaskParams{TaskID: id})
	if err != nil {
		return nil, err
	}
	terminal, err := deliver(next)
	if err != nil {
		return nil, err
	}
	if terminal {
		return current, nil
	}

	newTimer := cs.newTaskTimer
	if newTimer == nil {
		newTimer = func(delay time.Duration) taskTimer {
			return realTaskTimer{timer: time.NewTimer(delay)}
		}
	}
	var timer taskTimer
	var poll <-chan time.Time
	stopTimer := func() {
		if timer != nil {
			timer.stop()
			timer = nil
			poll = nil
		}
	}
	defer stopTimer()
	startTimer := func() {
		stopTimer()
		delay := defaultTaskPollInterval
		if current.PollIntervalMs != nil && *current.PollIntervalMs > 0 {
			if candidate := time.Duration(*current.PollIntervalMs) * time.Millisecond; candidate > 0 {
				delay = candidate
			}
		}
		timer = newTimer(delay)
		poll = timer.channel()
	}
	startTimer()
	subscribed := follower.subscribed
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-subscribed:
			subscribed = nil
			next, err := cs.GetTask(ctx, &GetTaskParams{TaskID: id})
			if err != nil {
				return nil, err
			}
			terminal, err := deliver(next)
			if err != nil {
				return nil, err
			}
			if terminal {
				return current, nil
			}
		case <-follower.ready:
			next, err := follower.take()
			if err != nil {
				return nil, err
			}
			terminal, err := deliver(next)
			if err != nil {
				return nil, err
			}
			if terminal {
				return current, nil
			}
		case <-poll:
			timer = nil
			poll = nil
			next, err := cs.GetTask(ctx, &GetTaskParams{TaskID: id})
			if err != nil {
				return nil, err
			}
			terminal, err := deliver(next)
			if err != nil {
				return nil, err
			}
			if terminal {
				return current, nil
			}
			startTimer()
		}
	}
}

// serverAdvertisesTasks reports whether the server named the tasks extension
// in its capabilities.
func (cs *ClientSession) serverAdvertisesTasks() bool {
	res := cs.state.InitializeResult
	if res == nil || res.Capabilities == nil {
		return false
	}
	_, ok := res.Capabilities.Extensions[ExtensionTasks]
	return ok
}

// answerTaskInput answers the input requests of a task that no FollowTask
// call of the session has taken yet: it has the client's handlers fulfil
// them, as the multi round-trip middleware does for a tool call, and sends
// the answers with tasks/update. A request stays in the task's snapshots
// until the server has seen its answer, so the keys are remembered in keys.
// If the requests are not answered, their keys are free again and the error
// is returned: a FollowTask call that is still following the task takes them
// at its next snapshot, new or not.
func (cs *ClientSession) answerTaskInput(ctx context.Context, id string, requests InputRequestMap, keys *taskInputKeys) error {
	pending := keys.take(requests)
	if len(pending) == 0 {
		return nil
	}
	responses, err := fulfillInputRequests(ctx, cs, pending)
	if err != nil {
		keys.giveBack(pending)
		return fmt.Errorf("following task %q: %w", id, err)
	}
	if err := cs.UpdateTask(ctx, &UpdateTaskParams{TaskID: id, InputResponses: responses}); err != nil {
		keys.giveBack(pending)
		return err
	}
	return nil
}

// validateTaskSnapshot checks a snapshot FollowTask received for the task id:
// it must be about that task and have one of the five statuses.
func validateTaskSnapshot(id string, task *DetailedTask) error {
	if task.TaskID != id {
		return fmt.Errorf("invalid task snapshot: task ID %q, want %q", task.TaskID, id)
	}
	switch task.Status {
	case TaskStatusWorking, TaskStatusInputRequired, TaskStatusCompleted, TaskStatusCancelled, TaskStatusFailed:
		return nil
	default:
		return fmt.Errorf("invalid task snapshot: unknown status %q", task.Status)
	}
}

// taskSnapshotSupersedes reports whether next shows a newer state of the task
// than current. Snapshots reach FollowTask from notifications and from polls,
// in any order and more than once; this one rule decides, for both, which to
// keep:
//
//   - nothing supersedes a terminal snapshot;
//   - a terminal snapshot supersedes any other;
//   - otherwise an older lastUpdatedAt never supersedes, a newer one does,
//     and at the same lastUpdatedAt next supersedes only if it shows
//     something else (status, statusMessage, inputRequests, result or
//     error; _meta and resultType differ between a notification and a poll
//     of the same state, and are ignored).
func taskSnapshotSupersedes(current, next *DetailedTask) bool {
	if current.Status.Terminal() {
		return false
	}
	if next.Status.Terminal() {
		return true
	}
	if order := compareTaskTimestamps(next.LastUpdatedAt, current.LastUpdatedAt); order != 0 {
		return order > 0
	}
	return next.Status != current.Status ||
		next.StatusMessage != current.StatusMessage ||
		!reflect.DeepEqual(next.InputRequests, current.InputRequests) ||
		!bytes.Equal(next.Result, current.Result) ||
		!reflect.DeepEqual(next.Error, current.Error)
}

// compareTaskTimestamps orders two lastUpdatedAt values: as times when both
// are RFC 3339 timestamps, else as strings.
func compareTaskTimestamps(a, b string) int {
	at, aerr := time.Parse(time.RFC3339Nano, a)
	bt, berr := time.Parse(time.RFC3339Nano, b)
	if aerr == nil && berr == nil {
		return at.Compare(bt)
	}
	return strings.Compare(a, b)
}

// callUsesTasks reports whether a tools/call uses the extension: the call
// set UseTasks, or the program declares the extension itself, in the client's
// capabilities or in the client capabilities of the call's _meta, on a
// session whose protocol version has the extension. Declaring is something
// the program does; nothing a server sends makes a call use the extension.
func (cs *ClientSession) callUsesTasks(params *CallToolParams) bool {
	if params.UseTasks {
		return true
	}
	if clientDeclaresTasks(cs.client) {
		return cs.usesNewProtocol()
	}
	return params.Meta != nil && metaUsesTasks(params.Meta) && cs.usesNewProtocol()
}

// clientDeclaresTasks reports whether the program declared the extension for
// the client, in ClientOptions.Capabilities.
func clientDeclaresTasks(c *Client) bool {
	if c == nil || c.opts.Capabilities == nil {
		return false
	}
	_, ok := c.opts.Capabilities.Extensions[ExtensionTasks]
	return ok
}

// useTasks makes one tools/call use the extension. It returns the context
// and the params CallTool sends. The context's [taskSend] decodes the answer
// of that request, and of its multi round-trip retries, with
// [taskCallToolResult].
//
// A call that set UseTasks is sent with a copy of params that declares the
// extension. A call that uses the extension because the program declared it
// is sent with the params as they are: the request is the one the program
// would send without this function.
func (cs *ClientSession) useTasks(ctx context.Context, params *CallToolParams) (context.Context, *CallToolParams, error) {
	if !cs.usesNewProtocol() {
		return nil, nil, tasksProtocolError()
	}
	if params.UseTasks {
		copy := *params
		copy.Meta = withTasksCapability(cs, params.Meta)
		params = &copy
	}
	ctx = withTaskSend(ctx, taskSend{
		session: cs,
		method:  methodCallTool,
		// Not identity: the multi round-trip middleware retries with a copy
		// of the params, which keeps UseTasks and _meta. A call made with
		// this context that does not use the extension is not owned.
		owns: func(p Params) bool {
			call, ok := p.(*CallToolParams)
			return ok && call != nil && cs.callUsesTasks(call)
		},
		send: sendTaskCallTool,
	})
	return ctx, params, nil
}

// taskCallToolResult decodes a tools/call result as CallToolResult does and
// keeps the bytes, so that an opted-in call can read the task handle from
// them. CallToolResult's own codec knows nothing about tasks.
type taskCallToolResult struct {
	CallToolResult
	raw json.RawMessage
}

func (r *taskCallToolResult) UnmarshalJSON(data []byte) error {
	r.raw = bytes.Clone(data)
	return r.CallToolResult.UnmarshalJSON(data)
}

// sendTaskCallTool is the terminal of an opted-in tools/call. The handle is
// attached before the result goes back up the middleware chain: nothing
// writes to the result after the chain returns it.
func sendTaskCallTool(ctx context.Context, req Request) (Result, error) {
	var result taskCallToolResult
	if err := call(ctx, req.GetSession().getConn(), methodCallTool, req.GetParams(), &result); err != nil {
		return nil, err
	}
	if result.resultType == resultTypeTask {
		task, err := decodeTaskHandle(result.raw)
		switch {
		case err == nil:
			result.CallToolResult.task = task
		case req.GetParams().(*CallToolParams).UseTasks:
			return nil, fmt.Errorf("calling %q: invalid task result: %w", methodCallTool, err)
		default:
			// The program declared the extension without UseTasks, as a
			// program written before UseTasks existed does: it gets the
			// result it always got.
		}
	}
	return &result.CallToolResult, nil
}

// decodeTaskHandle strictly decodes the task of a CreateTaskResult, with the
// decoder the result itself was decoded with.
func decodeTaskHandle(data []byte) (*Task, error) {
	var task Task
	if err := internaljson.Unmarshal(data, &task); err != nil {
		return nil, err
	}
	if task.TaskID == "" {
		return nil, errors.New("missing taskId")
	}
	switch task.Status {
	case TaskStatusWorking, TaskStatusInputRequired, TaskStatusCompleted, TaskStatusCancelled, TaskStatusFailed:
	default:
		return nil, fmt.Errorf("unknown status %q", task.Status)
	}
	return &task, nil
}
