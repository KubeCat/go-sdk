// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// taskKey identifies a task of a TaskServer.
type taskKey struct {
	scope string
	id    string
}

// A taskSubscription is the task part of one subscriptions/listen request:
// the tasks it follows and the ones that changed since it last sent their
// state.
//
// Everything a subscription sends, it sends from the goroutine of its listen
// request, with that request's context: the acknowledgement first, then
// (deliver) one notifications/tasks per changed task, each read from the
// store right before it is sent. So the acknowledgement precedes every
// notification, a snapshot is never followed by an older one, and a slow
// subscriber delays only its own listen. An update only marks the task dirty
// and wakes the subscription (TaskServer.notifyTask); it never reads the
// store, sends on a connection or waits.
type taskSubscription struct {
	server    *TaskServer
	session   *ServerSession
	requestID jsonrpc.ID
	scope     string
	// ids are the tasks the server agreed to send notifications for, in the
	// order the request named them. Not changed after subscribe.
	ids []string
	// wake holds a token while dirty may be non-empty.
	wake chan struct{}

	// Guarded by server.mu.
	following map[string]bool // the ids not yet seen in a terminal status
	dirty     map[string]bool // the followed ids whose state is to be sent
}

// subscribe registers the tasks a subscriptions/listen request names. It
// returns the missing-capability error for a request that does not declare
// the extension, and nil when the server agrees to none of the ids: an id
// that is empty, repeated, unknown or outside the caller's scope is left out
// of the acknowledgement, and the rest of the listen is honoured.
//
// The caller must call close when the listen ends.
func (s *TaskServer) subscribe(ctx context.Context, req *SubscriptionsListenRequest, requestID jsonrpc.ID) (*taskSubscription, error) {
	if !requestUsesTasks(req) {
		return nil, missingTasksCapabilityError()
	}
	sub := &taskSubscription{
		server:    s,
		session:   req.Session,
		requestID: requestID,
		scope:     s.scope(ctx, req.Session),
		wake:      make(chan struct{}, 1),
		following: make(map[string]bool),
		dirty:     make(map[string]bool),
	}
	for _, id := range req.Params.Notifications.TaskIDs {
		if id == "" || sub.following[id] {
			continue
		}
		if _, err := s.opts.Store.Get(ctx, sub.scope, id); err != nil {
			if errors.Is(err, ErrTaskNotFound) {
				continue
			}
			return nil, taskRequestError("subscribe to", id, err)
		}
		sub.ids = append(sub.ids, id)
		sub.following[id] = true
		// Every task starts dirty: its current state follows the
		// acknowledgement, so a subscriber need not poll for a change that
		// happened before it subscribed.
		sub.dirty[id] = true
	}
	if len(sub.ids) == 0 {
		return nil, nil
	}
	sub.wake <- struct{}{}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range sub.ids {
		key := taskKey{sub.scope, id}
		if s.subscriptions[key] == nil {
			s.subscriptions[key] = make(map[*taskSubscription]struct{})
		}
		s.subscriptions[key][sub] = struct{}{}
	}
	return sub, nil
}

// close removes the subscription from the server.
func (sub *taskSubscription) close() {
	s := sub.server
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range sub.ids {
		sub.unfollowLocked(id)
	}
}

// unfollowLocked stops following one task. The caller holds server.mu.
func (sub *taskSubscription) unfollowLocked(id string) {
	delete(sub.following, id)
	delete(sub.dirty, id)
	s := sub.server
	key := taskKey{sub.scope, id}
	delete(s.subscriptions[key], sub)
	if len(s.subscriptions[key]) == 0 {
		delete(s.subscriptions, key)
	}
}

// notifyTask records that a task changed. Each subscription sends the new
// state itself, from its listen request (see taskSubscription).
func (s *TaskServer) notifyTask(scope, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sub := range s.subscriptions[taskKey{scope, id}] {
		sub.dirty[id] = true
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}

// takeDirty returns the changed tasks, in the order of the request, and
// marks them clean.
func (sub *taskSubscription) takeDirty() []string {
	sub.server.mu.Lock()
	defer sub.server.mu.Unlock()
	var ids []string
	for _, id := range sub.ids {
		if sub.dirty[id] {
			ids = append(ids, id)
		}
	}
	clear(sub.dirty)
	return ids
}

// deliver sends the state of each changed task as notifications/tasks until
// ctx, the context of the listen request, is done. A task is no longer
// followed once its terminal state has been sent, or once the store no
// longer has it.
func (sub *taskSubscription) deliver(ctx context.Context) {
	s := sub.server
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.wake:
		}
		for _, id := range sub.takeDirty() {
			task, err := s.opts.Store.Get(ctx, sub.scope, id)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				if errors.Is(err, ErrTaskNotFound) {
					s.mu.Lock()
					sub.unfollowLocked(id)
					s.mu.Unlock()
				} else {
					s.server.opts.Logger.Error("loading task notification", "task_id", id, "error", err)
				}
				continue
			}
			if err := sub.notify(ctx, task); err != nil {
				s.server.opts.Logger.Warn(fmt.Sprintf("calling %s: %v", notificationTaskStatus, err))
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if task.Status.Terminal() {
				s.mu.Lock()
				sub.unfollowLocked(id)
				s.mu.Unlock()
			}
		}
	}
}

// notify sends one notifications/tasks on the subscription's session.
func (sub *taskSubscription) notify(ctx context.Context, task *DetailedTask) error {
	params := *task
	params.ResultType = ""
	injectMetaSubscriptionID(&params, sub.requestID)
	// The server's method table does not know notifications/tasks: a taskSend
	// sends this one notification, on the session the request names.
	ctx = withTaskSend(ctx, taskSend{
		session: sub.session,
		method:  notificationTaskStatus,
		owns:    ownsParams(&params),
		send: func(ctx context.Context, req Request) (Result, error) {
			return nil, taskNotify(ctx, req, notificationTaskStatus, req.GetParams())
		},
	})
	return handleNotify(ctx, notificationTaskStatus, newServerRequest(sub.session, &params))
}

// taskAckWire is the wire form of a subscriptions acknowledgement that names
// tasks.
type taskAckWire struct {
	Meta          `json:"_meta,omitempty"`
	Notifications taskSubscriptionsWire `json:"notifications"`
}

// context returns the context the listen request sends its acknowledgement
// with. The acknowledgement is built and sent by the shared listen handler as
// usual; the [taskSend] here only writes it to the connection with the task
// ids the server agreed to, which the shared type's JSON leaves out. It
// applies to the acknowledgement of this listen alone: this session, and the
// subscription id of this request.
func (sub *taskSubscription) context(ctx context.Context) context.Context {
	subscriptionID := sub.requestID.Raw()
	return withTaskSend(ctx, taskSend{
		session: sub.session,
		method:  notificationSubscriptionsAck,
		owns: func(p Params) bool {
			ack, ok := p.(*SubscriptionsAcknowledgedParams)
			return ok && ack != nil && reflect.DeepEqual(ack.Meta[MetaKeySubscriptionID], subscriptionID)
		},
		send: func(ctx context.Context, req Request) (Result, error) {
			ack := req.GetParams().(*SubscriptionsAcknowledgedParams)
			wire := &taskAckWire{Meta: ack.Meta, Notifications: newTaskSubscriptionsWire(ack.Notifications)}
			return nil, taskNotify(ctx, req, notificationSubscriptionsAck, wire)
		},
	})
}
