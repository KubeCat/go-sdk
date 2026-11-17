// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
)

// RequireInput suspends the task until all requested client inputs arrive.
func (c *TaskContext) RequireInput(ctx context.Context, requests InputRequestMap) (InputResponseMap, error) {
	if c.server == nil {
		return nil, errors.New("synchronous tool call has no task")
	}
	if len(requests) == 0 {
		return nil, errors.New("task input requires at least one request")
	}
	if err := c.runtime.beginInput(requests); err != nil {
		return nil, err
	}
	if err := c.server.updateTask(ctx, c.scope, c.taskID, func(task *DetailedTask) error {
		task.Status = TaskStatusInputRequired
		task.InputRequests = maps.Clone(requests)
		return nil
	}); err != nil {
		c.runtime.clearInput()
		return nil, err
	}
	for {
		responses, complete, changed := c.runtime.inputResponses()
		if complete {
			// tasks/update moved the task back to working with the last
			// answer; this only does so if the store has not seen it yet.
			if err := c.server.updateTask(ctx, c.scope, c.taskID, resolveTaskInput(maps.Keys(requests))); err != nil {
				return nil, err
			}
			return responses, nil
		}
		select {
		case <-ctx.Done():
			c.runtime.clearInput()
			// The requests are withdrawn: the task no longer waits for
			// them. A task that became terminal meanwhile is left alone.
			err := c.server.updateTask(context.WithoutCancel(ctx), c.scope, c.taskID, resolveTaskInput(maps.Keys(requests)))
			if err != nil && !errors.Is(err, errTaskTerminal) {
				c.server.server.opts.Logger.Error("withdrawing task input requests", "task_id", c.taskID, "error", err)
			}
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (r *taskRuntime) beginInput(requests InputRequestMap) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) != 0 {
		return errors.New("task already has outstanding input requests")
	}
	for key := range requests {
		if _, ok := r.usedInputKeys[key]; ok {
			return fmt.Errorf("task input request key %q was already used", key)
		}
	}
	for key := range requests {
		r.usedInputKeys[key] = struct{}{}
	}
	r.pending = maps.Clone(requests)
	r.responses = make(InputResponseMap)
	return nil
}

func (r *taskRuntime) answer(answers InputResponseMap) InputResponseMap {
	r.mu.Lock()
	defer r.mu.Unlock()
	answered := make(InputResponseMap)
	for key, response := range answers {
		if _, ok := r.pending[key]; ok {
			if _, answeredAlready := r.responses[key]; !answeredAlready {
				r.responses[key] = response
				answered[key] = response
			}
		}
	}
	if len(answered) > 0 {
		close(r.changed)
		r.changed = make(chan struct{})
	}
	return answered
}

func (r *taskRuntime) inputResponses() (InputResponseMap, bool, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) > 0 && len(r.responses) == len(r.pending) {
		responses := maps.Clone(r.responses)
		r.pending = nil
		r.responses = nil
		return responses, true, nil
	}
	return nil, false, r.changed
}

func (r *taskRuntime) clearInput() {
	r.mu.Lock()
	r.pending = nil
	r.responses = nil
	r.mu.Unlock()
}

// errTaskUnchanged is returned by a task update that has nothing to change:
// the task keeps its lastUpdatedAt and no notification is sent.
var errTaskUnchanged = errors.New("task unchanged")

// resolveTaskInput returns the task update that removes the input requests
// with the given keys, because they were answered or withdrawn. A task whose
// last request is removed goes back to working in the same update, so that no
// snapshot ever shows input_required without inputRequests.
//
// The update is idempotent: tasks/update applies it for the keys it accepted,
// and RequireInput applies it for all of its keys when it returns.
func resolveTaskInput(keys iter.Seq[string]) func(*DetailedTask) error {
	return func(task *DetailedTask) error {
		if task.Status != TaskStatusInputRequired {
			return errTaskUnchanged
		}
		changed := false
		for key := range keys {
			if _, ok := task.InputRequests[key]; ok {
				delete(task.InputRequests, key)
				changed = true
			}
		}
		if len(task.InputRequests) == 0 {
			task.Status = TaskStatusWorking
			task.InputRequests = nil
			changed = true
		}
		if !changed {
			return errTaskUnchanged
		}
		return nil
	}
}
