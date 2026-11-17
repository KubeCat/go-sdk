// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
)

// getTask asks for a task and checks that input_required and inputRequests
// come together.
func (f *deliveryFixture) getTask(peer *rawTaskPeer, id string) *DetailedTask {
	f.t.Helper()
	peer.request(methodGetTask, fmt.Sprintf(`{"taskId":%q,%s}`, id, rawTaskMetaDeclaring))
	answer := f.read(peer, "the answer to tasks/get")
	var task DetailedTask
	if err := json.Unmarshal(answer.Result, &task); err != nil || answer.Error != nil {
		f.t.Fatalf("tasks/get answered %+v (%v)", answer, err)
	}
	if (task.Status == TaskStatusInputRequired) != (len(task.InputRequests) > 0) {
		f.t.Fatalf("tasks/get answered %s: input_required and inputRequests must come together", answer.Result)
	}
	return &task
}

// answerTask sends tasks/update with the given inputResponses object.
func (f *deliveryFixture) answerTask(peer *rawTaskPeer, id, responses string) {
	f.t.Helper()
	peer.request(methodUpdateTask, fmt.Sprintf(`{"taskId":%q,"inputResponses":%s,%s}`, id, responses, rawTaskMetaDeclaring))
	if answer := f.read(peer, "the answer to tasks/update"); answer.Error != nil || len(answer.Result) == 0 {
		f.t.Fatalf("tasks/update answered %+v", answer)
	}
}

func inputKeys(task *DetailedTask) []string {
	return slices.Sorted(maps.Keys(task.InputRequests))
}

func elicitInput(keys ...string) InputRequestMap {
	requests := make(InputRequestMap)
	for _, key := range keys {
		requests[key] = &ElicitParams{Message: key}
	}
	return requests
}

// Every snapshot of a task, from tasks/get and from notifications, has
// status input_required exactly when it has inputRequests: the answer that
// leaves no request outstanding moves the task back to working in the same
// update.
func TestTaskInputRequiredHasInputRequests(t *testing.T) {
	f := newDeliveryFixture(t)
	requests, notifications := f.peer(), f.peer()
	id := f.startTask(requests, "a")
	listen := f.listen(notifications, taskIDsJSON(id))
	f.readAck(notifications)
	f.readTask(notifications, "the task's state")

	var responses InputResponseMap
	f.begin("a", func(ctx context.Context, taskContext *TaskContext) error {
		var err error
		responses, err = taskContext.RequireInput(context.WithValue(ctx, taskInputMark{}, true), elicitInput("first", "second"))
		return err
	})
	if task := f.readTask(notifications, "the task asking for input"); task.Status != TaskStatusInputRequired || !slices.Equal(inputKeys(task), []string{"first", "second"}) {
		t.Fatalf("notification = %s %v, want input_required with both requests", task.Status, inputKeys(task))
	}
	// From here on the handler's own updates are parked: what the store
	// shows is what tasks/update made of it.
	f.updateGate.arm()
	if task := f.getTask(requests, id); task.Status != TaskStatusInputRequired || !slices.Equal(inputKeys(task), []string{"first", "second"}) {
		t.Fatalf("tasks/get = %s %v, want input_required with both requests", task.Status, inputKeys(task))
	}

	f.answerTask(requests, id, `{"first":{"action":"accept","content":{"value":"A"}}}`)
	partial := f.getTask(requests, id)
	if partial.Status != TaskStatusInputRequired || !slices.Equal(inputKeys(partial), []string{"second"}) {
		t.Fatalf("after a partial answer tasks/get = %s %v, want input_required with the second request", partial.Status, inputKeys(partial))
	}
	if task := f.readTask(notifications, "the partially answered task"); task.Status != TaskStatusInputRequired || !slices.Equal(inputKeys(task), []string{"second"}) {
		t.Fatalf("after a partial answer the notification = %s %v", task.Status, inputKeys(task))
	}
	// Answers to a key that is not outstanding change nothing, not even
	// lastUpdatedAt.
	f.answerTask(requests, id, `{"first":{"action":"decline"},"never-asked":{"action":"accept"}}`)
	if task := f.getTask(requests, id); task.LastUpdatedAt != partial.LastUpdatedAt || !slices.Equal(inputKeys(task), []string{"second"}) {
		t.Fatalf("answers to keys that are not outstanding changed the task: %s %v, was %s", task.LastUpdatedAt, inputKeys(task), partial.LastUpdatedAt)
	}

	f.answerTask(requests, id, `{"second":{"action":"accept","content":{"value":"B"}}}`)
	answered := f.getTask(requests, id)
	if answered.Status != TaskStatusWorking {
		t.Fatalf("after the last answer tasks/get = %s, want working", answered.Status)
	}
	if task := f.readTask(notifications, "the answered task"); task.Status != TaskStatusWorking {
		t.Fatalf("after the last answer the notification = %s, want working", task.Status)
	}

	// The handler's RequireInput now finds nothing left to change.
	await(f, f.updateGate.held, "the handler's update after the answers")
	f.updateGate.open()
	if err := f.stepDone("a"); err != nil {
		t.Fatalf("RequireInput = %v", err)
	}
	first, _ := responses["first"].(*ElicitResult)
	second, _ := responses["second"].(*ElicitResult)
	if first == nil || second == nil || first.Content["value"] != "A" || second.Content["value"] != "B" {
		t.Errorf("RequireInput returned %+v, want the first answer to each request", responses)
	}
	if task := f.getTask(requests, id); task.LastUpdatedAt != answered.LastUpdatedAt || task.Status != TaskStatusWorking {
		t.Errorf("RequireInput's return changed the task: %s %s, was %s", task.Status, task.LastUpdatedAt, answered.LastUpdatedAt)
	}
	f.lanes["a"].finish()
	if task := f.readTask(notifications, "the completed task"); task.Status != TaskStatusCompleted {
		t.Fatalf("last notification = %s, want completed", task.Status)
	}
	f.endListen(notifications, listen)
}

// A RequireInput that gives up (its context ends) withdraws its requests: the
// task goes back to working. A task that became terminal meanwhile stays as
// it is.
func TestTaskRequireInputWithdrawn(t *testing.T) {
	t.Run("context cancelled", func(t *testing.T) {
		f := newDeliveryFixture(t)
		requests, notifications := f.peer(), f.peer()
		id := f.startTask(requests, "a")
		listen := f.listen(notifications, taskIDsJSON(id))
		f.readAck(notifications)
		f.readTask(notifications, "the task's state")

		giveUp, gaveUp := taskEvents[context.CancelFunc](t, "cancel functions")
		f.begin("a", func(ctx context.Context, taskContext *TaskContext) error {
			ctx, cancel := context.WithCancel(ctx)
			gaveUp(cancel)
			if _, err := taskContext.RequireInput(ctx, elicitInput("only")); !errors.Is(err, context.Canceled) {
				return fmt.Errorf("RequireInput = %v, want the context's error", err)
			}
			return nil
		})
		if task := f.readTask(notifications, "the task asking for input"); task.Status != TaskStatusInputRequired {
			t.Fatalf("notification = %s, want input_required", task.Status)
		}
		await(f, giveUp, "the handler's cancel function")()
		if err := f.stepDone("a"); err != nil {
			t.Fatal(err)
		}
		withdrawn := f.getTask(requests, id)
		if withdrawn.Status != TaskStatusWorking {
			t.Fatalf("after RequireInput gave up tasks/get = %s, want working", withdrawn.Status)
		}
		if task := f.readTask(notifications, "the task without its requests"); task.Status != TaskStatusWorking {
			t.Fatalf("after RequireInput gave up the notification = %s, want working", task.Status)
		}
		// A late answer is acknowledged and changes nothing; the key is
		// spent.
		f.answerTask(requests, id, `{"only":{"action":"accept"}}`)
		if task := f.getTask(requests, id); task.LastUpdatedAt != withdrawn.LastUpdatedAt {
			t.Errorf("a late answer changed the task: %s, was %s", task.LastUpdatedAt, withdrawn.LastUpdatedAt)
		}
		f.step("a", func(ctx context.Context, taskContext *TaskContext) error {
			if _, err := taskContext.RequireInput(ctx, elicitInput("only")); err == nil || !strings.Contains(err.Error(), "already used") {
				return fmt.Errorf("RequireInput with a spent key = %v, want a refusal", err)
			}
			return nil
		})
		f.lanes["a"].finish()
		if task := f.readTask(notifications, "the completed task"); task.Status != TaskStatusCompleted {
			t.Fatalf("last notification = %s, want completed", task.Status)
		}
		f.endListen(notifications, listen)
	})

	t.Run("task cancelled", func(t *testing.T) {
		f := newDeliveryFixture(t)
		requests, notifications := f.peer(), f.peer()
		id := f.startTask(requests, "a")
		listen := f.listen(notifications, taskIDsJSON(id))
		f.readAck(notifications)
		f.readTask(notifications, "the task's state")
		f.begin("a", func(ctx context.Context, taskContext *TaskContext) error {
			if _, err := taskContext.RequireInput(ctx, elicitInput("only")); !errors.Is(err, context.Canceled) {
				return fmt.Errorf("RequireInput = %v, want the context's error", err)
			}
			return nil
		})
		if task := f.readTask(notifications, "the task asking for input"); task.Status != TaskStatusInputRequired {
			t.Fatalf("notification = %s, want input_required", task.Status)
		}
		requests.request(methodCancelTask, fmt.Sprintf(`{"taskId":%q,%s}`, id, rawTaskMetaDeclaring))
		if answer := f.read(requests, "the answer to tasks/cancel"); answer.Error != nil {
			t.Fatalf("tasks/cancel answered %+v", answer)
		}
		if err := f.stepDone("a"); err != nil {
			t.Fatal(err)
		}
		if task := f.getTask(requests, id); task.Status != TaskStatusCancelled {
			t.Fatalf("tasks/get = %s, want cancelled", task.Status)
		}
		if task := f.readTask(notifications, "the cancelled task"); task.Status != TaskStatusCancelled {
			t.Fatalf("notification = %s, want cancelled", task.Status)
		}
		f.endListen(notifications, listen)
	})
}

func TestResolveTaskInput(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     TaskStatus
		requests   []string
		keys       []string
		wantErr    error
		wantStatus TaskStatus
		wantLeft   []string
	}{
		{"some answered", TaskStatusInputRequired, []string{"a", "b"}, []string{"a"}, nil, TaskStatusInputRequired, []string{"b"}},
		{"all answered", TaskStatusInputRequired, []string{"a", "b"}, []string{"a", "b"}, nil, TaskStatusWorking, nil},
		{"none outstanding", TaskStatusInputRequired, []string{"a"}, []string{"z"}, errTaskUnchanged, TaskStatusInputRequired, []string{"a"}},
		{"no key", TaskStatusInputRequired, []string{"a"}, nil, errTaskUnchanged, TaskStatusInputRequired, []string{"a"}},
		{"input_required without requests", TaskStatusInputRequired, nil, nil, nil, TaskStatusWorking, nil},
		{"working", TaskStatusWorking, nil, []string{"a"}, errTaskUnchanged, TaskStatusWorking, nil},
		{"completed", TaskStatusCompleted, nil, []string{"a"}, errTaskUnchanged, TaskStatusCompleted, nil},
	} {
		task := &DetailedTask{Task: Task{Status: test.status}}
		if test.requests != nil {
			task.InputRequests = elicitInput(test.requests...)
		}
		err := resolveTaskInput(slices.Values(test.keys))(task)
		if err != test.wantErr || task.Status != test.wantStatus || !slices.Equal(inputKeys(task), test.wantLeft) || (len(task.InputRequests) == 0 && task.InputRequests != nil) {
			t.Errorf("%s: err %v, status %s, requests %v; want %v, %s, %v", test.name, err, task.Status, task.InputRequests, test.wantErr, test.wantStatus, test.wantLeft)
		}
	}
}

// An input request key is used once in a task's life: a second request with
// a key the task has used is refused, also after the first was answered or
// withdrawn.
func TestTaskInputKeyUsedOnce(t *testing.T) {
	runtime := &taskRuntime{usedInputKeys: make(map[string]struct{}), changed: make(chan struct{})}
	if err := runtime.beginInput(elicitInput("only")); err != nil {
		t.Fatal(err)
	}
	runtime.clearInput()
	if err := runtime.beginInput(elicitInput("other", "only")); err == nil || !strings.Contains(err.Error(), "already used") {
		t.Fatalf("second request with a used key: %v; want it refused", err)
	}
	// A refused request uses up none of its keys.
	if err := runtime.beginInput(elicitInput("other")); err != nil {
		t.Fatalf("request with the unused key of a refused request: %v", err)
	}
}
