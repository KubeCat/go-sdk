// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryTaskStoreScopeAndSnapshots(t *testing.T) {
	store := NewMemoryTaskStore()
	task := &DetailedTask{Task: Task{TaskID: "task-1", Status: TaskStatusWorking}}
	if err := store.Create(t.Context(), "scope-a", task); err != nil {
		t.Fatal(err)
	}
	task.Status = TaskStatusFailed
	stored, err := store.Get(t.Context(), "scope-a", "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != TaskStatusWorking {
		t.Fatal("Create retained the caller's task pointer")
	}
	stored.Status = TaskStatusCompleted
	storedAgain, err := store.Get(t.Context(), "scope-a", "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if storedAgain.Status != TaskStatusWorking {
		t.Fatal("Get returned the store's task pointer")
	}
	if _, err := store.Get(t.Context(), "scope-b", "task-1"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("cross-scope Get error = %v, want ErrTaskNotFound", err)
	}
	updateErr := errors.New("reject update")
	if err := store.Update(t.Context(), "scope-a", "task-1", func(task *DetailedTask) error {
		task.Status = TaskStatusCompleted
		return updateErr
	}); !errors.Is(err, updateErr) {
		t.Fatalf("Update error = %v", err)
	}
	storedAgain, err = store.Get(t.Context(), "scope-a", "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if storedAgain.Status != TaskStatusWorking {
		t.Fatal("failed Update changed the stored task")
	}
	if err := store.Delete(t.Context(), "scope-a", "task-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(t.Context(), "scope-a", "task-1"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("Get after Delete error = %v", err)
	}
}

func TestMemoryTaskStoreExpiresTasks(t *testing.T) {
	store := NewMemoryTaskStore()
	ttl := int64(1)
	if err := store.Create(t.Context(), "scope", &DetailedTask{Task: Task{
		TaskID:        "expired",
		Status:        TaskStatusCompleted,
		CreatedAt:     time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
		LastUpdatedAt: time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
		TTLMs:         &ttl,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(t.Context(), "scope", "expired"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("Get expired task error = %v, want ErrTaskNotFound", err)
	}
	store.mu.Lock()
	remaining := len(store.tasks)
	store.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expired task remains in memory; store size = %d", remaining)
	}
}

func TestMemoryTaskStoreConcurrentUpdates(t *testing.T) {
	store := NewMemoryTaskStore()
	ttl := int64(0)
	if err := store.Create(t.Context(), "", &DetailedTask{Task: Task{
		TaskID: "task-1",
		Status: TaskStatusWorking,
		TTLMs:  &ttl,
	}}); err != nil {
		t.Fatal(err)
	}
	const updates = 32
	var wg sync.WaitGroup
	errs := make(chan error, updates)
	for range updates {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.Update(t.Context(), "", "task-1", func(task *DetailedTask) error {
				*task.TTLMs++
				return nil
			}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	task, err := store.Get(t.Context(), "", "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := *task.TTLMs; got != updates {
		t.Fatalf("TTL after concurrent updates = %d, want %d", got, updates)
	}
}

// Update changes a task only in the scope it was created in.
func TestMemoryTaskStoreUpdateScope(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryTaskStore()
	if err := store.Create(ctx, "one", &DetailedTask{Task: Task{TaskID: "task-1", Status: TaskStatusWorking}}); err != nil {
		t.Fatal(err)
	}
	called := false
	err := store.Update(ctx, "two", "task-1", func(task *DetailedTask) error {
		called = true
		task.StatusMessage = "from another scope"
		return nil
	})
	if !errors.Is(err, ErrTaskNotFound) || called {
		t.Fatalf("Update from another scope: error %v, update called %t; want ErrTaskNotFound and no call", err, called)
	}
	task, err := store.Get(ctx, "one", "task-1")
	if err != nil || task.StatusMessage != "" {
		t.Fatalf("task after an Update from another scope = %+v, %v; want it unchanged", task, err)
	}
}
