// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type memoryTaskKey struct {
	scope string
	id    string
}

// MemoryTaskStore is an in-memory TaskStore. It discards expired tasks during
// store operations.
type MemoryTaskStore struct {
	mu    sync.Mutex
	tasks map[memoryTaskKey]*DetailedTask
}

// NewMemoryTaskStore creates an empty in-memory task store.
func NewMemoryTaskStore() *MemoryTaskStore {
	return &MemoryTaskStore{tasks: make(map[memoryTaskKey]*DetailedTask)}
}

func cloneDetailedTask(task *DetailedTask) (*DetailedTask, error) {
	data, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	var clone DetailedTask
	if err := json.Unmarshal(data, &clone); err != nil {
		return nil, err
	}
	return &clone, nil
}

func (s *MemoryTaskStore) purgeExpired(now time.Time) {
	for key, task := range s.tasks {
		if task.TTLMs == nil {
			continue
		}
		created, err := time.Parse(time.RFC3339Nano, task.CreatedAt)
		if err != nil {
			continue
		}
		ttl := time.Duration(*task.TTLMs) * time.Millisecond
		if !now.Before(created.Add(ttl)) {
			delete(s.tasks, key)
		}
	}
}

// Create persists a new task.
func (s *MemoryTaskStore) Create(_ context.Context, scope string, task *DetailedTask) error {
	if task == nil || task.TaskID == "" {
		return fmt.Errorf("task ID is required")
	}
	clone, err := cloneDetailedTask(task)
	if err != nil {
		return fmt.Errorf("cloning task: %w", err)
	}
	key := memoryTaskKey{scope: scope, id: task.TaskID}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpired(time.Now())
	if _, ok := s.tasks[key]; ok {
		return fmt.Errorf("task %q already exists", task.TaskID)
	}
	s.tasks[key] = clone
	return nil
}

// Get returns a copy of a task snapshot.
func (s *MemoryTaskStore) Get(_ context.Context, scope, id string) (*DetailedTask, error) {
	s.mu.Lock()
	s.purgeExpired(time.Now())
	task, ok := s.tasks[memoryTaskKey{scope: scope, id: id}]
	s.mu.Unlock()
	if !ok {
		return nil, ErrTaskNotFound
	}
	clone, err := cloneDetailedTask(task)
	if err != nil {
		return nil, fmt.Errorf("cloning task: %w", err)
	}
	return clone, nil
}

// Update atomically applies update to a task and persists the result.
func (s *MemoryTaskStore) Update(_ context.Context, scope, id string, update func(*DetailedTask) error) error {
	key := memoryTaskKey{scope: scope, id: id}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpired(time.Now())
	task, ok := s.tasks[key]
	if !ok {
		return ErrTaskNotFound
	}
	clone, err := cloneDetailedTask(task)
	if err != nil {
		return fmt.Errorf("cloning task: %w", err)
	}
	if err := update(clone); err != nil {
		return err
	}
	clone, err = cloneDetailedTask(clone)
	if err != nil {
		return fmt.Errorf("cloning task: %w", err)
	}
	s.tasks[key] = clone
	return nil
}

// Delete removes a task.
func (s *MemoryTaskStore) Delete(_ context.Context, scope, id string) error {
	key := memoryTaskKey{scope: scope, id: id}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpired(time.Now())
	if _, ok := s.tasks[key]; !ok {
		return ErrTaskNotFound
	}
	delete(s.tasks, key)
	return nil
}
