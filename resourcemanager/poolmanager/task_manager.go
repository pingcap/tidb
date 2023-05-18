// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package poolmanager

import (
	"sync"
	"time"

	"go.uber.org/atomic"
)

const shard int = 8

func getShardID(id uint64) uint64 {
	return id % uint64(shard)
}

// Meta is a container that can control or watch the task in the pool.
type Meta struct {
	createTS           time.Time
	exitCh             chan struct{}
	taskCh             chan func()
	taskID             uint64
	running            atomic.Int32
	initialConcurrency int32
}

// NewMeta create a new meta.
func NewMeta(taskID uint64, exitCh chan struct{}, taskCh chan func(), concurrency int32) *Meta {
	s := &Meta{
		createTS:           time.Now(),
		initialConcurrency: concurrency,
		taskID:             taskID,
		exitCh:             exitCh,
		taskCh:             taskCh,
	}
	return s
}

func (m *Meta) getOriginConcurrency() int32 {
	return m.initialConcurrency
}

// TaskID is to get the task id.
func (m *Meta) TaskID() uint64 {
	return m.taskID
}

// IncTask is to add running task count.
func (m *Meta) IncTask() {
	m.running.Add(1)
}

// DecTask is to minus running task count.
func (m *Meta) DecTask() {
	m.running.Add(-1)
}

// GetTaskCh is to get the task channel.
func (m *Meta) GetTaskCh() chan func() {
	return m.taskCh
}

// GetExitCh is to get the exit channel.
func (m *Meta) GetExitCh() chan struct{} {
	return m.exitCh
}

// TaskStatusContainer is a container that can control or watch the pool.
type TaskStatusContainer struct {
	stats map[uint64]*Meta
	rw    sync.RWMutex
}

// TaskManager is a manager that can control or watch the pool.
type TaskManager struct {
	task        []TaskStatusContainer
	concurrency int32
}

// NewTaskManager create a new pool task manager.
func NewTaskManager(c int32) TaskManager {
	task := make([]TaskStatusContainer, shard)
	for i := 0; i < shard; i++ {
		task[i] = TaskStatusContainer{
			stats: make(map[uint64]*Meta),
		}
	}
	return TaskManager{
		task:        task,
		concurrency: c,
	}
}

// RegisterTask register a task to the manager.
func (t *TaskManager) RegisterTask(task *Meta) {
	id := getShardID(task.taskID)
	t.task[id].rw.Lock()
	t.task[id].stats[task.taskID] = task
	t.task[id].rw.Unlock()
}

// DeleteTask delete a task from the manager.
func (t *TaskManager) DeleteTask(taskID uint64) {
	shardID := getShardID(taskID)
	t.task[shardID].rw.Lock()
	delete(t.task[shardID].stats, taskID)
	t.task[shardID].rw.Unlock()
}

// hasTask check if the task is in the manager.
func (t *TaskManager) hasTask(taskID uint64) bool {
	shardID := getShardID(taskID)
	t.task[shardID].rw.Lock()
	defer t.task[shardID].rw.Unlock()
	_, ok := t.task[shardID].stats[taskID]
	return ok
}

// GetOriginConcurrency return the concurrency of the pool at the init.
func (t *TaskManager) GetOriginConcurrency() int32 {
	return t.concurrency
}
