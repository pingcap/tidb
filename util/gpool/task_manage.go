// Copyright 2022 PingCAP, Inc.
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

package gpool

import (
	"container/list"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sys/cpu"
)

const shard int = 8

func getShardID(id uint64) uint64 {
	return id % uint64(shard)
}

type tContainer[T any, U any, C any, CT any, TF Context[CT]] struct {
	task *TaskBox[T, U, C, CT, TF]
	_    cpu.CacheLinePad
}

type meta struct {
	stats       *list.List
	createTs    time.Time
	origin      int32
	concurrency atomic.Int32
	running     atomic.Int32
}

func newStats(concurrency int32) *meta {
	s := &meta{
		createTs: time.Now(),
		stats:    list.New(),
		origin:   concurrency,
	}
	return s
}

func (m *meta) setConcurrency(concurrency int32) {
	m.concurrency.Store(concurrency)
}

func (m *meta) getConcurrency() int32 {
	return m.concurrency.Load()
}

func (m *meta) getOriginConcurrency() int32 {
	return m.origin
}

// TaskStatusContainer is a container that can control or watch the pool.
type TaskStatusContainer[T any, U any, C any, CT any, TF Context[CT]] struct {
	stats map[uint64]*meta
	rw    sync.RWMutex
}

// TaskManager is a manager that can control or watch the pool.
type TaskManager[T any, U any, C any, CT any, TF Context[CT]] struct {
	task        []TaskStatusContainer[T, U, C, CT, TF]
	running     atomic.Int32
	concurrency int32
}

// NewTaskManager create a new task manager.
func NewTaskManager[T any, U any, C any, CT any, TF Context[CT]](c int32) TaskManager[T, U, C, CT, TF] {
	task := make([]TaskStatusContainer[T, U, C, CT, TF], shard)
	for i := 0; i < shard; i++ {
		task[i] = TaskStatusContainer[T, U, C, CT, TF]{
			stats: make(map[uint64]*meta),
		}
	}
	return TaskManager[T, U, C, CT, TF]{
		task:        task,
		concurrency: c,
	}
}

// CreatTask create a new task.
func (t *TaskManager[T, U, C, CT, TF]) CreatTask(task uint64, concurrency int32) {
	id := getShardID(task)
	t.task[id].rw.Lock()
	t.task[id].stats[task] = newStats(concurrency)
	t.task[id].rw.Unlock()
}

// DeleteTask delete a task.
func (t *TaskManager[T, U, C, CT, TF]) DeleteTask(id uint64) {
	shardID := getShardID(id)
	t.task[shardID].rw.Lock()
	delete(t.task[shardID].stats, id)
	t.task[shardID].rw.Unlock()
}

// AddSubTask AddTask add a task to the manager.
func (t *TaskManager[T, U, C, CT, TF]) AddSubTask(id uint64, task *TaskBox[T, U, C, CT, TF]) {
	shardID := getShardID(id)
	tc := tContainer[T, U, C, CT, TF]{
		task: task,
	}
	t.running.Inc()
	t.task[shardID].rw.Lock()
	t.task[shardID].stats[id].stats.PushBack(tc)
	t.task[shardID].stats[id].running.Inc()
	t.task[shardID].rw.Unlock()
}

// ExitSubTask is to exit a task, and it will decrease the count of running task.
func (t *TaskManager[T, U, C, CT, TF]) ExitSubTask(id uint64) {
	shardID := getShardID(id)
	t.task[shardID].rw.Lock()
	t.task[shardID].stats[id].running.Dec()
	t.running.Dec()
	t.task[shardID].rw.Unlock()
}
