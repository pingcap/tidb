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

package pooltask

import (
	"container/list"
	"sync"
	"time"

	"go.uber.org/atomic"
)

const shard int = 8

func getShardID(id uint64) uint64 {
	return id % uint64(shard)
}

type tContainer[T any, U any, C any, CT any, TF Context[CT]] struct {
	task *TaskBox[T, U, C, CT, TF]
}

type meta struct {
	stats    *list.List
	createTs time.Time
	origin   int32
	running  int32
}

func newStats(concurrency int32) *meta {
	s := &meta{
		createTs: time.Now(),
		stats:    list.New(),
		origin:   concurrency,
	}
	return s
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

// NewTaskManager create a new pooltask manager.
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

// RegisterTask register a task to the manager.
func (t *TaskManager[T, U, C, CT, TF]) RegisterTask(taskID uint64, concurrency int32) {
	id := getShardID(taskID)
	t.task[id].rw.Lock()
	t.task[id].stats[taskID] = newStats(concurrency)
	t.task[id].rw.Unlock()
}

// DeleteTask delete a task from the manager.
func (t *TaskManager[T, U, C, CT, TF]) DeleteTask(taskID uint64) {
	shardID := getShardID(taskID)
	t.task[shardID].rw.Lock()
	delete(t.task[shardID].stats, taskID)
	t.task[shardID].rw.Unlock()
}

// AddSubTask AddTask add a task to the manager.
func (t *TaskManager[T, U, C, CT, TF]) AddSubTask(taskID uint64, task *TaskBox[T, U, C, CT, TF]) {
	shardID := getShardID(taskID)
	tc := tContainer[T, U, C, CT, TF]{
		task: task,
	}
	t.running.Inc()
	t.task[shardID].rw.Lock()
	t.task[shardID].stats[taskID].stats.PushBack(tc)
	t.task[shardID].stats[taskID].running++ // running job in this task
	t.task[shardID].rw.Unlock()
}

// ExitSubTask is to exit a task, and it will decrease the count of running pooltask.
func (t *TaskManager[T, U, C, CT, TF]) ExitSubTask(taskID uint64) {
	shardID := getShardID(taskID)
	t.running.Dec() // total running tasks
	t.task[shardID].rw.Lock()
	t.task[shardID].stats[taskID].running-- // running job in this task
	t.task[shardID].rw.Unlock()
}

// Running return the count of running job in this task.
func (t *TaskManager[T, U, C, CT, TF]) Running(taskID uint64) int32 {
	shardID := getShardID(taskID)
	t.task[shardID].rw.Lock()
	defer t.task[shardID].rw.Unlock()
	return t.task[shardID].stats[taskID].running
}

// StopTask is to stop a task by TaskID.
func (t *TaskManager[T, U, C, CT, TF]) StopTask(taskID uint64) {
	shardID := getShardID(taskID)
	t.task[shardID].rw.Lock()
	defer t.task[shardID].rw.Unlock()
	l := t.task[shardID].stats[taskID].stats
	for e := l.Front(); e != nil; e = e.Next() {
		e.Value.(tContainer[T, U, C, CT, TF]).task.SetStatus(StopTask)
	}
}
