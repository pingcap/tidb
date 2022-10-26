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

package spmc

import (
	"container/list"
	"sync"

	"golang.org/x/sys/cpu"
)

const shard int = 8

func getShardID(id uint64) uint64 {
	return id % uint64(shard)
}

type tContainer[T any, U any, C any, CT any, TF Context[CT]] struct {
	task *taskBox[T, U, C, CT, TF]
	_    cpu.CacheLinePad
}

// TaskStatusContainer is a container that can control or watch the pool.
type TaskStatusContainer[T any, U any, C any, CT any, TF Context[CT]] struct {
	Status map[uint64]*list.List
	rw     sync.RWMutex
	_      cpu.CacheLinePad
}

// TaskManager is a manager that can control or watch the pool.
type TaskManager[T any, U any, C any, CT any, TF Context[CT]] struct {
	task         []TaskStatusContainer[T, U, C, CT, TF]
	conncurrency int32
}

// NewTaskManager create a new task manager.
func NewTaskManager[T any, U any, C any, CT any, TF Context[CT]](con int32) TaskManager[T, U, C, CT, TF] {
	task := make([]TaskStatusContainer[T, U, C, CT, TF], shard)
	for i := 0; i < shard; i++ {
		task[i] = TaskStatusContainer[T, U, C, CT, TF]{
			Status: make(map[uint64]*list.List),
		}
	}
	return TaskManager[T, U, C, CT, TF]{
		task:         task,
		conncurrency: con,
	}
}

// CreatTask create a new task.
func (t *TaskManager[T, U, C, CT, TF]) CreatTask(task uint64) {
	id := getShardID(task)
	t.task[id].rw.Lock()
	t.task[id].Status[task] = list.New()
	t.task[id].rw.Unlock()
}

// AddTask add a task to the manager.
func (t *TaskManager[T, U, C, CT, TF]) AddTask(id uint64, task *taskBox[T, U, C, CT, TF]) {
	shardID := getShardID(id)
	tc := tContainer[T, U, C, CT, TF]{
		task: task,
	}
	t.task[shardID].rw.Lock()
	t.task[shardID].Status[id].PushBack(tc)
	t.task[shardID].rw.Unlock()
}

// DeleteTask delete a task.
func (t *TaskManager[T, U, C, CT, TF]) DeleteTask(id uint64) {
	shardID := getShardID(id)
	t.task[shardID].rw.Lock()
	delete(t.task[shardID].Status, id)
	t.task[shardID].rw.Unlock()
}
