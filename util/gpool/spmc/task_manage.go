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
	"sync"

	"golang.org/x/sys/cpu"
)

const SHARD int = 8

func getShardID(id uint64) uint64 {
	return id % uint64(SHARD)
}

type TContainer[T any, U any, C any, CT any, TF Context[CT]] struct {
	_    cpu.CacheLinePad
	task *taskBox[T, U, C, CT, TF]
	_    cpu.CacheLinePad
}

type TaskStatusContainer[T any, U any, C any, CT any, TF Context[CT]] struct {
	_      cpu.CacheLinePad
	rw     sync.RWMutex
	Status map[uint64][]TContainer[T, U, C, CT, TF]
	_      cpu.CacheLinePad
}

type TaskManager[T any, U any, C any, CT any, TF Context[CT]] struct {
	task         []TaskStatusContainer[T, U, C, CT, TF]
	conncurrency int
}

func NewTaskManager[T any, U any, C any, CT any, TF Context[CT]](con int) TaskManager[T, U, C, CT, TF] {
	task := make([]TaskStatusContainer[T, U, C, CT, TF], 0, SHARD)
	return TaskManager[T, U, C, CT, TF]{
		task:         task,
		conncurrency: con,
	}
}

func (t *TaskManager[T, U, C, CT, TF]) CreatTask(task uint64) {
	id := getShardID(task)
	t.task[id].rw.Lock()
	t.task[id].Status[task] = make([]TContainer[T, U, C, CT, TF], t.conncurrency)
	t.task[id].rw.Unlock()
}

func (t *TaskManager[T, U, C, CT, TF]) AddTask(id uint64, task *taskBox[T, U, C, CT, TF]) {
	shardID := getShardID(id)
	tc := TContainer[T, U, C, CT, TF]{
		task: task,
	}
	t.task[shardID].rw.Lock()
	t.task[shardID].Status[id] = append(t.task[shardID].Status[id], tc)
	t.task[shardID].rw.Unlock()
}

func (t *TaskManager[T, U, C, CT, TF]) GetTask(id uint64, taskID int) *taskBox[T, U, C, CT, TF] {
	shardID := getShardID(id)
	t.task[shardID].rw.RLock()
	task := t.task[shardID].Status[id][taskID].task
	t.task[shardID].rw.RUnlock()
	return task
}

func (t *TaskManager[T, U, C, CT, TF]) GetTaskStatus(id uint64) []TContainer[T, U, C, CT, TF] {
	shardID := getShardID(id)
	t.task[shardID].rw.RLock()
	task := t.task[shardID].Status[id]
	t.task[shardID].rw.RUnlock()
	return task
}

func (t *TaskManager[T, U, C, CT, TF]) DeleteTask(id uint64) {
	shardID := getShardID(id)
	t.task[shardID].rw.Lock()
	delete(t.task[shardID].Status, id)
	t.task[shardID].rw.Unlock()
}

func (t *TaskManager[T, U, C, CT, TF]) GetTaskStatusLen(id uint64) int {
	shardID := getShardID(id)
	t.task[shardID].rw.RLock()
	task := len(t.task[shardID].Status[id])
	t.task[shardID].rw.RUnlock()
	return task
}

func (t *TaskManager[T, U, C, CT, TF]) GetTaskStatusLenWithLock(id uint64) int {
	shardID := getShardID(id)
	t.task[shardID].rw.Lock()
	task := len(t.task[shardID].Status[id])
	t.task[shardID].rw.Unlock()
	return task
}
