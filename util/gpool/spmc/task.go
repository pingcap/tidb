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
	"sync/atomic"
)

const (
	// PendingTask is a task waiting to start
	PendingTask int32 = iota
	// RunningTask is a task running
	RunningTask
	// StopTask is a stop task
	StopTask
)

type taskBox[T any, U any, C any, CT any, TF Context[CT]] struct {
	constArgs   C
	contextFunc TF
	wg          *sync.WaitGroup
	task        chan T
	resultCh    chan U
	taskID      uint64
	status      atomic.Int32 // task manager is able to make this task stop, wait or running
}

func (t *taskBox[T, U, C, CT, TF]) GetStatus() int32 {
	return t.status.Load()
}

func (t *taskBox[T, U, C, CT, TF]) SetStatus(s int32) {
	t.status.Store(s)
}

// Context is a interface that can be used to create a context.
type Context[T any] interface {
	GetContext() T
}

// NilContext is to create a nil as context
type NilContext struct{}

// GetContext is to get a nil as context
func (NilContext) GetContext() any {
	return nil
}

// TaskController is a controller that can control or watch the pool.
type TaskController[T any, U any, C any, CT any, TF Context[CT]] struct {
	pool   *Pool[T, U, C, CT, TF]
	close  chan struct{}
	wg     *sync.WaitGroup
	taskID uint64
}

// NewTaskController create a controller to deal with task's statue.
func NewTaskController[T any, U any, C any, CT any, TF Context[CT]](p *Pool[T, U, C, CT, TF], taskID uint64, closeCh chan struct{}, wg *sync.WaitGroup) TaskController[T, U, C, CT, TF] {
	return TaskController[T, U, C, CT, TF]{
		pool:   p,
		taskID: taskID,
		close:  closeCh,
		wg:     wg,
	}
}

// Wait is to wait the task to stop.
func (t *TaskController[T, U, C, CT, TF]) Wait() {
	<-t.close
	t.wg.Wait()
	t.pool.taskManager.DeleteTask(t.taskID)
}

// IsProduceClose is to judge whether the producer is completed.
func (t *TaskController[T, U, C, CT, TF]) IsProduceClose() bool {
	select {
	case <-t.close:
		return true
	default:
	}
	return false
}
