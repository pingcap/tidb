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
	"sync"
	"sync/atomic"
)

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

const (
	// PendingTask is a task waiting to start
	PendingTask int32 = iota
	// RunningTask is a task running
	RunningTask
	// StopTask is a stop task
	StopTask
)

// TaskBox is a box which contains all info about task.
type TaskBox[T any, U any, C any, CT any, TF Context[CT]] struct {
	constArgs   C
	contextFunc TF
	wg          *sync.WaitGroup
	task        chan T
	resultCh    chan U
	taskID      uint64
	status      atomic.Int32 // task manager is able to make this task stop, wait or running
}

// NewTaskBox is to create a task box.
func NewTaskBox[T any, U any, C any, CT any, TF Context[CT]](constArgs C, contextFunc TF, wg *sync.WaitGroup, taskCh chan T, resultCh chan U, taskID uint64) TaskBox[T, U, C, CT, TF] {
	return TaskBox[T, U, C, CT, TF]{
		constArgs:   constArgs,
		contextFunc: contextFunc,
		wg:          wg,
		task:        taskCh,
		resultCh:    resultCh,
		taskID:      taskID,
	}
}

// ConstArgs is to get the const args.
func (t *TaskBox[T, U, C, CT, TF]) ConstArgs() C {
	return t.constArgs
}

// GeTaskCh is to get the task channel.
func (t *TaskBox[T, U, C, CT, TF]) GeTaskCh() chan T {
	return t.task
}

// GetResultCh is to get result channel
func (t *TaskBox[T, U, C, CT, TF]) GetResultCh() chan U {
	return t.resultCh
}

// GetContextFunc is to get context func.
func (t *TaskBox[T, U, C, CT, TF]) GetContextFunc() TF {
	return t.contextFunc
}

// GetStatus is to get the status of task.
func (t *TaskBox[T, U, C, CT, TF]) GetStatus() int32 {
	return t.status.Load()
}

// SetStatus is to set the status of task.
func (t *TaskBox[T, U, C, CT, TF]) SetStatus(s int32) {
	t.status.Store(s)
}

// Done is to set the task status to complete.
func (t *TaskBox[T, U, C, CT, TF]) Done() {
	t.wg.Done()
}

// GPool is a goroutine pool.
type GPool[T any, U any, C any, CT any, TF Context[CT]] interface {
	Release()

	Tune(size int)

	DeleteTask(id uint64)
}

// TaskController is a controller that can control or watch the pool.
type TaskController[T any, U any, C any, CT any, TF Context[CT]] struct {
	pool   GPool[T, U, C, CT, TF]
	close  chan struct{}
	wg     *sync.WaitGroup
	taskID uint64
}

// NewTaskController create a controller to deal with task's statue.
func NewTaskController[T any, U any, C any, CT any, TF Context[CT]](p GPool[T, U, C, CT, TF], taskID uint64, closeCh chan struct{}, wg *sync.WaitGroup) TaskController[T, U, C, CT, TF] {
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
	t.pool.DeleteTask(t.taskID)
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
