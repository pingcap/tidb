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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/util/channel"
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
	// PendingTask is a task waiting to start.
	PendingTask int32 = iota
	// RunningTask is a task running.
	RunningTask
	// StopTask is a stop task.
	StopTask
)

// TaskBox is a box which contains all info about pool task.
type TaskBox[T any, U any, C any, CT any, TF Context[CT]] struct {
	constArgs   C
	contextFunc TF
	wg          *sync.WaitGroup
	task        chan Task[T]
	resultCh    chan U
	taskID      uint64
	status      atomic.Int32 // task manager is able to make this task stop, wait or running
}

// GetStatus is to get the status of task.
func (t *TaskBox[T, U, C, CT, TF]) GetStatus() int32 {
	return t.status.Load()
}

// SetStatus is to set the status of task.
func (t *TaskBox[T, U, C, CT, TF]) SetStatus(s int32) {
	t.status.Store(s)
}

// NewTaskBox is to create a task box for pool.
func NewTaskBox[T any, U any, C any, CT any, TF Context[CT]](constArgs C, contextFunc TF, wg *sync.WaitGroup, taskCh chan Task[T], resultCh chan U, taskID uint64) TaskBox[T, U, C, CT, TF] {
	// We still need to do some work after a TaskBox finishes.
	// So we need to add 1 to waitgroup. After we finish the work, we need to call TaskBox.Finish()
	wg.Add(1)
	return TaskBox[T, U, C, CT, TF]{
		constArgs:   constArgs,
		contextFunc: contextFunc,
		wg:          wg,
		task:        taskCh,
		resultCh:    resultCh,
		taskID:      taskID,
	}
}

// TaskID is to get the task id.
func (t *TaskBox[T, U, C, CT, TF]) TaskID() uint64 {
	return t.taskID
}

// ConstArgs is to get the const args.
func (t *TaskBox[T, U, C, CT, TF]) ConstArgs() C {
	return t.constArgs
}

// GetTaskCh is to get the task channel.
func (t *TaskBox[T, U, C, CT, TF]) GetTaskCh() chan Task[T] {
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

// Done is to set the pooltask status to complete.
func (t *TaskBox[T, U, C, CT, TF]) Done() {
	t.wg.Done()
}

// Finish is to set the TaskBox finish status.
func (t *TaskBox[T, U, C, CT, TF]) Finish() {
	t.wg.Done()
}

// Clone is to copy the box
func (t *TaskBox[T, U, C, CT, TF]) Clone() *TaskBox[T, U, C, CT, TF] {
	newBox := NewTaskBox[T, U, C, CT, TF](t.constArgs, t.contextFunc, t.wg, t.task, t.resultCh, t.taskID)
	return &newBox
}

// GPool is a goroutine pool.
type GPool[T any, U any, C any, CT any, TF Context[CT]] interface {
	Tune(size int)
	DeleteTask(id uint64)
	StopTask(id uint64)
}

// TaskController is a controller that can control or watch the pool.
type TaskController[T any, U any, C any, CT any, TF Context[CT]] struct {
	pool          GPool[T, U, C, CT, TF]
	productExitCh chan struct{}
	wg            *sync.WaitGroup
	taskID        uint64
	resultCh      chan U
	inputCh       chan Task[T]
}

// NewTaskController create a controller to deal with pooltask's status.
func NewTaskController[T any, U any, C any, CT any, TF Context[CT]](p GPool[T, U, C, CT, TF], taskID uint64, productExitCh chan struct{}, wg *sync.WaitGroup, inputCh chan Task[T], resultCh chan U) TaskController[T, U, C, CT, TF] {
	return TaskController[T, U, C, CT, TF]{
		pool:          p,
		taskID:        taskID,
		productExitCh: productExitCh,
		wg:            wg,
		resultCh:      resultCh,
		inputCh:       inputCh,
	}
}

// Wait is to wait the pool task to stop.
func (t *TaskController[T, U, C, CT, TF]) Wait() {
	t.wg.Wait()
	close(t.resultCh)
	t.pool.DeleteTask(t.taskID)
}

// Stop is to send stop command to the task. But you still need to wait the task to stop.
func (t *TaskController[T, U, C, CT, TF]) Stop() {
	close(t.productExitCh)
	// Clear all the task in the task queue and mark all task complete.
	// so that ```t.Wait``` is able to close resultCh
	for range t.inputCh {
		t.wg.Done()
	}
	t.pool.StopTask(t.TaskID())
	// Clear the resultCh to avoid blocking the consumer put result into the channel and cannot exit.
	channel.Clear(t.resultCh)
}

// TaskID is to get the task id.
func (t *TaskController[T, U, C, CT, TF]) TaskID() uint64 {
	return t.taskID
}

// Task is a task that can be executed.
type Task[T any] struct {
	Task T
}
