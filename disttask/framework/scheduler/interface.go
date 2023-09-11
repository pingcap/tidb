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

package scheduler

import (
	"context"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler/execute"
)

// TaskTable defines the interface to access task table.
type TaskTable interface {
	GetGlobalTasksInStates(states ...interface{}) (task []*proto.Task, err error)
	GetGlobalTaskByID(taskID int64) (task *proto.Task, err error)

	GetSubtaskInStates(instanceID string, taskID int64, step int64, states ...interface{}) (*proto.Subtask, error)
	StartManager(tidbID string, role string) error
	StartSubtask(subtaskID int64) error
	UpdateSubtaskStateAndError(subtaskID int64, state string, err error) error
	FinishSubtask(subtaskID int64, meta []byte) error

	HasSubtasksInStates(instanceID string, taskID int64, step int64, states ...interface{}) (bool, error)
	UpdateErrorToSubtask(instanceID string, taskID int64, err error) error
	IsSchedulerCanceled(taskID int64, instanceID string) (bool, error)
}

// Pool defines the interface of a pool.
type Pool interface {
	Run(func()) error
	RunWithConcurrency(chan func(), uint32) error
	ReleaseAndWait()
}

// Scheduler is the subtask scheduler for a task.
// each task type should implement this interface.
type Scheduler interface {
	Run(context.Context, *proto.Task) error
	Rollback(context.Context, *proto.Task) error
}

// Extension extends the scheduler.
// each task type should implement this interface.
type Extension interface {
	// GetSubtaskExecutor returns the subtask executor for the subtask.
	// Note: summary is the summary manager of all subtask of the same type now.
	GetSubtaskExecutor(ctx context.Context, task *proto.Task, summary *execute.Summary) (execute.SubtaskExecutor, error)
	GetMiniTaskExecutor(minimalTask proto.MinimalTask, tp string, step int64) (execute.MiniTaskExecutor, error)
}

// EmptySubtaskExecutor is an empty scheduler.
// it can be used for the task that does not need to split into subtasks.
type EmptySubtaskExecutor struct {
}

var _ execute.SubtaskExecutor = &EmptySubtaskExecutor{}

// Init implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) Init(context.Context) error {
	return nil
}

// SplitSubtask implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) SplitSubtask(context.Context, *proto.Subtask) ([]proto.MinimalTask, error) {
	return nil, nil
}

// Cleanup implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) Cleanup(context.Context) error {
	return nil
}

// OnFinished implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) OnFinished(_ context.Context, metaBytes []byte) ([]byte, error) {
	return metaBytes, nil
}

// Rollback implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) Rollback(context.Context) error {
	return nil
}

// EmptyMiniTaskExecutor is an empty minimal task executor.
// it can be used for the task that does not need to split into minimal tasks.
type EmptyMiniTaskExecutor struct {
}

var _ execute.MiniTaskExecutor = &EmptyMiniTaskExecutor{}

// Run implements the MiniTaskExecutor interface.
func (*EmptyMiniTaskExecutor) Run(context.Context) error {
	return nil
}
