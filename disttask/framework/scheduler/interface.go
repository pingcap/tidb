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
)

// TaskTable defines the interface to access task table.
type TaskTable interface {
	GetGlobalTasksInStates(states ...interface{}) (task []*proto.Task, err error)
	GetGlobalTaskByID(taskID int64) (task *proto.Task, err error)

	GetSubtaskInStates(instanceID string, taskID int64, step int64, states ...interface{}) (*proto.Subtask, error)
	StartSubtask(id int64) error
	UpdateSubtaskStateAndError(id int64, state string, err error) error
	FinishSubtask(id int64, meta []byte) error
	HasSubtasksInStates(instanceID string, taskID int64, step int64, states ...interface{}) (bool, error)
	UpdateErrorToSubtask(tidbID string, err error) error
	IsSchedulerCanceled(taskID int64, execID string) (bool, error)
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
	GetSubtaskExecutor(ctx context.Context, task *proto.Task) (SubtaskExecutor, error)
	GetMiniTaskExecutor(minimalTask proto.MinimalTask, tp string, step int64) (MiniTaskExecutor, error)
}

// SubtaskExecutor defines the executor of a subtask.
type SubtaskExecutor interface {
	// Init is used to initialize the environment for the subtask executor.
	Init(context.Context) error
	// SplitSubtask is used to split the subtask into multiple minimal tasks.
	SplitSubtask(ctx context.Context, subtask []byte) ([]proto.MinimalTask, error)
	// Cleanup is used to clean up the environment for the subtask executor.
	Cleanup(context.Context) error
	// OnFinished is used to handle the subtask when it is finished.
	// return the result of the subtask.
	// MUST return subtask meta back on success.
	OnFinished(ctx context.Context, subtask []byte) ([]byte, error)
	// Rollback is used to roll back all subtasks.
	Rollback(context.Context) error
}

// MiniTaskExecutor defines the interface of a subtask executor.
// User should implement this interface to define their own subtask executor.
// TODO: Rename to minimal task executor.
type MiniTaskExecutor interface {
	Run(ctx context.Context) error
}

// EmptyScheduler is an empty scheduler.
// it can be used for the task that does not need to split into subtasks.
type EmptyScheduler struct {
}

var _ SubtaskExecutor = &EmptyScheduler{}

// InitSubtaskExecEnv implements the SubtaskExecutor interface.
func (*EmptyScheduler) Init(context.Context) error {
	return nil
}

// SplitSubtask implements the SubtaskExecutor interface.
func (*EmptyScheduler) SplitSubtask(context.Context, []byte) ([]proto.MinimalTask, error) {
	return nil, nil
}

// CleanupSubtaskExecEnv implements the SubtaskExecutor interface.
func (*EmptyScheduler) Cleanup(context.Context) error {
	return nil
}

// OnSubtaskFinished implements the SubtaskExecutor interface.
func (*EmptyScheduler) OnFinished(_ context.Context, metaBytes []byte) ([]byte, error) {
	return metaBytes, nil
}

// Rollback implements the SubtaskExecutor interface.
func (*EmptyScheduler) Rollback(context.Context) error {
	return nil
}

// EmptyExecutor is an empty minimal task executor.
// it can be used for the task that does not need to split into minimal tasks.
type EmptyExecutor struct {
}

var _ MiniTaskExecutor = &EmptyExecutor{}

// Run implements the MiniTaskExecutor interface.
func (*EmptyExecutor) Run(context.Context) error {
	return nil
}
