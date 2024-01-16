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

package taskexecutor

import (
	"context"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
)

// TaskTable defines the interface to access the task table.
type TaskTable interface {
	GetTasksInStates(ctx context.Context, states ...interface{}) (task []*proto.Task, err error)
	GetTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error)
	GetSubtasksByStepAndStates(ctx context.Context, execID string, taskID int64, step proto.Step, states ...proto.SubtaskState) ([]*proto.Subtask, error)
	GetFirstSubtaskInStates(ctx context.Context, instanceID string, taskID int64, step proto.Step, states ...proto.SubtaskState) (*proto.Subtask, error)
	// InitMeta insert the manager information into dist_framework_meta.
	// Call it when starting task executor or in set variable operation.
	InitMeta(ctx context.Context, execID string, role string) error
	// RecoverMeta recover the manager information into dist_framework_meta.
	// Call it periodically to recover deleted meta.
	RecoverMeta(ctx context.Context, execID string, role string) error
	// StartSubtask try to update the subtask's state to running if the subtask is owned by execID.
	// If the update success, it means the execID's related task executor own the subtask.
	StartSubtask(ctx context.Context, subtaskID int64, execID string) error
	// UpdateSubtaskStateAndError update the subtask's state and error.
	UpdateSubtaskStateAndError(ctx context.Context, execID string, subtaskID int64, state proto.SubtaskState, err error) error
	// FailSubtask update the task's subtask state to failed and set the err.
	FailSubtask(ctx context.Context, execID string, taskID int64, err error) error
	// CancelSubtask update the task's subtasks' state to canceled.
	CancelSubtask(ctx context.Context, exe string, taskID int64) error
	// FinishSubtask updates the subtask meta and mark state to succeed.
	FinishSubtask(ctx context.Context, execID string, subtaskID int64, meta []byte) error
	// PauseSubtasks update subtasks state to paused.
	PauseSubtasks(ctx context.Context, execID string, taskID int64) error

	HasSubtasksInStates(ctx context.Context, execID string, taskID int64, step proto.Step, states ...proto.SubtaskState) (bool, error)
	// RunningSubtasksBack2Pending update the state of subtask which belongs to this
	// node from running to pending.
	// see subtask state machine for more detail.
	RunningSubtasksBack2Pending(ctx context.Context, subtasks []*proto.Subtask) error
}

// Pool defines the interface of a pool.
type Pool interface {
	Run(func()) error
	RunWithConcurrency(chan func(), uint32) error
	ReleaseAndWait()
}

// TaskExecutor is the subtask executor for a task.
// Each task type should implement this interface.
type TaskExecutor interface {
	Init(context.Context) error
	Run(context.Context, *proto.Task) error
	Rollback(context.Context, *proto.Task) error
	Close()
	IsRetryableError(err error) bool
}

// Extension extends the TaskExecutor.
// each task type should implement this interface.
type Extension interface {
	// IsIdempotent returns whether the subtask is idempotent.
	// when tidb restart, the subtask might be left in the running state.
	// if it's idempotent, the Executor can rerun the subtask, else
	// the Executor will mark the subtask as failed.
	IsIdempotent(subtask *proto.Subtask) bool
	// GetSubtaskExecutor returns the subtask executor for the subtask.
	// Note:
	// 1. summary is the summary manager of all subtask of the same type now.
	// 2. should not retry the error from it.
	GetSubtaskExecutor(ctx context.Context, task *proto.Task, summary *execute.Summary) (execute.SubtaskExecutor, error)
	// IsRetryableError returns whether the error is transient.
	// When error is transient, the framework won't mark subtasks as failed,
	// then the TaskExecutor can load the subtask again and redo it.
	IsRetryableError(err error) bool
}

// EmptySubtaskExecutor is an empty Executor.
// it can be used for the task that does not need to split into subtasks.
type EmptySubtaskExecutor struct {
}

var _ execute.SubtaskExecutor = &EmptySubtaskExecutor{}

// Init implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) Init(context.Context) error {
	return nil
}

// RunSubtask implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) RunSubtask(context.Context, *proto.Subtask) error {
	return nil
}

// Cleanup implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) Cleanup(context.Context) error {
	return nil
}

// OnFinished implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) OnFinished(_ context.Context, _ *proto.Subtask) error {
	return nil
}

// Rollback implements the SubtaskExecutor interface.
func (*EmptySubtaskExecutor) Rollback(context.Context) error {
	return nil
}
