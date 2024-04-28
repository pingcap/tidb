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
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
)

// TaskTable defines the interface to access the task table.
type TaskTable interface {
	// GetTaskExecInfoByExecID gets all task exec infos by given execID, if there's
	// no executable subtask on the execID for some task, it's not returned.
	GetTaskExecInfoByExecID(ctx context.Context, execID string) ([]*storage.TaskExecInfo, error)
	GetTasksInStates(ctx context.Context, states ...any) (task []*proto.Task, err error)
	GetTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error)
	GetTaskBaseByID(ctx context.Context, taskID int64) (task *proto.TaskBase, err error)
	// GetSubtasksByExecIDAndStepAndStates gets all subtasks by given states and execID.
	GetSubtasksByExecIDAndStepAndStates(ctx context.Context, execID string, taskID int64, step proto.Step, states ...proto.SubtaskState) ([]*proto.Subtask, error)
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
	RunningSubtasksBack2Pending(ctx context.Context, subtasks []*proto.SubtaskBase) error
}

// Pool defines the interface of a pool.
type Pool interface {
	Run(func()) error
	RunWithConcurrency(chan func(), uint32) error
	ReleaseAndWait()
}

// TaskExecutor is the executor for a task.
// Each task type should implement this interface.
// context tree of task execution:
//
//	Manager.ctx
//	└── TaskExecutor.ctx: Cancel cancels this one
//	   └── RunStep.ctx: CancelRunningSubtask cancels this one
type TaskExecutor interface {
	// Init initializes the TaskExecutor, the returned error is fatal, it will fail
	// the task directly, so be careful what to put into it.
	// The context passing in is Manager.ctx, don't use it to init long-running routines,
	// as it will NOT be cancelled when the task is finished.
	// NOTE: do NOT depend on task meta to do initialization, as we plan to pass
	// task-base to the TaskExecutor in the future, if you need to do some initialization
	// based on task meta, do it in GetStepExecutor, as execute.StepExecutor is
	// where subtasks are actually executed.
	Init(context.Context) error
	// Run runs the task with given resource, it will try to run each step one by
	// one, if it cannot find any subtask to run for a while(10s now), it will exit,
	// so manager can free and reuse the resource.
	// we assume that all steps will have same resource usage now, will change it
	// when we support different resource usage for different steps.
	Run(resource *proto.StepResource)
	// GetTaskBase returns the task, returned value is for read only, don't change it.
	GetTaskBase() *proto.TaskBase
	// CancelRunningSubtask cancels the running subtask and change its state to `cancelled`,
	// the task executor will keep running, so we can have a context to update the
	// subtask state or keep handling revert logic.
	CancelRunningSubtask()
	// Cancel cancels the task executor, the state of running subtask is not changed.
	// it's separated with Close as Close mostly mean will wait all resource released
	// before return, but we only want its context cancelled and check whether it's
	// closed later.
	Cancel()
	// Close closes the TaskExecutor.
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
	// GetStepExecutor returns the subtask executor for the subtask.
	// Note:
	// 1. summary is the summary manager of all subtask of the same type now.
	// 2. should not retry the error from it.
	GetStepExecutor(task *proto.Task) (execute.StepExecutor, error)
	// IsRetryableError returns whether the error is transient.
	// When error is transient, the framework won't mark subtasks as failed,
	// then the TaskExecutor can load the subtask again and redo it.
	IsRetryableError(err error) bool
}

// EmptyStepExecutor is an empty Executor.
// it can be used for the task that does not need to split into subtasks.
type EmptyStepExecutor struct {
	execute.StepExecFrameworkInfo
}

var _ execute.StepExecutor = &EmptyStepExecutor{}

// Init implements the StepExecutor interface.
func (*EmptyStepExecutor) Init(context.Context) error {
	return nil
}

// RunSubtask implements the StepExecutor interface.
func (*EmptyStepExecutor) RunSubtask(context.Context, *proto.Subtask) error {
	return nil
}

// RealtimeSummary implements the StepExecutor interface.
func (*EmptyStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return nil
}

// Cleanup implements the StepExecutor interface.
func (*EmptyStepExecutor) Cleanup(context.Context) error {
	return nil
}

// OnFinished implements the StepExecutor interface.
func (*EmptyStepExecutor) OnFinished(_ context.Context, _ *proto.Subtask) error {
	return nil
}
