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

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
)

<<<<<<< HEAD
// TaskTable defines the interface to access task table.
type TaskTable interface {
	GetGlobalTasksInStates(ctx context.Context, states ...interface{}) (task []*proto.Task, err error)
	GetGlobalTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error)
=======
// TaskManager defines the interface to access task table.
type TaskManager interface {
	// GetTopUnfinishedTasks returns unfinished tasks, limited by MaxConcurrentTask*2,
	// to make sure lower priority tasks can be scheduled if resource is enough.
	// The returned tasks are sorted by task order, see proto.Task, and only contains
	// some fields, see row2TaskBasic.
	GetTopUnfinishedTasks(ctx context.Context) ([]*proto.Task, error)
	GetTasksInStates(ctx context.Context, states ...interface{}) (task []*proto.Task, err error)
	GetTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error)
	UpdateTaskAndAddSubTasks(ctx context.Context, task *proto.Task, subtasks []*proto.Subtask, prevState proto.TaskState) (bool, error)
	GCSubtasks(ctx context.Context) error
	GetAllNodes(ctx context.Context) ([]proto.ManagedNode, error)
	DeleteDeadNodes(ctx context.Context, nodes []string) error
	// TransferTask2History transfer tasks and it's related subtasks to history tables.
	TransferTasks2History(ctx context.Context, tasks []*proto.Task) error
	// CancelTask updated task state to canceling.
	CancelTask(ctx context.Context, taskID int64) error
	// FailTask updates task state to Failed and updates task error.
	FailTask(ctx context.Context, taskID int64, currentState proto.TaskState, taskErr error) error
	// RevertedTask updates task state to reverted.
	RevertedTask(ctx context.Context, taskID int64) error
	// PauseTask updated task state to pausing.
	PauseTask(ctx context.Context, taskKey string) (bool, error)
	// PausedTask updated task state to paused.
	PausedTask(ctx context.Context, taskID int64) error
	// SucceedTask updates a task to success state.
	SucceedTask(ctx context.Context, taskID int64) error
	// SwitchTaskStep switches the task to the next step and add subtasks in one
	// transaction. It will change task state too if we're switch from InitStep to
	// next step.
	SwitchTaskStep(ctx context.Context, task *proto.Task, nextState proto.TaskState, nextStep proto.Step, subtasks []*proto.Subtask) error
	// SwitchTaskStepInBatch similar to SwitchTaskStep, but it will insert subtasks
	// in batch, and task step change will be in a separate transaction.
	// Note: subtasks of this step must be stable, i.e. count, order and content
	// should be the same on each try, else the subtasks inserted might be messed up.
	// And each subtask of this step must be different, to handle the network
	// partition or owner change.
	SwitchTaskStepInBatch(ctx context.Context, task *proto.Task, nextState proto.TaskState, nextStep proto.Step, subtasks []*proto.Subtask) error
	// GetUsedSlotsOnNodes returns the used slots on nodes that have subtask scheduled.
	// subtasks of each task on one node is only accounted once as we don't support
	// running them concurrently.
	// we only consider pending/running subtasks, subtasks related to revert are
	// not considered.
	GetUsedSlotsOnNodes(ctx context.Context) (map[string]int, error)
	// GetActiveSubtasks returns subtasks of the task that are in pending/running state.
	// the returned subtasks only contains some fields, see row2SubtaskBasic.
	GetActiveSubtasks(ctx context.Context, taskID int64) ([]*proto.Subtask, error)
	// GetSubtaskCntGroupByStates returns the count of subtasks of some step group by state.
	GetSubtaskCntGroupByStates(ctx context.Context, taskID int64, step proto.Step) (map[proto.SubtaskState]int64, error)
	ResumeSubtasks(ctx context.Context, taskID int64) error
	CollectSubTaskError(ctx context.Context, taskID int64) ([]error, error)
	UpdateSubtasksExecIDs(ctx context.Context, subtasks []*proto.Subtask) error
	// GetManagedNodes returns the nodes managed by dist framework and can be used
	// to execute tasks. If there are any nodes with background role, we use them,
	// else we use nodes without role.
	// returned nodes are sorted by node id(host:port).
	GetManagedNodes(ctx context.Context) ([]proto.ManagedNode, error)
	GetTaskExecutorIDsByTaskID(ctx context.Context, taskID int64) ([]string, error)
	GetSubtasksByStepAndState(ctx context.Context, taskID int64, step proto.Step, state proto.TaskState) ([]*proto.Subtask, error)
	GetSubtasksByExecIdsAndStepAndState(ctx context.Context, tidbIDs []string, taskID int64, step proto.Step, state proto.SubtaskState) ([]*proto.Subtask, error)
	GetTaskExecutorIDsByTaskIDAndStep(ctx context.Context, taskID int64, step proto.Step) ([]string, error)
>>>>>>> 720983a20c6 (disttask: merge transfer task/subtask (#50311))

	GetSubtasksInStates(ctx context.Context, tidbID string, taskID int64, step proto.Step, states ...interface{}) ([]*proto.Subtask, error)
	GetFirstSubtaskInStates(ctx context.Context, instanceID string, taskID int64, step proto.Step, states ...interface{}) (*proto.Subtask, error)
	StartManager(ctx context.Context, tidbID string, role string) error
	StartSubtask(ctx context.Context, subtaskID int64) error
	UpdateSubtaskStateAndError(ctx context.Context, tidbID string, subtaskID int64, state proto.TaskState, err error) error
	FinishSubtask(ctx context.Context, tidbID string, subtaskID int64, meta []byte) error

	HasSubtasksInStates(ctx context.Context, tidbID string, taskID int64, step proto.Step, states ...interface{}) (bool, error)
	UpdateErrorToSubtask(ctx context.Context, tidbID string, taskID int64, err error) error
	IsSchedulerCanceled(ctx context.Context, tidbID string, taskID int64) (bool, error)
	PauseSubtasks(ctx context.Context, tidbID string, taskID int64) error
}

// Pool defines the interface of a pool.
type Pool interface {
	Run(func()) error
	RunWithConcurrency(chan func(), uint32) error
	ReleaseAndWait()
}

// Scheduler is the subtask scheduler for a task.
// Each task type should implement this interface.
type Scheduler interface {
	Init(context.Context) error
	Run(context.Context, *proto.Task) error
	Rollback(context.Context, *proto.Task) error
	Pause(context.Context, *proto.Task) error
	Close()
}

// Extension extends the scheduler.
// each task type should implement this interface.
type Extension interface {
	// IsIdempotent returns whether the subtask is idempotent.
	// when tidb restart, the subtask might be left in the running state.
	// if it's idempotent, the scheduler can rerun the subtask, else
	// the scheduler will mark the subtask as failed.
	IsIdempotent(subtask *proto.Subtask) bool
	// GetSubtaskExecutor returns the subtask executor for the subtask.
	// Note: summary is the summary manager of all subtask of the same type now.
	GetSubtaskExecutor(ctx context.Context, task *proto.Task, summary *execute.Summary) (execute.SubtaskExecutor, error)
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
