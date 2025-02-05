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
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/syncutil"
)

// TaskManager defines the interface to access task table.
type TaskManager interface {
	// GetTopUnfinishedTasks returns unfinished tasks, limited by MaxConcurrentTask*2,
	// to make sure lower rank tasks can be scheduled if resource is enough.
	// The returned tasks are sorted by task order, see proto.Task.
	GetTopUnfinishedTasks(ctx context.Context) ([]*proto.TaskBase, error)
	// GetAllSubtasks gets all subtasks with basic columns.
	GetAllSubtasks(ctx context.Context) ([]*proto.SubtaskBase, error)
	GetTasksInStates(ctx context.Context, states ...any) (task []*proto.Task, err error)
	GetTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error)
	GetTaskBaseByID(ctx context.Context, taskID int64) (task *proto.TaskBase, err error)
	GCSubtasks(ctx context.Context) error
	GetAllNodes(ctx context.Context) ([]proto.ManagedNode, error)
	DeleteDeadNodes(ctx context.Context, nodes []string) error
	// TransferTasks2History transfer tasks, and it's related subtasks to history tables.
	TransferTasks2History(ctx context.Context, tasks []*proto.Task) error
	// CancelTask updated task state to canceling.
	CancelTask(ctx context.Context, taskID int64) error
	// FailTask updates task state to Failed and updates task error.
	FailTask(ctx context.Context, taskID int64, currentState proto.TaskState, taskErr error) error
	// RevertTask updates task state to reverting, and task error.
	RevertTask(ctx context.Context, taskID int64, taskState proto.TaskState, taskErr error) error
	// RevertedTask updates task state to reverted.
	RevertedTask(ctx context.Context, taskID int64) error
	// PauseTask updated task state to pausing.
	PauseTask(ctx context.Context, taskKey string) (bool, error)
	// PausedTask updated task state to paused.
	PausedTask(ctx context.Context, taskID int64) error
	// ResumedTask updated task state from resuming to running.
	ResumedTask(ctx context.Context, taskID int64) error
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
	GetActiveSubtasks(ctx context.Context, taskID int64) ([]*proto.SubtaskBase, error)
	// GetSubtaskCntGroupByStates returns the count of subtasks of some step group by state.
	GetSubtaskCntGroupByStates(ctx context.Context, taskID int64, step proto.Step) (map[proto.SubtaskState]int64, error)
	ResumeSubtasks(ctx context.Context, taskID int64) error
	GetSubtaskErrors(ctx context.Context, taskID int64) ([]error, error)
	UpdateSubtasksExecIDs(ctx context.Context, subtasks []*proto.SubtaskBase) error

	// GetAllSubtasksByStepAndState gets all subtasks by given states for one step.
	GetAllSubtasksByStepAndState(ctx context.Context, taskID int64, step proto.Step, state proto.SubtaskState) ([]*proto.Subtask, error)

	WithNewSession(fn func(se sessionctx.Context) error) error
	WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error
}

// Extension is used to control the process operations for each task.
// it's used to extend functions of BaseScheduler.
// as golang doesn't support inheritance, we embed this interface in Scheduler
// to simulate abstract method as in other OO languages.
type Extension interface {
	// OnTick is used to handle the ticker event, if business impl need to do some periodical work, you can
	// do it here, but don't do too much work here, because the ticker interval is small, and it will block
	// the event is generated every CheckTaskRunningInterval, and only when the task NOT FINISHED and NO ERROR.
	OnTick(ctx context.Context, task *proto.Task)

	// OnNextSubtasksBatch is used to generate batch of subtasks for next stage
	// NOTE: don't change task.State inside, framework will manage it.
	// it's called when:
	// 	1. task is pending and entering it's first step.
	// 	2. subtasks scheduled has all finished with no error.
	// when next step is StepDone, it should return nil, nil.
	OnNextSubtasksBatch(ctx context.Context, h storage.TaskHandle, task *proto.Task, execIDs []string, step proto.Step) (subtaskMetas [][]byte, err error)

	// OnDone is called when task is done, either finished successfully or failed
	// with error.
	// if the task is failed when initializing scheduler, or it's an unknown task,
	// we don't call this function.
	OnDone(ctx context.Context, h storage.TaskHandle, task *proto.Task) error

	// GetEligibleInstances is used to get the eligible instances for the task.
	// on certain condition we may want to use some instances to do the task, such as instances with more disk.
	// if returned instances is empty, it means all instances are eligible.
	// TODO: run import from server disk using framework makes this logic complicated,
	// the instance might not be managed by framework.
	GetEligibleInstances(ctx context.Context, task *proto.Task) ([]string, error)

	// IsRetryableErr is used to check whether the error occurred in scheduler is retryable.
	IsRetryableErr(err error) bool

	// GetNextStep is used to get the next step for the task.
	// if task runs successfully, it should go from StepInit to business steps,
	// then to StepDone, then scheduler will mark it as finished.
	// NOTE: don't depend on task meta to decide the next step, if it's really needed,
	// initialize required fields on scheduler.Init
	GetNextStep(task *proto.TaskBase) proto.Step
}

// Param is used to pass parameters when creating scheduler.
type Param struct {
	taskMgr        TaskManager
	nodeMgr        *NodeManager
	slotMgr        *SlotManager
	serverID       string
	allocatedSlots bool
}

// schedulerFactoryFn is used to create a scheduler.
type schedulerFactoryFn func(ctx context.Context, task *proto.Task, param Param) Scheduler

var schedulerFactoryMap = struct {
	syncutil.RWMutex
	m map[proto.TaskType]schedulerFactoryFn
}{
	m: make(map[proto.TaskType]schedulerFactoryFn),
}

// RegisterSchedulerFactory is used to register the scheduler factory.
// normally scheduler ctor should be registered before the server start.
// and should be called in a single routine, such as in init().
// after the server start, there's should be no write to the map.
// but for index backfill, the register call stack is so deep, not sure
// if it's safe to do so, so we use a lock here.
func RegisterSchedulerFactory(taskType proto.TaskType, ctor schedulerFactoryFn) {
	schedulerFactoryMap.Lock()
	defer schedulerFactoryMap.Unlock()
	schedulerFactoryMap.m[taskType] = ctor
}

// getSchedulerFactory is used to get the scheduler factory.
func getSchedulerFactory(taskType proto.TaskType) schedulerFactoryFn {
	schedulerFactoryMap.RLock()
	defer schedulerFactoryMap.RUnlock()
	return schedulerFactoryMap.m[taskType]
}

// ClearSchedulerFactory is only used in test.
func ClearSchedulerFactory() {
	schedulerFactoryMap.Lock()
	defer schedulerFactoryMap.Unlock()
	schedulerFactoryMap.m = make(map[proto.TaskType]schedulerFactoryFn)
}

// CleanUpRoutine is used for the framework to do some clean up work if the task is finished.
type CleanUpRoutine interface {
	// CleanUp do the cleanup work.
	// task.Meta can be updated here, such as redacting some sensitive info.
	CleanUp(ctx context.Context, task *proto.Task) error
}
type cleanUpFactoryFn func() CleanUpRoutine

var cleanUpFactoryMap = struct {
	syncutil.RWMutex
	m map[proto.TaskType]cleanUpFactoryFn
}{
	m: make(map[proto.TaskType]cleanUpFactoryFn),
}

// RegisterSchedulerCleanUpFactory is used to register the scheduler clean up factory.
// normally scheduler cleanup is used in the scheduler_manager gcTaskLoop to do clean up
// works when tasks are finished.
func RegisterSchedulerCleanUpFactory(taskType proto.TaskType, ctor cleanUpFactoryFn) {
	cleanUpFactoryMap.Lock()
	defer cleanUpFactoryMap.Unlock()
	cleanUpFactoryMap.m[taskType] = ctor
}

// getSchedulerCleanUpFactory is used to get the scheduler factory.
func getSchedulerCleanUpFactory(taskType proto.TaskType) cleanUpFactoryFn {
	cleanUpFactoryMap.RLock()
	defer cleanUpFactoryMap.RUnlock()
	return cleanUpFactoryMap.m[taskType]
}

// ClearSchedulerCleanUpFactory is only used in test.
func ClearSchedulerCleanUpFactory() {
	cleanUpFactoryMap.Lock()
	defer cleanUpFactoryMap.Unlock()
	cleanUpFactoryMap.m = make(map[proto.TaskType]cleanUpFactoryFn)
}
