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

package dispatcher

import (
	"context"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/syncutil"
)

// TaskManager defines the interface to access task table.
type TaskManager interface {
	GetGlobalTasksInStates(ctx context.Context, states ...interface{}) (task []*proto.Task, err error)
	GetGlobalTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error)
	UpdateGlobalTaskAndAddSubTasks(ctx context.Context, gTask *proto.Task, subtasks []*proto.Subtask, prevState proto.TaskState) (bool, error)
	GCSubtasks(ctx context.Context) error
	GetAllNodes(ctx context.Context) ([]string, error)
	CleanUpMeta(ctx context.Context, nodes []string) error
	TransferTasks2History(ctx context.Context, tasks []*proto.Task) error
	CancelGlobalTask(ctx context.Context, taskID int64) error
	PauseTask(ctx context.Context, taskKey string) (bool, error)
	GetSubtaskInStatesCnt(ctx context.Context, taskID int64, states ...interface{}) (int64, error)
	ResumeSubtasks(ctx context.Context, taskID int64) error
	CollectSubTaskError(ctx context.Context, taskID int64) ([]error, error)
	TransferSubTasks2History(ctx context.Context, taskID int64) error
	UpdateFailedSchedulerIDs(ctx context.Context, taskID int64, replaceNodes map[string]string) error
	GetNodesByRole(ctx context.Context, role string) (map[string]bool, error)
	GetSchedulerIDsByTaskID(ctx context.Context, taskID int64) ([]string, error)
	GetSucceedSubtasksByStep(ctx context.Context, taskID int64, step proto.Step) ([]*proto.Subtask, error)
	GetSchedulerIDsByTaskIDAndStep(ctx context.Context, taskID int64, step proto.Step) ([]string, error)
	// GetSubtaskCntGroupByStates returns the count of subtasks of some step group by state.
	GetSubtaskCntGroupByStates(ctx context.Context, taskID int64, step proto.Step) (map[proto.TaskState]int64, error)

	WithNewSession(fn func(se sessionctx.Context) error) error
	WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error
}

// Extension is used to control the process operations for each task.
// it's used to extend functions of BaseDispatcher.
// as golang doesn't support inheritance, we embed this interface in Dispatcher
// to simulate abstract method as in other OO languages.
type Extension interface {
	// OnTick is used to handle the ticker event, if business impl need to do some periodical work, you can
	// do it here, but don't do too much work here, because the ticker interval is small, and it will block
	// the event is generated every checkTaskRunningInterval, and only when the task NOT FINISHED and NO ERROR.
	OnTick(ctx context.Context, task *proto.Task)

	// OnNextSubtasksBatch is used to generate batch of subtasks for next stage
	// NOTE: don't change gTask.State inside, framework will manage it.
	// it's called when:
	// 	1. task is pending and entering it's first step.
	// 	2. subtasks dispatched has all finished with no error.
	// when next step is StepDone, it should return nil, nil.
	OnNextSubtasksBatch(ctx context.Context, h TaskHandle, task *proto.Task, serverInfo []*infosync.ServerInfo, step proto.Step) (subtaskMetas [][]byte, err error)

	// OnDone is called when task is done, either finished successfully or failed
	// with error.
	// if the task is failed when initializing dispatcher, or it's an unknown task,
	// we don't call this function.
	OnDone(ctx context.Context, h TaskHandle, task *proto.Task) error

	// GetEligibleInstances is used to get the eligible instances for the task.
	// on certain condition we may want to use some instances to do the task, such as instances with more disk.
	// The bool return value indicates whether filter instances by role.
	GetEligibleInstances(ctx context.Context, task *proto.Task) ([]*infosync.ServerInfo, bool, error)

	// IsRetryableErr is used to check whether the error occurred in dispatcher is retryable.
	IsRetryableErr(err error) bool

	// GetNextStep is used to get the next step for the task.
	// if task runs successfully, it should go from StepInit to business steps,
	// then to StepDone, then dispatcher will mark it as finished.
	GetNextStep(task *proto.Task) proto.Step
}

// dispatcherFactoryFn is used to create a dispatcher.
type dispatcherFactoryFn func(ctx context.Context, taskMgr TaskManager, serverID string, task *proto.Task) Dispatcher

var dispatcherFactoryMap = struct {
	syncutil.RWMutex
	m map[proto.TaskType]dispatcherFactoryFn
}{
	m: make(map[proto.TaskType]dispatcherFactoryFn),
}

// RegisterDispatcherFactory is used to register the dispatcher factory.
// normally dispatcher ctor should be registered before the server start.
// and should be called in a single routine, such as in init().
// after the server start, there's should be no write to the map.
// but for index backfill, the register call stack is so deep, not sure
// if it's safe to do so, so we use a lock here.
func RegisterDispatcherFactory(taskType proto.TaskType, ctor dispatcherFactoryFn) {
	dispatcherFactoryMap.Lock()
	defer dispatcherFactoryMap.Unlock()
	dispatcherFactoryMap.m[taskType] = ctor
}

// getDispatcherFactory is used to get the dispatcher factory.
func getDispatcherFactory(taskType proto.TaskType) dispatcherFactoryFn {
	dispatcherFactoryMap.RLock()
	defer dispatcherFactoryMap.RUnlock()
	return dispatcherFactoryMap.m[taskType]
}

// ClearDispatcherFactory is only used in test.
func ClearDispatcherFactory() {
	dispatcherFactoryMap.Lock()
	defer dispatcherFactoryMap.Unlock()
	dispatcherFactoryMap.m = make(map[proto.TaskType]dispatcherFactoryFn)
}

// CleanUpRoutine is used for the framework to do some clean up work if the task is finished.
type CleanUpRoutine interface {
	// CleanUp do the clean up work.
	CleanUp(ctx context.Context, task *proto.Task) error
}
type cleanUpFactoryFn func() CleanUpRoutine

var cleanUpFactoryMap = struct {
	syncutil.RWMutex
	m map[proto.TaskType]cleanUpFactoryFn
}{
	m: make(map[proto.TaskType]cleanUpFactoryFn),
}

// RegisterDispatcherCleanUpFactory is used to register the dispatcher clean up factory.
// normally dispatcher cleanup is used in the dispatcher_manager gcTaskLoop to do clean up
// works when tasks are finished.
func RegisterDispatcherCleanUpFactory(taskType proto.TaskType, ctor cleanUpFactoryFn) {
	cleanUpFactoryMap.Lock()
	defer cleanUpFactoryMap.Unlock()
	cleanUpFactoryMap.m[taskType] = ctor
}

// getDispatcherCleanUpFactory is used to get the dispatcher factory.
func getDispatcherCleanUpFactory(taskType proto.TaskType) cleanUpFactoryFn {
	cleanUpFactoryMap.RLock()
	defer cleanUpFactoryMap.RUnlock()
	return cleanUpFactoryMap.m[taskType]
}

// ClearDispatcherCleanUpFactory is only used in test.
func ClearDispatcherCleanUpFactory() {
	cleanUpFactoryMap.Lock()
	defer cleanUpFactoryMap.Unlock()
	cleanUpFactoryMap.m = make(map[proto.TaskType]cleanUpFactoryFn)
}
