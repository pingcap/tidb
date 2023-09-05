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

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/util/syncutil"
)

// Extension is used to control the process operations for each task.
// it's used to extend functions of BaseDispatcher.
// as golang doesn't support inheritance, we embed this interface in Dispatcher
// to simulate abstract method as in other OO languages.
type Extension interface {
	// OnTick is used to handle the ticker event, if business impl need to do some periodical work, you can
	// do it here, but don't do too much work here, because the ticker interval is small, and it will block
	// the event is generated every checkTaskRunningInterval, and only when the task NOT FINISHED and NO ERROR.
	OnTick(ctx context.Context, gTask *proto.Task)
	// OnNextStage is used to move the task to next stage, if returns no error and there's no new subtasks
	// the task is finished.
	// NOTE: don't change gTask.State inside, framework will manage it.
	// it's called when:
	// 	1. task is pending and entering it's first step.
	// 	2. subtasks of previous step has all finished with no error.
	OnNextStage(ctx context.Context, h TaskHandle, gTask *proto.Task) (subtaskMetas [][]byte, err error)
	// OnErrStage is called when:
	// 	1. subtask is finished with error.
	// 	2. task is cancelled after we have dispatched some subtasks.
	OnErrStage(ctx context.Context, h TaskHandle, gTask *proto.Task, receiveErr []error) (subtaskMeta []byte, err error)
	// GetEligibleInstances is used to get the eligible instances for the task.
	// on certain condition we may want to use some instances to do the task, such as instances with more disk.
	GetEligibleInstances(ctx context.Context, gTask *proto.Task) ([]*infosync.ServerInfo, error)
	// IsRetryableErr is used to check whether the error occurred in dispatcher is retryable.
	IsRetryableErr(err error) bool
}

// FactoryFn is used to create a dispatcher.
type FactoryFn func(ctx context.Context, taskMgr *storage.TaskManager, serverID string, task *proto.Task) Dispatcher

var dispatcherFactoryMap = struct {
	syncutil.RWMutex
	m map[string]FactoryFn
}{
	m: make(map[string]FactoryFn),
}

// RegisterDispatcherFactory is used to register the dispatcher factory.
// normally dispatcher ctor should be registered before the server start.
// and should be called in a single routine, such as in init().
// after the server start, there's should be no write to the map.
// but for index backfill, the register call stack is so deep, not sure
// if it's safe to do so, so we use a lock here.
func RegisterDispatcherFactory(taskType string, ctor FactoryFn) {
	dispatcherFactoryMap.Lock()
	defer dispatcherFactoryMap.Unlock()
	dispatcherFactoryMap.m[taskType] = ctor
}

// GetDispatcherFactory is used to get the dispatcher factory.
func GetDispatcherFactory(taskType string) FactoryFn {
	dispatcherFactoryMap.RLock()
	defer dispatcherFactoryMap.RUnlock()
	return dispatcherFactoryMap.m[taskType]
}

// ClearDispatcherFactory is only used in test
func ClearDispatcherFactory() {
	dispatcherFactoryMap.Lock()
	defer dispatcherFactoryMap.Unlock()
	dispatcherFactoryMap.m = make(map[string]FactoryFn)
}
