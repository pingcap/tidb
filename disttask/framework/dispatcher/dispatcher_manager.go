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
	"time"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/syncutil"
	"go.uber.org/zap"
)

func (dm *Manager) getRunningTaskCnt() int {
	dm.runningTasks.RLock()
	defer dm.runningTasks.RUnlock()
	return len(dm.runningTasks.taskIDs)
}

func (dm *Manager) setRunningTask(task *proto.Task, dispatcher *dispatcher) {
	dm.runningTasks.Lock()
	defer dm.runningTasks.Unlock()
	dm.runningTasks.taskIDs[task.ID] = struct{}{}
	dm.runningTasks.dispatchers[task.ID] = dispatcher
}

func (dm *Manager) isRunningTask(taskID int64) bool {
	dm.runningTasks.Lock()
	defer dm.runningTasks.Unlock()
	_, ok := dm.runningTasks.taskIDs[taskID]
	return ok
}

func (dm *Manager) delRunningTask(taskID int64) {
	dm.runningTasks.Lock()
	defer dm.runningTasks.Unlock()
	delete(dm.runningTasks.taskIDs, taskID)
	delete(dm.runningTasks.dispatchers, taskID)
}

func (dm *Manager) clearRunningTasks() {
	dm.runningTasks.Lock()
	defer dm.runningTasks.Unlock()
	for id := range dm.runningTasks.dispatchers {
		delete(dm.runningTasks.dispatchers, id)
	}
	for id := range dm.runningTasks.taskIDs {
		delete(dm.runningTasks.taskIDs, id)
	}
}

// Manager manage a bunch of dispatchers.
// Dispatcher schedule and monitor tasks.
// The scheduling task number is limited by size of gPool.
type Manager struct {
	ctx      context.Context
	cancel   context.CancelFunc
	taskMgr  *storage.TaskManager
	wg       tidbutil.WaitGroupWrapper
	gPool    *spool.Pool
	inited   bool
	serverID string

	runningTasks struct {
		syncutil.RWMutex
		taskIDs     map[int64]struct{}
		dispatchers map[int64]*dispatcher
	}
	finishedTaskCh chan *proto.Task
}

// NewManager creates a dispatcher struct.
func NewManager(ctx context.Context, taskTable *storage.TaskManager, serverID string) (*Manager, error) {
	dispatcherManager := &Manager{
		taskMgr:        taskTable,
		finishedTaskCh: make(chan *proto.Task, DefaultDispatchConcurrency),
		serverID:       serverID,
	}
	gPool, err := spool.NewPool("dispatch_pool", int32(DefaultDispatchConcurrency), util.DistTask, spool.WithBlocking(true))
	if err != nil {
		return nil, err
	}
	dispatcherManager.gPool = gPool
	dispatcherManager.ctx, dispatcherManager.cancel = context.WithCancel(ctx)
	dispatcherManager.runningTasks.taskIDs = make(map[int64]struct{})
	dispatcherManager.runningTasks.dispatchers = make(map[int64]*dispatcher)

	return dispatcherManager, nil
}

// Start the dispatcherManager, start the dispatchTaskLoop to start multiple dispatchers.
func (dm *Manager) Start() {
	dm.wg.Run(dm.dispatchTaskLoop)
	dm.inited = true
}

// Stop the dispatcherManager.
func (dm *Manager) Stop() {
	dm.cancel()
	dm.gPool.ReleaseAndWait()
	dm.wg.Wait()
	dm.clearRunningTasks()
	dm.inited = false
}

// Inited check the manager inited.
func (dm *Manager) Inited() bool {
	return dm.inited
}

// dispatchTaskLoop dispatches the global tasks.
func (dm *Manager) dispatchTaskLoop() {
	logutil.BgLogger().Info("dispatch task loop start")
	ticker := time.NewTicker(checkTaskRunningInterval)
	defer ticker.Stop()
	for {
		select {
		case <-dm.ctx.Done():
			logutil.BgLogger().Info("dispatch task loop exits", zap.Error(dm.ctx.Err()), zap.Int64("interval", int64(checkTaskRunningInterval)/1000000))
			return
		case <-ticker.C:
			cnt := dm.getRunningTaskCnt()
			if dm.checkConcurrencyOverflow(cnt) {
				break
			}

			// TODO: Consider getting these tasks, in addition to the task being worked on..
			tasks, err := dm.taskMgr.GetGlobalTasksInStates(proto.TaskStatePending, proto.TaskStateRunning, proto.TaskStateReverting, proto.TaskStateCancelling)
			if err != nil {
				logutil.BgLogger().Warn("get unfinished(pending, running, reverting or cancelling) tasks failed", zap.Error(err))
				break
			}

			// There are currently no global tasks to work on.
			if len(tasks) == 0 {
				break
			}
			for _, task := range tasks {
				// This global task is running, so no need to reprocess it.
				if dm.isRunningTask(task.ID) {
					continue
				}
				// the task is not in runningTasks set when:
				// owner changed or task is cancelled when status is pending.
				if task.State == proto.TaskStateRunning || task.State == proto.TaskStateReverting || task.State == proto.TaskStateCancelling {
					dm.startDispatcher(task)
					cnt++
					continue
				}
				if dm.checkConcurrencyOverflow(cnt) {
					break
				}
				dm.startDispatcher(task)
				cnt++
			}
		}
	}
}

func (*Manager) checkConcurrencyOverflow(cnt int) bool {
	if cnt >= DefaultDispatchConcurrency {
		logutil.BgLogger().Info("dispatch task loop, running task cnt is more than concurrency limitation",
			zap.Int("running cnt", cnt), zap.Int("concurrency", DefaultDispatchConcurrency))
		return true
	}
	return false
}

func (dm *Manager) startDispatcher(task *proto.Task) {
	// Using the pool with block, so it wouldn't return an error.
	_ = dm.gPool.Run(func() {
		dispatcher := newDispatcher(dm.ctx, dm.taskMgr, dm.serverID, task)
		dm.setRunningTask(task, dispatcher)
		dispatcher.executeTask()
		dm.delRunningTask(task.ID)
	})
}

// MockDispatcher mock one dispatcher for one task, only used for tests.
func (dm *Manager) MockDispatcher(task *proto.Task) *dispatcher {
	return newDispatcher(dm.ctx, dm.taskMgr, dm.serverID, task)
}
