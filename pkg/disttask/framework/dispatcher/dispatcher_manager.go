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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"go.uber.org/zap"
)

var (
	// DefaultDispatchConcurrency is the default concurrency for dispatching task.
	DefaultDispatchConcurrency = 4
	// checkTaskRunningInterval is the interval for loading tasks.
	checkTaskRunningInterval = 3 * time.Second
	// defaultHistorySubtaskTableGcInterval is the interval of gc history subtask table.
	defaultHistorySubtaskTableGcInterval = 24 * time.Hour
	// defaultCleanUpInterval is the interval of cleanUp routine.
	defaultCleanUpInterval = 10 * time.Minute
)

// WaitTaskFinished is used to sync the test.
var WaitTaskFinished = make(chan struct{})

func (dm *Manager) getRunningTaskCnt() int {
	dm.runningTasks.RLock()
	defer dm.runningTasks.RUnlock()
	return len(dm.runningTasks.taskIDs)
}

func (dm *Manager) setRunningTask(task *proto.Task, dispatcher Dispatcher) {
	dm.runningTasks.Lock()
	defer dm.runningTasks.Unlock()
	dm.runningTasks.taskIDs[task.ID] = struct{}{}
	dm.runningTasks.dispatchers[task.ID] = dispatcher
	metrics.UpdateMetricsForRunTask(task)
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
	ctx     context.Context
	cancel  context.CancelFunc
	taskMgr *storage.TaskManager
	wg      tidbutil.WaitGroupWrapper
	gPool   *spool.Pool
	inited  bool
	// serverID, it's value is ip:port now.
	serverID string

	finishCh chan struct{}

	runningTasks struct {
		syncutil.RWMutex
		taskIDs     map[int64]struct{}
		dispatchers map[int64]Dispatcher
	}
}

// NewManager creates a dispatcher struct.
func NewManager(ctx context.Context, taskTable *storage.TaskManager, serverID string) (*Manager, error) {
	dispatcherManager := &Manager{
		taskMgr:  taskTable,
		serverID: serverID,
	}
	gPool, err := spool.NewPool("dispatch_pool", int32(DefaultDispatchConcurrency), util.DistTask, spool.WithBlocking(true))
	if err != nil {
		return nil, err
	}
	dispatcherManager.gPool = gPool
	dispatcherManager.ctx, dispatcherManager.cancel = context.WithCancel(ctx)
	dispatcherManager.runningTasks.taskIDs = make(map[int64]struct{})
	dispatcherManager.runningTasks.dispatchers = make(map[int64]Dispatcher)
	dispatcherManager.finishCh = make(chan struct{}, DefaultDispatchConcurrency)

	return dispatcherManager, nil
}

// Start the dispatcherManager, start the dispatchTaskLoop to start multiple dispatchers.
func (dm *Manager) Start() {
	dm.wg.Run(dm.dispatchTaskLoop)
	dm.wg.Run(dm.gcSubtaskHistoryTableLoop)
	dm.wg.Run(dm.cleanUpLoop)
	dm.inited = true
}

// Stop the dispatcherManager.
func (dm *Manager) Stop() {
	dm.cancel()
	dm.gPool.ReleaseAndWait()
	dm.wg.Wait()
	dm.clearRunningTasks()
	dm.inited = false
	close(dm.finishCh)
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
			tasks, err := dm.taskMgr.GetGlobalTasksInStates(
				proto.TaskStatePending,
				proto.TaskStateRunning,
				proto.TaskStateReverting,
				proto.TaskStateCancelling,
				proto.TaskStateResuming,
			)
			if err != nil {
				logutil.BgLogger().Warn("get unfinished(pending, running, reverting, cancelling, resuming) tasks failed", zap.Error(err))
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
				metrics.DistTaskGauge.WithLabelValues(task.Type.String(), metrics.DispatchingStatus).Inc()
				// we check it before start dispatcher, so no need to check it again.
				// see startDispatcher.
				// this should not happen normally, unless user modify system table
				// directly.
				if getDispatcherFactory(task.Type) == nil {
					logutil.BgLogger().Warn("unknown task type", zap.Int64("task-id", task.ID),
						zap.Stringer("task-type", task.Type))
					dm.failTask(task, errors.New("unknown task type"))
					continue
				}
				// the task is not in runningTasks set when:
				// owner changed or task is cancelled when status is pending.
				if task.State == proto.TaskStateRunning || task.State == proto.TaskStateReverting || task.State == proto.TaskStateCancelling {
					metrics.UpdateMetricsForDispatchTask(task)
					dm.startDispatcher(task)
					cnt++
					continue
				}
				if dm.checkConcurrencyOverflow(cnt) {
					break
				}
				metrics.UpdateMetricsForDispatchTask(task)
				dm.startDispatcher(task)
				cnt++
			}
		}
	}
}

func (dm *Manager) failTask(task *proto.Task, err error) {
	prevState := task.State
	task.State = proto.TaskStateFailed
	task.Error = err
	if _, err2 := dm.taskMgr.UpdateGlobalTaskAndAddSubTasks(task, nil, prevState); err2 != nil {
		logutil.BgLogger().Warn("failed to update task state to failed",
			zap.Int64("task-id", task.ID), zap.Error(err2))
	}
}

func (dm *Manager) gcSubtaskHistoryTableLoop() {
	historySubtaskTableGcInterval := defaultHistorySubtaskTableGcInterval
	failpoint.Inject("historySubtaskTableGcInterval", func(val failpoint.Value) {
		if seconds, ok := val.(int); ok {
			historySubtaskTableGcInterval = time.Second * time.Duration(seconds)
		}

		<-WaitTaskFinished
	})

	logutil.Logger(dm.ctx).Info("subtask table gc loop start")
	ticker := time.NewTicker(historySubtaskTableGcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-dm.ctx.Done():
			logutil.BgLogger().Info("subtask history table gc loop exits", zap.Error(dm.ctx.Err()))
			return
		case <-ticker.C:
			err := dm.taskMgr.GCSubtasks()
			if err != nil {
				logutil.BgLogger().Warn("subtask history table gc failed", zap.Error(err))
			} else {
				logutil.Logger(dm.ctx).Info("subtask history table gc success")
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
		dispatcherFactory := getDispatcherFactory(task.Type)
		dispatcher := dispatcherFactory(dm.ctx, dm.taskMgr, dm.serverID, task)
		if err := dispatcher.Init(); err != nil {
			logutil.BgLogger().Error("init dispatcher failed", zap.Error(err))
			dm.failTask(task, err)
			return
		}
		defer func() {
			dispatcher.Close()
			dm.delRunningTask(task.ID)
		}()
		dm.setRunningTask(task, dispatcher)
		dispatcher.ExecuteTask()
		logutil.BgLogger().Info("task finished", zap.Int64("task-id", task.ID))
		dm.finishCh <- struct{}{}
	})
}

func (dm *Manager) cleanUpLoop() {
	logutil.Logger(dm.ctx).Info("cleanUp loop start")
	ticker := time.NewTicker(defaultCleanUpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-dm.ctx.Done():
			logutil.BgLogger().Info("cleanUp loop exits", zap.Error(dm.ctx.Err()))
			return
		case <-dm.finishCh:
			dm.doCleanUpRoutine()
		case <-ticker.C:
			dm.doCleanUpRoutine()
		}
	}
}

// WaitCleanUpFinished is used to sync the test.
var WaitCleanUpFinished = make(chan struct{})

// doCleanUpRoutine processes clean up routine defined by each type of tasks.
// For example:
//
//	tasks with global sort should clean up tmp files stored on S3.
func (dm *Manager) doCleanUpRoutine() {
	tasks, err := dm.taskMgr.GetGlobalTasksInStates(
		proto.TaskStateFailed,
		proto.TaskStateReverted,
		proto.TaskStateSucceed,
	)
	if err != nil {
		logutil.BgLogger().Warn("cleanUp routine failed", zap.Error(err))
		return
	}
	if len(tasks) == 0 {
		return
	}
	logutil.Logger(dm.ctx).Info("cleanUp routine start")
	err = dm.cleanUpFinishedTasks(tasks)
	if err != nil {
		logutil.BgLogger().Warn("cleanUp routine failed", zap.Error(err))
		return
	}
	failpoint.Inject("WaitCleanUpFinished", func() {
		WaitCleanUpFinished <- struct{}{}
	})
	logutil.Logger(dm.ctx).Info("cleanUp routine success")
}

func (dm *Manager) cleanUpFinishedTasks(tasks []*proto.Task) error {
	cleanedTasks := make([]*proto.Task, 0)
	var firstErr error
	for _, task := range tasks {
		cleanUpFactory := getDispatcherCleanUpFactory(task.Type)
		if cleanUpFactory != nil {
			cleanUp := cleanUpFactory()
			err := cleanUp.CleanUp(dm.ctx, task)
			if err != nil {
				firstErr = err
				break
			}
			cleanedTasks = append(cleanedTasks, task)
		} else {
			// if task doesn't register cleanUp function, mark it as cleaned.
			cleanedTasks = append(cleanedTasks, task)
		}
	}
	if firstErr != nil {
		logutil.BgLogger().Warn("cleanUp routine failed", zap.Error(errors.Trace(firstErr)))
	}

	return dm.taskMgr.TransferTasks2History(cleanedTasks)
}

// MockDispatcher mock one dispatcher for one task, only used for tests.
func (dm *Manager) MockDispatcher(task *proto.Task) *BaseDispatcher {
	return NewBaseDispatcher(dm.ctx, dm.taskMgr, dm.serverID, task)
}
