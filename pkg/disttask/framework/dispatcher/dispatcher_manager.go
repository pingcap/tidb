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
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"go.uber.org/zap"
)

var (
	// checkTaskRunningInterval is the interval for loading tasks.
	checkTaskRunningInterval = 3 * time.Second
	// defaultHistorySubtaskTableGcInterval is the interval of gc history subtask table.
	defaultHistorySubtaskTableGcInterval = 24 * time.Hour
	// defaultCleanUpInterval is the interval of cleanUp routine.
	defaultCleanUpInterval = 10 * time.Minute
)

// WaitTaskFinished is used to sync the test.
var WaitTaskFinished = make(chan struct{})

func (dm *Manager) getDispatcherCount() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return len(dm.mu.dispatchers)
}

func (dm *Manager) addDispatcher(taskID int64, dispatcher Dispatcher) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.mu.dispatchers[taskID] = dispatcher
}

func (dm *Manager) hasDispatcher(taskID int64) bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	_, ok := dm.mu.dispatchers[taskID]
	return ok
}

func (dm *Manager) delDispatcher(taskID int64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	delete(dm.mu.dispatchers, taskID)
}

func (dm *Manager) clearDispatchers() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.mu.dispatchers = make(map[int64]Dispatcher)
}

// Manager manage a bunch of dispatchers.
// Dispatcher schedule and monitor tasks.
// The scheduling task number is limited by size of gPool.
type Manager struct {
	ctx         context.Context
	cancel      context.CancelFunc
	taskMgr     TaskManager
	wg          tidbutil.WaitGroupWrapper
	gPool       *spool.Pool
	slotMgr     *slotManager
	initialized bool
	// serverID, it's value is ip:port now.
	serverID string

	finishCh chan struct{}

	mu struct {
		syncutil.RWMutex
		dispatchers map[int64]Dispatcher
	}
}

// NewManager creates a dispatcher struct.
func NewManager(ctx context.Context, taskMgr TaskManager, serverID string) (*Manager, error) {
	dispatcherManager := &Manager{
		taskMgr:  taskMgr,
		serverID: serverID,
		slotMgr:  newSlotManager(),
	}
	gPool, err := spool.NewPool("dispatch_pool", int32(proto.MaxConcurrentTask), util.DistTask, spool.WithBlocking(true))
	if err != nil {
		return nil, err
	}
	dispatcherManager.gPool = gPool
	dispatcherManager.ctx, dispatcherManager.cancel = context.WithCancel(ctx)
	dispatcherManager.mu.dispatchers = make(map[int64]Dispatcher)
	dispatcherManager.finishCh = make(chan struct{}, proto.MaxConcurrentTask)

	return dispatcherManager, nil
}

// Start the dispatcherManager, start the dispatchTaskLoop to start multiple dispatchers.
func (dm *Manager) Start() {
	failpoint.Inject("disableDispatcherManager", func() {
		failpoint.Return()
	})
	dm.wg.Run(dm.dispatchTaskLoop)
	dm.wg.Run(dm.gcSubtaskHistoryTableLoop)
	dm.wg.Run(dm.cleanUpLoop)
	dm.initialized = true
}

// Stop the dispatcherManager.
func (dm *Manager) Stop() {
	dm.cancel()
	dm.gPool.ReleaseAndWait()
	dm.wg.Wait()
	dm.clearDispatchers()
	dm.initialized = false
	close(dm.finishCh)
}

// Initialized check the manager initialized.
func (dm *Manager) Initialized() bool {
	return dm.initialized
}

// dispatchTaskLoop dispatches the tasks.
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
		}

		taskCnt := dm.getDispatcherCount()
		if taskCnt >= proto.MaxConcurrentTask {
			logutil.BgLogger().Info("dispatched tasks reached limit",
				zap.Int("current", taskCnt), zap.Int("max", proto.MaxConcurrentTask))
			continue
		}

		tasks, err := dm.taskMgr.GetTopUnfinishedTasks(dm.ctx)
		if err != nil {
			logutil.BgLogger().Warn("get unfinished tasks failed", zap.Error(err))
			continue
		}

		dispatchableTasks := make([]*proto.Task, 0, len(tasks))
		for _, task := range tasks {
			if dm.hasDispatcher(task.ID) {
				continue
			}
			// we check it before start dispatcher, so no need to check it again.
			// see startDispatcher.
			// this should not happen normally, unless user modify system table
			// directly.
			if getDispatcherFactory(task.Type) == nil {
				logutil.BgLogger().Warn("unknown task type", zap.Int64("task-id", task.ID),
					zap.Stringer("task-type", task.Type))
				dm.failTask(task.ID, task.State, errors.New("unknown task type"))
				continue
			}
			dispatchableTasks = append(dispatchableTasks, task)
		}
		if len(dispatchableTasks) == 0 {
			continue
		}

		if err = dm.slotMgr.update(dm.ctx, dm.taskMgr); err != nil {
			logutil.BgLogger().Warn("update used slot failed", zap.Error(err))
			continue
		}
		for _, task := range dispatchableTasks {
			taskCnt = dm.getDispatcherCount()
			if taskCnt >= proto.MaxConcurrentTask {
				break
			}
			reservedExecID, ok := dm.slotMgr.canReserve(task)
			if !ok {
				// task of lower priority might be able to be dispatched.
				continue
			}
			metrics.DistTaskGauge.WithLabelValues(task.Type.String(), metrics.DispatchingStatus).Inc()
			metrics.UpdateMetricsForDispatchTask(task.ID, task.Type)
			dm.startDispatcher(task, reservedExecID)
		}
	}
}

func (dm *Manager) failTask(id int64, currState proto.TaskState, err error) {
	if err2 := dm.taskMgr.FailTask(dm.ctx, id, currState, err); err2 != nil {
		logutil.BgLogger().Warn("failed to update task state to failed",
			zap.Int64("task-id", id), zap.Error(err2))
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
			err := dm.taskMgr.GCSubtasks(dm.ctx)
			if err != nil {
				logutil.BgLogger().Warn("subtask history table gc failed", zap.Error(err))
			} else {
				logutil.Logger(dm.ctx).Info("subtask history table gc success")
			}
		}
	}
}

func (dm *Manager) startDispatcher(basicTask *proto.Task, reservedExecID string) {
	task, err := dm.taskMgr.GetTaskByID(dm.ctx, basicTask.ID)
	if err != nil {
		logutil.BgLogger().Error("get task failed", zap.Error(err))
		return
	}

	dispatcherFactory := getDispatcherFactory(task.Type)
	dispatcher := dispatcherFactory(dm.ctx, dm.taskMgr, dm.serverID, task)
	if err = dispatcher.Init(); err != nil {
		logutil.BgLogger().Error("init dispatcher failed", zap.Error(err))
		dm.failTask(task.ID, task.State, err)
		return
	}
	dm.addDispatcher(task.ID, dispatcher)
	dm.slotMgr.reserve(basicTask, reservedExecID)
	// Using the pool with block, so it wouldn't return an error.
	_ = dm.gPool.Run(func() {
		defer func() {
			dispatcher.Close()
			dm.delDispatcher(task.ID)
			dm.slotMgr.unReserve(basicTask, reservedExecID)
		}()
		metrics.UpdateMetricsForRunTask(task)
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

// doCleanUpRoutine processes clean up routine defined by each type of tasks and cleanUpMeta.
// For example:
//
//	tasks with global sort should clean up tmp files stored on S3.
func (dm *Manager) doCleanUpRoutine() {
	cnt := dm.CleanUpMeta()
	if cnt != 0 {
		logutil.BgLogger().Info("clean up nodes in framework meta since nodes shutdown", zap.Int("cnt", cnt))
	}
	tasks, err := dm.taskMgr.GetTasksInStates(
		dm.ctx,
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

// CleanUpMeta clean up old node info in dist_framework_meta table.
func (dm *Manager) CleanUpMeta() int {
	// Safe to discard errors since this function can be called at regular intervals.
	serverInfos, err := GenerateTaskExecutorNodes(dm.ctx)
	if err != nil {
		logutil.BgLogger().Warn("generate task executor nodes met error")
		return 0
	}

	oldNodes, err := dm.taskMgr.GetAllNodes(dm.ctx)
	if err != nil {
		logutil.BgLogger().Warn("get all nodes met error")
		return 0
	}

	cleanNodes := make([]string, 0)
	for _, nodeID := range oldNodes {
		if ok := disttaskutil.MatchServerInfo(serverInfos, nodeID); !ok {
			cleanNodes = append(cleanNodes, nodeID)
		}
	}
	if len(cleanNodes) == 0 {
		return 0
	}
	logutil.BgLogger().Info("start to clean up dist_framework_meta")
	err = dm.taskMgr.CleanUpMeta(dm.ctx, cleanNodes)
	if err != nil {
		logutil.BgLogger().Warn("clean up dist_framework_meta met error")
		return 0
	}
	return len(cleanNodes)
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

	return dm.taskMgr.TransferTasks2History(dm.ctx, cleanedTasks)
}

// MockDispatcher mock one dispatcher for one task, only used for tests.
func (dm *Manager) MockDispatcher(task *proto.Task) *BaseDispatcher {
	return NewBaseDispatcher(dm.ctx, dm.taskMgr, dm.serverID, task)
}
