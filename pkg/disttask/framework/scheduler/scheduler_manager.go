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
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/metrics"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"go.uber.org/zap"
)

var (
	// checkTaskRunningInterval is the interval for loading tasks.
	checkTaskRunningInterval = 3 * time.Second
	// defaultHistorySubtaskTableGcInterval is the interval of gc history subtask table.
	defaultHistorySubtaskTableGcInterval = 24 * time.Hour
	// DefaultCleanUpInterval is the interval of cleanup routine.
	DefaultCleanUpInterval = 10 * time.Minute
)

// WaitTaskFinished is used to sync the test.
var WaitTaskFinished = make(chan struct{})

func (sm *Manager) getSchedulerCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.mu.schedulerMap)
}

func (sm *Manager) addScheduler(taskID int64, scheduler Scheduler) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.schedulerMap[taskID] = scheduler
	sm.mu.schedulers = append(sm.mu.schedulers, scheduler)
	slices.SortFunc(sm.mu.schedulers, func(i, j Scheduler) int {
		return i.GetTask().Compare(j.GetTask())
	})
}

func (sm *Manager) hasScheduler(taskID int64) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.mu.schedulerMap[taskID]
	return ok
}

func (sm *Manager) delScheduler(taskID int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.mu.schedulerMap, taskID)
	for i, scheduler := range sm.mu.schedulers {
		if scheduler.GetTask().ID == taskID {
			sm.mu.schedulers = append(sm.mu.schedulers[:i], sm.mu.schedulers[i+1:]...)
			break
		}
	}
}

func (sm *Manager) clearSchedulers() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.schedulerMap = make(map[int64]Scheduler)
	sm.mu.schedulers = sm.mu.schedulers[:0]
}

// getSchedulers returns a copy of schedulers.
func (sm *Manager) getSchedulers() []Scheduler {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	res := make([]Scheduler, len(sm.mu.schedulers))
	copy(res, sm.mu.schedulers)
	return res
}

// Manager manage a bunch of schedulers.
// Scheduler schedule and monitor tasks.
// The scheduling task number is limited by size of gPool.
type Manager struct {
	ctx         context.Context
	cancel      context.CancelFunc
	taskMgr     TaskManager
	wg          tidbutil.WaitGroupWrapper
	schedulerWG tidbutil.WaitGroupWrapper
	slotMgr     *SlotManager
	nodeMgr     *NodeManager
	balancer    *balancer
	initialized bool
	// serverID, it's value is ip:port now.
	serverID string

	finishCh chan struct{}

	mu struct {
		syncutil.RWMutex
		schedulerMap map[int64]Scheduler
		// in task order
		schedulers []Scheduler
	}
}

// NewManager creates a scheduler struct.
func NewManager(ctx context.Context, taskMgr TaskManager, serverID string) *Manager {
	schedulerManager := &Manager{
		taskMgr:  taskMgr,
		serverID: serverID,
		slotMgr:  newSlotManager(),
		nodeMgr:  newNodeManager(),
	}
	schedulerManager.ctx, schedulerManager.cancel = context.WithCancel(ctx)
	schedulerManager.mu.schedulerMap = make(map[int64]Scheduler)
	schedulerManager.finishCh = make(chan struct{}, proto.MaxConcurrentTask)
	schedulerManager.balancer = newBalancer(Param{
		taskMgr: taskMgr,
		nodeMgr: schedulerManager.nodeMgr,
		slotMgr: schedulerManager.slotMgr,
	})

	return schedulerManager
}

// Start the schedulerManager, start the scheduleTaskLoop to start multiple schedulers.
func (sm *Manager) Start() {
	failpoint.Inject("disableSchedulerManager", func() {
		failpoint.Return()
	})
	// init cached managed nodes
	sm.nodeMgr.refreshManagedNodes(sm.ctx, sm.taskMgr, sm.slotMgr)

	sm.wg.Run(sm.scheduleTaskLoop)
	sm.wg.Run(sm.gcSubtaskHistoryTableLoop)
	sm.wg.Run(sm.cleanupTaskLoop)
	sm.wg.Run(func() {
		sm.nodeMgr.maintainLiveNodesLoop(sm.ctx, sm.taskMgr)
	})
	sm.wg.Run(func() {
		sm.nodeMgr.refreshManagedNodesLoop(sm.ctx, sm.taskMgr, sm.slotMgr)
	})
	sm.wg.Run(func() {
		sm.balancer.balanceLoop(sm.ctx, sm)
	})
	sm.initialized = true
}

// Stop the schedulerManager.
func (sm *Manager) Stop() {
	sm.cancel()
	sm.schedulerWG.Wait()
	sm.wg.Wait()
	sm.clearSchedulers()
	sm.initialized = false
	close(sm.finishCh)
}

// Initialized check the manager initialized.
func (sm *Manager) Initialized() bool {
	return sm.initialized
}

// scheduleTaskLoop schedules the tasks.
func (sm *Manager) scheduleTaskLoop() {
	logutil.BgLogger().Info("schedule task loop start")
	ticker := time.NewTicker(checkTaskRunningInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			logutil.BgLogger().Info("schedule task loop exits", zap.Error(sm.ctx.Err()), zap.Int64("interval", int64(checkTaskRunningInterval)/1000000))
			return
		case <-ticker.C:
		case <-handle.TaskChangedCh:
		}

		taskCnt := sm.getSchedulerCount()
		if taskCnt >= proto.MaxConcurrentTask {
			logutil.BgLogger().Info("scheduled tasks reached limit",
				zap.Int("current", taskCnt), zap.Int("max", proto.MaxConcurrentTask))
			continue
		}

		tasks, err := sm.taskMgr.GetTopUnfinishedTasks(sm.ctx)
		if err != nil {
			logutil.BgLogger().Warn("get unfinished tasks failed", zap.Error(err))
			continue
		}

		schedulableTasks := make([]*proto.Task, 0, len(tasks))
		for _, task := range tasks {
			if sm.hasScheduler(task.ID) {
				continue
			}
			// we check it before start scheduler, so no need to check it again.
			// see startScheduler.
			// this should not happen normally, unless user modify system table
			// directly.
			if getSchedulerFactory(task.Type) == nil {
				logutil.BgLogger().Warn("unknown task type", zap.Int64("task-id", task.ID),
					zap.Stringer("task-type", task.Type))
				sm.failTask(task.ID, task.State, errors.New("unknown task type"))
				continue
			}
			schedulableTasks = append(schedulableTasks, task)
		}
		if len(schedulableTasks) == 0 {
			continue
		}

		if err = sm.slotMgr.update(sm.ctx, sm.nodeMgr, sm.taskMgr); err != nil {
			logutil.BgLogger().Warn("update used slot failed", zap.Error(err))
			continue
		}
		for _, task := range schedulableTasks {
			taskCnt = sm.getSchedulerCount()
			if taskCnt >= proto.MaxConcurrentTask {
				break
			}
			reservedExecID, ok := sm.slotMgr.canReserve(task)
			if !ok {
				// task of lower priority might be able to be scheduled.
				continue
			}
			metrics.DistTaskGauge.WithLabelValues(task.Type.String(), metrics.SchedulingStatus).Inc()
			metrics.UpdateMetricsForDispatchTask(task.ID, task.Type)
			sm.startScheduler(task, reservedExecID)
		}
	}
}

func (sm *Manager) failTask(id int64, currState proto.TaskState, err error) {
	if err2 := sm.taskMgr.FailTask(sm.ctx, id, currState, err); err2 != nil {
		logutil.BgLogger().Warn("failed to update task state to failed",
			zap.Int64("task-id", id), zap.Error(err2))
	}
}

func (sm *Manager) gcSubtaskHistoryTableLoop() {
	historySubtaskTableGcInterval := defaultHistorySubtaskTableGcInterval
	failpoint.Inject("historySubtaskTableGcInterval", func(val failpoint.Value) {
		if seconds, ok := val.(int); ok {
			historySubtaskTableGcInterval = time.Second * time.Duration(seconds)
		}

		<-WaitTaskFinished
	})

	logutil.BgLogger().Info("subtask table gc loop start")
	ticker := time.NewTicker(historySubtaskTableGcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			logutil.BgLogger().Info("subtask history table gc loop exits", zap.Error(sm.ctx.Err()))
			return
		case <-ticker.C:
			err := sm.taskMgr.GCSubtasks(sm.ctx)
			if err != nil {
				logutil.BgLogger().Warn("subtask history table gc failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("subtask history table gc success")
			}
		}
	}
}

func (sm *Manager) startScheduler(basicTask *proto.Task, reservedExecID string) {
	task, err := sm.taskMgr.GetTaskByID(sm.ctx, basicTask.ID)
	if err != nil {
		logutil.BgLogger().Error("get task failed", zap.Int64("task-id", basicTask.ID), zap.Error(err))
		return
	}

	schedulerFactory := getSchedulerFactory(task.Type)
	scheduler := schedulerFactory(sm.ctx, task, Param{
		taskMgr: sm.taskMgr,
		nodeMgr: sm.nodeMgr,
		slotMgr: sm.slotMgr,
	})
	if err = scheduler.Init(); err != nil {
		logutil.BgLogger().Error("init scheduler failed", zap.Error(err))
		sm.failTask(task.ID, task.State, err)
		return
	}
	sm.addScheduler(task.ID, scheduler)
	sm.slotMgr.reserve(basicTask, reservedExecID)
	// Using the pool with block, so it wouldn't return an error.
	sm.schedulerWG.RunWithLog(func() {
		defer func() {
			scheduler.Close()
			sm.delScheduler(task.ID)
			sm.slotMgr.unReserve(basicTask, reservedExecID)
			handle.NotifyTaskChange()
		}()
		metrics.UpdateMetricsForRunTask(task)
		scheduler.ScheduleTask()
		logutil.BgLogger().Info("task finished", zap.Int64("task-id", task.ID))
		sm.finishCh <- struct{}{}
	})
}

func (sm *Manager) cleanupTaskLoop() {
	logutil.BgLogger().Info("cleanup loop start")
	ticker := time.NewTicker(DefaultCleanUpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			logutil.BgLogger().Info("cleanup loop exits", zap.Error(sm.ctx.Err()))
			return
		case <-sm.finishCh:
			sm.doCleanupTask()
		case <-ticker.C:
			sm.doCleanupTask()
		}
	}
}

// WaitCleanUpFinished is used to sync the test.
var WaitCleanUpFinished = make(chan struct{}, 1)

// doCleanupTask processes clean up routine defined by each type of tasks and cleanupMeta.
// For example:
//
//	tasks with global sort should clean up tmp files stored on S3.
func (sm *Manager) doCleanupTask() {
	tasks, err := sm.taskMgr.GetTasksInStates(
		sm.ctx,
		proto.TaskStateFailed,
		proto.TaskStateReverted,
		proto.TaskStateSucceed,
	)
	if err != nil {
		logutil.BgLogger().Warn("cleanup routine failed", zap.Error(err))
		return
	}
	if len(tasks) == 0 {
		return
	}
	logutil.BgLogger().Info("cleanup routine start")
	err = sm.cleanupFinishedTasks(tasks)
	if err != nil {
		logutil.BgLogger().Warn("cleanup routine failed", zap.Error(err))
		return
	}
	failpoint.Inject("WaitCleanUpFinished", func() {
		WaitCleanUpFinished <- struct{}{}
	})
	logutil.BgLogger().Info("cleanup routine success")
}

func (sm *Manager) cleanupFinishedTasks(tasks []*proto.Task) error {
	cleanedTasks := make([]*proto.Task, 0)
	var firstErr error
	for _, task := range tasks {
		logutil.BgLogger().Info("cleanup task", zap.Int64("task-id", task.ID))
		cleanupFactory := getSchedulerCleanUpFactory(task.Type)
		if cleanupFactory != nil {
			cleanup := cleanupFactory()
			err := cleanup.CleanUp(sm.ctx, task)
			if err != nil {
				firstErr = err
				break
			}
			cleanedTasks = append(cleanedTasks, task)
		} else {
			// if task doesn't register cleanup function, mark it as cleaned.
			cleanedTasks = append(cleanedTasks, task)
		}
	}
	if firstErr != nil {
		logutil.BgLogger().Warn("cleanup routine failed", zap.Error(errors.Trace(firstErr)))
	}

	failpoint.Inject("mockTransferErr", func() {
		failpoint.Return(errors.New("transfer err"))
	})

	return sm.taskMgr.TransferTasks2History(sm.ctx, cleanedTasks)
}

// MockScheduler mock one scheduler for one task, only used for tests.
func (sm *Manager) MockScheduler(task *proto.Task) *BaseScheduler {
	return NewBaseScheduler(sm.ctx, task, Param{
		taskMgr: sm.taskMgr,
		nodeMgr: sm.nodeMgr,
		slotMgr: sm.slotMgr,
	})
}
