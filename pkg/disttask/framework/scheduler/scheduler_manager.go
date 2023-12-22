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

func (sm *Manager) getSchedulerCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.mu.schedulers)
}

func (sm *Manager) addScheduler(taskID int64, scheduler Scheduler) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.schedulers[taskID] = scheduler
}

func (sm *Manager) hasScheduler(taskID int64) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.mu.schedulers[taskID]
	return ok
}

func (sm *Manager) delScheduler(taskID int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.mu.schedulers, taskID)
}

func (sm *Manager) clearSchedulers() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.schedulers = make(map[int64]Scheduler)
}

// Manager manage a bunch of schedulers.
// Scheduler schedule and monitor tasks.
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
		schedulers map[int64]Scheduler
	}
}

// NewManager creates a scheduler struct.
func NewManager(ctx context.Context, taskMgr TaskManager, serverID string) (*Manager, error) {
	schedulerManager := &Manager{
		taskMgr:  taskMgr,
		serverID: serverID,
		slotMgr:  newSlotManager(),
	}
	gPool, err := spool.NewPool("schedule_pool", int32(proto.MaxConcurrentTask), util.DistTask, spool.WithBlocking(true))
	if err != nil {
		return nil, err
	}
	schedulerManager.gPool = gPool
	schedulerManager.ctx, schedulerManager.cancel = context.WithCancel(ctx)
	schedulerManager.mu.schedulers = make(map[int64]Scheduler)
	schedulerManager.finishCh = make(chan struct{}, proto.MaxConcurrentTask)

	return schedulerManager, nil
}

// Start the schedulerManager, start the scheduleTaskLoop to start multiple schedulers.
func (sm *Manager) Start() {
	failpoint.Inject("disableSchedulerManager", func() {
		failpoint.Return()
	})
	sm.wg.Run(sm.scheduleTaskLoop)
	sm.wg.Run(sm.gcSubtaskHistoryTableLoop)
	sm.wg.Run(sm.cleanUpLoop)
	sm.initialized = true
}

// Stop the schedulerManager.
func (sm *Manager) Stop() {
	sm.cancel()
	sm.gPool.ReleaseAndWait()
	sm.wg.Wait()
	sm.clearSchedulers()
	sm.initialized = false
	close(sm.finishCh)
}

// Initialized check the manager initialized.
func (sm *Manager) Initialized() bool {
	return sm.initialized
}

// scheduleTaskLoop schedulees the tasks.
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

		scheduleableTasks := make([]*proto.Task, 0, len(tasks))
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
			scheduleableTasks = append(scheduleableTasks, task)
		}
		if len(scheduleableTasks) == 0 {
			continue
		}

		if err = sm.slotMgr.update(sm.ctx, sm.taskMgr); err != nil {
			logutil.BgLogger().Warn("update used slot failed", zap.Error(err))
			continue
		}
		for _, task := range scheduleableTasks {
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

	logutil.Logger(sm.ctx).Info("subtask table gc loop start")
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
				logutil.Logger(sm.ctx).Info("subtask history table gc success")
			}
		}
	}
}

func (sm *Manager) startScheduler(basicTask *proto.Task, reservedExecID string) {
	task, err := sm.taskMgr.GetTaskByID(sm.ctx, basicTask.ID)
	if err != nil {
		logutil.BgLogger().Error("get task failed", zap.Error(err))
		return
	}

	schedulerFactory := getSchedulerFactory(task.Type)
	scheduler := schedulerFactory(sm.ctx, sm.taskMgr, sm.serverID, task)
	if err = scheduler.Init(); err != nil {
		logutil.BgLogger().Error("init scheduler failed", zap.Error(err))
		sm.failTask(task.ID, task.State, err)
		return
	}
	sm.addScheduler(task.ID, scheduler)
	sm.slotMgr.reserve(basicTask, reservedExecID)
	// Using the pool with block, so it wouldn't return an error.
	_ = sm.gPool.Run(func() {
		defer func() {
			scheduler.Close()
			sm.delScheduler(task.ID)
			sm.slotMgr.unReserve(basicTask, reservedExecID)
		}()
		metrics.UpdateMetricsForRunTask(task)
		scheduler.ExecuteTask()
		logutil.BgLogger().Info("task finished", zap.Int64("task-id", task.ID))
		sm.finishCh <- struct{}{}
	})
}

func (sm *Manager) cleanUpLoop() {
	logutil.Logger(sm.ctx).Info("cleanUp loop start")
	ticker := time.NewTicker(defaultCleanUpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			logutil.BgLogger().Info("cleanUp loop exits", zap.Error(sm.ctx.Err()))
			return
		case <-sm.finishCh:
			sm.doCleanUpRoutine()
		case <-ticker.C:
			sm.doCleanUpRoutine()
		}
	}
}

// WaitCleanUpFinished is used to sync the test.
var WaitCleanUpFinished = make(chan struct{})

// doCleanUpRoutine processes clean up routine defined by each type of tasks and cleanUpMeta.
// For example:
//
//	tasks with global sort should clean up tmp files stored on S3.
func (sm *Manager) doCleanUpRoutine() {
	cnt := sm.CleanUpMeta()
	if cnt != 0 {
		logutil.BgLogger().Info("clean up nodes in framework meta since nodes shutdown", zap.Int("cnt", cnt))
	}
	tasks, err := sm.taskMgr.GetTasksInStates(
		sm.ctx,
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
	logutil.Logger(sm.ctx).Info("cleanUp routine start")
	err = sm.cleanUpFinishedTasks(tasks)
	if err != nil {
		logutil.BgLogger().Warn("cleanUp routine failed", zap.Error(err))
		return
	}
	failpoint.Inject("WaitCleanUpFinished", func() {
		WaitCleanUpFinished <- struct{}{}
	})
	logutil.Logger(sm.ctx).Info("cleanUp routine success")
}

// CleanUpMeta clean up old node info in dist_framework_meta table.
func (sm *Manager) CleanUpMeta() int {
	// Safe to discard errors since this function can be called at regular intervals.
	serverInfos, err := GenerateTaskExecutorNodes(sm.ctx)
	if err != nil {
		logutil.BgLogger().Warn("generate task executor nodes met error")
		return 0
	}

	oldNodes, err := sm.taskMgr.GetAllNodes(sm.ctx)
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
	err = sm.taskMgr.CleanUpMeta(sm.ctx, cleanNodes)
	if err != nil {
		logutil.BgLogger().Warn("clean up dist_framework_meta met error")
		return 0
	}
	return len(cleanNodes)
}

func (sm *Manager) cleanUpFinishedTasks(tasks []*proto.Task) error {
	cleanedTasks := make([]*proto.Task, 0)
	var firstErr error
	for _, task := range tasks {
		cleanUpFactory := getSchedulerCleanUpFactory(task.Type)
		if cleanUpFactory != nil {
			cleanUp := cleanUpFactory()
			err := cleanUp.CleanUp(sm.ctx, task)
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

	return sm.taskMgr.TransferTasks2History(sm.ctx, cleanedTasks)
}

// MockScheduler mock one scheduler for one task, only used for tests.
func (sm *Manager) MockScheduler(task *proto.Task) *BaseScheduler {
	return NewBaseScheduler(sm.ctx, sm.taskMgr, sm.serverID, task)
}
