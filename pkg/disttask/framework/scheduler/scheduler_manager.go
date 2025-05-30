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
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"go.uber.org/zap"
)

var (
	// CheckTaskRunningInterval is the interval for loading tasks.
	// It is exported for testing.
	CheckTaskRunningInterval = 3 * time.Second
	// defaultHistorySubtaskTableGcInterval is the interval of gc history subtask table.
	defaultHistorySubtaskTableGcInterval = 24 * time.Hour
	// DefaultCleanUpInterval is the interval of cleanup routine.
	DefaultCleanUpInterval        = 10 * time.Minute
	defaultCollectMetricsInterval = 5 * time.Second
)

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
		return i.GetTask().CompareTask(j.GetTask())
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
			sm.mu.schedulers = slices.Delete(sm.mu.schedulers, i, i+1)
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
	logger   *zap.Logger

	finishCh chan struct{}

	mu struct {
		syncutil.RWMutex
		schedulerMap map[int64]Scheduler
		// in task order
		schedulers []Scheduler
	}
	nodeRes *proto.NodeResource
}

// NewManager creates a scheduler struct.
func NewManager(ctx context.Context, taskMgr TaskManager, serverID string, nodeRes *proto.NodeResource) *Manager {
	logger := logutil.ErrVerboseLogger()
	if intest.InTest {
		logger = logger.With(zap.String("server-id", serverID))
	}
	subCtx, cancel := context.WithCancel(ctx)
	slotMgr := newSlotManager()
	nodeMgr := newNodeManager(serverID)
	schedulerManager := &Manager{
		ctx:      subCtx,
		cancel:   cancel,
		taskMgr:  taskMgr,
		serverID: serverID,
		slotMgr:  slotMgr,
		nodeMgr:  nodeMgr,
		balancer: newBalancer(Param{
			taskMgr:  taskMgr,
			nodeMgr:  nodeMgr,
			slotMgr:  slotMgr,
			serverID: serverID,
		}),
		logger:   logger,
		finishCh: make(chan struct{}, proto.MaxConcurrentTask),
		nodeRes:  nodeRes,
	}
	schedulerManager.mu.schedulerMap = make(map[int64]Scheduler)

	return schedulerManager
}

// Start the schedulerManager, start the scheduleTaskLoop to start multiple schedulers.
func (sm *Manager) Start() {
	// init cached managed nodes
	sm.nodeMgr.refreshNodes(sm.ctx, sm.taskMgr, sm.slotMgr)

	sm.wg.Run(sm.scheduleTaskLoop)
	sm.wg.Run(sm.gcSubtaskHistoryTableLoop)
	sm.wg.Run(sm.cleanupTaskLoop)
	sm.wg.Run(sm.collectLoop)
	sm.wg.Run(func() {
		sm.nodeMgr.maintainLiveNodesLoop(sm.ctx, sm.taskMgr)
	})
	sm.wg.Run(func() {
		sm.nodeMgr.refreshNodesLoop(sm.ctx, sm.taskMgr, sm.slotMgr)
	})
	sm.wg.Run(func() {
		sm.balancer.balanceLoop(sm.ctx, sm)
	})
	sm.initialized = true
}

// Cancel cancels the scheduler manager.
// used in test to simulate tidb node shutdown.
func (sm *Manager) Cancel() {
	sm.cancel()
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
	sm.logger.Info("schedule task loop start")
	ticker := time.NewTicker(CheckTaskRunningInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			sm.logger.Info("schedule task loop exits")
			return
		case <-ticker.C:
		case <-handle.TaskChangedCh:
		}

		taskCnt := sm.getSchedulerCount()
		if taskCnt >= proto.MaxConcurrentTask {
			sm.logger.Debug("scheduled tasks reached limit",
				zap.Int("current", taskCnt), zap.Int("max", proto.MaxConcurrentTask))
			continue
		}

		failpoint.InjectCall("beforeGetSchedulableTasks")
		schedulableTasks, err := sm.getSchedulableTasks()
		if err != nil {
			continue
		}

		err = sm.startSchedulers(schedulableTasks)
		if err != nil {
			continue
		}
	}
}

func (sm *Manager) getSchedulableTasks() ([]*proto.TaskBase, error) {
	tasks, err := sm.taskMgr.GetTopUnfinishedTasks(sm.ctx)
	if err != nil {
		sm.logger.Warn("get unfinished tasks failed", zap.Error(err))
		return nil, err
	}

	schedulableTasks := make([]*proto.TaskBase, 0, len(tasks))
	for _, task := range tasks {
		if sm.hasScheduler(task.ID) {
			continue
		}
		// we check it before start scheduler, so no need to check it again.
		// see startScheduler.
		// this should not happen normally, unless user modify system table
		// directly.
		if getSchedulerFactory(task.Type) == nil {
			sm.logger.Warn("unknown task type", zap.Int64("task-id", task.ID),
				zap.Stringer("task-type", task.Type))
			sm.failTask(task.ID, task.State, errors.New("unknown task type"))
			continue
		}
		schedulableTasks = append(schedulableTasks, task)
	}
	return schedulableTasks, nil
}

func (sm *Manager) startSchedulers(schedulableTasks []*proto.TaskBase) error {
	if len(schedulableTasks) == 0 {
		return nil
	}
	if err := sm.slotMgr.update(sm.ctx, sm.nodeMgr, sm.taskMgr); err != nil {
		sm.logger.Warn("update used slot failed", zap.Error(err))
		return err
	}
	for _, task := range schedulableTasks {
		taskCnt := sm.getSchedulerCount()
		if taskCnt >= proto.MaxConcurrentTask {
			break
		}
		var reservedExecID string
		allocateSlots := true
		var ok bool
		switch task.State {
		case proto.TaskStatePending, proto.TaskStateRunning, proto.TaskStateResuming:
			reservedExecID, ok = sm.slotMgr.canReserve(task)
			if !ok {
				// task of low ranking might be able to be scheduled.
				continue
			}
		// reverting/cancelling/pausing/modifying, we don't allocate slots for them.
		default:
			allocateSlots = false
			sm.logger.Info("start scheduler without allocating slots",
				zap.Int64("task-id", task.ID), zap.Stringer("state", task.State))
		}
		sm.startScheduler(task, allocateSlots, reservedExecID)
	}
	return nil
}

func (sm *Manager) failTask(id int64, currState proto.TaskState, err error) {
	if err2 := sm.taskMgr.FailTask(sm.ctx, id, currState, err); err2 != nil {
		sm.logger.Warn("failed to update task state to failed",
			zap.Int64("task-id", id), zap.Error(err2))
	}
}

func (sm *Manager) gcSubtaskHistoryTableLoop() {
	historySubtaskTableGcInterval := defaultHistorySubtaskTableGcInterval
	failpoint.InjectCall("historySubtaskTableGcInterval", &historySubtaskTableGcInterval)

	sm.logger.Info("subtask table gc loop start")
	ticker := time.NewTicker(historySubtaskTableGcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			sm.logger.Info("subtask history table gc loop exits")
			return
		case <-ticker.C:
			err := sm.taskMgr.GCSubtasks(sm.ctx)
			if err != nil {
				sm.logger.Warn("subtask history table gc failed", zap.Error(err))
			} else {
				sm.logger.Info("subtask history table gc success")
			}
		}
	}
}

func (sm *Manager) startScheduler(basicTask *proto.TaskBase, allocateSlots bool, reservedExecID string) {
	task, err := sm.taskMgr.GetTaskByID(sm.ctx, basicTask.ID)
	if err != nil {
		sm.logger.Error("get task failed", zap.Int64("task-id", basicTask.ID), zap.Error(err))
		return
	}

	schedulerFactory := getSchedulerFactory(task.Type)
	scheduler := schedulerFactory(sm.ctx, task, Param{
		taskMgr:        sm.taskMgr,
		nodeMgr:        sm.nodeMgr,
		slotMgr:        sm.slotMgr,
		serverID:       sm.serverID,
		allocatedSlots: allocateSlots,
		nodeRes:        sm.nodeRes,
	})
	if err = scheduler.Init(); err != nil {
		sm.logger.Error("init scheduler failed", zap.Error(err))
		sm.failTask(task.ID, task.State, err)
		return
	}
	sm.addScheduler(task.ID, scheduler)
	if allocateSlots {
		sm.slotMgr.reserve(basicTask, reservedExecID)
	}
	sm.logger.Info("task scheduler started", zap.Int64("task-id", task.ID))
	sm.schedulerWG.RunWithLog(func() {
		defer func() {
			scheduler.Close()
			sm.delScheduler(task.ID)
			if allocateSlots {
				sm.slotMgr.unReserve(basicTask, reservedExecID)
			}
			handle.NotifyTaskChange()
			sm.logger.Info("task scheduler exit", zap.Int64("task-id", task.ID))
		}()
		scheduler.ScheduleTask()
		select {
		case sm.finishCh <- struct{}{}:
		default:
		}
	})
}

func (sm *Manager) cleanupTaskLoop() {
	sm.logger.Info("cleanup loop start")
	ticker := time.NewTicker(DefaultCleanUpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			sm.logger.Info("cleanup loop exits")
			return
		case <-sm.finishCh:
			sm.doCleanupTask()
		case <-ticker.C:
			sm.doCleanupTask()
		}
	}
}

// doCleanupTask processes clean up routine defined by each type of tasks and cleanupMeta.
// For example:
//
//	tasks with global sort should clean up tmp files stored on S3.
func (sm *Manager) doCleanupTask() {
	failpoint.InjectCall("doCleanupTask")
	tasks, err := sm.taskMgr.GetTasksInStates(
		sm.ctx,
		proto.TaskStateFailed,
		proto.TaskStateReverted,
		proto.TaskStateSucceed,
	)
	if err != nil {
		sm.logger.Warn("get task in states failed", zap.Error(err))
		return
	}
	if len(tasks) == 0 {
		return
	}
	sm.logger.Info("cleanup routine start")
	err = sm.cleanupFinishedTasks(tasks)
	if err != nil {
		sm.logger.Warn("cleanup routine failed", zap.Error(err))
		return
	}
	failpoint.InjectCall("WaitCleanUpFinished")
	sm.logger.Info("cleanup routine success")
}

func (sm *Manager) cleanupFinishedTasks(tasks []*proto.Task) error {
	cleanedTasks := make([]*proto.Task, 0)
	var firstErr error
	for _, task := range tasks {
		sm.logger.Info("cleanup task", zap.Int64("task-id", task.ID))
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
		sm.logger.Warn("cleanup routine failed", zap.Error(errors.Trace(firstErr)))
	}

	failpoint.Inject("mockTransferErr", func() {
		failpoint.Return(errors.New("transfer err"))
	})

	return sm.taskMgr.TransferTasks2History(sm.ctx, cleanedTasks)
}

func (sm *Manager) collectLoop() {
	sm.logger.Info("collect loop start")
	ticker := time.NewTicker(defaultCollectMetricsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			sm.logger.Info("collect loop exits")
			return
		case <-ticker.C:
			sm.collect()
		}
	}
}

func (sm *Manager) collect() {
	tasks, err := sm.taskMgr.GetAllTasks(sm.ctx)
	if err != nil {
		sm.logger.Warn("get all tasks failed", zap.Error(err))
	}
	subtasks, err := sm.taskMgr.GetAllSubtasks(sm.ctx)
	if err != nil {
		sm.logger.Warn("get all subtasks failed", zap.Error(err))
		return
	}
	disttaskCollector.taskInfo.Store(&tasks)
	disttaskCollector.subtaskInfo.Store(&subtasks)
}

// MockScheduler mock one scheduler for one task, only used for tests.
func (sm *Manager) MockScheduler(task *proto.Task) *BaseScheduler {
	return NewBaseScheduler(sm.ctx, task, Param{
		taskMgr:  sm.taskMgr,
		nodeMgr:  sm.nodeMgr,
		slotMgr:  sm.slotMgr,
		serverID: sm.serverID,
	})
}
