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

package taskexecutor

import (
	"container/heap"
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	executorPoolSize int32 = 4
	// same as dispatcher
	checkTime           = 300 * time.Millisecond
	recoverMetaInterval = 90 * time.Second
	retrySQLTimes       = 30
	retrySQLInterval    = 500 * time.Millisecond

	// for test
	onRunnableTasksTick = make(chan struct{})
	onRunnableTaskTick  = make(chan struct{})
)

// ManagerBuilder is used to build a Manager.
type ManagerBuilder struct {
	newPool func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error)
}

// NewManagerBuilder creates a new ManagerBuilder.
func NewManagerBuilder() *ManagerBuilder {
	return &ManagerBuilder{
		newPool: func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
			return spool.NewPool(name, size, component, options...)
		},
	}
}

// setPoolFactory sets the poolFactory to mock the Pool in unit test.
func (b *ManagerBuilder) setPoolFactory(poolFactory func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error)) {
	b.newPool = poolFactory
}

// Manager monitors the task table and manages the taskExecutors.
type Manager struct {
	taskTable    TaskTable
	executorPool Pool
	mu           struct {
		sync.RWMutex
		// taskID -> CancelCauseFunc.
		// CancelCauseFunc is used to fast cancel the executor.Run.
		handlingTasks map[int64]context.CancelCauseFunc
	}
	// id, it's the same as server id now, i.e. host:port.
	id          string
	wg          tidbutil.WaitGroupWrapper
	ctx         context.Context
	cancel      context.CancelFunc
	logCtx      context.Context
	newPool     func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error)
	slotManager *slotManager
}

// BuildManager builds a Manager.
func (b *ManagerBuilder) BuildManager(ctx context.Context, id string, taskTable TaskTable) (*Manager, error) {
	m := &Manager{
		id:        id,
		taskTable: taskTable,
		logCtx:    logutil.WithFields(context.Background()),
		newPool:   b.newPool,
		slotManager: &slotManager{
			schedulerSlotInfos: make(map[int64]*slotInfo, 0),
			available:          runtime.NumCPU(),
		},
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.mu.handlingTasks = make(map[int64]context.CancelCauseFunc)

	executorPool, err := m.newPool("executor_pool", executorPoolSize, util.DistTask)
	if err != nil {
		return nil, err
	}
	m.executorPool = executorPool

	return m, nil
}

func (m *Manager) initMeta() (err error) {
	for i := 0; i < retrySQLTimes; i++ {
		err = m.taskTable.StartManager(m.ctx, m.id, config.GetGlobalConfig().Instance.TiDBServiceScope)
		if err == nil {
			break
		}
		if i%10 == 0 {
			logutil.Logger(m.logCtx).Warn("start manager failed",
				zap.String("scope", config.GetGlobalConfig().Instance.TiDBServiceScope),
				zap.Int("retry times", i),
				zap.Error(err))
		}
		time.Sleep(retrySQLInterval)
	}
	return err
}

// Start starts the Manager.
func (m *Manager) Start() error {
	logutil.Logger(m.logCtx).Debug("manager start")
	if err := m.initMeta(); err != nil {
		return err
	}

	m.wg.Run(m.fetchAndHandleRunnableTasksLoop)
	m.wg.Run(m.fetchAndFastCancelTasksLoop)
	m.wg.Run(m.recoverMetaLoop)
	return nil
}

// Stop stops the Manager.
func (m *Manager) Stop() {
	m.cancel()
	m.executorPool.ReleaseAndWait()
	m.wg.Wait()
}

// fetchAndHandleRunnableTasks fetches the runnable tasks from the task table and handles them.
func (m *Manager) fetchAndHandleRunnableTasksLoop() {
	defer tidbutil.Recover(metrics.LabelDomain, "fetchAndHandleRunnableTasksLoop", m.fetchAndHandleRunnableTasksLoop, false)
	ticker := time.NewTicker(checkTime)
	for {
		select {
		case <-m.ctx.Done():
			logutil.Logger(m.logCtx).Info("fetchAndHandleRunnableTasksLoop done")
			return
		case <-ticker.C:
			tasks, err := m.taskTable.GetTasksInStates(m.ctx, proto.TaskStateRunning, proto.TaskStateReverting)
			if err != nil {
				m.logErr(err)
				continue
			}
			logutil.Logger(m.logCtx).Info("fetchAndHandleRunnableTasksLoop", zap.Any("tasks", tasks))
			m.onRunnableTasks(tasks)
		}
	}
}

// fetchAndFastCancelTasks fetches the reverting/pausing tasks from the task table and fast cancels them.
func (m *Manager) fetchAndFastCancelTasksLoop() {
	defer tidbutil.Recover(metrics.LabelDomain, "fetchAndFastCancelTasksLoop", m.fetchAndFastCancelTasksLoop, false)

	ticker := time.NewTicker(checkTime)
	for {
		select {
		case <-m.ctx.Done():
			m.cancelAllRunningTasks()
			logutil.Logger(m.logCtx).Info("fetchAndFastCancelTasksLoop done")
			return
		case <-ticker.C:
			tasks, err := m.taskTable.GetTasksInStates(m.ctx, proto.TaskStateReverting)
			if err != nil {
				m.logErr(err)
				continue
			}
			m.onCanceledTasks(m.ctx, tasks)

			// cancel pending/running subtasks, and mark them as paused.
			pausingTasks, err := m.taskTable.GetTasksInStates(m.ctx, proto.TaskStatePausing)
			if err != nil {
				m.logErr(err)
				continue
			}
			if err := m.onPausingTasks(pausingTasks); err != nil {
				m.logErr(err)
				continue
			}
		}
	}
}

// onRunnableTasks handles runnable tasks.
func (m *Manager) onRunnableTasks(tasks []*proto.Task) {
	if len(tasks) == 0 {
		return
	}
	tasks = m.filterAlreadyHandlingTasks(tasks)

	priorityQueue := make(proto.TaskPriorityQueue, 0)
	heap.Init(&priorityQueue)
	for _, task := range tasks {
		heap.Push(&priorityQueue, proto.WrapPriorityQueue(task))
	}
	for priorityQueue.Len() > 0 {
		task := heap.Pop(&priorityQueue).(*proto.TaskWrapper).Task
		logutil.Logger(m.logCtx).Info("get new subtask", zap.Int64("task-id", task.ID))
		exist, err := m.taskTable.HasSubtasksInStates(m.ctx, m.id, task.ID, task.Step,
			proto.TaskStatePending, proto.TaskStateRevertPending,
			// for the case that the tidb is restarted when the subtask is running.
			proto.TaskStateRunning, proto.TaskStateReverting)
		if err != nil {
			logutil.Logger(m.logCtx).Error("check subtask exist failed", zap.Error(err))
			m.logErr(err)
			continue
		}
		if !exist {
			continue
		}
		logutil.Logger(m.logCtx).Info("detect new subtask", zap.Int64("task-id", task.ID))

		if !m.slotManager.checkSlotAvailabilityForTask(task) {
			failpoint.Inject("taskTick", func() {
				<-onRunnableTasksTick
			})
			continue
		}
		m.addHandlingTask(task.ID)
		t := task
		err = m.executorPool.Run(func() {
			m.slotManager.addTask(t)
			defer m.slotManager.removeTask(t.ID)
			m.onRunnableTask(t)
			m.removeHandlingTask(t.ID)
		})
		// pool closed.
		if err != nil {
			m.slotManager.removeTask(t.ID)
			m.removeHandlingTask(task.ID)
			m.logErr(err)
			return
		}

		failpoint.Inject("taskTick", func() {
			<-onRunnableTasksTick
		})
	}
}

// onCanceledTasks cancels the running subtasks.
func (m *Manager) onCanceledTasks(_ context.Context, tasks []*proto.Task) {
	if len(tasks) == 0 {
		return
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, task := range tasks {
		if cancel, ok := m.mu.handlingTasks[task.ID]; ok && cancel != nil {
			logutil.Logger(m.logCtx).Info("onCanceledTasks", zap.Int64("task-id", task.ID))
			// subtask needs to change its state to canceled.
			cancel(ErrCancelSubtask)
		}
	}
}

// onPausingTasks pauses/cancels the pending/running subtasks.
func (m *Manager) onPausingTasks(tasks []*proto.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, task := range tasks {
		logutil.Logger(m.logCtx).Info("onPausingTasks", zap.Any("task_id", task.ID))
		if cancel, ok := m.mu.handlingTasks[task.ID]; ok && cancel != nil {
			// Pause all running subtasks, don't mark subtasks as canceled.
			// Should not change the subtask's state.
			cancel(nil)
		}
		if err := m.taskTable.PauseSubtasks(m.ctx, m.id, task.ID); err != nil {
			return err
		}
	}
	return nil
}

// recoverMetaLoop inits and recovers dist_framework_meta for the tidb node running the taskExecutor manager.
// This is necessary when the TiDB node experiences a prolonged network partition
// and the dispatcher deletes `dist_framework_meta`.
// When the TiDB node recovers from the network partition,
// we need to re-insert the metadata.
func (m *Manager) recoverMetaLoop() {
	defer tidbutil.Recover(metrics.LabelDomain, "recoverMetaLoop", m.recoverMetaLoop, false)
	ticker := time.NewTicker(recoverMetaInterval)
	for {
		select {
		case <-m.ctx.Done():
			logutil.Logger(m.logCtx).Info("recoverMetaLoop done")
			return
		case <-ticker.C:
			if err := m.initMeta(); err != nil {
				m.logErr(err)
				continue
			}
		}
	}
}

// cancelAllRunningTasks cancels all running tasks.
func (m *Manager) cancelAllRunningTasks() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, cancel := range m.mu.handlingTasks {
		logutil.Logger(m.logCtx).Info("cancelAllRunningTasks", zap.Int64("task-id", id))
		if cancel != nil {
			// tidb shutdown, don't mark subtask as canceled.
			// Should not change the subtask's state.
			cancel(nil)
		}
	}
}

// filterAlreadyHandlingTasks filters the tasks that are already handled.
func (m *Manager) filterAlreadyHandlingTasks(tasks []*proto.Task) []*proto.Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var i int
	for _, task := range tasks {
		if _, ok := m.mu.handlingTasks[task.ID]; !ok {
			tasks[i] = task
			i++
		}
	}
	return tasks[:i]
}

// TestContext only used in tests.
type TestContext struct {
	TestSyncSubtaskRun chan struct{}
	mockDown           atomic.Bool
}

var testContexts sync.Map

// onRunnableTask handles a runnable task.
func (m *Manager) onRunnableTask(task *proto.Task) {
	logutil.Logger(m.logCtx).Info("onRunnableTask", zap.Int64("task-id", task.ID), zap.Stringer("type", task.Type))
	// runCtx only used in executor.Run, cancel in m.fetchAndFastCancelTasks.
	factory := GetTaskExecutorFactory(task.Type)
	if factory == nil {
		err := errors.Errorf("task type %s not found", task.Type)
		m.logErrAndPersist(err, task.ID, nil)
		return
	}
	executor := factory(m.ctx, m.id, task, m.taskTable)
	taskCtx, taskCancel := context.WithCancelCause(m.ctx)
	m.registerCancelFunc(task.ID, taskCancel)
	defer taskCancel(nil)
	// executor should init before run()/pause()/rollback().
	err := executor.Init(taskCtx)
	if err != nil {
		m.logErrAndPersist(err, task.ID, executor)
		return
	}
	defer executor.Close()
	for {
		select {
		case <-m.ctx.Done():
			logutil.Logger(m.logCtx).Info("onRunnableTask exit for cancel", zap.Int64("task-id", task.ID), zap.Stringer("type", task.Type))
			return
		case <-time.After(checkTime):
		}
		failpoint.Inject("mockStopManager", func() {
			testContexts.Store(m.id, &TestContext{make(chan struct{}), atomic.Bool{}})
			go func() {
				v, ok := testContexts.Load(m.id)
				if ok {
					<-v.(*TestContext).TestSyncSubtaskRun
					_ = infosync.MockGlobalServerInfoManagerEntry.DeleteByID(m.id)
					m.Stop()
				}
			}()
		})
		task, err := m.taskTable.GetTaskByID(m.ctx, task.ID)
		if err != nil {
			m.logErr(err)
			return
		}
		if task == nil {
			return
		}
		if task.State != proto.TaskStateRunning && task.State != proto.TaskStateReverting {
			logutil.Logger(m.logCtx).Info("onRunnableTask exit",
				zap.Int64("task-id", task.ID), zap.Int64("step", int64(task.Step)), zap.Stringer("state", task.State))
			return
		}
		if exist, err := m.taskTable.HasSubtasksInStates(
			m.ctx,
			m.id, task.ID, task.Step,
			proto.TaskStatePending, proto.TaskStateRevertPending,
			// for the case that the tidb is restarted when the subtask is running.
			proto.TaskStateRunning, proto.TaskStateReverting); err != nil {
			m.logErr(err)
			return
		} else if !exist {
			continue
		}
		switch task.State {
		case proto.TaskStateRunning:
			// use taskCtx for canceling.
			err = executor.Run(taskCtx, task)
		case proto.TaskStatePausing:
			// use m.ctx since this process should not be canceled.
			err = executor.Pause(m.ctx, task)
		case proto.TaskStateReverting:
			// use m.ctx since this process should not be canceled.
			err = executor.Rollback(m.ctx, task)
		}
		if err != nil {
			logutil.Logger(m.logCtx).Error("failed to handle task", zap.Error(err))
		}

		failpoint.Inject("taskTick", func() {
			<-onRunnableTaskTick
		})
	}
}

// addHandlingTask adds a task to the handling task set.
func (m *Manager) addHandlingTask(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.handlingTasks[id] = nil
}

// registerCancelFunc registers a cancel function for a task.
func (m *Manager) registerCancelFunc(id int64, cancel context.CancelCauseFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.handlingTasks[id] = cancel
}

// removeHandlingTask removes a task from the handling task set.
func (m *Manager) removeHandlingTask(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mu.handlingTasks, id)
}

func (m *Manager) logErr(err error) {
	logutil.Logger(m.logCtx).Error("task manager met error", zap.Error(err), zap.Stack("stack"))
}

func (m *Manager) logErrAndPersist(err error, taskID int64, taskExecutor TaskExecutor) {
	m.logErr(err)
	if taskExecutor.IsRetryableError(err) {
		logutil.Logger(m.logCtx).Error("met retryable err", zap.Error(err), zap.Stack("stack"))
		return
	}
	err1 := m.taskTable.UpdateErrorToSubtask(m.ctx, m.id, taskID, err)
	if err1 != nil {
		logutil.Logger(m.logCtx).Error("update to subtask failed", zap.Error(err1), zap.Stack("stack"))
	}
	logutil.Logger(m.logCtx).Error("update error to subtask", zap.Int64("task-id", taskID), zap.Error(err1), zap.Stack("stack"))
}
