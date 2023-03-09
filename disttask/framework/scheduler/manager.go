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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	schedulerPoolSize       = 4
	subtaskExecutorPoolSize = 10
	checkTime               = time.Second
)

var (
	newPool      = NewMockPool
	newScheduler = NewInternalScheduler
)

// Manager monitors the global task table and manages the schedulers.
type Manager struct {
	globalTaskTable TaskTable
	subtaskTable    SubtaskTable
	schedulerPool   Pool
	// taskType -> subtaskExecutorPool
	subtaskExecutorPools map[string]Pool
	mu                   struct {
		sync.Mutex
		// taskID -> cancelFunc
		// cancelFunc is used to fast cancel the scheduler.Run
		handlingTasks map[int64]context.CancelFunc
	}
	id     string
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	logCtx context.Context
}

// NewManager creates a new Manager.
func NewManager(ctx context.Context, id string, globalTaskTable TaskTable, subtaskTable SubtaskTable) *Manager {
	m := &Manager{
		id:                   id,
		globalTaskTable:      globalTaskTable,
		subtaskTable:         subtaskTable,
		schedulerPool:        newPool(schedulerPoolSize),
		subtaskExecutorPools: make(map[string]Pool),
		logCtx:               logutil.WithKeyValue(context.Background(), "dist_task_manager", id),
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.mu.handlingTasks = make(map[int64]context.CancelFunc)
	for taskType := range subtaskExecutorConstructors {
		poolSize := subtaskExecutorPoolSize
		if opt, ok := subtaskExecutorOptions[taskType]; ok && opt.PoolSize > 0 {
			poolSize = opt.PoolSize
		}
		m.subtaskExecutorPools[taskType] = newPool(poolSize)
	}
	return m
}

// Start starts the Manager.
func (m *Manager) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.fetchAndHandleRunnableTasks(m.ctx)
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.fetchAndFastCancelTasks(m.ctx)
	}()
}

// Stop stops the Manager.
func (m *Manager) Stop() {
	m.cancel()
	for _, pool := range m.subtaskExecutorPools {
		pool.ReleaseAndWait()
	}
	m.schedulerPool.ReleaseAndWait()
	m.wg.Wait()
}

// fetchAndHandleRunnableTasks fetches the runnable tasks from the global task table and handles them.
func (m *Manager) fetchAndHandleRunnableTasks(ctx context.Context) {
	ticker := time.NewTicker(checkTime)
	for {
		select {
		case <-ctx.Done():
			logutil.Logger(m.logCtx).Info("fetchAndHandleRunnableTasks done")
			return
		case <-ticker.C:
			tasks, err := m.globalTaskTable.GetTasksInStates(proto.TaskStateRunning, proto.TaskStateReverting)
			if err != nil {
				m.onError(err)
				continue
			}
			m.onRunnableTasks(ctx, tasks)
		}
	}
}

// fetchAndFastCancelTasks fetches the reverting tasks from the global task table and fast cancels them.
func (m *Manager) fetchAndFastCancelTasks(ctx context.Context) {
	ticker := time.NewTicker(checkTime)
	for {
		select {
		case <-ctx.Done():
			m.cancelAllRunningTasks()
			logutil.Logger(m.logCtx).Info("fetchAndFastCancelTasks done")
			return
		case <-ticker.C:
			tasks, err := m.globalTaskTable.GetTasksInStates(proto.TaskStateReverting)
			if err != nil {
				m.onError(err)
				continue
			}
			m.onCanceledTasks(ctx, tasks)
		}
	}
}

// onRunnableTask handles runnable tasks.
func (m *Manager) onRunnableTasks(ctx context.Context, tasks []*proto.Task) {
	tasks = m.filterAlreadyHandlingTasks(tasks)
	for _, task := range tasks {
		logutil.Logger(m.logCtx).Info("onRunnableTasks", zap.Any("task", task))
		if _, ok := m.subtaskExecutorPools[task.Type]; !ok {
			logutil.Logger(m.logCtx).Error("unknown task type", zap.String("type", task.Type))
			continue
		}
		exist, err := m.subtaskTable.HasSubtasksInStates(m.id, task.ID, proto.TaskStatePending, proto.TaskStateRevertPending)
		if err != nil {
			m.onError(err)
			continue
		}
		if !exist {
			continue
		}
		logutil.Logger(m.logCtx).Info("detect new subtask", zap.Any("id", task.ID))
		m.addHandlingTask(task.ID)
		t := task
		err = m.schedulerPool.Run(func() {
			m.onRunnableTask(ctx, t.ID, t.Type)
			m.removeHandlingTask(t.ID)
		})
		// pool closed
		if err != nil {
			m.removeHandlingTask(task.ID)
			m.onError(err)
			return
		}
	}
}

// onCanceledTasks cancels the running tasks.
func (m *Manager) onCanceledTasks(_ context.Context, tasks []*proto.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range tasks {
		logutil.Logger(m.logCtx).Info("onCanceledTasks", zap.Any("id", task.ID))
		if cancel, ok := m.mu.handlingTasks[task.ID]; ok && cancel != nil {
			cancel()
		}
	}
}

// cancelAllRunningTasks cancels all running tasks.
func (m *Manager) cancelAllRunningTasks() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, cancel := range m.mu.handlingTasks {
		logutil.Logger(m.logCtx).Info("cancelAllRunningTasks", zap.Any("id", id))
		if cancel != nil {
			cancel()
		}
	}
}

// filterAlreadyHandlingTasks filters the tasks that are already handled.
func (m *Manager) filterAlreadyHandlingTasks(tasks []*proto.Task) []*proto.Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	var i int
	for _, task := range tasks {
		if _, ok := m.mu.handlingTasks[task.ID]; !ok {
			tasks[i] = task
			i++
		}
	}
	return tasks[:i]
}

// onRunnableTask handles a runnable task.
func (m *Manager) onRunnableTask(ctx context.Context, taskID int64, taskType string) {
	logutil.Logger(m.logCtx).Info("onRunnableTask", zap.Any("id", taskID), zap.Any("type", taskType))
	if _, ok := m.subtaskExecutorPools[taskType]; !ok {
		m.onError(errors.Errorf("task type %s not found", taskType))
		return
	}
	// runCtx only used in scheduler.Run, cancel in m.fetchAndFastCancelTasks
	scheduler := newScheduler(ctx, m.id, taskID, m.subtaskTable, m.subtaskExecutorPools[taskType])
	scheduler.Start()
	defer scheduler.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(checkTime):
		}
		task, err := m.globalTaskTable.GetTaskByID(taskID)
		if err != nil {
			m.onError(err)
			return
		}
		if task.State != proto.TaskStateRunning && task.State != proto.TaskStateReverting {
			logutil.Logger(m.logCtx).Info("onRunnableTask exit", zap.Any("id", taskID), zap.Any("state", task.State))
			return
		}
		if exist, err := m.subtaskTable.HasSubtasksInStates(m.id, task.ID, proto.TaskStatePending, proto.TaskStateRevertPending); err != nil {
			m.onError(err)
			return
		} else if !exist {
			continue
		}

		switch task.State {
		case proto.TaskStateRunning:
			runCtx, runCancel := context.WithCancel(ctx)
			m.registerCancelFunc(task.ID, runCancel)
			err = scheduler.Run(runCtx, task)
			runCancel()
		case proto.TaskStateReverting:
			err = scheduler.Rollback(ctx, task)
		}
		if err != nil {
			logutil.Logger(m.logCtx).Error("failed to handle task", zap.Error(err))
		}
	}
}

// addHandlingTask adds a task to the handling task set.
func (m *Manager) addHandlingTask(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.handlingTasks[id] = nil
}

// registerCancelFunc registers a cancel function for a task.
func (m *Manager) registerCancelFunc(id int64, cancel context.CancelFunc) {
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

// onError handles the error.
func (m *Manager) onError(err error) {
	if err == nil {
		return
	}

	logutil.Logger(m.logCtx).Error("task manager error", zap.Error(err))
}
