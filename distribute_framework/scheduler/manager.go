// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/distribute_framework/storage"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	schedulerPoolSize       = 4
	subtaskExecutorPoolSize = 20
	checkTime               = time.Second
)

type Manager struct {
	globalTaskTable     *storage.GlobalTaskManager
	subtaskTable        *storage.SubTaskManager
	schedulerPool       proto.Pool
	subtaskExecutorPool proto.Pool
	mu                  struct {
		sync.Mutex
		runningTasks map[proto.TaskID]*SchedulerImpl
	}
	id     proto.InstanceID
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewManager(ctx context.Context, id proto.InstanceID, globalTaskTable *storage.GlobalTaskManager, subtaskTable *storage.SubTaskManager) *Manager {
	m := &Manager{
		id:                  id,
		globalTaskTable:     globalTaskTable,
		subtaskTable:        subtaskTable,
		schedulerPool:       proto.NewPool(schedulerPoolSize),
		subtaskExecutorPool: proto.NewPool(subtaskExecutorPoolSize),
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.mu.runningTasks = make(map[proto.TaskID]*SchedulerImpl)
	return m
}

func (m *Manager) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.fetchAndHandleRunningTasks(m.ctx)
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.fetchAndHandleCanceledTasks(m.ctx)
	}()

	// TODO: handle rollback tasks
}

func (m *Manager) Stop() {
	m.cancel()
	m.subtaskExecutorPool.ReleaseAndWait()
	m.schedulerPool.ReleaseAndWait()
	m.wg.Wait()
}

func (m *Manager) fetchAndHandleRunningTasks(ctx context.Context) {
	ticker := time.NewTicker(checkTime)
	for {
		select {
		case <-ctx.Done():
			logutil.BgLogger().Info("fetchAndHandleRunningTasks done")
			return
		case <-ticker.C:
			tasks, err := m.globalTaskTable.GetTasksInStates(string(proto.TaskStateRunning))
			if err != nil {
				m.onError(err)
				continue
			}
			m.onRunningTasks(ctx, tasks)
		}
	}
}

func (m *Manager) fetchAndHandleCanceledTasks(ctx context.Context) {
	ticker := time.NewTicker(checkTime)
	for {
		select {
		case <-ctx.Done():
			m.cancelAllRunningTasks()
			logutil.BgLogger().Info("fetchAndHandleCanceledTasks done")
			return
		case <-ticker.C:
			tasks, err := m.globalTaskTable.GetTasksInStates(string(proto.TaskStateCanceled))
			if err != nil {
				m.onError(err)
				continue
			}
			m.onCanceledTasks(ctx, tasks)
		}
	}
}

func (m *Manager) onRunningTasks(ctx context.Context, tasks []*proto.Task) {
	m.filterAlreadyRunningTasks(tasks)
	for _, task := range tasks {
		logutil.BgLogger().Info("onRunningTasks", zap.Any("id", task.ID))
		exist, err := m.subtaskTable.HasSubtasksInStates(m.id, task.ID, string(proto.TaskStatePending))
		if err != nil {
			m.onError(err)
			continue
		}
		if !exist {
			continue
		}
		logutil.BgLogger().Info("detect new subtask", zap.Any("id", task.ID))
		err = m.schedulerPool.Run(func() {
			m.onRunningTask(ctx, task.ID)
		})
		// pool closed
		if err != nil {
			m.onError(err)
			return
		}
	}
}

func (m *Manager) onCanceledTasks(ctx context.Context, tasks []*proto.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range tasks {
		if scheduler, ok := m.mu.runningTasks[task.ID]; ok {
			scheduler.Stop()
			delete(m.mu.runningTasks, task.ID)
		}
	}
}

func (m *Manager) cancelAllRunningTasks() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, scheduler := range m.mu.runningTasks {
		scheduler.Stop()
	}
	m.mu.runningTasks = make(map[proto.TaskID]*SchedulerImpl)
}

func (m *Manager) filterAlreadyRunningTasks(runningTasks []*proto.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var i int
	for _, task := range runningTasks {
		if _, ok := m.mu.runningTasks[task.ID]; !ok {
			runningTasks[i] = task
			i++
		}
	}
	runningTasks = runningTasks[:i]
}

func (m *Manager) onRunningTask(ctx context.Context, taskID proto.TaskID) {
	scheduler := NewScheduler(ctx, m.id, taskID, m.subtaskTable, m.subtaskExecutorPool)
	scheduler.Start()
	defer scheduler.Stop()
	m.addRunningTask(taskID, scheduler)
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
		if task.State != proto.TaskStateRunning {
			return
		}
		if exist, err := m.subtaskTable.HasSubtasksInStates(m.id, task.ID, string(proto.TaskStatePending)); err != nil {
			m.onError(err)
			return
		} else if !exist {
			continue
		}
		m.handleTask(scheduler, task)
	}
}

func (m *Manager) addRunningTask(id proto.TaskID, scheduler *SchedulerImpl) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.runningTasks[id] = scheduler
}

func (m *Manager) handleTask(scheduler *SchedulerImpl, task *proto.Task) {
	logutil.BgLogger().Info("handle task", zap.Any("id", task.ID), zap.Any("state", task.State))
	switch task.State {
	case proto.TaskStateRunning:
		if err := scheduler.Run(task); err != nil {
			logutil.BgLogger().Error("run task failed", zap.Error(err))
		}
	default:
		// TODO: handle other status
	}
}

func (m *Manager) onError(err error) {
	if err == nil {
		return
	}

	logutil.BgLogger().Error("task manager error", zap.Error(err))
}
