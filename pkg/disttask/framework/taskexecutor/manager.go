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
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

var (
	executorPoolSize int32 = 4
	// same as scheduler
	checkTime               = 300 * time.Millisecond
	recoverMetaInterval     = 90 * time.Second
	retrySQLTimes           = 30
	retrySQLInterval        = 500 * time.Millisecond
	unfinishedSubtaskStates = []proto.SubtaskState{
		proto.SubtaskStatePending, proto.SubtaskStateRevertPending,
		// for the case that the tidb is restarted when the subtask is running.
		proto.SubtaskStateRunning, proto.SubtaskStateReverting,
	}
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
	logger      *zap.Logger
	newPool     func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error)
	slotManager *slotManager

	totalCPU int
	totalMem int64
}

// BuildManager builds a Manager.
func (b *ManagerBuilder) BuildManager(ctx context.Context, id string, taskTable TaskTable) (*Manager, error) {
	totalMem, err := memory.MemTotal()
	if err != nil {
		// should not happen normally, as in main function of tidb-server, we assert
		// that memory.MemTotal() will not fail.
		return nil, err
	}
	totalCPU := cpu.GetCPUCount()
	if totalCPU <= 0 || totalMem <= 0 {
		return nil, errors.Errorf("invalid cpu or memory, cpu: %d, memory: %d", totalCPU, totalMem)
	}
	logutil.BgLogger().Info("build manager", zap.Int("total-cpu", totalCPU),
		zap.String("total-mem", units.BytesSize(float64(totalMem))))
	m := &Manager{
		id:        id,
		taskTable: taskTable,
		logger:    logutil.BgLogger(),
		newPool:   b.newPool,
		slotManager: &slotManager{
			taskID2Index:  make(map[int64]int),
			executorTasks: make([]*proto.Task, 0),
			available:     totalCPU,
		},
		totalCPU: totalCPU,
		totalMem: int64(totalMem),
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

// InitMeta initializes the meta of the Manager.
// not a must-success step before start manager,
// manager will try to recover meta periodically.
func (m *Manager) InitMeta() (err error) {
	for i := 0; i < retrySQLTimes; i++ {
		err = m.taskTable.InitMeta(m.ctx, m.id, config.GetGlobalConfig().Instance.TiDBServiceScope)
		if err == nil {
			break
		}
		if err1 := m.ctx.Err(); err1 != nil {
			return err1
		}
		if i%10 == 0 {
			m.logger.Warn("start manager failed",
				zap.String("scope", config.GetGlobalConfig().Instance.TiDBServiceScope),
				zap.Int("retry times", i),
				zap.Error(err))
		}
		time.Sleep(retrySQLInterval)
	}
	return err
}

func (m *Manager) recoverMeta() (err error) {
	for i := 0; i < retrySQLTimes; i++ {
		err = m.taskTable.RecoverMeta(m.ctx, m.id, config.GetGlobalConfig().Instance.TiDBServiceScope)
		if err == nil {
			break
		}
		if err1 := m.ctx.Err(); err1 != nil {
			return err1
		}
		if i%10 == 0 {
			m.logger.Warn("recover meta failed",
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
	m.logger.Debug("manager start")
	m.wg.Run(m.handleTasksLoop)
	m.wg.Run(m.recoverMetaLoop)
	return nil
}

// Stop stops the Manager.
func (m *Manager) Stop() {
	m.cancel()
	m.executorPool.ReleaseAndWait()
	m.wg.Wait()
}

// handleTasksLoop handle tasks of interested states, including:
//   - pending/running: start the task executor.
//   - reverting: cancel the task executor, and mark running subtasks as Canceled.
//   - pausing: cancel the task executor, mark all pending/running subtasks of current
//     node as paused.
//
// Pausing is handled on every executor to make sure all subtasks are
// NOT running by executor before mark the task as paused.
func (m *Manager) handleTasksLoop() {
	defer tidbutil.Recover(metrics.LabelDomain, "handleTasksLoop", m.handleTasksLoop, false)
	ticker := time.NewTicker(checkTime)
	for {
		select {
		case <-m.ctx.Done():
			m.logger.Info("handle tasks loop done")
			return
		case <-ticker.C:
		}

		m.handleTasks()
	}
}

func (m *Manager) handleTasks() {
	tasks, err := m.taskTable.GetTasksInStates(m.ctx, proto.TaskStateRunning,
		proto.TaskStateReverting, proto.TaskStatePausing)
	if err != nil {
		m.logErr(err)
		return
	}

	executableTasks := make([]*proto.Task, 0, len(tasks))
	for _, task := range tasks {
		switch task.State {
		case proto.TaskStateRunning, proto.TaskStateReverting:
			if task.State == proto.TaskStateReverting {
				m.cancelRunningSubtaskOf(task)
			}
			// TaskStateReverting require executor to run rollback logic.
			if !m.isExecutorStarted(task.ID) {
				executableTasks = append(executableTasks, task)
			}
		case proto.TaskStatePausing:
			if err := m.handlePausingTask(task); err != nil {
				m.logErr(err)
			}
		}
	}

	if len(executableTasks) > 0 {
		m.handleExecutableTasks(executableTasks)
	}
}

// handleExecutableTasks handles executable tasks.
func (m *Manager) handleExecutableTasks(tasks []*proto.Task) {
	for _, task := range tasks {
		exist, err := m.taskTable.HasSubtasksInStates(m.ctx, m.id, task.ID, task.Step, unfinishedSubtaskStates...)
		if err != nil {
			m.logger.Error("check subtask exist failed", zap.Error(err))
			m.logErr(err)
			continue
		}
		if !exist {
			continue
		}
		m.logger.Info("detect new subtask", zap.Int64("task-id", task.ID))

		canAlloc, tasksNeedFree := m.slotManager.canAlloc(task)
		if len(tasksNeedFree) > 0 {
			m.cancelTaskExecutors(tasksNeedFree)
			// do not handle the tasks with lower priority if current task is waiting tasks free.
			break
		}

		if !canAlloc {
			m.logger.Debug("no enough slots to run task", zap.Int64("task-id", task.ID))
			continue
		}
		m.addHandlingTask(task.ID)
		m.slotManager.alloc(task)
		t := task
		err = m.executorPool.Run(func() {
			defer m.slotManager.free(t.ID)
			m.handleExecutableTask(t)
			m.removeHandlingTask(t.ID)
		})
		// pool closed.
		if err != nil {
			m.slotManager.free(t.ID)
			m.removeHandlingTask(task.ID)
			m.logErr(err)
			return
		}
	}
}

// cancelRunningSubtaskOf cancels the running subtask of the task, the subtask
// will switch to `canceled` state.
func (m *Manager) cancelRunningSubtaskOf(task *proto.Task) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if cancel, ok := m.mu.handlingTasks[task.ID]; ok && cancel != nil {
		m.logger.Info("onCanceledTasks", zap.Int64("task-id", task.ID))
		// subtask needs to change its state to `canceled`.
		cancel(ErrCancelSubtask)
	}
}

// onPausingTasks pauses/cancels the pending/running subtasks.
func (m *Manager) handlePausingTask(task *proto.Task) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.logger.Info("handle pausing task", zap.Int64("task-id", task.ID))
	if cancel, ok := m.mu.handlingTasks[task.ID]; ok && cancel != nil {
		// cancel the task executor
		cancel(nil)
	}
	// we pause subtasks belongs to this exec node even when there's no executor running.
	// as balancer might move subtasks to this node when the executor hasn't started.
	return m.taskTable.PauseSubtasks(m.ctx, m.id, task.ID)
}

// recoverMetaLoop recovers dist_framework_meta for the tidb node running the taskExecutor manager.
// This is necessary when the TiDB node experiences a prolonged network partition
// and the scheduler deletes `dist_framework_meta`.
// When the TiDB node recovers from the network partition,
// we need to re-insert the metadata.
func (m *Manager) recoverMetaLoop() {
	defer tidbutil.Recover(metrics.LabelDomain, "recoverMetaLoop", m.recoverMetaLoop, false)
	ticker := time.NewTicker(recoverMetaInterval)
	for {
		select {
		case <-m.ctx.Done():
			m.logger.Info("recoverMetaLoop done")
			return
		case <-ticker.C:
			if err := m.recoverMeta(); err != nil {
				m.logErr(err)
				continue
			}
		}
	}
}

// cancelTaskExecutors cancels the task executors.
// unlike cancelRunningSubtaskOf, this function doesn't change subtask state.
func (m *Manager) cancelTaskExecutors(tasks []*proto.Task) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, task := range tasks {
		m.logger.Info("cancelTasks", zap.Int64("task-id", task.ID))
		if cancel, ok := m.mu.handlingTasks[task.ID]; ok && cancel != nil {
			// only cancel the executor, subtask state is not changed.
			cancel(nil)
		}
	}
}

// TestContext only used in tests.
type TestContext struct {
	TestSyncSubtaskRun chan struct{}
	mockDown           atomic.Bool
}

var testContexts sync.Map

// handleExecutableTask handles a runnable task.
func (m *Manager) handleExecutableTask(task *proto.Task) {
	m.logger.Info("handleExecutableTask", zap.Int64("task-id", task.ID), zap.Stringer("type", task.Type))
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
			m.logger.Info("handleExecutableTask exit for cancel", zap.Int64("task-id", task.ID), zap.Stringer("type", task.Type))
			return
		case <-time.After(checkTime):
		}
		failpoint.Inject("mockStopManager", func() {
			testContexts.Store(m.id, &TestContext{make(chan struct{}), atomic.Bool{}})
			go func() {
				v, ok := testContexts.Load(m.id)
				if ok {
					<-v.(*TestContext).TestSyncSubtaskRun
					infosync.MockGlobalServerInfoManagerEntry.DeleteByExecID(m.id)
					m.Stop()
				}
			}()
		})
		task, err = m.taskTable.GetTaskByID(m.ctx, task.ID)
		if err != nil {
			m.logErr(err)
			return
		}
		if task.State != proto.TaskStateRunning && task.State != proto.TaskStateReverting {
			m.logger.Info("handleExecutableTask exit",
				zap.Int64("task-id", task.ID), zap.String("step", proto.Step2Str(task.Type, task.Step)), zap.Stringer("state", task.State))
			return
		}
		if exist, err := m.taskTable.HasSubtasksInStates(m.ctx, m.id, task.ID, task.Step,
			unfinishedSubtaskStates...); err != nil {
			m.logErr(err)
			return
		} else if !exist {
			continue
		}
		stepResource := m.getStepResource(task.Concurrency)
		m.logger.Info("execute task step with resource",
			zap.Int64("task-id", task.ID), zap.String("step", proto.Step2Str(task.Type, task.Step)),
			zap.Stringer("resource", stepResource))
		switch task.State {
		case proto.TaskStateRunning:
			if taskCtx.Err() != nil {
				return
			}
			// use taskCtx for canceling.
			err = executor.RunStep(taskCtx, task, stepResource)
		case proto.TaskStateReverting:
			// use m.ctx since this process should not be canceled.
			// TODO: will remove it later, leave it now.
			err = executor.Rollback(m.ctx, task)
		}
		if err != nil {
			m.logger.Error("failed to handle task", zap.Error(err))
		}
	}
}

func (m *Manager) getStepResource(concurrency int) *proto.StepResource {
	return &proto.StepResource{
		CPU: proto.NewAllocatable(int64(concurrency)),
		// same proportion as CPU
		Mem: proto.NewAllocatable(int64(float64(concurrency) / float64(m.totalCPU) * float64(m.totalMem))),
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

func (m *Manager) isExecutorStarted(taskID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.mu.handlingTasks[taskID]
	return ok
}

func (m *Manager) logErr(err error) {
	m.logger.Error("task manager met error", zap.Error(err), zap.Stack("stack"))
}

func (m *Manager) logErrAndPersist(err error, taskID int64, taskExecutor TaskExecutor) {
	m.logErr(err)
	if taskExecutor != nil && taskExecutor.IsRetryableError(err) {
		m.logger.Error("met retryable err", zap.Error(err), zap.Stack("stack"))
		return
	}
	err1 := m.taskTable.FailSubtask(m.ctx, m.id, taskID, err)
	if err1 != nil {
		m.logger.Error("update to subtask failed", zap.Error(err1), zap.Stack("stack"))
	}
	m.logger.Error("update error to subtask", zap.Int64("task-id", taskID), zap.Error(err1), zap.Stack("stack"))
}
