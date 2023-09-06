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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultCheckSubtaskCanceledInterval is the default check interval for cancel cancelled subtasks.
	DefaultCheckSubtaskCanceledInterval = 2 * time.Second
)

// TestSyncChan is used to sync the test.
var TestSyncChan = make(chan struct{})

// BaseScheduler is the base implementation of Scheduler.
type BaseScheduler struct {
	// id, it's the same as server id now, i.e. host:port.
	id        string
	taskID    int64
	taskTable TaskTable
	pool      Pool
	logCtx    context.Context
	Extension

	mu struct {
		sync.RWMutex
		err error
		// handled indicates whether the error has been updated to one of the subtask.
		handled bool
		// runtimeCancel is used to cancel the Run/Rollback when error occurs.
		runtimeCancel context.CancelFunc
	}
}

// NewBaseScheduler creates a new BaseScheduler.
func NewBaseScheduler(_ context.Context, id string, taskID int64, taskTable TaskTable, pool Pool) *BaseScheduler {
	logPrefix := fmt.Sprintf("id: %s, task_id: %d", id, taskID)
	schedulerImpl := &BaseScheduler{
		id:        id,
		taskID:    taskID,
		taskTable: taskTable,
		pool:      pool,
		logCtx:    logutil.WithKeyValue(context.Background(), "scheduler", logPrefix),
	}
	return schedulerImpl
}

func (s *BaseScheduler) startCancelCheck(ctx context.Context, wg *sync.WaitGroup, cancelFn context.CancelFunc) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(DefaultCheckSubtaskCanceledInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				logutil.Logger(s.logCtx).Info("scheduler exits", zap.Error(ctx.Err()))
				return
			case <-ticker.C:
				canceled, err := s.taskTable.IsSchedulerCanceled(s.taskID, s.id)
				logutil.Logger(s.logCtx).Info("scheduler before canceled")
				if err != nil {
					continue
				}
				if canceled {
					logutil.Logger(s.logCtx).Info("scheduler canceled")
					if cancelFn != nil {
						cancelFn()
					}
				}
			}
		}
	}()
}

// Run runs the scheduler task.
func (s *BaseScheduler) Run(ctx context.Context, task *proto.Task) error {
	err := s.run(ctx, task)
	if s.mu.handled {
		return err
	}
	return s.taskTable.UpdateErrorToSubtask(s.id, task.ID, err)
}

func (s *BaseScheduler) run(ctx context.Context, task *proto.Task) error {
	if ctx.Err() != nil {
		s.onError(ctx.Err())
		return s.getError()
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	s.registerCancelFunc(runCancel)
	s.resetError()
	logutil.Logger(s.logCtx).Info("scheduler run a step", zap.Any("step", task.Step), zap.Any("concurrency", task.Concurrency))

	summary, cleanup, err := runSummaryCollectLoop(ctx, task, s.taskTable)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	defer cleanup()

	executor, err := s.GetSubtaskExecutor(ctx, task, summary)
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	failpoint.Inject("mockExecSubtaskInitEnvErr", func() {
		failpoint.Return(errors.New("mockExecSubtaskInitEnvErr"))
	})
	if err := executor.Init(runCtx); err != nil {
		s.onError(err)
		return s.getError()
	}

	var wg sync.WaitGroup
	cancelCtx, checkCancel := context.WithCancel(ctx)
	s.startCancelCheck(cancelCtx, &wg, runCancel)

	defer func() {
		err := executor.Cleanup(runCtx)
		if err != nil {
			logutil.Logger(s.logCtx).Error("cleanup subtask exec env failed", zap.Error(err))
		}
		checkCancel()
		wg.Wait()
	}()

	minimalTaskCh := make(chan func(), task.Concurrency)
	defer close(minimalTaskCh)

	err = s.pool.RunWithConcurrency(minimalTaskCh, uint32(task.Concurrency))
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	for {
		// check if any error occurs.
		if err := s.getError(); err != nil {
			break
		}

		subtask, err := s.taskTable.GetSubtaskInStates(s.id, task.ID, task.Step, proto.TaskStatePending)
		if err != nil {
			s.onError(err)
			break
		}
		if subtask == nil {
			break
		}
		s.startSubtask(subtask.ID)
		if err := s.getError(); err != nil {
			break
		}
		failpoint.Inject("mockCleanScheduler", func() {
			v, ok := testContexts.Load(s.id)
			if ok {
				if v.(*TestContext).mockDown.Load() {
					failpoint.Break()
				}
			}
		})

		s.runSubtask(runCtx, executor, subtask, minimalTaskCh)
	}
	return s.getError()
}

func (s *BaseScheduler) runSubtask(ctx context.Context, scheduler execute.SubtaskExecutor, subtask *proto.Subtask, minimalTaskCh chan func()) {
	minimalTasks, err := scheduler.SplitSubtask(ctx, subtask)
	if err != nil {
		s.onError(err)
		if errors.Cause(err) == context.Canceled {
			s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateCanceled, nil)
		} else {
			s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateFailed, s.getError())
		}
		s.markErrorHandled()
		return
	}
	if ctx.Err() != nil {
		s.onError(ctx.Err())
		return
	}
	logutil.Logger(s.logCtx).Info("split subTask",
		zap.Int("cnt", len(minimalTasks)),
		zap.Int64("subtask_id", subtask.ID),
		zap.Int64("subtask_step", subtask.Step))

	failpoint.Inject("mockTiDBDown", func(val failpoint.Value) {
		if s.id == val.(string) || s.id == ":4001" || s.id == ":4002" {
			v, ok := testContexts.Load(s.id)
			if ok {
				v.(*TestContext).TestSyncSubtaskRun <- struct{}{}
				v.(*TestContext).mockDown.Store(true)
				time.Sleep(2 * time.Second)
				failpoint.Return()
			}
		}
	})
	failpoint.Inject("mockTiDBDown2", func() {
		if s.id == ":4003" && subtask.Step == proto.StepTwo {
			v, ok := testContexts.Load(s.id)
			if ok {
				v.(*TestContext).TestSyncSubtaskRun <- struct{}{}
				v.(*TestContext).mockDown.Store(true)
				time.Sleep(2 * time.Second)
				return
			}
		}
	})

	failpoint.Inject("mockTiDBPartitionThenResume", func(val failpoint.Value) {
		if val.(bool) && (s.id == ":4000" || s.id == ":4001" || s.id == ":4002") {
			_ = infosync.MockGlobalServerInfoManagerEntry.DeleteByID(s.id)
			time.Sleep(20 * time.Second)
		}
	})

	var minimalTaskWg sync.WaitGroup
	for _, minimalTask := range minimalTasks {
		minimalTaskWg.Add(1)
		j := minimalTask
		minimalTaskCh <- func() {
			s.runMinimalTask(ctx, j, subtask.Type, subtask.Step)
			minimalTaskWg.Done()
		}
	}
	failpoint.Inject("waitUntilError", func() {
		for i := 0; i < 10; i++ {
			if s.getError() != nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	})
	minimalTaskWg.Wait()
	s.onSubtaskFinished(ctx, scheduler, subtask)
}

func (s *BaseScheduler) onSubtaskFinished(ctx context.Context, scheduler execute.SubtaskExecutor, subtask *proto.Subtask) {
	var subtaskMeta []byte
	if err := s.getError(); err == nil {
		if subtaskMeta, err = scheduler.OnFinished(ctx, subtask.Meta); err != nil {
			s.onError(err)
		}
	}
	if err := s.getError(); err != nil {
		if errors.Cause(err) == context.Canceled {
			s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateCanceled, nil)
		} else {
			s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateFailed, s.getError())
		}
		s.markErrorHandled()
		return
	}
	if err := s.taskTable.FinishSubtask(subtask.ID, subtaskMeta); err != nil {
		s.onError(err)
	}
	failpoint.Inject("syncAfterSubtaskFinish", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})
}

func (s *BaseScheduler) runMinimalTask(minimalTaskCtx context.Context, minimalTask proto.MinimalTask, tp string, step int64) {
	select {
	case <-minimalTaskCtx.Done():
		s.onError(minimalTaskCtx.Err())
		return
	default:
	}
	if s.getError() != nil {
		return
	}
	logutil.Logger(s.logCtx).Info("scheduler run a minimalTask", zap.Any("step", step), zap.Stringer("minimal_task", minimalTask))
	executor, err := s.GetMiniTaskExecutor(minimalTask, tp, step)
	if err != nil {
		s.onError(err)
		return
	}

	failpoint.Inject("MockExecutorRunErr", func(val failpoint.Value) {
		if val.(bool) {
			s.onError(errors.New("MockExecutorRunErr"))
		}
	})
	failpoint.Inject("MockExecutorRunCancel", func(val failpoint.Value) {
		if taskID, ok := val.(int); ok {
			mgr, err := storage.GetTaskManager()
			if err != nil {
				logutil.BgLogger().Error("get task manager failed", zap.Error(err))
			} else {
				err = mgr.CancelGlobalTask(int64(taskID))
				if err != nil {
					logutil.BgLogger().Error("cancel global task failed", zap.Error(err))
				}
			}
		}
	})
	if err = executor.Run(minimalTaskCtx); err != nil {
		s.onError(err)
	}
	logutil.Logger(s.logCtx).Info("minimal task done", zap.Stringer("minimal_task", minimalTask))
}

// Rollback rollbacks the scheduler task.
func (s *BaseScheduler) Rollback(ctx context.Context, task *proto.Task) error {
	rollbackCtx, rollbackCancel := context.WithCancel(ctx)
	defer rollbackCancel()
	s.registerCancelFunc(rollbackCancel)

	s.resetError()
	logutil.Logger(s.logCtx).Info("scheduler rollback a step", zap.Any("step", task.Step))

	// We should cancel all subtasks before rolling back
	for {
		subtask, err := s.taskTable.GetSubtaskInStates(s.id, task.ID, task.Step, proto.TaskStatePending, proto.TaskStateRunning)
		if err != nil {
			s.onError(err)
			return s.getError()
		}

		if subtask == nil {
			break
		}

		s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateCanceled, nil)
		if err = s.getError(); err != nil {
			return err
		}
	}

	executor, err := s.GetSubtaskExecutor(ctx, task, nil)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	subtask, err := s.taskTable.GetSubtaskInStates(s.id, task.ID, task.Step, proto.TaskStateRevertPending)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if subtask == nil {
		logutil.BgLogger().Warn("scheduler rollback a step, but no subtask in revert_pending state", zap.Any("step", task.Step))
		return nil
	}
	s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateReverting, nil)
	if err := s.getError(); err != nil {
		return err
	}

	err = executor.Rollback(rollbackCtx)
	if err != nil {
		s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateRevertFailed, nil)
		s.onError(err)
	} else {
		s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateReverted, nil)
	}
	return s.getError()
}

func runSummaryCollectLoop(
	ctx context.Context,
	task *proto.Task,
	taskTable TaskTable,
) (summary *execute.Summary, cleanup func(), err error) {
	taskMgr, ok := taskTable.(*storage.TaskManager)
	if !ok {
		return nil, func() {}, nil
	}
	opt, ok := taskTypes[task.Type]
	if !ok {
		return nil, func() {}, errors.Errorf("scheduler option for type %s not found", task.Type)
	}
	if opt.Summary != nil {
		go opt.Summary.UpdateRowCountLoop(ctx, taskMgr)
		return opt.Summary, func() {
			opt.Summary.PersistRowCount(ctx, taskMgr)
		}, nil
	}
	return nil, func() {}, nil
}

func (s *BaseScheduler) registerCancelFunc(cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.runtimeCancel = cancel
}

func (s *BaseScheduler) onError(err error) {
	if err == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.err == nil {
		s.mu.err = err
		logutil.Logger(s.logCtx).Error("scheduler error", zap.Error(err))
	}

	if s.mu.runtimeCancel != nil {
		s.mu.runtimeCancel()
	}
}

func (s *BaseScheduler) markErrorHandled() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.handled = true
}

func (s *BaseScheduler) getError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.err
}

func (s *BaseScheduler) resetError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.err = nil
	s.mu.handled = false
}

func (s *BaseScheduler) startSubtask(id int64) {
	err := s.taskTable.StartSubtask(id)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseScheduler) updateSubtaskStateAndError(subtaskID int64, state string, subTaskErr error) {
	err := s.taskTable.UpdateSubtaskStateAndError(subtaskID, state, subTaskErr)
	if err != nil {
		s.onError(err)
	}
}
