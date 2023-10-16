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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultCheckSubtaskCanceledInterval is the default check interval for cancel cancelled subtasks.
	DefaultCheckSubtaskCanceledInterval = 2 * time.Second
)

var (
	// ErrCancelSubtask is the cancel cause when cancelling subtasks.
	ErrCancelSubtask = errors.New("cancel subtasks")
	// ErrFinishSubtask is the cancel cause when scheduler successfully processed subtasks.
	ErrFinishSubtask = errors.New("finish subtasks")
	// ErrFinishRollback is the cancel cause when scheduler rollback successfully.
	ErrFinishRollback = errors.New("finish rollback")

	// TestSyncChan is used to sync the test.
	TestSyncChan = make(chan struct{})
)

// BaseScheduler is the base implementation of Scheduler.
type BaseScheduler struct {
	// id, it's the same as server id now, i.e. host:port.
	id        string
	taskID    int64
	taskTable TaskTable
	logCtx    context.Context
	Extension

	mu struct {
		sync.RWMutex
		err error
		// handled indicates whether the error has been updated to one of the subtask.
		handled bool
		// runtimeCancel is used to cancel the Run/Rollback when error occurs.
		runtimeCancel context.CancelCauseFunc
	}
}

// NewBaseScheduler creates a new BaseScheduler.
func NewBaseScheduler(_ context.Context, id string, taskID int64, taskTable TaskTable) *BaseScheduler {
	schedulerImpl := &BaseScheduler{
		id:        id,
		taskID:    taskID,
		taskTable: taskTable,
		logCtx:    logutil.WithFields(context.Background(), zap.Int64("task-id", taskID)),
	}
	return schedulerImpl
}

func (s *BaseScheduler) startCancelCheck(ctx context.Context, wg *sync.WaitGroup, cancelFn context.CancelCauseFunc) {
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
				canceled, err := s.taskTable.IsSchedulerCanceled(s.id, s.taskID)
				if err != nil {
					continue
				}
				if canceled {
					logutil.Logger(s.logCtx).Info("scheduler canceled")
					if cancelFn != nil {
						// subtask transferred to other tidb, don't mark subtask as canceled.
						// Should not change the subtask's state.
						cancelFn(nil)
					}
				}
			}
		}
	}()
}

// Init implements the Scheduler interface.
func (*BaseScheduler) Init(_ context.Context) error {
	return nil
}

// Run runs the scheduler task.
func (s *BaseScheduler) Run(ctx context.Context, task *proto.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("BaseScheduler panicked", zap.Any("recover", r), zap.Stack("stack"))
			err4Panic := errors.Errorf("%v", r)
			err1 := s.updateErrorToSubtask(ctx, task.ID, err4Panic)
			if err == nil {
				err = err1
			}
		}
	}()
	err = s.run(ctx, task)
	if s.mu.handled {
		return err
	}
	if err == nil {
		return nil
	}
	return s.updateErrorToSubtask(ctx, task.ID, err)
}

func (s *BaseScheduler) run(ctx context.Context, task *proto.Task) error {
	if ctx.Err() != nil {
		s.onError(ctx.Err())
		return s.getError()
	}
	runCtx, runCancel := context.WithCancelCause(ctx)
	defer runCancel(ErrFinishSubtask)
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

	subtasks, err := s.taskTable.GetSubtasksInStates(s.id, task.ID, task.Step,
		proto.TaskStatePending, proto.TaskStateRunning)
	if err != nil {
		s.onError(err)
		if common.IsRetryableError(err) {
			logutil.Logger(s.logCtx).Warn("met retryable error", zap.Error(err))
			return nil
		}
		return s.getError()
	}
	for _, subtask := range subtasks {
		metrics.IncDistTaskSubTaskCnt(subtask)
		metrics.StartDistTaskSubTask(subtask)
	}

	for {
		// check if any error occurs.
		if err := s.getError(); err != nil {
			break
		}

		subtask, err := s.taskTable.GetFirstSubtaskInStates(s.id, task.ID, task.Step,
			proto.TaskStatePending, proto.TaskStateRunning)
		if err != nil {
			logutil.Logger(s.logCtx).Warn("GetFirstSubtaskInStates meets error", zap.Error(err))
			continue
		}
		if subtask == nil {
			newTask, err := s.taskTable.GetGlobalTaskByID(task.ID)
			if err != nil {
				logutil.Logger(s.logCtx).Warn("GetGlobalTaskByID meets error", zap.Error(err))
				continue
			}
			// When the task move to next step or task state changes, the scheduler should exit.
			if newTask.Step != task.Step || newTask.State != task.State {
				break
			}
			continue
		}

		if subtask.State == proto.TaskStateRunning {
			if !s.IsIdempotent(subtask) {
				logutil.Logger(s.logCtx).Info("subtask in running state and is not idempotent, fail it",
					zap.Int64("subtask-id", subtask.ID))
				subtaskErr := errors.New("subtask in running state and is not idempotent")
				s.onError(subtaskErr)
				s.updateSubtaskStateAndError(subtask, proto.TaskStateFailed, subtaskErr)
				s.markErrorHandled()
				break
			}
		} else {
			// subtask.State == proto.TaskStatePending
			s.startSubtaskAndUpdateState(ctx, subtask)
			if err := s.getError(); err != nil {
				logutil.Logger(s.logCtx).Warn("startSubtaskAndUpdateState meets error", zap.Error(err))
				continue
			}
		}

		failpoint.Inject("mockCleanScheduler", func() {
			v, ok := testContexts.Load(s.id)
			if ok {
				if v.(*TestContext).mockDown.Load() {
					failpoint.Break()
				}
			}
		})

		s.runSubtask(runCtx, executor, subtask)
	}
	return s.getError()
}

func (s *BaseScheduler) runSubtask(ctx context.Context, executor execute.SubtaskExecutor, subtask *proto.Subtask) {
	err := executor.RunSubtask(ctx, subtask)
	failpoint.Inject("MockRunSubtaskCancel", func(val failpoint.Value) {
		if val.(bool) {
			err = ErrCancelSubtask
		}
	})

	failpoint.Inject("MockRunSubtaskContextCanceled", func(val failpoint.Value) {
		if val.(bool) {
			err = context.Canceled
		}
	})

	if err != nil {
		s.onError(err)
	}

	finished := s.markSubTaskCanceledOrFailed(ctx, subtask)
	if finished {
		return
	}

	failpoint.Inject("mockTiDBDown", func(val failpoint.Value) {
		logutil.Logger(s.logCtx).Info("trigger mockTiDBDown")
		if s.id == val.(string) || s.id == ":4001" || s.id == ":4002" {
			v, ok := testContexts.Load(s.id)
			if ok {
				v.(*TestContext).TestSyncSubtaskRun <- struct{}{}
				v.(*TestContext).mockDown.Store(true)
				logutil.Logger(s.logCtx).Info("mockTiDBDown")
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
	s.onSubtaskFinished(ctx, executor, subtask)
}

func (s *BaseScheduler) onSubtaskFinished(ctx context.Context, executor execute.SubtaskExecutor, subtask *proto.Subtask) {
	if err := s.getError(); err == nil {
		if err = executor.OnFinished(ctx, subtask); err != nil {
			s.onError(err)
		}
	}
	failpoint.Inject("MockSubtaskFinishedCancel", func(val failpoint.Value) {
		if val.(bool) {
			s.onError(ErrCancelSubtask)
		}
	})

	finished := s.markSubTaskCanceledOrFailed(ctx, subtask)
	if finished {
		return
	}

	s.finishSubtaskAndUpdateState(ctx, subtask)

	finished = s.markSubTaskCanceledOrFailed(ctx, subtask)
	if finished {
		return
	}

	failpoint.Inject("syncAfterSubtaskFinish", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})
}

// Rollback rollbacks the scheduler task.
func (s *BaseScheduler) Rollback(ctx context.Context, task *proto.Task) error {
	rollbackCtx, rollbackCancel := context.WithCancelCause(ctx)
	defer rollbackCancel(ErrFinishRollback)
	s.registerCancelFunc(rollbackCancel)

	s.resetError()
	logutil.Logger(s.logCtx).Info("scheduler rollback a step", zap.Any("step", task.Step))

	// We should cancel all subtasks before rolling back
	for {
		subtask, err := s.taskTable.GetFirstSubtaskInStates(s.id, task.ID, task.Step,
			proto.TaskStatePending, proto.TaskStateRunning)
		if err != nil {
			s.onError(err)
			return s.getError()
		}

		if subtask == nil {
			break
		}

		s.updateSubtaskStateAndError(subtask, proto.TaskStateCanceled, nil)
		if err = s.getError(); err != nil {
			return err
		}
	}

	executor, err := s.GetSubtaskExecutor(ctx, task, nil)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	subtask, err := s.taskTable.GetFirstSubtaskInStates(s.id, task.ID, task.Step,
		proto.TaskStateRevertPending, proto.TaskStateReverting)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if subtask == nil {
		logutil.BgLogger().Warn("scheduler rollback a step, but no subtask in revert_pending state", zap.Any("step", task.Step))
		return nil
	}
	if subtask.State == proto.TaskStateRevertPending {
		s.updateSubtaskStateAndError(subtask, proto.TaskStateReverting, nil)
	}
	if err := s.getError(); err != nil {
		return err
	}

	// right now all impl of Rollback is empty, so we don't check idempotent here.
	// will try to remove this rollback completely in the future.
	err = executor.Rollback(rollbackCtx)
	if err != nil {
		s.updateSubtaskStateAndError(subtask, proto.TaskStateRevertFailed, nil)
		s.onError(err)
	} else {
		s.updateSubtaskStateAndError(subtask, proto.TaskStateReverted, nil)
	}
	return s.getError()
}

// Pause pause the scheduler task.
func (s *BaseScheduler) Pause(_ context.Context, task *proto.Task) error {
	logutil.Logger(s.logCtx).Info("scheduler pause subtasks")
	// pause all running subtasks.
	if err := s.taskTable.PauseSubtasks(s.id, task.ID); err != nil {
		s.onError(err)
		return s.getError()
	}
	return nil
}

// Close closes the scheduler when all the subtasks are complete.
func (*BaseScheduler) Close() {
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

func (s *BaseScheduler) registerCancelFunc(cancel context.CancelCauseFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.runtimeCancel = cancel
}

func (s *BaseScheduler) onError(err error) {
	if err == nil {
		return
	}
	err = errors.Trace(err)
	logutil.Logger(s.logCtx).Error("onError", zap.Error(err))
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.err == nil {
		s.mu.err = err
		logutil.Logger(s.logCtx).Error("scheduler error", zap.Error(err))
	}

	if s.mu.runtimeCancel != nil {
		s.mu.runtimeCancel(err)
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

func (s *BaseScheduler) startSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.startSubtask(ctx, subtask.ID)
	subtask.State = proto.TaskStateRunning
	metrics.IncDistTaskSubTaskCnt(subtask)
	metrics.StartDistTaskSubTask(subtask)
}

func (s *BaseScheduler) updateSubtaskStateAndErrorImpl(subtaskID int64, state proto.TaskState, subTaskErr error) {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	ctx := context.Background()
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.UpdateSubtaskStateAndError(subtaskID, state, subTaskErr)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseScheduler) startSubtask(ctx context.Context, subtaskID int64) {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.StartSubtask(subtaskID)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseScheduler) finishSubtask(ctx context.Context, subtask *proto.Subtask) {
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.FinishSubtask(subtask.ID, subtask.Meta)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseScheduler) updateSubtaskStateAndError(subtask *proto.Subtask, state proto.TaskState, subTaskErr error) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.updateSubtaskStateAndErrorImpl(subtask.ID, state, subTaskErr)
	subtask.State = state
	metrics.IncDistTaskSubTaskCnt(subtask)
	if !subtask.IsFinished() {
		metrics.StartDistTaskSubTask(subtask)
	}
}

func (s *BaseScheduler) finishSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.finishSubtask(ctx, subtask)
	subtask.State = proto.TaskStateSucceed
	metrics.IncDistTaskSubTaskCnt(subtask)
}

// markSubTaskCanceledOrFailed check the error type and decide the subtasks' state.
// 1. Only cancel subtasks when meet ErrCancelSubtask.
// 2. Only fail subtasks when meet non retryable error.
// 3. When meet other errors, don't change subtasks' state.
func (s *BaseScheduler) markSubTaskCanceledOrFailed(ctx context.Context, subtask *proto.Subtask) bool {
	if err := s.getError(); err != nil {
		if ctx.Err() != nil && context.Cause(ctx) == ErrCancelSubtask {
			logutil.Logger(s.logCtx).Warn("subtask canceled", zap.Error(err))
			s.updateSubtaskStateAndError(subtask, proto.TaskStateCanceled, nil)
		} else if common.IsRetryableError(err) {
			logutil.Logger(s.logCtx).Warn("met retryable error", zap.Error(err))
		} else if errors.Cause(err) != context.Canceled {
			logutil.Logger(s.logCtx).Warn("subtask failed", zap.Error(err))
			s.updateSubtaskStateAndError(subtask, proto.TaskStateFailed, err)
		} else {
			logutil.Logger(s.logCtx).Info("met context canceled for gracefully shutdown", zap.Error(err))
		}
		s.markErrorHandled()
		return true
	}
	return false
}

func (s *BaseScheduler) updateErrorToSubtask(ctx context.Context, taskID int64, err error) error {
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err1 := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.UpdateErrorToSubtask(s.id, taskID, err)
		},
	)
	return err1
}
