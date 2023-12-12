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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

const (
	// DefaultCheckSubtaskCanceledInterval is the default check interval for cancel cancelled subtasks.
	DefaultCheckSubtaskCanceledInterval = 2 * time.Second
)

var (
	// ErrCancelSubtask is the cancel cause when cancelling subtasks.
	ErrCancelSubtask = errors.New("cancel subtasks")
	// ErrFinishSubtask is the cancel cause when TaskExecutor successfully processed subtasks.
	ErrFinishSubtask = errors.New("finish subtasks")
	// ErrFinishRollback is the cancel cause when TaskExecutor rollback successfully.
	ErrFinishRollback = errors.New("finish rollback")

	// TestSyncChan is used to sync the test.
	TestSyncChan = make(chan struct{})
)

// BaseTaskExecutor is the base implementation of TaskExecutor.
type BaseTaskExecutor struct {
	// id, it's the same as server id now, i.e. host:port.
	id        string
	taskID    int64
	taskTable TaskTable
	logCtx    context.Context
	// ctx from manager
	ctx context.Context
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

// NewBaseTaskExecutor creates a new BaseTaskExecutor.
func NewBaseTaskExecutor(ctx context.Context, id string, taskID int64, taskTable TaskTable) *BaseTaskExecutor {
	taskExecutorImpl := &BaseTaskExecutor{
		id:        id,
		taskID:    taskID,
		taskTable: taskTable,
		ctx:       ctx,
		logCtx:    logutil.WithFields(context.Background(), zap.Int64("task-id", taskID)),
	}
	return taskExecutorImpl
}

func (s *BaseTaskExecutor) startCancelCheck(ctx context.Context, wg *sync.WaitGroup, cancelFn context.CancelCauseFunc) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(DefaultCheckSubtaskCanceledInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				logutil.Logger(s.logCtx).Info("taskExecutor exits", zap.Error(ctx.Err()))
				return
			case <-ticker.C:
				canceled, err := s.taskTable.IsTaskExecutorCanceled(ctx, s.id, s.taskID)
				if err != nil {
					continue
				}
				if canceled {
					logutil.Logger(s.logCtx).Info("taskExecutor canceled")
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

// Init implements the TaskExecutor interface.
func (*BaseTaskExecutor) Init(_ context.Context) error {
	return nil
}

// Run start to fetch and run all subtasks of the task on the node.
func (s *BaseTaskExecutor) Run(ctx context.Context, task *proto.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(s.logCtx).Error("BaseTaskExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
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

func (s *BaseTaskExecutor) run(ctx context.Context, task *proto.Task) (resErr error) {
	if ctx.Err() != nil {
		s.onError(ctx.Err())
		return s.getError()
	}
	runCtx, runCancel := context.WithCancelCause(ctx)
	defer runCancel(ErrFinishSubtask)
	s.registerCancelFunc(runCancel)
	s.resetError()
	stepLogger := log.BeginTask(logutil.Logger(s.logCtx).With(
		zap.Any("step", task.Step),
		zap.Int("concurrency", task.Concurrency),
		zap.Float64("mem-limit-percent", gctuner.GlobalMemoryLimitTuner.GetPercentage()),
		zap.String("server-mem-limit", memory.ServerMemoryLimitOriginText.Load()),
	), "schedule step")
	// log as info level, subtask might be cancelled, let caller check it.
	defer func() {
		stepLogger.End(zap.InfoLevel, resErr)
	}()

	summary, cleanup, err := runSummaryCollectLoop(ctx, task, s.taskTable)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	defer cleanup()
	subtaskExecutor, err := s.GetSubtaskExecutor(ctx, task, summary)
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	failpoint.Inject("mockExecSubtaskInitEnvErr", func() {
		failpoint.Return(errors.New("mockExecSubtaskInitEnvErr"))
	})
	if err := subtaskExecutor.Init(runCtx); err != nil {
		s.onError(err)
		return s.getError()
	}

	var wg sync.WaitGroup
	cancelCtx, checkCancel := context.WithCancel(ctx)
	s.startCancelCheck(cancelCtx, &wg, runCancel)

	defer func() {
		err := subtaskExecutor.Cleanup(runCtx)
		if err != nil {
			logutil.Logger(s.logCtx).Error("cleanup subtask exec env failed", zap.Error(err))
		}
		checkCancel()
		wg.Wait()
	}()

	subtasks, err := s.taskTable.GetSubtasksInStates(runCtx, s.id, task.ID, task.Step,
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
		if runCtx.Err() != nil {
			logutil.Logger(s.logCtx).Info("taskExecutor runSubtask loop exit")
			break
		}

		subtask, err := s.taskTable.GetFirstSubtaskInStates(runCtx, s.id, task.ID, task.Step,
			proto.TaskStatePending, proto.TaskStateRunning)
		if err != nil {
			logutil.Logger(s.logCtx).Warn("GetFirstSubtaskInStates meets error", zap.Error(err))
			continue
		}
		if subtask == nil {
			failpoint.Inject("breakInTaskExecutorUT", func() {
				failpoint.Break()
			})
			newTask, err := s.taskTable.GetTaskByID(runCtx, task.ID)
			if err != nil {
				logutil.Logger(s.logCtx).Warn("GetTaskByID meets error", zap.Error(err))
				continue
			}
			// When the task move to next step or task state changes, the TaskExecutor should exit.
			if newTask.Step != task.Step || newTask.State != task.State {
				break
			}
			time.Sleep(checkTime)
			continue
		}

		if subtask.State == proto.TaskStateRunning {
			if !s.IsIdempotent(subtask) {
				logutil.Logger(s.logCtx).Info("subtask in running state and is not idempotent, fail it",
					zap.Int64("subtask-id", subtask.ID))
				subtaskErr := errors.New("subtask in running state and is not idempotent")
				s.onError(subtaskErr)
				s.updateSubtaskStateAndError(runCtx, subtask, proto.TaskStateFailed, subtaskErr)
				s.markErrorHandled()
				break
			}
		} else {
			// subtask.State == proto.TaskStatePending
			s.startSubtaskAndUpdateState(runCtx, subtask)
			if err := s.getError(); err != nil {
				logutil.Logger(s.logCtx).Warn("startSubtaskAndUpdateState meets error", zap.Error(err))
				continue
			}
		}

		failpoint.Inject("mockCleanExecutor", func() {
			v, ok := testContexts.Load(s.id)
			if ok {
				if v.(*TestContext).mockDown.Load() {
					failpoint.Break()
				}
			}
		})

		failpoint.Inject("cancelBeforeRunSubtask", func() {
			runCancel(nil)
		})

		s.runSubtask(runCtx, subtaskExecutor, subtask)
	}
	return s.getError()
}

func (s *BaseTaskExecutor) runSubtask(ctx context.Context, subtaskExecutor execute.SubtaskExecutor, subtask *proto.Subtask) {
	err := subtaskExecutor.RunSubtask(ctx, subtask)
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
				err = mgr.CancelTask(ctx, int64(taskID))
				if err != nil {
					logutil.BgLogger().Error("cancel task failed", zap.Error(err))
				}
			}
		}
	})
	s.onSubtaskFinished(ctx, subtaskExecutor, subtask)
}

func (s *BaseTaskExecutor) onSubtaskFinished(ctx context.Context, executor execute.SubtaskExecutor, subtask *proto.Subtask) {
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

// Rollback rollbacks the subtask.
func (s *BaseTaskExecutor) Rollback(ctx context.Context, task *proto.Task) error {
	rollbackCtx, rollbackCancel := context.WithCancelCause(ctx)
	defer rollbackCancel(ErrFinishRollback)
	s.registerCancelFunc(rollbackCancel)

	s.resetError()
	logutil.Logger(s.logCtx).Info("taskExecutor rollback a step", zap.Any("step", task.Step))

	// We should cancel all subtasks before rolling back
	for {
		subtask, err := s.taskTable.GetFirstSubtaskInStates(ctx, s.id, task.ID, task.Step,
			proto.TaskStatePending, proto.TaskStateRunning)
		if err != nil {
			s.onError(err)
			return s.getError()
		}

		if subtask == nil {
			break
		}

		s.updateSubtaskStateAndError(ctx, subtask, proto.TaskStateCanceled, nil)
		if err = s.getError(); err != nil {
			return err
		}
	}

	executor, err := s.GetSubtaskExecutor(ctx, task, nil)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	subtask, err := s.taskTable.GetFirstSubtaskInStates(ctx, s.id, task.ID, task.Step,
		proto.TaskStateRevertPending, proto.TaskStateReverting)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if subtask == nil {
		logutil.BgLogger().Warn("taskExecutor rollback a step, but no subtask in revert_pending state", zap.Any("step", task.Step))
		return nil
	}
	if subtask.State == proto.TaskStateRevertPending {
		s.updateSubtaskStateAndError(ctx, subtask, proto.TaskStateReverting, nil)
	}
	if err := s.getError(); err != nil {
		return err
	}

	// right now all impl of Rollback is empty, so we don't check idempotent here.
	// will try to remove this rollback completely in the future.
	err = executor.Rollback(rollbackCtx)
	if err != nil {
		s.updateSubtaskStateAndError(ctx, subtask, proto.TaskStateRevertFailed, nil)
		s.onError(err)
	} else {
		s.updateSubtaskStateAndError(ctx, subtask, proto.TaskStateReverted, nil)
	}
	return s.getError()
}

// Pause pause the TaskExecutor's subtasks.
func (s *BaseTaskExecutor) Pause(ctx context.Context, task *proto.Task) error {
	logutil.Logger(s.logCtx).Info("taskExecutor pause subtasks")
	// pause all running subtasks.
	if err := s.taskTable.PauseSubtasks(ctx, s.id, task.ID); err != nil {
		s.onError(err)
		return s.getError()
	}
	return nil
}

// Close closes the TaskExecutor when all the subtasks are complete.
func (*BaseTaskExecutor) Close() {
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
		return nil, func() {}, errors.Errorf("taskExecutor option for type %s not found", task.Type)
	}
	if opt.Summary != nil {
		go opt.Summary.UpdateRowCountLoop(ctx, taskMgr)
		return opt.Summary, func() {
			opt.Summary.PersistRowCount(ctx, taskMgr)
		}, nil
	}
	return nil, func() {}, nil
}

func (s *BaseTaskExecutor) registerCancelFunc(cancel context.CancelCauseFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.runtimeCancel = cancel
}

func (s *BaseTaskExecutor) onError(err error) {
	if err == nil {
		return
	}
	err = errors.Trace(err)
	logutil.Logger(s.logCtx).Error("onError", zap.Error(err), zap.Stack("stack"))
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.err == nil {
		s.mu.err = err
		logutil.Logger(s.logCtx).Error("taskExecutor met first error", zap.Error(err))
	}

	if s.mu.runtimeCancel != nil {
		s.mu.runtimeCancel(err)
	}
}

func (s *BaseTaskExecutor) markErrorHandled() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.handled = true
}

func (s *BaseTaskExecutor) getError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.err
}

func (s *BaseTaskExecutor) resetError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.err = nil
	s.mu.handled = false
}

func (s *BaseTaskExecutor) startSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.startSubtask(ctx, subtask.ID)
	subtask.State = proto.TaskStateRunning
	metrics.IncDistTaskSubTaskCnt(subtask)
	metrics.StartDistTaskSubTask(subtask)
}

func (s *BaseTaskExecutor) updateSubtaskStateAndErrorImpl(ctx context.Context, tidbID string, subtaskID int64, state proto.TaskState, subTaskErr error) {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.UpdateSubtaskStateAndError(ctx, tidbID, subtaskID, state, subTaskErr)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseTaskExecutor) startSubtask(ctx context.Context, subtaskID int64) {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.StartSubtask(ctx, subtaskID)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseTaskExecutor) finishSubtask(ctx context.Context, subtask *proto.Subtask) {
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.FinishSubtask(ctx, subtask.ExecID, subtask.ID, subtask.Meta)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseTaskExecutor) updateSubtaskStateAndError(ctx context.Context, subtask *proto.Subtask, state proto.TaskState, subTaskErr error) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.updateSubtaskStateAndErrorImpl(ctx, subtask.ExecID, subtask.ID, state, subTaskErr)
	subtask.State = state
	metrics.IncDistTaskSubTaskCnt(subtask)
	if !subtask.IsFinished() {
		metrics.StartDistTaskSubTask(subtask)
	}
}

func (s *BaseTaskExecutor) finishSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) {
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
func (s *BaseTaskExecutor) markSubTaskCanceledOrFailed(ctx context.Context, subtask *proto.Subtask) bool {
	if err := s.getError(); err != nil {
		err := errors.Cause(err)
		if ctx.Err() != nil && context.Cause(ctx) == ErrCancelSubtask {
			logutil.Logger(s.logCtx).Warn("subtask canceled", zap.Error(err))
			s.updateSubtaskStateAndError(s.ctx, subtask, proto.TaskStateCanceled, nil)
		} else if s.IsRetryableError(err) {
			logutil.Logger(s.logCtx).Warn("met retryable error", zap.Error(err))
		} else if common.IsContextCanceledError(err) {
			logutil.Logger(s.logCtx).Info("met context canceled for gracefully shutdown", zap.Error(err))
		} else {
			logutil.Logger(s.logCtx).Warn("subtask failed", zap.Error(err))
			s.updateSubtaskStateAndError(s.ctx, subtask, proto.TaskStateFailed, err)
		}
		s.markErrorHandled()
		return true
	}
	return false
}

func (s *BaseTaskExecutor) updateErrorToSubtask(ctx context.Context, taskID int64, err error) error {
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err1 := handle.RunWithRetry(s.logCtx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(_ context.Context) (bool, error) {
			return true, s.taskTable.UpdateErrorToSubtask(ctx, s.id, taskID, err)
		},
	)
	if err1 == nil {
		logger.Warn("update error to subtask success", zap.Error(err))
	}
	return err1
}
