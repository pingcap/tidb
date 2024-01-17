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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

var (
	// checkBalanceSubtaskInterval is the default check interval for checking
	// subtasks balance to/away from this node.
	checkBalanceSubtaskInterval = 2 * time.Second
)

var (
	// ErrCancelSubtask is the cancel cause when cancelling subtasks.
	ErrCancelSubtask = errors.New("cancel subtasks")
	// ErrFinishSubtask is the cancel cause when TaskExecutor successfully processed subtasks.
	ErrFinishSubtask = errors.New("finish subtasks")
	// ErrFinishRollback is the cancel cause when TaskExecutor rollback successfully.
	ErrFinishRollback = errors.New("finish rollback")
	// ErrNonIdempotentSubtask means the subtask is left in running state and is not idempotent,
	// so cannot be run again.
	ErrNonIdempotentSubtask = errors.New("subtask in running state and is not idempotent")

	// TestSyncChan is used to sync the test.
	TestSyncChan = make(chan struct{})
)

// BaseTaskExecutor is the base implementation of TaskExecutor.
type BaseTaskExecutor struct {
	// id, it's the same as server id now, i.e. host:port.
	id        string
	task      atomic.Pointer[proto.Task]
	taskTable TaskTable
	logCtx    context.Context
	// ctx from manager
	ctx context.Context
	Extension

	currSubtaskID atomic.Int64

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
func NewBaseTaskExecutor(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) *BaseTaskExecutor {
	taskExecutorImpl := &BaseTaskExecutor{
		id:        id,
		taskTable: taskTable,
		ctx:       ctx,
		logCtx:    logutil.WithFields(context.Background(), zap.Int64("task-id", task.ID)),
	}
	taskExecutorImpl.task.Store(task)
	return taskExecutorImpl
}

// checkBalanceSubtask check whether the subtasks are balanced to or away from this node.
//   - If other subtask of `running` state is scheduled to this node, try changed to
//     `pending` state, to make sure subtasks can be balanced later when node scale out.
//   - If current running subtask are scheduled away from this node, i.e. this node
//     is taken as down, cancel running.
func (s *BaseTaskExecutor) checkBalanceSubtask(ctx context.Context) {
	ticker := time.NewTicker(checkBalanceSubtaskInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		task := s.task.Load()
		subtasks, err := s.taskTable.GetSubtasksByExecIDAndStepAndStates(ctx, s.id, task.ID, task.Step,
			proto.SubtaskStateRunning)
		if err != nil {
			logutil.Logger(s.logCtx).Error("get subtasks failed", zap.Error(err))
			continue
		}
		if len(subtasks) == 0 {
			logutil.Logger(s.logCtx).Info("subtask is scheduled away, cancel running")
			s.cancelRunStep()
			return
		}

		extraRunningSubtasks := make([]*proto.Subtask, 0, len(subtasks))
		for _, st := range subtasks {
			if st.ID == s.currSubtaskID.Load() {
				continue
			}
			if !s.IsIdempotent(st) {
				s.updateSubtaskStateAndError(ctx, st, proto.SubtaskStateFailed, ErrNonIdempotentSubtask)
				return
			}
			extraRunningSubtasks = append(extraRunningSubtasks, st)
		}
		if len(extraRunningSubtasks) > 0 {
			if err = s.taskTable.RunningSubtasksBack2Pending(ctx, extraRunningSubtasks); err != nil {
				logutil.Logger(s.logCtx).Error("update running subtasks back to pending failed", zap.Error(err))
			}
		}
	}
}

// Init implements the TaskExecutor interface.
func (*BaseTaskExecutor) Init(_ context.Context) error {
	return nil
}

// RunStep start to fetch and run all subtasks for the step of task on the node.
func (s *BaseTaskExecutor) RunStep(ctx context.Context, task *proto.Task, resource *proto.StepResource) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(s.logCtx).Error("BaseTaskExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
			err4Panic := errors.Errorf("%v", r)
			err1 := s.updateSubtask(ctx, task.ID, err4Panic)
			if err == nil {
				err = err1
			}
		}
	}()
	// TODO: we can centralized this when we move handleExecutableTask loop here.
	s.task.Store(task)
	err = s.runStep(ctx, task, resource)
	if s.mu.handled {
		return err
	}
	if err == nil {
		// may have error in
		// 1. defer function in run(ctx, task)
		// 2. cancel ctx
		// TODO: refine onError/getError
		if s.getError() != nil {
			err = s.getError()
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			return nil
		}
	}

	return s.updateSubtask(ctx, task.ID, err)
}

func (s *BaseTaskExecutor) runStep(ctx context.Context, task *proto.Task, resource *proto.StepResource) (resErr error) {
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
	), "execute task")
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
	stepExecutor, err := s.GetStepExecutor(ctx, task, summary, resource)
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	failpoint.Inject("mockExecSubtaskInitEnvErr", func() {
		failpoint.Return(errors.New("mockExecSubtaskInitEnvErr"))
	})
	if err := stepExecutor.Init(runCtx); err != nil {
		s.onError(err)
		return s.getError()
	}

	defer func() {
		err := stepExecutor.Cleanup(runCtx)
		if err != nil {
			logutil.Logger(s.logCtx).Error("cleanup subtask exec env failed", zap.Error(err))
			s.onError(err)
		}
	}()

	subtasks, err := s.taskTable.GetSubtasksByExecIDAndStepAndStates(
		runCtx, s.id, task.ID, task.Step,
		proto.SubtaskStatePending, proto.SubtaskStateRunning)
	if err != nil {
		s.onError(err)
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
			proto.SubtaskStatePending, proto.SubtaskStateRunning)
		if err != nil {
			logutil.Logger(s.logCtx).Warn("GetFirstSubtaskInStates meets error", zap.Error(err))
			continue
		}
		if subtask == nil {
			failpoint.Inject("breakInTaskExecutorUT", func() {
				failpoint.Break()
			})
			newTask, err := s.taskTable.GetTaskByID(runCtx, task.ID)
			// When the task move history table of not found, the task executor should exit.
			if err == storage.ErrTaskNotFound {
				break
			}
			if err != nil {
				logutil.Logger(s.logCtx).Warn("GetTaskByID meets error", zap.Error(err))
				continue
			}
			// When the task move to next step or task state changes, the task executor should exit.
			if newTask.Step != task.Step || newTask.State != task.State {
				break
			}
			time.Sleep(checkTime)
			continue
		}

		if subtask.State == proto.SubtaskStateRunning {
			if !s.IsIdempotent(subtask) {
				logutil.Logger(s.logCtx).Info("subtask in running state and is not idempotent, fail it",
					zap.Int64("subtask-id", subtask.ID))
				s.onError(ErrNonIdempotentSubtask)
				s.updateSubtaskStateAndError(runCtx, subtask, proto.SubtaskStateFailed, ErrNonIdempotentSubtask)
				s.markErrorHandled()
				break
			}
		} else {
			// subtask.State == proto.SubtaskStatePending
			err := s.startSubtaskAndUpdateState(runCtx, subtask)
			if err != nil {
				logutil.Logger(s.logCtx).Warn("startSubtaskAndUpdateState meets error", zap.Error(err))
				// should ignore ErrSubtaskNotFound
				// since the err only indicate that the subtask not owned by current task executor.
				if err == storage.ErrSubtaskNotFound {
					continue
				}
				s.onError(err)
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

		s.runSubtask(runCtx, stepExecutor, subtask)
	}
	return s.getError()
}

func (s *BaseTaskExecutor) runSubtask(ctx context.Context, stepExecutor execute.StepExecutor, subtask *proto.Subtask) {
	err := func() error {
		s.currSubtaskID.Store(subtask.ID)

		var wg util.WaitGroupWrapper
		checkCtx, checkCancel := context.WithCancel(ctx)
		wg.Go(func() {
			s.checkBalanceSubtask(checkCtx)
		})
		defer func() {
			checkCancel()
			wg.Wait()
		}()

		return stepExecutor.RunSubtask(ctx, subtask)
	}()
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
			infosync.MockGlobalServerInfoManagerEntry.DeleteByExecID(s.id)
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
	s.onSubtaskFinished(ctx, stepExecutor, subtask)
}

func (s *BaseTaskExecutor) onSubtaskFinished(ctx context.Context, executor execute.StepExecutor, subtask *proto.Subtask) {
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
	// TODO: we can centralized this when we move handleExecutableTask loop here.
	s.task.Store(task)
	rollbackCtx, rollbackCancel := context.WithCancelCause(ctx)
	defer rollbackCancel(ErrFinishRollback)
	s.registerCancelFunc(rollbackCancel)

	s.resetError()
	logutil.Logger(s.logCtx).Info("taskExecutor rollback a step", zap.Any("step", task.Step))

	// We should cancel all subtasks before rolling back
	for {
		subtask, err := s.taskTable.GetFirstSubtaskInStates(ctx, s.id, task.ID, task.Step,
			proto.SubtaskStatePending, proto.SubtaskStateRunning)
		if err != nil {
			s.onError(err)
			return s.getError()
		}

		if subtask == nil {
			break
		}

		s.updateSubtaskStateAndError(ctx, subtask, proto.SubtaskStateCanceled, nil)
		if err = s.getError(); err != nil {
			return err
		}
	}

	executor, err := s.GetStepExecutor(ctx, task, nil, nil)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	subtask, err := s.taskTable.GetFirstSubtaskInStates(ctx, s.id, task.ID, task.Step,
		proto.SubtaskStateRevertPending, proto.SubtaskStateReverting)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if subtask == nil {
		logutil.BgLogger().Warn("taskExecutor rollback a step, but no subtask in revert_pending state", zap.Any("step", task.Step))
		return nil
	}
	if subtask.State == proto.SubtaskStateRevertPending {
		s.updateSubtaskStateAndError(ctx, subtask, proto.SubtaskStateReverting, nil)
	}
	if err := s.getError(); err != nil {
		return err
	}

	// right now all impl of Rollback is empty, so we don't check idempotent here.
	// will try to remove this rollback completely in the future.
	err = executor.Rollback(rollbackCtx)
	if err != nil {
		s.updateSubtaskStateAndError(ctx, subtask, proto.SubtaskStateRevertFailed, nil)
		s.onError(err)
	} else {
		s.updateSubtaskStateAndError(ctx, subtask, proto.SubtaskStateReverted, nil)
	}
	return s.getError()
}

// Close closes the TaskExecutor when all the subtasks are complete.
func (*BaseTaskExecutor) Close() {
}

func runSummaryCollectLoop(
	ctx context.Context,
	task *proto.Task,
	taskTable TaskTable,
) (summary *execute.Summary, cleanup func(), err error) {
	failpoint.Inject("mockSummaryCollectErr", func() {
		failpoint.Return(nil, func() {}, errors.New("summary collect err"))
	})
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

// cancelRunStep cancels runStep, i.e. the running process, but leave the subtask
// state unchanged.
func (s *BaseTaskExecutor) cancelRunStep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.runtimeCancel != nil {
		s.mu.runtimeCancel(nil)
	}
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

func (s *BaseTaskExecutor) startSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) error {
	err := s.startSubtask(ctx, subtask.ID)
	if err == nil {
		metrics.DecDistTaskSubTaskCnt(subtask)
		metrics.EndDistTaskSubTask(subtask)
		subtask.State = proto.SubtaskStateRunning
		metrics.IncDistTaskSubTaskCnt(subtask)
		metrics.StartDistTaskSubTask(subtask)
	}
	return err
}

func (s *BaseTaskExecutor) updateSubtaskStateAndErrorImpl(ctx context.Context, execID string, subtaskID int64, state proto.SubtaskState, subTaskErr error) {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.UpdateSubtaskStateAndError(ctx, execID, subtaskID, state, subTaskErr)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

// startSubtask try to change the state of the subtask to running.
// If the subtask is not owned by the task executor,
// the update will fail and task executor should not run the subtask.
func (s *BaseTaskExecutor) startSubtask(ctx context.Context, subtaskID int64) error {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			err := s.taskTable.StartSubtask(ctx, subtaskID, s.id)
			if err == storage.ErrSubtaskNotFound {
				// No need to retry.
				return false, err
			}
			return true, err
		},
	)
}

func (s *BaseTaskExecutor) finishSubtask(ctx context.Context, subtask *proto.Subtask) {
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.FinishSubtask(ctx, subtask.ExecID, subtask.ID, subtask.Meta)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseTaskExecutor) updateSubtaskStateAndError(ctx context.Context, subtask *proto.Subtask, state proto.SubtaskState, subTaskErr error) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.updateSubtaskStateAndErrorImpl(ctx, subtask.ExecID, subtask.ID, state, subTaskErr)
	subtask.State = state
	metrics.IncDistTaskSubTaskCnt(subtask)
	if !subtask.IsDone() {
		metrics.StartDistTaskSubTask(subtask)
	}
}

func (s *BaseTaskExecutor) finishSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.finishSubtask(ctx, subtask)
	subtask.State = proto.SubtaskStateSucceed
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
			s.updateSubtaskStateAndError(s.ctx, subtask, proto.SubtaskStateCanceled, nil)
		} else if s.IsRetryableError(err) {
			logutil.Logger(s.logCtx).Warn("meet retryable error", zap.Error(err))
		} else if common.IsContextCanceledError(err) {
			logutil.Logger(s.logCtx).Info("meet context canceled for gracefully shutdown", zap.Error(err))
		} else {
			logutil.Logger(s.logCtx).Warn("subtask failed", zap.Error(err))
			s.updateSubtaskStateAndError(s.ctx, subtask, proto.SubtaskStateFailed, err)
		}
		s.markErrorHandled()
		return true
	}
	return false
}

func (s *BaseTaskExecutor) failSubtaskWithRetry(ctx context.Context, taskID int64, err error) error {
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err1 := handle.RunWithRetry(s.logCtx, scheduler.RetrySQLTimes, backoffer, logger,
		func(_ context.Context) (bool, error) {
			return true, s.taskTable.FailSubtask(ctx, s.id, taskID, err)
		},
	)
	if err1 == nil {
		logger.Info("failed one subtask succeed", zap.NamedError("subtask-err", err))
	}
	return err1
}

func (s *BaseTaskExecutor) cancelSubtaskWithRetry(ctx context.Context, taskID int64, err error) error {
	logutil.Logger(s.logCtx).Warn("subtask canceled", zap.NamedError("subtask-cancel", err))
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err1 := handle.RunWithRetry(s.logCtx, scheduler.RetrySQLTimes, backoffer, logger,
		func(_ context.Context) (bool, error) {
			return true, s.taskTable.CancelSubtask(ctx, s.id, taskID)
		},
	)
	if err1 == nil {
		logger.Info("canceled one subtask succeed", zap.NamedError("subtask-cancel", err))
	}
	return err1
}

// updateSubtask check the error type and decide the subtasks' state.
// 1. Only cancel subtasks when meet ErrCancelSubtask.
// 2. Only fail subtasks when meet non retryable error.
// 3. When meet other errors, don't change subtasks' state.
// Handled errors should not happened during subtasks execution.
// Only handle errors before subtasks execution and after subtasks execution.
func (s *BaseTaskExecutor) updateSubtask(ctx context.Context, taskID int64, err error) error {
	err = errors.Cause(err)
	logger := logutil.Logger(s.logCtx)
	if ctx.Err() != nil && context.Cause(ctx) == ErrCancelSubtask {
		return s.cancelSubtaskWithRetry(ctx, taskID, ErrCancelSubtask)
	} else if s.IsRetryableError(err) {
		logger.Warn("meet retryable error", zap.Error(err))
	} else if common.IsContextCanceledError(err) {
		logger.Info("meet context canceled for gracefully shutdown", zap.Error(err))
	} else {
		return s.failSubtaskWithRetry(ctx, taskID, err)
	}
	return nil
}
