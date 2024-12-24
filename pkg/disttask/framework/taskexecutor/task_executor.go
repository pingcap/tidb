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
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	llog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

var (
	// checkBalanceSubtaskInterval is the default check interval for checking
	// subtasks balance to/away from this node.
	checkBalanceSubtaskInterval = 2 * time.Second

	// updateSubtaskSummaryInterval is the interval for updating the subtask summary to
	// subtask table.
	updateSubtaskSummaryInterval = 3 * time.Second
)

var (
	// ErrCancelSubtask is the cancel cause when cancelling subtasks.
	ErrCancelSubtask = errors.New("cancel subtasks")
	// ErrNonIdempotentSubtask means the subtask is left in running state and is not idempotent,
	// so cannot be run again.
	ErrNonIdempotentSubtask = errors.New("subtask in running state and is not idempotent")
)

// BaseTaskExecutor is the base implementation of TaskExecutor.
type BaseTaskExecutor struct {
	// id, it's the same as server id now, i.e. host:port.
	id        string
	task      atomic.Pointer[proto.Task]
	taskTable TaskTable
	logger    *zap.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	Extension

	currSubtaskID atomic.Int64

	mu struct {
		sync.RWMutex
		// runtimeCancel is used to cancel the Run/Rollback when error occurs.
		runtimeCancel context.CancelCauseFunc
	}

	stepExec   execute.StepExecutor
	stepCtx    context.Context
	stepLogger *llog.Task
}

// NewBaseTaskExecutor creates a new BaseTaskExecutor.
// see TaskExecutor.Init for why we want to use task-base to create TaskExecutor.
// TODO: we can refactor this part to pass task base only, but currently ADD-INDEX
// depends on it to init, so we keep it for now.
func NewBaseTaskExecutor(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) *BaseTaskExecutor {
	logger := logutil.ErrVerboseLogger().With(zap.Int64("task-id", task.ID), zap.String("task-type", string(task.Type)))
	if intest.InTest {
		logger = logger.With(zap.String("server-id", id))
	}
	subCtx, cancelFunc := context.WithCancel(ctx)
	taskExecutorImpl := &BaseTaskExecutor{
		id:        id,
		taskTable: taskTable,
		ctx:       subCtx,
		cancel:    cancelFunc,
		logger:    logger,
	}
	taskExecutorImpl.task.Store(task)
	return taskExecutorImpl
}

// checkBalanceSubtask check whether the subtasks are balanced to or away from this node.
//   - If other subtask of `running` state is scheduled to this node, try changed to
//     `pending` state, to make sure subtasks can be balanced later when node scale out.
//   - If current running subtask are scheduled away from this node, i.e. this node
//     is taken as down, cancel running.
func (e *BaseTaskExecutor) checkBalanceSubtask(ctx context.Context) {
	ticker := time.NewTicker(checkBalanceSubtaskInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		task := e.task.Load()
		subtasks, err := e.taskTable.GetSubtasksByExecIDAndStepAndStates(ctx, e.id, task.ID, task.Step,
			proto.SubtaskStateRunning)
		if err != nil {
			e.logger.Error("get subtasks failed", zap.Error(err))
			continue
		}
		if len(subtasks) == 0 {
			e.logger.Info("subtask is scheduled away, cancel running")
			// cancels runStep, but leave the subtask state unchanged.
			e.cancelRunStepWith(nil)
			return
		}

		extraRunningSubtasks := make([]*proto.SubtaskBase, 0, len(subtasks))
		for _, st := range subtasks {
			if st.ID == e.currSubtaskID.Load() {
				continue
			}
			if !e.IsIdempotent(st) {
				if err := e.updateSubtaskStateAndErrorImpl(ctx, st.ExecID, st.ID,
					proto.SubtaskStateFailed, ErrNonIdempotentSubtask); err != nil {
					e.logger.Error("failed to update subtask to 'failed' state", zap.Error(err))
					continue
				}
				// if a subtask fail, scheduler will notice and start revert the
				// task, so we can directly return.
				return
			}
			extraRunningSubtasks = append(extraRunningSubtasks, &st.SubtaskBase)
		}
		if len(extraRunningSubtasks) > 0 {
			if err = e.taskTable.RunningSubtasksBack2Pending(ctx, extraRunningSubtasks); err != nil {
				e.logger.Error("update running subtasks back to pending failed", zap.Error(err))
			} else {
				e.logger.Info("update extra running subtasks back to pending",
					zap.Stringers("subtasks", extraRunningSubtasks))
			}
		}
	}
}

func (e *BaseTaskExecutor) updateSubtaskSummaryLoop(
	checkCtx, runStepCtx context.Context, stepExec execute.StepExecutor) {
	taskMgr := e.taskTable.(*storage.TaskManager)
	ticker := time.NewTicker(updateSubtaskSummaryInterval)
	defer ticker.Stop()
	curSubtaskID := e.currSubtaskID.Load()
	update := func() {
		summary := stepExec.RealtimeSummary()
		err := taskMgr.UpdateSubtaskRowCount(runStepCtx, curSubtaskID, summary.RowCount)
		if err != nil {
			e.logger.Info("update subtask row count failed", zap.Error(err))
		}
	}
	for {
		select {
		case <-checkCtx.Done():
			update()
			return
		case <-ticker.C:
		}
		update()
	}
}

// Init implements the TaskExecutor interface.
func (*BaseTaskExecutor) Init(_ context.Context) error {
	return nil
}

// Ctx returns the context of the task executor.
// TODO: remove it when add-index.taskexecutor.Init don't depends on it.
func (e *BaseTaskExecutor) Ctx() context.Context {
	return e.ctx
}

// Run implements the TaskExecutor interface.
func (e *BaseTaskExecutor) Run(resource *proto.StepResource) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("run task panicked, fail the task", zap.Any("recover", r), zap.Stack("stack"))
			err4Panic := errors.Errorf("%v", r)
			taskBase := e.task.Load()
			e.failOneSubtask(e.ctx, taskBase.ID, err4Panic)
		}
		if e.stepExec != nil {
			e.cleanStepExecutor()
		}
	}()
	// task executor occupies resources, if there's no subtask to run for 10s,
	// we release the resources so that other tasks can use them.
	// 300ms + 600ms + 1.2s + 2s * 4 = 10.1s
	backoffer := backoff.NewExponential(SubtaskCheckInterval, 2, MaxSubtaskCheckInterval)
	checkInterval, noSubtaskCheckCnt := SubtaskCheckInterval, 0
	skipBackoff := false
	for {
		if e.ctx.Err() != nil {
			return
		}
		if !skipBackoff {
			select {
			case <-e.ctx.Done():
				return
			case <-time.After(checkInterval):
			}
		}
		skipBackoff = false
		if err := e.refreshTask(); err != nil {
			if errors.Cause(err) == storage.ErrTaskNotFound {
				return
			}
			e.logger.Error("refresh task failed", zap.Error(err))
			continue
		}
		task := e.task.Load()
		if task.State != proto.TaskStateRunning {
			return
		}

		subtask, err := e.taskTable.GetFirstSubtaskInStates(e.ctx, e.id, task.ID, task.Step,
			proto.SubtaskStatePending, proto.SubtaskStateRunning)
		if err != nil {
			e.logger.Warn("get first subtask meets error", zap.Error(err))
			continue
		} else if subtask == nil {
			if noSubtaskCheckCnt >= maxChecksWhenNoSubtask {
				e.logger.Info("no subtask to run for a while, exit")
				break
			}
			checkInterval = backoffer.Backoff(noSubtaskCheckCnt)
			noSubtaskCheckCnt++
			continue
		}
		// reset it when we get a subtask
		checkInterval, noSubtaskCheckCnt = SubtaskCheckInterval, 0

		if e.stepExec != nil && e.stepExec.GetStep() != subtask.Step {
			e.cleanStepExecutor()
		}
		if e.stepExec == nil {
			if err2 := e.createStepExecutor(resource); err2 != nil {
				e.logger.Error("create step executor failed",
					zap.String("step", proto.Step2Str(task.Type, task.Step)), zap.Error(err2))
				continue
			}
		}
		err = e.runSubtask(subtask)
		if err != nil {
			// task executor keeps running its subtasks even though some subtask
			// might have failed, we rely on scheduler to detect the error, and
			// notify task executor or manager to cancel.
			e.logger.Error("run subtask failed", zap.Error(err))
		} else {
			// if we run a subtask successfully, we will try to run next subtask
			// immediately for once.
			skipBackoff = true
		}
	}
}

func (e *BaseTaskExecutor) createStepExecutor(resource *proto.StepResource) error {
	task := e.task.Load()

	stepExecutor, err := e.GetStepExecutor(task)
	if err != nil {
		e.logger.Info("failed to get step executor", zap.Error(err))
		e.failOneSubtask(e.ctx, task.ID, err)
		return errors.Trace(err)
	}
	execute.SetFrameworkInfo(stepExecutor, task.Step, resource)

	if err := stepExecutor.Init(e.ctx); err != nil {
		if e.IsRetryableError(err) {
			e.logger.Info("meet retryable err when init step executor", zap.Error(err))
		} else {
			e.logger.Info("failed to init step executor", zap.Error(err))
			e.failOneSubtask(e.ctx, task.ID, err)
		}
		return errors.Trace(err)
	}

	stepLogger := llog.BeginTask(e.logger.With(
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
		zap.Float64("mem-limit-percent", gctuner.GlobalMemoryLimitTuner.GetPercentage()),
		zap.String("server-mem-limit", memory.ServerMemoryLimitOriginText.Load()),
		zap.Stringer("resource", resource),
	), "execute task step")

	runStepCtx, runStepCancel := context.WithCancelCause(e.ctx)
	e.stepExec = stepExecutor
	e.stepCtx = runStepCtx
	e.stepLogger = stepLogger

	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.runtimeCancel = runStepCancel

	return nil
}

func (e *BaseTaskExecutor) cleanStepExecutor() {
	if err2 := e.stepExec.Cleanup(e.ctx); err2 != nil {
		e.logger.Error("cleanup subtask exec env failed", zap.Error(err2))
		// Cleanup is not a critical path of running subtask, so no need to
		// affect state of subtasks. there might be no subtask to change even
		// we want to if all subtasks are finished.
	}
	e.stepExec = nil
	e.stepLogger.End(zap.InfoLevel, nil)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.runtimeCancel(nil)
	e.mu.runtimeCancel = nil
}

func (e *BaseTaskExecutor) runSubtask(subtask *proto.Subtask) (resErr error) {
	if subtask.State == proto.SubtaskStateRunning {
		if !e.IsIdempotent(subtask) {
			e.logger.Info("subtask in running state and is not idempotent, fail it",
				zap.Int64("subtask-id", subtask.ID))
			if err := e.updateSubtaskStateAndErrorImpl(e.stepCtx, subtask.ExecID, subtask.ID,
				proto.SubtaskStateFailed, ErrNonIdempotentSubtask); err != nil {
				return err
			}
			return ErrNonIdempotentSubtask
		}
		e.logger.Info("subtask in running state and is idempotent",
			zap.Int64("subtask-id", subtask.ID))
	} else {
		// subtask.State == proto.SubtaskStatePending
		err := e.startSubtask(e.stepCtx, subtask.ID)
		if err != nil {
			// should ignore ErrSubtaskNotFound
			// since it only means that the subtask not owned by current task executor.
			if err != storage.ErrSubtaskNotFound {
				e.logger.Warn("start subtask meets error", zap.Error(err))
			}
			return errors.Trace(err)
		}
	}

	logger := e.logger.With(zap.Int64("subtaskID", subtask.ID), zap.String("step", proto.Step2Str(subtask.Type, subtask.Step)))
	logTask := llog.BeginTask(logger, "run subtask")
	subtaskErr := func() error {
		e.currSubtaskID.Store(subtask.ID)

		var wg util.WaitGroupWrapper
		checkCtx, checkCancel := context.WithCancel(e.stepCtx)
		wg.RunWithLog(func() {
			e.checkBalanceSubtask(checkCtx)
		})

		if e.hasRealtimeSummary(e.stepExec) {
			wg.RunWithLog(func() {
				e.updateSubtaskSummaryLoop(checkCtx, e.stepCtx, e.stepExec)
			})
		}
		defer func() {
			checkCancel()
			wg.Wait()
		}()
		return e.stepExec.RunSubtask(e.stepCtx, subtask)
	}()
	failpoint.InjectCall("afterRunSubtask", e, &subtaskErr)
	logTask.End2(zap.InfoLevel, subtaskErr)

	failpoint.InjectCall("mockTiDBShutdown", e, e.id, e.GetTaskBase())

	if subtaskErr != nil {
		if err := e.markSubTaskCanceledOrFailed(e.stepCtx, subtask, subtaskErr); err != nil {
			logger.Error("failed to handle subtask error", zap.Error(err))
		}
		return subtaskErr
	}

	err := e.finishSubtask(e.stepCtx, subtask)
	failpoint.InjectCall("syncAfterSubtaskFinish")
	return err
}

func (e *BaseTaskExecutor) hasRealtimeSummary(stepExecutor execute.StepExecutor) bool {
	_, ok := e.taskTable.(*storage.TaskManager)
	return ok && stepExecutor.RealtimeSummary() != nil
}

// GetTaskBase implements TaskExecutor.GetTaskBase.
func (e *BaseTaskExecutor) GetTaskBase() *proto.TaskBase {
	task := e.task.Load()
	return &task.TaskBase
}

// CancelRunningSubtask implements TaskExecutor.CancelRunningSubtask.
func (e *BaseTaskExecutor) CancelRunningSubtask() {
	e.cancelRunStepWith(ErrCancelSubtask)
}

// Cancel implements TaskExecutor.Cancel.
func (e *BaseTaskExecutor) Cancel() {
	e.cancel()
}

// Close closes the TaskExecutor when all the subtasks are complete.
func (e *BaseTaskExecutor) Close() {
	e.Cancel()
}

// refreshTask fetch task state from tidb_global_task table.
func (e *BaseTaskExecutor) refreshTask() error {
	oldTask := e.task.Load()
	newTask, err := e.taskTable.GetTaskByID(e.ctx, oldTask.ID)
	if err != nil {
		return err
	}
	e.task.Store(newTask)
	return nil
}

func (e *BaseTaskExecutor) cancelRunStepWith(cause error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mu.runtimeCancel != nil {
		e.mu.runtimeCancel(cause)
	}
}

func (e *BaseTaskExecutor) updateSubtaskStateAndErrorImpl(ctx context.Context, execID string, subtaskID int64, state proto.SubtaskState, subTaskErr error) error {
	start := time.Now()
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(ctx context.Context) (bool, error) {
			return true, e.taskTable.UpdateSubtaskStateAndError(ctx, execID, subtaskID, state, subTaskErr)
		},
	)
	if err != nil {
		e.logger.Error("failed to update subtask state", zap.Int64("subtaskID", subtaskID),
			zap.Stringer("targetState", state), zap.NamedError("subtaskErr", subTaskErr),
			zap.Duration("takes", time.Since(start)), zap.Error(err))
	}
	return err
}

// startSubtask try to change the state of the subtask to running.
// If the subtask is not owned by the task executor,
// the update will fail and task executor should not run the subtask.
func (e *BaseTaskExecutor) startSubtask(ctx context.Context, subtaskID int64) error {
	start := time.Now()
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(ctx context.Context) (bool, error) {
			err := e.taskTable.StartSubtask(ctx, subtaskID, e.id)
			if err == storage.ErrSubtaskNotFound {
				// No need to retry.
				return false, err
			}
			return true, err
		},
	)
	if err != nil && err != storage.ErrSubtaskNotFound {
		e.logger.Error("failed to start subtask", zap.Int64("subtaskID", subtaskID),
			zap.Duration("takes", time.Since(start)), zap.Error(err))
	}
	return err
}

func (e *BaseTaskExecutor) finishSubtask(ctx context.Context, subtask *proto.Subtask) error {
	start := time.Now()
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(ctx context.Context) (bool, error) {
			return true, e.taskTable.FinishSubtask(ctx, subtask.ExecID, subtask.ID, subtask.Meta)
		},
	)
	if err != nil {
		e.logger.Error("failed to finish subtask", zap.Int64("subtaskID", subtask.ID),
			zap.Duration("takes", time.Since(start)), zap.Error(err))
	}
	return err
}

// markSubTaskCanceledOrFailed check the error type and decide the subtasks' state.
// 1. Only cancel subtasks when meet ErrCancelSubtask.
// 2. Only fail subtasks when meet non retryable error.
// 3. When meet other errors, don't change subtasks' state.
func (e *BaseTaskExecutor) markSubTaskCanceledOrFailed(ctx context.Context, subtask *proto.Subtask, stErr error) error {
	if ctx.Err() != nil {
		if context.Cause(ctx) == ErrCancelSubtask {
			e.logger.Warn("subtask canceled")
			return e.updateSubtaskStateAndErrorImpl(e.ctx, subtask.ExecID, subtask.ID, proto.SubtaskStateCanceled, nil)
		}

		e.logger.Info("meet context canceled for gracefully shutdown")
	} else if e.IsRetryableError(stErr) {
		e.logger.Warn("meet retryable error", zap.Error(stErr))
	} else {
		e.logger.Warn("subtask failed", zap.Error(stErr))
		return e.updateSubtaskStateAndErrorImpl(e.ctx, subtask.ExecID, subtask.ID, proto.SubtaskStateFailed, stErr)
	}
	return nil
}

// on fatal error, we randomly fail a subtask to notify scheduler to revert the
// task. we don't return the internal error, what can we do if we failed to handle
// a fatal error?
func (e *BaseTaskExecutor) failOneSubtask(ctx context.Context, taskID int64, subtaskErr error) {
	start := time.Now()
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err1 := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(_ context.Context) (bool, error) {
			return true, e.taskTable.FailSubtask(ctx, e.id, taskID, subtaskErr)
		},
	)
	if err1 != nil {
		e.logger.Error("fail one subtask failed", zap.NamedError("subtaskErr", subtaskErr),
			zap.Duration("takes", time.Since(start)), zap.Error(err1))
	}
}
