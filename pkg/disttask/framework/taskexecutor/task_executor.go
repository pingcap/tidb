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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/lightning/common"
	llog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/intest"
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
	// ErrFinishSubtask is the cancel cause when TaskExecutor successfully processed subtasks.
	ErrFinishSubtask = errors.New("finish subtasks")
	// ErrNonIdempotentSubtask means the subtask is left in running state and is not idempotent,
	// so cannot be run again.
	ErrNonIdempotentSubtask = errors.New("subtask in running state and is not idempotent")

	// MockTiDBDown is used to mock TiDB node down, return true if it's chosen.
	MockTiDBDown func(execID string, task *proto.TaskBase) bool
)

// BaseTaskExecutor is the base implementation of TaskExecutor.
type BaseTaskExecutor struct {
	// id, it's the same as server id now, i.e. host:port.
	id string
	// we only store task base here to reduce overhead of refreshing it.
	// task meta is loaded when we do execute subtasks, see GetStepExecutor.
	taskBase  atomic.Pointer[proto.TaskBase]
	taskTable TaskTable
	logger    *zap.Logger
	ctx       context.Context
	cancel    context.CancelFunc
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
// see TaskExecutor.Init for why we want to use task-base to create TaskExecutor.
// TODO: we can refactor this part to pass task base only, but currently ADD-INDEX
// depends on it to init, so we keep it for now.
func NewBaseTaskExecutor(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) *BaseTaskExecutor {
	logger := log.L().With(zap.Int64("task-id", task.ID), zap.String("task-type", string(task.Type)))
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
	taskExecutorImpl.taskBase.Store(&task.TaskBase)
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

		task := e.taskBase.Load()
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
				e.updateSubtaskStateAndErrorImpl(ctx, st.ExecID, st.ID, proto.SubtaskStateFailed, ErrNonIdempotentSubtask)
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
	var err error
	// task executor occupies resources, if there's no subtask to run for 10s,
	// we release the resources so that other tasks can use them.
	// 300ms + 600ms + 1.2s + 2s * 4 = 10.1s
	backoffer := backoff.NewExponential(SubtaskCheckInterval, 2, MaxSubtaskCheckInterval)
	checkInterval, noSubtaskCheckCnt := SubtaskCheckInterval, 0
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-time.After(checkInterval):
		}
		if err = e.refreshTask(); err != nil {
			if errors.Cause(err) == storage.ErrTaskNotFound {
				return
			}
			e.logger.Error("refresh task failed", zap.Error(err))
			continue
		}
		task := e.taskBase.Load()
		if task.State != proto.TaskStateRunning {
			return
		}
		if exist, err := e.taskTable.HasSubtasksInStates(e.ctx, e.id, task.ID, task.Step,
			unfinishedSubtaskStates...); err != nil {
			e.logger.Error("check whether there are subtasks to run failed", zap.Error(err))
			continue
		} else if !exist {
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
		err = e.RunStep(resource)
		if err != nil {
			e.logger.Error("failed to handle task", zap.Error(err))
		}
	}
}

// RunStep start to fetch and run all subtasks for the step of task on the node.
// return if there's no subtask to run.
func (e *BaseTaskExecutor) RunStep(resource *proto.StepResource) (err error) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("BaseTaskExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
			err4Panic := errors.Errorf("%v", r)
			err1 := e.updateSubtask(err4Panic)
			if err == nil {
				err = err1
			}
		}
	}()
	err = e.runStep(resource)
	if e.mu.handled {
		return err
	}
	if err == nil {
		// may have error in
		// 1. defer function in run(ctx, task)
		// 2. cancel ctx
		// TODO: refine onError/getError
		if e.getError() != nil {
			err = e.getError()
		} else if e.ctx.Err() != nil {
			err = e.ctx.Err()
		} else {
			return nil
		}
	}

	return e.updateSubtask(err)
}

func (e *BaseTaskExecutor) runStep(resource *proto.StepResource) (resErr error) {
	runStepCtx, runStepCancel := context.WithCancelCause(e.ctx)
	e.registerRunStepCancelFunc(runStepCancel)
	defer func() {
		runStepCancel(ErrFinishSubtask)
		e.unregisterRunStepCancelFunc()
	}()
	e.resetError()
	taskBase := e.taskBase.Load()
	task, err := e.taskTable.GetTaskByID(e.ctx, taskBase.ID)
	if err != nil {
		e.onError(err)
		return e.getError()
	}
	stepLogger := llog.BeginTask(e.logger.With(
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
		zap.Float64("mem-limit-percent", gctuner.GlobalMemoryLimitTuner.GetPercentage()),
		zap.String("server-mem-limit", memory.ServerMemoryLimitOriginText.Load()),
		zap.Stringer("resource", resource),
	), "execute task step")
	// log as info level, subtask might be cancelled, let caller check it.
	defer func() {
		stepLogger.End(zap.InfoLevel, resErr)
	}()

	stepExecutor, err := e.GetStepExecutor(task)
	if err != nil {
		e.onError(err)
		return e.getError()
	}
	execute.SetFrameworkInfo(stepExecutor, resource)

	failpoint.Inject("mockExecSubtaskInitEnvErr", func() {
		failpoint.Return(errors.New("mockExecSubtaskInitEnvErr"))
	})
	if err := stepExecutor.Init(runStepCtx); err != nil {
		e.onError(err)
		return e.getError()
	}

	defer func() {
		err := stepExecutor.Cleanup(runStepCtx)
		if err != nil {
			e.logger.Error("cleanup subtask exec env failed", zap.Error(err))
			e.onError(err)
		}
	}()

	for {
		// check if any error occurs.
		if err := e.getError(); err != nil {
			break
		}
		if runStepCtx.Err() != nil {
			break
		}

		subtask, err := e.taskTable.GetFirstSubtaskInStates(runStepCtx, e.id, task.ID, task.Step,
			proto.SubtaskStatePending, proto.SubtaskStateRunning)
		if err != nil {
			e.logger.Warn("GetFirstSubtaskInStates meets error", zap.Error(err))
			continue
		}
		if subtask == nil {
			break
		}

		if subtask.State == proto.SubtaskStateRunning {
			if !e.IsIdempotent(subtask) {
				e.logger.Info("subtask in running state and is not idempotent, fail it",
					zap.Int64("subtask-id", subtask.ID))
				e.onError(ErrNonIdempotentSubtask)
				e.updateSubtaskStateAndErrorImpl(runStepCtx, subtask.ExecID, subtask.ID, proto.SubtaskStateFailed, ErrNonIdempotentSubtask)
				e.markErrorHandled()
				break
			}
			e.logger.Info("subtask in running state and is idempotent",
				zap.Int64("subtask-id", subtask.ID))
		} else {
			// subtask.State == proto.SubtaskStatePending
			err := e.startSubtask(runStepCtx, subtask.ID)
			if err != nil {
				e.logger.Warn("startSubtask meets error", zap.Error(err))
				// should ignore ErrSubtaskNotFound
				// since it only means that the subtask not owned by current task executor.
				if err == storage.ErrSubtaskNotFound {
					continue
				}
				e.onError(err)
				continue
			}
		}

		failpoint.Inject("cancelBeforeRunSubtask", func() {
			runStepCancel(nil)
		})

		e.runSubtask(runStepCtx, stepExecutor, subtask)
	}
	return e.getError()
}

func (e *BaseTaskExecutor) hasRealtimeSummary(stepExecutor execute.StepExecutor) bool {
	_, ok := e.taskTable.(*storage.TaskManager)
	return ok && stepExecutor.RealtimeSummary() != nil
}

func (e *BaseTaskExecutor) runSubtask(ctx context.Context, stepExecutor execute.StepExecutor, subtask *proto.Subtask) {
	err := func() error {
		e.currSubtaskID.Store(subtask.ID)

		var wg util.WaitGroupWrapper
		checkCtx, checkCancel := context.WithCancel(ctx)
		wg.RunWithLog(func() {
			e.checkBalanceSubtask(checkCtx)
		})

		if e.hasRealtimeSummary(stepExecutor) {
			wg.RunWithLog(func() {
				e.updateSubtaskSummaryLoop(checkCtx, ctx, stepExecutor)
			})
		}
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
		e.onError(err)
	}

	finished := e.markSubTaskCanceledOrFailed(ctx, subtask)
	if finished {
		return
	}

	failpoint.Inject("mockTiDBShutdown", func() {
		if MockTiDBDown(e.id, e.GetTaskBase()) {
			failpoint.Return()
		}
	})

	failpoint.Inject("MockExecutorRunErr", func(val failpoint.Value) {
		if val.(bool) {
			e.onError(errors.New("MockExecutorRunErr"))
		}
	})
	failpoint.Inject("MockExecutorRunCancel", func(val failpoint.Value) {
		if taskID, ok := val.(int); ok {
			mgr, err := storage.GetTaskManager()
			if err != nil {
				e.logger.Error("get task manager failed", zap.Error(err))
			} else {
				err = mgr.CancelTask(ctx, int64(taskID))
				if err != nil {
					e.logger.Error("cancel task failed", zap.Error(err))
				}
			}
		}
	})
	e.onSubtaskFinished(ctx, stepExecutor, subtask)
}

func (e *BaseTaskExecutor) onSubtaskFinished(ctx context.Context, executor execute.StepExecutor, subtask *proto.Subtask) {
	if err := e.getError(); err == nil {
		if err = executor.OnFinished(ctx, subtask); err != nil {
			e.onError(err)
		}
	}
	failpoint.Inject("MockSubtaskFinishedCancel", func(val failpoint.Value) {
		if val.(bool) {
			e.onError(ErrCancelSubtask)
		}
	})

	finished := e.markSubTaskCanceledOrFailed(ctx, subtask)
	if finished {
		return
	}

	e.finishSubtask(ctx, subtask)

	finished = e.markSubTaskCanceledOrFailed(ctx, subtask)
	if finished {
		return
	}

	failpoint.InjectCall("syncAfterSubtaskFinish")
}

// GetTaskBase implements TaskExecutor.GetTaskBase.
func (e *BaseTaskExecutor) GetTaskBase() *proto.TaskBase {
	return e.taskBase.Load()
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
	task := e.GetTaskBase()
	newTaskBase, err := e.taskTable.GetTaskBaseByID(e.ctx, task.ID)
	if err != nil {
		return err
	}
	e.taskBase.Store(newTaskBase)
	return nil
}

func (e *BaseTaskExecutor) registerRunStepCancelFunc(cancel context.CancelCauseFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.runtimeCancel = cancel
}

func (e *BaseTaskExecutor) unregisterRunStepCancelFunc() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.runtimeCancel = nil
}

func (e *BaseTaskExecutor) cancelRunStepWith(cause error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mu.runtimeCancel != nil {
		e.mu.runtimeCancel(cause)
	}
}

func (e *BaseTaskExecutor) onError(err error) {
	if err == nil {
		return
	}
	err = errors.Trace(err)
	e.logger.Error("onError", zap.Error(err), zap.Stack("stack"))
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.mu.err == nil {
		e.mu.err = err
		e.logger.Error("taskExecutor met first error", zap.Error(err))
	}

	if e.mu.runtimeCancel != nil {
		e.mu.runtimeCancel(err)
	}
}

func (e *BaseTaskExecutor) markErrorHandled() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.handled = true
}

func (e *BaseTaskExecutor) getError() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.mu.err
}

func (e *BaseTaskExecutor) resetError() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.err = nil
	e.mu.handled = false
}

func (e *BaseTaskExecutor) updateSubtaskStateAndErrorImpl(ctx context.Context, execID string, subtaskID int64, state proto.SubtaskState, subTaskErr error) {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(ctx context.Context) (bool, error) {
			return true, e.taskTable.UpdateSubtaskStateAndError(ctx, execID, subtaskID, state, subTaskErr)
		},
	)
	if err != nil {
		e.onError(err)
	}
}

// startSubtask try to change the state of the subtask to running.
// If the subtask is not owned by the task executor,
// the update will fail and task executor should not run the subtask.
func (e *BaseTaskExecutor) startSubtask(ctx context.Context, subtaskID int64) error {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(ctx context.Context) (bool, error) {
			err := e.taskTable.StartSubtask(ctx, subtaskID, e.id)
			if err == storage.ErrSubtaskNotFound {
				// No need to retry.
				return false, err
			}
			return true, err
		},
	)
}

func (e *BaseTaskExecutor) finishSubtask(ctx context.Context, subtask *proto.Subtask) {
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(ctx context.Context) (bool, error) {
			return true, e.taskTable.FinishSubtask(ctx, subtask.ExecID, subtask.ID, subtask.Meta)
		},
	)
	if err != nil {
		e.onError(err)
	}
}

// markSubTaskCanceledOrFailed check the error type and decide the subtasks' state.
// 1. Only cancel subtasks when meet ErrCancelSubtask.
// 2. Only fail subtasks when meet non retryable error.
// 3. When meet other errors, don't change subtasks' state.
func (e *BaseTaskExecutor) markSubTaskCanceledOrFailed(ctx context.Context, subtask *proto.Subtask) bool {
	if err := e.getError(); err != nil {
		err := errors.Cause(err)
		if ctx.Err() != nil && context.Cause(ctx) == ErrCancelSubtask {
			e.logger.Warn("subtask canceled", zap.Error(err))
			e.updateSubtaskStateAndErrorImpl(e.ctx, subtask.ExecID, subtask.ID, proto.SubtaskStateCanceled, nil)
		} else if e.IsRetryableError(err) {
			e.logger.Warn("meet retryable error", zap.Error(err))
		} else if common.IsContextCanceledError(err) {
			e.logger.Info("meet context canceled for gracefully shutdown", zap.Error(err))
		} else {
			e.logger.Warn("subtask failed", zap.Error(err))
			e.updateSubtaskStateAndErrorImpl(e.ctx, subtask.ExecID, subtask.ID, proto.SubtaskStateFailed, err)
		}
		e.markErrorHandled()
		return true
	}
	return false
}

func (e *BaseTaskExecutor) failSubtaskWithRetry(ctx context.Context, taskID int64, err error) error {
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err1 := handle.RunWithRetry(e.ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(_ context.Context) (bool, error) {
			return true, e.taskTable.FailSubtask(ctx, e.id, taskID, err)
		},
	)
	if err1 == nil {
		e.logger.Info("failed one subtask succeed", zap.NamedError("subtask-err", err))
	}
	return err1
}

func (e *BaseTaskExecutor) cancelSubtaskWithRetry(ctx context.Context, taskID int64, err error) error {
	e.logger.Warn("subtask canceled", zap.NamedError("subtask-cancel", err))
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err1 := handle.RunWithRetry(e.ctx, scheduler.RetrySQLTimes, backoffer, e.logger,
		func(_ context.Context) (bool, error) {
			return true, e.taskTable.CancelSubtask(ctx, e.id, taskID)
		},
	)
	if err1 == nil {
		e.logger.Info("canceled one subtask succeed", zap.NamedError("subtask-cancel", err))
	}
	return err1
}

// updateSubtask check the error type and decide the subtasks' state.
// 1. Only cancel subtasks when meet ErrCancelSubtask.
// 2. Only fail subtasks when meet non retryable error.
// 3. When meet other errors, don't change subtasks' state.
// Handled errors should not happen during subtasks execution.
// Only handle errors before subtasks execution and after subtasks execution.
func (e *BaseTaskExecutor) updateSubtask(err error) error {
	task := e.taskBase.Load()
	err = errors.Cause(err)
	// TODO this branch is unreachable now, remove it when we refactor error handling.
	if e.ctx.Err() != nil && context.Cause(e.ctx) == ErrCancelSubtask {
		return e.cancelSubtaskWithRetry(e.ctx, task.ID, ErrCancelSubtask)
	} else if e.IsRetryableError(err) {
		e.logger.Warn("meet retryable error", zap.Error(err))
	} else if common.IsContextCanceledError(err) {
		e.logger.Info("meet context canceled for gracefully shutdown", zap.Error(err))
	} else {
		return e.failSubtaskWithRetry(e.ctx, task.ID, err)
	}
	return nil
}
