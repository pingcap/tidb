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
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	llog "github.com/pingcap/tidb/br/pkg/lightning/log"
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
	logger    *zap.Logger
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
		logger: log.L().With(zap.Int64("task-id", task.ID),
			zap.String("task-type", string(task.Type))),
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
			e.cancelRunStep()
			return
		}

		extraRunningSubtasks := make([]*proto.Subtask, 0, len(subtasks))
		for _, st := range subtasks {
			if st.ID == e.currSubtaskID.Load() {
				continue
			}
			if !e.IsIdempotent(st) {
				e.updateSubtaskStateAndError(ctx, st, proto.SubtaskStateFailed, ErrNonIdempotentSubtask)
				return
			}
			extraRunningSubtasks = append(extraRunningSubtasks, st)
		}
		if len(extraRunningSubtasks) > 0 {
			if err = e.taskTable.RunningSubtasksBack2Pending(ctx, extraRunningSubtasks); err != nil {
				e.logger.Error("update running subtasks back to pending failed", zap.Error(err))
			}
		}
	}
}

// Init implements the TaskExecutor interface.
func (*BaseTaskExecutor) Init(_ context.Context) error {
	return nil
}

// RunStep start to fetch and run all subtasks for the step of task on the node.
func (e *BaseTaskExecutor) RunStep(ctx context.Context, task *proto.Task, resource *proto.StepResource) (err error) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("BaseTaskExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
			err4Panic := errors.Errorf("%v", r)
			err1 := e.updateSubtask(ctx, task.ID, err4Panic)
			if err == nil {
				err = err1
			}
		}
	}()
	// TODO: we can centralized this when we move handleExecutableTask loop here.
	e.task.Store(task)
	err = e.runStep(ctx, task, resource)
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
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			return nil
		}
	}

	return e.updateSubtask(ctx, task.ID, err)
}

func (e *BaseTaskExecutor) runStep(ctx context.Context, task *proto.Task, resource *proto.StepResource) (resErr error) {
	if ctx.Err() != nil {
		e.onError(ctx.Err())
		return e.getError()
	}
	runCtx, runCancel := context.WithCancelCause(ctx)
	defer runCancel(ErrFinishSubtask)
	e.registerCancelFunc(runCancel)
	e.resetError()
	stepLogger := llog.BeginTask(e.logger.With(
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
		zap.Int("concurrency", task.Concurrency),
		zap.Float64("mem-limit-percent", gctuner.GlobalMemoryLimitTuner.GetPercentage()),
		zap.String("server-mem-limit", memory.ServerMemoryLimitOriginText.Load()),
	), "execute task")
	// log as info level, subtask might be cancelled, let caller check it.
	defer func() {
		stepLogger.End(zap.InfoLevel, resErr)
	}()

	summary, cleanup, err := runSummaryCollectLoop(ctx, task, e.taskTable)
	if err != nil {
		e.onError(err)
		return e.getError()
	}
	defer cleanup()
	stepExecutor, err := e.GetStepExecutor(ctx, task, summary, resource)
	if err != nil {
		e.onError(err)
		return e.getError()
	}

	failpoint.Inject("mockExecSubtaskInitEnvErr", func() {
		failpoint.Return(errors.New("mockExecSubtaskInitEnvErr"))
	})
	if err := stepExecutor.Init(runCtx); err != nil {
		e.onError(err)
		return e.getError()
	}

	defer func() {
		err := stepExecutor.Cleanup(runCtx)
		if err != nil {
			e.logger.Error("cleanup subtask exec env failed", zap.Error(err))
			e.onError(err)
		}
	}()

	subtasks, err := e.taskTable.GetSubtasksByExecIDAndStepAndStates(
		runCtx, e.id, task.ID, task.Step,
		proto.SubtaskStatePending, proto.SubtaskStateRunning)
	if err != nil {
		e.onError(err)
		return e.getError()
	}
	for _, subtask := range subtasks {
		metrics.IncDistTaskSubTaskCnt(subtask)
		metrics.StartDistTaskSubTask(subtask)
	}

	for {
		// check if any error occurs.
		if err := e.getError(); err != nil {
			break
		}
		if runCtx.Err() != nil {
			e.logger.Info("taskExecutor runSubtask loop exit")
			break
		}

		subtask, err := e.taskTable.GetFirstSubtaskInStates(runCtx, e.id, task.ID, task.Step,
			proto.SubtaskStatePending, proto.SubtaskStateRunning)
		if err != nil {
			e.logger.Warn("GetFirstSubtaskInStates meets error", zap.Error(err))
			continue
		}
		if subtask == nil {
			failpoint.Inject("breakInTaskExecutorUT", func() {
				failpoint.Break()
			})
			newTask, err := e.taskTable.GetTaskByID(runCtx, task.ID)
			// When the task move history table of not found, the task executor should exit.
			if err == storage.ErrTaskNotFound {
				break
			}
			if err != nil {
				e.logger.Warn("GetTaskByID meets error", zap.Error(err))
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
			if !e.IsIdempotent(subtask) {
				e.logger.Info("subtask in running state and is not idempotent, fail it",
					zap.Int64("subtask-id", subtask.ID))
				e.onError(ErrNonIdempotentSubtask)
				e.updateSubtaskStateAndError(runCtx, subtask, proto.SubtaskStateFailed, ErrNonIdempotentSubtask)
				e.markErrorHandled()
				break
			}
		} else {
			// subtask.State == proto.SubtaskStatePending
			err := e.startSubtaskAndUpdateState(runCtx, subtask)
			if err != nil {
				e.logger.Warn("startSubtaskAndUpdateState meets error", zap.Error(err))
				// should ignore ErrSubtaskNotFound
				// since the err only indicate that the subtask not owned by current task executor.
				if err == storage.ErrSubtaskNotFound {
					continue
				}
				e.onError(err)
				continue
			}
		}

		failpoint.Inject("mockCleanExecutor", func() {
			v, ok := testContexts.Load(e.id)
			if ok {
				if v.(*TestContext).mockDown.Load() {
					failpoint.Break()
				}
			}
		})

		failpoint.Inject("cancelBeforeRunSubtask", func() {
			runCancel(nil)
		})

		e.runSubtask(runCtx, stepExecutor, subtask)
	}
	return e.getError()
}

func (e *BaseTaskExecutor) runSubtask(ctx context.Context, stepExecutor execute.StepExecutor, subtask *proto.Subtask) {
	err := func() error {
		e.currSubtaskID.Store(subtask.ID)

		var wg util.WaitGroupWrapper
		checkCtx, checkCancel := context.WithCancel(ctx)
		wg.Go(func() {
			e.checkBalanceSubtask(checkCtx)
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
		e.onError(err)
	}

	finished := e.markSubTaskCanceledOrFailed(ctx, subtask)
	if finished {
		return
	}

	failpoint.Inject("mockTiDBDown", func(val failpoint.Value) {
		e.logger.Info("trigger mockTiDBDown")
		if e.id == val.(string) || e.id == ":4001" || e.id == ":4002" {
			v, ok := testContexts.Load(e.id)
			if ok {
				v.(*TestContext).TestSyncSubtaskRun <- struct{}{}
				v.(*TestContext).mockDown.Store(true)
				e.logger.Info("mockTiDBDown")
				time.Sleep(2 * time.Second)
				failpoint.Return()
			}
		}
	})
	failpoint.Inject("mockTiDBDown2", func() {
		if e.id == ":4003" && subtask.Step == proto.StepTwo {
			v, ok := testContexts.Load(e.id)
			if ok {
				v.(*TestContext).TestSyncSubtaskRun <- struct{}{}
				v.(*TestContext).mockDown.Store(true)
				time.Sleep(2 * time.Second)
				return
			}
		}
	})

	failpoint.Inject("mockTiDBPartitionThenResume", func(val failpoint.Value) {
		if val.(bool) && (e.id == ":4000" || e.id == ":4001" || e.id == ":4002") {
			infosync.MockGlobalServerInfoManagerEntry.DeleteByExecID(e.id)
			time.Sleep(20 * time.Second)
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
				logutil.BgLogger().Error("get task manager failed", zap.Error(err))
			} else {
				err = mgr.CancelTask(ctx, int64(taskID))
				if err != nil {
					logutil.BgLogger().Error("cancel task failed", zap.Error(err))
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

	e.finishSubtaskAndUpdateState(ctx, subtask)

	finished = e.markSubTaskCanceledOrFailed(ctx, subtask)
	if finished {
		return
	}

	failpoint.Inject("syncAfterSubtaskFinish", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})
}

// Rollback rollbacks the subtask.
// TODO no need to start executor to do it, refactor it later.
func (e *BaseTaskExecutor) Rollback(ctx context.Context, task *proto.Task) error {
	// TODO: we can centralized this when we move handleExecutableTask loop here.
	e.task.Store(task)

	e.resetError()
	e.logger.Info("taskExecutor rollback a step", zap.String("step", proto.Step2Str(task.Type, task.Step)))

	// We should cancel all subtasks before rolling back
	for {
		// TODO we can update them using one sql, but requires change the metric
		// gathering logic.
		subtask, err := e.taskTable.GetFirstSubtaskInStates(ctx, e.id, task.ID, task.Step,
			proto.SubtaskStatePending, proto.SubtaskStateRunning)
		if err != nil {
			e.onError(err)
			return e.getError()
		}

		if subtask == nil {
			break
		}

		e.updateSubtaskStateAndError(ctx, subtask, proto.SubtaskStateCanceled, nil)
		if err = e.getError(); err != nil {
			return err
		}
	}
	return e.getError()
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

func (e *BaseTaskExecutor) registerCancelFunc(cancel context.CancelCauseFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.runtimeCancel = cancel
}

// cancelRunStep cancels runStep, i.e. the running process, but leave the subtask
// state unchanged.
func (e *BaseTaskExecutor) cancelRunStep() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mu.runtimeCancel != nil {
		e.mu.runtimeCancel(nil)
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

func (e *BaseTaskExecutor) startSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) error {
	err := e.startSubtask(ctx, subtask.ID)
	if err == nil {
		metrics.DecDistTaskSubTaskCnt(subtask)
		metrics.EndDistTaskSubTask(subtask)
		subtask.State = proto.SubtaskStateRunning
		metrics.IncDistTaskSubTaskCnt(subtask)
		metrics.StartDistTaskSubTask(subtask)
	}
	return err
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

func (e *BaseTaskExecutor) updateSubtaskStateAndError(ctx context.Context, subtask *proto.Subtask, state proto.SubtaskState, subTaskErr error) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	e.updateSubtaskStateAndErrorImpl(ctx, subtask.ExecID, subtask.ID, state, subTaskErr)
	subtask.State = state
	metrics.IncDistTaskSubTaskCnt(subtask)
	if !subtask.IsDone() {
		metrics.StartDistTaskSubTask(subtask)
	}
}

func (e *BaseTaskExecutor) finishSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	e.finishSubtask(ctx, subtask)
	subtask.State = proto.SubtaskStateSucceed
	metrics.IncDistTaskSubTaskCnt(subtask)
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
			e.updateSubtaskStateAndError(e.ctx, subtask, proto.SubtaskStateCanceled, nil)
		} else if e.IsRetryableError(err) {
			e.logger.Warn("meet retryable error", zap.Error(err))
		} else if common.IsContextCanceledError(err) {
			e.logger.Info("meet context canceled for gracefully shutdown", zap.Error(err))
		} else {
			e.logger.Warn("subtask failed", zap.Error(err))
			e.updateSubtaskStateAndError(e.ctx, subtask, proto.SubtaskStateFailed, err)
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
// Handled errors should not happened during subtasks execution.
// Only handle errors before subtasks execution and after subtasks execution.
func (e *BaseTaskExecutor) updateSubtask(ctx context.Context, taskID int64, err error) error {
	err = errors.Cause(err)
	if ctx.Err() != nil && context.Cause(ctx) == ErrCancelSubtask {
		return e.cancelSubtaskWithRetry(ctx, taskID, ErrCancelSubtask)
	} else if e.IsRetryableError(err) {
		e.logger.Warn("meet retryable error", zap.Error(err))
	} else if common.IsContextCanceledError(err) {
		e.logger.Info("meet context canceled for gracefully shutdown", zap.Error(err))
	} else {
		return e.failSubtaskWithRetry(ctx, taskID, err)
	}
	return nil
}
