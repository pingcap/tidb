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
	"bytes"
	"context"
	goerrors "errors"
	"fmt"
	"runtime"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	llog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

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
		dxfmetric.ExecuteEventCounter.WithLabelValues(fmt.Sprint(subtask.TaskID), dxfmetric.EventSubtaskRerun).Inc()
		e.logger.Info("subtask in running state and is idempotent",
			zap.Int64("subtask-id", subtask.ID))
	} else {
		// subtask.State == proto.SubtaskStatePending
		err := e.startSubtask(e.stepCtx, subtask.ID)
		if err != nil {
			// should ignore ErrSubtaskNotFound
			// since it only means that the subtask not owned by current task executor.
			if !goerrors.Is(err, storage.ErrSubtaskNotFound) {
				e.logger.Warn("start subtask meets error", zap.Error(err))
			}
			return errors.Trace(err)
		}
	}

	logger := e.logger.With(zap.Int64("subtaskID", subtask.ID), zap.String("step", proto.Step2Str(subtask.Type, subtask.Step)))
	logTask := llog.BeginTask(logger, "run subtask")
	subtaskCtx, subtaskCancel := context.WithCancel(e.stepCtx)
	subtaskCtx = logutil.WithLogger(subtaskCtx, logger)
	subtaskErr := func() error {
		e.currSubtaskID.Store(subtask.ID)

		var wg util.WaitGroupWrapper
		checkCtx, checkCancel := context.WithCancel(subtaskCtx)
		wg.RunWithLog(func() {
			e.checkBalanceSubtask(checkCtx, subtaskCancel)
		})

		if e.hasRealtimeSummary(e.stepExec) {
			e.stepExec.ResetSummary()
			wg.RunWithLog(func() {
				e.updateSubtaskSummaryLoop(checkCtx, subtaskCtx, e.stepExec)
			})
		}
		wg.RunWithLog(func() {
			e.detectAndHandleParamModifyLoop(checkCtx)
		})
		defer func() {
			checkCancel()
			wg.Wait()
		}()
		return e.stepExec.RunSubtask(subtaskCtx, subtask)
	}()
	defer subtaskCancel()
	failpoint.InjectCall("afterRunSubtask", e, &subtaskErr, subtaskCtx)
	logTask.End2(zap.InfoLevel, subtaskErr)
	failpoint.InjectCall("mockTiDBShutdown", e, e.execID, e.GetTaskBase())

	if subtaskErr != nil {
		if err := e.markSubTaskCanceledOrFailed(subtaskCtx, subtask, subtaskErr); err != nil {
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

// there are 2 places that will detect task param modification:
//   - Run loop to make 'modifies' apply to all later subtasks
//   - this loop to try to make 'modifies' apply to current running subtask
//
// for a single step executor, successfully applied 'modifies' will not be applied
// again, failed ones will be retried in this loop. To achieve this, we will update
// the task inside BaseTaskExecutor to reflect the 'modifies' that have applied
// successfully. the 'modifies' that failed to apply in this loop will be retried
// in the Run loop.
func (e *BaseTaskExecutor) detectAndHandleParamModifyLoop(ctx context.Context) {
	ticker := time.NewTicker(DetectParamModifyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		err := e.detectAndHandleParamModify(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.logger.Warn("failed to detect and handle param modification",
				zap.Int64("currSubtaskID", e.currSubtaskID.Load()), zap.Error(err))
		}
	}
}

func (e *BaseTaskExecutor) detectAndHandleParamModify(ctx context.Context) error {
	oldTask := e.task.Load()
	latestTask, err := e.taskTable.GetTaskByID(ctx, oldTask.ID)
	if err != nil {
		return err
	}

	metaModified := !bytes.Equal(latestTask.Meta, oldTask.Meta)
	if latestTask.RequiredSlots == oldTask.RequiredSlots && !metaModified {
		return nil
	}

	e.logger.Info("task param modification detected",
		zap.Int64("currSubtaskID", e.currSubtaskID.Load()),
		zap.Bool("metaModified", metaModified),
		zap.Int("oldRequiredSlots", oldTask.RequiredSlots),
		zap.Int("newRequiredSlots", latestTask.RequiredSlots))

	// we don't report error here, as we might fail to modify task required slots
	// due to not enough slots, we still need try to apply meta modification.
	e.tryModifyTaskRequiredSlots(ctx, oldTask, latestTask)
	if metaModified {
		if err := e.stepExec.TaskMetaModified(ctx, latestTask.Meta); err != nil {
			return errors.Annotate(err, "failed to apply task param modification")
		}
		e.metaModifyApplied(latestTask.Meta)
	}
	failpoint.InjectCall("afterDetectAndHandleParamModify", e.task.Load().Step)
	return nil
}

func (e *BaseTaskExecutor) tryModifyTaskRequiredSlots(ctx context.Context, oldTask, latestTask *proto.Task) {
	logger := e.logger.With(zap.Int64("currSubtaskID", e.currSubtaskID.Load()),
		zap.Int("old", oldTask.RequiredSlots), zap.Int("new", latestTask.RequiredSlots))
	if latestTask.RequiredSlots < oldTask.RequiredSlots {
		// we need try to release the resource first, then free slots, to avoid
		// OOM when manager starts other task executor and start to allocate memory
		// immediately.
		newResource := e.nodeRc.GetStepResource(&latestTask.TaskBase)
		if err := e.stepExec.ResourceModified(ctx, newResource); err != nil {
			logger.Warn("failed to reduce resource usage", zap.Error(err))
			return
		}
		if !e.slotMgr.exchange(&latestTask.TaskBase) {
			// we are returning resource back, should not happen
			logger.Warn("failed to free slots")
			intest.Assert(false, "failed to return slots")
			return
		}

		// After reducing memory usage, garbage may not be recycled
		// in time, so we trigger GC here.
		//nolint: revive
		runtime.GC()
		e.requiredSlotsModifyApplied(latestTask.RequiredSlots)
	} else if latestTask.RequiredSlots > oldTask.RequiredSlots {
		exchanged := e.slotMgr.exchange(&latestTask.TaskBase)
		if !exchanged {
			logger.Info("failed to exchange slots", zap.Int("availableSlots", e.slotMgr.availableSlots()))
			return
		}
		newResource := e.nodeRc.GetStepResource(&latestTask.TaskBase)
		if err := e.stepExec.ResourceModified(ctx, newResource); err != nil {
			exchanged := e.slotMgr.exchange(&oldTask.TaskBase)
			intest.Assert(exchanged, "failed to return slots")
			logger.Warn("failed to increase resource usage, return slots back", zap.Error(err),
				zap.Int("availableSlots", e.slotMgr.availableSlots()), zap.Bool("exchanged", exchanged))
			return
		}

		e.requiredSlotsModifyApplied(latestTask.RequiredSlots)
	}
}

func (e *BaseTaskExecutor) requiredSlotsModifyApplied(newSlots int) {
	clone := *e.task.Load()
	e.logger.Info("task required slots modification applied",
		zap.Int64("currSubtaskID", e.currSubtaskID.Load()), zap.Int("old", clone.RequiredSlots),
		zap.Int("new", newSlots), zap.Int("availableSlots", e.slotMgr.availableSlots()))
	clone.RequiredSlots = newSlots
	e.task.Store(&clone)
}

func (e *BaseTaskExecutor) metaModifyApplied(newMeta []byte) {
	e.logger.Info("task meta modification applied", zap.Int64("currSubtaskID", e.currSubtaskID.Load()))
	clone := *e.task.Load()
	clone.Meta = newMeta
	e.task.Store(&clone)
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

	dxfmetric.ExecuteEventCounter.DeletePartialMatch(prometheus.Labels{dxfmetric.LblTaskID: fmt.Sprint(e.GetTaskBase().ID)})
}

func (e *BaseTaskExecutor) cancelRunStepWith(cause error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mu.runtimeCancel != nil {
		e.mu.runtimeCancel(cause)
	}
}

func (e *BaseTaskExecutor) updateSubtaskStateAndErrorImpl(ctx context.Context, execID string, subtaskID int64, state proto.SubtaskState, subTaskErr error) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
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
			err := e.taskTable.StartSubtask(ctx, subtaskID, e.execID)
			if goerrors.Is(err, storage.ErrSubtaskNotFound) {
				// No need to retry.
				return false, err
			}
			return true, err
		},
	)
	if err != nil && !goerrors.Is(err, storage.ErrSubtaskNotFound) {
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
		dxfmetric.ExecuteEventCounter.WithLabelValues(fmt.Sprint(subtask.TaskID), dxfmetric.EventRetry).Inc()
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
			return true, e.taskTable.FailSubtask(ctx, e.execID, taskID, subtaskErr)
		},
	)
	if err1 != nil {
		e.logger.Error("fail one subtask failed", zap.NamedError("subtaskErr", subtaskErr),
			zap.Duration("takes", time.Since(start)), zap.Error(err1))
	}
}

// GetTaskTable returns the TaskTable of the TaskExecutor.
func (e *BaseTaskExecutor) GetTaskTable() TaskTable {
	return e.taskTable
}
