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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/metering"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	llog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

var (
	// checkBalanceSubtaskInterval is the default check interval for checking
	// subtasks balance to/away from this node.
	checkBalanceSubtaskInterval = 2 * time.Second

	// DetectParamModifyInterval is the interval to detect whether task params
	// are modified.
	// exported for testing.
	DetectParamModifyInterval = 5 * time.Second
)

var (
	// ErrCancelSubtask is the cancel cause when cancelling subtasks.
	ErrCancelSubtask = errors.New("cancel subtasks")
	// ErrNonIdempotentSubtask means the subtask is left in running state and is not idempotent,
	// so cannot be run again.
	ErrNonIdempotentSubtask = errors.New("subtask in running state and is not idempotent")
)

// Param is the parameters to create a task executor.
type Param struct {
	taskTable TaskTable
	slotMgr   *slotManager
	nodeRc    *proto.NodeResource
	// id, it's the same as server id now, i.e. host:port.
	execID string
	Store  kv.Storage
}

// NewParamForTest creates a new Param for test.
func NewParamForTest(taskTable TaskTable, slotMgr *slotManager, nodeRc *proto.NodeResource, execID string, store kv.Storage) Param {
	return Param{
		taskTable: taskTable,
		slotMgr:   slotMgr,
		nodeRc:    nodeRc,
		execID:    execID,
		Store:     store,
	}
}

// BaseTaskExecutor is the base implementation of TaskExecutor.
type BaseTaskExecutor struct {
	Param
	// task is a local state that periodically aligned with what's saved in system
	// table, but if the task has modified params, it might be updated in memory
	// to reflect that some param modification have been applied successfully,
	// see detectAndHandleParamModifyLoop for more detail.
	task   atomic.Pointer[proto.Task]
	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
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
func NewBaseTaskExecutor(ctx context.Context, task *proto.Task, param Param) *BaseTaskExecutor {
	logger := logutil.ErrVerboseLogger().With(
		zap.Int64("task-id", task.ID),
		zap.String("task-key", task.Key),
	)
	if intest.InTest {
		logger = logger.With(zap.String("server-id", param.execID))
	}
	subCtx, cancelFunc := context.WithCancel(ctx)
	subCtx = logutil.WithLogger(subCtx, logger)
	taskExecutorImpl := &BaseTaskExecutor{
		Param:  param,
		ctx:    subCtx,
		cancel: cancelFunc,
		logger: logger,
	}
	taskExecutorImpl.task.Store(task)
	return taskExecutorImpl
}

// checkBalanceSubtask check whether the subtasks are balanced to or away from this node.
//   - If other subtask of `running` state is scheduled to this node, try changed to
//     `pending` state, to make sure subtasks can be balanced later when node scale out.
//   - If current running subtask are scheduled away from this node, i.e. this node
//     is taken as down, cancel running.
func (e *BaseTaskExecutor) checkBalanceSubtask(ctx context.Context, subtaskCtxCancel context.CancelFunc) {
	start := time.Now()
	ticker := time.NewTicker(checkBalanceSubtaskInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		task := e.task.Load()
		if time.Since(start) > time.Hour {
			start = time.Now()
			e.logger.Info("subtask running for too long", zap.Int64("subtaskID", e.currSubtaskID.Load()))
			dxfmetric.ExecuteEventCounter.WithLabelValues(fmt.Sprint(task.ID), dxfmetric.EventSubtaskSlow).Inc()
		}

		subtasks, err := e.taskTable.GetSubtasksByExecIDAndStepAndStates(ctx, e.execID, task.ID, task.Step,
			proto.SubtaskStateRunning)
		if err != nil {
			e.logger.Error("get subtasks failed", zap.Error(err))
			continue
		}
		if len(subtasks) == 0 {
			e.logger.Info("subtask is scheduled away, cancel running",
				zap.Int64("subtaskID", e.currSubtaskID.Load()))
			// cancels runStep, but leave the subtask state unchanged.
			dxfmetric.ExecuteEventCounter.WithLabelValues(fmt.Sprint(task.ID), dxfmetric.EventSubtaskScheduledAway).Inc()
			if subtaskCtxCancel != nil {
				subtaskCtxCancel()
			}
			failpoint.InjectCall("afterCancelSubtaskExec")
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
	ticker := time.NewTicker(execute.UpdateSubtaskSummaryInterval)
	defer ticker.Stop()
	curSubtaskID := e.currSubtaskID.Load()
	update := func() {
		summary := stepExec.RealtimeSummary()
		err := taskMgr.UpdateSubtaskSummary(runStepCtx, curSubtaskID, summary)
		if err != nil {
			e.logger.Info("update subtask row count failed", zap.Error(err))
		}
	}

	lastUpdate := func() {
		// Try the best effort to update the summary before exiting.
		// Total retry time is 0.2s + 0.4s + 0.8s + 1.6s + 3.2s + 5s * 5 = 31.2s
		backoffer := backoff.NewExponential(time.Millisecond*200, 2, time.Second*5)
		if err := handle.RunWithRetry(runStepCtx, 10, backoffer, e.logger,
			func(context.Context) (bool, error) {
				summary := stepExec.RealtimeSummary()
				err := taskMgr.UpdateSubtaskSummary(runStepCtx, curSubtaskID, summary)
				return true, err
			},
		); err != nil {
			e.logger.Info("update subtask row count failed", zap.Error(err))
		}
	}

	for {
		select {
		case <-checkCtx.Done():
			lastUpdate()
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
// TODO: remove it when add-index.taskexecutor.Init don't depend on it.
func (e *BaseTaskExecutor) Ctx() context.Context {
	return e.ctx
}

// Run implements the TaskExecutor interface.
func (e *BaseTaskExecutor) Run() {
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
		metering.UnregisterRecorder(e.GetTaskBase().ID)
	}()

	trace := traceevent.NewTrace()
	ctx := tracing.WithFlightRecorder(e.ctx, trace)
	// task executor occupies resources, if there's no subtask to run for 10s,
	// we release the resources so that other tasks can use them.
	// 300ms + 600ms + 1.2s + 2s * 4 = 10.1s
	backoffer := backoff.NewExponential(SubtaskCheckInterval, 2, MaxSubtaskCheckInterval)
	checkInterval, noSubtaskCheckCnt := SubtaskCheckInterval, 0
	skipBackoff := false
	for {
		trace.DiscardOrFlush(ctx)
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
		oldTask := e.task.Load()
		failpoint.InjectCall("beforeGetTaskByIDInRun", oldTask.ID)
		newTask, err := e.taskTable.GetTaskByID(ctx, oldTask.ID)
		if err != nil {
			if goerrors.Is(err, storage.ErrTaskNotFound) {
				return
			}
			e.logger.Error("refresh task failed", zap.Error(err))
			continue
		}

		if !bytes.Equal(oldTask.Meta, newTask.Meta) {
			e.logger.Info("task meta modification applied",
				zap.String("oldStep", proto.Step2Str(oldTask.Type, oldTask.Step)),
				zap.String("newStep", proto.Step2Str(newTask.Type, newTask.Step)))
			// when task switch to next step, task meta might change too, but in
			// this case step executor will be recreated with new required slots and
			// meta, so we only notify it when it's still running the same step.
			if e.stepExec != nil && e.stepExec.GetStep() == newTask.Step {
				e.logger.Info("notify step executor to update task meta")
				if err2 := e.stepExec.TaskMetaModified(e.stepCtx, newTask.Meta); err2 != nil {
					e.logger.Info("notify step executor failed, will try recreate it later", zap.Error(err2))
					e.cleanStepExecutor()
					continue
				}
			}
		}
		if newTask.RequiredSlots != oldTask.RequiredSlots {
			if !e.slotMgr.exchange(&newTask.TaskBase) {
				e.logger.Info("task required slots modified, but not enough slots, executor exit",
					zap.Int("old", oldTask.RequiredSlots), zap.Int("new", newTask.RequiredSlots))
				return
			}
			e.logger.Info("task required slots modification applied",
				zap.Int("old", oldTask.RequiredSlots), zap.Int("new", newTask.RequiredSlots),
				zap.Int("availableSlots", e.slotMgr.availableSlots()))
			newResource := e.nodeRc.GetStepResource(&newTask.TaskBase)

			if e.stepExec != nil {
				e.stepExec.SetResource(newResource)
			}
		}

		e.task.Store(newTask)
		task := newTask
		if task.State != proto.TaskStateRunning && task.State != proto.TaskStateModifying {
			return
		}

		subtask, err := e.taskTable.GetFirstSubtaskInStates(ctx, e.execID, task.ID, task.Step,
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
			if err2 := e.createStepExecutor(); err2 != nil {
				e.logger.Error("create step executor failed",
					zap.String("step", proto.Step2Str(task.Type, task.Step)), zap.Error(err2))
				continue
			}
		}
		if err := e.stepCtx.Err(); err != nil {
			e.logger.Error("step executor context is done, the task should have been reverted",
				zap.String("step", proto.Step2Str(task.Type, task.Step)),
				zap.Error(err))
			continue
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

func (e *BaseTaskExecutor) createStepExecutor() error {
	task := e.task.Load()

	stepExecutor, err := e.GetStepExecutor(task)
	if err != nil {
		e.logger.Info("failed to get step executor", zap.Error(err))
		e.failOneSubtask(e.ctx, task.ID, err)
		return errors.Trace(err)
	}
	resource := e.nodeRc.GetStepResource(e.GetTaskBase())
	failpoint.InjectCall("beforeSetFrameworkInfo", resource)
	execute.SetFrameworkInfo(stepExecutor, task, resource, e.taskTable.UpdateSubtaskCheckpoint, e.taskTable.GetSubtaskCheckpoint)

	if err := stepExecutor.Init(e.ctx); err != nil {
		if e.IsRetryableError(err) {
			dxfmetric.ExecuteEventCounter.WithLabelValues(fmt.Sprint(task.ID), dxfmetric.EventRetry).Inc()
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
