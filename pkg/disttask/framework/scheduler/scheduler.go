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
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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

// NewBaseScheduler creates a new BaseScheduler.
func NewBaseScheduler(ctx context.Context, id string, taskID int64, taskTable TaskTable) *BaseScheduler {
	schedulerImpl := &BaseScheduler{
		id:        id,
		taskID:    taskID,
		taskTable: taskTable,
		ctx:       ctx,
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
				canceled, err := s.taskTable.IsSchedulerCanceled(ctx, s.id, s.taskID)
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
			logutil.Logger(s.logCtx).Error("BaseScheduler panicked", zap.Any("recover", r), zap.Stack("stack"))
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

func (s *BaseScheduler) run(ctx context.Context, task *proto.Task) (resErr error) {
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
		zap.Uint64("concurrency", task.Concurrency),
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
			logutil.Logger(s.logCtx).Info("scheduler runSubtask loop exit")
			break
		}

		subtask, err := s.taskTable.GetFirstSubtaskInStates(runCtx, s.id, task.ID, task.Step,
			proto.TaskStatePending, proto.TaskStateRunning)
		if err != nil {
			logutil.Logger(s.logCtx).Warn("GetFirstSubtaskInStates meets error", zap.Error(err))
			continue
		}
		if subtask == nil {
			newTask, err := s.taskTable.GetGlobalTaskByID(runCtx, task.ID)
			if err != nil {
				logutil.Logger(s.logCtx).Warn("GetGlobalTaskByID meets error", zap.Error(err))
				continue
			}
			// When the task move to next step or task state changes, the scheduler should exit.
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

		failpoint.Inject("mockCleanScheduler", func() {
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
				err = mgr.CancelGlobalTask(ctx, int64(taskID))
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
		logutil.BgLogger().Warn("scheduler rollback a step, but no subtask in revert_pending state", zap.Any("step", task.Step))
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

// Pause pause the scheduler task.
func (s *BaseScheduler) Pause(ctx context.Context, task *proto.Task) error {
	logutil.Logger(s.logCtx).Info("scheduler pause subtasks")
	// pause all running subtasks.
	if err := s.taskTable.PauseSubtasks(ctx, s.id, task.ID); err != nil {
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
	logutil.Logger(s.logCtx).Error("onError", zap.Error(err), zap.Stack("stack"))
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.err == nil {
		s.mu.err = err
		logutil.Logger(s.logCtx).Error("scheduler met first error", zap.Error(err))
	}

<<<<<<< HEAD
	if s.mu.runtimeCancel != nil {
		s.mu.runtimeCancel(err)
=======
			switch task.State {
			case proto.TaskStateCancelling:
				err = s.onCancelling()
			case proto.TaskStatePausing:
				err = s.onPausing()
			case proto.TaskStatePaused:
				err = s.onPaused()
				// close the scheduler.
				if err == nil {
					return
				}
			case proto.TaskStateResuming:
				err = s.onResuming()
			case proto.TaskStateReverting:
				err = s.onReverting()
			case proto.TaskStatePending:
				err = s.onPending()
			case proto.TaskStateRunning:
				err = s.onRunning()
			case proto.TaskStateSucceed, proto.TaskStateReverted, proto.TaskStateFailed:
				s.onFinished()
				return
			}
			if err != nil {
				logutil.Logger(s.logCtx).Info("schedule task meet err, reschedule it", zap.Error(err))
			}

			failpoint.Inject("mockOwnerChange", func(val failpoint.Value) {
				if val.(bool) {
					logutil.Logger(s.logCtx).Info("mockOwnerChange called")
					MockOwnerChange()
					time.Sleep(time.Second)
				}
			})
		}
>>>>>>> 720983a20c6 (disttask: merge transfer task/subtask (#50311))
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

<<<<<<< HEAD
func (s *BaseScheduler) updateSubtaskStateAndErrorImpl(ctx context.Context, tidbID string, subtaskID int64, state proto.TaskState, subTaskErr error) {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
=======
// handle task in reverting state, check all revert subtasks finishes.
func (s *BaseScheduler) onReverting() error {
	task := s.GetTask()
	logutil.Logger(s.logCtx).Debug("on reverting state", zap.Stringer("state", task.State), zap.Int64("step", int64(task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	activeRevertCnt := cntByStates[proto.SubtaskStateRevertPending] + cntByStates[proto.SubtaskStateReverting]
	if activeRevertCnt == 0 {
		if err = s.OnDone(s.ctx, s, task); err != nil {
			return errors.Trace(err)
		}
		return s.taskMgr.RevertedTask(s.ctx, task.ID)
	}
	// Wait all subtasks in this step finishes.
	s.OnTick(s.ctx, task)
	logutil.Logger(s.logCtx).Debug("on reverting state, this task keeps current state", zap.Stringer("state", task.State))
	return nil
}

// handle task in pending state, schedule subtasks.
func (s *BaseScheduler) onPending() error {
	task := s.GetTask()
	logutil.Logger(s.logCtx).Debug("on pending state", zap.Stringer("state", task.State), zap.Int64("step", int64(task.Step)))
	return s.switch2NextStep()
}

// handle task in running state, check all running subtasks finishes.
// If subtasks finished, run into the next step.
func (s *BaseScheduler) onRunning() error {
	task := s.GetTask()
	logutil.Logger(s.logCtx).Debug("on running state",
		zap.Stringer("state", task.State),
		zap.Int64("step", int64(task.Step)))
	// check current step finishes.
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	if cntByStates[proto.SubtaskStateFailed] > 0 || cntByStates[proto.SubtaskStateCanceled] > 0 {
		subTaskErrs, err := s.taskMgr.CollectSubTaskError(s.ctx, task.ID)
		if err != nil {
			logutil.Logger(s.logCtx).Warn("collect subtask error failed", zap.Error(err))
			return err
		}
		if len(subTaskErrs) > 0 {
			logutil.Logger(s.logCtx).Warn("subtasks encounter errors")
			return s.onErrHandlingStage(subTaskErrs)
		}
	} else if s.isStepSucceed(cntByStates) {
		return s.switch2NextStep()
	}

	// Wait all subtasks in this step finishes.
	s.OnTick(s.ctx, task)
	logutil.Logger(s.logCtx).Debug("on running state, this task keeps current state", zap.Stringer("state", task.State))
	return nil
}

func (s *BaseScheduler) onFinished() {
	task := s.GetTask()
	metrics.UpdateMetricsForFinishTask(task)
	logutil.Logger(s.logCtx).Debug("schedule task, task is finished", zap.Stringer("state", task.State))
}

// updateTask update the task in tidb_global_task table.
func (s *BaseScheduler) updateTask(taskState proto.TaskState, newSubTasks []*proto.Subtask, retryTimes int) (err error) {
	task := s.GetTask()
	prevState := task.State
	task.State = taskState
	logutil.BgLogger().Info("task state transform", zap.Stringer("from", prevState), zap.Stringer("to", taskState))
	if !VerifyTaskStateTransform(prevState, taskState) {
		return errors.Errorf("invalid task state transform, from %s to %s", prevState, taskState)
	}

	var retryable bool
	for i := 0; i < retryTimes; i++ {
		retryable, err = s.taskMgr.UpdateTaskAndAddSubTasks(s.ctx, task, newSubTasks, prevState)
		if err == nil || !retryable {
			break
		}
		if err1 := s.ctx.Err(); err1 != nil {
			return err1
		}
		if i%10 == 0 {
			logutil.Logger(s.logCtx).Warn("updateTask first failed", zap.Stringer("from", prevState), zap.Stringer("to", task.State),
				zap.Int("retry times", i), zap.Error(err))
		}
		time.Sleep(RetrySQLInterval)
	}
	if err != nil && retryTimes != nonRetrySQLTime {
		logutil.Logger(s.logCtx).Warn("updateTask failed",
			zap.Stringer("from", prevState), zap.Stringer("to", task.State), zap.Int("retry times", retryTimes), zap.Error(err))
	}
	return err
}

func (s *BaseScheduler) onErrHandlingStage(receiveErrs []error) error {
	task := s.GetTask()
	// we only store the first error.
	task.Error = receiveErrs[0]

	var subTasks []*proto.Subtask
	// when step of task is `StepInit`, no need to do revert
	if task.Step != proto.StepInit {
		instanceIDs, err := s.GetAllTaskExecutorIDs(s.ctx, task)
		if err != nil {
			logutil.Logger(s.logCtx).Warn("get task's all instances failed", zap.Error(err))
			return err
		}

		subTasks = make([]*proto.Subtask, 0, len(instanceIDs))
		for _, id := range instanceIDs {
			// reverting subtasks belong to the same step as current active step.
			subTasks = append(subTasks, proto.NewSubtask(
				task.Step, task.ID, task.Type, id,
				task.Concurrency, proto.EmptyMeta, 0))
		}
	}
	return s.updateTask(proto.TaskStateReverting, subTasks, RetrySQLTimes)
}

func (s *BaseScheduler) switch2NextStep() (err error) {
	task := s.GetTask()
	nextStep := s.GetNextStep(task)
	logutil.Logger(s.logCtx).Info("on next step",
		zap.Int64("current-step", int64(task.Step)),
		zap.Int64("next-step", int64(nextStep)))

	if nextStep == proto.StepDone {
		task.Step = nextStep
		task.StateUpdateTime = time.Now().UTC()
		if err = s.OnDone(s.ctx, s, task); err != nil {
			return errors.Trace(err)
		}
		return s.taskMgr.SucceedTask(s.ctx, task.ID)
	}

	eligibleNodes, err := getEligibleNodes(s.ctx, s, s.nodeMgr.getManagedNodes())
	if err != nil {
		return err
	}
	logutil.Logger(s.logCtx).Info("eligible instances", zap.Int("num", len(eligibleNodes)))
	if len(eligibleNodes) == 0 {
		return errors.New("no available TiDB node to dispatch subtasks")
	}

	metas, err := s.OnNextSubtasksBatch(s.ctx, s, task, eligibleNodes, nextStep)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("generate part of subtasks failed", zap.Error(err))
		return s.handlePlanErr(err)
	}

	return s.scheduleSubTask(nextStep, metas, eligibleNodes)
}

func (s *BaseScheduler) scheduleSubTask(
	subtaskStep proto.Step,
	metas [][]byte,
	eligibleNodes []string) error {
	task := s.GetTask()
	logutil.Logger(s.logCtx).Info("schedule subtasks",
		zap.Stringer("state", task.State),
		zap.Int64("step", int64(task.Step)),
		zap.Int("concurrency", task.Concurrency),
		zap.Int("subtasks", len(metas)))

	// the scheduled node of the subtask might not be optimal, as we run all
	// scheduler in parallel, and update might be called too many times when
	// multiple tasks are switching to next step.
	if err := s.slotMgr.update(s.ctx, s.nodeMgr, s.taskMgr); err != nil {
		return err
	}
	adjustedEligibleNodes := s.slotMgr.adjustEligibleNodes(eligibleNodes, task.Concurrency)
	var size uint64
	subTasks := make([]*proto.Subtask, 0, len(metas))
	for i, meta := range metas {
		// we assign the subtask to the instance in a round-robin way.
		// TODO: assign the subtask to the instance according to the system load of each nodes
		pos := i % len(adjustedEligibleNodes)
		instanceID := adjustedEligibleNodes[pos]
		logutil.Logger(s.logCtx).Debug("create subtasks", zap.String("instanceID", instanceID))
		subTasks = append(subTasks, proto.NewSubtask(
			subtaskStep, task.ID, task.Type, instanceID, task.Concurrency, meta, i+1))

		size += uint64(len(meta))
	}
	failpoint.Inject("cancelBeforeUpdateTask", func() {
		_ = s.taskMgr.CancelTask(s.ctx, task.ID)
	})

	// as other fields and generated key and index KV takes space too, we limit
	// the size of subtasks to 80% of the transaction limit.
	limit := max(uint64(float64(kv.TxnTotalSizeLimit.Load())*0.8), 1)
	fn := s.taskMgr.SwitchTaskStep
	if size >= limit {
		// On default, transaction size limit is controlled by tidb_mem_quota_query
		// which is 1G on default, so it's unlikely to reach this limit, but in
		// case user set txn-total-size-limit explicitly, we insert in batch.
		logutil.Logger(s.logCtx).Info("subtasks size exceed limit, will insert in batch",
			zap.Uint64("size", size), zap.Uint64("limit", limit))
		fn = s.taskMgr.SwitchTaskStepInBatch
	}

	backoffer := backoff.NewExponential(RetrySQLInterval, 2, RetrySQLMaxInterval)
	return handle.RunWithRetry(s.ctx, RetrySQLTimes, backoffer, logutil.Logger(s.logCtx),
>>>>>>> 720983a20c6 (disttask: merge transfer task/subtask (#50311))
		func(ctx context.Context) (bool, error) {
			return true, s.taskTable.UpdateSubtaskStateAndError(ctx, tidbID, subtaskID, state, subTaskErr)
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
			return true, s.taskTable.StartSubtask(ctx, subtaskID)
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
			return true, s.taskTable.FinishSubtask(ctx, subtask.SchedulerID, subtask.ID, subtask.Meta)
		},
	)
	if err != nil {
		s.onError(err)
	}
}

func (s *BaseScheduler) updateSubtaskStateAndError(ctx context.Context, subtask *proto.Subtask, state proto.TaskState, subTaskErr error) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.updateSubtaskStateAndErrorImpl(ctx, subtask.SchedulerID, subtask.ID, state, subTaskErr)
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

// TODO: abstract interface for each business to implement it.
func isRetryableError(err error) bool {
	originErr := errors.Cause(err)
	if tErr, ok := originErr.(*terror.Error); ok {
		sqlErr := terror.ToSQLError(tErr)
		_, ok := dbterror.ReorgRetryableErrCodes[sqlErr.Code]
		return ok
	}
	// can't retry Unknown err
	return false
}

// markSubTaskCanceledOrFailed check the error type and decide the subtasks' state.
// 1. Only cancel subtasks when meet ErrCancelSubtask.
// 2. Only fail subtasks when meet non retryable error.
// 3. When meet other errors, don't change subtasks' state.
func (s *BaseScheduler) markSubTaskCanceledOrFailed(ctx context.Context, subtask *proto.Subtask) bool {
	if err := s.getError(); err != nil {
		err := errors.Cause(err)
		if ctx.Err() != nil && context.Cause(ctx) == ErrCancelSubtask {
			logutil.Logger(s.logCtx).Warn("subtask canceled", zap.Error(err))
			s.updateSubtaskStateAndError(s.ctx, subtask, proto.TaskStateCanceled, nil)
		} else if common.IsRetryableError(err) || isRetryableError(err) {
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

func (s *BaseScheduler) updateErrorToSubtask(ctx context.Context, taskID int64, err error) error {
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
