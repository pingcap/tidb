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

	if s.mu.runtimeCancel != nil {
		s.mu.runtimeCancel(err)
	}
}

func (s *BaseScheduler) markErrorHandled() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.handled = true
}

<<<<<<< HEAD
func (s *BaseScheduler) getError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.err
=======
// handle task in pausing state, cancel all running subtasks.
func (s *BaseScheduler) onPausing() error {
	logutil.Logger(s.logCtx).Info("on pausing state", zap.Stringer("state", s.Task.State), zap.Int64("step", int64(s.Task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, s.Task.ID, s.Task.Step)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	runningPendingCnt := cntByStates[proto.SubtaskStateRunning] + cntByStates[proto.SubtaskStatePending]
	if runningPendingCnt == 0 {
		logutil.Logger(s.logCtx).Info("all running subtasks paused, update the task to paused state")
		return s.updateTask(proto.TaskStatePaused, nil, RetrySQLTimes)
	}
	logutil.Logger(s.logCtx).Debug("on pausing state, this task keeps current state", zap.Stringer("state", s.Task.State))
	return nil
>>>>>>> 99f0349bfb6 (disttask: fix failed step is taken as success (#49971))
}

func (s *BaseScheduler) resetError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.err = nil
	s.mu.handled = false
}

<<<<<<< HEAD
func (s *BaseScheduler) startSubtaskAndUpdateState(ctx context.Context, subtask *proto.Subtask) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	s.startSubtask(ctx, subtask.ID)
	subtask.State = proto.TaskStateRunning
	metrics.IncDistTaskSubTaskCnt(subtask)
	metrics.StartDistTaskSubTask(subtask)
}

func (s *BaseScheduler) updateSubtaskStateAndErrorImpl(ctx context.Context, tidbID string, subtaskID int64, state proto.TaskState, subTaskErr error) {
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	logger := logutil.Logger(s.logCtx)
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
=======
// TestSyncChan is used to sync the test.
var TestSyncChan = make(chan struct{})

// handle task in resuming state.
func (s *BaseScheduler) onResuming() error {
	logutil.Logger(s.logCtx).Info("on resuming state", zap.Stringer("state", s.Task.State), zap.Int64("step", int64(s.Task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, s.Task.ID, s.Task.Step)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	if cntByStates[proto.SubtaskStatePaused] == 0 {
		// Finish the resuming process.
		logutil.Logger(s.logCtx).Info("all paused tasks converted to pending state, update the task to running state")
		err := s.updateTask(proto.TaskStateRunning, nil, RetrySQLTimes)
		failpoint.Inject("syncAfterResume", func() {
			TestSyncChan <- struct{}{}
		})
		return err
	}

	return s.taskMgr.ResumeSubtasks(s.ctx, s.Task.ID)
}

// handle task in reverting state, check all revert subtasks finishes.
func (s *BaseScheduler) onReverting() error {
	logutil.Logger(s.logCtx).Debug("on reverting state", zap.Stringer("state", s.Task.State), zap.Int64("step", int64(s.Task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, s.Task.ID, s.Task.Step)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	activeRevertCnt := cntByStates[proto.SubtaskStateRevertPending] + cntByStates[proto.SubtaskStateReverting]
	if activeRevertCnt == 0 {
		if err = s.OnDone(s.ctx, s, s.Task); err != nil {
			return errors.Trace(err)
		}
		return s.updateTask(proto.TaskStateReverted, nil, RetrySQLTimes)
	}
	// Wait all subtasks in this step finishes.
	s.OnTick(s.ctx, s.Task)
	logutil.Logger(s.logCtx).Debug("on reverting state, this task keeps current state", zap.Stringer("state", s.Task.State))
	return nil
}

// handle task in pending state, schedule subtasks.
func (s *BaseScheduler) onPending() error {
	logutil.Logger(s.logCtx).Debug("on pending state", zap.Stringer("state", s.Task.State), zap.Int64("step", int64(s.Task.Step)))
	return s.switch2NextStep()
}

// handle task in running state, check all running subtasks finishes.
// If subtasks finished, run into the next step.
func (s *BaseScheduler) onRunning() error {
	logutil.Logger(s.logCtx).Debug("on running state",
		zap.Stringer("state", s.Task.State),
		zap.Int64("step", int64(s.Task.Step)))
	// check current step finishes.
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, s.Task.ID, s.Task.Step)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	if cntByStates[proto.SubtaskStateFailed] > 0 || cntByStates[proto.SubtaskStateCanceled] > 0 {
		subTaskErrs, err := s.taskMgr.CollectSubTaskError(s.ctx, s.Task.ID)
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

	if err := s.balanceSubtasks(); err != nil {
		return err
	}
	// Wait all subtasks in this step finishes.
	s.OnTick(s.ctx, s.Task)
	logutil.Logger(s.logCtx).Debug("on running state, this task keeps current state", zap.Stringer("state", s.Task.State))
	return nil
}

func (s *BaseScheduler) onFinished() error {
	metrics.UpdateMetricsForFinishTask(s.Task)
	logutil.Logger(s.logCtx).Debug("schedule task, task is finished", zap.Stringer("state", s.Task.State))
	return s.taskMgr.TransferSubTasks2History(s.ctx, s.Task.ID)
}

// balanceSubtasks check the liveNode num every liveNodeFetchInterval then rebalance subtasks.
func (s *BaseScheduler) balanceSubtasks() error {
	if len(s.TaskNodes) == 0 {
		var err error
		s.TaskNodes, err = s.taskMgr.GetTaskExecutorIDsByTaskIDAndStep(s.ctx, s.Task.ID, s.Task.Step)
		if err != nil {
			return err
		}
	}
	s.balanceSubtaskTick++
	if s.balanceSubtaskTick == defaultBalanceSubtaskTicks {
		s.balanceSubtaskTick = 0
		eligibleNodes, err := s.getEligibleNodes()
		if err != nil {
			return err
		}
		if len(eligibleNodes) > 0 {
			return s.doBalanceSubtasks(eligibleNodes)
		}
	}
	return nil
}

// DoBalanceSubtasks make count of subtasks on each liveNodes balanced and clean up subtasks on dead nodes.
// TODO(ywqzzy): refine to make it easier for testing.
func (s *BaseScheduler) doBalanceSubtasks(eligibleNodes []string) error {
	eligibleNodeMap := make(map[string]struct{}, len(eligibleNodes))
	for _, n := range eligibleNodes {
		eligibleNodeMap[n] = struct{}{}
	}
	// 1. find out nodes need to clean subtasks.
	deadNodes := make([]string, 0)
	deadNodesMap := make(map[string]bool, 0)
	for _, node := range s.TaskNodes {
		if _, ok := eligibleNodeMap[node]; !ok {
			deadNodes = append(deadNodes, node)
			deadNodesMap[node] = true
		}
	}
	// 2. get subtasks for each node before rebalance.
	subtasks, err := s.taskMgr.GetSubtasksByStepAndState(s.ctx, s.Task.ID, s.Task.Step, proto.TaskStatePending)
	if err != nil {
		return err
	}
	if len(deadNodes) != 0 {
		/// get subtask from deadNodes, since there might be some running subtasks on deadNodes.
		/// In this case, all subtasks on deadNodes are in running/pending state.
		subtasksOnDeadNodes, err := s.taskMgr.GetSubtasksByExecIdsAndStepAndState(
			s.ctx,
			deadNodes,
			s.Task.ID,
			s.Task.Step,
			proto.SubtaskStateRunning)
		if err != nil {
			return err
		}
		subtasks = append(subtasks, subtasksOnDeadNodes...)
	}
	// 3. group subtasks for each task executor.
	subtasksOnTaskExecutor := make(map[string][]*proto.Subtask, len(eligibleNodes)+len(deadNodes))
	for _, node := range eligibleNodes {
		subtasksOnTaskExecutor[node] = make([]*proto.Subtask, 0)
	}
	for _, subtask := range subtasks {
		subtasksOnTaskExecutor[subtask.ExecID] = append(
			subtasksOnTaskExecutor[subtask.ExecID],
			subtask)
	}
	// 4. prepare subtasks that need to rebalance to other nodes.
	averageSubtaskCnt := len(subtasks) / len(eligibleNodes)
	rebalanceSubtasks := make([]*proto.Subtask, 0)
	for k, v := range subtasksOnTaskExecutor {
		if ok := deadNodesMap[k]; ok {
			rebalanceSubtasks = append(rebalanceSubtasks, v...)
			continue
		}
		// When no tidb scale-in/out and averageSubtaskCnt*len(eligibleNodes) < len(subtasks),
		// no need to send subtask to other nodes.
		// eg: tidb1 with 3 subtasks, tidb2 with 2 subtasks, subtasks are balanced now.
		if averageSubtaskCnt*len(eligibleNodes) < len(subtasks) && len(s.TaskNodes) == len(eligibleNodes) {
			if len(v) > averageSubtaskCnt+1 {
				rebalanceSubtasks = append(rebalanceSubtasks, v[0:len(v)-averageSubtaskCnt]...)
			}
			continue
		}
		if len(v) > averageSubtaskCnt {
			rebalanceSubtasks = append(rebalanceSubtasks, v[0:len(v)-averageSubtaskCnt]...)
		}
	}
	// 5. skip rebalance.
	if len(rebalanceSubtasks) == 0 {
		return nil
	}
	// 6.rebalance subtasks to other nodes.
	rebalanceIdx := 0
	for k, v := range subtasksOnTaskExecutor {
		if ok := deadNodesMap[k]; !ok {
			if len(v) < averageSubtaskCnt {
				for i := 0; i < averageSubtaskCnt-len(v) && rebalanceIdx < len(rebalanceSubtasks); i++ {
					rebalanceSubtasks[rebalanceIdx].ExecID = k
					rebalanceIdx++
				}
			}
		}
	}
	// 7. rebalance rest subtasks evenly to liveNodes.
	liveNodeIdx := 0
	for rebalanceIdx < len(rebalanceSubtasks) {
		rebalanceSubtasks[rebalanceIdx].ExecID = eligibleNodes[liveNodeIdx]
		rebalanceIdx++
		liveNodeIdx++
	}

	// 8. update subtasks and do clean up logic.
	if err = s.taskMgr.UpdateSubtasksExecIDs(s.ctx, s.Task.ID, subtasks); err != nil {
		return err
	}
	logutil.Logger(s.logCtx).Info("balance subtasks",
		zap.Stringers("subtasks-rebalanced", subtasks))
	s.TaskNodes = append([]string{}, eligibleNodes...)
	return nil
}

// updateTask update the task in tidb_global_task table.
func (s *BaseScheduler) updateTask(taskState proto.TaskState, newSubTasks []*proto.Subtask, retryTimes int) (err error) {
	prevState := s.Task.State
	s.Task.State = taskState
	logutil.BgLogger().Info("task state transform", zap.Stringer("from", prevState), zap.Stringer("to", taskState))
	if !VerifyTaskStateTransform(prevState, taskState) {
		return errors.Errorf("invalid task state transform, from %s to %s", prevState, taskState)
	}

	var retryable bool
	for i := 0; i < retryTimes; i++ {
		retryable, err = s.taskMgr.UpdateTaskAndAddSubTasks(s.ctx, s.Task, newSubTasks, prevState)
		if err == nil || !retryable {
			break
		}
		if err1 := s.ctx.Err(); err1 != nil {
			return err1
		}
		if i%10 == 0 {
			logutil.Logger(s.logCtx).Warn("updateTask first failed", zap.Stringer("from", prevState), zap.Stringer("to", s.Task.State),
				zap.Int("retry times", i), zap.Error(err))
		}
		time.Sleep(RetrySQLInterval)
	}
	if err != nil && retryTimes != nonRetrySQLTime {
		logutil.Logger(s.logCtx).Warn("updateTask failed",
			zap.Stringer("from", prevState), zap.Stringer("to", s.Task.State), zap.Int("retry times", retryTimes), zap.Error(err))
	}
	return err
}

func (s *BaseScheduler) onErrHandlingStage(receiveErrs []error) error {
	// we only store the first error.
	s.Task.Error = receiveErrs[0]

	var subTasks []*proto.Subtask
	// when step of task is `StepInit`, no need to do revert
	if s.Task.Step != proto.StepInit {
		instanceIDs, err := s.GetAllTaskExecutorIDs(s.ctx, s.Task)
		if err != nil {
			logutil.Logger(s.logCtx).Warn("get task's all instances failed", zap.Error(err))
			return err
		}

		subTasks = make([]*proto.Subtask, 0, len(instanceIDs))
		for _, id := range instanceIDs {
			// reverting subtasks belong to the same step as current active step.
			subTasks = append(subTasks, proto.NewSubtask(
				s.Task.Step, s.Task.ID, s.Task.Type, id,
				s.Task.Concurrency, proto.EmptyMeta, 0))
		}
	}
	return s.updateTask(proto.TaskStateReverting, subTasks, RetrySQLTimes)
}

func (s *BaseScheduler) switch2NextStep() (err error) {
	nextStep := s.GetNextStep(s.Task)
	logutil.Logger(s.logCtx).Info("on next step",
		zap.Int64("current-step", int64(s.Task.Step)),
		zap.Int64("next-step", int64(nextStep)))

	if nextStep == proto.StepDone {
		s.Task.Step = nextStep
		s.Task.StateUpdateTime = time.Now().UTC()
		if err = s.OnDone(s.ctx, s, s.Task); err != nil {
			return errors.Trace(err)
		}
		return s.taskMgr.SucceedTask(s.ctx, s.Task.ID)
	}

	serverNodes, err := s.getEligibleNodes()
	if err != nil {
		return err
	}
	logutil.Logger(s.logCtx).Info("eligible instances", zap.Int("num", len(serverNodes)))
	if len(serverNodes) == 0 {
		return errors.New("no available TiDB node to dispatch subtasks")
	}

	metas, err := s.OnNextSubtasksBatch(s.ctx, s, s.Task, serverNodes, nextStep)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("generate part of subtasks failed", zap.Error(err))
		return s.handlePlanErr(err)
	}

	return s.scheduleSubTask(nextStep, metas, serverNodes)
}

// getEligibleNodes returns the eligible(live) nodes for the task.
// if the task can only be scheduled to some specific nodes, return them directly,
// we don't care liveliness of them.
func (s *BaseScheduler) getEligibleNodes() ([]string, error) {
	serverNodes, err := s.GetEligibleInstances(s.ctx, s.Task)
	if err != nil {
		return nil, err
	}
	logutil.Logger(s.logCtx).Debug("eligible instances", zap.Int("num", len(serverNodes)))
	if len(serverNodes) == 0 {
		serverNodes = append([]string{}, s.nodeMgr.getManagedNodes()...)
	}
	return serverNodes, nil
}

func (s *BaseScheduler) scheduleSubTask(
	subtaskStep proto.Step,
	metas [][]byte,
	serverNodes []string) error {
	logutil.Logger(s.logCtx).Info("schedule subtasks",
		zap.Stringer("state", s.Task.State),
		zap.Int64("step", int64(s.Task.Step)),
		zap.Int("concurrency", s.Task.Concurrency),
		zap.Int("subtasks", len(metas)))
	s.TaskNodes = serverNodes
	var size uint64
	subTasks := make([]*proto.Subtask, 0, len(metas))
	for i, meta := range metas {
		// we assign the subtask to the instance in a round-robin way.
		// TODO: assign the subtask to the instance according to the system load of each nodes
		pos := i % len(serverNodes)
		instanceID := serverNodes[pos]
		logutil.Logger(s.logCtx).Debug("create subtasks", zap.String("instanceID", instanceID))
		subTasks = append(subTasks, proto.NewSubtask(
			subtaskStep, s.Task.ID, s.Task.Type, instanceID, s.Task.Concurrency, meta, i+1))

		size += uint64(len(meta))
	}
	failpoint.Inject("cancelBeforeUpdateTask", func() {
		_ = s.taskMgr.CancelTask(s.ctx, s.Task.ID)
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
>>>>>>> 99f0349bfb6 (disttask: fix failed step is taken as success (#49971))
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
<<<<<<< HEAD
	return err1
=======
	previousSubtaskMetas := make([][]byte, 0, len(previousSubtasks))
	for _, subtask := range previousSubtasks {
		previousSubtaskMetas = append(previousSubtaskMetas, subtask.Meta)
	}
	return previousSubtaskMetas, nil
}

// WithNewSession executes the function with a new session.
func (s *BaseScheduler) WithNewSession(fn func(se sessionctx.Context) error) error {
	return s.taskMgr.WithNewSession(fn)
}

// WithNewTxn executes the fn in a new transaction.
func (s *BaseScheduler) WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error {
	return s.taskMgr.WithNewTxn(ctx, fn)
}

func (*BaseScheduler) isStepSucceed(cntByStates map[proto.SubtaskState]int64) bool {
	_, ok := cntByStates[proto.SubtaskStateSucceed]
	return len(cntByStates) == 0 || (len(cntByStates) == 1 && ok)
}

// IsCancelledErr checks if the error is a cancelled error.
func IsCancelledErr(err error) bool {
	return strings.Contains(err.Error(), taskCancelMsg)
>>>>>>> 99f0349bfb6 (disttask: fix failed step is taken as success (#49971))
}
