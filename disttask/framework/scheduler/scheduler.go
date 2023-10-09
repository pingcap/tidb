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
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultCheckSubtaskCanceledInterval is the default check interval for cancel cancelled subtasks.
	DefaultCheckSubtaskCanceledInterval = 2 * time.Second
)

// TestSyncChan is used to sync the test.
var TestSyncChan = make(chan struct{})

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
		runtimeCancel context.CancelFunc
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

func (s *BaseScheduler) startCancelCheck(ctx context.Context, wg *sync.WaitGroup, cancelFn context.CancelFunc) {
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
						cancelFn()
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
			err1 := s.taskTable.UpdateErrorToSubtask(s.id, task.ID, err4Panic)
			if err == nil {
				err = err1
			}
		}
	}()
	err = s.run(ctx, task)
	if s.mu.handled {
		return err
	}
	return s.taskTable.UpdateErrorToSubtask(s.id, task.ID, err)
}

func (s *BaseScheduler) run(ctx context.Context, task *proto.Task) error {
	if ctx.Err() != nil {
		s.onError(ctx.Err())
		return s.getError()
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
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
			s.startSubtaskAndUpdateState(subtask)
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
			err = context.Canceled
		}
	})
	if err != nil {
		s.onError(err)
		if errors.Cause(err) == context.Canceled {
			s.updateSubtaskStateAndError(subtask, proto.TaskStateCanceled, s.getError())
		} else {
			s.updateSubtaskStateAndError(subtask, proto.TaskStateFailed, s.getError())
		}
		s.markErrorHandled()
		return
	}
	if ctx.Err() != nil {
		s.onError(ctx.Err())
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
			s.onError(context.Canceled)
		}
	})
	if err := s.getError(); err != nil {
		if errors.Cause(err) == context.Canceled {
			s.updateSubtaskStateAndError(subtask, proto.TaskStateCanceled, nil)
		} else {
			s.updateSubtaskStateAndError(subtask, proto.TaskStateFailed, s.getError())
		}
		s.markErrorHandled()
		return
	}
	s.finishSubtaskAndUpdateState(subtask)
	failpoint.Inject("syncAfterSubtaskFinish", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})
}

// Rollback rollbacks the scheduler task.
func (s *BaseScheduler) Rollback(ctx context.Context, task *proto.Task) error {
	rollbackCtx, rollbackCancel := context.WithCancel(ctx)
	defer rollbackCancel()
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

func (s *BaseScheduler) registerCancelFunc(cancel context.CancelFunc) {
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
		s.mu.runtimeCancel()
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

func (s *BaseScheduler) startSubtaskAndUpdateState(subtask *proto.Subtask) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	err := s.taskTable.StartSubtask(subtask.ID)
	if err != nil {
		s.onError(err)
	}
	subtask.State = proto.TaskStateRunning
	metrics.IncDistTaskSubTaskCnt(subtask)
	metrics.StartDistTaskSubTask(subtask)
}

func (s *BaseScheduler) updateSubtaskStateAndError(subtask *proto.Subtask, state string, subTaskErr error) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	err := s.taskTable.UpdateSubtaskStateAndError(subtask.ID, state, subTaskErr)
	if err != nil {
		s.onError(err)
	}
	subtask.State = state
	metrics.IncDistTaskSubTaskCnt(subtask)
	if !subtask.IsFinished() {
		metrics.StartDistTaskSubTask(subtask)
	}
}

func (s *BaseScheduler) finishSubtaskAndUpdateState(subtask *proto.Subtask) {
	metrics.DecDistTaskSubTaskCnt(subtask)
	metrics.EndDistTaskSubTask(subtask)
	if err := s.taskTable.FinishSubtask(subtask.ID, subtask.Meta); err != nil {
		s.onError(err)
	}
	subtask.State = proto.TaskStateSucceed
	metrics.IncDistTaskSubTaskCnt(subtask)
}
