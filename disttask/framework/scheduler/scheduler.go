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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// TestSyncChan is used to sync the test.
var TestSyncChan = make(chan struct{})

// InternalSchedulerImpl is the implementation of InternalScheduler.
type InternalSchedulerImpl struct {
	ctx    context.Context
	cancel context.CancelFunc
	// id, it's the same as server id now, i.e. host:port.
	id        string
	taskID    int64
	taskTable TaskTable
	pool      Pool
	wg        sync.WaitGroup
	logCtx    context.Context

	mu struct {
		sync.RWMutex
		err error
		// handled indicates whether the error has been updated to one of the subtask.
		handled bool
		// runtimeCancel is used to cancel the Run/Rollback when error occurs.
		runtimeCancel context.CancelFunc
	}
}

// NewInternalScheduler creates a new InternalScheduler.
func NewInternalScheduler(ctx context.Context, id string, taskID int64, taskTable TaskTable, pool Pool) InternalScheduler {
	logPrefix := fmt.Sprintf("id: %s, task_id: %d", id, taskID)
	schedulerImpl := &InternalSchedulerImpl{
		id:        id,
		taskID:    taskID,
		taskTable: taskTable,
		pool:      pool,
		logCtx:    logutil.WithKeyValue(context.Background(), "scheduler", logPrefix),
	}
	schedulerImpl.ctx, schedulerImpl.cancel = context.WithCancel(ctx)

	return schedulerImpl
}

// Start starts the scheduler.
func (*InternalSchedulerImpl) Start() {
	//	s.wg.Add(1)
	//	go func() {
	//		defer s.wg.Done()
	//		s.heartbeat()
	//	}()
}

// Stop stops the scheduler.
func (s *InternalSchedulerImpl) Stop() {
	s.cancel()
	s.wg.Wait()
}

// Run runs the scheduler task.
func (s *InternalSchedulerImpl) Run(ctx context.Context, task *proto.Task) error {
	err := s.run(ctx, task)
	if s.mu.handled {
		return err
	}
	return s.taskTable.UpdateErrorToSubtask(s.id, err)
}

func (s *InternalSchedulerImpl) run(ctx context.Context, task *proto.Task) error {
	if ctx.Err() != nil {
		s.onError(ctx.Err())
		return s.getError()
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	s.registerCancelFunc(runCancel)

	s.resetError()
	logutil.Logger(s.logCtx).Info("scheduler run a step", zap.Any("step", task.Step), zap.Any("concurrency", task.Concurrency))
	scheduler, err := createScheduler(ctx, task)
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	failpoint.Inject("mockExecSubtaskInitEnvErr", func() {
		failpoint.Return(errors.New("mockExecSubtaskInitEnvErr"))
	})
	if err := scheduler.InitSubtaskExecEnv(runCtx); err != nil {
		s.onError(err)
		return s.getError()
	}
	defer func() {
		err := scheduler.CleanupSubtaskExecEnv(runCtx)
		if err != nil {
			logutil.Logger(s.logCtx).Error("cleanup subtask exec env failed", zap.Error(err))
		}
	}()

	minimalTaskCh := make(chan func(), task.Concurrency)
	defer close(minimalTaskCh)

	err = s.pool.RunWithConcurrency(minimalTaskCh, uint32(task.Concurrency))
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	for {
		// check if any error occurs.
		if err := s.getError(); err != nil {
			break
		}

		subtask, err := s.taskTable.GetSubtaskInStates(s.id, task.ID, task.Step, proto.TaskStatePending)
		if err != nil {
			s.onError(err)
			break
		}
		if subtask == nil {
			break
		}
		s.startSubtask(subtask.ID)
		if err := s.getError(); err != nil {
			break
		}
		s.runSubtask(runCtx, scheduler, subtask, minimalTaskCh)
	}
	return s.getError()
}

func (s *InternalSchedulerImpl) runSubtask(ctx context.Context, scheduler Scheduler, subtask *proto.Subtask, minimalTaskCh chan func()) {
	minimalTasks, err := scheduler.SplitSubtask(ctx, subtask.Meta)
	if err != nil {
		s.onError(err)
		if errors.Cause(err) == context.Canceled {
			s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateCanceled, nil)
		} else {
			s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateFailed, s.getError())
		}
		s.markErrorHandled()
		return
	}
	if ctx.Err() != nil {
		s.onError(ctx.Err())
		return
	}
	logutil.Logger(s.logCtx).Info("split subTask",
		zap.Int("cnt", len(minimalTasks)),
		zap.Int64("subtask_id", subtask.ID),
		zap.Int64("subtask_step", subtask.Step))

	var minimalTaskWg sync.WaitGroup
	for _, minimalTask := range minimalTasks {
		minimalTaskWg.Add(1)
		j := minimalTask
		minimalTaskCh <- func() {
			s.runMinimalTask(ctx, j, subtask.Type, subtask.Step)
			minimalTaskWg.Done()
		}
	}
	failpoint.Inject("waitUntilError", func() {
		for i := 0; i < 10; i++ {
			if s.getError() != nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	})
	minimalTaskWg.Wait()
	s.onSubtaskFinished(ctx, scheduler, subtask)
}

func (s *InternalSchedulerImpl) onSubtaskFinished(ctx context.Context, scheduler Scheduler, subtask *proto.Subtask) {
	var subtaskMeta []byte
	if err := s.getError(); err == nil {
		if subtaskMeta, err = scheduler.OnSubtaskFinished(ctx, subtask.Meta); err != nil {
			s.onError(err)
		}
	}
	if err := s.getError(); err != nil {
		if errors.Cause(err) == context.Canceled {
			s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateCanceled, nil)
		} else {
			s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateFailed, s.getError())
		}
		s.markErrorHandled()
		return
	}
	if err := s.taskTable.FinishSubtask(subtask.ID, subtaskMeta); err != nil {
		s.onError(err)
	}
	failpoint.Inject("syncAfterSubtaskFinish", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})
}

func (s *InternalSchedulerImpl) runMinimalTask(minimalTaskCtx context.Context, minimalTask proto.MinimalTask, tp string, step int64) {
	logutil.Logger(s.logCtx).Info("scheduler run a minimalTask", zap.Any("step", step), zap.Stringer("minimal_task", minimalTask))
	select {
	case <-minimalTaskCtx.Done():
		s.onError(minimalTaskCtx.Err())
		return
	default:
	}
	if s.getError() != nil {
		return
	}

	executor, err := createSubtaskExecutor(minimalTask, tp, step)
	if err != nil {
		s.onError(err)
		return
	}
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
	if err = executor.Run(minimalTaskCtx); err != nil {
		s.onError(err)
	}
}

// Rollback rollbacks the scheduler task.
func (s *InternalSchedulerImpl) Rollback(ctx context.Context, task *proto.Task) error {
	rollbackCtx, rollbackCancel := context.WithCancel(ctx)
	defer rollbackCancel()
	s.registerCancelFunc(rollbackCancel)

	s.resetError()
	logutil.Logger(s.logCtx).Info("scheduler rollback a step", zap.Any("step", task.Step))

	// We should cancel all subtasks before rolling back
	for {
		subtask, err := s.taskTable.GetSubtaskInStates(s.id, task.ID, task.Step, proto.TaskStatePending, proto.TaskStateRunning)
		if err != nil {
			s.onError(err)
			return s.getError()
		}

		if subtask == nil {
			break
		}

		s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateCanceled, nil)
		if err = s.getError(); err != nil {
			return err
		}
	}

	scheduler, err := createScheduler(ctx, task)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	subtask, err := s.taskTable.GetSubtaskInStates(s.id, task.ID, task.Step, proto.TaskStateRevertPending)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if subtask == nil {
		logutil.BgLogger().Warn("scheduler rollback a step, but no subtask in revert_pending state", zap.Any("step", task.Step))
		return nil
	}
	s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateReverting, nil)
	if err := s.getError(); err != nil {
		return err
	}

	err = scheduler.Rollback(rollbackCtx)
	if err != nil {
		s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateRevertFailed, nil)
		s.onError(err)
	} else {
		s.updateSubtaskStateAndError(subtask.ID, proto.TaskStateReverted, nil)
	}
	return s.getError()
}

func createScheduler(ctx context.Context, task *proto.Task) (Scheduler, error) {
	key := getKey(task.Type, task.Step)
	constructor, ok := schedulerConstructors[key]
	if !ok {
		return nil, errors.Errorf("constructor of scheduler for key %s not found", key)
	}
	return constructor(ctx, task.ID, task.Meta, task.Step)
}

func createSubtaskExecutor(minimalTask proto.MinimalTask, tp string, step int64) (SubtaskExecutor, error) {
	key := getKey(tp, step)
	constructor, ok := subtaskExecutorConstructors[key]
	if !ok {
		return nil, errors.Errorf("constructor of subtask executor for key %s not found", key)
	}
	return constructor(minimalTask, step)
}

func (s *InternalSchedulerImpl) registerCancelFunc(cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.runtimeCancel = cancel
}

func (s *InternalSchedulerImpl) onError(err error) {
	if err == nil {
		return
	}

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

func (s *InternalSchedulerImpl) markErrorHandled() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.handled = true
}

func (s *InternalSchedulerImpl) getError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.err
}

func (s *InternalSchedulerImpl) resetError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.err = nil
	s.mu.handled = false
}

func (s *InternalSchedulerImpl) startSubtask(id int64) {
	err := s.taskTable.StartSubtask(id)
	if err != nil {
		s.onError(err)
	}
}

func (s *InternalSchedulerImpl) updateSubtaskStateAndError(id int64, state string, subTaskErr error) {
	err := s.taskTable.UpdateSubtaskStateAndError(id, state, subTaskErr)
	if err != nil {
		s.onError(err)
	}
}
