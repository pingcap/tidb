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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InternalSchedulerImpl is the implementation of InternalScheduler.
type InternalSchedulerImpl struct {
	ctx       context.Context
	cancel    context.CancelFunc
	id        string
	taskID    int64
	taskTable TaskTable
	pool      Pool
	wg        sync.WaitGroup
	logCtx    context.Context

	mu struct {
		sync.RWMutex
		err error
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

//	func (s *InternalSchedulerImpl) heartbeat() {
//		ticker := time.NewTicker(proto.HeartbeatInterval)
//		for {
//			select {
//			case <-s.ctx.Done():
//				return
//			case <-ticker.C:
//				if err := s.subtaskTable.UpdateHeartbeat(s.id, s.taskID, time.Now()); err != nil {
//					s.onError(err)
//					return
//				}
//			}
//		}
//	}

// Run runs the scheduler task.
func (s *InternalSchedulerImpl) Run(ctx context.Context, task *proto.Task) error {
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	s.registerCancelFunc(runCancel)

	s.resetError()
	logutil.Logger(s.logCtx).Info("scheduler run a step", zap.Any("step", task.Step), zap.Any("concurrency", task.Concurrency))
	scheduler, err := createScheduler(task)
	if err != nil {
		s.onError(err)
		return s.getError()
	}

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
	var minimalTaskWg sync.WaitGroup

	err = s.pool.RunWithConcurrency(minimalTaskCh, uint32(task.Concurrency))
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	for {
		subtask, err := s.taskTable.GetSubtaskInStates(s.id, task.ID, proto.TaskStatePending)
		if err != nil {
			s.onError(err)
			break
		}
		if subtask == nil {
			logutil.Logger(s.logCtx).Info("scheduler finished subtasks", zap.Any("step", task.Step))
			break
		}
		s.updateSubtaskState(subtask.ID, proto.TaskStateRunning)
		if err := s.getError(); err != nil {
			break
		}

		minimalTasks, err := scheduler.SplitSubtask(runCtx, subtask.Meta)
		if err != nil {
			s.onError(err)
			break
		}
		logutil.Logger(s.logCtx).Info("split subTask", zap.Any("cnt", len(minimalTasks)), zap.Any("subtask_id", subtask.ID))
		for _, minimalTask := range minimalTasks {
			minimalTaskWg.Add(1)
			j := minimalTask
			minimalTaskCh <- func() {
				s.runMinimalTask(runCtx, j, task.Type, task.Step)
				minimalTaskWg.Done()
			}
		}
		minimalTaskWg.Wait()
		if err := s.getError(); err != nil {
			if errors.Cause(err) == context.Canceled {
				s.updateSubtaskState(subtask.ID, proto.TaskStateCanceled)
			} else {
				s.updateSubtaskState(subtask.ID, proto.TaskStateFailed)
			}
			break
		}
		if err := scheduler.OnSubtaskFinished(runCtx, subtask.Meta); err != nil {
			if errors.Cause(err) == context.Canceled {
				s.updateSubtaskState(subtask.ID, proto.TaskStateCanceled)
			} else {
				s.updateSubtaskState(subtask.ID, proto.TaskStateFailed)
			}
			s.onError(err)
			break
		}
		s.updateSubtaskState(subtask.ID, proto.TaskStateSucceed)
	}

	return s.getError()
}

func (s *InternalSchedulerImpl) runMinimalTask(minimalTaskCtx context.Context, minimalTask proto.MinimalTask, tp string, step int64) {
	logutil.Logger(s.logCtx).Info("scheduler run a minimalTask", zap.Any("step", step), zap.Any("minimal_task", minimalTask))
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
	scheduler, err := createScheduler(task)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	subtask, err := s.taskTable.GetSubtaskInStates(s.id, task.ID, proto.TaskStateRevertPending)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if subtask == nil {
		logutil.BgLogger().Warn("scheduler rollback a step, but no subtask in revert_pending state", zap.Any("step", task.Step))
		return nil
	}
	s.updateSubtaskState(subtask.ID, proto.TaskStateReverting)
	if err := s.getError(); err != nil {
		return err
	}

	err = scheduler.Rollback(rollbackCtx)
	if err != nil {
		s.updateSubtaskState(subtask.ID, proto.TaskStateRevertFailed)
		s.onError(err)
	} else {
		s.updateSubtaskState(subtask.ID, proto.TaskStateReverted)
	}
	return s.getError()
}

func createScheduler(task *proto.Task) (Scheduler, error) {
	constructor, ok := schedulerConstructors[task.Type]
	if !ok {
		return nil, errors.Errorf("constructor of scheduler for type %s not found", task.Type)
	}
	return constructor(task.Meta, task.Step)
}

func createSubtaskExecutor(minimalTask proto.MinimalTask, tp string, step int64) (SubtaskExecutor, error) {
	constructor, ok := subtaskExecutorConstructors[tp]
	if !ok {
		return nil, errors.Errorf("constructor of subtask executor for type %s not found", tp)
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

func (s *InternalSchedulerImpl) getError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.err
}

func (s *InternalSchedulerImpl) resetError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.err = nil
}

func (s *InternalSchedulerImpl) updateSubtaskState(id int64, state string) {
	err := s.taskTable.UpdateSubtaskState(id, state)
	if err != nil {
		s.onError(err)
	}
}
