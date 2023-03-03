// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/distribute_framework/storage"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type Scheduler interface {
	InitSubtaskExecEnv(context.Context) error
	SplitSubtask(*proto.Subtask) []proto.MinimalTask
	CleanupSubtaskExecEnv(context.Context) error
	Rollback(context.Context) error
}

// DefaultScheduler is the default scheduler.
// Remove it if we make sure all tasks need have their own schedulers.
type DefaultScheduler struct{}

func (s *DefaultScheduler) InitSubtaskExecEnv(ctx context.Context) error { return nil }

func (s *DefaultScheduler) CleanupSubtaskExecEnv(ctx context.Context) error { return nil }

func (s *DefaultScheduler) SplitSubtasks(subtasks []*proto.Subtask) []*proto.Subtask { return subtasks }

type SchedulerImpl struct {
	ctx          context.Context
	cancel       context.CancelFunc
	id           string
	taskID       int64
	subtaskTable *storage.SubTaskManager
	pool         proto.Pool
	wg           sync.WaitGroup

	mu struct {
		sync.Mutex
		err error
	}
}

func NewScheduler(ctx context.Context, id string, taskID int64, subtaskTable *storage.SubTaskManager, pool proto.Pool) *SchedulerImpl {
	schedulerImpl := &SchedulerImpl{
		id:           id,
		taskID:       taskID,
		subtaskTable: subtaskTable,
		pool:         pool,
	}
	schedulerImpl.ctx, schedulerImpl.cancel = context.WithCancel(ctx)
	return schedulerImpl
}

func (s *SchedulerImpl) Run(ctx context.Context, task *proto.Task) error {
	logutil.BgLogger().Info("scheduler run a step", zap.Any("id", s.id), zap.Any("step", task.Step), zap.Any("con", task.Concurrency))
	scheduler, err := s.createScheduler(task)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if err := scheduler.InitSubtaskExecEnv(ctx); err != nil {
		s.onError(err)
		return s.getError()
	}
	defer func() {
		err := scheduler.CleanupSubtaskExecEnv(ctx)
		if err != nil {
			logutil.BgLogger().Error("cleanup subtask exec env failed", zap.Error(err))
		}
	}()

	minimalTaskCtx, minimalTaskCancel := context.WithCancel(ctx)
	defer minimalTaskCancel()
	minimalTaskCh := make(chan func(), task.Concurrency)
	defer close(minimalTaskCh)
	var minimalTaskWg sync.WaitGroup

	err = s.pool.RunWithConcurrency(minimalTaskCh, task.Concurrency)
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	for {
		subtask, err := s.subtaskTable.GetSubtaskInStates(s.id, task.ID, proto.TaskStatePending)
		if err != nil {
			s.onError(err)
			break
		}
		if subtask == nil {
			logutil.BgLogger().Info("scheduler finished subtasks", zap.Any("id", s.id), zap.Any("step", task.Step), zap.Any("con", task.Concurrency))
			break
		}
		s.updateSubtaskState(subtask.ID, proto.TaskStateRunning)
		if err := s.getError(); err != nil {
			break
		}

		minimalTasks := scheduler.SplitSubtask(subtask)
		logutil.BgLogger().Info("splite subTask", zap.Any("cnt", len(minimalTasks)), zap.Any("id", s.id))
		for _, minimalTask := range minimalTasks {
			minimalTaskWg.Add(1)
			j := minimalTask
			minimalTaskCh <- func() {
				s.runMinimalTask(minimalTaskCtx, j, task.Type, task.Step)
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
		} else {
			s.updateSubtaskState(subtask.ID, proto.TaskStateSucceed)
		}
	}

	return s.getError()
}

func (s *SchedulerImpl) runMinimalTask(minimalTaskCtx context.Context, minimalTask proto.MinimalTask, tp string, step int64) {
	logutil.BgLogger().Info("scheduler run a minimalTask", zap.Any("id", s.id), zap.Any("type", tp), zap.Any("minimal_task", minimalTask))
	select {
	case <-minimalTaskCtx.Done():
		s.onError(minimalTaskCtx.Err())
		return
	default:
	}
	if s.getError() != nil {
		return
	}

	executor, err := s.createSubtaskExecutor(minimalTask, tp, step)
	if err != nil {
		s.onError(err)
		return
	}

	if err = executor.Run(minimalTaskCtx); err != nil {
		s.onError(err)
	}
}

func (s *SchedulerImpl) Rollback(ctx context.Context, task *proto.Task) error {
	logutil.BgLogger().Info("scheduler rollback a step", zap.Any("id", s.id), zap.Any("step", task.Step), zap.Any("con", task.Concurrency))
	scheduler, err := s.createScheduler(task)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	subtask, err := s.subtaskTable.GetSubtaskInStates(s.id, task.ID, proto.TaskStateRevertPending)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if subtask == nil {
		logutil.BgLogger().Warn("scheduler rollback a step, but no subtask in revert_pending state", zap.Any("id", s.id), zap.Any("step", task.Step), zap.Any("con", task.Concurrency))
		return nil
	}
	s.updateSubtaskState(subtask.ID, proto.TaskStateReverting)
	if err := s.getError(); err != nil {
		return err
	}

	rollbackCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	err = scheduler.Rollback(rollbackCtx)
	if err != nil {
		s.updateSubtaskState(subtask.ID, proto.TaskStateRevertFailed)
		s.onError(err)
	} else {
		s.updateSubtaskState(subtask.ID, proto.TaskStateReverted)
	}
	return s.getError()
}

func (s *SchedulerImpl) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *SchedulerImpl) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.heartbeat()
	}()
}

func (s *SchedulerImpl) heartbeat() {
	ticker := time.NewTicker(proto.HeartbeatInterval)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.subtaskTable.UpdateHeartbeat(s.id, s.taskID, time.Now()); err != nil {
				s.onError(err)
				return
			}
		}
	}
}

func (s *SchedulerImpl) createScheduler(task *proto.Task) (Scheduler, error) {
	constructor, ok := schedulerConstructors[task.Type]
	if !ok {
		return nil, errors.Errorf("constructor of scheduler for type %s not found", task.Type)
	}
	return constructor(task, task.Step)
}

func (s *SchedulerImpl) createSubtaskExecutor(minimalTask proto.MinimalTask, tp string, step int64) (SubtaskExecutor, error) {
	constructor, ok := subtaskExecutorConstructors[tp]
	if !ok {
		return nil, errors.Errorf("constructor of subtask executor for type %s not found", tp)
	}
	return constructor(minimalTask, step)
}

func (s *SchedulerImpl) onError(err error) {
	if err == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.err == nil {
		s.mu.err = err
	}
	s.cancel()
}

func (s *SchedulerImpl) getError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.err
}

func (s *SchedulerImpl) updateSubtaskState(id int64, state string) {
	err := s.subtaskTable.UpdateSubtaskState(id, state)
	if err != nil {
		s.onError(err)
	}
}
