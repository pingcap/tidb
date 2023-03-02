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
	SplitSubtask(*proto.Subtask) []*proto.Subtask
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
	subtaskWg    sync.WaitGroup

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

func (s *SchedulerImpl) Run(task *proto.Task) error {
	logutil.BgLogger().Info("scheduler run a step", zap.Any("id", s.id), zap.Any("step", task.Step), zap.Any("con", task.Concurrency))
	scheduler, err := s.createScheduler(task)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if err := scheduler.InitSubtaskExecEnv(s.ctx); err != nil {
		s.onError(err)
		return s.getError()
	}
	defer func() {
		err := scheduler.CleanupSubtaskExecEnv(s.ctx)
		if err != nil {
			logutil.BgLogger().Error("cleanup subtask exec env failed", zap.Error(err))
		}
	}()

	subtaskCtx, subtaskCancel := context.WithCancel(s.ctx)
	defer subtaskCancel()
	subtaskCh := make(chan *proto.Subtask, task.Concurrency)
	err = s.pool.RunWithConcurrency(func() {
		for subtask := range subtaskCh {
			// defer in a loop is not recommended, fix it after we support wait group in pool.
			// we should fetch all subtasks from subtaskCh even if there is an error or cancel.
			defer s.subtaskWg.Done()
			s.runSubtask(subtaskCtx, subtask, task.Step)
		}
	}, task.Concurrency)
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	for {
		subtask, err := s.subtaskTable.GetSubtaskInStates(s.id, task.ID, string(proto.TaskStatePending))
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

		subtasks := scheduler.SplitSubtask(subtask)
		for _, subtask := range subtasks {
			s.subtaskWg.Add(1)
			subtaskCh <- subtask
		}
	}

	close(subtaskCh)
	s.subtaskWg.Wait()
	return s.getError()
}

func (s *SchedulerImpl) runSubtask(subtaskCtx context.Context, subtask *proto.Subtask, step int64) {
	logutil.BgLogger().Info("scheduler run a subtask", zap.Any("id", s.id), zap.Any("subtask", subtask))
	select {
	case <-subtaskCtx.Done():
		s.updateSubtaskState(subtask.ID, proto.TaskStateCanceled)
		return
	default:
	}
	if s.getError() != nil {
		s.updateSubtaskState(subtask.ID, proto.TaskStateCanceled)
		return
	}

	executor, err := s.createSubtaskExecutor(subtask, step)
	if err != nil {
		s.updateSubtaskState(subtask.ID, proto.TaskStateFailed)
		s.onError(err)
		return
	}

	if err = executor.Run(subtaskCtx); err != nil {
		if errors.Cause(err) == context.Canceled {
			s.updateSubtaskState(subtask.ID, proto.TaskStateCanceled)
		} else {
			s.updateSubtaskState(subtask.ID, proto.TaskStateFailed)
			s.onError(err)
		}
		return
	}

	// TODO: if scheduler split subtasks, we update the status to succeed by original subtask id only when all split subtasks are successful.
	s.updateSubtaskState(subtask.ID, proto.TaskStateSucceed)
}

func (s *SchedulerImpl) Rollback(task *proto.Task) error {
	logutil.BgLogger().Info("scheduler rollback a step", zap.Any("id", s.id), zap.Any("step", task.Step), zap.Any("con", task.Concurrency))
	scheduler, err := s.createScheduler(task)
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	subtask, err := s.subtaskTable.GetSubtaskInStates(s.id, task.ID, string(proto.TaskStateRevertPending))
	if err != nil {
		s.onError(err)
		return s.getError()
	}
	if subtask == nil {
		logutil.BgLogger().Warn("scheduler rollback a step, but no subtask in revert_pending state", zap.Any("id", s.id), zap.Any("step", task.Step), zap.Any("con", task.Concurrency))
		return nil
	}

	rollbackCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	err = scheduler.Rollback(rollbackCtx)
	if err != nil {
		if errors.Cause(err) == context.Canceled {
			s.updateSubtaskState(subtask.ID, proto.TaskStateCanceled)
		} else {
			s.updateSubtaskState(subtask.ID, proto.TaskStateRevertFailed)
			s.onError(err)
		}
	} else {
		s.updateSubtaskState(subtask.ID, proto.TaskStateReverted)
	}
	return s.getError()
}

func (s *SchedulerImpl) Stop() {
	s.cancel()
	s.subtaskWg.Wait()
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

func (s *SchedulerImpl) createSubtaskExecutor(subtask *proto.Subtask, step int64) (SubtaskExecutor, error) {
	constructor, ok := subtaskExecutorConstructors[subtask.Type]
	if !ok {
		return nil, errors.Errorf("constructor of subtask executor for type %s not found", subtask.Type)
	}
	return constructor(subtask, step)
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
