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
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type Scheduler interface {
	InitSubtaskExecEnv() error
	SplitSubtasks([]*Subtask) []*Subtask
	CleanupSubtaskExecEnv() error
}

// DefaultScheduler is the default scheduler.
// Remove it if we make sure all tasks need have their own schedulers.
type DefaultScheduler struct{}

func (s *DefaultScheduler) InitSubtaskExecEnv() error { return nil }

func (s *DefaultScheduler) CleanupSubtaskExecEnv() error { return nil }

func (s *DefaultScheduler) SplitSubtasks(subtasks []*Subtask) []*Subtask { return subtasks }

type SchedulerImpl struct {
	Scheduler

	ctx          context.Context
	cancel       context.CancelFunc
	id           TiDBID
	task         *Task
	subtaskTable SubtaskTable
	pool         Pool
	wg           sync.WaitGroup

	mu struct {
		sync.Mutex
		err error
	}
}

func NewScheduler(ctx context.Context, id TiDBID, task *Task, subtaskTable SubtaskTable, pool Pool) *SchedulerImpl {
	schedulerImpl := &SchedulerImpl{
		task:         task,
		id:           id,
		subtaskTable: subtaskTable,
		pool:         pool,
	}
	if constructor, ok := schedulerConstructors[task.Type]; ok {
		schedulerImpl.Scheduler = constructor(task)
	} else {
		schedulerImpl.Scheduler = &DefaultScheduler{}
	}
	schedulerImpl.ctx, schedulerImpl.cancel = context.WithCancel(ctx)
	return schedulerImpl
}

func (s *SchedulerImpl) Run() error {
	if err := s.InitSubtaskExecEnv(); err != nil {
		s.onError(err)
		return s.getError()
	}
	defer func() {
		err := s.CleanupSubtaskExecEnv()
		if err != nil {
			logutil.BgLogger().Error("cleanup subtask exec env failed", zap.Error(err))
		}
	}()

	heartbeatCtx, heartbeatCancel := context.WithCancel(s.ctx)
	defer heartbeatCancel()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.heartbeat(heartbeatCtx)
	}()

	subtasks := s.generateSubtasks()
	subtaskCtx, subtaskCancel := context.WithCancel(s.ctx)
	defer subtaskCancel()
	subtaskCh := make(chan *Subtask, len(subtasks))
	s.wg.Add(len(subtasks))
	err := s.pool.RunWithConcurrency(func() {
		for subtask := range subtaskCh {
			// defer in a loop is not recommended, fix it after we support wait group in pool.
			// we should fetch all subtasks from subtaskCh even if there is an error or cancel.
			defer s.wg.Done()

			select {
			case <-subtaskCtx.Done():
				s.subtaskTable.UpdateSubtaskStatus(subtask.ID, TaskStatusCanceled)
				continue
			default:
			}
			if s.getError() != nil {
				s.subtaskTable.UpdateSubtaskStatus(subtask.ID, TaskStatusCanceled)
				continue
			}

			executor, err := s.createSubtaskExecutor(subtask)
			if err != nil {
				s.subtaskTable.UpdateSubtaskStatus(subtask.ID, TaskStatusFailed)
				s.onError(err)
				continue
			}

			if err = executor.Run(subtaskCtx); err != nil {
				if errors.Cause(err) == context.Canceled {
					s.subtaskTable.UpdateSubtaskStatus(subtask.ID, TaskStatusCanceled)
				} else {
					s.subtaskTable.UpdateSubtaskStatus(subtask.ID, TaskStatusFailed)
				}
				s.onError(err)
				continue
			}

			// TODO: if scheduler split subtasks, we update the status to succeed by original subtask id only when all split subtasks are successful.
			s.subtaskTable.UpdateSubtaskStatus(subtask.ID, TaskStatusSucceed)
		}
	}, s.task.Concurrency)
	if err != nil {
		s.onError(err)
		return s.getError()
	}

	for _, subtask := range subtasks {
		subtaskCh <- subtask
	}
	close(subtaskCh)
	heartbeatCancel()
	s.wg.Wait()

	return s.getError()
}

func (s *SchedulerImpl) Cancel() {
	s.cancel()
	s.wg.Wait()
}

func (s *SchedulerImpl) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.subtaskTable.UpdateHeartbeat(s.task.ID, s.id)
		}
	}
}

func (s *SchedulerImpl) createSubtaskExecutor(subtask *Subtask) (SubtaskExecutor, error) {
	constructor, ok := subtaskExecutorConstructors[subtask.Type]
	if !ok {
		return nil, errors.Errorf("constructor of subtask executor for type %s not found", subtask.Type)
	}
	return constructor(subtask), nil
}

func (s *SchedulerImpl) generateSubtasks() []*Subtask {
	subtasks := s.subtaskTable.GetRunningSubtasks(s.task.ID)
	return s.SplitSubtasks(subtasks)
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
