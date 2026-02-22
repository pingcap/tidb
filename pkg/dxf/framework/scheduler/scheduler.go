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
	goerrors "errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// for a cancelled task, it's terminal state is reverted or reverted_failed,
	// so we use a special error message to indicate that the task is cancelled
	// by user.
	taskCancelMsg = "cancelled by user"
)

var (
	// CheckTaskFinishedInterval is the interval for scheduler.
	// exported for testing.
	CheckTaskFinishedInterval = 500 * time.Millisecond
	// RetrySQLTimes is the max retry times when executing SQL.
	RetrySQLTimes = 30
	// RetrySQLInterval is the initial interval between two SQL retries.
	RetrySQLInterval = 3 * time.Second
	// RetrySQLMaxInterval is the max interval between two SQL retries.
	RetrySQLMaxInterval = 30 * time.Second
)

// Scheduler manages the lifetime of a task
// including submitting subtasks and updating the status of a task.
type Scheduler interface {
	// Init initializes the scheduler, should be called before ExecuteTask.
	// if Init returns error, scheduler manager will fail the task directly,
	// so the returned error should be a fatal error.
	Init() error
	// ScheduleTask schedules the task execution step by step.
	ScheduleTask()
	// Close closes the scheduler, should be called if Init returns nil.
	Close()
	// GetTask returns the task that the scheduler is managing.
	// the task is for read only, it might be accessed by multiple goroutines
	GetTask() *proto.Task
	Extension
}

// BaseScheduler is the base struct for Scheduler.
// each task type embed this struct and implement the Extension interface.
type BaseScheduler struct {
	ctx context.Context
	Param
	// task might be accessed by multiple goroutines, so don't change its fields
	// directly, make a copy, update and store it back to the atomic pointer.
	task   atomic.Pointer[proto.Task]
	logger *zap.Logger
	// when RegisterSchedulerFactory, the factory MUST initialize this fields.
	Extension

	// rand is for generating random selection of nodes.
	rand *rand.Rand
}

// NewBaseScheduler creates a new BaseScheduler.
func NewBaseScheduler(ctx context.Context, task *proto.Task, param Param) *BaseScheduler {
	logger := logutil.ErrVerboseLogger().With(zap.Int64("task-id", task.ID), zap.String("task-key", task.Key))
	if intest.InTest {
		logger = logger.With(zap.String("server-id", param.serverID))
	}
	ctx = logutil.WithLogger(ctx, logger)
	s := &BaseScheduler{
		ctx:    ctx,
		Param:  param,
		logger: logger,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	s.task.Store(task)
	logger.Info("create base scheduler", zap.Stringer("task-type", task.Type), zap.Bool("allocated-slots", param.allocatedSlots))
	return s
}

// Init implements the Scheduler interface.
func (s *BaseScheduler) Init() error {
	if s.TaskStore.GetKeyspace() != s.GetTask().Keyspace {
		// shouldn't happen normally, but since keyspace mismatch might cause
		// correctness error, we check it at runtime too.
		return errors.New("store keyspace mismatch with task")
	}
	return nil
}

// ScheduleTask implements the Scheduler interface.
func (s *BaseScheduler) ScheduleTask() {
	task := s.GetTask()
	s.logger.Info("schedule task",
		zap.Stringer("state", task.State), zap.Int("requiredSlots", task.RequiredSlots))
	s.scheduleTask()
}

// Close closes the scheduler.
func (s *BaseScheduler) Close() {
	dxfmetric.ScheduleEventCounter.DeletePartialMatch(prometheus.Labels{dxfmetric.LblTaskID: fmt.Sprint(s.GetTask().ID)})
}

// GetTask implements the Scheduler interface.
func (s *BaseScheduler) GetTask() *proto.Task {
	return s.task.Load()
}

// getTaskClone returns a clone of the task.
func (s *BaseScheduler) getTaskClone() *proto.Task {
	clone := *s.GetTask()
	return &clone
}

// refreshTaskIfNeeded fetch task state from tidb_global_task table.
func (s *BaseScheduler) refreshTaskIfNeeded() error {
	task := s.GetTask()
	// we only query the base fields of task to reduce memory usage, other fields
	// are refreshed when needed.
	newTaskBase, err := s.taskMgr.GetTaskBaseByID(s.ctx, task.ID)
	if err != nil {
		return err
	}
	// state might be changed by user to pausing/resuming/cancelling/modifying, or
	// in case of network partition, state/step/meta might be changed by other scheduler,
	// in both cases we refresh the whole task object.
	if newTaskBase.State != task.State || newTaskBase.Step != task.Step {
		s.logger.Info("task state/step changed by user or other scheduler",
			zap.Stringer("old-state", task.State),
			zap.Stringer("new-state", newTaskBase.State),
			zap.String("old-step", proto.Step2Str(task.Type, task.Step)),
			zap.String("new-step", proto.Step2Str(task.Type, newTaskBase.Step)))
		newTask, err := s.taskMgr.GetTaskByID(s.ctx, task.ID)
		if err != nil {
			return err
		}
		s.task.Store(newTask)
	}
	return nil
}

// scheduleTask schedule the task execution step by step.
func (s *BaseScheduler) scheduleTask() {
	ticker := time.NewTicker(CheckTaskFinishedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("schedule task exits")
			return
		case <-ticker.C:
		}

		failpoint.InjectCall("beforeRefreshTask", s.GetTask())
		err := s.refreshTaskIfNeeded()
		if err != nil {
			if goerrors.Is(err, storage.ErrTaskNotFound) {
				// this can happen when task is reverted/succeed, but before
				// we reach here, cleanup routine move it to history.
				s.logger.Debug("task not found, might be reverted/succeed/failed")
				return
			}
			s.logger.Error("refresh task failed", zap.Error(err))
			continue
		}
		failpoint.InjectCall("afterRefreshTask", s.GetTask())
		task := s.getTaskClone()
		// TODO: refine failpoints below.
		failpoint.Inject("exitScheduler", func() {
			failpoint.Return()
		})

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
			// need to check allocatedSlots for the following case:
			// 1. task in pausing state.
			// 2. node1 and node2 start schedulers with task in pausing state without allocatedSlots.
			// 3. node1's scheduler transfer the node from pausing to paused state.
			// 4. resume the task.
			// 5. node2 scheduler call refreshTask and get task with resuming state.
			if !s.allocatedSlots {
				s.logger.Info("scheduler exit since not allocated slots", zap.Stringer("state", task.State))
				return
			}
			err = s.onResuming()
		case proto.TaskStateReverting:
			err = s.onReverting()
		case proto.TaskStatePending:
			// need to check allocatedSlots for the following case:
			// 1. task in modifying state, node A and B start schedulers with
			//    task in modifying state without allocatedSlots.
			// 2. node A's scheduler finished modifying, and transfer the node
			//    from modifying to pending state.
			// 3. node B's scheduler call refreshTask and get task with pending
			//    state, but this scheduler has not allocated slots.
			if !s.allocatedSlots {
				s.logger.Info("scheduler exit since not allocated slots", zap.Stringer("state", task.State))
				return
			}
			err = s.onPending()
		case proto.TaskStateRunning:
			// need to check allocatedSlots for the following case:
			// 1. task in pausing state.
			// 2. node1 and node2 start schedulers with task in pausing state without allocatedSlots.
			// 3. node1's scheduler transfer the node from pausing to paused state.
			// 4. resume the task.
			// 5. node1 start another scheduler and transfer the node from resuming to running state.
			// 6. node2 scheduler call refreshTask and get task with running state.
			if !s.allocatedSlots {
				s.logger.Info("scheduler exit since not allocated slots", zap.Stringer("state", task.State))
				return
			}
			err = s.onRunning()
		case proto.TaskStateModifying:
			var recreateScheduler bool
			recreateScheduler, err = s.onModifying()
			if err == nil && recreateScheduler {
				return
			}
		case proto.TaskStateSucceed, proto.TaskStateReverted, proto.TaskStateFailed:
			s.onFinished()
			return
		}
		if err != nil {
			s.logger.Info("schedule task meet err, reschedule it", zap.Error(err))
		}

		failpoint.InjectCall("mockOwnerChange")
	}
}

// handle task in cancelling state, schedule revert subtasks.
func (s *BaseScheduler) onCancelling() error {
	task := s.GetTask()
	s.logger.Info("on cancelling state", zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, task.Step)))

	return s.revertTask(errors.New(taskCancelMsg))
}

// handle task in pausing state, cancel all running subtasks.
func (s *BaseScheduler) onPausing() error {
	task := s.getTaskClone()
	s.logger.Info("on pausing state", zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		s.logger.Warn("check task failed", zap.Error(err))
		return err
	}
	runningPendingCnt := cntByStates[proto.SubtaskStateRunning] + cntByStates[proto.SubtaskStatePending]
	if runningPendingCnt > 0 {
		s.logger.Debug("on pausing state, this task keeps current state", zap.Stringer("state", task.State))
		return nil
	}

	s.logger.Info("all running subtasks paused, update the task to paused state")
	if err = s.taskMgr.PausedTask(s.ctx, task.ID); err != nil {
		return err
	}
	task.State = proto.TaskStatePaused
	s.task.Store(task)
	return nil
}

// handle task in paused state.
func (s *BaseScheduler) onPaused() error {
	task := s.GetTask()
	s.logger.Info("on paused state", zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, task.Step)))
	failpoint.InjectCall("mockDMLExecutionOnPausedState")
	return nil
}

// handle task in resuming state.
func (s *BaseScheduler) onResuming() error {
	task := s.getTaskClone()
	s.logger.Info("on resuming state", zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		s.logger.Warn("check task failed", zap.Error(err))
		return err
	}
	if cntByStates[proto.SubtaskStatePaused] == 0 {
		// Finish the resuming process.
		s.logger.Info("all paused tasks converted to pending state, update the task to running state")
		if err = s.taskMgr.ResumedTask(s.ctx, task.ID); err != nil {
			return err
		}
		task.State = proto.TaskStateRunning
		s.task.Store(task)
		return nil
	}

	return s.taskMgr.ResumeSubtasks(s.ctx, task.ID)
}

// handle task in reverting state, check all revert subtasks finishes.
func (s *BaseScheduler) onReverting() error {
	task := s.getTaskClone()
	s.logger.Debug("on reverting state", zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		s.logger.Warn("check task failed", zap.Error(err))
		return err
	}
	runnableSubtaskCnt := cntByStates[proto.SubtaskStatePending] + cntByStates[proto.SubtaskStateRunning]
	if runnableSubtaskCnt == 0 {
		if err = s.OnDone(s.ctx, s, task); err != nil {
			return errors.Trace(err)
		}
		if err = s.taskMgr.RevertedTask(s.ctx, task.ID); err != nil {
			return errors.Trace(err)
		}
		task.State = proto.TaskStateReverted
		s.task.Store(task)
		onTaskFinished(task.State, task.Error)
		return nil
	}
	// Wait all subtasks in this step finishes.
	s.OnTick(s.ctx, task)
	s.logger.Debug("on reverting state, this task keeps current state", zap.Stringer("state", task.State))
	return nil
}

// handle task in pending state, schedule subtasks.
func (s *BaseScheduler) onPending() error {
	task := s.GetTask()
	s.logger.Debug("on pending state", zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, task.Step)))
	return s.switch2NextStep()
}

// handle task in running state, check all running subtasks finishes.
// If subtasks finished, run into the next step.
func (s *BaseScheduler) onRunning() error {
	task := s.GetTask()
	s.logger.Debug("on running state",
		zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, task.Step)))
	// check current step finishes.
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		s.logger.Warn("check task failed", zap.Error(err))
		return err
	}
	if cntByStates[proto.SubtaskStateFailed] > 0 || cntByStates[proto.SubtaskStateCanceled] > 0 {
		subTaskErrs, err := s.taskMgr.GetSubtaskErrors(s.ctx, task.ID)
		if err != nil {
			s.logger.Warn("collect subtask error failed", zap.Error(err))
			return err
		}
		if len(subTaskErrs) > 0 {
			s.logger.Warn("subtasks encounter errors", zap.Errors("subtask-errs", subTaskErrs))
			// we only store the first error as task error.
			return s.revertTaskOrManualRecover(subTaskErrs[0])
		}
	} else if s.isStepSucceed(cntByStates) {
		return s.switch2NextStep()
	}

	// Wait all subtasks in this step finishes.
	s.OnTick(s.ctx, task)
	s.logger.Debug("on running state, this task keeps current state", zap.Stringer("state", task.State))
	return nil
}

// onModifying is called when task is in modifying state.
// the first return value indicates whether the scheduler should be recreated.
func (s *BaseScheduler) onModifying() (bool, error) {
	task := s.getTaskClone()
	s.logger.Info("on modifying state", zap.Stringer("param", &task.ModifyParam))
	recreateScheduler := false
	metaModifies := make([]proto.Modification, 0, len(task.ModifyParam.Modifications))
	for _, m := range task.ModifyParam.Modifications {
		switch m.Type {
		case proto.ModifyRequiredSlots:
			if task.RequiredSlots == int(m.To) {
				// shouldn't happen normally.
				s.logger.Info("task required slots not changed, skip", zap.Int("requiredSlots", task.RequiredSlots))
				continue
			}
			s.logger.Info("modify task required slots", zap.Int("from", task.RequiredSlots), zap.Int64("to", m.To))
			recreateScheduler = true
			task.RequiredSlots = int(m.To)
		case proto.ModifyMaxNodeCount:
			if m.To <= 0 {
				s.logger.Warn("task max-node-count should be greater than 0, skip")
				continue
			}
			s.logger.Info("modify task max-node-count", zap.Int("from", task.MaxNodeCount), zap.Int64("to", m.To))
			task.MaxNodeCount = int(m.To)
		default:
			metaModifies = append(metaModifies, m)
		}
	}
	if len(metaModifies) > 0 {
		s.logger.Info("modify task meta", zap.Stringers("modifies", metaModifies))
		newMeta, err := s.ModifyMeta(task.Meta, metaModifies)
		if err != nil {
			return false, errors.Trace(err)
		}
		task.Meta = newMeta
	}
	if err := s.taskMgr.ModifiedTask(s.ctx, task); err != nil {
		return false, errors.Trace(err)
	}
	task.State = task.ModifyParam.PrevState
	task.ModifyParam = proto.ModifyParam{}
	s.task.Store(task)
	return recreateScheduler, nil
}
