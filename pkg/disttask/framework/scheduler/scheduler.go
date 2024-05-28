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
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/backoff"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
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

	balanceSubtaskTick int
	// rand is for generating random selection of nodes.
	rand *rand.Rand
}

// NewBaseScheduler creates a new BaseScheduler.
func NewBaseScheduler(ctx context.Context, task *proto.Task, param Param) *BaseScheduler {
	logger := log.L().With(zap.Int64("task-id", task.ID), zap.Stringer("task-type", task.Type), zap.Bool("allocated-slots", param.allocatedSlots))
	if intest.InTest {
		logger = logger.With(zap.String("server-id", param.serverID))
	}
	s := &BaseScheduler{
		ctx:    ctx,
		Param:  param,
		logger: logger,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	s.task.Store(task)
	return s
}

// Init implements the Scheduler interface.
func (*BaseScheduler) Init() error {
	return nil
}

// ScheduleTask implements the Scheduler interface.
func (s *BaseScheduler) ScheduleTask() {
	task := s.GetTask()
	s.logger.Info("schedule task",
		zap.Stringer("state", task.State), zap.Int("concurrency", task.Concurrency))
	s.scheduleTask()
}

// Close closes the scheduler.
func (*BaseScheduler) Close() {
}

// GetTask implements the Scheduler interface.
func (s *BaseScheduler) GetTask() *proto.Task {
	return s.task.Load()
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
	// state might be changed by user to pausing/resuming/cancelling, or
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
			err := s.refreshTaskIfNeeded()
			if err != nil {
				if errors.Cause(err) == storage.ErrTaskNotFound {
					// this can happen when task is reverted/succeed, but before
					// we reach here, cleanup routine move it to history.
					return
				}
				s.logger.Error("refresh task failed", zap.Error(err))
				continue
			}
			task := *s.GetTask()
			// TODO: refine failpoints below.
			failpoint.Inject("exitScheduler", func() {
				failpoint.Return()
			})
			failpoint.Inject("cancelTaskAfterRefreshTask", func(val failpoint.Value) {
				if val.(bool) && task.State == proto.TaskStateRunning {
					err := s.taskMgr.CancelTask(s.ctx, task.ID)
					if err != nil {
						s.logger.Error("cancel task failed", zap.Error(err))
					}
				}
			})

			failpoint.Inject("pausePendingTask", func(val failpoint.Value) {
				if val.(bool) && task.State == proto.TaskStatePending {
					_, err := s.taskMgr.PauseTask(s.ctx, task.Key)
					if err != nil {
						s.logger.Error("pause task failed", zap.Error(err))
					}
					task.State = proto.TaskStatePausing
					s.task.Store(&task)
				}
			})

			failpoint.Inject("pauseTaskAfterRefreshTask", func(val failpoint.Value) {
				if val.(bool) && task.State == proto.TaskStateRunning {
					_, err := s.taskMgr.PauseTask(s.ctx, task.Key)
					if err != nil {
						s.logger.Error("pause task failed", zap.Error(err))
					}
					task.State = proto.TaskStatePausing
					s.task.Store(&task)
				}
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
				// Case with 2 nodes.
				// Here is the timeline
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
				err = s.onPending()
			case proto.TaskStateRunning:
				// Case with 2 nodes.
				// Here is the timeline
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
}

// handle task in cancelling state, schedule revert subtasks.
func (s *BaseScheduler) onCancelling() error {
	task := s.GetTask()
	s.logger.Info("on cancelling state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))

	return s.revertTask(errors.New(taskCancelMsg))
}

// handle task in pausing state, cancel all running subtasks.
func (s *BaseScheduler) onPausing() error {
	task := *s.GetTask()
	s.logger.Info("on pausing state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
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
	s.task.Store(&task)
	return nil
}

// handle task in paused state.
func (s *BaseScheduler) onPaused() error {
	task := s.GetTask()
	s.logger.Info("on paused state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
	failpoint.InjectCall("mockDMLExecutionOnPausedState")
	return nil
}

// handle task in resuming state.
func (s *BaseScheduler) onResuming() error {
	task := *s.GetTask()
	s.logger.Info("on resuming state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
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
		s.task.Store(&task)
		return nil
	}

	return s.taskMgr.ResumeSubtasks(s.ctx, task.ID)
}

// handle task in reverting state, check all revert subtasks finishes.
func (s *BaseScheduler) onReverting() error {
	task := *s.GetTask()
	s.logger.Debug("on reverting state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		s.logger.Warn("check task failed", zap.Error(err))
		return err
	}
	runnableSubtaskCnt := cntByStates[proto.SubtaskStatePending] + cntByStates[proto.SubtaskStateRunning]
	if runnableSubtaskCnt == 0 {
		if err = s.OnDone(s.ctx, s, &task); err != nil {
			return errors.Trace(err)
		}
		if err = s.taskMgr.RevertedTask(s.ctx, task.ID); err != nil {
			return errors.Trace(err)
		}
		task.State = proto.TaskStateReverted
		s.task.Store(&task)
		return nil
	}
	// Wait all subtasks in this step finishes.
	s.OnTick(s.ctx, &task)
	s.logger.Debug("on reverting state, this task keeps current state", zap.Stringer("state", task.State))
	return nil
}

// handle task in pending state, schedule subtasks.
func (s *BaseScheduler) onPending() error {
	task := s.GetTask()
	s.logger.Debug("on pending state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
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
			return s.revertTask(subTaskErrs[0])
		}
	} else if s.isStepSucceed(cntByStates) {
		return s.switch2NextStep()
	}

	// Wait all subtasks in this step finishes.
	s.OnTick(s.ctx, task)
	s.logger.Debug("on running state, this task keeps current state", zap.Stringer("state", task.State))
	return nil
}

func (s *BaseScheduler) onFinished() {
	task := s.GetTask()
	metrics.UpdateMetricsForFinishTask(task)
	s.logger.Debug("schedule task, task is finished", zap.Stringer("state", task.State))
}

func (s *BaseScheduler) switch2NextStep() error {
	task := *s.GetTask()
	nextStep := s.GetNextStep(&task.TaskBase)
	s.logger.Info("switch to next step",
		zap.String("current-step", proto.Step2Str(task.Type, task.Step)),
		zap.String("next-step", proto.Step2Str(task.Type, nextStep)))

	if nextStep == proto.StepDone {
		if err := s.OnDone(s.ctx, s, &task); err != nil {
			return errors.Trace(err)
		}
		if err := s.taskMgr.SucceedTask(s.ctx, task.ID); err != nil {
			return errors.Trace(err)
		}
		task.Step = nextStep
		task.State = proto.TaskStateSucceed
		s.task.Store(&task)
		return nil
	}

	nodes := s.nodeMgr.getNodes()
	nodeIDs := filterByScope(nodes, task.TargetScope)
	eligibleNodes, err := getEligibleNodes(s.ctx, s, nodeIDs)
	if err != nil {
		return err
	}

	s.logger.Info("eligible instances", zap.Int("num", len(eligibleNodes)))
	if len(eligibleNodes) == 0 {
		return errors.New("no available TiDB node to dispatch subtasks")
	}

	metas, err := s.OnNextSubtasksBatch(s.ctx, s, &task, eligibleNodes, nextStep)
	if err != nil {
		s.logger.Warn("generate part of subtasks failed", zap.Error(err))
		return s.handlePlanErr(err)
	}

	if err = s.scheduleSubTask(&task, nextStep, metas, eligibleNodes); err != nil {
		return err
	}
	task.Step = nextStep
	task.State = proto.TaskStateRunning
	// and OnNextSubtasksBatch might change meta of task.
	s.task.Store(&task)
	return nil
}

func (s *BaseScheduler) scheduleSubTask(
	task *proto.Task,
	subtaskStep proto.Step,
	metas [][]byte,
	eligibleNodes []string) error {
	s.logger.Info("schedule subtasks",
		zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, subtaskStep)),
		zap.Int("concurrency", task.Concurrency),
		zap.Int("subtasks", len(metas)))

	// the scheduled node of the subtask might not be optimal, as we run all
	// scheduler in parallel, and update might be called too many times when
	// multiple tasks are switching to next step.
	// balancer will assign the subtasks to the right instance according to
	// the system load of all nodes.
	if err := s.slotMgr.update(s.ctx, s.nodeMgr, s.taskMgr); err != nil {
		return err
	}
	adjustedEligibleNodes := s.slotMgr.adjustEligibleNodes(eligibleNodes, task.Concurrency)
	var size uint64
	subTasks := make([]*proto.Subtask, 0, len(metas))
	for i, meta := range metas {
		// we assign the subtask to the instance in a round-robin way.
		pos := i % len(adjustedEligibleNodes)
		instanceID := adjustedEligibleNodes[pos]
		s.logger.Debug("create subtasks", zap.String("instanceID", instanceID))
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
		s.logger.Info("subtasks size exceed limit, will insert in batch",
			zap.Uint64("size", size), zap.Uint64("limit", limit))
		fn = s.taskMgr.SwitchTaskStepInBatch
	}

	backoffer := backoff.NewExponential(RetrySQLInterval, 2, RetrySQLMaxInterval)
	return handle.RunWithRetry(s.ctx, RetrySQLTimes, backoffer, s.logger,
		func(context.Context) (bool, error) {
			err := fn(s.ctx, task, proto.TaskStateRunning, subtaskStep, subTasks)
			if errors.Cause(err) == storage.ErrUnstableSubtasks {
				return false, err
			}
			return true, err
		},
	)
}

func (s *BaseScheduler) handlePlanErr(err error) error {
	task := *s.GetTask()
	s.logger.Warn("generate plan failed", zap.Error(err), zap.Stringer("state", task.State))
	if s.IsRetryableErr(err) {
		return err
	}
	return s.revertTask(err)
}

func (s *BaseScheduler) revertTask(taskErr error) error {
	task := *s.GetTask()
	if err := s.taskMgr.RevertTask(s.ctx, task.ID, task.State, taskErr); err != nil {
		return err
	}
	task.State = proto.TaskStateReverting
	task.Error = taskErr
	s.task.Store(&task)
	return nil
}

// MockServerInfo exported for scheduler_test.go
var MockServerInfo atomic.Pointer[[]string]

// GetLiveExecIDs returns all live executor node IDs.
func GetLiveExecIDs(ctx context.Context) ([]string, error) {
	failpoint.Inject("mockTaskExecutorNodes", func() {
		failpoint.Return(*MockServerInfo.Load(), nil)
	})
	serverInfos, err := generateTaskExecutorNodes(ctx)
	if err != nil {
		return nil, err
	}
	execIDs := make([]string, 0, len(serverInfos))
	for _, info := range serverInfos {
		execIDs = append(execIDs, disttaskutil.GenerateExecID(info))
	}
	return execIDs, nil
}

func generateTaskExecutorNodes(ctx context.Context) (serverNodes []*infosync.ServerInfo, err error) {
	var serverInfos map[string]*infosync.ServerInfo
	_, etcd := ctx.Value("etcd").(bool)
	if intest.InTest && !etcd {
		serverInfos = infosync.MockGlobalServerInfoManagerEntry.GetAllServerInfo()
	} else {
		serverInfos, err = infosync.GetAllServerInfo(ctx)
	}
	if err != nil {
		return nil, err
	}
	if len(serverInfos) == 0 {
		return nil, errors.New("not found instance")
	}

	serverNodes = make([]*infosync.ServerInfo, 0, len(serverInfos))
	for _, serverInfo := range serverInfos {
		serverNodes = append(serverNodes, serverInfo)
	}
	return serverNodes, nil
}

// GetPreviousSubtaskMetas get subtask metas from specific step.
func (s *BaseScheduler) GetPreviousSubtaskMetas(taskID int64, step proto.Step) ([][]byte, error) {
	previousSubtasks, err := s.taskMgr.GetAllSubtasksByStepAndState(s.ctx, taskID, step, proto.SubtaskStateSucceed)
	if err != nil {
		s.logger.Warn("get previous succeed subtask failed", zap.String("step", proto.Step2Str(s.GetTask().Type, step)))
		return nil, err
	}
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
}

// getEligibleNodes returns the eligible(live) nodes for the task.
// if the task can only be scheduled to some specific nodes, return them directly,
// we don't care liveliness of them.
func getEligibleNodes(ctx context.Context, sch Scheduler, managedNodes []string) ([]string, error) {
	serverNodes, err := sch.GetEligibleInstances(ctx, sch.GetTask())
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Debug("eligible instances", zap.Int("num", len(serverNodes)))
	if len(serverNodes) == 0 {
		serverNodes = managedNodes
	}

	return serverNodes, nil
}
