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
	// DefaultSubtaskConcurrency is the default concurrency for handling subtask.
	DefaultSubtaskConcurrency = 16
	// MaxSubtaskConcurrency is the maximum concurrency for handling subtask.
	MaxSubtaskConcurrency = 256
	// defaultBalanceSubtaskTicks is the tick interval of fetching all server infos from etcs.
	defaultBalanceSubtaskTicks = 2
	// for a cancelled task, it's terminal state is reverted or reverted_failed,
	// so we use a special error message to indicate that the task is cancelled
	// by user.
	taskCancelMsg = "cancelled by user"
)

var (
	checkTaskFinishedInterval = 500 * time.Millisecond
	nonRetrySQLTime           = 1
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

// MockOwnerChange mock owner change in tests.
var MockOwnerChange func()

// NewBaseScheduler creates a new BaseScheduler.
func NewBaseScheduler(ctx context.Context, task *proto.Task, param Param) *BaseScheduler {
	s := &BaseScheduler{
		ctx:   ctx,
		Param: param,
		logger: log.L().With(zap.Int64("task-id", task.ID),
			zap.Stringer("task-type", task.Type)),
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
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

// refreshTask fetch task state from tidb_global_task table.
func (s *BaseScheduler) refreshTask() error {
	task := s.GetTask()
	newTask, err := s.taskMgr.GetTaskByID(s.ctx, task.ID)
	if err != nil {
		s.logger.Error("refresh task failed", zap.Error(err))
		return err
	}
	s.task.Store(newTask)
	return nil
}

// scheduleTask schedule the task execution step by step.
func (s *BaseScheduler) scheduleTask() {
	ticker := time.NewTicker(checkTaskFinishedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("schedule task exits", zap.Error(s.ctx.Err()))
			return
		case <-ticker.C:
			err := s.refreshTask()
			if err != nil {
				continue
			}
			task := *s.GetTask()
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
				s.logger.Info("schedule task meet err, reschedule it", zap.Error(err))
			}

			failpoint.Inject("mockOwnerChange", func(val failpoint.Value) {
				if val.(bool) {
					s.logger.Info("mockOwnerChange called")
					MockOwnerChange()
					time.Sleep(time.Second)
				}
			})
		}
	}
}

// handle task in cancelling state, schedule revert subtasks.
func (s *BaseScheduler) onCancelling() error {
	task := s.GetTask()
	s.logger.Info("on cancelling state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
	errs := []error{errors.New(taskCancelMsg)}
	return s.onErrHandlingStage(errs)
}

// handle task in pausing state, cancel all running subtasks.
func (s *BaseScheduler) onPausing() error {
	task := s.GetTask()
	s.logger.Info("on pausing state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		s.logger.Warn("check task failed", zap.Error(err))
		return err
	}
	runningPendingCnt := cntByStates[proto.SubtaskStateRunning] + cntByStates[proto.SubtaskStatePending]
	if runningPendingCnt == 0 {
		s.logger.Info("all running subtasks paused, update the task to paused state")
		return s.taskMgr.PausedTask(s.ctx, task.ID)
	}
	s.logger.Debug("on pausing state, this task keeps current state", zap.Stringer("state", task.State))
	return nil
}

// MockDMLExecutionOnPausedState is used to mock DML execution when tasks pauses.
var MockDMLExecutionOnPausedState func(task *proto.Task)

// handle task in paused state.
func (s *BaseScheduler) onPaused() error {
	task := s.GetTask()
	s.logger.Info("on paused state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
	failpoint.Inject("mockDMLExecutionOnPausedState", func(val failpoint.Value) {
		if val.(bool) {
			MockDMLExecutionOnPausedState(task)
		}
	})
	return nil
}

// TestSyncChan is used to sync the test.
var TestSyncChan = make(chan struct{})

// handle task in resuming state.
func (s *BaseScheduler) onResuming() error {
	task := s.GetTask()
	s.logger.Info("on resuming state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		s.logger.Warn("check task failed", zap.Error(err))
		return err
	}
	if cntByStates[proto.SubtaskStatePaused] == 0 {
		// Finish the resuming process.
		s.logger.Info("all paused tasks converted to pending state, update the task to running state")
		err := s.updateTask(proto.TaskStateRunning, nil, RetrySQLTimes)
		failpoint.Inject("syncAfterResume", func() {
			TestSyncChan <- struct{}{}
		})
		return err
	}

	return s.taskMgr.ResumeSubtasks(s.ctx, task.ID)
}

// handle task in reverting state, check all revert subtasks finishes.
func (s *BaseScheduler) onReverting() error {
	task := s.GetTask()
	s.logger.Debug("on reverting state", zap.Stringer("state", task.State), zap.String("step", proto.Step2Str(task.Type, task.Step)))
	cntByStates, err := s.taskMgr.GetSubtaskCntGroupByStates(s.ctx, task.ID, task.Step)
	if err != nil {
		s.logger.Warn("check task failed", zap.Error(err))
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
			s.logger.Warn("subtasks encounter errors")
			return s.onErrHandlingStage(subTaskErrs)
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

// updateTask update the task in tidb_global_task table.
func (s *BaseScheduler) updateTask(taskState proto.TaskState, newSubTasks []*proto.Subtask, retryTimes int) (err error) {
	task := *s.GetTask()
	prevState := task.State
	task.State = taskState
	s.task.Store(&task)
	logutil.BgLogger().Info("task state transform", zap.Stringer("from", prevState), zap.Stringer("to", taskState))
	if !VerifyTaskStateTransform(prevState, taskState) {
		return errors.Errorf("invalid task state transform, from %s to %s", prevState, taskState)
	}

	var retryable bool
	for i := 0; i < retryTimes; i++ {
		retryable, err = s.taskMgr.UpdateTaskAndAddSubTasks(s.ctx, &task, newSubTasks, prevState)
		if err == nil || !retryable {
			break
		}
		if err1 := s.ctx.Err(); err1 != nil {
			return err1
		}
		if i%10 == 0 {
			s.logger.Warn("updateTask first failed", zap.Stringer("from", prevState), zap.Stringer("to", task.State),
				zap.Int("retry times", i), zap.Error(err))
		}
		time.Sleep(RetrySQLInterval)
	}
	if err != nil && retryTimes != nonRetrySQLTime {
		s.logger.Warn("updateTask failed",
			zap.Stringer("from", prevState), zap.Stringer("to", task.State), zap.Int("retry times", retryTimes), zap.Error(err))
	}
	return err
}

func (s *BaseScheduler) onErrHandlingStage(receiveErrs []error) error {
	task := *s.GetTask()
	// we only store the first error.
	task.Error = receiveErrs[0]
	s.task.Store(&task)

	var subTasks []*proto.Subtask
	// when step of task is `StepInit`, no need to do revert
	if task.Step != proto.StepInit {
		instanceIDs, err := s.GetAllTaskExecutorIDs(s.ctx, &task)
		if err != nil {
			s.logger.Warn("get task's all instances failed", zap.Error(err))
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
	task := *s.GetTask()
	nextStep := s.GetNextStep(&task)
	s.logger.Info("switch to next step",
		zap.String("current-step", proto.Step2Str(task.Type, task.Step)),
		zap.String("next-step", proto.Step2Str(task.Type, nextStep)))

	if nextStep == proto.StepDone {
		task.Step = nextStep
		task.StateUpdateTime = time.Now().UTC()
		s.task.Store(&task)
		if err = s.OnDone(s.ctx, s, &task); err != nil {
			return errors.Trace(err)
		}
		return s.taskMgr.SucceedTask(s.ctx, task.ID)
	}

	eligibleNodes, err := getEligibleNodes(s.ctx, s, s.nodeMgr.getManagedNodes())
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
	// OnNextSubtasksBatch might change meta of task.
	s.task.Store(&task)

	return s.scheduleSubTask(nextStep, metas, eligibleNodes)
}

func (s *BaseScheduler) scheduleSubTask(
	subtaskStep proto.Step,
	metas [][]byte,
	eligibleNodes []string) error {
	task := s.GetTask()
	s.logger.Info("schedule subtasks",
		zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
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
		func(ctx context.Context) (bool, error) {
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
	task.Error = err
	s.task.Store(&task)

	if err = s.OnDone(s.ctx, s, &task); err != nil {
		return errors.Trace(err)
	}

	return s.taskMgr.FailTask(s.ctx, task.ID, task.State, task.Error)
}

// MockServerInfo exported for scheduler_test.go
var MockServerInfo []*infosync.ServerInfo

// GenerateTaskExecutorNodes generate a eligible TiDB nodes.
func GenerateTaskExecutorNodes(ctx context.Context) (serverNodes []*infosync.ServerInfo, err error) {
	failpoint.Inject("mockTaskExecutorNodes", func() {
		failpoint.Return(MockServerInfo, nil)
	})
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

// GetAllTaskExecutorIDs gets all the task executor IDs.
func (s *BaseScheduler) GetAllTaskExecutorIDs(ctx context.Context, task *proto.Task) ([]string, error) {
	// We get all servers instead of eligible servers here
	// because eligible servers may change during the task execution.
	serverInfos, err := GenerateTaskExecutorNodes(ctx)
	if err != nil {
		return nil, err
	}
	if len(serverInfos) == 0 {
		return nil, nil
	}

	executorIDs, err := s.taskMgr.GetTaskExecutorIDsByTaskID(s.ctx, task.ID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(executorIDs))
	for _, id := range executorIDs {
		if ok := disttaskutil.MatchServerInfo(serverInfos, id); ok {
			ids = append(ids, id)
		}
	}
	return ids, nil
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
