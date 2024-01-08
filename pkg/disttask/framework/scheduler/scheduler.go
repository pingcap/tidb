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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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

// TaskHandle provides the interface for operations needed by Scheduler.
// Then we can use scheduler's function in Scheduler interface.
type TaskHandle interface {
	// GetPreviousSubtaskMetas gets previous subtask metas.
	GetPreviousSubtaskMetas(taskID int64, step proto.Step) ([][]byte, error)
	storage.SessionExecutor
}

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
}

// BaseScheduler is the base struct for Scheduler.
// each task type embed this struct and implement the Extension interface.
type BaseScheduler struct {
	ctx     context.Context
	taskMgr TaskManager
	nodeMgr *NodeManager
	Task    *proto.Task
	logCtx  context.Context
	// when RegisterSchedulerFactory, the factory MUST initialize this fields.
	Extension

	balanceSubtaskTick int
	// TaskNodes stores the exec id of current task executor nodes.
	TaskNodes []string
	// rand is for generating random selection of nodes.
	rand *rand.Rand
}

// MockOwnerChange mock owner change in tests.
var MockOwnerChange func()

// NewBaseScheduler creates a new BaseScheduler.
func NewBaseScheduler(ctx context.Context, taskMgr TaskManager, nodeMgr *NodeManager, task *proto.Task) *BaseScheduler {
	logCtx := logutil.WithFields(context.Background(), zap.Int64("task-id", task.ID),
		zap.Stringer("task-type", task.Type))
	return &BaseScheduler{
		ctx:       ctx,
		taskMgr:   taskMgr,
		nodeMgr:   nodeMgr,
		Task:      task,
		logCtx:    logCtx,
		TaskNodes: nil,
		rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Init implements the Scheduler interface.
func (*BaseScheduler) Init() error {
	return nil
}

// ScheduleTask implements the Scheduler interface.
func (s *BaseScheduler) ScheduleTask() {
	logutil.Logger(s.logCtx).Info("schedule task",
		zap.Stringer("state", s.Task.State), zap.Int("concurrency", s.Task.Concurrency))
	s.scheduleTask()
}

// Close closes the scheduler.
func (*BaseScheduler) Close() {
}

// refreshTask fetch task state from tidb_global_task table.
func (s *BaseScheduler) refreshTask() error {
	newTask, err := s.taskMgr.GetTaskByID(s.ctx, s.Task.ID)
	if err != nil {
		logutil.Logger(s.logCtx).Error("refresh task failed", zap.Error(err))
		return err
	}
	s.Task = newTask
	return nil
}

// scheduleTask schedule the task execution step by step.
func (s *BaseScheduler) scheduleTask() {
	ticker := time.NewTicker(checkTaskFinishedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			logutil.Logger(s.logCtx).Info("schedule task exits", zap.Error(s.ctx.Err()))
			return
		case <-ticker.C:
			err := s.refreshTask()
			if err != nil {
				continue
			}
			failpoint.Inject("cancelTaskAfterRefreshTask", func(val failpoint.Value) {
				if val.(bool) && s.Task.State == proto.TaskStateRunning {
					err := s.taskMgr.CancelTask(s.ctx, s.Task.ID)
					if err != nil {
						logutil.Logger(s.logCtx).Error("cancel task failed", zap.Error(err))
					}
				}
			})

			failpoint.Inject("pausePendingTask", func(val failpoint.Value) {
				if val.(bool) && s.Task.State == proto.TaskStatePending {
					_, err := s.taskMgr.PauseTask(s.ctx, s.Task.Key)
					if err != nil {
						logutil.Logger(s.logCtx).Error("pause task failed", zap.Error(err))
					}
					s.Task.State = proto.TaskStatePausing
				}
			})

			failpoint.Inject("pauseTaskAfterRefreshTask", func(val failpoint.Value) {
				if val.(bool) && s.Task.State == proto.TaskStateRunning {
					_, err := s.taskMgr.PauseTask(s.ctx, s.Task.Key)
					if err != nil {
						logutil.Logger(s.logCtx).Error("pause task failed", zap.Error(err))
					}
					s.Task.State = proto.TaskStatePausing
				}
			})

			switch s.Task.State {
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
				if err := s.onFinished(); err != nil {
					logutil.Logger(s.logCtx).Error("schedule task meet error", zap.Stringer("state", s.Task.State), zap.Error(err))
				}
				return
			}
			if err != nil {
				logutil.Logger(s.logCtx).Info("schedule task meet err, reschedule it", zap.Error(err))
			}

			failpoint.Inject("mockOwnerChange", func(val failpoint.Value) {
				if val.(bool) {
					logutil.Logger(s.logCtx).Info("mockOwnerChange called")
					MockOwnerChange()
					time.Sleep(time.Second)
				}
			})
		}
	}
}

// handle task in cancelling state, schedule revert subtasks.
func (s *BaseScheduler) onCancelling() error {
	logutil.Logger(s.logCtx).Info("on cancelling state", zap.Stringer("state", s.Task.State), zap.Int64("step", int64(s.Task.Step)))
	errs := []error{errors.New(taskCancelMsg)}
	return s.onErrHandlingStage(errs)
}

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
		return s.taskMgr.PausedTask(s.ctx, s.Task.ID)
	}
	logutil.Logger(s.logCtx).Debug("on pausing state, this task keeps current state", zap.Stringer("state", s.Task.State))
	return nil
}

// MockDMLExecutionOnPausedState is used to mock DML execution when tasks pauses.
var MockDMLExecutionOnPausedState func(task *proto.Task)

// handle task in paused state.
func (s *BaseScheduler) onPaused() error {
	logutil.Logger(s.logCtx).Info("on paused state", zap.Stringer("state", s.Task.State), zap.Int64("step", int64(s.Task.Step)))
	failpoint.Inject("mockDMLExecutionOnPausedState", func(val failpoint.Value) {
		if val.(bool) {
			MockDMLExecutionOnPausedState(s.Task)
		}
	})
	return nil
}

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
		return s.taskMgr.RevertedTask(s.ctx, s.Task.ID)
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
		func(ctx context.Context) (bool, error) {
			err := fn(s.ctx, s.Task, proto.TaskStateRunning, subtaskStep, subTasks)
			if errors.Cause(err) == storage.ErrUnstableSubtasks {
				return false, err
			}
			return true, err
		},
	)
}

func (s *BaseScheduler) handlePlanErr(err error) error {
	logutil.Logger(s.logCtx).Warn("generate plan failed", zap.Error(err), zap.Stringer("state", s.Task.State))
	if s.IsRetryableErr(err) {
		return err
	}
	s.Task.Error = err
	if err = s.OnDone(s.ctx, s, s.Task); err != nil {
		return errors.Trace(err)
	}

	return s.taskMgr.FailTask(s.ctx, s.Task.ID, s.Task.State, s.Task.Error)
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
	previousSubtasks, err := s.taskMgr.GetSubtasksByStepAndState(s.ctx, taskID, step, proto.TaskStateSucceed)
	if err != nil {
		logutil.Logger(s.logCtx).Warn("get previous succeed subtask failed", zap.Int64("step", int64(step)))
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
