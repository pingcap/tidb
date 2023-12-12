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

package dispatcher

import (
	"context"
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
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
	// DefaultLiveNodesCheckInterval is the tick interval of fetching all server infos from etcd.
	DefaultLiveNodesCheckInterval = 2
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

// TaskHandle provides the interface for operations needed by Dispatcher.
// Then we can use dispatcher's function in Dispatcher interface.
type TaskHandle interface {
	// GetPreviousTaskExecutorIDs gets previous task executor IDs.
	GetPreviousTaskExecutorIDs(_ context.Context, taskID int64, step proto.Step) ([]string, error)
	// GetPreviousSubtaskMetas gets previous subtask metas.
	GetPreviousSubtaskMetas(taskID int64, step proto.Step) ([][]byte, error)
	storage.SessionExecutor
}

// Dispatcher manages the lifetime of a task
// including submitting subtasks and updating the status of a task.
type Dispatcher interface {
	// Init initializes the dispatcher, should be called before ExecuteTask.
	// if Init returns error, dispatcher manager will fail the task directly,
	// so the returned error should be a fatal error.
	Init() error
	// ExecuteTask start to schedule a task.
	ExecuteTask()
	// Close closes the dispatcher, should be called if Init returns nil.
	Close()
}

// BaseDispatcher is the base struct for Dispatcher.
// each task type embed this struct and implement the Extension interface.
type BaseDispatcher struct {
	ctx     context.Context
	taskMgr TaskManager
	Task    *proto.Task
	logCtx  context.Context
	// serverID, it's value is ip:port now.
	serverID string
	// when RegisterDispatcherFactory, the factory MUST initialize this field.
	Extension

	// For subtasks rebalance.
	// LiveNodes will fetch and store all live nodes every liveNodeInterval ticks.
	LiveNodes             []*infosync.ServerInfo
	liveNodeFetchInterval int
	// liveNodeFetchTick is the tick variable.
	liveNodeFetchTick int
	// TaskNodes stores the id of current task executor nodes.
	TaskNodes []string
	// rand is for generating random selection of nodes.
	rand *rand.Rand
}

// MockOwnerChange mock owner change in tests.
var MockOwnerChange func()

// NewBaseDispatcher creates a new BaseDispatcher.
func NewBaseDispatcher(ctx context.Context, taskMgr TaskManager, serverID string, task *proto.Task) *BaseDispatcher {
	logCtx := logutil.WithFields(context.Background(), zap.Int64("task-id", task.ID),
		zap.Stringer("task-type", task.Type))
	return &BaseDispatcher{
		ctx:                   ctx,
		taskMgr:               taskMgr,
		Task:                  task,
		logCtx:                logCtx,
		serverID:              serverID,
		LiveNodes:             nil,
		liveNodeFetchInterval: DefaultLiveNodesCheckInterval,
		liveNodeFetchTick:     0,
		TaskNodes:             nil,
		rand:                  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Init implements the Dispatcher interface.
func (*BaseDispatcher) Init() error {
	return nil
}

// ExecuteTask implements the Dispatcher interface.
func (d *BaseDispatcher) ExecuteTask() {
	logutil.Logger(d.logCtx).Info("execute one task",
		zap.Stringer("state", d.Task.State), zap.Int("concurrency", d.Task.Concurrency))
	d.scheduleTask()
}

// Close closes the dispatcher.
func (*BaseDispatcher) Close() {
}

// refreshTask fetch task state from tidb_global_task table.
func (d *BaseDispatcher) refreshTask() error {
	newTask, err := d.taskMgr.GetTaskByID(d.ctx, d.Task.ID)
	if err != nil {
		logutil.Logger(d.logCtx).Error("refresh task failed", zap.Error(err))
		return err
	}
	// newTask might be nil when GC routine move the task into history table.
	if newTask != nil {
		d.Task = newTask
	}
	return nil
}

// scheduleTask schedule the task execution step by step.
func (d *BaseDispatcher) scheduleTask() {
	ticker := time.NewTicker(checkTaskFinishedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			logutil.Logger(d.logCtx).Info("schedule task exits", zap.Error(d.ctx.Err()))
			return
		case <-ticker.C:
			err := d.refreshTask()
			if err != nil {
				continue
			}
			failpoint.Inject("cancelTaskAfterRefreshTask", func(val failpoint.Value) {
				if val.(bool) && d.Task.State == proto.TaskStateRunning {
					err := d.taskMgr.CancelTask(d.ctx, d.Task.ID)
					if err != nil {
						logutil.Logger(d.logCtx).Error("cancel task failed", zap.Error(err))
					}
				}
			})

			failpoint.Inject("pausePendingTask", func(val failpoint.Value) {
				if val.(bool) && d.Task.State == proto.TaskStatePending {
					_, err := d.taskMgr.PauseTask(d.ctx, d.Task.Key)
					if err != nil {
						logutil.Logger(d.logCtx).Error("pause task failed", zap.Error(err))
					}
					d.Task.State = proto.TaskStatePausing
				}
			})

			failpoint.Inject("pauseTaskAfterRefreshTask", func(val failpoint.Value) {
				if val.(bool) && d.Task.State == proto.TaskStateRunning {
					_, err := d.taskMgr.PauseTask(d.ctx, d.Task.Key)
					if err != nil {
						logutil.Logger(d.logCtx).Error("pause task failed", zap.Error(err))
					}
					d.Task.State = proto.TaskStatePausing
				}
			})

			switch d.Task.State {
			case proto.TaskStateCancelling:
				err = d.onCancelling()
			case proto.TaskStatePausing:
				err = d.onPausing()
			case proto.TaskStatePaused:
				err = d.onPaused()
				// close the dispatcher.
				if err == nil {
					return
				}
			case proto.TaskStateResuming:
				err = d.onResuming()
			case proto.TaskStateReverting:
				err = d.onReverting()
			case proto.TaskStatePending:
				err = d.onPending()
			case proto.TaskStateRunning:
				err = d.onRunning()
			case proto.TaskStateSucceed, proto.TaskStateReverted, proto.TaskStateFailed:
				if err := d.onFinished(); err != nil {
					logutil.Logger(d.logCtx).Error("schedule task meet error", zap.Stringer("state", d.Task.State), zap.Error(err))
				}
				return
			}
			if err != nil {
				logutil.Logger(d.logCtx).Info("schedule task meet err, reschedule it", zap.Error(err))
			}

			failpoint.Inject("mockOwnerChange", func(val failpoint.Value) {
				if val.(bool) {
					logutil.Logger(d.logCtx).Info("mockOwnerChange called")
					MockOwnerChange()
					time.Sleep(time.Second)
				}
			})
		}
	}
}

// handle task in cancelling state, dispatch revert subtasks.
func (d *BaseDispatcher) onCancelling() error {
	logutil.Logger(d.logCtx).Info("on cancelling state", zap.Stringer("state", d.Task.State), zap.Int64("stage", int64(d.Task.Step)))
	errs := []error{errors.New(taskCancelMsg)}
	return d.onErrHandlingStage(errs)
}

// handle task in pausing state, cancel all running subtasks.
func (d *BaseDispatcher) onPausing() error {
	logutil.Logger(d.logCtx).Info("on pausing state", zap.Stringer("state", d.Task.State), zap.Int64("stage", int64(d.Task.Step)))
	cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.ctx, d.Task.ID, proto.TaskStateRunning, proto.TaskStatePending)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	if cnt == 0 {
		logutil.Logger(d.logCtx).Info("all running subtasks paused, update the task to paused state")
		return d.updateTask(proto.TaskStatePaused, nil, RetrySQLTimes)
	}
	logutil.Logger(d.logCtx).Debug("on pausing state, this task keeps current state", zap.Stringer("state", d.Task.State))
	return nil
}

// MockDMLExecutionOnPausedState is used to mock DML execution when tasks paused.
var MockDMLExecutionOnPausedState func(task *proto.Task)

// handle task in paused state.
func (d *BaseDispatcher) onPaused() error {
	logutil.Logger(d.logCtx).Info("on paused state", zap.Stringer("state", d.Task.State), zap.Int64("stage", int64(d.Task.Step)))
	failpoint.Inject("mockDMLExecutionOnPausedState", func(val failpoint.Value) {
		if val.(bool) {
			MockDMLExecutionOnPausedState(d.Task)
		}
	})
	return nil
}

// TestSyncChan is used to sync the test.
var TestSyncChan = make(chan struct{})

// handle task in resuming state.
func (d *BaseDispatcher) onResuming() error {
	logutil.Logger(d.logCtx).Info("on resuming state", zap.Stringer("state", d.Task.State), zap.Int64("stage", int64(d.Task.Step)))
	cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.ctx, d.Task.ID, proto.TaskStatePaused)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	if cnt == 0 {
		// Finish the resuming process.
		logutil.Logger(d.logCtx).Info("all paused tasks converted to pending state, update the task to running state")
		err := d.updateTask(proto.TaskStateRunning, nil, RetrySQLTimes)
		failpoint.Inject("syncAfterResume", func() {
			TestSyncChan <- struct{}{}
		})
		return err
	}

	return d.taskMgr.ResumeSubtasks(d.ctx, d.Task.ID)
}

// handle task in reverting state, check all revert subtasks finished.
func (d *BaseDispatcher) onReverting() error {
	logutil.Logger(d.logCtx).Debug("on reverting state", zap.Stringer("state", d.Task.State), zap.Int64("stage", int64(d.Task.Step)))
	cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.ctx, d.Task.ID, proto.TaskStateRevertPending, proto.TaskStateReverting)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	if cnt == 0 {
		if err = d.OnDone(d.ctx, d, d.Task); err != nil {
			return errors.Trace(err)
		}
		return d.updateTask(proto.TaskStateReverted, nil, RetrySQLTimes)
	}
	// Wait all subtasks in this stage finished.
	d.OnTick(d.ctx, d.Task)
	logutil.Logger(d.logCtx).Debug("on reverting state, this task keeps current state", zap.Stringer("state", d.Task.State))
	return nil
}

// handle task in pending state, dispatch subtasks.
func (d *BaseDispatcher) onPending() error {
	logutil.Logger(d.logCtx).Debug("on pending state", zap.Stringer("state", d.Task.State), zap.Int64("stage", int64(d.Task.Step)))
	return d.onNextStage()
}

// handle task in running state, check all running subtasks finished.
// If subtasks finished, run into the next stage.
func (d *BaseDispatcher) onRunning() error {
	logutil.Logger(d.logCtx).Debug("on running state", zap.Stringer("state", d.Task.State), zap.Int64("stage", int64(d.Task.Step)))
	subTaskErrs, err := d.taskMgr.CollectSubTaskError(d.ctx, d.Task.ID)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("collect subtask error failed", zap.Error(err))
		return err
	}
	if len(subTaskErrs) > 0 {
		logutil.Logger(d.logCtx).Warn("subtasks encounter errors")
		return d.onErrHandlingStage(subTaskErrs)
	}
	// check current stage finished.
	cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.ctx, d.Task.ID, proto.TaskStatePending, proto.TaskStateRunning)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}

	if cnt == 0 {
		return d.onNextStage()
	}

	if err := d.BalanceSubtasks(); err != nil {
		return err
	}
	// Wait all subtasks in this stage finished.
	d.OnTick(d.ctx, d.Task)
	logutil.Logger(d.logCtx).Debug("on running state, this task keeps current state", zap.Stringer("state", d.Task.State))
	return nil
}

func (d *BaseDispatcher) onFinished() error {
	metrics.UpdateMetricsForFinishTask(d.Task)
	logutil.Logger(d.logCtx).Debug("schedule task, task is finished", zap.Stringer("state", d.Task.State))
	return d.taskMgr.TransferSubTasks2History(d.ctx, d.Task.ID)
}

// BalanceSubtasks check the liveNode num every liveNodeFetchInterval then rebalance subtasks.
func (d *BaseDispatcher) BalanceSubtasks() error {
	// 1. init TaskNodes if needed.
	if len(d.TaskNodes) == 0 {
		var err error
		d.TaskNodes, err = d.taskMgr.GetTaskExecutorIDsByTaskIDAndStep(d.ctx, d.Task.ID, d.Task.Step)
		if err != nil {
			return err
		}
	}
	d.liveNodeFetchTick++
	if d.liveNodeFetchTick == d.liveNodeFetchInterval {
		// 2. update LiveNodes.
		d.liveNodeFetchTick = 0
		serverInfos, err := GenerateTaskExecutorNodes(d.ctx)
		if err != nil {
			return err
		}

		eligibleServerInfos, filter, err := d.GetEligibleInstances(d.ctx, d.Task)
		if err != nil {
			return err
		}
		if filter {
			eligibleServerInfos, err = d.filterByRole(eligibleServerInfos)
			if err != nil {
				return err
			}
		}
		newInfos := serverInfos[:0]
		for _, m := range serverInfos {
			found := false
			for _, n := range eligibleServerInfos {
				if m.ID == n.ID {
					found = true
					break
				}
			}
			if found {
				newInfos = append(newInfos, m)
			}
		}
		d.LiveNodes = newInfos
		// 3. balance subtasks.
		if len(d.LiveNodes) > 0 {
			return d.ReDispatchSubtasks()
		}
		return nil
	}
	return nil
}

func (d *BaseDispatcher) replaceTaskNodes() {
	d.TaskNodes = d.TaskNodes[:0]
	for _, serverInfo := range d.LiveNodes {
		d.TaskNodes = append(d.TaskNodes, disttaskutil.GenerateExecID(serverInfo.IP, serverInfo.Port))
	}
}

// ReDispatchSubtasks make count of subtasks on each liveNodes balanced and clean up subtasks on dead nodes.
// TODO(ywqzzy): refine to make it easier for testing.
func (d *BaseDispatcher) ReDispatchSubtasks() error {
	// 1. find out nodes need to clean subtasks.
	deadNodes := make([]string, 0)
	deadNodesMap := make(map[string]bool, 0)
	for _, node := range d.TaskNodes {
		if !disttaskutil.MatchServerInfo(d.LiveNodes, node) {
			deadNodes = append(deadNodes, node)
			deadNodesMap[node] = true
		}
	}
	// 2. get subtasks for each node before rebalance.
	subtasks, err := d.taskMgr.GetSubtasksByStepAndState(d.ctx, d.Task.ID, d.Task.Step, proto.TaskStatePending)
	if err != nil {
		return err
	}
	if len(deadNodes) != 0 {
		/// get subtask from deadNodes, since there might be some running subtasks on deadNodes.
		/// In this case, all subtasks on deadNodes are in running/pending state.
		subtasksOnDeadNodes, err := d.taskMgr.GetSubtasksByExecIdsAndStepAndState(d.ctx, deadNodes, d.Task.ID, d.Task.Step, proto.TaskStateRunning)
		if err != nil {
			return err
		}
		subtasks = append(subtasks, subtasksOnDeadNodes...)
	}
	// 3. group subtasks for each task executor.
	subtasksOnTaskExecutor := make(map[string][]*proto.Subtask, len(d.LiveNodes)+len(deadNodes))
	for _, node := range d.LiveNodes {
		execID := disttaskutil.GenerateExecID(node.IP, node.Port)
		subtasksOnTaskExecutor[execID] = make([]*proto.Subtask, 0)
	}
	for _, subtask := range subtasks {
		subtasksOnTaskExecutor[subtask.ExecID] = append(
			subtasksOnTaskExecutor[subtask.ExecID],
			subtask)
	}
	// 4. prepare subtasks that need to rebalance to other nodes.
	averageSubtaskCnt := len(subtasks) / len(d.LiveNodes)
	rebalanceSubtasks := make([]*proto.Subtask, 0)
	for k, v := range subtasksOnTaskExecutor {
		if ok := deadNodesMap[k]; ok {
			rebalanceSubtasks = append(rebalanceSubtasks, v...)
			continue
		}
		// When no tidb scale-in/out and averageSubtaskCnt*len(d.LiveNodes) < len(subtasks),
		// no need to send subtask to other nodes.
		// eg: tidb1 with 3 subtasks, tidb2 with 2 subtasks, subtasks are balanced now.
		if averageSubtaskCnt*len(d.LiveNodes) < len(subtasks) && len(d.TaskNodes) == len(d.LiveNodes) {
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
		node := d.LiveNodes[liveNodeIdx]
		rebalanceSubtasks[rebalanceIdx].ExecID = disttaskutil.GenerateExecID(node.IP, node.Port)
		rebalanceIdx++
		liveNodeIdx++
	}

	// 8. update subtasks and do clean up logic.
	if err = d.taskMgr.UpdateSubtasksExecIDs(d.ctx, d.Task.ID, subtasks); err != nil {
		return err
	}
	logutil.Logger(d.logCtx).Info("rebalance subtasks",
		zap.Stringers("subtasks-rebalanced", subtasks))
	if err = d.taskMgr.CleanUpMeta(d.ctx, deadNodes); err != nil {
		return err
	}
	d.replaceTaskNodes()
	return nil
}

// updateTask update the task in tidb_global_task table.
func (d *BaseDispatcher) updateTask(taskState proto.TaskState, newSubTasks []*proto.Subtask, retryTimes int) (err error) {
	prevState := d.Task.State
	d.Task.State = taskState
	logutil.BgLogger().Info("task state transform", zap.Stringer("from", prevState), zap.Stringer("to", taskState))
	if !VerifyTaskStateTransform(prevState, taskState) {
		return errors.Errorf("invalid task state transform, from %s to %s", prevState, taskState)
	}

	failpoint.Inject("cancelBeforeUpdate", func() {
		err := d.taskMgr.CancelTask(d.ctx, d.Task.ID)
		if err != nil {
			logutil.Logger(d.logCtx).Error("cancel task failed", zap.Error(err))
		}
	})

	var retryable bool
	for i := 0; i < retryTimes; i++ {
		retryable, err = d.taskMgr.UpdateTaskAndAddSubTasks(d.ctx, d.Task, newSubTasks, prevState)
		if err == nil || !retryable {
			break
		}
		if i%10 == 0 {
			logutil.Logger(d.logCtx).Warn("updateTask first failed", zap.Stringer("from", prevState), zap.Stringer("to", d.Task.State),
				zap.Int("retry times", i), zap.Error(err))
		}
		time.Sleep(RetrySQLInterval)
	}
	if err != nil && retryTimes != nonRetrySQLTime {
		logutil.Logger(d.logCtx).Warn("updateTask failed",
			zap.Stringer("from", prevState), zap.Stringer("to", d.Task.State), zap.Int("retry times", retryTimes), zap.Error(err))
	}
	return err
}

func (d *BaseDispatcher) onErrHandlingStage(receiveErrs []error) error {
	// we only store the first error.
	d.Task.Error = receiveErrs[0]

	var subTasks []*proto.Subtask
	// when step of task is `StepInit`, no need to do revert
	if d.Task.Step != proto.StepInit {
		instanceIDs, err := d.GetAllTaskExecutorIDs(d.ctx, d.Task)
		if err != nil {
			logutil.Logger(d.logCtx).Warn("get task's all instances failed", zap.Error(err))
			return err
		}

		subTasks = make([]*proto.Subtask, 0, len(instanceIDs))
		for _, id := range instanceIDs {
			// reverting subtasks belong to the same step as current active step.
			subTasks = append(subTasks, proto.NewSubtask(d.Task.Step, d.Task.ID, d.Task.Type, id, d.Task.Concurrency, []byte("{}")))
		}
	}
	return d.updateTask(proto.TaskStateReverting, subTasks, RetrySQLTimes)
}

func (*BaseDispatcher) nextStepSubtaskDispatched(*proto.Task) bool {
	// TODO: will implement it when we we support dispatch subtask by batch.
	// since subtask meta might be too large to save in one transaction.
	return true
}

func (d *BaseDispatcher) onNextStage() (err error) {
	/// dynamic dispatch subtasks.
	failpoint.Inject("mockDynamicDispatchErr", func() {
		failpoint.Return(errors.New("mockDynamicDispatchErr"))
	})

	nextStep := d.GetNextStep(d.Task)
	logutil.Logger(d.logCtx).Info("onNextStage",
		zap.Int64("current-step", int64(d.Task.Step)),
		zap.Int64("next-step", int64(nextStep)))

	// 1. Adjust the task's concurrency.
	if d.Task.State == proto.TaskStatePending {
		if d.Task.Concurrency == 0 {
			d.Task.Concurrency = DefaultSubtaskConcurrency
		}
		if d.Task.Concurrency > MaxSubtaskConcurrency {
			d.Task.Concurrency = MaxSubtaskConcurrency
		}
	}
	defer func() {
		if err != nil {
			return
		}
		// invariant: task.Step always means the most recent step that all
		// corresponding subtasks have been saved to system table.
		//
		// when all subtasks of task.Step is finished, we call OnNextSubtasksBatch
		// to generate subtasks of next step. after all subtasks of next step are
		// saved to system table, we will update task.Step to next step, so the
		// invariant hold.
		// see nextStepSubtaskDispatched for why we don't update task and subtasks
		// in a single transaction.
		if d.nextStepSubtaskDispatched(d.Task) {
			currStep := d.Task.Step
			d.Task.Step = nextStep
			// When all subtasks dispatched and processed, mark task as succeed.
			taskState := proto.TaskStateRunning
			if d.Task.Step == proto.StepDone {
				taskState = proto.TaskStateSucceed
				if err = d.OnDone(d.ctx, d, d.Task); err != nil {
					err = errors.Trace(err)
					return
				}
			} else {
				logutil.Logger(d.logCtx).Info("move to next stage",
					zap.Int64("from", int64(currStep)), zap.Int64("to", int64(d.Task.Step)))
			}
			d.Task.StateUpdateTime = time.Now().UTC()
			err = d.updateTask(taskState, nil, RetrySQLTimes)
		}
	}()

	for {
		// 3. generate a batch of subtasks.
		/// select all available TiDB nodes for task.
		serverNodes, filter, err := d.GetEligibleInstances(d.ctx, d.Task)
		logutil.Logger(d.logCtx).Debug("eligible instances", zap.Int("num", len(serverNodes)))

		if err != nil {
			return err
		}
		if filter {
			serverNodes, err = d.filterByRole(serverNodes)
			if err != nil {
				return err
			}
		}
		logutil.Logger(d.logCtx).Info("eligible instances", zap.Int("num", len(serverNodes)))
		if len(serverNodes) == 0 {
			return errors.New("no available TiDB node to dispatch subtasks")
		}

		metas, err := d.OnNextSubtasksBatch(d.ctx, d, d.Task, serverNodes, nextStep)
		if err != nil {
			logutil.Logger(d.logCtx).Warn("generate part of subtasks failed", zap.Error(err))
			return d.handlePlanErr(err)
		}

		failpoint.Inject("mockDynamicDispatchErr1", func() {
			failpoint.Return(errors.New("mockDynamicDispatchErr1"))
		})

		// 4. dispatch batch of subtasks to EligibleInstances.
		err = d.dispatchSubTask(nextStep, metas, serverNodes)
		if err != nil {
			return err
		}

		if d.nextStepSubtaskDispatched(d.Task) {
			break
		}

		failpoint.Inject("mockDynamicDispatchErr2", func() {
			failpoint.Return(errors.New("mockDynamicDispatchErr2"))
		})
	}
	return nil
}

func (d *BaseDispatcher) dispatchSubTask(
	subtaskStep proto.Step,
	metas [][]byte,
	serverNodes []*infosync.ServerInfo) error {
	logutil.Logger(d.logCtx).Info("dispatch subtasks",
		zap.Stringer("state", d.Task.State),
		zap.Int64("step", int64(d.Task.Step)),
		zap.Int("concurrency", d.Task.Concurrency),
		zap.Int("subtasks", len(metas)))
	d.TaskNodes = make([]string, len(serverNodes))
	for i := range serverNodes {
		d.TaskNodes[i] = disttaskutil.GenerateExecID(serverNodes[i].IP, serverNodes[i].Port)
	}
	subTasks := make([]*proto.Subtask, 0, len(metas))
	for i, meta := range metas {
		// we assign the subtask to the instance in a round-robin way.
		// TODO: assign the subtask to the instance according to the system load of each nodes
		pos := i % len(serverNodes)
		instanceID := disttaskutil.GenerateExecID(serverNodes[pos].IP, serverNodes[pos].Port)
		logutil.Logger(d.logCtx).Debug("create subtasks", zap.String("instanceID", instanceID))
		subTasks = append(subTasks, proto.NewSubtask(subtaskStep, d.Task.ID, d.Task.Type, instanceID, d.Task.Concurrency, meta))
	}
	failpoint.Inject("cancelBeforeUpdateTask", func() {
		_ = d.updateTask(proto.TaskStateCancelling, subTasks, RetrySQLTimes)
	})

	return d.updateTask(d.Task.State, subTasks, RetrySQLTimes)
}

func (d *BaseDispatcher) handlePlanErr(err error) error {
	logutil.Logger(d.logCtx).Warn("generate plan failed", zap.Error(err), zap.Stringer("state", d.Task.State))
	if d.IsRetryableErr(err) {
		return err
	}
	d.Task.Error = err

	if err = d.OnDone(d.ctx, d, d.Task); err != nil {
		return errors.Trace(err)
	}
	return d.updateTask(proto.TaskStateFailed, nil, RetrySQLTimes)
}

// MockServerInfo exported for dispatcher_test.go
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

func (d *BaseDispatcher) filterByRole(infos []*infosync.ServerInfo) ([]*infosync.ServerInfo, error) {
	nodes, err := d.taskMgr.GetManagedNodes(d.ctx)
	if err != nil {
		return nil, err
	}

	nodeMap := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		nodeMap[node] = struct{}{}
	}

	res := make([]*infosync.ServerInfo, 0, len(nodes))
	for _, info := range infos {
		_, ok := nodeMap[disttaskutil.GenerateExecID(info.IP, info.Port)]
		if ok {
			res = append(res, info)
		}
	}
	return res, nil
}

// GetAllTaskExecutorIDs gets all the task executor IDs.
func (d *BaseDispatcher) GetAllTaskExecutorIDs(ctx context.Context, task *proto.Task) ([]string, error) {
	// We get all servers instead of eligible servers here
	// because eligible servers may change during the task execution.
	serverInfos, err := GenerateTaskExecutorNodes(ctx)
	if err != nil {
		return nil, err
	}
	if len(serverInfos) == 0 {
		return nil, nil
	}

	executorIDs, err := d.taskMgr.GetTaskExecutorIDsByTaskID(d.ctx, task.ID)
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
func (d *BaseDispatcher) GetPreviousSubtaskMetas(taskID int64, step proto.Step) ([][]byte, error) {
	previousSubtasks, err := d.taskMgr.GetSubtasksByStepAndState(d.ctx, taskID, step, proto.TaskStateSucceed)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("get previous succeed subtask failed", zap.Int64("step", int64(step)))
		return nil, err
	}
	previousSubtaskMetas := make([][]byte, 0, len(previousSubtasks))
	for _, subtask := range previousSubtasks {
		previousSubtaskMetas = append(previousSubtaskMetas, subtask.Meta)
	}
	return previousSubtaskMetas, nil
}

// GetPreviousTaskExecutorIDs gets task executor IDs that run previous step.
func (d *BaseDispatcher) GetPreviousTaskExecutorIDs(_ context.Context, taskID int64, step proto.Step) ([]string, error) {
	return d.taskMgr.GetTaskExecutorIDsByTaskIDAndStep(d.ctx, taskID, step)
}

// WithNewSession executes the function with a new session.
func (d *BaseDispatcher) WithNewSession(fn func(se sessionctx.Context) error) error {
	return d.taskMgr.WithNewSession(fn)
}

// WithNewTxn executes the fn in a new transaction.
func (d *BaseDispatcher) WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error {
	return d.taskMgr.WithNewTxn(ctx, fn)
}

// IsCancelledErr checks if the error is a cancelled error.
func IsCancelledErr(err error) bool {
	return strings.Contains(err.Error(), taskCancelMsg)
}
