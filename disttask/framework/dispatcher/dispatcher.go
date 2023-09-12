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
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	disttaskutil "github.com/pingcap/tidb/util/disttask"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultSubtaskConcurrency is the default concurrency for handling subtask.
	DefaultSubtaskConcurrency = 16
	// MaxSubtaskConcurrency is the maximum concurrency for handling subtask.
	MaxSubtaskConcurrency = 256
	// DefaultLiveNodesCheckInterval is the tick interval of fetching all server infos from etcd.
	DefaultLiveNodesCheckInterval = 2
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
	// GetPreviousSubtaskMetas gets previous subtask metas.
	GetPreviousSubtaskMetas(taskID int64, step int64) ([][]byte, error)
	// UpdateTask update the task in tidb_global_task table.
	UpdateTask(taskState string, newSubTasks []*proto.Subtask, retryTimes int) error
	storage.SessionExecutor
}

// Dispatcher manages the lifetime of a task
// including submitting subtasks and updating the status of a task.
type Dispatcher interface {
	ExecuteTask()
	// Close closes the dispatcher, not routine-safe, and should be called
	// after ExecuteTask finished.
	Close()
}

// BaseDispatcher is the base struct for Dispatcher.
// each task type embed this struct and implement the Extension interface.
type BaseDispatcher struct {
	ctx      context.Context
	taskMgr  *storage.TaskManager
	Task     *proto.Task
	logCtx   context.Context
	serverID string
	// when RegisterDispatcherFactory, the factory MUST initialize this field.
	Extension

	// for HA
	// liveNodes will fetch and store all live nodes every liveNodeInterval ticks.
	liveNodes             []*infosync.ServerInfo
	liveNodeFetchInterval int
	// liveNodeFetchTick is the tick variable.
	liveNodeFetchTick int
	// taskNodes stores the id of current scheduler nodes.
	taskNodes []string
	// rand is for generating random selection of nodes.
	rand *rand.Rand
}

// MockOwnerChange mock owner change in tests.
var MockOwnerChange func()

// NewBaseDispatcher creates a new BaseDispatcher.
func NewBaseDispatcher(ctx context.Context, taskMgr *storage.TaskManager, serverID string, task *proto.Task) *BaseDispatcher {
	logPrefix := fmt.Sprintf("task_id: %d, task_type: %s, server_id: %s", task.ID, task.Type, serverID)
	return &BaseDispatcher{
		ctx:                   ctx,
		taskMgr:               taskMgr,
		Task:                  task,
		logCtx:                logutil.WithKeyValue(context.Background(), "dispatcher", logPrefix),
		serverID:              serverID,
		liveNodes:             nil,
		liveNodeFetchInterval: DefaultLiveNodesCheckInterval,
		liveNodeFetchTick:     0,
		taskNodes:             nil,
		rand:                  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ExecuteTask start to schedule a task.
func (d *BaseDispatcher) ExecuteTask() {
	logutil.Logger(d.logCtx).Info("execute one task",
		zap.String("state", d.Task.State), zap.Uint64("concurrency", d.Task.Concurrency))
	d.scheduleTask()
	// TODO: manage history task table.
}

// Close closes the dispatcher.
func (*BaseDispatcher) Close() {
}

// refreshTask fetch task state from tidb_global_task table.
func (d *BaseDispatcher) refreshTask() (err error) {
	d.Task, err = d.taskMgr.GetGlobalTaskByID(d.Task.ID)
	if err != nil {
		logutil.Logger(d.logCtx).Error("refresh task failed", zap.Error(err))
	}
	return err
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
					err := d.taskMgr.CancelGlobalTask(d.Task.ID)
					if err != nil {
						logutil.Logger(d.logCtx).Error("cancel task failed", zap.Error(err))
					}
				}
			})
			switch d.Task.State {
			case proto.TaskStateCancelling:
				err = d.onCancelling()
			case proto.TaskStateReverting:
				err = d.onReverting()
			case proto.TaskStatePending:
				err = d.onPending()
			case proto.TaskStateRunning:
				err = d.onRunning()
			case proto.TaskStateSucceed, proto.TaskStateReverted, proto.TaskStateFailed:
				logutil.Logger(d.logCtx).Info("schedule task, task is finished", zap.String("state", d.Task.State))
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
	logutil.Logger(d.logCtx).Info("on cancelling state", zap.String("state", d.Task.State), zap.Int64("stage", d.Task.Step))
	errs := []error{errors.New("cancel")}
	return d.onErrHandlingStage(errs)
}

// handle task in reverting state, check all revert subtasks finished.
func (d *BaseDispatcher) onReverting() error {
	logutil.Logger(d.logCtx).Debug("on reverting state", zap.String("state", d.Task.State), zap.Int64("stage", d.Task.Step))
	cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.Task.ID, proto.TaskStateRevertPending, proto.TaskStateReverting)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	if cnt == 0 {
		// Finish the rollback step.
		logutil.Logger(d.logCtx).Info("all reverting tasks finished, update the task to reverted state")
		return d.UpdateTask(proto.TaskStateReverted, nil, RetrySQLTimes)
	}
	// Wait all subtasks in this stage finished.
	d.OnTick(d.ctx, d.Task)
	logutil.Logger(d.logCtx).Debug("on reverting state, this task keeps current state", zap.String("state", d.Task.State))
	return nil
}

// handle task in pending state, dispatch subtasks.
func (d *BaseDispatcher) onPending() error {
	logutil.Logger(d.logCtx).Debug("on pending state", zap.String("state", d.Task.State), zap.Int64("stage", d.Task.Step))
	return d.onNextStage()
}

// handle task in running state, check all running subtasks finished.
// If subtasks finished, run into the next stage.
func (d *BaseDispatcher) onRunning() error {
	logutil.Logger(d.logCtx).Debug("on running state", zap.String("state", d.Task.State), zap.Int64("stage", d.Task.Step))
	subTaskErrs, err := d.taskMgr.CollectSubTaskError(d.Task.ID)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("collect subtask error failed", zap.Error(err))
		return err
	}
	if len(subTaskErrs) > 0 {
		logutil.Logger(d.logCtx).Warn("subtasks encounter errors")
		return d.onErrHandlingStage(subTaskErrs)
	}
	// check current stage finished.
	cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.Task.ID, proto.TaskStatePending, proto.TaskStateRunning)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}

	if cnt == 0 {
		logutil.Logger(d.logCtx).Info("previous subtasks finished, generate dist plan", zap.Int64("stage", d.Task.Step))
		// When all subtasks dispatched and processed, mark task as succeed.
		if d.Finished(d.Task) {
			d.Task.StateUpdateTime = time.Now().UTC()
			logutil.Logger(d.logCtx).Info("all subtasks dispatched and processed, finish the task")
			err := d.UpdateTask(proto.TaskStateSucceed, nil, RetrySQLTimes)
			if err != nil {
				logutil.Logger(d.logCtx).Warn("update task failed", zap.Error(err))
				return err
			}
			return nil
		}
		return d.onNextStage()
	}
	// Check if any node are down.
	if err := d.replaceDeadNodesIfAny(); err != nil {
		return err
	}
	// Wait all subtasks in this stage finished.
	d.OnTick(d.ctx, d.Task)
	logutil.Logger(d.logCtx).Debug("on running state, this task keeps current state", zap.String("state", d.Task.State))
	return nil
}

func (d *BaseDispatcher) replaceDeadNodesIfAny() error {
	if len(d.taskNodes) == 0 {
		var err error
		d.taskNodes, err = d.taskMgr.GetSchedulerIDsByTaskIDAndStep(d.Task.ID, d.Task.Step)
		if err != nil {
			return err
		}
	}
	d.liveNodeFetchTick++
	if d.liveNodeFetchTick == d.liveNodeFetchInterval {
		d.liveNodeFetchTick = 0
		serverInfos, err := GenerateSchedulerNodes(d.ctx)
		if err != nil {
			return err
		}
		eligibleServerInfos, err := d.GetEligibleInstances(d.ctx, d.Task)
		if err != nil {
			return err
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
		d.liveNodes = newInfos
	}
	if len(d.liveNodes) > 0 {
		replaceNodes := make(map[string]string)
		for _, nodeID := range d.taskNodes {
			if ok := disttaskutil.MatchServerInfo(d.liveNodes, nodeID); !ok {
				n := d.liveNodes[d.rand.Int()%len(d.liveNodes)] //nolint:gosec
				replaceNodes[nodeID] = disttaskutil.GenerateExecID(n.IP, n.Port)
			}
		}
		if err := d.taskMgr.UpdateFailedSchedulerIDs(d.Task.ID, replaceNodes); err != nil {
			return err
		}
		// replace local cache.
		for k, v := range replaceNodes {
			for m, n := range d.taskNodes {
				if n == k {
					d.taskNodes[m] = v
					break
				}
			}
		}
	}
	return nil
}

func (d *BaseDispatcher) addSubtasks(subtasks []*proto.Subtask) (err error) {
	for i := 0; i < RetrySQLTimes; i++ {
		err = d.taskMgr.AddSubTasks(d.Task, subtasks)
		if err == nil {
			break
		}
		if i%10 == 0 {
			logutil.Logger(d.logCtx).Warn("addSubtasks failed", zap.String("state", d.Task.State), zap.Int64("step", d.Task.Step),
				zap.Int("subtask cnt", len(subtasks)),
				zap.Int("retry times", i), zap.Error(err))
		}
		time.Sleep(RetrySQLInterval)
	}
	if err != nil {
		logutil.Logger(d.logCtx).Warn("addSubtasks failed", zap.String("state", d.Task.State), zap.Int64("step", d.Task.Step),
			zap.Int("subtask cnt", len(subtasks)),
			zap.Int("retry times", RetrySQLTimes), zap.Error(err))
	}
	return err
}

// UpdateTask update the task in tidb_global_task table.
func (d *BaseDispatcher) UpdateTask(taskState string, newSubTasks []*proto.Subtask, retryTimes int) (err error) {
	prevState := d.Task.State
	d.Task.State = taskState
	if !VerifyTaskStateTransform(prevState, taskState) {
		return errors.Errorf("invalid task state transform, from %s to %s", prevState, taskState)
	}

	failpoint.Inject("cancelBeforeUpdate", func() {
		err := d.taskMgr.CancelGlobalTask(d.Task.ID)
		if err != nil {
			logutil.Logger(d.logCtx).Error("cancel task failed", zap.Error(err))
		}
	})
	var retryable bool
	for i := 0; i < retryTimes; i++ {
		retryable, err = d.taskMgr.UpdateGlobalTaskAndAddSubTasks(d.Task, newSubTasks, prevState)
		if err == nil || !retryable {
			break
		}
		if i%10 == 0 {
			logutil.Logger(d.logCtx).Warn("updateTask first failed", zap.String("from", prevState), zap.String("to", d.Task.State),
				zap.Int("retry times", i), zap.Error(err))
		}
		time.Sleep(RetrySQLInterval)
	}
	if err != nil && retryTimes != nonRetrySQLTime {
		logutil.Logger(d.logCtx).Warn("updateTask failed",
			zap.String("from", prevState), zap.String("to", d.Task.State), zap.Int("retry times", retryTimes), zap.Error(err))
	}
	return err
}

func (d *BaseDispatcher) onErrHandlingStage(receiveErr []error) error {
	// 1. generate the needed task meta and subTask meta (dist-plan).
	meta, err := d.OnErrStage(d.ctx, d, d.Task, receiveErr)
	if err != nil {
		// OnErrStage must be retryable, if not, there will have resource leak for tasks.
		logutil.Logger(d.logCtx).Warn("handle error failed", zap.Error(err))
		return err
	}

	// 2. dispatch revert dist-plan to EligibleInstances.
	return d.dispatchSubTask4Revert(meta)
}

func (d *BaseDispatcher) dispatchSubTask4Revert(meta []byte) error {
	instanceIDs, err := d.GetAllSchedulerIDs(d.ctx, d.Task)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("get task's all instances failed", zap.Error(err))
		return err
	}

	subTasks := make([]*proto.Subtask, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		subTasks = append(subTasks, proto.NewSubtask(d.Task.ID, d.Task.Type, id, meta))
	}
	return d.UpdateTask(proto.TaskStateReverting, subTasks, RetrySQLTimes)
}

func (d *BaseDispatcher) onNextStage() error {
	/// dynamic dispatch subtasks.
	failpoint.Inject("mockDynamicDispatchErr", func() {
		failpoint.Return(errors.New("mockDynamicDispatchErr"))
	})

	// 1. Adjust the global task's concurrency.
	if d.Task.State == proto.TaskStatePending {
		if d.Task.Concurrency == 0 {
			d.Task.Concurrency = DefaultSubtaskConcurrency
		}
		if d.Task.Concurrency > MaxSubtaskConcurrency {
			d.Task.Concurrency = MaxSubtaskConcurrency
		}
		d.Task.StateUpdateTime = time.Now().UTC()
		if err := d.UpdateTask(proto.TaskStateRunning, nil, RetrySQLTimes); err != nil {
			return err
		}
	} else if d.StageFinished(d.Task) {
		// 2. when previous stage finished, update to next stage.
		d.Task.Step++
		logutil.Logger(d.logCtx).Info("previous stage finished, run into next stage", zap.Int64("from", d.Task.Step-1), zap.Int64("to", d.Task.Step))
		d.Task.StateUpdateTime = time.Now().UTC()
		err := d.UpdateTask(proto.TaskStateRunning, nil, RetrySQLTimes)
		if err != nil {
			return err
		}
	}

	for {
		// 3. generate a batch of subtasks.
		metas, err := d.OnNextSubtasksBatch(d.ctx, d, d.Task)
		if err != nil {
			logutil.Logger(d.logCtx).Warn("generate part of subtasks failed", zap.Error(err))
			return d.handlePlanErr(err)
		}

		failpoint.Inject("mockDynamicDispatchErr1", func() {
			failpoint.Return(errors.New("mockDynamicDispatchErr1"))
		})

		// 4. dispatch batch of subtasks to EligibleInstances.
		err = d.dispatchSubTask(metas)
		if err != nil {
			return err
		}

		if d.StageFinished(d.Task) {
			break
		}

		failpoint.Inject("mockDynamicDispatchErr2", func() {
			failpoint.Return(errors.New("mockDynamicDispatchErr2"))
		})
	}
	return nil
}

func (d *BaseDispatcher) dispatchSubTask(metas [][]byte) error {
	logutil.Logger(d.logCtx).Info("dispatch subtasks", zap.String("state", d.Task.State), zap.Int64("step", d.Task.Step), zap.Uint64("concurrency", d.Task.Concurrency), zap.Int("subtasks", len(metas)))

	// select all available TiDB nodes for task.
	serverNodes, err := d.GetEligibleInstances(d.ctx, d.Task)
	logutil.Logger(d.logCtx).Debug("eligible instances", zap.Int("num", len(serverNodes)))

	if err != nil {
		return err
	}
	// 4. filter by role.
	serverNodes, err = d.filterByRole(serverNodes)
	if err != nil {
		return err
	}

	logutil.Logger(d.logCtx).Info("eligible instances", zap.Int("num", len(serverNodes)))

	if len(serverNodes) == 0 {
		return errors.New("no available TiDB node to dispatch subtasks")
	}
	d.taskNodes = make([]string, len(serverNodes))
	for i := range serverNodes {
		d.taskNodes[i] = disttaskutil.GenerateExecID(serverNodes[i].IP, serverNodes[i].Port)
	}
	subTasks := make([]*proto.Subtask, 0, len(metas))
	for i, meta := range metas {
		// we assign the subtask to the instance in a round-robin way.
		// TODO: assign the subtask to the instance according to the system load of each nodes
		pos := i % len(serverNodes)
		instanceID := disttaskutil.GenerateExecID(serverNodes[pos].IP, serverNodes[pos].Port)
		logutil.Logger(d.logCtx).Debug("create subtasks", zap.String("instanceID", instanceID))
		subTasks = append(subTasks, proto.NewSubtask(d.Task.ID, d.Task.Type, instanceID, meta))
	}
	return d.addSubtasks(subTasks)
}

func (d *BaseDispatcher) handlePlanErr(err error) error {
	logutil.Logger(d.logCtx).Warn("generate plan failed", zap.Error(err), zap.String("state", d.Task.State))
	if d.IsRetryableErr(err) {
		return err
	}
	d.Task.Error = err
	// state transform: pending -> failed.
	return d.UpdateTask(proto.TaskStateFailed, nil, RetrySQLTimes)
}

// GenerateSchedulerNodes generate a eligible TiDB nodes.
func GenerateSchedulerNodes(ctx context.Context) (serverNodes []*infosync.ServerInfo, err error) {
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
	nodes, err := d.taskMgr.GetNodesByRole("background")
	if err != nil {
		return nil, err
	}

	if len(nodes) == 0 {
		nodes, err = d.taskMgr.GetNodesByRole("")
	}

	if err != nil {
		return nil, err
	}

	res := make([]*infosync.ServerInfo, 0, len(nodes))
	for _, info := range infos {
		_, ok := nodes[disttaskutil.GenerateExecID(info.IP, info.Port)]
		if ok {
			res = append(res, info)
		}
	}
	return res, nil
}

// GetAllSchedulerIDs gets all the scheduler IDs.
func (d *BaseDispatcher) GetAllSchedulerIDs(ctx context.Context, task *proto.Task) ([]string, error) {
	serverInfos, err := d.GetEligibleInstances(ctx, task)
	if err != nil {
		return nil, err
	}
	if len(serverInfos) == 0 {
		return nil, nil
	}

	schedulerIDs, err := d.taskMgr.GetSchedulerIDsByTaskID(task.ID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(schedulerIDs))
	for _, id := range schedulerIDs {
		if ok := disttaskutil.MatchServerInfo(serverInfos, id); ok {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// GetPreviousSubtaskMetas get subtask metas from specific step.
func (d *BaseDispatcher) GetPreviousSubtaskMetas(taskID int64, step int64) ([][]byte, error) {
	previousSubtasks, err := d.taskMgr.GetSucceedSubtasksByStep(taskID, step)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("get previous succeed subtask failed", zap.Int64("step", step))
		return nil, err
	}
	previousSubtaskMetas := make([][]byte, 0, len(previousSubtasks))
	for _, subtask := range previousSubtasks {
		previousSubtaskMetas = append(previousSubtaskMetas, subtask.Meta)
	}
	return previousSubtaskMetas, nil
}

// WithNewSession executes the function with a new session.
func (d *BaseDispatcher) WithNewSession(fn func(se sessionctx.Context) error) error {
	return d.taskMgr.WithNewSession(fn)
}

// WithNewTxn executes the fn in a new transaction.
func (d *BaseDispatcher) WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error {
	return d.taskMgr.WithNewTxn(ctx, fn)
}

// VerifyTaskStateTransform verifies whether the task state transform is valid.
func VerifyTaskStateTransform(from, to string) bool {
	rules := map[string][]string{
		proto.TaskStatePending: {
			proto.TaskStateRunning,
			proto.TaskStateCancelling,
			proto.TaskStatePausing,
			proto.TaskStateSucceed,
			proto.TaskStateFailed,
		},
		proto.TaskStateRunning: {
			proto.TaskStateSucceed,
			proto.TaskStateReverting,
			proto.TaskStateFailed,
			proto.TaskStateCancelling,
			proto.TaskStatePausing,
		},
		proto.TaskStateSucceed: {},
		proto.TaskStateReverting: {
			proto.TaskStateReverted,
			// no revert_failed now
			// proto.TaskStateRevertFailed,
		},
		proto.TaskStateFailed:       {},
		proto.TaskStateRevertFailed: {},
		proto.TaskStateCancelling: {
			proto.TaskStateReverting,
			// no canceled now
			// proto.TaskStateCanceled,
		},
		proto.TaskStateCanceled: {},
		proto.TaskStatePausing: {
			proto.TaskStatePaused,
		},
		proto.TaskStatePaused: {
			proto.TaskStateResuming,
		},
		proto.TaskStateResuming: {
			proto.TaskStateRunning,
		},
		proto.TaskStateRevertPending: {},
		proto.TaskStateReverted:      {},
	}
	logutil.BgLogger().Info("task state transform", zap.String("from", from), zap.String("to", to))

	if from == to {
		return true
	}

	for _, state := range rules[from] {
		if state == to {
			return true
		}
	}
	return false
}
