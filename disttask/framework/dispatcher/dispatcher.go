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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	disttaskutil "github.com/pingcap/tidb/util/disttask"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultSubtaskConcurrency is the default concurrency for handling subtask.
	DefaultSubtaskConcurrency = 16
	// MaxSubtaskConcurrency is the maximum concurrency for handling subtask.
	MaxSubtaskConcurrency = 256
)

var (
	// DefaultDispatchConcurrency is the default concurrency for handling global task.
	DefaultDispatchConcurrency = 4
	checkTaskFinishedInterval  = 500 * time.Millisecond
	checkTaskRunningInterval   = 300 * time.Millisecond
	nonRetrySQLTime            = 1
	retrySQLTimes              = variable.DefTiDBDDLErrorCountLimit
	retrySQLInterval           = 500 * time.Millisecond
)

// TaskHandle provides the interface for operations needed by task flow handles.
type TaskHandle interface {
	// GetAllSchedulerIDs gets handles the task's all scheduler instances.
	GetAllSchedulerIDs(ctx context.Context, handle TaskFlowHandle, task *proto.Task) ([]string, error)
	// GetPreviousSubtaskMetas gets previous subtask metas.
	GetPreviousSubtaskMetas(taskID int64, step int64) ([][]byte, error)
	storage.SessionExecutor
}

// Manage the lifetime of a task
// including submitting subtasks and updating the status of a task.
type dispatcher struct {
	ctx     context.Context
	taskMgr *storage.TaskManager
	task    *proto.Task
	logCtx  context.Context
}

// MockOwnerChange mock owner change in tests.
var MockOwnerChange func()

func newDispatcher(ctx context.Context, taskMgr *storage.TaskManager, task *proto.Task) *dispatcher {
	logPrefix := fmt.Sprintf("task_id: %d, task_type: %s", task.ID, task.Type)
	return &dispatcher{
		ctx,
		taskMgr,
		task,
		logutil.WithKeyValue(context.Background(), "dispatcher", logPrefix),
	}
}

// ExecuteTask start to schedule a task.
func (d *dispatcher) executeTask() {
	logutil.Logger(d.logCtx).Info("execute one task",
		zap.String("state", d.task.State), zap.Uint64("concurrency", d.task.Concurrency))
	d.scheduleTask()
	// TODO: manage history task table.
}

// refreshTask fetch task state from tidb_global_task table.
func (d *dispatcher) refreshTask() (err error) {
	d.task, err = d.taskMgr.GetGlobalTaskByID(d.task.ID)
	if err != nil {
		logutil.Logger(d.logCtx).Error("refresh task failed", zap.Error(err))
	}
	return err
}

// scheduleTask schedule the task execution step by step.
func (d *dispatcher) scheduleTask() {
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
				if val.(bool) && d.task.State == proto.TaskStateRunning {
					err := d.taskMgr.CancelGlobalTask(d.task.ID)
					if err != nil {
						logutil.Logger(d.logCtx).Error("cancel task failed", zap.Error(err))
					}
				}
			})
			switch d.task.State {
			case proto.TaskStateCancelling:
				err = d.handleCancelling()
			case proto.TaskStateReverting:
				err = d.handleReverting()
			case proto.TaskStatePending:
				err = d.handlePending()
			case proto.TaskStateRunning:
				err = d.handleRunning()
			case proto.TaskStateSucceed, proto.TaskStateReverted:
				logutil.Logger(d.logCtx).Info("schedule task, task is finished", zap.String("state", d.task.State))
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
func (d *dispatcher) handleCancelling() error {
	logutil.Logger(d.logCtx).Debug("handle cancelling state", zap.String("state", d.task.State), zap.Int64("stage", d.task.Step))
	errs := []error{errors.New("cancel")}
	return d.processErrFlow(errs)
}

// handle task in reverting state, check all revert subtasks finished.
func (d *dispatcher) handleReverting() error {
	logutil.Logger(d.logCtx).Debug("handle reverting state", zap.String("state", d.task.State), zap.Int64("stage", d.task.Step))
	cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.task.ID, proto.TaskStateRevertPending, proto.TaskStateReverting)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}
	prevStageFinished := cnt == 0
	if prevStageFinished {
		// Finish the rollback step.
		logutil.Logger(d.logCtx).Info("update the task to reverted state")
		return d.updateTask(proto.TaskStateReverted, nil, retrySQLTimes)
	}
	// Wait all subtasks in this stage finished.
	GetTaskFlowHandle(d.task.Type).OnTicker(d.ctx, d.task)
	logutil.Logger(d.logCtx).Debug("handle reverting state, this task keeps current state", zap.String("state", d.task.State))
	return nil
}

// handle task in pending state, dispatch subtasks.
func (d *dispatcher) handlePending() error {
	logutil.Logger(d.logCtx).Debug("handle pending state", zap.String("state", d.task.State), zap.Int64("stage", d.task.Step))
	return d.processNormalFlow()
}

// handle task in running state, check all running subtasks finished.
// If subtasks finished, run into the next stage.
func (d *dispatcher) handleRunning() error {
	logutil.Logger(d.logCtx).Debug("handle running state", zap.String("state", d.task.State), zap.Int64("stage", d.task.Step))
	subTaskErrs, err := d.taskMgr.CollectSubTaskError(d.task.ID)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("collect subtask error failed", zap.Error(err))
		return err
	}
	if len(subTaskErrs) > 0 {
		logutil.Logger(d.logCtx).Warn("subtasks encounter errors")
		return d.processErrFlow(subTaskErrs)
	}
	// check current stage finished.
	cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.task.ID, proto.TaskStatePending, proto.TaskStateRunning)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("check task failed", zap.Error(err))
		return err
	}

	prevStageFinished := cnt == 0
	if prevStageFinished {
		logutil.Logger(d.logCtx).Info("previous stage finished, generate dist plan", zap.Int64("stage", d.task.Step))
		return d.processNormalFlow()
	}
	// Wait all subtasks in this stage finished.
	GetTaskFlowHandle(d.task.Type).OnTicker(d.ctx, d.task)
	logutil.Logger(d.logCtx).Debug("handing running state, this task keeps current state", zap.String("state", d.task.State))
	return nil
}

func (d *dispatcher) updateTask(taskState string, newSubTasks []*proto.Subtask, retryTimes int) (err error) {
	prevState := d.task.State
	d.task.State = taskState
	for i := 0; i < retryTimes; i++ {
		err = d.taskMgr.UpdateGlobalTaskAndAddSubTasks(d.task, newSubTasks)
		if err == nil {
			break
		}
		if i%10 == 0 {
			logutil.Logger(d.logCtx).Warn("updateTask first failed", zap.String("previous state", prevState), zap.String("curr state", d.task.State),
				zap.Int("retry times", retryTimes), zap.Error(err))
		}
		time.Sleep(retrySQLInterval)
	}
	if err != nil && retryTimes != nonRetrySQLTime {
		logutil.Logger(d.logCtx).Warn("updateTask failed",
			zap.String("previous state", prevState), zap.String("curr state", d.task.State), zap.Int("retry times", retryTimes), zap.Error(err))
	}
	return err
}

func (d *dispatcher) processErrFlow(receiveErr []error) error {
	// TODO: Maybe it gets GetTaskFlowHandle fails when rolling upgrades.
	// 1. generate the needed global task meta and subTask meta (dist-plan).
	handle := GetTaskFlowHandle(d.task.Type)
	if handle == nil {
		logutil.Logger(d.logCtx).Warn("gen task flow handle failed, this type handle doesn't register")
		return d.updateTask(proto.TaskStateReverted, nil, retrySQLTimes)
	}
	meta, err := handle.ProcessErrFlow(d.ctx, d, d.task, receiveErr)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("handle error failed", zap.Error(err))
		return err
	}

	// 2. dispatch revert dist-plan to EligibleInstances.
	return d.dispatchSubTask4Revert(d.task, handle, meta)
}

func (d *dispatcher) dispatchSubTask4Revert(task *proto.Task, handle TaskFlowHandle, meta []byte) error {
	instanceIDs, err := d.GetAllSchedulerIDs(d.ctx, handle, task)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("get task's all instances failed", zap.Error(err))
		return err
	}

	if len(instanceIDs) == 0 {
		return d.updateTask(proto.TaskStateReverted, nil, retrySQLTimes)
	}

	subTasks := make([]*proto.Subtask, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		subTasks = append(subTasks, proto.NewSubtask(task.ID, task.Type, id, meta))
	}
	return d.updateTask(proto.TaskStateReverting, subTasks, retrySQLTimes)
}

func (d *dispatcher) processNormalFlow() error {
	// 1. generate the needed global task meta and subTask meta (dist-plan).
	handle := GetTaskFlowHandle(d.task.Type)
	if handle == nil {
		logutil.Logger(d.logCtx).Warn("gen task flow handle failed, this type handle doesn't register", zap.String("type", d.task.Type))
		d.task.Error = errors.New("unsupported task type")
		return d.updateTask(proto.TaskStateReverted, nil, retrySQLTimes)
	}
	metas, err := handle.ProcessNormalFlow(d.ctx, d, d.task)
	if err != nil {
		logutil.Logger(d.logCtx).Warn("generate dist-plan failed", zap.Error(err))
		if handle.IsRetryableErr(err) {
			return err
		}
		d.task.Error = err
		return d.updateTask(proto.TaskStateReverted, nil, retrySQLTimes)
	}
	// 2. dispatch dist-plan to EligibleInstances.
	return d.dispatchSubTask(d.task, handle, metas)
}

func (d *dispatcher) dispatchSubTask(task *proto.Task, handle TaskFlowHandle, metas [][]byte) error {
	logutil.Logger(d.logCtx).Info("dispatch subtasks", zap.String("state", d.task.State), zap.Uint64("concurrency", d.task.Concurrency), zap.Int("subtasks", len(metas)))
	// 1. Adjust the global task's concurrency.
	if task.Concurrency == 0 {
		task.Concurrency = DefaultSubtaskConcurrency
	}
	if task.Concurrency > MaxSubtaskConcurrency {
		task.Concurrency = MaxSubtaskConcurrency
	}

	retryTimes := retrySQLTimes
	// 2. Special handling for the new tasks.
	if task.State == proto.TaskStatePending {
		// TODO: Consider using TS.
		nowTime := time.Now().UTC()
		task.StartTime = nowTime
		task.State = proto.TaskStateRunning
		task.StateUpdateTime = nowTime
		retryTimes = nonRetrySQLTime
	}

	if len(metas) == 0 {
		task.StateUpdateTime = time.Now().UTC()
		// Write the global task meta into the storage.
		err := d.updateTask(proto.TaskStateSucceed, nil, retryTimes)
		if err != nil {
			logutil.Logger(d.logCtx).Warn("update global task failed", zap.Error(err))
			return err
		}
		return nil
	}

	// 3. select all available TiDB nodes for this global tasks.
	serverNodes, err := handle.GetEligibleInstances(d.ctx, task)
	logutil.Logger(d.logCtx).Debug("eligible instances", zap.Int("num", len(serverNodes)))

	if err != nil {
		return err
	}
	if len(serverNodes) == 0 {
		return errors.New("no available TiDB node to dispatch subtasks")
	}
	subTasks := make([]*proto.Subtask, 0, len(metas))
	for i, meta := range metas {
		// we assign the subtask to the instance in a round-robin way.
		pos := i % len(serverNodes)
		instanceID := disttaskutil.GenerateExecID(serverNodes[pos].IP, serverNodes[pos].Port)
		logutil.Logger(d.logCtx).Debug("create subtasks", zap.String("instanceID", instanceID))
		subTasks = append(subTasks, proto.NewSubtask(task.ID, task.Type, instanceID, meta))
	}

	return d.updateTask(task.State, subTasks, retrySQLTimes)
}

// GenerateSchedulerNodes generate a eligible TiDB nodes.
func GenerateSchedulerNodes(ctx context.Context) ([]*infosync.ServerInfo, error) {
	serverInfos, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	if len(serverInfos) == 0 {
		return nil, errors.New("not found instance")
	}

	serverNodes := make([]*infosync.ServerInfo, 0, len(serverInfos))
	for _, serverInfo := range serverInfos {
		serverNodes = append(serverNodes, serverInfo)
	}
	return serverNodes, nil
}

// GetAllSchedulerIDs gets all the scheduler IDs.
func (d *dispatcher) GetAllSchedulerIDs(ctx context.Context, handle TaskFlowHandle, task *proto.Task) ([]string, error) {
	serverInfos, err := handle.GetEligibleInstances(ctx, task)
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
func (d *dispatcher) GetPreviousSubtaskMetas(taskID int64, step int64) ([][]byte, error) {
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
func (d *dispatcher) WithNewSession(fn func(se sessionctx.Context) error) error {
	return d.taskMgr.WithNewSession(fn)
}

// WithNewTxn executes the fn in a new transaction.
func (d *dispatcher) WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error {
	return d.taskMgr.WithNewTxn(ctx, fn)
}
