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

// Dispatcher defines the interface for operations inside a dispatcher.
type Dispatcher interface {
	// Start enables dispatching and monitoring mechanisms.
	Start()
	// GetAllSchedulerIDs gets handles the task's all available instances.
	GetAllSchedulerIDs(ctx context.Context, handle TaskFlowHandle, task *proto.Task) ([]string, error)
	// Stop stops the dispatcher.
	Stop()
	// Inited check if the dispatcher Started.
	Inited() bool
}

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
}

// MockOwnerChange mock owner change in tests.
var MockOwnerChange func()

func newDispatcher(ctx context.Context, taskMgr *storage.TaskManager, task *proto.Task) *dispatcher {
	return &dispatcher{
		ctx,
		taskMgr,
		task,
	}
}

// ExecuteTask start to schedule a task
func (d *dispatcher) ExecuteTask() {
	logutil.BgLogger().Info("execute one task", zap.Int64("task ID", d.task.ID),
		zap.String("state", d.task.State), zap.Uint64("concurrency", d.task.Concurrency))
	d.scheduleTask(d.task.ID)
}

// monitorTask checks whether the current step of one task is finished,
// and gather subTaskErrs to handle subTask fails.
func (d *dispatcher) monitorTask(taskID int64) (finished bool, subTaskErrs []error) {
	// TODO: Consider putting the following operations into a transaction.
	var err error
	d.task, err = d.taskMgr.GetGlobalTaskByID(taskID)
	if err != nil {
		logutil.BgLogger().Error("check task failed", zap.Int64("task ID", d.task.ID), zap.Error(err))
		return false, nil
	}
	switch d.task.State {
	case proto.TaskStateCancelling:
		return false, []error{errors.New("cancel")}
	case proto.TaskStateReverting:
		cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.task.ID, proto.TaskStateRevertPending, proto.TaskStateReverting)
		if err != nil {
			logutil.BgLogger().Warn("check task failed", zap.Int64("task ID", d.task.ID), zap.Error(err))
			return false, nil
		}
		return cnt == 0, nil
	default:
		subTaskErrs, err = d.taskMgr.CollectSubTaskError(d.task.ID)
		if err != nil {
			logutil.BgLogger().Warn("collect subtask error failed", zap.Int64("task ID", d.task.ID), zap.Error(err))
			return false, nil
		}
		if len(subTaskErrs) > 0 {
			return false, subTaskErrs
		}
		// check subtasks pending or running.
		cnt, err := d.taskMgr.GetSubtaskInStatesCnt(d.task.ID, proto.TaskStatePending, proto.TaskStateRunning)
		if err != nil {
			logutil.BgLogger().Warn("check task failed", zap.Int64("task ID", d.task.ID), zap.Error(err))
			return false, nil
		}
		return cnt == 0, nil
	}
}

// scheduleTask schedule the task execution step by step.
func (d *dispatcher) scheduleTask(taskID int64) {
	ticker := time.NewTicker(checkTaskFinishedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			logutil.BgLogger().Info("schedule task exits", zap.Int64("task ID", taskID), zap.Error(d.ctx.Err()))
			return
		case <-ticker.C:
			stepIsFinished, errs := d.monitorTask(taskID)
			failpoint.Inject("cancelTaskAfterMonitorTask", func(val failpoint.Value) {
				if val.(bool) && d.task.State == proto.TaskStateRunning {
					err := d.taskMgr.CancelGlobalTask(taskID)
					if err != nil {
						logutil.BgLogger().Error("cancel task failed", zap.Error(err))
					}
				}
			})
			// The global task isn't finished and not failed.
			if !stepIsFinished && len(errs) == 0 {
				GetTaskFlowHandle(d.task.Type).OnTicker(d.ctx, d.task)
				logutil.BgLogger().Debug("schedule task, this task keeps current state",
					zap.Int64("task-id", d.task.ID), zap.String("state", d.task.State))
				break
			}

			err := d.processFlow(d.task, errs)
			if err == nil && d.task.IsFinished() {
				logutil.BgLogger().Info("schedule task, task is finished",
					zap.Int64("task-id", d.task.ID), zap.String("state", d.task.State))
				return
			}
		}

		failpoint.Inject("mockOwnerChange", func(val failpoint.Value) {
			if val.(bool) {
				logutil.BgLogger().Info("mockOwnerChange called")
				MockOwnerChange()
				time.Sleep(time.Second)
			}
		})
	}
}

func (d *dispatcher) processFlow(task *proto.Task, errs []error) error {
	if len(errs) > 0 {
		// Found an error when task is running.
		logutil.BgLogger().Info("process flow, handle an error", zap.Int64("task-id", task.ID), zap.Errors("err msg", errs))
		return d.processErrFlow(task, errs)
	}
	// previous step is finished.
	if task.State == proto.TaskStateReverting {
		// Finish the rollback step.
		logutil.BgLogger().Info("process flow, update the task to reverted", zap.Int64("task-id", task.ID))
		return d.updateTask(task, proto.TaskStateReverted, nil, retrySQLTimes)
	}
	// Finish the normal step.
	logutil.BgLogger().Info("process flow, process normal", zap.Int64("task-id", task.ID))
	return d.processNormalFlow(task)
}

func (d *dispatcher) updateTask(task *proto.Task, taskState string, newSubTasks []*proto.Subtask, retryTimes int) (err error) {
	prevState := task.State
	task.State = taskState
	for i := 0; i < retryTimes; i++ {
		err = d.taskMgr.UpdateGlobalTaskAndAddSubTasks(task, newSubTasks)
		if err == nil {
			break
		}
		if i%10 == 0 {
			logutil.BgLogger().Warn("updateTask first failed", zap.Int64("task-id", task.ID),
				zap.String("previous state", prevState), zap.String("curr state", task.State),
				zap.Int("retry times", retryTimes), zap.Error(err))
		}
		time.Sleep(retrySQLInterval)
	}
	if err != nil && retryTimes != nonRetrySQLTime {
		logutil.BgLogger().Warn("updateTask failed", zap.Int64("task-id", task.ID),
			zap.String("previous state", prevState), zap.String("curr state", task.State), zap.Int("retry times", retryTimes), zap.Error(err))
	}
	return err
}

func (d *dispatcher) processErrFlow(task *proto.Task, receiveErr []error) error {
	// TODO: Maybe it gets GetTaskFlowHandle fails when rolling upgrades.
	// 1. generate the needed global task meta and subTask meta (dist-plan).
	handle := GetTaskFlowHandle(task.Type)
	if handle == nil {
		logutil.BgLogger().Warn("gen task flow handle failed, this type handle doesn't register", zap.Int64("ID", task.ID), zap.String("type", task.Type))
		return d.updateTask(task, proto.TaskStateReverted, nil, retrySQLTimes)
	}
	meta, err := handle.ProcessErrFlow(d.ctx, d, task, receiveErr)
	if err != nil {
		logutil.BgLogger().Warn("handle error failed", zap.Error(err))
		return err
	}

	// 2. dispatch revert dist-plan to EligibleInstances.
	return d.dispatchSubTask4Revert(task, handle, meta)
}

func (d *dispatcher) dispatchSubTask4Revert(task *proto.Task, handle TaskFlowHandle, meta []byte) error {
	instanceIDs, err := d.GetAllSchedulerIDs(d.ctx, handle, task)
	if err != nil {
		logutil.BgLogger().Warn("get task's all instances failed", zap.Error(err))
		return err
	}

	if len(instanceIDs) == 0 {
		return d.updateTask(task, proto.TaskStateReverted, nil, retrySQLTimes)
	}

	subTasks := make([]*proto.Subtask, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		subTasks = append(subTasks, proto.NewSubtask(task.ID, task.Type, id, meta))
	}
	return d.updateTask(task, proto.TaskStateReverting, subTasks, retrySQLTimes)
}

func (d *dispatcher) processNormalFlow(task *proto.Task) error {
	// 1. generate the needed global task meta and subTask meta (dist-plan).
	handle := GetTaskFlowHandle(task.Type)
	if handle == nil {
		logutil.BgLogger().Warn("gen task flow handle failed, this type handle doesn't register", zap.Int64("ID", task.ID), zap.String("type", task.Type))
		task.Error = errors.New("unsupported task type")
		return d.updateTask(task, proto.TaskStateReverted, nil, retrySQLTimes)
	}
	metas, err := handle.ProcessNormalFlow(d.ctx, d, task)
	if err != nil {
		logutil.BgLogger().Warn("gen dist-plan failed", zap.Error(err))
		if handle.IsRetryableErr(err) {
			return err
		}
		task.Error = err
		return d.updateTask(task, proto.TaskStateReverted, nil, retrySQLTimes)
	}
	logutil.BgLogger().Info("process normal flow", zap.Int64("task ID", task.ID),
		zap.String("state", task.State), zap.Uint64("concurrency", task.Concurrency), zap.Int("subtasks", len(metas)))

	// 2. dispatch dist-plan to EligibleInstances.
	return d.dispatchSubTask(task, handle, metas)
}

func (d *dispatcher) dispatchSubTask(task *proto.Task, handle TaskFlowHandle, metas [][]byte) error {
	// Adjust the global task's concurrency.
	if task.Concurrency == 0 {
		task.Concurrency = DefaultSubtaskConcurrency
	}
	if task.Concurrency > MaxSubtaskConcurrency {
		task.Concurrency = MaxSubtaskConcurrency
	}

	retryTimes := retrySQLTimes
	// Special handling for the new tasks.
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
		err := d.updateTask(task, proto.TaskStateSucceed, nil, retryTimes)
		if err != nil {
			logutil.BgLogger().Warn("update global task failed", zap.Error(err))
			return err
		}
		return nil
	}
	// select all available TiDB nodes for this global tasks.
	serverNodes, err1 := handle.GetEligibleInstances(d.ctx, task)
	logutil.BgLogger().Debug("eligible instances", zap.Int("num", len(serverNodes)))

	if err1 != nil {
		return err1
	}
	if len(serverNodes) == 0 {
		return errors.New("no available TiDB node")
	}
	subTasks := make([]*proto.Subtask, 0, len(metas))
	for i, meta := range metas {
		// we assign the subtask to the instance in a round-robin way.
		pos := i % len(serverNodes)
		instanceID := disttaskutil.GenerateExecID(serverNodes[pos].IP, serverNodes[pos].Port)
		logutil.BgLogger().Debug("create subtasks",
			zap.Int("task.ID", int(task.ID)), zap.String("type", task.Type), zap.String("instanceID", instanceID))
		subTasks = append(subTasks, proto.NewSubtask(task.ID, task.Type, instanceID, meta))
	}

	return d.updateTask(task, task.State, subTasks, retrySQLTimes)
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
		logutil.BgLogger().Warn("get previous succeed subtask failed", zap.Int64("ID", taskID), zap.Int64("step", step))
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
