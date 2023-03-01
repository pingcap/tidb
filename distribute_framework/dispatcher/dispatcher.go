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
	"sync"
	"time"

	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/distribute_framework/storage"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	DefaultConcurrency        = 8
	DefaultSubtaskConcurrency = 16
	checkTaskFinishedInterval = 300 * time.Millisecond
	checkTaskRunningInterval  = 300 * time.Millisecond
)

type Dispatcher struct {
	ctx        context.Context
	cancel     context.CancelFunc
	gTaskMgr   *storage.GlobalTaskManager
	subTaskMgr *storage.SubTaskManager
	wg         tidbutil.WaitGroupWrapper

	runningGlobalTasks struct {
		sync.RWMutex
		tasks map[int64]*proto.Task
	}
}

func (d *Dispatcher) getRunningGlobalTasks() map[int64]*proto.Task {
	d.runningGlobalTasks.RLock()
	defer d.runningGlobalTasks.RUnlock()
	return d.runningGlobalTasks.tasks
}

func (d *Dispatcher) setRunningGlobalTasks(gTask *proto.Task) {
	d.runningGlobalTasks.Lock()
	defer d.runningGlobalTasks.Unlock()
	d.runningGlobalTasks.tasks[int64(gTask.ID)] = gTask
}

func (d *Dispatcher) delRunningGlobalTasks(globalTaskID int64) {
	d.runningGlobalTasks.Lock()
	defer d.runningGlobalTasks.Unlock()
	delete(d.runningGlobalTasks.tasks, globalTaskID)
}

func (d *Dispatcher) detectionTask(gTask *proto.Task) (isFinished bool, subTaskErr string) {
	// TODO: Consider putting the following operations into a transaction.
	// TODO: Consider retry
	// TODO: Consider collect some information about the tasks.
	cnt, err := d.subTaskMgr.CheckTaskState(gTask.ID, proto.TaskStateFailed, true)
	if err != nil {
		logutil.BgLogger().Warn("check task failed", zap.Error(err))
		return false, ""
	}
	if cnt > 0 {
		return false, "failed"
	}

	// Suppose that the task pending = 0 means that all subtask finish.
	cnt, err = d.subTaskMgr.CheckTaskState(gTask.ID, proto.TaskStatePending, true)
	if err != nil {
		logutil.BgLogger().Warn("check task failed", zap.Error(err))
		return false, ""
	}
	if cnt == 0 {
		return true, ""
	}

	return false, ""
}

// DetectionTaskLoop monitors the status of the subtasks.
func (d *Dispatcher) DetectionTaskLoop() {
	ticker := time.NewTicker(checkTaskFinishedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			logutil.BgLogger().Info("detection task loop exits", zap.Error(d.ctx.Err()))
			return
		case <-ticker.C:
			gTasks := d.getRunningGlobalTasks()
			// TODO: Do we need to handle it asynchronously.
			for _, gTask := range gTasks {
				stepIsFinished, errStr := d.detectionTask(gTask)
				if errStr != "" {
					d.HandleError(gTask, errStr)
				} else if stepIsFinished {
					logutil.BgLogger().Info("a step of task finished", zap.Int64("taskID", int64(gTask.ID)))
					d.loadTaskAndProgress(gTask, false)
				}
			}
		}
	}
}

func (d *Dispatcher) HandleError(gTask *proto.Task, receiveErr string) {
	err := GetTaskDispatcherHandle(gTask.Type).HandleError(d, gTask, receiveErr)
	if err != nil {
		logutil.BgLogger().Warn("handle error failed", zap.Error(err))
	}
}

func (d *Dispatcher) loadTaskAndProgress(gTask *proto.Task, fromPending bool) (err error) {
	// Generate the needed global task meta and subTask meta.
	finished, subTasks, err := GetTaskDispatcherHandle(gTask.Type).Progress(d, gTask, fromPending)
	if err != nil {
		logutil.BgLogger().Warn("gen dist-plan failed", zap.Error(err))
		return err
	}

	// Adjust global task meta.
	if gTask.Concurrency == 0 {
		gTask.Concurrency = DefaultSubtaskConcurrency
	}
	if finished {
		gTask.State = proto.TaskStateSucceed
	}

	// Special handling for the new/finished tasks.
	if gTask.State == proto.TaskStateSucceed {
		d.delRunningGlobalTasks(int64(gTask.ID))
		_ = d.subTaskMgr.DeleteTasks(gTask.ID)
	} else if gTask.State == proto.TaskStatePending {
		gTask.StartTime = time.Now()
		d.setRunningGlobalTasks(gTask)
		gTask.State = proto.TaskStateRunning
	}

	// Write the global task meta into the storage.
	err = d.gTaskMgr.UpdateTask(gTask)
	if err != nil {
		logutil.BgLogger().Warn("update global task failed", zap.Error(err))
		return err
	}

	if finished {
		return nil
	}

	// TODO: Consider batch splitting
	// TODO: Synchronization interruption problem, e.g. AddNewTask failed
	// TODO: batch insert
	// Write the subTask meta into the storage.
	for _, subTask := range subTasks {
		// TODO: Get TiDB_Instance_ID
		err := d.subTaskMgr.AddNewTask(gTask.ID, subTask.SchedulerID, subTask.Meta.Serialize(), gTask.Type)
		if err != nil {
			logutil.BgLogger().Warn("add subtask failed", zap.Stringer("subTask", subTask), zap.Error(err))
			return err
		}
	}
	return nil
}

// DispatchTaskLoop dispatches the global tasks.
func (d *Dispatcher) DispatchTaskLoop() {
	ticker := time.NewTicker(checkTaskRunningInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			logutil.BgLogger().Info("dispatch task loop exits", zap.Error(d.ctx.Err()))
			return
		case <-ticker.C:
			cnt := len(d.getRunningGlobalTasks())

			// TODO: Consider retry
			gTask, err := d.gTaskMgr.GetNewTask()
			if err != nil {
				logutil.BgLogger().Warn("get new task failed", zap.Error(err))
				continue
			}
			if gTask == nil {
				continue
			}
			d.wg.Run(func() {
				d.loadTaskAndProgress(gTask, true)
			})
			cnt++
		}
	}
}

func NewDispatcher(ctx context.Context, globalTaskTable *storage.GlobalTaskManager, subtaskTable *storage.SubTaskManager) (*Dispatcher, error) {
	// TODO: Consider session using.
	dispatcher := &Dispatcher{
		gTaskMgr:   globalTaskTable,
		subTaskMgr: subtaskTable,
	}
	dispatcher.ctx, dispatcher.cancel = context.WithCancel(ctx)
	dispatcher.runningGlobalTasks.tasks = make(map[int64]*proto.Task)

	return dispatcher, nil
}

func (d *Dispatcher) Start() {
	d.wg.Run(func() {
		d.DispatchTaskLoop()
	})
	d.wg.Run(func() {
		d.DetectionTaskLoop()
	})
}

func (d *Dispatcher) Stop() {
	d.cancel()
	d.wg.Wait()
}
