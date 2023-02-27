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

	"github.com/pingcap/errors"
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

type DistPlanner struct {
	concurrency       int
	distributionType  proto.TaskType
	needRedistributed bool
	splitter          Splitter
}

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
	d.runningGlobalTasks.RLock()
	defer d.runningGlobalTasks.RUnlock()
	d.runningGlobalTasks.tasks[int64(gTask.ID)] = gTask
}

func (d *Dispatcher) delRunningGlobalTasks(globalTaskID int64) {
	d.runningGlobalTasks.Lock()
	defer d.runningGlobalTasks.Unlock()
	delete(d.runningGlobalTasks.tasks, globalTaskID)
}

// GenerateDistPlan generates distributed plan from Task.
func (d *Dispatcher) GenerateDistPlan(gTask *proto.Task) (*DistPlanner, error) {
	distPlan := &DistPlanner{
		concurrency:      DefaultSubtaskConcurrency,
		distributionType: gTask.Type,
	}
	splitter, ok := SplitterConstructors[gTask.Type]
	if !ok {
		return nil, errors.New("type is invalid")
	}
	distPlan.splitter = splitter
	return distPlan, nil
}

func (d *Dispatcher) FinishTask(gTask *proto.Task) error {
	// TODO: Consider putting the following operations into a transaction.
	err := d.gTaskMgr.UpdateTask(gTask)
	if err != nil {
		return err
	}
	if gTask.State != proto.TaskStateSucceed {
		// TODO: Update tasks states to cancelled.
		return d.subTaskMgr.DeleteTasks(gTask.ID)
	}
	return nil
}

func (d *Dispatcher) detectionTask(gTask *proto.Task) (isFinished bool) {
	// TODO: Consider putting the following operations into a transaction.
	// TODO: Consider retry
	// TODO: Consider collect some information about the tasks.
	tasks, err := d.subTaskMgr.GetInterruptedTask(gTask.ID)
	if err != nil {
		logutil.BgLogger().Info("detection task failed", zap.Error(err))
		return false
	}
	if len(tasks) == 0 {
		isFinished, err = d.subTaskMgr.IsFinishedTask(gTask.ID)
		if err != nil {
			logutil.BgLogger().Info("get task cnt failed", zap.Uint64("global task ID", uint64(gTask.ID)), zap.Error(err))
		}
		if isFinished {
			gTask.State = proto.TaskStateSucceed
		}
	} else {
		// TODO: Consider cancelled or failed.
		gTask.State = tasks[0].State
	}
	if err := d.FinishTask(gTask); err != nil {
		logutil.BgLogger().Info("finish task failed", zap.Uint64("global task ID", uint64(gTask.ID)), zap.Error(err))
		return false
	}
	return true
}

// DetectionTaskLoop monitors the status of the subtasks.
func (d *Dispatcher) DetectionTaskLoop() {
	ticker := time.NewTicker(checkTaskFinishedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			logutil.BgLogger().Info("detection task loop exits", zap.Error(d.ctx.Err()))
		case <-ticker.C:
			gTasks := d.getRunningGlobalTasks()
			// TODO: Do we need to handle it asynchronously.
			for _, gTask := range gTasks {
				isFinished := d.detectionTask(gTask)
				if isFinished {
					d.delRunningGlobalTasks(int64(gTask.ID))
				}
			}
		}
	}
}

func (d *Dispatcher) loadTaskAndRun(gTask *proto.Task) (err error) {
	// TODO: Consider retry
	if gTask.MetaM.DistPlan == nil {
		distPlan, err := d.GenerateDistPlan(gTask)
		if err != nil {
			logutil.BgLogger().Warn("gen dist-plan failed", zap.Error(err))
			return err
		}
		gTask.MetaM.DistPlan = distPlan
		err = d.gTaskMgr.UpdateTask(gTask)
		if err != nil {
			logutil.BgLogger().Warn("update global task failed", zap.Error(err))
			return err
		}
	}

	// TODO: Consider batch splitting
	// TODO: Synchronization interruption problem, e.g. AddNewTask failed
	subTasks, err := gTask.MetaM.DistPlan.splitter.SplitTask()
	if err != nil {
		logutil.BgLogger().Warn("update global task failed", zap.Error(err))
		return err
	}
	for _, subTask := range subTasks {
		// TODO: Get TiDB_Instance_ID
		err := d.subTaskMgr.AddNewTask(subTask.TaskID, "", subTask.Meta)
		if err != nil {
			logutil.BgLogger().Warn("add subtask failed", zap.Stringer("subTask", subTask), zap.Error(err))
			return err
		}
	}
	d.setRunningGlobalTasks(gTask)
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
		case <-ticker.C:
			cnt := len(d.getRunningGlobalTasks())
			for cnt < DefaultConcurrency {
				// TODO: Consider retry
				gTask, err := d.gTaskMgr.GetNewTask()
				if err != nil {
					logutil.BgLogger().Warn("get new task failed", zap.Error(err))
					continue
				}
				d.wg.Run(func() {
					d.loadTaskAndRun(gTask)
				})
				cnt++
			}
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
