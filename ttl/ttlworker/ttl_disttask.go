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

package ttlworker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/handle"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/util/codec"
)

type ttlJobMeta struct {
	TableID       int64 `json:"table_id"`
	PhysicalID    int64 `json:"physical_id"`
	WatermarkUnix int64 `json:"watermark_unix"`
}

type ttlTaskMeta struct {
	ttlJobMeta
	JobID             string              `json:"job_id"`
	ScanID            int64               `json:"scan_id"`
	RangeStartEncoded []byte              `json:"range_start_encoded"`
	RangeEndEncoded   []byte              `json:"range_end_encoded"`
	ExpireTimeUnix    int64               `json:"expire_time_unix"`
	TaskState         *cache.TTLTaskState `json:"task_state,omitempty"`
}

type ttlMinimalTask struct {
	*ttlTaskMeta
	statistics ttlStatistics
	err        atomic.Pointer[error]
}

func (_ *ttlMinimalTask) IsMinimalTask() {

}

func (t *ttlMinimalTask) String() string {
	return fmt.Sprintf("tableID: %d, physicalID: %d, start: %s, end: %s", t.TableID, t.PhysicalID, t.RangeStartEncoded, t.RangeEndEncoded)
}

func (t *ttlMinimalTask) ToResultMeta() *ttlTaskMeta {
	meta := *t.ttlTaskMeta
	errStr := ""
	if err := t.err.Load(); err != nil {
		errStr = (*err).Error()
	}

	meta.TaskState = &cache.TTLTaskState{
		TotalRows:   t.statistics.TotalRows.Load(),
		SuccessRows: t.statistics.SuccessRows.Load(),
		ErrorRows:   t.statistics.ErrorRows.Load(),
		ScanTaskErr: errStr,
	}
	return &meta
}

type ttlJobTaskHandle struct {
	pool  sessionPool
	store kv.Storage
}

func (t *ttlJobTaskHandle) OnTicker(_ context.Context, _ *proto.Task) {
	return
}

func (t *ttlJobTaskHandle) processInitStep(ctx context.Context, gTask *proto.Task, jobMeta *ttlJobMeta) (subtaskMetas [][]byte, err error) {
	se, err := getSession(t.pool)
	if err != nil {
		return nil, err
	}
	defer se.Close()

	partitionID := int64(0)
	if jobMeta.PhysicalID != jobMeta.TableID {
		partitionID = jobMeta.PhysicalID
	}

	tbl, err := cache.NewPhysicalTableByID(jobMeta.TableID, partitionID, se.SessionInfoSchema())
	if err != nil {
		return nil, nil
	}

	ranges, err := tbl.SplitScanRanges(ctx, t.store, splitScanCount)
	if err != nil {
		return nil, err
	}

	expire, err := tbl.EvalExpireTime(ctx, se, time.Unix(jobMeta.WatermarkUnix, 0))
	if err != nil {
		return nil, err
	}

	subtaskMetas = make([][]byte, 0, len(ranges))
	for i, r := range ranges {
		var taskMeta ttlTaskMeta
		taskMeta.ttlJobMeta = *jobMeta
		taskMeta.JobID = fmt.Sprintf("%d", gTask.ID)
		taskMeta.ScanID = int64(i)
		taskMeta.ExpireTimeUnix = expire.Unix()

		rangeStart, err := codec.EncodeKey(se.GetSessionVars().StmtCtx, []byte{}, r.Start...)
		if err != nil {
			return nil, err
		}
		rangeEnd, err := codec.EncodeKey(se.GetSessionVars().StmtCtx, []byte{}, r.End...)
		if err != nil {
			return nil, err
		}

		taskMeta.RangeStartEncoded = rangeStart
		taskMeta.RangeEndEncoded = rangeEnd

		bs, err := json.Marshal(&taskMeta)
		if err != nil {
			return nil, err
		}
		subtaskMetas = append(subtaskMetas, bs)
	}
	return subtaskMetas, nil
}

func (t *ttlJobTaskHandle) ProcessNormalFlow(ctx context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (subtaskMetas [][]byte, err error) {
	var jobMeta ttlJobMeta
	if err = json.Unmarshal(gTask.Meta, &jobMeta); err != nil {
		return nil, err
	}

	switch gTask.Step {
	case proto.StepInit:
		gTask.Step = proto.StepOne
		return t.processInitStep(ctx, gTask, &jobMeta)
	case proto.StepOne:
		return nil, nil
	default:
		return nil, errors.Errorf("invalid step %d", gTask.Step)
	}
}

func (t *ttlJobTaskHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (subtaskMeta []byte, err error) {
	return nil, nil
}

func (t *ttlJobTaskHandle) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return dispatcher.GenerateSchedulerNodes(ctx)
}

func (t *ttlJobTaskHandle) IsRetryableErr(_ error) bool {
	return true
}

type ttlTaskSchedulerHandler struct {
	tasks sync.Map
}

func (h *ttlTaskSchedulerHandler) InitSubtaskExecEnv(_ context.Context) error {
	return nil
}

func (h *ttlTaskSchedulerHandler) SplitSubtask(_ context.Context, subtask []byte) ([]proto.MinimalTask, error) {
	var taskMeta ttlTaskMeta
	if err := json.Unmarshal(subtask, &taskMeta); err != nil {
		return nil, err
	}

	minimalTask := &ttlMinimalTask{ttlTaskMeta: &taskMeta}
	h.tasks.Store(taskMeta.ScanID, minimalTask)

	return []proto.MinimalTask{minimalTask}, nil
}

func (h *ttlTaskSchedulerHandler) CleanupSubtaskExecEnv(_ context.Context) error {
	return nil
}

func (h *ttlTaskSchedulerHandler) OnSubtaskFinished(_ context.Context, subtask []byte) ([]byte, error) {
	var taskMeta ttlTaskMeta
	if err := json.Unmarshal(subtask, &taskMeta); err != nil {
		return nil, err
	}

	t, ok := h.tasks.Load(taskMeta.ScanID)
	if !ok {
		return nil, errors.Errorf("cannot find minimal task with scan id: %d", taskMeta.ScanID)
	}

	bs, err := json.Marshal(t.(*ttlMinimalTask).ToResultMeta())
	if err != nil {
		return nil, err
	}

	return bs, nil
}

func (h *ttlTaskSchedulerHandler) Rollback(_ context.Context) error {
	return nil
}

type ttlTaskExecutor struct {
	task    *ttlMinimalTask
	manager *JobManagerV2
}

func (e *ttlTaskExecutor) Run(ctx context.Context) error {
	var waitRowCompleteStart time.Time
	for {
		resp, err := e.manager.ScheduleTask(ctx, e.task)
		if err != nil {
			return err
		}

		var result *ttlScanTaskExecResult
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result = <-resp:
		}

		if errors.ErrorEqual(errScanWorkerShrink, result.err) {
			time.Sleep(10 * time.Second)
			continue
		}

		if result.err != nil {
			e.task.err.Store(&result.err)
			return nil
		}

		if waitRowCompleteStart.IsZero() {
			waitRowCompleteStart = time.Now()
		}

		statistics := result.task.statistics
		if statistics.TotalRows.Load() > statistics.ErrorRows.Load()+statistics.SuccessRows.Load() {
			if time.Since(waitRowCompleteStart) > 10*time.Minute {
				err = errors.New("timeout for wait rows complete")
				e.task.err.Store(&err)
				return nil
			}

			time.Sleep(2 * time.Second)
			continue
		}

		return nil
	}
}

func (m *JobManagerV2) RegisterDistTask() {
	dispatcher.RegisterTaskFlowHandle(proto.TaskTTL, &ttlJobTaskHandle{
		pool:  m.sessPool,
		store: m.store,
	})

	scheduler.RegisterTaskType(proto.TaskTTL, scheduler.WithPoolSize(32))

	scheduler.RegisterSchedulerConstructor(proto.TaskTTL, proto.StepOne, func(taskID int64, _ []byte, _ int64) (scheduler.Scheduler, error) {
		return &ttlTaskSchedulerHandler{}, nil
	}, scheduler.WithConcurrentSubtask())

	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTTL, proto.StepOne,
		func(minimalTask proto.MinimalTask, step int64) (scheduler.SubtaskExecutor, error) {
			task, ok := minimalTask.(*ttlMinimalTask)
			if !ok {
				return nil, errors.Errorf("invalid task type %T", minimalTask)
			}
			return &ttlTaskExecutor{manager: m, task: task}, nil
		},
	)
}

type disttaskTTLJobAdapter struct {
}

func (d *disttaskTTLJobAdapter) SubmitJob(_ context.Context, id string, tableID int64, physicalID int64, watermark time.Time) (*ttlJobBrief, error) {
	jobMeta := &ttlJobMeta{
		TableID:       tableID,
		PhysicalID:    physicalID,
		WatermarkUnix: watermark.Unix(),
	}

	bs, err := json.Marshal(jobMeta)
	if err != nil {
		return nil, err
	}

	taskKey := fmt.Sprintf("/tidb/ttl/table/%d/%d/%s", tableID, physicalID, id)
	_, err = handle.SubmitGlobalTask(taskKey, proto.TaskTTL, dispatcher.MaxSubtaskConcurrency, bs)
	if err != nil {
		return nil, err
	}

	return &ttlJobBrief{
		ID:       id,
		Finished: false,
	}, nil
}

func (d *disttaskTTLJobAdapter) GetJob(_ context.Context, id string, tableID int64, physicalID int64) (*ttlJobBrief, error) {
	mgr, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}

	taskKey := fmt.Sprintf("/tidb/ttl/table/%d/%d/%s", tableID, physicalID, id)
	task, err := mgr.GetGlobalTaskByKey(taskKey)
	if err != nil {
		return nil, err
	}

	if task == nil {
		return nil, nil
	}

	subtasks, err := mgr.GetSubtasksByStep(task.ID, proto.StepOne)
	if err != nil {
		return nil, err
	}

	var summary TTLSummary
	summary.TotalScanTask = len(subtasks)
	summary.ScheduledScanTask = len(subtasks)

	for _, subtask := range subtasks {
		var subtaskMeta ttlTaskMeta
		if err = json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
			return nil, err
		}

		if subtask.State == proto.TaskStateSucceed {
			summary.FinishedScanTask++
		}

		if taskState := subtaskMeta.TaskState; taskState != nil {
			summary.TotalRows += taskState.TotalRows
			summary.SuccessRows += taskState.SuccessRows
			summary.ErrorRows += taskState.ErrorRows
			if taskState.ScanTaskErr != "" {
				summary.ScanTaskErr = taskState.ScanTaskErr
			}
		}
	}

	return &ttlJobBrief{
		ID:       id,
		Finished: task.IsFinished(),
		Summary:  &summary,
	}, nil
}
