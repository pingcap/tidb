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

package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/taskkey"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
)

// RuntimeInfo is the runtime information of the task for corresponding job.
type RuntimeInfo struct {
	Status     proto.TaskState
	ImportRows int64
	ErrorMsg   string

	Step       proto.Step
	UpdateTime types.Time
	Speed      int64
	Processed  int64
	Total      int64
}

var notAvailable = "N/A"

func (ri *RuntimeInfo) isConflictStep() bool {
	// Conflict steps track "processed" by conflicted-row count, not by byte size
	// like other import steps.
	return ri.Step == proto.ImportStepCollectConflicts || ri.Step == proto.ImportStepConflictResolution
}

// Percent returns the progress percentage of the current step.
func (ri *RuntimeInfo) Percent() string {
	// Query output size is unknown; initialization and post-process have no measurable total.
	if ri.Step == proto.ImportStepPostProcess || ri.Step == proto.StepInit || ri.Step == proto.ImportStepQuery {
		return notAvailable
	}

	percentage := 0.0
	if ri.Total > 0 {
		percentage = float64(ri.Processed) / float64(ri.Total)
		percentage = min(percentage, 1.0)
	}
	return strconv.FormatInt(int64(percentage*100), 10)
}

// FormatSecondAsTime formats the given seconds into the given format
// If the duration is less than a day, it returns the time in HH:MM:SS format.
// Otherwise, it returns the time in DD d HH:MM:SS format.
func FormatSecondAsTime(sec int64) string {
	day := ""
	dur := time.Duration(sec) * time.Second
	if dur.Hours() >= 24 {
		day = fmt.Sprintf("%d d ", int(dur.Hours()/24))
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", day, int(dur.Hours())%24, int(dur.Minutes())%60, int(dur.Seconds())%60)
}

// ETA returns the estimated time of arrival (ETA) for the current step.
func (ri *RuntimeInfo) ETA() string {
	remainTime := notAvailable
	if ri.Speed > 0 && ri.Total > 0 {
		remainSecond := max((ri.Total-ri.Processed)/ri.Speed, 0)
		remainTime = FormatSecondAsTime(remainSecond)
	}

	return remainTime
}

// TotalSize returns the total size of the current step in human-readable format.
func (ri *RuntimeInfo) TotalSize() string {
	if ri.Step == proto.ImportStepQuery {
		return notAvailable
	}
	if ri.isConflictStep() {
		return fmt.Sprintf("%d conflicts", ri.Total)
	}
	return units.BytesSize(float64(ri.Total))
}

// ProcessedSize returns the processed size of the current step in human-readable format.
func (ri *RuntimeInfo) ProcessedSize() string {
	if ri.Step == proto.ImportStepQuery {
		return notAvailable
	}
	if ri.isConflictStep() {
		return fmt.Sprintf("%d conflicts", ri.Processed)
	}
	return units.BytesSize(float64(ri.Processed))
}

// SpeedStr returns the processing speed of the current step in human-readable format.
func (ri *RuntimeInfo) SpeedStr() string {
	if ri.Step == proto.ImportStepQuery {
		return notAvailable
	}
	if ri.isConflictStep() {
		return fmt.Sprintf("%d conflicts/s", ri.Speed)
	}
	return fmt.Sprintf("%s/s", units.BytesSize(float64(ri.Speed)))
}

// convertToMySQLTime converts go time to MySQL time with the specified location.
// It's partially copied from builtin_time.go
func convertToMySQLTime(t time.Time, loc *time.Location) (types.Time, error) {
	tr, err := types.TruncateFrac(t, 0)
	if err != nil {
		return types.ZeroTime, err
	}

	result := types.NewTime(types.FromGoTime(tr), mysql.TypeDatetime, 0)
	err = result.ConvertTimeZone(t.Location(), loc)
	return result, err
}

// GetJobRuntimeInfo get the corresponding DXF task runtime info for the job.
func GetJobRuntimeInfo(
	ctx context.Context,
	location *time.Location,
	jobID int64,
) (*RuntimeInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)

	dxfTaskMgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return nil, err
	}

	task, err := dxfTaskMgr.GetTaskByKeyWithHistory(ctx, taskkey.ForImportJob(jobID))
	if err != nil {
		return nil, err
	}

	var (
		taskMeta TaskMeta

		latestTime time.Time
		ri         = &RuntimeInfo{
			Status: task.State,
			Step:   task.Step,
		}
	)

	if err = json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, errors.Trace(err)
	}

	if task.Error != nil {
		ri.ErrorMsg = task.Error.Error()
		return ri, nil
	}

	summaries, err := dxfTaskMgr.GetAllSubtaskSummaryByStep(ctx, task.ID, task.Step)
	if err != nil {
		return nil, err
	}

	currentTime := time.Now()
	timeRange := execute.SubtaskSpeedUpdateInterval

	failpoint.Inject("mockSpeedDuration", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			currentTime = time.Unix(1000, int64(v*1000000))
			timeRange = time.Millisecond * time.Duration(v)
		}
	})

	ri.Speed = 0
	for _, s := range summaries {
		ri.Processed += s.Processed.Load()
		ri.ImportRows += s.RowCnt.Load()
		ri.Speed += s.GetSpeedInTimeRange(currentTime, timeRange)
		if s.UpdateTime().After(latestTime) {
			latestTime = s.UpdateTime()
		}
	}

	if task.Step == proto.ImportStepPostProcess {
		ri.ImportRows = taskMeta.Summary.ImportedRows
	} else if task.Step != proto.ImportStepWriteAndIngest && task.Step != proto.ImportStepImport {
		ri.ImportRows = 0
	}

	switch task.Step {
	case proto.ImportStepImport, proto.ImportStepWriteAndIngest:
		ri.Total = taskMeta.Summary.IngestSummary.Bytes
	case proto.ImportStepEncodeAndSort:
		ri.Total = taskMeta.Summary.EncodeSummary.Bytes
	case proto.ImportStepMergeSort:
		ri.Total = taskMeta.Summary.MergeSummary.Bytes
	case proto.ImportStepCollectConflicts:
		ri.Total = taskMeta.Summary.CollectConflictsSummary.RowCnt
	case proto.ImportStepConflictResolution:
		ri.Total = taskMeta.Summary.ResolveConflictsSummary.RowCnt
	}

	if !latestTime.IsZero() {
		ri.UpdateTime, err = convertToMySQLTime(latestTime, location)
		if err != nil {
			return nil, err
		}
	}

	return ri, nil
}

// GetJobLastUpdateTime get the last update time for given job from all subtasks.
func GetJobLastUpdateTime(ctx context.Context, jobID int64) (types.Time, error) {
	taskManager, err := storage.GetDXFSvcTaskMgr()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return types.ZeroTime, err
	}
	taskKey := taskkey.ForImportJob(jobID)
	task, err := taskManager.GetTaskBaseByKeyWithHistory(ctx, taskKey)
	if err != nil {
		return types.ZeroTime, err
	}

	var rs []chunk.Row
	err = taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		rs, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`select FROM_UNIXTIME(max(state_update_time)) from
				(select state_update_time from mysql.tidb_background_subtask where task_key = %?
					union
				select state_update_time from mysql.tidb_background_subtask_history where task_key = %?
			) t`,
			storage.TaskIDToKey(task.ID), storage.TaskIDToKey(task.ID),
		)
		return err
	})

	if rs[0].IsNull(0) {
		return types.ZeroTime, nil
	}

	return rs[0].GetTime(0), nil
}
