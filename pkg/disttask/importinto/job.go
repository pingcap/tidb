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

package importinto

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// SubmitStandaloneTask submits a task to the distribute framework that only runs on the current node.
// when import from server-disk, pass engine checkpoints too, as scheduler might run on another
// node where we can't access the data files.
func SubmitStandaloneTask(ctx context.Context, plan *importer.Plan, stmt string, chunkMap map[int32][]importer.Chunk) (int64, *proto.TaskBase, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return 0, nil, err
	}
	return doSubmitTask(ctx, plan, stmt, serverInfo, chunkMap)
}

// SubmitTask submits a task to the distribute framework that runs on all managed nodes.
func SubmitTask(ctx context.Context, plan *importer.Plan, stmt string) (int64, *proto.TaskBase, error) {
	return doSubmitTask(ctx, plan, stmt, nil, nil)
}

func doSubmitTask(ctx context.Context, plan *importer.Plan, stmt string, instance *serverinfo.ServerInfo, chunkMap map[int32][]importer.Chunk) (int64, *proto.TaskBase, error) {
	var instances []*serverinfo.ServerInfo
	if instance != nil {
		instances = append(instances, instance)
	}
	// we use taskManager to submit task, user might not have the privilege to system tables.
	taskManager, err := storage.GetTaskManager()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return 0, nil, err
	}

	logicalPlan := &LogicalPlan{
		Plan:              *plan,
		Stmt:              stmt,
		EligibleInstances: instances,
		ChunkMap:          chunkMap,
	}
	planCtx := planner.PlanCtx{
		Ctx:        ctx,
		TaskType:   proto.ImportInto,
		ThreadCnt:  plan.ThreadCnt,
		MaxNodeCnt: plan.MaxNodeCnt,
	}
	var (
		jobID, taskID int64
	)
	var runningOnUserKS bool
	if err = taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		runningOnUserKS = kv.IsUserKS(se.GetStore())
		var err2 error
		exec := se.GetSQLExecutor()
		jobID, err2 = importer.CreateJob(ctx, exec, plan.DBName, plan.TableInfo.Name.L, plan.TableInfo.ID,
			plan.User, plan.GroupKey, plan.Parameters, plan.TotalFileSize)
		if err2 != nil {
			return err2
		}
		if kerneltype.IsClassic() {
			err2 = ddl.AlterTableMode(domain.GetDomain(se).DDLExecutor(), se, model.TableModeImport, plan.DBID, plan.TableInfo.ID)
			if err2 != nil {
				return err2
			}
		}
		// in classical kernel or if we are inside SYSTEM keyspace itself, we
		// submit the task to DXF in the same transaction as creating the job.
		if kerneltype.IsClassic() || kv.IsSystemKS(se.GetStore()) {
			logicalPlan.JobID = jobID
			planCtx.SessionCtx = se
			planCtx.TaskKey = TaskKey(jobID)
			planCtx.Keyspace = plan.Keyspace
			if taskID, err2 = submitTask2DXF(logicalPlan, planCtx, taskManager); err2 != nil {
				return err2
			}
		}
		return nil
	}); err != nil {
		return 0, nil, err
	}
	// in next-gen kernel and we are not running in SYSTEM KS, we submit the task
	// to DXF service after creating the job, as DXF service runs in SYSTEM keyspace.
	// TODO: we need to cleanup the job, if we failed to submit the task to DXF service.
	dxfTaskMgr := taskManager
	if runningOnUserKS {
		var err2 error
		dxfTaskMgr, err2 = storage.GetDXFSvcTaskMgr()
		if err2 != nil {
			return 0, nil, err2
		}
		if err2 = dxfTaskMgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
			logicalPlan.JobID = jobID
			planCtx.SessionCtx = se
			planCtx.TaskKey = TaskKey(jobID)
			planCtx.Keyspace = plan.Keyspace
			var err2 error
			if taskID, err2 = submitTask2DXF(logicalPlan, planCtx, dxfTaskMgr); err2 != nil {
				return err2
			}
			return nil
		}); err2 != nil {
			return 0, nil, err2
		}
	}
	handle.NotifyTaskChange()
	task, err := dxfTaskMgr.GetTaskBaseByID(ctx, taskID)
	if err != nil {
		return 0, nil, err
	}

	logutil.BgLogger().Info("job submitted to task queue",
		zap.Int64("job-id", jobID),
		zap.String("task-key", task.Key),
		zap.Int64("task-id", task.ID),
		zap.String("data-size", units.BytesSize(float64(plan.TotalFileSize))),
		zap.Int("thread-cnt", plan.ThreadCnt),
		zap.Bool("global-sort", plan.IsGlobalSort()))

	return jobID, task, nil
}

func submitTask2DXF(logicalPlan *LogicalPlan, planCtx planner.PlanCtx, taskMgr *storage.TaskManager) (int64, error) {
	// TODO: use planner.Run to run the logical plan
	// now creating import job and submitting distributed task should be in the same transaction.
	p := planner.NewPlanner()
	return p.Run(planCtx, logicalPlan, taskMgr)
}

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

// Percent returns the progress percentage of the current step.
func (ri *RuntimeInfo) Percent() string {
	// Currently, we can't track the progress of post process
	if ri.Step == proto.ImportStepPostProcess || ri.Step == proto.StepInit {
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
	return units.BytesSize(float64(ri.Total))
}

// ProcessedSize returns the processed size of the current step in human-readable format.
func (ri *RuntimeInfo) ProcessedSize() string {
	return units.BytesSize(float64(ri.Processed))
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

// GetRuntimeInfoForJob get the corresponding DXF task runtime info for the job.
func GetRuntimeInfoForJob(
	ctx context.Context,
	location *time.Location,
	jobID int64,
) (*RuntimeInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)

	dxfTaskMgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return nil, err
	}

	task, err := dxfTaskMgr.GetTaskByKeyWithHistory(ctx, TaskKey(jobID))
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
		ri.Processed += s.Bytes.Load()
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
	taskKey := TaskKey(jobID)
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
			task.ID, task.ID,
		)
		return err
	})

	if rs[0].IsNull(0) {
		return types.ZeroTime, nil
	}

	return rs[0].GetTime(0), nil
}

// TaskKey returns the task key for a job.
func TaskKey(jobID int64) string {
	if kerneltype.IsNextGen() {
		ks := keyspace.GetKeyspaceNameBySettings()
		return fmt.Sprintf("%s/%s/%d", ks, proto.ImportInto, jobID)
	}
	return fmt.Sprintf("%s/%d", proto.ImportInto, jobID)
}
