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

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
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

func doSubmitTask(ctx context.Context, plan *importer.Plan, stmt string, instance *infosync.ServerInfo, chunkMap map[int32][]importer.Chunk) (int64, *proto.TaskBase, error) {
	var instances []*infosync.ServerInfo
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
	if err = taskManager.WithNewTxn(ctx, func(se sessionapi.Context) error {
		var err2 error
		exec := se.GetSQLExecutor()
		jobID, err2 = importer.CreateJob(ctx, exec, plan.DBName, plan.TableInfo.Name.L, plan.TableInfo.ID,
			plan.User, plan.Parameters, plan.TotalFileSize)
		if err2 != nil {
			return err2
		}
		// in classical kernel or if we are inside SYSTEM keyspace itself, we
		// submit the task to DXF in the same transaction as creating the job.
		if kerneltype.IsClassic() || config.GetGlobalKeyspaceName() == keyspace.System {
			logicalPlan.JobID = jobID
			planCtx.SessionCtx = se
			planCtx.TaskKey = TaskKey(jobID)
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
	if keyspace.IsRunningOnUser() {
		var err2 error
		dxfTaskMgr, err2 = storage.GetDXFSvcTaskMgr()
		if err2 != nil {
			return 0, nil, err2
		}
		if err2 = dxfTaskMgr.WithNewTxn(ctx, func(se sessionapi.Context) error {
			logicalPlan.JobID = jobID
			planCtx.SessionCtx = se
			planCtx.TaskKey = TaskKey(jobID)
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
}

// GetRuntimeInfoForJob get the corresponding DXF task runtime info for the job.
func GetRuntimeInfoForJob(ctx context.Context, jobID int64) (*RuntimeInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	dxfTaskMgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return nil, err
	}

	taskKey := TaskKey(jobID)
	task, err := dxfTaskMgr.GetTaskByKeyWithHistory(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	taskMeta := TaskMeta{}
	if err = json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, errors.Trace(err)
	}

	step := proto.ImportStepImport
	if taskMeta.Plan.CloudStorageURI != "" {
		step = proto.ImportStepWriteAndIngest
	}

	summaries, err := dxfTaskMgr.GetAllSubtaskSummaryByStep(ctx, task.ID, step)
	if err != nil {
		return nil, err
	}

	var importedRows int64
	for _, summary := range summaries {
		importedRows += summary.RowCnt.Load()
	}

	var errMsg string
	if task.Error != nil {
		errMsg = task.Error.Error()
	}
	return &RuntimeInfo{
		Status:     task.State,
		ImportRows: importedRows,
		ErrorMsg:   errMsg,
	}, nil
}

// TaskKey returns the task key for a job.
func TaskKey(jobID int64) string {
	if kerneltype.IsNextGen() {
		ks := keyspace.GetKeyspaceNameBySettings()
		return fmt.Sprintf("%s/%s/%d", ks, proto.ImportInto, jobID)
	}
	return fmt.Sprintf("%s/%d", proto.ImportInto, jobID)
}
