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
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// SubmitStandaloneTask submits a task to the distribute framework that only runs on the current node.
// when import from server-disk, pass engine checkpoints too, as scheduler might run on another
// node where we can't access the data files.
func SubmitStandaloneTask(ctx context.Context, plan *importer.Plan, stmt string, ecp map[int32]*checkpoints.EngineCheckpoint) (int64, *proto.TaskBase, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return 0, nil, err
	}
	return doSubmitTask(ctx, plan, stmt, serverInfo, toChunkMap(ecp))
}

// SubmitTask submits a task to the distribute framework that runs on all managed nodes.
func SubmitTask(ctx context.Context, plan *importer.Plan, stmt string) (int64, *proto.TaskBase, error) {
	return doSubmitTask(ctx, plan, stmt, nil, nil)
}

func doSubmitTask(ctx context.Context, plan *importer.Plan, stmt string, instance *infosync.ServerInfo, chunkMap map[int32][]Chunk) (int64, *proto.TaskBase, error) {
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

	var jobID, taskID int64
	if err = taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		var err2 error
		exec := se.GetSQLExecutor()
		jobID, err2 = importer.CreateJob(ctx, exec, plan.DBName, plan.TableInfo.Name.L, plan.TableInfo.ID,
			plan.User, plan.Parameters, plan.TotalFileSize)
		if err2 != nil {
			return err2
		}

		// TODO: use planner.Run to run the logical plan
		// now creating import job and submitting distributed task should be in the same transaction.
		logicalPlan := &LogicalPlan{
			JobID:             jobID,
			Plan:              *plan,
			Stmt:              stmt,
			EligibleInstances: instances,
			ChunkMap:          chunkMap,
		}
		planCtx := planner.PlanCtx{
			Ctx:        ctx,
			SessionCtx: se,
			TaskKey:    TaskKey(jobID),
			TaskType:   proto.ImportInto,
			ThreadCnt:  plan.ThreadCnt,
		}
		p := planner.NewPlanner()
		taskID, err2 = p.Run(planCtx, logicalPlan)
		if err2 != nil {
			return err2
		}
		return nil
	}); err != nil {
		return 0, nil, err
	}
	handle.NotifyTaskChange()
	task, err := taskManager.GetTaskBaseByID(ctx, taskID)
	if err != nil {
		return 0, nil, err
	}

	metrics.UpdateMetricsForAddTask(task)

	logutil.BgLogger().Info("job submitted to task queue",
		zap.Int64("job-id", jobID),
		zap.Int64("task-id", task.ID),
		zap.String("data-size", units.BytesSize(float64(plan.TotalFileSize))),
		zap.Int("thread-cnt", plan.ThreadCnt),
		zap.Bool("global-sort", plan.IsGlobalSort()))

	return jobID, task, nil
}

// GetTaskImportedRows gets the number of imported rows of a job.
// Note: for finished job, we can get the number of imported rows from task meta.
func GetTaskImportedRows(ctx context.Context, jobID int64) (uint64, error) {
	taskManager, err := storage.GetTaskManager()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return 0, err
	}
	taskKey := TaskKey(jobID)
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	if err != nil {
		return 0, err
	}
	taskMeta := TaskMeta{}
	if err = json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return 0, errors.Trace(err)
	}
	var importedRows uint64
	if taskMeta.Plan.CloudStorageURI == "" {
		subtasks, err := taskManager.GetSubtasksWithHistory(ctx, task.ID, proto.ImportStepImport)
		if err != nil {
			return 0, err
		}
		for _, subtask := range subtasks {
			var subtaskMeta ImportStepMeta
			if err2 := json.Unmarshal(subtask.Meta, &subtaskMeta); err2 != nil {
				return 0, errors.Trace(err2)
			}
			importedRows += subtaskMeta.Result.LoadedRowCnt
		}
	} else {
		subtasks, err := taskManager.GetSubtasksWithHistory(ctx, task.ID, proto.ImportStepWriteAndIngest)
		if err != nil {
			return 0, err
		}
		for _, subtask := range subtasks {
			var subtaskMeta WriteIngestStepMeta
			if err2 := json.Unmarshal(subtask.Meta, &subtaskMeta); err2 != nil {
				return 0, errors.Trace(err2)
			}
			importedRows += subtaskMeta.Result.LoadedRowCnt
		}
	}
	return importedRows, nil
}

// TaskKey returns the task key for a job.
func TaskKey(jobID int64) string {
	return fmt.Sprintf("%s/%d", proto.ImportInto, jobID)
}
