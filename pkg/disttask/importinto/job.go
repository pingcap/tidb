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

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// DistImporter is a JobImporter for distributed IMPORT INTO.
type DistImporter struct {
	*importer.JobImportParam
	plan   *importer.Plan
	stmt   string
	logger *zap.Logger
	// the instance to import data, used for single-node import, nil means import data on all instances.
	instance *infosync.ServerInfo
	// the files to import, when import from server file, we need to pass those file to the framework.
	chunkMap       map[int32][]Chunk
	sourceFileSize int64
	// only set after submit task
	jobID  int64
	taskID int64
}

// NewDistImporter creates a new DistImporter.
func NewDistImporter(param *importer.JobImportParam, plan *importer.Plan, stmt string, sourceFileSize int64) (*DistImporter, error) {
	return &DistImporter{
		JobImportParam: param,
		plan:           plan,
		stmt:           stmt,
		logger:         logutil.BgLogger(),
		sourceFileSize: sourceFileSize,
	}, nil
}

// NewDistImporterCurrNode creates a new DistImporter to import data on current node.
func NewDistImporterCurrNode(param *importer.JobImportParam, plan *importer.Plan, stmt string, sourceFileSize int64) (*DistImporter, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return nil, err
	}
	return &DistImporter{
		JobImportParam: param,
		plan:           plan,
		stmt:           stmt,
		logger:         logutil.BgLogger(),
		instance:       serverInfo,
		sourceFileSize: sourceFileSize,
	}, nil
}

// NewDistImporterServerFile creates a new DistImporter to import given files on current node.
// we also run import on current node.
// todo: merge all 3 ctor into one.
func NewDistImporterServerFile(param *importer.JobImportParam, plan *importer.Plan, stmt string, ecp map[int32]*checkpoints.EngineCheckpoint, sourceFileSize int64) (*DistImporter, error) {
	distImporter, err := NewDistImporterCurrNode(param, plan, stmt, sourceFileSize)
	if err != nil {
		return nil, err
	}
	distImporter.chunkMap = toChunkMap(ecp)
	return distImporter, nil
}

// Param implements JobImporter.Param.
func (ti *DistImporter) Param() *importer.JobImportParam {
	return ti.JobImportParam
}

// Import implements JobImporter.Import.
func (*DistImporter) Import() {
	// todo: remove it
}

// ImportTask import task.
func (ti *DistImporter) ImportTask(task *proto.Task) {
	ti.logger.Info("start distribute IMPORT INTO")
	ti.Group.Go(func() error {
		defer close(ti.Done)
		// task is run using distribute framework, so we only wait for the task to finish.
		return handle.WaitTask(ti.GroupCtx, task.ID)
	})
}

// Result implements JobImporter.Result.
func (ti *DistImporter) Result(ctx context.Context) importer.JobImportResult {
	var result importer.JobImportResult
	taskMeta, err := getTaskMeta(ctx, ti.jobID)
	if err != nil {
		return result
	}

	return importer.JobImportResult{
		Affected:   taskMeta.Result.LoadedRowCnt,
		ColSizeMap: taskMeta.Result.ColSizeMap,
	}
}

// Close implements the io.Closer interface.
func (*DistImporter) Close() error {
	return nil
}

// SubmitTask submits a task to the distribute framework.
func (ti *DistImporter) SubmitTask(ctx context.Context) (int64, *proto.Task, error) {
	var instances []*infosync.ServerInfo
	if ti.instance != nil {
		instances = append(instances, ti.instance)
	}
	// we use taskManager to submit task, user might not have the privilege to system tables.
	taskManager, err := storage.GetTaskManager()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return 0, nil, err
	}

	var jobID, taskID int64
	plan := ti.plan
	if err = taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		var err2 error
		exec := se.(sqlexec.SQLExecutor)
		// If 2 client try to execute IMPORT INTO concurrently, there's chance that both of them will pass the check.
		// We can enforce ONLY one import job running by:
		// 	- using LOCK TABLES, but it requires enable-table-lock=true, it's not enabled by default.
		// 	- add a key to PD as a distributed lock, but it's a little complex, and we might support job queuing later.
		// So we only add this simple soft check here and doc it.
		activeJobCnt, err2 := importer.GetActiveJobCnt(ctx, exec)
		if err2 != nil {
			return err2
		}
		if activeJobCnt > 0 {
			return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("there's pending or running jobs")
		}
		jobID, err2 = importer.CreateJob(ctx, exec, plan.DBName, plan.TableInfo.Name.L, plan.TableInfo.ID,
			plan.User, plan.Parameters, ti.sourceFileSize)
		if err2 != nil {
			return err2
		}

		// TODO: use planner.Run to run the logical plan
		// now creating import job and submitting distributed task should be in the same transaction.
		logicalPlan := &LogicalPlan{
			JobID:             jobID,
			Plan:              *plan,
			Stmt:              ti.stmt,
			EligibleInstances: instances,
			ChunkMap:          ti.chunkMap,
		}
		planCtx := planner.PlanCtx{
			Ctx:        ctx,
			SessionCtx: se,
			TaskKey:    TaskKey(jobID),
			TaskType:   proto.ImportInto,
			ThreadCnt:  int(plan.ThreadCnt),
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
	task, err := taskManager.GetTaskByID(ctx, taskID)
	if err != nil {
		return 0, nil, err
	}
	if task == nil {
		return 0, nil, errors.Errorf("cannot find task with ID %d", taskID)
	}

	metrics.UpdateMetricsForAddTask(task)
	// update logger with task id.
	ti.jobID = jobID
	ti.taskID = taskID
	ti.logger = ti.logger.With(zap.Int64("task-id", task.ID))

	ti.logger.Info("job submitted to task queue",
		zap.Int64("job-id", jobID), zap.Int64("thread-cnt", plan.ThreadCnt))

	return jobID, task, nil
}

func (*DistImporter) taskKey() string {
	// task key is meaningless to IMPORT INTO, so we use a random uuid.
	return fmt.Sprintf("%s/%s", proto.ImportInto, uuid.New().String())
}

// JobID returns the job id.
func (ti *DistImporter) JobID() int64 {
	return ti.jobID
}

func getTaskMeta(ctx context.Context, jobID int64) (*TaskMeta, error) {
	taskManager, err := storage.GetTaskManager()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return nil, err
	}
	taskKey := TaskKey(jobID)
	task, err := taskManager.GetTaskByKey(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, errors.Errorf("cannot find task with key %s", taskKey)
	}
	var taskMeta TaskMeta
	if err := json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, errors.Trace(err)
	}
	return &taskMeta, nil
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
	if task == nil {
		return 0, errors.Errorf("cannot find task with key %s", taskKey)
	}
	taskMeta := TaskMeta{}
	if err = json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return 0, errors.Trace(err)
	}
	var importedRows uint64
	if taskMeta.Plan.CloudStorageURI == "" {
		subtasks, err := taskManager.GetSubtasksForImportInto(ctx, task.ID, StepImport)
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
		subtasks, err := taskManager.GetSubtasksForImportInto(ctx, task.ID, StepWriteAndIngest)
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
