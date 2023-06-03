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

package loaddata

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/handle"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// DistImporter is a JobImporter for distributed load data.
type DistImporter struct {
	*importer.JobImportParam
	plan   *importer.Plan
	stmt   string
	logger *zap.Logger
	// the instance to import data, used for single-node import, nil means import data on all instances.
	instance       *infosync.ServerInfo
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
		logger:         logutil.BgLogger().With(zap.String("component", "importer")),
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
		logger:         logutil.BgLogger().With(zap.String("component", "importer")),
		instance:       serverInfo,
		sourceFileSize: sourceFileSize,
	}, nil
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
	ti.logger.Info("start distribute load data")
	ti.Group.Go(func() error {
		defer close(ti.Done)
		// task is run using distribute framework, so we only wait for the task to finish.
		return handle.WaitGlobalTask(ti.GroupCtx, task)
	})
}

// Result implements JobImporter.Result.
func (ti *DistImporter) Result() importer.JobImportResult {
	var result importer.JobImportResult
	taskMeta, err := getTaskMeta(ti.jobID)
	if err != nil {
		result.Msg = err.Error()
		return result
	}

	ti.logger.Info("finish distribute load data", zap.Any("task meta", taskMeta))
	var (
		numWarnings uint64
		numRecords  uint64
		numDeletes  uint64
		numSkipped  uint64
	)
	numRecords = taskMeta.Result.ReadRowCnt
	// todo: we don't have a strict REPLACE or IGNORE mode in physical mode, so we can't get the numDeletes/numSkipped.
	// we can have it when there's duplicate detection.
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo].Raw, numRecords, numDeletes, numSkipped, numWarnings)
	return importer.JobImportResult{
		Msg:      msg,
		Affected: taskMeta.Result.ReadRowCnt,
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
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return 0, nil, err
	}

	var jobID, taskID int64
	plan := ti.plan
	if err = globalTaskManager.WithNewTxn(func(se sessionctx.Context) error {
		var err2 error
		exec := se.(sqlexec.SQLExecutor)
		jobID, err2 = importer.CreateJob(ctx, exec, plan.DBName, plan.TableInfo.Name.L, plan.TableInfo.ID,
			plan.User, plan.Parameters, ti.sourceFileSize)
		if err2 != nil {
			return err2
		}
		task := TaskMeta{
			JobID:             jobID,
			Plan:              *plan,
			Stmt:              ti.stmt,
			EligibleInstances: instances,
		}
		taskMeta, err2 := json.Marshal(task)
		if err2 != nil {
			return err2
		}
		taskID, err2 = globalTaskManager.AddGlobalTaskWithSession(se, TaskKey(jobID), proto.ImportInto,
			int(plan.ThreadCnt), taskMeta)
		if err2 != nil {
			return err2
		}
		return nil
	}); err != nil {
		return 0, nil, err
	}

	globalTask, err := globalTaskManager.GetGlobalTaskByID(taskID)
	if err != nil {
		return 0, nil, err
	}
	if globalTask == nil {
		return 0, nil, errors.Errorf("cannot find global task with ID %d", taskID)
	}
	// update logger with task id.
	ti.jobID = jobID
	ti.taskID = taskID
	ti.logger = ti.logger.With(zap.Int64("task-id", globalTask.ID))

	ti.logger.Info("job submitted to global task queue", zap.Int64("job-id", jobID))

	return jobID, globalTask, nil
}

func (*DistImporter) taskKey() string {
	// task key is meaningless to IMPORT INTO, so we use a random uuid.
	return fmt.Sprintf("%s/%s", proto.ImportInto, uuid.New().String())
}

func getTaskMeta(jobID int64) (*TaskMeta, error) {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	taskKey := TaskKey(jobID)
	globalTask, err := globalTaskManager.GetGlobalTaskByKey(taskKey)
	if err != nil {
		return nil, err
	}
	if globalTask == nil {
		return nil, errors.Errorf("cannot find global task with key %s", taskKey)
	}
	var taskMeta TaskMeta
	if err := json.Unmarshal(globalTask.Meta, &taskMeta); err != nil {
		return nil, err
	}
	return &taskMeta, nil
}

// GetTaskImportedRows gets the number of imported rows of a job.
// Note: for finished job, we can get the number of imported rows from task meta.
func GetTaskImportedRows(jobID int64) (uint64, error) {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return 0, err
	}
	taskKey := TaskKey(jobID)
	globalTask, err := globalTaskManager.GetGlobalTaskByKey(taskKey)
	if err != nil {
		return 0, err
	}
	if globalTask == nil {
		return 0, errors.Errorf("cannot find global task with key %s", taskKey)
	}
	subtasks, err := globalTaskManager.GetSubtasksByStep(globalTask.ID, Import)
	if err != nil {
		return 0, err
	}
	var importedRows uint64
	for _, subtask := range subtasks {
		var subtaskMeta SubtaskMeta
		if err2 := json.Unmarshal(subtask.Meta, &subtaskMeta); err2 != nil {
			return 0, err2
		}
		importedRows += subtaskMeta.Result.LoadedRowCnt
	}
	return importedRows, nil
}

// TaskKey returns the task key for a job.
func TaskKey(jobID int64) string {
	return fmt.Sprintf("%s/%d", proto.ImportInto, jobID)
}
