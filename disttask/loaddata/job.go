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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	distLoadDataConcurrency = 16
	checkTaskFinishInterval = 300 * time.Millisecond
)

// DistImporter is a JobImporter for distributed load data.
type DistImporter struct {
	*importer.JobImportParam
	plan *importer.Plan
	stmt string
}

var _ importer.JobImporter = &DistImporter{}

// NewDistImporter creates a new DistImporter.
func NewDistImporter(param *importer.JobImportParam, plan *importer.Plan, stmt string) (*DistImporter, error) {
	return &DistImporter{
		JobImportParam: param,
		plan:           plan,
		stmt:           stmt,
	}, nil
}

// Param implements JobImporter.Param.
func (ti *DistImporter) Param() *importer.JobImportParam {
	return ti.JobImportParam
}

// Import implements JobImporter.Import.
func (ti *DistImporter) Import() {
	logutil.BgLogger().Info("start distribute load data", zap.Int64("jobID", ti.Job.ID))
	ti.Group.Go(func() error {
		defer close(ti.Done)
		return ti.doImport(ti.GroupCtx)
	})
}

// Result implements JobImporter.Result.
func (ti *DistImporter) Result() importer.JobImportResult {
	var result importer.JobImportResult
	taskMeta, err := ti.getTaskMeta()
	if err != nil {
		result.Msg = err.Error()
		return result
	}

	logutil.BgLogger().Info("finish distribute load data", zap.Int64("jobID", ti.Job.ID), zap.Any("task meta", taskMeta))
	var (
		numWarnings uint64
		numRecords  uint64
		numDeletes  uint64
		numSkipped  uint64
	)
	numRecords = taskMeta.Metrics.ReadRowCnt
	// todo: we don't have a strict REPLACE or IGNORE mode in physical mode, so we can't get the numDeletes/numSkipped.
	// we can have it when there's duplicate detection.
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo].Raw, numRecords, numDeletes, numSkipped, numWarnings)
	return importer.JobImportResult{
		Msg:          msg,
		Affected:     taskMeta.Metrics.ReadRowCnt,
		LastInsertID: taskMeta.Metrics.LastInsertID,
	}
}

// Close implements the io.Closer interface.
func (*DistImporter) Close() error {
	return nil
}

func buildDistTask(plan *importer.Plan, jobID int64, stmt string) TaskMeta {
	return TaskMeta{
		Plan:  *plan,
		JobID: jobID,
		Stmt:  stmt,
	}
}

// submitGlobalTaskAndRun submits a global task and returns a channel that will be closed when the task is done.
// TODO: move to handle, see https://github.com/pingcap/tidb/pull/43066
func submitGlobalTaskAndRun(ctx context.Context, taskKey, taskType string, concurrency int, taskMeta []byte) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	globalTask, err := globalTaskManager.GetGlobalTaskByKey(taskKey)
	if err != nil {
		return err
	}

	if globalTask == nil {
		taskID, err := globalTaskManager.AddNewGlobalTask(taskKey, taskType, concurrency, taskMeta)
		if err != nil {
			return err
		}

		globalTask, err = globalTaskManager.GetGlobalTaskByID(taskID)
		if err != nil {
			return err
		}

		if globalTask == nil {
			return errors.Errorf("cannot find global task with ID %d", taskID)
		}
	}

	ticker := time.NewTicker(checkTaskFinishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logutil.BgLogger().Warn("context done", zap.Error(ctx.Err()))
			return ctx.Err()
		case <-ticker.C:
			found, err := globalTaskManager.GetGlobalTaskByID(globalTask.ID)
			if err != nil {
				logutil.BgLogger().Info("get global task error", zap.Int64("taskID", globalTask.ID), zap.Error(err))
				continue
			}

			if found == nil {
				return errors.Errorf("cannot find global task with ID %d", globalTask.ID)
			}

			if found.State == proto.TaskStateSucceed {
				return nil
			}

			// TODO: get the original error message.
			if found.State == proto.TaskStateFailed || found.State == proto.TaskStateCanceled || found.State == proto.TaskStateReverted {
				return errors.Errorf("task stopped with state %s", found.State)
			}
		}
	}
}

func (ti *DistImporter) doImport(ctx context.Context) error {
	task := buildDistTask(ti.plan, ti.Job.ID, ti.stmt)
	taskMeta, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return submitGlobalTaskAndRun(ctx, ti.taskKey(), proto.LoadData, distLoadDataConcurrency, taskMeta)
}

func (ti *DistImporter) taskKey() string {
	return fmt.Sprintf("ddl/%s/%d", proto.LoadData, ti.Job.ID)
}

func (ti *DistImporter) getTaskMeta() (*TaskMeta, error) {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	taskKey := ti.taskKey()
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
