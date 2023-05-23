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
	"github.com/pingcap/tidb/disttask/framework/handle"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	checkTaskFinishInterval = 300 * time.Millisecond
)

// DistImporter is a JobImporter for distributed load data.
type DistImporter struct {
	*importer.JobImportParam
	plan   *importer.Plan
	stmt   string
	logger *zap.Logger
	// the instance to import data, used for single-node import, nil means import data on all instances.
	instance *infosync.ServerInfo
}

var _ importer.JobImporter = &DistImporter{}

// NewDistImporter creates a new DistImporter.
func NewDistImporter(param *importer.JobImportParam, plan *importer.Plan, stmt string) (*DistImporter, error) {
	return &DistImporter{
		JobImportParam: param,
		plan:           plan,
		stmt:           stmt,
		logger:         logutil.BgLogger().With(zap.String("component", "distribute importer"), zap.Int("id", int(param.Job.ID))),
	}, nil
}

// NewDistImporterCurrNode creates a new DistImporter to import data on current node.
func NewDistImporterCurrNode(param *importer.JobImportParam, plan *importer.Plan, stmt string) (*DistImporter, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return nil, err
	}
	return &DistImporter{
		JobImportParam: param,
		plan:           plan,
		stmt:           stmt,
		instance:       serverInfo,
	}, nil
}

// Param implements JobImporter.Param.
func (ti *DistImporter) Param() *importer.JobImportParam {
	return ti.JobImportParam
}

// Import implements JobImporter.Import.
func (ti *DistImporter) Import() {
	ti.logger.Info("start distribute load data")
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

func buildDistTask(plan *importer.Plan, jobID int64, stmt string) TaskMeta {
	return TaskMeta{
		Plan:  *plan,
		JobID: jobID,
		Stmt:  stmt,
	}
}

func (ti *DistImporter) doImport(ctx context.Context) error {
	var instances []*infosync.ServerInfo
	if ti.instance != nil {
		instances = append(instances, ti.instance)
	}
	task := TaskMeta{
		Plan:              *ti.plan,
		JobID:             ti.Job.ID,
		Stmt:              ti.stmt,
		EligibleInstances: instances,
	}
	taskMeta, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return handle.SubmitAndRunGlobalTask(ctx, ti.taskKey(), proto.LoadData, int(ti.plan.ThreadCnt), taskMeta)
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
