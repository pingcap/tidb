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
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/disttask/framework/handle"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// DistImporter is a JobImporter for distributed load data.
type DistImporter struct {
	*importer.JobImportParam
	plan   *importer.Plan
	stmt   string
	logger *zap.Logger
	// the instance to import data, used for single-node import, nil means import data on all instances.
	instance *infosync.ServerInfo
	// the files to import, when import from server file, we need to pass those file to the framework.
	chunkMap map[int32][]Chunk
}

// NewDistImporter creates a new DistImporter.
func NewDistImporter(param *importer.JobImportParam, plan *importer.Plan, stmt string) (*DistImporter, error) {
	return &DistImporter{
		JobImportParam: param,
		plan:           plan,
		stmt:           stmt,
		logger:         logutil.BgLogger().With(zap.String("component", "importer")),
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
		logger:         logutil.BgLogger().With(zap.String("component", "importer")),
		instance:       serverInfo,
	}, nil
}

// NewDistImporterServerFile creates a new DistImporter to import given files on current node.
// we also run import on current node.
func NewDistImporterServerFile(param *importer.JobImportParam, plan *importer.Plan, stmt string, ecp map[int32]*checkpoints.EngineCheckpoint) (*DistImporter, error) {
	distImporter, err := NewDistImporterCurrNode(param, plan, stmt)
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
	ti.logger.Info("start distribute load data")
	ti.Group.Go(func() error {
		defer close(ti.Done)
		// task is run using distribute framework, so we only wait for the task to finish.
		return handle.WaitGlobalTask(ti.GroupCtx, task)
	})
}

// Result implements JobImporter.Result.
func (ti *DistImporter) Result(task *proto.Task) importer.JobImportResult {
	var result importer.JobImportResult
	taskMeta, err := ti.getTaskMeta(task)
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
func (ti *DistImporter) SubmitTask() (*proto.Task, error) {
	var instances []*infosync.ServerInfo
	if ti.instance != nil {
		instances = append(instances, ti.instance)
	}
	task := TaskMeta{
		Plan:              *ti.plan,
		Stmt:              ti.stmt,
		EligibleInstances: instances,
		ChunkMap:          ti.chunkMap,
	}
	taskMeta, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	globalTask, err := handle.SubmitGlobalTask(ti.taskKey(), proto.LoadData, int(ti.plan.ThreadCnt), taskMeta)
	if err != nil {
		return nil, err
	}

	// update logger with task id.
	ti.logger = ti.logger.With(zap.Int64("id", globalTask.ID))

	return globalTask, nil
}

func (*DistImporter) taskKey() string {
	// task key is meaningless to IMPORT INTO, so we use a random uuid.
	return fmt.Sprintf("%s/%s", proto.LoadData, uuid.New().String())
}

func (*DistImporter) getTaskMeta(task *proto.Task) (*TaskMeta, error) {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	globalTask, err := globalTaskManager.GetGlobalTaskByID(task.ID)
	if err != nil {
		return nil, err
	}
	if globalTask == nil {
		return nil, errors.Errorf("cannot find global task with ID %d", task.ID)
	}
	var taskMeta TaskMeta
	if err := json.Unmarshal(globalTask.Meta, &taskMeta); err != nil {
		return nil, err
	}
	return &taskMeta, nil
}
