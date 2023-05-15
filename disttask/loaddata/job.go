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
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	checkTaskFinishInterval = 300 * time.Millisecond
)

// DistImporter is a JobImporter for distributed load data.
type DistImporter struct {
	*importer.JobImportParam
	plan *importer.Plan
	stmt string
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
	logutil.BgLogger().Info("start distribute load data", zap.Int64("jobID", ti.Job.ID))
	ti.Group.Go(func() error {
		defer close(ti.Done)
		return ti.doImport(ti.GroupCtx)
	})
}

// Result implements JobImporter.Result.
func (*DistImporter) Result() importer.JobImportResult {
	return importer.JobImportResult{}
}

// Close implements the io.Closer interface.
func (*DistImporter) Close() error {
	return nil
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
	taskType := proto.LoadData
	taskKey := fmt.Sprintf("ddl/%s/%d", taskType, ti.Job.ID)

	return submitGlobalTaskAndRun(ctx, taskKey, taskType, int(ti.plan.ThreadCnt), taskMeta)
}
