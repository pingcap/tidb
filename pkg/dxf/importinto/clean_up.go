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
	goerrors "errors"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/tablemode"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var _ scheduler.CleanUpRoutine = (*ImportCleanUp)(nil)

// ImportCleanUp implements scheduler.CleanUpRoutine.
type ImportCleanUp struct {
}

func newImportCleanUpS3() scheduler.CleanUpRoutine {
	return &ImportCleanUp{}
}

// CleanUp implements the CleanUpRoutine.CleanUp interface.
func (*ImportCleanUp) CleanUp(ctx context.Context, task *proto.Task) error {
	// we can only clean up files after all write&ingest subtasks are finished,
	// since they might share the same file.
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(task.Meta, taskMeta)
	if err != nil {
		return err
	}
	defer redactSensitiveInfo(task, taskMeta)

	logger := logutil.BgLogger().With(zap.Int64("task-id", task.ID), zap.String("task-key", task.Key))
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	plan := &taskMeta.Plan
	// we reset the mode on task Done, this reset here works as a fallback.
	// and since cleanup is an async process, the db/table might already be
	// dropped/truncated, we can ignore the error.
	// TODO: we should avoid calling reset when the reset on task Done is success,
	// else it's possible to have below race due to async cleanup:
	// 1. task Done, reset to normal mode
	// 2. table rows are deleted and empty, table ID remains the same, imported
	//    again and switch to Import mode
	// 3. cleanup starts, reset to the table of same ID to normal mode, but we
	//    shouldn't do it actually.
	if err = tablemode.ResetWithTaskRuntime(ctx, taskMgr, task.Keyspace, task.ID, plan); err != nil {
		if !goerrors.Is(err, infoschema.ErrDatabaseNotExists) &&
			!goerrors.Is(err, infoschema.ErrTableNotExists) {
			return err
		}
		logger.Info("db/table not found during import cleanup, skip altering table mode",
			zap.String("db-name", plan.DBName),
			zap.String("table-name", plan.TableInfo.Name.O),
			zap.Int64("db-id", plan.DBID),
			zap.Int64("tableID", plan.TableInfo.ID),
			zap.Error(err),
		)
	}

	failpoint.InjectCall("mockCleanupError", &err)
	if err != nil {
		return err
	}

	// Not use cloud storage, no need to cleanUp.
	if taskMeta.Plan.CloudStorageURI == "" {
		return nil
	}
	callLog := log.BeginTask(logger, "cleanup global sorted data")
	defer callLog.End(zap.InfoLevel, nil)

	store, err := importer.GetSortStore(ctx, taskMeta.Plan.CloudStorageURI)
	if err != nil {
		logger.Warn("failed to create store", zap.Error(err))
		return err
	}
	defer store.Close()
	if err = globalsort.CleanUpFiles(ctx, store, strconv.Itoa(int(task.ID))); err != nil {
		logger.Warn("failed to clean up files of task", zap.Error(err))
		return err
	}
	// send metering data for nextgen kernel, only for succeed tasks
	if kerneltype.IsNextGen() && task.State == proto.TaskStateSucceed {
		if err = sendMeterOnCleanUp(ctx, task, logger); err != nil {
			logger.Warn("failed to send metering data on cleanup", zap.Error(err))
			return err
		}
	}
	return nil
}

func sendMeterOnCleanUp(ctx context.Context, task *proto.Task, logger *zap.Logger) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	subtasks, err := taskManager.GetAllSubtasksByStepAndState(ctx, task.ID, proto.ImportStepPostProcess, proto.SubtaskStateSucceed)
	if err != nil {
		return err
	}
	if len(subtasks) != 1 {
		// should not happen, checksum is required in nextgen
		return nil
	}
	stMeta := &PostProcessStepMeta{}
	subtask := subtasks[0]
	if err = json.Unmarshal(subtask.Meta, stMeta); err != nil {
		return errors.Trace(err)
	}
	var rowCount, dataKVSize, indexKVSize uint64
	for group, ckSum := range stMeta.Checksum {
		if group == verification.DataKVGroupID {
			rowCount = ckSum.KVs
			dataKVSize = ckSum.Size
		} else {
			indexKVSize += ckSum.Size
		}
	}
	return handle.SendRowAndSizeMeterData(ctx, task, int64(rowCount), int64(dataKVSize), int64(indexKVSize), logger)
}

func init() {
	scheduler.RegisterSchedulerCleanUpFactory(proto.ImportInto, newImportCleanUpS3)
}
