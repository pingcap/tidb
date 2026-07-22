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
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	_ scheduler.CleanUpRoutine      = (*ImportCleanUp)(nil)
	_ scheduler.BatchCleanUpRoutine = (*ImportCleanUp)(nil)
)

// ImportCleanUp implements scheduler.BatchCleanUpRoutine.
type ImportCleanUp struct {
}

func newImportCleanUpS3() scheduler.CleanUpRoutine {
	return &ImportCleanUp{}
}

// CleanUp implements the CleanUpRoutine.CleanUp interface.
func (c *ImportCleanUp) CleanUp(ctx context.Context, task *proto.Task) error {
	return c.CleanUpBatch(ctx, []*proto.Task{task})
}

type cleanUpFileGroup struct {
	cloudStorageURI    string
	nonPartitionedDirs []string
	taskIDs            []int64
}

// CleanUpBatch implements scheduler.BatchCleanUpRoutine.
// Global-sort files are partitioned by task ID, but finding them requires a scan
// of the shared object store. Batching lets cleanup scan each store once instead
// of once per task. The scheduler moves the tasks to history only after this
// method succeeds, but the cleanup side effects themselves are not atomic: a
// retry may repeat work that completed before an earlier failure.
//
// TODO: Move global-sort file management into DXF so task cleanup can remain
// independent without giving up batched object-store scans.
func (*ImportCleanUp) CleanUpBatch(ctx context.Context, tasks []*proto.Task) error {
	if len(tasks) == 0 {
		return nil
	}

	// we can only clean up files after all write&ingest subtasks are finished,
	// since they might share the same file.
	meterTasks := make([]*proto.Task, 0, len(tasks))
	fileGroups := make(map[string]*cleanUpFileGroup, len(tasks))
	for _, task := range tasks {
		taskMeta := &TaskMeta{}
		err := json.Unmarshal(task.Meta, taskMeta)
		if err != nil {
			return err
		}
		cloudStorageURI := taskMeta.Plan.CloudStorageURI
		isGlobalSort := taskMeta.Plan.IsGlobalSort()
		redactSensitiveInfo(task, taskMeta)

		if err = cleanUpTableMode(ctx, taskMeta); err != nil {
			return err
		}
		failpoint.InjectCall("mockCleanupError", &err)
		if err != nil {
			return err
		}

		if isGlobalSort {
			// in next-gen, and most cases of classic kernel, all tasks share the
			// same cloud storage uri.
			fileGroup, ok := fileGroups[cloudStorageURI]
			if !ok {
				fileGroup = &cleanUpFileGroup{
					cloudStorageURI: cloudStorageURI,
				}
				fileGroups[cloudStorageURI] = fileGroup
			}
			fileGroup.nonPartitionedDirs = append(fileGroup.nonPartitionedDirs, strconv.Itoa(int(task.ID)))
			fileGroup.taskIDs = append(fileGroup.taskIDs, task.ID)
			if kerneltype.IsNextGen() && task.State == proto.TaskStateSucceed {
				meterTasks = append(meterTasks, task)
			}
		}
	}

	for _, fileGroup := range fileGroups {
		if err := cleanUpExternalFiles(ctx, *fileGroup); err != nil {
			return err
		}
	}

	for _, task := range meterTasks {
		logger := logutil.BgLogger().With(zap.Int64("task-id", task.ID))
		if err := sendMeterOnCleanUp(ctx, task, logger); err != nil {
			logger.Warn("failed to send metering data on cleanup", zap.Error(err))
			return err
		}
	}
	return nil
}

func cleanUpTableMode(ctx context.Context, taskMeta *TaskMeta) error {
	if !kerneltype.IsClassic() {
		return nil
	}
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	if err = taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		return ddl.AlterTableMode(domain.GetDomain(se).DDLExecutor(), se, model.TableModeNormal, taskMeta.Plan.DBID, taskMeta.Plan.TableInfo.ID)
	}); err != nil {
		// If the table is not found, it means the table has been either
		// dropped or truncated. In such cases, the table mode has already
		// been reset to normal, so we can ignore this error.
		if !goerrors.Is(err, infoschema.ErrTableNotExists) {
			return err
		}

		logutil.BgLogger().Warn(
			"table not found during import cleanup, skip altering table mode",
			zap.Int64("tableID", taskMeta.Plan.TableInfo.ID),
		)
	}
	return nil
}

func cleanUpExternalFiles(ctx context.Context, fileGroup cleanUpFileGroup) error {
	logger := logutil.BgLogger().With(zap.Int64s("task-ids", fileGroup.taskIDs))
	callLog := log.BeginTask(logger, "cleanup global sorted data")
	defer callLog.End(zap.InfoLevel, nil)

	store, err := importer.GetSortStore(ctx, fileGroup.cloudStorageURI)
	if err != nil {
		logger.Warn("failed to create store", zap.Error(err))
		return err
	}
	defer store.Close()
	if err = external.CleanUpFiles(ctx, store, fileGroup.nonPartitionedDirs...); err != nil {
		logger.Warn("failed to clean up files of tasks", zap.Error(err))
		return err
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
