// Copyright 2025 PingCAP, Inc.
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
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type collectConflictsStepExecutor struct {
	taskexecutor.EmptyStepExecutor
	taskID   int64
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	tableImporter *importer.TableImporter

	// per subtask fields
	conflictedRowCount   int64
	conflictedRowSize    int64
	conflictRowsChecksum *verification.KVChecksum
	handledRowsFromIndex map[string]bool
	conflictRowFilename  string
	conflictRowWriter    storage.ExternalFileWriter
}

var _ execute.StepExecutor = &collectConflictsStepExecutor{}

func (e *collectConflictsStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta, e.store)
	if err != nil {
		return err
	}
	e.tableImporter = tableImporter
	return nil
}

func (e *collectConflictsStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()
	stepMeta := &CollectConflictsStepMeta{}
	if err = json.Unmarshal(subtask.Meta, stepMeta); err != nil {
		return errors.Trace(err)
	}
	if stepMeta.ExternalPath != "" {
		if err := stepMeta.ReadJSONFromExternalStorage(ctx, e.tableImporter.GlobalSortStore, stepMeta); err != nil {
			return errors.Trace(err)
		}
	}
	// TODO if there are too many conflicts due to index, we need to skip this
	// step to avoid OOM.
	var potentialConflictRowsDueToIndex int
	for kvGroup, ci := range stepMeta.Infos.ConflictInfos {
		if kvGroup != dataKVGroup {
			potentialConflictRowsDueToIndex += int(ci.Count)
		}
	}
	// TODO consider the memory usage of the conflict rows.
	potentialConflictRowsDueToIndex = min(potentialConflictRowsDueToIndex, 128)
	e.conflictedRowCount, e.conflictedRowSize = 0, 0
	e.conflictRowsChecksum = verification.NewKVChecksumWithKeyspace(e.store.GetCodec().GetKeyspace())
	e.handledRowsFromIndex = make(map[string]bool, potentialConflictRowsDueToIndex)
	e.conflictRowFilename = conflictRowFileName(e.taskID, subtask.ID)
	e.conflictRowWriter, err = e.tableImporter.GlobalSortStore.Create(ctx, e.conflictRowFilename, &storage.WriterOption{
		// TODO max 50GiB can be stored for now.
		Concurrency: 20,
		PartSize:    external.MinUploadPartSize,
	})
	if err != nil {
		return errors.Trace(err)
	}
	for kvGroup, ci := range stepMeta.Infos.ConflictInfos {
		baseHandler := &baseConflictKVHandler{
			tableImporter:       e.tableImporter,
			store:               e.store,
			logger:              e.logger,
			kvGroup:             kvGroup,
			handleConflictRowFn: e.recordConflictRow,
		}
		var handler conflictKVHandler = &conflictDataKVHandler{baseConflictKVHandler: baseHandler}
		if kvGroup != dataKVGroup {
			handler = &conflictIndexKVHandler{
				baseConflictKVHandler: baseHandler,
				isRowHandledFn:        e.isRowHandledFn,
			}
		}
		err = handleKVGroupConflicts(ctx, e.logger, handler, e.tableImporter.GlobalSortStore, kvGroup, ci)
		failpoint.InjectCall("afterCollectOneKVGroup", &err)
		if err != nil {
			_ = e.conflictRowWriter.Close(ctx)
			return err
		}
	}
	return e.conflictRowWriter.Close(ctx)
}

func (e *collectConflictsStepExecutor) OnFinished(_ context.Context, subtask *proto.Subtask) error {
	subtaskMeta := &CollectConflictsStepMeta{}
	if err := json.Unmarshal(subtask.Meta, subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	e.logger.Info("collected conflict row info", zap.Int64("count", e.conflictedRowCount),
		zap.Stringer("checksum", e.conflictRowsChecksum),
		zap.String("targetFile", e.conflictRowFilename), zap.Int64("fileSize", e.conflictedRowSize))
	subtaskMeta.Checksum = newFromKVChecksum(e.conflictRowsChecksum)
	subtaskMeta.ConflictedRowCount = e.conflictedRowCount
	subtaskMeta.ConflictedRowFilename = e.conflictRowFilename
	newMeta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

func (e *collectConflictsStepExecutor) isRowHandledFn(handle tidbkv.Handle) bool {
	// TODO this memory check limit how many rows can be conflicted by index solely.
	//  might OOM if there are too many conflicts.
	return e.handledRowsFromIndex[handle.String()]
}
func (e *collectConflictsStepExecutor) recordConflictRow(ctx context.Context, kvGroup string, handle tidbkv.Handle, row []types.Datum, kvPairs *kv.Pairs) error {
	// every conflicted row from data KV group must be recorded, but for index KV
	// group, we only need to record the row if it's not handled before.
	if kvGroup != dataKVGroup {
		e.handledRowsFromIndex[handle.String()] = true
	}
	e.conflictRowsChecksum.Update(kvPairs.Pairs)
	e.conflictedRowCount++

	str, err := types.DatumsToString(row, true)
	if err != nil {
		return errors.Trace(err)
	}
	content := []byte(str + "\n")
	e.conflictedRowSize += int64(len(content))
	_, err = e.conflictRowWriter.Write(ctx, content)
	return errors.Trace(err)
}

func (e *collectConflictsStepExecutor) Cleanup(_ context.Context) (err error) {
	e.logger.Info("cleanup subtask env")
	return e.tableImporter.Close()
}

// conflictRowFileName returns the file name to store the conflict rows for the
// given task and subtask.
// user can check this file to resolve the conflict rows manually.
func conflictRowFileName(taskID, subtaskID int64) string {
	// we need to keep this file for the user to check the conflict rows, so we
	// don't put it under '<task-id>/' directory to avoid it being deleted by the
	// cleanup process.
	return filepath.Join(fmt.Sprintf("conflict-rows-%d-%d", taskID, subtaskID), "data.txt")
}
