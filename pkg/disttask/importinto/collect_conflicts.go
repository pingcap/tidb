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
	"unsafe"

	"github.com/docker/go-units"
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

const (
	// we use this size as a hint to the map size for the conflict rows which is
	// from index KV conflict and ingested to downstream.
	// as conflict row might generate more than one conflict KV, and its data KV
	// might not be ingested to downstream, so we choose a small value for its init
	// size.
	initMapSizeForConflictedRows = 128
	handleMapEntryShallowSize    = int64(unsafe.Sizeof("") + unsafe.Sizeof(true))
)

// MaxConflictRowFileSize is the maximum size of the conflict row file.
// exported for testing.
var MaxConflictRowFileSize int64 = 8 * units.GiB

type collectConflictsStepExecutor struct {
	taskexecutor.EmptyStepExecutor
	taskID   int64
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	tableImporter *importer.TableImporter

	// per subtask fields
	currSubtaskID        int64
	conflictedRowCount   int64
	conflictedRowSize    int64
	conflictRowsChecksum *verification.KVChecksum
	handledRowsFromIndex map[string]bool
	// if there are too many conflict rows from index, we must stop caching them
	// in memory, and we must skip the checksum for the whole task.
	// but we still need keep running this step to record all conflicted rows to
	// let user resolve them manually.
	tooManyConflictsFromIndex        bool
	conflictHandleFromIndexSize      int64
	conflictHandleFromIndexSizeLimit int64
	conflictRowFileSeq               int
	conflictRowFilenames             []string
	conflictRowWriter                storage.ExternalFileWriter
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

	var potentialConflictRowsDueToIndex int
	for kvGroup, ci := range stepMeta.Infos.ConflictInfos {
		if kvGroup != dataKVGroup {
			potentialConflictRowsDueToIndex += int(ci.Count)
		}
	}
	potentialConflictRowsDueToIndex = min(potentialConflictRowsDueToIndex, initMapSizeForConflictedRows)
	e.resetForNewSubtask(subtask.ID, potentialConflictRowsDueToIndex)
	if err = e.switchConflictRowFile(ctx); err != nil {
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
		zap.Strings("targetFiles", e.conflictRowFilenames),
		zap.String("fileSize", units.BytesSize(float64(e.conflictedRowSize))),
		zap.Bool("tooManyConflictsFromIndex", e.tooManyConflictsFromIndex),
	)
	subtaskMeta.Checksum = newFromKVChecksum(e.conflictRowsChecksum)
	subtaskMeta.ConflictedRowCount = e.conflictedRowCount
	subtaskMeta.ConflictedRowFilenames = e.conflictRowFilenames
	subtaskMeta.TooManyConflictsFromIndex = e.tooManyConflictsFromIndex
	newMeta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

// right now we only have 1 subtask, but later we might have multiple subtasks
// to run it distributively.
func (e *collectConflictsStepExecutor) resetForNewSubtask(subtaskID int64, potentialConflictRowsDueToIndex int) {
	e.currSubtaskID = subtaskID
	e.conflictedRowCount = 0
	e.conflictedRowSize = 0
	e.conflictRowsChecksum = verification.NewKVChecksumWithKeyspace(e.store.GetCodec().GetKeyspace())
	e.handledRowsFromIndex = make(map[string]bool, potentialConflictRowsDueToIndex)
	e.tooManyConflictsFromIndex = false
	e.conflictHandleFromIndexSize = 0
	// we use half of the subtask memory to cache the conflict row handle from index.
	e.conflictHandleFromIndexSizeLimit = e.GetResource().Mem.Capacity() / 2
	e.conflictRowFileSeq = 0
	e.conflictRowFilenames = make([]string, 0, 1)
	e.conflictRowWriter = nil
}

func (e *collectConflictsStepExecutor) switchConflictRowFile(ctx context.Context) error {
	if e.conflictRowWriter != nil {
		if err := e.conflictRowWriter.Close(ctx); err != nil {
			e.logger.Warn("failed to close conflict row writer", zap.Error(err))
			return errors.Trace(err)
		}
		e.conflictRowWriter = nil
	}
	e.conflictRowFileSeq++
	filename := getConflictRowFileName(e.taskID, e.currSubtaskID, e.conflictRowFileSeq)
	e.logger.Info("switch conflict row file", zap.String("filename", filename))
	writer, err := e.tableImporter.GlobalSortStore.Create(ctx, filename, &storage.WriterOption{
		Concurrency: 20,
		PartSize:    external.MinUploadPartSize,
	})
	if err != nil {
		return errors.Trace(err)
	}
	e.conflictRowFilenames = append(e.conflictRowFilenames, filename)
	e.conflictRowWriter = writer
	return nil
}

func (e *collectConflictsStepExecutor) isRowHandledFn(handle tidbkv.Handle) bool {
	if e.tooManyConflictsFromIndex {
		return false
	}
	return e.handledRowsFromIndex[handle.String()]
}

func (e *collectConflictsStepExecutor) tryCacheHandledRowFromIndex(handle tidbkv.Handle) {
	if e.tooManyConflictsFromIndex {
		return
	}

	hdlStr := handle.String()
	e.conflictHandleFromIndexSize += int64(len(hdlStr)) + handleMapEntryShallowSize
	limit := e.conflictHandleFromIndexSizeLimit
	failpoint.InjectCall("tryCacheHandledRowFromIndex", &limit)
	if e.conflictHandleFromIndexSize >= limit {
		e.logger.Info("too many conflict rows from index, skip cache",
			zap.String("handleSize", units.BytesSize(float64(e.conflictHandleFromIndexSize))))
		e.tooManyConflictsFromIndex = true
		e.handledRowsFromIndex = make(map[string]bool)
		return
	}

	e.handledRowsFromIndex[hdlStr] = true
}

func (e *collectConflictsStepExecutor) recordConflictRow(ctx context.Context, kvGroup string,
	handle tidbkv.Handle, row []types.Datum, kvPairs *kv.Pairs) error {
	if e.conflictedRowSize >= MaxConflictRowFileSize {
		e.logger.Info("conflict row file size exceeds the limit, switch to a new file",
			zap.String("currSize", units.BytesSize(float64(e.conflictedRowSize))))
		if err := e.switchConflictRowFile(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	// every conflicted row from data KV group must be recorded, but for index KV
	// group, they might come from the same row, so we only need to record it on
	// the first time we meet it.
	// currently, we use memory to do this check, if it's too large, we just skip
	// the cache and skip later checksum.
	// TODO we can upload those handles to sort storage and check them in another
	//  pass later.
	if kvGroup != dataKVGroup {
		e.tryCacheHandledRowFromIndex(handle)
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

// getConflictRowFileName returns the file name to store the conflict rows for the
// given task and subtask.
// user can check this file to resolve the conflict rows manually.
func getConflictRowFileName(taskID, subtaskID int64, seq int) string {
	// we need to keep this file for the user to check the conflict rows, so we
	// don't put it under '<task-id>/' directory to avoid it being deleted by the
	// cleanup process.
	return fmt.Sprintf("conflict-rows-%d-%d/data-%d.txt", taskID, subtaskID, seq)
}
