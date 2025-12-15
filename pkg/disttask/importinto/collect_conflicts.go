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
	"path"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/google/uuid"
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
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/maps"
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
	taskexecutor.BaseStepExecutor
	taskID   int64
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	tableImporter *importer.TableImporter

	// per subtask fields
	currSubtaskID                    int64
	conflictHandleFromIndexSize      atomic.Int64
	conflictHandleFromIndexSizeLimit int64
	result                           *collectConflictResult
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

	e.resetForNewSubtask(subtask.ID)

	for kvGroup, ci := range stepMeta.Infos.ConflictInfos {
		result, err := e.collectConflictsOfKVGroup(ctx, subtask.Concurrency, kvGroup, ci)
		failpoint.InjectCall("afterCollectOneKVGroup", &err)
		if err != nil {
			return err
		}
		e.result.merge(result)
	}
	return e.onFinished(ctx, subtask)
}

func (e *collectConflictsStepExecutor) onFinished(_ context.Context, subtask *proto.Subtask) error {
	subtaskMeta := &CollectConflictsStepMeta{}
	if err := json.Unmarshal(subtask.Meta, subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	e.logger.Info("collected conflict row info", zap.Int64("count", e.result.count),
		zap.Stringer("checksum", e.result.checksum),
		zap.Strings("targetFiles", e.result.filenames),
		zap.String("fileSize", units.BytesSize(float64(e.result.size))),
		zap.Bool("skipSaveHandle", e.result.skipSaveHandle),
	)
	subtaskMeta.Checksum = newFromKVChecksum(e.result.checksum)
	subtaskMeta.ConflictedRowCount = e.result.count
	subtaskMeta.ConflictedRowFilenames = e.result.filenames
	subtaskMeta.TooManyConflictsFromIndex = e.result.skipSaveHandle
	newMeta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

func (e *collectConflictsStepExecutor) collectConflictsOfKVGroup(
	ctx context.Context,
	concurrency int,
	kvGroup string,
	ci *common.ConflictInfo,
) (result *collectConflictResult, err error) {
	failpoint.Inject("forceHandleConflictsBySingleThread", func() {
		concurrency = 1
	})
	task := log.BeginTask(e.logger.With(
		zap.String("kvGroup", kvGroup), zap.Uint64("duplicates", ci.Count),
		zap.Int("file-count", len(ci.Files)), zap.Int("concurrency", concurrency),
	), "collect conflicts of kv group")

	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	eg, egCtx := tidbutil.NewErrorGroupWithRecoverWithCtx(ctx)

	pairCh := startReadFiles(egCtx, eg, e.tableImporter.GlobalSortStore, ci.Files)

	handlers, err := e.buildHandlers(egCtx, concurrency, kvGroup)
	if err != nil {
		return nil, err
	}
	result = newCollectConflictResultForMerge()
	var mu sync.Mutex
	for _, h := range handlers {
		handler := h
		eg.Go(func() error {
			if err := handler.run(egCtx, pairCh); err != nil {
				_ = handler.close(egCtx)
				return err
			}
			if err := handler.close(egCtx); err != nil {
				return err
			}
			res := handler.getCollectResult()
			mu.Lock()
			result.merge(res)
			mu.Unlock()
			return nil
		})
	}
	return result, eg.Wait()
}

func (e *collectConflictsStepExecutor) buildHandlers(ctx context.Context, concurrency int, kvGroup string) (handlers []conflictKVHandler, err error) {
	handlers = make([]conflictKVHandler, 0, concurrency)
	defer func() {
		if err != nil {
			for _, hdl := range handlers {
				_ = hdl.close(ctx)
			}
		}
	}()
	for range concurrency {
		handler := e.getHandler(kvGroup)
		// when create encoder, if the table have generated column, when calling
		// backend/kv.CollectGeneratedColumns(), buildSimpleExpr will rewrite the
		// AST node, and data race. and the data race might happen during encoding,
		// in EvalGeneratedColumns, so we have to finish initialize all handlers
		// before running them.
		if err = handler.init(); err != nil {
			return nil, err
		}
		handlers = append(handlers, handler)
	}
	return handlers, nil
}

func (e *collectConflictsStepExecutor) getHandler(kvGroup string) conflictKVHandler {
	prefix := uuid.New().String()
	collector := &conflictRowCollector{
		logger:                e.logger,
		store:                 e.tableImporter.GlobalSortStore,
		filenamePrefix:        getConflictRowFilenamePrefix(e.taskID, e.currSubtaskID, prefix),
		collectConflictResult: newCollectConflictResult(e.store.GetCodec().GetKeyspace(), e.result.skipSaveHandle),
		savedHandleSize:       &e.conflictHandleFromIndexSize,
		savedHandleSizeLimit:  e.conflictHandleFromIndexSizeLimit,
	}
	baseHandler := &baseConflictKVHandler{
		tableImporter: e.tableImporter,
		store:         e.store,
		logger:        e.logger,
		kvGroup:       kvGroup,
		collector:     collector,
	}
	dataKVHandler := &conflictDataKVHandler{baseConflictKVHandler: baseHandler}
	baseHandler.handleFn = dataKVHandler.handle
	var handler conflictKVHandler = dataKVHandler
	if kvGroup != dataKVGroup {
		indexKVHandler := &conflictIndexKVHandler{
			baseConflictKVHandler: baseHandler,
			isRowHandledFn:        e.isRowFromIndexHandled,
		}
		baseHandler.handleFn = indexKVHandler.handle
		handler = indexKVHandler
	}
	return handler
}

// right now we only have 1 subtask, but later we might have multiple subtasks
// to run it distributively.
func (e *collectConflictsStepExecutor) resetForNewSubtask(subtaskID int64) {
	e.currSubtaskID = subtaskID
	e.conflictHandleFromIndexSize.Store(0)
	// we use half of the subtask memory to cache the conflict row handle from index.
	e.conflictHandleFromIndexSizeLimit = e.GetResource().Mem.Capacity() / 2
	e.result = newCollectConflictResult(e.store.GetCodec().GetKeyspace(), false)
}

func (e *collectConflictsStepExecutor) isRowFromIndexHandled(handle tidbkv.Handle) bool {
	// during handling the first kv group, this map is empty
	if len(e.result.savedHandles) == 0 {
		return false
	}
	return e.result.savedHandles[handle.String()]
}

func (e *collectConflictsStepExecutor) Cleanup(_ context.Context) (err error) {
	e.logger.Info("cleanup subtask env")
	return e.tableImporter.Close()
}

type collectConflictResult struct {
	count    int64
	size     int64
	checksum *verification.KVChecksum
	// if there are too many conflict rows from index, we must stop saving them
	// in memory, and we must skip the checksum for the whole task.
	// but we still need keep running this step to record all conflicted rows to
	// let user resolve them manually.
	skipSaveHandle bool
	savedHandles   map[string]bool
	filenames      []string
}

func newCollectConflictResult(keyspace []byte, skipSaveHandle bool) *collectConflictResult {
	size := initMapSizeForConflictedRows
	if skipSaveHandle {
		size = 0
	}
	return &collectConflictResult{
		checksum:       verification.NewKVChecksumWithKeyspace(keyspace),
		skipSaveHandle: skipSaveHandle,
		savedHandles:   make(map[string]bool, size),
		filenames:      make([]string, 0, 1),
	}
}

func newCollectConflictResultForMerge() *collectConflictResult {
	return &collectConflictResult{
		checksum:     verification.NewKVChecksum(),
		savedHandles: make(map[string]bool),
	}
}

func (r *collectConflictResult) merge(other *collectConflictResult) {
	if other == nil {
		return
	}
	r.count += other.count
	r.size += other.size
	r.checksum.Add(other.checksum)
	r.skipSaveHandle = r.skipSaveHandle || other.skipSaveHandle
	// we still merge the saved handles even when skipSaveHandle=true, so we can
	// avoid handling conflicted rows twice as much as possible.
	if len(r.savedHandles) == 0 {
		r.savedHandles = other.savedHandles
	} else {
		maps.Copy(r.savedHandles, other.savedHandles)
	}
	r.filenames = append(r.filenames, other.filenames...)
}

type conflictRowCollector struct {
	logger         *zap.Logger
	store          storage.ExternalStorage
	filenamePrefix string

	*collectConflictResult
	// we use a shared size, as we collect conflicted rows concurrently
	savedHandleSize      *atomic.Int64
	savedHandleSizeLimit int64

	fileSeq      int
	currFileSize int64
	writer       storage.ExternalFileWriter
}

func (c *conflictRowCollector) recordConflictRow(ctx context.Context, kvGroup string,
	handle tidbkv.Handle, row []types.Datum, kvPairs *kv.Pairs) error {
	if c.writer == nil || c.currFileSize >= MaxConflictRowFileSize {
		if err := c.switchConflictRowFile(ctx); err != nil {
			return errors.Trace(err)
		}
		c.currFileSize = 0
	}
	// every conflicted row from data KV group must be recorded, but for index KV
	// group, they might come from the same row, so we only need to record it on
	// the first time we meet it.
	// currently, we use memory to do this check, if it's too large, we just skip
	// the checking and skip later checksum.
	// TODO we can upload those handles to sort storage and check them in another
	//  pass later.
	if kvGroup != dataKVGroup {
		c.trySaveHandledRowFromIndex(handle)
	}

	str, err := types.DatumsToString(row, true)
	if err != nil {
		return errors.Trace(err)
	}
	content := []byte(str + "\n")
	if _, err = c.writer.Write(ctx, content); err != nil {
		return errors.Trace(err)
	}

	c.count++
	c.size += int64(len(content))
	c.checksum.Update(kvPairs.Pairs)
	c.currFileSize += int64(len(content))
	return nil
}

func (c *conflictRowCollector) switchConflictRowFile(ctx context.Context) error {
	if c.writer != nil {
		if err := c.writer.Close(ctx); err != nil {
			c.logger.Warn("failed to close conflict row writer", zap.Error(err))
			return errors.Trace(err)
		}
		c.writer = nil
	}
	c.fileSeq++
	filename := getConflictRowFileName(c.filenamePrefix, c.fileSeq)
	c.logger.Info("switch conflict row file", zap.String("filename", filename),
		zap.String("lastFileSize", units.BytesSize(float64(c.currFileSize))))
	writer, err := c.store.Create(ctx, filename, &storage.WriterOption{
		Concurrency: 20,
		PartSize:    external.MinUploadPartSize,
	})
	if err != nil {
		return errors.Trace(err)
	}
	c.filenames = append(c.filenames, filename)
	c.writer = writer
	return nil
}

func (c *conflictRowCollector) trySaveHandledRowFromIndex(handle tidbkv.Handle) {
	if c.skipSaveHandle {
		return
	}

	hdlStr := handle.String()
	c.savedHandleSize.Add(int64(len(hdlStr)) + handleMapEntryShallowSize)
	limit := c.savedHandleSizeLimit
	failpoint.InjectCall("trySaveHandledRowFromIndex", &limit)
	if c.savedHandleSize.Load() >= limit {
		c.logger.Info("too many conflict rows from index, skip checking",
			zap.String("handleSize", units.BytesSize(float64(c.savedHandleSize.Load()))),
			zap.Int("handleCount", len(c.savedHandles)))
		c.skipSaveHandle = true
		// Note: we still keep the savedHandles, to make it merged into the same
		// field in collectConflictsStepExecutor, so we can avoid handling other
		// KV groups of same handle as much as possible.
		return
	}

	c.savedHandles[hdlStr] = true
}

func (c *conflictRowCollector) close(ctx context.Context) error {
	if c.writer != nil {
		return c.writer.Close(ctx)
	}
	return nil
}

// getConflictRowFilenamePrefix returns the file name prefix to store the conflict
// rows for the given task and subtask.
func getConflictRowFilenamePrefix(taskID, subtaskID int64, uuid string) string {
	// we need to keep this file for the user to check the conflict rows, so we
	// don't put it under '<task-id>/' directory to avoid it being deleted by the
	// cleanup process.
	return path.Join("conflicted-rows", fmt.Sprintf("%d", taskID), fmt.Sprintf("%d-%s", subtaskID, uuid))
}

// getConflictRowFileName returns the file name to store the conflict rows.
// user can check this file to resolve the conflict rows manually.
func getConflictRowFileName(prefix string, seq int) string {
	return path.Join(prefix, fmt.Sprintf("data-%d.txt", seq))
}
