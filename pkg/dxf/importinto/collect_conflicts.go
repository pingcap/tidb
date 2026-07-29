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
	"hash/crc32"
	"path"
	"sync"
	"sync/atomic"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	dxfhandle "github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/importinto/conflictedkv"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/tablecodec"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type collectConflictsStepExecutor struct {
	taskexecutor.BaseStepExecutor
	task     *proto.TaskBase
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	tableImporter *importer.TableImporter

	// per subtask fields
	currSubtaskID               int64
	sizeOfRowKeysFromIndex      atomic.Int64
	sizeLimitOfRowKeysFromIndex int64
	sizeOfConflictRowFiles      atomic.Int64
	result                      *conflictedkv.CollectResult
	// one conflicted row might generate multiple conflicted UK KV, this set is
	// used to avoid collecting checksum for this row multiple times.
	// such as for `create table t(id int primary key, c1 int, c2 int, unique u1(c1), unique u2(c2))`
	// if we have 2 rows (1, 3, 4), (2, 3, 4), one pair of conflicted UK KV will
	// be generated for kv group u1 and u2 respectively.
	// this also means we need to process conflicted UK KV group one by one.
	sharedRowKeySet *conflictedkv.BoundedKeySet
	summary         execute.SubtaskSummary
}

var _ execute.StepExecutor = &collectConflictsStepExecutor{}
var _ execute.Collector = &collectConflictsStepExecutor{}

// NewCollectConflictsStepExecutor creates a new collectConflictsStepExecutor.
// exported for test.
func NewCollectConflictsStepExecutor(
	task *proto.TaskBase,
	store tidbkv.Storage,
	taskMeta *TaskMeta,
	logger *zap.Logger,
) execute.StepExecutor {
	return &collectConflictsStepExecutor{
		task:     task,
		store:    store,
		taskMeta: taskMeta,
		logger:   logger,
	}
}

func (e *collectConflictsStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.task.ID, e.taskMeta, e.store, e.logger)
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
	accessRec, objStore, err := dxfhandle.NewObjStoreWithRecording(ctx, e.taskMeta.Plan.CloudStorageURI)
	if err != nil {
		return err
	}
	defer func() {
		objStore.Close()
		e.summary.MergeObjStoreRequests(&accessRec.Requests)
		e.GetMeterRecorder().MergeObjStoreAccess(accessRec)
	}()

	stMeta := &CollectConflictsStepMeta{}
	if err = json.Unmarshal(subtask.Meta, stMeta); err != nil {
		return errors.Trace(err)
	}
	if stMeta.ExternalPath != "" {
		if err := stMeta.ReadJSONFromExternalStorage(ctx, objStore, stMeta); err != nil {
			return errors.Trace(err)
		}
	}

	e.resetForNewSubtask(subtask.ID)

	for kvGroup, ci := range stMeta.Infos.ConflictInfos {
		err := e.collectConflictsOfKVGroup(ctx, objStore, int(e.GetResource().CPU.Capacity()), kvGroup, ci)
		failpoint.InjectCall("afterCollectOneKVGroup", &err)
		if err != nil {
			return err
		}
	}
	return e.onFinished(ctx, subtask, stMeta)
}

func (e *collectConflictsStepExecutor) onFinished(_ context.Context, subtask *proto.Subtask, subtaskMeta *CollectConflictsStepMeta) error {
	e.logger.Info("collected conflict row info", zap.Int64("count", e.result.RowCount),
		zap.Stringer("checksum", e.result.Checksum),
		zap.Strings("targetFiles", e.result.Filenames),
		zap.String("fileSize", units.BytesSize(float64(e.result.TotalFileSize))),
		zap.Bool("rowKeySetLimitExceeded", e.sharedRowKeySet.BoundExceeded()),
	)
	subtaskMeta.Checksum = newFromKVChecksum(e.result.Checksum)
	subtaskMeta.ConflictedRowCount = e.result.RowCount
	subtaskMeta.ConflictedRowFilenames = e.result.Filenames
	subtaskMeta.ConflictedRowRecordingCapped = e.result.RowRecordingCapped
	subtaskMeta.TooManyConflictsFromIndex = e.sharedRowKeySet.BoundExceeded()
	newMeta, err := subtaskMeta.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

func (e *collectConflictsStepExecutor) collectConflictsOfKVGroup(
	ctx context.Context,
	objStore storeapi.Storage,
	concurrency int,
	kvGroup string,
	ci *engineapi.ConflictInfo,
) (err error) {
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

	targetIdx, err := getKVGroupIndexInfo(e.tableImporter, kvGroup)
	if err != nil {
		return err
	}
	encoders, err := createEncoders(concurrency, e.tableImporter)
	if err != nil {
		return err
	}

	pairCh := globalsort.ReadKVFilesAsync(egCtx, eg, objStore, ci.Files)
	collectorChs, needDispatch := createConflictHandlerChannels(pairCh, concurrency, targetIdx)

	var (
		mu             sync.Mutex
		mergedLocalSet = conflictedkv.NewBoundedKeySet(e.logger, &e.sizeOfRowKeysFromIndex, e.sizeLimitOfRowKeysFromIndex)
	)
	for i := range concurrency {
		collectorCh := collectorChs[i]
		encoder := encoders[i]
		uid := uuid.New().String()
		filenamePrefix := getConflictRowFilenamePrefix(e.task.ID, e.currSubtaskID, uid)
		localSet := conflictedkv.NewBoundedKeySet(e.logger, &e.sizeOfRowKeysFromIndex, e.sizeLimitOfRowKeysFromIndex)
		collector := conflictedkv.NewCollector(
			e.tableImporter.Table,
			e.logger,
			objStore,
			e.store,
			filenamePrefix,
			kvGroup,
			encoder,
			e.sharedRowKeySet,
			localSet,
			&e.sizeOfConflictRowFiles,
			e,
			e.GetMeterRecorder(),
		)
		eg.Go(func() (err error) {
			defer func() {
				err2 := collector.Close(egCtx)
				if err == nil {
					err = err2
				}
				mu.Lock()
				mergedLocalSet.Merge(localSet)
				e.result.Merge(collector.GetCollectResult())
				mu.Unlock()
			}()
			return collector.Run(egCtx, collectorCh)
		})
	}
	if needDispatch {
		eg.Go(func() error {
			return dispatchMVIndexKVPairs(egCtx, e.store, pairCh, collectorChs, targetIdx)
		})
	}

	if err = eg.Wait(); err != nil {
		return err
	}

	e.sharedRowKeySet.Merge(mergedLocalSet)
	return nil
}

func getKVGroupIndexInfo(tableImporter *importer.TableImporter, kvGroup string) (*model.IndexInfo, error) {
	if kvGroup == globalsort.DataKVGroup {
		return nil, nil
	}

	indexID, err := globalsort.KVGroup2IndexID(kvGroup)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tblMeta := tableImporter.Table.Meta()
	targetIdx := model.FindIndexInfoByID(tblMeta.Indices, indexID)
	if targetIdx == nil {
		// should not happen
		return nil, errors.Errorf("index %d from KV group %q not found in table %s", indexID, kvGroup, tblMeta.Name)
	}
	return targetIdx, nil
}

func createConflictHandlerChannels(
	pairCh chan *simplesst.KVPair,
	concurrency int,
	targetIdx *model.IndexInfo,
) ([]chan *simplesst.KVPair, bool) {
	handlerChs := make([]chan *simplesst.KVPair, concurrency)
	// there might be multiple UK KV for MV index for a single row, when they
	// are handled concurrently, we want to make sure UK KVs for some row route
	// to the same handler to properly handle them.
	needDispatch := concurrency > 1 && targetIdx != nil && targetIdx.MVIndex
	for i := range handlerChs {
		handlerChs[i] = pairCh
		if needDispatch {
			// A handler processes BufferedHandleLimit index handles in one batch.
			// Buffer one batch so a busy handler does not block dispatch to the others.
			handlerChs[i] = make(chan *simplesst.KVPair, conflictedkv.BufferedHandleLimit)
		}
	}
	return handlerChs, needDispatch
}

func dispatchMVIndexKVPairs(
	ctx context.Context,
	store tidbkv.Storage,
	pairCh <-chan *simplesst.KVPair,
	handlerChs []chan *simplesst.KVPair,
	targetIdx *model.IndexInfo,
) error {
	defer func() {
		for _, handlerCh := range handlerChs {
			close(handlerCh)
		}
	}()

	for {
		var pair *simplesst.KVPair
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p, ok := <-pairCh:
			if !ok {
				return nil
			}
			pair = p
		}

		key, err := store.GetCodec().DecodeKey(pair.Key)
		if err != nil {
			return errors.Trace(err)
		}
		handle, err := tablecodec.DecodeIndexHandle(key, pair.Value, len(targetIdx.Columns))
		if err != nil {
			return errors.Trace(err)
		}
		// Keep all index KVs for one row in the same handler.
		handlerIdx := int(crc32.ChecksumIEEE(handle.Encoded()) % uint32(len(handlerChs)))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case handlerChs[handlerIdx] <- pair:
		}
	}
}

// right now we only have 1 subtask, but later we might have multiple subtasks
// to run it distributively.
func (e *collectConflictsStepExecutor) resetForNewSubtask(subtaskID int64) {
	e.currSubtaskID = subtaskID
	e.sizeOfRowKeysFromIndex.Store(0)
	e.sizeOfConflictRowFiles.Store(0)
	// we use half of the subtask memory to cache conflict row keys from indexes.
	e.sizeLimitOfRowKeysFromIndex = e.GetResource().Mem.Capacity() / 2
	e.result = conflictedkv.NewCollectResult(e.store.GetCodec().GetKeyspace())
	e.sharedRowKeySet = conflictedkv.NewBoundedKeySet(e.logger, &e.sizeOfRowKeysFromIndex, e.sizeLimitOfRowKeysFromIndex)
}

func (e *collectConflictsStepExecutor) Cleanup(_ context.Context) (err error) {
	e.logger.Info("cleanup subtask env")
	return e.tableImporter.Close()
}

func (e *collectConflictsStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	e.summary.Update()
	return &e.summary
}

func (e *collectConflictsStepExecutor) ResetSummary() {
	e.summary.Reset()
}

// Accepted implements Collector.Accepted interface.
func (*collectConflictsStepExecutor) Accepted(_ int64) {}

// Processed implements Collector.Processed interface.
func (e *collectConflictsStepExecutor) Processed(processedConflictKVs, _ int64) {
	e.summary.Processed.Add(processedConflictKVs)
}

// getConflictRowFilenamePrefix returns the file name prefix to store the conflict
// rows for the given task and subtask.
func getConflictRowFilenamePrefix(taskID, subtaskID int64, uuid string) string {
	// we need to keep this file for the user to check the conflict rows, so we
	// don't put it under '<task-id>/' directory to avoid it being deleted by the
	// cleanup process.
	return path.Join("conflicted-rows", fmt.Sprintf("%d", taskID), fmt.Sprintf("%d-%s", subtaskID, uuid))
}
