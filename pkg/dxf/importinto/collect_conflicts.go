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
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
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
	sizeOfHandlesFromIndex      atomic.Int64
	sizeLimitOfHandlesFromIndex int64
	result                      *conflictedkv.CollectResult
	// one conflicted row might generate multiple conflicted UK KV, this set is
	// used to avoid collecting checksum for this row multiple times.
	// such as for `create table t(id int primary key, c1 int, c2 int, unique u1(c1), unique u2(c2))`
	// if we have 2 rows (1, 3, 4), (2, 3, 4), one pair of conflicted UK KV will
	// be generated for kv group u1 and u2 respectively.
	// this also means to need to process conflicted UK KV group one by one.
	sharedHandleSet *conflictedkv.BoundedHandleSet
	summary         execute.SubtaskSummary
}

var _ execute.StepExecutor = &collectConflictsStepExecutor{}

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
		zap.Bool("skipSaveHandle", e.sharedHandleSet.BoundExceeded()),
	)
	subtaskMeta.Checksum = newFromKVChecksum(e.result.Checksum)
	subtaskMeta.ConflictedRowCount = e.result.RowCount
	subtaskMeta.ConflictedRowFilenames = e.result.Filenames
	subtaskMeta.TooManyConflictsFromIndex = e.sharedHandleSet.BoundExceeded()
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

	pairCh := external.ReadKVFilesAsync(egCtx, eg, objStore, ci.Files)

	encoders, err := createEncoders(concurrency, e.tableImporter)
	if err != nil {
		return err
	}

	var (
		mu             sync.Mutex
		mergedLocalSet = conflictedkv.NewBoundedHandleSet(e.logger, &e.sizeOfHandlesFromIndex, e.sizeLimitOfHandlesFromIndex)
	)
	for i := range concurrency {
		encoder := encoders[i]
		uid := uuid.New().String()
		filenamePrefix := getConflictRowFilenamePrefix(e.task.ID, e.currSubtaskID, uid)
		localSet := conflictedkv.NewBoundedHandleSet(e.logger, &e.sizeOfHandlesFromIndex, e.sizeLimitOfHandlesFromIndex)
		collector := conflictedkv.NewCollector(e.tableImporter.Table, e.logger, objStore, e.store, filenamePrefix, kvGroup, encoder, e.sharedHandleSet, localSet)
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
			return collector.Run(egCtx, pairCh)
		})
	}

	if err = eg.Wait(); err != nil {
		return err
	}

	e.sharedHandleSet.Merge(mergedLocalSet)
	return nil
}

// right now we only have 1 subtask, but later we might have multiple subtasks
// to run it distributively.
func (e *collectConflictsStepExecutor) resetForNewSubtask(subtaskID int64) {
	e.currSubtaskID = subtaskID
	e.sizeOfHandlesFromIndex.Store(0)
	// we use half of the subtask memory to cache the conflict row handle from index.
	e.sizeLimitOfHandlesFromIndex = e.GetResource().Mem.Capacity() / 2
	e.result = conflictedkv.NewCollectResult(e.store.GetCodec().GetKeyspace())
	e.sharedHandleSet = conflictedkv.NewBoundedHandleSet(e.logger, &e.sizeOfHandlesFromIndex, e.sizeLimitOfHandlesFromIndex)
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

// getConflictRowFilenamePrefix returns the file name prefix to store the conflict
// rows for the given task and subtask.
func getConflictRowFilenamePrefix(taskID, subtaskID int64, uuid string) string {
	// we need to keep this file for the user to check the conflict rows, so we
	// don't put it under '<task-id>/' directory to avoid it being deleted by the
	// cleanup process.
	return path.Join("conflicted-rows", fmt.Sprintf("%d", taskID), fmt.Sprintf("%d-%s", subtaskID, uuid))
}
