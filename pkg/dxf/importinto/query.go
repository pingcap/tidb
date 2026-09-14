// Copyright 2026 PingCAP, Inc.
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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/operator"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

// queryStepExecutor shares importer resources and KV writers with file imports,
// but owns SQL execution and never opens local engines or partitions source files.
type queryStepExecutor struct {
	importStepExecutor
	queryRuntime sqlsvrapi.Runtime
}

func (s *queryStepExecutor) Init(ctx context.Context) error {
	if s.taskMeta.Plan.Query == nil || !s.taskMeta.Plan.IsGlobalSort() {
		return errors.New("query step requires a query plan and global sort storage")
	}
	return s.importStepExecutor.Init(ctx)
}

func (s *queryStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	defer func() { err = normalizeSubtaskErr(err) }()
	logger := s.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run query subtask")
	var dataKVFiles, indexKVFiles atomic.Int64
	accessRec, objStore, err := handle.NewObjStoreWithRecording(ctx, s.tableImporter.CloudStorageURI)
	if err != nil {
		return err
	}
	defer func() {
		objStore.Close()
		s.summary.MergeObjStoreRequests(&accessRec.Requests)
		s.GetMeterRecorder().MergeObjStoreAccess(accessRec)
		task.End2(zapcore.ErrorLevel, err,
			zap.Int64("data-kv-files", dataKVFiles.Load()),
			zap.Int64("index-kv-files", indexKVFiles.Load()),
			zap.Stringer("obj-store-access", accessRec))
	}()
	var meta ImportStepMeta
	if err := json.Unmarshal(subtask.Meta, &meta); err != nil {
		return errors.Trace(err)
	}
	shared := &SharedVars{
		TableImporter:    s.tableImporter,
		Checksum:         verification.NewKVGroupChecksumWithKeyspace(s.tableImporter.GetKeySpace()),
		SortedDataMeta:   &globalsort.SortedKVMeta{},
		SortedIndexMetas: make(map[int64]*globalsort.SortedKVMeta),
		globalSortStore:  objStore,
		dataKVFileCount:  &dataKVFiles,
		indexKVFileCount: &indexKVFiles,
	}
	s.sharedVars.Store(meta.ID, shared)
	defer s.sharedVars.Delete(meta.ID)
	return s.runQueryPipeline(ctx, subtask, objStore, shared, logger)
}

func (s *queryStepExecutor) runQueryPipeline(
	ctx context.Context, subtask *proto.Subtask, objStore storeapi.Storage,
	shared *SharedVars, logger *zap.Logger,
) error {
	if importer.RunImportQuery == nil || s.queryRuntime == nil {
		return errors.New("import query runtime is not registered")
	}
	query := s.taskMeta.Plan.Query
	selected := make(chan importer.QueryChunk, 1)
	s.tableImporter.SetSelectedChunkCh(selected)
	concurrency := max(1, min(s.taskMeta.Plan.ThreadCnt, s.concurrency))
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(selected)
		pool := s.queryRuntime.SysSessionPool()
		resource, err := pool.Get()
		if err != nil {
			return err
		}
		// Query execution mutates session state. Destroy the session after the
		// attempt instead of restoring every variable and returning it to the pool.
		defer pool.Destroy(resource)
		se := resource.(sessionctx.Context)
		return importer.RunImportQuery(groupCtx, query, importer.QueryRuntime{
			TotalMemoryLimit: s.GetResource().Mem.Capacity() / 2,
			Session:          se, Storage: objStore, Prefix: subtaskPrefix(s.taskID, subtask.ID),
			MemoryLimit: s.GetResource().Mem.Capacity() / 4,
		}, selected)
	})
	group.Go(func() error {
		wctx := workerpool.NewContext(groupCtx)
		tasks := make([]*importStepMinimalTask, 1)
		for i := range tasks {
			tasks[i] = &importStepMinimalTask{
				Plan: s.taskMeta.Plan, Chunk: importer.Chunk{Timestamp: query.Timestamp}, SharedVars: shared, logger: logger,
			}
		}
		source := operator.NewSimpleDataSource(wctx, tasks)
		op := newEncodeAndSortOperator(wctx, &s.importStepExecutor, shared, s, subtask.ID, concurrency)
		operator.Compose(source, op)
		pipe := operator.NewAsyncPipeline(source, op)
		if err := pipe.Execute(); err != nil {
			_ = pipe.Close()
			return err
		}
		err := pipe.Close()
		if opErr := wctx.OperatorErr(); opErr != nil {
			return opErr
		}
		return err
	})
	if err := group.Wait(); err != nil {
		return err
	}
	return s.onFinished(ctx, subtask, objStore)
}
