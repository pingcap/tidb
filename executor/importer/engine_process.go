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

package importer

import (
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// encode and write all data in this engine as sst files, and write index in indexEngine
// todo: would be better if merge with backend/local/Engine
type engine struct {
	importTableCancel context.CancelFunc

	chunks     []*checkpoints.ChunkCheckpoint
	logger     *zap.Logger
	dataEngine *backend.OpenedEngine
	// the group used to encode and sort the engine data.
	// we use a separate group, so we can know what error happened in the sort phase for this engine.
	// this group might be cancelled by other engine's sort or ingest.
	sortEG    *errgroup.Group
	sortEGCtx context.Context
	sortTask  *log.Task
}

func (en *engine) asyncSort(importer *TableImporter, pool *worker.Pool, indexEngine *backend.OpenedEngine) {
	for _, chunkCP := range en.chunks {
		w := pool.Apply()
		en.sortEG.Go(func() error {
			defer pool.Recycle(w)
			if err := ProcessChunk(en.sortEGCtx, chunkCP, importer, en.dataEngine, indexEngine, en.logger); err != nil {
				importer.firstErr.Set(err)
				// there might be multiple engine sorting at the same time, we need cancel other engines and
				// ingest routine too.
				en.importTableCancel()
				return err
			}
			return nil
		})
	}
}

func (en *engine) ingestAndCleanup(ctx context.Context, importer *TableImporter) error {
	err := en.sortEG.Wait()
	en.sortTask.End(zap.ErrorLevel, err)
	// ctx maybe canceled, so to avoid Close engine failed, we use `context.Background()` here
	// todo: remove ctx param in Close()
	closeCtx := ctx
	if common.IsContextCanceledError(err) {
		closeCtx = context.Background()
	}
	// close the engine even when sort failed
	closedDataEngine, err2 := en.dataEngine.Close(closeCtx)
	if err2 != nil {
		en.logger.Warn("close data engine failed", zap.Error(err2))
	}
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}
	if err2 = importer.IngestAndCleanup(ctx, closedDataEngine); err2 != nil {
		return err2
	}

	failpoint.Inject("AfterIngestDataEngine", nil)
	failpoint.Inject("SyncAfterIngestDataEngine", func() {
		TestSyncCh <- struct{}{}
		<-TestSyncCh
	})
	return nil
}

// ProcessChunk processes a chunk, and write kv pairs to dataEngine and indexEngine.
func ProcessChunk(
	ctx context.Context,
	chunk *checkpoints.ChunkCheckpoint,
	tableImporter *TableImporter,
	dataEngine,
	indexEngine *backend.OpenedEngine,
	logger *zap.Logger,
) error {
	// if the key are ordered, LocalWrite can optimize the writing.
	// table has auto-incremented _tidb_rowid must satisfy following restrictions:
	// - clustered index disable and primary key is not number
	// - no auto random bits (auto random or shard row id)
	// - no partition table
	// - no explicit _tidb_rowid field (At this time we can't determine if the source file contains _tidb_rowid field,
	//   so we will do this check in LocalWriter when the first row is received.)
	hasAutoIncrementAutoID := common.TableHasAutoRowID(tableImporter.tableInfo.Core) &&
		tableImporter.tableInfo.Core.AutoRandomBits == 0 && tableImporter.tableInfo.Core.ShardRowIDBits == 0 &&
		tableImporter.tableInfo.Core.Partition == nil
	dataWriterCfg := &backend.LocalWriterConfig{
		IsKVSorted: hasAutoIncrementAutoID,
	}
	parser, err := tableImporter.getParser(ctx, chunk)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := parser.Close(); err2 != nil {
			logger.Warn("close parser failed", zap.Error(err2))
		}
	}()
	encoder, err := tableImporter.getKVEncoder(chunk)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := encoder.Close(); err2 != nil {
			logger.Warn("close encoder failed", zap.Error(err2))
		}
	}()
	dataWriter, err := dataEngine.LocalWriter(ctx, dataWriterCfg)
	if err != nil {
		return err
	}
	defer func() {
		if _, err2 := dataWriter.Close(ctx); err2 != nil {
			logger.Warn("close data writer failed", zap.Error(err2))
		}
	}()
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	if err != nil {
		return err
	}
	defer func() {
		if _, err2 := indexWriter.Close(ctx); err2 != nil {
			logger.Warn("close index writer failed", zap.Error(err2))
		}
	}()

	cp := &chunkProcessor{
		parser:      parser,
		chunkInfo:   chunk,
		logger:      logger.With(zap.String("key", chunk.GetKey())),
		kvsCh:       make(chan []deliveredRow, maxKVQueueSize),
		dataWriter:  dataWriter,
		indexWriter: indexWriter,
		encoder:     encoder,
		kvCodec:     tableImporter.kvStore.GetCodec(),
		progress:    tableImporter.Progress,
	}
	// todo: process in parallel
	err = cp.process(ctx)
	if err != nil {
		return err
	}
	tableImporter.setLastInsertID(encoder.GetLastInsertID())
	return nil
}
