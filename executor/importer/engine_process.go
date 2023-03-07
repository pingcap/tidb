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

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"go.uber.org/zap"
)

// encode and write all data in this engine as sst files, and write index in indexEngine
type dataEngineProcessor struct {
	engineID     int32
	tableName    string
	backend      backend.Backend
	tableInfo    *checkpoints.TidbTableInfo
	logger       *zap.Logger
	dataImporter DataImporter
	kvSorted     bool
	rowOrdered   bool
	indexEngine  *backend.OpenedEngine
	chunks       []*checkpoints.ChunkCheckpoint
}

func (ei *dataEngineProcessor) process(ctx context.Context) (*backend.ClosedEngine, error) {
	ei.logger.Info("encode kv data and write start")
	dataEngineCfg := &backend.EngineConfig{
		TableInfo: ei.tableInfo,
	}
	if !ei.rowOrdered {
		dataEngineCfg.Local.Compact = true
		dataEngineCfg.Local.CompactConcurrency = 4
		dataEngineCfg.Local.CompactThreshold = local.CompactionUpperThreshold
	}
	dataEngine, err := ei.backend.OpenEngine(ctx, dataEngineCfg, ei.tableName, ei.engineID)
	if err != nil {
		return nil, err
	}

	err = ei.localSort(ctx, dataEngine)
	// ctx maybe canceled, so to avoid Close engine failed, we use `context.Background()` here
	// todo: remove ctx param in Close()
	closeCtx := ctx
	if common.IsContextCanceledError(err) {
		closeCtx = context.Background()
	}
	closedDataEngine, err2 := dataEngine.Close(closeCtx)
	if err2 != nil {
		ei.logger.Warn("flush all chunk checkpoints failed before manually exits", zap.Error(err2))
	}
	return closedDataEngine, err
}

// sort data in all chunks, then close the opened data engine
func (ei *dataEngineProcessor) localSort(ctx context.Context, dataEngine *backend.OpenedEngine) error {
	dataWriterCfg := &backend.LocalWriterConfig{
		IsKVSorted: ei.kvSorted,
	}
	for _, chunk := range ei.chunks {
		// todo: on panic which will be recovered since we run in tidb, we need to make sure all opened fd is closed.
		dataWriter, err := dataEngine.LocalWriter(ctx, dataWriterCfg)
		if err != nil {
			return err
		}
		indexWriter, err := ei.indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
		if err != nil {
			_, _ = dataWriter.Close(ctx)
			return err
		}

		cp := &chunkProcessor{
			parser:      ei.dataImporter.GetParser(),
			chunkInfo:   chunk,
			logger:      ei.logger.With(zap.String("key", chunk.GetKey())),
			kvsCh:       make(chan []deliveredKVs, maxKVQueueSize),
			dataWriter:  dataWriter,
			indexWriter: indexWriter,
			encoder:     ei.dataImporter.GetKVEncoder(),
		}
		// todo: process in parallel
		err = cp.process(ctx)
		// chunk process is responsible to close data/index writer
		cp.close(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
