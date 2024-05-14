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

	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"go.uber.org/zap"
)

// ProcessChunk processes a chunk, and write kv pairs to dataEngine and indexEngine.
func ProcessChunk(
	ctx context.Context,
	chunk *checkpoints.ChunkCheckpoint,
	tableImporter *TableImporter,
	dataEngine, indexEngine *backend.OpenedEngine,
	progress *Progress,
	logger *zap.Logger,
	groupChecksum *verification.KVGroupChecksum,
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

	return ProcessChunkWithWriter(ctx, chunk, tableImporter, dataWriter, indexWriter, progress, logger, groupChecksum)
}

// ProcessChunkWithWriter processes a chunk, and write kv pairs to dataWriter and indexWriter.
func ProcessChunkWithWriter(
	ctx context.Context,
	chunk *checkpoints.ChunkCheckpoint,
	tableImporter *TableImporter,
	dataWriter, indexWriter backend.EngineWriter,
	progress *Progress,
	logger *zap.Logger,
	groupChecksum *verification.KVGroupChecksum,
) error {
	encoder, err := tableImporter.getKVEncoder(chunk)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := encoder.Close(); err2 != nil {
			logger.Warn("close encoder failed", zap.Error(err2))
		}
	}()

	// TODO: right now we use this chunk processor for global sort too, will
	// impl another one for it later.
	var cp ChunkProcessor
	switch tableImporter.DataSourceType {
	case DataSourceTypeFile:
		parser, err := tableImporter.getParser(ctx, chunk)
		if err != nil {
			return err
		}
		defer func() {
			if err2 := parser.Close(); err2 != nil {
				logger.Warn("close parser failed", zap.Error(err2))
			}
		}()
		cp = NewFileChunkProcessor(
			parser, encoder, tableImporter.GetKeySpace(), chunk, logger,
			tableImporter.diskQuotaLock, dataWriter, indexWriter, groupChecksum,
		)
	case DataSourceTypeQuery:
		cp = newQueryChunkProcessor(
			tableImporter.rowCh, encoder, tableImporter.GetKeySpace(), logger,
			tableImporter.diskQuotaLock, dataWriter, indexWriter, groupChecksum,
		)
	}
	err = cp.Process(ctx)
	if err != nil {
		return err
	}
	progress.AddColSize(encoder.GetColumnSize())
	return nil
}
