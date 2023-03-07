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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"go.uber.org/zap"
)

type DataImporter interface {
	GetParser() mydump.Parser
	GetKVEncoder() KVEncoder
}

type dataImporterImpl struct {
	cp        *checkpoints.TableCheckpoint
	backend   backend.Backend
	cfg       *config.Config
	store     storage.ExternalStorage
	dbInfo    *checkpoints.TidbDBInfo
	tableInfo *checkpoints.TidbTableInfo
	tableMeta *mydump.MDTableMeta
	logger    *zap.Logger
	alloc     autoid.Allocators
	kvStore   tidbkv.Storage
}

func (di *dataImporterImpl) GetParser() mydump.Parser {
	//TODO implement me
	panic("implement me")
}

func (di *dataImporterImpl) GetKVEncoder() KVEncoder {
	//TODO implement me
	panic("implement me")
}

func (di *dataImporterImpl) doImport(ctx context.Context) error {
	importer := &tableImporter{
		backend:         di.backend,
		cp:              di.cp,
		tableInfo:       di.tableInfo,
		tableMeta:       di.tableMeta,
		dataImporter:    di,
		logger:          di.logger,
		regionSplitSize: int64(di.cfg.TikvImporter.RegionSplitSize),
		regionSplitKeys: int64(di.cfg.TikvImporter.RegionSplitKeys),
	}
	return importer.importTable(ctx)
}

// in dist framework, this should be done in the tidb node which is responsible for splitting job into subtasks
// then table-importer handles data belongs to the subtask.
func (di *dataImporterImpl) populateChunks(ctx context.Context) error {
	di.logger.Info("populate chunks")
	tableRegions, err := mydump.MakeTableRegions(ctx, di.tableMeta, len(di.tableInfo.Core.Columns), di.cfg, nil, di.store)

	if err != nil {
		di.logger.Error("populate chunks failed", zap.Error(err))
		return err
	}

	var maxRowId int64
	timestamp := time.Now().Unix()
	for _, region := range tableRegions {
		engine, found := di.cp.Engines[region.EngineID]
		if !found {
			engine = &checkpoints.EngineCheckpoint{
				Status: checkpoints.CheckpointStatusLoaded,
			}
			di.cp.Engines[region.EngineID] = engine
		}
		ccp := &checkpoints.ChunkCheckpoint{
			Key: checkpoints.ChunkCheckpointKey{
				Path:   region.FileMeta.Path,
				Offset: region.Chunk.Offset,
			},
			FileMeta:          region.FileMeta,
			ColumnPermutation: nil,
			Chunk:             region.Chunk,
			Timestamp:         timestamp,
		}
		engine.Chunks = append(engine.Chunks, ccp)
		if region.Chunk.RowIDMax > maxRowId {
			maxRowId = region.Chunk.RowIDMax
		}
	}

	if common.TableHasAutoID(di.tableInfo.Core) {
		if err = common.RebaseGlobalAutoID(ctx, 0, di.kvStore, di.dbInfo.ID, di.tableInfo.Core); err != nil {
			return errors.Trace(err)
		}
		newMinRowId, _, err := common.AllocGlobalAutoID(ctx, maxRowId, di.kvStore, di.dbInfo.ID, di.tableInfo.Core)
		if err != nil {
			return errors.Trace(err)
		}
		di.rebaseChunkRowID(newMinRowId)
	}

	// Add index engine checkpoint
	di.cp.Engines[common.IndexEngineID] = &checkpoints.EngineCheckpoint{Status: checkpoints.CheckpointStatusLoaded}
	return nil
}

func (di *dataImporterImpl) rebaseChunkRowID(rowIDBase int64) {
	if rowIDBase == 0 {
		return
	}
	for _, engine := range di.cp.Engines {
		for _, chunk := range engine.Chunks {
			chunk.Chunk.PrevRowIDMax += rowIDBase
			chunk.Chunk.RowIDMax += rowIDBase
		}
	}
}
