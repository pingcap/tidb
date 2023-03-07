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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type tableImporter struct {
	backend         backend.Backend
	cp              *checkpoints.TableCheckpoint
	tableInfo       *checkpoints.TidbTableInfo
	tableMeta       *mydump.MDTableMeta
	dataImporter    DataImporter
	logger          *zap.Logger
	regionSplitSize int64
	regionSplitKeys int64
}

func (ti *tableImporter) importTable(ctx context.Context) error {
	if err := ti.importEngines(ctx); err != nil {
		return nil
	}
	// todo: post process
	return nil
}

func (ti *tableImporter) importEngines(ctx context.Context) error {
	idxEngineCfg := &backend.EngineConfig{
		TableInfo: ti.tableInfo,
	}
	idxCnt := len(ti.tableInfo.Core.Indices)
	if !common.TableHasAutoRowID(ti.tableInfo.Core) {
		idxCnt--
	}
	threshold := local.EstimateCompactionThreshold(ti.tableMeta.DataFiles, ti.cp, int64(idxCnt))
	idxEngineCfg.Local = backend.LocalEngineConfig{
		Compact:            threshold > 0,
		CompactConcurrency: 4,
		CompactThreshold:   threshold,
	}
	fullTableName := ti.tableMeta.FullTableName()
	// todo: cleanup all engine data on any error since we don't support checkpoint for now
	// some return path, didn't make sure all data engine and index engine are cleaned up.
	// maybe we can add this in upper level to clean the whole local-sort directory
	indexEngine, err := ti.backend.OpenEngine(ctx, idxEngineCfg, fullTableName, common.IndexEngineID)
	if err != nil {
		return errors.Trace(err)
	}

	for id, engineCP := range ti.cp.Engines {
		dataClosedEngine, err2 := ti.preprocessEngine(ctx, indexEngine, id, engineCP.Chunks)
		if err2 != nil {
			return err2
		}
		if err2 = ti.importAndCleanup(ctx, dataClosedEngine); err2 != nil {
			return err2
		}
	}

	closedIndexEngine, err := indexEngine.Close(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return ti.importAndCleanup(ctx, closedIndexEngine)
}

func (ti *tableImporter) preprocessEngine(ctx context.Context, indexEngine *backend.OpenedEngine, engineID int32, chunks []*checkpoints.ChunkCheckpoint) (*backend.ClosedEngine, error) {
	// if the key are ordered, LocalWrite can optimize the writing.
	// table has auto-incremented _tidb_rowid must satisfy following restrictions:
	// - clustered index disable and primary key is not number
	// - no auto random bits (auto random or shard row id)
	// - no partition table
	// - no explicit _tidb_rowid field (At this time we can't determine if the source file contains _tidb_rowid field,
	//   so we will do this check in LocalWriter when the first row is received.)
	hasAutoIncrementAutoID := common.TableHasAutoRowID(ti.tableInfo.Core) &&
		ti.tableInfo.Core.AutoRandomBits == 0 && ti.tableInfo.Core.ShardRowIDBits == 0 &&
		ti.tableInfo.Core.Partition == nil

	engineImporter := dataEngineProcessor{
		engineID:     engineID,
		tableName:    ti.tableMeta.FullTableName(),
		backend:      ti.backend,
		tableInfo:    ti.tableInfo,
		logger:       ti.logger.With(zap.Int32("engine-id", engineID)),
		dataImporter: ti.dataImporter,
		kvSorted:     hasAutoIncrementAutoID,
		rowOrdered:   ti.tableMeta.IsRowOrdered,
		indexEngine:  indexEngine,
		chunks:       chunks,
	}
	return engineImporter.process(ctx)
}

func (ti *tableImporter) importAndCleanup(ctx context.Context, closedEngine *backend.ClosedEngine) error {
	importErr := closedEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys)
	// todo: if we need support checkpoint, engine should not be cleanup if import failed.
	cleanupErr := closedEngine.Cleanup(ctx)
	return multierr.Combine(importErr, cleanupErr)
}
