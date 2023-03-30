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
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidb "github.com/pingcap/tidb/config"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/filter"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

func (e *LoadDataController) toDataFiles() []mydump.FileInfo {
	tbl := filter.Table{
		Schema: e.DBName,
		Name:   e.Table.Meta().Name.O,
	}
	res := []mydump.FileInfo{}
	for _, f := range e.dataFiles {
		res = append(res, mydump.FileInfo{
			TableName: tbl,
			FileMeta:  *f,
		})
	}
	return res
}

func (e *LoadDataController) prepareSortDir() (string, error) {
	tidbCfg := tidb.GetGlobalConfig()
	// todo: add job id too
	sortPathSuffix := "import-" + strconv.Itoa(int(tidbCfg.Port))
	sortPath := filepath.Join(tidbCfg.TempDir, sortPathSuffix)

	if info, err := os.Stat(sortPath); err != nil {
		if !os.IsNotExist(err) {
			e.logger.Error("stat sort dir failed", zap.String("path", sortPath), zap.Error(err))
			return "", errors.Trace(err)
		}
	} else if info.IsDir() {
		// Currently remove all dir to clean garbage data.
		// TODO: when do checkpoint should change follow logic.
		err := os.RemoveAll(sortPath)
		if err != nil {
			e.logger.Error("remove sort dir failed", zap.String("path", sortPath), zap.Error(err))
		}
	}

	err := os.MkdirAll(sortPath, 0o700)
	if err != nil {
		e.logger.Error("failed to make sort dir", zap.String("path", sortPath), zap.Error(err))
		return "", errors.Trace(err)
	}
	e.logger.Info("sort dir prepared", zap.String("path", sortPath))
	return sortPath, nil
}

func (e *LoadDataController) import0(ctx context.Context) error {
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, e.Table.Meta())
	if err != nil {
		return errors.Annotatef(err, "failed to tables.TableFromMeta %s", e.Table.Meta().Name)
	}

	tidbCfg := tidb.GetGlobalConfig()
	dir, err := e.prepareSortDir()
	if err != nil {
		return err
	}

	hostPort := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort)))
	tls, err := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		hostPort,
		nil, nil, nil,
	)
	if err != nil {
		return err
	}

	// Disable GC because TiDB enables GC already.
	keySpaceName := tidb.GetGlobalKeyspaceName()
	kvStore, err := GetKVStore(fmt.Sprintf("tikv://%s?disableGC=true&keyspaceName=%s", tidbCfg.Path, keySpaceName), tls.ToTiKVSecurityConfig())
	if err != nil {
		return errors.Trace(err)
	}

	backendConfig := local.BackendConfig{
		PDAddr:                  tidbCfg.Path,
		LocalStoreDir:           dir,
		MaxConnPerStore:         config.DefaultRangeConcurrency,
		ConnCompressType:        config.CompressionNone,
		WorkerConcurrency:       config.DefaultRangeConcurrency * 2,
		KVWriteBatchSize:        config.KVWriteBatchSize,
		CheckpointEnabled:       false,
		MemTableSize:            int(config.DefaultEngineMemCacheSize),
		LocalWriterMemCacheSize: int64(config.DefaultLocalWriterMemCacheSize),
		ShouldCheckTiKV:         true,
		DupeDetectEnabled:       false,
		DuplicateDetectOpt:      local.DupDetectOpt{ReportErrOnDup: false},
		StoreWriteBWLimit:       0,
		ShouldCheckWriteStall:   false,
		MaxOpenFiles:            int(util.GenRLimit()),
		KeyspaceName:            keySpaceName,
	}

	tableMeta := &mydump.MDTableMeta{
		DB:        e.DBName,
		Name:      e.Table.Meta().Name.O,
		DataFiles: e.toDataFiles(),
	}
	dataDivideCfg := &mydump.DataDivideConfig{
		ColumnCnt:         len(e.Table.Meta().Columns),
		EngineDataSize:    int64(config.DefaultBatchSize),
		MaxChunkSize:      int64(config.MaxRegionSize),
		Concurrency:       int(e.threadCnt),
		EngineConcurrency: config.DefaultTableConcurrency,
		BatchImportRatio:  config.DefaultBatchImportRatio,
		IOWorkers:         nil,
		Store:             e.dataStore,
		TableMeta:         tableMeta,
	}

	// todo: use a real region size getter
	regionSizeGetter := &local.TableRegionSizeGetterImpl{}
	localBackend, err := local.NewLocalBackend(ctx, tls, backendConfig, regionSizeGetter)
	if err != nil {
		return err
	}
	tblImporter := &tableImporterImpl{
		LoadDataController: e,
		backend:            localBackend,
		tableCp: &checkpoints.TableCheckpoint{
			Engines: map[int32]*checkpoints.EngineCheckpoint{},
		},
		tableInfo: &checkpoints.TidbTableInfo{
			ID:   e.Table.Meta().ID,
			Name: e.Table.Meta().Name.O,
			Core: e.Table.Meta(),
		},
		tableMeta:       tableMeta,
		encTable:        tbl,
		dbID:            e.DBID,
		dataDivideCfg:   dataDivideCfg,
		store:           e.dataStore,
		kvStore:         kvStore,
		logger:          e.logger,
		regionSplitSize: int64(config.SplitRegionSize),
		regionSplitKeys: int64(config.SplitRegionKeys),
	}
	return tblImporter.importTable(ctx)
}

type TableImporter interface {
	GetParser(ctx context.Context, chunk *checkpoints.ChunkCheckpoint) (mydump.Parser, error)
	GetKVEncoder(chunk *checkpoints.ChunkCheckpoint) (KVEncoder, error)
}

var _ TableImporter = &tableImporterImpl{}

type tableImporterImpl struct {
	*LoadDataController
	backend   backend.Backend
	tableCp   *checkpoints.TableCheckpoint
	tableInfo *checkpoints.TidbTableInfo
	tableMeta *mydump.MDTableMeta
	// this table has a separate id allocator used to record the max row id allocated.
	encTable      table.Table
	dbID          int64
	dataDivideCfg *mydump.DataDivideConfig

	store           storage.ExternalStorage
	kvStore         tidbkv.Storage
	cfg             *config.Config
	logger          *zap.Logger
	regionSplitSize int64
	regionSplitKeys int64
}

func (d *tableImporterImpl) GetParser(ctx context.Context, chunk *checkpoints.ChunkCheckpoint) (mydump.Parser, error) {
	info := LoadDataReaderInfo{
		Opener: func(ctx context.Context) (io.ReadSeekCloser, error) {
			reader, err := mydump.OpenReader(ctx, chunk.FileMeta, d.dataStore)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return reader, nil
		},
		Remote: &chunk.FileMeta,
	}
	return d.LoadDataController.GetParser(ctx, info)
}

func (d *tableImporterImpl) GetKVEncoder(chunk *checkpoints.ChunkCheckpoint) (KVEncoder, error) {
	cfg := &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{
			SQLMode:        d.sqlMode,
			Timestamp:      chunk.Timestamp,
			SysVars:        d.importantSysVars,
			AutoRandomSeed: chunk.Chunk.PrevRowIDMax,
		},
		Path:   chunk.FileMeta.Path,
		Table:  d.encTable,
		Logger: log.Logger{Logger: d.logger.With(zap.String("path", chunk.FileMeta.Path))},
	}
	return newTableKVEncoder(cfg, d.ColumnAssignments, d.ColumnsAndUserVars, d.FieldMappings, d.InsertColumns)
}

func (ti *tableImporterImpl) importTable(ctx context.Context) error {
	if err := ti.populateChunks(ctx); err != nil {
		return err
	}
	if err := ti.importEngines(ctx); err != nil {
		return err
	}
	// todo: post process
	return nil
}

// in dist framework, this should be done in the tidb node which is responsible for splitting job into subtasks
// then table-importer handles data belongs to the subtask.
func (ti *tableImporterImpl) populateChunks(ctx context.Context) error {
	ti.logger.Info("populate chunks")
	tableRegions, err := mydump.MakeTableRegions(ctx, ti.dataDivideCfg)

	if err != nil {
		ti.logger.Error("populate chunks failed", zap.Error(err))
		return err
	}

	var maxRowId int64
	timestamp := time.Now().Unix()
	for _, region := range tableRegions {
		engine, found := ti.tableCp.Engines[region.EngineID]
		if !found {
			engine = &checkpoints.EngineCheckpoint{
				Status: checkpoints.CheckpointStatusLoaded,
			}
			ti.tableCp.Engines[region.EngineID] = engine
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

	if common.TableHasAutoID(ti.tableInfo.Core) {
		if err = common.RebaseGlobalAutoID(ctx, 0, ti.kvStore, ti.dbID, ti.tableInfo.Core); err != nil {
			return errors.Trace(err)
		}
		newMinRowId, _, err := common.AllocGlobalAutoID(ctx, maxRowId, ti.kvStore, ti.dbID, ti.tableInfo.Core)
		if err != nil {
			return errors.Trace(err)
		}
		ti.rebaseChunkRowID(newMinRowId)
	}

	// Add index engine checkpoint
	ti.tableCp.Engines[common.IndexEngineID] = &checkpoints.EngineCheckpoint{Status: checkpoints.CheckpointStatusLoaded}
	return nil
}

func (ti *tableImporterImpl) rebaseChunkRowID(rowIDBase int64) {
	if rowIDBase == 0 {
		return
	}
	for _, engine := range ti.tableCp.Engines {
		for _, chunk := range engine.Chunks {
			chunk.Chunk.PrevRowIDMax += rowIDBase
			chunk.Chunk.RowIDMax += rowIDBase
		}
	}
}

func (ti *tableImporterImpl) importEngines(ctx context.Context) error {
	idxEngineCfg := &backend.EngineConfig{
		TableInfo: ti.tableInfo,
	}
	idxCnt := len(ti.tableInfo.Core.Indices)
	if !common.TableHasAutoRowID(ti.tableInfo.Core) {
		idxCnt--
	}
	threshold := local.EstimateCompactionThreshold(ti.tableMeta.DataFiles, ti.tableCp, int64(idxCnt))
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

	for id, engineCP := range ti.tableCp.Engines {
		// skip index engine
		if id == common.IndexEngineID {
			continue
		}
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

func (ti *tableImporterImpl) preprocessEngine(ctx context.Context, indexEngine *backend.OpenedEngine, engineID int32, chunks []*checkpoints.ChunkCheckpoint) (*backend.ClosedEngine, error) {
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

	processor := engineProcessor{
		engineID:      engineID,
		fullTableName: ti.tableMeta.FullTableName(),
		backend:       ti.backend,
		tableInfo:     ti.tableInfo,
		logger:        ti.logger.With(zap.Int32("engine-id", engineID)),
		tableImporter: ti,
		kvSorted:      hasAutoIncrementAutoID,
		rowOrdered:    ti.tableMeta.IsRowOrdered,
		indexEngine:   indexEngine,
		chunks:        chunks,
		kvStore:       ti.kvStore,
	}
	return processor.process(ctx)
}

func (ti *tableImporterImpl) importAndCleanup(ctx context.Context, closedEngine *backend.ClosedEngine) error {
	importErr := closedEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys)
	// todo: if we need support checkpoint, engine should not be cleanup if import failed.
	cleanupErr := closedEngine.Cleanup(ctx)
	return multierr.Combine(importErr, cleanupErr)
}
