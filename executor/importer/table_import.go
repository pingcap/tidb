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
	"github.com/pingcap/failpoint"
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
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

func prepareSortDir(e *LoadDataController) (string, error) {
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

// NewTableImporter creates a new table importer.
func NewTableImporter(param *JobImportParam, e *LoadDataController) (ti JobImporter, err error) {
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, e.Table.Meta())
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", e.Table.Meta().Name)
	}

	tidbCfg := tidb.GetGlobalConfig()
	dir, err := prepareSortDir(e)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	// Disable GC because TiDB enables GC already.
	keySpaceName := tidb.GetGlobalKeyspaceName()
	// the kv store we get is a cached store, so we can't close it.
	kvStore, err := GetKVStore(fmt.Sprintf("tikv://%s?disableGC=true&keyspaceName=%s", tidbCfg.Path, keySpaceName), tls.ToTiKVSecurityConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}

	backendConfig := local.BackendConfig{
		PDAddr:            tidbCfg.Path,
		LocalStoreDir:     dir,
		MaxConnPerStore:   config.DefaultRangeConcurrency,
		ConnCompressType:  config.CompressionNone,
		WorkerConcurrency: config.DefaultRangeConcurrency * 2,
		KVWriteBatchSize:  config.KVWriteBatchSize,
		// todo: local backend report error when the sort-dir already exists & checkpoint disabled.
		// set to false when we fix it.
		CheckpointEnabled:       true,
		MemTableSize:            int(config.DefaultEngineMemCacheSize),
		LocalWriterMemCacheSize: int64(config.DefaultLocalWriterMemCacheSize),
		ShouldCheckTiKV:         true,
		DupeDetectEnabled:       false,
		DuplicateDetectOpt:      local.DupDetectOpt{ReportErrOnDup: false},
		StoreWriteBWLimit:       0,
		// todo: we can set it false when we support switch import mode.
		ShouldCheckWriteStall: true,
		MaxOpenFiles:          int(util.GenRLimit()),
		KeyspaceName:          keySpaceName,
	}

	tableMeta := &mydump.MDTableMeta{
		DB:        e.DBName,
		Name:      e.Table.Meta().Name.O,
		DataFiles: e.toMyDumpFiles(),
		// todo: set IsRowOrdered.
	}
	dataDivideCfg := &mydump.DataDivideConfig{
		ColumnCnt:         len(e.Table.Meta().Columns),
		EngineDataSize:    int64(config.DefaultBatchSize),
		MaxChunkSize:      int64(config.MaxRegionSize),
		Concurrency:       int(e.ThreadCnt),
		EngineConcurrency: config.DefaultTableConcurrency,
		IOWorkers:         nil,
		Store:             e.dataStore,
		TableMeta:         tableMeta,
	}

	// todo: use a real region size getter
	regionSizeGetter := &local.TableRegionSizeGetterImpl{}
	localBackend, err := local.NewBackend(param.GroupCtx, tls, backendConfig, regionSizeGetter)
	if err != nil {
		return nil, err
	}

	return &tableImporter{
		JobImportParam:     param,
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
	}, nil
}

type tableImporter struct {
	*JobImportParam
	*LoadDataController
	backend   *local.Backend
	tableCp   *checkpoints.TableCheckpoint
	tableInfo *checkpoints.TidbTableInfo
	tableMeta *mydump.MDTableMeta
	// this table has a separate id allocator used to record the max row id allocated.
	encTable      table.Table
	dbID          int64
	dataDivideCfg *mydump.DataDivideConfig

	store           storage.ExternalStorage
	kvStore         tidbkv.Storage
	logger          *zap.Logger
	regionSplitSize int64
	regionSplitKeys int64
}

var _ JobImporter = &tableImporter{}

// Param implements JobImporter.Param.
func (ti *tableImporter) Param() *JobImportParam {
	return ti.JobImportParam
}

// Import implements JobImporter.Import.
func (ti *tableImporter) Import() {
	ti.Group.Go(func() error {
		defer close(ti.Done)
		return ti.importTable(ti.GroupCtx)
	})
}

// Result implements JobImporter.Result.
func (ti *tableImporter) Result() string {
	return ""
}

func (ti *tableImporter) getParser(ctx context.Context, chunk *checkpoints.ChunkCheckpoint) (mydump.Parser, error) {
	info := LoadDataReaderInfo{
		Opener: func(ctx context.Context) (io.ReadSeekCloser, error) {
			reader, err := mydump.OpenReader(ctx, &chunk.FileMeta, ti.dataStore)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return reader, nil
		},
		Remote: &chunk.FileMeta,
	}
	parser, err := ti.LoadDataController.GetParser(ctx, info)
	if err != nil {
		return nil, err
	}
	if chunk.FileMeta.Compression == mydump.CompressionNone {
		// todo: when support checkpoint, we should set pos too.
		// WARN: parser.SetPos can only be set before we read anything now. should fix it before set pos.
		parser.SetRowID(chunk.Chunk.PrevRowIDMax)
	} else {
		// todo: chunk.Chunk.Offset is not set, if we ignore N lines, should set it.
		if err := mydump.ReadUntil(parser, chunk.Chunk.Offset); err != nil {
			return nil, errors.Trace(err)
		}
		parser.SetRowID(chunk.Chunk.PrevRowIDMax)
	}
	return parser, nil
}

func (ti *tableImporter) getKVEncoder(chunk *checkpoints.ChunkCheckpoint) (kvEncoder, error) {
	cfg := &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{
			SQLMode:        ti.sqlMode,
			Timestamp:      chunk.Timestamp,
			SysVars:        ti.importantSysVars,
			AutoRandomSeed: chunk.Chunk.PrevRowIDMax,
		},
		Path:   chunk.FileMeta.Path,
		Table:  ti.encTable,
		Logger: log.Logger{Logger: ti.logger.With(zap.String("path", chunk.FileMeta.Path))},
	}
	return newTableKVEncoder(cfg, ti.ColumnAssignments, ti.ColumnsAndUserVars, ti.FieldMappings, ti.InsertColumns)
}

func (ti *tableImporter) importTable(ctx context.Context) error {
	// todo: pause GC if we need duplicate detection
	// todo: register task to pd?
	// no need to pause all schedulers, since we can pause them by key range
	// todo: if add index by sql, drop all index first
	// todo: tikv enter into import mode
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
func (ti *tableImporter) populateChunks(ctx context.Context) error {
	ti.logger.Info("populate chunks")
	tableRegions, err := mydump.MakeTableRegions(ctx, ti.dataDivideCfg)

	if err != nil {
		ti.logger.Error("populate chunks failed", zap.Error(err))
		return err
	}

	var maxRowID int64
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
		if region.Chunk.RowIDMax > maxRowID {
			maxRowID = region.Chunk.RowIDMax
		}
	}

	if common.TableHasAutoID(ti.tableInfo.Core) {
		// todo: the new base should be the max row id of the last Node if we support distributed import.
		if err = common.RebaseGlobalAutoID(ctx, 0, ti.kvStore, ti.dbID, ti.tableInfo.Core); err != nil {
			return errors.Trace(err)
		}
		newMinRowID, _, err := common.AllocGlobalAutoID(ctx, maxRowID, ti.kvStore, ti.dbID, ti.tableInfo.Core)
		if err != nil {
			return errors.Trace(err)
		}
		ti.rebaseChunkRowID(newMinRowID)
	}

	// Add index engine checkpoint
	ti.tableCp.Engines[common.IndexEngineID] = &checkpoints.EngineCheckpoint{Status: checkpoints.CheckpointStatusLoaded}
	return nil
}

func (ti *tableImporter) rebaseChunkRowID(rowIDBase int64) {
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

func (ti *tableImporter) importEngines(ctx context.Context) error {
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
	mgr := backend.MakeEngineManager(ti.backend)
	indexEngine, err := mgr.OpenEngine(ctx, idxEngineCfg, fullTableName, common.IndexEngineID)
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

		failpoint.Inject("SyncAfterImportDataEngine", func() {
			TestSyncCh <- struct{}{}
			<-TestSyncCh
		})
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

func (ti *tableImporter) importAndCleanup(ctx context.Context, closedEngine *backend.ClosedEngine) error {
	importErr := closedEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys)
	if closedEngine.GetID() != common.IndexEngineID {
		// todo: change to a finer-grain progress later.
		kvCount := ti.backend.GetImportedKVCount(closedEngine.GetUUID())
		ti.Progress.LoadedRowCnt.Add(uint64(kvCount))
	}
	// todo: if we need support checkpoint, engine should not be cleanup if import failed.
	cleanupErr := closedEngine.Cleanup(ctx)
	return multierr.Combine(importErr, cleanupErr)
}

// Close implements the io.Closer interface.
func (ti *tableImporter) Close() error {
	ti.backend.Close()
	return nil
}
