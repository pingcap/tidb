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
	"runtime"
	"strconv"
	"sync"
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
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidb "github.com/pingcap/tidb/config"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/syncutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// NewTiKVModeSwitcher make it a var, so we can mock it in tests.
var NewTiKVModeSwitcher = local.NewTiKVModeSwitcher

var (
	// CheckDiskQuotaInterval is the default time interval to check disk quota.
	// TODO: make it dynamically adjusting according to the speed of import and the disk size.
	CheckDiskQuotaInterval = time.Minute
)

func prepareSortDir(e *LoadDataController, taskID int64, tidbCfg *tidb.Config) (string, error) {
	sortPathSuffix := "import-" + strconv.Itoa(int(tidbCfg.Port))
	importDir := filepath.Join(tidbCfg.TempDir, sortPathSuffix)
	sortDir := filepath.Join(importDir, strconv.FormatInt(taskID, 10))

	if info, err := os.Stat(importDir); err != nil {
		if !os.IsNotExist(err) {
			e.logger.Error("stat import dir failed", zap.String("import_dir", importDir), zap.Error(err))
			return "", errors.Trace(err)
		}
	} else if !info.IsDir() {
		e.logger.Warn("import dir is not a dir, remove it", zap.String("import_dir", importDir))
		err := os.RemoveAll(importDir)
		if err != nil {
			e.logger.Error("remove import dir failed", zap.String("import_dir", importDir), zap.Error(err))
		}
	} else {
		return sortDir, nil
	}

	err := os.MkdirAll(importDir, 0o700)
	if err != nil {
		e.logger.Error("failed to make dir", zap.String("import_dir", importDir), zap.Error(err))
		return "", errors.Trace(err)
	}
	e.logger.Info("import dir prepared", zap.String("import_dir", importDir), zap.String("sort_dir", sortDir))
	return sortDir, nil
}

// GetTiKVModeSwitcher creates a new TiKV mode switcher.
func GetTiKVModeSwitcher(logger *zap.Logger) (local.TiKVModeSwitcher, error) {
	tidbCfg := tidb.GetGlobalConfig()
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
	return NewTiKVModeSwitcher(tls, tidbCfg.Path, logger), nil
}

func getCachedKVStoreFrom(pdAddr string, tls *common.TLS) (tidbkv.Storage, error) {
	// Disable GC because TiDB enables GC already.
	keySpaceName := tidb.GetGlobalKeyspaceName()
	// the kv store we get is a cached store, so we can't close it.
	kvStore, err := GetKVStore(fmt.Sprintf("tikv://%s?disableGC=true&keyspaceName=%s", pdAddr, keySpaceName), tls.ToTiKVSecurityConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return kvStore, nil
}

// NewTableImporter creates a new table importer.
func NewTableImporter(param *JobImportParam, e *LoadDataController, taskID int64) (ti *TableImporter, err error) {
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, e.Table.Meta())
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", e.Table.Meta().Name)
	}

	tidbCfg := tidb.GetGlobalConfig()
	// todo: we only need to prepare this once on each node(we might call it 3 times in distribution framework)
	dir, err := prepareSortDir(e, taskID, tidbCfg)
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

	// no need to close kvStore, since it's a cached store.
	kvStore, err := getCachedKVStoreFrom(tidbCfg.Path, tls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	backendConfig := local.BackendConfig{
		PDAddr:                 tidbCfg.Path,
		LocalStoreDir:          dir,
		MaxConnPerStore:        config.DefaultRangeConcurrency,
		ConnCompressType:       config.CompressionNone,
		WorkerConcurrency:      config.DefaultRangeConcurrency * 2,
		KVWriteBatchSize:       config.KVWriteBatchSize,
		RegionSplitBatchSize:   config.DefaultRegionSplitBatchSize,
		RegionSplitConcurrency: runtime.GOMAXPROCS(0),
		// enable after we support checkpoint
		CheckpointEnabled:       false,
		MemTableSize:            config.DefaultEngineMemCacheSize,
		LocalWriterMemCacheSize: int64(config.DefaultLocalWriterMemCacheSize),
		ShouldCheckTiKV:         true,
		DupeDetectEnabled:       false,
		DuplicateDetectOpt:      local.DupDetectOpt{ReportErrOnDup: false},
		StoreWriteBWLimit:       int(e.MaxWriteSpeed),
		MaxOpenFiles:            int(util.GenRLimit("table_import")),
		KeyspaceName:            tidb.GetGlobalKeyspaceName(),
		PausePDSchedulerScope:   config.PausePDSchedulerScopeTable,
	}

	// todo: use a real region size getter
	regionSizeGetter := &local.TableRegionSizeGetterImpl{}
	localBackend, err := local.NewBackend(param.GroupCtx, tls, backendConfig, regionSizeGetter)
	if err != nil {
		return nil, err
	}

	return &TableImporter{
		JobImportParam:     param,
		LoadDataController: e,
		backend:            localBackend,
		tableInfo: &checkpoints.TidbTableInfo{
			ID:   e.Table.Meta().ID,
			Name: e.Table.Meta().Name.O,
			Core: e.Table.Meta(),
		},
		encTable:        tbl,
		dbID:            e.DBID,
		store:           e.dataStore,
		kvStore:         kvStore,
		logger:          e.logger,
		regionSplitSize: int64(config.SplitRegionSize),
		regionSplitKeys: int64(config.SplitRegionKeys),
		diskQuota:       adjustDiskQuota(int64(e.DiskQuota), dir, e.logger),
		diskQuotaLock:   new(syncutil.RWMutex),
	}, nil
}

// TableImporter is a table importer.
type TableImporter struct {
	*JobImportParam
	*LoadDataController
	backend   *local.Backend
	tableInfo *checkpoints.TidbTableInfo
	// this table has a separate id allocator used to record the max row id allocated.
	encTable table.Table
	dbID     int64

	store storage.ExternalStorage
	// the kv store we get is a cached store, so we can't close it.
	kvStore         tidbkv.Storage
	logger          *zap.Logger
	regionSplitSize int64
	regionSplitKeys int64
	// the smallest auto-generated ID in current import.
	// if there's no auto-generated id column or the column value is not auto-generated, it will be 0.
	lastInsertID  uint64
	diskQuota     int64
	diskQuotaLock *syncutil.RWMutex
}

func (ti *TableImporter) getParser(ctx context.Context, chunk *checkpoints.ChunkCheckpoint) (mydump.Parser, error) {
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
	// todo: when support checkpoint, we should set pos too.
	// WARN: parser.SetPos can only be set before we read anything now. should fix it before set pos.
	parser.SetRowID(chunk.Chunk.PrevRowIDMax)
	return parser, nil
}

func (ti *TableImporter) getKVEncoder(chunk *checkpoints.ChunkCheckpoint) (kvEncoder, error) {
	cfg := &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{
			SQLMode:        ti.SQLMode,
			Timestamp:      chunk.Timestamp,
			SysVars:        ti.ImportantSysVars,
			AutoRandomSeed: chunk.Chunk.PrevRowIDMax,
		},
		Path:   chunk.FileMeta.Path,
		Table:  ti.encTable,
		Logger: log.Logger{Logger: ti.logger.With(zap.String("path", chunk.FileMeta.Path))},
	}
	return newTableKVEncoder(cfg, ti)
}

// VerifyChecksum verify the checksum of the table.
func (e *LoadDataController) VerifyChecksum(ctx context.Context, localChecksum verify.KVChecksum) (err error) {
	task := log.BeginTask(e.logger, "verify checksum")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()
	tidbCfg := tidb.GetGlobalConfig()
	hostPort := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort)))
	tls, err2 := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		hostPort,
		nil, nil, nil,
	)
	if err2 != nil {
		return errors.Trace(err2)
	}

	// no need to close kvStore, since it's a cached store.
	kvStore, err2 := getCachedKVStoreFrom(tidbCfg.Path, tls)
	if err2 != nil {
		return errors.Trace(err2)
	}
	pdCli, err2 := pd.NewClientWithContext(ctx, []string{tidbCfg.Path}, tls.ToPDSecurityOption())
	if err2 != nil {
		return errors.Trace(err2)
	}
	defer pdCli.Close()

	tableInfo := &checkpoints.TidbTableInfo{
		ID:   e.Table.Meta().ID,
		Name: e.Table.Meta().Name.O,
		Core: e.Table.Meta(),
	}
	manager := local.NewTiKVChecksumManager(kvStore.GetClient(), pdCli, uint(e.DistSQLScanConcurrency))
	remoteChecksum, err2 := manager.Checksum(ctx, tableInfo)
	if err2 != nil {
		return err2
	}
	if !remoteChecksum.IsEqual(&localChecksum) {
		err3 := common.ErrChecksumMismatch.GenWithStackByArgs(
			remoteChecksum.Checksum, localChecksum.Sum(),
			remoteChecksum.TotalKVs, localChecksum.SumKVS(),
			remoteChecksum.TotalBytes, localChecksum.SumSize(),
		)
		if e.Checksum == config.OpLevelOptional {
			e.logger.Warn("verify checksum failed, but checksum is optional, will skip it", log.ShortError(err3))
			err3 = nil
		}
		return err3
	}
	e.logger.Info("checksum pass", zap.Object("local", &localChecksum))
	return nil
}

// PopulateChunks populates chunks from table regions.
// in dist framework, this should be done in the tidb node which is responsible for splitting job into subtasks
// then table-importer handles data belongs to the subtask.
func (e *LoadDataController) PopulateChunks(ctx context.Context) (ecp map[int32]*checkpoints.EngineCheckpoint, err error) {
	task := log.BeginTask(e.logger, "populate chunks")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	tableMeta := &mydump.MDTableMeta{
		DB:        e.DBName,
		Name:      e.Table.Meta().Name.O,
		DataFiles: e.toMyDumpFiles(),
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
	tableRegions, err2 := mydump.MakeTableRegions(ctx, dataDivideCfg)

	if err2 != nil {
		e.logger.Error("populate chunks failed", zap.Error(err2))
		return nil, err2
	}

	var maxRowID int64
	timestamp := time.Now().Unix()
	tableCp := &checkpoints.TableCheckpoint{
		Engines: map[int32]*checkpoints.EngineCheckpoint{},
	}
	for _, region := range tableRegions {
		engine, found := tableCp.Engines[region.EngineID]
		if !found {
			engine = &checkpoints.EngineCheckpoint{
				Status: checkpoints.CheckpointStatusLoaded,
			}
			tableCp.Engines[region.EngineID] = engine
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

	if common.TableHasAutoID(e.Table.Meta()) {
		tidbCfg := tidb.GetGlobalConfig()
		hostPort := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort)))
		tls, err4 := common.NewTLS(
			tidbCfg.Security.ClusterSSLCA,
			tidbCfg.Security.ClusterSSLCert,
			tidbCfg.Security.ClusterSSLKey,
			hostPort,
			nil, nil, nil,
		)
		if err4 != nil {
			return nil, err4
		}

		// no need to close kvStore, since it's a cached store.
		kvStore, err4 := getCachedKVStoreFrom(tidbCfg.Path, tls)
		if err4 != nil {
			return nil, errors.Trace(err4)
		}
		if err3 := common.RebaseGlobalAutoID(ctx, 0, kvStore, e.DBID, e.Table.Meta()); err3 != nil {
			return nil, errors.Trace(err3)
		}
		newMinRowID, _, err3 := common.AllocGlobalAutoID(ctx, maxRowID, kvStore, e.DBID, e.Table.Meta())
		if err3 != nil {
			return nil, errors.Trace(err3)
		}
		e.rebaseChunkRowID(newMinRowID, tableCp.Engines)
	}

	// Add index engine checkpoint
	tableCp.Engines[common.IndexEngineID] = &checkpoints.EngineCheckpoint{Status: checkpoints.CheckpointStatusLoaded}
	return tableCp.Engines, nil
}

func (*LoadDataController) rebaseChunkRowID(rowIDBase int64, engines map[int32]*checkpoints.EngineCheckpoint) {
	if rowIDBase == 0 {
		return
	}
	for _, engine := range engines {
		for _, chunk := range engine.Chunks {
			chunk.Chunk.PrevRowIDMax += rowIDBase
			chunk.Chunk.RowIDMax += rowIDBase
		}
	}
}

// a simplified version of EstimateCompactionThreshold
func (ti *TableImporter) getTotalRawFileSize(indexCnt int64) int64 {
	var totalSize int64
	for _, file := range ti.dataFiles {
		size := file.RealSize
		if file.Type == mydump.SourceTypeParquet {
			// parquet file is compressed, thus estimates with a factor of 2
			size *= 2
		}
		totalSize += size
	}
	return totalSize * indexCnt
}

// OpenIndexEngine opens an index engine.
func (ti *TableImporter) OpenIndexEngine(ctx context.Context, engineID int32) (*backend.OpenedEngine, error) {
	idxEngineCfg := &backend.EngineConfig{
		TableInfo: ti.tableInfo,
	}
	idxCnt := len(ti.tableInfo.Core.Indices)
	if !common.TableHasAutoRowID(ti.tableInfo.Core) {
		idxCnt--
	}
	// todo: getTotalRawFileSize returns size of all data files, but in distributed framework,
	// we create one index engine for each engine, should reflect this in the future.
	threshold := local.EstimateCompactionThreshold2(ti.getTotalRawFileSize(int64(idxCnt)))
	idxEngineCfg.Local = backend.LocalEngineConfig{
		Compact:            threshold > 0,
		CompactConcurrency: 4,
		CompactThreshold:   threshold,
	}
	fullTableName := ti.fullTableName()
	// todo: cleanup all engine data on any error since we don't support checkpoint for now
	// some return path, didn't make sure all data engine and index engine are cleaned up.
	// maybe we can add this in upper level to clean the whole local-sort directory
	mgr := backend.MakeEngineManager(ti.backend)
	return mgr.OpenEngine(ctx, idxEngineCfg, fullTableName, engineID)
}

// OpenDataEngine opens a data engine.
func (ti *TableImporter) OpenDataEngine(ctx context.Context, engineID int32) (*backend.OpenedEngine, error) {
	dataEngineCfg := &backend.EngineConfig{
		TableInfo: ti.tableInfo,
	}
	// todo: support checking IsRowOrdered later.
	//if ti.tableMeta.IsRowOrdered {
	//	dataEngineCfg.Local.Compact = true
	//	dataEngineCfg.Local.CompactConcurrency = 4
	//	dataEngineCfg.Local.CompactThreshold = local.CompactionUpperThreshold
	//}
	mgr := backend.MakeEngineManager(ti.backend)
	return mgr.OpenEngine(ctx, dataEngineCfg, ti.fullTableName(), engineID)
}

// ImportAndCleanup imports the engine and cleanup the engine data.
func (ti *TableImporter) ImportAndCleanup(ctx context.Context, closedEngine *backend.ClosedEngine) (int64, error) {
	var kvCount int64
	importErr := closedEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys)
	if closedEngine.GetID() != common.IndexEngineID {
		// todo: change to a finer-grain progress later.
		// each row is encoded into 1 data key
		kvCount = ti.backend.GetImportedKVCount(closedEngine.GetUUID())
	}
	// todo: if we need support checkpoint, engine should not be cleanup if import failed.
	cleanupErr := closedEngine.Cleanup(ctx)
	return kvCount, multierr.Combine(importErr, cleanupErr)
}

// FullTableName return FQDN of the table.
func (ti *TableImporter) fullTableName() string {
	return common.UniqueTable(ti.DBName, ti.Table.Meta().Name.O)
}

// Close implements the io.Closer interface.
func (ti *TableImporter) Close() error {
	ti.backend.Close()
	return nil
}

func (ti *TableImporter) setLastInsertID(id uint64) {
	// todo: if we run concurrently, we should use atomic operation here.
	if id == 0 {
		return
	}
	if ti.lastInsertID == 0 || id < ti.lastInsertID {
		ti.lastInsertID = id
	}
}

// CheckDiskQuota checks disk quota.
func (ti *TableImporter) CheckDiskQuota(ctx context.Context) {
	var locker sync.Locker
	lockDiskQuota := func() {
		if locker == nil {
			ti.diskQuotaLock.Lock()
			locker = ti.diskQuotaLock
		}
	}
	unlockDiskQuota := func() {
		if locker != nil {
			locker.Unlock()
			locker = nil
		}
	}

	defer unlockDiskQuota()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(CheckDiskQuotaInterval):
		}

		largeEngines, inProgressLargeEngines, totalDiskSize, totalMemSize := local.CheckDiskQuota(ti.backend, ti.diskQuota)
		if len(largeEngines) == 0 && inProgressLargeEngines == 0 {
			unlockDiskQuota()
			continue
		}

		ti.logger.Warn("disk quota exceeded",
			zap.Int64("diskSize", totalDiskSize),
			zap.Int64("memSize", totalMemSize),
			zap.Int64("quota", ti.diskQuota),
			zap.Int("largeEnginesCount", len(largeEngines)),
			zap.Int("inProgressLargeEnginesCount", inProgressLargeEngines))

		lockDiskQuota()

		if len(largeEngines) == 0 {
			ti.logger.Warn("all large engines are already importing, keep blocking all writes")
			continue
		}

		if err := ti.backend.FlushAllEngines(ctx); err != nil {
			ti.logger.Error("flush engine for disk quota failed, check again later", log.ShortError(err))
			unlockDiskQuota()
			continue
		}

		// at this point, all engines are synchronized on disk.
		// we then import every large engines one by one and complete.
		// if any engine failed to import, we just try again next time, since the data are still intact.
		var importErr error
		for _, engine := range largeEngines {
			// Use a larger split region size to avoid split the same region by many times.
			if err := ti.backend.UnsafeImportAndReset(
				ctx,
				engine,
				ti.regionSplitSize*int64(config.MaxSplitRegionSizeRatio),
				ti.regionSplitKeys*int64(config.MaxSplitRegionSizeRatio),
			); err != nil {
				importErr = multierr.Append(importErr, err)
			}
		}
		if importErr != nil {
			// discuss: should we return the error and cancel the import?
			ti.logger.Error("import large engines failed, check again later", log.ShortError(importErr))
		}
		unlockDiskQuota()
	}
}

func adjustDiskQuota(diskQuota int64, sortDir string, logger *zap.Logger) int64 {
	sz, err := common.GetStorageSize(sortDir)
	if err != nil {
		logger.Warn("failed to get storage size", zap.Error(err))
		if diskQuota != 0 {
			return diskQuota
		}
		logger.Info("use default quota instead", zap.Int64("quota", int64(DefaultDiskQuota)))
		return int64(DefaultDiskQuota)
	}

	maxDiskQuota := int64(float64(sz.Capacity) * 0.8)
	switch {
	case diskQuota == 0:
		logger.Info("use 0.8 of the storage size as default disk quota", zap.Int64("quota", maxDiskQuota))
		return maxDiskQuota
	case diskQuota > maxDiskQuota:
		logger.Warn("disk quota is larger than 0.8 of the storage size, use 0.8 of the storage size instead", zap.Int64("quota", maxDiskQuota))
		return maxDiskQuota
	default:
		return diskQuota
	}
}
