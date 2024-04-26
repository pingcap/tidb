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
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/keyspace"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// NewTiKVModeSwitcher make it a var, so we can mock it in tests.
var NewTiKVModeSwitcher = local.NewTiKVModeSwitcher

var (
	// CheckDiskQuotaInterval is the default time interval to check disk quota.
	// TODO: make it dynamically adjusting according to the speed of import and the disk size.
	CheckDiskQuotaInterval = 10 * time.Second

	// defaultMaxEngineSize is the default max engine size in bytes.
	// we make it 5 times larger than lightning default engine size to reduce range overlap, especially for index,
	// since we have an index engine per distributed subtask.
	// for 1TiB data, we can divide it into 2 engines that runs on 2 TiDB. it can have a good balance between
	// range overlap and sort speed in one of our test of:
	// 	- 10 columns, PK + 6 secondary index 2 of which is mv index
	//	- 1.05 KiB per row, 527 MiB per file, 1024000000 rows, 1 TiB total
	//
	// it might not be the optimal value for other cases.
	defaultMaxEngineSize = int64(5 * config.DefaultBatchSize)
)

// prepareSortDir creates a new directory for import, remove previous sort directory if exists.
func prepareSortDir(e *LoadDataController, id string, tidbCfg *tidb.Config) (string, error) {
	importDir := GetImportRootDir(tidbCfg)
	sortDir := filepath.Join(importDir, id)

	if info, err := os.Stat(importDir); err != nil || !info.IsDir() {
		if err != nil && !os.IsNotExist(err) {
			e.logger.Error("stat import dir failed", zap.String("import_dir", importDir), zap.Error(err))
			return "", errors.Trace(err)
		}
		if info != nil && !info.IsDir() {
			e.logger.Warn("import dir is not a dir, remove it", zap.String("import_dir", importDir))
			if err := os.RemoveAll(importDir); err != nil {
				return "", errors.Trace(err)
			}
		}
		e.logger.Info("import dir not exists, create it", zap.String("import_dir", importDir))
		if err := os.MkdirAll(importDir, 0o700); err != nil {
			e.logger.Error("failed to make dir", zap.String("import_dir", importDir), zap.Error(err))
			return "", errors.Trace(err)
		}
	}

	// todo: remove this after we support checkpoint
	if _, err := os.Stat(sortDir); err != nil {
		if !os.IsNotExist(err) {
			e.logger.Error("stat sort dir failed", zap.String("sort_dir", sortDir), zap.Error(err))
			return "", errors.Trace(err)
		}
	} else {
		e.logger.Warn("sort dir already exists, remove it", zap.String("sort_dir", sortDir))
		if err := os.RemoveAll(sortDir); err != nil {
			return "", errors.Trace(err)
		}
	}
	return sortDir, nil
}

// GetRegionSplitSizeKeys gets the region split size and keys from PD.
func GetRegionSplitSizeKeys(ctx context.Context) (regionSplitSize int64, regionSplitKeys int64, err error) {
	tidbCfg := tidb.GetGlobalConfig()
	tls, err := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		"",
		nil, nil, nil,
	)
	if err != nil {
		return 0, 0, err
	}
	tlsOpt := tls.ToPDSecurityOption()
	addrs := strings.Split(tidbCfg.Path, ",")
	pdCli, err := NewClientWithContext(ctx, addrs, tlsOpt)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	defer pdCli.Close()
	return local.GetRegionSplitSizeKeys(ctx, pdCli, tls)
}

// NewTableImporter creates a new table importer.
func NewTableImporter(
	ctx context.Context,
	e *LoadDataController,
	id string,
	kvStore tidbkv.Storage,
) (ti *TableImporter, err error) {
	idAlloc := kv.NewPanickingAllocators(e.Table.Meta().SepAutoInc(), 0)
	tbl, err := tables.TableFromMeta(idAlloc, e.Table.Meta())
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", e.Table.Meta().Name)
	}

	tidbCfg := tidb.GetGlobalConfig()
	// todo: we only need to prepare this once on each node(we might call it 3 times in distribution framework)
	dir, err := prepareSortDir(e, id, tidbCfg)
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

	backendConfig := e.getLocalBackendCfg(tidbCfg.Path, dir)
	d := kvStore.(tidbkv.StorageWithPD).GetPDClient().GetServiceDiscovery()
	localBackend, err := local.NewBackend(ctx, tls, backendConfig, d)
	if err != nil {
		return nil, err
	}

	return &TableImporter{
		LoadDataController: e,
		id:                 id,
		backend:            localBackend,
		tableInfo: &checkpoints.TidbTableInfo{
			ID:   e.Table.Meta().ID,
			Name: e.Table.Meta().Name.O,
			Core: e.Table.Meta(),
		},
		encTable: tbl,
		dbID:     e.DBID,
		keyspace: kvStore.GetCodec().GetKeyspace(),
		logger:   e.logger.With(zap.String("import-id", id)),
		// this is the value we use for 50TiB data parallel import.
		// this might not be the optimal value.
		// todo: use different default for single-node import and distributed import.
		regionSplitSize: 2 * int64(config.SplitRegionSize),
		regionSplitKeys: 2 * int64(config.SplitRegionKeys),
		diskQuota:       adjustDiskQuota(int64(e.DiskQuota), dir, e.logger),
		diskQuotaLock:   new(syncutil.RWMutex),
	}, nil
}

// TableImporter is a table importer.
type TableImporter struct {
	*LoadDataController
	// id is the unique id for this importer.
	// it's the task id if we are running in distributed framework, else it's an
	// uuid. we use this id to create a unique directory for this importer.
	id        string
	backend   *local.Backend
	tableInfo *checkpoints.TidbTableInfo
	// this table has a separate id allocator used to record the max row id allocated.
	encTable table.Table
	dbID     int64

	keyspace        []byte
	logger          *zap.Logger
	regionSplitSize int64
	regionSplitKeys int64
	diskQuota       int64
	diskQuotaLock   *syncutil.RWMutex

	rowCh chan QueryRow
}

// NewTableImporterForTest creates a new table importer for test.
func NewTableImporterForTest(ctx context.Context, e *LoadDataController, id string, helper local.StoreHelper) (*TableImporter, error) {
	idAlloc := kv.NewPanickingAllocators(e.Table.Meta().SepAutoInc(), 0)
	tbl, err := tables.TableFromMeta(idAlloc, e.Table.Meta())
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", e.Table.Meta().Name)
	}

	tidbCfg := tidb.GetGlobalConfig()
	dir, err := prepareSortDir(e, id, tidbCfg)
	if err != nil {
		return nil, err
	}

	backendConfig := e.getLocalBackendCfg(tidbCfg.Path, dir)
	localBackend, err := local.NewBackendForTest(ctx, backendConfig, helper)
	if err != nil {
		return nil, err
	}

	return &TableImporter{
		LoadDataController: e,
		id:                 id,
		backend:            localBackend,
		tableInfo: &checkpoints.TidbTableInfo{
			ID:   e.Table.Meta().ID,
			Name: e.Table.Meta().Name.O,
			Core: e.Table.Meta(),
		},
		encTable:      tbl,
		dbID:          e.DBID,
		logger:        e.logger.With(zap.String("import-id", id)),
		diskQuotaLock: new(syncutil.RWMutex),
	}, nil
}

// GetKeySpace gets the keyspace of the kv store.
func (ti *TableImporter) GetKeySpace() []byte {
	return ti.keyspace
}

func (ti *TableImporter) getParser(ctx context.Context, chunk *checkpoints.ChunkCheckpoint) (mydump.Parser, error) {
	info := LoadDataReaderInfo{
		Opener: func(ctx context.Context) (io.ReadSeekCloser, error) {
			reader, err := mydump.OpenReader(ctx, &chunk.FileMeta, ti.dataStore, storage.DecompressConfig{
				ZStdDecodeConcurrency: 1,
			})
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
	if chunk.Chunk.Offset == 0 {
		// if data file is split, only the first chunk need to do skip.
		// see check in initOptions.
		if err = ti.LoadDataController.HandleSkipNRows(parser); err != nil {
			return nil, err
		}
		parser.SetRowID(chunk.Chunk.PrevRowIDMax)
	} else {
		// if we reached here, the file must be an uncompressed CSV file.
		if err = parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax); err != nil {
			return nil, err
		}
	}
	return parser, nil
}

func (ti *TableImporter) getKVEncoder(chunk *checkpoints.ChunkCheckpoint) (KVEncoder, error) {
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
	return NewTableKVEncoder(cfg, ti)
}

func (e *LoadDataController) calculateSubtaskCnt() int {
	// we want to split data files into subtask of size close to MaxEngineSize to reduce range overlap,
	// and evenly distribute them to subtasks.
	// we calculate subtask count first by round(TotalFileSize / maxEngineSize)

	// AllocateEngineIDs is using ceil() to calculate subtask count, engine size might be too small in some case,
	// such as 501G data, maxEngineSize will be about 250G, so we don't relay on it.
	// see https://github.com/pingcap/tidb/blob/b4183e1dc9bb01fb81d3aa79ca4b5b74387c6c2a/br/pkg/lightning/mydump/region.go#L109
	//
	// for default e.MaxEngineSize = 500GiB, we have:
	// data size range(G)   cnt    adjusted-engine-size range(G)
	// [0, 750)               1    [0, 750)
	// [750, 1250)            2    [375, 625)
	// [1250, 1750)           3    [416, 583)
	// [1750, 2250)           4    [437, 562)
	var (
		subtaskCount  float64
		maxEngineSize = int64(e.MaxEngineSize)
	)
	if e.TotalFileSize <= maxEngineSize {
		subtaskCount = 1
	} else {
		subtaskCount = math.Round(float64(e.TotalFileSize) / float64(e.MaxEngineSize))
	}

	// for global sort task, since there is no overlap,
	// we make sure subtask count is a multiple of execute nodes count
	if e.IsGlobalSort() && e.ExecuteNodesCnt > 0 {
		subtaskCount = math.Ceil(subtaskCount/float64(e.ExecuteNodesCnt)) * float64(e.ExecuteNodesCnt)
	}
	return int(subtaskCount)
}

func (e *LoadDataController) getAdjustedMaxEngineSize() int64 {
	subtaskCount := e.calculateSubtaskCnt()
	// we adjust MaxEngineSize to make sure each subtask has a similar amount of data to import.
	return int64(math.Ceil(float64(e.TotalFileSize) / float64(subtaskCount)))
}

// SetExecuteNodeCnt sets the execute node count.
func (e *LoadDataController) SetExecuteNodeCnt(cnt int) {
	e.ExecuteNodesCnt = cnt
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
	adjustedMaxEngineSize := e.getAdjustedMaxEngineSize()
	e.logger.Info("adjust max engine size", zap.Int64("before", int64(e.MaxEngineSize)),
		zap.Int64("after", adjustedMaxEngineSize))
	dataDivideCfg := &mydump.DataDivideConfig{
		ColumnCnt:      len(e.Table.Meta().Columns),
		EngineDataSize: adjustedMaxEngineSize,
		MaxChunkSize:   int64(config.MaxRegionSize),
		Concurrency:    e.ThreadCnt,
		IOWorkers:      nil,
		Store:          e.dataStore,
		TableMeta:      tableMeta,

		StrictFormat:           e.SplitFile,
		DataCharacterSet:       *e.Charset,
		DataInvalidCharReplace: string(utf8.RuneError),
		ReadBlockSize:          LoadDataReadBlockSize,
		CSV:                    *e.GenerateCSVConfig(),
	}
	makeEngineCtx := log.NewContext(ctx, log.Logger{Logger: e.logger})
	tableRegions, err2 := mydump.MakeTableRegions(makeEngineCtx, dataDivideCfg)

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

	// Add index engine checkpoint
	tableCp.Engines[common.IndexEngineID] = &checkpoints.EngineCheckpoint{Status: checkpoints.CheckpointStatusLoaded}
	return tableCp.Engines, nil
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
		BlockSize:          16 * 1024,
	}
	fullTableName := ti.FullTableName()
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
	// also see test result here: https://github.com/pingcap/tidb/pull/47147
	//if ti.tableMeta.IsRowOrdered {
	//	dataEngineCfg.Local.Compact = true
	//	dataEngineCfg.Local.CompactConcurrency = 4
	//	dataEngineCfg.Local.CompactThreshold = local.CompactionUpperThreshold
	//}
	mgr := backend.MakeEngineManager(ti.backend)
	return mgr.OpenEngine(ctx, dataEngineCfg, ti.FullTableName(), engineID)
}

// ImportAndCleanup imports the engine and cleanup the engine data.
func (ti *TableImporter) ImportAndCleanup(ctx context.Context, closedEngine *backend.ClosedEngine) (int64, error) {
	var kvCount int64
	importErr := closedEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys)
	if common.ErrFoundDuplicateKeys.Equal(importErr) {
		importErr = local.ConvertToErrFoundConflictRecords(importErr, ti.encTable)
	}
	if closedEngine.GetID() != common.IndexEngineID {
		// todo: change to a finer-grain progress later.
		// each row is encoded into 1 data key
		kvCount = ti.backend.GetImportedKVCount(closedEngine.GetUUID())
	}
	cleanupErr := closedEngine.Cleanup(ctx)
	return kvCount, multierr.Combine(importErr, cleanupErr)
}

// Backend returns the backend of the importer.
func (ti *TableImporter) Backend() *local.Backend {
	return ti.backend
}

// Close implements the io.Closer interface.
func (ti *TableImporter) Close() error {
	ti.backend.Close()
	return nil
}

// Allocators returns allocators used to record max used ID, i.e. PanickingAllocators.
func (ti *TableImporter) Allocators() autoid.Allocators {
	return ti.encTable.Allocators(nil)
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
	ti.logger.Info("start checking disk quota", zap.String("disk-quota", units.BytesSize(float64(ti.diskQuota))))
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
				int64(config.SplitRegionSize)*int64(config.MaxSplitRegionSizeRatio),
				int64(config.SplitRegionKeys)*int64(config.MaxSplitRegionSizeRatio),
			); err != nil {
				if common.ErrFoundDuplicateKeys.Equal(err) {
					err = local.ConvertToErrFoundConflictRecords(err, ti.encTable)
				}
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

// SetSelectedRowCh sets the channel to receive selected rows.
func (ti *TableImporter) SetSelectedRowCh(ch chan QueryRow) {
	ti.rowCh = ch
}

func (ti *TableImporter) closeAndCleanupEngine(engine *backend.OpenedEngine) {
	// outer context might be done, so we create a new context here.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	closedEngine, err := engine.Close(ctx)
	if err != nil {
		ti.logger.Error("close engine failed", zap.Error(err))
		return
	}
	if err = closedEngine.Cleanup(ctx); err != nil {
		ti.logger.Error("cleanup engine failed", zap.Error(err))
	}
}

// ImportSelectedRows imports selected rows.
func (ti *TableImporter) ImportSelectedRows(ctx context.Context, se sessionctx.Context) (*JobImportResult, error) {
	var (
		err                     error
		dataEngine, indexEngine *backend.OpenedEngine
	)
	metrics := tidbmetrics.GetRegisteredImportMetrics(promutil.NewDefaultFactory(),
		prometheus.Labels{
			proto.TaskIDLabelName: ti.id,
		})
	ctx = metric.WithCommonMetric(ctx, metrics)
	defer func() {
		tidbmetrics.UnregisterImportMetrics(metrics)
		if dataEngine != nil {
			ti.closeAndCleanupEngine(dataEngine)
		}
		if indexEngine != nil {
			ti.closeAndCleanupEngine(indexEngine)
		}
	}()

	dataEngine, err = ti.OpenDataEngine(ctx, 1)
	if err != nil {
		return nil, err
	}
	indexEngine, err = ti.OpenIndexEngine(ctx, common.IndexEngineID)
	if err != nil {
		return nil, err
	}

	var (
		mu         sync.Mutex
		checksum   = verify.NewKVGroupChecksumWithKeyspace(ti.keyspace)
		colSizeMap = make(map[int64]int64)
	)
	eg, egCtx := tidbutil.NewErrorGroupWithRecoverWithCtx(ctx)
	for i := 0; i < ti.ThreadCnt; i++ {
		eg.Go(func() error {
			chunkCheckpoint := checkpoints.ChunkCheckpoint{}
			chunkChecksum := verify.NewKVGroupChecksumWithKeyspace(ti.keyspace)
			progress := NewProgress()
			defer func() {
				mu.Lock()
				defer mu.Unlock()
				checksum.Add(chunkChecksum)
				for k, v := range progress.GetColSize() {
					colSizeMap[k] += v
				}
			}()
			return ProcessChunk(egCtx, &chunkCheckpoint, ti, dataEngine, indexEngine, progress, ti.logger, chunkChecksum)
		})
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	closedDataEngine, err := dataEngine.Close(ctx)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("mockImportFromSelectErr", func() {
		failpoint.Return(nil, errors.New("mock import from select error"))
	})
	if err = closedDataEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys); err != nil {
		if common.ErrFoundDuplicateKeys.Equal(err) {
			err = local.ConvertToErrFoundConflictRecords(err, ti.encTable)
		}
		return nil, err
	}
	dataKVCount := ti.backend.GetImportedKVCount(closedDataEngine.GetUUID())

	closedIndexEngine, err := indexEngine.Close(ctx)
	if err != nil {
		return nil, err
	}
	if err = closedIndexEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys); err != nil {
		if common.ErrFoundDuplicateKeys.Equal(err) {
			err = local.ConvertToErrFoundConflictRecords(err, ti.encTable)
		}
		return nil, err
	}

	allocators := ti.Allocators()
	maxIDs := map[autoid.AllocatorType]int64{
		autoid.RowIDAllocType:    allocators.Get(autoid.RowIDAllocType).Base(),
		autoid.AutoIncrementType: allocators.Get(autoid.AutoIncrementType).Base(),
		autoid.AutoRandomType:    allocators.Get(autoid.AutoRandomType).Base(),
	}
	if err = PostProcess(ctx, se, maxIDs, ti.Plan, checksum, ti.logger); err != nil {
		return nil, err
	}

	return &JobImportResult{
		Affected:   uint64(dataKVCount),
		ColSizeMap: colSizeMap,
	}, nil
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
		logger.Info("use 0.8 of the storage size as default disk quota",
			zap.String("quota", units.HumanSize(float64(maxDiskQuota))))
		return maxDiskQuota
	case diskQuota > maxDiskQuota:
		logger.Warn("disk quota is larger than 0.8 of the storage size, use 0.8 of the storage size instead",
			zap.String("quota", units.HumanSize(float64(maxDiskQuota))))
		return maxDiskQuota
	default:
		return diskQuota
	}
}

// PostProcess does the post-processing for the task.
// exported for testing.
func PostProcess(
	ctx context.Context,
	se sessionctx.Context,
	maxIDs map[autoid.AllocatorType]int64,
	plan *Plan,
	localChecksum *verify.KVGroupChecksum,
	logger *zap.Logger,
) (err error) {
	callLog := log.BeginTask(logger.With(zap.Object("checksum", localChecksum)), "post process")
	defer func() {
		callLog.End(zap.ErrorLevel, err)
	}()

	if err = RebaseAllocatorBases(ctx, se.GetStore(), maxIDs, plan, logger); err != nil {
		return err
	}

	return VerifyChecksum(ctx, plan, localChecksum.MergedChecksum(), se, logger)
}

type autoIDRequirement struct {
	store     tidbkv.Storage
	autoidCli *autoid.ClientDiscover
}

func (r *autoIDRequirement) Store() tidbkv.Storage {
	return r.store
}

func (r *autoIDRequirement) AutoIDClient() *autoid.ClientDiscover {
	return r.autoidCli
}

// RebaseAllocatorBases rebase the allocator bases.
func RebaseAllocatorBases(ctx context.Context, kvStore tidbkv.Storage, maxIDs map[autoid.AllocatorType]int64, plan *Plan, logger *zap.Logger) (err error) {
	callLog := log.BeginTask(logger, "rebase allocators")
	defer func() {
		callLog.End(zap.ErrorLevel, err)
	}()

	if !common.TableHasAutoID(plan.DesiredTableInfo) {
		return nil
	}

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
		return err2
	}

	addrs := strings.Split(tidbCfg.Path, ",")
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:        addrs,
		AutoSyncInterval: 30 * time.Second,
		TLS:              tls.TLSConfig(),
	})
	if err != nil {
		return errors.Trace(err)
	}
	etcd.SetEtcdCliByNamespace(etcdCli, keyspace.MakeKeyspaceEtcdNamespace(kvStore.GetCodec()))
	autoidCli := autoid.NewClientDiscover(etcdCli)
	r := autoIDRequirement{store: kvStore, autoidCli: autoidCli}
	err = common.RebaseTableAllocators(ctx, maxIDs, &r, plan.DBID, plan.DesiredTableInfo)
	if err1 := etcdCli.Close(); err1 != nil {
		logger.Info("close etcd client error", zap.Error(err1))
	}
	autoidCli.ResetConn(nil)
	return errors.Trace(err)
}

// VerifyChecksum verify the checksum of the table.
func VerifyChecksum(ctx context.Context, plan *Plan, localChecksum verify.KVChecksum, se sessionctx.Context, logger *zap.Logger) error {
	if plan.Checksum == config.OpLevelOff {
		return nil
	}
	logger.Info("local checksum", zap.Object("checksum", &localChecksum))

	failpoint.Inject("waitCtxDone", func() {
		<-ctx.Done()
	})

	remoteChecksum, err := checksumTable(ctx, se, plan, logger)
	if err != nil {
		if plan.Checksum != config.OpLevelOptional {
			return err
		}
		logger.Warn("checksumTable failed, will skip this error and go on", zap.Error(err))
	}
	if remoteChecksum != nil {
		if !remoteChecksum.IsEqual(&localChecksum) {
			err2 := common.ErrChecksumMismatch.GenWithStackByArgs(
				remoteChecksum.Checksum, localChecksum.Sum(),
				remoteChecksum.TotalKVs, localChecksum.SumKVS(),
				remoteChecksum.TotalBytes, localChecksum.SumSize(),
			)
			if plan.Checksum == config.OpLevelOptional {
				logger.Warn("verify checksum failed, but checksum is optional, will skip it", zap.Error(err2))
				err2 = nil
			}
			return err2
		}
		logger.Info("checksum pass", zap.Object("local", &localChecksum))
	}
	return nil
}

func checksumTable(ctx context.Context, se sessionctx.Context, plan *Plan, logger *zap.Logger) (*local.RemoteChecksum, error) {
	var (
		tableName                    = common.UniqueTable(plan.DBName, plan.TableInfo.Name.L)
		sql                          = "ADMIN CHECKSUM TABLE " + tableName
		maxErrorRetryCount           = 3
		distSQLScanConcurrencyFactor = 1
		remoteChecksum               *local.RemoteChecksum
		txnErr                       error
		doneCh                       = make(chan struct{})
	)
	checkCtx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		<-doneCh
	}()

	go func() {
		<-checkCtx.Done()
		se.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
		close(doneCh)
	}()

	distSQLScanConcurrencyBak := se.GetSessionVars().DistSQLScanConcurrency()
	defer func() {
		se.GetSessionVars().SetDistSQLScanConcurrency(distSQLScanConcurrencyBak)
	}()
	ctx = util.WithInternalSourceType(checkCtx, tidbkv.InternalImportInto)
	for i := 0; i < maxErrorRetryCount; i++ {
		txnErr = func() error {
			// increase backoff weight
			if err := setBackoffWeight(se, plan, logger); err != nil {
				logger.Warn("set tidb_backoff_weight failed", zap.Error(err))
			}

			newConcurrency := mathutil.Max(plan.DistSQLScanConcurrency/distSQLScanConcurrencyFactor, local.MinDistSQLScanConcurrency)
			logger.Info("checksum with adjusted distsql scan concurrency", zap.Int("concurrency", newConcurrency))
			se.GetSessionVars().SetDistSQLScanConcurrency(newConcurrency)

			// TODO: add resource group name

			rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sql)
			if err != nil {
				return err
			}
			if len(rs) < 1 {
				return errors.New("empty checksum result")
			}

			failpoint.Inject("errWhenChecksum", func() {
				failpoint.Return(errors.New("occur an error when checksum, coprocessor task terminated due to exceeding the deadline"))
			})

			// ADMIN CHECKSUM TABLE <schema>.<table>  example.
			// 	mysql> admin checksum table test.t;
			// +---------+------------+---------------------+-----------+-------------+
			// | Db_name | Table_name | Checksum_crc64_xor  | Total_kvs | Total_bytes |
			// +---------+------------+---------------------+-----------+-------------+
			// | test    | t          | 8520875019404689597 |   7296873 |   357601387 |
			// +---------+------------+-------------
			remoteChecksum = &local.RemoteChecksum{
				Schema:     rs[0].GetString(0),
				Table:      rs[0].GetString(1),
				Checksum:   rs[0].GetUint64(2),
				TotalKVs:   rs[0].GetUint64(3),
				TotalBytes: rs[0].GetUint64(4),
			}
			return nil
		}()
		if !common.IsRetryableError(txnErr) {
			break
		}
		distSQLScanConcurrencyFactor *= 2
		logger.Warn("retry checksum table", zap.Int("retry count", i+1), zap.Error(txnErr))
	}
	return remoteChecksum, txnErr
}

func setBackoffWeight(se sessionctx.Context, plan *Plan, logger *zap.Logger) error {
	backoffWeight := local.DefaultBackoffWeight
	if val, ok := plan.ImportantSysVars[variable.TiDBBackOffWeight]; ok {
		if weight, err := strconv.Atoi(val); err == nil && weight > backoffWeight {
			backoffWeight = weight
		}
	}
	logger.Info("set backoff weight", zap.Int("weight", backoffWeight))
	return se.GetSessionVars().SetSystemVar(variable.TiDBBackOffWeight, strconv.Itoa(backoffWeight))
}

// GetImportRootDir returns the root directory for import.
// The directory structure is like:
//
//	-> /path/to/tidb-tmpdir
//	  -> import-4000
//	  -> 1
//	  -> some-uuid
//
// exported for testing.
func GetImportRootDir(tidbCfg *tidb.Config) string {
	sortPathSuffix := "import-" + strconv.Itoa(int(tidbCfg.Port))
	return filepath.Join(tidbCfg.TempDir, sortPathSuffix)
}

// FlushTableStats flushes the stats of the table.
// stats will be flushed in domain.updateStatsWorker, default interval is [1, 2) minutes,
// see DumpStatsDeltaToKV for more details. then the background analyzer will analyze
// the table.
// the stats stay in memory until the next flush, so it might be lost if the tidb-server restarts.
func FlushTableStats(ctx context.Context, se sessionctx.Context, tableID int64, result *JobImportResult) error {
	if err := sessiontxn.NewTxn(ctx, se); err != nil {
		return err
	}
	sessionVars := se.GetSessionVars()
	sessionVars.TxnCtxMu.Lock()
	defer sessionVars.TxnCtxMu.Unlock()
	sessionVars.TxnCtx.UpdateDeltaForTable(tableID, int64(result.Affected), int64(result.Affected), result.ColSizeMap)
	se.StmtCommit(ctx)
	return se.CommitTxn(ctx)
}
