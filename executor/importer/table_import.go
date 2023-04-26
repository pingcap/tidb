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
	"sync"
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
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidb "github.com/pingcap/tidb/config"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func prepareSortDir(e *LoadDataController, jobID int64) (string, error) {
	tidbCfg := tidb.GetGlobalConfig()
	sortPathSuffix := "import-" + strconv.Itoa(int(tidbCfg.Port))
	sortPath := filepath.Join(tidbCfg.TempDir, sortPathSuffix, strconv.FormatInt(jobID, 10))

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
func NewTableImporter(param *JobImportParam, e *LoadDataController) (ti *TableImporter, err error) {
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, e.Table.Meta())
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", e.Table.Meta().Name)
	}

	tidbCfg := tidb.GetGlobalConfig()
	dir, err := prepareSortDir(e, param.Job.ID)
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
		MemTableSize:            config.DefaultEngineMemCacheSize,
		LocalWriterMemCacheSize: int64(config.DefaultLocalWriterMemCacheSize),
		ShouldCheckTiKV:         true,
		DupeDetectEnabled:       false,
		DuplicateDetectOpt:      local.DupDetectOpt{ReportErrOnDup: false},
		StoreWriteBWLimit:       int(e.MaxWriteSpeed),
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

	return &TableImporter{
		JobImportParam:     param,
		LoadDataController: e,
		backend:            localBackend,
		TableCP: &checkpoints.TableCheckpoint{
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

		firstErr: &common.OnceError{},
	}, nil
}

// TableImporter is a table importer.
// Note: in lightning we use import to mean write&ingest data into TiKV, but we use import to mean import table too,
// which is confusing. So we clarify the meaning of each word(only take effects in LOAD DATA physical mode):
// - import: import the data into a table of TiDB cluster, which is separated into 3 steps:
//   - sort: encode the data into kv pairs and sort them.
//   - ingest: write the sorted kv pairs into TiKV, and let TiKV ingest them.
//   - post-process: checksum, analyze table, etc.
//
// to speed up import, data is divided into multiple engines, and each engine can do sort&ingest on its own,
// so we can do sort and ingest in a pipelined way.
// post-process can only be done after all engines are imported, so it's done after all engines are processed.
type TableImporter struct {
	*JobImportParam
	*LoadDataController
	backend *local.Backend
	// some fields are exported to ease testing.
	TableCP   *checkpoints.TableCheckpoint
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
	// the smallest auto-generated ID in current import.
	// if there's no auto-generated id column or the column value is not auto-generated, it will be 0.
	fieldMu struct {
		sync.Mutex
		lastInsertID uint64
	}

	// the first error occurred during import.
	firstErr *common.OnceError
}

var _ JobImporter = &TableImporter{}

// Param implements JobImporter.Param.
func (ti *TableImporter) Param() *JobImportParam {
	return ti.JobImportParam
}

// Import implements JobImporter.Import.
func (ti *TableImporter) Import() {
	ti.Group.Go(func() error {
		defer close(ti.Done)
		return ti.importTable(ti.GroupCtx)
	})
}

// Result implements JobImporter.Result.
func (ti *TableImporter) Result() JobImportResult {
	var (
		numWarnings uint64
		numRecords  uint64
		numDeletes  uint64
		numSkipped  uint64
	)
	numRecords = ti.Progress.ReadRowCnt.Load()
	// todo: we don't have a strict REPLACE or IGNORE mode in physical mode, so we can't get the numDeletes/numSkipped.
	// we can have it when there's duplicate detection.
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo].Raw, numRecords, numDeletes, numSkipped, numWarnings)
	return JobImportResult{
		Msg:          msg,
		Affected:     ti.Progress.LoadedRowCnt.Load(),
		LastInsertID: ti.getLastInsertID(),
	}
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
	return newTableKVEncoder(cfg, ti.ColumnAssignments, ti.ColumnsAndUserVars, ti.FieldMappings, ti.InsertColumns)
}

func (ti *TableImporter) importTable(ctx context.Context) (err error) {
	task := log.BeginTask(ti.logger, "import table")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()
	// todo: pause GC if we need duplicate detection
	// todo: register task to pd?
	// no need to pause all schedulers, since we can pause them by key range
	// todo: if add index by sql, drop all index first
	// todo: tikv enter into import mode
	if _, err2 := ti.PopulateChunks(ctx); err2 != nil {
		return err2
	}
	if err2 := ti.sortAndIngestEngines(ctx); err2 != nil {
		return err2
	}
	return ti.postProcess(ctx)
}

func (ti *TableImporter) postProcess(ctx context.Context) (err error) {
	task := log.BeginTask(ti.logger, "post process")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()
	// todo: post process
	if ti.Checksum != config.OpLevelOff {
		return ti.verifyChecksum(ctx)
	}
	return nil
}

func (ti *TableImporter) verifyChecksum(ctx context.Context) (err error) {
	task := log.BeginTask(ti.logger, "verify checksum")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()
	var localChecksum verify.KVChecksum
	for _, en := range ti.TableCP.Engines {
		for _, chunk := range en.Chunks {
			localChecksum.Add(&chunk.Checksum)
		}
	}
	ti.logger.Info("local checksum", zap.Object("checksum", &localChecksum))
	manager := local.NewTiKVChecksumManager(ti.kvStore.GetClient(), ti.backend.GetPDClient(), uint(ti.DistSQLScanConcurrency))
	remoteChecksum, err2 := manager.Checksum(ctx, ti.tableInfo)
	if err2 != nil {
		return err2
	}
	if remoteChecksum.IsEqual(&localChecksum) {
		ti.logger.Info("checksum pass", zap.Object("local", &localChecksum))
	} else {
		err3 := common.ErrChecksumMismatch.GenWithStackByArgs(
			remoteChecksum.Checksum, localChecksum.Sum(),
			remoteChecksum.TotalKVs, localChecksum.SumKVS(),
			remoteChecksum.TotalBytes, localChecksum.SumSize(),
		)
		if ti.Checksum == config.OpLevelOptional {
			ti.logger.Warn("verify checksum failed, but checksum is optional, will skip it", log.ShortError(err3))
			err3 = nil
		}
		return err3
	}
	return nil
}

// PopulateChunks populates chunks from table regions.
// in dist framework, this should be done in the tidb node which is responsible for splitting job into subtasks
// then table-importer handles data belongs to the subtask.
func (ti *TableImporter) PopulateChunks(ctx context.Context) (ecp map[int32]*checkpoints.EngineCheckpoint, err error) {
	task := log.BeginTask(ti.logger, "populate chunks")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()
	tableRegions, err2 := mydump.MakeTableRegions(ctx, ti.dataDivideCfg)

	if err2 != nil {
		ti.logger.Error("populate chunks failed", zap.Error(err2))
		return nil, err2
	}

	var maxRowID int64
	timestamp := time.Now().Unix()
	for _, region := range tableRegions {
		engine, found := ti.TableCP.Engines[region.EngineID]
		if !found {
			engine = &checkpoints.EngineCheckpoint{
				Status: checkpoints.CheckpointStatusLoaded,
			}
			ti.TableCP.Engines[region.EngineID] = engine
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
		if err3 := common.RebaseGlobalAutoID(ctx, 0, ti.kvStore, ti.dbID, ti.tableInfo.Core); err3 != nil {
			return nil, errors.Trace(err3)
		}
		newMinRowID, _, err3 := common.AllocGlobalAutoID(ctx, maxRowID, ti.kvStore, ti.dbID, ti.tableInfo.Core)
		if err3 != nil {
			return nil, errors.Trace(err3)
		}
		ti.rebaseChunkRowID(newMinRowID)
	}

	// Add index engine checkpoint
	ti.TableCP.Engines[common.IndexEngineID] = &checkpoints.EngineCheckpoint{Status: checkpoints.CheckpointStatusLoaded}
	return ti.TableCP.Engines, nil
}

func (ti *TableImporter) rebaseChunkRowID(rowIDBase int64) {
	if rowIDBase == 0 {
		return
	}
	for _, engine := range ti.TableCP.Engines {
		for _, chunk := range engine.Chunks {
			chunk.Chunk.PrevRowIDMax += rowIDBase
			chunk.Chunk.RowIDMax += rowIDBase
		}
	}
}

// OpenIndexEngine opens an index engine.
func (ti *TableImporter) OpenIndexEngine(ctx context.Context) (*backend.OpenedEngine, error) {
	idxEngineCfg := &backend.EngineConfig{
		TableInfo: ti.tableInfo,
	}
	idxCnt := len(ti.tableInfo.Core.Indices)
	if !common.TableHasAutoRowID(ti.tableInfo.Core) {
		idxCnt--
	}
	threshold := local.EstimateCompactionThreshold(ti.tableMeta.DataFiles, ti.TableCP, int64(idxCnt))
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
	return mgr.OpenEngine(ctx, idxEngineCfg, fullTableName, common.IndexEngineID)
}

// OpenDataEngine opens a data engine.
func (ti *TableImporter) OpenDataEngine(ctx context.Context, engineID int32) (*backend.OpenedEngine, error) {
	dataEngineCfg := &backend.EngineConfig{
		TableInfo: ti.tableInfo,
	}
	if ti.tableMeta.IsRowOrdered {
		dataEngineCfg.Local.Compact = true
		dataEngineCfg.Local.CompactConcurrency = 4
		dataEngineCfg.Local.CompactThreshold = local.CompactionUpperThreshold
	}
	mgr := backend.MakeEngineManager(ti.backend)
	return mgr.OpenEngine(ctx, dataEngineCfg, ti.tableMeta.FullTableName(), engineID)
}

func (ti *TableImporter) sortAndIngestEngines(ctx context.Context) (err error) {
	task := log.BeginTask(ti.logger, "sort and ingest engines")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()
	indexEngine, err3 := ti.OpenIndexEngine(ctx)
	if err3 != nil {
		return errors.Trace(err3)
	}
	defer func() {
		closedIndexEngine, err4 := indexEngine.Close(ctx)
		if err == nil {
			if err4 == nil {
				err = ti.IngestAndCleanup(ctx, closedIndexEngine)
			} else {
				err = err4
			}
		} else if err4 != nil {
			ti.logger.Error("failed to close index engine", zap.Error(err4))
		}
	}()

	sortPool := worker.NewPool(ctx, int(ti.ThreadCnt), "sorter")
	engineCh := make(chan *engine, len(ti.TableCP.Engines))

	importTableCtx, importTableCancelFn := context.WithCancel(ctx)
	ingestGroup, _ := errgroup.WithContext(importTableCtx)
	ingestGroup.Go(func() error {
		return ti.ingestAndCleanupEngines(importTableCtx, importTableCancelFn, engineCh)
	})
engineLoop:
	for id, engineCP := range ti.TableCP.Engines {
		// skip index engine
		if id == common.IndexEngineID {
			continue
		}
		select {
		case <-importTableCtx.Done():
			break engineLoop
		default:
		}

		// we use parent ctx for when open data engine, since we may cancel importTableCtx explicitly,
		// and we want to make sure engine closed even if importTableCtx is cancelled.
		dataEngine, err2 := ti.OpenDataEngine(ctx, id)
		if err2 != nil {
			if !common.IsContextCanceledError(err2) {
				ti.logger.Error("failed to create engine", zap.Error(err2), zap.Int32("engineID", id))
			}
			ti.firstErr.Set(err2)
			importTableCancelFn()
			break
		}
		en := ti.createEngine(importTableCtx, importTableCancelFn, engineCP.Chunks, dataEngine)
		en.asyncSort(ti, sortPool, indexEngine)
		engineCh <- en
		failpoint.Inject("SyncAfterSendEngine", func() {
			TestSyncCh <- struct{}{}
			<-TestSyncCh
		})
	}

	close(engineCh)
	return ingestGroup.Wait()
}

func (ti *TableImporter) createEngine(ctx context.Context, cancel context.CancelFunc, chunks []*checkpoints.ChunkCheckpoint,
	openedEn *backend.OpenedEngine) *engine {
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(int(ti.ThreadCnt))
	en := &engine{
		importTableCancel: cancel,
		chunks:            chunks,
		logger:            ti.logger.With(zap.Int32("engine-id", openedEn.GetID())),
		dataEngine:        openedEn,
		sortEG:            eg,
		sortEGCtx:         egCtx,
	}
	en.sortTask = log.BeginTask(en.logger, "sort engine")
	return en
}

func (ti *TableImporter) ingestAndCleanupEngines(ctx context.Context, importTableCancelFn context.CancelFunc, engineCh chan *engine) error {
	for en := range engineCh {
		// only one engine can do ingest at a time.
		// since ingest already has a large concurrency inside.
		// we need to call ingestAndCleanup for each engine, since we need wait all its sub-routines to finish.
		if err := en.ingestAndCleanup(ctx, ti); err != nil {
			ti.firstErr.Set(err)
			// cancel goroutines used for sorting
			importTableCancelFn()
			failpoint.Inject("SetImportCancelledOnErr", func() {
				TestImportCancelledOnErr.Store(true)
			})
		}
	}
	return ti.firstErr.Get()
}

// IngestAndCleanup imports the engine and cleanup the engine data.
func (ti *TableImporter) IngestAndCleanup(ctx context.Context, closedEngine *backend.ClosedEngine) error {
	failpoint.Inject("IngestAndCleanupError", func() {
		failpoint.Return(errors.New("mock ingest fail"))
	})
	importErr := closedEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys)
	if closedEngine.GetID() != common.IndexEngineID {
		// todo: change to a finer-grain progress later.
		// each row is encoded into 1 data key
		kvCount := ti.backend.GetImportedKVCount(closedEngine.GetUUID())
		ti.Progress.LoadedRowCnt.Add(uint64(kvCount))
	}
	// todo: if we need support checkpoint, engine should not be cleanup if import failed.
	cleanupErr := closedEngine.Cleanup(ctx)
	return multierr.Combine(importErr, cleanupErr)
}

// Close implements the io.Closer interface.
func (ti *TableImporter) Close() error {
	ti.backend.Close()
	return nil
}

func (ti *TableImporter) getLastInsertID() uint64 {
	ti.fieldMu.Lock()
	defer ti.fieldMu.Unlock()
	return ti.fieldMu.lastInsertID
}

func (ti *TableImporter) setLastInsertID(id uint64) {
	if id == 0 {
		return
	}
	ti.fieldMu.Lock()
	defer ti.fieldMu.Unlock()
	if ti.fieldMu.lastInsertID == 0 || id < ti.fieldMu.lastInsertID {
		ti.fieldMu.lastInsertID = id
	}
}
