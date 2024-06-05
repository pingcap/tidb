// Copyright 2021 PingCAP, Inc.
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
	"cmp"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/lightning/pkg/web"
	"github.com/pingcap/tidb/pkg/errno"
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
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/extsort"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TableImporter is a helper struct to import a table.
type TableImporter struct {
	// The unique table name in the form "`db`.`tbl`".
	tableName string
	dbInfo    *checkpoints.TidbDBInfo
	tableInfo *checkpoints.TidbTableInfo
	tableMeta *mydump.MDTableMeta
	encTable  table.Table
	alloc     autoid.Allocators
	logger    log.Logger
	kvStore   tidbkv.Storage
	etcdCli   *clientv3.Client
	autoidCli *autoid.ClientDiscover

	// dupIgnoreRows tracks the rowIDs of rows that are duplicated and should be ignored.
	dupIgnoreRows extsort.ExternalSorter

	ignoreColumns map[string]struct{}
}

// NewTableImporter creates a new TableImporter.
func NewTableImporter(
	tableName string,
	tableMeta *mydump.MDTableMeta,
	dbInfo *checkpoints.TidbDBInfo,
	tableInfo *checkpoints.TidbTableInfo,
	cp *checkpoints.TableCheckpoint,
	ignoreColumns map[string]struct{},
	kvStore tidbkv.Storage,
	etcdCli *clientv3.Client,
	logger log.Logger,
) (*TableImporter, error) {
	idAlloc := kv.NewPanickingAllocators(tableInfo.Core.SepAutoInc(), cp.AllocBase)
	tbl, err := tables.TableFromMeta(idAlloc, tableInfo.Core)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", tableName)
	}
	autoidCli := autoid.NewClientDiscover(etcdCli)

	return &TableImporter{
		tableName:     tableName,
		dbInfo:        dbInfo,
		tableInfo:     tableInfo,
		tableMeta:     tableMeta,
		encTable:      tbl,
		alloc:         idAlloc,
		kvStore:       kvStore,
		etcdCli:       etcdCli,
		autoidCli:     autoidCli,
		logger:        logger.With(zap.String("table", tableName)),
		ignoreColumns: ignoreColumns,
	}, nil
}

func (tr *TableImporter) importTable(
	ctx context.Context,
	rc *Controller,
	cp *checkpoints.TableCheckpoint,
) (bool, error) {
	// 1. Load the table info.
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	metaMgr := rc.metaMgrBuilder.TableMetaMgr(tr)
	// no need to do anything if the chunks are already populated
	if len(cp.Engines) > 0 {
		tr.logger.Info("reusing engines and files info from checkpoint",
			zap.Int("enginesCnt", len(cp.Engines)),
			zap.Int("filesCnt", cp.CountChunks()),
		)
		err := addExtendDataForCheckpoint(ctx, rc.cfg, cp)
		if err != nil {
			return false, errors.Trace(err)
		}
	} else if cp.Status < checkpoints.CheckpointStatusAllWritten {
		if err := tr.populateChunks(ctx, rc, cp); err != nil {
			return false, errors.Trace(err)
		}

		// fetch the max chunk row_id max value as the global max row_id
		rowIDMax := int64(0)
		for _, engine := range cp.Engines {
			if len(engine.Chunks) > 0 && engine.Chunks[len(engine.Chunks)-1].Chunk.RowIDMax > rowIDMax {
				rowIDMax = engine.Chunks[len(engine.Chunks)-1].Chunk.RowIDMax
			}
		}
		versionStr, err := version.FetchVersion(ctx, rc.db)
		if err != nil {
			return false, errors.Trace(err)
		}

		versionInfo := version.ParseServerInfo(versionStr)

		// "show table next_row_id" is only available after tidb v4.0.0
		if versionInfo.ServerVersion.Major >= 4 && isLocalBackend(rc.cfg) {
			// first, insert a new-line into meta table
			if err = metaMgr.InitTableMeta(ctx); err != nil {
				return false, err
			}

			checksum, rowIDBase, err := metaMgr.AllocTableRowIDs(ctx, rowIDMax)
			if err != nil {
				return false, err
			}
			tr.RebaseChunkRowIDs(cp, rowIDBase)

			if checksum != nil {
				if cp.Checksum != *checksum {
					cp.Checksum = *checksum
					rc.saveCpCh <- saveCp{
						tableName: tr.tableName,
						merger: &checkpoints.TableChecksumMerger{
							Checksum: cp.Checksum,
						},
					}
				}
				tr.logger.Info("checksum before restore table", zap.Object("checksum", &cp.Checksum))
			}
		}
		if err := rc.checkpointsDB.InsertEngineCheckpoints(ctx, tr.tableName, cp.Engines); err != nil {
			return false, errors.Trace(err)
		}
		web.BroadcastTableCheckpoint(tr.tableName, cp)

		// rebase the allocator so it exceeds the number of rows.
		if tr.tableInfo.Core.ContainsAutoRandomBits() {
			cp.AllocBase = max(cp.AllocBase, tr.tableInfo.Core.AutoRandID)
			if err := tr.alloc.Get(autoid.AutoRandomType).Rebase(context.Background(), cp.AllocBase, false); err != nil {
				return false, err
			}
		} else {
			cp.AllocBase = max(cp.AllocBase, tr.tableInfo.Core.AutoIncID)
			if err := tr.alloc.Get(autoid.RowIDAllocType).Rebase(context.Background(), cp.AllocBase, false); err != nil {
				return false, err
			}
		}
		rc.saveCpCh <- saveCp{
			tableName: tr.tableName,
			merger: &checkpoints.RebaseCheckpointMerger{
				AllocBase: cp.AllocBase,
			},
		}
	}

	// 2. Do duplicate detection if needed
	if isLocalBackend(rc.cfg) && rc.cfg.Conflict.PrecheckConflictBeforeImport && rc.cfg.Conflict.Strategy != config.NoneOnDup {
		_, uuid := backend.MakeUUID(tr.tableName, common.IndexEngineID)
		workingDir := filepath.Join(rc.cfg.TikvImporter.SortedKVDir, uuid.String()+local.DupDetectDirSuffix)
		resultDir := filepath.Join(rc.cfg.TikvImporter.SortedKVDir, uuid.String()+local.DupResultDirSuffix)

		dupIgnoreRows, err := extsort.OpenDiskSorter(resultDir, &extsort.DiskSorterOptions{
			Concurrency: rc.cfg.App.RegionConcurrency,
		})
		if err != nil {
			return false, errors.Trace(err)
		}
		tr.dupIgnoreRows = dupIgnoreRows

		if cp.Status < checkpoints.CheckpointStatusDupDetected {
			err := tr.preDeduplicate(ctx, rc, cp, workingDir)
			saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusDupDetected)
			if err := firstErr(err, saveCpErr); err != nil {
				return false, errors.Trace(err)
			}
		}

		if !dupIgnoreRows.IsSorted() {
			if err := dupIgnoreRows.Sort(ctx); err != nil {
				return false, errors.Trace(err)
			}
		}

		failpoint.Inject("FailAfterDuplicateDetection", func() {
			panic("forcing failure after duplicate detection")
		})
	}

	// 3. Drop indexes if add-index-by-sql is enabled
	if cp.Status < checkpoints.CheckpointStatusIndexDropped && isLocalBackend(rc.cfg) && rc.cfg.TikvImporter.AddIndexBySQL {
		err := tr.dropIndexes(ctx, rc.db)
		saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusIndexDropped)
		if err := firstErr(err, saveCpErr); err != nil {
			return false, errors.Trace(err)
		}
	}

	// 4. Restore engines (if still needed)
	err := tr.importEngines(ctx, rc, cp)
	if err != nil {
		return false, errors.Trace(err)
	}

	err = metaMgr.UpdateTableStatus(ctx, metaStatusRestoreFinished)
	if err != nil {
		return false, errors.Trace(err)
	}

	// 5. Post-process. With the last parameter set to false, we can allow delay analyze execute latter
	return tr.postProcess(ctx, rc, cp, false /* force-analyze */, metaMgr)
}

// Close implements the Importer interface.
func (tr *TableImporter) Close() {
	tr.encTable = nil
	if tr.dupIgnoreRows != nil {
		_ = tr.dupIgnoreRows.Close()
	}
	tr.logger.Info("restore done")
}

func (tr *TableImporter) populateChunks(ctx context.Context, rc *Controller, cp *checkpoints.TableCheckpoint) error {
	task := tr.logger.Begin(zap.InfoLevel, "load engines and files")
	divideConfig := mydump.NewDataDivideConfig(rc.cfg, len(tr.tableInfo.Core.Columns), rc.ioWorkers, rc.store, tr.tableMeta)
	tableRegions, err := mydump.MakeTableRegions(ctx, divideConfig)
	if err == nil {
		timestamp := time.Now().Unix()
		failpoint.Inject("PopulateChunkTimestamp", func(v failpoint.Value) {
			timestamp = int64(v.(int))
		})
		for _, region := range tableRegions {
			engine, found := cp.Engines[region.EngineID]
			if !found {
				engine = &checkpoints.EngineCheckpoint{
					Status: checkpoints.CheckpointStatusLoaded,
				}
				cp.Engines[region.EngineID] = engine
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
			if len(region.Chunk.Columns) > 0 {
				perms, err := parseColumnPermutations(
					tr.tableInfo.Core,
					region.Chunk.Columns,
					tr.ignoreColumns,
					log.FromContext(ctx))
				if err != nil {
					return errors.Trace(err)
				}
				ccp.ColumnPermutation = perms
			}
			engine.Chunks = append(engine.Chunks, ccp)
		}

		// Add index engine checkpoint
		cp.Engines[common.IndexEngineID] = &checkpoints.EngineCheckpoint{Status: checkpoints.CheckpointStatusLoaded}
	}
	task.End(zap.ErrorLevel, err,
		zap.Int("enginesCnt", len(cp.Engines)),
		zap.Int("filesCnt", len(tableRegions)),
	)
	return err
}

// AutoIDRequirement implements autoid.Requirement.
var _ autoid.Requirement = &TableImporter{}

// Store implements the autoid.Requirement interface.
func (tr *TableImporter) Store() tidbkv.Storage {
	return tr.kvStore
}

// AutoIDClient implements the autoid.Requirement interface.
func (tr *TableImporter) AutoIDClient() *autoid.ClientDiscover {
	return tr.autoidCli
}

// RebaseChunkRowIDs rebase the row id of the chunks.
func (*TableImporter) RebaseChunkRowIDs(cp *checkpoints.TableCheckpoint, rowIDBase int64) {
	if rowIDBase == 0 {
		return
	}
	for _, engine := range cp.Engines {
		for _, chunk := range engine.Chunks {
			chunk.Chunk.PrevRowIDMax += rowIDBase
			chunk.Chunk.RowIDMax += rowIDBase
		}
	}
}

// initializeColumns computes the "column permutation" for an INSERT INTO
// statement. Suppose a table has columns (a, b, c, d) in canonical order, and
// we execute `INSERT INTO (d, b, a) VALUES ...`, we will need to remap the
// columns as:
//
// - column `a` is at position 2
// - column `b` is at position 1
// - column `c` is missing
// - column `d` is at position 0
//
// The column permutation of (d, b, a) is set to be [2, 1, -1, 0].
//
// The argument `columns` _must_ be in lower case.
func (tr *TableImporter) initializeColumns(columns []string, ccp *checkpoints.ChunkCheckpoint) error {
	colPerm, err := createColumnPermutation(columns, tr.ignoreColumns, tr.tableInfo.Core, tr.logger)
	if err != nil {
		return err
	}
	ccp.ColumnPermutation = colPerm
	return nil
}

func createColumnPermutation(
	columns []string,
	ignoreColumns map[string]struct{},
	tableInfo *model.TableInfo,
	logger log.Logger,
) ([]int, error) {
	var colPerm []int
	if len(columns) == 0 {
		colPerm = make([]int, 0, len(tableInfo.Columns)+1)
		shouldIncludeRowID := common.TableHasAutoRowID(tableInfo)

		// no provided columns, so use identity permutation.
		for i, col := range tableInfo.Columns {
			idx := i
			if _, ok := ignoreColumns[col.Name.L]; ok {
				idx = -1
			} else if col.IsGenerated() {
				idx = -1
			}
			colPerm = append(colPerm, idx)
		}
		if shouldIncludeRowID {
			colPerm = append(colPerm, -1)
		}
	} else {
		var err error
		colPerm, err = parseColumnPermutations(tableInfo, columns, ignoreColumns, logger)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return colPerm, nil
}

func (tr *TableImporter) importEngines(pCtx context.Context, rc *Controller, cp *checkpoints.TableCheckpoint) error {
	indexEngineCp := cp.Engines[common.IndexEngineID]
	if indexEngineCp == nil {
		tr.logger.Error("fail to importEngines because indexengine is nil")
		return common.ErrCheckpointNotFound.GenWithStack("table %v index engine checkpoint not found", tr.tableName)
	}

	ctx, cancel := context.WithCancel(pCtx)
	defer cancel()

	// The table checkpoint status set to `CheckpointStatusIndexImported` only if
	// both all data engines and the index engine had been imported to TiKV.
	// But persist index engine checkpoint status and table checkpoint status are
	// not an atomic operation, so `cp.Status < CheckpointStatusIndexImported`
	// but `indexEngineCp.Status == CheckpointStatusImported` could happen
	// when kill lightning after saving index engine checkpoint status before saving
	// table checkpoint status.
	var closedIndexEngine *backend.ClosedEngine
	var restoreErr error
	// if index-engine checkpoint is lower than `CheckpointStatusClosed`, there must be
	// data-engines that need to be restore or import. Otherwise, all data-engines should
	// be finished already.

	handleDataEngineThisRun := false
	idxEngineCfg := &backend.EngineConfig{
		TableInfo: tr.tableInfo,
	}
	if indexEngineCp.Status < checkpoints.CheckpointStatusClosed {
		handleDataEngineThisRun = true
		indexWorker := rc.indexWorkers.Apply()
		defer rc.indexWorkers.Recycle(indexWorker)

		if rc.cfg.TikvImporter.Backend == config.BackendLocal {
			// for index engine, the estimate factor is non-clustered index count
			idxCnt := len(tr.tableInfo.Core.Indices)
			if !common.TableHasAutoRowID(tr.tableInfo.Core) {
				idxCnt--
			}
			threshold := local.EstimateCompactionThreshold(tr.tableMeta.DataFiles, cp, int64(idxCnt))
			idxEngineCfg.Local = backend.LocalEngineConfig{
				Compact:            threshold > 0,
				CompactConcurrency: 4,
				CompactThreshold:   threshold,
				BlockSize:          int(rc.cfg.TikvImporter.BlockSize),
			}
		}
		// import backend can't reopen engine if engine is closed, so
		// only open index engine if any data engines don't finish writing.
		var indexEngine *backend.OpenedEngine
		var err error
		for engineID, engine := range cp.Engines {
			if engineID == common.IndexEngineID {
				continue
			}
			if engine.Status < checkpoints.CheckpointStatusAllWritten {
				indexEngine, err = rc.engineMgr.OpenEngine(ctx, idxEngineCfg, tr.tableName, common.IndexEngineID)
				if err != nil {
					return errors.Trace(err)
				}
				break
			}
		}

		logTask := tr.logger.Begin(zap.InfoLevel, "import whole table")
		var wg sync.WaitGroup
		var engineErr common.OnceError
		setError := func(err error) {
			engineErr.Set(err)
			// cancel this context to fail fast
			cancel()
		}

		type engineCheckpoint struct {
			engineID   int32
			checkpoint *checkpoints.EngineCheckpoint
		}
		allEngines := make([]engineCheckpoint, 0, len(cp.Engines))
		for engineID, engine := range cp.Engines {
			allEngines = append(allEngines, engineCheckpoint{engineID: engineID, checkpoint: engine})
		}
		slices.SortFunc(allEngines, func(i, j engineCheckpoint) int { return cmp.Compare(i.engineID, j.engineID) })

		for _, ecp := range allEngines {
			engineID := ecp.engineID
			engine := ecp.checkpoint
			select {
			case <-ctx.Done():
				// Set engineErr and break this for loop to wait all the sub-routines done before return.
				// Directly return may cause panic because caller will close the pebble db but some sub routines
				// are still reading from or writing to the pebble db.
				engineErr.Set(ctx.Err())
			default:
			}
			if engineErr.Get() != nil {
				break
			}

			// Should skip index engine
			if engineID < 0 {
				continue
			}

			if engine.Status < checkpoints.CheckpointStatusImported {
				wg.Add(1)

				// If the number of chunks is small, it means that this engine may be finished in a few times.
				// We do not limit it in TableConcurrency
				restoreWorker := rc.tableWorkers.Apply()
				go func(w *worker.Worker, eid int32, ecp *checkpoints.EngineCheckpoint) {
					defer wg.Done()
					engineLogTask := tr.logger.With(zap.Int32("engineNumber", eid)).Begin(zap.InfoLevel, "restore engine")
					dataClosedEngine, err := tr.preprocessEngine(ctx, rc, indexEngine, eid, ecp)
					engineLogTask.End(zap.ErrorLevel, err)
					rc.tableWorkers.Recycle(w)
					if err == nil {
						dataWorker := rc.closedEngineLimit.Apply()
						defer rc.closedEngineLimit.Recycle(dataWorker)
						err = tr.importEngine(ctx, dataClosedEngine, rc, ecp)
						if rc.status != nil && rc.status.backend == config.BackendLocal {
							for _, chunk := range ecp.Chunks {
								rc.status.FinishedFileSize.Add(chunk.TotalSize())
							}
						}
					}
					if err != nil {
						setError(err)
					}
				}(restoreWorker, engineID, engine)
			} else {
				for _, chunk := range engine.Chunks {
					rc.status.FinishedFileSize.Add(chunk.TotalSize())
				}
			}
		}

		wg.Wait()

		restoreErr = engineErr.Get()
		logTask.End(zap.ErrorLevel, restoreErr)
		if restoreErr != nil {
			return errors.Trace(restoreErr)
		}

		if indexEngine != nil {
			closedIndexEngine, restoreErr = indexEngine.Close(ctx)
		} else {
			closedIndexEngine, restoreErr = rc.engineMgr.UnsafeCloseEngine(ctx, idxEngineCfg, tr.tableName, common.IndexEngineID)
		}

		if err = rc.saveStatusCheckpoint(ctx, tr.tableName, common.IndexEngineID, restoreErr, checkpoints.CheckpointStatusClosed); err != nil {
			return errors.Trace(firstErr(restoreErr, err))
		}
	} else if indexEngineCp.Status == checkpoints.CheckpointStatusClosed {
		// If index engine file has been closed but not imported only if context cancel occurred
		// when `importKV()` execution, so `UnsafeCloseEngine` and continue import it.
		closedIndexEngine, restoreErr = rc.engineMgr.UnsafeCloseEngine(ctx, idxEngineCfg, tr.tableName, common.IndexEngineID)
	}
	if restoreErr != nil {
		return errors.Trace(restoreErr)
	}

	// if data engine is handled in previous run and we continue importing from checkpoint
	if !handleDataEngineThisRun {
		for _, engine := range cp.Engines {
			for _, chunk := range engine.Chunks {
				rc.status.FinishedFileSize.Add(chunk.Chunk.EndOffset - chunk.Key.Offset)
			}
		}
	}

	if cp.Status < checkpoints.CheckpointStatusIndexImported {
		var err error
		if indexEngineCp.Status < checkpoints.CheckpointStatusImported {
			failpoint.Inject("FailBeforeStartImportingIndexEngine", func() {
				errMsg := "fail before importing index KV data"
				tr.logger.Warn(errMsg)
				failpoint.Return(errors.New(errMsg))
			})
			err = tr.importKV(ctx, closedIndexEngine, rc)
			failpoint.Inject("FailBeforeIndexEngineImported", func() {
				finished := rc.status.FinishedFileSize.Load()
				total := rc.status.TotalFileSize.Load()
				tr.logger.Warn("print lightning status",
					zap.Int64("finished", finished),
					zap.Int64("total", total),
					zap.Bool("equal", finished == total))
				panic("forcing failure due to FailBeforeIndexEngineImported")
			})
		}

		saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusIndexImported)
		if err = firstErr(err, saveCpErr); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// preprocessEngine do some preprocess work
// for local backend, it do local sort, for tidb backend it transforms data into sql and execute
// TODO: it's not a correct name for tidb backend, since there's no post-process for it
// TODO: after separate local/tidb backend more clearly, rename it.
func (tr *TableImporter) preprocessEngine(
	pCtx context.Context,
	rc *Controller,
	indexEngine *backend.OpenedEngine,
	engineID int32,
	cp *checkpoints.EngineCheckpoint,
) (*backend.ClosedEngine, error) {
	ctx, cancel := context.WithCancel(pCtx)
	defer cancel()
	// all data has finished written, we can close the engine directly.
	if cp.Status >= checkpoints.CheckpointStatusAllWritten {
		engineCfg := &backend.EngineConfig{
			TableInfo: tr.tableInfo,
		}
		closedEngine, err := rc.engineMgr.UnsafeCloseEngine(ctx, engineCfg, tr.tableName, engineID)
		// If any error occurred, recycle worker immediately
		if err != nil {
			return closedEngine, errors.Trace(err)
		}
		if rc.status != nil && rc.status.backend == config.BackendTiDB {
			for _, chunk := range cp.Chunks {
				rc.status.FinishedFileSize.Add(chunk.Chunk.EndOffset - chunk.Key.Offset)
			}
		}
		return closedEngine, nil
	}

	// if the key are ordered, LocalWrite can optimize the writing.
	// table has auto-incremented _tidb_rowid must satisfy following restrictions:
	// - clustered index disable and primary key is not number
	// - no auto random bits (auto random or shard row id)
	// - no partition table
	// - no explicit _tidb_rowid field (At this time we can't determine if the source file contains _tidb_rowid field,
	//   so we will do this check in LocalWriter when the first row is received.)
	hasAutoIncrementAutoID := common.TableHasAutoRowID(tr.tableInfo.Core) &&
		tr.tableInfo.Core.AutoRandomBits == 0 && tr.tableInfo.Core.ShardRowIDBits == 0 &&
		tr.tableInfo.Core.Partition == nil
	dataWriterCfg := &backend.LocalWriterConfig{
		IsKVSorted: hasAutoIncrementAutoID,
		TableName:  tr.tableName,
	}

	logTask := tr.logger.With(zap.Int32("engineNumber", engineID)).Begin(zap.InfoLevel, "encode kv data and write")
	dataEngineCfg := &backend.EngineConfig{
		TableInfo: tr.tableInfo,
	}
	if !tr.tableMeta.IsRowOrdered {
		dataEngineCfg.Local.Compact = true
		dataEngineCfg.Local.CompactConcurrency = 4
		dataEngineCfg.Local.CompactThreshold = local.CompactionUpperThreshold
	}
	dataEngine, err := rc.engineMgr.OpenEngine(ctx, dataEngineCfg, tr.tableName, engineID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var wg sync.WaitGroup
	var chunkErr common.OnceError

	type chunkFlushStatus struct {
		dataStatus  backend.ChunkFlushStatus
		indexStatus backend.ChunkFlushStatus
		chunkCp     *checkpoints.ChunkCheckpoint
	}

	// chunks that are finished writing, but checkpoints are not finished due to flush not finished.
	var checkFlushLock sync.Mutex
	flushPendingChunks := make([]chunkFlushStatus, 0, 16)

	chunkCpChan := make(chan *checkpoints.ChunkCheckpoint, 16)
	go func() {
		for {
			select {
			case cp, ok := <-chunkCpChan:
				if !ok {
					return
				}
				saveCheckpoint(rc, tr, engineID, cp)
			case <-ctx.Done():
				return
			}
		}
	}()

	setError := func(err error) {
		chunkErr.Set(err)
		cancel()
	}

	metrics, _ := metric.FromContext(ctx)

	// Restore table data
ChunkLoop:
	for chunkIndex, chunk := range cp.Chunks {
		if rc.status != nil && rc.status.backend == config.BackendTiDB {
			rc.status.FinishedFileSize.Add(chunk.Chunk.Offset - chunk.Key.Offset)
		}
		if chunk.Chunk.Offset >= chunk.Chunk.EndOffset {
			continue
		}

		checkFlushLock.Lock()
		finished := 0
		for _, c := range flushPendingChunks {
			if !(c.indexStatus.Flushed() && c.dataStatus.Flushed()) {
				break
			}
			chunkCpChan <- c.chunkCp
			finished++
		}
		if finished > 0 {
			flushPendingChunks = flushPendingChunks[finished:]
		}
		checkFlushLock.Unlock()

		failpoint.Inject("orphanWriterGoRoutine", func() {
			if chunkIndex > 0 {
				<-pCtx.Done()
			}
		})

		select {
		case <-pCtx.Done():
			break ChunkLoop
		default:
		}

		if chunkErr.Get() != nil {
			break
		}

		// Flows :
		// 	1. read mydump file
		// 	2. sql -> kvs
		// 	3. load kvs data (into kv deliver server)
		// 	4. flush kvs data (into tikv node)
		var remainChunkCnt float64
		if chunk.Chunk.Offset < chunk.Chunk.EndOffset {
			remainChunkCnt = float64(chunk.UnfinishedSize()) / float64(chunk.TotalSize())
			if metrics != nil {
				metrics.ChunkCounter.WithLabelValues(metric.ChunkStatePending).Add(remainChunkCnt)
			}
		}

		dataWriter, err := dataEngine.LocalWriter(ctx, dataWriterCfg)
		if err != nil {
			setError(err)
			break
		}

		indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{TableName: tr.tableName})
		if err != nil {
			_, _ = dataWriter.Close(ctx)
			setError(err)
			break
		}
		cr, err := newChunkProcessor(ctx, chunkIndex, rc.cfg, chunk, rc.ioWorkers, rc.store, tr.tableInfo.Core)
		if err != nil {
			setError(err)
			break
		}

		if chunk.FileMeta.Type == mydump.SourceTypeParquet {
			// TODO: use the compressed size of the chunk to conduct memory control
			if _, err = getChunkCompressedSizeForParquet(ctx, chunk, rc.store); err != nil {
				return nil, errors.Trace(err)
			}
		}

		restoreWorker := rc.regionWorkers.Apply()
		wg.Add(1)
		go func(w *worker.Worker, cr *chunkProcessor) {
			// Restore a chunk.
			defer func() {
				cr.close()
				wg.Done()
				rc.regionWorkers.Recycle(w)
			}()
			if metrics != nil {
				metrics.ChunkCounter.WithLabelValues(metric.ChunkStateRunning).Add(remainChunkCnt)
			}
			err := cr.process(ctx, tr, engineID, dataWriter, indexWriter, rc)
			var dataFlushStatus, indexFlushStaus backend.ChunkFlushStatus
			if err == nil {
				dataFlushStatus, err = dataWriter.Close(ctx)
			}
			if err == nil {
				indexFlushStaus, err = indexWriter.Close(ctx)
			}
			if err == nil {
				if metrics != nil {
					metrics.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Add(remainChunkCnt)
					metrics.BytesCounter.WithLabelValues(metric.StateRestoreWritten).Add(float64(cr.chunk.Checksum.SumSize()))
				}
				if dataFlushStatus != nil && indexFlushStaus != nil {
					if dataFlushStatus.Flushed() && indexFlushStaus.Flushed() {
						saveCheckpoint(rc, tr, engineID, cr.chunk)
					} else {
						checkFlushLock.Lock()
						flushPendingChunks = append(flushPendingChunks, chunkFlushStatus{
							dataStatus:  dataFlushStatus,
							indexStatus: indexFlushStaus,
							chunkCp:     cr.chunk,
						})
						checkFlushLock.Unlock()
					}
				}
			} else {
				if metrics != nil {
					metrics.ChunkCounter.WithLabelValues(metric.ChunkStateFailed).Add(remainChunkCnt)
				}
				setError(err)
			}
		}(restoreWorker, cr)
	}

	wg.Wait()
	select {
	case <-pCtx.Done():
		return nil, pCtx.Err()
	default:
	}

	// Report some statistics into the log for debugging.
	totalKVSize := uint64(0)
	totalSQLSize := int64(0)
	logKeyName := "read(bytes)"
	for _, chunk := range cp.Chunks {
		totalKVSize += chunk.Checksum.SumSize()
		totalSQLSize += chunk.UnfinishedSize()
		if chunk.FileMeta.Type == mydump.SourceTypeParquet {
			logKeyName = "read(rows)"
		}
	}

	err = chunkErr.Get()
	logTask.End(zap.ErrorLevel, err,
		zap.Int64(logKeyName, totalSQLSize),
		zap.Uint64("written", totalKVSize),
	)

	trySavePendingChunks := func(context.Context) error {
		checkFlushLock.Lock()
		cnt := 0
		for _, chunk := range flushPendingChunks {
			if !(chunk.dataStatus.Flushed() && chunk.indexStatus.Flushed()) {
				break
			}
			saveCheckpoint(rc, tr, engineID, chunk.chunkCp)
			cnt++
		}
		flushPendingChunks = flushPendingChunks[cnt:]
		checkFlushLock.Unlock()
		return nil
	}

	// in local mode, this check-point make no sense, because we don't do flush now,
	// so there may be data lose if exit at here. So we don't write this checkpoint
	// here like other mode.
	if !isLocalBackend(rc.cfg) {
		if saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, engineID, err, checkpoints.CheckpointStatusAllWritten); saveCpErr != nil {
			return nil, errors.Trace(firstErr(err, saveCpErr))
		}
	}
	if err != nil {
		// if process is canceled, we should flush all chunk checkpoints for local backend
		if isLocalBackend(rc.cfg) && common.IsContextCanceledError(err) {
			// ctx is canceled, so to avoid Close engine failed, we use `context.Background()` here
			if _, err2 := dataEngine.Close(context.Background()); err2 != nil {
				log.FromContext(ctx).Warn("flush all chunk checkpoints failed before manually exits", zap.Error(err2))
				return nil, errors.Trace(err)
			}
			if err2 := trySavePendingChunks(context.Background()); err2 != nil {
				log.FromContext(ctx).Warn("flush all chunk checkpoints failed before manually exits", zap.Error(err2))
			}
		}
		return nil, errors.Trace(err)
	}

	closedDataEngine, err := dataEngine.Close(ctx)
	// For local backend, if checkpoint is enabled, we must flush index engine to avoid data loss.
	// this flush action impact up to 10% of the performance, so we only do it if necessary.
	if err == nil && rc.cfg.Checkpoint.Enable && isLocalBackend(rc.cfg) {
		if err = indexEngine.Flush(ctx); err != nil {
			return nil, errors.Trace(err)
		}
		if err = trySavePendingChunks(ctx); err != nil {
			return nil, errors.Trace(err)
		}
	}
	saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, engineID, err, checkpoints.CheckpointStatusClosed)
	if err = firstErr(err, saveCpErr); err != nil {
		// If any error occurred, recycle worker immediately
		return nil, errors.Trace(err)
	}
	return closedDataEngine, nil
}

func (tr *TableImporter) importEngine(
	ctx context.Context,
	closedEngine *backend.ClosedEngine,
	rc *Controller,
	cp *checkpoints.EngineCheckpoint,
) error {
	if cp.Status >= checkpoints.CheckpointStatusImported {
		return nil
	}

	// 1. calling import
	if err := tr.importKV(ctx, closedEngine, rc); err != nil {
		return errors.Trace(err)
	}

	// 2. perform a level-1 compact if idling.
	if rc.cfg.PostRestore.Level1Compact && rc.compactState.CompareAndSwap(compactStateIdle, compactStateDoing) {
		go func() {
			// we ignore level-1 compact failure since it is not fatal.
			// no need log the error, it is done in (*Importer).Compact already.
			_ = rc.doCompact(ctx, Level1Compact)
			rc.compactState.Store(compactStateIdle)
		}()
	}

	return nil
}

// postProcess execute rebase-auto-id/checksum/analyze according to the task config.
//
// if the parameter forcePostProcess to true, postProcess force run checksum and analyze even if the
// post-process-at-last config is true. And if this two phases are skipped, the first return value will be true.
func (tr *TableImporter) postProcess(
	ctx context.Context,
	rc *Controller,
	cp *checkpoints.TableCheckpoint,
	forcePostProcess bool,
	metaMgr tableMetaMgr,
) (bool, error) {
	if !rc.backend.ShouldPostProcess() {
		return false, nil
	}

	// alter table set auto_increment
	if cp.Status < checkpoints.CheckpointStatusAlteredAutoInc {
		tblInfo := tr.tableInfo.Core
		var err error
		// TODO why we have to rebase id for tidb backend??? remove it later.
		if tblInfo.ContainsAutoRandomBits() {
			ft := &common.GetAutoRandomColumn(tblInfo).FieldType
			shardFmt := autoid.NewShardIDFormat(ft, tblInfo.AutoRandomBits, tblInfo.AutoRandomRangeBits)
			maxCap := shardFmt.IncrementalBitsCapacity()
			err = AlterAutoRandom(ctx, rc.db, tr.tableName, uint64(tr.alloc.Get(autoid.AutoRandomType).Base())+1, maxCap)
		} else if common.TableHasAutoRowID(tblInfo) || tblInfo.GetAutoIncrementColInfo() != nil {
			if isLocalBackend(rc.cfg) {
				// for TiDB version >= 6.5.0, a table might have separate allocators for auto_increment column and _tidb_rowid,
				// especially when a table has auto_increment non-clustered PK, it will use both allocators.
				// And in this case, ALTER TABLE xxx AUTO_INCREMENT = xxx only works on the allocator of auto_increment column,
				// not for allocator of _tidb_rowid.
				// So we need to rebase IDs for those 2 allocators explicitly.
				err = common.RebaseTableAllocators(ctx, map[autoid.AllocatorType]int64{
					autoid.RowIDAllocType:    tr.alloc.Get(autoid.RowIDAllocType).Base(),
					autoid.AutoIncrementType: tr.alloc.Get(autoid.AutoIncrementType).Base(),
				}, tr, tr.dbInfo.ID, tr.tableInfo.Core)
			} else {
				// only alter auto increment id iff table contains auto-increment column or generated handle.
				// ALTER TABLE xxx AUTO_INCREMENT = yyy has a bad naming.
				// if a table has implicit _tidb_rowid column & tbl.SepAutoID=false, then it works on _tidb_rowid
				// allocator, even if the table has NO auto-increment column.
				newBase := uint64(tr.alloc.Get(autoid.RowIDAllocType).Base()) + 1
				err = AlterAutoIncrement(ctx, rc.db, tr.tableName, newBase)
			}
		}
		saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusAlteredAutoInc)
		if err = firstErr(err, saveCpErr); err != nil {
			return false, errors.Trace(err)
		}
		cp.Status = checkpoints.CheckpointStatusAlteredAutoInc
	}

	// tidb backend don't need checksum & analyze
	if rc.cfg.PostRestore.Checksum == config.OpLevelOff && rc.cfg.PostRestore.Analyze == config.OpLevelOff {
		tr.logger.Debug("skip checksum & analyze, either because not supported by this backend or manually disabled")
		err := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, nil, checkpoints.CheckpointStatusAnalyzeSkipped)
		return false, errors.Trace(err)
	}

	if !forcePostProcess && rc.cfg.PostRestore.PostProcessAtLast {
		return true, nil
	}

	w := rc.checksumWorks.Apply()
	defer rc.checksumWorks.Recycle(w)

	shouldSkipAnalyze := false
	estimatedModifyCnt := 100_000_000
	if cp.Status < checkpoints.CheckpointStatusChecksumSkipped {
		// 4. do table checksum
		var localChecksum verify.KVChecksum
		for _, engine := range cp.Engines {
			for _, chunk := range engine.Chunks {
				localChecksum.Add(&chunk.Checksum)
			}
		}
		indexNum := len(tr.tableInfo.Core.Indices)
		if common.TableHasAutoRowID(tr.tableInfo.Core) {
			indexNum++
		}
		estimatedModifyCnt = int(localChecksum.SumKVS()) / (1 + indexNum)
		tr.logger.Info("local checksum", zap.Object("checksum", &localChecksum))

		// 4.5. do duplicate detection.
		// if we came here, it must be a local backend.
		// todo: remove this cast after we refactor the backend interface. Physical mode is so different, we shouldn't
		// try to abstract it with logical mode.
		localBackend := rc.backend.(*local.Backend)
		dupeController := localBackend.GetDupeController(rc.cfg.TikvImporter.RangeConcurrency*2, rc.errorMgr)
		hasDupe := false
		if rc.cfg.Conflict.Strategy != config.NoneOnDup {
			opts := &encode.SessionOptions{
				SQLMode: mysql.ModeStrictAllTables,
				SysVars: rc.sysVars,
			}
			var err error
			hasLocalDupe, err := dupeController.CollectLocalDuplicateRows(ctx, tr.encTable, tr.tableName, opts, rc.cfg.Conflict.Strategy)
			if err != nil {
				tr.logger.Error("collect local duplicate keys failed", log.ShortError(err))
				return false, errors.Trace(err)
			}
			hasDupe = hasLocalDupe
		}
		failpoint.Inject("SlowDownCheckDupe", func(v failpoint.Value) {
			sec := v.(int)
			tr.logger.Warn("start to sleep several seconds before checking other dupe",
				zap.Int("seconds", sec))
			time.Sleep(time.Duration(sec) * time.Second)
		})

		otherHasDupe, needRemoteDupe, baseTotalChecksum, err := metaMgr.CheckAndUpdateLocalChecksum(ctx, &localChecksum, hasDupe)
		if err != nil {
			return false, errors.Trace(err)
		}
		needChecksum := !otherHasDupe && needRemoteDupe
		hasDupe = hasDupe || otherHasDupe

		if needRemoteDupe && rc.cfg.Conflict.Strategy != config.NoneOnDup {
			opts := &encode.SessionOptions{
				SQLMode: mysql.ModeStrictAllTables,
				SysVars: rc.sysVars,
			}
			hasRemoteDupe, e := dupeController.CollectRemoteDuplicateRows(ctx, tr.encTable, tr.tableName, opts, rc.cfg.Conflict.Strategy)
			if e != nil {
				tr.logger.Error("collect remote duplicate keys failed", log.ShortError(e))
				return false, errors.Trace(e)
			}
			hasDupe = hasDupe || hasRemoteDupe

			if hasDupe {
				if err = dupeController.ResolveDuplicateRows(ctx, tr.encTable, tr.tableName, rc.cfg.Conflict.Strategy); err != nil {
					tr.logger.Error("resolve remote duplicate keys failed", log.ShortError(err))
					return false, errors.Trace(err)
				}
			}
		}

		if rc.dupIndicator != nil {
			tr.logger.Debug("set dupIndicator", zap.Bool("has-duplicate", hasDupe))
			rc.dupIndicator.CompareAndSwap(false, hasDupe)
		}

		nextStage := checkpoints.CheckpointStatusChecksummed
		if rc.cfg.PostRestore.Checksum != config.OpLevelOff && !hasDupe && needChecksum {
			if cp.Checksum.SumKVS() > 0 || baseTotalChecksum.SumKVS() > 0 {
				localChecksum.Add(&cp.Checksum)
				localChecksum.Add(baseTotalChecksum)
				tr.logger.Info("merged local checksum", zap.Object("checksum", &localChecksum))
			}

			var remoteChecksum *local.RemoteChecksum
			remoteChecksum, err = DoChecksum(ctx, tr.tableInfo)
			failpoint.Inject("checksum-error", func() {
				tr.logger.Info("failpoint checksum-error injected.")
				remoteChecksum = nil
				err = status.Error(codes.Unknown, "Checksum meets error.")
			})
			if err != nil {
				if rc.cfg.PostRestore.Checksum != config.OpLevelOptional {
					return false, errors.Trace(err)
				}
				tr.logger.Warn("do checksum failed, will skip this error and go on", log.ShortError(err))
				err = nil
			}
			if remoteChecksum != nil {
				err = tr.compareChecksum(remoteChecksum, localChecksum)
				// with post restore level 'optional', we will skip checksum error
				if rc.cfg.PostRestore.Checksum == config.OpLevelOptional {
					if err != nil {
						tr.logger.Warn("compare checksum failed, will skip this error and go on", log.ShortError(err))
						err = nil
					}
				}
			}
		} else {
			switch {
			case rc.cfg.PostRestore.Checksum == config.OpLevelOff:
				tr.logger.Info("skip checksum because the checksum option is off")
			case hasDupe:
				tr.logger.Info("skip checksum&analyze because duplicates were detected")
				shouldSkipAnalyze = true
			case !needChecksum:
				tr.logger.Info("skip checksum&analyze because other lightning instance will do this")
				shouldSkipAnalyze = true
			}
			err = nil
			nextStage = checkpoints.CheckpointStatusChecksumSkipped
		}

		// Don't call FinishTable when other lightning will calculate checksum.
		if err == nil && needChecksum {
			err = metaMgr.FinishTable(ctx)
		}

		saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, err, nextStage)
		if err = firstErr(err, saveCpErr); err != nil {
			return false, errors.Trace(err)
		}
		cp.Status = nextStage
	}

	if cp.Status < checkpoints.CheckpointStatusIndexAdded {
		var err error
		if rc.cfg.TikvImporter.AddIndexBySQL {
			w := rc.addIndexLimit.Apply()
			err = tr.addIndexes(ctx, rc.db)
			rc.addIndexLimit.Recycle(w)
			// Analyze will be automatically triggered after indexes are added by SQL. We can skip manual analyze.
			shouldSkipAnalyze = true
		}
		saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusIndexAdded)
		if err = firstErr(err, saveCpErr); err != nil {
			return false, errors.Trace(err)
		}
		cp.Status = checkpoints.CheckpointStatusIndexAdded
	}

	// do table analyze
	if cp.Status < checkpoints.CheckpointStatusAnalyzeSkipped {
		switch {
		case shouldSkipAnalyze || rc.cfg.PostRestore.Analyze == config.OpLevelOff:
			if !shouldSkipAnalyze {
				updateStatsMeta(ctx, rc.db, tr.tableInfo.ID, estimatedModifyCnt)
			}
			tr.logger.Info("skip analyze")
			if err := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, nil, checkpoints.CheckpointStatusAnalyzeSkipped); err != nil {
				return false, errors.Trace(err)
			}
			cp.Status = checkpoints.CheckpointStatusAnalyzeSkipped
		case forcePostProcess || !rc.cfg.PostRestore.PostProcessAtLast:
			err := tr.analyzeTable(ctx, rc.db)
			// witch post restore level 'optional', we will skip analyze error
			if rc.cfg.PostRestore.Analyze == config.OpLevelOptional {
				if err != nil {
					tr.logger.Warn("analyze table failed, will skip this error and go on", log.ShortError(err))
					err = nil
				}
			}
			saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusAnalyzed)
			if err = firstErr(err, saveCpErr); err != nil {
				return false, errors.Trace(err)
			}
			cp.Status = checkpoints.CheckpointStatusAnalyzed
		}
	}

	return true, nil
}

func getChunkCompressedSizeForParquet(
	ctx context.Context,
	chunk *checkpoints.ChunkCheckpoint,
	store storage.ExternalStorage,
) (int64, error) {
	reader, err := mydump.OpenReader(ctx, &chunk.FileMeta, store, storage.DecompressConfig{
		ZStdDecodeConcurrency: 1,
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	parser, err := mydump.NewParquetParser(ctx, store, reader, chunk.FileMeta.Path)
	if err != nil {
		_ = reader.Close()
		return 0, errors.Trace(err)
	}
	//nolint: errcheck
	defer parser.Close()
	err = parser.Reader.ReadFooter()
	if err != nil {
		return 0, errors.Trace(err)
	}
	rowGroups := parser.Reader.Footer.GetRowGroups()
	var maxRowGroupSize int64
	for _, rowGroup := range rowGroups {
		var rowGroupSize int64
		columnChunks := rowGroup.GetColumns()
		for _, columnChunk := range columnChunks {
			columnChunkSize := columnChunk.MetaData.GetTotalCompressedSize()
			rowGroupSize += columnChunkSize
		}
		maxRowGroupSize = max(maxRowGroupSize, rowGroupSize)
	}
	return maxRowGroupSize, nil
}

func updateStatsMeta(ctx context.Context, db *sql.DB, tableID int64, count int) {
	s := common.SQLWithRetry{
		DB:     db,
		Logger: log.FromContext(ctx).With(zap.Int64("tableID", tableID)),
	}
	err := s.Transact(ctx, "update stats_meta", func(ctx context.Context, tx *sql.Tx) error {
		rs, err := tx.ExecContext(ctx, `
update mysql.stats_meta
	set modify_count = ?,
		count = ?,
		version = @@tidb_current_ts
	where table_id = ?;
`, count, count, tableID)
		if err != nil {
			return errors.Trace(err)
		}
		affected, err := rs.RowsAffected()
		if err != nil {
			return errors.Trace(err)
		}
		if affected == 0 {
			return errors.Errorf("record with table_id %d not found", tableID)
		}
		return nil
	})
	if err != nil {
		s.Logger.Warn("failed to update stats_meta", zap.Error(err))
	}
}

func parseColumnPermutations(
	tableInfo *model.TableInfo,
	columns []string,
	ignoreColumns map[string]struct{},
	logger log.Logger,
) ([]int, error) {
	colPerm := make([]int, 0, len(tableInfo.Columns)+1)

	columnMap := make(map[string]int)
	for i, column := range columns {
		columnMap[column] = i
	}

	tableColumnMap := make(map[string]int)
	for i, col := range tableInfo.Columns {
		tableColumnMap[col.Name.L] = i
	}

	// check if there are some unknown columns
	var unknownCols []string
	for _, c := range columns {
		if _, ok := tableColumnMap[c]; !ok && c != model.ExtraHandleName.L {
			if _, ignore := ignoreColumns[c]; !ignore {
				unknownCols = append(unknownCols, c)
			}
		}
	}

	if len(unknownCols) > 0 {
		return colPerm, common.ErrUnknownColumns.GenWithStackByArgs(strings.Join(unknownCols, ","), tableInfo.Name)
	}

	for _, colInfo := range tableInfo.Columns {
		if i, ok := columnMap[colInfo.Name.L]; ok {
			if _, ignore := ignoreColumns[colInfo.Name.L]; !ignore {
				colPerm = append(colPerm, i)
			} else {
				logger.Debug("column ignored by user requirements",
					zap.Stringer("table", tableInfo.Name),
					zap.String("colName", colInfo.Name.O),
					zap.Stringer("colType", &colInfo.FieldType),
				)
				colPerm = append(colPerm, -1)
			}
		} else {
			if len(colInfo.GeneratedExprString) == 0 {
				logger.Warn("column missing from data file, going to fill with default value",
					zap.Stringer("table", tableInfo.Name),
					zap.String("colName", colInfo.Name.O),
					zap.Stringer("colType", &colInfo.FieldType),
				)
			}
			colPerm = append(colPerm, -1)
		}
	}
	// append _tidb_rowid column
	rowIDIdx := -1
	if i, ok := columnMap[model.ExtraHandleName.L]; ok {
		if _, ignored := ignoreColumns[model.ExtraHandleName.L]; !ignored {
			rowIDIdx = i
		}
	}
	// FIXME: the schema info for tidb backend is not complete, so always add the _tidb_rowid field.
	// Other logic should ignore this extra field if not needed.
	colPerm = append(colPerm, rowIDIdx)

	return colPerm, nil
}

func (tr *TableImporter) importKV(
	ctx context.Context,
	closedEngine *backend.ClosedEngine,
	rc *Controller,
) error {
	task := closedEngine.Logger().Begin(zap.InfoLevel, "import and cleanup engine")
	regionSplitSize := int64(rc.cfg.TikvImporter.RegionSplitSize)
	regionSplitKeys := int64(rc.cfg.TikvImporter.RegionSplitKeys)

	if regionSplitSize == 0 && rc.taskMgr != nil {
		regionSplitSize = int64(config.SplitRegionSize)
		if err := rc.taskMgr.CheckTasksExclusively(ctx, func(tasks []taskMeta) ([]taskMeta, error) {
			if len(tasks) > 0 {
				regionSplitSize = int64(config.SplitRegionSize) * int64(min(len(tasks), config.MaxSplitRegionSizeRatio))
			}
			return nil, nil
		}); err != nil {
			return errors.Trace(err)
		}
	}
	if regionSplitKeys == 0 {
		if regionSplitSize > int64(config.SplitRegionSize) {
			regionSplitKeys = int64(float64(regionSplitSize) / float64(config.SplitRegionSize) * float64(config.SplitRegionKeys))
		} else {
			regionSplitKeys = int64(config.SplitRegionKeys)
		}
	}
	err := closedEngine.Import(ctx, regionSplitSize, regionSplitKeys)
	if common.ErrFoundDuplicateKeys.Equal(err) {
		err = local.ConvertToErrFoundConflictRecords(err, tr.encTable)
	}
	saveCpErr := rc.saveStatusCheckpoint(ctx, tr.tableName, closedEngine.GetID(), err, checkpoints.CheckpointStatusImported)
	// Don't clean up when save checkpoint failed, because we will verifyLocalFile and import engine again after restart.
	if err == nil && saveCpErr == nil {
		err = multierr.Append(err, closedEngine.Cleanup(ctx))
	}
	err = firstErr(err, saveCpErr)

	dur := task.End(zap.ErrorLevel, err)

	if err != nil {
		return errors.Trace(err)
	}

	if m, ok := metric.FromContext(ctx); ok {
		m.ImportSecondsHistogram.Observe(dur.Seconds())
	}

	failpoint.Inject("SlowDownImport", func() {})

	return nil
}

// do checksum for each table.
func (tr *TableImporter) compareChecksum(remoteChecksum *local.RemoteChecksum, localChecksum verify.KVChecksum) error {
	if remoteChecksum.Checksum != localChecksum.Sum() ||
		remoteChecksum.TotalKVs != localChecksum.SumKVS() ||
		remoteChecksum.TotalBytes != localChecksum.SumSize() {
		return common.ErrChecksumMismatch.GenWithStackByArgs(
			remoteChecksum.Checksum, localChecksum.Sum(),
			remoteChecksum.TotalKVs, localChecksum.SumKVS(),
			remoteChecksum.TotalBytes, localChecksum.SumSize(),
		)
	}

	tr.logger.Info("checksum pass", zap.Object("local", &localChecksum))
	return nil
}

func (tr *TableImporter) analyzeTable(ctx context.Context, db *sql.DB) error {
	task := tr.logger.Begin(zap.InfoLevel, "analyze")
	exec := common.SQLWithRetry{
		DB:     db,
		Logger: tr.logger,
	}
	err := exec.Exec(ctx, "analyze table", "ANALYZE TABLE "+tr.tableName)
	task.End(zap.ErrorLevel, err)
	return err
}

func (tr *TableImporter) dropIndexes(ctx context.Context, db *sql.DB) error {
	logger := log.FromContext(ctx).With(zap.String("table", tr.tableName))

	tblInfo := tr.tableInfo
	remainIndexes, dropIndexes := common.GetDropIndexInfos(tblInfo.Core)
	for _, idxInfo := range dropIndexes {
		sqlStr := common.BuildDropIndexSQL(tblInfo.DB, tblInfo.Name, idxInfo)

		logger.Info("drop index", zap.String("sql", sqlStr))

		s := common.SQLWithRetry{
			DB:     db,
			Logger: logger,
		}
		if err := s.Exec(ctx, "drop index", sqlStr); err != nil {
			if merr, ok := errors.Cause(err).(*dmysql.MySQLError); ok {
				switch merr.Number {
				case errno.ErrCantDropFieldOrKey, errno.ErrDropIndexNeededInForeignKey:
					remainIndexes = append(remainIndexes, idxInfo)
					logger.Info("can't drop index, skip", zap.String("index", idxInfo.Name.O), zap.Error(err))
					continue
				}
			}
			return common.ErrDropIndexFailed.Wrap(err).GenWithStackByArgs(common.EscapeIdentifier(idxInfo.Name.O), tr.tableName)
		}
	}
	if len(remainIndexes) < len(tblInfo.Core.Indices) {
		// Must clone (*model.TableInfo) before modifying it, since it may be referenced in other place.
		tblInfo.Core = tblInfo.Core.Clone()
		tblInfo.Core.Indices = remainIndexes

		// Rebuild encTable.
		encTable, err := tables.TableFromMeta(tr.alloc, tblInfo.Core)
		if err != nil {
			return errors.Trace(err)
		}
		tr.encTable = encTable
	}
	return nil
}

func (tr *TableImporter) addIndexes(ctx context.Context, db *sql.DB) (retErr error) {
	const progressStep = "add-index"
	task := tr.logger.Begin(zap.InfoLevel, "add indexes")
	defer func() {
		task.End(zap.ErrorLevel, retErr)
	}()

	tblInfo := tr.tableInfo
	tableName := tr.tableName

	singleSQL, multiSQLs := common.BuildAddIndexSQL(tableName, tblInfo.Core, tblInfo.Desired)
	if len(multiSQLs) == 0 {
		return nil
	}

	logger := log.FromContext(ctx).With(zap.String("table", tableName))

	defer func() {
		if retErr == nil {
			web.BroadcastTableProgress(tr.tableName, progressStep, 1)
		} else if !log.IsContextCanceledError(retErr) {
			// Try to strip the prefix of the error message.
			// e.g "add index failed: Error 1062 ..." -> "Error 1062 ..."
			cause := errors.Cause(retErr)
			if cause == nil {
				cause = retErr
			}
			retErr = common.ErrAddIndexFailed.GenWithStack(
				"add index failed on table %s: %v, you can add index manually by the following SQL: %s",
				tableName, cause, singleSQL)
		}
	}()

	var totalRows int
	if m, ok := metric.FromContext(ctx); ok {
		totalRows = int(metric.ReadCounter(m.RowsCounter.WithLabelValues(metric.StateRestored, tableName)))
	}

	// Try to add all indexes in one statement.
	err := tr.executeDDL(ctx, db, singleSQL, func(status *ddlStatus) {
		if totalRows > 0 {
			progress := float64(status.rowCount) / float64(totalRows*len(multiSQLs))
			if progress > 1 {
				progress = 1
			}
			web.BroadcastTableProgress(tableName, progressStep, progress)
			logger.Info("add index progress", zap.String("progress", fmt.Sprintf("%.1f%%", progress*100)))
		}
	})
	if err == nil {
		return nil
	}
	if !common.IsDupKeyError(err) {
		return err
	}
	if len(multiSQLs) == 1 {
		return nil
	}
	logger.Warn("cannot add all indexes in one statement, try to add them one by one", zap.Strings("sqls", multiSQLs), zap.Error(err))

	baseProgress := float64(0)
	for _, ddl := range multiSQLs {
		err := tr.executeDDL(ctx, db, ddl, func(status *ddlStatus) {
			if totalRows > 0 {
				p := float64(status.rowCount) / float64(totalRows)
				progress := baseProgress + p/float64(len(multiSQLs))
				web.BroadcastTableProgress(tableName, progressStep, progress)
				logger.Info("add index progress", zap.String("progress", fmt.Sprintf("%.1f%%", progress*100)))
			}
		})
		if err != nil && !common.IsDupKeyError(err) {
			return err
		}
		baseProgress += 1.0 / float64(len(multiSQLs))
		web.BroadcastTableProgress(tableName, progressStep, baseProgress)
	}
	return nil
}

func (*TableImporter) executeDDL(
	ctx context.Context,
	db *sql.DB,
	ddl string,
	updateProgress func(status *ddlStatus),
) error {
	logger := log.FromContext(ctx).With(zap.String("ddl", ddl))
	logger.Info("execute ddl")

	s := common.SQLWithRetry{
		DB:     db,
		Logger: logger,
	}

	var currentTS int64
	if err := s.QueryRow(ctx, "", "SELECT UNIX_TIMESTAMP()", &currentTS); err != nil {
		currentTS = time.Now().Unix()
		logger.Warn("failed to query current timestamp, use current time instead", zap.Int64("currentTS", currentTS), zap.Error(err))
	}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- s.Exec(ctx, "add index", ddl)
	}()

	failpoint.Inject("AddIndexCrash", func() {
		_ = common.KillMySelf()
	})

	var ddlErr error
	for {
		select {
		case ddlErr = <-resultCh:
			failpoint.Inject("AddIndexFail", func() {
				ddlErr = errors.New("injected error")
			})
			if ddlErr == nil {
				return nil
			}
			if log.IsContextCanceledError(ddlErr) {
				return ddlErr
			}
			if isDeterminedError(ddlErr) {
				return ddlErr
			}
			logger.Warn("failed to execute ddl, try to query ddl status", zap.Error(ddlErr))
		case <-time.After(getDDLStatusInterval):
		}

		var status *ddlStatus
		err := common.Retry("query ddl status", logger, func() error {
			var err error
			status, err = getDDLStatus(ctx, db, ddl, time.Unix(currentTS, 0))
			return err
		})
		if err != nil || status == nil {
			logger.Warn("failed to query ddl status", zap.Error(err))
			if ddlErr != nil {
				return ddlErr
			}
			continue
		}
		updateProgress(status)

		if ddlErr != nil {
			switch state := status.state; state {
			case model.JobStateDone, model.JobStateSynced:
				logger.Info("ddl job is finished", zap.Stringer("state", state))
				return nil
			case model.JobStateRunning, model.JobStateQueueing, model.JobStateNone:
				logger.Info("ddl job is running", zap.Stringer("state", state))
			default:
				logger.Warn("ddl job is canceled or rollbacked", zap.Stringer("state", state))
				return ddlErr
			}
		}
	}
}

func isDeterminedError(err error) bool {
	if merr, ok := errors.Cause(err).(*dmysql.MySQLError); ok {
		switch merr.Number {
		case errno.ErrDupKeyName, errno.ErrMultiplePriKey, errno.ErrDupUnique, errno.ErrDupEntry:
			return true
		}
	}
	return false
}

const (
	getDDLStatusInterval = time.Minute
	// Limit the number of jobs to query. Large limit may result in empty result. See https://github.com/pingcap/tidb/issues/42298.
	// A new TiDB cluster has at least 40 jobs in the history queue, so 30 is a reasonable value.
	getDDLStatusMaxJobs = 30
)

type ddlStatus struct {
	state    model.JobState
	rowCount int64
}

func getDDLStatus(
	ctx context.Context,
	db *sql.DB,
	query string,
	minCreateTime time.Time,
) (*ddlStatus, error) {
	jobID, err := getDDLJobIDByQuery(ctx, db, query)
	if err != nil || jobID == 0 {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf("ADMIN SHOW DDL JOBS %d WHERE job_id = %d", getDDLStatusMaxJobs, jobID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var (
		rowCount      int64
		state         string
		createTimeStr sql.NullString
	)
	dest := make([]any, len(cols))
	for i, col := range cols {
		switch strings.ToLower(col) {
		case "row_count":
			dest[i] = &rowCount
		case "state":
			dest[i] = &state
		case "create_time":
			dest[i] = &createTimeStr
		default:
			var anyStr sql.NullString
			dest[i] = &anyStr
		}
	}
	status := &ddlStatus{}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Trace(err)
		}
		status.rowCount += rowCount
		// subjob doesn't have create_time, ignore it.
		if !createTimeStr.Valid || createTimeStr.String == "" {
			continue
		}
		createTime, err := time.Parse(time.DateTime, createTimeStr.String)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// The job is not created by the current task, ignore it.
		if createTime.Before(minCreateTime) {
			return nil, nil
		}
		status.state = model.StrToJobState(state)
	}
	return status, errors.Trace(rows.Err())
}

func getDDLJobIDByQuery(ctx context.Context, db *sql.DB, wantQuery string) (int64, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT %d", getDDLStatusMaxJobs))
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			jobID int64
			query string
		)
		if err := rows.Scan(&jobID, &query); err != nil {
			return 0, errors.Trace(err)
		}
		if query == wantQuery {
			return jobID, errors.Trace(rows.Err())
		}
	}
	return 0, errors.Trace(rows.Err())
}

func (tr *TableImporter) preDeduplicate(
	ctx context.Context,
	rc *Controller,
	cp *checkpoints.TableCheckpoint,
	workingDir string,
) error {
	d := &dupDetector{
		tr:     tr,
		rc:     rc,
		cp:     cp,
		logger: tr.logger,
	}
	originalErr := d.run(ctx, workingDir, tr.dupIgnoreRows)
	if originalErr == nil {
		return nil
	}

	if !ErrDuplicateKey.Equal(originalErr) {
		return errors.Trace(originalErr)
	}

	var (
		idxName                          string
		oneConflictMsg, otherConflictMsg string
	)

	// provide a more friendly error message

	dupErr := errors.Cause(originalErr).(*errors.Error)
	conflictIdxID := dupErr.Args()[0].(int64)
	if conflictIdxID == conflictOnHandle {
		idxName = "PRIMARY"
	} else {
		for _, idxInfo := range tr.tableInfo.Core.Indices {
			if idxInfo.ID == conflictIdxID {
				idxName = idxInfo.Name.O
				break
			}
		}
	}
	if idxName == "" {
		tr.logger.Error("cannot find index name", zap.Int64("conflictIdxID", conflictIdxID))
		return errors.Trace(originalErr)
	}
	if !rc.cfg.Checkpoint.Enable {
		err := errors.Errorf("duplicate key in table %s caused by index `%s`, but because checkpoint is off we can't have more details",
			tr.tableName, idxName)
		rc.errorMgr.RecordDuplicateOnce(
			ctx, tr.logger, tr.tableName, "<unknown-path>", -1, err.Error(), -1, "<unknown-data>",
		)
		return err
	}
	conflictEncodedRowIDs := dupErr.Args()[1].([][]byte)
	if len(conflictEncodedRowIDs) < 2 {
		tr.logger.Error("invalid conflictEncodedRowIDs", zap.Int("len", len(conflictEncodedRowIDs)))
		return errors.Trace(originalErr)
	}
	rowID := make([]int64, 2)
	var err error
	_, rowID[0], err = codec.DecodeComparableVarint(conflictEncodedRowIDs[0])
	if err != nil {
		rowIDHex := hex.EncodeToString(conflictEncodedRowIDs[0])
		tr.logger.Error("failed to decode rowID",
			zap.String("rowID", rowIDHex),
			zap.Error(err))
		return errors.Trace(originalErr)
	}
	_, rowID[1], err = codec.DecodeComparableVarint(conflictEncodedRowIDs[1])
	if err != nil {
		rowIDHex := hex.EncodeToString(conflictEncodedRowIDs[1])
		tr.logger.Error("failed to decode rowID",
			zap.String("rowID", rowIDHex),
			zap.Error(err))
		return errors.Trace(originalErr)
	}

	tableCp, err := rc.checkpointsDB.Get(ctx, tr.tableName)
	if err != nil {
		tr.logger.Error("failed to get table checkpoint", zap.Error(err))
		return errors.Trace(err)
	}
	var (
		secondConflictPath string
	)
	for _, engineCp := range tableCp.Engines {
		for _, chunkCp := range engineCp.Chunks {
			if chunkCp.Chunk.PrevRowIDMax <= rowID[0] && rowID[0] < chunkCp.Chunk.RowIDMax {
				oneConflictMsg = fmt.Sprintf("row %d counting from offset %d in file %s",
					rowID[0]-chunkCp.Chunk.PrevRowIDMax,
					chunkCp.Chunk.Offset,
					chunkCp.FileMeta.Path)
			}
			if chunkCp.Chunk.PrevRowIDMax <= rowID[1] && rowID[1] < chunkCp.Chunk.RowIDMax {
				secondConflictPath = chunkCp.FileMeta.Path
				otherConflictMsg = fmt.Sprintf("row %d counting from offset %d in file %s",
					rowID[1]-chunkCp.Chunk.PrevRowIDMax,
					chunkCp.Chunk.Offset,
					chunkCp.FileMeta.Path)
			}
		}
	}
	if oneConflictMsg == "" || otherConflictMsg == "" {
		tr.logger.Error("cannot find conflict rows by rowID",
			zap.Int64("rowID[0]", rowID[0]),
			zap.Int64("rowID[1]", rowID[1]))
		return errors.Trace(originalErr)
	}
	err = errors.Errorf("duplicate entry for key '%s', a pair of conflicting rows are (%s, %s)",
		idxName, oneConflictMsg, otherConflictMsg)
	rc.errorMgr.RecordDuplicateOnce(
		ctx, tr.logger, tr.tableName, secondConflictPath, -1, err.Error(), rowID[1], "<unknown-data>",
	)
	return err
}
