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
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type TableRestore struct {
	// The unique table name in the form "`db`.`tbl`".
	tableName string
	dbInfo    *checkpoints.TidbDBInfo
	tableInfo *checkpoints.TidbTableInfo
	tableMeta *mydump.MDTableMeta
	encTable  table.Table
	alloc     autoid.Allocators
	logger    log.Logger

	ignoreColumns []string
}

func NewTableRestore(
	tableName string,
	tableMeta *mydump.MDTableMeta,
	dbInfo *checkpoints.TidbDBInfo,
	tableInfo *checkpoints.TidbTableInfo,
	cp *checkpoints.TableCheckpoint,
	ignoreColumns []string,
) (*TableRestore, error) {
	idAlloc := kv.NewPanickingAllocators(cp.AllocBase)
	tbl, err := tables.TableFromMeta(idAlloc, tableInfo.Core)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", tableName)
	}

	return &TableRestore{
		tableName:     tableName,
		dbInfo:        dbInfo,
		tableInfo:     tableInfo,
		tableMeta:     tableMeta,
		encTable:      tbl,
		alloc:         idAlloc,
		logger:        log.With(zap.String("table", tableName)),
		ignoreColumns: ignoreColumns,
	}, nil
}

func (tr *TableRestore) Close() {
	tr.encTable = nil
	tr.logger.Info("restore done")
}

func (tr *TableRestore) populateChunks(ctx context.Context, rc *Controller, cp *checkpoints.TableCheckpoint) error {
	task := tr.logger.Begin(zap.InfoLevel, "load engines and files")
	chunks, err := mydump.MakeTableRegions(ctx, tr.tableMeta, len(tr.tableInfo.Core.Columns), rc.cfg, rc.ioWorkers, rc.store)
	if err == nil {
		timestamp := time.Now().Unix()
		failpoint.Inject("PopulateChunkTimestamp", func(v failpoint.Value) {
			timestamp = int64(v.(int))
		})
		for _, chunk := range chunks {
			engine, found := cp.Engines[chunk.EngineID]
			if !found {
				engine = &checkpoints.EngineCheckpoint{
					Status: checkpoints.CheckpointStatusLoaded,
				}
				cp.Engines[chunk.EngineID] = engine
			}
			ccp := &checkpoints.ChunkCheckpoint{
				Key: checkpoints.ChunkCheckpointKey{
					Path:   chunk.FileMeta.Path,
					Offset: chunk.Chunk.Offset,
				},
				FileMeta:          chunk.FileMeta,
				ColumnPermutation: nil,
				Chunk:             chunk.Chunk,
				Timestamp:         timestamp,
			}
			if len(chunk.Chunk.Columns) > 0 {
				perms, err := parseColumnPermutations(tr.tableInfo.Core, chunk.Chunk.Columns, tr.ignoreColumns)
				if err != nil {
					return errors.Trace(err)
				}
				ccp.ColumnPermutation = perms
			}
			engine.Chunks = append(engine.Chunks, ccp)
		}

		// Add index engine checkpoint
		cp.Engines[indexEngineID] = &checkpoints.EngineCheckpoint{Status: checkpoints.CheckpointStatusLoaded}
	}
	task.End(zap.ErrorLevel, err,
		zap.Int("enginesCnt", len(cp.Engines)),
		zap.Int("filesCnt", len(chunks)),
	)
	return err
}

func (t *TableRestore) RebaseChunkRowIDs(cp *checkpoints.TableCheckpoint, rowIDBase int64) {
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
func (tr *TableRestore) initializeColumns(columns []string, ccp *checkpoints.ChunkCheckpoint) error {
	var colPerm []int
	if len(columns) == 0 {
		colPerm = make([]int, 0, len(tr.tableInfo.Core.Columns)+1)
		shouldIncludeRowID := common.TableHasAutoRowID(tr.tableInfo.Core)

		// no provided columns, so use identity permutation.
		for i := range tr.tableInfo.Core.Columns {
			colPerm = append(colPerm, i)
		}
		if shouldIncludeRowID {
			colPerm = append(colPerm, -1)
		}
	} else {
		var err error
		colPerm, err = parseColumnPermutations(tr.tableInfo.Core, columns, tr.ignoreColumns)
		if err != nil {
			return errors.Trace(err)
		}
	}

	ccp.ColumnPermutation = colPerm
	return nil
}

func (tr *TableRestore) restoreEngines(pCtx context.Context, rc *Controller, cp *checkpoints.TableCheckpoint) error {
	indexEngineCp := cp.Engines[indexEngineID]
	if indexEngineCp == nil {
		return errors.Errorf("table %v index engine checkpoint not found", tr.tableName)
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

	idxEngineCfg := &backend.EngineConfig{
		TableInfo: tr.tableInfo,
	}
	if indexEngineCp.Status < checkpoints.CheckpointStatusClosed {
		indexWorker := rc.indexWorkers.Apply()
		defer rc.indexWorkers.Recycle(indexWorker)

		if rc.cfg.TikvImporter.Backend == config.BackendLocal {
			// for index engine, the estimate factor is non-clustered index count
			idxCnt := len(tr.tableInfo.Core.Indices)
			if !common.TableHasAutoRowID(tr.tableInfo.Core) {
				idxCnt--
			}
			threshold := estimateCompactionThreshold(cp, int64(idxCnt))
			idxEngineCfg.Local = &backend.LocalEngineConfig{
				Compact:            threshold > 0,
				CompactConcurrency: 4,
				CompactThreshold:   threshold,
			}
		}
		// import backend can't reopen engine if engine is closed, so
		// only open index engine if any data engines don't finish writing.
		var indexEngine *backend.OpenedEngine
		var err error
		for engineID, engine := range cp.Engines {
			if engineID == indexEngineID {
				continue
			}
			if engine.Status < checkpoints.CheckpointStatusAllWritten {
				indexEngine, err = rc.backend.OpenEngine(ctx, idxEngineCfg, tr.tableName, indexEngineID)
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
		sort.Slice(allEngines, func(i, j int) bool { return allEngines[i].engineID < allEngines[j].engineID })

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

				// Note: We still need tableWorkers to control the concurrency of tables.
				// In the future, we will investigate more about
				// the difference between restoring tables concurrently and restoring tables one by one.
				restoreWorker := rc.tableWorkers.Apply()

				go func(w *worker.Worker, eid int32, ecp *checkpoints.EngineCheckpoint) {
					defer wg.Done()

					engineLogTask := tr.logger.With(zap.Int32("engineNumber", eid)).Begin(zap.InfoLevel, "restore engine")
					dataClosedEngine, err := tr.restoreEngine(ctx, rc, indexEngine, eid, ecp)
					engineLogTask.End(zap.ErrorLevel, err)
					rc.tableWorkers.Recycle(w)
					if err != nil {
						setError(err)
						return
					}

					dataWorker := rc.closedEngineLimit.Apply()
					defer rc.closedEngineLimit.Recycle(dataWorker)
					if err := tr.importEngine(ctx, dataClosedEngine, rc, eid, ecp); err != nil {
						setError(err)
					}
				}(restoreWorker, engineID, engine)
			}
		}

		wg.Wait()

		restoreErr = engineErr.Get()
		logTask.End(zap.ErrorLevel, restoreErr)
		if restoreErr != nil {
			return errors.Trace(restoreErr)
		}

		if indexEngine != nil {
			closedIndexEngine, restoreErr = indexEngine.Close(ctx, idxEngineCfg)
		} else {
			closedIndexEngine, restoreErr = rc.backend.UnsafeCloseEngine(ctx, idxEngineCfg, tr.tableName, indexEngineID)
		}

		rc.saveStatusCheckpoint(tr.tableName, indexEngineID, restoreErr, checkpoints.CheckpointStatusClosed)
	} else if indexEngineCp.Status == checkpoints.CheckpointStatusClosed {
		// If index engine file has been closed but not imported only if context cancel occurred
		// when `importKV()` execution, so `UnsafeCloseEngine` and continue import it.
		closedIndexEngine, restoreErr = rc.backend.UnsafeCloseEngine(ctx, idxEngineCfg, tr.tableName, indexEngineID)
	}
	if restoreErr != nil {
		return errors.Trace(restoreErr)
	}

	if cp.Status < checkpoints.CheckpointStatusIndexImported {
		var err error
		if indexEngineCp.Status < checkpoints.CheckpointStatusImported {
			err = tr.importKV(ctx, closedIndexEngine, rc, indexEngineID)
		}

		failpoint.Inject("FailBeforeIndexEngineImported", func() {
			panic("forcing failure due to FailBeforeIndexEngineImported")
		})

		rc.saveStatusCheckpoint(tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusIndexImported)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (tr *TableRestore) restoreEngine(
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
		closedEngine, err := rc.backend.UnsafeCloseEngine(ctx, engineCfg, tr.tableName, engineID)
		// If any error occurred, recycle worker immediately
		if err != nil {
			return closedEngine, errors.Trace(err)
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
	}

	logTask := tr.logger.With(zap.Int32("engineNumber", engineID)).Begin(zap.InfoLevel, "encode kv data and write")
	dataEngineCfg := &backend.EngineConfig{
		TableInfo: tr.tableInfo,
		Local:     &backend.LocalEngineConfig{},
	}
	dataEngine, err := rc.backend.OpenEngine(ctx, dataEngineCfg, tr.tableName, engineID)
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

	// Restore table data
	for chunkIndex, chunk := range cp.Chunks {
		if chunk.Chunk.Offset >= chunk.Chunk.EndOffset {
			continue
		}

		checkFlushLock.Lock()
		finished := 0
		for _, c := range flushPendingChunks {
			if c.indexStatus.Flushed() && c.dataStatus.Flushed() {
				chunkCpChan <- c.chunkCp
				finished++
			} else {
				break
			}
		}
		if finished > 0 {
			flushPendingChunks = flushPendingChunks[finished:]
		}
		checkFlushLock.Unlock()

		select {
		case <-pCtx.Done():
			return nil, pCtx.Err()
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
		cr, err := newChunkRestore(ctx, chunkIndex, rc.cfg, chunk, rc.ioWorkers, rc.store, tr.tableInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var remainChunkCnt float64
		if chunk.Chunk.Offset < chunk.Chunk.EndOffset {
			remainChunkCnt = float64(chunk.Chunk.EndOffset-chunk.Chunk.Offset) / float64(chunk.Chunk.EndOffset-chunk.Key.Offset)
			metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending).Add(remainChunkCnt)
		}

		restoreWorker := rc.regionWorkers.Apply()
		wg.Add(1)

		dataWriter, err := dataEngine.LocalWriter(ctx, dataWriterCfg)
		if err != nil {
			return nil, errors.Trace(err)
		}

		indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
		if err != nil {
			return nil, errors.Trace(err)
		}

		go func(w *worker.Worker, cr *chunkRestore) {
			// Restore a chunk.
			defer func() {
				cr.close()
				wg.Done()
				rc.regionWorkers.Recycle(w)
			}()
			metric.ChunkCounter.WithLabelValues(metric.ChunkStateRunning).Add(remainChunkCnt)
			err := cr.restore(ctx, tr, engineID, dataWriter, indexWriter, rc)
			var dataFlushStatus, indexFlushStaus backend.ChunkFlushStatus
			if err == nil {
				dataFlushStatus, err = dataWriter.Close(ctx)
			}
			if err == nil {
				indexFlushStaus, err = indexWriter.Close(ctx)
			}
			if err == nil {
				metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Add(remainChunkCnt)
				metric.BytesCounter.WithLabelValues(metric.TableStateWritten).Add(float64(cr.chunk.Checksum.SumSize()))
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
				metric.ChunkCounter.WithLabelValues(metric.ChunkStateFailed).Add(remainChunkCnt)
				chunkErr.Set(err)
				cancel()
			}
		}(restoreWorker, cr)
	}

	wg.Wait()

	// Report some statistics into the log for debugging.
	totalKVSize := uint64(0)
	totalSQLSize := int64(0)
	for _, chunk := range cp.Chunks {
		totalKVSize += chunk.Checksum.SumSize()
		totalSQLSize += chunk.Chunk.EndOffset - chunk.Chunk.Offset
	}

	err = chunkErr.Get()
	logTask.End(zap.ErrorLevel, err,
		zap.Int64("read", totalSQLSize),
		zap.Uint64("written", totalKVSize),
	)

	trySavePendingChunks := func(flushCtx context.Context) error {
		checkFlushLock.Lock()
		cnt := 0
		for _, chunk := range flushPendingChunks {
			if chunk.dataStatus.Flushed() && chunk.indexStatus.Flushed() {
				saveCheckpoint(rc, tr, engineID, chunk.chunkCp)
				cnt++
			} else {
				break
			}
		}
		flushPendingChunks = flushPendingChunks[cnt:]
		checkFlushLock.Unlock()
		return nil
	}

	// in local mode, this check-point make no sense, because we don't do flush now,
	// so there may be data lose if exit at here. So we don't write this checkpoint
	// here like other mode.
	if !rc.isLocalBackend() {
		rc.saveStatusCheckpoint(tr.tableName, engineID, err, checkpoints.CheckpointStatusAllWritten)
	}
	if err != nil {
		// if process is canceled, we should flush all chunk checkpoints for local backend
		if rc.isLocalBackend() && common.IsContextCanceledError(err) {
			// ctx is canceled, so to avoid Close engine failed, we use `context.Background()` here
			if _, err2 := dataEngine.Close(context.Background(), dataEngineCfg); err2 != nil {
				log.L().Warn("flush all chunk checkpoints failed before manually exits", zap.Error(err2))
				return nil, errors.Trace(err)
			}
			if err2 := trySavePendingChunks(context.Background()); err2 != nil {
				log.L().Warn("flush all chunk checkpoints failed before manually exits", zap.Error(err2))
			}
		}
		return nil, errors.Trace(err)
	}

	closedDataEngine, err := dataEngine.Close(ctx, dataEngineCfg)
	// For local backend, if checkpoint is enabled, we must flush index engine to avoid data loss.
	// this flush action impact up to 10% of the performance, so we only do it if necessary.
	if err == nil && rc.cfg.Checkpoint.Enable && rc.isLocalBackend() {
		if err = indexEngine.Flush(ctx); err != nil {
			return nil, errors.Trace(err)
		}
		if err = trySavePendingChunks(ctx); err != nil {
			return nil, errors.Trace(err)
		}
	}
	rc.saveStatusCheckpoint(tr.tableName, engineID, err, checkpoints.CheckpointStatusClosed)
	if err != nil {
		// If any error occurred, recycle worker immediately
		return nil, errors.Trace(err)
	}
	return closedDataEngine, nil
}

func (tr *TableRestore) importEngine(
	ctx context.Context,
	closedEngine *backend.ClosedEngine,
	rc *Controller,
	engineID int32,
	cp *checkpoints.EngineCheckpoint,
) error {
	if cp.Status >= checkpoints.CheckpointStatusImported {
		return nil
	}

	// 1. calling import
	if err := tr.importKV(ctx, closedEngine, rc, engineID); err != nil {
		return errors.Trace(err)
	}

	// 2. perform a level-1 compact if idling.
	if rc.cfg.PostRestore.Level1Compact && rc.compactState.CAS(compactStateIdle, compactStateDoing) {
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
func (tr *TableRestore) postProcess(
	ctx context.Context,
	rc *Controller,
	cp *checkpoints.TableCheckpoint,
	forcePostProcess bool,
	metaMgr tableMetaMgr,
) (bool, error) {
	// there are no data in this table, no need to do post process
	// this is important for tables that are just the dump table of views
	// because at this stage, the table was already deleted and replaced by the related view
	if !rc.backend.ShouldPostProcess() || len(cp.Engines) == 1 {
		return false, nil
	}

	// 3. alter table set auto_increment
	if cp.Status < checkpoints.CheckpointStatusAlteredAutoInc {
		rc.alterTableLock.Lock()
		tblInfo := tr.tableInfo.Core
		var err error
		if tblInfo.PKIsHandle && tblInfo.ContainsAutoRandomBits() {
			err = AlterAutoRandom(ctx, rc.tidbGlue.GetSQLExecutor(), tr.tableName, tr.alloc.Get(autoid.AutoRandomType).Base()+1)
		} else if common.TableHasAutoRowID(tblInfo) || tblInfo.GetAutoIncrementColInfo() != nil {
			// only alter auto increment id iff table contains auto-increment column or generated handle
			err = AlterAutoIncrement(ctx, rc.tidbGlue.GetSQLExecutor(), tr.tableName, tr.alloc.Get(autoid.RowIDAllocType).Base()+1)
		}
		rc.alterTableLock.Unlock()
		rc.saveStatusCheckpoint(tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusAlteredAutoInc)
		if err != nil {
			return false, err
		}
		cp.Status = checkpoints.CheckpointStatusAlteredAutoInc
	}

	// tidb backend don't need checksum & analyze
	if !rc.backend.ShouldPostProcess() {
		tr.logger.Debug("skip checksum & analyze, not supported by this backend")
		rc.saveStatusCheckpoint(tr.tableName, checkpoints.WholeTableEngineID, nil, checkpoints.CheckpointStatusAnalyzeSkipped)
		return false, nil
	}

	w := rc.checksumWorks.Apply()
	defer rc.checksumWorks.Recycle(w)

	finished := true
	if cp.Status < checkpoints.CheckpointStatusChecksummed {
		// 4. do table checksum
		var localChecksum verify.KVChecksum
		for _, engine := range cp.Engines {
			for _, chunk := range engine.Chunks {
				localChecksum.Add(&chunk.Checksum)
			}
		}

		if rc.cfg.PostRestore.Checksum == config.OpLevelOff {
			tr.logger.Info("skip checksum")
			rc.saveStatusCheckpoint(tr.tableName, checkpoints.WholeTableEngineID, nil, checkpoints.CheckpointStatusChecksumSkipped)
		} else {
			if forcePostProcess || !rc.cfg.PostRestore.PostProcessAtLast {
				tr.logger.Info("local checksum", zap.Object("checksum", &localChecksum))
				if rc.cfg.TikvImporter.DuplicateDetection {
					if err := rc.backend.CollectLocalDuplicateRows(ctx, tr.encTable); err != nil {
						tr.logger.Error("collect local duplicate keys failed", log.ShortError(err))
					}
				}
				needChecksum, baseTotalChecksum, err := metaMgr.CheckAndUpdateLocalChecksum(ctx, &localChecksum)
				if err != nil {
					return false, err
				}
				if !needChecksum {
					return false, nil
				}
				if rc.cfg.TikvImporter.DuplicateDetection {
					if err := rc.backend.CollectRemoteDuplicateRows(ctx, tr.encTable); err != nil {
						tr.logger.Error("collect remote duplicate keys failed", log.ShortError(err))
						err = nil
					}
				}
				if cp.Checksum.SumKVS() > 0 || baseTotalChecksum.SumKVS() > 0 {
					localChecksum.Add(&cp.Checksum)
					localChecksum.Add(baseTotalChecksum)
					tr.logger.Info("merged local checksum", zap.Object("checksum", &localChecksum))
				}

				remoteChecksum, err := DoChecksum(ctx, tr.tableInfo)
				// TODO: If there are duplicate keys, do not set the `ChecksumMismatch` error
				err = tr.compareChecksum(remoteChecksum, localChecksum)
				// with post restore level 'optional', we will skip checksum error
				if rc.cfg.PostRestore.Checksum == config.OpLevelOptional {
					if err != nil {
						tr.logger.Warn("compare checksum failed, will skip this error and go on", log.ShortError(err))
						err = nil
					}
				}
				if err == nil {
					err = metaMgr.FinishTable(ctx)
				}

				rc.saveStatusCheckpoint(tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusChecksummed)
				if err != nil {
					return false, errors.Trace(err)
				}

				cp.Status = checkpoints.CheckpointStatusChecksummed
			} else {
				finished = false
			}
		}
	}
	if !finished {
		return !finished, nil
	}

	// 5. do table analyze
	if cp.Status < checkpoints.CheckpointStatusAnalyzed {
		switch {
		case rc.cfg.PostRestore.Analyze == config.OpLevelOff:
			tr.logger.Info("skip analyze")
			rc.saveStatusCheckpoint(tr.tableName, checkpoints.WholeTableEngineID, nil, checkpoints.CheckpointStatusAnalyzeSkipped)
			cp.Status = checkpoints.CheckpointStatusAnalyzed
		case forcePostProcess || !rc.cfg.PostRestore.PostProcessAtLast:
			err := tr.analyzeTable(ctx, rc.tidbGlue.GetSQLExecutor())
			// witch post restore level 'optional', we will skip analyze error
			if rc.cfg.PostRestore.Analyze == config.OpLevelOptional {
				if err != nil {
					tr.logger.Warn("analyze table failed, will skip this error and go on", log.ShortError(err))
					err = nil
				}
			}
			rc.saveStatusCheckpoint(tr.tableName, checkpoints.WholeTableEngineID, err, checkpoints.CheckpointStatusAnalyzed)
			if err != nil {
				return false, errors.Trace(err)
			}
			cp.Status = checkpoints.CheckpointStatusAnalyzed
		default:
			finished = false
		}
	}

	return !finished, nil
}

func parseColumnPermutations(tableInfo *model.TableInfo, columns []string, ignoreColumns []string) ([]int, error) {
	colPerm := make([]int, 0, len(tableInfo.Columns)+1)

	columnMap := make(map[string]int)
	for i, column := range columns {
		columnMap[column] = i
	}

	ignoreMap := make(map[string]int)
	for _, column := range ignoreColumns {
		if i, ok := columnMap[column]; ok {
			ignoreMap[column] = i
		}
	}

	tableColumnMap := make(map[string]int)
	for i, col := range tableInfo.Columns {
		tableColumnMap[col.Name.L] = i
	}

	// check if there are some unknown columns
	var unknownCols []string
	for _, c := range columns {
		if _, ok := tableColumnMap[c]; !ok && c != model.ExtraHandleName.L {
			if _, ignore := ignoreMap[c]; !ignore {
				unknownCols = append(unknownCols, c)
			}
		}
	}

	if len(unknownCols) > 0 {
		return colPerm, errors.Errorf("unknown columns in header %s", unknownCols)
	}

	for _, colInfo := range tableInfo.Columns {
		if i, ok := columnMap[colInfo.Name.L]; ok {
			if _, ignore := ignoreMap[colInfo.Name.L]; !ignore {
				colPerm = append(colPerm, i)
			} else {
				log.L().Debug("column ignored by user requirements",
					zap.Stringer("table", tableInfo.Name),
					zap.String("colName", colInfo.Name.O),
					zap.Stringer("colType", &colInfo.FieldType),
				)
				colPerm = append(colPerm, -1)
			}
		} else {
			if len(colInfo.GeneratedExprString) == 0 {
				log.L().Warn("column missing from data file, going to fill with default value",
					zap.Stringer("table", tableInfo.Name),
					zap.String("colName", colInfo.Name.O),
					zap.Stringer("colType", &colInfo.FieldType),
				)
			}
			colPerm = append(colPerm, -1)
		}
	}
	if i, ok := columnMap[model.ExtraHandleName.L]; ok {
		colPerm = append(colPerm, i)
	} else if common.TableHasAutoRowID(tableInfo) {
		colPerm = append(colPerm, -1)
	}

	return colPerm, nil
}

func (tr *TableRestore) importKV(
	ctx context.Context,
	closedEngine *backend.ClosedEngine,
	rc *Controller,
	engineID int32,
) error {
	task := closedEngine.Logger().Begin(zap.InfoLevel, "import and cleanup engine")

	err := closedEngine.Import(ctx)
	rc.saveStatusCheckpoint(tr.tableName, engineID, err, checkpoints.CheckpointStatusImported)
	// Also cleanup engine when encountered ErrDuplicateDetected, since all duplicates kv pairs are recorded.
	if err == nil {
		err = multierr.Append(err, closedEngine.Cleanup(ctx))
	}

	dur := task.End(zap.ErrorLevel, err)

	if err != nil {
		return errors.Trace(err)
	}

	metric.ImportSecondsHistogram.Observe(dur.Seconds())

	failpoint.Inject("SlowDownImport", func() {})

	return nil
}

// do checksum for each table.
func (tr *TableRestore) compareChecksum(remoteChecksum *RemoteChecksum, localChecksum verify.KVChecksum) error {
	if remoteChecksum.Checksum != localChecksum.Sum() ||
		remoteChecksum.TotalKVs != localChecksum.SumKVS() ||
		remoteChecksum.TotalBytes != localChecksum.SumSize() {
		return errors.Errorf("checksum mismatched remote vs local => (checksum: %d vs %d) (total_kvs: %d vs %d) (total_bytes:%d vs %d)",
			remoteChecksum.Checksum, localChecksum.Sum(),
			remoteChecksum.TotalKVs, localChecksum.SumKVS(),
			remoteChecksum.TotalBytes, localChecksum.SumSize(),
		)
	}

	tr.logger.Info("checksum pass", zap.Object("local", &localChecksum))
	return nil
}

func (tr *TableRestore) analyzeTable(ctx context.Context, g glue.SQLExecutor) error {
	task := tr.logger.Begin(zap.InfoLevel, "analyze")
	err := g.ExecuteWithLog(ctx, "ANALYZE TABLE "+tr.tableName, "analyze table", tr.logger)
	task.End(zap.ErrorLevel, err)
	return err
}
