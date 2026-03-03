// Copyright 2024 PingCAP, Inc.
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

package snapclient

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/tablecodec"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/engine"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const defaultChannelSize = 1024

// defaultChecksumConcurrency is the default number of the concurrent
// checksum tasks.
const defaultChecksumConcurrency = 64

type PhysicalTable struct {
	NewPhysicalID int64
	OldPhysicalID int64
	RewriteRules  *restoreutils.RewriteRules
	Files         []*backuppb.File
}

func defaultOutputTableChan() chan *restoreutils.CreatedTable {
	return make(chan *restoreutils.CreatedTable, defaultChannelSize)
}

// ExhaustErrors drains all remaining errors in the channel, into a slice of errors.
func ExhaustErrors(ec <-chan error) []error {
	out := make([]error, 0, len(ec))
	for {
		select {
		case err := <-ec:
			out = append(out, err)
		default:
			// errCh will NEVER be closed(ya see, it has multi sender-part),
			// so we just consume the current backlog of this channel, then return.
			return out
		}
	}
}

func (rc *SnapClient) filterAndValidateTemporaryTables(
	ctx context.Context,
	createdTables []*restoreutils.CreatedTable,
	temporaryTablesCheckFn func(string, string) (string, bool),
	kvClient kv.Client,
	checksum bool,
	checksumConcurrency uint,
) (map[string]map[string]struct{}, int, error) {
	renamedTables := make(map[string]map[string]struct{})
	renamedTableCount := 0
	workerpool := tidbutil.NewWorkerPool(defaultChecksumConcurrency, "Restore Statistic Checksum")
	eg, ectx := errgroup.WithContext(ctx)
	for _, createdTable := range createdTables {
		tempSchemaName := createdTable.OldTable.DB.Name.O
		tableName := createdTable.OldTable.Info.Name.O
		if dbName, ok := temporaryTablesCheckFn(tempSchemaName, tableName); ok {
			renamedTableMap, ok := renamedTables[dbName]
			if !ok {
				renamedTableMap = make(map[string]struct{})
				renamedTables[dbName] = renamedTableMap
			}
			renamedTableMap[tableName] = struct{}{}
			if checksum {
				workerpool.ApplyOnErrorGroup(eg, func() error {
					return rc.execAndValidateChecksum(ectx, createdTable, kvClient, checksumConcurrency)
				})
			}
			renamedTableCount += 1
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, 0, errors.Trace(err)
	}
	return renamedTables, renamedTableCount, nil
}

func (rc *SnapClient) moveRenamedTable(ctx context.Context, restoreTS uint64, statisticTables map[string]map[string]struct{}) error {
	// the renamed tables will be deleted by DROP DATABASE in the function cleanTemporaryDatabase later
	renameSQL := GenerateMoveRenamedTableSQLPair(restoreTS, statisticTables)
	if err := rc.db.Session().Execute(ctx, renameSQL); err != nil {
		log.Error("failed to move rename tables", zap.String("sql", renameSQL), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (rc *SnapClient) updateTemporaryUserTable(ctx context.Context, renamedTables map[string]map[string]struct{}) error {
	if tables, exists := renamedTables[mysql.SystemDB]; exists {
		if _, exists = tables[sysUserTableName]; exists {
			return removeUserResourceGroup(ctx, utils.TemporaryDBName(mysql.SystemDB).O, rc.db.Session().Execute)
		}
	}
	return nil
}

func (rc *SnapClient) replaceTables(
	ctx context.Context,
	createdTables []*restoreutils.CreatedTable,
	restoreTS uint64,
	loadStatsPhysical, loadSysTablePhysical bool,
	kvClient kv.Client,
	checksum bool,
	checksumConcurrency uint,
) (int, error) {
	temporaryTableChecker := &TemporaryTableChecker{
		loadStatsPhysical:    loadStatsPhysical,
		loadSysTablePhysical: loadSysTablePhysical,
	}
	renamedTables, renamedTableCount, err := rc.filterAndValidateTemporaryTables(ctx, createdTables, temporaryTableChecker.CheckTemporaryTables, kvClient, checksum, checksumConcurrency)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if len(renamedTables) == 0 {
		return 0, nil
	}

	if err := rc.updateTemporaryUserTable(ctx, renamedTables); err != nil {
		return 0, errors.Trace(err)
	}

	if err := updateStatsTableSchema(ctx, renamedTables, rc.dom.InfoSchema(), rc.db.Session().Execute); err != nil {
		return 0, errors.Trace(err)
	}

	if err := rc.moveRenamedTable(ctx, restoreTS, renamedTables); err != nil {
		return 0, errors.Trace(err)
	}

	if err := notifyUpdateAllUsersPrivilege(renamedTables, rc.dom.NotifyUpdateAllUsersPrivilege); err != nil {
		return 0, errors.Trace(err)
	}

	return renamedTableCount, nil
}

type PipelineContext struct {
	// pipeline item switch
	Checksum             bool
	LoadStats            bool
	LoadStatsPhysical    bool
	LoadSysTablePhysical bool
	WaitTiflashReady     bool

	// pipeline item configuration
	LogProgress         bool
	ChecksumConcurrency uint
	StatsConcurrency    uint
	RestoreTS           uint64

	// pipeline item tool client
	KvClient   kv.Client
	ExtStorage storeapi.Storage
	Glue       glue.Glue
}

// RestorePipeline does checksum, load stats and wait for tiflash to be ready.
func (rc *SnapClient) RestorePipeline(ctx context.Context, plCtx PipelineContext, createdTables []*restoreutils.CreatedTable) (err error) {
	start := time.Now()
	defer func() {
		summary.CollectDuration("restore pipeline", time.Since(start))
	}()
	pipelineNum := 0
	if plCtx.Checksum {
		pipelineNum += 1
	}
	if plCtx.LoadStats && !plCtx.LoadStatsPhysical {
		pipelineNum += 1
	}
	if plCtx.WaitTiflashReady {
		pipelineNum += 1
	}
	progressLen := int64(pipelineNum * len(createdTables))
	if plCtx.LoadStatsPhysical || plCtx.LoadSysTablePhysical {
		renamedTableCount, err := rc.replaceTables(ctx, createdTables, plCtx.RestoreTS, plCtx.LoadStatsPhysical, plCtx.LoadSysTablePhysical, plCtx.KvClient, plCtx.Checksum, plCtx.ChecksumConcurrency)
		if err != nil {
			return errors.Trace(err)
		}
		progressLen -= int64(pipelineNum * renamedTableCount)
	}

	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := plCtx.Glue.StartProgress(ctx, "Restore Pipeline", progressLen, !plCtx.LogProgress)
	defer updateCh.Close()

	handlerBuilder := &PipelineConcurrentBuilder{loadStatsPhysical: plCtx.LoadStatsPhysical, loadSysTablePhysical: plCtx.LoadSysTablePhysical}
	// pipeline checksum
	if plCtx.Checksum {
		rc.registerValidateChecksum(handlerBuilder, plCtx.KvClient, updateCh, plCtx.ChecksumConcurrency)
	}

	// pipeline update meta and load stats
	if plCtx.LoadStats && !plCtx.LoadStatsPhysical {
		rc.registerUpdateMetaAndLoadStats(handlerBuilder, plCtx.ExtStorage, updateCh, plCtx.StatsConcurrency)
	}

	// pipeline wait Tiflash synced
	if plCtx.WaitTiflashReady {
		if err := rc.registerWaitTiFlashReady(handlerBuilder, updateCh); err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(handlerBuilder.StartPipelineTask(ctx, createdTables))
}

type pipelineFunction struct {
	taskLabel   string
	concurrency uint

	processFn func(context.Context, *restoreutils.CreatedTable) error
	endFn     func(context.Context) error
}

type PipelineConcurrentBuilder struct {
	pipelineFunctions []pipelineFunction

	loadStatsPhysical    bool
	loadSysTablePhysical bool
}

func (builder *PipelineConcurrentBuilder) RegisterPipelineTask(
	taskLabel string,
	concurrency uint,
	processFn func(context.Context, *restoreutils.CreatedTable) error,
	endFn func(context.Context) error,
) {
	builder.pipelineFunctions = append(builder.pipelineFunctions, pipelineFunction{
		taskLabel:   taskLabel,
		concurrency: concurrency,
		processFn:   processFn,
		endFn:       endFn,
	})
}

func (builder *PipelineConcurrentBuilder) StartPipelineTask(ctx context.Context, createdTables []*restoreutils.CreatedTable) error {
	eg, pipelineTaskCtx := errgroup.WithContext(ctx)
	handler := &PipelineConcurrentHandler{
		pipelineTaskCtx: pipelineTaskCtx,
		eg:              eg,
	}

	// the first pipeline task
	postHandleCh := handler.afterTableRestoredCh(createdTables, builder.loadStatsPhysical, builder.loadSysTablePhysical)

	// the middle pipeline tasks
	for _, f := range builder.pipelineFunctions {
		postHandleCh = handler.concurrentHandleTablesCh(postHandleCh, f.concurrency, f.taskLabel, f.processFn, f.endFn)
	}

	// the last pipeline task
	handler.dropToBlackhole(postHandleCh)

	return eg.Wait()
}

type PipelineConcurrentHandler struct {
	pipelineTaskCtx context.Context
	eg              *errgroup.Group
}

func (handler *PipelineConcurrentHandler) afterTableRestoredCh(createdTables []*restoreutils.CreatedTable, loadStatsPhysical, loadSysTablePhysical bool) <-chan *restoreutils.CreatedTable {
	outCh := make(chan *restoreutils.CreatedTable)

	handler.eg.Go(func() error {
		defer close(outCh)

		for _, createdTable := range createdTables {
			if loadStatsPhysical && IsStatsTemporaryTable(createdTable.OldTable.DB.Name.O, createdTable.OldTable.Info.Name.O) {
				continue
			}
			if loadSysTablePhysical && IsRenameableSysTemporaryTable(createdTable.OldTable.DB.Name.O, createdTable.OldTable.Info.Name.O) {
				continue
			}
			select {
			case <-handler.pipelineTaskCtx.Done():
				return handler.pipelineTaskCtx.Err()
			case outCh <- createdTable:
			}
		}

		return nil
	})
	return outCh
}

// dropToBlackhole drop all incoming tables into black hole,
// i.e. don't execute checksum, just increase the process anyhow.
func (handler *PipelineConcurrentHandler) dropToBlackhole(inCh <-chan *restoreutils.CreatedTable) {
	handler.eg.Go(func() error {
		for {
			select {
			case <-handler.pipelineTaskCtx.Done():
				return handler.pipelineTaskCtx.Err()
			case _, ok := <-inCh:
				if !ok {
					return nil
				}
			}
		}
	})
}

func (handler *PipelineConcurrentHandler) concurrentHandleTablesCh(
	inCh <-chan *restoreutils.CreatedTable,
	concurrency uint,
	taskLabel string,
	processFun func(context.Context, *restoreutils.CreatedTable) error,
	endFun func(context.Context) error,
) (outCh chan *restoreutils.CreatedTable) {
	outCh = defaultOutputTableChan()
	handler.eg.Go(func() (pipelineErr error) {
		workers := tidbutil.NewWorkerPool(concurrency, taskLabel)
		eg, ectx := errgroup.WithContext(handler.pipelineTaskCtx)
		defer func() {
			// Note: directly return the error and then the pipelineTaskCtx will be cancelled.
			if err := eg.Wait(); err != nil {
				log.Error("pipeline item execution is failed", zap.String("task", taskLabel), zap.Error(err))
				pipelineErr = errors.Trace(err)
				return
			}
			if handler.pipelineTaskCtx.Err() != nil {
				pipelineErr = handler.pipelineTaskCtx.Err()
				return
			}
			if err := endFun(handler.pipelineTaskCtx); err != nil {
				log.Error("pipeline defer execution is failed", zap.String("task", taskLabel), zap.Error(err))
				pipelineErr = errors.Trace(err)
				return
			}
			// Note: No need to close `outCh` if an error occurs because the `handler.pipelineTaskCtx` will be cancelled
			// and all the pipelines will be returned in time.
			close(outCh)
		}()

		for {
			select {
			// ectx will be cancelled if all the pipelines stop (handler.pipelineTaskCtx is cancelled by another pipeline error)
			// or this pipeline stops (ectx is cancelled by this pipeline error)
			case <-ectx.Done():
				return
			case tbl, ok := <-inCh:
				if !ok {
					return
				}
				if ectx.Err() != nil {
					// ectx is cancelled, return and get the error from error group in defer function
					return
				}
				cloneTable := tbl
				workers.ApplyOnErrorGroup(eg, func() error {
					if err := processFun(ectx, cloneTable); err != nil {
						return err
					}
					select {
					case <-ectx.Done():
						// ectx is cancelled, return and get the error from error group in defer function
					case outCh <- cloneTable:
					}
					return nil
				})
			}
		}
	})
	return outCh
}

// registerValidateChecksum validates checksum after restore.
func (rc *SnapClient) registerValidateChecksum(
	builder *PipelineConcurrentBuilder,
	kvClient kv.Client,
	updateCh glue.Progress,
	concurrency uint,
) {
	builder.RegisterPipelineTask("Restore Checksum", defaultChecksumConcurrency, func(c context.Context, tbl *restoreutils.CreatedTable) error {
		err := rc.execAndValidateChecksum(c, tbl, kvClient, concurrency)
		if err != nil {
			return errors.Trace(err)
		}
		updateCh.Inc()
		return nil
	}, func(context.Context) error {
		log.Info("all checksum ended")
		return nil
	})
}

const statsMetaItemBufferSize = 3000

type statsMetaItemBuffer struct {
	sync.Mutex
	metaUpdates []statstypes.MetaUpdate
}

func NewStatsMetaItemBuffer() *statsMetaItemBuffer {
	return &statsMetaItemBuffer{
		metaUpdates: make([]statstypes.MetaUpdate, 0, statsMetaItemBufferSize),
	}
}

func (buffer *statsMetaItemBuffer) appendItem(item statstypes.MetaUpdate) (metaUpdates []statstypes.MetaUpdate) {
	buffer.Lock()
	defer buffer.Unlock()
	buffer.metaUpdates = append(buffer.metaUpdates, item)
	if len(buffer.metaUpdates) < statsMetaItemBufferSize {
		return
	}
	metaUpdates = buffer.metaUpdates
	buffer.metaUpdates = make([]statstypes.MetaUpdate, 0, statsMetaItemBufferSize)
	return metaUpdates
}

func (buffer *statsMetaItemBuffer) take() (metaUpdates []statstypes.MetaUpdate) {
	buffer.Lock()
	defer buffer.Unlock()
	metaUpdates = buffer.metaUpdates
	buffer.metaUpdates = nil
	return metaUpdates
}

func (buffer *statsMetaItemBuffer) UpdateMetasRest(ctx context.Context, statsHandler *handle.Handle) error {
	metaUpdates := buffer.take()
	if len(metaUpdates) == 0 {
		return nil
	}
	return buffer.saveMetaToStorageWithRetry(ctx, statsHandler, metaUpdates)
}

func (buffer *statsMetaItemBuffer) TryUpdateMetas(ctx context.Context, statsHandler *handle.Handle, physicalID, count int64) error {
	item := statstypes.MetaUpdate{
		PhysicalID:  physicalID,
		Count:       count,
		ModifyCount: count,
	}
	metaUpdates := buffer.appendItem(item)
	if len(metaUpdates) == 0 {
		return nil
	}
	return buffer.saveMetaToStorageWithRetry(ctx, statsHandler, metaUpdates)
}

func (buffer *statsMetaItemBuffer) saveMetaToStorageWithRetry(
	ctx context.Context,
	statsHandler *handle.Handle,
	metaUpdates []statstypes.MetaUpdate,
) error {
	state := utils.InitialRetryState(8, 500*time.Millisecond, 500*time.Millisecond)
	err := utils.WithRetry(ctx, func() error {
		if err := statsHandler.SaveMetaToStorage("br restore", false, metaUpdates...); err != nil {
			log.Error("failed to save meta to storage", zap.Error(err))
			return errors.Trace(err)
		}
		return nil
	}, &state)
	return errors.Trace(err)
}

func calculateRowCountForPhysicalTable(files []*backuppb.File) int64 {
	totalKvs := uint64(0)
	for _, file := range files {
		if tablecodec.IsRecordKey(file.StartKey) {
			totalKvs += file.TotalKvs
		}
	}
	return int64(totalKvs)
}

func updateStatsMetaForNonPartitionTable(ctx context.Context, buffer *statsMetaItemBuffer, statsHandler *handle.Handle,
	tbl *restoreutils.CreatedTable) error {
	count := calculateRowCountForPhysicalTable(tbl.OldTable.FilesOfPhysicals[tbl.OldTable.Info.ID])
	if statsErr := buffer.TryUpdateMetas(ctx, statsHandler, tbl.Table.ID, count); statsErr != nil {
		log.Error("update stats meta failed", zap.Error(statsErr))
		return statsErr
	}
	return nil
}

func updateStatsMetaForPartitionTable(ctx context.Context, buffer *statsMetaItemBuffer, statsHandler *handle.Handle,
	tbl *restoreutils.CreatedTable) error {
	totalCount := int64(0)
	physicalRowCountMap := make(map[int64]int64)
	for physicalID, files := range tbl.OldTable.FilesOfPhysicals {
		if physicalID == tbl.OldTable.Info.ID {
			continue
		}
		count := calculateRowCountForPhysicalTable(files)
		totalCount += count
		physicalRowCountMap[physicalID] = count
	}
	for _, oldDef := range tbl.OldTable.Info.Partition.Definitions {
		count := physicalRowCountMap[oldDef.ID]
		if count > 0 {
			newDefID, err := utils.GetPartitionByName(tbl.Table, oldDef.Name)
			if err != nil {
				log.Error("failed to get the partition by name",
					zap.String("db name", tbl.OldTable.DB.Name.O),
					zap.String("table name", tbl.Table.Name.O),
					zap.String("partition name", oldDef.Name.O),
					zap.Int64("downstream table id", tbl.Table.ID),
					zap.Int64("upstream partition id", oldDef.ID),
				)
				return errors.Trace(err)
			}
			if statsErr := buffer.TryUpdateMetas(ctx, statsHandler, newDefID, count); statsErr != nil {
				log.Error("update stats meta failed", zap.Error(statsErr))
				return statsErr
			}
		}
	}
	if statsErr := buffer.TryUpdateMetas(ctx, statsHandler, tbl.Table.ID, totalCount); statsErr != nil {
		log.Error("update stats meta failed", zap.Error(statsErr))
		return statsErr
	}
	return nil
}

func updateStatsMetaForTable(ctx context.Context, buffer *statsMetaItemBuffer, statsHandler *handle.Handle,
	tbl *restoreutils.CreatedTable) error {
	if tbl.OldTable.Info.Partition == nil {
		return updateStatsMetaForNonPartitionTable(ctx, buffer, statsHandler, tbl)
	}
	return updateStatsMetaForPartitionTable(ctx, buffer, statsHandler, tbl)
}

func (rc *SnapClient) registerUpdateMetaAndLoadStats(
	builder *PipelineConcurrentBuilder,
	s storeapi.Storage,
	updateCh glue.Progress,
	statsConcurrency uint,
) {
	statsHandler := rc.dom.StatsHandle()
	buffer := NewStatsMetaItemBuffer()

	builder.RegisterPipelineTask("Update Stats", statsConcurrency, func(c context.Context, tbl *restoreutils.CreatedTable) error {
		oldTable := tbl.OldTable
		var statsErr error = nil
		if oldTable.Stats != nil {
			log.Info("start loads analyze after validate checksum",
				zap.Int64("old id", oldTable.Info.ID),
				zap.Int64("new id", tbl.Table.ID),
			)
			start := time.Now()
			// NOTICE: skip updating cache after load stats from json
			if statsErr = statsHandler.LoadStatsFromJSONNoUpdate(c, rc.dom.InfoSchema(), oldTable.Stats, 0); statsErr != nil {
				log.Error("analyze table failed", zap.Any("table", oldTable.Stats), zap.Error(statsErr))
			}
			log.Info("restore stat done",
				zap.Stringer("table", oldTable.Info.Name),
				zap.Stringer("db", oldTable.DB.Name),
				zap.Duration("cost", time.Since(start)))
		} else if len(oldTable.StatsFileIndexes) > 0 {
			log.Info("start to load statistic data for each partition",
				zap.Int64("old id", oldTable.Info.ID),
				zap.Int64("new id", tbl.Table.ID),
			)
			start := time.Now()
			rewriteIDMap := restoreutils.GetTableIDMap(tbl.Table, tbl.OldTable.Info)
			if statsErr = metautil.RestoreStats(c, s, rc.cipher, statsHandler, tbl.Table, oldTable.StatsFileIndexes, rewriteIDMap); statsErr != nil {
				log.Error("analyze table failed", zap.Any("table", oldTable.StatsFileIndexes), zap.Error(statsErr))
			}
			log.Info("restore statistic data done",
				zap.Stringer("table", oldTable.Info.Name),
				zap.Stringer("db", oldTable.DB.Name),
				zap.Duration("cost", time.Since(start)))
		}

		if statsErr != nil || (oldTable.Stats == nil && len(oldTable.StatsFileIndexes) == 0) {
			// Not need to return err when failed because of update analysis-meta
			log.Info("start update metas", zap.Stringer("table", oldTable.Info.Name), zap.Stringer("db", oldTable.DB.Name))
			if statsErr = updateStatsMetaForTable(c, buffer, statsHandler, tbl); statsErr != nil {
				return statsErr
			}
		}
		updateCh.Inc()
		return nil
	}, func(c context.Context) error {
		if statsErr := buffer.UpdateMetasRest(c, statsHandler); statsErr != nil {
			log.Error("update stats meta failed", zap.Error(statsErr))
			return statsErr
		}
		log.Info("all stats updated")
		return nil
	})
}

func (rc *SnapClient) registerWaitTiFlashReady(
	builder *PipelineConcurrentBuilder,
	updateCh glue.Progress,
) error {
	// TODO support tiflash store changes
	tikvStats, err := infosync.GetTiFlashStoresStat(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	tiFlashStores := make(map[int64]pdhttp.StoreInfo)
	for _, store := range tikvStats.Stores {
		// Note that only TiFlash write nodes need to be polled under NextGen kernel.
		// TiFlash compute nodes under NextGen kernel do not hold any Regions data, so it is excluded here.
		if engine.IsTiFlashWriteHTTPResp(&store.Store) {
			tiFlashStores[store.Store.ID] = store
		}
	}

	builder.RegisterPipelineTask("Wait For Tiflash Ready", 4, func(c context.Context, tbl *restoreutils.CreatedTable) error {
		if tbl.Table != nil && tbl.Table.TiFlashReplica == nil {
			log.Info("table has no tiflash replica",
				zap.Stringer("table", tbl.OldTable.Info.Name),
				zap.Stringer("db", tbl.OldTable.DB.Name))
			updateCh.Inc()
			return nil
		}
		if rc.dom == nil {
			// unreachable, current we have initial domain in mgr.
			log.Fatal("unreachable, domain is nil")
		}
		log.Info("table has tiflash replica, start sync..",
			zap.Stringer("table", tbl.OldTable.Info.Name),
			zap.Stringer("db", tbl.OldTable.DB.Name))
		for {
			var progress float64
			if pi := tbl.Table.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
				for _, p := range pi.Definitions {
					progressOfPartition, err := infosync.MustGetTiFlashProgress(p.ID, tbl.Table.TiFlashReplica.Count, &tiFlashStores)
					if err != nil {
						log.Warn("failed to get progress for tiflash partition replica, retry it",
							zap.Int64("tableID", tbl.Table.ID), zap.Int64("partitionID", p.ID), zap.Error(err))
						time.Sleep(time.Second)
						continue
					}
					progress += progressOfPartition
				}
				progress = progress / float64(len(pi.Definitions))
			} else {
				var err error
				progress, err = infosync.MustGetTiFlashProgress(tbl.Table.ID, tbl.Table.TiFlashReplica.Count, &tiFlashStores)
				if err != nil {
					log.Warn("failed to get progress for tiflash replica, retry it",
						zap.Int64("tableID", tbl.Table.ID), zap.Error(err))
					time.Sleep(time.Second)
					continue
				}
			}
			// check until progress is 1
			if progress == 1 {
				log.Info("tiflash replica synced",
					zap.Stringer("table", tbl.OldTable.Info.Name),
					zap.Stringer("db", tbl.OldTable.DB.Name))
				break
			}
			// just wait for next check
			// tiflash check the progress every 2s
			// we can wait 2.5x times
			time.Sleep(5 * time.Second)
		}
		updateCh.Inc()
		return nil
	}, func(context.Context) error {
		log.Info("all tiflash replica synced")
		return nil
	})
	return nil
}
