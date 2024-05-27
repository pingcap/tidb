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
	"sort"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	tidallocdb "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_db"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
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

// TableSink is the 'sink' of restored data by a sender.
type TableSink interface {
	EmitTables(tables ...CreatedTable)
	EmitError(error)
	Close()
}

type chanTableSink struct {
	outCh chan<- []CreatedTable
	errCh chan<- error
}

func (sink chanTableSink) EmitTables(tables ...CreatedTable) {
	sink.outCh <- tables
}

func (sink chanTableSink) EmitError(err error) {
	sink.errCh <- err
}

func (sink chanTableSink) Close() {
	// ErrCh may has multi sender part, don't close it.
	close(sink.outCh)
}

// CreatedTable is a table created on restore process,
// but not yet filled with data.
type CreatedTable struct {
	RewriteRule *restoreutils.RewriteRules
	Table       *model.TableInfo
	OldTable    *metautil.Table
}

func defaultOutputTableChan() chan *CreatedTable {
	return make(chan *CreatedTable, defaultChannelSize)
}

// TableWithRange is a CreatedTable that has been bind to some of key ranges.
type TableWithRange struct {
	CreatedTable

	// Range has been rewrited by rewrite rules.
	Range []rtree.Range
}

type TableIDWithFiles struct {
	TableID int64

	Files []*backuppb.File
	// RewriteRules is the rewrite rules for the specify table.
	// because these rules belongs to the *one table*.
	// we can hold them here.
	RewriteRules *restoreutils.RewriteRules
}

// BatchSender is the abstract of how the batcher send a batch.
type BatchSender interface {
	// PutSink sets the sink of this sender, user to this interface promise
	// call this function at least once before first call to `RestoreBatch`.
	PutSink(sink TableSink)
	// RestoreBatch will send the restore request.
	RestoreBatch(ranges DrainResult)
	Close()
}

// TiKVRestorer is the minimal methods required for restoring.
// It contains the primitive APIs extract from `restore.Client`, so some of arguments may seem redundant.
// Maybe TODO: make a better abstraction?
type TiKVRestorer interface {
	// SplitRanges split regions implicated by the ranges and rewrite rules.
	// After spliting, it also scatters the fresh regions.
	SplitRanges(ctx context.Context,
		ranges []rtree.Range,
		updateCh glue.Progress,
		isRawKv bool) error
	// RestoreSSTFiles import the files to the TiKV.
	RestoreSSTFiles(ctx context.Context,
		tableIDWithFiles []TableIDWithFiles,
		updateCh glue.Progress) error
}

type tikvSender struct {
	client TiKVRestorer

	updateCh glue.Progress

	sink TableSink
	inCh chan<- DrainResult

	wg *sync.WaitGroup

	tableWaiters *sync.Map
}

func (b *tikvSender) PutSink(sink TableSink) {
	// don't worry about visibility, since we will call this before first call to
	// RestoreBatch, which is a sync point.
	b.sink = sink
}

func (b *tikvSender) RestoreBatch(ranges DrainResult) {
	log.Info("restore batch: waiting ranges", zap.Int("range", len(b.inCh)))
	b.inCh <- ranges
}

// NewTiKVSender make a sender that send restore requests to TiKV.
func NewTiKVSender(
	ctx context.Context,
	cli TiKVRestorer,
	updateCh glue.Progress,
	splitConcurrency uint,
) (BatchSender, error) {
	inCh := make(chan DrainResult, defaultChannelSize)
	midCh := make(chan drainResultAndDone, defaultChannelSize)

	sender := &tikvSender{
		client:       cli,
		updateCh:     updateCh,
		inCh:         inCh,
		wg:           new(sync.WaitGroup),
		tableWaiters: new(sync.Map),
	}

	sender.wg.Add(2)
	go sender.splitWorker(ctx, inCh, midCh, splitConcurrency)
	outCh := make(chan drainResultAndDone, defaultChannelSize)
	// block on splitting and scattering regions.
	// in coarse-grained mode, wait all regions are split and scattered is
	// no longer a time-consuming operation, then we can batch download files
	// as much as enough and reduce the time of blocking restore.
	go sender.blockPipelineWorker(ctx, midCh, outCh)
	go sender.restoreWorker(ctx, outCh)
	return sender, nil
}

func (b *tikvSender) Close() {
	close(b.inCh)
	b.wg.Wait()
	log.Debug("tikv sender closed")
}

type drainResultAndDone struct {
	result DrainResult
	done   func()
}

func (b *tikvSender) blockPipelineWorker(ctx context.Context,
	inCh <-chan drainResultAndDone,
	outCh chan<- drainResultAndDone,
) {
	defer close(outCh)
	res := make([]drainResultAndDone, 0, defaultChannelSize)
	for dr := range inCh {
		res = append(res, dr)
	}

	for _, dr := range res {
		select {
		case <-ctx.Done():
			return
		default:
			outCh <- dr
		}
	}
}

func (b *tikvSender) splitWorker(ctx context.Context,
	ranges <-chan DrainResult,
	next chan<- drainResultAndDone,
	concurrency uint,
) {
	defer log.Debug("split worker closed")
	eg, ectx := errgroup.WithContext(ctx)
	defer func() {
		b.wg.Done()
		if err := eg.Wait(); err != nil {
			b.sink.EmitError(err)
		}
		close(next)
		log.Info("TiKV Sender: split worker exits.")
	}()

	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		summary.CollectDuration("split region", elapsed)
	}()

	pool := tidbutil.NewWorkerPool(concurrency, "split")
	for {
		select {
		case <-ectx.Done():
			return
		case result, ok := <-ranges:
			if !ok {
				return
			}
			// When the batcher has sent all ranges from a table, it would
			// mark this table 'all done'(BlankTablesAfterSend), and then we can send it to checksum.
			//
			// When there a sole worker sequentially running those batch tasks, everything is fine, however,
			// in the context of multi-workers, that become buggy, for example:
			// |------table 1, ranges 1------|------table 1, ranges 2------|
			// The batcher send batches: [
			//		{Ranges: ranges 1},
			// 		{Ranges: ranges 2, BlankTablesAfterSend: table 1}
			// ]
			// And there are two workers runs concurrently:
			// 		worker 1: {Ranges: ranges 1}
			//      worker 2: {Ranges: ranges 2, BlankTablesAfterSend: table 1}
			// And worker 2 finished its job before worker 1 done. Note the table wasn't restored fully,
			// hence the checksum would fail.
			done := b.registerTableIsRestoring(result.TablesToSend)
			pool.ApplyOnErrorGroup(eg, func() error {
				err := b.client.SplitRanges(ectx, result.Ranges, b.updateCh, false)
				if err != nil {
					log.Error("failed on split range", rtree.ZapRanges(result.Ranges), zap.Error(err))
					return err
				}
				next <- drainResultAndDone{
					result: result,
					done:   done,
				}
				return nil
			})
		}
	}
}

// registerTableIsRestoring marks some tables as 'current restoring'.
// Returning a function that mark the restore has been done.
func (b *tikvSender) registerTableIsRestoring(ts []CreatedTable) func() {
	wgs := make([]*sync.WaitGroup, 0, len(ts))
	for _, t := range ts {
		i, _ := b.tableWaiters.LoadOrStore(t.Table.ID, new(sync.WaitGroup))
		wg := i.(*sync.WaitGroup)
		wg.Add(1)
		wgs = append(wgs, wg)
	}
	return func() {
		for _, wg := range wgs {
			wg.Done()
		}
	}
}

// waitTablesDone block the current goroutine,
// till all tables provided are no more ‘current restoring’.
func (b *tikvSender) waitTablesDone(ts []CreatedTable) {
	for _, t := range ts {
		wg, ok := b.tableWaiters.LoadAndDelete(t.Table.ID)
		if !ok {
			log.Panic("bug! table done before register!",
				zap.Any("wait-table-map", b.tableWaiters),
				zap.Stringer("table", t.Table.Name))
		}
		wg.(*sync.WaitGroup).Wait()
	}
}

func (b *tikvSender) restoreWorker(ctx context.Context, ranges <-chan drainResultAndDone) {
	eg, ectx := errgroup.WithContext(ctx)
	defer func() {
		log.Info("TiKV Sender: restore worker prepare to close.")
		if err := eg.Wait(); err != nil {
			b.sink.EmitError(err)
		}
		b.sink.Close()
		b.wg.Done()
		log.Info("TiKV Sender: restore worker exits.")
	}()
	for {
		select {
		case <-ectx.Done():
			return
		case r, ok := <-ranges:
			if !ok {
				return
			}

			files := r.result.Files()
			// There has been a worker in the `RestoreSSTFiles` procedure.
			// Spawning a raw goroutine won't make too many requests to TiKV.
			eg.Go(func() error {
				e := b.client.RestoreSSTFiles(ectx, files, b.updateCh)
				if e != nil {
					log.Error("restore batch meet error", logutil.ShortError(e), zapTableIDWithFiles(files))
					r.done()
					return e
				}
				log.Info("restore batch done", rtree.ZapRanges(r.result.Ranges), zapTableIDWithFiles(files))
				r.done()
				b.waitTablesDone(r.result.BlankTablesAfterSend)
				b.sink.EmitTables(r.result.BlankTablesAfterSend...)
				return nil
			})
		}
	}
}

func concurrentHandleTablesCh(
	ctx context.Context,
	inCh <-chan *CreatedTable,
	outCh chan<- *CreatedTable,
	errCh chan<- error,
	workers *tidbutil.WorkerPool,
	processFun func(context.Context, *CreatedTable) error,
	deferFun func()) {
	eg, ectx := errgroup.WithContext(ctx)
	defer func() {
		if err := eg.Wait(); err != nil {
			errCh <- err
		}
		close(outCh)
		deferFun()
	}()

	for {
		select {
		// if we use ectx here, maybe canceled will mask real error.
		case <-ctx.Done():
			errCh <- ctx.Err()
		case tbl, ok := <-inCh:
			if !ok {
				return
			}
			cloneTable := tbl
			worker := workers.ApplyWorker()
			eg.Go(func() error {
				defer workers.RecycleWorker(worker)
				err := processFun(ectx, cloneTable)
				if err != nil {
					return err
				}
				outCh <- cloneTable
				return nil
			})
		}
	}
}

// GoCreateTables create tables, and generate their information.
// this function will use workers as the same number of sessionPool,
// leave sessionPool nil to send DDLs sequential.
func (rc *SnapClient) GoCreateTables(
	ctx context.Context,
	tables []*metautil.Table,
	newTS uint64,
	errCh chan<- error,
) <-chan CreatedTable {
	// Could we have a smaller size of tables?
	log.Info("start create tables")

	rc.generateRebasedTables(tables)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.GoCreateTables", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	outCh := make(chan CreatedTable, len(tables))
	rater := logutil.TraceRateOver(logutil.MetricTableCreatedCounter)

	var err error

	if rc.batchDdlSize > minBatchDdlSize && len(rc.dbPool) > 0 {
		err = rc.createTablesInWorkerPool(ctx, tables, newTS, outCh)
		if err == nil {
			defer log.Debug("all tables are created")
			close(outCh)
			return outCh
		} else if !utils.FallBack2CreateTable(err) {
			errCh <- err
			close(outCh)
			return outCh
		}
		// fall back to old create table (sequential create table)
		log.Info("fall back to the sequential create table")
	}

	createOneTable := func(c context.Context, db *tidallocdb.DB, t *metautil.Table) error {
		select {
		case <-c.Done():
			return c.Err()
		default:
		}
		rt, err := rc.createTable(c, db, t, newTS)
		if err != nil {
			log.Error("create table failed",
				zap.Error(err),
				zap.Stringer("db", t.DB.Name),
				zap.Stringer("table", t.Info.Name))
			return errors.Trace(err)
		}
		log.Debug("table created and send to next",
			zap.Int("output chan size", len(outCh)),
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name))
		outCh <- rt
		rater.Inc()
		rater.L().Info("table created",
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name))
		return nil
	}
	go func() {
		defer close(outCh)
		defer log.Debug("all tables are created")
		var err error
		if len(rc.dbPool) > 0 {
			err = rc.createTablesWithDBPool(ctx, createOneTable, tables)
		} else {
			err = rc.createTablesWithSoleDB(ctx, createOneTable, tables)
		}
		if err != nil {
			errCh <- err
		}
	}()

	return outCh
}

func (rc *SnapClient) GoBlockCreateTablesPipeline(ctx context.Context, sz int, inCh <-chan CreatedTable) <-chan CreatedTable {
	outCh := make(chan CreatedTable, sz)

	go func() {
		defer close(outCh)
		cachedTables := make([]CreatedTable, 0, sz)
		for tbl := range inCh {
			cachedTables = append(cachedTables, tbl)
		}

		sort.Slice(cachedTables, func(a, b int) bool {
			return cachedTables[a].Table.ID < cachedTables[b].Table.ID
		})

		for _, tbl := range cachedTables {
			select {
			case <-ctx.Done():
				return
			default:
				outCh <- tbl
			}
		}
	}()
	return outCh
}

// GoValidateFileRanges validate files by a stream of tables and yields
// tables with range.
func (rc *SnapClient) GoValidateFileRanges(
	ctx context.Context,
	tableStream <-chan CreatedTable,
	fileOfTable map[int64][]*backuppb.File,
	splitSizeBytes, splitKeyCount uint64,
	errCh chan<- error,
) <-chan TableWithRange {
	// Could we have a smaller outCh size?
	outCh := make(chan TableWithRange, len(fileOfTable))
	go func() {
		defer close(outCh)
		defer log.Info("all range generated")
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case t, ok := <-tableStream:
				if !ok {
					return
				}
				files := fileOfTable[t.OldTable.Info.ID]
				if partitions := t.OldTable.Info.Partition; partitions != nil {
					log.Debug("table partition",
						zap.Stringer("database", t.OldTable.DB.Name),
						zap.Stringer("table", t.Table.Name),
						zap.Any("partition info", partitions),
					)
					for _, partition := range partitions.Definitions {
						files = append(files, fileOfTable[partition.ID]...)
					}
				}
				for _, file := range files {
					err := restoreutils.ValidateFileRewriteRule(file, t.RewriteRule)
					if err != nil {
						errCh <- err
						return
					}
				}
				// Merge small ranges to reduce split and scatter regions.
				ranges, stat, err := restoreutils.MergeAndRewriteFileRanges(
					files, t.RewriteRule, splitSizeBytes, splitKeyCount)
				if err != nil {
					errCh <- err
					return
				}
				log.Info("merge and validate file",
					zap.Stringer("database", t.OldTable.DB.Name),
					zap.Stringer("table", t.Table.Name),
					zap.Int("Files(total)", stat.TotalFiles),
					zap.Int("File(write)", stat.TotalWriteCFFile),
					zap.Int("File(default)", stat.TotalDefaultCFFile),
					zap.Int("Region(total)", stat.TotalRegions),
					zap.Int("Regoin(keys avg)", stat.RegionKeysAvg),
					zap.Int("Region(bytes avg)", stat.RegionBytesAvg),
					zap.Int("Merged(regions)", stat.MergedRegions),
					zap.Int("Merged(keys avg)", stat.MergedRegionKeysAvg),
					zap.Int("Merged(bytes avg)", stat.MergedRegionBytesAvg))

				tableWithRange := TableWithRange{
					CreatedTable: t,
					Range:        ranges,
				}
				log.Debug("sending range info",
					zap.Stringer("table", t.Table.Name),
					zap.Int("files", len(files)),
					zap.Int("range size", len(ranges)),
					zap.Int("output channel size", len(outCh)))
				outCh <- tableWithRange
			}
		}
	}()
	return outCh
}

// GoValidateChecksum forks a goroutine to validate checksum after restore.
// it returns a channel fires a struct{} when all things get done.
func (rc *SnapClient) GoValidateChecksum(
	ctx context.Context,
	inCh <-chan *CreatedTable,
	kvClient kv.Client,
	errCh chan<- error,
	updateCh glue.Progress,
	concurrency uint,
) chan *CreatedTable {
	log.Info("Start to validate checksum")
	outCh := defaultOutputTableChan()
	workers := tidbutil.NewWorkerPool(defaultChecksumConcurrency, "RestoreChecksum")
	go concurrentHandleTablesCh(ctx, inCh, outCh, errCh, workers, func(c context.Context, tbl *CreatedTable) error {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			summary.CollectSuccessUnit("table checksum", 1, elapsed)
		}()
		err := rc.execChecksum(c, tbl, kvClient, concurrency)
		if err != nil {
			return errors.Trace(err)
		}
		updateCh.Inc()
		return nil
	}, func() {
		log.Info("all checksum ended")
	})
	return outCh
}

func (rc *SnapClient) GoUpdateMetaAndLoadStats(
	ctx context.Context,
	s storage.ExternalStorage,
	inCh <-chan *CreatedTable,
	errCh chan<- error,
	statsConcurrency uint,
	loadStats bool,
) chan *CreatedTable {
	log.Info("Start to update meta then load stats")
	outCh := defaultOutputTableChan()
	workers := tidbutil.NewWorkerPool(statsConcurrency, "UpdateStats")
	statsHandler := rc.dom.StatsHandle()

	go concurrentHandleTablesCh(ctx, inCh, outCh, errCh, workers, func(c context.Context, tbl *CreatedTable) error {
		oldTable := tbl.OldTable
		var statsErr error = nil
		if loadStats && oldTable.Stats != nil {
			log.Info("start loads analyze after validate checksum",
				zap.Int64("old id", oldTable.Info.ID),
				zap.Int64("new id", tbl.Table.ID),
			)
			start := time.Now()
			// NOTICE: skip updating cache after load stats from json
			if statsErr = statsHandler.LoadStatsFromJSONNoUpdate(ctx, rc.dom.InfoSchema(), oldTable.Stats, 0); statsErr != nil {
				log.Error("analyze table failed", zap.Any("table", oldTable.Stats), zap.Error(statsErr))
			}
			log.Info("restore stat done",
				zap.Stringer("table", oldTable.Info.Name),
				zap.Stringer("db", oldTable.DB.Name),
				zap.Duration("cost", time.Since(start)))
		} else if loadStats && len(oldTable.StatsFileIndexes) > 0 {
			log.Info("start to load statistic data for each partition",
				zap.Int64("old id", oldTable.Info.ID),
				zap.Int64("new id", tbl.Table.ID),
			)
			start := time.Now()
			rewriteIDMap := restoreutils.GetTableIDMap(tbl.Table, tbl.OldTable.Info)
			if statsErr = metautil.RestoreStats(ctx, s, rc.cipher, statsHandler, tbl.Table, oldTable.StatsFileIndexes, rewriteIDMap); statsErr != nil {
				log.Error("analyze table failed", zap.Any("table", oldTable.StatsFileIndexes), zap.Error(statsErr))
			}
			log.Info("restore statistic data done",
				zap.Stringer("table", oldTable.Info.Name),
				zap.Stringer("db", oldTable.DB.Name),
				zap.Duration("cost", time.Since(start)))
		}

		if statsErr != nil || !loadStats || (oldTable.Stats == nil && len(oldTable.StatsFileIndexes) == 0) {
			// Not need to return err when failed because of update analysis-meta
			log.Info("start update metas", zap.Stringer("table", oldTable.Info.Name), zap.Stringer("db", oldTable.DB.Name))
			// the total kvs contains the index kvs, but the stats meta needs the count of rows
			count := int64(oldTable.TotalKvs / uint64(len(oldTable.Info.Indices)+1))
			if statsErr = statsHandler.SaveMetaToStorage(tbl.Table.ID, count, 0, "br restore"); statsErr != nil {
				log.Error("update stats meta failed", zap.Any("table", tbl.Table), zap.Error(statsErr))
			}
		}
		return nil
	}, func() {
		log.Info("all stats updated")
	})
	return outCh
}

func (rc *SnapClient) GoWaitTiFlashReady(
	ctx context.Context,
	inCh <-chan *CreatedTable,
	updateCh glue.Progress,
	errCh chan<- error,
) chan *CreatedTable {
	log.Info("Start to wait tiflash replica sync")
	outCh := defaultOutputTableChan()
	workers := tidbutil.NewWorkerPool(4, "WaitForTiflashReady")
	// TODO support tiflash store changes
	tikvStats, err := infosync.GetTiFlashStoresStat(context.Background())
	if err != nil {
		errCh <- err
	}
	tiFlashStores := make(map[int64]pdhttp.StoreInfo)
	for _, store := range tikvStats.Stores {
		if engine.IsTiFlashHTTPResp(&store.Store) {
			tiFlashStores[store.Store.ID] = store
		}
	}
	go concurrentHandleTablesCh(ctx, inCh, outCh, errCh, workers, func(c context.Context, tbl *CreatedTable) error {
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
	}, func() {
		log.Info("all tiflash replica synced")
	})
	return outCh
}
