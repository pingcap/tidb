// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultChannelSize = 1024
)

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

// ContextManager is the struct to manage a TiKV 'context' for restore.
// Batcher will call Enter when any table should be restore on batch,
// so you can do some prepare work here(e.g. set placement rules for online restore).
type ContextManager interface {
	// Enter make some tables 'enter' this context(a.k.a., prepare for restore).
	Enter(ctx context.Context, tables []CreatedTable) error
	// Leave make some tables 'leave' this context(a.k.a., restore is done, do some post-works).
	Leave(ctx context.Context, tables []CreatedTable) error
	// Close closes the context manager, sometimes when the manager is 'killed' and should do some cleanup
	// it would be call.
	Close(ctx context.Context)
}

// NewBRContextManager makes a BR context manager, that is,
// set placement rules for online restore when enter(see <splitPrepareWork>),
// unset them when leave.
func NewBRContextManager(client *Client) ContextManager {
	return &brContextManager{
		client: client,

		hasTable: make(map[int64]CreatedTable),
	}
}

type brContextManager struct {
	client *Client

	// This 'set' of table ID allow us to handle each table just once.
	hasTable map[int64]CreatedTable
	mu       sync.Mutex
}

func (manager *brContextManager) Close(ctx context.Context) {
	tbls := make([]*model.TableInfo, 0, len(manager.hasTable))
	for _, tbl := range manager.hasTable {
		tbls = append(tbls, tbl.Table)
	}
	splitPostWork(ctx, manager.client, tbls)
}

func (manager *brContextManager) Enter(ctx context.Context, tables []CreatedTable) error {
	placementRuleTables := make([]*model.TableInfo, 0, len(tables))
	manager.mu.Lock()
	defer manager.mu.Unlock()

	for _, tbl := range tables {
		if _, ok := manager.hasTable[tbl.Table.ID]; !ok {
			placementRuleTables = append(placementRuleTables, tbl.Table)
		}
		manager.hasTable[tbl.Table.ID] = tbl
	}

	return splitPrepareWork(ctx, manager.client, placementRuleTables)
}

func (manager *brContextManager) Leave(ctx context.Context, tables []CreatedTable) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	placementRuleTables := make([]*model.TableInfo, 0, len(tables))

	for _, table := range tables {
		placementRuleTables = append(placementRuleTables, table.Table)
	}

	splitPostWork(ctx, manager.client, placementRuleTables)
	log.Info("restore table done", ZapTables(tables))
	for _, tbl := range placementRuleTables {
		delete(manager.hasTable, tbl.ID)
	}
	return nil
}

func splitPostWork(ctx context.Context, client *Client, tables []*model.TableInfo) {
	err := client.ResetPlacementRules(ctx, tables)
	if err != nil {
		log.Warn("reset placement rules failed", zap.Error(err))
		return
	}
}

func splitPrepareWork(ctx context.Context, client *Client, tables []*model.TableInfo) error {
	err := client.SetupPlacementRules(ctx, tables)
	if err != nil {
		log.Error("setup placement rules failed", zap.Error(err))
		return errors.Trace(err)
	}

	err = client.WaitPlacementSchedule(ctx, tables)
	if err != nil {
		log.Error("wait placement schedule failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// CreatedTable is a table created on restore process,
// but not yet filled with data.
type CreatedTable struct {
	RewriteRule *RewriteRules
	Table       *model.TableInfo
	OldTable    *metautil.Table
}

// TableWithRange is a CreatedTable that has been bind to some of key ranges.
type TableWithRange struct {
	CreatedTable

	Range []rtree.Range
}

// Exhaust drains all remaining errors in the channel, into a slice of errors.
func Exhaust(ec <-chan error) []error {
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
		rewriteRules *RewriteRules,
		updateCh glue.Progress,
		isRawKv bool) error
	// RestoreSSTFiles import the files to the TiKV.
	RestoreSSTFiles(ctx context.Context,
		files []*backuppb.File,
		rewriteRules *RewriteRules,
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
	go sender.restoreWorker(ctx, midCh)
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

	pool := utils.NewWorkerPool(concurrency, "split")
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
				err := b.client.SplitRanges(ectx, result.Ranges, result.RewriteRules, b.updateCh, false)
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
				e := b.client.RestoreSSTFiles(ectx, files, r.result.RewriteRules, b.updateCh)
				if e != nil {
					log.Error("restore batch meet error", logutil.ShortError(e), logutil.Files(files))
					r.done()
					return e
				}
				log.Info("restore batch done", rtree.ZapRanges(r.result.Ranges))
				r.done()
				b.waitTablesDone(r.result.BlankTablesAfterSend)
				b.sink.EmitTables(r.result.BlankTablesAfterSend...)
				return nil
			})
		}
	}
}
