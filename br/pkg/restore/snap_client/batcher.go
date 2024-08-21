// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package snapclient

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/summary"
	"go.uber.org/zap"
)

// SendType is the 'type' of a send.
// when we make a 'send' command to worker, we may want to flush all pending ranges (when auto commit enabled),
// or, we just want to clean overflowing ranges(when just adding a table to batcher).
type SendType int

const (
	// SendUntilLessThanBatch will make the batcher send batch until
	// its remaining range is less than its batchSizeThreshold.
	SendUntilLessThanBatch SendType = iota
	// SendAll will make the batcher send all pending ranges.
	SendAll
	// SendAllThenClose will make the batcher send all pending ranges and then close itself.
	SendAllThenClose
)

// Batcher collects ranges to restore and send batching split/ingest request.
type Batcher struct {
	cachedTables   []TableWithRange
	cachedTablesMu *sync.Mutex

	// autoCommitJoiner is for joining the background batch sender.
	autoCommitJoiner chan<- struct{}
	// everythingIsDone is for waiting for worker done: that is, after we send a
	// signal to autoCommitJoiner, we must give it enough time to get things done.
	// Then, it should notify us by this wait group.
	// Use wait group instead of a trivial channel for further extension.
	everythingIsDone *sync.WaitGroup
	// sendErr is for output error information.
	sendErr chan<- error
	// sendCh is for communiate with sendWorker.
	sendCh chan<- SendType
	// outCh is for output the restored table, so it can be sent to do something like checksum.
	outCh chan<- *CreatedTable

	updateCh glue.Progress

	sender             BatchSender
	manager            ContextManager
	batchSizeThreshold int
	size               int32

	checkpointSetWithTableID map[int64]map[string]struct{}
}

// Len calculate the current size of this batcher.
func (b *Batcher) Len() int {
	return int(atomic.LoadInt32(&b.size))
}

// contextCleaner is the worker goroutine that cleaning the 'context'
// (e.g. make regions leave restore mode).
func (b *Batcher) contextCleaner(ctx context.Context, tables <-chan []CreatedTable) {
	defer func() {
		if ctx.Err() != nil {
			log.Info("restore canceled, cleaning in background context")
			b.manager.Close(context.Background())
		} else {
			b.manager.Close(ctx)
		}
	}()
	defer b.everythingIsDone.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case tbls, ok := <-tables:
			if !ok {
				return
			}
			if err := b.manager.Leave(ctx, tbls); err != nil {
				b.sendErr <- err
				return
			}
			for _, tbl := range tbls {
				cloneTable := tbl
				b.outCh <- &cloneTable
			}
		}
	}
}

// NewBatcher creates a new batcher by a sender and a context manager.
// the former defines how the 'restore' a batch(i.e. send, or 'push down' the task to where).
// the context manager defines the 'lifetime' of restoring tables(i.e. how to enter 'restore' mode, and how to exit).
// this batcher will work background, send batches per second, or batch size reaches limit.
// and it will emit full-restored tables to the output channel returned.
func NewBatcher(
	ctx context.Context,
	sender BatchSender,
	manager ContextManager,
	errCh chan<- error,
	updateCh glue.Progress,
) (*Batcher, chan *CreatedTable) {
	outCh := defaultOutputTableChan()
	sendChan := make(chan SendType, 2)
	b := &Batcher{
		sendErr:            errCh,
		outCh:              outCh,
		sender:             sender,
		manager:            manager,
		sendCh:             sendChan,
		updateCh:           updateCh,
		cachedTablesMu:     new(sync.Mutex),
		everythingIsDone:   new(sync.WaitGroup),
		batchSizeThreshold: 1,
	}
	b.everythingIsDone.Add(2)
	go b.sendWorker(ctx, sendChan)
	restoredTables := make(chan []CreatedTable, defaultChannelSize)
	go b.contextCleaner(ctx, restoredTables)
	sink := chanTableSink{restoredTables, errCh}
	sender.PutSink(sink)
	return b, outCh
}

// EnableAutoCommit enables the batcher commit batch periodically even batcher size isn't big enough.
// we make this function for disable AutoCommit in some case.
func (b *Batcher) EnableAutoCommit(ctx context.Context, delay time.Duration) {
	if b.autoCommitJoiner != nil {
		// IMO, making two auto commit goroutine wouldn't be a good idea.
		// If desire(e.g. change the peroid of auto commit), please disable auto commit firstly.
		log.L().DPanic("enabling auto commit on a batcher that auto commit has been enabled, which isn't allowed")
	}
	joiner := make(chan struct{})
	go b.autoCommitWorker(ctx, joiner, delay)
	b.autoCommitJoiner = joiner
}

// DisableAutoCommit blocks the current goroutine until the worker can gracefully stop,
// and then disable auto commit.
func (b *Batcher) DisableAutoCommit() {
	b.joinAutoCommitWorker()
	b.autoCommitJoiner = nil
}

func (b *Batcher) waitUntilSendDone() {
	b.sendCh <- SendAllThenClose
	b.everythingIsDone.Wait()
}

// joinAutoCommitWorker blocks the current goroutine until the worker can gracefully stop.
// return immediately when auto commit disabled.
func (b *Batcher) joinAutoCommitWorker() {
	if b.autoCommitJoiner != nil {
		log.Debug("gracefully stopping worker goroutine")
		b.autoCommitJoiner <- struct{}{}
		close(b.autoCommitJoiner)
		log.Debug("gracefully stopped worker goroutine")
	}
}

// sendWorker is the 'worker' that send all ranges to TiKV.
// TODO since all operations are asynchronous now, it's possible to remove this worker.
func (b *Batcher) sendWorker(ctx context.Context, send <-chan SendType) {
	sendUntil := func(lessOrEqual int) {
		for b.Len() > lessOrEqual {
			b.Send(ctx)
		}
	}

	for sendType := range send {
		switch sendType {
		case SendUntilLessThanBatch:
			sendUntil(b.batchSizeThreshold)
		case SendAll:
			sendUntil(0)
		case SendAllThenClose:
			sendUntil(0)
			b.sender.Close()
			b.everythingIsDone.Done()
			return
		}
	}
}

func (b *Batcher) autoCommitWorker(ctx context.Context, joiner <-chan struct{}, delay time.Duration) {
	tick := time.NewTicker(delay)
	defer tick.Stop()
	for {
		select {
		case <-joiner:
			log.Debug("graceful stop signal received")
			return
		case <-ctx.Done():
			b.sendErr <- ctx.Err()
			return
		case <-tick.C:
			if b.Len() > 0 {
				log.Debug("sending batch because time limit exceed", zap.Int("size", b.Len()))
				b.asyncSend(SendAll)
			}
		}
	}
}

func (b *Batcher) asyncSend(t SendType) {
	// add a check here so we won't replica sending.
	if len(b.sendCh) == 0 {
		b.sendCh <- t
	}
}

// DrainResult is the collection of some ranges and theirs metadata.
type DrainResult struct {
	// TablesToSend are tables that would be send at this batch.
	TablesToSend []CreatedTable
	// BlankTablesAfterSend are tables that will be full-restored after this batch send.
	BlankTablesAfterSend []CreatedTable
	// RewriteRules are the rewrite rules for the tables.
	// the key is the table id after rewritten.
	RewriteRulesMap map[int64]*utils.RewriteRules
	Ranges          []rtree.Range
	// Record which part of ranges belongs to the table
	TableEndOffsetInRanges []int
}

// Files returns all files of this drain result.
func (result DrainResult) Files() []TableIDWithFiles {
	tableIDWithFiles := make([]TableIDWithFiles, 0, len(result.TableEndOffsetInRanges))
	var startOffset int = 0
	for i, endOffset := range result.TableEndOffsetInRanges {
		tableID := result.TablesToSend[i].Table.ID
		ranges := result.Ranges[startOffset:endOffset]
		// each range has at least a default file + a write file
		files := make([]*backuppb.File, 0, len(ranges)*2)
		for _, rg := range ranges {
			files = append(files, rg.Files...)
		}
		var rules *utils.RewriteRules
		if r, ok := result.RewriteRulesMap[tableID]; ok {
			rules = r
		}
		tableIDWithFiles = append(tableIDWithFiles, TableIDWithFiles{
			TableID:      tableID,
			Files:        files,
			RewriteRules: rules,
		})

		// update start offset
		startOffset = endOffset
	}

	return tableIDWithFiles
}

func newDrainResult() DrainResult {
	return DrainResult{
		TablesToSend:           make([]CreatedTable, 0),
		BlankTablesAfterSend:   make([]CreatedTable, 0),
		RewriteRulesMap:        utils.EmptyRewriteRulesMap(),
		Ranges:                 make([]rtree.Range, 0),
		TableEndOffsetInRanges: make([]int, 0),
	}
}

// fileterOutRanges filter out the files from `drained-range` that exists in the checkpoint set.
func (b *Batcher) filterOutRanges(checkpointSet map[string]struct{}, drained []rtree.Range) []rtree.Range {
	progress := int(0)
	totalKVs := uint64(0)
	totalBytes := uint64(0)
	for i, rg := range drained {
		newFiles := make([]*backuppb.File, 0, len(rg.Files))
		for _, f := range rg.Files {
			rangeKey := getFileRangeKey(f.Name)
			if _, exists := checkpointSet[rangeKey]; exists {
				// the range has been import done, so skip it and
				// update the summary information
				progress += 1
				totalKVs += f.TotalKvs
				totalBytes += f.TotalBytes
			} else {
				newFiles = append(newFiles, f)
			}
		}
		// the newFiles may be empty
		drained[i].Files = newFiles
	}
	if progress > 0 {
		// (split/scatter + download/ingest) / (default cf + write cf)
		b.updateCh.IncBy(int64(progress) * 2 / 2)
		summary.CollectSuccessUnit(summary.TotalKV, progress, totalKVs)
		summary.CollectSuccessUnit(summary.SkippedKVCountByCheckpoint, progress, totalKVs)
		summary.CollectSuccessUnit(summary.TotalBytes, progress, totalBytes)
		summary.CollectSuccessUnit(summary.SkippedBytesByCheckpoint, progress, totalBytes)
	}
	return drained
}

// drainRanges 'drains' ranges from current tables.
// for example, let a '-' character be a range, assume we have:
// |---|-----|-------|
// |t1 |t2   |t3     |
// after we run drainRanges() with batchSizeThreshold = 6, let '*' be the ranges will be sent this batch :
// |***|***--|-------|
// |t1 |t2   |-------|
//
// drainRanges() will return:
// TablesToSend: [t1, t2] (so we can make them enter restore mode)
// BlankTableAfterSend: [t1] (so we can make them leave restore mode after restoring this batch)
// RewriteRules: rewrite rules for [t1, t2] (so we can restore them)
// Ranges: those stared ranges (so we can restore them)
//
// then, it will leaving the batcher's cachedTables like this:
// |--|-------|
// |t2|t3     |
// as you can see, all restored ranges would be removed.
func (b *Batcher) drainRanges() DrainResult {
	result := newDrainResult()

	b.cachedTablesMu.Lock()
	defer b.cachedTablesMu.Unlock()

	for offset, thisTable := range b.cachedTables {
		t, exists := b.checkpointSetWithTableID[thisTable.Table.ID]
		thisTableLen := len(thisTable.Range)
		collected := len(result.Ranges)

		result.RewriteRulesMap[thisTable.Table.ID] = thisTable.RewriteRule
		result.TablesToSend = append(result.TablesToSend, thisTable.CreatedTable)

		// the batch is full, we should stop here!
		// we use strictly greater than because when we send a batch at equal, the offset should plus one.
		// (because the last table is sent, we should put it in emptyTables), and this will introduce extra complex.
		if thisTableLen+collected > b.batchSizeThreshold {
			drainSize := b.batchSizeThreshold - collected
			thisTableRanges := thisTable.Range

			var drained []rtree.Range
			drained, b.cachedTables[offset].Range = thisTableRanges[:drainSize], thisTableRanges[drainSize:]
			log.Debug("draining partial table to batch",
				zap.Stringer("db", thisTable.OldTable.DB.Name),
				zap.Stringer("table", thisTable.Table.Name),
				zap.Int("size", thisTableLen),
				zap.Int("drained", drainSize),
			)
			// Firstly calculated the batcher size, and then
			// filter out ranges by checkpoint.
			atomic.AddInt32(&b.size, -int32(len(drained)))
			if exists {
				drained = b.filterOutRanges(t, drained)
			}
			result.Ranges = append(result.Ranges, drained...)
			result.TableEndOffsetInRanges = append(result.TableEndOffsetInRanges, len(result.Ranges))
			b.cachedTables = b.cachedTables[offset:]
			return result
		}

		result.BlankTablesAfterSend = append(result.BlankTablesAfterSend, thisTable.CreatedTable)
		// Firstly calculated the batcher size, and then filter out ranges by checkpoint.
		atomic.AddInt32(&b.size, -int32(len(thisTable.Range)))
		// let's 'drain' the ranges of current table. This op must not make the batch full.
		if exists {
			result.Ranges = append(result.Ranges, b.filterOutRanges(t, thisTable.Range)...)
		} else {
			result.Ranges = append(result.Ranges, thisTable.Range...)
		}
		result.TableEndOffsetInRanges = append(result.TableEndOffsetInRanges, len(result.Ranges))
		// clear the table length.
		b.cachedTables[offset].Range = []rtree.Range{}
		log.Debug("draining table to batch",
			zap.Stringer("db", thisTable.OldTable.DB.Name),
			zap.Stringer("table", thisTable.Table.Name),
			zap.Int("size", thisTableLen),
		)
	}

	// all tables are drained.
	b.cachedTables = []TableWithRange{}
	return result
}

// Send sends all pending requests in the batcher.
// returns tables sent FULLY in the current batch.
func (b *Batcher) Send(ctx context.Context) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Batcher.Send", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	drainResult := b.drainRanges()
	tbs := drainResult.TablesToSend
	ranges := drainResult.Ranges
	log.Info("restore batch start", rtree.ZapRanges(ranges), zapTables(tbs))
	// Leave is called at b.contextCleaner
	if err := b.manager.Enter(ctx, drainResult.TablesToSend); err != nil {
		b.sendErr <- err
		return
	}
	b.sender.RestoreBatch(drainResult)
}

func (b *Batcher) sendIfFull() {
	if b.Len() >= b.batchSizeThreshold {
		log.Debug("sending batch because batcher is full", zap.Int("size", b.Len()))
		b.asyncSend(SendUntilLessThanBatch)
	}
}

// Add adds a task to the Batcher.
func (b *Batcher) Add(tbs TableWithRange) {
	b.cachedTablesMu.Lock()
	log.Debug("adding table to batch",
		zap.Stringer("db", tbs.OldTable.DB.Name),
		zap.Stringer("table", tbs.Table.Name),
		zap.Int64("old id", tbs.OldTable.Info.ID),
		zap.Int64("new id", tbs.Table.ID),
		zap.Int("table size", len(tbs.Range)),
		zap.Int("batch size", b.Len()),
	)
	b.cachedTables = append(b.cachedTables, tbs)
	atomic.AddInt32(&b.size, int32(len(tbs.Range)))
	b.cachedTablesMu.Unlock()

	b.sendIfFull()
}

// Close closes the batcher, sending all pending requests, close updateCh.
func (b *Batcher) Close() {
	log.Info("sending batch lastly on close", zap.Int("size", b.Len()))
	b.DisableAutoCommit()
	b.waitUntilSendDone()
	close(b.outCh)
	close(b.sendCh)
}

// SetThreshold sets the threshold that how big the batch size reaching need to send batch.
// note this function isn't goroutine safe yet,
// just set threshold before anything starts(e.g. EnableAutoCommit), please.
func (b *Batcher) SetThreshold(newThreshold int) {
	b.batchSizeThreshold = newThreshold
}

func (b *Batcher) SetCheckpoint(sets map[int64]map[string]struct{}) {
	b.checkpointSetWithTableID = sets
}
