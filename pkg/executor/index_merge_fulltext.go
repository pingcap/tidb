// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"context"
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// fullTextScan describes a partial plan that reads a FULLTEXT index built in
// TiKV: the search string, the value of each key column of the index, the
// columns before the tokenized one, that the scan is confined to, and the
// ranges of the clustered handle, which ends every entry, that the postings
// of each exact term are confined to.
type fullTextScan struct {
	search    string
	keyValues []types.Datum
	// handleRanges are over a leading prefix of the handle columns, sorted
	// and disjoint; empty when the scan reads each term's postings whole.
	handleRanges []*ranger.Range
}

// newFullTextScan takes the search, the key-column values and the handle
// ranges from the partial plan. The planner records the key columns, which
// its conditions pin to one value, as the leading dimensions of every range
// and the handle columns as the trailing ones, so a scan with no handle
// ranges has a single range as wide as the key columns.
func newFullTextScan(is *physicalop.PhysicalIndexScan) (*fullTextScan, error) {
	scan := &fullTextScan{search: is.FullText.Search}
	keyColumnCount := len(is.Index.Columns) - 1
	if keyColumnCount == 0 && len(is.Ranges) == 0 {
		return scan, nil
	}
	if len(is.Ranges) == 0 || len(is.Ranges[0].LowVal) < keyColumnCount {
		return nil, errors.Errorf("fulltext index %s has %d key columns but the scan pins %d ranges", is.Index.Name.O, keyColumnCount, len(is.Ranges))
	}
	scan.keyValues = is.Ranges[0].LowVal[:keyColumnCount]
	if len(is.Ranges[0].LowVal) == keyColumnCount {
		if len(is.Ranges) != 1 {
			return nil, errors.Errorf("fulltext index %s has %d key columns but the scan pins %d ranges", is.Index.Name.O, keyColumnCount, len(is.Ranges))
		}
		return scan, nil
	}
	for _, ran := range is.Ranges {
		if len(ran.LowVal) <= keyColumnCount || len(ran.HighVal) != len(ran.LowVal) {
			return nil, errors.Errorf("fulltext index %s scan has a handle range of width %d past %d key columns", is.Index.Name.O, len(ran.LowVal), keyColumnCount)
		}
		scan.handleRanges = append(scan.handleRanges, &ranger.Range{
			LowVal:      ran.LowVal[keyColumnCount:],
			HighVal:     ran.HighVal[keyColumnCount:],
			LowExclude:  ran.LowExclude,
			HighExclude: ran.HighExclude,
		})
	}
	return scan, nil
}

// encodeFullTextHandleRanges encodes the bounds of each handle range the way
// the handle is encoded after the term in an index key. Whether a bound is
// inclusive is applied later, against the whole key of a term, since the
// bound is a suffix of it.
func encodeFullTextHandleRanges(sc *stmtctx.StatementContext, ranges []*ranger.Range) ([]fullTextHandleRange, error) {
	encoded := make([]fullTextHandleRange, 0, len(ranges))
	for _, ran := range ranges {
		low, err := codec.EncodeKey(sc.TimeZone(), nil, ran.LowVal...)
		if err = sc.HandleError(err); err != nil {
			return nil, errors.Trace(err)
		}
		high, err := codec.EncodeKey(sc.TimeZone(), nil, ran.HighVal...)
		if err = sc.HandleError(err); err != nil {
			return nil, errors.Trace(err)
		}
		encoded = append(encoded, fullTextHandleRange{low: low, high: high, lowExclude: ran.LowExclude, highExclude: ran.HighExclude})
	}
	return encoded, nil
}

// isFullTextPartial reports whether partial plan i reads a FULLTEXT index
// built in TiKV.
func (e *IndexMergeReaderExecutor) isFullTextPartial(i int) bool {
	return i < len(e.fullTextScans) && e.fullTextScans[i] != nil
}

// fullTextUnionScanRange is the record range of a physical table, within
// which UnionScan looks for the rows the transaction changed.
func fullTextUnionScanRange(physicalID int64) *kvRangesWithPhysicalTblID {
	prefix := tablecodec.GenTableRecordPrefix(physicalID)
	return &kvRangesWithPhysicalTblID{
		PhysicalTableID: physicalID,
		KeyRanges:       []kv.KeyRange{{StartKey: prefix, EndKey: prefix.PrefixNext()}},
	}
}

// startPartialFullTextWorker runs the posting-list engine for one FULLTEXT
// partial plan and feeds the handles it yields to the table lookup, the way a
// partial index worker feeds the handles a coprocessor index scan returns.
// The search string is compiled with the analyzer frozen in the index, so the
// terms looked up are the terms the index stores.
func (e *IndexMergeReaderExecutor) startPartialFullTextWorker(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask, workID int) error {
	idx := e.indexes[workID]
	if idx == nil || idx.TiKVFullText == nil {
		return errors.Errorf("partial plan %d is not a fulltext index scan", workID)
	}
	scan := e.fullTextScans[workID]
	query, err := fulltext.CompileBooleanQuery(scan.search, fulltext.AnalyzerConfigFromTiKVFullTextIndex(idx.TiKVFullText))
	if err != nil {
		return errors.Trace(err)
	}
	// The key-column values are encoded once, the way the index encodes them
	// ahead of every term, and prefix every term's range; the handle ranges
	// likewise, the way the handle follows every term.
	sc := e.Ctx().GetSessionVars().StmtCtx
	var keyPrefix []byte
	if len(scan.keyValues) > 0 {
		keyPrefix, err = codec.EncodeKey(sc.TimeZone(), nil, scan.keyValues...)
		if err = sc.HandleError(err); err != nil {
			return errors.Trace(err)
		}
	}
	handleRanges, err := encodeFullTextHandleRanges(sc, scan.handleRanges)
	if err != nil {
		return err
	}
	worker := &partialFullTextWorker{
		indexMerge:   e,
		workID:       workID,
		query:        query,
		index:        idx,
		keyPrefix:    keyPrefix,
		handleRanges: handleRanges,
		batchSize:    e.MaxChunkSize(),
		maxBatch:     e.Ctx().GetSessionVars().IndexLookupSize,
	}
	// The scan runs in TiDB, so no coprocessor summary reports its rows and
	// time; the worker records them on the partial plan itself, with what the
	// posting reads cost.
	if e.stats != nil {
		worker.planID = e.getPartitalPlanID(workID)
		worker.scanStats = &fullTextScanRuntimeStats{}
		e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(worker.planID, true)
	}

	go func() {
		defer trace.StartRegion(ctx, "IndexMergePartialFullTextWorker").End()
		defer e.idxWorkerWg.Done()
		util.WithRecovery(
			func() {
				if worker.scanStats != nil {
					defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(worker.planID, worker.scanStats)
				}
				if err := worker.run(ctx, exitCh, fetchCh); err != nil {
					syncErr(ctx, e.finished, fetchCh, err)
				}
			},
			handleWorkerPanic(ctx, e.finished, nil, fetchCh, nil, partialFullTextWorkerType),
		)
	}()
	return nil
}

type partialFullTextWorker struct {
	indexMerge *IndexMergeReaderExecutor
	workID     int
	query      *fulltext.Query
	index      *model.IndexInfo
	// keyPrefix is the encoded key-column values the scan is confined to,
	// empty for an index without key columns.
	keyPrefix []byte
	// handleRanges are the encoded handle ranges each exact term's postings
	// are confined to, empty when they are read whole.
	handleRanges []fullTextHandleRange
	batchSize    int
	maxBatch     int
	// planID and scanStats record the scan's runtime statistics on the
	// partial plan; scanStats is nil when they are not collected.
	planID    int
	scanStats *fullTextScanRuntimeStats
}

// run scans each physical table's index in turn. Handles are batched into
// table tasks that grow from a chunk to the index lookup size, as the other
// partial workers do, so a search that matches little costs little.
func (w *partialFullTextWorker) run(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask) error {
	e := w.indexMerge
	physicalIDs := []int64{getPhysicalTableID(e.table)}
	if e.partitionTableMode {
		physicalIDs = physicalIDs[:0]
		for _, p := range e.prunedPartitions {
			physicalIDs = append(physicalIDs, p.GetPhysicalID())
		}
	}
	for parTblIdx, physicalID := range physicalIDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-exitCh:
			return nil
		case <-e.finished:
			return nil
		default:
		}
		if err := w.scanPhysicalTable(ctx, physicalID, parTblIdx, exitCh, fetchCh); err != nil {
			return err
		}
	}
	return nil
}

func (w *partialFullTextWorker) scanPhysicalTable(ctx context.Context, physicalID int64, parTblIdx int, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask) error {
	e := w.indexMerge
	source := &tikvPostingSource{
		snapshot:        e.fullTextSnapshot,
		physicalTableID: physicalID,
		index:           w.index,
		keyPrefix:       w.keyPrefix,
		handleRanges:    w.handleRanges,
		stats:           w.scanStats,
	}
	iter, err := w.query.OpenPostings(source)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		}
	}()
	batchSize := w.batchSize
	for {
		start := time.Now()
		handles := make([]kv.Handle, 0, batchSize)
		for len(handles) < batchSize {
			handle, ok, err := iter.Next()
			if err != nil {
				return errors.Trace(err)
			}
			if !ok {
				break
			}
			handles = append(handles, handle)
		}
		if w.scanStats != nil {
			e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(w.planID, false).Record(time.Since(start), len(handles))
		}
		if len(handles) == 0 {
			return nil
		}
		task := &indexMergeTableTask{
			lookupTableTask: lookupTableTask{handles: handles},
			parTblIdx:       parTblIdx,
			partialPlanID:   w.workID,
		}
		if e.prunedPartitions != nil {
			task.partitionTable = e.prunedPartitions[parTblIdx]
		}
		task.doneCh = make(chan error, 1)
		if e.stats != nil {
			atomic.AddInt64(&e.stats.FetchIdxTime, int64(time.Since(start)))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-exitCh:
			return nil
		case <-e.finished:
			return nil
		case fetchCh <- task:
		}
		if len(handles) < batchSize {
			return nil
		}
		batchSize = min(batchSize*2, w.maxBatch)
	}
}
