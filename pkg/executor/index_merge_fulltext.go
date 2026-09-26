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
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// fullTextScan describes a partial plan that reads a FULLTEXT index built in
// TiKV: the search string, and the value of each key column of the index,
// the columns before the tokenized one, that the scan is confined to.
type fullTextScan struct {
	search    string
	keyValues []types.Datum
}

// newFullTextScan takes the search and the key-column values from the
// partial plan. The planner offers the plan only when its conditions pin
// every key column to one value, which it records as a single point range.
func newFullTextScan(is *physicalop.PhysicalIndexScan) (*fullTextScan, error) {
	scan := &fullTextScan{search: is.FullText.Search}
	keyColumnCount := len(is.Index.Columns) - 1
	if keyColumnCount == 0 {
		return scan, nil
	}
	if len(is.Ranges) != 1 || len(is.Ranges[0].LowVal) != keyColumnCount {
		return nil, errors.Errorf("fulltext index %s has %d key columns but the scan pins %d ranges", is.Index.Name.O, keyColumnCount, len(is.Ranges))
	}
	scan.keyValues = is.Ranges[0].LowVal
	return scan, nil
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
	// ahead of every term, and prefix every term's range.
	var keyPrefix []byte
	if len(scan.keyValues) > 0 {
		sc := e.Ctx().GetSessionVars().StmtCtx
		keyPrefix, err = codec.EncodeKey(sc.TimeZone(), nil, scan.keyValues...)
		if err = sc.HandleError(err); err != nil {
			return errors.Trace(err)
		}
	}
	worker := &partialFullTextWorker{
		indexMerge: e,
		workID:     workID,
		query:      query,
		index:      idx,
		keyPrefix:  keyPrefix,
		batchSize:  e.MaxChunkSize(),
		maxBatch:   e.Ctx().GetSessionVars().IndexLookupSize,
	}

	go func() {
		defer trace.StartRegion(ctx, "IndexMergePartialFullTextWorker").End()
		defer e.idxWorkerWg.Done()
		util.WithRecovery(
			func() {
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
	batchSize int
	maxBatch  int
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
