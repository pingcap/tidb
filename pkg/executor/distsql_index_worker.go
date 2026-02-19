// Copyright 2017 PingCAP, Inc.
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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type indexWorker struct {
	idxLookup *IndexLookUpExecutor
	finished  <-chan struct{}
	resultCh  chan<- *lookupTableTask
	keepOrder bool

	// batchSize is for lightweight startup. It will be increased exponentially until reaches the max batch size value.
	batchSize    int
	maxBatchSize int
	maxChunkSize int

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue
	// PushedLimit is used to skip the preceding and tailing handles when Limit is sunk into IndexLookUpReader.
	PushedLimit *physicalop.PushedDownLimit
	// scannedKeys indicates how many keys be scanned
	scannedKeys uint64
}

func (w *indexWorker) syncErr(err error) {
	doneCh := make(chan error, 1)
	doneCh <- err
	w.resultCh <- &lookupTableTask{
		doneCh: doneCh,
	}
}

type selectResultList []struct {
	Result  distsql.SelectResult
	RowIter distsql.SelectResultIter
}

func newSelectResultList(results []distsql.SelectResult) selectResultList {
	l := make(selectResultList, len(results))
	for i, r := range results {
		l[i].Result = r
	}
	return l
}

func newSelectResultRowIterList(results []distsql.SelectResult, intermediateResultTypes [][]*types.FieldType) (selectResultList, error) {
	ret := newSelectResultList(results)
	for i, r := range ret {
		rowIter, err := r.Result.IntoIter(intermediateResultTypes)
		if err != nil {
			ret.Close()
			return nil, err
		}
		ret[i].RowIter = rowIter
		ret[i].Result = nil
	}
	return ret, nil
}

func (l selectResultList) Close() {
	for _, r := range l {
		var err error
		if r.RowIter != nil {
			err = r.RowIter.Close()
		} else if r.Result != nil {
			err = r.Result.Close()
		}

		if err != nil {
			logutil.BgLogger().Error("close Select result failed", zap.Error(err))
		}
	}
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are submitted to the pool and processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorker) fetchHandles(ctx context.Context, results selectResultList, indexTps []*types.FieldType) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Warn("indexWorker in IndexLookupExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
			err4Panic := util.GetRecoverError(r)
			w.syncErr(err4Panic)
			if err != nil {
				err = errors.Trace(err4Panic)
			}
		}
	}()
	var chk *chunk.Chunk
	if !w.idxLookup.indexLookUpPushDown {
		// chk is only used by non-indexLookUpPushDown mode for mem-reuse
		chk = w.idxLookup.AllocPool.Alloc(indexTps, w.idxLookup.MaxChunkSize(), w.idxLookup.MaxChunkSize())
	}
	handleOffsets, err := w.getHandleOffsets(len(indexTps))
	if err != nil {
		w.syncErr(err)
		return err
	}
	idxID := w.idxLookup.getIndexPlanRootID()
	if w.idxLookup.stmtRuntimeStatsColl != nil {
		if idxID != w.idxLookup.ID() && w.idxLookup.stats != nil {
			w.idxLookup.stats.indexScanBasicStats = w.idxLookup.stmtRuntimeStatsColl.GetBasicRuntimeStats(idxID, true)
		}
	}
	taskID := 0
	for i := 0; i < len(results); {
		curResultIdx := i
		result := results[curResultIdx]
		if w.PushedLimit != nil && w.scannedKeys >= w.PushedLimit.Count+w.PushedLimit.Offset {
			break
		}
		startTime := time.Now()
		var completedRows []chunk.Row
		var handles []kv.Handle
		var retChunk *chunk.Chunk
		var curResultExhausted bool
		if w.idxLookup.indexLookUpPushDown {
			completedRows, handles, curResultExhausted, err = w.extractLookUpPushDownRowsOrHandles(ctx, result.RowIter, handleOffsets)
		} else {
			handles, retChunk, err = w.extractTaskHandles(ctx, chk, result.Result, handleOffsets)
			curResultExhausted = len(handles) == 0
		}
		finishFetch := time.Now()
		if err != nil {
			w.syncErr(err)
			return err
		}

		if curResultExhausted {
			i++
		}

		if len(handles) == 0 && len(completedRows) == 0 {
			continue
		}

		var completedTask *lookupTableTask
		if rowCnt := len(completedRows); rowCnt > 0 {
			metrics.IndexLookUpPushDownRowsCounterHit.Add(float64(rowCnt))
			// Currently, completedRows is only produced by index lookup push down which does not support keep order.
			// for non-keep-order request, the completed rows can be sent to resultCh directly.
			completedTask = w.buildCompletedTask(taskID, completedRows)
			taskID++
		}

		var tableLookUpTask *lookupTableTask
		if rowCnt := len(handles); rowCnt > 0 {
			if w.idxLookup.indexLookUpPushDown {
				metrics.IndexLookUpPushDownRowsCounterMiss.Add(float64(rowCnt))
			} else {
				metrics.IndexLookUpNormalRowsCounter.Add(float64(rowCnt))
			}
			tableLookUpTask = w.buildTableTask(taskID, handles, retChunk)
			if w.idxLookup.partitionTableMode {
				tableLookUpTask.partitionTable = w.idxLookup.prunedPartitions[curResultIdx]
			}
			taskID++
		}

		finishBuild := time.Now()
		select {
		case <-ctx.Done():
			return nil
		case <-w.finished:
			return nil
		default:
			if completedTask != nil {
				w.resultCh <- completedTask
			}

			if tableLookUpTask != nil {
				e := w.idxLookup
				e.tblWorkerWg.Add(1)
				e.pool.submit(func() {
					defer e.tblWorkerWg.Done()
					select {
					case <-e.finished:
						return
					default:
						growWorkerStack16K()
						execTableTask(e, tableLookUpTask)
					}
				})
				w.resultCh <- tableLookUpTask
			}
		}
		if w.idxLookup.stats != nil {
			atomic.AddInt64(&w.idxLookup.stats.FetchHandle, int64(finishFetch.Sub(startTime)))
			atomic.AddInt64(&w.idxLookup.stats.TaskWait, int64(time.Since(finishBuild)))
			atomic.AddInt64(&w.idxLookup.stats.FetchHandleTotal, int64(time.Since(startTime)))
		}
	}
	return nil
}

func (w *indexWorker) getHandleOffsets(indexTpsLen int) ([]int, error) {
	numColsWithoutPid := indexTpsLen
	ok, err := w.idxLookup.needPartitionHandle(getHandleFromIndex)
	if err != nil {
		return nil, err
	}
	if ok {
		numColsWithoutPid = numColsWithoutPid - 1
	}
	handleOffset := make([]int, 0, len(w.idxLookup.handleCols))
	for i := range w.idxLookup.handleCols {
		handleOffset = append(handleOffset, numColsWithoutPid-len(w.idxLookup.handleCols)+i)
	}
	if len(handleOffset) == 0 {
		handleOffset = []int{numColsWithoutPid - 1}
	}
	return handleOffset, nil
}

func (w *indexWorker) extractLookUpPushDownRowsOrHandles(ctx context.Context, iter distsql.SelectResultIter, handleOffset []int) (rows []chunk.Row, handles []kv.Handle, exhausted bool, err error) {
	intest.Assert(w.checkIndexValue == nil, "CheckIndex or CheckTable should not use index lookup push down")
	const channelIdxIndex = 0
	const channelIdxRow = 1

	startTime := time.Now()
	startScanKeys := w.scannedKeys
	defer func() {
		if cnt := w.scannedKeys - startScanKeys; w.idxLookup.stats != nil {
			w.idxLookup.stats.indexScanBasicStats.Record(time.Since(startTime), int(cnt))
		}
	}()

	checkLimit := w.PushedLimit != nil
	for len(handles)+len(rows) < w.batchSize {
		var row distsql.SelectResultRow
		row, err = iter.Next(ctx)
		if err != nil {
			return nil, nil, false, errors.Trace(err)
		}

		if row.IsEmpty() {
			exhausted = true
			return
		}

		w.scannedKeys++
		if checkLimit {
			if w.scannedKeys <= w.PushedLimit.Offset {
				continue
			}
			if w.scannedKeys > (w.PushedLimit.Offset + w.PushedLimit.Count) {
				// Skip the handles after Offset+Count.
				return
			}
		}

		switch row.ChannelIndex {
		case channelIdxRow:
			rows = append(rows, row.Row)
		case channelIdxIndex:
			h, err := w.idxLookup.getHandle(row.Row, handleOffset, w.idxLookup.isCommonHandle(), getHandleFromIndex)
			if err != nil {
				return nil, nil, false, errors.Trace(err)
			}
			handles = append(handles, h)
		default:
			return nil, nil, false, errors.Errorf("unexpected channel index %d", row.ChannelIndex)
		}
	}

	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return
}

func (w *indexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult, handleOffset []int) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	// PushedLimit would always be nil for CheckIndex or CheckTable, we add this check just for insurance.
	checkLimit := (w.PushedLimit != nil) && (w.checkIndexValue == nil)
	for len(handles) < w.batchSize {
		requiredRows := w.batchSize - len(handles)
		if checkLimit {
			if w.PushedLimit.Offset+w.PushedLimit.Count <= w.scannedKeys {
				return handles, nil, nil
			}
			leftCnt := w.PushedLimit.Offset + w.PushedLimit.Count - w.scannedKeys
			if uint64(requiredRows) > leftCnt {
				requiredRows = int(leftCnt)
			}
		}
		chk.SetRequiredRows(requiredRows, w.maxChunkSize)
		startTime := time.Now()
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, nil, err
		}
		if w.idxLookup.stats != nil {
			w.idxLookup.stats.indexScanBasicStats.Record(time.Since(startTime), chk.NumRows())
		}
		if chk.NumRows() == 0 {
			return handles, retChk, nil
		}
		if handles == nil {
			handles = make([]kv.Handle, 0, chk.NumRows())
		}
		for i := range chk.NumRows() {
			w.scannedKeys++
			if checkLimit {
				if w.scannedKeys <= w.PushedLimit.Offset {
					continue
				}
				if w.scannedKeys > (w.PushedLimit.Offset + w.PushedLimit.Count) {
					// Skip the handles after Offset+Count.
					return handles, nil, nil
				}
			}
			h, err := w.idxLookup.getHandle(chk.GetRow(i), handleOffset, w.idxLookup.isCommonHandle(), getHandleFromIndex)
			if err != nil {
				return handles, retChk, err
			}
			handles = append(handles, h)
		}
		if w.checkIndexValue != nil {
			if retChk == nil {
				retChk = chunk.NewChunkWithCapacity(w.idxColTps, w.batchSize)
			}
			retChk.Append(chk, 0, chk.NumRows())
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (*indexWorker) buildCompletedTask(taskID int, rows []chunk.Row) *lookupTableTask {
	task := &lookupTableTask{
		id:     taskID,
		rows:   rows,
		doneCh: make(chan error, 1),
	}
	task.doneCh <- nil
	return task
}

func (w *indexWorker) buildTableTask(taskID int, handles []kv.Handle, retChk *chunk.Chunk) *lookupTableTask {
	var indexOrder *kv.HandleMap
	var duplicatedIndexOrder *kv.HandleMap
	if w.keepOrder {
		// Save the index order.
		indexOrder = kv.NewHandleMap()
		for i, h := range handles {
			indexOrder.Set(h, i)
		}
	}

	if w.checkIndexValue != nil {
		// Save the index order.
		indexOrder = kv.NewHandleMap()
		duplicatedIndexOrder = kv.NewHandleMap()
		for i, h := range handles {
			if _, ok := indexOrder.Get(h); ok {
				duplicatedIndexOrder.Set(h, i)
			} else {
				indexOrder.Set(h, i)
			}
		}
	}

	task := &lookupTableTask{
		id:                   taskID,
		handles:              handles,
		indexOrder:           indexOrder,
		duplicatedIndexOrder: duplicatedIndexOrder,
		idxRows:              retChk,
	}

	task.doneCh = make(chan error, 1)
	return task
}
