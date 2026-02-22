// Copyright 2018 PingCAP, Inc.
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

package distsql

import (
	"container/heap"
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	dcontext "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	telemetryBatchedQueryTaskCnt     = metrics.TelemetryBatchedQueryTaskCnt
	telemetryStoreBatchedCnt         = metrics.TelemetryStoreBatchedCnt
	telemetryStoreBatchedFallbackCnt = metrics.TelemetryStoreBatchedFallbackCnt
)

var (
	_ SelectResult = (*selectResult)(nil)
	_ SelectResult = (*serialSelectResults)(nil)
	_ SelectResult = (*sortedSelectResults)(nil)
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// NextRaw gets the next raw result.
	NextRaw(context.Context) ([]byte, error)
	// Next reads the data into chunk.
	Next(context.Context, *chunk.Chunk) error
	// IntoIter converts the SelectResult into an iterator.
	IntoIter([][]*types.FieldType) (SelectResultIter, error)
	// Close closes the iterator.
	Close() error
}

// SelectResultRow indicates the row returned by the SelectResultIter
type SelectResultRow struct {
	// `ChannelIndex` indicates the index where this row locates.
	// When ChannelIndex < len(IntermediateChannels), it means this row is from intermediate result.
	// Otherwise, if ChannelIndex == len(IntermediateChannels), it means this row is from final result.
	ChannelIndex int
	// Row is the actual data of this row.
	chunk.Row
}

// SelectResultIter is an iterator that is used to iterate the rows from SelectResult.
type SelectResultIter interface {
	// Next returns the next row.
	// If the iterator is drained, the `SelectResultRow.IsEmpty()` returns true.
	Next(ctx context.Context) (SelectResultRow, error)
	// Close closes the iterator.
	Close() error
}

type chunkRowHeap struct {
	*sortedSelectResults
}

func (h chunkRowHeap) Len() int {
	return len(h.rowPtrs)
}

func (h chunkRowHeap) Less(i, j int) bool {
	iPtr := h.rowPtrs[i]
	jPtr := h.rowPtrs[j]
	return h.lessRow(h.cachedChunks[iPtr.ChkIdx].GetRow(int(iPtr.RowIdx)),
		h.cachedChunks[jPtr.ChkIdx].GetRow(int(jPtr.RowIdx)))
}

func (h chunkRowHeap) Swap(i, j int) {
	h.rowPtrs[i], h.rowPtrs[j] = h.rowPtrs[j], h.rowPtrs[i]
}

func (h *chunkRowHeap) Push(x any) {
	h.rowPtrs = append(h.rowPtrs, x.(chunk.RowPtr))
}

func (h *chunkRowHeap) Pop() any {
	ret := h.rowPtrs[len(h.rowPtrs)-1]
	h.rowPtrs = h.rowPtrs[0 : len(h.rowPtrs)-1]
	return ret
}

// NewSortedSelectResults is only for partition table
// If schema == nil, sort by first few columns.
func NewSortedSelectResults(ectx expression.EvalContext, selectResult []SelectResult, schema *expression.Schema, byitems []*util.ByItems, memTracker *memory.Tracker) SelectResult {
	s := &sortedSelectResults{
		schema:       schema,
		selectResult: selectResult,
		byItems:      byitems,
		memTracker:   memTracker,
	}
	s.initCompareFuncs(ectx)
	s.buildKeyColumns()
	s.heap = &chunkRowHeap{s}
	s.cachedChunks = make([]*chunk.Chunk, len(selectResult))
	return s
}

type sortedSelectResults struct {
	schema       *expression.Schema
	selectResult []SelectResult
	compareFuncs []chunk.CompareFunc
	byItems      []*util.ByItems
	keyColumns   []int

	cachedChunks []*chunk.Chunk
	rowPtrs      []chunk.RowPtr
	heap         *chunkRowHeap

	memTracker *memory.Tracker
}

func (ssr *sortedSelectResults) updateCachedChunk(ctx context.Context, idx uint32) error {
	prevMemUsage := ssr.cachedChunks[idx].MemoryUsage()
	if err := ssr.selectResult[idx].Next(ctx, ssr.cachedChunks[idx]); err != nil {
		return err
	}
	ssr.memTracker.Consume(ssr.cachedChunks[idx].MemoryUsage() - prevMemUsage)
	if ssr.cachedChunks[idx].NumRows() == 0 {
		return nil
	}
	heap.Push(ssr.heap, chunk.RowPtr{ChkIdx: idx, RowIdx: 0})
	return nil
}

func (ssr *sortedSelectResults) initCompareFuncs(ectx expression.EvalContext) {
	ssr.compareFuncs = make([]chunk.CompareFunc, len(ssr.byItems))
	for i, item := range ssr.byItems {
		keyType := item.Expr.GetType(ectx)
		ssr.compareFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (ssr *sortedSelectResults) buildKeyColumns() {
	ssr.keyColumns = make([]int, 0, len(ssr.byItems))
	for i, by := range ssr.byItems {
		col := by.Expr.(*expression.Column)
		if ssr.schema == nil {
			ssr.keyColumns = append(ssr.keyColumns, i)
		} else {
			ssr.keyColumns = append(ssr.keyColumns, ssr.schema.ColumnIndex(col))
		}
	}
}

func (ssr *sortedSelectResults) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range ssr.keyColumns {
		cmpFunc := ssr.compareFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if ssr.byItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

func (*sortedSelectResults) NextRaw(context.Context) ([]byte, error) {
	panic("Not support NextRaw for sortedSelectResults")
}

func (ssr *sortedSelectResults) Next(ctx context.Context, c *chunk.Chunk) (err error) {
	c.Reset()
	for i := range ssr.cachedChunks {
		if ssr.cachedChunks[i] == nil {
			ssr.cachedChunks[i] = c.CopyConstruct()
			ssr.memTracker.Consume(ssr.cachedChunks[i].MemoryUsage())
		}
	}

	if ssr.heap.Len() == 0 {
		for i := range ssr.cachedChunks {
			if err = ssr.updateCachedChunk(ctx, uint32(i)); err != nil {
				return err
			}
		}
	}

	for c.NumRows() < c.RequiredRows() {
		if ssr.heap.Len() == 0 {
			break
		}

		idx := heap.Pop(ssr.heap).(chunk.RowPtr)
		c.AppendRow(ssr.cachedChunks[idx.ChkIdx].GetRow(int(idx.RowIdx)))
		if int(idx.RowIdx) >= ssr.cachedChunks[idx.ChkIdx].NumRows()-1 {
			if err = ssr.updateCachedChunk(ctx, idx.ChkIdx); err != nil {
				return err
			}
		} else {
			heap.Push(ssr.heap, chunk.RowPtr{ChkIdx: idx.ChkIdx, RowIdx: idx.RowIdx + 1})
		}
	}
	return nil
}

func (*sortedSelectResults) IntoIter(_ [][]*types.FieldType) (SelectResultIter, error) {
	return nil, errors.New("not implemented")
}

func (ssr *sortedSelectResults) Close() (err error) {
	for i, sr := range ssr.selectResult {
		err = sr.Close()
		if err != nil {
			return err
		}
		ssr.memTracker.Consume(-ssr.cachedChunks[i].MemoryUsage())
		ssr.cachedChunks[i] = nil
	}
	return nil
}

// NewSerialSelectResults create a SelectResult which will read each SelectResult serially.
func NewSerialSelectResults(selectResults []SelectResult) SelectResult {
	return &serialSelectResults{
		selectResults: selectResults,
		cur:           0,
	}
}

// serialSelectResults reads each SelectResult serially
type serialSelectResults struct {
	selectResults []SelectResult
	cur           int
}

func (ssr *serialSelectResults) NextRaw(ctx context.Context) ([]byte, error) {
	for ssr.cur < len(ssr.selectResults) {
		resultSubset, err := ssr.selectResults[ssr.cur].NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		if len(resultSubset) > 0 {
			return resultSubset, nil
		}
		ssr.cur++ // move to the next SelectResult
	}
	return nil, nil
}

func (ssr *serialSelectResults) Next(ctx context.Context, chk *chunk.Chunk) error {
	for ssr.cur < len(ssr.selectResults) {
		if err := ssr.selectResults[ssr.cur].Next(ctx, chk); err != nil {
			return err
		}
		if chk.NumRows() > 0 {
			return nil
		}
		ssr.cur++ // move to the next SelectResult
	}
	return nil
}

func (*serialSelectResults) IntoIter(_ [][]*types.FieldType) (SelectResultIter, error) {
	return nil, errors.New("not implemented")
}

func (ssr *serialSelectResults) Close() (err error) {
	for _, r := range ssr.selectResults {
		if rerr := r.Close(); rerr != nil {
			err = rerr
		}
	}
	return
}

type selectResult struct {
	label string
	resp  kv.Response

	rowLen                  int
	fieldTypes              []*types.FieldType
	intermediateOutputTypes [][]*types.FieldType
	ctx                     *dcontext.DistSQLContext

	selectResp       *tipb.SelectResponse
	selectRespSize   int64 // record the selectResp.Size() when it is initialized.
	respChkIdx       int
	respChunkDecoder *chunk.Decoder

	partialCount int64 // number of partial results.
	sqlType      string

	// copPlanIDs contains all copTasks' planIDs,
	// which help to collect copTasks' runtime stats.
	copPlanIDs []int
	rootPlanID int

	storeType kv.StoreType

	fetchDuration    time.Duration
	durationReported bool
	memTracker       *memory.Tracker

	stats *selectResultRuntimeStats
	// distSQLConcurrency and paging are only for collecting information, and they don't affect the process of execution.
	distSQLConcurrency int
	paging             bool

	iter *selectResultIter
}

func (r *selectResult) fetchResp(ctx context.Context) error {
	return r.fetchRespWithIntermediateResults(ctx, nil)
}

func (r *selectResult) fetchRespWithIntermediateResults(ctx context.Context, intermediateOutputTypes [][]*types.FieldType) error {
	defer func() {
		if r.stats != nil {
			// Ignore internal sql.
			if !r.ctx.InRestrictedSQL && r.stats.copRespTime.Size() > 0 {
				ratio := r.stats.calcCacheHit()
				if ratio >= 1 {
					telemetry.CurrentCoprCacheHitRatioGTE100Count.Inc()
				}
				if ratio >= 0.8 {
					telemetry.CurrentCoprCacheHitRatioGTE80Count.Inc()
				}
				if ratio >= 0.4 {
					telemetry.CurrentCoprCacheHitRatioGTE40Count.Inc()
				}
				if ratio >= 0.2 {
					telemetry.CurrentCoprCacheHitRatioGTE20Count.Inc()
				}
				if ratio >= 0.1 {
					telemetry.CurrentCoprCacheHitRatioGTE10Count.Inc()
				}
				if ratio >= 0.01 {
					telemetry.CurrentCoprCacheHitRatioGTE1Count.Inc()
				}
				if ratio >= 0 {
					telemetry.CurrentCoprCacheHitRatioGTE0Count.Inc()
				}
			}
		}
	}()
	for {
		r.respChkIdx = 0
		startTime := time.Now()
		resultSubset, err := r.resp.Next(ctx)
		duration := time.Since(startTime)
		r.fetchDuration += duration
		if err != nil {
			return errors.Trace(err)
		}
		if r.selectResp != nil {
			r.memConsume(-atomic.LoadInt64(&r.selectRespSize))
		}
		if resultSubset == nil {
			r.selectResp = nil
			atomic.StoreInt64(&r.selectRespSize, 0)
			if !r.durationReported {
				// final round of fetch
				// TODO: Add a label to distinguish between success or failure.
				// https://github.com/pingcap/tidb/issues/11397
				if r.paging {
					metrics.DistSQLQueryHistogram.WithLabelValues(r.label, r.sqlType, "paging").Observe(r.fetchDuration.Seconds())
				} else {
					metrics.DistSQLQueryHistogram.WithLabelValues(r.label, r.sqlType, "common").Observe(r.fetchDuration.Seconds())
				}
				r.durationReported = true
			}
			return nil
		}
		r.selectResp = new(tipb.SelectResponse)
		err = r.selectResp.Unmarshal(resultSubset.GetData())
		if err != nil {
			return errors.Trace(err)
		}

		respSize := int64(r.selectResp.Size())
		atomic.StoreInt64(&r.selectRespSize, respSize)
		r.memConsume(respSize)
		if err := r.selectResp.Error; err != nil {
			return dbterror.ClassTiKV.Synthesize(terror.ErrCode(err.Code), err.Msg)
		}

		if len(r.selectResp.IntermediateOutputs) != len(intermediateOutputTypes) {
			return errors.Errorf(
				"The length of intermediate output types %d mismatches the length of got intermediate outputs %d."+
					" If a response contains intermediate outputs, you should use the SelectResultIter to read the data.",
				len(intermediateOutputTypes), len(r.selectResp.IntermediateOutputs),
			)
		}
		r.intermediateOutputTypes = intermediateOutputTypes

		if err = r.ctx.SQLKiller.HandleSignal(); err != nil {
			return err
		}
		for _, warning := range r.selectResp.Warnings {
			r.ctx.AppendWarning(dbterror.ClassTiKV.Synthesize(terror.ErrCode(warning.Code), warning.Msg))
		}

		r.partialCount++

		hasStats, ok := resultSubset.(CopRuntimeStats)
		if ok {
			copStats := hasStats.GetCopRuntimeStats()
			if copStats != nil {
				if err := r.updateCopRuntimeStats(ctx, copStats, resultSubset.RespTime(), false); err != nil {
					return err
				}
				r.ctx.ExecDetails.MergeCopExecDetails(&copStats.CopExecDetails, duration)
			}
		}
		if len(r.selectResp.Chunks) != 0 {
			break
		}

		if intermediate := r.selectResp.IntermediateOutputs; len(intermediate) > 0 {
			for _, output := range intermediate {
				if len(output.Chunks) != 0 {
					// some intermediate output contains data,
					// we should return the response even if the main output is empty.
					return nil
				}
			}
		}
	}
	return nil
}
