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
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	tikvmetrics "github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var (
	errQueryInterrupted = dbterror.ClassExecutor.NewStd(errno.ErrQueryInterrupted)
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

func (h *chunkRowHeap) Push(x interface{}) {
	h.rowPtrs = append(h.rowPtrs, x.(chunk.RowPtr))
}

func (h *chunkRowHeap) Pop() interface{} {
	ret := h.rowPtrs[len(h.rowPtrs)-1]
	h.rowPtrs = h.rowPtrs[0 : len(h.rowPtrs)-1]
	return ret
}

// NewSortedSelectResults is only for partition table
// If schema == nil, sort by first few columns.
func NewSortedSelectResults(selectResult []SelectResult, schema *expression.Schema, byitems []*util.ByItems, memTracker *memory.Tracker) SelectResult {
	s := &sortedSelectResults{
		schema:       schema,
		selectResult: selectResult,
		byItems:      byitems,
		memTracker:   memTracker,
	}
	s.initCompareFuncs()
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

func (ssr *sortedSelectResults) initCompareFuncs() {
	ssr.compareFuncs = make([]chunk.CompareFunc, len(ssr.byItems))
	for i, item := range ssr.byItems {
		keyType := item.Expr.GetType()
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

	rowLen     int
	fieldTypes []*types.FieldType
	ctx        sessionctx.Context

	selectResp       *tipb.SelectResponse
	selectRespSize   int64 // record the selectResp.Size() when it is initialized.
	respChkIdx       int
	respChunkDecoder *chunk.Decoder

	feedback     *statistics.QueryFeedback
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
}

func (r *selectResult) fetchResp(ctx context.Context) error {
	defer func() {
		if r.stats != nil {
			// Ignore internal sql.
			if !r.ctx.GetSessionVars().InRestrictedSQL && len(r.stats.copRespTime) > 0 {
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
		sessVars := r.ctx.GetSessionVars()
		if atomic.LoadUint32(&sessVars.Killed) == 1 {
			return errors.Trace(errQueryInterrupted)
		}
		sc := sessVars.StmtCtx
		for _, warning := range r.selectResp.Warnings {
			sc.AppendWarning(dbterror.ClassTiKV.Synthesize(terror.ErrCode(warning.Code), warning.Msg))
		}
		if r.feedback != nil {
			r.feedback.Update(resultSubset.GetStartKey(), r.selectResp.OutputCounts, r.selectResp.Ndvs)
		}
		r.partialCount++

		hasStats, ok := resultSubset.(CopRuntimeStats)
		if ok {
			copStats := hasStats.GetCopRuntimeStats()
			if copStats != nil {
				r.updateCopRuntimeStats(ctx, copStats, resultSubset.RespTime())
				copStats.CopTime = duration
				sc.MergeExecDetails(&copStats.ExecDetails, nil)
			}
		}
		if len(r.selectResp.Chunks) != 0 {
			break
		}
	}
	return nil
}

func (r *selectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if r.selectResp == nil || r.respChkIdx == len(r.selectResp.Chunks) {
		err := r.fetchResp(ctx)
		if err != nil {
			return err
		}
		if r.selectResp == nil {
			return nil
		}
	}
	// TODO(Shenghui Wu): add metrics
	encodeType := r.selectResp.GetEncodeType()
	switch encodeType {
	case tipb.EncodeType_TypeDefault:
		return r.readFromDefault(ctx, chk)
	case tipb.EncodeType_TypeChunk:
		return r.readFromChunk(ctx, chk)
	}
	return errors.Errorf("unsupported encode type:%v", encodeType)
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) (data []byte, err error) {
	failpoint.Inject("mockNextRawError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mockNextRawError"))
		}
	})

	resultSubset, err := r.resp.Next(ctx)
	r.partialCount++
	r.feedback.Invalidate()
	if resultSubset != nil && err == nil {
		data = resultSubset.GetData()
	}
	return data, err
}

func (r *selectResult) readFromDefault(ctx context.Context, chk *chunk.Chunk) error {
	for !chk.IsFull() {
		if r.respChkIdx == len(r.selectResp.Chunks) {
			err := r.fetchResp(ctx)
			if err != nil || r.selectResp == nil {
				return err
			}
		}
		err := r.readRowsData(chk)
		if err != nil {
			return err
		}
		if len(r.selectResp.Chunks[r.respChkIdx].RowsData) == 0 {
			r.respChkIdx++
		}
	}
	return nil
}

func (r *selectResult) readFromChunk(ctx context.Context, chk *chunk.Chunk) error {
	if r.respChunkDecoder == nil {
		r.respChunkDecoder = chunk.NewDecoder(
			chunk.NewChunkWithCapacity(r.fieldTypes, 0),
			r.fieldTypes,
		)
	}

	for !chk.IsFull() {
		if r.respChkIdx == len(r.selectResp.Chunks) {
			err := r.fetchResp(ctx)
			if err != nil || r.selectResp == nil {
				return err
			}
		}

		if r.respChunkDecoder.IsFinished() {
			r.respChunkDecoder.Reset(r.selectResp.Chunks[r.respChkIdx].RowsData)
		}
		// If the next chunk size is greater than required rows * 0.8, reuse the memory of the next chunk and return
		// immediately. Otherwise, splice the data to one chunk and wait the next chunk.
		if r.respChunkDecoder.RemainedRows() > int(float64(chk.RequiredRows())*0.8) {
			if chk.NumRows() > 0 {
				return nil
			}
			r.respChunkDecoder.ReuseIntermChk(chk)
			r.respChkIdx++
			return nil
		}
		r.respChunkDecoder.Decode(chk)
		if r.respChunkDecoder.IsFinished() {
			r.respChkIdx++
		}
	}
	return nil
}

func (r *selectResult) updateCopRuntimeStats(ctx context.Context, copStats *copr.CopRuntimeStats, respTime time.Duration) {
	callee := copStats.CalleeAddress
	if r.rootPlanID <= 0 || r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl == nil || callee == "" {
		return
	}

	if copStats.ScanDetail != nil {
		readKeys := copStats.ScanDetail.ProcessedKeys
		readTime := copStats.TimeDetail.KvReadWallTime.Seconds()
		readSize := float64(copStats.ScanDetail.ProcessedKeysSize)
		tikvmetrics.ObserveReadSLI(uint64(readKeys), readTime, readSize)
	}

	if r.stats == nil {
		r.stats = &selectResultRuntimeStats{
			backoffSleep:       make(map[string]time.Duration),
			rpcStat:            tikv.NewRegionRequestRuntimeStats(),
			distSQLConcurrency: r.distSQLConcurrency,
		}
		if ci, ok := r.resp.(copr.CopInfo); ok {
			conc, extraConc := ci.GetConcurrency()
			r.stats.distSQLConcurrency = conc
			r.stats.extraConcurrency = extraConc
		}
	}
	r.stats.mergeCopRuntimeStats(copStats, respTime)

	if copStats.ScanDetail != nil && len(r.copPlanIDs) > 0 {
		r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RecordScanDetail(r.copPlanIDs[len(r.copPlanIDs)-1], r.storeType.Name(), copStats.ScanDetail)
	}

	// If hasExecutor is true, it means the summary is returned from TiFlash.
	hasExecutor := false
	for _, detail := range r.selectResp.GetExecutionSummaries() {
		if detail != nil && detail.TimeProcessedNs != nil &&
			detail.NumProducedRows != nil && detail.NumIterations != nil {
			if detail.ExecutorId != nil {
				hasExecutor = true
			}
			break
		}
	}
	if hasExecutor {
		var recorededPlanIDs = make(map[int]int)
		for i, detail := range r.selectResp.GetExecutionSummaries() {
			if detail != nil && detail.TimeProcessedNs != nil &&
				detail.NumProducedRows != nil && detail.NumIterations != nil {
				planID := r.copPlanIDs[i]
				recorededPlanIDs[r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.
					RecordOneCopTask(planID, r.storeType.Name(), callee, detail)] = 0
			}
		}
		num := uint64(0)
		dummySummary := &tipb.ExecutorExecutionSummary{TimeProcessedNs: &num, NumProducedRows: &num, NumIterations: &num, ExecutorId: nil}
		for _, planID := range r.copPlanIDs {
			if _, ok := recorededPlanIDs[planID]; !ok {
				r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RecordOneCopTask(planID, r.storeType.Name(), callee, dummySummary)
			}
		}
	} else {
		// For cop task cases, we still need this protection.
		if len(r.selectResp.GetExecutionSummaries()) != len(r.copPlanIDs) {
			// for TiFlash streaming call(BatchCop and MPP), it is by design that only the last response will
			// carry the execution summaries, so it is ok if some responses have no execution summaries, should
			// not trigger an error log in this case.
			if !(r.storeType == kv.TiFlash && len(r.selectResp.GetExecutionSummaries()) == 0) {
				logutil.Logger(ctx).Error("invalid cop task execution summaries length",
					zap.Int("expected", len(r.copPlanIDs)),
					zap.Int("received", len(r.selectResp.GetExecutionSummaries())))
			}
			return
		}
		for i, detail := range r.selectResp.GetExecutionSummaries() {
			if detail != nil && detail.TimeProcessedNs != nil &&
				detail.NumProducedRows != nil && detail.NumIterations != nil {
				planID := r.copPlanIDs[i]
				r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.
					RecordOneCopTask(planID, r.storeType.Name(), callee, detail)
			}
		}
	}
}

func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData
	decoder := codec.NewDecoder(chk, r.ctx.GetSessionVars().Location())
	for !chk.IsFull() && len(rowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			rowsData, err = decoder.DecodeOne(rowsData, i, r.fieldTypes[i])
			if err != nil {
				return err
			}
		}
	}
	r.selectResp.Chunks[r.respChkIdx].RowsData = rowsData
	return nil
}

func (r *selectResult) memConsume(bytes int64) {
	if r.memTracker != nil {
		r.memTracker.Consume(bytes)
	}
}

// Close closes selectResult.
func (r *selectResult) Close() error {
	if r.feedback.Actual() >= 0 {
		metrics.DistSQLScanKeysHistogram.Observe(float64(r.feedback.Actual()))
	}
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	respSize := atomic.SwapInt64(&r.selectRespSize, 0)
	if respSize > 0 {
		r.memConsume(-respSize)
	}
	if r.stats != nil {
		defer func() {
			if ci, ok := r.resp.(copr.CopInfo); ok {
				r.stats.buildTaskDuration = ci.GetBuildTaskElapsed()
				batched, fallback := ci.GetStoreBatchInfo()
				if batched != 0 || fallback != 0 {
					r.stats.storeBatchedNum, r.stats.storeBatchedFallbackNum = batched, fallback
					telemetryStoreBatchedCnt.Add(float64(r.stats.storeBatchedNum))
					telemetryStoreBatchedFallbackCnt.Add(float64(r.stats.storeBatchedFallbackNum))
					telemetryBatchedQueryTaskCnt.Add(float64(len(r.stats.copRespTime)))
				}
			}
			r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(r.rootPlanID, r.stats)
		}()
	}
	return r.resp.Close()
}

// CopRuntimeStats is an interface uses to check whether the result has cop runtime stats.
type CopRuntimeStats interface {
	// GetCopRuntimeStats gets the cop runtime stats information.
	GetCopRuntimeStats() *copr.CopRuntimeStats
}

type selectResultRuntimeStats struct {
	copRespTime             []time.Duration
	procKeys                []int64
	backoffSleep            map[string]time.Duration
	totalProcessTime        time.Duration
	totalWaitTime           time.Duration
	rpcStat                 tikv.RegionRequestRuntimeStats
	distSQLConcurrency      int
	extraConcurrency        int
	CoprCacheHitNum         int64
	storeBatchedNum         uint64
	storeBatchedFallbackNum uint64
	buildTaskDuration       time.Duration
}

func (s *selectResultRuntimeStats) mergeCopRuntimeStats(copStats *copr.CopRuntimeStats, respTime time.Duration) {
	s.copRespTime = append(s.copRespTime, respTime)
	if copStats.ScanDetail != nil {
		s.procKeys = append(s.procKeys, copStats.ScanDetail.ProcessedKeys)
	} else {
		s.procKeys = append(s.procKeys, 0)
	}
	maps.Copy(s.backoffSleep, copStats.BackoffSleep)
	s.totalProcessTime += copStats.TimeDetail.ProcessTime
	s.totalWaitTime += copStats.TimeDetail.WaitTime
	s.rpcStat.Merge(copStats.RegionRequestRuntimeStats)
	if copStats.CoprCacheHit {
		s.CoprCacheHitNum++
	}
}

func (s *selectResultRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := selectResultRuntimeStats{
		copRespTime:             make([]time.Duration, 0, len(s.copRespTime)),
		procKeys:                make([]int64, 0, len(s.procKeys)),
		backoffSleep:            make(map[string]time.Duration, len(s.backoffSleep)),
		rpcStat:                 tikv.NewRegionRequestRuntimeStats(),
		distSQLConcurrency:      s.distSQLConcurrency,
		extraConcurrency:        s.extraConcurrency,
		CoprCacheHitNum:         s.CoprCacheHitNum,
		storeBatchedNum:         s.storeBatchedNum,
		storeBatchedFallbackNum: s.storeBatchedFallbackNum,
		buildTaskDuration:       s.buildTaskDuration,
	}
	newRs.copRespTime = append(newRs.copRespTime, s.copRespTime...)
	newRs.procKeys = append(newRs.procKeys, s.procKeys...)
	for k, v := range s.backoffSleep {
		newRs.backoffSleep[k] += v
	}
	newRs.totalProcessTime += s.totalProcessTime
	newRs.totalWaitTime += s.totalWaitTime
	maps.Copy(newRs.rpcStat.Stats, s.rpcStat.Stats)
	return &newRs
}

func (s *selectResultRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	other, ok := rs.(*selectResultRuntimeStats)
	if !ok {
		return
	}
	s.copRespTime = append(s.copRespTime, other.copRespTime...)
	s.procKeys = append(s.procKeys, other.procKeys...)

	for k, v := range other.backoffSleep {
		s.backoffSleep[k] += v
	}
	s.totalProcessTime += other.totalProcessTime
	s.totalWaitTime += other.totalWaitTime
	s.rpcStat.Merge(other.rpcStat)
	s.CoprCacheHitNum += other.CoprCacheHitNum
	if other.distSQLConcurrency > s.distSQLConcurrency {
		s.distSQLConcurrency = other.distSQLConcurrency
	}
	if other.extraConcurrency > s.extraConcurrency {
		s.extraConcurrency = other.extraConcurrency
	}
	s.storeBatchedNum += other.storeBatchedNum
	s.storeBatchedFallbackNum += other.storeBatchedFallbackNum
	s.buildTaskDuration += other.buildTaskDuration
}

func (s *selectResultRuntimeStats) String() string {
	buf := bytes.NewBuffer(nil)
	rpcStat := s.rpcStat
	if len(s.copRespTime) > 0 {
		size := len(s.copRespTime)
		if size == 1 {
			buf.WriteString(fmt.Sprintf("cop_task: {num: 1, max: %v, proc_keys: %v", execdetails.FormatDuration(s.copRespTime[0]), s.procKeys[0]))
		} else {
			slices.Sort(s.copRespTime)
			vMax, vMin := s.copRespTime[size-1], s.copRespTime[0]
			vP95 := s.copRespTime[size*19/20]
			sum := 0.0
			for _, t := range s.copRespTime {
				sum += float64(t)
			}
			vAvg := time.Duration(sum / float64(size))

			slices.Sort(s.procKeys)
			keyMax := s.procKeys[size-1]
			keyP95 := s.procKeys[size*19/20]
			buf.WriteString(fmt.Sprintf("cop_task: {num: %v, max: %v, min: %v, avg: %v, p95: %v", size,
				execdetails.FormatDuration(vMax), execdetails.FormatDuration(vMin),
				execdetails.FormatDuration(vAvg), execdetails.FormatDuration(vP95)))
			if keyMax > 0 {
				buf.WriteString(", max_proc_keys: ")
				buf.WriteString(strconv.FormatInt(keyMax, 10))
				buf.WriteString(", p95_proc_keys: ")
				buf.WriteString(strconv.FormatInt(keyP95, 10))
			}
		}
		if s.totalProcessTime > 0 {
			buf.WriteString(", tot_proc: ")
			buf.WriteString(execdetails.FormatDuration(s.totalProcessTime))
			if s.totalWaitTime > 0 {
				buf.WriteString(", tot_wait: ")
				buf.WriteString(execdetails.FormatDuration(s.totalWaitTime))
			}
		}
		copRPC := rpcStat.Stats[tikvrpc.CmdCop]
		if copRPC != nil && copRPC.Count > 0 {
			rpcStat = rpcStat.Clone()
			delete(rpcStat.Stats, tikvrpc.CmdCop)
			buf.WriteString(", rpc_num: ")
			buf.WriteString(strconv.FormatInt(copRPC.Count, 10))
			buf.WriteString(", rpc_time: ")
			buf.WriteString(execdetails.FormatDuration(time.Duration(copRPC.Consume)))
		}
		if config.GetGlobalConfig().TiKVClient.CoprCache.CapacityMB > 0 {
			buf.WriteString(fmt.Sprintf(", copr_cache_hit_ratio: %v",
				strconv.FormatFloat(s.calcCacheHit(), 'f', 2, 64)))
		} else {
			buf.WriteString(", copr_cache: disabled")
		}
		if s.buildTaskDuration > 0 {
			buf.WriteString(", build_task_duration: ")
			buf.WriteString(execdetails.FormatDuration(s.buildTaskDuration))
		}
		if s.distSQLConcurrency > 0 {
			buf.WriteString(", max_distsql_concurrency: ")
			buf.WriteString(strconv.FormatInt(int64(s.distSQLConcurrency), 10))
		}
		if s.extraConcurrency > 0 {
			buf.WriteString(", max_extra_concurrency: ")
			buf.WriteString(strconv.FormatInt(int64(s.extraConcurrency), 10))
		}
		if s.storeBatchedNum > 0 {
			buf.WriteString(", store_batch_num: ")
			buf.WriteString(strconv.FormatInt(int64(s.storeBatchedNum), 10))
		}
		if s.storeBatchedFallbackNum > 0 {
			buf.WriteString(", store_batch_fallback_num: ")
			buf.WriteString(strconv.FormatInt(int64(s.storeBatchedFallbackNum), 10))
		}
		buf.WriteString("}")
	}

	rpcStatsStr := rpcStat.String()
	if len(rpcStatsStr) > 0 {
		buf.WriteString(", ")
		buf.WriteString(rpcStatsStr)
	}

	if len(s.backoffSleep) > 0 {
		buf.WriteString(", backoff{")
		idx := 0
		for k, d := range s.backoffSleep {
			if idx > 0 {
				buf.WriteString(", ")
			}
			idx++
			buf.WriteString(fmt.Sprintf("%s: %s", k, execdetails.FormatDuration(d)))
		}
		buf.WriteString("}")
	}
	return buf.String()
}

// Tp implements the RuntimeStats interface.
func (*selectResultRuntimeStats) Tp() int {
	return execdetails.TpSelectResultRuntimeStats
}

func (s *selectResultRuntimeStats) calcCacheHit() float64 {
	hit := s.CoprCacheHitNum
	tot := len(s.copRespTime)
	if s.storeBatchedNum > 0 {
		tot += int(s.storeBatchedNum)
	}
	if tot == 0 {
		return 0
	}
	return float64(hit) / float64(tot)
}
