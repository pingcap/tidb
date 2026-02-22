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
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	tikvmetrics "github.com/tikv/client-go/v2/metrics"
	clientutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (r *selectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	if r.iter != nil {
		return errors.New("selectResult is invalid after IntoIter()")
	}

	chk.Reset()
	if r.selectResp == nil || r.respChkIdx == len(r.selectResp.Chunks) {
		err := r.fetchResp(ctx)
		if err != nil {
			return err
		}
		if r.selectResp == nil {
			return nil
		}
		failpoint.Inject("mockConsumeSelectRespSlow", func(val failpoint.Value) {
			time.Sleep(time.Duration(val.(int) * int(time.Millisecond)))
		})
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

func (r *selectResult) IntoIter(intermediateFieldTypes [][]*types.FieldType) (SelectResultIter, error) {
	if r.iter != nil {
		return nil, errors.New("selectResult is invalid after IntoIter()")
	}
	return newSelectResultIter(r, intermediateFieldTypes), nil
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) (data []byte, err error) {
	failpoint.Inject("mockNextRawError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mockNextRawError"))
		}
	})

	if r.iter != nil {
		return nil, errors.New("selectResult is invalid after IntoIter()")
	}

	resultSubset, err := r.resp.Next(ctx)
	r.partialCount++
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

// FillDummySummariesForTiFlashTasks fills dummy execution summaries for mpp tasks which lack summaries
func FillDummySummariesForTiFlashTasks(runtimeStatsColl *execdetails.RuntimeStatsColl, storeType kv.StoreType, allPlanIDs []int, recordedPlanIDs map[int]int) {
	num := uint64(0)
	dummySummary := &tipb.ExecutorExecutionSummary{TimeProcessedNs: &num, NumProducedRows: &num, NumIterations: &num, ExecutorId: nil}
	for _, planID := range allPlanIDs {
		if _, ok := recordedPlanIDs[planID]; !ok {
			runtimeStatsColl.RecordOneCopTask(planID, storeType, dummySummary)
		}
	}
}

// recordExecutionSummariesForTiFlashTasks records mpp task execution summaries
func recordExecutionSummariesForTiFlashTasks(runtimeStatsColl *execdetails.RuntimeStatsColl, executionSummaries []*tipb.ExecutorExecutionSummary, storeType kv.StoreType, allPlanIDs []int) {
	var recordedPlanIDs = make(map[int]int)
	for _, detail := range executionSummaries {
		if detail != nil && detail.TimeProcessedNs != nil &&
			detail.NumProducedRows != nil && detail.NumIterations != nil {
			recordedPlanIDs[runtimeStatsColl.
				RecordOneCopTask(-1, storeType, detail)] = 0
		}
	}
	FillDummySummariesForTiFlashTasks(runtimeStatsColl, storeType, allPlanIDs, recordedPlanIDs)
}

func (r *selectResult) updateCopRuntimeStats(ctx context.Context, copStats *copr.CopRuntimeStats, respTime time.Duration, forUnconsumedStats bool) (err error) {
	callee := copStats.CalleeAddress
	if r.rootPlanID <= 0 || r.ctx.RuntimeStatsColl == nil || (callee == "" && (copStats.ReqStats == nil || copStats.ReqStats.GetRPCStatsCount() == 0)) {
		return
	}
	if (copStats.ScanDetail != nil && copStats.ScanDetail.ProcessedKeys > 0) || copStats.TimeDetail.KvReadWallTime > 0 {
		var readKeys int64
		var readSize float64
		if copStats.ScanDetail != nil {
			readKeys = copStats.ScanDetail.ProcessedKeys
			readSize = float64(copStats.ScanDetail.ProcessedKeysSize)
		}
		readTime := copStats.TimeDetail.KvReadWallTime.Seconds()
		tikvmetrics.ObserveReadSLI(uint64(readKeys), readTime, readSize)
	}

	if r.stats == nil {
		r.stats = &selectResultRuntimeStats{
			distSQLConcurrency: r.distSQLConcurrency,
		}
		if ci, ok := r.resp.(copr.CopInfo); ok {
			conc, extraConc := ci.GetConcurrency()
			r.stats.distSQLConcurrency = conc
			r.stats.extraConcurrency = extraConc
		}
	}
	r.stats.mergeCopRuntimeStats(copStats, respTime)

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

	if ruDetailsRaw := ctx.Value(clientutil.RUDetailsCtxKey); ruDetailsRaw != nil && r.storeType == kv.TiFlash {
		if err = execdetails.MergeTiFlashRUConsumption(r.selectResp.GetExecutionSummaries(), ruDetailsRaw.(*clientutil.RUDetails)); err != nil {
			return err
		}
	}
	if copStats.TimeDetail.ProcessTime > 0 {
		r.ctx.CPUUsage.MergeTikvCPUTime(copStats.TimeDetail.ProcessTime)
	}
	if hasExecutor {
		if len(r.copPlanIDs) > 0 {
			r.ctx.RuntimeStatsColl.RecordCopStats(r.copPlanIDs[len(r.copPlanIDs)-1], r.storeType, copStats.ScanDetail, copStats.TimeDetail, nil)
		}
		recordExecutionSummariesForTiFlashTasks(r.ctx.RuntimeStatsColl, r.selectResp.GetExecutionSummaries(), r.storeType, r.copPlanIDs)
		// report MPP cross AZ network traffic bytes to resource control manager.
		interZoneBytes := r.ctx.RuntimeStatsColl.GetStmtCopRuntimeStats().TiflashNetworkStats.GetInterZoneTrafficBytes()
		if interZoneBytes > 0 {
			consumption := &rmpb.Consumption{
				ReadCrossAzTrafficBytes: interZoneBytes,
			}
			if r.ctx.RUConsumptionReporter != nil {
				r.ctx.RUConsumptionReporter.ReportConsumption(r.ctx.ResourceGroupName, consumption)
			}
		}
	} else {
		// For cop task cases, we still need this protection.
		if len(r.selectResp.GetExecutionSummaries()) != len(r.copPlanIDs) {
			// for TiFlash streaming call(BatchCop and MPP), it is by design that only the last response will
			// carry the execution summaries, so it is ok if some responses have no execution summaries, should
			// not trigger an error log in this case.
			if !forUnconsumedStats && !(r.storeType == kv.TiFlash && len(r.selectResp.GetExecutionSummaries()) == 0) {
				logutil.Logger(ctx).Warn("invalid cop task execution summaries length",
					zap.Int("expected", len(r.copPlanIDs)),
					zap.Int("received", len(r.selectResp.GetExecutionSummaries())))
			}
			return
		}
		for i, detail := range r.selectResp.GetExecutionSummaries() {
			var summary *tipb.ExecutorExecutionSummary
			if detail != nil && detail.TimeProcessedNs != nil &&
				detail.NumProducedRows != nil && detail.NumIterations != nil {
				summary = detail
			}
			planID := r.copPlanIDs[i]
			if i == len(r.copPlanIDs)-1 {
				r.ctx.RuntimeStatsColl.RecordCopStats(planID, r.storeType, copStats.ScanDetail, copStats.TimeDetail, summary)
			} else if summary != nil {
				r.ctx.RuntimeStatsColl.RecordOneCopTask(planID, r.storeType, summary)
			}
		}
	}
	return
}

func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData
	decoder := codec.NewDecoder(chk, r.ctx.Location)
	for !chk.IsFull() && len(rowsData) > 0 {
		for i := range r.rowLen {
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
	if r.iter != nil {
		return errors.New("selectResult is invalid after IntoIter()")
	}
	return r.close()
}

func (r *selectResult) close() error {
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	respSize := atomic.SwapInt64(&r.selectRespSize, 0)
	if respSize > 0 {
		r.memConsume(-respSize)
	}
	if r.ctx != nil {
		if unconsumed, ok := r.resp.(copr.HasUnconsumedCopRuntimeStats); ok && unconsumed != nil {
			unconsumedCopStats := unconsumed.CollectUnconsumedCopRuntimeStats()
			for _, copStats := range unconsumedCopStats {
				_ = r.updateCopRuntimeStats(context.Background(), copStats, time.Duration(0), true)
				r.ctx.ExecDetails.MergeCopExecDetails(&copStats.CopExecDetails, 0)
			}
		}
	}
	if r.stats != nil && r.ctx != nil {
		defer func() {
			if ci, ok := r.resp.(copr.CopInfo); ok {
				r.stats.buildTaskDuration = ci.GetBuildTaskElapsed()
				batched, fallback := ci.GetStoreBatchInfo()
				if batched != 0 || fallback != 0 {
					r.stats.storeBatchedNum, r.stats.storeBatchedFallbackNum = batched, fallback
					telemetryStoreBatchedCnt.Add(float64(r.stats.storeBatchedNum))
					telemetryStoreBatchedFallbackCnt.Add(float64(r.stats.storeBatchedFallbackNum))
					telemetryBatchedQueryTaskCnt.Add(float64(r.stats.copRespTime.Size()))
				}
			}
			r.stats.fetchRspDuration = r.fetchDuration
			r.ctx.RuntimeStatsColl.RegisterStats(r.rootPlanID, r.stats)
		}()
	}
	return r.resp.Close()
}
