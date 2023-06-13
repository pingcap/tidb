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
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
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
)

var (
	errQueryInterrupted = dbterror.ClassExecutor.NewStd(errno.ErrQueryInterrupted)
)

var (
	_ SelectResult = (*selectResult)(nil)
	_ SelectResult = (*serialSelectResults)(nil)
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
<<<<<<< HEAD
			if !r.ctx.GetSessionVars().InRestrictedSQL && len(r.stats.copRespTime) > 0 {
				ratio := float64(r.stats.CoprCacheHitNum) / float64(len(r.stats.copRespTime))
=======
			if !r.ctx.GetSessionVars().InRestrictedSQL && r.stats.copRespTime.Size() > 0 {
				ratio := r.stats.calcCacheHit()
>>>>>>> 6f54a29444a (*: use approximately algorithm to calculate p90 in slowlog. (#44269))
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
		readTime := copStats.TimeDetail.KvReadWallTimeMs.Seconds()
		readSize := float64(copStats.ScanDetail.ProcessedKeysSize)
		tikvmetrics.ObserveReadSLI(uint64(readKeys), readTime, readSize)
	}

	if r.stats == nil {
		r.stats = &selectResultRuntimeStats{
			backoffSleep:       make(map[string]time.Duration),
			rpcStat:            tikv.NewRegionRequestRuntimeStats(),
			distSQLConcurrency: r.distSQLConcurrency,
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
<<<<<<< HEAD
		defer r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(r.rootPlanID, r.stats)
=======
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
			r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(r.rootPlanID, r.stats)
		}()
>>>>>>> 6f54a29444a (*: use approximately algorithm to calculate p90 in slowlog. (#44269))
	}
	return r.resp.Close()
}

// CopRuntimeStats is a interface uses to check whether the result has cop runtime stats.
type CopRuntimeStats interface {
	// GetCopRuntimeStats gets the cop runtime stats information.
	GetCopRuntimeStats() *copr.CopRuntimeStats
}

type selectResultRuntimeStats struct {
<<<<<<< HEAD
	copRespTime        []time.Duration
	procKeys           []int64
	backoffSleep       map[string]time.Duration
	totalProcessTime   time.Duration
	totalWaitTime      time.Duration
	rpcStat            tikv.RegionRequestRuntimeStats
	distSQLConcurrency int
	CoprCacheHitNum    int64
=======
	copRespTime             execdetails.Percentile[execdetails.Duration]
	procKeys                execdetails.Percentile[execdetails.Int64]
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
>>>>>>> 6f54a29444a (*: use approximately algorithm to calculate p90 in slowlog. (#44269))
}

func (s *selectResultRuntimeStats) mergeCopRuntimeStats(copStats *copr.CopRuntimeStats, respTime time.Duration) {
	s.copRespTime.Add(execdetails.Duration(respTime))
	if copStats.ScanDetail != nil {
		s.procKeys.Add(execdetails.Int64(copStats.ScanDetail.ProcessedKeys))
	} else {
		s.procKeys.Add(0)
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
<<<<<<< HEAD
		copRespTime:        make([]time.Duration, 0, len(s.copRespTime)),
		procKeys:           make([]int64, 0, len(s.procKeys)),
		backoffSleep:       make(map[string]time.Duration, len(s.backoffSleep)),
		rpcStat:            tikv.NewRegionRequestRuntimeStats(),
		distSQLConcurrency: s.distSQLConcurrency,
		CoprCacheHitNum:    s.CoprCacheHitNum,
=======
		copRespTime:             execdetails.Percentile[execdetails.Duration]{},
		procKeys:                execdetails.Percentile[execdetails.Int64]{},
		backoffSleep:            make(map[string]time.Duration, len(s.backoffSleep)),
		rpcStat:                 tikv.NewRegionRequestRuntimeStats(),
		distSQLConcurrency:      s.distSQLConcurrency,
		extraConcurrency:        s.extraConcurrency,
		CoprCacheHitNum:         s.CoprCacheHitNum,
		storeBatchedNum:         s.storeBatchedNum,
		storeBatchedFallbackNum: s.storeBatchedFallbackNum,
		buildTaskDuration:       s.buildTaskDuration,
>>>>>>> 6f54a29444a (*: use approximately algorithm to calculate p90 in slowlog. (#44269))
	}
	newRs.copRespTime.MergePercentile(&s.copRespTime)
	newRs.procKeys.MergePercentile(&s.procKeys)
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
	s.copRespTime.MergePercentile(&other.copRespTime)
	s.procKeys.MergePercentile(&other.procKeys)

	for k, v := range other.backoffSleep {
		s.backoffSleep[k] += v
	}
	s.totalProcessTime += other.totalProcessTime
	s.totalWaitTime += other.totalWaitTime
	s.rpcStat.Merge(other.rpcStat)
	s.CoprCacheHitNum += other.CoprCacheHitNum
}

func (s *selectResultRuntimeStats) String() string {
	buf := bytes.NewBuffer(nil)
	rpcStat := s.rpcStat
	if s.copRespTime.Size() > 0 {
		size := s.copRespTime.Size()
		if size == 1 {
<<<<<<< HEAD
			buf.WriteString(fmt.Sprintf("cop_task: {num: 1, max: %v, proc_keys: %v", execdetails.FormatDuration(s.copRespTime[0]), s.procKeys[0]))
=======
			fmt.Fprintf(buf, "cop_task: {num: 1, max: %v, proc_keys: %v", execdetails.FormatDuration(time.Duration(s.copRespTime.GetPercentile(0))), s.procKeys.GetPercentile(0))
>>>>>>> 6f54a29444a (*: use approximately algorithm to calculate p90 in slowlog. (#44269))
		} else {
			vMax, vMin := s.copRespTime.GetMax(), s.copRespTime.GetMin()
			vP95 := s.copRespTime.GetPercentile(0.95)
			sum := s.copRespTime.Sum()
			vAvg := time.Duration(sum / float64(size))

<<<<<<< HEAD
			slices.Sort(s.procKeys)
			keyMax := s.procKeys[size-1]
			keyP95 := s.procKeys[size*19/20]
			buf.WriteString(fmt.Sprintf("cop_task: {num: %v, max: %v, min: %v, avg: %v, p95: %v", size,
				execdetails.FormatDuration(vMax), execdetails.FormatDuration(vMin),
				execdetails.FormatDuration(vAvg), execdetails.FormatDuration(vP95)))
=======
			keyMax := s.procKeys.GetMax()
			keyP95 := s.procKeys.GetPercentile(0.95)
			fmt.Fprintf(buf, "cop_task: {num: %v, max: %v, min: %v, avg: %v, p95: %v", size,
				execdetails.FormatDuration(time.Duration(vMax.GetFloat64())), execdetails.FormatDuration(time.Duration(vMin.GetFloat64())),
				execdetails.FormatDuration(vAvg), execdetails.FormatDuration(time.Duration(vP95)))
>>>>>>> 6f54a29444a (*: use approximately algorithm to calculate p90 in slowlog. (#44269))
			if keyMax > 0 {
				buf.WriteString(", max_proc_keys: ")
				buf.WriteString(strconv.FormatInt(int64(keyMax), 10))
				buf.WriteString(", p95_proc_keys: ")
				buf.WriteString(strconv.FormatInt(int64(keyP95), 10))
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
				strconv.FormatFloat(float64(s.CoprCacheHitNum)/float64(len(s.copRespTime)), 'f', 2, 64)))
		} else {
			buf.WriteString(", copr_cache: disabled")
		}
		if s.distSQLConcurrency > 0 {
			buf.WriteString(", distsql_concurrency: ")
			buf.WriteString(strconv.FormatInt(int64(s.distSQLConcurrency), 10))
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
<<<<<<< HEAD
=======

func (s *selectResultRuntimeStats) calcCacheHit() float64 {
	hit := s.CoprCacheHitNum
	tot := s.copRespTime.Size()
	if s.storeBatchedNum > 0 {
		tot += int(s.storeBatchedNum)
	}
	if tot == 0 {
		return 0
	}
	return float64(hit) / float64(tot)
}
>>>>>>> 6f54a29444a (*: use approximately algorithm to calculate p90 in slowlog. (#44269))
