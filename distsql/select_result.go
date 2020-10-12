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
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	errQueryInterrupted = terror.ClassExecutor.NewStd(errno.ErrQueryInterrupted)

	coprCacheHistogramHit  = metrics.DistSQLCoprCacheHistogram.WithLabelValues("hit")
	coprCacheHistogramMiss = metrics.DistSQLCoprCacheHistogram.WithLabelValues("miss")
)

var (
	_ SelectResult = (*selectResult)(nil)
	_ SelectResult = (*streamResult)(nil)
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// Fetch fetches partial results from client.
	Fetch(context.Context)
	// NextRaw gets the next raw result.
	NextRaw(context.Context) ([]byte, error)
	// Next reads the data into chunk.
	Next(context.Context, *chunk.Chunk) error
	// Close closes the iterator.
	Close() error
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
	encodeType   tipb.EncodeType

	// copPlanIDs contains all copTasks' planIDs,
	// which help to collect copTasks' runtime stats.
	copPlanIDs []int
	rootPlanID int

	fetchDuration    time.Duration
	durationReported bool
	memTracker       *memory.Tracker

	stats *selectResultRuntimeStats
}

func (r *selectResult) Fetch(ctx context.Context) {
}

func (r *selectResult) fetchResp(ctx context.Context) error {
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
				metrics.DistSQLQueryHistogram.WithLabelValues(r.label, r.sqlType).Observe(r.fetchDuration.Seconds())
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
			return terror.ClassTiKV.Synthesize(terror.ErrCode(err.Code), err.Msg)
		}
		sessVars := r.ctx.GetSessionVars()
		if atomic.LoadUint32(&sessVars.Killed) == 1 {
			return errors.Trace(errQueryInterrupted)
		}
		sc := sessVars.StmtCtx
		for _, warning := range r.selectResp.Warnings {
			sc.AppendWarning(terror.ClassTiKV.Synthesize(terror.ErrCode(warning.Code), warning.Msg))
		}
		r.feedback.Update(resultSubset.GetStartKey(), r.selectResp.OutputCounts)
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
	if r.stats != nil {
		coprCacheHistogramHit.Observe(float64(r.stats.CoprCacheHitNum))
		coprCacheHistogramMiss.Observe(float64(len(r.stats.copRespTime) - int(r.stats.CoprCacheHitNum)))
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
	switch r.selectResp.GetEncodeType() {
	case tipb.EncodeType_TypeDefault:
		return r.readFromDefault(ctx, chk)
	case tipb.EncodeType_TypeChunk:
		return r.readFromChunk(ctx, chk)
	}
	return errors.Errorf("unsupported encode type:%v", r.encodeType)
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) (data []byte, err error) {
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

func (r *selectResult) updateCopRuntimeStats(ctx context.Context, copStats *tikv.CopRuntimeStats, respTime time.Duration) {
	callee := copStats.CalleeAddress
	if r.rootPlanID <= 0 || r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl == nil || callee == "" {
		return
	}
	if len(r.selectResp.GetExecutionSummaries()) != len(r.copPlanIDs) {
		logutil.Logger(ctx).Error("invalid cop task execution summaries length",
			zap.Int("expected", len(r.copPlanIDs)),
			zap.Int("received", len(r.selectResp.GetExecutionSummaries())))

		return
	}
	if r.stats == nil {
		id := r.rootPlanID
		r.stats = &selectResultRuntimeStats{
			backoffSleep: make(map[string]time.Duration),
			rpcStat:      tikv.NewRegionRequestRuntimeStats(),
		}
		r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(id, r.stats)
	}
	r.stats.mergeCopRuntimeStats(copStats, respTime)

	for i, detail := range r.selectResp.GetExecutionSummaries() {
		if detail != nil && detail.TimeProcessedNs != nil &&
			detail.NumProducedRows != nil && detail.NumIterations != nil {
			planID := r.copPlanIDs[i]
			r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.
				RecordOneCopTask(planID, callee, detail)
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
	return r.resp.Close()
}

// CopRuntimeStats is a interface uses to check whether the result has cop runtime stats.
type CopRuntimeStats interface {
	// GetCopRuntimeStats gets the cop runtime stats information.
	GetCopRuntimeStats() *tikv.CopRuntimeStats
}

type selectResultRuntimeStats struct {
	copRespTime      []time.Duration
	procKeys         []int64
	backoffSleep     map[string]time.Duration
	totalProcessTime time.Duration
	totalWaitTime    time.Duration
	rpcStat          tikv.RegionRequestRuntimeStats
	CoprCacheHitNum  int64
}

func (s *selectResultRuntimeStats) mergeCopRuntimeStats(copStats *tikv.CopRuntimeStats, respTime time.Duration) {
	s.copRespTime = append(s.copRespTime, respTime)
	s.procKeys = append(s.procKeys, copStats.ProcessedKeys)

	for k, v := range copStats.BackoffSleep {
		s.backoffSleep[k] += v
	}
	s.totalProcessTime += copStats.ProcessTime
	s.totalWaitTime += copStats.WaitTime
	s.rpcStat.Merge(copStats.RegionRequestRuntimeStats)
	if copStats.CoprCacheHit {
		s.CoprCacheHitNum++
	}
}

func (s *selectResultRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := selectResultRuntimeStats{
		copRespTime:  make([]time.Duration, 0, len(s.copRespTime)),
		procKeys:     make([]int64, 0, len(s.procKeys)),
		backoffSleep: make(map[string]time.Duration, len(s.backoffSleep)),
		rpcStat:      tikv.NewRegionRequestRuntimeStats(),
	}
	newRs.copRespTime = append(newRs.copRespTime, s.copRespTime...)
	newRs.procKeys = append(newRs.procKeys, s.procKeys...)
	for k, v := range s.backoffSleep {
		newRs.backoffSleep[k] += v
	}
	newRs.totalProcessTime += s.totalProcessTime
	newRs.totalWaitTime += s.totalWaitTime
	for k, v := range s.rpcStat.Stats {
		newRs.rpcStat.Stats[k] = v
	}
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
}

func (s *selectResultRuntimeStats) String() string {
	buf := bytes.NewBuffer(nil)
	if len(s.copRespTime) > 0 {
		size := len(s.copRespTime)
		if size == 1 {
			buf.WriteString(fmt.Sprintf("cop_task: {num: 1, max:%v, proc_keys: %v", s.copRespTime[0], s.procKeys[0]))
		} else {
			sort.Slice(s.copRespTime, func(i, j int) bool {
				return s.copRespTime[i] < s.copRespTime[j]
			})
			vMax, vMin := s.copRespTime[size-1], s.copRespTime[0]
			vP95 := s.copRespTime[size*19/20]
			sum := 0.0
			for _, t := range s.copRespTime {
				sum += float64(t)
			}
			vAvg := time.Duration(sum / float64(size))

			sort.Slice(s.procKeys, func(i, j int) bool {
				return s.procKeys[i] < s.procKeys[j]
			})
			keyMax := s.procKeys[size-1]
			keyP95 := s.procKeys[size*19/20]
			buf.WriteString(fmt.Sprintf("cop_task: {num: %v, max: %v, min: %v, avg: %v, p95: %v", size, vMax, vMin, vAvg, vP95))
			if keyMax > 0 {
				buf.WriteString(", max_proc_keys: ")
				buf.WriteString(strconv.FormatInt(keyMax, 10))
				buf.WriteString(", p95_proc_keys: ")
				buf.WriteString(strconv.FormatInt(keyP95, 10))
			}
			if s.totalProcessTime > 0 {
				buf.WriteString(", tot_proc: ")
				buf.WriteString(s.totalProcessTime.String())
				if s.totalWaitTime > 0 {
					buf.WriteString(", tot_wait: ")
					buf.WriteString(s.totalWaitTime.String())
				}
			}
		}
		copRPC := s.rpcStat.Stats[tikvrpc.CmdCop]
		if copRPC != nil && copRPC.Count > 0 {
			delete(s.rpcStat.Stats, tikvrpc.CmdCop)
			buf.WriteString(", rpc_num: ")
			buf.WriteString(strconv.FormatInt(copRPC.Count, 10))
			buf.WriteString(", rpc_time: ")
			buf.WriteString(time.Duration(copRPC.Consume).String())
		}
		buf.WriteString(fmt.Sprintf(", copr_cache_hit_ratio: %v",
			strconv.FormatFloat(float64(s.CoprCacheHitNum)/float64(len(s.copRespTime)), 'f', 2, 64)))
		buf.WriteString("}")
	}

	rpcStatsStr := s.rpcStat.String()
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
			buf.WriteString(fmt.Sprintf("%s: %s", k, d.String()))
		}
		buf.WriteString("}")
	}
	return buf.String()
}

// Tp implements the RuntimeStats interface.
func (s *selectResultRuntimeStats) Tp() int {
	return execdetails.TpSelectResultRuntimeStats
}
