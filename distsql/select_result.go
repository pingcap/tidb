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
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
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

type resultWithErr struct {
	result kv.ResultSubset
	err    error
}

type selectResult struct {
	label string
	resp  kv.Response

	results chan resultWithErr
	closed  chan struct{}

	rowLen     int
	fieldTypes []*types.FieldType
	ctx        sessionctx.Context

	selectResp     *tipb.SelectResponse
	selectRespSize int // record the selectResp.Size() when it is initialized.
	respChkIdx     int

	feedback     *statistics.QueryFeedback
	partialCount int64 // number of partial results.
	sqlType      string
	encodeType   tipb.EncodeType

	// copPlanIDs contains all copTasks' planIDs,
	// which help to collect copTasks' runtime stats.
	copPlanIDs []fmt.Stringer
	rootPlanID fmt.Stringer

	memTracker *memory.Tracker
}

func (r *selectResult) Fetch(ctx context.Context) {
	go r.fetch(ctx)
}

func (r *selectResult) fetch(ctx context.Context) {
	startTime := time.Now()
	defer func() {
		if c := recover(); c != nil {
			err := fmt.Errorf("%v", c)
			logutil.Logger(ctx).Error("OOM", zap.Error(err))
			r.results <- resultWithErr{err: err}
		}

		close(r.results)
		duration := time.Since(startTime)
		// TODO: Add a label to distinguish between success or failure.
		// https://github.com/pingcap/tidb/issues/11397
		metrics.DistSQLQueryHistgram.WithLabelValues(r.label, r.sqlType).Observe(duration.Seconds())
	}()
	for {
		var result resultWithErr
		resultSubset, err := r.resp.Next(ctx)
		if err != nil {
			result.err = err
		} else if resultSubset == nil {
			// If the result is drained, the resultSubset would be nil
			return
		} else {
			result.result = resultSubset
			r.memConsume(int64(resultSubset.MemSize()))
		}

		select {
		case r.results <- result:
		case <-r.closed:
			// If selectResult called Close() already, make fetch goroutine exit.
			if resultSubset != nil {
				r.memConsume(-int64(resultSubset.MemSize()))
			}
			return
		case <-ctx.Done():
			if resultSubset != nil {
				r.memConsume(-int64(resultSubset.MemSize()))
			}
			return
		}
	}
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) (data []byte, err error) {
	re := <-r.results
	r.partialCount++
	r.feedback.Invalidate()
	if re.result != nil && re.err == nil {
		data = re.result.GetData()
	}
	return data, re.err
}

// Next reads data to the chunk.
func (r *selectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	// Check the returned data is default/arrow format.
	if r.selectResp == nil || r.respChkIdx == len(r.selectResp.Chunks) {
		err := r.getSelectResp()
		if err != nil || r.selectResp == nil {
			return err
		}
	}
	// TODO(Shenghui Wu): add metrics
	switch r.selectResp.EncodeType {
	case tipb.EncodeType_TypeDefault:
		return r.readFromDefault(ctx, chk)
	case tipb.EncodeType_TypeArrow:
		return r.readFromArrow(ctx, chk)
	}
	return errors.Errorf("unsupported encode type:%v", r.encodeType)
}

func (r *selectResult) readFromDefault(ctx context.Context, chk *chunk.Chunk) error {
	for !chk.IsFull() {
		if r.respChkIdx == len(r.selectResp.Chunks) {
			err := r.getSelectResp()
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

func (r *selectResult) readFromArrow(ctx context.Context, chk *chunk.Chunk) error {
	rowBatchData := r.selectResp.Chunks[r.respChkIdx].RowsData
	codec := chunk.NewCodec(r.fieldTypes)
	_ = codec.DecodeToChunk(rowBatchData, chk)
	r.respChkIdx++
	return nil
}

func (r *selectResult) getSelectResp() error {
	r.respChkIdx = 0
	for {
		re := <-r.results
		if re.err != nil {
			return errors.Trace(re.err)
		}
		if r.selectResp != nil {
			r.memConsume(-int64(r.selectRespSize))
		}
		if re.result == nil {
			r.selectResp = nil
			return nil
		}
		r.memConsume(-int64(re.result.MemSize()))
		r.selectResp = new(tipb.SelectResponse)
		err := r.selectResp.Unmarshal(re.result.GetData())
		if err != nil {
			return errors.Trace(err)
		}
		r.selectRespSize = r.selectResp.Size()
		r.memConsume(int64(r.selectRespSize))
		if err := r.selectResp.Error; err != nil {
			return terror.ClassTiKV.New(terror.ErrCode(err.Code), err.Msg)
		}
		sc := r.ctx.GetSessionVars().StmtCtx
		for _, warning := range r.selectResp.Warnings {
			sc.AppendWarning(terror.ClassTiKV.New(terror.ErrCode(warning.Code), warning.Msg))
		}
		r.updateCopRuntimeStats(re.result.GetExecDetails().CalleeAddress, re.result.RespTime())
		r.feedback.Update(re.result.GetStartKey(), r.selectResp.OutputCounts)
		r.partialCount++
		sc.MergeExecDetails(re.result.GetExecDetails(), nil)
		if len(r.selectResp.Chunks) == 0 {
			continue
		}
		return nil
	}
}

func (r *selectResult) updateCopRuntimeStats(callee string, respTime time.Duration) {
	if r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl == nil || callee == "" {
		return
	}
	if len(r.selectResp.GetExecutionSummaries()) != len(r.copPlanIDs) {
		logutil.BgLogger().Error("invalid cop task execution summaries length",
			zap.Int("expected", len(r.copPlanIDs)),
			zap.Int("received", len(r.selectResp.GetExecutionSummaries())))

		return
	}

	r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RecordOneReaderStats(r.rootPlanID.String(), respTime)
	for i, detail := range r.selectResp.GetExecutionSummaries() {
		if detail != nil && detail.TimeProcessedNs != nil &&
			detail.NumProducedRows != nil && detail.NumIterations != nil {
			planID := r.copPlanIDs[i]
			r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.
				RecordOneCopTask(planID.String(), callee, detail)
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
	// Close this channel to tell the fetch goroutine to exit.
	close(r.closed)
	for re := range r.results {
		if re.result != nil {
			r.memConsume(-int64(re.result.MemSize()))
		}
	}
	if r.selectResp != nil {
		r.memConsume(-int64(r.selectRespSize))
	}
	return r.resp.Close()
}
