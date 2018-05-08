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
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/goroutine_pool"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

var (
	errInvalidResp = terror.ClassXEval.New(codeInvalidResp, "invalid response")
	selectResultGP = gp.New(time.Minute * 2)
)

var (
	_ SelectResult = &selectResult{}
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// NextRaw gets the next raw result.
	NextRaw(context.Context) ([]byte, error)
	// Next reads the data into chunk.
	Next(context.Context, *chunk.Chunk) error
	// Close closes the iterator.
	Close() error
	// Fetch fetches partial results from client.
	// The caller should call SetFields() before call Fetch().
	Fetch(context.Context)
}

type selectResult struct {
	label string
	resp  kv.Response

	results chan resultWithErr
	closed  chan struct{}

	rowLen     int
	fieldTypes []*types.FieldType
	ctx        sessionctx.Context

	selectResp *tipb.SelectResponse
	respChkIdx int

	feedback     *statistics.QueryFeedback
	partialCount int64 // number of partial results.
}

type resultWithErr struct {
	result kv.ResultSubset
	err    error
}

func (r *selectResult) Fetch(ctx context.Context) {
	selectResultGP.Go(func() {
		r.fetch(ctx)
	})
}

func (r *selectResult) fetch(ctx context.Context) {
	startTime := time.Now()
	defer func() {
		close(r.results)
		duration := time.Since(startTime)
		metrics.DistSQLQueryHistgram.WithLabelValues(r.label).Observe(duration.Seconds())
	}()
	for {
		resultSubset, err := r.resp.Next(ctx)
		if err != nil {
			r.results <- resultWithErr{err: errors.Trace(err)}
			return
		}
		if resultSubset == nil {
			return
		}

		select {
		case r.results <- resultWithErr{result: resultSubset}:
		case <-r.closed:
			// If selectResult called Close() already, make fetch goroutine exit.
			return
		case <-ctx.Done():
			return
		}
	}
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) ([]byte, error) {
	re := <-r.results
	r.partialCount++
	r.feedback.Invalidate()
	if re.result == nil || re.err != nil {
		return nil, errors.Trace(re.err)
	}
	return re.result.GetData(), nil
}

// Next reads data to the chunk.
func (r *selectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for chk.NumRows() < r.ctx.GetSessionVars().MaxChunkSize {
		if r.selectResp == nil || r.respChkIdx == len(r.selectResp.Chunks) {
			err := r.getSelectResp()
			if err != nil || r.selectResp == nil {
				return errors.Trace(err)
			}
		}
		err := r.readRowsData(chk)
		if err != nil {
			return errors.Trace(err)
		}
		if len(r.selectResp.Chunks[r.respChkIdx].RowsData) == 0 {
			r.respChkIdx++
		}
	}
	return nil
}

func (r *selectResult) getSelectResp() error {
	r.respChkIdx = 0
	for {
		re := <-r.results
		if re.err != nil {
			return errors.Trace(re.err)
		}
		if re.result == nil {
			r.selectResp = nil
			return nil
		}
		r.selectResp = new(tipb.SelectResponse)
		err := r.selectResp.Unmarshal(re.result.GetData())
		if err != nil {
			return errors.Trace(err)
		}
		if err := r.selectResp.Error; err != nil {
			return terror.ClassTiKV.New(terror.ErrCode(err.Code), err.Msg)
		}
		for _, warning := range r.selectResp.Warnings {
			r.ctx.GetSessionVars().StmtCtx.AppendWarning(terror.ClassTiKV.New(terror.ErrCode(warning.Code), warning.Msg))
		}
		r.feedback.Update(re.result.GetStartKey(), r.selectResp.OutputCounts)
		r.partialCount++
		if len(r.selectResp.Chunks) == 0 {
			continue
		}
		return nil
	}
}

func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData
	maxChunkSize := r.ctx.GetSessionVars().MaxChunkSize
	decoder := codec.NewDecoder(chk, r.ctx.GetSessionVars().GetTimeZone())
	for chk.NumRows() < maxChunkSize && len(rowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			rowsData, err = decoder.DecodeOne(rowsData, i, r.fieldTypes[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	r.selectResp.Chunks[r.respChkIdx].RowsData = rowsData
	return nil
}

// Close closes selectResult.
func (r *selectResult) Close() error {
	// Close this channel tell fetch goroutine to exit.
	if r.feedback.Actual() >= 0 {
		metrics.DistSQLScanKeysHistogram.Observe(float64(r.feedback.Actual()))
	}
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	close(r.closed)
	return r.resp.Close()
}

func readRowFromChunk(chunk *tipb.Chunk, numCols int) (row []types.Datum, err error) {
	row = make([]types.Datum, numCols)
	for i := 0; i < numCols; i++ {
		var raw []byte
		raw, chunk.RowsData, err = codec.CutOne(chunk.RowsData)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row[i].SetRaw(raw)
	}
	return
}

// Select sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func Select(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request, fieldTypes []*types.FieldType, fb *statistics.QueryFeedback) (SelectResult, error) {
	// For testing purpose.
	if hook := ctx.Value("CheckSelectRequestHook"); hook != nil {
		hook.(func(*kv.Request))(kvReq)
	}

	if !sctx.GetSessionVars().EnableStreaming {
		kvReq.Streaming = false
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars)
	if resp == nil {
		err := errors.New("client returns nil response")
		return nil, errors.Trace(err)
	}

	if kvReq.Streaming {
		return &streamResult{
			resp:       resp,
			rowLen:     len(fieldTypes),
			fieldTypes: fieldTypes,
			ctx:        sctx,
			feedback:   fb,
		}, nil
	}

	return &selectResult{
		label:      "dag",
		resp:       resp,
		results:    make(chan resultWithErr, kvReq.Concurrency),
		closed:     make(chan struct{}),
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        sctx,
		feedback:   fb,
	}, nil
}

// Analyze do a analyze request.
func Analyze(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label:    "analyze",
		resp:     resp,
		results:  make(chan resultWithErr, kvReq.Concurrency),
		closed:   make(chan struct{}),
		feedback: statistics.NewQueryFeedback(0, nil, 0, false),
	}
	return result, nil
}

// Checksum sends a checksum request.
func Checksum(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label:    "checksum",
		resp:     resp,
		results:  make(chan resultWithErr, kvReq.Concurrency),
		closed:   make(chan struct{}),
		feedback: statistics.NewQueryFeedback(0, nil, 0, false),
	}
	return result, nil
}

// XAPI error codes.
const (
	codeInvalidResp = 1
)
