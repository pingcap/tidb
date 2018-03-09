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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/goroutine_pool"
	tipb "github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

var (
	errInvalidResp = terror.ClassXEval.New(codeInvalidResp, "invalid response")
	selectResultGP = gp.New(time.Minute * 2)
)

var (
	_ SelectResult  = &selectResult{}
	_ PartialResult = &partialResult{}
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// Next gets the next partial result.
	Next(context.Context) (PartialResult, error)
	// NextRaw gets the next raw result.
	NextRaw(context.Context) ([]byte, error)
	// NextChunk reads the data into chunk.
	NextChunk(context.Context, *chunk.Chunk) error
	// Close closes the iterator.
	Close() error
	// Fetch fetches partial results from client.
	// The caller should call SetFields() before call Fetch().
	Fetch(context.Context)
	// ScanKeys gets the total scan row count.
	ScanKeys() int64
}

// PartialResult is the result from a single region server.
type PartialResult interface {
	// Next returns the next rowData of the sub result.
	// If no more row to return, rowData would be nil.
	Next(context.Context) (rowData []types.Datum, err error)
	// Close closes the partial result.
	Close() error
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

	scanKeys     int64 // number of keys scanned by TiKV.
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

// Next returns the next row.
func (r *selectResult) Next(ctx context.Context) (PartialResult, error) {
	re := <-r.results
	if re.err != nil {
		return nil, errors.Trace(re.err)
	}
	if re.result == nil {
		return nil, nil
	}
	pr := &partialResult{}
	pr.rowLen = r.rowLen
	err := pr.unmarshal(re.result.GetData())
	if len(pr.resp.OutputCounts) > 0 {
		scanKeysPartial := pr.resp.OutputCounts[0]
		metrics.DistSQLScanKeysPartialHistogram.Observe(float64(scanKeysPartial))
		r.scanKeys += scanKeysPartial
	} else {
		r.scanKeys = -1
	}
	r.partialCount++
	return pr, errors.Trace(err)
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) ([]byte, error) {
	re := <-r.results
	r.partialCount++
	r.scanKeys = -1
	if re.result == nil || re.err != nil {
		return nil, errors.Trace(re.err)
	}
	return re.result.GetData(), nil
}

// NextChunk reads data to the chunk.
func (r *selectResult) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
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
		if len(r.selectResp.OutputCounts) > 0 {
			scanCountPartial := r.selectResp.OutputCounts[0]
			metrics.DistSQLScanKeysPartialHistogram.Observe(float64(scanCountPartial))
			r.scanKeys += scanCountPartial
		} else {
			r.scanKeys = -1
		}
		r.partialCount++
		if len(r.selectResp.Chunks) == 0 {
			continue
		}
		return nil
	}
}

func (r *selectResult) ScanKeys() int64 {
	return r.scanKeys
}

func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData
	maxChunkSize := r.ctx.GetSessionVars().MaxChunkSize
	timeZone := r.ctx.GetSessionVars().GetTimeZone()
	for chk.NumRows() < maxChunkSize && len(rowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			rowsData, err = codec.DecodeOneToChunk(rowsData, chk, i, r.fieldTypes[i], timeZone)
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
	if r.scanKeys >= 0 {
		metrics.DistSQLScanKeysHistogram.Observe(float64(r.scanKeys))
	}
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	close(r.closed)
	return r.resp.Close()
}

type partialResult struct {
	resp     *tipb.SelectResponse
	chunkIdx int
	rowLen   int
}

func (pr *partialResult) unmarshal(resultSubset []byte) error {
	pr.resp = new(tipb.SelectResponse)
	err := pr.resp.Unmarshal(resultSubset)
	if err != nil {
		return errors.Trace(err)
	}

	if pr.resp.Error != nil {
		return errInvalidResp.Gen("[%d %s]", pr.resp.Error.GetCode(), pr.resp.Error.GetMsg())
	}

	return nil
}

// Next returns the next row of the sub result.
// If no more row to return, data would be nil.
func (pr *partialResult) Next(ctx context.Context) (data []types.Datum, err error) {
	nextChunk := pr.getChunk()
	if nextChunk == nil {
		return nil, nil
	}
	return readRowFromChunk(nextChunk, pr.rowLen)
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

func (pr *partialResult) getChunk() *tipb.Chunk {
	for {
		if pr.chunkIdx >= len(pr.resp.Chunks) {
			return nil
		}
		currentChunk := &pr.resp.Chunks[pr.chunkIdx]
		if len(currentChunk.RowsData) > 0 {
			return currentChunk
		}
		pr.chunkIdx++
	}
}

// Close closes the sub result.
func (pr *partialResult) Close() error {
	return nil
}

// Select sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func Select(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request, fieldTypes []*types.FieldType) (SelectResult, error) {
	// For testing purpose.
	if hook := ctx.Value("CheckSelectRequestHook"); hook != nil {
		hook.(func(*kv.Request))(kvReq)
	}

	if !sctx.GetSessionVars().EnableStreaming {
		kvReq.Streaming = false
	}
	resp := sctx.GetClient().Send(ctx, kvReq)
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
	}, nil
}

// Analyze do a analyze request.
func Analyze(ctx context.Context, client kv.Client, kvReq *kv.Request) (SelectResult, error) {
	resp := client.Send(ctx, kvReq)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label:   "analyze",
		resp:    resp,
		results: make(chan resultWithErr, kvReq.Concurrency),
		closed:  make(chan struct{}),
	}
	return result, nil
}

// XAPI error codes.
const (
	codeInvalidResp = 1
)
