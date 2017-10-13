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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var (
	_ NewSelectResult  = &newSelectResult{}
	_ NewPartialResult = &newPartialResult{}
)

// NewSelectResult is an iterator of coprocessor partial results.
type NewSelectResult interface {
	// Next gets the next partial result.
	Next() (NewPartialResult, error)
	// NextRaw gets the next raw result.
	NextRaw() ([]byte, error)
	// Close closes the iterator.
	Close() error
	// Fetch fetches partial results from client.
	// The caller should call SetFields() before call Fetch().
	Fetch(ctx goctx.Context)
}

// NewPartialResult is the result from a single region server.
type NewPartialResult interface {
	// Next returns the next rowData of the sub result.
	// If no more row to return, rowData would be nil.
	Next() (rowData []types.Datum, err error)
	// Close closes the partial result.
	Close() error
}

type newSelectResult struct {
	label     string
	aggregate bool
	resp      kv.Response

	results chan newResultWithErr
	closed  chan struct{}

	rowLen int
}

type newResultWithErr struct {
	result []byte
	err    error
}

func (r *newSelectResult) Fetch(ctx goctx.Context) {
	go r.fetch(ctx)
}

func (r *newSelectResult) fetch(ctx goctx.Context) {
	startTime := time.Now()
	defer func() {
		close(r.results)
		duration := time.Since(startTime)
		queryHistgram.WithLabelValues(r.label).Observe(duration.Seconds())
	}()
	for {
		resultSubset, err := r.resp.Next()
		if err != nil {
			r.results <- newResultWithErr{err: errors.Trace(err)}
			return
		}
		if resultSubset == nil {
			return
		}

		select {
		case r.results <- newResultWithErr{result: resultSubset}:
		case <-r.closed:
			// If selectResult called Close() already, make fetch goroutine exit.
			return
		case <-ctx.Done():
			return
		}
	}
}

// Next returns the next row.
func (r *newSelectResult) Next() (NewPartialResult, error) {
	re := <-r.results
	if re.err != nil {
		return nil, errors.Trace(re.err)
	}
	if re.result == nil {
		return nil, nil
	}
	pr := &newPartialResult{}
	pr.rowLen = r.rowLen
	err := pr.unmarshal(re.result)
	return pr, errors.Trace(err)
}

// NextRaw returns the next raw partial result.
func (r *newSelectResult) NextRaw() ([]byte, error) {
	re := <-r.results
	return re.result, errors.Trace(re.err)
}

// Close closes SelectResult.
func (r *newSelectResult) Close() error {
	// Close this channel tell fetch goroutine to exit.
	close(r.closed)
	return r.resp.Close()
}

type newPartialResult struct {
	resp     *tipb.SelectResponse
	chunkIdx int
	rowLen   int
}

func (pr *newPartialResult) unmarshal(resultSubset []byte) error {
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
func (pr *newPartialResult) Next() (data []types.Datum, err error) {
	chunk := pr.getChunk()
	if chunk == nil {
		return nil, nil
	}
	data = make([]types.Datum, pr.rowLen)
	for i := 0; i < pr.rowLen; i++ {
		var l []byte
		l, chunk.RowsData, err = codec.CutOne(chunk.RowsData)
		if err != nil {
			return nil, errors.Trace(err)
		}
		data[i].SetRaw(l)
	}
	return
}

func (pr *newPartialResult) getChunk() *tipb.Chunk {
	for {
		if pr.chunkIdx >= len(pr.resp.Chunks) {
			return nil
		}
		chunk := &pr.resp.Chunks[pr.chunkIdx]
		if len(chunk.RowsData) > 0 {
			return chunk
		}
		pr.chunkIdx++
	}
}

// Close closes the sub result.
func (pr *newPartialResult) Close() error {
	return nil
}

// NewSelectDAG sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func NewSelectDAG(ctx goctx.Context, client kv.Client, kvReq *kv.Request, colLen int) (NewSelectResult, error) {
	var err error
	defer func() {
		// Add metrics.
		if err != nil {
			queryCounter.WithLabelValues(queryFailed).Inc()
		} else {
			queryCounter.WithLabelValues(querySucc).Inc()
		}
	}()

	resp := client.Send(ctx, kvReq)
	if resp == nil {
		err = errors.New("client returns nil response")
		return nil, errors.Trace(err)
	}
	result := &newSelectResult{
		label:   "dag",
		resp:    resp,
		results: make(chan newResultWithErr, kvReq.Concurrency),
		closed:  make(chan struct{}),
		rowLen:  colLen,
	}
	return result, nil
}

// NewAnalyze do a analyze request.
func NewAnalyze(ctx goctx.Context, client kv.Client, kvReq *kv.Request) (NewSelectResult, error) {
	var err error
	defer func() {
		// Add metrics.
		if err != nil {
			queryCounter.WithLabelValues(queryFailed).Inc()
		} else {
			queryCounter.WithLabelValues(querySucc).Inc()
		}
	}()

	resp := client.Send(ctx, kvReq)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &newSelectResult{
		label:   "analyze",
		resp:    resp,
		results: make(chan newResultWithErr, kvReq.Concurrency),
		closed:  make(chan struct{}),
	}
	return result, nil
}
