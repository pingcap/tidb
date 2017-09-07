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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

// AnalyzeResult is an iterator of coprocessor partial analyze results.
type AnalyzeResult struct {
	resp    kv.Response
	results chan analyzeResultWithError
	closed  chan struct{}
}

type analyzeResultWithError struct {
	result []byte
	err    error
}

// Fetch fetches partial analyze results from client.
func (r *AnalyzeResult) Fetch(ctx goctx.Context) {
	go r.fetch(ctx)
}

func (r *AnalyzeResult) fetch(ctx goctx.Context) {
	for {
		resultSubset, err := r.resp.Next()
		if err != nil {
			r.results <- analyzeResultWithError{err: errors.Trace(err)}
			return
		}
		if resultSubset == nil {
			r.results <- analyzeResultWithError{}
			return
		}

		select {
		case r.results <- analyzeResultWithError{result: resultSubset}:
		case <-r.closed:
			// if AnalyzeResult called Close() already, make fetch goroutine exit
			return
		case <-ctx.Done():
			return
		}
	}
}

// Next gets the next partial result.
func (r *AnalyzeResult) Next() ([]byte, error) {
	re := <-r.results
	return re.result, errors.Trace(re.err)
}

// Close closes the iterator.
func (r *AnalyzeResult) Close() error {
	// close this channel tell fetch goroutine to exit
	close(r.closed)
	return r.resp.Close()
}

// Analyze do a analyze request, returns AnalyzeResult.
func Analyze(client kv.Client, ctx goctx.Context, req *tipb.AnalyzeReq, keyRanges []kv.KeyRange, concurrency int, keepOrder bool, priority int) (*AnalyzeResult, error) {
	kvReq := &kv.Request{
		Tp:             kv.ReqTypeAnalyze,
		Concurrency:    concurrency,
		KeepOrder:      keepOrder,
		KeyRanges:      keyRanges,
		Desc:           false,
		IsolationLevel: kv.RC,
		Priority:       priority,
	}
	var err error
	kvReq.Data, err = req.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}

	resp := client.Send(ctx, kvReq)
	if resp == nil {
		err = errors.New("client returns nil response")
		return nil, errors.Trace(err)
	}
	result := &AnalyzeResult{
		resp:    resp,
		results: make(chan analyzeResultWithError, concurrency),
		closed:  make(chan struct{}),
	}
	return result, nil
}
