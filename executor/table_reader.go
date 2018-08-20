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

package executor

import (
	"sync"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ Executor = &TableReaderExecutor{}

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type TableReaderExecutor struct {
	baseExecutor

	table     table.Table
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []*ranger.Range
	dagPB     *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler resultHandler
	streaming     bool
	feedback      *statistics.QueryFeedback

	// corColInFilter tells whether there's correlated column in filter.
	corColInFilter bool
	// corColInAccess tells whether there's correlated column in access conditions.
	corColInAccess bool
	plans          []plan.PhysicalPlan
}

// Open initialzes necessary variables for using this executor.
func (e *TableReaderExecutor) Open(ctx context.Context) error {
	span, ctx := startSpanFollowsContext(ctx, "executor.TableReader.Open")
	defer span.Finish()

	var err error
	if e.corColInFilter {
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if e.corColInAccess {
		ts := e.plans[0].(*plan.PhysicalTableScan)
		access := ts.AccessCondition
		pkTP := ts.Table.GetPkColInfo().FieldType
		e.ranges, err = ranger.BuildTableRange(access, e.ctx.GetSessionVars().StmtCtx, &pkTP)
		if err != nil {
			return errors.Trace(err)
		}
	}

	firstPartRanges, secondPartRanges := splitRanges(e.ranges, e.keepOrder)

	if e.keepOrder || len(e.ranges) <= rangePerRequest {
		handler := &tableResultHandler{}
		e.resultHandler = handler
		firstResult, err := e.buildResp(ctx, firstPartRanges)
		if err != nil {
			e.feedback.Invalidate()
			return errors.Trace(err)
		}
		if len(secondPartRanges) == 0 {
			handler.open(nil, firstResult)
			e.resultHandler = handler
			return nil
		}
		var secondResult distsql.SelectResult
		secondResult, err = e.buildResp(ctx, secondPartRanges)
		if err != nil {
			e.feedback.Invalidate()
			return errors.Trace(err)
		}
		handler.open(firstResult, secondResult)
		return nil
	}
	handler := &multiRangeNoOrderResultHandler{
		buildResp: e.buildResp,
		retTypes:  e.retTypes(),
	}
	handler.openRanges(ctx, append(secondPartRanges, firstPartRanges...))
	e.resultHandler = handler
	return nil
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by tableReaderHandler.
func (e *TableReaderExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	err := e.resultHandler.nextChunk(ctx, chk)
	if err != nil {
		e.feedback.Invalidate()
	}
	return errors.Trace(err)
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	e.ctx.StoreQueryFeedback(e.feedback)
	err := e.resultHandler.Close()
	return errors.Trace(err)
}

// buildResp first build request and send it to tikv using distsql.Select. It uses SelectResut returned by the callee
// to fetch all results.
func (e *TableReaderExecutor) buildResp(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(e.tableID, ranges, e.feedback).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.retTypes(), e.feedback)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

type buildResp func(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error)

const (
	rangePerRequest    = 50000
	requestWorkerCount = 8
)

type resultHandler interface {
	nextChunk(ctx context.Context, chk *chunk.Chunk) error
	Close() error
}

type tableResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true.
	// optionalResult handles the request whose range is in signed int range.
	// result handles the request whose range is exceed signed int range.
	// Otherwise, we just set optionalFinished true and the result handles the whole ranges.
	optionalResult   distsql.SelectResult
	result           distsql.SelectResult
	optionalFinished bool
}

func (tr *tableResultHandler) open(optionalResult, result distsql.SelectResult) {
	if optionalResult == nil {
		tr.optionalFinished = true
		tr.result = result
		return
	}
	tr.optionalResult = optionalResult
	tr.result = result
	tr.optionalFinished = false
}

func (tr *tableResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if !tr.optionalFinished {
		err := tr.optionalResult.Next(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() > 0 {
			return nil
		}
		tr.optionalFinished = true
	}
	return tr.result.Next(ctx, chk)
}

func (tr *tableResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	if !tr.optionalFinished {
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (tr *tableResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return errors.Trace(err)
}

type resultWithErr struct {
	chk *chunk.Chunk
	err error
}

// multiRangeNoOrderResultHandler will handle the case that the range is too large and the we don't need to keep the order of the result.
type multiRangeNoOrderResultHandler struct {
	buildResp     buildResp
	ranges        []*ranger.Range
	rangeCh       chan []*ranger.Range
	resultCh      chan *resultWithErr
	resultWg      *sync.WaitGroup
	retTypes      []*types.FieldType
	chkResourceCh chan *chunk.Chunk
	cancelFunc    context.CancelFunc
}

func (mr *multiRangeNoOrderResultHandler) openRanges(ctx context.Context, ranges []*ranger.Range) {
	mr.ranges = ranges
	mr.rangeCh = make(chan []*ranger.Range, 1)
	mr.resultCh = make(chan *resultWithErr, 1)
	count := mathutil.Min(requestWorkerCount, (len(ranges)+rangePerRequest-1)/rangePerRequest)
	childCtx, cancelFunc := context.WithCancel(ctx)
	mr.cancelFunc = cancelFunc

	go mr.splitRanges(childCtx)
	mr.chkResourceCh = make(chan *chunk.Chunk, count)
	for i := 0; i < count; i++ {
		mr.chkResourceCh <- chunk.NewChunkWithCapacity(mr.retTypes, 1024)
	}
	mr.resultWg = &sync.WaitGroup{}
	mr.resultWg.Add(count)
	go mr.checkWg()
	for i := 0; i < count; i++ {
		go mr.newWorker().run(childCtx)
	}
}

func (mr *multiRangeNoOrderResultHandler) newWorker() *resultWorker {
	return &resultWorker{
		rangeCh:         mr.rangeCh,
		resultCh:        mr.resultCh,
		wg:              mr.resultWg,
		buildResp:       mr.buildResp,
		chkResourceChan: mr.chkResourceCh,
		tps:             mr.retTypes,
	}
}

func (mr *multiRangeNoOrderResultHandler) splitRanges(ctx context.Context) {
splitRangeLoop:
	for i := 0; i < len(mr.ranges); {
		nextI := i + rangePerRequest
		if nextI > len(mr.ranges) {
			nextI = len(mr.ranges)
		}
		select {
		case <-ctx.Done():
			break splitRangeLoop
		case mr.rangeCh <- mr.ranges[i:nextI]:
		}
		i = nextI
	}
	close(mr.rangeCh)
}

func (mr *multiRangeNoOrderResultHandler) checkWg() {
	mr.resultWg.Wait()
	close(mr.resultCh)
}

func (mr *multiRangeNoOrderResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	result, ok := <-mr.resultCh
	if !ok {
		chk.Reset()
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}
	chk.SwapColumns(result.chk)
	result.chk.Reset()
	mr.chkResourceCh <- result.chk
	return nil
}

func (mr *multiRangeNoOrderResultHandler) Close() error {
	mr.cancelFunc()
	if mr.chkResourceCh != nil {
		close(mr.chkResourceCh)
		for range mr.chkResourceCh {
		}
	}
	return nil
}

type resultWorker struct {
	rangeCh         <-chan []*ranger.Range
	request         distsql.SelectResult
	resultCh        chan<- *resultWithErr
	buildResp       buildResp
	wg              *sync.WaitGroup
	tps             []*types.FieldType
	chkResourceChan chan *chunk.Chunk
}

func (rw *resultWorker) recvRanges(ctx context.Context) (ranges []*ranger.Range, ok bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case ranges, ok = <-rw.rangeCh:
	}
	return
}

func (rw *resultWorker) run(ctx context.Context) {
	defer func() {
		rw.Close()
	}()
	var (
		err error
		chk *chunk.Chunk
		ok  bool
	)
	for {
		select {
		case chk, ok = <-rw.chkResourceChan:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}
	reUseChunk:
		if rw.request == nil {
			rw.request, err = rw.newRequest(ctx)
			if err != nil {
				rw.sendResult(ctx, &resultWithErr{chk: nil, err: errors.Trace(err)})
				return
			}
			if rw.request == nil {
				return
			}
		}
		err = rw.request.Next(ctx, chk)
		if err != nil {
			rw.sendResult(ctx, &resultWithErr{chk: nil, err: errors.Trace(err)})
			return
		}
		if chk.NumRows() > 0 {
			rw.sendResult(ctx, &resultWithErr{chk: chk, err: nil})
			continue
		}
		err = rw.request.Close()
		if err != nil {
			rw.sendResult(ctx, &resultWithErr{chk: nil, err: errors.Trace(err)})
			return
		}
		rw.request = nil
		goto reUseChunk
	}
}

func (rw *resultWorker) newRequest(ctx context.Context) (distsql.SelectResult, error) {
	ranges, recved := rw.recvRanges(ctx)
	if !recved {
		return nil, nil
	}
	resp, err := rw.buildResp(ctx, ranges)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (rw *resultWorker) sendResult(ctx context.Context, result *resultWithErr) {
	select {
	case <-ctx.Done():
	case rw.resultCh <- result:
	}
}

func (rw *resultWorker) Close() {
	rw.wg.Done()
	if rw.request != nil {
		terror.Call(rw.request.Close)
	}
}
