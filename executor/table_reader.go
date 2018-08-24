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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	tipb "github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ Executor = &TableReaderExecutor{}

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type TableReaderExecutor struct {
	baseExecutor

	table           table.Table
	physicalTableID int64
	keepOrder       bool
	desc            bool
	ranges          []*ranger.Range
	dagPB           *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *tableResultHandler
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

	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := splitRanges(e.ranges, e.keepOrder)
	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	e.resultHandler.open(firstResult, secondResult)
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
	kvReq, err := builder.SetTableRanges(e.physicalTableID, ranges, e.feedback).
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

type tableResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true.
	// optionalResult handles the request whose range is in signed int range.
	// result handles the request whose range is exceed signed int range.
	// Otherwise, we just set optionalFinished true and the result handles the whole ranges.
	optionalResult distsql.SelectResult
	result         distsql.SelectResult

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
