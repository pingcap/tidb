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

package mocktikv

import (
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/distsql/xeval"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
)

// MockDAGRequest is used for testing now.
var MockDAGRequest bool

type dagContext struct {
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	eval      *xeval.Evaluator
	columns   []*tipb.ColumnInfo
}

func (h *rpcHandler) handleCopDAGRequest(req *coprocessor.Request) (*coprocessor.Response, error) {
	resp := &coprocessor.Response{}
	if len(req.Ranges) == 0 {
		return resp, nil
	}
	if req.GetTp() != kv.ReqTypeDAG {
		return resp, nil
	}
	if err := h.checkContext(req.GetContext()); err != nil {
		resp.RegionError = err
		return resp, nil
	}

	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sc := xeval.FlagsToStatementContext(dagReq.Flags)
	ctx := &dagContext{
		dagReq:    dagReq,
		keyRanges: req.Ranges,
		eval:      xeval.NewEvaluator(sc),
	}
	e, err := h.buildDAG(ctx, dagReq.Executors)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var chunks []tipb.Chunk
	for {
		handle, row, err := e.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		data := dummySlice
		for _, val := range row {
			data = append(data, val...)
		}
		chunks = appendRow(chunks, handle, data)
	}
	return buildResp(chunks, err)
}

func (h *rpcHandler) buildExec(ctx *dagContext, curr *tipb.Executor) (executor, error) {
	var currExec executor
	var err error
	switch curr.GetTp() {
	case tipb.ExecType_TypeTableScan:
		currExec = h.buildTableScan(ctx, curr)
	case tipb.ExecType_TypeIndexScan:
		currExec = h.buildIndexScan(ctx, curr)
	case tipb.ExecType_TypeSelection:
		currExec, err = h.buildSelection(ctx, curr)
	case tipb.ExecType_TypeAggregation:
		currExec, err = h.buildAggregation(ctx, curr)
	case tipb.ExecType_TypeLimit:
		currExec = &limitExec{Limit: curr.Limit}
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", curr.GetTp())
	}

	return currExec, errors.Trace(err)
}

func (h *rpcHandler) buildDAG(ctx *dagContext, executors []*tipb.Executor) (executor, error) {
	var src executor
	for i := 0; i < len(executors); i++ {
		curr, err := h.buildExec(ctx, executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(src)
		src = curr
	}
	return src, nil
}

func (h *rpcHandler) buildTableScan(ctx *dagContext, executor *tipb.Executor) *tableScanExec {
	columns := executor.TblScan.Columns
	ctx.eval.SetColumnInfos(columns)
	colIDs := make(map[int64]int)
	for i, col := range columns {
		colIDs[col.GetColumnId()] = i
	}
	ranges := h.extractKVRanges(ctx.keyRanges, *executor.TblScan.Desc)

	return &tableScanExec{
		TableScan:   executor.TblScan,
		kvRanges:    ranges,
		colIDs:      colIDs,
		startTS:     ctx.dagReq.GetStartTs(),
		mvccStore:   h.mvccStore,
		rawStartKey: h.rawStartKey,
		rawEndKey:   h.rawEndKey,
	}
}

func (h *rpcHandler) buildIndexScan(ctx *dagContext, executor *tipb.Executor) *indexScanExec {
	columns := executor.IdxScan.Columns
	ctx.eval.SetColumnInfos(columns)
	length := len(columns)
	// The PKHandle column info has been collected in ctx.
	if columns[length-1].GetPkHandle() {
		columns = columns[:length-1]
	}
	ranges := h.extractKVRanges(ctx.keyRanges, *executor.IdxScan.Desc)

	return &indexScanExec{
		IndexScan:   executor.IdxScan,
		kvRanges:    ranges,
		colsLen:     len(columns),
		startTS:     ctx.dagReq.GetStartTs(),
		mvccStore:   h.mvccStore,
		rawStartKey: h.rawStartKey,
		rawEndKey:   h.rawEndKey,
	}
}

func (h *rpcHandler) buildSelection(ctx *dagContext, executor *tipb.Executor) (*selectionExec, error) {
	var cond *tipb.Expr
	// TODO: Now len(Conditions) is 1.
	if len(executor.Selection.Conditions) > 0 {
		cond = executor.Selection.Conditions[0]
	}
	colIDs := make(map[int64]int)
	err := extractColIDsInExpr(cond, ctx.eval.ColumnInfos, colIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &selectionExec{
		Selection: executor.Selection,
		eval:      ctx.eval,
		colIDs:    colIDs,
	}, nil
}

func (h *rpcHandler) buildAggregation(ctx *dagContext, executor *tipb.Executor) (*aggregateExec, error) {
	aggs := make([]*aggregateFuncExpr, 0, len(executor.Aggregation.AggFunc))
	colIDs := make(map[int64]int)
	for _, agg := range executor.Aggregation.AggFunc {
		aggExpr := &aggregateFuncExpr{expr: agg}
		aggs = append(aggs, aggExpr)
		err := extractColIDsInExpr(agg, ctx.eval.ColumnInfos, colIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for _, item := range executor.Aggregation.GroupBy {
		err := extractColIDsInExpr(item, ctx.eval.ColumnInfos, colIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return &aggregateExec{
		Aggregation: executor.Aggregation,
		eval:        ctx.eval,
		aggFuncs:    aggs,
		groups:      make(map[string]struct{}),
		groupKeys:   make([][]byte, 0),
		colIDs:      colIDs,
	}, nil
}
