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
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/distsql/xeval"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// MockDAGRequest is used for testing now.
var MockDAGRequest bool

type dagContext struct {
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	// TODO: Remove it.
	eval    *xeval.Evaluator
	evalCtx *evalContext
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
		evalCtx:   &evalContext{sc: sc},
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
	case tipb.ExecType_TypeTopN:
		currExec, err = h.buildTopN(ctx, curr)
	case tipb.ExecType_TypeLimit:
		currExec = &limitExec{limit: curr.Limit.GetLimit()}
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
	ctx.evalCtx.setColumnInfo(columns)
	ranges := h.extractKVRanges(ctx.keyRanges, *executor.TblScan.Desc)

	return &tableScanExec{
		TableScan:   executor.TblScan,
		kvRanges:    ranges,
		colIDs:      ctx.eval.ColIDs,
		startTS:     ctx.dagReq.GetStartTs(),
		mvccStore:   h.mvccStore,
		rawStartKey: h.rawStartKey,
		rawEndKey:   h.rawEndKey,
	}
}

func (h *rpcHandler) buildIndexScan(ctx *dagContext, executor *tipb.Executor) *indexScanExec {
	columns := executor.IdxScan.Columns
	ctx.eval.SetColumnInfos(columns)
	ctx.evalCtx.setColumnInfo(columns)
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
	pbConds := executor.Selection.Conditions
	var err error
	var relatedColOffsets []int
	for _, cond := range pbConds {
		relatedColOffsets, err = extractOffsetsInExpr(cond, ctx.eval.ColumnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	conds, err := convertToExprs(ctx.eval.StatementCtx, ctx.evalCtx.colIDs, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &selectionExec{
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		conditions:        conds,
		row:               make([]types.Datum, len(ctx.eval.ColumnInfos)),
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

func (h *rpcHandler) buildTopN(ctx *dagContext, executor *tipb.Executor) (*topNExec, error) {
	topN := executor.TopN
	var err error
	var relatedColOffsets []int
	pbConds := make([]*tipb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
		relatedColOffsets, err = extractOffsetsInExpr(item.Expr, ctx.eval.ColumnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pbConds[i] = item.Expr
	}
	heap := &topnHeap{
		totalCount: int(*topN.Limit),
		topnSorter: topnSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.eval.StatementCtx,
		},
	}

	conds, err := convertToExprs(ctx.eval.StatementCtx, ctx.evalCtx.colIDs, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &topNExec{
		heap:              heap,
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		orderByExprs:      conds,
		row:               make([]types.Datum, len(ctx.eval.ColumnInfos)),
	}, nil
}

type evalContext struct {
	colIDs      map[int64]int
	columnInfos []*tipb.ColumnInfo
	fieldTps    []*types.FieldType
	sc          *variable.StatementContext
}

func (e *evalContext) setColumnInfo(cols []*tipb.ColumnInfo) {
	e.columnInfos = make([]*tipb.ColumnInfo, len(cols))
	copy(e.columnInfos, cols)

	e.colIDs = make(map[int64]int)
	e.fieldTps = make([]*types.FieldType, 0, len(e.columnInfos))
	for i, col := range e.columnInfos {
		ft := distsql.FieldTypeFromPBColumn(col)
		e.fieldTps = append(e.fieldTps, ft)
		e.colIDs[col.GetColumnId()] = i
	}
}

// decodeRelatedColumnVals decodes data to Datum slice according to the row information.
func (e *evalContext) decodeRelatedColumnVals(relatedColOffsets []int, handle int64, value [][]byte, row []types.Datum) error {
	var err error
	for _, offset := range relatedColOffsets {
		col := e.columnInfos[offset]
		if col.GetPkHandle() {
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				row[offset] = types.NewUintDatum(uint64(handle))
			} else {
				row[offset] = types.NewIntDatum(handle)
			}
			continue
		}
		row[offset], err = tablecodec.DecodeColumnValue(value[offset], e.fieldTps[offset])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
