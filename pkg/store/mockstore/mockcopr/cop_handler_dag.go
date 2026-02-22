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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockcopr

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

var dummySlice = make([]byte, 0)

type dagContext struct {
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	startTS   uint64
	evalCtx   *evalContext
}

func (h coprHandler) handleCopDAGRequest(req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	dagCtx, e, dagReq, err := h.buildDAGExecutor(req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}

	var rows [][][]byte
	ctx := context.TODO()
	for {
		var row [][]byte
		row, err = e.Next(ctx)
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}

	var execDetails []*execDetail
	if dagReq.GetCollectExecutionSummaries() {
		execDetails = e.ExecDetails()
	}

	sc := dagCtx.evalCtx.sctx.GetSessionVars().StmtCtx
	selResp := h.initSelectResponse(err, sc.GetWarnings(), e.Counts())
	if err == nil {
		err = h.fillUpData4SelectResponse(selResp, dagReq, dagCtx, rows)
	}
	return buildResp(selResp, execDetails, err)
}

func (h coprHandler) buildDAGExecutor(req *coprocessor.Request) (*dagContext, executor, *tipb.DAGRequest, error) {
	if len(req.Ranges) == 0 {
		return nil, nil, nil, errors.New("request range is null")
	}
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	tz, err := timeutil.ConstructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	sctx := flagsAndTzToSessionContext(dagReq.Flags, tz)
	if dagReq.DivPrecisionIncrement != nil {
		sctx.GetSessionVars().DivPrecisionIncrement = int(*dagReq.DivPrecisionIncrement)
	} else {
		sctx.GetSessionVars().DivPrecisionIncrement = vardef.DefDivPrecisionIncrement
	}

	ctx := &dagContext{
		dagReq:    dagReq,
		keyRanges: req.Ranges,
		startTS:   req.StartTs,
		evalCtx:   &evalContext{sctx: sctx},
	}
	var e executor
	if len(dagReq.Executors) == 0 {
		e, err = h.buildDAGForTiFlash(ctx, dagReq.RootExecutor)
	} else {
		e, err = h.buildDAG(ctx, dagReq.Executors)
	}
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return ctx, e, dagReq, err
}

func (h coprHandler) buildExec(ctx *dagContext, curr *tipb.Executor) (executor, *tipb.Executor, error) {
	var currExec executor
	var err error
	var childExec *tipb.Executor
	switch curr.GetTp() {
	case tipb.ExecType_TypeTableScan:
		currExec, err = h.buildTableScan(ctx, curr)
	case tipb.ExecType_TypeIndexScan:
		currExec, err = h.buildIndexScan(ctx, curr)
	case tipb.ExecType_TypeSelection:
		currExec, err = h.buildSelection(ctx, curr)
		childExec = curr.Selection.Child
	case tipb.ExecType_TypeAggregation:
		currExec, err = h.buildHashAgg(ctx, curr)
		childExec = curr.Aggregation.Child
	case tipb.ExecType_TypeStreamAgg:
		currExec, err = h.buildStreamAgg(ctx, curr)
		childExec = curr.Aggregation.Child
	case tipb.ExecType_TypeTopN:
		currExec, err = h.buildTopN(ctx, curr)
		childExec = curr.TopN.Child
	case tipb.ExecType_TypeLimit:
		currExec = &limitExec{limit: curr.Limit.GetLimit(), execDetail: new(execDetail)}
		childExec = curr.Limit.Child
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet", curr.GetTp())
	}

	return currExec, childExec, errors.Trace(err)
}

func (h coprHandler) buildDAGForTiFlash(ctx *dagContext, farther *tipb.Executor) (executor, error) {
	curr, child, err := h.buildExec(ctx, farther)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if child != nil {
		childExec, err := h.buildDAGForTiFlash(ctx, child)
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(childExec)
	}
	return curr, nil
}

func (h coprHandler) buildDAG(ctx *dagContext, executors []*tipb.Executor) (executor, error) {
	var src executor
	for i := range executors {
		curr, _, err := h.buildExec(ctx, executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(src)
		src = curr
	}
	return src, nil
}

func (h coprHandler) buildTableScan(ctx *dagContext, executor *tipb.Executor) (*tableScanExec, error) {
	columns := executor.TblScan.Columns
	ctx.evalCtx.setColumnInfo(columns)
	ranges, err := h.extractKVRanges(ctx.keyRanges, executor.TblScan.Desc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startTS := ctx.startTS
	if startTS == 0 {
		startTS = ctx.dagReq.GetStartTsFallback()
	}
	colInfos := make([]rowcodec.ColInfo, len(columns))
	for i := range colInfos {
		col := columns[i]
		colInfos[i] = rowcodec.ColInfo{
			ID:         col.ColumnId,
			Ft:         ctx.evalCtx.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		}
	}
	defVal := func(i int) ([]byte, error) {
		col := columns[i]
		if col.DefaultVal == nil {
			return nil, nil
		}
		// col.DefaultVal always be  varint `[flag]+[value]`.
		if len(col.DefaultVal) < 1 {
			panic("invalid default value")
		}
		return col.DefaultVal, nil
	}
	rd := rowcodec.NewByteDecoder(colInfos, []int64{-1}, defVal, nil)
	e := &tableScanExec{
		TableScan:      executor.TblScan,
		kvRanges:       ranges,
		colIDs:         ctx.evalCtx.colIDs,
		startTS:        startTS,
		isolationLevel: h.GetIsolationLevel(),
		resolvedLocks:  h.GetResolvedLocks(),
		mvccStore:      h.GetMVCCStore(),
		execDetail:     new(execDetail),
		rd:             rd,
	}

	if ctx.dagReq.CollectRangeCounts != nil && *ctx.dagReq.CollectRangeCounts {
		e.counts = make([]int64, len(ranges))
	}
	return e, nil
}

func (h coprHandler) buildIndexScan(ctx *dagContext, executor *tipb.Executor) (*indexScanExec, error) {
	var err error
	columns := executor.IdxScan.Columns
	ctx.evalCtx.setColumnInfo(columns)
	length := len(columns)
	hdStatus := tablecodec.HandleNotNeeded
	// The PKHandle column info has been collected in ctx.
	if columns[length-1].GetPkHandle() {
		if mysql.HasUnsignedFlag(uint(columns[length-1].GetFlag())) {
			hdStatus = tablecodec.HandleIsUnsigned
		} else {
			hdStatus = tablecodec.HandleDefault
		}
		columns = columns[:length-1]
	} else if columns[length-1].ColumnId == model.ExtraHandleID {
		columns = columns[:length-1]
	}
	ranges, err := h.extractKVRanges(ctx.keyRanges, executor.IdxScan.Desc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startTS := ctx.startTS
	if startTS == 0 {
		startTS = ctx.dagReq.GetStartTsFallback()
	}
	colInfos := make([]rowcodec.ColInfo, 0, len(columns))
	for i := range columns {
		col := columns[i]
		colInfos = append(colInfos, rowcodec.ColInfo{
			ID:         col.ColumnId,
			Ft:         ctx.evalCtx.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		})
	}
	e := &indexScanExec{
		IndexScan:      executor.IdxScan,
		kvRanges:       ranges,
		colsLen:        len(columns),
		startTS:        startTS,
		isolationLevel: h.GetIsolationLevel(),
		resolvedLocks:  h.GetResolvedLocks(),
		mvccStore:      h.GetMVCCStore(),
		hdStatus:       hdStatus,
		execDetail:     new(execDetail),
		colInfos:       colInfos,
	}
	if ctx.dagReq.CollectRangeCounts != nil && *ctx.dagReq.CollectRangeCounts {
		e.counts = make([]int64, len(ranges))
	}
	return e, nil
}

func (h coprHandler) buildSelection(ctx *dagContext, executor *tipb.Executor) (*selectionExec, error) {
	var err error
	var relatedColOffsets []int
	pbConds := executor.Selection.Conditions
	for _, cond := range pbConds {
		relatedColOffsets, err = extractOffsetsInExpr(cond, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	conds, err := convertToExprs(ctx.evalCtx.sctx, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &selectionExec{
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		conditions:        conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h coprHandler) getAggInfo(ctx *dagContext, executor *tipb.Executor) ([]aggregation.Aggregation, []expression.Expression, []int, error) {
	length := len(executor.Aggregation.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	var relatedColOffsets []int
	for _, expr := range executor.Aggregation.AggFunc {
		var aggExpr aggregation.Aggregation
		aggExpr, _, err = aggregation.NewDistAggFunc(expr, ctx.evalCtx.fieldTps, ctx.evalCtx.sctx.GetExprCtx())
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		aggs = append(aggs, aggExpr)
		relatedColOffsets, err = extractOffsetsInExpr(expr, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}
	for _, item := range executor.Aggregation.GroupBy {
		relatedColOffsets, err = extractOffsetsInExpr(item, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}
	groupBys, err := convertToExprs(ctx.evalCtx.sctx, ctx.evalCtx.fieldTps, executor.Aggregation.GetGroupBy())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	return aggs, groupBys, relatedColOffsets, nil
}

func (h coprHandler) buildHashAgg(ctx *dagContext, executor *tipb.Executor) (*hashAggExec, error) {
	aggs, groupBys, relatedColOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &hashAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		groupByExprs:      groupBys,
		groups:            make(map[string]struct{}),
		groupKeys:         make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h coprHandler) buildStreamAgg(ctx *dagContext, executor *tipb.Executor) (*streamAggExec, error) {
	aggs, groupBys, relatedColOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		return nil, errors.Trace(err)
	}
	aggCtxs := make([]*aggregation.AggEvaluateContext, 0, len(aggs))
	for _, agg := range aggs {
		aggCtxs = append(aggCtxs, agg.CreateContext(ctx.evalCtx.sctx.GetExprCtx().GetEvalCtx()))
	}
	groupByCollators := make([]collate.Collator, 0, len(groupBys))
	for _, expr := range groupBys {
		groupByCollators = append(groupByCollators, collate.GetCollator(expr.GetType(ctx.evalCtx.sctx.GetExprCtx().GetEvalCtx()).GetCollate()))
	}

	return &streamAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		aggCtxs:           aggCtxs,
		groupByExprs:      groupBys,
		groupByCollators:  groupByCollators,
		currGroupByValues: make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h coprHandler) buildTopN(ctx *dagContext, executor *tipb.Executor) (*topNExec, error) {
	topN := executor.TopN
	var err error
	var relatedColOffsets []int
	pbConds := make([]*tipb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
		relatedColOffsets, err = extractOffsetsInExpr(item.Expr, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pbConds[i] = item.Expr
	}
	heap := &topNHeap{
		totalCount: int(topN.Limit),
		topNSorter: topNSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.evalCtx.sctx.GetSessionVars().StmtCtx,
		},
	}

	conds, err := convertToExprs(ctx.evalCtx.sctx, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &topNExec{
		heap:              heap,
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		orderByExprs:      conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}
