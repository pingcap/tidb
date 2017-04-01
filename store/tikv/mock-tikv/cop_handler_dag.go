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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tipb/go-tipb"
)

// MockDAGRequest is used for testing now.
var MockDAGRequest bool

type dagContext struct {
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	eval      *xeval.Evaluator
	sc        *variable.StatementContext
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
		sc:        sc,
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
	switch curr.GetTp() {
	case tipb.ExecType_TypeTableScan:
		columns := curr.TblScan.Columns
		ctx.eval.SetColumnInfos(columns)
		colIDs := make(map[int64]int)
		for i, col := range columns {
			colIDs[col.GetColumnId()] = i
		}
		ranges := h.extractKVRanges(ctx.keyRanges, *curr.TblScan.Desc)
		currExec = &tableScanExec{
			TableScan:   curr.TblScan,
			kvRanges:    ranges,
			colsID:      colIDs,
			startTS:     ctx.dagReq.GetStartTs(),
			mvccStore:   h.mvccStore,
			rawStartKey: h.rawStartKey,
			rawEndKey:   h.rawEndKey,
		}
	case tipb.ExecType_TypeIndexScan:
		columns := curr.IdxScan.Columns
		ctx.eval.SetColumnInfos(columns)
		length := len(columns)
		// The PKHandle column info has been collected in ctx.
		if columns[length-1].GetPkHandle() {
			columns = columns[:length-1]
		}
		ranges := h.extractKVRanges(ctx.keyRanges, *curr.IdxScan.Desc)
		currExec = &indexScanExec{
			IndexScan:   curr.IdxScan,
			kvRanges:    ranges,
			colsLen:     len(columns),
			startTS:     ctx.dagReq.GetStartTs(),
			mvccStore:   h.mvccStore,
			rawStartKey: h.rawStartKey,
			rawEndKey:   h.rawEndKey,
		}
	case tipb.ExecType_TypeSelection:
		var cond *tipb.Expr
		// TODO: Now len(Conditions) is 1.
		if len(curr.Selection.Conditions) > 0 {
			cond = curr.Selection.Conditions[0]
		}
		colIDs := make(map[int64]int)
		err := extractColIDsInExpr(cond, ctx.eval.ColumnInfos, colIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		currExec = &selectionExec{
			Selection: curr.Selection,
			eval:      ctx.eval,
			colsID:    colIDs,
			sc:        ctx.sc,
		}
	default:
		// TODO: Support other types.
	}

	return currExec, nil
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
