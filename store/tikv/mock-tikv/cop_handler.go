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
	"bytes"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

type dagContext struct {
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	evalCtx   *evalContext
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
	sc := flagsToStatementContext(dagReq.Flags)
	ctx := &dagContext{
		dagReq:    dagReq,
		keyRanges: req.Ranges,
		evalCtx:   &evalContext{sc: sc},
	}
	e, err := h.buildDAG(ctx, dagReq.Executors)
	if err != nil {
		return nil, errors.Trace(err)
	}
	data := make([]byte, 0)
	var (
		handle int64
		row    [][]byte
		chunks []tipb.Chunk
	)
	for {
		handle, row, err = e.Next()
		if err != nil {
			// Wrap this error into response.
			break
		}
		if row == nil {
			break
		}
		data = data[:0]
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
	ctx.evalCtx.setColumnInfo(columns)
	ranges := h.extractKVRanges(ctx.keyRanges, *executor.TblScan.Desc)

	return &tableScanExec{
		TableScan:   executor.TblScan,
		kvRanges:    ranges,
		colIDs:      ctx.evalCtx.colIDs,
		startTS:     ctx.dagReq.GetStartTs(),
		mvccStore:   h.mvccStore,
		rawStartKey: h.rawStartKey,
		rawEndKey:   h.rawEndKey,
	}
}

func (h *rpcHandler) buildIndexScan(ctx *dagContext, executor *tipb.Executor) *indexScanExec {
	columns := executor.IdxScan.Columns
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
	var err error
	var relatedColOffsets []int
	pbConds := executor.Selection.Conditions
	for _, cond := range pbConds {
		relatedColOffsets, err = extractOffsetsInExpr(cond, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.colIDs, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &selectionExec{
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		conditions:        conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
	}, nil
}

func (h *rpcHandler) buildAggregation(ctx *dagContext, executor *tipb.Executor) (*aggregateExec, error) {
	length := len(executor.Aggregation.AggFunc)
	aggs := make([]expression.AggregationFunction, 0, length)
	var err error
	var relatedColOffsets []int
	for _, expr := range executor.Aggregation.AggFunc {
		aggExpr, err := expression.NewDistAggFunc(expr, ctx.evalCtx.colIDs, ctx.evalCtx.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		aggs = append(aggs, aggExpr)
		relatedColOffsets, err = extractOffsetsInExpr(expr, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for _, item := range executor.Aggregation.GroupBy {
		relatedColOffsets, err = extractOffsetsInExpr(item, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	groupBys, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.colIDs, executor.Aggregation.GetGroupBy())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &aggregateExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		groupByExprs:      groupBys,
		groups:            make(map[string]struct{}),
		groupKeys:         make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
	}, nil
}

func (h *rpcHandler) buildTopN(ctx *dagContext, executor *tipb.Executor) (*topNExec, error) {
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
	heap := &topnHeap{
		totalCount: int(*topN.Limit),
		topnSorter: topnSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.evalCtx.sc,
		},
	}

	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.colIDs, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &topNExec{
		heap:              heap,
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		orderByExprs:      conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
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

func buildResp(chunks []tipb.Chunk, err error) (*coprocessor.Response, error) {
	resp := &coprocessor.Response{}
	selResp := &tipb.SelectResponse{
		Error:  toPBError(err),
		Chunks: chunks,
	}
	if err != nil {
		if locked, ok := errors.Cause(err).(*ErrLocked); ok {
			resp.Locked = &kvrpcpb.LockInfo{
				Key:         locked.Key,
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
			}
		} else {
			resp.OtherError = err.Error()
		}
	}
	data, err := proto.Marshal(selResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp.Data = data
	return resp, nil
}

const rowsPerChunk = 64

func appendRow(chunks []tipb.Chunk, handle int64, data []byte) []tipb.Chunk {
	if len(chunks) == 0 || len(chunks[len(chunks)-1].RowsMeta) >= rowsPerChunk {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsMeta = append(cur.RowsMeta, tipb.RowMeta{Handle: handle, Length: int64(len(data))})
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

func maxStartKey(rangeStartKey kv.Key, regionStartKey []byte) []byte {
	if bytes.Compare([]byte(rangeStartKey), regionStartKey) > 0 {
		return []byte(rangeStartKey)
	}
	return regionStartKey
}

func minEndKey(rangeEndKey kv.Key, regionEndKey []byte) []byte {
	if len(regionEndKey) == 0 || bytes.Compare([]byte(rangeEndKey), regionEndKey) < 0 {
		return []byte(rangeEndKey)
	}
	return regionEndKey
}

func toPBError(err error) *tipb.Error {
	if err == nil {
		return nil
	}
	perr := new(tipb.Error)
	perr.Code = int32(1)
	errStr := err.Error()
	perr.Msg = errStr
	return perr
}

// extractKVRanges extracts kv.KeyRanges slice from a SelectRequest.
func (h *rpcHandler) extractKVRanges(keyRanges []*coprocessor.KeyRange, descScan bool) (kvRanges []kv.KeyRange) {
	for _, kran := range keyRanges {
		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, h.rawStartKey) <= 0 {
			continue
		}
		lowerKey := kran.GetStart()
		if len(h.rawEndKey) != 0 && bytes.Compare([]byte(lowerKey), h.rawEndKey) >= 0 {
			break
		}
		var kvr kv.KeyRange
		kvr.StartKey = kv.Key(maxStartKey(lowerKey, h.rawStartKey))
		kvr.EndKey = kv.Key(minEndKey(upperKey, h.rawEndKey))
		kvRanges = append(kvRanges, kvr)
	}
	if descScan {
		reverseKVRanges(kvRanges)
	}
	return
}

func reverseKVRanges(kvRanges []kv.KeyRange) {
	for i := 0; i < len(kvRanges)/2; i++ {
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

func convertToExprs(sc *variable.StatementContext, colIDs map[int64]int, pbExprs []*tipb.Expr) ([]expression.Expression, error) {
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := expression.PBToExpr(expr, colIDs, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

// Flags are used by tipb.SelectRequest.Flags to handle execution mode, like how to handle truncate error.
const (
	// FlagIgnoreTruncate indicates if truncate error should be ignored.
	// Read-only statements should ignore truncate error, write statements should not ignore truncate error.
	FlagIgnoreTruncate uint64 = 1
	// FlagTruncateAsWarning indicates if truncate error should be returned as warning.
	// This flag only matters if FlagIgnoreTruncate is not set, in strict sql mode, truncate error should
	// be returned as error, in non-strict sql mode, truncate error should be saved as warning.
	FlagTruncateAsWarning uint64 = 1 << 1
)

// flagsToStatementContext creates a StatementContext from a `tipb.SelectRequest.Flags`.
func flagsToStatementContext(flags uint64) *variable.StatementContext {
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = (flags & FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & FlagTruncateAsWarning) > 0
	return sc
}
