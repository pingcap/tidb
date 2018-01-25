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
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

var dummySlice = make([]byte, 0)

type dagContext struct {
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	evalCtx   *evalContext
}

func (h *rpcHandler) handleCopDAGRequest(req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	if err := h.checkRequestContext(req.GetContext()); err != nil {
		resp.RegionError = err
		return resp
	}
	e, dagReq, err := h.buildDAGExecutor(req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}

	var (
		chunks []tipb.Chunk
		rowCnt int
	)
	goCtx := goctx.TODO()
	for {
		var row [][]byte
		row, err = e.Next(goCtx)
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		data := dummySlice
		for _, offset := range dagReq.OutputOffsets {
			data = append(data, row[offset]...)
		}
		chunks = appendRow(chunks, data, rowCnt)
		rowCnt++
	}
	counts := make([]int64, len(dagReq.Executors))
	for offset := len(dagReq.Executors) - 1; e != nil; e, offset = e.GetSrcExec(), offset-1 {
		// Because the last call to `executor.Next` always returns a `nil`, so the actual count should be `Count - 1`
		counts[offset] = e.Count() - 1
	}
	return buildResp(chunks, counts, err)
}

func (h *rpcHandler) buildDAGExecutor(req *coprocessor.Request) (executor, *tipb.DAGRequest, error) {
	if len(req.Ranges) == 0 {
		return nil, nil, errors.New("request range is null")
	}
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	sc := flagsToStatementContext(dagReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(dagReq.TimeZoneOffset))
	ctx := &dagContext{
		dagReq:    dagReq,
		keyRanges: req.Ranges,
		evalCtx:   &evalContext{sc: sc},
	}
	e, err := h.buildDAG(ctx, dagReq.Executors)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return e, dagReq, err
}

func (h *rpcHandler) handleCopStream(req *coprocessor.Request) (tikvpb.Tikv_CoprocessorStreamClient, error) {
	e, dagReq, err := h.buildDAGExecutor(req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mockCopStreamClient{
		exec:  e,
		req:   dagReq,
		goCtx: goctx.TODO(),
	}, nil
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
		currExec, err = h.buildHashAgg(ctx, curr)
	case tipb.ExecType_TypeStreamAgg:
		currExec, err = h.buildStreamAgg(ctx, curr)
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
	ranges := h.extractKVRanges(ctx.keyRanges, executor.TblScan.Desc)

	return &tableScanExec{
		TableScan:      executor.TblScan,
		kvRanges:       ranges,
		colIDs:         ctx.evalCtx.colIDs,
		startTS:        ctx.dagReq.GetStartTs(),
		isolationLevel: h.isolationLevel,
		mvccStore:      h.mvccStore,
	}
}

func (h *rpcHandler) buildIndexScan(ctx *dagContext, executor *tipb.Executor) *indexScanExec {
	columns := executor.IdxScan.Columns
	ctx.evalCtx.setColumnInfo(columns)
	length := len(columns)
	pkStatus := pkColNotExists
	// The PKHandle column info has been collected in ctx.
	if columns[length-1].GetPkHandle() {
		if mysql.HasUnsignedFlag(uint(columns[length-1].GetFlag())) {
			pkStatus = pkColIsUnsigned
		} else {
			pkStatus = pkColIsSigned
		}
		columns = columns[:length-1]
	} else if columns[length-1].ColumnId == model.ExtraHandleID {
		pkStatus = pkColIsSigned
		columns = columns[:length-1]
	}
	ranges := h.extractKVRanges(ctx.keyRanges, executor.IdxScan.Desc)

	return &indexScanExec{
		IndexScan:      executor.IdxScan,
		kvRanges:       ranges,
		colsLen:        len(columns),
		startTS:        ctx.dagReq.GetStartTs(),
		isolationLevel: h.isolationLevel,
		mvccStore:      h.mvccStore,
		pkStatus:       pkStatus,
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
	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbConds)
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

func (h *rpcHandler) getAggInfo(ctx *dagContext, executor *tipb.Executor) ([]aggregation.Aggregation, []expression.Expression, []int, error) {
	length := len(executor.Aggregation.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	var relatedColOffsets []int
	for _, expr := range executor.Aggregation.AggFunc {
		var aggExpr aggregation.Aggregation
		aggExpr, err = aggregation.NewDistAggFunc(expr, ctx.evalCtx.fieldTps, ctx.evalCtx.sc)
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
	groupBys, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, executor.Aggregation.GetGroupBy())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	return aggs, groupBys, relatedColOffsets, nil
}

func (h *rpcHandler) buildHashAgg(ctx *dagContext, executor *tipb.Executor) (*hashAggExec, error) {
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
	}, nil
}

func (h *rpcHandler) buildStreamAgg(ctx *dagContext, executor *tipb.Executor) (*streamAggExec, error) {
	aggs, groupBys, relatedColOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		return nil, errors.Trace(err)
	}
	aggCtxs := make([]*aggregation.AggEvaluateContext, 0, len(aggs))
	for _, agg := range aggs {
		aggCtxs = append(aggCtxs, agg.CreateContext(ctx.evalCtx.sc))
	}

	return &streamAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		aggCtxs:           aggCtxs,
		groupByExprs:      groupBys,
		currGroupByValues: make([][]byte, 0),
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
	heap := &topNHeap{
		totalCount: int(topN.Limit),
		topNSorter: topNSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.evalCtx.sc,
		},
	}

	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbConds)
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
	sc          *stmtctx.StatementContext
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
func (e *evalContext) decodeRelatedColumnVals(relatedColOffsets []int, value [][]byte, row []types.Datum) error {
	var err error
	for _, offset := range relatedColOffsets {
		row[offset], err = tablecodec.DecodeColumnValue(value[offset], e.fieldTps[offset], e.sc.TimeZone)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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

	// FlagPadCharToFullLength indicates if sql_mode 'PAD_CHAR_TO_FULL_LENGTH' is set.
	FlagPadCharToFullLength uint64 = 1 << 2
)

// flagsToStatementContext creates a StatementContext from a `tipb.SelectRequest.Flags`.
func flagsToStatementContext(flags uint64) *stmtctx.StatementContext {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = (flags & FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & FlagTruncateAsWarning) > 0
	sc.PadCharToFullLength = (flags & FlagPadCharToFullLength) > 0
	return sc
}

// mockClientStream implements grpc ClientStream interface, its methods are never called.
type mockClientStream struct{}

func (mockClientStream) Header() (metadata.MD, error) { return nil, nil }
func (mockClientStream) Trailer() metadata.MD         { return nil }
func (mockClientStream) CloseSend() error             { return nil }
func (mockClientStream) Context() goctx.Context       { return nil }
func (mockClientStream) SendMsg(m interface{}) error  { return nil }
func (mockClientStream) RecvMsg(m interface{}) error  { return nil }

type mockCopStreamClient struct {
	mockClientStream

	req      *tipb.DAGRequest
	exec     executor
	goCtx    goctx.Context
	finished bool
}

type mockCopStreamErrClient struct {
	mockClientStream

	*errorpb.Error
}

func (mock *mockCopStreamErrClient) Recv() (*coprocessor.Response, error) {
	return &coprocessor.Response{
		RegionError: mock.Error,
	}, nil
}

func (mock *mockCopStreamClient) Recv() (*coprocessor.Response, error) {
	if mock.finished {
		return nil, io.EOF
	}

	var resp coprocessor.Response
	chunk, finish, err := mock.readBlockFromExecutor()
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
		return &resp, nil
	}
	if finish {
		// Just mark it, need to handle the last chunk.
		mock.finished = true
	}

	data, err := chunk.Marshal()
	if err != nil {
		resp.OtherError = err.Error()
		return &resp, nil
	}
	streamResponse := tipb.StreamResponse{
		Error:      toPBError(err),
		EncodeType: tipb.EncodeType_TypeDefault,
		Data:       data,
	}
	resp.Data, err = proto.Marshal(&streamResponse)
	if err != nil {
		resp.OtherError = err.Error()
	}
	return &resp, nil
}

func (mock *mockCopStreamClient) readBlockFromExecutor() (tipb.Chunk, bool, error) {
	var chunk tipb.Chunk
	for count := 0; count < rowsPerChunk; count++ {
		row, err := mock.exec.Next(mock.goCtx)
		if err != nil {
			return chunk, false, errors.Trace(err)
		}
		if row == nil {
			return chunk, true, nil
		}
		for _, offset := range mock.req.OutputOffsets {
			chunk.RowsData = append(chunk.RowsData, row[offset]...)
		}
	}
	return chunk, false, nil
}

func buildResp(chunks []tipb.Chunk, counts []int64, err error) *coprocessor.Response {
	resp := &coprocessor.Response{}
	selResp := &tipb.SelectResponse{
		Error:        toPBError(err),
		Chunks:       chunks,
		OutputCounts: counts,
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
		resp.OtherError = err.Error()
		return resp
	}
	resp.Data = data
	return resp
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
		if len(h.rawEndKey) != 0 && bytes.Compare(lowerKey, h.rawEndKey) >= 0 {
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

const rowsPerChunk = 64

func appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
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

func isDuplicated(offsets []int, offset int) bool {
	for _, idx := range offsets {
		if idx == offset {
			return true
		}
	}
	return false
}

func extractOffsetsInExpr(expr *tipb.Expr, columns []*tipb.ColumnInfo, collector []int) ([]int, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		_, idx, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !isDuplicated(collector, int(idx)) {
			collector = append(collector, int(idx))
		}
		return collector, nil
	}
	var err error
	for _, child := range expr.Children {
		collector, err = extractOffsetsInExpr(child, columns, collector)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return collector, nil
}
