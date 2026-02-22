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
	"bytes"
	"context"
	"io"
	"slices"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/testutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type evalContext struct {
	colIDs      map[int64]int
	columnInfos []*tipb.ColumnInfo
	fieldTps    []*types.FieldType
	sctx        sessionctx.Context
}

func (e *evalContext) setColumnInfo(cols []*tipb.ColumnInfo) {
	e.columnInfos = slices.Clone(cols)

	e.colIDs = make(map[int64]int, len(e.columnInfos))
	e.fieldTps = make([]*types.FieldType, 0, len(e.columnInfos))
	for i, col := range e.columnInfos {
		ft := fieldTypeFromPBColumn(col)
		e.fieldTps = append(e.fieldTps, ft)
		e.colIDs[col.GetColumnId()] = i
	}
}

// decodeRelatedColumnVals decodes data to Datum slice according to the row information.
func (e *evalContext) decodeRelatedColumnVals(relatedColOffsets []int, value [][]byte, row []types.Datum) error {
	var err error
	for _, offset := range relatedColOffsets {
		row[offset], err = tablecodec.DecodeColumnValue(value[offset], e.fieldTps[offset], e.sctx.GetSessionVars().StmtCtx.TimeZone())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// flagsAndTzToSessionContext creates a StatementContext from a `tipb.SelectRequest.Flags`.
func flagsAndTzToSessionContext(flags uint64, tz *time.Location) sessionctx.Context {
	sctx := mock.NewContextDeprecated()
	sc := stmtctx.NewStmtCtx()
	sc.InitFromPBFlagAndTz(flags, tz)
	sctx.GetSessionVars().StmtCtx = sc
	sctx.GetSessionVars().TimeZone = tz
	return sctx
}

// MockGRPCClientStream is exported for testing purpose.
func MockGRPCClientStream() grpc.ClientStream {
	return mockClientStream{}
}

// mockClientStream implements grpc ClientStream interface, its methods are never called.
type mockClientStream struct{}

// Header implements grpc.ClientStream interface
func (mockClientStream) Header() (metadata.MD, error) { return nil, nil }

// Trailer implements grpc.ClientStream interface
func (mockClientStream) Trailer() metadata.MD { return nil }

// CloseSend implements grpc.ClientStream interface
func (mockClientStream) CloseSend() error { return nil }

// Context implements grpc.ClientStream interface
func (mockClientStream) Context() context.Context { return nil }

// SendMsg implements grpc.ClientStream interface
func (mockClientStream) SendMsg(m any) error { return nil }

// RecvMsg implements grpc.ClientStream interface
func (mockClientStream) RecvMsg(m any) error { return nil }

type mockBathCopErrClient struct {
	mockClientStream

	*errorpb.Error
}

func (mock *mockBathCopErrClient) Recv() (*coprocessor.BatchResponse, error) {
	return &coprocessor.BatchResponse{
		OtherError: mock.Error.Message,
	}, nil
}

type mockBatchCopDataClient struct {
	mockClientStream

	chunks []tipb.Chunk
	idx    int
}

func (mock *mockBatchCopDataClient) Recv() (*coprocessor.BatchResponse, error) {
	if mock.idx < len(mock.chunks) {
		res := tipb.SelectResponse{
			Chunks: []tipb.Chunk{mock.chunks[mock.idx]},
		}
		raw, err := res.Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		mock.idx++
		return &coprocessor.BatchResponse{
			Data: raw,
		}, nil
	}
	return nil, io.EOF
}

func (h coprHandler) initSelectResponse(err error, warnings []contextutil.SQLWarn, counts []int64) *tipb.SelectResponse {
	selResp := &tipb.SelectResponse{
		Error:        toPBError(err),
		OutputCounts: counts,
	}
	for i := range warnings {
		selResp.Warnings = append(selResp.Warnings, toPBError(warnings[i].Err))
	}
	return selResp
}

func (h coprHandler) fillUpData4SelectResponse(selResp *tipb.SelectResponse, dagReq *tipb.DAGRequest, dagCtx *dagContext, rows [][][]byte) error {
	switch dagReq.EncodeType {
	case tipb.EncodeType_TypeDefault:
		h.encodeDefault(selResp, rows, dagReq.OutputOffsets)
	case tipb.EncodeType_TypeChunk:
		colTypes := h.constructRespSchema(dagCtx)
		loc := dagCtx.evalCtx.sctx.GetSessionVars().StmtCtx.TimeZone()
		err := h.encodeChunk(selResp, rows, colTypes, dagReq.OutputOffsets, loc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h coprHandler) constructRespSchema(dagCtx *dagContext) []*types.FieldType {
	var root *tipb.Executor
	if len(dagCtx.dagReq.Executors) == 0 {
		root = dagCtx.dagReq.RootExecutor
	} else {
		root = dagCtx.dagReq.Executors[len(dagCtx.dagReq.Executors)-1]
	}
	agg := root.Aggregation
	if agg == nil {
		return dagCtx.evalCtx.fieldTps
	}

	schema := make([]*types.FieldType, 0, len(agg.AggFunc)+len(agg.GroupBy))
	for i := range agg.AggFunc {
		if agg.AggFunc[i].Tp == tipb.ExprType_Avg {
			// Avg function requests two columns : Count , Sum
			// This line addend the Count(TypeLonglong) to the schema.
			schema = append(schema, types.NewFieldType(mysql.TypeLonglong))
		}
		schema = append(schema, expression.PbTypeToFieldType(agg.AggFunc[i].FieldType))
	}
	for i := range agg.GroupBy {
		schema = append(schema, expression.PbTypeToFieldType(agg.GroupBy[i].FieldType))
	}
	return schema
}

func (h coprHandler) encodeDefault(selResp *tipb.SelectResponse, rows [][][]byte, colOrdinal []uint32) {
	var chunks []tipb.Chunk
	for i := range rows {
		requestedRow := dummySlice
		for _, ordinal := range colOrdinal {
			requestedRow = append(requestedRow, rows[i][ordinal]...)
		}
		chunks = appendRow(chunks, requestedRow, i)
	}
	selResp.Chunks = chunks
	selResp.EncodeType = tipb.EncodeType_TypeDefault
}

func (h coprHandler) encodeChunk(selResp *tipb.SelectResponse, rows [][][]byte, colTypes []*types.FieldType, colOrdinal []uint32, loc *time.Location) error {
	var chunks []tipb.Chunk
	respColTypes := make([]*types.FieldType, 0, len(colOrdinal))
	for _, ordinal := range colOrdinal {
		respColTypes = append(respColTypes, colTypes[ordinal])
	}
	chk := chunk.NewChunkWithCapacity(respColTypes, rowsPerChunk)
	encoder := chunk.NewCodec(respColTypes)
	decoder := codec.NewDecoder(chk, loc)
	for i := range rows {
		for j, ordinal := range colOrdinal {
			_, err := decoder.DecodeOne(rows[i][ordinal], j, colTypes[ordinal])
			if err != nil {
				return err
			}
		}
		if i%rowsPerChunk == rowsPerChunk-1 {
			chunks = append(chunks, tipb.Chunk{})
			cur := &chunks[len(chunks)-1]
			cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
			chk.Reset()
		}
	}
	if chk.NumRows() > 0 {
		chunks = append(chunks, tipb.Chunk{})
		cur := &chunks[len(chunks)-1]
		cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
		chk.Reset()
	}
	selResp.Chunks = chunks
	selResp.EncodeType = tipb.EncodeType_TypeChunk
	return nil
}

func buildResp(selResp *tipb.SelectResponse, execDetails []*execDetail, err error) *coprocessor.Response {
	resp := &coprocessor.Response{}

	if len(execDetails) > 0 {
		execSummary := make([]*tipb.ExecutorExecutionSummary, 0, len(execDetails))
		for _, d := range execDetails {
			costNs := uint64(d.timeProcessed / time.Nanosecond)
			rows := uint64(d.numProducedRows)
			numIter := uint64(d.numIterations)
			execSummary = append(execSummary, &tipb.ExecutorExecutionSummary{
				TimeProcessedNs: &costNs,
				NumProducedRows: &rows,
				NumIterations:   &numIter,
			})
		}
		selResp.ExecutionSummaries = execSummary
	}

	// Select errors have been contained in `SelectResponse.Error`
	if locked, ok := errors.Cause(err).(*testutils.ErrLocked); ok {
		resp.Locked = &kvrpcpb.LockInfo{
			Key:         locked.Key,
			PrimaryLock: locked.Primary,
			LockVersion: locked.StartTS,
			LockTtl:     locked.TTL,
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
	switch x := err.(type) {
	case *terror.Error:
		sqlErr := terror.ToSQLError(x)
		perr.Code = int32(sqlErr.Code)
		perr.Msg = sqlErr.Message
	default:
		e := errors.Cause(err)
		switch y := e.(type) {
		case *terror.Error:
			tmp := terror.ToSQLError(y)
			perr.Code = int32(tmp.Code)
			perr.Msg = tmp.Message
		default:
			perr.Code = int32(1)
			perr.Msg = err.Error()
		}
	}
	return perr
}

// extractKVRanges extracts kv.KeyRanges slice from a SelectRequest.
func (h coprHandler) extractKVRanges(keyRanges []*coprocessor.KeyRange, descScan bool) (kvRanges []kv.KeyRange, err error) {
	for _, kran := range keyRanges {
		if bytes.Compare(kran.GetStart(), kran.GetEnd()) >= 0 {
			err = errors.Errorf("invalid range, start should be smaller than end: %v %v", kran.GetStart(), kran.GetEnd())
			return
		}

		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, h.GetRawStartKey()) <= 0 {
			continue
		}
		lowerKey := kran.GetStart()
		if len(h.GetRawEndKey()) != 0 && bytes.Compare(lowerKey, h.GetRawEndKey()) >= 0 {
			break
		}
		var kvr kv.KeyRange
		kvr.StartKey = maxStartKey(lowerKey, h.GetRawStartKey())
		kvr.EndKey = minEndKey(upperKey, h.GetRawEndKey())
		kvRanges = append(kvRanges, kvr)
	}
	if descScan {
		reverseKVRanges(kvRanges)
	}
	return
}

func reverseKVRanges(kvRanges []kv.KeyRange) {
	for i := range len(kvRanges) / 2 {
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
	if bytes.Compare(rangeStartKey, regionStartKey) > 0 {
		return rangeStartKey
	}
	return regionStartKey
}

func minEndKey(rangeEndKey kv.Key, regionEndKey []byte) []byte {
	if len(regionEndKey) == 0 || bytes.Compare(rangeEndKey, regionEndKey) < 0 {
		return rangeEndKey
	}
	return regionEndKey
}

func isDuplicated(offsets []int, offset int) bool {
	return slices.Contains(offsets, offset)
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

// fieldTypeFromPBColumn creates a types.FieldType from tipb.ColumnInfo.
func fieldTypeFromPBColumn(col *tipb.ColumnInfo) *types.FieldType {
	charsetStr, collationStr, _ := charset.GetCharsetInfoByID(int(collate.RestoreCollationIDIfNeeded(col.GetCollation())))
	ft := &types.FieldType{}
	ft.SetType(byte(col.GetTp()))
	ft.SetFlag(uint(col.GetFlag()))
	ft.SetFlen(int(col.GetColumnLen()))
	ft.SetDecimal(int(col.GetDecimal()))
	ft.SetElems(col.Elems)
	ft.SetCharset(charsetStr)
	ft.SetCollate(collationStr)
	return ft
}
