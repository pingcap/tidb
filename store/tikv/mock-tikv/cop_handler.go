// Copyright 2016 PingCAP, Inc.
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
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/distsql/xeval"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

type selectContext struct {
	sel          *tipb.SelectRequest
	eval         *xeval.Evaluator
	whereColumns map[int64]*tipb.ColumnInfo
	aggColumns   map[int64]*tipb.ColumnInfo
	groups       map[string]bool
	groupKeys    [][]byte
	aggregates   []*aggregateFuncExpr
	aggregate    bool
	keyRanges    []*coprocessor.KeyRange

	// Use for DecodeRow.
	colTps map[int64]*types.FieldType
}

func (h *rpcHandler) handleCopRequest(req *coprocessor.Request) (*coprocessor.Response, error) {
	resp := &coprocessor.Response{}
	if err := h.checkContext(req.GetContext()); err != nil {
		resp.RegionError = err
		return resp, nil
	}
	if len(req.Ranges) == 0 {
		return resp, nil
	}
	if req.GetTp() == kv.ReqTypeSelect || req.GetTp() == kv.ReqTypeIndex {
		sel := new(tipb.SelectRequest)
		err := proto.Unmarshal(req.Data, sel)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ctx := &selectContext{
			sel:       sel,
			keyRanges: req.Ranges,
		}
		ctx.eval = &xeval.Evaluator{Row: make(map[int64]types.Datum)}
		if sel.Where != nil {
			ctx.whereColumns = make(map[int64]*tipb.ColumnInfo)
			collectColumnsInExpr(sel.Where, ctx, ctx.whereColumns)
		}
		ctx.aggregate = len(sel.Aggregates) > 0 || len(sel.GetGroupBy()) > 0
		if ctx.aggregate {
			// compose aggregateFuncExpr
			ctx.aggregates = make([]*aggregateFuncExpr, 0, len(sel.Aggregates))
			ctx.aggColumns = make(map[int64]*tipb.ColumnInfo)
			for _, agg := range sel.Aggregates {
				aggExpr := &aggregateFuncExpr{expr: agg}
				ctx.aggregates = append(ctx.aggregates, aggExpr)
				collectColumnsInExpr(agg, ctx, ctx.aggColumns)
			}
			ctx.groups = make(map[string]bool)
			ctx.groupKeys = make([][]byte, 0)
			for _, item := range ctx.sel.GetGroupBy() {
				collectColumnsInExpr(item.Expr, ctx, ctx.aggColumns)
			}
			for k := range ctx.whereColumns {
				// It is will be handled in where.
				delete(ctx.aggColumns, k)
			}
		}

		var chunks []tipb.Chunk
		if req.GetTp() == kv.ReqTypeSelect {
			chunks, err = h.getChunksFromSelectReq(ctx)
		} else {
			// The PKHandle column info has been collected in ctx, so we can remove it in IndexInfo.
			length := len(sel.IndexInfo.Columns)
			if sel.IndexInfo.Columns[length-1].GetPkHandle() {
				sel.IndexInfo.Columns = sel.IndexInfo.Columns[:length-1]
			}
			chunks, err = h.getChunksFromIndexReq(ctx)
		}

		selResp := new(tipb.SelectResponse)
		selResp.Error = toPBError(err)
		selResp.Chunks = chunks
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
	}
	return resp, nil
}

func (h *rpcHandler) getRowsFromAgg(ctx *selectContext) ([]tipb.Chunk, error) {
	chunks := make([]tipb.Chunk, 0, len(ctx.groupKeys)/rowsPerChunk+1)
	for _, gk := range ctx.groupKeys {
		// Each aggregate partial result will be converted to one or two datums.
		rowData := make([]types.Datum, 0, 1+2*len(ctx.aggregates))
		// The first column is group key.
		rowData = append(rowData, types.NewBytesDatum(gk))
		for _, agg := range ctx.aggregates {
			agg.currentGroup = gk
			ds, err := agg.toDatums()
			if err != nil {
				return nil, errors.Trace(err)
			}
			rowData = append(rowData, ds...)
		}
		row, err := codec.EncodeValue(nil, rowData...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chunks = appendRow(chunks, 0, row)
	}
	return chunks, nil
}

func collectColumnsInExpr(expr *tipb.Expr, ctx *selectContext, collector map[int64]*tipb.ColumnInfo) error {
	if expr == nil {
		return nil
	}
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		_, i, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return errors.Trace(err)
		}
		var columns []*tipb.ColumnInfo
		if ctx.sel.TableInfo != nil {
			columns = ctx.sel.TableInfo.Columns
		} else {
			columns = ctx.sel.IndexInfo.Columns
		}
		for _, c := range columns {
			if c.GetColumnId() == i {
				collector[i] = c
				return nil
			}
		}
		return xeval.ErrInvalid.Gen("column %d not found", i)
	}
	for _, child := range expr.Children {
		err := collectColumnsInExpr(child, ctx, collector)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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

func (h *rpcHandler) getChunksFromSelectReq(ctx *selectContext) ([]tipb.Chunk, error) {
	// Init ctx.colTps and use it to decode all the rows.
	columns := ctx.sel.TableInfo.Columns
	ctx.colTps = make(map[int64]*types.FieldType, len(columns))
	for _, col := range columns {
		if col.GetPkHandle() {
			continue
		}
		ctx.colTps[col.GetColumnId()] = distsql.FieldTypeFromPBColumn(col)
	}

	kvRanges, desc := h.extractKVRanges(ctx)
	limit := int64(-1)
	if ctx.sel.Limit != nil {
		limit = ctx.sel.GetLimit()
	}

	var chunks []tipb.Chunk
	var err error
	for _, ran := range kvRanges {
		if limit == 0 {
			break
		}
		chunks, err = h.getRowsFromRange(ctx, ran, &limit, desc, chunks)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if ctx.aggregate {
		return h.getRowsFromAgg(ctx)
	}
	return chunks, nil
}

// extractKVRanges extracts kv.KeyRanges slice from a SelectRequest, and also returns if it is in descending order.
func (h *rpcHandler) extractKVRanges(ctx *selectContext) (kvRanges []kv.KeyRange, desc bool) {
	sel := ctx.sel
	for _, kran := range ctx.keyRanges {
		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, h.startKey) <= 0 {
			continue
		}
		lowerKey := kran.GetStart()
		if len(h.endKey) != 0 && bytes.Compare([]byte(lowerKey), h.endKey) >= 0 {
			break
		}
		var kvr kv.KeyRange
		kvr.StartKey = kv.Key(maxStartKey(lowerKey, h.startKey))
		kvr.EndKey = kv.Key(minEndKey(upperKey, h.endKey))
		kvRanges = append(kvRanges, kvr)
	}
	if sel.OrderBy != nil {
		desc = sel.OrderBy[0].Desc
	}
	if desc {
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

func (h *rpcHandler) getRowsFromRange(ctx *selectContext, ran kv.KeyRange, limit *int64, desc bool, chunks []tipb.Chunk) ([]tipb.Chunk, error) {
	startKey := maxStartKey(ran.StartKey, h.startKey)
	endKey := minEndKey(ran.EndKey, h.endKey)
	if (*limit) == 0 || bytes.Compare(startKey, endKey) >= 0 {
		return chunks, nil
	}

	if ran.IsPoint() {
		val, err := h.mvccStore.Get(startKey, ctx.sel.GetStartTs())
		if len(val) == 0 {
			return chunks, nil
		} else if err != nil {
			return nil, errors.Trace(err)
		}
		handle, err := tablecodec.DecodeRowKey(kv.Key(startKey))
		if err != nil {
			return nil, errors.Trace(err)
		}
		row, err := h.handleRowData(ctx, handle, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			chunks = appendRow(chunks, handle, row)
		}
		return chunks, nil
	}
	var seekKey []byte
	if desc {
		seekKey = endKey
	} else {
		seekKey = startKey
	}
	for {
		if (*limit) == 0 {
			break
		}
		var (
			pairs []Pair
			pair  Pair
			err   error
		)
		if desc {
			pairs = h.mvccStore.ReverseScan(startKey, seekKey, 1, ctx.sel.GetStartTs())
		} else {
			pairs = h.mvccStore.Scan(seekKey, endKey, 1, ctx.sel.GetStartTs())
		}
		if len(pairs) > 0 {
			pair = pairs[0]
		}
		if pair.Err != nil {
			// TODO: handle lock error.
			return nil, errors.Trace(pair.Err)
		}
		if pair.Key == nil {
			break
		}
		if desc {
			if bytes.Compare(pair.Key, startKey) < 0 {
				break
			}
			seekKey = []byte(tablecodec.TruncateToRowKeyLen(kv.Key(pair.Key)))
		} else {
			if bytes.Compare(pair.Key, endKey) >= 0 {
				break
			}
			seekKey = []byte(kv.Key(pair.Key).PrefixNext())
		}
		handle, err := tablecodec.DecodeRowKey(pair.Key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row, err := h.handleRowData(ctx, handle, pair.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			chunks = appendRow(chunks, handle, row)
			(*limit)--
		}
	}
	return chunks, nil
}

// handleRowData deals with raw row data:
//	1. Decodes row from raw byte slice.
//	2. Checks if it fit where condition.
//	3. Update aggregate functions.
func (h *rpcHandler) handleRowData(ctx *selectContext, handle int64, value []byte) ([]byte, error) {
	columns := ctx.sel.TableInfo.Columns
	values, err := h.getRowData(value, ctx.colTps)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Fill handle and null columns.
	for _, col := range columns {
		if col.GetPkHandle() {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				// PK column is Unsigned
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(nil, handleDatum)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[col.GetColumnId()] = handleData
		} else {
			_, ok := values[col.GetColumnId()]
			if !ok {
				if mysql.HasNotNullFlag(uint(col.GetFlag())) {
					return nil, errors.New("Miss column")
				}
				values[col.GetColumnId()] = []byte{codec.NilFlag}
			}
		}
	}
	return h.valuesToRow(ctx, handle, values)
}

var dummySlice = make([]byte, 0)

func (h *rpcHandler) valuesToRow(ctx *selectContext, handle int64, values map[int64][]byte) ([]byte, error) {
	var columns []*tipb.ColumnInfo
	if ctx.sel.TableInfo != nil {
		columns = ctx.sel.TableInfo.Columns
	} else {
		columns = ctx.sel.IndexInfo.Columns
	}
	// Evaluate where
	match, err := h.evalWhereForRow(ctx, handle, values)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !match {
		return nil, nil
	}
	data := dummySlice
	if ctx.aggregate {
		// Update aggregate functions.
		err = h.aggregate(ctx, handle, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		// If without aggregate functions, just return raw row data.
		for _, col := range columns {
			data = append(data, values[col.GetColumnId()]...)
		}
	}
	return data, nil
}

func (h *rpcHandler) getRowData(value []byte, colTps map[int64]*types.FieldType) (map[int64][]byte, error) {
	res, err := tablecodec.CutRow(value, colTps)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res == nil {
		res = make(map[int64][]byte, len(colTps))
	}
	return res, nil
}

// Put column values into ctx, the values will be used for expr evaluation.
func (h *rpcHandler) setColumnValueToCtx(ctx *selectContext, handle int64, row map[int64][]byte, cols map[int64]*tipb.ColumnInfo) error {
	for colID, col := range cols {
		if col.GetPkHandle() {
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				ctx.eval.Row[colID] = types.NewUintDatum(uint64(handle))
			} else {
				ctx.eval.Row[colID] = types.NewIntDatum(handle)
			}
		} else {
			data := row[colID]
			ft := distsql.FieldTypeFromPBColumn(col)
			datum, err := tablecodec.DecodeColumnValue(data, ft)
			if err != nil {
				return errors.Trace(err)
			}
			ctx.eval.Row[colID] = datum
		}
	}
	return nil
}

func (h *rpcHandler) evalWhereForRow(ctx *selectContext, handle int64, row map[int64][]byte) (bool, error) {
	if ctx.sel.Where == nil {
		return true, nil
	}
	err := h.setColumnValueToCtx(ctx, handle, row, ctx.whereColumns)
	if err != nil {
		return false, errors.Trace(err)
	}
	result, err := ctx.eval.Eval(ctx.sel.Where)
	if err != nil {
		return false, errors.Trace(err)
	}
	if result.IsNull() {
		return false, nil
	}
	boolResult, err := result.ToBool()
	if err != nil {
		return false, errors.Trace(err)
	}
	return boolResult == 1, nil
}

func (h *rpcHandler) getChunksFromIndexReq(ctx *selectContext) ([]tipb.Chunk, error) {
	kvRanges, desc := h.extractKVRanges(ctx)
	limit := int64(-1)
	if ctx.sel.Limit != nil {
		limit = ctx.sel.GetLimit()
	}
	var chunks []tipb.Chunk
	var err error
	for _, ran := range kvRanges {
		if limit == 0 {
			break
		}
		chunks, err = h.getIndexRowFromRange(ctx, ran, desc, &limit, chunks)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if ctx.aggregate {
		return h.getRowsFromAgg(ctx)
	}
	return chunks, nil
}

func (h *rpcHandler) getIndexRowFromRange(ctx *selectContext, ran kv.KeyRange, desc bool, limit *int64, chunks []tipb.Chunk) ([]tipb.Chunk, error) {
	idxInfo := ctx.sel.IndexInfo
	startKey := maxStartKey(ran.StartKey, h.startKey)
	endKey := minEndKey(ran.EndKey, h.endKey)
	if (*limit) == 0 || bytes.Compare(startKey, endKey) >= 0 {
		return nil, nil
	}
	var seekKey kv.Key
	if desc {
		seekKey = endKey
	} else {
		seekKey = startKey
	}
	ids := make([]int64, len(idxInfo.Columns))
	for i, col := range idxInfo.Columns {
		ids[i] = col.GetColumnId()
	}
	for {
		if *limit == 0 {
			break
		}
		var (
			pairs []Pair
			pair  Pair
			err   error
		)
		if desc {
			pairs = h.mvccStore.ReverseScan(startKey, seekKey, 1, ctx.sel.GetStartTs())
		} else {
			pairs = h.mvccStore.Scan(seekKey, endKey, 1, ctx.sel.GetStartTs())
		}
		if len(pairs) > 0 {
			pair = pairs[0]
		}
		if pair.Err != nil {
			return nil, errors.Trace(pair.Err)
		}
		if pair.Key == nil {
			break
		}
		if desc {
			if bytes.Compare(pair.Key, startKey) < 0 {
				break
			}
			seekKey = pair.Key
		} else {
			if bytes.Compare(pair.Key, endKey) >= 0 {
				break
			}
			seekKey = []byte(kv.Key(pair.Key).PrefixNext())
		}
		values, b, err := tablecodec.CutIndexKey(pair.Key, ids)
		var handle int64
		if len(b) > 0 {
			var handleDatum types.Datum
			_, handleDatum, err = codec.DecodeOne(b)
			if err != nil {
				return nil, errors.Trace(err)
			}
			handle = handleDatum.GetInt64()
		} else {
			handle, err = decodeHandle(pair.Value)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		row, err := h.valuesToRow(ctx, handle, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			chunks = appendRow(chunks, handle, row)
			(*limit)--
		}
	}
	return chunks, nil
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

func decodeHandle(data []byte) (int64, error) {
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, errors.Trace(err)
}

func isDefaultNull(err error, col *tipb.ColumnInfo) bool {
	return terror.ErrorEqual(err, kv.ErrNotExist) && !mysql.HasNotNullFlag(uint(col.GetFlag()))
}
