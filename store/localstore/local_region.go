package localstore

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tidb/xapi/xeval"
	"github.com/pingcap/tipb/go-tipb"
)

// local region server.
type localRegion struct {
	id       int
	store    *dbStore
	startKey []byte
	endKey   []byte
}

type regionRequest struct {
	Tp       int64
	data     []byte
	startKey []byte
	endKey   []byte
	ranges   []kv.KeyRange
}

type regionResponse struct {
	req  *regionRequest
	err  error
	data []byte
	// If region missed some request key range, newStartKey and newEndKey is returned.
	newStartKey []byte
	newEndKey   []byte
}

const chunkSize = 64

type selectContext struct {
	sel          *tipb.SelectRequest
	txn          kv.Transaction
	eval         *xeval.Evaluator
	whereColumns map[int64]*tipb.ColumnInfo
	aggColumns   map[int64]*tipb.ColumnInfo
	groups       map[string]bool
	groupKeys    [][]byte
	aggregates   []*aggregateFuncExpr
	aggregate    bool
	keyRanges    []kv.KeyRange

	// Use for DecodeRow.
	colTps map[int64]*types.FieldType

	chunks []tipb.Chunk
}

func (rs *localRegion) Handle(req *regionRequest) (*regionResponse, error) {
	resp := &regionResponse{
		req: req,
	}
	if req.Tp == kv.ReqTypeSelect || req.Tp == kv.ReqTypeIndex {
		sel := new(tipb.SelectRequest)
		err := proto.Unmarshal(req.data, sel)
		if err != nil {
			return nil, errors.Trace(err)
		}
		txn := newTxn(rs.store, kv.Version{Ver: uint64(sel.StartTs)})
		ctx := &selectContext{
			sel:       sel,
			txn:       txn,
			keyRanges: req.ranges,
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
		if req.Tp == kv.ReqTypeSelect {
			err = rs.getRowsFromSelectReq(ctx)
		} else {
			// The PKHandle column info has been collected in ctx, so we can remove it in IndexInfo.
			length := len(sel.IndexInfo.Columns)
			if sel.IndexInfo.Columns[length-1].GetPkHandle() {
				sel.IndexInfo.Columns = sel.IndexInfo.Columns[:length-1]
			}
			err = rs.getRowsFromIndexReq(ctx)
		}
		selResp := new(tipb.SelectResponse)
		selResp.Error = toPBError(err)
		selResp.Chunks = ctx.chunks
		resp.err = err
		data, err := proto.Marshal(selResp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.data = data
	}
	if bytes.Compare(rs.startKey, req.startKey) < 0 || bytes.Compare(rs.endKey, req.endKey) > 0 {
		resp.newStartKey = rs.startKey
		resp.newEndKey = rs.endKey
	}
	return resp, nil
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

func (rs *localRegion) getRowsFromSelectReq(ctx *selectContext) error {
	// Init ctx.colTps and use it to decode all the rows.
	columns := ctx.sel.TableInfo.Columns
	ctx.colTps = make(map[int64]*types.FieldType, len(columns))
	for _, col := range columns {
		if col.GetPkHandle() {
			continue
		}
		ctx.colTps[col.GetColumnId()] = xapi.FieldTypeFromPBColumn(col)
	}

	kvRanges, desc := rs.extractKVRanges(ctx)
	limit := int64(-1)
	if ctx.sel.Limit != nil {
		limit = ctx.sel.GetLimit()
	}
	for _, ran := range kvRanges {
		if limit == 0 {
			break
		}
		count, err := rs.getRowsFromRange(ctx, ran, limit, desc)
		if err != nil {
			return errors.Trace(err)
		}
		limit -= count
	}
	if ctx.aggregate {
		return rs.getRowsFromAgg(ctx)
	}
	return nil
}

/*
 * Convert aggregate partial result to rows.
 * Data layout example:
 *	SQL:	select count(c1), sum(c2), avg(c3) from t;
 *	Aggs:	count(c1), sum(c2), avg(c3)
 *	Rows:	groupKey1, count1, value2, count3, value3
 *		groupKey2, count1, value2, count3, value3
 */
func (rs *localRegion) getRowsFromAgg(ctx *selectContext) error {
	for _, gk := range ctx.groupKeys {
		chunk := rs.getChunk(ctx)
		// Each aggregate partial result will be converted to one or two datums.
		rowData := make([]types.Datum, 0, 1+2*len(ctx.aggregates))
		// The first column is group key.
		rowData = append(rowData, types.NewBytesDatum(gk))
		for _, agg := range ctx.aggregates {
			agg.currentGroup = gk
			ds, err := agg.toDatums()
			if err != nil {
				return errors.Trace(err)
			}
			rowData = append(rowData, ds...)
		}
		var err error
		beforeLen := len(chunk.RowsData)
		chunk.RowsData, err = codec.EncodeValue(chunk.RowsData, rowData...)
		if err != nil {
			return errors.Trace(err)
		}
		var rowMeta tipb.RowMeta
		rowMeta.Length = int64(len(chunk.RowsData) - beforeLen)
		chunk.RowsMeta = append(chunk.RowsMeta, rowMeta)
	}
	return nil
}

// extractKVRanges extracts kv.KeyRanges slice from ctx.keyRanges, and also returns if it is in descending order.
func (rs *localRegion) extractKVRanges(ctx *selectContext) (kvRanges []kv.KeyRange, desc bool) {
	sel := ctx.sel
	for _, kran := range ctx.keyRanges {
		upperKey := kran.EndKey
		if bytes.Compare(upperKey, rs.startKey) <= 0 {
			continue
		}
		lowerKey := kran.StartKey
		if bytes.Compare(lowerKey, rs.endKey) >= 0 {
			break
		}
		var kvr kv.KeyRange
		if bytes.Compare(lowerKey, rs.startKey) <= 0 {
			kvr.StartKey = rs.startKey
		} else {
			kvr.StartKey = lowerKey
		}
		if bytes.Compare(upperKey, rs.endKey) <= 0 {
			kvr.EndKey = upperKey
		} else {
			kvr.EndKey = rs.endKey
		}
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

func (rs *localRegion) getRowsFromRange(ctx *selectContext, ran kv.KeyRange, limit int64, desc bool) (count int64, err error) {
	if limit == 0 {
		return 0, nil
	}
	if ran.IsPoint() {
		var value []byte
		value, err = ctx.txn.Get(ran.StartKey)
		if terror.ErrorEqual(err, kv.ErrNotExist) {
			return 0, nil
		} else if err != nil {
			return 0, errors.Trace(err)
		}
		var h int64
		h, err = tablecodec.DecodeRowKey(ran.StartKey)
		if err != nil {
			return 0, errors.Trace(err)
		}
		gotRow, err1 := rs.handleRowData(ctx, h, value)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}

		if gotRow {
			count++
		}
		return
	}
	var seekKey kv.Key
	if desc {
		seekKey = ran.EndKey
	} else {
		seekKey = ran.StartKey
	}
	for {
		if limit == 0 {
			break
		}
		var (
			it  kv.Iterator
			err error
		)
		if desc {
			it, err = ctx.txn.SeekReverse(seekKey)
		} else {
			it, err = ctx.txn.Seek(seekKey)
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !it.Valid() {
			break
		}
		if desc {
			if it.Key().Cmp(ran.StartKey) < 0 {
				break
			}
			seekKey = tablecodec.TruncateToRowKeyLen(it.Key())
		} else {
			if it.Key().Cmp(ran.EndKey) >= 0 {
				break
			}
			seekKey = it.Key().PrefixNext()
		}
		h, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return 0, errors.Trace(err)
		}
		gotRow, err := rs.handleRowData(ctx, h, it.Value())
		if err != nil {
			return 0, errors.Trace(err)
		}
		if gotRow {
			limit--
			count++
		}
	}
	return count, nil
}

// handleRowData deals with raw row data:
//	1. Decodes row from raw byte slice.
//	2. Checks if it fit where condition.
//	3. Update aggregate functions.
// returns true if got a row.
func (rs *localRegion) handleRowData(ctx *selectContext, handle int64, value []byte) (bool, error) {
	columns := ctx.sel.TableInfo.Columns
	values, err := rs.getRowData(value, ctx.colTps)
	if err != nil {
		return false, errors.Trace(err)
	}
	// Fill handle and null columns.
	for _, col := range columns {
		if col.GetPkHandle() {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(uint(col.Flag)) {
				// PK column is Unsigned
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(nil, handleDatum)
			if err1 != nil {
				return false, errors.Trace(err1)
			}
			values[col.GetColumnId()] = handleData
		} else {
			_, ok := values[col.GetColumnId()]
			if !ok {
				if mysql.HasNotNullFlag(uint(col.GetFlag())) {
					return false, errors.New("Miss column")
				}
				values[col.GetColumnId()] = []byte{codec.NilFlag}
			}
		}
	}
	return rs.valuesToRow(ctx, handle, values)
}

func (rs *localRegion) valuesToRow(ctx *selectContext, handle int64, values map[int64][]byte) (bool, error) {
	var columns []*tipb.ColumnInfo
	if ctx.sel.TableInfo != nil {
		columns = ctx.sel.TableInfo.Columns
	} else {
		columns = ctx.sel.IndexInfo.Columns
	}
	// Evaluate where
	match, err := rs.evalWhereForRow(ctx, handle, values)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !match {
		return false, nil
	}
	if ctx.aggregate {
		// Update aggregate functions.
		err = rs.aggregate(ctx, handle, values)
		if err != nil {
			return false, errors.Trace(err)
		}
		return false, nil
	}
	chunk := rs.getChunk(ctx)
	var rowMeta tipb.RowMeta
	rowMeta.Handle = handle

	// If without aggregate functions, just return raw row data.
	for _, col := range columns {
		val := values[col.GetColumnId()]
		rowMeta.Length += int64(len(val))
		chunk.RowsData = append(chunk.RowsData, val...)
	}
	chunk.RowsMeta = append(chunk.RowsMeta, rowMeta)
	return true, nil
}

func (rs *localRegion) getChunk(ctx *selectContext) *tipb.Chunk {
	chunkLen := len(ctx.chunks)
	if chunkLen == 0 || len(ctx.chunks[chunkLen-1].RowsMeta) >= chunkSize {
		newChunk := tipb.Chunk{}
		ctx.chunks = append(ctx.chunks, newChunk)
	}
	return &ctx.chunks[len(ctx.chunks)-1]
}

func (rs *localRegion) getRowData(value []byte, colTps map[int64]*types.FieldType) (map[int64][]byte, error) {
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
func (rs *localRegion) setColumnValueToCtx(ctx *selectContext, h int64, row map[int64][]byte, cols map[int64]*tipb.ColumnInfo) error {
	for colID, col := range cols {
		if col.GetPkHandle() {
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				ctx.eval.Row[colID] = types.NewUintDatum(uint64(h))
			} else {
				ctx.eval.Row[colID] = types.NewIntDatum(h)
			}
		} else {
			data := row[colID]
			ft := xapi.FieldTypeFromPBColumn(col)
			datum, err := tablecodec.DecodeColumnValue(data, ft)
			if err != nil {
				return errors.Trace(err)
			}
			ctx.eval.Row[colID] = datum
		}
	}
	return nil
}

func (rs *localRegion) evalWhereForRow(ctx *selectContext, h int64, row map[int64][]byte) (bool, error) {
	if ctx.sel.Where == nil {
		return true, nil
	}
	err := rs.setColumnValueToCtx(ctx, h, row, ctx.whereColumns)
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

func toPBError(err error) *tipb.Error {
	if err == nil {
		return nil
	}
	perr := new(tipb.Error)
	code := int32(1)
	perr.Code = code
	errStr := err.Error()
	perr.Msg = errStr
	return perr
}

func (rs *localRegion) getRowsFromIndexReq(ctx *selectContext) error {
	kvRanges, desc := rs.extractKVRanges(ctx)
	limit := int64(-1)
	if ctx.sel.Limit != nil {
		limit = ctx.sel.GetLimit()
	}
	for _, ran := range kvRanges {
		if limit == 0 {
			break
		}
		count, err := rs.getIndexRowFromRange(ctx, ran, desc, limit)
		if err != nil {
			return errors.Trace(err)
		}
		limit -= int64(count)
	}
	if ctx.aggregate {
		return rs.getRowsFromAgg(ctx)
	}
	return nil
}

func reverseKVRanges(kvRanges []kv.KeyRange) {
	for i := 0; i < len(kvRanges)/2; i++ {
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

func (rs *localRegion) getIndexRowFromRange(ctx *selectContext, ran kv.KeyRange, desc bool, limit int64) (count int64, err error) {
	idxInfo := ctx.sel.IndexInfo
	txn := ctx.txn
	var seekKey kv.Key
	if desc {
		seekKey = ran.EndKey
	} else {
		seekKey = ran.StartKey
	}
	ids := make([]int64, len(idxInfo.Columns))
	for i, col := range idxInfo.Columns {
		ids[i] = col.GetColumnId()
	}
	for {
		if limit == 0 {
			break
		}
		var it kv.Iterator
		if desc {
			it, err = txn.SeekReverse(seekKey)
			if err != nil {
				return 0, errors.Trace(err)
			}
			seekKey = it.Key()
		} else {
			it, err = txn.Seek(seekKey)
			if err != nil {
				return 0, errors.Trace(err)
			}
			seekKey = it.Key().PrefixNext()
		}
		if !it.Valid() {
			break
		}
		if desc {
			if it.Key().Cmp(ran.StartKey) < 0 {
				break
			}
		} else {
			if it.Key().Cmp(ran.EndKey) >= 0 {
				break
			}
		}
		values, b, err1 := tablecodec.CutIndexKey(it.Key(), ids)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}
		var handle int64
		if len(b) > 0 {
			var handleDatum types.Datum
			_, handleDatum, err = codec.DecodeOne(b)
			if err != nil {
				return 0, errors.Trace(err)
			}
			handle = handleDatum.GetInt64()
		} else {
			handle, err = decodeHandle(it.Value())
			if err != nil {
				return 0, errors.Trace(err)
			}
		}
		gotRow, err := rs.valuesToRow(ctx, handle, values)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if gotRow {
			limit--
			count++
		}
	}
	return count, nil
}

func decodeHandle(data []byte) (int64, error) {
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, errors.Trace(err)
}

func buildLocalRegionServers(store *dbStore) []*localRegion {
	return []*localRegion{
		{
			id:       1,
			store:    store,
			startKey: []byte(""),
			endKey:   []byte("t"),
		},
		{
			id:       2,
			store:    store,
			startKey: []byte("t"),
			endKey:   []byte("u"),
		},
		{
			id:       3,
			store:    store,
			startKey: []byte("u"),
			endKey:   []byte("z"),
		},
	}
}

func isDefaultNull(err error, col *tipb.ColumnInfo) bool {
	return terror.ErrorEqual(err, kv.ErrNotExist) && !mysql.HasNotNullFlag(uint(col.GetFlag()))
}
