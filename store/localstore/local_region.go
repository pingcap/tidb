package localstore

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi/tablecodec"
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
}

type regionResponse struct {
	req  *regionRequest
	err  error
	data []byte
	// If region missed some request key range, newStartKey and newEndKey is returned.
	newStartKey []byte
	newEndKey   []byte
}

type selectContext struct {
	sel          *tipb.SelectRequest
	txn          kv.Transaction
	eval         *xeval.Evaluator
	whereColumns map[int64]*tipb.ColumnInfo
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
		txn := newTxn(rs.store, kv.Version{Ver: uint64(*sel.StartTs)})
		ctx := &selectContext{
			sel: sel,
			txn: txn,
		}
		if sel.Where != nil {
			ctx.eval = &xeval.Evaluator{Row: make(map[int64]types.Datum)}
			ctx.whereColumns = make(map[int64]*tipb.ColumnInfo)
			collectColumnsInWhere(sel.Where, ctx)
		}
		var rows []*tipb.Row
		if req.Tp == kv.ReqTypeSelect {
			rows, err = rs.getRowsFromSelectReq(ctx)
		} else {
			rows, err = rs.getRowsFromIndexReq(txn, sel)
		}
		selResp := new(tipb.SelectResponse)
		selResp.Error = toPBError(err)
		selResp.Rows = rows
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

func collectColumnsInWhere(expr *tipb.Expr, ctx *selectContext) error {
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
				ctx.whereColumns[i] = c
				return nil
			}
		}
		return xeval.ErrInvalid.Gen("column %d not found", i)
	}
	for _, child := range expr.Children {
		err := collectColumnsInWhere(child, ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (rs *localRegion) getRowsFromSelectReq(ctx *selectContext) ([]*tipb.Row, error) {
	kvRanges, desc := rs.extractKVRanges(ctx.sel)
	var rows []*tipb.Row
	limit := int64(-1)
	if ctx.sel.Limit != nil {
		limit = ctx.sel.GetLimit()
	}
	for _, ran := range kvRanges {
		if limit == 0 {
			break
		}
		ranRows, err := rs.getRowsFromRange(ctx, ran, limit, desc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, ranRows...)
		limit -= int64(len(ranRows))
	}
	return rows, nil
}

// extractKVRanges extracts kv.KeyRanges slice from a SelectRequest, and also returns if it is in descending order.
func (rs *localRegion) extractKVRanges(sel *tipb.SelectRequest) (kvRanges []kv.KeyRange, desc bool) {
	var (
		tid   int64
		idxID int64
	)
	if sel.IndexInfo != nil {
		tid = sel.IndexInfo.GetTableId()
		idxID = sel.IndexInfo.GetIndexId()
	} else {
		tid = sel.TableInfo.GetTableId()
	}
	for _, kran := range sel.Ranges {
		var upperKey, lowerKey kv.Key
		if idxID == 0 {
			upperKey = tablecodec.EncodeRowKey(tid, kran.GetHigh())
			if bytes.Compare(upperKey, rs.startKey) <= 0 {
				continue
			}
			lowerKey = tablecodec.EncodeRowKey(tid, kran.GetLow())
		} else {
			upperKey = tablecodec.EncodeIndexSeekKey(tid, idxID, kran.GetHigh())
			if bytes.Compare(upperKey, rs.startKey) <= 0 {
				continue
			}
			lowerKey = tablecodec.EncodeIndexSeekKey(tid, idxID, kran.GetLow())
		}
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
		desc = *sel.OrderBy[0].Desc
	}
	if desc {
		reverseKVRanges(kvRanges)
	}
	return
}

func (rs *localRegion) getRowsFromRange(ctx *selectContext, ran kv.KeyRange, limit int64, desc bool) ([]*tipb.Row, error) {
	if limit == 0 {
		return nil, nil
	}
	var rows []*tipb.Row
	if ran.IsPoint() {
		_, err := ctx.txn.Get(ran.StartKey)
		if terror.ErrorEqual(err, kv.ErrNotExist) {
			return nil, nil
		} else if err != nil {
			return nil, errors.Trace(err)
		}
		h, err := tablecodec.DecodeRowKey(ran.StartKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		match, err := rs.evalWhereForRow(ctx, h)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !match {
			return nil, nil
		}
		row, err := rs.getRowByHandle(ctx, h)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			rows = append(rows, row)
		}
		return rows, nil
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
			return nil, errors.Trace(err)
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
			return nil, errors.Trace(err)
		}
		match, err := rs.evalWhereForRow(ctx, h)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !match {
			continue
		}
		row, err := rs.getRowByHandle(ctx, h)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			rows = append(rows, row)
			limit--
		}
	}
	return rows, nil
}

func (rs *localRegion) getRowByHandle(ctx *selectContext, handle int64) (*tipb.Row, error) {
	tid := ctx.sel.TableInfo.GetTableId()
	columns := ctx.sel.TableInfo.Columns
	row := new(tipb.Row)
	var d types.Datum
	d.SetInt64(handle)
	var err error
	row.Handle, err = codec.EncodeValue(nil, d)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, col := range columns {
		if *col.PkHandle {
			if mysql.HasUnsignedFlag(uint(*col.Flag)) {
				// PK column is Unsigned
				var ud types.Datum
				ud.SetUint64(uint64(handle))
				uHandle, err1 := codec.EncodeValue(nil, ud)
				if err1 != nil {
					return nil, errors.Trace(err1)
				}
				row.Data = append(row.Data, uHandle...)
			} else {
				row.Data = append(row.Data, row.Handle...)
			}
		} else {
			colID := col.GetColumnId()
			if ctx.whereColumns[colID] != nil {
				// The column is saved in evaluator, use it directly.
				datum := ctx.eval.Row[colID]
				row.Data, err = codec.EncodeValue(row.Data, datum)
				if err != nil {
					return nil, errors.Trace(err)
				}
			} else {
				key := tablecodec.EncodeColumnKey(tid, handle, colID)
				data, err1 := ctx.txn.Get(key)
				if isDefaultNull(err1, col) {
					row.Data = append(row.Data, codec.NilFlag)
					continue
				} else if err1 != nil {
					return nil, errors.Trace(err1)
				}
				row.Data = append(row.Data, data...)
			}
		}
	}
	return row, nil
}

func (rs *localRegion) evalWhereForRow(ctx *selectContext, h int64) (bool, error) {
	if ctx.sel.Where == nil {
		return true, nil
	}
	tid := ctx.sel.TableInfo.GetTableId()
	for colID, col := range ctx.whereColumns {
		if col.GetPkHandle() {
			ctx.eval.Row[colID] = types.NewIntDatum(h)
		} else {
			key := tablecodec.EncodeColumnKey(tid, h, colID)
			data, err := ctx.txn.Get(key)
			if isDefaultNull(err, col) {
				ctx.eval.Row[colID] = types.Datum{}
				continue
			} else if err != nil {
				return false, errors.Trace(err)
			}
			datum, err := tablecodec.DecodeColumnValue(data, col)
			if err != nil {
				return false, errors.Trace(err)
			}
			ctx.eval.Row[colID] = datum
		}
	}
	result, err := ctx.eval.Eval(ctx.sel.Where)
	if err != nil {
		return false, errors.Trace(err)
	}
	if result.Kind() == types.KindNull {
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
	perr.Code = &code
	errStr := err.Error()
	perr.Msg = &errStr
	return perr
}

func (rs *localRegion) getRowsFromIndexReq(txn kv.Transaction, sel *tipb.SelectRequest) ([]*tipb.Row, error) {
	kvRanges, desc := rs.extractKVRanges(sel)
	var rows []*tipb.Row
	limit := int64(-1)
	if sel.Limit != nil {
		limit = sel.GetLimit()
	}
	for _, ran := range kvRanges {
		if limit == 0 {
			break
		}
		ranRows, err := getIndexRowFromRange(sel.IndexInfo, txn, ran, desc, limit)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, ranRows...)
		limit -= int64(len(ranRows))
	}
	return rows, nil
}

func reverseKVRanges(kvRanges []kv.KeyRange) {
	for i := 0; i < len(kvRanges)/2; i++ {
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

func getIndexRowFromRange(idxInfo *tipb.IndexInfo, txn kv.Transaction, ran kv.KeyRange, desc bool, limit int64) ([]*tipb.Row, error) {
	var rows []*tipb.Row
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
		var it kv.Iterator
		var err error
		if desc {
			it, err = txn.SeekReverse(seekKey)
			if err != nil {
				return nil, errors.Trace(err)
			}
			seekKey = it.Key()
		} else {
			it, err = txn.Seek(seekKey)
			if err != nil {
				return nil, errors.Trace(err)
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

		datums, err := tablecodec.DecodeIndexKey(it.Key())
		if err != nil {
			return nil, errors.Trace(err)
		}
		var handle types.Datum
		if len(datums) > len(idxInfo.Columns) {
			handle = datums[len(idxInfo.Columns)]
			datums = datums[:len(idxInfo.Columns)]
		} else {
			var intHandle int64
			intHandle, err = decodeHandle(it.Value())
			if err != nil {
				return nil, errors.Trace(err)
			}
			handle.SetInt64(intHandle)
		}
		data, err := codec.EncodeValue(nil, datums...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		handleData, err := codec.EncodeValue(nil, handle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row := &tipb.Row{Handle: handleData, Data: data}
		rows = append(rows, row)
		limit--
	}

	return rows, nil
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
