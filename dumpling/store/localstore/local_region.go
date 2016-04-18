package localstore

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
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
	kvRanges := rs.extractKVRanges(ctx.sel.TableInfo.GetTableId(), 0, ctx.sel.Ranges)
	var rows []*tipb.Row
	for _, ran := range kvRanges {
		ranRows, err := rs.getRowsFromRange(ctx, ran)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, ranRows...)
	}
	return rows, nil
}

func (rs *localRegion) extractKVRanges(tid int64, idxID int64, krans []*tipb.KeyRange) []kv.KeyRange {
	var kvRanges []kv.KeyRange
	for _, kran := range krans {
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
	return kvRanges
}

func (rs *localRegion) getRowsFromRange(ctx *selectContext, ran kv.KeyRange) ([]*tipb.Row, error) {
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
	seekKey := ran.StartKey
	for {
		it, err := ctx.txn.Seek(seekKey)
		seekKey = it.Key().PrefixNext()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !it.Valid() || it.Key().Cmp(ran.EndKey) >= 0 {
			break
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
			row.Data = append(row.Data, row.Handle...)
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
				if err1 != nil {
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
			if err != nil {
				return false, errors.Trace(err)
			}
			_, datum, err := codec.DecodeOne(data)
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
	tid := sel.IndexInfo.GetTableId()
	idxID := sel.IndexInfo.GetIndexId()
	kvRanges := rs.extractKVRanges(tid, idxID, sel.Ranges)
	var rows []*tipb.Row
	for _, ran := range kvRanges {
		ranRows, err := getIndexRowFromRange(sel.IndexInfo, txn, ran)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, ranRows...)
	}
	return rows, nil
}

func getIndexRowFromRange(idxInfo *tipb.IndexInfo, txn kv.Transaction, ran kv.KeyRange) ([]*tipb.Row, error) {
	var rows []*tipb.Row
	seekKey := ran.StartKey
	for {
		it, err := txn.Seek(seekKey)
		// We have to update the seekKey here, because decoding may change the it.Key(), which should not be allowed.
		// TODO: make sure decoding don't modify the original data.
		seekKey = it.Key().PrefixNext()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !it.Valid() || it.Key().Cmp(ran.EndKey) >= 0 {
			break
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
