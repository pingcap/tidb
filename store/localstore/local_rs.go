package localstore

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tidb/xapi/tipb"
)

// local region server.
type localRS struct {
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

func (rs *localRS) Handle(req *regionRequest) (*regionResponse, error) {
	resp := &regionResponse{
		req: req,
	}
	if req.Tp == kv.ReqTypeSelect {
		sel := new(tipb.SelectRequest)
		err := proto.Unmarshal(req.data, sel)
		if err != nil {
			return nil, errors.Trace(err)
		}
		txn := newTxn(rs.store, kv.Version{Ver: uint64(*sel.StartTs)})
		rows, err := rs.getRowsFromSelectReq(txn, sel)
		selResp := new(tipb.SelectResponse)
		selResp.Error = toPBError(err)
		selResp.Rows = rows
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

// low is closed, high is open.
type kvRange struct {
	low  kv.Key
	high kv.Key
}

type kvPoint struct {
	key    kv.Key
	handle int64
}

func (rs *localRS) getRowsFromSelectReq(txn kv.Transaction, sel *tipb.SelectRequest) ([]*tipb.Row, error) {
	tid := sel.TableInfo.GetTableId()
	kvRanges := rs.extractRecordKVRanges(tid, sel.Ranges)
	kvPoints := rs.extractRecordKVPoints(tid, sel.Points)
	var handles []int64
	for _, ran := range kvRanges {
		ranHandles, err := seekRangeHandles(txn, ran)
		if err != nil {
			return nil, errors.Trace(err)
		}
		handles = append(handles, ranHandles...)
	}
	for _, point := range kvPoints {
		_, err := txn.Get(point.key)
		if terror.ErrorEqual(err, kv.ErrNotExist) {
			continue
		}
		handles = append(handles, point.handle)
	}
	tablecodec.SortHandles(handles)
	var rows []*tipb.Row
	for _, h := range handles {
		row, err := rs.getRowFromHandle(txn, tid, h, sel.TableInfo.Columns)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (rs *localRS) extractRecordKVRanges(tid int64, krans []*tipb.HandleRange) []kvRange {
	var kvRanges []kvRange
	for _, kran := range krans {
		upperKey := tablecodec.EncodeRecordKey(tid, kran.GetHigh(), 0)
		if bytes.Compare(upperKey, rs.startKey) <= 0 {
			continue
		}
		lowerKey := tablecodec.EncodeRecordKey(tid, kran.GetLow(), 0)
		if bytes.Compare(lowerKey, rs.endKey) >= 0 {
			continue
		}
		var kvr kvRange
		if bytes.Compare(lowerKey, rs.startKey) <= 0 {
			kvr.low = rs.startKey
		} else {
			kvr.low = lowerKey
		}
		if bytes.Compare(upperKey, rs.endKey) <= 0 {
			kvr.high = upperKey
		} else {
			kvr.high = rs.endKey
		}
		kvRanges = append(kvRanges, kvr)
	}
	return kvRanges
}

func (rs *localRS) extractRecordKVPoints(tid int64, points []int64) []kvPoint {
	var kvPoints []kvPoint
	for _, point := range points {
		kvKey := tablecodec.EncodeRecordKey(tid, point, 0)
		if kvKey.Cmp(kv.Key(rs.startKey)) < 0 {
			continue
		}
		if kvKey.Cmp(kv.Key(rs.endKey)) >= 0 {
			continue
		}
		kvPoints = append(kvPoints, kvPoint{key: kvKey, handle: point})
	}
	return kvPoints
}

func (rs *localRS) getRowFromHandle(txn kv.Transaction, tid, h int64, columns []*tipb.ColumnInfo) (*tipb.Row, error) {
	row := new(tipb.Row)
	row.Handle = &h
	for _, col := range columns {
		if *col.PkHandle {
			var d types.Datum
			d.SetInt64(h)
			var err error
			row.Data, err = codec.EncodeValue(row.Data, d)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			key := tablecodec.EncodeRecordKey(tid, h, col.GetColumnId())
			data, err := txn.Get(key)
			if err != nil {
				return nil, errors.Trace(err)
			}
			row.Data = append(row.Data, data...)
		}
	}
	return row, nil
}

func toPBError(err error) *tipb.Error {
	if err == nil {
		return nil
	}
	perr := new(tipb.Error)
	code := tipb.ErrorCode_UnkownError
	perr.Code = &code
	errStr := err.Error()
	perr.ErrMsg = &errStr
	return perr
}

func seekRangeHandles(txn kv.Transaction, ran kvRange) ([]int64, error) {
	seekKey := ran.low
	var handles []int64
	for {
		it, err := txn.Seek(seekKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !it.Valid() || it.Key().Cmp(ran.high) >= 0 {
			break
		}
		tid, h, _, err := tablecodec.DecodeRecordKey(it.Key())
		if err != nil {
			return nil, errors.Trace(err)
		}
		handles = append(handles, h)
		seekKey = tablecodec.EncodeRecordKey(tid, h+1, 0)
	}
	return handles, nil
}

func buildLocalRegionServers(store *dbStore) []*localRS {
	return []*localRS{
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
