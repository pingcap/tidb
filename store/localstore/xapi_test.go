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

package localstore

import (
	"fmt"
	"io/ioutil"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tipb/go-tipb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testXAPISuite{})

type testXAPISuite struct {
}

var tbInfo = &simpleTableInfo{
	tID:     1,
	cTypes:  []byte{mysql.TypeVarchar, mysql.TypeDouble},
	cIDs:    []int64{3, 4},
	indices: []int{0}, // column 3 of varchar type.
	iIDs:    []int64{5},
}

func (s *testXAPISuite) TestSelect(c *C) {
	defer testleak.AfterTest(c)()
	store := createMemStore(time.Now().Nanosecond())
	count := int64(10)
	err := prepareTableData(store, tbInfo, count, genValues)
	c.Check(err, IsNil)

	// Select Table request.
	txn, err := store.Begin()
	c.Check(err, IsNil)
	client := txn.GetClient()
	req, err := prepareSelectRequest(tbInfo, txn.StartTS())
	c.Check(err, IsNil)
	resp := client.Send(req)
	subResp, err := resp.Next()
	c.Check(err, IsNil)
	data, err := ioutil.ReadAll(subResp)
	c.Check(err, IsNil)
	selResp := new(tipb.SelectResponse)
	proto.Unmarshal(data, selResp)
	c.Check(selResp.Rows, HasLen, int(count))
	for i, row := range selResp.Rows {
		handle := int64(i + 1)
		expectedDatums := []types.Datum{types.NewDatum(handle)}
		expectedDatums = append(expectedDatums, genValues(handle, tbInfo)...)
		var expectedEncoded []byte
		expectedEncoded, err = codec.EncodeValue(nil, expectedDatums...)
		c.Assert(err, IsNil)
		c.Assert(row.Data, BytesEquals, expectedEncoded)
	}
	txn.Commit()

	// Select Index request.
	txn, err = store.Begin()
	c.Check(err, IsNil)
	client = txn.GetClient()
	req, err = prepareIndexRequest(tbInfo, txn.StartTS())
	c.Check(err, IsNil)
	resp = client.Send(req)
	subResp, err = resp.Next()
	c.Check(err, IsNil)
	data, err = ioutil.ReadAll(subResp)
	c.Check(err, IsNil)
	idxResp := new(tipb.SelectResponse)
	proto.Unmarshal(data, idxResp)
	c.Check(idxResp.Rows, HasLen, int(count))
	handles := make([]int, 0, 10)
	for _, row := range idxResp.Rows {
		var err error
		datums, err := codec.Decode(row.Handle)
		c.Check(err, IsNil)
		c.Check(datums, HasLen, 1)
		handles = append(handles, int(datums[0].GetInt64()))
	}
	sort.Ints(handles)
	for i, h := range handles {
		c.Assert(h, Equals, i+1)
	}
	txn.Commit()

	store.Close()
}

// simpleTableInfo just have the minimum information enough to describe the table.
// The first column is pk handle column.
type simpleTableInfo struct {
	tID     int64  // table ID.
	cTypes  []byte // columns not including pk handle column.
	cIDs    []int64
	indices []int // indexed column offsets. only single column index for now.
	iIDs    []int64
}

func (s *simpleTableInfo) toPBTableInfo() *tipb.TableInfo {
	tbInfo := new(tipb.TableInfo)
	tbInfo.TableId = proto.Int64(s.tID)
	pkColumn := new(tipb.ColumnInfo)
	pkColumn.Tp = proto.Int32(int32(mysql.TypeLonglong))
	// It's ok to just use table ID for pk column ID, as it doesn't have a column kv.
	pkColumn.ColumnId = tbInfo.TableId
	pkColumn.PkHandle = proto.Bool(true)
	pkColumn.Flag = proto.Int32(0)
	tbInfo.Columns = append(tbInfo.Columns, pkColumn)
	for i, colTp := range s.cTypes {
		coInfo := &tipb.ColumnInfo{
			ColumnId: proto.Int64(s.cIDs[i]),
			Tp:       proto.Int32(int32(colTp)),
			PkHandle: proto.Bool(false),
		}
		tbInfo.Columns = append(tbInfo.Columns, coInfo)
	}
	return tbInfo
}

func (s *simpleTableInfo) toPBIndexInfo(idxOff int) *tipb.IndexInfo {
	idxInfo := new(tipb.IndexInfo)
	idxInfo.TableId = proto.Int64(s.tID)
	idxInfo.IndexId = proto.Int64(s.iIDs[idxOff])
	colOff := s.indices[idxOff]
	idxInfo.Columns = []*tipb.ColumnInfo{
		{
			ColumnId: proto.Int64(s.cIDs[colOff]),
			Tp:       proto.Int32((int32(s.cTypes[colOff]))),
			PkHandle: proto.Bool(false),
		},
	}
	return idxInfo
}

func genValues(handle int64, tbl *simpleTableInfo) []types.Datum {
	values := make([]types.Datum, 0, len(tbl.cTypes))
	for _, tp := range tbl.cTypes {
		switch tp {
		case mysql.TypeLong:
			values = append(values, types.NewDatum(handle))
		case mysql.TypeVarchar:
			values = append(values, types.NewDatum(fmt.Sprintf("varchar:%d", handle)))
		case mysql.TypeDouble:
			values = append(values, types.NewDatum(float64(handle)/10))
		default:
			values = append(values, types.Datum{})
		}
	}
	return values
}

type genValueFunc func(handle int64, tbl *simpleTableInfo) []types.Datum

func prepareTableData(store kv.Storage, tbl *simpleTableInfo, count int64, gen genValueFunc) error {
	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	for i := int64(1); i <= count; i++ {
		setRow(txn, i, tbl, gen)
	}
	return txn.Commit()
}

func setRow(txn kv.Transaction, handle int64, tbl *simpleTableInfo, gen genValueFunc) error {
	rowKey := tablecodec.EncodeRowKey(tbl.tID, codec.EncodeInt(nil, handle))
	txn.Set(rowKey, []byte(txn.String()))
	columnValues := gen(handle, tbl)
	for i, v := range columnValues {
		cKey, cVal, err := encodeColumnKV(tbl.tID, handle, tbl.cIDs[i], v)
		if err != nil {
			return errors.Trace(err)
		}
		err = txn.Set(cKey, cVal)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, idxCol := range tbl.indices {
		idxVal := columnValues[idxCol]
		encoded, err := codec.EncodeKey(nil, idxVal, types.NewDatum(handle))
		if err != nil {
			return errors.Trace(err)
		}
		idxKey := tablecodec.EncodeIndexSeekKey(tbl.tID, tbl.iIDs[i], encoded)
		err = txn.Set(idxKey, []byte{0})
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func encodeColumnKV(tid, handle, cid int64, value types.Datum) (kv.Key, []byte, error) {
	key := tablecodec.EncodeColumnKey(tid, handle, cid)
	val, err := codec.EncodeValue(nil, value)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return key, val, nil
}

func prepareSelectRequest(simpleInfo *simpleTableInfo, startTs uint64) (*kv.Request, error) {
	selReq := new(tipb.SelectRequest)
	selReq.TableInfo = simpleInfo.toPBTableInfo()
	selReq.StartTs = proto.Uint64(startTs)
	selReq.Ranges = []*tipb.KeyRange{fullPBTableRange}
	data, err := proto.Marshal(selReq)
	if err != nil {
		return nil, errors.Trace(err)
	}
	req := new(kv.Request)
	req.Tp = kv.ReqTypeSelect
	req.Concurrency = 1
	req.KeyRanges = []kv.KeyRange{fullTableRange(simpleInfo.tID)}
	req.Data = data
	return req, nil
}

func fullTableRange(tid int64) kv.KeyRange {
	return kv.KeyRange{
		StartKey: tablecodec.EncodeRowKey(tid, codec.EncodeInt(nil, math.MinInt64)),
		EndKey:   tablecodec.EncodeRowKey(tid, codec.EncodeInt(nil, math.MaxInt64)),
	}
}

var fullPBTableRange = &tipb.KeyRange{
	Low:  codec.EncodeInt(nil, math.MinInt64),
	High: codec.EncodeInt(nil, math.MaxInt64),
}
var fullPBIndexRange = &tipb.KeyRange{
	Low:  []byte{0},
	High: []byte{255},
}

func fullIndexRange(tid int64, idxID int64) kv.KeyRange {
	return kv.KeyRange{
		StartKey: tablecodec.EncodeIndexSeekKey(tid, idxID, []byte{0}),
		EndKey:   tablecodec.EncodeIndexSeekKey(tid, idxID, []byte{255}),
	}
}

func prepareIndexRequest(simpleInfo *simpleTableInfo, startTs uint64) (*kv.Request, error) {
	selReq := new(tipb.SelectRequest)
	selReq.IndexInfo = simpleInfo.toPBIndexInfo(0)
	selReq.StartTs = proto.Uint64(startTs)
	selReq.Ranges = []*tipb.KeyRange{fullPBIndexRange}
	data, err := proto.Marshal(selReq)
	if err != nil {
		return nil, errors.Trace(err)
	}
	req := new(kv.Request)
	req.Tp = kv.ReqTypeIndex
	req.Concurrency = 1
	req.KeyRanges = []kv.KeyRange{fullIndexRange(simpleInfo.tID, simpleInfo.iIDs[0])}
	req.Data = data
	return req, nil
}
