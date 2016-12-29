// Copyright 2015 PingCAP, Inc.
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

package inspectkv

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store  kv.Storage
	ctx    *mock.Context
	dbInfo *model.DBInfo
	tbInfo *model.TableInfo
}

func (s *testSuite) SetUpSuite(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	var err error
	s.store, err = driver.Open("memory:test_inspect")
	c.Assert(err, IsNil)

	s.ctx = mock.NewContext()
	s.ctx.Store = s.store

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)

	s.dbInfo = &model.DBInfo{
		ID:   1,
		Name: model.NewCIStr("a"),
	}
	err = t.CreateDatabase(s.dbInfo)
	c.Assert(err, IsNil)
	pkFieldType := types.NewFieldType(mysql.TypeLong)
	pkFieldType.Flag = mysql.PriKeyFlag | mysql.NotNullFlag
	pkCol := &model.ColumnInfo{
		Name:      model.NewCIStr("pk"),
		ID:        1,
		Offset:    0,
		State:     model.StatePublic,
		FieldType: *pkFieldType,
	}
	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c"),
		ID:           2,
		Offset:       1,
		DefaultValue: 1,
		State:        model.StatePublic,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
	}
	col1 := &model.ColumnInfo{
		Name:         model.NewCIStr("c1"),
		ID:           3,
		Offset:       2,
		DefaultValue: 1,
		State:        model.StatePublic,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
	}
	idx := &model.IndexInfo{
		Name:   model.NewCIStr("c"),
		ID:     4,
		Unique: true,
		Columns: []*model.IndexColumn{{
			Name:   model.NewCIStr("c"),
			Offset: 1,
			Length: 255,
		}},
		State: model.StatePublic,
	}
	s.tbInfo = &model.TableInfo{
		ID:         5,
		Name:       model.NewCIStr("t"),
		State:      model.StatePublic,
		Columns:    []*model.ColumnInfo{pkCol, col, col1},
		Indices:    []*model.IndexInfo{idx},
		PKIsHandle: true,
	}
	err = t.CreateTable(s.dbInfo.ID, s.tbInfo)
	c.Assert(err, IsNil)

	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testSuite) TearDownSuite(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)

	err = t.DropTable(s.dbInfo.ID, s.tbInfo.ID)
	c.Assert(err, IsNil)
	err = t.DropDatabase(s.dbInfo.ID)
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)

	err = s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetDDLInfo(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)

	owner := &model.Owner{OwnerID: "owner"}
	err = t.SetDDLJobOwner(owner)
	c.Assert(err, IsNil)
	dbInfo2 := &model.DBInfo{
		ID:    2,
		Name:  model.NewCIStr("b"),
		State: model.StateNone,
	}
	job := &model.Job{
		SchemaID: dbInfo2.ID,
		Type:     model.ActionCreateSchema,
		RowCount: 0,
	}
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	info, err := GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Owner, DeepEquals, owner)
	c.Assert(info.Job, DeepEquals, job)
	c.Assert(info.ReorgHandle, Equals, int64(0))
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetBgDDLInfo(c *C) {
	defer testleak.AfterTest(c)()
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)

	owner := &model.Owner{OwnerID: "owner"}
	err = t.SetBgJobOwner(owner)
	c.Assert(err, IsNil)
	job := &model.Job{
		SchemaID: 1,
		Type:     model.ActionDropTable,
		RowCount: 0,
	}
	err = t.EnQueueBgJob(job)
	c.Assert(err, IsNil)
	info, err := GetBgDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Owner, DeepEquals, owner)
	c.Assert(info.Job, DeepEquals, job)
	c.Assert(info.ReorgHandle, Equals, int64(0))
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestScan(c *C) {
	defer testleak.AfterTest(c)()
	alloc := autoid.NewAllocator(s.store, s.dbInfo.ID)
	tb, err := tables.TableFromMeta(alloc, s.tbInfo)
	c.Assert(err, IsNil)
	indices := tb.Indices()
	c.Assert(s.ctx.NewTxn(), IsNil)
	_, err = tb.AddRecord(s.ctx, types.MakeDatums(1, 10, 11))
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(), IsNil)

	record1 := &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(1), int64(10), int64(11))}
	record2 := &RecordData{Handle: int64(2), Values: types.MakeDatums(int64(2), int64(20), int64(21))}
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	records, _, err := ScanSnapshotTableRecord(s.store, ver, tb, int64(1), 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record1})

	c.Assert(s.ctx.NewTxn(), IsNil)
	_, err = tb.AddRecord(s.ctx, record2.Values)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(), IsNil)
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	records, nextHandle, err := ScanTableRecord(txn, tb, int64(1), 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record1})
	records, nextHandle, err = ScanTableRecord(txn, tb, nextHandle, 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record2})
	startHandle := nextHandle
	records, nextHandle, err = ScanTableRecord(txn, tb, startHandle, 1)
	c.Assert(err, IsNil)
	c.Assert(records, IsNil)
	c.Assert(nextHandle, Equals, startHandle)

	idxRow1 := &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(10))}
	idxRow2 := &RecordData{Handle: int64(2), Values: types.MakeDatums(int64(20))}
	kvIndex := tables.NewIndex(tb.Meta(), indices[0].Meta())
	idxRows, nextVals, err := ScanIndexData(txn, kvIndex, idxRow1.Values, 2)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*RecordData{idxRow1, idxRow2})
	idxRows, nextVals, err = ScanIndexData(txn, kvIndex, idxRow1.Values, 1)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*RecordData{idxRow1})
	idxRows, nextVals, err = ScanIndexData(txn, kvIndex, nextVals, 1)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*RecordData{idxRow2})
	idxRows, nextVals, err = ScanIndexData(txn, kvIndex, nextVals, 1)
	c.Assert(idxRows, IsNil)
	c.Assert(nextVals, DeepEquals, types.MakeDatums(nil))
	c.Assert(err, IsNil)

	s.testTableData(c, tb, []*RecordData{record1, record2})

	s.testIndex(c, tb, tb.Indices()[0])

	c.Assert(s.ctx.NewTxn(), IsNil)
	err = tb.RemoveRecord(s.ctx, 1, record1.Values)
	c.Assert(err, IsNil)
	err = tb.RemoveRecord(s.ctx, 2, record2.Values)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(), IsNil)
}

func newDiffRetError(prefix string, ra, rb *RecordData) string {
	return fmt.Sprintf("[inspectkv:1]%s:%v != record:%v", prefix, ra, rb)
}

func (s *testSuite) testTableData(c *C, tb table.Table, rs []*RecordData) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	err = CompareTableRecord(txn, tb, rs, true)
	c.Assert(err, IsNil)

	cnt, err := GetTableRecordsCount(txn, tb, 0)
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(len(rs)))

	records := []*RecordData{
		{Handle: rs[0].Handle},
		{Handle: rs[1].Handle},
	}
	err = CompareTableRecord(txn, tb, records, false)
	c.Assert(err, IsNil)

	record := &RecordData{Handle: rs[1].Handle, Values: types.MakeDatums(int64(30))}
	err = CompareTableRecord(txn, tb, []*RecordData{rs[0], record}, true)
	c.Assert(err, NotNil)
	diffMsg := newDiffRetError("data", record, rs[1])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	record.Handle = 3
	err = CompareTableRecord(txn, tb, []*RecordData{rs[0], record, rs[1]}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", record, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(txn, tb, []*RecordData{rs[0], rs[1], record}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", record, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(txn, tb, []*RecordData{rs[0]}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", nil, rs[1])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(txn, tb, nil, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", nil, rs[0])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	errRs := append(rs, &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(3))})
	err = CompareTableRecord(txn, tb, errRs, false)
	c.Assert(err.Error(), DeepEquals, "[inspectkv:2]handle:1 is repeated in data")
}

func (s *testSuite) testIndex(c *C, tb table.Table, idx table.Index) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	err = CompareIndexData(txn, tb, idx)
	c.Assert(err, IsNil)

	cnt, err := GetIndexRecordsCount(txn, idx, nil)
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(2))

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	_, err = idx.Create(txn, types.MakeDatums(int64(30)), 3)
	c.Assert(err, IsNil)
	key := tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(txn, tb, idx)
	c.Assert(err, NotNil)
	record1 := &RecordData{Handle: int64(3), Values: types.MakeDatums(int64(30))}
	diffMsg := newDiffRetError("index", record1, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40), (3, 31)
	_, err = idx.Create(txn, types.MakeDatums(int64(40)), 4)
	c.Assert(err, IsNil)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 3))
	setColValue(c, txn, key, types.NewDatum(int64(31)))
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(txn, tb, idx)
	c.Assert(err, NotNil)
	record2 := &RecordData{Handle: int64(3), Values: types.MakeDatums(int64(31))}
	diffMsg = newDiffRetError("index", record1, record2)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40), (5, 30)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 3))
	txn.Delete(key)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 5))
	setColValue(c, txn, key, types.NewDatum(int64(30)))
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = checkRecordAndIndex(txn, tb, idx)
	c.Assert(err, NotNil)
	record2 = &RecordData{Handle: int64(5), Values: types.MakeDatums(int64(30))}
	diffMsg = newDiffRetError("index", record1, record2)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (3, 30)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	txn.Delete(key)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 3))
	setColValue(c, txn, key, types.NewDatum(int64(30)))
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(txn, tb, idx)
	c.Assert(err, NotNil)
	record1 = &RecordData{Handle: int64(4), Values: types.MakeDatums(int64(40))}
	diffMsg = newDiffRetError("index", record1, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	err = idx.Delete(txn, types.MakeDatums(int64(40)), 4)
	c.Assert(err, IsNil)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(txn, tb, idx)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("index", nil, record1)
	c.Assert(err.Error(), DeepEquals, diffMsg)
}

func setColValue(c *C, txn kv.Transaction, key kv.Key, v types.Datum) {
	row := []types.Datum{v, {}}
	colIDs := []int64{2, 3}
	value, err := tablecodec.EncodeRow(row, colIDs)
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
}
