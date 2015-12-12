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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testInspectSuite{})

type testInspectSuite struct {
}

func (s *testInspectSuite) TestInspect(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory:test_inspect")
	c.Assert(err, IsNil)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil)

	t := meta.NewMeta(txn)

	dbInfo := &model.DBInfo{
		ID:   1,
		Name: model.NewCIStr("a"),
	}
	err = t.CreateDatabase(dbInfo)
	c.Assert(err, IsNil)

	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c"),
		ID:           0,
		Offset:       0,
		DefaultValue: 1,
		State:        model.StatePublic,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
	}
	idx := &model.IndexInfo{
		Name:   model.NewCIStr("c"),
		ID:     1,
		Unique: true,
		Columns: []*model.IndexColumn{{
			Name:   model.NewCIStr("c"),
			Offset: 0,
			Length: 255,
		}},
		State: model.StatePublic,
	}
	tbInfo := &model.TableInfo{
		ID:      1,
		Name:    model.NewCIStr("t"),
		State:   model.StatePublic,
		Columns: []*model.ColumnInfo{col},
		Indices: []*model.IndexInfo{idx},
	}
	err = t.CreateTable(dbInfo.ID, tbInfo)
	c.Assert(err, IsNil)

	owner := &model.Owner{OwnerID: "owner"}
	err = t.SetDDLOwner(owner)
	c.Assert(err, IsNil)
	dbInfo2 := &model.DBInfo{
		ID:    2,
		Name:  model.NewCIStr("b"),
		State: model.StateNone,
	}
	job := &model.Job{
		SchemaID: dbInfo2.ID,
		Type:     model.ActionCreateSchema,
	}
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	info, err := GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Owner, DeepEquals, owner)
	c.Assert(info.Job, DeepEquals, job)
	c.Assert(info.ReorgHandle, Equals, int64(0))
	txn.Commit()

	ctx := mock.NewContext()
	c.Assert(err, IsNil)
	ctx.Store = store
	variable.BindSessionVars(ctx)

	alloc := autoid.NewAllocator(store, dbInfo.ID)
	tb, err := tables.TableFromMeta(alloc, tbInfo)
	c.Assert(err, IsNil)
	indices := tb.Indices()
	_, err = tb.AddRecord(ctx, []interface{}{10}, 0)
	c.Assert(err, IsNil)
	ctx.FinishTxn(false)

	record := &RecordData{Handle: int64(1), Values: []interface{}{int64(10)}}
	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	records, _, err := ScanSnapshotTableData(store, ver, tb, int64(1), 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record})

	_, err = tb.AddRecord(ctx, []interface{}{20}, 0)
	c.Assert(err, IsNil)
	ctx.FinishTxn(false)
	txn, err = store.Begin()
	c.Assert(err, IsNil)

	records, nextHandle, err := ScanTableData(tb, txn, int64(1), 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record})
	records, nextHandle, err = ScanTableData(tb, txn, nextHandle, 1)
	c.Assert(err, IsNil)
	record.Handle = int64(2)
	record.Values = []interface{}{int64(20)}
	c.Assert(records, DeepEquals, []*RecordData{record})
	startHandle := nextHandle
	records, nextHandle, err = ScanTableData(tb, txn, startHandle, 1)
	c.Assert(records, IsNil)
	c.Assert(nextHandle, Equals, startHandle)
	c.Assert(err, IsNil)

	kvIndex := kv.NewKVIndex(tb.IndexPrefix(), indices[0].Name.L, indices[0].ID, indices[0].Unique)
	idxRows, nextVals, err := ScanIndexData(txn, kvIndex, []interface{}{int64(10)}, 1)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*IndexRow{{Handle: int64(1), Values: []interface{}{int64(10)}}})
	idxRows, nextVals, err = ScanIndexData(txn, kvIndex, nextVals, 1)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*IndexRow{{Handle: int64(2), Values: []interface{}{int64(20)}}})
	idxRows, nextVals, err = ScanIndexData(txn, kvIndex, nextVals, 1)
	c.Assert(idxRows, IsNil)
	c.Assert(nextVals, DeepEquals, []interface{}{nil})
	c.Assert(err, IsNil)
}
