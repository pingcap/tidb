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

package tables_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testMemoryTableSuite{})

type testMemoryTableSuite struct {
	store kv.Storage
	se    tidb.Session
	tbl   table.Table
}

func (ts *testMemoryTableSuite) SetUpSuite(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Check(err, IsNil)
	ts.store = store
	ts.se, err = tidb.CreateSession(ts.store)
	c.Assert(err, IsNil)

	// create table
	tp1 := types.NewFieldType(mysql.TypeLong)
	col1 := &model.ColumnInfo{
		ID:        1,
		Name:      model.NewCIStr("a"),
		Offset:    0,
		FieldType: *tp1,
	}
	tp2 := types.NewFieldType(mysql.TypeVarchar)
	tp2.Flen = 255
	col2 := &model.ColumnInfo{
		ID:        2,
		Name:      model.NewCIStr("b"),
		Offset:    1,
		FieldType: *tp2,
	}

	tblInfo := &model.TableInfo{
		ID:      100,
		Name:    model.NewCIStr("t"),
		Columns: []*model.ColumnInfo{col1, col2},
	}
	alloc := autoid.NewMemoryAllocator(int64(10))
	ts.tbl, _ = tables.MemoryTableFromMeta(alloc, tblInfo)
}

func (ts *testMemoryTableSuite) TestMemoryBasic(c *C) {
	ctx := ts.se.(context.Context)
	tb := ts.tbl
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Meta().ID, Greater, int64(0))
	c.Assert(tb.Meta().Name.L, Equals, "t")
	c.Assert(tb.Indices(), IsNil)
	c.Assert(string(tb.FirstKey()), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")

	autoid, err := tb.AllocAutoID()
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))

	rid, err := tb.AddRecord(ctx, types.MakeDatums(1, "abc"))
	c.Assert(err, IsNil)
	row, err := tb.Row(ctx, rid)
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].GetInt64(), Equals, int64(1))

	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "aba"))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ctx, types.MakeDatums(2, "abc"))
	c.Assert(err, IsNil)

	tb.IterRecords(ctx, tb.FirstKey(), tb.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		return true, nil
	})

	// RowWithCols test
	vals, err := tb.RowWithCols(ctx, rid, tb.Cols())
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 2)
	c.Assert(vals[0].GetInt64(), Equals, int64(1))
	cols := []*table.Column{tb.Cols()[1]}
	vals, err = tb.RowWithCols(ctx, rid, cols)
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 1)
	c.Assert(vals[0].GetString(), Equals, "abc")

	c.Assert(tb.RemoveRecord(ctx, rid, types.MakeDatums(1, "cba")), IsNil)
	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "abc"))
	c.Assert(err, IsNil)
	tb.(*tables.MemoryTable).Truncate()
	_, err = tb.Row(ctx, rid)
	c.Assert(err, NotNil)

	// Basic test for MemoryTable
	tb.(*tables.MemoryTable).Seek(nil, 0)
	tb.(*tables.MemoryTable).WritableCols()
	tb.(*tables.MemoryTable).IndexPrefix()
	tb.(*tables.MemoryTable).UpdateRecord(nil, 0, nil, nil, nil)
	tb.(*tables.MemoryTable).Allocator()
	tb.(*tables.MemoryTable).RebaseAutoID(0, false)
}
