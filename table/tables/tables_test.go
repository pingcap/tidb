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

package tables_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
	se    tidb.Session
}

func (ts *testSuite) SetUpSuite(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Check(err, IsNil)
	ts.store = store
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	ts.se, err = tidb.CreateSession(ts.store)
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestBasic(c *C) {
	_, err := ts.se.Execute("CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	c.Assert(ctx.NewTxn(), IsNil)
	dom := sessionctx.GetDomain(ctx)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().ID, Greater, int64(0))
	c.Assert(tb.Meta().Name.L, Equals, "t")
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Indices(), NotNil)
	c.Assert(string(tb.FirstKey()), Not(Equals), "")
	c.Assert(string(tb.IndexPrefix()), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")
	c.Assert(tables.FindIndexByColName(tb, "b"), NotNil)

	autoid, err := tb.AllocAutoID()
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))

	ctx.GetSessionVars().BinlogClient = binloginfo.GetPumpClient()
	ctx.GetSessionVars().InRestrictedSQL = false
	rid, err := tb.AddRecord(ctx, types.MakeDatums(1, "abc"))
	c.Assert(err, IsNil)
	c.Assert(rid, Greater, int64(0))
	row, err := tb.Row(ctx, rid)
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].GetInt64(), Equals, int64(1))

	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "aba"))
	c.Assert(err, NotNil)
	_, err = tb.AddRecord(ctx, types.MakeDatums(2, "abc"))
	c.Assert(err, NotNil)

	c.Assert(tb.UpdateRecord(ctx, rid, types.MakeDatums(1, "abc"), types.MakeDatums(1, "cba"), map[int]bool{0: false, 1: true}), IsNil)

	tb.IterRecords(ctx, tb.FirstKey(), tb.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		return true, nil
	})

	indexCnt := func() int {
		cnt, err1 := countEntriesWithPrefix(ctx, tb.IndexPrefix())
		c.Assert(err1, IsNil)
		return cnt
	}

	// RowWithCols test
	vals, err := tb.RowWithCols(ctx, 1, tb.Cols())
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 2)
	c.Assert(vals[0].GetInt64(), Equals, int64(1))
	cols := []*table.Column{tb.Cols()[1]}
	vals, err = tb.RowWithCols(ctx, 1, cols)
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 1)
	c.Assert(vals[0].GetBytes(), DeepEquals, []byte("cba"))

	// Make sure there is index data in the storage.
	c.Assert(indexCnt(), Greater, 0)
	c.Assert(tb.RemoveRecord(ctx, rid, types.MakeDatums(1, "cba")), IsNil)
	// Make sure index data is also removed after tb.RemoveRecord().
	c.Assert(indexCnt(), Equals, 0)
	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "abc"))
	c.Assert(err, IsNil)
	c.Assert(indexCnt(), Greater, 0)
	handle, found, err := tb.Seek(ctx, 0)
	c.Assert(handle, Equals, int64(1))
	c.Assert(found, Equals, true)
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)

	table.MockTableFromMeta(tb.Meta())
	alc := tb.Allocator()
	c.Assert(alc, NotNil)

	err = tb.RebaseAutoID(0, false)
	c.Assert(err, IsNil)
	handle, found, err = tb.Seek(nil, 0)
	c.Assert(handle, Equals, int64(0))
	c.Assert(found, Equals, false)
	c.Assert(err, IsNil)
}

func countEntriesWithPrefix(ctx context.Context, prefix []byte) (int, error) {
	cnt := 0
	err := util.ScanMetaWithPrefix(ctx.Txn(), prefix, func(k kv.Key, v []byte) bool {
		cnt++
		return true
	})
	return cnt, err
}

func (ts *testSuite) TestTypes(c *C) {
	_, err := ts.se.Execute("CREATE TABLE test.t (c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 text, c6 blob, c7 varchar(64), c8 time, c9 timestamp null default CURRENT_TIMESTAMP, c10 decimal(10,1))")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	dom := sessionctx.GetDomain(ctx)
	_, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("insert test.t values (1, 2, 3, 4, '5', '6', '7', '10:10:10', null, 1.4)")
	c.Assert(err, IsNil)
	rs, err := ts.se.Execute("select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	row, err := rs[0].Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, NotNil)
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute("CREATE TABLE test.t (c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned, c5 double, c6 bit(8))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("insert test.t values (1, 2, 3, 4, 5, 6)")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute("select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	row, err = rs[0].Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, NotNil)
	c.Assert(row.Data[5].GetMysqlBit(), Equals, types.Bit{Value: 6, Width: 8})
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute("CREATE TABLE test.t (c1 enum('a', 'b', 'c'))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("insert test.t values ('a'), (2), ('c')")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute("select c1 + 1 from test.t where c1 = 1")
	c.Assert(err, IsNil)
	row, err = rs[0].Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, NotNil)
	c.Assert(row.Data[0].GetFloat64(), DeepEquals, float64(2))
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestUniqueIndexMultipleNullEntries(c *C) {
	_, err := ts.se.Execute("CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	dom := sessionctx.GetDomain(ctx)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().ID, Greater, int64(0))
	c.Assert(tb.Meta().Name.L, Equals, "t")
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Indices(), NotNil)
	c.Assert(string(tb.FirstKey()), Not(Equals), "")
	c.Assert(string(tb.IndexPrefix()), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")
	c.Assert(tables.FindIndexByColName(tb, "b"), NotNil)

	autoid, err := tb.AllocAutoID()
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))
	c.Assert(ctx.NewTxn(), IsNil)
	_, err = tb.AddRecord(ctx, types.MakeDatums(1, nil))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ctx, types.MakeDatums(2, nil))
	c.Assert(err, IsNil)
	c.Assert(ctx.Txn().Rollback(), IsNil)
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestRowKeyCodec(c *C) {
	table := []struct {
		tableID int64
		h       int64
		ID      int64
	}{
		{1, 1234567890, 0},
		{2, 1, 0},
		{3, -1, 0},
		{4, -1, 1},
	}

	for _, t := range table {
		b := tablecodec.EncodeRowKeyWithHandle(t.tableID, t.h)
		tableID, handle, err := tablecodec.DecodeRecordKey(b)
		c.Assert(err, IsNil)
		c.Assert(tableID, Equals, t.tableID)
		c.Assert(handle, Equals, t.h)

		handle, err = tablecodec.DecodeRowKey(b)
		c.Assert(err, IsNil)
		c.Assert(handle, Equals, t.h)
	}

	// test error
	tbl := []string{
		"",
		"x",
		"t1",
		"t12345678",
		"t12345678_i",
		"t12345678_r1",
		"t12345678_r1234567",
	}

	for _, t := range tbl {
		_, err := tablecodec.DecodeRowKey(kv.Key(t))
		c.Assert(err, NotNil)
	}
}

func (ts *testSuite) TestUnsignedPK(c *C) {
	defer testleak.AfterTest(c)()
	ts.se.Execute("DROP TABLE IF EXISTS test.tPK")
	_, err := ts.se.Execute("CREATE TABLE test.tPK (a bigint unsigned primary key, b varchar(255))")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	dom := sessionctx.GetDomain(ctx)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tPK"))
	c.Assert(err, IsNil)
	c.Assert(ctx.NewTxn(), IsNil)
	rid, err := tb.AddRecord(ctx, types.MakeDatums(1, "abc"))
	c.Assert(err, IsNil)
	row, err := tb.Row(ctx, rid)
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].Kind(), Equals, types.KindUint64)
	c.Assert(ctx.Txn().Commit(), IsNil)
}

func (ts *testSuite) TestIterRecords(c *C) {
	defer testleak.AfterTest(c)()
	ts.se.Execute("DROP TABLE IF EXISTS test.tIter")
	_, err := ts.se.Execute("CREATE TABLE test.tIter (a int primary key, b int)")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("INSERT test.tIter VALUES (1, 2), (2, NULL)")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	c.Assert(ctx.NewTxn(), IsNil)
	dom := sessionctx.GetDomain(ctx)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tIter"))
	c.Assert(err, IsNil)
	totalCount := 0
	err = tb.IterRecords(ctx, tb.FirstKey(), tb.Cols(), func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
		totalCount++
		c.Assert(rec[0].IsNull(), IsFalse)
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(totalCount, Equals, 2)
	c.Assert(ctx.Txn().Commit(), IsNil)
}

func (ts *testSuite) TestTableFromMeta(c *C) {
	defer testleak.AfterTest(c)()
	_, err := ts.se.Execute("CREATE TABLE test.meta (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	c.Assert(ctx.NewTxn(), IsNil)
	dom := sessionctx.GetDomain(ctx)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("meta"))
	tbInfo := tb.Meta()
	tbInfo.Columns[0].GeneratedExprString = "test"
	tables.TableFromMeta(nil, tbInfo)
	tbInfo.Columns[0].State = model.StateNone
	tb, err = tables.TableFromMeta(nil, tbInfo)
	c.Assert(tb, IsNil)
	c.Assert(err, NotNil)
	tbInfo.State = model.StateNone
	tables.TableFromMeta(nil, tbInfo)
	c.Assert(tb, IsNil)
	c.Assert(err, NotNil)
}
