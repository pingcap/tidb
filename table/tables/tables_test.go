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
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	binlog "github.com/pingcap/tipb/go-binlog"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
	dom   *domain.Domain
	se    session.Session
}

func (ts *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, err := mockstore.NewMockTikvStore()
	c.Check(err, IsNil)
	ts.store = store
	ts.dom, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
	ts.se, err = session.CreateSession4Test(ts.store)
	c.Assert(err, IsNil)
	ctx := ts.se
	ctx.GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(mockPumpClient{})
	ctx.GetSessionVars().InRestrictedSQL = false
}

func (ts *testSuite) TearDownSuite(c *C) {
	ts.dom.Close()
	c.Assert(ts.store.Close(), IsNil)
	testleak.AfterTest(c)()
}

type mockPumpClient struct{}

func (m mockPumpClient) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq, opts ...grpc.CallOption) (*binlog.WriteBinlogResp, error) {
	return &binlog.WriteBinlogResp{}, nil
}

func (m mockPumpClient) PullBinlogs(ctx context.Context, in *binlog.PullBinlogReq, opts ...grpc.CallOption) (binlog.Pump_PullBinlogsClient, error) {
	return nil, nil
}

func (ts *testSuite) TestBasic(c *C) {
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().ID, Greater, int64(0))
	c.Assert(tb.Meta().Name.L, Equals, "t")
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Indices(), NotNil)
	c.Assert(string(tb.FirstKey()), Not(Equals), "")
	c.Assert(string(tb.IndexPrefix()), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")
	c.Assert(tables.FindIndexByColName(tb, "b"), NotNil)

	autoid, err := table.AllocAutoIncrementValue(context.Background(), tb, nil)
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))

	handle, err := tb.AllocHandle(nil)
	c.Assert(err, IsNil)
	c.Assert(handle, Greater, int64(0))

	ctx := ts.se
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

	c.Assert(tb.UpdateRecord(ctx, rid, types.MakeDatums(1, "abc"), types.MakeDatums(1, "cba"), []bool{false, true}), IsNil)

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
	_, err = ts.se.Execute(context.Background(), "drop table test.t")
	c.Assert(err, IsNil)

	table.MockTableFromMeta(tb.Meta())
	alc := tb.Allocator(nil)
	c.Assert(alc, NotNil)

	err = tb.RebaseAutoID(nil, 0, false)
	c.Assert(err, IsNil)
}

func countEntriesWithPrefix(ctx sessionctx.Context, prefix []byte) (int, error) {
	cnt := 0
	txn, err := ctx.Txn(true)
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = util.ScanMetaWithPrefix(txn, prefix, func(k kv.Key, v []byte) bool {
		cnt++
		return true
	})
	return cnt, err
}

func (ts *testSuite) TestTypes(c *C) {
	ctx := context.Background()
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.t (c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 text, c6 blob, c7 varchar(64), c8 time, c9 timestamp null default CURRENT_TIMESTAMP, c10 decimal(10,1))")
	c.Assert(err, IsNil)
	_, err = ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "insert test.t values (1, 2, 3, 4, '5', '6', '7', '10:10:10', null, 1.4)")
	c.Assert(err, IsNil)
	rs, err := ts.se.Execute(ctx, "select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	req := rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	c.Assert(rs[0].Close(), IsNil)
	_, err = ts.se.Execute(ctx, "drop table test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute(ctx, "CREATE TABLE test.t (c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned, c5 double, c6 bit(8))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "insert test.t values (1, 2, 3, 4, 5, 6)")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute(ctx, "select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	req = rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	row := req.GetRow(0)
	c.Assert(types.BinaryLiteral(row.GetBytes(5)), DeepEquals, types.NewBinaryLiteralFromUint(6, -1))
	c.Assert(rs[0].Close(), IsNil)
	_, err = ts.se.Execute(ctx, "drop table test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute(ctx, "CREATE TABLE test.t (c1 enum('a', 'b', 'c'))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "insert test.t values ('a'), (2), ('c')")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute(ctx, "select c1 + 1 from test.t where c1 = 1")
	c.Assert(err, IsNil)
	req = rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	c.Assert(req.GetRow(0).GetFloat64(0), DeepEquals, float64(2))
	c.Assert(rs[0].Close(), IsNil)
	_, err = ts.se.Execute(ctx, "drop table test.t")
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestUniqueIndexMultipleNullEntries(c *C) {
	ctx := context.Background()
	_, err := ts.se.Execute(ctx, "drop table if exists test.t")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().ID, Greater, int64(0))
	c.Assert(tb.Meta().Name.L, Equals, "t")
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Indices(), NotNil)
	c.Assert(string(tb.FirstKey()), Not(Equals), "")
	c.Assert(string(tb.IndexPrefix()), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")
	c.Assert(tables.FindIndexByColName(tb, "b"), NotNil)

	handle, err := tb.AllocHandle(nil)
	c.Assert(err, IsNil)
	c.Assert(handle, Greater, int64(0))

	autoid, err := table.AllocAutoIncrementValue(context.Background(), tb, nil)
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))

	sctx := ts.se
	c.Assert(sctx.NewTxn(ctx), IsNil)
	_, err = tb.AddRecord(sctx, types.MakeDatums(1, nil))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(sctx, types.MakeDatums(2, nil))
	c.Assert(err, IsNil)
	txn, err := sctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Rollback(), IsNil)
	_, err = ts.se.Execute(context.Background(), "drop table test.t")
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestRowKeyCodec(c *C) {
	tableVal := []struct {
		tableID int64
		h       int64
		ID      int64
	}{
		{1, 1234567890, 0},
		{2, 1, 0},
		{3, -1, 0},
		{4, -1, 1},
	}

	for _, t := range tableVal {
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
	ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tPK")
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.tPK (a bigint unsigned primary key, b varchar(255))")
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tPK"))
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	rid, err := tb.AddRecord(ts.se, types.MakeDatums(1, "abc"))
	c.Assert(err, IsNil)
	row, err := tb.Row(ts.se, rid)
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].Kind(), Equals, types.KindUint64)
	c.Assert(ts.se.StmtCommit(), IsNil)
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
}

func (ts *testSuite) TestIterRecords(c *C) {
	ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tIter")
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.tIter (a int primary key, b int)")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "INSERT test.tIter VALUES (-1, 2), (2, NULL)")
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tIter"))
	c.Assert(err, IsNil)
	totalCount := 0
	err = tb.IterRecords(ts.se, tb.FirstKey(), tb.Cols(), func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
		totalCount++
		c.Assert(rec[0].IsNull(), IsFalse)
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(totalCount, Equals, 2)
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
}

func (ts *testSuite) TestTableFromMeta(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE meta (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("meta"))
	c.Assert(err, IsNil)
	tbInfo := tb.Meta()

	// For test coverage
	tbInfo.Columns[0].GeneratedExprString = "a"
	tables.TableFromMeta(nil, tbInfo)

	tbInfo.Columns[0].GeneratedExprString = "test"
	tables.TableFromMeta(nil, tbInfo)
	tbInfo.Columns[0].State = model.StateNone
	tb, err = tables.TableFromMeta(nil, tbInfo)
	c.Assert(tb, IsNil)
	c.Assert(err, NotNil)
	tbInfo.State = model.StateNone
	tb, err = tables.TableFromMeta(nil, tbInfo)
	c.Assert(tb, IsNil)
	c.Assert(err, NotNil)

	tk.MustExec(`create table t_mock (id int) partition by range (id) (partition p0 values less than maxvalue)`)
	tb, err = ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t_mock"))
	c.Assert(err, IsNil)
	t := table.MockTableFromMeta(tb.Meta())
	_, ok := t.(table.PartitionedTable)
	c.Assert(ok, IsTrue)
	tk.MustExec("drop table t_mock")
	c.Assert(t.Type(), Equals, table.NormalTable)

	tk.MustExec("create table t_meta (a int) shard_row_id_bits = 15")
	tb, err = domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t_meta"))
	c.Assert(err, IsNil)
	_, err = tb.AllocHandle(tk.Se)
	c.Assert(err, IsNil)

	maxID := 1<<(64-15-1) - 1
	err = tb.RebaseAutoID(tk.Se, int64(maxID), false)
	c.Assert(err, IsNil)

	_, err = tb.AllocHandle(tk.Se)
	c.Assert(err, NotNil)
}

func (ts *testSuite) TestHiddenColumn(c *C) {
	tk := testkit.NewTestKit(c, ts.store)
	tk.MustExec("DROP DATABASE IF EXISTS test_hidden;")
	tk.MustExec("CREATE DATABASE test_hidden;")
	tk.MustExec("USE test_hidden;")
	tk.MustExec("CREATE TABLE hidden (a int primary key, b int as (a+1), c int, d int as (c+1), e int, f tinyint as (a+1));")
	tk.MustExec("insert into hidden values (1, default, 3, default, 5, default);")
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test_hidden"), model.NewCIStr("hidden"))
	c.Assert(err, IsNil)
	colInfo := tb.Meta().Columns
	// Set column b, d, f to hidden
	colInfo[1].Hidden = true
	colInfo[3].Hidden = true
	colInfo[5].Hidden = true
	tc := tb.(*tables.TableCommon)
	// Reset related caches
	tc.VisibleColumns = nil
	tc.WritableColumns = nil
	tc.HiddenColumns = nil

	// Basic test
	cols := tb.VisibleCols()
	c.Assert(table.FindCol(cols, "a"), NotNil)
	c.Assert(table.FindCol(cols, "b"), IsNil)
	c.Assert(table.FindCol(cols, "c"), NotNil)
	c.Assert(table.FindCol(cols, "d"), IsNil)
	c.Assert(table.FindCol(cols, "e"), NotNil)
	hiddenCols := tb.HiddenCols()
	c.Assert(table.FindCol(hiddenCols, "a"), IsNil)
	c.Assert(table.FindCol(hiddenCols, "b"), NotNil)
	c.Assert(table.FindCol(hiddenCols, "c"), IsNil)
	c.Assert(table.FindCol(hiddenCols, "d"), NotNil)
	c.Assert(table.FindCol(hiddenCols, "e"), IsNil)

	// Can't select with b and d and f
	tk.MustQuery("select * from hidden;").Check(testkit.Rows("1 3 5"))
	_, err = tk.Exec("select b from hidden;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'field list'")
	_, err = tk.Exec("select d from hidden;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'd' in 'field list'")
	err = tk.QueryToErr("select a, c, e from hidden;")
	c.Assert(err, IsNil)
	_, err = tk.Exec("select d from hidden;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'd' in 'field list'")
	_, err = tk.Exec("select * from hidden where b>1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'where clause'")
	_, err = tk.Exec("select * from hidden order by b;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'order clause'")
	_, err = tk.Exec("select * from hidden group by b;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'group statement'")

	// Can't update with b and d
	_, err = tk.Exec("update hidden set d=1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'd' in 'field_list'")
	_, err = tk.Exec("update hidden set a=1 where b=1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'where clause'")
	_, err = tk.Exec("update hidden set a=1 where c=2 order by b;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'order clause'")
	tk.MustExec("update hidden set a = 5;")
	colInfo[5].Hidden = false
	tc.VisibleColumns = nil
	tk.MustQuery("select * from hidden;").Check(testkit.Rows("5 3 5 6"))
	colInfo[5].Hidden = true
	tc.VisibleColumns = nil

	// Can't delete with b and d
	_, err = tk.Exec("delete from hidden where b=1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'where clause'")
	_, err = tk.Exec("delete from hidden order by d;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'd' in 'order clause'")

	// Can't drop column b and d
	_, err = tk.Exec("ALTER TABLE hidden DROP COLUMN b")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1091]column b doesn't exist")
	_, err = tk.Exec("ALTER TABLE hidden DROP COLUMN d")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1091]column d doesn't exist")

	// Test show create table
	tk.MustQuery("show create table hidden;").Check(testkit.Rows(
		"hidden CREATE TABLE `hidden` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `e` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}
