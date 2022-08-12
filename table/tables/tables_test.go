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
	"math"
	"strconv"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
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
	"github.com/pingcap/tidb/util/testutil"
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
	store, err := mockstore.NewMockStore()
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

func firstKey(t table.Table) kv.Key {
	return tablecodec.EncodeRecordKey(t.RecordPrefix(), kv.IntHandle(math.MinInt64))
}

func indexPrefix(t table.PhysicalTable) kv.Key {
	return tablecodec.GenTableIndexPrefix(t.GetPhysicalID())
}

func seek(t table.PhysicalTable, ctx sessionctx.Context, h kv.Handle) (kv.Handle, bool, error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, false, err
	}
	recordPrefix := t.RecordPrefix()
	seekKey := tablecodec.EncodeRowKeyWithHandle(t.GetPhysicalID(), h)
	iter, err := txn.Iter(seekKey, recordPrefix.PrefixNext())
	if err != nil {
		return nil, false, err
	}
	if !iter.Valid() || !iter.Key().HasPrefix(recordPrefix) {
		// No more records in the table, skip to the end.
		return nil, false, nil
	}
	handle, err := tablecodec.DecodeRowKey(iter.Key())
	if err != nil {
		return nil, false, err
	}
	return handle, true, nil
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
	c.Assert(string(firstKey(tb)), Not(Equals), "")
	c.Assert(string(indexPrefix(tb.(table.PhysicalTable))), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")
	c.Assert(tables.FindIndexByColName(tb, "b"), NotNil)

	autoID, err := table.AllocAutoIncrementValue(context.Background(), tb, ts.se)
	c.Assert(err, IsNil)
	c.Assert(autoID, Greater, int64(0))

	handle, err := tables.AllocHandle(context.Background(), nil, tb)
	c.Assert(err, IsNil)
	c.Assert(handle.IntValue(), Greater, int64(0))

	ctx := ts.se
	rid, err := tb.AddRecord(ctx, types.MakeDatums(1, "abc"))
	c.Assert(err, IsNil)
	c.Assert(rid.IntValue(), Greater, int64(0))
	row, err := tables.RowWithCols(tb, ctx, rid, tb.Cols())
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].GetInt64(), Equals, int64(1))

	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "aba"))
	c.Assert(err, NotNil)
	_, err = tb.AddRecord(ctx, types.MakeDatums(2, "abc"))
	c.Assert(err, NotNil)

	c.Assert(tb.UpdateRecord(context.Background(), ctx, rid, types.MakeDatums(1, "abc"), types.MakeDatums(1, "cba"), []bool{false, true}), IsNil)

	err = tables.IterRecords(tb, ctx, tb.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		return true, nil
	})
	c.Assert(err, IsNil)

	indexCnt := func() int {
		cnt, err1 := countEntriesWithPrefix(ctx, indexPrefix(tb.(table.PhysicalTable)))
		c.Assert(err1, IsNil)
		return cnt
	}

	// RowWithCols test
	vals, err := tables.RowWithCols(tb, ctx, kv.IntHandle(1), tb.Cols())
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 2)
	c.Assert(vals[0].GetInt64(), Equals, int64(1))
	cols := []*table.Column{tb.Cols()[1]}
	vals, err = tables.RowWithCols(tb, ctx, kv.IntHandle(1), cols)
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
	handle, found, err := seek(tb.(table.PhysicalTable), ctx, kv.IntHandle(0))
	c.Assert(handle.IntValue(), Equals, int64(1))
	c.Assert(found, Equals, true)
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "drop table test.t")
	c.Assert(err, IsNil)

	table.MockTableFromMeta(tb.Meta())
	alc := tb.Allocators(nil).Get(autoid.RowIDAllocType)
	c.Assert(alc, NotNil)

	err = tb.RebaseAutoID(nil, 0, false, autoid.RowIDAllocType)
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
	c.Assert(string(firstKey(tb)), Not(Equals), "")
	c.Assert(string(indexPrefix(tb.(table.PhysicalTable))), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")
	c.Assert(tables.FindIndexByColName(tb, "b"), NotNil)

	handle, err := tables.AllocHandle(context.Background(), nil, tb)
	c.Assert(err, IsNil)
	c.Assert(handle.IntValue(), Greater, int64(0))

	autoid, err := table.AllocAutoIncrementValue(context.Background(), tb, ts.se)
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
		b := tablecodec.EncodeRowKeyWithHandle(t.tableID, kv.IntHandle(t.h))
		tableID, handle, err := tablecodec.DecodeRecordKey(b)
		c.Assert(err, IsNil)
		c.Assert(tableID, Equals, t.tableID)
		c.Assert(handle.IntValue(), Equals, t.h)

		handle, err = tablecodec.DecodeRowKey(b)
		c.Assert(err, IsNil)
		c.Assert(handle.IntValue(), Equals, t.h)
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
	_, err := ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tPK")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "CREATE TABLE test.tPK (a bigint unsigned primary key, b varchar(255))")
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tPK"))
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	rid, err := tb.AddRecord(ts.se, types.MakeDatums(1, "abc"))
	c.Assert(err, IsNil)
	pt := tb.(table.PhysicalTable)
	row, err := tables.RowWithCols(pt, ts.se, rid, tb.Cols())
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].Kind(), Equals, types.KindUint64)
	ts.se.StmtCommit()
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
}

func (ts *testSuite) TestIterRecords(c *C) {
	_, err := ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tIter")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "CREATE TABLE test.tIter (a int primary key, b int)")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "INSERT test.tIter VALUES (-1, 2), (2, NULL)")
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tIter"))
	c.Assert(err, IsNil)
	totalCount := 0
	err = tables.IterRecords(tb, ts.se, tb.Cols(), func(_ kv.Handle, rec []types.Datum, cols []*table.Column) (bool, error) {
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
	_, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("meta"))
	c.Assert(err, IsNil)
	tbInfo := tb.Meta().Clone()

	// For test coverage
	tbInfo.Columns[0].GeneratedExprString = "a"
	_, err = tables.TableFromMeta(nil, tbInfo)
	c.Assert(err, IsNil)

	tbInfo.Columns[0].GeneratedExprString = "test"
	_, err = tables.TableFromMeta(nil, tbInfo)
	c.Assert(err, NotNil)
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
	_, err = tables.AllocHandle(context.Background(), tk.Se, tb)
	c.Assert(err, IsNil)

	maxID := 1<<(64-15-1) - 1
	err = tb.RebaseAutoID(tk.Se, int64(maxID), false, autoid.RowIDAllocType)
	c.Assert(err, IsNil)

	_, err = tables.AllocHandle(context.Background(), tk.Se, tb)
	c.Assert(err, NotNil)
}

func (ts *testSuite) TestShardRowIDBitsStep(c *C) {
	tk := testkit.NewTestKit(c, ts.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists shard_t;")
	tk.MustExec("create table shard_t (a int) shard_row_id_bits = 15;")
	tk.MustExec("set @@tidb_shard_allocate_step=3;")
	tk.MustExec("insert into shard_t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11);")
	rows := tk.MustQuery("select _tidb_rowid from shard_t;").Rows()
	shards := make(map[int]struct{})
	for _, row := range rows {
		id, err := strconv.ParseUint(row[0].(string), 10, 64)
		c.Assert(err, IsNil)
		shards[int(id>>48)] = struct{}{}
	}
	c.Assert(len(shards), Equals, 4)
}

func (ts *testSuite) TestHiddenColumn(c *C) {
	tk := testkit.NewTestKit(c, ts.store)
	tk.MustExec("DROP DATABASE IF EXISTS test_hidden;")
	tk.MustExec("CREATE DATABASE test_hidden;")
	tk.MustExec("USE test_hidden;")
	tk.MustExec("CREATE TABLE t (a int primary key, b int as (a+1), c int, d int as (c+1) stored, e int, f tinyint as (a+1));")
	tk.MustExec("insert into t values (1, default, 3, default, 5, default);")
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test_hidden"), model.NewCIStr("t"))
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
	tc.FullHiddenColsAndVisibleColumns = nil

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
	colInfo[1].State = model.StateDeleteOnly
	colInfo[2].State = model.StateDeleteOnly
	fullHiddenColsAndVisibleColumns := tb.FullHiddenColsAndVisibleCols()
	c.Assert(table.FindCol(fullHiddenColsAndVisibleColumns, "a"), NotNil)
	c.Assert(table.FindCol(fullHiddenColsAndVisibleColumns, "b"), NotNil)
	c.Assert(table.FindCol(fullHiddenColsAndVisibleColumns, "c"), IsNil)
	c.Assert(table.FindCol(fullHiddenColsAndVisibleColumns, "d"), NotNil)
	c.Assert(table.FindCol(fullHiddenColsAndVisibleColumns, "e"), NotNil)
	// Reset schema states.
	colInfo[1].State = model.StatePublic
	colInfo[2].State = model.StatePublic

	// Test show create table
	tk.MustQuery("show create table t;").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `e` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test show (extended) columns
	tk.MustQuery("show columns from t").Check(testutil.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"c|int(11)|YES||<nil>|",
		"e|int(11)|YES||<nil>|"))
	tk.MustQuery("show extended columns from t").Check(testutil.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"b|int(11)|YES||<nil>|VIRTUAL GENERATED",
		"c|int(11)|YES||<nil>|",
		"d|int(11)|YES||<nil>|STORED GENERATED",
		"e|int(11)|YES||<nil>|",
		"f|tinyint(4)|YES||<nil>|VIRTUAL GENERATED"))

	// `SELECT` statement
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustQuery("select a, c, e from t;").Check(testkit.Rows("1 3 5"))

	// Can't use hidden columns in `SELECT` statement
	tk.MustGetErrMsg("select b from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("select b+1 from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("select b, c from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("select a, d from t;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("select d, b from t;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("select * from t where b > 1;", "[planner:1054]Unknown column 'b' in 'where clause'")
	tk.MustGetErrMsg("select * from t order by b;", "[planner:1054]Unknown column 'b' in 'order clause'")
	tk.MustGetErrMsg("select * from t group by b;", "[planner:1054]Unknown column 'b' in 'group statement'")

	// Can't use hidden columns in `INSERT` statement
	// 1. insert into ... values ...
	tk.MustGetErrMsg("insert into t values (1, 2, 3, 4, 5, 6);", "[planner:1136]Column count doesn't match value count at row 1")
	tk.MustGetErrMsg("insert into t(b) values (2)", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(b, c) values (2, 3);", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(a, d) values (1, 4);", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t(d, b) values (4, 2);", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t(a) values (b);", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(a) values (d+1);", "[planner:1054]Unknown column 'd' in 'field list'")
	// 2. insert into ... set ...
	tk.MustGetErrMsg("insert into t set b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set b = 2, c = 3;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1, d = 4;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set d = 4, b = 2;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = b;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = d + 1;", "[planner:1054]Unknown column 'd' in 'field list'")
	// 3. insert into ... on duplicated key update ...
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update b = 2, c = 3;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update c = 3, d = 4;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update d = 4, b = 2;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update c = b;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update c = d + 1;", "[planner:1054]Unknown column 'd' in 'field list'")
	// 4. replace into ... set ...
	tk.MustGetErrMsg("replace into t set a = 1, b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, b = 2, c = 3;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, d = 4;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, d = 4, b = 2;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, c = b;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, c = d + 1;", "[planner:1054]Unknown column 'd' in 'field list'")
	// 5. insert into ... select ...
	tk.MustExec("create table t1(a int, b int, c int, d int);")
	tk.MustGetErrMsg("insert into t1 select b from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select b+1 from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select b, c from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select a, d from t;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select d, b from t;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select a from t where b > 1;", "[planner:1054]Unknown column 'b' in 'where clause'")
	tk.MustGetErrMsg("insert into t1 select a from t order by b;", "[planner:1054]Unknown column 'b' in 'order clause'")
	tk.MustGetErrMsg("insert into t1 select a from t group by b;", "[planner:1054]Unknown column 'b' in 'group statement'")
	tk.MustExec("drop table t1")

	// `UPDATE` statement
	tk.MustGetErrMsg("update t set b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("update t set b = 2, c = 3;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("update t set a = 1, d = 4;", "[planner:1054]Unknown column 'd' in 'field list'")

	// FIXME: This sql return unknown column 'd' in MySQL
	tk.MustGetErrMsg("update t set d = 4, b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")

	tk.MustGetErrMsg("update t set a = b;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("update t set a = d + 1;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("update t set a=1 where b=1;", "[planner:1054]Unknown column 'b' in 'where clause'")
	tk.MustGetErrMsg("update t set a=1 where c=3 order by b;", "[planner:1054]Unknown column 'b' in 'order clause'")

	// `DELETE` statement
	tk.MustExec("delete from t;")
	tk.MustQuery("select count(*) from t;").Check(testkit.Rows("0"))
	tk.MustExec("insert into t values (1, 3, 5);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustGetErrMsg("delete from t where b = 1;", "[planner:1054]Unknown column 'b' in 'where clause'")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustGetErrMsg("delete from t order by d = 1;", "[planner:1054]Unknown column 'd' in 'order clause'")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))

	// `DROP COLUMN` statement
	tk.MustGetErrMsg("ALTER TABLE t DROP COLUMN b;", "[ddl:1091]column b doesn't exist")
	tk.MustQuery("show create table t;").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `e` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show extended columns from t").Check(testutil.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"b|int(11)|YES||<nil>|VIRTUAL GENERATED",
		"c|int(11)|YES||<nil>|",
		"d|int(11)|YES||<nil>|STORED GENERATED",
		"e|int(11)|YES||<nil>|",
		"f|tinyint(4)|YES||<nil>|VIRTUAL GENERATED"))
}

func (ts *testSuite) TestAddRecordWithCtx(c *C) {
	_, err := ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tRecord")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "CREATE TABLE test.tRecord (a bigint unsigned primary key, b varchar(255))")
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tRecord"))
	c.Assert(err, IsNil)
	defer func() {
		_, err := ts.se.Execute(context.Background(), "DROP TABLE test.tRecord")
		c.Assert(err, IsNil)
	}()

	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	_, err = ts.se.Txn(true)
	c.Assert(err, IsNil)
	recordCtx := tables.NewCommonAddRecordCtx(len(tb.Cols()))
	tables.SetAddRecordCtx(ts.se, recordCtx)
	defer tables.ClearAddRecordCtx(ts.se)

	records := [][]types.Datum{types.MakeDatums(uint64(1), "abc"), types.MakeDatums(uint64(2), "abcd")}
	for _, r := range records {
		rid, err := tb.AddRecord(ts.se, r)
		c.Assert(err, IsNil)
		row, err := tables.RowWithCols(tb.(table.PhysicalTable), ts.se, rid, tb.Cols())
		c.Assert(err, IsNil)
		c.Assert(len(row), Equals, len(r))
		c.Assert(row[0].Kind(), Equals, types.KindUint64)
	}

	i := 0
	err = tables.IterRecords(tb, ts.se, tb.Cols(), func(_ kv.Handle, rec []types.Datum, cols []*table.Column) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, len(records))

	ts.se.StmtCommit()
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
}

func (ts *testSuite) TestConstraintCheckForUniqueIndex(c *C) {
	// auto-commit
	tk := testkit.NewTestKit(c, ts.store)
	tk.MustExec("set @@autocommit = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ttt")
	tk.MustExec("create table ttt(id int(11) NOT NULL AUTO_INCREMENT,k int(11) NOT NULL DEFAULT '0',c char(120) NOT NULL DEFAULT '',PRIMARY KEY (id),UNIQUE KEY k_1 (k,c))")
	tk.MustExec("insert into ttt(k,c) values(1, 'tidb')")
	tk.MustExec("insert into ttt(k,c) values(2, 'tidb')")
	_, err := tk.Exec("update ttt set k=1 where id=2")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1-tidb' for key 'k_1'")
	tk.MustExec("rollback")

	// no auto-commit
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("set @@tidb_constraint_check_in_place = 0")
	tk.MustExec("begin")
	_, err = tk.Exec("update ttt set k=1 where id=2")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1-tidb' for key 'k_1'")
	tk.MustExec("rollback")

	tk.MustExec("set @@tidb_constraint_check_in_place = 1")
	tk.MustExec("begin")
	_, err = tk.Exec("update ttt set k=1 where id=2")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1-tidb' for key 'k_1'")
	tk.MustExec("rollback")

	// This test check that with @@tidb_constraint_check_in_place = 0, although there is not KV request for the unique index, the pessimistic lock should still be written.
	tk1 := testkit.NewTestKit(c, ts.store)
	tk2 := testkit.NewTestKit(c, ts.store)
	tk1.MustExec("set @@tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set @@tidb_constraint_check_in_place = 0")
	tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("use test")
	tk1.MustExec("begin")
	tk1.MustExec("update ttt set k=3 where id=2")

	ch := make(chan int, 2)
	go func() {
		tk2.MustExec("use test")
		_, err = tk2.Exec("insert into ttt(k,c) values(3, 'tidb')")
		c.Assert(err, IsNil)
		ch <- 2
	}()
	// Sleep 100ms for tk2 to execute, if it's not blocked, 2 should have been sent to the channel.
	time.Sleep(100 * time.Millisecond)
	ch <- 1
	_, err = tk1.Exec("commit")
	c.Assert(err, IsNil)
	// The data in channel is 1 means tk2 is blocked, that's the expected behavior.
	c.Assert(<-ch, Equals, 1)
}

func (ts *testSuite) TestViewColumns(c *C) {
	se, err := session.CreateSession4Test(ts.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk := testkit.NewTestKitWithSession(c, ts.store, se)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(20))")
	tk.MustExec("drop view if exists v")
	tk.MustExec("create view v as select * from t")
	tk.MustExec("drop view if exists va")
	tk.MustExec("create view va as select count(a) from t")
	testCases := []struct {
		query    string
		expected []string
	}{
		{"select data_type from INFORMATION_SCHEMA.columns where table_name = 'v'", []string{types.TypeToStr(mysql.TypeLong, ""), types.TypeToStr(mysql.TypeVarchar, "")}},
		{"select data_type from INFORMATION_SCHEMA.columns where table_name = 'va'", []string{types.TypeToStr(mysql.TypeLonglong, "")}},
	}
	for _, testCase := range testCases {
		tk.MustQuery(testCase.query).Check(testutil.RowsWithSep("|", testCase.expected...))
	}
	tk.MustExec("drop table if exists t")
	for _, testCase := range testCases {
		c.Assert(tk.MustQuery(testCase.query).Rows(), HasLen, 0)
		tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|",
			"Warning|1356|View 'test.v' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them",
			"Warning|1356|View 'test.va' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them"))
	}
}

func (ts *testSuite) TestConstraintCheckForOptimisticUntouched(c *C) {
	se, err := session.CreateSession4Test(ts.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk := testkit.NewTestKitWithSession(c, ts.store, se)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_optimistic_untouched_flag;")
	tk.MustExec(`create table test_optimistic_untouched_flag(c0 int, c1 varchar(20), c2 varchar(20), unique key uk(c0));`)
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, null, 'green');`)

	// Insert a row with duplicated entry on the unique key, the commit should fail.
	tk.MustExec("begin optimistic;")
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, 'red', 'white');`)
	tk.MustExec(`delete from test_optimistic_untouched_flag where c1 is null;`)
	tk.MustExec("update test_optimistic_untouched_flag set c2 = 'green' where c2 between 'purple' and 'white';")
	err = tk.ExecToErr("commit")
	c.Assert(err, NotNil)

	tk.MustExec("begin optimistic;")
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, 'red', 'white');`)
	tk.MustExec("update test_optimistic_untouched_flag set c2 = 'green' where c2 between 'purple' and 'white';")
	err = tk.ExecToErr("commit")
	c.Assert(err, NotNil)
}
