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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
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
	"golang.org/x/net/context"
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
}

func (ts *testSuite) TearDownSuite(c *C) {
	ts.dom.Close()
	c.Assert(ts.store.Close(), IsNil)
	testleak.AfterTest(c)()
}

func (ts *testSuite) TestBasic(c *C) {
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(), IsNil)
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

	autoid, err := tb.AllocAutoID(nil)
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))

	ctx := ts.se
	ctx.GetSessionVars().BinlogClient = binloginfo.GetPumpClient()
	ctx.GetSessionVars().InRestrictedSQL = false
	rid, err := tb.AddRecord(ctx, types.MakeDatums(1, "abc"), false)
	c.Assert(err, IsNil)
	c.Assert(rid, Greater, int64(0))
	row, err := tb.Row(ctx, rid)
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].GetInt64(), Equals, int64(1))

	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "aba"), false)
	c.Assert(err, NotNil)
	_, err = tb.AddRecord(ctx, types.MakeDatums(2, "abc"), false)
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
	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "abc"), false)
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
	err := util.ScanMetaWithPrefix(ctx.Txn(), prefix, func(k kv.Key, v []byte) bool {
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
	chk := rs[0].NewChunk()
	err = rs[0].Next(ctx, chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows() == 0, IsFalse)
	c.Assert(rs[0].Close(), IsNil)
	_, err = ts.se.Execute(ctx, "drop table test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute(ctx, "CREATE TABLE test.t (c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned, c5 double, c6 bit(8))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "insert test.t values (1, 2, 3, 4, 5, 6)")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute(ctx, "select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	chk = rs[0].NewChunk()
	err = rs[0].Next(ctx, chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows() == 0, IsFalse)
	row := chk.GetRow(0)
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
	chk = rs[0].NewChunk()
	err = rs[0].Next(ctx, chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows() == 0, IsFalse)
	c.Assert(chk.GetRow(0).GetFloat64(0), DeepEquals, float64(2))
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

	autoid, err := tb.AllocAutoID(nil)
	sctx := ts.se
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))
	c.Assert(sctx.NewTxn(), IsNil)
	_, err = tb.AddRecord(sctx, types.MakeDatums(1, nil), false)
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(sctx, types.MakeDatums(2, nil), false)
	c.Assert(err, IsNil)
	c.Assert(sctx.Txn().Rollback(), IsNil)
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
	c.Assert(ts.se.NewTxn(), IsNil)
	rid, err := tb.AddRecord(ts.se, types.MakeDatums(1, "abc"), false)
	c.Assert(err, IsNil)
	row, err := tb.Row(ts.se, rid)
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].Kind(), Equals, types.KindUint64)
	c.Assert(ts.se.Txn().Commit(context.Background()), IsNil)
}

func (ts *testSuite) TestIterRecords(c *C) {
	ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tIter")
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.tIter (a int primary key, b int)")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "INSERT test.tIter VALUES (-1, 2), (2, NULL)")
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(), IsNil)
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
	c.Assert(ts.se.Txn().Commit(context.Background()), IsNil)
}

func (ts *testSuite) TestTableFromMeta(c *C) {
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.meta (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(), IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("meta"))
	tbInfo := tb.Meta()
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
}

func (ts *testSuite) TestPartitionAddRecord(c *C) {
	createTable1 := `CREATE TABLE test.t1 (id int(11), index(id))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`

	_, err := ts.se.Execute(context.Background(), "set @@session.tidb_enable_table_partition=1")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), createTable1)
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	tbInfo := tb.Meta()
	p0 := tbInfo.Partition.Definitions[0]
	c.Assert(p0.Name, Equals, model.NewCIStr("p0"))
	c.Assert(ts.se.NewTxn(), IsNil)
	rid, err := tb.AddRecord(ts.se, types.MakeDatums(1), false)
	c.Assert(err, IsNil)

	// Check that add record writes to the partition, rather than the table.
	txn := ts.se.Txn()
	val, err := txn.Get(tables.PartitionRecordKey(p0.ID, rid))
	c.Assert(err, IsNil)
	c.Assert(len(val), Greater, 0)
	_, err = txn.Get(tables.PartitionRecordKey(tbInfo.ID, rid))
	c.Assert(kv.ErrNotExist.Equal(err), IsTrue)

	// Cover more code.
	_, err = tb.AddRecord(ts.se, types.MakeDatums(7), false)
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(12), false)
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(16), false)
	c.Assert(err, IsNil)

	// Make the changes visible.
	_, err = ts.se.Execute(context.Background(), "commit")
	c.Assert(err, IsNil)

	// Check index count equals to data count.
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id)").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id) where id > 6").Check(testkit.Rows("3"))

	// Value must locates in one partition.
	_, err = tb.AddRecord(ts.se, types.MakeDatums(22), false)
	c.Assert(table.ErrTrgInvalidCreationCtx.Equal(err), IsTrue)
	ts.se.Execute(context.Background(), "rollback")

	createTable2 := `CREATE TABLE test.t2 (id int(11))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p3 VALUES LESS THAN MAXVALUE
)`
	_, err = ts.se.Execute(context.Background(), createTable2)
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(), IsNil)
	tb, err = ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	tbInfo = tb.Meta()
	_, err = tb.AddRecord(ts.se, types.MakeDatums(22), false)
	c.Assert(err, IsNil) // Insert into maxvalue partition.
}

// TestPartitionGetPhysicalID tests partition.GetPhysicalID().
func (ts *testSuite) TestPartitionGetPhysicalID(c *C) {
	createTable1 := `CREATE TABLE test.t1 (id int(11), index(id))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`

	_, err := ts.se.Execute(context.Background(), "set @@session.tidb_enable_table_partition=1")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "Drop table if exists test.t1;")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), createTable1)
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	tbInfo := tb.Meta()
	ps := tbInfo.GetPartitionInfo()
	c.Assert(ps, NotNil)
	for _, pd := range ps.Definitions {
		p := tb.(table.PartitionedTable).GetPartition(pd.ID)
		c.Assert(p, NotNil)
		c.Assert(pd.ID, Equals, p.GetPhysicalID())
	}
}
