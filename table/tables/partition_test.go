// Copyright 2019 PingCAP, Inc.
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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

func (ts *testSuite) TestPartitionBasic(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustExec("use test")
	ctx := tk.Se
	ctx.GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(mockPumpClient{})
	tk.MustExec("set @@session.tidb_enable_table_partition = '1'")
	tk.MustExec(`CREATE TABLE partition_basic (id int(11), unique index(id))
PARTITION BY RANGE COLUMNS ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`)
	tk.MustExec("insert into partition_basic values(0)")
	tk.MustExec("insert into partition_basic values(2) on duplicate key update id = 1")
	tk.MustExec("update partition_basic set id = 7 where id = 0")

	tk.MustQuery("select * from partition_basic where id = 7").Check(testkit.Rows("7"))
	tk.MustQuery("select * from partition_basic partition (p1)").Check(testkit.Rows("7"))
	_, err := tk.Exec("select * from partition_basic partition (p5)")
	c.Assert(err, NotNil)

	_, err = tk.Exec("update partition_basic set id = 666 where id = 7")
	c.Assert(err, NotNil)
	tk.MustExec("update partition_basic set id = 9 where id = 7")
	tk.MustExec("delete from partition_basic where id = 7")
	tk.MustExec("delete from partition_basic where id = 9")
	tk.MustExec("drop table partition_basic")
}

func (ts *testSuite) TestPartitionAddRecord(c *C) {
	createTable1 := `CREATE TABLE test.t1 (id int(11), index(id))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	ctx := context.Background()
	_, err := ts.se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "drop table if exists t1, t2;")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, createTable1)
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tbInfo := tb.Meta()
	p0 := tbInfo.Partition.Definitions[0]
	c.Assert(p0.Name, Equals, model.NewCIStr("p0"))
	c.Assert(ts.se.NewTxn(ctx), IsNil)
	rid, err := tb.AddRecord(ts.se, types.MakeDatums(1))
	c.Assert(err, IsNil)

	// Check that add record writes to the partition, rather than the table.
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	val, err := txn.Get(context.TODO(), tables.PartitionRecordKey(p0.ID, rid))
	c.Assert(err, IsNil)
	c.Assert(len(val), Greater, 0)
	_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid))
	c.Assert(kv.ErrNotExist.Equal(err), IsTrue)

	// Cover more code.
	_, err = tb.AddRecord(ts.se, types.MakeDatums(7))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(12))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(16))
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
	_, err = tb.AddRecord(ts.se, types.MakeDatums(22))
	c.Assert(table.ErrNoPartitionForGivenValue.Equal(err), IsTrue)
	ts.se.Execute(context.Background(), "rollback")

	createTable2 := `CREATE TABLE test.t2 (id int(11))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p3 VALUES LESS THAN MAXVALUE
)`
	_, err = ts.se.Execute(context.Background(), createTable2)
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(ctx), IsNil)
	tb, err = ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(22))
	c.Assert(err, IsNil) // Insert into maxvalue partition.

	createTable3 := `create table test.t3 (id int) partition by range (id) 
	(
       partition p0 values less than (10)
	)`
	_, err = ts.se.Execute(context.Background(), createTable3)
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(ctx), IsNil)
	tb, err = ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(11))
	c.Assert(table.ErrNoPartitionForGivenValue.Equal(err), IsTrue)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(10))
	c.Assert(table.ErrNoPartitionForGivenValue.Equal(err), IsTrue)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(0))
	c.Assert(err, IsNil)

	createTable4 := `create table test.t4 (a int,b int) partition by range (a+b)
	(
	partition p0 values less than (10)
	);`
	_, err = ts.se.Execute(context.Background(), createTable4)
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(ctx), IsNil)
	tb, err = ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(1, 11))
	c.Assert(table.ErrNoPartitionForGivenValue.Equal(err), IsTrue)
}

func (ts *testSuite) TestHashPartitionAddRecord(c *C) {
	_, err := ts.se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "drop table if exists t1;")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "set @@session.tidb_enable_table_partition = '1';")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), `CREATE TABLE test.t1 (id int(11), index(id)) PARTITION BY HASH (id) partitions 4;`)
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tbInfo := tb.Meta()
	p0 := tbInfo.Partition.Definitions[0]
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	rid, err := tb.AddRecord(ts.se, types.MakeDatums(8))
	c.Assert(err, IsNil)

	// Check that add record writes to the partition, rather than the table.
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	val, err := txn.Get(context.TODO(), tables.PartitionRecordKey(p0.ID, rid))
	c.Assert(err, IsNil)
	c.Assert(len(val), Greater, 0)
	_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid))
	c.Assert(kv.ErrNotExist.Equal(err), IsTrue)

	// Cover more code.
	_, err = tb.AddRecord(ts.se, types.MakeDatums(-1))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(3))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ts.se, types.MakeDatums(6))
	c.Assert(err, IsNil)

	// Make the changes visible.
	_, err = ts.se.Execute(context.Background(), "commit")
	c.Assert(err, IsNil)

	// Check index count equals to data count.
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id)").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id) where id > 2").Check(testkit.Rows("3"))

	// Test for partition expression is negative number.
	_, err = ts.se.Execute(context.Background(), `CREATE TABLE test.t2 (id int(11), index(id)) PARTITION BY HASH (id) partitions 11;`)
	c.Assert(err, IsNil)
	tb, err = ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tbInfo = tb.Meta()
	for i := 0; i < 11; i++ {
		c.Assert(ts.se.NewTxn(context.Background()), IsNil)
		rid, err = tb.AddRecord(ts.se, types.MakeDatums(-i))
		c.Assert(err, IsNil)
		txn, err = ts.se.Txn(true)
		c.Assert(err, IsNil)
		val, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.Partition.Definitions[i].ID, rid))
		c.Assert(err, IsNil)
		c.Assert(len(val), Greater, 0)
		_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid))
		c.Assert(kv.ErrNotExist.Equal(err), IsTrue)
	}
	_, err = ts.se.Execute(context.Background(), "drop table if exists t1, t2;")
	c.Assert(err, IsNil)
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

	_, err := ts.se.Execute(context.Background(), "Drop table if exists test.t1;")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), createTable1)
	c.Assert(err, IsNil)
	tb, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tbInfo := tb.Meta()
	ps := tbInfo.GetPartitionInfo()
	c.Assert(ps, NotNil)
	for _, pd := range ps.Definitions {
		p := tb.(table.PartitionedTable).GetPartition(pd.ID)
		c.Assert(p, NotNil)
		c.Assert(pd.ID, Equals, p.GetPhysicalID())
	}
}

func (ts *testSuite) TestGeneratePartitionExpr(c *C) {
	_, err := ts.se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "drop table if exists t1;")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), `create table t1 (id int)
							partition by range (id) (
							partition p0 values less than (4),
							partition p1 values less than (7),
							partition p3 values less than maxvalue)`)
	c.Assert(err, IsNil)

	tbl, err := ts.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	type partitionExpr interface {
		PartitionExpr(ctx sessionctx.Context, columns []*expression.Column, names types.NameSlice) (*tables.PartitionExpr, error)
	}
	ctx := mock.NewContext()
	columns, names := expression.ColumnInfos2ColumnsAndNames(ctx, model.NewCIStr("test"), tbl.Meta().Name, tbl.Meta().Columns)
	pe, err := tbl.(partitionExpr).PartitionExpr(ctx, columns, names)
	c.Assert(err, IsNil)
	c.Assert(pe.Column.ID, Equals, int64(1))

	ranges := []string{
		"or(lt(test.t1.id, 4), isnull(test.t1.id))",
		"and(lt(test.t1.id, 7), ge(test.t1.id, 4))",
		"and(1, ge(test.t1.id, 7))",
	}
	upperBounds := []string{
		"lt(test.t1.id, 4)",
		"lt(test.t1.id, 7)",
		"1",
	}
	for i, expr := range pe.Ranges {
		c.Assert(expr.String(), Equals, ranges[i])
	}
	for i, expr := range pe.UpperBounds {
		c.Assert(expr.String(), Equals, upperBounds[i])
	}
}

func (ts *testSuite) TestLocateRangePartitionErr(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t_month_data_monitor (
		id int(20) NOT NULL AUTO_INCREMENT,
		data_date date NOT NULL,
		PRIMARY KEY (id, data_date)
	) PARTITION BY RANGE COLUMNS(data_date) (
		PARTITION p20190401 VALUES LESS THAN ('2019-04-02'),
		PARTITION p20190402 VALUES LESS THAN ('2019-04-03')
	)`)

	_, err := tk.Exec("INSERT INTO t_month_data_monitor VALUES (4, '2019-04-04')")
	c.Assert(table.ErrNoPartitionForGivenValue.Equal(err), IsTrue)
}
