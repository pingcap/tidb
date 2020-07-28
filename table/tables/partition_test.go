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
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
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
	val, err := txn.Get(context.TODO(), tables.PartitionRecordKey(p0.ID, rid.IntValue()))
	c.Assert(err, IsNil)
	c.Assert(len(val), Greater, 0)
	_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid.IntValue()))
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
	val, err := txn.Get(context.TODO(), tables.PartitionRecordKey(p0.ID, rid.IntValue()))
	c.Assert(err, IsNil)
	c.Assert(len(val), Greater, 0)
	_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid.IntValue()))
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
		val, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.Partition.Definitions[i].ID, rid.IntValue()))
		c.Assert(err, IsNil)
		c.Assert(len(val), Greater, 0)
		_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid.IntValue()))
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
		PartitionExpr() (*tables.PartitionExpr, error)
	}
	pe, err := tbl.(partitionExpr).PartitionExpr()
	c.Assert(err, IsNil)

	upperBounds := []string{
		"lt(t1.id, 4)",
		"lt(t1.id, 7)",
		"1",
	}
	for i, expr := range pe.UpperBounds {
		c.Assert(expr.String(), Equals, upperBounds[i])
	}
}

func (ts *testSuite) TestLocateRangeColumnPartitionErr(c *C) {
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

func (ts *testSuite) TestLocateRangePartitionErr(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t_range_locate (
		id int(20) NOT NULL AUTO_INCREMENT,
		data_date date NOT NULL,
		PRIMARY KEY (id, data_date)
	) PARTITION BY RANGE(id) (
		PARTITION p0 VALUES LESS THAN (1024),
		PARTITION p1 VALUES LESS THAN (4096)
	)`)

	_, err := tk.Exec("INSERT INTO t_month_data_monitor VALUES (5000, '2019-04-04')")
	c.Assert(table.ErrNoPartitionForGivenValue.Equal(err), IsTrue)
}

func (ts *testSuite) TestTimeZoneChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustExec("use test")
	createTable := `CREATE TABLE timezone_test (
	id int(11) NOT NULL,
	creation_dt timestamp DEFAULT CURRENT_TIMESTAMP ) PARTITION BY RANGE ( ` + "UNIX_TIMESTAMP(`creation_dt`)" + ` )
( PARTITION p5 VALUES LESS THAN ( UNIX_TIMESTAMP('2020-01-03 15:10:00') ),
	PARTITION p6 VALUES LESS THAN ( UNIX_TIMESTAMP('2020-01-03 15:15:00') ),
	PARTITION p7 VALUES LESS THAN ( UNIX_TIMESTAMP('2020-01-03 15:20:00') ),
	PARTITION p8 VALUES LESS THAN ( UNIX_TIMESTAMP('2020-01-03 15:25:00') ),
	PARTITION p9 VALUES LESS THAN (MAXVALUE) )`
	tk.MustExec("SET @@time_zone = 'Asia/Shanghai'")
	tk.MustExec(createTable)
	tk.MustQuery("SHOW CREATE TABLE timezone_test").Check(testkit.Rows("timezone_test CREATE TABLE `timezone_test` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `creation_dt` timestamp DEFAULT CURRENT_TIMESTAMP\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE ( UNIX_TIMESTAMP(`creation_dt`) ) (\n" +
		"  PARTITION `p5` VALUES LESS THAN (1578035400),\n" +
		"  PARTITION `p6` VALUES LESS THAN (1578035700),\n" +
		"  PARTITION `p7` VALUES LESS THAN (1578036000),\n" +
		"  PARTITION `p8` VALUES LESS THAN (1578036300),\n" +
		"  PARTITION `p9` VALUES LESS THAN (MAXVALUE)\n)"))
	tk.MustExec("DROP TABLE timezone_test")

	// Note that the result of "show create table" varies with time_zone.
	tk.MustExec("SET @@time_zone = 'UTC'")
	tk.MustExec(createTable)
	tk.MustQuery("SHOW CREATE TABLE timezone_test").Check(testkit.Rows("timezone_test CREATE TABLE `timezone_test` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `creation_dt` timestamp DEFAULT CURRENT_TIMESTAMP\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE ( UNIX_TIMESTAMP(`creation_dt`) ) (\n" +
		"  PARTITION `p5` VALUES LESS THAN (1578064200),\n" +
		"  PARTITION `p6` VALUES LESS THAN (1578064500),\n" +
		"  PARTITION `p7` VALUES LESS THAN (1578064800),\n" +
		"  PARTITION `p8` VALUES LESS THAN (1578065100),\n" +
		"  PARTITION `p9` VALUES LESS THAN (MAXVALUE)\n)"))

	// Change time zone and insert data, check the data locates in the correct partition.
	tk.MustExec("SET @@time_zone = 'Asia/Shanghai'")
	tk.MustExec("INSERT INTO timezone_test VALUES (1,'2020-01-03 15:16:59')")
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p5)").Check(testkit.Rows("1 2020-01-03 15:16:59"))
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p6)").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p7)").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p8)").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p9)").Check(testkit.Rows())

	tk.MustExec("SET @@time_zone = 'UTC'")
	tk.MustExec("INSERT INTO timezone_test VALUES (1,'2020-01-03 15:16:59')")
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p5)").Check(testkit.Rows("1 2020-01-03 07:16:59"))
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p6)").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p7)").Check(testkit.Rows("1 2020-01-03 15:16:59"))
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p8)").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM timezone_test PARTITION (p9)").Check(testkit.Rows())
}

func (ts *testSuite) TestCreatePartitionTableNotSupport(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustExec("use test")
	_, err := tk.Exec(`create table t7 (a int) partition by range (mod((select * from t), 5)) (partition p1 values less than (1));`)
	c.Assert(ddl.ErrPartitionFunctionIsNotAllowed.Equal(err), IsTrue)
	_, err = tk.Exec(`create table t7 (a int) partition by range (1 + (select * from t)) (partition p1 values less than (1));`)
	c.Assert(ddl.ErrPartitionFunctionIsNotAllowed.Equal(err), IsTrue)
	_, err = tk.Exec(`create table t7 (a int) partition by range (a + row(1, 2, 3)) (partition p1 values less than (1));`)
	c.Assert(ddl.ErrPartitionFunctionIsNotAllowed.Equal(err), IsTrue)
	_, err = tk.Exec(`create table t7 (a int) partition by range (-(select * from t)) (partition p1 values less than (1));`)
	c.Assert(ddl.ErrPartitionFunctionIsNotAllowed.Equal(err), IsTrue)
}

func (ts *testSuite) TestIntUint(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustExec("use test")
	tk.MustExec(`create table t_uint (id bigint unsigned) partition by range (id) (
partition p0 values less than (4294967293),
partition p1 values less than (4294967296),
partition p2 values less than (484467440737095),
partition p3 values less than (18446744073709551614))`)
	tk.MustExec("insert into t_uint values (1)")
	tk.MustExec("insert into t_uint values (4294967294)")
	tk.MustExec("insert into t_uint values (4294967295)")
	tk.MustExec("insert into t_uint values (18446744073709551613)")
	tk.MustQuery("select * from t_uint where id > 484467440737095").Check(testkit.Rows("18446744073709551613"))
	tk.MustQuery("select * from t_uint where id = 4294967295").Check(testkit.Rows("4294967295"))
	tk.MustQuery("select * from t_uint where id < 4294967294").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t_uint where id >= 4294967293 order by id").Check(testkit.Rows("4294967294", "4294967295", "18446744073709551613"))

	tk.MustExec(`create table t_int (id bigint signed) partition by range (id) (
partition p0 values less than (-4294967293),
partition p1 values less than (-12345),
partition p2 values less than (0),
partition p3 values less than (484467440737095),
partition p4 values less than (9223372036854775806))`)
	tk.MustExec("insert into t_int values (-9223372036854775803)")
	tk.MustExec("insert into t_int values (-429496729312)")
	tk.MustExec("insert into t_int values (-1)")
	tk.MustExec("insert into t_int values (4294967295)")
	tk.MustExec("insert into t_int values (9223372036854775805)")
	tk.MustQuery("select * from t_int where id > 484467440737095").Check(testkit.Rows("9223372036854775805"))
	tk.MustQuery("select * from t_int where id = 4294967295").Check(testkit.Rows("4294967295"))
	tk.MustQuery("select * from t_int where id = -4294967294").Check(testkit.Rows())
	tk.MustQuery("select * from t_int where id < -12345 order by id desc").Check(testkit.Rows("-429496729312", "-9223372036854775803"))
}

func (ts *testSuite) TestHashPartitionAndConditionConflict(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3;")
	tk.MustExec("create table t1 (a int, b tinyint)  partition by range (a) (" +
		"    partition p0 values less than (10)," +
		"    partition p1 values less than (20)," +
		"    partition p2 values less than (30)," +
		"    partition p3 values less than (40)," +
		"    partition p4 values less than MAXVALUE" +
		");")

	tk.MustExec("insert into t1 values(NULL, NULL), (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24), (25, 25), (30, 30), (31, 31), (32, 32), (33, 33), (34, 34), (35, 35), (36, 36), (40, 40), (50, 50), (80, 80), (90, 90), (100, 100);")
	tk.MustExec("create table t2 (a int, b bigint) partition by hash(a) partitions 10;")
	tk.MustExec("insert into t2 values (NULL, NULL), (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23);")
	tk.MustQuery("select /*+ HASH_JOIN(t1, t2) */ * from t1 partition (p0) left join t2 partition (p1) on t1.a = t2.a where t1.a = 6 order by t1.a, t1.b, t2.a, t2.b;").
		Check(testkit.Rows("6 6 <nil> <nil>"))
	tk.MustQuery("select /*+ HASH_JOIN(t1, t2) */ * from t2 partition (p1) left join t1 partition (p0) on t2.a = t1.a where t2.a = 6 order by t1.a, t1.b, t2.a, t2.b;").
		Check(testkit.Rows())

	tk.MustQuery("select * from t2 partition (p1) where t2.a = 6;").Check(testkit.Rows())
}
