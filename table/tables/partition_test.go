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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestPartitionBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(mockPumpClient{})
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
	tk.MustExecToErr("select * from partition_basic partition (p5)")

	tk.MustExecToErr("update partition_basic set id = 666 where id = 7")
	tk.MustExec("update partition_basic set id = 9 where id = 7")
	tk.MustExec("delete from partition_basic where id = 7")
	tk.MustExec("delete from partition_basic where id = 9")
	tk.MustExec("drop table partition_basic")
}

func TestPartitionAddRecord(t *testing.T) {
	createTable1 := `CREATE TABLE test.t1 (id int(11), index(id))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	_, err := tk.Session().Execute(ctx, "use test")
	require.NoError(t, err)
	_, err = tk.Session().Execute(ctx, "drop table if exists t1, t2;")
	require.NoError(t, err)
	_, err = tk.Session().Execute(ctx, createTable1)
	require.NoError(t, err)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tbInfo := tb.Meta()
	p0 := tbInfo.Partition.Definitions[0]
	require.Equal(t, model.NewCIStr("p0"), p0.Name)
	require.Nil(t, sessiontxn.NewTxn(ctx, tk.Session()))
	rid, err := tb.AddRecord(tk.Session(), types.MakeDatums(1))
	require.NoError(t, err)

	// Check that add record writes to the partition, rather than the table.
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	val, err := txn.Get(context.TODO(), tables.PartitionRecordKey(p0.ID, rid.IntValue()))
	require.NoError(t, err)
	require.Greater(t, len(val), 0)
	_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid.IntValue()))
	require.True(t, kv.ErrNotExist.Equal(err))

	// Cover more code.
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(7))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(12))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(16))
	require.NoError(t, err)

	// Make the changes visible.
	_, err = tk.Session().Execute(context.Background(), "commit")
	require.NoError(t, err)

	// Check index count equals to data count.
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id)").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id) where id > 6").Check(testkit.Rows("3"))

	// Value must locates in one partition.
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(22))
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
	_, err = tk.Session().Execute(context.Background(), "rollback")
	require.NoError(t, err)

	createTable2 := `CREATE TABLE test.t2 (id int(11))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p3 VALUES LESS THAN MAXVALUE
)`
	_, err = tk.Session().Execute(context.Background(), createTable2)
	require.NoError(t, err)
	require.Nil(t, sessiontxn.NewTxn(ctx, tk.Session()))
	tb, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(22))
	require.NoError(t, err)

	createTable3 := `create table test.t3 (id int) partition by range (id)
	(
       partition p0 values less than (10)
	)`
	_, err = tk.Session().Execute(context.Background(), createTable3)
	require.NoError(t, err)
	require.Nil(t, sessiontxn.NewTxn(ctx, tk.Session()))
	tb, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(11))
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(10))
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(0))
	require.NoError(t, err)

	createTable4 := `create table test.t4 (a int,b int) partition by range (a+b)
	(
	partition p0 values less than (10)
	);`
	_, err = tk.Session().Execute(context.Background(), createTable4)
	require.NoError(t, err)
	require.Nil(t, sessiontxn.NewTxn(ctx, tk.Session()))
	tb, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(1, 11))
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
}

func TestHashPartitionAddRecord(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(context.Background(), "use test")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), "drop table if exists t1;")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), "set @@session.tidb_enable_table_partition = '1';")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), `CREATE TABLE test.t1 (id int(11), index(id)) PARTITION BY HASH (id) partitions 4;`)
	require.NoError(t, err)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tbInfo := tb.Meta()
	p0 := tbInfo.Partition.Definitions[0]
	require.Nil(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	rid, err := tb.AddRecord(tk.Session(), types.MakeDatums(8))
	require.NoError(t, err)

	// Check that add record writes to the partition, rather than the table.
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	val, err := txn.Get(context.TODO(), tables.PartitionRecordKey(p0.ID, rid.IntValue()))
	require.NoError(t, err)
	require.Greater(t, len(val), 0)
	_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid.IntValue()))
	require.True(t, kv.ErrNotExist.Equal(err))

	// Cover more code.
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(-1))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(3))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session(), types.MakeDatums(6))
	require.NoError(t, err)

	// Make the changes visible.
	_, err = tk.Session().Execute(context.Background(), "commit")
	require.NoError(t, err)

	// Check index count equals to data count.
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id)").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id) where id > 2").Check(testkit.Rows("3"))

	// Test for partition expression is negative number.
	_, err = tk.Session().Execute(context.Background(), `CREATE TABLE test.t2 (id int(11), index(id)) PARTITION BY HASH (id) partitions 11;`)
	require.NoError(t, err)
	tb, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tbInfo = tb.Meta()
	for i := 0; i < 11; i++ {
		require.Nil(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
		rid, err = tb.AddRecord(tk.Session(), types.MakeDatums(-i))
		require.NoError(t, err)
		txn, err = tk.Session().Txn(true)
		require.NoError(t, err)
		val, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.Partition.Definitions[i].ID, rid.IntValue()))
		require.NoError(t, err)
		require.Greater(t, len(val), 0)
		_, err = txn.Get(context.TODO(), tables.PartitionRecordKey(tbInfo.ID, rid.IntValue()))
		require.True(t, kv.ErrNotExist.Equal(err))
	}
	_, err = tk.Session().Execute(context.Background(), "drop table if exists t1, t2;")
	require.NoError(t, err)
}

// TestPartitionGetPhysicalID tests partition.GetPhysicalID().
func TestPartitionGetPhysicalID(t *testing.T) {
	createTable1 := `CREATE TABLE test.t1 (id int(11), index(id))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(context.Background(), "Drop table if exists test.t1;")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), createTable1)
	require.NoError(t, err)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tbInfo := tb.Meta()
	ps := tbInfo.GetPartitionInfo()
	require.NotNil(t, ps)
	for _, pd := range ps.Definitions {
		p := tb.(table.PartitionedTable).GetPartition(pd.ID)
		require.NotNil(t, p)
		require.Equal(t, p.GetPhysicalID(), pd.ID)
	}
}

func TestGeneratePartitionExpr(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(context.Background(), "use test")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), "drop table if exists t1;")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), `create table t1 (id int)
							partition by range (id) (
							partition p0 values less than (4),
							partition p1 values less than (7),
							partition p3 values less than maxvalue)`)
	require.NoError(t, err)

	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	type partitionExpr interface {
		PartitionExpr() *tables.PartitionExpr
	}
	pe := tbl.(partitionExpr).PartitionExpr()

	upperBounds := []string{
		"lt(t1.id, 4)",
		"lt(t1.id, 7)",
		"1",
	}
	for i, expr := range pe.UpperBounds {
		require.Equal(t, upperBounds[i], expr.String())
	}
}

func TestLocateRangeColumnPartitionErr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
}

func TestLocateRangePartitionErr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t_range_locate (
		id int(20) NOT NULL AUTO_INCREMENT,
		data_date date NOT NULL,
		PRIMARY KEY (id, data_date)
	) PARTITION BY RANGE(id) (
		PARTITION p0 VALUES LESS THAN (1024),
		PARTITION p1 VALUES LESS THAN (4096)
	)`)

	_, err := tk.Exec("INSERT INTO t_range_locate VALUES (5000, '2019-04-04')")
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
}

func TestLocatePartitionWithExtraHandle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t_extra (
		id int(20) NOT NULL AUTO_INCREMENT,
		x int(10) not null,
		PRIMARY KEY (id, x)
	) PARTITION BY RANGE(id) (
		PARTITION p0 VALUES LESS THAN (1024),
		PARTITION p1 VALUES LESS THAN (4096)
	)`)
	tk.MustExec("INSERT INTO t_extra VALUES (1000, 1000), (2000, 2000)")
	tk.MustExec("set autocommit=0;")
	tk.MustQuery("select * from t_extra where id = 1000 for update").Check(testkit.Rows("1000 1000"))
	tk.MustExec("commit")
}

func TestMultiTableUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t_a (
		id int(20),
		data_date date
	) partition by hash(id) partitions 10`)
	tk.MustExec(`CREATE TABLE t_b (
		id int(20),
		data_date date
	) PARTITION BY RANGE(id) (
		PARTITION p0 VALUES LESS THAN (2),
		PARTITION p1 VALUES LESS THAN (4),
		PARTITION p2 VALUES LESS THAN (6)
	)`)
	tk.MustExec("INSERT INTO t_a VALUES (1, '2020-08-25'), (2, '2020-08-25'), (3, '2020-08-25'), (4, '2020-08-25'), (5, '2020-08-25')")
	tk.MustExec("INSERT INTO t_b VALUES (1, '2020-08-25'), (2, '2020-08-25'), (3, '2020-08-25'), (4, '2020-08-25'), (5, '2020-08-25')")
	tk.MustExec("update t_a, t_b set t_a.data_date = '2020-08-24',  t_a.data_date = '2020-08-23', t_a.id = t_a.id + t_b.id where t_a.id = t_b.id")
	tk.MustQuery("select id from t_a order by id").Check(testkit.Rows("2", "4", "6", "8", "10"))
}

func TestLocatePartitionSingleColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t_hash_locate (
		id int(20),
		data_date date
	) partition by hash(id) partitions 10`)

	tk.MustExec(`CREATE TABLE t_range (
		id int(10) NOT NULL,
		data_date date,
		PRIMARY KEY (id)
	) PARTITION BY RANGE(id) (
		PARTITION p0 VALUES LESS THAN (1),
		PARTITION p1 VALUES LESS THAN (2),
		PARTITION p2 VALUES LESS THAN (4)
	)`)

	tk.MustExec("INSERT INTO t_hash_locate VALUES (), (), (), ()")
	tk.MustQuery("SELECT count(*) FROM t_hash_locate PARTITION (p0)").Check(testkit.Rows("4"))
	tk.MustExec("INSERT INTO t_range VALUES (-1, NULL), (1, NULL), (2, NULL), (3, NULL)")
	tk.MustQuery("SELECT count(*) FROM t_range PARTITION (p0)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT count(*) FROM t_range PARTITION (p1)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT count(*) FROM t_range PARTITION (p2)").Check(testkit.Rows("2"))
	_, err := tk.Exec("INSERT INTO t_range VALUES (4, NULL)")
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
}

func TestLocatePartition(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON;")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("drop table if exists t;")

	tk.MustExec(`CREATE TABLE t (
    	id bigint(20) DEFAULT NULL,
    	type varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL
    	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    	PARTITION BY LIST COLUMNS(type)
    	(PARTITION push_event VALUES IN ("PushEvent"),
    	PARTITION watch_event VALUES IN ("WatchEvent")
    );`)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tks := []*testkit.TestKit{tk1, tk2, tk3}

	wg := util.WaitGroupWrapper{}
	exec := func(tk0 *testkit.TestKit) {
		tk0.MustExec("use test")
		tk0.MustQuery("desc select id, type from t where  type = 'WatchEvent';").Check(testkit.Rows("TableReader_7 10.00 root partition:watch_event data:Selection_6]\n[└─Selection_6 10.00 cop[tikv]  eq(test.t.type, \"WatchEvent\")]\n[  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	}

	run := func(num int) {
		tk := tks[num]
		wg.Run(func() {
			exec(tk)
		})
	}
	for i := 0; i < len(tks); i++ {
		run(i)
	}
	wg.Wait()
}

func TestTimeZoneChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
		"PARTITION BY RANGE (UNIX_TIMESTAMP(`creation_dt`))\n" +
		"(PARTITION `p5` VALUES LESS THAN (1578035400),\n" +
		" PARTITION `p6` VALUES LESS THAN (1578035700),\n" +
		" PARTITION `p7` VALUES LESS THAN (1578036000),\n" +
		" PARTITION `p8` VALUES LESS THAN (1578036300),\n" +
		" PARTITION `p9` VALUES LESS THAN (MAXVALUE))"))
	tk.MustExec("DROP TABLE timezone_test")

	// Note that the result of "show create table" varies with time_zone.
	tk.MustExec("SET @@time_zone = 'UTC'")
	tk.MustExec(createTable)
	tk.MustQuery("SHOW CREATE TABLE timezone_test").Check(testkit.Rows("timezone_test CREATE TABLE `timezone_test` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `creation_dt` timestamp DEFAULT CURRENT_TIMESTAMP\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (UNIX_TIMESTAMP(`creation_dt`))\n" +
		"(PARTITION `p5` VALUES LESS THAN (1578064200),\n" +
		" PARTITION `p6` VALUES LESS THAN (1578064500),\n" +
		" PARTITION `p7` VALUES LESS THAN (1578064800),\n" +
		" PARTITION `p8` VALUES LESS THAN (1578065100),\n" +
		" PARTITION `p9` VALUES LESS THAN (MAXVALUE))"))

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

func TestCreatePartitionTableNotSupport(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	_, err := tk.Exec(`create table t7 (a int) partition by range (mod((select * from t), 5)) (partition p1 values less than (1));`)
	require.True(t, dbterror.ErrPartitionFunctionIsNotAllowed.Equal(err))
	_, err = tk.Exec(`create table t7 (a int) partition by range (1 + (select * from t)) (partition p1 values less than (1));`)
	require.True(t, dbterror.ErrPartitionFunctionIsNotAllowed.Equal(err))
	_, err = tk.Exec(`create table t7 (a int) partition by range (a + row(1, 2, 3)) (partition p1 values less than (1));`)
	require.True(t, dbterror.ErrPartitionFunctionIsNotAllowed.Equal(err))
	_, err = tk.Exec(`create table t7 (a int) partition by range (-(select * from t)) (partition p1 values less than (1));`)
	require.True(t, dbterror.ErrPartitionFunctionIsNotAllowed.Equal(err))
}

func TestRangePartitionUnderNoUnsigned(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists tu;")
	defer tk.MustExec("drop table if exists t2;")
	defer tk.MustExec("drop table if exists tu;")
	tk.MustGetErrCode(`CREATE TABLE tu (c1 BIGINT UNSIGNED) PARTITION BY RANGE(c1 - 10) (
							PARTITION p0 VALUES LESS THAN (-5),
							PARTITION p1 VALUES LESS THAN (0),
							PARTITION p2 VALUES LESS THAN (5),
							PARTITION p3 VALUES LESS THAN (10),
							PARTITION p4 VALUES LESS THAN (MAXVALUE));`, mysql.ErrPartitionConstDomain)
	tk.MustExec("SET @@sql_mode='NO_UNSIGNED_SUBTRACTION';")
	tk.MustExec(`create table t2 (a bigint unsigned) partition by range (a) (
  						  partition p1 values less than (0),
  						  partition p2 values less than (1),
  						  partition p3 values less than (18446744073709551614),
  						  partition p4 values less than (18446744073709551615),
  						  partition p5 values less than maxvalue);`)
	tk.MustExec("insert into t2 values(10);")
	tk.MustGetErrCode(`CREATE TABLE tu (c1 BIGINT UNSIGNED) PARTITION BY RANGE(c1 - 10) (
							PARTITION p0 VALUES LESS THAN (-5),
							PARTITION p1 VALUES LESS THAN (0),
							PARTITION p2 VALUES LESS THAN (5),
							PARTITION p3 VALUES LESS THAN (10),
							PARTITION p4 VALUES LESS THAN (MAXVALUE));`, mysql.ErrPartitionConstDomain)
}

func TestIntUint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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

func TestHashPartitionAndConditionConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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

func TestHashPartitionInsertValue(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop tables if exists t4")
	tk.MustExec(`CREATE TABLE t4(
	a bit(1) DEFAULT NULL,
	b int(11) DEFAULT NULL
	) PARTITION BY HASH(a)
	PARTITIONS 3`)
	defer tk.MustExec("drop tables if exists t4")
	tk.MustExec("INSERT INTO t4 VALUES(0, 0)")
	tk.MustExec("INSERT INTO t4 VALUES(1, 1)")
	result := tk.MustQuery("SELECT * FROM t4 WHERE a = 1")
	result.Check(testkit.Rows("\x01 1"))
}

func TestIssue21574(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop tables if exists t_21574")
	tk.MustExec("create table t_21574 (`key` int, `table` int) partition by range columns (`key`) (partition p0 values less than (10));")
	tk.MustExec("drop table t_21574")
	tk.MustExec("create table t_21574 (`key` int, `table` int) partition by list columns (`key`) (partition p0 values in (10));")
	tk.MustExec("drop table t_21574")
	tk.MustExec("create table t_21574 (`key` int, `table` int) partition by list columns (`key`,`table`) (partition p0 values in ((1,1)));")
}

func TestIssue24746(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop tables if exists t_24746")
	tk.MustExec("create table t_24746 (a int, b varchar(60), c int, primary key(a)) partition by range(a) (partition p0 values less than (5),partition p1 values less than (10), partition p2 values less than maxvalue)")
	defer tk.MustExec("drop table t_24746")
	err := tk.ExecToErr("insert into t_24746 partition (p1) values(4,'ERROR, not matching partition p1',4)")
	require.True(t, table.ErrRowDoesNotMatchGivenPartitionSet.Equal(err))
	tk.MustExec("insert into t_24746 partition (p0) values(4,'OK, first row in correct partition',4)")
	err = tk.ExecToErr("insert into t_24746 partition (p0) values(4,'DUPLICATE, in p0',4) on duplicate key update a = a + 1, b = 'ERROR, not allowed to write to p1'")
	require.True(t, table.ErrRowDoesNotMatchGivenPartitionSet.Equal(err))
	// Actual bug, before the fix this was updating the row in p0 (deleting it in p0 and inserting in p1):
	err = tk.ExecToErr("insert into t_24746 partition (p1) values(4,'ERROR, not allowed to read from partition p0',4) on duplicate key update a = a + 1, b = 'ERROR, not allowed to read from p0!'")
	require.True(t, table.ErrRowDoesNotMatchGivenPartitionSet.Equal(err))
}

func TestIssue31629(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_list_partition = 1")
	tk.MustExec("create database Issue31629")
	defer tk.MustExec("drop database Issue31629")
	tk.MustExec("use Issue31629")
	// Test following partition types:
	// HASH, RANGE, LIST:
	// - directly on a single int column
	// - with expression on multiple columns
	// RANGE/LIST COLUMNS single column
	// RANGE/LIST COLUMNS -- Verify that only single column is allowed and no expression
	tests := []struct {
		create string
		fail   bool
		cols   []string
	}{
		{"(col1 int, col2 varchar(60), col3 int, primary key(col1)) partition by range(col1) (partition p0 values less than (5),partition p1 values less than (10), partition p2 values less than maxvalue)", false, []string{"col1"}},
		{"(Col1 int, col2 varchar(60), col3 int, primary key(Col1,col3)) partition by range(Col1+col3) (partition p0 values less than (5),partition p1 values less than (10), partition p2 values less than maxvalue)", false, []string{"Col1", "col3"}},
		{"(col1 int, col2 varchar(60), col3 int, primary key(col1)) partition by hash(col1) partitions 3", false, []string{"col1"}},
		{"(Col1 int, col2 varchar(60), col3 int, primary key(Col1,col3)) partition by hash(Col1+col3) partitions 3", false, []string{"Col1", "col3"}},
		{"(col1 int, col2 varchar(60), col3 int, primary key(col1)) partition by list(col1) (partition p0 values in (5,6,7,8,9),partition p1 values in (10,11,12,13,14), partition p2 values in (20,21,22,23,24))", false, []string{"col1"}},
		{"(Col1 int, col2 varchar(60), col3 int, primary key(Col1,col3)) partition by list(Col1+col3) (partition p0 values in (5,6,7,8,9),partition p1 values in (10,11,12,13,14), partition p2 values in (20,21,22,23,24))", false, []string{"Col1", "col3"}},
		{`(col1 int, col2 varchar(60), col3 int, primary key(col2)) partition by range columns (col2) (partition p0 values less than (""),partition p1 values less than ("MID"), partition p2 values less than maxvalue)`, false, []string{"col2"}},
		{`(col1 int, col2 varchar(60), col3 int, primary key(col2)) partition by range columns (col2,col3) (partition p0 values less than (""),partition p1 values less than ("MID"), partition p2 values less than maxvalue)`, true, nil},
		{`(col1 int, col2 varchar(60), col3 int, primary key(col2)) partition by range columns (col1+1) (partition p0 values less than (""),partition p1 values less than ("MID"), partition p2 values less than maxvalue)`, true, nil},
		{`(col1 int, col2 varchar(60), col3 int, primary key(col2)) partition by list columns (col2) (partition p0 values in ("","First"),partition p1 values in ("MID","Middle"), partition p2 values in ("Last","Unknown"))`, false, []string{"col2"}},
		{`(col1 int, col2 varchar(60), col3 int, primary key(col2)) partition by list columns (col2,col3) (partition p0 values in ("","First"),partition p1 values in ("MID","Middle"), partition p2 values in ("Last","Unknown"))`, true, nil},
		{`(col1 int, col2 varchar(60), col3 int, primary key(col2)) partition by list columns (col1+1) (partition p0 values in ("","First"),partition p1 values in ("MID","Middle"), partition p2 values in ("Last","Unknown"))`, true, nil},
	}

	for i, tt := range tests {
		createTable := "create table t1 " + tt.create
		res, err := tk.Exec(createTable)
		if res != nil {
			res.Close()
		}
		if err != nil {
			if tt.fail {
				continue
			}
		}
		require.Falsef(t, tt.fail, "test %d succeeded but was expected to fail! %s", i, createTable)
		require.NoError(t, err)
		tk.MustQuery("show warnings").Check(testkit.Rows())

		tb, err := dom.InfoSchema().TableByName(model.NewCIStr("Issue31629"), model.NewCIStr("t1"))
		require.NoError(t, err)
		tbp, ok := tb.(table.PartitionedTable)
		require.Truef(t, ok, "test %d does not generate a table.PartitionedTable: %s (%T, %+v)", i, createTable, tb, tb)
		colNames := tbp.GetPartitionColumnNames()
		checkNames := []model.CIStr{model.NewCIStr(tt.cols[0])}
		for i := 1; i < len(tt.cols); i++ {
			checkNames = append(checkNames, model.NewCIStr(tt.cols[i]))
		}
		require.ElementsMatchf(t, colNames, checkNames, "test %d %s", i, createTable)
		tk.MustExec("drop table t1")
	}
}

type compoundSQL struct {
	selectSQL        string
	point            bool
	batchPoint       bool
	pruned           bool
	executeExplain   bool
	usedPartition    []string
	notUsedPartition []string
	rowCount         int
}

type partTableCase struct {
	partitionbySQL string
	selectInfo     []compoundSQL
}

func executePartTableCase(t *testing.T, tk *testkit.TestKit, testCases []partTableCase,
	createSQL string, insertSQLs []string, dropSQL string) {
	for i, testCase := range testCases {
		// create table ... partition by key ...
		ddlSQL := createSQL + testCase.partitionbySQL
		fmt.Println(i, ":", ddlSQL)
		executeSQLWrapper(t, tk, ddlSQL)
		// insert data
		for _, insertsql := range insertSQLs {
			executeSQLWrapper(t, tk, insertsql)
		}
		// execute testcases
		for j, selInfo := range testCase.selectInfo {
			fmt.Println(j, ":", selInfo.selectSQL)
			tk.MustQuery(selInfo.selectSQL).Check(testkit.Rows(strconv.Itoa(selInfo.rowCount)))
			if selInfo.executeExplain {
				result := tk.MustQuery("EXPLAIN " + selInfo.selectSQL)
				if selInfo.point {
					result.CheckContain("Point_Get")
				}
				if selInfo.batchPoint {
					result.CheckContain("Batch_Point_Get")
				}
				if selInfo.pruned {
					for _, part := range selInfo.usedPartition {
						result.CheckContain(part)
					}
					for _, part := range selInfo.notUsedPartition {
						result.CheckNotContain(part)
					}
				}
			}
		}
		executeSQLWrapper(t, tk, dropSQL)
	}
}

func executeSQLWrapper(t *testing.T, tk *testkit.TestKit, SQLString string) {
	res, err := tk.Exec(SQLString)
	if res != nil {
		res.Close()
	}
	require.Nil(t, err)
}

func TestKeyPartitionTableBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database partitiondb")
	defer tk.MustExec("drop database partitiondb")
	tk.MustExec("use partitiondb")
	testCases := []struct {
		createSQL  string
		dropSQL    string
		insertSQL  string
		selectInfo []compoundSQL
	}{
		{
			createSQL: "CREATE TABLE tkey0 (col1 INT NOT NULL, col2 DATE NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL,UNIQUE KEY (col3)) PARTITION BY KEY(col3) PARTITIONS 4",
			insertSQL: "INSERT INTO tkey0 VALUES(1, '2023-02-22', 1, 1), (2, '2023-02-22', 2, 2), (3, '2023-02-22', 3, 3), (4, '2023-02-22', 4, 4)",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey0",
					false, false, false, false, []string{}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey0 PARTITION(p0)",
					false, false, false, false, []string{}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey0 PARTITION(p1)",
					false, false, false, false, []string{}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey0 PARTITION(p2)",
					false, false, false, false, []string{}, []string{}, 0,
				},
				{
					"SELECT count(*) FROM tkey0 PARTITION(p3)",
					false, false, false, false, []string{}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey0 WHERE col3 = 3",
					true, false, true, true, []string{"partition:p3"}, []string{"partition:p0", "partition:p1", "partition:p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey0 WHERE col3 = 3 or col3 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p3"}, []string{"partition:p1", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey0 WHERE col3 >1 AND col3 < 4",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p0", "partition:p2"}, 2,
				},
			},

			dropSQL: "DROP TABLE IF EXISTS tkey0",
		},
		{
			createSQL: "CREATE TABLE tkey7 (col1 INT NOT NULL, col2 DATE NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL,UNIQUE KEY (col3,col1)) PARTITION BY KEY(col3,col1) PARTITIONS 4",
			insertSQL: "INSERT INTO tkey7 VALUES(1, '2023-02-22', 1, 1), (1, '2023-02-22', 2, 1),(2, '2023-02-22', 2, 2), (3, '2023-02-22', 3, 3), (4, '2023-02-22', 4, 4),(4, '2023-02-22', 5, 4)",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey7",
					false, false, false, false, []string{}, []string{}, 6,
				},
				{
					"SELECT count(*) FROM tkey7 PARTITION(p0)",
					false, false, false, false, []string{}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey7 PARTITION(p1)",
					false, false, false, false, []string{}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey7 PARTITION(p2)",
					false, false, false, false, []string{}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey7 PARTITION(p3)",
					false, false, false, false, []string{}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col3 = 3",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col3 = 3 and col1 = 3",
					true, false, true, true, []string{"partition:p1"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col3 = 3 or col3 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col3 = 3 and col1 = 3 OR col3 = 4 and col1 = 4",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col1>1 and col3 >1 AND col3 < 4 and col1<3",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
			dropSQL: "DROP TABLE IF EXISTS tkey7",
		},
		{
			createSQL: "CREATE TABLE tkey8 (col1 INT NOT NULL, col2 DATE NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL,PRIMARY KEY (col3,col1)) PARTITION BY KEY(col3,col1) PARTITIONS 4",
			insertSQL: "INSERT INTO tkey8 VALUES(1, '2023-02-22', 111, 1), (1, '2023-02-22', 2, 1),(2, '2023-02-22', 218, 2), (3, '2023-02-22', 3, 3), (4, '2023-02-22', 4, 4),(4, '2023-02-22', 5, 4),(5, '2023-02-22', 5, 5),(5, '2023-02-22', 50, 2),(6, '2023-02-22', 62, 2),(60, '2023-02-22', 6, 5),(70, '2023-02-22', 50, 2),(80, '2023-02-22', 62, 2),(100, '2023-02-22', 62, 2),(2000, '2023-02-22', 6, 5),(400, '2023-02-22', 50, 2),(90, '2023-02-22', 62, 2)",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey8",
					false, false, false, false, []string{}, []string{}, 16,
				},
				{
					"SELECT count(*) FROM tkey8 PARTITION(p0)",
					false, false, false, false, []string{}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey8 PARTITION(p1)",
					false, false, false, false, []string{}, []string{}, 7,
				},
				{
					"SELECT count(*) FROM tkey8 PARTITION(p2)",
					false, false, false, false, []string{}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey8 PARTITION(p3)",
					false, false, false, false, []string{}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col3 = 3",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col3 = 3 and col1 = 3",
					true, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col3 = 3 or col3 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col3 = 3 and col1 = 3 OR col3 = 4 and col1 = 4",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col1>1 and col3 >1 AND col3 < 4 and col1<3",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 0,
				},
			},
			dropSQL: "DROP TABLE IF EXISTS tkey8",
		},
		{
			createSQL: "CREATE TABLE tkey6 (col1 INT NOT NULL, col2 DATE NOT NULL, col3 VARCHAR(12) NOT NULL, col4 INT NOT NULL,UNIQUE KEY (col3)) PARTITION BY KEY(col3) PARTITIONS 4",
			insertSQL: "INSERT INTO tkey6 VALUES(1, '2023-02-22', 'linpin', 1), (2, '2023-02-22', 'zhangsan', 2), (3, '2023-02-22', 'anqila', 3), (4, '2023-02-22', 'xingtian', 4),(1, '2023-02-22', 'renleifeng', 5), (2, '2023-02-22', 'peilin', 2),(1, '2023-02-22', 'abcdeeg', 7), (2, '2023-02-22', 'rpstdfed', 8)",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey6",
					false, false, false, false, []string{}, []string{}, 8,
				},
				{
					"SELECT count(*) FROM tkey6 PARTITION(p0)",
					false, false, false, false, []string{}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey6 PARTITION(p1)",
					false, false, false, false, []string{}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey6 PARTITION(p2)",
					false, false, false, false, []string{}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey6 PARTITION(p3)",
					false, false, false, false, []string{}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey6 WHERE col3 = 'linpin'",
					true, false, true, true, []string{"partition:p3"}, []string{"partition:p0", "partition:p1", "partition:p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey6 WHERE col3 = 'zhangsan' or col3 = 'linpin'",
					true, true, true, true, []string{}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey6 WHERE col3 > 'linpin' AND col3 < 'qing'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
			dropSQL: "DROP TABLE IF EXISTS tkey6",
		},
		{
			createSQL: "CREATE TABLE tkey2 (JYRQ INT not null,KHH VARCHAR(12) not null,ZJZH CHAR(14) not null,primary key (JYRQ, KHH, ZJZH))PARTITION BY KEY(KHH) partitions 4",
			insertSQL: "INSERT INTO tkey2 VALUES(1,'nanjing','025'),(2,'huaian','0517'),(3,'zhenjiang','0518'),(4,'changzhou','0519'),(5,'wuxi','0511'),(6,'suzhou','0512'),(7,'xuzhou','0513'),(8,'suqian','0513'),(9,'lianyungang','0514'),(10,'yangzhou','0515'),(11,'taizhou','0516'),(12,'nantong','0520'),(13,'yancheng','0521'),(14,'NANJING','025'),(15,'HUAIAN','0527'),(16,'ZHENJIANG','0529'),(17,'CHANGZHOU','0530')",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey2",
					false, false, false, false, []string{}, []string{}, 17,
				},
				{
					"SELECT count(*) FROM tkey2 PARTITION(p0)",
					false, false, false, false, []string{}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey2 PARTITION(p1)",
					false, false, false, false, []string{}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey2 PARTITION(p2)",
					false, false, false, false, []string{}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey2 PARTITION(p3)",
					false, false, false, false, []string{}, []string{}, 6,
				},
				{
					"SELECT count(*) FROM tkey2 WHERE KHH = 'huaian'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p0", "partition:p1", "partition:p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey2 WHERE KHH = 'huaian' or KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p0", "partition:p1", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey2 WHERE KHH > 'nanjing' AND KHH < 'suzhou'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 2,
				},
			},
			dropSQL: "DROP TABLE IF EXISTS tkey2",
		},
		{
			createSQL: "CREATE TABLE tkey5 (JYRQ INT not null,KHH VARCHAR(12) not null,ZJZH CHAR(14) not null,primary key (KHH, JYRQ, ZJZH))PARTITION BY KEY(KHH) partitions 4",
			insertSQL: "INSERT INTO tkey5 VALUES(1,'nanjing','025'),(2,'huaian','0517'),(3,'zhenjiang','0518'),(4,'changzhou','0519'),(5,'wuxi','0511'),(6,'suzhou','0512'),(7,'xuzhou','0513'),(8,'suqian','0513'),(9,'lianyungang','0514'),(10,'yangzhou','0515'),(11,'taizhou','0516'),(12,'nantong','0520'),(13,'yancheng','0521'),(14,'NANJING','025'),(15,'HUAIAN','0527'),(16,'ZHENJIANG','0529'),(17,'CHANGZHOU','0530')",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey5",
					false, false, false, false, []string{}, []string{}, 17,
				},
				{
					"SELECT count(*) FROM tkey5 PARTITION(p0)",
					false, false, false, false, []string{}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey5 PARTITION(p1)",
					false, false, false, false, []string{}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey5 PARTITION(p2)",
					false, false, false, false, []string{}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey5 PARTITION(p3)",
					false, false, false, false, []string{}, []string{}, 6,
				},
				{
					"SELECT count(*) FROM tkey5 WHERE KHH = 'huaian'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p0", "partition:p1", "partition:p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey5 WHERE KHH = 'huaian' or KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p0", "partition:p1", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey5 WHERE KHH > 'nanjing' AND KHH < 'suzhou'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 2,
				},
			},
			dropSQL: "DROP TABLE IF EXISTS tkey5",
		},
		{
			createSQL: "CREATE TABLE tkey4 (JYRQ INT not null,KHH VARCHAR(12) not null,ZJZH CHAR(14) not null,primary key (JYRQ, KHH, ZJZH))PARTITION BY KEY(JYRQ, KHH) partitions 4",
			insertSQL: "INSERT INTO tkey4 VALUES(1,'nanjing','025'),(2,'huaian','0517'),(3,'zhenjiang','0518'),(4,'changzhou','0519'),(5,'wuxi','0511'),(6,'suzhou','0512'),(7,'xuzhou','0513'),(8,'suqian','0513'),(9,'lianyungang','0514'),(10,'yangzhou','0515'),(11,'taizhou','0516'),(12,'nantong','0520'),(13,'yancheng','0521'),(14,'NANJING','025'),(15,'HUAIAN','0527'),(16,'ZHENJIANG','0529'),(17,'CHANGZHOU','0530'),(1,'beijing','010'),(2,'beijing','010'),(2,'zzzzwuhan','027')",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey4",
					false, false, false, false, []string{}, []string{}, 20,
				},
				{
					"SELECT count(*) FROM tkey4 PARTITION(p0)",
					false, false, false, false, []string{}, []string{}, 7,
				},
				{
					"SELECT count(*) FROM tkey4 PARTITION(p1)",
					false, false, false, false, []string{}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey4 PARTITION(p2)",
					false, false, false, false, []string{}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey4 PARTITION(p3)",
					false, false, false, false, []string{}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ = 2",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian' and JYRQ = 2",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian' and JYRQ = 2  or KHH = 'zhenjiang' and JYRQ = 3",
					false, false, true, true, []string{"partition:p0", "partition:p1"}, []string{"partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian' and JYRQ = 2  or KHH = 'zhenjiang' and JYRQ = 3 or KHH = 'HUAIAN' and JYRQ = 15",
					false, false, true, true, []string{"partition:p0", "partition:p1"}, []string{"partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian' or KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ = 2  OR  JYRQ = 3",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ = 2  OR  JYRQ = 3 OR JYRQ = 15",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ >6 AND JYRQ < 10",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ >6 and KHH>'lianyungang' AND JYRQ < 10 and KHH<'xuzhou'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
			dropSQL: "DROP TABLE IF EXISTS tkey4",
		},
		{
			createSQL: "CREATE TABLE tkey9 (JYRQ INT not null,KHH VARCHAR(12) not null,ZJZH CHAR(14) not null,primary key (JYRQ, KHH, ZJZH))PARTITION BY KEY(JYRQ, KHH, ZJZH) partitions 4",
			insertSQL: "INSERT INTO tkey9 VALUES(1,'nanjing','025'),(2,'huaian','0517'),(3,'zhenjiang','0518'),(4,'changzhou','0519'),(5,'wuxi','0511'),(6,'suzhou','0512'),(7,'xuzhou','0513'),(8,'suqian','0513'),(9,'lianyungang','0514'),(10,'yangzhou','0515'),(11,'taizhou','0516'),(12,'nantong','0520'),(13,'yancheng','0521'),(14,'NANJING','025'),(15,'HUAIAN','0527'),(16,'ZHENJIANG','0529'),(17,'CHANGZHOU','0530'),(1,'beijing','010'),(2,'beijing','010'),(2,'zzzzwuhan','027')",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey9",
					false, false, false, false, []string{}, []string{}, 20,
				},
				{
					"SELECT count(*) FROM tkey9 PARTITION(p0)",
					false, false, false, false, []string{}, []string{}, 6,
				},
				{
					"SELECT count(*) FROM tkey9 PARTITION(p1)",
					false, false, false, false, []string{}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey9 PARTITION(p2)",
					false, false, false, false, []string{}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey9 PARTITION(p3)",
					false, false, false, false, []string{}, []string{}, 8,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' and JYRQ = 2 and ZJZH = '0517'",
					true, false, true, true, []string{"partition:p0"}, []string{"partition:p3", "partition:p1", "partition:p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' and JYRQ = 2",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ = 2",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' and JYRQ = 2 and ZJZH='0517'  or KHH = 'zhenjiang' and JYRQ = 3 and ZJZH = '0518'",
					false, false, true, true, []string{"partition:p3", "partition:p0"}, []string{"partition:p1", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' and JYRQ = 2 and ZJZH='0517'  or KHH = 'zhenjiang' and JYRQ = 3 and ZJZH = '0518' or KHH = 'NANJING' and JYRQ = 14 and ZJZH = '025'",
					false, false, true, true, []string{"partition:p0", "partition:p3"}, []string{"partition:p2", "partition:p1"}, 3,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' or KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ = 2  OR  JYRQ = 3",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ = 2  OR  JYRQ = 3 OR JYRQ = 15",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ >6 AND JYRQ < 10",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ = 2 and KHH = 'huaian' OR JYRQ = 3 and KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 2,
				},
			},
			dropSQL: "DROP TABLE IF EXISTS tkey9",
		},
	}

	for i, testCase := range testCases {
		fmt.Println(i, ":", testCase.createSQL)
		executeSQLWrapper(t, tk, testCase.createSQL)
		executeSQLWrapper(t, tk, testCase.insertSQL)
		for j, selInfo := range testCase.selectInfo {
			fmt.Println(j, ":", selInfo.selectSQL)
			tk.MustQuery(selInfo.selectSQL).Check(testkit.Rows(strconv.Itoa(selInfo.rowCount)))
			if selInfo.executeExplain {
				result := tk.MustQuery("EXPLAIN " + selInfo.selectSQL)
				if selInfo.point {
					result.CheckContain("Point_Get")
				}
				if selInfo.batchPoint {
					result.CheckContain("Batch_Point_Get")
				}
				if selInfo.pruned {
					for _, part := range selInfo.usedPartition {
						result.CheckContain(part)
					}
				}
			}
		}
		executeSQLWrapper(t, tk, testCase.dropSQL)
	}
}

func TestKeyPartitionTableAllFeildType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database partitiondb3")
	defer tk.MustExec("drop database partitiondb3")
	tk.MustExec("use partitiondb3")
	// partition column is numeric family
	createSQL := "create table tkey_numeric(\n" +
		"id1 BIT(8) not null,\n" +
		"id2 TINYINT not null,\n" +
		"id3 BOOL not null,\n" +
		"id4 SMALLINT not null,\n" +
		"id5 MEDIUMINT not null,\n" +
		"id6 INT not null,\n" +
		"id7 BIGINT not null,\n" +
		"id8 DECIMAL(12,4) not null,\n" +
		"id9 FLOAT not null,\n" +
		"id10 DOUBLE not null,\n" +
		"name varchar(20),\n" +
		"primary key(id1,id2,id3,id4,id5,id6,id7,id8,id9,id10)\n" +
		")\n"
	dropSQL := "drop table tkey_numeric"
	insertSQLS := []string{
		"INSERT INTO tkey_numeric VALUES(1,1,0,1,1,1,1,1.1,120.1,367.45,'linpin'),(12,12,12,12,12,12,12,12.1,1220.1,3267.45,'anqila')",
		"INSERT INTO tkey_numeric VALUES(0,2,1,2,2,2,2,2.78,16.78,17.25,'ring'),(33,33,33,33,33,33,33,33.78,336.78,37.25,'black')",
		"INSERT INTO tkey_numeric VALUES(2,3,1,3,3,3,3,3.78,26.78,417.25,'liudehua'),(22,23,21,23,23,23,23,32.78,26.72,27.15,'chenchen')",
		"INSERT INTO tkey_numeric VALUES(3,3,2,4,4,4,4,4.78,46.48,89.35,'guofucheng'), (4,4,4,5,5,5,5,5.78,56.48,59.35,'zhangxuyou')",
		"INSERT INTO tkey_numeric VALUES(5,5,5,5,5,5,5,5.78,56.48,59.35,'xietingfeng'),(34,34,34,34,34,34,34,34.78,346.78,34.25,'dongxu')",
		"INSERT INTO tkey_numeric VALUES(250,120,120,250,250,258,348,38.78,186.48,719.35,'chenguanxi'),(35,35,35,250,35,35,35,35.78,356.48,35.35,'chenguanxi')",
	}
	testCases := []partTableCase{
		{
			partitionbySQL: "PARTITION BY KEY(id1) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 5,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id1 = 3",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id1 = 3 or id1 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p3"}, []string{"partition:p1", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id1 >1 AND id1 < 4",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p2", "partition:p0"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id2) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id2 = 3",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p0", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id2 = 3 or id2 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p3"}, []string{"partition:p1", "partition:p2"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id2 >1 AND id2 < 4",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p0", "partition:p2"}, 3,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id3) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p0", "partition:p1", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id3 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id3 = 5 or id3 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p2"}, []string{"partition:p1", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id3 >1 AND id3 < 4",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p2", "partition:p0"}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id4) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 5,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id4 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id4 = 5 or id4 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p2"}, []string{"partition:p1", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id4 >1 AND id4 < 4",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p0", "partition:p2"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id5) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p3", "partition:p0"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id5 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id5 = 5 or id5 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p2"}, []string{"partition:p1", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id5 >1 AND id5 < 4",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p2", "partition:p0"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id6) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id6 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id6 = 5 or id6 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p2"}, []string{"partition:p1", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id6 >1 AND id6 < 4",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p2", "partition:p0"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id7) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id7 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id7 = 5 or id7 = 4",
					false, false, true, true, []string{"partition:p0", "partition:p2"}, []string{"partition:p1", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id7 >1 AND id7 < 4",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p2", "partition:p0"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id8) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id8 = 1.1",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p2", "partition:p0", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id8 = 1.1 or id8 = 33.78",
					false, false, true, true, []string{"partition:p0", "partition:p1"}, []string{"partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id8 >1 AND id8 < 4",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 3,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id9) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id9 = 46.48",
					false, false, true, true, []string{}, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id9 = 46.48 or id9 = 336.78",
					false, false, true, true, []string{}, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id9 >45 AND id9 < 47",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id10) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id10 = 46.48",
					false, false, true, true, []string{}, []string{}, 0,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id10 = 46.48 or id10 = 336.78",
					false, false, true, true, []string{}, []string{}, 0,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id10 >366 AND id10 < 368",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
		},
	}
	executePartTableCase(t, tk, testCases, createSQL, insertSQLS, dropSQL)

	// partition column is date/time family
	createSQL2 := "create table tkey_datetime(\n" +
		"id1 DATE not null,\n" +
		"id2 TIME not null,\n" +
		"id3 DATETIME not null,\n" +
		"id4 TIMESTAMP not null,\n" +
		"id5 YEAR not null,\n" +
		"name varchar(20),\n" +
		"primary key(id1, id2, id3, id4, id5)\n" +
		")\n"
	dropSQL2 := "drop table tkey_datetime"
	insertSQLS2 := []string{
		"insert into tkey_datetime values('2012-04-10', '12:12:12', '2012-04-10 12:12:12', '2012-04-10 12:12:12.12345', 2012, 'linpin')",
		"insert into tkey_datetime values('2013-05-11', '13:13:13', '2013-05-11 13:13:13', '2013-05-11 13:13:13.43133', 2013, 'minghua')",
		"insert into tkey_datetime values('2014-06-12', '14:14:14', '2014-06-12 14:14:14', '2014-06-12 14:14:14.32344', 2014, 'oyangfeng')",
		"insert into tkey_datetime values('2015-07-13', '15:15:15', '2015-07-13 15:15:15', '2015-07-13 15:15:15.42544', 2015, 'pengdehuai')",
		"insert into tkey_datetime values('2021-08-14', '16:16:16', '2021-08-14 16:16:16', '2021-08-14 16:16:16.18945', 2021, 'shenwanshan')",
		"insert into tkey_datetime values('2022-12-23', '23:12:15', '2022-12-23 23:12:15', '2022-12-23 23:12:15.43133', 2022, 'tangchap')",
		"insert into tkey_datetime values('2023-01-12', '20:38:14', '2023-01-12 20:38:14', '2023-01-12 20:38:14.32344', 2023, 'xinyu')",
		"insert into tkey_datetime values('2018-07-13', '07:15:15', '2018-07-13 07:15:15', '2018-07-13 07:15:15.42544', 2018, 'zongyang')",
		"insert into tkey_datetime values('1980-01-30', '00:12:15', '1980-01-30 00:12:15', '1980-01-30 00:12:15.42544', 1980, 'MAYUWEI')",
		"insert into tkey_datetime values('1980-03-30', '00:13:15', '1980-03-30 00:13:15', '1980-03-30 00:13:15.42544', 1980, 'maqinwei')",
	}
	testCases2 := []partTableCase{
		{
			partitionbySQL: "PARTITION BY KEY(id1) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_datetime",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 10,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id1 = '2012-04-10'",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id1 = '2012-04-10' or id1 = '2018-07-13'",
					false, false, true, true, []string{"partition:p0", "partition:p2"}, []string{"partition:p1", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id1 >'2012-04-10' AND id1 < '2014-04-10'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id3) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_datetime",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 10,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id3 = '2012-04-10 12:12:12'",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id3 = '2012-04-10 12:12:12' or id3 = '2021-08-14 16:16:16'",
					false, false, true, true, []string{"partition:p3", "partition:p1"}, []string{"partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id3 >'2012-04-10 12:12:12' AND id3 < '2014-04-10 12:12:12'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id4) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_datetime",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 10,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id4 = '2012-04-10 12:12:12'",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id4 = '2012-04-10 12:12:12' or id4 = '2021-08-14 16:16:16'",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p0", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id4 >'2012-04-10 12:12:12' AND id4 < '2014-04-10 12:12:12'",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id5) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_datetime",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 10,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id5 = 2012",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id5 = 2012 or id5 = 2018",
					false, false, true, true, []string{"partition:p0", "partition:p2"}, []string{"partition:p1", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id5 >2012 AND id5 < 2014",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p3", "partition:p0"}, 1,
				},
			},
		},
	}
	executePartTableCase(t, tk, testCases2, createSQL2, insertSQLS2, dropSQL2)

	// partition column is string family
	createSQL3 := "create table tkey_string(\n" +
		"id1 CHAR(16) not null,\n" +
		"id2 VARCHAR(16) not null,\n" +
		"id3 BINARY(16) not null,\n" +
		"id4 VARBINARY(16) not null,\n" +
		"id5 BLOB not null,\n" +
		"id6 TEXT not null,\n" +
		"id7 ENUM('x-small', 'small', 'medium', 'large', 'x-large') not null,\n" +
		"id8 SET ('a', 'b', 'c', 'd') not null,\n" +
		"name varchar(16),\n" +
		"primary key(id1, id2, id3, id4, id7, id8)\n" +
		")\n"
	dropSQL3 := "drop table tkey_string"
	insertSQLS3 := []string{
		"INSERT INTO tkey_string VALUES('huaian','huaian','huaian','huaian','huaian','huaian','x-small','a','linpin')",
		"INSERT INTO tkey_string VALUES('nanjing','nanjing','nanjing','nanjing','nanjing','nanjing','small','b','linpin')",
		"INSERT INTO tkey_string VALUES('zhenjiang','zhenjiang','zhenjiang','zhenjiang','zhenjiang','zhenjiang','medium','c','linpin')",
		"INSERT INTO tkey_string VALUES('suzhou','suzhou','suzhou','suzhou','suzhou','suzhou','large','d','linpin')",
		"INSERT INTO tkey_string VALUES('wuxi','wuxi','wuxi','wuxi','wuxi','wuxi','x-large','a','linpin')",
	}
	testCases3 := []partTableCase{
		{
			partitionbySQL: "PARTITION BY KEY(id1) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id1 = 'huaian'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p0", "partition:p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id1 = 'huaian' or id1 = 'suzhou'",
					false, false, true, true, []string{"partition:p3", "partition:p0"}, []string{"partition:p1", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id1 >'huaian' AND id1 < 'suzhou'",
					false, false, true, true, []string{"partition:p1", "partition:p2", "partition:p0", "partition:p3"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id2) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id2 = 'huaian'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id2 = 'huaian' or id2 = 'suzhou'",
					false, false, true, true, []string{"partition:p3", "partition:p0"}, []string{"partition:p1", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id2 >'huaian' AND id2 < 'suzhou'",
					false, false, true, true, []string{"partition:p1", "partition:p2", "partition:p0", "partition:p3"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id3) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id3 = 0x73757A686F7500000000000000000000",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id3 = 0x73757A686F7500000000000000000000 or id3 = 0x6E616E6A696E67000000000000000000",
					false, false, true, true, []string{"partition:p0", "partition:p1"}, []string{"partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id3 >0x67756169616E00000000000000000000 AND id3 < 0x6E616E6A696E67000000000000000000",
					false, false, true, true, []string{"partition:p1", "partition:p0", "partition:p2", "partition:p3"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id4) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id4 = 0x68756169616E",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p0", "partition:p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id4 = 0x68756169616E or id4 = 0x73757A686F75",
					false, false, true, true, []string{"partition:p3", "partition:p0"}, []string{"partition:p1", "partition:p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id4 >0x73757A686F75 AND id4 < 0x78757869",
					false, false, true, true, []string{"partition:p1", "partition:p2", "partition:p0", "partition:p3"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id7) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id7 = 'x-small'",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id7 = 'x-small' or id7 = 'large'",
					false, false, true, true, []string{"partition:p0", "partition:p2"}, []string{"partition:p1", "partition:p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id7 > 'large' AND id7 < 'x-small'",
					false, false, true, true, []string{"partition:p1", "partition:p0", "partition:p3"}, []string{"partition:p2"}, 3,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id8) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0", "partition:p2", "partition:p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1", "partition:p0", "partition:p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id8 = 'a'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1", "partition:p2", "partition:p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id8 = 'a' or id8 = 'b'",
					false, false, true, true, []string{"partition:p1", "partition:p3"}, []string{"partition:p0", "partition:p2"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id8 > 'a' AND id8 < 'c'",
					false, false, true, true, []string{"partition:p1", "partition:p2", "partition:p0", "partition:p3"}, []string{}, 1,
				},
			},
		},
	}
	executePartTableCase(t, tk, testCases3, createSQL3, insertSQLS3, dropSQL3)
}

func TestKeyPartitionTableMixed(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database partitiondb2")
	defer tk.MustExec("drop database partitiondb2")
	tk.MustExec("use partitiondb2")
	// SHOW CREATE TABLE
	tk.MustExec("CREATE TABLE tkey1 (col1 INT NOT NULL, col2 DATE NOT NULL,col3 INT NOT NULL, col4 INT NOT NULL, UNIQUE KEY (col3))" +
		" PARTITION BY KEY(col3)" +
		"(PARTITION `p0`," +
		"PARTITION `p1`," +
		"PARTITION `p2`," +
		"PARTITION `p3`)")
	tk.MustQuery("show create table tkey1").Check(testkit.Rows("tkey1 CREATE TABLE `tkey1` (\n" +
		"  `col1` int(11) NOT NULL,\n" +
		"  `col2` date NOT NULL,\n" +
		"  `col3` int(11) NOT NULL,\n" +
		"  `col4` int(11) NOT NULL,\n" +
		"  UNIQUE KEY `col3` (`col3`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY KEY (`col3`) PARTITIONS 4"))

	// BLOB, JSON don't support key partition
	err := tk.ExecToErr("create table tkey_string(\n" +
		"id5 BLOB not null,\n" +
		"id6 TEXT not null,\n" +
		"name varchar(16)\n" +
		") PARTITION BY KEY(id5) partitions 4\n")
	require.Error(t, err)
	require.Regexp(t, "Field 'id5' is of a not allowed type for this type of partitioning", err)

	// BLOB, JSON don't support key partition
	err = tk.ExecToErr("create table tkey_string2(\n" +
		"id5 BLOB not null,\n" +
		"id6 TEXT not null,\n" +
		"name varchar(16)\n" +
		") PARTITION BY KEY(id6) partitions 4\n")
	require.Error(t, err)
	require.Regexp(t, "Field 'id6' is of a not allowed type for this type of partitioning", err)

	err = tk.ExecToErr("CREATE TABLE tkey_json (c1 JSON) PARTITION BY KEY(c1) partitions 4")
	require.Error(t, err)
	require.Regexp(t, "Field 'c1' is of a not allowed type for this type of partitioning", err)

	// It doesn't support LINEAR KEY partition
	tk.MustExec("CREATE TABLE tkey_linear (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR KEY(col3) PARTITIONS 5")
	result := tk.MustQuery("show warnings")
	result.CheckContain("LINEAR KEY is not supported, using non-linear KEY instead")

	// It will ignore ALGORITHM=1|2
	tk.MustExec("CREATE TABLE tkey_algorithm1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM=1 (col3) PARTITIONS 5")
	tk.MustExec("CREATE TABLE tkey_algorithm2 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM=2 (col3) PARTITIONS 5")

	err = tk.ExecToErr("CREATE TABLE tkey_algorithm3 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM=3 (col3) PARTITIONS 5")
	require.Error(t, err)
	require.Regexp(t, "You have an error in your SQL syntax", err)

	// Key partition can't be as subpartition
	tk.MustExec("CREATE TABLE tkey_subpartition1 (JYRQ INT not null,KHH VARCHAR(12) not null,ZJZH CHAR(14) not null,primary key (JYRQ, KHH, ZJZH))" +
		"PARTITION BY RANGE(JYRQ)\n" +
		"SUBPARTITION BY KEY(KHH) SUBPARTITIONS 2 \n" +
		"(\n" +
		"PARTITION p0 VALUES LESS THAN (8),\n" +
		"PARTITION p1 VALUES LESS THAN (16),\n" +
		"PARTITION p2 VALUES LESS THAN MAXVALUE\n" +
		")")
	result = tk.MustQuery("show warnings")
	result.CheckContain("Unsupported partition type RANGE, treat as normal table")

	// It ignores /*!50100 */ format
	tk.MustExec("CREATE TABLE tkey10 (`col1` int, `col2` char(5),`col3` date)" +
		"/*!50100 PARTITION BY KEY (col3) PARTITIONS 5 */")
	result = tk.MustQuery("show create table tkey10")
	result.Check(testkit.Rows("tkey10 CREATE TABLE `tkey10` (\n" +
		"  `col1` int(11) DEFAULT NULL,\n" +
		"  `col2` char(5) DEFAULT NULL,\n" +
		"  `col3` date DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY KEY (`col3`) PARTITIONS 5"))

	// It ignores /*!50100 */ format, but doesn't ignore specified partition names
	tk.MustExec("CREATE TABLE tkey11 (`col1` int, `col2` char(5),`col3` date)" +
		"/*!50100 PARTITION BY KEY (col1) PARTITIONS 4 \n" +
		"(PARTITION `pp0`,\n" +
		"PARTITION `pp1`,\n" +
		"PARTITION `pp2`,\n" +
		"PARTITION `pp3`)\n" +
		"*/")
	result = tk.MustQuery("show create table tkey11")
	result.Check(testkit.Rows("tkey11 CREATE TABLE `tkey11` (\n" +
		"  `col1` int(11) DEFAULT NULL,\n" +
		"  `col2` char(5) DEFAULT NULL,\n" +
		"  `col3` date DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY KEY (`col1`)\n" +
		"(PARTITION `pp0`,\n" +
		" PARTITION `pp1`,\n" +
		" PARTITION `pp2`,\n" +
		" PARTITION `pp3`)"))

	// It shows the comment defined in the ddl
	tk.MustExec("CREATE TABLE tkey12 (`col1` int, `col2` char(5),`col3` date)" +
		"PARTITION BY KEY (col1) \n" +
		"(PARTITION `pp0` comment 'huaian',\n" +
		"PARTITION `pp1` comment 'nanjing',\n" +
		"PARTITION `pp2` comment 'zhenjiang',\n" +
		"PARTITION `pp3` comment 'suzhou')\n")
	result = tk.MustQuery("show create table tkey12")
	result.Check(testkit.Rows("tkey12 CREATE TABLE `tkey12` (\n" +
		"  `col1` int(11) DEFAULT NULL,\n" +
		"  `col2` char(5) DEFAULT NULL,\n" +
		"  `col3` date DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY KEY (`col1`)\n" +
		"(PARTITION `pp0` COMMENT 'huaian',\n" +
		" PARTITION `pp1` COMMENT 'nanjing',\n" +
		" PARTITION `pp2` COMMENT 'zhenjiang',\n" +
		" PARTITION `pp3` COMMENT 'suzhou')"))

	// It shows the placement policy defined in the ddl
	tk.MustExec("drop placement policy if exists fivereplicas")
	tk.MustExec("CREATE PLACEMENT POLICY fivereplicas FOLLOWERS=4")
	tk.MustExec("CREATE TABLE tkey13 (`col1` int, `col2` char(5),`col3` date) placement policy fivereplicas\n" +
		"PARTITION BY KEY (col1) PARTITIONS 4")
	result = tk.MustQuery("show create table tkey13")
	result.Check(testkit.Rows("tkey13 CREATE TABLE `tkey13` (\n" +
		"  `col1` int(11) DEFAULT NULL,\n" +
		"  `col2` char(5) DEFAULT NULL,\n" +
		"  `col3` date DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`fivereplicas` */\n" +
		"PARTITION BY KEY (`col1`) PARTITIONS 4"))

	// The partition column can has null value
	tk.MustExec("CREATE TABLE tkey14 (`col1` int, `col2` int,`col3` int, col4 int)\n" +
		"PARTITION BY KEY (col3) PARTITIONS 4")
	tk.MustExec("INSERT INTO tkey14 values(20,1,1,1),(1,2,NULL,2),(3,3,3,3),(3,3,NULL,3),(4,4,4,4),(5,5,5,5),(6,6,null,6),(7,7,7,7),(8,8,8,8),(9,9,9,9),(10,10,10,5),(11,11,11,6),(12,12,12,12),(13,13,13,13),(14,14,null,14)")
	tk.MustQuery("SELECT count(*) FROM tkey14 WHERE col3 = NULL").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT count(*) FROM tkey14 WHERE col3 IS NULL").Check(testkit.Rows("4"))
	result = tk.MustQuery("EXPLAIN SELECT count(*) FROM tkey14 WHERE col3 IS NULL")
	result.CheckContain("partition:p1")
	result.MultiCheckNotContain([]string{"partition:p0", "partition:p2", "partition:p3"})

	tk.MustExec("CREATE TABLE tkey15 (`col1` int, col2 DATE NOT NULL,col3 VARCHAR(12), col4 int)\n" +
		"PARTITION BY KEY (col3) PARTITIONS 4")
	tk.MustExec("INSERT INTO tkey15 VALUES(1, '2023-02-22', 'linpin', 1), (2, '2023-02-22', NULL, 2), (3, '2023-02-22', 'anqila', 3), (4, '2023-02-22', NULL, 4)")
	result = tk.MustQuery("EXPLAIN SELECT count(*) FROM tkey15 WHERE col3 IS NULL")
	result.CheckContain("partition:p1")
	result.MultiCheckNotContain([]string{"partition:p0", "partition:p2", "partition:p3"})

	tk.MustExec("CREATE TABLE tkey12_2 (col1 INT, col2 INT ,col3 INT ,col4 INT , UNIQUE KEY(col2, col3)" +
		") PARTITION BY KEY(col2, col3) PARTITIONS 4")
	tk.MustExec("INSERT INTO tkey12_2 values(20,1,1,1),(1,2,NULL,2),(3,3,3,3),(3,3,NULL,3),(4,4,4,4)," +
		"(5,5,5,5), (6,6,null,6),(7,7,7,7),(8,8,8,8),(9,9,9,9),(10,10,10,5),(11,11,11,6),(12,12,12,12)," +
		"(13,13,13,13),(14,14,null,14)")
	result = tk.MustQuery("EXPLAIN SELECT * FROM tkey12_2 WHERE col2 = 2 and col3 IS NULL")
	result.MultiCheckNotContain([]string{"partition:p1", "partition:p0", "partition:p3"})
	tk.MustQuery("SELECT * FROM tkey12_2 WHERE col2 = 2 and col3 IS NULL").Check(testkit.Rows("1 2 <nil> 2"))
	result = tk.MustQuery("EXPLAIN SELECT * FROM tkey12_2 WHERE col2 = 2")
	result.MultiCheckContain([]string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"})
	tk.MustQuery("SELECT * FROM tkey12_2 WHERE col2 = 2").Check(testkit.Rows("1 2 <nil> 2"))
	tk.MustQuery("EXPLAIN SELECT * FROM tkey12_2 WHERE col2 = 2").MultiCheckContain([]string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"})
	tk.MustQuery("SELECT * FROM tkey12_2 WHERE col2 IS NULL")
	tk.MustQuery("EXPLAIN SELECT * FROM tkey12_2 WHERE col2 IS NULL").MultiCheckContain([]string{"partition:p0", "partition:p1", "partition:p2", "partition:p3"})
	// Get the partition information from information_schema.partitions
	result = tk.MustQuery("select PARTITION_NAME,PARTITION_ORDINAL_POSITION,PARTITION_METHOD,PARTITION_EXPRESSION " +
		"FROM information_schema.partitions where TABLE_NAME = 'tkey12_2'")
	result.Check(testkit.Rows("p0 1 KEY `col2`,`col3`", "p1 2 KEY `col2`,`col3`", "p2 3 KEY `col2`,`col3`", "p3 4 KEY `col2`,`col3`"))

	// This tests caculating the boundary partition ID when it prunes partition table
	tk.MustExec("create table tkey16 (a int) partition by key (a) partitions 12")
	tk.MustExec("insert into tkey16 values (0), (1), (2), (3)")
	tk.MustExec("insert into tkey16 select a + 4 from tkey16")
	tk.MustExec("insert into tkey16 select a + 8 from tkey16")
	tk.MustExec("select * from information_schema.partitions where partition_name is not null")
}

func TestKeyPartitionWithDifferentCharsets(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database partitiondb4")
	defer tk.MustExec("drop database partitiondb4")
	tk.MustExec("use partitiondb4")

	tk.MustExec("CREATE TABLE tkey29 (" +
		"col1 INT NOT NULL," +
		"col2 DATE NOT NULL," +
		"col3 VARCHAR(12) NOT NULL," +
		"col4 INT NOT NULL," +
		"UNIQUE KEY (col3)" +
		") CHARSET=utf8mb4 COLLATE=utf8mb4_bin " +
		"PARTITION BY KEY(col3) " +
		"PARTITIONS 4")
	// ignore tail spaces
	err := tk.ExecToErr("INSERT INTO tkey29 VALUES(1, '2023-02-22', 'linpin', 1), (1, '2023-02-22', 'linpin ', 5)")
	require.Regexp(t, "Duplicate entry 'linpin ' for key 'tkey29.col3'", err)
	// case sensitive
	tk.MustExec("INSERT INTO tkey29 VALUES(3, '2023-02-22', 'abc', 1), (4, '2023-02-22', 'ABC ', 5)")

	tk.MustExec("CREATE TABLE tkey30 (" +
		"col1 INT NOT NULL," +
		"col2 DATE NOT NULL," +
		"col3 VARCHAR(12) NOT NULL," +
		"col4 INT NOT NULL," +
		"UNIQUE KEY (col3)" +
		") CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci " +
		"PARTITION BY KEY(col3) " +
		"PARTITIONS 4")
	// case insensitive
	err = tk.ExecToErr("INSERT INTO tkey30 VALUES(1, '2023-02-22', 'linpin', 1), (1, '2023-02-22', 'LINPIN', 5)")
	require.Regexp(t, "Duplicate entry 'LINPIN' for key 'tkey30.col3'", err)
	// ignore tail spaces
	err = tk.ExecToErr("INSERT INTO tkey30 VALUES(1, '2023-02-22', 'linpin', 1), (1, '2023-02-22', 'LINPIN ', 5)")
	require.Regexp(t, "Duplicate entry 'LINPIN ' for key 'tkey30.col3'", err)

	tk.MustExec("CREATE TABLE tkey31 (" +
		"col1 INT NOT NULL," +
		"col2 DATE NOT NULL," +
		"col3 VARCHAR(12) NOT NULL," +
		"col4 INT NOT NULL," +
		"UNIQUE KEY (col3)" +
		") CHARSET=gbk COLLATE=gbk_chinese_ci " +
		"PARTITION BY KEY(col3) " +
		"PARTITIONS 4")
	err = tk.ExecToErr("INSERT INTO tkey31 VALUES(1, '2023-02-22', '刘德华', 1), (1, '2023-02-22', '刘德华 ', 5)")
	require.Regexp(t, "Duplicate entry '刘德华 ' for key 'tkey31.col3'", err)
	tk.MustExec("INSERT INTO tkey31 VALUES(1, '2023-02-22', '刘德华', 1), (5, '2023-02-22', '张学友', 5),(6, '2023-02-22', '艾伦', 6), (7, '2023-02-22', '宁采臣', 7)")
	tk.MustQuery("SELECT * FROM tkey31 partition(p0)").Check(testkit.Rows("1 2023-02-22 刘德华 1"))
	tk.MustQuery("SELECT * FROM tkey31 partition(p1)").Check(testkit.Rows("6 2023-02-22 艾伦 6"))
	tk.MustQuery("SELECT * FROM tkey31 partition(p2)").Check(testkit.Rows("5 2023-02-22 张学友 5"))
	tk.MustQuery("SELECT * FROM tkey31 partition(p3)").Check(testkit.Rows("7 2023-02-22 宁采臣 7"))
}

func TestIssue31721(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_list_partition=on;")
	tk.MustExec("drop tables if exists t_31721")
	tk.MustExec("CREATE TABLE `t_31721` (`COL1` char(1) NOT NULL) CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY LIST COLUMNS(`COL1`) " +
		"(PARTITION `P0` VALUES IN ('1')," +
		"PARTITION `P1` VALUES IN ('2')," +
		"PARTITION `P2` VALUES IN ('3'));")
	tk.MustExec("insert into t_31721 values ('1')")
	tk.MustExec("select * from t_31721 partition(p0, p1) where col1 != 2;")
}

func TestKeyPartitionTableDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database partitiondb3")
	defer tk.MustExec("drop database partitiondb3")
	tk.MustExec("use partitiondb3")
	tk.MustExec("CREATE TABLE tkey14 (\n" +
		"col1 INT NOT NULL," +
		"col2 INT NOT NULL," +
		"col3 INT NOT NULL," +
		"col4 INT NOT NULL," +
		"primary KEY (col1,col3)\n" +
		")" +
		"PARTITION BY KEY(col3) PARTITIONS 4")
	tk.MustExec("INSERT INTO tkey14 values(1,1,1,1),(1,1,2,2),(3,3,3,3),(3,3,4,3),(4,4,4,4),(5,5,5,5),(6,6,6,6),(7,7,7,7),(8,8,8,8),(9,9,9,9),(10,10,10,5),(11,11,11,6),(12,12,12,12),(13,13,13,13),(14,14,14,14)")

	tk.MustExec("CREATE TABLE tkey15 (\n" +
		"col1 INT NOT NULL," +
		"col2 INT NOT NULL," +
		"col3 INT NOT NULL," +
		"col4 INT NOT NULL," +
		"primary KEY (col1,col3)\n" +
		")")
	tk.MustExec("INSERT INTO tkey15 values (20,20,20,20)")

	tk.MustExec("CREATE TABLE tkey16 (\n" +
		"col1 INT NOT NULL," +
		"col2 INT NOT NULL," +
		"col3 INT NOT NULL," +
		"col4 INT NOT NULL," +
		"primary KEY (col1,col3)\n" +
		")" +
		"PARTITION BY KEY(col3) PARTITIONS 4")
	tk.MustExec("INSERT INTO tkey16 values(1,1,1,1),(1,1,2,2),(3,3,3,3),(3,3,4,3),(4,4,4,4),(5,5,5,5),(6,6,6,6),(7,7,7,7),(8,8,8,8),(9,9,9,9),(10,10,10,5),(11,11,11,6),(12,12,12,12),(13,13,13,13),(14,14,14,14)")

	err := tk.ExecToErr("ALTER TABLE tkey15 PARTITION BY KEY(col3) PARTITIONS 4")
	require.Regexp(t, "alter table partition is unsupported", err)
	err = tk.ExecToErr("ALTER TABLE tkey14 ADD PARTITION PARTITIONS 1")
	require.Regexp(t, "Unsupported add partitions", err)
	err = tk.ExecToErr("ALTER TABLE tkey14 DROP PARTITION p4")
	require.Regexp(t, "DROP PARTITION can only be used on RANGE/LIST partitions", err)
	tk.MustExec("ALTER TABLE tkey14 TRUNCATE PARTITION p3")
	tk.MustQuery("SELECT COUNT(*) FROM tkey14 partition(p3)").Check(testkit.Rows("0"))
	err = tk.ExecToErr("ALTER TABLE tkey16 COALESCE PARTITION 2")
	require.Regexp(t, "Unsupported coalesce partitions", err)
	tk.MustExec("ALTER TABLE tkey14 ANALYZE PARTITION p3")
	err = tk.ExecToErr("ALTER TABLE tkey14 CHECK PARTITION p2")
	require.Regexp(t, "Unsupported check partition", err)
	err = tk.ExecToErr("ALTER TABLE tkey14 OPTIMIZE PARTITION p2")
	require.Regexp(t, "Unsupported optimize partition", err)
	err = tk.ExecToErr("ALTER TABLE tkey14 REBUILD PARTITION p2")
	require.Regexp(t, "Unsupported rebuild partition", err)
	err = tk.ExecToErr("ALTER TABLE tkey14 EXCHANGE PARTITION p3 WITH TABLE tkey15")
	require.Regexp(t, "Unsupported partition type of table tkey14 when exchanging partition", err)

	err = tk.ExecToErr("ALTER TABLE tkey16 REORGANIZE PARTITION")
	require.Regexp(t, "Unsupported reorganize partition", err)
	err = tk.ExecToErr("ALTER TABLE tkey16 REORGANIZE PARTITION p0 INTO (PARTITION p0,PARTITION p1)")
	require.Regexp(t, "Unsupported reorganize partition", err)
	err = tk.ExecToErr("ALTER TABLE tkey16 REORGANIZE PARTITION p0 INTO (PARTITION p0)")
	require.Regexp(t, "Unsupported reorganize partition", err)
	err = tk.ExecToErr("ALTER TABLE tkey16 REORGANIZE PARTITION p0 INTO (PARTITION p4)")
	require.Regexp(t, "Unsupported reorganize partition", err)
	err = tk.ExecToErr("ALTER TABLE tkey16 REMOVE PARTITIONING")
	require.Regexp(t, "Unsupported remove partitioning", err)

	tk.MustExec("CREATE TABLE tkey17 (" +
		"id INT NOT NULL PRIMARY KEY," +
		"name VARCHAR(20)" +
		")" +
		"PARTITION BY KEY()" +
		"PARTITIONS 2")
	result := tk.MustQuery("show warnings")
	result.CheckContain("Unsupported partition type KEY, treat as normal table")
}

func TestPruneModeWarningInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("set session tidb_partition_prune_mode = 'dynamic'")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 Please analyze all partition tables again for consistency between partition and global stats",
		"Warning 1105 Please avoid setting partition prune mode to dynamic at session level and set partition prune mode to dynamic at global level"))
	tk.MustExec("set global tidb_partition_prune_mode = 'dynamic'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Please analyze all partition tables again for consistency between partition and global stats"))
}
