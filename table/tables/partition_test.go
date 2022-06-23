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
	"testing"

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	_, err := tk.Exec("select * from partition_basic partition (p5)")
	require.Error(t, err)

	_, err = tk.Exec("update partition_basic set id = 666 where id = 7")
	require.Error(t, err)
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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
		PartitionExpr() (*tables.PartitionExpr, error)
	}
	pe, err := tbl.(partitionExpr).PartitionExpr()
	require.NoError(t, err)

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists tu;")
	defer tk.MustExec("drop table if exists t2;")
	defer tk.MustExec("drop table if exists tu;")
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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

func TestIssue31721(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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

func TestPruneModeWarningInfo(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("set session tidb_partition_prune_mode = 'dynamic'")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 Please analyze all partition tables again for consistency between partition and global stats",
		"Warning 1105 Please avoid setting partition prune mode to dynamic at session level and set partition prune mode to dynamic at global level"))
	tk.MustExec("set global tidb_partition_prune_mode = 'dynamic'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Please analyze all partition tables again for consistency between partition and global stats"))
}
