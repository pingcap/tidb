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

package partition

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	gotime "time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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
	rid, err := tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(1))
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
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(7))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(12))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(16))
	require.NoError(t, err)

	// Make the changes visible.
	_, err = tk.Session().Execute(context.Background(), "commit")
	require.NoError(t, err)

	// Check index count equals to data count.
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id)").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from t1 use index(id) where id > 6").Check(testkit.Rows("3"))

	// Value must locates in one partition.
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(22))
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
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(22))
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
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(11))
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(10))
	require.True(t, table.ErrNoPartitionForGivenValue.Equal(err))
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(0))
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
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(1, 11))
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
	rid, err := tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(8))
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
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(-1))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(3))
	require.NoError(t, err)
	_, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(6))
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
		rid, err = tb.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(-i))
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

func TestLocatePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")

	tk.MustExec(`CREATE TABLE t (
    	id bigint(20) DEFAULT NULL,
    	type varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL
    	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    	PARTITION BY LIST COLUMNS(type)
    	(PARTITION push_event VALUES IN ("PushEvent"),
    	PARTITION watch_event VALUES IN ("WatchEvent")
    )`)
	tk.MustExec(`insert into t values (1,"PushEvent"),(2,"WatchEvent"),(3, "WatchEvent")`)
	tk.MustExec(`analyze table t`)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tks := []*testkit.TestKit{tk1, tk2, tk3}

	wg := util.WaitGroupWrapper{}
	exec := func(tk0 *testkit.TestKit) {
		tk0.MustExec("use test")
		tk0.MustQuery("explain format = 'brief' select id, type from t where  type = 'WatchEvent';").Check(testkit.Rows(""+
			`TableReader 2.00 root partition:watch_event data:Selection`,
			`└─Selection 2.00 cop[tikv]  eq(test.t.type, "WatchEvent")`,
			`  └─TableFullScan 3.00 cop[tikv] table:t keep order:false`))
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

func TestExchangePartitionStates(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dbName := "partSchemaVer"
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec(`set @@global.tidb_enable_metadata_lock = ON`)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use " + dbName)
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use " + dbName)
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec("use " + dbName)
	tk.MustExec(`create table t (a int primary key, b varchar(255), key (b))`)
	tk.MustExec(`create table tp (a int primary key, b varchar(255), key (b)) partition by range (a) (partition p0 values less than (1000000), partition p1M values less than (2000000))`)
	tk.MustExec(`insert into t values (1, "1")`)
	tk.MustExec(`insert into tp values (2, "2")`)
	tk.MustExec(`analyze table t,tp`)
	tk.MustExec("BEGIN")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1 1"))
	tk.MustQuery(`select * from tp`).Check(testkit.Rows("2 2"))
	alterChan := make(chan error)
	go func() {
		// WITH VALIDATION is the default
		err := tk2.ExecToErr(`alter table tp exchange partition p0 with table t`)
		alterChan <- err
	}()
	waitFor := func(tableName, s string, pos int) {
		for {
			select {
			case alterErr := <-alterChan:
				require.Fail(t, "Alter completed unexpectedly", "With error %v", alterErr)
			default:
				// Alter still running
			}
			res := tk4.MustQuery(`admin show ddl jobs where db_name = '` + strings.ToLower(dbName) + `' and table_name = '` + tableName + `' and job_type = 'exchange partition'`).Rows()
			if len(res) == 1 && res[0][pos] == s {
				logutil.BgLogger().Info("Got state", zap.String("State", s))
				break
			}
			gotime.Sleep(10 * gotime.Millisecond)
		}
		dom := domain.GetDomain(tk.Session())
		// Make sure the table schema is the new schema.
		require.NoError(t, dom.Reload())
	}
	waitFor("t", "write only", 4)
	tk3.MustExec(`BEGIN`)
	tk3.MustExec(`insert into t values (4,"4")`)
	tk3.MustContainErrMsg(`insert into t values (1000004,"1000004")`, "[table:1748]Found a row not matching the given partition set")
	tk.MustExec(`insert into t values (5,"5")`)
	// This should fail the alter table!
	tk.MustExec(`insert into t values (1000005,"1000005")`)

	// MDL will block the alter to not continue until all clients
	// are in StateWriteOnly, which tk is blocking until it commits
	tk.MustExec(`COMMIT`)
	waitFor("t", "rollback done", 11)
	// MDL will block the alter from finish, tk is in 'rollbacked' schema version
	// but the alter is still waiting for tk3 to commit, before continuing
	tk.MustExec("BEGIN")
	tk.MustExec(`insert into t values (1000006,"1000006")`)
	tk.MustExec(`insert into t values (6,"6")`)
	tk3.MustExec(`insert into t values (7,"7")`)
	tk3.MustContainErrMsg(`insert into t values (1000007,"1000007")`,
		"[table:1748]Found a row not matching the given partition set")
	tk3.MustExec("COMMIT")
	require.ErrorContains(t, <-alterChan,
		"[ddl:1737]Found a row that does not match the partition")
	tk3.MustExec(`BEGIN`)
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows(
		"1 1", "1000005 1000005", "1000006 1000006", "5 5", "6 6"))
	tk.MustQuery(`select * from tp`).Sort().Check(testkit.Rows("2 2"))
	tk3.MustQuery(`select * from t`).Sort().Check(testkit.Rows(
		"1 1", "1000005 1000005", "4 4", "5 5", "7 7"))
	tk3.MustQuery(`select * from tp`).Sort().Check(testkit.Rows("2 2"))
	tk.MustContainErrMsg(`insert into t values (7,"7")`,
		"[kv:1062]Duplicate entry '7' for key 't.PRIMARY'")
	tk.MustExec(`insert into t values (8,"8")`)
	tk.MustExec(`insert into t values (1000008,"1000008")`)
	tk.MustExec(`insert into tp values (9,"9")`)
	tk.MustExec(`insert into tp values (1000009,"1000009")`)
	tk3.MustExec(`insert into t values (10,"10")`)
	tk3.MustExec(`insert into t values (1000010,"1000010")`)

	tk3.MustExec(`COMMIT`)
	tk.MustQuery(`show create table tp`).Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (1000000),\n" +
		" PARTITION `p1M` VALUES LESS THAN (2000000))"))
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec(`commit`)
	tk.MustExec(`insert into t values (11,"11")`)
	tk.MustExec(`insert into t values (1000011,"1000011")`)
	tk.MustExec(`insert into tp values (12,"12")`)
	tk.MustExec(`insert into tp values (1000012,"1000012")`)
}

// Test partition and non-partition both have check constraints.
func TestExchangePartitionCheckConstraintStates(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`create database check_constraint`)
	tk.MustExec(`set @@global.tidb_enable_check_constraint = 1`)
	tk.MustExec(`use check_constraint`)
	tk.MustExec(`create table nt (a int check (a > 75) not ENFORCED, b int check (b > 50) ENFORCED)`)
	tk.MustExec(`create table pt (a int check (a < 75) ENFORCED, b int check (b < 75) ENFORCED) partition by range (a) (partition p0 values less than (50), partition p1 values less than (100) )`)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use check_constraint`)
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec(`use check_constraint`)
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec(`use check_constraint`)
	// TODO: error message to check.
	errMsg := "[table:3819]Check constraint"

	tk2.MustExec("begin")
	// Get table mdl.
	tk2.MustQuery(`select * from nt`).Check(testkit.Rows())
	tk2.MustQuery(`select * from pt`).Check(testkit.Rows())
	alterChan := make(chan error)
	go func() {
		err := tk3.ExecToErr(`alter table pt exchange partition p1 with table nt`)
		alterChan <- err
	}()
	waitFor := func(tableName, s string, pos int) {
		for {
			select {
			case alterErr := <-alterChan:
				require.Fail(t, "Alter completed unexpectedly", "With error %v", alterErr)
			default:
				// Alter still running
			}
			res := tk4.MustQuery(`admin show ddl jobs where db_name = 'check_constraint' and table_name = '` + tableName + `' and job_type = 'exchange partition'`).Rows()
			if len(res) == 1 && res[0][pos] == s {
				logutil.BgLogger().Info("Got state", zap.String("State", s))
				break
			}
			gotime.Sleep(10 * gotime.Millisecond)
		}
		dom := domain.GetDomain(tk.Session())
		// Make sure the table schema is the new schema.
		require.NoError(t, dom.Reload())
	}
	waitFor("nt", "write only", 4)

	tk.MustExec(`insert into nt values (60, 60)`)
	// violate pt (a < 75)
	tk.MustContainErrMsg(`insert into nt values (80, 60)`, errMsg)
	// violate pt (b < 75)
	tk.MustContainErrMsg(`insert into nt values (60, 80)`, errMsg)
	// violate pt (a < 75)
	tk.MustContainErrMsg(`update nt set a = 80 where a = 60`, errMsg)
	// violate pt (b < 75)
	tk.MustContainErrMsg(`update nt set b = 80 where b = 60`, errMsg)

	tk.MustExec(`insert into pt values (60, 60)`)
	// violate nt (b > 50)
	tk.MustContainErrMsg(`insert into pt values (60, 50)`, errMsg)
	// violate nt (b > 50)
	tk.MustContainErrMsg(`update pt set b = 50 where b = 60`, errMsg)
	// row in partition p0(less than (50)), is ok.
	tk.MustExec(`insert into pt values (30, 50)`)

	tk5 := testkit.NewTestKit(t, store)
	tk5.MustExec(`use check_constraint`)
	tk5.MustExec("begin")
	// Let tk5 get mdl of pt with the version of write-only state.
	tk5.MustQuery(`select * from pt`)

	tk6 := testkit.NewTestKit(t, store)
	tk6.MustExec(`use check_constraint`)
	tk6.MustExec("begin")
	// Let tk6 get mdl of nt with the version of write-only state.
	tk6.MustQuery(`select * from nt`)

	// Release tk2 mdl, wait ddl enter next state.
	tk2.MustExec("commit")
	waitFor("pt", "none", 4)

	// violate nt (b > 50)
	// Now tk5 handle the sql with MDL: pt version state is write-only, nt version state is none.
	tk5.MustContainErrMsg(`insert into pt values (60, 50)`, errMsg)
	// Verify exists row(60, 60) in pt.
	tk5.MustQuery(`select * from pt where a = 60 and b = 60`).Check(testkit.Rows("60 60"))
	// Update oldData and newData both in p1, violate nt (b > 50)
	tk5.MustContainErrMsg(`update pt set b = 50 where a = 60 and b = 60`, errMsg)
	// Verify exists row(30, 50) in pt.
	tk5.MustQuery(`select * from pt where a = 30 and b = 50`).Check(testkit.Rows("30 50"))
	// update oldData in p0, newData in p1, violate nt (b > 50)
	tk5.MustContainErrMsg(`update pt set a = 60 where a = 30 and b = 50`, errMsg)

	// violate pt (a < 75)
	tk6.MustContainErrMsg(`insert into nt values (80, 60)`, errMsg)
	// violate pt (b < 75)
	tk6.MustContainErrMsg(`insert into nt values (60, 80)`, errMsg)
	// Verify exists row(60, 60) in nt.
	tk6.MustQuery(`select * from pt where a = 60 and b = 60`).Check(testkit.Rows("60 60"))
	// violate pt (a < 75)
	tk6.MustContainErrMsg(`update nt set a = 80 where a = 60 and b = 60`, errMsg)

	// Let tk5, tk6 release mdl.
	tk5.MustExec("commit")
	tk6.MustExec("commit")

	// Wait ddl finish.
	<-alterChan
}

// Test partition table has check constraints while non-partition table do not have.
func TestExchangePartitionCheckConstraintStatesTwo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`create database check_constraint`)
	tk.MustExec(`set @@global.tidb_enable_check_constraint = 1`)
	tk.MustExec(`use check_constraint`)
	tk.MustExec(`create table nt (a int, b int)`)
	tk.MustExec(`create table pt (a int check (a < 75) ENFORCED, b int check (b < 75) ENFORCED) partition by range (a) (partition p0 values less than (50), partition p1 values less than (100) )`)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use check_constraint`)
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec(`use check_constraint`)
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec(`use check_constraint`)
	// TODO: error message to check.
	errMsg := "[table:3819]Check constraint"

	tk2.MustExec("begin")
	// Get table mdl.
	tk2.MustQuery(`select * from nt`).Check(testkit.Rows())
	alterChan := make(chan error)
	go func() {
		err := tk3.ExecToErr(`alter table pt exchange partition p1 with table nt`)
		alterChan <- err
	}()
	waitFor := func(tableName, s string, pos int) {
		for {
			select {
			case alterErr := <-alterChan:
				require.Fail(t, "Alter completed unexpectedly", "With error %v", alterErr)
			default:
				// Alter still running
			}
			res := tk4.MustQuery(`admin show ddl jobs where db_name = 'check_constraint' and table_name = '` + tableName + `' and job_type = 'exchange partition'`).Rows()
			if len(res) == 1 && res[0][pos] == s {
				logutil.BgLogger().Info("Got state", zap.String("State", s))
				break
			}
			gotime.Sleep(10 * gotime.Millisecond)
		}
		dom := domain.GetDomain(tk.Session())
		// Make sure the table schema is the new schema.
		require.NoError(t, dom.Reload())
	}
	waitFor("nt", "write only", 4)

	tk.MustExec(`insert into nt values (60, 60)`)
	// violate pt (a < 75)
	tk.MustContainErrMsg(`insert into nt values (80, 60)`, errMsg)
	// violate pt (b < 75)
	tk.MustContainErrMsg(`insert into nt values (60, 80)`, errMsg)
	// violate pt (a < 75)
	tk.MustContainErrMsg(`update nt set a = 80 where a = 60`, errMsg)
	// violate pt (b < 75)
	tk.MustContainErrMsg(`update nt set b = 80 where b = 60`, errMsg)

	tk.MustExec(`insert into pt values (60, 60)`)
	tk.MustExec(`insert into pt values (60, 50)`)
	tk.MustExec(`update pt set b = 50 where b = 60`)
	// row in partition p0(less than (50)), is ok.
	tk.MustExec(`insert into pt values (30, 50)`)

	// Release tk2 mdl.
	tk2.MustExec("commit")
	// Wait ddl finish.
	<-alterChan
}

func TestAddKeyPartitionStates(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dbName := "partSchemaVer"
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec(`set @@global.tidb_enable_metadata_lock = ON`)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use " + dbName)
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use " + dbName)
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec("use " + dbName)
	tk.MustExec(`create table t (a int primary key, b varchar(255), key (b)) partition by hash (a) partitions 3`)
	tk.MustExec(`insert into t values (1, "1")`)
	tk.MustExec(`analyze table t`)
	tk.MustExec("BEGIN")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1 1"))
	tk.MustExec(`insert into t values (2, "2")`)
	syncChan := make(chan bool)
	go func() {
		tk2.MustExec(`alter table t add partition partitions 1`)
		syncChan <- true
	}()
	waitFor := func(i int, s string) {
		for {
			res := tk4.MustQuery(`admin show ddl jobs where db_name = '` + strings.ToLower(dbName) + `' and table_name = 't' and job_type like 'alter table%'`).Rows()
			if len(res) == 1 && res[0][i] == s {
				break
			}
			gotime.Sleep(10 * gotime.Millisecond)
		}
		dom := domain.GetDomain(tk.Session())
		// Make sure the table schema is the new schema.
		require.NoError(t, dom.Reload())
	}
	waitFor(4, "delete only")
	tk3.MustExec(`BEGIN`)
	tk3.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1"))
	tk3.MustExec(`insert into t values (3,"3")`)

	tk.MustExec(`COMMIT`)
	waitFor(4, "write only")
	tk.MustExec(`BEGIN`)
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "2 2"))
	tk.MustExec(`insert into t values (4,"4")`)

	tk3.MustExec(`COMMIT`)
	waitFor(4, "write reorganization")
	tk3.MustExec(`BEGIN`)
	tk3.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY HASH (`a`) PARTITIONS 3"))
	tk3.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk3.MustExec(`insert into t values (5,"5")`)

	tk.MustExec(`COMMIT`)
	waitFor(4, "delete reorganization")
	tk.MustExec(`BEGIN`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY HASH (`a`) PARTITIONS 4"))
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "2 2", "3 3", "4 4"))
	tk.MustExec(`insert into t values (6,"6")`)

	tk3.MustExec(`COMMIT`)
	tk.MustExec(`COMMIT`)
	<-syncChan
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6"))
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
		logutil.BgLogger().Info("Partition DDL test", zap.Int("i", i), zap.String("ddlSQL", ddlSQL))
		executeSQLWrapper(t, tk, ddlSQL)
		// insert data
		for _, insertsql := range insertSQLs {
			executeSQLWrapper(t, tk, insertsql)
		}
		// execute testcases
		for j, selInfo := range testCase.selectInfo {
			logutil.BgLogger().Info("Select", zap.Int("j", j), zap.String("selectSQL", selInfo.selectSQL))
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
					true, false, true, true, []string{"partition:p3"}, []string{"partition:p0,p1,p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey0 WHERE col3 = 3 or col3 = 4",
					false, false, true, true, []string{"partition:p0,p3"}, []string{"partition:p1,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey0 WHERE col3 >1 AND col3 < 4",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p0,p2"}, 2,
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
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col3 = 3 and col1 = 3",
					true, false, true, true, []string{"partition:p1"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col3 = 3 or col3 = 4",
					false, false, true, true, []string{"partition:all"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col3 = 3 and col1 = 3 OR col3 = 4 and col1 = 4",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p1,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey7 WHERE col1>1 and col3 >1 AND col3 < 4 and col1<3",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
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
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col3 = 3 and col1 = 3",
					true, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col3 = 3 or col3 = 4",
					false, false, true, true, []string{"partition:all"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col3 = 3 and col1 = 3 OR col3 = 4 and col1 = 4",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey8 WHERE col1>1 and col3 >1 AND col3 < 4 and col1<3",
					false, false, true, true, []string{"partition:all"}, []string{}, 0,
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
					true, false, true, true, []string{"partition:p3"}, []string{"partition:p0,p1,p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey6 WHERE col3 = 'zhangsan' or col3 = 'linpin'",
					false, false, true, true, []string{"partition:p2,p3"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey6 WHERE col3 > 'linpin' AND col3 < 'qing'",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
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
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p0,p1,p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey2 WHERE KHH = 'huaian' or KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p0,p1,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey2 WHERE KHH > 'nanjing' AND KHH < 'suzhou'",
					false, false, true, true, []string{"partition:all"}, []string{}, 2,
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
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p0,p1,p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey5 WHERE KHH = 'huaian' or KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p0,p1,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey5 WHERE KHH > 'nanjing' AND KHH < 'suzhou'",
					false, false, true, true, []string{"partition:all"}, []string{}, 2,
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
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ = 2",
					false, false, true, true, []string{"partition:all"}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian' and JYRQ = 2",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian' and JYRQ = 2  or KHH = 'zhenjiang' and JYRQ = 3",
					false, false, true, true, []string{"partition:p0,p1"}, []string{"partition:p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian' and JYRQ = 2  or KHH = 'zhenjiang' and JYRQ = 3 or KHH = 'HUAIAN' and JYRQ = 15",
					false, false, true, true, []string{"partition:p0,p1"}, []string{"partition:p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE KHH = 'huaian' or KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:all"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ = 2  OR  JYRQ = 3",
					false, false, true, true, []string{"partition:all"}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ = 2  OR  JYRQ = 3 OR JYRQ = 15",
					false, false, true, true, []string{"partition:all"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ >6 AND JYRQ < 10",
					false, false, true, true, []string{"partition:all"}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey4 WHERE JYRQ >6 and KHH>'lianyungang' AND JYRQ < 10 and KHH<'xuzhou'",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
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
					true, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' and JYRQ = 2",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ = 2",
					false, false, true, true, []string{"partition:all"}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' and JYRQ = 2 and ZJZH='0517'  or KHH = 'zhenjiang' and JYRQ = 3 and ZJZH = '0518'",
					false, false, true, true, []string{"partition:p0,p3"}, []string{"partition:p1,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' and JYRQ = 2 and ZJZH='0517'  or KHH = 'zhenjiang' and JYRQ = 3 and ZJZH = '0518' or KHH = 'NANJING' and JYRQ = 14 and ZJZH = '025'",
					false, false, true, true, []string{"partition:p0,p3"}, []string{"partition:p2,p1"}, 3,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE KHH = 'huaian' or KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:all"}, []string{}, 2,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ = 2  OR  JYRQ = 3",
					false, false, true, true, []string{"partition:all"}, []string{}, 4,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ = 2  OR  JYRQ = 3 OR JYRQ = 15",
					false, false, true, true, []string{"partition:all"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ >6 AND JYRQ < 10",
					false, false, true, true, []string{"partition:all"}, []string{}, 3,
				},
				{
					"SELECT count(*) FROM tkey9 WHERE JYRQ = 2 and KHH = 'huaian' OR JYRQ = 3 and KHH = 'zhenjiang'",
					false, false, true, true, []string{"partition:all"}, []string{}, 2,
				},
			},
			dropSQL: "DROP TABLE IF EXISTS tkey9",
		},
	}

	for i, testCase := range testCases {
		logutil.BgLogger().Info("Partition DDL test", zap.Int("i", i), zap.String("createSQL", testCase.createSQL))
		executeSQLWrapper(t, tk, testCase.createSQL)
		executeSQLWrapper(t, tk, testCase.insertSQL)
		for j, selInfo := range testCase.selectInfo {
			logutil.BgLogger().Info("Select", zap.Int("j", j), zap.String("selectSQL", selInfo.selectSQL))
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
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 5,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id1 = 3",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id1 = 3 or id1 = 4",
					false, false, true, true, []string{"partition:p0,p3"}, []string{"partition:p1,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id1 >1 AND id1 < 4",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p2,p0"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id2) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id2 = 3",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p0,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id2 = 3 or id2 = 4",
					false, false, true, true, []string{"partition:p0,p3"}, []string{"partition:p1,p2"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id2 >1 AND id2 < 4",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p0,p2"}, 3,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id3) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p0,p1,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id3 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id3 = 5 or id3 = 4",
					false, false, true, true, []string{"partition:p0,p2"}, []string{"partition:p1,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id3 >1 AND id3 < 4",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p2,p0"}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id4) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 5,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id4 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id4 = 5 or id4 = 4",
					false, false, true, true, []string{"partition:p0,p2"}, []string{"partition:p1,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id4 >1 AND id4 < 4",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p0,p2"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id5) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p3,p0"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id5 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id5 = 5 or id5 = 4",
					false, false, true, true, []string{"partition:p0,p2"}, []string{"partition:p1,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id5 >1 AND id5 < 4",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p2,p0"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id6) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id6 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id6 = 5 or id6 = 4",
					false, false, true, true, []string{"partition:p0,p2"}, []string{"partition:p1,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id6 >1 AND id6 < 4",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p2,p0"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id7) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id7 = 5",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id7 = 5 or id7 = 4",
					false, false, true, true, []string{"partition:p0,p2"}, []string{"partition:p1,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id7 >1 AND id7 < 4",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p2,p0"}, 2,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id8) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id8 = 1.1",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p2,p0,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id8 = 1.1 or id8 = 33.78",
					false, false, true, true, []string{"partition:p0,p1"}, []string{"partition:p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id8 >1 AND id8 < 4",
					false, false, true, true, []string{"partition:all"}, []string{}, 3,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id9) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id9 = 46.48",
					false, false, true, true, []string{}, []string{"partition:all"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id9 = 46.48 or id9 = 336.78",
					false, false, true, true, []string{}, []string{"partition:all"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_numeric WHERE id9 >45 AND id9 < 47",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id10) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_numeric",
					false, false, true, true, []string{"partition:all"}, []string{}, 12,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_numeric PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
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
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
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
					false, false, true, true, []string{"partition:all"}, []string{}, 10,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id1 = '2012-04-10'",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id1 = '2012-04-10' or id1 = '2018-07-13'",
					false, false, true, true, []string{"partition:p0,p2"}, []string{"partition:p1,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id1 >'2012-04-10' AND id1 < '2014-04-10'",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id3) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_datetime",
					false, false, true, true, []string{"partition:all"}, []string{}, 10,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id3 = '2012-04-10 12:12:12'",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id3 = '2012-04-10 12:12:12' or id3 = '2021-08-14 16:16:16'",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id3 >'2012-04-10 12:12:12' AND id3 < '2014-04-10 12:12:12'",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id4) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_datetime",
					false, false, true, true, []string{"partition:all"}, []string{}, 10,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 4,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id4 = '2012-04-10 12:12:12'",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id4 = '2012-04-10 12:12:12' or id4 = '2021-08-14 16:16:16'",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p0,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id4 >'2012-04-10 12:12:12' AND id4 < '2014-04-10 12:12:12'",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id5) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_datetime",
					false, false, true, true, []string{"partition:all"}, []string{}, 10,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id5 = 2012",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id5 = 2012 or id5 = 2018",
					false, false, true, true, []string{"partition:p0,p2"}, []string{"partition:p1,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_datetime WHERE id5 >2012 AND id5 < 2014",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p3,p0"}, 1,
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
					false, false, true, true, []string{"partition:all"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id1 = 'huaian'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p0,p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id1 = 'huaian' or id1 = 'suzhou'",
					false, false, true, true, []string{"partition:p0,p3"}, []string{"partition:p1,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id1 >'huaian' AND id1 < 'suzhou'",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id2) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:all"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id2 = 'huaian'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id2 = 'huaian' or id2 = 'suzhou'",
					false, false, true, true, []string{"partition:p0,p3"}, []string{"partition:p1,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id2 >'huaian' AND id2 < 'suzhou'",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id3) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:all"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id3 = 0x73757A686F7500000000000000000000",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id3 = 0x73757A686F7500000000000000000000 or id3 = 0x6E616E6A696E67000000000000000000",
					false, false, true, true, []string{"partition:p0,p1"}, []string{"partition:p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id3 >0x67756169616E00000000000000000000 AND id3 < 0x6E616E6A696E67000000000000000000",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id4) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:all"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id4 = 0x68756169616E",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p0,p2"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id4 = 0x68756169616E or id4 = 0x73757A686F75",
					false, false, true, true, []string{"partition:p0,p3"}, []string{"partition:p1,p2"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id4 >0x73757A686F75 AND id4 < 0x78757869",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id7) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:all"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id7 = 'x-small'",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id7 = 'x-small' or id7 = 'large'",
					false, false, true, true, []string{"partition:p0,p2"}, []string{"partition:p1,p3"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id7 > 'large' AND id7 < 'x-small'",
					false, false, true, true, []string{"partition:p0,p1,p3"}, []string{"partition:p2"}, 3,
				},
			},
		},
		{
			partitionbySQL: "PARTITION BY KEY(id8) partitions 4",
			selectInfo: []compoundSQL{
				{
					"SELECT count(*) FROM tkey_string",
					false, false, true, true, []string{"partition:all"}, []string{}, 5,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p0)",
					false, false, true, true, []string{"partition:p0"}, []string{"partition:p1,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p1)",
					false, false, true, true, []string{"partition:p1"}, []string{"partition:p0,p2,p3"}, 1,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p2)",
					false, false, true, true, []string{"partition:p2"}, []string{"partition:p1,p0,p3"}, 0,
				},
				{
					"SELECT count(*) FROM tkey_string PARTITION(p3)",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id8 = 'a'",
					false, false, true, true, []string{"partition:p3"}, []string{"partition:p1,p2,p0"}, 2,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id8 = 'a' or id8 = 'b'",
					false, false, true, true, []string{"partition:p1,p3"}, []string{"partition:p0,p2"}, 3,
				},
				{
					"SELECT count(*) FROM tkey_string WHERE id8 > 'a' AND id8 < 'c'",
					false, false, true, true, []string{"partition:all"}, []string{}, 1,
				},
			},
		},
	}
	executePartTableCase(t, tk, testCases3, createSQL3, insertSQLS3, dropSQL3)
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

type testCallback struct {
	ddl.Callback
	OnJobRunBeforeExported func(job *model.Job)
}

func newTestCallBack(t *testing.T, dom *domain.Domain) *testCallback {
	defHookFactory, err := ddl.GetCustomizedHook("default_hook")
	require.NoError(t, err)
	return &testCallback{
		Callback: defHookFactory(dom),
	}
}

func (c *testCallback) OnJobRunBefore(job *model.Job) {
	if c.OnJobRunBeforeExported != nil {
		c.OnJobRunBeforeExported(job)
	}
}

func TestPartitionByIntListExtensivePart(t *testing.T) {
	limitSizeOfTest := true
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "PartitionByIntListExtensive"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use " + schemaName)

	tBase := `(lp tinyint unsigned, a int unsigned, b varchar(255) collate utf8mb4_general_ci, c int, d datetime, e timestamp, f double, g text, key (b), key (c,b), key (d,c), key(e), primary key (a, lp))`
	t2Str := `create table t2 ` + tBase
	tStr := `create table t ` + tBase

	rows := 100
	pkInserts := 20
	pkUpdates := 20
	pkDeletes := 10 // Enough to delete half of what is inserted?
	tStart := []string{
		// Non partitioned
		tStr,
		// RANGE COLUMNS
		tStr + ` partition by list (lp) (partition p0 values in (0,6),partition p1 values in (1), partition p2 values in (2), partition p3 values in (3), partition p4 values in (4,5))`,
		// KEY
		tStr + ` partition by key(a) partitions 5`,
		// HASH
		tStr + ` partition by hash(a) partitions 5`,
		// HASH with function
		tStr + ` partition by hash(a DIV 3) partitions 5`,
	}
	if limitSizeOfTest {
		tStart = tStart[:2]
	}
	quarterUintRange := 1 << 30
	quarterUintRangeStr := fmt.Sprintf("%d", quarterUintRange)
	halfUintRangeStr := fmt.Sprintf("%d", 2*quarterUintRange)
	threeQuarterUintRangeStr := fmt.Sprintf("%d", 3*quarterUintRange)
	tAlter := []string{
		// LIST
		`alter table t partition by list (lp) (partition p0 values in (2), partition p1 values in (1,3,5), partition p2 values in (0,4,6))`,
		`alter table t partition by list (lp) (partition p3 values in (3), partition p4 values in (4), partition p2 values in (2), partition p6 values in (6), partition p5 values in (5), partition p1 values in (1), partition p0 values in (0))`,
		// LIST COLUMNS
		`alter table t partition by list columns (lp) (partition p0 values in (2), partition p1 values in (1,3,5), partition p2 values in (0,4,6))`,
		`alter table t partition by list columns (lp) (partition p3 values in (3), partition p4 values in (4), partition p2 values in (2), partition p6 values in (6), partition p5 values in (5), partition p1 values in (1), partition p0 values in (0))`,
		// RANGE COLUMNS
		`alter table t partition by range (a) (partition pFirst values less than (` + halfUintRangeStr + `), partition pLast values less than (MAXVALUE))`,
		// RANGE COLUMNS
		`alter table t partition by range (a) (partition pFirst values less than (` + quarterUintRangeStr + `),` +
			`partition pLowMid values less than (` + halfUintRangeStr + `),` +
			`partition pHighMid values less than (` + threeQuarterUintRangeStr + `),` +
			`partition pLast values less than (maxvalue))`,
		// KEY
		`alter table t partition by key(a) partitions 7`,
		`alter table t partition by key(a) partitions 3`,
		// Hash
		`alter table t partition by hash(a) partitions 7`,
		`alter table t partition by hash(a) partitions 3`,
		// Hash
		`alter table t partition by hash(a DIV 13) partitions 7`,
		`alter table t partition by hash(a DIV 13) partitions 3`,
	}
	if limitSizeOfTest {
		tAlter = tAlter[:2]
	}

	seed := gotime.Now().UnixNano()
	logutil.BgLogger().Info("Seeding rand", zap.Int64("seed", seed))
	reorgRand := rand.New(rand.NewSource(seed))
	for _, createSQL := range tStart {
		for _, alterSQL := range tAlter {
			tk.MustExec(createSQL)
			tk.MustExec(t2Str)
			getNewPK := getNewIntPK()
			getValues := getInt7ValuesFunc()
			checkDMLInAllStates(t, tk, tk2, schemaName, alterSQL, rows, pkInserts, pkUpdates, pkDeletes, reorgRand, getNewPK, getValues)
			tk.MustExec(`drop table t`)
			tk.MustExec(`drop table t2`)
		}
	}
	for _, createSQL := range tStart[1:] {
		tk.MustExec(createSQL)
		tk.MustExec(t2Str)
		getNewPK := getNewIntPK()
		getValues := getInt7ValuesFunc()
		checkDMLInAllStates(t, tk, tk2, schemaName, "alter table t remove partitioning", rows, pkInserts, pkUpdates, pkDeletes, reorgRand, getNewPK, getValues)
		tk.MustExec(`drop table t`)
		tk.MustExec(`drop table t2`)
	}
}

func getInt7ValuesFunc() func(string, bool, *rand.Rand) string {
	cnt := 0
	return func(pk string, asAssignment bool, reorgRand *rand.Rand) string {
		s := `(%d, %s, '%s', %d, '%s', '%s', %f, '%s')`
		if asAssignment {
			s = `lp = %d, a = %s, b = '%s', c = %d,  d = '%s', e = '%s', f = %f, g = '%s'`
		}
		cnt++
		lp, err := strconv.Atoi(pk)
		if err != nil {
			lp = 0
		}
		return fmt.Sprintf(s,
			lp%7,
			pk,
			randStr(reorgRand.Intn(19), reorgRand),
			cnt, //reorgRand.Int31(),
			gotime.Unix(413487608+int64(reorgRand.Intn(1705689644)), 0).Format("2006-01-02T15:04:05"),
			gotime.Unix(413487608+int64(reorgRand.Intn(1705689644)), 0).Format("2006-01-02T15:04:05"),
			reorgRand.Float64(),
			randStr(512+reorgRand.Intn(1024), reorgRand))
	}
}

func TestPartitionByIntExtensivePart(t *testing.T) {
	limitSizeOfTest := true
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "PartitionByIntExtensive"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use " + schemaName)

	tBase := `(a int unsigned, b varchar(255) collate utf8mb4_general_ci, c int, d datetime, e timestamp, f double, g text, primary key (a), key (b), key (c,b), key (d,c), key(e))`
	t2Str := `create table t2 ` + tBase
	tStr := `create table t ` + tBase

	rows := 100
	pkInserts := 20
	pkUpdates := 20
	pkDeletes := 10 // Enough to delete half of what is inserted?
	thirdUintRange := 1 << 32 / 2
	thirdUintRangeStr := fmt.Sprintf("%d", thirdUintRange)
	twoThirdUintRangeStr := fmt.Sprintf("%d", 2*thirdUintRange)
	tStart := []string{
		// Non partitioned
		tStr,
		// RANGE COLUMNS
		tStr + ` partition by range (a) (partition pFirst values less than (` + thirdUintRangeStr + `),` +
			`partition pMid values less than (` + twoThirdUintRangeStr + `), partition pLast values less than (maxvalue))`,
		// KEY
		tStr + ` partition by key(a) partitions 5`,
		// HASH
		tStr + ` partition by hash(a) partitions 5`,
		// HASH with function
		tStr + ` partition by hash(a DIV 3) partitions 5`,
	}
	if limitSizeOfTest {
		tStart = tStart[:2]
	}
	quarterUintRange := 1 << 30
	quarterUintRangeStr := fmt.Sprintf("%d", quarterUintRange)
	halfUintRangeStr := fmt.Sprintf("%d", 2*quarterUintRange)
	threeQuarterUintRangeStr := fmt.Sprintf("%d", 3*quarterUintRange)
	tAlter := []string{
		// RANGE COLUMNS
		`alter table t partition by range (a) (partition pFirst values less than (` + halfUintRangeStr + `), partition pLast values less than (MAXVALUE))`,
		// RANGE COLUMNS
		`alter table t partition by range (a) (partition pFirst values less than (` + quarterUintRangeStr + `),` +
			`partition pLowMid values less than (` + halfUintRangeStr + `),` +
			`partition pHighMid values less than (` + threeQuarterUintRangeStr + `),` +
			`partition pLast values less than (maxvalue))`,
		// KEY
		`alter table t partition by key(a) partitions 7`,
		`alter table t partition by key(a) partitions 3`,
		// Hash
		`alter table t partition by hash(a) partitions 7`,
		`alter table t partition by hash(a) partitions 3`,
		// Hash
		`alter table t partition by hash(a DIV 13) partitions 7`,
		`alter table t partition by hash(a DIV 13) partitions 3`,
	}
	if limitSizeOfTest {
		tAlter = tAlter[:2]
	}

	seed := gotime.Now().UnixNano()
	logutil.BgLogger().Info("Seeding rand", zap.Int64("seed", seed))
	reorgRand := rand.New(rand.NewSource(seed))
	for _, createSQL := range tStart {
		for _, alterSQL := range tAlter {
			tk.MustExec(createSQL)
			tk.MustExec(t2Str)
			getNewPK := getNewIntPK()
			getValues := getIntValuesFunc()
			checkDMLInAllStates(t, tk, tk2, schemaName, alterSQL, rows, pkInserts, pkUpdates, pkDeletes, reorgRand, getNewPK, getValues)
			tk.MustExec(`drop table t`)
			tk.MustExec(`drop table t2`)
		}
	}
	for _, createSQL := range tStart[1:] {
		tk.MustExec(createSQL)
		tk.MustExec(t2Str)
		getNewPK := getNewIntPK()
		getValues := getIntValuesFunc()
		checkDMLInAllStates(t, tk, tk2, schemaName, "alter table t remove partitioning", rows, pkInserts, pkUpdates, pkDeletes, reorgRand, getNewPK, getValues)
		tk.MustExec(`drop table t`)
		tk.MustExec(`drop table t2`)
	}
}

func getNewIntPK() func(map[string]struct{}, string, *rand.Rand) string {
	return func(m map[string]struct{}, suf string, reorgRand *rand.Rand) string {
		uintPK := reorgRand.Uint32()
		newPK := strconv.FormatUint(uint64(uintPK), 10)
		for _, ok := m[newPK]; ok; {
			uintPK = reorgRand.Uint32()
			newPK = strconv.FormatUint(uint64(uintPK), 10)
			_, ok = m[newPK]
		}
		m[newPK] = struct{}{}
		return newPK
	}
}

func getIntValuesFunc() func(string, bool, *rand.Rand) string {
	cnt := 0
	return func(pk string, asAssignment bool, reorgRand *rand.Rand) string {
		s := `(%s, '%s', %d, '%s', '%s', %f, '%s')`
		if asAssignment {
			s = `a = %s, b = '%s', c = %d,  d = '%s', e = '%s', f = %f, g = '%s'`
		}
		cnt++
		return fmt.Sprintf(s,
			pk,
			randStr(reorgRand.Intn(19), reorgRand),
			cnt, //reorgRand.Int31(),
			gotime.Unix(413487608+int64(reorgRand.Intn(1705689644)), 0).Format("2006-01-02T15:04:05"),
			gotime.Unix(413487608+int64(reorgRand.Intn(1705689644)), 0).Format("2006-01-02T15:04:05"),
			reorgRand.Float64(),
			randStr(512+reorgRand.Intn(1024), reorgRand))
	}
}

func TestPartitionByExtensivePart(t *testing.T) {
	limitSizeOfTest := true
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "PartitionByExtensive"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use " + schemaName)

	tBase := `(a varchar(255) collate utf8mb4_unicode_ci, b varchar(255) collate utf8mb4_general_ci, c int, d datetime, e timestamp, f double, g text, primary key (a), key (b), key (c,b), key (d,c), key(e))`
	t2Str := `create table t2 ` + tBase
	tStr := `create table t ` + tBase

	rows := 100
	pkInserts := 20
	pkUpdates := 20
	pkDeletes := 10 // Enough to delete half of what is inserted?
	tStart := []string{
		// Non partitioned
		tStr,
		// RANGE COLUMNS
		tStr + ` partition by range columns (a) (partition pNull values less than (""), partition pM values less than ("M"), partition pLast values less than (maxvalue))`,
		// KEY
		tStr + ` partition by key(a) partitions 5`,
	}
	if limitSizeOfTest {
		tStart = tStart[:2]
	}
	showCreateStr := "t CREATE TABLE `t` (\n" +
		"  `a` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `d` datetime DEFAULT NULL,\n" +
		"  `e` timestamp NULL DEFAULT NULL,\n" +
		"  `f` double DEFAULT NULL,\n" +
		"  `g` text DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`),\n" +
		"  KEY `d` (`d`,`c`),\n" +
		"  KEY `e` (`e`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"
	tAlter := []struct{ alter, result string }{
		{
			// RANGE COLUMNS
			alter: `alter table t partition by range columns (a) (partition pH values less than ("H"), partition pLast values less than (MAXVALUE))`,
			result: showCreateStr +
				"PARTITION BY RANGE COLUMNS(`a`)\n" +
				"(PARTITION `pH` VALUES LESS THAN ('H'),\n" +
				" PARTITION `pLast` VALUES LESS THAN (MAXVALUE))",
		},
		{
			// RANGE COLUMNS
			alter: `alter table t partition by range columns (a) (partition pNull values less than (""), partition pG values less than ("G"), partition pR values less than ("R"), partition pLast values less than (maxvalue))`,
			result: showCreateStr +
				"PARTITION BY RANGE COLUMNS(`a`)\n" +
				"(PARTITION `pNull` VALUES LESS THAN (''),\n" +
				" PARTITION `pG` VALUES LESS THAN ('G'),\n" +
				" PARTITION `pR` VALUES LESS THAN ('R'),\n" +
				" PARTITION `pLast` VALUES LESS THAN (MAXVALUE))",
		},
		// KEY
		{
			alter: `alter table t partition by key(a) partitions 7`,
			result: showCreateStr +
				"PARTITION BY KEY (`a`) PARTITIONS 7",
		},
		{
			alter: `alter table t partition by key(a) partitions 3`,
			result: showCreateStr +
				"PARTITION BY KEY (`a`) PARTITIONS 3",
		},
	}
	if limitSizeOfTest {
		tAlter = tAlter[:2]
	}

	seed := gotime.Now().UnixNano()
	logutil.BgLogger().Info("Seeding rand", zap.Int64("seed", seed))
	reorgRand := rand.New(rand.NewSource(seed))
	for _, createSQL := range tStart {
		for _, alterSQL := range tAlter {
			tk.MustExec(createSQL)
			tk.MustExec(t2Str)
			getNewPK := getNewStringPK()
			getValues := getValuesFunc()
			checkDMLInAllStates(t, tk, tk2, schemaName, alterSQL.alter, rows, pkInserts, pkUpdates, pkDeletes, reorgRand, getNewPK, getValues)
			res := tk.MustQuery(`show create table t`)
			res.AddComment("create SQL: " + createSQL + "\nalterSQL: " + alterSQL.alter)
			res.Check(testkit.Rows(alterSQL.result))
			tk.MustExec(`drop table t`)
			tk.MustExec(`drop table t2`)
		}
	}

	for _, createSQL := range tStart[1:] {
		tk.MustExec(createSQL)
		tk.MustExec(t2Str)
		getNewPK := getNewStringPK()
		getValues := getValuesFunc()
		checkDMLInAllStates(t, tk, tk2, schemaName, "alter table t remove partitioning", rows, pkInserts, pkUpdates, pkDeletes, reorgRand, getNewPK, getValues)
		tk.MustExec(`drop table t`)
		tk.MustExec(`drop table t2`)
	}
}

func TestReorgPartExtensivePart(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "ReorgPartExtensive"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use " + schemaName)
	// TODO: Handle different column types?
	// TODO: Handle index for different column types / combinations as well?

	// Steps:
	// - create a table (should at least test both LIST and RANGE partition, Including COLUMNS)
	// - add base data
	// - start DDL
	// - before each (and after?) each state transition:
	//   - insert, update and delete concurrently, to verify that the states are correctly handled.
	//   - TODO: Crash (if rollback is needed, then OK, but the rest need to be tested
	//   - TODO: Fail
	//   - TODO: run queries that could clash with backfill etc. (How to handle expected errors?)
	//     - TODO: on both the 'current' state and 'previous' state!
	//   - run ADMIN CHECK TABLE
	//

	tk.MustExec(`create table t (a varchar(255) collate utf8mb4_unicode_ci, b varchar(255) collate utf8mb4_general_ci, c int, d datetime, e timestamp, f double, g text, primary key (a)) partition by range columns (a) (partition pNull values less than (""), partition pM values less than ("M"), partition pLast values less than (maxvalue))`)
	tk.MustExec(`create table t2 (a varchar(255) collate utf8mb4_unicode_ci, b varchar(255) collate utf8mb4_general_ci, c int, d datetime, e timestamp, f double, g text, primary key (a), key (b), key (c,b), key (d,c), key(e))`)

	// TODO: Test again with timestamp in col e!!
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `d` datetime DEFAULT NULL,\n" +
		"  `e` timestamp NULL DEFAULT NULL,\n" +
		"  `f` double DEFAULT NULL,\n" +
		"  `g` text DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`a`)\n" +
		"(PARTITION `pNull` VALUES LESS THAN (''),\n" +
		" PARTITION `pM` VALUES LESS THAN ('M'),\n" +
		" PARTITION `pLast` VALUES LESS THAN (MAXVALUE))"))

	tk.MustQuery(`show create table t2`).Check(testkit.Rows("" +
		"t2 CREATE TABLE `t2` (\n" +
		"  `a` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `d` datetime DEFAULT NULL,\n" +
		"  `e` timestamp NULL DEFAULT NULL,\n" +
		"  `f` double DEFAULT NULL,\n" +
		"  `g` text DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`),\n" +
		"  KEY `d` (`d`,`c`),\n" +
		"  KEY `e` (`e`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	rows := 1000
	pkInserts := 200
	pkUpdates := 200
	pkDeletes := 100 // Enough to delete half of what is inserted?
	alterStr := `alter table t reorganize partition pNull, pM, pLast into (partition pI values less than ("I"), partition pQ values less than ("q"), partition pLast values less than (MAXVALUE))`
	seed := rand.Int63()
	logutil.BgLogger().Info("Seeding rand", zap.Int64("seed", seed))
	reorgRand := rand.New(rand.NewSource(seed))
	getNewPK := getNewStringPK()
	getValues := getValuesFunc()
	checkDMLInAllStates(t, tk, tk2, schemaName, alterStr, rows, pkInserts, pkUpdates, pkDeletes, reorgRand, getNewPK, getValues)
}

func getNewStringPK() func(map[string]struct{}, string, *rand.Rand) string {
	return func(m map[string]struct{}, suf string, reorgRand *rand.Rand) string {
		newPK := randStr(2+reorgRand.Intn(5), reorgRand) + suf
		lowerPK := strings.ToLower(newPK)
		for _, ok := m[lowerPK]; ok; {
			newPK = randStr(2+reorgRand.Intn(5), reorgRand)
			lowerPK = strings.ToLower(newPK)
			_, ok = m[lowerPK]
		}
		m[lowerPK] = struct{}{}
		return newPK
	}
}

func getValuesFunc() func(string, bool, *rand.Rand) string {
	cnt := 0
	return func(pk string, asAssignment bool, reorgRand *rand.Rand) string {
		s := `('%s', '%s', %d, '%s', '%s', %f, '%s')`
		if asAssignment {
			s = `a = '%s', b = '%s', c = %d,  d = '%s', e = '%s', f = %f, g = '%s'`
		}
		cnt++
		return fmt.Sprintf(s,
			pk,
			randStr(reorgRand.Intn(19), reorgRand),
			cnt, //reorgRand.Int31(),
			gotime.Unix(413487608+int64(reorgRand.Intn(1705689644)), 0).Format("2006-01-02T15:04:05"),
			gotime.Unix(413487608+int64(reorgRand.Intn(1705689644)), 0).Format("2006-01-02T15:04:05"),
			reorgRand.Float64(),
			randStr(512+reorgRand.Intn(1024), reorgRand))
	}
}

func checkDMLInAllStates(t *testing.T, tk, tk2 *testkit.TestKit, schemaName, alterStr string,
	rows, pkInserts, pkUpdates, pkDeletes int,
	reorgRand *rand.Rand,
	getNewPK func(map[string]struct{}, string, *rand.Rand) string,
	getValues func(string, bool, *rand.Rand) string) {
	dom := domain.GetDomain(tk.Session())
	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	hook := newTestCallBack(t, dom)
	dom.DDL().SetHook(hook)

	pkMap := make(map[string]struct{}, rows)
	pkArray := make([]string, 0, len(pkMap))
	// Generate a start set:
	for i := 0; i < rows; i++ {
		pk := getNewPK(pkMap, "-o", reorgRand)
		pkArray = append(pkArray, pk)
		values := getValues(pk, false, reorgRand)
		tk.MustExec(`insert into t values ` + values)
		tk.MustExec(`insert into t2 values ` + values)
	}
	tk.MustExec(`analyze table t`)
	tk.MustExec(`analyze table t2`)
	tk.MustQuery(`select * from t except select * from t2`).Check(testkit.Rows())
	tk.MustQuery(`select * from t2 except select * from t`).Check(testkit.Rows())

	// How to arrange data for possible collisions?
	// change both PK column, SK column and non indexed column!
	// Run various changes in transactions, in two concurrent sessions
	// + mirror those transactions on a copy of the same table and data without DDL
	// to verify expected transaction conflicts!
	// We should try to collide:
	// Current data : 1-1000
	// insert vN    1-200 // random order, random length of transaction?
	// insert vN-1 100-300 // interleaved with vN, random order+length of txn?
	// update vN    1-20, 100-120, 200-220, 300-320..
	// update vN-1  10-30, 110-130, 210-230, ...
	// delete vN
	// delete vN-1
	//               insert      update       delete <-
	//  insert
	//  update
	//  delete
	// Note: update the PK so it moves between different before and after partitions
	tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
	tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
	currentState := model.StateNone
	transitions := 0
	var currTbl table.Table
	currSchema := sessiontxn.GetTxnManager(tk2.Session()).GetTxnInfoSchema()
	prevTbl, err := currSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	var hookErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if hookErr != nil {
			// Enough to find a single error
			return
		}
		if job.Type == model.ActionReorganizePartition && job.SchemaState != currentState {
			transitions++
			// use random generation to possibly trigger txn collisions / deadlocks?
			// insert (dup in new/old , non dup)
			// update (dup in new/old , non dup as in same old/new partition -> update, different new/old -> insert + delete)
			// delete
			// verify with select after commit?

			logutil.BgLogger().Info("State before ins/upd/del", zap.Int("transitions", transitions),
				zap.Int("rows", len(pkMap)), zap.Stringer("SchemaState", job.SchemaState))
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			// Start with PK changes (non duplicate keys)
			insPK := make([]string, 0, pkInserts)
			values := make([]string, 0, pkInserts)
			for i := 0; i < pkInserts; i += 2 {
				pk := getNewPK(pkMap, "-i0", reorgRand)
				logutil.BgLogger().Debug("insert1", zap.String("pk", pk))
				pkArray = append(pkArray, pk)
				insPK = append(insPK, pk)
				values = append(values, getValues(pk, false, reorgRand))
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			hookErr = tk2.ExecToErr(`insert into t values ` + strings.Join(values, ","))
			if hookErr != nil {
				return
			}
			hookErr = tk2.ExecToErr(`insert into t2 values ` + strings.Join(values, ","))
			if hookErr != nil {
				return
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			currSchema = sessiontxn.GetTxnManager(tk2.Session()).GetTxnInfoSchema()
			currTbl, hookErr = currSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))

			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using previous schema version

			values = values[:0]
			for i := 1; i < pkInserts; i += 2 {
				pk := getNewPK(pkMap, "-i1", reorgRand)
				logutil.BgLogger().Debug("insert2", zap.String("pk", pk))
				pkArray = append(pkArray, pk)
				insPK = append(insPK, pk)
				values = append(values, getValues(pk, false, reorgRand))
			}
			hookErr = tk2.ExecToErr(`insert into t values ` + strings.Join(values, ","))
			if hookErr != nil {
				return
			}
			hookErr = tk2.ExecToErr(`insert into t2 values ` + strings.Join(values, ","))
			if hookErr != nil {
				return
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			rs, err := tk2.Exec(`select count(*) from t`)
			if err != nil {
				hookErr = err
				return
			}
			tRows := tk2.ResultSetToResult(rs, "").Rows()[0][0].(string)
			rs, err = tk2.Exec(`select count(*) from t2`)
			if err != nil {
				hookErr = err
				return
			}
			t2Rows := tk2.ResultSetToResult(rs, "").Rows()[0][0].(string)
			if tRows != t2Rows {
				logutil.BgLogger().Error("rows do not match", zap.String("t", tRows), zap.String("t2", t2Rows), zap.Stringer("state", job.SchemaState))
			}

			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using current schema version

			// Half from insert (1/4 in current schema version)
			values = values[:0]
			for i := 0; i < pkUpdates; i += 4 {
				insIdx := reorgRand.Intn(len(insPK))
				oldPK := insPK[insIdx]
				lowerPK := strings.ToLower(oldPK)
				delete(pkMap, lowerPK)
				newPK := getNewPK(pkMap, "-u0", reorgRand)
				insPK[insIdx] = newPK
				idx := len(pkArray) - len(insPK) + insIdx
				pkArray[idx] = newPK
				value := getValues(newPK, true, reorgRand)

				logutil.BgLogger().Debug("update1", zap.String("old", oldPK), zap.String("value", value))
				hookErr = tk2.ExecToErr(`update t set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`update t2 set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}

				// Also do some non-pk column updates!
				insIdx = reorgRand.Intn(len(insPK))
				oldPK = insPK[insIdx]
				value = getValues(oldPK, true, reorgRand)

				hookErr = tk2.ExecToErr(`update t set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`update t2 set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))

			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using previous schema version

			// Half from insert (1/4 in previous schema version)
			values = values[:0]
			for i := 1; i < pkUpdates; i += 4 {
				insIdx := reorgRand.Intn(len(insPK))
				oldPK := insPK[insIdx]
				lowerPK := strings.ToLower(oldPK)
				delete(pkMap, lowerPK)
				newPK := getNewPK(pkMap, "-u1", reorgRand)
				insPK[insIdx] = newPK
				idx := len(pkArray) - len(insPK) + insIdx
				pkArray[idx] = newPK
				value := getValues(newPK, true, reorgRand)
				logutil.BgLogger().Debug("update2", zap.String("old", oldPK), zap.String("value", value))

				hookErr = tk2.ExecToErr(`update t set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`update t2 set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}

				// Also do some non-pk column updates!
				// Note: if PK changes it does RemoveRecord + AddRecord
				insIdx = reorgRand.Intn(len(insPK))
				oldPK = insPK[insIdx]
				value = getValues(oldPK, true, reorgRand)

				hookErr = tk2.ExecToErr(`update t set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`update t2 set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))

			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			// Half from Old
			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using current schema version

			// Half from old (1/4 in current schema version)
			values = values[:0]
			for i := 2; i < pkUpdates; i += 4 {
				idx := reorgRand.Intn(len(pkArray) - len(insPK))
				oldPK := pkArray[idx]
				lowerPK := strings.ToLower(oldPK)
				delete(pkMap, lowerPK)
				newPK := getNewPK(pkMap, "-u2", reorgRand)
				pkArray[idx] = newPK
				value := getValues(newPK, true, reorgRand)
				logutil.BgLogger().Debug("update3", zap.String("old", oldPK), zap.String("value", value))

				hookErr = tk2.ExecToErr(`update t set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`update t2 set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}

				// Also do some non-pk column updates!
				idx = reorgRand.Intn(len(pkArray) - len(insPK))
				oldPK = pkArray[idx]
				value = getValues(oldPK, true, reorgRand)

				hookErr = tk2.ExecToErr(`update t set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`update t2 set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))

			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using previous schema version

			// Half from old (1/4 in previous schema version)
			values = values[:0]
			for i := 3; i < pkUpdates; i += 4 {
				idx := reorgRand.Intn(len(pkArray) - len(insPK))
				oldPK := pkArray[idx]
				lowerPK := strings.ToLower(oldPK)
				delete(pkMap, lowerPK)
				newPK := getNewPK(pkMap, "-u3", reorgRand)
				pkArray[idx] = newPK
				value := getValues(newPK, true, reorgRand)
				logutil.BgLogger().Debug("update4", zap.String("old", oldPK), zap.String("value", value))

				hookErr = tk2.ExecToErr(`update t set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`update t2 set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}

				// Also do some non-pk column updates!
				idx = reorgRand.Intn(len(pkArray) - len(insPK))
				oldPK = pkArray[idx]
				value = getValues(oldPK, true, reorgRand)

				hookErr = tk2.ExecToErr(`update t set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`update t2 set ` + value + ` where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			rs, err = tk2.Exec(`select count(*) from t`)
			if err != nil {
				hookErr = err
				return
			}
			tRows = tk2.ResultSetToResult(rs, "").Rows()[0][0].(string)
			rs, err = tk2.Exec(`select count(*) from t2`)
			if err != nil {
				hookErr = err
				return
			}
			t2Rows = tk2.ResultSetToResult(rs, "").Rows()[0][0].(string)
			if tRows != t2Rows {
				logutil.BgLogger().Error("rows do not match", zap.String("t", tRows), zap.String("t2", t2Rows), zap.Stringer("state", job.SchemaState))
			}

			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using current schema version

			// Half from insert (1/4 in current schema version)
			values = values[:0]
			for i := 0; i < pkDeletes; i += 4 {
				insIdx := reorgRand.Intn(len(insPK))
				oldPK := insPK[insIdx]
				lowerPK := strings.ToLower(oldPK)
				delete(pkMap, lowerPK)
				idx := len(pkArray) - len(insPK) + insIdx
				insPK = append(insPK[:insIdx], insPK[insIdx+1:]...)
				pkArray = append(pkArray[:idx], pkArray[idx+1:]...)
				logutil.BgLogger().Debug("delete0", zap.String("pk", oldPK))

				hookErr = tk2.ExecToErr(`delete from t where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`delete from t2 where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))

			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using previous schema version

			// Half from insert (1/4 in previous schema version)
			values = values[:0]
			for i := 1; i < pkDeletes; i += 4 {
				insIdx := reorgRand.Intn(len(insPK))
				oldPK := insPK[insIdx]
				lowerPK := strings.ToLower(oldPK)
				delete(pkMap, lowerPK)
				idx := len(pkArray) - len(insPK) + insIdx
				insPK = append(insPK[:insIdx], insPK[insIdx+1:]...)
				pkArray = append(pkArray[:idx], pkArray[idx+1:]...)
				logutil.BgLogger().Debug("delete1", zap.String("pk", oldPK))

				hookErr = tk2.ExecToErr(`delete from t where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`delete from t2 where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))

			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			// Half from Old
			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using current schema version

			// Half from old (1/4 in current schema version)
			values = values[:0]
			for i := 2; i < pkDeletes; i += 4 {
				idx := reorgRand.Intn(len(pkArray) - len(insPK))
				oldPK := pkArray[idx]
				lowerPK := strings.ToLower(oldPK)
				delete(pkMap, lowerPK)
				pkArray = append(pkArray[:idx], pkArray[idx+1:]...)
				logutil.BgLogger().Debug("delete2", zap.String("pk", oldPK))

				hookErr = tk2.ExecToErr(`delete from t where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`delete from t2 where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
			}
			if len(pkMap) != len(pkArray) {
				panic("Different length!!!")
			}
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))

			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using previous schema version

			// Half from old (1/4 in previous schema version)
			values = values[:0]
			for i := 3; i < pkDeletes; i += 4 {
				idx := reorgRand.Intn(len(pkArray) - len(insPK))
				oldPK := pkArray[idx]
				lowerPK := strings.ToLower(oldPK)
				delete(pkMap, lowerPK)
				pkArray = append(pkArray[:idx], pkArray[idx+1:]...)
				logutil.BgLogger().Debug("delete3", zap.String("pk", oldPK))

				hookErr = tk2.ExecToErr(`delete from t where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
				hookErr = tk2.ExecToErr(`delete from t2 where a = "` + oldPK + `"`)
				if hookErr != nil {
					return
				}
			}
			tk2.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
			tk2.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
			rs, err = tk2.Exec(`select count(*) from t`)
			if err != nil {
				hookErr = err
				return
			}
			tRows = tk2.ResultSetToResult(rs, "").Rows()[0][0].(string)
			rs, err = tk2.Exec(`select count(*) from t2`)
			if err != nil {
				hookErr = err
				return
			}
			t2Rows = tk2.ResultSetToResult(rs, "").Rows()[0][0].(string)
			if tRows != t2Rows {
				logutil.BgLogger().Error("rows do not match", zap.String("t", tRows), zap.String("t2", t2Rows), zap.Stringer("state", job.SchemaState))
			}

			require.True(t, tables.SwapReorgPartFields(currTbl, prevTbl))
			// Now using current schema version
			tk2.MustQuery(`select count(*) from t2`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			tk2.MustQuery(`select count(*) from t`).Check(testkit.Rows(fmt.Sprintf("%d", len(pkMap))))
			prevTbl = currTbl
			logutil.BgLogger().Info("State after ins/upd/del", zap.Int("transitions", transitions),
				zap.Int("rows", len(pkMap)), zap.Stringer("SchemaState", job.SchemaState))
		}
	}
	tk.MustExec(alterStr)
	require.NoError(t, hookErr)
	tk.MustExec(`admin check table t`)
	tk.MustExec(`admin check table t2`)
	tk.MustQuery(`select count(*) from (select a from t except select a from t2) a`).Check(testkit.Rows("0"))
	tk.MustQuery(`select count(*) from (select a from t2 except select a from t) a`).Check(testkit.Rows("0"))
	tk.MustQuery(`select * from t except select * from t2 LIMIT 1`).Check(testkit.Rows())
	tk.MustQuery(`select * from t2 except select * from t LIMIT 1`).Check(testkit.Rows())
}

// Emojis fold to a single rune, and ö compares as o, so just complicated having other runes.
// Enough to just distribute between A and Z + testing simple folding
var runes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStr(n int, r *rand.Rand) string {
	var sb strings.Builder
	sb.Grow(n)
	for i := 0; i < n; i++ {
		_, _ = sb.WriteRune(runes[r.Intn(len(runes))])
	}
	return sb.String()
}

func TestPointGetKeyPartitioning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE t (a VARCHAR(30) NOT NULL, b VARCHAR(45) NOT NULL,
 c VARCHAR(45) NOT NULL, PRIMARY KEY (b, a)) PARTITION BY KEY(b) PARTITIONS 5`)
	tk.MustExec(`INSERT INTO t VALUES ('Aa', 'Ab', 'Ac'), ('Ba', 'Bb', 'Bc')`)
	tk.MustQuery(`SELECT * FROM t WHERE b = 'Ab'`).Check(testkit.Rows("Aa Ab Ac"))
}

func TestExplainPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE t (a int, b int) PARTITION BY hash(a) PARTITIONS 3`)
	tk.MustExec(`INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6)`)
	tk.MustExec(`analyze table t`)
	tk.MustExec(`set tidb_partition_prune_mode = 'static'`)
	tk.MustQuery(`EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE a = 3`).Check(testkit.Rows(""+
		`TableReader 1.00 root  data:Selection`,
		`└─Selection 1.00 cop[tikv]  eq(test.t.a, 3)`,
		`  └─TableFullScan 2.00 cop[tikv] table:t, partition:p0 keep order:false`))
	tk.MustExec(`set tidb_partition_prune_mode = 'dynamic'`)
	tk.MustQuery(`EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE a = 3`).Check(testkit.Rows(""+
		`TableReader 1.00 root partition:p0 data:Selection`,
		`└─Selection 1.00 cop[tikv]  eq(test.t.a, 3)`,
		`  └─TableFullScan 6.00 cop[tikv] table:t keep order:false`))
	tk.MustExec(`drop table t`)

	tk.MustExec(`CREATE TABLE t (a int unsigned primary key, b int) PARTITION BY hash(a) PARTITIONS 3`)
	tk.MustExec(`INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6)`)
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`SELECT * FROM t WHERE a = 3`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE a = 3`).Check(testkit.Rows("Point_Get 1.00 root table:t, partition:p0 handle:3"))
}

func TestPruningOverflow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("CREATE TABLE t (a int NOT NULL, b bigint NOT NULL,PRIMARY KEY (a,b)) PARTITION BY HASH ((a*b))PARTITIONS 13")
	tk.MustExec(`insert into t values(0, 3522101843073676459)`)
	tk.MustQuery(`SELECT a, b FROM t WHERE a IN (0,14158354938390,0) AND b IN (3522101843073676459,-2846203247576845955,838395691793635638)`).Check(testkit.Rows("0 3522101843073676459"))
}

func TestPartitionCoverage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set tidb_partition_prune_mode = 'dynamic'`)
	tk.MustExec(`create table t (id int, d date, filler varchar(255))`)
	tk.MustExec(`insert into t (id, d) values (1, '2024-02-29'), (2,'2024-03-01')`)
	tk.MustExec(`alter table t partition by list (YEAR(d)) (partition p0 values in  (2024,2025), partition p1 values in (2023))`)
	tk.MustQuery(`select id,d from t partition (p0)`).Check(testkit.Rows("1 2024-02-29", "2 2024-03-01"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustQuery(`select id,d from t partition (p0)`).Check(testkit.Rows("1 2024-02-29", "2 2024-03-01"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustQuery(`select id,d from t partition (p1)`).Check(testkit.Rows())
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustQuery(`select id,d from t partition (p1)`).Check(testkit.Rows())
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`update t set filler = 'updated' where id = 1`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`drop table t`)
	tk.MustExec(`create table t (a int, b int, primary key (a,b)) partition by hash(b) partitions 3`)
	tk.MustExec(`insert into t values (1,1),(1,2),(2,1),(2,2),(1,3)`)
	tk.MustExec(`analyze table t`)
	tk.MustExec(`set tidb_partition_prune_mode = 'static'`)
	query := `select * from t where a in (1,2) and b = 1 order by a`
	tk.MustQuery(`explain format='brief' ` + query).Check(testkit.Rows("Batch_Point_Get 2.00 root table:t, partition:p1, clustered index:PRIMARY(a, b) keep order:true, desc:false"))
	tk.MustQuery(query).Check(testkit.Rows("1 1", "2 1"))
	tk.MustExec(`set tidb_partition_prune_mode = 'dynamic'`)
	tk.MustQuery(`explain format='brief' ` + query).Check(testkit.Rows(""+
		"TableReader 2.00 root partition:p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t range:[1 1,1 1], [2 1,2 1], keep order:true"))
	tk.MustQuery(query).Check(testkit.Rows("1 1", "2 1"))

	query = `select * from t where a = 1 and b in (1,2)`
	tk.MustExec(`set tidb_partition_prune_mode = 'static'`)
	tk.MustQuery(`explain format='brief' ` + query).Check(testkit.Rows(""+
		"PartitionUnion 2.00 root  ",
		"├─Batch_Point_Get 2.00 root table:t, partition:p1, clustered index:PRIMARY(a, b) keep order:false, desc:false",
		"└─Batch_Point_Get 2.00 root table:t, partition:p2, clustered index:PRIMARY(a, b) keep order:false, desc:false"))

	tk.MustQuery(query).Sort().Check(testkit.Rows("1 1", "1 2"))
	tk.MustExec(`set tidb_partition_prune_mode = 'dynamic'`)
	tk.MustQuery(`explain format='brief' ` + query).Check(testkit.Rows(""+
		"TableReader 3.00 root partition:p1,p2 data:TableRangeScan",
		"└─TableRangeScan 3.00 cop[tikv] table:t range:[1 1,1 1], [1 2,1 2], keep order:false"))
	tk.MustQuery(query).Sort().Check(testkit.Rows("1 1", "1 2"))
	tk.MustExec(`drop table t`)

	tk.MustExec(`create table t (a int) partition by range (a) (partition p values less than (10))`)
	tk.MustExec(`insert into t values (1)`)
	tk.MustQuery(`explain format='brief' select * from t where a = 10`).Check(testkit.Rows(""+
		"TableReader 10.00 root partition:dual data:Selection",
		"└─Selection 10.00 cop[tikv]  eq(test.t.a, 10)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`explain format='brief' select * from t where a = 10`).Check(testkit.Rows(""+
		`TableReader 0.00 root partition:dual data:Selection`,
		`└─Selection 0.00 cop[tikv]  eq(test.t.a, 10)`,
		`  └─TableFullScan 1.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`select * from t where a = 10`).Check(testkit.Rows())

	tk.MustExec(`drop table t`)
	tk.MustExec(`set @p=1,@q=2,@u=3;`)
	tk.MustExec(`create table t(a int, b int, primary key(a)) partition by hash(a) partitions 2`)
	tk.MustExec(`insert into t values(1,0),(2,0),(3,0),(4,0)`)
	tk.MustQuery(`explain format = 'brief' select * from t where ((a >= 3 and a <= 1) or a = 2) and 1 = 1`).Check(testkit.Rows("Point_Get 1.00 root table:t, partition:p0 handle:2"))
	tk.MustQuery(`select * from t where ((a >= 3 and a <= 1) or a = 2) and 1 = 1`).Sort().Check(testkit.Rows("2 0"))
	tk.MustExec(`prepare stmt from 'select * from t where ((a >= ? and a <= ?) or a = 2) and 1 = 1'`)
	tk.MustQuery(`execute stmt using @p,@p`).Sort().Check(testkit.Rows("1 0", "2 0"))
	tk.MustQuery(`execute stmt using @q,@q`).Sort().Check(testkit.Rows("2 0"))
	tk.MustQuery(`execute stmt using @p,@u`).Sort().Check(testkit.Rows("1 0", "2 0", "3 0"))
	tk.MustQuery(`execute stmt using @u,@p`).Sort().Check(testkit.Rows("2 0"))

	tk.MustExec(`create table t19141 (c_int int, primary key (c_int)) partition by hash ( c_int ) partitions 4`)
	tk.MustExec(`insert into t19141 values (1), (2), (3), (4)`)
	tk.MustQuery(`explain format = 'brief' select * from t19141 partition (p0)`).Check(testkit.Rows(""+
		"TableReader 10000.00 root partition:p0 data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:t19141 keep order:false, stats:pseudo"))
	tk.MustQuery(`select * from t19141 partition (p0)`).Sort().Check(testkit.Rows("4"))
	tk.MustQuery(`select * from t19141 partition (p0) where c_int = 1`).Sort().Check(testkit.Rows())
	tk.MustExec(`update t19141 partition (p0) set c_int = -c_int where c_int = 1`)
	tk.MustQuery(`select * from t19141 order by c_int`).Sort().Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery(`select * from t19141 partition (p0, p2) where c_int in (1,2,3)`).Sort().Check(testkit.Rows("2"))
	tk.MustExec(`update t19141 partition (p1) set c_int = -c_int where c_int in (2,3)`)
	tk.MustQuery(`select * from t19141 order by c_int`).Sort().Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec(`delete from t19141 partition (p0) where c_int in (2,3)`)
	tk.MustQuery(`select * from t19141 order by c_int`).Sort().Check(testkit.Rows("1", "2", "3", "4"))
}

// Issue TiDB #51090.
func TestAlterTablePartitionRollback(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk4 := testkit.NewTestKit(t, store)
	tk5 := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk2.MustExec(`use test;`)
	tk3.MustExec(`use test;`)
	tk4.MustExec(`use test;`)
	tk5.MustExec(`use test;`)
	tk.MustExec(`create table t(a int);`)
	tk.MustExec(`insert into t values(1), (2), (3);`)

	alterChan := make(chan error)
	alterPartition := func() {
		err := tk4.ExecToErr(`alter table t partition by hash(a) partitions 3;`)
		alterChan <- err
	}
	waitFor := func(s string) {
		for {
			select {
			case alterErr := <-alterChan:
				require.Fail(t, "Alter completed unexpectedly", "With error %v", alterErr)
			default:
				// Alter still running
			}
			res := tk5.MustQuery(`admin show ddl jobs where db_name = 'test' and table_name = 't' and job_type = 'alter table partition by'`).Rows()
			if len(res) > 0 && res[0][4] == s {
				logutil.BgLogger().Info("Got state", zap.String("State", s))
				break
			}
			gotime.Sleep(10 * gotime.Millisecond)
		}
		dom := domain.GetDomain(tk5.Session())
		// Make sure the table schema is the new schema.
		require.NoError(t, dom.Reload())
	}

	testFunc := func(states []string) {
		for i, s := range states {
			if i%2 == 0 {
				tk2.MustExec(`begin;`)
				tk2.MustExec(`select 1 from t;`)
				if i > 0 {
					tk3.MustExec(`commit;`)
				}
			} else {
				tk3.MustExec(`begin;`)
				tk3.MustExec(`select 1 from t;`)
				tk2.MustExec(`commit;`)
			}
			if i == 0 {
				go alterPartition()
			}
			waitFor(s)
			if i == len(states)-1 {
				break
			}
		}
		res := tk.MustQuery(`admin show ddl jobs where table_name = 't' and job_type = 'alter table partition by'`).Rows()
		tk.MustExec(fmt.Sprintf("admin cancel ddl jobs %v", res[0][0]))
		tk2.MustExec(`commit;`)
		tk3.MustExec(`commit;`)
		require.ErrorContains(t, <-alterChan, "[ddl:8214]Cancelled DDL job")
		tk.MustQuery(`show create table t;`).Check(testkit.Rows(
			"t CREATE TABLE `t` (\n" +
				"  `a` int(11) DEFAULT NULL\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		tk.MustQuery(`select a from t order by a;`).Check(testkit.Rows("1", "2", "3"))
	}

	states := []string{"delete only", "write only", "write reorganization", "delete reorganization"}
	for i := range states {
		testFunc(states[:i+1])
	}
}
