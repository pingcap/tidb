// Copyright 2022 PingCAP, Inc.
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

package session_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func TestNonTransactionalDeleteSharding(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")

	tables := []string{
		"create table t(a int, b int, primary key(a, b) clustered)",
		"create table t(a int, b int, primary key(a, b) nonclustered)",
		"create table t(a int, b int, primary key(a) clustered)",
		"create table t(a int, b int, primary key(a) nonclustered)",
		"create table t(a int, b int, key(a, b))",
		"create table t(a int, b int, key(a))",
		"create table t(a int, b int, unique key(a, b))",
		"create table t(a int, b int, unique key(a))",
		"create table t(a varchar(30), b int, primary key(a, b) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a) clustered)",
		"create table t(a varchar(30), b int, primary key(a) nonclustered)",
		"create table t(a varchar(30), b int, key(a, b))",
		"create table t(a varchar(30), b int, key(a))",
		"create table t(a varchar(30), b int, unique key(a, b))",
		"create table t(a varchar(30), b int, unique key(a))",
	}
	tableSizes := []int{0, 1, 10, 35, 40, 100}
	batchSizes := []int{1, 10, 25, 35, 50, 80, 120}
	for _, table := range tables {
		tk.MustExec("drop table if exists t")
		tk.MustExec(table)
		for _, tableSize := range tableSizes {
			for _, batchSize := range batchSizes {
				for i := 0; i < tableSize; i++ {
					tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
				}
				tk.MustQuery(fmt.Sprintf("batch on a limit %d delete from t", batchSize)).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (tableSize+batchSize-1)/batchSize)))
				tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
			}
		}
	}
}

func TestNonTransactionalDeleteDryRun(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	rows := tk.MustQuery("batch on a limit 3 dry run delete from t").Rows()
	for _, row := range rows {
		require.True(t, strings.HasPrefix(row[0].(string), "DELETE FROM `test`.`t` WHERE `a` BETWEEN"))
	}
	tk.MustQuery("batch on a limit 3 dry run query delete from t").Check(testkit.Rows(
		"SELECT `a` FROM `test`.`t` WHERE TRUE ORDER BY IF(ISNULL(`a`),0,1),`a`"))
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
}

func TestNonTransactionalDeleteErrorMessage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/batchDeleteError", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/session/batchDeleteError")
	err := tk.ExecToErr("batch on a limit 3 delete from t")
	require.EqualError(t, err, "Early return: error occurred in the first job. All jobs are canceled: injected batch delete error")

	tk.MustExec("truncate t")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/batchDeleteError", `1*return(false)->return(true)`))
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.ErrorContains(t, err, "33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: DELETE FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, injected batch delete error;\n")

	tk.MustExec("truncate t")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=0")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/batchDeleteError", `1*return(false)->return(true)`))
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.EqualError(t, err, "[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: DELETE FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, err: injected batch delete error")
}

func TestNonTransactionalDeleteSplitOnTiDBRowID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	// auto select results in full col name
	tk.MustQuery("batch limit 3 dry run delete from t").Check(testkit.Rows(
		"DELETE FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 1 AND 3",
		"DELETE FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 100 AND 100",
	))
	// otherwise the name is the same as what is given
	tk.MustQuery("batch on _tidb_rowid limit 3 dry run delete from t").Check(testkit.Rows(
		"DELETE FROM `test`.`t` WHERE `_tidb_rowid` BETWEEN 1 AND 3",
		"DELETE FROM `test`.`t` WHERE `_tidb_rowid` BETWEEN 100 AND 100",
	))
	tk.MustQuery("batch on t._tidb_rowid limit 3 dry run delete from t").Check(testkit.Rows(
		"DELETE FROM `test`.`t` WHERE `t`.`_tidb_rowid` BETWEEN 1 AND 3",
		"DELETE FROM `test`.`t` WHERE `t`.`_tidb_rowid` BETWEEN 100 AND 100",
	))
	tk.MustExec("batch on _tidb_rowid limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteNull(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.MustExec("insert into t values (null, null)")
	}

	tk.MustExec("batch on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))

	// all values are null
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (null, null)")
	}
	tk.MustExec("batch on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteSmallBatch(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=1024")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.MustExec("insert into t values (null, null)")
	}
	require.Equal(t, 1, len(tk.MustQuery("batch on a limit 1000 dry run delete from t").Rows()))
	tk.MustExec("batch on a limit 1000 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteShardOnGeneratedColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c double as (sqrt(a * a + b * b)), key(c))")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d, default)", i, i*2))
	}
	tk.MustExec("batch on c limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteAutoDetectShardColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")

	goodTables := []string{
		"create table t(a int, b int)",
		"create table t(a int, b int, primary key(a) clustered)",
		"create table t(a int, b int, primary key(a) nonclustered)",
		"create table t(a int, b int, primary key(a, b) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) nonclustered)",
	}
	badTables := []string{
		"create table t(a int, b int, primary key(a, b) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) clustered)",
	}

	testFunc := func(table string, expectSuccess bool) {
		tk.MustExec("drop table if exists t")
		tk.MustExec(table)
		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		}
		_, err := tk.Exec("batch limit 3 delete from t")
		require.Equal(t, expectSuccess, err == nil)
	}

	for _, table := range goodTables {
		testFunc(table, true)
	}
	for _, table := range badTables {
		testFunc(table, false)
	}
}

func TestNonTransactionalDeleteInvisibleIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	err := tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("CREATE UNIQUE INDEX c1 ON t (a) INVISIBLE")
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("CREATE UNIQUE INDEX c2 ON t (a)")
	tk.MustExec("batch on a limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteIgnoreSelectLimit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("set @@sql_select_limit=3")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.MustExec("batch on a limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteReadStaleness(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("set @@tidb_read_staleness=-100")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.MustExec("batch on a limit 10 delete from t")
	tk.MustExec("set @@tidb_read_staleness=0")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteCheckConstraint(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")

	// For mocked tikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	now := time.Now()
	safePointValue := now.Format(tikvutil.GCTimeFormat)
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf("INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s') ON DUPLICATE KEY UPDATE variable_value = '%[2]s', comment = '%[3]s'", safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("set @a=now(6)")

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_snapshot=@a")
	err := tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("set @@tidb_snapshot=''")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))

	tk.MustExec("set @@tidb_read_consistency=weak")
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.MustExec("set @@tidb_read_consistency=strict")

	tk.MustExec("set autocommit=0")
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.MustExec("set autocommit=1")

	tk.MustExec("begin")
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.MustExec("commit")

	tk.MustExec("SET GLOBAL tidb_enable_batch_dml = 1")
	tk.MustExec("SET tidb_batch_insert = 1")
	tk.MustExec("SET tidb_dml_batch_size = 1")
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.MustExec("SET GLOBAL tidb_enable_batch_dml = 0")
	tk.MustExec("SET tidb_batch_insert = 0")
	tk.MustExec("SET tidb_dml_batch_size = 0")

	err = tk.ExecToErr("batch on a limit 10 delete from t limit 10")
	require.EqualError(t, err, "Non-transactional delete doesn't support limit")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))

	err = tk.ExecToErr("batch on a limit 10 delete from t order by a")
	require.EqualError(t, err, "Non-transactional delete doesn't support order by")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))

	err = tk.ExecToErr("prepare nt FROM 'batch limit 1 delete from t'")
	require.EqualError(t, err, "[executor:1295]This command is not supported in the prepared statement protocol yet")
}

func TestNonTransactionalDeleteOptimizerHints(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	result := tk.MustQuery("batch on a limit 10 dry run delete /*+ USE_INDEX(t) */ from t").Rows()[0][0].(string)
	require.Equal(t, result, "DELETE /*+ USE_INDEX(`t` )*/ FROM `test`.`t` WHERE `a` BETWEEN 0 AND 9")
}

func TestNonTransactionalDeleteMultiTables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}

	tk.MustExec("create table t1(a int, b int, key(a))")
	tk.MustExec("insert into t1 values (1, 1)")
	err := tk.ExecToErr("batch limit 1 delete t, t1 from t, t1 where t.a = t1.a")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("1"))
}

func TestNonTransactionalDeleteAlias(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	goodBatchStmts := []string{
		"batch on test.t1.a limit 5 delete t1.* from test.t as t1",
		"batch on a limit 5 delete t1.* from test.t as t1",
		"batch on _tidb_rowid limit 5 delete from test.t as t1",
		"batch on t1._tidb_rowid limit 5 delete from test.t as t1",
		"batch on test.t1._tidb_rowid limit 5 delete from test.t as t1",
		"batch limit 5 delete from test.t as t1", // auto assigns table name to be the alias
	}

	badBatchStmts := []string{
		"batch on test.t.a limit 5 delete t1.* from test.t as t1",
		"batch on t.a limit 5 delete t1.* from test.t as t1",
		"batch on t._tidb_rowid limit 5 delete from test.t as t1",
		"batch on test.t._tidb_rowid limit 5 delete from test.t as t1",
	}

	tk.MustExec("create table test.t(a int, b int, key(a))")
	tk.MustExec("create table test.t2(a int, b int, key(a))")

	for _, sql := range goodBatchStmts {
		for i := 0; i < 5; i++ {
			tk.MustExec(fmt.Sprintf("insert into test.t values (%d, %d)", i, i*2))
		}
		tk.MustExec(sql)
		tk.MustQuery("select count(*) from test.t").Check(testkit.Rows("0"))
	}

	for i := 0; i < 5; i++ {
		tk.MustExec(fmt.Sprintf("insert into test.t values (%d, %d)", i, i*2))
	}
	for _, sql := range badBatchStmts {
		err := tk.ExecToErr(sql)
		require.Error(t, err)
		tk.MustQuery("select count(*) from test.t").Check(testkit.Rows("5"))
	}
}

func TestNonTransactionalDeleteShardOnUnsupportedTypes(t *testing.T) {
	// When some day the test fail because such types are supported, we can update related docs and consider remove the test.
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a set('e0', 'e1', 'e2'), b int, primary key(a) clustered, key(b))")
	tk.MustExec("insert into t values ('e2,e0', 3)")
	err := tk.ExecToErr("batch limit 1 delete from t where a = 'e0,e2'")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	tk.MustExec("create table t2(a enum('e0', 'e1', 'e2'), b int, key(a))")
	tk.MustExec("insert into t2 values ('e0', 1)")
	err = tk.ExecToErr("batch on a limit 1 delete from t2")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t2").Check(testkit.Rows("1"))
}
