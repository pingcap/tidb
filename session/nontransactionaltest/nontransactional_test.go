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

package nontransactionaltest

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

func TestNonTransactionalDMLShardingOnInt(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	}
	testSharding(tables, tk, "int")
}

func TestNonTransactionalDMLShardingOnVarchar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")

	tables := []string{
		"create table t(a varchar(30), b int, primary key(a, b) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a) clustered)",
		"create table t(a varchar(30), b int, primary key(a) nonclustered)",
		"create table t(a varchar(30), b int, key(a, b))",
		"create table t(a varchar(30), b int, key(a))",
		"create table t(a varchar(30), b int, unique key(a, b))",
		"create table t(a varchar(30), b int, unique key(a))",
	}
	testSharding(tables, tk, "varchar(30)")
}

func testSharding(tables []string, tk *testkit.TestKit, tp string) {
	compositions := []struct{ tableSize, batchSize int }{
		{0, 10},
		{1, 1},
		{1, 2},
		{30, 25},
		{30, 35},
		{35, 25},
		{35, 35},
		{35, 40},
		{40, 25},
		{40, 35},
		{100, 25},
		{100, 40},
	}
	tk.MustExec("drop table if exists t2")
	tk.MustExec(fmt.Sprintf("create table t2(a %s, b int, primary key(a) clustered)", tp))
	for _, table := range tables {
		tk.MustExec("drop table if exists t, t1")
		tk.MustExec(table)
		tk.MustExec(strings.Replace(table, "create table t", "create table t1", 1))
		for _, c := range compositions {
			tk.MustExec("truncate t2")
			rows := make([]string, 0, c.tableSize)
			for i := 0; i < c.tableSize; i++ {
				tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
				tk.MustExec(fmt.Sprintf("insert into t2 values ('%d', %d)", i, i))
				rows = append(rows, fmt.Sprintf("%d %d", i, i*2))
			}
			tk.MustExec("truncate t1")
			tk.MustQuery(
				fmt.Sprintf("batch on a limit %d insert into t1 select * from t", c.batchSize),
			).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (c.tableSize+c.batchSize-1)/c.batchSize)))
			tk.MustQuery("select a, b from t1 order by b").
				Check(testkit.Rows(rows...))
			tk.MustQuery(
				fmt.Sprintf("batch on a limit %d insert into t2 select * from t on duplicate key update t2.b = t.b", c.batchSize),
			).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (c.tableSize+c.batchSize-1)/c.batchSize)))
			tk.MustQuery("select a, b from t2 order by b").
				Check(testkit.Rows(rows...))
			tk.MustQuery(
				fmt.Sprintf(
					"batch on a limit %d update t set b = b * 2", c.batchSize,
				),
			).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (c.tableSize+c.batchSize-1)/c.batchSize)))
			tk.MustQuery("select coalesce(sum(b), 0) from t").Check(
				testkit.Rows(
					fmt.Sprintf(
						"%d", (c.tableSize-1)*c.tableSize*2,
					),
				),
			)
			tk.MustQuery(
				fmt.Sprintf(
					"batch on a limit %d delete from t", c.batchSize,
				),
			).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (c.tableSize+c.batchSize-1)/c.batchSize)))
			tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
		}
	}
}

func TestNonTransactionalDMLDryRun(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, primary key(a, b) clustered)")
	tk.MustExec("create table t1(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	rows := tk.MustQuery("batch on a limit 3 dry run insert into t1 select * from t").Rows()
	for _, row := range rows {
		col := row[0].(string)
		require.True(t, strings.HasPrefix(col,
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN"), col)
	}
	rows = tk.MustQuery("batch on a limit 3 dry run insert into t1 select * from t on duplicate key update t1.b=t.b").Rows()
	for _, row := range rows {
		col := row[0].(string)
		require.True(t, strings.HasPrefix(col,
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN"), col)
		require.True(t, strings.HasSuffix(col,
			"ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`"), col)
	}
	rows = tk.MustQuery("batch on a limit 3 dry run delete from t").Rows()
	for _, row := range rows {
		col := row[0].(string)
		require.True(t, strings.HasPrefix(col, "DELETE FROM `test`.`t` WHERE `a` BETWEEN"), col)
	}
	rows = tk.MustQuery("batch on a limit 3 dry run update t set b = b + 42").Rows()
	for _, row := range rows {
		col := row[0].(string)
		require.True(t, strings.HasPrefix(col, "UPDATE `test`.`t` SET `b`=(`b` + 42) WHERE `a` BETWEEN"), col)
	}
	querySQL := "SELECT `a` FROM `test`.`t` WHERE TRUE ORDER BY IF(ISNULL(`a`),0,1),`a`"
	tk.MustQuery("batch on a limit 3 dry run query insert into t1 select * from t").Check(testkit.Rows(querySQL))
	tk.MustQuery("batch on a limit 3 dry run query insert into t1 select * from t on duplicate key update t1.b=t.b").
		Check(testkit.Rows(querySQL))
	tk.MustQuery("batch on a limit 3 dry run query delete from t").Check(testkit.Rows(querySQL))
	tk.MustQuery("batch on a limit 3 dry run query update t set b = b + 42").Check(testkit.Rows(querySQL))
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("9900"))
}

func TestNonTransactionalDMLErrorMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, primary key(a, b) clustered)")
	tk.MustExec("create table t1(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/session/batchDMLError")
	err := tk.ExecToErr("batch on a limit 3 insert into t1 select * from t")
	require.EqualError(
		t, err,
		"Early return: error occurred in the first job. All jobs are canceled: injected batch(non-transactional) DML error",
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.EqualError(
		t, err,
		"Early return: error occurred in the first job. All jobs are canceled: injected batch(non-transactional) DML error",
	)
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.EqualError(
		t, err,
		"Early return: error occurred in the first job. All jobs are canceled: injected batch(non-transactional) DML error",
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = 42")
	require.EqualError(
		t, err,
		"Early return: error occurred in the first job. All jobs are canceled: injected batch(non-transactional) DML error",
	)

	tk.MustExec("truncate t")
	tk.MustExec("truncate t1")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=1")

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, injected batch(non-transactional) DML error;\n",
	)
	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5 ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`, injected batch(non-transactional) DML error;\n",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = 42")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: UPDATE `test`.`t` SET `b`=42 WHERE `a` BETWEEN 3 AND 5, injected batch(non-transactional) DML error;\n",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: DELETE FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, injected batch(non-transactional) DML error;\n",
	)

	tk.MustExec("truncate t")
	tk.MustExec("truncate t1")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=0")

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5 ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = b + 42")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: UPDATE `test`.`t` SET `b`=(`b` + 42) WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: DELETE FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)
}

func TestNonTransactionalDMLShardingOnTiDBRowID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table t1(a int, b int, unique key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	// auto select results in full col name
	tk.MustQuery("batch limit 3 dry run delete from t").Check(
		testkit.Rows(
			"DELETE FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 1 AND 3",
			"DELETE FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch limit 3 dry run update t set b = 42").Check(
		testkit.Rows(
			"UPDATE `test`.`t` SET `b`=42 WHERE `test`.`t`.`_tidb_rowid` BETWEEN 1 AND 3",
			"UPDATE `test`.`t` SET `b`=42 WHERE `test`.`t`.`_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch limit 3 dry run insert into t1 select * from t").Check(
		testkit.Rows(
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 1 AND 3",
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch limit 3 dry run insert into t1 select * from t on duplicate key update t1.b=t.b").Check(
		testkit.Rows(
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 1 AND 3"+
				" ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`",
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 100 AND 100"+
				" ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`",
		),
	)

	// otherwise the name is the same as what is given
	tk.MustQuery("batch on _tidb_rowid limit 3 dry run delete from t").Check(
		testkit.Rows(
			"DELETE FROM `test`.`t` WHERE `_tidb_rowid` BETWEEN 1 AND 3",
			"DELETE FROM `test`.`t` WHERE `_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch on t._tidb_rowid limit 3 dry run delete from t").Check(
		testkit.Rows(
			"DELETE FROM `test`.`t` WHERE `t`.`_tidb_rowid` BETWEEN 1 AND 3",
			"DELETE FROM `test`.`t` WHERE `t`.`_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch on _tidb_rowid limit 3 dry run update t set b = 42").Check(
		testkit.Rows(
			"UPDATE `test`.`t` SET `b`=42 WHERE `_tidb_rowid` BETWEEN 1 AND 3",
			"UPDATE `test`.`t` SET `b`=42 WHERE `_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch on t._tidb_rowid limit 3 dry run update t set b = 42").Check(
		testkit.Rows(
			"UPDATE `test`.`t` SET `b`=42 WHERE `t`.`_tidb_rowid` BETWEEN 1 AND 3",
			"UPDATE `test`.`t` SET `b`=42 WHERE `t`.`_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch on _tidb_rowid limit 3 dry run insert into t1 select * from t").Check(
		testkit.Rows(
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `_tidb_rowid` BETWEEN 1 AND 3",
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch on t._tidb_rowid limit 3 dry run insert into t1 select * from t").Check(
		testkit.Rows(
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `t`.`_tidb_rowid` BETWEEN 1 AND 3",
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `t`.`_tidb_rowid` BETWEEN 100 AND 100",
		),
	)
	tk.MustQuery("batch on _tidb_rowid limit 3 dry run insert into t1 select * from t on duplicate key update t1.b=t.b").Check(
		testkit.Rows(
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `_tidb_rowid` BETWEEN 1 AND 3"+
				" ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`",
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `_tidb_rowid` BETWEEN 100 AND 100"+
				" ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`",
		),
	)
	tk.MustQuery("batch on t._tidb_rowid limit 3 dry run insert into t1 select * from t on duplicate key update t1.b=t.b").Check(
		testkit.Rows(
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `t`.`_tidb_rowid` BETWEEN 1 AND 3"+
				" ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`",
			"INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `t`.`_tidb_rowid` BETWEEN 100 AND 100"+
				" ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`",
		),
	)

	tk.MustExec("batch on _tidb_rowid limit 3 insert into t1 select * from t")
	tk.MustQuery("select * from t1 order by a").
		Check(tk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("update t1 set b = 0")
	tk.MustExec("batch on _tidb_rowid limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	tk.MustQuery("select * from t1 order by a").
		Check(tk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("batch on _tidb_rowid limit 3 update t set b = 42")
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("4200"))
	tk.MustExec("batch on _tidb_rowid limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalWithNull(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t1(a int, b int, key(a))")
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
	tk.MustExec("batch on a limit 3 insert into t1 select * from t")
	tk.MustQuery("select * from t1 order by a").
		Check(tk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("200"))
	tk.MustExec("batch on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalWithSmallBatch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=1024")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t1(a int, b int, unique key(a))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.MustExec("insert into t values (null, null)")
	}
	require.Equal(t, 1, len(tk.MustQuery("batch on a limit 1000 dry run delete from t").Rows()))
	require.Equal(t, 1, len(tk.MustQuery("batch on a limit 1000 dry run insert into t1 select * from t").Rows()))
	require.Equal(t, 1, len(tk.MustQuery("batch on a limit 1000 dry run insert into t1 select * from t on duplicate key update t1.b=t.b").Rows()))
	tk.MustExec("batch on a limit 1000 insert into t1 select * from t")
	tk.MustQuery("select * from t1 order by a").
		Check(tk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("update t1 set b = a")
	// non-null rows will be updated, and null rows will be duplicated
	tk.MustExec("batch on a limit 1000 insert into t1 select * from t on duplicate key update t1.b=t.b")
	tk.MustQuery("select * from t1 where a is not null order by a").
		Check(tk.MustQuery("select * from t where a is not null order by a").Rows())
	tk.MustQuery("select count(*) from t1 where a is null").Check(testkit.Rows("20"))
	tk.MustExec("batch on a limit 1000 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalWithShardOnGeneratedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, c double as (sqrt(a * a + b * b)), key(c))")
	tk.MustExec("create table t1(a int, b int, c double as (sqrt(a * a + b * b)), unique key(c))")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d, default)", i, i*2))
	}
	tk.MustExec("batch on c limit 10 insert into t1(a, b) select a, b from t")
	tk.MustQuery("select * from t1 order by a").
		Check(tk.MustQuery("select * from t order by a").Rows())
	// swap a and b
	tk.MustExec("update t1 set a=b, b=a")
	tk.MustQuery("select b, a from t1 order by a").
		Check(tk.MustQuery("select a, b from t order by a").Rows())
	tk.MustExec("batch on c limit 10 insert into t1(a, b) select a, b from t on duplicate key update t1.a=t.a, t1.b=t.b")
	// insert on duplicate key update should swap back a and b
	tk.MustQuery("select * from t1 order by a").
		Check(tk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("batch on c limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalWithAutoDetectShardColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
		tk.MustExec("drop table if exists t, t1")
		tk.MustExec(table)
		tk.MustExec(strings.Replace(table, "create table t", "create table t1", 1))
		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		}
		_, err := tk.Exec("batch limit 3 insert into t1 select * from t")
		require.Equal(t, expectSuccess, err == nil)
		_, err = tk.Exec("batch limit 3 insert into t1 select * from t on duplicate key update t1.a=t.a, t1.b=t.b")
		require.Equal(t, expectSuccess, err == nil)
		_, err = tk.Exec("batch limit 3 delete from t")
		require.Equal(t, expectSuccess, err == nil)
	}

	for _, table := range goodTables {
		testFunc(table, true)
	}
	for _, table := range badTables {
		testFunc(table, false)
	}
}

func TestNonTransactionalWithInvisibleIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table t1(a int, b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	err := tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("CREATE UNIQUE INDEX c1 ON t (a) INVISIBLE")
	tk.MustExec("CREATE UNIQUE INDEX c1 ON t1 (a) INVISIBLE")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("CREATE UNIQUE INDEX c2 ON t (a)")
	tk.MustExec("CREATE UNIQUE INDEX c2 ON t1 (a)")
	tk.MustExec("batch on a limit 10 insert into t1 select * from t")
	tk.MustQuery("select * from t1 order by a").
		Check(tk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("update t1 set b=a")
	tk.MustExec("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	tk.MustQuery("select * from t1 order by a").
		Check(tk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("batch on a limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalWithIgnoreSelectLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	checkTk := testkit.NewTestKit(t, store)
	checkTk.MustExec("use test")
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("set @@sql_select_limit=3")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t1(a int, b int, unique key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.MustExec("batch on a limit 10 insert into t1 select * from t")
	checkTk.MustQuery("select * from t1 order by a").
		Check(checkTk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("update t1 set b=a")
	tk.MustExec("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	checkTk.MustQuery("select * from t1 order by a").
		Check(checkTk.MustQuery("select * from t order by a").Rows())
	tk.MustExec("batch on a limit 10 delete from t")
	checkTk.MustQuery("select * from t").Check(testkit.Rows())
}

func TestNonTransactionalWithReadStaleness(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	checkTk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("set @@tidb_read_staleness=-100")
	tk.MustExec("use test")
	checkTk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t1(a int, b int, unique key(a))")
	rows := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
		rows = append(rows, fmt.Sprintf("%d %d", i, i*2))
	}
	tk.MustExec("batch on a limit 10 insert into t1 select * from t")
	checkTk.MustQuery("select * from t1 order by a").Check(testkit.Rows(rows...))
	checkTk.MustExec("update t1 set b=a")
	tk.MustExec("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	tk.MustExec("batch on a limit 10 delete from t")
	tk.MustExec("set @@tidb_read_staleness=0")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows(rows...))
}

func TestNonTransactionalWithCheckConstraint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t1(a int, b int, key(a))")

	checkFn := func() {
		tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
		tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	}

	// For mocked tikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	now := time.Now()
	safePointValue := now.Format(tikvutil.GCTimeFormat)
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(
		"INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s') ON DUPLICATE KEY UPDATE variable_value = '%[2]s', comment = '%[3]s'",
		safePointName, safePointValue, safePointComment,
	)
	tk.MustExec(updateSafePoint)

	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("set @a=now(6)")

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_snapshot=@a")
	err := tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("set @@tidb_snapshot=''")
	checkFn()

	tk.MustExec("set @@tidb_read_consistency=weak")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("set @@tidb_read_consistency=strict")
	checkFn()

	tk.MustExec("set autocommit=0")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("commit")
	tk.MustExec("set autocommit=1")
	checkFn()

	tk.MustExec("begin")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("commit")
	checkFn()

	tk.MustExec("SET GLOBAL tidb_enable_batch_dml = 1")
	tk.MustExec("SET tidb_batch_insert = 1")
	tk.MustExec("SET tidb_dml_batch_size = 1")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("SET GLOBAL tidb_enable_batch_dml = 0")
	tk.MustExec("SET tidb_batch_insert = 0")
	tk.MustExec("SET tidb_dml_batch_size = 0")
	checkFn()

	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t limit 10")
	require.EqualError(t, err, "Non-transactional statements don't support limit")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t limit 10 on duplicate key update t1.b=t.b")
	require.EqualError(t, err, "Non-transactional statements don't support limit")
	err = tk.ExecToErr("batch on a limit 10 delete from t limit 10")
	require.EqualError(t, err, "Non-transactional statements don't support limit")
	checkFn()

	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t order by a")
	require.EqualError(t, err, "Non-transactional statements don't support order by")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t order by a on duplicate key update t1.b=t.b")
	require.EqualError(t, err, "Non-transactional statements don't support order by")
	err = tk.ExecToErr("batch on a limit 10 delete from t order by a")
	require.EqualError(t, err, "Non-transactional statements don't support order by")
	checkFn()

	err = tk.ExecToErr("prepare nt FROM 'batch limit 1 insert into t1 select * from t'")
	require.EqualError(t, err, "[executor:1295]This command is not supported in the prepared statement protocol yet")
	err = tk.ExecToErr("prepare nt FROM 'batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b'")
	require.EqualError(t, err, "[executor:1295]This command is not supported in the prepared statement protocol yet")
	err = tk.ExecToErr("prepare nt FROM 'batch limit 1 delete from t'")
	require.EqualError(t, err, "[executor:1295]This command is not supported in the prepared statement protocol yet")

	err = tk.ExecToErr("batch limit 1 insert into t select 1, 1")
	require.EqualError(t, err, "table reference is nil")
	err = tk.ExecToErr("batch limit 1 insert into t select * from (select 1, 2) tmp")
	require.EqualError(t, err, "Non-transactional DML, table name not found in join")
}

func TestNonTransactionalWithOptimizerHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t1(a int, b int, key(a))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	result := tk.MustQuery("batch on a limit 10 dry run insert into t1 select /*+ USE_INDEX(t) */ * from t").Rows()[0][0].(string)
	require.Equal(t, result, "INSERT INTO `test`.`t1` SELECT /*+ USE_INDEX(`t` )*/ * FROM `test`.`t` WHERE `a` BETWEEN 0 AND 9")
	result = tk.MustQuery("batch on a limit 10 dry run insert into t1 select /*+ USE_INDEX(t) */ * from t on duplicate key update t1.b=t.b").Rows()[0][0].(string)
	require.Equal(t, result, "INSERT INTO `test`.`t1` SELECT /*+ USE_INDEX(`t` )*/ * FROM `test`.`t` WHERE `a` BETWEEN 0 AND 9 ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`")
	result = tk.MustQuery("batch on a limit 10 dry run delete /*+ USE_INDEX(t) */ from t").Rows()[0][0].(string)
	require.Equal(t, result, "DELETE /*+ USE_INDEX(`t` )*/ FROM `test`.`t` WHERE `a` BETWEEN 0 AND 9")
}

func TestNonTransactionalWithMultiTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t1(a int, b int, key(a))")
	tk.MustExec("create table t2(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.MustExec("insert into t1 values (1, 1)")

	err := tk.ExecToErr("batch limit 1 insert into t2 select t.a, t1.b from t, t1 where t.a = t1.a")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t2").Check(testkit.Rows("0"))
	err = tk.ExecToErr("batch limit 1 insert into t2 select t.a, t1.b from t, t1 where t.a = t1.a on duplicate key update t2.b=t.b")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t2").Check(testkit.Rows("0"))
	err = tk.ExecToErr("batch limit 1 delete t, t1 from t, t1 where t.a = t1.a")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("1"))
}

func TestNonTransactionalWithAlias(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	goodBatchDeletes := []string{
		"batch on test.t1.a limit 5 delete t1.* from test.t as t1",
		"batch on a limit 5 delete t1.* from test.t as t1",
		"batch on _tidb_rowid limit 5 delete from test.t as t1",
		"batch on t1._tidb_rowid limit 5 delete from test.t as t1",
		"batch on test.t1._tidb_rowid limit 5 delete from test.t as t1",
		"batch limit 5 delete from test.t as t1", // auto assigns table name to be the alias
	}

	badBatchDeletes := []string{
		"batch on test.t.a limit 5 delete t1.* from test.t as t1",
		"batch on t.a limit 5 delete t1.* from test.t as t1",
		"batch on t._tidb_rowid limit 5 delete from test.t as t1",
		"batch on test.t._tidb_rowid limit 5 delete from test.t as t1",
	}

	goodBatchInserts := []string{
		"batch on test.t1.a limit 5 insert into test.s select t1.* from test.t as t1",
		"batch on a limit 5 insert into test.s select t1.* from test.t as t1",
		"batch on _tidb_rowid limit 5 insert into test.s select t1.* from test.t as t1",
		"batch on t1._tidb_rowid limit 5 insert into test.s select t1.* from test.t as t1",
		"batch on test.t1._tidb_rowid limit 5 insert into test.s select t1.* from test.t as t1",
		"batch limit 5 insert into test.s select t1.* from test.t as t1", // auto assigns table name to be the alias
	}

	badBatchInserts := []string{
		"batch on test.t.a limit 5 insert into test.s select t1.* from test.t as t1",
		"batch on t.a limit 5 insert into test.s select t1.* from test.t as t1",
		"batch on t._tidb_rowid limit 5 insert into test.s select t1.* from test.t as t1",
		"batch on test.t._tidb_rowid limit 5 insert into test.s select t1.* from test.t as t1",
	}

	tk.MustExec("drop table if exists test.t, test.s, test.t2")
	tk.MustExec("create table test.t(a int, b int, key(a))")
	tk.MustExec("create table test.s(a int, b int, unique key(a))")
	tk.MustExec("create table test.t2(a int, b int, key(a))")

	for _, sql := range goodBatchDeletes {
		for i := 0; i < 5; i++ {
			tk.MustExec(fmt.Sprintf("insert into test.t values (%d, %d)", i, i*2))
		}
		tk.MustExec(sql)
		tk.MustQuery("select count(*) from test.t").Check(testkit.Rows("0"))
	}

	rows := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		tk.MustExec(fmt.Sprintf("insert into test.t values (%d, %d)", i, i*2))
		rows = append(rows, fmt.Sprintf("%d %d", i, i*2))
	}

	for _, sql := range goodBatchInserts {
		tk.MustExec(sql)
		tk.MustQuery("select * from test.s order by a").Check(testkit.Rows(rows...))
		tk.MustExec("update test.s set b=a")
		tk.MustExec(sql + " on duplicate key update s.b=t1.b")
		tk.MustQuery("select * from test.s order by a").Check(testkit.Rows(rows...))
		tk.MustExec("truncate test.s")
	}

	for _, sql := range badBatchDeletes {
		err := tk.ExecToErr(sql)
		require.Error(t, err)
		tk.MustQuery("select count(*) from test.t").Check(testkit.Rows("5"))
	}

	for _, sql := range badBatchInserts {
		err := tk.ExecToErr(sql)
		require.Error(t, err)
		tk.MustQuery("select count(*) from test.s").Check(testkit.Rows("0"))
	}
}

func TestNonTransactionalWithShardOnUnsupportedTypes(t *testing.T) {
	// When some day the test fail because such types are supported, we can update related docs and consider remove the test.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2")
	tk.MustExec("create table t(a set('e0', 'e1', 'e2'), b int, primary key(a) clustered, key(b))")
	tk.MustExec("create table t1(a set('e0', 'e1', 'e2'), b int, primary key(a) clustered, key(b))")
	tk.MustExec("insert into t values ('e2,e0', 3)")
	err := tk.ExecToErr("batch limit 1 insert into t1 select * from t where a = 'e0,e2'")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	err = tk.ExecToErr("batch limit 1 insert into t1 select * from t where a = 'e0,e2' on duplicate key update t1.b=t.b")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	err = tk.ExecToErr("batch limit 1 delete from t where a = 'e0,e2'")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	tk.MustExec("create table t2(a enum('e0', 'e1', 'e2'), b int, key(a))")
	tk.MustExec("insert into t2 values ('e0', 1)")
	err = tk.ExecToErr("batch on a limit 1 insert into t1 select * from t2")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	err = tk.ExecToErr("batch on a limit 1 insert into t1 select * from t2 on duplicate key update t1.b=t2.b")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	err = tk.ExecToErr("batch on a limit 1 delete from t2")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t2").Check(testkit.Rows("1"))
}

func TestNonTransactionalWithJoin(t *testing.T) {
	// insert
	// BATCH ON t2.id LIMIT 10000 insert into t1 select id, name from t2 inner join t3 on t2.id = t3.id;
	// insert into t1 select id, name from t2 inner join t3 on t2.id = t3.id where t2.id between 1 and 10000

	// replace
	// BATCH ON t1.id LIMIT 10000 replace into t3
	// 	select * from t1 left join t2 on t1.id = t2.id;

	// update
	// BATCH ON t1.id LIMIT 10000 update t1 join t2 on t1.a=t2.a set t1.a=t1.a*1000;
	// update t1 join t2 on t1.a=t2.a set t1.a=t1.a*1000 where t1.id between 1 and 10000;

	// delete
	// BATCH ON pa.id LIMIT 10000 DELETE pa
	// 	FROM pets_activities pa JOIN pets p ON pa.id = p.pet_id
	// 	WHERE p.order > :order AND p.pet_id = :pet_id
	// delete pa
	// 	from pets_activities pa join pets p on pa.id = p.pet_id
	// 	WHERE p.order > :order AND p.pet_id = :pet_id and pa.id between 1 and 10000;
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, v1 int, v2 int, unique key (id))")
	tk.MustExec("create table t2(id int, v int, key i1(id))")
	tk.MustExec("create table t3(id int, v int, key i1(id))")
	tk.MustExec("insert into t2 values (1, 2), (2, 3), (3, 4)")
	tk.MustExec("insert into t3 values (1, 4), (2, 5), (4, 6)")

	tk.MustExec("batch on test.t2.id limit 1 insert into t select t2.id, t2.v, t3.v from t2 join t3 on t2.id=t3.id")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2 4", "2 3 5"))

	tk.MustContainErrMsg(
		"batch on id limit 1 insert into t select t2.id, t2.v, t3.v from t2 join t3 on t2.id=t3.id",
		"Non-transactional DML, shard column must be fully specified",
	)
	tk.MustContainErrMsg(
		"batch on test.t1.id limit 1 insert into t select t2.id, t2.v, t3.v from t2 join t3 on t2.id=t3.id",
		"shard column test.t1.id is not in the tables involved in the join",
	)

	tk.MustExec("batch on test.t2.id limit 1 update t2 join t3 on t2.id=t3.id set t2.v=t2.v*100, t3.v=t3.v*200")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 200", "2 300", "3 4"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1 800", "2 1000", "4 6"))

	tk.MustExec("batch limit 1 delete t2 from t2 join t3 on t2.id=t3.id")
	tk.MustQuery("select * from t2").Check(testkit.Rows("3 4"))

	tk.MustExec("insert into t2 values (1, 11), (2, 22)")
	tk.MustExec("batch limit 1 replace into t select t2.id, t2.v, t3.v from t2 join t3 on t2.id=t3.id")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 11 800", "2 22 1000"))
}

func TestAnomalousNontransactionalDML(t *testing.T) {
	// some weird and error-prone behavior
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, v int)")

	// self-insert, this is allowed but can be dangerous
	tk.MustExec("insert into t values (1, 1)")
	tk.MustExec("batch limit 1 insert into t select * from t")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "1 1"))
	tk.MustExec("drop table t")

	tk.MustExec("create table t(id int, v int, key(id))")
	tk.MustExec("create table t2(id int, v int, key(id))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (4, 4)")

	tk.MustExec("batch on test.t.id limit 1 update t join t2 on t.id=t2.id set t2.id = t2.id+1")
	tk.MustQuery("select * from t2").Check(testkit.Rows("4 1", "4 2", "4 4"))
}

func TestAlias(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, v1 int, v2 int, unique key (id))")
	tk.MustExec("create table t2(id int, v int, key (id))")
	tk.MustExec("create table t3(id int, v int, key (id))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 20), (4, 40)")
	tk.MustExec("insert into t3 values (2, 21), (4, 41), (5, 50)")
	tk.MustExec("update t as t1 set v1 = test.t1.id + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2 1", "2 3 2", "3 4 3"))

	tk.MustExec("batch on test.tt2.id limit 1 replace into t select tt2.id, tt2.v, t3.v from t2 as tt2 join t3 on tt2.id=t3.id")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 2 1", "2 20 21", "3 4 3", "4 40 41"))
}

func TestUpdatingShardColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, v int, unique key(id))")
	tk.MustExec("create table t2(id int, v int, unique key(id))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (4, 4)")

	// update stmt
	tk.MustContainErrMsg("batch on id limit 1 update t set id=id+1", "Non-transactional DML, shard column cannot be updated")
	// insert on dup update
	tk.MustContainErrMsg("batch on id limit 1 insert into t select * from t on duplicate key update t.id=t.id+10", "Non-transactional DML, shard column cannot be updated")
	// update with table alias
	tk.MustContainErrMsg("batch on id limit 1 update t as t1 set t1.id=t1.id+1", "Non-transactional DML, shard column cannot be updated")
	// insert on dup update with table alias
	tk.MustContainErrMsg("batch on id limit 1 insert into t select * from t as t1 on duplicate key update t1.id=t1.id+10", "Non-transactional DML, shard column cannot be updated")
	// update stmt, multiple table
	tk.MustContainErrMsg("batch on test.t.id limit 1 update t join t2 on t.id=t2.id set t.id=t.id+1", "Non-transactional DML, shard column cannot be updated")
	// update stmt, multiple table, alias
	tk.MustContainErrMsg("batch on test.tt.id limit 1 update t as tt join t2 as tt2 on tt.id=tt2.id set tt.id=tt.id+10", "Non-transactional DML, shard column cannot be updated")
}

func TestNameAmbiguity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, v1 int, v2 int, unique key (id))")
	tk.MustExec("create table t2(id int, v int, key (id))")
	tk.MustExec("create table t3(id int, v int, key (id))")

	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create table t(id int, v1 int, v2 int, unique key (id))")
	tk.MustExec("create table t2(id int, v int, key (id))")
	tk.MustExec("create table t3(id int, v int, key (id))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2)")

	tk.MustExec("use test")
	tk.MustExec("batch on id limit 1 insert into t select * from test2.t")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1", "2 2 2"))
}
