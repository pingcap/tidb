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
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func TestNonTransactionalDMLSharding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")

	// On int
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

	// On varchar
	tables = []string{
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/batchDMLError")
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
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, injected batch(non-transactional) DML error;\n",
	)
	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5 ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`, injected batch(non-transactional) DML error;\n",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = 42")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: UPDATE `test`.`t` SET `b`=42 WHERE `a` BETWEEN 3 AND 5, injected batch(non-transactional) DML error;\n",
	)

	tk.MustExec("set @@tidb_redact_log=marker")
	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = 32")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: ‹UPDATE `test`.`t` SET `b`=32 WHERE `a` BETWEEN 3 AND 5›, injected batch(non-transactional) DML error;\n",
	)
	tk.MustExec("set @@tidb_redact_log=0")

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
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
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5 ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = b + 42")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: UPDATE `test`.`t` SET `b`=(`b` + 42) WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: DELETE FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)
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
