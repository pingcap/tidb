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

package executor_test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestInvalidReadTemporaryTable(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, time.Second)
	tk := testkit.NewTestKit(t, store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp1")
	tk.MustExec("create global temporary table tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))" +
		"on commit delete rows")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp2")
	tk.MustExec("create temporary table tmp2 (id int not null primary key, code int not null, value int default null, unique key code(code));")
	tk.MustExec("create table tmp3 (id int not null primary key, code int not null, value int default null, unique key code(code));")
	tk.MustExec("create table tmp4 (id int not null primary key, code int not null, value int default null, unique key code(code));")
	tk.MustExec("create temporary table tmp5(id int);")
	tk.MustExec("create table tmp6 (id int primary key);")

	// sleep 1us to make test stale
	time.Sleep(time.Microsecond)

	queries := []struct {
		sql string
	}{
		{
			sql: "select * from tmp1 where id=1",
		},
		{
			sql: "select * from tmp1 where code=1",
		},
		{
			sql: "select * from tmp1 where id in (1, 2, 3)",
		},
		{
			sql: "select * from tmp1 where code in (1, 2, 3)",
		},
		{
			sql: "select * from tmp1 where id > 1",
		},
		{
			sql: "select /*+use_index(tmp1, code)*/ * from tmp1 where code > 1",
		},
		{
			sql: "select /*+use_index(tmp1, code)*/ code from tmp1 where code > 1",
		},
		{
			sql: "select /*+ use_index_merge(tmp1, primary, code) */ * from tmp1 where id > 1 or code > 2",
		},
	}

	addStaleReadToSQL := func(sql string) string {
		idx := strings.Index(sql, " where ")
		if idx < 0 {
			return ""
		}
		return sql[0:idx] + " as of timestamp NOW(6)" + sql[idx:]
	}
	genLocalTemporarySQL := func(sql string) string {
		return strings.Replace(sql, "tmp1", "tmp2", -1)
	}

	for _, query := range queries {
		localSQL := genLocalTemporarySQL(query.sql)
		queries = append(queries, struct{ sql string }{sql: localSQL})
	}
	for _, query := range queries {
		sql := addStaleReadToSQL(query.sql)
		if sql != "" {
			tk.MustGetErrMsg(sql, "can not stale read temporary table")
		}
	}

	tk.MustExec("start transaction read only as of timestamp NOW(6)")
	for _, query := range queries {
		tk.MustGetErrMsg(query.sql, "can not stale read temporary table")
	}
	tk.MustExec("commit")

	for _, query := range queries {
		tk.MustQuery(query.sql)
	}

	// Test normal table when local temporary exits.
	tk.MustExec("insert into tmp6 values(1);")
	time.Sleep(100 * time.Millisecond)
	tk.MustExec("set @a=now(6);")
	tk.MustExec("drop table tmp6")
	tk.MustExec("create table tmp6 (id int primary key);")
	tk.MustQuery("select * from tmp6 as of timestamp(@a) where id=1;").Check(testkit.Rows("1"))
	tk.MustQuery("select * from tmp4 as of timestamp(@a), tmp3 as of timestamp(@a) where tmp3.id=1;")
	tk.MustGetErrMsg("select * from tmp4 as of timestamp(@a), tmp2 as of timestamp(@a) where tmp2.id=1;", "can not stale read temporary table")

	tk.MustExec("set transaction read only as of timestamp NOW(6)")
	tk.MustExec("start transaction")
	for _, query := range queries {
		tk.MustGetErrMsg(query.sql, "can not stale read temporary table")
	}
	tk.MustExec("commit")

	for _, query := range queries {
		tk.MustExec(query.sql)
	}

	tk.MustExec("set @@tidb_snapshot=NOW(6)")
	for _, query := range queries {
		// forbidden historical read local temporary table
		if strings.Contains(query.sql, "tmp2") {
			tk.MustGetErrMsg(query.sql, "can not read local temporary table when 'tidb_snapshot' is set")
			continue
		}
		// Will success here for compatibility with some tools like dumping
		tk.MustQuery(query.sql).Check(testkit.Rows())
	}
}

func TestInvalidReadCacheTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists cache_tmp1")
	tk.MustExec("create table cache_tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))")
	tk.MustExec("alter table cache_tmp1 cache")
	tk.MustExec("drop table if exists cache_tmp2")
	tk.MustExec("create table cache_tmp2 (id int not null primary key, code int not null, value int default null, unique key code(code));")
	tk.MustExec("alter table cache_tmp2 cache")
	tk.MustExec("drop table if exists cache_tmp3 , cache_tmp4, cache_tmp5")
	tk.MustExec("create table cache_tmp3 (id int not null primary key, code int not null, value int default null, unique key code(code));")
	tk.MustExec("create table cache_tmp4 (id int not null primary key, code int not null, value int default null, unique key code(code));")
	tk.MustExec("create table cache_tmp5 (id int primary key);")
	// sleep 1us to make test stale
	time.Sleep(time.Microsecond)

	queries := []struct {
		sql string
	}{
		{
			sql: "select * from cache_tmp1 where id=1",
		},
		{
			sql: "select * from cache_tmp1 where code=1",
		},
		{
			sql: "select * from cache_tmp1 where id in (1, 2, 3)",
		},
		{
			sql: "select * from cache_tmp1 where code in (1, 2, 3)",
		},
		{
			sql: "select * from cache_tmp1 where id > 1",
		},
		{
			sql: "select /*+use_index(cache_tmp1, code)*/ * from cache_tmp1 where code > 1",
		},
		{
			sql: "select /*+use_index(cache_tmp1, code)*/ code from cache_tmp1 where code > 1",
		},
	}

	addStaleReadToSQL := func(sql string) string {
		idx := strings.Index(sql, " where ")
		if idx < 0 {
			return ""
		}
		return sql[0:idx] + " as of timestamp NOW(6)" + sql[idx:]
	}
	for _, query := range queries {
		sql := addStaleReadToSQL(query.sql)
		if sql != "" {
			tk.MustGetErrMsg(sql, "can not stale read cache table")
		}
	}

	tk.MustExec("start transaction read only as of timestamp NOW(6)")
	for _, query := range queries {
		tk.MustGetErrMsg(query.sql, "can not stale read cache table")
	}
	tk.MustExec("commit")

	for _, query := range queries {
		tk.MustQuery(query.sql)
	}

	// Test normal table when cache table exits.
	tk.MustExec("insert into cache_tmp5 values(1);")
	time.Sleep(100 * time.Millisecond)
	tk.MustExec("set @a=now(6);")
	tk.MustExec("drop table cache_tmp5")
	tk.MustExec("create table cache_tmp5 (id int primary key);")
	tk.MustQuery("select * from cache_tmp5 as of timestamp(@a) where id=1;").Check(testkit.Rows("1"))
	tk.MustQuery("select * from cache_tmp4 as of timestamp(@a), cache_tmp3 as of timestamp(@a) where cache_tmp3.id=1;")
	tk.MustGetErrMsg("select * from cache_tmp4 as of timestamp(@a), cache_tmp2 as of timestamp(@a) where cache_tmp2.id=1;", "can not stale read cache table")
	tk.MustExec("set transaction read only as of timestamp NOW(6)")
	tk.MustExec("start transaction")
	for _, query := range queries {
		tk.MustGetErrMsg(query.sql, "can not stale read cache table")
	}
	tk.MustExec("commit")

	for _, query := range queries {
		tk.MustExec(query.sql)
	}

	tk.MustExec("set @@tidb_snapshot=NOW(6)")
	for _, query := range queries {
		// enable historical read cache table
		tk.MustExec(query.sql)
	}
}

func TestTxnSavepoint0(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, a int, unique index idx(id))")

	cases := []struct {
		sql string
		sps []string
		err string
	}{
		{"set autocommit=1", nil, ""},
		{"delete from t", nil, ""},

		{"savepoint s1", nil, ""},
		{"rollback to s1", nil, "[executor:1305]SAVEPOINT s1 does not exist"},
		{"begin", nil, ""},
		{"savepoint s1", []string{"s1"}, ""},
		{"savepoint s2", []string{"s1", "s2"}, ""},
		{"savepoint s3", []string{"s1", "s2", "s3"}, ""},
		{"savepoint S1", []string{"s2", "s3", "s1"}, ""},
		{"rollback to S3", []string{"s2", "s3"}, ""},
		{"rollback to S1", []string{"s2", "s3"}, "[executor:1305]SAVEPOINT S1 does not exist"},
		{"rollback to s1", []string{"s2", "s3"}, "[executor:1305]SAVEPOINT s1 does not exist"},
		{"rollback to S3", []string{"s2", "s3"}, ""},
		{"rollback to S2", []string{"s2"}, ""},
		{"rollback to S2", []string{"s2"}, ""},
		{"rollback", nil, ""},

		{"set autocommit=1", nil, ""},
		{"savepoint s1", nil, ""},
		{"set autocommit=0", nil, ""},
		{"savepoint s1", []string{"s1"}, ""},
		{"savepoint s2", []string{"s1", "s2"}, ""},
		{"savepoint S1", []string{"s2", "s1"}, ""},
		{"set autocommit=1", nil, ""},
		{"savepoint s1", nil, ""},

		{"set autocommit=0", nil, ""},
		{"begin", nil, ""},
		{"savepoint s1", []string{"s1"}, ""},
		{"set autocommit=1", nil, ""},
		{"set autocommit=0", nil, ""},
		{"savepoint s1", []string{"s1"}, ""},
		{"commit", nil, ""},

		{"begin", nil, ""},
		{"savepoint s1", []string{"s1"}, ""},
		{"savepoint s2", []string{"s1", "s2"}, ""},
		{"savepoint s3", []string{"s1", "s2", "s3"}, ""},
		{"release savepoint s2", []string{"s1"}, ""},
		{"rollback to S2", []string{"s1"}, "[executor:1305]SAVEPOINT S2 does not exist"},
		{"release savepoint s3", []string{"s1"}, "[executor:1305]SAVEPOINT s3 does not exist"},
		{"savepoint s2", []string{"s1", "s2"}, ""},
		{"release savepoint s1", nil, ""},
		{"release savepoint s1", nil, "[executor:1305]SAVEPOINT s1 does not exist"},
		{"release savepoint S2", nil, "[executor:1305]SAVEPOINT S2 does not exist"},
		{"commit", nil, ""},
	}

	txnModes := []string{"optimistic", "pessimistic", ""}
	for _, txnMode := range txnModes {
		tk.MustExec(fmt.Sprintf("set session tidb_txn_mode='%v';", txnMode))
		for idx, ca := range cases {
			comment := fmt.Sprintf("txn_mode: %v,idx: %v, %#v", txnMode, idx, ca)
			_, err := tk.Exec(ca.sql)
			if ca.err == "" {
				require.NoError(t, err, comment)
			} else {
				require.Error(t, err, comment)
				require.Equal(t, ca.err, err.Error(), comment)
			}
			txnCtx := tk.Session().GetSessionVars().TxnCtx
			require.Equal(t, len(ca.sps), len(txnCtx.Savepoints), comment)
			for i, sp := range ca.sps {
				require.Equal(t, sp, txnCtx.Savepoints[i].Name, comment)
			}
		}
	}
}

func TestTxnSavepoint1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, a int, unique index idx(id))")

	cases := []struct {
		sql    string
		result []string
		err    string
	}{
		// execute savepoint not in transaction
		{sql: "commit"},
		{sql: "savepoint s1"},
		{sql: "insert into t values (3, 3)"},
		{sql: "rollback to s1", err: "[executor:1305]SAVEPOINT s1 does not exist"},
		{sql: "delete from t"},

		{sql: "begin"},
		{sql: "release savepoint x", err: "[executor:1305]SAVEPOINT x does not exist"},
		{sql: "savepoint s1"},
		{sql: "savepoint s2"},
		{sql: "savepoint s3"},
		{sql: "savepoint s4"},
		{sql: "release savepoint s1"},
		{sql: "rollback to s1", err: "[executor:1305]SAVEPOINT s1 does not exist"},
		{sql: "rollback to s2", err: "[executor:1305]SAVEPOINT s2 does not exist"},
		{sql: "rollback to s3", err: "[executor:1305]SAVEPOINT s3 does not exist"},
		{sql: "rollback to savepoint s4", err: "[executor:1305]SAVEPOINT s4 does not exist"},
		{sql: "rollback"},

		{sql: "begin"},
		{sql: "insert into t values (1, 1), (2, 2)"},
		{sql: "savepoint s1"},
		{sql: "insert into t values (3, 3)"},
		{sql: "savepoint s2"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2", "3 3"}},
		{sql: "rollback to s1"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},
		{sql: "insert into t values (3, 4), (4, 4)"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2", "3 4", "4 4"}},
		{sql: "rollback to s1"},
		{sql: "insert into t values (3, 5)"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2", "3 5"}},
		{sql: "rollback to s2", err: "[executor:1305]SAVEPOINT s2 does not exist"},
		{sql: "rollback to s1"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},
		{sql: "commit"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},

		{sql: "begin"},
		{sql: "insert into t values (1, 1), (3, 3) on duplicate key update a=a+1"},
		{sql: "select * from t order by id", result: []string{"1 2", "2 2", "3 3"}},
		{sql: "savepoint s1"},
		{sql: "update t set a=a+1 where id in (1, 2)"},
		{sql: "rollback to s1"},
		{sql: "insert into t values (4, 4)"},
		{sql: "commit"},
		{sql: "select * from t order by id", result: []string{"1 2", "2 2", "3 3", "4 4"}},

		{sql: "delete from t"},
		{sql: "begin"},
		{sql: "savepoint s1"},
		{sql: "insert into t values (1, 1)"},
		{sql: "savepoint s2"},
		{sql: "insert into t values (2, 2)"},
		{sql: "savepoint s1"},
		{sql: "insert into t values (3, 3)"},
		{sql: "rollback to s1"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},
		{sql: "commit"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},

		// test for savepoint name case
		{sql: "delete from t"},
		{sql: "begin"},
		{sql: "savepoint s1"},
		{sql: "insert into t values (1, 1)"},
		{sql: "release savepoint S1"},
		{sql: "rollback to s1", err: "[executor:1305]SAVEPOINT s1 does not exist"},
		{sql: "rollback to S1", err: "[executor:1305]SAVEPOINT S1 does not exist"},
		{sql: "rollback"},

		{sql: "begin"},
		{sql: "savepoint s1"},
		{sql: "insert into t values (1, 1)"},
		{sql: "savepoint s2"},
		{sql: "insert into t values (2, 2)"},
		{sql: "savepoint S1"},
		{sql: "insert into t values (3, 3)"},
		{sql: "rollback to s1"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},
		{sql: "commit"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},
		{sql: "delete from t"},

		// Test for release savepoint
		{sql: "begin;"},
		{sql: "insert into t values (1, 1)"},
		{sql: "savepoint s1"},
		{sql: "insert into t values (2, 2)"},
		{sql: "savepoint s2"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},
		{sql: "release savepoint s1;"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},
		{sql: "rollback to s2", err: "[executor:1305]SAVEPOINT s2 does not exist"},
		{sql: "select * from t order by id", result: []string{"1 1", "2 2"}},
		{sql: "rollback"},
	}
	txnModes := []string{"optimistic", "pessimistic", ""}
	for _, txnMode := range txnModes {
		tk.MustExec(fmt.Sprintf("set session tidb_txn_mode='%v';", txnMode))
		for idx, ca := range cases {
			comment := fmt.Sprintf("idx: %v, %#v", idx, ca)
			if ca.result == nil {
				if ca.err == "" {
					tk.MustExec(ca.sql)
				} else {
					err := tk.ExecToErr(ca.sql)
					require.Error(t, err, comment)
					require.Equal(t, ca.err, err.Error(), comment)
				}
			} else {
				tk.MustQuery(ca.sql).Check(testkit.Rows(ca.result...))
			}
		}
	}
}

func TestRollbackToSavepointReleasePessimisticLock(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int key, a int)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("insert into t values (1,1)")
	tk1.MustExec("savepoint s1")
	tk1.MustExec("insert into t values (2,2)")
	tk1.MustExec("rollback to s1")

	tk2.MustExec("begin pessimistic")
	start := time.Now()
	// test for release lock after rollback to savepoint then commit.
	tk1.MustExec("commit")
	tk2.MustExec("insert into t values (2,2)")
	require.Less(t, time.Since(start).Seconds(), float64(2))

	tk2.MustExec("commit")
	tk1.MustExec("delete from t")
	tk1.MustExec("insert into t values (1, 1)")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t where a= 1 for update")
	tk1.MustExec("savepoint s1")
	tk1.MustExec("delete from t where a = 1")
	// After rollback to s1, should not release lock in the row which a = 1
	tk1.MustExec("rollback to s1")

	tk2.MustExec("begin pessimistic")
	start = time.Now()
	var wait time.Duration
	go func() {
		time.Sleep(time.Millisecond * 100)
		wait = time.Since(start)
		tk1.MustExec("rollback")
	}()
	// should wait until tk1 rollback and release the lock.
	tk2.MustExec("select * from t where a= 1 for update")
	require.Less(t, wait.Seconds(), time.Since(start).Seconds())
}

func TestSavepointInPessimisticAndOptimistic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int key, a int)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	// Test for rollback savepoint in pessimistic txn.
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("insert into t values (1,1)")
	tk1.MustExec("savepoint s1")
	tk1.MustExec("insert into t values (2,2)")
	tk1.MustExec("rollback to s1")

	tk2.MustExec("begin optimistic")
	tk2.MustExec("insert into t values (2,2)")
	tk1.MustExec("commit")
	tk1.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	tk2.MustQuery("select * from t").Check(testkit.Rows("2 2"))
	tk2.MustExec("commit")
	tk1.MustQuery("select * from t").Check(testkit.Rows("1 1", "2 2"))

	// Test for rollback savepoint in optimistic txn.
	tk1.MustExec("truncate table t")
	tk1.MustExec("begin optimistic")
	tk1.MustExec("insert into t values (1,1)")
	tk1.MustExec("savepoint s1")
	tk1.MustExec("insert into t values (2,2)")
	tk1.MustExec("rollback to s1")

	tk2.MustExec("begin pessimistic")
	tk2.MustExec("insert into t values (2,2)")
	tk1.MustExec("commit")
	tk1.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	tk2.MustQuery("select * from t").Check(testkit.Rows("2 2"))
	tk2.MustExec("commit")
	tk1.MustQuery("select * from t").Check(testkit.Rows("1 1", "2 2"))
}

func TestSavepointInBigTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int key, a int)")
	rowCount := 10000

	// Test for rollback batch insert
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("insert into t values (0, 0)")
	tk1.MustExec("savepoint s1")
	for i := 1; i < rowCount; i++ {
		insert := fmt.Sprintf("insert into t values (%v, %v)", i, i)
		tk1.MustExec(insert)
	}
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows(strconv.Itoa(rowCount)))
	tk1.MustExec("rollback to s1")
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows("1"))
	tk1.MustExec("commit")
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	// Test for rollback batch update
	tk1.MustExec("begin")
	for i := 1; i < rowCount; i++ {
		insert := fmt.Sprintf("insert into t values (%v, %v)", i, i)
		tk1.MustExec(insert)
	}
	tk1.MustExec("commit")
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("savepoint s1")
	for i := 1; i < rowCount; i++ {
		update := fmt.Sprintf("update t set a=a+1 where id = %v", i)
		tk1.MustExec(update)
	}
	tk1.MustQuery("select count(*) from t where id != a").Check(testkit.Rows(strconv.Itoa(rowCount - 1)))
	tk1.MustExec("rollback to s1")
	tk1.MustQuery("select count(*) from t where id != a").Check(testkit.Rows("0"))
	tk1.MustExec("commit")
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows(strconv.Itoa(rowCount)))

	// Test for rollback batch insert on duplicate update
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("savepoint s1")
	for i := 1; i < rowCount; i++ {
		insert := fmt.Sprintf("insert into t values (%v, %v) on duplicate key update a=a+1", i, i)
		tk1.MustExec(insert)
	}
	tk1.MustQuery("select count(*) from t where id != a").Check(testkit.Rows(strconv.Itoa(rowCount - 1)))
	tk1.MustExec("rollback to s1")
	tk1.MustQuery("select count(*) from t where id != a").Check(testkit.Rows("0"))
	tk1.MustExec("commit")
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows(strconv.Itoa(rowCount)))

	// Test for rollback batch delete.
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("insert into t values (-1, -1)")
	tk1.MustExec("savepoint s1")
	for i := 0; i < rowCount; i++ {
		update := fmt.Sprintf("delete from t where id = %v", i)
		tk1.MustExec(update)
	}
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows("1"))
	tk1.MustExec("rollback to s1")
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows(strconv.Itoa(rowCount + 1)))
	tk1.MustExec("rollback")
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows(strconv.Itoa(rowCount)))

	// Test for many savepoint in 1 txn.
	tk1.MustExec("truncate table t")
	tk1.MustExec("begin pessimistic")
	for i := 0; i < rowCount; i++ {
		insert := fmt.Sprintf("insert into t values (%v, %v)", i, i)
		tk1.MustExec(insert)
		tk1.MustExec(fmt.Sprintf("savepoint s%v", i))
	}
	tk1.MustQuery("select count(*) from t").Check(testkit.Rows(strconv.Itoa(rowCount)))
	tk1.MustExec("rollback to s1")
	tk1.MustExec("commit")
	tk1.MustQuery("select * from t order by id").Check(testkit.Rows("0 0", "1 1"))
}

func TestSavepointWithCacheTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t0 (id int primary key, v int)")

	txnModes := []string{"optimistic", "pessimistic", ""}
	for _, txnMode := range txnModes {
		tk.MustExec(fmt.Sprintf("set session tidb_txn_mode='%v';", txnMode))
		tk.MustExec("create table if not exists t (id int primary key auto_increment, u int unique, v int)")
		tk.MustExec("delete from t")
		tk.MustExec("delete from t0")
		tk.MustExec("ALTER TABLE t CACHE;")
		tk.MustExec("begin")
		tk.MustExec("insert into t0 values(1, 1)")
		tk.MustExec("savepoint sp0;")
		tk.MustExec("insert into t values(1, 11, 101)")
		txnCtx := tk.Session().GetSessionVars().TxnCtx
		require.Equal(t, 1, len(txnCtx.CachedTables))
		tk.MustExec("savepoint sp1;")
		tk.MustExec("insert into t values(2, 22, 202)")
		tk.MustExec("savepoint sp2;")
		tk.MustExec("insert into t values(3, 33, 303)")
		tk.MustExec("rollback to sp2;")
		require.Equal(t, 1, len(txnCtx.CachedTables))
		tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 11 101", "2 22 202"))
		tk.MustExec("rollback to sp1;")
		require.Equal(t, 1, len(txnCtx.CachedTables))
		tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 11 101"))
		tk.MustExec("rollback to sp0;")
		tk.MustQuery("select * from t order by id").Check(testkit.Rows())
		tk.MustQuery("select * from t0 order by id").Check(testkit.Rows("1 1"))
		require.Equal(t, 0, len(txnCtx.CachedTables))
		tk.MustExec("commit")
		tk.MustQuery("select * from t order by id").Check(testkit.Rows())
		tk.MustQuery("select * from t0 order by id").Check(testkit.Rows("1 1"))
	}
}

func TestSavepointWithBinlog(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	// mock for binlog enabled.
	tk.Session().GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&testkit.MockPumpClient{})
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, a int, unique index idx(id))")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1,1)")
	err := tk.ExecToErr("savepoint s1")
	require.Error(t, err)
	require.Equal(t, executor.ErrSavepointNotSupportedWithBinlog.Error(), err.Error())
	err = tk.ExecToErr("rollback to s1")
	require.Error(t, err)
	require.Equal(t, "[executor:1305]SAVEPOINT s1 does not exist", err.Error())
	err = tk.ExecToErr("release savepoint s1")
	require.Error(t, err)
	require.Equal(t, "[executor:1305]SAVEPOINT s1 does not exist", err.Error())
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

func TestColumnNotMatchError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&testkit.MockPumpClient{})
	tk.MustExec("set @@global.tidb_enable_metadata_lock=0")
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("create table t(id int primary key, a int)")
	tk.MustExec("insert into t values(1, 2)")

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onAddColumnStateWriteReorg", func() {
		tk.MustExec("begin;")
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tk2.MustExec("alter table t add column wait_notify int")
		wg.Done()
	}()
	wg.Wait()
	tk.MustExec("delete from t where id=1")
	tk.MustGetErrCode("commit", errno.ErrInfoSchemaChanged)

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onDropColumnStateWriteOnly", func() {
		tk.MustExec("begin;")
	})
	wg.Add(1)
	go func() {
		tk2.MustExec("alter table t drop column wait_notify")
		wg.Done()
	}()
	wg.Wait()
	tk.MustExec("delete from t where id=1")
	tk.MustGetErrCode("commit", errno.ErrInfoSchemaChanged)
}
