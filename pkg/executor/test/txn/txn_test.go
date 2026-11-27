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

package txn_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
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
	for i := range rowCount {
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
	for i := range rowCount {
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

func TestColumnNotMatchError(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("MDL is always enabled and read only in nextgen")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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

func TestSavepointWithForeignKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table parent (id int primary key)")
	tk.MustExec("create table child (id int primary key, parent_id int, foreign key (parent_id) references parent(id))")

	tk.MustExec("begin")
	// rollback insertion to child table
	tk.MustExec("savepoint sp1")
	tk.MustExec("insert into parent values (1)")
	tk.MustExec("savepoint sp2")
	tk.MustExec("insert into child values (1, 1)")
	tk.MustExec("rollback to sp2")
	tk.MustQuery("select * from child").Check(testkit.Rows())
	// rollback insertion to parent table
	tk.MustExec("rollback to sp1")
	require.Contains(t, tk.ExecToErr("insert into child values (1, 1)").Error(), "a foreign key constraint fails")
	tk.MustQuery("select * from parent").Check(testkit.Rows())
	tk.MustExec("rollback")

	// A known limitation: the lock of the parent table is not released after rollback to savepoint.
	tk.MustExec("insert into parent values (1)")

	tk.MustExec("begin")
	tk.MustExec("savepoint sp1")
	tk.MustExec("insert into child values (1, 1)")
	// Now, the row in parent table is locked
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	var wg sync.WaitGroup
	wg.Add(1)
	var updateTime, beforeCommitTime time.Time
	go func() {
		defer wg.Done()

		// This SQL will be blocked until the transaction is committed. It's a known limitation that the pessimistic lock
		// is not released after rollback to savepoint. If the lock can be released after rollback in the future, this test
		// can be modified.
		tk2.MustExec("update parent set id = 2 where id = 1")
		updateTime = time.Now()
	}()
	tk.MustExec("rollback to sp1")
	time.Sleep(500 * time.Millisecond)
	beforeCommitTime = time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Greater(t, updateTime, beforeCommitTime)
}

func TestInnodbLockWaitTimeout(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int auto_increment, k int,c varchar(255), unique index idx(id))")
	tk.MustExec("insert into t (k,c) values (1,'abcdefg');")
	for range 8 {
		tk.MustExec("insert into t (k,c) select k,c from t;")
	}
	tk.MustExec("update t set k= id, c = id")
	tk.MustExec("split table t by (0), (50), (100);")
	tk.MustExec("split table t index idx by (0), (50), (100);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/pessimisticLockReturnWriteConflict", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/pessimisticLockReturnWriteConflict"))
	}()
	tk.MustExec("set @@innodb_lock_wait_timeout=1")
	isolations := []string{"REPEATABLE READ", "READ COMMITTED"}
	for _, isolation := range isolations {
		tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL " + isolation)
		tk.MustExec("begin")
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		res, err := tk.ExecWithContext(ctx, "update t use index (idx) set k=k+1 where id >0;")
		cancel()
		if res != nil {
			require.NoError(t, res.Close())
		}
		require.Error(t, err)
		msg := fmt.Sprintf("cost: %v", time.Since(start))
		require.Equal(t, "lock wait timeout", err.Error(), msg)
		require.Less(t, time.Since(start), time.Second*2)
		tk.MustExec("commit")
	}
}

func TestNativeSharedLockExecution(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("native shared reads are Classic-only")
	}
	store := testkit.CreateMockStore(t)
	newSession := func(t *testing.T) *testkit.TestKit {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set tidb_enable_select_for_share=on")
		t.Cleanup(func() { tk.MustExec("rollback") })
		return tk
	}
	admin := newSession(t)
	admin.MustExec("create table t (id int primary key, u int unique, v int)")
	admin.MustExec("insert into t values (1,10,100),(2,20,200)")
	admin.MustExec("create table pt (id int primary key, v int) partition by hash(id) partitions 2")
	admin.MustExec("insert into pt values (1,100),(2,200)")
	checkLock := func(probe *testkit.TestKit, table, id string, locked bool) {
		probe.MustExec("begin pessimistic")
		sql := "select id from " + table + " where id=" + id + " for update nowait"
		if locked {
			probe.MustGetErrCode(sql, mysql.ErrLockAcquireFailAndNoWaitSet)
		} else {
			probe.MustQuery(sql).Check(testkit.Rows(id))
		}
		probe.MustExec("rollback")
	}

	t.Run("commit preserves row and index", func(t *testing.T) {
		tk := newSession(t)
		tk.MustExec("create table commit_shared like t")
		tk.MustExec("insert into commit_shared select * from t")
		for _, mixed := range []bool{false, true} {
			tk.MustExec("begin pessimistic")
			tk.MustQuery("select v from commit_shared where u=10 for share").Check(testkit.Rows("100"))
			if mixed {
				tk.MustExec("update commit_shared set v=v+1 where id=2")
			}
			tk.MustExec("commit")
			fresh := newSession(t)
			fresh.MustQuery("select v from commit_shared where id=1").Check(testkit.Rows("100"))
			fresh.MustQuery("select id,v from commit_shared force index(u) where u=10").Check(testkit.Rows("1 100"))
			fresh.MustExec("admin check table commit_shared")
			if mixed {
				fresh.MustQuery("select v from commit_shared where id=2").Check(testkit.Rows("201"))
			}
		}
	})
	for _, column := range []string{"id", "u"} {
		t.Run("RC miss "+column, func(t *testing.T) {
			reader, writer := newSession(t), newSession(t)
			writer.MustExec("delete from t where id=3")
			reader.MustExec("set transaction isolation level read committed")
			reader.MustExec("begin pessimistic")
			reader.MustQuery("select id from t where " + column + "=3 for share").Check(testkit.Rows())
			writer.MustExec("insert into t values (3,3,300)")
			reader.MustQuery("select id from t where " + column + " in (3,999) for share").Check(testkit.Rows("3"))
			checkLock(writer, "t", "3", true)
			reader.MustExec("rollback")
			writer.MustExec("delete from t where id=3")
		})
	}
	for _, tc := range []struct {
		name, query, table string
	}{
		{"partition OF", "select id from pt where v=100 for share of pt", "pt"},
		{"self join OF", "select a.id from pt a join pt b on b.v=a.v+100 for share of a", "pt"},
		{"EXISTS", "select exists(select 1 from t where v=100 for share)", "t"},
		{"LIMIT", "select id from t where v=100 limit 1 for share", "t"},
		{"outer LIMIT", "select id from (select id from t where v=100 for share) x limit 1", "t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, probe := newSession(t), newSession(t)
			reader.MustExec("begin pessimistic")
			reader.MustQuery(tc.query).Check(testkit.Rows("1"))
			checkLock(probe, tc.table, "1", true)
			checkLock(probe, tc.table, "2", false)
		})
	}
	t.Run("scalar prepare", func(t *testing.T) {
		reader, probe := newSession(t), newSession(t)
		reader.MustExec("begin pessimistic")
		reader.MustExec("prepare s from 'select (select sum(v) from pt for share)'")
		checkLock(probe, "pt", "1", false)
		reader.MustQuery("execute s").Check(testkit.Rows("300"))
		checkLock(probe, "pt", "1", true)
	})
	t.Run("mixed union targets and modes", func(t *testing.T) {
		reader, probe := newSession(t), newSession(t)
		reader.MustExec("prepare s from '(select id from pt where v=100 for share of pt) union all (select id from t where v=100 for update)'")
		reader.MustExec("set tidb_enable_noop_functions=on")
		for _, tc := range []struct {
			native, promoted string
			shareLocked      bool
		}{
			{"on", "off", true}, {"off", "on", true}, {"off", "off", false},
		} {
			reader.MustExec("set tidb_enable_select_for_share=" + tc.native + ", tidb_enable_shared_lock_promotion=" + tc.promoted)
			reader.MustExec("begin pessimistic")
			reader.MustQuery("execute s").Check(testkit.Rows("1", "1"))
			stats := reader.Session().GetSessionVars().StmtCtx.GetExecDetails()
			require.NotNil(t, stats.LockKeysDetail)
			require.Equal(t, tc.native == "on", stats.SharedLockKeysDetail != nil)
			checkLock(probe, "t", "1", true)
			checkLock(probe, "pt", "1", tc.shareLocked)
			reader.MustExec("rollback")
		}
	})
	t.Run("optimistic empty lookups", func(t *testing.T) {
		tk := newSession(t)
		tk.MustExec("begin optimistic")
		for _, query := range []string{"select * from t where id=999999999999999 for share",
			"select * from pt partition(p0) where id=1 for share",
			"select * from pt partition(p0) where id in(1,3) for share"} {
			tk.MustGetErrCode(query, mysql.ErrNotSupportedYet)
		}
	})
	t.Run("empty locking scan disables optimistic retry", func(t *testing.T) {
		tk := newSession(t)
		tk.MustExec("begin optimistic")
		tk.MustQuery("select id from t where v=999 for update").Check(testkit.Rows())
		require.EqualValues(t, 1, tk.Session().GetSessionVars().TxnCtx.ForUpdate)
	})
	t.Run("shared lock stats", func(t *testing.T) {
		tk := newSession(t)
		tk.MustExec("begin pessimistic")
		tk.MustQuery("select id from t where v=100 for share").Check(testkit.Rows("1"))
		stats := tk.Session().GetSessionVars().StmtCtx.GetExecDetails()
		require.NotNil(t, stats.SharedLockKeysDetail)
		require.Positive(t, stats.SharedLockKeysDetail.LockKeys)
		require.Nil(t, stats.LockKeysDetail)
	})
	t.Run("SELECT privilege", func(t *testing.T) {
		admin.MustExec("create user reader@'%'")
		admin.MustExec("grant select on test.t to reader@'%'")
		tk := newSession(t)
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "reader", Hostname: "localhost"}, nil, nil, nil))
		tk.MustExec("begin pessimistic")
		for _, pred := range []string{"id=1", "id in(1,999)", "v=100"} {
			sql := "select id from t where " + pred
			tk.MustQuery(sql + " for share").Check(testkit.Rows("1"))
			tk.MustExec("prepare s from '" + sql + " for share'")
			tk.MustQuery("execute s").Check(testkit.Rows("1"))
			tk.MustGetErrCode(sql+" for update", mysql.ErrTableaccessDenied)
		}
	})
	t.Run("dropped unique index", func(t *testing.T) {
		reader, writer := newSession(t), newSession(t)
		writer.MustExec("set global tidb_enable_metadata_lock=off")
		defer writer.MustExec("set global tidb_enable_metadata_lock=on")
		writer.MustExec("create table dropped_idx(id int primary key,u int unique)")
		writer.MustExec("insert into dropped_idx values(1,10)")
		reader.MustExec("set session transaction isolation level read committed")
		reader.MustExec("begin pessimistic")
		reader.MustQuery("select id from dropped_idx where id=1").Check(testkit.Rows("1"))
		writer.MustExec("alter table dropped_idx drop index u")
		writer.MustExec("insert into dropped_idx values(2,20)")
		reader.MustQuery("select id from dropped_idx where u in(20,30) for share").Check(testkit.Rows("2"))
		reader.MustExec("rollback")
	})
	t.Run("cached lock timeout", func(t *testing.T) {
		tk := newSession(t)
		tk.MustExec("set tidb_enable_prepared_plan_cache=on")
		var observed atomic.Int64
		ctx := interceptor.WithRPCInterceptor(context.Background(), interceptor.NewRPCInterceptor("lock wait timeout", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
			return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
				if req.Type == tikvrpc.CmdPessimisticLock {
					observed.Store(req.PessimisticLock().WaitTimeout)
				}
				return next(target, req)
			}
		}))
		for _, pred := range []string{"id=?", "id in(?,999)"} {
			for _, clause := range []string{"for share", "for share nowait", "for update wait 7"} {
				tk.MustExec("prepare s from 'select /*+ use_plan_cache() */ id from t where " + pred + " " + clause + "'")
				for i, timeout := range []int64{60, 30} {
					tk.MustExec(fmt.Sprintf("set innodb_lock_wait_timeout=%d", timeout))
					tk.MustExec("set @id=1")
					tk.MustExec("begin pessimistic")
					observed.Store(0)
					tk.MustQueryWithContext(ctx, "execute s using @id").Check(testkit.Rows("1"))
					require.Equal(t, i > 0, tk.Session().GetSessionVars().FoundInPlanCache)
					wait := observed.Load()
					switch clause {
					case "for share nowait":
						require.EqualValues(t, -1, wait)
					case "for update wait 7":
						require.Positive(t, wait)
						require.LessOrEqual(t, wait, int64(7000))
					default:
						require.Greater(t, wait, (timeout-10)*1000)
						require.LessOrEqual(t, wait, timeout*1000)
					}
					tk.MustExec("rollback")
				}
			}
		}
	})
}
