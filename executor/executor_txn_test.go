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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/testkit"
)

func TestInvalidReadTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, time.Second)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
