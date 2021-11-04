package executor_test

import (
	"fmt"
	"github.com/pingcap/tidb/testkit"
	"strings"
	"testing"
	"time"
)

func TestDebugBasic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	//tk.MustExec("use test")
	//tk.MustExec("drop table if exists cache_admin_test;")
	//tk.MustExec("create table cache_admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2))")
	//tk.MustExec("insert cache_admin_test (c1, c2) values (1, 1), (2, 2), (5, 5), (10, 10), (11, 11)")
	//tk.MustExec("alter table cache_admin_test cache")
	//tk.MustExec("admin check table cache_admin_test;")
	//tk.MustExec("admin check index cache_admin_test c1;")
	//tk.MustExec("admin check index cache_admin_test c2;")
	//tk.MustExec("drop table if exists cache_admin_test;")
	//
	//tk.MustExec(`drop table if exists check_index_test;`)
	//tk.MustExec(`create table check_index_test (a int, b varchar(10), index a_b (a, b), index b (b))`)
	//tk.MustExec(`insert check_index_test values (3, "ab"),(2, "cd"),(1, "ef"),(-1, "hi")`)
	//tk.MustExec("alter table  check_index_test cache")
	//result := tk.MustQuery("admin check index check_index_test a_b (2, 4);")
	//result.Check(testkit.Rows("1 ef 3", "2 cd 2"))
	//result = tk.MustQuery("admin check index check_index_test a_b (3, 5);")
	//result.Check(testkit.Rows("-1 hi 4", "1 ef 3"))
	//tk.MustExec("drop table if exists check_index_test;")
	//
	//tk.MustExec("drop table if exists cache_admin_table_with_index_test;")
	//tk.MustExec("drop table if exists cache_admin_table_without_index_test;")
	//tk.MustExec("create table cache_admin_table_with_index_test (id int, count int, PRIMARY KEY(id), KEY(count))")
	//tk.MustExec("create table cache_admin_table_without_index_test (id int, count int, PRIMARY KEY(id))")
	//tk.MustExec("alter table cache_admin_table_with_index_test cache")
	//tk.MustExec("alter table cache_admin_table_without_index_test cache")
	//tk.MustExec("admin checksum table cache_admin_table_with_index_test;")
	//tk.MustExec("admin checksum table cache_admin_table_without_index_test;")
	//tk.MustExec("drop table if exists cache_admin_table_with_index_test,cache_admin_table_without_index_test;")
	////tk.MustExec("create temporary table tmp(id int);")
	////tk.MustExec("insert into tmp values(1)
	////tk.MustExec("prepare stmt from 'insert into t select * from tmp';")
	////tk.MustExec("execute stmt")
	////tk.MustQuery("select *from t").Check(testkit.Rows("1"))
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
		tk.MustExec(query.sql)
	}

	// Test normal table when cache table exits.
	tk.MustExec("insert into cache_tmp5 values(1);")
	tk.MustExec("set @a=now(6);")
	time.Sleep(time.Microsecond)
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
