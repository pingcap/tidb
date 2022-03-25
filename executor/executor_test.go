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
	"archive/zip"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestPessimisticSelectForUpdate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, a int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("begin PESSIMISTIC")
	tk.MustQuery("select a from t where id=1 for update").Check(testkit.Rows("1"))
	tk.MustExec("update t set a=a+1 where id=1")
	tk.MustExec("commit")
	tk.MustQuery("select a from t where id=1").Check(testkit.Rows("2"))
}

func TestBind(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists testbind")

	tk.MustExec("create table testbind(i int, s varchar(20))")
	tk.MustExec("create index index_t on testbind(i,s)")
	tk.MustExec("create global binding for select * from testbind using select * from testbind use index for join(index_t)")
	require.Len(t, tk.MustQuery("show global bindings").Rows(), 1)

	tk.MustExec("create session binding for select * from testbind using select * from testbind use index for join(index_t)")
	require.Len(t, tk.MustQuery("show session bindings").Rows(), 1)
	tk.MustExec("drop session binding for select * from testbind")
}

func TestChangePumpAndDrainer(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	// change pump or drainer's state need connect to etcd
	// so will meet error "URL scheme must be http, https, unix, or unixs: /tmp/tidb"
	tk.MustMatchErrMsg("change pump to node_state ='paused' for node_id 'pump1'", "URL scheme must be http, https, unix, or unixs.*")
	tk.MustMatchErrMsg("change drainer to node_state ='paused' for node_id 'drainer1'", "URL scheme must be http, https, unix, or unixs.*")
}

func TestLoadStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.Error(t, tk.ExecToErr("load stats"))
	require.Error(t, tk.ExecToErr("load stats ./xxx.json"))
}

func TestPlanReplayer(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	tk.MustExec("plan replayer dump explain select * from t where a=10")
}

func TestShow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test_show;")
	tk.MustExec("use test_show")

	tk.MustQuery("show engines")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	require.Len(t, tk.MustQuery("show index in t").Rows(), 1)
	require.Len(t, tk.MustQuery("show index from t").Rows(), 1)
	require.Len(t, tk.MustQuery("show master status").Rows(), 1)

	tk.MustQuery("show create database test_show").Check(testkit.Rows("test_show CREATE DATABASE `test_show` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk.MustQuery("show privileges").Check(testkit.Rows("Alter Tables To alter the table",
		"Alter routine Functions,Procedures To alter or drop stored functions/procedures",
		"Create Databases,Tables,Indexes To create new databases and tables",
		"Create routine Databases To use CREATE FUNCTION/PROCEDURE",
		"Create temporary tables Databases To use CREATE TEMPORARY TABLE",
		"Create view Tables To create new views",
		"Create user Server Admin To create new users",
		"Delete Tables To delete existing rows",
		"Drop Databases,Tables To drop databases, tables, and views",
		"Event Server Admin To create, alter, drop and execute events",
		"Execute Functions,Procedures To execute stored routines",
		"File File access on server To read and write files on the server",
		"Grant option Databases,Tables,Functions,Procedures To give to other users those privileges you possess",
		"Index Tables To create or drop indexes",
		"Insert Tables To insert data into tables",
		"Lock tables Databases To use LOCK TABLES (together with SELECT privilege)",
		"Process Server Admin To view the plain text of currently executing queries",
		"Proxy Server Admin To make proxy user possible",
		"References Databases,Tables To have references on tables",
		"Reload Server Admin To reload or refresh tables, logs and privileges",
		"Replication client Server Admin To ask where the slave or master servers are",
		"Replication slave Server Admin To read binary log events from the master",
		"Select Tables To retrieve rows from table",
		"Show databases Server Admin To see all databases with SHOW DATABASES",
		"Show view Tables To see views with SHOW CREATE VIEW",
		"Shutdown Server Admin To shut down the server",
		"Super Server Admin To use KILL thread, SET GLOBAL, CHANGE MASTER, etc.",
		"Trigger Tables To use triggers",
		"Create tablespace Server Admin To create/alter/drop tablespaces",
		"Update Tables To update existing rows",
		"Usage Server Admin No privileges - allow connect only",
		"BACKUP_ADMIN Server Admin ",
		"RESTORE_ADMIN Server Admin ",
		"SYSTEM_USER Server Admin ",
		"SYSTEM_VARIABLES_ADMIN Server Admin ",
		"ROLE_ADMIN Server Admin ",
		"CONNECTION_ADMIN Server Admin ",
		"PLACEMENT_ADMIN Server Admin ",
		"DASHBOARD_CLIENT Server Admin ",
		"RESTRICTED_TABLES_ADMIN Server Admin ",
		"RESTRICTED_STATUS_ADMIN Server Admin ",
		"RESTRICTED_VARIABLES_ADMIN Server Admin ",
		"RESTRICTED_USER_ADMIN Server Admin ",
		"RESTRICTED_CONNECTION_ADMIN Server Admin ",
		"RESTRICTED_REPLICA_WRITER_ADMIN Server Admin ",
	))
	require.Len(t, tk.MustQuery("show table status").Rows(), 1)
}

func TestSelectWithoutFrom(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select 1 + 2*3;").Check(testkit.Rows("7"))
	tk.MustQuery(`select _utf8"string";`).Check(testkit.Rows("string"))
	tk.MustQuery("select 1 order by 1;").Check(testkit.Rows("1"))
}

// TestSelectBackslashN Issue 3685.
func TestSelectBackslashN(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	sql := `select \N;`
	tk.MustQuery(sql).Check(testkit.Rows("<nil>"))
	rs, err := tk.Exec(sql)
	require.NoError(t, err)
	fields := rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "NULL", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select "\N";`
	tk.MustQuery(sql).Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `N`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	tk.MustExec("use test;")
	tk.MustExec("create table test (`\\N` int);")
	tk.MustExec("insert into test values (1);")
	tk.CheckExecResult(1, 0)
	sql = "select * from test;"
	tk.MustQuery(sql).Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `\N`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select \N from test;`
	tk.MustQuery(sql).Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, `NULL`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select (\N) from test;`
	tk.MustQuery(sql).Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `NULL`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = "select `\\N` from test;"
	tk.MustQuery(sql).Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `\N`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = "select (`\\N`) from test;"
	tk.MustQuery(sql).Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `\N`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select '\N' from test;`
	tk.MustQuery(sql).Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `N`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select ('\N') from test;`
	tk.MustQuery(sql).Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `N`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())
}

// TestSelectNull Issue #4053.
func TestSelectNull(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	sql := `select nUll;`
	tk.MustQuery(sql).Check(testkit.Rows("<nil>"))
	rs, err := tk.Exec(sql)
	require.NoError(t, err)
	fields := rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `NULL`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select (null);`
	tk.MustQuery(sql).Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `NULL`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select null+NULL;`
	tk.MustQuery(sql).Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, `null+NULL`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())
}

// TestSelectStringLiteral Issue #3686.
func TestSelectStringLiteral(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	sql := `select 'abc';`
	tk.MustQuery(sql).Check(testkit.Rows("abc"))
	rs, err := tk.Exec(sql)
	require.NoError(t, err)
	fields := rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `abc`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select (('abc'));`
	tk.MustQuery(sql).Check(testkit.Rows("abc"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `abc`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select 'abc'+'def';`
	tk.MustQuery(sql).Check(testkit.Rows("0"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, `'abc'+'def'`, fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	// Below checks whether leading invalid chars are trimmed.
	sql = "select '\n';"
	tk.MustQuery(sql).Check(testkit.Rows("\n"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = "select '\t   col';" // Lowercased letter is a valid char.
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "col", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = "select '\t   Col';" // Uppercased letter is a valid char.
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "Col", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = "select '\n\t   中文 col';" // Chinese char is a valid char.
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "中文 col", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = "select ' \r\n  .col';" // Punctuation is a valid char.
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, ".col", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = "select '   😆col';" // Emoji is a valid char.
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "😆col", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	// Below checks whether trailing invalid chars are preserved.
	sql = `select 'abc   ';`
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "abc   ", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select '  abc   123   ';`
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "abc   123   ", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	// Issue #4239.
	sql = `select 'a' ' ' 'string';`
	tk.MustQuery(sql).Check(testkit.Rows("a string"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "a", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select 'a' " " "string";`
	tk.MustQuery(sql).Check(testkit.Rows("a string"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "a", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select 'string' 'string';`
	tk.MustQuery(sql).Check(testkit.Rows("stringstring"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "string", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select "ss" "a";`
	tk.MustQuery(sql).Check(testkit.Rows("ssa"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "ss", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select "ss" "a" "b";`
	tk.MustQuery(sql).Check(testkit.Rows("ssab"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "ss", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select "ss" "a" ' ' "b";`
	tk.MustQuery(sql).Check(testkit.Rows("ssa b"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "ss", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())

	sql = `select "ss" "a" ' ' "b" ' ' "d";`
	tk.MustQuery(sql).Check(testkit.Rows("ssa b d"))
	rs, err = tk.Exec(sql)
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "ss", fields[0].Column.Name.O)
	require.NoError(t, rs.Close())
}

func TestSelectLimit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table select_limit(id int not null default 1, name varchar(255), PRIMARY KEY(id));")
	// insert data
	tk.MustExec("insert INTO select_limit VALUES (1, \"hello\");")
	tk.CheckExecResult(1, 0)
	tk.MustExec("insert into select_limit values (2, \"hello\");")
	tk.CheckExecResult(1, 0)

	tk.MustExec("insert INTO select_limit VALUES (3, \"hello\");")
	tk.CheckExecResult(1, 0)
	tk.MustExec("insert INTO select_limit VALUES (4, \"hello\");")
	tk.CheckExecResult(1, 0)

	tk.MustQuery("select * from select_limit limit 1;").Check(testkit.Rows("1 hello"))

	tk.MustQuery("select id from (select * from select_limit limit 1) k where id != 1;").Check(testkit.Rows())

	tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 0;").Check(testkit.Rows("1 hello", "2 hello", "3 hello", "4 hello"))

	tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 1;").Check(testkit.Rows("2 hello", "3 hello", "4 hello"))

	tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 3;").Check(testkit.Rows("4 hello"))

	err := tk.ExecToErr("select * from select_limit limit 18446744073709551616 offset 3;")
	require.Error(t, err)
}

func TestSelectOrderBy(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table select_order_test(id int not null default 1, name varchar(255), PRIMARY KEY(id));")

	// insert data
	tk.MustExec("insert INTO select_order_test VALUES (1, \"hello\");")
	tk.CheckExecResult(1, 0)
	tk.MustExec("insert into select_order_test values (2, \"hello\");")
	tk.CheckExecResult(1, 0)

	// Test star field
	tk.MustQuery("select * from select_order_test where id = 1 order by id limit 1 offset 0;").Check(testkit.Rows("1 hello"))
	tk.MustQuery("select id from select_order_test order by id desc limit 1 ").Check(testkit.Rows("2"))
	tk.MustQuery("select id from select_order_test order by id + 1 desc limit 1 ").Check(testkit.Rows("2"))
	// Test limit
	tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 0;").Check(testkit.Rows("1 hello"))
	// Test limit
	tk.MustQuery("select id as c1, name from select_order_test order by 2, id limit 1 offset 0;").Check(testkit.Rows("1 hello"))
	// Test limit overflow
	tk.MustQuery("select * from select_order_test order by name, id limit 100 offset 0;").Check(testkit.Rows("1 hello", "2 hello"))
	// Test offset overflow
	tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 100;").Check(testkit.Rows())
	// Test limit exceeds int range.
	tk.MustQuery("select id from select_order_test order by name, id limit 18446744073709551615;").Check(testkit.Rows("1", "2"))
	// Test multiple field
	tk.MustQuery("select id, name from select_order_test where id = 1 group by id, name limit 1 offset 0;").Check(testkit.Rows("1 hello"))

	// Test limit + order by
	for i := 3; i <= 10; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"zz\");", i))
	}
	tk.MustExec("insert INTO select_order_test VALUES (10086, \"hi\");")
	for i := 11; i <= 20; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"hh\");", i))
	}
	for i := 21; i <= 30; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"zz\");", i))
	}
	tk.MustExec("insert INTO select_order_test VALUES (1501, \"aa\");")
	tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 3;").Check(testkit.Rows("11 hh"))
	tk.MustExec("drop table select_order_test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (1, 3)")
	tk.MustQuery("select 1-d as d from t order by d;").Check(testkit.Rows("-2", "-1", "0"))
	tk.MustQuery("select 1-d as d from t order by d + 1;").Check(testkit.Rows("0", "-1", "-2"))
	tk.MustQuery("select t.d from t order by d;").Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert t values (1, 2, 3)")
	tk.MustQuery("select b from (select a,b from t order by a,c) t").Check(testkit.Rows("2"))
	tk.MustQuery("select b from (select a,b from t order by a,c limit 1) t").Check(testkit.Rows("2"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("insert into t values(1, 1), (2, 2)")
	tk.MustQuery("select * from t where 1 order by b").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select * from t where a between 1 and 2 order by a desc").Check(testkit.Rows("2 2", "1 1"))

	// Test double read and topN is pushed down to first read plannercore.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values(1, 3, 1)")
	tk.MustExec("insert into t values(2, 2, 2)")
	tk.MustExec("insert into t values(3, 1, 3)")
	tk.MustQuery("select * from t use index(idx) order by a desc limit 1").Check(testkit.Rows("3 1 3"))

	// Test double read which needs to keep order.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key b (b))")
	tk.Session().GetSessionVars().IndexLookupSize = 3
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d, %d)", i, 10-i))
	}
	tk.MustQuery("select a from t use index(b) order by b").Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"))
}

func TestOrderBy(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 int, c3 varchar(20))")
	tk.MustExec("insert into t values (1, 2, 'abc'), (2, 1, 'bcd')")

	// Fix issue https://github.com/pingcap/tidb/issues/337
	tk.MustQuery("select c1 as a, c1 as b from t order by c1").Check(testkit.Rows("1 1", "2 2"))

	tk.MustQuery("select c1 as a, t.c1 as a from t order by a desc").Check(testkit.Rows("2 2", "1 1"))
	tk.MustQuery("select c1 as c2 from t order by c2").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select sum(c1) from t order by sum(c1)").Check(testkit.Rows("3"))
	tk.MustQuery("select c1 as c2 from t order by c2 + 1").Check(testkit.Rows("2", "1"))

	// Order by position.
	tk.MustQuery("select * from t order by 1").Check(testkit.Rows("1 2 abc", "2 1 bcd"))
	tk.MustQuery("select * from t order by 2").Check(testkit.Rows("2 1 bcd", "1 2 abc"))

	// Order by binary.
	tk.MustQuery("select c1, c3 from t order by binary c1 desc").Check(testkit.Rows("2 bcd", "1 abc"))
	tk.MustQuery("select c1, c2 from t order by binary c3").Check(testkit.Rows("1 2", "2 1"))
}

func TestSelectErrorRow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.Error(t, tk.ExecToErr("select row(1, 1) from test"))
	require.Error(t, tk.ExecToErr("select * from test group by row(1, 1);"))
	require.Error(t, tk.ExecToErr("select * from test order by row(1, 1);"))
	require.Error(t, tk.ExecToErr("select * from test having row(1, 1);"))
	require.Error(t, tk.ExecToErr("select (select 1, 1) from test;"))
	require.Error(t, tk.ExecToErr("select * from test group by (select 1, 1);"))
	require.Error(t, tk.ExecToErr("select * from test order by (select 1, 1);"))
	require.Error(t, tk.ExecToErr("select * from test having (select 1, 1);"))
}

func TestNeighbouringProj(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 value(1, 1), (2, 2)")
	tk.MustExec("insert into t2 value(1, 1), (2, 2)")
	tk.MustQuery("select sum(c) from (select t1.a as a, t1.a as c, length(t1.b) from t1  union select a, b, b from t2) t;").Check(testkit.Rows("5"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint, b bigint, c bigint);")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3);")
	tk.MustQuery("select cast(count(a) as signed), a as another, a from t group by a order by cast(count(a) as signed), a limit 10;").Check(testkit.Rows("1 1 1", "1 2 2", "1 3 3"))
}

func TestIn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (c1 int primary key, c2 int, key c (c2));`)
	for i := 0; i <= 200; i++ {
		tk.MustExec(fmt.Sprintf("insert t values(%d, %d)", i, i))
	}
	queryStr := `select c2 from t where c1 in ('7', '10', '112', '111', '98', '106', '100', '9', '18', '17') order by c2`
	tk.MustQuery(queryStr).Check(testkit.Rows("7", "9", "10", "17", "18", "98", "100", "106", "111", "112"))

	queryStr = `select c2 from t where c1 in ('7a')`
	tk.MustQuery(queryStr).Check(testkit.Rows("7"))
}

func TestTablePKisHandleScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int PRIMARY KEY AUTO_INCREMENT)")
	tk.MustExec("insert t values (),()")
	tk.MustExec("insert t values (-100),(0)")

	tests := []struct {
		sql    string
		result [][]interface{}
	}{
		{
			"select * from t",
			testkit.Rows("-100", "1", "2", "3"),
		},
		{
			"select * from t where a = 1",
			testkit.Rows("1"),
		},
		{
			"select * from t where a != 1",
			testkit.Rows("-100", "2", "3"),
		},
		{
			"select * from t where a >= '1.1'",
			testkit.Rows("2", "3"),
		},
		{
			"select * from t where a < '1.1'",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a > '-100.1' and a < 2",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a is null",
			testkit.Rows(),
		}, {
			"select * from t where a is true",
			testkit.Rows("-100", "1", "2", "3"),
		}, {
			"select * from t where a is false",
			testkit.Rows(),
		},
		{
			"select * from t where a in (1, 2)",
			testkit.Rows("1", "2"),
		},
		{
			"select * from t where a between 1 and 2",
			testkit.Rows("1", "2"),
		},
	}

	for _, tt := range tests {
		tk.MustQuery(tt.sql).Check(tt.result)
	}
}

func TestIndexReverseOrder(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int, index idx (b))")
	tk.MustExec("insert t (b) values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)")
	tk.MustQuery("select b from t order by b desc").Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"))
	tk.MustQuery("select b from t where b <3 or (b >=6 and b < 8) order by b desc").Check(testkit.Rows("7", "6", "2", "1", "0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx (b, a))")
	tk.MustExec("insert t values (0, 2), (1, 2), (2, 2), (0, 1), (1, 1), (2, 1), (0, 0), (1, 0), (2, 0)")
	tk.MustQuery("select b, a from t order by b, a desc").Check(testkit.Rows("0 2", "0 1", "0 0", "1 2", "1 1", "1 0", "2 2", "2 1", "2 0"))
}

func TestTableReverseOrder(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int)")
	tk.MustExec("insert t (b) values (1), (2), (3), (4), (5), (6), (7), (8), (9)")
	tk.MustQuery("select b from t order by a desc").Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1"))
	tk.MustQuery("select a from t where a <3 or (a >=6 and a < 8) order by a desc").Check(testkit.Rows("7", "6", "2", "1"))
}

func TestDefaultNull(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int default 1, c int)")
	tk.MustExec("insert t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("update t set b = NULL where a = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustExec("update t set c = 1")
	tk.MustQuery("select * from t ").Check(testkit.Rows("1 <nil> 1"))
	tk.MustExec("delete from t where a = 1")
	tk.MustExec("insert t (a) values (1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 <nil>"))
}

func TestUnsignedPKColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unsigned primary key, b int, c int, key idx_ba (b, c, a));")
	tk.MustExec("insert t values (1, 1, 1)")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1"))
	tk.MustExec("update t set c=2 where a=1;")
	tk.MustQuery("select * from t where b=1;").Check(testkit.Rows("1 1 2"))
}

func TestJSON(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_json")
	tk.MustExec("create table test_json (id int, a json)")
	tk.MustExec(`insert into test_json (id, a) values (1, '{"a":[1,"2",{"aa":"bb"},4],"b":true}')`)
	tk.MustExec(`insert into test_json (id, a) values (2, "null")`)
	tk.MustExec(`insert into test_json (id, a) values (3, null)`)
	tk.MustExec(`insert into test_json (id, a) values (4, 'true')`)
	tk.MustExec(`insert into test_json (id, a) values (5, '3')`)
	tk.MustExec(`insert into test_json (id, a) values (5, '4.0')`)
	tk.MustExec(`insert into test_json (id, a) values (6, '"string"')`)

	tk.MustQuery(`select tj.a from test_json tj order by tj.id`).Check(testkit.Rows(`{"a": [1, "2", {"aa": "bb"}, 4], "b": true}`, "null", "<nil>", "true", "3", "4", `"string"`))

	// Check json_type function
	tk.MustQuery(`select json_type(a) from test_json tj order by tj.id`).Check(testkit.Rows("OBJECT", "NULL", "<nil>", "BOOLEAN", "INTEGER", "DOUBLE", "STRING"))

	// Check json compare with primitives.
	tk.MustQuery(`select a from test_json tj where a = 3`).Check(testkit.Rows("3"))
	tk.MustQuery(`select a from test_json tj where a = 4.0`).Check(testkit.Rows("4"))
	tk.MustQuery(`select a from test_json tj where a = true`).Check(testkit.Rows("true"))
	tk.MustQuery(`select a from test_json tj where a = "string"`).Check(testkit.Rows(`"string"`))

	// Check cast(true/false as JSON).
	tk.MustQuery(`select cast(true as JSON)`).Check(testkit.Rows(`true`))
	tk.MustQuery(`select cast(false as JSON)`).Check(testkit.Rows(`false`))

	// Check two json grammar sugar.
	tk.MustQuery(`select a->>'$.a[2].aa' as x, a->'$.b' as y from test_json having x is not null order by id`).Check(testkit.Rows(`bb true`))
	tk.MustQuery(`select a->'$.a[2].aa' as x, a->>'$.b' as y from test_json having x is not null order by id`).Check(testkit.Rows(`"bb" true`))

	// Check some DDL limits for TEXT/BLOB/JSON column.
	tk.MustGetErrCode(`create table test_bad_json(a json default '{}')`, mysql.ErrBlobCantHaveDefault)
	tk.MustGetErrCode(`create table test_bad_json(a blob default 'hello')`, mysql.ErrBlobCantHaveDefault)
	tk.MustGetErrCode(`create table test_bad_json(a text default 'world')`, mysql.ErrBlobCantHaveDefault)

	// check json fields cannot be used as key.
	tk.MustGetErrCode(`create table test_bad_json(id int, a json, key (a))`, mysql.ErrJSONUsedAsKey)

	// check CAST AS JSON.
	tk.MustQuery(`select CAST('3' AS JSON), CAST('{}' AS JSON), CAST(null AS JSON)`).Check(testkit.Rows(`3 {} <nil>`))

	tk.MustQuery("select a, count(1) from test_json group by a order by a").Check(testkit.Rows(
		"<nil> 1",
		"null 1",
		"3 1",
		"4 1",
		`"string" 1`,
		"{\"a\": [1, \"2\", {\"aa\": \"bb\"}, 4], \"b\": true} 1",
		"true 1"))

	// Check cast json to decimal.
	// NOTE: this test case contains a bug, it should be uncommented after the bug is fixed.
	// TODO: Fix bug https://github.com/pingcap/tidb/issues/12178
	// tk.MustExec("drop table if exists test_json")
	// tk.MustExec("create table test_json ( a decimal(60,2) as (JSON_EXTRACT(b,'$.c')), b json );")
	// tk.MustExec(`insert into test_json (b) values
	//	('{"c": "1267.1"}'),
	//	('{"c": "1267.01"}'),
	//	('{"c": "1267.1234"}'),
	//	('{"c": "1267.3456"}'),
	//	('{"c": "1234567890123456789012345678901234567890123456789012345"}'),
	//	('{"c": "1234567890123456789012345678901234567890123456789012345.12345"}');`)
	//
	// tk.MustQuery("select a from test_json;").Check(testkit.Rows("1267.10", "1267.01", "1267.12",
	//	"1267.35", "1234567890123456789012345678901234567890123456789012345.00",
	//	"1234567890123456789012345678901234567890123456789012345.12"))
}

func TestMultiUpdate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_mu (a int primary key, b int, c int)`)
	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)`)

	// Test INSERT ... ON DUPLICATE UPDATE set_lists.
	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE b = 3, c = b`)
	tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`).Check(testkit.Rows(`1 3 3`, `4 5 6`, `7 8 9`))

	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE c = 2, b = c+5`)
	tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`).Check(testkit.Rows(`1 7 2`, `4 5 6`, `7 8 9`))

	// Test UPDATE ... set_lists.
	tk.MustExec(`UPDATE test_mu SET b = 0, c = b WHERE a = 4`)
	tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`).Check(testkit.Rows(`1 7 2`, `4 0 5`, `7 8 9`))

	tk.MustExec(`UPDATE test_mu SET c = 8, b = c WHERE a = 4`)
	tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`).Check(testkit.Rows(`1 7 2`, `4 5 8`, `7 8 9`))

	tk.MustExec(`UPDATE test_mu SET c = b, b = c WHERE a = 7`)
	tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`).Check(testkit.Rows(`1 7 2`, `4 5 8`, `7 9 8`))
}

func TestGeneratedColumnWrite(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrMsg(`CREATE TABLE test_gc_write (a int primary key auto_increment, b int, c int as (a+8) virtual)`, dbterror.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs("c").Error())
	tk.MustExec(`CREATE TABLE test_gc_write (a int primary key auto_increment, b int, c int as (b+8) virtual)`)
	tk.MustExec(`CREATE TABLE test_gc_write_1 (a int primary key, b int, c int)`)

	tests := []struct {
		stmt string
		err  int
	}{
		// Can't modify generated column by values.
		{`insert into test_gc_write (a, b, c) values (1, 1, 1)`, mysql.ErrBadGeneratedColumn},
		{`insert into test_gc_write values (1, 1, 1)`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by select clause.
		{`insert into test_gc_write select 1, 1, 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by on duplicate clause.
		{`insert into test_gc_write (a, b) values (1, 1) on duplicate key update c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by set.
		{`insert into test_gc_write set a = 1, b = 1, c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by update clause.
		{`update test_gc_write set c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by multi-table update clause.
		{`update test_gc_write, test_gc_write_1 set test_gc_write.c = 1`, mysql.ErrBadGeneratedColumn},

		// Can insert without generated columns.
		{`insert into test_gc_write (a, b) values (1, 1)`, 0},
		{`insert into test_gc_write set a = 2, b = 2`, 0},
		{`insert into test_gc_write (b) select c from test_gc_write`, 0},
		// Can update without generated columns.
		{`update test_gc_write set b = 2 where a = 2`, 0},
		{`update test_gc_write t1, test_gc_write_1 t2 set t1.b = 3, t2.b = 4`, 0},

		// But now we can't do this, just as same with MySQL 5.7:
		{`insert into test_gc_write values (1, 1)`, mysql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write select 1, 1`, mysql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write (c) select a, b from test_gc_write`, mysql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write (b, c) select a, b from test_gc_write`, mysql.ErrBadGeneratedColumn},
	}
	for _, tt := range tests {
		if tt.err != 0 {
			tk.MustGetErrCode(tt.stmt, tt.err)
		} else {
			tk.MustExec(tt.stmt)
		}
	}
}

// TestGeneratedColumnRead tests select generated columns from table.
// They should be calculated from their generation expressions.
func TestGeneratedColumnRead(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_gc_read(a int primary key, b int, c int as (a+b), d int as (a*b) stored, e int as (c*2))`)

	tk.MustQuery(`SELECT generation_expression FROM information_schema.columns WHERE table_name = 'test_gc_read' AND column_name = 'd'`).Check(testkit.Rows("`a` * `b`"))

	// Insert only column a and b, leave c and d be calculated from them.
	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (0,null),(1,2),(3,4)`)
	tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`))

	tk.MustExec(`INSERT INTO test_gc_read SET a = 5, b = 10`)
	tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 10 15 50 30`))

	tk.MustExec(`REPLACE INTO test_gc_read (a, b) VALUES (5, 6)`)
	tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 6 11 30 22`))

	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (5, 8) ON DUPLICATE KEY UPDATE b = 9`)
	tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 9 14 45 28`))

	// Test select only-generated-column-without-dependences.
	tk.MustQuery(`SELECT c, d FROM test_gc_read`).Check(testkit.Rows(`<nil> <nil>`, `3 2`, `7 12`, `14 45`))

	// Test select only virtual generated column that refers to other virtual generated columns.
	tk.MustQuery(`SELECT e FROM test_gc_read`).Check(testkit.Rows(`<nil>`, `6`, `14`, `28`))

	// Test order of on duplicate key update list.
	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (5, 8) ON DUPLICATE KEY UPDATE a = 6, b = a`)
	tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `6 6 12 36 24`))

	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (6, 8) ON DUPLICATE KEY UPDATE b = 8, a = b`)
	tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	// Test where-conditions on virtual/stored generated columns.
	tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 7`).Check(testkit.Rows(`3 4 7 12 14`))

	tk.MustQuery(`SELECT * FROM test_gc_read WHERE d = 64`).Check(testkit.Rows(`8 8 16 64 32`))

	tk.MustQuery(`SELECT * FROM test_gc_read WHERE e = 6`).Check(testkit.Rows(`1 2 3 2 6`))

	// Test update where-conditions on virtual/generated columns.
	tk.MustExec(`UPDATE test_gc_read SET a = a + 100 WHERE c = 7`)
	tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 107`).Check(testkit.Rows(`103 4 107 412 214`))

	// Test update where-conditions on virtual/generated columns.
	tk.MustExec(`UPDATE test_gc_read m SET m.a = m.a + 100 WHERE c = 107`)
	tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 207`).Check(testkit.Rows(`203 4 207 812 414`))

	tk.MustExec(`UPDATE test_gc_read SET a = a - 200 WHERE d = 812`)
	tk.MustQuery(`SELECT * FROM test_gc_read WHERE d = 12`).Check(testkit.Rows(`3 4 7 12 14`))

	tk.MustExec(`INSERT INTO test_gc_read set a = 4, b = d + 1`)
	tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`,
		`4 <nil> <nil> <nil> <nil>`, `8 8 16 64 32`))
	tk.MustExec(`DELETE FROM test_gc_read where a = 4`)

	// Test on-conditions on virtual/stored generated columns.
	tk.MustExec(`CREATE TABLE test_gc_help(a int primary key, b int, c int, d int, e int)`)
	tk.MustExec(`INSERT INTO test_gc_help(a, b, c, d, e) SELECT * FROM test_gc_read`)

	tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.c = t2.c ORDER BY t1.a`).Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.d = t2.d ORDER BY t1.a`).Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.e = t2.e ORDER BY t1.a`).Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	// Test generated column in subqueries.
	tk.MustQuery(`SELECT * FROM test_gc_read t WHERE t.a not in (SELECT t.a FROM test_gc_read t where t.c > 5)`).Sort().Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`))
	tk.MustQuery(`SELECT * FROM test_gc_read t WHERE t.c in (SELECT t.c FROM test_gc_read t where t.c > 5)`).Sort().Check(testkit.Rows(`3 4 7 12 14`, `8 8 16 64 32`))

	tk.MustQuery(`SELECT tt.b FROM test_gc_read tt WHERE tt.a = (SELECT max(t.a) FROM test_gc_read t WHERE t.c = tt.c) ORDER BY b`).Check(testkit.Rows(`2`, `4`, `8`))

	// Test aggregation on virtual/stored generated columns.
	tk.MustQuery(`SELECT c, sum(a) aa, max(d) dd, sum(e) ee FROM test_gc_read GROUP BY c ORDER BY aa`).Check(testkit.Rows(`<nil> 0 <nil> <nil>`, `3 1 2 6`, `7 3 12 14`, `16 8 64 32`))

	tk.MustQuery(`SELECT a, sum(c), sum(d), sum(e) FROM test_gc_read GROUP BY a ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 3 2 6`, `3 7 12 14`, `8 16 64 32`))

	// Test multi-update on generated columns.
	tk.MustExec(`UPDATE test_gc_read m, test_gc_read n SET m.b = m.b + 10, n.b = n.b + 10`)
	tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`).Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 12 13 12 26`, `3 14 17 42 34`, `8 18 26 144 52`))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(8)")
	tk.MustExec("update test_gc_read set a = a+1 where a in (select a from t)")
	tk.MustQuery("select * from test_gc_read order by a").Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 12 13 12 26`, `3 14 17 42 34`, `9 18 27 162 54`))

	// Test different types between generation expression and generated column.
	tk.MustExec(`CREATE TABLE test_gc_read_cast(a VARCHAR(255), b VARCHAR(255), c INT AS (JSON_EXTRACT(a, b)), d INT AS (JSON_EXTRACT(a, b)) STORED)`)
	tk.MustExec(`INSERT INTO test_gc_read_cast (a, b) VALUES ('{"a": "3"}', '$.a')`)
	tk.MustQuery(`SELECT c, d FROM test_gc_read_cast`).Check(testkit.Rows(`3 3`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_1(a VARCHAR(255), b VARCHAR(255), c ENUM("red", "yellow") AS (JSON_UNQUOTE(JSON_EXTRACT(a, b))))`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_1 (a, b) VALUES ('{"a": "yellow"}', '$.a')`)
	tk.MustQuery(`SELECT c FROM test_gc_read_cast_1`).Check(testkit.Rows(`yellow`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_2( a JSON, b JSON AS (a->>'$.a'))`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_2(a) VALUES ('{"a": "{    \\\"key\\\": \\\"\\u6d4b\\\"    }"}')`)
	tk.MustQuery(`SELECT b FROM test_gc_read_cast_2`).Check(testkit.Rows(`{"key": "测"}`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_3( a JSON, b JSON AS (a->>'$.a'), c INT AS (b * 3.14) )`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_3(a) VALUES ('{"a": "5"}')`)
	tk.MustQuery(`SELECT c FROM test_gc_read_cast_3`).Check(testkit.Rows(`16`))

	require.Error(t, tk.ExecToErr(`INSERT INTO test_gc_read_cast_1 (a, b) VALUES ('{"a": "invalid"}', '$.a')`))

	// Test read generated columns after drop some irrelevant column
	tk.MustExec(`DROP TABLE IF EXISTS test_gc_read_m`)
	tk.MustExec(`CREATE TABLE test_gc_read_m (a int primary key, b int, c int as (a+1), d int as (c*2))`)
	tk.MustExec(`INSERT INTO test_gc_read_m(a) values (1), (2)`)
	tk.MustExec(`ALTER TABLE test_gc_read_m DROP b`)
	tk.MustQuery(`SELECT * FROM test_gc_read_m`).Check(testkit.Rows(`1 2 4`, `2 3 6`))

	// Test not null generated columns.
	tk.MustExec(`CREATE TABLE test_gc_read_1(a int primary key, b int, c int as (a+b) not null, d int as (a*b) stored)`)
	tk.MustExec(`CREATE TABLE test_gc_read_2(a int primary key, b int, c int as (a+b), d int as (a*b) stored not null)`)
	tests := []struct {
		stmt string
		err  int
	}{
		// Can't insert these records, because generated columns are not null.
		{`insert into test_gc_read_1(a, b) values (1, null)`, mysql.ErrBadNull},
		{`insert into test_gc_read_2(a, b) values (1, null)`, mysql.ErrBadNull},
	}
	for _, tt := range tests {
		if tt.err != 0 {
			tk.MustGetErrCode(tt.stmt, tt.err)
		} else {
			tk.MustExec(tt.stmt)
		}
	}
}

// TestGeneratedColumnRead tests generated columns using point get and batch point get
func TestGeneratedColumnPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tu")
	tk.MustExec("CREATE TABLE tu(a int, b int, c int GENERATED ALWAYS AS (a + b) VIRTUAL, d int as (a * b) stored, " +
		"e int GENERATED ALWAYS as (b * 2) VIRTUAL, PRIMARY KEY (a), UNIQUE KEY ukc (c), unique key ukd(d), key ke(e))")
	tk.MustExec("insert into tu(a, b) values(1, 2)")
	tk.MustExec("insert into tu(a, b) values(5, 6)")
	tk.MustQuery("select * from tu for update").Check(testkit.Rows("1 2 3 2 4", "5 6 11 30 12"))
	tk.MustQuery("select * from tu where a = 1").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where a in (1, 2)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where c in (1, 2, 3)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where c = 3").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select d, e from tu where c = 3").Check(testkit.Rows("2 4"))
	tk.MustQuery("select * from tu where d in (1, 2, 3)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where d = 2").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select c, d from tu where d = 2").Check(testkit.Rows("3 2"))
	tk.MustQuery("select d, e from tu where e = 4").Check(testkit.Rows("2 4"))
	tk.MustQuery("select * from tu where e = 4").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustExec("update tu set a = a + 1, b = b + 1 where c = 11")
	tk.MustQuery("select * from tu for update").Check(testkit.Rows("1 2 3 2 4", "6 7 13 42 14"))
	tk.MustQuery("select * from tu where a = 6").Check(testkit.Rows("6 7 13 42 14"))
	tk.MustQuery("select * from tu where c in (5, 6, 13)").Check(testkit.Rows("6 7 13 42 14"))
	tk.MustQuery("select b, c, e, d from tu where c = 13").Check(testkit.Rows("7 13 14 42"))
	tk.MustQuery("select a, e, d from tu where c in (5, 6, 13)").Check(testkit.Rows("6 14 42"))
	tk.MustExec("drop table if exists tu")
}

func TestUnionAutoSignedCast(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (id int, i int, b bigint, d double, dd decimal)")
	tk.MustExec("create table t2 (id int, i int unsigned, b bigint unsigned, d double unsigned, dd decimal unsigned)")
	tk.MustExec("insert into t1 values(1, -1, -1, -1.1, -1)")
	tk.MustExec("insert into t2 values(2, 1, 1, 1.1, 1)")
	tk.MustQuery("select * from t1 union select * from t2 order by id").
		Check(testkit.Rows("1 -1 -1 -1.1 -1", "2 1 1 1.1 1"))
	tk.MustQuery("select id, i, b, d, dd from t2 union select id, i, b, d, dd from t1 order by id").
		Check(testkit.Rows("1 -1 -1 -1.1 -1", "2 1 1 1.1 1"))
	tk.MustQuery("select id, i from t2 union select id, cast(i as unsigned int) from t1 order by id").
		Check(testkit.Rows("1 18446744073709551615", "2 1"))
	tk.MustQuery("select dd from t2 union all select dd from t2").
		Check(testkit.Rows("1", "1"))

	tk.MustExec("drop table if exists t3,t4")
	tk.MustExec("create table t3 (id int, v int)")
	tk.MustExec("create table t4 (id int, v double unsigned)")
	tk.MustExec("insert into t3 values (1, -1)")
	tk.MustExec("insert into t4 values (2, 1)")
	tk.MustQuery("select id, v from t3 union select id, v from t4 order by id").
		Check(testkit.Rows("1 -1", "2 1"))
	tk.MustQuery("select id, v from t4 union select id, v from t3 order by id").
		Check(testkit.Rows("1 -1", "2 1"))

	tk.MustExec("drop table if exists t5,t6,t7")
	tk.MustExec("create table t5 (id int, v bigint unsigned)")
	tk.MustExec("create table t6 (id int, v decimal)")
	tk.MustExec("create table t7 (id int, v bigint)")
	tk.MustExec("insert into t5 values (1, 1)")
	tk.MustExec("insert into t6 values (2, -1)")
	tk.MustExec("insert into t7 values (3, -1)")
	tk.MustQuery("select id, v from t5 union select id, v from t6 order by id").
		Check(testkit.Rows("1 1", "2 -1"))
	tk.MustQuery("select id, v from t5 union select id, v from t7 union select id, v from t6 order by id").
		Check(testkit.Rows("1 1", "2 -1", "3 -1"))
}

func TestUpdateClustered(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	type resultChecker struct {
		check  string
		assert []string
	}

	for _, clustered := range []string{"", "clustered"} {
		tests := []struct {
			initSchema  []string
			initData    []string
			dml         string
			resultCheck []resultChecker
		}{
			{ // left join + update both + match & unmatched + pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 int, k2 int, v int)",
					fmt.Sprintf("create table b (a int not null, k1 int, k2 int, v int, primary key(k1, k2) %s)", clustered),
				},
				[]string{
					"insert into a values (1, 1, 1), (2, 2, 2)", // unmatched + matched
					"insert into b values (2, 2, 2, 2)",
				},
				"update a left join b on a.k1 = b.k1 and a.k2 = b.k2 set a.v = 20, b.v = 100, a.k1 = a.k1 + 1, b.k1 = b.k1 + 1, a.k2 = a.k2 + 2, b.k2 = b.k2 + 2",
				[]resultChecker{
					{
						"select * from b",
						[]string{"2 3 4 100"},
					},
					{
						"select * from a",
						[]string{"2 3 20", "3 4 20"},
					},
				},
			},
			{ // left join + update both + match & unmatched + pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 int, k2 int, v int)",
					fmt.Sprintf("create table b (a int not null, k1 int, k2 int, v int, primary key(k1, k2) %s)", clustered),
				},
				[]string{
					"insert into a values (1, 1, 1), (2, 2, 2)", // unmatched + matched
					"insert into b values (2, 2, 2, 2)",
				},
				"update a left join b on a.k1 = b.k1 and a.k2 = b.k2 set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, b.k1 = b.k1 + 1, b.k2 = b.k2 + 2,  a.v = 20, b.v = 100",
				[]resultChecker{
					{
						"select * from b",
						[]string{"2 3 4 100"},
					},
					{
						"select * from a",
						[]string{"2 3 20", "3 4 20"},
					},
				},
			},
			{ // left join + update both + match & unmatched + prefix pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 varchar(100), k2 varchar(100), v varchar(100))",
					fmt.Sprintf("create table b (a varchar(100) not null, k1 varchar(100), k2 varchar(100), v varchar(100), primary key(k1(1), k2(1)) %s, key kk1(k1(1), v(1)))", clustered),
				},
				[]string{
					"insert into a values ('11', '11', '11'), ('22', '22', '22')", // unmatched + matched
					"insert into b values ('22', '22', '22', '22')",
				},
				"update a left join b on a.k1 = b.k1 and a.k2 = b.k2 set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, b.k1 = b.k1 + 1, b.k2 = b.k2 + 2, a.v = 20, b.v = 100",
				[]resultChecker{
					{
						"select * from b",
						[]string{"22 23 24 100"},
					},
					{
						"select * from a",
						[]string{"12 13 20", "23 24 20"},
					},
				},
			},
			{ // right join + update both + match & unmatched + prefix pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 varchar(100), k2 varchar(100), v varchar(100))",
					fmt.Sprintf("create table b (a varchar(100) not null, k1 varchar(100), k2 varchar(100), v varchar(100), primary key(k1(1), k2(1)) %s, key kk1(k1(1), v(1)))", clustered),
				},
				[]string{
					"insert into a values ('11', '11', '11'), ('22', '22', '22')", // unmatched + matched
					"insert into b values ('22', '22', '22', '22')",
				},
				"update b right join a on a.k1 = b.k1 and a.k2 = b.k2 set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, b.k1 = b.k1 + 1, b.k2 = b.k2 + 2, a.v = 20, b.v = 100",
				[]resultChecker{
					{
						"select * from b",
						[]string{"22 23 24 100"},
					},
					{
						"select * from a",
						[]string{"12 13 20", "23 24 20"},
					},
				},
			},
			{ // inner join + update both + match & unmatched + prefix pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 varchar(100), k2 varchar(100), v varchar(100))",
					fmt.Sprintf("create table b (a varchar(100) not null, k1 varchar(100), k2 varchar(100), v varchar(100), primary key(k1(1), k2(1)) %s, key kk1(k1(1), v(1)))", clustered),
				},
				[]string{
					"insert into a values ('11', '11', '11'), ('22', '22', '22')", // unmatched + matched
					"insert into b values ('22', '22', '22', '22')",
				},
				"update b join a on a.k1 = b.k1 and a.k2 = b.k2 set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, b.k1 = b.k1 + 1, b.k2 = b.k2 + 2, a.v = 20, b.v = 100",
				[]resultChecker{
					{
						"select * from b",
						[]string{"22 23 24 100"},
					},
					{
						"select * from a",
						[]string{"11 11 11", "23 24 20"},
					},
				},
			},
			{
				[]string{
					"drop table if exists a, b",
					"create table a (k1 varchar(100), k2 varchar(100), v varchar(100))",
					fmt.Sprintf("create table b (a varchar(100) not null, k1 varchar(100), k2 varchar(100), v varchar(100), primary key(k1(1), k2(1)) %s, key kk1(k1(1), v(1)))", clustered),
				},
				[]string{
					"insert into a values ('11', '11', '11'), ('22', '22', '22')", // unmatched + matched
					"insert into b values ('22', '22', '22', '22')",
				},
				"update a set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, a.v = 20 where exists (select 1 from b where a.k1 = b.k1 and a.k2 = b.k2)",
				[]resultChecker{
					{
						"select * from b",
						[]string{"22 22 22 22"},
					},
					{
						"select * from a",
						[]string{"11 11 11", "23 24 20"},
					},
				},
			},
		}

		for _, test := range tests {
			for _, s := range test.initSchema {
				tk.MustExec(s)
			}
			for _, s := range test.initData {
				tk.MustExec(s)
			}
			tk.MustExec(test.dml)
			for _, checker := range test.resultCheck {
				tk.MustQuery(checker.check).Check(testkit.Rows(checker.assert...))
			}
			tk.MustExec("admin check table a")
			tk.MustExec("admin check table b")
		}
	}
}

func TestSelectPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("set @@session.tidb_enable_list_partition = ON;")
	tk.MustExec(`create table th (a int, b int) partition by hash(a) partitions 3;`)
	tk.MustExec(`create table tr (a int, b int)
							partition by range (a) (
							partition r0 values less than (4),
							partition r1 values less than (7),
							partition r3 values less than maxvalue)`)
	tk.MustExec(`create table tl (a int, b int, unique index idx(a)) partition by list  (a) (
					    partition p0 values in (3,5,6,9,17),
					    partition p1 values in (1,2,10,11,19,20),
					    partition p2 values in (4,12,13,14,18),
					    partition p3 values in (7,8,15,16,null));`)
	tk.MustExec(`insert into th values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);`)
	tk.MustExec("insert into th values (-1,-1),(-2,-2),(-3,-3),(-4,-4),(-5,-5),(-6,-6),(-7,-7),(-8,-8);")
	tk.MustExec(`insert into tr values (-3,-3),(3,3),(4,4),(7,7),(8,8);`)
	tk.MustExec(`insert into tl values (3,3),(1,1),(4,4),(7,7),(8,8),(null,null);`)
	// select 1 partition.
	tk.MustQuery("select b from th partition (p0) order by a").Check(testkit.Rows("-6", "-3", "0", "3", "6"))
	tk.MustQuery("select b from tr partition (r0) order by a").Check(testkit.Rows("-3", "3"))
	tk.MustQuery("select b from tl partition (p0) order by a").Check(testkit.Rows("3"))
	tk.MustQuery("select b from th partition (p0,P0) order by a").Check(testkit.Rows("-6", "-3", "0", "3", "6"))
	tk.MustQuery("select b from tr partition (r0,R0,r0) order by a").Check(testkit.Rows("-3", "3"))
	tk.MustQuery("select b from tl partition (p0,P0,p0) order by a").Check(testkit.Rows("3"))
	// select multi partition.
	tk.MustQuery("select b from th partition (P2,p0) order by a").Check(testkit.Rows("-8", "-6", "-5", "-3", "-2", "0", "2", "3", "5", "6", "8"))
	tk.MustQuery("select b from tr partition (r1,R3) order by a").Check(testkit.Rows("4", "7", "8"))
	tk.MustQuery("select b from tl partition (p0,P3) order by a").Check(testkit.Rows("<nil>", "3", "7", "8"))

	// test select unknown partition error
	tk.MustGetErrMsg("select b from th partition (p0,p4)", "[table:1735]Unknown partition 'p4' in table 'th'")
	tk.MustGetErrMsg("select b from tr partition (r1,r4)", "[table:1735]Unknown partition 'r4' in table 'tr'")
	tk.MustGetErrMsg("select b from tl partition (p0,p4)", "[table:1735]Unknown partition 'p4' in table 'tl'")

	// test select partition table in transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into th values (10,10),(11,11)")
	tk.MustQuery("select a, b from th where b>10").Check(testkit.Rows("11 11"))
	tk.MustExec("commit")
	tk.MustQuery("select a, b from th where b>10").Check(testkit.Rows("11 11"))

	// test partition function is scalar func
	tk.MustExec("drop table if exists tscalar")
	tk.MustExec(`create table tscalar (c1 int) partition by range (c1 % 30) (
								partition p0 values less than (0),
								partition p1 values less than (10),
								partition p2 values less than (20),
								partition pm values less than (maxvalue));`)
	tk.MustExec("insert into tscalar values(0), (10), (40), (50), (55)")
	// test IN expression
	tk.MustExec("insert into tscalar values(-0), (-10), (-40), (-50), (-55)")
	tk.MustQuery("select * from tscalar where c1 in (55, 55)").Check(testkit.Rows("55"))
	tk.MustQuery("select * from tscalar where c1 in (40, 40)").Check(testkit.Rows("40"))
	tk.MustQuery("select * from tscalar where c1 in (40)").Check(testkit.Rows("40"))
	tk.MustQuery("select * from tscalar where c1 in (-40)").Check(testkit.Rows("-40"))
	tk.MustQuery("select * from tscalar where c1 in (-40, -40)").Check(testkit.Rows("-40"))
	tk.MustQuery("select * from tscalar where c1 in (-1)").Check(testkit.Rows())
}

func TestDeletePartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1 (a int) partition by range (a) (
 partition p0 values less than (10),
 partition p1 values less than (20),
 partition p2 values less than (30),
 partition p3 values less than (40),
 partition p4 values less than MAXVALUE
 )`)
	tk.MustExec("insert into t1 values (1),(11),(21),(31)")
	tk.MustExec("delete from t1 partition (p4)")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("1", "11", "21", "31"))
	tk.MustExec("delete from t1 partition (p0) where a > 10")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("1", "11", "21", "31"))
	tk.MustExec("delete from t1 partition (p0,p1,p2)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("31"))
}

func TestPrepareLoadData(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustGetErrCode(`prepare stmt from "load data local infile '/tmp/load_data_test.csv' into table test";`, mysql.ErrUnsupportedPs)
}

func TestSetOperation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1, t2, t3`)
	tk.MustExec(`create table t1(a int)`)
	tk.MustExec(`create table t2 like t1`)
	tk.MustExec(`create table t3 like t1`)
	tk.MustExec(`insert into t1 values (1),(1),(2),(3),(null)`)
	tk.MustExec(`insert into t2 values (1),(2),(null),(null)`)
	tk.MustExec(`insert into t3 values (2),(3)`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	executorSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func TestSetOperationOnDiffColType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1, t2, t3`)
	tk.MustExec(`create table t1(a int, b int)`)
	tk.MustExec(`create table t2(a int, b varchar(20))`)
	tk.MustExec(`create table t3(a int, b decimal(30,10))`)
	tk.MustExec(`insert into t1 values (1,1),(1,1),(2,2),(3,3),(null,null)`)
	tk.MustExec(`insert into t2 values (1,'1'),(2,'2'),(null,null),(null,'3')`)
	tk.MustExec(`insert into t3 values (2,2.1),(3,3)`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	executorSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

// issue-23038: wrong key range of index scan for year column
func TestIndexScanWithYearCol(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 year(4), c2 int, key(c1));")
	tk.MustExec("insert into t values(2001, 1);")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	executorSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func TestClusterIndexOuterJoinElimination(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (a int, b int, c int, primary key(a,b))")
	rows := tk.MustQuery(`explain format = 'brief' select t1.a from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b`).Rows()
	rowStrs := testdata.ConvertRowsToStrings(rows)
	for _, row := range rowStrs {
		// outer join has been eliminated.
		require.NotContains(t, row, "Join")
	}
}

func TestPlanReplayerDumpSingle(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_dump_single")
	tk.MustExec("create table t_dump_single(a int)")
	res := tk.MustQuery("plan replayer dump explain select * from t_dump_single")
	path := testdata.ConvertRowsToStrings(res.Rows())

	reader, err := zip.OpenReader(filepath.Join(domain.GetPlanReplayerDirName(), path[0]))
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	for _, file := range reader.File {
		require.True(t, checkFileName(file.Name))
	}
}

func TestDropColWithPrimaryKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, c1 int, c2 int, c3 int, index idx1(c1, c2), index idx2(c3))")
	tk.MustExec("set global tidb_enable_change_multi_schema = off")
	tk.MustGetErrMsg("alter table t drop column id", "[ddl:8200]Unsupported drop integer primary key")
	tk.MustGetErrMsg("alter table t drop column c1", "[ddl:8200]can't drop column c1 with composite index covered or Primary Key covered now")
	tk.MustGetErrMsg("alter table t drop column c3", "[ddl:8200]can't drop column c3 with tidb_enable_change_multi_schema is disable")
	tk.MustExec("set global tidb_enable_change_multi_schema = on")
	tk.MustExec("alter table t drop column c3")
}
