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
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	error2 "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
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

func TestUnsignedFeedback(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	oriProbability := statistics.FeedbackProbability.Load()
	statistics.FeedbackProbability.Store(1.0)
	defer func() { statistics.FeedbackProbability.Store(oriProbability) }()
	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint unsigned, b int, primary key(a))")
	tk.MustExec("insert into t values (1,1),(2,2)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(distinct b) from t").Check(testkit.Rows("2"))
	result := tk.MustQuery("explain analyze select count(distinct b) from t")
	require.Equal(t, "table:t", result.Rows()[2][4])
	require.Equal(t, "keep order:false", result.Rows()[2][6])
}

func TestAlterTableComment(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_1")
	tk.MustExec("create table t_1 (c1 int, c2 int, c3 int default 1, index (c1)) comment = 'test table';")
	tk.MustExec("alter table `t_1` comment 'this is table comment';")
	tk.MustQuery("select table_comment from information_schema.tables where table_name = 't_1';").Check(testkit.Rows("this is table comment"))
	tk.MustExec("alter table `t_1` comment 'table t comment';")
	tk.MustQuery("select table_comment from information_schema.tables where table_name = 't_1';").Check(testkit.Rows("table t comment"))
}

func TestTimezonePushDown(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (ts timestamp)")
	defer tk.MustExec("drop table t")
	tk.MustExec(`insert into t values ("2018-09-13 10:02:06")`)

	systemTZ := timeutil.SystemLocation()
	require.NotEqual(t, "System", systemTZ.String())
	require.NotEqual(t, "Local", systemTZ.String())
	ctx := context.Background()
	count := 0
	ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *kv.Request) {
		count += 1
		dagReq := new(tipb.DAGRequest)
		require.NoError(t, proto.Unmarshal(req.Data, dagReq))
		require.Equal(t, systemTZ.String(), dagReq.GetTimeZoneName())
	})
	_, err := tk.Session().Execute(ctx1, `select * from t where ts = "2018-09-13 10:02:06"`)
	require.NoError(t, err)

	tk.MustExec(`set time_zone="System"`)
	_, err = tk.Session().Execute(ctx1, `select * from t where ts = "2018-09-13 10:02:06"`)
	require.NoError(t, err)

	require.Equal(t, 2, count) // Make sure the hook function is called.
}

func TestNotFillCacheFlag(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	tk.MustExec("insert into t values (1)")

	tests := []struct {
		sql    string
		expect bool
	}{
		{"select SQL_NO_CACHE * from t", true},
		{"select SQL_CACHE * from t", false},
		{"select * from t", false},
	}
	count := 0
	ctx := context.Background()
	for _, test := range tests {
		ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *kv.Request) {
			count++
			comment := fmt.Sprintf("sql=%s, expect=%v, get=%v", test.sql, test.expect, req.NotFillCache)
			require.Equal(t, test.expect, req.NotFillCache, comment)
		})
		rs, err := tk.Session().Execute(ctx1, test.sql)
		require.NoError(t, err)
		tk.ResultSetToResult(rs[0], fmt.Sprintf("sql: %v", test.sql))
	}
	require.Equal(t, len(tests), count) // Make sure the hook function is called.
}

func TestHandleTransfer(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1), (2), (4)")
	tk.MustExec("begin")
	tk.MustExec("update t set a = 3 where a = 4")
	// test table scan read whose result need handle.
	tk.MustQuery("select * from t ignore index(idx)").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("insert into t values(4)")
	// test single read whose result need handle
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("select * from t use index(idx) order by a desc").Check(testkit.Rows("4", "3", "2", "1"))
	tk.MustExec("update t set a = 5 where a = 3")
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1", "2", "4", "5"))
	tk.MustExec("commit")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("insert into t values(3, 3), (1, 1), (2, 2)")
	// Second test double read.
	tk.MustQuery("select * from t use index(idx) order by a").Check(testkit.Rows("1 1", "2 2", "3 3"))
}

func TestExecutorBit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 bit(2))")
	tk.MustExec("insert into t values (0), (1), (2), (3)")
	err := tk.ExecToErr("insert into t values (4)")
	require.Error(t, err)
	err = tk.ExecToErr("insert into t values ('a')")
	require.Error(t, err)
	r, err := tk.Exec("select * from t where c1 = 2")
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, types.NewBinaryLiteralFromUint(2, -1), types.BinaryLiteral(req.GetRow(0).GetBytes(0)))
	require.NoError(t, r.Close())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(31))")
	tk.MustExec("insert into t values (0x7fffffff)")
	err = tk.ExecToErr("insert into t values (0x80000000)")
	require.Error(t, err)
	err = tk.ExecToErr("insert into t values (0xffffffff)")
	require.Error(t, err)
	tk.MustExec("insert into t values ('123')")
	tk.MustExec("insert into t values ('1234')")
	err = tk.ExecToErr("insert into t values ('12345)")
	require.Error(t, err)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(62))")
	tk.MustExec("insert into t values ('12345678')")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(61))")
	err = tk.ExecToErr("insert into t values ('12345678')")
	require.Error(t, err)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(32))")
	tk.MustExec("insert into t values (0x7fffffff)")
	tk.MustExec("insert into t values (0xffffffff)")
	err = tk.ExecToErr("insert into t values (0x1ffffffff)")
	require.Error(t, err)
	tk.MustExec("insert into t values ('1234')")
	err = tk.ExecToErr("insert into t values ('12345')")
	require.Error(t, err)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(64))")
	tk.MustExec("insert into t values (0xffffffffffffffff)")
	tk.MustExec("insert into t values ('12345678')")
	err = tk.ExecToErr("insert into t values ('123456789')")
	require.Error(t, err)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(64))")
	tk.MustExec("insert into t values (0xffffffffffffffff)")
	tk.MustExec("insert into t values ('12345678')")
	tk.MustQuery("select * from t where c1").Check(testkit.Rows("\xff\xff\xff\xff\xff\xff\xff\xff", "12345678"))
}

func TestExecutorEnum(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c enum('a', 'b', 'c'))")
	tk.MustExec("insert into t values ('a'), (2), ('c')")
	tk.MustQuery("select * from t where c = 'a'").Check(testkit.Rows("a"))

	tk.MustQuery("select c + 1 from t where c = 2").Check(testkit.Rows("3"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (null), ('1')")
	tk.MustQuery("select c + 1 from t where c = 1").Check(testkit.Rows("2"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(1), (2), (3)")
	tk.MustQuery("select * from t where c").Check(testkit.Rows("a", "b", "c"))
}

func TestExecutorSet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c set('a', 'b', 'c'))")
	tk.MustExec("insert into t values ('a'), (2), ('c'), ('a,b'), ('b,a')")
	tk.MustQuery("select * from t where c = 'a'").Check(testkit.Rows("a"))
	tk.MustQuery("select * from t where c = 'a,b'").Check(testkit.Rows("a,b", "a,b"))
	tk.MustQuery("select c + 1 from t where c = 2").Check(testkit.Rows("3"))
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (null), ('1')")
	tk.MustQuery("select c + 1 from t where c = 1").Check(testkit.Rows("2"))
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(3)")
	tk.MustQuery("select * from t where c").Check(testkit.Rows("a,b"))
}

func TestSubQueryInValues(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, name varchar(20))")
	tk.MustExec("create table t1 (gid int)")
	tk.MustExec("insert into t1 (gid) value (1)")
	tk.MustExec("insert into t (id, name) value ((select gid from t1) ,'asd')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 asd"))
}

func TestEnhancedRangeAccess(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values(1, 2), (2, 1)")
	tk.MustQuery("select * from t where (a = 1 and b = 2) or (a = 2 and b = 1)").Check(testkit.Rows("1 2", "2 1"))
	tk.MustQuery("select * from t where (a = 1 and b = 1) or (a = 2 and b = 2)").Check(nil)
}

// TestMaxInt64Handle Issue #4810
func TestMaxInt64Handle(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id bigint, PRIMARY KEY (id))")
	tk.MustExec("insert into t values(9223372036854775807)")
	tk.MustExec("select * from t where id = 9223372036854775807")
	tk.MustQuery("select * from t where id = 9223372036854775807;").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("select * from t").Check(testkit.Rows("9223372036854775807"))
	err := tk.ExecToErr("insert into t values(9223372036854775807)")
	require.Error(t, err)
	tk.MustExec("delete from t where id = 9223372036854775807")
	tk.MustQuery("select * from t").Check(nil)
}

func TestTableScanWithPointRanges(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, PRIMARY KEY (id))")
	tk.MustExec("insert into t values(1), (5), (10)")
	tk.MustQuery("select * from t where id in(1, 2, 10)").Check(testkit.Rows("1", "10"))
}

func TestUnsignedPk(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id bigint unsigned primary key)")
	var num1, num2 uint64 = math.MaxInt64 + 1, math.MaxInt64 + 2
	tk.MustExec(fmt.Sprintf("insert into t values(%v), (%v), (1), (2)", num1, num2))
	num1Str := strconv.FormatUint(num1, 10)
	num2Str := strconv.FormatUint(num2, 10)
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1", "2", num1Str, num2Str))
	tk.MustQuery("select * from t where id not in (2)").Check(testkit.Rows(num1Str, num2Str, "1"))
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a bigint unsigned primary key, b int, index idx(b))")
	tk.MustExec("insert into t values(9223372036854775808, 1), (1, 1)")
	tk.MustQuery("select * from t use index(idx) where b = 1 and a < 2").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t use index(idx) where b = 1 order by b, a").Check(testkit.Rows("1 1", "9223372036854775808 1"))
}

func TestSignedCommonHandle(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("use test")
	tk.MustExec("create table t(k1 int, k2 int, primary key(k1, k2))")
	tk.MustExec("insert into t(k1, k2) value(-100, 1), (-50, 1), (0, 0), (1, 1), (3, 3)")
	tk.MustQuery("select k1 from t order by k1").Check(testkit.Rows("-100", "-50", "0", "1", "3"))
	tk.MustQuery("select k1 from t order by k1 desc").Check(testkit.Rows("3", "1", "0", "-50", "-100"))
	tk.MustQuery("select k1 from t where k1 < -51").Check(testkit.Rows("-100"))
	tk.MustQuery("select k1 from t where k1 < -1").Check(testkit.Rows("-100", "-50"))
	tk.MustQuery("select k1 from t where k1 <= 0").Check(testkit.Rows("-100", "-50", "0"))
	tk.MustQuery("select k1 from t where k1 < 2").Check(testkit.Rows("-100", "-50", "0", "1"))
	tk.MustQuery("select k1 from t where k1 < -1 and k1 > -90").Check(testkit.Rows("-50"))
}

func TestContainDotColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test.t1(t1.a char)")
	tk.MustExec("create table t2(a char, t2.b int)")

	tk.MustGetErrCode("create table t3(s.a char);", mysql.ErrWrongTableName)
}

func TestCheckIndex(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	ctx := mock.NewContext()
	ctx.Store = store
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()

	_, err = se.Execute(context.Background(), "create database test_admin")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "use test_admin")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "create table t (pk int primary key, c int default 1, c1 int default 1, unique key c(c))")
	require.NoError(t, err)

	is := dom.InfoSchema()
	db := model.NewCIStr("test_admin")
	dbInfo, ok := is.SchemaByName(db)
	require.True(t, ok)

	tblName := model.NewCIStr("t")
	tbl, err := is.TableByName(db, tblName)
	require.NoError(t, err)
	tbInfo := tbl.Meta()

	alloc := autoid.NewAllocator(store, dbInfo.ID, tbInfo.ID, false, autoid.RowIDAllocType)
	tb, err := tables.TableFromMeta(autoid.NewAllocators(alloc), tbInfo)
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "admin check index t c")
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "admin check index t C")
	require.NoError(t, err)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20)
	// table     data (handle, data): (1, 10), (2, 20)
	recordVal1 := types.MakeDatums(int64(1), int64(10), int64(11))
	recordVal2 := types.MakeDatums(int64(2), int64(20), int64(21))
	require.NoError(t, ctx.NewTxn(context.Background()))
	_, err = tb.AddRecord(ctx, recordVal1)
	require.NoError(t, err)
	_, err = tb.AddRecord(ctx, recordVal2)
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(context.Background()))

	mockCtx := mock.NewContext()
	idx := tb.Indices()[0]
	sc := &stmtctx.StatementContext{TimeZone: time.Local}

	_, err = se.Execute(context.Background(), "admin check index t idx_inexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not exist")

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(30)), kv.IntHandle(3), nil)
	require.NoError(t, err)
	key := tablecodec.EncodeRowKey(tb.Meta().ID, kv.IntHandle(4).Encoded())
	setColValue(t, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "admin check index t c")
	require.Error(t, err)
	require.Equal(t, "[admin:8223]data inconsistency in table: t, index: c, handle: 3, index-values:\"handle: 3, values: [KindInt64 30 KindInt64 3]\" != record-values:\"\"", err.Error())

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(40)), kv.IntHandle(4), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "admin check index t c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "table count 3 != index(c) count 4")

	// set data to:
	// index     data (handle, data): (1, 10), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = store.Begin()
	require.NoError(t, err)
	err = idx.Delete(sc, txn, types.MakeDatums(int64(30)), kv.IntHandle(3))
	require.NoError(t, err)
	err = idx.Delete(sc, txn, types.MakeDatums(int64(20)), kv.IntHandle(2))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "admin check index t c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "table count 3 != index(c) count 2")

	// TODO: pass the case below：
	// set data to:
	// index     data (handle, data): (1, 10), (4, 40), (2, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
}

func setColValue(t *testing.T, txn kv.Transaction, key kv.Key, v types.Datum) {
	row := []types.Datum{v, {}}
	colIDs := []int64{2, 3}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	rd := rowcodec.Encoder{Enable: true}
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil, &rd)
	require.NoError(t, err)
	err = txn.Set(key, value)
	require.NoError(t, err)
}

func TestCheckTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Test 'admin check table' when the table has a unique index with null values.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test;")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2));")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (NULL, NULL);")
	tk.MustExec("admin check table admin_test;")
}

func TestCheckTableClusterIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists admin_test;")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key (c1, c2), index (c1), unique key(c2));")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("admin check table admin_test;")
}

func TestCoprocessorStreamingFlag(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, value int, index idx(id))")
	// Add some data to make statistics work.
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	tests := []struct {
		sql    string
		expect bool
	}{
		{"select * from t", true},                         // TableReader
		{"select * from t where id = 5", true},            // IndexLookup
		{"select * from t where id > 5", true},            // Filter
		{"select * from t limit 3", false},                // Limit
		{"select avg(id) from t", false},                  // Aggregate
		{"select * from t order by value limit 3", false}, // TopN
	}

	ctx := context.Background()
	for _, test := range tests {
		ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *kv.Request) {
			comment := fmt.Sprintf("sql=%s, expect=%v, get=%v", test.sql, test.expect, req.Streaming)
			require.Equal(t, test.expect, req.Streaming, comment)
		})
		rs, err := tk.Session().Execute(ctx1, test.sql)
		require.NoError(t, err)
		tk.ResultSetToResult(rs[0], fmt.Sprintf("sql: %v", test.sql))
	}
}

func TestIncorrectLimitArg(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`create table t(a bigint);`)
	tk.MustExec(`prepare stmt1 from 'select * from t limit ?';`)
	tk.MustExec(`prepare stmt2 from 'select * from t limit ?, ?';`)
	tk.MustExec(`set @a = -1;`)
	tk.MustExec(`set @b =  1;`)

	tk.MustGetErrMsg(`execute stmt1 using @a;`, `[planner:1210]Incorrect arguments to LIMIT`)
	tk.MustGetErrMsg(`execute stmt2 using @b, @a;`, `[planner:1210]Incorrect arguments to LIMIT`)
}

func TestExecutorLimit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`create table t(a bigint, b bigint);`)
	tk.MustExec(`insert into t values(1, 1), (2, 2), (3, 30), (4, 40), (5, 5), (6, 6);`)
	tk.MustQuery(`select * from t order by a limit 1, 1;`).Check(testkit.Rows("2 2"))
	tk.MustQuery(`select * from t order by a limit 1, 2;`).Check(testkit.Rows("2 2", "3 30"))
	tk.MustQuery(`select * from t order by a limit 1, 3;`).Check(testkit.Rows("2 2", "3 30", "4 40"))
	tk.MustQuery(`select * from t order by a limit 1, 4;`).Check(testkit.Rows("2 2", "3 30", "4 40", "5 5"))

	// test inline projection
	tk.MustQuery(`select a from t where a > 0 limit 1, 1;`).Check(testkit.Rows("2"))
	tk.MustQuery(`select a from t where a > 0 limit 1, 2;`).Check(testkit.Rows("2", "3"))
	tk.MustQuery(`select b from t where a > 0 limit 1, 3;`).Check(testkit.Rows("2", "30", "40"))
	tk.MustQuery(`select b from t where a > 0 limit 1, 4;`).Check(testkit.Rows("2", "30", "40", "5"))

	// test @@tidb_init_chunk_size=2
	tk.MustExec(`set @@tidb_init_chunk_size=2;`)
	tk.MustQuery(`select * from t where a > 0 limit 2, 1;`).Check(testkit.Rows("3 30"))
	tk.MustQuery(`select * from t where a > 0 limit 2, 2;`).Check(testkit.Rows("3 30", "4 40"))
	tk.MustQuery(`select * from t where a > 0 limit 2, 3;`).Check(testkit.Rows("3 30", "4 40", "5 5"))
	tk.MustQuery(`select * from t where a > 0 limit 2, 4;`).Check(testkit.Rows("3 30", "4 40", "5 5", "6 6"))

	// test inline projection
	tk.MustQuery(`select a from t order by a limit 2, 1;`).Check(testkit.Rows("3"))
	tk.MustQuery(`select b from t order by a limit 2, 2;`).Check(testkit.Rows("30", "40"))
	tk.MustQuery(`select a from t order by a limit 2, 3;`).Check(testkit.Rows("3", "4", "5"))
	tk.MustQuery(`select b from t order by a limit 2, 4;`).Check(testkit.Rows("30", "40", "5", "6"))
}

func TestIndexScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int unique)")
	tk.MustExec("insert t values (-1), (2), (3), (5), (6), (7), (8), (9)")
	tk.MustQuery("select a from t where a < 0 or (a >= 2.1 and a < 5.1) or ( a > 5.9 and a <= 7.9) or a > '8.1'").Check(testkit.Rows("-1", "3", "5", "6", "7", "9"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique)")
	tk.MustExec("insert t values (0)")
	tk.MustQuery("select NULL from t ").Check(testkit.Rows("<nil>"))

	// test for double read
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique, b int)")
	tk.MustExec("insert t values (5, 0)")
	tk.MustExec("insert t values (4, 0)")
	tk.MustExec("insert t values (3, 0)")
	tk.MustExec("insert t values (2, 0)")
	tk.MustExec("insert t values (1, 0)")
	tk.MustExec("insert t values (0, 0)")
	tk.MustQuery("select * from t order by a limit 3").Check(testkit.Rows("0 0", "1 0", "2 0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique, b int)")
	tk.MustExec("insert t values (0, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (2, 1)")
	tk.MustExec("insert t values (3, 2)")
	tk.MustExec("insert t values (4, 1)")
	tk.MustExec("insert t values (5, 2)")
	tk.MustQuery("select * from t where a < 5 and b = 1 limit 2").Check(testkit.Rows("0 1", "2 1"))

	tk.MustExec("drop table if exists tab1")
	tk.MustExec("CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col3 INTEGER, col4 FLOAT)")
	tk.MustExec("CREATE INDEX idx_tab1_0 on tab1 (col0)")
	tk.MustExec("CREATE INDEX idx_tab1_1 on tab1 (col1)")
	tk.MustExec("CREATE INDEX idx_tab1_3 on tab1 (col3)")
	tk.MustExec("CREATE INDEX idx_tab1_4 on tab1 (col4)")
	tk.MustExec("INSERT INTO tab1 VALUES(1,37,20.85,30,10.69)")
	tk.MustQuery("SELECT pk FROM tab1 WHERE ((col3 <= 6 OR col3 < 29 AND (col0 < 41)) OR col3 > 42) AND col1 >= 96.1 AND col3 = 30 AND col3 > 17 AND (col0 BETWEEN 36 AND 42)").Check(testkit.Rows())

	tk.MustExec("drop table if exists tab1")
	tk.MustExec("CREATE TABLE tab1(pk INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	tk.MustExec("CREATE INDEX idx_tab1_0 on tab1 (a)")
	tk.MustExec("INSERT INTO tab1 VALUES(1,1,1)")
	tk.MustExec("INSERT INTO tab1 VALUES(2,2,1)")
	tk.MustExec("INSERT INTO tab1 VALUES(3,1,2)")
	tk.MustExec("INSERT INTO tab1 VALUES(4,2,2)")
	tk.MustQuery("SELECT * FROM tab1 WHERE pk <= 3 AND a = 1").Check(testkit.Rows("1 1 1", "3 1 2"))
	tk.MustQuery("SELECT * FROM tab1 WHERE pk <= 4 AND a = 1 AND b = 2").Check(testkit.Rows("3 1 2"))

	tk.MustExec("CREATE INDEX idx_tab1_1 on tab1 (b, a)")
	tk.MustQuery("SELECT pk FROM tab1 WHERE b > 1").Check(testkit.Rows("3", "4"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a varchar(3), index(a))")
	tk.MustExec("insert t values('aaa'), ('aab')")
	tk.MustQuery("select * from t where a >= 'aaaa' and a < 'aabb'").Check(testkit.Rows("aab"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int primary key, b int, c int, index(c))")
	tk.MustExec("insert t values(1, 1, 1), (2, 2, 2), (4, 4, 4), (3, 3, 3), (5, 5, 5)")
	// Test for double read and top n.
	tk.MustQuery("select a from t where c >= 2 order by b desc limit 1").Check(testkit.Rows("5"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(50) primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values('aa', 1, 1)")
	tk.MustQuery("select * from t use index(idx) where a > 'a'").Check(testkit.Rows("aa 1 1"))

	// fix issue9636
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (a int, KEY (a))")
	tk.MustQuery(`SELECT * FROM (SELECT * FROM (SELECT a as d FROM t WHERE a IN ('100')) AS x WHERE x.d < "123" ) tmp_count`).Check(testkit.Rows())
}

func TestUpdateJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(k int, v int)")
	tk.MustExec("create table t2(k int, v int)")
	tk.MustExec("create table t3(id int auto_increment, k int, v int, primary key(id))")
	tk.MustExec("create table t4(k int, v int)")
	tk.MustExec("create table t5(v int, k int, primary key(k))")
	tk.MustExec("insert into t1 values (1, 1)")
	tk.MustExec("insert into t4 values (3, 3)")
	tk.MustExec("create table t6 (id int, v longtext)")
	tk.MustExec("create table t7 (x int, id int, v longtext, primary key(id))")

	// test the normal case that update one row for a single table.
	tk.MustExec("update t1 set v = 0 where k = 1")
	tk.MustQuery("select k, v from t1 where k = 1").Check(testkit.Rows("1 0"))

	// test the case that the table with auto_increment or none-null columns as the right table of left join.
	tk.MustExec("update t1 left join t3 on t1.k = t3.k set t1.v = 1")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select id, k, v from t3").Check(testkit.Rows())

	// test left join and the case that the right table has no matching record but has updated the right table columns.
	tk.MustExec("update t1 left join t2 on t1.k = t2.k set t1.v = t2.v, t2.v = 3")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test the case that the update operation in the left table references data in the right table while data of the right table columns is modified.
	tk.MustExec("update t1 left join t2 on t1.k = t2.k set t2.v = 3, t1.v = t2.v")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test right join and the case that the left table has no matching record but has updated the left table columns.
	tk.MustExec("update t2 right join t1 on t2.k = t1.k set t2.v = 4, t1.v = 0")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 0"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test the case of right join and left join at the same time.
	tk.MustExec("update t1 left join t2 on t1.k = t2.k right join t4 on t4.k = t2.k set t1.v = 4, t2.v = 4, t4.v = 4")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 0"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())
	tk.MustQuery("select k, v from t4").Check(testkit.Rows("3 4"))

	// test normal left join and the case that the right table has matching rows.
	tk.MustExec("insert t2 values (1, 10)")
	tk.MustExec("update t1 left join t2 on t1.k = t2.k set t2.v = 11")
	tk.MustQuery("select k, v from t2").Check(testkit.Rows("1 11"))

	// test the case of continuously joining the same table and updating the unmatching records.
	tk.MustExec("update t1 t11 left join t2 on t11.k = t2.k left join t1 t12 on t2.v = t12.k set t12.v = 233, t11.v = 111")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 111"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows("1 11"))

	// test the left join case that the left table has records but all records are null.
	tk.MustExec("delete from t1")
	tk.MustExec("delete from t2")
	tk.MustExec("insert into t1 values (null, null)")
	tk.MustExec("update t1 left join t2 on t1.k = t2.k set t1.v = 1")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("<nil> 1"))

	// test the case that the right table of left join has an primary key.
	tk.MustExec("insert t5 values(0, 0)")
	tk.MustExec("update t1 left join t5 on t1.k = t5.k set t1.v = 2")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("<nil> 2"))
	tk.MustQuery("select k, v from t5").Check(testkit.Rows("0 0"))

	tk.MustExec("insert into t6 values (1, NULL)")
	tk.MustExec("insert into t7 values (5, 1, 'a')")
	tk.MustExec("update t6, t7 set t6.v = t7.v where t6.id = t7.id and t7.x = 5")
	tk.MustQuery("select v from t6").Check(testkit.Rows("a"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int primary key, v int, gv int GENERATED ALWAYS AS (v * 2) STORED)")
	tk.MustExec("create table t2(id int, v int)")
	tk.MustExec("update t1 tt1 inner join (select count(t1.id) a, t1.id from t1 left join t2 on t1.id = t2.id group by t1.id) x on tt1.id = x.id set tt1.v = tt1.v + x.a")
}

func TestScanControlSelection(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx_b(b))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select (select count(1) k from t s where s.b = t1.c) from t t1").Sort().Check(testkit.Rows("0", "1", "3", "3"))
}

func TestSimpleDAG(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select a from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("select * from t where a = 4").Check(testkit.Rows("4 2 3"))
	tk.MustQuery("select a from t limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t order by a desc").Check(testkit.Rows("4", "3", "2", "1"))
	tk.MustQuery("select a from t order by a desc limit 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t order by b desc limit 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t where a < 3").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where b > 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t where b > 1 and a < 3").Check(testkit.Rows())
	tk.MustQuery("select count(*) from t where b > 1 and a < 3").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*), c from t group by c order by c").Check(testkit.Rows("2 1", "1 2", "1 3"))
	tk.MustQuery("select sum(c) as s from t group by b order by s").Check(testkit.Rows("3", "4"))
	tk.MustQuery("select avg(a) as s from t group by b order by s").Check(testkit.Rows("2.0000", "4.0000"))
	tk.MustQuery("select sum(distinct c) from t group by b").Check(testkit.Rows("3", "3"))

	tk.MustExec("create index i on t(c,b)")
	tk.MustQuery("select a from t where c = 1").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where c = 1 and a < 2").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t where c = 1 order by a limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t where c = 1 ").Check(testkit.Rows("2"))
	tk.MustExec("create index i1 on t(b)")
	tk.MustQuery("select c from t where b = 2").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where b = 2").Check(testkit.Rows("4 2 3"))
	tk.MustQuery("select count(*) from t where b = 1").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where b = 1 and a > 1 limit 1").Check(testkit.Rows("2 1 1"))

	// Test time push down.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, c1 datetime);")
	tk.MustExec("insert into t values (1, '2015-06-07 12:12:12')")
	tk.MustQuery("select id from t where c1 = '2015-06-07 12:12:12'").Check(testkit.Rows("1"))

	// Test issue 17816
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT)")
	tk.MustExec("INSERT INTO t0 VALUES (100000)")
	tk.MustQuery("SELECT * FROM t0 WHERE NOT SPACE(t0.c0)").Check(testkit.Rows("100000"))
}

func TestTimestampTimeZone(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (ts timestamp)")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t values ('2017-04-27 22:40:42')")
	// The timestamp will get different value if time_zone session variable changes.
	tests := []struct {
		timezone string
		expect   string
	}{
		{"+10:00", "2017-04-28 08:40:42"},
		{"-6:00", "2017-04-27 16:40:42"},
	}
	for _, tt := range tests {
		tk.MustExec(fmt.Sprintf("set time_zone = '%s'", tt.timezone))
		tk.MustQuery("select * from t").Check(testkit.Rows(tt.expect))
	}

	// For issue https://github.com/pingcap/tidb/issues/3467
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
 	      id bigint(20) NOT NULL AUTO_INCREMENT,
 	      uid int(11) DEFAULT NULL,
 	      datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 	      ip varchar(128) DEFAULT NULL,
 	    PRIMARY KEY (id),
 	      KEY i_datetime (datetime),
 	      KEY i_userid (uid)
 	    );`)
	tk.MustExec(`INSERT INTO t1 VALUES (123381351,1734,"2014-03-31 08:57:10","127.0.0.1");`)
	r := tk.MustQuery("select datetime from t1;") // Cover TableReaderExec
	r.Check(testkit.Rows("2014-03-31 08:57:10"))
	r = tk.MustQuery("select datetime from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("2014-03-31 08:57:10")) // Cover IndexReaderExec
	r = tk.MustQuery("select * from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("123381351 1734 2014-03-31 08:57:10 127.0.0.1")) // Cover IndexLookupExec

	// For issue https://github.com/pingcap/tidb/issues/3485
	tk.MustExec("set time_zone = 'Asia/Shanghai'")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
	    id bigint(20) NOT NULL AUTO_INCREMENT,
	    datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    PRIMARY KEY (id)
	  );`)
	tk.MustExec(`INSERT INTO t1 VALUES (123381351,"2014-03-31 08:57:10");`)
	r = tk.MustQuery(`select * from t1 where datetime="2014-03-31 08:57:10";`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	tk.MustExec(`alter table t1 add key i_datetime (datetime);`)
	r = tk.MustQuery(`select * from t1 where datetime="2014-03-31 08:57:10";`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	r = tk.MustQuery(`select * from t1;`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	r = tk.MustQuery("select datetime from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("2014-03-31 08:57:10"))
}

func TestTimestampDefaultValueTimeZone(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec(`create table t (a int, b timestamp default "2019-01-17 14:46:14")`)
	tk.MustExec("insert into t set a=1")
	r := tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2019-01-17 14:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t set a=2")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2019-01-17 06:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2019-01-17 06:46:14", "2 2019-01-17 06:46:14"))
	// Test the column's version is greater than ColumnInfoVersion1.
	is := domain.GetDomain(tk.Session()).InfoSchema()
	require.NotNil(t, is)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tb.Cols()[1].Version = model.ColumnInfoVersion1 + 1
	tk.MustExec("insert into t set a=3")
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2019-01-17 06:46:14", "2 2019-01-17 06:46:14", "3 2019-01-17 06:46:14"))
	tk.MustExec("delete from t where a=3")
	// Change time zone back.
	tk.MustExec("set time_zone = '+08:00'")
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2019-01-17 14:46:14", "2 2019-01-17 14:46:14"))
	tk.MustExec("set time_zone = '-08:00'")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2019-01-16 22:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// test zero default value in multiple time zone.
	defer tk.MustExec(fmt.Sprintf("set @@sql_mode='%s'", tk.MustQuery("select @@sql_mode").Rows()[0][0]))
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec(`create table t (a int, b timestamp default "0000-00-00 00")`)
	tk.MustExec("insert into t set a=1")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t set a=2")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '-08:00'")
	tk.MustExec("insert into t set a=3")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 0000-00-00 00:00:00", "2 0000-00-00 00:00:00", "3 0000-00-00 00:00:00"))

	// test add timestamp column default current_timestamp.
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`set time_zone = 'Asia/Shanghai'`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`insert into t set a=1`)
	tk.MustExec(`alter table t add column b timestamp not null default current_timestamp;`)
	timeIn8 := tk.MustQuery("select b from t").Rows()[0][0]
	tk.MustExec(`set time_zone = '+00:00'`)
	timeIn0 := tk.MustQuery("select b from t").Rows()[0][0]
	require.NotEqual(t, timeIn8, timeIn0)
	datumTimeIn8, err := expression.GetTimeValue(tk.Session(), timeIn8, mysql.TypeTimestamp, 0)
	require.NoError(t, err)
	tIn8To0 := datumTimeIn8.GetMysqlTime()
	timeZoneIn8, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	err = tIn8To0.ConvertTimeZone(timeZoneIn8, time.UTC)
	require.NoError(t, err)
	require.Equal(t, tIn8To0.String(), timeIn0)

	// test add index.
	tk.MustExec(`alter table t add index(b);`)
	tk.MustExec("admin check table t")
	tk.MustExec(`set time_zone = '+05:00'`)
	tk.MustExec("admin check table t")

	// 1. add a timestamp general column
	// 2. add the index
	tk.MustExec(`drop table if exists t`)
	// change timezone
	tk.MustExec(`set time_zone = 'Asia/Shanghai'`)
	tk.MustExec(`create table t(a timestamp default current_timestamp)`)
	tk.MustExec(`insert into t set a=now()`)
	tk.MustExec(`alter table t add column b timestamp as (a+1) virtual;`)
	// change timezone
	tk.MustExec(`set time_zone = '+05:00'`)
	tk.MustExec(`insert into t set a=now()`)
	tk.MustExec(`alter table t add index(b);`)
	tk.MustExec("admin check table t")
	tk.MustExec(`set time_zone = '-03:00'`)
	tk.MustExec("admin check table t")
}

func TestTiDBCurrentTS(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))
	tk.MustExec("begin")
	rows := tk.MustQuery("select @@tidb_current_ts").Rows()
	tsStr := rows[0][0].(string)
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", txn.StartTS()), tsStr)
	tk.MustExec("begin")
	rows = tk.MustQuery("select @@tidb_current_ts").Rows()
	newTsStr := rows[0][0].(string)
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", txn.StartTS()), newTsStr)
	require.NotEqual(t, tsStr, newTsStr)
	tk.MustExec("commit")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	err = tk.ExecToErr("set @@tidb_current_ts = '1'")
	require.True(t, terror.ErrorEqual(err, variable.ErrIncorrectScope), fmt.Sprintf("err: %v", err))
}

func TestTiDBLastTxnInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	tk.MustQuery("select @@tidb_last_txn_info").Check(testkit.Rows(""))

	tk.MustExec("insert into t values (1)")
	rows1 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	require.Greater(t, rows1[0][0].(string), "0")
	require.Less(t, rows1[0][0].(string), rows1[0][1].(string))

	tk.MustExec("begin")
	tk.MustQuery("select a from t where a = 1").Check(testkit.Rows("1"))
	rows2 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts'), @@tidb_current_ts").Rows()
	tk.MustExec("commit")
	rows3 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	require.Equal(t, rows1[0][0], rows2[0][0])
	require.Equal(t, rows1[0][1], rows2[0][1])
	require.Equal(t, rows1[0][0], rows3[0][0])
	require.Equal(t, rows1[0][1], rows3[0][1])
	require.Less(t, rows2[0][1], rows2[0][2])

	tk.MustExec("begin")
	tk.MustExec("update t set a = a + 1 where a = 1")
	rows4 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts'), @@tidb_current_ts").Rows()
	tk.MustExec("commit")
	rows5 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	require.Equal(t, rows1[0][0], rows4[0][0])
	require.Equal(t, rows1[0][1], rows4[0][1])
	require.Equal(t, rows5[0][0], rows4[0][2])
	require.Less(t, rows4[0][1], rows4[0][2])
	require.Less(t, rows4[0][2], rows5[0][1])

	tk.MustExec("begin")
	tk.MustExec("update t set a = a + 1 where a = 2")
	tk.MustExec("rollback")
	rows6 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	require.Equal(t, rows5[0][0], rows6[0][0])
	require.Equal(t, rows5[0][1], rows6[0][1])

	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (2)")
	err := tk.ExecToErr("commit")
	require.Error(t, err)
	rows7 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts'), json_extract(@@tidb_last_txn_info, '$.error')").Rows()
	require.Greater(t, rows7[0][0], rows5[0][0])
	require.Equal(t, "0", rows7[0][1])
	require.Contains(t, err.Error(), rows7[0][1])

	err = tk.ExecToErr("set @@tidb_last_txn_info = '{}'")
	require.True(t, terror.ErrorEqual(err, variable.ErrIncorrectScope), fmt.Sprintf("err: %v", err))
}

func TestTiDBLastQueryInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, v int)")
	tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.start_ts')").Check(testkit.Rows("0 0"))

	toUint64 := func(str interface{}) uint64 {
		res, err := strconv.ParseUint(str.(string), 10, 64)
		require.NoError(t, err)
		return res
	}

	tk.MustExec("select * from t")
	rows := tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	require.Greater(t, toUint64(rows[0][0]), uint64(0))
	require.Equal(t, rows[0][1], rows[0][0])

	tk.MustExec("insert into t values (1, 10)")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	require.Greater(t, toUint64(rows[0][0]), uint64(0))
	require.Equal(t, rows[0][1], rows[0][0])
	// tidb_last_txn_info is still valid after checking query info.
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	require.Greater(t, toUint64(rows[0][0]), uint64(0))
	require.Less(t, rows[0][0].(string), rows[0][1].(string))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	require.Greater(t, toUint64(rows[0][0]), uint64(0))
	require.Equal(t, rows[0][1], rows[0][0])

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("update t set v = 11 where a = 1")

	tk.MustExec("select * from t")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	require.Greater(t, toUint64(rows[0][0]), uint64(0))
	require.Equal(t, rows[0][1], rows[0][0])

	tk.MustExec("update t set v = 12 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	require.Greater(t, toUint64(rows[0][0]), uint64(0))
	require.Less(t, toUint64(rows[0][0]), toUint64(rows[0][1]))

	tk.MustExec("commit")

	tk.MustExec("set transaction isolation level read committed")
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	require.Greater(t, toUint64(rows[0][0]), uint64(0))
	require.Less(t, toUint64(rows[0][0]), toUint64(rows[0][1]))

	tk.MustExec("rollback")
}

func TestSelectForUpdate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t, t1")

	txn, err := tk.Session().Txn(true)
	require.True(t, kv.ErrInvalidTxn.Equal(err))
	require.False(t, txn.Valid())
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert t values (11, 2, 3)")
	tk.MustExec("insert t values (12, 2, 3)")
	tk.MustExec("insert t values (13, 2, 3)")

	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("insert t1 values (11)")

	// conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=11")
	tk2.MustExec("commit")

	err = tk1.ExecToErr("commit")
	require.Error(t, err)

	// no conflict for subquery.
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where exists(select null from t1 where t1.c1=t.c1) for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// not conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=22 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// not conflict, auto commit
	tk1.MustExec("set @@autocommit=1;")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=11")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from (select * from t for update) t join t1 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t1 set c1 = 13")
	tk2.MustExec("commit")

	err = tk1.ExecToErr("commit")
	require.Error(t, err)

}

func TestSelectForUpdateOf(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (i int)")
	tk.MustExec("create table t1 (i int)")
	tk.MustExec("insert t values (1)")
	tk.MustExec("insert t1 values (1)")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t, t1 where t.i = t1.i for update of t").Check(testkit.Rows("1 1"))

	tk1.MustExec("begin pessimistic")

	// no lock for t
	tk1.MustQuery("select * from t1 for update").Check(testkit.Rows("1"))

	// meet lock for t1
	err := tk1.ExecToErr("select * from t for update nowait")
	require.True(t, terror.ErrorEqual(err, error2.ErrLockAcquireFailAndNoWaitSet), fmt.Sprintf("err: %v", err))

	// t1 rolled back, tk1 acquire the lock
	tk.MustExec("rollback")
	tk1.MustQuery("select * from t for update nowait").Check(testkit.Rows("1"))

	tk1.MustExec("rollback")
}

func TestEmptyEnum(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (e enum('Y', 'N'))")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'")
	err := tk.ExecToErr("insert into t values (0)")
	require.True(t, terror.ErrorEqual(err, types.ErrTruncated), fmt.Sprintf("err: %v", err))
	err = tk.ExecToErr("insert into t values ('abc')")
	require.True(t, terror.ErrorEqual(err, types.ErrTruncated), fmt.Sprintf("err: %v", err))

	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert into t values (0)")
	tk.MustQuery("select * from t").Check(testkit.Rows(""))
	tk.MustExec("insert into t values ('abc')")
	tk.MustQuery("select * from t").Check(testkit.Rows("", ""))
	tk.MustExec("insert into t values (null)")
	tk.MustQuery("select * from t").Check(testkit.Rows("", "", "<nil>"))

	// Test https://github.com/pingcap/tidb/issues/29525.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int auto_increment primary key, c1 enum('a', '', 'c'));")
	tk.MustExec("insert into t(c1) values (0);")
	tk.MustQuery("select id, c1+0, c1 from t;").Check(testkit.Rows("1 0 "))
	tk.MustExec("alter table t change c1 c1 enum('a', '') not null;")
	tk.MustQuery("select id, c1+0, c1 from t;").Check(testkit.Rows("1 0 "))
	tk.MustExec("insert into t(c1) values (0);")
	tk.MustQuery("select id, c1+0, c1 from t;").Check(testkit.Rows("1 0 ", "2 0 "))
}

func TestPartitionHashCode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(c1 bigint, c2 bigint, c3 bigint, primary key(c1)) partition by hash (c1) partitions 4;`)
	var wg util.WaitGroupWrapper
	for i := 0; i < 5; i++ {
		wg.Run(func() {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			for i := 0; i < 5; i++ {
				tk1.MustExec("select * from t")
			}
		})
	}
	wg.Wait()
}

func TestAlterDefaultValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, primary key(a))")
	tk.MustExec("insert into t(a) values(1)")
	tk.MustExec("alter table t add column b int default 1")
	tk.MustExec("alter table t alter b set default 2")
	tk.MustQuery("select b from t where a = 1").Check(testkit.Rows("1"))
}

// this is from jira issue #5856
func TestInsertValuesWithSubQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t2")

	// should not reference upper scope
	require.Error(t, tk.ExecToErr("insert into t2 values (11, 8, (select not b))"))
	require.Error(t, tk.ExecToErr("insert into t2 set a = 11, b = 8, c = (select b))"))

	// subquery reference target table is allowed
	tk.MustExec("insert into t2 values(1, 1, (select b from t2))")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("insert into t2 set a = 1, b = 1, c = (select b+1 from t2)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1 <nil>", "1 1 2"))

	// insert using column should work normally
	tk.MustExec("delete from t2")
	tk.MustExec("insert into t2 values(2, 4, a)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 4 2"))
	tk.MustExec("insert into t2 set a = 3, b = 5, c = b")
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 4 2", "3 5 5"))

	// issue #30626
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	// TODO: should insert success and get (81,1) from the table
	tk.MustGetErrMsg(
		"insert into t values ( 81, ( select ( SELECT '1' AS `c0` WHERE '1' >= `subq_0`.`c0` ) as `c1` FROM ( SELECT '1' AS `c0` ) AS `subq_0` ) );",
		"Insert's SET operation or VALUES_LIST doesn't support complex subqueries now")
	tk.MustGetErrMsg(
		"insert into t set a = 81, b =  (select ( SELECT '1' AS `c0` WHERE '1' >= `subq_0`.`c0` ) as `c1` FROM ( SELECT '1' AS `c0` ) AS `subq_0` );",
		"Insert's SET operation or VALUES_LIST doesn't support complex subqueries now")

}

func TestDIVZeroInPartitionExpr(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(a int) partition by range (10 div a) (partition p0 values less than (10), partition p1 values less than maxvalue)")

	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert into t1 values (NULL), (0), (1)")
	tk.MustExec("set @@sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'")
	tk.MustGetErrCode("insert into t1 values (NULL), (0), (1)", mysql.ErrDivisionByZero)
}

func TestInsertIntoGivenPartitionSet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`create table t1(
	a int(11) DEFAULT NULL,
	b varchar(10) DEFAULT NULL,
	UNIQUE KEY idx_a (a)) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)

	// insert into
	tk.MustExec("insert into t1 partition(p0) values(1, 'a'), (2, 'b')")
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b"))
	tk.MustExec("insert into t1 partition(p0, p1) values(3, 'c'), (4, 'd')")
	tk.MustQuery("select * from t1 partition(p1)").Check(testkit.Rows())

	tk.MustGetErrMsg("insert into t1 values(1, 'a')", "[kv:1062]Duplicate entry '1' for key 'idx_a'")
	tk.MustGetErrMsg("insert into t1 partition(p0, p_non_exist) values(1, 'a')", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	tk.MustGetErrMsg("insert into t1 partition(p0, p1) values(40, 'a')", "[table:1748]Found a row not matching the given partition set")

	// replace into
	tk.MustExec("replace into t1 partition(p0) values(1, 'replace')")
	tk.MustExec("replace into t1 partition(p0, p1) values(3, 'replace'), (4, 'replace')")
	tk.MustExec("replace into t1 values(1, 'a')")
	tk.MustQuery("select * from t1 partition (p0) order by a").Check(testkit.Rows("1 a", "2 b", "3 replace", "4 replace"))

	tk.MustGetErrMsg("replace into t1 partition(p0, p_non_exist) values(1, 'a')", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	tk.MustGetErrMsg("replace into t1 partition(p0, p1) values(40, 'a')", "[table:1748]Found a row not matching the given partition set")

	tk.MustExec("truncate table t1")

	tk.MustExec("create table t(a int, b char(10))")

	// insert into general table
	tk.MustGetErrMsg("insert into t partition(p0, p1) values(1, 'a')", "[planner:1747]PARTITION () clause on non partitioned table")

	// insert into from select
	tk.MustExec("insert into t values(1, 'a'), (2, 'b')")
	tk.MustExec("insert into t1 partition(p0) select * from t")
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b"))

	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(3, 'c'), (4, 'd')")
	tk.MustExec("insert into t1 partition(p0, p1) select * from t")
	tk.MustQuery("select * from t1 partition(p1) order by a").Check(testkit.Rows())
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b", "3 c", "4 d"))

	tk.MustGetErrMsg("insert into t1 select 1, 'a'", "[kv:1062]Duplicate entry '1' for key 'idx_a'")
	tk.MustGetErrMsg("insert into t1 partition(p0, p_non_exist) select 1, 'a'", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	tk.MustGetErrMsg("insert into t1 partition(p0, p1) select 40, 'a'", "[table:1748]Found a row not matching the given partition set")

	// replace into from select
	tk.MustExec("replace into t1 partition(p0) select 1, 'replace'")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(3, 'replace'), (4, 'replace')")
	tk.MustExec("replace into t1 partition(p0, p1) select * from t")

	tk.MustExec("replace into t1 select 1, 'a'")
	tk.MustQuery("select * from t1 partition (p0) order by a").Check(testkit.Rows("1 a", "2 b", "3 replace", "4 replace"))
	tk.MustGetErrMsg("replace into t1 partition(p0, p_non_exist) select 1, 'a'", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	tk.MustGetErrMsg("replace into t1 partition(p0, p1) select 40, 'a'", "[table:1748]Found a row not matching the given partition set")
}

// fix issue https://github.com/pingcap/tidb/issues/32871
func TestBitColumnIn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bit(16), key id(id))")
	tk.MustExec("insert into t values (65)")
	tk.MustQuery("select * from t where id not in (-1,2)").Check(testkit.Rows("\x00A"))
	tk.MustGetErrMsg(
		"select * from t where id in (-1, -2)",
		"[expression:1582]Incorrect parameter count in the call to native function 'in'")
}

func TestUpdateGivenPartitionSet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`create table t1(
	a int(11),
	b varchar(10) DEFAULT NULL,
	primary key idx_a (a)) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)

	tk.MustExec(`create table t2(
	a int(11) DEFAULT NULL,
	b varchar(10) DEFAULT NULL) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)

	tk.MustExec(`create table t3 (a int(11), b varchar(10) default null)`)
	tk.MustExec("insert into t3 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	tk.MustGetErrMsg(
		"update t3 partition(p0) set a = 40 where a = 2",
		"[planner:1747]PARTITION () clause on non partitioned table")

	// update with primary key change
	tk.MustExec("insert into t1 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	tk.MustGetErrMsg("update t1 partition(p0, p1) set a = 40", "[table:1748]Found a row not matching the given partition set")
	tk.MustGetErrMsg("update t1 partition(p0) set a = 40 where a = 2", "[table:1748]Found a row not matching the given partition set")
	// test non-exist partition.
	tk.MustGetErrMsg("update t1 partition (p0, p_non_exist) set a = 40", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	// test join.
	tk.MustGetErrMsg("update t1 partition (p0), t3 set t1.a = 40 where t3.a = 2", "[table:1748]Found a row not matching the given partition set")

	tk.MustExec("update t1 partition(p0) set a = 3 where a = 2")
	tk.MustExec("update t1 partition(p0, p3) set a = 33 where a = 1")

	// update without partition change
	tk.MustExec("insert into t2 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	tk.MustGetErrMsg("update t2 partition(p0, p1) set a = 40", "[table:1748]Found a row not matching the given partition set")
	tk.MustGetErrMsg("update t2 partition(p0) set a = 40 where a = 2", "[table:1748]Found a row not matching the given partition set")

	tk.MustExec("update t2 partition(p0) set a = 3 where a = 2")
	tk.MustExec("update t2 partition(p0, p3) set a = 33 where a = 1")

	tk.MustExec("create table t4(a int primary key, b int) partition by hash(a) partitions 2")
	tk.MustExec("insert into t4(a, b) values(1, 1),(2, 2),(3, 3);")
	tk.MustGetErrMsg("update t4 partition(p0) set a = 5 where a = 2", "[table:1748]Found a row not matching the given partition set")
}
