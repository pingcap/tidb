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
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/statistics"
	error2 "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
)

func checkFileName(s string) bool {
	files := []string{
		"config.toml",
		"meta.txt",
		"stats/test.t_dump_single.json",
		"schema/test.t_dump_single.schema.txt",
		"variables.toml",
		"sqls.sql",
		"session_bindings.sql",
		"global_bindings.sql",
		"explain.txt",
	}
	for _, f := range files {
		if strings.Compare(f, s) == 0 {
			return true
		}
	}
	return false
}

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
	require.NoError(t, sessiontxn.NewTxn(context.Background(), ctx))
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
	tk.MustExec(`insert into t set a="20220413154712"`)
	tk.MustExec(`alter table t add column b timestamp as (a+1) virtual;`)
	// change timezone
	tk.MustExec(`set time_zone = '+05:00'`)
	tk.MustExec(`insert into t set a="20220413154840"`)
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

func TestIndexLookupRuntimeStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index(a))")
	tk.MustExec("insert into t1 values (1,2),(2,3),(3,4)")
	rows := tk.MustQuery("explain analyze select * from t1 use index(a) where a > 1").Rows()
	require.Len(t, rows, 3)
	explain := fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*time:.*loops:.*index_task:.*table_task: {total_time.*num.*concurrency.*}.*", explain)
	indexExplain := fmt.Sprintf("%v", rows[1])
	tableExplain := fmt.Sprintf("%v", rows[2])
	require.Regexp(t, ".*time:.*loops:.*cop_task:.*", indexExplain)
	require.Regexp(t, ".*time:.*loops:.*cop_task:.*", tableExplain)
}

func TestHashAggRuntimeStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("insert into t1 values (1,2),(2,3),(3,4)")
	rows := tk.MustQuery("explain analyze SELECT /*+ HASH_AGG() */ count(*) FROM t1 WHERE a < 10;").Rows()
	require.Len(t, rows, 5)
	explain := fmt.Sprintf("%v", rows[0])
	pattern := ".*time:.*loops:.*partial_worker:{wall_time:.*concurrency:.*task_num:.*tot_wait:.*tot_exec:.*tot_time:.*max:.*p95:.*}.*final_worker:{wall_time:.*concurrency:.*task_num:.*tot_wait:.*tot_exec:.*tot_time:.*max:.*p95:.*}.*"
	require.Regexp(t, pattern, explain)
}

func TestIndexMergeRuntimeStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_index_merge = 1")
	tk.MustExec("create table t1(id int primary key, a int, b int, c int, d int)")
	tk.MustExec("create index t1a on t1(a)")
	tk.MustExec("create index t1b on t1(b)")
	tk.MustExec("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	rows := tk.MustQuery("explain analyze select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4;").Rows()
	require.Len(t, rows, 4)
	explain := fmt.Sprintf("%v", rows[0])
	pattern := ".*time:.*loops:.*index_task:{fetch_handle:.*, merge:.*}.*table_task:{num.*concurrency.*fetch_row.*wait_time.*}.*"
	require.Regexp(t, pattern, explain)
	tableRangeExplain := fmt.Sprintf("%v", rows[1])
	indexExplain := fmt.Sprintf("%v", rows[2])
	tableExplain := fmt.Sprintf("%v", rows[3])
	require.Regexp(t, ".*time:.*loops:.*cop_task:.*", tableRangeExplain)
	require.Regexp(t, ".*time:.*loops:.*cop_task:.*", indexExplain)
	require.Regexp(t, ".*time:.*loops:.*cop_task:.*", tableExplain)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4 order by a").Check(testkit.Rows("1 1 1 1 1", "5 5 5 5 5"))
}

// For issue 17256
func TestGenerateColumnReplace(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1 (a int, b int as (a + 1) virtual not null, unique index idx(b));")
	tk.MustExec("REPLACE INTO `t1` (`a`) VALUES (2);")
	tk.MustExec("REPLACE INTO `t1` (`a`) VALUES (2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2 3"))
	tk.MustExec("insert into `t1` (`a`) VALUES (2) on duplicate key update a = 3;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("3 4"))
}

func TestPrevStmtDesensitization(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(fmt.Sprintf("set @@session.%v=1", variable.TiDBRedactLog))
	defer tk.MustExec(fmt.Sprintf("set @@session.%v=0", variable.TiDBRedactLog))
	tk.MustExec("create table t (a int, unique key (a))")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1),(2)")
	require.Equal(t, "insert into `t` values ( ? ) , ( ? )", tk.Session().GetSessionVars().PrevStmt.String())
	tk.MustGetErrMsg("insert into t values (1)", `[kv:1062]Duplicate entry '?' for key 'a'`)
}

func TestIssue19372(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1 (c_int int, c_str varchar(40), key(c_str));")
	tk.MustExec("create table t2 like t1;")
	tk.MustExec("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c');")
	tk.MustExec("insert into t2 select * from t1;")
	tk.MustQuery("select (select t2.c_str from t2 where t2.c_str <= t1.c_str and t2.c_int in (1, 2) order by t2.c_str limit 1) x from t1 order by c_int;").Check(testkit.Rows("a", "a", "a"))
}

func TestIssue19148(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(16, 2));")
	tk.MustExec("select * from t where a > any_value(a);")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Zero(t, tblInfo.Meta().Columns[0].GetFlag())
}

func TestIssue19667(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a DATETIME)")
	tk.MustExec("INSERT INTO t VALUES('1988-04-17 01:59:59')")
	tk.MustQuery(`SELECT DATE_ADD(a, INTERVAL 1 SECOND) FROM t`).Check(testkit.Rows("1988-04-17 02:00:00"))
}

func TestZeroDateTimeCompatibility(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	sqls := []string{
		`select YEAR(0000-00-00), YEAR("0000-00-00")`,
		`select MONTH(0000-00-00), MONTH("0000-00-00")`,
		`select DAYOFMONTH(0000-00-00), DAYOFMONTH("0000-00-00")`,
		`select QUARTER(0000-00-00), QUARTER("0000-00-00")`,
		`select EXTRACT(DAY FROM 0000-00-00), EXTRACT(DAY FROM "0000-00-00")`,
		`select EXTRACT(MONTH FROM 0000-00-00), EXTRACT(MONTH FROM "0000-00-00")`,
		`select EXTRACT(YEAR FROM 0000-00-00), EXTRACT(YEAR FROM "0000-00-00")`,
		`select EXTRACT(WEEK FROM 0000-00-00), EXTRACT(WEEK FROM "0000-00-00")`,
		`select EXTRACT(QUARTER FROM 0000-00-00), EXTRACT(QUARTER FROM "0000-00-00")`,
	}
	for _, sql := range sqls {
		tk.MustQuery(sql).Check(testkit.Rows("0 <nil>"))
		require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	}

	sqls = []string{
		`select DAYOFWEEK(0000-00-00), DAYOFWEEK("0000-00-00")`,
		`select DAYOFYEAR(0000-00-00), DAYOFYEAR("0000-00-00")`,
	}
	for _, sql := range sqls {
		tk.MustQuery(sql).Check(testkit.Rows("<nil> <nil>"))
		require.Equal(t, uint16(2), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	}

	tk.MustExec("use test")
	tk.MustExec("create table t(v1 datetime, v2 datetime(3))")
	tk.MustExec("insert ignore into t values(0,0)")

	sqls = []string{
		`select YEAR(v1), YEAR(v2) from t`,
		`select MONTH(v1), MONTH(v2) from t`,
		`select DAYOFMONTH(v1), DAYOFMONTH(v2) from t`,
		`select QUARTER(v1), QUARTER(v2) from t`,
		`select EXTRACT(DAY FROM v1), EXTRACT(DAY FROM v2) from t`,
		`select EXTRACT(MONTH FROM v1), EXTRACT(MONTH FROM v2) from t`,
		`select EXTRACT(YEAR FROM v1), EXTRACT(YEAR FROM v2) from t`,
		`select EXTRACT(WEEK FROM v1), EXTRACT(WEEK FROM v2) from t`,
		`select EXTRACT(QUARTER FROM v1), EXTRACT(QUARTER FROM v2) from t`,
	}
	for _, sql := range sqls {
		tk.MustQuery(sql).Check(testkit.Rows("0 0"))
		require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	}

	sqls = []string{
		`select DAYOFWEEK(v1), DAYOFWEEK(v2) from t`,
		`select DAYOFYEAR(v1), DAYOFYEAR(v2) from t`,
	}
	for _, sql := range sqls {
		tk.MustQuery(sql).Check(testkit.Rows("<nil> <nil>"))
		require.Equal(t, uint16(2), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	}
}

// https://github.com/pingcap/tidb/issues/24165.
func TestInvalidDateValueInCreateTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")

	// Test for sql mode 'NO_ZERO_IN_DATE'.
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE';")
	tk.MustGetErrCode("create table t (a datetime default '2999-00-00 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("create table t (a datetime);")
	tk.MustGetErrCode("alter table t modify column a datetime default '2999-00-00 00:00:00';", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")

	// Test for sql mode 'NO_ZERO_DATE'.
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE';")
	tk.MustGetErrCode("create table t (a datetime default '0000-00-00 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("create table t (a datetime);")
	tk.MustGetErrCode("alter table t modify column a datetime default '0000-00-00 00:00:00';", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")

	// Remove NO_ZERO_DATE and NO_ZERO_IN_DATE.
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES';")
	// Test create table with zero datetime as a default value.
	tk.MustExec("create table t (a datetime default '2999-00-00 00:00:00');")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime default '0000-00-00 00:00:00');")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime);")
	tk.MustExec("alter table t modify column a datetime default '2999-00-00 00:00:00';")
	tk.MustExec("alter table t modify column a datetime default '0000-00-00 00:00:00';")
	tk.MustExec("drop table if exists t;")

	// Test create table with invalid datetime(02-30) as a default value.
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES';")
	tk.MustGetErrCode("create table t (a datetime default '2999-02-30 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")
	// NO_ZERO_IN_DATE and NO_ZERO_DATE have nothing to do with invalid datetime(02-30).
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE';")
	tk.MustGetErrCode("create table t (a datetime default '2999-02-30 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")
	// ALLOW_INVALID_DATES allows invalid datetime(02-30).
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,ALLOW_INVALID_DATES';")
	tk.MustExec("create table t (a datetime default '2999-02-30 00:00:00');")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime);")
	tk.MustExec("alter table t modify column a datetime default '2999-02-30 00:00:00';")
	tk.MustExec("drop table if exists t;")
}

func TestOOMActionPriority(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("drop table if exists t4")
	tk.MustExec("create table t0(a int)")
	tk.MustExec("insert into t0 values(1)")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("insert into t2 values(1)")
	tk.MustExec("create table t3(a int)")
	tk.MustExec("insert into t3 values(1)")
	tk.MustExec("create table t4(a int)")
	tk.MustExec("insert into t4 values(1)")
	tk.MustQuery("select * from t0 join t1 join t2 join t3 join t4 order by t0.a").Check(testkit.Rows("1 1 1 1 1"))
	action := tk.Session().GetSessionVars().StmtCtx.MemTracker.GetFallbackForTest(true)
	// All actions are finished and removed.
	require.Equal(t, action.GetPriority(), int64(memory.DefLogPriority))
}

func TestTrackAggMemoryUsage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("set tidb_track_aggregate_memory_usage = off;")
	rows := tk.MustQuery("explain analyze select /*+ HASH_AGG() */ sum(a) from t").Rows()
	require.Equal(t, "N/A", rows[0][7])
	rows = tk.MustQuery("explain analyze select /*+ STREAM_AGG() */ sum(a) from t").Rows()
	require.Equal(t, "N/A", rows[0][7])
	tk.MustExec("set tidb_track_aggregate_memory_usage = on;")
	rows = tk.MustQuery("explain analyze select /*+ HASH_AGG() */ sum(a) from t").Rows()
	require.NotEqual(t, "N/A", rows[0][7])
	rows = tk.MustQuery("explain analyze select /*+ STREAM_AGG() */ sum(a) from t").Rows()
	require.NotEqual(t, "N/A", rows[0][7])
}

func TestProjectionBitType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(k1 int, v bit(34) DEFAULT b'111010101111001001100111101111111', primary key(k1) clustered);")
	tk.MustExec("create table t1(k1 int, v bit(34) DEFAULT b'111010101111001001100111101111111', primary key(k1) nonclustered);")
	tk.MustExec("insert into t(k1) select 1;")
	tk.MustExec("insert into t1(k1) select 1;")

	tk.MustExec("set @@tidb_enable_vectorized_expression = 0;")
	// following SQL should returns same result
	tk.MustQuery("(select * from t where false) union(select * from t for update);").Check(testkit.Rows("1 \x01\xd5\xe4\xcf\u007f"))
	tk.MustQuery("(select * from t1 where false) union(select * from t1 for update);").Check(testkit.Rows("1 \x01\xd5\xe4\xcf\u007f"))

	tk.MustExec("set @@tidb_enable_vectorized_expression = 1;")
	tk.MustQuery("(select * from t where false) union(select * from t for update);").Check(testkit.Rows("1 \x01\xd5\xe4\xcf\u007f"))
	tk.MustQuery("(select * from t1 where false) union(select * from t1 for update);").Check(testkit.Rows("1 \x01\xd5\xe4\xcf\u007f"))
}

func TestExprBlackListForEnum(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a enum('a','b','c'), b enum('a','b','c'), c int, index idx(b,a));")
	tk.MustExec("insert into t values(1,1,1),(2,2,2),(3,3,3);")

	checkFuncPushDown := func(rows [][]interface{}, keyWord string) bool {
		for _, line := range rows {
			// Agg/Expr push down
			if line[2].(string) == "cop[tikv]" && strings.Contains(line[4].(string), keyWord) {
				return true
			}
			// access index
			if line[2].(string) == "cop[tikv]" && strings.Contains(line[3].(string), keyWord) {
				return true
			}
		}
		return false
	}

	// Test agg(enum) push down
	tk.MustExec("insert into mysql.expr_pushdown_blacklist(name) values('enum');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows := tk.MustQuery("desc format='brief' select /*+ HASH_AGG() */ max(a) from t;").Rows()
	require.False(t, checkFuncPushDown(rows, "max"))
	rows = tk.MustQuery("desc format='brief' select /*+ STREAM_AGG() */ max(a) from t;").Rows()
	require.False(t, checkFuncPushDown(rows, "max"))

	tk.MustExec("delete from mysql.expr_pushdown_blacklist;")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select /*+ HASH_AGG() */ max(a) from t;").Rows()
	require.True(t, checkFuncPushDown(rows, "max"))
	rows = tk.MustQuery("desc format='brief' select /*+ STREAM_AGG() */ max(a) from t;").Rows()
	require.True(t, checkFuncPushDown(rows, "max"))

	// Test expr(enum) push down
	tk.MustExec("insert into mysql.expr_pushdown_blacklist(name) values('enum');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select * from t where a + b;").Rows()
	require.False(t, checkFuncPushDown(rows, "plus"))
	rows = tk.MustQuery("desc format='brief' select * from t where a + b;").Rows()
	require.False(t, checkFuncPushDown(rows, "plus"))

	tk.MustExec("delete from mysql.expr_pushdown_blacklist;")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select * from t where a + b;").Rows()
	require.True(t, checkFuncPushDown(rows, "plus"))
	rows = tk.MustQuery("desc format='brief' select * from t where a + b;").Rows()
	require.True(t, checkFuncPushDown(rows, "plus"))

	// Test enum index
	tk.MustExec("insert into mysql.expr_pushdown_blacklist(name) values('enum');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select * from t where b = 1;").Rows()
	require.False(t, checkFuncPushDown(rows, "index:idx(b)"))
	rows = tk.MustQuery("desc format='brief' select * from t where b = 'a';").Rows()
	require.False(t, checkFuncPushDown(rows, "index:idx(b)"))
	rows = tk.MustQuery("desc format='brief' select * from t where b > 1;").Rows()
	require.False(t, checkFuncPushDown(rows, "index:idx(b)"))
	rows = tk.MustQuery("desc format='brief' select * from t where b > 'a';").Rows()
	require.False(t, checkFuncPushDown(rows, "index:idx(b)"))

	tk.MustExec("delete from mysql.expr_pushdown_blacklist;")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select * from t where b = 1 and a = 1;").Rows()
	require.True(t, checkFuncPushDown(rows, "index:idx(b, a)"))
	rows = tk.MustQuery("desc format='brief' select * from t where b = 'a' and a = 'a';").Rows()
	require.True(t, checkFuncPushDown(rows, "index:idx(b, a)"))
	rows = tk.MustQuery("desc format='brief' select * from t where b = 1 and a > 1;").Rows()
	require.True(t, checkFuncPushDown(rows, "index:idx(b, a)"))
	rows = tk.MustQuery("desc format='brief' select * from t where b = 1 and a > 'a'").Rows()
	require.True(t, checkFuncPushDown(rows, "index:idx(b, a)"))
}

// Test invoke Close without invoking Open before for each operators.
func TestUnreasonablyClose(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable(), plannercore.MockUnsignedTable()})
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// To enable the shuffleExec operator.
	tk.MustExec("set @@tidb_merge_join_concurrency=4")

	var opsNeedsCovered = []plannercore.PhysicalPlan{
		&plannercore.PhysicalHashJoin{},
		&plannercore.PhysicalMergeJoin{},
		&plannercore.PhysicalIndexJoin{},
		&plannercore.PhysicalIndexHashJoin{},
		&plannercore.PhysicalTableReader{},
		&plannercore.PhysicalIndexReader{},
		&plannercore.PhysicalIndexLookUpReader{},
		&plannercore.PhysicalIndexMergeReader{},
		&plannercore.PhysicalApply{},
		&plannercore.PhysicalHashAgg{},
		&plannercore.PhysicalStreamAgg{},
		&plannercore.PhysicalLimit{},
		&plannercore.PhysicalSort{},
		&plannercore.PhysicalTopN{},
		&plannercore.PhysicalCTE{},
		&plannercore.PhysicalCTETable{},
		&plannercore.PhysicalMaxOneRow{},
		&plannercore.PhysicalProjection{},
		&plannercore.PhysicalSelection{},
		&plannercore.PhysicalTableDual{},
		&plannercore.PhysicalWindow{},
		&plannercore.PhysicalShuffle{},
		&plannercore.PhysicalUnionAll{},
	}

	opsNeedsCoveredMask := uint64(1<<len(opsNeedsCovered) - 1)
	opsAlreadyCoveredMask := uint64(0)
	p := parser.New()
	for i, tc := range []string{
		"select /*+ hash_join(t1)*/ * from t t1 join t t2 on t1.a = t2.a",
		"select /*+ merge_join(t1)*/ * from t t1 join t t2 on t1.f = t2.f",
		"select t.f from t use index(f)",
		"select /*+ inl_join(t1) */ * from t t1 join t t2 on t1.f=t2.f",
		"select /*+ inl_hash_join(t1) */ * from t t1 join t t2 on t1.f=t2.f",
		"SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t",
		"select /*+ hash_agg() */ count(f) from t group by a",
		"select /*+ stream_agg() */ count(f) from t group by a",
		"select * from t order by a, f",
		"select * from t order by a, f limit 1",
		"select * from t limit 1",
		"select (select t1.a from t t1 where t1.a > t2.a) as a from t t2;",
		"select a + 1 from t",
		"select count(*) a from t having a > 1",
		"select * from t where a = 1.1",
		"with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 0) select * from cte1",
		"select /*+use_index_merge(t, c_d_e, f)*/ * from t where c < 1 or f > 2",
		"select sum(f) over (partition by f) from t",
		"select /*+ merge_join(t1)*/ * from t t1 join t t2 on t1.d = t2.d",
		"select a from t union all select a from t",
	} {
		comment := fmt.Sprintf("case:%v sql:%s", i, tc)
		stmt, err := p.ParseOneStmt(tc, "", "")
		require.NoError(t, err, comment)
		err = sessiontxn.NewTxn(context.Background(), tk.Session())
		require.NoError(t, err, comment)

		err = sessiontxn.GetTxnManager(tk.Session()).OnStmtStart(context.TODO())
		require.NoError(t, err, comment)

		executorBuilder := executor.NewMockExecutorBuilderForTest(tk.Session(), is, nil, oracle.GlobalTxnScope)

		p, _, _ := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NotNil(t, p)

		// This for loop level traverses the plan tree to get which operators are covered.
		for child := []plannercore.PhysicalPlan{p.(plannercore.PhysicalPlan)}; len(child) != 0; {
			newChild := make([]plannercore.PhysicalPlan, 0, len(child))
			for _, ch := range child {
				found := false
				for k, t := range opsNeedsCovered {
					if reflect.TypeOf(t) == reflect.TypeOf(ch) {
						opsAlreadyCoveredMask |= 1 << k
						found = true
						break
					}
				}
				require.True(t, found, fmt.Sprintf("case: %v sql: %s operator %v is not registered in opsNeedsCoveredMask", i, tc, reflect.TypeOf(ch)))
				switch x := ch.(type) {
				case *plannercore.PhysicalCTE:
					newChild = append(newChild, x.RecurPlan)
					newChild = append(newChild, x.SeedPlan)
					continue
				case *plannercore.PhysicalShuffle:
					newChild = append(newChild, x.DataSources...)
					newChild = append(newChild, x.Tails...)
					continue
				}
				newChild = append(newChild, ch.Children()...)
			}
			child = newChild
		}

		e := executorBuilder.Build(p)

		func() {
			defer func() {
				r := recover()
				buf := make([]byte, 4096)
				stackSize := runtime.Stack(buf, false)
				buf = buf[:stackSize]
				require.Nil(t, r, fmt.Sprintf("case: %v\n sql: %s\n error stack: %v", i, tc, string(buf)))
			}()
			require.NoError(t, e.Close(), comment)
		}()
	}
	// The following code is used to make sure all the operators registered
	// in opsNeedsCoveredMask are covered.
	commentBuf := strings.Builder{}
	if opsAlreadyCoveredMask != opsNeedsCoveredMask {
		for i := range opsNeedsCovered {
			if opsAlreadyCoveredMask&(1<<i) != 1<<i {
				commentBuf.WriteString(fmt.Sprintf(" %v", reflect.TypeOf(opsNeedsCovered[i])))
			}
		}
	}
	require.Equal(t, opsNeedsCoveredMask, opsAlreadyCoveredMask, fmt.Sprintf("these operators are not covered %s", commentBuf.String()))
}

func TestEncodingSet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `enum-set` (`set` SET(" +
		"'x00','x01','x02','x03','x04','x05','x06','x07','x08','x09','x10','x11','x12','x13','x14','x15'," +
		"'x16','x17','x18','x19','x20','x21','x22','x23','x24','x25','x26','x27','x28','x29','x30','x31'," +
		"'x32','x33','x34','x35','x36','x37','x38','x39','x40','x41','x42','x43','x44','x45','x46','x47'," +
		"'x48','x49','x50','x51','x52','x53','x54','x55','x56','x57','x58','x59','x60','x61','x62','x63'" +
		")NOT NULL PRIMARY KEY)")
	tk.MustExec("INSERT INTO `enum-set` VALUES\n(\"x00,x59\");")
	tk.MustQuery("select `set` from `enum-set` use index(PRIMARY)").Check(testkit.Rows("x00,x59"))
	tk.MustExec("admin check table `enum-set`")
}

func TestDeleteWithMulTbl(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	// Delete multiple tables from left joined table.
	// The result of left join is (3, null, null).
	// Because rows in t2 are not matched, so no row will be deleted in t2.
	// But row in t1 is matched, so it should be deleted.
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (c1 int);")
	tk.MustExec("create table t2 (c1 int primary key, c2 int);")
	tk.MustExec("insert into t1 values(3);")
	tk.MustExec("insert into t2 values(2, 2);")
	tk.MustExec("insert into t2 values(0, 0);")
	tk.MustExec("delete from t1, t2 using t1 left join t2 on t1.c1 = t2.c2;")
	tk.MustQuery("select * from t1 order by c1;").Check(testkit.Rows())
	tk.MustQuery("select * from t2 order by c1;").Check(testkit.Rows("0 0", "2 2"))

	// Rows in both t1 and t2 are matched, so will be deleted even if it's null.
	// NOTE: The null values are not generated by join.
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (c1 int);")
	tk.MustExec("create table t2 (c2 int);")
	tk.MustExec("insert into t1 values(null);")
	tk.MustExec("insert into t2 values(null);")
	tk.MustExec("delete from t1, t2 using t1 join t2 where t1.c1 is null;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows())
	tk.MustQuery("select * from t2;").Check(testkit.Rows())
}

func TestOOMPanicAction(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b double);")
	tk.MustExec("insert into t values (1,1)")
	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
	tk.MustExec("set @@tidb_mem_quota_query=1;")
	err := tk.QueryToErr("select sum(b) from t group by a;")
	require.Error(t, err)
	require.Regexp(t, "Out Of Memory Quota!.*", err.Error())

	// Test insert from select oom panic.
	tk.MustExec("drop table if exists t,t1")
	tk.MustExec("create table t (a bigint);")
	tk.MustExec("create table t1 (a bigint);")
	tk.MustExec("set @@tidb_mem_quota_query=200;")
	tk.MustMatchErrMsg("insert into t1 values (1),(2),(3),(4),(5);", "Out Of Memory Quota!.*")
	tk.MustMatchErrMsg("replace into t1 values (1),(2),(3),(4),(5);", "Out Of Memory Quota!.*")
	tk.MustExec("set @@tidb_mem_quota_query=10000")
	tk.MustExec("insert into t1 values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@tidb_mem_quota_query=10;")
	tk.MustMatchErrMsg("insert into t select a from t1 order by a desc;", "Out Of Memory Quota!.*")
	tk.MustMatchErrMsg("replace into t select a from t1 order by a desc;", "Out Of Memory Quota!.*")

	tk.MustExec("set @@tidb_mem_quota_query=10000")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5);")
	// Set the memory quota to 244 to make this SQL panic during the DeleteExec
	// instead of the TableReaderExec.
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	tk.MustMatchErrMsg("delete from t", "Out Of Memory Quota!.*")

	tk.MustExec("set @@tidb_mem_quota_query=10000;")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	tk.MustMatchErrMsg("delete t, t1 from t join t1 on t.a = t1.a", "Out Of Memory Quota!.*")

	tk.MustExec("set @@tidb_mem_quota_query=100000;")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1),(2),(3)")
	// set the memory to quota to make the SQL panic during UpdateExec instead
	// of TableReader.
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	tk.MustMatchErrMsg("update t set a = 4", "Out Of Memory Quota!.*")
}

func TestPointGetPreparedPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists ps_text")
	defer tk.MustExec("drop database if exists ps_text")
	tk.MustExec("create database ps_text")
	tk.MustExec("use ps_text")

	tk.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk.MustExec("insert into t values (1, 1, 1)")
	tk.MustExec("insert into t values (2, 2, 2)")
	tk.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk.Session().PrepareStmt("select * from t where a = ?")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[pspk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	pspk2Id, _, _, err := tk.Session().PrepareStmt("select * from t where ? = a ")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[pspk2Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(0)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(0)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	// unique index
	psuk1Id, _, _, err := tk.Session().PrepareStmt("select * from t where b = ? ")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[psuk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(0)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// test schema changed, cached plan should be invalidated
	tk.MustExec("alter table t add column col4 int default 10 after c")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(0)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	tk.MustExec("alter table t drop index k_b")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(0)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	tk.MustExec(`insert into t values(4, 3, 3, 11)`)
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10", "4 3 3 11"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(0)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	tk.MustExec("delete from t where a = 4")
	tk.MustExec("alter table t add index k_b(b)")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(0)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// use pk again
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(3)})
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))
}

func TestPointGetPreparedPlanWithCommitMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("drop database if exists ps_text")
	defer tk1.MustExec("drop database if exists ps_text")
	tk1.MustExec("create database ps_text")
	tk1.MustExec("use ps_text")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk1.Session().PrepareStmt("select * from t where a = ?")
	require.NoError(t, err)
	tk1.Session().GetSessionVars().PreparedStmts[pspk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(0)})
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// update rows
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use ps_text")
	tk2.MustExec("update t set c = c + 10 where c = 1")

	// try to point get again
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// try to update in session 1
	tk1.MustExec("update t set c = c + 10 where c = 1")
	err = tk1.ExecToErr("commit")
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("error: %s", err))

	// verify
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 11"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 11"))
}

func TestPointUpdatePreparedPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists pu_test")
	defer tk.MustExec("drop database if exists pu_test")
	tk.MustExec("create database pu_test")
	tk.MustExec("use pu_test")

	tk.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk.MustExec("insert into t values (1, 1, 1)")
	tk.MustExec("insert into t values (2, 2, 2)")
	tk.MustExec("insert into t values (3, 3, 3)")

	updateID1, pc, _, err := tk.Session().PrepareStmt(`update t set c = c + 1 where a = ?`)
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[updateID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	require.Equal(t, 1, pc)
	updateID2, pc, _, err := tk.Session().PrepareStmt(`update t set c = c + 2 where ? = a`)
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[updateID2].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	require.Equal(t, 1, pc)

	ctx := context.Background()
	// first time plan generated
	rs, err := tk.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	// using the generated plan but with different params
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// updateID2
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID2, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 8"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID2, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	// unique index
	updUkID1, _, _, err := tk.Session().PrepareStmt(`update t set c = c + 10 where b = ?`)
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[updUkID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 20"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 30"))

	// test schema changed, cached plan should be invalidated
	tk.MustExec("alter table t add column col4 int default 10 after c")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 31 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 32 10"))

	tk.MustExec("alter table t drop index k_b")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 42 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 52 10"))

	tk.MustExec("alter table t add unique index k_b(b)")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 62 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 72 10"))

	tk.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1 10"))
	tk.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2 10"))
}

func TestPointUpdatePreparedPlanWithCommitMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("drop database if exists pu_test2")
	defer tk1.MustExec("drop database if exists pu_test2")
	tk1.MustExec("create database pu_test2")
	tk1.MustExec("use pu_test2")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	ctx := context.Background()
	updateID1, _, _, err := tk1.Session().PrepareStmt(`update t set c = c + 1 where a = ?`)
	tk1.Session().GetSessionVars().PreparedStmts[updateID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	require.NoError(t, err)

	// first time plan generated
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// update rows
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use pu_test2")
	tk2.MustExec(`prepare pu2 from "update t set c = c + 2 where ? = a "`)
	tk2.MustExec("set @p3 = 3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))
	tk2.MustExec("execute pu2 using @p3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 7"))
	tk2.MustExec("execute pu2 using @p3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))

	// try to update in session 1
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))
	err = tk1.ExecToErr("commit")
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("error: %s", err))

	// verify
	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2"))
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2"))
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))

	// again next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
	tk1.MustExec("commit")

	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
}

func TestApplyCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values (1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("analyze table t;")
	result := tk.MustQuery("explain analyze SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t;")
	require.Equal(t, "└─Apply_39", result.Rows()[1][0])
	var (
		ind  int
		flag bool
	)
	value := (result.Rows()[1][5]).(string)
	for ind = 0; ind < len(value)-5; ind++ {
		if value[ind:ind+5] == "cache" {
			flag = true
			break
		}
	}
	require.True(t, flag)
	require.Equal(t, "cache:ON, cacheHitRatio:88.889%", value[ind:])

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7),(8),(9);")
	tk.MustExec("analyze table t;")
	result = tk.MustQuery("explain analyze SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t;")
	require.Equal(t, "└─Apply_39", result.Rows()[1][0])
	flag = false
	value = (result.Rows()[1][5]).(string)
	for ind = 0; ind < len(value)-5; ind++ {
		if value[ind:ind+5] == "cache" {
			flag = true
			break
		}
	}
	require.True(t, flag)
	require.Equal(t, "cache:OFF", value[ind:])
}

func TestCollectDMLRuntimeStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, unique index (a))")

	testSQLs := []string{
		"insert ignore into t1 values (5,5);",
		"insert into t1 values (5,5) on duplicate key update a=a+1;",
		"replace into t1 values (5,6),(6,7)",
		"update t1 set a=a+1 where a=6;",
	}

	getRootStats := func() string {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(plannercore.Plan)
		require.True(t, ok)
		stats := tk.Session().GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(p.ID())
		return stats.String()
	}
	for _, sql := range testSQLs {
		tk.MustExec(sql)
		require.Regexp(t, "time.*loops.*Get.*num_rpc.*total_time.*", getRootStats())
	}

	// Test for lock keys stats.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t1 set b=b+1")
	require.Regexp(t, "time.*lock_keys.*time.* region.* keys.* lock_rpc:.* rpc_count.*", getRootStats())
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 for update").Check(testkit.Rows("5 6", "7 7"))
	require.Regexp(t, "time.*lock_keys.*time.* region.* keys.* lock_rpc:.* rpc_count.*", getRootStats())
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert ignore into t1 values (9,9)")
	require.Regexp(t, "time:.*, loops:.*, prepare:.*, check_insert: {total_time:.*, mem_insert_time:.*, prefetch:.*, rpc:{BatchGet:{num_rpc:.*, total_time:.*}}}.*", getRootStats())
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values (10,10) on duplicate key update a=a+1")
	require.Regexp(t, "time:.*, loops:.*, prepare:.*, check_insert: {total_time:.*, mem_insert_time:.*, prefetch:.*, rpc:{BatchGet:{num_rpc:.*, total_time:.*}.*", getRootStats())
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values (1,2)")
	require.Regexp(t, "time:.*, loops:.*, prepare:.*, insert:.*", getRootStats())
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert ignore into t1 values(11,11) on duplicate key update `a`=`a`+1")
	require.Regexp(t, "time:.*, loops:.*, prepare:.*, check_insert: {total_time:.*, mem_insert_time:.*, prefetch:.*, rpc:.*}", getRootStats())
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("replace into t1 values (1,4)")
	require.Regexp(t, "time:.*, loops:.*, prefetch:.*, rpc:.*", getRootStats())
	tk.MustExec("rollback")
}

func TestIssue13758(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (pk int(11) primary key, a int(11) not null, b int(11), key idx_b(b), key idx_a(a))")
	tk.MustExec("insert into `t1` values (1,1,0),(2,7,6),(3,2,null),(4,1,null),(5,4,5)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t2 values (1),(null)")
	tk.MustQuery("select (select a from t1 use index(idx_a) where b >= t2.a order by a limit 1) as field from t2").Check(testkit.Rows(
		"4",
		"<nil>",
	))
}

func TestIssue20237(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table t(a date, b float)")
	tk.MustExec("create table s(b float)")
	tk.MustExec(`insert into t values(NULL,-37), ("2011-11-04",105), ("2013-03-02",-22), ("2006-07-02",-56), (NULL,124), (NULL,111), ("2018-03-03",-5);`)
	tk.MustExec(`insert into s values(-37),(105),(-22),(-56),(124),(105),(111),(-5);`)
	tk.MustQuery(`select count(distinct t.a, t.b) from t join s on t.b= s.b;`).Check(testkit.Rows("4"))
}

func TestIssue24933(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop view if exists v;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3);")

	tk.MustExec("create definer='root'@'localhost' view v as select count(*) as c1 from t;")
	tk.MustQuery("select * from v;").Check(testkit.Rows("3"))

	// Test subquery and outer field is wildcard.
	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select count(*) from t) s;")
	tk.MustQuery("select * from v order by 1;").Check(testkit.Rows("3"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select avg(a) from t group by a) s;")
	tk.MustQuery("select * from v order by 1;").Check(testkit.Rows("1.0000", "2.0000", "3.0000"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select sum(a) from t group by a) s;")
	tk.MustQuery("select * from v order by 1;").Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select group_concat(a) from t group by a) s;")
	tk.MustQuery("select * from v order by 1;").Check(testkit.Rows("1", "2", "3"))

	// Test alias names.
	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select count(0) as c1 from t) s;")
	tk.MustQuery("select * from v order by 1;").Check(testkit.Rows("3"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select count(*) as c1 from t) s;")
	tk.MustQuery("select * from v order by 1;").Check(testkit.Rows("3"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select group_concat(a) as `concat(a)` from t group by a) s;")
	tk.MustQuery("select * from v order by 1;").Check(testkit.Rows("1", "2", "3"))

	// Test firstrow.
	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select a from t group by a) s;")
	tk.MustQuery("select * from v order by 1;").Check(testkit.Rows("1", "2", "3"))

	// Test direct select.
	tk.MustGetErrMsg("SELECT `s`.`count(a)` FROM (SELECT COUNT(`a`) FROM `test`.`t`) AS `s`", "[planner:1054]Unknown column 's.count(a)' in 'field list'")

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select count(a) from t) s;")
	tk.MustQuery("select * from v").Check(testkit.Rows("3"))

	// Test window function.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(c1 int);")
	tk.MustExec("insert into t values(111), (222), (333);")
	tk.MustExec("drop view if exists v;")
	tk.MustExec("create definer='root'@'localhost' view v as (select * from (select row_number() over (order by c1) from t) s);")
	tk.MustQuery("select * from v;").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop view if exists v;")
	tk.MustExec("create definer='root'@'localhost' view v as (select * from (select c1, row_number() over (order by c1) from t) s);")
	tk.MustQuery("select * from v;").Check(testkit.Rows("111 1", "222 2", "333 3"))

	// Test simple expr.
	tk.MustExec("drop view if exists v;")
	tk.MustExec("create definer='root'@'localhost' view v as (select * from (select c1 or 0 from t) s)")
	tk.MustQuery("select * from v;").Check(testkit.Rows("1", "1", "1"))
	tk.MustQuery("select `c1 or 0` from v;").Check(testkit.Rows("1", "1", "1"))

	tk.MustExec("drop view v;")
}

func TestTableSampleTemporaryTable(t *testing.T) {
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
	tk.MustExec("drop table if exists tmp1")
	tk.MustExec("create global temporary table tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))" +
		"on commit delete rows")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp2")
	tk.MustExec("create temporary table tmp2 (id int not null primary key, code int not null, value int default null, unique key code(code));")

	// sleep 1us to make test stale
	time.Sleep(time.Microsecond)

	// test tablesample return empty for global temporary table
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustExec("insert into tmp1 values (1, 1, 1)")
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())
	tk.MustExec("commit")

	// tablesample for global temporary table should not return error for compatibility of tools like dumpling
	tk.MustExec("set @@tidb_snapshot=NOW(6)")
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())
	tk.MustExec("commit")
	tk.MustExec("set @@tidb_snapshot=''")

	// test tablesample returns error for local temporary table
	tk.MustGetErrMsg("select * from tmp2 tablesample regions()", "TABLESAMPLE clause can not be applied to local temporary tables")

	tk.MustExec("begin")
	tk.MustExec("insert into tmp2 values (1, 1, 1)")
	tk.MustGetErrMsg("select * from tmp2 tablesample regions()", "TABLESAMPLE clause can not be applied to local temporary tables")
	tk.MustExec("commit")
	tk.MustGetErrMsg("select * from tmp2 tablesample regions()", "TABLESAMPLE clause can not be applied to local temporary tables")
}

func TestCTEWithIndexLookupJoinDeadLock(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int(11) default null,b int(11) default null,key b (b),key ba (b))")
	tk.MustExec("create table t1 (a int(11) default null,b int(11) default null,key idx_ab (a,b),key idx_a (a),key idx_b (b))")
	tk.MustExec("create table t2 (a int(11) default null,b int(11) default null,key idx_ab (a,b),key idx_a (a),key idx_b (b))")
	// It's easy to reproduce this problem in 30 times execution of IndexLookUpJoin.
	for i := 0; i < 30; i++ {
		tk.MustExec("with cte as (with cte1 as (select * from t2 use index(idx_ab) where a > 1 and b > 1) select * from cte1) select /*+use_index(t1 idx_ab)*/ * from cte join t1 on t1.a=cte.a;")
	}
}

func TestGetResultRowsCount(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	for i := 1; i <= 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v)", i))
	}
	cases := []struct {
		sql string
		row int64
	}{
		{"select * from t", 10},
		{"select * from t where a < 0", 0},
		{"select * from t where a <= 3", 3},
		{"insert into t values (11)", 0},
		{"replace into t values (12)", 0},
		{"update t set a=13 where a=12", 0},
	}

	for _, ca := range cases {
		if strings.HasPrefix(ca.sql, "select") {
			tk.MustQuery(ca.sql)
		} else {
			tk.MustExec(ca.sql)
		}
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(plannercore.Plan)
		require.True(t, ok)
		cnt := executor.GetResultRowsCount(tk.Session(), p)
		require.Equal(t, cnt, ca.row, fmt.Sprintf("sql: %v", ca.sql))
	}
}

func TestAdminShowDDLJobs(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_admin_show_ddl_jobs")
	tk.MustExec("use test_admin_show_ddl_jobs")
	tk.MustExec("create table t (a int);")

	re := tk.MustQuery("admin show ddl jobs 1")
	row := re.Rows()[0]
	require.Equal(t, "test_admin_show_ddl_jobs", row[1])
	jobID, err := strconv.Atoi(row[0].(string))
	require.NoError(t, err)

	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMeta(txn)
		job, err := tt.GetHistoryDDLJob(int64(jobID))
		require.NoError(t, err)
		require.NotNil(t, job)
		// Test for compatibility. Old TiDB version doesn't have SchemaName field, and the BinlogInfo maybe nil.
		// See PR: 11561.
		job.BinlogInfo = nil
		job.SchemaName = ""
		err = tt.AddHistoryDDLJob(job, true)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	re = tk.MustQuery("admin show ddl jobs 1")
	row = re.Rows()[0]
	require.Equal(t, "test_admin_show_ddl_jobs", row[1])

	re = tk.MustQuery("admin show ddl jobs 1 where job_type='create table'")
	row = re.Rows()[0]
	require.Equal(t, "test_admin_show_ddl_jobs", row[1])
	require.Equal(t, "<nil>", row[10])

	// Test the START_TIME and END_TIME field.
	tk.MustExec(`set @@time_zone = 'Asia/Shanghai'`)
	re = tk.MustQuery("admin show ddl jobs where end_time is not NULL")
	row = re.Rows()[0]
	createTime, err := types.ParseDatetime(nil, row[8].(string))
	require.NoError(t, err)
	startTime, err := types.ParseDatetime(nil, row[9].(string))
	require.NoError(t, err)
	endTime, err := types.ParseDatetime(nil, row[10].(string))
	require.NoError(t, err)
	tk.MustExec(`set @@time_zone = 'Europe/Amsterdam'`)
	re = tk.MustQuery("admin show ddl jobs where end_time is not NULL")
	row2 := re.Rows()[0]
	require.NotEqual(t, row[8], row2[8])
	require.NotEqual(t, row[9], row2[9])
	require.NotEqual(t, row[10], row2[10])
	createTime2, err := types.ParseDatetime(nil, row2[8].(string))
	require.NoError(t, err)
	startTime2, err := types.ParseDatetime(nil, row2[9].(string))
	require.NoError(t, err)
	endTime2, err := types.ParseDatetime(nil, row2[10].(string))
	require.NoError(t, err)
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	loc2, err := time.LoadLocation("Europe/Amsterdam")
	require.NoError(t, err)
	tt, err := createTime.GoTime(loc)
	require.NoError(t, err)
	t2, err := createTime2.GoTime(loc2)
	require.NoError(t, err)
	require.Equal(t, t2.In(time.UTC), tt.In(time.UTC))
	tt, err = startTime.GoTime(loc)
	require.NoError(t, err)
	t2, err = startTime2.GoTime(loc2)
	require.NoError(t, err)
	require.Equal(t, t2.In(time.UTC), tt.In(time.UTC))
	tt, err = endTime.GoTime(loc)
	require.NoError(t, err)
	t2, err = endTime2.GoTime(loc2)
	require.NoError(t, err)
	require.Equal(t, t2.In(time.UTC), tt.In(time.UTC))
}

func TestAdminShowDDLJobsInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// Test for issue: https://github.com/pingcap/tidb/issues/29915
	tk.MustExec("create placement policy x followers=4;")
	tk.MustExec("create placement policy y " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2")
	tk.MustExec("create database if not exists test_admin_show_ddl_jobs")
	tk.MustExec("use test_admin_show_ddl_jobs")

	tk.MustExec("create table t (a int);")
	tk.MustExec("create table t1 (a int);")

	tk.MustExec("alter table t placement policy x;")
	require.Equal(t, "alter table placement", tk.MustQuery("admin show ddl jobs 1").Rows()[0][3])

	tk.MustExec("rename table t to tt, t1 to tt1")
	require.Equal(t, "rename tables", tk.MustQuery("admin show ddl jobs 1").Rows()[0][3])

	tk.MustExec("create table tt2 (c int) PARTITION BY RANGE (c) " +
		"(PARTITION p0 VALUES LESS THAN (6)," +
		"PARTITION p1 VALUES LESS THAN (11)," +
		"PARTITION p2 VALUES LESS THAN (16)," +
		"PARTITION p3 VALUES LESS THAN (21));")
	tk.MustExec("alter table tt2 partition p0 placement policy y")
	require.Equal(t, "alter table partition placement", tk.MustQuery("admin show ddl jobs 1").Rows()[0][3])

	tk.MustExec("alter table tt1 cache")
	require.Equal(t, "alter table cache", tk.MustQuery("admin show ddl jobs 1").Rows()[0][3])
	tk.MustExec("alter table tt1 nocache")
	require.Equal(t, "alter table nocache", tk.MustQuery("admin show ddl jobs 1").Rows()[0][3])
}

func TestAdminChecksumOfPartitionedTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS admin_checksum_partition_test;")
	tk.MustExec("CREATE TABLE admin_checksum_partition_test (a INT) PARTITION BY HASH(a) PARTITIONS 4;")
	tk.MustExec("INSERT INTO admin_checksum_partition_test VALUES (1), (2);")

	r := tk.MustQuery("ADMIN CHECKSUM TABLE admin_checksum_partition_test;")
	r.Check(testkit.Rows("test admin_checksum_partition_test 1 5 5"))
}

func TestUnion2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	testSQL := `drop table if exists union_test; create table union_test(id int);`
	tk.MustExec(testSQL)

	testSQL = `drop table if exists union_test;`
	tk.MustExec(testSQL)
	testSQL = `create table union_test(id int);`
	tk.MustExec(testSQL)
	testSQL = `insert union_test values (1),(2)`
	tk.MustExec(testSQL)

	testSQL = `select * from (select id from union_test union select id from union_test) t order by id;`
	r := tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1", "2"))

	r = tk.MustQuery("select 1 union all select 1")
	r.Check(testkit.Rows("1", "1"))

	r = tk.MustQuery("select 1 union all select 1 union select 1")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select 1 as a union (select 2) order by a limit 1")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select 1 as a union (select 2) order by a limit 1, 1")
	r.Check(testkit.Rows("2"))

	r = tk.MustQuery("select id from union_test union all (select 1) order by id desc")
	r.Check(testkit.Rows("2", "1", "1"))

	r = tk.MustQuery("select id as a from union_test union (select 1) order by a desc")
	r.Check(testkit.Rows("2", "1"))

	r = tk.MustQuery(`select null as a union (select "abc") order by a`)
	r.Check(testkit.Rows("<nil>", "abc"))

	r = tk.MustQuery(`select "abc" as a union (select 1) order by a`)
	r.Check(testkit.Rows("1", "abc"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c int, d int)")
	tk.MustExec("insert t1 values (NULL, 1)")
	tk.MustExec("insert t1 values (1, 1)")
	tk.MustExec("insert t1 values (1, 2)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (c int, d int)")
	tk.MustExec("insert t2 values (1, 3)")
	tk.MustExec("insert t2 values (1, 1)")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (c int, d int)")
	tk.MustExec("insert t3 values (3, 2)")
	tk.MustExec("insert t3 values (4, 3)")
	r = tk.MustQuery(`select sum(c1), c2 from (select c c1, d c2 from t1 union all select d c1, c c2 from t2 union all select c c1, d c2 from t3) x group by c2 order by c2`)
	r.Check(testkit.Rows("5 1", "4 2", "4 3"))

	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1 (a int primary key)")
	tk.MustExec("create table t2 (a int primary key)")
	tk.MustExec("create table t3 (a int primary key)")
	tk.MustExec("insert t1 values (7), (8)")
	tk.MustExec("insert t2 values (1), (9)")
	tk.MustExec("insert t3 values (2), (3)")
	r = tk.MustQuery("select * from t1 union all select * from t2 union all (select * from t3) order by a limit 2")
	r.Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert t1 values (2), (1)")
	tk.MustExec("insert t2 values (3), (4)")
	r = tk.MustQuery("select * from t1 union all (select * from t2) order by a limit 1")
	r.Check(testkit.Rows("1"))
	r = tk.MustQuery("select (select * from t1 where a != t.a union all (select * from t2 where a != t.a) order by a limit 1) from t1 t")
	r.Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int unsigned primary key auto_increment, c1 int, c2 int, index c1_c2 (c1, c2))")
	tk.MustExec("insert into t (c1, c2) values (1, 1)")
	tk.MustExec("insert into t (c1, c2) values (1, 2)")
	tk.MustExec("insert into t (c1, c2) values (2, 3)")
	r = tk.MustQuery("select * from (select * from t where t.c1 = 1 union select * from t where t.id = 1) s order by s.id")
	r.Check(testkit.Rows("1 1 1", "2 1 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (f1 DATE)")
	tk.MustExec("INSERT INTO t VALUES ('1978-11-26')")
	r = tk.MustQuery("SELECT f1+0 FROM t UNION SELECT f1+0 FROM t")
	r.Check(testkit.Rows("19781126"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b int)")
	tk.MustExec("INSERT INTO t VALUES ('1', '1')")
	r = tk.MustQuery("select b from (SELECT * FROM t UNION ALL SELECT a, b FROM t order by a) t")
	r.Check(testkit.Rows("1", "1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a DECIMAL(4,2))")
	tk.MustExec("INSERT INTO t VALUE(12.34)")
	r = tk.MustQuery("SELECT 1 AS c UNION select a FROM t")
	r.Sort().Check(testkit.Rows("1.00", "12.34"))

	// #issue3771
	r = tk.MustQuery("SELECT 'a' UNION SELECT CONCAT('a', -4)")
	r.Sort().Check(testkit.Rows("a", "a-4"))

	// test race
	tk.MustQuery("SELECT @x:=0 UNION ALL SELECT @x:=0 UNION ALL SELECT @x")

	// test field tp
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("CREATE TABLE t1 (a date)")
	tk.MustExec("CREATE TABLE t2 (a date)")
	tk.MustExec("SELECT a from t1 UNION select a FROM t2")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" + "  `a` date DEFAULT NULL\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Move from session test.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c double);")
	tk.MustExec("create table t2 (c double);")
	tk.MustExec("insert into t1 value (73);")
	tk.MustExec("insert into t2 value (930);")
	// If set unspecified column flen to 0, it will cause bug in union.
	// This test is used to prevent the bug reappear.
	tk.MustQuery("select c from t1 union (select c from t2) order by c").Check(testkit.Rows("73", "930"))

	// issue 5703
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a date)")
	tk.MustExec("insert into t value ('2017-01-01'), ('2017-01-02')")
	r = tk.MustQuery("(select a from t where a < 0) union (select a from t where a > 0) order by a")
	r.Check(testkit.Rows("2017-01-01", "2017-01-02"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t value(0),(0)")
	tk.MustQuery("select 1 from (select a from t union all select a from t) tmp").Check(testkit.Rows("1", "1", "1", "1"))
	tk.MustQuery("select 10 as a from dual union select a from t order by a desc limit 1 ").Check(testkit.Rows("10"))
	tk.MustQuery("select -10 as a from dual union select a from t order by a limit 1 ").Check(testkit.Rows("-10"))
	tk.MustQuery("select count(1) from (select a from t union all select a from t) tmp").Check(testkit.Rows("4"))

	err := tk.ExecToErr("select 1 from (select a from t limit 1 union all select a from t limit 1) tmp")
	require.Error(t, err)
	terr := errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrWrongUsage), terr.Code())

	err = tk.ExecToErr("select 1 from (select a from t order by a union all select a from t limit 1) tmp")
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrWrongUsage), terr.Code())

	_, err = tk.Exec("(select a from t order by a) union all select a from t limit 1 union all select a from t limit 1")
	require.Truef(t, terror.ErrorEqual(err, plannercore.ErrWrongUsage), "err %v", err)

	_, err = tk.Exec("(select a from t limit 1) union all select a from t limit 1")
	require.NoError(t, err)
	_, err = tk.Exec("(select a from t order by a) union all select a from t order by a")
	require.NoError(t, err)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t value(1),(2),(3)")

	tk.MustQuery("(select a from t order by a limit 2) union all (select a from t order by a desc limit 2) order by a desc limit 1,2").Check(testkit.Rows("2", "2"))
	tk.MustQuery("select a from t union all select a from t order by a desc limit 5").Check(testkit.Rows("3", "3", "2", "2", "1"))
	tk.MustQuery("(select a from t order by a desc limit 2) union all select a from t group by a order by a").Check(testkit.Rows("1", "2", "2", "3", "3"))
	tk.MustQuery("(select a from t order by a desc limit 2) union all select 33 as a order by a desc limit 2").Check(testkit.Rows("33", "3"))

	tk.MustQuery("select 1 union select 1 union all select 1").Check(testkit.Rows("1", "1"))
	tk.MustQuery("select 1 union all select 1 union select 1").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`create table t1(a bigint, b bigint);`)
	tk.MustExec(`create table t2(a bigint, b bigint);`)
	tk.MustExec(`insert into t1 values(1, 1);`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t2 values(1, 1);`)
	tk.MustExec(`set @@tidb_init_chunk_size=2;`)
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustQuery(`select count(*) from (select t1.a, t1.b from t1 left join t2 on t1.a=t2.a union all select t1.a, t1.a from t1 left join t2 on t1.a=t2.a) tmp;`).Check(testkit.Rows("128"))
	tk.MustQuery(`select tmp.a, count(*) from (select t1.a, t1.b from t1 left join t2 on t1.a=t2.a union all select t1.a, t1.a from t1 left join t2 on t1.a=t2.a) tmp;`).Check(testkit.Rows("1 128"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t value(1 ,2)")
	tk.MustQuery("select a, b from (select a, 0 as d, b from t union all select a, 0 as d, b from t) test;").Check(testkit.Rows("1 2", "1 2"))

	// #issue 8141
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t1 value(1,2),(1,1),(2,2),(2,2),(3,2),(3,2)")
	tk.MustExec("set @@tidb_init_chunk_size=2;")
	tk.MustQuery("select count(*) from (select a as c, a as d from t1 union all select a, b from t1) t;").Check(testkit.Rows("12"))

	// #issue 8189 and #issue 8199
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("CREATE TABLE t1 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustExec("CREATE TABLE t2 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t2 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustQuery("select a from t1 union select a from t1 order by (select a+1);").Check(testkit.Rows("1", "2", "3"))

	// #issue 8201
	for i := 0; i < 4; i++ {
		tk.MustQuery("SELECT(SELECT 0 AS a FROM dual UNION SELECT 1 AS a FROM dual ORDER BY a ASC  LIMIT 1) AS dev").Check(testkit.Rows("0"))
	}

	// #issue 8231
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t1 (uid int(1))")
	tk.MustExec("INSERT INTO t1 SELECT 150")
	tk.MustQuery("SELECT 'a' UNION SELECT uid FROM t1 order by 1 desc;").Check(testkit.Rows("a", "150"))

	// #issue 8196
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("CREATE TABLE t1 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustExec("CREATE TABLE t2 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t2 values(3,'c'),(4,'d'),(5,'f'),(6,'e')")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	_, err = tk.Exec("(select a,b from t1 limit 2) union all (select a,b from t2 order by a limit 1) order by t1.b")
	require.Equal(t, "[planner:1250]Table 't1' from one of the SELECTs cannot be used in global ORDER clause", err.Error())

	// #issue 9900
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b decimal(6, 3))")
	tk.MustExec("insert into t values(1, 1.000)")
	tk.MustQuery("select count(distinct a), sum(distinct a), avg(distinct a) from (select a from t union all select b from t) tmp;").Check(testkit.Rows("1 1.000 1.0000000"))

	// #issue 23832
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bit(20), b float, c double, d int)")
	tk.MustExec("insert into t values(10, 10, 10, 10), (1, -1, 2, -2), (2, -2, 1, 1), (2, 1.1, 2.1, 10.1)")
	tk.MustQuery("select a from t union select 10 order by a").Check(testkit.Rows("1", "2", "10"))
}

func TestUnionLimit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists union_limit")
	tk.MustExec("create table union_limit (id int) partition by hash(id) partitions 30")
	for i := 0; i < 60; i++ {
		tk.MustExec(fmt.Sprintf("insert into union_limit values (%d)", i))
	}
	// Cover the code for worker count limit in the union executor.
	tk.MustQuery("select * from union_limit limit 10")
}

func TestLowResolutionTSORead(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@autocommit=1")
	tk.MustExec("use test")
	tk.MustExec("create table low_resolution_tso(a int)")
	tk.MustExec("insert low_resolution_tso values (1)")

	// enable low resolution tso
	require.False(t, tk.Session().GetSessionVars().LowResolutionTSO)
	tk.MustExec("set @@tidb_low_resolution_tso = 'on'")
	require.True(t, tk.Session().GetSessionVars().LowResolutionTSO)

	time.Sleep(3 * time.Second)
	tk.MustQuery("select * from low_resolution_tso").Check(testkit.Rows("1"))
	err := tk.ExecToErr("update low_resolution_tso set a = 2")
	require.Error(t, err)
	tk.MustExec("set @@tidb_low_resolution_tso = 'off'")
	tk.MustExec("update low_resolution_tso set a = 2")
	tk.MustQuery("select * from low_resolution_tso").Check(testkit.Rows("2"))
}

func TestStaleReadAtFutureTime(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Setting tx_read_ts to a time in the future will fail. (One day before the 2038 problem)
	tk.MustGetErrMsg("set @@tx_read_ts = '2038-01-18 03:14:07'", "cannot set read timestamp to a future time")
	// TxnReadTS Is not updated if check failed.
	require.Zero(t, tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
}

func TestYearTypeDeleteIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a YEAR, PRIMARY KEY(a));")
	tk.MustExec("insert into t set a = '2151';")
	tk.MustExec("delete from t;")
	tk.MustExec("admin check table t")
}

func TestToPBExpr(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a decimal(10,6), b decimal, index idx_b (b))")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values (1.1, 1.1)")
	tk.MustExec("insert t values (2.4, 2.4)")
	tk.MustExec("insert t values (3.3, 2.7)")
	result := tk.MustQuery("select * from t where a < 2.399999")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where a > 1.5")
	result.Check(testkit.Rows("2.400000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where a <= 1.1")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where b >= 3")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where not (b = 1)")
	result.Check(testkit.Rows("2.400000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where b&1 = a|1")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where b != 2 and b <=> 3")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where b in (3)")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where b not in (1, 2)")
	result.Check(testkit.Rows("3.300000 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(255), b int)")
	tk.MustExec("insert t values ('abc123', 1)")
	tk.MustExec("insert t values ('ab123', 2)")
	result = tk.MustQuery("select * from t where a like 'ab%'")
	result.Check(testkit.Rows("abc123 1", "ab123 2"))
	result = tk.MustQuery("select * from t where a like 'ab_12'")
	result.Check(nil)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	tk.MustExec("insert t values (1)")
	tk.MustExec("insert t values (2)")
	result = tk.MustQuery("select * from t where not (a = 1)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select * from t where not(not (a = 1))")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t where not(a != 1 and a != 2)")
	result.Check(testkit.Rows("1", "2"))
}

func TestDatumXAPI(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a decimal(10,6), b decimal, index idx_b (b))")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values (1.1, 1.1)")
	tk.MustExec("insert t values (2.2, 2.2)")
	tk.MustExec("insert t values (3.3, 2.7)")
	result := tk.MustQuery("select * from t where a > 1.5")
	result.Check(testkit.Rows("2.200000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where b > 1.5")
	result.Check(testkit.Rows("2.200000 2", "3.300000 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a time(3), b time, index idx_a (a))")
	tk.MustExec("insert t values ('11:11:11', '11:11:11')")
	tk.MustExec("insert t values ('11:11:12', '11:11:12')")
	tk.MustExec("insert t values ('11:11:13', '11:11:13')")
	result = tk.MustQuery("select * from t where a > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12.000 11:11:12", "11:11:13.000 11:11:13"))
	result = tk.MustQuery("select * from t where b > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12.000 11:11:12", "11:11:13.000 11:11:13"))
}

func TestSQLMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a tinyint not null)")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	_, err := tk.Exec("insert t values ()")
	require.Error(t, err)

	_, err = tk.Exec("insert t values ('1000')")
	require.Error(t, err)

	tk.MustExec("create table if not exists tdouble (a double(3,2))")
	_, err = tk.Exec("insert tdouble values (10.23)")
	require.Error(t, err)

	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values ()")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value"))
	tk.MustExec("insert t values (null)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null"))
	tk.MustExec("insert ignore t values (null)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null"))
	tk.MustExec("insert t select null")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null"))
	tk.MustExec("insert t values (1000)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("0", "0", "0", "0", "127"))

	tk.MustExec("insert tdouble values (10.23)")
	tk.MustQuery("select * from tdouble").Check(testkit.Rows("9.99"))

	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	tk.MustExec("set @@global.sql_mode = ''")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("drop table if exists t2")
	tk2.MustExec("create table t2 (a varchar(3))")
	tk2.MustExec("insert t2 values ('abcd')")
	tk2.MustQuery("select * from t2").Check(testkit.Rows("abc"))

	// session1 is still in strict mode.
	_, err = tk.Exec("insert t2 values ('abcd')")
	require.Error(t, err)
	// Restore original global strict mode.
	tk.MustExec("set @@global.sql_mode = 'STRICT_TRANS_TABLES'")
}

func TestTableDual(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	result := tk.MustQuery("Select 1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select 1 from dual")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select count(*) from dual")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select 1 from dual where 1")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustQuery("select t1.* from t t1, t t2 where t1.a=t2.a and 1=0").Check(testkit.Rows())
}

func TestTableScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use information_schema")
	result := tk.MustQuery("select * from schemata")
	// There must be these tables: information_schema, mysql, performance_schema and test.
	require.GreaterOrEqual(t, len(result.Rows()), 4)
	tk.MustExec("use test")
	tk.MustExec("create database mytest")
	rowStr1 := fmt.Sprintf("%s %s %s %s %v %v", "def", "mysql", "utf8mb4", "utf8mb4_bin", nil, nil)
	rowStr2 := fmt.Sprintf("%s %s %s %s %v %v", "def", "mytest", "utf8mb4", "utf8mb4_bin", nil, nil)
	tk.MustExec("use information_schema")
	result = tk.MustQuery("select * from schemata where schema_name = 'mysql'")
	result.Check(testkit.Rows(rowStr1))
	result = tk.MustQuery("select * from schemata where schema_name like 'my%'")
	result.Check(testkit.Rows(rowStr1, rowStr2))
	result = tk.MustQuery("select 1 from tables limit 1")
	result.Check(testkit.Rows("1"))
}

func TestAdapterStatement(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.Session().GetSessionVars().TxnCtx.InfoSchema = domain.GetDomain(tk.Session()).InfoSchema()
	compiler := &executor.Compiler{Ctx: tk.Session()}
	s := parser.New()
	stmtNode, err := s.ParseOneStmt("select 1", "", "")
	require.NoError(t, err)
	stmt, err := compiler.Compile(context.TODO(), stmtNode)
	require.NoError(t, err)
	require.Equal(t, "select 1", stmt.OriginText())

	stmtNode, err = s.ParseOneStmt("create table test.t (a int)", "", "")
	require.NoError(t, err)
	stmt, err = compiler.Compile(context.TODO(), stmtNode)
	require.NoError(t, err)
	require.Equal(t, "create table test.t (a int)", stmt.OriginText())
}

func TestIsPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql")
	ctx := tk.Session().(sessionctx.Context)
	tests := map[string]bool{
		"select * from help_topic where name='aaa'":         false,
		"select 1 from help_topic where name='aaa'":         false,
		"select * from help_topic where help_topic_id=1":    true,
		"select * from help_topic where help_category_id=1": false,
	}
	s := parser.New()
	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		require.NoError(t, err)
		preprocessorReturn := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(ctx, stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, preprocessorReturn.InfoSchema)
		require.NoError(t, err)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		require.NoError(t, err)
		require.Equal(t, result, ret)
	}
}

func TestClusteredIndexIsPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_cluster_index_is_point_get;")
	tk.MustExec("create database test_cluster_index_is_point_get;")
	tk.MustExec("use test_cluster_index_is_point_get;")

	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255), b int, c char(10), primary key (c, a));")
	ctx := tk.Session().(sessionctx.Context)

	tests := map[string]bool{
		"select 1 from t where a='x'":                   false,
		"select * from t where c='x'":                   false,
		"select * from t where a='x' and c='x'":         true,
		"select * from t where a='x' and c='x' and b=1": false,
	}
	s := parser.New()
	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		require.NoError(t, err)
		preprocessorReturn := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(ctx, stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, preprocessorReturn.InfoSchema)
		require.NoError(t, err)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		require.NoError(t, err)
		require.Equal(t, result, ret)
	}
}

func TestRow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 3)")
	tk.MustExec("insert t values (2, 1)")
	tk.MustExec("insert t values (2, 3)")
	result := tk.MustQuery("select * from t where (c, d) < (2,2)")
	result.Check(testkit.Rows("1 1", "1 3", "2 1"))
	result = tk.MustQuery("select * from t where (1,2,3) > (3,2,1)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t where row(1,2,3) > (3,2,1)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t where (c, d) = (select * from t where (c,d) = (1,1))")
	result.Check(testkit.Rows("1 1"))
	result = tk.MustQuery("select * from t where (c, d) = (select * from t k where (t.c,t.d) = (c,d))")
	result.Check(testkit.Rows("1 1", "1 3", "2 1", "2 3"))
	result = tk.MustQuery("select (1, 2, 3) < (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 3, 3)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 1, 4)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (2, 3, 4) >= (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) = (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) != (2, 3, 4)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select row(1, 1) in (row(1, 1))")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 0) in (row(1, 1))")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select row(1, 1) in (select 1, 1)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 1) > row(1, 0)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 1) > (select 1, 0)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select 1 > (select 1)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (select 1)")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("insert t1 values (1,2),(1,null)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (c int, d int)")
	tk.MustExec("insert t2 values (0,0)")

	tk.MustQuery("select * from t2 where (1,2) in (select * from t1)").Check(testkit.Rows("0 0"))
	tk.MustQuery("select * from t2 where (1,2) not in (select * from t1)").Check(testkit.Rows())
	tk.MustQuery("select * from t2 where (1,1) not in (select * from t1)").Check(testkit.Rows())
	tk.MustQuery("select * from t2 where (1,null) in (select * from t1)").Check(testkit.Rows())
	tk.MustQuery("select * from t2 where (null,null) in (select * from t1)").Check(testkit.Rows())

	tk.MustExec("delete from t1 where a=1 and b=2")
	tk.MustQuery("select (1,1) in (select * from t2) from t1").Check(testkit.Rows("0"))
	tk.MustQuery("select (1,1) not in (select * from t2) from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select (1,1) in (select 1,1 from t2) from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select (1,1) not in (select 1,1 from t2) from t1").Check(testkit.Rows("0"))

	// MySQL 5.7 returns 1 for these 2 queries, which is wrong.
	tk.MustQuery("select (1,null) not in (select 1,1 from t2) from t1").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select (t1.a,null) not in (select 1,1 from t2) from t1").Check(testkit.Rows("<nil>"))

	tk.MustQuery("select (1,null) in (select * from t1)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select (1,null) not in (select * from t1)").Check(testkit.Rows("<nil>"))
}

func TestColumnName(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	// disable only full group by
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'")
	rs, err := tk.Exec("select 1 + c, count(*) from t")
	require.NoError(t, err)
	fields := rs.Fields()
	require.Len(t, fields, 2)
	require.Equal(t, "1 + c", fields[0].Column.Name.L)
	require.Equal(t, "1 + c", fields[0].ColumnAsName.L)
	require.Equal(t, "count(*)", fields[1].Column.Name.L)
	require.Equal(t, "count(*)", fields[1].ColumnAsName.L)
	require.NoError(t, rs.Close())
	rs, err = tk.Exec("select (c) > all (select c from t) from t")
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, "(c) > all (select c from t)", fields[0].Column.Name.L)
	require.Equal(t, "(c) > all (select c from t)", fields[0].ColumnAsName.L)
	require.NoError(t, rs.Close())
	tk.MustExec("begin")
	tk.MustExec("insert t values(1,1)")
	rs, err = tk.Exec("select c d, d c from t")
	require.NoError(t, err)
	fields = rs.Fields()
	require.Len(t, fields, 2)
	require.Equal(t, "c", fields[0].Column.Name.L)
	require.Equal(t, "d", fields[0].ColumnAsName.L)
	require.Equal(t, "d", fields[1].Column.Name.L)
	require.Equal(t, "c", fields[1].ColumnAsName.L)
	require.NoError(t, rs.Close())
	// Test case for query a column of a table.
	// In this case, all attributes have values.
	rs, err = tk.Exec("select c as a from t as t2")
	require.NoError(t, err)
	fields = rs.Fields()
	require.Equal(t, "c", fields[0].Column.Name.L)
	require.Equal(t, "a", fields[0].ColumnAsName.L)
	require.Equal(t, "t", fields[0].Table.Name.L)
	require.Equal(t, "t2", fields[0].TableAsName.L)
	require.Equal(t, "test", fields[0].DBName.L)
	require.Nil(t, rs.Close())
	// Test case for query a expression which only using constant inputs.
	// In this case, the table, org_table and database attributes will all be empty.
	rs, err = tk.Exec("select hour(1) as a from t as t2")
	require.NoError(t, err)
	fields = rs.Fields()
	require.Equal(t, "a", fields[0].Column.Name.L)
	require.Equal(t, "a", fields[0].ColumnAsName.L)
	require.Equal(t, "", fields[0].Table.Name.L)
	require.Equal(t, "", fields[0].TableAsName.L)
	require.Equal(t, "", fields[0].DBName.L)
	require.Nil(t, rs.Close())
	// Test case for query a column wrapped with parentheses and unary plus.
	// In this case, the column name should be its original name.
	rs, err = tk.Exec("select (c), (+c), +(c), +(+(c)), ++c from t")
	require.NoError(t, err)
	fields = rs.Fields()
	for i := 0; i < 5; i++ {
		require.Equal(t, "c", fields[i].Column.Name.L)
		require.Equal(t, "c", fields[i].ColumnAsName.L)
	}
	require.Nil(t, rs.Close())

	// Test issue https://github.com/pingcap/tidb/issues/9639 .
	// Both window function and expression appear in final result field.
	tk.MustExec("set @@tidb_enable_window_function = 1")
	rs, err = tk.Exec("select 1+1, row_number() over() num from t")
	require.NoError(t, err)
	fields = rs.Fields()
	require.Equal(t, "1+1", fields[0].Column.Name.L)
	require.Equal(t, "1+1", fields[0].ColumnAsName.L)
	require.Equal(t, "num", fields[1].Column.Name.L)
	require.Equal(t, "num", fields[1].ColumnAsName.L)
	tk.MustExec("set @@tidb_enable_window_function = 0")
	require.Nil(t, rs.Close())

	rs, err = tk.Exec("select if(1,c,c) from t;")
	require.NoError(t, err)
	fields = rs.Fields()
	require.Equal(t, "if(1,c,c)", fields[0].Column.Name.L)
	// It's a compatibility issue. Should be empty instead.
	require.Equal(t, "if(1,c,c)", fields[0].ColumnAsName.L)
	require.Nil(t, rs.Close())
}

func TestSelectVar(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (d int)")
	tk.MustExec("insert into t values(1), (2), (1)")
	// This behavior is different from MySQL.
	result := tk.MustQuery("select @a, @a := d+1 from t")
	result.Check(testkit.Rows("<nil> 2", "2 3", "3 2"))
	// Test for PR #10658.
	tk.MustExec("select SQL_BIG_RESULT d from t group by d")
	tk.MustExec("select SQL_SMALL_RESULT d from t group by d")
	tk.MustExec("select SQL_BUFFER_RESULT d from t group by d")
}

func TestHistoryRead(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists history_read")
	tk.MustExec("create table history_read (a int)")
	tk.MustExec("insert history_read values (1)")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
    ON DUPLICATE KEY
    UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	// Set snapshot to a time before save point will fail.
	_, err := tk.Exec("set @@tidb_snapshot = '2006-01-01 15:04:05.999999'")
	require.True(t, terror.ErrorEqual(err, variable.ErrSnapshotTooOld), "err %v", err)
	// SnapshotTS Is not updated if check failed.
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().SnapshotTS)

	// Setting snapshot to a time in the future will fail. (One day before the 2038 problem)
	_, err = tk.Exec("set @@tidb_snapshot = '2038-01-18 03:14:07'")
	require.Regexp(t, "cannot set read timestamp to a future time", err)
	// SnapshotTS Is not updated if check failed.
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().SnapshotTS)

	curVer1, _ := store.CurrentVersion(kv.GlobalTxnScope)
	time.Sleep(time.Millisecond)
	snapshotTime := time.Now()
	time.Sleep(time.Millisecond)
	curVer2, _ := store.CurrentVersion(kv.GlobalTxnScope)
	tk.MustExec("insert history_read values (2)")
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1", "2"))
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	ctx := tk.Session().(sessionctx.Context)
	snapshotTS := ctx.GetSessionVars().SnapshotTS
	require.Greater(t, snapshotTS, curVer1.Ver)
	require.Less(t, snapshotTS, curVer2.Ver)
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1"))
	_, err = tk.Exec("insert history_read values (2)")
	require.Error(t, err)
	_, err = tk.Exec("update history_read set a = 3 where a = 1")
	require.Error(t, err)
	_, err = tk.Exec("delete from history_read where a = 1")
	require.Error(t, err)
	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1", "2"))
	tk.MustExec("insert history_read values (3)")
	tk.MustExec("update history_read set a = 4 where a = 3")
	tk.MustExec("delete from history_read where a = 1")

	time.Sleep(time.Millisecond)
	snapshotTime = time.Now()
	time.Sleep(time.Millisecond)
	tk.MustExec("alter table history_read add column b int")
	tk.MustExec("insert history_read values (8, 8), (9, 9)")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2 <nil>", "4 <nil>", "8 8", "9 9"))
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2", "4"))
	tsoStr := strconv.FormatUint(oracle.GoTimeToTS(snapshotTime), 10)

	tk.MustExec("set @@tidb_snapshot = '" + tsoStr + "'")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2", "4"))

	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2 <nil>", "4 <nil>", "8 8", "9 9"))
}

func TestHistoryReadInTxn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
    ON DUPLICATE KEY
    UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("drop table if exists his_t0, his_t1")
	tk.MustExec("create table his_t0(id int primary key, v int)")
	tk.MustExec("insert into his_t0 values(1, 10)")

	time.Sleep(time.Millisecond)
	tk.MustExec("set @a=now(6)")
	time.Sleep(time.Millisecond)
	tk.MustExec("create table his_t1(id int primary key, v int)")
	tk.MustExec("update his_t0 set v=v+1")
	time.Sleep(time.Millisecond)
	tk.MustExec("set tidb_snapshot=now(6)")
	ts2 := tk.Session().GetSessionVars().SnapshotTS
	tk.MustExec("set tidb_snapshot=''")
	time.Sleep(time.Millisecond)
	tk.MustExec("update his_t0 set v=v+1")
	tk.MustExec("insert into his_t1 values(10, 100)")

	init := func(isolation string, setSnapshotBeforeTxn bool) {
		if isolation == "none" {
			tk.MustExec("set @@tidb_snapshot=@a")
			return
		}

		if setSnapshotBeforeTxn {
			tk.MustExec("set @@tidb_snapshot=@a")
		}

		if isolation == "optimistic" {
			tk.MustExec("begin optimistic")
		} else {
			tk.MustExec(fmt.Sprintf("set @@tx_isolation='%s'", isolation))
			tk.MustExec("begin pessimistic")
		}

		if !setSnapshotBeforeTxn {
			tk.MustExec("set @@tidb_snapshot=@a")
		}
	}

	for _, isolation := range []string{
		"none", // not start an explicit txn
		"optimistic",
		"REPEATABLE-READ",
		"READ-COMMITTED",
	} {
		for _, setSnapshotBeforeTxn := range []bool{false, true} {
			t.Run(fmt.Sprintf("[%s] setSnapshotBeforeTxn[%v]", isolation, setSnapshotBeforeTxn), func(t *testing.T) {
				tk.MustExec("rollback")
				tk.MustExec("set @@tidb_snapshot=''")

				init(isolation, setSnapshotBeforeTxn)
				// When tidb_snapshot is set, should use the snapshot info schema
				tk.MustQuery("show tables like 'his_%'").Check(testkit.Rows("his_t0"))

				// When tidb_snapshot is set, select should use select ts
				tk.MustQuery("select * from his_t0").Check(testkit.Rows("1 10"))
				tk.MustQuery("select * from his_t0 where id=1").Check(testkit.Rows("1 10"))

				// When tidb_snapshot is set, write statements should not be allowed
				if isolation != "none" && isolation != "optimistic" {
					notAllowedSQLs := []string{
						"insert into his_t0 values(5, 1)",
						"delete from his_t0 where id=1",
						"update his_t0 set v=v+1",
						"select * from his_t0 for update",
						"select * from his_t0 where id=1 for update",
						"create table his_t2(id int)",
					}

					for _, sql := range notAllowedSQLs {
						err := tk.ExecToErr(sql)
						require.Errorf(t, err, "can not execute write statement when 'tidb_snapshot' is set")
					}
				}

				// After `ExecRestrictedSQL` with a specified snapshot and use current session, the original snapshot ts should not be reset
				// See issue: https://github.com/pingcap/tidb/issues/34529
				exec := tk.Session().(sqlexec.RestrictedSQLExecutor)
				rows, _, err := exec.ExecRestrictedSQL(context.TODO(), []sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(ts2), sqlexec.ExecOptionUseCurSession}, "select * from his_t0 where id=1")
				require.NoError(t, err)
				require.Equal(t, 1, len(rows))
				require.Equal(t, int64(1), rows[0].GetInt64(0))
				require.Equal(t, int64(11), rows[0].GetInt64(1))
				tk.MustQuery("select * from his_t0 where id=1").Check(testkit.Rows("1 10"))
				tk.MustQuery("show tables like 'his_%'").Check(testkit.Rows("his_t0"))

				// CLEAR
				tk.MustExec("set @@tidb_snapshot=''")

				// When tidb_snapshot is not set, should use the transaction's info schema
				tk.MustQuery("show tables like 'his_%'").Check(testkit.Rows("his_t0", "his_t1"))

				// When tidb_snapshot is not set, select should use the transaction's ts
				tk.MustQuery("select * from his_t0").Check(testkit.Rows("1 12"))
				tk.MustQuery("select * from his_t0 where id=1").Check(testkit.Rows("1 12"))
				tk.MustQuery("select * from his_t1").Check(testkit.Rows("10 100"))
				tk.MustQuery("select * from his_t1 where id=10").Check(testkit.Rows("10 100"))

				// When tidb_snapshot is not set, select ... for update should not be effected
				tk.MustQuery("select * from his_t0 for update").Check(testkit.Rows("1 12"))
				tk.MustQuery("select * from his_t0 where id=1 for update").Check(testkit.Rows("1 12"))
				tk.MustQuery("select * from his_t1 for update").Check(testkit.Rows("10 100"))
				tk.MustQuery("select * from his_t1 where id=10 for update").Check(testkit.Rows("10 100"))

				tk.MustExec("rollback")
			})
		}
	}
}

func TestCurrentTimestampValueSelection(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t,t1")

	tk.MustExec("create table t (id int, t0 timestamp null default current_timestamp, t1 timestamp(1) null default current_timestamp(1), t2 timestamp(2) null default current_timestamp(2) on update current_timestamp(2))")
	tk.MustExec("insert into t (id) values (1)")
	rs := tk.MustQuery("select t0, t1, t2 from t where id = 1")
	t0 := rs.Rows()[0][0].(string)
	t1 := rs.Rows()[0][1].(string)
	t2 := rs.Rows()[0][2].(string)
	require.Equal(t, 1, len(strings.Split(t0, ".")))
	require.Equal(t, 1, len(strings.Split(t1, ".")[1]))
	require.Equal(t, 2, len(strings.Split(t2, ".")[1]))
	tk.MustQuery("select id from t where t0 = ?", t0).Check(testkit.Rows("1"))
	tk.MustQuery("select id from t where t1 = ?", t1).Check(testkit.Rows("1"))
	tk.MustQuery("select id from t where t2 = ?", t2).Check(testkit.Rows("1"))
	time.Sleep(time.Second)
	tk.MustExec("update t set t0 = now() where id = 1")
	rs = tk.MustQuery("select t2 from t where id = 1")
	newT2 := rs.Rows()[0][0].(string)
	require.True(t, newT2 != t2)

	tk.MustExec("create table t1 (id int, a timestamp, b timestamp(2), c timestamp(3))")
	tk.MustExec("insert into t1 (id, a, b, c) values (1, current_timestamp(2), current_timestamp, current_timestamp(3))")
	rs = tk.MustQuery("select a, b, c from t1 where id = 1")
	a := rs.Rows()[0][0].(string)
	b := rs.Rows()[0][1].(string)
	d := rs.Rows()[0][2].(string)
	require.Equal(t, 1, len(strings.Split(a, ".")))
	require.Equal(t, "00", strings.Split(b, ".")[1])
	require.Equal(t, 3, len(strings.Split(d, ".")[1]))
}

func TestStrToDateBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%!') from dual`).Check(testkit.Rows("2019-01-01"))
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%f') from dual`).Check(testkit.Rows("2019-01-01 00:00:00.000000"))
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%H%i%s') from dual`).Check(testkit.Rows("2019-01-01 00:00:00"))
	tk.MustQuery(`select str_to_date('18/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('a18/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('69/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('70/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("1970-10-22"))
	tk.MustQuery(`select str_to_date('8/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2008-10-22"))
	tk.MustQuery(`select str_to_date('8/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2008-10-22"))
	tk.MustQuery(`select str_to_date('18/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('a18/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('69/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('70/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("1970-10-22"))
	tk.MustQuery(`select str_to_date('018/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("0018-10-22"))
	tk.MustQuery(`select str_to_date('2018/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('018/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18/10/22','%y0/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18/10/22','%Y0/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18a/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18a/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('20188/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2018510522','%Y5%m5%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018^10^22','%Y^%m^%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018@10@22','%Y@%m@%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018%10%22','%Y%%m%%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2018(10(22','%Y(%m(%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018\10\22','%Y\%m\%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2018=10=22','%Y=%m=%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018+10+22','%Y+%m+%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018_10_22','%Y_%m_%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('69510522','%y5%m5%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('69^10^22','%y^%m^%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('18@10@22','%y@%m@%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('18%10%22','%y%%m%%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18(10(22','%y(%m(%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('18\10\22','%y\%m\%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18+10+22','%y+%m+%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('18=10=22','%y=%m=%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('18_10_22','%y_%m_%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 11:22:33 PM', '%Y-%m-%d %r')`).Check(testkit.Rows("2020-07-04 23:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 12:22:33 AM', '%Y-%m-%d %r')`).Check(testkit.Rows("2020-07-04 00:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 12:22:33', '%Y-%m-%d %T')`).Check(testkit.Rows("2020-07-04 12:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 00:22:33', '%Y-%m-%d %T')`).Check(testkit.Rows("2020-07-04 00:22:33"))
}

func TestAddDateBuiltinWithWarnings(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@sql_mode='NO_ZERO_DATE'")
	result := tk.MustQuery(`select date_add('2001-01-00', interval -2 hour);`)
	result.Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '2001-01-00'"))
}

func TestStrToDateBuiltinWithWarnings(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@sql_mode='NO_ZERO_DATE'")
	tk.MustExec("use test")
	tk.MustQuery(`SELECT STR_TO_DATE('0000-1-01', '%Y-%m-%d');`).Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1411 Incorrect datetime value: '0000-1-01' for function str_to_date"))
}

func TestReadPartitionedTable(t *testing.T) {
	// Test three reader on partitioned table.
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists pt")
	tk.MustExec("create table pt (a int, b int, index i_b(b)) partition by range (a) (partition p1 values less than (2), partition p2 values less than (4), partition p3 values less than (6))")
	for i := 0; i < 6; i++ {
		tk.MustExec(fmt.Sprintf("insert into pt values(%d, %d)", i, i))
	}
	// Table reader
	tk.MustQuery("select * from pt order by a").Check(testkit.Rows("0 0", "1 1", "2 2", "3 3", "4 4", "5 5"))
	// Index reader
	tk.MustQuery("select b from pt where b = 3").Check(testkit.Rows("3"))
	// Index lookup
	tk.MustQuery("select a from pt where b = 3").Check(testkit.Rows("3"))
}

func TestIssue10435(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(i int, j int, k int)")
	tk.MustExec("insert into t1 VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4)")
	tk.MustExec("INSERT INTO t1 SELECT 10*i,j,5*j FROM t1 UNION SELECT 20*i,j,5*j FROM t1 UNION SELECT 30*i,j,5*j FROM t1")

	tk.MustExec("set @@session.tidb_enable_window_function=1")
	tk.MustQuery("SELECT SUM(i) OVER W FROM t1 WINDOW w AS (PARTITION BY j ORDER BY i) ORDER BY 1+SUM(i) OVER w").Check(
		testkit.Rows("1", "2", "3", "4", "11", "22", "31", "33", "44", "61", "62", "93", "122", "124", "183", "244"),
	)
}

func TestAdmin(t *testing.T) {
	var cluster testutils.Cluster
	store, clean := testkit.CreateMockStore(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("insert admin_test (c1) values (1),(2),(NULL)")

	ctx := context.Background()
	// cancel DDL jobs test
	r, err := tk.Exec("admin cancel ddl jobs 1")
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	row := req.GetRow(0)
	require.Equal(t, 2, row.Len())
	require.Equal(t, "1", row.GetString(0))
	require.Regexp(t, ".*DDL Job:1 not found", row.GetString(1))

	// show ddl test;
	r, err = tk.Exec("admin show ddl")
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	row = req.GetRow(0)
	require.Equal(t, 6, row.Len())
	txn, err := store.Begin()
	require.NoError(t, err)
	ddlInfo, err := ddl.GetDDLInfo(txn)
	require.NoError(t, err)
	require.Equal(t, ddlInfo.SchemaVer, row.GetInt64(0))
	// TODO: Pass this test.
	// rowOwnerInfos := strings.Split(row.Data[1].GetString(), ",")
	// ownerInfos := strings.Split(ddlInfo.Owner.String(), ",")
	// c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	serverInfo, err := infosync.GetServerInfoByID(ctx, row.GetString(1))
	require.NoError(t, err)
	require.Equal(t, serverInfo.IP+":"+strconv.FormatUint(uint64(serverInfo.Port), 10), row.GetString(2))
	require.Equal(t, "", row.GetString(3))
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Zero(t, req.NumRows())
	err = txn.Rollback()
	require.NoError(t, err)

	// show DDL jobs test
	r, err = tk.Exec("admin show ddl jobs")
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	row = req.GetRow(0)
	require.Equal(t, 12, row.Len())
	txn, err = store.Begin()
	require.NoError(t, err)
	historyJobs, err := ddl.GetHistoryDDLJobs(txn, ddl.DefNumHistoryJobs)
	require.Greater(t, len(historyJobs), 1)
	require.Greater(t, len(row.GetString(1)), 0)
	require.NoError(t, err)
	require.Equal(t, historyJobs[0].ID, row.GetInt64(0))
	require.NoError(t, err)

	r, err = tk.Exec("admin show ddl jobs 20")
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	row = req.GetRow(0)
	require.Equal(t, 12, row.Len())
	require.Equal(t, historyJobs[0].ID, row.GetInt64(0))
	require.NoError(t, err)

	// show DDL job queries test
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test2")
	tk.MustExec("create table admin_test2 (c1 int, c2 int, c3 int default 1, index (c1))")
	result := tk.MustQuery(`admin show ddl job queries 1, 1, 1`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`admin show ddl job queries 1, 2, 3, 4`)
	result.Check(testkit.Rows())
	historyJobs, err = ddl.GetHistoryDDLJobs(txn, ddl.DefNumHistoryJobs)
	result = tk.MustQuery(fmt.Sprintf("admin show ddl job queries %d", historyJobs[0].ID))
	result.Check(testkit.Rows(historyJobs[0].Query))
	require.NoError(t, err)

	// check table test
	tk.MustExec("create table admin_test1 (c1 int, c2 int default 1, index (c1))")
	tk.MustExec("insert admin_test1 (c1) values (21),(22)")
	r, err = tk.Exec("admin check table admin_test, admin_test1")
	require.NoError(t, err)
	require.Nil(t, r)
	// error table name
	require.Error(t, tk.ExecToErr("admin check table admin_test_error"))
	// different index values
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	require.NotNil(t, is)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("admin_test"))
	require.NoError(t, err)
	require.Len(t, tb.Indices(), 1)
	_, err = tb.Indices()[0].Create(mock.NewContext(), txn, types.MakeDatums(int64(10)), kv.IntHandle(1), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	errAdmin := tk.ExecToErr("admin check table admin_test")
	require.Error(t, errAdmin)

	if config.CheckTableBeforeDrop {
		tk.MustGetErrMsg("drop table admin_test", errAdmin.Error())

		// Drop inconsistency index.
		tk.MustExec("alter table admin_test drop index c1")
		tk.MustExec("admin check table admin_test")
	}
	// checksum table test
	tk.MustExec("create table checksum_with_index (id int, count int, PRIMARY KEY(id), KEY(count))")
	tk.MustExec("create table checksum_without_index (id int, count int, PRIMARY KEY(id))")
	r, err = tk.Exec("admin checksum table checksum_with_index, checksum_without_index")
	require.NoError(t, err)
	res := tk.ResultSetToResult(r, "admin checksum table")
	// Mocktikv returns 1 for every table/index scan, then we will xor the checksums of a table.
	// For "checksum_with_index", we have two checksums, so the result will be 1^1 = 0.
	// For "checksum_without_index", we only have one checksum, so the result will be 1.
	res.Sort().Check(testkit.Rows("test checksum_with_index 0 2 2", "test checksum_without_index 1 1 1"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (c2 BOOL, PRIMARY KEY (c2));")
	tk.MustExec("INSERT INTO t1 SET c2 = '0';")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c3 DATETIME NULL DEFAULT '2668-02-03 17:19:31';")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx2 (c3);")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c4 bit(10) default 127;")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx3 (c4);")
	tk.MustExec("admin check table t1;")

	// Test admin show ddl jobs table name after table has been droped.
	tk.MustExec("drop table if exists t1;")
	re := tk.MustQuery("admin show ddl jobs 1")
	rows := re.Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "t1", rows[0][2])

	// Test for reverse scan get history ddl jobs when ddl history jobs queue has multiple regions.
	txn, err = store.Begin()
	require.NoError(t, err)
	historyJobs, err = ddl.GetHistoryDDLJobs(txn, 20)
	require.NoError(t, err)

	// Split region for history ddl job queues.
	m := meta.NewMeta(txn)
	startKey := meta.DDLJobHistoryKey(m, 0)
	endKey := meta.DDLJobHistoryKey(m, historyJobs[0].ID)
	cluster.SplitKeys(startKey, endKey, int(historyJobs[0].ID/5))

	historyJobs2, err := ddl.GetHistoryDDLJobs(txn, 20)
	require.NoError(t, err)
	require.Equal(t, historyJobs2, historyJobs)
}

func TestForSelectScopeInUnion(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// A union B for update, the "for update" option belongs to union statement, so
	// it should works on both A and B.
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t")
	tk1.MustExec("create table t(a int)")
	tk1.MustExec("insert into t values (1)")

	tk1.MustExec("begin")
	// 'For update' would act on the second select.
	tk1.MustQuery("select 1 as a union select a from t for update")

	tk2.MustExec("use test")
	tk2.MustExec("update t set a = a + 1")

	// As tk1 use select 'for update', it should detect conflict and fail.
	_, err := tk1.Exec("commit")
	require.Error(t, err)

	tk1.MustExec("begin")
	tk1.MustQuery("select 1 as a union select a from t limit 5 for update")
	tk1.MustQuery("select 1 as a union select a from t order by a for update")

	tk2.MustExec("update t set a = a + 1")

	_, err = tk1.Exec("commit")
	require.Error(t, err)
}

func TestUnsignedDecimalOverflow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tests := []struct {
		input  interface{}
		hasErr bool
		err    string
	}{{
		-1,
		true,
		"Out of range value for column",
	}, {
		"-1.1e-1",
		true,
		"Out of range value for column",
	}, {
		-1.1,
		true,
		"Out of range value for column",
	}, {
		-0,
		false,
		"",
	},
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(10,2) unsigned)")
	for _, test := range tests {
		err := tk.ExecToErr("insert into t values (?)", test.input)
		if test.hasErr {
			require.Error(t, err)
			require.Contains(t, err.Error(), test.err)
		} else {
			require.NoError(t, err)
		}
	}

	tk.MustExec("set sql_mode=''")
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values (?)", -1)
	tk.MustQuery("select a from t limit 1").Check(testkit.Rows("0.00"))
}

func TestIndexJoinTableDualPanic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists a")
	tk.MustExec("create table a (f1 int, f2 varchar(32), primary key (f1))")
	tk.MustExec("insert into a (f1,f2) values (1,'a'), (2,'b'), (3,'c')")
	// TODO here: index join cause the data race of txn.
	tk.MustQuery("select /*+ inl_merge_join(a) */ a.* from a inner join (select 1 as k1,'k2-1' as k2) as k on a.f1=k.k1;").
		Check(testkit.Rows("1 a"))
}

func TestSortLeftJoinWithNullColumnInRightChildPanic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("insert into t1(a) select 1;")
	tk.MustQuery("select b.n from t1 left join (select a as a, null as n from t2) b on b.a = t1.a order by t1.a").
		Check(testkit.Rows("<nil>"))
}

func TestMaxOneRow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`drop table if exists t2`)
	tk.MustExec(`create table t1(a double, b double);`)
	tk.MustExec(`create table t2(a double, b double);`)
	tk.MustExec(`insert into t1 values(1, 1), (2, 2), (3, 3);`)
	tk.MustExec(`insert into t2 values(0, 0);`)
	tk.MustExec(`set @@tidb_init_chunk_size=1;`)
	rs, err := tk.Exec(`select (select t1.a from t1 where t1.a > t2.a) as a from t2;`)
	require.NoError(t, err)

	err = rs.Next(context.TODO(), rs.NewChunk(nil))
	require.Error(t, err)
	require.Equal(t, "[executor:1242]Subquery returns more than 1 row", err.Error())
	require.NoError(t, rs.Close())
}

func TestRowID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec(`create table t(a varchar(10), b varchar(10), c varchar(1), index idx(a, b, c));`)
	tk.MustExec(`insert into t values('a', 'b', 'c');`)
	tk.MustExec(`insert into t values('a', 'b', 'c');`)
	tk.MustQuery(`select b, _tidb_rowid from t use index(idx) where a = 'a';`).Check(testkit.Rows(
		`b 1`,
		`b 2`,
	))
	tk.MustExec(`begin;`)
	tk.MustExec(`select * from t for update`)
	tk.MustQuery(`select distinct b from t use index(idx) where a = 'a';`).Check(testkit.Rows(`b`))
	tk.MustExec(`commit;`)

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(5) primary key)`)
	tk.MustExec(`insert into t values('a')`)
	tk.MustQuery("select *, _tidb_rowid from t use index(`primary`) where _tidb_rowid=1").Check(testkit.Rows("a 1"))
}

func TestDoSubquery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`do 1 in (select * from t)`)
	tk.MustExec(`insert into t values(1)`)
	r, err := tk.Exec(`do 1 in (select * from t)`)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestSubqueryTableAlias(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)

	tk.MustExec("set sql_mode = ''")
	tk.MustGetErrCode("select a, b from (select 1 a) ``, (select 2 b) ``;", mysql.ErrDerivedMustHaveAlias)
	tk.MustGetErrCode("select a, b from (select 1 a) `x`, (select 2 b) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select a, b from (select 1 a), (select 2 b);", mysql.ErrDerivedMustHaveAlias)
	// ambiguous column name
	tk.MustGetErrCode("select a from (select 1 a) ``, (select 2 a) ``;", mysql.ErrDerivedMustHaveAlias)
	tk.MustGetErrCode("select a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select x.a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select a from (select 1 a), (select 2 a);", mysql.ErrDerivedMustHaveAlias)

	tk.MustExec("set sql_mode = 'oracle';")
	tk.MustQuery("select a, b from (select 1 a) ``, (select 2 b) ``;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select a, b from (select 1 a) `x`, (select 2 b) `x`;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select a, b from (select 1 a), (select 2 b);").Check(testkit.Rows("1 2"))
	// ambiguous column name
	tk.MustGetErrCode("select a from (select 1 a) ``, (select 2 a) ``;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select x.a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select a from (select 1 a), (select 2 a);", mysql.ErrNonUniq)
}

func TestSelectHashPartitionTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists th`)
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec(`create table th (a int, b int) partition by hash(a) partitions 3;`)
	defer tk.MustExec(`drop table if exists th`)
	tk.MustExec(`insert into th values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);`)
	tk.MustExec("insert into th values (-1,-1),(-2,-2),(-3,-3),(-4,-4),(-5,-5),(-6,-6),(-7,-7),(-8,-8);")
	tk.MustQuery("select b from th order by a").Check(testkit.Rows("-8", "-7", "-6", "-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5", "6", "7", "8"))
	tk.MustQuery(" select * from th where a=-2;").Check(testkit.Rows("-2 -2"))
	tk.MustQuery(" select * from th where a=5;").Check(testkit.Rows("5 5"))
}

func TestSelectView(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table view_t (a int,b int)")
	tk.MustExec("insert into view_t values(1,2)")
	tk.MustExec("create definer='root'@'localhost' view view1 as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view2(c,d) as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view3(c,d) as select a,b from view_t")
	tk.MustExec("create definer='root'@'localhost' view view4 as select * from (select * from (select * from view_t) tb1) tb;")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop table view_t;")
	tk.MustExec("create table view_t(c int,d int)")
	tk.MustGetErrMsg("select * from view1", "[planner:1356]View 'test.view1' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")
	tk.MustGetErrMsg("select * from view2", "[planner:1356]View 'test.view2' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")
	tk.MustGetErrMsg("select * from view3", plannercore.ErrViewInvalid.GenWithStackByArgs("test", "view3").Error())
	tk.MustExec("drop table view_t;")
	tk.MustExec("create table view_t(a int,b int,c int)")
	tk.MustExec("insert into view_t values(1,2,3)")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("alter table view_t drop column a")
	tk.MustExec("alter table view_t add column a int after b")
	tk.MustExec("update view_t set a=1;")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop table view_t;")
	tk.MustExec("drop view view1,view2,view3,view4;")

	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("create definer='root'@'localhost' view v as select a, first_value(a) over(rows between 1 preceding and 1 following), last_value(a) over(rows between 1 preceding and 1 following) from t")
	result := tk.MustQuery("select * from v")
	result.Check(testkit.Rows("1 1 1", "1 1 2", "2 1 2", "2 2 2"))
	tk.MustExec("drop view v;")
}

func TestSummaryFailedUpdate(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int as(-a))")
	tk.MustExec("insert into t(a) values(1), (3), (7)")
	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("set @@tidb_mem_quota_query=1")
	tk.MustMatchErrMsg("update t set t.a = t.a - 1 where t.a in (select a from t where a < 4)", "Out Of Memory Quota!.*")
	tk.MustExec("set @@tidb_mem_quota_query=1000000000")
	tk.MustQuery("select stmt_type from information_schema.statements_summary where digest_text = 'update `t` set `t` . `a` = `t` . `a` - ? where `t` . `a` in ( select `a` from `t` where `a` < ? )'").Check(testkit.Rows("Update"))
}

func TestIsFastPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, a int)")

	cases := []struct {
		sql        string
		isFastPlan bool
	}{
		{"select a from t where id=1", true},
		{"select a+id from t where id=1", true},
		{"select 1", true},
		{"select @@autocommit", true},
		{"set @@autocommit=1", true},
		{"set @a=1", true},
		{"select * from t where a=1", false},
		{"select * from t", false},
	}

	for _, ca := range cases {
		if strings.HasPrefix(ca.sql, "select") {
			tk.MustQuery(ca.sql)
		} else {
			tk.MustExec(ca.sql)
		}
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(plannercore.Plan)
		require.True(t, ok)
		ok = executor.IsFastPlan(p)
		require.Equal(t, ca.isFastPlan, ok)
	}
}
