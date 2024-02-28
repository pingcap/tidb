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

package executor

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
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	error2 "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
)

func checkFileName(s string) bool {
	files := []string{
		"config.toml",
		"debug_trace/debug_trace0.json",
		"meta.txt",
		"stats/test.t_dump_single.json",
		"schema/test.t_dump_single.schema.txt",
		"schema/schema_meta.txt",
		"table_tiflash_replica.txt",
		"variables.toml",
		"session_bindings.sql",
		"global_bindings.sql",
		"sql/sql0.sql",
		"explain.txt",
		"statsMem/test.t_dump_single.txt",
		"sql_meta.toml",
	}
	for _, f := range files {
		if strings.Compare(f, s) == 0 {
			return true
		}
	}
	return false
}

func TestBind(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestLoadStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.Error(t, tk.ExecToErr("load stats"))
	require.Error(t, tk.ExecToErr("load stats ./xxx.json"))
}

func TestPlanReplayer(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustQuery("plan replayer dump explain select * from t where a=10")
	tk.MustQuery("plan replayer dump explain select /*+ read_from_storage(tiflash[t]) */ * from t")

	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v1 as select * from t1")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v2 as select * from v1")
	tk.MustQuery("plan replayer dump explain with tmp as (select a from t1 group by t1.a) select * from tmp, t2 where t2.a=tmp.a;")
	tk.MustQuery("plan replayer dump explain select * from t1 where t1.a > (with cte1 as (select 1) select count(1) from cte1);")
	tk.MustQuery("plan replayer dump explain select * from v1")
	tk.MustQuery("plan replayer dump explain select * from v2")
	require.True(t, len(tk.Session().GetSessionVars().LastPlanReplayerToken) > 0)

	// clear the status table and assert
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustQuery("plan replayer dump explain select * from v2")
	token := tk.Session().GetSessionVars().LastPlanReplayerToken
	rows := tk.MustQuery(fmt.Sprintf("select * from mysql.plan_replayer_status where token = '%v'", token)).Rows()
	require.Len(t, rows, 1)
}

func TestPlanReplayerCaptureSEM(t *testing.T) {
	originSEM := config.GetGlobalConfig().Security.EnableSEM
	defer func() {
		config.GetGlobalConfig().Security.EnableSEM = originSEM
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("plan replayer capture '123' '123';")
	tk.MustExec("create table t(id int)")
	tk.MustQuery("plan replayer dump explain select * from t")
	tk.MustQuery("select count(*) from mysql.plan_replayer_status").Check(testkit.Rows("1"))
}

func TestPlanReplayerCapture(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("plan replayer capture '123' '123';")
	tk.MustQuery("select sql_digest, plan_digest from mysql.plan_replayer_task;").Check(testkit.Rows("123 123"))
	tk.MustGetErrMsg("plan replayer capture '123' '123';", "plan replayer capture task already exists")
	tk.MustExec("plan replayer capture remove '123' '123'")
	tk.MustQuery("select count(*) from mysql.plan_replayer_task;").Check(testkit.Rows("0"))
	tk.MustExec("create table t(id int)")
	tk.MustExec("prepare stmt from 'update t set id = ?  where id = ? + 1';")
	tk.MustExec("SET @number = 5;")
	tk.MustExec("execute stmt using @number,@number")
	_, sqlDigest := tk.Session().GetSessionVars().StmtCtx.SQLDigest()
	_, planDigest := tk.Session().GetSessionVars().StmtCtx.GetPlanDigest()
	tk.MustExec("SET @@tidb_enable_plan_replayer_capture = ON;")
	tk.MustExec("SET @@global.tidb_enable_historical_stats_for_capture='ON'")
	tk.MustExec(fmt.Sprintf("plan replayer capture '%v' '%v'", sqlDigest.String(), planDigest.String()))
	err := dom.GetPlanReplayerHandle().CollectPlanReplayerTask()
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/shouldDumpStats", "return(true)"))
	defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/shouldDumpStats"))
	tk.MustExec("execute stmt using @number,@number")
	task := dom.GetPlanReplayerHandle().DrainTask()
	require.NotNil(t, task)
}

func TestPlanReplayerContinuesCapture(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@global.tidb_enable_historical_stats='OFF'")
	_, err := tk.Exec("set @@global.tidb_enable_plan_replayer_continuous_capture='ON'")
	require.Error(t, err)
	require.Equal(t, err.Error(), "tidb_enable_historical_stats should be enabled before enabling tidb_enable_plan_replayer_continuous_capture")

	tk.MustExec("set @@global.tidb_enable_historical_stats='ON'")
	tk.MustExec("set @@global.tidb_enable_plan_replayer_continuous_capture='ON'")

	prHandle := dom.GetPlanReplayerHandle()
	tk.MustExec("delete from mysql.plan_replayer_status;")
	tk.MustExec("use test")
	tk.MustExec("create table t(id int);")
	tk.MustExec("set @@tidb_enable_plan_replayer_continuous_capture = 'ON'")
	tk.MustQuery("select * from t;")
	task := prHandle.DrainTask()
	require.NotNil(t, task)
	worker := prHandle.GetWorker()
	success := worker.HandleTask(task)
	require.True(t, success)
	tk.MustQuery("select count(*) from mysql.plan_replayer_status").Check(testkit.Rows("1"))
}

func TestShow(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
		"Config Server Admin To use SHOW CONFIG and SET CONFIG statements",
		"Create Databases,Tables,Indexes To create new databases and tables",
		"Create routine Databases To use CREATE FUNCTION/PROCEDURE",
		"Create role Server Admin To create new roles",
		"Create temporary tables Databases To use CREATE TEMPORARY TABLE",
		"Create view Tables To create new views",
		"Create user Server Admin To create new users",
		"Delete Tables To delete existing rows",
		"Drop Databases,Tables To drop databases, tables, and views",
		"Drop role Server Admin To drop roles",
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
		"RESOURCE_GROUP_ADMIN Server Admin ",
	))
	require.Len(t, tk.MustQuery("show table status").Rows(), 1)
}

// TestSelectBackslashN Issue 3685.
func TestSelectBackslashN(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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

func TestUpdateClustered(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestClusterIndexOuterJoinElimination(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_dump_single")
	tk.MustExec("create table t_dump_single(a int)")
	res := tk.MustQuery("plan replayer dump explain select * from t_dump_single")
	path := testdata.ConvertRowsToStrings(res.Rows())

	reader, err := zip.OpenReader(filepath.Join(replayer.GetPlanReplayerDirName(), path[0]))
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	for _, file := range reader.File {
		require.True(t, checkFileName(file.Name), file.Name)
	}
}

func TestTimezonePushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
		count++
		dagReq := new(tipb.DAGRequest)
		require.NoError(t, proto.Unmarshal(req.Data, dagReq))
		require.Equal(t, systemTZ.String(), dagReq.GetTimeZoneName())
	})
	rs, err := tk.Session().Execute(ctx1, `select * from t where ts = "2018-09-13 10:02:06"`)
	require.NoError(t, err)
	rs[0].Close()

	tk.MustExec(`set time_zone="System"`)
	rs, err = tk.Session().Execute(ctx1, `select * from t where ts = "2018-09-13 10:02:06"`)
	require.NoError(t, err)
	rs[0].Close()

	require.Equal(t, 2, count) // Make sure the hook function is called.
}

func TestNotFillCacheFlag(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestExecutorBit(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestCheckIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

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

	alloc := autoid.NewAllocator(dom, dbInfo.ID, tbInfo.ID, false, autoid.RowIDAllocType)
	tb, err := tables.TableFromMeta(autoid.NewAllocators(false, alloc), tbInfo)
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
	sc := stmtctx.NewStmtCtxWithTimeZone(time.Local)

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
	require.Equal(t, "[admin:8223]data inconsistency in table: t, index: c, handle: 3, index-values:\"handle: 3, values: [KindInt64 30]\" != record-values:\"\"", err.Error())

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
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: t, index: c, handle: 3, index-values:\"handle: 3, values: [KindInt64 30]\" != record-values:\"\"")

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
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: t, index: c, handle: 2, index-values:\"\" != record-values:\"handle: 2, values: [KindInt64 20]\"")

	// TODO: pass the case below：
	// set data to:
	// index     data (handle, data): (1, 10), (4, 40), (2, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
}

func setColValue(t *testing.T, txn kv.Transaction, key kv.Key, v types.Datum) {
	row := []types.Datum{v, {}}
	colIDs := []int64{2, 3}
	sc := stmtctx.NewStmtCtxWithTimeZone(time.Local)
	rd := rowcodec.Encoder{Enable: true}
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil, &rd)
	require.NoError(t, err)
	err = txn.Set(key, value)
	require.NoError(t, err)
}

func TestTimestampTimeZone(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	datumTimeIn8, err := expression.GetTimeValue(tk.Session(), timeIn8, mysql.TypeTimestamp, 0, nil)
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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
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
	store := testkit.CreateMockStore(t)

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

func TestPartitionHashCode(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

// this is from jira issue #5856
func TestInsertValuesWithSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

// fix issue https://github.com/pingcap/tidb/issues/32871
func TestBitColumnIn(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bit(16), key id(id))")
	tk.MustExec("insert into t values (65)")
	tk.MustQuery("select * from t where id not in (-1,2)").Check(testkit.Rows("\x00A"))
	tk.MustGetErrMsg(
		"select * from t where id in (-1, -2)",
		"[expression:1582]Incorrect parameter count in the call to native function 'in'")
}

func TestIndexLookupRuntimeStats(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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

func TestPrevStmtDesensitization(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(fmt.Sprintf("set @@session.%v=1", variable.TiDBRedactLog))
	defer tk.MustExec(fmt.Sprintf("set @@session.%v=0", variable.TiDBRedactLog))
	tk.MustExec("create table t (a int, unique key (a))")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1),(2)")
	require.Equal(t, "insert into `t` values ( ... )", tk.Session().GetSessionVars().PrevStmt.String())
	tk.MustGetErrMsg("insert into t values (1)", `[kv:1062]Duplicate entry '?' for key 't.a'`)
}

func TestIssue19148(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestOOMActionPriority(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable(), plannercore.MockUnsignedTable()})
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
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
		"select /*+ stream_agg() */ count(f) from t",
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

		err = sessiontxn.GetTxnManager(tk.Session()).OnStmtStart(context.TODO(), stmt)
		require.NoError(t, err, comment)

		executorBuilder := executor.NewMockExecutorBuilderForTest(tk.Session(), is, nil)

		p, _, _ := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NotNil(t, p)

		// This for loop level traverses the plan tree to get which operators are covered.
		var hasCTE bool
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
					hasCTE = true
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

		if hasCTE {
			// Normally CTEStorages will be setup in ResetContextOfStmt.
			// But the following case call e.Close() directly, instead of calling session.ExecStmt(), which calls ResetContextOfStmt.
			// So need to setup CTEStorages manually.
			tk.Session().GetSessionVars().StmtCtx.CTEStorageMap = map[int]*executor.CTEStorages{}
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

func TestOOMPanicAction(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

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
	require.Regexp(t, memory.PanicMemoryExceedWarnMsg, err.Error())

	// Test insert from select oom panic.
	tk.MustExec("drop table if exists t,t1")
	tk.MustExec("create table t (a bigint);")
	tk.MustExec("create table t1 (a bigint);")
	tk.MustExec("set @@tidb_mem_quota_query=200;")
	tk.MustMatchErrMsg("insert into t1 values (1),(2),(3),(4),(5);", memory.PanicMemoryExceedWarnMsg)
	tk.MustMatchErrMsg("replace into t1 values (1),(2),(3),(4),(5);", memory.PanicMemoryExceedWarnMsg)
	tk.MustExec("set @@tidb_mem_quota_query=10000")
	tk.MustExec("insert into t1 values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@tidb_mem_quota_query=10;")
	tk.MustMatchErrMsg("insert into t select a from t1 order by a desc;", memory.PanicMemoryExceedWarnMsg)
	tk.MustMatchErrMsg("replace into t select a from t1 order by a desc;", memory.PanicMemoryExceedWarnMsg)

	tk.MustExec("set @@tidb_mem_quota_query=10000")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5);")
	// Set the memory quota to 244 to make this SQL panic during the DeleteExec
	// instead of the TableReaderExec.
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	tk.MustMatchErrMsg("delete from t", memory.PanicMemoryExceedWarnMsg)

	tk.MustExec("set @@tidb_mem_quota_query=10000;")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	tk.MustMatchErrMsg("delete t, t1 from t join t1 on t.a = t1.a", memory.PanicMemoryExceedWarnMsg)

	tk.MustExec("set @@tidb_mem_quota_query=100000;")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1),(2),(3)")
	// set the memory to quota to make the SQL panic during UpdateExec instead
	// of TableReader.
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	tk.MustMatchErrMsg("update t set a = 4", memory.PanicMemoryExceedWarnMsg)
}

func TestPointGetPreparedPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	tk.Session().GetSessionVars().PreparedStmts[pspk1Id].(*plannercore.PlanCacheStmt).StmtCacheable = false
	pspk2Id, _, _, err := tk.Session().PrepareStmt("select * from t where ? = a ")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[pspk2Id].(*plannercore.PlanCacheStmt).StmtCacheable = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	// unique index
	psuk1Id, _, _, err := tk.Session().PrepareStmt("select * from t where b = ? ")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[psuk1Id].(*plannercore.PlanCacheStmt).StmtCacheable = false

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// test schema changed, cached plan should be invalidated
	tk.MustExec("alter table t add column col4 int default 10 after c")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	tk.MustExec("alter table t drop index k_b")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	tk.MustExec(`insert into t values(4, 3, 3, 11)`)
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10", "4 3 3 11"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	tk.MustExec("delete from t where a = 4")
	tk.MustExec("alter table t add index k_b(b)")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// use pk again
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))
}

func TestPointGetPreparedPlanWithCommitMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
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
	tk1.Session().GetSessionVars().PreparedStmts[pspk1Id].(*plannercore.PlanCacheStmt).StmtCacheable = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// update rows
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use ps_text")
	tk2.MustExec("update t set c = c + 10 where c = 1")

	// try to point get again
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// try to update in session 1
	tk1.MustExec("update t set c = c + 10 where c = 1")
	err = tk1.ExecToErr("commit")
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("error: %s", err))

	// verify
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 11"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 11"))
}

func TestPointUpdatePreparedPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	tk.Session().GetSessionVars().PreparedStmts[updateID1].(*plannercore.PlanCacheStmt).StmtCacheable = false
	require.Equal(t, 1, pc)
	updateID2, pc, _, err := tk.Session().PrepareStmt(`update t set c = c + 2 where ? = a`)
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[updateID2].(*plannercore.PlanCacheStmt).StmtCacheable = false
	require.Equal(t, 1, pc)

	ctx := context.Background()
	// first time plan generated
	rs, err := tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	// using the generated plan but with different params
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// updateID2
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID2, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 8"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID2, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	// unique index
	updUkID1, _, _, err := tk.Session().PrepareStmt(`update t set c = c + 10 where b = ?`)
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[updUkID1].(*plannercore.PlanCacheStmt).StmtCacheable = false
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 20"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 30"))

	// test schema changed, cached plan should be invalidated
	tk.MustExec("alter table t add column col4 int default 10 after c")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 31 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 32 10"))

	tk.MustExec("alter table t drop index k_b")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 42 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 52 10"))

	tk.MustExec("alter table t add unique index k_b(b)")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 62 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 72 10"))

	tk.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1 10"))
	tk.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2 10"))
}

func TestPointUpdatePreparedPlanWithCommitMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
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
	tk1.Session().GetSessionVars().PreparedStmts[updateID1].(*plannercore.PlanCacheStmt).StmtCacheable = false
	require.NoError(t, err)

	// first time plan generated
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
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
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
	tk1.MustExec("commit")

	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
}

func TestApplyCache(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values (1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("analyze table t;")
	result := tk.MustQuery("explain analyze SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t;")
	require.Contains(t, result.Rows()[1][0], "Apply")
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
	require.Contains(t, result.Rows()[1][0], "Apply")
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
	store := testkit.CreateMockStore(t)

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

func TestIssue24933(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
		cnt := executor.GetResultRowsCount(tk.Session().GetSessionVars().StmtCtx, p)
		require.Equal(t, ca.row, cnt, fmt.Sprintf("sql: %v", ca.sql))
	}
}

func TestAdminShowDDLJobs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_admin_show_ddl_jobs")
	tk.MustExec("use test_admin_show_ddl_jobs")
	tk.MustExec("create table t (a int);")

	re := tk.MustQuery("admin show ddl jobs 1")
	row := re.Rows()[0]
	require.Equal(t, "test_admin_show_ddl_jobs", row[1])
	jobID, err := strconv.Atoi(row[0].(string))
	require.NoError(t, err)

	job, err := ddl.GetHistoryJobByID(tk.Session(), int64(jobID))
	require.NoError(t, err)
	require.NotNil(t, job)
	// Test for compatibility. Old TiDB version doesn't have SchemaName field, and the BinlogInfo maybe nil.
	// See PR: 11561.
	job.BinlogInfo = nil
	job.SchemaName = ""
	err = sessiontxn.NewTxnInStmt(context.Background(), tk.Session())
	require.NoError(t, err)
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	err = meta.NewMeta(txn).AddHistoryDDLJob(job, true)
	require.NoError(t, err)
	tk.Session().StmtCommit(context.Background())

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

func TestAdminShowDDLJobsRowCount(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Test for issue: https://github.com/pingcap/tidb/issues/25968
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id bigint key,b int);")
	tk.MustExec("split table t by (10),(20),(30);")
	tk.MustExec("insert into t values (0,0),(10,10),(20,20),(30,30);")
	tk.MustExec("alter table t add index idx1(b);")
	require.Equal(t, "4", tk.MustQuery("admin show ddl jobs 1").Rows()[0][7])

	tk.MustExec("insert into t values (1,0),(2,10),(3,20),(4,30);")
	tk.MustExec("alter table t add index idx2(b);")
	require.Equal(t, "8", tk.MustQuery("admin show ddl jobs 1").Rows()[0][7])
}

func TestAdminShowDDLJobsInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS admin_checksum_partition_test;")
	tk.MustExec("CREATE TABLE admin_checksum_partition_test (a INT) PARTITION BY HASH(a) PARTITIONS 4;")
	tk.MustExec("INSERT INTO admin_checksum_partition_test VALUES (1), (2);")

	r := tk.MustQuery("ADMIN CHECKSUM TABLE admin_checksum_partition_test;")
	r.Check(testkit.Rows("test admin_checksum_partition_test 1 5 5"))
}

func TestUnion2(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

	tk.MustGetDBError("(select a from t order by a) union all select a from t limit 1 union all select a from t limit 1", plannercore.ErrWrongUsage)

	tk.MustExec("(select a from t limit 1) union all select a from t limit 1")
	tk.MustExec("(select a from t order by a) union all select a from t order by a")

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
	tk.MustGetErrMsg("(select a,b from t1 limit 2) union all (select a,b from t2 order by a limit 1) order by t1.b",
		"[planner:1250]Table 't1' from one of the SELECTs cannot be used in global ORDER clause")

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
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	// Setting tx_read_ts to a time in the future will fail. (One day before the 2038 problem)
	tk.MustGetErrMsg("set @@tx_read_ts = '2038-01-18 03:14:07'", "cannot set read timestamp to a future time")
	// TxnReadTS Is not updated if check failed.
	require.Zero(t, tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
}

func TestSQLMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a tinyint not null)")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	tk.ExecToErr("insert t values ()")
	tk.ExecToErr("insert t values ('1000')")

	tk.MustExec("create table if not exists tdouble (a double(3,2))")
	tk.ExecToErr("insert tdouble values (10.23)")

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
	tk.ExecToErr("insert t2 values ('abcd')")
	// Restore original global strict mode.
	tk.MustExec("set @@global.sql_mode = 'STRICT_TRANS_TABLES'")
}

func TestTableScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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
		err = plannercore.Preprocess(context.Background(), ctx, stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, preprocessorReturn.InfoSchema)
		require.NoError(t, err)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		require.NoError(t, err)
		require.Equal(t, result, ret)
	}
}

func TestPointGetOrderby(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (i int key)")
	require.Equal(t, tk.ExecToErr("select * from t where i = 1 order by j limit 10;").Error(), "[planner:1054]Unknown column 'j' in 'order clause'")
}

func TestClusteredIndexIsPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
		err = plannercore.Preprocess(context.Background(), ctx, stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, preprocessorReturn.InfoSchema)
		require.NoError(t, err)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		require.NoError(t, err)
		require.Equal(t, result, ret)
	}
}

func TestColumnName(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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
	tk.MustExecToErr("insert history_read values (2)")
	tk.MustExecToErr("update history_read set a = 3 where a = 1")
	tk.MustExecToErr("delete from history_read where a = 1")
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
	store := testkit.CreateMockStore(t)
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
				ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
				rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(ts2), sqlexec.ExecOptionUseCurSession}, "select * from his_t0 where id=1")
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
	store := testkit.CreateMockStore(t)
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

func TestAddDateBuiltinWithWarnings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@sql_mode='NO_ZERO_DATE'")
	result := tk.MustQuery(`select date_add('2001-01-00', interval -2 hour);`)
	result.Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '2001-01-00'"))
}

func TestStrToDateBuiltinWithWarnings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@sql_mode='NO_ZERO_DATE'")
	tk.MustExec("use test")
	tk.MustQuery(`SELECT STR_TO_DATE('0000-1-01', '%Y-%m-%d');`).Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1411 Incorrect datetime value: '0000-1-01' for function str_to_date"))
	tk.MustQuery("SELECT CAST('4#,8?Q' AS DATE);").Check(testkit.Rows("<nil>"))
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(
		`Warning 8034 Incorrect datetime value: '4#,8?Q'`,
	))
	tk.MustExec("CREATE TABLE t1 (c1 INT, c2 TEXT);")
	tk.MustExec("INSERT INTO t1 VALUES (1833458842, '0.3503490908550797');")
	tk.MustQuery(`SELECT  CAST(t1.c2 AS DATE) FROM t1`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(
		`Warning 1292 Incorrect datetime value: '0.3503490908550797'`,
	))
}

func TestAdmin(t *testing.T) {
	var cluster testutils.Cluster
	store := testkit.CreateMockStore(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
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
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("begin")
	sess := tk.Session()
	ddlInfo, err := ddl.GetDDLInfo(sess)
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
	tk.MustExec("rollback")

	// show DDL jobs test
	r, err = tk.Exec("admin show ddl jobs")
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	row = req.GetRow(0)
	require.Equal(t, 12, row.Len())
	txn, err := store.Begin()
	require.NoError(t, err)
	historyJobs, err := ddl.GetLastNHistoryDDLJobs(meta.NewMeta(txn), ddl.DefNumHistoryJobs)
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
	historyJobs, err = ddl.GetLastNHistoryDDLJobs(meta.NewMeta(txn), ddl.DefNumHistoryJobs)
	result = tk.MustQuery(fmt.Sprintf("admin show ddl job queries %d", historyJobs[0].ID))
	result.Check(testkit.Rows(historyJobs[0].Query))
	require.NoError(t, err)

	// show DDL job queries with range test
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test2")
	tk.MustExec("create table admin_test2 (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("drop table if exists admin_test3")
	tk.MustExec("create table admin_test3 (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("drop table if exists admin_test4")
	tk.MustExec("create table admin_test4 (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("drop table if exists admin_test5")
	tk.MustExec("create table admin_test5 (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("drop table if exists admin_test6")
	tk.MustExec("create table admin_test6 (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("drop table if exists admin_test7")
	tk.MustExec("create table admin_test7 (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("drop table if exists admin_test8")
	tk.MustExec("create table admin_test8 (c1 int, c2 int, c3 int default 1, index (c1))")
	historyJobs, err = ddl.GetLastNHistoryDDLJobs(meta.NewMeta(txn), ddl.DefNumHistoryJobs)
	result = tk.MustQuery(`admin show ddl job queries limit 3`)
	result.Check(testkit.Rows(fmt.Sprintf("%d %s", historyJobs[0].ID, historyJobs[0].Query), fmt.Sprintf("%d %s", historyJobs[1].ID, historyJobs[1].Query), fmt.Sprintf("%d %s", historyJobs[2].ID, historyJobs[2].Query)))
	result = tk.MustQuery(`admin show ddl job queries limit 3, 2`)
	result.Check(testkit.Rows(fmt.Sprintf("%d %s", historyJobs[3].ID, historyJobs[3].Query), fmt.Sprintf("%d %s", historyJobs[4].ID, historyJobs[4].Query)))
	result = tk.MustQuery(`admin show ddl job queries limit 3 offset 2`)
	result.Check(testkit.Rows(fmt.Sprintf("%d %s", historyJobs[2].ID, historyJobs[2].Query), fmt.Sprintf("%d %s", historyJobs[3].ID, historyJobs[3].Query), fmt.Sprintf("%d %s", historyJobs[4].ID, historyJobs[4].Query)))
	require.NoError(t, err)

	// check situations when `admin show ddl job 20` happens at the same time with new DDLs being executed
	var wg sync.WaitGroup
	wg.Add(2)
	flag := true
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			tk.MustExec("drop table if exists admin_test9")
			tk.MustExec("create table admin_test9 (c1 int, c2 int, c3 int default 1, index (c1))")
		}
	}()
	go func() {
		// check that the result set has no duplication
		defer wg.Done()
		for i := 0; i < 10; i++ {
			result := tk2.MustQuery(`admin show ddl job queries 20`)
			rows := result.Rows()
			rowIDs := make(map[string]struct{})
			for _, row := range rows {
				rowID := fmt.Sprintf("%v", row[0])
				if _, ok := rowIDs[rowID]; ok {
					flag = false
					return
				}
				rowIDs[rowID] = struct{}{}
			}
		}
	}()
	wg.Wait()
	require.True(t, flag)

	// check situations when `admin show ddl job queries limit 3 offset 2` happens at the same time with new DDLs being executed
	var wg2 sync.WaitGroup
	wg2.Add(2)
	flag = true
	go func() {
		defer wg2.Done()
		for i := 0; i < 10; i++ {
			tk.MustExec("drop table if exists admin_test9")
			tk.MustExec("create table admin_test9 (c1 int, c2 int, c3 int default 1, index (c1))")
		}
	}()
	go func() {
		// check that the result set has no duplication
		defer wg2.Done()
		for i := 0; i < 10; i++ {
			result := tk2.MustQuery(`admin show ddl job queries limit 3 offset 2`)
			rows := result.Rows()
			rowIDs := make(map[string]struct{})
			for _, row := range rows {
				rowID := fmt.Sprintf("%v", row[0])
				if _, ok := rowIDs[rowID]; ok {
					flag = false
					return
				}
				rowIDs[rowID] = struct{}{}
			}
		}
	}()
	wg2.Wait()
	require.True(t, flag)

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
	historyJobs, err = ddl.GetLastNHistoryDDLJobs(meta.NewMeta(txn), 20)
	require.NoError(t, err)

	// Split region for history ddl job queues.
	m := meta.NewMeta(txn)
	startKey := meta.DDLJobHistoryKey(m, 0)
	endKey := meta.DDLJobHistoryKey(m, historyJobs[0].ID)
	cluster.SplitKeys(startKey, endKey, int(historyJobs[0].ID/5))

	historyJobs2, err := ddl.GetLastNHistoryDDLJobs(meta.NewMeta(txn), 20)
	require.NoError(t, err)
	require.Equal(t, historyJobs2, historyJobs)
}

func TestForSelectScopeInUnion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
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
	store := testkit.CreateMockStore(t)

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

func TestMaxOneRow(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestDoSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestSummaryFailedUpdate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	tk.MustQuery("select variable_value from mysql.GLOBAL_VARIABLES where variable_name = 'tidb_mem_oom_action'").Check(testkit.Rows("LOG"))

	tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("set @@tidb_mem_quota_query=1")
	tk.MustMatchErrMsg("update t set t.a = t.a - 1 where t.a in (select a from t where a < 4)", memory.PanicMemoryExceedWarnMsg)
	tk.MustExec("set @@tidb_mem_quota_query=1000000000")
	tk.MustQuery("select stmt_type from information_schema.statements_summary where digest_text = 'update `t` set `t` . `a` = `t` . `a` - ? where `t` . `a` in ( select `a` from `t` where `a` < ? )'").Check(testkit.Rows("Update"))
}

func TestIsFastPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestCountDistinctJSON(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(j JSON)")
	tk.MustExec("insert into t values('2010')")
	tk.MustExec("insert into t values('2011')")
	tk.MustExec("insert into t values('2012')")
	tk.MustExec("insert into t values('2010.000')")
	tk.MustExec("insert into t values(cast(? as JSON))", uint64(math.MaxUint64))
	tk.MustExec("insert into t values(cast(? as JSON))", float64(math.MaxUint64))

	tk.MustQuery("select count(distinct j) from t").Check(testkit.Rows("5"))
}

func TestHashJoinJSON(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int(11), j JSON, d DOUBLE)")
	tk.MustExec("insert into t values(0, '2010', 2010)")
	tk.MustExec("insert into t values(1, '2011', 2011)")
	tk.MustExec("insert into t values(2, '2012', 2012)")
	tk.MustExec("insert into t values(3, cast(? as JSON), ?)", uint64(math.MaxUint64), float64(math.MaxUint64))

	tk.MustQuery("select /*+inl_hash_join(t2)*/ t1.id, t2.id from t t1 join t t2 on t1.j = t2.d;").Check(testkit.Rows("0 0", "1 1", "2 2"))
}

func TestTableLockPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("create user 'testuser'@'localhost'")
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "testuser", Hostname: "localhost"}, nil, nil, nil))
	tk2.MustGetErrMsg("LOCK TABLE test.t WRITE", "[planner:1044]Access denied for user 'testuser'@'localhost' to database 'test'")
	tk.MustExec("GRANT LOCK TABLES ON test.* to 'testuser'@'localhost'")
	tk2.MustGetErrMsg("LOCK TABLE test.t WRITE", "[planner:1142]SELECT command denied to user 'testuser'@'localhost' for table 't'")
	tk.MustExec("REVOKE ALL ON test.* FROM 'testuser'@'localhost'")
	tk.MustExec("GRANT SELECT ON test.* to 'testuser'@'localhost'")
	tk2.MustGetErrMsg("LOCK TABLE test.t WRITE", "[planner:1044]Access denied for user 'testuser'@'localhost' to database 'test'")
	tk.MustExec("GRANT LOCK TABLES ON test.* to 'testuser'@'localhost'")
	tk2.MustExec("LOCK TABLE test.t WRITE")

	tk.MustExec("create database test2")
	tk.MustExec("create table test2.t2(a int)")
	tk2.MustGetErrMsg("LOCK TABLE test.t WRITE, test2.t2 WRITE", "[planner:1044]Access denied for user 'testuser'@'localhost' to database 'test2'")
	tk.MustExec("GRANT LOCK TABLES ON test2.* to 'testuser'@'localhost'")
	tk2.MustGetErrMsg("LOCK TABLE test.t WRITE, test2.t2 WRITE", "[planner:1142]SELECT command denied to user 'testuser'@'localhost' for table 't2'")
	tk.MustExec("GRANT SELECT ON test2.* to 'testuser'@'localhost'")
	tk2.MustExec("LOCK TABLE test.t WRITE, test2.t2 WRITE")
	tk.MustExec("LOCK TABLE test.t WRITE, test2.t2 WRITE")
}

func TestGlobalMemoryControl2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk0 := testkit.NewTestKit(t, store)
	tk0.MustExec("set global tidb_mem_oom_action = 'cancel'")
	tk0.MustExec("set global tidb_server_memory_limit = 1 << 30")
	tk0.MustExec("set global tidb_server_memory_limit_sess_min_size = 128")

	sm := &testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk0.Session().ShowProcess()},
	}
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk0.MustExec("use test")
	tk0.MustExec("create table t(a int)")
	tk0.MustExec("insert into t select 1")
	for i := 1; i <= 8; i++ {
		tk0.MustExec("insert into t select * from t") // 256 Lines
	}

	var test []int
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond) // Make sure the sql is running.
		test = make([]int, 128<<20)        // Keep 1GB HeapInuse
		wg.Done()
	}()
	sql := "select * from t t1 join t t2 join t t3 on t1.a=t2.a and t1.a=t3.a order by t1.a;" // Need 500MB
	require.True(t, strings.Contains(tk0.QueryToErr(sql).Error(), memory.PanicMemoryExceedWarnMsg))
	require.Equal(t, tk0.Session().GetSessionVars().DiskTracker.MaxConsumed(), int64(0))
	wg.Wait()
	test[0] = 0
	runtime.GC()
}

func TestCompileOutOfMemoryQuota(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Test for issue: https://github.com/pingcap/tidb/issues/38322
	defer tk.MustExec("set global tidb_mem_oom_action = DEFAULT")
	tk.MustExec("set global tidb_mem_oom_action='CANCEL'")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create table t1(a int, c int, index idx(a))")
	tk.MustExec("set tidb_mem_quota_query=10")
	err := tk.ExecToErr("select t.a, t1.a from t use index(idx), t1 use index(idx) where t.a = t1.a")
	require.Contains(t, err.Error(), memory.PanicMemoryExceedWarnMsg)
}

func TestSignalCheckpointForSort(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/SignalCheckpointForSort", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/SignalCheckpointForSort"))
	}()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/SignalCheckpointForSort", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/SignalCheckpointForSort"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	defer tk.MustExec("set global tidb_mem_oom_action = DEFAULT")
	tk.MustExec("set global tidb_mem_oom_action='CANCEL'")
	tk.MustExec("set tidb_mem_quota_query = 100000000")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d)", i))
	}
	tk.Session().GetSessionVars().ConnectionID = 123456

	err := tk.QueryToErr("select * from t order by a")
	require.Contains(t, err.Error(), memory.PanicMemoryExceedWarnMsg)
}

func TestSessionRootTrackerDetach(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("set global tidb_mem_oom_action = DEFAULT")
	tk.MustExec("set global tidb_mem_oom_action='CANCEL'")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create table t1(a int, c int, index idx(a))")
	tk.MustExec("set tidb_mem_quota_query=10")
	tk.MustContainErrMsg("select /*+hash_join(t1)*/ t.a, t1.a from t use index(idx), t1 use index(idx) where t.a = t1.a", memory.PanicMemoryExceedWarnMsg)
	tk.MustExec("set tidb_mem_quota_query=1000")
	rs, err := tk.Exec("select /*+hash_join(t1)*/ t.a, t1.a from t use index(idx), t1 use index(idx) where t.a = t1.a")
	require.NoError(t, err)
	require.NotNil(t, tk.Session().GetSessionVars().MemTracker.GetFallbackForTest(false))
	err = rs.Close()
	require.NoError(t, err)
	require.Nil(t, tk.Session().GetSessionVars().MemTracker.GetFallbackForTest(false))
}

func TestPlanReplayerDumpTPCDS(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table catalog_sales
(
    cs_sold_date_sk           int                       ,
    cs_sold_time_sk           int                       ,
    cs_ship_date_sk           int                       ,
    cs_bill_customer_sk       int                       ,
    cs_bill_cdemo_sk          int                       ,
    cs_bill_hdemo_sk          int                       ,
    cs_bill_addr_sk           int                       ,
    cs_ship_customer_sk       int                       ,
    cs_ship_cdemo_sk          int                       ,
    cs_ship_hdemo_sk          int                       ,
    cs_ship_addr_sk           int                       ,
    cs_call_center_sk         int                       ,
    cs_catalog_page_sk        int                       ,
    cs_ship_mode_sk           int                       ,
    cs_warehouse_sk           int                       ,
    cs_item_sk                int               not null,
    cs_promo_sk               int                       ,
    cs_order_number           int               not null,
    cs_quantity               int                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  ,
    primary key (cs_item_sk, cs_order_number)
);`)
	tk.MustExec(`create table store_sales
(
    ss_sold_date_sk           int                       ,
    ss_sold_time_sk           int                       ,
    ss_item_sk                int               not null,
    ss_customer_sk            int                       ,
    ss_cdemo_sk               int                       ,
    ss_hdemo_sk               int                       ,
    ss_addr_sk                int                       ,
    ss_store_sk               int                       ,
    ss_promo_sk               int                       ,
    ss_ticket_number          int               not null,
    ss_quantity               int                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)
);`)
	tk.MustExec(`create table date_dim
(
    d_date_sk                 int               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                          ,
    d_month_seq               int                       ,
    d_week_seq                int                       ,
    d_quarter_seq             int                       ,
    d_year                    int                       ,
    d_dow                     int                       ,
    d_moy                     int                       ,
    d_dom                     int                       ,
    d_qoy                     int                       ,
    d_fy_year                 int                       ,
    d_fy_quarter_seq          int                       ,
    d_fy_week_seq             int                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               int                       ,
    d_last_dom                int                       ,
    d_same_day_ly             int                       ,
    d_same_day_lq             int                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       ,
    primary key (d_date_sk)
);`)
	tk.MustQuery(`plan replayer dump explain with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales,date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1212 and 1212 + 11
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from catalog_sales,date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1212 and 1212 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci left join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
UNION
 select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci right join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
limit 100;`)
}

func TestProcessInfoOfSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (i int, j int);")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tk.MustQuery("select 1, (select sleep(count(1) + 2) from t);")
		wg.Done()
	}()
	time.Sleep(time.Second)
	tk2.MustQuery("select 1 from information_schema.processlist where TxnStart != '' and info like 'select%sleep% from t%'").Check(testkit.Rows("1"))
	wg.Wait()
}

func TestIssue38756(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int)")
	tk.MustExec("insert into t values (1), (2), (3)")
	tk.MustQuery("SELECT SQRT(1) FROM t").Check(testkit.Rows("1", "1", "1"))
	tk.MustQuery("(SELECT DISTINCT SQRT(1) FROM t)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT DISTINCT cast(1 as double) FROM t").Check(testkit.Rows("1"))
}

func TestIssues49377(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table employee (employee_id int, name varchar(20), dept_id int)")
	tk.MustExec("insert into employee values (1, 'Furina', 1), (2, 'Klee', 1), (3, 'Eula', 1), (4, 'Diluc', 2), (5, 'Tartaglia', 2)")

	tk.MustQuery("select 1,1,1 union all ( " +
		"(select * from employee where dept_id = 1) " +
		"union all  " +
		"(select * from employee where dept_id = 1 order by employee_id) " +
		"order by 1 limit 1 " +
		");").Sort().Check(testkit.Rows("1 1 1", "1 Furina 1"))

	tk.MustQuery("select 1,1,1 union all ( " +
		"(select * from employee where dept_id = 1) " +
		"union all  " +
		"(select * from employee where dept_id = 1 order by employee_id) " +
		"order by 1" +
		");").Sort().Check(testkit.Rows("1 1 1", "1 Furina 1", "1 Furina 1", "2 Klee 1", "2 Klee 1", "3 Eula 1", "3 Eula 1"))

	tk.MustQuery("select * from employee where dept_id = 1 " +
		"union all " +
		"(select * from employee where dept_id = 1 order by employee_id) " +
		"union all" +
		"(" +
		"select * from employee where dept_id = 1 " +
		"union all " +
		"(select * from employee where dept_id = 1 order by employee_id) " +
		"limit 1" +
		");").Sort().Check(testkit.Rows("1 Furina 1", "1 Furina 1", "1 Furina 1", "2 Klee 1", "2 Klee 1", "3 Eula 1", "3 Eula 1"))
}

func TestIssue50043(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// Test simplified case by update.
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 boolean ,c2 decimal ( 37 , 17 ), unique key idx1 (c1 ,c2),unique key idx2 ( c1 ));")
	tk.MustExec("insert into t values (0,NULL);")
	tk.MustExec("alter table t alter column c2 drop default;")
	tk.MustExec("update t set c2 = 5 where c1 = 0;")
	tk.MustQuery("select * from t order by c1,c2").Check(testkit.Rows("0 5.00000000000000000"))

	// Test simplified case by insert on duplicate key update.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 boolean ,c2 decimal ( 37 , 17 ), unique key idx1 (c1 ,c2));")
	tk.MustExec("alter table t alter column c2 drop default;")
	tk.MustExec("alter table t add unique key idx4 ( c1 );")
	tk.MustExec("insert into t values (0, NULL), (1, 1);")
	tk.MustExec("insert into t values (0, 2) ,(1, 3) on duplicate key update c2 = 5;")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from t order by c1,c2").Check(testkit.Rows("0 5.00000000000000000", "1 5.00000000000000000"))

	// Test Issue 50043.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 boolean ,c2 decimal ( 37 , 17 ), unique key idx1 (c1 ,c2));")
	tk.MustExec("alter table t alter column c2 drop default;")
	tk.MustExec("alter table t add unique key idx4 ( c1 );")
	tk.MustExec("insert into t values (0, NULL), (1, 1);")
	tk.MustExec("insert ignore into t values (0, 2) ,(1, 3) on duplicate key update c2 = 5, c1 = 0")
	tk.MustQuery("select * from t order by c1,c2").Check(testkit.Rows("0 5.00000000000000000", "1 1.00000000000000000"))
}

func TestIssue51324(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int key, a int, b enum('a', 'b'))")
	tk.MustGetErrMsg("insert into t values ()", "[table:1364]Field 'id' doesn't have a default value")
	tk.MustExec("insert into t set id = 1")
	tk.MustExec("insert into t set id = 2, a = NULL, b = NULL")
	tk.MustExec("insert into t set id = 3, a = DEFAULT, b = DEFAULT")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 <nil> <nil>", "2 <nil> <nil>", "3 <nil> <nil>"))

	tk.MustExec("alter table t alter column a drop default")
	tk.MustExec("alter table t alter column b drop default")
	tk.MustGetErrMsg("insert into t set id = 4;", "[table:1364]Field 'a' doesn't have a default value")
	tk.MustExec("insert into t set id = 5, a = NULL, b = NULL;")
	tk.MustGetErrMsg("insert into t set id = 6, a = DEFAULT, b = DEFAULT;", "[table:1364]Field 'a' doesn't have a default value")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 <nil> <nil>", "2 <nil> <nil>", "3 <nil> <nil>", "5 <nil> <nil>"))

	tk.MustExec("insert ignore into t set id = 4;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value"))
	tk.MustExec("insert ignore into t set id = 6, a = DEFAULT, b = DEFAULT;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value"))
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 <nil> <nil>", "2 <nil> <nil>", "3 <nil> <nil>", "4 <nil> <nil>", "5 <nil> <nil>", "6 <nil> <nil>"))
	tk.MustExec("update t set id = id + 10")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("11 <nil> <nil>", "12 <nil> <nil>", "13 <nil> <nil>", "14 <nil> <nil>", "15 <nil> <nil>", "16 <nil> <nil>"))

	// Test not null case.
	tk.MustExec("drop table t")
	tk.MustExec("create table t (id int key, a int not null, b enum('a', 'b') not null)")
	tk.MustGetErrMsg("insert into t values ()", "[table:1364]Field 'id' doesn't have a default value")
	tk.MustGetErrMsg("insert into t set id = 1", "[table:1364]Field 'a' doesn't have a default value")
	tk.MustGetErrMsg("insert into t set id = 2, a = NULL, b = NULL", "[table:1048]Column 'a' cannot be null")
	tk.MustGetErrMsg("insert into t set id = 2, a = 2, b = NULL", "[table:1048]Column 'b' cannot be null")
	tk.MustGetErrMsg("insert into t set id = 3, a = DEFAULT, b = DEFAULT", "[table:1364]Field 'a' doesn't have a default value")
	tk.MustExec("alter table t alter column a drop default")
	tk.MustExec("alter table t alter column b drop default")
	tk.MustExec("insert ignore into t set id = 4;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value"))
	tk.MustExec("insert ignore into t set id = 5, a = NULL, b = NULL;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null", "Warning 1048 Column 'b' cannot be null"))
	tk.MustExec("insert ignore into t set id = 6, a = 6,    b = NULL;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'b' cannot be null"))
	tk.MustExec("insert ignore into t set id = 7, a = DEFAULT, b = DEFAULT;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value"))
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("4 0 a", "5 0 ", "6 6 ", "7 0 a"))

	// Test add column with OriginDefaultValue case.
	tk.MustExec("drop table t")
	tk.MustExec("create table t (id int, unique key idx (id))")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("alter table t add column a int default 1")
	tk.MustExec("alter table t add column b int default null")
	tk.MustExec("alter table t add column c int not null")
	tk.MustExec("alter table t add column d int not null default 1")
	tk.MustExec("insert ignore into t (id) values (2)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'c' doesn't have a default value"))
	tk.MustExec("insert ignore into t (id) values (1),(2) on duplicate key update id = id+10")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'c' doesn't have a default value"))
	tk.MustExec("alter table t alter column a drop default")
	tk.MustExec("alter table t alter column b drop default")
	tk.MustExec("alter table t alter column c drop default")
	tk.MustExec("alter table t alter column d drop default")
	tk.MustExec("insert ignore into t (id) values (3)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value", "Warning 1364 Field 'b' doesn't have a default value", "Warning 1364 Field 'c' doesn't have a default value", "Warning 1364 Field 'd' doesn't have a default value"))
	tk.MustExec("insert ignore into t (id) values (11),(12),(3) on duplicate key update id = id+10")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value", "Warning 1364 Field 'b' doesn't have a default value", "Warning 1364 Field 'c' doesn't have a default value", "Warning 1364 Field 'd' doesn't have a default value"))
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("13 <nil> <nil> 0 0", "21 1 <nil> 0 1", "22 1 <nil> 0 1"))
}
