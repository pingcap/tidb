// Copyright 2021 PingCAP, Inc.
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

package bindinfo_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/bindinfo/internal"
	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/stretchr/testify/require"
)

func TestGlobalAndSessionBindingBothExist(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	tk.MustHavePlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin")
	tk.MustHavePlan("SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin")

	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	// Test 'tidb_use_plan_baselines'
	tk.MustExec("set @@tidb_use_plan_baselines = 0")
	tk.MustHavePlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin")
	tk.MustExec("set @@tidb_use_plan_baselines = 1")

	// Test 'drop global binding'
	tk.MustHavePlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin")
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	tk.MustHavePlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin")

	// Test the case when global and session binding both exist
	// PART1 : session binding should totally cover global binding
	// use merge join as session binding here since the optimizer will choose hash join for this stmt in default
	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_HJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	tk.MustHavePlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin")
	tk.MustExec("create binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	tk.MustHavePlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin")
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	tk.MustHavePlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin")
}

func TestSessionBinding(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)

	for _, testSQL := range testSQLs {
		internal.UtilCleanBindingEnv(tk, dom)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t(i int, s varchar(20))")
		tk.MustExec("create table t1(i int, s varchar(20))")
		tk.MustExec("create index index_t on t(i,s)")

		_, err := tk.Exec("create session " + testSQL.createSQL)
		require.NoError(t, err, "err %v", err)

		if testSQL.overlaySQL != "" {
			_, err = tk.Exec("create session " + testSQL.overlaySQL)
			require.NoError(t, err)
		}

		handle := tk.Session().Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
		stmt, err := parser.New().ParseOneStmt(testSQL.originSQL, "", "")
		require.NoError(t, err)

		_, fuzzyDigest := norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
		binding, matched := handle.MatchSessionBinding(tk.Session(), fuzzyDigest, bindinfo.CollectTableNames(stmt))
		require.True(t, matched)
		require.Equal(t, testSQL.originSQL, binding.OriginalSQL)
		require.Equal(t, testSQL.bindSQL, binding.BindSQL)
		require.Equal(t, "test", binding.Db)
		require.Equal(t, bindinfo.Enabled, binding.Status)
		require.NotNil(t, binding.Charset)
		require.NotNil(t, binding.Collation)
		require.NotNil(t, binding.CreateTime)
		require.NotNil(t, binding.UpdateTime)

		rs, err := tk.Exec("show global bindings")
		require.NoError(t, err)
		chk := rs.NewChunk(nil)
		err = rs.Next(context.TODO(), chk)
		require.NoError(t, err)
		require.Equal(t, 0, chk.NumRows())

		rs, err = tk.Exec("show session bindings")
		require.NoError(t, err)
		chk = rs.NewChunk(nil)
		err = rs.Next(context.TODO(), chk)
		require.NoError(t, err)
		require.Equal(t, 1, chk.NumRows())
		row := chk.GetRow(0)
		require.Equal(t, testSQL.originSQL, row.GetString(0))
		require.Equal(t, testSQL.bindSQL, row.GetString(1))
		require.Equal(t, "test", row.GetString(2))
		require.Equal(t, bindinfo.Enabled, row.GetString(3))
		require.NotNil(t, row.GetTime(4))
		require.NotNil(t, row.GetTime(5))
		require.NotNil(t, row.GetString(6))
		require.NotNil(t, row.GetString(7))

		_, err = tk.Exec("drop session " + testSQL.dropSQL)
		require.NoError(t, err)
		_, fuzzyDigest = norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
		_, matched = handle.MatchSessionBinding(tk.Session(), fuzzyDigest, bindinfo.CollectTableNames(stmt))
		require.False(t, matched) // dropped
	}
}

func TestBaselineDBLowerCase(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("drop database if exists SPM")
	tk.MustExec("create database SPM")
	tk.MustExec("use SPM")
	tk.MustExec("create table t(a int, b int)")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("update t set a = a + 1")
	tk.MustExec("update t set a = a + 1")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "update `spm` . `t` set `a` = `a` + ?", rows[0][0])
	// default_db should have lower case.
	require.Equal(t, "spm", rows[0][2])
	tk.MustExec("drop global binding for update t set a = a + 1")
	rows = tk.MustQuery("show global bindings").Rows()
	// DROP GLOBAL BINGING should remove the binding even if we are in SPM database.
	require.Len(t, rows, 0)

	tk.MustExec("create global binding for select * from t using select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `spm` . `t`", rows[0][0])
	// default_db should have lower case.
	require.Equal(t, "spm", rows[0][2])
	tk.MustExec("drop global binding for select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	// DROP GLOBAL BINGING should remove the binding even if we are in SPM database.
	require.Len(t, rows, 0)

	tk.MustExec("create session binding for select * from t using select * from t")
	rows = tk.MustQuery("show session bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `spm` . `t`", rows[0][0])
	// default_db should have lower case.
	require.Equal(t, "spm", rows[0][2])
	tk.MustExec("drop session binding for select * from t")
	rows = tk.MustQuery("show session bindings").Rows()
	// DROP SESSION BINGING should remove the binding even if we are in SPM database.
	require.Len(t, rows, 0)

	internal.UtilCleanBindingEnv(tk, dom)

	// Simulate existing bindings with upper case default_db.
	_, sqlDigest := parser.NormalizeDigestForBinding("select * from `spm` . `t`")
	tk.MustExec("insert into mysql.bind_info values('select * from `spm` . `t`', 'select * from `spm` . `t`', 'SPM', 'enabled', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "', '" + sqlDigest.String() + "', '')")
	tk.MustQuery("select original_sql, default_db from mysql.bind_info where original_sql = 'select * from `spm` . `t`'").Check(testkit.Rows(
		"select * from `spm` . `t` SPM",
	))
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `spm` . `t`", rows[0][0])
	// default_db should have lower case.
	require.Equal(t, "spm", rows[0][2])
	tk.MustExec("drop global binding for select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	// DROP GLOBAL BINGING should remove the binding even if we are in SPM database.
	require.Len(t, rows, 0)

	internal.UtilCleanBindingEnv(tk, dom)
	// Simulate existing bindings with upper case default_db.
	tk.MustExec("insert into mysql.bind_info values('select * from `spm` . `t`', 'select * from `spm` . `t`', 'SPM', 'enabled', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "', '" + sqlDigest.String() + "', '')")
	tk.MustQuery("select original_sql, default_db from mysql.bind_info where original_sql = 'select * from `spm` . `t`'").Check(testkit.Rows(
		"select * from `spm` . `t` SPM",
	))
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `spm` . `t`", rows[0][0])
	// default_db should have lower case.
	require.Equal(t, "spm", rows[0][2])
	tk.MustExec("create global binding for select * from t using select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `spm` . `t`", rows[0][0])
	// default_db should have lower case.
	require.Equal(t, "spm", rows[0][2])
	tk.MustQuery("select original_sql, default_db, status from mysql.bind_info where original_sql = 'select * from `spm` . `t`'").Check(testkit.Rows(
		"select * from `spm` . `t` SPM deleted",
		"select * from `spm` . `t` spm enabled",
	))
}

func TestShowGlobalBindings(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("drop database if exists SPM")
	tk.MustExec("create database SPM")
	tk.MustExec("use SPM")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t0(a int, b int, key(a))")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)
	// Simulate existing bindings in the mysql.bind_info.
	tk.MustExec("create global binding using select * from `spm` . `t` USE INDEX (`a`)")
	tk.MustExec("create global binding using select * from `spm` . `t0` USE INDEX (`a`)")
	tk.MustExec("create global binding using select /*+ use_index(`t` `a`)*/ * from `spm` . `t`")
	tk.MustExec("create global binding using select /*+ use_index(`t0` `a`)*/ * from `spm` . `t0`")

	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "select * from `spm` . `t0`", rows[0][0])
	require.Equal(t, "select * from `spm` . `t`", rows[1][0])

	rows = tk.MustQuery("show session bindings").Rows()
	require.Len(t, rows, 0)
	tk.MustExec("create session binding for select a from t using select a from t")
	tk.MustExec("create session binding for select a from t0 using select a from t0")
	tk.MustExec("create session binding for select b from t using select b from t")
	tk.MustExec("create session binding for select b from t0 using select b from t0")
	rows = tk.MustQuery("show session bindings").Rows()
	require.Len(t, rows, 4)
	require.Equal(t, "select `b` from `spm` . `t0`", rows[0][0])
	require.Equal(t, "select `b` from `spm` . `t`", rows[1][0])
	require.Equal(t, "select `a` from `spm` . `t0`", rows[2][0])
	require.Equal(t, "select `a` from `spm` . `t`", rows[3][0])
}

func TestDuplicateBindings(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	createTime := rows[0][4]
	time.Sleep(time.Millisecond)
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.False(t, createTime == rows[0][4])

	tk.MustExec("create session binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show session bindings").Rows()
	require.Len(t, rows, 1)
	createTime = rows[0][4]
	time.Sleep(time.Millisecond)
	tk.MustExec("create session binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show session bindings").Rows()
	require.Len(t, rows, 1)
	require.False(t, createTime == rows[0][4])
}

func TestDefaultDB(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from test.t using select * from test.t use index(idx)")
	tk.MustExec("use mysql")
	tk.MustQuery("select * from test.t")
	// Even in another database, we could still use the bindings.
	require.Equal(t, "t:idx", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("drop global binding for select * from test.t")
	tk.MustQuery("show global bindings").Check(testkit.Rows())

	tk.MustExec("use test")
	tk.MustExec("create session binding for select * from test.t using select * from test.t use index(idx)")
	tk.MustExec("use mysql")
	tk.MustQuery("select * from test.t")
	// Even in another database, we could still use the bindings.
	require.Equal(t, "t:idx", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("drop session binding for select * from test.t")
	tk.MustQuery("show session bindings").Check(testkit.Rows())
}

func TestIssue19836(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, key (a));")
	tk.MustExec("CREATE SESSION BINDING FOR select * from t where a = 1 limit 5, 5 USING select * from t ignore index (a) where a = 1 limit 5, 5;")
	tk.MustExec("PREPARE stmt FROM 'select * from t where a = 40 limit ?, ?';")
	tk.MustExec("set @a=1;")
	tk.MustExec("set @b=2;")
	tk.MustExec("EXECUTE stmt USING @a, @b;")
	explainResult := testkit.Rows(
		"Limit_8 2.00 0 root  time:0s, loops:0 offset:1, count:2 N/A N/A",
		"└─TableReader_13 3.00 0 root  time:0s, loops:0 data:Limit_12 N/A N/A",
		"  └─Limit_12 3.00 0 cop[tikv]   offset:0, count:3 N/A N/A",
		"    └─Selection_11 3.00 0 cop[tikv]   eq(test.t.a, 40) N/A N/A",
		"      └─TableFullScan_10 3000.00 0 cop[tikv] table:t  keep order:false, stats:pseudo N/A N/A",
	)
	tk.MustQuery("explain for connection " + strconv.FormatUint(tk.Session().ShowProcess().ID, 10)).Check(explainResult)
}

func TestDropSingleBindings(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b))")

	// Test drop session bindings.
	tk.MustExec("create binding for select * from t using select * from t use index(idx_a)")
	tk.MustExec("create binding for select * from t using select * from t use index(idx_b)")
	rows := tk.MustQuery("show bindings").Rows()
	// The size of bindings is equal to one. Because for one normalized sql,
	// the `create binding` clears all the origin bindings.
	require.Len(t, rows, 1)
	require.Equal(t, "SELECT * FROM `test`.`t` USE INDEX (`idx_b`)", rows[0][1])
	tk.MustExec("drop binding for select * from t using select * from t use index(idx_a)")
	rows = tk.MustQuery("show bindings").Rows()
	require.Len(t, rows, 0)
	tk.MustExec("drop table t")

	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b))")
	// Test drop global bindings.
	tk.MustExec("create global binding for select * from t using select * from t use index(idx_a)")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx_b)")
	rows = tk.MustQuery("show global bindings").Rows()
	// The size of bindings is equal to one. Because for one normalized sql,
	// the `create binding` clears all the origin bindings.
	require.Len(t, rows, 1)
	require.Equal(t, "SELECT * FROM `test`.`t` USE INDEX (`idx_b`)", rows[0][1])
	tk.MustExec("drop global binding for select * from t using select * from t use index(idx_a)")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)
	tk.MustExec("drop table t")
}

func TestPreparedStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec(`prepare stmt1 from 'select * from t'`)
	tk.MustExec("execute stmt1")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 0)

	tk.MustExec("create binding for select * from t using select * from t use index(idx)")
	tk.MustExec("execute stmt1")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])

	tk.MustExec("drop binding for select * from t")
	tk.MustExec("execute stmt1")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 0)

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c(c))")
	tk.MustExec("set @p = 1")

	tk.MustExec("prepare stmt from 'delete from t where b = ? and c > ?'")
	tk.MustExec("execute stmt using @p,@p")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("create binding for delete from t where b = 2 and c > 2 using delete /*+ use_index(t,idx_c) */ from t where b = 2 and c > 2")
	tk.MustExec("execute stmt using @p,@p")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])

	tk.MustExec("prepare stmt from 'update t set a = 1 where b = ? and c > ?'")
	tk.MustExec("execute stmt using @p,@p")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("create binding for update t set a = 2 where b = 2 and c > 2 using update /*+ use_index(t,idx_c) */ t set a = 2 where b = 2 and c > 2")
	tk.MustExec("execute stmt using @p,@p")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 like t")
	tk.MustExec("prepare stmt from 'insert into t1 select * from t where t.b = ? and t.c > ?'")
	tk.MustExec("execute stmt using @p,@p")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("create binding for insert into t1 select * from t where t.b = 2 and t.c > 2 using insert into t1 select /*+ use_index(t,idx_c) */ * from t where t.b = 2 and t.c > 2")
	tk.MustExec("execute stmt using @p,@p")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])

	tk.MustExec("prepare stmt from 'replace into t1 select * from t where t.b = ? and t.c > ?'")
	tk.MustExec("execute stmt using @p,@p")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("create binding for replace into t1 select * from t where t.b = 2 and t.c > 2 using replace into t1 select /*+ use_index(t,idx_c) */ * from t where t.b = 2 and t.c > 2")
	tk.MustExec("execute stmt using @p,@p")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
}
