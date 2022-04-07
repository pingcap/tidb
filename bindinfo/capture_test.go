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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/stmtsummary"
	"github.com/stretchr/testify/require"
)

func TestDMLCapturePlanBaseline(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec(" SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t1 like t")
	dom.BindHandle().CaptureBaselines()
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("delete from t where b = 1 and c > 1")
	tk.MustExec("delete from t where b = 1 and c > 1")
	tk.MustExec("update t set a = 1 where b = 1 and c > 1")
	tk.MustExec("update t set a = 1 where b = 1 and c > 1")
	tk.MustExec("insert into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("insert into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("replace into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("replace into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("insert into t1 values(1,1,1)")
	tk.MustExec("insert into t1 values(1,1,1)")
	tk.MustExec("replace into t1 values(1,1,1)")
	tk.MustExec("replace into t1 values(1,1,1)")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("delete from t where b = 1 and c > 1")
	tk.MustExec("delete from t where b = 1 and c > 1")
	tk.MustExec("update t set a = 1 where b = 1 and c > 1")
	tk.MustExec("update t set a = 1 where b = 1 and c > 1")
	tk.MustExec("insert into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("insert into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("replace into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("replace into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("insert into t1 values(1,1,1)")
	tk.MustExec("insert into t1 values(1,1,1)")
	tk.MustExec("replace into t1 values(1,1,1)")
	tk.MustExec("replace into t1 values(1,1,1)")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Sort().Rows()
	require.Len(t, rows, 4)
	require.Equal(t, "delete from `test` . `t` where `b` = ? and `c` > ?", rows[0][0])
	require.Equal(t, "DELETE /*+ use_index(@`del_1` `test`.`t` `idx_b`)*/ FROM `test`.`t` WHERE `b` = 1 AND `c` > 1", rows[0][1])
	require.Equal(t, "insert into `test` . `t1` select * from `test` . `t` where `t` . `b` = ? and `t` . `c` > ?", rows[1][0])
	require.Equal(t, "INSERT INTO `test`.`t1` SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_b`)*/ * FROM `test`.`t` WHERE `t`.`b` = 1 AND `t`.`c` > 1", rows[1][1])
	require.Equal(t, "replace into `test` . `t1` select * from `test` . `t` where `t` . `b` = ? and `t` . `c` > ?", rows[2][0])
	require.Equal(t, "REPLACE INTO `test`.`t1` SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_b`)*/ * FROM `test`.`t` WHERE `t`.`b` = 1 AND `t`.`c` > 1", rows[2][1])
	require.Equal(t, "update `test` . `t` set `a` = ? where `b` = ? and `c` > ?", rows[3][0])
	require.Equal(t, "UPDATE /*+ use_index(@`upd_1` `test`.`t` `idx_b`)*/ `test`.`t` SET `a`=1 WHERE `b` = 1 AND `c` > 1", rows[3][1])
}

func TestCapturePlanBaseline(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	dom.BindHandle().CaptureBaselines()
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("select count(*) from t where a > 10")
	tk.MustExec("select count(*) from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a` > 10", rows[0][1])
}

func TestCapturePlanBaseline4DisabledStatus(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")
	dom.BindHandle().CaptureBaselines()
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("select /*+ USE_INDEX(t, idx_a) */ * from t where a > 10")
	tk.MustExec("select /*+ USE_INDEX(t, idx_a) */ * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Enabled, rows[0][3])
	require.Equal(t, bindinfo.Capture, rows[0][8])

	tk.MustExec("select * from t where a > 10")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	tk.MustExec("set binding disabled for select * from t where a > 10")

	tk.MustExec("select * from t where a > 10")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Disabled, rows[0][3])

	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Disabled, rows[0][3])

	tk.MustExec("select * from t where a > 10")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))

	tk.MustExec("drop global binding for select * from t where a > 10")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)

	utilCleanBindingEnv(tk, dom)
}

func TestCaptureDBCaseSensitivity(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("drop database if exists SPM")
	tk.MustExec("create database SPM")
	tk.MustExec("use SPM")
	tk.MustExec("create table t(a int, b int, key(b))")
	tk.MustExec("create global binding for select * from t using select /*+ use_index(t) */ * from t")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select /*+ use_index(t,b) */ * from t")
	tk.MustExec("select /*+ use_index(t,b) */ * from t")
	tk.MustExec("admin capture bindings")
	// The capture should ignore the case sensitivity for DB name when checking if any binding exists,
	// so there would be no new binding captured.
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "SELECT /*+ use_index(`t` )*/ * FROM `SPM`.`t`", rows[0][1])
	require.Equal(t, "manual", rows[0][8])
}

func TestCaptureBaselinesDefaultDB(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop database if exists spm")
	tk.MustExec("create database spm")
	tk.MustExec("create table spm.t(a int, index idx_a(a))")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from spm.t ignore index(idx_a) where a > 10")
	tk.MustExec("select * from spm.t ignore index(idx_a) where a > 10")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	// Default DB should be "" when all columns have explicit database name.
	require.Equal(t, "", rows[0][2])
	require.Equal(t, bindinfo.Enabled, rows[0][3])
	tk.MustExec("use spm")
	tk.MustExec("select * from spm.t where a > 10")
	// Should use TableScan because of the "ignore index" binding.
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 0)
}

func TestCapturePreparedStmt(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, key idx_b(b), key idx_c(c))")
	require.True(t, tk.MustUseIndex("select * from t where b = 1 and c > 1", "idx_b(b)"))
	tk.MustExec("prepare stmt from 'select /*+ use_index(t,idx_c) */ * from t where b = ? and c > ?'")
	tk.MustExec("set @p = 1")
	tk.MustExec("execute stmt using @p, @p")
	tk.MustExec("execute stmt using @p, @p")

	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `b` = ? and `c` > ?", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_c`)*/ * FROM `test`.`t` WHERE `b` = ? AND `c` > ?", rows[0][1])

	require.True(t, tk.MustUseIndex("select /*+ use_index(t,idx_b) */ * from t where b = 1 and c > 1", "idx_c(c)"))
	tk.MustExec("admin flush bindings")
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `b` = ? and `c` > ?", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_c`)*/ * FROM `test`.`t` WHERE `b` = ? AND `c` > ?", rows[0][1])
}

func TestCapturePlanBaselineIgnoreTiFlash(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a), key(b))")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from t")
	tk.MustExec("select * from t")
	// Create virtual tiflash replica info.
	domSession := domain.GetDomain(tk.Session())
	is := domSession.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	// Here the plan is the TiFlash plan.
	rows := tk.MustQuery("explain select * from t").Rows()
	require.Equal(t, "cop[tiflash]", fmt.Sprintf("%v", rows[len(rows)-1][2]))

	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("admin capture bindings")
	// Don't have the TiFlash plan even we have TiFlash replica.
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t`", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t`", rows[0][1])
}

func TestBindingSource(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")

	// Test Source for SQL created sql
	tk.MustExec("create global binding for select * from t where a > 10 using select * from t ignore index(idx_a) where a > 10")
	bindHandle := dom.BindHandle()
	sql, hash := utilNormalizeWithDefaultDB(t, "select * from t where a > ?", "test")
	bindData := bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 1)
	bind := bindData.Bindings[0]
	require.Equal(t, bindinfo.Manual, bind.Source)

	// Test Source for evolved sql
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	tk.MustQuery("select * from t where a > 10")
	bindHandle.SaveEvolveTasksToStore()
	sql, hash = utilNormalizeWithDefaultDB(t, "select * from t where a > ?", "test")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 2)
	bind = bindData.Bindings[1]
	require.Equal(t, bindinfo.Evolve, bind.Source)
	tk.MustExec("set @@tidb_evolve_plan_baselines=0")

	// Test Source for captured sqls
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from t ignore index(idx_a) where a < 10")
	tk.MustExec("select * from t ignore index(idx_a) where a < 10")
	tk.MustExec("admin capture bindings")
	bindHandle.CaptureBaselines()
	sql, hash = utilNormalizeWithDefaultDB(t, "select * from t where a < ?", "test")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` < ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 1)
	bind = bindData.Bindings[0]
	require.Equal(t, bindinfo.Capture, bind.Source)
}

func TestCapturedBindingCharset(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("create table t(name varchar(25), index idx(name))")

	tk.MustExec("set character_set_connection = 'ascii'")
	tk.MustExec("update t set name = 'hello' where name <= 'abc'")
	tk.MustExec("update t set name = 'hello' where name <= 'abc'")
	tk.MustExec("set character_set_connection = 'utf8mb4'")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "update `test` . `t` set `name` = ? where `name` <= ?", rows[0][0])
	require.Equal(t, "UPDATE /*+ use_index(@`upd_1` `test`.`t` `idx`)*/ `test`.`t` SET `name`='hello' WHERE `name` <= 'abc'", rows[0][1])
	require.Equal(t, "utf8mb4", rows[0][6])
	require.Equal(t, "utf8mb4_bin", rows[0][7])
}

func TestConcurrentCapture(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	// Simulate an existing binding generated by concurrent CREATE BINDING, which has not been synchronized to current tidb-server yet.
	// Actually, it is more common to be generated by concurrent baseline capture, I use Manual just for simpler test verification.
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t`', 'select * from `test` . `t`', '', 'enabled', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustQuery("select original_sql, source from mysql.bind_info where source != 'builtin'").Check(testkit.Rows(
		"select * from `test` . `t` manual",
	))
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from t")
	tk.MustExec("select * from t")
	tk.MustExec("admin capture bindings")
	tk.MustQuery("select original_sql, source, status from mysql.bind_info where source != 'builtin'").Check(testkit.Rows(
		"select * from `test` . `t` manual deleted",
		"select * from `test` . `t` capture enabled",
	))
}

func TestUpdateSubqueryCapture(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, key idx_b(b))")
	tk.MustExec("create table t2(a int, b int)")
	stmtsummary.StmtSummaryByDigestMap.Clear()
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("update t1 set b = 1 where b = 2 and (a in (select a from t2 where b = 1) or c in (select a from t2 where b = 1))")
	tk.MustExec("update t1 set b = 1 where b = 2 and (a in (select a from t2 where b = 1) or c in (select a from t2 where b = 1))")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	bindSQL := "UPDATE /*+ use_index(@`upd_1` `test`.`t1` `idx_b`), use_index(@`sel_1` `test`.`t2` ), hash_join(@`upd_1` `test`.`t1`), use_index(@`sel_2` `test`.`t2` )*/ `test`.`t1` SET `b`=1 WHERE `b` = 2 AND (`a` IN (SELECT `a` FROM `test`.`t2` WHERE `b` = 1) OR `c` IN (SELECT `a` FROM `test`.`t2` WHERE `b` = 1))"
	require.Equal(t, bindSQL, rows[0][1])
	tk.MustExec(bindSQL)
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
}

func TestIssue20417(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t (
		 pk VARBINARY(36) NOT NULL PRIMARY KEY,
		 b BIGINT NOT NULL,
		 c BIGINT NOT NULL,
		 pad VARBINARY(2048),
		 INDEX idxb(b),
		 INDEX idxc(c)
		)`)

	// Test for create binding
	utilCleanBindingEnv(tk, dom)
	tk.MustExec("create global binding for select * from t using select /*+ use_index(t, idxb) */ * from t")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t`", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(`t` `idxb`)*/ * FROM `test`.`t`", rows[0][1])
	require.True(t, tk.MustUseIndex("select * from t", "idxb(b)"))
	require.True(t, tk.MustUseIndex("select * from test.t", "idxb(b)"))

	tk.MustExec("create global binding for select * from t WHERE b=2 AND c=3924541 using select /*+ use_index(@sel_1 test.t idxb) */ * from t WHERE b=2 AND c=3924541")
	require.True(t, tk.MustUseIndex("SELECT /*+ use_index(@`sel_1` `test`.`t` `idxc`)*/ * FROM `test`.`t` WHERE `b`=2 AND `c`=3924541", "idxb(b)"))
	require.True(t, tk.MustUseIndex("SELECT /*+ use_index(@`sel_1` `test`.`t` `idxc`)*/ * FROM `t` WHERE `b`=2 AND `c`=3924541", "idxb(b)"))

	// Test for capture baseline
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	dom.BindHandle().CaptureBaselines()
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from t where b=2 and c=213124")
	tk.MustExec("select * from t where b=2 and c=213124")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `b` = ? and `c` = ?", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idxb`)*/ * FROM `test`.`t` WHERE `b` = 2 AND `c` = 213124", rows[0][1])
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")

	// Test for evolve baseline
	utilCleanBindingEnv(tk, dom)
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	tk.MustExec("create global binding for select * from t WHERE c=3924541 using select /*+ use_index(@sel_1 test.t idxb) */ * from t WHERE c=3924541")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `c` = ?", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idxb`)*/ * FROM `test`.`t` WHERE `c` = 3924541", rows[0][1])
	tk.MustExec("select /*+ use_index(t idxc)*/ * from t where c=3924541")
	require.Equal(t, "t:idxb", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "select * from `test` . `t` where `c` = ?", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idxc`)*/ * FROM `test`.`t` WHERE `c` = 3924541", rows[0][1])
	require.Equal(t, "pending verify", rows[0][3])
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "select * from `test` . `t` where `c` = ?", rows[0][0])
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idxc`)*/ * FROM `test`.`t` WHERE `c` = 3924541", rows[0][1])
	status := rows[0][3].(string)
	require.True(t, status == bindinfo.Enabled || status == bindinfo.Rejected)
	tk.MustExec("set @@tidb_evolve_plan_baselines=0")
}

func TestCaptureWithZeroSlowLogThreshold(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	stmtsummary.StmtSummaryByDigestMap.Clear()
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("set tidb_slow_log_threshold = 0")
	tk.MustExec("select * from t")
	tk.MustExec("select * from t")
	tk.MustExec("set tidb_slow_log_threshold = 300")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t`", rows[0][0])
}

func TestIssue25505(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	stmtsummary.StmtSummaryByDigestMap.Clear()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold = 300")
	}()
	tk.MustExec("set tidb_slow_log_threshold = 0")
	tk.MustExec("create table t (a int(11) default null,b int(11) default null,key b (b),key ba (b))")
	tk.MustExec("create table t1 (a int(11) default null,b int(11) default null,key idx_ab (a,b),key idx_a (a),key idx_b (b))")
	tk.MustExec("create table t2 (a int(11) default null,b int(11) default null,key idx_ab (a,b),key idx_a (a),key idx_b (b))")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

	spmMap := map[string]string{}
	spmMap["with recursive `cte` ( `a` ) as ( select ? union select `a` + ? from `test` . `t1` where `a` < ? ) select * from `cte`"] =
		"WITH RECURSIVE `cte` (`a`) AS (SELECT 2 UNION SELECT `a` + 1 FROM `test`.`t1` WHERE `a` < 5) SELECT /*+ use_index(@`sel_3` `test`.`t1` `idx_ab`), hash_agg(@`sel_1`)*/ * FROM `cte`"
	spmMap["with recursive `cte1` ( `a` , `b` ) as ( select * from `test` . `t` where `b` = ? union select `a` + ? , `b` + ? from `cte1` where `a` < ? ) select * from `test` . `t`"] =
		"WITH RECURSIVE `cte1` (`a`, `b`) AS (SELECT * FROM `test`.`t` WHERE `b` = 1 UNION SELECT `a` + 1,`b` + 1 FROM `cte1` WHERE `a` < 2) SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t`"
	spmMap["with `cte1` as ( select * from `test` . `t` ) , `cte2` as ( select ? ) select * from `test` . `t`"] =
		"WITH `cte1` AS (SELECT * FROM `test`.`t`), `cte2` AS (SELECT 4) SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t`"
	spmMap["with `cte` as ( select * from `test` . `t` where `b` = ? ) select * from `test` . `t`"] =
		"WITH `cte` AS (SELECT * FROM `test`.`t` WHERE `b` = 6) SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t`"
	spmMap["with recursive `cte` ( `a` ) as ( select ? union select `a` + ? from `test` . `t1` where `a` > ? ) select * from `cte`"] =
		"WITH RECURSIVE `cte` (`a`) AS (SELECT 2 UNION SELECT `a` + 1 FROM `test`.`t1` WHERE `a` > 5) SELECT /*+ use_index(@`sel_3` `test`.`t1` `idx_b`), hash_agg(@`sel_1`)*/ * FROM `cte`"
	spmMap["with `cte` as ( with `cte1` as ( select * from `test` . `t2` where `a` > ? and `b` > ? ) select * from `cte1` ) select * from `cte` join `test` . `t1` on `t1` . `a` = `cte` . `a`"] =
		"WITH `cte` AS (WITH `cte1` AS (SELECT * FROM `test`.`t2` WHERE `a` > 1 AND `b` > 1) SELECT * FROM `cte1`) SELECT /*+ use_index(@`sel_3` `test`.`t2` `idx_ab`), use_index(@`sel_1` `test`.`t1` `idx_ab`), inl_join(@`sel_1` `test`.`t1`)*/ * FROM `cte` JOIN `test`.`t1` ON `t1`.`a` = `cte`.`a`"
	spmMap["with `cte` as ( with `cte1` as ( select * from `test` . `t2` where `a` = ? and `b` = ? ) select * from `cte1` ) select * from `cte` join `test` . `t1` on `t1` . `a` = `cte` . `a`"] =
		"WITH `cte` AS (WITH `cte1` AS (SELECT * FROM `test`.`t2` WHERE `a` = 1 AND `b` = 1) SELECT * FROM `cte1`) SELECT /*+ use_index(@`sel_3` `test`.`t2` `idx_a`), use_index(@`sel_1` `test`.`t1` `idx_a`), inl_join(@`sel_1` `test`.`t1`)*/ * FROM `cte` JOIN `test`.`t1` ON `t1`.`a` = `cte`.`a`"

	tk.MustExec("with cte as (with cte1 as (select /*+use_index(t2 idx_a)*/ * from t2 where a = 1 and b = 1) select * from cte1) select /*+use_index(t1 idx_a)*/ * from cte join t1 on t1.a=cte.a;")
	tk.MustExec("with cte as (with cte1 as (select /*+use_index(t2 idx_a)*/ * from t2 where a = 1 and b = 1) select * from cte1) select /*+use_index(t1 idx_a)*/ * from cte join t1 on t1.a=cte.a;")
	tk.MustExec("with cte as (with cte1 as (select /*+use_index(t2 idx_a)*/ * from t2 where a = 1 and b = 1) select * from cte1) select /*+use_index(t1 idx_a)*/ * from cte join t1 on t1.a=cte.a;")

	tk.MustExec("with cte as (with cte1 as (select * from t2 use index(idx_ab) where a > 1 and b > 1) select * from cte1) select /*+use_index(t1 idx_ab)*/ * from cte join t1 on t1.a=cte.a;")
	tk.MustExec("with cte as (with cte1 as (select * from t2 use index(idx_ab) where a > 1 and b > 1) select * from cte1) select /*+use_index(t1 idx_ab)*/ * from cte join t1 on t1.a=cte.a;")
	tk.MustExec("with cte as (with cte1 as (select * from t2 use index(idx_ab) where a > 1 and b > 1) select * from cte1) select /*+use_index(t1 idx_ab)*/ * from cte join t1 on t1.a=cte.a;")

	tk.MustExec("WITH RECURSIVE cte(a) AS (SELECT 2 UNION SELECT a+1 FROM t1 use index(idx_ab) WHERE a < 5) SELECT * FROM cte;")
	tk.MustExec("WITH RECURSIVE cte(a) AS (SELECT 2 UNION SELECT a+1 FROM t1 use index(idx_ab) WHERE a < 5) SELECT * FROM cte;")
	tk.MustExec("WITH RECURSIVE cte(a) AS (SELECT 2 UNION SELECT a+1 FROM t1 use index(idx_ab) WHERE a < 5) SELECT * FROM cte;")

	tk.MustExec("WITH RECURSIVE cte(a) AS (SELECT 2 UNION SELECT /*+use_index(t1 idx_b)*/  a+1 FROM t1  WHERE a > 5) SELECT * FROM cte;")
	tk.MustExec("WITH RECURSIVE cte(a) AS (SELECT 2 UNION SELECT /*+use_index(t1 idx_b)*/  a+1 FROM t1  WHERE a > 5) SELECT * FROM cte;")
	tk.MustExec("WITH RECURSIVE cte(a) AS (SELECT 2 UNION SELECT /*+use_index(t1 idx_b)*/  a+1 FROM t1  WHERE a > 5) SELECT * FROM cte;")

	tk.MustExec("with cte as (select * from t where b=6) select * from t")
	tk.MustExec("with cte as (select * from t where b=6) select * from t")
	tk.MustExec("with cte as (select * from t where b=6) select * from t")

	tk.MustExec("with cte1 as (select * from t), cte2 as (select 4) select * from t")
	tk.MustExec("with cte1 as (select * from t), cte2 as (select 5) select * from t")
	tk.MustExec("with cte1 as (select * from t), cte2 as (select 6) select * from t")

	tk.MustExec("with recursive cte1(a,b) as (select * from t where b = 1 union select a+1,b+1 from cte1 where a < 2) select * from t")
	tk.MustExec("with recursive cte1(a,b) as (select * from t where b = 1 union select a+1,b+1 from cte1 where a < 2) select * from t")
	tk.MustExec("with recursive cte1(a,b) as (select * from t where b = 1 union select a+1,b+1 from cte1 where a < 2) select * from t")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 7)
	for _, row := range rows {
		str := fmt.Sprintf("%s", row[0])
		require.Equal(t, spmMap[str], row[1])
	}
}

func TestCaptureUserFilter(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])

	// test user filter
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('user', 'root')")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0) // cannot capture the stmt

	// change another user
	tk.MustExec(`create user usr1`)
	tk.MustExec(`grant all on *.* to usr1 with grant option`)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	require.True(t, tk2.Session().Auth(&auth.UserIdentity{Username: "usr1", Hostname: "%"}, nil, nil))
	tk2.MustExec("select * from t where a > 10")
	tk2.MustExec("select * from t where a > 10")
	tk2.MustExec("admin capture bindings")
	rows = tk2.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1) // can capture the stmt

	// use user-filter with other types of filter together
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('user', 'root')")
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'test.t')")
	tk2.MustExec("select * from t where a > 10")
	tk2.MustExec("select * from t where a > 10")
	tk2.MustExec("admin capture bindings")
	rows = tk2.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0) // filtered by the table filter
}

func TestCaptureTableFilterValid(t *testing.T) {
	type matchCase struct {
		table   string
		matched bool
	}
	type filterCase struct {
		filter string
		valid  bool
		mcases []matchCase
	}
	filterCases := []filterCase{
		{"*.*", true, []matchCase{{"db.t", true}}},
		{"***.***", true, []matchCase{{"db.t", true}}},
		{"d*.*", true, []matchCase{{"db.t", true}}},
		{"*.t", true, []matchCase{{"db.t", true}}},
		{"?.t*", true, []matchCase{{"d.t", true}, {"d.tb", true}, {"db.t", false}}},
		{"db.t[1-3]", true, []matchCase{{"db.t1", true}, {"db.t2", true}, {"db.t4", false}}},
		{"!db.table", false, nil},
		{"@db.table", false, nil},
		{"table", false, nil},
		{"", false, nil},
		{"\t  ", false, nil},
	}
	for _, fc := range filterCases {
		f, valid := bindinfo.ParseCaptureTableFilter(fc.filter)
		require.Equal(t, fc.valid, valid)
		if valid {
			for _, mc := range fc.mcases {
				tmp := strings.Split(mc.table, ".")
				require.Equal(t, mc.matched, f.MatchTable(tmp[0], tmp[1]))
			}
		}
	}
}

func TestCaptureWildcardFilter(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	dbs := []string{"db11", "db12", "db2"}
	tbls := []string{"t11", "t12", "t2"}
	for _, db := range dbs {
		tk.MustExec(fmt.Sprintf(`drop database if exists %v`, db))
		tk.MustExec(fmt.Sprintf(`create database %v`, db))
		tk.MustExec(fmt.Sprintf(`use %v`, db))
		for _, tbl := range tbls {
			tk.MustExec(fmt.Sprintf(`create table %v(a int)`, tbl))
		}
	}
	mustExecTwice := func() {
		for _, db := range dbs {
			for _, tbl := range tbls {
				tk.MustExec(fmt.Sprintf(`select * from %v.%v where a>10`, db, tbl))
				tk.MustExec(fmt.Sprintf(`select * from %v.%v where a>10`, db, tbl))
			}
		}
	}
	checkBindings := func(dbTbls ...string) {
		m := make(map[string]bool) // map[query]existed
		for _, dbTbl := range dbTbls {
			tmp := strings.Split(dbTbl, ".")
			q := fmt.Sprintf("select * from `%v` . `%v` where `a` > ?", tmp[0], tmp[1])
			m[q] = false
		}

		tk.MustExec("admin capture bindings")
		rows := tk.MustQuery("show global bindings").Sort().Rows()
		require.Len(t, rows, len(dbTbls))
		for _, r := range rows {
			q := r[0].(string)
			if _, exist := m[q]; !exist { // encounter an unexpected binding
				t.Fatalf("unexpected binding %v", q)
			}
			m[q] = true
		}
		for q, exist := range m {
			if !exist { // a expected binding is not existed
				t.Fatalf("missed binding %v", q)
			}
		}
	}

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec(`insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'db11.t1*')`)
	mustExecTwice()
	checkBindings("db11.t2", "db12.t11", "db12.t12", "db12.t2", "db2.t11", "db2.t12", "db2.t2") // db11.t11 and db11.t12 are filtered

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec(`insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'db1*.t11')`)
	mustExecTwice()
	checkBindings("db11.t12", "db11.t2", "db12.t12", "db12.t2", "db2.t11", "db2.t12", "db2.t2") // db11.t11 and db12.t11 are filtered

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec(`insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'db1*.t1*')`)
	mustExecTwice()
	checkBindings("db11.t2", "db12.t2", "db2.t11", "db2.t12", "db2.t2") // db11.t11 / db12.t11 / db11.t12 / db12.t12 are filtered

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec(`insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'db1*.*')`)
	mustExecTwice()
	checkBindings("db2.t11", "db2.t12", "db2.t2") // db11.* / db12.* are filtered

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec(`insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', '*.t1*')`)
	mustExecTwice()
	checkBindings("db11.t2", "db12.t2", "db2.t2") // *.t11 and *.t12 are filtered

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec(`insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'db*.t*')`)
	mustExecTwice()
	checkBindings() // all are filtered

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	mustExecTwice()
	checkBindings("db11.t11", "db11.t12", "db11.t2", "db12.t11", "db12.t12", "db12.t2", "db2.t11", "db2.t12", "db2.t2") // no filter, all can be captured
}

func TestCaptureFilter(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])

	// Valid table filter.
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'test.t')")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)
	tk.MustExec("select * from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("select * from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `mysql` . `capture_plan_baselines_blacklist`", rows[0][0])

	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Sort().Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "select * from `mysql` . `capture_plan_baselines_blacklist`", rows[0][0])
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[1][0])

	// Invalid table filter.
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 't')")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])

	// Valid database filter.
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'mysql.*')")
	tk.MustExec("select * from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("select * from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])

	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Sort().Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "select * from `mysql` . `capture_plan_baselines_blacklist`", rows[0][0])
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[1][0])

	// Valid frequency filter.
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('frequency', '2')")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)

	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")

	// Invalid frequency filter.
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('frequency', '0')")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)

	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")

	// Invalid filter type.
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('unknown', 'xx')")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])
	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")

	// Case sensitivity.
	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('tABle', 'tESt.T')")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)

	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Sort().Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", rows[0][0])

	utilCleanBindingEnv(tk, dom)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into mysql.capture_plan_baselines_blacklist(filter_type, filter_value) values('table', 'mySQl.*')")
	tk.MustExec("select * from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("select * from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)

	tk.MustExec("delete from mysql.capture_plan_baselines_blacklist")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Sort().Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `mysql` . `capture_plan_baselines_blacklist`", rows[0][0])
}

func TestCaptureHints(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a int, b int, key(a), key(b))")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

	captureCases := []struct {
		query string
		hint  string
	}{
		// agg hints
		{"select /*+ hash_agg() */ count(1) from t", "hash_agg"},
		{"select /*+ stream_agg() */ count(1) from t", "stream_agg"},
		// join hints
		{"select /*+ merge_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a", "merge_join"},
		{"select /*+ tidb_smj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a", "merge_join"},
		{"select /*+ hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a", "hash_join"},
		{"select /*+ tidb_hj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a", "hash_join"},
		{"select /*+ inl_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a", "inl_join"},
		{"select /*+ tidb_inlj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a", "inl_join"},
		{"select /*+ inl_hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a", "inl_hash_join"},
		// index hints
		{"select * from t use index(primary)", "use_index(@`sel_1` `test`.`t` )"},
		{"select /*+ use_index(primary) */ * from t", "use_index(@`sel_1` `test`.`t` )"},
		{"select * from t use index(a)", "use_index(@`sel_1` `test`.`t` `a`)"},
		{"select /*+ use_index(a) */ * from t use index(a)", "use_index(@`sel_1` `test`.`t` `a`)"},
		{"select * from t use index(b)", "use_index(@`sel_1` `test`.`t` `b`)"},
		{"select /*+ use_index(b) */ * from t use index(b)", "use_index(@`sel_1` `test`.`t` `b`)"},
		{"select /*+ use_index_merge(t, a, b) */ a, b from t where a=1 or b=1", "use_index_merge(@`sel_1` `t` `a`, `b`)"},
		{"select /*+ ignore_index(t, a) */ * from t where a=1", "ignore_index(`t` `a`)"},
		// push-down hints
		{"select /*+ limit_to_cop() */ * from t limit 10", "limit_to_cop()"},
		{"select /*+ agg_to_cop() */ a, count(*) from t group by a", "agg_to_cop()"},
		// index-merge hints
		{"select /*+ no_index_merge() */ a, b from t where a>1 or b>1", "no_index_merge()"},
		{"select /*+ use_index_merge(t, a, b) */ a, b from t where a>1 or b>1", "use_index_merge(@`sel_1` `t` `a`, `b`)"},
		// runtime hints
		{"select /*+ memory_quota(1024 MB) */ * from t", "memory_quota(1024 mb)"},
		{"select /*+ max_execution_time(1000) */ * from t", "max_execution_time(1000)"},
		// storage hints
		{"select /*+ read_from_storage(tikv[t]) */ * from t", "read_from_storage(tikv[`t`])"},
		// others
		{"select /*+ use_toja(true) */ t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)", "use_toja(true)"},
	}
	for _, capCase := range captureCases {
		stmtsummary.StmtSummaryByDigestMap.Clear()
		utilCleanBindingEnv(tk, dom)
		tk.MustExec(capCase.query)
		tk.MustExec(capCase.query)
		tk.MustExec("admin capture bindings")
		res := tk.MustQuery(`show global bindings`).Rows()
		require.Equal(t, len(res), 1)                                       // this query is captured, and
		require.True(t, strings.Contains(res[0][1].(string), capCase.hint)) // the binding contains the expected hint
	}
}
