// Copyright 2026 PingCAP, Inc.
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

package materializedview

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestShowMaterializedView(t *testing.T) {
	t.Run("ShowMaterializedViews", testShowMaterializedViews)
	t.Run("ShowMaterializedViewLogs", testShowMaterializedViewLogs)
	t.Run("ShowMaterializedViewStatusPrivilege", testShowMaterializedViewStatusPrivilege)
	t.Run("ShowMaterializedViewRemainLogs", testShowMaterializedViewRemainLogs)
	t.Run("ShowMaterializedViewLogWaitPurge", testShowMaterializedViewLogWaitPurge)
	t.Run("ShowCreateMaterializedView", testShowCreateMaterializedView)
	t.Run("ShowCreateMaterializedViewLog", testShowCreateMaterializedViewLog)
	t.Run("GrantMaterializedViewObjectPrivileges", testGrantMaterializedViewObjectPrivileges)
}

func testShowMaterializedViews(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create database test_show_mv_other")
	tk.MustExec("create table test_show_mv_other.t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on test_show_mv_other.t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view test_show_mv_other.mv_other (a, s, cnt) as select a, sum(b), count(1) from test_show_mv_other.t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	otherMVTable, err := is.TableByName(context.Background(), ast.NewCIStr("test_show_mv_other"), ast.NewCIStr("mv_other"))
	require.NoError(t, err)
	otherMVID := otherMVTable.Meta().ID

	tk.MustQuery("show materialized views").Check(testkit.Rows(fmt.Sprintf("%d mv", mvID)))
	tk.MustQuery(fmt.Sprintf("show materialized views where mview_id = %d", mvID)).
		Check(testkit.Rows(fmt.Sprintf("%d mv", mvID)))
	tk.MustQuery("show materialized views where mview_name = 'mv'").Check(testkit.Rows(fmt.Sprintf("%d mv", mvID)))
	tk.MustQuery("show materialized views where mview_name = 'mv_not_exist'").Check(testkit.Rows())
	tk.MustQuery("show materialized views from test_show_mv_other").
		Check(testkit.Rows(fmt.Sprintf("%d mv_other", otherMVID)))
	tk.MustQuery("show materialized views in test_show_mv_other like 'mv_%'").
		Check(testkit.Rows(fmt.Sprintf("%d mv_other", otherMVID)))
}

func testShowMaterializedViewLogs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create database test_show_mlog_other")
	tk.MustExec("create table test_show_mlog_other.t_other (a int not null, b int not null)")
	tk.MustExec("create materialized view log on test_show_mlog_other.t_other (a, b) purge next date_add(now(), interval 1 hour)")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	mlogID := baseTable.Meta().MaterializedViewBase.MLogID
	mlogTable, ok := is.TableByID(context.Background(), mlogID)
	require.True(t, ok)
	mlogName := mlogTable.Meta().Name.O
	baseID := baseTable.Meta().ID
	baseName := baseTable.Meta().Name.O
	otherBaseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test_show_mlog_other"), ast.NewCIStr("t_other"))
	require.NoError(t, err)
	require.NotNil(t, otherBaseTable.Meta().MaterializedViewBase)
	otherMLogID := otherBaseTable.Meta().MaterializedViewBase.MLogID
	otherMLogTable, ok := is.TableByID(context.Background(), otherMLogID)
	require.True(t, ok)
	otherMLogName := otherMLogTable.Meta().Name.O
	otherExpected := fmt.Sprintf("%d %s %d %s", otherMLogID, otherMLogName, otherBaseTable.Meta().ID, otherBaseTable.Meta().Name.O)

	expected := fmt.Sprintf("%d %s %d %s", mlogID, mlogName, baseID, baseName)
	tk.MustQuery("show materialized view logs").Check(testkit.Rows(expected))
	tk.MustQuery(fmt.Sprintf("show materialized view logs where mlog_id = %d", mlogID)).
		Check(testkit.Rows(expected))
	tk.MustQuery(fmt.Sprintf("show materialized view logs where base_table_id = %d", baseID)).
		Check(testkit.Rows(expected))
	tk.MustQuery(fmt.Sprintf("show materialized view logs where mlog_name = '%s'", mlogName)).
		Check(testkit.Rows(expected))
	tk.MustQuery(fmt.Sprintf("show materialized view logs where base_table_name = '%s'", baseName)).
		Check(testkit.Rows(expected))
	tk.MustQuery("show materialized view logs where base_table_name = 't_not_exist'").Check(testkit.Rows())
	tk.MustQuery("show materialized view logs from test_show_mlog_other").
		Check(testkit.Rows(otherExpected))
	tk.MustQuery("show materialized view logs in test_show_mlog_other like '$mlog$%'").
		Check(testkit.Rows(otherExpected))
	tk.MustQuery("show materialized view logs in test_show_mlog_other like 'test_show_mlog_other.$mlog$%'").
		Check(testkit.Rows())

	tk.MustExec("create user 'show_mlog_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'show_mlog_u'@'%'")
	tk.MustExec("grant select on test.t to 'show_mlog_u'@'%'")
	tkUser := newMViewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "show_mlog_u", Hostname: "%"}, nil, nil, nil))
	tkUser.MustQuery("show materialized view logs from test").Check(testkit.Rows())
	tk.MustExec("grant alter on test.`$mlog$t` to 'show_mlog_u'@'%'")
	tkUser.MustQuery("show materialized view logs from test").Check(testkit.Rows(expected))
}

func testShowMaterializedViewStatusPrivilege(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_show_mv_status (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_show_mv_status (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_show_mv_status values (1, 10)")
	tk.MustExec("create materialized view mv_show_status (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_show_mv_status group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_show_status"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_show_mv_status"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	mlogTable, ok := is.TableByID(context.Background(), baseTable.Meta().MaterializedViewBase.MLogID)
	require.True(t, ok)

	mvExpected := func(pending int64) string {
		return fmt.Sprintf("%d mv_show_status %d %s %d", mvID, mlogTable.Meta().ID, mlogTable.Meta().Name.O, pending)
	}
	mlogExpected := func(pending int64) string {
		return fmt.Sprintf(
			"%d %s %d t_show_mv_status %d",
			mlogTable.Meta().ID,
			mlogTable.Meta().Name.O,
			baseTable.Meta().ID,
			pending,
		)
	}

	tk.MustQuery("show materialized view test.mv_show_status remain_logs").Check(testkit.Rows(mvExpected(0)))
	tk.MustQuery("show materialized view log on test.t_show_mv_status wait_purge").Check(testkit.Rows(mlogExpected(1)))

	tk.MustExec("insert into t_show_mv_status values (2, 20)")
	tk.MustQuery("show materialized view test.mv_show_status remain_logs").Check(testkit.Rows(mvExpected(1)))
	tk.MustQuery("show materialized view log on test.t_show_mv_status wait_purge").Check(testkit.Rows(mlogExpected(1)))

	tk.MustExec("refresh materialized view mv_show_status complete delta apply")
	tk.MustQuery("show materialized view test.mv_show_status remain_logs").Check(testkit.Rows(mvExpected(0)))
	tk.MustQuery("show materialized view log on test.t_show_mv_status wait_purge").Check(testkit.Rows(mlogExpected(2)))

	tk.MustExec("create user 'show_mv_status_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'show_mv_status_u'@'%'")
	tkUser := newMViewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "show_mv_status_u", Hostname: "%"}, nil, nil, nil))

	err = tkUser.ExecToErr("show materialized view test.mv_show_status remain_logs")
	require.ErrorContains(t, err, "SHOW VIEW command denied")
	err = tkUser.ExecToErr("show materialized view log on test.t_show_mv_status wait_purge")
	require.ErrorContains(t, err, "SHOW VIEW command denied")

	tk.MustExec("grant show view on test.mv_show_status to 'show_mv_status_u'@'%'")
	tkUser.MustQuery("show materialized view test.mv_show_status remain_logs").Check(testkit.Rows(mvExpected(0)))
	err = tkUser.ExecToErr("show materialized view log on test.t_show_mv_status wait_purge")
	require.ErrorContains(t, err, "SHOW VIEW command denied")

	tk.MustExec("grant show view on test.`$mlog$t_show_mv_status` to 'show_mv_status_u'@'%'")
	tkUser.MustQuery("show materialized view log on test.t_show_mv_status wait_purge").Check(testkit.Rows(mlogExpected(2)))
}

func testShowMaterializedViewRemainLogs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_remain_logs (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_remain_logs (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_remain_logs (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_remain_logs group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_remain_logs"))
	require.NoError(t, err)
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_remain_logs"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	mlogID := baseTable.Meta().MaterializedViewBase.MLogID
	mlogTable, ok := is.TableByID(context.Background(), mlogID)
	require.True(t, ok)

	expectedRow := func(remainLogs int64) string {
		return fmt.Sprintf("%d mv_remain_logs %d %s %d", mvTable.Meta().ID, mlogID, mlogTable.Meta().Name.O, remainLogs)
	}
	tk.MustQuery("show materialized view mv_remain_logs remain_logs").Check(testkit.Rows(expectedRow(0)))

	tk.MustExec("insert into t_remain_logs values (1, 10), (1, 20), (2, 30)")
	tk.MustQuery("show materialized view mv_remain_logs remain_logs").Check(testkit.Rows(expectedRow(3)))

	tk.MustExec("refresh materialized view mv_remain_logs fast")
	tk.MustQuery("show materialized view mv_remain_logs remain_logs").Check(testkit.Rows(expectedRow(0)))

	err = tk.QueryToErr("show materialized view t_remain_logs remain_logs")
	require.ErrorContains(t, err, "is not MATERIALIZED VIEW")
	err = tk.ExecToErr("show materialized view mv_remain_logs unknown_option")
	require.ErrorContains(t, err, "syntax error: expected REMAIN_LOGS")
}

func testShowMaterializedViewLogWaitPurge(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_wait_purge (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_wait_purge (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_wait_purge (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_wait_purge group by a")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_wait_purge"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	mlogID := baseTable.Meta().MaterializedViewBase.MLogID
	mlogTable, ok := is.TableByID(context.Background(), mlogID)
	require.True(t, ok)

	expectedRow := func(waitPurge int64) string {
		return fmt.Sprintf("%d %s %d t_wait_purge %d", mlogID, mlogTable.Meta().Name.O, baseTable.Meta().ID, waitPurge)
	}
	tk.MustQuery("show materialized view log on t_wait_purge wait_purge").Check(testkit.Rows(expectedRow(0)))

	tk.MustExec("insert into t_wait_purge values (1, 10), (1, 20), (2, 30)")
	tk.MustQuery("show materialized view log on t_wait_purge wait_purge").Check(testkit.Rows(expectedRow(0)))

	tk.MustExec("refresh materialized view mv_wait_purge fast")
	tk.MustQuery("show materialized view log on t_wait_purge wait_purge").Check(testkit.Rows(expectedRow(3)))

	tk.MustExec("purge materialized view log on t_wait_purge")
	tk.MustQuery("show materialized view log on t_wait_purge wait_purge").Check(testkit.Rows(expectedRow(0)))

	tk.MustExec("create temporary table t_wait_purge (a int)")
	tk.MustQuery("show materialized view log on t_wait_purge wait_purge").Check(testkit.Rows(expectedRow(0)))

	tk.MustExec("create table t_wait_purge_no_log (a int)")
	err = tk.QueryToErr("show materialized view log on t_wait_purge_no_log wait_purge")
	require.ErrorContains(t, err, "materialized view log does not exist for base table test.t_wait_purge_no_log")
	err = tk.ExecToErr("show materialized view log on t_wait_purge unknown_option")
	require.ErrorContains(t, err, "syntax error: expected WAIT_PURGE")
}

func testShowCreateMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_show_mv (a int, b int not null)")
	tk.MustExec("create materialized view log on t_show_mv (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_show_mv (a, s, cnt) comment = 'c1' shard_row_id_bits = 2 pre_split_regions = 2 refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_show_mv group by a")

	rows := tk.MustQuery("show create materialized view mv_show_mv").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "mv_show_mv", rows[0][0])
	showCreate, ok := rows[0][1].(string)
	require.True(t, ok)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW `mv_show_mv` (`a`, `s`, `cnt`)")
	require.Contains(t, showCreate, "COMMENT = 'c1'")
	require.Contains(t, showCreate, "SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 2")
	require.Contains(t, showCreate, "REFRESH FAST NEXT DATE_ADD(NOW(), INTERVAL 1 HOUR)")
	require.NotContains(t, showCreate, "ATTRIBUTES='")
	require.Contains(t, showCreate, "AS SELECT `a`,SUM(`b`),COUNT(1) FROM `test`.`t_show_mv` GROUP BY `a`")
	_, err := parser.New().ParseOneStmt(showCreate, "", "")
	require.NoError(t, err)
	require.Equal(t, "utf8mb4", rows[0][2])
	require.Equal(t, "utf8mb4_bin", rows[0][3])

	tk.MustExec("create materialized view mv_show_mv_attr (a, s, cnt) refresh fast attributes='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t_show_mv group by a")
	attrRows := tk.MustQuery("show create materialized view mv_show_mv_attr").Rows()
	require.Len(t, attrRows, 1)
	attrShowCreate, ok := attrRows[0][1].(string)
	require.True(t, ok)
	require.Contains(t, attrShowCreate, "ATTRIBUTES='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes'")

	tk.MustExec("create materialized view mv_show_mv_attr_partial (a, s, cnt) refresh fast attributes='mview_alert_warning=0,mview_alert_overdue=10' as select a, sum(b), count(1) from t_show_mv group by a")
	attrPartialRows := tk.MustQuery("show create materialized view mv_show_mv_attr_partial").Rows()
	require.Len(t, attrPartialRows, 1)
	attrPartialShowCreate, ok := attrPartialRows[0][1].(string)
	require.True(t, ok)
	require.Contains(t, attrPartialShowCreate, "ATTRIBUTES='mview_alert_overdue=10'")
	require.NotContains(t, attrPartialShowCreate, "mview_alert_warning=0")
}

func testShowCreateMaterializedViewLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_show_mlog (a int, b int)")
	tk.MustExec("create materialized view log on t_show_mlog (a, b) shard_row_id_bits = 2 pre_split_regions = 2 purge start with cast('2026-01-02 03:04:05' as datetime) next date_add(now(), interval 1 hour) alert rows 2048")

	rows := tk.MustQuery("show create materialized view log on t_show_mlog").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "t_show_mlog", rows[0][0])
	showCreate, ok := rows[0][1].(string)
	require.True(t, ok)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `t_show_mlog` (`a`, `b`)")
	require.Contains(t, showCreate, "SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 2")
	require.Contains(t, showCreate, "PURGE START WITH CAST('2026-01-02 03:04:05' AS DATETIME) NEXT DATE_ADD(NOW(), INTERVAL 1 HOUR)")
	require.Contains(t, showCreate, "ALERT ROWS 2048")
	_, err := parser.New().ParseOneStmt(showCreate, "", "")
	require.NoError(t, err)

	tk.MustExec("create temporary table t_show_mlog (a int)")
	rows = tk.MustQuery("show create materialized view log on t_show_mlog").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "t_show_mlog", rows[0][0])
	require.Contains(t, rows[0][1], "CREATE MATERIALIZED VIEW LOG ON `t_show_mlog` (`a`, `b`)")

	tk.MustExec("create user 'u_show_create_mlog'@'%'")
	defer tk.MustExec("drop user 'u_show_create_mlog'@'%'")
	tkShow := newMViewTestKit(t, store)
	require.NoError(t, tkShow.Session().Auth(&auth.UserIdentity{Username: "u_show_create_mlog", Hostname: "%"}, nil, nil, nil))
	err = tkShow.ExecToErr("show create materialized view log on test.t_show_mlog")
	require.ErrorContains(t, err, "SHOW VIEW command denied")
	tk.MustExec("grant show view on test.`$mlog$t_show_mlog` to 'u_show_create_mlog'@'%'")
	err = tkShow.ExecToErr("show create materialized view log on test.t_show_mlog")
	require.ErrorContains(t, err, "SELECT command denied")
	tk.MustExec("grant select on test.`$mlog$t_show_mlog` to 'u_show_create_mlog'@'%'")
	userRows := tkShow.MustQuery("show create materialized view log on test.t_show_mlog").Rows()
	require.Len(t, userRows, 1)
	require.Equal(t, "t_show_mlog", userRows[0][0])
	require.Equal(t, showCreate, userRows[0][1])

	tk.MustExec("create table t_no_mlog (a int)")
	err = tk.QueryToErr("show create materialized view log on t_no_mlog")
	require.ErrorContains(t, err, "'test.t_no_mlog' is not BASE TABLE WITH MATERIALIZED VIEW LOG")
}

func testGrantMaterializedViewObjectPrivileges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_grant_mv_priv (a int)")
	tk.MustExec("create materialized view log on t_grant_mv_priv (a)")
	tk.MustExec("create materialized view mv_grant_priv (a, cnt) as select a, count(1) from t_grant_mv_priv group by a")
	tk.MustExec("create user 'u_grant_mv_priv'@'%'")
	defer tk.MustExec("drop user 'u_grant_mv_priv'@'%'")

	tk.MustExec("grant all privileges on test.mv_grant_priv to 'u_grant_mv_priv'@'%'")
	rows := tk.MustQuery("select Table_priv, Column_priv from mysql.tables_priv where User = 'u_grant_mv_priv' and Table_name = 'mv_grant_priv'").Rows()
	require.Len(t, rows, 1)
	mvTablePrivs := fmt.Sprint(rows[0][0])
	require.Contains(t, mvTablePrivs, "Select")
	require.Contains(t, mvTablePrivs, "Show View")
	require.Contains(t, mvTablePrivs, "Alter")
	require.Contains(t, mvTablePrivs, "Drop")
	require.Contains(t, mvTablePrivs, "Operate View")
	require.NotContains(t, mvTablePrivs, "Insert")
	require.NotContains(t, mvTablePrivs, "Update")
	require.NotContains(t, mvTablePrivs, "Delete")
	require.Equal(t, "", fmt.Sprint(rows[0][1]))

	err := tk.ExecToErr("grant insert on test.mv_grant_priv to 'u_grant_mv_priv'@'%'")
	require.ErrorContains(t, err, "cannot grant Insert privilege on materialized view")
	err = tk.ExecToErr("grant operate view (a) on test.mv_grant_priv to 'u_grant_mv_priv'@'%'")
	require.ErrorContains(t, err, "COLUMN GRANT")

	tk.MustExec("grant all privileges on test.`$mlog$t_grant_mv_priv` to 'u_grant_mv_priv'@'%'")
	rows = tk.MustQuery("select Table_priv, Column_priv from mysql.tables_priv where User = 'u_grant_mv_priv' and Table_name = '$mlog$t_grant_mv_priv'").Rows()
	require.Len(t, rows, 1)
	mlogTablePrivs := fmt.Sprint(rows[0][0])
	require.Contains(t, mlogTablePrivs, "Select")
	require.Contains(t, mlogTablePrivs, "Show View")
	require.Contains(t, mlogTablePrivs, "Alter")
	require.Contains(t, mlogTablePrivs, "Drop")
	require.Contains(t, mlogTablePrivs, "Operate View")
	require.NotContains(t, mlogTablePrivs, "Insert")
	require.NotContains(t, mlogTablePrivs, "Update")
	require.NotContains(t, mlogTablePrivs, "Delete")
	require.Equal(t, "", fmt.Sprint(rows[0][1]))

	err = tk.ExecToErr("grant update on test.`$mlog$t_grant_mv_priv` to 'u_grant_mv_priv'@'%'")
	require.ErrorContains(t, err, "cannot grant Update privilege on materialized view log")
}
