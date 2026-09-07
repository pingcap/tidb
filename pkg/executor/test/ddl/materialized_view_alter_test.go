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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func mustExecMViewMaintenance(t *testing.T, tk *testkit.TestKit, sql string) {
	t.Helper()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	vars := tk.Session().GetSessionVars()
	originalMaintenance := vars.InMViewMaintenance
	originalRestrictedSQL := vars.InRestrictedSQL
	vars.InMViewMaintenance = true
	vars.InRestrictedSQL = true
	defer func() {
		vars.InMViewMaintenance = originalMaintenance
		vars.InRestrictedSQL = originalRestrictedSQL
	}()
	rs, err := tk.Session().ExecuteInternal(ctx, sql)
	require.NoError(t, err)
	require.Nil(t, rs)
}

func TestAlterMaterializedViewRefreshExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	err := tk.ExecToErr("alter materialized view mv refresh next 300")
	require.ErrorContains(t, err, "REFRESH NEXT expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view mv refresh start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "REFRESH START WITH expression must return DATETIME/TIMESTAMP")

	tk.MustExec("alter materialized view mv refresh start with now() next date_add(now(), interval 1 hour)")
}

func TestAlterMaterializedViewMetadataDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t group by a")

	getMView := func() (*model.TableInfo, *model.MaterializedViewInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedView)
		return tbl.Meta(), tbl.Meta().MaterializedView
	}

	mvTable, mvInfo := getMView()
	tk.MustExec("alter materialized view mv comment = 'updated comment'")
	mvTable, mvInfo = getMView()
	require.Equal(t, "updated comment", mvTable.Comment)

	tk.MustExec("alter materialized view mv refresh start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	_, mvInfo = getMView()
	require.Equal(t, "FAST", mvInfo.RefreshMethod)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)", mvInfo.RefreshStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)", mvInfo.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is not null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvTable.ID)).Check(testkit.Rows("1"))

	tk.MustExec("alter materialized view mv attributes='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes'")
	_, mvInfo = getMView()
	require.Equal(t, int64(5), mvInfo.AlertWarningSec)
	require.Equal(t, int64(10), mvInfo.AlertOverdueSec)
	require.True(t, mvInfo.AlertRefreshFailed)

	err := tk.ExecToErr("alter materialized view mv attributes='mview_alert_warning=20,mview_alert_overdue=10'")
	require.ErrorContains(t, err, "must be less than or equal")

	tk.MustExec("alter materialized view mv refresh")
	_, mvInfo = getMView()
	require.Empty(t, mvInfo.RefreshStartWith)
	require.Empty(t, mvInfo.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvTable.ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewAttributesUpdatesAlertThresholds(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_attr (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_attr (a, b)")
	tk.MustExec("create materialized view mv_attr (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) attributes='mview_alert_warning=300,mview_alert_overdue=600,mview_alert_refresh_failed=no' as select a, sum(b), count(1) from t_mv_attr group by a")

	getMViewInfo := func() *model.MaterializedViewInfo {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_attr"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedView)
		return tbl.Meta().MaterializedView
	}

	mvInfo := getMViewInfo()
	require.Equal(t, int64(300), mvInfo.AlertWarningSec)
	require.Equal(t, int64(600), mvInfo.AlertOverdueSec)
	require.False(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", mvInfo.RefreshNext)

	tk.MustExec("alter materialized view mv_attr attributes='mview_alert_warning=5,mview_alert_overdue=5,mview_alert_refresh_failed=yes'")
	mvInfo = getMViewInfo()
	require.Equal(t, int64(5), mvInfo.AlertWarningSec)
	require.Equal(t, int64(5), mvInfo.AlertOverdueSec)
	require.True(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", mvInfo.RefreshNext)

	tk.MustExec("alter materialized view mv_attr refresh next date_add(now(), interval 25 minute)")
	tk.MustExec("alter materialized view mv_attr attributes='mview_alert_warning=10,mview_alert_overdue=20,mview_alert_refresh_failed=no'")
	mvInfo = getMViewInfo()
	require.Equal(t, int64(10), mvInfo.AlertWarningSec)
	require.Equal(t, int64(20), mvInfo.AlertOverdueSec)
	require.False(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mvInfo.RefreshNext)

	err := tk.ExecToErr("alter materialized view mv_attr attributes='unknown=1'")
	require.ErrorContains(t, err, "unsupported ATTRIBUTES key")
	err = tk.ExecToErr("alter materialized view mv_attr attributes='mview_alert_refresh_failed=maybe'")
	require.ErrorContains(t, err, "must be yes or no")
	err = tk.ExecToErr("alter materialized view mv_attr attributes='mview_alert_warning=20,mview_alert_overdue=10'")
	require.ErrorContains(t, err, "must be less than or equal")
}

func TestAlterMaterializedViewRefreshDisableScheduleUpdatesAlert(t *testing.T) {
	tests := []struct {
		name           string
		refreshFailed  bool
		expectAlertRow bool
	}{
		{name: "clears non-failed alert"},
		{name: "preserves refresh-failed alert", refreshFailed: true, expectAlertRow: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mv_alert (a int not null, b int not null)")
			tk.MustExec("create materialized view log on t_mv_alert (a, b)")
			tk.MustExec("create materialized view mv_alert (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t_mv_alert group by a")

			tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_alert"))
			require.NoError(t, err)
			refreshFailed := "NULL"
			if tt.refreshFailed {
				refreshFailed = "'YES'"
			}
			tk.MustExec(fmt.Sprintf(
				"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, REFRESH_FAILED, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, 'test', 'mv_alert', 'warning', %s, UTC_TIMESTAMP(), UTC_TIMESTAMP())",
				tbl.Meta().ID,
				refreshFailed,
			))

			tk.MustExec("alter materialized view mv_alert refresh")
			tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", tbl.Meta().ID)).Check(testkit.Rows("1"))
			if !tt.expectAlertRow {
				tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", tbl.Meta().ID)).Check(testkit.Rows("0"))
				return
			}
			tk.MustQuery(fmt.Sprintf("select ALERT_LEVEL is null, REFRESH_FAILED from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", tbl.Meta().ID)).Check(testkit.Rows("1 YES"))
		})
	}
}

func TestAlterMaterializedViewRefreshDisableScheduleIgnoresAlertDeleteFailure(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_alert_fail (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_alert_fail (a, b)")
	tk.MustExec("create materialized view mv_alert_fail (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t_mv_alert_fail group by a")

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_alert_fail"))
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, 'test', 'mv_alert_fail', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())",
		tbl.Meta().ID,
	))

	const fp = "github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewRefreshAlertTableNotExists"
	require.NoError(t, failpoint.Enable(fp, "return(true)"))
	defer func() { require.NoError(t, failpoint.Disable(fp)) }()

	tk.MustExec("alter materialized view mv_alert_fail refresh")
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", tbl.Meta().ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewRefreshBestEffortInfoUpdateWarning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tkLock := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tkLock.MustExec("use test")
	tk.MustExec("create table t_mv_info_lock (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_info_lock (a, b)")
	tk.MustExec("create materialized view mv_info_lock (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t_mv_info_lock group by a")

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_info_lock"))
	require.NoError(t, err)
	const expectedNextUnixSeconds int64 = 1_925_089_445
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = %d where MVIEW_ID = %d", expectedNextUnixSeconds, tbl.Meta().ID))
	tkLock.MustExec("begin pessimistic")
	defer tkLock.MustExec("rollback")
	tkLock.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = NEXT_REFRESH_UNIX_SECONDS where MVIEW_ID = %d", tbl.Meta().ID))

	tk.MustExec("alter materialized view mv_info_lock refresh next date_add(now(), interval 25 minute)")
	tk.MustQuery("show warnings").CheckContain("alter materialized view refresh: metadata updated but failed to update mysql.tidb_mview_refresh_info.NEXT_REFRESH_UNIX_SECONDS within 10s due to row lock contention")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_info_lock"))
	require.NoError(t, err)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", tbl.Meta().MaterializedView.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS = %d from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", expectedNextUnixSeconds, tbl.Meta().ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewRefreshUpdatesNextUnixSecondsWithAlterPrivilegeOnly(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create user 'mv_alter_refresh_u'@'%'")
	defer tk.MustExec("drop user 'mv_alter_refresh_u'@'%'")
	tk.MustExec("grant alter on test.mv to 'mv_alter_refresh_u'@'%'")

	tkUser := newMViewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_refresh_u", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("alter materialized view test.mv refresh next date_add(now(), interval 25 minute)")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mvTable.Meta().MaterializedView.RefreshNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 15 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvTable.Meta().ID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestAlterMaterializedViewRefreshScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next cast('2030-01-01 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a")

	getMView := func() (*model.TableInfo, *model.MaterializedViewInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedView)
		return tbl.Meta(), tbl.Meta().MaterializedView
	}

	mvTable, info := getMView()
	initialTimeZone := info.RefreshScheduleTimeZone
	require.Equal(t, 0, initialTimeZone.Offset)

	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("alter materialized view mv refresh")
	_, info = getMView()
	require.Equal(t, initialTimeZone.Name, info.RefreshScheduleTimeZone.Name)
	require.Equal(t, initialTimeZone.Offset, info.RefreshScheduleTimeZone.Offset)
	require.Empty(t, info.RefreshNext)

	tk.MustExec("alter materialized view mv refresh next cast('2030-01-02 10:00:00' as datetime)")
	_, info = getMView()
	require.Equal(t, 8*60*60, info.RefreshScheduleTimeZone.Offset)
	tk.MustQuery("select NEXT_REFRESH_UNIX_SECONDS = 1893549600 from mysql.tidb_mview_refresh_info where MVIEW_ID = " + strconv.FormatInt(mvTable.ID, 10)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewLogDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b varchar(10) not null, c int)")
	tk.MustExec("create materialized view log on t (a) purge next date_add(now(), interval 2 hour)")
	tk.MustExec("alter materialized view log on t add column (b, c)")

	getMLog := func() (*model.TableInfo, *model.MaterializedViewLogInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedViewLog)
		return tbl.Meta(), tbl.Meta().MaterializedViewLog
	}

	mlogTable, mlogInfo := getMLog()
	require.Equal(t, []ast.CIStr{ast.NewCIStr("a"), ast.NewCIStr("b"), ast.NewCIStr("c")}, mlogInfo.Columns)
	columnNames := make([]string, 0, len(mlogTable.Columns))
	for _, col := range mlogTable.Columns {
		columnNames = append(columnNames, col.Name.O)
	}
	require.Equal(t, []string{"a", "b", "c", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"}, columnNames)

	err := tk.ExecToErr("alter materialized view log on t add column (b)")
	require.ErrorContains(t, err, "Duplicate column name")
	err = tk.ExecToErr("alter materialized view log on t add column (missing_col)")
	require.ErrorContains(t, err, "Unknown column")

	tk.MustExec("alter materialized view log on t purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	mlogTable, mlogInfo = getMLog()
	require.Equal(t, "DEFERRED", mlogInfo.PurgeMethod)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)", mlogInfo.PurgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)", mlogInfo.PurgeNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS is not null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogTable.ID)).Check(testkit.Rows("1"))

	tk.MustExec("alter materialized view log on t purge")
	_, mlogInfo = getMLog()
	require.Empty(t, mlogInfo.PurgeStartWith)
	require.Empty(t, mlogInfo.PurgeNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogTable.ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewLogPurgeExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a) purge next date_add(now(), interval 1 hour)")

	err := tk.ExecToErr("alter materialized view log on t purge start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "PURGE START WITH expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view log on t purge next 300")
	require.ErrorContains(t, err, "PURGE NEXT expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view log on t purge immediate")
	require.ErrorContains(t, err, "PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG")

	tk.MustExec("alter materialized view log on t purge start with now() next date_add(now(), interval 1 hour)")
}

func TestAlterMaterializedViewLogPurgeScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a) purge next cast('2030-01-01 10:00:00' as datetime)")

	getMLog := func() (*model.TableInfo, *model.MaterializedViewLogInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedViewLog)
		return tbl.Meta(), tbl.Meta().MaterializedViewLog
	}

	mlogTable, info := getMLog()
	initialTimeZone := info.PurgeScheduleTimeZone
	require.Equal(t, 0, initialTimeZone.Offset)

	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("alter materialized view log on t purge")
	_, info = getMLog()
	require.Equal(t, initialTimeZone.Name, info.PurgeScheduleTimeZone.Name)
	require.Equal(t, initialTimeZone.Offset, info.PurgeScheduleTimeZone.Offset)
	require.Empty(t, info.PurgeNext)

	tk.MustExec("alter materialized view log on t purge next cast('2030-01-02 10:00:00' as datetime)")
	_, info = getMLog()
	require.Equal(t, 8*60*60, info.PurgeScheduleTimeZone.Offset)
	tk.MustQuery("select NEXT_PURGE_UNIX_SECONDS = 1893549600 from mysql.tidb_mlog_purge_info where MLOG_ID = " + strconv.FormatInt(mlogTable.ID, 10)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewLogPurgeUpdatesNextUnixSecondsWithMLogAlterPrivilege(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_purge_priv (a int, b int)")
	tk.MustExec("create materialized view log on t_mlog_purge_priv (a, b) purge next date_add(now(), interval 2 hour)")
	tk.MustExec("create user 'mv_alter_purge_u'@'%'")
	tk.MustExec("create user 'mv_alter_purge_select_u'@'%'")
	defer tk.MustExec("drop user 'mv_alter_purge_u'@'%'")
	defer tk.MustExec("drop user 'mv_alter_purge_select_u'@'%'")
	tk.MustExec("grant alter on test.`$mlog$t_mlog_purge_priv` to 'mv_alter_purge_u'@'%'")
	tk.MustExec("grant select on test.t_mlog_purge_priv to 'mv_alter_purge_select_u'@'%'")

	tkUser := newMViewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_purge_u", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("alter materialized view log on test.t_mlog_purge_priv purge next date_add(now(), interval 25 minute)")

	tkSelectUser := newMViewTestKit(t, store)
	require.NoError(t, tkSelectUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_purge_select_u", Hostname: "%"}, nil, nil, nil))
	err := tkSelectUser.ExecToErr("alter materialized view log on test.t_mlog_purge_priv purge next date_add(now(), interval 30 minute)")
	require.ErrorContains(t, err, "ALTER command denied")

	mlogTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_purge_priv"))
	require.NoError(t, err)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mlogTable.Meta().MaterializedViewLog.PurgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 15 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogTable.Meta().ID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestAlterMaterializedViewLogAddColumnBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_add_column (id int not null, n int not null, s varchar(10) not null, d date not null, note text not null, untouched int)")
	tk.MustExec("create materialized view log on t_mlog_add_column (id)")
	mustExecMViewMaintenance(t, tk, "insert into `$mlog$t_mlog_add_column` (id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`) values (1, 'I', 1)")

	tk.MustExec("alter materialized view log on t_mlog_add_column add column (n, s, d, note)")
	mlogTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_add_column"))
	require.NoError(t, err)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("id"), ast.NewCIStr("n"), ast.NewCIStr("s"), ast.NewCIStr("d"), ast.NewCIStr("note")}, mlogTable.Meta().MaterializedViewLog.Columns)

	columnNames := make([]string, 0, len(mlogTable.Meta().Columns))
	columns := make(map[string]*model.ColumnInfo, len(mlogTable.Meta().Columns))
	for _, column := range mlogTable.Meta().Columns {
		columnNames = append(columnNames, column.Name.O)
		columns[column.Name.L] = column
	}
	require.Equal(t, []string{"id", "n", "s", "d", "note", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"}, columnNames)
	for _, name := range []string{"n", "s", "d", "note"} {
		require.True(t, mysql.HasNotNullFlag(columns[name].GetFlag()))
	}
	require.Equal(t, "0", fmt.Sprint(columns["n"].GetOriginDefaultValue()))
	require.Equal(t, " ", fmt.Sprint(columns["s"].GetOriginDefaultValue()))
	require.Equal(t, "0000-00-00", fmt.Sprint(columns["d"].GetOriginDefaultValue()))
	require.Equal(t, " ", fmt.Sprint(columns["note"].GetOriginDefaultValue()))
	require.Equal(t, mysql.TypeBlob, columns["note"].GetType())
	tk.MustQuery("select n, hex(s), cast(d as char), hex(note), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_add_column`").Check(testkit.Rows("0 20 0000-00-00 20 I 1"))

	tk.MustExec("create table t_mlog_add_column_atomic (id int, b int, c int)")
	tk.MustExec("create materialized view log on t_mlog_add_column_atomic (id)")
	cancelTK := testkit.NewTestKit(t, store)
	cancelTK.MustExec("use test")
	cancelTriggered := atomic.Bool{}
	cancelDone := make(chan error, 1)
	const fp = "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced"
	require.NoError(t, failpoint.EnableCall(fp, func(job *model.Job) {
		if !cancelTriggered.CompareAndSwap(false, true) {
			return
		}
		if job.Type != model.ActionMultiSchemaChange || job.SchemaName != "test" || job.TableName != "$mlog$t_mlog_add_column_atomic" || job.MultiSchemaInfo == nil || len(job.MultiSchemaInfo.SubJobs) != 2 || job.MultiSchemaInfo.SubJobs[1].SchemaState != model.StateWriteReorganization {
			cancelTriggered.Store(false)
			return
		}
		errs, err := ddl.CancelJobs(context.Background(), cancelTK.Session(), []int64{job.ID})
		if len(errs) > 0 && errs[0] != nil {
			cancelDone <- errs[0]
			return
		}
		cancelDone <- err
	}))
	err = tk.ExecToErr("alter materialized view log on t_mlog_add_column_atomic add column (b, c)")
	require.NoError(t, failpoint.Disable(fp))
	require.ErrorContains(t, err, "Cancelled DDL job")
	select {
	case cancelErr := <-cancelDone:
		require.NoError(t, cancelErr)
	default:
		require.FailNow(t, "expected mlog multi-column add cancellation")
	}
	atomicMLog, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_add_column_atomic"))
	require.NoError(t, err)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("id")}, atomicMLog.Meta().MaterializedViewLog.Columns)
	require.Nil(t, model.FindColumnInfo(atomicMLog.Meta().Columns, "b"))
	require.Nil(t, model.FindColumnInfo(atomicMLog.Meta().Columns, "c"))
}

func TestAlterMaterializedViewLogAddColumnDefaultSemantics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_add_defaults (id int, nullable_varchar varchar(10), nullable_text text, nn_enum enum('a','b') not null, nn_set set('x','y') not null, nullable_enum enum('a','b'), nullable_set set('x','y'), nn_varchar varchar(10) not null, nn_text text not null)")
	tk.MustExec("create materialized view log on t_mlog_add_defaults (id)")
	mustExecMViewMaintenance(t, tk, "insert into `$mlog$t_mlog_add_defaults` (id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`) values (1, 'I', 1)")

	tk.MustExec("alter materialized view log on t_mlog_add_defaults add column (nullable_varchar, nullable_text, nn_enum, nn_set, nullable_enum, nullable_set, nn_varchar, nn_text)")
	tk.MustQuery("select nullable_varchar is null, nullable_text is null, cast(nn_enum as char), cast(nn_set as char), nullable_enum is null, nullable_set is null, hex(nn_varchar), hex(nn_text), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_add_defaults`").Check(testkit.Rows("1 1 a  1 1 20 20 I 1"))
}

func TestAlterMaterializedViewLogPurgeBestEffortInfoUpdateWarning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tkLock := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tkLock.MustExec("use test")
	tk.MustExec("create table t_mlog_info_lock (a int, b int)")
	tk.MustExec("create materialized view log on t_mlog_info_lock (a, b) purge next date_add(now(), interval 2 hour)")

	mlogTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_info_lock"))
	require.NoError(t, err)
	const expectedNextUnixSeconds int64 = 1_925_089_445
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = %d where MLOG_ID = %d", expectedNextUnixSeconds, mlogTable.Meta().ID))
	tkLock.MustExec("begin pessimistic")
	defer tkLock.MustExec("rollback")
	tkLock.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = NEXT_PURGE_UNIX_SECONDS where MLOG_ID = %d", mlogTable.Meta().ID))

	tk.MustExec("alter materialized view log on t_mlog_info_lock purge next date_add(now(), interval 25 minute)")
	tk.MustQuery("show warnings").CheckContain("alter materialized view log purge: metadata updated but failed to update mysql.tidb_mlog_purge_info.NEXT_PURGE_UNIX_SECONDS within 10s due to row lock contention")
	mlogTable, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_info_lock"))
	require.NoError(t, err)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mlogTable.Meta().MaterializedViewLog.PurgeNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS = %d from mysql.tidb_mlog_purge_info where MLOG_ID = %d", expectedNextUnixSeconds, mlogTable.Meta().ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewLogAddColumnPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_priv (a int, b int)")
	tk.MustExec("create materialized view log on t_add_mlog_priv (a)")
	tk.MustExec("create user 'u_add_mlog_no_select'@'%'")
	tk.MustExec("create user 'u_add_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_add_mlog_no_select'@'%'")
	defer tk.MustExec("drop user 'u_add_mlog_ok'@'%'")

	tk.MustExec("grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_no_select'@'%'")
	tkNoSelect := newMViewTestKit(t, store)
	require.NoError(t, tkNoSelect.Session().Auth(&auth.UserIdentity{Username: "u_add_mlog_no_select", Hostname: "%"}, nil, nil, nil))
	err := tkNoSelect.ExecToErr("alter materialized view log on test.t_add_mlog_priv add column (b)")
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_add_mlog_priv to 'u_add_mlog_ok'@'%'")
	tkOK := newMViewTestKit(t, store)
	require.NoError(t, tkOK.Session().Auth(&auth.UserIdentity{Username: "u_add_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkOK.MustExec("alter materialized view log on test.t_add_mlog_priv add column (b)")
}

func TestAlterMaterializedViewLogAddColumnRejectsInvalidColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_invalid (a int, b int, c int)")
	tk.MustExec("create materialized view log on t_add_mlog_invalid (a)")

	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (a)", errno.ErrDupFieldName)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (b, b)", errno.ErrDupFieldName)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (missing_col)", errno.ErrBadField)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (`_MLOG$_DML_TYPE`)", errno.ErrDupFieldName)
	tk.MustGetErrMsg(
		"alter materialized view log on t_add_mlog_invalid add column (b), add column (c)",
		"[ddl:8200]Unsupported ALTER MATERIALIZED VIEW LOG with multiple ADD COLUMN actions",
	)

	tk.MustExec("create table t_add_mlog_unsupported (id int, b blob, j json, g1 int, g2 int as (g1 + 1) stored)")
	tk.MustExec("create materialized view log on t_add_mlog_unsupported (id)")
	err := tk.ExecToErr("alter materialized view log on t_add_mlog_unsupported add column (b)")
	require.ErrorContains(t, err, "ALTER MATERIALIZED VIEW LOG does not support BLOB column b")
	err = tk.ExecToErr("alter materialized view log on t_add_mlog_unsupported add column (j)")
	require.ErrorContains(t, err, "ALTER MATERIALIZED VIEW LOG does not support JSON column j")
	tk.MustExec("alter materialized view log on t_add_mlog_unsupported add column (g2)")
}

func TestAlterMaterializedViewLogAddColumnCopiesBaseColumnMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_col (id int not null, n int not null, s varchar(10) not null, d date not null, note text not null)")
	tk.MustExec("create materialized view log on t_add_mlog_col (id)")
	tk.MustExec("alter materialized view log on t_add_mlog_col add column (n, s, d, note)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_add_mlog_col"))
	require.NoError(t, err)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("id"), ast.NewCIStr("n"), ast.NewCIStr("s"), ast.NewCIStr("d"), ast.NewCIStr("note")}, mlogTable.Meta().MaterializedViewLog.Columns)

	columns := make(map[string]*model.ColumnInfo, len(mlogTable.Meta().Columns))
	for _, column := range mlogTable.Meta().Columns {
		columns[column.Name.L] = column
	}
	for _, name := range []string{"n", "s", "d", "note"} {
		require.True(t, mysql.HasNotNullFlag(columns[name].GetFlag()))
	}
	require.Equal(t, mysql.TypeBlob, columns["note"].GetType())
}

func TestAlterMaterializedViewLogAddColumnSupportsNewMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_mv (a int not null, b int not null, c int not null)")
	tk.MustExec("insert into t_add_mlog_mv values (1, 10, 100), (1, 20, 200), (2, 30, 300)")
	tk.MustExec("create materialized view log on t_add_mlog_mv (a, b)")

	err := tk.ExecToErr("create materialized view mv_add_mlog_col_before (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a")
	require.ErrorContains(t, err, "materialized view log does not contain column c")

	tk.MustExec("alter materialized view log on t_add_mlog_mv add column (c)")
	tk.MustExec("create materialized view mv_add_mlog_col_after (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a")
	tk.MustQuery("select a, s, cnt from mv_add_mlog_col_after order by a").Check(testkit.Rows("1 300 2", "2 300 1"))
}

func TestAlterTableWithMaterializedViewDependencies(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, -1)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, cnt) as select a, count(1) from t group by a")
	tk.MustExec("create materialized view mv_where (a, cnt) as select a, count(1) from t where b > 0 group by a")

	tk.MustExec("alter table t modify column a bigint not null")
	showCreate := tk.MustQuery("show create table `$mlog$t`").Rows()[0][1].(string)
	require.Contains(t, showCreate, "`a` bigint")
	showCreate = tk.MustQuery("show create table mv").Rows()[0][1].(string)
	require.Contains(t, showCreate, "`a` bigint")

	err := tk.ExecToErr("alter table t modify column b bigint not null")
	require.ErrorContains(t, err, "does not support modifying columns used in WHERE clause")
	err = tk.ExecToErr("alter table t change column a a2 bigint")
	require.ErrorContains(t, err, "does not support renaming")
	err = tk.ExecToErr("alter table t modify column b smallint")
	require.ErrorContains(t, err, "only supports no-reorg compatible type changes")
}

func TestMaterializedViewBaseModifyColumnMultiSchemaInvolvingSchemaInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_multi_schema (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_multi_schema (a, b)")
	tk.MustExec("create materialized view mv_multi_schema (a, b, cnt) as select a, b, count(1) from t_multi_schema group by a, b")
	tk.MustExec("alter table t_multi_schema modify column a bigint not null, modify column b bigint not null")

	var historyJob *model.Job
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		jobs, err := ddl.GetLastNHistoryDDLJobs(m, 16)
		if err != nil {
			return err
		}
		for _, job := range jobs {
			if job.Type == model.ActionMultiSchemaChange && job.TableName == "t_multi_schema" {
				historyJob = job
				return nil
			}
		}
		return nil
	}))
	require.NotNil(t, historyJob)
	involving := make(map[string]struct{}, len(historyJob.GetInvolvingSchemaInfo()))
	for _, info := range historyJob.GetInvolvingSchemaInfo() {
		involving[info.Database+"\x00"+info.Table] = struct{}{}
	}
	require.Equal(t, map[string]struct{}{
		"test\x00t_multi_schema":       {},
		"test\x00$mlog$t_multi_schema": {},
		"test\x00mv_multi_schema":      {},
	}, involving)

	showCreate := tk.MustQuery("show create table t_multi_schema").Rows()[0][1].(string)
	require.Contains(t, showCreate, "`a` bigint")
	require.Contains(t, showCreate, "`b` bigint")
	showCreate = tk.MustQuery("show create table `$mlog$t_multi_schema`").Rows()[0][1].(string)
	require.Contains(t, showCreate, "`a` bigint")
	require.Contains(t, showCreate, "`b` bigint")
	showCreate = tk.MustQuery("show create table mv_multi_schema").Rows()[0][1].(string)
	require.Contains(t, showCreate, "`a` bigint")
	require.Contains(t, showCreate, "`b` bigint")
}

func TestMaterializedViewDDLProtectsMinMaxSupportingBaseTableIndexes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	createMinMaxMV := func(baseTable, mvName, indexDDL string) {
		tk.MustExec(fmt.Sprintf("create table %s (a int not null, b int not null, c int not null%s)", baseTable, indexDDL))
		tk.MustExec(fmt.Sprintf("create materialized view log on %s (a, b, c)", baseTable))
		tk.MustExec(fmt.Sprintf("create materialized view %s (a, b, minc, cnt) refresh fast next now() as select a, b, min(c), count(1) from %s group by a, b", mvName, baseTable))
	}

	createMinMaxMV("t_drop_idx", "mv_drop_idx", ", index idx_ab(a, b)")
	err := tk.ExecToErr("drop index idx_ab on t_drop_idx")
	require.ErrorContains(t, err, "required by materialized view mv_drop_idx")
	require.ErrorContains(t, err, "MIN/MAX fast refresh")

	createMinMaxMV("t_invisible_idx", "mv_invisible_idx", ", index idx_ab(a, b)")
	err = tk.ExecToErr("alter table t_invisible_idx alter index idx_ab invisible")
	require.ErrorContains(t, err, "required by materialized view mv_invisible_idx")

	createMinMaxMV("t_multi_schema_replace_idx", "mv_multi_schema_replace_idx", ", index idx_ab(a, b)")
	tk.MustExec("alter table t_multi_schema_replace_idx drop index idx_ab, add index idx_ba(b, a)")

	createMinMaxMV("t_multi_schema_bad_idx", "mv_multi_schema_bad_idx", ", index idx_ab(a, b)")
	err = tk.ExecToErr("alter table t_multi_schema_bad_idx drop index idx_ab, add index idx_c(c)")
	require.ErrorContains(t, err, "required by materialized view mv_multi_schema_bad_idx")
}

func TestMaterializedViewDDLProtectsMinMaxSupportingBaseTableIndexesAgainstConcurrentDrop(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_concurrent_drop_idx (a int not null, b int not null, c int not null, index idx_ab(a, b), index idx_ba(b, a))")
	tk.MustExec("create materialized view log on t_concurrent_drop_idx (a, b, c)")
	tk.MustExec("create materialized view mv_concurrent_drop_idx (a, b, minc, cnt) refresh fast next now() as select a, b, min(c), count(1) from t_concurrent_drop_idx group by a, b")

	var pausedJobID atomic.Int64
	pauseCh := make(chan struct{})
	blockedCh := make(chan struct{}, 1)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionDropIndex || job.TableName != "t_concurrent_drop_idx" || job.SchemaState != model.StatePublic {
			return
		}
		if !pausedJobID.CompareAndSwap(0, job.ID) {
			return
		}
		select {
		case blockedCh <- struct{}{}:
		default:
		}
		<-pauseCh
	})

	tk1 := newMViewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := newMViewTestKit(t, store)
	tk2.MustExec("use test")

	var (
		err1 error
		err2 error
		wg   sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err1 = tk1.ExecToErr("drop index idx_ab on t_concurrent_drop_idx")
	}()

	select {
	case <-blockedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first drop index job to pause")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err2 = tk2.ExecToErr("drop index idx_ba on t_concurrent_drop_idx")
	}()

	require.Eventually(t, func() bool {
		return fmt.Sprint(tk.MustQuery("select count(*) from mysql.tidb_ddl_job").Rows()[0][0]) == "2"
	}, 5*time.Second, 50*time.Millisecond)

	close(pauseCh)
	wg.Wait()

	require.NoError(t, err1)
	require.ErrorContains(t, err2, "required by materialized view mv_concurrent_drop_idx")
	require.ErrorContains(t, err2, "MIN/MAX fast refresh")
}

func TestMaterializedViewRelatedTablesDDLRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_ddl_mv (a int not null, b int)")
	tk.MustExec("create materialized view log on t_ddl_mv (a, b)")

	err := tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")
	err = tk.ExecToErr("rename table t_ddl_mv to t_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on base table with materialized view log")
	err = tk.ExecToErr("drop table `$mlog$t_ddl_mv`")
	require.ErrorContains(t, err, "DROP TABLE on materialized view log table")

	tk.MustExec("create materialized view mv_ddl_mv (a, cnt) as select a, count(1) from t_ddl_mv group by a")
	tk.MustExec("alter table t_ddl_mv add column c int")
	err = tk.ExecToErr("alter table t_ddl_mv modify column a bigint")
	require.ErrorContains(t, err, "does not support changing charset/collation/nullability of group keys")
	err = tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")
	err = tk.ExecToErr("alter table mv_ddl_mv add column x int")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view table")
	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` add index idx_mlog_b(b)")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view log table")
	err = tk.ExecToErr("create index idx_mlog_b_create on `$mlog$t_ddl_mv`(b)")
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
}
