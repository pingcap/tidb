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

package ddl_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
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
