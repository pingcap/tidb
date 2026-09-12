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

package materializedview_test

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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

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
	initialTimeZoneName := info.RefreshScheduleTimeZone.Name
	initialTimeZoneOffset := info.RefreshScheduleTimeZone.Offset
	require.Equal(t, 0, initialTimeZoneOffset)

	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("alter materialized view mv refresh")
	_, info = getMView()
	require.Equal(t, initialTimeZoneName, info.RefreshScheduleTimeZone.Name)
	require.Equal(t, initialTimeZoneOffset, info.RefreshScheduleTimeZone.Offset)
	require.Empty(t, info.RefreshNext)

	tk.MustExec("alter materialized view mv refresh next cast('2030-01-02 10:00:00' as datetime)")
	_, info = getMView()
	require.Equal(t, 8*60*60, info.RefreshScheduleTimeZone.Offset)
	tk.MustQuery("select NEXT_REFRESH_UNIX_SECONDS = 1893549600 from mysql.tidb_mview_refresh_info where MVIEW_ID = " + strconv.FormatInt(mvTable.ID, 10)).Check(testkit.Rows("1"))
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
