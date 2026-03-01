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

package executor

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func mustExecInternal(t *testing.T, tk *testkit.TestKit, sql string) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMVMaintenance)
	rs, err := tk.Session().ExecuteInternal(ctx, sql)
	require.NoError(t, err)
	require.Nil(t, rs)
}

func TestMaterializedViewRefreshCompleteBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, oldTSRow, 1)
	oldTS, err := strconv.ParseUint(fmt.Sprintf("%v", oldTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, oldTS)

	// Make MV stale by changing base table.
	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	tk.MustExec("refresh materialized view mv complete")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))

	newTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, newTSRow, 1)
	newTS, err := strconv.ParseUint(fmt.Sprintf("%v", newTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	t.Logf("mview refresh tso: old=%d new=%d", oldTS, newTS)

	require.NotEqual(t, oldTS, newTS)

	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO > 0 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("success complete manually 1 1 1 1"))
}

func TestMaterializedViewRefreshNextTimeOnlyUpdatesForInternalSQL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_internal_next (a int not null, b int not null)")
	tk.MustExec("insert into t_internal_next values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_internal_next (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_internal_next (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t_internal_next group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_internal_next"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_TIME = null where MVIEW_ID = %d", mviewID))

	// User SQL refresh should not update NEXT_TIME.
	tk.MustExec("refresh materialized view mv_internal_next complete")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete manually"))

	// Internal SQL refresh should update NEXT_TIME by evaluating RefreshNext.
	mustExecInternal(t, tk, "refresh materialized view mv_internal_next complete")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 20 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete automatically"))
}

func TestMaterializedViewRefreshInternalSQLStartWithNoNextSetsNextTimeNull(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_internal_start_only (a int not null, b int not null)")
	tk.MustExec("insert into t_internal_start_only values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_internal_start_only (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_internal_start_only (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t_internal_start_only group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_internal_start_only"))
	require.NoError(t, err)
	require.NotNil(t, mvTable.Meta().MaterializedView)
	// Simulate scheduler metadata state: START WITH is set but NEXT is empty.
	mvTable.Meta().MaterializedView.RefreshStartWith = "DATE_ADD(NOW(), INTERVAL 2 HOUR)"
	mvTable.Meta().MaterializedView.RefreshNext = ""
	mviewID := mvTable.Meta().ID

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_TIME = UTC_TIMESTAMP() + interval 3 hour where MVIEW_ID = %d", mviewID))

	// User SQL refresh should keep NEXT_TIME unchanged.
	tk.MustExec("refresh materialized view mv_internal_start_only complete")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is not null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete manually"))

	// Internal SQL refresh should explicitly set NEXT_TIME = NULL when START WITH exists and NEXT is empty.
	mustExecInternal(t, tk, "refresh materialized view mv_internal_start_only complete")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete automatically"))
}

func TestMaterializedViewRefreshFastMethodTracksManualAndAutomatic(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv fast")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery("select REFRESH_METHOD, REFRESH_ROWS > 0 from mysql.tidb_mview_refresh_hist order by REFRESH_JOB_ID desc limit 1").
		Check(testkit.Rows("fast manually 1"))

	tk.MustExec("insert into t values (4, 8)")
	mustExecInternal(t, tk, "refresh materialized view mv fast")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1", "4 8 1"))
	tk.MustQuery("select REFRESH_METHOD, REFRESH_ROWS > 0 from mysql.tidb_mview_refresh_hist order by REFRESH_JOB_ID desc limit 1").
		Check(testkit.Rows("fast automatically 1"))
}

func TestMaterializedViewRefreshCompleteUsesDefinitionSessionSemantics(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.sql_mode = 'NO_UNSIGNED_SUBTRACTION'")
	tk.MustExec("set @@session.time_zone = '+00:00'")
	tk.MustExec("create table t (a int not null, b int not null, ts timestamp not null)")
	tk.MustExec("insert into t values (1, 10, '2020-01-01 00:30:00'), (2, 20, '2019-12-31 23:30:00')")
	tk.MustExec("create materialized view log on t (a, b, ts) purge next date_add(now(), interval 1 hour)")
	tk.MustExec(`create materialized view mv (a, s, cnt) refresh fast next now() as
select a, sum(b), count(1) from t
where ts >= '2020-01-01 00:00:00'
group by a`)
	tk.MustQuery("select a, s, cnt from mv").Check(testkit.Rows("1 10 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	expectedSQLMode := mvTable.Meta().MaterializedView.DefinitionSQLMode
	expectedLoc, err := mvTable.Meta().MaterializedView.DefinitionTimeZone.GetLocation()
	require.NoError(t, err)
	expectedTimeZone := expectedLoc.String()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInitSession", func(mode mysql.SQLMode, tz string) {
		require.Equal(t, expectedSQLMode, mode)
		require.Equal(t, expectedTimeZone, tz)
	})

	// Keep creator/session semantics and refresh caller semantics different.
	tk.MustExec("set @@session.sql_mode = ''")
	tk.MustExec("set @@session.time_zone = '-01:00'")
	tk.MustExec("refresh materialized view mv complete")
	tk.MustQuery("select a, s, cnt from mv").Check(testkit.Rows("1 10 1"))
}

func TestMaterializedViewRefreshCompleteFinalizeHistoryRetry(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec("insert into t values (2, 3), (3, 4)")

	const finalizeFailpoint = "github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError"
	require.NoError(t, failpoint.Enable(finalizeFailpoint, "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(finalizeFailpoint))
	}()

	tk.MustExec("refresh materialized view mv complete")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.Rows("success complete manually 1 1 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'", mviewID)).
		Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshCompleteRunningHistLifecycle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec("insert into t values (2, 3), (3, 4)")

	const afterInsertHistRunningFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInsertRefreshHistRunning"
	pauseCh := make(chan struct{})
	hitCh := make(chan struct{})
	require.NoError(t, failpoint.EnableCall(afterInsertHistRunningFailpoint, func() {
		select {
		case <-hitCh:
		default:
			close(hitCh)
		}
		<-pauseCh
	}))
	defer func() {
		select {
		case <-pauseCh:
		default:
			close(pauseCh)
		}
		require.NoError(t, failpoint.Disable(afterInsertHistRunningFailpoint))
	}()

	refreshDone := make(chan error, 1)
	go func() {
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete")
	}()

	select {
	case <-hitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to persist running history row")
	}

	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.Rows("running complete manually 1 1 1"))

	close(pauseCh)

	select {
	case err := <-refreshDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to finish")
	}

	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.Rows("success complete manually 1 1 1"))
}

func TestMaterializedViewRefreshCompleteReadTSOSnapshotMatchesMV(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	// Make MV stale by changing base table.
	tk.MustExec("insert into t values (1, 1)")

	const afterBeginFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterBegin"
	pauseCh := make(chan struct{})
	hitCh := make(chan struct{})
	require.NoError(t, failpoint.EnableCall(afterBeginFailpoint, func() {
		select {
		case <-hitCh:
		default:
			close(hitCh)
		}
		<-pauseCh
	}))
	defer func() {
		select {
		case <-pauseCh:
		default:
			close(pauseCh)
		}
		require.NoError(t, failpoint.Disable(afterBeginFailpoint))
	}()

	refreshDone := make(chan error, 1)
	go func() {
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete")
	}()

	select {
	case <-hitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to reach failpoint")
	}

	// Commit new base table data after refresh txn start_ts but before refresh reads with for_update_ts.
	tkConcurrent := testkit.NewTestKit(t, store)
	tkConcurrent.MustExec("use test")
	tkConcurrent.MustExec("insert into t values (2, 7)")

	close(pauseCh)

	select {
	case err := <-refreshDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to finish")
	}

	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 16 3", "2 7 1"))

	tsRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, tsRow, 1)
	refreshReadTSO, err := strconv.ParseUint(fmt.Sprintf("%v", tsRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, refreshReadTSO)

	tk.MustExec("set @@tidb_snapshot = '" + strconv.FormatUint(refreshReadTSO, 10) + "'")
	tk.MustQuery("select a, sum(b), count(1) from t group by a order by a").Check(testkit.Rows("1 16 3", "2 7 1"))
	tk.MustExec("set @@tidb_snapshot = ''")
}

func TestMaterializedViewRefreshCompleteRefreshInfoCASUpdateAfterConcurrentPreUpdate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	// Make MV stale by changing base table.
	tk.MustExec("insert into t values (2, 3)")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, oldTSRow, 1)
	oldTS, err := strconv.ParseUint(fmt.Sprintf("%v", oldTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, oldTS)

	const afterBeginFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterBegin"
	pauseCh := make(chan struct{})
	hitCh := make(chan struct{})
	require.NoError(t, failpoint.EnableCall(afterBeginFailpoint, func() {
		select {
		case <-hitCh:
		default:
			close(hitCh)
		}
		<-pauseCh
	}))
	defer func() {
		select {
		case <-pauseCh:
		default:
			close(pauseCh)
		}
		require.NoError(t, failpoint.Disable(afterBeginFailpoint))
	}()

	refreshDone := make(chan error, 1)
	go func() {
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete")
	}()

	select {
	case <-hitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to reach failpoint")
	}

	// Update refresh info row after refresh txn start_ts but before it does the locking read.
	injectedTS := oldTS + 1000
	tkConcurrent := testkit.NewTestKit(t, store)
	tkConcurrent.MustExec("use test")
	tkConcurrent.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d", injectedTS, mviewID))

	close(pauseCh)

	select {
	case err := <-refreshDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to finish")
	}

	// Refresh should succeed and must override LAST_SUCCESS_READ_TSO by this refresh's read tso.
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2"))

	newTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, newTSRow, 1)
	newTS, err := strconv.ParseUint(fmt.Sprintf("%v", newTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, newTS)
	require.NotEqual(t, injectedTS, newTS)

	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO = %d from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", newTS, mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", newTS, mviewID)).
		Check(testkit.Rows("success complete manually 1 1 1"))
}

func TestMaterializedViewRefreshCompleteConcurrentNowait(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tkLocker := testkit.NewTestKit(t, store)
	tkLocker.MustExec("use test")
	tkLocker.MustExec("begin pessimistic")
	tkLocker.MustQuery(fmt.Sprintf("select MVIEW_ID from mysql.tidb_mview_refresh_info where MVIEW_ID = %d for update", mviewID))

	tkRefresh := testkit.NewTestKit(t, store)
	tkRefresh.MustExec("use test")
	err = tkRefresh.ExecToErr("refresh materialized view mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "NOWAIT")

	tkLocker.MustExec("rollback")

	// After releasing the lock, refresh should succeed.
	tkRefresh.MustExec("refresh materialized view mv complete")
}

func TestMaterializedViewRefreshCompleteMissingRefreshInfoRow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID))
	err = tk.ExecToErr("refresh materialized view mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
}

func TestMaterializedViewRefreshWithSyncModeComplete(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv with sync mode complete")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
}

func TestMaterializedViewRefreshCompleteFailureKeepsRefreshInfoReadTSO(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, oldTSRow, 1)
	oldTS, err := strconv.ParseUint(fmt.Sprintf("%v", oldTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, oldTS)

	// Create a unique index that will be violated after refresh.
	tk.MustExec("create unique index idx_unique_s on mv (s)")

	// Make the refreshed MV contain duplicate `s` values: sum(b) for a=2 becomes 15.
	tk.MustExec("insert into t values (2, 8)")

	const finalizeFailpoint = "github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError"
	require.NoError(t, failpoint.Enable(finalizeFailpoint, "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(finalizeFailpoint))
	}()

	err = tk.ExecToErr("refresh materialized view mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "Duplicate")

	// MV data should remain unchanged due to transactional refresh.
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO = %d from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldTS, mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.Rows("failed complete manually 1 1 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'", mviewID)).
		Check(testkit.Rows("0"))
	reasonRow := tk.MustQuery(fmt.Sprintf("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).Rows()
	require.Len(t, reasonRow, 1)
	require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "Duplicate")
}

func TestMaterializedViewRefreshCompleteWithConstraintCheckInPlacePessimisticOff(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	// Turn off in-place constraint check for pessimistic txn.
	// Refresh should still succeed.
	tk.MustExec("set session tidb_constraint_check_in_place_pessimistic = off")
	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv complete")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery("select @@tidb_constraint_check_in_place_pessimistic").Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshRequiresAlterPrivilege(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create user 'mv_refresh_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'mv_refresh_u'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_refresh_u", Hostname: "%"}, nil, nil, nil))

	err := tkUser.ExecToErr("refresh materialized view test.mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "ALTER command denied")

	tk.MustExec("grant alter on test.mv to 'mv_refresh_u'@'%'")
	tkUser.MustExec("refresh materialized view test.mv complete")
}
