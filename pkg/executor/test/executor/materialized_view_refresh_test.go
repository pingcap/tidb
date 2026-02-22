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
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewRefreshCompleteBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESSFUL_REFRESH_READ_TSO from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, oldTSRow, 1)
	oldTS, err := strconv.ParseUint(fmt.Sprintf("%v", oldTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, oldTS)

	// Make MV stale by changing base table.
	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	tk.MustExec("refresh materialized view mv complete")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))

	newTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESSFUL_REFRESH_READ_TSO from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, newTSRow, 1)
	newTS, err := strconv.ParseUint(fmt.Sprintf("%v", newTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	t.Logf("mview refresh tso: old=%d new=%d", oldTS, newTS)

	require.NotEqual(t, oldTS, newTS)

	tk.MustQuery(fmt.Sprintf("select LAST_REFRESH_RESULT, LAST_REFRESH_TYPE, LAST_SUCCESSFUL_REFRESH_READ_TSO > 0 from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("success complete 1"))
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
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
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

	tsRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESSFUL_REFRESH_READ_TSO from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, tsRow, 1)
	refreshReadTSO, err := strconv.ParseUint(fmt.Sprintf("%v", tsRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, refreshReadTSO)

	tk.MustExec("set @@tidb_snapshot = '" + strconv.FormatUint(refreshReadTSO, 10) + "'")
	tk.MustQuery("select a, sum(b), count(1) from t group by a order by a").Check(testkit.Rows("1 16 3", "2 7 1"))
	tk.MustExec("set @@tidb_snapshot = ''")
}

func TestMaterializedViewRefreshCompleteAbortOnRefreshInfoReadTSOMismatch(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	// Make MV stale by changing base table.
	tk.MustExec("insert into t values (2, 3)")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESSFUL_REFRESH_READ_TSO from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).Rows()
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
	tkConcurrent := testkit.NewTestKit(t, store)
	tkConcurrent.MustExec("use test")
	tkConcurrent.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh set LAST_SUCCESSFUL_REFRESH_READ_TSO = %d where MVIEW_ID = %d", oldTS+1, mviewID))

	close(pauseCh)

	select {
	case err := <-refreshDone:
		require.Error(t, err)
		require.ErrorContains(t, err, "inconsistent LAST_SUCCESSFUL_REFRESH_READ_TSO")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to finish")
	}

	// Refresh should abort early and must not override refresh info or MV data.
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	newTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESSFUL_REFRESH_READ_TSO from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, newTSRow, 1)
	newTS, err := strconv.ParseUint(fmt.Sprintf("%v", newTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.Equal(t, oldTS+1, newTS)

	tk.MustQuery(fmt.Sprintf(`select
  LAST_REFRESH_RESULT,
  LAST_REFRESH_TYPE,
  LAST_SUCCESSFUL_REFRESH_READ_TSO = %d,
  LAST_REFRESH_FAILED_REASON is not null
from mysql.tidb_mview_refresh where MVIEW_ID = %d`, oldTS+1, mviewID)).Check(testkit.Rows("failed complete 1 1"))
	reasonRow := tk.MustQuery(fmt.Sprintf("select LAST_REFRESH_FAILED_REASON from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, reasonRow, 1)
	require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "inconsistent LAST_SUCCESSFUL_REFRESH_READ_TSO")
}

func TestMaterializedViewRefreshCompleteConcurrentNowait(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tkLocker := testkit.NewTestKit(t, store)
	tkLocker.MustExec("use test")
	tkLocker.MustExec("begin pessimistic")
	tkLocker.MustQuery(fmt.Sprintf("select MVIEW_ID from mysql.tidb_mview_refresh where MVIEW_ID = %d for update", mviewID))

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
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID))
	err = tk.ExecToErr("refresh materialized view mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh")
}

func TestMaterializedViewRefreshWithSyncModeComplete(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv with sync mode complete")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
}

func TestMaterializedViewRefreshCompleteFailureWritesRefreshInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESSFUL_REFRESH_READ_TSO from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, oldTSRow, 1)
	oldTS, err := strconv.ParseUint(fmt.Sprintf("%v", oldTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, oldTS)

	// Create a unique index that will be violated after refresh.
	tk.MustExec("create unique index idx_unique_s on mv (s)")

	// Make the refreshed MV contain duplicate `s` values: sum(b) for a=2 becomes 15.
	tk.MustExec("insert into t values (2, 8)")

	err = tk.ExecToErr("refresh materialized view mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "Duplicate")

	// MV data should remain unchanged due to transactional refresh.
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	tk.MustQuery(fmt.Sprintf(`select
  LAST_REFRESH_RESULT,
  LAST_REFRESH_TYPE,
  LAST_SUCCESSFUL_REFRESH_READ_TSO = %d,
  LAST_REFRESH_FAILED_REASON is not null
from mysql.tidb_mview_refresh where MVIEW_ID = %d`, oldTS, mviewID)).Check(testkit.Rows("failed complete 1 1"))

	reasonRow := tk.MustQuery(fmt.Sprintf("select LAST_REFRESH_FAILED_REASON from mysql.tidb_mview_refresh where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, reasonRow, 1)
	require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "Duplicate")
}

func TestMaterializedViewRefreshCompleteForceConstraintCheckInPlacePessimisticOn(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")

	// Turn off in-place constraint check for pessimistic txn, which disables SAVEPOINT in pessimistic txn.
	// Refresh should still succeed because it forces the var to ON internally and restores it afterward.
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
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
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
