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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestMaterializedViewRefreshCompleteBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
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

	tk.MustExec("refresh materialized view mv complete delta apply")
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
	tk.MustQuery(fmt.Sprintf("select MVIEW_SCHEMA, MVIEW_NAME from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("test mv"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_SCHEMA = 'TEST' and MVIEW_NAME = 'MV' and MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD = 'complete delta apply manual', REFRESH_END_TIME is not null, REFRESH_ROWS is null, "+
			"REFRESH_DURATION_SEC = cast(timestampdiff(microsecond, REFRESH_START_TIME, REFRESH_END_TIME) as decimal(18,6)) / 1000000, REFRESH_DURATION_SEC >= 0, "+
			"REFRESH_READ_TSO > 0, REFRESH_COMMIT_TSO > REFRESH_READ_TSO, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows("success 1 1 1 1 1 1 1 1"))
}

func TestMaterializedViewRefreshStatementResult(t *testing.T) {
	for _, tc := range []struct {
		mode                               string
		insertRows, updateRows, deleteRows int64
	}{
		{"fast", 1, 1, 1},
		{"fast as of timestamp", 1, 1, 1},
		{"complete delta apply", 1, 1, 1},
		{"complete in place", 2, 0, 2},
		{"complete out of place", 2, 0, 0},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			tk := setupMaterializedViewRefreshStatementResultTest(t)
			if tc.mode == "fast as of timestamp" {
				tk.MustExec("set time_zone = '+00:00'")
			}
			makeMaterializedViewRefreshResultStale(t, tk)
			refreshSQL := "refresh materialized view mv_refresh_result " + tc.mode
			if tc.mode == "fast as of timestamp" {
				targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
				if sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond)); sleepUntilTarget > 0 {
					time.Sleep(sleepUntilTarget)
				}
				mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))
				refreshSQL += fmt.Sprintf(" '%s'", targetTime.Format("2006-01-02 15:04:05.000"))
			}
			var beforeRefreshMV float64
			if tc.mode == "fast" {
				beforeRefreshMV = readAffectedRowsMetricValue(t, "RefreshMV")
			}

			tk.MustExec(refreshSQL)
			requireRefreshMVStatementResult(t, tk, tc.insertRows, tc.updateRows, tc.deleteRows)
			if tc.mode == "fast" {
				require.Equal(t, 3.0, readAffectedRowsMetricValue(t, "RefreshMV")-beforeRefreshMV)
			}
			tk.MustQuery("select * from mv_refresh_result order by a").Check(testkit.Rows("1 11 1", "3 30 1"))
			// Out-of-place refresh has no commit TSO. Its historical snapshot
			// behavior is covered by the cutover snapshot test below.
			if tc.mode != "complete out of place" {
				requireRefreshInfoSnapshotMatchesLatestSuccessHist(t, tk, "test", "mv_refresh_result")
			}
		})
	}
}

func TestMaterializedViewRefreshFastAsOfTimestampNoOpStatementResult(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t_mv_refresh_result_noop (a int primary key, b int not null)")
	tk.MustExec("insert into t_mv_refresh_result_noop values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_result_noop (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_result_noop (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_result_noop group by a")

	mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_result_noop'").Rows()
	require.Len(t, mvIDRow, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
	require.NoError(t, err)

	targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
	if sleepUntilTarget > 0 {
		time.Sleep(sleepUntilTarget)
	}
	targetTSO := oracle.GoTimeToTS(targetTime)
	mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
		targetTSO,
		mvID,
	))

	tk.MustExec(fmt.Sprintf(
		"refresh materialized view mv_refresh_result_noop fast as of timestamp '%s'",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	requireRefreshMVStatementResult(t, tk, 0, 0, 0)
	tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mvID,
	)).Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshCompleteOutOfPlacePreservesPreviousCommitTSOSnapshot(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)

	beforeCutoverMViewIDRows := tk.MustQuery(
		"select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_result'",
	).Rows()
	require.Len(t, beforeCutoverMViewIDRows, 1)
	beforeCutoverMViewID, err := strconv.ParseInt(fmt.Sprintf("%v", beforeCutoverMViewIDRows[0][0]), 10, 64)
	require.NoError(t, err)

	makeMaterializedViewRefreshResultStale(t, tk)
	tk.MustExec("refresh materialized view mv_refresh_result fast")

	previousRefreshRows := tk.MustQuery("select REFRESH_COMMIT_TSO from mysql.tidb_mview_refresh_hist where MVIEW_SCHEMA = 'test' and MVIEW_NAME = 'mv_refresh_result' and REFRESH_STATUS = 'success' order by REFRESH_JOB_ID desc limit 1").Rows()
	require.Len(t, previousRefreshRows, 1)
	previousRefreshCommitTSO, err := strconv.ParseUint(fmt.Sprintf("%v", previousRefreshRows[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, previousRefreshCommitTSO)

	tk.MustExec("update t_mv_refresh_result set b = 12 where a = 1")
	tk.MustExec("insert into t_mv_refresh_result values (4, 40)")
	tk.MustExec("refresh materialized view mv_refresh_result complete out of place")

	afterCutoverMViewIDRows := tk.MustQuery(
		"select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_result'",
	).Rows()
	require.Len(t, afterCutoverMViewIDRows, 1)
	afterCutoverMViewID, err := strconv.ParseInt(fmt.Sprintf("%v", afterCutoverMViewIDRows[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotEqual(t, beforeCutoverMViewID, afterCutoverMViewID)

	requireRefreshInfoSnapshotMatchesHistAtCommitTSO(
		t,
		tk,
		"test",
		"mv_refresh_result",
		previousRefreshCommitTSO,
	)
}

func TestProfileMaterializedViewRefreshStepRuntime(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_explain_analyze (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_explain_analyze values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_explain_analyze (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_explain_analyze (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_explain_analyze group by a")

	tk.MustExec("insert into t_mv_explain_analyze values (2, 3), (3, 4)")
	fastRows := tk.MustQuery("refresh materialized view mv_mv_explain_analyze fast with profile").Rows()
	requireRowsContainPrefix(t, fastRows, "[S01 TXN_BEGIN]")
	requireRowsContainPrefix(t, fastRows, "[S04 DATA_CHANGE_FAST_MERGE]")
	requireRowsContainPrefix(t, fastRows, "  MViewDeltaMerge")
	requireRowsContainPrefix(t, fastRows, "[S07 FINALIZE_HIST]")

	tk.MustExec("insert into t_mv_explain_analyze values (4, 8)")
	inPlaceRows := tk.MustQuery("refresh materialized view mv_mv_explain_analyze complete in place with profile").Rows()
	requireRowsContainPrefix(t, inPlaceRows, "[S04 DATA_CHANGE_COMPLETE_DELETE]")
	requireRowsContainPrefix(t, inPlaceRows, "[S05 DATA_CHANGE_COMPLETE_INSERT]")
	requireRowsContainPrefix(t, inPlaceRows, "[S08 FINALIZE_HIST]")

	tk.MustExec("update t_mv_explain_analyze set b = 11 where a = 1 and b = 10")
	deltaApplyDryRunRows := tk.MustQuery("refresh materialized view mv_mv_explain_analyze complete delta apply dry run").Rows()
	requireRowsContainPrefix(t, deltaApplyDryRunRows, "[S04 DATA_CHANGE_COMPLETE_DELTA_APPLY]")
	requireRowsContainPrefix(t, deltaApplyDryRunRows, "  MViewCompleteDeltaApply")

	deltaApplyProfileRows := tk.MustQuery("refresh materialized view mv_mv_explain_analyze complete delta apply with profile").Rows()
	requireRowsContainPrefix(t, deltaApplyProfileRows, "[S04 DATA_CHANGE_COMPLETE_DELTA_APPLY]")
	requireRowsContainPrefix(t, deltaApplyProfileRows, "  MViewCompleteDeltaApply")
	require.Contains(t, joinRowsAsText(deltaApplyProfileRows), "mview_complete_delta_apply:{writer:{")
	require.Contains(t, joinRowsAsText(deltaApplyProfileRows), "rows:{insert:")
	requireRowsContainPrefix(t, deltaApplyProfileRows, "[S07 FINALIZE_HIST]")
}

func TestMaterializedViewRefreshFastDryRun(t *testing.T) {
	t.Run("current_db", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_mv_fast_dry_run (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_fast_dry_run values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t_mv_fast_dry_run (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_fast_dry_run (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_fast_dry_run group by a")

		rows := tk.MustQuery("refresh materialized view mv_fast_dry_run fast dry run").Rows()
		requireRowsContainPrefix(t, rows, "[S04 DATA_CHANGE_FAST_MERGE]")
		requireRowsContainPrefix(t, rows, "  MViewDeltaMerge")
	})

	t.Run("hazardous_purge", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_mv_fast_dry_run_purge_guard (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_fast_dry_run_purge_guard values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_mv_fast_dry_run_purge_guard (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_fast_dry_run_purge_guard (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_fast_dry_run_purge_guard group by a")
		tk.MustExec("refresh materialized view mv_fast_dry_run_purge_guard fast")

		mvIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_fast_dry_run_purge_guard'").Rows()
		require.Len(t, mvIDRows, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRows[0][0]), 10, 64)
		require.NoError(t, err)

		mlogIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '$mlog$t_mv_fast_dry_run_purge_guard'").Rows()
		require.Len(t, mlogIDRows, 1)
		mlogID, err := strconv.ParseInt(fmt.Sprintf("%v", mlogIDRows[0][0]), 10, 64)
		require.NoError(t, err)

		lastSuccessRows := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvID)).Rows()
		require.Len(t, lastSuccessRows, 1)
		lastSuccessReadTSO, err := strconv.ParseUint(fmt.Sprintf("%v", lastSuccessRows[0][0]), 10, 64)
		require.NoError(t, err)

		tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = %d where MLOG_ID = %d", lastSuccessReadTSO+1, mlogID))

		err = tk.QueryToErr("refresh materialized view mv_fast_dry_run_purge_guard fast dry run")
		require.Error(t, err)
		require.ErrorContains(t, err, "materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO")
	})

	t.Run("noop_skips_purge", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")
		tk.MustExec("create table t_mv_fast_dry_run_noop_purge_guard (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_fast_dry_run_noop_purge_guard values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_mv_fast_dry_run_noop_purge_guard (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_fast_dry_run_noop_purge_guard (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_fast_dry_run_noop_purge_guard group by a")

		mvIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_fast_dry_run_noop_purge_guard'").Rows()
		require.Len(t, mvIDRows, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRows[0][0]), 10, 64)
		require.NoError(t, err)

		mlogIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '$mlog$t_mv_fast_dry_run_noop_purge_guard'").Rows()
		require.Len(t, mlogIDRows, 1)
		mlogID, err := strconv.ParseInt(fmt.Sprintf("%v", mlogIDRows[0][0]), 10, 64)
		require.NoError(t, err)

		targetTime := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
		targetTSO := oracle.GoTimeToTS(targetTime)
		mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))
		tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d", targetTSO, mvID))
		tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = %d where MLOG_ID = %d", targetTSO+1, mlogID))

		rows := tk.MustQuery(fmt.Sprintf(
			"refresh materialized view mv_fast_dry_run_noop_purge_guard fast as of timestamp '%s' dry run",
			targetTime.Format("2006-01-02 15:04:05.000"),
		)).Rows()
		requireRowsContainPrefix(t, rows, "[S04 DATA_CHANGE_FAST_MERGE]")
		requireRowsNotContainSubstring(t, rows, "MViewDeltaMerge")

		tk.MustExec(fmt.Sprintf(
			"refresh materialized view mv_fast_dry_run_noop_purge_guard fast as of timestamp '%s'",
			targetTime.Format("2006-01-02 15:04:05.000"),
		))
	})

	t.Run("as_of_window", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")
		tk.MustExec("create table t_mv_refresh_asof_dry_run (a int not null, b int not null)")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_dry_run (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_dry_run (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_dry_run group by a")

		mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_dry_run'").Rows()
		require.Len(t, mvIDRow, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
		require.NoError(t, err)

		lastSuccessTSORow := tk.MustQuery(fmt.Sprintf(
			"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			mvID,
		)).Rows()
		require.Len(t, lastSuccessTSORow, 1)
		lastSuccessTSO, err := strconv.ParseUint(fmt.Sprintf("%v", lastSuccessTSORow[0][0]), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, lastSuccessTSO)

		targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
		sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
		if sleepUntilTarget > 0 {
			time.Sleep(sleepUntilTarget)
		}
		targetTSO := oracle.GoTimeToTS(targetTime)
		mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))

		rows := tk.MustQuery(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_dry_run fast as of timestamp '%s' dry run",
			targetTime.Format("2006-01-02 15:04:05.000"),
		)).Rows()
		requireRowsContainSubstring(t, rows, "_tidb_commit_ts")
		requireRowsContainSubstring(t, rows, strconv.FormatUint(lastSuccessTSO, 10))
		requireRowsContainSubstring(t, rows, strconv.FormatUint(targetTSO, 10))
	})

	t.Run("without_as_of", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")
		tk.MustExec("create table t_mv_refresh_dry_run_no_asof (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_refresh_dry_run_no_asof values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_mv_refresh_dry_run_no_asof (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_dry_run_no_asof (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_dry_run_no_asof group by a")

		mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_dry_run_no_asof'").Rows()
		require.Len(t, mvIDRow, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
		require.NoError(t, err)

		lastSuccessTSORow := tk.MustQuery(fmt.Sprintf(
			"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			mvID,
		)).Rows()
		require.Len(t, lastSuccessTSORow, 1)
		lastSuccessTSO, err := strconv.ParseUint(fmt.Sprintf("%v", lastSuccessTSORow[0][0]), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, lastSuccessTSO)

		rows := tk.MustQuery("refresh materialized view mv_refresh_dry_run_no_asof fast dry run").Rows()
		requireRowsContainSubstring(t, rows, "_tidb_commit_ts")
		requireRowsContainSubstring(t, rows, strconv.FormatUint(lastSuccessTSO, 10))
	})

	t.Run("older_than_last_success", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")
		tk.MustExec("create table t_mv_refresh_asof_dry_run_old (a int not null, b int not null)")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_dry_run_old (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_dry_run_old (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_dry_run_old group by a")

		mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_dry_run_old'").Rows()
		require.Len(t, mvIDRow, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
		require.NoError(t, err)

		lastSuccessTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
		lastSuccessTSO := oracle.GoTimeToTS(lastSuccessTime)
		targetTime := lastSuccessTime.Add(-time.Second)
		targetTSO := oracle.GoTimeToTS(targetTime)
		mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))
		tk.MustExec(fmt.Sprintf(
			"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
			lastSuccessTSO,
			mvID,
		))

		err = tk.QueryToErr(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_dry_run_old fast as of timestamp '%s' dry run",
			targetTime.Format("2006-01-02 15:04:05.000"),
		))
		require.ErrorContains(t, err, fmt.Sprintf(
			"refresh materialized view fast as of timestamp: target tso %d is older than LAST_SUCCESS_READ_TSO %d",
			targetTSO,
			lastSuccessTSO,
		))
	})
}

func TestMaterializedViewRefreshOutOfPlaceObserveLoadShadowPlanUsesBuildSQL(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_oop_observe (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_oop_observe values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_oop_observe (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_oop_observe (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_oop_observe group by a")
	tk.MustExec("insert into t_mv_oop_observe values (2, 3), (3, 4)")

	dryRunRows := tk.MustQuery("refresh materialized view mv_mv_oop_observe complete out of place dry run").Rows()
	requireRowsContainPrefix(t, dryRunRows, "[S03 OUT_OF_PLACE_LOAD_SHADOW]")
	requireRowsContainAnyPrefix(t, dryRunRows, "  Insert", "  ImportInto")

	tk.MustExec("insert into t_mv_oop_observe values (4, 8)")
	profileRows := tk.MustQuery("refresh materialized view mv_mv_oop_observe complete out of place with profile").Rows()
	requireRowsContainPrefix(t, profileRows, "[S03 OUT_OF_PLACE_LOAD_SHADOW]")
	requireRowsContainAnyPrefix(t, profileRows, "  Insert", "  ImportInto")
	require.NotContains(t, joinRowsAsText(profileRows), "@@tidb_last_query_info")
}

func TestMaterializedViewRefreshSessionVars(t *testing.T) {
	t.Run("maintain_mem_quota", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_mv_quota_refresh (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_quota_refresh values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_mv_quota_refresh (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_mv_quota_refresh (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_quota_refresh group by a")
		tk.MustExec("set @@session.tidb_mem_quota_query = 1073741824")
		tk.MustExec("set @@global.tidb_mview_maintain_mem_quota = 536870912")
		tk.MustExec("set @@session.tidb_mview_maintain_mem_quota = 268435456")
		defer tk.MustExec(fmt.Sprintf("set @@global.tidb_mview_maintain_mem_quota = %d", 2*1024*1024*1024))

		applied := false
		lastAppliedMemQuotaQuery := int64(0)
		lastAppliedMaintainQuota := int64(0)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnRefreshSession", func(memQuotaQuery int64, maintainMemQuota int64) {
			applied = true
			lastAppliedMemQuotaQuery = memQuotaQuery
			lastAppliedMaintainQuota = maintainMemQuota
		})

		tk.MustExec("refresh materialized view mv_mv_quota_refresh complete delta apply")
		require.True(t, applied)
		require.Equal(t, int64(268435456), lastAppliedMaintainQuota)
		require.Equal(t, lastAppliedMaintainQuota, lastAppliedMemQuotaQuery)

		applied = false
		lastAppliedMemQuotaQuery = 0
		lastAppliedMaintainQuota = 0
		mustExecRefreshInternal(t, tk, "refresh materialized view mv_mv_quota_refresh complete delta apply")
		require.True(t, applied)
		require.Equal(t, int64(268435456), lastAppliedMaintainQuota)
		require.Equal(t, lastAppliedMaintainQuota, lastAppliedMemQuotaQuery)

		buildApplied := false
		lastBuildAppliedMemQuotaQuery := int64(0)
		lastBuildAppliedMaintainQuota := int64(0)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnRefreshOutOfPlaceBuildSession", func(memQuotaQuery int64, maintainMemQuota int64) {
			buildApplied = true
			lastBuildAppliedMemQuotaQuery = memQuotaQuery
			lastBuildAppliedMaintainQuota = maintainMemQuota
		})

		tk.MustExec("insert into t_mv_quota_refresh values (3, 30)")
		tk.MustExec("refresh materialized view mv_mv_quota_refresh complete out of place")
		require.True(t, buildApplied)
		require.Equal(t, int64(268435456), lastBuildAppliedMaintainQuota)
		require.Equal(t, lastBuildAppliedMaintainQuota, lastBuildAppliedMemQuotaQuery)

		buildApplied = false
		lastBuildAppliedMemQuotaQuery = 0
		lastBuildAppliedMaintainQuota = 0
		tk.MustExec("insert into t_mv_quota_refresh values (4, 40)")
		mustExecRefreshInternal(t, tk, "refresh materialized view mv_mv_quota_refresh complete out of place")
		require.True(t, buildApplied)
		require.Equal(t, int64(268435456), lastBuildAppliedMaintainQuota)
		require.Equal(t, lastBuildAppliedMaintainQuota, lastBuildAppliedMemQuotaQuery)
	})

	t.Run("isolation_read_engines", func(t *testing.T) {
		for _, useDefault := range []bool{false, true} {
			t.Run(fmt.Sprintf("default=%t", useDefault), func(t *testing.T) {
				store := testkit.CreateMockStore(t)
				tk := newMViewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("create table t_mv_refresh_isolation (a int not null, b int not null)")
				tk.MustExec("insert into t_mv_refresh_isolation values (1, 10), (2, 20)")
				tk.MustExec("create materialized view log on t_mv_refresh_isolation (a, b) purge next date_add(now(), interval 1 hour)")
				tk.MustExec("create materialized view mv_mv_refresh_isolation (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_isolation group by a")

				maintainEngines, currentEngines := "tikv", "tikv,tiflash,tidb"
				if useDefault {
					maintainEngines = variable.GetSysVar(vardef.TiDBMViewMaintainIsolationReadEngines).Value
					currentEngines = differentIsolationReadEnginesForTest(maintainEngines)
				} else {
					tk.MustExec(fmt.Sprintf("set @@session.%s = '%s'", vardef.TiDBMViewMaintainIsolationReadEngines, maintainEngines))
				}
				tk.MustExec(fmt.Sprintf("set @@session.%s = '%s'", vardef.TiDBIsolationReadEngines, currentEngines))

				for _, mode := range []string{"complete in place", "complete out of place"} {
					failpointName := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewIsolationReadEnginesApplied"
					if mode == "complete out of place" {
						tk.MustExec("insert into t_mv_refresh_isolation values (3, 30)")
						failpointName = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildIsolationReadEnginesApplied"
					}
					requireRefreshIsolationReadEnginesApplied(t, failpointName, func() {
						tk.MustExec("refresh materialized view mv_mv_refresh_isolation " + mode)
					}, maintainEngines)
					tk.MustQuery(fmt.Sprintf("select @@session.%s", vardef.TiDBIsolationReadEngines)).Check(testkit.Rows(currentEngines))
				}
			})
		}
	})

	t.Run("tiflash_session_vars", func(t *testing.T) {
		for _, tc := range []struct {
			name, mode string
			internal   bool
		}{
			{"manual", "complete delta apply", false},
			{"internal", "complete delta apply", true},
			{"out_of_place_manual", "complete out of place", false},
			{"out_of_place_internal", "complete out of place", true},
			{"dry_run", "complete delta apply dry run", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				store := testkit.CreateMockStore(t)
				tk := newMViewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("create table t_mv_refresh_tiflash_vars (a int not null, b int not null)")
				tk.MustExec("insert into t_mv_refresh_tiflash_vars values (1, 10), (2, 20)")
				tk.MustExec("create materialized view log on t_mv_refresh_tiflash_vars (a, b) purge next date_add(now(), interval 1 hour)")
				tk.MustExec("create materialized view mv_mv_refresh_tiflash_vars (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_refresh_tiflash_vars group by a")
				prepareRefreshTiFlashSessionVarsForTest(t, tk)
				if tc.internal && tc.mode == "complete out of place" {
					tk.MustExec("insert into t_mv_refresh_tiflash_vars values (3, 30)")
				}

				refresh := func() {
					refreshSQL := "refresh materialized view mv_mv_refresh_tiflash_vars " + tc.mode
					switch {
					case tc.internal:
						mustExecRefreshInternal(t, tk, refreshSQL)
					case tc.mode == "complete delta apply dry run":
						tk.MustQuery(refreshSQL).Rows()
					default:
						tk.MustExec(refreshSQL)
					}
				}
				if tc.mode == "complete out of place" {
					varsFailpoint := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildTiFlashSessionVarsApplied"
					spillFailpoint := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildTiFlashSpillSessionVarsApplied"
					importFailpoint := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildImportSessionVarsApplied"
					requireImportSessionVarsAppliedWithFailpoint(t, importFailpoint, func() {
						requireTiFlashSessionVarsAppliedWithFailpoint(t, varsFailpoint, spillFailpoint, refresh,
							int64(8), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
					}, 12, "64gib")
				} else {
					requireRefreshTiFlashSessionVarsApplied(t, refresh,
						int64(8), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
				}
				requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
			})
		}
	})

	t.Run("apply_failure", func(t *testing.T) {
		for _, tc := range []struct {
			name                string
			mode                string
			internal, singleVar bool
		}{
			{"manual_all", "complete delta apply", false, false},
			{"internal_all", "complete delta apply", true, false},
			{"manual_single", "complete delta apply", false, true},
			{"internal_single", "complete delta apply", true, true},
			{"internal_out_of_place_all", "complete out of place", true, false},
			{"internal_out_of_place_single", "complete out of place", true, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				store := testkit.CreateMockStore(t)
				tk := newMViewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("create table t_mv_refresh_apply_vars (a int not null, b int not null)")
				tk.MustExec("insert into t_mv_refresh_apply_vars values (1, 10), (2, 20)")
				tk.MustExec("create materialized view log on t_mv_refresh_apply_vars (a, b) purge next date_add(now(), interval 1 hour)")
				tk.MustExec("create materialized view mv_mv_refresh_apply_vars (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_apply_vars group by a")
				failpointName := "github.com/pingcap/tidb/pkg/executor/mockRefreshExecutionSessionVarsApplyError"
				failpointExpr := "return(true)"
				if tc.singleVar {
					prepareRefreshTiFlashSessionVarsForTest(t, tk)
					failpointName = "github.com/pingcap/tidb/pkg/ddl/mockMViewExecutionSessionVarApplyError"
					failpointExpr = fmt.Sprintf("return(%q)", vardef.TiDBMaxTiFlashThreads)
				}
				require.NoError(t, failpoint.Enable(failpointName, failpointExpr))
				t.Cleanup(func() {
					require.NoError(t, failpoint.Disable(failpointName))
				})

				refreshSQL := "refresh materialized view mv_mv_refresh_apply_vars " + tc.mode
				if !tc.internal {
					err := tk.ExecToErr(refreshSQL)
					if tc.singleVar {
						require.ErrorContains(t, err, vardef.TiDBMaxTiFlashThreads)
					} else {
						require.ErrorContains(t, err, "mock refresh execution session vars apply error")
					}
					return
				}

				tk.MustExec("insert into t_mv_refresh_apply_vars values (3, 30)")
				refresh := func() {
					mustExecRefreshInternal(t, tk, refreshSQL)
				}
				if tc.singleVar {
					varsFailpoint := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewTiFlashSessionVarsApplied"
					spillFailpoint := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewTiFlashSpillSessionVarsApplied"
					if tc.mode == "complete out of place" {
						varsFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildTiFlashSessionVarsApplied"
						spillFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildTiFlashSpillSessionVarsApplied"
					}
					requireTiFlashSessionVarsAppliedWithFailpoint(
						t, varsFailpoint, spillFailpoint, refresh,
						int64(vardef.DefTiFlashMaxThreads), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096),
					)
				} else {
					refresh()
				}
				tk.MustQuery("select * from mv_mv_refresh_apply_vars order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
				if tc.singleVar {
					requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
				}
			})
		}
	})

	t.Run("initial_build", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_mv_create_build_vars (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_create_build_vars values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_mv_create_build_vars (a, b)")
		prepareRefreshTiFlashSessionVarsForTest(t, tk)
		tk.MustExec("set @@session.tidb_mem_quota_query = 1073741824")
		tk.MustExec("set @@global.tidb_mview_maintain_mem_quota = 536870912")
		tk.MustExec("set @@session.tidb_mview_maintain_mem_quota = 268435456")
		t.Cleanup(func() {
			tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiDBMViewMaintainMemQuota, 2*1024*1024*1024))
		})

		memQuotaApplied := false
		gotMemQuotaQuery := int64(0)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/createMaterializedViewBuildMaintainMemQuotaApplied", func(memQuotaQuery int64) {
			memQuotaApplied = true
			gotMemQuotaQuery = memQuotaQuery
		})
		importFailpointName := "github.com/pingcap/tidb/pkg/ddl/createMaterializedViewBuildImportSessionVarsApplied"
		tiflashFailpointName := "github.com/pingcap/tidb/pkg/ddl/createMaterializedViewBuildTiFlashSessionVarsApplied"
		spillFailpointName := "github.com/pingcap/tidb/pkg/ddl/createMaterializedViewBuildTiFlashSpillSessionVarsApplied"

		requireImportSessionVarsAppliedWithFailpoint(t, importFailpointName, func() {
			requireTiFlashSessionVarsAppliedWithFailpoint(t, tiflashFailpointName, spillFailpointName, func() {
				tk.MustExec("create materialized view mv_mv_create_build_vars (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_create_build_vars group by a")
			}, int64(8), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
		}, 12, "64gib")
		require.True(t, memQuotaApplied)
		require.Equal(t, int64(268435456), gotMemQuotaQuery)
		requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
		require.Equal(t, int64(268435456), tk.Session().GetSessionVars().MViewMaintainMemQuota)
		require.Equal(t, int64(1073741824), tk.Session().GetSessionVars().MemQuotaQuery)
		tk.MustQuery("select * from mv_mv_create_build_vars order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
	})
}

func TestMaterializedViewRefreshScheduling(t *testing.T) {
	t.Run("next_refresh", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_internal_next (a int not null, b int not null)")
		tk.MustExec("insert into t_internal_next values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_internal_next (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_internal_next (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t_internal_next group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_internal_next"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = null where MVIEW_ID = %d", mviewID))

		// User SQL refresh should not update NEXT_REFRESH_UNIX_SECONDS.
		tk.MustExec("refresh materialized view mv_internal_next complete delta apply")
		tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("complete delta apply manual"))

		// Internal SQL refresh should update NEXT_REFRESH_UNIX_SECONDS by evaluating RefreshNext.
		mustExecRefreshInternal(t, tk, "refresh materialized view mv_internal_next complete delta apply")
		tk.MustQuery(fmt.Sprintf(
			"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			mviewID,
		)).Check(testkit.Rows("1 1 1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("complete delta apply auto"))
	})

	t.Run("schedule_duration", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_refresh_schedule_duration (a int not null, b int not null)")
		tk.MustExec("insert into t_refresh_schedule_duration values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_refresh_schedule_duration (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_schedule_duration (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_schedule_duration group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_refresh_schedule_duration"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_REFRESH_END_UNIX_SECONDS > 0 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))

		tk.MustExec("insert into t_refresh_schedule_duration values (3, 30)")
		tk.MustExec("refresh materialized view mv_refresh_schedule_duration fast")
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD, REFRESH_SCHEDULE_DURATION_SEC is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("fast manual 1"))

		tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = UNIX_TIMESTAMP() - 120 where MVIEW_ID = %d", mviewID))
		tk.MustExec("insert into t_refresh_schedule_duration values (4, 40)")
		metricCountBefore := readMVServiceRefreshScheduleDurationCount(t)
		mustExecRefreshInternal(t, tk, "refresh materialized view mv_refresh_schedule_duration fast")
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD, REFRESH_SCHEDULE_DURATION_SEC >= 100, REFRESH_SCHEDULE_DURATION_SEC <= 150 from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("fast auto 1 1"))
		require.Greater(t, readMVServiceRefreshScheduleDurationCount(t), metricCountBefore)

		tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = null where MVIEW_ID = %d", mviewID))
		tk.MustExec("insert into t_refresh_schedule_duration values (5, 50)")
		mustExecRefreshInternal(t, tk, "refresh materialized view mv_refresh_schedule_duration fast")
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD, REFRESH_SCHEDULE_DURATION_SEC is not null, REFRESH_SCHEDULE_DURATION_SEC >= 0 from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("fast auto 1 1"))
	})

	t.Run("start_without_next", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_internal_start_only (a int not null, b int not null)")
		tk.MustExec("insert into t_internal_start_only values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_internal_start_only (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_internal_start_only (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t_internal_start_only group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_internal_start_only"))
		require.NoError(t, err)
		require.NotNil(t, mvTable.Meta().MaterializedView)
		// Simulate scheduler metadata state: START WITH is set but NEXT is empty.
		mvTable.Meta().MaterializedView.RefreshStartWith = "DATE_ADD(NOW(), INTERVAL 2 HOUR)"
		mvTable.Meta().MaterializedView.RefreshNext = ""
		mviewID := mvTable.Meta().ID

		tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = UNIX_TIMESTAMP() + 3 * 60 * 60 where MVIEW_ID = %d", mviewID))

		// User SQL refresh should keep NEXT_REFRESH_UNIX_SECONDS unchanged.
		tk.MustExec("refresh materialized view mv_internal_start_only complete delta apply")
		tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is not null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("complete delta apply manual"))

		// Internal SQL refresh should explicitly set NEXT_REFRESH_UNIX_SECONDS = NULL when START WITH exists and NEXT is empty.
		mustExecRefreshInternal(t, tk, "refresh materialized view mv_internal_start_only complete delta apply")
		tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("complete delta apply auto"))
	})

	t.Run("no_schedule", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_internal_no_schedule (a int not null, b int not null)")
		tk.MustExec("insert into t_internal_no_schedule values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_internal_no_schedule (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_internal_no_schedule (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t_internal_no_schedule group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_internal_no_schedule"))
		require.NoError(t, err)
		require.NotNil(t, mvTable.Meta().MaterializedView)
		// Simulate scheduler metadata state: schedule is fully removed.
		mvTable.Meta().MaterializedView.RefreshStartWith = ""
		mvTable.Meta().MaterializedView.RefreshNext = ""
		mviewID := mvTable.Meta().ID

		tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = UNIX_TIMESTAMP() + 3 * 60 * 60 where MVIEW_ID = %d", mviewID))

		mustExecRefreshInternal(t, tk, "refresh materialized view mv_internal_no_schedule complete delta apply")
		tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("complete delta apply auto"))
	})

	t.Run("out_of_place", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_internal_oop_next (a int not null, b int not null)")
		tk.MustExec("insert into t_internal_oop_next values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_internal_oop_next (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_internal_oop_next (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t_internal_oop_next group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_internal_oop_next"))
		require.NoError(t, err)
		oldMViewID := mvTable.Meta().ID

		tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = null where MVIEW_ID = %d", oldMViewID))
		tk.MustExec("insert into t_internal_oop_next values (3, 30)")

		mustExecRefreshInternal(t, tk, "refresh materialized view mv_internal_oop_next complete out of place")

		is = dom.InfoSchema()
		mvTable, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_internal_oop_next"))
		require.NoError(t, err)
		newMViewID := mvTable.Meta().ID
		require.NotEqual(t, oldMViewID, newMViewID)

		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).
			Check(testkit.Rows("0"))
		tk.MustQuery(fmt.Sprintf(
			"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			newMViewID,
		)).Check(testkit.Rows("1 1 1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			oldMViewID,
		)).Check(testkit.Rows("complete out of place auto"))
	})
}

func TestMaterializedViewRefreshFastMethodTracksManualAndAutomatic(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv fast")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery("select REFRESH_METHOD, REFRESH_ROWS > 0 from mysql.tidb_mview_refresh_hist order by REFRESH_JOB_ID desc limit 1").
		Check(testkit.Rows("fast manual 1"))

	tk.MustExec("insert into t values (4, 8)")
	mustExecRefreshInternal(t, tk, "refresh materialized view mv fast")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1", "4 8 1"))
	tk.MustQuery("select REFRESH_METHOD, REFRESH_ROWS > 0 from mysql.tidb_mview_refresh_hist order by REFRESH_JOB_ID desc limit 1").
		Check(testkit.Rows("fast auto 1"))
}

func TestMaterializedViewRefreshFastUpdatesStatsModifyCount(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_fast_stats (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_fast_stats values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_fast_stats (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_fast_stats (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_fast_stats group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_fast_stats"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID

	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table mv_fast_stats")
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", mvID)).
		Check(testkit.Rows("0 2"))

	tk.MustExec("delete from t_mv_fast_stats where a = 1 and b = 10")
	tk.MustExec("delete from t_mv_fast_stats where a = 2 and b = 7")
	tk.MustExec("insert into t_mv_fast_stats values (3, 4)")
	tk.MustExec("refresh materialized view mv_fast_stats fast")
	tk.MustQuery("select a, s, cnt from mv_fast_stats order by a").Check(testkit.Rows("1 5 1", "3 4 1"))

	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", mvID)).
		Check(testkit.Rows("3 2"))
}

func TestMaterializedViewRefreshFastAsOfTimestamp(t *testing.T) {
	t.Run("reject_non_fast", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_mv_refresh_asof_nonfast (a int not null, b int not null)")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_nonfast (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_nonfast (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_nonfast group by a")

		err := tk.ExecToErr("refresh materialized view mv_refresh_asof_nonfast complete delta apply as of timestamp '2021-04-15 00:00:00'")
		require.ErrorContains(t, err, "You have an error in your SQL syntax")
	})

	t.Run("outer_semantics", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")
		tk.MustExec("create table t_mv_refresh_asof_fast (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_refresh_asof_fast values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_fast (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_fast (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_fast group by a")

		mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_fast'").Rows()
		require.Len(t, mvIDRow, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
		require.NoError(t, err)

		fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
		fromTSO := oracle.GoTimeToTS(fromTime)
		mustSetMockGCSafePoint(t, tk, fromTime.Add(-time.Hour))
		tk.MustExec(fmt.Sprintf(
			"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
			fromTSO,
			mvID,
		))

		// Make the MV stale so the no-op path proves we did not accidentally run a normal FAST refresh.
		tk.MustExec("insert into t_mv_refresh_asof_fast values (3, 30)")
		tk.MustQuery("select a, s, cnt from mv_refresh_asof_fast order by a").Check(testkit.Rows(
			"1 10 1",
			"2 20 1",
		))
		histCountBeforeNoOp := tk.MustQuery(fmt.Sprintf(
			"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
			mvID,
		)).Rows()[0][0]

		fromLiteral := fromTime.Format("2006-01-02 15:04:05.000")
		olderLiteral := fromTime.Add(-time.Millisecond).Format("2006-01-02 15:04:05.000")
		newerLiteral := fromTime.Add(time.Millisecond).Format("2006-01-02 15:04:05.000")
		newerTSO := oracle.GoTimeToTS(fromTime.Add(time.Millisecond))

		tk.MustExec(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_fast fast as of timestamp '%s'",
			fromLiteral,
		))
		tk.MustQuery("select a, s, cnt from mv_refresh_asof_fast order by a").Check(testkit.Rows(
			"1 10 1",
			"2 20 1",
		))
		tk.MustQuery(fmt.Sprintf(
			"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			mvID,
		)).Check(testkit.Rows(strconv.FormatUint(fromTSO, 10)))
		tk.MustQuery(fmt.Sprintf(
			"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
			mvID,
		)).Check(testkit.Rows(fmt.Sprintf("%v", histCountBeforeNoOp)))

		err = tk.ExecToErr(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_fast fast as of timestamp '%s'",
			olderLiteral,
		))
		require.ErrorContains(t, err, "target tso")
		require.ErrorContains(t, err, "older than LAST_SUCCESS_READ_TSO")
		olderTSO := oracle.GoTimeToTS(fromTime.Add(-time.Millisecond))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			olderTSO,
			mvID,
		)).Check(testkit.Rows("failed bounded fast manual 1 1"))

		tk.MustExec(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_fast fast as of timestamp '%s'",
			newerLiteral,
		))
		tk.MustQuery("select a, s, cnt from mv_refresh_asof_fast order by a").Check(testkit.Rows(
			"1 10 1",
			"2 20 1",
		))
		tk.MustQuery(fmt.Sprintf(
			"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			mvID,
		)).Check(testkit.Rows(strconv.FormatUint(newerTSO, 10)))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = %d from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			newerTSO,
			mvID,
		)).Check(testkit.Rows("success bounded fast manual 1"))
	})

	t.Run("watermark_windows", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")
		tk.MustExec("create table t_mv_refresh_asof_window (a int not null, b int not null)")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_window (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_window (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_window group by a")

		mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_window'").Rows()
		require.Len(t, mvIDRow, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
		require.NoError(t, err)

		initialTSRow := tk.MustQuery(fmt.Sprintf(
			"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			mvID,
		)).Rows()
		require.Len(t, initialTSRow, 1)
		initialTSO, err := strconv.ParseUint(fmt.Sprintf("%v", initialTSRow[0][0]), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, initialTSO)

		tk.MustExec("insert into t_mv_refresh_asof_window values (1, 10)")
		firstTargetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
		sleepUntilFirstTarget := time.Until(firstTargetTime.Add(20 * time.Millisecond))
		if sleepUntilFirstTarget > 0 {
			time.Sleep(sleepUntilFirstTarget)
		}
		tk.MustExec("insert into t_mv_refresh_asof_window values (2, 20)")

		secondTargetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
		sleepUntilSecondTarget := time.Until(secondTargetTime.Add(20 * time.Millisecond))
		if sleepUntilSecondTarget > 0 {
			time.Sleep(sleepUntilSecondTarget)
		}

		mustSetMockGCSafePoint(t, tk, firstTargetTime.Add(-time.Hour))

		firstTargetTSO := oracle.GoTimeToTS(firstTargetTime)
		tk.MustExec(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_window fast as of timestamp '%s'",
			firstTargetTime.Format("2006-01-02 15:04:05.000"),
		))
		tk.MustQuery("select a, s, cnt from mv_refresh_asof_window order by a").Check(testkit.Rows(
			"1 10 1",
		))
		tk.MustQuery(fmt.Sprintf(
			"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			mvID,
		)).Check(testkit.Rows(strconv.FormatUint(firstTargetTSO, 10)))

		secondTargetTSO := oracle.GoTimeToTS(secondTargetTime)
		tk.MustExec(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_window fast as of timestamp '%s'",
			secondTargetTime.Format("2006-01-02 15:04:05.000"),
		))
		tk.MustQuery("select a, s, cnt from mv_refresh_asof_window order by a").Check(testkit.Rows(
			"1 10 1",
			"2 20 1",
		))
		tk.MustQuery(fmt.Sprintf(
			"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
			mvID,
		)).Check(testkit.Rows(strconv.FormatUint(secondTargetTSO, 10)))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_READ_TSO from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'success' order by REFRESH_JOB_ID desc limit 2",
			mvID,
		)).Check(testkit.Rows(
			strconv.FormatUint(secondTargetTSO, 10),
			strconv.FormatUint(firstTargetTSO, 10),
		))
		require.Less(t, initialTSO, firstTargetTSO)
		require.Less(t, firstTargetTSO, secondTargetTSO)
	})

	t.Run("automatic_method", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")
		tk.MustExec("create table t_mv_refresh_asof_internal (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_refresh_asof_internal values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_internal (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_internal (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_internal group by a")

		mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_internal'").Rows()
		require.Len(t, mvIDRow, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
		require.NoError(t, err)

		fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
		fromTSO := oracle.GoTimeToTS(fromTime)
		mustSetMockGCSafePoint(t, tk, fromTime.Add(-time.Hour))
		tk.MustExec(fmt.Sprintf(
			"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
			fromTSO,
			mvID,
		))

		targetTime := fromTime.Add(time.Millisecond)
		targetTSO := oracle.GoTimeToTS(targetTime)
		mustExecRefreshInternal(t, tk, fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_internal fast as of timestamp '%s'",
			targetTime.Format("2006-01-02 15:04:05.000"),
		))

		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = %d from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			targetTSO,
			mvID,
		)).Check(testkit.Rows("success bounded fast auto 1"))
	})

	t.Run("early_failure_history", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")
		tk.MustExec("create table t_mv_refresh_asof_early_fail (a int not null, b int not null)")
		tk.MustExec("insert into t_mv_refresh_asof_early_fail values (1, 10), (2, 20)")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_early_fail (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_early_fail (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_early_fail group by a")

		mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_early_fail'").Rows()
		require.Len(t, mvIDRow, 1)
		mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
		require.NoError(t, err)

		targetTime := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
		targetTSO := oracle.GoTimeToTS(targetTime)
		mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))

		const failpointName = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist"
		require.NoError(t, failpoint.Enable(failpointName, `return("mock as-of early refresh failure")`))
		defer func() {
			require.NoError(t, failpoint.Disable(failpointName))
		}()

		err = tk.ExecToErr(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_early_fail fast as of timestamp '%s'",
			targetTime.Format("2006-01-02 15:04:05.000"),
		))
		require.Error(t, err)
		require.ErrorContains(t, err, "mock as-of early refresh failure")

		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD = 'bounded fast manual', REFRESH_START_TIME is not null, REFRESH_END_TIME is not null, "+
				"REFRESH_DURATION_SEC = cast(timestampdiff(microsecond, REFRESH_START_TIME, REFRESH_END_TIME) as decimal(18,6)) / 1000000, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is not null "+
				"from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			targetTSO,
			mvID,
		)).Check(testkit.Rows("failed 1 1 1 1 1 1"))
	})
}

func TestMaterializedViewFastRefreshRejectsHazardousPurgeHist(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_refresh_purge_guard (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_refresh_purge_guard (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_purge_guard (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_purge_guard group by a")
	tk.MustExec("insert into t_refresh_purge_guard values (1, 10), (2, 20)")
	tk.MustExec("refresh materialized view mv_refresh_purge_guard fast")
	tk.MustExec("insert into t_refresh_purge_guard values (3, 30)")

	mvIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_purge_guard'").Rows()
	require.Len(t, mvIDRows, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRows[0][0]), 10, 64)
	require.NoError(t, err)

	mlogIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '$mlog$t_refresh_purge_guard'").Rows()
	require.Len(t, mlogIDRows, 1)
	mlogID, err := strconv.ParseInt(fmt.Sprintf("%v", mlogIDRows[0][0]), 10, 64)
	require.NoError(t, err)

	lastSuccessRows := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvID)).Rows()
	require.Len(t, lastSuccessRows, 1)
	lastSuccessReadTSO, err := strconv.ParseUint(fmt.Sprintf("%v", lastSuccessRows[0][0]), 10, 64)
	require.NoError(t, err)

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = %d where MLOG_ID = %d", lastSuccessReadTSO, mlogID))
	tk.MustExec(fmt.Sprintf(
		`insert into mysql.tidb_mlog_purge_hist (
			PURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD,
			PURGE_START_TIME, PURGE_ROWS, PURGE_STATUS, PURGE_CUTOFF_TSO, LAST_HEARTBEAT_TIME
		) values (
			%[1]d, %[2]d, 'test', 't_refresh_purge_guard', 'manual',
			now(6), 0, 'running', %[3]d, now(6)
		)`,
		lastSuccessReadTSO+1000,
		mlogID,
		lastSuccessReadTSO+1,
	))

	err = tk.ExecToErr("refresh materialized view mv_refresh_purge_guard fast")
	require.Error(t, err)
	require.ErrorContains(t, err, "materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO")

	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvID)).
		Check(testkit.Rows(fmt.Sprintf("%d", lastSuccessReadTSO)))

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

	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mlog_purge_hist where MLOG_ID = %d", mlogID))
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = %d where MLOG_ID = %d", lastSuccessReadTSO, mlogID))

	refreshDone := make(chan error, 1)
	go func() {
		tkRefresh := newMViewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_refresh_purge_guard fast")
	}()

	select {
	case <-hitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to reach failpoint")
	}

	tkConcurrent := newMViewTestKit(t, store)
	tkConcurrent.MustExec("use test")
	tkConcurrent.MustExec(fmt.Sprintf(
		`insert into mysql.tidb_mlog_purge_hist (
			PURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD,
			PURGE_START_TIME, PURGE_ROWS, PURGE_STATUS, PURGE_CUTOFF_TSO, LAST_HEARTBEAT_TIME
		) values (
			%[1]d, %[2]d, 'test', 't_refresh_purge_guard', 'manual',
			now(6), 0, 'running', %[3]d, now(6)
		)`,
		lastSuccessReadTSO+2000,
		mlogID,
		lastSuccessReadTSO+1,
	))

	close(pauseCh)

	select {
	case err = <-refreshDone:
		require.Error(t, err)
		require.ErrorContains(t, err, "materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to finish")
	}
}

func TestMaterializedViewRefreshFailedAlertByAttribute(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t_mv_refresh_failed_alert (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_failed_alert values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_failed_alert (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_failed_yes (a, s, cnt) refresh fast attributes='mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t_mv_refresh_failed_alert group by a")
	tk.MustExec("create materialized view mv_refresh_failed_no (a, s, cnt) refresh fast attributes='mview_alert_refresh_failed=no' as select a, sum(b), count(1) from t_mv_refresh_failed_alert group by a")

	mvYesIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_failed_yes'").Rows()
	require.Len(t, mvYesIDRows, 1)
	mvYesID, err := strconv.ParseInt(fmt.Sprintf("%v", mvYesIDRows[0][0]), 10, 64)
	require.NoError(t, err)
	mvNoIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_failed_no'").Rows()
	require.Len(t, mvNoIDRows, 1)
	mvNoID, err := strconv.ParseInt(fmt.Sprintf("%v", mvNoIDRows[0][0]), 10, 64)
	require.NoError(t, err)

	const failpointName = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist"
	require.NoError(t, failpoint.Enable(failpointName, `return("mock refresh failed alert")`))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(failpointName))
		}
	}()

	err = tk.ExecToErr("refresh materialized view mv_refresh_failed_yes fast")
	require.Error(t, err)
	require.ErrorContains(t, err, "mock refresh failed alert")
	err = tk.ExecToErr("refresh materialized view mv_refresh_failed_no fast")
	require.Error(t, err)
	require.ErrorContains(t, err, "mock refresh failed alert")

	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_FAILED, ALERT_LEVEL is null from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d",
		mvYesID,
	)).Check(testkit.Rows("YES 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", mvNoID)).
		Check(testkit.Rows("0"))

	require.NoError(t, failpoint.Disable(failpointName))
	enabled = false

	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_alert set ALERT_LEVEL = 'warning' where MVIEW_ID = %d",
		mvYesID,
	))
	tk.MustExec("refresh materialized view mv_refresh_failed_yes fast")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", mvYesID)).
		Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshFallbackReportsAlertBeforeInsertHistFailure(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_failed_alert_fallback (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_failed_alert_fallback values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_failed_alert_fallback (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_failed_alert_fallback (a, s, cnt) refresh fast attributes='mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t_mv_refresh_failed_alert_fallback group by a")

	mvIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_failed_alert_fallback'").Rows()
	require.Len(t, mvIDRows, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRows[0][0]), 10, 64)
	require.NoError(t, err)

	const earlyRefreshFailpoint = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist"
	require.NoError(t, failpoint.Enable(earlyRefreshFailpoint, `return("mock fallback refresh failed alert")`))
	defer func() {
		require.NoError(t, failpoint.Disable(earlyRefreshFailpoint))
	}()
	const insertHistFailpoint = "github.com/pingcap/tidb/pkg/executor/mockInsertRefreshHistFailedError"
	require.NoError(t, failpoint.Enable(insertHistFailpoint, "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(insertHistFailpoint))
	}()

	err = tk.ExecToErr("refresh materialized view mv_refresh_failed_alert_fallback fast")
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to insert failed refresh history after error")
	require.ErrorContains(t, err, "mock fallback refresh failed alert")

	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_FAILED, ALERT_LEVEL is null from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d",
		mvID,
	)).Check(testkit.Rows("YES 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mvID)).
		Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshFastAsOfTimestampGCSafePoint(t *testing.T) {
	for _, minMax := range []bool{false, true} {
		for _, dryRun := range []bool{false, true} {
			t.Run(fmt.Sprintf("minmax=%t/dry_run=%t", minMax, dryRun), func(t *testing.T) {
				store, _ := testkit.CreateMockStoreAndDomain(t)
				tk := newMViewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("set time_zone = '+00:00'")
				if minMax {
					tk.MustExec("create table t_mv_refresh_asof_gc (a int not null, b int not null, key idx_a(a))")
				} else {
					tk.MustExec("create table t_mv_refresh_asof_gc (a int not null, b int not null)")
				}
				tk.MustExec("create materialized view log on t_mv_refresh_asof_gc (a, b) purge next date_add(now(), interval 1 hour)")
				if minMax {
					tk.MustExec("create materialized view mv_refresh_asof_gc (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_gc group by a")
				} else {
					tk.MustExec("create materialized view mv_refresh_asof_gc (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_gc group by a")
				}

				mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_gc'").Rows()
				require.Len(t, mvIDRow, 1)
				mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
				require.NoError(t, err)
				fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
				fromTSO := oracle.GoTimeToTS(fromTime)
				tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d", fromTSO, mvID))
				tk.MustExec("insert into t_mv_refresh_asof_gc values (1, 10)")
				targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
				if sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond)); sleepUntilTarget > 0 {
					time.Sleep(sleepUntilTarget)
				}
				targetTSO := oracle.GoTimeToTS(targetTime)
				mustSetMockGCSafePoint(t, tk, targetTime.Add(time.Second))
				refreshSQL := fmt.Sprintf("refresh materialized view mv_refresh_asof_gc fast as of timestamp '%s'", targetTime.Format("2006-01-02 15:04:05.000"))
				if dryRun {
					refreshSQL += " dry run"
					if minMax {
						err = tk.QueryToErr(refreshSQL)
						require.True(t, terror.ErrorEqual(err, variable.ErrSnapshotTooOld), "err %v", err)
					} else {
						rows := tk.MustQuery(refreshSQL).Rows()
						requireRowsContainSubstring(t, rows, "_tidb_commit_ts")
						requireRowsContainSubstring(t, rows, strconv.FormatUint(fromTSO, 10))
						requireRowsContainSubstring(t, rows, strconv.FormatUint(targetTSO, 10))
					}
					return
				}

				if minMax {
					err = tk.ExecToErr(refreshSQL)
					require.True(t, terror.ErrorEqual(err, variable.ErrSnapshotTooOld), "err %v", err)
					tk.MustQuery("select * from mv_refresh_asof_gc").Check(testkit.Rows())
					tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvID)).Check(testkit.Rows(strconv.FormatUint(fromTSO, 10)))
					tk.MustQuery(fmt.Sprintf(
						"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
						targetTSO, mvID,
					)).Check(testkit.Rows("failed bounded fast manual 1 1"))
				} else {
					tk.MustExec(refreshSQL)
					tk.MustQuery("select a, s, cnt from mv_refresh_asof_gc").Check(testkit.Rows("1 10 1"))
					tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvID)).Check(testkit.Rows(strconv.FormatUint(targetTSO, 10)))
					tk.MustQuery(fmt.Sprintf(
						"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = %d from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
						targetTSO, mvID,
					)).Check(testkit.Rows("success bounded fast manual 1"))
				}
			})
		}
	}
}

func TestMaterializedViewRefreshFastAsOfTimestampMinMaxTargetSnapshot(t *testing.T) {
	t.Run("data", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")

		tk.MustExec("create table t_mv_refresh_asof_minmax_data (a int not null, b int not null, key idx_a(a))")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_minmax_data (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_minmax_data (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_minmax_data group by a")

		tk.MustExec("insert into t_mv_refresh_asof_minmax_data values (1, 10), (1, 20), (1, 30)")
		tk.MustExec("refresh materialized view mv_refresh_asof_minmax_data complete delta apply")
		tk.MustQuery("select * from mv_refresh_asof_minmax_data").Check(testkit.Rows("1 3 30 10"))

		tk.MustExec("delete from t_mv_refresh_asof_minmax_data where a = 1 and b = 30")
		targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
		sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
		if sleepUntilTarget > 0 {
			time.Sleep(sleepUntilTarget)
		}
		tk.MustExec("insert into t_mv_refresh_asof_minmax_data values (1, 100)")
		mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))

		targetTSO := oracle.GoTimeToTS(targetTime)
		tk.MustExec(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_minmax_data fast as of timestamp '%s'",
			targetTime.Format("2006-01-02 15:04:05.000"),
		))
		tk.MustQuery("select * from mv_refresh_asof_minmax_data").Check(testkit.Rows("1 2 20 10"))
		tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_minmax_data')").
			Check(testkit.Rows(strconv.FormatUint(targetTSO, 10)))
	})

	t.Run("schema", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set time_zone = '+00:00'")

		tk.MustExec("create table t_mv_refresh_asof_minmax_schema (a int not null, b int not null, key idx_ab(a, b))")
		tk.MustExec("create materialized view log on t_mv_refresh_asof_minmax_schema (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_asof_minmax_schema (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_minmax_schema group by a")

		tk.MustExec("insert into t_mv_refresh_asof_minmax_schema values (1, 10), (1, 20), (1, 30)")
		tk.MustExec("refresh materialized view mv_refresh_asof_minmax_schema complete delta apply")
		tk.MustQuery("select * from mv_refresh_asof_minmax_schema").Check(testkit.Rows("1 3 30 10"))

		tk.MustExec("delete from t_mv_refresh_asof_minmax_schema where a = 1 and b = 30")
		targetBeforeInvisible := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
		sleepUntilBeforeInvisible := time.Until(targetBeforeInvisible.Add(20 * time.Millisecond))
		if sleepUntilBeforeInvisible > 0 {
			time.Sleep(sleepUntilBeforeInvisible)
		}
		tk.MustExec("create index idx_a on t_mv_refresh_asof_minmax_schema (a)")
		tk.MustExec("alter table t_mv_refresh_asof_minmax_schema alter index idx_ab invisible")
		mustSetMockGCSafePoint(t, tk, targetBeforeInvisible.Add(-time.Hour))

		targetBeforeInvisibleTSO := oracle.GoTimeToTS(targetBeforeInvisible)
		tk.MustExec(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_minmax_schema fast as of timestamp '%s'",
			targetBeforeInvisible.Format("2006-01-02 15:04:05.000"),
		))
		tk.MustQuery("select * from mv_refresh_asof_minmax_schema").Check(testkit.Rows("1 2 20 10"))
		tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_minmax_schema')").
			Check(testkit.Rows(strconv.FormatUint(targetBeforeInvisibleTSO, 10)))

		tk.MustExec("delete from t_mv_refresh_asof_minmax_schema where a = 1 and b = 20")
		targetAfterInvisible := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
		sleepUntilAfterInvisible := time.Until(targetAfterInvisible.Add(20 * time.Millisecond))
		if sleepUntilAfterInvisible > 0 {
			time.Sleep(sleepUntilAfterInvisible)
		}
		mustSetMockGCSafePoint(t, tk, targetAfterInvisible.Add(-time.Hour))

		targetAfterInvisibleTSO := oracle.GoTimeToTS(targetAfterInvisible)
		tk.MustExec(fmt.Sprintf(
			"refresh materialized view mv_refresh_asof_minmax_schema fast as of timestamp '%s'",
			targetAfterInvisible.Format("2006-01-02 15:04:05.000"),
		))
		tk.MustQuery("select * from mv_refresh_asof_minmax_schema").Check(testkit.Rows("1 1 10 10"))
		tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_minmax_schema')").
			Check(testkit.Rows(strconv.FormatUint(targetAfterInvisibleTSO, 10)))
	})
}

func TestMaterializedViewRefreshFastMinMax(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")

		tk.MustExec("create table t_mv_fast_minmax (a int not null, b int not null, key idx_a(a))")
		tk.MustExec("create materialized view log on t_mv_fast_minmax (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_fast_minmax (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_mv_fast_minmax group by a")

		tk.MustExec("insert into t_mv_fast_minmax values (1, 10), (1, 20), (1, 30), (2, 5), (2, 8)")
		tk.MustExec("refresh materialized view mv_fast_minmax complete delta apply")
		tk.MustQuery("select * from mv_fast_minmax order by a").Check(testkit.Rows(
			"1 3 30 10",
			"2 2 8 5",
		))

		tk.MustExec("delete from t_mv_fast_minmax where a = 1 and b = 30")
		tk.MustExec("delete from t_mv_fast_minmax where a = 2 and b = 5")
		tk.MustExec("insert into t_mv_fast_minmax values (2, 12)")
		tk.MustExec("refresh materialized view mv_fast_minmax fast")
		tk.MustQuery("select * from mv_fast_minmax order by a").Check(testkit.Rows(
			"1 2 20 10",
			"2 2 12 8",
		))
	})

	t.Run("supporting_index", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")

		tk.MustExec(`CREATE TABLE t_mv_fast_minmax_hint (
		id BIGINT NOT NULL,
		c01 VARCHAR(32) NOT NULL DEFAULT '',
		c02 VARCHAR(64) NOT NULL DEFAULT '',
		c03 INT NOT NULL DEFAULT 0,
		c04 INT NOT NULL DEFAULT 0,
		c05 INT NOT NULL DEFAULT 0,
		c06 VARCHAR(16) NOT NULL DEFAULT '',
		c07 DATE NOT NULL,
		c08 INT NOT NULL DEFAULT 0,
		c09 DATETIME NOT NULL,
		c10 DATETIME DEFAULT '2100-01-01 00:00:00',
		m01 DECIMAL(19,6) NOT NULL DEFAULT 0,
		m02 DECIMAL(19,6) NOT NULL DEFAULT 0,
		m03 DECIMAL(19,6) NOT NULL DEFAULT 0,
		PRIMARY KEY (id),
		KEY idx_c02_c09 (c02, c09),
		KEY idx_mv_minmax (c07, c02, c06, c05, c04, c03, c01, c08, m02, m03)
	)`)
		tk.MustExec(`CREATE MATERIALIZED VIEW LOG ON t_mv_fast_minmax_hint(
		c04, c03, c01, c02, m01, m02, m03, c08, c05, c06, c07
	)`)
		tk.MustExec(`CREATE MATERIALIZED VIEW mv_fast_minmax_hint (
		c01, c02, c03, c04, c05, c06, c07,
		cnt, sum_m01, sum_m02, sum_m03, max_m02, max_m03, min_m03
	) REFRESH FAST AS
	SELECT
		t.c01, t.c02, t.c03, t.c04, t.c05, t.c06, t.c07,
		COUNT(1), SUM(t.m01), SUM(t.m02), SUM(t.m03),
		MAX(t.m02), MAX(t.m03), MIN(t.m03)
	FROM t_mv_fast_minmax_hint AS t
	WHERE t.c08 = 2
		AND t.c01 != 0
		AND t.c01 != ''
		AND t.c02 != ''
		AND t.c06 != '0'
		AND t.c06 != ''
		AND t.c05 != 0
		AND t.c03 != 0
		AND t.c04 != 0
		AND t.c01 = 'sample'
	GROUP BY t.c01, t.c02, t.c03, t.c04, t.c05, t.c06, t.c07`)
		tk.MustExec(`INSERT INTO t_mv_fast_minmax_hint VALUES (
		1, 'sample', 'k-1', 20, 10, 30, 'X', '2026-06-26', 2,
		'2026-06-26 10:00:00', '2026-06-26 10:05:00',
		100.000000, 80.000000, -5.000000
	)`)

		tk.MustExec("REFRESH MATERIALIZED VIEW mv_fast_minmax_hint FAST")
		requireRefreshMVStatementResult(t, tk, 0, 0, 0)
		tk.MustQuery("SELECT * FROM mv_fast_minmax_hint").Check(testkit.Rows())
		requireRefreshInfoSnapshotMatchesLatestSuccessHist(t, tk, "test", "mv_fast_minmax_hint")
		// Repeated empty refreshes still advance the refresh watermark and commit history.
		tk.MustExec("REFRESH MATERIALIZED VIEW mv_fast_minmax_hint FAST")
		requireRefreshMVStatementResult(t, tk, 0, 0, 0)
		requireRefreshInfoSnapshotMatchesLatestSuccessHist(t, tk, "test", "mv_fast_minmax_hint")
	})

	t.Run("missing_index", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")

		tk.MustExec("create table t_mv_fast_minmax_noidx (id int not null primary key, a int not null, b int not null, key idx_a(a))")
		tk.MustExec("create materialized view log on t_mv_fast_minmax_noidx (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_fast_minmax_noidx (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_mv_fast_minmax_noidx group by a")

		tk.MustExec("insert into t_mv_fast_minmax_noidx values (1, 1, 10), (2, 1, 20)")
		tk.MustExec("refresh materialized view mv_fast_minmax_noidx complete delta apply")

		err := tk.ExecToErr("alter table t_mv_fast_minmax_noidx alter index idx_a invisible")
		require.ErrorContains(t, err, "Unsupported ALTER INDEX INVISIBLE on base table index idx_a required by materialized view mv_fast_minmax_noidx for MIN/MAX fast refresh")
	})

	t.Run("nullable_duplicate_count", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")

		tk.MustExec("create table t_mv_fast_nullable_dup_count (a int not null, b int, key idx_ab(a, b))")
		tk.MustExec("create materialized view log on t_mv_fast_nullable_dup_count (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec(`create materialized view mv_fast_nullable_dup_count (a, cnt, cnt_b1, cnt_b2, s, mx, mn)
		refresh fast as
		select a, count(1), count(b) as cnt_b1, count(b) as cnt_b2, sum(b), max(b), min(b)
		from t_mv_fast_nullable_dup_count
		group by a`)

		tk.MustExec("insert into t_mv_fast_nullable_dup_count values (1, 10), (1, null), (1, 5), (2, null)")
		tk.MustExec("refresh materialized view mv_fast_nullable_dup_count complete delta apply")
		tk.MustQuery("select a, cnt, cnt_b1, cnt_b2, s, mx, mn from mv_fast_nullable_dup_count order by a").Check(testkit.Rows(
			"1 3 2 2 15 10 5",
			"2 1 0 0 <nil> <nil> <nil>",
		))

		tk.MustExec("delete from t_mv_fast_nullable_dup_count where a = 1 and b = 10")
		tk.MustExec("insert into t_mv_fast_nullable_dup_count values (1, 20), (1, null), (2, 7), (3, null), (3, 4)")
		tk.MustExec("refresh materialized view mv_fast_nullable_dup_count fast")
		tk.MustQuery("select a, cnt, cnt_b1, cnt_b2, s, mx, mn from mv_fast_nullable_dup_count order by a").Check(testkit.Rows(
			"1 4 2 2 25 20 5",
			"2 2 1 1 7 7 7",
			"3 2 1 1 4 4 4",
		))

		tk.MustExec("delete from t_mv_fast_nullable_dup_count where a = 1 and b in (5, 20)")
		tk.MustExec("delete from t_mv_fast_nullable_dup_count where a = 2 and b = 7")
		tk.MustExec("refresh materialized view mv_fast_nullable_dup_count fast")
		tk.MustQuery("select a, cnt, cnt_b1, cnt_b2, s, mx, mn from mv_fast_nullable_dup_count order by a").Check(testkit.Rows(
			"1 2 0 0 <nil> <nil> <nil>",
			"2 1 0 0 <nil> <nil> <nil>",
			"3 2 1 1 4 4 4",
		))
	})

	t.Run("where_separate_indexes", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")

		tk.MustExec(`create table t_mv_fast_minmax_where (
		id bigint not null primary key,
		g1 int not null,
		v1 bigint not null,
		v2 decimal(10,2) not null,
		c1 int not null,
		f1 int not null,
		key idx_g1(g1),
		key idx_f1(f1)
	)`)
		tk.MustExec("create materialized view log on t_mv_fast_minmax_where (id, g1, v1, v2, c1, f1) purge next date_add(now(), interval 1 hour)")
		tk.MustExec(`create materialized view mv_fast_minmax_where (g1, cnt, s_v1, min_v2, max_v2, cnt_c1)
		refresh fast next date_add(now(), interval 1 hour) as
		select g1, count(*), sum(v1), min(v2), max(v2), count(c1)
		from t_mv_fast_minmax_where
		where f1 = 1
		group by g1`)

		tk.MustExec("insert into t_mv_fast_minmax_where values (19001, 1, 1, 1.00, 1, 1), (19002, 2, 2, 2.00, 1, 1)")
		tk.MustExec("refresh materialized view mv_fast_minmax_where fast")
		tk.MustQuery("select * from mv_fast_minmax_where order by g1").Check(testkit.Rows(
			"1 1 1 1.00 1.00 1",
			"2 1 2 2.00 2.00 1",
		))

		tk.MustExec("update t_mv_fast_minmax_where set g1 = 2, v1 = 10, f1 = 2, v2 = 9.00, c1 = 1 where id = 19001")
		tk.MustExec("refresh materialized view mv_fast_minmax_where fast")
		tk.MustQuery("select * from mv_fast_minmax_where order by g1").Check(testkit.Rows(
			"2 1 2 2.00 2.00 1",
		))

		tk.MustExec("update t_mv_fast_minmax_where set f1 = 1 where id = 19001")
		tk.MustExec("refresh materialized view mv_fast_minmax_where fast")
		tk.MustQuery("select * from mv_fast_minmax_where order by g1").Check(testkit.Rows(
			"2 2 12 2.00 9.00 2",
		))
	})
}

func TestMaterializedViewRefreshCompleteUsesDefinitionSessionSemantics(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.sql_mode = 'NO_UNSIGNED_SUBTRACTION'")
	tk.MustExec("set @@session.time_zone = '+00:00'")
	tk.MustExec("create table t (a int not null, b int not null, ts timestamp not null)")
	tk.MustExec("insert into t values (1, 10, '2020-01-01 00:30:00'), (2, 20, '2019-12-31 23:30:00')")
	tk.MustExec("create materialized view log on t (a, b, ts) purge next date_add(now(), interval 1 hour)")
	tk.MustExec(`create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as
select a, sum(b), count(1) from t
where ts >= '2020-01-01 00:00:00'
group by a`)
	tk.MustQuery("select a, s, cnt from mv").Check(testkit.Rows("1 10 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
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
	tk.MustExec("refresh materialized view mv complete delta apply")
	tk.MustQuery("select a, s, cnt from mv").Check(testkit.Rows("1 10 1"))
}

func TestMaterializedViewRefreshHistory(t *testing.T) {
	t.Run("finalize_retry", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		tk.MustExec("insert into t values (2, 3), (3, 4)")

		const finalizeFailpoint = "github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError"
		require.NoError(t, failpoint.Enable(finalizeFailpoint, "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable(finalizeFailpoint))
		}()

		tk.MustExec("refresh materialized view mv complete delta apply")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
		tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
			Check(testkit.Rows("success complete delta apply manual 1 1 1"))
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'", mviewID)).
			Check(testkit.Rows("0"))
	})

	t.Run("finalize_and_cleanup_failure", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		tk.MustExec("insert into t values (2, 3), (3, 4)")

		const finalizeFailpoint = "github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError"
		require.NoError(t, failpoint.Enable(finalizeFailpoint, "return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable(finalizeFailpoint))
		}()
		const releaseLockFailpoint = "github.com/pingcap/tidb/pkg/executor/mockReleaseMVRefreshAdvisoryLockFullyCount"
		require.NoError(t, failpoint.Enable(releaseLockFailpoint, "return(0)"))
		defer func() {
			require.NoError(t, failpoint.Disable(releaseLockFailpoint))
		}()

		err = tk.ExecToErr("refresh materialized view mv complete delta apply")
		require.Error(t, err)
		require.ErrorContains(t, err, "advisory lock cleanup invariant violated")

		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
		tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_END_TIME is null, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
			Check(testkit.Rows("running 1 1"))
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'failed'", mviewID)).
			Check(testkit.Rows("0"))
	})

	t.Run("running_lifecycle", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
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
			tkRefresh := newMViewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete delta apply")
		}()

		select {
		case <-hitCh:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh to persist running history row")
		}

		tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is null, LAST_HEARTBEAT_TIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
			Check(testkit.Rows("running complete delta apply manual 1 1 1 1"))

		close(pauseCh)

		select {
		case err := <-refreshDone:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh to finish")
		}

		tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
			Check(testkit.Rows("success complete delta apply manual 1 1 1"))
	})

	t.Run("heartbeat", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		tk.MustExec("insert into t values (2, 3), (3, 4)")

		pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
		require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
		defer func() {
			require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
		}()
		heartbeatIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskHistHeartbeatInterval"
		require.NoError(t, failpoint.Enable(heartbeatIntervalFailpoint, "return(50)"))
		defer func() {
			require.NoError(t, failpoint.Disable(heartbeatIntervalFailpoint))
		}()

		pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning"
		require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
		paused := true
		defer func() {
			if paused {
				require.NoError(t, failpoint.Disable(pauseFailpoint))
			}
		}()

		refreshDone := make(chan error, 1)
		go func() {
			tkRefresh := newMViewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete delta apply")
		}()

		var firstHeartbeat string
		require.Eventually(t, func() bool {
			rows := tk.MustQuery(fmt.Sprintf(
				"select cast(LAST_HEARTBEAT_TIME as char) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running' order by REFRESH_JOB_ID desc limit 1",
				mviewID,
			)).Rows()
			if len(rows) == 0 || rows[0][0] == nil {
				return false
			}
			firstHeartbeat = fmt.Sprint(rows[0][0])
			return firstHeartbeat != ""
		}, 10*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			rows := tk.MustQuery(fmt.Sprintf(
				"select cast(LAST_HEARTBEAT_TIME as char) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running' order by REFRESH_JOB_ID desc limit 1",
				mviewID,
			)).Rows()
			if len(rows) == 0 || rows[0][0] == nil {
				return false
			}
			return fmt.Sprint(rows[0][0]) != firstHeartbeat
		}, 10*time.Second, 100*time.Millisecond)

		require.NoError(t, failpoint.Disable(pauseFailpoint))
		paused = false

		select {
		case err := <-refreshDone:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh to finish")
		}

		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, LAST_HEARTBEAT_TIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("success 1"))
	})
}

func TestMaterializedViewRefreshCompleteReadTSOSnapshotMatchesMV(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
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
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
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
		tkRefresh := newMViewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete delta apply")
	}()

	select {
	case <-hitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to reach failpoint")
	}

	// Commit new base table data after refresh txn start_ts but before refresh reads with for_update_ts.
	tkConcurrent := newMViewTestKit(t, store)
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

	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", refreshReadTSO, mviewID)).
		Check(testkit.Rows("success complete delta apply manual 1 1 1"))
}

func TestMaterializedViewRefreshCompleteRefreshInfoCASUpdateAfterConcurrentPreUpdate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
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
		tkRefresh := newMViewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete delta apply")
	}()

	select {
	case <-hitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to reach failpoint")
	}

	// Update refresh info row after refresh txn start_ts but before it does the locking read.
	injectedTS := oldTS + 1000
	tkConcurrent := newMViewTestKit(t, store)
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
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", newTS, mviewID)).
		Check(testkit.Rows("success complete delta apply manual 1 1 1"))
}

func TestMaterializedViewRefreshCompleteConcurrentNowait(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tkLocker := newMViewTestKit(t, store)
	tkLocker.MustExec("use test")
	tkLocker.MustExec("begin pessimistic")
	tkLocker.MustQuery(fmt.Sprintf("select MVIEW_ID from mysql.tidb_mview_refresh_info where MVIEW_ID = %d for update", mviewID))

	tkRefresh := newMViewTestKit(t, store)
	tkRefresh.MustExec("use test")
	err = tkRefresh.ExecToErr("refresh materialized view mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "NOWAIT")

	tkLocker.MustExec("rollback")

	// After releasing the lock, refresh should succeed.
	tkRefresh.MustExec("refresh materialized view mv complete delta apply")
}

func TestMaterializedViewRefreshCompleteMissingRefreshInfoRow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID))
	err = tk.ExecToErr("refresh materialized view mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
}

func TestMaterializedViewRefreshWithAsyncModeComplete(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	err := tk.ExecToErr("refresh materialized view mv with async mode complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "WITH ASYNC MODE is not supported yet")
}

func TestMaterializedViewRefreshCompleteDeltaApply(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
		require.Len(t, oldTSRow, 1)
		oldTS, err := strconv.ParseUint(fmt.Sprintf("%v", oldTSRow[0][0]), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, oldTS)

		tk.MustExec("insert into t values (2, 3), (3, 4)")
		tk.MustExec("refresh materialized view mv complete delta apply")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))

		newTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
		require.Len(t, newTSRow, 1)
		newTS, err := strconv.ParseUint(fmt.Sprintf("%v", newTSRow[0][0]), 10, 64)
		require.NoError(t, err)
		require.NotEqual(t, oldTS, newTS)

		tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO > 0 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mviewID)).
			Check(testkit.RowsWithSep("|", "success|complete delta apply manual|1|1|1|1"))

		tk.MustExec("insert into t values (4, 8)")
		mustExecRefreshInternal(t, tk, "refresh materialized view mv complete delta apply")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1", "4 8 1"))
		tk.MustQuery(fmt.Sprintf("select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
			Check(testkit.RowsWithSep("|", "complete delta apply auto"))
	})

	t.Run("in_place", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		tk.MustExec("insert into t values (2, 3), (3, 4)")
		tk.MustExec("refresh materialized view mv complete in place")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
			mviewID,
		)).Check(testkit.RowsWithSep("|", "success|complete in place manual|1|1|1|1"))
	})

	t.Run("implement_stmt", func(t *testing.T) {
		testCases := []struct {
			name     string
			prepare  func(*testkit.TestKit)
			expected []string
		}{
			{
				name: "insert-only",
				prepare: func(tk *testkit.TestKit) {
					tk.MustExec("insert into t values (2, 3), (3, 4)")
				},
				expected: []string{"1 15 2", "2 10 2", "3 4 1"},
			},
			{
				name: "delete-only",
				prepare: func(tk *testkit.TestKit) {
					tk.MustExec("delete from t where a = 1 and b = 5")
				},
				expected: []string{"1 10 1", "2 7 1"},
			},
			{
				name: "update-only",
				prepare: func(tk *testkit.TestKit) {
					tk.MustExec("update t set b = 8 where a = 2 and b = 7")
				},
				expected: []string{"1 15 2", "2 8 1"},
			},
			{
				name: "mixed",
				prepare: func(tk *testkit.TestKit) {
					tk.MustExec("update t set b = 11 where a = 1 and b = 10")
					tk.MustExec("delete from t where a = 2 and b = 7")
					tk.MustExec("insert into t values (3, 4)")
				},
				expected: []string{"1 16 2", "3 4 1"},
			},
			{
				name:     "noop",
				prepare:  func(*testkit.TestKit) {},
				expected: []string{"1 15 2", "2 7 1"},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				store, _ := testkit.CreateMockStoreAndDomain(t)
				tk := newMViewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("create table t (a int not null, b int not null)")
				tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
				tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
				tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
				tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

				tc.prepare(tk)
				mustExecCompleteDeltaApplyImplementStmt(t, tk, "test", "mv")
				tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows(tc.expected...))
			})
		}
	})

	t.Run("for_update_ts", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2"))

		tkRefresh := newMViewTestKit(t, store)
		tkRefresh.MustExec("use test")
		tkRefresh.MustExec("begin pessimistic")

		tkConcurrent := newMViewTestKit(t, store)
		tkConcurrent.MustExec("use test")
		tkConcurrent.MustExec("insert into t values (2, 7)")

		mustExecCompleteDeltaApplyImplementStmt(t, tkRefresh, "test", "mv")
		tkRefresh.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
		tkRefresh.MustExec("commit")

		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
	})

	t.Run("rollback", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
		require.Len(t, oldTSRow, 1)
		oldTS, err := strconv.ParseUint(fmt.Sprintf("%v", oldTSRow[0][0]), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, oldTS)

		tk.MustExec("insert into t values (2, 8)")

		const refreshFailpoint = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges"
		require.NoError(t, failpoint.Enable(refreshFailpoint, `return("mock refresh data change failure")`))
		defer func() {
			require.NoError(t, failpoint.Disable(refreshFailpoint))
		}()

		err = tk.ExecToErr("refresh materialized view mv complete delta apply")
		require.Error(t, err)
		require.ErrorContains(t, err, "mock refresh data change failure")

		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
		tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO = %d from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldTS, mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
			Check(testkit.RowsWithSep("|", "failed|complete delta apply manual|1|1|1"))
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'", mviewID)).
			Check(testkit.Rows("0"))
		reasonRow := tk.MustQuery(fmt.Sprintf("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).Rows()
		require.Len(t, reasonRow, 1)
		require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "mock refresh data change failure")
	})
}

func TestMaterializedViewRefreshEarlyFailureWritesHist(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	histCountBefore := tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mviewID,
	)).Rows()
	require.Len(t, histCountBefore, 1)
	require.Len(t, histCountBefore[0], 1)
	histCountBeforeVal, err := strconv.Atoi(fmt.Sprintf("%v", histCountBefore[0][0]))
	require.NoError(t, err)

	const failpointName = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist"
	require.NoError(t, failpoint.Enable(failpointName, `return("mock early refresh failure")`))
	defer func() {
		require.NoError(t, failpoint.Disable(failpointName))
	}()

	err = tk.ExecToErr("refresh materialized view mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "mock early refresh failure")

	tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows(fmt.Sprintf("%d", histCountBeforeVal+1)))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("failed complete delta apply manual 1 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'",
		mviewID,
	)).Check(testkit.Rows("0"))
	reasonRow := tk.MustQuery(fmt.Sprintf(
		"select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Rows()
	require.Len(t, reasonRow, 1)
	require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "mock early refresh failure")
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, 'test', 'mv', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())",
		oldMViewID,
	))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", oldMViewID)).
		Check(testkit.Rows("1"))

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv complete out of place")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))

	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	newMViewID := mvTable.Meta().ID
	require.NotEqual(t, oldMViewID, newMViewID)

	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Contains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, newMViewID)
	require.NotContains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, oldMViewID)

	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).
		Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", oldMViewID)).
		Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO > 0 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", newMViewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		oldMViewID,
	)).Check(testkit.Rows("success complete out of place manual 1 1"))
	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())

	rows := tk.MustQuery("admin show ddl jobs where JOB_TYPE='refresh materialized view complete out-of-place cutover'").Rows()
	require.NotEmpty(t, rows)
	jobID := fmt.Sprint(rows[0][0])
	tk.MustQuery("select ((select count(*) from mysql.gc_delete_range where job_id=" + jobID + ") + (select count(*) from mysql.gc_delete_range_done where job_id=" + jobID + ")) > 0").Check(testkit.Rows("1"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceUpdatesMLogDependencies(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_dep (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_dep (a, b)")
	tk.MustExec("create materialized view mv_dep (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_dep group by a")

	is := dom.InfoSchema()
	oldMV, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_dep"))
	require.NoError(t, err)
	oldMVID := oldMV.Meta().ID

	tk.MustExec("refresh materialized view mv_dep complete out of place")

	is = dom.InfoSchema()
	newMV, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_dep"))
	require.NoError(t, err)
	newMVID := newMV.Meta().ID
	require.NotEqual(t, oldMVID, newMVID)
	mlog, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_dep"))
	require.NoError(t, err)
	require.NotNil(t, mlog.Meta().MaterializedViewLog)
	require.Contains(t, mlog.Meta().MaterializedViewLog.DependentMViewIDs, newMVID)
	require.NotContains(t, mlog.Meta().MaterializedViewLog.DependentMViewIDs, oldMVID)

	tk.MustExec("drop materialized view mv_dep")
	tk.MustExec("drop materialized view log on t_dep")
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverFailure(t *testing.T) {
	t.Run("refresh_info_rollback", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		oldMViewID := mvTable.Meta().ID

		originErrLimit := vardef.GetDDLErrorCountLimit()
		vardef.SetDDLErrorCountLimit(0)
		defer vardef.SetDDLErrorCountLimit(originErrLimit)

		const cutoverAfterMigrateRefreshInfoErrorFailpoint = "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError"
		require.NoError(t, failpoint.Enable(cutoverAfterMigrateRefreshInfoErrorFailpoint, "1*return()"))
		defer func() {
			require.NoError(t, failpoint.Disable(cutoverAfterMigrateRefreshInfoErrorFailpoint))
		}()

		tk.MustExec("insert into t values (2, 3), (3, 4)")
		err = tk.ExecToErr("refresh materialized view mv complete out of place")
		require.Error(t, err)

		is = dom.InfoSchema()
		mvTable, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.Equal(t, oldMViewID, mvTable.Meta().ID)
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		require.Equal(t, []int64{oldMViewID}, mlogTable.Meta().MaterializedViewLog.DependentMViewIDs)
		tk.MustQuery("select MVIEW_ID from mysql.tidb_mview_refresh_info").Check(testkit.Rows(fmt.Sprintf("%d", oldMViewID)))
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
	})

	t.Run("publish_event", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_cutover_publish_error (a int not null, b int not null)")
		tk.MustExec("create materialized view log on t_cutover_publish_error (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_cutover_publish_error (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_cutover_publish_error group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_cutover_publish_error"))
		require.NoError(t, err)
		oldMViewID := mvTable.Meta().ID
		tk.MustExec(fmt.Sprintf(
			"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, UPDATE_TIME) values (%d, 'test', 'mv_cutover_publish_error', 'warning', UTC_TIMESTAMP())",
			oldMViewID,
		))

		originErrLimit := vardef.GetDDLErrorCountLimit()
		vardef.SetDDLErrorCountLimit(0)
		defer vardef.SetDDLErrorCountLimit(originErrLimit)

		const publishEventErrorFailpoint = "github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError"
		require.NoError(t, failpoint.Enable(publishEventErrorFailpoint, "1*return()"))
		defer func() { _ = failpoint.Disable(publishEventErrorFailpoint) }()
		tk.MustExec("insert into t_cutover_publish_error values (1, 10)")
		err = tk.ExecToErr("refresh materialized view mv_cutover_publish_error complete out of place")
		require.ErrorContains(t, err, "mock publish event error")
		require.NoError(t, failpoint.Disable(publishEventErrorFailpoint))

		is = dom.InfoSchema()
		mvTable, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_cutover_publish_error"))
		require.NoError(t, err)
		require.Equal(t, oldMViewID, mvTable.Meta().ID)
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", oldMViewID)).Check(testkit.Rows("1"))

		tk.MustExec("refresh materialized view mv_cutover_publish_error complete out of place")
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", oldMViewID)).Check(testkit.Rows("0"))
	})

	t.Run("watermark_cas", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		tkConcurrent := newMViewTestKit(t, store)
		tkConcurrent.MustExec("use test")
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterBuildDataLoad", func(uint64) {
			tkConcurrent.MustExec(fmt.Sprintf(
				"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = LAST_SUCCESS_READ_TSO + 1 where MVIEW_ID = %d",
				mviewID,
			))
		})

		tk.MustExec("insert into t values (2, 3), (3, 4)")
		err = tk.ExecToErr("refresh materialized view mv complete out of place")
		require.Error(t, err)
		require.ErrorContains(t, err, "stale LAST_SUCCESS_READ_TSO")

		is = dom.InfoSchema()
		mvTable, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.Equal(t, mviewID, mvTable.Meta().ID)
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("failed complete out of place manual 1 1"))
		tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
		// Cutover CAS mismatch should keep old MV serving table unchanged.
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
	})

	t.Run("revision_cas", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int not null, b int not null)")
		tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		tk.MustExec("insert into t values (2, 3), (3, 4)")

		const pauseCreateShadowFailpoint = "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewOutOfPlaceAfterCreateShadow"
		require.NoError(t, failpoint.Enable(pauseCreateShadowFailpoint, "pause"))
		enabled := true
		defer func() {
			if enabled {
				require.NoError(t, failpoint.Disable(pauseCreateShadowFailpoint))
			}
		}()

		refreshDone := make(chan error, 1)
		go func() {
			tkRefresh := newMViewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete out of place")
		}()

		require.Eventually(t, func() bool {
			return len(tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Rows()) == 1
		}, 30*time.Second, 100*time.Millisecond)

		tkConcurrent := newMViewTestKit(t, store)
		tkConcurrent.MustExec("use test")
		tkConcurrent.MustExec("create index idx_mv_s_refresh_ddl on mv (s)")

		require.NoError(t, failpoint.Disable(pauseCreateShadowFailpoint))
		enabled = false

		err = <-refreshDone
		require.Error(t, err)
		require.ErrorContains(t, err, "stale old materialized view revision detected before cutover")

		is = dom.InfoSchema()
		mvTable, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.Equal(t, mviewID, mvTable.Meta().ID)

		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
			Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("failed complete out of place manual 1 1"))
		tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
		tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
		require.NotEmpty(t, tk.MustQuery("show index from mv where Key_name = 'idx_mv_s_refresh_ddl'").Rows())
	})
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverKeepsTiFlashProgressUntilCommit(t *testing.T) {
	const mockTiFlashStoreCountFailpoint = "github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"
	const cutoverBeforeCommitFailpoint = "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverBeforeCommitError"
	require.NoError(t, failpoint.Enable(mockTiFlashStoreCountFailpoint, "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(mockTiFlashStoreCountFailpoint))
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_tiflash_cutover (a int not null, b int not null)")
	tk.MustExec("insert into t_tiflash_cutover values (1, 10), (2, 7)")
	tk.MustExec("create materialized view log on t_tiflash_cutover (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_tiflash_cutover (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_tiflash_cutover group by a")
	tk.MustExec("alter table mv_tiflash_cutover set tiflash replica 1")

	mvTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_tiflash_cutover"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID
	infosync.UpdateTiFlashProgressCache(oldMViewID, 0.75)
	_, exists := infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.True(t, exists)

	originErrLimit := vardef.GetDDLErrorCountLimit()
	vardef.SetDDLErrorCountLimit(0)
	defer vardef.SetDDLErrorCountLimit(originErrLimit)
	require.NoError(t, failpoint.Enable(cutoverBeforeCommitFailpoint, "return"))
	tk.MustExec("insert into t_tiflash_cutover values (3, 4)")
	err = tk.ExecToErr("refresh materialized view mv_tiflash_cutover complete out of place")
	require.ErrorContains(t, err, "error before commit")
	require.NoError(t, failpoint.Disable(cutoverBeforeCommitFailpoint))

	_, exists = infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.True(t, exists)

	tk.MustExec("refresh materialized view mv_tiflash_cutover complete out of place")
	_, exists = infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.False(t, exists)
}

func TestMaterializedViewRefreshCompleteOutOfPlaceShadowTableProtected(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_shadow_guard (a int not null, b int not null)")
	tk.MustExec("insert into t_shadow_guard values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_shadow_guard (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_shadow_guard (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_shadow_guard group by a")
	tk.MustExec("insert into t_shadow_guard values (2, 3), (3, 4)")

	const pauseCreateShadowFailpoint = "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewOutOfPlaceAfterCreateShadow"
	require.NoError(t, failpoint.Enable(pauseCreateShadowFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseCreateShadowFailpoint))
		}
	}()

	refreshDone := make(chan error, 1)
	go func() {
		tkRefresh := newMViewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_shadow_guard complete out of place")
	}()

	var shadowTableName string
	require.Eventually(t, func() bool {
		rows := tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Rows()
		if len(rows) != 1 {
			return false
		}
		shadowTableName = fmt.Sprint(rows[0][0])
		return shadowTableName != ""
	}, 30*time.Second, 100*time.Millisecond)

	err := tk.ExecToErr(fmt.Sprintf("create materialized view log on `%s` (a)", shadowTableName))
	require.EqualError(t, err, dbterror.ErrWrongObject.GenWithStackByArgs("test", shadowTableName, "BASE TABLE").Error())

	tkUser := newMViewTestKit(t, store)
	tkUser.MustExec("use test")
	tkUser.Session().GetSessionVars().User = &auth.UserIdentity{AuthUsername: "test", AuthHostname: "%"}
	err = tkUser.ExecToErr(fmt.Sprintf("insert into `%s` values (9, 9, 9)", shadowTableName))
	require.ErrorContains(t, err, "not updatable")
	err = tkUser.ExecToErr(fmt.Sprintf("alter table `%s` add column x int", shadowTableName))
	require.ErrorContains(t, err, "ALTER TABLE on materialized view shadow table")
	err = tkUser.ExecToErr(fmt.Sprintf("drop table `%s`", shadowTableName))
	require.ErrorContains(t, err, "DROP TABLE on materialized view shadow table")
	err = tkUser.ExecToErr(fmt.Sprintf(
		"update t_shadow_guard n join `%s` s on n.a = s.a set n.b = s.s", shadowTableName,
	))
	require.ErrorContains(t, err, "SELECT command denied")
	err = tkUser.ExecToErr(fmt.Sprintf(
		"delete n from t_shadow_guard n join `%s` s on n.a = s.a", shadowTableName,
	))
	require.ErrorContains(t, err, "SELECT command denied")

	require.NoError(t, failpoint.Disable(pauseCreateShadowFailpoint))
	enabled = false
	require.NoError(t, <-refreshDone)

	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
	tk.MustQuery("select a, s, cnt from mv_shadow_guard order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverWithUnsignedBuildReadTSO(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID

	// Keep buildReadTSO above MaxInt64 to guard against any signed-int conversion in cutover path.
	buildReadTSO := (uint64(1) << 63) + 12345
	const overrideBuildTSFailpoint = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewOutOfPlaceBuildReadTSO"
	require.NoError(t, failpoint.Enable(overrideBuildTSFailpoint, fmt.Sprintf(`return("%d")`, buildReadTSO)))
	defer func() {
		require.NoError(t, failpoint.Disable(overrideBuildTSFailpoint))
	}()

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv complete out of place")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))

	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	newMViewID := mvTable.Meta().ID
	require.NotEqual(t, oldMViewID, newMViewID)

	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).
		Check(testkit.Rows("0"))
	persistedTSRows := tk.MustQuery(fmt.Sprintf(
		"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		newMViewID,
	)).Rows()
	require.Len(t, persistedTSRows, 1)
	persistedBuildReadTSO, err := strconv.ParseUint(fmt.Sprintf("%v", persistedTSRows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Equal(t, buildReadTSO, persistedBuildReadTSO)
}

func TestMaterializedViewRefreshCompleteOutOfPlaceBuildFailureCleansShadow(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	mviewIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv'").Rows()
	require.Len(t, mviewIDRows, 1)
	mviewID, err := strconv.ParseInt(fmt.Sprintf("%v", mviewIDRows[0][0]), 10, 64)
	require.NoError(t, err)
	tk.MustExec("insert into t values (2, 8)")

	const buildFailpoint = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow"
	require.NoError(t, failpoint.Enable(buildFailpoint, `return("mock out-of-place shadow load failure")`))
	defer func() {
		require.NoError(t, failpoint.Disable(buildFailpoint))
	}()

	err = tk.ExecToErr("refresh materialized view mv complete out of place")
	require.Error(t, err)
	require.ErrorContains(t, err, "mock out-of-place shadow load failure")
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("failed complete out of place manual 1 1"))
	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
	rows := tk.MustQuery("select job_id from mysql.tidb_ddl_history order by job_id desc limit 20").Rows()
	var cleanupJobID string
	for _, row := range rows {
		id, parseErr := strconv.ParseInt(fmt.Sprint(row[0]), 10, 64)
		if parseErr != nil {
			continue
		}
		job, jobErr := ddl.GetHistoryJobByID(tk.Session(), id)
		if jobErr == nil && job != nil && job.Type == model.ActionDropMaterializedViewShadow {
			cleanupJobID = fmt.Sprint(id)
			break
		}
	}
	require.NotEmpty(t, cleanupJobID)
	tk.MustQuery("select ((select count(*) from mysql.gc_delete_range where job_id=" + cleanupJobID + ") + (select count(*) from mysql.gc_delete_range_done where job_id=" + cleanupJobID + ")) > 0").Check(testkit.Rows("1"))
	// Out-of-place build failure should not modify old MV serving table.
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestMaterializedViewRefreshCompleteFailureKeepsRefreshInfoReadTSO(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	oldTSRow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, oldTSRow, 1)
	oldTS, err := strconv.ParseUint(fmt.Sprintf("%v", oldTSRow[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, oldTS)

	tk.MustExec("insert into t values (2, 8)")

	const refreshFailpoint = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges"
	require.NoError(t, failpoint.Enable(refreshFailpoint, `return("mock refresh data change failure")`))
	defer func() {
		require.NoError(t, failpoint.Disable(refreshFailpoint))
	}()

	const finalizeFailpoint = "github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError"
	require.NoError(t, failpoint.Enable(finalizeFailpoint, "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(finalizeFailpoint))
	}()

	err = tk.ExecToErr("refresh materialized view mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "mock refresh data change failure")

	// MV data should remain unchanged due to transactional refresh.
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO = %d from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldTS, mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.Rows("failed complete delta apply manual 1 1 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'", mviewID)).
		Check(testkit.Rows("0"))
	reasonRow := tk.MustQuery(fmt.Sprintf("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).Rows()
	require.Len(t, reasonRow, 1)
	require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "mock refresh data change failure")
}

func TestMaterializedViewRefreshFailureReportsAlertBeforeFinalizeHistFailure(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) attributes='mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec("insert into t values (2, 8)")

	const refreshFailpoint = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges"
	require.NoError(t, failpoint.Enable(refreshFailpoint, `return("mock refresh data change failure")`))
	defer func() {
		require.NoError(t, failpoint.Disable(refreshFailpoint))
	}()

	const finalizeFailpoint = "github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError"
	require.NoError(t, failpoint.Enable(finalizeFailpoint, "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(finalizeFailpoint))
	}()

	err = tk.ExecToErr("refresh materialized view mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to finalize refresh history after error")
	require.ErrorContains(t, err, "mock refresh data change failure")

	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_FAILED, ALERT_LEVEL is null from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows("YES 1"))
}

func TestMaterializedViewRefreshCompleteWithConstraintCheckInPlacePessimisticOff(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	// Turn off in-place constraint check for pessimistic txn.
	// Refresh should still succeed.
	tk.MustExec("set session tidb_constraint_check_in_place_pessimistic = off")
	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv complete in place")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery("select @@tidb_constraint_check_in_place_pessimistic").Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshRequiresOperateViewPrivilege(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create user 'mv_refresh_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'mv_refresh_u'@'%'")

	tkUser := newMViewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_refresh_u", Hostname: "%"}, nil, nil, nil))

	err := tkUser.ExecToErr("refresh materialized view test.mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "OPERATE VIEW command denied")

	tk.MustExec("grant operate view on test.mv to 'mv_refresh_u'@'%'")
	err = tkUser.ExecToErr("refresh materialized view test.mv complete delta apply")
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant select on test.t to 'mv_refresh_u'@'%'")
	tkUser.MustExec("refresh materialized view test.mv complete delta apply")

	err = tkUser.ExecToErr("refresh materialized view test.mv complete delta apply dry run")
	require.ErrorContains(t, err, "SHOW VIEW command denied")

	tk.MustExec("grant show view on test.mv to 'mv_refresh_u'@'%'")
	rows := tkUser.MustQuery("refresh materialized view test.mv complete delta apply dry run").Rows()
	dryRunOutput := fmt.Sprint(rows)
	require.Contains(t, dryRunOutput, "INSERT_HIST_RUNNING")
	require.Contains(t, dryRunOutput, "FINALIZE_HIST")
}

func TestMaterializedViewRefreshCancellation(t *testing.T) {
	t.Run("watcher_hist_request", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_refresh_cancel_watch (a int not null, b int not null)")
		tk.MustExec("insert into t_refresh_cancel_watch values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t_refresh_cancel_watch (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_cancel_watch (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_cancel_watch group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_refresh_cancel_watch"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
		require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
		defer func() {
			require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
		}()

		pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning"
		require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
		paused := true
		defer func() {
			if paused {
				require.NoError(t, failpoint.Disable(pauseFailpoint))
			}
		}()

		refreshDone := make(chan error, 1)
		go func() {
			tkRefresh := newMViewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_refresh_cancel_watch complete delta apply")
		}()

		require.Eventually(t, func() bool {
			rows := tk.MustQuery(fmt.Sprintf(
				"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'",
				mviewID,
			)).Rows()
			return fmt.Sprint(rows[0][0]) == "1"
		}, 10*time.Second, 100*time.Millisecond)

		requester := "'refresh_watcher_req'@'stage-c'"
		requestedCh := waitMVTaskCancelWatcherRequested(t, "refresh-")
		tk.MustExec(
			`UPDATE mysql.tidb_mview_refresh_hist
SET CANCEL_REQUEST_TIME = NOW(6),
	CANCEL_REQUESTED_BY = ?
WHERE MVIEW_ID = ?
  AND REFRESH_STATUS = 'running'
  AND CANCEL_REQUEST_TIME IS NULL`,
			requester,
			mviewID,
		)
		select {
		case <-requestedCh:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh cancel watcher to observe request")
		}

		require.NoError(t, failpoint.Disable(pauseFailpoint))
		paused = false

		select {
		case err := <-refreshDone:
			require.Error(t, err)
			require.ErrorContains(t, err, "materialized view task canceled manually")
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh to finish")
		}

		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("failed complete delta apply manual 1"))
		tk.MustQuery(fmt.Sprintf(
			"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'",
			mviewID,
		)).Check(testkit.Rows("0"))
		reasonRows := tk.MustQuery(fmt.Sprintf(
			"select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Rows()
		require.Len(t, reasonRows, 1)
		require.Equal(t, "cancelled manually by "+requester, fmt.Sprint(reasonRows[0][0]))
	})

	t.Run("cancel_job", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_refresh_cancel_job (a int not null, b int not null)")
		tk.MustExec("insert into t_refresh_cancel_job values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t_refresh_cancel_job (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_cancel_job (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_cancel_job group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_refresh_cancel_job"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
		require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
		defer func() {
			require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
		}()

		pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning"
		require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
		paused := true
		defer func() {
			if paused {
				require.NoError(t, failpoint.Disable(pauseFailpoint))
			}
		}()

		refreshDone := make(chan error, 1)
		go func() {
			tkRefresh := newMViewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_refresh_cancel_job complete delta apply")
		}()

		require.Eventually(t, func() bool {
			rows := tk.MustQuery(fmt.Sprintf(
				"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'",
				mviewID,
			)).Rows()
			return fmt.Sprint(rows[0][0]) == "1"
		}, 10*time.Second, 100*time.Millisecond)

		jobIDRows := tk.MustQuery(fmt.Sprintf(
			"select REFRESH_JOB_ID from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running' order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Rows()
		require.Len(t, jobIDRows, 1)
		jobID := fmt.Sprint(jobIDRows[0][0])

		tk.MustExec("create user 'mv_refresh_cancel_u'@'%' identified by ''")
		defer tk.MustExec("drop user 'mv_refresh_cancel_u'@'%'")

		tkCancel := newMViewTestKit(t, store)
		require.NoError(t, tkCancel.Session().Auth(&auth.UserIdentity{Username: "mv_refresh_cancel_u", Hostname: "%"}, nil, nil, nil))
		err = tkCancel.ExecToErr(fmt.Sprintf("cancel materialized view refresh job %s", jobID))
		require.ErrorContains(t, err, "OPERATE VIEW command denied")
		tk.MustExec("grant operate view on test.mv_refresh_cancel_job to 'mv_refresh_cancel_u'@'%'")
		requestedCh := waitMVTaskCancelWatcherRequested(t, "refresh-")
		tkCancel.MustExec(fmt.Sprintf("cancel materialized view refresh job %s", jobID))
		select {
		case <-requestedCh:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh cancel watcher to observe request")
		}

		require.NoError(t, failpoint.Disable(pauseFailpoint))
		paused = false

		select {
		case err := <-refreshDone:
			require.Error(t, err)
			require.ErrorContains(t, err, "materialized view task canceled manually")
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh to finish")
		}

		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("failed complete delta apply manual 1"))
		reasonRows := tk.MustQuery(fmt.Sprintf(
			"select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Rows()
		require.Len(t, reasonRows, 1)
		require.Equal(t, "cancelled manually by 'mv_refresh_cancel_u'@'%'", fmt.Sprint(reasonRows[0][0]))
	})

	t.Run("job_not_running", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := newMViewTestKit(t, store)

		err := tk.ExecToErr("cancel materialized view refresh job 1")
		require.ErrorContains(t, err, "cannot cancel materialized view refresh job 1")
		err = tk.ExecToErr("cancel materialized view log purge job 1")
		require.ErrorContains(t, err, "cannot cancel materialized view log purge job 1")
	})

	t.Run("watcher_stops_after_finish", func(t *testing.T) {
		store, _ := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_refresh_cancel_watch_stop (a int not null, b int not null)")
		tk.MustExec("insert into t_refresh_cancel_watch_stop values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t_refresh_cancel_watch_stop (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_refresh_cancel_watch_stop (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_cancel_watch_stop group by a")

		pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
		require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
		defer func() {
			require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
		}()

		var pollCount atomic.Int32
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvTaskMonitorPolled", func(monitorName string) {
			if strings.HasPrefix(monitorName, "refresh-") {
				pollCount.Add(1)
			}
		})

		tk.MustExec("insert into t_refresh_cancel_watch_stop values (2, 3), (3, 4)")
		tk.MustExec("refresh materialized view mv_refresh_cancel_watch_stop complete delta apply")

		require.Greater(t, pollCount.Load(), int32(0))
		countAfterReturn := pollCount.Load()
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, countAfterReturn, pollCount.Load())
	})

	t.Run("watcher_stops_before_shadow", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_oop_cancel_watch (a int not null, b int not null)")
		tk.MustExec("insert into t_oop_cancel_watch values (1, 10), (1, 5), (2, 7)")
		tk.MustExec("create materialized view log on t_oop_cancel_watch (a, b) purge next date_add(now(), interval 1 hour)")
		tk.MustExec("create materialized view mv_oop_cancel_watch (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_oop_cancel_watch group by a")

		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_oop_cancel_watch"))
		require.NoError(t, err)
		mviewID := mvTable.Meta().ID

		pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval"
		require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
		defer func() {
			require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
		}()

		pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning"
		require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
		paused := true
		defer func() {
			if paused {
				require.NoError(t, failpoint.Disable(pauseFailpoint))
			}
		}()

		createShadowHit := false
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterCreateShadow", func() {
			createShadowHit = true
		})

		refreshDone := make(chan error, 1)
		go func() {
			tkRefresh := newMViewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_oop_cancel_watch complete out of place")
		}()

		require.Eventually(t, func() bool {
			rows := tk.MustQuery(fmt.Sprintf(
				"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'",
				mviewID,
			)).Rows()
			return fmt.Sprint(rows[0][0]) == "1"
		}, 10*time.Second, 100*time.Millisecond)

		requester := "'oop_watcher_req'@'stage-c'"
		requestedCh := waitMVTaskCancelWatcherRequested(t, "refresh-")
		tk.MustExec(
			`UPDATE mysql.tidb_mview_refresh_hist
SET CANCEL_REQUEST_TIME = NOW(6),
	CANCEL_REQUESTED_BY = ?
WHERE MVIEW_ID = ?
  AND REFRESH_STATUS = 'running'
  AND CANCEL_REQUEST_TIME IS NULL`,
			requester,
			mviewID,
		)
		select {
		case <-requestedCh:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh cancel watcher to observe request")
		}

		require.NoError(t, failpoint.Disable(pauseFailpoint))
		paused = false

		select {
		case err := <-refreshDone:
			require.Error(t, err)
			require.ErrorContains(t, err, "materialized view task canceled manually")
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for refresh to finish")
		}

		require.False(t, createShadowHit)
		tk.MustQuery(fmt.Sprintf(
			"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Check(testkit.Rows("failed complete out of place manual 1"))
		tk.MustQuery(fmt.Sprintf(
			"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'",
			mviewID,
		)).Check(testkit.Rows("0"))
		reasonRows := tk.MustQuery(fmt.Sprintf(
			"select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
			mviewID,
		)).Rows()
		require.Len(t, reasonRows, 1)
		require.Equal(t, "cancelled manually by "+requester, fmt.Sprint(reasonRows[0][0]))
		tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
	})
}
