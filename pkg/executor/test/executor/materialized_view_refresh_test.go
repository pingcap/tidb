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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func mustExecInternal(t *testing.T, tk *testkit.TestKit, sql string) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMVMaintenance)
	rs, err := tk.Session().ExecuteInternal(ctx, sql)
	require.NoError(t, err)
	require.Nil(t, rs)
}

func mustSetMockGCSafePoint(t *testing.T, tk *testkit.TestKit, safePoint time.Time) {
	t.Helper()

	safePointName := "tikv_gc_safe_point"
	safePointValue := safePoint.UTC().Format("20060102-15:04:05 -0700")
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
}

func requireRowsContainPrefix(t *testing.T, rows [][]any, prefix string) {
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		if strings.HasPrefix(fmt.Sprintf("%v", row[0]), prefix) {
			return
		}
	}
	require.Failf(t, "prefix not found", "prefix=%s rows=%v", prefix, rows)
}

func requireRowsContainSubstring(t *testing.T, rows [][]any, substr string) {
	t.Helper()

	for _, row := range rows {
		for _, col := range row {
			if strings.Contains(fmt.Sprintf("%v", col), substr) {
				return
			}
		}
	}
	require.Failf(t, "substring not found", "substring=%s rows=%v", substr, rows)
}

func requireRefreshTiFlashSessionVarsApplied(
	t *testing.T,
	refresh func(),
	expectedMaxThreads int64,
	expectedFineGrainedStreamCount int64,
	expectedFineGrainedBatchSize uint64,
) {
	t.Helper()

	applied := false
	var gotMaxThreads int64
	var gotFineGrainedStreamCount int64
	var gotFineGrainedBatchSize uint64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewTiFlashSessionVarsApplied",
		func(maxThreads int64, fineGrainedStreamCount int64, fineGrainedBatchSize uint64) {
			applied = true
			gotMaxThreads = maxThreads
			gotFineGrainedStreamCount = fineGrainedStreamCount
			gotFineGrainedBatchSize = fineGrainedBatchSize
		},
	)

	refresh()

	require.True(t, applied)
	require.Equal(t, expectedMaxThreads, gotMaxThreads)
	require.Equal(t, expectedFineGrainedStreamCount, gotFineGrainedStreamCount)
	require.Equal(t, expectedFineGrainedBatchSize, gotFineGrainedBatchSize)
}

func requireRowsContainAnyPrefix(t *testing.T, rows [][]any, prefixes ...string) {
	for _, prefix := range prefixes {
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			if strings.HasPrefix(fmt.Sprintf("%v", row[0]), prefix) {
				return
			}
		}
	}
	require.Failf(t, "prefixes not found", "prefixes=%v rows=%v", prefixes, rows)
}

func joinRowsAsText(rows [][]any) string {
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		lines = append(lines, fmt.Sprintf("%v", row[0]))
	}
	return strings.Join(lines, "\n")
}

func mustExecRefreshImplementStmt(t *testing.T, tk *testkit.TestKit, stmt *ast.RefreshMaterializedViewImplementStmt) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMVMaintenance)
	rs, err := tk.Session().ExecuteStmt(ctx, stmt)
	if err == nil && rs != nil {
		_, err = sqlexec.DrainRecordSet(ctx, rs, tk.Session().GetSessionVars().MaxChunkSize)
	}
	if rs != nil {
		require.NoError(t, rs.Close())
	}
	require.NoError(t, err)
}

func buildCompleteDeltaApplyImplementStmt(schema, view string) *ast.RefreshMaterializedViewImplementStmt {
	return &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt: &ast.RefreshMaterializedViewStmt{
			ViewName: &ast.TableName{
				Schema: pmodel.NewCIStr(schema),
				Name:   pmodel.NewCIStr(view),
			},
			Type:         ast.RefreshMaterializedViewTypeComplete,
			CompleteType: ast.RefreshMaterializedViewCompleteTypeDeltaApply,
		},
	}
}

func mustExecCompleteDeltaApplyImplementStmt(t *testing.T, tk *testkit.TestKit, schema, view string) {
	mustExecRefreshImplementStmt(t, tk, buildCompleteDeltaApplyImplementStmt(schema, view))
}

func TestMaterializedViewRefreshCompleteBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
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
		Check(testkit.Rows("success complete delta apply manual 1 1 1 1"))
}

func TestProfileMaterializedViewRefreshStepRuntime(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_explain_analyze (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_explain_analyze values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_explain_analyze (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_explain_analyze (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_explain_analyze group by a")

	tk.MustExec("insert into t_mv_explain_analyze values (2, 3), (3, 4)")
	fastRows := tk.MustQuery("refresh materialized view mv_mv_explain_analyze fast with profile").Rows()
	requireRowsContainPrefix(t, fastRows, "[S01 TXN_BEGIN]")
	requireRowsContainPrefix(t, fastRows, "[S04 DATA_CHANGE_FAST_MERGE]")
	requireRowsContainPrefix(t, fastRows, "  MVDeltaMerge")
	requireRowsContainPrefix(t, fastRows, "[S07 FINALIZE_HIST]")

	tk.MustExec("insert into t_mv_explain_analyze values (4, 8)")
	inPlaceRows := tk.MustQuery("refresh materialized view mv_mv_explain_analyze complete in place with profile").Rows()
	requireRowsContainPrefix(t, inPlaceRows, "[S04 DATA_CHANGE_COMPLETE_DELETE]")
	requireRowsContainPrefix(t, inPlaceRows, "[S05 DATA_CHANGE_COMPLETE_INSERT]")
	requireRowsContainPrefix(t, inPlaceRows, "[S08 FINALIZE_HIST]")

	tk.MustExec("update t_mv_explain_analyze set b = 11 where a = 1 and b = 10")
	deltaApplyDryRunRows := tk.MustQuery("refresh materialized view mv_mv_explain_analyze complete delta apply dry run").Rows()
	requireRowsContainPrefix(t, deltaApplyDryRunRows, "[S04 DATA_CHANGE_COMPLETE_DELTA_APPLY]")
	requireRowsContainPrefix(t, deltaApplyDryRunRows, "  MVCompleteDeltaApply")

	deltaApplyProfileRows := tk.MustQuery("refresh materialized view mv_mv_explain_analyze complete delta apply with profile").Rows()
	requireRowsContainPrefix(t, deltaApplyProfileRows, "[S04 DATA_CHANGE_COMPLETE_DELTA_APPLY]")
	requireRowsContainPrefix(t, deltaApplyProfileRows, "  MVCompleteDeltaApply")
	require.Contains(t, joinRowsAsText(deltaApplyProfileRows), "mv_complete_delta_apply:{writer:{")
	require.Contains(t, joinRowsAsText(deltaApplyProfileRows), "rows:{insert:")
	requireRowsContainPrefix(t, deltaApplyProfileRows, "[S07 FINALIZE_HIST]")
}

func TestMaterializedViewRefreshOutOfPlaceObserveLoadShadowPlanUsesBuildSQL(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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

func TestMaterializedViewRefreshUsesMVMaintainMemQuota(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_quota_refresh (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_quota_refresh values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_quota_refresh (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_quota_refresh (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_quota_refresh group by a")
	tk.MustExec("set @@session.tidb_mem_quota_query = 1073741824")
	tk.MustExec("set @@global.tidb_mv_maintain_mem_quota = 536870912")
	tk.MustExec("set @@session.tidb_mv_maintain_mem_quota = 268435456")
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_mv_maintain_mem_quota = %d", 2*1024*1024*1024))

	applied := false
	lastAppliedMemQuotaQuery := int64(0)
	lastAppliedMaintainQuota := int64(0)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnRefreshSession", func(memQuotaQuery int64, maintainMemQuota int64) {
		applied = true
		lastAppliedMemQuotaQuery = memQuotaQuery
		lastAppliedMaintainQuota = maintainMemQuota
	})

	tk.MustExec("refresh materialized view mv_mv_quota_refresh complete")
	require.True(t, applied)
	require.Equal(t, int64(268435456), lastAppliedMaintainQuota)
	require.Equal(t, lastAppliedMaintainQuota, lastAppliedMemQuotaQuery)

	applied = false
	lastAppliedMemQuotaQuery = 0
	lastAppliedMaintainQuota = 0
	mustExecInternal(t, tk, "refresh materialized view mv_mv_quota_refresh complete")
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
	mustExecInternal(t, tk, "refresh materialized view mv_mv_quota_refresh complete out of place")
	require.True(t, buildApplied)
	require.Equal(t, int64(536870912), lastBuildAppliedMaintainQuota)
	require.Equal(t, lastBuildAppliedMaintainQuota, lastBuildAppliedMemQuotaQuery)
}

func TestMaterializedViewRefreshManualSQLUsesCurrentSessionTiFlashSessionVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_tiflash_vars (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_tiflash_vars values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_tiflash_vars (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_tiflash_vars (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_refresh_tiflash_vars group by a")
	tk.MustExec(fmt.Sprintf("set @@global.%s = 2", variable.TiDBMaxTiFlashThreads))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 4", variable.TiFlashFineGrainedShuffleStreamCount))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 1024", variable.TiFlashFineGrainedShuffleBatchSize))
	t.Cleanup(func() {
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiDBMaxTiFlashThreads, variable.DefTiFlashMaxThreads))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiFlashFineGrainedShuffleStreamCount, variable.DefTiFlashFineGrainedShuffleStreamCount))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiFlashFineGrainedShuffleBatchSize, variable.DefTiFlashFineGrainedShuffleBatchSize))
	})

	tk.MustExec(fmt.Sprintf("set @@session.%s = 8", variable.TiDBMaxTiFlashThreads))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 16", variable.TiFlashFineGrainedShuffleStreamCount))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 4096", variable.TiFlashFineGrainedShuffleBatchSize))

	requireRefreshTiFlashSessionVarsApplied(t, func() {
		tk.MustExec("refresh materialized view mv_mv_refresh_tiflash_vars complete")
	}, int64(8), int64(16), uint64(4096))
	tk.MustQuery(fmt.Sprintf(
		"select @@session.%s, @@session.%s, @@session.%s",
		variable.TiDBMaxTiFlashThreads,
		variable.TiFlashFineGrainedShuffleStreamCount,
		variable.TiFlashFineGrainedShuffleBatchSize,
	)).Check(testkit.Rows("8 16 4096"))
}

func TestMaterializedViewRefreshInternalSQLUsesCurrentSessionTiFlashSessionVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_tiflash_vars (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_tiflash_vars values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_tiflash_vars (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_tiflash_vars (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_refresh_tiflash_vars group by a")
	tk.MustExec(fmt.Sprintf("set @@global.%s = 2", variable.TiDBMaxTiFlashThreads))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 4", variable.TiFlashFineGrainedShuffleStreamCount))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 1024", variable.TiFlashFineGrainedShuffleBatchSize))
	t.Cleanup(func() {
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiDBMaxTiFlashThreads, variable.DefTiFlashMaxThreads))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiFlashFineGrainedShuffleStreamCount, variable.DefTiFlashFineGrainedShuffleStreamCount))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiFlashFineGrainedShuffleBatchSize, variable.DefTiFlashFineGrainedShuffleBatchSize))
	})

	tk.MustExec(fmt.Sprintf("set @@session.%s = 8", variable.TiDBMaxTiFlashThreads))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 16", variable.TiFlashFineGrainedShuffleStreamCount))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 4096", variable.TiFlashFineGrainedShuffleBatchSize))

	requireRefreshTiFlashSessionVarsApplied(t, func() {
		mustExecInternal(t, tk, "refresh materialized view mv_mv_refresh_tiflash_vars complete")
	}, int64(8), int64(16), uint64(4096))
	tk.MustQuery(fmt.Sprintf(
		"select @@session.%s, @@session.%s, @@session.%s",
		variable.TiDBMaxTiFlashThreads,
		variable.TiFlashFineGrainedShuffleStreamCount,
		variable.TiFlashFineGrainedShuffleBatchSize,
	)).Check(testkit.Rows("8 16 4096"))
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
	)).Check(testkit.Rows("complete delta apply manual"))

	// Internal SQL refresh should update NEXT_TIME by evaluating RefreshNext.
	mustExecInternal(t, tk, "refresh materialized view mv_internal_next complete")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 20 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete delta apply auto"))
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
	)).Check(testkit.Rows("complete delta apply manual"))

	// Internal SQL refresh should explicitly set NEXT_TIME = NULL when START WITH exists and NEXT is empty.
	mustExecInternal(t, tk, "refresh materialized view mv_internal_start_only complete")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete delta apply auto"))
}

func TestMaterializedViewRefreshInternalSQLNoScheduleSetsNextTimeNull(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_internal_no_schedule (a int not null, b int not null)")
	tk.MustExec("insert into t_internal_no_schedule values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_internal_no_schedule (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_internal_no_schedule (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t_internal_no_schedule group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_internal_no_schedule"))
	require.NoError(t, err)
	require.NotNil(t, mvTable.Meta().MaterializedView)
	// Simulate scheduler metadata state: schedule is fully removed.
	mvTable.Meta().MaterializedView.RefreshStartWith = ""
	mvTable.Meta().MaterializedView.RefreshNext = ""
	mviewID := mvTable.Meta().ID

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_TIME = UTC_TIMESTAMP() + interval 3 hour where MVIEW_ID = %d", mviewID))

	mustExecInternal(t, tk, "refresh materialized view mv_internal_no_schedule complete")
	tk.MustQuery(fmt.Sprintf("select NEXT_TIME is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete delta apply auto"))
}

func TestMaterializedViewRefreshInternalSQLOutOfPlaceUpdatesNextTime(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_internal_oop_next (a int not null, b int not null)")
	tk.MustExec("insert into t_internal_oop_next values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_internal_oop_next (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_internal_oop_next (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t_internal_oop_next group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_internal_oop_next"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_TIME = null where MVIEW_ID = %d", oldMViewID))
	tk.MustExec("insert into t_internal_oop_next values (3, 30)")

	mustExecInternal(t, tk, "refresh materialized view mv_internal_oop_next complete out of place")

	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_internal_oop_next"))
	require.NoError(t, err)
	newMViewID := mvTable.Meta().ID
	require.NotEqual(t, oldMViewID, newMViewID)

	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).
		Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 20 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		newMViewID,
	)).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		oldMViewID,
	)).Check(testkit.Rows("complete out of place auto"))
}

/*
func TestMaterializedViewRefreshFastMethodTracksManualAndAutomatic(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	mustExecInternal(t, tk, "refresh materialized view mv fast")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1", "4 8 1"))
	tk.MustQuery("select REFRESH_METHOD, REFRESH_ROWS > 0 from mysql.tidb_mview_refresh_hist order by REFRESH_JOB_ID desc limit 1").
		Check(testkit.Rows("fast auto 1"))
}
*/

func TestMaterializedViewRefreshFastUpdatesStatsModifyCount(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_fast_stats (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_fast_stats values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_fast_stats (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_fast_stats (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_fast_stats group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_fast_stats"))
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

func TestMaterializedViewRefreshAsOfTimestampRejectsNonFastSyntax(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_asof_nonfast (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_nonfast (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_nonfast (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_nonfast group by a")

	err := tk.ExecToErr("refresh materialized view mv_refresh_asof_nonfast complete as of timestamp '2021-04-15 00:00:00'")
	require.ErrorContains(t, err, "You have an error in your SQL syntax")
}

func TestMaterializedViewRefreshFastAsOfTimestampOuterSemantics(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
		"select REFRESH_STATUS, REFRESH_READ_TSO = %d from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		newerTSO,
		mvID,
	)).Check(testkit.Rows("success 1"))
}

func TestMaterializedViewRefreshFastAsOfTimestampAdvancesWatermarkInWindows(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
}

func TestMaterializedViewRefreshFastAsOfTimestampDryRunUsesRefreshInfoWindow(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
}

func TestMaterializedViewRefreshFastDryRunWithoutAsOfDoesNotRequireTargetTSOValidation(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
}

func TestMaterializedViewRefreshFastAsOfTimestampDryRunRejectsOlderThanLastSuccessTSO(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
}

func TestMaterializedViewRefreshFastAsOfTimestampDryRunRejectsTooOldGCSafePoint(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t_mv_refresh_asof_dry_run_gc (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_asof_dry_run_gc values (1, 10)")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_dry_run_gc (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_dry_run_gc (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_dry_run_gc group by a")

	mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_dry_run_gc'").Rows()
	require.Len(t, mvIDRow, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
	require.NoError(t, err)

	fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	fromTSO := oracle.GoTimeToTS(fromTime)
	targetTime := fromTime.Add(2 * time.Second)
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
		fromTSO,
		mvID,
	))
	mustSetMockGCSafePoint(t, tk, targetTime.Add(time.Second))

	err = tk.QueryToErr(fmt.Sprintf(
		"refresh materialized view mv_refresh_asof_dry_run_gc fast as of timestamp '%s' dry run",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	require.True(t, terror.ErrorEqual(err, variable.ErrSnapshotTooOld), "err %v", err)
}

func TestMaterializedViewRefreshFastAsOfTimestampRejectsTooOldGCSafePoint(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t_mv_refresh_asof_gc (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_asof_gc values (1, 10)")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_gc (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_gc (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_gc group by a")

	mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_gc'").Rows()
	require.Len(t, mvIDRow, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
	require.NoError(t, err)

	fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	fromTSO := oracle.GoTimeToTS(fromTime)
	targetTime := fromTime.Add(2 * time.Second)
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
		fromTSO,
		mvID,
	))
	mustSetMockGCSafePoint(t, tk, targetTime.Add(time.Second))

	histCountBefore := tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mvID,
	)).Rows()[0][0]

	err = tk.ExecToErr(fmt.Sprintf(
		"refresh materialized view mv_refresh_asof_gc fast as of timestamp '%s'",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	require.True(t, terror.ErrorEqual(err, variable.ErrSnapshotTooOld), "err %v", err)
	tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mvID,
	)).Check(testkit.Rows(fmt.Sprintf("%v", histCountBefore)))
}

func TestMaterializedViewRefreshFastAsOfTimestampMinMaxUsesTargetSnapshotData(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")

	tk.MustExec("create table t_mv_refresh_asof_minmax_data (a int not null, b int not null, key idx_a(a))")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_minmax_data (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_minmax_data (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_minmax_data group by a")

	tk.MustExec("insert into t_mv_refresh_asof_minmax_data values (1, 10), (1, 20), (1, 30)")
	tk.MustExec("refresh materialized view mv_refresh_asof_minmax_data complete")
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
}

func TestMaterializedViewRefreshFastAsOfTimestampMinMaxUsesTargetSnapshotSchema(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")

	tk.MustExec("create table t_mv_refresh_asof_minmax_schema (a int not null, b int not null, key idx_ab(a, b))")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_minmax_schema (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_minmax_schema (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_minmax_schema group by a")

	tk.MustExec("insert into t_mv_refresh_asof_minmax_schema values (1, 10), (1, 20), (1, 30)")
	tk.MustExec("refresh materialized view mv_refresh_asof_minmax_schema complete")
	tk.MustQuery("select * from mv_refresh_asof_minmax_schema").Check(testkit.Rows("1 3 30 10"))

	tk.MustExec("delete from t_mv_refresh_asof_minmax_schema where a = 1 and b = 30")
	targetBeforeInvisible := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	sleepUntilBeforeInvisible := time.Until(targetBeforeInvisible.Add(20 * time.Millisecond))
	if sleepUntilBeforeInvisible > 0 {
		time.Sleep(sleepUntilBeforeInvisible)
	}
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

	err := tk.ExecToErr(fmt.Sprintf(
		"refresh materialized view mv_refresh_asof_minmax_schema fast as of timestamp '%s'",
		targetAfterInvisible.Format("2006-01-02 15:04:05.000"),
	))
	require.ErrorContains(t, err, "refresh materialized view fast with MIN/MAX requires base table index whose leading columns cover all GROUP BY columns")
	tk.MustQuery("select * from mv_refresh_asof_minmax_schema").Check(testkit.Rows("1 2 20 10"))
	tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_minmax_schema')").
		Check(testkit.Rows(strconv.FormatUint(targetBeforeInvisibleTSO, 10)))
}

func TestMaterializedViewRefreshFastMinMax(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_mv_fast_minmax (a int not null, b int not null, key idx_a(a))")
	tk.MustExec("create materialized view log on t_mv_fast_minmax (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_fast_minmax (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_mv_fast_minmax group by a")

	tk.MustExec("insert into t_mv_fast_minmax values (1, 10), (1, 20), (1, 30), (2, 5), (2, 8)")
	tk.MustExec("refresh materialized view mv_fast_minmax complete")
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
}

func TestMaterializedViewRefreshFastMinMaxRequiresSupportingIndex(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_mv_fast_minmax_noidx (id int not null primary key, a int not null, b int not null, key idx_a(a))")
	tk.MustExec("create materialized view log on t_mv_fast_minmax_noidx (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_fast_minmax_noidx (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_mv_fast_minmax_noidx group by a")

	tk.MustExec("insert into t_mv_fast_minmax_noidx values (1, 1, 10), (2, 1, 20)")
	tk.MustExec("refresh materialized view mv_fast_minmax_noidx complete")

	tk.MustExec("alter table t_mv_fast_minmax_noidx alter index idx_a invisible")
	err := tk.ExecToErr("refresh materialized view mv_fast_minmax_noidx fast")
	require.ErrorContains(t, err, "refresh materialized view fast with MIN/MAX requires base table index whose leading columns cover all GROUP BY columns")
}

func TestMaterializedViewRefreshFastNullableAggregatesWithDuplicateCountExpr(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_mv_fast_nullable_dup_count (a int not null, b int, key idx_ab(a, b))")
	tk.MustExec("create materialized view log on t_mv_fast_nullable_dup_count (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec(`create materialized view mv_fast_nullable_dup_count (a, cnt, cnt_b1, cnt_b2, s, mx, mn)
		refresh fast as
		select a, count(1), count(b) as cnt_b1, count(b) as cnt_b2, sum(b), max(b), min(b)
		from t_mv_fast_nullable_dup_count
		group by a`)

	tk.MustExec("insert into t_mv_fast_nullable_dup_count values (1, 10), (1, null), (1, 5), (2, null)")
	tk.MustExec("refresh materialized view mv_fast_nullable_dup_count complete")
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
}

func TestMaterializedViewRefreshFastMinMaxWhereSeparateIndexes(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	tk.MustExec(`create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as
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
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

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
		Check(testkit.Rows("success complete delta apply manual 1 1 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'", mviewID)).
		Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshFinalizeSuccessFailureWithCleanupErrorDoesNotRewriteFailedHist(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
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

	err = tk.ExecToErr("refresh materialized view mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "advisory lock cleanup invariant violated")

	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_ENDTIME is null, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.Rows("running 1 1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'failed'", mviewID)).
		Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshCompleteRunningHistLifecycle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

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
		Check(testkit.Rows("running complete delta apply manual 1 1 1"))

	close(pauseCh)

	select {
	case err := <-refreshDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to finish")
	}

	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.Rows("success complete delta apply manual 1 1 1"))
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
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
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

func TestMaterializedViewRefreshFastStmtRetryReadTSOSnapshotMatchesMV(t *testing.T) {
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

	// Make MV stale before refresh starts.
	tk.MustExec("insert into t values (1, 1)")

	const (
		afterInsertHistRunningFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInsertRefreshHistRunning"
		beforeRetryTSFailpoint          = "github.com/pingcap/tidb/pkg/executor/beforePessimisticStmtErrorForNextAction"
		writeConflictFailpoint          = "github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/pessimisticLockReturnWriteConflict"
	)

	preMergeResumeCh := make(chan struct{})
	preMergeReadyCh := make(chan struct{})
	require.NoError(t, failpoint.EnableCall(afterInsertHistRunningFailpoint, func() {
		select {
		case <-preMergeReadyCh:
		default:
			close(preMergeReadyCh)
		}
		<-preMergeResumeCh
	}))
	defer func() {
		select {
		case <-preMergeResumeCh:
		default:
			close(preMergeResumeCh)
		}
		require.NoError(t, failpoint.Disable(afterInsertHistRunningFailpoint))
	}()

	retryResumeCh := make(chan struct{})
	retryReadyCh := make(chan struct{})
	require.NoError(t, failpoint.EnableCall(beforeRetryTSFailpoint, func() {
		select {
		case <-retryReadyCh:
		default:
			close(retryReadyCh)
		}
		<-retryResumeCh
	}))
	defer func() {
		select {
		case <-retryResumeCh:
		default:
			close(retryResumeCh)
		}
		require.NoError(t, failpoint.Disable(beforeRetryTSFailpoint))
	}()

	refreshDone := make(chan error, 1)
	go func() {
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv fast")
	}()

	select {
	case <-preMergeReadyCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to reach fast-merge prelude")
	}

	require.NoError(t, failpoint.Enable(writeConflictFailpoint, "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(writeConflictFailpoint))
	}()
	close(preMergeResumeCh)

	select {
	case <-retryReadyCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh fast merge to enter statement retry")
	}

	// This base-table change commits after the first attempt fails but before the retry chooses its new for_update_ts.
	tkConcurrent := testkit.NewTestKit(t, store)
	tkConcurrent.MustExec("use test")
	tkConcurrent.MustExec("insert into t values (2, 7)")

	close(retryResumeCh)

	select {
	case err := <-refreshDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh fast merge to finish")
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

	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", refreshReadTSO, mviewID)).
		Check(testkit.Rows("success fast manually 1 1 1"))
}

func TestMaterializedViewRefreshCompleteRefreshInfoCASUpdateAfterConcurrentPreUpdate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
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
		Check(testkit.Rows("success complete delta apply manual 1 1 1"))
}

func TestMaterializedViewRefreshCompleteConcurrentNowait(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

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
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID))
	err = tk.ExecToErr("refresh materialized view mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
}

func TestMaterializedViewRefreshWithAsyncModeComplete(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	err := tk.ExecToErr("refresh materialized view mv with async mode complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "WITH ASYNC MODE is not supported yet")
}

func TestMaterializedViewRefreshCompleteDeltaApply(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
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
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mviewID)).
		Check(testkit.RowsWithSep("|", "success|complete delta apply manual|1|1|1|1"))

	tk.MustExec("insert into t values (4, 8)")
	mustExecInternal(t, tk, "refresh materialized view mv complete delta apply")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1", "4 8 1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.RowsWithSep("|", "complete delta apply auto"))
}

func TestMaterializedViewRefreshCompleteInPlace(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv complete in place")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.RowsWithSep("|", "success|complete in place manual|1|1|1|1"))
}

func TestMaterializedViewRefreshCompleteDeltaApplyImplementStmt(t *testing.T) {
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
			tk := testkit.NewTestKit(t, store)
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
}

func TestMaterializedViewRefreshCompleteDeltaApplyImplementStmtUsesForUpdateTS(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2"))

	tkRefresh := testkit.NewTestKit(t, store)
	tkRefresh.MustExec("use test")
	tkRefresh.MustExec("begin pessimistic")

	tkConcurrent := testkit.NewTestKit(t, store)
	tkConcurrent.MustExec("use test")
	tkConcurrent.MustExec("insert into t values (2, 7)")

	mustExecCompleteDeltaApplyImplementStmt(t, tkRefresh, "test", "mv")
	tkRefresh.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
	tkRefresh.MustExec("commit")

	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestMaterializedViewRefreshCompleteDeltaApplyRollbackOnError(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
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

	tk.MustExec("create unique index idx_unique_s on mv (s)")
	tk.MustExec("insert into t values (2, 8)")

	err = tk.ExecToErr("refresh materialized view mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "Duplicate")

	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO = %d from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldTS, mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.RowsWithSep("|", "failed|complete delta apply manual|1|1|1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'", mviewID)).
		Check(testkit.Rows("0"))
	reasonRow := tk.MustQuery(fmt.Sprintf("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).Rows()
	require.Len(t, reasonRow, 1)
	require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "Duplicate")
}

func TestMaterializedViewRefreshEarlyFailureWritesHist(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
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

	err = tk.ExecToErr("refresh materialized view mv complete")
	require.Error(t, err)
	require.ErrorContains(t, err, "mock early refresh failure")

	tk.MustQuery(fmt.Sprintf(
		"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows(fmt.Sprintf("%d", histCountBeforeVal+1)))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
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
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv complete out of place")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))

	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	newMViewID := mvTable.Meta().ID
	require.NotEqual(t, oldMViewID, newMViewID)

	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Contains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, newMViewID)
	require.NotContains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, oldMViewID)

	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).
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

func TestMaterializedViewRefreshCompleteOutOfPlaceShadowTableProtected(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
		tkRefresh := testkit.NewTestKit(t, store)
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

	err := tk.ExecToErr(fmt.Sprintf("insert into `%s` values (9, 9, 9)", shadowTableName))
	require.ErrorContains(t, err, "not updatable")
	err = tk.ExecToErr(fmt.Sprintf("alter table `%s` add column x int", shadowTableName))
	require.ErrorContains(t, err, "ALTER TABLE on materialized view shadow table")

	require.NoError(t, failpoint.Disable(pauseCreateShadowFailpoint))
	enabled = false
	require.NoError(t, <-refreshDone)

	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
	tk.MustQuery("select a, s, cnt from mv_shadow_guard order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverWithUnsignedBuildReadTSO(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
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
	mvTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
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
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	mviewIDRows := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv'").Rows()
	require.Len(t, mviewIDRows, 1)
	mviewID, err := strconv.ParseInt(fmt.Sprintf("%v", mviewIDRows[0][0]), 10, 64)
	require.NoError(t, err)
	tk.MustExec("create unique index idx_unique_s on mv (s)")
	tk.MustExec("insert into t values (2, 8)")

	err = tk.ExecToErr("refresh materialized view mv complete out of place")
	require.Error(t, err)
	require.ErrorContains(t, err, "Duplicate")
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("failed complete out of place manual 1 1"))
	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
	// Out-of-place build failure should not modify old MV serving table.
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCancelWatcherStopsBeforeCreateShadow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_oop_cancel_watch (a int not null, b int not null)")
	tk.MustExec("insert into t_oop_cancel_watch values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_oop_cancel_watch (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_oop_cancel_watch (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_oop_cancel_watch group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_oop_cancel_watch"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskCancelWatchPollInterval"
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
		tkRefresh := testkit.NewTestKit(t, store)
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
	tk.MustExec(
		`UPDATE mysql.tidb_mview_refresh_hist
SET CANCEL_REQUESTED_AT = NOW(6),
	CANCEL_REQUESTED_BY = ?
WHERE MVIEW_ID = ?
  AND REFRESH_STATUS = 'running'
  AND CANCEL_REQUESTED_AT IS NULL`,
		requester,
		mviewID,
	)
	time.Sleep(300 * time.Millisecond)

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
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
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
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverCASMismatch(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	tkConcurrent := testkit.NewTestKit(t, store)
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
	mvTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
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
}

func TestMaterializedViewRefreshCompleteFailureKeepsRefreshInfoReadTSO(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
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
		Check(testkit.Rows("failed complete delta apply manual 1 1 1"))
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
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	// Turn off in-place constraint check for pessimistic txn.
	// Refresh should still succeed.
	tk.MustExec("set session tidb_constraint_check_in_place_pessimistic = off")
	tk.MustExec("insert into t values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv complete in place")
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
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
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

func TestMaterializedViewRefreshCancelWatcherUsesHistRequest(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_refresh_cancel_watch (a int not null, b int not null)")
	tk.MustExec("insert into t_refresh_cancel_watch values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_refresh_cancel_watch (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_cancel_watch (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_cancel_watch group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_refresh_cancel_watch"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskCancelWatchPollInterval"
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
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_refresh_cancel_watch complete")
	}()

	require.Eventually(t, func() bool {
		rows := tk.MustQuery(fmt.Sprintf(
			"select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'",
			mviewID,
		)).Rows()
		return fmt.Sprint(rows[0][0]) == "1"
	}, 10*time.Second, 100*time.Millisecond)

	requester := "'refresh_watcher_req'@'stage-c'"
	tk.MustExec(
		`UPDATE mysql.tidb_mview_refresh_hist
SET CANCEL_REQUESTED_AT = NOW(6),
	CANCEL_REQUESTED_BY = ?
WHERE MVIEW_ID = ?
  AND REFRESH_STATUS = 'running'
  AND CANCEL_REQUESTED_AT IS NULL`,
		requester,
		mviewID,
	)
	time.Sleep(300 * time.Millisecond)

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
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
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
}

func TestCancelMaterializedViewRefreshJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_refresh_cancel_job (a int not null, b int not null)")
	tk.MustExec("insert into t_refresh_cancel_job values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_refresh_cancel_job (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_cancel_job (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_cancel_job group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_refresh_cancel_job"))
	require.NoError(t, err)
	mviewID := mvTable.Meta().ID

	pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskCancelWatchPollInterval"
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
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_refresh_cancel_job complete")
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

	tkCancel := testkit.NewTestKit(t, store)
	require.NoError(t, tkCancel.Session().Auth(&auth.UserIdentity{Username: "mv_refresh_cancel_u", Hostname: "%"}, nil, nil, nil))
	tkCancel.MustGetErrCode(fmt.Sprintf("cancel materialized view refresh job %s", jobID), errno.ErrTableaccessDenied)
	tk.MustExec("grant alter on test.mv_refresh_cancel_job to 'mv_refresh_cancel_u'@'%'")
	tkCancel.MustExec(fmt.Sprintf("cancel materialized view refresh job %s", jobID))
	time.Sleep(300 * time.Millisecond)

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
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_ENDTIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("failed complete delta apply manual 1"))
	reasonRows := tk.MustQuery(fmt.Sprintf(
		"select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Rows()
	require.Len(t, reasonRows, 1)
	require.Equal(t, "cancelled manually by 'mv_refresh_cancel_u'@'%'", fmt.Sprint(reasonRows[0][0]))
}

func TestCancelMaterializedViewRefreshJobNotRunning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	err := tk.ExecToErr("cancel materialized view refresh job 1")
	require.ErrorContains(t, err, "cannot cancel materialized view refresh job 1: job not running, not found, or cancel already requested")
}

func TestMaterializedViewRefreshCancelWatcherStopsAfterTaskFinish(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_refresh_cancel_watch_stop (a int not null, b int not null)")
	tk.MustExec("insert into t_refresh_cancel_watch_stop values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_refresh_cancel_watch_stop (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_cancel_watch_stop (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_cancel_watch_stop group by a")

	pollIntervalFailpoint := "github.com/pingcap/tidb/pkg/executor/mockMVTaskCancelWatchPollInterval"
	require.NoError(t, failpoint.Enable(pollIntervalFailpoint, "return(50)"))
	defer func() {
		require.NoError(t, failpoint.Disable(pollIntervalFailpoint))
	}()

	var pollCount atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvTaskCancelWatcherPolled", func(watchName string) {
		if strings.HasPrefix(watchName, "refresh-") {
			pollCount.Add(1)
		}
	})

	tk.MustExec("insert into t_refresh_cancel_watch_stop values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv_refresh_cancel_watch_stop complete")

	require.Greater(t, pollCount.Load(), int32(0))
	countAfterReturn := pollCount.Load()
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, countAfterReturn, pollCount.Load())
}
