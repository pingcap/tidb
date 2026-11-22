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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func newMViewTestKit(t testing.TB, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	return tk
}

func waitMVTaskCancelWatcherRequested(t *testing.T, watchNamePrefix string) <-chan struct{} {
	t.Helper()

	requestedCh := make(chan struct{})
	var requestedChClosed atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvTaskCancelWatcherRequested", func(watchName string) {
		if strings.HasPrefix(watchName, watchNamePrefix) && requestedChClosed.CompareAndSwap(false, true) {
			close(requestedCh)
		}
	})
	return requestedCh
}

func readAffectedRowsMetricValue(t *testing.T, label string) float64 {
	t.Helper()

	counter, err := metrics.AffectedRowsCounter.GetMetricWithLabelValues(label)
	require.NoError(t, err)
	pb := &dto.Metric{}
	require.NoError(t, counter.Write(pb))
	return pb.GetCounter().GetValue()
}

func differentIsolationReadEnginesForTest(current string) string {
	for _, candidate := range []string{
		"tikv",
		"tiflash",
		"tidb",
		"tikv,tidb",
		"tikv,tiflash",
		"tikv,tiflash,tidb",
	} {
		if candidate != current {
			return candidate
		}
	}
	return "tikv"
}

func mustExecRefreshInternal(t *testing.T, tk *testkit.TestKit, sql string) {
	t.Helper()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	vars := tk.Session().GetSessionVars()
	origMaint, origRestr := vars.InMViewMaintenance, vars.InRestrictedSQL
	vars.InMViewMaintenance, vars.InRestrictedSQL = true, true
	defer func() {
		vars.InMViewMaintenance, vars.InRestrictedSQL = origMaint, origRestr
	}()
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

func requireRowsNotContainSubstring(t *testing.T, rows [][]any, substr string) {
	t.Helper()

	for _, row := range rows {
		for _, col := range row {
			require.NotContains(t, fmt.Sprintf("%v", col), substr, "rows=%v", rows)
		}
	}
}

func requireRefreshTiFlashSessionVarsApplied(
	t *testing.T,
	refresh func(),
	expectedMaxThreads int64,
	expectedMaxBytesBeforeExternalJoin int64,
	expectedMaxBytesBeforeExternalGroupBy int64,
	expectedMaxBytesBeforeExternalSort int64,
	expectedMemQuotaQueryPerNode int64,
	expectedQuerySpillRatio float64,
	expectedFineGrainedStreamCount int64,
	expectedFineGrainedBatchSize uint64,
) {
	t.Helper()
	requireTiFlashSessionVarsAppliedWithFailpoint(
		t,
		"github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewTiFlashSessionVarsApplied",
		"github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewTiFlashSpillSessionVarsApplied",
		refresh,
		expectedMaxThreads,
		expectedMaxBytesBeforeExternalJoin,
		expectedMaxBytesBeforeExternalGroupBy,
		expectedMaxBytesBeforeExternalSort,
		expectedMemQuotaQueryPerNode,
		expectedQuerySpillRatio,
		expectedFineGrainedStreamCount,
		expectedFineGrainedBatchSize,
	)
}

func requireTiFlashSessionVarsAppliedWithFailpoint(
	t *testing.T,
	failpointName string,
	spillFailpointName string,
	refresh func(),
	expectedMaxThreads int64,
	expectedMaxBytesBeforeExternalJoin int64,
	expectedMaxBytesBeforeExternalGroupBy int64,
	expectedMaxBytesBeforeExternalSort int64,
	expectedMemQuotaQueryPerNode int64,
	expectedQuerySpillRatio float64,
	expectedFineGrainedStreamCount int64,
	expectedFineGrainedBatchSize uint64,
) {
	t.Helper()

	sessionVarsApplied := false
	var gotMaxThreads int64
	spillVarsApplied := false
	var gotMaxBytesBeforeExternalJoin int64
	var gotMaxBytesBeforeExternalGroupBy int64
	var gotMaxBytesBeforeExternalSort int64
	var gotMemQuotaQueryPerNode int64
	var gotQuerySpillRatio float64
	var gotFineGrainedStreamCount int64
	var gotFineGrainedBatchSize uint64
	testfailpoint.EnableCall(t, failpointName, func(maxThreads int64, fineGrainedStreamCount int64, fineGrainedBatchSize uint64) {
		sessionVarsApplied = true
		gotMaxThreads = maxThreads
		gotFineGrainedStreamCount = fineGrainedStreamCount
		gotFineGrainedBatchSize = fineGrainedBatchSize
	})
	testfailpoint.EnableCall(t, spillFailpointName, func(maxBytesBeforeExternalJoin int64, maxBytesBeforeExternalGroupBy int64, maxBytesBeforeExternalSort int64, memQuotaQueryPerNode int64, querySpillRatio float64) {
		spillVarsApplied = true
		gotMaxBytesBeforeExternalJoin = maxBytesBeforeExternalJoin
		gotMaxBytesBeforeExternalGroupBy = maxBytesBeforeExternalGroupBy
		gotMaxBytesBeforeExternalSort = maxBytesBeforeExternalSort
		gotMemQuotaQueryPerNode = memQuotaQueryPerNode
		gotQuerySpillRatio = querySpillRatio
	})

	refresh()

	require.True(t, sessionVarsApplied)
	require.True(t, spillVarsApplied)
	require.Equal(t, expectedMaxThreads, gotMaxThreads)
	require.Equal(t, expectedMaxBytesBeforeExternalJoin, gotMaxBytesBeforeExternalJoin)
	require.Equal(t, expectedMaxBytesBeforeExternalGroupBy, gotMaxBytesBeforeExternalGroupBy)
	require.Equal(t, expectedMaxBytesBeforeExternalSort, gotMaxBytesBeforeExternalSort)
	require.Equal(t, expectedMemQuotaQueryPerNode, gotMemQuotaQueryPerNode)
	require.Equal(t, expectedQuerySpillRatio, gotQuerySpillRatio)
	require.Equal(t, expectedFineGrainedStreamCount, gotFineGrainedStreamCount)
	require.Equal(t, expectedFineGrainedBatchSize, gotFineGrainedBatchSize)
}

func requireImportSessionVarsAppliedWithFailpoint(
	t *testing.T,
	failpointName string,
	run func(),
	expectedImportThreads int,
	expectedImportDiskQuota string,
) {
	t.Helper()

	applied := false
	gotImportThreads := 0
	gotImportDiskQuota := ""
	testfailpoint.EnableCall(t, failpointName, func(importThreads int, importDiskQuota string) {
		applied = true
		gotImportThreads = importThreads
		gotImportDiskQuota = importDiskQuota
	})

	run()

	require.True(t, applied)
	require.Equal(t, expectedImportThreads, gotImportThreads)
	require.Equal(t, expectedImportDiskQuota, gotImportDiskQuota)
}

func prepareRefreshTiFlashSessionVarsForTest(t *testing.T, tk *testkit.TestKit) {
	t.Helper()

	tk.MustExec(fmt.Sprintf("set @@global.%s = 2", vardef.TiDBMaxTiFlashThreads))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 111", vardef.TiDBMaxBytesBeforeTiFlashExternalJoin))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 222", vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 333", vardef.TiDBMaxBytesBeforeTiFlashExternalSort))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 444", vardef.TiFlashMemQuotaQueryPerNode))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 0.25", vardef.TiFlashQuerySpillRatio))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 4", vardef.TiFlashFineGrainedShuffleStreamCount))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 1024", vardef.TiFlashFineGrainedShuffleBatchSize))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 3", vardef.TiDBMViewMaintainImportThreads))
	tk.MustExec(fmt.Sprintf("set @@global.%s = '32gib'", vardef.TiDBMViewMaintainImportDiskQuota))
	t.Cleanup(func() {
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiDBMaxTiFlashThreads, vardef.DefTiFlashMaxThreads))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiDBMaxBytesBeforeTiFlashExternalJoin, vardef.DefTiFlashMaxBytesBeforeExternalJoin))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy, vardef.DefTiFlashMaxBytesBeforeExternalGroupBy))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiDBMaxBytesBeforeTiFlashExternalSort, vardef.DefTiFlashMaxBytesBeforeExternalSort))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiFlashMemQuotaQueryPerNode, vardef.DefTiFlashMemQuotaQueryPerNode))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %s", vardef.TiFlashQuerySpillRatio, strconv.FormatFloat(vardef.DefTiFlashQuerySpillRatio, 'f', -1, 64)))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiFlashFineGrainedShuffleStreamCount, vardef.DefTiFlashFineGrainedShuffleStreamCount))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiFlashFineGrainedShuffleBatchSize, vardef.DefTiFlashFineGrainedShuffleBatchSize))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", vardef.TiDBMViewMaintainImportThreads, vardef.DefTiDBMViewMaintainImportThreads))
		tk.MustExec(fmt.Sprintf("set @@global.%s = ''", vardef.TiDBMViewMaintainImportDiskQuota))
	})

	tk.MustExec(fmt.Sprintf("set @@session.%s = 8", vardef.TiDBMaxTiFlashThreads))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 101", vardef.TiDBMaxBytesBeforeTiFlashExternalJoin))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 202", vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 303", vardef.TiDBMaxBytesBeforeTiFlashExternalSort))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 404", vardef.TiFlashMemQuotaQueryPerNode))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 0.75", vardef.TiFlashQuerySpillRatio))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 16", vardef.TiFlashFineGrainedShuffleStreamCount))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 4096", vardef.TiFlashFineGrainedShuffleBatchSize))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 12", vardef.TiDBMViewMaintainImportThreads))
	tk.MustExec(fmt.Sprintf("set @@session.%s = '64gib'", vardef.TiDBMViewMaintainImportDiskQuota))
}

func requireCurrentSessionTiFlashSessionVarsRestored(t *testing.T, sessVars *variable.SessionVars) {
	t.Helper()

	require.Equal(t, int64(8), sessVars.TiFlashMaxThreads)
	require.Equal(t, int64(101), sessVars.TiFlashMaxBytesBeforeExternalJoin)
	require.Equal(t, int64(202), sessVars.TiFlashMaxBytesBeforeExternalGroupBy)
	require.Equal(t, int64(303), sessVars.TiFlashMaxBytesBeforeExternalSort)
	require.Equal(t, int64(404), sessVars.TiFlashMaxQueryMemoryPerNode)
	require.Equal(t, float64(0.75), sessVars.TiFlashQuerySpillRatio)
	require.Equal(t, int64(16), sessVars.TiFlashFineGrainedShuffleStreamCount)
	require.Equal(t, uint64(4096), sessVars.TiFlashFineGrainedShuffleBatchSize)
	require.Equal(t, 12, sessVars.MViewMaintainImportThreads)
	require.Equal(t, "64gib", sessVars.MViewMaintainImportDiskQuota)
}

func requireRefreshIsolationReadEnginesApplied(
	t *testing.T,
	failpointName string,
	refresh func(),
	expectedIsolationReadEngines string,
) {
	t.Helper()

	applied := false
	gotIsolationReadEngines := ""
	testfailpoint.EnableCall(t, failpointName, func(currentIsolationReadEngines string, targetIsolationReadEngines string) {
		applied = true
		gotIsolationReadEngines = currentIsolationReadEngines
		require.Equal(t, expectedIsolationReadEngines, targetIsolationReadEngines)
	})

	refresh()

	require.True(t, applied)
	require.Equal(t, expectedIsolationReadEngines, gotIsolationReadEngines)
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
	t.Helper()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	vars := tk.Session().GetSessionVars()
	origMaint, origRestr := vars.InMViewMaintenance, vars.InRestrictedSQL
	vars.InMViewMaintenance, vars.InRestrictedSQL = true, true
	defer func() {
		vars.InMViewMaintenance, vars.InRestrictedSQL = origMaint, origRestr
	}()
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
				Schema: ast.NewCIStr(schema),
				Name:   ast.NewCIStr(view),
			},
			Type:         ast.RefreshMaterializedViewTypeComplete,
			CompleteType: ast.RefreshMaterializedViewCompleteTypeDeltaApply,
		},
	}
}

func mustExecCompleteDeltaApplyImplementStmt(t *testing.T, tk *testkit.TestKit, schema, view string) {
	mustExecRefreshImplementStmt(t, tk, buildCompleteDeltaApplyImplementStmt(schema, view))
}

func refreshMVStatementResultMessage(insertRows, updateRows, deleteRows int64) string {
	return fmt.Sprintf(
		"Rows inserted: %d  Updated: %d  Deleted: %d",
		insertRows,
		updateRows,
		deleteRows,
	)
}

func requireRefreshMVStatementResult(t *testing.T, tk *testkit.TestKit, insertRows, updateRows, deleteRows int64) {
	t.Helper()
	require.Equal(t, uint64(insertRows+updateRows+deleteRows), tk.Session().AffectedRows())
	tk.CheckLastMessage(refreshMVStatementResultMessage(insertRows, updateRows, deleteRows))
}

func readMVServiceRefreshScheduleDurationCount(t *testing.T) uint64 {
	t.Helper()

	pb := &dto.Metric{}
	require.NoError(t, metrics.MVServiceRefreshScheduleDurationHistogram.Write(pb))
	return pb.GetHistogram().GetSampleCount()
}

func setupMaterializedViewRefreshStatementResultTest(t *testing.T) *testkit.TestKit {
	t.Helper()
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_result (a int primary key, b int not null)")
	tk.MustExec("insert into t_mv_refresh_result values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_result (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_result (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_result group by a")
	return tk
}

func makeMaterializedViewRefreshResultStale(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("update t_mv_refresh_result set b = 11 where a = 1")
	tk.MustExec("delete from t_mv_refresh_result where a = 2")
	tk.MustExec("insert into t_mv_refresh_result values (3, 30)")
}

func requireRefreshInfoSnapshotMatchesLatestSuccessHist(t *testing.T, tk *testkit.TestKit, schema, view string) {
	t.Helper()

	histRows := tk.MustQuery(fmt.Sprintf(
		"select REFRESH_READ_TSO, REFRESH_COMMIT_TSO from mysql.tidb_mview_refresh_hist where MVIEW_SCHEMA = '%s' and MVIEW_NAME = '%s' and REFRESH_STATUS = 'success' order by REFRESH_JOB_ID desc limit 1",
		schema,
		view,
	)).Rows()
	require.Len(t, histRows, 1)
	refreshReadTSO, err := strconv.ParseUint(fmt.Sprintf("%v", histRows[0][0]), 10, 64)
	require.NoError(t, err)
	refreshCommitTSO, err := strconv.ParseUint(fmt.Sprintf("%v", histRows[0][1]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, refreshReadTSO)
	require.NotZero(t, refreshCommitTSO)

	// MockTiKV tests need a GC safe point before enabling stale reads.
	mustSetMockGCSafePoint(t, tk, time.Now().Add(-time.Hour))
	tk.MustExec("set @@tidb_snapshot = '" + strconv.FormatUint(refreshCommitTSO, 10) + "'")
	defer tk.MustExec("set @@tidb_snapshot = ''")

	tk.MustQuery(fmt.Sprintf(
		"select r.LAST_SUCCESS_READ_TSO from information_schema.tables t join mysql.tidb_mview_refresh_info r on t.tidb_table_id = r.MVIEW_ID where t.table_schema = '%s' and t.table_name = '%s'",
		schema,
		view,
	)).Check(testkit.Rows(strconv.FormatUint(refreshReadTSO, 10)))
}

func requireRefreshInfoSnapshotMatchesHistAtCommitTSO(
	t *testing.T,
	tk *testkit.TestKit,
	schema string,
	view string,
	refreshCommitTSO uint64,
) {
	t.Helper()

	histRows := tk.MustQuery(fmt.Sprintf(
		"select REFRESH_READ_TSO from mysql.tidb_mview_refresh_hist where MVIEW_SCHEMA = '%s' and MVIEW_NAME = '%s' and REFRESH_STATUS = 'success' and REFRESH_COMMIT_TSO = %d",
		schema,
		view,
		refreshCommitTSO,
	)).Rows()
	require.Len(t, histRows, 1)
	refreshReadTSO, err := strconv.ParseUint(fmt.Sprintf("%v", histRows[0][0]), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, refreshReadTSO)

	// MockTiKV tests need a GC safe point before enabling stale reads.
	mustSetMockGCSafePoint(t, tk, time.Now().Add(-time.Hour))
	tk.MustExec("set @@tidb_snapshot = '" + strconv.FormatUint(refreshCommitTSO, 10) + "'")
	defer tk.MustExec("set @@tidb_snapshot = ''")

	tk.MustQuery(fmt.Sprintf(
		"select r.LAST_SUCCESS_READ_TSO from information_schema.tables t join mysql.tidb_mview_refresh_info r on t.tidb_table_id = r.MVIEW_ID where t.table_schema = '%s' and t.table_name = '%s'",
		schema,
		view,
	)).Check(testkit.Rows(strconv.FormatUint(refreshReadTSO, 10)))
}
