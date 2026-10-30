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
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	dto "github.com/prometheus/client_model/go"
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

func nextCompareTimestamp(t *testing.T, tk *testkit.TestKit) string {
	t.Helper()

	mustSetMockGCSafePoint(t, tk, time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC))
	// AS OF timestamps have no TSO logical component; move past the last commit millisecond.
	time.Sleep(10 * time.Millisecond)
	ts := fmt.Sprint(tk.MustQuery("select now(6)").Rows()[0][0])
	time.Sleep(10 * time.Millisecond)
	return ts
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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewTiFlashSessionVarsApplied",
		func(maxThreads int64, fineGrainedStreamCount int64, fineGrainedBatchSize uint64) {
			sessionVarsApplied = true
			gotMaxThreads = maxThreads
			gotFineGrainedStreamCount = fineGrainedStreamCount
			gotFineGrainedBatchSize = fineGrainedBatchSize
		},
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewTiFlashSpillSessionVarsApplied",
		func(maxBytesBeforeExternalJoin int64, maxBytesBeforeExternalGroupBy int64, maxBytesBeforeExternalSort int64, memQuotaQueryPerNode int64, querySpillRatio float64) {
			spillVarsApplied = true
			gotMaxBytesBeforeExternalJoin = maxBytesBeforeExternalJoin
			gotMaxBytesBeforeExternalGroupBy = maxBytesBeforeExternalGroupBy
			gotMaxBytesBeforeExternalSort = maxBytesBeforeExternalSort
			gotMemQuotaQueryPerNode = memQuotaQueryPerNode
			gotQuerySpillRatio = querySpillRatio
		},
	)

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

	tk.MustExec(fmt.Sprintf("set @@global.%s = 2", variable.TiDBMaxTiFlashThreads))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 111", variable.TiDBMaxBytesBeforeTiFlashExternalJoin))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 222", variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 333", variable.TiDBMaxBytesBeforeTiFlashExternalSort))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 444", variable.TiFlashMemQuotaQueryPerNode))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 0.25", variable.TiFlashQuerySpillRatio))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 4", variable.TiFlashFineGrainedShuffleStreamCount))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 1024", variable.TiFlashFineGrainedShuffleBatchSize))
	tk.MustExec(fmt.Sprintf("set @@global.%s = 3", variable.TiDBMViewMaintainImportThreads))
	tk.MustExec(fmt.Sprintf("set @@global.%s = '32gib'", variable.TiDBMViewMaintainImportDiskQuota))
	t.Cleanup(func() {
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiDBMaxTiFlashThreads, variable.DefTiFlashMaxThreads))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiDBMaxBytesBeforeTiFlashExternalJoin, variable.DefTiFlashMaxBytesBeforeExternalJoin))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy, variable.DefTiFlashMaxBytesBeforeExternalGroupBy))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiDBMaxBytesBeforeTiFlashExternalSort, variable.DefTiFlashMaxBytesBeforeExternalSort))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiFlashMemQuotaQueryPerNode, variable.DefTiFlashMemQuotaQueryPerNode))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %s", variable.TiFlashQuerySpillRatio, strconv.FormatFloat(variable.DefTiFlashQuerySpillRatio, 'f', -1, 64)))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiFlashFineGrainedShuffleStreamCount, variable.DefTiFlashFineGrainedShuffleStreamCount))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiFlashFineGrainedShuffleBatchSize, variable.DefTiFlashFineGrainedShuffleBatchSize))
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiDBMViewMaintainImportThreads, variable.DefTiDBMViewMaintainImportThreads))
		tk.MustExec(fmt.Sprintf("set @@global.%s = ''", variable.TiDBMViewMaintainImportDiskQuota))
	})

	tk.MustExec(fmt.Sprintf("set @@session.%s = 8", variable.TiDBMaxTiFlashThreads))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 101", variable.TiDBMaxBytesBeforeTiFlashExternalJoin))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 202", variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 303", variable.TiDBMaxBytesBeforeTiFlashExternalSort))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 404", variable.TiFlashMemQuotaQueryPerNode))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 0.75", variable.TiFlashQuerySpillRatio))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 16", variable.TiFlashFineGrainedShuffleStreamCount))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 4096", variable.TiFlashFineGrainedShuffleBatchSize))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 12", variable.TiDBMViewMaintainImportThreads))
	tk.MustExec(fmt.Sprintf("set @@session.%s = '64gib'", variable.TiDBMViewMaintainImportDiskQuota))
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

func readAffectedRowsMetricValue(t *testing.T, label string) float64 {
	t.Helper()

	counter, err := metrics.AffectedRowsCounter.GetMetricWithLabelValues(label)
	require.NoError(t, err)
	pb := &dto.Metric{}
	require.NoError(t, counter.Write(pb))
	return pb.GetCounter().GetValue()
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
	tk := testkit.NewTestKit(t, store)
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

func TestMaterializedViewRefreshFastStatementResult(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	makeMaterializedViewRefreshResultStale(t, tk)
	beforeRefreshMV := readAffectedRowsMetricValue(t, "RefreshMV")

	tk.MustExec("refresh materialized view mv_refresh_result fast")
	requireRefreshMVStatementResult(t, tk, 1, 1, 1)
	require.Equal(t, 3.0, readAffectedRowsMetricValue(t, "RefreshMV")-beforeRefreshMV)
	tk.MustQuery("select * from mv_refresh_result order by a").Check(testkit.Rows("1 11 1", "3 30 1"))
}

func TestMaterializedViewRefreshFastWritesMLog(t *testing.T) {
	testMaterializedViewRefreshWritesMLog(t, "fast")
}

func TestMaterializedViewRefreshCompleteDeltaApplyWritesMLog(t *testing.T) {
	testMaterializedViewRefreshWritesMLog(t, "complete delta apply")
}

func TestNestedMaterializedViewFastRefresh(t *testing.T) {
	for _, parentRefreshMethod := range []string{"fast", "complete delta apply"} {
		t.Run(parentRefreshMethod, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_nested_mv_refresh (a int not null, b int not null, v int)")
			tk.MustExec("insert into t_nested_mv_refresh values (1, 1, 10), (1, 1, 5), (1, 2, 7), (2, 1, 4)")
			tk.MustExec("create materialized view log on t_nested_mv_refresh (a, b, v)")
			tk.MustExec("create materialized view mv_nested_parent (a, b, s, cnt, cnt_v) refresh fast as select a, b, sum(v), count(1), count(v) from t_nested_mv_refresh group by a, b")
			tk.MustExec("create materialized view log on mv_nested_parent (a, b, s, cnt)")
			tk.MustExec("create materialized view mv_nested_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(1), count(s) from mv_nested_parent group by a")

			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 22 2 2",
				"2 4 1 1",
			))

			// These mutations make the parent update one group, delete another, and insert a third.
			tk.MustExec("update t_nested_mv_refresh set v = 11 where a = 1 and b = 1 and v = 10")
			tk.MustExec("delete from t_nested_mv_refresh where a = 1 and b = 2")
			tk.MustExec("insert into t_nested_mv_refresh values (3, 1, 8)")
			// A child refresh only consumes published parent deltas; it does not refresh its parent.
			tk.MustExec("refresh materialized view mv_nested_child fast")
			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 22 2 2", "2 4 1 1",
			))
			tk.MustExec("refresh materialized view mv_nested_parent " + parentRefreshMethod)
			tk.MustExec("refresh materialized view mv_nested_child fast")

			tk.MustQuery("select a, b, s, cnt from mv_nested_parent order by a, b").Check(testkit.Rows(
				"1 1 16 2",
				"2 1 4 1",
				"3 1 8 1",
			))
			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 16 1 1",
				"2 4 1 1",
				"3 8 1 1",
			))

			// Moving a parent group and making all SUM inputs NULL exercises both group deletion
			// and nullable aggregate transitions in the child's delta stream.
			tk.MustExec("update t_nested_mv_refresh set a = 1, b = 3, v = null where a = 3 and b = 1")
			tk.MustExec("update t_nested_mv_refresh set v = null where a = 1 and b = 1")
			tk.MustExec("refresh materialized view mv_nested_parent " + parentRefreshMethod)
			tk.MustExec("refresh materialized view mv_nested_child fast")
			tk.MustQuery("select a, b, s, cnt, cnt_v from mv_nested_parent order by a, b").Check(testkit.Rows(
				"1 1 <nil> 2 0", "1 3 <nil> 1 0", "2 1 4 1 1",
			))
			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 <nil> 2 0", "2 4 1 1",
			))

			tk.MustExec("update t_nested_mv_refresh set v = 9 where a = 1 and b = 3")
			tk.MustExec("refresh materialized view mv_nested_parent " + parentRefreshMethod)
			tk.MustExec("refresh materialized view mv_nested_child fast")
			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 9 2 1", "2 4 1 1",
			))
		})
	}
}

func TestNestedMaterializedViewFastRefreshMinMax(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_nested_mv_minmax (a int not null, b int not null, v int not null, key idx_ab (a, b))")
	tk.MustExec("insert into t_nested_mv_minmax values (1, 1, 10), (1, 1, 30), (1, 2, 5), (2, 1, 8)")
	tk.MustExec("create materialized view log on t_nested_mv_minmax (a, b, v)")
	tk.MustExec("create materialized view mv_nested_parent_minmax (a, b, cnt, mx, mn) refresh fast as select a, b, count(1), max(v), min(v) from t_nested_mv_minmax group by a, b")
	tk.MustExec("create materialized view log on mv_nested_parent_minmax (a, b, cnt, mx, mn)")
	tk.MustExec("create materialized view mv_nested_child_minmax (a, cnt, mx, mn) refresh fast as select a, count(1), max(mx), min(mn) from mv_nested_parent_minmax group by a")

	tk.MustQuery("select a, cnt, mx, mn from mv_nested_child_minmax order by a").Check(testkit.Rows(
		"1 2 30 5",
		"2 1 8 8",
	))

	// Removing the parent group's MAX forces the child MAX fallback to probe the parent MV by its
	// group-key index, rather than treating the source as a physical base table.
	tk.MustExec("delete from t_nested_mv_minmax where a = 1 and b = 1 and v = 30")
	tk.MustExec("refresh materialized view mv_nested_parent_minmax fast")
	tk.MustExec("refresh materialized view mv_nested_child_minmax fast")

	tk.MustQuery("select a, b, cnt, mx, mn from mv_nested_parent_minmax order by a, b").Check(testkit.Rows(
		"1 1 1 10 10",
		"1 2 1 5 5",
		"2 1 1 8 8",
	))
	tk.MustQuery("select a, cnt, mx, mn from mv_nested_child_minmax order by a").Check(testkit.Rows(
		"1 2 10 5",
		"2 1 8 8",
	))
}

func testMaterializedViewRefreshWritesMLog(t *testing.T, refreshMethod string) {
	t.Helper()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_mlog (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_mlog values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_refresh_mlog (a, b)")
	tk.MustExec("create materialized view mv_refresh_mlog (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog group by a")
	tk.MustExec("create materialized view log on mv_refresh_mlog (a, s, cnt)")

	// The three base-table mutations make the parent MV update one group, delete one group,
	// and insert one group. The parent MLog must retain the corresponding logical operations.
	tk.MustExec("update t_mv_refresh_mlog set b = 11 where a = 1 and b = 10")
	tk.MustExec("delete from t_mv_refresh_mlog where a = 2")
	tk.MustExec("insert into t_mv_refresh_mlog values (3, 4)")
	tk.MustExec("refresh materialized view mv_refresh_mlog " + refreshMethod)

	tk.MustQuery("select a, s, cnt from mv_refresh_mlog order by a").Check(testkit.Rows(
		"1 16 2",
		"3 4 1",
	))
	tk.MustQuery("select a, s, cnt, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$mv_refresh_mlog` order by a, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`").Check(testkit.Rows(
		"1 15 2 U -1",
		"1 16 2 U 1",
		"2 7 1 D -1",
		"3 4 1 I 1",
	))

	// A refresh with no source changes must not append a MLog row.
	tk.MustExec("refresh materialized view mv_refresh_mlog " + refreshMethod)
	tk.MustQuery("select count(*) from `$mlog$mv_refresh_mlog`").Check(testkit.Rows("4"))
}

func TestMaterializedViewRefreshSkipsMLogWhenTrackedColumnsAreUnchanged(t *testing.T) {
	for _, refreshMethod := range []string{"fast", "complete delta apply"} {
		t.Run(refreshMethod, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mv_refresh_mlog_tracked (a int not null, b int not null)")
			tk.MustExec("insert into t_mv_refresh_mlog_tracked values (1, 10), (1, 5)")
			tk.MustExec("create materialized view log on t_mv_refresh_mlog_tracked (a, b)")
			tk.MustExec("create materialized view mv_refresh_mlog_tracked (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog_tracked group by a")
			tk.MustExec("create materialized view log on mv_refresh_mlog_tracked (a)")

			tk.MustExec("update t_mv_refresh_mlog_tracked set b = 11 where a = 1 and b = 10")
			tk.MustExec("refresh materialized view mv_refresh_mlog_tracked " + refreshMethod)

			tk.MustQuery("select a, s, cnt from mv_refresh_mlog_tracked").Check(testkit.Rows("1 16 2"))
			tk.MustQuery("select * from `$mlog$mv_refresh_mlog_tracked`").Check(testkit.Rows())
		})
	}
}

func TestMaterializedViewRefreshMLogWriteFailureRollsBack(t *testing.T) {
	for _, refreshMethod := range []string{"fast", "complete delta apply"} {
		t.Run(refreshMethod, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mv_refresh_mlog_rollback (a int not null, b int not null)")
			tk.MustExec("insert into t_mv_refresh_mlog_rollback values (1, 10), (1, 5)")
			tk.MustExec("create materialized view log on t_mv_refresh_mlog_rollback (a, b)")
			tk.MustExec("create materialized view mv_refresh_mlog_rollback (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog_rollback group by a")
			tk.MustExec("create materialized view log on mv_refresh_mlog_rollback (a, s, cnt)")
			tk.MustExec("update t_mv_refresh_mlog_rollback set b = 11 where a = 1 and b = 10")

			const failpointName = "github.com/pingcap/tidb/pkg/table/tables/mockMLogWriteError"
			require.NoError(t, failpoint.Enable(failpointName, `return("mock mlog write failure")`))
			defer func() {
				require.NoError(t, failpoint.Disable(failpointName))
			}()

			err := tk.ExecToErr("refresh materialized view mv_refresh_mlog_rollback " + refreshMethod)
			require.ErrorContains(t, err, "mock mlog write failure")
			tk.MustQuery("select a, s, cnt from mv_refresh_mlog_rollback").Check(testkit.Rows("1 15 2"))
			tk.MustQuery("select * from `$mlog$mv_refresh_mlog_rollback`").Check(testkit.Rows())
		})
	}
}

func TestMaterializedViewRefreshFastCommitTSOSnapshotMatchesRefreshInfo(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	makeMaterializedViewRefreshResultStale(t, tk)

	tk.MustExec("refresh materialized view mv_refresh_result fast")
	requireRefreshInfoSnapshotMatchesLatestSuccessHist(t, tk, "test", "mv_refresh_result")
}

func TestMaterializedViewRefreshFastAsOfTimestampStatementResult(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	tk.MustExec("set time_zone = '+00:00'")
	makeMaterializedViewRefreshResultStale(t, tk)
	targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
	if sleepUntilTarget > 0 {
		time.Sleep(sleepUntilTarget)
	}
	mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))

	tk.MustExec(fmt.Sprintf(
		"refresh materialized view mv_refresh_result fast as of timestamp '%s'",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	requireRefreshMVStatementResult(t, tk, 1, 1, 1)
	tk.MustQuery("select * from mv_refresh_result order by a").Check(testkit.Rows("1 11 1", "3 30 1"))
}

func TestMaterializedViewRefreshFastAsOfTimestampCommitTSOSnapshotMatchesRefreshInfo(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	tk.MustExec("set time_zone = '+00:00'")
	makeMaterializedViewRefreshResultStale(t, tk)
	targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
	if sleepUntilTarget > 0 {
		time.Sleep(sleepUntilTarget)
	}
	mustSetMockGCSafePoint(t, tk, targetTime.Add(-time.Hour))

	tk.MustExec(fmt.Sprintf(
		"refresh materialized view mv_refresh_result fast as of timestamp '%s'",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	requireRefreshInfoSnapshotMatchesLatestSuccessHist(t, tk, "test", "mv_refresh_result")
}

func TestMaterializedViewRefreshFastAsOfTimestampNoOpStatementResult(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	// An empty interval does not read input logs or advance the watermark.
	tk.MustExec(fmt.Sprintf("insert into mysql.tidb_mview_fast_refresh_source_info values (%d, 0)", mvID))
	tk.MustExec(fmt.Sprintf(
		"refresh materialized view mv_refresh_result_noop fast as of timestamp '%s'",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	requireRefreshMVStatementResult(t, tk, 0, 0, 0)
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvID)).
		Check(testkit.Rows(fmt.Sprint(targetTSO)))
}

func TestMaterializedViewRefreshCompleteDeltaApplyStatementResult(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	makeMaterializedViewRefreshResultStale(t, tk)

	tk.MustExec("refresh materialized view mv_refresh_result complete delta apply")
	requireRefreshMVStatementResult(t, tk, 1, 1, 1)
	tk.MustQuery("select * from mv_refresh_result order by a").Check(testkit.Rows("1 11 1", "3 30 1"))
}

func TestMaterializedViewRefreshCompleteDeltaApplyCommitTSOSnapshotMatchesRefreshInfo(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	makeMaterializedViewRefreshResultStale(t, tk)

	tk.MustExec("refresh materialized view mv_refresh_result complete delta apply")
	requireRefreshInfoSnapshotMatchesLatestSuccessHist(t, tk, "test", "mv_refresh_result")
}

func TestMaterializedViewRefreshCompleteInPlaceStatementResult(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	makeMaterializedViewRefreshResultStale(t, tk)

	tk.MustExec("refresh materialized view mv_refresh_result complete in place")
	requireRefreshMVStatementResult(t, tk, 2, 0, 2)
	tk.MustQuery("select * from mv_refresh_result order by a").Check(testkit.Rows("1 11 1", "3 30 1"))
}

func TestMaterializedViewRefreshCompleteInPlaceCommitTSOSnapshotMatchesRefreshInfo(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	makeMaterializedViewRefreshResultStale(t, tk)

	tk.MustExec("refresh materialized view mv_refresh_result complete in place")
	requireRefreshInfoSnapshotMatchesLatestSuccessHist(t, tk, "test", "mv_refresh_result")
}

func TestMaterializedViewRefreshCompleteOutOfPlaceStatementResult(t *testing.T) {
	tk := setupMaterializedViewRefreshStatementResultTest(t)
	makeMaterializedViewRefreshResultStale(t, tk)

	tk.MustExec("refresh materialized view mv_refresh_result complete out of place")
	requireRefreshMVStatementResult(t, tk, 2, 0, 0)
	tk.MustQuery("select * from mv_refresh_result order by a").Check(testkit.Rows("1 11 1", "3 30 1"))
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

func TestMaterializedViewRefreshDryRunUsesInternalTxnInfoSchema(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_dry_run (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_dry_run values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_dry_run (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_dry_run (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_dry_run group by a")
	lastSuccessReadTSO := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
	tk.MustExec("insert into t_mv_dry_run values (1, 5)")

	for _, tc := range []struct {
		mode     string
		operator string
	}{
		{"complete delta apply", "MViewCompleteDeltaApply"},
		{"fast", "MViewDeltaMerge"},
	} {
		// Each statement registers the outer session's MV dependency before planning
		// in an internal session, which must use its own transaction's MDL table set.
		for range 2 {
			rows := tk.MustQuery("refresh materialized view mv_dry_run " + tc.mode + " dry run").Rows()
			requireRowsContainPrefix(t, rows, "  "+tc.operator)
			tk.MustQuery("select * from mv_dry_run order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
			tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Check(lastSuccessReadTSO)
			tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_hist").Check(testkit.Rows("0"))
		}
	}

	tk.MustExec("refresh materialized view mv_dry_run complete delta apply")
	tk.MustQuery("select * from mv_dry_run order by a").Check(testkit.Rows("1 15 2", "2 20 1"))
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

func TestFastDryRunMaterializedViewRefreshUsesCurrentDB(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_fast_dry_run (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_fast_dry_run values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_fast_dry_run (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_fast_dry_run (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_fast_dry_run group by a")

	rows := tk.MustQuery("refresh materialized view mv_fast_dry_run fast dry run").Rows()
	requireRowsContainPrefix(t, rows, "[S04 DATA_CHANGE_FAST_MERGE]")
	requireRowsContainPrefix(t, rows, "  MViewDeltaMerge")
}

func TestFastDryRunMaterializedViewRefreshRejectsHazardousPurgeInfo(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	err = tk.ExecToErr("refresh materialized view mv_fast_dry_run_purge_guard fast")
	require.ErrorContains(t, err, "materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO")
}

func TestFastDryRunMaterializedViewRefreshNoopSkipsHazardousPurgeInfo(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	tk.MustQuery("select * from mv_fast_dry_run_noop_purge_guard order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvID)).
		Check(testkit.Rows(fmt.Sprint(targetTSO)))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mvID)).
		Check(testkit.Rows("0"))
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

	tk.MustExec("refresh materialized view mv_mv_quota_refresh complete delta apply")
	require.True(t, applied)
	require.Equal(t, int64(268435456), lastAppliedMaintainQuota)
	require.Equal(t, lastAppliedMaintainQuota, lastAppliedMemQuotaQuery)

	applied = false
	lastAppliedMemQuotaQuery = 0
	lastAppliedMaintainQuota = 0
	mustExecInternal(t, tk, "refresh materialized view mv_mv_quota_refresh complete delta apply")
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
	require.Equal(t, int64(268435456), lastBuildAppliedMaintainQuota)
	require.Equal(t, lastBuildAppliedMaintainQuota, lastBuildAppliedMemQuotaQuery)
}

func TestMaterializedViewRefreshUsesMVMaintainIsolationReadEngines(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_isolation (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_isolation values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_isolation (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_isolation (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_isolation group by a")
	tk.MustExec(fmt.Sprintf("set @@session.%s = 'tikv,tiflash,tidb'", variable.TiDBIsolationReadEngines))
	tk.MustExec(fmt.Sprintf("set @@session.%s = 'tikv'", variable.TiDBMVMaintainIsolationReadEngines))

	requireRefreshIsolationReadEnginesApplied(
		t,
		"github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewIsolationReadEnginesApplied",
		func() {
			tk.MustExec("refresh materialized view mv_mv_refresh_isolation complete in place")
		},
		"tikv",
	)
	tk.MustQuery(fmt.Sprintf("select @@session.%s", variable.TiDBIsolationReadEngines)).Check(testkit.Rows("tikv,tiflash,tidb"))

	tk.MustExec("insert into t_mv_refresh_isolation values (3, 30)")
	requireRefreshIsolationReadEnginesApplied(
		t,
		"github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildIsolationReadEnginesApplied",
		func() {
			tk.MustExec("refresh materialized view mv_mv_refresh_isolation complete out of place")
		},
		"tikv",
	)
	tk.MustQuery(fmt.Sprintf("select @@session.%s", variable.TiDBIsolationReadEngines)).Check(testkit.Rows("tikv,tiflash,tidb"))
}

func TestMaterializedViewRefreshDefaultMVMaintainIsolationReadEnginesDoesNotInheritCurrentSession(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_isolation_default (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_isolation_default values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_isolation_default (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_isolation_default (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_isolation_default group by a")

	defaultMaintainIsolationReadEngines := variable.GetSysVar(variable.TiDBMVMaintainIsolationReadEngines).Value
	currentIsolationReadEngines := differentIsolationReadEnginesForTest(defaultMaintainIsolationReadEngines)
	tk.MustExec(fmt.Sprintf("set @@session.%s = '%s'", variable.TiDBIsolationReadEngines, currentIsolationReadEngines))

	requireRefreshIsolationReadEnginesApplied(
		t,
		"github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewIsolationReadEnginesApplied",
		func() {
			tk.MustExec("refresh materialized view mv_mv_refresh_isolation_default complete in place")
		},
		defaultMaintainIsolationReadEngines,
	)
	tk.MustQuery(fmt.Sprintf("select @@session.%s", variable.TiDBIsolationReadEngines)).Check(testkit.Rows(currentIsolationReadEngines))
}

func TestMaterializedViewRefreshManualSQLUsesCurrentSessionTiFlashSessionVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_tiflash_vars (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_tiflash_vars values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_tiflash_vars (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_tiflash_vars (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_refresh_tiflash_vars group by a")
	prepareRefreshTiFlashSessionVarsForTest(t, tk)

	requireRefreshTiFlashSessionVarsApplied(t, func() {
		tk.MustExec("refresh materialized view mv_mv_refresh_tiflash_vars complete delta apply")
	}, int64(8), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
	requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
}

func TestMaterializedViewRefreshInternalSQLUsesCurrentSessionTiFlashSessionVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_tiflash_vars (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_tiflash_vars values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_tiflash_vars (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_tiflash_vars (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_refresh_tiflash_vars group by a")
	prepareRefreshTiFlashSessionVarsForTest(t, tk)

	requireRefreshTiFlashSessionVarsApplied(t, func() {
		mustExecInternal(t, tk, "refresh materialized view mv_mv_refresh_tiflash_vars complete delta apply")
	}, int64(8), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
	requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
}

func TestMaterializedViewRefreshManualSQLFailsWhenApplyExecutionSessionVarsFails(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_apply_vars_manual (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_apply_vars_manual values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_apply_vars_manual (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_apply_vars_manual (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_apply_vars_manual group by a")

	failpointName := "github.com/pingcap/tidb/pkg/executor/mockRefreshExecutionSessionVarsApplyError"
	require.NoError(t, failpoint.Enable(failpointName, "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})

	err := tk.ExecToErr("refresh materialized view mv_mv_refresh_apply_vars_manual complete delta apply")
	require.ErrorContains(t, err, "mock refresh execution session vars apply error")
}

func TestMaterializedViewRefreshInternalSQLFallsBackWhenApplyExecutionSessionVarsFails(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_apply_vars_internal (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_apply_vars_internal values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_apply_vars_internal (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_apply_vars_internal (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_apply_vars_internal group by a")

	failpointName := "github.com/pingcap/tidb/pkg/executor/mockRefreshExecutionSessionVarsApplyError"
	require.NoError(t, failpoint.Enable(failpointName, "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})

	tk.MustExec("insert into t_mv_refresh_apply_vars_internal values (3, 30)")
	mustExecInternal(t, tk, "refresh materialized view mv_mv_refresh_apply_vars_internal complete delta apply")
	tk.MustQuery("select * from mv_mv_refresh_apply_vars_internal order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
}

func TestMaterializedViewRefreshManualSQLFailsWhenSingleExecutionSessionVarApplyFails(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_single_apply_var_manual (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_single_apply_var_manual values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_single_apply_var_manual (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_single_apply_var_manual (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_single_apply_var_manual group by a")
	prepareRefreshTiFlashSessionVarsForTest(t, tk)

	failpointName := "github.com/pingcap/tidb/pkg/ddl/mockMViewExecutionSessionVarApplyError"
	require.NoError(t, failpoint.Enable(failpointName, fmt.Sprintf("return(%q)", variable.TiDBMaxTiFlashThreads)))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})

	err := tk.ExecToErr("refresh materialized view mv_mv_refresh_single_apply_var_manual complete delta apply")
	require.ErrorContains(t, err, variable.TiDBMaxTiFlashThreads)
}

func TestMaterializedViewRefreshInternalSQLFallsBackPerVarWhenExecutionSessionVarApplyFails(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_single_apply_var_internal (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_single_apply_var_internal values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_single_apply_var_internal (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_single_apply_var_internal (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_single_apply_var_internal group by a")
	prepareRefreshTiFlashSessionVarsForTest(t, tk)

	failpointName := "github.com/pingcap/tidb/pkg/ddl/mockMViewExecutionSessionVarApplyError"
	require.NoError(t, failpoint.Enable(failpointName, fmt.Sprintf("return(%q)", variable.TiDBMaxTiFlashThreads)))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})

	tk.MustExec("insert into t_mv_refresh_single_apply_var_internal values (3, 30)")
	requireRefreshTiFlashSessionVarsApplied(t, func() {
		mustExecInternal(t, tk, "refresh materialized view mv_mv_refresh_single_apply_var_internal complete delta apply")
	}, int64(variable.DefTiFlashMaxThreads), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
	tk.MustQuery("select * from mv_mv_refresh_single_apply_var_internal order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
	requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
}

func TestMaterializedViewRefreshInternalSQLOutOfPlaceFallsBackWhenApplyExecutionSessionVarsFails(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_apply_vars_internal_oop (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_apply_vars_internal_oop values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_apply_vars_internal_oop (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_apply_vars_internal_oop (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_apply_vars_internal_oop group by a")

	failpointName := "github.com/pingcap/tidb/pkg/executor/mockRefreshExecutionSessionVarsApplyError"
	require.NoError(t, failpoint.Enable(failpointName, "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})

	tk.MustExec("insert into t_mv_refresh_apply_vars_internal_oop values (3, 30)")
	mustExecInternal(t, tk, "refresh materialized view mv_mv_refresh_apply_vars_internal_oop complete out of place")
	tk.MustQuery("select * from mv_mv_refresh_apply_vars_internal_oop order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
}

func TestMaterializedViewRefreshInternalSQLOutOfPlaceFallsBackPerVarWhenExecutionSessionVarApplyFails(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_single_apply_var_internal_oop (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_single_apply_var_internal_oop values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_single_apply_var_internal_oop (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_single_apply_var_internal_oop (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_single_apply_var_internal_oop group by a")
	prepareRefreshTiFlashSessionVarsForTest(t, tk)

	failpointName := "github.com/pingcap/tidb/pkg/ddl/mockMViewExecutionSessionVarApplyError"
	require.NoError(t, failpoint.Enable(failpointName, fmt.Sprintf("return(%q)", variable.TiDBMaxTiFlashThreads)))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})

	tk.MustExec("insert into t_mv_refresh_single_apply_var_internal_oop values (3, 30)")
	requireTiFlashSessionVarsAppliedWithFailpoint(
		t,
		"github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildTiFlashSessionVarsApplied",
		"github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildTiFlashSpillSessionVarsApplied",
		func() {
			mustExecInternal(t, tk, "refresh materialized view mv_mv_refresh_single_apply_var_internal_oop complete out of place")
		},
		int64(variable.DefTiFlashMaxThreads), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096),
	)
	tk.MustQuery("select * from mv_mv_refresh_single_apply_var_internal_oop order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
	requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
}

func TestMaterializedViewRefreshOutOfPlaceUsesCurrentSessionTiFlashSessionVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_oop_tiflash_vars (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_oop_tiflash_vars values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_oop_tiflash_vars (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_oop_tiflash_vars (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_refresh_oop_tiflash_vars group by a")
	prepareRefreshTiFlashSessionVarsForTest(t, tk)

	failpointName := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildTiFlashSessionVarsApplied"
	spillFailpointName := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildTiFlashSpillSessionVarsApplied"
	importFailpointName := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceBuildImportSessionVarsApplied"
	requireImportSessionVarsAppliedWithFailpoint(t, importFailpointName, func() {
		requireTiFlashSessionVarsAppliedWithFailpoint(t, failpointName, spillFailpointName, func() {
			tk.MustExec("refresh materialized view mv_mv_refresh_oop_tiflash_vars complete out of place")
		}, int64(8), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
	}, 12, "64gib")

	tk.MustExec("insert into t_mv_refresh_oop_tiflash_vars values (3, 30)")
	requireImportSessionVarsAppliedWithFailpoint(t, importFailpointName, func() {
		requireTiFlashSessionVarsAppliedWithFailpoint(t, failpointName, spillFailpointName, func() {
			mustExecInternal(t, tk, "refresh materialized view mv_mv_refresh_oop_tiflash_vars complete out of place")
		}, int64(8), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
	}, 12, "64gib")
	requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
}

func TestCreateMaterializedViewInitBuildUsesCurrentSessionExecutionSessionVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_create_build_vars (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_create_build_vars values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_create_build_vars (a, b)")
	prepareRefreshTiFlashSessionVarsForTest(t, tk)
	tk.MustExec("set @@session.tidb_mem_quota_query = 1073741824")
	tk.MustExec("set @@global.tidb_mv_maintain_mem_quota = 536870912")
	tk.MustExec("set @@session.tidb_mv_maintain_mem_quota = 268435456")
	t.Cleanup(func() {
		tk.MustExec(fmt.Sprintf("set @@global.%s = %d", variable.TiDBMVMaintainMemQuota, 2*1024*1024*1024))
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
	require.Equal(t, int64(268435456), tk.Session().GetSessionVars().MVMaintainMemQuota)
	require.Equal(t, int64(1073741824), tk.Session().GetSessionVars().MemQuotaQuery)
	tk.MustQuery("select * from mv_mv_create_build_vars order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
}

func TestMaterializedViewRefreshDryRunUsesCurrentSessionTiFlashSessionVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_dry_run_tiflash_vars (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_dry_run_tiflash_vars values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_refresh_dry_run_tiflash_vars (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_mv_refresh_dry_run_tiflash_vars (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_refresh_dry_run_tiflash_vars group by a")
	prepareRefreshTiFlashSessionVarsForTest(t, tk)

	requireRefreshTiFlashSessionVarsApplied(t, func() {
		tk.MustQuery("refresh materialized view mv_mv_refresh_dry_run_tiflash_vars complete delta apply dry run").Rows()
	}, int64(8), int64(101), int64(202), int64(303), int64(404), float64(0.75), int64(16), uint64(4096))
	requireCurrentSessionTiFlashSessionVarsRestored(t, tk.Session().GetSessionVars())
}

func TestMaterializedViewRefreshNextUnixSecondsOnlyUpdatesForInternalSQL(t *testing.T) {
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
	mustExecInternal(t, tk, "refresh materialized view mv_internal_next complete delta apply")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete delta apply auto"))
}

func TestMaterializedViewRefreshScheduleDurationOnlyForInternalSQL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_refresh_schedule_duration (a int not null, b int not null)")
	tk.MustExec("insert into t_refresh_schedule_duration values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_refresh_schedule_duration (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_schedule_duration (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_schedule_duration group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_refresh_schedule_duration"))
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
	mustExecInternal(t, tk, "refresh materialized view mv_refresh_schedule_duration fast")
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD, REFRESH_SCHEDULE_DURATION_SEC >= 100, REFRESH_SCHEDULE_DURATION_SEC <= 150 from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("fast auto 1 1"))
	require.Greater(t, readMVServiceRefreshScheduleDurationCount(t), metricCountBefore)

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = null where MVIEW_ID = %d", mviewID))
	tk.MustExec("insert into t_refresh_schedule_duration values (5, 50)")
	mustExecInternal(t, tk, "refresh materialized view mv_refresh_schedule_duration fast")
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD, REFRESH_SCHEDULE_DURATION_SEC is not null, REFRESH_SCHEDULE_DURATION_SEC >= 0 from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("fast auto 1 1"))
}

func TestMaterializedViewRefreshInternalSQLStartWithNoNextSetsNextUnixSecondsNull(t *testing.T) {
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
	mustExecInternal(t, tk, "refresh materialized view mv_internal_start_only complete delta apply")
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete delta apply auto"))
}

func TestMaterializedViewRefreshInternalSQLNoScheduleSetsNextUnixSecondsNull(t *testing.T) {
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

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = UNIX_TIMESTAMP() + 3 * 60 * 60 where MVIEW_ID = %d", mviewID))

	mustExecInternal(t, tk, "refresh materialized view mv_internal_no_schedule complete delta apply")
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		mviewID,
	)).Check(testkit.Rows("complete delta apply auto"))
}

func TestMaterializedViewRefreshInternalSQLOutOfPlaceUpdatesNextUnixSeconds(t *testing.T) {
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

	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = null where MVIEW_ID = %d", oldMViewID))
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
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		newMViewID,
	)).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		oldMViewID,
	)).Check(testkit.Rows("complete out of place auto"))
}

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

	err := tk.ExecToErr("refresh materialized view mv_refresh_asof_nonfast complete delta apply as of timestamp '2021-04-15 00:00:00'")
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

func TestMaterializedViewRefreshFastAsOfTimestampTracksAutomaticMethod(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	mustExecInternal(t, tk, fmt.Sprintf(
		"refresh materialized view mv_refresh_asof_internal fast as of timestamp '%s'",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))

	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = %d from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		targetTSO,
		mvID,
	)).Check(testkit.Rows("success bounded fast auto 1"))
}

func TestMaterializedViewRefreshFastAsOfTimestampEarlyFailureWritesHistReadTSO(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
}

func TestMaterializedViewFastRefreshRejectsHazardousPurgeHist(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_refresh_purge_guard fast")
	}()

	select {
	case <-hitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for refresh to reach failpoint")
	}

	tkConcurrent := testkit.NewTestKit(t, store)
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
	tk := testkit.NewTestKit(t, store)
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
	tk := testkit.NewTestKit(t, store)
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

func TestMaterializedViewRefreshFastAsOfTimestampDryRunAllowsTooOldGCSafePointWithoutTargetSnapshot(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t_mv_refresh_asof_dry_run_gc (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_dry_run_gc (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_dry_run_gc (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_dry_run_gc group by a")

	mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_dry_run_gc'").Rows()
	require.Len(t, mvIDRow, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
	require.NoError(t, err)

	fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	fromTSO := oracle.GoTimeToTS(fromTime)
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
		fromTSO,
		mvID,
	))
	tk.MustExec("insert into t_mv_refresh_asof_dry_run_gc values (1, 10)")
	targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
	if sleepUntilTarget > 0 {
		time.Sleep(sleepUntilTarget)
	}
	targetTSO := oracle.GoTimeToTS(targetTime)
	mustSetMockGCSafePoint(t, tk, targetTime.Add(time.Second))

	rows := tk.MustQuery(fmt.Sprintf(
		"refresh materialized view mv_refresh_asof_dry_run_gc fast as of timestamp '%s' dry run",
		targetTime.Format("2006-01-02 15:04:05.000"),
	)).Rows()
	requireRowsContainSubstring(t, rows, "_tidb_commit_ts")
	requireRowsContainSubstring(t, rows, strconv.FormatUint(fromTSO, 10))
	requireRowsContainSubstring(t, rows, strconv.FormatUint(targetTSO, 10))
}

func TestMaterializedViewRefreshFastAsOfTimestampAllowsTooOldGCSafePointWithoutTargetSnapshot(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t_mv_refresh_asof_gc (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_gc (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_gc (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_gc group by a")

	mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_gc'").Rows()
	require.Len(t, mvIDRow, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
	require.NoError(t, err)

	fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	fromTSO := oracle.GoTimeToTS(fromTime)
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
		fromTSO,
		mvID,
	))
	tk.MustExec("insert into t_mv_refresh_asof_gc values (1, 10)")
	targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
	if sleepUntilTarget > 0 {
		time.Sleep(sleepUntilTarget)
	}
	targetTSO := oracle.GoTimeToTS(targetTime)
	mustSetMockGCSafePoint(t, tk, targetTime.Add(time.Second))

	tk.MustExec(fmt.Sprintf(
		"refresh materialized view mv_refresh_asof_gc fast as of timestamp '%s'",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	tk.MustQuery("select a, s, cnt from mv_refresh_asof_gc").Check(testkit.Rows("1 10 1"))
	tk.MustQuery(fmt.Sprintf(
		"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvID,
	)).Check(testkit.Rows(strconv.FormatUint(targetTSO, 10)))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = %d from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		targetTSO,
		mvID,
	)).Check(testkit.Rows("success bounded fast manual 1"))
}

func TestMaterializedViewRefreshFastAsOfTimestampDryRunRejectsTooOldGCSafePointWithMinMax(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t_mv_refresh_asof_dry_run_gc_minmax (a int not null, b int not null, key idx_a(a))")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_dry_run_gc_minmax (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_dry_run_gc_minmax (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_dry_run_gc_minmax group by a")

	mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_dry_run_gc_minmax'").Rows()
	require.Len(t, mvIDRow, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
	require.NoError(t, err)

	fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	fromTSO := oracle.GoTimeToTS(fromTime)
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
		fromTSO,
		mvID,
	))
	tk.MustExec("insert into t_mv_refresh_asof_dry_run_gc_minmax values (1, 10)")
	targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
	if sleepUntilTarget > 0 {
		time.Sleep(sleepUntilTarget)
	}
	mustSetMockGCSafePoint(t, tk, targetTime.Add(time.Second))

	err = tk.QueryToErr(fmt.Sprintf(
		"refresh materialized view mv_refresh_asof_dry_run_gc_minmax fast as of timestamp '%s' dry run",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	require.True(t, terror.ErrorEqual(err, variable.ErrSnapshotTooOld), "err %v", err)
}

func TestMaterializedViewRefreshFastAsOfTimestampRejectsTooOldGCSafePointWithMinMax(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t_mv_refresh_asof_gc_minmax (a int not null, b int not null, key idx_a(a))")
	tk.MustExec("create materialized view log on t_mv_refresh_asof_gc_minmax (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_refresh_asof_gc_minmax (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_gc_minmax group by a")

	mvIDRow := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_asof_gc_minmax'").Rows()
	require.Len(t, mvIDRow, 1)
	mvID, err := strconv.ParseInt(fmt.Sprintf("%v", mvIDRow[0][0]), 10, 64)
	require.NoError(t, err)

	fromTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	fromTSO := oracle.GoTimeToTS(fromTime)
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = %d",
		fromTSO,
		mvID,
	))
	tk.MustExec("insert into t_mv_refresh_asof_gc_minmax values (1, 10)")
	targetTime := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	sleepUntilTarget := time.Until(targetTime.Add(20 * time.Millisecond))
	if sleepUntilTarget > 0 {
		time.Sleep(sleepUntilTarget)
	}
	targetTSO := oracle.GoTimeToTS(targetTime)
	mustSetMockGCSafePoint(t, tk, targetTime.Add(time.Second))

	err = tk.ExecToErr(fmt.Sprintf(
		"refresh materialized view mv_refresh_asof_gc_minmax fast as of timestamp '%s'",
		targetTime.Format("2006-01-02 15:04:05.000"),
	))
	require.True(t, terror.ErrorEqual(err, variable.ErrSnapshotTooOld), "err %v", err)
	tk.MustQuery("select * from mv_refresh_asof_gc_minmax").Check(testkit.Rows())
	tk.MustQuery(fmt.Sprintf(
		"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvID,
	)).Check(testkit.Rows(strconv.FormatUint(fromTSO, 10)))
	tk.MustQuery(fmt.Sprintf(
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1",
		targetTSO,
		mvID,
	)).Check(testkit.Rows("failed bounded fast manual 1 1"))
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
}

func TestMaterializedViewRefreshFastMinMax(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
}

func TestMaterializedViewRefreshFastMinMaxUsesSupportingIndex(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	tk.MustQuery("SELECT * FROM mv_fast_minmax_hint").Check(testkit.Rows())
}

func TestMaterializedViewRefreshFastMinMaxRequiresSupportingIndex(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_mv_fast_minmax_noidx (id int not null primary key, a int not null, b int not null, key idx_a(a))")
	tk.MustExec("create materialized view log on t_mv_fast_minmax_noidx (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_fast_minmax_noidx (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_mv_fast_minmax_noidx group by a")

	tk.MustExec("insert into t_mv_fast_minmax_noidx values (1, 1, 10), (2, 1, 20)")
	tk.MustExec("refresh materialized view mv_fast_minmax_noidx complete delta apply")

	err := tk.ExecToErr("alter table t_mv_fast_minmax_noidx alter index idx_a invisible")
	require.ErrorContains(t, err, "Unsupported ALTER INDEX INVISIBLE on base table index idx_a required by materialized view mv_fast_minmax_noidx for MIN/MAX fast refresh")
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
	tk.MustExec("refresh materialized view mv complete delta apply")
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

	tk.MustExec("refresh materialized view mv complete delta apply")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
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

	err = tk.ExecToErr("refresh materialized view mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "advisory lock cleanup invariant violated")

	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 10 2", "3 4 1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_END_TIME is null, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
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
}

func TestMaterializedViewRefreshRunningHistHeartbeat(t *testing.T) {
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
		tkRefresh := testkit.NewTestKit(t, store)
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
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete delta apply")
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

	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", refreshReadTSO, mviewID)).
		Check(testkit.Rows("success complete delta apply manual 1 1 1"))
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
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete delta apply")
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
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO = %d, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", newTS, mviewID)).
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
	tkRefresh := testkit.NewTestKit(t, store)
	tkRefresh.MustExec("use test")
	advisoryLockName := fmt.Sprintf("mv_refresh_%d_%d", mvTable.Meta().DBID, mviewID)
	require.NoError(t, tkLocker.Session().GetAdvisoryLock(advisoryLockName, 1))
	defer tkLocker.Session().ReleaseAllAdvisoryLocks()
	for _, method := range []string{"fast", "complete delta apply", "complete in place", "complete out of place"} {
		tkRefresh.MustGetErrCode("refresh materialized view mv "+method, int(exeerrors.ErrMVAdvisoryLockConflict.Code()))
	}
	require.True(t, tkLocker.Session().ReleaseAdvisoryLock(advisoryLockName))

	tkLocker.MustExec("begin pessimistic")
	tkLocker.MustQuery(fmt.Sprintf("select MVIEW_ID from mysql.tidb_mview_refresh_info where MVIEW_ID = %d for update", mviewID))

	err = tkRefresh.ExecToErr("refresh materialized view mv complete delta apply")
	require.Error(t, err)
	require.ErrorContains(t, err, "NOWAIT")

	tkLocker.MustExec("rollback")

	// After releasing the lock, refresh should succeed.
	tkRefresh.MustExec("refresh materialized view mv complete delta apply")
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
	err = tk.ExecToErr("refresh materialized view mv complete delta apply")
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
	err := tk.ExecToErr("refresh materialized view mv with async mode complete delta apply")
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
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d", mviewID)).
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
		"select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.RowsWithSep("|", "success|complete in place manual|1|1|1|1"))
}

func TestMaterializedViewRefreshCompleteInPlaceRejectsMLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create materialized view log on mv (a, s, cnt)")
	tk.MustExec("insert into t values (3, 30)")

	err := tk.ExecToErr("refresh materialized view mv complete in place")
	require.ErrorContains(t, err, "complete IN PLACE is not supported")
	require.ErrorContains(t, err, "with a materialized view log")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceReplacesMLog(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create placement policy mv_oop_policy followers=2")
	tk.MustExec("alter database test placement policy=mv_oop_policy")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create materialized view log on mv (a, s, cnt) purge next date_add(now(), interval 1 hour)")
	getInfo := func(name string) *model.TableInfo {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta()
	}
	oldMV, oldLog := getInfo("mv"), getInfo("$mlog$mv")
	checkLogPlacement := func(logInfo *model.TableInfo) {
		require.NotNil(t, logInfo.PlacementPolicyRef)
		require.Equal(t, "mv_oop_policy", logInfo.PlacementPolicyRef.Name.L)
		bundle, ok := dom.InfoSchema().PlacementBundleByPhysicalTableID(logInfo.ID)
		require.True(t, ok)
		pdBundle, err := infosync.GetRuleBundle(context.Background(), bundle.ID)
		require.NoError(t, err)
		require.Equal(t, bundle, pdBundle)
	}
	checkLogPlacement(oldLog)
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = 1 where MLOG_ID = %d", oldLog.ID))
	for range 2 {
		tk.MustExec("refresh materialized view mv complete out of place")
		newMV, newLog := getInfo("mv"), getInfo("$mlog$mv")
		require.NotEqual(t, oldMV.ID, newMV.ID)
		require.NotEqual(t, oldLog.ID, newLog.ID)
		require.Equal(t, newLog.ID, newMV.MaterializedViewBase.MLogID)
		expectedLog := oldLog.MaterializedViewLog.Clone()
		expectedLog.BaseTableID = newMV.ID
		require.Equal(t, expectedLog, newLog.MaterializedViewLog)
		require.Equal(t, oldLog.Columns, newLog.Columns)
		require.ElementsMatch(t, oldLog.Indices, newLog.Indices)
		checkLogPlacement(newLog)
		tk.MustQuery("select count(*) from `$mlog$mv`").Check(testkit.Rows("0"))
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", oldLog.ID)).Check(testkit.Rows("0"))
		tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS is not null, LAST_PURGED_TSO is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", newLog.ID)).Check(testkit.Rows("1 1"))
		_, ok := dom.InfoSchema().TableByID(context.Background(), oldLog.ID)
		require.False(t, ok)
		oldMV, oldLog = newMV, newLog
	}
	tk.MustExec("insert into t values (3, 30)")
	tk.MustExec("refresh materialized view mv fast")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
	tk.MustQuery("select count(*) > 0 from `$mlog$mv`").Check(testkit.Rows("1"))
	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
}

func TestMaterializedViewRefreshCompleteOutOfPlaceRebindsDependentMaterializedViews(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_oop (a int not null, b int not null)")
	tk.MustExec("insert into t_oop values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_oop (a, b)")
	tk.MustExec("create materialized view mv_oop (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_oop group by a")
	tk.MustExec("create materialized view log on mv_oop (a, s, cnt)")
	for _, name := range []string{"child1", "child2"} {
		tk.MustExec("create materialized view " + name + " (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_oop group by a")
	}
	getInfo := func(name string) *model.TableInfo {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta()
	}
	oldMV, oldLog := getInfo("mv_oop"), getInfo("$mlog$mv_oop")
	child1, child2 := getInfo("child1"), getInfo("child2")
	childIDs := fmt.Sprintf("%d,%d", child1.ID, child2.ID)
	childInfoSQL := "select * from mysql.tidb_mview_refresh_info where MVIEW_ID in (" + childIDs + ") order by MVIEW_ID"
	childSourcesSQL := "select * from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID in (" + childIDs + ") order by MVIEW_ID, MLOG_ID"
	beforeInfo, beforeSources := tk.MustQuery(childInfoSQL).Rows(), tk.MustQuery(childSourcesSQL).Rows()
	tk.MustExec("insert into t_oop values (3, 30)")
	tk.MustExec("refresh materialized view mv_oop complete out of place")
	newMV, newLog := getInfo("mv_oop"), getInfo("$mlog$mv_oop")
	require.NotEqual(t, oldMV.ID, newMV.ID)
	require.NotEqual(t, oldLog.ID, newLog.ID)
	require.ElementsMatch(t, []int64{child1.ID, child2.ID}, newMV.MaterializedViewBase.MViewIDs)
	require.ElementsMatch(t, []int64{child1.ID, child2.ID}, newLog.MaterializedViewLog.DependentMViewIDs)
	for _, name := range []string{"child1", "child2"} {
		require.Equal(t, []int64{newMV.ID}, getInfo(name).MaterializedView.BaseTableIDs)
		tk.MustGetErrCode("refresh materialized view "+name+" fast", int(exeerrors.ErrMVRefreshLogBaselineChanged.Code()))
		tk.MustQuery("select * from " + name + " order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1"))
	}
	require.Equal(t, beforeInfo, tk.MustQuery(childInfoSQL).Rows())
	require.Equal(t, beforeSources, tk.MustQuery(childSourcesSQL).Rows())
	tk.MustQuery("select count(*) from `$mlog$mv_oop`").Check(testkit.Rows("0"))

	// Old physical objects can be collected without deleting child registrations.
	jobs := tk.MustQuery("admin show ddl jobs where JOB_TYPE='refresh materialized view complete out-of-place cutover'").Rows()
	require.NotEmpty(t, jobs)
	tk.MustQuery(fmt.Sprintf("select count(*) from (select element_id from mysql.gc_delete_range where job_id=%v union all select element_id from mysql.gc_delete_range_done where job_id=%v) g", jobs[0][0], jobs[0][0])).Check(testkit.Rows("2"))

	// A child with an output log can recover its input with DELTA APPLY while
	// preserving the log consumed by the next level.
	tk.MustExec("create materialized view log on child1 (a, s, cnt, cnt_s)")
	tk.MustExec("create materialized view grandchild (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from child1 group by a")
	childLogID := getInfo("$mlog$child1").ID
	tk.MustExec("refresh materialized view child1 complete delta apply")
	require.Equal(t, childLogID, getInfo("$mlog$child1").ID)
	tk.MustExec("refresh materialized view grandchild fast")
	tk.MustQuery("select * from grandchild order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1", "3 30 1 1"))
	tk.MustQuery(fmt.Sprintf("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID=%d", child2.ID)).Check(testkit.Rows(fmt.Sprint(oldLog.ID)))

	// Purge must protect the new log using child2's watermark even though its
	// input registration still names the retired output log.
	tk.MustExec("insert into t_oop values (1, 5), (4, 40)")
	tk.MustExec("update t_oop set b=25 where a=2")
	tk.MustExec("delete from t_oop where a=3")
	tk.MustExec("refresh materialized view mv_oop fast")
	delta := tk.MustQuery("select * from `$mlog$mv_oop` order by _tidb_rowid").Rows()
	require.NotEmpty(t, delta)
	tk.MustExec("purge materialized view log on mv_oop")
	require.Equal(t, delta, tk.MustQuery("select * from `$mlog$mv_oop` order by _tidb_rowid").Rows())
	beforePeer := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID=%d", child2.ID)).Rows()
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID=%d", newLog.ID)).Check(beforePeer)
	tk.MustExec("refresh materialized view child1 fast")
	tk.MustExec("refresh materialized view child2 complete in place")
	for _, name := range []string{"child1", "child2"} {
		tk.MustQuery("select * from " + name + " order by a").Check(testkit.Rows("1 15 1 1", "2 25 1 1", "4 40 1 1"))
		tk.MustQuery(fmt.Sprintf("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID=%d", getInfo(name).ID)).Check(testkit.Rows(fmt.Sprint(newLog.ID)))
	}

	// OOP of the intermediate view replaces its own output log, invalidating
	// only the next level. The sibling's recovered input remains valid.
	tk.MustExec("refresh materialized view child1 complete out of place")
	require.NotEqual(t, childLogID, getInfo("$mlog$child1").ID)
	tk.MustGetErrCode("refresh materialized view grandchild fast", int(exeerrors.ErrMVRefreshLogBaselineChanged.Code()))
	tk.MustExec("refresh materialized view grandchild complete delta apply")
	tk.MustExec("insert into t_oop values (5, 50)")
	for _, name := range []string{"mv_oop", "child1", "child2", "grandchild"} {
		tk.MustExec("refresh materialized view " + name + " fast")
	}
	tk.MustQuery("select * from grandchild order by a").Check(testkit.Rows("1 15 1 1", "2 25 1 1", "4 40 1 1", "5 50 1 1"))
	tk.MustExec("drop materialized view grandchild")
	tk.MustExec("drop materialized view log on child1")
	for _, name := range []string{"child1", "child2", "mv_oop"} {
		if name == "mv_oop" {
			tk.MustExec("drop materialized view log on mv_oop")
		}
		tk.MustExec("drop materialized view " + name)
	}
}

func TestMaterializedViewRefreshCompleteOutOfPlaceRejectsConcurrentMLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_oop_mlog_race (a int not null, b int not null)")
	tk.MustExec("insert into t_oop_mlog_race values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_oop_mlog_race (a, b)")
	tk.MustExec("create materialized view mv_oop_mlog_race (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_oop_mlog_race group by a")
	tk.MustExec("insert into t_oop_mlog_race values (3, 30)")

	const pauseFailpoint = "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewOutOfPlaceAfterCreateShadow"
	require.NoError(t, failpoint.Enable(pauseFailpoint, "pause"))
	refreshDone := make(chan error, 1)
	enabled, finished := true, false
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseFailpoint))
		}
		if !finished {
			select {
			case <-refreshDone:
			case <-time.After(30 * time.Second):
				t.Error("timeout cleaning up OOP refresh")
			}
		}
	}()
	go func() {
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_oop_mlog_race complete out of place")
	}()

	require.Eventually(t, func() bool {
		return len(tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Rows()) == 1
	}, 30*time.Second, 10*time.Millisecond)
	// CREATE MLog must release its DDL scheduling claim when the refresh lock is busy,
	// so the in-flight OOP refresh can still submit and finish its cutover DDL.
	err := tk.ExecToErr("create materialized view log on mv_oop_mlog_race (a, s, cnt)")
	require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
	require.ErrorContains(t, err, "materialized view refresh is running, please retry later")
	tk.MustQuery("show tables like '$mlog$mv_oop_mlog_race'").Check(testkit.Rows())
	require.NoError(t, failpoint.Disable(pauseFailpoint))
	enabled = false
	select {
	case err = <-refreshDone:
		finished = true
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for OOP cutover after CREATE MLog failed")
	}
	require.NoError(t, err)
	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
	tk.MustQuery("select a, s, cnt from mv_oop_mlog_race order by a").Check(testkit.Rows(
		"1 10 1", "2 20 1", "3 30 1",
	))
	// The failed CREATE job must not leave metadata or locks that prevent a retry.
	tk.MustExec("create materialized view log on mv_oop_mlog_race (a, s, cnt)")
}

func TestMaterializedViewRefreshSchemaVersion(t *testing.T) {
	for _, scenario := range []string{"current", "empty_versions", "mlog", "reload_failure"} {
		t.Run(scenario, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_schema (a int not null, b int not null)")
			tk.MustExec("insert into t_schema values (1, 10)")
			tk.MustExec("create materialized view log on t_schema (a, b)")
			tk.MustExec("create materialized view mv_schema (a, s, cnt) as select a, sum(b), count(1) from t_schema group by a")
			tk.MustExec("insert into t_schema values (2, 20)")
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/disableOnTickReload", "return(true)")
			oldIS := dom.InfoSchema()
			if scenario == "empty_versions" {
				// Failed DDL steps can leave multiple reserved versions without diffs.
				ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
				require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
					_, err := meta.NewMutator(txn).GenSchemaVersions(2)
					return err
				}))
			} else if scenario != "current" {
				if scenario == "mlog" {
					tk.MustExec("create materialized view log on mv_schema (a, s, cnt)")
				} else {
					tk.MustExec("create table schema_bump (a int)")
				}
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
				// Simulate a node whose cache has not caught up with committed DDL.
				dom.InfoCache().Upsert(oldIS, 0)
			}
			const reloadFailure = "github.com/pingcap/tidb/pkg/domain/ErrorMockReloadFailed"
			if scenario != "mlog" {
				testfailpoint.Enable(t, reloadFailure, "return(true)")
			}
			switch scenario {
			case "current", "empty_versions":
				// An up-to-date refresh succeeds even when Reload would fail.
				tk.MustExec("refresh materialized view mv_schema complete in place")
				testfailpoint.Disable(t, reloadFailure)
			case "mlog":
				err := tk.ExecToErr("refresh materialized view mv_schema complete in place")
				require.ErrorContains(t, err, "with a materialized view log")
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
				tk.MustQuery("select * from mv_schema").Check(testkit.Rows("1 10 1"))
				return
			case "reload_failure":
				err := tk.ExecToErr("refresh materialized view mv_schema complete in place")
				require.ErrorContains(t, err, "mock reload failed")
				testfailpoint.Disable(t, reloadFailure)
				// Both the reload mutex and the MV advisory lock must be released.
				tk.MustExec("refresh materialized view mv_schema complete in place")
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
			}
			tk.MustQuery("select * from mv_schema order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
		})
	}
}

func TestMaterializedViewRefreshSchemaReloadConcurrent(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_schema (a int not null, b int not null)")
	tk.MustExec("insert into t_schema values (1, 10)")
	tk.MustExec("create materialized view log on t_schema (a, b)")
	const refreshCount = 10
	refreshers := make([]*testkit.TestKit, refreshCount)
	for i := range refreshCount {
		tk.MustExec(fmt.Sprintf("create materialized view mv_schema_%d (a, s, cnt) as select a, sum(b), count(1) from t_schema group by a", i))
		refreshers[i] = testkit.NewTestKit(t, store)
		refreshers[i].MustExec("use test")
	}
	tk.MustExec("insert into t_schema values (2, 20)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/disableOnTickReload", "return(true)")
	oldIS := dom.InfoSchema()
	tk.MustExec("create table schema_bump (a int)")
	requiredVersion := dom.InfoSchema().SchemaMetaVersion()
	require.Greater(t, requiredVersion, oldIS.SchemaMetaVersion())
	dom.InfoCache().Upsert(oldIS, 0)

	waiting := make(chan int64, refreshCount)
	resume := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReloadLock", func(version int64) {
		waiting <- version
		<-resume
	})
	var reloads atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReload", func(int64) {
		reloads.Add(1)
	})
	done := make(chan error, refreshCount)
	finished := 0
	release := func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}
	defer func() {
		release()
		for finished < refreshCount {
			select {
			case <-done:
				finished++
			case <-time.After(15 * time.Second):
				t.Error("timeout cleaning up concurrent refreshes")
				return
			}
		}
	}()
	for i, refresher := range refreshers {
		go func() {
			done <- refresher.ExecToErr(fmt.Sprintf("refresh materialized view mv_schema_%d fast", i))
		}()
	}
	for range refreshCount {
		select {
		case version := <-waiting:
			require.Equal(t, requiredVersion, version)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for every refresh to observe the stale schema")
		}
	}
	release()
	for finished < refreshCount {
		select {
		case err := <-done:
			finished++
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for concurrent refresh completion")
		}
	}
	require.EqualValues(t, 1, reloads.Load(), "concurrent refreshes must share the first successful schema reload")
	require.GreaterOrEqual(t, dom.InfoSchema().SchemaMetaVersion(), requiredVersion)
	for i := range refreshCount {
		tk.MustQuery(fmt.Sprintf("select * from mv_schema_%d order by a", i)).Check(testkit.Rows("1 10 1", "2 20 1"))
	}
}

func TestMaterializedViewRefreshCreateMLogConflict(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		rollback bool
	}{
		{"fast", false},
		{"complete delta apply", false},
		{"complete in place", false},
		{"complete in place", true},
		{"complete out of place", false},
	} {
		t.Run(fmt.Sprintf("%s/rollback=%t", tc.mode, tc.rollback), func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mlog_refresh (a int not null, b int not null)")
			tk.MustExec("insert into t_mlog_refresh values (1, 10)")
			tk.MustExec("create materialized view log on t_mlog_refresh (a, b)")
			tk.MustExec("create materialized view mv_mlog_refresh (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mlog_refresh group by a")
			tk.MustExec("insert into t_mlog_refresh values (2, 20)")

			paused := make(chan struct{})
			resume := make(chan struct{})
			pause := func() { close(paused); <-resume }
			release := func() {
				select {
				case <-resume:
				default:
					close(resume)
				}
			}
			if tc.mode == "complete out of place" {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterBuildDataLoad", func(uint64) { pause() })
			} else {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges", pause)
			}
			if tc.rollback {
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges", `return("rollback refresh for lock test")`)
			}
			var createAttempts atomic.Int32
			var cancelled atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
				if job.Type == model.ActionCreateMaterializedViewLog && job.TableName == "$mlog$mv_mlog_refresh" {
					createAttempts.Add(1)
					cancelled.Store(job.IsCancelled())
				}
			})
			tkRefresh := testkit.NewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_mlog_refresh " + tc.mode) }()
			refreshFinished := false
			defer func() {
				release()
				if !refreshFinished {
					select {
					case <-refreshDone:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up refresh")
					}
				}
			}()
			select {
			case <-paused:
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for refresh")
			}
			ddlDone := make(chan error, 1)
			go func() { ddlDone <- tk.ExecToErr("create materialized view log on mv_mlog_refresh (a, s, cnt)") }()
			ddlFinished := false
			defer func() {
				release()
				if !ddlFinished {
					select {
					case <-ddlDone:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up CREATE MLog")
					}
				}
			}()
			select {
			case err := <-ddlDone:
				ddlFinished = true
				require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
				require.ErrorContains(t, err, "materialized view refresh is running")
				require.ErrorContains(t, err, "please retry later")
			case <-time.After(15 * time.Second):
				t.Fatal("CREATE MLog must terminate while refresh still holds its lock")
			}
			require.EqualValues(t, 1, createAttempts.Load(), "lock conflict must not retry the same job")
			require.True(t, cancelled.Load())
			tk.MustQuery("show tables like '$mlog$mv_mlog_refresh'").Check(testkit.Rows())
			release()
			select {
			case err := <-refreshDone:
				refreshFinished = true
				if tc.rollback {
					require.ErrorContains(t, err, "rollback refresh for lock test")
				} else {
					require.NoError(t, err, "OOP cutover must remain runnable after CREATE MLog is cancelled")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for refresh completion")
			}
			if tc.rollback {
				tk.MustQuery("select * from mv_mlog_refresh order by a").Check(testkit.Rows("1 10 1"))
			} else {
				tk.MustQuery("select * from mv_mlog_refresh order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
			}
			// A new user retry succeeds after refresh releases its lock, including
			// after OOP cutover has replaced the MV's table ID.
			tk.MustExec("create materialized view log on mv_mlog_refresh (a, s, cnt)")
			mv, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_mlog_refresh"))
			require.NoError(t, err)
			mlog, ok := dom.InfoSchema().TableByID(context.Background(), mv.Meta().MaterializedViewBase.MLogID)
			require.True(t, ok)
			require.Equal(t, mv.Meta().ID, mlog.Meta().MaterializedViewLog.BaseTableID)
			tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
		})
	}
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
	tk.MustExec("create materialized view log on mv (a, s, cnt)")
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
	tk.MustQuery("select * from `$mlog$mv`").Check(testkit.Rows())
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO = %d from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldTS, mviewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).
		Check(testkit.RowsWithSep("|", "failed|complete delta apply manual|1|1|1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d and REFRESH_STATUS = 'running'", mviewID)).
		Check(testkit.Rows("0"))
	reasonRow := tk.MustQuery(fmt.Sprintf("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = %d order by REFRESH_JOB_ID desc limit 1", mviewID)).Rows()
	require.Len(t, reasonRow, 1)
	require.Contains(t, fmt.Sprintf("%v", reasonRow[0][0]), "mock refresh data change failure")
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
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, 'test', 'mv', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())",
		oldMViewID,
	))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", oldMViewID)).
		Check(testkit.Rows("1"))
	nextRefresh := tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).Rows()

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
	tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(
		fmt.Sprintf("%d %d", newMViewID, baseTable.Meta().MaterializedViewBase.MLogID),
	))

	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).
		Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", oldMViewID)).
		Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO > 0 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", newMViewID)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", newMViewID)).Check(nextRefresh)
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
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_dep (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_dep (a, b)")
	tk.MustExec("create materialized view mv_dep (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_dep group by a")

	is := dom.InfoSchema()
	oldMV, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_dep"))
	require.NoError(t, err)
	oldMVID := oldMV.Meta().ID

	tk.MustExec("refresh materialized view mv_dep complete out of place")

	is = dom.InfoSchema()
	newMV, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_dep"))
	require.NoError(t, err)
	newMVID := newMV.Meta().ID
	require.NotEqual(t, oldMVID, newMVID)
	mlog, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_dep"))
	require.NoError(t, err)
	require.NotNil(t, mlog.Meta().MaterializedViewLog)
	require.Contains(t, mlog.Meta().MaterializedViewLog.DependentMViewIDs, newMVID)
	require.NotContains(t, mlog.Meta().MaterializedViewLog.DependentMViewIDs, oldMVID)

	tk.MustExec("drop materialized view mv_dep")
	tk.MustExec("drop materialized view log on t_dep")
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverFailureRollsBackRefreshInfo(t *testing.T) {
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
	oldMViewID := mvTable.Meta().ID

	originErrLimit := variable.GetDDLErrorCountLimit()
	variable.SetDDLErrorCountLimit(0)
	defer variable.SetDDLErrorCountLimit(originErrLimit)

	const cutoverAfterMigrateRefreshInfoErrorFailpoint = "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError"
	require.NoError(t, failpoint.Enable(cutoverAfterMigrateRefreshInfoErrorFailpoint, "1*return()"))
	defer func() {
		require.NoError(t, failpoint.Disable(cutoverAfterMigrateRefreshInfoErrorFailpoint))
	}()

	tk.MustExec("insert into t values (2, 3), (3, 4)")
	err = tk.ExecToErr("refresh materialized view mv complete out of place")
	require.Error(t, err)

	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)
	require.Equal(t, oldMViewID, mvTable.Meta().ID)
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
	require.Equal(t, []int64{oldMViewID}, mlogTable.Meta().MaterializedViewLog.DependentMViewIDs)
	tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(fmt.Sprintf("%d %d", oldMViewID, mlogTable.Meta().ID)))
	tk.MustQuery("select MVIEW_ID from mysql.tidb_mview_refresh_info").Check(testkit.Rows(fmt.Sprintf("%d", oldMViewID)))
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverPublishEventErrorKeepsMetadataAtomic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_cutover_publish_error (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_cutover_publish_error (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_cutover_publish_error (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_cutover_publish_error group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_cutover_publish_error"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, UPDATE_TIME) values (%d, 'test', 'mv_cutover_publish_error', 'warning', UTC_TIMESTAMP())",
		oldMViewID,
	))

	originErrLimit := variable.GetDDLErrorCountLimit()
	variable.SetDDLErrorCountLimit(0)
	defer variable.SetDDLErrorCountLimit(originErrLimit)

	const publishEventErrorFailpoint = "github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError"
	require.NoError(t, failpoint.Enable(publishEventErrorFailpoint, "1*return()"))
	defer func() { _ = failpoint.Disable(publishEventErrorFailpoint) }()
	tk.MustExec("insert into t_cutover_publish_error values (1, 10)")
	err = tk.ExecToErr("refresh materialized view mv_cutover_publish_error complete out of place")
	require.ErrorContains(t, err, "mock publish event error")
	require.NoError(t, failpoint.Disable(publishEventErrorFailpoint))

	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_cutover_publish_error"))
	require.NoError(t, err)
	require.Equal(t, oldMViewID, mvTable.Meta().ID)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", oldMViewID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = %d", oldMViewID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", oldMViewID)).Check(testkit.Rows("1"))

	tk.MustExec("refresh materialized view mv_cutover_publish_error complete out of place")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = %d", oldMViewID)).Check(testkit.Rows("0"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverKeepsTiFlashProgressUntilCommit(t *testing.T) {
	const mockTiFlashStoreCountFailpoint = "github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"
	const cutoverBeforeCommitFailpoint = "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverBeforeCommitError"
	require.NoError(t, failpoint.Enable(mockTiFlashStoreCountFailpoint, "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(mockTiFlashStoreCountFailpoint))
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		infosync.SetMockTiFlash(nil)
		tiflash.StatusServer.Close()
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_tiflash_cutover (a int not null, b int not null)")
	tk.MustExec("insert into t_tiflash_cutover values (1, 10), (2, 7)")
	tk.MustExec("create materialized view log on t_tiflash_cutover (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_tiflash_cutover (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_tiflash_cutover group by a")
	tk.MustExec("alter table mv_tiflash_cutover set tiflash replica 1")
	tk.MustExec("create materialized view log on mv_tiflash_cutover (a, s, cnt) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("alter table `$mlog$mv_tiflash_cutover` set tiflash replica 2 location labels 'zone'")

	mvTable, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_tiflash_cutover"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID
	oldMLogID := mvTable.Meta().MaterializedViewBase.MLogID
	require.NoError(t, dom.DDLExecutor().UpdateTableReplicaInfo(tk.Session(), oldMLogID, true))
	oldLog, ok := dom.InfoSchema().TableByID(context.Background(), oldMLogID)
	require.True(t, ok)
	oldReplica := *oldLog.Meta().TiFlashReplica
	require.True(t, oldReplica.Available)
	infosync.UpdateTiFlashProgressCache(oldMViewID, 0.75)
	infosync.UpdateTiFlashProgressCache(oldMLogID, 1)
	_, exists := infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.True(t, exists)

	originErrLimit := variable.GetDDLErrorCountLimit()
	variable.SetDDLErrorCountLimit(0)
	defer variable.SetDDLErrorCountLimit(originErrLimit)
	var cancelledJobID, cancelledLogID atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionMViewRefreshOutOfPlaceCutover && job.TableID == oldMViewID {
			args, err := model.GetRefreshMaterializedViewCompleteOutOfPlaceCutoverArgs(job)
			require.NoError(t, err)
			cancelledJobID.Store(job.ID)
			cancelledLogID.Store(args.NewMLogID)
		}
	})
	require.NoError(t, failpoint.Enable(cutoverBeforeCommitFailpoint, "return"))
	tk.MustExec("insert into t_tiflash_cutover values (3, 4)")
	err = tk.ExecToErr("refresh materialized view mv_tiflash_cutover complete out of place")
	require.ErrorContains(t, err, "error before commit")
	require.NoError(t, failpoint.Disable(cutoverBeforeCommitFailpoint))
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep")
	require.Positive(t, cancelledLogID.Load())
	tk.MustQuery(fmt.Sprintf("select start_key, end_key from mysql.gc_delete_range where job_id = %[1]d union all select start_key, end_key from mysql.gc_delete_range_done where job_id = %[1]d", cancelledJobID.Load())).Check(testkit.Rows(fmt.Sprintf("%x %x", []byte(tablecodec.EncodeTablePrefix(cancelledLogID.Load())), []byte(tablecodec.EncodeTablePrefix(cancelledLogID.Load()+1)))))

	_, exists = infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.True(t, exists)
	_, exists = infosync.GetTiFlashProgressFromCache(oldMLogID)
	require.True(t, exists)
	oldLog, ok = dom.InfoSchema().TableByID(context.Background(), oldMLogID)
	require.True(t, ok)
	require.Equal(t, oldReplica, *oldLog.Meta().TiFlashReplica)

	tk.MustExec("refresh materialized view mv_tiflash_cutover complete out of place")
	_, exists = infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.False(t, exists)
	newLog, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$mv_tiflash_cutover"))
	require.NoError(t, err)
	require.NotEqual(t, oldMLogID, newLog.Meta().ID)
	replica := newLog.Meta().TiFlashReplica
	require.NotNil(t, replica)
	require.Equal(t, oldReplica.Count, replica.Count)
	require.Equal(t, oldReplica.LocationLabels, replica.LocationLabels)
	t.Run("replica state", func(t *testing.T) {
		require.False(t, replica.Available)
		require.Empty(t, replica.AvailablePartitionIDs)
		require.True(t, oldLog.Meta().TiFlashReplica.Available)
	})
	t.Run("placement rule", func(t *testing.T) {
		rule, ok := tiflash.GetPlacementRule(infosync.MakeRuleID(newLog.Meta().ID))
		require.True(t, ok)
		require.Equal(t, int(replica.Count), rule.Count)
		require.Equal(t, replica.LocationLabels, rule.LocationLabels)
	})
	t.Run("old log progress", func(t *testing.T) {
		_, exists := infosync.GetTiFlashProgressFromCache(oldMLogID)
		require.False(t, exists)
	})
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

	tkUser := testkit.NewTestKit(t, store)
	tkUser.MustExec("use test")
	tkUser.Session().GetSessionVars().User = &auth.UserIdentity{AuthUsername: "test", AuthHostname: "%"}
	err := tkUser.ExecToErr(fmt.Sprintf("insert into `%s` values (9, 9, 9)", shadowTableName))
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

	// Exercise unsigned SQL persistence without using the synthetic TSO as a storage snapshot.
	buildReadTSO := (uint64(1) << 63) + 12345
	const overrideBuildTSFailpoint = "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlacePersistReadTSO"
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

func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverRevisionCASMismatch(t *testing.T) {
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
		tkRefresh := testkit.NewTestKit(t, store)
		tkRefresh.MustExec("use test")
		refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv complete out of place")
	}()

	require.Eventually(t, func() bool {
		return len(tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Rows()) == 1
	}, 30*time.Second, 100*time.Millisecond)

	tkConcurrent := testkit.NewTestKit(t, store)
	tkConcurrent.MustExec("use test")
	tkConcurrent.MustExec("create index idx_mv_s_refresh_ddl on mv (s)")

	require.NoError(t, failpoint.Disable(pauseCreateShadowFailpoint))
	enabled = false

	err = <-refreshDone
	require.Error(t, err)
	require.ErrorContains(t, err, "stale old materialized view revision detected before cutover")

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
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
	require.NotEmpty(t, tk.MustQuery("show index from mv where Key_name = 'idx_mv_s_refresh_ddl'").Rows())
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
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) attributes='mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
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

func TestMaterializedViewRefreshRequiresOperateViewPrivilege(t *testing.T) {
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

func TestCompareMaterializedViewPrivilegeSkeleton(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_priv (a int not null, b int not null)")
	tk.MustExec("insert into t_compare_priv values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_compare_priv (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_priv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_priv group by a")
	tk.MustExec("create user 'mv_compare_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'mv_compare_u'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_compare_u", Hostname: "%"}, nil, nil, nil))

	compareTS := nextCompareTimestamp(t, tk)
	compareSQL := fmt.Sprintf("compare materialized view test.mv_compare_priv as of timestamp '%s'", compareTS)
	err := tkUser.ExecToErr(compareSQL)
	require.ErrorContains(t, err, "OPERATE VIEW command denied")

	tk.MustExec("grant operate view on test.mv_compare_priv to 'mv_compare_u'@'%'")
	err = tkUser.QueryToErr(compareSQL)
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant select on test.t_compare_priv to 'mv_compare_u'@'%'")
	rows := tkUser.MustQuery(compareSQL).Rows()
	require.Len(t, rows, 1)
	require.Contains(t, fmt.Sprint(rows[0][0]), "timestamp")

	outputSQL := compareSQL + " output into table test.mv_compare_priv_diff"
	err = tkUser.ExecToErr(outputSQL)
	require.ErrorContains(t, err, "CREATE command denied")

	tk.MustExec("grant create on test.* to 'mv_compare_u'@'%'")
	err = tkUser.ExecToErr(outputSQL)
	require.ErrorContains(t, err, "INSERT command denied")

	tk.MustExec("grant insert on test.* to 'mv_compare_u'@'%'")
	tkUser.MustExec(outputSQL)
	tk.MustQuery("select count(*) from test.mv_compare_priv_diff").Check(testkit.Rows("0"))

	tk.MustExec("drop table mv_compare_priv_diff")
	tk.MustExec("create table mv_compare_priv_diff (a int)")
	err = tkUser.ExecToErr(outputSQL)
	require.ErrorContains(t, err, "Table 'test.mv_compare_priv_diff' already exists")
}

func TestCompareMaterializedViewPrivilegeCheckBeforeSnapshotResolution(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	compareTS := nextCompareTimestamp(t, tk)

	tk.MustExec("create database mv_compare_priv_snapshot")
	tk.MustExec("use mv_compare_priv_snapshot")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create user 'mv_compare_snapshot_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'mv_compare_snapshot_u'@'%'")
	tk.MustExec("grant operate view on mv_compare_priv_snapshot.mv to 'mv_compare_snapshot_u'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_compare_snapshot_u", Hostname: "%"}, nil, nil, nil))
	err := tkUser.QueryToErr(fmt.Sprintf(
		"compare materialized view mv_compare_priv_snapshot.mv as of timestamp '%s'",
		compareTS,
	))
	require.ErrorContains(t, err, "SELECT command denied")
	require.NotContains(t, err.Error(), "Unknown database")
}

func TestCompareMaterializedViewSummaryAndOutput(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("create table t_compare_diff (a int not null, b int not null)")
	tk.MustExec("insert into t_compare_diff values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_compare_diff (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_diff (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_diff group by a")

	lastSuccessRows := tk.MustQuery(
		"select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_diff')",
	).Rows()
	lastSuccessReadTSO, err := strconv.ParseUint(fmt.Sprint(lastSuccessRows[0][0]), 10, 64)
	require.NoError(t, err)

	origInMVMaintenance := tk.Session().GetSessionVars().InMaterializedViewMaintenance
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = true
	mustExecInternal(t, tk, "update mv_compare_diff set s = s + 7 where a = 1")
	mustExecInternal(t, tk, "delete from mv_compare_diff where a = 2")
	mustExecInternal(t, tk, "insert into mv_compare_diff values (3, 30, 1)")
	mustExecInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = origInMVMaintenance
	tk.MustQuery("select a, s, cnt from mv_compare_diff order by a").Check(testkit.Rows(
		"1 17 1",
		"3 30 1",
	))

	compareTS := nextCompareTimestamp(t, tk)
	compareSQL := fmt.Sprintf(
		"compare materialized view test.mv_compare_diff as of timestamp '%s'",
		compareTS,
	)

	summaryRows := tk.MustQuery(compareSQL).Rows()
	require.Len(t, summaryRows, 1)
	summary := fmt.Sprint(summaryRows[0][0])
	expectedTimestamp := oracle.GetTimeFromTS(lastSuccessReadTSO).In(tk.Session().GetSessionVars().Location()).Format("2006-01-02 15:04:05.000000 -07:00")
	require.Equal(
		t,
		fmt.Sprintf(
			"There are differences result in 3 rows compared to source base tables at timestamp '%s' (TSO: %d)",
			expectedTimestamp,
			lastSuccessReadTSO,
		),
		summary,
	)

	tk.MustExec(compareSQL + " output into table test.mv_compare_diff_output")
	tk.MustQuery("select a, s, cnt, _Differ_type_ from test.mv_compare_diff_output order by a").
		Check(testkit.Rows(
			"1 17 1 mview_differ",
			"2 20 1 mview_vacuum",
			"3 30 1 mview_excessive",
		))
}

func TestCompareMaterializedViewUsesSnapshotMetadataAfterOutOfPlaceCutover(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_oop_snapshot (a int not null, b int not null)")
	tk.MustExec("insert into t_compare_oop_snapshot values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_compare_oop_snapshot (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_oop_snapshot (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_oop_snapshot group by a")

	compareTS := nextCompareTimestamp(t, tk)
	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_compare_oop_snapshot"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID

	tk.MustExec("insert into t_compare_oop_snapshot values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv_compare_oop_snapshot complete out of place")

	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_compare_oop_snapshot"))
	require.NoError(t, err)
	require.NotEqual(t, oldMViewID, mvTable.Meta().ID)
	tk.MustExec("create index idx_current_s on mv_compare_oop_snapshot (s)")
	require.NotEmpty(t, tk.MustQuery("show index from mv_compare_oop_snapshot").Rows())

	compareSQL := fmt.Sprintf("compare materialized view test.mv_compare_oop_snapshot as of timestamp '%s'", compareTS)
	rows := tk.MustQuery(compareSQL).Rows()
	require.Len(t, rows, 1)
	require.Contains(t, fmt.Sprint(rows[0][0]), "0 rows")

	tk.MustExec(compareSQL + " output into table test.mv_compare_oop_snapshot_diff")
	tk.MustQuery("show index from mv_compare_oop_snapshot_diff").Check(testkit.Rows())
}

func TestCompareMaterializedViewOutputWriterBatchesByTxnSize(t *testing.T) {
	const rowCount = 900

	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_output_batch (a int not null, c varchar(1700) not null)")
	tk.MustExec("create index idx_a on t_compare_output_batch (a)")
	for start := 0; start < rowCount; start += 100 {
		var sb strings.Builder
		sb.WriteString("insert into t_compare_output_batch values ")
		end := min(start+100, rowCount)
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteString(", ")
			}
			val := fmt.Sprintf("%04d_%s", i, strings.Repeat("x", 1500))
			fmt.Fprintf(&sb, "(%d, '%s')", i, val)
		}
		tk.MustExec(sb.String())
	}
	tk.MustExec("create materialized view log on t_compare_output_batch (a, c) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_output_batch (a, mx, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, max(c), count(1) from t_compare_output_batch group by a")

	origInMVMaintenance := tk.Session().GetSessionVars().InMaterializedViewMaintenance
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = true
	mustExecInternal(t, tk, "delete from mv_compare_output_batch")
	mustExecInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = origInMVMaintenance

	origTxnLimit := kv.TxnTotalSizeLimit.Load()
	kv.TxnTotalSizeLimit.Store(1024 * 1024)
	defer kv.TxnTotalSizeLimit.Store(origTxnLimit)

	compareSQL := fmt.Sprintf(
		"compare materialized view test.mv_compare_output_batch as of timestamp '%s' output into table test.mv_compare_output_batch_diff",
		nextCompareTimestamp(t, tk),
	)
	tk.MustExec(compareSQL)
	tk.MustQuery("select count(*) from test.mv_compare_output_batch_diff").Check(testkit.Rows(fmt.Sprint(rowCount)))
}

func TestCompareMaterializedViewDropsOutputTableOnWriteFailure(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_output_cleanup (a int not null, b int not null)")
	tk.MustExec("insert into t_compare_output_cleanup values (1, 10)")
	tk.MustExec("create materialized view log on t_compare_output_cleanup (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_output_cleanup (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_output_cleanup group by a")

	origInMVMaintenance := tk.Session().GetSessionVars().InMaterializedViewMaintenance
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = true
	mustExecInternal(t, tk, "update mv_compare_output_cleanup set s = s + 1 where a = 1")
	mustExecInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = origInMVMaintenance

	failpointName := "github.com/pingcap/tidb/pkg/executor/mockCompareMaterializedViewOutputFlushError"
	require.NoError(t, failpoint.Enable(failpointName, `return("mock compare output flush error")`))
	defer func() {
		require.NoError(t, failpoint.Disable(failpointName))
	}()

	compareSQL := fmt.Sprintf(
		"compare materialized view test.mv_compare_output_cleanup as of timestamp '%s' output into table test.mv_compare_output_cleanup_diff",
		nextCompareTimestamp(t, tk),
	)
	err := tk.ExecToErr(compareSQL)
	require.ErrorContains(t, err, "mock compare output flush error")
	tk.MustQuery("select count(*) from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_output_cleanup_diff'").
		Check(testkit.Rows("0"))
}

func TestCompareMaterializedViewUsesDefinitionTimeZone(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.time_zone = '+00:00'")
	tk.MustExec("create table t_compare_tz (a int not null, ts timestamp not null, b int not null)")
	tk.MustExec("insert into t_compare_tz values (1, '2025-12-31 23:30:00', 20), (1, '2026-01-01 00:30:00', 10)")
	tk.MustExec("create materialized view log on t_compare_tz (a, ts, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_tz (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_tz where ts >= '2026-01-01 00:00:00' group by a")
	tk.MustQuery("select a, s, cnt from mv_compare_tz").Check(testkit.Rows("1 10 1"))

	tk.MustExec("set @@session.time_zone = '+08:00'")
	compareSQL := fmt.Sprintf(
		"compare materialized view test.mv_compare_tz as of timestamp '%s'",
		nextCompareTimestamp(t, tk),
	)
	rows := tk.MustQuery(compareSQL).Rows()
	require.Len(t, rows, 1)
	require.Contains(t, fmt.Sprint(rows[0][0]), "0 rows")
}

func TestCompareMaterializedViewOutputTimestampUsesDefinitionTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.time_zone = '+08:00'")
	tk.MustExec("create table t_compare_ts_output (ts timestamp not null, dt datetime not null, v int not null)")
	tk.MustExec("insert into t_compare_ts_output values ('2026-01-01 08:00:00', '2026-01-01 08:00:00', 10), ('2026-01-02 08:00:00', '2026-01-02 08:00:00', 20)")
	tk.MustExec("create materialized view log on t_compare_ts_output (ts, dt, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_ts_output (ts, dt, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select ts, dt, sum(v), count(1) from t_compare_ts_output group by ts, dt")
	origMaintenance := tk.Session().GetSessionVars().InMaterializedViewMaintenance
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = true
	mustExecInternal(t, tk, "update mv_compare_ts_output set s = 99 where ts = '2026-01-01 08:00:00'")
	mustExecInternal(t, tk, "delete from mv_compare_ts_output where ts = '2026-01-02 08:00:00'")
	mustExecInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = origMaintenance
	tk.MustExec("set @@session.time_zone = '+00:00'")

	// Seed enough pooled sessions for COMPARE with a timezone distinct from the MV definition.
	const poolSessionCount = 16
	pool := dom.SysSessionPool()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMVMaintenance)
	func() {
		var releases []func()
		defer func() {
			for _, release := range releases {
				release()
			}
		}()
		for range poolSessionCount {
			resource, err := pool.Get()
			require.NoError(t, err)
			releases = append(releases, func() { pool.Put(resource) })
			sctx := resource.(sessionctx.Context)
			for _, sql := range []string{"set @@session.time_zone = '+00:00'", "rollback"} {
				rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, sql)
				require.NoError(t, err)
				require.Nil(t, rs)
			}
		}
	}()
	checkPoolTimeZones := func() {
		for range poolSessionCount {
			func() {
				resource, err := pool.Get()
				require.NoError(t, err)
				defer pool.Put(resource)
				vars := resource.(sessionctx.Context).GetSessionVars()
				_, sessionOffset := time.Now().In(vars.Location()).Zone()
				_, stmtOffset := time.Now().In(vars.StmtCtx.TimeZone()).Zone()
				require.Zero(t, sessionOffset)
				require.Zero(t, stmtOffset)
			}()
		}
	}

	compareSQL := fmt.Sprintf("compare materialized view test.mv_compare_ts_output as of timestamp '%s'", nextCompareTimestamp(t, tk))
	tk.MustExec(compareSQL + " output into table test.mv_compare_ts_output_diff")
	// Both current-MV and recomputed rows must preserve their TIMESTAMP instants.
	tk.MustQuery("select ts, dt, s, cnt, _Differ_type_ from mv_compare_ts_output_diff order by ts").Check(testkit.Rows(
		"2026-01-01 00:00:00 2026-01-01 08:00:00 99 1 mview_differ",
		"2026-01-02 00:00:00 2026-01-02 08:00:00 20 1 mview_vacuum",
	))
	checkPoolTimeZones()

	failpointName := "github.com/pingcap/tidb/pkg/executor/mockCompareMaterializedViewOutputFlushError"
	require.NoError(t, failpoint.Enable(failpointName, `return("mock compare output flush error")`))
	defer func() {
		require.NoError(t, failpoint.Disable(failpointName))
	}()
	err := tk.ExecToErr(compareSQL + " output into table test.mv_compare_ts_output_error")
	require.ErrorContains(t, err, "mock compare output flush error")
	tk.MustQuery("select count(*) from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_ts_output_error'").Check(testkit.Rows("0"))
	checkPoolTimeZones()
}

func TestCompareMaterializedViewNullableGroupKeyMinMax(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_nullable_minmax (a int, b int)")
	tk.MustExec("create index idx_a_b on t_compare_nullable_minmax (a, b)")
	tk.MustExec("insert into t_compare_nullable_minmax values (null, 1), (null, 3), (1, 5)")
	tk.MustExec("create materialized view log on t_compare_nullable_minmax (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_nullable_minmax (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_compare_nullable_minmax group by a")

	origInMVMaintenance := tk.Session().GetSessionVars().InMaterializedViewMaintenance
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = true
	mustExecInternal(t, tk, "update mv_compare_nullable_minmax set mx = 99 where a is null")
	mustExecInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMaterializedViewMaintenance = origInMVMaintenance

	compareSQL := fmt.Sprintf(
		"compare materialized view test.mv_compare_nullable_minmax as of timestamp '%s'",
		nextCompareTimestamp(t, tk),
	)
	summaryRows := tk.MustQuery(compareSQL).Rows()
	require.Len(t, summaryRows, 1)
	require.Contains(t, fmt.Sprint(summaryRows[0][0]), "1 rows")

	tk.MustExec(compareSQL + " output into table test.mv_compare_nullable_minmax_output")
	tk.MustQuery("select a, cnt, mx, mn, _Differ_type_ from test.mv_compare_nullable_minmax_output").
		Check(testkit.Rows("<nil> 2 99 1 mview_differ"))
}

func TestCompareMaterializedViewEmptyInputs(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_empty (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_compare_empty (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_empty (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_empty group by a")

	compareSQL := fmt.Sprintf(
		"compare materialized view test.mv_compare_empty as of timestamp '%s'",
		nextCompareTimestamp(t, tk),
	)
	summaryRows := tk.MustQuery(compareSQL).Rows()
	require.Len(t, summaryRows, 1)
	require.Contains(t, fmt.Sprint(summaryRows[0][0]), "0 rows")

	tk.MustExec(compareSQL + " output into table test.mv_compare_empty_output")
	tk.MustQuery("select count(*) from test.mv_compare_empty_output").Check(testkit.Rows("0"))
}

func TestCompareMaterializedViewRejectsReadTSOAfterSnapshot(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.time_zone = '+00:00'")
	tk.MustExec("create table t_compare_tso_guard (a int not null, b int not null)")
	tk.MustExec("insert into t_compare_tso_guard values (1, 10)")
	tk.MustExec("create materialized view log on t_compare_tso_guard (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_tso_guard (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_tso_guard group by a")

	futureTSO := oracle.GoTimeToTS(time.Now().UTC().Add(time.Hour))
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = %d where MVIEW_ID = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_compare_tso_guard')",
		futureTSO,
	))

	compareTS := nextCompareTimestamp(t, tk)
	err := tk.QueryToErr(fmt.Sprintf("compare materialized view test.mv_compare_tso_guard as of timestamp '%s'", compareTS))
	require.ErrorContains(t, err, "LAST_SUCCESS_READ_TSO")
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
		tkRefresh := testkit.NewTestKit(t, store)
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
		tkRefresh := testkit.NewTestKit(t, store)
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

	tkCancel := testkit.NewTestKit(t, store)
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
}

func TestCancelMaterializedViewJobNotRunning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	err := tk.ExecToErr("cancel materialized view refresh job 1")
	require.ErrorContains(t, err, "cannot cancel materialized view refresh job 1")
	err = tk.ExecToErr("cancel materialized view log purge job 1")
	require.ErrorContains(t, err, "cannot cancel materialized view log purge job 1")
}

func TestMaterializedViewRefreshCancelWatcherStopsAfterTaskFinish(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
}

func TestMaterializedViewRefreshCompleteOutOfPlaceOutputMLogRollbackAndRetry(t *testing.T) {
	for _, point := range []string{
		"mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError",
		"mockMViewRefreshOutOfPlaceCutoverAfterMetadataError",
	} {
		t.Run(point, func(t *testing.T) {
			_, tk, oldID, _ := newMViewRefreshSourceFixture(t)
			tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
			tk.MustExec("create materialized view mv_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_source_init group by a")
			beforeInfo := tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows()
			beforeSources := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
			beforePurge := tk.MustQuery("select * from mysql.tidb_mlog_purge_info order by MLOG_ID").Rows()
			beforeTables := tk.MustQuery("select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME").Rows()
			tk.MustExec("insert into t_source_init values (3, 30)")
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/"+point, "1*return()")
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
				if job.Type == model.ActionMViewRefreshOutOfPlaceCutover && job.TableID == oldID && job.Error != nil {
					job.State = model.JobStateCancelled
				}
			})
			require.Error(t, tk.ExecToErr("refresh materialized view mv_source_init complete out of place"))
			require.Equal(t, beforeInfo, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
			require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
			require.Equal(t, beforePurge, tk.MustQuery("select * from mysql.tidb_mlog_purge_info order by MLOG_ID").Rows())
			require.Equal(t, beforeTables, tk.MustQuery("select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME").Rows())
			tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
			tk.MustExec("refresh materialized view mv_child fast")
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/onJobRunAfter"))

			// An internal retry must reuse the log ID allocated in the persisted job.
			var allocated atomic.Int64
			var attempts atomic.Int64
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/"+point, "1*return()")
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
				if job.Type != model.ActionMViewRefreshOutOfPlaceCutover || job.TableID != oldID || job.IsDone() {
					return
				}
				args, err := model.GetRefreshMaterializedViewCompleteOutOfPlaceCutoverArgs(job)
				require.NoError(t, err)
				require.Positive(t, args.NewMLogID)
				allocated.CompareAndSwap(0, args.NewMLogID)
				require.Equal(t, allocated.Load(), args.NewMLogID)
				attempts.Add(1)
			})
			tk.MustExec("refresh materialized view mv_source_init complete out of place")
			require.GreaterOrEqual(t, attempts.Load(), int64(2))
			tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='$mlog$mv_source_init'").Check(testkit.Rows(fmt.Sprint(allocated.Load())))
			tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
		})
	}
}

func TestMaterializedViewRefreshCompleteOutOfPlacePurgeConcurrency(t *testing.T) {
	for _, stage := range []string{"running", "starting", "resolved", "timeout"} {
		t.Run(stage, func(t *testing.T) {
			store, tk, oldMV, _ := newMViewRefreshSourceFixture(t)
			tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
			tk.MustExec("insert into t_source_init values (3, 30)")
			tk.MustExec("refresh materialized view mv_source_init fast")
			oldLog := tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='$mlog$mv_source_init'").Rows()[0][0]
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(10)")
			var beforeInfo, beforeSources, beforePurge, beforeTables [][]any
			var cutoverAttempts atomic.Int32
			if stage == "timeout" {
				beforeInfo = tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows()
				beforeSources = tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
				beforePurge = tk.MustQuery("select * from mysql.tidb_mlog_purge_info order by MLOG_ID").Rows()
				beforeTables = tk.MustQuery("select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME").Rows()
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMViewOutOfPlacePurgeAdvisoryLockTimeout", "return(1000)")
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
					if job.Type == model.ActionMViewRefreshOutOfPlaceCutover && job.TableID == oldMV {
						cutoverAttempts.Add(1)
					}
				})
			}

			entered, release := make(chan struct{}), make(chan struct{})
			running, releaseRunning := make(chan struct{}), make(chan struct{})
			var paused, released, runningPaused, runningReleased atomic.Bool
			unblock := func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
			}
			unblockRunning := func() {
				if runningReleased.CompareAndSwap(false, true) {
					close(releaseRunning)
				}
			}
			defer unblock()
			defer unblockRunning()
			waitFor := func(ch <-chan struct{}, what string) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(10 * time.Second):
					t.Fatalf("timeout waiting for %s", what)
				}
			}
			pause := func(int64, int64) {
				if paused.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
			}
			switch stage {
			case "starting":
				// The advisory lock is held, but no running history exists yet.
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeDeleteSession", pause)
			case "resolved":
				// Old metadata/MDL is held before advisory lock acquisition.
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeSession", pause)
			}
			if stage != "resolved" {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/purgeMaterializedViewLogDeleteSQL", func(string) {
					if stage == "running" || stage == "timeout" {
						pause(0, 0)
					} else if runningPaused.CompareAndSwap(false, true) {
						close(running)
						<-releaseRunning
					}
				})
			}
			requested := waitMVTaskCancelWatcherRequested(t, "mlog-purge-")
			retried := make(chan struct{})
			var retriedOnce atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mViewOutOfPlacePurgeAdvisoryLockRetry", func(int64) {
				if retriedOnce.CompareAndSwap(false, true) {
					close(retried)
				}
			})
			var retiring atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mViewOutOfPlaceBeforeRetireMLog", func(int64) {
				retiring.Store(true)
			})

			purgeDone, cutoverDone := make(chan error, 1), make(chan error, 1)
			purgeFinished, cutoverFinished := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(purgeFinished)
				purger := testkit.NewTestKit(t, store)
				purger.MustExec("use test")
				purgeDone <- purger.ExecToErr("purge materialized view log on mv_source_init")
			}()
			waitFor(entered, "purge pause point")
			go func() {
				defer close(cutoverFinished)
				refresher := testkit.NewTestKit(t, store)
				refresher.MustExec("use test")
				cutoverDone <- refresher.ExecToErr("refresh materialized view mv_source_init complete out of place")
			}()
			var otherFinished <-chan struct{}
			defer func() {
				unblock()
				unblockRunning()
				waitFor(purgeFinished, "purge cleanup")
				waitFor(cutoverFinished, "cutover cleanup")
				if otherFinished != nil {
					waitFor(otherFinished, "unrelated DDL cleanup")
				}
			}()

			if stage == "resolved" {
				// Cutover commits, then waits for this statement's old MDL.
				// Purge must fail instead of waiting for the advisory lock.
				require.Eventually(t, func() bool {
					id := tk.MustQueryWithContext(context.Background(), "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_source_init'").Rows()[0][0]
					return fmt.Sprint(id) != fmt.Sprint(oldMV)
				}, 10*time.Second, 10*time.Millisecond)
				unblock()
				waitFor(purgeFinished, "stale purge")
				err := <-purgeDone
				require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
			} else {
				if stage == "starting" {
					waitFor(retried, "cutover retry before running history exists")
					tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID=%v and PURGE_STATUS='running'", oldLog)).Check(testkit.Rows("0"))
					unblock()
					waitFor(running, "purge running history")
				}
				waitFor(requested, "cutover cancellation observed by purge")
				require.False(t, retiring.Load(), "cutover must wait before mutating runtime metadata")
				if stage == "timeout" {
					// Purge still holds the lock after observing cancellation. The
					// total budget must fail the job instead of restarting DDL retries.
					waitFor(cutoverFinished, "cutover purge-lock timeout")
					err := <-cutoverDone
					require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
					require.ErrorContains(t, err, "timed out waiting for materialized view log purge")
					require.Equal(t, int32(1), cutoverAttempts.Load())
					require.False(t, retiring.Load())
					require.Equal(t, beforeInfo, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
					require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
					require.Equal(t, beforePurge, tk.MustQuery("select * from mysql.tidb_mlog_purge_info order by MLOG_ID").Rows())
					require.Equal(t, beforeTables, tk.MustQuery("select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME").Rows())
					tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
					unblock()
					waitFor(purgeFinished, "cancelled purge after cutover timeout")
					require.ErrorContains(t, <-purgeDone, "materialized view task canceled manually")
					require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockMViewOutOfPlacePurgeAdvisoryLockTimeout"))
					tk.MustExec("refresh materialized view mv_source_init complete out of place")
					tk.MustExec("purge materialized view log on mv_source_init")
					return
				}

				// A cancelled purge may still be finishing a DELETE batch.
				// Waiting must not hold the shared schema-version lock.
				otherDone, otherExited := make(chan error, 1), make(chan struct{})
				otherFinished = otherExited
				go func() {
					defer close(otherExited)
					other := testkit.NewTestKit(t, store)
					other.MustExec("use test")
					otherDone <- other.ExecToErr("create table t_unrelated_to_purge (a int)")
				}()
				waitFor(otherExited, "unrelated DDL while cutover waits")
				require.NoError(t, <-otherDone)
				tk.MustQueryWithContext(context.Background(), "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_source_init'").Check(testkit.Rows(fmt.Sprint(oldMV)))
				unblock()
				unblockRunning()
				waitFor(purgeFinished, "cancelled purge")
				require.ErrorContains(t, <-purgeDone, "materialized view task canceled manually")
			}
			waitFor(cutoverFinished, "cutover after purge exits")
			require.NoError(t, <-cutoverDone)
			tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID=%v", oldLog)).Check(testkit.Rows("0"))
			tk.MustExec("insert into t_source_init values (4, 40)")
			tk.MustExec("refresh materialized view mv_source_init fast")
			tk.MustQuery("select count(*) > 0 from `$mlog$mv_source_init`").Check(testkit.Rows("1"))
			tk.MustExec("purge materialized view log on mv_source_init")
		})
	}
}

func TestMaterializedViewRefreshCompleteOutOfPlaceConcurrentChildFast(t *testing.T) {
	store, tk, oldMV, _ := newMViewRefreshSourceFixture(t)
	tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
	tk.MustExec("create materialized view mv_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_source_init group by a")
	tk.MustExec("insert into t_source_init values (3, 30)")
	before := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
	entered, release := make(chan struct{}), make(chan struct{})
	var paused, released atomic.Bool
	unblock := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer unblock()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInsertRefreshHistRunning", func() {
		if paused.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	})
	childDone := make(chan error, 1)
	go func() {
		child := testkit.NewTestKit(t, store)
		child.MustExec("use test")
		childDone <- child.ExecToErr("refresh materialized view mv_child fast")
	}()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("child FAST did not reach its data statement")
	}
	parentDone := make(chan error, 1)
	go func() {
		parent := testkit.NewTestKit(t, store)
		parent.MustExec("use test")
		parentDone <- parent.ExecToErr("refresh materialized view mv_source_init complete out of place")
	}()
	require.Eventually(t, func() bool {
		id := tk.MustQueryWithContext(context.Background(), "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_source_init'").Rows()[0][0]
		return fmt.Sprint(id) != fmt.Sprint(oldMV)
	}, 30*time.Second, 10*time.Millisecond)
	unblock()
	select {
	case err := <-childDone:
		require.Error(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("child FAST did not finish after parent cutover")
	}
	select {
	case err := <-parentDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("parent OOP did not finish after child FAST")
	}
	tk.MustQuery("select * from mv_child order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1"))
	// The parent's registration moves to its new ID; the child keeps its old
	// input log registration until COMPLETE succeeds.
	tk.MustQuery("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID=(select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_child')").Check(testkit.Rows(fmt.Sprint(before[1][1])))
	tk.MustExec("refresh materialized view mv_child complete delta apply")
	tk.MustExec("refresh materialized view mv_child fast")
	tk.MustQuery("select * from mv_child order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1", "3 30 1 1"))
}
