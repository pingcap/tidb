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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestCompareMaterializedView(t *testing.T) {
	t.Run("PrivilegeSkeleton", testCompareMaterializedViewPrivilegeSkeleton)
	t.Run("PrivilegeCheckBeforeSnapshotResolution", testCompareMaterializedViewPrivilegeCheckBeforeSnapshotResolution)
	t.Run("SummaryAndOutput", testCompareMaterializedViewSummaryAndOutput)
	t.Run("UsesSnapshotMetadataAfterOutOfPlaceCutover", testCompareMaterializedViewUsesSnapshotMetadataAfterOutOfPlaceCutover)
	t.Run("OutputWriterBatchesByTxnSize", testCompareMaterializedViewOutputWriterBatchesByTxnSize)
	t.Run("DropsOutputTableOnWriteFailure", testCompareMaterializedViewDropsOutputTableOnWriteFailure)
	t.Run("UsesDefinitionTimeZone", testCompareMaterializedViewUsesDefinitionTimeZone)
	t.Run("OutputTimestampUsesDefinitionTimeZone", testCompareMaterializedViewOutputTimestampUsesDefinitionTimeZone)
	t.Run("NullableGroupKeyMinMax", testCompareMaterializedViewNullableGroupKeyMinMax)
	t.Run("EmptyInputs", testCompareMaterializedViewEmptyInputs)
	t.Run("RejectsReadTSOAfterSnapshot", testCompareMaterializedViewRejectsReadTSOAfterSnapshot)
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

func testCompareMaterializedViewPrivilegeSkeleton(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_priv (a int not null, b int not null)")
	tk.MustExec("insert into t_compare_priv values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_compare_priv (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_priv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_priv group by a")
	tk.MustExec("create user 'mv_compare_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'mv_compare_u'@'%'")

	tkUser := newMViewTestKit(t, store)
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

func testCompareMaterializedViewPrivilegeCheckBeforeSnapshotResolution(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
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

	tkUser := newMViewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_compare_snapshot_u", Hostname: "%"}, nil, nil, nil))
	err := tkUser.QueryToErr(fmt.Sprintf(
		"compare materialized view mv_compare_priv_snapshot.mv as of timestamp '%s'",
		compareTS,
	))
	require.ErrorContains(t, err, "SELECT command denied")
	require.NotContains(t, err.Error(), "Unknown database")
}

func testCompareMaterializedViewSummaryAndOutput(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
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

	origInMVMaintenance := tk.Session().GetSessionVars().InMViewMaintenance
	tk.Session().GetSessionVars().InMViewMaintenance = true
	mustExecRefreshInternal(t, tk, "update mv_compare_diff set s = s + 7 where a = 1")
	mustExecRefreshInternal(t, tk, "delete from mv_compare_diff where a = 2")
	mustExecRefreshInternal(t, tk, "insert into mv_compare_diff values (3, 30, 1)")
	mustExecRefreshInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMViewMaintenance = origInMVMaintenance
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

func testCompareMaterializedViewUsesSnapshotMetadataAfterOutOfPlaceCutover(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_oop_snapshot (a int not null, b int not null)")
	tk.MustExec("insert into t_compare_oop_snapshot values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_compare_oop_snapshot (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_oop_snapshot (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_oop_snapshot group by a")

	compareTS := nextCompareTimestamp(t, tk)
	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_compare_oop_snapshot"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID

	tk.MustExec("insert into t_compare_oop_snapshot values (2, 3), (3, 4)")
	tk.MustExec("refresh materialized view mv_compare_oop_snapshot complete out of place")

	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_compare_oop_snapshot"))
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

func testCompareMaterializedViewOutputWriterBatchesByTxnSize(t *testing.T) {
	const rowCount = 900

	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
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

	origInMVMaintenance := tk.Session().GetSessionVars().InMViewMaintenance
	tk.Session().GetSessionVars().InMViewMaintenance = true
	mustExecRefreshInternal(t, tk, "delete from mv_compare_output_batch")
	mustExecRefreshInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMViewMaintenance = origInMVMaintenance

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

func testCompareMaterializedViewDropsOutputTableOnWriteFailure(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_output_cleanup (a int not null, b int not null)")
	tk.MustExec("insert into t_compare_output_cleanup values (1, 10)")
	tk.MustExec("create materialized view log on t_compare_output_cleanup (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_output_cleanup (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_compare_output_cleanup group by a")

	origInMVMaintenance := tk.Session().GetSessionVars().InMViewMaintenance
	tk.Session().GetSessionVars().InMViewMaintenance = true
	mustExecRefreshInternal(t, tk, "update mv_compare_output_cleanup set s = s + 1 where a = 1")
	mustExecRefreshInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMViewMaintenance = origInMVMaintenance

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

func testCompareMaterializedViewUsesDefinitionTimeZone(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
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

func testCompareMaterializedViewOutputTimestampUsesDefinitionTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.time_zone = '+08:00'")
	tk.MustExec("create table t_compare_ts_output (ts timestamp not null, dt datetime not null, v int not null)")
	tk.MustExec("insert into t_compare_ts_output values ('2026-01-01 08:00:00', '2026-01-01 08:00:00', 10), ('2026-01-02 08:00:00', '2026-01-02 08:00:00', 20)")
	tk.MustExec("create materialized view log on t_compare_ts_output (ts, dt, v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_ts_output (ts, dt, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select ts, dt, sum(v), count(1) from t_compare_ts_output group by ts, dt")
	mustExecRefreshInternal(t, tk, "update mv_compare_ts_output set s = 99 where ts = '2026-01-01 08:00:00'")
	mustExecRefreshInternal(t, tk, "delete from mv_compare_ts_output where ts = '2026-01-02 08:00:00'")
	mustExecRefreshInternal(t, tk, "commit")
	tk.MustExec("set @@session.time_zone = '+00:00'")

	// Seed enough pooled sessions for COMPARE with a timezone distinct from the MV definition.
	const poolSessionCount = 16
	pool := dom.SysSessionPool()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
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

func testCompareMaterializedViewNullableGroupKeyMinMax(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compare_nullable_minmax (a int, b int)")
	tk.MustExec("create index idx_a_b on t_compare_nullable_minmax (a, b)")
	tk.MustExec("insert into t_compare_nullable_minmax values (null, 1), (null, 3), (1, 5)")
	tk.MustExec("create materialized view log on t_compare_nullable_minmax (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_compare_nullable_minmax (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_compare_nullable_minmax group by a")

	origInMVMaintenance := tk.Session().GetSessionVars().InMViewMaintenance
	tk.Session().GetSessionVars().InMViewMaintenance = true
	mustExecRefreshInternal(t, tk, "update mv_compare_nullable_minmax set mx = 99 where a is null")
	mustExecRefreshInternal(t, tk, "commit")
	tk.Session().GetSessionVars().InMViewMaintenance = origInMVMaintenance

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

func testCompareMaterializedViewEmptyInputs(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
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

func testCompareMaterializedViewRejectsReadTSOAfterSnapshot(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
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
