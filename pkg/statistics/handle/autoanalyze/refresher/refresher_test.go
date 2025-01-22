// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package refresher_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/refresher"
	"github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestChangePruneMode(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (140))")
	tk.MustExec("insert into t1 values (0, 0)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	tk.MustExec("analyze table t1")
	r := refresher.NewRefresher(handle, dom.SysProcTracker(), dom.DDLNotifier())
	defer r.Close()

	// Insert more data to each partition.
	tk.MustExec("insert into t1 values (1, 1), (11, 11)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))

	// Two jobs are added because the prune mode is static.
	tk.MustExec("set global tidb_partition_prune_mode = 'static'")
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, handle.HandleAutoAnalyze())
		return nil
	}))
	r.WaitAutoAnalyzeFinishedForTest()
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	r.WaitAutoAnalyzeFinishedForTest()
	require.Equal(t, 0, r.Len())

	// Insert more data to each partition.
	tk.MustExec("insert into t1 values (2, 2), (3, 3), (4, 4), (12, 12), (13, 13), (14, 14)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))

	// One job is added because the prune mode is dynamic.
	tk.MustExec("set global tidb_partition_prune_mode = 'dynamic'")
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	r.WaitAutoAnalyzeFinishedForTest()
	require.Equal(t, 0, r.Len())
}

func TestSkipAnalyzeTableWhenAutoAnalyzeRatioIsZero(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) " +
		"partition by range (a) " +
		"(partition p0 values less than (2), " +
		"partition p1 values less than (4), " +
		"partition p2 values less than (16))",
	)

	tk.MustExec("create table t2 (a int, b int, index idx(a)) " +
		"partition by range (a) " +
		"(partition p0 values less than (2), " +
		"partition p1 values less than (4), " +
		"partition p2 values less than (16))",
	)
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")
	// HACK: Set the auto analyze ratio to 0.
	// We don't allow users to set the ratio to 0 anymore, but we still need to test this case.
	// Because we need to compilable with the old configuration.
	tk.MustExec("update mysql.global_variables set variable_value = '0' where variable_name = 'tidb_auto_analyze_ratio'")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Analyze those tables first.
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	// Insert more data into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker, dom.DDLNotifier())
	defer r.Close()
	// No jobs are added.
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.False(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	require.Equal(t, 0, r.Len())
	// Enable the auto analyze.
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.2")
	// Jobs are added.
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	require.Equal(t, 0, r.Len())
}

func TestIgnoreNilOrPseudoStatsOfPartitionedTable(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("create table t2 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")
	handle := dom.StatsHandle()
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker, dom.DDLNotifier())
	defer r.Close()
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.False(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	require.Equal(t, 0, r.Len(), "No jobs are added because table stats are nil")
}

func TestIgnoreNilOrPseudoStatsOfNonPartitionedTable(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("create table t2 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")
	handle := dom.StatsHandle()
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker, dom.DDLNotifier())
	defer r.Close()
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.False(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	require.Equal(t, 0, r.Len(), "No jobs are added because table stats are nil")
}

func TestIgnoreTinyTable(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 10
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("create table t2 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Analyze those tables first.
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Make sure table stats are not pseudo.
	tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.False(t, tblStats1.Pseudo)
	tbl2, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.False(t, tblStats2.Pseudo)

	// Insert more data into t1 and t2, but more data is inserted into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13)")
	tk.MustExec("insert into t2 values (4, 4)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker, dom.DDLNotifier())
	defer r.Close()
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	require.Equal(t, 0, r.Len(), "Only t1 is added to the job queue, because t2 is a tiny table(not enough data)")
}

func TestAnalyzeHighestPriorityTables(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_auto_analyze=true")
	tk.MustExec("set global tidb_auto_analyze_concurrency=1")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("create table t2 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Analyze those tables first.
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Insert more data into t1 and t2, but more data is inserted into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13)")
	tk.MustExec("insert into t2 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker, dom.DDLNotifier())
	defer r.Close()
	// Analyze t1 first.
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	r.WaitAutoAnalyzeFinishedForTest()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// The table is analyzed.
	tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.Equal(t, int64(0), tblStats1.ModifyCount)
	require.Equal(t, int64(12), tblStats1.RealtimeCount)
	// t2 is not analyzed.
	tbl2, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.Equal(t, int64(6), tblStats2.ModifyCount)
	// Do one more round.
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	r.WaitAutoAnalyzeFinishedForTest()
	// t2 is analyzed.
	pid2 = tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 = handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.Equal(t, int64(0), tblStats2.ModifyCount)
	require.Equal(t, int64(8), tblStats2.RealtimeCount)
}

func TestAnalyzeHighestPriorityTablesConcurrently(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_auto_analyze=true")
	tk.MustExec("set global tidb_auto_analyze_concurrency=2")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("create table t2 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("create table t3 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t3 values (1, 1), (2, 2), (3, 3)")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Analyze those tables first.
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	tk.MustExec("analyze table t3")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Insert more data into t1, t2, and t3, with different amounts of new data.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13)")
	tk.MustExec("insert into t2 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)")
	tk.MustExec("insert into t3 values (4, 4), (5, 5), (6, 6), (7, 7)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker, dom.DDLNotifier())
	defer r.Close()
	// Analyze tables concurrently.
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	r.WaitAutoAnalyzeFinishedForTest()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Check if t1 and t2 are analyzed (they should be, as they have more new data).
	tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.Equal(t, int64(0), tblStats1.ModifyCount)
	require.Equal(t, int64(12), tblStats1.RealtimeCount)

	tbl2, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.Equal(t, int64(0), tblStats2.ModifyCount)
	require.Equal(t, int64(8), tblStats2.RealtimeCount)

	// t3 should not be analyzed yet, as it has the least new data.
	tbl3, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t3"))
	require.NoError(t, err)
	pid3 := tbl3.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats3 := handle.GetPartitionStats(tbl3.Meta(), pid3)
	require.Equal(t, int64(4), tblStats3.ModifyCount)

	// Do one more round to analyze t3.
	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	r.WaitAutoAnalyzeFinishedForTest()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))

	// Now t3 should be analyzed.
	tblStats3 = handle.GetPartitionStats(tbl3.Meta(), pid3)
	require.Equal(t, int64(0), tblStats3.ModifyCount)
	require.Equal(t, int64(6), tblStats3.RealtimeCount)
}

func TestDoNotRetryTableNotExistJob(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	testutil.HandleNextDDLEventWithTxn(handle)
	// Insert some data.
	tk.MustExec("insert into t1 values (1, 1)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker, dom.DDLNotifier())
	defer r.Close()

	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	require.Equal(t, 0, r.Len())
	r.WaitAutoAnalyzeFinishedForTest()

	// Insert more data.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))

	r.ProcessDMLChangesForTest()
	require.Equal(t, 1, r.Len())

	// Drop the database.
	tk.MustExec("drop database test")

	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.False(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	require.Equal(t, 0, r.Len())

	r.ProcessDMLChangesForTest()
	require.Equal(t, 0, r.Len())
}

func TestAnalyzeHighestPriorityTablesWithFailedAnalysis(t *testing.T) {
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	handle := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_auto_analyze=true")
	tk.MustExec("set global tidb_auto_analyze_concurrency=2")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	testutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("create table t2 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	testutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("analyze table t2")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")
	// Add a failed job to t1.
	startTime := tk.MustQuery("select now() - interval 2 second").Rows()[0][0].(string)
	insertFailedJobForPartitionWithStartTime(tk, "test", "t1", "p0", startTime)
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker, dom.DDLNotifier())
	defer r.Close()

	require.NoError(t, util.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		require.True(t, r.AnalyzeHighestPriorityTables(sctx))
		return nil
	}))
	r.WaitAutoAnalyzeFinishedForTest()

	is := dom.InfoSchema()
	// t1 is not analyzed.
	tbl1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.False(t, tblStats1.Pseudo)
	require.Equal(t, int64(1), tblStats1.ModifyCount)

	// t2 is analyzed.
	tbl2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.False(t, tblStats2.Pseudo)
	require.Equal(t, int64(0), tblStats2.ModifyCount)
}

func insertFailedJobForPartitionWithStartTime(
	tk *testkit.TestKit,
	dbName string,
	tableName string,
	partitionName string,
	startTime string,
) {
	tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		partition_name,
		job_info,
		start_time,
		end_time,
		state,
		fail_reason,
		instance
	) VALUES (
		?,
		?,
		?,
		'Job information for failed job',
		?,
		'2024-01-01 10:00:00',
		'failed',
		'Some reason for failure',
		'example_instance'
	);
		`,
		dbName,
		tableName,
		partitionName,
		startTime,
	)
}
