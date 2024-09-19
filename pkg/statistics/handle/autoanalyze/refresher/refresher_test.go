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
	"math"
	"testing"
	"time"

	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/refresher"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

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
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	// No jobs are added.
	require.Equal(t, 0, r.Jobs.Len())
	require.False(t, r.AnalyzeHighestPriorityTables())
	// Enable the auto analyze.
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.2")
	r.RebuildTableAnalysisJobQueue()
	// Jobs are added.
	require.Equal(t, 1, r.Jobs.Len())
	require.True(t, r.AnalyzeHighestPriorityTables())
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
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	require.Equal(t, 0, r.Jobs.Len(), "No jobs are added because table stats are nil")
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
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	require.Equal(t, 0, r.Jobs.Len(), "No jobs are added because table stats are nil")
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
	tbl1, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.False(t, tblStats1.Pseudo)
	tbl2, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t2"))
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
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	require.Equal(t, 1, r.Jobs.Len(), "Only t1 is added to the job queue, because t2 is a tiny table(not enough data)")
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
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	require.Equal(t, 2, r.Jobs.Len())
	// Analyze t1 first.
	require.True(t, r.AnalyzeHighestPriorityTables())
	r.WaitAutoAnalyzeFinishedForTest()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// The table is analyzed.
	tbl1, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.Equal(t, int64(0), tblStats1.ModifyCount)
	require.Equal(t, int64(12), tblStats1.RealtimeCount)
	// t2 is not analyzed.
	tbl2, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t2"))
	require.NoError(t, err)
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.Equal(t, int64(6), tblStats2.ModifyCount)
	// Do one more round.
	require.True(t, r.AnalyzeHighestPriorityTables())
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
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	require.Equal(t, 3, r.Jobs.Len())
	// Analyze tables concurrently.
	require.True(t, r.AnalyzeHighestPriorityTables())
	r.WaitAutoAnalyzeFinishedForTest()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Check if t1 and t2 are analyzed (they should be, as they have more new data).
	tbl1, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.Equal(t, int64(0), tblStats1.ModifyCount)
	require.Equal(t, int64(12), tblStats1.RealtimeCount)

	tbl2, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t2"))
	require.NoError(t, err)
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.Equal(t, int64(0), tblStats2.ModifyCount)
	require.Equal(t, int64(8), tblStats2.RealtimeCount)

	// t3 should not be analyzed yet, as it has the least new data.
	tbl3, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t3"))
	require.NoError(t, err)
	pid3 := tbl3.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats3 := handle.GetPartitionStats(tbl3.Meta(), pid3)
	require.Equal(t, int64(4), tblStats3.ModifyCount)

	// Do one more round to analyze t3.
	require.True(t, r.AnalyzeHighestPriorityTables())
	r.WaitAutoAnalyzeFinishedForTest()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))

	// Now t3 should be analyzed.
	tblStats3 = handle.GetPartitionStats(tbl3.Meta(), pid3)
	require.Equal(t, int64(0), tblStats3.ModifyCount)
	require.Equal(t, int64(6), tblStats3.RealtimeCount)
}

func TestAnalyzeHighestPriorityTablesWithFailedAnalysis(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_auto_analyze=true")
	tk.MustExec("set global tidb_auto_analyze_concurrency=1")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("create table t2 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")

	handle := dom.StatsHandle()
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	// No jobs in the queue.
	r.AnalyzeHighestPriorityTables()
	r.WaitAutoAnalyzeFinishedForTest()
	// The table is not analyzed.
	is := dom.InfoSchema()
	tbl1, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.True(t, tblStats1.Pseudo)

	// Add a job to the queue.
	job1 := &priorityqueue.NonPartitionedTableAnalysisJob{
		TableID:     tbl1.Meta().ID,
		TableSchema: "test",
		TableName:   "t1",
		Weight:      1,
		Indicators: priorityqueue.Indicators{
			ChangePercentage: 0.5,
		},
	}
	r.Jobs.Push(job1)
	tbl2, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t2"))
	require.NoError(t, err)
	job2 := &priorityqueue.NonPartitionedTableAnalysisJob{
		TableID:     tbl2.Meta().ID,
		TableSchema: "test",
		TableName:   "t2",
		Weight:      0.9,
		Indicators: priorityqueue.Indicators{
			ChangePercentage: 0.5,
		},
	}
	r.Jobs.Push(job2)
	// Add a failed job to t1.
	startTime := tk.MustQuery("select now() - interval 2 second").Rows()[0][0].(string)
	insertFailedJobForPartitionWithStartTime(tk, "test", "t1", "p0", startTime)

	r.AnalyzeHighestPriorityTables()
	r.WaitAutoAnalyzeFinishedForTest()
	// t2 is analyzed.
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.True(t, tblStats2.Pseudo)
	// t1 is not analyzed.
	tblStats1 = handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.False(t, tblStats1.Pseudo)
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

func TestRebuildTableAnalysisJobQueue(t *testing.T) {
	old := statistics.AutoAnalyzeMinCnt
	defer func() {
		statistics.AutoAnalyzeMinCnt = old
	}()
	statistics.AutoAnalyzeMinCnt = 0
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	handle := dom.StatsHandle()
	require.Nil(t, handle.DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table t1")
	require.Nil(t, handle.Update(context.Background(), dom.InfoSchema()))

	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker)

	// Rebuild the job queue. No jobs are added.
	err := r.RebuildTableAnalysisJobQueue()
	require.NoError(t, err)
	require.Equal(t, 0, r.Jobs.Len())
	// Insert more data into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6)")
	require.Nil(t, handle.DumpStatsDeltaToKV(true))
	require.Nil(t, handle.Update(context.Background(), dom.InfoSchema()))
	err = r.RebuildTableAnalysisJobQueue()
	require.NoError(t, err)
	require.Equal(t, 1, r.Jobs.Len())
	job1 := r.Jobs.Pop()
	indicators := job1.GetIndicators()
	require.Equal(t, 1.2, math.Round(job1.GetWeight()*10)/10)
	require.Equal(t, float64(1), indicators.ChangePercentage)
	require.Equal(t, float64(6*2), indicators.TableSize)
	require.GreaterOrEqual(t, indicators.LastAnalysisDuration, time.Duration(0))
}
