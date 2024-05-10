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
	"math"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/refresher"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestSkipAnalyzeTableWhenAutoAnalyzeRatioIsZero(t *testing.T) {
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
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
	require.NoError(t, handle.Update(dom.InfoSchema()))
	// Analyze those tables first.
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	// Insert more data into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	// No jobs are added.
	require.Equal(t, 0, r.Jobs.Len())
	require.False(t, r.PickOneTableAndAnalyzeByPriority())
	// Enable the auto analyze.
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.2")
	r.RebuildTableAnalysisJobQueue()
	// Jobs are added.
	require.Equal(t, 1, r.Jobs.Len())
	require.True(t, r.PickOneTableAndAnalyzeByPriority())
}

func TestIgnoreNilOrPseudoStatsOfPartitionedTable(t *testing.T) {
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
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
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
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
	exec.AutoAnalyzeMinCnt = 10
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
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
	require.NoError(t, handle.Update(dom.InfoSchema()))
	// Analyze those tables first.
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(dom.InfoSchema()))
	// Make sure table stats are not pseudo.
	tbl1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.False(t, tblStats1.Pseudo)
	tbl2, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.False(t, tblStats2.Pseudo)

	// Insert more data into t1 and t2, but more data is inserted into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13)")
	tk.MustExec("insert into t2 values (4, 4)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	require.Equal(t, 1, r.Jobs.Len(), "Only t1 is added to the job queue, because t2 is a tiny table(not enough data)")
}

func TestPickOneTableAndAnalyzeByPriority(t *testing.T) {
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
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
	require.NoError(t, handle.Update(dom.InfoSchema()))
	// Analyze those tables first.
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(dom.InfoSchema()))
	// Insert more data into t1 and t2, but more data is inserted into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13)")
	tk.MustExec("insert into t2 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(dom.InfoSchema()))
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	require.Equal(t, 2, r.Jobs.Len())
	// Analyze t1 first.
	require.True(t, r.PickOneTableAndAnalyzeByPriority())
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(dom.InfoSchema()))
	// The table is analyzed.
	tbl1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.Equal(t, int64(0), tblStats1.ModifyCount)
	require.Equal(t, int64(12), tblStats1.RealtimeCount)
	// t2 is not analyzed.
	tbl2, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.Equal(t, int64(6), tblStats2.ModifyCount)
	// Do one more round.
	require.True(t, r.PickOneTableAndAnalyzeByPriority())
	// t2 is analyzed.
	pid2 = tbl2.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats2 = handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.Equal(t, int64(0), tblStats2.ModifyCount)
	require.Equal(t, int64(8), tblStats2.RealtimeCount)
}

func TestPickOneTableAndAnalyzeByPriorityWithFailedAnalysis(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("create table t2 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")

	handle := dom.StatsHandle()
	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker)
	r.RebuildTableAnalysisJobQueue()
	// No jobs in the queue.
	r.PickOneTableAndAnalyzeByPriority()
	// The table is not analyzed.
	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
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
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
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

	r.PickOneTableAndAnalyzeByPriority()
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
	old := exec.AutoAnalyzeMinCnt
	defer func() {
		exec.AutoAnalyzeMinCnt = old
	}()
	exec.AutoAnalyzeMinCnt = 0
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	handle := dom.StatsHandle()
	require.Nil(t, handle.DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table t1")
	require.Nil(t, handle.Update(dom.InfoSchema()))

	sysProcTracker := dom.SysProcTracker()
	r := refresher.NewRefresher(handle, sysProcTracker)

	// Rebuild the job queue. No jobs are added.
	err := r.RebuildTableAnalysisJobQueue()
	require.NoError(t, err)
	require.Equal(t, 0, r.Jobs.Len())
	// Insert more data into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6)")
	require.Nil(t, handle.DumpStatsDeltaToKV(true))
	require.Nil(t, handle.Update(dom.InfoSchema()))
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

func TestCalculateChangePercentage(t *testing.T) {
	unanalyzedColumns := map[int64]*statistics.Column{
		1: {},
		2: {},
	}
	unanalyzedIndices := map[int64]*statistics.Index{
		1: {},
		2: {},
	}
	analyzedColumns := map[int64]*statistics.Column{
		1: {
			StatsVer: 2,
		},
		2: {
			StatsVer: 2,
		},
	}
	analyzedIndices := map[int64]*statistics.Index{
		1: {
			StatsVer: 2,
		},
		2: {
			StatsVer: 2,
		},
	}
	bothUnanalyzedMap := statistics.NewColAndIndexExistenceMap(0, 0)
	bothAnalyzedMap := statistics.NewColAndIndexExistenceMap(2, 2)
	bothAnalyzedMap.InsertCol(1, nil, true)
	bothAnalyzedMap.InsertCol(2, nil, true)
	bothAnalyzedMap.InsertIndex(1, nil, true)
	bothAnalyzedMap.InsertIndex(2, nil, true)
	tests := []struct {
		name             string
		tblStats         *statistics.Table
		autoAnalyzeRatio float64
		want             float64
	}{
		{
			name: "Test Table not analyzed",
			tblStats: &statistics.Table{
				HistColl: statistics.HistColl{
					Pseudo:        false,
					RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
					Columns:       unanalyzedColumns,
					Indices:       unanalyzedIndices,
				},
				ColAndIdxExistenceMap: bothUnanalyzedMap,
			},
			autoAnalyzeRatio: 0.5,
			want:             1,
		},
		{
			name: "Based on change percentage",
			tblStats: &statistics.Table{
				HistColl: statistics.HistColl{
					Pseudo:        false,
					RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
					Columns:       analyzedColumns,
					Indices:       analyzedIndices,
					ModifyCount:   (exec.AutoAnalyzeMinCnt + 1) * 2,
				},
				ColAndIdxExistenceMap: bothAnalyzedMap,
				LastAnalyzeVersion:    1,
			},
			autoAnalyzeRatio: 0.5,
			want:             2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := refresher.CalculateChangePercentage(tt.tblStats, tt.autoAnalyzeRatio)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGetTableLastAnalyzeDuration(t *testing.T) {
	// 2023-12-31 10:00:00
	lastUpdateTime := time.Date(2023, 12, 31, 10, 0, 0, 0, time.UTC)
	lastUpdateTs := oracle.GoTimeToTS(lastUpdateTime)
	tblStats := &statistics.Table{
		HistColl: statistics.HistColl{
			Pseudo: false,
			Columns: map[int64]*statistics.Column{
				1: {
					StatsVer: 2,
					Histogram: statistics.Histogram{
						LastUpdateVersion: lastUpdateTs,
					},
				},
			},
		},
		LastAnalyzeVersion: lastUpdateTs,
	}
	// 2024-01-01 10:00:00
	currentTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	currentTs := oracle.GoTimeToTS(currentTime)
	want := 24 * time.Hour

	got := refresher.GetTableLastAnalyzeDuration(tblStats, currentTs)
	require.Equal(t, want, got)
}

func TestGetTableLastAnalyzeDurationForUnanalyzedTable(t *testing.T) {
	tblStats := &statistics.Table{
		HistColl: statistics.HistColl{},
	}
	// 2024-01-01 10:00:00
	currentTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	currentTs := oracle.GoTimeToTS(currentTime)
	want := 1800 * time.Second

	got := refresher.GetTableLastAnalyzeDuration(tblStats, currentTs)
	require.Equal(t, want, got)
}

func TestCheckIndexesNeedAnalyze(t *testing.T) {
	analyzedMap := statistics.NewColAndIndexExistenceMap(1, 0)
	analyzedMap.InsertCol(1, nil, true)
	analyzedMap.InsertIndex(1, nil, false)
	tests := []struct {
		name     string
		tblInfo  *model.TableInfo
		tblStats *statistics.Table
		want     []string
	}{
		{
			name: "Test Table not analyzed",
			tblInfo: &model.TableInfo{
				Indices: []*model.IndexInfo{
					{
						ID:    1,
						Name:  model.NewCIStr("index1"),
						State: model.StatePublic,
					},
				},
			},
			tblStats: &statistics.Table{ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(0, 0)},
			want:     nil,
		},
		{
			name: "Test Index not analyzed",
			tblInfo: &model.TableInfo{
				Indices: []*model.IndexInfo{
					{
						ID:    1,
						Name:  model.NewCIStr("index1"),
						State: model.StatePublic,
					},
				},
			},
			tblStats: &statistics.Table{
				HistColl: statistics.HistColl{
					Pseudo:  false,
					Indices: map[int64]*statistics.Index{},
					Columns: map[int64]*statistics.Column{
						1: {
							StatsVer: 2,
						},
					},
				},
				ColAndIdxExistenceMap: analyzedMap,
				LastAnalyzeVersion:    1,
			},
			want: []string{"index1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := refresher.CheckIndexesNeedAnalyze(tt.tblInfo, tt.tblStats)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCalculateIndicatorsForPartitions(t *testing.T) {
	// 2024-01-01 10:00:00
	currentTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	currentTs := oracle.GoTimeToTS(currentTime)
	// 2023-12-31 10:00:00
	lastUpdateTime := time.Date(2023, 12, 31, 10, 0, 0, 0, time.UTC)
	lastUpdateTs := oracle.GoTimeToTS(lastUpdateTime)
	unanalyzedMap := statistics.NewColAndIndexExistenceMap(0, 0)
	analyzedMap := statistics.NewColAndIndexExistenceMap(2, 1)
	analyzedMap.InsertCol(1, nil, true)
	analyzedMap.InsertCol(2, nil, true)
	analyzedMap.InsertIndex(1, nil, true)
	tests := []struct {
		name                       string
		tblInfo                    *model.TableInfo
		partitionStats             map[refresher.PartitionIDAndName]*statistics.Table
		defs                       []model.PartitionDefinition
		autoAnalyzeRatio           float64
		currentTs                  uint64
		wantAvgChangePercentage    float64
		wantAvgSize                float64
		wantAvgLastAnalyzeDuration time.Duration
		wantPartitions             []string
	}{
		{
			name: "Test Table not analyzed",
			tblInfo: &model.TableInfo{
				Indices: []*model.IndexInfo{
					{
						ID:    1,
						Name:  model.NewCIStr("index1"),
						State: model.StatePublic,
					},
				},
				Columns: []*model.ColumnInfo{
					{
						ID: 1,
					},
					{
						ID: 2,
					},
				},
			},
			partitionStats: map[refresher.PartitionIDAndName]*statistics.Table{
				{
					ID:   1,
					Name: "p0",
				}: {
					HistColl: statistics.HistColl{
						Pseudo:        false,
						RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
					},
					ColAndIdxExistenceMap: unanalyzedMap,
				},
				{
					ID:   2,
					Name: "p1",
				}: {
					HistColl: statistics.HistColl{
						Pseudo:        false,
						RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
					},
					ColAndIdxExistenceMap: unanalyzedMap,
				},
			},
			defs: []model.PartitionDefinition{
				{
					ID:   1,
					Name: model.NewCIStr("p0"),
				},
				{
					ID:   2,
					Name: model.NewCIStr("p1"),
				},
			},
			autoAnalyzeRatio:           0.5,
			currentTs:                  currentTs,
			wantAvgChangePercentage:    1,
			wantAvgSize:                2002,
			wantAvgLastAnalyzeDuration: 1800 * time.Second,
			wantPartitions:             []string{"p0", "p1"},
		},
		{
			name: "Test Table analyzed and only one partition meets the threshold",
			tblInfo: &model.TableInfo{
				Indices: []*model.IndexInfo{
					{
						ID:    1,
						Name:  model.NewCIStr("index1"),
						State: model.StatePublic,
					},
				},
				Columns: []*model.ColumnInfo{
					{
						ID: 1,
					},
					{
						ID: 2,
					},
				},
			},
			partitionStats: map[refresher.PartitionIDAndName]*statistics.Table{
				{
					ID:   1,
					Name: "p0",
				}: {
					HistColl: statistics.HistColl{
						Pseudo:        false,
						RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
						ModifyCount:   (exec.AutoAnalyzeMinCnt + 1) * 2,
						Columns: map[int64]*statistics.Column{
							1: {
								StatsVer: 2,
								Histogram: statistics.Histogram{
									LastUpdateVersion: lastUpdateTs,
								},
							},
							2: {
								StatsVer: 2,
								Histogram: statistics.Histogram{
									LastUpdateVersion: lastUpdateTs,
								},
							},
						},
					},
					Version:               currentTs,
					ColAndIdxExistenceMap: analyzedMap,
					LastAnalyzeVersion:    lastUpdateTs,
				},
				{
					ID:   2,
					Name: "p1",
				}: {
					HistColl: statistics.HistColl{
						Pseudo:        false,
						RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
						ModifyCount:   0,
						Columns: map[int64]*statistics.Column{
							1: {
								StatsVer: 2,
								Histogram: statistics.Histogram{
									LastUpdateVersion: lastUpdateTs,
								},
							},
							2: {
								StatsVer: 2,
								Histogram: statistics.Histogram{
									LastUpdateVersion: lastUpdateTs,
								},
							},
						},
					},
					Version:               currentTs,
					ColAndIdxExistenceMap: analyzedMap,
					LastAnalyzeVersion:    lastUpdateTs,
				},
			},
			defs: []model.PartitionDefinition{
				{
					ID:   1,
					Name: model.NewCIStr("p0"),
				},
				{
					ID:   2,
					Name: model.NewCIStr("p1"),
				},
			},
			autoAnalyzeRatio:           0.5,
			currentTs:                  currentTs,
			wantAvgChangePercentage:    2,
			wantAvgSize:                2002,
			wantAvgLastAnalyzeDuration: 24 * time.Hour,
			wantPartitions:             []string{"p0"},
		},
		{
			name: "No partition meets the threshold",
			tblInfo: &model.TableInfo{
				Indices: []*model.IndexInfo{
					{
						ID:    1,
						Name:  model.NewCIStr("index1"),
						State: model.StatePublic,
					},
				},
				Columns: []*model.ColumnInfo{
					{
						ID: 1,
					},
					{
						ID: 2,
					},
				},
			},
			partitionStats: map[refresher.PartitionIDAndName]*statistics.Table{
				{
					ID:   1,
					Name: "p0",
				}: {
					HistColl: statistics.HistColl{
						Pseudo:        false,
						RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
						ModifyCount:   0,
						Columns: map[int64]*statistics.Column{
							1: {
								StatsVer: 2,
								Histogram: statistics.Histogram{
									LastUpdateVersion: lastUpdateTs,
								},
							},
							2: {
								StatsVer: 2,
								Histogram: statistics.Histogram{
									LastUpdateVersion: lastUpdateTs,
								},
							},
						},
					},
					Version:               currentTs,
					ColAndIdxExistenceMap: analyzedMap,
					LastAnalyzeVersion:    lastUpdateTs,
				},
				{
					ID:   2,
					Name: "p1",
				}: {
					HistColl: statistics.HistColl{
						Pseudo:        false,
						RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
						ModifyCount:   0,
						Columns: map[int64]*statistics.Column{
							1: {
								StatsVer: 2,
								Histogram: statistics.Histogram{
									LastUpdateVersion: lastUpdateTs,
								},
							},
							2: {
								StatsVer: 2,
								Histogram: statistics.Histogram{
									LastUpdateVersion: lastUpdateTs,
								},
							},
						},
					},
					Version:               currentTs,
					ColAndIdxExistenceMap: analyzedMap,
					LastAnalyzeVersion:    lastUpdateTs,
				},
			},
			defs: []model.PartitionDefinition{
				{
					ID:   1,
					Name: model.NewCIStr("p0"),
				},
				{
					ID:   2,
					Name: model.NewCIStr("p1"),
				},
			},
			autoAnalyzeRatio:           0.5,
			currentTs:                  currentTs,
			wantAvgChangePercentage:    0,
			wantAvgSize:                0,
			wantAvgLastAnalyzeDuration: 0,
			wantPartitions:             []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAvgChangePercentage,
				gotAvgSize,
				gotAvgLastAnalyzeDuration,
				gotPartitions :=
				refresher.CalculateIndicatorsForPartitions(
					tt.tblInfo,
					tt.partitionStats,
					tt.autoAnalyzeRatio,
					tt.currentTs,
				)
			require.Equal(t, tt.wantAvgChangePercentage, gotAvgChangePercentage)
			require.Equal(t, tt.wantAvgSize, gotAvgSize)
			require.Equal(t, tt.wantAvgLastAnalyzeDuration, gotAvgLastAnalyzeDuration)
			// Sort the partitions.
			sort.Strings(tt.wantPartitions)
			sort.Strings(gotPartitions)
			require.Equal(t, tt.wantPartitions, gotPartitions)
		})
	}
}

func TestCheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable(t *testing.T) {
	tblInfo := model.TableInfo{
		Indices: []*model.IndexInfo{
			{
				ID:    1,
				Name:  model.NewCIStr("index1"),
				State: model.StatePublic,
			},
			{
				ID:    2,
				Name:  model.NewCIStr("index2"),
				State: model.StatePublic,
			},
		},
		Columns: []*model.ColumnInfo{
			{
				ID: 1,
			},
			{
				ID: 2,
			},
		},
	}
	partitionStats := map[refresher.PartitionIDAndName]*statistics.Table{
		{
			ID:   1,
			Name: "p0",
		}: {
			HistColl: statistics.HistColl{
				Pseudo:        false,
				RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
				ModifyCount:   0,
				Indices:       map[int64]*statistics.Index{},
			},
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(0, 0),
		},
		{
			ID:   2,
			Name: "p1",
		}: {
			HistColl: statistics.HistColl{
				Pseudo:        false,
				RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
				ModifyCount:   0,
				Indices: map[int64]*statistics.Index{
					2: {
						StatsVer: 2,
					},
				},
			},
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(0, 1),
		},
	}

	partitionIndexes := refresher.CheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable(&tblInfo, partitionStats)
	expected := map[string][]string{"index1": {"p0", "p1"}, "index2": {"p0"}}
	require.Equal(t, len(expected), len(partitionIndexes))

	for k, v := range expected {
		sort.Strings(v)
		if val, ok := partitionIndexes[k]; ok {
			sort.Strings(val)
			require.Equal(t, v, val)
		} else {
			require.Fail(t, "key not found in partitionIndexes: "+k)
		}
	}
}
