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

package refresher

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestPickOneTableAndAnalyzeByPriority(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("create table t2 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")

	handle := dom.StatsHandle()
	sysProcTracker := dom.SysProcTracker()
	r, err := NewRefresher(handle, sysProcTracker)
	require.NoError(t, err)
	// No jobs in the queue.
	r.pickOneTableAndAnalyzeByPriority()
	// The table is not analyzed.
	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.True(t, tblStats1.Pseudo)

	// Add a job to the queue.
	job1 := &priorityqueue.TableAnalysisJob{
		TableID:          tbl1.Meta().ID,
		TableSchema:      "test",
		TableName:        "t1",
		ChangePercentage: 0.5,
		Weight:           1,
	}
	r.jobs.Push(job1)
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	job2 := &priorityqueue.TableAnalysisJob{
		TableID:          tbl2.Meta().ID,
		TableSchema:      "test",
		TableName:        "t2",
		ChangePercentage: 0.5,
		Weight:           0.9,
	}
	r.jobs.Push(job2)
	r.pickOneTableAndAnalyzeByPriority()
	// The table is analyzed.
	tblStats1 = handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.False(t, tblStats1.Pseudo)
	// t2 is not analyzed.
	pid2 := tbl2.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats2 := handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.True(t, tblStats2.Pseudo)
	// Do one more round.
	r.pickOneTableAndAnalyzeByPriority()
	// t2 is analyzed.
	pid2 = tbl2.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats2 = handle.GetPartitionStats(tbl2.Meta(), pid2)
	require.False(t, tblStats2.Pseudo)
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
	r, err := NewRefresher(handle, sysProcTracker)
	require.NoError(t, err)
	// No jobs in the queue.
	r.pickOneTableAndAnalyzeByPriority()
	// The table is not analyzed.
	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	pid1 := tbl1.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats1 := handle.GetPartitionStats(tbl1.Meta(), pid1)
	require.True(t, tblStats1.Pseudo)

	// Add a job to the queue.
	job1 := &priorityqueue.TableAnalysisJob{
		TableID:          tbl1.Meta().ID,
		TableSchema:      "test",
		TableName:        "t1",
		ChangePercentage: 0.5,
		Weight:           1,
	}
	r.jobs.Push(job1)
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	job2 := &priorityqueue.TableAnalysisJob{
		TableID:          tbl2.Meta().ID,
		TableSchema:      "test",
		TableName:        "t2",
		ChangePercentage: 0.5,
		Weight:           0.9,
	}
	r.jobs.Push(job2)
	// Add a failed job to t1.
	startTime := tk.MustQuery("select now() - interval 2 second").Rows()[0][0].(string)
	insertFailedJobForPartitionWithStartTime(tk, "test", "t1", "p0", startTime)

	r.pickOneTableAndAnalyzeByPriority()
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
	r, err := NewRefresher(handle, sysProcTracker)
	require.NoError(t, err)

	// Rebuild the job queue.
	err = r.rebuildTableAnalysisJobQueue()
	require.NoError(t, err)
	require.Equal(t, 1, r.jobs.Len())
	job1 := r.jobs.Pop()
	require.Equal(t, float64(1), job1.Weight)
	require.Equal(t, float64(0), job1.ChangePercentage)
	require.Equal(t, float64(3*2), job1.TableSize)
	require.GreaterOrEqual(t, job1.LastAnalysisDuration, time.Duration(0))
	// Insert more data into t1.
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6)")
	require.Nil(t, handle.DumpStatsDeltaToKV(true))
	require.Nil(t, handle.Update(dom.InfoSchema()))
	err = r.rebuildTableAnalysisJobQueue()
	require.NoError(t, err)
	require.Equal(t, 1, r.jobs.Len())
	job1 = r.jobs.Pop()
	require.Equal(t, float64(1), job1.Weight)
	require.Equal(t, float64(1), job1.ChangePercentage)
	require.Equal(t, float64(6*2), job1.TableSize)
	require.GreaterOrEqual(t, job1.LastAnalysisDuration, time.Duration(0))
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
	tests := []struct {
		name             string
		tblStats         *statistics.Table
		autoAnalyzeRatio float64
		want             float64
	}{
		{
			name: "Test Pseudo",
			tblStats: &statistics.Table{
				HistColl: statistics.HistColl{
					Pseudo: true,
				},
			},
			autoAnalyzeRatio: 0.5,
			want:             0,
		},
		{
			name: "Test RealtimeCount less than AutoAnalyzeMinCnt",
			tblStats: &statistics.Table{
				HistColl: statistics.HistColl{
					RealtimeCount: exec.AutoAnalyzeMinCnt - 1,
				},
			},
			autoAnalyzeRatio: 0.5,
			want:             0,
		},
		{
			name: "Test Table not analyzed",
			tblStats: &statistics.Table{
				HistColl: statistics.HistColl{
					Pseudo:        false,
					RealtimeCount: exec.AutoAnalyzeMinCnt + 1,
					Columns:       unanalyzedColumns,
					Indices:       unanalyzedIndices,
				},
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
			},
			autoAnalyzeRatio: 0.5,
			want:             2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateChangePercentage(tt.tblStats, tt.autoAnalyzeRatio)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGetTableLastAnalyzeDuration(t *testing.T) {
	tblStats := &statistics.Table{
		Version: oracle.ComposeTS(time.Hour.Nanoseconds()*1000, 0),
	}
	currentTs := oracle.ComposeTS((time.Hour.Nanoseconds()+time.Second.Nanoseconds())*1000, 0)
	want := time.Second

	got := getTableLastAnalyzeDuration(tblStats, currentTs)
	require.Equal(t, want, got)
}

func TestCheckIndexesNeedAnalyze(t *testing.T) {
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
			tblStats: &statistics.Table{},
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
			},
			want: []string{"index1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkIndexesNeedAnalyze(tt.tblInfo, tt.tblStats)
			require.Equal(t, tt.want, got)
		})
	}
}
