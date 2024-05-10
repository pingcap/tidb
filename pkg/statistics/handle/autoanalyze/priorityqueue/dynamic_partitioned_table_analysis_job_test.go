// Copyright 2024 PingCAP, Inc.
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

package priorityqueue_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeDynamicPartitionedTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &priorityqueue.DynamicPartitionedTableAnalysisJob{
		TableSchema:     "test",
		GlobalTableName: "t",
		Partitions:      []string{"p0", "p1"},
		TableStatsVer:   2,
	}

	// Before analyze partitions.
	handle := dom.StatsHandle()
	// Check the result of analyze.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats := handle.GetPartitionStats(tbl.Meta(), pid)
	require.True(t, tblStats.Pseudo)

	job.Analyze(handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid = tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Pseudo)
	require.Equal(t, int64(1), tblStats.RealtimeCount)
}

func TestAnalyzeDynamicPartitionedTableIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a), index idx1(b)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &priorityqueue.DynamicPartitionedTableAnalysisJob{
		TableSchema:     "test",
		GlobalTableName: "t",
		PartitionIndexes: map[string][]string{
			"idx":  {"p0", "p1"},
			"idx1": {"p0", "p1"},
		},
		TableStatsVer: 2,
	}

	// Before analyze partitions.
	handle := dom.StatsHandle()
	// Check the result of analyze index.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats := handle.GetPartitionStats(tbl.Meta(), pid)
	require.True(t, tblStats.Pseudo)
	require.NotNil(t, tblStats.Indices[1])
	require.False(t, tblStats.Indices[1].IsAnalyzed())

	job.Analyze(handle, dom.SysProcTracker())
	// Check the result of analyze index.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid = tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Pseudo)
	require.NotNil(t, tblStats.Indices[1])
	require.True(t, tblStats.Indices[1].IsAnalyzed())
	require.NotNil(t, tblStats.Indices[2])
	require.True(t, tblStats.Indices[2].IsAnalyzed())
	// partition p1
	pid = tbl.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Pseudo)
	require.NotNil(t, tblStats.Indices[1])
	require.True(t, tblStats.Indices[1].IsAnalyzed())
	require.NotNil(t, tblStats.Indices[2])
	require.True(t, tblStats.Indices[2].IsAnalyzed())
	// Check analyze jobs are created.
	rows := tk.MustQuery("select * from mysql.analyze_jobs").Rows()
	// Because analyze one index will analyze all indexes and all columns together, so there are 5 jobs.
	require.Len(t, rows, 5)
}

func TestIsValidToAnalyzeForDynamicPartitionedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	job := &priorityqueue.DynamicPartitionedTableAnalysisJob{
		TableSchema:     "example_schema",
		GlobalTableName: "example_table",
		Partitions:      []string{"p0", "p1"},
		Weight:          2,
	}
	initJobs(tk)
	insertMultipleFinishedJobs(tk, job.GlobalTableName, "p0")

	se := tk.Session()
	sctx := se.(sessionctx.Context)
	valid, failReason := job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)

	// Insert some failed jobs.
	// Just failed.
	now := tk.MustQuery("select now()").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.GlobalTableName, "p0", now)
	// Note: The failure reason is not checked in this test because the time duration can sometimes be inaccurate.(not now)
	valid, _ = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	// Failed 10 seconds ago.
	startTime := tk.MustQuery("select now() - interval 10 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.GlobalTableName, "p0", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 2 times the average analysis duration", failReason)
	// Failed long long ago.
	startTime = tk.MustQuery("select now() - interval 300 day").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.GlobalTableName, "p0", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)
	// Smaller start time for p1.
	startTime = tk.MustQuery("select now() - interval 1 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.GlobalTableName, "p1", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 2 times the average analysis duration", failReason)
}
