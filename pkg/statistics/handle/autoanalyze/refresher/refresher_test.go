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

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
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
