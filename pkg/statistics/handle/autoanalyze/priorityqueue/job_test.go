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

func TestGenSQLForAnalyzeTable(t *testing.T) {
	job := &priorityqueue.TableAnalysisJob{
		TableSchema: "test_schema",
		TableName:   "test_table",
	}

	expectedSQL := "analyze table %n.%n"
	expectedParams := []any{"test_schema", "test_table"}

	sql, params := job.GenSQLForAnalyzeTable()

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestGenSQLForAnalyzeStaticPartition(t *testing.T) {
	job := &priorityqueue.TableAnalysisJob{
		TableSchema:         "test_schema",
		TableName:           "test_table",
		StaticPartitionName: "p0",
	}

	expectedSQL := "analyze table %n.%n partition %n"
	expectedParams := []any{"test_schema", "test_table", "p0"}

	sql, params := job.GenSQLForAnalyzeStaticPartition()

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestGenSQLForAnalyzeIndex(t *testing.T) {
	job := &priorityqueue.TableAnalysisJob{
		TableSchema: "test_schema",
		TableName:   "test_table",
	}

	index := "test_index"

	expectedSQL := "analyze table %n.%n index %n"
	expectedParams := []any{"test_schema", "test_table", index}

	sql, params := job.GenSQLForAnalyzeIndex(index)

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestGenSQLForAnalyzePartitionIndex(t *testing.T) {
	job := &priorityqueue.TableAnalysisJob{
		TableSchema:         "test_schema",
		TableName:           "test_table",
		StaticPartitionName: "p0",
	}

	index := "test_index"

	expectedSQL := "analyze table %n.%n partition %n index %n"
	expectedParams := []any{"test_schema", "test_table", "p0", index}

	sql, params := job.GenSQLForAnalyzeStaticPartitionIndex(index)

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestAnalyzeTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &priorityqueue.TableAnalysisJob{
		TableSchema:   "test",
		TableName:     "t",
		TableStatsVer: 2,
	}

	// Before analyze table.
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetTableStats(tbl.Meta())
	require.True(t, tblStats.Pseudo)

	job.Analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetTableStats(tbl.Meta())
	require.Equal(t, int64(3), tblStats.RealtimeCount)
}

func TestAnalyzeStaticPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")

	job := &priorityqueue.TableAnalysisJob{
		TableSchema:         "test",
		TableName:           "t",
		StaticPartitionName: "p0",
		TableStatsVer:       2,
	}

	// Before analyze the partition.
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats := handle.GetPartitionStats(tbl.Meta(), pid)
	require.True(t, tblStats.Pseudo)

	job.Analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid = tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Pseudo)
	require.Equal(t, int64(1), tblStats.RealtimeCount)
}

func TestAnalyzeIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &priorityqueue.TableAnalysisJob{
		TableSchema:   "test",
		TableName:     "t",
		Indexes:       []string{"idx"},
		TableStatsVer: 2,
	}
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()
	// Before analyze indexes.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetTableStats(tbl.Meta())
	require.False(t, tblStats.Indices[1].IsAnalyzed())

	job.Analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetTableStats(tbl.Meta())
	require.NotNil(t, tblStats.Indices[1])
	require.True(t, tblStats.Indices[1].IsAnalyzed())
	// Add a new index.
	tk.MustExec("alter table t add index idx2(b)")
	job = &priorityqueue.TableAnalysisJob{
		TableSchema:   "test",
		TableName:     "t",
		Indexes:       []string{"idx", "idx2"},
		TableStatsVer: 2,
	}
	require.NoError(t, handle.Update(dom.InfoSchema()))
	// Before analyze indexes.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetTableStats(tbl.Meta())
	require.Len(t, tblStats.Indices, 1)

	job.Analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetTableStats(tbl.Meta())
	require.NotNil(t, tblStats.Indices[2])
	require.True(t, tblStats.Indices[2].IsAnalyzed())
}

func TestAnalyzeStaticPartitionIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &priorityqueue.TableAnalysisJob{
		TableSchema:         "test",
		TableName:           "t",
		StaticPartitionName: "p0",
		Indexes:             []string{"idx"},
		TableStatsVer:       2,
	}
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()
	// Before analyze indexes.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats := handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Indices[1].IsAnalyzed())

	job.Analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid = tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.NotNil(t, tblStats.Indices[1])
	require.True(t, tblStats.Indices[1].IsAnalyzed())
	// Add a new index.
	tk.MustExec("alter table t add index idx2(b)")
	job = &priorityqueue.TableAnalysisJob{
		TableSchema:         "test",
		TableName:           "t",
		StaticPartitionName: "p0",
		Indexes:             []string{"idx", "idx2"},
		TableStatsVer:       2,
	}
	require.NoError(t, handle.Update(dom.InfoSchema()))
	// Before analyze indexes.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid = tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.Len(t, tblStats.Indices, 1)

	job.Analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid = tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.NotNil(t, tblStats.Indices[2])
	require.True(t, tblStats.Indices[2].IsAnalyzed())
}

func TestAnalyzePartitions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &priorityqueue.TableAnalysisJob{
		TableSchema:   "test",
		TableName:     "t",
		Partitions:    []string{"p0", "p1"},
		TableStatsVer: 2,
	}

	// Before analyze partitions.
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()
	// Check the result of analyze.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats := handle.GetPartitionStats(tbl.Meta(), pid)
	require.True(t, tblStats.Pseudo)

	job.Analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid = tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Pseudo)
	require.Equal(t, int64(1), tblStats.RealtimeCount)
}

func TestAnalyzePartitionIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &priorityqueue.TableAnalysisJob{
		TableSchema: "test",
		TableName:   "t",
		PartitionIndexes: map[string][]string{
			"idx": {"p0", "p1"},
		},
		TableStatsVer: 2,
	}

	// Before analyze partitions.
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()
	// Check the result of analyze.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats := handle.GetPartitionStats(tbl.Meta(), pid)
	require.True(t, tblStats.Pseudo)
	// Check the result of analyze index.
	require.NotNil(t, tblStats.Indices[1])
	require.False(t, tblStats.Indices[1].IsAnalyzed())

	job.Analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid = tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Pseudo)
	require.Equal(t, int64(1), tblStats.RealtimeCount)
	// Check the result of analyze index.
	require.NotNil(t, tblStats.Indices[1])
	require.True(t, tblStats.Indices[1].IsAnalyzed())
	// partition p1
	pid = tbl.Meta().GetPartitionInfo().Definitions[1].ID
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Pseudo)
	require.Equal(t, int64(2), tblStats.RealtimeCount)
	// Check the result of analyze index.
	require.NotNil(t, tblStats.Indices[1])
	require.True(t, tblStats.Indices[1].IsAnalyzed())
}

func TestIsValidToAnalyze(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	job := &priorityqueue.TableAnalysisJob{
		TableSchema: "example_schema",
		TableName:   "example_table1",
		Weight:      2,
	}
	initJobs(tk)
	insertMultipleFinishedJobs(tk, job.TableName, "")

	se := tk.Session()
	sctx := se.(sessionctx.Context)
	valid, failReason := job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)

	// Insert some failed jobs.
	// Just failed.
	now := tk.MustQuery("select now()").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "", now)
	// Note: The failure reason is not checked in this test because the time duration can sometimes be inaccurate.(not now)
	valid, _ = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	// Failed 10 seconds ago.
	startTime := tk.MustQuery("select now() - interval 10 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 2 times the average analysis duration", failReason)
	// Failed long long ago.
	startTime = tk.MustQuery("select now() - interval 300 day").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)
}

func TestIsValidToAnalyzeWhenOnlyHasFailedAnalysisRecords(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	job := &priorityqueue.TableAnalysisJob{
		TableSchema: "example_schema",
		TableName:   "example_table1",
		Weight:      2,
	}
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	valid, failReason := job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)
	// Failed long long ago.
	startTime := tk.MustQuery("select now() - interval 30 day").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)

	// Failed recently.
	tenSecondsAgo := tk.MustQuery("select now() - interval 10 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "", tenSecondsAgo)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 30m0s", failReason)
}

func TestIsValidToAnalyzeForPartitionedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	job := &priorityqueue.TableAnalysisJob{
		TableSchema: "example_schema",
		TableName:   "example_table",
		Weight:      2,
		Partitions:  []string{"p0", "p1"},
	}
	initJobs(tk)
	insertMultipleFinishedJobs(tk, job.TableName, "p0")

	se := tk.Session()
	sctx := se.(sessionctx.Context)
	valid, failReason := job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)

	// Insert some failed jobs.
	// Just failed.
	now := tk.MustQuery("select now()").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "p0", now)
	// Note: The failure reason is not checked in this test because the time duration can sometimes be inaccurate.(not now)
	valid, _ = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	// Failed 10 seconds ago.
	startTime := tk.MustQuery("select now() - interval 10 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "p0", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 2 times the average analysis duration", failReason)
	// Failed long long ago.
	startTime = tk.MustQuery("select now() - interval 300 day").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "p0", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)
	// Smaller start time for p1.
	startTime = tk.MustQuery("select now() - interval 1 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "p1", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 2 times the average analysis duration", failReason)
}

func TestIsValidToAnalyzeForStaticPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	job := &priorityqueue.TableAnalysisJob{
		TableSchema:         "example_schema",
		TableName:           "example_table",
		Weight:              2,
		StaticPartitionName: "p0",
	}
	initJobs(tk)
	insertMultipleFinishedJobs(tk, job.TableName, "p0")
	insertMultipleFinishedJobs(tk, job.TableName, "p1")

	se := tk.Session()
	sctx := se.(sessionctx.Context)
	valid, failReason := job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)

	// Insert some failed jobs.
	// Just failed.
	now := tk.MustQuery("select now()").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "p0", now)
	// Note: The failure reason is not checked in this test because the time duration can sometimes be inaccurate.(not now)
	valid, _ = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	// Failed 10 seconds ago.
	startTime := tk.MustQuery("select now() - interval 10 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "p0", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 2 times the average analysis duration", failReason)
	// Failed long long ago.
	startTime = tk.MustQuery("select now() - interval 300 day").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.TableSchema, job.TableName, "p0", startTime)
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)
	// Do not affect other partitions.
	job = &priorityqueue.TableAnalysisJob{
		TableSchema:         "example_schema",
		TableName:           "example_table",
		Weight:              2,
		StaticPartitionName: "p1",
	}
	valid, failReason = job.IsValidToAnalyze(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)
}

func TestStringer(t *testing.T) {
	tests := []struct {
		name string
		job  *priorityqueue.TableAnalysisJob
		want string
	}{
		{
			name: "analyze table",
			job: &priorityqueue.TableAnalysisJob{
				TableID:          1,
				TableSchema:      "test_schema",
				TableName:        "test_table",
				TableStatsVer:    1,
				ChangePercentage: 0.5,
				Weight:           1.999999,
			},
			want: "TableAnalysisJob: {AnalyzeType: table, Schema: test_schema, Table: test_table, TableID: 1, TableStatsVer: 1, ChangePercentage: 0.50, Weight: 2.0000}",
		},
		{
			name: "analyze table index",
			job: &priorityqueue.TableAnalysisJob{
				TableID:          2,
				TableSchema:      "test_schema",
				TableName:        "test_table",
				Indexes:          []string{"idx"},
				TableStatsVer:    1,
				ChangePercentage: 0.5,
				Weight:           1.999999,
			},
			want: "TableAnalysisJob: {AnalyzeType: index, Indexes: idx, Schema: test_schema, Table: test_table, TableID: 2, TableStatsVer: 1, ChangePercentage: 0.50, Weight: 2.0000}",
		},
		{
			name: "analyze partitions",
			job: &priorityqueue.TableAnalysisJob{
				TableID:          3,
				TableSchema:      "test_schema",
				TableName:        "test_table",
				Partitions:       []string{"p0", "p1"},
				TableStatsVer:    1,
				ChangePercentage: 0.5,
				Weight:           1.999999,
			},
			want: "TableAnalysisJob: {AnalyzeType: partition, Partitions: p0, p1, Schema: test_schema, Table: test_table, TableID: 3, TableStatsVer: 1, ChangePercentage: 0.50, Weight: 2.0000}",
		},
		{
			name: "analyze partition indexes",
			job: &priorityqueue.TableAnalysisJob{
				TableID:     4,
				TableSchema: "test_schema",
				TableName:   "test_table",
				PartitionIndexes: map[string][]string{
					"idx": {"p0", "p1"},
				},
				TableStatsVer:    1,
				ChangePercentage: 0.5,
				Weight:           1.999999,
			},
			want: "TableAnalysisJob: {AnalyzeType: partitionIndex, PartitionIndexes: map[idx:[p0 p1]], Schema: test_schema, Table: test_table, TableID: 4, TableStatsVer: 1, ChangePercentage: 0.50, Weight: 2.0000}",
		},
		{
			name: "analyze static partition",
			job: &priorityqueue.TableAnalysisJob{
				TableID:             5,
				TableSchema:         "test_schema",
				TableName:           "test_table",
				StaticPartitionName: "p0",
				StaticPartitionID:   6,
				TableStatsVer:       1,
				ChangePercentage:    0.5,
				Weight:              1.999999,
			},
			want: "TableAnalysisJob: {AnalyzeType: staticPartition, Schema: test_schema, Table: test_table, TableID: 5, StaticPartition: p0, StaticPartitionID: 6, TableStatsVer: 1, ChangePercentage: 0.50, Weight: 2.0000}",
		},
		{
			name: "analyze static partition index",
			job: &priorityqueue.TableAnalysisJob{
				TableID:             7,
				TableSchema:         "test_schema",
				TableName:           "test_table",
				StaticPartitionName: "p0",
				StaticPartitionID:   8,
				Indexes:             []string{"idx"},
				TableStatsVer:       1,
				ChangePercentage:    0.5,
				Weight:              1.999999,
			},
			want: "TableAnalysisJob: {AnalyzeType: staticPartitionIndex, Indexes: idx, Schema: test_schema, Table: test_table, TableID: 7, StaticPartition: p0, StaticPartitionID: 8, TableStatsVer: 1, ChangePercentage: 0.50, Weight: 2.0000}",
		},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.job.String())
	}
}
