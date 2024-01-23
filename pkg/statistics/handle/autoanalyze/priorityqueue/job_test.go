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

package priorityqueue

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGenSQLForAnalyzeTable(t *testing.T) {
	job := &TableAnalysisJob{
		TableSchema: "test_schema",
		TableName:   "test_table",
	}

	expectedSQL := "analyze table %n.%n"
	expectedParams := []interface{}{"test_schema", "test_table"}

	sql, params := job.genSQLForAnalyzeTable()

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestGenSQLForAnalyzeIndex(t *testing.T) {
	job := &TableAnalysisJob{
		TableSchema: "test_schema",
		TableName:   "test_table",
	}

	index := "test_index"

	expectedSQL := "analyze table %n.%n index %n"
	expectedParams := []interface{}{"test_schema", "test_table", index}

	sql, params := job.genSQLForAnalyzeIndex(index)

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestAnalyzeTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &TableAnalysisJob{
		TableSchema:   "test",
		TableName:     "t",
		TableStatsVer: 2,
	}

	// Before analyze table.
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()
	// Check the result of analyze.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetTableStats(tbl.Meta())
	require.True(t, tblStats.Pseudo)

	job.analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetTableStats(tbl.Meta())
	require.Equal(t, int64(3), tblStats.RealtimeCount)
}

func TestAnalyzeIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &TableAnalysisJob{
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

	job.analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetTableStats(tbl.Meta())
	require.NotNil(t, tblStats.Indices[1])
	require.True(t, tblStats.Indices[1].IsAnalyzed())
	// Add a new index.
	tk.MustExec("alter table t add index idx2(b)")
	job = &TableAnalysisJob{
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

	job.analyze(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetTableStats(tbl.Meta())
	require.NotNil(t, tblStats.Indices[2])
	require.True(t, tblStats.Indices[2].IsAnalyzed())
}

func TestAnalyzePartitions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &TableAnalysisJob{
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

	job.analyze(sctx, handle, dom.SysProcTracker())
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
	job := &TableAnalysisJob{
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

	job.analyzePartitionIndexes(sctx, handle, dom.SysProcTracker())
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
