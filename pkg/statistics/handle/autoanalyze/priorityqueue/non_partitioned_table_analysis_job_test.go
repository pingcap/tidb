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
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGenSQLForNonPartitionedTable(t *testing.T) {
	job := &priorityqueue.NonPartitionedTableAnalysisJob{
		SchemaName: "test_schema",
		TableName:  "test_table",
	}

	expectedSQL := "analyze table %n.%n"
	expectedParams := []any{"test_schema", "test_table"}

	sql, params := job.GenSQLForAnalyzeTable()

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestGenSQLForNonPartitionedTableIndex(t *testing.T) {
	job := &priorityqueue.NonPartitionedTableAnalysisJob{
		SchemaName: "test_schema",
		TableName:  "test_table",
	}

	index := "test_index"

	expectedSQL := "analyze table %n.%n index %n"
	expectedParams := []any{"test_schema", "test_table", index}

	sql, params := job.GenSQLForAnalyzeIndex(index)

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestAnalyzeNonPartitionedTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &priorityqueue.NonPartitionedTableAnalysisJob{
		SchemaName:    "test",
		TableName:     "t",
		TableStatsVer: 2,
	}

	// Before analyze table.
	handle := dom.StatsHandle()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.True(t, tblStats.Pseudo)

	job.Analyze(handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.Equal(t, int64(3), tblStats.RealtimeCount)
}

func TestAnalyzeNonPartitionedIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a), index idx1(b))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tblInfo, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	job := &priorityqueue.NonPartitionedTableAnalysisJob{
		TableID:       tblInfo.Meta().ID,
		IndexIDs:      map[int64]struct{}{1: {}, 2: {}},
		TableStatsVer: 2,
	}
	handle := dom.StatsHandle()
	// Before analyze indexes.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.False(t, tblStats.GetIdx(1).IsAnalyzed())

	valid, failReason := job.ValidateAndPrepare(tk.Session())
	require.True(t, valid)
	require.Equal(t, "", failReason)
	job.Analyze(handle, dom.SysProcTracker())
	// Check the result of analyze.
	is = dom.InfoSchema()
	tbl, err = is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats = handle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.NotNil(t, tblStats.GetIdx(1))
	require.True(t, tblStats.GetIdx(1).IsAnalyzed())
	require.NotNil(t, tblStats.GetIdx(2))
	require.True(t, tblStats.GetIdx(2).IsAnalyzed())
	// Check analyze jobs are created.
	rows := tk.MustQuery("select * from mysql.analyze_jobs").Rows()
	// Because analyze one index will analyze all indexes and all columns together, so there is only 1 job.
	require.Len(t, rows, 1)
}

func TestNonPartitionedTableValidateAndPrepare(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(session.CreateAnalyzeJobs)
	tk.MustExec("create schema example_schema")
	tk.MustExec("use example_schema")
	tk.MustExec("create table example_table1 (a int, b int, index idx(a))")
	tableInfo, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("example_schema"), model.NewCIStr("example_table1"))
	require.NoError(t, err)
	job := &priorityqueue.NonPartitionedTableAnalysisJob{
		TableID:       tableInfo.Meta().ID,
		TableStatsVer: 2,
		Weight:        3,
	}
	initJobs(tk)
	insertMultipleFinishedJobs(tk, "example_table1", "")

	se := tk.Session()
	sctx := se.(sessionctx.Context)
	valid, failReason := job.ValidateAndPrepare(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)

	// Insert some failed jobs.
	// Just failed.
	now := tk.MustQuery("select now()").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.SchemaName, job.TableName, "", now)
	// Note: The failure reason is not checked in this test because the time duration can sometimes be inaccurate.(not now)
	valid, _ = job.ValidateAndPrepare(sctx)
	require.False(t, valid)
	// Failed 10 seconds ago.
	startTime := tk.MustQuery("select now() - interval 10 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.SchemaName, job.TableName, "", startTime)
	valid, failReason = job.ValidateAndPrepare(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 2 times the average analysis duration", failReason)
	// Failed long long ago.
	startTime = tk.MustQuery("select now() - interval 300 day").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.SchemaName, job.TableName, "", startTime)
	valid, failReason = job.ValidateAndPrepare(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)
}

func TestValidateAndPrepareWhenOnlyHasFailedAnalysisRecords(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(session.CreateAnalyzeJobs)
	tk.MustExec("create schema example_schema")
	tk.MustExec("use example_schema")
	tk.MustExec("create table example_table1 (a int, b int, index idx(a))")
	tableInfo, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("example_schema"), model.NewCIStr("example_table1"))
	require.NoError(t, err)
	job := &priorityqueue.NonPartitionedTableAnalysisJob{
		TableID: tableInfo.Meta().ID,
		Weight:  2,
	}
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	valid, failReason := job.ValidateAndPrepare(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)
	// Failed long long ago.
	startTime := tk.MustQuery("select now() - interval 30 day").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.SchemaName, job.TableName, "", startTime)
	valid, failReason = job.ValidateAndPrepare(sctx)
	require.True(t, valid)
	require.Equal(t, "", failReason)

	// Failed recently.
	tenSecondsAgo := tk.MustQuery("select now() - interval 10 second").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, job.SchemaName, job.TableName, "", tenSecondsAgo)
	valid, failReason = job.ValidateAndPrepare(sctx)
	require.False(t, valid)
	require.Equal(t, "last failed analysis duration is less than 30m0s", failReason)
}

func TestNonPartitionedTableValidateAndPrepareRetriesNotReadyMV(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	const pauseBuildFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := testkit.NewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_not_ready (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	require.Eventually(t, func() bool {
		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("mv_not_ready"))
		if err != nil {
			return false
		}
		return mvTable.Meta().MaterializedView != nil &&
			mvTable.Meta().MaterializedView.GetInitBuildState() == metamodel.MVInitBuildBuilding
	}, 30*time.Second, 100*time.Millisecond)

	mvTable, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("mv_not_ready"))
	require.NoError(t, err)
	job := &priorityqueue.NonPartitionedTableAnalysisJob{
		TableID: mvTable.Meta().ID,
		Weight:  1,
	}
	var (
		failureHookCalled bool
		needRetry         bool
	)
	job.RegisterFailureHook(func(_ priorityqueue.AnalysisJob, boolRetry bool) {
		failureHookCalled = true
		needRetry = boolRetry
	})

	valid, failReason := job.ValidateAndPrepare(tk.Session().(sessionctx.Context))
	require.False(t, valid)
	require.True(t, failureHookCalled)
	require.True(t, needRetry)
	require.Contains(t, failReason, "initial build is in progress")

	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false
	select {
	case err := <-ddlDone:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting CREATE MATERIALIZED VIEW to finish")
	}
}
