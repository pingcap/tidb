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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCallAPIBeforeInitialize(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()

	t.Run("IsEmpty", func(t *testing.T) {
		isEmpty, err := pq.IsEmpty()
		require.Error(t, err)
		require.False(t, isEmpty)
	})

	t.Run("Push", func(t *testing.T) {
		err := pq.Push(nil)
		require.Error(t, err)
	})

	t.Run("Pop", func(t *testing.T) {
		poppedJob, err := pq.Pop()
		require.Error(t, err)
		require.Nil(t, poppedJob)
	})

	t.Run("GetAllJobs", func(t *testing.T) {
		jobs := pq.GetRunningJobs()
		require.Len(t, jobs, 0)
	})

	t.Run("Peek", func(t *testing.T) {
		job, err := pq.Peek()
		require.Error(t, err)
		require.Nil(t, job)
	})
}

func TestAnalysisPriorityQueue(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	handle := dom.StatsHandle()
	tk.MustExec("create table t1 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("create table t2 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()

	t.Run("Initialize", func(t *testing.T) {
		err := pq.Initialize()
		require.NoError(t, err)
		require.True(t, pq.IsInitialized())

		// Test double initialization
		err = pq.Initialize()
		require.NoError(t, err)
	})

	t.Run("IsEmpty And Pop", func(t *testing.T) {
		isEmpty, err := pq.IsEmpty()
		require.NoError(t, err)
		require.False(t, isEmpty)

		poppedJob, err := pq.Pop()
		require.NoError(t, err)
		require.NotNil(t, poppedJob)

		poppedJob, err = pq.Pop()
		require.NoError(t, err)
		require.NotNil(t, poppedJob)

		isEmpty, err = pq.IsEmpty()
		require.NoError(t, err)
		require.True(t, isEmpty)

		runningJobs := pq.GetRunningJobs()
		require.Len(t, runningJobs, 2)
	})
}

func TestRefreshLastAnalysisDuration(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("create table t2 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	// Check current jobs
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)

	// Analyze the tables
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Call RefreshLastAnalysisDuration
	pq.RefreshLastAnalysisDuration()

	// Check if the jobs' last analysis durations and weights have been updated
	updatedJob1, err := pq.Pop()
	require.NoError(t, err)
	require.NotZero(t, updatedJob1.GetWeight())
	require.NotZero(t, updatedJob1.GetIndicators().LastAnalysisDuration)
	require.NotEqual(t, time.Minute*3, updatedJob1.GetIndicators().LastAnalysisDuration)

	updatedJob2, err := pq.Pop()
	require.NoError(t, err)
	require.NotZero(t, updatedJob2.GetWeight())
	require.NotZero(t, updatedJob2.GetIndicators().LastAnalysisDuration)
	require.NotEqual(t, time.Minute*3, updatedJob2.GetIndicators().LastAnalysisDuration)

	// Check running jobs
	runningJobs := pq.GetRunningJobs()
	require.Len(t, runningJobs, 2)
}

func testProcessDMLChanges(t *testing.T, partitioned bool) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	if partitioned {
		tk.MustExec("use test")
		tk.MustExec("create table t1 (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
		statstestutil.HandleNextDDLEventWithTxn(handle)
		tk.MustExec("create table t2 (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
		statstestutil.HandleNextDDLEventWithTxn(handle)
		// Because we don't handle the DDL events in unit tests by default,
		// we need to use this way to make sure the stats record for the global table is created.
		// Insert some rows into the tables.
		tk.MustExec("insert into t1 values (11)")
		tk.MustExec("insert into t2 values (12)")
		require.NoError(t, handle.DumpStatsDeltaToKV(true))
		// Analyze the tables.
		tk.MustExec("analyze table t1")
		tk.MustExec("analyze table t2")
		require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	} else {
		tk.MustExec("use test")
		tk.MustExec("create table t1 (a int)")
		statstestutil.HandleNextDDLEventWithTxn(handle)
		tk.MustExec("create table t2 (a int)")
		statstestutil.HandleNextDDLEventWithTxn(handle)
	}
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1), (2)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	schema := ast.NewCIStr("test")
	tbl1, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t2"))
	require.NoError(t, err)

	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	// Check current jobs.
	job1, err := pq.Pop()
	require.NoError(t, err)
	require.Equal(t, tbl1.Meta().ID, job1.GetTableID())
	job2, err := pq.Pop()
	require.NoError(t, err)
	require.Equal(t, tbl2.Meta().ID, job2.GetTableID())
	valid, _ := job1.ValidateAndPrepare(tk.Session())
	require.True(t, valid)
	valid, _ = job2.ValidateAndPrepare(tk.Session())
	require.True(t, valid)
	require.NoError(t, job1.Analyze(handle, dom.SysProcTracker()))
	require.NoError(t, job2.Analyze(handle, dom.SysProcTracker()))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Insert 9 rows into t1.
	tk.MustExec("insert into t1 values (3), (4), (5), (6), (7), (8), (9), (10), (11)")
	// Insert 1 row into t2.
	tk.MustExec("insert into t2 values (3)")

	// Dump the stats to kv.
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Check if the jobs have been updated.
	updatedJob1, err := pq.Peek()
	require.NoError(t, err)
	require.NotZero(t, updatedJob1.GetWeight())
	require.Equal(t, tbl1.Meta().ID, updatedJob1.GetTableID())

	// Update some rows in t2.
	tk.MustExec("update t2 set a = 3 where a = 2")
	tk.MustExec("update t2 set a = 4 where a = 3")
	tk.MustExec("update t2 set a = 5 where a = 4")

	// Dump the stats to kv.
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Check if the jobs have been updated.
	updatedJob2, err := pq.Peek()
	require.NoError(t, err)
	require.NotZero(t, updatedJob2.GetWeight())
	require.Equal(t, tbl2.Meta().ID, updatedJob2.GetTableID(), "t2 should have higher weight due to smaller table size")
}

func TestProcessDMLChanges(t *testing.T) {
	testProcessDMLChanges(t, false)
}

func TestProcessDMLChangesPartitioned(t *testing.T) {
	testProcessDMLChanges(t, true)
}

func TestProcessDMLChangesWithRunningJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("create table t2 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	schema := ast.NewCIStr("test")
	tbl1, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t2"))
	require.NoError(t, err)
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	// Check there are no running jobs.
	runningJobs := pq.GetRunningJobs()
	require.Len(t, runningJobs, 0)
	// Check no jobs are in the queue.
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)

	// Insert 10 rows into t1.
	tk.MustExec("insert into t1 values (2), (3)")
	// Insert 2 rows into t2.
	tk.MustExec("insert into t2 values (2), (3)")
	// Dump the stats to kv.
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Pop the t1 job.
	job1, err := pq.Pop()
	require.NoError(t, err)
	require.Equal(t, tbl1.Meta().ID, job1.GetTableID())

	// Check if the running job is still in the queue.
	runningJobs = pq.GetRunningJobs()
	require.Len(t, runningJobs, 1)

	job2, err := pq.Pop()
	require.NoError(t, err)
	require.NotZero(t, job2.GetWeight())
	require.Equal(t, tbl2.Meta().ID, job2.GetTableID(), "t1 should not be in the queue since it's a running job")

	// Analyze the job.
	valid, _ := job1.ValidateAndPrepare(tk.Session())
	require.True(t, valid)
	require.NoError(t, job1.Analyze(handle, dom.SysProcTracker()))

	// Add more rows to t1.
	tk.MustExec("insert into t1 values (4), (5), (6), (7), (8), (9), (10), (11), (12), (13)")
	// Dump the stats to kv.
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Check if the jobs have been updated.
	job1, err = pq.Pop()
	require.NoError(t, err)
	require.NotZero(t, job1.GetWeight())
	require.Equal(t, tbl1.Meta().ID, job1.GetTableID(), "t1 has been removed from running jobs and should be in the queue")
}

func TestRequeueMustRetryJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database example_schema")
	tk.MustExec("use example_schema")
	tk.MustExec("create table example_table (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	initJobs(tk)
	insertMultipleFinishedJobs(tk, "example_table", "")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	// Insert the failed job.
	// Just failed.
	now := tk.MustQuery("select now()").Rows()[0][0].(string)
	insertFailedJobWithStartTime(tk, "example_schema", "example_table", "", now)

	// Insert some rows.
	tk.MustExec("insert into example_table values (11), (12), (13), (14), (15), (16), (17), (18), (19)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	job, err := pq.Pop()
	require.NoError(t, err)
	require.NotNil(t, job)
	sctx := tk.Session().(sessionctx.Context)
	ok, _ := job.ValidateAndPrepare(sctx)
	require.False(t, ok)

	// Insert more rows.
	tk.MustExec("insert into example_table values (20), (21), (22), (23), (24), (25), (26), (27), (28), (29)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()
	l, err := pq.Len()
	require.NoError(t, err)
	require.Equal(t, 0, l)

	// Requeue the failed jobs.
	pq.RequeueMustRetryJobs()
	l, err = pq.Len()
	require.NoError(t, err)
	require.Equal(t, 1, l)
}

func TestProcessDMLChangesWithLockedTables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("create table t2 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	schema := ast.NewCIStr("test")
	tbl1, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t2"))
	require.NoError(t, err)

	// Check current jobs.
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tbl1.Meta().ID, job.GetTableID())

	// Lock t1.
	tk.MustExec("lock stats t1")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Check if the jobs have been updated.
	job, err = pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tbl2.Meta().ID, job.GetTableID())

	// Unlock t1.
	tk.MustExec("unlock stats t1")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Check if the jobs have been updated.
	l, err := pq.Len()
	require.NoError(t, err)
	require.Equal(t, 2, l)
}

func TestProcessDMLChangesWithLockedPartitionsAndDynamicPruneMode(t *testing.T) {
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("insert into t1 values (1)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	tk.MustExec("analyze table t1")
	tk.MustExec("set global tidb_partition_prune_mode = 'dynamic'")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	// Insert more rows into partition p0.
	tk.MustExec("insert into t1 partition (p0) values (2), (3), (4), (5), (6), (7), (8), (9)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	schema := ast.NewCIStr("test")
	tbl, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t1"))
	require.NoError(t, err)

	// Check current jobs.
	job, err := pq.Peek()
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	require.Equal(t, tableID, job.GetTableID())

	// Lock the whole table.
	tk.MustExec("lock stats t1")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// No jobs should be in the queue.
	l, err := pq.Len()
	require.NoError(t, err)
	require.Equal(t, 0, l)

	// Unlock the whole table.
	tk.MustExec("unlock stats t1")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Check if the jobs have been updated.
	job, err = pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableID, job.GetTableID())
}

func TestProcessDMLChangesWithLockedPartitionsAndStaticPruneMode(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("set global tidb_partition_prune_mode = 'static'")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	tk.MustExec("analyze table t1")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	schema := ast.NewCIStr("test")
	tbl, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t1"))
	require.NoError(t, err)

	// Insert more rows into partition p0.
	tk.MustExec("insert into t1 partition (p0) values (2), (3), (4), (5), (6), (7), (8), (9)")
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	// Check current jobs.
	job, err := pq.Peek()
	require.NoError(t, err)
	pid := tbl.Meta().Partition.Definitions[0].ID
	require.Equal(t, pid, job.GetTableID())

	// Lock partition p0.
	tk.MustExec("lock stats t1 partition p0")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// No jobs should be in the queue.
	l, err := pq.Len()
	require.NoError(t, err)
	require.Equal(t, 0, l)

	// Unlock partition p0.
	tk.MustExec("unlock stats t1 partition (p0)")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Check if the jobs have been updated.
	job, err = pq.Peek()
	require.NoError(t, err)
	pid = tbl.Meta().Partition.Definitions[0].ID
	require.Equal(t, pid, job.GetTableID())
}

func TestPQCanBeClosedAndReInitialized(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	// Close the priority queue.
	pq.Close()

	// Check if the priority queue is closed.
	require.False(t, pq.IsInitialized())

	// Re-initialize the priority queue.
	require.NoError(t, pq.Initialize())

	// Check if the priority queue is initialized.
	require.True(t, pq.IsInitialized())
}

func TestPQHandlesTableDeletionGracefully(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int)")
	statstestutil.HandleNextDDLEventWithTxn(handle)
	tk.MustExec("insert into t1 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	// Check the priority queue is not empty.
	l, err := pq.Len()
	require.NoError(t, err)
	require.NotEqual(t, 0, l)

	tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)

	// Drop the table and mock the table stats is removed from the cache.
	tk.MustExec("drop table t1")
	deleteEvent := findEvent(handle.DDLEventCh(), model.ActionDropTable)
	require.NotNil(t, deleteEvent)
	err = statstestutil.HandleDDLEventWithTxn(handle, deleteEvent)
	require.NoError(t, err)
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Make sure handle.Get() returns false.
	_, ok := handle.Get(tbl.Meta().ID)
	require.False(t, ok)

	require.NotPanics(t, func() {
		pq.RefreshLastAnalysisDuration()
	})
}
