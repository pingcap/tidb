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

	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCallAPIBeforeInitialize(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	pq := priorityqueue.NewAnalysisPriorityQueueV2(handle)
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

func TestAnalysisPriorityQueueV2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueueV2(handle)
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
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	pq := priorityqueue.NewAnalysisPriorityQueueV2(handle)
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

func TestProcessDMLChanges(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	schema := pmodel.NewCIStr("test")
	tbl1, err := dom.InfoSchema().TableByName(ctx, schema, pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(ctx, schema, pmodel.NewCIStr("t2"))
	require.NoError(t, err)

	pq := priorityqueue.NewAnalysisPriorityQueueV2(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize())

	// Check current jobs.
	job1, err := pq.Pop()
	require.NoError(t, err)
	require.Equal(t, tbl1.Meta().ID, job1.GetTableID())
	job2, err := pq.Pop()
	require.NoError(t, err)
	require.Equal(t, tbl2.Meta().ID, job2.GetTableID())
	require.NoError(t, job1.Analyze(handle, dom.SysProcTracker()))
	require.NoError(t, job2.Analyze(handle, dom.SysProcTracker()))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Insert 10 rows into t1.
	tk.MustExec("insert into t1 values (2), (3), (4), (5), (6), (7), (8), (9), (10), (11)")
	// Insert 2 rows into t2.
	tk.MustExec("insert into t2 values (2), (3)")

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

func TestProcessDMLChangesWithRunningJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	ctx := context.Background()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))
	schema := pmodel.NewCIStr("test")
	tbl1, err := dom.InfoSchema().TableByName(ctx, schema, pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(ctx, schema, pmodel.NewCIStr("t2"))
	require.NoError(t, err)
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueueV2(handle)
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
