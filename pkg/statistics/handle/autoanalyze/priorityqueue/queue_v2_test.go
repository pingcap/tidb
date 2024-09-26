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

	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAnalysisPriorityQueueV2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	handle := dom.StatsHandle()
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

	t.Run("Push and Pop", func(t *testing.T) {
		// TODO: avoid manually inserting jobs after the queue has been initialized.
		// This is a temporary solution to test the Push and Pop methods.
		job1 := &priorityqueue.NonPartitionedTableAnalysisJob{
			TableID: 1,
			Weight:  10,
		}
		job2 := &priorityqueue.NonPartitionedTableAnalysisJob{
			TableID: 2,
			Weight:  20,
		}

		err := pq.Push(job1)
		require.NoError(t, err)
		err = pq.Push(job2)
		require.NoError(t, err)

		require.False(t, pq.IsEmpty())

		poppedJob, err := pq.Pop()
		require.NoError(t, err)
		require.Equal(t, job2, poppedJob)

		poppedJob, err = pq.Pop()
		require.NoError(t, err)
		require.Equal(t, job1, poppedJob)

		require.True(t, pq.IsEmpty())
	})
}

func TestIsWithinTimeWindow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	handle := dom.StatsHandle()
	pq := priorityqueue.NewAnalysisPriorityQueueV2(handle)
	err := pq.Initialize()
	require.NoError(t, err)
	require.True(t, pq.IsWithinTimeWindow())
	pq.Close()

	tk.MustExec("set global tidb_auto_analyze_start_time = '00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time = '00:00 +0000'")
	// Reset the priority queue with the new time window.
	pq = priorityqueue.NewAnalysisPriorityQueueV2(handle)
	err = pq.Initialize()
	require.NoError(t, err)
	require.False(t, pq.IsWithinTimeWindow())
	pq.Close()
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

	// Add some jobs to the queue
	// TODO: avoid manually inserting jobs after the queue has been initialized.
	// This is a temporary solution to test the RefreshLastAnalysisDuration method.
	job1 := &priorityqueue.NonPartitionedTableAnalysisJob{
		TableID:     tbl1.Meta().ID,
		TableSchema: schema.O,
		TableName:   tbl1.Meta().Name.O,
	}
	err = pq.Push(job1)
	require.NoError(t, err)
	job2 := &priorityqueue.NonPartitionedTableAnalysisJob{
		TableID:     tbl2.Meta().ID,
		TableSchema: schema.O,
		TableName:   tbl2.Meta().Name.O,
	}
	err = pq.Push(job2)
	require.NoError(t, err)

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

	updatedJob2, err := pq.Pop()
	require.NoError(t, err)
	require.NotZero(t, updatedJob2.GetWeight())
	require.NotZero(t, updatedJob2.GetIndicators().LastAnalysisDuration)
}
