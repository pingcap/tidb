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
		require.False(t, pq.IsEmpty())

		poppedJob, err := pq.Pop()
		require.NoError(t, err)
		require.NotNil(t, poppedJob)

		poppedJob, err = pq.Pop()
		require.NoError(t, err)
		require.NotNil(t, poppedJob)

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
	oldLastAnalysisDuration1 := job1.GetIndicators().LastAnalysisDuration
	require.Equal(t, time.Minute*30, oldLastAnalysisDuration1)
	job2, err := pq.Pop()
	require.NoError(t, err)
	require.Equal(t, tbl2.Meta().ID, job2.GetTableID())
	oldLastAnalysisDuration2 := job2.GetIndicators().LastAnalysisDuration
	require.Equal(t, time.Minute*30, oldLastAnalysisDuration2)

	// Push the jobs back to the queue
	require.NoError(t, pq.Push(job1))
	require.NoError(t, pq.Push(job2))

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
	require.NotEqual(t, oldLastAnalysisDuration1, updatedJob1.GetIndicators().LastAnalysisDuration)

	updatedJob2, err := pq.Pop()
	require.NoError(t, err)
	require.NotZero(t, updatedJob2.GetWeight())
	require.NotZero(t, updatedJob2.GetIndicators().LastAnalysisDuration)
	require.NotEqual(t, oldLastAnalysisDuration2, updatedJob2.GetIndicators().LastAnalysisDuration)
}
