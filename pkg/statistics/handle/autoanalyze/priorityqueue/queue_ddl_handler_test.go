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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

// enableAutoAnalyze enables auto-analyze for the test and restores it on cleanup.
// In tests, auto-analyze is disabled by default, so tests that need it enabled
// should call this helper at the beginning.
func enableAutoAnalyze(t *testing.T, tk *testkit.TestKit) {
	bakRunAutoAnalyze := vardef.RunAutoAnalyze.Load()
	tk.MustExec("set @@global.tidb_enable_auto_analyze = 1")
	t.Cleanup(func() {
		tk.MustExec(fmt.Sprintf("set @@global.tidb_enable_auto_analyze = %t", bakRunAutoAnalyze))
	})
}

func TestHandleDDLEventsWithRunningJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	enableAutoAnalyze(t, tk)
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
	schema := ast.NewCIStr("test")
	tbl1, err := dom.InfoSchema().TableByName(ctx, schema, ast.NewCIStr("t1"))
	require.NoError(t, err)
	tk.MustExec("analyze table t1")
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueue(handle)
	defer pq.Close()
	require.NoError(t, pq.Initialize(ctx))

	// Check there are no running jobs.
	runningJobs := pq.GetRunningJobs()
	require.Len(t, runningJobs, 0)
	// Check no jobs are in the queue.
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)

	// Insert 10 rows into t1.
	tk.MustExec("insert into t1 values (2), (3)")
	// Dump the stats to kv.
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(ctx, dom.InfoSchema()))

	// Process the DML changes.
	pq.ProcessDMLChanges()

	// Pop the t1 job.
	job1, err := pq.Pop()
	require.NoError(t, err)
	require.Equal(t, tbl1.Meta().ID, job1.GetTableID())
	valid, _ := job1.ValidateAndPrepare(tk.Session())
	require.True(t, valid)

	// Check if the running job is still in the queue.
	runningJobs = pq.GetRunningJobs()
	require.Len(t, runningJobs, 1)

	down := make(chan struct{})
	fp := "github.com/pingcap/tidb/pkg/executor/mockStuckAnalyze"
	go func() {
		defer close(down)
		require.NoError(t, failpoint.Enable(fp, "return(1)"))
		require.NoError(t, job1.Analyze(handle, dom.SysProcTracker()))
	}()

	// Create a new index on t1.
	tk.MustExec("alter table t1 add index idx (a)")

	// Find the add index event.
	addIndexEvent := statstestutil.FindEvent(handle.DDLEventCh(), model.ActionAddIndex)

	// Handle the add index event.
	err = statstestutil.HandleDDLEventWithTxn(handle, addIndexEvent)
	require.NoError(t, err)

	// Handle the add index event in priority queue.
	require.NoError(t, statsutil.CallWithSCtx(handle.SPool(), func(sctx sessionctx.Context) error {
		return pq.HandleDDLEvent(ctx, sctx, addIndexEvent)
	}, statsutil.FlagWrapTxn))

	// Check the queue is empty.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)

	// Requeue the running jobs.
	pq.RequeueMustRetryJobs()

	// Still no jobs in the queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)

	require.NoError(t, failpoint.Disable(fp))
	// Wait for the analyze job to finish.
	<-down

	// Requeue the running jobs again.
	pq.RequeueMustRetryJobs()

	// Check the job is in the queue.
	job, err := pq.Pop()
	require.NoError(t, err)
	require.Equal(t, tbl1.Meta().ID, job.GetTableID())
	require.True(t, job.HasNewlyAddedIndex())
}

func TestTruncateTable(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	statstestutil.HandleNextDDLEventWithTxn(h)
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Truncate table.
	testKit.MustExec("truncate table t")

	// Find the truncate table partition event.
	truncateTableEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionTruncateTable)

	// Handle the truncate table event.
	err = statstestutil.HandleDDLEventWithTxn(h, truncateTableEvent)
	require.NoError(t, err)

	sctx := testKit.Session().(sessionctx.Context)
	// Handle the truncate table event in priority queue.
	require.NoError(t, pq.HandleDDLEvent(ctx, sctx, truncateTableEvent))

	// The table is truncated, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestTruncatePartitionedTableWithStaticPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set global tidb_partition_prune_mode='static'")
	testTruncatePartitionedTable(t, do, testKit)
}

func TestTruncatePartitionedTableWithDynamicPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set global tidb_partition_prune_mode='dynamic'")
	testTruncatePartitionedTable(t, do, testKit)
}

func testTruncatePartitionedTable(
	t *testing.T,
	do *domain.Domain,
	testKit *testkit.TestKit,
) {
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	h := do.StatsHandle()
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)

	// Truncate table.
	testKit.MustExec("truncate table t")

	// Find the truncate table partition event.
	truncateTableEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionTruncateTable)

	// Handle the truncate table event.
	err = statstestutil.HandleDDLEventWithTxn(h, truncateTableEvent)
	require.NoError(t, err)

	sctx := testKit.Session().(sessionctx.Context)
	// Handle the truncate table event in priority queue.
	require.NoError(t, pq.HandleDDLEvent(ctx, sctx, truncateTableEvent))

	// The table is truncated, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestDropTable(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	statstestutil.HandleNextDDLEventWithTxn(h)
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Drop table.
	testKit.MustExec("drop table t")

	// Find the drop table partition event.
	dropTableEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionDropTable)

	// Handle the drop table event.
	err = statstestutil.HandleDDLEventWithTxn(h, dropTableEvent)
	require.NoError(t, err)

	sctx := testKit.Session().(sessionctx.Context)
	// Handle the drop table event in priority queue.
	require.NoError(t, pq.HandleDDLEvent(ctx, sctx, dropTableEvent))

	// The table is dropped, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestDropPartitionedTableWithStaticPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set global tidb_partition_prune_mode='static'")
	testDropPartitionedTable(t, do, testKit)
}

func TestDropPartitionedTableWithDynamicPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set global tidb_partition_prune_mode='dynamic'")
	testDropPartitionedTable(t, do, testKit)
}

func testDropPartitionedTable(
	t *testing.T,
	do *domain.Domain,
	testKit *testkit.TestKit,
) {
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	h := do.StatsHandle()
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)

	// Drop table.
	testKit.MustExec("drop table t")

	// Find the drop table partition event.
	dropTableEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionDropTable)

	// Handle the drop table event.
	err = statstestutil.HandleDDLEventWithTxn(h, dropTableEvent)
	require.NoError(t, err)

	sctx := testKit.Session().(sessionctx.Context)
	// Handle the drop table event in priority queue.
	require.NoError(t, pq.HandleDDLEvent(ctx, sctx, dropTableEvent))

	// The table is dropped, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestTruncateTablePartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Truncate table partition.
	testKit.MustExec("alter table t truncate partition p0")

	// Find the truncate table partition event.
	truncateTablePartitionEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionTruncateTablePartition)

	// Handle the truncate table partition event.
	err = statstestutil.HandleDDLEventWithTxn(h, truncateTablePartitionEvent)
	require.NoError(t, err)

	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			// Handle the truncate table partition event in priority queue.
			require.NoError(t, pq.HandleDDLEvent(ctx, sctx, truncateTablePartitionEvent))
			return nil
		}, statsutil.FlagWrapTxn),
	)

	// The table partition is truncated, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestDropTablePartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Drop table partition.
	testKit.MustExec("alter table t drop partition p0")

	// Find the drop table partition event.
	dropTablePartitionEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionDropTablePartition)

	// Handle the drop table partition event.
	err = statstestutil.HandleDDLEventWithTxn(h, dropTablePartitionEvent)
	require.NoError(t, err)

	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			// Handle the drop table partition event in priority queue.
			require.NoError(t, pq.HandleDDLEvent(ctx, sctx, dropTablePartitionEvent))
			return nil
		}, statsutil.FlagWrapTxn),
	)

	// The table partition is dropped, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestExchangeTablePartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t1 (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	testKit.MustExec("create table t2 (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()
	tbl2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo2 := tbl2.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t1")
	testKit.MustExec("analyze table t2")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t1 values (1,2),(2,2),(3,3),(4,4)")
	testKit.MustExec("insert into t2 values (1,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo2.ID, job.GetTableID())

	// Exchange table partition.
	testKit.MustExec("alter table t1 exchange partition p0 with table t2")

	// Find the exchange table partition event.
	exchangeTablePartitionEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionExchangeTablePartition)

	// Handle the exchange table partition event.
	err = statstestutil.HandleDDLEventWithTxn(h, exchangeTablePartitionEvent)
	require.NoError(t, err)

	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			// Handle the exchange table partition event in priority queue.
			require.NoError(t, pq.HandleDDLEvent(ctx, sctx, exchangeTablePartitionEvent))
			return nil
		}, statsutil.FlagWrapTxn),
	)

	// They are exchanged, the job should be updated to the exchanged table.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err = pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo1.ID, job.GetTableID(), "The job should be updated to the exchanged table")
}

func TestReorganizeTablePartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Reorganize table partition.
	testKit.MustExec("alter table t reorganize partition p0 into (partition p0 values less than (5), partition p2 values less than (10))")

	// Find the reorganize table partition event.
	reorganizeTablePartitionEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionReorganizePartition)

	// Handle the reorganize table partition event.
	err = statstestutil.HandleDDLEventWithTxn(h, reorganizeTablePartitionEvent)
	require.NoError(t, err)

	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			// Handle the reorganize table partition event in priority queue.
			require.NoError(t, pq.HandleDDLEvent(ctx, sctx, reorganizeTablePartitionEvent))
			return nil
		}, statsutil.FlagWrapTxn),
	)

	// The table partition is reorganized, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestAlterTablePartitioning(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Alter table partitioning.
	testKit.MustExec("alter table t partition by range columns (c1) (partition p0 values less than (5), partition p1 values less than (10))")

	// Find the alter table partitioning event.
	alterTablePartitioningEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionAlterTablePartitioning)

	// Handle the alter table partitioning event.
	err = statstestutil.HandleDDLEventWithTxn(h, alterTablePartitioningEvent)
	require.NoError(t, err)

	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			// Handle the alter table partitioning event in priority queue.
			require.NoError(t, pq.HandleDDLEvent(ctx, sctx, alterTablePartitioningEvent))
			return nil
		}, statsutil.FlagWrapTxn),
	)

	// The table partitioning is altered, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestRemovePartitioning(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range columns (c1) (partition p0 values less than (5), partition p1 values less than (10))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Remove partitioning.
	testKit.MustExec("alter table t remove partitioning")

	// Find the remove partitioning event.
	removePartitioningEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionRemovePartitioning)

	// Handle the remove partitioning event.
	err = statstestutil.HandleDDLEventWithTxn(h, removePartitioningEvent)
	require.NoError(t, err)

	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			// Handle the remove partitioning event in priority queue.
			require.NoError(t, pq.HandleDDLEvent(ctx, sctx, removePartitioningEvent))
			return nil
		}, statsutil.FlagWrapTxn),
	)

	// The table partitioning is removed, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestDropSchemaEventWithDynamicPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range columns (c1) (partition p0 values less than (5), partition p1 values less than (10))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Create a non-partitioned table.
	testKit.MustExec("create table t2 (c1 int, c2 int, index idx(c1, c2))")
	testKit.MustExec("analyze table t2")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t2 values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())
	l, err := pq.Len()
	require.NoError(t, err)
	require.Equal(t, l, 2)

	// Drop schema.
	testKit.MustExec("drop database test")

	// Find the drop schema event.
	dropSchemaEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionDropSchema)
	require.NotNil(t, dropSchemaEvent)

	// Handle the drop schema event.
	err = statstestutil.HandleDDLEventWithTxn(h, dropSchemaEvent)
	require.NoError(t, err)

	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			require.NoError(t, pq.HandleDDLEvent(ctx, sctx, dropSchemaEvent))
			return nil
		}, statsutil.FlagWrapTxn),
	)

	// The table should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestDropSchemaEventWithStaticPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	h := do.StatsHandle()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range columns (c1) (partition p0 values less than (5), partition p1 values less than (10))")
	statstestutil.HandleNextDDLEventWithTxn(h)
	testKit.MustExec("set global tidb_partition_prune_mode='static'")
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(6,6)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	l, err := pq.Len()
	require.NoError(t, err)
	require.Equal(t, l, 2)

	// Drop schema.
	testKit.MustExec("drop database test")

	// Find the drop schema event.
	dropSchemaEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionDropSchema)
	require.NotNil(t, dropSchemaEvent)

	// Handle the drop schema event.
	err = statstestutil.HandleDDLEventWithTxn(h, dropSchemaEvent)
	require.NoError(t, err)

	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			require.NoError(t, pq.HandleDDLEvent(ctx, sctx, dropSchemaEvent))
			return nil
		}, statsutil.FlagWrapTxn),
	)

	// The table should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

const tiflashReplicaLease = 600 * time.Millisecond

func TestVectorIndexTriggerAutoAnalyze(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, tiflashReplicaLease, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()
	dom := domain.GetDomain(tk.Session())
	h := dom.StatsHandle()

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

	tk.MustExec("create table t (a int, b vector, c vector(3), d vector(4));")
	tk.MustExec("analyze table t")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	tk.MustExec("alter table t set tiflash replica 1;")
	testkit.SetTiFlashReplica(t, dom, "test", "t")
	defer pq.Close()
	require.NoError(t, pq.Initialize(context.Background()))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)

	tk.MustExec("alter table t add vector index vecIdx1((vec_cosine_distance(d))) USING HNSW;")

	addIndexEvent := statstestutil.FindEventWithTimeout(h.DDLEventCh(), model.ActionAddColumnarIndex, 1)
	// No event is found
	require.Nil(t, addIndexEvent)
}

func TestAddIndexTriggerAutoAnalyzeWithStatsVersion1(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set @@global.tidb_analyze_version=1;")
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range columns (c1) (partition p0 values less than (5), partition p1 values less than (10))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Add two indexes.
	testKit.MustExec("alter table t add index idx1(c1)")
	testKit.MustExec("alter table t add index idx2(c2)")

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize(context.Background()))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())
	valid, _ := job.ValidateAndPrepare(testKit.Session())
	require.True(t, valid)
	require.NoError(t, job.Analyze(h, do.SysProcTracker()))

	// Check the stats of the indexes.
	tableStats := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.True(t, tableStats.GetIdx(1).IsAnalyzed())
	require.True(t, tableStats.GetIdx(2).IsAnalyzed())
	require.True(t, tableStats.GetIdx(3).IsAnalyzed())
}

func TestAddIndexTriggerAutoAnalyzeWithStatsVersion1AndStaticPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	// Enable the static partition mode.
	testKit.MustExec("set @@global.tidb_partition_prune_mode='static'")
	testKit.MustExec("set @@global.tidb_analyze_version=1;")
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range columns (c1) (partition p0 values less than (5), partition p1 values less than (10))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	p0ID := tableInfo.GetPartitionInfo().Definitions[0].ID
	p1ID := tableInfo.GetPartitionInfo().Definitions[1].ID
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize(context.Background()))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)

	// Add two indexes.
	testKit.MustExec("alter table t add index idx1(c1)")
	testKit.MustExec("alter table t add index idx2(c2)")
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()
	addIndexEvent1 := statstestutil.FindEvent(h.DDLEventCh(), model.ActionAddIndex)
	require.NotNil(t, addIndexEvent1)
	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			require.NoError(t, pq.HandleDDLEvent(context.Background(), sctx, addIndexEvent1))
			return nil
		}, statsutil.FlagWrapTxn),
	)
	addIndexEvent2 := statstestutil.FindEvent(h.DDLEventCh(), model.ActionAddIndex)
	require.NotNil(t, addIndexEvent2)
	require.NoError(t, statsutil.CallWithSCtx(
		h.SPool(),
		func(sctx sessionctx.Context) error {
			require.NoError(t, pq.HandleDDLEvent(context.Background(), sctx, addIndexEvent2))
			return nil
		}, statsutil.FlagWrapTxn),
	)
	job, err := pq.Pop()
	require.NoError(t, err)
	valid, _ := job.ValidateAndPrepare(testKit.Session())
	require.True(t, valid)
	require.NoError(t, job.Analyze(h, do.SysProcTracker()))
	job, err = pq.Pop()
	require.NoError(t, err)
	valid, _ = job.ValidateAndPrepare(testKit.Session())
	require.True(t, valid)
	require.NoError(t, job.Analyze(h, do.SysProcTracker()))

	// Check the stats of the indexes for each partition.
	tableStats := h.GetPhysicalTableStats(p0ID, tableInfo)
	require.True(t, tableStats.GetIdx(1).IsAnalyzed())
	require.True(t, tableStats.GetIdx(2).IsAnalyzed())
	require.True(t, tableStats.GetIdx(3).IsAnalyzed())
	tableStats = h.GetPhysicalTableStats(p1ID, tableInfo)
	require.True(t, tableStats.GetIdx(1).IsAnalyzed())
	require.True(t, tableStats.GetIdx(2).IsAnalyzed())
	require.True(t, tableStats.GetIdx(3).IsAnalyzed())
}

func TestCreateIndexUnderDDLAnalyzeEnabled(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int)")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	statstestutil.HandleNextDDLEventWithTxn(h)
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)

	// enable ddl analyze.
	testKit.MustExec("set @@tidb_stats_update_during_ddl = 1")
	// create index.
	testKit.MustExec("alter table t add index idx(c1, c2)")

	// Find the create index event.
	addIndexEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionAddIndex)
	tblInfo, idxInfo, analyzed := addIndexEvent.GetAddIndexInfo()
	require.Equal(t, tableInfo.ID, tblInfo.ID)
	require.Equal(t, analyzed, true)
	require.Equal(t, idxInfo[0].Name.L, "idx")

	// Handle the add index event.
	err = statstestutil.HandleDDLEventWithTxn(h, addIndexEvent)
	require.NoError(t, err)

	sctx := testKit.Session().(sessionctx.Context)
	// Handle the add index event in priority queue.
	require.NoError(t, pq.HandleDDLEvent(ctx, sctx, addIndexEvent))

	// The table is truncated, the job should be removed from the priority queue.
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)

	// modify column.
	testKit.MustExec("alter table t modify c1 varchar(10)")

	// Find the modify column event.
	modifyColumnEvent := statstestutil.FindEvent(h.DDLEventCh(), model.ActionModifyColumn)
	tblInfo, columnInfo, analyzed := modifyColumnEvent.GetModifyColumnInfo()
	require.Equal(t, tableInfo.ID, tblInfo.ID)
	require.Equal(t, analyzed, true)
	require.Equal(t, columnInfo[0].Name.L, "c1")
}

func TestTurnOffAutoAnalyze(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	enableAutoAnalyze(t, testKit)
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	statstestutil.HandleNextDDLEventWithTxn(h)
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueue(h)
	defer pq.Close()
	ctx := context.Background()
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Truncate table.
	testKit.MustExec("truncate table t")

	// Find the truncate table event.
	truncateTableEvent := findEvent(h.DDLEventCh(), model.ActionTruncateTable)

	// Disable the auto analyze.
	testKit.MustExec("set @@global.tidb_enable_auto_analyze = 0;")

	// Handle the truncate table event.
	err = statstestutil.HandleDDLEventWithTxn(h, truncateTableEvent)
	require.NoError(t, err)

	sctx := testKit.Session().(sessionctx.Context)
	// Handle the truncate table event in priority queue.
	require.NoError(t, pq.HandleDDLEvent(ctx, sctx, truncateTableEvent))

	// Because the auto analyze is turned off, the priority queue should be closed properly.
	_, err = pq.IsEmpty()
	require.ErrorContains(t, err, "priority queue not initialized")

	// The priority queue can be re-initialized after turning on the auto analyze.
	// We manually mock it here.
	require.NoError(t, pq.Initialize(ctx))
	isEmpty, err = pq.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}
