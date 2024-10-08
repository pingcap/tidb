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

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func findEvent(eventCh <-chan *notifier.SchemaChangeEvent, eventType model.ActionType) *notifier.SchemaChangeEvent {
	// Find the target event.
	for {
		event := <-eventCh
		if event.GetType() == eventType {
			return event
		}
	}
}

func TestTruncateTable(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Truncate table.
	testKit.MustExec("truncate table t")

	// Find the truncate table partition event.
	truncateTableEvent := findEvent(h.DDLEventCh(), model.ActionTruncateTable)

	// Handle the truncate table event.
	require.NoError(t, h.HandleDDLEvent(truncateTableEvent))

	ctx := context.Background()
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

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)

	// Truncate table.
	testKit.MustExec("truncate table t")

	// Find the truncate table partition event.
	truncateTableEvent := findEvent(h.DDLEventCh(), model.ActionTruncateTable)

	// Handle the truncate table event.
	require.NoError(t, h.HandleDDLEvent(truncateTableEvent))

	ctx := context.Background()
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
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	// Insert some data.
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Drop table.
	testKit.MustExec("drop table t")

	// Find the drop table partition event.
	dropTableEvent := findEvent(h.DDLEventCh(), model.ActionDropTable)

	// Handle the drop table event.
	require.NoError(t, h.HandleDDLEvent(dropTableEvent))

	ctx := context.Background()
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

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)

	// Drop table.
	testKit.MustExec("drop table t")

	// Find the drop table partition event.
	dropTableEvent := findEvent(h.DDLEventCh(), model.ActionDropTable)

	// Handle the drop table event.
	require.NoError(t, h.HandleDDLEvent(dropTableEvent))

	ctx := context.Background()
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
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Truncate table partition.
	testKit.MustExec("alter table t truncate partition p0")

	// Find the truncate table partition event.
	truncateTablePartitionEvent := findEvent(h.DDLEventCh(), model.ActionTruncateTablePartition)

	// Handle the truncate table partition event.
	require.NoError(t, h.HandleDDLEvent(truncateTablePartitionEvent))

	ctx := context.Background()
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
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Drop table partition.
	testKit.MustExec("alter table t drop partition p0")

	// Find the drop table partition event.
	dropTablePartitionEvent := findEvent(h.DDLEventCh(), model.ActionDropTablePartition)

	// Handle the drop table partition event.
	require.NoError(t, h.HandleDDLEvent(dropTablePartitionEvent))

	ctx := context.Background()
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
	testKit.MustExec("create table t1 (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	testKit.MustExec("create table t2 (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl1, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()
	tbl2, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo2 := tbl2.Meta()
	h := do.StatsHandle()
	// Analyze table.
	testKit.MustExec("analyze table t1")
	testKit.MustExec("analyze table t2")
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))
	// Insert some data.
	testKit.MustExec("insert into t1 values (1,2),(2,2)")
	testKit.MustExec("insert into t2 values (1,2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), do.InfoSchema()))

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo2.ID, job.GetTableID())

	// Exchange table partition.
	testKit.MustExec("alter table t1 exchange partition p0 with table t2")

	// Find the exchange table partition event.
	exchangeTablePartitionEvent := findEvent(h.DDLEventCh(), model.ActionExchangeTablePartition)

	// Handle the exchange table partition event.
	require.NoError(t, h.HandleDDLEvent(exchangeTablePartitionEvent))

	ctx := context.Background()
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
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range (c1) (partition p0 values less than (10), partition p1 values less than (20))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Reorganize table partition.
	testKit.MustExec("alter table t reorganize partition p0 into (partition p0 values less than (5), partition p2 values less than (10))")

	// Find the reorganize table partition event.
	reorganizeTablePartitionEvent := findEvent(h.DDLEventCh(), model.ActionReorganizePartition)

	// Handle the reorganize table partition event.
	require.NoError(t, h.HandleDDLEvent(reorganizeTablePartitionEvent))

	ctx := context.Background()
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
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Alter table partitioning.
	testKit.MustExec("alter table t partition by range columns (c1) (partition p0 values less than (5), partition p1 values less than (10))")

	// Find the alter table partitioning event.
	alterTablePartitioningEvent := findEvent(h.DDLEventCh(), model.ActionAlterTablePartitioning)

	// Handle the alter table partitioning event.
	require.NoError(t, h.HandleDDLEvent(alterTablePartitioningEvent))

	ctx := context.Background()
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
	testKit.MustExec("create table t (c1 int, c2 int, index idx(c1, c2)) partition by range columns (c1) (partition p0 values less than (5), partition p1 values less than (10))")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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

	pq := priorityqueue.NewAnalysisPriorityQueueV2(h)
	defer pq.Close()
	require.NoError(t, pq.Initialize())
	isEmpty, err := pq.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)
	job, err := pq.Peek()
	require.NoError(t, err)
	require.Equal(t, tableInfo.ID, job.GetTableID())

	// Remove partitioning.
	testKit.MustExec("alter table t remove partitioning")

	// Find the remove partitioning event.
	removePartitioningEvent := findEvent(h.DDLEventCh(), model.ActionRemovePartitioning)

	// Handle the remove partitioning event.
	require.NoError(t, h.HandleDDLEvent(removePartitioningEvent))

	ctx := context.Background()
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
