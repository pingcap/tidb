// Copyright 2017 PingCAP, Inc.
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

package handle_test

import (
	"testing"
	"time"

	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	handle "github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestDDLAfterLoad(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		testKit.MustExec("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
	// add column
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()

	sctx := mock.NewContext()
	count := cardinality.ColumnGreaterRowCount(sctx, statsTbl, types.NewDatum(recordCount+1), tableInfo.Columns[0].ID)
	require.Equal(t, 0.0, count)
	count = cardinality.ColumnGreaterRowCount(sctx, statsTbl, types.NewDatum(recordCount+1), tableInfo.Columns[2].ID)
	require.Equal(t, 333, int(count))
}

func TestDDLTable(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsTbl := h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	testKit.MustExec("create table t1 (c1 int, c2 int, index idx(c1))")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	testKit.MustExec("truncate table t1")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	// For FK table's CreateTable Event
	// https://github.com/pingcap/tidb/issues/53652
	testKit.MustExec("create table t_parent (id int primary key)")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t_parent"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	testKit.MustExec("create table t_child (id int primary key, pid int, foreign key (pid) references t_parent(id) on delete cascade on update cascade);")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t_child"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
}

func TestDDLHistogram(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	<-h.DDLEventCh()
	testKit.MustExec("insert into t values(1,2),(3,4)")
	testKit.MustExec("analyze table t")

	testKit.MustExec("alter table t add column c_null int")
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	is := do.InfoSchema()
	require.Nil(t, h.Update(is))
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
	require.True(t, statsTbl.Columns[tableInfo.Columns[2].ID].IsStatsInitialized())
	require.Equal(t, int64(2), statsTbl.Columns[tableInfo.Columns[2].ID].NullCount)
	require.Equal(t, int64(0), statsTbl.Columns[tableInfo.Columns[2].ID].Histogram.NDV)

	testKit.MustExec("alter table t add column c3 int NOT NULL")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	is = do.InfoSchema()
	require.Nil(t, h.Update(is))
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
	require.True(t, statsTbl.Columns[tableInfo.Columns[3].ID].IsStatsInitialized())
	sctx := mock.NewContext()
	count, err := cardinality.ColumnEqualRowCount(sctx, statsTbl, types.NewIntDatum(0), tableInfo.Columns[3].ID)
	require.NoError(t, err)
	require.Equal(t, float64(2), count)
	count, err = cardinality.ColumnEqualRowCount(sctx, statsTbl, types.NewIntDatum(1), tableInfo.Columns[3].ID)
	require.NoError(t, err)
	require.Equal(t, float64(0), count)

	testKit.MustExec("alter table t add column c4 datetime NOT NULL default CURRENT_TIMESTAMP")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	is = do.InfoSchema()
	require.Nil(t, h.Update(is))
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	// If we don't use original default value, we will get a pseudo table.
	require.False(t, statsTbl.Pseudo)

	testKit.MustExec("alter table t add column c5 varchar(15) DEFAULT '123'")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	is = do.InfoSchema()
	require.Nil(t, h.Update(is))
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
	require.True(t, statsTbl.Columns[tableInfo.Columns[5].ID].IsStatsInitialized())
	require.Equal(t, 3.0, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[5].ID], statsTbl.RealtimeCount, false))

	testKit.MustExec("alter table t add column c6 varchar(15) DEFAULT '123', add column c7 varchar(15) DEFAULT '123'")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	is = do.InfoSchema()
	require.Nil(t, h.Update(is))
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	testKit.MustExec("create index i on t(c2, c1)")
	testKit.MustExec("analyze table t")
	rs := testKit.MustQuery("select count(*) from mysql.stats_histograms where table_id = ? and hist_id = 1 and is_index =1", tableInfo.ID)
	rs.Check(testkit.Rows("1"))
	rs = testKit.MustQuery("select count(*) from mysql.stats_buckets where table_id = ? and hist_id = 1 and is_index = 1", tableInfo.ID)
	rs.Check(testkit.Rows("0"))
	rs = testKit.MustQuery("select count(*) from mysql.stats_top_n where table_id = ? and hist_id = 1 and is_index = 1", tableInfo.ID)
	rs.Check(testkit.Rows("2"))
}

func TestDDLPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	for i, pruneMode := range []string{"static", "dynamic"} {
		testKit.MustExec("set @@tidb_partition_prune_mode=`" + pruneMode + "`")
		testKit.MustExec("set global tidb_partition_prune_mode=`" + pruneMode + "`")
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		h := do.StatsHandle()
		if i == 1 {
			err := h.HandleDDLEvent(<-h.DDLEventCh())
			require.NoError(t, err)
		}
		createTable := `CREATE TABLE t (a int, b int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
		testKit.MustExec(createTable)
		is := do.InfoSchema()
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		err = h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		require.Nil(t, h.Update(is))
		pi := tableInfo.GetPartitionInfo()
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.False(t, statsTbl.Pseudo, "for %v", pruneMode)
		}

		testKit.MustExec("insert into t values (1,2),(6,2),(11,2),(16,2)")
		testKit.MustExec("analyze table t")
		testKit.MustExec("alter table t add column c varchar(15) DEFAULT '123'")
		err = h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		is = do.InfoSchema()
		require.Nil(t, h.Update(is))
		tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo = tbl.Meta()
		pi = tableInfo.GetPartitionInfo()
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.False(t, statsTbl.Pseudo)
			require.Equal(t, 3.0, cardinality.AvgColSize(statsTbl.Columns[tableInfo.Columns[2].ID], statsTbl.RealtimeCount, false))
		}

		addPartition := "alter table t add partition (partition p4 values less than (26))"
		testKit.MustExec(addPartition)
		is = do.InfoSchema()
		tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo = tbl.Meta()
		err = h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		require.Nil(t, h.Update(is))
		pi = tableInfo.GetPartitionInfo()
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.False(t, statsTbl.Pseudo)
		}

		truncatePartition := "alter table t truncate partition p4"
		testKit.MustExec(truncatePartition)
		is = do.InfoSchema()
		tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo = tbl.Meta()
		err = h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		require.Nil(t, h.Update(is))
		pi = tableInfo.GetPartitionInfo()
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.False(t, statsTbl.Pseudo)
		}

		reorganizePartition := "alter table t reorganize partition p0,p1 into (partition p0 values less than (11))"
		testKit.MustExec(reorganizePartition)
		is = do.InfoSchema()
		tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo = tbl.Meta()
		err = h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		require.Nil(t, h.Update(is))
		pi = tableInfo.GetPartitionInfo()
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.False(t, statsTbl.Pseudo)
		}
	}
}

func TestTruncateTableUpdatesStatsVersionForGC(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int)")
	handleNextDDLEvent(t, h, model.ActionCreateTable)
	testKit.MustExec("insert into t values (1)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))

	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	oldTableInfo := tbl.Meta()
	oldVersion := statsMetaVersion(t, testKit, oldTableInfo.ID)

	testKit.MustExec("truncate table t")
	event := handleNextDDLEvent(t, h, model.ActionTruncateTable)
	require.NotNil(t, event.OldTableInfo)
	require.Equal(t, oldTableInfo.ID, event.OldTableInfo.ID)

	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	newTableInfo := tbl.Meta()
	require.NotEqual(t, oldTableInfo.ID, newTableInfo.ID)
	require.NotEmpty(t, statsMetaVersion(t, testKit, newTableInfo.ID))
	require.NotEqual(t, oldVersion, statsMetaVersion(t, testKit, oldTableInfo.ID))
}

func TestTruncatePartitionedTableUpdatesStatsVersionForGC(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	// Create and analyze the partitioned table in dynamic mode so both the global
	// table ID and partition IDs have stats_meta rows, then truncate in static mode
	// to ensure drop marking does not depend on the current prune mode.
	testKit.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	testKit.MustExec("set global tidb_partition_prune_mode='dynamic'")
	testKit.MustExec("use test")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11)
		)
	`)
	handleNextDDLEvent(t, h, model.ActionCreateTable)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2)")
	testKit.MustExec("analyze table t")

	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	oldTableInfo := tbl.Meta()
	require.NotNil(t, oldTableInfo.Partition)
	oldStatsIDs := []int64{
		oldTableInfo.ID,
		oldTableInfo.Partition.Definitions[0].ID,
		oldTableInfo.Partition.Definitions[1].ID,
	}
	oldVersions := statsMetaVersions(t, testKit, oldStatsIDs)

	testKit.MustExec("set @@tidb_partition_prune_mode='static'")
	testKit.MustExec("set global tidb_partition_prune_mode='static'")
	testKit.MustExec("truncate table t")
	event := handleNextDDLEvent(t, h, model.ActionTruncateTable)
	require.NotNil(t, event.OldTableInfo)
	require.Equal(t, oldTableInfo.ID, event.OldTableInfo.ID)
	require.Equal(t, oldTableInfo.Partition.Definitions[0].ID, event.OldTableInfo.Partition.Definitions[0].ID)
	require.Equal(t, oldTableInfo.Partition.Definitions[1].ID, event.OldTableInfo.Partition.Definitions[1].ID)

	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	newTableInfo := tbl.Meta()
	require.NotNil(t, newTableInfo.Partition)
	newPartitionIDs := []int64{
		newTableInfo.Partition.Definitions[0].ID,
		newTableInfo.Partition.Definitions[1].ID,
	}
	for _, id := range newPartitionIDs {
		require.NotEmpty(t, statsMetaVersion(t, testKit, id))
	}
	newVersions := statsMetaVersions(t, testKit, oldStatsIDs)
	for i := range oldStatsIDs {
		require.NotEqual(t, oldVersions[i], newVersions[i])
	}
}

func handleNextDDLEvent(t *testing.T, h *handle.Handle, expectedType model.ActionType) *ddlutil.Event {
	t.Helper()
	select {
	case event := <-h.DDLEventCh():
		require.Equal(t, expectedType, event.Tp)
		require.NoError(t, h.HandleDDLEvent(event))
		return event
	case <-time.After(time.Second):
		t.Fatalf("expected ddl event %s", expectedType)
	}
	return nil
}

func statsMetaVersion(t *testing.T, testKit *testkit.TestKit, tableID int64) string {
	t.Helper()
	rows := testKit.MustQuery("select version from mysql.stats_meta where table_id = ?", tableID).Rows()
	require.Len(t, rows, 1)
	return rows[0][0].(string)
}

func statsMetaVersions(t *testing.T, testKit *testkit.TestKit, tableIDs []int64) []string {
	t.Helper()
	versions := make([]string, 0, len(tableIDs))
	for _, id := range tableIDs {
		versions = append(versions, statsMetaVersion(t, testKit, id))
	}
	return versions
}
