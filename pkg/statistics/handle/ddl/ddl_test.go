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

package ddl_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
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

func TestTruncateAPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	pi := tableInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		require.False(t, statsTbl.Pseudo)
	}
	err = h.Update(is)
	require.NoError(t, err)

	// Get partition p0's stats update version.
	partitionID := pi.Definitions[0].ID
	// Get it from stats_meat first.
	rows := testKit.MustQuery(
		"select version from mysql.stats_meta where table_id = ?", partitionID,
	).Rows()
	require.Len(t, rows, 1)
	version := rows[0][0].(string)

	testKit.MustExec("alter table t truncate partition p0")
	// Find the truncate partition event.
	truncatePartitionEvent := findEvent(h.DDLEventCh(), model.ActionTruncateTablePartition)
	err = h.HandleDDLEvent(truncatePartitionEvent)
	require.NoError(t, err)
	// Check global stats meta.
	// Because we have truncated a partition, the count should be 5 - 2 = 3 and the modify count should be 2.
	testKit.MustQuery(
		"select count, modify_count from mysql.stats_meta where table_id = ?", tableInfo.ID,
	).Check(
		testkit.Rows("3 2"),
	)

	// Check the version again.
	rows = testKit.MustQuery(
		"select version from mysql.stats_meta where table_id = ?", partitionID,
	).Rows()
	require.Len(t, rows, 1)
	// Version gets updated after truncate the partition.
	require.NotEqual(t, version, rows[0][0].(string))
}

func TestTruncatePartitions(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	pi := tableInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		require.False(t, statsTbl.Pseudo)
	}
	err = h.Update(is)
	require.NoError(t, err)

	// Get partition p0 and p1's stats update version.
	partitionP0ID := pi.Definitions[0].ID
	partitionP1ID := pi.Definitions[1].ID
	// Get it from stats_meat first.
	rows := testKit.MustQuery(
		"select version from mysql.stats_meta where table_id in (?, ?) order by table_id", partitionP0ID, partitionP1ID,
	).Rows()
	require.Len(t, rows, 2)
	versionP0 := rows[0][0].(string)
	versionP1 := rows[1][0].(string)

	// Truncate two partitions.
	testKit.MustExec("alter table t truncate partition p0, p1")
	// Find the truncate partition event.
	truncatePartitionEvent := findEvent(h.DDLEventCh(), model.ActionTruncateTablePartition)
	err = h.HandleDDLEvent(truncatePartitionEvent)
	require.NoError(t, err)
	// Check global stats meta.
	// Because we have truncated two partitions, the count should be 5 - 2 - 1  = 2 and the modify count should be 3.
	testKit.MustQuery(
		"select count, modify_count from mysql.stats_meta where table_id = ?", tableInfo.ID,
	).Check(
		testkit.Rows("2 3"),
	)

	// Check the version again.
	rows = testKit.MustQuery(
		"select version from mysql.stats_meta where table_id in (?, ?) order by table_id", partitionP0ID, partitionP1ID,
	).Rows()
	require.Len(t, rows, 2)
	// Version gets updated after truncate the partition.
	require.NotEqual(t, versionP0, rows[0][0].(string))
	require.NotEqual(t, versionP1, rows[1][0].(string))
}

func TestDropAPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	pi := tableInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		require.False(t, statsTbl.Pseudo)
	}
	err = h.Update(is)
	require.NoError(t, err)

	testKit.MustExec("alter table t drop partition p0")
	// Find the drop partition event.
	dropPartitionEvent := findEvent(h.DDLEventCh(), model.ActionDropTablePartition)

	err = h.HandleDDLEvent(dropPartitionEvent)
	require.NoError(t, err)
	// Check the global stats meta.
	// Because we have dropped a partition, the count should be 3 and the modify count should be 2.
	testKit.MustQuery(
		"select count, modify_count from mysql.stats_meta where table_id = ?", tableInfo.ID,
	).Check(
		testkit.Rows("3 2"),
	)

	// Get partition p0's stats update version.
	partitionID := pi.Definitions[0].ID
	// Get it from stats_meta first.
	rows := testKit.MustQuery(
		"select version from mysql.stats_meta where table_id = ?", partitionID,
	).Rows()
	require.Len(t, rows, 1)
	version := rows[0][0].(string)

	// Check the update version is changed.
	rows = testKit.MustQuery(
		"select version from mysql.stats_meta where table_id = ?", tableInfo.ID,
	).Rows()
	require.Len(t, rows, 1)
	require.NotEqual(t, version, rows[0][0].(string))
}

func TestDropPartitions(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	pi := tableInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		require.False(t, statsTbl.Pseudo)
	}
	err = h.Update(is)
	require.NoError(t, err)

	// Get partition p0 and p1's stats update version.
	partitionP0ID := pi.Definitions[0].ID
	partitionP1ID := pi.Definitions[1].ID
	// Get it from stats_meat first.
	rows := testKit.MustQuery(
		"select version from mysql.stats_meta where table_id in (?, ?) order by table_id",
		partitionP0ID, partitionP1ID,
	).Rows()
	require.Len(t, rows, 2)
	versionP0 := rows[0][0].(string)
	versionP1 := rows[1][0].(string)

	// Drop partition p0 and p1.
	testKit.MustExec("alter table t drop partition p0,p1")
	// Find the drop partition event.
	dropPartitionEvent := findEvent(h.DDLEventCh(), model.ActionDropTablePartition)

	err = h.HandleDDLEvent(dropPartitionEvent)
	require.NoError(t, err)

	// Check the global stats meta.
	// Because we have dropped two partitions,
	// the count should be 5 - 2 - 1 = 2 and the modify count should be 2 +1 = 3.
	testKit.MustQuery(
		"select count, modify_count from mysql.stats_meta where table_id = ?", tableInfo.ID,
	).Check(
		testkit.Rows("2 3"),
	)

	// Check the update versions are changed.
	rows = testKit.MustQuery(
		"select version from mysql.stats_meta where table_id in (?, ?) order by table_id",
		partitionP0ID, partitionP1ID,
	).Rows()
	require.Len(t, rows, 2)
	require.NotEqual(t, versionP0, rows[0][0].(string))
	require.NotEqual(t, versionP1, rows[1][0].(string))
}

func TestExchangeAPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	// Create a table with 4 partitions.
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	h.DumpStatsDeltaToKV(true)

	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	pi := tableInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		require.False(t, statsTbl.Pseudo)
	}
	// Create a normal table to exchange partition.
	testKit.MustExec("drop table if exists t1")
	testKit.MustExec("create table t1 (a int, b int, primary key(a), index idx(b))")
	// Insert some data which meets the condition of the partition p0.
	testKit.MustExec("insert into t1 values (1,2),(2,2),(3,2),(4,2),(5,2)")
	err = h.DumpStatsDeltaToKV(true)
	require.NoError(t, err)

	testKit.MustExec("analyze table t1")
	is = do.InfoSchema()
	tbl1, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t1"),
	)
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()
	statsTbl1 := h.GetTableStats(tableInfo1)
	require.False(t, statsTbl1.Pseudo)

	// Check the global stats meta before exchange partition.
	testKit.MustQuery(
		fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tableInfo.ID),
	).Check(
		testkit.Rows("5 0"),
	)

	// Exchange partition p0 with table t1.
	testKit.MustExec("alter table t exchange partition p0 with table t1")
	// Find the exchange partition event.
	exchangePartitionEvent := findEvent(h.DDLEventCh(), model.ActionExchangeTablePartition)
	err = h.HandleDDLEvent(exchangePartitionEvent)
	require.NoError(t, err)
	// Check the global stats meta.
	// Because we have exchanged a partition, the count should be 5 and the modify count should be 5(table) + 2(partition).
	// 5 -> Five rows are added to table 't' as 't1' is included as a new partition.
	// 2 -> Two rows are removed from table 't' as partition 'p0' is no longer a part of it.
	testKit.MustQuery(
		fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tableInfo.ID),
	).Check(
		testkit.Rows("8 7"),
	)

	// Create another normal table with no data to exchange partition.
	testKit.MustExec("drop table if exists t2")
	testKit.MustExec("create table t2 (a int, b int, primary key(a), index idx(b))")
	testKit.MustExec("analyze table t2")
	is = do.InfoSchema()
	tbl2, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t2"),
	)
	require.NoError(t, err)
	tableInfo2 := tbl2.Meta()
	statsTbl2 := h.GetTableStats(tableInfo2)
	require.False(t, statsTbl2.Pseudo)
	err = h.Update(do.InfoSchema())
	require.NoError(t, err)

	// Insert some data to partition p1 before exchange partition.
	testKit.MustExec("insert into t values (7,2),(8,2),(9,2),(10,2)")
	err = h.DumpStatsDeltaToKV(true)
	require.NoError(t, err)
	testKit.MustQuery(
		fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tableInfo.ID),
	).Check(
		// modify_count = 7 + 4 = 11
		testkit.Rows("12 11"),
	)

	testKit.MustExec("alter table t exchange partition p1 with table t2")
	// Find the exchange partition event.
	exchangePartitionEvent = findEvent(h.DDLEventCh(), model.ActionExchangeTablePartition)
	err = h.HandleDDLEvent(exchangePartitionEvent)
	require.NoError(t, err)
	// Check the global stats meta.
	testKit.MustQuery(
		fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tableInfo.ID),
	).Check(
		// count = 12 - 5(old partition) + 0(new table) = 7
		// modify_count = 11 + 5(old partition) + 0(new table) - 4(old partition) = 12
		// 5 -> Five rows are removed from table 't' as partition 'p1' is no longer a part of it.
		// 0 -> No rows are added to table 't' as 't2' is added as a partition to it.
		// 4 -> Four rows are subtracted from table 't' due to the insertion of four rows into partition 'p1'.
		testkit.Rows("7 12"),
	)

	// Test if the global stats is accidentally dropped.
	// Create another normal table with no data to exchange partition.
	testKit.MustExec("drop table if exists t3")
	testKit.MustExec("create table t3 (a int, b int, primary key(a), index idx(b))")
	testKit.MustExec("analyze table t3")
	is = do.InfoSchema()
	tbl3, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t3"),
	)
	require.NoError(t, err)
	tableInfo3 := tbl3.Meta()
	statsTbl3 := h.GetTableStats(tableInfo3)
	require.False(t, statsTbl3.Pseudo)
	err = h.Update(do.InfoSchema())
	require.NoError(t, err)

	testKit.MustExec("alter table t exchange partition p2 with table t3")
	// Drop the global stats.
	testKit.MustExec(fmt.Sprintf("delete from mysql.stats_meta where table_id = %d", tableInfo.ID))
	// Find the exchange partition event.
	exchangePartitionEvent = findEvent(h.DDLEventCh(), model.ActionExchangeTablePartition)
	err = h.HandleDDLEvent(exchangePartitionEvent)
	require.NoError(t, err)
	// Check the global stats meta.
	testKit.MustQuery(
		fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tableInfo.ID),
	).Check(
		// Insert the global stats back.
		testkit.Rows("0 1"),
	)
}

func findEvent(eventCh <-chan *util.DDLEvent, eventType model.ActionType) *util.DDLEvent {
	// Find the target event.
	for {
		event := <-eventCh
		if event.GetType() == eventType {
			return event
		}
	}
}
