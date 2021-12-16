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

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestDDLAfterLoad(t *testing.T) {
	testKit, do, clean := createTestKitAndDom(t)
	defer clean()
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
	count := statsTbl.ColumnGreaterRowCount(sctx, types.NewDatum(recordCount+1), tableInfo.Columns[0].ID)
	require.Equal(t, 0.0, count)
	count = statsTbl.ColumnGreaterRowCount(sctx, types.NewDatum(recordCount+1), tableInfo.Columns[2].ID)
	require.Equal(t, 333, int(count))
}

func TestDDLTable(t *testing.T) {
	testKit, do, clean := createTestKitAndDom(t)
	defer clean()
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
	testKit, do, clean := createTestKitAndDom(t)
	defer clean()
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
	sctx := mock.NewContext()
	count, err := statsTbl.ColumnEqualRowCount(sctx, types.NewIntDatum(0), tableInfo.Columns[3].ID)
	require.NoError(t, err)
	require.Equal(t, float64(2), count)
	count, err = statsTbl.ColumnEqualRowCount(sctx, types.NewIntDatum(1), tableInfo.Columns[3].ID)
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
	require.Equal(t, 3.0, statsTbl.Columns[tableInfo.Columns[5].ID].AvgColSize(statsTbl.Count, false))

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
	testKit, do, clean := createTestKitAndDom(t)
	defer clean()
	testkit.WithPruneMode(testKit, variable.Static, func() {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
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
		h := do.StatsHandle()
		err = h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		require.Nil(t, h.Update(is))
		pi := tableInfo.GetPartitionInfo()
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.False(t, statsTbl.Pseudo)
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
			require.Equal(t, 3.0, statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSize(statsTbl.Count, false))
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
	})
}
