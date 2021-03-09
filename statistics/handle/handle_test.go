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
// See the License for the specific language governing permissions and
// limitations under the License.

package handle_test

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func cleanEnv(c *C, store kv.Storage, do *domain.Domain) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	tk.MustExec("delete from mysql.stats_extended")
	tk.MustExec("delete from mysql.stats_fm_sketch")
	tk.MustExec("delete from mysql.schema_index_usage")
	do.StatsHandle().Clear()
}

func (s *testStatsSuite) TestStatsCache(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	testKit.MustExec("create index idx_t on t(c1)")
	do.InfoSchema()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	// If index is build, but stats is not updated. statsTbl can also work.
	c.Assert(statsTbl.Pseudo, IsFalse)
	// But the added index will not work.
	c.Assert(statsTbl.Indices[int64(1)], IsNil)

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	// If the new schema drop a column, the table stats can still work.
	testKit.MustExec("alter table t drop column c2")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
}

func (s *testStatsSuite) TestStatsCacheMemTracker(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int,c3 int)")
	testKit.MustExec("insert into t values(1, 2, 3)")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.MemoryUsage() > 0, IsTrue)
	c.Assert(statsTbl.Pseudo, IsTrue)

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)

	c.Assert(statsTbl.Pseudo, IsFalse)
	testKit.MustExec("create index idx_t on t(c1)")
	do.InfoSchema()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)

	// If index is build, but stats is not updated. statsTbl can also work.
	c.Assert(statsTbl.Pseudo, IsFalse)
	// But the added index will not work.
	c.Assert(statsTbl.Indices[int64(1)], IsNil)

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)

	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schema drop a column, the table stats can still work.
	testKit.MustExec("alter table t drop column c2")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)

	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.MemoryUsage() > 0, IsTrue)
	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
}

func assertTableEqual(c *C, a *statistics.Table, b *statistics.Table) {
	c.Assert(a.Count, Equals, b.Count)
	c.Assert(a.ModifyCount, Equals, b.ModifyCount)
	c.Assert(len(a.Columns), Equals, len(b.Columns))
	for i := range a.Columns {
		c.Assert(a.Columns[i].Count, Equals, b.Columns[i].Count)
		c.Assert(statistics.HistogramEqual(&a.Columns[i].Histogram, &b.Columns[i].Histogram, false), IsTrue)
		if a.Columns[i].CMSketch == nil {
			c.Assert(b.Columns[i].CMSketch, IsNil)
		} else {
			c.Assert(a.Columns[i].CMSketch.Equal(b.Columns[i].CMSketch), IsTrue)
		}
		// The nil case has been considered in (*TopN).Equal() so we don't need to consider it here.
		c.Assert(a.Columns[i].TopN.Equal(b.Columns[i].TopN), IsTrue)
	}
	c.Assert(len(a.Indices), Equals, len(b.Indices))
	for i := range a.Indices {
		c.Assert(statistics.HistogramEqual(&a.Indices[i].Histogram, &b.Indices[i].Histogram, false), IsTrue)
		if a.Indices[i].CMSketch == nil {
			c.Assert(b.Indices[i].CMSketch, IsNil)
		} else {
			c.Assert(a.Indices[i].CMSketch.Equal(b.Indices[i].CMSketch), IsTrue)
		}
		c.Assert(a.Indices[i].TopN.Equal(b.Indices[i].TopN), IsTrue)
	}
	c.Assert(isSameExtendedStats(a.ExtendedStats, b.ExtendedStats), IsTrue)
}

func isSameExtendedStats(a, b *statistics.ExtendedStatsColl) bool {
	aEmpty := (a == nil) || len(a.Stats) == 0
	bEmpty := (b == nil) || len(b.Stats) == 0
	if (aEmpty && !bEmpty) || (!aEmpty && bEmpty) {
		return false
	}
	if aEmpty && bEmpty {
		return true
	}
	if len(a.Stats) != len(b.Stats) {
		return false
	}
	for aKey, aItem := range a.Stats {
		bItem, ok := b.Stats[aKey]
		if !ok {
			return false
		}
		for i, id := range aItem.ColIDs {
			if id != bItem.ColIDs[i] {
				return false
			}
		}
		if (aItem.Tp != bItem.Tp) || (aItem.ScalarVals != bItem.ScalarVals) || (aItem.StringVals != bItem.StringVals) {
			return false
		}
	}
	return true
}

func (s *testStatsSuite) TestStatsStoreAndLoad(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		testKit.MustExec("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustExec("create index idx_t on t(c2)")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()

	testKit.MustExec("analyze table t")
	statsTbl1 := do.StatsHandle().GetTableStats(tableInfo)

	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl2 := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	c.Assert(statsTbl2.Count, Equals, int64(recordCount))
	assertTableEqual(c, statsTbl1, statsTbl2)
}

func (s *testStatsSuite) TestEmptyTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int, key cc1(c1), key cc2(c2))")
	testKit.MustExec("analyze table t")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	sc := new(stmtctx.StatementContext)
	count := statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(1), tableInfo.Columns[0].ID)
	c.Assert(count, Equals, 0.0)
}

func (s *testStatsSuite) TestColumnIDs(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	testKit.MustExec("analyze table t")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	sc := new(stmtctx.StatementContext)
	count := statsTbl.ColumnLessRowCount(sc, types.NewDatum(2), tableInfo.Columns[0].ID)
	c.Assert(count, Equals, float64(1))

	// Drop a column and the offset changed,
	testKit.MustExec("alter table t drop column c1")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	// At that time, we should get c2's stats instead of c1's.
	count = statsTbl.ColumnLessRowCount(sc, types.NewDatum(2), tableInfo.Columns[0].ID)
	c.Assert(count, Equals, 0.0)
}

func (s *testStatsSuite) TestAvgColLen(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 varchar(100), c3 float, c4 datetime, c5 varchar(100))")
	testKit.MustExec("insert into t values(1, '1234567', 12.3, '2018-03-07 19:00:57', NULL)")
	testKit.MustExec("analyze table t")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Columns[tableInfo.Columns[0].ID].AvgColSize(statsTbl.Count, false), Equals, 1.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[0].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[0].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, 8.0)

	// The size of varchar type is LEN + BYTE, here is 1 + 7 = 8
	c.Assert(statsTbl.Columns[tableInfo.Columns[1].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[3].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[1].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, 8.0-3)
	c.Assert(statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, float64(unsafe.Sizeof(float32(12.3))))
	c.Assert(statsTbl.Columns[tableInfo.Columns[3].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, float64(unsafe.Sizeof(types.ZeroTime)))
	c.Assert(statsTbl.Columns[tableInfo.Columns[1].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, 8.0-3+8)
	c.Assert(statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, float64(unsafe.Sizeof(float32(12.3))))
	c.Assert(statsTbl.Columns[tableInfo.Columns[3].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, float64(unsafe.Sizeof(types.ZeroTime)))
	c.Assert(statsTbl.Columns[tableInfo.Columns[4].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[4].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, 0.0)
	testKit.MustExec("insert into t values(132, '123456789112', 1232.3, '2018-03-07 19:17:29', NULL)")
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Columns[tableInfo.Columns[0].ID].AvgColSize(statsTbl.Count, false), Equals, 1.5)
	c.Assert(statsTbl.Columns[tableInfo.Columns[1].ID].AvgColSize(statsTbl.Count, false), Equals, 10.5)
	c.Assert(statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[3].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[0].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[1].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, math.Round((10.5-math.Log2(10.5))*100)/100)
	c.Assert(statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, float64(unsafe.Sizeof(float32(12.3))))
	c.Assert(statsTbl.Columns[tableInfo.Columns[3].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, float64(unsafe.Sizeof(types.ZeroTime)))
	c.Assert(statsTbl.Columns[tableInfo.Columns[0].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[1].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, math.Round((10.5-math.Log2(10.5))*100)/100+8)
	c.Assert(statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, float64(unsafe.Sizeof(float32(12.3))))
	c.Assert(statsTbl.Columns[tableInfo.Columns[3].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, float64(unsafe.Sizeof(types.ZeroTime)))
	c.Assert(statsTbl.Columns[tableInfo.Columns[4].ID].AvgColSizeChunkFormat(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[4].ID].AvgColSizeListInDisk(statsTbl.Count), Equals, 0.0)
}

func (s *testStatsSuite) TestDurationToTS(c *C) {
	tests := []time.Duration{time.Millisecond, time.Second, time.Minute, time.Hour}
	for _, t := range tests {
		ts := handle.DurationToTS(t)
		c.Assert(oracle.ExtractPhysical(ts)*int64(time.Millisecond), Equals, int64(t))
	}
}

func (s *testStatsSuite) TestVersion(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("analyze table t1")
	do := s.do
	is := do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()
	h, err := handle.NewHandle(testKit.Se, time.Millisecond, do.SysSessionPool())
	c.Assert(err, IsNil)
	unit := oracle.ComposeTS(1, 0)
	testKit.MustExec("update mysql.stats_meta set version = ? where table_id = ?", 2*unit, tableInfo1.ID)

	c.Assert(h.Update(is), IsNil)
	c.Assert(h.LastUpdateVersion(), Equals, 2*unit)
	statsTbl1 := h.GetTableStats(tableInfo1)
	c.Assert(statsTbl1.Pseudo, IsFalse)

	testKit.MustExec("create table t2 (c1 int, c2 int)")
	testKit.MustExec("analyze table t2")
	is = do.InfoSchema()
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tableInfo2 := tbl2.Meta()
	// A smaller version write, and we can still read it.
	testKit.MustExec("update mysql.stats_meta set version = ? where table_id = ?", unit, tableInfo2.ID)
	c.Assert(h.Update(is), IsNil)
	c.Assert(h.LastUpdateVersion(), Equals, 2*unit)
	statsTbl2 := h.GetTableStats(tableInfo2)
	c.Assert(statsTbl2.Pseudo, IsFalse)

	testKit.MustExec("insert t1 values(1,2)")
	testKit.MustExec("analyze table t1")
	offset := 3 * unit
	testKit.MustExec("update mysql.stats_meta set version = ? where table_id = ?", offset+4, tableInfo1.ID)
	c.Assert(h.Update(is), IsNil)
	c.Assert(h.LastUpdateVersion(), Equals, offset+uint64(4))
	statsTbl1 = h.GetTableStats(tableInfo1)
	c.Assert(statsTbl1.Count, Equals, int64(1))

	testKit.MustExec("insert t2 values(1,2)")
	testKit.MustExec("analyze table t2")
	// A smaller version write, and we can still read it.
	testKit.MustExec("update mysql.stats_meta set version = ? where table_id = ?", offset+3, tableInfo2.ID)
	c.Assert(h.Update(is), IsNil)
	c.Assert(h.LastUpdateVersion(), Equals, offset+uint64(4))
	statsTbl2 = h.GetTableStats(tableInfo2)
	c.Assert(statsTbl2.Count, Equals, int64(1))

	testKit.MustExec("insert t2 values(1,2)")
	testKit.MustExec("analyze table t2")
	// A smaller version write, and we cannot read it. Because at this time, lastThree Version is 4.
	testKit.MustExec("update mysql.stats_meta set version = 1 where table_id = ?", tableInfo2.ID)
	c.Assert(h.Update(is), IsNil)
	c.Assert(h.LastUpdateVersion(), Equals, offset+uint64(4))
	statsTbl2 = h.GetTableStats(tableInfo2)
	c.Assert(statsTbl2.Count, Equals, int64(1))

	// We add an index and analyze it, but DDL doesn't load.
	testKit.MustExec("alter table t2 add column c3 int")
	testKit.MustExec("analyze table t2")
	// load it with old schema.
	c.Assert(h.Update(is), IsNil)
	statsTbl2 = h.GetTableStats(tableInfo2)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	c.Assert(statsTbl2.Columns[int64(3)], IsNil)
	// Next time DDL updated.
	is = do.InfoSchema()
	c.Assert(h.Update(is), IsNil)
	statsTbl2 = h.GetTableStats(tableInfo2)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	// We can read it without analyze again! Thanks for PrevLastVersion.
	c.Assert(statsTbl2.Columns[int64(3)], NotNil)
}

func (s *testStatsSuite) TestLoadHist(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 varchar(12), c2 char(12))")
	do := s.do
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	rowCount := 10
	for i := 0; i < rowCount; i++ {
		testKit.MustExec("insert into t values('a','ddd')")
	}
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	oldStatsTbl := h.GetTableStats(tableInfo)
	for i := 0; i < rowCount; i++ {
		testKit.MustExec("insert into t values('bb','sdfga')")
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	h.Update(do.InfoSchema())
	newStatsTbl := h.GetTableStats(tableInfo)
	// The stats table is updated.
	c.Assert(oldStatsTbl == newStatsTbl, IsFalse)
	// Only the TotColSize of histograms is updated.
	for id, hist := range oldStatsTbl.Columns {
		c.Assert(hist.TotColSize, Less, newStatsTbl.Columns[id].TotColSize)

		temp := hist.TotColSize
		hist.TotColSize = newStatsTbl.Columns[id].TotColSize
		c.Assert(statistics.HistogramEqual(&hist.Histogram, &newStatsTbl.Columns[id].Histogram, false), IsTrue)
		hist.TotColSize = temp

		c.Assert(hist.CMSketch.Equal(newStatsTbl.Columns[id].CMSketch), IsTrue)
		c.Assert(hist.Count, Equals, newStatsTbl.Columns[id].Count)
		c.Assert(hist.Info, Equals, newStatsTbl.Columns[id].Info)
	}
	// Add column c3, we only update c3.
	testKit.MustExec("alter table t add column c3 int")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	c.Assert(h.Update(is), IsNil)
	newStatsTbl2 := h.GetTableStats(tableInfo)
	c.Assert(newStatsTbl2 == newStatsTbl, IsFalse)
	// The histograms is not updated.
	for id, hist := range newStatsTbl.Columns {
		c.Assert(hist, Equals, newStatsTbl2.Columns[id])
	}
	c.Assert(newStatsTbl2.Columns[int64(3)].LastUpdateVersion, Greater, newStatsTbl2.Columns[int64(1)].LastUpdateVersion)
}

func (s *testStatsSuite) TestInitStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	testKit.MustExec("analyze table t")
	h := s.do.StatsHandle()
	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)

	h.Clear()
	c.Assert(h.InitStats(is), IsNil)
	table0 := h.GetTableStats(tbl.Meta())
	cols := table0.Columns
	c.Assert(cols[1].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x36))
	c.Assert(cols[2].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x37))
	c.Assert(cols[3].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x38))
	h.Clear()
	c.Assert(h.Update(is), IsNil)
	table1 := h.GetTableStats(tbl.Meta())
	assertTableEqual(c, table0, table1)
	h.SetLease(0)
}

func (s *testStatsSuite) TestInitStatsVer2(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("create table t(a int, b int, c int, index idx(a), index idxab(a, b))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (4, 4, 4), (4, 4, 4)")
	tk.MustExec("analyze table t with 2 topn, 3 buckets")
	h := s.do.StatsHandle()
	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)

	h.Clear()
	c.Assert(h.InitStats(is), IsNil)
	table0 := h.GetTableStats(tbl.Meta())
	cols := table0.Columns
	c.Assert(cols[1].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x33))
	c.Assert(cols[2].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x33))
	c.Assert(cols[3].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x33))
	h.Clear()
	c.Assert(h.Update(is), IsNil)
	table1 := h.GetTableStats(tbl.Meta())
	assertTableEqual(c, table0, table1)
	h.SetLease(0)
}

func (s *testStatsSuite) TestLoadStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := s.do.StatsHandle().Lease()
	s.do.StatsHandle().SetLease(1)
	defer func() {
		s.do.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := s.do.StatsHandle()
	stat := h.GetTableStats(tableInfo)
	hg := stat.Columns[tableInfo.Columns[0].ID].Histogram
	c.Assert(hg.Len(), Greater, 0)
	cms := stat.Columns[tableInfo.Columns[0].ID].CMSketch
	c.Assert(cms, IsNil)
	hg = stat.Indices[tableInfo.Indices[0].ID].Histogram
	c.Assert(hg.Len(), Greater, 0)
	cms = stat.Indices[tableInfo.Indices[0].ID].CMSketch
	topN := stat.Indices[tableInfo.Indices[0].ID].TopN
	c.Assert(cms.TotalCount()+topN.TotalCount(), Greater, uint64(0))
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	c.Assert(hg.Len(), Equals, 0)
	cms = stat.Columns[tableInfo.Columns[2].ID].CMSketch
	c.Assert(cms, IsNil)
	_, err = stat.ColumnEqualRowCount(testKit.Se.GetSessionVars().StmtCtx, types.NewIntDatum(1), tableInfo.Columns[2].ID)
	c.Assert(err, IsNil)
	c.Assert(h.LoadNeededHistograms(), IsNil)
	stat = h.GetTableStats(tableInfo)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	c.Assert(hg.Len(), Greater, 0)
	// Following test tests whether the LoadNeededHistograms would panic.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/statistics/handle/mockGetStatsReaderFail", `return(true)`), IsNil)
	err = h.LoadNeededHistograms()
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/statistics/handle/mockGetStatsReaderFail"), IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/statistics/handle/mockGetStatsReaderPanic", "panic"), IsNil)
	err = h.LoadNeededHistograms()
	c.Assert(err, ErrorMatches, ".*getStatsReader panic.*")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/statistics/handle/mockGetStatsReaderPanic"), IsNil)
	err = h.LoadNeededHistograms()
	c.Assert(err, IsNil)
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain.RunAutoAnalyze = false
	do, err := session.BootstrapSession(store)
	do.SetStatsUpdating(true)
	return store, do, errors.Trace(err)
}

func (s *testStatsSuite) TestCorrelation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(c1 int primary key, c2 int)")
	testKit.MustExec("insert into t values(1,1),(3,12),(4,20),(2,7),(5,21)")
	testKit.MustExec("analyze table t")
	result := testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")
	testKit.MustExec("insert into t values(8,18)")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "0.8285714285714286")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "0.8285714285714286")

	testKit.MustExec("truncate table t")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 0)
	testKit.MustExec("insert into t values(1,21),(3,12),(4,7),(2,20),(5,1)")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "-1")
	testKit.MustExec("insert into t values(8,4)")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "-0.9428571428571428")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "-0.9428571428571428")

	testKit.MustExec("truncate table t")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("insert into t values (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1),(11,1),(12,1),(13,1),(14,1),(15,1),(16,1),(17,1),(18,1),(19,1),(20,2),(21,2),(22,2),(23,2),(24,2),(25,2)")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(c1 int, c2 int)")
	testKit.MustExec("insert into t values(1,1),(2,7),(3,12),(4,20),(5,21),(8,18)")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "0.8285714285714286")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "0.8285714285714286")

	testKit.MustExec("truncate table t")
	testKit.MustExec("insert into t values(1,1),(2,7),(3,12),(8,18),(4,20),(5,21)")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0.8285714285714286")
	c.Assert(result.Rows()[1][9], Equals, "1")

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(c1 int primary key, c2 int, c3 int, key idx_c2(c2))")
	testKit.MustExec("insert into t values(1,1,1),(2,2,2),(3,3,3)")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 0").Sort()
	c.Assert(len(result.Rows()), Equals, 3)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")
	c.Assert(result.Rows()[2][9], Equals, "1")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 1").Sort()
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][9], Equals, "0")
}

func (s *testStatsSuite) TestShowGlobalStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustExec("create table t (a int, key(a)) partition by hash(a) partitions 2")
	tk.MustExec("insert into t values (1), (2), (3), (4)")
	tk.MustExec("analyze table t with 1 buckets")
	c.Assert(len(tk.MustQuery("show stats_meta").Rows()), Equals, 2)
	c.Assert(len(tk.MustQuery("show stats_meta where partition_name='global'").Rows()), Equals, 0)
	c.Assert(len(tk.MustQuery("show stats_buckets").Rows()), Equals, 4) // 2 partitions * (1 for the column_a and 1 for the index_a)
	c.Assert(len(tk.MustQuery("show stats_buckets where partition_name='global'").Rows()), Equals, 0)
	c.Assert(len(tk.MustQuery("show stats_histograms").Rows()), Equals, 4)
	c.Assert(len(tk.MustQuery("show stats_histograms where partition_name='global'").Rows()), Equals, 0)
	c.Assert(len(tk.MustQuery("show stats_healthy").Rows()), Equals, 2)
	c.Assert(len(tk.MustQuery("show stats_healthy where partition_name='global'").Rows()), Equals, 0)

	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t with 0 topn, 1 buckets")
	c.Assert(len(tk.MustQuery("show stats_meta").Rows()), Equals, 3)
	c.Assert(len(tk.MustQuery("show stats_meta where partition_name='global'").Rows()), Equals, 1)
	c.Assert(len(tk.MustQuery("show stats_buckets").Rows()), Equals, 6)
	c.Assert(len(tk.MustQuery("show stats_buckets where partition_name='global'").Rows()), Equals, 2)
	c.Assert(len(tk.MustQuery("show stats_histograms").Rows()), Equals, 6)
	c.Assert(len(tk.MustQuery("show stats_histograms where partition_name='global'").Rows()), Equals, 2)
	c.Assert(len(tk.MustQuery("show stats_healthy").Rows()), Equals, 3)
	c.Assert(len(tk.MustQuery("show stats_healthy where partition_name='global'").Rows()), Equals, 1)
}

func (s *testStatsSuite) TestBuildGlobalLevelStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1;")
	testKit.MustExec("set @@tidb_analyze_version = 2")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static';")
	testKit.MustExec("create table t(a int, b int, c int) PARTITION BY HASH(a) PARTITIONS 3;")
	testKit.MustExec("create table t1(a int);")
	testKit.MustExec("insert into t values(1,1,1),(3,12,3),(4,20,4),(2,7,2),(5,21,5);")
	testKit.MustExec("insert into t1 values(1),(3),(4),(2),(5);")
	testKit.MustExec("create index idx_t_ab on t(a, b);")
	testKit.MustExec("create index idx_t_b on t(b);")
	testKit.MustExec("analyze table t, t1;")
	result := testKit.MustQuery("show stats_meta where table_name = 't';").Sort()
	c.Assert(len(result.Rows()), Equals, 3)
	c.Assert(result.Rows()[0][5], Equals, "1")
	c.Assert(result.Rows()[1][5], Equals, "2")
	c.Assert(result.Rows()[2][5], Equals, "2")
	result = testKit.MustQuery("show stats_histograms where table_name = 't';").Sort()
	c.Assert(len(result.Rows()), Equals, 15)

	result = testKit.MustQuery("show stats_meta where table_name = 't1';").Sort()
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][5], Equals, "5")
	result = testKit.MustQuery("show stats_histograms where table_name = 't1';").Sort()
	c.Assert(len(result.Rows()), Equals, 1)

	// Test the 'dynamic' mode
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	testKit.MustExec("analyze table t, t1;")
	result = testKit.MustQuery("show stats_meta where table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 4)
	c.Assert(result.Rows()[0][5], Equals, "5")
	c.Assert(result.Rows()[1][5], Equals, "1")
	c.Assert(result.Rows()[2][5], Equals, "2")
	c.Assert(result.Rows()[3][5], Equals, "2")
	result = testKit.MustQuery("show stats_histograms where table_name = 't';").Sort()
	c.Assert(len(result.Rows()), Equals, 20)

	result = testKit.MustQuery("show stats_meta where table_name = 't1';").Sort()
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][5], Equals, "5")
	result = testKit.MustQuery("show stats_histograms where table_name = 't1';").Sort()
	c.Assert(len(result.Rows()), Equals, 1)

	testKit.MustExec("analyze table t index idx_t_ab, idx_t_b;")
	result = testKit.MustQuery("show stats_meta where table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 4)
	c.Assert(result.Rows()[0][5], Equals, "5")
	c.Assert(result.Rows()[1][5], Equals, "1")
	c.Assert(result.Rows()[2][5], Equals, "2")
	c.Assert(result.Rows()[3][5], Equals, "2")
	result = testKit.MustQuery("show stats_histograms where table_name = 't';").Sort()
	c.Assert(len(result.Rows()), Equals, 20)
}

func (s *testStatsSuite) prepareForGlobalStatsWithOpts(c *C, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(` create table t (a int, key(a)) partition by range (a) ` +
		`(partition p0 values less than (100000), partition p1 values less than (200000))`)
	buf1 := bytes.NewBufferString("insert into t values (0)")
	buf2 := bytes.NewBufferString("insert into t values (100000)")
	for i := 0; i < 5000; i += 3 {
		buf1.WriteString(fmt.Sprintf(", (%v)", i))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100000+i))
	}
	tk.MustExec(buf1.String())
	tk.MustExec(buf2.String())
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
}

func (s *testStatsSuite) checkForGlobalStatsWithOpts(c *C, tk *testkit.TestKit, p string, topn, buckets int) {
	delta := buckets/2 + 1
	for _, isIdx := range []int{0, 1} {
		c.Assert(len(tk.MustQuery(fmt.Sprintf("show stats_topn where partition_name='%v' and is_index=%v", p, isIdx)).Rows()), Equals, topn)
		numBuckets := len(tk.MustQuery(fmt.Sprintf("show stats_buckets where partition_name='%v' and is_index=%v", p, isIdx)).Rows())
		// since the hist-building algorithm doesn't stipulate the final bucket number to be equal to the expected number exactly,
		// we have to check the results by a range here.
		c.Assert(numBuckets >= buckets-delta, IsTrue)
		c.Assert(numBuckets <= buckets+delta, IsTrue)
	}
}

func (s *testStatsSuite) TestAnalyzeGlobalStatsWithOpts(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	s.prepareForGlobalStatsWithOpts(c, tk)

	type opt struct {
		topn    int
		buckets int
		err     bool
	}

	cases := []opt{
		{1, 37, false},
		{2, 47, false},
		{10, 77, false},
		{77, 219, false},
		{-31, 222, true},
		{10, -77, true},
		{10000, 47, true},
		{77, 47000, true},
	}
	for _, ca := range cases {
		sql := fmt.Sprintf("analyze table t with %v topn, %v buckets", ca.topn, ca.buckets)
		if !ca.err {
			tk.MustExec(sql)
			s.checkForGlobalStatsWithOpts(c, tk, "global", ca.topn, ca.buckets)
			s.checkForGlobalStatsWithOpts(c, tk, "p0", ca.topn, ca.buckets)
			s.checkForGlobalStatsWithOpts(c, tk, "p1", ca.topn, ca.buckets)
		} else {
			err := tk.ExecToErr(sql)
			c.Assert(err, NotNil)
		}
	}
}

func (s *testStatsSuite) TestAnalyzeGlobalStatsWithOpts2(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	s.prepareForGlobalStatsWithOpts(c, tk)

	tk.MustExec("analyze table t with 20 topn, 50 buckets")
	s.checkForGlobalStatsWithOpts(c, tk, "global", 20, 50)
	s.checkForGlobalStatsWithOpts(c, tk, "p0", 20, 50)
	s.checkForGlobalStatsWithOpts(c, tk, "p1", 20, 50)

	// analyze a partition to let its options be different with others'
	tk.MustExec("analyze table t partition p0 with 10 topn, 20 buckets")
	s.checkForGlobalStatsWithOpts(c, tk, "global", 10, 20) // use new options
	s.checkForGlobalStatsWithOpts(c, tk, "p0", 10, 20)
	s.checkForGlobalStatsWithOpts(c, tk, "p1", 20, 50)

	tk.MustExec("analyze table t partition p1 with 100 topn, 200 buckets")
	s.checkForGlobalStatsWithOpts(c, tk, "global", 100, 200)
	s.checkForGlobalStatsWithOpts(c, tk, "p0", 10, 20)
	s.checkForGlobalStatsWithOpts(c, tk, "p1", 100, 200)

	tk.MustExec("analyze table t partition p0") // default options
	s.checkForGlobalStatsWithOpts(c, tk, "global", 20, 256)
	s.checkForGlobalStatsWithOpts(c, tk, "p0", 20, 256)
	s.checkForGlobalStatsWithOpts(c, tk, "p1", 100, 200)
}

func (s *testStatsSuite) TestGlobalStatsData(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`
create table t (
	a int,
	key(a)
)
partition by range (a) (
	partition p0 values less than (10),
	partition p1 values less than (20)
)`)
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (6), (null), (11), (12), (13), (14), (15), (16), (17), (18), (19), (19)")
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table t with 0 topn, 2 buckets")

	tk.MustQuery("select modify_count, count from mysql.stats_meta order by table_id asc").Check(
		testkit.Rows("0 18", "0 8", "0 10")) // global row-count = sum(partition row-count)

	// distinct, null_count, tot_col_size should be the sum of their values in partition-stats, and correlation should be 0
	tk.MustQuery("select distinct_count, null_count, tot_col_size, correlation=0 from mysql.stats_histograms where is_index=0 order by table_id asc").Check(
		testkit.Rows("15 1 17 1", "6 1 7 0", "9 0 10 0"))
	tk.MustQuery("select distinct_count, null_count, tot_col_size, correlation=0 from mysql.stats_histograms where is_index=1 order by table_id asc").Check(
		testkit.Rows("15 1 0 1", "6 1 0 1", "9 0 0 1"))

	tk.MustQuery("show stats_buckets where is_index=0").Check(
		// db table partition col is_idx bucket_id count repeats lower upper ndv
		testkit.Rows("test t global a 0 0 7 2 1 6 0",
			"test t global a 0 1 17 2 6 19 0",
			"test t p0 a 0 0 4 1 1 4 0",
			"test t p0 a 0 1 7 2 5 6 0",
			"test t p1 a 0 0 6 1 11 16 0",
			"test t p1 a 0 1 10 2 17 19 0"))
	tk.MustQuery("show stats_buckets where is_index=1").Check(
		testkit.Rows("test t global a 1 0 7 2 1 6 6",
			"test t global a 1 1 17 2 6 19 9",
			"test t p0 a 1 0 4 1 1 4 4",
			"test t p0 a 1 1 7 2 5 6 2",
			"test t p1 a 1 0 8 1 11 18 8",
			"test t p1 a 1 1 10 2 19 19 1"))
}

func (s *testStatsSuite) TestGlobalStatsData2(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@tidb_analyze_version=2")

	// int + (column & index with 1 column)
	tk.MustExec("drop table if exists tint")
	tk.MustExec("create table tint (c int, key(c)) partition by range (c) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("insert into tint values (1), (2), (3), (4), (4), (5), (5), (5), (null), (11), (12), (13), (14), (15), (16), (16), (16), (16), (17), (17)")
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table tint with 2 topn, 2 buckets")

	tk.MustQuery("select modify_count, count from mysql.stats_meta order by table_id asc").Check(testkit.Rows(
		"0 20",  // global: g.count = p0.count + p1.count
		"0 9",   // p0
		"0 11")) // p1

	tk.MustQuery("show stats_topn where table_name='tint' and is_index=0").Check(testkit.Rows(
		"test tint global c 0 5 3",
		"test tint global c 0 16 4",
		"test tint p0 c 0 4 2",
		"test tint p0 c 0 5 3",
		"test tint p1 c 0 16 4",
		"test tint p1 c 0 17 2"))

	tk.MustQuery("show stats_topn where table_name='tint' and is_index=1").Check(testkit.Rows(
		"test tint global c 1 5 3",
		"test tint global c 1 16 4",
		"test tint p0 c 1 4 2",
		"test tint p0 c 1 5 3",
		"test tint p1 c 1 16 4",
		"test tint p1 c 1 17 2"))

	tk.MustQuery("show stats_buckets where is_index=0").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tint global c 0 0 5 2 1 4 0", // bucket.ndv is not maintained for column histograms
		"test tint global c 0 1 12 2 4 17 0",
		"test tint p0 c 0 0 2 1 1 2 0",
		"test tint p0 c 0 1 3 1 3 3 0",
		"test tint p1 c 0 0 3 1 11 13 0",
		"test tint p1 c 0 1 5 1 14 15 0"))

	tk.MustQuery("select distinct_count, null_count, tot_col_size from mysql.stats_histograms where is_index=0 order by table_id asc").Check(
		testkit.Rows("12 1 19", // global, g = p0 + p1
			"5 1 8",   // p0
			"7 0 11")) // p1

	tk.MustQuery("show stats_buckets where is_index=1").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tint global c 1 0 5 0 1 5 4", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tint global c 1 1 12 2 5 17 6",
		"test tint p0 c 1 0 3 0 1 4 3",
		"test tint p0 c 1 1 3 0 5 5 0",
		"test tint p1 c 1 0 5 0 11 16 5",
		"test tint p1 c 1 1 5 0 17 17 0"))

	tk.MustQuery("select distinct_count, null_count from mysql.stats_histograms where is_index=1 order by table_id asc").Check(
		testkit.Rows("12 1", // global, g = p0 + p1
			"5 1",  // p0
			"7 0")) // p1

	// double + (column + index with 1 column)
	tk.MustExec("drop table if exists tdouble")
	tk.MustExec(`create table tdouble (a int, c double, key(c)) partition by range (a)` +
		`(partition p0 values less than(10),partition p1 values less than(20))`)
	tk.MustExec(`insert into tdouble values ` +
		`(1, 1), (2, 2), (3, 3), (4, 4), (4, 4), (5, 5), (5, 5), (5, 5), (null, null), ` + // values in p0
		`(11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (16, 16), (16, 16), (16, 16), (17, 17), (17, 17)`) // values in p1
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table tdouble with 2 topn, 2 buckets")

	rs := tk.MustQuery("show stats_meta where table_name='tdouble'").Rows()
	c.Assert(rs[0][5].(string), Equals, "20") // g.count = p0.count + p1.count
	c.Assert(rs[1][5].(string), Equals, "9")  // p0.count
	c.Assert(rs[2][5].(string), Equals, "11") // p1.count

	tk.MustQuery("show stats_topn where table_name='tdouble' and is_index=0 and column_name='c'").Check(testkit.Rows(
		`test tdouble global c 0 5 3`,
		`test tdouble global c 0 16 4`,
		`test tdouble p0 c 0 4 2`,
		`test tdouble p0 c 0 5 3`,
		`test tdouble p1 c 0 16 4`,
		`test tdouble p1 c 0 17 2`))

	tk.MustQuery("show stats_topn where table_name='tdouble' and is_index=1 and column_name='c'").Check(testkit.Rows(
		`test tdouble global c 1 5 3`,
		`test tdouble global c 1 16 4`,
		`test tdouble p0 c 1 4 2`,
		`test tdouble p0 c 1 5 3`,
		`test tdouble p1 c 1 16 4`,
		`test tdouble p1 c 1 17 2`))

	tk.MustQuery("show stats_buckets where table_name='tdouble' and is_index=0 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdouble global c 0 0 5 2 1 4 0", // bucket.ndv is not maintained for column histograms
		"test tdouble global c 0 1 12 2 4 17 0",
		"test tdouble p0 c 0 0 2 1 1 2 0",
		"test tdouble p0 c 0 1 3 1 3 3 0",
		"test tdouble p1 c 0 0 3 1 11 13 0",
		"test tdouble p1 c 0 1 5 1 14 15 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdouble' and column_name='c' and is_index=0").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")

	tk.MustQuery("show stats_buckets where table_name='tdouble' and is_index=1 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdouble global c 1 0 5 0 1 5 4", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdouble global c 1 1 12 2 5 17 6",
		"test tdouble p0 c 1 0 3 0 1 4 3",
		"test tdouble p0 c 1 1 3 0 5 5 0",
		"test tdouble p1 c 1 0 5 0 11 16 5",
		"test tdouble p1 c 1 1 5 0 17 17 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdouble' and column_name='c' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")

	// decimal + (column + index with 1 column)
	tk.MustExec("drop table if exists tdecimal")
	tk.MustExec(`create table tdecimal (a int, c decimal(10, 2), key(c)) partition by range (a)` +
		`(partition p0 values less than(10),partition p1 values less than(20))`)
	tk.MustExec(`insert into tdecimal values ` +
		`(1, 1), (2, 2), (3, 3), (4, 4), (4, 4), (5, 5), (5, 5), (5, 5), (null, null), ` + // values in p0
		`(11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (16, 16), (16, 16), (16, 16), (17, 17), (17, 17)`) // values in p1
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table tdecimal with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tdecimal'").Rows()
	c.Assert(rs[0][5].(string), Equals, "20") // g.count = p0.count + p1.count
	c.Assert(rs[1][5].(string), Equals, "9")  // p0.count
	c.Assert(rs[2][5].(string), Equals, "11") // p1.count

	tk.MustQuery("show stats_topn where table_name='tdecimal' and is_index=0 and column_name='c'").Check(testkit.Rows(
		`test tdecimal global c 0 5.00 3`,
		`test tdecimal global c 0 16.00 4`,
		`test tdecimal p0 c 0 4.00 2`,
		`test tdecimal p0 c 0 5.00 3`,
		`test tdecimal p1 c 0 16.00 4`,
		`test tdecimal p1 c 0 17.00 2`))

	tk.MustQuery("show stats_topn where table_name='tdecimal' and is_index=1 and column_name='c'").Check(testkit.Rows(
		`test tdecimal global c 1 5.00 3`,
		`test tdecimal global c 1 16.00 4`,
		`test tdecimal p0 c 1 4.00 2`,
		`test tdecimal p0 c 1 5.00 3`,
		`test tdecimal p1 c 1 16.00 4`,
		`test tdecimal p1 c 1 17.00 2`))

	tk.MustQuery("show stats_buckets where table_name='tdecimal' and is_index=0 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdecimal global c 0 0 5 2 1.00 4.00 0", // bucket.ndv is not maintained for column histograms
		"test tdecimal global c 0 1 12 2 4.00 17.00 0",
		"test tdecimal p0 c 0 0 2 1 1.00 2.00 0",
		"test tdecimal p0 c 0 1 3 1 3.00 3.00 0",
		"test tdecimal p1 c 0 0 3 1 11.00 13.00 0",
		"test tdecimal p1 c 0 1 5 1 14.00 15.00 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdecimal' and column_name='c' and is_index=0").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")

	tk.MustQuery("show stats_buckets where table_name='tdecimal' and is_index=1 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdecimal global c 1 0 5 0 1.00 5.00 4", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdecimal global c 1 1 12 2 5.00 17.00 6",
		"test tdecimal p0 c 1 0 3 0 1.00 4.00 3",
		"test tdecimal p0 c 1 1 3 0 5.00 5.00 0",
		"test tdecimal p1 c 1 0 5 0 11.00 16.00 5",
		"test tdecimal p1 c 1 1 5 0 17.00 17.00 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdecimal' and column_name='c' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")

	// datetime + (column + index with 1 column)
	tk.MustExec("drop table if exists tdatetime")
	tk.MustExec(`create table tdatetime (a int, c datetime, key(c)) partition by range (a)` +
		`(partition p0 values less than(10),partition p1 values less than(20))`)
	tk.MustExec(`insert into tdatetime values ` +
		`(1, '2000-01-01'), (2, '2000-01-02'), (3, '2000-01-03'), (4, '2000-01-04'), (4, '2000-01-04'), (5, '2000-01-05'), (5, '2000-01-05'), (5, '2000-01-05'), (null, null), ` + // values in p0
		`(11, '2000-01-11'), (12, '2000-01-12'), (13, '2000-01-13'), (14, '2000-01-14'), (15, '2000-01-15'), (16, '2000-01-16'), (16, '2000-01-16'), (16, '2000-01-16'), (16, '2000-01-16'), (17, '2000-01-17'), (17, '2000-01-17')`) // values in p1
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table tdatetime with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tdatetime'").Rows()
	c.Assert(rs[0][5].(string), Equals, "20") // g.count = p0.count + p1.count
	c.Assert(rs[1][5].(string), Equals, "9")  // p0.count
	c.Assert(rs[2][5].(string), Equals, "11") // p1.count

	tk.MustQuery("show stats_topn where table_name='tdatetime' and is_index=0 and column_name='c'").Check(testkit.Rows(
		`test tdatetime global c 0 2000-01-05 00:00:00 3`,
		`test tdatetime global c 0 2000-01-16 00:00:00 4`,
		`test tdatetime p0 c 0 2000-01-04 00:00:00 2`,
		`test tdatetime p0 c 0 2000-01-05 00:00:00 3`,
		`test tdatetime p1 c 0 2000-01-16 00:00:00 4`,
		`test tdatetime p1 c 0 2000-01-17 00:00:00 2`))

	tk.MustQuery("show stats_topn where table_name='tdatetime' and is_index=1 and column_name='c'").Check(testkit.Rows(
		`test tdatetime global c 1 2000-01-05 00:00:00 3`,
		`test tdatetime global c 1 2000-01-16 00:00:00 4`,
		`test tdatetime p0 c 1 2000-01-04 00:00:00 2`,
		`test tdatetime p0 c 1 2000-01-05 00:00:00 3`,
		`test tdatetime p1 c 1 2000-01-16 00:00:00 4`,
		`test tdatetime p1 c 1 2000-01-17 00:00:00 2`))

	tk.MustQuery("show stats_buckets where table_name='tdatetime' and is_index=0 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdatetime global c 0 0 5 2 2000-01-01 00:00:00 2000-01-04 00:00:00 0", // bucket.ndv is not maintained for column histograms
		"test tdatetime global c 0 1 12 2 2000-01-04 00:00:00 2000-01-17 00:00:00 0",
		"test tdatetime p0 c 0 0 2 1 2000-01-01 00:00:00 2000-01-02 00:00:00 0",
		"test tdatetime p0 c 0 1 3 1 2000-01-03 00:00:00 2000-01-03 00:00:00 0",
		"test tdatetime p1 c 0 0 3 1 2000-01-11 00:00:00 2000-01-13 00:00:00 0",
		"test tdatetime p1 c 0 1 5 1 2000-01-14 00:00:00 2000-01-15 00:00:00 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdatetime' and column_name='c' and is_index=0").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")

	tk.MustQuery("show stats_buckets where table_name='tdatetime' and is_index=1 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdatetime global c 1 0 5 0 2000-01-01 00:00:00 2000-01-05 00:00:00 4", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdatetime global c 1 1 12 2 2000-01-05 00:00:00 2000-01-17 00:00:00 6",
		"test tdatetime p0 c 1 0 3 0 2000-01-01 00:00:00 2000-01-04 00:00:00 3",
		"test tdatetime p0 c 1 1 3 0 2000-01-05 00:00:00 2000-01-05 00:00:00 0",
		"test tdatetime p1 c 1 0 5 0 2000-01-11 00:00:00 2000-01-16 00:00:00 5",
		"test tdatetime p1 c 1 1 5 0 2000-01-17 00:00:00 2000-01-17 00:00:00 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdatetime' and column_name='c' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")

	// string + (column + index with 1 column)
	tk.MustExec("drop table if exists tstring")
	tk.MustExec(`create table tstring (a int, c varchar(32), key(c)) partition by range (a)` +
		`(partition p0 values less than(10),partition p1 values less than(20))`)
	tk.MustExec(`insert into tstring values ` +
		`(1, 'a1'), (2, 'a2'), (3, 'a3'), (4, 'a4'), (4, 'a4'), (5, 'a5'), (5, 'a5'), (5, 'a5'), (null, null), ` + // values in p0
		`(11, 'b11'), (12, 'b12'), (13, 'b13'), (14, 'b14'), (15, 'b15'), (16, 'b16'), (16, 'b16'), (16, 'b16'), (16, 'b16'), (17, 'b17'), (17, 'b17')`) // values in p1
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table tstring with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tstring'").Rows()
	c.Assert(rs[0][5].(string), Equals, "20") // g.count = p0.count + p1.count
	c.Assert(rs[1][5].(string), Equals, "9")  // p0.count
	c.Assert(rs[2][5].(string), Equals, "11") // p1.count

	tk.MustQuery("show stats_topn where table_name='tstring' and is_index=0 and column_name='c'").Check(testkit.Rows(
		`test tstring global c 0 a5 3`,
		`test tstring global c 0 b16 4`,
		`test tstring p0 c 0 a4 2`,
		`test tstring p0 c 0 a5 3`,
		`test tstring p1 c 0 b16 4`,
		`test tstring p1 c 0 b17 2`))

	tk.MustQuery("show stats_topn where table_name='tstring' and is_index=1 and column_name='c'").Check(testkit.Rows(
		`test tstring global c 1 a5 3`,
		`test tstring global c 1 b16 4`,
		`test tstring p0 c 1 a4 2`,
		`test tstring p0 c 1 a5 3`,
		`test tstring p1 c 1 b16 4`,
		`test tstring p1 c 1 b17 2`))

	tk.MustQuery("show stats_buckets where table_name='tstring' and is_index=0 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tstring global c 0 0 5 2 a1 a4 0", // bucket.ndv is not maintained for column histograms
		"test tstring global c 0 1 12 2 a4 b17 0",
		"test tstring p0 c 0 0 2 1 a1 a2 0",
		"test tstring p0 c 0 1 3 1 a3 a3 0",
		"test tstring p1 c 0 0 3 1 b11 b13 0",
		"test tstring p1 c 0 1 5 1 b14 b15 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tstring' and column_name='c' and is_index=0").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")

	tk.MustQuery("show stats_buckets where table_name='tstring' and is_index=1 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tstring global c 1 0 5 0 a1 a5 4", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tstring global c 1 1 12 2 a5 b17 6",
		"test tstring p0 c 1 0 3 0 a1 a4 3",
		"test tstring p0 c 1 1 3 0 a5 a5 0",
		"test tstring p1 c 1 0 5 0 b11 b16 5",
		"test tstring p1 c 1 1 5 0 b17 b17 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tstring' and column_name='c' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")
}

func (s *testStatsSuite) TestGlobalStatsVersion(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`
create table t (
	a int
)
partition by range (a) (
	partition p0 values less than (10),
	partition p1 values less than (20)
)`)
	tk.MustExec("insert into t values (1), (5), (null), (11), (15)")
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)

	tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("analyze table t")
	c.Assert(len(tk.MustQuery("show stats_meta").Rows()), Equals, 2) // p0 + p1

	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@tidb_analyze_version=1")
	err := tk.ExecToErr("analyze table t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[stats]: global statistics for partitioned tables only available in statistics version2, please set tidb_analyze_version to 2")

	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	c.Assert(len(tk.MustQuery("show stats_meta").Rows()), Equals, 3) // p0 + p1 + global
}

func (s *testStatsSuite) TestExtendedStatsDefaultSwitch(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int, d int)")
	err := tk.ExecToErr("alter table t add stats_extended s1 correlation(b,c)")
	c.Assert(err.Error(), Equals, "Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	err = tk.ExecToErr("alter table t drop stats_extended s1")
	c.Assert(err.Error(), Equals, "Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	err = tk.ExecToErr("admin reload stats_extended")
	c.Assert(err.Error(), Equals, "Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
}

func (s *testStatsSuite) TestExtendedStatsOps(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int, d int)")
	tk.MustExec("insert into t values(1,1,5,1),(2,2,4,2),(3,3,3,3),(4,4,2,4),(5,5,1,5)")
	tk.MustExec("analyze table t")
	err := tk.ExecToErr("alter table not_exist_db.t add stats_extended s1 correlation(b,c)")
	c.Assert(err.Error(), Equals, "[schema:1146]Table 'not_exist_db.t' doesn't exist")
	err = tk.ExecToErr("alter table not_exist_tbl add stats_extended s1 correlation(b,c)")
	c.Assert(err.Error(), Equals, "[schema:1146]Table 'test.not_exist_tbl' doesn't exist")
	err = tk.ExecToErr("alter table t add stats_extended s1 correlation(b,e)")
	c.Assert(err.Error(), Equals, "[schema:1054]Unknown column 'e' in 't'")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 No need to create correlation statistics on the integer primary key column",
	))
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows())
	err = tk.ExecToErr("alter table t add stats_extended s1 correlation(b,c,d)")
	c.Assert(err.Error(), Equals, "Only support Correlation and Dependency statistics types on 2 columns")

	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows())
	tk.MustExec("alter table t add stats_extended s1 correlation(b,c)")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"2 [2,3] <nil> 0",
	))
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	do.StatsHandle().Update(is)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustExec("update mysql.stats_extended set status = 1 where name = 's1'")
	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustExec("alter table t drop stats_extended s1")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"2 [2,3] <nil> 2",
	))
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)
}

func (s *testStatsSuite) TestAdminReloadStatistics(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int, d int)")
	tk.MustExec("insert into t values(1,1,5,1),(2,2,4,2),(3,3,3,3),(4,4,2,4),(5,5,1,5)")
	tk.MustExec("analyze table t")
	tk.MustExec("alter table t add stats_extended s1 correlation(b,c)")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"2 [2,3] <nil> 0",
	))
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	do.StatsHandle().Update(is)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustExec("update mysql.stats_extended set status = 1 where name = 's1'")
	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustExec("delete from mysql.stats_extended where name = 's1'")
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustExec("admin reload stats_extended")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)
}

func (s *testStatsSuite) TestCorrelationStatsCompute(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("insert into t values(1,1,5),(2,2,4),(3,3,3),(4,4,2),(5,5,1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Check(testkit.Rows())
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("alter table t add stats_extended s2 correlation(a,c)")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] <nil> 0",
		"2 [1,3] <nil> 0",
	))
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	do.StatsHandle().Update(is)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] -1.000000 1",
	))
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 2)
	foundS1, foundS2 := false, false
	for name, item := range statsTbl.ExtendedStats.Stats {
		switch name {
		case "s1":
			foundS1 = true
			c.Assert(item.ScalarVals, Equals, float64(1))
		case "s2":
			foundS2 = true
			c.Assert(item.ScalarVals, Equals, float64(-1))
		default:
			c.Assert("Unexpected extended stats in cache", IsNil)
		}
	}
	c.Assert(foundS1 && foundS2, IsTrue)
}

func (s *testStatsSuite) TestStaticPartitionPruneMode(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Static) + "'")
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, key(a)) partition by range(a)
					(partition p0 values less than (10),
					partition p1 values less than (22))`)
	tk.MustExec(`insert into t values (1), (2), (3), (10), (11)`)
	tk.MustExec(`analyze table t`)
	c.Assert(tk.MustNoGlobalStats("t"), IsTrue)
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Dynamic) + "'")
	c.Assert(tk.MustNoGlobalStats("t"), IsTrue)

	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Static) + "'")
	tk.MustExec(`insert into t values (4), (5), (6)`)
	tk.MustExec(`analyze table t partition p0`)
	c.Assert(tk.MustNoGlobalStats("t"), IsTrue)
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Dynamic) + "'")
	c.Assert(tk.MustNoGlobalStats("t"), IsTrue)
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Static) + "'")
}

func (s *testStatsSuite) TestMergeIdxHist(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Dynamic) + "'")
	defer tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Static) + "'")
	tk.MustExec("use test")
	tk.MustExec(`
		create table t (a int, key(a))
		partition by range (a) (
			partition p0 values less than (10),
			partition p1 values less than (20))`)
	tk.MustExec("set @@tidb_analyze_version=2")
	defer tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (6), (null), (11), (12), (13), (14), (15), (16), (17), (18), (19), (19)")

	tk.MustExec("analyze table t with 2 topn, 2 buckets")
	rows := tk.MustQuery("show stats_buckets where partition_name like 'global'")
	c.Assert(len(rows.Rows()), Equals, 4)
}

func (s *testStatsSuite) TestAnalyzeWithDynamicPartitionPruneMode(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec(`create table t (a int, key(a)) partition by range(a)
					(partition p0 values less than (10),
					partition p1 values less than (22))`)
	tk.MustExec(`insert into t values (1), (2), (3), (10), (11)`)
	tk.MustExec(`analyze table t with 1 topn, 2 buckets`)
	rows := tk.MustQuery("show stats_buckets where partition_name = 'global' and is_index=1").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[1][6], Equals, "4")
	tk.MustExec("insert into t values (1), (2), (2)")
	tk.MustExec("analyze table t partition p0 with 1 topn, 2 buckets")
	rows = tk.MustQuery("show stats_buckets where partition_name = 'global' and is_index=1").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[1][6], Equals, "5")
	tk.MustExec("insert into t values (3)")
	tk.MustExec("analyze table t partition p0 index a with 1 topn, 2 buckets")
	rows = tk.MustQuery("show stats_buckets where partition_name = 'global' and is_index=1").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[1][6], Equals, "6")
}

func (s *testStatsSuite) TestFMSWithAnalyzePartition(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec(`create table t (a int, key(a)) partition by range(a) 
					(partition p0 values less than (10),
					partition p1 values less than (22))`)
	tk.MustExec(`insert into t values (1), (2), (3), (10), (11)`)
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("0"))
	tk.MustExec("analyze table t partition p0 with 1 topn, 2 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 8131 Build table: `t` global-level stats failed due to missing partition-level stats",
		"Warning 8131 Build table: `t` index: `a` global-level stats failed due to missing partition-level stats",
	))
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("1"))
}

var _ = SerialSuites(&statsSerialSuite{})

type statsSerialSuite struct {
	testSuiteBase
}

func (s *statsSerialSuite) TestIndexUsageInformation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_idx(a int, b int)")
	tk.MustExec("create unique index idx_a on t_idx(a)")
	tk.MustExec("create unique index idx_b on t_idx(b)")
	tk.MustQuery("select a from t_idx where a=1")
	querySQL := `select idx.table_schema, idx.table_name, idx.key_name, stats.query_count, stats.rows_selected
					from mysql.schema_index_usage as stats, information_schema.tidb_indexes as idx, information_schema.tables as tables
					where tables.table_schema = idx.table_schema
						AND tables.table_name = idx.table_name
						AND tables.tidb_table_id = stats.table_id
						AND idx.index_id = stats.index_id
						AND idx.table_name = "t_idx"`
	do := s.do
	err := do.StatsHandle().DumpIndexUsageToKV()
	c.Assert(err, IsNil)
	tk.MustQuery(querySQL).Check(testkit.Rows(
		"test t_idx idx_a 1 0",
	))
	tk.MustExec("insert into t_idx values(1, 0)")
	tk.MustQuery("select a from t_idx where a=1")
	tk.MustQuery("select a from t_idx where a=1")
	err = do.StatsHandle().DumpIndexUsageToKV()
	c.Assert(err, IsNil)
	tk.MustQuery(querySQL).Check(testkit.Rows(
		"test t_idx idx_a 3 2",
	))
	tk.MustQuery("select b from t_idx where b=0")
	tk.MustQuery("select b from t_idx where b=0")
	err = do.StatsHandle().DumpIndexUsageToKV()
	c.Assert(err, IsNil)
	tk.MustQuery(querySQL).Check(testkit.Rows(
		"test t_idx idx_a 3 2",
		"test t_idx idx_b 2 2",
	))
}

func (s *statsSerialSuite) TestGCIndexUsageInformation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_idx(a int, b int)")
	tk.MustExec("create unique index idx_a on t_idx(a)")
	tk.MustQuery("select a from t_idx where a=1")
	do := s.do
	err := do.StatsHandle().DumpIndexUsageToKV()
	c.Assert(err, IsNil)
	querySQL := `select count(distinct idx.table_schema, idx.table_name, idx.key_name, stats.query_count, stats.rows_selected)
					from mysql.schema_index_usage as stats, information_schema.tidb_indexes as idx, information_schema.tables as tables
					where tables.table_schema = idx.table_schema
						AND tables.table_name = idx.table_name
						AND tables.tidb_table_id = stats.table_id
						AND idx.index_id = stats.index_id
						AND idx.table_name = "t_idx"`
	tk.MustQuery(querySQL).Check(testkit.Rows("1"))
	tk.MustExec("drop index `idx_a` on t_idx")
	err = do.StatsHandle().GCIndexUsage()
	c.Assert(err, IsNil)
	tk.MustQuery(querySQL).Check(testkit.Rows("0"))
}

func (s *testStatsSuite) TestExtendedStatsPartitionTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int) partition by range(a) (partition p0 values less than (5), partition p1 values less than (10))")
	tk.MustExec("create table t2(a int, b int, c int) partition by hash(a) partitions 4")
	err := tk.ExecToErr("alter table t1 add stats_extended s1 correlation(b,c)")
	c.Assert(err.Error(), Equals, "Extended statistics on partitioned tables are not supported now")
	err = tk.ExecToErr("alter table t2 add stats_extended s1 correlation(b,c)")
	c.Assert(err.Error(), Equals, "Extended statistics on partitioned tables are not supported now")
}
