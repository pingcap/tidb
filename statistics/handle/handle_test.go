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
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestT(t *testing.T) {
	TestingT(t)
}

// TODO replace cleanEnv with createTestKitAndDom in gc_series_test.go when migrate this file
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
	tk.MustExec("delete from mysql.column_stats_usage")
	tk.MustExec("delete from mysql.tidb where variable_name = 'tidb_disable_column_tracking_time'")
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
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
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
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)

	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.MemoryUsage() > 0, IsTrue)
	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
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
		c.Assert(a.Columns[i].TopN.Equal(b.Columns[i].TopN), IsTrue, Commentf("%v, %v", a.Columns[i].TopN, b.Columns[i].TopN))
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
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
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
	count := statsTbl.ColumnGreaterRowCount(mock.NewContext(), types.NewDatum(1), tableInfo.Columns[0].ID)
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
	sctx := mock.NewContext()
	ran := &ranger.Range{
		LowVal:      []types.Datum{types.MinNotNullDatum()},
		HighVal:     []types.Datum{types.NewIntDatum(2)},
		LowExclude:  false,
		HighExclude: true,
		Collators:   collate.GetBinaryCollatorSlice(1),
	}
	count, err := statsTbl.GetRowCountByColumnRanges(sctx, tableInfo.Columns[0].ID, []*ranger.Range{ran})
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(1))

	// Drop a column and the offset changed,
	testKit.MustExec("alter table t drop column c1")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	// At that time, we should get c2's stats instead of c1's.
	count, err = statsTbl.GetRowCountByColumnRanges(sctx, tableInfo.Columns[0].ID, []*ranger.Range{ran})
	c.Assert(err, IsNil)
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
	err = h.Update(do.InfoSchema())
	c.Assert(err, IsNil)
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
	testKit.MustExec("set @@session.tidb_analyze_version = 1")
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

func (s *testStatsSuite) TestReloadExtStatsLockRelease(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/statistics/handle/injectExtStatsLoadErr", `return("")`), IsNil)
	err := tk.ExecToErr("admin reload stats_extended")
	c.Assert(err.Error(), Equals, "gofail extendedStatsFromStorage error")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/statistics/handle/injectExtStatsLoadErr"), IsNil)
	// Check the lock is released by `admin reload stats_extended` if error happens.
	tk.MustExec("analyze table t")
}

func (s *testStatsSuite) TestLoadStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
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
	_, err = stat.ColumnEqualRowCount(testKit.Se, types.NewIntDatum(1), tableInfo.Columns[2].ID)
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
	testKit.MustExec("select * from t where c1 > 10 and c2 > 10")
	testKit.MustExec("insert into t values(1,1),(3,12),(4,20),(2,7),(5,21)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result := testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "1")
	testKit.MustExec("insert into t values(8,18)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "0.8285714285714286")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "0.8285714285714286")

	testKit.MustExec("truncate table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 0)
	testKit.MustExec("insert into t values(1,21),(3,12),(4,7),(2,20),(5,1)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "-1")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "-1")
	testKit.MustExec("insert into t values(8,4)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "-0.9428571428571428")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "-0.9428571428571428")

	testKit.MustExec("truncate table t")
	testKit.MustExec("insert into t values (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1),(11,1),(12,1),(13,1),(14,1),(15,1),(16,1),(17,1),(18,1),(19,1),(20,2),(21,2),(22,2),(23,2),(24,2),(25,2)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "1")

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(c1 int, c2 int)")
	testKit.MustExec("insert into t values(1,1),(2,7),(3,12),(4,20),(5,21),(8,18)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
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
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0.8285714285714286")
	c.Assert(result.Rows()[1][9], Equals, "1")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0.8285714285714286")
	c.Assert(result.Rows()[1][9], Equals, "1")

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(c1 int primary key, c2 int, c3 int, key idx_c2(c2))")
	testKit.MustExec("insert into t values(1,1,1),(2,2,2),(3,3,3)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 0").Sort()
	c.Assert(len(result.Rows()), Equals, 3)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")
	c.Assert(result.Rows()[2][9], Equals, "1")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 1").Sort()
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][9], Equals, "0")
	testKit.MustExec("set @@tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 0").Sort()
	c.Assert(len(result.Rows()), Equals, 3)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "1")
	c.Assert(result.Rows()[2][9], Equals, "1")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 1").Sort()
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][9], Equals, "0")
}

func (s *testStatsSuite) TestAnalyzeVirtualCol(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int generated always as (-a) virtual, c int generated always as (-a) stored, index (c))")
	tk.MustExec("insert into t(a) values(2),(1),(1),(3),(NULL)")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("analyze table t")
	c.Assert(len(tk.MustQuery("show stats_histograms where table_name ='t'").Rows()), Equals, 3)
}

func (s *testStatsSuite) TestShowGlobalStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 0")
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

// nolint:unused
func (s *testSerialStatsSuite) prepareForGlobalStatsWithOpts(c *C, tk *testkit.TestKit, tblName, dbName string) {
	tk.MustExec("create database if not exists " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec("drop table if exists " + tblName)
	tk.MustExec(` create table ` + tblName + ` (a int, key(a)) partition by range (a) ` +
		`(partition p0 values less than (100000), partition p1 values less than (200000))`)
	buf1 := bytes.NewBufferString("insert into " + tblName + " values (0)")
	buf2 := bytes.NewBufferString("insert into " + tblName + " values (100000)")
	for i := 0; i < 5000; i += 3 {
		buf1.WriteString(fmt.Sprintf(", (%v)", i))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100000+i))
	}
	for i := 0; i < 1000; i++ {
		buf1.WriteString(fmt.Sprintf(", (%v)", 0))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100000))
	}
	tk.MustExec(buf1.String())
	tk.MustExec(buf2.String())
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
}

// nolint:unused
func (s *testSerialStatsSuite) checkForGlobalStatsWithOpts(c *C, tk *testkit.TestKit, db, t, p string, topn, buckets int) {
	tbl, err := s.do.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(t))
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	physicalID := tblInfo.ID
	if p != "global" {
		for _, def := range tbl.Meta().GetPartitionInfo().Definitions {
			if def.Name.L == p {
				physicalID = def.ID
			}
		}
	}
	tblStats, err := s.do.StatsHandle().TableStatsFromStorage(tblInfo, physicalID, true, 0)
	c.Assert(err, IsNil)

	delta := buckets/2 + 10
	for _, idxStats := range tblStats.Indices {
		numTopN := idxStats.TopN.Num()
		numBuckets := len(idxStats.Buckets)
		// since the hist-building algorithm doesn't stipulate the final bucket number to be equal to the expected number exactly,
		// we have to check the results by a range here.
		c.Assert(numTopN, Equals, topn)
		c.Assert(numBuckets, GreaterEqual, buckets-delta)
		c.Assert(numBuckets, LessEqual, buckets+delta)
	}
	for _, colStats := range tblStats.Columns {
		if len(colStats.Buckets) == 0 {
			continue // it's not loaded
		}
		numTopN := colStats.TopN.Num()
		numBuckets := len(colStats.Buckets)
		c.Assert(numTopN, Equals, topn)
		c.Assert(numBuckets, GreaterEqual, buckets-delta)
		c.Assert(numBuckets, LessEqual, buckets+delta)
	}
}

func (s *testSerialStatsSuite) TestAnalyzeGlobalStatsWithOpts1(c *C) {
	c.Skip("unstable test")
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	s.prepareForGlobalStatsWithOpts(c, tk, "test_gstats_opt", "test_gstats_opt")

	// nolint:unused
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
		sql := fmt.Sprintf("analyze table test_gstats_opt with %v topn, %v buckets", ca.topn, ca.buckets)
		if !ca.err {
			tk.MustExec(sql)
			s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt", "test_gstats_opt", "global", ca.topn, ca.buckets)
			s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt", "test_gstats_opt", "p0", ca.topn, ca.buckets)
			s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt", "test_gstats_opt", "p1", ca.topn, ca.buckets)
		} else {
			err := tk.ExecToErr(sql)
			c.Assert(err, NotNil)
		}
	}
}

func (s *testSerialStatsSuite) TestAnalyzeGlobalStatsWithOpts2(c *C) {
	c.Skip("unstable test")
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
	}()
	tk.MustExec("set global tidb_persist_analyze_options=false")
	s.prepareForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2")

	tk.MustExec("analyze table test_gstats_opt2 with 20 topn, 50 buckets, 1000 samples")
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "global", 2, 50)
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "p0", 1, 50)
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "p1", 1, 50)

	// analyze a partition to let its options be different with others'
	tk.MustExec("analyze table test_gstats_opt2 partition p0 with 10 topn, 20 buckets")
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "global", 10, 20) // use new options
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "p0", 10, 20)
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "p1", 1, 50)

	tk.MustExec("analyze table test_gstats_opt2 partition p1 with 100 topn, 200 buckets")
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "global", 100, 200)
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "p0", 10, 20)
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "p1", 100, 200)

	tk.MustExec("analyze table test_gstats_opt2 partition p0 with 20 topn") // change back to 20 topn
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "global", 20, 256)
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "p0", 20, 256)
	s.checkForGlobalStatsWithOpts(c, tk, "test_gstats_opt2", "test_gstats_opt2", "p1", 100, 200)
}

func (s *testStatsSuite) TestGlobalStatsHealthy(c *C) {
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

	checkModifyAndCount := func(gModify, gCount, p0Modify, p0Count, p1Modify, p1Count int) {
		rs := tk.MustQuery("show stats_meta").Rows()
		c.Assert(rs[0][4].(string), Equals, fmt.Sprintf("%v", gModify))  // global.modify_count
		c.Assert(rs[0][5].(string), Equals, fmt.Sprintf("%v", gCount))   // global.row_count
		c.Assert(rs[1][4].(string), Equals, fmt.Sprintf("%v", p0Modify)) // p0.modify_count
		c.Assert(rs[1][5].(string), Equals, fmt.Sprintf("%v", p0Count))  // p0.row_count
		c.Assert(rs[2][4].(string), Equals, fmt.Sprintf("%v", p1Modify)) // p1.modify_count
		c.Assert(rs[2][5].(string), Equals, fmt.Sprintf("%v", p1Count))  // p1.row_count
	}
	checkHealthy := func(gH, p0H, p1H int) {
		tk.MustQuery("show stats_healthy").Check(testkit.Rows(
			fmt.Sprintf("test t global %v", gH),
			fmt.Sprintf("test t p0 %v", p0H),
			fmt.Sprintf("test t p1 %v", p1H)))
	}

	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("analyze table t")
	checkModifyAndCount(0, 0, 0, 0, 0, 0)
	checkHealthy(100, 100, 100)

	tk.MustExec("insert into t values (1), (2)") // update p0
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(s.do.StatsHandle().Update(s.do.InfoSchema()), IsNil)
	checkModifyAndCount(2, 2, 2, 2, 0, 0)
	checkHealthy(0, 0, 100)

	tk.MustExec("insert into t values (11), (12), (13), (14)") // update p1
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(s.do.StatsHandle().Update(s.do.InfoSchema()), IsNil)
	checkModifyAndCount(6, 6, 2, 2, 4, 4)
	checkHealthy(0, 0, 0)

	tk.MustExec("analyze table t")
	checkModifyAndCount(0, 6, 0, 2, 0, 4)
	checkHealthy(100, 100, 100)

	tk.MustExec("insert into t values (4), (5), (15), (16)") // update p0 and p1 together
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(s.do.StatsHandle().Update(s.do.InfoSchema()), IsNil)
	checkModifyAndCount(4, 10, 2, 4, 2, 6)
	checkHealthy(60, 50, 66)
}

func (s *testStatsSuite) TestHideGlobalStatsSwitch(c *C) {
	// NOTICE: remove this test when this global-stats is GA.
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	rs := tk.MustQuery("show variables").Rows()
	for _, r := range rs {
		c.Assert(strings.ToLower(r[0].(string)), Not(Equals), "tidb_partition_prune_mode")
	}
	c.Assert(len(tk.MustQuery("show variables where variable_name like '%tidb_partition_prune_mode%'").Rows()), Equals, 0)
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
		testkit.Rows("15 1 0 1", "6 1 6 1", "9 0 10 1"))

	tk.MustQuery("show stats_buckets where is_index=0").Check(
		// db table partition col is_idx bucket_id count repeats lower upper ndv
		testkit.Rows("test t global a 0 0 7 2 1 6 0",
			"test t global a 0 1 17 2 6 19 0",
			"test t p0 a 0 0 4 1 1 4 0",
			"test t p0 a 0 1 7 2 5 6 0",
			"test t p1 a 0 0 6 1 11 16 0",
			"test t p1 a 0 1 10 2 17 19 0"))
	tk.MustQuery("show stats_buckets where is_index=1").Check(
		testkit.Rows("test t global a 1 0 7 2 1 6 0",
			"test t global a 1 1 17 2 6 19 0",
			"test t p0 a 1 0 4 1 1 4 0",
			"test t p0 a 1 1 7 2 5 6 0",
			"test t p1 a 1 0 6 1 11 16 0",
			"test t p1 a 1 1 10 2 17 19 0"))
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
		"test tint global c 0 1 12 2 17 17 0",
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
		"test tint global c 1 0 5 2 1 4 0",    // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tint global c 1 1 12 2 17 17 0", // same with the column's
		"test tint p0 c 1 0 2 1 1 2 0",
		"test tint p0 c 1 1 3 1 3 3 0",
		"test tint p1 c 1 0 3 1 11 13 0",
		"test tint p1 c 1 1 5 1 14 15 0"))

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
		"test tdouble global c 0 1 12 2 17 17 0",
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
		"test tdouble global c 1 0 5 2 1 4 0", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdouble global c 1 1 12 2 17 17 0",
		"test tdouble p0 c 1 0 2 1 1 2 0",
		"test tdouble p0 c 1 1 3 1 3 3 0",
		"test tdouble p1 c 1 0 3 1 11 13 0",
		"test tdouble p1 c 1 1 5 1 14 15 0"))

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
		"test tdecimal global c 0 1 12 2 17.00 17.00 0",
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
		"test tdecimal global c 1 0 5 2 1.00 4.00 0", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdecimal global c 1 1 12 2 17.00 17.00 0",
		"test tdecimal p0 c 1 0 2 1 1.00 2.00 0",
		"test tdecimal p0 c 1 1 3 1 3.00 3.00 0",
		"test tdecimal p1 c 1 0 3 1 11.00 13.00 0",
		"test tdecimal p1 c 1 1 5 1 14.00 15.00 0"))

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
		"test tdatetime global c 0 1 12 2 2000-01-17 00:00:00 2000-01-17 00:00:00 0",
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
		"test tdatetime global c 1 0 5 2 2000-01-01 00:00:00 2000-01-04 00:00:00 0", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdatetime global c 1 1 12 2 2000-01-17 00:00:00 2000-01-17 00:00:00 0",
		"test tdatetime p0 c 1 0 2 1 2000-01-01 00:00:00 2000-01-02 00:00:00 0",
		"test tdatetime p0 c 1 1 3 1 2000-01-03 00:00:00 2000-01-03 00:00:00 0",
		"test tdatetime p1 c 1 0 3 1 2000-01-11 00:00:00 2000-01-13 00:00:00 0",
		"test tdatetime p1 c 1 1 5 1 2000-01-14 00:00:00 2000-01-15 00:00:00 0"))

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
		"test tstring global c 0 1 12 2 b17 b17 0",
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
		"test tstring global c 1 0 5 2 a1 a4 0", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tstring global c 1 1 12 2 b17 b17 0",
		"test tstring p0 c 1 0 2 1 a1 a2 0",
		"test tstring p0 c 1 1 3 1 a3 a3 0",
		"test tstring p1 c 1 0 3 1 b11 b13 0",
		"test tstring p1 c 1 1 5 1 b14 b15 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tstring' and column_name='c' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "12") // g.ndv = p0 + p1
	c.Assert(rs[1][6].(string), Equals, "5")
	c.Assert(rs[2][6].(string), Equals, "7")
	c.Assert(rs[0][7].(string), Equals, "1") // g.null_count = p0 + p1
	c.Assert(rs[1][7].(string), Equals, "1")
	c.Assert(rs[2][7].(string), Equals, "0")
}

func (s *testStatsSuite) TestGlobalStatsData3(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@tidb_analyze_version=2")

	// index(int, int)
	tk.MustExec("drop table if exists tintint")
	tk.MustExec("create table tintint (a int, b int, key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tintint values ` +
		`(1, 1), (1, 2), (2, 1), (2, 2), (2, 3), (2, 3), (3, 1), (3, 1), (3, 1),` + // values in p0
		`(11, 1), (12, 1), (12, 2), (13, 1), (13, 1), (13, 2), (13, 2), (13, 2)`) // values in p1
	tk.MustExec("analyze table tintint with 2 topn, 2 buckets")

	rs := tk.MustQuery("show stats_meta where table_name='tintint'").Rows()
	c.Assert(rs[0][5].(string), Equals, "17") // g.total = p0.total + p1.total
	c.Assert(rs[1][5].(string), Equals, "9")
	c.Assert(rs[2][5].(string), Equals, "8")

	tk.MustQuery("show stats_topn where table_name='tintint' and is_index=1").Check(testkit.Rows(
		"test tintint global a 1 (3, 1) 3",
		"test tintint global a 1 (13, 2) 3",
		"test tintint p0 a 1 (2, 3) 2",
		"test tintint p0 a 1 (3, 1) 3",
		"test tintint p1 a 1 (13, 1) 2",
		"test tintint p1 a 1 (13, 2) 3"))

	tk.MustQuery("show stats_buckets where table_name='tintint' and is_index=1").Check(testkit.Rows(
		"test tintint global a 1 0 6 2 (1, 1) (2, 3) 0",    // (2, 3) is popped into it
		"test tintint global a 1 1 11 2 (13, 1) (13, 1) 0", // (13, 1) is popped into it
		"test tintint p0 a 1 0 3 1 (1, 1) (2, 1) 0",
		"test tintint p0 a 1 1 4 1 (2, 2) (2, 2) 0",
		"test tintint p1 a 1 0 2 1 (11, 1) (12, 1) 0",
		"test tintint p1 a 1 1 3 1 (12, 2) (12, 2) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tintint' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "11") // g.ndv = p0.ndv + p1.ndv
	c.Assert(rs[1][6].(string), Equals, "6")
	c.Assert(rs[2][6].(string), Equals, "5")

	// index(int, string)
	tk.MustExec("drop table if exists tintstr")
	tk.MustExec("create table tintstr (a int, b varchar(32), key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tintstr values ` +
		`(1, '1'), (1, '2'), (2, '1'), (2, '2'), (2, '3'), (2, '3'), (3, '1'), (3, '1'), (3, '1'),` + // values in p0
		`(11, '1'), (12, '1'), (12, '2'), (13, '1'), (13, '1'), (13, '2'), (13, '2'), (13, '2')`) // values in p1
	tk.MustExec("analyze table tintstr with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tintstr'").Rows()
	c.Assert(rs[0][5].(string), Equals, "17") // g.total = p0.total + p1.total
	c.Assert(rs[1][5].(string), Equals, "9")
	c.Assert(rs[2][5].(string), Equals, "8")

	tk.MustQuery("show stats_topn where table_name='tintstr' and is_index=1").Check(testkit.Rows(
		"test tintstr global a 1 (3, 1) 3",
		"test tintstr global a 1 (13, 2) 3",
		"test tintstr p0 a 1 (2, 3) 2",
		"test tintstr p0 a 1 (3, 1) 3",
		"test tintstr p1 a 1 (13, 1) 2",
		"test tintstr p1 a 1 (13, 2) 3"))

	tk.MustQuery("show stats_buckets where table_name='tintstr' and is_index=1").Check(testkit.Rows(
		"test tintstr global a 1 0 6 2 (1, 1) (2, 3) 0",    // (2, 3) is popped into it
		"test tintstr global a 1 1 11 2 (13, 1) (13, 1) 0", // (13, 1) is popped into it
		"test tintstr p0 a 1 0 3 1 (1, 1) (2, 1) 0",
		"test tintstr p0 a 1 1 4 1 (2, 2) (2, 2) 0",
		"test tintstr p1 a 1 0 2 1 (11, 1) (12, 1) 0",
		"test tintstr p1 a 1 1 3 1 (12, 2) (12, 2) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tintstr' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "11") // g.ndv = p0.ndv + p1.ndv
	c.Assert(rs[1][6].(string), Equals, "6")
	c.Assert(rs[2][6].(string), Equals, "5")

	// index(int, double)
	tk.MustExec("drop table if exists tintdouble")
	tk.MustExec("create table tintdouble (a int, b double, key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tintdouble values ` +
		`(1, 1), (1, 2), (2, 1), (2, 2), (2, 3), (2, 3), (3, 1), (3, 1), (3, 1),` + // values in p0
		`(11, 1), (12, 1), (12, 2), (13, 1), (13, 1), (13, 2), (13, 2), (13, 2)`) // values in p1
	tk.MustExec("analyze table tintdouble with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tintdouble'").Rows()
	c.Assert(rs[0][5].(string), Equals, "17") // g.total = p0.total + p1.total
	c.Assert(rs[1][5].(string), Equals, "9")
	c.Assert(rs[2][5].(string), Equals, "8")

	tk.MustQuery("show stats_topn where table_name='tintdouble' and is_index=1").Check(testkit.Rows(
		"test tintdouble global a 1 (3, 1) 3",
		"test tintdouble global a 1 (13, 2) 3",
		"test tintdouble p0 a 1 (2, 3) 2",
		"test tintdouble p0 a 1 (3, 1) 3",
		"test tintdouble p1 a 1 (13, 1) 2",
		"test tintdouble p1 a 1 (13, 2) 3"))

	tk.MustQuery("show stats_buckets where table_name='tintdouble' and is_index=1").Check(testkit.Rows(
		"test tintdouble global a 1 0 6 2 (1, 1) (2, 3) 0",    // (2, 3) is popped into it
		"test tintdouble global a 1 1 11 2 (13, 1) (13, 1) 0", // (13, 1) is popped into it
		"test tintdouble p0 a 1 0 3 1 (1, 1) (2, 1) 0",
		"test tintdouble p0 a 1 1 4 1 (2, 2) (2, 2) 0",
		"test tintdouble p1 a 1 0 2 1 (11, 1) (12, 1) 0",
		"test tintdouble p1 a 1 1 3 1 (12, 2) (12, 2) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tintdouble' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "11") // g.ndv = p0.ndv + p1.ndv
	c.Assert(rs[1][6].(string), Equals, "6")
	c.Assert(rs[2][6].(string), Equals, "5")

	// index(double, decimal)
	tk.MustExec("drop table if exists tdoubledecimal")
	tk.MustExec("create table tdoubledecimal (a int, b decimal(30, 2), key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tdoubledecimal values ` +
		`(1, 1), (1, 2), (2, 1), (2, 2), (2, 3), (2, 3), (3, 1), (3, 1), (3, 1),` + // values in p0
		`(11, 1), (12, 1), (12, 2), (13, 1), (13, 1), (13, 2), (13, 2), (13, 2)`) // values in p1
	tk.MustExec("analyze table tdoubledecimal with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tdoubledecimal'").Rows()
	c.Assert(rs[0][5].(string), Equals, "17") // g.total = p0.total + p1.total
	c.Assert(rs[1][5].(string), Equals, "9")
	c.Assert(rs[2][5].(string), Equals, "8")

	tk.MustQuery("show stats_topn where table_name='tdoubledecimal' and is_index=1").Check(testkit.Rows(
		"test tdoubledecimal global a 1 (3, 1.00) 3",
		"test tdoubledecimal global a 1 (13, 2.00) 3",
		"test tdoubledecimal p0 a 1 (2, 3.00) 2",
		"test tdoubledecimal p0 a 1 (3, 1.00) 3",
		"test tdoubledecimal p1 a 1 (13, 1.00) 2",
		"test tdoubledecimal p1 a 1 (13, 2.00) 3"))

	tk.MustQuery("show stats_buckets where table_name='tdoubledecimal' and is_index=1").Check(testkit.Rows(
		"test tdoubledecimal global a 1 0 6 2 (1, 1.00) (2, 3.00) 0",    // (2, 3) is popped into it
		"test tdoubledecimal global a 1 1 11 2 (13, 1.00) (13, 1.00) 0", // (13, 1) is popped into it
		"test tdoubledecimal p0 a 1 0 3 1 (1, 1.00) (2, 1.00) 0",
		"test tdoubledecimal p0 a 1 1 4 1 (2, 2.00) (2, 2.00) 0",
		"test tdoubledecimal p1 a 1 0 2 1 (11, 1.00) (12, 1.00) 0",
		"test tdoubledecimal p1 a 1 1 3 1 (12, 2.00) (12, 2.00) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdoubledecimal' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "11") // g.ndv = p0.ndv + p1.ndv
	c.Assert(rs[1][6].(string), Equals, "6")
	c.Assert(rs[2][6].(string), Equals, "5")

	// index(string, datetime)
	tk.MustExec("drop table if exists tstrdt")
	tk.MustExec("create table tstrdt (a int, b datetime, key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tstrdt values ` +
		`(1, '2000-01-01'), (1, '2000-01-02'), (2, '2000-01-01'), (2, '2000-01-02'), (2, '2000-01-03'), (2, '2000-01-03'), (3, '2000-01-01'), (3, '2000-01-01'), (3, '2000-01-01'),` + // values in p0
		`(11, '2000-01-01'), (12, '2000-01-01'), (12, '2000-01-02'), (13, '2000-01-01'), (13, '2000-01-01'), (13, '2000-01-02'), (13, '2000-01-02'), (13, '2000-01-02')`) // values in p1
	tk.MustExec("analyze table tstrdt with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tstrdt'").Rows()
	c.Assert(rs[0][5].(string), Equals, "17") // g.total = p0.total + p1.total
	c.Assert(rs[1][5].(string), Equals, "9")
	c.Assert(rs[2][5].(string), Equals, "8")

	tk.MustQuery("show stats_topn where table_name='tstrdt' and is_index=1").Check(testkit.Rows(
		"test tstrdt global a 1 (3, 2000-01-01 00:00:00) 3",
		"test tstrdt global a 1 (13, 2000-01-02 00:00:00) 3",
		"test tstrdt p0 a 1 (2, 2000-01-03 00:00:00) 2",
		"test tstrdt p0 a 1 (3, 2000-01-01 00:00:00) 3",
		"test tstrdt p1 a 1 (13, 2000-01-01 00:00:00) 2",
		"test tstrdt p1 a 1 (13, 2000-01-02 00:00:00) 3"))

	tk.MustQuery("show stats_buckets where table_name='tstrdt' and is_index=1").Check(testkit.Rows(
		"test tstrdt global a 1 0 6 2 (1, 2000-01-01 00:00:00) (2, 2000-01-03 00:00:00) 0",    // (2, 3) is popped into it
		"test tstrdt global a 1 1 11 2 (13, 2000-01-01 00:00:00) (13, 2000-01-01 00:00:00) 0", // (13, 1) is popped into it
		"test tstrdt p0 a 1 0 3 1 (1, 2000-01-01 00:00:00) (2, 2000-01-01 00:00:00) 0",
		"test tstrdt p0 a 1 1 4 1 (2, 2000-01-02 00:00:00) (2, 2000-01-02 00:00:00) 0",
		"test tstrdt p1 a 1 0 2 1 (11, 2000-01-01 00:00:00) (12, 2000-01-01 00:00:00) 0",
		"test tstrdt p1 a 1 1 3 1 (12, 2000-01-02 00:00:00) (12, 2000-01-02 00:00:00) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tstrdt' and is_index=1").Rows()
	c.Assert(rs[0][6].(string), Equals, "11") // g.ndv = p0.ndv + p1.ndv
	c.Assert(rs[1][6].(string), Equals, "6")
	c.Assert(rs[2][6].(string), Equals, "5")
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
	tk.MustExec("set @@session.tidb_analyze_version=1")
	tk.MustExec("analyze table t") // both p0 and p1 are in ver1
	c.Assert(len(tk.MustQuery("show stats_meta").Rows()), Equals, 2)

	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	err := tk.ExecToErr("analyze table t") // try to build global-stats on ver1
	c.Assert(err, IsNil)

	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	err = tk.ExecToErr("analyze table t partition p1") // only analyze p1 to let it in ver2 while p0 is in ver1
	c.Assert(err, IsNil)

	tk.MustExec("analyze table t") // both p0 and p1 are in ver2
	c.Assert(len(tk.MustQuery("show stats_meta").Rows()), Equals, 3)

	// If we already have global-stats, we can get the latest global-stats by analyzing the newly added partition.
	tk.MustExec("alter table t add partition (partition p2 values less than (30))")
	tk.MustExec("insert t values (13), (14), (22), (23)")
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table t partition p2") // it will success since p0 and p1 are both in ver2
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	do := s.do
	is := do.InfoSchema()
	h := do.StatsHandle()
	c.Assert(h.Update(is), IsNil)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	globalStats := h.GetTableStats(tableInfo)
	// global.count = p0.count(3) + p1.count(2) + p2.count(2)
	// We did not analyze partition p1, so the value here has not changed
	c.Assert(globalStats.Count, Equals, int64(7))

	tk.MustExec("analyze table t partition p1;")
	globalStats = h.GetTableStats(tableInfo)
	// global.count = p0.count(3) + p1.count(4) + p2.count(4)
	// The value of p1.Count is correct now.
	c.Assert(globalStats.Count, Equals, int64(9))
	c.Assert(globalStats.ModifyCount, Equals, int64(0))

	tk.MustExec("alter table t drop partition p2;")
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table t;")
	globalStats = h.GetTableStats(tableInfo)
	// global.count = p0.count(3) + p1.count(4)
	c.Assert(globalStats.Count, Equals, int64(7))
}

func (s *testSerialStatsSuite) TestDDLPartition4GlobalStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec(`create table t (a int) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (30),
		partition p3 values less than (40),
		partition p4 values less than (50),
		partition p5 values less than (60)
	)`)
	do := s.do
	is := do.InfoSchema()
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustExec("insert into t values (1), (2), (3), (4), (5), " +
		"(11), (21), (31), (41), (51)," +
		"(12), (22), (32), (42), (52);")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustExec("analyze table t")
	result := tk.MustQuery("show stats_meta where table_name = 't';").Rows()
	c.Assert(len(result), Equals, 7)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	globalStats := h.GetTableStats(tableInfo)
	c.Assert(globalStats.Count, Equals, int64(15))

	tk.MustExec("alter table t drop partition p3, p5;")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)
	c.Assert(h.Update(is), IsNil)
	result = tk.MustQuery("show stats_meta where table_name = 't';").Rows()
	c.Assert(len(result), Equals, 5)
	// The value of global.count will be updated automatically after we drop the table partition.
	globalStats = h.GetTableStats(tableInfo)
	c.Assert(globalStats.Count, Equals, int64(11))

	tk.MustExec("alter table t truncate partition p2, p4;")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)
	c.Assert(h.Update(is), IsNil)
	// The value of global.count will not be updated automatically when we truncate the table partition.
	// Because the partition-stats in the partition table which have been truncated has not been updated.
	globalStats = h.GetTableStats(tableInfo)
	c.Assert(globalStats.Count, Equals, int64(11))

	tk.MustExec("analyze table t;")
	result = tk.MustQuery("show stats_meta where table_name = 't';").Rows()
	// The truncate operation only delete the data from the partition p2 and p4. It will not delete the partition-stats.
	c.Assert(len(result), Equals, 5)
	// The result for the globalStats.count will be right now
	globalStats = h.GetTableStats(tableInfo)
	c.Assert(globalStats.Count, Equals, int64(7))
}

func (s *testStatsSuite) TestMergeGlobalTopN(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_analyze_version=2;")
	tk.MustExec("set @@session.tidb_partition_prune_mode='dynamic';")
	tk.MustExec(`create table t (a int, b int, key(b)) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	);`)
	tk.MustExec("insert into t values(1, 1), (1, 1), (1, 1), (1, 1), (2, 2), (2, 2), (3, 3), (3, 3), (3, 3), " +
		"(11, 11), (11, 11), (11, 11), (12, 12), (12, 12), (12, 12), (13, 3), (13, 3);")
	tk.MustExec("analyze table t with 2 topn;")
	// The top2 values in partition p0 are 1(count = 4) and 3(count = 3).
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'b' and partition_name = 'p0';").Check(testkit.Rows(
		("test t p0 b 0 1 4"),
		("test t p0 b 0 3 3"),
		("test t p0 b 1 1 4"),
		("test t p0 b 1 3 3")))
	// The top2 values in partition p1 are 11(count = 3) and 12(count = 3).
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'b' and partition_name = 'p1';").Check(testkit.Rows(
		("test t p1 b 0 11 3"),
		("test t p1 b 0 12 3"),
		("test t p1 b 1 11 3"),
		("test t p1 b 1 12 3")))
	// The top2 values in global are 1(count = 4) and 3(count = 5).
	// Notice: The value 3 does not appear in the topN structure of partition one.
	// But we can still use the histogram to calculate its accurate value.
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'b' and partition_name = 'global';").Check(testkit.Rows(
		("test t global b 0 1 4"),
		("test t global b 0 3 5"),
		("test t global b 1 1 4"),
		("test t global b 1 3 5")))
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
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustExec("update mysql.stats_extended set status = 1 where name = 's1'")
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustExec("alter table t drop stats_extended s1")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"2 [2,3] <nil> 2",
	))
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)
}

func (s *testStatsSuite) TestAdminReloadStatistics1(c *C) {
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
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustExec("update mysql.stats_extended set status = 1 where name = 's1'")
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustExec("delete from mysql.stats_extended where name = 's1'")
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustExec("admin reload stats_extended")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)
}

func (s *testStatsSuite) TestAdminReloadStatistics2(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"1.000000 1",
	))
	rows := tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)

	tk.MustExec("delete from mysql.stats_extended where name = 's1'")
	is := s.do.InfoSchema()
	s.do.StatsHandle().Update(is)
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows())
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)

	tk.MustExec("admin reload stats_extended")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows())
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 0)
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
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] -1.000000 1",
	))
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] -1.000000 1",
	))
	err = do.StatsHandle().Update(is)
	c.Assert(err, IsNil)
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

	// Check that table with NULLs won't cause panic
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(1,null,2), (2,null,null)")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 0.000000 1",
		"2 [1,3] 1.000000 1",
	))
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 0.000000 1",
		"2 [1,3] 1.000000 1",
	))
	tk.MustExec("insert into t values(3,3,3)")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] 1.000000 1",
	))
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] 1.000000 1",
	))
}

func (s *testStatsSuite) TestSyncStatsExtendedRemoval(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)
	item := statsTbl.ExtendedStats.Stats["s1"]
	c.Assert(item, NotNil)
	result := tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 1)

	tk.MustExec("alter table t drop stats_extended s1")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 0)
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
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][6], Equals, "6")
}

func (s *testStatsSuite) TestPartitionPruneModeSessionVariable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk1.MustExec(`set @@tidb_analyze_version=2`)

	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")
	tk2.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Static) + "'")
	tk2.MustExec(`set @@tidb_analyze_version=2`)

	tk1.MustExec(`create table t (a int, key(a)) partition by range(a)
					(partition p0 values less than (10),
					partition p1 values less than (22))`)

	tk1.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"TableReader 10000.00 root partition:all data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk2.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"PartitionUnion 20000.00 root  ",
		"├─TableReader 10000.00 root  data:TableFullScan",
		"│ └─TableFullScan 10000.00 cop[tikv] table:t, partition:p0 keep order:false, stats:pseudo",
		"└─TableReader 10000.00 root  data:TableFullScan",
		"  └─TableFullScan 10000.00 cop[tikv] table:t, partition:p1 keep order:false, stats:pseudo",
	))

	tk1.MustExec(`insert into t values (1), (2), (3), (10), (11)`)
	tk1.MustExec(`analyze table t with 1 topn, 2 buckets`)
	tk1.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"TableReader 5.00 root partition:all data:TableFullScan",
		"└─TableFullScan 5.00 cop[tikv] table:t keep order:false",
	))
	tk2.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"PartitionUnion 5.00 root  ",
		"├─TableReader 3.00 root  data:TableFullScan",
		"│ └─TableFullScan 3.00 cop[tikv] table:t, partition:p0 keep order:false",
		"└─TableReader 2.00 root  data:TableFullScan",
		"  └─TableFullScan 2.00 cop[tikv] table:t, partition:p1 keep order:false",
	))

	tk1.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Static) + "'")
	tk1.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"PartitionUnion 5.00 root  ",
		"├─TableReader 3.00 root  data:TableFullScan",
		"│ └─TableFullScan 3.00 cop[tikv] table:t, partition:p0 keep order:false",
		"└─TableReader 2.00 root  data:TableFullScan",
		"  └─TableFullScan 2.00 cop[tikv] table:t, partition:p1 keep order:false",
	))
	tk2.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk2.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"TableReader 5.00 root partition:all data:TableFullScan",
		"└─TableFullScan 5.00 cop[tikv] table:t keep order:false",
	))
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
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0.",
		"Warning 8131 Build table: `t` global-level stats failed due to missing partition-level stats",
		"Warning 8131 Build table: `t` index: `a` global-level stats failed due to missing partition-level stats",
	))
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("2"))
}

var _ = SerialSuites(&statsSerialSuite{})

type statsSerialSuite struct {
	testSuiteBase
}

func (s *statsSerialSuite) TestIndexUsageInformation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	session.SetIndexUsageSyncLease(1)
	defer session.SetIndexUsageSyncLease(0)
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
	session.SetIndexUsageSyncLease(1)
	defer session.SetIndexUsageSyncLease(0)
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

func (s *statsSerialSuite) TestFeedbackWithGlobalStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@tidb_analyze_version = 1")

	oriProbability := statistics.FeedbackProbability.Load()
	oriNumber := statistics.MaxNumberOfRanges
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		statistics.MaxNumberOfRanges = oriNumber
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	// Case 1: You can't set tidb_analyze_version to 2 if feedback is enabled.
	// Note: if we want to set @@tidb_partition_prune_mode = 'dynamic'. We must set tidb_analyze_version to 2 first. We have already tested this.
	statistics.FeedbackProbability.Store(1)
	testKit.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("1"))
	testKit.MustExec("set @@tidb_analyze_version = 2")
	testKit.MustQuery("show warnings").Check(testkit.Rows(`Error 1105 variable tidb_analyze_version not updated because analyze version 2 is incompatible with query feedback. Please consider setting feedback-probability to 0.0 in config file to disable query feedback`))
	testKit.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("1"))

	h := s.do.StatsHandle()
	var err error
	// checkFeedbackOnPartitionTable is used to check whether the statistics are the same as before.
	checkFeedbackOnPartitionTable := func(statsBefore *statistics.Table, tblInfo *model.TableInfo) {
		h.UpdateStatsByLocalFeedback(s.do.InfoSchema())
		err = h.DumpStatsFeedbackToKV()
		c.Assert(err, IsNil)
		err = h.HandleUpdateStats(s.do.InfoSchema())
		c.Assert(err, IsNil)
		statsTblAfter := h.GetTableStats(tblInfo)
		// assert that statistics not changed
		// the feedback can not work for the partition table in both static and dynamic mode
		assertTableEqual(c, statsBefore, statsTblAfter)
	}

	// Case 2: Feedback wouldn't be applied on version 2 and global-level statistics.
	statistics.FeedbackProbability.Store(0)
	testKit.MustExec("set @@tidb_analyze_version = 2")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	testKit.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("2"))
	testKit.MustExec("create table t (a bigint(64), b bigint(64), index idx(b)) PARTITION BY HASH(a) PARTITIONS 2;")
	for i := 0; i < 200; i++ {
		testKit.MustExec("insert into t values (1,2),(2,2),(4,5),(2,3),(3,4)")
	}
	testKit.MustExec("analyze table t with 0 topn")
	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	testKit.MustExec("analyze table t")
	err = h.Update(s.do.InfoSchema())
	c.Assert(err, IsNil)
	statsTblBefore := h.GetTableStats(tblInfo)
	statistics.FeedbackProbability.Store(1)
	// make the statistics inaccurate.
	for i := 0; i < 200; i++ {
		testKit.MustExec("insert into t values (3,4), (3,4), (3,4), (3,4), (3,4)")
	}
	// trigger feedback
	testKit.MustExec("select b from t partition(p0) use index(idx) where t.b <= 3;")
	testKit.MustExec("select b from t partition(p1) use index(idx) where t.b <= 3;")
	testKit.MustExec("select b from t use index(idx) where t.b <= 3 order by b;")
	testKit.MustExec("select b from t use index(idx) where t.b <= 3;")
	checkFeedbackOnPartitionTable(statsTblBefore, tblInfo)

	// Case 3: Feedback is also not effective on version 1 and partition-level statistics.
	testKit.MustExec("set tidb_analyze_version = 1")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static';")
	testKit.MustExec("create table t1 (a bigint(64), b bigint(64), index idx(b)) PARTITION BY HASH(a) PARTITIONS 2")
	for i := 0; i < 200; i++ {
		testKit.MustExec("insert into t1 values (1,2),(2,2),(4,5),(2,3),(3,4)")
	}
	testKit.MustExec("analyze table t1 with 0 topn")
	// make the statistics inaccurate.
	for i := 0; i < 200; i++ {
		testKit.MustExec("insert into t1 values (3,4), (3,4), (3,4), (3,4), (3,4)")
	}
	is = s.do.InfoSchema()
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tblInfo = table.Meta()
	statsTblBefore = h.GetTableStats(tblInfo)
	// trigger feedback
	testKit.MustExec("select b from t1 partition(p0) use index(idx) where t1.b <= 3;")
	testKit.MustExec("select b from t1 partition(p1) use index(idx) where t1.b <= 3;")
	testKit.MustExec("select b from t1 use index(idx) where t1.b <= 3 order by b;")
	testKit.MustExec("select b from t1 use index(idx) where t1.b <= 3;")
	checkFeedbackOnPartitionTable(statsTblBefore, tblInfo)
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

func (s *testStatsSuite) TestHideIndexUsageSyncLease(c *C) {
	// NOTICE: remove this test when index usage is GA.
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	rs := tk.MustQuery("select @@tidb_config").Rows()
	for _, r := range rs {
		c.Assert(strings.Contains(strings.ToLower(r[0].(string)), "index-usage-sync-lease"), IsFalse)
	}
}

func (s *testStatsSuite) TestHideExtendedStatsSwitch(c *C) {
	// NOTICE: remove this test when this extended-stats reaches GA state.
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	rs := tk.MustQuery("show variables").Rows()
	for _, r := range rs {
		c.Assert(strings.ToLower(r[0].(string)), Not(Equals), "tidb_enable_extended_stats")
	}
	tk.MustQuery("show variables like 'tidb_enable_extended_stats'").Check(testkit.Rows())
}

func (s *testStatsSuite) TestRepetitiveAddDropExtendedStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 0",
	))
	result := tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 0)
	tk.MustExec("analyze table t")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 1",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 1)
	tk.MustExec("alter table t drop stats_extended s1")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 2",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 0)
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 0",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 0)
	tk.MustExec("analyze table t")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 1",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 1)
}

func (s *testStatsSuite) TestDuplicateExtendedStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	err := tk.ExecToErr("alter table t add stats_extended s1 correlation(a,a)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Cannot create extended statistics on duplicate column names 'a'")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	err = tk.ExecToErr("alter table t add stats_extended s1 correlation(a,c)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "extended statistics 's1' for the specified table already exists")
	err = tk.ExecToErr("alter table t add stats_extended s2 correlation(a,b)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "extended statistics 's2' with same type on same columns already exists")
	err = tk.ExecToErr("alter table t add stats_extended s2 correlation(b,a)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "extended statistics 's2' with same type on same columns already exists")
	tk.MustExec("alter table t add stats_extended s2 correlation(a,c)")
}

func (s *testStatsSuite) TestDuplicateFMSketch(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	defer tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("create table t(a int, b int, c int) partition by hash(a) partitions 3")
	tk.MustExec("insert into t values (1, 1, 1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("9"))
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("9"))

	tk.MustExec("alter table t drop column b")
	c.Assert(s.do.StatsHandle().GCStats(s.do.InfoSchema(), time.Duration(0)), IsNil)
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("6"))
}

func (s *testStatsSuite) TestIndexFMSketch(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index ia(a), index ibc(b, c)) partition by hash(a) partitions 3")
	tk.MustExec("insert into t values (1, 1, 1)")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	defer tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("analyze table t index ia")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("3"))
	tk.MustExec("analyze table t index ibc")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("6"))
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("15"))
	tk.MustExec("drop table if exists t")
	c.Assert(s.do.StatsHandle().GCStats(s.do.InfoSchema(), 0), IsNil)

	// clustered index
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_enable_clustered_index=ON")
	tk.MustExec("create table t (a datetime, b datetime, primary key (a)) partition by hash(year(a)) partitions 3")
	tk.MustExec("insert into t values ('2000-01-01', '2000-01-01')")
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("6"))
	tk.MustExec("drop table if exists t")
	c.Assert(s.do.StatsHandle().GCStats(s.do.InfoSchema(), 0), IsNil)

	// test NDV
	checkNDV := func(rows, ndv int) {
		tk.MustExec("analyze table t")
		rs := tk.MustQuery("select value from mysql.stats_fm_sketch").Rows()
		c.Assert(len(rs), Equals, rows)
		for i := range rs {
			fm, err := statistics.DecodeFMSketch([]byte(rs[i][0].(string)))
			c.Assert(err, IsNil)
			c.Assert(fm.NDV(), Equals, int64(ndv))
		}
	}

	tk.MustExec("set @@tidb_enable_clustered_index=OFF")
	tk.MustExec("create table t(a int, key(a)) partition by hash(a) partitions 3")
	tk.MustExec("insert into t values (1), (2), (2), (3)")
	checkNDV(6, 1)
	tk.MustExec("insert into t values (4), (5), (6)")
	checkNDV(6, 2)
	tk.MustExec("insert into t values (2), (5)")
	checkNDV(6, 2)
	tk.MustExec("drop table if exists t")
	c.Assert(s.do.StatsHandle().GCStats(s.do.InfoSchema(), 0), IsNil)

	// clustered index
	tk.MustExec("set @@tidb_enable_clustered_index=ON")
	tk.MustExec("create table t (a datetime, b datetime, primary key (a)) partition by hash(year(a)) partitions 3")
	tk.MustExec("insert into t values ('2000-01-01', '2001-01-01'), ('2001-01-01', '2001-01-01'), ('2002-01-01', '2001-01-01')")
	checkNDV(6, 1)
	tk.MustExec("insert into t values ('1999-01-01', '1998-01-01'), ('1997-01-02', '1999-01-02'), ('1998-01-03', '1999-01-03')")
	checkNDV(6, 2)
}

func (s *testStatsSuite) TestShowExtendedStats4DropColumn(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("insert into t values(1,1,1),(2,2,2),(3,3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("alter table t add stats_extended s2 correlation(b,c)")
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("show stats_extended").Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[0][2], Equals, "s1")
	c.Assert(rows[0][3], Equals, "[a,b]")
	c.Assert(rows[1][2], Equals, "s2")
	c.Assert(rows[1][3], Equals, "[b,c]")

	tk.MustExec("alter table t drop column b")
	rows = tk.MustQuery("show stats_extended").Rows()
	c.Assert(len(rows), Equals, 0)

	// Previously registered extended stats should be invalid for re-created columns.
	tk.MustExec("alter table t add column b int")
	rows = tk.MustQuery("show stats_extended").Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testStatsSuite) TestGlobalStatsNDV(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec(`CREATE TABLE t ( a int, key(a) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20),
    PARTITION p2 VALUES LESS THAN (30),
    PARTITION p3 VALUES LESS THAN (40))`)

	checkNDV := func(ndvs ...int) { // g, p0, ..., p3
		tk.MustExec("analyze table t")
		rs := tk.MustQuery(`show stats_histograms where is_index=1`).Rows()
		c.Assert(len(rs), Equals, 5)
		for i, ndv := range ndvs {
			c.Assert(rs[i][6].(string), Equals, fmt.Sprintf("%v", ndv))
		}
	}

	// all partitions are empty
	checkNDV(0, 0, 0, 0, 0)

	// p0 has data while others are empty
	tk.MustExec("insert into t values (1), (2), (3)")
	checkNDV(3, 3, 0, 0, 0)

	// p0, p1, p2 have data while p3 is empty
	tk.MustExec("insert into t values (11), (12), (13), (21), (22), (23)")
	checkNDV(9, 3, 3, 3, 0)

	// all partitions are not empty
	tk.MustExec("insert into t values (31), (32), (33), (34)")
	checkNDV(13, 3, 3, 3, 4)

	// insert some duplicated records
	tk.MustExec("insert into t values (31), (33), (34)")
	tk.MustExec("insert into t values (1), (2), (3)")
	checkNDV(13, 3, 3, 3, 4)
}

func (s *testStatsSuite) TestGlobalStatsIndexNDV(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	checkNDV := func(tbl string, g int, ps ...int) { // g, p0, ..., p3
		tk.MustExec("analyze table " + tbl)
		rs := tk.MustQuery(fmt.Sprintf(`show stats_histograms where is_index=1 and table_name='%v'`, tbl)).Rows()
		c.Assert(len(rs), Equals, 1+len(ps))                      // 1(global) + number of partitions
		c.Assert(rs[0][6].(string), Equals, fmt.Sprintf("%v", g)) // global
		for i, ndv := range ps {
			c.Assert(rs[i+1][6].(string), Equals, fmt.Sprintf("%v", ndv))
		}
	}

	// int
	tk.MustExec("drop table if exists tint")
	tk.MustExec(`CREATE TABLE tint ( a int, b int, key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tint values (1, 1), (1, 2), (1, 3)") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tint", 3, 3, 0)
	tk.MustExec("insert into tint values (11, 1), (11, 2), (11, 3)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tint", 3, 3, 3)
	tk.MustExec("insert into tint values (11, 4), (11, 5), (11, 6)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tint", 6, 3, 6)
	tk.MustExec("insert into tint values (1, 4), (1, 5), (1, 6), (1, 7), (1, 8)") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tint", 8, 8, 6)

	// double
	tk.MustExec("drop table if exists tdouble")
	tk.MustExec(`CREATE TABLE tdouble ( a int, b double, key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tdouble values (1, 1.1), (1, 2.2), (1, 3.3)") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tdouble", 3, 3, 0)
	tk.MustExec("insert into tdouble values (11, 1.1), (11, 2.2), (11, 3.3)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tdouble", 3, 3, 3)
	tk.MustExec("insert into tdouble values (11, 4.4), (11, 5.5), (11, 6.6)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdouble", 6, 3, 6)
	tk.MustExec("insert into tdouble values (1, 4.4), (1, 5.5), (1, 6.6), (1, 7.7), (1, 8.8)") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdouble", 8, 8, 6)

	// decimal
	tk.MustExec("drop table if exists tdecimal")
	tk.MustExec(`CREATE TABLE tdecimal ( a int, b decimal(30, 15), key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tdecimal values (1, 1.1), (1, 2.2), (1, 3.3)") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tdecimal", 3, 3, 0)
	tk.MustExec("insert into tdecimal values (11, 1.1), (11, 2.2), (11, 3.3)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tdecimal", 3, 3, 3)
	tk.MustExec("insert into tdecimal values (11, 4.4), (11, 5.5), (11, 6.6)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdecimal", 6, 3, 6)
	tk.MustExec("insert into tdecimal values (1, 4.4), (1, 5.5), (1, 6.6), (1, 7.7), (1, 8.8)") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdecimal", 8, 8, 6)

	// string
	tk.MustExec("drop table if exists tstring")
	tk.MustExec(`CREATE TABLE tstring ( a int, b varchar(30), key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tstring values (1, '111'), (1, '222'), (1, '333')") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tstring", 3, 3, 0)
	tk.MustExec("insert into tstring values (11, '111'), (11, '222'), (11, '333')") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tstring", 3, 3, 3)
	tk.MustExec("insert into tstring values (11, '444'), (11, '555'), (11, '666')") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tstring", 6, 3, 6)
	tk.MustExec("insert into tstring values (1, '444'), (1, '555'), (1, '666'), (1, '777'), (1, '888')") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tstring", 8, 8, 6)

	// datetime
	tk.MustExec("drop table if exists tdatetime")
	tk.MustExec(`CREATE TABLE tdatetime ( a int, b datetime, key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tdatetime values (1, '2001-01-01'), (1, '2002-01-01'), (1, '2003-01-01')") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tdatetime", 3, 3, 0)
	tk.MustExec("insert into tdatetime values (11, '2001-01-01'), (11, '2002-01-01'), (11, '2003-01-01')") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tdatetime", 3, 3, 3)
	tk.MustExec("insert into tdatetime values (11, '2004-01-01'), (11, '2005-01-01'), (11, '2006-01-01')") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdatetime", 6, 3, 6)
	tk.MustExec("insert into tdatetime values (1, '2004-01-01'), (1, '2005-01-01'), (1, '2006-01-01'), (1, '2007-01-01'), (1, '2008-01-01')") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdatetime", 8, 8, 6)
}

func (s *testStatsSuite) TestExtStatsOnReCreatedTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("select table_id, stats from mysql.stats_extended where name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)
	tableID1 := rows[0][0]
	c.Assert(rows[0][1], Equals, "1.000000")
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][5], Equals, "1.000000")

	tk.MustExec("drop table t")
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 0)

	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,3),(2,2),(3,1)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	rows = tk.MustQuery("select table_id, stats from mysql.stats_extended where name = 's1' order by stats").Rows()
	c.Assert(len(rows), Equals, 2)
	tableID2 := rows[0][0]
	c.Assert(tableID2, Not(Equals), tableID1)
	c.Assert(rows[1][0], Equals, tableID1)
	c.Assert(rows[0][1], Equals, "-1.000000")
	c.Assert(rows[1][1], Equals, "1.000000")
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][5], Equals, "-1.000000")
}

func (s *testStatsSuite) TestExtStatsOnReCreatedColumn(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] 1.000000",
	))
	rows := tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][3], Equals, "[a,b]")
	c.Assert(rows[0][5], Equals, "1.000000")

	tk.MustExec("alter table t drop column b")
	tk.MustExec("alter table t add column b int")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 <nil>",
		"2 <nil>",
		"3 <nil>",
	))
	tk.MustExec("update t set b = 3 where a = 1")
	tk.MustExec("update t set b = 2 where a = 2")
	tk.MustExec("update t set b = 1 where a = 3")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 3",
		"2 2",
		"3 1",
	))
	tk.MustExec("analyze table t")
	// Previous extended stats would not be collected and would not take effect anymore, it will be removed by stats GC.
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] 1.000000",
	))
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testStatsSuite) TestExtStatsOnRenamedColumn(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] 1.000000",
	))
	rows := tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][3], Equals, "[a,b]")
	c.Assert(rows[0][5], Equals, "1.000000")

	tk.MustExec("alter table t rename column b to c")
	tk.MustExec("update t set c = 3 where a = 1")
	tk.MustExec("update t set c = 2 where a = 2")
	tk.MustExec("update t set c = 1 where a = 3")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 3",
		"2 2",
		"3 1",
	))
	tk.MustExec("analyze table t")
	// Previous extended stats would still be collected and take effect.
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] -1.000000",
	))
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][3], Equals, "[a,c]")
	c.Assert(rows[0][5], Equals, "-1.000000")
}

func (s *testStatsSuite) TestExtStatsOnModifiedColumn(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] 1.000000",
	))
	rows := tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][3], Equals, "[a,b]")
	c.Assert(rows[0][5], Equals, "1.000000")

	tk.MustExec("alter table t modify column b bigint")
	tk.MustExec("update t set b = 3 where a = 1")
	tk.MustExec("update t set b = 2 where a = 2")
	tk.MustExec("update t set b = 1 where a = 3")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 3",
		"2 2",
		"3 1",
	))
	tk.MustExec("analyze table t")
	// Previous extended stats would still be collected and take effect.
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] -1.000000",
	))
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][3], Equals, "[a,b]")
	c.Assert(rows[0][5], Equals, "-1.000000")
}

func (s *testSerialStatsSuite) TestCorrelationWithDefinedCollate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int primary key, b varchar(8) character set utf8mb4 collate utf8mb4_general_ci, c varchar(8) character set utf8mb4 collate utf8mb4_bin)")
	testKit.MustExec("insert into t values(1,'aa','aa'),(2,'Cb','Cb'),(3,'CC','CC')")
	testKit.MustExec("analyze table t")
	testKit.MustQuery("select a from t order by b").Check(testkit.Rows(
		"1",
		"2",
		"3",
	))
	testKit.MustQuery("select a from t order by c").Check(testkit.Rows(
		"3",
		"2",
		"1",
	))
	rows := testKit.MustQuery("show stats_histograms where table_name = 't'").Sort().Rows()
	c.Assert(len(rows), Equals, 3)
	c.Assert(rows[1][9], Equals, "1")
	c.Assert(rows[2][9], Equals, "-1")
	testKit.MustExec("set session tidb_enable_extended_stats = on")
	testKit.MustExec("alter table t add stats_extended s1 correlation(b,c)")
	testKit.MustExec("analyze table t")
	rows = testKit.MustQuery("show stats_extended where stats_name = 's1'").Sort().Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][3], Equals, "[b,c]")
	c.Assert(rows[0][5], Equals, "-1.000000")
}

func (s *testSerialStatsSuite) TestLoadHistogramWithCollate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a varchar(10) collate utf8mb4_unicode_ci);")
	testKit.MustExec("insert into t values('abcdefghij');")
	testKit.MustExec("insert into t values('abcdufghij');")
	testKit.MustExec("analyze table t with 0 topn;")
	do := s.do
	h := do.StatsHandle()
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	_, err = h.TableStatsFromStorage(tblInfo, tblInfo.ID, true, 0)
	c.Assert(err, IsNil)
}

func (s *testSerialStatsSuite) TestFastAnalyzeColumnHistWithNullValue(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int)")
	testKit.MustExec("insert into t values (1), (2), (3), (4), (NULL)")
	testKit.MustExec("set @@session.tidb_analyze_version = 1")
	testKit.MustExec("set @@tidb_enable_fast_analyze=1")
	defer testKit.MustExec("set @@tidb_enable_fast_analyze=0")
	testKit.MustExec("analyze table t with 0 topn, 2 buckets")
	// If NULL is in hist, the min(lower_bound) will be "".
	testKit.MustQuery("select min(lower_bound) from mysql.stats_buckets").Check(testkit.Rows("1"))
}

func (s *testStatsSuite) TestStatsCacheUpdateSkip(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	do := s.do
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl1 := h.GetTableStats(tableInfo)
	c.Assert(statsTbl1.Pseudo, IsFalse)
	h.Update(is)
	statsTbl2 := h.GetTableStats(tableInfo)
	c.Assert(statsTbl1, Equals, statsTbl2)
}

func (s *testSerialStatsSuite) TestIssues24349(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	testKit.MustExec("set @@tidb_analyze_version=2")
	defer testKit.MustExec("set @@tidb_analyze_version=1")
	defer testKit.MustExec("set @@tidb_partition_prune_mode='static'")
	testKit.MustExec("create table t (a int, b int) partition by hash(a) partitions 3")
	testKit.MustExec("insert into t values (0, 3), (0, 3), (0, 3), (0, 2), (1, 1), (1, 2), (1, 2), (1, 2), (1, 3), (1, 4), (2, 1), (2, 1)")
	testKit.MustExec("analyze table t with 1 topn, 3 buckets")
	testKit.MustQuery("show stats_buckets where partition_name='global'").Check(testkit.Rows(
		"test t global a 0 0 2 2 0 2 0",
		"test t global b 0 0 3 1 1 2 0",
		"test t global b 0 1 10 1 4 4 0",
	))
}

func (s *testStatsSuite) TestIssues24401(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")

	// normal table with static prune mode
	testKit.MustExec("set @@tidb_partition_prune_mode='static'")
	testKit.MustExec("create table t(a int, index(a))")
	testKit.MustExec("insert into t values (1), (2), (3)")
	testKit.MustExec("analyze table t")
	testKit.MustQuery("select * from mysql.stats_fm_sketch").Check(testkit.Rows())

	// partition table with static prune mode
	testKit.MustExec("create table tp(a int, index(a)) partition by hash(a) partitions 3")
	testKit.MustExec("insert into tp values (1), (2), (3)")
	testKit.MustExec("analyze table tp")
	testKit.MustQuery("select * from mysql.stats_fm_sketch").Check(testkit.Rows())

	// normal table with dynamic prune mode
	testKit.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	defer testKit.MustExec("set @@tidb_partition_prune_mode='static'")
	testKit.MustExec("analyze table t")
	testKit.MustQuery("select * from mysql.stats_fm_sketch").Check(testkit.Rows())

	// partition table with dynamic prune mode
	testKit.MustExec("analyze table tp")
	rows := testKit.MustQuery("select * from mysql.stats_fm_sketch").Rows()
	lenRows := len(rows)
	c.Assert(lenRows, Equals, 6)

	// check fm-sketch won't increase infinitely
	testKit.MustExec("insert into t values (10), (20), (30), (12), (23), (23), (4344)")
	testKit.MustExec("analyze table tp")
	rows = testKit.MustQuery("select * from mysql.stats_fm_sketch").Rows()
	c.Assert(len(rows), Equals, lenRows)
}

func (s *testStatsSuite) TestIssues27147(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")

	testKit.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int, b int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than maxvalue);")
	testKit.MustExec("alter table t add index idx((a+5));")
	err := testKit.ExecToErr("analyze table t;")
	c.Assert(err, Equals, nil)

	testKit.MustExec("drop table if exists t1")
	testKit.MustExec("create table t1 (a int, b int as (a+1) virtual, c int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than maxvalue);")
	testKit.MustExec("alter table t1 add index idx((a+5));")
	err = testKit.ExecToErr("analyze table t1;")
	c.Assert(err, Equals, nil)
}

func (s *testStatsSuite) TestColumnCountFromStorage(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	do := s.do
	h := do.StatsHandle()
	originLease := h.Lease()
	defer h.SetLease(originLease)
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)
	testKit.MustExec("use test")
	testKit.MustExec("set tidb_analyze_version = 2")
	testKit.MustExec("create table tt (c int)")
	testKit.MustExec("insert into tt values(1), (2)")
	testKit.MustExec("analyze table tt")
	is := do.InfoSchema()
	h = do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tt"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	h.TableStatsFromStorage(tblInfo, tblInfo.ID, false, 0)
	statsTbl := h.GetTableStats(tblInfo)
	c.Assert(statsTbl.Columns[tblInfo.Columns[0].ID].Count, Equals, int64(2))
}

func (s *testStatsSuite) TestIncrementalModifyCountUpdate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	h := s.do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	tbl, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID

	tk.MustExec("insert into t values(1),(2),(3)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	err = h.Update(s.do.InfoSchema())
	c.Assert(err, IsNil)
	tk.MustExec("analyze table t")
	tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tid)).Check(testkit.Rows(
		"3 0",
	))

	tk.MustExec("begin")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	startTS := txn.StartTS()
	tk.MustExec("commit")

	tk.MustExec("insert into t values(4),(5),(6)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	err = h.Update(s.do.InfoSchema())
	c.Assert(err, IsNil)

	// Simulate that the analyze would start before and finish after the second insert.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS)), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/injectBaseCount", "return(3)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/injectBaseModifyCount", "return(0)"), IsNil)
	tk.MustExec("analyze table t")
	// Check the count / modify_count changes during the analyze are not lost.
	tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tid)).Check(testkit.Rows(
		"6 3",
	))
	// Check the histogram is correct for the snapshot analyze.
	tk.MustQuery(fmt.Sprintf("select distinct_count from mysql.stats_histograms where table_id = %d", tid)).Check(testkit.Rows(
		"3",
	))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/injectAnalyzeSnapshot"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/injectBaseCount"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/injectBaseModifyCount"), IsNil)
}

func (s *testStatsSuite) TestRecordHistoricalStatsToStorage(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")
	tk.MustExec("insert into t value(1, 'aaa'), (3, 'aab'), (5, 'bba'), (2, 'bbb'), (4, 'cca'), (6, 'ccc')")
	// mark column stats as needed
	tk.MustExec("select * from t where a = 3")
	tk.MustExec("select * from t where b = 'bbb'")
	tk.MustExec("alter table t add index single(a)")
	tk.MustExec("alter table t add index multi(a, b)")
	tk.MustExec("analyze table t with 2 topn")

	tableInfo, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(c, err)
	version, err := s.do.StatsHandle().RecordHistoricalStatsToStorage("t", tableInfo.Meta())
	require.NoError(c, err)

	rows := tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where version = '%d'", version)).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.GreaterOrEqual(c, num, 1)
}
