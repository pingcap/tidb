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
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
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
	}
	c.Assert(len(a.Indices), Equals, len(b.Indices))
	for i := range a.Indices {
		c.Assert(statistics.HistogramEqual(&a.Indices[i].Histogram, &b.Indices[i].Histogram, false), IsTrue)
		if a.Columns[i].CMSketch == nil {
			c.Assert(b.Columns[i].CMSketch, IsNil)
		} else {
			c.Assert(a.Columns[i].CMSketch.Equal(b.Columns[i].CMSketch), IsTrue)
		}
	}
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
	testKit.MustExec("create table t (c1 int, c2 varchar(100), c3 float, c4 datetime)")
	testKit.MustExec("insert into t values(1, '1234567', 12.3, '2018-03-07 19:00:57')")
	testKit.MustExec("analyze table t")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Columns[tableInfo.Columns[0].ID].AvgColSize(statsTbl.Count, false), Equals, 1.0)

	// The size of varchar type is LEN + BYTE, here is 1 + 7 = 8
	c.Assert(statsTbl.Columns[tableInfo.Columns[1].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[3].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	testKit.MustExec("insert into t values(132, '123456789112', 1232.3, '2018-03-07 19:17:29')")
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	c.Assert(statsTbl.Columns[tableInfo.Columns[0].ID].AvgColSize(statsTbl.Count, false), Equals, 1.5)
	c.Assert(statsTbl.Columns[tableInfo.Columns[1].ID].AvgColSize(statsTbl.Count, false), Equals, 10.5)
	c.Assert(statsTbl.Columns[tableInfo.Columns[2].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.Columns[tableInfo.Columns[3].ID].AvgColSize(statsTbl.Count, false), Equals, 8.0)
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
	h := handle.NewHandle(testKit.Se, time.Millisecond)
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
	c.Assert(cms.TotalCount(), Greater, uint64(0))
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
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
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
	c.Assert(result.Rows()[1][9], Equals, "0.828571")

	testKit.MustExec("truncate table t")
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
	c.Assert(result.Rows()[1][9], Equals, "-0.942857")

	testKit.MustExec("truncate table t")
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
	c.Assert(result.Rows()[1][9], Equals, "0.828571")

	testKit.MustExec("truncate table t")
	testKit.MustExec("insert into t values(1,1),(2,7),(3,12),(8,18),(4,20),(5,21)")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0.828571")
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
