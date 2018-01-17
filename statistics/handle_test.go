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

package statistics_test

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testStatsCacheSuite{})

type testStatsCacheSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (s *testStatsCacheSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	s.store, s.do, err = newStoreWithBootstrap(0)
	c.Assert(err, IsNil)
}

func (s *testStatsCacheSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func cleanEnv(c *C, store kv.Storage, do *domain.Domain) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
	do.StatsHandle().Clear()
	tk.MustExec("truncate table mysql.stats_meta")
	tk.MustExec("truncate table mysql.stats_histograms")
	tk.MustExec("truncate table mysql.stats_buckets")
}

func (s *testStatsCacheSuite) TestStatsCache(c *C) {
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
	statsTbl := do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsTrue)
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
	testKit.MustExec("create index idx_t on t(c1)")
	is = do.InfoSchema()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	// If index is build, but stats is not updated. statsTbl can also work.
	c.Assert(statsTbl.Pseudo, IsFalse)
	// But the added index will not work.
	c.Assert(statsTbl.Indices[int64(1)], IsNil)

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
	// If the new schema drop a column, the table stats can still work.
	testKit.MustExec("alter table t drop column c2")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
}

func assertTableEqual(c *C, a *statistics.Table, b *statistics.Table) {
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

func (s *testStatsCacheSuite) TestStatsStoreAndLoad(c *C) {
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
	statsTbl1 := do.StatsHandle().GetTableStats(tableInfo.ID)

	do.StatsHandle().Clear()
	do.StatsHandle().Update(is)
	statsTbl2 := do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	c.Assert(statsTbl2.Count, Equals, int64(recordCount))
	assertTableEqual(c, statsTbl1, statsTbl2)
}

func (s *testStatsCacheSuite) TestEmptyTable(c *C) {
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
	statsTbl := do.StatsHandle().GetTableStats(tableInfo.ID)
	sc := new(stmtctx.StatementContext)
	count := statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(1), tableInfo.Columns[0].ID)
	c.Assert(count, Equals, 0.0)
}

func (s *testStatsCacheSuite) TestColumnIDs(c *C) {
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
	statsTbl := do.StatsHandle().GetTableStats(tableInfo.ID)
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
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	// At that time, we should get c2's stats instead of c1's.
	count = statsTbl.ColumnLessRowCount(sc, types.NewDatum(2), tableInfo.Columns[0].ID)
	c.Assert(count, Equals, 0.0)
}

func (s *testStatsCacheSuite) TestVersion(c *C) {
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
	h := statistics.NewHandle(testKit.Se, 0)
	testKit.MustExec("update mysql.stats_meta set version = 2 where table_id = ?", tableInfo1.ID)

	h.Update(is)
	c.Assert(h.LastVersion, Equals, uint64(2))
	c.Assert(h.PrevLastVersion, Equals, uint64(0))
	statsTbl1 := h.GetTableStats(tableInfo1.ID)
	c.Assert(statsTbl1.Pseudo, IsFalse)

	testKit.MustExec("create table t2 (c1 int, c2 int)")
	testKit.MustExec("analyze table t2")
	is = do.InfoSchema()
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tableInfo2 := tbl2.Meta()
	// A smaller version write, and we can still read it.
	testKit.MustExec("update mysql.stats_meta set version = 1 where table_id = ?", tableInfo2.ID)
	h.Update(is)
	c.Assert(h.LastVersion, Equals, uint64(2))
	c.Assert(h.PrevLastVersion, Equals, uint64(2))
	statsTbl2 := h.GetTableStats(tableInfo2.ID)
	c.Assert(statsTbl2.Pseudo, IsFalse)

	testKit.MustExec("insert t1 values(1,2)")
	testKit.MustExec("analyze table t1")
	testKit.MustExec("update mysql.stats_meta set version = 4 where table_id = ?", tableInfo1.ID)
	h.Update(is)
	c.Assert(h.LastVersion, Equals, uint64(4))
	c.Assert(h.PrevLastVersion, Equals, uint64(2))
	statsTbl1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(statsTbl1.Count, Equals, int64(1))

	testKit.MustExec("insert t2 values(1,2)")
	testKit.MustExec("analyze table t2")
	// A smaller version write, and we can still read it.
	testKit.MustExec("update mysql.stats_meta set version = 3 where table_id = ?", tableInfo2.ID)
	h.Update(is)
	c.Assert(h.LastVersion, Equals, uint64(4))
	c.Assert(h.PrevLastVersion, Equals, uint64(4))
	statsTbl2 = h.GetTableStats(tableInfo2.ID)
	c.Assert(statsTbl2.Count, Equals, int64(1))

	testKit.MustExec("insert t2 values(1,2)")
	testKit.MustExec("analyze table t2")
	// A smaller version write, and we cannot read it. Because at this time, lastTwo Version is 4.
	testKit.MustExec("update mysql.stats_meta set version = 3 where table_id = ?", tableInfo2.ID)
	h.Update(is)
	c.Assert(h.LastVersion, Equals, uint64(4))
	c.Assert(h.PrevLastVersion, Equals, uint64(4))
	statsTbl2 = h.GetTableStats(tableInfo2.ID)
	c.Assert(statsTbl2.Count, Equals, int64(1))

	// We add an index and analyze it, but DDL doesn't load.
	testKit.MustExec("alter table t2 add column c3 int")
	testKit.MustExec("analyze table t2")
	// load it with old schema.
	h.Update(is)
	statsTbl2 = h.GetTableStats(tableInfo2.ID)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	c.Assert(statsTbl2.Columns[int64(3)], IsNil)
	// Next time DDL updated.
	is = do.InfoSchema()
	h.Update(is)
	statsTbl2 = h.GetTableStats(tableInfo2.ID)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	// We can read it without analyze again! Thanks for PrevLastVersion.
	c.Assert(statsTbl2.Columns[int64(3)], NotNil)
}

func (s *testStatsCacheSuite) TestLoadHist(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	do := s.do
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	rowCount := 10
	for i := 0; i < rowCount; i++ {
		testKit.MustExec("insert into t values(1,2)")
	}
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	oldStatsTbl := h.GetTableStats(tableInfo.ID)
	for i := 0; i < rowCount; i++ {
		testKit.MustExec("insert into t values(1,2)")
	}
	h.DumpStatsDeltaToKV()
	h.Update(do.InfoSchema())
	newStatsTbl := h.GetTableStats(tableInfo.ID)
	// The stats table is updated.
	c.Assert(oldStatsTbl == newStatsTbl, IsFalse)
	// The histograms is not updated.
	for id, hist := range oldStatsTbl.Columns {
		c.Assert(hist, Equals, newStatsTbl.Columns[id])
	}
	// Add column c3, we only update c3.
	testKit.MustExec("alter table t add column c3 int")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	h.Update(is)
	newStatsTbl2 := h.GetTableStats(tableInfo.ID)
	c.Assert(newStatsTbl2 == newStatsTbl, IsFalse)
	// The histograms is not updated.
	for id, hist := range newStatsTbl.Columns {
		c.Assert(hist, Equals, newStatsTbl2.Columns[id])
	}
	c.Assert(newStatsTbl2.Columns[int64(3)].LastUpdateVersion, Greater, newStatsTbl2.Columns[int64(1)].LastUpdateVersion)
}

func (s *testStatsCacheSuite) TestInitStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6)")
	testKit.MustExec("analyze table t")
	h := s.do.StatsHandle()
	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.Lease = time.Millisecond

	h.Clear()
	c.Assert(h.InitStats(is), IsNil)
	table0 := h.GetTableStats(tbl.Meta().ID)
	h.Clear()
	c.Assert(h.Update(is), IsNil)
	table1 := h.GetTableStats(tbl.Meta().ID)
	assertTableEqual(c, table0, table1)
	h.Lease = 0
}

func (s *testStatsUpdateSuite) TestLoadStats(c *C) {
	store, do, err := newStoreWithBootstrap(10 * time.Millisecond)
	c.Assert(err, IsNil)
	defer store.Close()
	defer do.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")
	testKit.MustExec("analyze table t")

	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	time.Sleep(1 * time.Second)
	stat := h.GetTableStats(tableInfo.ID)
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
	time.Sleep(1 * time.Second)
	stat = h.GetTableStats(tableInfo.ID)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	c.Assert(hg.Len(), Greater, 0)
}

func newStoreWithBootstrap(statsLease time.Duration) (kv.Storage, *domain.Domain, error) {
	store, err := tikv.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	tidb.SetSchemaLease(0)
	tidb.SetStatsLease(statsLease)
	domain.RunAutoAnalyze = false
	do, err := tidb.BootstrapSession(store)
	return store, do, errors.Trace(err)
}
