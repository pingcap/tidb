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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStatsCacheSuite) TestDDLAfterLoad(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("analyze table t")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		testKit.MustExec("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
	// add column
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()

	sc := new(stmtctx.StatementContext)
	count := statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(recordCount+1), tableInfo.Columns[0].ID)
	c.Assert(count, Equals, 0.0)
	count = statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(recordCount+1), tableInfo.Columns[2].ID)
	c.Assert(int(count), Equals, 333)
}

func (s *testStatsCacheSuite) TestDDLTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	h.Update(is)
	statsTbl := h.GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustExec("create table t1 (c1 int, c2 int, index idx(c1))")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	h.Update(is)
	statsTbl = h.GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
}

func (s *testStatsCacheSuite) TestDDLHistogram(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1,2),(3,4)")
	testKit.MustExec("analyze table t")
	do := s.do
	h := do.StatsHandle()

	testKit.MustExec("alter table t add column c_null int")
	<-h.DDLEventCh()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	is := do.InfoSchema()
	h.Update(is)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
	sc := new(stmtctx.StatementContext)
	c.Assert(statsTbl.ColumnIsInvalid(sc, tableInfo.Columns[2].ID), IsTrue)
	c.Check(statsTbl.Columns[tableInfo.Columns[2].ID].NDV, Equals, int64(0))

	testKit.MustExec("alter table t add column c3 int NOT NULL")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	is = do.InfoSchema()
	h.Update(is)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
	sc = new(stmtctx.StatementContext)
	count, err := statsTbl.ColumnEqualRowCount(sc, types.NewIntDatum(0), tableInfo.Columns[3].ID)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(2))
	count, err = statsTbl.ColumnEqualRowCount(sc, types.NewIntDatum(1), tableInfo.Columns[3].ID)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(0))

	testKit.MustExec("alter table t add column c4 datetime NOT NULL default CURRENT_TIMESTAMP")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	is = do.InfoSchema()
	h.Update(is)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	// If we don't use original default value, we will get a pseudo table.
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustExec("create index i on t(c2, c1)")
	testKit.MustExec("analyze table t")
	rs := testKit.MustQuery("select count(*) from mysql.stats_histograms where table_id = ? and hist_id = 1 and is_index =1", tableInfo.ID)
	rs.Check(testkit.Rows("1"))
	rs = testKit.MustQuery("select count(*) from mysql.stats_buckets where table_id = ? and hist_id = 1 and is_index = 1", tableInfo.ID)
	rs.Check(testkit.Rows("2"))
}
