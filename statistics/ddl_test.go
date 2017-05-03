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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
)

func (s *testStatsCacheSuite) TestDDLAfterLoad(c *C) {
	store, do, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("analyze table t")
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

	sc := new(variable.StatementContext)
	count, err := statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(recordCount+1), tableInfo.Columns[0])
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.0)
	count, err = statsTbl.ColumnGreaterRowCount(sc, types.NewDatum(recordCount+1), tableInfo.Columns[2])
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 333)
}

func (s *testStatsCacheSuite) TestDDLTable(c *C) {
	store, do, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
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

	testKit.MustExec("drop table t")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	// Update it with old schema.
	h.Update(is)
	statsTbl = h.GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsTrue)

	testKit.MustExec("create table t1 (c1 int, c2 int)")
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	h.Update(is)
	statsTbl = h.GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustExec("drop table t1")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	// Update it with new schema.
	h.Update(do.InfoSchema())
	statsTbl = h.GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsTrue)
}

func (s *testStatsCacheSuite) TestDDLHistogram(c *C) {
	store, do, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1,2),(3,4)")
	testKit.MustExec("analyze table t")
	h := do.StatsHandle()

	testKit.MustExec("alter table t add column c3 int NOT NULL")
	<-h.DDLEventCh()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	is := do.InfoSchema()
	h.Update(is)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
	sc := new(variable.StatementContext)
	count, err := statsTbl.ColumnEqualRowCount(sc, types.NewIntDatum(0), tableInfo.Columns[2])
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(2))
	count, err = statsTbl.ColumnEqualRowCount(sc, types.NewIntDatum(1), tableInfo.Columns[2])
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

	rs := testKit.MustQuery("select count(*) from mysql.stats_histograms where table_id = ? and hist_id = 3 and is_index = 0", tableInfo.ID)
	rs.Check(testkit.Rows("1"))
	rs = testKit.MustQuery("select count(*) from mysql.stats_buckets where table_id = ? and hist_id = 3 and is_index = 0", tableInfo.ID)
	rs.Check(testkit.Rows("1"))
	testKit.MustExec("alter table t drop column c3")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	is = do.InfoSchema()
	h.Update(is)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo.ID)
	c.Assert(statsTbl.Pseudo, IsFalse)
	rs = testKit.MustQuery("select count(*) from mysql.stats_histograms where table_id = ? and hist_id = 3 and is_index = 0", tableInfo.ID)
	rs.Check(testkit.Rows("0"))
	rs = testKit.MustQuery("select count(*) from mysql.stats_buckets where table_id = ? and hist_id = 3 and is_index = 0", tableInfo.ID)
	rs.Check(testkit.Rows("0"))

	testKit.MustExec("create index i on t(c2, c1)")
	testKit.MustExec("analyze table t")
	rs = testKit.MustQuery("select count(*) from mysql.stats_histograms where table_id = ? and hist_id = 1 and is_index =1", tableInfo.ID)
	rs.Check(testkit.Rows("1"))
	rs = testKit.MustQuery("select count(*) from mysql.stats_buckets where table_id = ? and hist_id = 1 and is_index = 1", tableInfo.ID)
	rs.Check(testkit.Rows("2"))
	testKit.MustExec("drop index i on t")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	rs = testKit.MustQuery("select count(*) from mysql.stats_histograms where table_id = ? and hist_id = 1 and is_index =1", tableInfo.ID)
	rs.Check(testkit.Rows("0"))
	rs = testKit.MustQuery("select count(*) from mysql.stats_buckets where table_id = ? and hist_id = 1 and is_index = 1", tableInfo.ID)
	rs.Check(testkit.Rows("0"))
}
