// Copyright 2020 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStatsSuite) TestStatsCacheMiniMemoryLimit(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("insert into t1 values(1, 2)")
	do := s.do
	is := do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()
	statsTbl1 := do.StatsHandle().GetTableStats(tableInfo1)
	c.Assert(statsTbl1.Pseudo, IsTrue)

	testKit.MustExec("analyze table t1")
	statsTbl1 = do.StatsHandle().GetTableStats(tableInfo1)
	c.Assert(statsTbl1.Pseudo, IsFalse)

	//set new BytesLimit
	BytesLimit := int64(90000)

	do.StatsHandle().SetBytesLimit(BytesLimit)
	//create t2 and kick t1 of cache
	testKit.MustExec("create table t2 (c1 int, c2 int)")
	testKit.MustExec("insert into t2 values(1, 2)")
	do = s.do
	is = do.InfoSchema()
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tableInfo2 := tbl2.Meta()
	statsTbl2 := do.StatsHandle().GetTableStats(tableInfo2)
	statsTbl1 = do.StatsHandle().GetTableStats(tableInfo1)

	c.Assert(statsTbl2.Pseudo, IsTrue)
	testKit.MustExec("analyze table t2")
	tbl2, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)

	statsTbl2 = do.StatsHandle().GetTableStats(tableInfo2)
	c.Assert(statsTbl2.Pseudo, IsFalse)

	c.Assert(BytesLimit >= do.StatsHandle().GetMemConsumed(), IsTrue)
}

func (s *testStatsSuite) TestLoadHistWithLimit(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	h := s.do.StatsHandle()
	origLease := h.Lease()
	h.SetLease(time.Second)
	defer func() { h.SetLease(origLease) }()
	BytesLimit := int64(300000)
	h.SetBytesLimit(BytesLimit)

	testKit.MustExec("use test")
	testKit.MustExec("create table t1(c int)")
	testKit.MustExec("insert into t1 values(1),(2),(3),(4),(5)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t1")
	h.Clear()
	h.SetBytesLimit(BytesLimit)

	c.Assert(h.Update(s.do.InfoSchema()), IsNil)
	result := testKit.MustQuery("show stats_histograms where Table_name = 't1'")
	c.Assert(len(result.Rows()), Equals, 0)
	testKit.MustExec("explain select * from t1 where c = 1")
	c.Assert(h.LoadNeededHistograms(), IsNil)
	result = testKit.MustQuery("show stats_histograms where Table_name = 't1'")
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(BytesLimit >= h.GetMemConsumed(), IsTrue)

	//create new table
	testKit.MustExec("create table t2(c int)")
	testKit.MustExec("insert into t2 values(1),(2),(3),(4),(5)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t2")
	c.Assert(BytesLimit >= h.GetMemConsumed(), IsTrue)

}

func (s *testStatsSuite) TestLoadHistWithInvalidIndex(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	h := s.do.StatsHandle()
	origLease := h.Lease()
	h.SetLease(time.Second)
	defer func() { h.SetLease(origLease) }()
	BytesLimit := int64(300000)
	h.SetBytesLimit(BytesLimit)

	testKit.MustExec("use test")
	testKit.MustExec("create table t1(c int)")
	testKit.MustExec("insert into t1 values(1),(2),(3),(4),(5)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("create index idx_t on t1(c)")

	testKit.MustExec("analyze table t1")
	// update all information to statscache
	c.Assert(h.Update(s.do.InfoSchema()), IsNil)

	tbl1, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()

	//erease old table
	h.EraseTable(tbl1.Meta().ID)

	//add empty table
	statsTbl1 := h.GetTableStats(tableInfo1)
	c.Assert(statsTbl1.Indices[tbl1.Meta().Indices[0].ID].Len() == 0, IsTrue)
	//load index
	for _, v := range statsTbl1.Indices {
		c.Assert(v.IsInvalid(&stmtctx.StatementContext{}, false), IsTrue)
	}
	for _, v := range statsTbl1.Columns {
		c.Assert(v.IsInvalid(&stmtctx.StatementContext{}, false), IsTrue)
	}
	c.Assert(h.LoadNeededHistograms(), IsNil)
	c.Assert(BytesLimit >= h.GetMemConsumed(), IsTrue)
	statsTbl1new := h.GetTableStats(tableInfo1)
	c.Assert(statsTbl1new.Indices[tbl1.Meta().Indices[0].ID].Len() > 0, IsTrue)

	c.Assert(statsTbl1new.Indices[tbl1.Meta().Indices[0].ID].String(), Equals, "index:1 ndv:5\n"+
		"num: 1 lower_bound: 1 upper_bound: 1 repeats: 1\n"+
		"num: 1 lower_bound: 2 upper_bound: 2 repeats: 1\n"+
		"num: 1 lower_bound: 3 upper_bound: 3 repeats: 1\n"+
		"num: 1 lower_bound: 4 upper_bound: 4 repeats: 1\n"+
		"num: 1 lower_bound: 5 upper_bound: 5 repeats: 1")
	c.Assert(statsTbl1new.Columns[tbl1.Meta().Columns[0].ID].String(), Equals, "column:1 ndv:5 totColSize:5\n"+
		"num: 1 lower_bound: 1 upper_bound: 1 repeats: 1\n"+
		"num: 1 lower_bound: 2 upper_bound: 2 repeats: 1\n"+
		"num: 1 lower_bound: 3 upper_bound: 3 repeats: 1\n"+
		"num: 1 lower_bound: 4 upper_bound: 4 repeats: 1\n"+
		"num: 1 lower_bound: 5 upper_bound: 5 repeats: 1")
}
func (s *testStatsSuite) TestManyTableChange(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	h := s.do.StatsHandle()
	origLease := h.Lease()
	h.SetLease(time.Second)
	defer func() { h.SetLease(origLease) }()

	BytesLimit := int64(300000)
	h.SetBytesLimit(BytesLimit)
	tableSize := 100
	testKit.MustExec("use test")
	for i := 0; i <= tableSize; i++ {
		testKit.MustExec(fmt.Sprintf("create table t%d(c int)", i))
		testKit.MustExec(fmt.Sprintf("insert into t%d values(1),(2),(3)", i))
		testKit.MustExec(fmt.Sprintf("analyze table t%d", i))
	}

	// update all information to statscache
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)

	c.Assert(h.Update(s.do.InfoSchema()), IsNil)
	for i := 0; i <= tableSize; i++ {
		tableName := fmt.Sprintf("t%d", i)
		tbl, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr(tableName))
		c.Assert(err, IsNil)
		tableInfo := tbl.Meta()

		//add empty table
		statsTbl := h.GetTableStats(tableInfo)

		//load indexs and column
		for _, v := range statsTbl.Indices {
			v.IsInvalid(&stmtctx.StatementContext{}, false)
		}

		for _, v := range statsTbl.Columns {
			v.IsInvalid(&stmtctx.StatementContext{}, false)
		}
		c.Assert(h.LoadNeededHistograms(), IsNil)
		c.Assert(BytesLimit >= h.GetMemConsumed(), IsTrue)
		statsTblnew := h.GetTableStats(tableInfo)
		c.Assert(statsTblnew.MemoryUsage() > 0, IsTrue)

		for _, v := range statsTblnew.Columns {
			c.Assert(v.IsInvalid(&stmtctx.StatementContext{}, false), IsFalse)
		}
		for _, v := range statsTblnew.Indices {
			c.Assert(v.IsInvalid(&stmtctx.StatementContext{}, false), IsFalse)
		}

	}
}
func (s *testStatsSuite) TestManyTableChangeWithQuery(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	h := s.do.StatsHandle()
	origLease := h.Lease()
	h.SetLease(time.Second)
	defer func() { h.SetLease(origLease) }()

	BytesLimit := int64(300000)
	h.SetBytesLimit(BytesLimit)
	tableSize := 100
	testKit.MustExec("use test")
	for i := 0; i <= tableSize; i++ {
		testKit.MustExec(fmt.Sprintf("create table t%d(a int,b int,index idx(b))", i))
		testKit.MustExec(fmt.Sprintf("insert into t%d values(1,2),(2,5),(3,5)", i))
		testKit.MustExec(fmt.Sprintf("analyze table t%d", i))
	}

	// update all information to statscache
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)

	c.Assert(h.Update(s.do.InfoSchema()), IsNil)
	for i := 0; i <= tableSize; i++ {
		tableName := fmt.Sprintf("t%d", i)
		tbl, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr(tableName))
		c.Assert(err, IsNil)
		tableInfo := tbl.Meta()
		testKit.MustQuery(fmt.Sprintf("select * from t%d use index(idx) where b <= 5", i))
		testKit.MustQuery(fmt.Sprintf("select * from t%d where a > 1", i))
		testKit.MustQuery(fmt.Sprintf("select * from t%d use index(idx) where b = 5", i))

		c.Assert(h.LoadNeededHistograms(), IsNil)
		c.Assert(BytesLimit >= h.GetMemConsumed(), IsTrue)
		statsTblnew := h.GetTableStats(tableInfo)
		c.Assert(statsTblnew.MemoryUsage() > 0, IsTrue)

		for _, v := range statsTblnew.Columns {
			c.Assert(v.IsInvalid(&stmtctx.StatementContext{}, false), IsFalse)
		}
		for _, v := range statsTblnew.Indices {
			c.Assert(v.IsInvalid(&stmtctx.StatementContext{}, false), IsFalse)
		}

	}
}
