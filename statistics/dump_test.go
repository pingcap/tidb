// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testDumpStatsSuite{})

type testDumpStatsSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (s *testDumpStatsSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	s.store, s.do, err = newStoreWithBootstrap(0)
	c.Assert(err, IsNil)
}

func (s *testDumpStatsSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testDumpStatsSuite) TestConversion(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create index c on t(a,b)")
	tk.MustExec("insert into t(a,b) values (3, 1),(2, 1),(1, 10)")
	tk.MustExec("analyze table t")

	is := s.do.InfoSchema()
	h := s.do.StatsHandle()
	h.Update(is)
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo.Meta())
	c.Assert(err, IsNil)
	loadTbl, err := h.LoadStatsFromJSON(tableInfo.Meta(), jsonTbl)
	c.Assert(err, IsNil)
	tbl := h.GetTableStats(tableInfo.Meta())
	assertTableEqual(c, loadTbl, tbl)
}

func (s *testDumpStatsSuite) TestDumpAlteredTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	h := s.do.StatsHandle()
	oriLease := h.Lease
	h.Lease = 1
	defer func() { h.Lease = oriLease }()
	tk.MustExec("create table t(a int, b int)")
	h.HandleDDLEvent(<-h.DDLEventCh())
	tk.MustExec("analyze table t")
	t := <-h.AnalyzeResultCh()
	h.Update(s.do.InfoSchema())
	for i, hg := range t.Hist {
		err := statistics.SaveStatsToStorage(tk.Se, t.TableID, t.Count, t.IsIndex, hg, t.Cms[i])
		c.Assert(err, IsNil)
	}
	h.Update(s.do.InfoSchema())
	tk.MustExec("alter table t drop column a")
	table, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	_, err = h.DumpStatsToJSON("test", table.Meta())
	c.Assert(err, IsNil)
}
