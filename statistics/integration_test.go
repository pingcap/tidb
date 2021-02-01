// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testIntegrationSuite{})

type testIntegrationSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	s.do.Close()
	c.Assert(s.store.Close(), IsNil)
	testleak.AfterTest(c)()
}

func (s *testIntegrationSuite) TestChangeVerTo2Behavior(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("insert into t values(1, 1), (1, 2), (1, 3)")
	tk.MustExec("analyze table t")
	is := s.do.InfoSchema()
	tblT, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	h := s.do.StatsHandle()
	c.Assert(h.Update(is), IsNil)
	statsTblT := h.GetTableStats(tblT.Meta())
	// Analyze table with version 1 success, all statistics are version 1.
	for _, col := range statsTblT.Columns {
		c.Assert(col.StatsVer, Equals, int64(1))
	}
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(1))
	}
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	c.Assert(h.Update(is), IsNil)
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(1))
	}
	tk.MustExec("analyze table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	c.Assert(h.Update(is), IsNil)
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(1))
	}
	tk.MustExec("analyze table t ")
	c.Assert(h.Update(is), IsNil)
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		c.Assert(col.StatsVer, Equals, int64(2))
	}
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(2))
	}
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	c.Assert(h.Update(is), IsNil)
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(2))
	}
	tk.MustExec("analyze table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	c.Assert(h.Update(is), IsNil)
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(2))
	}
	tk.MustExec("analyze table t ")
	c.Assert(h.Update(is), IsNil)
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		c.Assert(col.StatsVer, Equals, int64(1))
	}
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(1))
	}
}

func (s *testIntegrationSuite) TestFastAnalyzeOnVer2(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_enable_fast_analyze = 1")
	tk.MustExec("insert into t values(1, 1), (1, 2), (1, 3)")
	_, err := tk.Exec("analyze table t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently.")
	tk.MustExec("set @@session.tidb_enable_fast_analyze = 0")
	tk.MustExec("analyze table t")
	is := s.do.InfoSchema()
	tblT, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	h := s.do.StatsHandle()
	c.Assert(h.Update(is), IsNil)
	statsTblT := h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		c.Assert(col.StatsVer, Equals, int64(2))
	}
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(2))
	}
	tk.MustExec("set @@session.tidb_enable_fast_analyze = 1")
	err = tk.ExecToErr("analyze table t index idx")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently.")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	_, err = tk.Exec("analyze table t index idx")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently. But the existing statistics of the table is not version 1.")
	_, err = tk.Exec("analyze table t index")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently. But the existing statistics of the table is not version 1.")
	tk.MustExec("analyze table t")
	c.Assert(h.Update(is), IsNil)
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		c.Assert(col.StatsVer, Equals, int64(1))
	}
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(1))
	}
}

func (s *testIntegrationSuite) TestIncAnalyzeOnVer2(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("insert into t values(1, 1), (1, 2)")
	tk.MustExec("analyze table t with 2 topn")
	is := s.do.InfoSchema()
	h := s.do.StatsHandle()
	c.Assert(h.Update(is), IsNil)
	tk.MustExec("insert into t values(2, 1), (2, 2), (2, 3), (3, 3), (4, 4), (4, 3), (4, 2), (4, 1)")
	c.Assert(h.Update(is), IsNil)
	tk.MustExec("analyze incremental table t index idx with 2 topn")
	// After analyze, there's two val in hist.
	tk.MustQuery("show stats_buckets where table_name = 't' and column_name = 'idx'").Check(testkit.Rows(
		"test t  idx 1 0 2 2 1 1 1",
		"test t  idx 1 1 3 0 2 4 1",
	))
	// Two val in topn.
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'idx'").Check(testkit.Rows(
		"test t  idx 1 2 3",
		"test t  idx 1 4 4",
	))
}
