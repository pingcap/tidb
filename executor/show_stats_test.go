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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestShowStatsMeta(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("analyze table t, t1")
	result := tk.MustQuery("show stats_meta")
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][1], Equals, "t")
	c.Assert(result.Rows()[1][1], Equals, "t1")
	result = tk.MustQuery("show stats_meta where table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][1], Equals, "t")
}

func (s *testSuite) TestShowStatsHistograms(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("analyze table t")
	result := tk.MustQuery("show stats_histograms").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][3], Equals, "a")
	c.Assert(result.Rows()[1][3], Equals, "b")
	result = tk.MustQuery("show stats_histograms where column_name = 'a'")
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][3], Equals, "a")
}

func (s *testSuite) TestShowStatsBuckets(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create index idx on t(a,b)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze table t")
	result := tk.MustQuery("show stats_buckets").Sort()
	result.Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  b 0 0 1 1 1 1", "test t  idx 1 0 1 1 (1, 1) (1, 1)"))
	result = tk.MustQuery("show stats_buckets where column_name = 'idx'")
	result.Check(testkit.Rows("test t  idx 1 0 1 1 (1, 1) (1, 1)"))
}

func (s *testSuite) TestShowStatsHealthy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create index idx on t(a)")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  100"))
	tk.MustExec("insert into t values (1), (2)")
	do, _ := session.GetDomain(s.store)
	do.StatsHandle().DumpStatsDeltaToKV(statistics.DumpAll)
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  100"))
	tk.MustExec("insert into t values (3), (4), (5), (6), (7), (8), (9), (10)")
	do.StatsHandle().DumpStatsDeltaToKV(statistics.DumpAll)
	do.StatsHandle().Update(do.InfoSchema())
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  19"))
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  100"))
	tk.MustExec("delete from t")
	do.StatsHandle().DumpStatsDeltaToKV(statistics.DumpAll)
	do.StatsHandle().Update(do.InfoSchema())
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  0"))
}

func (s *testSuite) TestShowStatsHasNullValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, index idx(a))")
	tk.MustExec("insert into t values(NULL)")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  idx 1 0 1 1 NULL NULL"))
}

func (s *testSuite) TestShowPartitionStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@session.tidb_enable_table_partition=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b int, primary key(a), index idx(b))
						PARTITION BY RANGE ( a ) (PARTITION p0 VALUES LESS THAN (6))`
	tk.MustExec(createTable)
	tk.MustExec(`insert into t values (1, 1)`)
	tk.MustExec("analyze table t")

	result := tk.MustQuery("show stats_meta")
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][0], Equals, "test")
	c.Assert(result.Rows()[0][1], Equals, "t")
	c.Assert(result.Rows()[0][2], Equals, "p0")

	result = tk.MustQuery("show stats_histograms").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][2], Equals, "p0")
	c.Assert(result.Rows()[0][3], Equals, "a")
	c.Assert(result.Rows()[1][2], Equals, "p0")
	c.Assert(result.Rows()[1][3], Equals, "idx")

	result = tk.MustQuery("show stats_buckets").Sort()
	result.Check(testkit.Rows("test t p0 a 0 0 1 1 1 1", "test t p0 idx 1 0 1 1 1 1"))

	result = tk.MustQuery("show stats_healthy")
	result.Check(testkit.Rows("test t p0 100"))
}
