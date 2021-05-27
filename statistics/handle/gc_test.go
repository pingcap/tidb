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

package handle_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStatsSuite) TestGCStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, index idx(a, b), index idx_a(a))")
	testKit.MustExec("insert into t values (1,1),(2,2),(3,3)")
	testKit.MustExec("analyze table t")

	testKit.MustExec("alter table t drop index idx")
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("4"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("12"))
	h := s.do.StatsHandle()
	ddlLease := time.Duration(0)
	c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("3"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("9"))

	testKit.MustExec("alter table t drop index idx_a")
	testKit.MustExec("alter table t drop column a")
	c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("1"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("3"))

	testKit.MustExec("drop table t")
	c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
	testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("1"))
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("0"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("0"))
	c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
	testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("0"))
}

func (s *testStatsSuite) TestGCPartition(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testkit.WithPruneMode(testKit, variable.Static, func() {
		testKit.MustExec("use test")
		testKit.MustExec("set @@session.tidb_enable_table_partition=1")
		testKit.MustExec(`create table t (a bigint(64), b bigint(64), index idx(a, b))
			    partition by range (a) (
			    partition p0 values less than (3),
			    partition p1 values less than (6))`)
		testKit.MustExec("insert into t values (1,2),(2,3),(3,4),(4,5),(5,6)")
		testKit.MustExec("analyze table t")

		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("6"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("15"))
		h := s.do.StatsHandle()
		ddlLease := time.Duration(0)
		testKit.MustExec("alter table t drop index idx")
		c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("4"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("10"))

		testKit.MustExec("alter table t drop column b")
		c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("2"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("5"))

		testKit.MustExec("drop table t")
		c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
		testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("2"))
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("0"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("0"))
		c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
		testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("0"))
	})
}

func (s *testStatsSuite) TestGCExtendedStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("set session tidb_enable_extended_stats = on")
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int)")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")
	testKit.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	testKit.MustExec("alter table t add stats_extended s2 correlation(b,c)")
	testKit.MustExec("analyze table t")

	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s1 2 [1,2] 1.000000 1",
		"s2 2 [2,3] 1.000000 1",
	))
	testKit.MustExec("alter table t drop column a")
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s1 2 [1,2] 1.000000 1",
		"s2 2 [2,3] 1.000000 1",
	))
	h := s.do.StatsHandle()
	ddlLease := time.Duration(0)
	c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s1 2 [1,2] 1.000000 2",
		"s2 2 [2,3] 1.000000 1",
	))
	c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s2 2 [2,3] 1.000000 1",
	))

	testKit.MustExec("drop table t")
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s2 2 [2,3] 1.000000 1",
	))
	c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s2 2 [2,3] 1.000000 2",
	))
	c.Assert(h.GCStats(s.do.InfoSchema(), ddlLease), IsNil)
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows())
}
