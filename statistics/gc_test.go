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
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStatsUpdateSuite) TestGCStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, index idx(a, b))")
	testKit.MustExec("insert into t values (1,1),(2,2),(3,3)")
	testKit.MustExec("analyze table t")

	testKit.MustExec("alter table t drop index idx")
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_histograms").Rows()), Equals, 3)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_buckets").Rows()), Equals, 9)
	h := s.do.StatsHandle()
	h.PrevLastVersion = math.MaxUint64
	c.Assert(h.GCStats(s.do.InfoSchema()), IsNil)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_histograms").Rows()), Equals, 2)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_buckets").Rows()), Equals, 6)

	testKit.MustExec("alter table t drop column a")
	c.Assert(h.GCStats(s.do.InfoSchema()), IsNil)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_histograms").Rows()), Equals, 1)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_buckets").Rows()), Equals, 3)

	testKit.MustExec("drop table t")
	c.Assert(h.GCStats(s.do.InfoSchema()), IsNil)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_meta").Rows()), Equals, 1)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_histograms").Rows()), Equals, 0)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_buckets").Rows()), Equals, 0)
	c.Assert(h.GCStats(s.do.InfoSchema()), IsNil)
	c.Assert(len(testKit.MustQuery("select * from mysql.stats_meta").Rows()), Equals, 0)
}
