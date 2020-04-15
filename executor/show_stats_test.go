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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/statistics"
	"github.com/pingcap/tidb/v4/util/testkit"
)

type testShowStatsSuite struct {
	*baseTestSuite
}

func (s *testShowStatsSuite) TestShowStatsMeta(c *C) {
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

func (s *testShowStatsSuite) TestShowStatsHistograms(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("analyze table t")
	result := tk.MustQuery("show stats_histograms")
	c.Assert(len(result.Rows()), Equals, 0)
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("analyze table t")
	result = tk.MustQuery("show stats_histograms").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][3], Equals, "a")
	c.Assert(result.Rows()[1][3], Equals, "b")
	result = tk.MustQuery("show stats_histograms where column_name = 'a'")
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][3], Equals, "a")

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c_a(c, a))")
	tk.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null)")
	res := tk.MustQuery("show stats_histograms where table_name = 't'")
	c.Assert(len(res.Rows()), Equals, 0)
	tk.MustExec("analyze table t index idx_b")
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'idx_b'")
	c.Assert(len(res.Rows()), Equals, 1)
}

func (s *testShowStatsSuite) TestShowStatsBuckets(c *C) {
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

func (s *testShowStatsSuite) TestShowStatsHasNullValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, index idx(a))")
	tk.MustExec("insert into t values(NULL)")
	tk.MustExec("analyze table t")
	// Null values are excluded from histogram for single-column index.
	tk.MustQuery("show stats_buckets").Check(testkit.Rows())
	tk.MustExec("insert into t values(1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_buckets").Sort().Check(testkit.Rows(
		"test t  a 0 0 1 1 1 1",
		"test t  idx 1 0 1 1 1 1",
	))
	tk.MustExec("drop table t")
	tk.MustExec("create table t (a int, b int, index idx(a, b))")
	tk.MustExec("insert into t values(NULL, NULL)")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  idx 1 0 1 1 (NULL, NULL) (NULL, NULL)"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c_a(c, a))")
	tk.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null)")
	res := tk.MustQuery("show stats_histograms where table_name = 't'")
	c.Assert(len(res.Rows()), Equals, 0)
	tk.MustExec("analyze table t index idx_b")
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'idx_b'")
	c.Assert(len(res.Rows()), Equals, 1)
	c.Assert(res.Rows()[0][7], Equals, "4")
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'b'")
	c.Assert(len(res.Rows()), Equals, 0)
	tk.MustExec("analyze table t index idx_c_a")
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'idx_c_a'")
	c.Assert(len(res.Rows()), Equals, 1)
	c.Assert(res.Rows()[0][7], Equals, "0")
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'c'")
	c.Assert(len(res.Rows()), Equals, 0)
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'a'")
	c.Assert(len(res.Rows()), Equals, 0)
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null)")
	res = tk.MustQuery("show stats_histograms where table_name = 't'")
	c.Assert(len(res.Rows()), Equals, 0)
	tk.MustExec("analyze table t index")
	res = tk.MustQuery("show stats_histograms where table_name = 't'").Sort()
	c.Assert(len(res.Rows()), Equals, 2)
	c.Assert(res.Rows()[0][7], Equals, "4")
	c.Assert(res.Rows()[1][7], Equals, "0")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null)")
	tk.MustExec("analyze table t")
	res = tk.MustQuery("show stats_histograms where table_name = 't'").Sort()
	c.Assert(len(res.Rows()), Equals, 5)
	c.Assert(res.Rows()[0][7], Equals, "1")
	c.Assert(res.Rows()[1][7], Equals, "4")
	c.Assert(res.Rows()[2][7], Equals, "1")
	c.Assert(res.Rows()[3][7], Equals, "4")
	c.Assert(res.Rows()[4][7], Equals, "0")
}

func (s *testShowStatsSuite) TestShowPartitionStats(c *C) {
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
	c.Assert(len(result.Rows()), Equals, 3)
	c.Assert(result.Rows()[0][2], Equals, "p0")
	c.Assert(result.Rows()[0][3], Equals, "a")
	c.Assert(result.Rows()[1][2], Equals, "p0")
	c.Assert(result.Rows()[1][3], Equals, "b")
	c.Assert(result.Rows()[2][2], Equals, "p0")
	c.Assert(result.Rows()[2][3], Equals, "idx")

	result = tk.MustQuery("show stats_buckets").Sort()
	result.Check(testkit.Rows("test t p0 a 0 0 1 1 1 1", "test t p0 b 0 0 1 1 1 1", "test t p0 idx 1 0 1 1 1 1"))

	result = tk.MustQuery("show stats_healthy")
	result.Check(testkit.Rows("test t p0 100"))
}

func (s *testShowStatsSuite) TestShowAnalyzeStatus(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	statistics.ClearHistoryJobs()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, primary key(a), index idx(b))")
	tk.MustExec(`insert into t values (1, 1), (2, 2)`)
	tk.MustExec("analyze table t")

	result := tk.MustQuery("show analyze status").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][0], Equals, "test")
	c.Assert(result.Rows()[0][1], Equals, "t")
	c.Assert(result.Rows()[0][2], Equals, "")
	c.Assert(result.Rows()[0][3], Equals, "analyze columns")
	c.Assert(result.Rows()[0][4], Equals, "2")
	c.Assert(result.Rows()[0][5], NotNil)
	c.Assert(result.Rows()[0][6], Equals, "finished")

	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[1][0], Equals, "test")
	c.Assert(result.Rows()[1][1], Equals, "t")
	c.Assert(result.Rows()[1][2], Equals, "")
	c.Assert(result.Rows()[1][3], Equals, "analyze index idx")
	c.Assert(result.Rows()[1][4], Equals, "2")
	c.Assert(result.Rows()[1][5], NotNil)
	c.Assert(result.Rows()[1][6], Equals, "finished")
}

func (s *testShowStatsSuite) TestShowStatusSnapshot(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int);")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	snapshotTime := time.Now()

	tk.MustExec("drop table t;")
	tk.MustQuery("show table status;").Check(testkit.Rows())
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	result := tk.MustQuery("show table status;")
	c.Check(result.Rows()[0][0], Matches, "t")
}
