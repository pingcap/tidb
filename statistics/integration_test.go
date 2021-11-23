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
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testIntegrationSuite{})
var _ = SerialSuites(&testSerialIntegrationSuite{})

type testIntegrationSuite struct {
	store    kv.Storage
	do       *domain.Domain
	testData testutil.TestData
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.testData, err = testutil.LoadTestSuiteData("testdata", "integration_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	s.do.Close()
	c.Assert(s.store.Close(), IsNil)
	testleak.AfterTest(c)()
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

type testSerialIntegrationSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (s *testSerialIntegrationSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testSerialIntegrationSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead",
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes"))
	c.Assert(h.Update(is), IsNil)
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		c.Assert(idx.StatsVer, Equals, int64(2))
	}
	tk.MustExec("analyze table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead",
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes"))
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
		"test t  idx 1 0 2 2 1 1 0",
		"test t  idx 1 1 3 1 3 3 0",
	))
	// Two val in topn.
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'idx'").Check(testkit.Rows(
		"test t  idx 1 2 3",
		"test t  idx 1 4 4",
	))
}

func (s *testIntegrationSuite) TestExpBackoffEstimation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table exp_backoff(a int, b int, c int, d int, index idx(a, b, c, d))")
	tk.MustExec("insert into exp_backoff values(1, 1, 1, 1), (1, 1, 1, 2), (1, 1, 2, 3), (1, 2, 2, 4), (1, 2, 3, 5)")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table exp_backoff")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	inputLen := len(input)
	// The test cases are:
	// Query a = 1, b = 1, c = 1, d >= 3 and d <= 5 separately. We got 5, 3, 2, 3.
	// And then query and a = 1 and b = 1 and c = 1 and d >= 3 and d <= 5. It's result should follow the exp backoff,
	// which is 2/5 * (3/5)^{1/2} * (3/5)*{1/4} * 1^{1/8} * 5 = 1.3634.
	for i := 0; i < inputLen-1; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}

	// The last case is that no column is loaded and we get no stats at all.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/statistics/cleanEstResults", `return(true)`), IsNil)
	s.testData.OnRecord(func() {
		output[inputLen-1] = s.testData.ConvertRowsToStrings(tk.MustQuery(input[inputLen-1]).Rows())
	})
	tk.MustQuery(input[inputLen-1]).Check(testkit.Rows(output[inputLen-1]...))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/statistics/cleanEstResults"), IsNil)
}

func (s *testIntegrationSuite) TestGlobalStats(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec(`create table t (a int, key(a)) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (30)
	);`)
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("insert into t values (1), (5), (null), (11), (15), (21), (25);")
	tk.MustExec("analyze table t;")
	// On the table with global-stats, we use explain to query a multi-partition query.
	// And we should get the result that global-stats is used instead of pseudo-stats.
	tk.MustQuery("explain format = 'brief' select a from t where a > 5").Check(testkit.Rows(
		"IndexReader 4.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 4.00 cop[tikv] table:t, index:a(a) range:(5,+inf], keep order:false"))
	// On the table with global-stats, we use explain to query a single-partition query.
	// And we should get the result that global-stats is used instead of pseudo-stats.
	tk.MustQuery("explain format = 'brief' select * from t partition(p1) where a > 15;").Check(testkit.Rows(
		"IndexReader 2.00 root partition:p1 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t, index:a(a) range:(15,+inf], keep order:false"))

	// Even if we have global-stats, we will not use it when the switch is set to `static`.
	tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
	tk.MustQuery("explain format = 'brief' select a from t where a > 5").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─IndexReader 0.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 0.00 cop[tikv] table:t, partition:p0, index:a(a) range:(5,+inf], keep order:false",
		"├─IndexReader 2.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 2.00 cop[tikv] table:t, partition:p1, index:a(a) range:(5,+inf], keep order:false",
		"└─IndexReader 2.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 2.00 cop[tikv] table:t, partition:p2, index:a(a) range:(5,+inf], keep order:false"))

	tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a int, b int, key(a)) PARTITION BY HASH(a) PARTITIONS 2;")
	tk.MustExec("insert into t values(1,1),(3,3),(4,4),(2,2),(5,5);")
	// When we set the mode to `static`, using analyze will not report an error and will not generate global-stats.
	// In addition, when using explain to view the plan of the related query, it was found that `Union` was used.
	tk.MustExec("analyze table t;")
	result := tk.MustQuery("show stats_meta where table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][5], Equals, "2")
	c.Assert(result.Rows()[1][5], Equals, "3")
	tk.MustQuery("explain format = 'brief' select a from t where a > 3;").Check(testkit.Rows(
		"PartitionUnion 2.00 root  ",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p0, index:a(a) range:(3,+inf], keep order:false",
		"└─IndexReader 1.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p1, index:a(a) range:(3,+inf], keep order:false"))

	// When we turned on the switch, we found that pseudo-stats will be used in the plan instead of `Union`.
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustQuery("explain format = 'brief' select a from t where a > 3;").Check(testkit.Rows(
		"IndexReader 3333.33 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 3333.33 cop[tikv] table:t, index:a(a) range:(3,+inf], keep order:false, stats:pseudo"))

	// Execute analyze again without error and can generate global-stats.
	// And when executing related queries, neither Union nor pseudo-stats are used.
	tk.MustExec("analyze table t;")
	result = tk.MustQuery("show stats_meta where table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 3)
	c.Assert(result.Rows()[0][5], Equals, "5")
	c.Assert(result.Rows()[1][5], Equals, "2")
	c.Assert(result.Rows()[2][5], Equals, "3")
	tk.MustQuery("explain format = 'brief' select a from t where a > 3;").Check(testkit.Rows(
		"IndexReader 2.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t, index:a(a) range:(3,+inf], keep order:false"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, c int)  PARTITION BY HASH(a) PARTITIONS 2;")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("create index idx_ab on t(a, b);")
	tk.MustExec("insert into t values (1, 1, 1), (5, 5, 5), (11, 11, 11), (15, 15, 15), (21, 21, 21), (25, 25, 25);")
	tk.MustExec("analyze table t;")
	// test the indexScan
	tk.MustQuery("explain format = 'brief' select b from t where a > 5 and b > 10;").Check(testkit.Rows(
		"Projection 2.67 root  test.t.b",
		"└─IndexReader 2.67 root partition:all index:Selection",
		"  └─Selection 2.67 cop[tikv]  gt(test.t.b, 10)",
		"    └─IndexRangeScan 4.00 cop[tikv] table:t, index:idx_ab(a, b) range:(5,+inf], keep order:false"))
	// test the indexLookUp
	tk.MustQuery("explain format = 'brief' select * from t use index(idx_ab) where a > 1;").Check(testkit.Rows(
		"IndexLookUp 5.00 root partition:all ",
		"├─IndexRangeScan(Build) 5.00 cop[tikv] table:t, index:idx_ab(a, b) range:(1,+inf], keep order:false",
		"└─TableRowIDScan(Probe) 5.00 cop[tikv] table:t keep order:false"))
	// test the tableScan
	tk.MustQuery("explain format = 'brief' select * from t;").Check(testkit.Rows(
		"TableReader 6.00 root partition:all data:TableFullScan",
		"└─TableFullScan 6.00 cop[tikv] table:t keep order:false"))
}

func (s *testIntegrationSuite) TestNULLOnFullSampling(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1), (1), (1), (2), (2), (3), (4), (null), (null), (null)")
	var (
		input  []string
		output [][]string
	)
	tk.MustExec("analyze table t with 2 topn")
	is := s.do.InfoSchema()
	tblT, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	h := s.do.StatsHandle()
	c.Assert(h.Update(is), IsNil)
	statsTblT := h.GetTableStats(tblT.Meta())
	// Check the null count is 3.
	for _, col := range statsTblT.Columns {
		c.Assert(col.NullCount, Equals, int64(3))
	}

	s.testData.GetTestCases(c, &input, &output)
	// Check the topn and buckets contains no null values.
	for i := 0; i < len(input); i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func (s *testSerialIntegrationSuite) TestOutdatedStatsCheck(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	h := s.do.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19)) // 20 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is := s.do.InfoSchema()
	c.Assert(h.Update(is), IsNil)
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	c.Assert(h.LoadNeededHistograms(), IsNil)

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 13)) // 34 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(tk.HasPseudoStats("select * from t where a = 1"), IsFalse)

	tk.MustExec("insert into t values (1)") // 35 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(tk.HasPseudoStats("select * from t where a = 1"), IsTrue)

	tk.MustExec("analyze table t")

	tk.MustExec("delete from t limit 24") // 11 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(tk.HasPseudoStats("select * from t where a = 1"), IsFalse)

	tk.MustExec("delete from t limit 1") // 10 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(tk.HasPseudoStats("select * from t where a = 1"), IsTrue)
}
