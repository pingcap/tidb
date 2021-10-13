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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"fmt"
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testRuleReorderResults{})
var _ = SerialSuites(&testRuleReorderResultsSerial{})

type testRuleReorderResultsSerial struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testRuleReorderResultsSerial) SetUpTest(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testRuleReorderResultsSerial) TearDownTest(c *C) {
	s.dom.Close()
	c.Assert(s.store.Close(), IsNil)
}

func (s *testRuleReorderResultsSerial) TestPlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")
	tk.MustExec("prepare s1 from 'select * from t where a > ? limit 10'")
	tk.MustExec("set @a = 10")
	tk.MustQuery("execute s1 using @a").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s1 using @a").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // plan cache is still working
}

func (s *testRuleReorderResultsSerial) TestSQLBinding(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("set tidb_opt_limit_push_down_threshold=0")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")
	tk.MustQuery("explain select * from t where a > 0 limit 1").Check(testkit.Rows(
		"Limit_12 1.00 root  offset:0, count:1",
		"└─TableReader_22 1.00 root  data:Limit_21",
		"  └─Limit_21 1.00 cop[tikv]  offset:0, count:1",
		"    └─TableRangeScan_20 1.00 cop[tikv] table:t range:(0,+inf], keep order:true, stats:pseudo"))

	tk.MustExec("create session binding for select * from t where a>0 limit 1 using select * from t use index(b) where a>0 limit 1")
	tk.MustQuery("explain select * from t where a > 0 limit 1").Check(testkit.Rows(
		"TopN_9 1.00 root  test.t.a, offset:0, count:1",
		"└─IndexLookUp_19 1.00 root  ",
		"  ├─TopN_18(Build) 1.00 cop[tikv]  test.t.a, offset:0, count:1",
		"  │ └─Selection_17 3333.33 cop[tikv]  gt(test.t.a, 0)",
		"  │   └─IndexFullScan_15 10000.00 cop[tikv] table:t, index:b(b) keep order:false, stats:pseudo",
		"  └─TableRowIDScan_16(Probe) 1.00 cop[tikv] table:t keep order:false, stats:pseudo"))
}

func (s *testRuleReorderResultsSerial) TestClusteredIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int,b int,c int, PRIMARY KEY (a,b))")
	tk.MustQuery("explain format=brief select * from t limit 10").Check(testkit.Rows(
		"TopN 10.00 root  test.t.a, test.t.b, test.t.c, offset:0, count:10",
		"└─TableReader 10.00 root  data:TopN",
		"  └─TopN 10.00 cop[tikv]  test.t.a, test.t.b, test.t.c, offset:0, count:10",
		"    └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOff
}

type testRuleReorderResults struct {
	store kv.Storage
	dom   *domain.Domain

	testData testutil.TestData
}

func (s *testRuleReorderResults) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)

	s.testData, err = testutil.LoadTestSuiteData("testdata", "ordered_result_mode_suite")
	c.Assert(err, IsNil)
}

func (s *testRuleReorderResults) TearDownSuite(c *C) {
	s.dom.Close()
	c.Assert(s.store.Close(), IsNil)
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testRuleReorderResults) runTestData(c *C, tk *testkit.TestKit, name string) {
	var input []string
	var output []struct {
		Plan []string
	}
	s.testData.GetTestCasesByName(name, c, &input, &output)
	c.Assert(len(input), Equals, len(output))
	for i := range input {
		s.testData.OnRecord(func() {
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + input[i]).Rows())
		})
		tk.MustQuery("explain " + input[i]).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testRuleReorderResults) TestOrderedResultMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")
	s.runTestData(c, tk, "TestOrderedResultMode")
}

func (s *testRuleReorderResults) TestOrderedResultModeOnDML(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, key(b))")
	s.runTestData(c, tk, "TestOrderedResultModeOnDML")
}

func (s *testRuleReorderResults) TestOrderedResultModeOnSubQuery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int primary key, b int, c int, d int, key(b))")
	tk.MustExec("create table t2 (a int primary key, b int, c int, d int, key(b))")
	s.runTestData(c, tk, "TestOrderedResultModeOnSubQuery")
}

func (s *testRuleReorderResults) TestOrderedResultModeOnJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int primary key, b int, c int, d int, key(b))")
	tk.MustExec("create table t2 (a int primary key, b int, c int, d int, key(b))")
	s.runTestData(c, tk, "TestOrderedResultModeOnJoin")
}

func (s *testRuleReorderResults) TestOrderedResultModeOnOtherOperators(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int primary key, b int, c int, d int, unique key(b))")
	tk.MustExec("create table t2 (a int primary key, b int, c int, d int, unique key(b))")
	s.runTestData(c, tk, "TestOrderedResultModeOnOtherOperators")
}

func (s *testRuleReorderResults) TestOrderedResultModeOnPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf(`set tidb_partition_prune_mode='%v'`, variable.DefTiDBPartitionPruneMode))
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists thash")
	tk.MustExec("drop table if exists trange")
	tk.MustExec("create table thash (a int primary key, b int, c int, d int) partition by hash(a) partitions 4")
	tk.MustExec(`create table trange (a int primary key, b int, c int, d int) partition by range(a) (
					partition p0 values less than (100),
					partition p1 values less than (200),
					partition p2 values less than (300),
					partition p3 values less than (400))`)
	tk.MustQuery("select @@tidb_partition_prune_mode").Check(testkit.Rows("static"))
	s.runTestData(c, tk, "TestOrderedResultModeOnPartitionTable")
}

func (s *testRuleReorderResults) TestStableResultSwitch(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	c.Assert(len(tk.MustQuery("show variables where variable_name like 'tidb_enable_ordered_result_mode'").Rows()), Equals, 1)
}
