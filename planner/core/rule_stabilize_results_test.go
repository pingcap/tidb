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

package core_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testRuleStabilizeRestuls{})

type testRuleStabilizeRestuls struct {
	store kv.Storage
	dom   *domain.Domain

	testData testutil.TestData
}

func (s *testRuleStabilizeRestuls) SetUpTest(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)

	s.testData, err = testutil.LoadTestSuiteData("testdata", "plan_normalized_suite")
	c.Assert(err, IsNil)
}

func (s *testRuleStabilizeRestuls) TestStableResultMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_stable_result_mode=1")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
}

func (s *testRuleStabilizeRestuls) TestEnableStableResultMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_stable_result_mode=1")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")

	tk.MustQuery("explain select * from t use index(primary)").Check(testkit.Rows(
		"TableReader_10 10000.00 root  data:TableFullScan_9",
		"└─TableFullScan_9 10000.00 cop[tikv] table:t keep order:true, stats:pseudo")) // keep order: true

	tk.MustQuery("explain select b from t use index(b)").Check(testkit.Rows(
		"IndexReader_10 10000.00 root  index:IndexFullScan_9",
		"└─IndexFullScan_9 10000.00 cop[tikv] table:t, index:b(b) keep order:true, stats:pseudo")) // keep order: true

	tk.MustQuery("explain select a, b from t use index(b)").Check(testkit.Rows(
		"Sort_5 10000.00 root  test.t.a", // since PK is needed, order results by it
		"└─IndexReader_8 10000.00 root  index:IndexFullScan_7",
		"  └─IndexFullScan_7 10000.00 cop[tikv] table:t, index:b(b) keep order:false, stats:pseudo"))

	tk.MustQuery("explain select b, c from t use index(b)").Check(testkit.Rows(
		"Sort_5 10000.00 root  test.t.b, test.t.c", // order by both columns to make results stable
		"└─IndexLookUp_9 10000.00 root  ",
		"  ├─IndexFullScan_7(Build) 10000.00 cop[tikv] table:t, index:b(b) keep order:false, stats:pseudo",
		"  └─TableRowIDScan_8(Probe) 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	tk.MustQuery("explain select b, c from t use index(primary)").Check(testkit.Rows(
		"Sort_5 10000.00 root  test.t.b, test.t.c", // since PK is pruned, order results by b, c
		"└─TableReader_8 10000.00 root  data:TableFullScan_7",
		"  └─TableFullScan_7 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// test agg
	tk.MustQuery("explain select min(b), max(c) from t use index(primary) group by d").Check(testkit.Rows(
		"Sort_6 8000.00 root  Column#5, Column#6", // order by min(b), max(c)
		"└─HashAgg_12 8000.00 root  group by:test.t.d, funcs:min(Column#7)->Column#5, funcs:max(Column#8)->Column#6",
		"  └─TableReader_13 8000.00 root  data:HashAgg_8",
		"    └─HashAgg_8 8000.00 cop[tikv]  group by:test.t.d, funcs:min(test.t.b)->Column#7, funcs:max(test.t.c)->Column#8",
		"      └─TableFullScan_11 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	tk.MustQuery("explain select min(b), max(c) from t use index(primary) group by a").Check(testkit.Rows(
		"Sort_7 10000.00 root  Column#5, Column#6", // order by min(b), max(c)
		"└─Projection_9 10000.00 root  cast(test.t.b, int(11))->Column#5, cast(test.t.c, int(11))->Column#6",
		"  └─TableReader_11 10000.00 root  data:TableFullScan_10", // agg is eliminated since a is PK
		"    └─TableFullScan_10 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// test limit
	tk.MustQuery("explain select * from t use index(b) limit 10").Check(testkit.Rows(
		"TopN_8 10.00 root  test.t.a, offset:0, count:10", // order by PK
		"└─IndexLookUp_16 10.00 root  ",
		"  ├─TopN_15(Build) 10.00 cop[tikv]  test.t.a, offset:0, count:10",
		"  │ └─IndexFullScan_13 10000.00 cop[tikv] table:t, index:b(b) keep order:false, stats:pseudo",
		"  └─TableRowIDScan_14(Probe) 10.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	tk.MustQuery("explain select * from t use index(primary) limit 10").Check(testkit.Rows(
		"Limit_10 10.00 root  offset:0, count:10",
		"└─TableReader_20 10.00 root  data:Limit_19",
		"  └─Limit_19 10.00 cop[tikv]  offset:0, count:10",
		"    └─TableFullScan_18 10.00 cop[tikv] table:t keep order:true, stats:pseudo")) // keep order: true

	// test order
	tk.MustQuery("explain select b from t use index(b) order by b").Check(testkit.Rows(
		"IndexReader_11 10000.00 root  index:IndexFullScan_10",
		"└─IndexFullScan_10 10000.00 cop[tikv] table:t, index:b(b) keep order:true, stats:pseudo")) // keep order: true

	tk.MustQuery("explain select b, c, d from t use index(b) order by b").Check(testkit.Rows(
		"Sort_4 10000.00 root  test.t.b, test.t.c, test.t.d", // order by all columns to keep stable
		"└─IndexLookUp_9 10000.00 root  ",
		"  ├─IndexFullScan_7(Build) 10000.00 cop[tikv] table:t, index:b(b) keep order:false, stats:pseudo",
		"  └─TableRowIDScan_8(Probe) 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// test join
	tk.MustQuery("explain select t1.a, t2.a from t t1, t t2 where t1.a=t2.a").Check(testkit.Rows(
		"Sort_9 12500.00 root  test.t.a, test.t.a",
		"└─HashJoin_30 12500.00 root  inner join, equal:[eq(test.t.a, test.t.a)]",
		"  ├─IndexReader_43(Build) 10000.00 root  index:IndexFullScan_42",
		"  │ └─IndexFullScan_42 10000.00 cop[tikv] table:t2, index:b(b) keep order:false, stats:pseudo",
		"  └─IndexReader_39(Probe) 10000.00 root  index:IndexFullScan_38",
		"    └─IndexFullScan_38 10000.00 cop[tikv] table:t1, index:b(b) keep order:false, stats:pseudo"))
}
