// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite4) TestSortRand(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int);")

	tk.MustQuery("explain select a from t order by rand()").Check(testkit.Rows(
		"Projection_8 10000.00 root test.t.a",
		"└─Sort_4 10000.00 root col_1:asc",
		"  └─Projection_9 10000.00 root test.t.a, rand()",
		"    └─TableReader_7 10000.00 root data:TableScan_6",
		"      └─TableScan_6 10000.00 cop table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
	))

	tk.MustQuery("explain select a, b from t order by abs(2)").Check(testkit.Rows(
		"TableReader_8 10000.00 root data:TableScan_7",
		"└─TableScan_7 10000.00 cop table:t, range:[-inf,+inf], keep order:false, stats:pseudo"))

	tk.MustQuery("explain select a from t order by abs(rand())+1").Check(testkit.Rows(
		"Projection_8 10000.00 root test.t.a",
		"└─Sort_4 10000.00 root col_1:asc",
		"  └─Projection_9 10000.00 root test.t.a, plus(abs(rand()), 1)",
		"    └─TableReader_7 10000.00 root data:TableScan_6",
		"      └─TableScan_6 10000.00 cop table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
}

func (s *testSuite4) TestSortCorrelatedColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int, b int);")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2(a int, b int);")

	tk.MustQuery("explain select * from t1 where t1.a in (select t2.a as a from t2 where t2.b > t1.b order by t1.b);").Check(testkit.Rows(
		"Apply_11 9990.00 root semi join, inner:TableReader_19, equal:[eq(test.t1.a, test.t2.a)]",
		"├─TableReader_14 9990.00 root data:Selection_13",
		"│ └─Selection_13 9990.00 cop not(isnull(test.t1.a))",
		"│   └─TableScan_12 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_19 7992.00 root data:Selection_18",
		"  └─Selection_18 7992.00 cop gt(test.t2.b, test.t1.b), not(isnull(test.t2.a))",
		"    └─TableScan_17 10000.00 cop table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
}
