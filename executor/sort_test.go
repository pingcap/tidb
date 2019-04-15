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

func (s *testSuite2) TestSortRand(c *C) {
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
