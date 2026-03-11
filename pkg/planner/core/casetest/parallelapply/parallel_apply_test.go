// Copyright 2024 PingCAP, Inc.
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

package parallelapply

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestParallelApplyWarnning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, c int);")
	tk.MustExec("create table t2 (a int, b int, c int, key(a));")
	tk.MustExec("create table t3(a int, b int, c int, key(a));")
	tk.MustExec("set tidb_enable_parallel_apply=on;")
	tk.MustQuery("select (select /*+ inl_hash_join(t2, t3) */  1 from t2, t3 where t2.a=t3.a and t2.b > t1.b) from t1;")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	// https://github.com/pingcap/tidb/issues/59863
	tk.MustExec("create table t(a int, b int, index idx(a));")
	tk.MustQuery(`explain format='brief' select  t3.a from t t3 where (select /*+ inl_join(t1) */  count(*) from t t1 join t t2 on t1.a=t2.a and t1.b>t3.b);`).
		Check(testkit.Rows(
			`Projection 10000.00 root  test.t.a`,
			`└─Apply 10000.00 root  CARTESIAN inner join`,
			`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
			`  │ └─TableFullScan 10000.00 cop[tikv] table:t3 keep order:false, stats:pseudo`,
			`  └─Selection(Probe) 8000.00 root  Column#10`,
			`    └─HashAgg 10000.00 root  funcs:count(1)->Column#10`,
			"      └─IndexJoin 99900000.00 root  inner join, inner:IndexLookUp, outer key:test.t.a, inner key:test.t.a, equal cond:eq(test.t.a, test.t.a)",
			"        ├─IndexReader(Build) 99900000.00 root  index:IndexFullScan",
			"        │ └─IndexFullScan 99900000.00 cop[tikv] table:t2, index:idx(a) keep order:false, stats:pseudo",
			"        └─IndexLookUp(Probe) 99900000.00 root  ",
			"          ├─Selection(Build) 124875000.00 cop[tikv]  not(isnull(test.t.a))",
			"          │ └─IndexRangeScan 125000000.00 cop[tikv] table:t1, index:idx(a) range: decided by [eq(test.t.a, test.t.a)], keep order:false, stats:pseudo",
			"          └─Selection(Probe) 99900000.00 cop[tikv]  gt(test.t.b, test.t.b)",
			"            └─TableRowIDScan 124875000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings;").Check(testkit.Rows())
}
