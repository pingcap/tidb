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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestParallelApplyWarnning(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t1 (a int, b int, c int);")
		testKit.MustExec("create table t2 (a int, b int, c int, key(a));")
		testKit.MustExec("create table t3(a int, b int, c int, key(a));")
		testKit.MustExec("set tidb_enable_parallel_apply=on;")
		testKit.MustQuery("select (select /*+ inl_hash_join(t2, t3) */  1 from t2, t3 where t2.a=t3.a and t2.b > t1.b) from t1;")
		testKit.MustQuery("show warnings").Check(testkit.Rows())
		// https://github.com/pingcap/tidb/issues/59863
		testKit.MustExec("create table t(a int, b int, index idx(a));")
		testKit.MustQuery(`explain format='brief' select  t3.a from t t3 where (select /*+ inl_join(t1) */  count(*) from t t1 join t t2 on t1.a=t2.a and t1.b>t3.b);`).
			Check(testkit.Rows(
				`Projection 10000.00 root  test.t.a`,
				`└─Apply 10000.00 root  CARTESIAN inner join`,
				`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
				`  │ └─TableFullScan 10000.00 cop[tikv] table:t3 keep order:false, stats:pseudo`,
				`  └─Selection(Probe) 8000.00 root  Column#13`,
				`    └─HashAgg 10000.00 root  funcs:count(1)->Column#13`,
				"      └─IndexJoin 99900000.00 root  inner join, inner:IndexLookUp, outer key:test.t.a, inner key:test.t.a, equal cond:eq(test.t.a, test.t.a)",
				"        ├─IndexReader(Build) 99900000.00 root  index:IndexFullScan",
				"        │ └─IndexFullScan 99900000.00 cop[tikv] table:t2, index:idx(a) keep order:false, stats:pseudo",
				"        └─IndexLookUp(Probe) 99900000.00 root  ",
				"          ├─Selection(Build) 124875000.00 cop[tikv]  not(isnull(test.t.a))",
				"          │ └─IndexRangeScan 125000000.00 cop[tikv] table:t1, index:idx(a) range: decided by [eq(test.t.a, test.t.a)], keep order:false, stats:pseudo",
				"          └─Selection(Probe) 99900000.00 cop[tikv]  gt(test.t.b, test.t.b)",
				"            └─TableRowIDScan 124875000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		testKit.MustQuery("show warnings;").Check(testkit.Rows())
	})
}

// TestParallelApplyOrderedPlan verifies that the planner produces valid plans
// for parallel apply with ORDER BY and LIMIT.  This exercises the
// outerExpectedCnt computation in exhaustPhysicalPlans4LogicalApply and
// the KeepOrder setting in enableParallelApply.
func TestParallelApplyOrderedPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx_a(a))")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("set tidb_enable_parallel_apply=on")
	tk.MustExec("set tidb_executor_concurrency=5")

	// Helper: check that EXPLAIN output contains Apply and, when
	// expectKeepOrder is true, that the outer (Build) subtree contains
	// "keep order:true" — indicating ordered parallel apply is used.
	checkHasApply := func(sql string, expectKeepOrder bool) {
		rows := tk.MustQuery("explain " + sql).Rows()
		foundApply := false
		foundKeepOrder := false
		inBuildSide := false
		for _, row := range rows {
			line := fmt.Sprintf("%v", row)
			if strings.Contains(line, "Apply") {
				foundApply = true
			}
			// Track when we enter the Build subtree (outer side)
			// and leave it when we hit the Probe subtree.
			if strings.Contains(line, "Build") {
				inBuildSide = true
			} else if strings.Contains(line, "Probe") {
				inBuildSide = false
			}
			if inBuildSide && strings.Contains(line, "keep order:true") {
				foundKeepOrder = true
			}
		}
		require.True(t, foundApply, "plan should contain Apply: %s", sql)
		if expectKeepOrder {
			require.True(t, foundKeepOrder, "plan should have keep order:true on outer (Build) side: %s", sql)
		}
	}

	// 1. ORDER BY with correlated subquery — should produce Apply with
	//    ordered outer scan (keep order:true).  Exercises enableParallelApply
	//    setting KeepOrder = true.
	checkHasApply("select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a", true)

	// 2. ORDER BY + LIMIT — exercises the outerExpectedCnt computation
	//    in exhaustPhysicalPlans4LogicalApply.  The planner should still
	//    produce an Apply (not reject it due to sort properties).
	checkHasApply("select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a limit 5", true)

	// 3. No ORDER BY — basic unordered parallel apply still works.
	checkHasApply("select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1", false)

	// 4. ORDER BY with EXISTS (semi-join) + LIMIT — exercises the
	//    selectivity-based outerExpectedCnt calculation where
	//    applyRowCount < outerRowCount.
	checkHasApply("select t1.a from t1 where exists (select /*+ NO_DECORRELATE() */ 1 from t2 where t2.a = t1.a) order by t1.a limit 3", true)

	// 5. Verify no warnings are emitted for ordered parallel apply
	//    (the old code would emit "Parallel Apply rejects the possible
	//    order properties" which we removed).
	tk.MustQuery("explain select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// 6. Verify correctness: parallel + ordered should match serial.
	tk.MustExec("insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50)")
	tk.MustExec("insert into t2 values (1,1),(2,2),(3,3)")

	tk.MustExec("set tidb_enable_parallel_apply=off")
	serialRows := tk.MustQuery("select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a limit 3").Rows()

	tk.MustExec("set tidb_enable_parallel_apply=on")
	parallelRows := tk.MustQuery("select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a limit 3").Rows()

	require.Equal(t, serialRows, parallelRows)
}
