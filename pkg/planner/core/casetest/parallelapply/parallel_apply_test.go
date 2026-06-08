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

// TestLateralHierarchyParallelApply verifies two things for LATERAL + parallel apply:
//
//  1. Plan-level: the customer's recursive-CTE hierarchy query keeps Apply in the
//     plan (LATERAL is not decorrelated away) when parallel_apply is on.
//
//  2. Concurrency: a flat (non-recursive) LATERAL join reports Concurrency > 1 in
//     EXPLAIN ANALYZE when tidb_enable_parallel_apply=on and
//     tidb_executor_concurrency=5.  We use a flat query here because parallel apply
//     + recursive CTE has a known correctness issue (grandchildren are dropped) that
//     is tracked separately; fixing that is out of scope for this PR.
func TestLateralHierarchyParallelApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table category (
		id int primary key, parent_id int, name varchar(50), sort_order int,
		index idx_parent(parent_id, sort_order, name))`)
	tk.MustExec(`insert into category values
		(1, null, 'root', 0),
		(2, 1, 'child_a', 1), (3, 1, 'child_b', 2), (4, 1, 'child_c', 3), (5, 1, 'child_d', 4),
		(6, 2, 'grandchild_a1', 1), (7, 2, 'grandchild_a2', 2), (8, 2, 'grandchild_a3', 3),
		(9, 3, 'grandchild_b1', 1), (10, 3, 'grandchild_b2', 2)`)

	tk.MustExec("set tidb_enable_parallel_apply=on")
	tk.MustExec("set tidb_executor_concurrency=5")

	hierarchySQL := `with recursive tree as (
		select id, parent_id, name, 1 as depth from category where parent_id is null
		union all
		select c.id, c.parent_id, c.name, tree.depth + 1
		from tree cross join lateral (
			select id, parent_id, name from category
			where parent_id = tree.id
			order by parent_id, sort_order limit 2
		) as c
		where tree.depth < 3
	) select id, name, depth from tree order by depth, id, name`

	// 1. Verify the recursive-CTE hierarchy plan contains Apply with parallel_apply on.
	planRows := tk.MustQuery("explain format='plan_tree' " + hierarchySQL).Rows()
	foundApply := false
	for _, row := range planRows {
		if strings.Contains(fmt.Sprintf("%v", row), "Apply") {
			foundApply = true
			break
		}
	}
	require.True(t, foundApply, "plan must contain Apply for the LATERAL join")

	// 2. Use a flat (non-recursive) LATERAL query to verify EXPLAIN ANALYZE reports
	//    Concurrency > 1 in the Apply execution info.
	//    (EXPLAIN ANALYZE + FORMAT='plan_tree' is unsupported; use default tabular format.)
	flatSQL := `select p.id, c.id as child_id from category p cross join lateral (
		select id from category where parent_id = p.id order by sort_order limit 2
	) as c where p.parent_id is null`

	analyzeRows := tk.MustQuery("explain analyze " + flatSQL).Rows()
	foundConcurrency := false
	for _, row := range analyzeRows {
		line := fmt.Sprintf("%v", row)
		if strings.Contains(line, "Apply") && strings.Contains(line, "Concurrency:") {
			idx := strings.Index(line, "Concurrency:")
			if idx >= 0 {
				rest := line[idx+len("Concurrency:"):]
				var n int
				if _, err := fmt.Sscanf(rest, "%d", &n); err == nil && n > 1 {
					foundConcurrency = true
				}
			}
			break
		}
	}
	require.True(t, foundConcurrency, "EXPLAIN ANALYZE must report Concurrency > 1 for Apply on flat LATERAL when parallel_apply is on")

	// 3. Correctness: the recursive-CTE hierarchy query must produce the same result
	//    with parallel_apply=on as with parallel_apply=off.  The recursive body's Apply
	//    is intentionally serialized (see logical_cte.go) to avoid a known bug where
	//    grandchildren are dropped when parallel apply runs inside the CTE iteration.
	tk.MustExec("set tidb_enable_parallel_apply=off")
	serialRows := tk.MustQuery(hierarchySQL).Rows()

	tk.MustExec("set tidb_enable_parallel_apply=on")
	parallelRows := tk.MustQuery(hierarchySQL).Rows()

	require.Equal(t, serialRows, parallelRows, "recursive CTE + LATERAL must produce the same result regardless of parallel_apply setting")
}

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
		testKit.MustQuery(`explain format = 'plan_tree' select  t3.a from t t3 where (select /*+ inl_join(t1) */  count(*) from t t1 join t t2 on t1.a=t2.a and t1.b>t3.b);`).
			Check(testkit.Rows(
				`Projection root  test.t.a`,
				`└─Apply root  CARTESIAN inner join`,
				`  ├─TableReader(Build) root  data:TableFullScan`,
				`  │ └─TableFullScan cop[tikv] table:t3 keep order:false, stats:pseudo`,
				`  └─Selection(Probe) root  Column`,
				`    └─HashAgg root  funcs:count(1)->Column`,
				"      └─IndexJoin root  inner join, inner:IndexLookUp, outer key:test.t.a, inner key:test.t.a, equal cond:eq(test.t.a, test.t.a)",
				"        ├─IndexReader(Build) root  index:IndexFullScan",
				"        │ └─IndexFullScan cop[tikv] table:t2, index:idx(a) keep order:false, stats:pseudo",
				"        └─IndexLookUp(Probe) root  ",
				"          ├─Selection(Build) cop[tikv]  not(isnull(test.t.a))",
				"          │ └─IndexRangeScan cop[tikv] table:t1, index:idx(a) range: decided by [eq(test.t.a, test.t.a)], keep order:false, stats:pseudo",
				"          └─Selection(Probe) cop[tikv]  gt(test.t.b, test.t.b)",
				"            └─TableRowIDScan cop[tikv] table:t1 keep order:false, stats:pseudo"))
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
		rows := tk.MustQuery("explain format = 'plan_tree' " + sql).Rows()
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
	tk.MustQuery("explain format = 'plan_tree' select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a")
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
