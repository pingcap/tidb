// Copyright 2026 PingCAP, Inc.
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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestIssue69925 verifies that duplicate GROUP BY expressions are eliminated,
// preventing redundant computation in aggregation. Before the fix, GROUP BY
// items like REPEAT(c6,c1), REPEAT(c6,c1) were kept as-is.
func TestIssue69925(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c6 int, c2 int, c3 int, c4 int)")
	tk.MustExec("insert into t values (1, 10, 100, 1000, 10000), (2, 20, 200, 2000, 20000), (1, 10, 300, 1000, 30000)")

	t.Run("SimpleColumn", func(t *testing.T) {
		rows := tk.MustQuery("select c1, count(*) from t group by c1, c1").Sort().Rows()
		refRows := tk.MustQuery("select c1, count(*) from t group by c1").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("FunctionExpr", func(t *testing.T) {
		rows := tk.MustQuery("select c6 + c1, count(*) from t group by c6 + c1, c6 + c1").Sort().Rows()
		refRows := tk.MustQuery("select c6 + c1, count(*) from t group by c6 + c1").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("Triple", func(t *testing.T) {
		rows := tk.MustQuery("select c1, count(*) from t group by c1, c1, c1").Sort().Rows()
		refRows := tk.MustQuery("select c1, count(*) from t group by c1").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	// Scattered duplicates interleaved with non-duplicate items.
	t.Run("Scattered", func(t *testing.T) {
		rows := tk.MustQuery("select c1, c2, c3, count(*) from t group by c1, c2, c3, c1, c4, c2").Sort().Rows()
		refRows := tk.MustQuery("select c1, c2, c3, count(*) from t group by c1, c2, c3, c4").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("BookendDup", func(t *testing.T) {
		rows := tk.MustQuery("select c1, c2, count(*) from t group by c1, c2, c1").Sort().Rows()
		refRows := tk.MustQuery("select c1, c2, count(*) from t group by c1, c2").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("ConstantExpr", func(t *testing.T) {
		rows := tk.MustQuery("select c1 + 1, count(*) from t group by c1 + 1, c1 + 1").Sort().Rows()
		refRows := tk.MustQuery("select c1 + 1, count(*) from t group by c1 + 1").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("WithFilter", func(t *testing.T) {
		rows := tk.MustQuery("select c1, count(*) from t where c1 > 0 group by c1, c1").Sort().Rows()
		refRows := tk.MustQuery("select c1, count(*) from t where c1 > 0 group by c1").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("WithOrder", func(t *testing.T) {
		rows := tk.MustQuery("select c1, count(*) from t group by c1, c1 order by c1").Rows()
		refRows := tk.MustQuery("select c1, count(*) from t group by c1 order by c1").Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("WithHaving", func(t *testing.T) {
		rows := tk.MustQuery("select c1, count(*) from t group by c1, c1 having count(*) > 0").Sort().Rows()
		refRows := tk.MustQuery("select c1, count(*) from t group by c1 having count(*) > 0").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("PlanMatch", func(t *testing.T) {
		planRows := tk.MustQuery("explain format='brief' select c1, count(*) from t group by c1, c1").Rows()
		refPlanRows := tk.MustQuery("explain format='brief' select c1, count(*) from t group by c1").Rows()
		require.Equal(t, len(refPlanRows), len(planRows), "plan line count")
		for i := range refPlanRows {
			require.Equal(t, fmt.Sprintf("%v", refPlanRows[i][0]), fmt.Sprintf("%v", planRows[i][0]),
				"plan operator at line %d", i)
			require.Equal(t, fmt.Sprintf("%v", refPlanRows[i][4]), fmt.Sprintf("%v", planRows[i][4]),
				"plan operator info at line %d", i)
		}
	})
}

// TestIssue69925_Commutative verifies that CanonicalHashCode-based dedup
// handles commutative operators (+/*) where a+b and b+a are equivalent.
func TestIssue69925_Commutative(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1, 10), (2, 20), (1, 30)")

	t.Run("Plus", func(t *testing.T) {
		rows := tk.MustQuery("select a+b, count(*) from t group by a+b, b+a").Sort().Rows()
		refRows := tk.MustQuery("select a+b, count(*) from t group by a+b").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("Mul", func(t *testing.T) {
		rows := tk.MustQuery("select a*b, count(*) from t group by a*b, b*a").Sort().Rows()
		refRows := tk.MustQuery("select a*b, count(*) from t group by a*b").Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	t.Run("MixedCommutAndNonCommut", func(t *testing.T) {
		rows := tk.MustQuery("select a+b, a-b, count(*) from t group by a+b, b+a, a-b").Sort().Rows()
		refRows := tk.MustQuery("select a+b, a-b, count(*) from t group by a+b, a-b").Sort().Rows()
		require.Equal(t, refRows, rows)
	})
}

// TestIssue69925_Null verifies that dedup does not change NULL grouping semantics.
func TestIssue69925_Null(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int)")
	tk.MustExec("insert into t values (1), (NULL), (NULL), (2), (1)")

	rows := tk.MustQuery("select c1, count(*) from t group by c1, c1 order by c1").Rows()
	refRows := tk.MustQuery("select c1, count(*) from t group by c1 order by c1").Rows()
	require.Equal(t, refRows, rows)

	nullCount := tk.MustQuery("select count(*) from t where c1 is null").Rows()
	groupedNull := tk.MustQuery("select count(*) from t group by c1, c1 having c1 is null").Rows()
	require.Equal(t, fmt.Sprintf("%v", nullCount[0][0]), fmt.Sprintf("%v", groupedNull[0][0]))
}

// TestIssue69925_Join verifies dedup in multi-table JOIN queries.
func TestIssue69925_Join(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("create table t2 (c1 int, c3 int)")
	tk.MustExec("insert into t1 values (1, 10), (2, 20), (1, 30)")
	tk.MustExec("insert into t2 values (1, 100), (2, 200)")

	t.Run("SimpleJoin", func(t *testing.T) {
		rows := tk.MustQuery(`
			select t1.c1, count(*)
			from t1 join t2 on t1.c1 = t2.c1
			group by t1.c1, t1.c1
		`).Sort().Rows()
		refRows := tk.MustQuery(`
			select t1.c1, count(*)
			from t1 join t2 on t1.c1 = t2.c1
			group by t1.c1
		`).Sort().Rows()
		require.Equal(t, refRows, rows)
	})

	// t1.c1 and t2.c1 have different UniqueIDs. Dedup must NOT merge them.
	// t1.c1 repeated 3 times → removed; t2.c1 stays distinct.
	t.Run("SameNameDiffTable", func(t *testing.T) {
		rows := tk.MustQuery(`
			select t1.c1, t2.c1, count(*)
			from t1 join t2 on t1.c1 = t2.c1
			group by t1.c1, t1.c1, t1.c1, t2.c1
		`).Sort().Rows()
		refRows := tk.MustQuery(`
			select t1.c1, t2.c1, count(*)
			from t1 join t2 on t1.c1 = t2.c1
			group by t1.c1, t2.c1
		`).Sort().Rows()
		require.Equal(t, refRows, rows)
	})
}

// TestIssue69925_WithRollup verifies that ROLLUP preserves duplicate GROUP BY
// items as separate grouping sets, producing the expected number of rows.
func TestIssue69925_WithRollup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c6 int, c2 int)")
	tk.MustExec("insert into t values (1, 10, 100), (2, 20, 200), (1, 10, 300)")

	// GROUP BY c1, c1, c6 WITH ROLLUP produces 4 grouping sets:
	// (c1,c1,c6), (c1,c1), (c1), () → 2 + 2 + 2 + 1 = 7 rows
	t.Run("RollupDup", func(t *testing.T) {
		rows := tk.MustQuery("select c1, c6, count(*) from t group by c1, c1, c6 with rollup").Sort().Rows()
		require.Equal(t, 7, len(rows))
		last := rows[len(rows)-1]
		require.Equal(t, "<nil>", fmt.Sprintf("%v", last[0]))
		require.Equal(t, "<nil>", fmt.Sprintf("%v", last[1]))
		require.Equal(t, "3", fmt.Sprintf("%v", last[2]))
	})

	// GROUP BY c1, c1, c1, c6 WITH ROLLUP produces 5 grouping sets:
	// (c1,c1,c1,c6), (c1,c1,c1), (c1,c1), (c1), () → 2 + 2 + 2 + 2 + 1 = 9 rows
	t.Run("RollupTriple", func(t *testing.T) {
		rows := tk.MustQuery("select c1, c6, count(*) from t group by c1, c1, c1, c6 with rollup").Sort().Rows()
		require.Equal(t, 9, len(rows))
		last := rows[len(rows)-1]
		require.Equal(t, "<nil>", fmt.Sprintf("%v", last[0]))
		require.Equal(t, "<nil>", fmt.Sprintf("%v", last[1]))
		require.Equal(t, "3", fmt.Sprintf("%v", last[2]))
	})

	t.Run("RollupNoDup", func(t *testing.T) {
		rows := tk.MustQuery("select c1, c6, count(*) from t group by c1, c6 with rollup").Sort().Rows()
		require.Equal(t, 5, len(rows))
	})
}

// TestIssue69925_PlanOperatorInfo verifies the EXPLAIN output for deduped
// GROUP BY queries is identical to the non-duplicate reference.
func TestIssue69925_PlanOperatorInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert into t values (1, 10), (2, 20), (1, 30)")

	queries := []struct {
		name    string
		deduped string
		ref     string
	}{
		{"ColumnDup", "select c1, count(*) from t group by c1, c1", "select c1, count(*) from t group by c1"},
		{"FunctionDup", "select c1+c2, count(*) from t group by c1+c2, c1+c2", "select c1+c2, count(*) from t group by c1+c2"},
		{"CommutativeDup", "select c1+c2, count(*) from t group by c1+c2, c2+c1", "select c1+c2, count(*) from t group by c1+c2"},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			planRows := tk.MustQuery("explain format='brief' " + q.deduped).Rows()
			refPlanRows := tk.MustQuery("explain format='brief' " + q.ref).Rows()

			require.Equal(t, len(refPlanRows), len(planRows), "plan line count")
			for i := range refPlanRows {
				require.Equal(t, fmt.Sprintf("%v", refPlanRows[i][0]), fmt.Sprintf("%v", planRows[i][0]),
					"plan operator at line %d", i)
				require.Equal(t, fmt.Sprintf("%v", refPlanRows[i][4]), fmt.Sprintf("%v", planRows[i][4]),
					"plan operator info at line %d", i)
			}
		})
	}
}

// TestIssue69925_FirstOccurrenceOrder verifies dedup preserves the order
// of first occurrence for GROUP BY items.
func TestIssue69925_FirstOccurrenceOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int, c3 int, c4 int, c5 int)")
	tk.MustExec("insert into t values (1, 10, 100, 1000, 10000), (2, 20, 200, 2000, 20000), (1, 10, 100, 1000, 30000)")

	rows := tk.MustQuery("select c3, c1, c2, count(*) from t group by c3, c1, c2, c3, c1 order by c3, c1, c2").Rows()
	refRows := tk.MustQuery("select c3, c1, c2, count(*) from t group by c3, c1, c2 order by c3, c1, c2").Rows()
	require.Equal(t, refRows, rows)

	plan := tk.MustQuery("explain format='brief' select c3, c1, c2, count(*) from t group by c3, c1, c2, c3, c1").Rows()
	refPlan := tk.MustQuery("explain format='brief' select c3, c1, c2, count(*) from t group by c3, c1, c2").Rows()
	require.Equal(t, len(refPlan), len(plan))
	for i := range plan {
		require.Equal(t, fmt.Sprintf("%v", refPlan[i][0]), fmt.Sprintf("%v", plan[i][0]))
		require.Equal(t, fmt.Sprintf("%v", refPlan[i][4]), fmt.Sprintf("%v", plan[i][4]))
	}

	hashAggInfo := ""
	for _, row := range plan {
		op := fmt.Sprintf("%v", row[0])
		if strings.Contains(op, "HashAgg") || strings.Contains(op, "StreamAgg") {
			hashAggInfo = fmt.Sprintf("%v", row[4])
		}
	}
	if hashAggInfo != "" {
		c1Count := strings.Count(hashAggInfo, "c1")
		require.LessOrEqual(t, c1Count, 1, "c1 should appear at most once in group-by info")
	}
}

// TestIssue69925_NoGroupBy ensures queries without GROUP BY are unaffected.
func TestIssue69925_NoGroupBy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	rows := tk.MustQuery("select count(*) from t").Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, "3", fmt.Sprintf("%v", rows[0][0]))
}
