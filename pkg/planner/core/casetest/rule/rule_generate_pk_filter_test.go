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

package rule

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGeneratePKFilterHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1")
		// Use a single secondary index so the optimizer can choose between it and the table path.
		tk.MustExec("create table t1(a int primary key clustered, b int, c int, key idx_b(b))")
		tk.MustExec("insert into t1 values(1,1,1),(2,2,2),(3,3,3),(4,1,4),(5,2,5)")
		tk.MustExec("analyze table t1")

		// pk_filter(t1, idx_b) generates PK range conditions from idx_b.
		// b=1 matches rows with a=1 and a=4, so PK filter produces pk >= 1 AND pk <= 4.
		// The optimizer may choose the table path (TableRangeScan range:[1,4]) or
		// the index path depending on cost estimation. Verify correct results.
		tk.MustQuery("select /*+ pk_filter(t1, idx_b) */ * from t1 where b = 1 order by a limit 10").Check(testkit.Rows("1 1 1", "4 1 4"))

		// Verify the optimization is active: with session var + LIMIT, it should
		// produce a Point_Get when PK range collapses to a single value.
		tk.MustExec("drop table if exists t1b")
		tk.MustExec("create table t1b(a int primary key clustered, b int, key idx_b(b))")
		tk.MustExec("insert into t1b values(10,1),(20,2),(30,3)")
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t1b, idx_b) */ * from t1b where b = 1 order by a limit 10")
		planStr := planToString(plan.Rows())
		t.Logf("Point PK filter plan:\n%s", planStr)
		// b=1 matches only a=10, so MIN=MAX=10, PK filter collapses to point.
		require.Contains(t, planStr, "Point_Get", "expected Point_Get when PK filter collapses to single value")
	})
}

func TestGeneratePKFilterSessionVar(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t2")
		tk.MustExec("create table t2(a int primary key clustered, b int, key idx_b(b))")
		tk.MustExec("insert into t2 values(1,1),(2,2),(3,3)")

		// Session variable ON - b=1 matches only row a=1, so MIN=MAX=1.
		// The PK filter range collapses to a Point_Get.
		tk.MustExec("set tidb_opt_generate_pk_filter = ON")
		plan := tk.MustQuery("explain format='brief' select * from t2 where b = 1 order by a limit 10")
		planStr := planToString(plan.Rows())
		require.Contains(t, planStr, "Point_Get", "expected Point_Get with session var ON (PK filter collapsed to point)")

		// Session variable OFF - should use IndexLookUp on idx_b, not PK-bounded scan
		tk.MustExec("set tidb_opt_generate_pk_filter = OFF")
		plan = tk.MustQuery("explain format='brief' select * from t2 where b = 1 order by a limit 10")
		planStr = planToString(plan.Rows())
		require.NotContains(t, planStr, "Point_Get", "should NOT generate Point_Get with session var OFF")
	})
}

func TestGeneratePKFilterCompositePK(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t3")
		tk.MustExec("create table t3(a int, b int, c int, d int, primary key(a,b) clustered, key idx_c(c))")
		tk.MustExec("insert into t3 values(1,1,1,1),(2,2,2,2),(3,3,3,3)")

		// Composite PK, no PK col has filter - should generate on first PK col (a).
		// c=1 matches only row (1,1,1,1), so MIN(a)=1, MAX(a)=1 → range:[1,1].
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where c = 1 order by a, b limit 10")
		planStr := planToString(plan.Rows())
		require.Contains(t, planStr, "TableRangeScan", "expected TableRangeScan with PK filter on first PK col")
		require.Contains(t, planStr, "range:[1,1]", "expected PK filter range on first PK col")

		// Composite PK, first PK col has EQ filter - should generate on second PK col (b).
		// a=1 AND c=1 matches only row (1,1,1,1), so MIN(b)=1, MAX(b)=1.
		// Combined with a=1, this produces a Point_Get on clustered PK (a=1,b=1).
		plan = tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where a = 1 and c = 1 order by b limit 10")
		planStr = planToString(plan.Rows())
		require.Contains(t, planStr, "Point_Get", "expected Point_Get with PK filter on second PK col")

		// All PK cols have EQ filter - should NOT generate filter (no target PK col).
		plan = tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where a = 1 and b = 2 and c = 1 order by d limit 10")
		planStr = planToString(plan.Rows())
		require.Contains(t, planStr, "Point_Get", "expected Point_Get when all PK cols have EQ (no PK filter needed)")
	})
}

func TestGeneratePKFilterNoPKTable(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_nopk")
		tk.MustExec("create table t_nopk(a int, b int, key idx_b(b))")
		tk.MustExec("insert into t_nopk values(1,1),(2,2),(3,3)")

		// Table with no clustered PK — hint should be silently ignored.
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t_nopk, idx_b) */ * from t_nopk where b = 1 order by a limit 10")
		planStr := planToString(plan.Rows())
		t.Logf("No-PK table plan:\n%s", planStr)
		// Should NOT produce a TableRangeScan with PK filter bounds.
		require.NotContains(t, planStr, "TableRangeScan", "should not generate PK filter on a table without clustered PK")

		// Verify query returns correct results.
		tk.MustQuery("select /*+ pk_filter(t_nopk, idx_b) */ * from t_nopk where b = 1 order by a limit 10").Check(testkit.Rows("1 1"))
	})
}

func TestGeneratePKFilterNoWhere(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_nowhere")
		tk.MustExec("create table t_nowhere(a int primary key clustered, b int, c int, key idx_b(b))")
		tk.MustExec("insert into t_nowhere values(1,1,1),(2,2,2),(3,3,3)")

		// No WHERE clause — PushedDownConds is empty, hint should be ignored.
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t_nowhere, idx_b) */ * from t_nowhere order by a limit 10")
		planStr := planToString(plan.Rows())
		t.Logf("No WHERE plan:\n%s", planStr)
		// Should use a simple TableFullScan or Limit+TableFullScan, not a PK filter bounded range.
		require.NotContains(t, planStr, "ge(test.t_nowhere.a", "should not generate PK filter without WHERE clause")

		// Verify query returns correct results.
		tk.MustQuery("select /*+ pk_filter(t_nowhere, idx_b) */ * from t_nowhere order by a limit 10").Check(testkit.Rows("1 1 1", "2 2 2", "3 3 3"))
	})
}

func TestGeneratePKFilterZeroRows(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_zero")
		tk.MustExec("create table t_zero(a int primary key clustered, b int, c int, key idx_b(b))")
		tk.MustExec("insert into t_zero values(1,1,1),(2,2,2),(3,3,3)")

		// b=999 matches zero rows — MIN/MAX subquery returns NULL, PK filter should be skipped.
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t_zero, idx_b) */ * from t_zero where b = 999 order by a limit 10")
		planStr := planToString(plan.Rows())
		t.Logf("Zero rows plan:\n%s", planStr)

		// Verify query returns empty result (no crash).
		tk.MustQuery("select /*+ pk_filter(t_zero, idx_b) */ * from t_zero where b = 999 order by a limit 10").Check(testkit.Rows())
	})
}

func TestGeneratePKFilterPKNotInSelect(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_nosel")
		tk.MustExec("create table t_nosel(a int primary key clustered, b int, c int, key idx_b(b))")
		tk.MustExec("insert into t_nosel values(1,1,10),(2,2,20),(3,3,30),(4,1,40),(5,2,50)")

		// SELECT b, c — PK column 'a' is not in the select list.
		// The PK filter rule should re-add 'a' to the schema for the ge/le conditions.
		// Verify query returns correct results (PK filter correctly handles pruned PK col).
		tk.MustQuery("select /*+ pk_filter(t_nosel, idx_b) */ b, c from t_nosel where b = 1 order by a limit 10").Check(testkit.Rows("1 10", "1 40"))
	})
}

func TestGeneratePKFilterInvalidIndex(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_inv")
		tk.MustExec("create table t_inv(a int primary key clustered, b int, key idx_b(b))")
		tk.MustExec("insert into t_inv values(1,1),(2,2),(3,3)")

		// Invalid index name — should produce a warning.
		tk.MustQuery("explain format='brief' select /*+ pk_filter(t_inv, idx_nonexistent) */ * from t_inv where b = 1 order by a limit 10")
		tk.MustQuery("show warnings").CheckContain("pk_filter")
		tk.MustQuery("show warnings").CheckContain("idx_nonexistent")
	})
}

func TestGeneratePKFilterUnmatchedTable(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_unm")
		tk.MustExec("create table t_unm(a int primary key clustered, b int, key idx_b(b))")
		tk.MustExec("insert into t_unm values(1,1),(2,2),(3,3)")

		// Hint references a non-existent table — should produce a warning.
		tk.MustQuery("explain format='brief' select /*+ pk_filter(no_such_table, idx_b) */ * from t_unm where b = 1 order by a limit 10")
		tk.MustQuery("show warnings").CheckContain("pk_filter")
		tk.MustQuery("show warnings").CheckContain("no_such_table")
	})
}

func TestGeneratePKFilterExplicitDB(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_edb")
		tk.MustExec("create table t_edb(a int primary key clustered, b int, key idx_b(b))")
		tk.MustExec("insert into t_edb values(1,1),(2,2),(3,3),(4,1),(5,2)")

		// pk_filter with explicit database name — verify correct results.
		tk.MustQuery("select /*+ pk_filter(test.t_edb, idx_b) */ * from t_edb where b = 1 order by a limit 10").Check(testkit.Rows("1 1", "4 1"))
	})
}

func TestGeneratePKFilterWithoutLimit(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_nolimit")
		tk.MustExec("create table t_nolimit(a int primary key clustered, b int, c int, key idx_b(b))")
		tk.MustExec("insert into t_nolimit values(1,1,10),(2,2,20),(3,3,30),(4,1,40),(5,2,50)")

		// PK filter without LIMIT — should still generate PK filter.
		// b=1 matches rows with a=1 and a=4, so PK filter produces pk >= 1 AND pk <= 4.
		// Verify correct results regardless of which path the optimizer picks.
		tk.MustQuery("select /*+ pk_filter(t_nolimit, idx_b) */ * from t_nolimit where b = 1").Sort().Check(testkit.Rows("1 1 10", "4 1 40"))

		// Verify the PK filter activates without LIMIT when MIN=MAX collapses to a point.
		tk.MustExec("drop table if exists t_nolimit2")
		tk.MustExec("create table t_nolimit2(a int primary key clustered, b int, key idx_b(b))")
		tk.MustExec("insert into t_nolimit2 values(10,1),(20,2),(30,3)")
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t_nolimit2, idx_b) */ * from t_nolimit2 where b = 1")
		planStr := planToString(plan.Rows())
		t.Logf("No LIMIT point PK filter plan:\n%s", planStr)
		// b=1 matches only a=10, so MIN=MAX=10, PK range collapses to point → Point_Get.
		require.Contains(t, planStr, "Point_Get", "expected Point_Get when PK filter collapses to single value without LIMIT")
	})
}

func TestGeneratePKFilterSourceIndexPreserved(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_preserved")
		tk.MustExec("create table t_preserved(a int primary key clustered, b int, c int, key idx_b(b), key idx_c(c))")
		tk.MustExec("insert into t_preserved values(1,1,1),(2,2,2),(3,3,3),(4,1,4),(5,2,5)")

		// The source index (idx_b) should still be present in access paths
		// after PK filter generation — it should NOT be removed.
		// Use use_index hint to force idx_b usage and verify it's still available.
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t_preserved, idx_b), use_index(t_preserved, idx_b) */ * from t_preserved where b = 1")
		planStr := planToString(plan.Rows())
		t.Logf("Source index preserved plan:\n%s", planStr)
		// idx_b should still be a valid access path.
		require.Contains(t, planStr, "idx_b", "expected source index idx_b to still be in access paths")

		// Verify query returns correct results.
		tk.MustQuery("select /*+ pk_filter(t_preserved, idx_b), use_index(t_preserved, idx_b) */ * from t_preserved where b = 1").Sort().Check(testkit.Rows("1 1 1", "4 1 4"))
	})
}

func TestGeneratePKFilterIndexPath(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_idxpath")
		tk.MustExec("create table t_idxpath(a int primary key clustered, b int, c int, key idx_b(b), key idx_c(c))")
		tk.MustExec("insert into t_idxpath values(1,1,1),(2,2,2),(3,3,3),(4,1,4),(5,2,5)")

		// PK filter from idx_c should be applied to idx_b path as IndexFilter.
		// c=1 matches only a=1, so PK filter produces pk >= 1 AND pk <= 1.
		// Verify correct results when using idx_b with PK filter from idx_c.
		tk.MustQuery("select /*+ pk_filter(t_idxpath, idx_c) */ * from t_idxpath where b > 0 and c = 1 order by b limit 10").Check(testkit.Rows("1 1 1"))

		// PK filter should NOT be self-applied to idx_c (source index).
		// c=1 matches only a=1, so MIN=MAX=1 → PK filter collapses to a Point_Get.
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t_idxpath, idx_c) */ * from t_idxpath where c = 1 order by a limit 10")
		planStr := planToString(plan.Rows())
		t.Logf("Self-application check plan:\n%s", planStr)
		// Verify PK filter is active (Point_Get from range collapse).
		require.Contains(t, planStr, "Point_Get", "expected Point_Get when PK filter collapses to single value")
	})
}

func TestGeneratePKFilterTableOnlyHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_tblonly")
		tk.MustExec("create table t_tblonly(a int primary key clustered, b int, c int, key idx_b(b), key idx_c(c))")
		tk.MustExec("insert into t_tblonly values(1,1,1),(2,2,2),(3,3,3),(4,1,4),(5,2,5)")

		// pk_filter(t_tblonly) without index names — should use all qualifying indexes.
		// b=1 matches a=1 and a=4, so PK filter produces pk >= 1 AND pk <= 4.
		tk.MustQuery("select /*+ pk_filter(t_tblonly) */ * from t_tblonly where b = 1 order by a limit 10").Check(testkit.Rows("1 1 1", "4 1 4"))
	})
}

func TestGeneratePKFilterNoArgs(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_noargs")
		tk.MustExec("create table t_noargs(a int primary key clustered, b int, c int, key idx_b(b), key idx_c(c))")
		tk.MustExec("insert into t_noargs values(1,1,1),(2,2,2),(3,3,3),(4,1,4),(5,2,5)")

		// pk_filter() with no arguments — should apply to all qualifying tables.
		// b=1 matches a=1 and a=4, so PK filter produces pk >= 1 AND pk <= 4.
		tk.MustQuery("select /*+ pk_filter() */ * from t_noargs where b = 1 order by a limit 10").Check(testkit.Rows("1 1 1", "4 1 4"))
	})
}

func planToString(rows [][]any) string {
	var sb strings.Builder
	for _, row := range rows {
		for _, col := range row {
			sb.WriteString(col.(string))
			sb.WriteString(" ")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
