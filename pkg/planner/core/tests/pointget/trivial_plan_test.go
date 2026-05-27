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

package pointget

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// requireTrivialPlan asserts that the most recently executed statement did (or
// did not) use the trivial plan fast path, by checking @@last_plan_from_trivial.
func requireTrivialPlan(t *testing.T, tk *testkit.TestKit, expected bool) {
	t.Helper()
	val := "0"
	if expected {
		val = "1"
	}
	tk.MustQuery("select @@last_plan_from_trivial").Check(testkit.Rows(val))
}

func TestTrivialPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Table with no secondary indexes — eligible for trivial plan.
	tk.MustExec("drop table if exists t_simple")
	tk.MustExec("create table t_simple(a int primary key, b int, c varchar(32))")
	tk.MustExec("insert into t_simple values(1, 10, 'hello'), (2, 20, 'world'), (3, 30, 'foo')")

	// Basic full table scan: SELECT *
	rows := tk.MustQuery("select * from t_simple").Sort().Rows()
	require.Len(t, rows, 3)
	require.Equal(t, "1", rows[0][0])
	require.Equal(t, "hello", rows[0][2])
	requireTrivialPlan(t, tk, true)

	// SELECT with specific columns.
	rows = tk.MustQuery("select a, c from t_simple").Sort().Rows()
	require.Len(t, rows, 3)
	require.Equal(t, "foo", rows[2][1])
	requireTrivialPlan(t, tk, true)

	// SELECT with column alias.
	rows = tk.MustQuery("select a as id, b as val from t_simple").Sort().Rows()
	require.Len(t, rows, 3)
	requireTrivialPlan(t, tk, true)

	// SELECT with reordered columns (verifies column mapping correctness).
	rows = tk.MustQuery("select c, a from t_simple").Sort().Rows()
	require.Len(t, rows, 3)
	// Sorted by first column (c): "foo"→3, "hello"→1, "world"→2
	require.Equal(t, "foo", rows[0][0])
	require.Equal(t, "3", rows[0][1])
	require.Equal(t, "hello", rows[1][0])
	require.Equal(t, "1", rows[1][1])
	requireTrivialPlan(t, tk, true)

	// SELECT with duplicate column references.
	rows = tk.MustQuery("select a, a from t_simple").Sort().Rows()
	require.Len(t, rows, 3)
	require.Equal(t, rows[0][0], rows[0][1])
	requireTrivialPlan(t, tk, true)

	// Verify EXPLAIN shows a table scan plan (EXPLAIN uses normal optimizer).
	tk.MustQuery("explain format = 'brief' select * from t_simple").Check(testkit.Rows(
		"TableReader 10000.00 root  data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:t_simple keep order:false, stats:pseudo",
	))
}

func TestTrivialPlanFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_idx, t_part, t_gen")

	// Partitioned table — should NOT use trivial plan.
	tk.MustExec("create table t_part(a int primary key, b int) partition by hash(a) partitions 4")
	tk.MustExec("insert into t_part values(1, 10), (2, 20)")
	rows := tk.MustQuery("select * from t_part").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, false)

	// Table with virtual generated column — should NOT use trivial plan.
	tk.MustExec("create table t_gen(a int primary key, b int, c int as (a + b))")
	tk.MustExec("insert into t_gen(a, b) values(1, 10), (2, 20)")
	rows = tk.MustQuery("select * from t_gen").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, false)

	tk.MustExec("drop table if exists t_no_idx")
	tk.MustExec("create table t_no_idx(a int primary key, b int)")
	tk.MustExec("insert into t_no_idx values(1, 10), (2, 20)")

	// Query with ORDER BY — should NOT use trivial plan.
	rows = tk.MustQuery("select * from t_no_idx order by b").Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, false)

	// Query with LIMIT — should NOT use trivial plan.
	rows = tk.MustQuery("select * from t_no_idx limit 1").Rows()
	require.Len(t, rows, 1)
	requireTrivialPlan(t, tk, false)

	// Query with DISTINCT — should NOT use trivial plan.
	rows = tk.MustQuery("select distinct b from t_no_idx").Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, false)

	// Query with expression in SELECT — should NOT use trivial plan
	// (buildSchemaFromFields returns nil for non-column expressions).
	rows = tk.MustQuery("select a + 1 from t_no_idx").Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, false)

	// WHERE on the PK column — could become a range scan, so bail.
	rows = tk.MustQuery("select * from t_no_idx where a > 1").Rows()
	require.Len(t, rows, 1)
	requireTrivialPlan(t, tk, false)

	// WHERE with a subquery — not supported on the trivial path.
	rows = tk.MustQuery("select * from t_no_idx where b > (select min(b) from t_no_idx)").Rows()
	require.Len(t, rows, 1)
	requireTrivialPlan(t, tk, false)

	// sql_select_limit — should NOT use trivial plan.
	tk.MustExec("set @@sql_select_limit = 1")
	rows = tk.MustQuery("select * from t_no_idx").Rows()
	require.Len(t, rows, 1)
	requireTrivialPlan(t, tk, false)
	tk.MustExec("set @@sql_select_limit = default")

	// Dirty transaction (uncommitted writes) — should NOT use trivial plan.
	tk.MustExec("begin")
	tk.MustExec("insert into t_no_idx values(3, 30)")
	rows = tk.MustQuery("select * from t_no_idx").Sort().Rows()
	require.Len(t, rows, 3)
	requireTrivialPlan(t, tk, false)
	tk.MustExec("rollback")
}

// TestTrivialPlanWithSecondaryIndex covers cases where the table has secondary
// indexes but the per-query gate determines that no index could compete with
// a table scan. The trivial fast path should still apply.
func TestTrivialPlanWithSecondaryIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_idx, t_two_idx")
	tk.MustExec("create table t_idx(a int primary key, b int, c varchar(32), index idx_c(c))")
	tk.MustExec("insert into t_idx values(1, 10, 'foo'), (2, 20, 'bar')")

	// SELECT * needs every column, so idx_c is not covering and not referenced
	// by any predicate → table scan provably wins → trivial plan applies.
	rows := tk.MustQuery("select * from t_idx").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, true)

	// SELECT a only — idx_c does not contain a, so still not covering.
	rows = tk.MustQuery("select a from t_idx").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, true)

	// SELECT a, b — idx_c is not covering (b is missing) → trivial plan.
	rows = tk.MustQuery("select a, b from t_idx").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, true)

	// SELECT only the indexed column — idx_c covers {c} → fall back to the
	// full optimizer because a covering index scan could win.
	rows = tk.MustQuery("select c from t_idx").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, false)

	// WHERE references an indexed column → idx_c could produce a range scan.
	rows = tk.MustQuery("select * from t_idx where c = 'foo'").Rows()
	require.Len(t, rows, 1)
	requireTrivialPlan(t, tk, false)

	// Multiple secondary indexes — same logic applies per index.
	tk.MustExec("create table t_two_idx(a int primary key, b int, c int, d int, index idx_b(b), index idx_c(c))")
	tk.MustExec("insert into t_two_idx values(1, 10, 100, 1000), (2, 20, 200, 2000)")

	// SELECT * — no index covers, no index referenced → trivial plan.
	rows = tk.MustQuery("select * from t_two_idx").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, true)

	// WHERE on b (indexed) — bail.
	rows = tk.MustQuery("select * from t_two_idx where b > 5").Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, false)

	// WHERE on d (non-indexed) — trivial plan.
	rows = tk.MustQuery("select * from t_two_idx where d > 1500").Rows()
	require.Len(t, rows, 1)
	requireTrivialPlan(t, tk, true)
}

// TestTrivialPlanWithWhere covers WHERE clauses that don't enable any index.
func TestTrivialPlanWithWhere(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_w")
	tk.MustExec("create table t_w(a int primary key, b int, c varchar(32), d int)")
	tk.MustExec("insert into t_w values(1, 10, 'apple', 100), (2, 20, 'banana', 200), (3, 30, 'cherry', 300)")

	// Simple non-indexed equality.
	rows := tk.MustQuery("select * from t_w where b = 20").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "2", rows[0][0])
	requireTrivialPlan(t, tk, true)

	// Inequality predicate.
	rows = tk.MustQuery("select a, c from t_w where d >= 200").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, true)

	// Compound predicate referencing two non-indexed columns.
	rows = tk.MustQuery("select * from t_w where b > 10 and c <> 'banana'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "3", rows[0][0])
	requireTrivialPlan(t, tk, true)

	// WHERE references a column not in the SELECT list — the scan must include
	// it for filtering, then a Projection strips it from the output.
	rows = tk.MustQuery("select a from t_w where c = 'banana'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "2", rows[0][0])
	require.Len(t, rows[0], 1)
	requireTrivialPlan(t, tk, true)

	// Disjunction across non-indexed columns.
	rows = tk.MustQuery("select b, d from t_w where b = 10 or d = 300").Sort().Rows()
	require.Len(t, rows, 2)
	requireTrivialPlan(t, tk, true)

	// IS NULL on a non-indexed column.
	tk.MustExec("insert into t_w values(4, null, 'date', 400)")
	rows = tk.MustQuery("select a from t_w where b is null").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "4", rows[0][0])
	requireTrivialPlan(t, tk, true)

	// Unknown column name — fall through; the full planner produces the error.
	err := tk.ExecToErr("select * from t_w where nonexistent = 1")
	require.Error(t, err)
}

// TestTrivialPlanEnumPredicate guards against a regression where a WHERE
// expression returning an enum (or other ETString hybrid type) was treated as
// always-false because the trivial path skipped the bool-cast that
// PlanBuilder.buildSelection applies. With the cast in place, the runtime
// reads the enum index for truthiness so all four rows match.
func TestTrivialPlanEnumPredicate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_enum")
	tk.MustExec("create table t_enum(e enum('c', 'b', 'a'))")
	tk.MustExec("insert into t_enum values ('a'),('b'),('a'),('b')")

	rows := tk.MustQuery("select e from t_enum where if(e>1, e, e)").Sort().Rows()
	require.Len(t, rows, 4)
	requireTrivialPlan(t, tk, true)

	rows = tk.MustQuery("select e from t_enum where case e when 1 then e else e end").Sort().Rows()
	require.Len(t, rows, 4)
	requireTrivialPlan(t, tk, true)
}
