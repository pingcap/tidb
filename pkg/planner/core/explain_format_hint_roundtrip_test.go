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
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

const explainFormatHintDerivedTableSQL = `select *
from t1
join t2 on t1.a = t2.a
join (
    select a, b, row_number() over(partition by a order by b desc) as rn
    from t3
) dt on t2.a = dt.a
where dt.rn = 1`

const explainFormatHintNestedDerivedTableSQL = `select *
from t1
join (
    select a, b
    from (select a, b from t2 limit 100) d2
) dt on t1.a = dt.a
join t3 on dt.a = t3.a`

const explainFormatHintFlattenedNestedDerivedTableSQL = `select *
from t1
join (
    select *
    from (select * from t2) d2
) dt on t1.a = dt.a
join t3 on dt.a = t3.a`

const explainFormatHintNestedDerivedLeadingSQL = `select *
from (select * from (select * from t1) x1) o1
join (select * from t2) o2 on o1.a = o2.a
join (select * from t3) o3 on o2.a = o3.a
join (select * from t4) o4 on o3.a = o4.a`

const explainFormatHintMixedQueryBlockLeadingSQL = `select /*+ leading(t2, t1) */ *
from t1
join t2 on t1.a = t2.a
where t2.b in (
    select a
    from t3
    where t3.b = 1
)
and t1.c = 1`

const explainFormatHintMultipleMixedQueryBlockLeadingSQL = `update /*+ leading(t2, t1, t4@sel_2) */ t1
join t2 on t1.a = t2.a
set t1.a = 1
where t2.b in (
    select a
    from t3
    where t3.b = 1
)
and t1.b in (
    select b
    from t4
    where t4.b = 1
)
and t1.c = 1`

func prepareExplainFormatHintDerivedTableAliasTestKit(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int, b int, key(a))")
	tk.MustExec("create table t2(a int, b int, key(a))")
	tk.MustExec("create table t3(a int, b int, key(a, b))")
	return tk
}

func prepareExplainFormatHintMixedLeadingTestKit(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.MustExec("create table t1(pk int, a int, b int, c int, primary key(pk), key k_a(a), key k_bc(b, c))")
	tk.MustExec("create table t2(a int, b int, c int, key k_a(a), key k_bc(b, c))")
	tk.MustExec("create table t3(a int, b int, c int, key k_a(a), key k_bc(b, c))")
	tk.MustExec("create table t4(a int, b int, c int, key k_a(a), key k_bc(b, c))")
	return tk
}

func TestExplainFormatHintRecoverableForDerivedTableAlias(t *testing.T) {
	tk := prepareExplainFormatHintDerivedTableAliasTestKit(t)

	t.Run("simple derived table", func(t *testing.T) {
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintDerivedTableSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`test`.`t1`, `test`.`t2`, `test`.`dt`)")

		replayedHints := tk.MustQuery(fmt.Sprintf("explain format='hint' select /*+ %s */ * from t1 join t2 on t1.a = t2.a join (select a, b, row_number() over(partition by a order by b desc) as rn from t3) dt on t2.a = dt.a where dt.rn = 1", hints)).Rows()[0][0]
		require.Contains(t, replayedHints, "leading(`test`.`t1`, `test`.`t2`, `test`.`dt`)")
		require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
	})

	t.Run("nested derived table keeps outer alias on replay", func(t *testing.T) {
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintNestedDerivedTableSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")

		replayedHints := tk.MustQuery(fmt.Sprintf("explain format='hint' select /*+ %s */ * from t1 join (select a, b from (select a, b from t2 limit 100) d2) dt on t1.a = dt.a join t3 on dt.a = t3.a", hints)).Rows()[0][0]
		require.Contains(t, replayedHints, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")
		require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
	})

	t.Run("flattened nested derived table keeps outer alias on replay", func(t *testing.T) {
		t.Run("advanced_reorder_ON", func(t *testing.T) {
			tk.MustExec("set tidb_opt_enable_advanced_join_reorder=ON")

			hints := tk.MustQuery("explain format='hint' " + explainFormatHintFlattenedNestedDerivedTableSQL).Rows()[0][0]
			require.Contains(t, hints, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")
			require.NotContains(t, hints, "`test`.`d2`")

			replayedHints := tk.MustQuery(fmt.Sprintf("explain format='hint' select /*+ %s */ * from t1 join (select * from (select * from t2) d2) dt on t1.a = dt.a join t3 on dt.a = t3.a", hints)).Rows()[0][0]
			require.Contains(t, replayedHints, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")
			require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())

			baselinePlan := fmt.Sprint(tk.MustQuery("explain format='brief' " + explainFormatHintFlattenedNestedDerivedTableSQL).Rows())
			reversedPlan := fmt.Sprint(tk.MustQuery("explain format='brief' select /*+ leading(t3, dt, t1) */ * from t1 join (select * from (select * from t2) d2) dt on t1.a = dt.a join t3 on dt.a = t3.a").Rows())
			require.NotEqual(t, baselinePlan, reversedPlan)
			require.Contains(t, reversedPlan, "eq(test.t2.a, test.t1.a)")
			require.Contains(t, reversedPlan, "eq(test.t3.a, test.t2.a)")
			require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())

			tk.MustQuery("explain format='brief' select /*+ leading(t1, d2@sel_2, t3) */ * from t1 join (select * from (select * from t2) d2) dt on t1.a = dt.a join t3 on dt.a = t3.a")
			require.Contains(
				t,
				fmt.Sprint(tk.Session().GetSessionVars().StmtCtx.GetWarnings()),
				"leading hint is inapplicable",
			)
		})
		tk.MustExec("set tidb_opt_enable_advanced_join_reorder=default")
	})

	t.Run("flattened nested derived table keeps outer alias on replay with legacy reorder", func(t *testing.T) {
		tk.MustExec("set tidb_opt_enable_advanced_join_reorder=OFF")
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintFlattenedNestedDerivedTableSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")
		require.NotContains(t, hints, "`test`.`d2`")
		replayedHints := tk.MustQuery(fmt.Sprintf("explain format='hint' select /*+ %s */ * from t1 join (select * from (select * from t2) d2) dt on t1.a = dt.a join t3 on dt.a = t3.a", hints)).Rows()[0][0]
		require.Contains(t, replayedHints, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")
	})

	t.Run("nested derived leading resolves every outer alias on replay", func(t *testing.T) {
		tk.MustExec("create table t4(a int, b int, key(a))")

		hints := tk.MustQuery("explain format='hint' " + explainFormatHintNestedDerivedLeadingSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`test`.`o1`, `test`.`o2`, `test`.`o3`, `test`.`o4`)")
		require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())

		replayedHints := tk.MustQuery(fmt.Sprintf("explain format='hint' select /*+ %s */ * from (select * from (select * from t1) x1) o1 join (select * from t2) o2 on o1.a = o2.a join (select * from t3) o3 on o2.a = o3.a join (select * from t4) o4 on o3.a = o4.a", hints)).Rows()[0][0]
		require.Contains(t, replayedHints, "leading(`test`.`o1`, `test`.`o2`, `test`.`o3`, `test`.`o4`)")
		require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
	})

	t.Run("flattened multi-table derived alias is not emitted as duplicate leading operands", func(t *testing.T) {
		const sql = `select *
				from t1
				join (
					select /*+ merge_join(t2, t3) */ t2.a, t2.b
					from t2 join t3 on t2.a = t3.a
				) dt on t1.a = dt.a`
		for _, advanced := range []string{"ON", "OFF"} {
			t.Run("advanced_reorder_"+advanced, func(t *testing.T) {
				tk.MustExec("set tidb_opt_enable_advanced_join_reorder=" + advanced)

				hints := tk.MustQuery("explain format='hint' " + sql).Rows()[0][0]
				require.Contains(t, hints, "leading(`test`.`t1`, `test`.`t2`@`sel_2`, `test`.`t3`@`sel_2`)")
				require.NotRegexp(t, `leading\([^)]*dt[^)]*dt`, hints)
				require.NotRegexp(t, `hash_join_(build|probe)\([^)]*dt`, hints)
				require.Contains(t, hints, "merge_join(`test`.`dt`)")

				leadingHint := ""
				for _, candidate := range regexp.MustCompile(`(?i)leading\([^)]*\)`).FindAllString(fmt.Sprint(hints), -1) {
					if regexp.MustCompile(`(?i)@[^,)]*sel_2`).MatchString(candidate) {
						leadingHint = candidate
						break
					}
				}
				require.NotEmpty(t, leadingHint)
				tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
				replayedHints := tk.MustQuery(fmt.Sprintf("explain format='hint' select /*+ %s */ * from t1 join (select t2.a, t2.b from t2 join t3 on t2.a = t3.a) dt on t1.a = dt.a", leadingHint)).Rows()[0][0]
				require.Contains(t, replayedHints, "leading(`test`.`t1`, `test`.`t2`@`sel_2`, `test`.`t3`@`sel_2`)")
				// Cross-QB LEADING operands (t2@sel_2, t3@sel_2) cannot be proven
				// matched while the outer query block is still being built, so the
				// conservative unmatched-table warning fires even though join reorder
				// replays the order. The source branch (and master's own
				// join_reorder_suite expectations) emits the same warning, so only
				// assert that no warning other than this known one appears.
				warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
				for _, warning := range warnings {
					require.Contains(t, warning.Err.Error(),
						"There are no matching table names for (t2, t3) in optimizer hint /*+ LEADING(t1, t2, t3) */")
				}
			})
		}
		tk.MustExec("set tidb_opt_enable_advanced_join_reorder=default")
	})
}

func TestExplainFormatHintDoesNotExtendLeadingAcrossQueryBlockOwners(t *testing.T) {
	tk := prepareExplainFormatHintMixedLeadingTestKit(t)

	t.Run("single inner query block", func(t *testing.T) {
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintMixedQueryBlockLeadingSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`t2`, `t1`)")
		require.NotRegexp(t, `leading\(.*leading\(`, hints)
		require.NotContains(t, hints, "leading(`test`.`t2`, `test`.`t1`, `test`.`t3`@`sel_2`)")
	})

	t.Run("multiple inner query blocks", func(t *testing.T) {
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintMultipleMixedQueryBlockLeadingSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`t2`, `t1`, `t4`@`sel_2`)")
		require.NotRegexp(t, `leading\(.*leading\(`, hints)
		require.NotContains(t, hints, "leading(`test`.`t2`, `test`.`t1`, `test`.`t4`@`sel_2`, `test`.`t3`@`sel_1`)")
	})
}

func TestExplainFormatHintIdentityCoverage(t *testing.T) {
	tk := prepareExplainFormatHintDerivedTableAliasTestKit(t)
	tk.MustExec("create table t4(a int, b int, key(a))")

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "self join keeps same-name occurrences distinct",
			sql: `select *
from t1 a
join t1 b on a.a = b.a
join t2 c on b.a = c.a`,
		},
		{
			name: "cte participates without leaking its inner query block",
			sql: `with c as (select * from t2)
select *
from t1
join c on t1.a = c.a
join t3 on c.a = t3.a`,
		},
		{
			name: "outer join with derived table generates replayable leading",
			sql: `select *
from t1
left join (select * from t2) dt on t1.a = dt.a
join t3 on dt.a = t3.a`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseline := tk.MustQuery("explain format='brief' " + test.sql).Rows()
			hints := tk.MustQuery("explain format='hint' " + test.sql).Rows()[0][0]
			hintText := fmt.Sprint(hints)
			require.Contains(t, hintText, "leading(")
			if test.name == "self join keeps same-name occurrences distinct" {
				leadingHint := regexp.MustCompile(`(?i)leading\([^)]*\)`).FindString(hintText)
				require.NotEmpty(t, leadingHint)
				require.Contains(t, leadingHint, "`test`.`a`")
				require.Contains(t, leadingHint, "`test`.`b`")
				require.Contains(t, leadingHint, "`test`.`c`")
				require.NotContains(t, leadingHint, "`test`.`t1`")
			}
			if test.name == "outer join with derived table generates replayable leading" {
				require.Contains(t, hintText, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")
				require.NotContains(t, hintText, "leading(`test`.`t1`, `test`.`t2`")
			}

			// Replaying the generated hint must preserve the actual join order,
			// not merely produce similar EXPLAIN FORMAT='hint' text. CTE expansion
			// may add a harmless Projection wrapper, so compare only Join nodes.
			replayedSQL := "explain format='brief' " + addExplainHints(test.sql, hintText)
			replayed := tk.MustQuery(replayedSQL).Rows()
			require.Equal(t, joinPlanSignature(baseline), joinPlanSignature(replayed))
		})
	}
}

func joinPlanSignature(rows [][]any) []string {
	signature := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) < 5 || !strings.Contains(strings.ToLower(fmt.Sprint(row[0])), "join") {
			continue
		}
		operator := strings.TrimLeft(fmt.Sprint(row[0]), " ├└│─")
		info := normalizeJoinConditions(fmt.Sprint(row[4]))
		signature = append(signature, operator+"|"+info)
	}
	return signature
}

var joinConditionPattern = regexp.MustCompile(`eq\(([^,]+), ([^)]+)\)`)

func normalizeJoinConditions(info string) string {
	return joinConditionPattern.ReplaceAllStringFunc(info, func(condition string) string {
		matches := joinConditionPattern.FindStringSubmatch(condition)
		if len(matches) != 3 || matches[1] <= matches[2] {
			return condition
		}
		return "eq(" + matches[2] + ", " + matches[1] + ")"
	})
}

func addExplainHints(sql, hints string) string {
	lowerSQL := strings.ToLower(sql)
	selectOffset := strings.Index(lowerSQL, "select ")
	if strings.HasPrefix(strings.TrimSpace(lowerSQL), "with") {
		// A CTE may contain an inner SELECT. The final SELECT is the statement
		// whose top-level hints should be replayed.
		selectOffset = strings.LastIndex(lowerSQL, "select ")
	}
	if selectOffset < 0 {
		return sql
	}
	return sql[:selectOffset] + "select /*+ " + hints + " */ " + sql[selectOffset+len("select "):]
}

// queryResultFields runs sql and returns its result fields so tests can assert
// on the table/column name metadata (which testkit.Result does not expose).
func queryResultFields(t *testing.T, tk *testkit.TestKit, sql string) []*resolve.ResultField {
	recordSets, err := tk.Session().Execute(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, recordSets, 1)
	require.NoError(t, recordSets[0].Close())
	return recordSets[0].Fields()
}

// TestCTEMultiConsumerAliasIndependence locks in the alias independence of
// multiple consumers of one CTE (review comment 1): consumers must never
// observe each other's aliases, and a recursive CTE's self-reference alias
// must not leak into the names other consumers see. With the old in-place
// FieldName mutation the self-reference shared objects with the seed, so the
// alias leaked into result metadata and `cte.n` stopped resolving in queries
// that also aliased the CTE.
func TestCTEMultiConsumerAliasIndependence(t *testing.T) {
	tk := prepareExplainFormatHintDerivedTableAliasTestKit(t)
	tk.MustExec("insert into t2 values (1, 1), (2, 2)")

	t.Run("two aliased consumers keep independent names", func(t *testing.T) {
		fields := queryResultFields(t, tk, "with cte as (select 1 as a) select * from cte as x join cte as y on x.a = y.a")
		require.Len(t, fields, 2)
		require.Equal(t, "a", fields[0].ColumnAsName.L)
		require.Equal(t, "a", fields[1].ColumnAsName.L)
		require.Equal(t, "x", fields[0].TableAsName.L)
		require.Equal(t, "y", fields[1].TableAsName.L)
	})

	t.Run("recursive self-reference alias does not leak into the plain consumer", func(t *testing.T) {
		fields := queryResultFields(t, tk, "with recursive cte(n) as (select 1 union all select n + 1 from cte as r where n < 3) select * from cte")
		require.Len(t, fields, 1)
		require.Equal(t, "n", fields[0].ColumnAsName.L)
		require.Equal(t, "cte", fields[0].TableAsName.L)
	})

	t.Run("recursive mixed consumers resolve and keep aliases distinct", func(t *testing.T) {
		// Under the old in-place mutation this failed with
		// `Unknown column 'cte.n' in 'on clause'`.
		tk.MustQuery("with recursive cte(n) as (select 1 union all select n + 1 from cte as r where n < 3) select * from cte join cte as y on cte.n = y.n").
			Check(testkit.Rows("1 1", "2 2", "3 3"))

		fields := queryResultFields(t, tk, "with recursive cte(n) as (select 1 union all select n + 1 from cte as r where n < 3) select * from cte join cte as y on cte.n = y.n")
		require.Len(t, fields, 2)
		require.Equal(t, "cte", fields[0].TableAsName.L)
		require.Equal(t, "y", fields[1].TableAsName.L)
	})

	t.Run("recursive arm keeps its own alias usable", func(t *testing.T) {
		tk.MustQuery("with recursive cte(n) as (select 1 union all select r.n + 1 from cte as r where r.n < 3) select * from cte").
			Check(testkit.Rows("1", "2", "3"))
	})
}

// TestExplainFormatHintCTEMultiConsumerContract turns the conservative
// behavior for multi-consumer CTEs into a contract (review comment 2):
// multi-consumer CTEs are materialized and their inner query blocks are not
// visible at the outer level, so EXPLAIN FORMAT='hint' must not emit LEADING
// for joins over them. The hints that are emitted must replay stably — same
// join plan, no warnings.
func TestExplainFormatHintCTEMultiConsumerContract(t *testing.T) {
	tk := prepareExplainFormatHintDerivedTableAliasTestKit(t)
	tk.MustExec("insert into t1 values (1, 1), (2, 2)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2)")
	tk.MustExec("insert into t3 values (1, 1), (2, 2)")

	shapes := []struct {
		name string
		sql  string
	}{
		{
			name: "non-recursive multi-consumer CTE join",
			sql:  "with cte as (select a from t2) select * from cte as x join cte as y on x.a = y.a",
		},
		{
			name: "non-recursive multi-consumer CTE join with outer base tables",
			sql:  "with cte as (select a from t2) select * from t1 join cte as x on t1.a = x.a join cte as y on x.a = y.a join t3 on y.a = t3.a",
		},
		{
			name: "recursive CTE consumed twice with aliased self-reference",
			sql:  "with recursive cte(n) as (select 1 union all select n + 1 from cte as r where n < 3) select * from cte join cte as y on cte.n = y.n",
		},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			baseline := tk.MustQuery("explain format='brief' " + shape.sql).Rows()

			rows := tk.MustQuery("explain format='hint' " + shape.sql).Rows()
			hints := ""
			if len(rows) > 0 {
				hints = fmt.Sprint(rows[0][0])
			}
			require.NotContains(t, strings.ToLower(hints), "leading(")

			// The remaining hints must replay stably: the join plan is preserved
			// and no warning is raised. A plan with no hints at all (e.g. the
			// recursive CTE) replays as the plain statement — an empty /*+ */
			// comment would itself be a syntax error.
			replaySQL := shape.sql
			if hints != "" {
				replaySQL = addExplainHints(shape.sql, hints)
			}
			tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
			replayed := tk.MustQuery("explain format='brief' " + replaySQL).Rows()
			require.Equal(t, joinPlanSignature(baseline), joinPlanSignature(replayed))
			require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
	}

	// Flip side of the contract: a single-consumer CTE is inlined, its source
	// tables are visible at the outer level, and LEADING is emitted and
	// replayable (see also TestExplainFormatHintIdentityCoverage).
	t.Run("single-consumer CTE still emits leading", func(t *testing.T) {
		hints := fmt.Sprint(tk.MustQuery("explain format='hint' with c as (select * from t2) select * from t1 join c on t1.a = c.a join t3 on c.a = t3.a").Rows()[0][0])
		require.Contains(t, hints, "leading(")
	})
}

// TestViewJoinQueryExecutesWithoutLeadingDiagnostics locks in the scoping of
// the ambiguity diagnostic: LEADING-generation warnings may only appear where
// the user asked for generated hints (EXPLAIN FORMAT='hint'). Ordinary
// execution also generates plan hints — lazily, for statements_summary — and
// must not append warnings to statements that contain no hint at all.
// Regression test: this shape made the mysqltest suite surface
// "Warning 1815 leading hint is not generated ..." after plain SELECTs
// (join, join_outer, derived, select_all, view).
func TestViewJoinQueryExecutesWithoutLeadingDiagnostics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// Auth as a real user so statements_summary records this session's
	// statements: the lazy plan-hint generation runs from there, which is the
	// path that used to leak the LEADING diagnostics into plain statements.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, key(a))")
	tk.MustExec("create table t2(a int, b int, key(a))")
	// A mergeable view joined with itself: after inlining, the join group
	// holds t1 and t2 twice — the ambiguous-identity shape that drops LEADING.
	tk.MustExec("create view v1 as select t1.a, t2.b from t1 join t2 on t1.a = t2.a")
	const sql = "select * from v1 A, v1 B where A.a = B.a"

	tk.MustQuery(sql)
	require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())

	// The same statement under EXPLAIN FORMAT='hint' keeps the diagnostic:
	// the user explicitly asked for hints and is told why LEADING is absent.
	tk.MustQuery("explain format='hint' " + sql)
	require.Contains(t, fmt.Sprint(tk.Session().GetSessionVars().StmtCtx.GetWarnings()),
		"leading hint is not generated because join operands have ambiguous visible identities")
}

func prepareExplainFormatHintCrossDBTestKit(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists db1")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("create database db1")
	tk.MustExec("create database db2")
	tk.MustExec("create table db1.t1(a int, b int, key(a))")
	tk.MustExec("create table db1.t3(a int, b int, key(a, b))")
	tk.MustExec("create table db1.t4(a int, b int, key(a))")
	tk.MustExec("create table db2.t2(a int, b int, key(a))")
	tk.MustExec("create view db2.v as select a, b from db2.t2")
	tk.MustExec("create view db2.vd as select a, rn from (select a, row_number() over(partition by a order by b desc) as rn from db2.t2) d")
	tk.MustExec("use db1")
	return tk
}

// TestExplainFormatHintCrossDBIdentity locks in which database a generated
// qualified identity uses: a derived alias belongs to the statement's current
// database, never to the inner base table's or the view definition's
// database, and the generated LEADING round-trips warning-free. Views are the
// flip side: an inlined view exposes its inner base table under that table's
// own database, while a non-mergeable view is not addressable by a generated
// LEADING at all, and the aliases recorded inside the view body never leak
// outward.
func TestExplainFormatHintCrossDBIdentity(t *testing.T) {
	tk := prepareExplainFormatHintCrossDBTestKit(t)

	extractLeading := func(hints string) string {
		return regexp.MustCompile(`(?i)leading\([^)]*\)`).FindString(hints)
	}

	t.Run("derived alias uses the statement database not the inner table database", func(t *testing.T) {
		const sql = `select *
from t1
join (
    select a, b, row_number() over(partition by a order by b desc) as rn
    from db2.t2
) dt on t1.a = dt.a
join t3 on dt.a = t3.a
where dt.rn = 1`

		baseline := tk.MustQuery("explain format='brief' " + sql).Rows()
		hints := fmt.Sprint(tk.MustQuery("explain format='hint' " + sql).Rows()[0][0])
		require.Equal(t, "leading(`db1`.`t1`, `db1`.`dt`, `db1`.`t3`)", extractLeading(hints))

		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		replayedHints := fmt.Sprint(tk.MustQuery("explain format='hint' " + addExplainHints(sql, hints)).Rows()[0][0])
		require.Contains(t, replayedHints, "leading(`db1`.`t1`, `db1`.`dt`, `db1`.`t3`)")
		require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		replayed := tk.MustQuery("explain format='brief' " + addExplainHints(sql, hints)).Rows()
		require.Equal(t, joinPlanSignature(baseline), joinPlanSignature(replayed))
	})

	t.Run("inlined view exposes its inner base table under that table database", func(t *testing.T) {
		const sql = `select *
from t1
join db2.v on t1.a = db2.v.a
join t3 on db2.v.a = t3.a`

		baseline := tk.MustQuery("explain format='brief' " + sql).Rows()
		hints := fmt.Sprint(tk.MustQuery("explain format='hint' " + sql).Rows()[0][0])
		require.Equal(t, "leading(`db1`.`t1`, `db2`.`t2`, `db1`.`t3`)", extractLeading(hints))

		// The LEADING hint round-trips. Replaying also re-emits use_index hints
		// for the table inlined from the view; those cannot re-match (a known
		// pre-existing limitation of index hints on view-internal tables), so
		// only assert that no warning other than that one appears.
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		replayedHints := fmt.Sprint(tk.MustQuery("explain format='hint' " + addExplainHints(sql, hints)).Rows()[0][0])
		require.Contains(t, replayedHints, "leading(`db1`.`t1`, `db2`.`t2`, `db1`.`t3`)")
		for _, warning := range tk.Session().GetSessionVars().StmtCtx.GetWarnings() {
			require.Contains(t, warning.Err.Error(), "use_index(db2.t2) is inapplicable")
		}
		replayed := tk.MustQuery("explain format='brief' " + addExplainHints(sql, hints)).Rows()
		require.Equal(t, joinPlanSignature(baseline), joinPlanSignature(replayed))
	})

	t.Run("non-mergeable view gets no leading and its inner alias never leaks", func(t *testing.T) {
		const sql = `select *
from t1
join db2.vd on t1.a = db2.vd.a
join t3 on db2.vd.a = t3.a`

		baseline := tk.MustQuery("explain format='brief' " + sql).Rows()
		hints := fmt.Sprint(tk.MustQuery("explain format='hint' " + sql).Rows()[0][0])
		require.Empty(t, extractLeading(hints))
		// The alias recorded inside the view body ("d", qualified by the view
		// definition database) is scoped to the view and must not surface as
		// an outer operand identity.
		require.NotContains(t, hints, "`.`d`")

		// The view's inner blocks are numbered in the view's own query-block
		// sequence, so the replayed use_index hints referring to them cannot
		// resolve in the outer statement's numbering (pre-existing view QB
		// numbering limitation); allow only that warning.
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		replayed := tk.MustQuery("explain format='brief' " + addExplainHints(sql, hints)).Rows()
		require.Equal(t, joinPlanSignature(baseline), joinPlanSignature(replayed))
		for _, warning := range tk.Session().GetSessionVars().StmtCtx.GetWarnings() {
			require.Contains(t, warning.Err.Error(), "unknown query block name")
		}
	})

	t.Run("view inner block colliding with an outer derived offset stays conservative", func(t *testing.T) {
		// The non-mergeable view's inner query blocks are numbered in the
		// view's own sequence, which can numerically collide with the outer
		// statement's offsets (both db1.t4 and db2.t2 sit in "sel_2" here).
		// The collided alias lookup can make the view subtree resolve to
		// another operand's alias (db1.dt), so the group must degrade to "no
		// LEADING" instead of emitting an operand list that cannot replay,
		// and the drop must be explainable via a warning. A full fix needs
		// view-expansion boundaries to be recorded at plan-build time.
		const sql = `select *
from t1
join (
    select a, b, row_number() over(partition by a order by b desc) as rn
    from db1.t4
) dt on t1.a = dt.a
join db2.vd on dt.a = db2.vd.a
join t3 on db2.vd.a = t3.a
where dt.rn = 1`

		baseline := tk.MustQuery("explain format='brief' " + sql).Rows()
		hints := fmt.Sprint(tk.MustQuery("explain format='hint' " + sql).Rows()[0][0])
		require.Empty(t, extractLeading(hints))
		require.Contains(t, fmt.Sprint(tk.Session().GetSessionVars().StmtCtx.GetWarnings()),
			"leading hint is not generated because join operands have ambiguous visible identities")

		replayed := tk.MustQuery("explain format='brief' " + addExplainHints(sql, hints)).Rows()
		require.Equal(t, joinPlanSignature(baseline), joinPlanSignature(replayed))
	})
}
