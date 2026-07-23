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

package correlated

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func TestAlternativeLogicalPlansTrySecondRoundPerApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareAlternativeLogicalPlanReviewTables(tk)

	scalarSubquerySQL := "select alt_pick_t1.a, (select count(*) from alt_pick_t2 join alt_pick_t3 on alt_pick_t2.a = alt_pick_t3.a where alt_pick_t2.a = alt_pick_t1.a) as cnt from alt_pick_t1 order by alt_pick_t1.a"
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	offPlan := tk.MustQuery("explain format = 'brief' " + scalarSubquerySQL).Rows()
	require.False(t, rowsContainText(offPlan, "Apply"), rowsText(offPlan))
	tk.MustQuery(scalarSubquerySQL).Check(testkit.Rows("1 4", "2 4"))

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	scalarPlan := tk.MustQuery("explain format = 'brief' " + scalarSubquerySQL).Rows()
	require.True(t, rowsContainText(scalarPlan, "Apply"), rowsText(scalarPlan))
	tk.MustQuery(scalarSubquerySQL).Check(testkit.Rows("1 4", "2 4"))

	combinedSQL := "select alt_pick_t1.a, (select count(*) from alt_pick_t2 join alt_pick_t3 on alt_pick_t2.a = alt_pick_t3.a where alt_pick_t2.a = alt_pick_t1.a) as cnt from alt_pick_t1 where exists (select 1 from alt_skip_t2 where alt_skip_t2.a = alt_pick_t1.a and alt_skip_t2.b > 0) order by alt_pick_t1.a"
	combinedPlan := tk.MustQuery("explain format = 'brief' " + combinedSQL).Rows()
	require.True(t, rowsContainText(combinedPlan, "Apply"), rowsText(combinedPlan))
}

func TestAlternativeLogicalPlansSkipSecondRoundForIndexJoinVariants(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareAlternativeLogicalPlanReviewTables(tk)
	tk.MustExec("set @@tidb_enable_index_merge_join=on")

	for _, tt := range alternativeLogicalPlanIndexJoinVariantCases() {
		t.Run(tt.name, func(t *testing.T) {
			explainSQL := "explain format = 'brief' " + tt.sql

			tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
			onPlan := tk.MustQuery(explainSQL).Rows()
			// The decorrelated first round already produced a same-order index-join
			// candidate, so the no-decorrelate second round must be skipped and the Apply
			// alternative must not show up in the chosen plan.
			require.False(t, rowsContainText(onPlan, "Apply"), rowsText(onPlan))
			if tt.wantInPlan != "" {
				require.True(t, rowsContainText(onPlan, tt.wantInPlan), rowsText(onPlan))
			}
			// Dropping the marker call from this variant's Attach2Task flips
			// NeedAlternativeLogicalPlanRound back to true (and the plan above back to Apply).
			stmtCtx := tk.Session().GetSessionVars().StmtCtx
			require.False(t, stmtCtx.NeedAlternativeLogicalPlanRound())
		})
	}
}

func TestAlternativeLogicalPlansPreparedPlanCacheKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareAlternativeLogicalPlanReviewTables(tk)
	tk.MustExec("set @@tidb_enable_prepared_plan_cache=on")
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	tk.MustExec("set @min_a=0")

	sql := "select alt_pick_t1.a, (select count(*) from alt_pick_t2 join alt_pick_t3 on alt_pick_t2.a = alt_pick_t3.a where alt_pick_t2.a = alt_pick_t1.a) as cnt from alt_pick_t1 where alt_pick_t1.a >= ? order by alt_pick_t1.a"
	literalSQL := strings.Replace(sql, "?", "0", 1)
	offPlan := tk.MustQuery("explain format = 'brief' " + literalSQL).Rows()
	require.False(t, rowsContainText(offPlan, "Apply"), rowsText(offPlan))
	tk.MustExec("prepare alt_plan_cache_stmt from '" + sql + "'")

	// Warm the prepared plan cache under tidb_opt_enable_alternative_logical_plans=off.
	tk.MustQuery("execute alt_plan_cache_stmt using @min_a").Check(testkit.Rows("1 4", "2 4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute alt_plan_cache_stmt using @min_a").Check(testkit.Rows("1 4", "2 4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	onPlan := tk.MustQuery("explain format = 'brief' " + literalSQL).Rows()
	require.True(t, rowsContainText(onPlan, "Apply"), rowsText(onPlan))

	// The first execute after flipping the optimizer switch must miss the old
	// cache entry. Without the switch in the cache key, this would incorrectly
	// hit the plan built with tidb_opt_enable_alternative_logical_plans=off.
	tk.MustQuery("execute alt_plan_cache_stmt using @min_a").Check(testkit.Rows("1 4", "2 4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	tk.MustQuery("execute alt_plan_cache_stmt using @min_a").Check(testkit.Rows("1 4", "2 4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestAlternativeLogicalPlansDoNotReevaluateBuildTimeSubqueries(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareAlternativeLogicalPlanReviewTables(tk)
	tk.MustExec("drop sequence if exists alt_side_effect_seq")
	tk.MustExec("create sequence alt_side_effect_seq start 1 nocache")
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")

	sql := "select alt_pick_t1.a, (select nextval(alt_side_effect_seq)), (select count(*) from alt_pick_t2 join alt_pick_t3 on alt_pick_t2.a = alt_pick_t3.a where alt_pick_t2.a = alt_pick_t1.a) as cnt from alt_pick_t1 order by alt_pick_t1.a limit 1"

	tk.MustQuery(sql).Check(testkit.Rows("1 1 4"))
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	require.False(t, stmtCtx.NeedAlternativeLogicalPlanRound())
	tk.MustQuery("select lastval(alt_side_effect_seq)").Check(testkit.Rows("1"))

	tk.MustQuery("explain format = 'brief' " + sql)
	stmtCtx = tk.Session().GetSessionVars().StmtCtx
	require.False(t, stmtCtx.NeedAlternativeLogicalPlanRound())
	tk.MustQuery("select lastval(alt_side_effect_seq)").Check(testkit.Rows("2"))
}

// TestAlternativeLogicalPlansFirstRoundWinsAfterSecondRound covers the case the
// other tests miss: the no-decorrelate second round actually runs, but the
// decorrelated first round still wins the cost comparison. It also pins the
// PlanColumnID/PlanID restore -- when the second round runs and loses, the
// session id allocators must be restored to the winning first round's high-water
// marks, matching the single-round build.
func TestAlternativeLogicalPlansFirstRoundWinsAfterSecondRound(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists alt_r1win_outer, alt_r1win_inner")
	tk.MustExec("create table alt_r1win_outer(a int primary key)")
	// The inner correlation column is deliberately NOT indexed, so decorrelation
	// cannot produce a same-order index join. That keeps NeedAlternativeLogicalPlanRound
	// true (the alternative no-decorrelate round runs), while the large outer makes the
	// decorrelated hash-join plan cheaper than re-running the Apply once per outer row,
	// so the first (decorrelated) round wins the cost comparison.
	tk.MustExec("create table alt_r1win_inner(a int, c int)")

	outerVals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		outerVals = append(outerVals, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert into alt_r1win_outer values " + strings.Join(outerVals, ","))
	innerVals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		innerVals = append(innerVals, fmt.Sprintf("(%d, %d)", i%500, i))
	}
	tk.MustExec("insert into alt_r1win_inner values " + strings.Join(innerVals, ","))
	tk.MustExec("analyze table alt_r1win_outer, alt_r1win_inner")

	// Two correlated scalar subqueries: both decorrelate, and neither has an index
	// on the correlation column, so the decorrelated first round allocates strictly
	// more plan column ids than the no-decorrelate second round. That size gap is
	// what makes the PlanColumnID restore observable end-to-end below.
	sql := "select alt_r1win_outer.a, (select count(*) from alt_r1win_inner where alt_r1win_inner.a = alt_r1win_outer.a) as cnt, (select max(alt_r1win_inner.c) from alt_r1win_inner where alt_r1win_inner.a = alt_r1win_outer.a) as mx from alt_r1win_outer order by alt_r1win_outer.a"

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	offPlan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	// The default decorrelated plan keeps no Apply.
	require.False(t, rowsContainText(offPlan, "Apply"), rowsText(offPlan))
	offResult := tk.MustQuery(sql).Rows()
	offColumnID := tk.Session().GetSessionVars().PlanColumnID.Load()

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	onPlan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	// The first (decorrelated) round still wins on cost, so the chosen plan has no Apply.
	require.False(t, rowsContainText(onPlan, "Apply"), rowsText(onPlan))
	onResult := tk.MustQuery(sql).Rows()
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	// Prove the alternative no-decorrelate round actually ran (was not skipped): a
	// decorrelated Apply exists but has no same-order index-join equivalent.
	require.True(t, stmtCtx.NeedAlternativeLogicalPlanRound())
	onColumnID := tk.Session().GetSessionVars().PlanColumnID.Load()

	// Correctness and plan stability: when the first round wins, enabling the feature
	// must not change the chosen plan or its results.
	require.Equal(t, offResult, onResult)
	require.Equal(t, rowsText(offPlan), rowsText(onPlan))
	// Regression for the id-allocator restore: after the second round ran and lost,
	// the session PlanColumnID must be restored to the winning first round's
	// high-water mark, matching the single-round build. Without the restore it would
	// be left at the smaller second-round mark, below UniqueIDs still live in the plan.
	require.Equal(t, offColumnID, onColumnID)
}

func TestAlternativeLogicalPlansMatchSingleRoundSemantics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists alt_diff_outer, alt_diff_inner")
	tk.MustExec("create table alt_diff_outer(id int primary key, k int)")
	tk.MustExec("create table alt_diff_inner(k int, v int)")
	tk.MustExec("insert into alt_diff_outer values (1, null), (2, 1), (3, 2), (4, 3)")
	tk.MustExec("insert into alt_diff_inner values (null, 10), (1, null), (1, 10), (1, 10), (2, 20)")
	tk.MustExec("analyze table alt_diff_outer, alt_diff_inner")

	cases := []struct {
		name string
		sql  string
	}{
		{name: "scalar", sql: "select o.id, (select max(i.v) from alt_diff_inner i where i.k <=> o.k) from alt_diff_outer o order by o.id"},
		{name: "exists", sql: "select o.id from alt_diff_outer o where exists (select 1 from alt_diff_inner i where i.k <=> o.k) order by o.id"},
		{name: "not exists anti semi", sql: "select o.id from alt_diff_outer o where not exists (select 1 from alt_diff_inner i where i.k <=> o.k) order by o.id"},
		{name: "in", sql: "select o.id from alt_diff_outer o where o.id in (select i.v from alt_diff_inner i where i.k <=> o.k) order by o.id"},
		{name: "not in", sql: "select o.id from alt_diff_outer o where o.id not in (select i.v from alt_diff_inner i where i.k <=> o.k) order by o.id"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
			offRows := tk.MustQuery(tt.sql).Rows()

			tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
			onRows := tk.MustQuery(tt.sql).Rows()
			require.Equal(t, offRows, onRows)

			stmtCtx := tk.Session().GetSessionVars().StmtCtx
			require.True(t, stmtCtx.NeedAlternativeLogicalPlanRound())
		})
	}
}

func TestAlternativeLogicalPlanRoundErrorHandling(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table alt_round_error_outer(a int primary key)")
	tk.MustExec("create table alt_round_error_inner(a int)")
	tk.MustExec("insert into alt_round_error_outer values (1), (2)")
	tk.MustExec("insert into alt_round_error_inner values (1), (1), (2)")
	tk.MustExec("analyze table alt_round_error_outer, alt_round_error_inner")
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")

	// The unindexed correlation column makes the first round decorrelate the Apply
	// without producing a same-order index join, so the alternative round is
	// guaranteed to be scheduled.
	sql := "select alt_round_error_outer.a, (select count(*) from alt_round_error_inner where alt_round_error_inner.a = alt_round_error_outer.a) as cnt from alt_round_error_outer order by alt_round_error_outer.a"
	tk.MustQuery("explain format = 'brief' " + sql)
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.NeedAlternativeLogicalPlanRound())

	failpointName := "github.com/pingcap/tidb/pkg/planner/mockAlternativeLogicalPlanRoundError"

	t.Run("fallback ordinary error", func(t *testing.T) {
		testfailpoint.Enable(t, failpointName, `return("ordinary")`)
		plan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
		require.False(t, rowsContainText(plan, "Apply"), rowsText(plan))
		tk.MustQuery(sql).Check(testkit.Rows("1 2", "2 1"))
	})

	t.Run("do not fallback memory error", func(t *testing.T) {
		testfailpoint.Enable(t, failpointName, `return("memory")`)
		err := tk.ExecToErr(sql)
		require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err), err)
	})
}

type alternativeLogicalPlanIndexJoinVariantCase struct {
	name string
	sql  string
	// wantInPlan, when set, must appear in the chosen plan. It is intentionally
	// empty for variants where the statement signals provide stronger coverage
	// than the final plan shape.
	wantInPlan string
}

func alternativeLogicalPlanIndexJoinVariantCases() []alternativeLogicalPlanIndexJoinVariantCase {
	return []alternativeLogicalPlanIndexJoinVariantCase{
		{
			name: "IndexJoin",
			sql:  "select alt_pick_t1.a from alt_pick_t1 where exists (select 1 from alt_skip_t2 where alt_skip_t2.a = alt_pick_t1.a and alt_skip_t2.b > 0) order by alt_pick_t1.a",
		},
		{
			// For this semi join the only index-join variant generated is the index hash
			// join, so only PhysicalIndexHashJoin.Attach2Task can mark the equivalent
			// candidate. The index hash join also wins, so it shows up in the chosen plan.
			name:       "IndexHashJoin",
			sql:        "select /*+ inl_hash_join(alt_skip_t2@sel_2) */ alt_pick_t1.a from alt_pick_t1 where exists (select 1 from alt_skip_t2 where alt_skip_t2.a = alt_pick_t1.a and alt_skip_t2.b > 0) order by alt_pick_t1.a",
			wantInPlan: "IndexHashJoin",
		},
		{
			// The INL_MERGE_JOIN hint forces the decorrelated Apply candidate through
			// PhysicalIndexMergeJoin.Attach2Task. We assert via the signal rather than the
			// final plan shape on purpose: the shapes where IndexMergeJoin wins can also
			// enumerate a plain PhysicalIndexJoin candidate, which would mask a regression
			// in the merge-join marker.
			name: "IndexMergeJoin",
			sql:  "select /*+ inl_merge_join(alt_skip_t2@sel_2) no_index_join(alt_skip_t2@sel_2) no_index_hash_join(alt_skip_t2@sel_2) */ alt_pick_t1.a from alt_pick_t1 where exists (select 1 from alt_skip_t2 where alt_skip_t2.a = alt_pick_t1.a and alt_skip_t2.b > 0) order by alt_pick_t1.a",
		},
	}
}

func TestAlternativeLogicalPlansPreserveUnusedViewHintWarnings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareAlternativeLogicalPlanReviewTables(tk)

	sql := "select /*+ qb_name(qb_unused, missing_view) */ alt_pick_t1.a, (select count(*) from alt_pick_t2 join alt_pick_t3 on alt_pick_t2.a = alt_pick_t3.a where alt_pick_t2.a = alt_pick_t1.a) as cnt from alt_pick_t1 order by alt_pick_t1.a"
	expectedWarning := "The qb_name hint qb_unused is unused, please check whether the table list in the qb_name hint qb_unused is correct"

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	tk.MustQuery(sql).Check(testkit.Rows("1 4", "2 4"))
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.NeedAlternativeLogicalPlanRound())

	warnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, warnings, 1)
	require.Len(t, warnings[0], 3)
	require.Equal(t, expectedWarning, warnings[0][2])
}

func TestAlternativeLogicalPlansTraceMatchesSelectedPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareAlternativeLogicalPlanReviewTables(tk)
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")

	sql := "select alt_pick_t1.a from alt_pick_t1 where exists (select 1 from alt_pick_t2 join alt_pick_t3 on alt_pick_t2.a = alt_pick_t3.a where alt_pick_t2.a = alt_pick_t1.a) order by alt_pick_t1.a"

	explainRows := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	explainHasApply := rowsContainText(explainRows, "Apply")
	traceTypes := readOptimizerTracePlanTypes(t, tk, sql)
	traceHasApply := slicesContainText(traceTypes, "Apply")

	require.Equal(t, explainHasApply, traceHasApply, "optimizer trace final plan should describe the selected plan\nexplain:\n%s\ntrace types:\n%s", rowsText(explainRows), strings.Join(traceTypes, "\n"))
}

func TestAlternativeLogicalPlansCETraceMatchesSelectedRound(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareAlternativeLogicalPlanReviewTables(tk)

	sql := "select alt_pick_t1.a from alt_pick_t1 where exists (select 1 from alt_pick_t2 join alt_pick_t3 on alt_pick_t2.a = alt_pick_t3.a where alt_pick_t2.a = alt_pick_t1.a) order by alt_pick_t1.a"

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	offPlan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	offTrace := tk.MustQuery("trace plan target = 'estimation' " + sql).Rows()[0][0].(string)

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	onPlan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	onTrace := tk.MustQuery("trace plan target = 'estimation' " + sql).Rows()[0][0].(string)

	require.Equal(t, rowsText(offPlan), rowsText(onPlan))
	require.JSONEq(t, offTrace, onTrace)
}

func TestAlternativeLogicalPlansDebugTraceMatchesSelectedRound(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareAlternativeLogicalPlanReviewTables(tk)

	sql := "select alt_pick_t1.a from alt_pick_t1 where exists (select 1 from alt_pick_t2 join alt_pick_t3 on alt_pick_t2.a = alt_pick_t3.a where alt_pick_t2.a = alt_pick_t1.a) order by alt_pick_t1.a"

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	offPlan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	offTrace := readOptimizerDebugTrace(t, tk, sql)

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	onPlan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	onTrace := readOptimizerDebugTrace(t, tk, sql)
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.NeedAlternativeLogicalPlanRound())

	require.Equal(t, rowsText(offPlan), rowsText(onPlan))
	require.JSONEq(t, offTrace, onTrace)
}

func prepareAlternativeLogicalPlanReviewTables(tk *testkit.TestKit) {
	tk.MustExec("drop table if exists alt_pick_t1, alt_pick_t2, alt_pick_t3, alt_skip_t2")
	tk.MustExec("create table alt_pick_t1(a int primary key)")
	tk.MustExec("create table alt_pick_t2(a int, b int, key idx_a(a))")
	tk.MustExec("create table alt_pick_t3(a int, c int, key idx_a(a))")
	tk.MustExec("create table alt_skip_t2(a int, b int, key idx_a(a))")
	tk.MustExec("insert into alt_pick_t1 values (1), (2)")
	tk.MustExec("insert into alt_skip_t2 values (1, 1), (1, 2), (2, 3)")

	vals2 := make([]string, 0, 200)
	vals3 := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		vals2 = append(vals2, fmt.Sprintf("(%d, %d)", i%100, i))
		vals3 = append(vals3, fmt.Sprintf("(%d, %d)", i%100, i))
	}
	tk.MustExec("insert into alt_pick_t2 values " + strings.Join(vals2, ","))
	tk.MustExec("insert into alt_pick_t3 values " + strings.Join(vals3, ","))
	tk.MustExec("analyze table alt_pick_t1, alt_pick_t2, alt_pick_t3, alt_skip_t2")
}

func readOptimizerTracePlanTypes(t *testing.T, tk *testkit.TestKit, sql string) []string {
	rows := tk.MustQuery("trace plan " + sql).Rows()
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	fileName, ok := rows[0][0].(string)
	require.True(t, ok)

	data := readOptimizerTraceJSON(t, filepath.Join(domain.GetOptimizerTraceDirName(), fileName))
	var trace struct {
		FinalPlan []struct {
			TP string `json:"type"`
		} `json:"final"`
	}
	require.NoError(t, json.Unmarshal(data, &trace))
	types := make([]string, 0, len(trace.FinalPlan))
	for _, plan := range trace.FinalPlan {
		types = append(types, plan.TP)
	}
	return types
}

func readOptimizerDebugTrace(t *testing.T, tk *testkit.TestKit, sql string) string {
	rows := tk.MustQuery("trace plan target = 'debug' " + sql).Rows()
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	trace, ok := rows[0][0].(string)
	require.True(t, ok)
	return trace
}

func readOptimizerTraceJSON(t *testing.T, filePath string) []byte {
	reader, err := zip.OpenReader(filePath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
		require.NoError(t, os.Remove(filePath))
	}()
	for _, f := range reader.File {
		if f.Name != "trace.json" {
			continue
		}
		rc, err := f.Open()
		require.NoError(t, err)
		defer rc.Close()
		data, err := io.ReadAll(rc)
		require.NoError(t, err)
		return data
	}
	require.FailNow(t, "trace.json not found in optimizer trace zip")
	return nil
}

func slicesContainText(values []string, needle string) bool {
	for _, value := range values {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
