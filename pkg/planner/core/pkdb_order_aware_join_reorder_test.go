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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestOrderAwareJoinReorderSchedulesOnlyWithFeatureEnabled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table oa_left (id int primary key, join_id int, order_col int, key idx_order(order_col))")
	tk.MustExec("create table oa_right (id int primary key)")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = on")
	tk.MustExec("set tidb_opt_enable_advanced_join_reorder = on")
	tk.MustQuery("explain select l.id from oa_left l join oa_right r on l.join_id = r.id order by l.order_col desc limit 5")
	require.True(t, tk.Session().GetSessionVars().StmtCtx.NeedAlternativeLogicalPlanOrderAwareJoinReorder())
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = off")
	tk.MustQuery("explain select l.id from oa_left l join oa_right r on l.join_id = r.id order by l.order_col desc limit 5")
	require.False(t, tk.Session().GetSessionVars().StmtCtx.NeedAlternativeLogicalPlanOrderAwareJoinReorder())
}

func TestOrderAwareJoinReorderDoesNotReevaluateBuildTimeSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table oa_side_effect_left (id int primary key, join_id int, order_col int, key idx_order(order_col))")
	tk.MustExec("create table oa_side_effect_right (id int primary key)")
	tk.MustExec("insert into oa_side_effect_left values (1, 1, 1), (2, 2, 2)")
	tk.MustExec("insert into oa_side_effect_right values (1), (2)")
	tk.MustExec("create sequence oa_side_effect_seq start 1 nocache")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = on")
	tk.MustExec("set tidb_opt_enable_advanced_join_reorder = on")

	sql := "select l.id, (select nextval(oa_side_effect_seq)) from oa_side_effect_left l join oa_side_effect_right r on l.join_id = r.id order by l.order_col desc limit 1"
	tk.MustQuery(sql).Check(testkit.Rows("2 1"))
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	// The order-aware candidate is present, but the alternative round must be
	// rejected because the baseline build evaluated a SQL-visible subquery.
	require.True(t, stmtCtx.NeedAlternativeLogicalPlanOrderAwareJoinReorder())
	require.False(t, stmtCtx.CanTryAlternativeLogicalPlanRound())
	tk.MustQuery("select lastval(oa_side_effect_seq)").Check(testkit.Rows("1"))
}

func TestOrderAwareJoinReorderRequiresSingleValueIndexPrefix(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table oa_prefix_left (id int primary key, join_id int, prefix_col int, order_col int, key idx_prefix_order(prefix_col, order_col))")
	tk.MustExec("create table oa_prefix_right (id int primary key)")
	tk.MustExec("insert into oa_prefix_left values (1, 1, 1, 10), (2, 2, 1, 20), (3, 3, 2, 30)")
	tk.MustExec("insert into oa_prefix_right values (1), (2), (3)")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = on")
	tk.MustExec("set tidb_opt_enable_advanced_join_reorder = on")

	for _, test := range []struct {
		name      string
		predicate string
		candidate bool
		rows      []string
	}{
		{name: "equality", predicate: "l.prefix_col = 1", candidate: true, rows: []string{"2", "1"}},
		// A single-value IN is rewritten to EQ by the expression rewriter.
		{name: "single value IN", predicate: "l.prefix_col in (1)", candidate: true, rows: []string{"2", "1"}},
		{name: "multiple value IN", predicate: "l.prefix_col in (1, 2)", candidate: false, rows: []string{"3", "2"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sql := "select l.id from oa_prefix_left l join oa_prefix_right r on l.join_id = r.id where " +
				test.predicate + " order by l.order_col desc limit 2"
			tk.MustQuery("explain " + sql)
			require.Equal(t, test.candidate, tk.Session().GetSessionVars().StmtCtx.NeedAlternativeLogicalPlanOrderAwareJoinReorder())
			tk.MustQuery(sql).Check(testkit.Rows(test.rows...))
		})
	}
}
