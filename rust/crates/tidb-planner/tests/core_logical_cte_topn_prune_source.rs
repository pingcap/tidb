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

//! Port of `pkg/planner.part13` items that drive PORTED per-operator bodies on
//! hand-built trees:
//!
//! * `logicalop_test/logical_operator_test.go:191
//!   TestLogicalCTEPreparePossiblePropertiesSkipNilChild` — Go calls
//!   `LogicalCTE.PreparePossibleProperties(nil, nil,
//!   &PossiblePropertiesInfo{HasTiFlash: true})`
//!   (`logical_cte.go:239-261`) and requires the result to carry the child's
//!   `HasTiFlash` although the CHILDREN slice starts with nil entries.
//! * `logical_operator_test.go:242
//!   TestLogicalTopNPruneColumnsRefreshesSchemaBeforeInlineProjection` — the
//!   TopN starts with a STALE four-slot schema (a duplicated sort column) and
//!   after `PruneColumns` (`logical_top_n.go:79-95`: snapshot the parent set,
//!   prune ByItems into the child call, refresh the schema from the pruned
//!   child BEFORE inline projection) ends with exactly the three parent
//!   columns, ids 1/2/3.
//!
//! Both drive the enum-level dispatches this crate ships:
//! [`tidb_planner::logical::LogicalCTE::prepare_possible_properties`] and
//! [`tidb_planner::logical::LogicalPlan::prune_columns`] (the owned rewrite
//! whose `TopN` arm mirrors `logical_top_n.go:79` arm by arm).

use std::cell::RefCell;
use std::rc::Rc;

use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expr_util::builder::PreservingFunctionBuilder;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use tidb_planner::logical::cte::{CteClass, LogicalCTE};
use tidb_planner::logical::rule::RuleContext;
use tidb_planner::logical::table_dual::LogicalTableDual;
use tidb_planner::logical::topn::LogicalTopN;
use tidb_planner::plan_base::{PlanIdAllocator, PossiblePropertiesInfo};

use tidb_planner::logical::{BaseLogicalPlan, LogicalPlan};

fn column(unique_id: i64) -> Column {
    let mut col = Column::new(
        unique_id,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    );
    col.id = unique_id;
    col.index = (unique_id as i64 - 1).max(0);
    col
}

fn col_expr(unique_id: i64) -> Expression {
    Expression::Column(column(unique_id))
}

fn schema_of(ids: &[i64]) -> Schema {
    Schema::new(ids.iter().copied().map(column).collect())
}

fn test_context<'a>(allocator: &'a PlanIdAllocator) -> RuleContext<'a> {
    static COLUMN_ALLOCATOR: tidb_planner::expression_rewriter::ColumnIdAllocator =
        tidb_planner::expression_rewriter::ColumnIdAllocator::new();
    RuleContext {
        allocator,
        column_allocator: &COLUMN_ALLOCATOR,
        builder: &PreservingFunctionBuilder,
        use_plan_cache: false,
        plan_cache_marker: None,
        // Go's `AllowDeriveTopN` defaults ON.
        allow_derive_topn: true,
        disabled_rules: Default::default(),
        statistics_load: None,
        partition_pruning: None,
        opt_index_prune_threshold: 20,
        always_keep_join_key: true,
        enable_unsafe_substitute: false,
        enable_semi_join_rewrite: false,
        join_reorder_threshold: 0,
        advanced_join_reorder: true,
        cartesian_join_order_threshold: 0.0,
        join_reorder_through_proj: false,
        join_reorder_through_sel: false,
        outer_join_reorder: true,
        advanced_join_hint: true,
        hint_warning_sink: None,
    }
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/logical_operator_test.go:191
/// TestLogicalCTEPreparePossiblePropertiesSkipNilChild`.
///
/// Re-derived from the source: the children-properties argument is
/// `[nil, nil, {HasTiFlash: true}]`, and `LogicalCTE.PreparePossibleProperties`
/// (`logical_cte.go:239`) SKIPS nil entries — starting its conjunction at the
/// first NON-nil child — so the answer is true even though an earlier entry
/// would have zeroed it. The seed plan's capability is passed explicitly here
/// as FALSE so only the skip-nil behavior can produce the true result.
#[test]
fn cte_prepare_possible_properties_skips_nil_children_to_the_first_non_nil_answer() {
    let class = Rc::new(RefCell::new(CteClass::default()));
    let mut cte = LogicalCTE::new(BaseLogicalPlan::default(), class);

    let with_tiflash = PossiblePropertiesInfo {
        orders: Vec::new(),
        has_tiflash: true,
    };
    let props = cte.prepare_possible_properties(
        &[None, None, Some(with_tiflash)],
        // Seed says NO TiFlash; the non-nil child must still win.
        false,
    );
    assert!(props.has_tiflash);
    // A CTE offers no order property (:255-258 body shape).
    assert!(props.orders.is_empty());
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/logical_operator_test.go:242
/// TestLogicalTopNPruneColumnsRefreshesSchemaBeforeInlineProjection`.
///
/// Re-derived from the source (`logical_top_n.go:79-95`): pruning snapshots
/// the PARENT set before widening it with ByItems columns; runs the child's
/// prune; then DROPS the operator schema and rebuilds it via InlineProjection
/// on the pruned child — so stale hidden sort-column slots left in the old
/// schema disappear. The Go fixture feeds a deliberately stale four-slot TopN
/// schema `[out1,out2,sortCol,sortCol]` and requires the pruned schema to be
/// exactly three slots with UniqueIDs 1, 2, 3.
#[test]
fn topn_prune_columns_refreshes_schema_before_inline_projection_dropping_stale_slots() {
    let allocator = PlanIdAllocator::new();

    // child := TableDual{RowCount:1}.Init(...); schema [out1(1), out2(2), sort(3)].
    let mut dual = LogicalTableDual::new(BaseLogicalPlan::new(&allocator, "TableDual", 0), 1);
    dual.base.base.set_schema(Some(schema_of(&[1, 2, 3])));
    let child = LogicalPlan::TableDual(dual);

    // topN := LogicalTopN{ByItems:[{sortCol,false}], Count:1}.Init(...),
    // carrying the STALE four-slot schema [1,2,3,3](:253-260).
    let mut top_n = LogicalTopN::new(
        BaseLogicalPlan::new(&allocator, "TopN", 0),
        vec![ByItems::new(col_expr(3), false)],
        0,
        1,
    );
    let mut stale_schema = schema_of(&[1, 2, 3]);
    stale_schema.columns.push(column(3));
    top_n.base.base.set_schema(Some(stale_schema.clone()));

    let mut plan = LogicalPlan::TopN(top_n);
    plan.set_children(vec![child]);

    // PruneColumns([out1', out2', sortCol']) :262-274.
    let ctx = test_context(&allocator);
    let pruned = plan
        .prune_columns(&ctx, &[column(1), column(2), column(3)])
        .expect("TopN prune over TableDual must succeed");

    let LogicalPlan::TopN(top_n) = &pruned else {
        panic!("expected the TopN to survive pruning");
    };
    // require.Equal(t, 3, topN.Schema().Len()) and UniqueIDs {1,2,3}
    // (:275-280). The fourth slot, a duplicate of the sort column, MUST be
    // gone: it was never rebuilt from the operator's own schema.
    let schema = top_n
        .base
        .base
        .schema()
        .expect("pruned TopN must carry a refreshed schema");
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns[0].unique_id, 1);
    assert_eq!(schema.columns[1].unique_id, 2);
    assert_eq!(schema.columns[2].unique_id, 3);

    pruned.dismantle();
}
