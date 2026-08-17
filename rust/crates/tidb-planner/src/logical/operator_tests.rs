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

//! Semantic tests for the ported logical operators.
//!
//! These are WRITTEN, not transcreated. Go exercises every body below only
//! through the full optimizer — `pkg/planner/core/casetest/**` runs whole
//! statements against a session and diffs an `EXPLAIN` — which this crate has
//! no session for. Each test therefore states the CONTRACT of one ported Go
//! body directly: what it classifies, what it keeps, what it drops.

use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode, BaseFuncDesc, ByItems};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;

use super::aggregation::{
    agg_func_result_matches_arg_for_non_empty_group, max_sort_prefix_len, LogicalAggregation,
    AGG_FUNC_FIRST_ROW, AGG_FUNC_MAX,
};
use super::apply::{find_child_full_schema, LogicalApply};
use super::cte::{
    extract_correlated_cols_for_plan, get_has_tiflash, CteClass, CtePredicatePushDown, LogicalCTE,
    LogicalCTETable,
};
use super::data_source::{DataSource, DataSourceColumn};
use super::expand::{GroupingMode, LogicalExpand, RollupGroupingSet};
use super::index_scan::{matches_indices_prop, LogicalIndexScan};
use super::join::{is_eq_cond_from_in, LogicalJoin, OnConditionSplit};
use super::limit::LogicalLimit;
use super::lock::{
    is_select_for_share_lock_type, is_select_for_update_lock_type, is_supported_select_lock_type,
    LogicalLock, SelectLockType,
};
use super::max_one_row::LogicalMaxOneRow;
use super::mem_table::{
    LogicalMemTable, MemTableColumn, MemTableTopNHints, CLUSTER_TABLE_SLOW_LOG, SLOW_LOG_TIME_STR,
    TABLE_SLOW_QUERY,
};
use super::projection::LogicalProjection;
use super::schema_producer;
use super::selection::{is_valid_compare_constant_predicate, LogicalSelection, SELECTION_FACTOR};
use super::sequence::LogicalSequence;
use super::show::{
    extract_stats_meta_filter_values, extract_stats_meta_filters, find_show_column_ids,
    get_string_value_from_constant, LogicalShow, ShowContents,
};
use super::show_ddl_jobs::LogicalShowDDLJobs;
use super::sort::{
    get_possible_property_from_by_items, prune_by_items, LogicalSort, SortTopNPushDown,
};
use super::table_dual::LogicalTableDual;
use super::table_scan::LogicalTableScan;
use super::tikv_single_gather::TiKVSingleGather;
use super::topn::LogicalTopN;
use super::union_all::{LogicalPartitionUnionAll, LogicalUnionAll};
use super::union_scan::{contains_virtual_column, LogicalUnionScan, EXTRA_PHYS_TBL_ID};
use super::window::{
    BoundType, FrameBound, FrameType, LogicalWindow, RangeCmpDataType, WindowFrame, WindowSortItem,
};
use super::{BaseLogicalPlan, LogicalPlan};
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

fn column(unique_id: i64) -> Column {
    Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
}

fn col_expr(unique_id: i64) -> Expression {
    Expression::Column(column(unique_id))
}

fn schema(ids: &[i64]) -> Schema {
    Schema::new(ids.iter().copied().map(column).collect())
}

fn tiny() -> FieldType {
    FieldType::new(FieldTypeCode::Tiny)
}

fn call(name: &str, args: Vec<Expression>) -> ScalarFunction {
    ScalarFunction::new(tidb_ast::CiString::new(name), tiny(), args)
}

fn eq(left: Expression, right: Expression) -> Expression {
    Expression::ScalarFunction(call("eq", vec![left, right]))
}

fn one() -> Expression {
    Expression::Constant(Constant::new_one())
}

fn agg(name: &str, args: Vec<Expression>) -> AggFuncDesc {
    AggFuncDesc {
        base: BaseFuncDesc {
            name: name.to_owned(),
            args,
            ret_type: tiny(),
        },
        mode: AggFunctionMode::Complete,
        has_distinct: false,
        order_by_items: Vec::new(),
        grouping_id: 0,
    }
}

// ***** LogicalSchemaProducer *****

/// Go `expression.GetUsedList` (`schema.go:338`).
#[test]
fn get_used_list_marks_only_referenced_columns() {
    let output = schema(&[1, 2, 3]);
    let used = schema_producer::get_used_list(&[column(3), column(1)], &output);
    assert_eq!(used, vec![true, false, true]);
}

/// Go `LogicalSchemaProducer.Schema()` (`logical_schema_producer.go:80`).
#[test]
fn materialized_schema_falls_back_to_the_single_child() {
    let child = schema(&[7, 8]);
    let own = schema(&[1]);
    assert_eq!(
        schema_producer::materialized_schema(Some(&own), &[&child]).len(),
        1
    );
    assert_eq!(
        schema_producer::materialized_schema(None, &[&child]).len(),
        2
    );
    // Two children: Go returns an EMPTY schema, not a merged one.
    assert_eq!(
        schema_producer::materialized_schema(None, &[&child, &own]).len(),
        0
    );
}

/// Go `LogicalSchemaProducer.InlineProjection`
/// (`logical_schema_producer.go:120`).
#[test]
fn inline_projection_drops_unused_columns() {
    let mut output = schema(&[1, 2, 3]);
    let pruned = schema_producer::inline_projection(&mut output, &[column(2)]);
    assert_eq!(
        output
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![2]
    );
    // Go appends while walking BACKWARDS, so the pruned list is reversed.
    assert_eq!(
        pruned.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![3, 1]
    );
}

/// The `len(parentUsedCols) == 0` escape of `InlineProjection`: one column is
/// always kept, and it is the narrowest.
#[test]
fn inline_projection_keeps_the_narrowest_column_when_nothing_is_used() {
    let mut narrow = Column::new(9, FieldType::new(FieldTypeCode::Tiny));
    narrow.ret_type.as_mut().unwrap().set_flen(1);
    let mut wide = Column::new(10, FieldType::new(FieldTypeCode::LongLong));
    wide.ret_type.as_mut().unwrap().set_flen(20);
    let mut output = Schema::new(vec![wide, narrow]);
    schema_producer::inline_projection(&mut output, &[]);
    assert_eq!(
        output
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![9]
    );
}

/// Go `LogicalSchemaProducer.BuildKeyInfo`
/// (`logical_schema_producer.go:148`): a child key survives only when every
/// one of its columns does.
#[test]
fn propagate_child_keys_needs_every_key_column() {
    let mut child = schema(&[1, 2, 3]);
    child.set_keys(vec![vec![column(1)], vec![column(1), column(3)]]);
    let mut output = schema(&[1, 2]);
    schema_producer::propagate_child_keys(&mut output, std::slice::from_ref(&child));
    assert_eq!(output.pk_or_uk.len(), 1);
    assert_eq!(output.pk_or_uk[0][0].unique_id, 1);
}

/// Go `LogicalSchemaProducer.Hash64`/`Equals`, which the crate's former
/// `logical_schema_producer` adapter modelled: nil and empty are DIFFERENT.
#[test]
fn schema_identity_separates_nil_from_empty() {
    let empty = Schema::default();
    assert_ne!(
        schema_producer::schema_hash64(None),
        schema_producer::schema_hash64(Some(&empty))
    );
    assert!(!schema_producer::schema_equals(None, Some(&empty)));
    assert!(schema_producer::schema_equals(None, None));
    let a = schema(&[1, 2]);
    let b = schema(&[1, 2]);
    assert_eq!(
        schema_producer::schema_hash64(Some(&a)),
        schema_producer::schema_hash64(Some(&b))
    );
    assert!(schema_producer::schema_equals(Some(&a), Some(&b)));
    assert!(!schema_producer::schema_equals(
        Some(&a),
        Some(&schema(&[2, 1]))
    ));
}

// ***** LogicalSelection *****

/// Go `splitSetGetVarFunc` (`logical_selection.go:328`).
#[test]
fn selection_pins_get_set_var_conditions_in_place() {
    let pinned = Expression::ScalarFunction(call("getvar", vec![one()]));
    let plain = eq(col_expr(1), one());
    let (pushable, kept) =
        LogicalSelection::split_set_get_var_func(&[plain.clone(), pinned.clone()]);
    assert_eq!(pushable.len(), 1);
    assert_eq!(kept.len(), 1);
    assert!(schema_producer::expressions_equal(&pushable[0], &plain));
    assert!(schema_producer::expressions_equal(&kept[0], &pinned));
}

/// Go `LogicalSelection.PruneColumns` (`logical_selection.go:127`): the child
/// must keep producing whatever the filter reads.
#[test]
fn selection_adds_its_own_columns_to_the_child_request() {
    let selection = LogicalSelection::new(
        BaseLogicalPlan::with_id(1, LogicalSelection::TYPE, 0),
        vec![eq(col_expr(5), one())],
    );
    let requested = selection.child_used_cols(&[column(2)]);
    let ids: Vec<i64> = requested.iter().map(|c| c.unique_id).collect();
    assert_eq!(ids, vec![2, 5]);
}

/// Go `LogicalSelection.BuildKeyInfo` (`logical_selection.go:141`): a filter
/// that pins a whole key to constants returns at most one row.
#[test]
fn selection_build_key_info_detects_max_one_row() {
    let mut child = schema(&[1, 2]);
    child.set_keys(vec![vec![column(1)]]);
    let mut selection = LogicalSelection::new(
        BaseLogicalPlan::with_id(1, LogicalSelection::TYPE, 0),
        vec![eq(col_expr(1), one())],
    );
    selection.build_key_info(std::slice::from_ref(&child));
    assert!(selection.base.max_one_row());

    // A filter on a NON-key column proves nothing.
    let mut other = LogicalSelection::new(
        BaseLogicalPlan::with_id(2, LogicalSelection::TYPE, 0),
        vec![eq(col_expr(2), one())],
    );
    other.build_key_info(std::slice::from_ref(&child));
    assert!(!other.base.max_one_row());
}

/// `col = col` is not an equal-CONSTANT condition, so it pins nothing.
#[test]
fn selection_equal_constant_columns_ignores_column_to_column() {
    let selection = LogicalSelection::new(
        BaseLogicalPlan::default(),
        vec![eq(col_expr(1), col_expr(2)), eq(col_expr(3), one())],
    );
    let ids: Vec<i64> = selection
        .equal_constant_columns()
        .iter()
        .map(|c| c.unique_id)
        .collect();
    assert_eq!(ids, vec![3]);
}

/// Go `LogicalSelection.DeriveStats` (`logical_selection.go:227`).
#[test]
fn selection_derive_stats_scales_by_the_selection_factor() {
    let mut selection = LogicalSelection::new(
        BaseLogicalPlan::with_id(1, LogicalSelection::TYPE, 0),
        vec![],
    );
    let child = StatsInfo::new(100.0, [(1, 40.0)]);
    let (stats, derived) = selection.derive_stats(&[child], &[true]).unwrap();
    assert!(derived);
    assert!((stats.row_count() - 100.0 * SELECTION_FACTOR).abs() < 1e-9);
    assert!((stats.col_ndvs()[&1] - 40.0 * SELECTION_FACTOR).abs() < 1e-9);

    // Without a reload the existing profile is returned untouched.
    let (again, derived) = selection
        .derive_stats(&[StatsInfo::new(1.0, [])], &[false])
        .unwrap();
    assert!(!derived);
    assert!((again.row_count() - stats.row_count()).abs() < 1e-9);
}

/// Go `expression.ValidCompareConstantPredicate`, through
/// `LogicalSelection.PullUpConstantPredicates` (`logical_selection.go:212`).
#[test]
fn selection_pulls_up_only_compare_constant_predicates() {
    assert!(is_valid_compare_constant_predicate(&eq(col_expr(1), one())));
    assert!(!is_valid_compare_constant_predicate(&eq(
        col_expr(1),
        col_expr(2)
    )));
    assert!(!is_valid_compare_constant_predicate(
        &Expression::ScalarFunction(call(
            "and",
            vec![eq(col_expr(1), one()), eq(col_expr(2), one())]
        ))
    ));
    let selection = LogicalSelection::new(
        BaseLogicalPlan::default(),
        vec![eq(col_expr(1), one()), eq(col_expr(1), col_expr(2))],
    );
    assert_eq!(selection.pull_up_constant_predicates().len(), 1);
}

// ***** LogicalProjection *****

/// Go `LogicalProjection.GetUsedCols` and `ExtractCorrelatedCols`
/// (`logical_projection.go:496`, `:367`).
#[test]
fn projection_reports_the_columns_it_reads() {
    let projection = LogicalProjection::new(
        BaseLogicalPlan::with_id(1, LogicalProjection::TYPE, 0),
        vec![col_expr(4), eq(col_expr(5), one())],
    );
    let ids: Vec<i64> = projection
        .get_used_cols()
        .iter()
        .map(|c| c.unique_id)
        .collect();
    assert_eq!(ids, vec![4, 5]);
    assert!(projection.extract_correlated_cols().is_empty());
}

/// Go `canProjectionBeEliminatedLoose` (`logical_projection.go:663`).
#[test]
fn projection_elimination_needs_pure_column_refs_and_no_expand() {
    let mut plain = LogicalProjection::new(BaseLogicalPlan::default(), vec![col_expr(1)]);
    assert!(plain.can_be_eliminated_loose());
    plain.proj4_expand = true;
    assert!(!plain.can_be_eliminated_loose());
    let computed = LogicalProjection::new(BaseLogicalPlan::default(), vec![eq(col_expr(1), one())]);
    assert!(!computed.can_be_eliminated_loose());
}

/// Go `LogicalProjection.PruneColumns` (`logical_projection.go:105`).
#[test]
fn projection_prunes_unused_outputs_and_reports_child_needs() {
    let mut projection = LogicalProjection::new(
        BaseLogicalPlan::with_id(1, LogicalProjection::TYPE, 0),
        vec![col_expr(10), col_expr(11), col_expr(12)],
    );
    let mut output = schema(&[1, 2, 3]);
    let (child_used, empty) = projection.prune_columns_local(&[column(2)], &mut output);
    assert!(!empty);
    assert_eq!(
        output
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(projection.exprs.len(), 1);
    assert_eq!(
        child_used.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![11]
    );
}

/// Go `LogicalProjection.buildSchemaByExprs` (`logical_projection.go:505`)
/// and `BuildKeyInfo` (`:163`): a key survives only when its columns are
/// projected as bare references.
#[test]
fn projection_build_key_info_maps_through_the_expression_schema() {
    let mut child = schema(&[10, 11]);
    child.set_keys(vec![vec![column(10)], vec![column(11)]]);
    let projection = LogicalProjection::new(
        BaseLogicalPlan::default(),
        // Output 0 renames child column 10; output 1 is computed.
        vec![col_expr(10), eq(col_expr(11), one())],
    );
    let by_exprs = projection.build_schema_by_exprs();
    assert_eq!(by_exprs.columns[0].unique_id, 10);
    assert_eq!(by_exprs.columns[1].unique_id, i64::MIN + 1);

    let mut output = schema(&[100, 101]);
    projection.build_key_info(&mut output, std::slice::from_ref(&child));
    assert_eq!(output.pk_or_uk.len(), 1);
    assert_eq!(output.pk_or_uk[0][0].unique_id, 100);
}

/// Go `LogicalProjection.DeriveStats` (`logical_projection.go:278`): the row
/// count passes through and a renaming output inherits its source NDV.
#[test]
fn projection_derive_stats_passes_the_row_count_through() {
    let mut projection = LogicalProjection::new(
        BaseLogicalPlan::with_id(1, LogicalProjection::TYPE, 0),
        vec![col_expr(10)],
    );
    let output = schema(&[100]);
    let child = StatsInfo::new(50.0, [(10, 7.0)]);
    let (stats, derived) = projection.derive_stats(&[child], &output, &[true]).unwrap();
    assert!(derived);
    assert!((stats.row_count() - 50.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&100] - 7.0).abs() < 1e-9);
}

/// The merged identity: the crate's former `LogicalProjectionIdentity` hashed
/// the schema, the expressions, and both flags. All four still separate.
#[test]
fn projection_identity_covers_schema_exprs_and_flags() {
    let output = schema(&[1]);
    let base = LogicalProjection::new(BaseLogicalPlan::default(), vec![col_expr(10)]);
    let same = LogicalProjection::new(BaseLogicalPlan::default(), vec![col_expr(10)]);
    assert_eq!(base.hash64(Some(&output)), same.hash64(Some(&output)));
    assert!(base.equals(Some(&output), &same, Some(&output)));

    let other_expr = LogicalProjection::new(BaseLogicalPlan::default(), vec![col_expr(11)]);
    assert_ne!(base.hash64(Some(&output)), other_expr.hash64(Some(&output)));
    assert!(!base.equals(Some(&output), &other_expr, Some(&output)));

    let mut flagged = same.clone();
    flagged.proj4_expand = true;
    assert_ne!(base.hash64(Some(&output)), flagged.hash64(Some(&output)));
    assert!(!base.equals(Some(&output), &flagged, Some(&output)));

    // The schema is part of the identity, so the same exprs under a different
    // schema are a different operator.
    assert_ne!(base.hash64(Some(&output)), base.hash64(Some(&schema(&[2]))));
}

// ***** LogicalAggregation *****

/// Go `GetGroupByCols` (`logical_aggregation.go:508`): only BARE columns.
#[test]
fn aggregation_group_by_cols_skips_expressions() {
    let agg_plan = LogicalAggregation::new(
        BaseLogicalPlan::with_id(1, LogicalAggregation::TYPE, 0),
        vec![],
        vec![col_expr(1), eq(col_expr(2), one()), col_expr(3)],
    );
    let ids: Vec<i64> = agg_plan
        .get_group_by_cols()
        .iter()
        .map(|c| c.unique_id)
        .collect();
    assert_eq!(ids, vec![1, 3]);
}

/// Go `GetUsedCols` and `ExtractCorrelatedCols`
/// (`logical_aggregation.go:533`, `:303`) both walk the aggregate arguments
/// AND its `ORDER BY` items.
#[test]
fn aggregation_used_cols_include_agg_order_by() {
    let mut desc = agg("group_concat", vec![col_expr(5)]);
    desc.order_by_items = vec![ByItems::new(col_expr(6), true)];
    let agg_plan =
        LogicalAggregation::new(BaseLogicalPlan::default(), vec![desc], vec![col_expr(4)]);
    let ids: Vec<i64> = agg_plan
        .get_used_cols()
        .iter()
        .map(|c| c.unique_id)
        .collect();
    assert_eq!(ids, vec![4, 5, 6]);
    assert!(agg_plan.extract_correlated_cols().is_empty());
}

/// Go `HasDistinct`/`HasOrderBy` (`logical_aggregation.go:464`, `:474`).
#[test]
fn aggregation_reports_distinct_and_order_by() {
    let mut desc = agg("count", vec![col_expr(1)]);
    let plain = LogicalAggregation::new(BaseLogicalPlan::default(), vec![desc.clone()], vec![]);
    assert!(!plain.has_distinct());
    assert!(!plain.has_order_by());
    desc.has_distinct = true;
    desc.order_by_items = vec![ByItems::new(col_expr(2), false)];
    let fancy = LogicalAggregation::new(BaseLogicalPlan::default(), vec![desc], vec![]);
    assert!(fancy.has_distinct());
    assert!(fancy.has_order_by());
}

/// Go `BuildSelfKeyInfo` (`logical_aggregation.go:797`): the group-by columns
/// are a key, and a group-less aggregate is one row.
#[test]
fn aggregation_build_self_key_info() {
    let mut grouped = LogicalAggregation::new(
        BaseLogicalPlan::default(),
        vec![],
        vec![col_expr(1), col_expr(2)],
    );
    let mut output = schema(&[1, 2, 3]);
    grouped.build_self_key_info(&mut output);
    assert_eq!(output.pk_or_uk.len(), 1);
    assert_eq!(output.pk_or_uk[0].len(), 2);
    assert!(!grouped.base.max_one_row());

    let mut scalar = LogicalAggregation::new(BaseLogicalPlan::default(), vec![], vec![]);
    let mut out = schema(&[1]);
    scalar.build_self_key_info(&mut out);
    assert!(scalar.base.max_one_row());
}

/// Go `getAggFuncsColsForFirstRow` (`logical_aggregation.go:716`): a
/// constant-only GROUP BY makes `firstrow()` outputs unusable.
#[test]
fn aggregation_first_row_cols_refuse_constant_groups() {
    let output = schema(&[100]);
    let desc = agg(AGG_FUNC_FIRST_ROW, vec![col_expr(1)]);
    let real = LogicalAggregation::new(
        BaseLogicalPlan::default(),
        vec![desc.clone()],
        vec![col_expr(2)],
    );
    assert_eq!(real.agg_funcs_cols_for_first_row(&output).len(), 1);

    let constant_group =
        LogicalAggregation::new(BaseLogicalPlan::default(), vec![desc], vec![one()]);
    assert!(constant_group
        .agg_funcs_cols_for_first_row(&output)
        .is_empty());
}

/// Go `aggFuncResultMatchesArgForNonEmptyGroup`
/// (`logical_aggregation.go:703`).
#[test]
fn aggregation_const_result_needs_max_or_min_of_a_constant() {
    let matching = agg(AGG_FUNC_MAX, vec![one()]);
    assert!(agg_func_result_matches_arg_for_non_empty_group(&matching));

    let of_a_column = agg(AGG_FUNC_MAX, vec![col_expr(1)]);
    assert!(!agg_func_result_matches_arg_for_non_empty_group(
        &of_a_column
    ));

    let wrong_func = agg("sum", vec![one()]);
    assert!(!agg_func_result_matches_arg_for_non_empty_group(
        &wrong_func
    ));

    let mut distinct = matching.clone();
    distinct.has_distinct = true;
    assert!(!agg_func_result_matches_arg_for_non_empty_group(&distinct));
}

/// Go `ExtractColGroups` (`logical_aggregation.go:250`): the parent's groups
/// are discarded and only a MULTI-column group-by is asked for.
#[test]
fn aggregation_extract_col_groups_asks_only_for_its_own_keys() {
    let single = LogicalAggregation::new(BaseLogicalPlan::default(), vec![], vec![col_expr(1)]);
    assert!(single.extract_col_groups().is_empty());

    let multi = LogicalAggregation::new(
        BaseLogicalPlan::default(),
        vec![],
        vec![col_expr(3), col_expr(1)],
    );
    let groups = multi.extract_col_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0].iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 3]
    );
}

/// Go `PreparePossibleProperties` (`logical_aggregation.go:268`).
#[test]
fn aggregation_possible_properties_keep_prefix_orders() {
    let mut agg_plan = LogicalAggregation::new(
        BaseLogicalPlan::default(),
        vec![],
        vec![col_expr(1), col_expr(2)],
    );
    let child = crate::plan_base::PossiblePropertiesInfo {
        orders: vec![
            vec![column(1), column(2), column(3)],
            vec![column(3), column(1)],
        ],
        has_tiflash: true,
    };
    let props = agg_plan.prepare_possible_properties(Some(&child));
    assert_eq!(props.orders.len(), 1);
    assert_eq!(props.orders[0].len(), 2);
    assert!(props.has_tiflash);
    assert!(agg_plan.base.has_tiflash());

    // A group-less aggregate offers exactly ONE empty order.
    let mut scalar = LogicalAggregation::new(BaseLogicalPlan::default(), vec![], vec![]);
    let props = scalar.prepare_possible_properties(Some(&child));
    assert_eq!(props.orders.len(), 1);
    assert!(props.orders[0].is_empty());
}

/// Go `util.GetMaxSortPrefix`, as `PreparePossibleProperties` uses it.
#[test]
fn max_sort_prefix_stops_at_the_first_miss() {
    assert_eq!(
        max_sort_prefix_len(&[column(1), column(9), column(2)], &[column(1), column(2)]),
        1
    );
    assert_eq!(max_sort_prefix_len(&[], &[column(1)]), 0);
}

/// Go `LogicalAggregation.DeriveStats` (`logical_aggregation.go:219`): the
/// row count becomes the group NDV, capped at the child's rows, and
/// `InputCount` records the child's rows.
#[test]
fn aggregation_derive_stats_uses_the_group_ndv() {
    let mut agg_plan = LogicalAggregation::new(
        BaseLogicalPlan::with_id(1, LogicalAggregation::TYPE, 0),
        vec![],
        vec![col_expr(1)],
    );
    let output = schema(&[100, 101]);
    let child = StatsInfo::new(1000.0, [(1, 25.0)]);
    let (stats, derived) = agg_plan.derive_stats(&[child], &output, &[true]).unwrap();
    assert!(derived);
    assert!((stats.row_count() - 25.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&100] - 25.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&101] - 25.0).abs() < 1e-9);
    assert!((agg_plan.input_count - 1000.0).abs() < 1e-9);
}

/// A scalar aggregate is exactly one row.
#[test]
fn aggregation_derive_stats_for_a_scalar_aggregate_is_one_row() {
    let mut agg_plan = LogicalAggregation::new(
        BaseLogicalPlan::with_id(1, LogicalAggregation::TYPE, 0),
        vec![],
        vec![],
    );
    let output = schema(&[100]);
    let (stats, _) = agg_plan
        .derive_stats(&[StatsInfo::new(1000.0, [])], &output, &[true])
        .unwrap();
    assert!((stats.row_count() - 1.0).abs() < 1e-9);
}

/// The merged identity: the crate's former `LogicalAggregationIdentity`.
#[test]
fn aggregation_identity_covers_funcs_and_group_items() {
    let output = schema(&[1]);
    let base = LogicalAggregation::new(
        BaseLogicalPlan::default(),
        vec![agg("count", vec![col_expr(1)])],
        vec![col_expr(2)],
    );
    let same = base.clone();
    assert_eq!(base.hash64(Some(&output)), same.hash64(Some(&output)));
    assert!(base.equals(Some(&output), &same, Some(&output)));

    let other_func = LogicalAggregation::new(
        BaseLogicalPlan::default(),
        vec![agg("sum", vec![col_expr(1)])],
        vec![col_expr(2)],
    );
    assert_ne!(base.hash64(Some(&output)), other_func.hash64(Some(&output)));

    let other_group = LogicalAggregation::new(
        BaseLogicalPlan::default(),
        vec![agg("count", vec![col_expr(1)])],
        vec![col_expr(3)],
    );
    assert_ne!(
        base.hash64(Some(&output)),
        other_group.hash64(Some(&output))
    );
    assert!(!base.equals(Some(&output), &other_group, Some(&output)));
}

// ***** LogicalJoin *****

/// Go `base.JoinType.String()` (`base/plan_base.go:353`), through
/// `LogicalJoin.ExplainInfo` (`logical_join.go:127`).
#[test]
fn join_explain_info_opens_with_the_join_type() {
    let mut join = LogicalJoin::new(
        BaseLogicalPlan::with_id(1, LogicalJoin::TYPE, 0),
        LogicalJoinType::LeftOuter,
    );
    assert_eq!(join.explain_info(), "left outer join");
    join.equal_conditions
        .push(call("eq", vec![col_expr(1), col_expr(2)]));
    assert!(join.explain_info().starts_with("left outer join, equal:1"));
    assert_eq!(
        LogicalJoin::join_type_name(LogicalJoinType::AntiLeftOuterSemi),
        "anti left outer semi join"
    );
}

/// Go `AppendJoinConds` (`logical_join.go:1148`): new conditions go in FRONT.
#[test]
fn join_append_conds_prepends() {
    let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    join.other_conditions.push(eq(col_expr(9), one()));
    join.append_join_conds(OnConditionSplit {
        equal: vec![call("eq", vec![col_expr(1), col_expr(2)])],
        left: vec![],
        right: vec![],
        other: vec![eq(col_expr(8), one())],
    });
    assert_eq!(join.equal_conditions.len(), 1);
    assert_eq!(join.other_conditions.len(), 2);
    // The incoming condition is first.
    let first = &join.other_conditions[0];
    assert!(schema_producer::expressions_equal(
        first,
        &eq(col_expr(8), one())
    ));
}

/// Go `GetJoinKeys` / `ExtractJoinKeys` (`logical_join.go:1011`, `:1203`).
#[test]
fn join_keys_come_from_the_equal_conditions() {
    let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    join.equal_conditions = vec![
        call("eq", vec![col_expr(1), col_expr(11)]),
        call("nulleq", vec![col_expr(2), col_expr(12)]),
    ];
    let (left, right, is_null_eq, has_null_eq) = join.get_join_keys();
    assert_eq!(
        left.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        right.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![11, 12]
    );
    assert_eq!(is_null_eq, vec![false, true]);
    assert!(has_null_eq);

    let right_schema = join.extract_join_keys(1);
    assert_eq!(
        right_schema
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![11, 12]
    );
}

/// Go `ExtractUsedCols` (`logical_join.go:1212`): the join's own conditions
/// join the parent's request, and each column is routed to its side.
#[test]
fn join_extract_used_cols_routes_by_child_schema() {
    let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    join.equal_conditions = vec![call("eq", vec![col_expr(1), col_expr(11)])];
    join.other_conditions = vec![eq(col_expr(2), col_expr(12))];
    let (left, right) =
        join.extract_used_cols(&[column(3)], &schema(&[1, 2, 3]), &schema(&[11, 12]));
    assert_eq!(
        left.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![3, 1, 2]
    );
    assert_eq!(
        right.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![11, 12]
    );
}

/// Go `pushDownConstExpr` (`logical_join.go:1555`): where a column-free
/// condition may go depends on the join type AND on whether it came from a
/// filter or from the `ON` clause.
#[test]
fn join_push_down_const_expr_follows_the_join_type() {
    let cond = one();

    let mut inner = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    let (mut left, mut right) = (Vec::new(), Vec::new());
    inner.push_down_const_expr(&cond, &mut left, &mut right, false);
    assert_eq!((left.len(), right.len()), (1, 1));

    // A LEFT OUTER join from the ON clause: only the inner (right) side.
    let mut outer = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::LeftOuter);
    let (mut left, mut right) = (Vec::new(), Vec::new());
    outer.push_down_const_expr(&cond, &mut left, &mut right, false);
    assert_eq!((left.len(), right.len()), (0, 1));

    // The same join from a FILTER: the left side, plus a right JOIN condition
    // recorded on the operator so it can keep travelling.
    let mut outer = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::LeftOuter);
    let (mut left, mut right) = (Vec::new(), Vec::new());
    outer.push_down_const_expr(&cond, &mut left, &mut right, true);
    assert_eq!((left.len(), right.len()), (1, 0));
    assert_eq!(outer.right_conditions.len(), 1);

    // ANTI SEMI from the ON clause pushes only right.
    let mut anti = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::AntiSemi);
    let (mut left, mut right) = (Vec::new(), Vec::new());
    anti.push_down_const_expr(&cond, &mut left, &mut right, false);
    assert_eq!((left.len(), right.len()), (0, 1));
}

/// Go `LogicalJoin.BuildKeyInfo` (`logical_join.go:365`): a semi join keeps
/// the LEFT child's keys.
#[test]
fn join_build_key_info_semi_keeps_the_left_keys() {
    let mut left = schema(&[1, 2]);
    left.set_keys(vec![vec![column(1)]]);
    let right = schema(&[11]);
    let join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Semi);
    let mut output = schema(&[1, 2]);
    join.build_key_info(&mut output, &[left, right]);
    assert_eq!(output.pk_or_uk.len(), 1);
    assert_eq!(output.pk_or_uk[0][0].unique_id, 1);
}

/// The inner-join arm: a side's keys survive only when the OTHER side's join
/// keys cover one of its keys, and never for the null-filling side.
#[test]
fn join_build_key_info_inner_needs_a_covered_key() {
    let mut left = schema(&[1]);
    left.set_keys(vec![vec![column(1)]]);
    let mut right = schema(&[11]);
    right.set_keys(vec![vec![column(11)]]);

    let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    join.equal_conditions = vec![call("eq", vec![col_expr(1), col_expr(11)])];
    let mut output = schema(&[1, 11]);
    join.build_key_info(&mut output, &[left.clone(), right.clone()]);
    // Both sides are covered, so both sets of keys carry.
    assert_eq!(output.pk_or_uk.len(), 2);

    // With NO equal condition the cartesian product destroys uniqueness.
    let bare = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    let mut output = schema(&[1, 11]);
    bare.build_key_info(&mut output, &[left.clone(), right.clone()]);
    assert!(output.pk_or_uk.is_empty());

    // A LEFT OUTER join may not carry the RIGHT child's keys.
    let mut outer = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::LeftOuter);
    outer.equal_conditions = vec![call("eq", vec![col_expr(1), col_expr(11)])];
    let mut output = schema(&[1, 11]);
    outer.build_key_info(&mut output, &[left, right]);
    assert_eq!(output.pk_or_uk.len(), 1);
    assert_eq!(output.pk_or_uk[0][0].unique_id, 1);
}

/// Go `LogicalJoin.DeriveStats` (`logical_join.go:560`).
#[test]
fn join_derive_stats_by_join_type() {
    let left = StatsInfo::new(100.0, [(1, 50.0)]);
    let right = StatsInfo::new(200.0, [(11, 80.0)]);
    let output = schema(&[1, 11]);

    let mut semi = LogicalJoin::new(
        BaseLogicalPlan::with_id(1, LogicalJoin::TYPE, 0),
        LogicalJoinType::Semi,
    );
    let (stats, _) = semi
        .derive_stats(&[left.clone(), right.clone()], &output, 300.0, &[true])
        .unwrap();
    assert!((stats.row_count() - 100.0 * SELECTION_FACTOR).abs() < 1e-9);

    let mut inner = LogicalJoin::new(
        BaseLogicalPlan::with_id(2, LogicalJoin::TYPE, 0),
        LogicalJoinType::Inner,
    );
    let (stats, _) = inner
        .derive_stats(&[left.clone(), right.clone()], &output, 30.0, &[true])
        .unwrap();
    assert!((stats.row_count() - 30.0).abs() < 1e-9);
    // Every NDV is capped at the output row count.
    assert!((stats.col_ndvs()[&1] - 30.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&11] - 30.0).abs() < 1e-9);
    assert!((inner.equal_cond_out_cnt - 30.0).abs() < 1e-9);

    // A LEFT OUTER join emits at least the preserved side's rows.
    let mut outer = LogicalJoin::new(
        BaseLogicalPlan::with_id(3, LogicalJoin::TYPE, 0),
        LogicalJoinType::LeftOuter,
    );
    let (stats, _) = outer
        .derive_stats(&[left.clone(), right.clone()], &output, 30.0, &[true])
        .unwrap();
    assert!((stats.row_count() - 100.0).abs() < 1e-9);

    // A left-outer-SEMI join adds a two-valued marker column.
    let mut marker = LogicalJoin::new(
        BaseLogicalPlan::with_id(4, LogicalJoin::TYPE, 0),
        LogicalJoinType::LeftOuterSemi,
    );
    let (stats, _) = marker
        .derive_stats(&[left, right], &output, 30.0, &[true])
        .unwrap();
    assert!((stats.row_count() - 100.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&11] - 2.0).abs() < 1e-9);
}

/// Go `PreparePossibleProperties` (`logical_join.go:646`): the null-filled
/// side of an outer join loses its orders.
#[test]
fn join_possible_properties_drop_the_null_filled_side() {
    let left = crate::plan_base::PossiblePropertiesInfo {
        orders: vec![vec![column(1)]],
        has_tiflash: true,
    };
    let right = crate::plan_base::PossiblePropertiesInfo {
        orders: vec![vec![column(11)]],
        has_tiflash: true,
    };
    let mut inner = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    assert_eq!(
        inner
            .prepare_possible_properties(&left, &right)
            .orders
            .len(),
        2
    );
    let mut outer = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::LeftOuter);
    let props = outer.prepare_possible_properties(&left, &right);
    assert_eq!(props.orders.len(), 1);
    assert_eq!(props.orders[0][0].unique_id, 1);
    assert!(outer.base.has_tiflash());
    // Both children must report TiFlash for the join to.
    let mut mixed = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    let no_tiflash = crate::plan_base::PossiblePropertiesInfo::default();
    assert!(
        !mixed
            .prepare_possible_properties(&left, &no_tiflash)
            .has_tiflash
    );
}

/// Go `expression.IsEQCondFromIn` (`expression.go:325`), which
/// `ExtractOnCondition` uses to keep an `IN (subq)` rewrite out of the equal
/// bucket.
#[test]
fn eq_cond_from_in_needs_an_in_operand_column() {
    assert!(!is_eq_cond_from_in(&eq(col_expr(1), col_expr(2))));
    let mut in_col = column(1);
    in_col.in_operand = true;
    let marked = eq(Expression::Column(in_col), col_expr(2));
    assert!(is_eq_cond_from_in(&marked));
    // A non-`eq` function never qualifies.
    let ne = Expression::ScalarFunction(call("ne", vec![col_expr(1), col_expr(2)]));
    assert!(!is_eq_cond_from_in(&ne));
}

/// Go `RegisterRedundantColumnMapping` / `ResolveRedundantColumn`
/// (`logical_join.go:796`, `:818`).
#[test]
fn join_redundant_column_mapping_round_trips() {
    let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    let output = schema(&[1, 2]);
    let redundant = column(99);
    assert!(join.register_redundant_column_mapping(&redundant, &column(2), &output));
    let (resolved, _) = join
        .resolve_redundant_column(&redundant, &output, &[])
        .unwrap();
    assert_eq!(resolved.unique_id, 2);

    // A type mismatch is refused, so no wrong value is ever read back.
    let differently_typed = Column::new(98, FieldType::new(FieldTypeCode::Varchar));
    assert!(!join.register_redundant_column_mapping(&differently_typed, &column(2), &output));
    // A column that is not in the output schema cannot be a target.
    assert!(!join.register_redundant_column_mapping(&redundant, &column(42), &output));
}

/// The merged identity for a join separates the type and every bucket.
#[test]
fn join_identity_covers_type_and_conditions() {
    let output = schema(&[1]);
    let mut base = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
    base.equal_conditions = vec![call("eq", vec![col_expr(1), col_expr(11)])];
    let same = base.clone();
    assert_eq!(base.hash64(Some(&output)), same.hash64(Some(&output)));
    assert!(base.equals(Some(&output), &same, Some(&output)));

    let mut other_type = base.clone();
    other_type.join_type = LogicalJoinType::LeftOuter;
    assert_ne!(base.hash64(Some(&output)), other_type.hash64(Some(&output)));
    assert!(!base.equals(Some(&output), &other_type, Some(&output)));

    let mut extra_cond = base.clone();
    extra_cond.other_conditions.push(one());
    assert_ne!(base.hash64(Some(&output)), extra_cond.hash64(Some(&output)));
}

// ***** DataSource *****

/// Go `DataSource.ExplainInfo` (`logical_datasource.go:163`).
#[test]
fn data_source_explain_info_prefers_the_alias_and_names_the_partition() {
    let mut source = DataSource::new(BaseLogicalPlan::with_id(1, DataSource::TYPE, 0), 7, "t");
    assert_eq!(source.explain_info(), "table:t");
    source.table_as_name = Some("alias".to_owned());
    assert_eq!(source.explain_info(), "table:alias");
    source.partition_definition_names = vec!["p0".to_owned(), "p1".to_owned()];
    source.partition_def_idx = Some(1);
    assert_eq!(source.explain_info(), "table:alias, partition:p1");
    // An empty alias is Go's "no alias".
    source.table_as_name = Some(String::new());
    assert!(source.explain_info().starts_with("table:t"));
}

/// Go `DataSource.ExtractCorrelatedCols` (`logical_datasource.go:377`) reads
/// the PUSHED-DOWN conditions only.
#[test]
fn data_source_correlated_cols_come_from_pushed_down_conds() {
    let mut source = DataSource::new(BaseLogicalPlan::default(), 7, "t");
    source.all_conds = vec![eq(col_expr(1), one())];
    assert!(source.extract_correlated_cols().is_empty());
}

/// Go `DataSource.PredicatePushDown` (`logical_datasource.go:185`): every
/// predicate is recorded in `AllConds`, and only the pushable ones stay.
#[test]
fn data_source_records_all_conds_and_returns_the_remainder() {
    let mut source = DataSource::new(BaseLogicalPlan::default(), 7, "t");
    let pushable = eq(col_expr(1), one());
    let kept = eq(col_expr(2), one());
    let remaining = source.predicate_push_down_local(vec![pushable.clone()], vec![kept.clone()]);
    assert_eq!(source.all_conds.len(), 2);
    assert_eq!(source.pushed_down_conds.len(), 1);
    assert_eq!(remaining.len(), 1);
    assert!(schema_producer::expressions_equal(&remaining[0], &kept));
}

/// Go `DataSource.PruneColumns` (`logical_datasource.go:200`): a column kept
/// only for `AllConds` stays in the schema but NOT in
/// `ColsRequiringFullLen`.
#[test]
fn data_source_prune_columns_separates_conds_from_output() {
    let mut source = DataSource::new(BaseLogicalPlan::default(), 7, "t");
    source.columns = (1..=3)
        .map(|id| DataSourceColumn {
            id,
            name: format!("c{id}"),
            is_primary_key: false,
        })
        .collect();
    source.all_conds = vec![eq(col_expr(2), one())];
    let mut output = schema(&[1, 2, 3]);
    let empty = source.prune_columns_local(&[column(1)], &mut output);
    assert!(!empty);
    assert_eq!(
        output
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        source
            .cols_requiring_full_len
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(source.columns.len(), 2);
}

/// The int handle is forgotten once it no longer appears in the schema.
#[test]
fn data_source_prune_columns_forgets_a_dropped_int_handle() {
    let mut source = DataSource::new(BaseLogicalPlan::default(), 7, "t");
    source.handle_cols = vec![column(3)];
    source.handle_is_int = true;
    source.columns = vec![DataSourceColumn::default(); 3];
    let mut output = schema(&[1, 2, 3]);
    source.prune_columns_local(&[column(1)], &mut output);
    assert!(source.handle_cols.is_empty());
    assert!(!source.handle_is_int);
}

/// Go `DataSource.BuildKeyInfo` (`logical_datasource.go:278`): the
/// `PKIsHandle` primary key becomes a key of the output.
#[test]
fn data_source_build_key_info_adds_the_int_primary_key() {
    let mut source = DataSource::new(BaseLogicalPlan::default(), 7, "t");
    source.pk_is_handle = true;
    source.columns = vec![
        DataSourceColumn {
            id: 1,
            name: "a".to_owned(),
            is_primary_key: false,
        },
        DataSourceColumn {
            id: 2,
            name: "id".to_owned(),
            is_primary_key: true,
        },
    ];
    let mut output = schema(&[1, 2]);
    source.build_key_info(&mut output, Vec::new());
    assert_eq!(output.pk_or_uk.len(), 1);
    assert_eq!(output.pk_or_uk[0][0].unique_id, 2);
    assert_eq!(source.get_pk_is_handle_col(&output).unwrap().unique_id, 2);

    // Without PKIsHandle there is no handle column and no derived key.
    source.pk_is_handle = false;
    let mut output = schema(&[1, 2]);
    source.build_key_info(&mut output, Vec::new());
    assert!(output.pk_or_uk.is_empty());
    assert!(source.get_pk_is_handle_col(&output).is_none());
}

/// Go `DataSource.PreparePossibleProperties` (`logical_datasource.go:343`):
/// a TiKV-only hint suppresses the TiFlash flag.
#[test]
fn data_source_possible_properties_respect_the_store_hint() {
    let mut source = DataSource::new(BaseLogicalPlan::default(), 7, "t");
    source.has_tiflash_replica = true;
    let orders = vec![vec![column(1)]];
    let props = source.prepare_possible_properties(orders.clone(), true, true);
    assert!(props.has_tiflash);
    assert_eq!(props.orders.len(), 1);

    source.prefer_store_type = super::data_source::PREFER_TIKV;
    assert!(
        !source
            .prepare_possible_properties(orders.clone(), true, true)
            .has_tiflash
    );
    // MPP off also suppresses it.
    source.prefer_store_type = 0;
    assert!(
        !source
            .prepare_possible_properties(orders, true, false)
            .has_tiflash
    );
}

// ***** the enum-level dispatch *****

/// Every ported operator answers `ExtractCorrelatedCols` through the enum, so
/// a new operator cannot silently fall through to the base's empty answer.
#[test]
fn enum_dispatch_reaches_every_ported_operator() {
    let plans = [
        LogicalPlan::Selection(LogicalSelection::new(
            BaseLogicalPlan::with_id(1, LogicalSelection::TYPE, 0),
            vec![eq(col_expr(1), one())],
        )),
        LogicalPlan::Projection(LogicalProjection::new(
            BaseLogicalPlan::with_id(2, LogicalProjection::TYPE, 0),
            vec![col_expr(1)],
        )),
        LogicalPlan::Join(LogicalJoin::new(
            BaseLogicalPlan::with_id(3, LogicalJoin::TYPE, 0),
            LogicalJoinType::Inner,
        )),
        LogicalPlan::Aggregation(LogicalAggregation::new(
            BaseLogicalPlan::with_id(4, LogicalAggregation::TYPE, 0),
            vec![],
            vec![col_expr(1)],
        )),
        LogicalPlan::DataSource(DataSource::new(
            BaseLogicalPlan::with_id(5, DataSource::TYPE, 0),
            7,
            "t",
        )),
    ];
    for plan in &plans {
        assert!(plan.extract_correlated_cols().is_empty());
    }
    // Only the two operators whose ExplainInfo is dependency-closed answer.
    assert_eq!(plans[2].explain_info(), "inner join");
    assert_eq!(plans[4].explain_info(), "table:t");
    assert_eq!(plans[0].explain_info(), "");
}

/// `clone_shallow` must copy the operator's own state and DROP the children,
/// for every ported operator.
#[test]
fn clone_shallow_keeps_operator_state_and_drops_children() {
    let mut base = BaseLogicalPlan::with_id(2, LogicalJoin::TYPE, 3);
    base.set_children(vec![LogicalPlan::TableDual(super::LogicalTableDual {
        base: BaseLogicalPlan::with_id(1, "TableDual", 0),
        row_count: 1,
    })]);
    let mut join = LogicalJoin::new(base, LogicalJoinType::AntiSemi);
    join.straight_join = true;
    join.equal_cond_out_cnt = 12.5;
    let plan = LogicalPlan::Join(join);
    let shallow = plan.clone_shallow();
    assert!(shallow.children().is_empty());
    assert_eq!(shallow.id(), 2);
    assert_eq!(shallow.query_block_offset(), 3);
    let LogicalPlan::Join(cloned) = &shallow else {
        panic!("the variant must be preserved");
    };
    assert_eq!(cloned.join_type, LogicalJoinType::AntiSemi);
    assert!(cloned.straight_join);
    assert!((cloned.equal_cond_out_cnt - 12.5).abs() < 1e-9);
    // The deep clone still reproduces the whole subtree.
    assert_eq!(plan.deep_clone().plan_count(), 2);
}

/// `LogicalPlan::build_key_info` inherits `maxOneRow` from a single child
/// before running the operator's own override.
#[test]
fn enum_build_key_info_inherits_max_one_row_from_one_child() {
    let mut child_base = BaseLogicalPlan::with_id(1, LogicalAggregation::TYPE, 0);
    child_base.set_max_one_row(true);
    let child = LogicalPlan::Aggregation(LogicalAggregation::new(child_base, vec![], vec![]));

    let mut base = BaseLogicalPlan::with_id(2, LogicalProjection::TYPE, 0);
    base.set_children(vec![child]);
    let mut plan = LogicalPlan::Projection(LogicalProjection::new(base, vec![col_expr(1)]));
    let mut output = schema(&[100]);
    plan.build_key_info(&mut output, &[schema(&[1])]);
    assert!(plan.max_one_row());
}

// ***** LogicalSort / LogicalLimit / LogicalTopN *****

fn by(expr: Expression, desc: bool) -> ByItems {
    ByItems::new(expr, desc)
}

/// Go `getPossiblePropertyFromByItems` (`logical_sort.go:169`): the LEADING
/// run of column items, truncated at the first expression.
#[test]
fn possible_property_stops_at_the_first_non_column_item() {
    let items = vec![
        by(col_expr(1), false),
        by(col_expr(2), true),
        by(
            Expression::ScalarFunction(call("plus", vec![col_expr(3)])),
            false,
        ),
        by(col_expr(4), false),
    ];
    let cols = get_possible_property_from_by_items(&items);
    assert_eq!(
        cols.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 2]
    );
    // No leading column at all yields no offered order.
    assert!(get_possible_property_from_by_items(&items[2..]).is_empty());
}

/// Go `pruneByItems` (`logical_plans_misc.go:139`): duplicates by `HashCode`
/// and runtime constants go, real column items stay and widen the child's set.
#[test]
fn prune_by_items_drops_duplicates_and_constants() {
    let items = vec![
        by(col_expr(1), false),
        // A duplicate of the first item: same HashCode, pruned.
        by(col_expr(1), true),
        // No column and runtime-constant: pruned.
        by(one(), false),
        by(col_expr(2), false),
    ];
    let (kept, used) = prune_by_items(&items);
    assert_eq!(kept.len(), 2);
    assert_eq!(kept[0].expr.as_column().unwrap().unique_id, 1);
    assert!(!kept[0].desc);
    assert_eq!(kept[1].expr.as_column().unwrap().unique_id, 2);
    assert_eq!(
        used.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

/// Go `LogicalSort.PruneColumns` (`logical_sort.go:67`): the parent's set is
/// WIDENED by the surviving items, never replaced.
#[test]
fn sort_prune_columns_appends_by_item_columns_to_the_parent_set() {
    let mut sort = LogicalSort::new(
        BaseLogicalPlan::with_id(1, LogicalSort::TYPE, 0),
        vec![by(col_expr(5), false), by(one(), true)],
    );
    let used = sort.prune_columns_local(&[column(9)]);
    assert_eq!(
        used.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![9, 5]
    );
    assert_eq!(sort.by_items.len(), 1);
}

/// Go `LogicalSort.PreparePossibleProperties` (`logical_sort.go:114`): the
/// sort ESTABLISHES an order, so the child's is discarded, and `hasTiFlash`
/// is the child's answer.
#[test]
fn sort_prepare_possible_properties_replaces_the_child_order() {
    let mut sort = LogicalSort::new(
        BaseLogicalPlan::with_id(1, LogicalSort::TYPE, 0),
        vec![by(col_expr(5), false)],
    );
    let child = PossiblePropertiesInfo {
        orders: vec![vec![column(77)]],
        has_tiflash: true,
    };
    let info = sort.prepare_possible_properties(Some(&child));
    assert_eq!(info.orders.len(), 1);
    assert_eq!(info.orders[0][0].unique_id, 5);
    assert!(info.has_tiflash);
    assert!(sort.base.has_tiflash());
    // No child at all: no TiFlash, and the sort still offers its own order.
    let info = sort.prepare_possible_properties(None);
    assert!(!info.has_tiflash);
    assert_eq!(info.orders[0][0].unique_id, 5);
}

/// Go `LogicalSort.ExtractCorrelatedCols` (`logical_sort.go:133`).
#[test]
fn sort_extracts_correlated_cols_from_every_by_item() {
    let cor = Expression::CorrelatedColumn(tidb_expr::column::CorrelatedColumn {
        column: column(42),
        data: None,
    });
    let sort = LogicalSort::new(
        BaseLogicalPlan::with_id(1, LogicalSort::TYPE, 0),
        vec![by(col_expr(1), false), by(cor, false)],
    );
    assert_eq!(sort.extract_correlated_cols().len(), 1);
    // The enum dispatches to it.
    assert_eq!(LogicalPlan::Sort(sort).extract_correlated_cols().len(), 1);
}

/// Go `LogicalSort.PushDownTopN` (`logical_sort.go:84`)'s three outcomes.
#[test]
fn sort_push_down_topn_decision_matches_gos_three_branches() {
    assert_eq!(
        LogicalSort::push_down_topn_decision(None),
        SortTopNPushDown::KeepSort
    );
    assert_eq!(
        LogicalSort::push_down_topn_decision(Some(true)),
        SortTopNPushDown::AdoptByItemsAndDropSort
    );
    assert_eq!(
        LogicalSort::push_down_topn_decision(Some(false)),
        SortTopNPushDown::DropSort
    );
}

/// Go `LogicalLimit.BuildKeyInfo` (`logical_limit.go:98`): only `LIMIT 1` is
/// at most one row, and the child's keys carry forward either way.
#[test]
fn limit_build_key_info_marks_max_one_row_only_for_count_one() {
    let mut child = schema(&[1, 2]);
    child.pk_or_uk = vec![vec![column(1)]];

    let mut limit = LogicalLimit::new(BaseLogicalPlan::with_id(1, LogicalLimit::TYPE, 0), 0, 1);
    let mut output = schema(&[1, 2]);
    limit.build_key_info(&mut output, std::slice::from_ref(&child));
    assert!(limit.base.max_one_row());
    assert_eq!(output.pk_or_uk.len(), 1);

    let mut limit = LogicalLimit::new(BaseLogicalPlan::with_id(2, LogicalLimit::TYPE, 0), 0, 5);
    let mut output = schema(&[1, 2]);
    limit.build_key_info(&mut output, &[child]);
    assert!(!limit.base.max_one_row());
}

/// Go `LogicalLimit.DeriveStats` (`logical_limit.go:129`): the count caps the
/// rows and every NDV, and a second call without a reload is memoised.
#[test]
fn limit_derive_stats_caps_at_the_count_and_memoises() {
    let mut limit = LogicalLimit::new(BaseLogicalPlan::with_id(1, LogicalLimit::TYPE, 0), 0, 10);
    let child = StatsInfo::new(1000.0, [(1_i64, 500.0), (2, 3.0)]);
    let (stats, derived) = limit
        .derive_stats(std::slice::from_ref(&child), &[true])
        .expect("a limit always has a child profile");
    assert!(derived);
    assert!((stats.row_count() - 10.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&1] - 10.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&2] - 3.0).abs() < 1e-9);
    // Without a reload the stored profile comes back untouched.
    let (_, derived) = limit.derive_stats(&[child], &[false]).unwrap();
    assert!(!derived);
}

/// Go `LogicalLimit.ExplainInfo` (`logical_limit.go:48`).
#[test]
fn limit_explain_info_is_exact_without_partitioning() {
    let mut limit = LogicalLimit::new(BaseLogicalPlan::with_id(1, LogicalLimit::TYPE, 0), 3, 7);
    assert_eq!(limit.explain_info(), "offset:3, count:7");
    limit.partition_by = vec![crate::physical_property::SortItem::new(1, false)];
    assert_eq!(
        limit.explain_info(),
        "partition by 1 cols, offset:3, count:7"
    );
}

/// Go `LogicalLimit.convertToTopN` (`logical_limit.go:172`): the result is a
/// TopN with NO order — so `IsLimit()` — and does NOT carry `PartitionBy`.
#[test]
fn limit_converts_to_a_topn_that_is_still_a_limit() {
    let mut limit = LogicalLimit::new(BaseLogicalPlan::with_id(1, LogicalLimit::TYPE, 4), 2, 9);
    limit.prefer_limit_to_cop = true;
    limit.partition_by = vec![crate::physical_property::SortItem::new(1, false)];
    let topn = limit.convert_to_topn();
    assert!(topn.is_limit());
    assert_eq!(topn.offset, 2);
    assert_eq!(topn.count, 9);
    assert!(topn.prefer_limit_to_cop);
    assert!(topn.partition_by.is_empty());
    assert_eq!(topn.base.base.tp(), LogicalTopN::TYPE);
    assert_eq!(topn.base.base.query_block_offset(), 4);
}

/// Go `LogicalTopN.AttachChild` (`logical_top_n.go:200`), dual branch: the
/// dual absorbs the window and the TopN disappears.
#[test]
fn topn_attach_child_folds_into_a_table_dual() {
    let dual = |rows: usize| {
        LogicalPlan::TableDual(super::LogicalTableDual {
            base: BaseLogicalPlan::with_id(1, "TableDual", 0),
            row_count: rows,
        })
    };
    let topn = LogicalTopN::new(
        BaseLogicalPlan::with_id(2, LogicalTopN::TYPE, 0),
        vec![],
        2,
        3,
    );
    let LogicalPlan::TableDual(folded) = topn.attach_child(dual(10)) else {
        panic!("a dual child must absorb the TopN");
    };
    // min(10 - 2, 3) == 3.
    assert_eq!(folded.row_count, 3);

    let topn = LogicalTopN::new(
        BaseLogicalPlan::with_id(2, LogicalTopN::TYPE, 0),
        vec![],
        5,
        3,
    );
    let LogicalPlan::TableDual(folded) = topn.attach_child(dual(4)) else {
        panic!("a dual child must absorb the TopN");
    };
    // The offset skips past the end: nothing is left.
    assert_eq!(folded.row_count, 0);
}

/// Go `LogicalTopN.AttachChild` (`logical_top_n.go:213`), limit branch: with
/// no `ByItems` the TopN becomes a `LogicalLimit` that DOES carry
/// `PartitionBy`.
#[test]
fn topn_attach_child_degrades_to_a_limit_without_by_items() {
    let mut topn = LogicalTopN::new(
        BaseLogicalPlan::with_id(2, LogicalTopN::TYPE, 0),
        vec![],
        1,
        4,
    );
    topn.partition_by = vec![crate::physical_property::SortItem::new(8, true)];
    topn.prefer_limit_to_cop = true;
    let child = LogicalPlan::TableDual(super::LogicalTableDual {
        base: BaseLogicalPlan::with_id(1, "TableDual", 0),
        row_count: 1,
    });
    // A dual would be absorbed, so use a selection to reach the limit branch.
    let child = LogicalPlan::Selection(LogicalSelection::new(
        {
            let mut base = BaseLogicalPlan::with_id(3, LogicalSelection::TYPE, 0);
            base.set_children(vec![child]);
            base
        },
        vec![],
    ));
    let LogicalPlan::Limit(limit) = topn.attach_child(child) else {
        panic!("a TopN with no ByItems must become a Limit");
    };
    assert_eq!(limit.offset, 1);
    assert_eq!(limit.count, 4);
    assert!(limit.prefer_limit_to_cop);
    assert_eq!(limit.partition_by.len(), 1);
    assert_eq!(limit.base.child_len(), 1);
    assert_eq!(limit.base.base.tp(), LogicalLimit::TYPE);
}

/// Go `LogicalTopN.AttachChild` (`logical_top_n.go:224`), default branch.
#[test]
fn topn_attach_child_keeps_a_real_topn() {
    let topn = LogicalTopN::new(
        BaseLogicalPlan::with_id(2, LogicalTopN::TYPE, 0),
        vec![by(col_expr(1), false)],
        0,
        4,
    );
    assert!(!topn.is_limit());
    let child = LogicalPlan::Selection(LogicalSelection::new(
        BaseLogicalPlan::with_id(3, LogicalSelection::TYPE, 0),
        vec![],
    ));
    let attached = topn.attach_child(child);
    assert!(matches!(attached, LogicalPlan::TopN(_)));
    assert_eq!(attached.children().len(), 1);
}

/// Go `LogicalTopN.ExplainInfo` (`logical_top_n.go:50`): the offset/count
/// suffix is exact and the two lists are never silently missing.
#[test]
fn topn_explain_info_reports_every_list_it_carries() {
    let mut topn = LogicalTopN::new(
        BaseLogicalPlan::with_id(1, LogicalTopN::TYPE, 0),
        vec![by(col_expr(1), false)],
        0,
        5,
    );
    assert_eq!(topn.explain_info(), "1 by items, offset:0, count:5");
    topn.partition_by = vec![crate::physical_property::SortItem::new(2, false)];
    assert_eq!(
        topn.explain_info(),
        "partition by 1 cols order by 1 by items, offset:0, count:5"
    );
    topn.by_items.clear();
    assert_eq!(
        topn.explain_info(),
        "partition by 1 cols, offset:0, count:5"
    );
}

/// `LogicalTopN` reaches every enum dispatch the keystone requires.
#[test]
fn topn_is_wired_into_the_enum_dispatches() {
    let mut plan = LogicalPlan::TopN(LogicalTopN::new(
        BaseLogicalPlan::with_id(1, LogicalTopN::TYPE, 0),
        vec![by(col_expr(1), false)],
        0,
        1,
    ));
    let mut output = schema(&[1]);
    plan.build_key_info(&mut output, &[schema(&[1])]);
    assert!(plan.max_one_row());
    assert!(plan.extract_correlated_cols().is_empty());
    assert!(plan.pull_up_constant_predicates().is_empty());
    assert!(plan.extract_col_groups(&[]).is_empty());
    assert!(plan.explain_info().contains("count:1"));
    assert!(matches!(plan.clone_shallow(), LogicalPlan::TopN(_)));
}

// ***** LogicalUnionAll / LogicalPartitionUnionAll *****

/// Go `LogicalUnionAll.PruneColumns` (`logical_union_all.go:60`): the parent's
/// set reaches every child unchanged when it used something.
#[test]
fn union_all_pruning_pushes_the_parent_set_unchanged() {
    let mut output = schema(&[1, 2, 3]);
    let pruning = LogicalUnionAll::prune_columns_local(&[column(1), column(3)], &mut output);
    assert!(pruning.has_been_used);
    assert_eq!(
        pruning
            .child_used_cols
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    // Column 2 is dropped from the union's own schema.
    assert_eq!(
        output
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(pruning.pruned_columns.len(), 1);
    assert_eq!(pruning.pruned_columns[0].unique_id, 2);
}

/// Go's `!hasBeenUsed` escape (`logical_union_all.go:74`): a union that the
/// parent reads nothing from keeps its WHOLE schema rather than collapsing.
#[test]
fn union_all_pruning_keeps_everything_when_the_parent_uses_nothing() {
    let mut output = schema(&[1, 2, 3]);
    let pruning = LogicalUnionAll::prune_columns_local(&[column(99)], &mut output);
    assert!(!pruning.has_been_used);
    assert_eq!(
        pruning
            .child_used_cols
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(pruning.pruned_columns.is_empty());
    assert_eq!(output.len(), 3);
}

/// Go's repair test (`logical_union_all.go:136`).
#[test]
fn union_all_repairs_only_a_child_wider_than_itself() {
    assert!(LogicalUnionAll::child_needs_pruning_projection(2, 3));
    assert!(!LogicalUnionAll::child_needs_pruning_projection(3, 3));
    assert!(!LogicalUnionAll::child_needs_pruning_projection(4, 3));
}

/// Go `LogicalUnionAll.PushDownTopN` (`logical_union_all.go:159`): the child
/// copy folds the offset into the count and keeps no offset of its own.
#[test]
fn union_all_child_topn_folds_the_offset_into_the_count() {
    let mut topn = LogicalTopN::new(
        BaseLogicalPlan::with_id(1, LogicalTopN::TYPE, 2),
        vec![by(col_expr(7), true)],
        10,
        5,
    );
    topn.prefer_limit_to_cop = true;
    topn.partition_by = vec![crate::physical_property::SortItem::new(1, false)];
    let child = LogicalUnionAll::push_down_topn_for_child(&topn);
    assert_eq!(child.offset, 0);
    assert_eq!(child.count, 15);
    assert!(child.prefer_limit_to_cop);
    assert_eq!(child.by_items.len(), 1);
    assert!(child.by_items[0].desc);
    // Go builds the copy from `LogicalTopN{Count, PreferLimitToCop}` alone, so
    // PartitionBy does NOT travel with it.
    assert!(child.partition_by.is_empty());
    assert!(child.base.children().is_empty());
}

/// Go `LogicalUnionAll.DeriveStats` (`logical_union_all.go:187`): rows and
/// NDVs both ADD across the branches, and a column a branch lacks contributes
/// zero rather than being dropped.
#[test]
fn union_all_derive_stats_adds_rows_and_ndvs() {
    let mut union = LogicalUnionAll::new(BaseLogicalPlan::with_id(1, LogicalUnionAll::TYPE, 0));
    let output = schema(&[1, 2]);
    let (stats, derived) = union.derive_stats(
        &[
            StatsInfo::new(100.0, [(1_i64, 10.0), (2, 4.0)]),
            StatsInfo::new(50.0, [(1_i64, 7.0)]),
        ],
        &output,
        &[true],
    );
    assert!(derived);
    assert!((stats.row_count() - 150.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&1] - 17.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&2] - 4.0).abs() < 1e-9);
    // Memoised without a reload.
    let (_, derived) = union.derive_stats(&[], &output, &[false]);
    assert!(!derived);
}

/// Go `LogicalUnionAll.PreparePossibleProperties` (`logical_union_all.go:206`):
/// no order survives a union, and TiFlash needs EVERY branch.
#[test]
fn union_all_offers_no_order_and_needs_every_branch_on_tiflash() {
    let mut union = LogicalUnionAll::new(BaseLogicalPlan::with_id(1, LogicalUnionAll::TYPE, 0));
    let ordered = PossiblePropertiesInfo {
        orders: vec![vec![column(5)]],
        has_tiflash: true,
    };
    let info = union.prepare_possible_properties(&[Some(ordered.clone()), Some(ordered.clone())]);
    assert!(info.orders.is_empty());
    assert!(info.has_tiflash);
    let mixed = PossiblePropertiesInfo {
        orders: vec![],
        has_tiflash: false,
    };
    assert!(
        !union
            .prepare_possible_properties(&[Some(ordered), Some(mixed)])
            .has_tiflash
    );
    // No children at all is not TiFlash-capable.
    assert!(!union.prepare_possible_properties(&[]).has_tiflash);
}

/// Go `LogicalUnionAll.PredicatePushDown` (`logical_union_all.go:45`) returns
/// `nil` to its parent: a union never holds a predicate itself.
#[test]
fn union_all_keeps_no_predicate_for_its_parent() {
    assert!(LogicalUnionAll::predicate_push_down_local().is_empty());
}

/// `LogicalPartitionUnionAll` is Go's embedding, and both variants reach every
/// enum dispatch.
#[test]
fn partition_union_all_embeds_the_union_and_is_wired_into_the_enum() {
    let partition = LogicalPartitionUnionAll::new(BaseLogicalPlan::with_id(
        7,
        LogicalPartitionUnionAll::TYPE,
        1,
    ));
    let plan = LogicalPlan::PartitionUnionAll(partition);
    assert_eq!(plan.id(), 7);
    assert_eq!(plan.tp(), LogicalPartitionUnionAll::TYPE);
    assert_eq!(plan.query_block_offset(), 1);
    assert!(plan.extract_correlated_cols().is_empty());
    assert!(plan.pull_up_constant_predicates().is_empty());
    assert!(plan.extract_col_groups(&[]).is_empty());
    assert_eq!(plan.explain_info(), "");
    assert!(matches!(
        plan.clone_shallow(),
        LogicalPlan::PartitionUnionAll(_)
    ));

    let plan = LogicalPlan::UnionAll(LogicalUnionAll::new(BaseLogicalPlan::with_id(
        8,
        LogicalUnionAll::TYPE,
        0,
    )));
    assert_eq!(plan.tp(), LogicalUnionAll::TYPE);
    assert!(matches!(plan.clone_shallow(), LogicalPlan::UnionAll(_)));
}

// ***** LogicalApply *****

fn apply(join_type: LogicalJoinType) -> LogicalApply {
    LogicalApply::new(
        BaseLogicalPlan::with_id(1, LogicalApply::TYPE, 0),
        join_type,
    )
}

fn cor(unique_id: i64) -> tidb_expr::column::CorrelatedColumn {
    tidb_expr::column::CorrelatedColumn {
        column: column(unique_id),
        data: None,
    }
}

/// Go's apply-elimination test (`logical_apply.go:110`): LATERAL is the veto,
/// because it breaks the max-one-row guarantee the rewrite depends on.
#[test]
fn apply_elimination_is_vetoed_by_lateral() {
    let mut la = apply(LogicalJoinType::LeftOuter);
    assert!(la.can_eliminate_apply(true, true));
    // The fix-control switch is off.
    assert!(!la.can_eliminate_apply(false, true));
    // The inner side still contributes a column.
    assert!(!la.can_eliminate_apply(true, false));
    la.is_lateral = true;
    assert!(!la.can_eliminate_apply(true, true));
    // Only LEFT OUTER may be eliminated.
    assert!(!apply(LogicalJoinType::Inner).can_eliminate_apply(true, true));
    assert!(!apply(LogicalJoinType::Semi).can_eliminate_apply(true, true));
}

/// Go `LogicalApply.ExtractCorrelatedCols` (`logical_apply.go:250`): a
/// correlated column the OUTER child supplies is resolved here and does not
/// travel further out.
#[test]
fn apply_hides_the_correlated_cols_its_outer_child_resolves() {
    let mut la = apply(LogicalJoinType::Inner);
    la.join.other_conditions = vec![
        Expression::CorrelatedColumn(cor(1)),
        Expression::CorrelatedColumn(cor(9)),
    ];
    let outer = schema(&[1, 2]);
    let survivors = la.extract_correlated_cols(&outer);
    assert_eq!(survivors.len(), 1);
    assert_eq!(survivors[0].column.unique_id, 9);

    // Through the enum, with the outer child supplying the schema.
    let mut base = BaseLogicalPlan::with_id(2, LogicalApply::TYPE, 0);
    let mut outer_base = BaseLogicalPlan::with_id(3, LogicalSelection::TYPE, 0);
    outer_base.base.set_schema(Some(outer));
    base.set_children(vec![
        LogicalPlan::Selection(LogicalSelection::new(outer_base, vec![])),
        LogicalPlan::Selection(LogicalSelection::new(
            BaseLogicalPlan::with_id(4, LogicalSelection::TYPE, 0),
            vec![],
        )),
    ]);
    la.join.base = base;
    let plan = LogicalPlan::Apply(la);
    assert_eq!(plan.extract_correlated_cols().len(), 1);
    // A two-child operator, so the join accessor answers.
    assert!(plan.get_join_child_stats_and_schema().is_some());
}

/// Go `LogicalApply.DeriveStats` (`logical_apply.go:157`), scalar path: the
/// row count is the OUTER child's, and every inner column's NDV becomes it.
#[test]
fn apply_derive_stats_keeps_the_outer_row_count_for_a_scalar_subquery() {
    let mut la = apply(LogicalJoinType::LeftOuter);
    let output = schema(&[1, 2, 30]);
    let (stats, derived) = la
        .derive_stats(
            &[
                StatsInfo::new(200.0, [(1_i64, 20.0), (2, 5.0)]),
                StatsInfo::new(7.0, [(30_i64, 7.0)]),
            ],
            &output,
            2,
            None,
            &[true],
        )
        .expect("both children have profiles");
    assert!(derived);
    assert!((stats.row_count() - 200.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&1] - 20.0).abs() < 1e-9);
    // The one inner column takes the apply's row count.
    assert!((stats.col_ndvs()[&30] - 200.0).abs() < 1e-9);
}

/// Go's `SemiJoin`/`AntiSemiJoin` branch (`logical_apply.go:213`): an
/// undecorrelatable `EXISTS` is scaled by the selection factor.
#[test]
fn apply_derive_stats_scales_a_semi_apply_by_the_selection_factor() {
    let mut la = apply(LogicalJoinType::Semi);
    let output = schema(&[1]);
    let (stats, _) = la
        .derive_stats(
            &[
                StatsInfo::new(100.0, [(1_i64, 10.0)]),
                StatsInfo::new(3.0, []),
            ],
            &output,
            1,
            None,
            &[true],
        )
        .unwrap();
    assert!((stats.row_count() - 100.0 * SELECTION_FACTOR).abs() < 1e-9);
}

/// Go's `LeftOuterSemiJoin` branch (`logical_apply.go:230`): the marker column
/// is two-valued, not row-count-valued.
#[test]
fn apply_derive_stats_marks_a_left_outer_semi_marker_at_two() {
    let mut la = apply(LogicalJoinType::LeftOuterSemi);
    let output = schema(&[1, 2, 99]);
    let (stats, _) = la
        .derive_stats(
            &[
                StatsInfo::new(40.0, [(1_i64, 4.0), (2, 2.0)]),
                StatsInfo::new(9.0, []),
            ],
            &output,
            2,
            None,
            &[true],
        )
        .unwrap();
    assert!((stats.row_count() - 40.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&99] - 2.0).abs() < 1e-9);
}

/// Go's LATERAL branch (`logical_apply.go:172`): the caller's estimate wins,
/// floored at the outer count for a left outer apply, and the Cartesian
/// fallback is Go's own third branch.
#[test]
fn apply_derive_stats_takes_the_lateral_estimate_and_floors_it() {
    let output = schema(&[1, 50]);
    let children = [
        StatsInfo::new(80.0, [(1_i64, 8.0)]),
        StatsInfo::new(3.0, [(50_i64, 3.0)]),
    ];

    let mut la = apply(LogicalJoinType::Inner);
    la.is_lateral = true;
    let (stats, _) = la
        .derive_stats(&children, &output, 1, Some(500.0), &[true])
        .unwrap();
    assert!((stats.row_count() - 500.0).abs() < 1e-9);

    // A left outer apply never drops below its outer count.
    let mut la = apply(LogicalJoinType::LeftOuter);
    la.is_lateral = true;
    let (stats, _) = la
        .derive_stats(&children, &output, 1, Some(5.0), &[true])
        .unwrap();
    assert!((stats.row_count() - 80.0).abs() < 1e-9);

    // No estimate and no correlation: Go's Cartesian bound.
    let mut la = apply(LogicalJoinType::Inner);
    la.is_lateral = true;
    let (stats, _) = la
        .derive_stats(&children, &output, 1, None, &[true])
        .unwrap();
    assert!((stats.row_count() - 240.0).abs() < 1e-9);
}

/// `needs_lateral_row_count_estimate` names exactly Go's two estimator
/// branches, so a caller cannot take the Cartesian fallback by accident.
#[test]
fn apply_reports_when_the_lateral_estimate_is_mandatory() {
    let mut la = apply(LogicalJoinType::Inner);
    // Not lateral at all.
    assert!(!la.needs_lateral_row_count_estimate());
    la.is_lateral = true;
    // Lateral, but neither join keys nor correlation: Go's third branch.
    assert!(!la.needs_lateral_row_count_estimate());
    la.cor_cols = vec![cor(1)];
    assert!(la.needs_lateral_row_count_estimate());
    la.cor_cols.clear();
    la.join.equal_conditions = vec![call("eq", vec![col_expr(1), col_expr(2)])];
    assert!(la.needs_lateral_row_count_estimate());
    // A semi apply is never on the lateral path.
    la.join.join_type = LogicalJoinType::Semi;
    assert!(!la.needs_lateral_row_count_estimate());
}

/// Go `LogicalApply.CanPullUpAgg` (`logical_apply.go:305`): no conditions, an
/// inner or left-outer join type, and a key on the outer side.
#[test]
fn apply_can_pull_up_agg_only_with_a_keyed_unconditional_outer() {
    let mut keyed = schema(&[1]);
    keyed.pk_or_uk = vec![vec![column(1)]];

    let la = apply(LogicalJoinType::Inner);
    assert!(la.can_pull_up_agg(&keyed));
    // No key on the outer side.
    assert!(!la.can_pull_up_agg(&schema(&[1])));

    let mut la = apply(LogicalJoinType::Inner);
    la.join.other_conditions = vec![one()];
    assert!(!la.can_pull_up_agg(&keyed));

    assert!(!apply(LogicalJoinType::Semi).can_pull_up_agg(&keyed));
}

/// Go `LogicalApply.ExtractColGroups` (`logical_apply.go:228`): only an
/// outer-preserving apply passes a group down, and never a right outer one —
/// "Apply doesn't have RightOuterJoin".
#[test]
fn apply_passes_col_groups_only_through_a_preserved_outer_side() {
    assert!(apply(LogicalJoinType::LeftOuter).col_groups_outer_side());
    assert!(apply(LogicalJoinType::LeftOuterSemi).col_groups_outer_side());
    assert!(apply(LogicalJoinType::AntiLeftOuterSemi).col_groups_outer_side());
    assert!(!apply(LogicalJoinType::Inner).col_groups_outer_side());
    assert!(!apply(LogicalJoinType::Semi).col_groups_outer_side());
}

/// Go `LogicalApply.DeCorColFromEqExpr` (`logical_apply.go:316`): both
/// argument orders normalise to `decorrelated = col`, and a correlated column
/// that does not resolve is refused.
#[test]
fn apply_decorrelates_an_equality_in_either_argument_order() {
    let la = apply(LogicalJoinType::Inner);
    let resolvable = schema(&[7]);
    for expr in [
        eq(col_expr(3), Expression::CorrelatedColumn(cor(7))),
        eq(Expression::CorrelatedColumn(cor(7)), col_expr(3)),
    ] {
        let rewritten = la
            .de_cor_col_from_eq_expr(&expr, &resolvable)
            .expect("a resolvable correlated column decorrelates");
        let Expression::ScalarFunction(function) = &rewritten else {
            panic!("the result is an equality");
        };
        let args = function.get_args();
        // Left is the decorrelated (left join key) side, right is the column.
        assert_eq!(args[0].as_column().unwrap().unique_id, 7);
        assert_eq!(args[1].as_column().unwrap().unique_id, 3);
    }
    // The schema does not contain the correlated column, so it stays correlated.
    assert!(la
        .de_cor_col_from_eq_expr(
            &eq(col_expr(3), Expression::CorrelatedColumn(cor(7))),
            &schema(&[100])
        )
        .is_none());
    // Not an equality at all.
    assert!(la
        .de_cor_col_from_eq_expr(
            &Expression::ScalarFunction(call("lt", vec![col_expr(3), col_expr(7)])),
            &resolvable
        )
        .is_none());
    // Two plain columns.
    assert!(la
        .de_cor_col_from_eq_expr(&eq(col_expr(3), col_expr(7)), &resolvable)
        .is_none());
}

/// Go `findChildFullSchema` (`logical_apply.go:84`): sees THROUGH the
/// selections an `ON` clause leaves behind, and stops at anything else.
#[test]
fn find_child_full_schema_sees_through_on_clause_selections() {
    let mut join = LogicalJoin::new(
        BaseLogicalPlan::with_id(1, LogicalJoin::TYPE, 0),
        LogicalJoinType::Inner,
    );
    join.full_schema = Some(schema(&[1, 2, 3]));
    let wrapped = LogicalPlan::Selection(LogicalSelection::new(
        {
            let mut base = BaseLogicalPlan::with_id(2, LogicalSelection::TYPE, 0);
            base.set_children(vec![LogicalPlan::Join(join)]);
            base
        },
        vec![],
    ));
    assert_eq!(find_child_full_schema(&wrapped).unwrap().len(), 3);

    // A selection over something that is not a join has no full schema.
    let plain = LogicalPlan::Selection(LogicalSelection::new(
        {
            let mut base = BaseLogicalPlan::with_id(3, LogicalSelection::TYPE, 0);
            base.set_children(vec![LogicalPlan::TableDual(super::LogicalTableDual {
                base: BaseLogicalPlan::with_id(4, "TableDual", 0),
                row_count: 1,
            })]);
            base
        },
        vec![],
    ));
    assert!(find_child_full_schema(&plain).is_none());
    // A childless selection stops rather than looping.
    assert!(
        find_child_full_schema(&LogicalPlan::Selection(LogicalSelection::new(
            BaseLogicalPlan::with_id(5, LogicalSelection::TYPE, 0),
            vec![]
        )))
        .is_none()
    );
}

/// Every correlated column widens the OUTER child's used set
/// (`logical_apply.go:130`).
#[test]
fn apply_widens_the_outer_used_set_by_its_correlated_cols() {
    let mut la = apply(LogicalJoinType::Inner);
    la.cor_cols = vec![cor(11), cor(12)];
    let mut left_cols = vec![column(1)];
    assert_eq!(la.widen_outer_used_cols(&mut left_cols), 2);
    assert_eq!(
        left_cols.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 11, 12]
    );
}

// ***** LogicalWindow *****

fn window_desc(args: Vec<Expression>) -> tidb_expr::aggregation::WindowFuncDesc {
    tidb_expr::aggregation::WindowFuncDesc {
        base: BaseFuncDesc {
            name: "row_number".to_owned(),
            args,
            ret_type: tiny(),
        },
    }
}

fn window(descs: usize) -> LogicalWindow {
    LogicalWindow::new(
        BaseLogicalPlan::with_id(1, LogicalWindow::TYPE, 0),
        (0..descs).map(|_| window_desc(vec![])).collect(),
    )
}

/// Go `LogicalWindow.PredicatePushDown` (`logical_window.go:334`): only a
/// predicate written entirely in PARTITION BY columns may cross the window.
#[test]
fn window_pushes_down_only_partition_column_predicates() {
    let mut w = window(1);
    w.partition_by = vec![WindowSortItem::new(column(1), false)];
    let (pushed, kept) = w.predicate_push_down(&[
        eq(col_expr(1), one()),
        eq(col_expr(2), one()),
        eq(col_expr(1), col_expr(2)),
    ]);
    assert_eq!(pushed.len(), 1);
    assert_eq!(kept.len(), 2);
    // With no PARTITION BY at all, nothing crosses.
    let w = window(1);
    let (pushed, kept) = w.predicate_push_down(&[eq(col_expr(1), one())]);
    assert!(pushed.is_empty());
    assert_eq!(kept.len(), 1);
}

/// Go `LogicalWindow.GetWindowResultColumns` (`logical_window.go:558`): the
/// TRAILING columns, one per descriptor.
#[test]
fn window_result_columns_are_the_trailing_schema_slots() {
    let w = window(2);
    let output = schema(&[1, 2, 90, 91]);
    let result = w.get_window_result_columns(&output);
    assert_eq!(
        result.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![90, 91]
    );
}

/// Go `LogicalWindow.PruneColumns` (`logical_window.go:352`): the window's own
/// outputs are stripped from the parent's set before the child sees it, and
/// everything the window reads is added.
#[test]
fn window_pruning_strips_its_own_outputs_and_adds_what_it_reads() {
    let mut w = LogicalWindow::new(
        BaseLogicalPlan::with_id(1, LogicalWindow::TYPE, 0),
        vec![window_desc(vec![col_expr(5)])],
    );
    w.partition_by = vec![WindowSortItem::new(column(6), false)];
    w.order_by = vec![WindowSortItem::new(column(7), true)];
    let output = schema(&[1, 2, 90]);
    // The parent asks for a child column and for the window's own output.
    let used = w.prune_columns_local(&[column(1), column(90)], &output);
    assert_eq!(
        used.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 5, 6, 7]
    );

    // The rebuild re-appends the window columns above the pruned child.
    let rebuilt = w.rebuild_schema_after_pruning(&schema(&[1]), &[column(90)]);
    assert_eq!(
        rebuilt
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![1, 90]
    );
}

/// Go `LogicalWindow.DeriveStats` (`logical_window.go:398`): rows pass
/// through, child NDVs pass through, and each window output is assumed
/// distinct per row.
#[test]
fn window_derive_stats_gives_each_result_column_the_row_count() {
    let mut w = window(1);
    let output = schema(&[1, 2, 90]);
    let (stats, derived) = w
        .derive_stats(
            &[StatsInfo::new(500.0, [(1_i64, 50.0), (2, 3.0)])],
            &output,
            &[true],
        )
        .unwrap();
    assert!(derived);
    assert!((stats.row_count() - 500.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&1] - 50.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&90] - 500.0).abs() < 1e-9);
}

/// Go `LogicalWindow.PreparePossibleProperties` (`logical_window.go:437`):
/// PARTITION BY then ORDER BY, as one offered order.
#[test]
fn window_offers_partition_by_then_order_by() {
    let mut w = window(1);
    w.partition_by = vec![WindowSortItem::new(column(3), false)];
    w.order_by = vec![WindowSortItem::new(column(4), true)];
    let info = w.prepare_possible_properties(Some(&PossiblePropertiesInfo {
        orders: vec![vec![column(99)]],
        has_tiflash: true,
    }));
    assert!(info.has_tiflash);
    assert_eq!(
        info.orders[0]
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    // Go builds the Orders slice unconditionally, so an unpartitioned,
    // unordered window still offers ONE (empty) order.
    let mut w = window(1);
    let info = w.prepare_possible_properties(None);
    assert_eq!(info.orders.len(), 1);
    assert!(info.orders[0].is_empty());
}

/// Go `LogicalWindow.ExtractCorrelatedCols` (`logical_window.go:471`): the
/// arguments and BOTH bounds' `CalcFuncs`, and NOT `CompareCols`.
#[test]
fn window_extracts_correlated_cols_from_args_and_frame_calc_funcs() {
    let cor = |id| {
        Expression::CorrelatedColumn(tidb_expr::column::CorrelatedColumn {
            column: column(id),
            data: None,
        })
    };
    let mut w = LogicalWindow::new(
        BaseLogicalPlan::with_id(1, LogicalWindow::TYPE, 0),
        vec![window_desc(vec![cor(1)])],
    );
    w.frame = Some(WindowFrame {
        frame_type: FrameType::Ranges,
        start: Some(FrameBound {
            calc_funcs: vec![cor(2)],
            // Deliberately correlated, and deliberately NOT counted.
            compare_cols: vec![cor(3)],
            ..FrameBound::default()
        }),
        end: Some(FrameBound {
            calc_funcs: vec![cor(4)],
            ..FrameBound::default()
        }),
    });
    let cor_cols = w.extract_correlated_cols();
    assert_eq!(
        cor_cols
            .iter()
            .map(|c| c.column.unique_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 4]
    );
    assert_eq!(LogicalPlan::Window(w).extract_correlated_cols().len(), 3);
}

/// Go `EqualPartitionBy` is a SET test and `EqualOrderBy` is a SEQUENCE test
/// (`logical_window.go:500`, `:516`).
#[test]
fn window_partition_equality_ignores_order_but_order_equality_does_not() {
    let mut a = window(1);
    a.partition_by = vec![
        WindowSortItem::new(column(1), false),
        WindowSortItem::new(column(2), false),
    ];
    a.order_by = vec![
        WindowSortItem::new(column(3), false),
        WindowSortItem::new(column(4), true),
    ];
    let mut b = window(1);
    b.partition_by = vec![
        WindowSortItem::new(column(2), true),
        WindowSortItem::new(column(1), false),
    ];
    b.order_by = a.order_by.clone();
    assert!(a.equal_partition_by(&b));
    assert!(a.equal_order_by(&b));

    // Swapping the ORDER BY sequence breaks it.
    b.order_by.swap(0, 1);
    assert!(!a.equal_order_by(&b));
    // A different direction breaks it too.
    b.order_by = vec![
        WindowSortItem::new(column(3), true),
        WindowSortItem::new(column(4), true),
    ];
    assert!(!a.equal_order_by(&b));
    // A different partition column set breaks partition equality.
    b.partition_by = vec![
        WindowSortItem::new(column(1), false),
        WindowSortItem::new(column(9), false),
    ];
    assert!(!a.equal_partition_by(&b));
}

/// Go `LogicalWindow.EqualFrame` (`logical_window.go:530`) compares the frame
/// SHAPE only — deliberately not `CompareCols`, `CmpFuncs` or `CmpDataType`.
#[test]
fn window_equal_frame_ignores_the_derived_comparison_fields() {
    let bound = |num, tokens: Vec<&str>| FrameBound {
        bound_type: BoundType::Preceding,
        unbounded: false,
        num,
        calc_funcs: vec![col_expr(1)],
        compare_cols: vec![],
        cmp_func_tokens: tokens.into_iter().map(str::to_owned).collect(),
        cmp_data_type: RangeCmpDataType::Int,
        is_explicit_range: false,
    };
    let frame = |num, tokens: Vec<&str>| WindowFrame {
        frame_type: FrameType::Ranges,
        start: Some(bound(num, tokens)),
        end: Some(bound(num, vec![])),
    };
    let mut a = window(1);
    a.frame = Some(frame(1, vec!["0x1"]));
    let mut b = window(1);
    b.frame = Some(frame(1, vec!["0xdeadbeef"]));
    assert!(a.equal_frame(&b));
    // A different Num is a different frame.
    b.frame = Some(frame(2, vec!["0x1"]));
    assert!(!a.equal_frame(&b));
    // One frame present, one absent.
    b.frame = None;
    assert!(!a.equal_frame(&b));
    // Both absent.
    a.frame = None;
    assert!(a.equal_frame(&b));
}

/// Go `FrameBound.Hash64`/`Equals` (`logical_window.go:112`, `:148`), which
/// this file merged from the crate's former `window_frame` leaf. The compare
/// functions are part of the identity, by ADDRESS.
#[test]
fn frame_bound_identity_includes_the_compare_function_tokens() {
    let base = FrameBound {
        bound_type: BoundType::Following,
        unbounded: true,
        num: 3,
        calc_funcs: vec![col_expr(1)],
        compare_cols: vec![col_expr(2)],
        cmp_func_tokens: vec!["CompareInt".to_owned()],
        cmp_data_type: RangeCmpDataType::Int,
        is_explicit_range: true,
    };
    let same = base.clone();
    assert_eq!(base.hash64(), same.hash64());
    assert!(base.equals(&same));

    let mut different = base.clone();
    different.cmp_func_tokens = vec!["CompareTime".to_owned()];
    assert_ne!(base.hash64(), different.hash64());
    assert!(!base.equals(&different));

    // An empty CalcFuncs list takes the nil branch and hashes differently.
    let mut empty = base.clone();
    empty.calc_funcs.clear();
    assert_ne!(base.hash64(), empty.hash64());
}

/// Go `FrameBound.UpdateCmpFuncsAndCmpDataType` (`logical_window.go:207`),
/// including its deliberate fall-through for a type that matches no arm.
#[test]
fn frame_bound_update_cmp_maps_each_eval_type_and_ignores_the_rest() {
    for (eval, token, data) in [
        (EvalType::Int, "CompareInt", RangeCmpDataType::Int),
        (
            EvalType::Datetime,
            "CompareTime",
            RangeCmpDataType::DateTime,
        ),
        (
            EvalType::Timestamp,
            "CompareTime",
            RangeCmpDataType::DateTime,
        ),
        (
            EvalType::Duration,
            "CompareDuration",
            RangeCmpDataType::Duration,
        ),
        (EvalType::Real, "CompareReal", RangeCmpDataType::Float),
        (
            EvalType::Decimal,
            "CompareDecimal",
            RangeCmpDataType::Decimal,
        ),
    ] {
        let mut bound = FrameBound {
            cmp_func_tokens: vec![String::new()],
            cmp_data_type: RangeCmpDataType::Float,
            ..FrameBound::default()
        };
        bound.update_cmp_funcs_and_cmp_data_type(eval);
        assert_eq!(bound.cmp_func_tokens[0], token);
        assert_eq!(bound.cmp_data_type, data);
    }
    // ETString matches nothing and leaves the bound untouched.
    let mut bound = FrameBound {
        cmp_func_tokens: vec!["untouched".to_owned()],
        cmp_data_type: RangeCmpDataType::Decimal,
        ..FrameBound::default()
    };
    bound.update_cmp_funcs_and_cmp_data_type(EvalType::String);
    assert_eq!(bound.cmp_func_tokens[0], "untouched");
    assert_eq!(bound.cmp_data_type, RangeCmpDataType::Decimal);
}

/// Go `WindowFrame.Hash64` (`logical_window.go:50`) folds in only ONE bound —
/// `Start` when it is present, `End` when it is not. That quirk is preserved,
/// while `Equals` compares both.
#[test]
fn window_frame_hash_folds_one_bound_but_equals_compares_both() {
    let bound = |num| FrameBound {
        num,
        ..FrameBound::default()
    };
    let a = WindowFrame {
        frame_type: FrameType::Rows,
        start: Some(bound(1)),
        end: Some(bound(2)),
    };
    let b = WindowFrame {
        frame_type: FrameType::Rows,
        start: Some(bound(1)),
        end: Some(bound(99)),
    };
    // Go's Hash64 never reaches `End` when `Start` is set.
    assert_eq!(a.hash64(), b.hash64());
    // Equals does reach it.
    assert!(!a.equals(&b));
    assert!(a.equals(&WindowFrame {
        frame_type: FrameType::Rows,
        start: Some(bound(1)),
        end: Some(bound(2)),
    }));
    // A different frame type separates them everywhere.
    assert!(!a.equals(&WindowFrame {
        frame_type: FrameType::Ranges,
        start: Some(bound(1)),
        end: Some(bound(2)),
    }));
}

/// Go `LogicalWindow.CheckComparisonForTiFlash` (`logical_window.go:568`):
/// Duration against Datetime is refused in either direction.
#[test]
fn window_refuses_duration_against_datetime_on_tiflash() {
    let typed = |code| Column::new(1, FieldType::new(code));
    let bound = |code| FrameBound {
        calc_funcs: vec![Expression::Column(Column::new(2, FieldType::new(code)))],
        compare_cols: vec![col_expr(3)],
        ..FrameBound::default()
    };
    let mut w = window(1);
    w.order_by = vec![WindowSortItem::new(typed(FieldTypeCode::Duration), false)];
    assert!(!w.check_comparison_for_tiflash(&bound(FieldTypeCode::Datetime)));
    assert!(!w.check_comparison_for_tiflash(&bound(FieldTypeCode::Timestamp)));
    assert!(w.check_comparison_for_tiflash(&bound(FieldTypeCode::LongLong)));

    let mut w = window(1);
    w.order_by = vec![WindowSortItem::new(typed(FieldTypeCode::Datetime), false)];
    assert!(!w.check_comparison_for_tiflash(&bound(FieldTypeCode::Duration)));

    // A bound with no CompareCols is not a range bound and is always fine.
    assert!(w.check_comparison_for_tiflash(&FrameBound::default()));
}

/// `LogicalWindow` reaches every enum dispatch.
#[test]
fn window_is_wired_into_the_enum_dispatches() {
    let plan = LogicalPlan::Window(window(1));
    assert_eq!(plan.tp(), LogicalWindow::TYPE);
    assert!(plan.pull_up_constant_predicates().is_empty());
    assert!(plan.extract_col_groups(&[]).is_empty());
    assert_eq!(plan.explain_info(), "");
    assert!(matches!(plan.clone_shallow(), LogicalPlan::Window(_)));
}

// ***** LogicalCTE / LogicalCTETable *****

fn cte_class(build: impl FnOnce(&mut CteClass)) -> std::rc::Rc<std::cell::RefCell<CteClass>> {
    let mut class = CteClass {
        is_outer_most_cte: true,
        ..CteClass::default()
    };
    build(&mut class);
    std::rc::Rc::new(std::cell::RefCell::new(class))
}

fn cte(class: std::rc::Rc<std::cell::RefCell<CteClass>>) -> LogicalCTE {
    LogicalCTE::new(BaseLogicalPlan::with_id(1, LogicalCTE::TYPE, 0), class)
}

fn cor_expr(unique_id: i64) -> Expression {
    Expression::CorrelatedColumn(tidb_expr::column::CorrelatedColumn {
        column: column(unique_id),
        data: None,
    })
}

/// Go `LogicalCTE.PredicatePushDown` (`logical_cte.go:103`): a recursive or
/// non-outermost CTE records nothing at all.
#[test]
fn cte_records_no_predicate_for_a_recursive_or_inner_cte() {
    let dual = || {
        Box::new(LogicalPlan::TableDual(super::LogicalTableDual {
            base: BaseLogicalPlan::with_id(9, "TableDual", 0),
            row_count: 1,
        }))
    };
    let recursive = cte(cte_class(|c| c.recursive_part_logical_plan = Some(dual())));
    assert!(matches!(
        recursive.predicate_push_down(&[eq(col_expr(1), one())]),
        CtePredicatePushDown::Unsupported
    ));
    let inner = cte(cte_class(|c| c.is_outer_most_cte = false));
    assert!(matches!(
        inner.predicate_push_down(&[eq(col_expr(1), one())]),
        CtePredicatePushDown::Unsupported
    ));
}

/// Go's correlated-column caution (`logical_cte.go:115`), which applies only
/// OUTSIDE an apply.
#[test]
fn cte_drops_correlated_predicates_unless_it_is_inside_an_apply() {
    let plain = cte(cte_class(|_| {}));
    let predicates = [eq(col_expr(1), one()), eq(cor_expr(2), one())];
    let CtePredicatePushDown::Record(recorded) = plain.predicate_push_down(&predicates) else {
        panic!("an outermost non-recursive CTE records something");
    };
    assert_eq!(recorded.len(), 1);

    let in_apply = cte(cte_class(|c| c.is_in_apply = true));
    let CtePredicatePushDown::Record(recorded) = in_apply.predicate_push_down(&predicates) else {
        panic!("inside an apply, correlation is expected");
    };
    assert_eq!(recorded.len(), 2);

    // Every candidate dropped: Go records a literal `1` so this reference does
    // not let the others restrict the shared seed.
    assert!(matches!(
        plain.predicate_push_down(&[eq(cor_expr(2), one())]),
        CtePredicatePushDown::RecordAlwaysTrue
    ));
    // No predicates at all takes the same branch.
    assert!(matches!(
        plain.predicate_push_down(&[]),
        CtePredicatePushDown::RecordAlwaysTrue
    ));
}

/// Go `LogicalCTE.PushDownTopN` (`logical_cte.go:139`): the TopN is attached
/// ABOVE the CTE, never pushed into it — and it may still collapse to a limit.
#[test]
fn cte_attaches_a_topn_above_itself() {
    let plan = cte(cte_class(|_| {})).push_down_topn(None);
    assert!(matches!(plan, LogicalPlan::CTE(_)));

    let topn = LogicalTopN::new(
        BaseLogicalPlan::with_id(2, LogicalTopN::TYPE, 0),
        vec![by(col_expr(1), false)],
        0,
        5,
    );
    let plan = cte(cte_class(|_| {})).push_down_topn(Some(topn));
    assert!(matches!(plan, LogicalPlan::TopN(_)));
    assert!(matches!(plan.children()[0], LogicalPlan::CTE(_)));

    // A TopN with no ByItems is a limit, and AttachChild says so.
    let limit_shaped = LogicalTopN::new(
        BaseLogicalPlan::with_id(3, LogicalTopN::TYPE, 0),
        vec![],
        0,
        5,
    );
    let plan = cte(cte_class(|_| {})).push_down_topn(Some(limit_shaped));
    assert!(matches!(plan, LogicalPlan::Limit(_)));
}

/// Go `LogicalCTE.DeriveStats` (`logical_cte.go:167`): the NDV mapping is
/// POSITIONAL against the seed schema, and the seed profile is written THROUGH
/// the shared pointer so every `LogicalCTETable` sees it.
#[test]
fn cte_derive_stats_maps_seed_ndvs_positionally_and_publishes_the_seed_stat() {
    let seed_stat = std::rc::Rc::new(std::cell::RefCell::new(StatsInfo::new(0.0, [])));
    let mut c = cte(cte_class(|_| {}));
    c.seed_stat = Some(std::rc::Rc::clone(&seed_stat));

    // The CTE renames: its output ids differ from the seed's, position by
    // position.
    let seed_schema = schema(&[10, 11]);
    let self_schema = schema(&[20, 21]);
    let seed = StatsInfo::new(300.0, [(10_i64, 30.0), (11, 7.0)]);
    let (stats, derived) = c.derive_stats(&seed, &seed_schema, None, &self_schema, None, &[true]);
    assert!(derived);
    assert!((stats.row_count() - 300.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&20] - 30.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&21] - 7.0).abs() < 1e-9);
    // The shared seed profile was published.
    assert!((seed_stat.borrow().row_count() - 300.0).abs() < 1e-9);

    // A LogicalCTETable on the same storage adopts it.
    let mut table = LogicalCTETable::new(
        BaseLogicalPlan::with_id(5, LogicalCTETable::TYPE, 0),
        seed_stat,
    );
    let (table_stats, derived) = table.derive_stats(&[true]).unwrap();
    assert!(derived);
    assert!((table_stats.row_count() - 300.0).abs() < 1e-9);
    // Memoised without a reload.
    assert!(!table.derive_stats(&[false]).unwrap().1);
}

/// The recursive half (`logical_cte.go:203`): NDVs ADD, and `DISTINCT` takes
/// the caller's estimate instead of the row sum.
#[test]
fn cte_derive_stats_adds_the_recursive_part_unless_distinct() {
    let seed_schema = schema(&[10]);
    let recur_schema = schema(&[30]);
    let self_schema = schema(&[20]);
    let seed = StatsInfo::new(100.0, [(10_i64, 10.0)]);
    let recur = StatsInfo::new(40.0, [(30_i64, 4.0)]);

    let mut c = cte(cte_class(|_| {}));
    let (stats, _) = c.derive_stats(
        &seed,
        &seed_schema,
        Some((&recur, &recur_schema)),
        &self_schema,
        None,
        &[true],
    );
    assert!((stats.row_count() - 140.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&20] - 14.0).abs() < 1e-9);

    let mut c = cte(cte_class(|class| class.is_distinct = true));
    let (stats, _) = c.derive_stats(
        &seed,
        &seed_schema,
        Some((&recur, &recur_schema)),
        &self_schema,
        Some(120.0),
        &[true],
    );
    assert!((stats.row_count() - 120.0).abs() < 1e-9);
    // NDVs still add, DISTINCT or not.
    assert!((stats.col_ndvs()[&20] - 14.0).abs() < 1e-9);
}

/// Go `LogicalCTE.PreparePossibleProperties` (`logical_cte.go:239`): nil
/// children are IGNORED, and an all-nil child list falls through to the seed.
#[test]
fn cte_tiflash_comes_from_non_nil_children_or_else_the_seed() {
    let with = |has_tiflash| {
        Some(PossiblePropertiesInfo {
            orders: vec![],
            has_tiflash,
        })
    };
    let mut c = cte(cte_class(|_| {}));
    // A nil child is skipped entirely, so a single true child wins.
    let info = c.prepare_possible_properties(&[None, with(true)], false);
    assert!(info.has_tiflash);
    assert!(info.orders.is_empty());
    // Two children conjoin.
    assert!(
        !c.prepare_possible_properties(&[with(true), with(false)], true)
            .has_tiflash
    );
    // All-nil falls through to the seed's answer.
    assert!(
        c.prepare_possible_properties(&[None, None], true)
            .has_tiflash
    );
    // No children at all is the same fall-through, unlike every other operator.
    assert!(c.prepare_possible_properties(&[], true).has_tiflash);
    assert!(!c.prepare_possible_properties(&[], false).has_tiflash);
}

/// Go `LogicalCTE.ExtractCorrelatedCols` (`logical_cte.go:271`) reads the SEED
/// subtree, not this operator's children — via
/// `coreusage.ExtractCorrelatedCols4LogicalPlan`.
#[test]
fn cte_extracts_correlated_cols_from_the_seed_subtree() {
    let seed = LogicalPlan::Selection(LogicalSelection::new(
        {
            let mut base = BaseLogicalPlan::with_id(8, LogicalSelection::TYPE, 0);
            base.set_children(vec![LogicalPlan::Selection(LogicalSelection::new(
                BaseLogicalPlan::with_id(9, LogicalSelection::TYPE, 0),
                vec![eq(cor_expr(2), one())],
            ))]);
            base
        },
        vec![eq(cor_expr(1), one())],
    ));
    // The walk reaches BOTH levels.
    assert_eq!(extract_correlated_cols_for_plan(&seed).len(), 2);

    let c = cte(cte_class(|class| {
        class.seed_part_logical_plan = Some(Box::new(seed));
    }));
    assert_eq!(c.extract_correlated_cols().len(), 2);
    assert_eq!(LogicalPlan::CTE(c).extract_correlated_cols().len(), 2);
}

/// Go `logicalop.GetHasTiFlash` (`logical_plans_misc.go:128`).
#[test]
fn get_has_tiflash_reads_the_bit_prepare_possible_properties_left() {
    assert!(!get_has_tiflash(None));
    let mut plan = LogicalPlan::Selection(LogicalSelection::new(
        BaseLogicalPlan::with_id(1, LogicalSelection::TYPE, 0),
        vec![],
    ));
    assert!(!get_has_tiflash(Some(&plan)));
    plan.base_mut().set_has_tiflash(true);
    assert!(get_has_tiflash(Some(&plan)));
}

/// A shallow clone of a `LogicalCTE` still points at the SAME `CTEClass`, which
/// is Go's pointer copy.
#[test]
fn cte_shallow_clone_shares_the_class() {
    let class = cte_class(|_| {});
    let plan = LogicalPlan::CTE(cte(std::rc::Rc::clone(&class)));
    let LogicalPlan::CTE(cloned) = plan.clone_shallow() else {
        panic!("the variant is preserved");
    };
    class.borrow_mut().push_down_predicates.push(one());
    assert_eq!(cloned.cte.unwrap().borrow().push_down_predicates.len(), 1);
    // Go's PruneColumns is an empty call: it never rewrites the plan.
    assert!(!LogicalCTE::prune_columns_local());
}

// ***** LogicalMaxOneRow / LogicalLock / LogicalSequence / LogicalUnionScan /
// ***** TiKVSingleGather

/// Go `LogicalMaxOneRow.Schema` (`logical_max_one_row.go:41`): every column
/// becomes nullable, because a childless run emits one row of NULLs.
#[test]
fn max_one_row_makes_every_child_column_nullable() {
    let mut not_null = FieldType::new(FieldTypeCode::LongLong);
    not_null.set_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let child = Schema::new(vec![Column::new(1, not_null)]);
    assert!(child.columns[0]
        .ret_type
        .as_ref()
        .unwrap()
        .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL));
    let relaxed = LogicalMaxOneRow::schema(&child);
    assert!(!relaxed.columns[0]
        .ret_type
        .as_ref()
        .unwrap()
        .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL));
}

/// Go `getSingletonStats` (`logical_max_one_row.go:117`) and
/// `LogicalMaxOneRow.DeriveStats` (`:73`): one row, every NDV one, regardless
/// of the child.
#[test]
fn max_one_row_derives_a_singleton_profile() {
    let mut op = LogicalMaxOneRow::new(BaseLogicalPlan::with_id(1, LogicalMaxOneRow::TYPE, 0));
    let output = schema(&[1, 2]);
    let (stats, derived) = op.derive_stats(&output, &[true]);
    assert!(derived);
    assert!((stats.row_count() - 1.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&1] - 1.0).abs() < 1e-9);
    assert!((stats.col_ndvs()[&2] - 1.0).abs() < 1e-9);
    assert!(!op.derive_stats(&output, &[false]).1);
    // A filter never crosses it.
    let predicates = vec![eq(col_expr(1), one())];
    assert_eq!(LogicalMaxOneRow::predicate_push_down(predicates).len(), 1);
}

/// Go `IsSupportedSelectLockType` (`logical_lock.go:151`) and its two halves.
#[test]
fn lock_type_support_covers_for_update_and_for_share_only() {
    for supported in [
        SelectLockType::ForUpdate,
        SelectLockType::ForUpdateNoWait,
        SelectLockType::ForUpdateWaitN,
        SelectLockType::ForShare,
        SelectLockType::ForShareNoWait,
    ] {
        assert!(is_supported_select_lock_type(supported));
    }
    assert!(!is_supported_select_lock_type(SelectLockType::None));
    assert!(is_select_for_update_lock_type(
        SelectLockType::ForUpdateWaitN
    ));
    assert!(!is_select_for_update_lock_type(SelectLockType::ForShare));
    assert!(is_select_for_share_lock_type(
        SelectLockType::ForShareNoWait
    ));
}

/// Go `LogicalLock.PruneColumns` (`logical_lock.go:52`): a supported lock
/// forces its handle and partition-id columns to survive; an unsupported one
/// forces nothing.
#[test]
fn lock_pruning_keeps_handles_only_for_a_supported_lock() {
    let mut op = LogicalLock::new(
        BaseLogicalPlan::with_id(1, LogicalLock::TYPE, 0),
        SelectLockType::ForUpdate,
    );
    op.tbl_id_to_handle_cols
        .insert(7, vec![column(70), column(71)]);
    op.tbl_id_to_phys_tbl_id_col.insert(7, column(79));
    // A table with a handle but no partition column contributes only handles.
    op.tbl_id_to_handle_cols.insert(8, vec![column(80)]);

    let used = op.prune_columns_local(&[column(1)]);
    assert_eq!(
        used.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 70, 71, 79, 80]
    );

    op.lock_type = SelectLockType::None;
    assert_eq!(op.prune_columns_local(&[column(1)]).len(), 1);
    // A TopN always goes through the lock rather than removing it.
    assert!(LogicalLock::pushes_topn_into_child(true));
    assert!(!LogicalLock::pushes_topn_into_child(false));
}

/// Go `LogicalSequence`'s three single-child rules (`logical_sequence.go:47`,
/// `:57`, `:65`): everything addresses the LAST child, the main query.
#[test]
fn sequence_addresses_only_its_last_child() {
    let schemas = [schema(&[1]), schema(&[9])];
    assert_eq!(
        LogicalSequence::schema(&schemas).unwrap().columns[0].unique_id,
        9
    );
    assert!(LogicalSequence::schema(&[]).is_none());
    assert_eq!(LogicalSequence::predicate_push_down_child(4), Some(3));
    assert_eq!(LogicalSequence::prune_columns_child(4), Some(3));
    assert_eq!(LogicalSequence::predicate_push_down_child(0), None);
}

/// Go `LogicalSequence.DeriveStats` (`logical_sequence.go:82`): the LAST
/// child's profile, and "sequence only care about the last child stats is
/// changed or not" — so there is NO memoisation escape.
#[test]
fn sequence_adopts_the_last_child_profile_and_reports_its_reload_flag() {
    let mut op = LogicalSequence::new(BaseLogicalPlan::with_id(1, LogicalSequence::TYPE, 0));
    let children = [
        StatsInfo::new(1.0, [(1_i64, 1.0)]),
        StatsInfo::new(77.0, [(9_i64, 9.0)]),
    ];
    let (stats, reload) = op.derive_stats(&children, &[true, true]).unwrap();
    assert!((stats.row_count() - 77.0).abs() < 1e-9);
    assert!(reload);
    // Only the LAST reload flag counts, and the profile is re-adopted anyway.
    let (stats, reload) = op.derive_stats(&children, &[true, false]).unwrap();
    assert!(!reload);
    assert!((stats.row_count() - 77.0).abs() < 1e-9);
    // No reload flags at all defaults to reloaded.
    assert!(op.derive_stats(&children, &[]).unwrap().1);
    assert!(op.derive_stats(&[], &[true]).is_none());

    let info = op.prepare_possible_properties(&[
        Some(PossiblePropertiesInfo {
            orders: vec![vec![column(1)]],
            has_tiflash: true,
        }),
        Some(PossiblePropertiesInfo {
            orders: vec![],
            has_tiflash: true,
        }),
    ]);
    assert!(info.orders.is_empty());
    assert!(info.has_tiflash);
}

/// Go `LogicalUnionScan.PredicatePushDown` (`logical_union_scan.go:58`): a
/// predicate that reads a VIRTUAL column stays above, per issue #53951.
#[test]
fn union_scan_holds_back_virtual_column_predicates() {
    let mut generated = column(2);
    generated.virtual_expr = Some(Box::new(one()));
    let split = LogicalUnionScan::predicate_push_down(&[
        eq(col_expr(1), one()),
        eq(Expression::Column(generated.clone()), one()),
    ]);
    assert_eq!(split.without_virtual_column.len(), 1);
    assert_eq!(split.with_virtual_column.len(), 1);
    assert!(contains_virtual_column(&eq(
        Expression::Column(generated),
        one()
    )));
    assert!(!contains_virtual_column(&eq(col_expr(1), one())));
}

/// Go `LogicalUnionScan.PruneColumns` (`logical_union_scan.go:88`): the handle,
/// the partition-id column, and every condition column all survive.
#[test]
fn union_scan_pruning_keeps_the_handle_and_the_partition_column() {
    let mut op = LogicalUnionScan::new(
        BaseLogicalPlan::with_id(1, LogicalUnionScan::TYPE, 0),
        vec![column(50)],
    );
    op.conditions = vec![eq(col_expr(60), one())];
    let mut phys = column(70);
    phys.id = EXTRA_PHYS_TBL_ID;
    let output = Schema::new(vec![column(1), phys]);
    let used = op.prune_columns_local(&[column(1)], &output);
    assert_eq!(
        used.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 50, 70, 60]
    );
    assert_eq!(op.explain_info(), "conds:1 exprs, handle:1 cols");
}

/// Go `LogicalUnionScan.PreparePossibleProperties`
/// (`logical_union_scan.go:120`): the child's orders pass through, TiFlash is
/// unconditionally false.
#[test]
fn union_scan_passes_orders_through_but_never_tiflash() {
    let mut op = LogicalUnionScan::new(
        BaseLogicalPlan::with_id(1, LogicalUnionScan::TYPE, 0),
        vec![],
    );
    let info = op.prepare_possible_properties(Some(&PossiblePropertiesInfo {
        orders: vec![vec![column(5)]],
        has_tiflash: true,
    }));
    assert_eq!(info.orders[0][0].unique_id, 5);
    assert!(!info.has_tiflash);
    assert!(!op.base.has_tiflash());
    assert!(op.prepare_possible_properties(None).orders.is_empty());
}

/// Go `TiKVSingleGather.BuildKeyInfo` (`logical_tikv_single_gather.go:60`):
/// the child's keys are adopted WHOLESALE, without the survival check the
/// schema producer applies.
#[test]
fn tikv_single_gather_adopts_child_keys_wholesale() {
    let mut child = schema(&[1, 2]);
    // A key naming a column this operator's schema does NOT list still carries
    // across, unlike `propagate_child_keys`.
    child.pk_or_uk = vec![vec![column(1)], vec![column(999)]];
    let mut output = schema(&[1]);
    TiKVSingleGather::build_key_info(&mut output, std::slice::from_ref(&child));
    assert_eq!(output.pk_or_uk.len(), 2);
    // The schema-producer body would have dropped the second key.
    let mut compare = schema(&[1]);
    schema_producer::propagate_child_keys(&mut compare, &[child]);
    assert_eq!(compare.pk_or_uk.len(), 1);

    // No child leaves no keys rather than panicking.
    let mut output = schema(&[1]);
    output.pk_or_uk = vec![vec![column(1)]];
    TiKVSingleGather::build_key_info(&mut output, &[]);
    assert!(output.pk_or_uk.is_empty());
}

/// Go `TiKVSingleGather.PreparePossibleProperties`
/// (`logical_tikv_single_gather.go:74`) and `ExplainInfo` (`:46`).
#[test]
fn tikv_single_gather_is_transparent_to_order_and_names_its_index() {
    let mut op = TiKVSingleGather::new(BaseLogicalPlan::with_id(1, TiKVSingleGather::TYPE, 0));
    let info = op.prepare_possible_properties(Some(&PossiblePropertiesInfo {
        orders: vec![vec![column(5)]],
        has_tiflash: true,
    }));
    assert_eq!(info.orders[0][0].unique_id, 5);
    assert!(info.has_tiflash);
    assert!(op.base.has_tiflash());
    let info = op.prepare_possible_properties(None);
    assert!(info.orders.is_empty());
    assert!(!info.has_tiflash);

    op.source = Some(Box::new(DataSource {
        table_name: "t".to_owned(),
        ..DataSource::default()
    }));
    let table_only = op.explain_info();
    op.is_index_gather = true;
    op.index_name = Some("idx_a".to_owned());
    assert_eq!(op.explain_info(), format!("{table_only}, index:idx_a"));
}

/// The five small operators all reach the enum dispatches.
#[test]
fn the_small_operators_are_wired_into_the_enum() {
    for plan in [
        LogicalPlan::MaxOneRow(LogicalMaxOneRow::new(BaseLogicalPlan::with_id(
            1,
            LogicalMaxOneRow::TYPE,
            0,
        ))),
        LogicalPlan::Lock(LogicalLock::new(
            BaseLogicalPlan::with_id(2, LogicalLock::TYPE, 0),
            SelectLockType::ForUpdate,
        )),
        LogicalPlan::Sequence(LogicalSequence::new(BaseLogicalPlan::with_id(
            3,
            LogicalSequence::TYPE,
            0,
        ))),
        LogicalPlan::UnionScan(LogicalUnionScan::new(
            BaseLogicalPlan::with_id(4, LogicalUnionScan::TYPE, 0),
            vec![],
        )),
        LogicalPlan::TiKVSingleGather(TiKVSingleGather::new(BaseLogicalPlan::with_id(
            5,
            TiKVSingleGather::TYPE,
            0,
        ))),
    ] {
        assert!(plan.extract_correlated_cols().is_empty());
        assert!(plan.pull_up_constant_predicates().is_empty());
        assert!(plan.extract_col_groups(&[]).is_empty());
        let shallow = plan.clone_shallow();
        assert_eq!(shallow.id(), plan.id());
        assert_eq!(shallow.tp(), plan.tp());
    }
}

// ***** LogicalTableScan / LogicalIndexScan *****

/// Go `LogicalTableScan.PreparePossibleProperties`
/// (`logical_table_scan.go:77`): the HANDLE order, and TiFlash only when the
/// source has it AND MPP is allowed.
#[test]
fn table_scan_offers_the_handle_order_and_gates_tiflash_on_mpp() {
    let mut ts = LogicalTableScan::new(BaseLogicalPlan::with_id(1, LogicalTableScan::TYPE, 0));
    ts.source = Some(Box::new(DataSource::default()));
    ts.handle_cols = vec![column(1)];
    let info = ts.prepare_possible_properties(true, true);
    assert_eq!(info.orders[0][0].unique_id, 1);
    assert!(info.has_tiflash);
    assert!(ts.base.has_tiflash());
    // MPP off, or a source with no TiFlash replica, both veto it.
    assert!(!ts.prepare_possible_properties(true, false).has_tiflash);
    assert!(!ts.prepare_possible_properties(false, true).has_tiflash);
    // No handle: no offered order.
    ts.handle_cols.clear();
    assert!(ts.prepare_possible_properties(true, true).orders.is_empty());
    // No source at all is never TiFlash-capable.
    ts.source = None;
    assert!(!ts.prepare_possible_properties(true, true).has_tiflash);
}

/// Go `LogicalTableScan.BuildKeyInfo` (`logical_table_scan.go:66`) delegates to
/// the source, and `ExplainInfo` (`:46`) never drops a list silently.
#[test]
fn table_scan_delegates_keys_and_reports_every_explain_list() {
    let mut ts = LogicalTableScan::new(BaseLogicalPlan::with_id(1, LogicalTableScan::TYPE, 0));
    ts.source = Some(Box::new(DataSource {
        table_name: "t".to_owned(),
        ..DataSource::default()
    }));
    let mut output = schema(&[1, 2]);
    ts.build_key_info(&mut output, vec![vec![column(1)]]);
    assert_eq!(output.pk_or_uk.len(), 1);

    let table_only = ts.explain_info();
    ts.handle_cols = vec![column(1)];
    ts.access_conds = vec![eq(col_expr(1), one())];
    assert_eq!(
        ts.explain_info(),
        format!("{table_only}, pk col:1 cols, cond:1 exprs")
    );
}

/// Go `LogicalIndexScan.PreparePossibleProperties`
/// (`logical_index_scan.go:117`): `EqCondCount + 1` offered orders, one per
/// equality-pinned prefix.
#[test]
fn index_scan_offers_one_order_per_equality_pinned_prefix() {
    let mut is = LogicalIndexScan::new(BaseLogicalPlan::with_id(1, LogicalIndexScan::TYPE, 0));
    is.source = Some(Box::new(DataSource::default()));
    is.idx_cols = vec![column(1), column(2), column(3)];
    is.eq_cond_count = 1;
    let info = is.prepare_possible_properties(true, true);
    // [a, b, c] and [b, c]: every row already agrees on `a`.
    assert_eq!(info.orders.len(), 2);
    assert_eq!(
        info.orders[0]
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        info.orders[1]
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    // No equality at all still offers the whole index.
    is.eq_cond_count = 0;
    assert_eq!(is.prepare_possible_properties(true, true).orders.len(), 1);
    // No index columns: nothing offered.
    is.idx_cols.clear();
    assert!(is.prepare_possible_properties(true, true).orders.is_empty());
}

/// Go `LogicalIndexScan.GetPKIsHandleCol` (`logical_index_scan.go:196`), which
/// resolves against THIS operator's schema and not the source's columns.
#[test]
fn index_scan_finds_the_handle_in_its_own_schema() {
    let mut is = LogicalIndexScan::new(BaseLogicalPlan::with_id(1, LogicalIndexScan::TYPE, 0));
    is.source = Some(Box::new(DataSource {
        pk_is_handle: true,
        handle_cols: vec![column(1)],
        ..DataSource::default()
    }));
    assert!(is.get_pk_is_handle_col(&schema(&[1, 2])).is_some());
    // The handle was pruned out of this operator's schema.
    assert!(is.get_pk_is_handle_col(&schema(&[2])).is_none());
    // A clustered/non-PKIsHandle table has none.
    is.source.as_mut().unwrap().pk_is_handle = false;
    assert!(is.get_pk_is_handle_col(&schema(&[1, 2])).is_none());

    // BuildKeyInfo appends it above the caller's index keys.
    is.source.as_mut().unwrap().pk_is_handle = true;
    let mut output = schema(&[1, 2]);
    is.build_key_info(&mut output, vec![vec![column(2)]], vec![vec![column(2)]]);
    assert_eq!(output.pk_or_uk.len(), 2);
    assert_eq!(output.nullable_uk.len(), 1);
}

/// Go `matchIndicesProp` (`logical_index_scan.go:204`): a PREFIX index cannot
/// satisfy an order on the whole column.
#[test]
fn index_prop_matching_rejects_prefix_indexes_and_short_indexes() {
    let idx = [column(1), column(2)];
    let full = [
        tidb_datatype::UNSPECIFIED_LENGTH,
        tidb_datatype::UNSPECIFIED_LENGTH,
    ];
    let prop = [
        crate::physical_property::SortItem::new(1, false),
        crate::physical_property::SortItem::new(2, false),
    ];
    assert!(matches_indices_prop(&idx, &full, &prop));
    // A prefix length on the first column breaks it.
    assert!(!matches_indices_prop(&idx, &[10, -1], &prop));
    // A different column order breaks it.
    assert!(!matches_indices_prop(
        &idx,
        &full,
        &[
            crate::physical_property::SortItem::new(2, false),
            crate::physical_property::SortItem::new(1, false),
        ]
    ));
    // Fewer index columns than required attributes.
    assert!(!matches_indices_prop(&idx[..1], &full[..1], &prop));
    // An empty requirement is always satisfied.
    assert!(matches_indices_prop(&idx, &full, &[]));
}

/// Go `LogicalIndexScan.ExplainInfo()` (`logical_index_scan.go:58`): the index
/// column names are exact.
#[test]
fn index_scan_explain_names_its_index_columns() {
    let mut is = LogicalIndexScan::new(BaseLogicalPlan::with_id(1, LogicalIndexScan::TYPE, 0));
    is.source = Some(Box::new(DataSource {
        table_name: "t".to_owned(),
        ..DataSource::default()
    }));
    let table_only = is.explain_info();
    is.index_column_names = vec!["a".to_owned(), "b".to_owned()];
    is.access_conds = vec![eq(col_expr(1), one())];
    assert_eq!(
        is.explain_info(),
        format!("{table_only}, index:a, b, cond:1 exprs")
    );
}

// ***** LogicalExpand *****

fn expand(distinct_group_by_ids: &[i64]) -> LogicalExpand {
    let mut expand = LogicalExpand::new(BaseLogicalPlan::with_id(1, LogicalExpand::TYPE, 0));
    expand.distinct_group_by_col = distinct_group_by_ids.iter().copied().map(column).collect();
    expand
}

/// Go `LogicalExpand.PredicatePushDown` (`logical_expand.go:75`): NOTHING
/// crosses an Expand, because a grouping column's nullability changes here.
#[test]
fn expand_pushes_no_predicate_through() {
    let expand = expand(&[1]);
    let remained = expand.predicate_push_down(vec![eq(col_expr(1), one())]);
    assert_eq!(remained.len(), 1);
}

/// Go `LogicalExpand.PruneColumns` (`logical_expand.go:95`): the distinct
/// group-by columns are re-appended for the child, and the operator's own
/// schema loses whatever the widened set does not name.
#[test]
fn expand_pruning_re_adds_the_group_by_columns_and_narrows_its_schema() {
    let expand = expand(&[2]);
    let widened = expand.prune_columns_local(&[column(1)]);
    assert_eq!(
        widened.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 2]
    );

    let mut own = schema(&[1, 2, 3]);
    // Go walks BACKWARDS, so the removed positions come out descending.
    assert_eq!(LogicalExpand::prune_schema(&mut own, &widened), vec![2]);
    assert_eq!(
        own.columns.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![1, 2]
    );
    // Go's `GetUsedCols` answers nothing on purpose.
    assert!(LogicalExpand::get_used_cols().is_empty());
}

/// Go `LogicalExpand.BuildKeyInfo` (`logical_expand.go:389`): row replication
/// destroys every key, so the enum's dispatch must NOT propagate the child's.
#[test]
fn expand_build_key_info_drops_every_key() {
    let mut child = schema(&[1, 2]);
    child.set_keys(vec![vec![column(1)]]);
    let mut self_schema = schema(&[1, 2]);
    self_schema.set_keys(vec![vec![column(1)]]);
    self_schema.set_unique_keys(vec![vec![column(2)]]);
    let mut plan = LogicalPlan::Expand(expand(&[1]));
    plan.set_children(vec![LogicalPlan::TableDual(LogicalTableDual::new(
        BaseLogicalPlan::with_id(2, LogicalTableDual::TYPE, 0),
        1,
    ))]);
    plan.build_key_info(&mut self_schema, std::slice::from_ref(&child));
    assert!(self_schema.pk_or_uk.is_empty());
    assert!(self_schema.nullable_uk.is_empty());
}

/// Go `LogicalExpand.GenerateGroupingIDModeBitAnd` (`logical_expand.go:349`)
/// and `GenerateGroupingIDIncrementModeNumericSet` (`logical_expand.go:375`).
#[test]
fn expand_grouping_ids_are_a_bitmask_or_a_stored_index() {
    // Distinct GBY (a, b, c) = (1, 2, 3); the set {a, c} is 0b101.
    let mut expand = expand(&[1, 2, 3]);
    assert_eq!(
        expand.generate_grouping_id_mode_bit_and(&RollupGroupingSet::new([1, 3])),
        0b101
    );
    assert_eq!(
        expand.generate_grouping_id_mode_bit_and(&RollupGroupingSet::new([])),
        0
    );
    // A duplicate set names the same columns and so shares the id.
    assert_eq!(
        expand.generate_grouping_id_mode_bit_and(&RollupGroupingSet::new([1, 1, 3])),
        0b101
    );

    expand.rollup_grouping_ids = vec![7, 9];
    assert_eq!(
        expand.generate_grouping_id_increment_mode_numeric_set(1),
        Some(9)
    );
    // Go would index past the end and panic.
    assert_eq!(
        expand.generate_grouping_id_increment_mode_numeric_set(2),
        None
    );
}

/// Go `LogicalExpand.GenerateGroupingMarks` (`logical_expand.go:246`): one
/// single-column mark per argument in `ModeBitAnd`, the stored id set in
/// `ModeNumericSet`.
#[test]
fn expand_grouping_marks_are_per_argument() {
    let mut expand = expand(&[1, 2, 3]);
    let marks = expand.generate_grouping_marks(&[column(1), column(3), column(99)]);
    assert_eq!(marks[0], std::collections::BTreeSet::from([0b001]));
    assert_eq!(marks[1], std::collections::BTreeSet::from([0b100]));
    // A column that is not a group-by column marks as 0, as Go's zero value.
    assert_eq!(marks[2], std::collections::BTreeSet::from([0]));

    expand.grouping_mode = Some(GroupingMode::NumericSet);
    expand.rollup_id_to_gids = std::collections::BTreeMap::from([(
        1_i64,
        std::collections::BTreeSet::from([0_u64, 1, 2]),
    )]);
    let marks = expand.generate_grouping_marks(&[column(1), column(2)]);
    assert_eq!(marks[0], std::collections::BTreeSet::from([0, 1, 2]));
    assert!(marks[1].is_empty());
}

/// Go `LogicalExpand.GenLevelProjections` (`logical_expand.go:191`): a column
/// the current set does not group by is projected as a typed NULL, and the
/// trailing generated columns become the gid — plus a gpos when two grouping
/// sets are duplicates.
#[test]
fn expand_level_projections_null_out_ungrouped_columns() {
    let mut expand = expand(&[1, 2]);
    expand.rollup_grouping_sets = vec![RollupGroupingSet::new([1, 2]), RollupGroupingSet::new([1])];
    expand.distinct_size = 2;
    // Schema: the two group-by columns, an unrelated column, then gid.
    let gid_schema = schema(&[1, 2, 5, 100]);
    expand.gen_level_projections(&gid_schema);
    let levels = expand.level_exprs.clone().expect("levels generated");
    assert_eq!(levels.len(), 2);
    // Set {a, b}: both columns are references, gid 0b11.
    assert!(matches!(levels[0][0], Expression::Column(_)));
    assert!(matches!(levels[0][1], Expression::Column(_)));
    // The unrelated column is never nulled.
    assert!(matches!(levels[0][2], Expression::Column(_)));
    // Set {a}: b becomes NULL, gid 0b01.
    assert!(matches!(levels[1][1], Expression::Constant(_)));
    let gids: Vec<_> = levels
        .iter()
        .map(|level| match level.last() {
            Some(Expression::Constant(c)) => c.value.clone(),
            _ => panic!("gid is a constant"),
        })
        .collect();
    assert_eq!(gids, vec![Datum::UInt(0b11), Datum::UInt(0b01)]);

    // A duplicate grouping set adds gpos, so the LAST two columns are generated
    // and the gpos value is the set's own offset.
    let mut dup = expand;
    dup.level_exprs = None;
    dup.distinct_size = 1;
    let gpos_schema = schema(&[1, 2, 5, 100, 101]);
    dup.gen_level_projections(&gpos_schema);
    let levels = dup.level_exprs.expect("levels generated");
    assert_eq!(levels[1].len(), 5);
    match levels[1].last() {
        Some(Expression::Constant(c)) => assert_eq!(c.value, Datum::UInt(1)),
        _ => panic!("gpos is a constant"),
    }
}

/// Go `LogicalExpand.ExtractCorrelatedCols` (`logical_expand.go:138`): a NIL
/// `LevelExprs` means the projections have not been generated and answers
/// nothing, which is NOT the same as a generated but empty one.
#[test]
fn expand_extracts_correlated_cols_only_once_levels_exist() {
    let mut expand = expand(&[1]);
    assert!(expand.extract_correlated_cols().is_empty());
    expand.level_exprs = Some(vec![vec![cor_expr(4)]]);
    assert_eq!(expand.extract_correlated_cols().len(), 1);
    assert_eq!(
        LogicalPlan::Expand(expand).extract_correlated_cols().len(),
        1
    );
}

/// Go `LogicalExpand.TrySubstituteExprWithGroupingSetCol`
/// (`logical_expand.go:290`) and `ResolveGroupingFuncArgsInGroupBy`
/// (`logical_expand.go:304`).
#[test]
fn expand_resolves_grouping_arguments_to_grouping_set_columns() {
    let mut expand = expand(&[11, 12]);
    // The ORIGINAL group-by expressions, in the same order as the columns.
    expand.distinct_gby_exprs = vec![eq(col_expr(1), one()), col_expr(2)];

    let (substituted, found) = expand.try_substitute_expr_with_grouping_set_col(&col_expr(2));
    assert!(found);
    assert!(matches!(substituted, Expression::Column(c) if c.unique_id == 12));
    let (unchanged, found) = expand.try_substitute_expr_with_grouping_set_col(&col_expr(9));
    assert!(!found);
    assert!(matches!(unchanged, Expression::Column(c) if c.unique_id == 9));

    // An argument already rewritten to the grouping-set column resolves too.
    let resolved = expand
        .resolve_grouping_func_args_in_group_by(&[col_expr(2), col_expr(11)])
        .expect("both arguments are group-by items");
    assert_eq!(
        resolved.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
        vec![12, 11]
    );
    // Anything else is Go's ErrFieldInGroupingNotGroupBy.
    assert!(expand
        .resolve_grouping_func_args_in_group_by(&[col_expr(77)])
        .is_err());
}

/// Go `LogicalExpand.Hash64`/`Equals`, which the crate's former
/// `logical_expand::LogicalExpandIdentity` modelled and this operator now
/// carries: the schema, the two distinct lists, the size, the sets, the level
/// projections' NIL state, and the two generated columns.
#[test]
fn expand_identity_covers_every_generated_field() {
    let first = expand(&[1]);
    let second = expand(&[1]);
    assert_eq!(first.hash64(None), second.hash64(None));
    assert!(first.equals(None, &second, None));

    let mut differs = expand(&[2]);
    assert_ne!(first.hash64(None), differs.hash64(None));
    assert!(!first.equals(None, &differs, None));

    // The schema is part of the identity.
    let own = schema(&[1]);
    assert_ne!(first.hash64(None), first.hash64(Some(&own)));
    assert!(!first.equals(None, &second, Some(&own)));

    // A nil `LevelExprs` and a generated-but-empty one are different.
    differs = expand(&[1]);
    differs.level_exprs = Some(Vec::new());
    assert_ne!(first.hash64(None), differs.hash64(None));
    assert!(!first.equals(None, &differs, None));

    // So are the grouping sets, the distinct size, and the generated columns.
    let mut sets = expand(&[1]);
    sets.rollup_grouping_sets = vec![RollupGroupingSet::new([1])];
    assert_ne!(first.hash64(None), sets.hash64(None));
    let mut size = expand(&[1]);
    size.distinct_size = 3;
    assert_ne!(first.hash64(None), size.hash64(None));
    let mut gid = expand(&[1]);
    gid.gid = Some(Box::new(column(100)));
    assert_ne!(first.hash64(None), gid.hash64(None));
    assert!(!first.equals(None, &gid, None));
    let mut gpos = expand(&[1]);
    gpos.gpos = Some(Box::new(column(101)));
    assert_ne!(first.hash64(None), gpos.hash64(None));
}

// ***** LogicalTableDual *****

/// Go `LogicalTableDual.ExplainInfo` (`logical_table_dual.go:49`) and
/// `HashCode` (`logical_table_dual.go:61`), which deliberately omits the plan
/// id so two duals of the same shape hash alike.
#[test]
fn table_dual_explains_and_hashes_its_row_count() {
    let one_row = LogicalTableDual::new(BaseLogicalPlan::with_id(1, LogicalTableDual::TYPE, 0), 1);
    let other = LogicalTableDual::new(BaseLogicalPlan::with_id(7, LogicalTableDual::TYPE, 0), 1);
    let empty = LogicalTableDual::new(BaseLogicalPlan::with_id(1, LogicalTableDual::TYPE, 0), 0);
    assert_eq!(one_row.explain_info(), "rowcount:1");
    assert_eq!(empty.explain_info(), "rowcount:0");
    assert_eq!(one_row.hash_code(3), other.hash_code(3));
    assert_ne!(one_row.hash_code(3), empty.hash_code(3));
    assert_eq!(one_row.hash_code(3).len(), 12);
    assert_eq!(
        LogicalPlan::TableDual(one_row.clone()).explain_info(),
        "rowcount:1"
    );
}

/// Go `LogicalTableDual.BuildKeyInfo` (`logical_table_dual.go:89`): a one-row
/// dual is `maxOneRow`, and an empty one is not.
#[test]
fn table_dual_marks_max_one_row_only_for_one_row() {
    for (row_count, expected) in [(0, false), (1, true)] {
        let mut plan = LogicalPlan::TableDual(LogicalTableDual::new(
            BaseLogicalPlan::with_id(1, LogicalTableDual::TYPE, 0),
            row_count,
        ));
        let mut self_schema = schema(&[1]);
        plan.build_key_info(&mut self_schema, &[]);
        assert_eq!(plan.max_one_row(), expected);
    }
}

/// Go `LogicalTableDual.DeriveStats` (`logical_table_dual.go:109`) and
/// `PruneColumns` (`logical_table_dual.go:76`).
#[test]
fn table_dual_stats_are_its_row_count_and_pruning_keeps_used_columns() {
    let mut dual = LogicalTableDual::new(BaseLogicalPlan::with_id(1, LogicalTableDual::TYPE, 0), 1);
    let self_schema = schema(&[1, 2]);
    let (stats, reloaded) = dual.derive_stats(&self_schema, &[true]);
    assert!(reloaded);
    assert!((stats.row_count() - 1.0).abs() < f64::EPSILON);
    assert_eq!(stats.col_ndvs().get(&2).copied(), Some(1.0));
    // Without a reload the stored profile is returned unchanged.
    let (again, reloaded) = dual.derive_stats(&self_schema, &[false]);
    assert!(!reloaded);
    assert!((again.row_count() - 1.0).abs() < f64::EPSILON);

    let mut dual_schema = schema(&[1, 2, 3]);
    assert_eq!(
        LogicalTableDual::prune_columns(&mut dual_schema, &[column(2)]),
        vec![2, 0]
    );
    assert_eq!(
        dual_schema
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![2]
    );
    // Go's dual has no child, so every predicate stays above it.
    assert_eq!(
        LogicalTableDual::predicate_push_down(vec![eq(col_expr(1), one())]).len(),
        1
    );
}

// ***** LogicalMemTable *****

fn mem_table(table_name: &str) -> LogicalMemTable {
    LogicalMemTable::new(
        BaseLogicalPlan::with_id(1, LogicalMemTable::TYPE, 0),
        "information_schema",
        table_name,
    )
}

/// Go `LogicalMemTable.PruneColumns` (`logical_mem_table.go:80`): the table
/// switch is an ALLOW-list, and the last column is never pruned away.
#[test]
fn mem_table_prunes_only_the_listed_tables_and_keeps_one_column() {
    let mut other = mem_table("COLUMNS");
    assert!(!other.is_prunable());
    let mut mem_schema = schema(&[1, 2, 3]);
    assert!(other
        .prune_columns(&mut mem_schema, &[column(1)])
        .is_empty());
    assert_eq!(mem_schema.len(), 3);

    let mut slow = mem_table(TABLE_SLOW_QUERY);
    slow.columns = vec![
        MemTableColumn {
            id: 1,
            name: "Time".to_owned(),
        },
        MemTableColumn {
            id: 2,
            name: "Query".to_owned(),
        },
        MemTableColumn {
            id: 3,
            name: "Digest".to_owned(),
        },
    ];
    assert!(slow.is_prunable());
    assert_eq!(
        slow.prune_columns(&mut mem_schema, &[column(2)]),
        vec![2, 0]
    );
    assert_eq!(
        mem_schema
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(slow.columns.len(), 1);
    assert_eq!(slow.columns[0].name, "Query");
    // Nothing used at all still leaves the single remaining column.
    assert!(slow.prune_columns(&mut mem_schema, &[]).is_empty());
    assert_eq!(mem_schema.len(), 1);
}

/// Go `LogicalMemTable.PushDownTopN` (`logical_mem_table.go:114`),
/// `pushDownRowLimit` (`logical_mem_table.go:145`) and `isSlowLogTopNByTime`
/// (`logical_mem_table.go:153`).
#[test]
fn mem_table_pushes_hints_only_for_a_limit_or_a_slow_log_time_order() {
    let mut slow = mem_table(TABLE_SLOW_QUERY);
    // The table's own `Time` column, matched BY ID.
    slow.table_columns = vec![MemTableColumn {
        id: 42,
        name: SLOW_LOG_TIME_STR.to_owned(),
    }];

    let mut limit = LogicalTopN::new(
        BaseLogicalPlan::with_id(2, LogicalTopN::TYPE, 0),
        Vec::new(),
        5,
        10,
    );
    assert_eq!(
        slow.push_down_topn(&limit),
        MemTableTopNHints {
            row_limit_hint: Some(15),
            desc: None
        }
    );
    // Go detects the wrap explicitly and asks for everything.
    limit.offset = u64::MAX;
    assert_eq!(LogicalMemTable::push_down_row_limit(&limit), u64::MAX);
    // A partitioned TopN pushes nothing at all.
    limit.offset = 5;
    limit.partition_by = vec![crate::physical_property::SortItem::new(1, false)];
    assert_eq!(slow.push_down_topn(&limit), MemTableTopNHints::default());

    let mut time_col = column(1);
    time_col.id = 42;
    let by_time = LogicalTopN::new(
        BaseLogicalPlan::with_id(3, LogicalTopN::TYPE, 0),
        vec![ByItems::new(Expression::Column(time_col.clone()), true)],
        0,
        10,
    );
    assert!(slow.is_slow_log_topn_by_time(&by_time));
    assert_eq!(
        slow.push_down_topn(&by_time),
        MemTableTopNHints {
            row_limit_hint: Some(10),
            desc: Some(true)
        }
    );
    // A different column id is not the ordering key, even under the same name.
    let other_col = column(1);
    let by_other = LogicalTopN::new(
        BaseLogicalPlan::with_id(4, LogicalTopN::TYPE, 0),
        vec![ByItems::new(Expression::Column(other_col), false)],
        0,
        10,
    );
    assert!(!slow.is_slow_log_topn_by_time(&by_other));
    assert_eq!(slow.push_down_topn(&by_other), MemTableTopNHints::default());
    // Nor is the same order on a non-slow-log table.
    let mut summary = mem_table("STATEMENTS_SUMMARY");
    summary.table_columns = slow.table_columns.clone();
    assert!(!summary.is_slow_log_topn_by_time(&by_time));
    // The cluster-wide slow log is the OTHER table this applies to.
    let mut cluster = mem_table(CLUSTER_TABLE_SLOW_LOG);
    cluster.table_columns = slow.table_columns.clone();
    assert!(cluster.is_slow_log_topn_by_time(&by_time));
}

/// Go `LogicalMemTable.DeriveStats` (`logical_mem_table.go:181`): the pseudo
/// table's realtime count, for the row count AND every column's NDV.
#[test]
fn mem_table_stats_follow_the_pseudo_realtime_count() {
    let mut table = mem_table(TABLE_SLOW_QUERY);
    let self_schema = schema(&[1, 2]);
    let (stats, reloaded) = table.derive_stats(&self_schema, &[true], 10_000.0);
    assert!(reloaded);
    assert!((stats.row_count() - 10_000.0).abs() < f64::EPSILON);
    assert_eq!(stats.col_ndvs().get(&1).copied(), Some(10_000.0));
    let (_, reloaded) = table.derive_stats(&self_schema, &[false], 1.0);
    assert!(!reloaded);
}

// ***** LogicalShow and LogicalShowDDLJobs *****

fn field_name(column_name: &str) -> tidb_datatype::FieldName {
    tidb_datatype::FieldName::new(tidb_datatype::FieldNameMetadata {
        column: tidb_datatype::IdentifierMetadata::new(column_name),
        ..tidb_datatype::FieldNameMetadata::default()
    })
}

fn string_const(value: &str) -> Expression {
    Expression::Constant(Constant::new(
        Datum::String(tidb_datatype::StringDatum::new(
            value.as_bytes().to_vec(),
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
        )),
        FieldType::new(FieldTypeCode::VarString),
    ))
}

fn show(is_stats_meta: bool) -> LogicalShow {
    LogicalShow::new(
        BaseLogicalPlan::with_id(1, LogicalShow::TYPE, 0),
        ShowContents {
            is_stats_meta,
            ..ShowContents::default()
        },
    )
}

/// Go `findShowColumnIDs` (`logical_show.go:266`) and
/// `extractStatsMetaFilterValues` (`logical_show.go:279`).
#[test]
fn show_extracts_eq_in_and_or_filters_on_a_named_column() {
    let show_schema = schema(&[1, 2]);
    let names = [field_name("db_name"), field_name("table_name")];
    let ids = find_show_column_ids(&show_schema, &names, "db_name");
    assert_eq!(ids.len(), 1);
    assert!(ids.contains(&1));
    assert!(find_show_column_ids(&show_schema, &names, "missing").is_empty());

    // `db_name = 'X'`, either way round.
    let values = extract_stats_meta_filter_values(&eq(col_expr(1), string_const("X")), &ids);
    assert_eq!(values, Some(vec!["X".to_owned()]));
    assert_eq!(
        extract_stats_meta_filter_values(&eq(string_const("X"), col_expr(1)), &ids),
        Some(vec!["X".to_owned()])
    );
    // A predicate on some other column is not extractable.
    assert_eq!(
        extract_stats_meta_filter_values(&eq(col_expr(2), string_const("X")), &ids),
        None
    );
    // `db_name IN ('a', 'b')`, with the column FIRST.
    let in_expr = Expression::ScalarFunction(call(
        "in",
        vec![col_expr(1), string_const("a"), string_const("b")],
    ));
    assert_eq!(
        extract_stats_meta_filter_values(&in_expr, &ids),
        Some(vec!["a".to_owned(), "b".to_owned()])
    );
    // One unextractable OR branch makes the whole disjunction unextractable.
    let good_or = Expression::ScalarFunction(call(
        "or",
        vec![
            eq(col_expr(1), string_const("a")),
            eq(col_expr(1), string_const("b")),
        ],
    ));
    assert_eq!(
        extract_stats_meta_filter_values(&good_or, &ids),
        Some(vec!["a".to_owned(), "b".to_owned()])
    );
    let bad_or = Expression::ScalarFunction(call(
        "or",
        vec![eq(col_expr(1), string_const("a")), eq(col_expr(2), one())],
    ));
    assert_eq!(extract_stats_meta_filter_values(&bad_or, &ids), None);
    // A constant with no usable value refuses.
    assert_eq!(get_string_value_from_constant(&col_expr(1)), None);
}

/// Go `extractStatsMetaFilters` (`logical_show.go:210`): the INTERSECTION of
/// every claimed predicate, lower-cased for `db_name`, and the two refusals
/// that keep the original predicates.
#[test]
fn show_intersects_filters_and_keeps_contradictions_as_predicates() {
    let show_schema = schema(&[1, 2]);
    let names = [field_name("db_name"), field_name("table_name")];
    let predicates = vec![
        eq(col_expr(1), string_const("MyDB")),
        eq(col_expr(2), string_const("t")),
    ];
    let (remained, filters) =
        extract_stats_meta_filters(&show_schema, &names, predicates.clone(), "db_name", true);
    assert_eq!(remained.len(), 1);
    // `toLower` applies to db names only.
    assert_eq!(
        filters,
        Some(std::collections::BTreeSet::from(["mydb".to_owned()]))
    );
    let (remained, filters) =
        extract_stats_meta_filters(&show_schema, &names, remained, "table_name", false);
    assert!(remained.is_empty());
    assert_eq!(
        filters,
        Some(std::collections::BTreeSet::from(["t".to_owned()]))
    );

    // Two contradictory filters intersect to nothing, which Go keeps as
    // ordinary predicates rather than handing over an empty filter set.
    let contradiction = vec![
        eq(col_expr(1), string_const("a")),
        eq(col_expr(1), string_const("b")),
    ];
    let (remained, filters) =
        extract_stats_meta_filters(&show_schema, &names, contradiction, "db_name", true);
    assert_eq!(remained.len(), 2);
    assert_eq!(filters, None);

    // A column the schema does not name is not filterable at all.
    let (remained, filters) =
        extract_stats_meta_filters(&show_schema, &names, predicates, "no_such_col", false);
    assert_eq!(remained.len(), 2);
    assert_eq!(filters, None);
}

/// Go `LogicalShow.PredicatePushDown` (`logical_show.go:130`): only
/// `ast.ShowStatsMeta` extracts, and the extractor is installed only when
/// something was actually claimed.
#[test]
fn show_installs_an_extractor_only_when_it_claimed_a_predicate() {
    let show_schema = schema(&[1, 2]);
    let names = [field_name("db_name"), field_name("table_name")];

    let mut not_stats_meta = show(false);
    let remained = not_stats_meta.predicate_push_down(
        &show_schema,
        &names,
        vec![eq(col_expr(1), string_const("d"))],
    );
    assert_eq!(remained.len(), 1);
    assert!(not_stats_meta.extractor.is_none());

    let mut claimed = show(true);
    let remained = claimed.predicate_push_down(
        &show_schema,
        &names,
        vec![
            eq(col_expr(1), string_const("D")),
            eq(col_expr(2), string_const("t")),
        ],
    );
    assert!(remained.is_empty());
    let extractor = claimed.extractor.clone().expect("extractor installed");
    assert!(extractor.db.contains("d"));
    assert!(extractor.table.contains("t"));

    let mut unclaimed = show(true);
    let remained =
        // Column-to-column: nothing constant to claim.
        unclaimed.predicate_push_down(&show_schema, &names, vec![eq(col_expr(1), col_expr(2))]);
    assert_eq!(remained.len(), 1);
    assert!(unclaimed.extractor.is_none());
}

/// Go `getFakeStats` (`logical_show.go:199`), shared by `LogicalShow`
/// (`logical_show.go:163`) and `LogicalShowDDLJobs`
/// (`logical_show_ddl_jobs.go:60`).
#[test]
fn both_show_operators_derive_the_same_fake_stats() {
    let self_schema = schema(&[1, 2]);
    let mut show = show(false);
    let (stats, reloaded) = show.derive_stats(&self_schema, &[true]);
    assert!(reloaded);
    assert!((stats.row_count() - 1.0).abs() < f64::EPSILON);
    assert_eq!(stats.col_ndvs().get(&2).copied(), Some(1.0));

    let mut jobs =
        LogicalShowDDLJobs::new(BaseLogicalPlan::with_id(1, LogicalShowDDLJobs::TYPE, 0), 10);
    let (job_stats, reloaded) = jobs.derive_stats(&self_schema, &[true]);
    assert!(reloaded);
    assert_eq!(job_stats.col_ndvs(), stats.col_ndvs());
    let (_, reloaded) = jobs.derive_stats(&self_schema, &[false]);
    assert!(!reloaded);
}

/// The four operators this batch adds are reachable through every enum
/// dispatch, and none of them claims an explain body Go does not have.
#[test]
fn the_last_operators_are_wired_into_the_enum() {
    let plans = [
        LogicalPlan::Expand(expand(&[1])),
        LogicalPlan::MemTable(mem_table(TABLE_SLOW_QUERY)),
        LogicalPlan::Show(show(false)),
        LogicalPlan::ShowDDLJobs(LogicalShowDDLJobs::new(
            BaseLogicalPlan::with_id(1, LogicalShowDDLJobs::TYPE, 0),
            3,
        )),
    ];
    for plan in &plans {
        assert_eq!(plan.id(), 1);
        assert!(plan.base().children().is_empty());
        assert!(plan.extract_col_groups(&[]).is_empty());
        assert!(plan.pull_up_constant_predicates().is_empty());
        assert_eq!(plan.explain_info(), "");
        let copy = plan.clone_shallow();
        assert_eq!(copy.tp(), plan.tp());
        assert!(find_child_full_schema(plan).is_none());
    }
    // Expand is the only one of the four with correlated columns of its own.
    assert!(plans
        .iter()
        .all(|plan| plan.extract_correlated_cols().is_empty()));
}
