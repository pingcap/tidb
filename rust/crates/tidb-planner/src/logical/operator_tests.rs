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

use tidb_datatype::{FieldType, FieldTypeCode};
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
use super::data_source::{DataSource, DataSourceColumn};
use super::join::{is_eq_cond_from_in, LogicalJoin, OnConditionSplit};
use super::projection::LogicalProjection;
use super::schema_producer;
use super::selection::{is_valid_compare_constant_predicate, LogicalSelection, SELECTION_FACTOR};
use super::{BaseLogicalPlan, LogicalPlan};
use crate::find_best_task::LogicalJoinType;
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
