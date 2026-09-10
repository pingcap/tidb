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

//! The live executor seam to the ported Go logical planner.
//!
//! The executor still owns physical operator construction, but it must not
//! independently rediscover logical properties from the SQL AST. This module
//! builds and optimizes one logical tree, runs Go's possible-property pass,
//! and translates the aggregation property back to stable relation-qualified
//! column identities understood by the executor's physical source builder.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::rc::Rc;

use tidb_expr::expr_util::normal_form::extract_filters_from_dnfs;
use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::ZonedNoResolver;
use tidb_expr::simple_expr::compose_dnf_condition;
use tidb_planner::cardinality::row_size::{RowSizeColumnStats, RowSizeType};
use tidb_planner::expression_rewriter::ColumnIdAllocator;
use tidb_planner::find_best_task::coster::Ver2Coster;
use tidb_planner::find_best_task::dispatch::{find_best_task, DispatchContext};
use tidb_planner::logical::cte::CteClass;
use tidb_planner::logical::fold::{fold_owned, Descend, OwnedRewrite};
use tidb_planner::logical::rule::{flags, logical_optimize, DisabledLogicalRules, RuleContext};
use tidb_planner::logical::{
    prepare_possible_properties, BaseLogicalPlan, LogicalPlan, LogicalSelection,
};
use tidb_planner::physical::PhysicalPlan;
use tidb_planner::physical_property::PhysicalProperty;
use tidb_planner::plan_base::PlanIdAllocator;
use tidb_planner::plan_builder::PlanBuilder;
use tidb_planner::stats_info::{HistColl, StatsInfo};

use super::catalog::{Catalog, TableEntry};
use super::from::FromScope;
use super::FromTable;

enum ListColumnsLocated {
    Full,
    Location(crate::partition_pruning::ListPartitionLocation),
}

struct PartialIndexChecker<'a> {
    resolver: &'a ZonedNoResolver,
    use_plan_cache: bool,
    opt_prefix_index_single_scan: bool,
}

impl OwnedRewrite for PartialIndexChecker<'_> {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): ()) -> Descend<(), ()> {
        if let LogicalPlan::DataSource(source) = node {
            source.check_partial_indexes(
                self.resolver,
                self.use_plan_cache,
                self.opt_prefix_index_single_scan,
            );
        }
        Descend::Children(vec![(); node.children().len()])
    }

    fn ascend(&mut self, node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
        (node, ())
    }
}

fn check_partial_index_paths(
    plan: LogicalPlan,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> LogicalPlan {
    let resolver =
        ZonedNoResolver::with_like_default_escape(ctx.session_zone(), ctx.like_default_escape());
    fold_owned(
        &mut PartialIndexChecker {
            resolver: &resolver,
            use_plan_cache,
            opt_prefix_index_single_scan: ctx.opt_prefix_index_single_scan(),
        },
        plan,
        (),
    )
    .0
}

fn remap_list_columns_location(
    partition: &crate::partition_routing::PartitionSpec,
    location: crate::partition_pruning::ListPartitionLocation,
) -> crate::partition_pruning::ListPartitionLocation {
    let mut remapped = crate::partition_pruning::ListPartitionLocation::new();
    for (index, mut groups) in location {
        let Some(replacement) = partition.overlapping_dropping_partition_index(index) else {
            continue;
        };
        if replacement != index {
            groups.clear();
            groups.insert(-1);
        }
        remapped.entry(replacement).or_default().extend(groups);
    }
    remapped
}

fn remap_partition_indices(
    partition: &crate::partition_routing::PartitionSpec,
    partition_names: &[String],
    indices: impl IntoIterator<Item = usize>,
) -> Vec<usize> {
    let mut used_ids = std::collections::BTreeSet::new();
    indices
        .into_iter()
        .filter_map(|index| partition.overlapping_dropping_partition_index(index))
        .filter(|index| {
            partition_names.is_empty()
                || partition_names.iter().any(|name| {
                    tidb_ast::CiString::new(name)
                        == tidb_ast::CiString::new(&partition.definitions[*index].name)
                })
        })
        .filter(|index| used_ids.insert(partition.definitions[*index].id))
        .collect()
}

fn locate_list_columns_condition(
    partition: &crate::partition_routing::PartitionSpec,
    condition: &Expression,
    columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<ListColumnsLocated, tidb_planner::plan_base::PlanError> {
    let crate::partition_routing::PartitionKind::ListColumns {
        values,
        default_partition,
        field_types,
        ..
    } = &partition.kind
    else {
        return Ok(ListColumnsLocated::Full);
    };

    match condition {
        Expression::Constant(constant) => match tidb_expr::truthy_of(&constant.value) {
            Ok(Some(false) | None) => Ok(ListColumnsLocated::Location(Default::default())),
            Ok(Some(true)) | Err(_) => Ok(ListColumnsLocated::Full),
        },
        Expression::ScalarFunction(function) => match function.func_name.lowercase() {
            "and" => locate_list_columns_cnf(partition, function.get_args(), columns, context),
            "or" => locate_list_columns_dnf(partition, function.get_args(), columns, context),
            _ => {
                let referenced = tidb_expr::simple_expr::extract_columns(condition);
                if referenced.len() != 1 {
                    return Ok(ListColumnsLocated::Full);
                }
                let Some(column_index) = columns
                    .iter()
                    .position(|column| column.id == referenced[0].id)
                else {
                    return Ok(ListColumnsLocated::Full);
                };
                let detached =
                    tidb_planner::ranger::detacher::detach_partition_range_with_fallback_handler(
                        std::slice::from_ref(condition),
                        std::slice::from_ref(&columns[column_index]),
                        &[tidb_datatype::UNSPECIFIED_LENGTH],
                        context.range_max_size(),
                        context.range_fallback_handler(),
                    )
                    .map_err(|error| {
                        tidb_planner::plan_base::PlanError::internal(format!(
                            "LIST COLUMNS range detachment failed: {error:?}"
                        ))
                    })?;
                let ranges = detached
                    .ranges
                    .iter()
                    .map(|range| crate::IndexRange {
                        low: range.low_val.clone(),
                        high: range.high_val.clone(),
                        low_exclusive: range.low_exclude,
                        high_exclusive: range.high_exclude,
                    })
                    .collect::<Vec<_>>();
                let location = crate::partition_pruning::list_column_location_for_ranges(
                    &ranges,
                    values,
                    *default_partition,
                    field_types,
                    column_index,
                )
                .map_err(|error| {
                    tidb_planner::plan_base::PlanError::internal(format!(
                        "LIST COLUMNS location failed: {error:?}"
                    ))
                })?;
                Ok(location.map_or(ListColumnsLocated::Full, |location| {
                    ListColumnsLocated::Location(remap_list_columns_location(partition, location))
                }))
            }
        },
        Expression::Column(_) | Expression::CorrelatedColumn(_) => Ok(ListColumnsLocated::Full),
    }
}

fn locate_list_columns_cnf(
    partition: &crate::partition_routing::PartitionSpec,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<ListColumnsLocated, tidb_planner::plan_base::PlanError> {
    let mut location = None;
    for condition in conditions {
        match locate_list_columns_condition(partition, condition, columns, context)? {
            ListColumnsLocated::Full => {}
            ListColumnsLocated::Location(found) => {
                if let Some(current) = &mut location {
                    crate::partition_pruning::intersect_list_partition_location(current, &found);
                } else {
                    location = Some(found);
                }
            }
        }
    }
    Ok(location.map_or(ListColumnsLocated::Full, ListColumnsLocated::Location))
}

fn locate_list_columns_dnf(
    partition: &crate::partition_routing::PartitionSpec,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<ListColumnsLocated, tidb_planner::plan_base::PlanError> {
    if conditions.is_empty() {
        return Ok(ListColumnsLocated::Full);
    }
    let mut location = crate::partition_pruning::ListPartitionLocation::new();
    for condition in conditions {
        match locate_list_columns_condition(partition, condition, columns, context)? {
            ListColumnsLocated::Full => return Ok(ListColumnsLocated::Full),
            ListColumnsLocated::Location(found) => {
                crate::partition_pruning::union_list_partition_location(&mut location, found);
            }
        }
    }
    Ok(ListColumnsLocated::Location(location))
}

fn list_columns_pruned_ids(
    partition: &crate::partition_routing::PartitionSpec,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<Option<Vec<i64>>, tidb_planner::plan_base::PlanError> {
    Ok(
        match locate_list_columns_cnf(partition, conditions, columns, context)? {
            ListColumnsLocated::Full => None,
            ListColumnsLocated::Location(location) => Some(
                partition
                    .definitions
                    .iter()
                    .enumerate()
                    .filter_map(|(index, definition)| {
                        location.contains_key(&index).then_some(definition.id)
                    })
                    .collect(),
            ),
        },
    )
}

fn partition_indices_for_spec(
    partition: &crate::partition_routing::PartitionSpec,
    source: &tidb_planner::logical::DataSource,
    builder: &RealFunctionBuilder<'_, crate::StmtContext>,
    context: &crate::StmtContext,
) -> Result<Vec<usize>, tidb_planner::plan_base::PlanError> {
    let mut surviving = (0..partition.definitions.len()).collect::<Vec<_>>();
    if source.all_conds.is_empty() || partition.dependencies.is_empty() {
        return Ok(remap_partition_indices(
            partition,
            &source.partition_names,
            0..partition.definitions.len(),
        ));
    }
    let columns = partition
        .dependencies
        .iter()
        .map(|dependency| {
            let dependency = tidb_ast::CiString::new(dependency);
            source
                .table_columns
                .iter()
                .find(|column| {
                    column
                        .orig_name
                        .rsplit('.')
                        .next()
                        .is_some_and(|name| tidb_ast::CiString::new(name) == dependency)
                })
                .cloned()
        })
        .collect::<Option<Vec<_>>>();
    let Some(columns) = columns else {
        return Ok(remap_partition_indices(
            partition,
            &source.partition_names,
            0..partition.definitions.len(),
        ));
    };
    let conditions = source
        .all_conds
        .iter()
        .map(|condition| tidb_expr::expr_util::push_not::push_down_not(condition, builder))
        .collect::<Vec<_>>();
    if matches!(
        partition.kind,
        crate::partition_routing::PartitionKind::ListColumns { .. }
    ) {
        if let Some(ids) = list_columns_pruned_ids(partition, &conditions, &columns, context)? {
            surviving.retain(|index| ids.contains(&partition.definitions[*index].id));
        }
        return Ok(remap_partition_indices(
            partition,
            &source.partition_names,
            surviving,
        ));
    }
    let lengths = vec![tidb_datatype::UNSPECIFIED_LENGTH; columns.len()];
    let Ok(detached) = tidb_planner::ranger::detacher::detach_partition_range_with_fallback_handler(
        &conditions,
        &columns,
        &lengths,
        context.range_max_size(),
        context.range_fallback_handler(),
    ) else {
        return Ok(remap_partition_indices(
            partition,
            &source.partition_names,
            0..partition.definitions.len(),
        ));
    };
    let pruned =
        crate::partition_pruning::pruned_ids_from_ranger(partition, &detached.ranges, context)
            .map_err(|error| tidb_planner::plan_base::PlanError::internal(format!("{error:?}")))?;
    if let Some(ids) = pruned {
        surviving.retain(|index| ids.contains(&partition.definitions[*index].id));
    }
    Ok(remap_partition_indices(
        partition,
        &source.partition_names,
        surviving,
    ))
}

fn attach_dynamic_partition_access(
    plan: &mut LogicalPlan,
    pruning: &dyn tidb_planner::logical::rule_partition_processor::PartitionPruning,
) -> Result<(), tidb_planner::plan_base::PlanError> {
    if let LogicalPlan::DataSource(source) = plan {
        if !source.partition_definition_ids.is_empty() {
            let indices = pruning.partition_indices(source)?;
            let all_partitions = source.partition_names.is_empty()
                && indices.len() == source.partition_definition_ids.len();
            let partitions = if all_partitions {
                Vec::new()
            } else {
                indices
                    .into_iter()
                    .filter_map(|index| source.partition_definition_names.get(index).cloned())
                    .collect()
            };
            source.dynamic_partition_access =
                Some(tidb_planner::access::DynamicPartitionAccessObject {
                    database: source.db_name.clone(),
                    table: source
                        .table_as_name
                        .clone()
                        .unwrap_or_else(|| source.table_name.clone()),
                    all_partitions,
                    partitions,
                    error: String::new(),
                });
        }
        return Ok(());
    }
    for child in plan.base_mut().children_mut() {
        attach_dynamic_partition_access(child, pruning)?;
    }
    Ok(())
}

#[cfg(test)]
mod list_columns_pruning_tests {
    use super::*;
    use crate::partition_routing::{PartitionDef, PartitionKind, PartitionSpec};
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::{column::Column, constant::Constant, scalar_function::ScalarFunction};

    fn integer_type() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(id: i64, unique_id: i64, index: i64) -> Column {
        let mut column = Column::new(unique_id, integer_type());
        column.id = id;
        column.index = index;
        column
    }

    fn equals(column: &Column, value: i64) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(column.clone()),
                Expression::Constant(Constant::new(Datum::Int(value), integer_type())),
            ],
        ))
    }

    fn or(arguments: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("or"),
            FieldType::new(FieldTypeCode::Tiny),
            arguments,
        ))
    }

    fn list_columns_spec() -> PartitionSpec {
        let field_type = integer_type();
        PartitionSpec {
            overlapping_dropping_partition_indices: Vec::new(),
            is_empty_columns: false,
            kind: PartitionKind::ListColumns {
                values: vec![
                    (vec![Datum::Int(1), Datum::Int(5)], 0),
                    (vec![Datum::Int(1), Datum::Int(6)], 0),
                    (vec![Datum::Int(1), Datum::Int(7)], 1),
                    (vec![Datum::Int(9), Datum::Int(9)], 1),
                ],
                keys: Default::default(),
                default_partition: Some(2),
                field_types: vec![field_type.clone(), field_type.clone()],
            },
            expr_text: "`a`,`b`".to_owned(),
            expr: Expression::Constant(Constant::new(Datum::Null, field_type)),
            dependencies: vec!["a".to_owned(), "b".to_owned()],
            definitions: (0..3)
                .map(|ordinal| PartitionDef {
                    id: 501 + ordinal,
                    name: format!("p{ordinal}"),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                })
                .collect(),
        }
    }

    #[test]
    fn list_columns_prunes_each_referenced_column_and_intersects_tuple_groups() {
        let spec = list_columns_spec();
        let columns = vec![column(1, 11, 0), column(2, 12, 1)];
        let context = crate::StmtContext::for_query();

        assert_eq!(
            list_columns_pruned_ids(&spec, &[equals(&columns[1], 7)], &columns, &context).unwrap(),
            Some(vec![502, 503]),
            "a predicate on the second partition column is prunable; Go also retains DEFAULT"
        );
        assert_eq!(
            list_columns_pruned_ids(
                &spec,
                &[equals(&columns[0], 1), equals(&columns[1], 9)],
                &columns,
                &context,
            )
            .unwrap(),
            Some(vec![503]),
            "CNF intersection must use tuple-group identity, not just partition identity"
        );
        assert_eq!(
            list_columns_pruned_ids(
                &spec,
                &[or(vec![equals(&columns[1], 6), equals(&columns[0], 9)])],
                &columns,
                &context,
            )
            .unwrap(),
            Some(vec![501, 502, 503]),
            "DNF union keeps every located tuple group and Go's DEFAULT group"
        );
    }

    #[test]
    fn dropping_partition_ordinals_are_remapped_before_static_children() {
        let mut spec = list_columns_spec();
        spec.overlapping_dropping_partition_indices = vec![Some(2), Some(2), Some(2)];

        assert_eq!(
            remap_partition_indices(&spec, &[], [0, 1, 2]),
            vec![2],
            "Go deduplicates every dropping definition that overlaps the same readable partition"
        );
        assert!(
            remap_partition_indices(&spec, &["p0".to_owned()], [0]).is_empty(),
            "the explicit partition name is checked against Go's remapped definition"
        );

        let location = crate::partition_pruning::ListPartitionLocation::from([
            (0, std::collections::BTreeSet::from([0, 1])),
            (1, std::collections::BTreeSet::from([2])),
        ]);
        assert_eq!(
            remap_list_columns_location(&spec, location),
            crate::partition_pruning::ListPartitionLocation::from([(
                2,
                std::collections::BTreeSet::from([-1])
            ),]),
            "a remapped LIST COLUMNS location uses Go's special group and merges duplicates"
        );

        spec.overlapping_dropping_partition_indices = vec![None, Some(1), Some(2)];
        assert!(
            remap_partition_indices(&spec, &[], [0]).is_empty(),
            "Go skips a dropping definition with no readable overlap"
        );
    }

    #[test]
    fn partition_pruning_fallback_keeps_explicit_partition_names() {
        let spec = list_columns_spec();
        let mut source = tidb_planner::logical::DataSource::default();
        source.partition_names = vec!["p1".to_owned()];
        source.all_conds = vec![Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::Tiny),
        ))];
        let context = crate::StmtContext::for_query();
        let builder = RealFunctionBuilder::new(&context);

        assert_eq!(
            partition_indices_for_spec(&spec, &source, &builder, &context).unwrap(),
            vec![1]
        );
    }
}

impl tidb_planner::logical::rule::PlanCacheMarker for crate::StmtContext {
    fn set_skip_plan_cache(&self, reason: &str) {
        crate::StmtContext::set_skip_plan_cache(self, reason);
    }
}

impl tidb_planner::logical::rule::HintWarningSink for crate::StmtContext {
    fn set_hint_warning(&self, message: &str) {
        self.append_warning_parts(1815, message);
    }
}

/// Builds the name-resolution scope of one `FROM` node through Go's logical
/// `buildResultSetNode` path. Correlation discovery needs the logical schema
/// and output names, not an executor or a second AST-side join builder.
pub(crate) fn logical_from_scope(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<FromScope, tidb_planner::plan_base::PlanError> {
    let source = catalog.planner_catalog(current_database, ctx.latest_index_schema());
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone());
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_partition_processor_enabled(ctx.static_partition_prune());
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
    builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
    builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
    let plan = builder.build_join(join)?;
    let schema = plan.schema().ok_or_else(|| {
        tidb_planner::plan_base::PlanError::internal("FROM logical plan has no schema")
    })?;
    let names = plan.output_names();
    if schema.columns.len() != names.len() {
        return Err(tidb_planner::plan_base::PlanError::internal(
            "FROM logical schema and output names have different widths",
        ));
    }

    let mut scope = FromScope::for_statement(ctx);
    for (column, name) in schema.columns.iter().zip(names) {
        if name.hidden || name.not_explicit_usable {
            continue;
        }
        let field_type = column.ret_type.clone().ok_or_else(|| {
            tidb_planner::plan_base::PlanError::internal("FROM column has no field type")
        })?;
        let database = &name.names.database.original;
        let visible_table = &name.names.table.original;
        let column_name = &name.names.column.original;
        let offset = scope.width();
        let append = scope.tables.last_mut().filter(|table| {
            table.name.eq_ignore_ascii_case(visible_table)
                && table
                    .database
                    .as_deref()
                    .unwrap_or_default()
                    .eq_ignore_ascii_case(database)
        });
        match append {
            Some(table) => table.columns.push((column_name.clone(), field_type)),
            None => scope.tables.push(FromTable {
                name: visible_table.clone(),
                database: (!database.is_empty()).then(|| database.clone()),
                columns: vec![(column_name.clone(), field_type)],
                offset,
            }),
        }
        if name.redundant {
            scope.coalesced.push(offset);
        } else {
            scope.star.push(offset);
        }
    }
    if scope.star.len() == scope.width() {
        scope.star.clear();
    }
    Ok(scope)
}

/// Detaches parameter/deferred markers from an executor-owned expression
/// after its current value has been materialized. The cache-owned physical
/// tree keeps its markers and is rebuilt before this executor copy is made.
pub(super) fn materialize_physical_expression(expression: &mut Expression) {
    match expression {
        Expression::Column(_) | Expression::CorrelatedColumn(_) => {}
        Expression::Constant(constant) => {
            constant.param_marker = None;
            constant.deferred_expr = None;
        }
        Expression::ScalarFunction(function) => {
            for argument in &mut function.args {
                materialize_physical_expression(argument);
            }
        }
    }
}

/// Splits an AST predicate into its top-level `AND` conjuncts.
fn split_ast_conjuncts(expr: &tidb_ast::Expr, out: &mut Vec<tidb_ast::Expr>) {
    if let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, left, right) = expr {
        split_ast_conjuncts(left, out);
        split_ast_conjuncts(right, out);
    } else {
        out.push(expr.clone());
    }
}

/// Keeps only the `WHERE` conjuncts whose every column resolves against ONE
/// data source's scope. Go derives a data source's statistics from its
/// `PushedDownConds`; before predicate push-down runs, this is the equivalent
/// split of the statement predicate, and it keeps a cross-table equality from
/// being charged to either side as an extra selection factor.
fn single_table_predicate(
    expr: &tidb_ast::Expr,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> Option<tidb_ast::Expr> {
    struct Paths(Vec<Vec<String>>);
    impl tidb_ast::Visitor for Paths {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Column(path)) = node.downcast_ref::<tidb_ast::Expr>() {
                self.0.push(path.clone());
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    // A conjunct that CONTAINS a subquery is never one source's own filter:
    // Go's pre-push-down `DataSource` has no `PushedDownConds`, and the
    // predicate push-down rules attach subquery-derived predicates (the
    // semi-join's not-null probe, for instance) later. Without this an
    // unqualified `k IN (SELECT k FROM t)` resolved against the SUBQUERY's
    // own source too and charged it the generic 0.8 fallback, halving its NDV.
    struct ContainsSubquery(bool);
    impl tidb_ast::Visitor for ContainsSubquery {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() {
                if matches!(
                    expr,
                    tidb_ast::Expr::Subquery(_)
                        | tidb_ast::Expr::CompareSubquery { .. }
                        | tidb_ast::Expr::InSubquery { .. }
                        | tidb_ast::Expr::Exists { .. }
                ) {
                    self.0 = true;
                }
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut conjuncts = Vec::new();
    split_ast_conjuncts(expr, &mut conjuncts);
    conjuncts.retain(|conjunct| {
        let mut visitor = ContainsSubquery(false);
        let mut owned = conjunct.clone();
        tidb_ast::Visitable::accept(&mut owned, &mut visitor);
        !visitor.0
    });
    let resolves = |conjunct: &tidb_ast::Expr| {
        let mut paths = Paths(Vec::new());
        let mut owned = conjunct.clone();
        tidb_ast::Visitable::accept(&mut owned, &mut paths);
        !paths.0.is_empty()
            && paths
                .0
                .iter()
                .all(|path| resolver.resolve_expression(path).is_some())
    };
    let mut kept: Vec<tidb_ast::Expr> = conjuncts
        .iter()
        .filter(|conjunct| resolves(conjunct))
        .cloned()
        .collect();
    // Go `PropagateConstantForJoin`: `src.col = other.col AND other.col = 1`
    // also filters `src.col = 1`. The pre-push-down split needs the same
    // constant so the source's point estimate sees the complete key.
    for conjunct in &conjuncts {
        let Some((left, right)) = equality_sides(conjunct) else {
            continue;
        };
        let (local, foreign) = match (resolves(left), resolves(right)) {
            (true, false) => (left, right),
            (false, true) => (right, left),
            _ => continue,
        };
        let tidb_ast::Expr::Column(local_path) = local else {
            continue;
        };
        let tidb_ast::Expr::Column(foreign_path) = foreign else {
            continue;
        };
        let Some(constant) = conjuncts.iter().find_map(|candidate| {
            let (candidate_left, candidate_right) = equality_sides(candidate)?;
            let same = |side: &tidb_ast::Expr| {
                matches!(
                    side,
                    tidb_ast::Expr::Column(path)
                        if path.len() == foreign_path.len()
                            && path
                                .iter()
                                .zip(foreign_path.iter())
                                .all(|(a, b)| a.eq_ignore_ascii_case(b))
                )
            };
            if same(candidate_left) && !matches!(candidate_right, tidb_ast::Expr::Column(_)) {
                Some(candidate_right.clone())
            } else if same(candidate_right) && !matches!(candidate_left, tidb_ast::Expr::Column(_))
            {
                Some(candidate_left.clone())
            } else {
                None
            }
        }) else {
            continue;
        };
        let synthesized = tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::Eq,
            Box::new(tidb_ast::Expr::Column(local_path.clone())),
            Box::new(constant),
        );
        if !kept.iter().any(|existing| existing == &synthesized) {
            kept.push(synthesized);
        }
    }
    let mut iter = kept.into_iter();
    let mut combined = iter.next()?;
    for conjunct in iter {
        combined = tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(combined),
            Box::new(conjunct),
        );
    }
    Some(combined)
}

/// The two operands of a top-level `=` conjunct.
fn equality_sides(expr: &tidb_ast::Expr) -> Option<(&tidb_ast::Expr, &tidb_ast::Expr)> {
    let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::Eq, left, right) = expr else {
        return None;
    };
    Some((left, right))
}

/// The columns Go's lite statistics initialization loads for this statement:
/// every column compared against a constant in any query block. Go loads
/// exactly these payloads (and the indexes whose first column they cover) and
/// leaves every other column evicted, which is what makes
/// `EstimateColumnNDV` borrow a loaded index's analyzed row count.
fn predicate_column_names(select: &tidb_ast::SelectStmt) -> Vec<(Option<String>, String)> {
    struct FilterColumns(Vec<(Option<String>, String)>);

    impl FilterColumns {
        fn collect_paths(&mut self, expr: &tidb_ast::Expr) {
            struct Paths(Vec<Vec<String>>);
            impl tidb_ast::Visitor for Paths {
                fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
                    if let Some(tidb_ast::Expr::Column(path)) =
                        node.downcast_ref::<tidb_ast::Expr>()
                    {
                        self.0.push(path.clone());
                    }
                    false
                }
                fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
                    true
                }
            }
            let mut paths = Paths(Vec::new());
            let mut owned = expr.clone();
            tidb_ast::Visitable::accept(&mut owned, &mut paths);
            for path in paths.0 {
                let Some(name) = path.last() else {
                    continue;
                };
                let qualifier = (path.len() > 1).then(|| path[path.len() - 2].clone());
                self.0.push((qualifier, name.clone()));
            }
        }
    }

    impl tidb_ast::Visitor for FilterColumns {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() else {
                return false;
            };
            match expr {
                tidb_ast::Expr::Binary(op, lhs, rhs) if is_comparison_operator(*op) => {
                    if is_constant_literal(rhs) {
                        self.collect_paths(lhs);
                    } else if is_constant_literal(lhs) {
                        self.collect_paths(rhs);
                    }
                }
                tidb_ast::Expr::In { expr, list, .. }
                    if !list.is_empty() && list.iter().all(is_constant_literal) =>
                {
                    self.collect_paths(expr);
                }
                tidb_ast::Expr::Is { expr, .. } => self.collect_paths(expr),
                _ => {}
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut visitor = FilterColumns(Vec::new());
    let mut owned = select.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut visitor);
    visitor.0
}

fn is_comparison_operator(op: tidb_ast::BinaryOp) -> bool {
    matches!(
        op,
        tidb_ast::BinaryOp::Eq
            | tidb_ast::BinaryOp::NullEq
            | tidb_ast::BinaryOp::Ne
            | tidb_ast::BinaryOp::Lt
            | tidb_ast::BinaryOp::Le
            | tidb_ast::BinaryOp::Gt
            | tidb_ast::BinaryOp::Ge
    )
}

fn is_constant_literal(expr: &tidb_ast::Expr) -> bool {
    matches!(
        expr,
        tidb_ast::Expr::Int(_)
            | tidb_ast::Expr::Decimal(_)
            | tidb_ast::Expr::Float(_)
            | tidb_ast::Expr::Hex(_)
            | tidb_ast::Expr::Bit(_)
            | tidb_ast::Expr::String(_)
            | tidb_ast::Expr::RawString(_)
            | tidb_ast::Expr::Null
            | tidb_ast::Expr::Bool(_)
            | tidb_ast::Expr::ParamMarker {
                in_execute: true,
                value: Some(_),
                ..
            }
    )
}

struct InitStats<'a> {
    range_context: crate::index_range::RangeContext<'a>,
    catalog: &'a Catalog,
    select: Option<&'a tidb_ast::SelectStmt>,
    default_string_match_selectivity: f64,
    selectivity_factor: f64,
    enable_pseudo_for_outdated_stats: bool,
    context: &'a crate::StmtContext,
}

fn same_statistics_predicate(
    left: &tidb_expr::expression::Expression,
    right: &tidb_expr::expression::Expression,
) -> bool {
    use tidb_expr::expression::Expression;
    if left.equal(right) {
        return true;
    }
    let (Expression::ScalarFunction(lhs), Expression::ScalarFunction(rhs)) = (left, right) else {
        return false;
    };
    if lhs.func_name.lowercase() != rhs.func_name.lowercase() {
        return false;
    }
    let split = match lhs.func_name.lowercase() {
        "and" => tidb_expr::expr_util::split_cnf_items,
        "or" => tidb_expr::expr_util::split_dnf_items,
        _ => return false,
    };
    let left = split(left);
    let mut right = split(right);
    if left.len() != right.len() {
        return false;
    }
    for item in left {
        let Some(index) = right
            .iter()
            .position(|other| same_statistics_predicate(&item, other))
        else {
            return false;
        };
        right.swap_remove(index);
    }
    true
}

impl InitStats<'_> {
    /// The `(column ids, index ids)` Go's lite statistics initialization
    /// loads for this source: the columns its own predicates compare against
    /// a constant, plus the indexes whose first column those cover.
    ///
    /// `None` when the statement carries no such predicate for this source,
    /// which leaves the caller on the "everything is loaded" approximation
    /// that predates the loading model.
    fn predicate_loaded_items(
        &self,
        source: &tidb_planner::logical::DataSource,
        statistics: Option<&crate::access_cost::TableStatistics>,
    ) -> Option<(BTreeSet<i64>, BTreeSet<i64>)> {
        statistics?;
        let names = predicate_column_names(self.select?);
        if names.is_empty() {
            return None;
        }
        let visible = source
            .table_as_name
            .as_deref()
            .unwrap_or(&source.table_name)
            .to_lowercase();
        let table_name = source.table_name.to_lowercase();
        let db_name = source.db_name.to_lowercase();
        let mut columns = BTreeSet::new();
        for (qualifier, name) in &names {
            if let Some(qualifier) = qualifier {
                let qualifier = qualifier.to_lowercase();
                if qualifier != visible && qualifier != table_name && qualifier != db_name {
                    continue;
                }
            }
            for column in &source.columns {
                if column.name.eq_ignore_ascii_case(name) {
                    columns.insert(column.id);
                }
            }
        }
        if columns.is_empty() {
            return None;
        }
        let mut indexes = BTreeSet::new();
        if let Some(TableEntry::Kv(table)) =
            self.catalog.get_in(&source.db_name, &source.table_name)
        {
            for index in table.indexes() {
                let first = index
                    .column_offsets
                    .first()
                    .and_then(|offset| table.visible_columns().get(*offset));
                if first.is_some_and(|column| columns.contains(&column.id)) {
                    indexes.insert(index.id);
                }
            }
        }
        Some((columns, indexes))
    }
}

impl OwnedRewrite for InitStats<'_> {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): Self::Down) -> Descend<Self::Down, Self::Up> {
        let LogicalPlan::DataSource(source) = node else {
            return Descend::Children(vec![(); node.children().len()]);
        };
        source.base.base.set_stats(None);
        source.table_path_count_after_access = None;
        source.index_path_count_after_access.clear();
        source.index_path_row_estimates.clear();
        // Go `initStats` calls `GetStatsTable(..., ds.PhysicalTableID)`: a
        // static-pruning child owns one physical partition's statistics,
        // while an ordinary/dynamic source keeps the logical table ID here.
        let stored_statistics = self.catalog.table_statistics(source.physical_table_id);
        // Go `GetStatsTable` copies the cached table before marking an
        // outdated distribution pseudo. The switch belongs to this session,
        // so the shared statistics cache must remain unchanged for peers.
        let statistics = stored_statistics.as_deref().map(|statistics| {
            if self.enable_pseudo_for_outdated_stats && statistics.is_outdated() {
                let mut copied = statistics.clone();
                copied.pseudo = true;
                Cow::Owned(copied)
            } else {
                Cow::Borrowed(statistics)
            }
        });
        let statistics = statistics.as_deref();
        source.analyzed_index_ids = statistics
            .map(|stats| {
                stats
                    .index_stats_existence
                    .iter()
                    .filter_map(|(id, analyzed)| analyzed.then_some(*id))
                    .collect()
            })
            .unwrap_or_default();
        let row_count = crate::access_cost::realtime_row_count(statistics);
        // Go loads only the predicate columns' payloads (and the indexes they
        // cover) and leaves the rest evicted; `estimate_column_ndv` then
        // borrows a loaded same-version index's analyzed count for an evicted
        // column. Without a predicate for this source the approximation is
        // "everything is loaded", which is the pre-lite-init shape.
        let (loaded_columns, loaded_indexes) = self
            .predicate_loaded_items(source, statistics)
            .unwrap_or_else(|| {
                (
                    statistics
                        .map(|statistics| statistics.columns.keys().copied().collect())
                        .unwrap_or_default(),
                    statistics
                        .map(|statistics| statistics.indexes.keys().copied().collect())
                        .unwrap_or_default(),
                )
            });
        let ndvs = source
            .columns
            .iter()
            .zip(
                source
                    .base
                    .base
                    .schema()
                    .into_iter()
                    .flat_map(|schema| &schema.columns),
            )
            .map(|(metadata, column)| {
                // Go `cardinality.EstimateColumnNDV`: a pseudo or missing
                // histogram uses `RealtimeCount * distinctFactor` (0.8),
                // while an analyzed histogram scales its NDV from analyze
                // time to the current realtime row count.
                let ndv = statistics.map_or(row_count * 0.8, |statistics| {
                    if statistics.pseudo {
                        row_count * 0.8
                    } else {
                        statistics
                            .estimate_column_ndv(metadata.id, &loaded_columns, &loaded_indexes)
                            .unwrap_or(row_count * 0.8)
                    }
                });
                (column.unique_id, ndv)
            })
            .collect::<Vec<_>>();
        // Go `initStats` always attaches the table's generated HistColl,
        // including for a pseudo table. That presence is semantically visible
        // to cost model v2: base-table rows include DataInDiskByRows' eight
        // bytes per column, while a join/projection-created StatsInfo has a
        // nil HistColl and uses static type width only.
        let row_size_columns = source
            .table_columns
            .iter()
            .filter_map(|column| {
                let loaded = statistics?.columns.get(&column.id)?;
                let field_type = column.ret_type.as_ref()?;
                let is_handle = source
                    .handle_cols
                    .iter()
                    .any(|handle| handle.unique_id == column.unique_id);
                Some((
                    column.unique_id,
                    RowSizeColumnStats::new(
                        RowSizeType::from_field_type_code(field_type.code()),
                        loaded.histogram.tot_col_size,
                        loaded.histogram.null_count,
                        loaded.total_row_count(),
                        is_handle,
                    ),
                ))
            })
            .collect::<Vec<_>>();
        // The planner's DataSource-statistics rule reads the loaded
        // histograms through `HistColl`; Go's `deriveStats4DataSource` uses
        // the histogram-aware `Selectivity`, not an NDV approximation.
        let histograms = source
            .table_columns
            .iter()
            .filter_map(|column| {
                let loaded = statistics?.columns.get(&column.id)?;
                Some((column.unique_id, std::sync::Arc::new(loaded.clone())))
            })
            .collect::<Vec<_>>();
        // Go `HistColl.Indices`' NDVs feed `getGroupNDVs`, which matches an
        // index's whole column list against a source's asked column groups.
        let index_ndvs = source
            .indexes
            .iter()
            .filter_map(|index| {
                let loaded = statistics?.indexes.get(&index.id)?;
                if loaded.histogram.ndv <= 0 {
                    return None;
                }
                let columns = index
                    .columns
                    .iter()
                    .filter_map(|column| {
                        source
                            .schema_column_for_index_column(column)
                            .map(|planned| planned.unique_id)
                    })
                    .collect::<Vec<_>>();
                (columns.len() == index.columns.len())
                    .then(|| (index.id, (columns, loaded.histogram.ndv as f64)))
            })
            .collect::<Vec<_>>();
        source.table_stats = Some(
            StatsInfo::new(row_count, ndvs)
                .with_hist_coll(
                    HistColl::new(
                        statistics.is_none_or(|statistics| statistics.pseudo),
                        row_count as i64,
                        row_size_columns,
                    )
                    .with_histograms(histograms)
                    .with_modify_count(statistics.map_or(0, |statistics| statistics.modify_count))
                    .with_pk_is_handle(source.handle_is_int)
                    .with_index_ndvs(index_ndvs),
                )
                .with_stats_version(statistics.map_or(tidb_stats::PSEUDO_VERSION, |statistics| {
                    if statistics.pseudo || statistics.stats_ver <= 0 {
                        tidb_stats::PSEUDO_VERSION
                    } else {
                        statistics.stats_ver as u64
                    }
                })),
        );
        // Go derives a data source's statistics from its own pushed-down
        // conditions. Before predicate push-down, keep only the statement
        // conjuncts that resolve against THIS source, so a cross-table
        // equality cannot scale either side's profile.
        let source_table = self.catalog.get_in(&source.db_name, &source.table_name);
        let scoped_predicate = self.select.and_then(|select| {
            let where_clause = select.where_clause.as_ref()?;
            let TableEntry::Kv(table) = source_table? else {
                return None;
            };
            let visible = source
                .table_as_name
                .as_deref()
                .unwrap_or(&source.table_name);
            // Go uses the data source's current ranger/evaluation context.
            // A name-only scope cannot evaluate EXECUTE parameters or apply
            // the statement's conversion policy while deriving path costs.
            let mut scope = FromScope::for_statement(self.context);
            scope.tables.push(FromTable {
                name: visible.to_owned(),
                database: Some(source.db_name.clone()),
                columns: table
                    .visible_columns()
                    .iter()
                    .map(|column| (column.name.clone(), column.field_type.clone()))
                    .collect(),
                offset: 0,
            });
            let predicate =
                single_table_predicate(where_clause, &crate::driver::from::scope_resolver(&scope))?;
            Some((predicate, scope))
        });
        if let (Some((predicate, scope)), Some(TableEntry::Kv(table)), Some(table_stats)) = (
            scoped_predicate.as_ref(),
            source_table,
            source.table_stats.clone(),
        ) {
            let resolver = crate::driver::from::scope_resolver(scope);
            source.table_path_count_after_access =
                crate::handle_range::build_handle_ranges(table, predicate, &resolver)
                    .map(|built| {
                        crate::handle_range::handle_range_row_count(
                            table,
                            &built.ranges,
                            statistics,
                            false,
                        )
                    })
                    .or(Some(row_count));
            for index in table.plan_indexes() {
                let columns = index
                    .column_offsets
                    .iter()
                    .enumerate()
                    .map(|(position, offset)| {
                        let column = table.columns.get(*offset)?;
                        Some(crate::index_range::RangeColumn {
                            name: column.name.clone(),
                            field_type: column.field_type.clone(),
                            prefix_len: index.prefix_length(position),
                        })
                    })
                    .collect::<Option<Vec<_>>>();
                let Some(columns) = columns else {
                    continue;
                };
                let Some(built) = crate::index_range::detach_cond_and_build_range_for_index(
                    &columns, predicate, &resolver,
                ) else {
                    continue;
                };
                let estimate = crate::access_cost::index_row_count(
                    index,
                    table,
                    &built.ranges,
                    statistics,
                    row_count,
                    false,
                );
                source
                    .index_path_count_after_access
                    .insert(index.id, estimate.est);
                source.index_path_row_estimates.insert(index.id, estimate);
            }
            let selectivity = crate::access_cost::selectivity_with_range_context(
                predicate,
                table,
                &resolver,
                statistics,
                tidb_planner::selectivity_greedy::SelectivityDefaults {
                    trigger_load: false,
                    ..tidb_planner::selectivity_greedy::SelectivityDefaults::from_session(
                        self.default_string_match_selectivity,
                        self.selectivity_factor,
                    )
                },
                self.range_context,
            );
            let cached_predicate_matches = source.pushed_down_conds.is_empty()
                || source.base.base.schema().is_some_and(|schema| {
                    let names = source
                        .columns
                        .iter()
                        .map(|column| {
                            tidb_datatype::FieldName::new(tidb_datatype::FieldNameMetadata {
                                table: tidb_datatype::IdentifierMetadata::new(
                                    source
                                        .table_as_name
                                        .as_deref()
                                        .unwrap_or(&source.table_name),
                                ),
                                column: tidb_datatype::IdentifierMetadata::new(&column.name),
                                ..Default::default()
                            })
                        })
                        .collect();
                    let options = tidb_expr::simple_expr::BuildOptions::new()
                        .with_input_schema_and_names(schema.clone(), names);
                    tidb_expr::simple_expr::build_simple_expr(&resolver, predicate, &options)
                        .ok()
                        .is_some_and(|expression| {
                            let conditions = tidb_expr::expr_util::split_cnf_items(&expression);
                            let mut unmatched = source.pushed_down_conds.iter().collect::<Vec<_>>();
                            conditions.len() == source.pushed_down_conds.len()
                                && conditions.iter().all(|condition| {
                                    let Some(index) = unmatched.iter().position(|pushed| {
                                        same_statistics_predicate(condition, pushed)
                                    }) else {
                                        return false;
                                    };
                                    unmatched.swap_remove(index);
                                    true
                                })
                        })
                });
            // A source estimate belongs to its predicates. Optimizer-added
            // filters must be derived from the current expressions, not the
            // original statement's WHERE clause.
            if cached_predicate_matches {
                source.base.base.set_stats(Some(table_stats.scale(
                    selectivity,
                    tidb_planner::cardinality::derive_stats::DEF_SCALE_NDV_SKEW_RATIO,
                )));
            }
        }
        // Propagated predicates may not occur in this source's AST WHERE.
        // Rebuild path estimates from the optimizer's current expressions.
        if !source.pushed_down_conds.is_empty() {
            if let Some(TableEntry::Kv(table)) = source_table {
                for index in &source.indexes {
                    let prefix = index
                        .columns
                        .iter()
                        .map(|column| {
                            source
                                .schema_column_for_index_column(column)
                                .cloned()
                                .map(|planned| (planned, column.length))
                        })
                        .take_while(Option::is_some)
                        .flatten()
                        .collect::<Vec<_>>();
                    if prefix.is_empty() {
                        continue;
                    }
                    let (columns, lengths): (Vec<_>, Vec<_>) = prefix.into_iter().unzip();
                    let evaluate = |expression: &tidb_expr::expression::Expression| {
                        tidb_expr::eval_expression_once(expression, self.context)
                    };
                    let Ok(built) =
                        tidb_planner::ranger::detacher::detach_index_range_with_fallback_handler_in(
                            &source.pushed_down_conds,
                            &columns,
                            &lengths,
                            self.context.range_max_size(),
                            self.context.range_fallback_handler(),
                            &evaluate,
                        )
                    else {
                        continue;
                    };
                    let Some(metadata) = table
                        .plan_indexes()
                        .find(|candidate| candidate.id == index.id)
                    else {
                        continue;
                    };
                    let ranges = built
                        .ranges
                        .iter()
                        .map(|range| crate::kv_table::IndexRange {
                            low: range.low_val.clone(),
                            high: range.high_val.clone(),
                            low_exclusive: range.low_exclude,
                            high_exclusive: range.high_exclude,
                        })
                        .collect::<Vec<_>>();
                    let estimate = crate::access_cost::index_row_count(
                        metadata, table, &ranges, statistics, row_count, false,
                    );
                    source
                        .index_path_count_after_access
                        .insert(index.id, estimate.est);
                    source.index_path_row_estimates.insert(index.id, estimate);
                    if index.primary && source.is_common_handle {
                        source.table_path_count_after_access = Some(estimate.est);
                    }
                }
            }
        }
        source.table_scan_penalty = tidb_planner::plan_cost_ver2::TableScanPenaltyInput {
            has_range_info: false,
            // Go's `tidb_opt_prefer_range_scan` default is ON. This executor
            // does not expose a session override yet.
            allow_prefer_range_scan: true,
            pseudo_stats: statistics.is_none_or(|statistics| statistics.pseudo),
            analyze_row_count: statistics
                .map_or(-1, |statistics| statistics.analyze_row_count() as i64),
            modify_count: statistics.map_or(0, |statistics| statistics.modify_count),
            has_partition_scan: false,
            has_index_force: false,
        };
        tidb_planner::logical::rule_collect_plan_stats::refresh_source_group_ndvs(source);
        Descend::Stop(())
    }

    fn ascend(
        &mut self,
        mut node: LogicalPlan,
        _children: Vec<Self::Up>,
    ) -> (LogicalPlan, Self::Up) {
        if !matches!(node, LogicalPlan::DataSource(_)) {
            node.set_stats(None);
        }
        (node, ())
    }
}

fn planner_physical_select(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner::plan_base::PlanError> {
    let query = tidb_ast::QueryStmt::Select(Box::new(select.clone()));
    let (logical, plan_ids, column_ids) = planner_optimized_query(
        &query,
        Some(select),
        catalog,
        current_database,
        ctx,
        use_plan_cache,
    )?;
    let physical = physical_plan_for_logical(&logical, &plan_ids, &column_ids, ctx)?;
    Ok((logical, physical))
}

fn planner_physical_query(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner::plan_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    planner_physical_query_with_allocators(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        &plan_ids,
        &column_ids,
    )
}

fn planner_physical_query_with_allocators(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner::plan_base::PlanError> {
    planner_physical_query_with_registry(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
    )
    .map(|(logical, physical, _)| (logical, physical))
}

/// [`planner_physical_query_with_allocators`], additionally handing back the
/// uncorrelated subqueries the build evaluated so EXPLAIN can append their
/// registered roots.
fn planner_physical_query_with_registry(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<
    (LogicalPlan, PhysicalPlan, Vec<RegisteredScalarSubquery>),
    tidb_planner::plan_base::PlanError,
> {
    let select_hint = match query {
        tidb_ast::QueryStmt::Select(select) => Some(select.as_ref()),
        tidb_ast::QueryStmt::SetOpr(_) => None,
    };
    let registry = ScalarSubqueryRegistry::default();
    let logical = planner_optimized_query_with_allocators(
        query,
        select_hint,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        &registry,
    )?;
    let physical = physical_plan_for_logical(&logical, plan_ids, column_ids, ctx)?;
    let registered = Rc::try_unwrap(registry)
        .map(RefCell::into_inner)
        .unwrap_or_default();
    Ok((logical, physical, registered))
}

pub(crate) fn physical_plan_for_logical(
    logical: &LogicalPlan,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    ctx: &crate::StmtContext,
) -> Result<PhysicalPlan, tidb_planner::plan_base::PlanError> {
    let coster = Ver2Coster::from_env(ctx.optimizer_cost_env());
    let evaluate = |expression: &tidb_expr::expression::Expression| {
        tidb_expr::eval_expression_once(expression, ctx)
    };
    let mut dispatch = DispatchContext::new(plan_ids, &coster, 1.0)
        .with_expression_evaluator(&evaluate)
        .with_range_quota(ctx.range_max_size(), ctx.range_fallback_handler())
        .with_selectivity_factor(ctx.selectivity_factor())
        .with_ordering_index_selectivity_ratio(ctx.ordering_index_selectivity_ratio())
        .with_projection_push_down(ctx.allow_projection_push_down())
        .with_limit_push_down_threshold(ctx.limit_push_down_threshold())
        .with_paging(ctx.optimizer_cost_env().session.enable_paging)
        .with_hash_join_concurrency(
            ctx.optimizer_cost_env()
                .session
                .hash_join_concurrency
                .max(1.0) as usize,
        )
        .with_apply_cache_capacity(ctx.apply_cache_capacity())
        .with_point_get_conversion(
            !ctx.optimizer_fix_control()
                .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false),
        )
        .with_index_join_probe_row_count_fix(
            ctx.optimizer_fix_control()
                .get_bool_with_default(tidb_planner::fix_control::FIX_44855, true),
        )
        .with_column_ids(column_ids);
    let task = find_best_task(logical, &PhysicalProperty::default(), &mut dispatch)?;
    let physical = task.plan().cloned().ok_or_else(|| {
        tidb_planner::plan_base::PlanError::internal("physical planning produced no plan")
    })?;
    let physical = tidb_planner::physical::eliminate_physical_projection(physical);
    // Go postOptimize: eliminatePhysicalProjection → InjectExtraProjection.
    // The projection re-injection restores the purposeful projections
    // (scalar aggregate arguments, scalar order-by items, expression
    // nominal sorts) that the elimination pass removed.
    let mut physical =
        tidb_planner::physical::inject_extra_projection(physical, plan_ids, column_ids);
    physical.resolve_indices()?;
    physical
        .base_mut()
        .base
        .set_output_names(logical.output_names().to_vec());
    Ok(physical)
}

type CteReference = (Rc<RefCell<CteClass>>, Option<Rc<RefCell<StatsInfo>>>);

/// Collect the CTE class handles visible in one logical tree. Hidden seed and
/// recursive roots are visited when their owning class is optimized.
fn cte_references(plan: &LogicalPlan) -> Vec<CteReference> {
    let mut references = Vec::new();
    let mut seen = HashSet::new();
    plan.walk_preorder(&mut |node| {
        let LogicalPlan::CTE(cte) = node else {
            return;
        };
        let Some(class) = &cte.cte else {
            return;
        };
        let identity = Rc::as_ptr(class) as usize;
        if seen.insert(identity) {
            references.push((Rc::clone(class), cte.seed_stat.clone()));
        }
    });
    references
}

#[allow(clippy::too_many_arguments)]
fn optimize_cte_classes(
    plan: &LogicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    zone: &tidb_datatype::SessionTimeZone,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    rule_context: &RuleContext<'_>,
    visiting: &mut HashSet<usize>,
) -> Result<(), tidb_planner::plan_base::PlanError> {
    for (class, seed_stat) in cte_references(plan) {
        optimize_cte_class(
            &class,
            seed_stat.as_ref(),
            catalog,
            ctx,
            zone,
            plan_ids,
            column_ids,
            rule_context,
            visiting,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn optimize_cte_tree(
    plan: LogicalPlan,
    opt_flag: u64,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    zone: &tidb_datatype::SessionTimeZone,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    rule_context: &RuleContext<'_>,
    visiting: &mut HashSet<usize>,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner::plan_base::PlanError> {
    let optimized = logical_optimize(rule_context, opt_flag, plan)
        .map_err(|(_, error)| error)?
        .plan;
    let optimized = check_partial_index_paths(optimized, ctx, rule_context.use_plan_cache);
    let (mut optimized, ()) = fold_owned(
        &mut InitStats {
            range_context: crate::index_range::RangeContext {
                max_size: ctx.range_max_size(),
                fallback_handler: Some(ctx.range_fallback_handler()),
            },
            catalog,
            select: None,
            default_string_match_selectivity: ctx.default_string_match_selectivity(),
            selectivity_factor: ctx.selectivity_factor(),
            enable_pseudo_for_outdated_stats: ctx.enable_pseudo_for_outdated_stats(),
            context: ctx,
        },
        optimized,
        (),
    );
    optimize_cte_classes(
        &optimized,
        catalog,
        ctx,
        zone,
        plan_ids,
        column_ids,
        rule_context,
        visiting,
    )?;
    optimized.recursive_derive_stats_with_context(&[], rule_context)?;
    let logical = prepare_possible_properties(optimized).0;
    let physical = physical_plan_for_logical(&logical, plan_ids, column_ids, ctx)?;
    Ok((logical, physical))
}

#[allow(clippy::too_many_arguments)]
fn optimize_cte_class(
    class: &Rc<RefCell<CteClass>>,
    seed_stat: Option<&Rc<RefCell<StatsInfo>>>,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    zone: &tidb_datatype::SessionTimeZone,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    rule_context: &RuleContext<'_>,
    visiting: &mut HashSet<usize>,
) -> Result<(), tidb_planner::plan_base::PlanError> {
    let identity = Rc::as_ptr(class) as usize;
    {
        let class = class.borrow();
        if let Some(physical) = class.seed_part_physical_plan.as_deref() {
            if let (Some(seed_stat), Some(stats)) = (seed_stat, physical.stats_info()) {
                *seed_stat.borrow_mut() = stats.clone();
            }
            return Ok(());
        }
    }
    if !visiting.insert(identity) {
        return Err(tidb_planner::plan_base::PlanError::internal(
            "LogicalCTE.DeriveStats: cyclic CTE class optimization",
        ));
    }

    let result = (|| {
        let (mut seed, recursive, mut opt_flag, pushed_predicates) = {
            let class = class.borrow();
            let seed = class
                .seed_part_logical_plan
                .as_deref()
                .ok_or_else(|| {
                    tidb_planner::plan_base::PlanError::internal(
                        "LogicalCTE.DeriveStats: seed logical plan is nil",
                    )
                })?
                .deep_clone();
            (
                seed,
                class
                    .recursive_part_logical_plan
                    .as_deref()
                    .map(LogicalPlan::deep_clone),
                class.opt_flag,
                class.push_down_predicates.clone(),
            )
        };

        // Go composes the predicates recorded by every reference as one DNF,
        // extracts common conjuncts, and puts that Selection above the seed
        // before running the CTE's own optimizer pass.
        if let Some(dnf) = compose_dnf_condition(pushed_predicates) {
            let conditions = extract_filters_from_dnfs(vec![dnf]);
            let query_block_offset = seed.base().base.query_block_offset();
            let mut selection = LogicalSelection::new(
                BaseLogicalPlan::new(plan_ids, LogicalSelection::TYPE, query_block_offset),
                conditions,
            );
            selection.base.set_children(vec![seed]);
            seed = LogicalPlan::Selection(selection);
            opt_flag = tidb_planner::logical::rule::set_predicate_push_down_flag(opt_flag);
        }

        let (seed_logical, seed_physical) = optimize_cte_tree(
            seed,
            opt_flag,
            catalog,
            ctx,
            zone,
            plan_ids,
            column_ids,
            rule_context,
            visiting,
        )?;
        let seed_stats = seed_physical.stats_info().cloned().ok_or_else(|| {
            tidb_planner::plan_base::PlanError::internal(
                "LogicalCTE.DeriveStats: seed physical stats are nil",
            )
        })?;
        if let Some(seed_stat) = seed_stat {
            *seed_stat.borrow_mut() = seed_stats;
        }
        {
            let mut class = class.borrow_mut();
            class.seed_part_logical_plan = Some(Box::new(seed_logical));
            class.seed_part_physical_plan = Some(Box::new(seed_physical));
        }

        if let Some(recursive) = recursive {
            let (recursive_logical, recursive_physical) = optimize_cte_tree(
                recursive,
                opt_flag,
                catalog,
                ctx,
                zone,
                plan_ids,
                column_ids,
                rule_context,
                visiting,
            )?;
            let mut class = class.borrow_mut();
            class.recursive_part_logical_plan = Some(Box::new(recursive_logical));
            class.recursive_part_physical_plan = Some(Box::new(recursive_physical));
        }
        Ok(())
    })();
    visiting.remove(&identity);
    result
}

/// Builds the ordinary physical SELECT tree consumed by the common executor
/// builder. Go passes both fresh and cache-rebuilt plans to
/// `executorBuilder.build`; keeping the full tree here prevents ordinary
/// execution from rediscovering its operators from the SQL AST.
pub(crate) fn physical_select_plan(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<PhysicalPlan, tidb_planner::plan_base::PlanError> {
    if select.rollup {
        return Err(tidb_planner::plan_base::PlanError::internal(
            "ROLLUP physical planning is not implemented",
        ));
    }
    planner_physical_select(select, catalog, current_database, ctx, false)
        .map(|(_, physical)| physical)
}

/// Builds the ordinary physical tree for either Go query-statement shape.
/// Set operations, their CTEs, and plain SELECTs therefore all enter the same
/// logical optimizer, physical search, and executor-builder switch.
pub(crate) fn physical_query_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<PhysicalPlan, tidb_planner::plan_base::PlanError> {
    if matches!(query, tidb_ast::QueryStmt::Select(select) if select.rollup) {
        return Err(tidb_planner::plan_base::PlanError::internal(
            "ROLLUP physical planning is not implemented",
        ));
    }
    planner_physical_query(query, catalog, current_database, ctx, false)
        .map(|(_, physical)| physical)
}

/// [`physical_query_plan`], additionally handing back the uncorrelated
/// subqueries the build evaluated. Go keeps them in `StmtCtx` and EXPLAIN
/// appends each one's optimized child as an extra root.
pub(crate) fn physical_query_plan_with_scalar_subqueries(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<(PhysicalPlan, Vec<RegisteredScalarSubquery>), tidb_planner::plan_base::PlanError> {
    if matches!(query, tidb_ast::QueryStmt::Select(select) if select.rollup) {
        return Err(tidb_planner::plan_base::PlanError::internal(
            "ROLLUP physical planning is not implemented",
        ));
    }
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    planner_physical_query_with_registry(
        query,
        catalog,
        current_database,
        ctx,
        false,
        &plan_ids,
        &column_ids,
    )
    .map(|(_, physical, registered)| (physical, registered))
}

/// Builds a query below a non-query physical root using that statement's
/// plan and column allocators. Go's DML builders allocate the write root and
/// its `SelectPlan` from the same session counters; keeping those counters
/// shared preserves both tree ownership and explain identities.
pub(crate) fn physical_query_plan_with_allocators(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<PhysicalPlan, tidb_planner::plan_base::PlanError> {
    if matches!(query, tidb_ast::QueryStmt::Select(select) if select.rollup) {
        return Err(tidb_planner::plan_base::PlanError::internal(
            "ROLLUP physical planning is not implemented",
        ));
    }
    planner_physical_query_with_allocators(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
    )
    .map(|(_, physical)| physical)
}

/// One uncorrelated subquery Go registers in the statement context while
/// `handleScalarSubquery` / `handleExistSubquery` evaluates it.
///
/// Go's `ScalarSubqueryEvalCtx` (`pkg/planner/core/scalar_subq_expression.go`)
/// keeps the OPTIMIZED child plan and the output column ids its placeholder
/// expressions carry; `FlatPhysicalPlan.flatten` appends the plan as an extra
/// EXPLAIN root whose `ExplainInfo` names those ids.
pub(crate) struct RegisteredScalarSubquery {
    /// Go `ScalarSubqueryEvalCtx.outputColIDs`.
    pub output_col_ids: Vec<i64>,
    /// Go `ScalarSubqueryEvalCtx.scalarSubQuery`, the optimized child.
    pub physical: PhysicalPlan,
    /// Go `ScalarSubqueryEvalCtx.QueryBlockOffset()`, the EXPLAIN node id.
    pub block_offset: i32,
}

/// The per-statement list of [`RegisteredScalarSubquery`]s, Go's
/// `SessionVars.MapScalarSubQ` narrowed to one statement.
pub(crate) type ScalarSubqueryRegistry = Rc<RefCell<Vec<RegisteredScalarSubquery>>>;

/// Carries an executor failure back across the planner boundary.
///
/// Go's `EvalSubqueryFirstRow` returns a plain error, so the typed identities
/// the driver layer owns have to survive the round trip through
/// [`tidb_planner::plan_base::PlanError`]; `planner_error_to_driver` maps them
/// back when the statement unwinds.
fn driver_error_to_plan(error: crate::DriverError) -> tidb_planner::plan_base::PlanError {
    match error {
        crate::DriverError::SubqueryReturnsMoreThanOneRow => {
            tidb_planner::plan_base::PlanError::subquery_returns_more_than_one_row()
        }
        crate::DriverError::Exec(crate::ExecError::Eval(eval)) => {
            tidb_planner::plan_base::PlanError::eval(eval)
        }
        other => tidb_planner::plan_base::PlanError::internal(other.to_string()),
    }
}

/// Go's `EvalSubqueryFirstRow` function variable
/// (`pkg/planner/core/expression_rewriter.go:55`), closed over one
/// statement's catalog and allocators.
///
/// The body is the executor half of `handleScalarSubquery` /
/// `handleExistSubquery`: `DoOptimize` the child, allocate its output column
/// ids, register it for EXPLAIN, then run it and report the first row.
fn subquery_evaluator<'a>(
    catalog: &'a Catalog,
    ctx: &'a crate::StmtContext,
    plan_ids: &'a PlanIdAllocator,
    column_ids: &'a ColumnIdAllocator,
    use_plan_cache: bool,
    session_zone: &'a tidb_expr::SessionTimeZone,
    registry: &ScalarSubqueryRegistry,
) -> impl Fn(
    &LogicalPlan,
    tidb_planner::plan_builder::SubqueryKind,
    u64,
) -> Result<
    tidb_planner::plan_builder::EvaluatedSubquery,
    tidb_planner::plan_base::PlanError,
> + 'a {
    let registry = Rc::clone(registry);
    move |inner, _kind, opt_flag| {
        // Go `DoOptimize(ctx, planCtx.builder.ctx, planCtx.builder.optFlag, np)`.
        let logical = optimize_built_logical(
            inner.clone(),
            opt_flag,
            None,
            catalog,
            ctx,
            use_plan_cache,
            plan_ids,
            column_ids,
            session_zone,
        )?;
        let mut physical = physical_plan_for_logical(&logical, plan_ids, column_ids, ctx)?;
        // Go allocates `outputColIDs` from `np.Schema()` before it runs the
        // child, so the ids precede any execution-time allocation.
        let output_col_ids = (0..logical.schema().map_or(0, |schema| schema.len()))
            .map(|_| column_ids.alloc())
            .collect::<Vec<_>>();
        let block_offset = logical.base().base.query_block_offset();
        let (row, has_row) =
            crate::driver::physical_builder::execute_first_row(&mut physical, catalog, ctx)
                .map_err(driver_error_to_plan)?;
        registry.borrow_mut().push(RegisteredScalarSubquery {
            output_col_ids: output_col_ids.clone(),
            physical,
            block_offset,
        });
        Ok(tidb_planner::plan_builder::EvaluatedSubquery {
            column_ids: output_col_ids,
            row,
            has_row,
        })
    }
}

fn planner_optimized_query(
    query: &tidb_ast::QueryStmt,
    select_hint: Option<&tidb_ast::SelectStmt>,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PlanIdAllocator, ColumnIdAllocator), tidb_planner::plan_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let logical = planner_optimized_query_with_allocators(
        query,
        select_hint,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        &plan_ids,
        &column_ids,
        &registry,
    )?;
    Ok((logical, plan_ids, column_ids))
}

#[allow(clippy::too_many_arguments)]
fn planner_optimized_query_with_allocators(
    query: &tidb_ast::QueryStmt,
    select_hint: Option<&tidb_ast::SelectStmt>,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    registry: &ScalarSubqueryRegistry,
) -> Result<LogicalPlan, tidb_planner::plan_base::PlanError> {
    let source = catalog.planner_catalog(current_database, ctx.latest_index_schema());
    let session_zone = ctx.session_zone();
    let evaluator = subquery_evaluator(
        catalog,
        ctx,
        plan_ids,
        column_ids,
        use_plan_cache,
        &session_zone,
        registry,
    );
    let mut builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, session_zone.clone())
        .with_subquery_evaluator(&evaluator);
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_partition_processor_enabled(ctx.static_partition_prune());
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
    builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
    builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
    builder.prefer_index_merge_by_fix_control = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52869, false);
    let node = tidb_resolve::NodeW::new(query.clone());
    let plan = builder.build_query_node(&node, false)?;
    optimize_built_logical(
        plan,
        builder.get_opt_flag(),
        select_hint,
        catalog,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        &session_zone,
    )
}

/// Builds the retained read child of a single-table UPDATE or DELETE through
/// Go's DML-specific logical builder sequence rather than through SELECT-list
/// wildcard expansion.
#[allow(clippy::too_many_arguments)]
pub(crate) fn physical_dml_source_plan_with_allocators(
    select: &tidb_ast::SelectStmt,
    update_assignment_values: Option<&[Option<tidb_ast::Expr>]>,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<(PhysicalPlan, Vec<Option<Expression>>), tidb_planner::plan_base::PlanError> {
    let source = catalog.planner_catalog(current_database, ctx.latest_index_schema());
    let session_zone = ctx.session_zone();
    let mut builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, session_zone.clone());
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_partition_processor_enabled(ctx.static_partition_prune());
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
    builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
    builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
    builder.prefer_index_merge_by_fix_control = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52869, false);
    builder.add_opt_flag(flags::PRUNE_COLUMNS);
    let (plan, mut update_expressions, flags) = match update_assignment_values {
        Some(values) => builder.build_update_dml_source(select, values)?,
        None => {
            let (plan, flags) = builder.build_dml_source(select)?;
            (plan, Vec::new(), flags)
        }
    };
    let logical = optimize_built_logical(
        plan,
        flags,
        Some(select),
        catalog,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        &session_zone,
    )?;
    let physical = physical_plan_for_logical(&logical, plan_ids, column_ids, ctx)?;
    let schema = physical.schema().ok_or_else(|| {
        tidb_planner::plan_base::PlanError::internal("physical DML source has no schema")
    })?;
    for expression in update_expressions.iter_mut().flatten() {
        tidb_expr::simple_expr::resolve_indices_in_place(expression, schema).map_err(|_| {
            tidb_planner::plan_base::PlanError::internal(
                "UPDATE assignment does not resolve in its physical source",
            )
        })?;
    }
    Ok((physical, update_expressions))
}

#[allow(clippy::too_many_arguments)]
fn optimize_built_logical(
    plan: LogicalPlan,
    flags: u64,
    select_hint: Option<&tidb_ast::SelectStmt>,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    session_zone: &tidb_expr::SessionTimeZone,
) -> Result<LogicalPlan, tidb_planner::plan_base::PlanError> {
    struct PlannerStatisticsLoad<'a> {
        catalog: &'a Catalog,
        context: &'a crate::StmtContext,
        select: Option<&'a tidb_ast::SelectStmt>,
    }

    impl tidb_planner::logical::rule_collect_plan_stats::StatisticsLoadRequester
        for PlannerStatisticsLoad<'_>
    {
        fn request(
            &self,
            usage: &tidb_planner::logical::rule_collect_plan_stats::ColumnStatsUsage,
        ) -> Result<(), tidb_planner::plan_base::PlanError> {
            self.context.set_operator_num(usage.operator_count);
            self.context
                .update_col_stats_usage(usage.predicate_columns.keys().copied());
            self.context.record_table_runtime_statistics(
                usage.visited_logical_table_ids.iter().copied(),
                |table_id| self.catalog.table_statistics(table_id),
            );
            self.catalog.request_statistics_load(usage, self.context)
        }

        fn wait(&self) -> Result<(), tidb_planner::plan_base::PlanError> {
            self.catalog.wait_statistics_load(self.context)
        }

        fn initialize(&self, plan: LogicalPlan) -> LogicalPlan {
            fold_owned(
                &mut InitStats {
                    range_context: crate::index_range::RangeContext {
                        max_size: self.context.range_max_size(),
                        fallback_handler: Some(self.context.range_fallback_handler()),
                    },
                    catalog: self.catalog,
                    select: self.select,
                    default_string_match_selectivity: self
                        .context
                        .default_string_match_selectivity(),
                    selectivity_factor: self.context.selectivity_factor(),
                    enable_pseudo_for_outdated_stats: self
                        .context
                        .enable_pseudo_for_outdated_stats(),
                    context: self.context,
                },
                plan,
                (),
            )
            .0
        }
    }

    struct PlannerPartitionPruning<'a> {
        catalog: &'a Catalog,
        builder: &'a RealFunctionBuilder<'a, crate::StmtContext>,
        context: &'a crate::StmtContext,
    }

    impl tidb_planner::logical::rule_partition_processor::PartitionPruning
        for PlannerPartitionPruning<'_>
    {
        fn partition_indices(
            &self,
            source: &tidb_planner::logical::DataSource,
        ) -> Result<Vec<usize>, tidb_planner::plan_base::PlanError> {
            let table = self
                .catalog
                .kv_table_by_id(source.table_id)
                .ok_or_else(|| {
                    tidb_planner::plan_base::PlanError::internal(
                        "partitioned logical table is absent from the catalog",
                    )
                })?;
            let partition = table.partition().ok_or_else(|| {
                tidb_planner::plan_base::PlanError::internal(
                    "partition processor received an unpartitioned table",
                )
            })?;
            partition_indices_for_spec(partition, source, self.builder, self.context)
        }
    }

    // Go `adjustOptimizationFlags` enables both statistics rules for every
    // ordinary (non-restricted) statement; the builder never owns these
    // flags because they are session/execution policy, not AST shape.
    let flags = flags | flags::COLLECT_PREDICATE_COLUMNS_POINT | flags::SYNC_WAIT_STATS_LOAD_POINT;
    let flags = if ctx.static_partition_prune() {
        flags
    } else {
        flags & !flags::PARTITION_PROCESSOR
    };
    let flags = tidb_planner::logical::rule::add_second_column_prune(flags);
    let function_builder = RealFunctionBuilder::new(ctx);
    let statistics_load = PlannerStatisticsLoad {
        catalog,
        context: ctx,
        select: select_hint,
    };
    let partition_pruning = PlannerPartitionPruning {
        catalog,
        builder: &function_builder,
        context: ctx,
    };
    let rule_context = RuleContext {
        allocator: plan_ids,
        column_allocator: column_ids,
        builder: &function_builder,
        use_plan_cache,
        eval_context: ctx,
        plan_cache_marker: Some(ctx),
        allow_derive_topn: true,
        disabled_rules: DisabledLogicalRules::default(),
        statistics_load: Some(&statistics_load),
        partition_pruning: Some(&partition_pruning),
        opt_index_prune_threshold: ctx.opt_index_prune_threshold(),
        range_max_size: ctx.range_max_size(),
        selectivity_factor: ctx.selectivity_factor(),
        range_fallback_handler: Some(ctx.range_fallback_handler()),
        always_keep_join_key: ctx.always_keep_join_key(),
        enable_unsafe_substitute: ctx.enable_unsafe_substitute(),
        enable_semi_join_rewrite: ctx.enable_semi_join_rewrite(),
        enable_no_decorrelate_in_select: ctx.enable_no_decorrelate_in_select(),
        join_reorder_threshold: ctx.join_reorder_threshold(),
        allow_agg_push_down: ctx.allow_agg_push_down(),
        advanced_join_reorder: ctx.advanced_join_reorder(),
        cartesian_join_order_threshold: ctx.cartesian_join_order_threshold(),
        join_reorder_through_proj: ctx.join_reorder_through_proj(),
        join_reorder_through_sel: ctx.join_reorder_through_sel(),
        outer_join_reorder: ctx.outer_join_reorder(),
        advanced_join_hint: ctx.advanced_join_hint(),
        hint_warning_sink: Some(ctx),
    };
    let mut source_count = 0;
    plan.walk_preorder(&mut |plan| {
        source_count += usize::from(matches!(plan, LogicalPlan::DataSource(_)));
    });
    // Go DataSource.InitStats is available to logical rules themselves;
    // join reorder derives candidate statistics while logical optimization
    // is still running. Attach real-or-pseudo base statistics before entering
    // that rule list, rather than delaying them until physical optimization.
    let plan = tidb_planner::logical::rule_collect_plan_stats::StatisticsLoadRequester::initialize(
        &statistics_load,
        plan,
    );
    // Go's `LogicalCTE.DeriveStats` optimizes its CTE class the first time a
    // logical rule asks the producer for statistics, which happens DURING
    // `logicalOptimize`. A class reference kept alive by two or more uses (a
    // single use is inlined) therefore has to be optimized before entering the
    // rule list, or the first `recursive_derive_stats` inside a rule sees a
    // nil seed physical plan.
    optimize_cte_classes(
        &plan,
        catalog,
        ctx,
        session_zone,
        plan_ids,
        column_ids,
        &rule_context,
        &mut HashSet::new(),
    )?;
    let mut optimized = logical_optimize(&rule_context, flags, plan)
        .map_err(|(_, error)| error)?
        .plan;
    // SyncWaitStatsLoadPoint has initialized ordinary and statically pruned
    // sources from the loaded snapshot before join reorder. No partition-only
    // reinitialization or pre-load access-path estimates survive here.
    optimized = check_partial_index_paths(optimized, ctx, use_plan_cache);
    if !ctx.static_partition_prune() {
        attach_dynamic_partition_access(&mut optimized, &partition_pruning)?;
    }
    optimize_cte_classes(
        &optimized,
        catalog,
        ctx,
        session_zone,
        plan_ids,
        column_ids,
        &rule_context,
        &mut HashSet::new(),
    )?;
    optimized.recursive_derive_stats_with_context(&[], &rule_context)?;
    let logical = prepare_possible_properties(optimized).0;
    Ok(logical)
}

/// The complete physical tree retained by the prepared-plan cache. A hit
/// recursively rebuilds every parameter-dependent range in place and derives
/// the executor receipt from that rebuilt tree. No access,
/// aggregation, join, sort, or reader policy is re-run in the executor.
#[derive(Debug)]
pub(crate) struct CachedSelectPlan {
    statement: tidb_ast::Stmt,
    physical: PhysicalPlan,
    generation: u64,
}

impl CachedSelectPlan {
    pub(crate) fn bind(&mut self, values: &[tidb_datatype::Datum]) -> Option<u64> {
        super::bind_prepared_statement_in_place(&mut self.statement, values).ok()?;
        // Rebuild through the current execute parameters, never the datum
        // cached when the marker-bearing expression was first planned.
        let parameters = tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values);
        let evaluator = |expression: &tidb_expr::expression::Expression| {
            tidb_expr::eval_expression_once(expression, &parameters)
        };
        let rebuilt = self.physical.rebuild_plan_for_cache_in_place(
            &tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values)
                .with_deferred_evaluator(&evaluator),
        );
        rebuilt.ok()?;
        self.generation = self.generation.wrapping_add(1);
        Some(self.generation)
    }

    pub(crate) fn execution_mut(
        &mut self,
        generation: u64,
    ) -> Option<(&tidb_ast::Stmt, &mut PhysicalPlan)> {
        (self.generation == generation).then_some((&self.statement, &mut self.physical))
    }
}

/// Builds the same logical and physical plan as ordinary execution, with
/// plan-cache-safe logical rewrites enabled so parameter markers are not
/// folded into a value-specific shape.
pub(crate) fn cached_query_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    cacheability: tidb_planner::physical_plan_cache::PlanCacheabilityContext,
) -> Option<(CachedSelectPlan, bool)> {
    let (physical, cacheable) =
        cached_physical_query_plan(query, catalog, current_database, ctx, cacheability)?;
    Some((
        CachedSelectPlan {
            statement: tidb_ast::Stmt::Query(tidb_ast::NodeBox::new(query.clone())),
            physical,
            generation: 0,
        },
        cacheable,
    ))
}

/// Builds and admits the physical source tree shared by cached queries and
/// cached DML roots.
pub(crate) fn cached_physical_query_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    cacheability: tidb_planner::physical_plan_cache::PlanCacheabilityContext,
) -> Option<(PhysicalPlan, bool)> {
    if matches!(query, tidb_ast::QueryStmt::Select(select) if select.rollup) {
        return None;
    }
    ctx.start_prepared_range_tracking();
    let (_, physical) = planner_physical_query(query, catalog, current_database, ctx, true).ok()?;
    if ctx.skip_plan_cache() {
        return Some((physical, false));
    }
    tidb_planner::physical_plan_cache::plan_cacheable(&physical, cacheability).ok()?;
    Some((physical, true))
}

#[cfg(test)]
pub(crate) fn statistics_usage_before_and_after_logical_optimization(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner::logical::rule_collect_plan_stats::ColumnStatsUsage,
        tidb_planner::logical::rule_collect_plan_stats::ColumnStatsUsage,
    ),
    tidb_planner::plan_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let source = catalog.planner_catalog(current_database, ctx.latest_index_schema());
    let session_zone = ctx.session_zone();
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, session_zone.clone());
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_partition_processor_enabled(ctx.static_partition_prune());
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
    builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
    builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
    let node = tidb_resolve::NodeW::new(query.clone());
    let plan = builder.build_query_node(&node, false)?;
    let flags = builder.get_opt_flag();
    let (plan, before) = tidb_planner::logical::rule_collect_plan_stats::collect_column_stats_usage(
        plan,
        ctx.opt_index_prune_threshold(),
    );
    let select_hint = match query {
        tidb_ast::QueryStmt::Select(select) => Some(select.as_ref()),
        tidb_ast::QueryStmt::SetOpr(_) => None,
    };
    let optimized = optimize_built_logical(
        plan,
        flags,
        select_hint,
        catalog,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        &session_zone,
    )?;
    let (_, after) = tidb_planner::logical::rule_collect_plan_stats::collect_column_stats_usage(
        optimized,
        ctx.opt_index_prune_threshold(),
    );
    Ok((before, after))
}

#[cfg(test)]
mod predicate_column_tests {
    use super::predicate_column_names;

    /// The loading model must read the source's OWN constant filters. A
    /// column=column join condition is not one: Go's `PushedDownConds` do not
    /// contain it, so the column stays evicted and borrows a loaded index's
    /// analyzed row count.
    #[test]
    fn filter_columns_load_and_join_keys_do_not() {
        let statement = tidb_parser::parse(
            "SELECT sum(h_amount) FROM history \
             WHERE h_c_w_id = 1 AND h_c_d_id = 5 AND h_c_id = c_id",
        )
        .unwrap();
        let tidb_ast::Stmt::Query(query) = &statement else {
            panic!("not a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };
        let names = predicate_column_names(select);
        let loaded = |name: &str| names.iter().any(|(_, candidate)| candidate == name);
        assert!(loaded("h_c_w_id") && loaded("h_c_d_id"), "{names:?}");
        assert!(!loaded("h_c_id"), "{names:?}");
    }
}
