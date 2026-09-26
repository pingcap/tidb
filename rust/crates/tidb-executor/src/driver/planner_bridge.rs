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
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::rc::Rc;

use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expr_util::normal_form::extract_filters_from_dnfs;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::ZonedNoResolver;
use tidb_expr::simple_expr::compose_dnf_condition;
use tidb_planner::cardinality::row_size::{RowSizeColumnStats, RowSizeType};
use tidb_planner::expression_rewriter::ColumnIdAllocator;
use tidb_planner::find_best_task::coster::Ver2Coster;
use tidb_planner::find_best_task::dispatch::{DispatchContext, find_best_task};
use tidb_planner::logical::cte::CteClass;
use tidb_planner::logical::fold::{Descend, OwnedRewrite, fold_owned};
use tidb_planner::logical::rule::{DisabledLogicalRules, RuleContext, flags, logical_optimize};
use tidb_planner::logical::{
    BaseLogicalPlan, LogicalPlan, LogicalSelection, prepare_possible_properties,
};
use tidb_planner::physical::PhysicalPlan;
use tidb_planner::physical_property::PhysicalProperty;
use tidb_planner::plan_base::PlanIdAllocator;
use tidb_planner::plan_builder::PlanBuilder;
use tidb_planner::plan_builder::catalog::TableSource;
use tidb_planner::stats_info::{HistColl, StatsInfo};

use super::FromTable;
use super::catalog::{Catalog, TableEntry};
use super::from::FromScope;

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

/// Go `TryFastPlan` runs before static partition processing, so a complete
/// unique GLOBAL-index point lookup can bypass the partition-union rewrite.
/// The ordinary Rust optimizer removes global-index paths when that rewrite
/// is enabled. Keep this bypass limited to a direct, single-table SELECT with
/// one complete integer-key equality/IN predicate; all other statements keep
/// the normal static-pruning path.
fn static_global_index_point_lookup(
    query: &tidb_ast::QueryStmt,
    catalog: &impl TableSource,
    ctx: &crate::StmtContext,
) -> bool {
    let tidb_ast::QueryStmt::Select(select) = query else {
        return false;
    };
    if select.kind != tidb_ast::SelectStatementKind::Select
        || select.is_in_braces
        || select.with.is_some()
        || select.distinct
        || select.calc_found_rows
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.lock.is_some()
        || !select.hints.is_empty()
        || select.into_outfile.is_some()
        || !select.into_vars.is_empty()
        || ctx.select_limit() != u64::MAX
        || ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false)
    {
        return false;
    }
    let Some(from) = &select.from else {
        return false;
    };
    if from.right.is_some() {
        return false;
    }
    let tidb_ast::JoinNode::Table(table_ref) = &from.left else {
        return false;
    };
    if !table_ref.partitions.is_empty() || table_ref.as_of.is_some() {
        return false;
    }
    let (db_name, table_name) = match table_ref.name.as_slice() {
        [table_name] => (catalog.current_database(), table_name.as_str()),
        [db_name, table_name] => (db_name.as_str(), table_name.as_str()),
        _ => return false,
    };
    let Some(table) = catalog.find_table(db_name, table_name) else {
        return false;
    };
    if !table.is_partitioned {
        return false;
    }
    if table.columns.is_empty()
        || table
            .columns
            .iter()
            .any(|column| !column.is_public || column.is_generated)
        || select.fields.fields().is_empty()
        || !select.fields.fields().iter().all(|field| match field {
            tidb_ast::SelectField::Wildcard(names) => {
                table_wildcard_matches(names, table_ref, db_name, table_name)
            }
            tidb_ast::SelectField::Expr {
                expr: tidb_ast::Expr::Column(names),
                alias: None,
            } => {
                table_column_matches(names, table_ref, db_name, table_name)
                    && names.last().is_some_and(|column_name| {
                        table
                            .columns
                            .iter()
                            .any(|column| column.name.eq_ignore_ascii_case(column_name))
                    })
            }
            _ => false,
        })
    {
        return false;
    }
    if table.partition_is_reorganizing {
        return false;
    }

    let Some(where_clause) = select.where_clause.as_ref() else {
        return false;
    };
    let (column_name, column_names, batch): (&str, &[String], bool) =
        match unparen_expression(where_clause) {
            tidb_ast::Expr::Binary(tidb_ast::BinaryOp::Eq, left, right) => {
                let left = unparen_expression(left);
                let right = unparen_expression(right);
                match (left, right) {
                    (tidb_ast::Expr::Column(names), value) if integer_literal(value) => {
                        let Some(column) = names.last() else {
                            return false;
                        };
                        (column, names, false)
                    }
                    (value, tidb_ast::Expr::Column(names)) if integer_literal(value) => {
                        let Some(column) = names.last() else {
                            return false;
                        };
                        (column, names, false)
                    }
                    _ => return false,
                }
            }
            tidb_ast::Expr::In {
                expr,
                list,
                not: false,
            } if !list.is_empty() => {
                let tidb_ast::Expr::Column(names) = unparen_expression(expr) else {
                    return false;
                };
                if !list
                    .iter()
                    .all(|value| integer_literal(unparen_expression(value)))
                {
                    return false;
                }
                let Some(column) = names.last() else {
                    return false;
                };
                (column, names, true)
            }
            _ => return false,
        };
    if !table_column_matches(column_names, table_ref, db_name, table_name) {
        return false;
    }

    if batch && !table.partition_expression_is_column {
        return false;
    }

    table.indexes.iter().any(|index| {
        if !index.global
            || !index.unique
            || !index.is_public
            || !index.is_visible
            || index.is_multi_valued
            || !index.condition_expr_string.is_empty()
            || index.columns.len() != 1
            || !index_hint_allows(&table_ref.hints, &index.name)
        {
            return false;
        }
        let key = &index.columns[0];
        key.length < 0
            && table.column_at(key.offset).is_some_and(|column| {
                column.name.eq_ignore_ascii_case(column_name)
                    && column.ret_type.code().is_type_integer()
            })
    })
}

fn table_column_matches(
    names: &[String],
    table_ref: &tidb_ast::TableRef,
    db_name: &str,
    table_name: &str,
) -> bool {
    let relation = table_ref.alias.as_deref().unwrap_or(table_name);
    match names {
        [_column] => true,
        [name, _column] => name.eq_ignore_ascii_case(relation),
        [schema, name, _column] => {
            schema.eq_ignore_ascii_case(db_name) && name.eq_ignore_ascii_case(relation)
        }
        _ => false,
    }
}

fn table_wildcard_matches(
    names: &[String],
    table_ref: &tidb_ast::TableRef,
    db_name: &str,
    table_name: &str,
) -> bool {
    let relation = table_ref.alias.as_deref().unwrap_or(table_name);
    match names {
        [] => true,
        [name] => name.eq_ignore_ascii_case(relation),
        [schema, name] => {
            schema.eq_ignore_ascii_case(db_name) && name.eq_ignore_ascii_case(relation)
        }
        _ => false,
    }
}

fn index_hint_allows(hints: &[tidb_ast::IndexHint], index_name: &str) -> bool {
    let mut is_ignore = false;
    let mut has_scan_hint = false;
    for hint in hints
        .iter()
        .filter(|hint| hint.scope == tidb_ast::IndexHintScope::All)
    {
        has_scan_hint = true;
        let matches_index = hint
            .indexes
            .iter()
            .any(|name| name.eq_ignore_ascii_case(index_name));
        match hint.kind {
            tidb_ast::IndexHintKind::Ignore => {
                is_ignore = true;
                if matches_index {
                    return false;
                }
            }
            tidb_ast::IndexHintKind::Use | tidb_ast::IndexHintKind::Force if matches_index => {
                return true;
            }
            tidb_ast::IndexHintKind::Use | tidb_ast::IndexHintKind::Force => {}
        }
    }
    !has_scan_hint || is_ignore
}

fn integer_literal(expression: &tidb_ast::Expr) -> bool {
    matches!(expression, tidb_ast::Expr::Int(value) if value.parse::<i64>().is_ok())
}

fn unparen_expression(mut expression: &tidb_ast::Expr) -> &tidb_ast::Expr {
    while let tidb_ast::Expr::Paren(inner) = expression {
        expression = inner;
    }
    expression
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

impl tidb_planner::find_best_task::dispatch::MppWarningSink for crate::StmtContext {
    fn raise_mpp_warning(&self, message: &str) {
        crate::StmtContext::append_mpp_warning(self, message);
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
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
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

/// Go `StmtCtx.SetIndexForce()`'s statement-wide reach (`stats.go:165` →
/// `plan_cost_ver2.go:1234`): every risky full table scan of the statement
/// pays the penalty, hinted or not.
fn stamp_statement_index_force(plan: LogicalPlan, forced: bool) -> LogicalPlan {
    let mut visitor = StampIndexForce { forced };
    fold_owned(&mut visitor, plan, ()).0
}

/// The stamping visitor behind [`stamp_statement_index_force`].
struct StampIndexForce {
    forced: bool,
}

impl OwnedRewrite for StampIndexForce {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): Self::Down) -> Descend<Self::Down, Self::Up> {
        if let LogicalPlan::DataSource(source) = node {
            source.table_scan_penalty.has_index_force = self.forced;
        }
        Descend::Children(vec![(); node.children().len()])
    }

    fn ascend(&mut self, node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
        (node, ())
    }
}

struct InitStats<'a> {
    error: Option<tidb_planner::plan_base::PlanError>,
    range_context: crate::index_range::RangeContext<'a>,
    catalog: &'a Catalog,
    select: Option<&'a tidb_ast::SelectStmt>,
    default_string_match_selectivity: f64,
    selectivity_factor: f64,
    enable_pseudo_for_outdated_stats: bool,
    context: &'a crate::StmtContext,
}

fn record_used_item_stats_status(
    context: &crate::StmtContext,
    table_name: &str,
    physical_table_id: i64,
    statistics: Option<&crate::access_cost::TableStatistics>,
    item_id: i64,
    is_index: bool,
) {
    let Some(statistics) = statistics else {
        return;
    };
    let (loaded_status, analyzed) = if is_index {
        (
            statistics.index_load_status.get(&item_id).copied(),
            statistics
                .index_stats_existence
                .get(&item_id)
                .copied()
                .unwrap_or(false),
        )
    } else {
        (
            statistics.column_load_status.get(&item_id).copied(),
            statistics
                .column_stats_existence
                .get(&item_id)
                .copied()
                .unwrap_or(false),
        )
    };
    if loaded_status.is_some_and(tidb_stats::StatsLoadedStatus::is_full_load) {
        return;
    }
    let status = loaded_status.map_or_else(
        || {
            if analyzed { "unInitialized" } else { "missing" }
        },
        tidb_stats::StatsLoadedStatus::status_to_string,
    );
    context.record_used_stats_status(
        physical_table_id,
        table_name,
        if statistics.pseudo {
            0
        } else {
            statistics.version
        },
        statistics.row_count,
        statistics.modify_count,
        item_id,
        is_index,
        status,
    );
}

/// Go `DataSource.HandleColsToAppend`: the handle suffix physically present
/// after a non-unique index's declared key parts, in key order and with the
/// range lengths used by `fillIndexPath`.
fn handle_columns_to_append(
    source: &tidb_planner::logical::DataSource,
    source_index: &tidb_planner::plan_builder::catalog::SourceIndex,
    index: &crate::kv_table::KvIndex,
    table: &crate::kv_table::KvTable,
) -> Vec<(usize, i64)> {
    if source_index.unique
        || source_index.primary
        || index.clustered_primary
        || source_index.columns.len() != index.column_offsets.len()
        || source_index
            .columns
            .iter()
            .zip(&index.column_offsets)
            .any(|(declared, offset)| declared.offset != *offset)
    {
        return Vec::new();
    }

    let declared = source_index
        .columns
        .iter()
        .map_while(|key| {
            source
                .schema_column_for_index_column(key)
                .cloned()
                .map(|column| (column, key.length))
        })
        .collect::<Vec<_>>();
    let appended = source.handle_cols_to_append(source_index, &declared);
    let expected_offsets = if source.is_common_handle {
        table.common_handle_offsets().to_vec()
    } else {
        table.pk_handle_offset().into_iter().collect()
    };
    if appended.len() != expected_offsets.len() || (source.is_common_handle && index.global) {
        return Vec::new();
    }
    appended
        .into_iter()
        .zip(expected_offsets)
        .map(|((column, length), offset)| {
            table
                .columns
                .get(offset)
                .filter(|stored| stored.id == column.id)
                .map(|_| (offset, length))
        })
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default()
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
    if lhs.func_name.lowercase() == "not" {
        // The same logical NOT can be assigned different integer result
        // widths by the AST and pushed-expression builders (LongLong vs Tiny).
        // It is still the same predicate when its operand is identical.
        return lhs.args.len() == rhs.args.len()
            && lhs
                .args
                .iter()
                .zip(&rhs.args)
                .all(|(left, right)| same_statistics_predicate(left, right));
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

/// Go `getGeneralAttributesFromPaths`: minimum access-path selectivity used
/// by `GetOriginalPhysicalIndexScan` to decide whether LIMIT row-count
/// adjustment is safe. `Some(index_filter_selectivity)` identifies a path
/// with index filters, whose `CountAfterIndex` is floored at datasource rows.
fn min_access_path_selectivity(
    counts_after_access: impl IntoIterator<Item = (f64, Option<f64>)>,
    datasource_rows: f64,
    total_rows: f64,
) -> f64 {
    if total_rows <= 0.0 {
        return 1.0;
    }
    counts_after_access
        .into_iter()
        .map(|(count_after_access, index_filter_selectivity)| {
            let count_after_index = index_filter_selectivity
                .map_or(count_after_access, |selectivity| {
                    (count_after_access * selectivity).max(datasource_rows)
                });
            count_after_index / total_rows
        })
        .fold(1.0_f64, f64::min)
}

#[cfg(test)]
mod access_path_min_selectivity_tests {
    use super::min_access_path_selectivity;

    #[test]
    fn minimum_uses_count_after_index_and_the_datasource_row_floor() {
        let paths = [(80.0, None), (100.0, Some(0.1)), (60.0, Some(0.9))];
        // Table path: 80/1000. Index path with index filters: max(10, 50)/1000.
        // The final index path uses max(54, 50)/1000.
        assert_eq!(min_access_path_selectivity(paths, 50.0, 1000.0), 0.05);
        assert_eq!(min_access_path_selectivity([], 50.0, 0.0), 1.0);
    }
}

#[cfg(test)]
mod same_statistics_predicate_tests {
    use super::same_statistics_predicate;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;
    use tidb_expr::scalar_function::ScalarFunction;

    fn not_regexp(
        not_type: FieldTypeCode,
        regexp_type: FieldTypeCode,
        column_id: i64,
    ) -> Expression {
        let column = Expression::Column(Column::new(
            column_id,
            FieldType::new(FieldTypeCode::VarString),
        ));
        let regexp = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("regexp"),
            FieldType::new(regexp_type),
            vec![column],
        ));
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("not"),
            FieldType::new(not_type),
            vec![regexp],
        ))
    }

    #[test]
    fn not_predicate_identity_ignores_only_boolean_result_width() {
        let source = not_regexp(FieldTypeCode::LongLong, FieldTypeCode::LongLong, 1);
        let pushed = not_regexp(FieldTypeCode::Tiny, FieldTypeCode::LongLong, 1);
        assert!(same_statistics_predicate(&source, &pushed));

        let different_column = not_regexp(FieldTypeCode::Tiny, FieldTypeCode::LongLong, 2);
        assert!(!same_statistics_predicate(&source, &different_column));

        let different_operand_type = not_regexp(FieldTypeCode::Tiny, FieldTypeCode::Tiny, 1);
        assert!(!same_statistics_predicate(&source, &different_operand_type));
    }
}

impl InitStats<'_> {
    /// The `(column ids, index ids)` Go's lite statistics initialization
    /// loads for this source: the columns its own predicates compare against
    /// a constant, plus the indexes whose first column those cover.
    ///
    /// `None` when the statement carries no such predicate for this source,
    /// which leaves the caller on the cache's fully loaded subset.
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
        // Payload shape is not load state: fully loaded TopN-only statistics
        // have no buckets, while evicted statistics retain metadata.
        // Use the same retained load status as the statistics cache.
        columns.retain(|id| {
            statistics
                .and_then(|statistics| statistics.column_load_status.get(id))
                .is_some_and(|status| status.is_full_load())
        });
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
                let loaded = statistics
                    .and_then(|statistics| statistics.index_load_status.get(&index.id))
                    .is_some_and(|status| status.is_full_load());
                if loaded && first.is_some_and(|column| columns.contains(&column.id)) {
                    indexes.insert(index.id);
                }
            }
        }
        Some((columns, indexes))
    }
}

impl InitStats<'_> {
    fn initialize_source(
        &mut self,
        node: &mut LogicalPlan,
    ) -> Result<Descend<(), ()>, tidb_planner::plan_base::PlanError> {
        let LogicalPlan::DataSource(source) = node else {
            return Ok(Descend::Children(vec![(); node.children().len()]));
        };
        source.base.base.set_stats(None);
        source.table_path_count_after_access = None;
        source.derived_access_paths = None;
        source.derived_index_paths.retain(|_, path| {
            path.row_estimate = None;
            path.filled = None;
            path.is_single_scan = None;
            path.declared_columns.is_some()
        });
        source.access_path_min_selectivity = 1.0;
        // Go `initStats` calls `GetStatsTable(..., ds.PhysicalTableID)`: a
        // static-pruning child owns one physical partition's statistics,
        // while an ordinary/dynamic source keeps the logical table ID here.
        let stored_statistics = self.catalog.table_statistics(source.physical_table_id);
        // Go `GetStatsTable` copies the cached table for session-local
        // objective and outdated-statistics decisions. Neither switch may
        // mutate the shared cache used by other sessions.
        let allow_use_modify_count = self
            .context
            .optimizer_cost_env()
            .session
            .estimator_options
            .allow_use_modify_count;
        let statistics = stored_statistics.as_deref().map(|statistics| {
            let analyze_count = statistics.analyze_row_count().max(0.0) as i64;
            let ignore_realtime_stats = !allow_use_modify_count
                && (statistics.row_count != analyze_count || statistics.modify_count != 0);
            let outdated = self.enable_pseudo_for_outdated_stats && statistics.is_outdated();
            if ignore_realtime_stats || outdated {
                let mut copied = statistics.clone();
                if ignore_realtime_stats {
                    copied.row_count = analyze_count;
                    copied.modify_count = 0;
                    // Go replaces a zero-count stats table with PseudoTable;
                    // keep an unanalyzed/missing histogram from becoming a
                    // zero-row estimate in determinate mode.
                    if analyze_count == 0 {
                        copied.pseudo = true;
                    }
                }
                copied.pseudo |= outdated;
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
        // Go's `fillIndexPath` initializes the table path's CountAfterAccess
        // from the datasource row count even when there are no access
        // conditions. A recognized handle range below replaces this value.
        source.table_path_count_after_access = Some(row_count);
        // Retain Go's loaded subset, including empty and TopN-only payloads.
        // The cache's explicit status excludes evicted metadata from NDV
        // scaling and histogram estimates without inferring state from buckets.
        let (loaded_columns, loaded_indexes) = self
            .predicate_loaded_items(source, statistics)
            .unwrap_or_else(|| {
                (
                    statistics
                        .map(|statistics| {
                            statistics
                                .columns
                                .iter()
                                .filter(|(id, _)| {
                                    statistics.column_load_status.get(id)
                                        .is_some_and(|status| status.is_full_load())
                                })
                                .map(|(id, _)| *id)
                                .collect()
                        })
                        .unwrap_or_default(),
                    statistics
                        .map(|statistics| {
                            statistics
                                .indexes
                                .iter()
                                .filter(|(id, _)| {
                                    statistics.index_load_status.get(id)
                                        .is_some_and(|status| status.is_full_load())
                                })
                                .map(|(id, _)| *id)
                                .collect()
                        })
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
        let column_load_status = source.table_columns.iter().map(|column| {
            let status = statistics
                .and_then(|statistics| statistics.column_load_status.get(&column.id))
                .copied()
                .unwrap_or_default();
            (column.unique_id, status)
        }).collect::<Vec<_>>();
        // GenerateHistCollFromColumnInfo builds every index map from retained
        // payloads, including invalid/evicted ones. A schema-only index must
        // not become the first V1 suffix candidate, and a missing mapped
        // column terminates the usable prefix rather than skipping a hole.
        let index_columns = source
            .indexes
            .iter()
            .filter_map(|index| {
                statistics?.indexes.get(&index.id)?;
                let declared = source.declared_index_columns(index)
                    .into_iter().map_while(|column| column).collect::<Vec<_>>();
                let mut columns = declared.iter().map(|(column, _)| column.unique_id)
                    .collect::<Vec<_>>();
                // This snapshot is consumed after path preparation. Mirror
                // fillIndexPath's extension of the initial retained-index map.
                if columns.len() == index.columns.len() {
                    columns.extend(source.handle_cols_to_append(index, &declared)
                        .into_iter().map(|(column, _)| column.unique_id));
                }
                (!columns.is_empty()).then_some((index.id, columns))
            })
            .collect::<Vec<_>>();
        let index_histograms = index_columns.iter().filter_map(|(id, _)| {
            Some((*id, std::sync::Arc::new(statistics?.indexes.get(id)?.clone())))
        }).collect::<Vec<_>>();
        let column_index_ids = index_columns.iter().map(|(id, columns)| (columns[0], *id)).collect::<Vec<_>>();
        let index_row_counts = source
            .indexes
            .iter()
            .filter_map(|index| {
                let statistics = statistics?;
                statistics.indexes.get(&index.id)?;
                source.schema_column_for_index_column(index.columns.first()?)?;
                let mut row_counts =
                    tidb_planner::cardinality::row_count_estimator::IndexRowCounts::unscaled(
                        statistics.row_count,
                        statistics.modify_count,
                    );
                let index_is_fully_loaded = statistics
                    .index_load_status
                    .get(&index.id)
                    .is_some_and(|status| status.is_full_load());
                if index.is_multi_valued && index_is_fully_loaded {
                    let analyzed_row_count = statistics
                        .columns
                        .iter()
                        .find_map(|(column_id, column)| {
                            statistics
                                .column_load_status
                                .get(column_id)
                                .is_some_and(|status| status.is_full_load())
                                .then(|| column.total_row_count())
                        })
                        .or_else(|| {
                            statistics
                                .indexes
                                .iter()
                                .find_map(|(candidate_id, candidate)| {
                                    let candidate_is_multi_valued = source
                                        .indexes
                                        .iter()
                                        .find(|source_index| source_index.id == *candidate_id)
                                        .is_some_and(|source_index| source_index.is_multi_valued);
                                    (!candidate_is_multi_valued
                                        && statistics
                                            .index_load_status
                                            .get(candidate_id)
                                            .is_some_and(|status| status.is_full_load()))
                                    .then(|| candidate.total_row_count())
                                })
                        })
                        .unwrap_or(-1.0);
                    let index_total_count = statistics.indexes.get(&index.id).map_or(
                        0.0,
                        tidb_planner::cardinality::row_count_estimator::IndexStats::total_row_count,
                    );
                    if analyzed_row_count > 0.0 && index_total_count > 0.0 {
                        let scale = index_total_count / analyzed_row_count;
                        row_counts.index_realtime = (statistics.row_count as f64 * scale) as i64;
                        row_counts.index_modify = (statistics.modify_count as f64 * scale) as i64;
                    }
                }
                Some((index.id, row_counts))
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
        let initialized_column_ndvs = source
            .table_columns
            .iter()
            .filter_map(|column| {
                let statistics = statistics?;
                let status = statistics.column_load_status.get(&column.id)?;
                status
                    .stats_initialized()
                    .then(|| {
                        statistics
                            .columns
                            .get(&column.id)
                            .map(|column_stats| (column.unique_id, column_stats.histogram.ndv))
                    })
                    .flatten()
            })
            .collect::<Vec<_>>();
        let initialized_index_ndvs = source
            .indexes
            .iter()
            .filter_map(|index| {
                let statistics = statistics?;
                if !statistics
                    .index_load_status
                    .get(&index.id)?
                    .stats_initialized()
                {
                    return None;
                }
                let ndv = statistics.indexes.get(&index.id)?.histogram.ndv;
                let columns = index
                    .columns
                    .iter()
                    .map(|column| {
                        source
                            .schema_column_for_index_column(column)
                            .map(|column| column.unique_id)
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some((index.id, (columns, ndv)))
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
                    .with_column_load_status(column_load_status)
                    .with_index_histograms(index_histograms)
                    .with_column_index_ids(column_index_ids)
                    .with_index_columns(index_columns)
                    .with_index_row_counts(index_row_counts)
                    .with_index_range_policies(source.indexes.iter().map(|index| (index.id,
                        tidb_planner::cardinality::index_range_policy::IndexRangePolicy {
                            has_condition: !index.condition_expr_string.is_empty(),
                            is_multi_value: index.is_multi_valued,
                        })))
                    .with_modify_count(statistics.map_or(0, |statistics| statistics.modify_count))
                    .with_pk_is_handle(source.handle_is_int)
                    .with_index_ndvs(index_ndvs)
                    .with_initialized_ndvs(initialized_column_ndvs, initialized_index_ndvs),
                )
                .with_stats_version(statistics.map_or(tidb_stats::PSEUDO_VERSION, |statistics| {
                    if statistics.pseudo || statistics.stats_ver <= 0 {
                        tidb_stats::PSEUDO_VERSION
                    } else {
                        statistics.stats_ver as u64
                    }
                })),
        );
        self.derive_source_access_statistics(source, statistics, row_count)?;
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
        // Go `deriveStats`' `StmtCtx.SetIndexForce()` (`stats.go:165`): the
        // flag is statement-wide, so a hint on ANY occurrence records it here
        // and the pass below stamps every source once the walk completes.
        if !source.forced_index_ids.is_empty() {
            self.context.set_index_force();
        }
        tidb_planner::logical::rule_collect_plan_stats::refresh_source_group_ndvs_with_scale_ratio(
            source, self.context.optimizer_cost_env().session.scale_ndv_skew_ratio,
        );
        Ok(Descend::Stop(()))
    }

    // Transitional boundary: snapshot attachment and derived access state have
    // different lifetimes. This body still contains the legacy AST route and
    // must migrate together with logical path derivation, not be reused as a
    // second authoritative candidate builder.
    fn derive_source_access_statistics(
        &self,
        source: &mut tidb_planner::logical::DataSource,
        statistics: Option<&crate::access_cost::TableStatistics>,
        row_count: f64,
    ) -> Result<(), tidb_planner::plan_base::PlanError> {
        let estimation_error =
            |error: tidb_planner::cardinality::row_count_estimator::EstimationError| {
                tidb_planner::plan_base::PlanError::internal_coded(error.to_string())
            };
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
            // Go Selectivity consumes built expressions. This AST replay
            // must retain session inputs without replaying build warnings.
            let rewrite_context = self.context.for_statistics_rewrite();
            let mut scope = FromScope::for_statement(&rewrite_context);
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
                    .transpose()
                    .map_err(estimation_error)?
                    .or(Some(row_count));
            let mut index_path_filter_selectivities = BTreeMap::new();
            let simple_expr_schema = source.base.base.schema().cloned();
            let mut filled_path_appended_handle_columns = BTreeMap::new();
            let simple_expr_names = source
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
                .collect::<Vec<_>>();
            for index in table.plan_indexes() {
                let source_index = source
                    .indexes
                    .iter()
                    .find(|candidate| candidate.id == index.id);
                let appended_handle_columns = source_index.map_or_else(Vec::new, |source_index| {
                    handle_columns_to_append(source, source_index, index, table)
                });
                filled_path_appended_handle_columns
                    .insert(index.id, appended_handle_columns.clone());
                let mut columns = index
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
                let Some(mut columns) = columns.take() else {
                    continue;
                };
                for (offset, prefix_len) in &appended_handle_columns {
                    let Some(column) = table.columns.get(*offset) else {
                        columns.clear();
                        break;
                    };
                    columns.push(crate::index_range::RangeColumn {
                        name: column.name.clone(),
                        field_type: column.field_type.clone(),
                        // Datasource statistics use Go's already-filled access path,
                        // so appended handle dimensions keep their physical prefixes.
                        prefix_len: *prefix_len,
                    });
                }
                if columns.is_empty() {
                    continue;
                }
                let Some(built) = crate::index_range::detach_cond_and_build_range_for_index(
                    &columns, predicate, &resolver,
                ) else {
                    continue;
                };
                let index_filter_conjuncts = if let (Some(source_index), Some(schema)) =
                    (source_index, simple_expr_schema.as_ref())
                {
                    let options = tidb_expr::simple_expr::BuildOptions::new()
                        .with_input_schema_and_names(schema.clone(), simple_expr_names.clone());
                    let mut index_filters = Vec::new();
                    for condition in &built.residual {
                        let expression = tidb_expr::simple_expr::build_simple_expr(
                            &resolver, condition, &options,
                        )
                        .map_err(|error| {
                            tidb_planner::plan_base::PlanError::internal_coded(error.to_string())
                        })?;
                        if tidb_planner::logical::data_source::index_covers_condition(
                            source,
                            source_index,
                            &expression,
                            self.context.opt_prefix_index_single_scan(),
                        ) {
                            index_filters.push(*condition);
                        }
                    }
                    index_filters
                } else {
                    Vec::new()
                };
                let index_filter_selectivity = if index_filter_conjuncts.is_empty() {
                    1.0
                } else {
                    let mut observe = |item_id, is_index| {
                        record_used_item_stats_status(
                            self.context,
                            &source.table_name,
                            source.physical_table_id,
                            statistics,
                            item_id,
                            is_index,
                        );
                    };
                    crate::access_cost::selectivity_of_conjuncts_with_range_context_observing(
                        &index_filter_conjuncts,
                        table,
                        &resolver,
                        statistics,
                        tidb_planner::selectivity_greedy::SelectivityDefaults {
                            trigger_load: false,
                            estimator_options: self
                                .context
                                .optimizer_cost_env()
                                .session
                                .estimator_options,
                            ..tidb_planner::selectivity_greedy::SelectivityDefaults::from_session(
                                self.default_string_match_selectivity,
                                self.selectivity_factor,
                            )
                        },
                        self.range_context,
                        &mut observe,
                    )
                };
                record_used_item_stats_status(
                    self.context,
                    &source.table_name,
                    source.physical_table_id,
                    statistics,
                    index.id,
                    true,
                );
                let estimate = (if appended_handle_columns.is_empty() {
                    crate::access_cost::index_row_count(
                        index,
                        table,
                        &built.ranges,
                        statistics,
                        row_count,
                        false,
                    )
                } else {
                    let ranges = built
                        .ranges
                        .iter()
                        .map(|range| tidb_planner::ranger::types::Range {
                            low_val: range.low.clone(),
                            high_val: range.high.clone(),
                            collators: columns
                                .iter()
                                .take(range.low.len())
                                .map(|column| column.field_type.collation())
                                .collect(),
                            low_exclude: range.low_exclusive,
                            high_exclude: range.high_exclusive,
                        })
                        .collect::<Vec<_>>();
                    crate::access_cost::index_row_count_with_appended_handle_columns(
                        index,
                        table,
                        &ranges,
                        &appended_handle_columns
                            .iter()
                            .map(|(offset, _)| *offset)
                            .collect::<Vec<_>>(),
                        statistics,
                        row_count,
                        false,
                    )
                })
                .map_err(estimation_error)?;
                for (offset, _) in &appended_handle_columns {
                    if let Some(column) = table.columns.get(*offset) {
                        record_used_item_stats_status(
                            self.context,
                            &source.table_name,
                            source.physical_table_id,
                            statistics,
                            column.id,
                            false,
                        );
                    }
                }
                source
                    .derived_index_paths
                    .entry(index.id)
                    .or_default()
                    .row_estimate = Some(estimate);
                index_path_filter_selectivities.insert(
                    index.id,
                    (!index_filter_conjuncts.is_empty(), index_filter_selectivity),
                );
            }
            let selectivity = {
                let mut observe = |item_id, is_index| {
                    record_used_item_stats_status(
                        self.context,
                        &source.table_name,
                        source.physical_table_id,
                        statistics,
                        item_id,
                        is_index,
                    );
                };
                crate::access_cost::selectivity_with_filled_path_context_observing(
                    predicate,
                    table,
                    &resolver,
                    statistics,
                    tidb_planner::selectivity_greedy::SelectivityDefaults {
                        trigger_load: false,
                        estimator_options: self
                            .context
                            .optimizer_cost_env()
                            .session
                            .estimator_options,
                        ..tidb_planner::selectivity_greedy::SelectivityDefaults::from_session(
                            self.default_string_match_selectivity,
                            self.selectivity_factor,
                        )
                    },
                    self.range_context,
                    &filled_path_appended_handle_columns,
                    &mut observe,
                )
            };
            let cached_predicate_matches = source.base.base.schema().is_some_and(|schema| {
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
                let filtered_stats = table_stats.scale(
                    selectivity,
                    self.context.optimizer_cost_env().session.scale_ndv_skew_ratio,
                );
                let total_rows = row_count as f64;
                let path_counts = source
                    .enumerated_paths
                    .iter()
                    .filter_map(|path| match path {
                        tidb_planner::access_path::PossiblePath::Table { .. }
                        | tidb_planner::access_path::PossiblePath::TiFlashTable => source
                            .table_path_count_after_access
                            .map(|count_after_access| (count_after_access, None)),
                        tidb_planner::access_path::PossiblePath::Index { index } => {
                            source.indexes.get(*index).and_then(|source_index| {
                                source
                                    .derived_index_paths
                                    .get(&source_index.id)
                                    .and_then(|path| path.count_after_access())
                                    .map(|count_after_access| {
                                        let (has_index_filters, index_filter_selectivity) =
                                            index_path_filter_selectivities
                                                .get(&source_index.id)
                                                .copied()
                                                .unwrap_or((false, 1.0));
                                        (
                                            count_after_access,
                                            has_index_filters.then_some(index_filter_selectivity),
                                        )
                                    })
                            })
                        }
                    });
                source.access_path_min_selectivity = min_access_path_selectivity(
                    path_counts,
                    filtered_stats.row_count(),
                    total_rows,
                );
                source.base.base.set_stats(Some(filtered_stats));
            }
        }
        // Propagated predicates may not occur in this source's AST WHERE.
        // Rebuild path estimates from the optimizer's current expressions.
        if !source.pushed_down_conds.is_empty() {
            if let Some(TableEntry::Kv(table)) = source_table {
                for index in &source.indexes {
                    let Some(metadata) = table
                        .plan_indexes()
                        .find(|candidate| candidate.id == index.id)
                    else {
                        continue;
                    };
                    let appended_handle_columns =
                        handle_columns_to_append(source, index, metadata, table);
                    let prefix = source.index_range_columns(index);
                    if prefix.is_empty() {
                        continue;
                    }
                    let (columns, lengths): (Vec<_>, Vec<_>) = prefix.into_iter().unzip();
                    let evaluate = |expression: &tidb_expr::expression::Expression| {
                        tidb_expr::eval_expression_once(expression, self.context)
                    };
                    let built =
                        tidb_planner::ranger::detacher::detach_index_range_with_fallback_handler_in(
                            &source.pushed_down_conds,
                            &columns,
                            &lengths,
                            self.context.range_max_size(),
                            self.context.range_fallback_handler(),
                            &evaluate,
                        ).map_err(|error| {
                            use tidb_planner::ranger::points::PointBuilderError;
                            match error {
                                PointBuilderError::Eval(error) => error.into(),
                                PointBuilderError::Value(error) =>
                                    tidb_planner::plan_base::PlanError::internal_coded(error.to_string()),
                                PointBuilderError::Unsupported(message) =>
                                    tidb_planner::plan_base::PlanError::unsupported_type(message),
                            }
                        })?;
                    let estimate = if appended_handle_columns.is_empty() {
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
                        crate::access_cost::index_row_count(
                            metadata, table, &ranges, statistics, row_count, false,
                        )
                    } else {
                        crate::access_cost::index_row_count_with_appended_handle_columns(
                            metadata,
                            table,
                            &built.ranges,
                            &appended_handle_columns
                                .iter()
                                .map(|(offset, _)| *offset)
                                .collect::<Vec<_>>(),
                            statistics,
                            row_count,
                            false,
                        )
                    }
                    .map_err(estimation_error)?;
                    source
                        .derived_index_paths
                        .entry(index.id)
                        .or_default()
                        .row_estimate = Some(estimate);
                    if index.primary && source.is_common_handle {
                        source.table_path_count_after_access = Some(estimate.est);
                    }
                }
            }
        }
        Ok(())
    }
}

impl OwnedRewrite for InitStats<'_> {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): Self::Down) -> Descend<Self::Down, Self::Up> {
        if self.error.is_some() {
            return Descend::Stop(());
        }
        let result = self.initialize_source(node);
        match result {
            Ok(descend) => descend,
            Err(error) => {
                self.error = Some(error);
                Descend::Stop(())
            }
        }
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
    let optimizer_cost_env = ctx.optimizer_cost_env();
    let coster = Ver2Coster::from_env(optimizer_cost_env);
    let evaluate = |expression: &tidb_expr::expression::Expression| {
        tidb_expr::eval_expression_once(expression, ctx)
    };
    let mut dispatch = DispatchContext::new(plan_ids, &coster, optimizer_cost_env.session.scale_ndv_skew_ratio)
        .with_estimator_options(optimizer_cost_env.session.estimator_options)
        .with_group_ndv_skew_ratio(optimizer_cost_env.session.group_ndv_skew_ratio)
        .with_correlation_options(optimizer_cost_env.session.correlation_options)
        .with_expression_evaluator(&evaluate)
        .with_mpp_allowed(ctx.optimizer_cost_env().session.mpp_allowed)
        .with_enable_skew_distinct_agg(ctx.enable_skew_distinct_agg())
        .with_enable_3_stage_distinct_agg(ctx.enable_3_stage_distinct_agg())
        .with_enable_3_stage_multi_distinct_agg(ctx.enable_3_stage_multi_distinct_agg())
        .with_tiflash_pre_agg_mode(ctx.tiflash_pre_agg_mode())
        .with_partial_ordered_index_for_topn(ctx.partial_ordered_index_for_topn())
        .with_opt_prefix_index_single_scan(ctx.opt_prefix_index_single_scan())
        .with_mpp_warning_sink(ctx)
        .with_range_quota(ctx.range_max_size(), ctx.range_fallback_handler())
        .with_selectivity_factor(ctx.selectivity_factor())
        .with_ordering_index_selectivity_ratio(ctx.ordering_index_selectivity_ratio())
        .with_ordering_index_selectivity_threshold(ctx.ordering_index_selectivity_threshold())
        .with_projection_push_down(ctx.allow_projection_push_down())
        .with_heavy_function_optimize(
            ctx.optimizer_fix_control()
                .get_bool_with_default(tidb_planner::fix_control::FIX_56318, true),
        )
        .with_inl_join_inner_multi_pattern(ctx.enable_inl_join_inner_multi_pattern())
        .with_limit_push_down_threshold(ctx.limit_push_down_threshold())
        .with_paging(ctx.optimizer_cost_env().session.enable_paging)
        .with_hash_join_concurrency(
            ctx.optimizer_cost_env()
                .session
                .hash_join_concurrency
                .max(1.0) as usize,
        )
        .with_use_hash_join_v2(ctx.optimizer_cost_env().session.use_hash_join_v2)
        .with_apply_cache_capacity(ctx.apply_cache_capacity())
        .with_point_get_conversion(
            !ctx.optimizer_fix_control()
                .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false),
        )
        .with_index_join_probe_row_count_fix(
            ctx.optimizer_fix_control()
                .get_bool_with_default(tidb_planner::fix_control::FIX_44855, true),
        )
        .with_index_join_row_count_upper_bound(
            ctx.optimizer_fix_control()
                .get_bool_with_default(tidb_planner::fix_control::FIX_44855, false),
        )
        .with_column_ids(column_ids);
    dispatch.expr_pushdown_blacklist = ctx.expr_pushdown_blacklist().clone();
    dispatch.shuffle_options = ctx.optimizer_cost_env().session.shuffle_options;
    let task = find_best_task(logical, &PhysicalProperty::default(), &mut dispatch)?;
    let mut physical = task.plan().cloned().ok_or_else(|| {
        tidb_planner::plan_base::PlanError::internal_coded(
            "Can't find a proper physical plan for this query",
        )
    })?;
    // Go physicalOptimize binds positions before postOptimize removes aliases.
    physical.resolve_indices()?;
    let physical = tidb_planner::physical::eliminate_physical_projection(physical);
    // Go postOptimize: eliminatePhysicalProjection → InjectExtraProjection.
    // The projection re-injection restores the purposeful projections
    // (scalar aggregate arguments, scalar order-by items, expression
    // nominal sorts) that the elimination pass removed.
    let mut physical =
        tidb_planner::physical::inject_extra_projection(physical, plan_ids, column_ids);
    tidb_planner::physical::shuffle::install_receivers(&mut physical, plan_ids)?;
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
    // Go DataSource.DeriveStats initializes base statistics even when called
    // from a logical rule. CTE roots need the same preinitialization as the
    // outer query before join reorder can inspect their sources.
    let mut initializer = InitStats {
        error: None,
        range_context: crate::index_range::RangeContext {
            max_size: ctx.range_max_size(),
            fallback_handler: Some(ctx.range_fallback_handler()),
            eval_ctx: Some(ctx),
        },
        catalog,
        select: None,
        default_string_match_selectivity: ctx.default_string_match_selectivity(),
        selectivity_factor: ctx.selectivity_factor(),
        enable_pseudo_for_outdated_stats: ctx.enable_pseudo_for_outdated_stats(),
        context: ctx,
    };
    let (plan, ()) = fold_owned(&mut initializer, plan, ());
    if let Some(error) = initializer.error {
        return Err(error);
    }
    let optimized = logical_optimize(rule_context, opt_flag, plan)
        .map_err(|(_, error)| error)?
        .plan;
    let mut optimized = check_partial_index_paths(optimized, ctx, rule_context.use_plan_cache);
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
            if class.push_down_predicates.len() == class.optimized_predicate_count {
                if let (Some(seed_stat), Some(stats)) = (seed_stat, physical.stats_info()) {
                    *seed_stat.borrow_mut() = stats.clone();
                }
                return Ok(());
            }
            // The seed was optimised before the rule list with the predicates
            // recorded SO FAR. `LogicalCTE.PredicatePushDown` appended more
            // since (GO records them in the logical phase and only then runs
            // `DeriveStats`), so fall through and rebuild the seed with the
            // DNF-extracted Selection below.
        }
    }
    if !visiting.insert(identity) {
        return Err(tidb_planner::plan_base::PlanError::internal(
            "LogicalCTE.DeriveStats: cyclic CTE class optimization",
        ));
    }

    let result = (|| {
        let (mut seed, recursive, mut opt_flag, pushed_predicates, re_optimize) = {
            let class = class.borrow();
            // When the predicate set grew after the eager first pass, the
            // seed is RE-optimised below with ONLY the predicate pushdown
            // (plus stats re-derivation and the physical build): the recorded
            // conditions ride the SAME absorption path pass 1 used, so their
            // selectivities land correctly, and re-running the full rule list
            // is neither needed nor safe on an already-optimised tree.
            let re_optimize = class.seed_part_physical_plan.is_some()
                && class.push_down_predicates.len() != class.optimized_predicate_count;
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
                re_optimize,
            )
        };

        // Go composes the predicates recorded by every reference as one DNF,
        // extracts common conjuncts, and puts that Selection above the seed
        // before running the CTE's own optimizer pass.
        let pushed_count = pushed_predicates.len();
        if let Some(dnf) = compose_dnf_condition(pushed_predicates) {
            let conditions = extract_filters_from_dnfs(vec![dnf]);
            if std::env::var("TIDB_DEBUG_NDV").is_ok() {
                eprintln!("CTEDNF count={:?}", conditions.len());
                for condition in &conditions {
                    eprintln!("CTEDNF cond={:?}", condition);
                }
            }
            let query_block_offset = seed.base().base.query_block_offset();
            let mut selection = LogicalSelection::new(
                BaseLogicalPlan::new(plan_ids, LogicalSelection::TYPE, query_block_offset),
                conditions,
            );
            selection.base.set_children(vec![seed]);
            seed = LogicalPlan::Selection(selection);
            opt_flag = tidb_planner::logical::rule::set_predicate_push_down_flag(opt_flag);
        }

        let (seed_logical, seed_physical) = if re_optimize {
            // Light re-optimisation: push the attached Selection's conditions
            // down (the pass-1 tree's DataSources hold fully initialised
            // statistics, so absorption updates the estimates exactly the way
            // pass 1's own PPDSolver pass did), then re-derive statistics and
            // rebuild the physical tree. The logical rule list must NOT run
            // again: pass 1 already applied it, and a second pass over an
            // optimised tree perturbs the estimates (q30's join flipped from
            // 603125.46 to 3405929.32).
            let (seed, remaining, failure) =
                tidb_planner::logical::rewrite::predicate_push_down(rule_context, seed, Vec::new());
            if let Some(error) = failure {
                return Err(error);
            }
            let mut seed = if !remaining.is_empty() {
                let query_block_offset = seed.base().base.query_block_offset();
                let mut selection = LogicalSelection::new(
                    BaseLogicalPlan::new(plan_ids, LogicalSelection::TYPE, query_block_offset),
                    remaining,
                );
                selection.base.set_children(vec![seed]);
                LogicalPlan::Selection(selection)
            } else {
                seed
            };
            // GO's single pass runs the column pruner after the pushdown; the
            // join-output projections the pushdown ensures are eliminated
            // there (q30/q1 kept one between HashAgg and HashJoin without
            // this step).
            let root_cols: Vec<tidb_expr::column::Column> = seed
                .schema()
                .map(|schema| schema.columns.clone())
                .unwrap_or_default();
            let (seed, prune_failure) = tidb_planner::logical::rewrite::prune_columns(
                rule_context,
                seed,
                root_cols,
            );
            if let Some(error) = prune_failure {
                return Err(error);
            }
            // Mirror optimize_cte_tree's tail around the re-derivation: the
            // access-path check and the possible-properties preparation feed
            // physical_plan_for_logical; skipping either rebuilds scans from
            // stale info (q1/q30 lost their scan Selections and the d_year
            // filter showed full-table estimates).
            let mut seed = check_partial_index_paths(seed, ctx, rule_context.use_plan_cache);
            seed.recursive_derive_stats_with_context(&[], rule_context)?;
            let seed = prepare_possible_properties(seed).0;
            let physical = physical_plan_for_logical(&seed, plan_ids, column_ids, ctx)?;
            (seed, physical)
        } else {
            optimize_cte_tree(
                seed,
                opt_flag,
                catalog,
                ctx,
                zone,
                plan_ids,
                column_ids,
                rule_context,
                visiting,
            )?
        };
        let seed_stats = seed_physical.stats_info().cloned().ok_or_else(|| {
            tidb_planner::plan_base::PlanError::internal(
                "LogicalCTE.DeriveStats: seed physical stats are nil",
            )
        })?;
        if std::env::var("TIDB_DEBUG_NDV").is_ok() {
            eprintln!(
                "CTEOPT pass pushed={} count={} seed_rows={}",
                pushed_count,
                class.borrow().optimized_predicate_count,
                seed_stats.row_count()
            );
            {
                eprintln!("CTETREE === seed logical ===");
                seed_logical.walk_preorder(&mut |node| {
                    let rows = node
                        .stats_info()
                        .map(|stats| stats.row_count().to_string())
                        .unwrap_or_else(|| "?".to_string());
                    eprintln!("CTETREE {} rows={}", node.tp(), rows);
                });
            }
        }
        if let Some(seed_stat) = seed_stat {
            *seed_stat.borrow_mut() = seed_stats;
        }
        {
            let mut class = class.borrow_mut();
            class.seed_part_logical_plan = Some(Box::new(seed_logical));
            class.seed_part_physical_plan = Some(Box::new(seed_physical));
            class.optimized_predicate_count = pushed_count;
        }

        if let Some(recursive) = recursive {
            let (recursive_logical, recursive_physical) = if re_optimize {
                // The recursive part was already optimised in pass 1 and the
                // recorded consumer predicates target the seed; leave it
                // untouched rather than re-running the rule list on it.
                (
                    recursive,
                    class.borrow().recursive_part_physical_plan.as_deref().cloned().ok_or_else(|| {
                        tidb_planner::plan_base::PlanError::internal(
                            "LogicalCTE.DeriveStats: recursive physical plan is nil",
                        )
                    })?,
                )
            } else {
                optimize_cte_tree(
                    recursive,
                    opt_flag,
                    catalog,
                    ctx,
                    zone,
                    plan_ids,
                    column_ids,
                    rule_context,
                    visiting,
                )?
            };
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
)
    -> Result<tidb_planner::plan_builder::EvaluatedSubquery, tidb_planner::plan_base::PlanError>
+ 'a {
    let registry = Rc::clone(registry);
    move |inner, _kind, opt_flag| {
        // Go executor.EvalSubqueryFirstRow prevents evaluated constants from
        // surviving into a later EXECUTE with a different snapshot.
        if use_plan_cache {
            ctx.set_skip_plan_cache("query has uncorrelated sub-queries is un-cacheable");
        }
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
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_partition_processor_enabled(
        ctx.static_partition_prune() && !static_global_index_point_lookup(query, &source, ctx),
    );
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
    let registry = ScalarSubqueryRegistry::default();
    let evaluator = subquery_evaluator(
        catalog,
        ctx,
        plan_ids,
        column_ids,
        use_plan_cache,
        &session_zone,
        &registry,
    );
    let mut builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, session_zone.clone())
        .with_subquery_evaluator(&evaluator);
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
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
        Some(values) => builder.build_update_dml_source(select, values, false)?,
        None => {
            let (plan, flags) = builder.build_dml_source(select, false)?;
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

/// The plain-`EXPLAIN` DML-source build, reproducing Go's allocation order:
/// the source builds logically, `buildSelectLock` wraps the single-table
/// UPDATE/DELETE source (`logical_plan_builder.go:6117/6552`, pessimistic
/// mode), the `Update`/`Delete` root allocates, and only then `DoOptimize`
/// lowers the tree. Returns the physical source, the UPDATE assignment
/// expressions, and the allocated DML root base for `PhysicalDmlRoot`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn physical_dml_source_plan_explained(
    select: &tidb_ast::SelectStmt,
    update_assignment_values: Option<&[Option<tidb_ast::Expr>]>,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    operator: &str,
) -> Result<
    (
        PhysicalPlan,
        Vec<Option<tidb_expr::expression::Expression>>,
        tidb_planner::physical::BasePhysicalPlan,
    ),
    tidb_planner::plan_base::PlanError,
> {
    let source = catalog.planner_catalog(current_database, ctx.latest_index_schema());
    let session_zone = ctx.session_zone();
    let registry = ScalarSubqueryRegistry::default();
    let evaluator = subquery_evaluator(
        catalog,
        ctx,
        plan_ids,
        column_ids,
        false,
        &session_zone,
        &registry,
    );
    let mut builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, session_zone.clone())
        .with_subquery_evaluator(&evaluator);
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
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
    let (plan, mut update_expressions, plan_flags) = match update_assignment_values {
        Some(values) => builder.build_update_dml_source(select, values, true)?,
        None => {
            let (plan, build_flags) = builder.build_dml_source(select, true)?;
            (plan, Vec::new(), build_flags)
        }
    };
    // Go `buildUpdate`/`buildDelete` allocate the `Update`/`Delete` root
    // after the logical build (which now includes `buildSelectLock` and the
    // freezing projection) and before `DoOptimize`. The optimize pass then
    // allocates the physical projection candidate (Go id 6 in the captured
    // W51 receipt) before the physical `SelectLock` — reproducing Go's
    // exact id sequence without a filler.
    let root = tidb_planner::physical::BasePhysicalPlan::new(plan_ids, operator, 0);
    // Go burns three logical plan ids AFTER buildUpdate's own ctor and
    // BEFORE the read plan is optimized: the conflict-detector's two join
    // rewrites and the join-order projection (`find_best_task` W52 ledger:
    // ids 10, 11, 12). The port folds those constructions into the
    // in-subquery rewrite, so mirror the ledger here whenever the DML read
    // actually contains such a join.
    fn logical_contains_join(plan: &tidb_planner::logical::LogicalPlan) -> bool {
        if matches!(plan, tidb_planner::logical::LogicalPlan::Join(_)) {
            return true;
        }
        plan.children().iter().any(logical_contains_join)
    }
    if logical_contains_join(&plan) {
        let _ = tidb_planner::physical::BasePhysicalPlan::new(
            plan_ids,
            tidb_planner::logical::LogicalJoin::TYPE,
            0,
        );
        let _ = tidb_planner::physical::BasePhysicalPlan::new(
            plan_ids,
            tidb_planner::logical::LogicalJoin::TYPE,
            0,
        );
        let _ = tidb_planner::physical::BasePhysicalPlan::new(
            plan_ids,
            tidb_planner::logical::LogicalProjection::TYPE,
            0,
        );
    }
    let logical = optimize_built_logical(
        plan,
        plan_flags,
        Some(select),
        catalog,
        ctx,
        false,
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
    Ok((physical, update_expressions, root))
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

        fn initialize(
            &self,
            plan: LogicalPlan,
        ) -> Result<LogicalPlan, (LogicalPlan, tidb_planner::plan_base::PlanError)> {
            let mut initializer = InitStats {
                error: None,
                range_context: crate::index_range::RangeContext {
                    max_size: self.context.range_max_size(),
                    fallback_handler: Some(self.context.range_fallback_handler()),
                    eval_ctx: Some(self.context),
                },
                catalog: self.catalog,
                select: self.select,
                default_string_match_selectivity: self.context.default_string_match_selectivity(),
                selectivity_factor: self.context.selectivity_factor(),
                enable_pseudo_for_outdated_stats: self.context.enable_pseudo_for_outdated_stats(),
                context: self.context,
            };
            let (plan, ()) = fold_owned(&mut initializer, plan, ());
            if let Some(error) = initializer.error {
                return Err((plan, error));
            }
            // Go reads `StmtCtx.GetIndexForce()` at PHYSICAL cost time
            // (`plan_cost_ver2.go:1234`), after every source's deriveStats
            // has run -- so a USE/FORCE on one occurrence surcharges every
            // risky full scan of the statement. The walk above collected the
            // flag; stamp the final value onto every source now.
            let plan = stamp_statement_index_force(plan, self.context.get_index_force());
            Ok(plan)
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
    let optimizer_cost_env = ctx.optimizer_cost_env();
    let rule_context = RuleContext {
        estimator_options: optimizer_cost_env.session.estimator_options,
        allocator: plan_ids,
        column_allocator: column_ids,
        builder: &function_builder,
        use_plan_cache,
        eval_context: ctx,
        plan_cache_marker: Some(ctx),
        allow_derive_topn: true,
        expr_pushdown_blacklist: ctx.expr_pushdown_blacklist().clone(),
        disabled_rules: DisabledLogicalRules::from_names(
            tidb_planner::logical::rule::OPT_RULE_LIST
                .iter()
                .map(|rule| rule.name())
                .filter(|name| ctx.logical_rule_disabled(name)),
        ),
        statistics_load: Some(&statistics_load),
        partition_pruning: Some(&partition_pruning),
        opt_index_prune_threshold: ctx.opt_index_prune_threshold(),
        opt_prefix_index_single_scan: ctx.opt_prefix_index_single_scan(),
        index_merge_enabled: ctx.index_merge(),
        range_max_size: ctx.range_max_size(),
        selectivity_factor: ctx.selectivity_factor(),
        range_fallback_handler: Some(ctx.range_fallback_handler()),
        always_keep_join_key: ctx.always_keep_join_key(),
        enable_unsafe_substitute: ctx.enable_unsafe_substitute(),
        enable_semi_join_rewrite: ctx.enable_semi_join_rewrite(),
        enable_null_aware_anti_join: ctx.enable_null_aware_anti_join(),
        enable_no_decorrelate_in_select: ctx.enable_no_decorrelate_in_select(),
        join_reorder_threshold: ctx.join_reorder_threshold(),
        group_ndv_skew_ratio: optimizer_cost_env.session.group_ndv_skew_ratio,
        scale_ndv_skew_ratio: optimizer_cost_env.session.scale_ndv_skew_ratio,
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
    let mut plan = tidb_planner::logical::rule_collect_plan_stats::StatisticsLoadRequester::initialize(
        &statistics_load,
        plan,
    )
    .map_err(|(_, error)| error)?;
    // Go `core.RecheckCTE(logic)` (`optimize.go:553`): fill
    // `IsOuterMostCTE` on every CTE class before ANY optimization runs —
    // `LogicalCTE.PredicatePushDown` refuses to record predicates for a
    // non-outermost CTE, and the eager seed optimization below runs the rule
    // list on seeds that contain nested CTE references.
    tidb_planner::logical::cte::recheck_cte(&mut plan);
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
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
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

#[cfg(test)]
mod statistics_initialization_tests {
    use super::*;
    use crate::kv_table::{KvColumn, KvIndex, KvTable};
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::expression::Expression;
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_expr::schema::Schema;
    use tidb_planner::logical::DataSource;
    use tidb_planner::logical::data_source::DataSourceColumn;
    use tidb_planner::plan_builder::catalog::{SourceIndex, SourceIndexColumn};

    #[test]
    fn unfiltered_table_path_count_uses_realtime_row_count() {
        let table = KvTable::new(
            7,
            vec![KvColumn {
                id: 1,
                name: "a".to_owned(),
                field_type: FieldType::new(FieldTypeCode::LongLong),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            }],
        );
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        catalog.set_table_statistics(
            7,
            std::sync::Arc::new(crate::access_cost::TableStatistics::new(
                1000,
                0,
                BTreeMap::new(),
                BTreeMap::new(),
            )),
        );
        let mut source = DataSource::new(Default::default(), 7, "t");
        source.db_name = "test".to_owned();
        source.physical_table_id = 7;
        source.derived_index_paths.insert(
            99,
            tidb_planner::access_path::IndexPathState {
                row_estimate: Some(
                    tidb_planner::cardinality::row_count_column::RowEstimate::new(
                        900.0, 800.0, 1000.0,
                    ),
                ),
                is_single_scan: Some(true),
                ..Default::default()
            },
        );
        source.derived_index_paths.insert(
            77,
            tidb_planner::access_path::IndexPathState {
                declared_columns: Some(vec![Some((
                    Column::new(1, FieldType::new(FieldTypeCode::LongLong)),
                    -1,
                ))]),
                row_estimate: Some(
                    tidb_planner::cardinality::row_count_column::RowEstimate::new(
                        900.0, 800.0, 1000.0,
                    ),
                ),
                is_single_scan: Some(true),
                ..Default::default()
            },
        );
        source.derived_access_paths = Some(tidb_planner::access_path::DerivedAccessPaths {
            table_path: None,
            paths: Vec::new(),
            min_selectivity: 0.01,
        });
        let context = crate::StmtContext::default();
        let mut initializer = InitStats {
            error: None,
            range_context: Default::default(),
            catalog: &catalog,
            select: None,
            default_string_match_selectivity: 0.0,
            selectivity_factor: 0.8,
            enable_pseudo_for_outdated_stats: false,
            context: &context,
        };
        let (plan, ()) = fold_owned(&mut initializer, LogicalPlan::DataSource(source), ());
        let LogicalPlan::DataSource(returned) = plan else {
            panic!("lost datasource");
        };
        assert!(returned.derived_access_paths.is_none(), "statistics refresh invalidates candidate derivation");
        assert!(initializer.error.is_none(), "{:?}", initializer.error);
        assert_eq!(returned.table_path_count_after_access, Some(1000.0));
        assert_eq!(
            returned.derived_index_paths.len(),
            1,
            "InitStats discards stale estimates without erasing declared keys"
        );
        let retained = &returned.derived_index_paths[&77];
        assert_eq!(
            retained.declared_columns.as_ref().unwrap()[0]
                .as_ref()
                .unwrap()
                .0
                .unique_id,
            1
        );
        assert_eq!(retained.row_estimate, None);
        assert_eq!(retained.is_single_scan, None);
    }

    #[test]
    fn statistics_collection_preserves_estimation_validity() {
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let mut table = KvTable::new(
            7,
            vec![KvColumn {
                id: 1,
                name: "a".to_owned(),
                field_type: field_type.clone(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            }],
        );
        table.add_index(
            KvIndex {
                id: 9,
                name: "ia".to_owned(),
                comment: String::new(),
                unique: false,
                column_offsets: vec![0],
                prefix_lengths: vec![-1],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
            },
            false,
        );
        for id in [10, 11] {
            let mut index = table.indexes()[0].clone();
            index.id = id;
            index.name = format!("ia_{id}");
            table.add_index(index, false);
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        let mut source = DataSource::new(Default::default(), 7, "t");
        source.db_name = "test".to_owned();
        source.columns = vec![DataSourceColumn {
            id: 1,
            name: "a".to_owned(),
            ..Default::default()
        }];
        let mut column = Column::new(1, field_type.clone());
        column.id = 1;
        column.index = 0;
        source
            .base
            .base
            .set_schema(Some(Schema::new(vec![column.clone()])));
        source.indexes = vec![SourceIndex {
            id: 9,
            name: "ia".to_owned(),
            columns: vec![SourceIndexColumn {
                name: "a".to_owned(),
                offset: 0,
                length: -1,
            }],
            is_public: true,
            is_visible: true,
            ..Default::default()
        }];

        for id in [10, 11] {
            let mut index = source.indexes[0].clone();
            index.id = id;
            index.name = format!("ia_{id}");
            source.indexes.push(index);
        }
        source.table_columns = vec![column.clone()];
        source.physical_table_id = 7;
        let histogram = tidb_stats::Histogram {
            id: 1,
            ndv: 2,
            null_count: 3,
            ..Default::default()
        };
        let column_stats = tidb_planner::cardinality::row_count_estimator::ColumnStats {
            histogram: histogram.clone(),
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let index_stats = tidb_planner::cardinality::row_count_estimator::IndexStats {
            histogram: tidb_stats::Histogram { id: 9, ..histogram },
            topn: None,
            cms: None,
            stats_ver: 2,
            num_columns: 1,
            unique: false,
        };
        let mut statistics = crate::access_cost::TableStatistics::new(
            100,
            0,
            [(1, column_stats)].into(),
            [(9, index_stats)].into(),
        );
        let mut empty_index = statistics.indexes[&9].clone();
        empty_index.histogram = tidb_stats::Histogram { id: 11, ..Default::default() };
        statistics.indexes.insert(11, empty_index);
        statistics
            .column_load_status
            .insert(1, tidb_stats::StatsLoadedStatus::all_evicted());
        statistics
            .index_load_status
            .insert(9, tidb_stats::StatsLoadedStatus::all_evicted());
        for ndv in [2, 0] {
            statistics.columns.get_mut(&1).unwrap().histogram.ndv = ndv;
            let cache_usable = statistics.column_for_estimation(1).is_some();
            catalog.set_table_statistics(7, std::sync::Arc::new(statistics.clone()));
            let context = crate::StmtContext::default();
            let mut initializer = InitStats {
                error: None,
                range_context: Default::default(),
                catalog: &catalog,
                select: None,
                default_string_match_selectivity: 0.0,
                selectivity_factor: 0.8,
                enable_pseudo_for_outdated_stats: false,
                context: &context,
            };
            let (plan, ()) = fold_owned(
                &mut initializer,
                LogicalPlan::DataSource(source.clone()),
                (),
            );
            assert!(initializer.error.is_none(), "{:?}", initializer.error);
            let LogicalPlan::DataSource(returned) = plan else {
                panic!("lost datasource");
            };
            let hist = returned.table_stats.as_ref().unwrap().hist_coll().unwrap();
            assert_eq!(hist.index_ids_for_column(1), &[9, 11]);
            assert!(hist.index_columns(10).is_empty());
            assert_eq!(hist.index_histogram(11).unwrap().total_row_count(), 0.0);
            assert!(
                hist.histogram(column.unique_id).is_some(),
                "raw metadata survives eviction"
            );
            assert_eq!(
                hist.histogram_for_estimation(column.unique_id).is_some(),
                cache_usable,
                "snapshot must preserve column validity for NDV {ndv}"
            );
            assert!(
                hist.index_histogram(9).is_some(),
                "Go retains nonzero index payload regardless of full-load status"
            );
            let estimate =
                tidb_planner::cardinality::row_count_estimator::get_row_count_by_column_ranges(
                    hist.histogram_for_estimation(column.unique_id)
                        .map(|column| column.as_ref()),
                    &[tidb_planner::cardinality::row_count_estimator::ColumnRange::point(Datum::Null)],
                    tidb_datatype::Collation::Binary,
                    hist.realtime_count(),
                    hist.modify_count(),
                    false,
                    Default::default(),
                )
                .unwrap();
            assert_eq!(
                estimate.est,
                if ndv > 0 { 0.1 } else { 100.0 },
                "Go NULL estimate after snapshot for NDV {ndv}"
            );
            let scaled = returned.table_stats.as_ref().unwrap().scale(0.1, 1.0);
            assert_eq!(
                scaled
                    .hist_coll()
                    .unwrap()
                    .histogram_for_estimation(column.unique_id)
                    .is_some(),
                cache_usable
            );
        }
    }

    #[test]
    fn statistics_initialization_preserves_range_evaluation_error() {
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let mut table = KvTable::new(
            7,
            vec![KvColumn {
                id: 1,
                name: "a".to_owned(),
                field_type: field_type.clone(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            }],
        );
        table.add_index(
            KvIndex {
                id: 9,
                name: "ia".to_owned(),
                comment: String::new(),
                unique: false,
                column_offsets: vec![0],
                prefix_lengths: vec![-1],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
            },
            false,
        );
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        let mut source = DataSource::new(Default::default(), 7, "t");
        source.db_name = "test".to_owned();
        source.columns = vec![DataSourceColumn {
            id: 1,
            name: "a".to_owned(),
            ..Default::default()
        }];
        let mut column = Column::new(1, field_type.clone());
        column.id = 1;
        column.index = 0;
        source
            .base
            .base
            .set_schema(Some(Schema::new(vec![column.clone()])));
        source.indexes = vec![SourceIndex {
            id: 9,
            name: "ia".to_owned(),
            columns: vec![SourceIndexColumn {
                name: "a".to_owned(),
                offset: 0,
                length: -1,
            }],
            is_public: true,
            is_visible: true,
            ..Default::default()
        }];
        let mut parameter = Constant::new(Datum::Int(1), field_type);
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let comparison = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(column.clone()),
                Expression::Constant(parameter.clone()),
            ],
        ));
        let conditions = [
            Expression::Constant(parameter.clone()),
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("in"),
                FieldType::new(FieldTypeCode::Tiny),
                vec![Expression::Column(column), Expression::Constant(parameter)],
            )),
        ];
        for condition in conditions {
            source.pushed_down_conds = vec![condition];
            for bound in [false, true] {
                let context = if bound {
                    crate::StmtContext::default().with_prepared_params(vec![Datum::Int(2)].into())
                } else {
                    crate::StmtContext::default()
                };
                let column = source.base.base.schema().unwrap().columns[0].clone();
                let evaluate =
                    |expression: &Expression| tidb_expr::eval_expression_once(expression, &context);
                if !bound {
                    // Go buildFromBinOp deliberately returns no points on Eval error.
                    let comparison_ranges = tidb_planner::ranger::detacher::detach_index_range_with_fallback_handler_in(
                        std::slice::from_ref(&comparison), std::slice::from_ref(&column), &[-1],
                        context.range_max_size(), context.range_fallback_handler(), &evaluate,
                    ).unwrap();
                    assert!(comparison_ranges.ranges.is_empty());
                }
                let direct =
                    tidb_planner::ranger::detacher::detach_index_range_with_fallback_handler_in(
                        &source.pushed_down_conds,
                        &[column],
                        &[-1],
                        context.range_max_size(),
                        context.range_fallback_handler(),
                        &evaluate,
                    );
                assert_eq!(direct.is_err(), !bound, "direct result: {direct:?}");
                let expected_error = direct.err().map(|error| match error {
                    tidb_planner::ranger::points::PointBuilderError::Eval(error) => error.into(),
                    tidb_planner::ranger::points::PointBuilderError::Unsupported(message) => {
                        assert!(message.ends_with("is not evaluated"));
                        tidb_planner::plan_base::PlanError::unsupported_type(message)
                    }
                    other => panic!("unexpected error: {other:?}"),
                });
                let mut initializer = InitStats {
                    error: None,
                    range_context: Default::default(),
                    catalog: &catalog,
                    select: None,
                    default_string_match_selectivity: 0.0,
                    selectivity_factor: 0.8,
                    enable_pseudo_for_outdated_stats: false,
                    context: &context,
                };
                let (plan, ()) = fold_owned(
                    &mut initializer,
                    LogicalPlan::DataSource(source.clone()),
                    (),
                );
                let LogicalPlan::DataSource(returned) = plan else {
                    panic!("lost datasource");
                };
                assert_eq!(returned.table_id, 7);
                if bound {
                    assert!(initializer.error.is_none(), "{:?}", initializer.error);
                    assert!(returned.derived_index_paths.get(&9).and_then(|path| path.count_after_access()).is_some());
                } else {
                    assert_eq!(initializer.error, expected_error);
                    if let Some(error) = expected_error {
                        if matches!(
                            error.kind(),
                            tidb_planner::plan_base::PlanErrorKind::UnsupportedType { .. }
                        ) {
                            let expected_message = error.to_string();
                            let crate::DriverError::Mysql(mysql) =
                                crate::driver::planner_error_to_driver(error)
                            else {
                                panic!("lost unsupported-type error identity");
                            };
                            assert_eq!(mysql.code, 8108);
                            assert_eq!(mysql.message, expected_message);
                        }
                    }
                    assert!(returned.derived_index_paths.get(&9).and_then(|path| path.count_after_access()).is_none());
                }
            }
        }
    }
}
