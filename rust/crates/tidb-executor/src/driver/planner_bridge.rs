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

use tidb_planner::physical_plan_cache::RebuildPlanForCache;
use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use crate::StatsStmtCtx as _;
use tidb_planner::stmt_ctx::PlannerStmtCtx as _;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::rc::Rc;

use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expr_util::normal_form::extract_filters_from_dnfs;
use tidb_expr::expression::Expression;
use tidb_expr::simple_expr::compose_dnf_condition;
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_planner_core_base::ColumnIdAllocator;
use tidb_planner_core_operator_logicalop::cte::CteClass;
use tidb_planner_core_operator_logicalop::fold::{Descend, OwnedRewrite, fold_owned};
use tidb_planner::rule::logical_optimize;
use tidb_planner_core_rule::flags;
use tidb_planner_core_operator_logicalop::plan_context::{RuleContext};
use tidb_planner_core_operator_logicalop::{
    BaseLogicalPlan, LogicalPlan, LogicalSelection, prepare_possible_properties,
};
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_planner_core_base::PlanIdAllocator;
use tidb_planner::plan_builder::PlanBuilder;
use tidb_infoschema::table_source::TableSource;
use super::{CatalogStatisticsLoad, PlannerCatalogContext};
use tidb_planner_property::stats_info::StatsInfo;

use super::FromTable;
use tidb_infoschema::Catalog;
use super::from::FromScope;
use tidb_planner_cardinality::derive_stats::StatsInfoScale;

enum ListColumnsLocated {
    Full,
    Location(tidb_planner_rule::ListPartitionLocation),
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
        || ctx.select_limit() != u64::MAX
        || ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_52592, false)
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
            .any(|column| {
                let column = column.read();
                column.state != tidb_model::SchemaState::PUBLIC || column.is_generated()
            })
        || select.fields.fields().is_empty()
        || !select.fields.fields().iter().all(|field| match field {
            tidb_ast::SelectField::Wildcard(names) => {
                table_wildcard_matches(names, table_ref, table_name)
            }
            tidb_ast::SelectField::Expr {
                expr: tidb_ast::Expr::Column(names),
                alias: None,
            } => {
                table_column_matches(names, table_ref, table_name)
                    && names.last().is_some_and(|column_name| {
                        table
                            .columns
                            .iter()
                            .any(|column| column.read().name.original().go_lower_eq(column_name))
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
    if !table_column_matches(column_names, table_ref, table_name) {
        return false;
    }

    if batch && !table.partition_expression_is_column {
        return false;
    }

    table.indexes.iter().any(|index| {
        let index = index.read();
        if !index.global
            || !index.unique
            || !index.is_public()
            || index.invisible
            || index.mv_index
            || !index.condition_expr_string.is_empty()
            || index.columns.len() != 1
            || !index_hint_allows(&table_ref.hints, index.name.original())
        {
            return false;
        }
        let Some(key) = index.columns.get(0) else {
            return false;
        };
        let key = key.read();
        key.length < 0
            && table.column_at(key.offset as usize).is_some_and(|column| {
                column.read().name.original().go_lower_eq(column_name) && column.read().field_type.code().is_type_integer()
            })
    })
}

// Go's fast path compares only `Name.Table` / `WildCard.Table` with the
// table (`buildSchemaFromFields`, `getNameValuePairs`); a schema qualifier is
// never read, whatever it names.
fn table_column_matches(
    names: &[String],
    table_ref: &tidb_ast::TableRef,
    table_name: &str,
) -> bool {
    let relation = table_ref.alias.as_deref().unwrap_or(table_name);
    match names {
        [_column] => true,
        [name, _column] | [_, name, _column] => name.go_lower_eq(relation),
        _ => false,
    }
}

fn table_wildcard_matches(
    names: &[String],
    table_ref: &tidb_ast::TableRef,
    table_name: &str,
) -> bool {
    let relation = table_ref.alias.as_deref().unwrap_or(table_name);
    match names {
        [] => true,
        [name] | [_, name] => name.go_lower_eq(relation),
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
        let matches_index = hint.indexes.iter().any(|name| name.go_lower_eq(index_name));
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
    partition: &tidb_table::partition_routing::PartitionSpec,
    location: tidb_planner_rule::ListPartitionLocation,
) -> tidb_planner_rule::ListPartitionLocation {
    let mut remapped = tidb_planner_rule::ListPartitionLocation::new();
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
    partition: &tidb_table::partition_routing::PartitionSpec,
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
    partition: &tidb_table::partition_routing::PartitionSpec,
    condition: &Expression,
    columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<ListColumnsLocated, tidb_planner_core_base::PlanError> {
    let tidb_table::partition_routing::PartitionKind::ListColumns {
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
                    tidb_ranger::detacher::detach_partition_range_with_fallback_handler(
                        std::slice::from_ref(condition),
                        std::slice::from_ref(&columns[column_index]),
                        &[tidb_datatype::UNSPECIFIED_LENGTH],
                        context.range_max_size(),
                        context.range_fallback_handler(),
                        context.ranger_options(),
                    )
                    .map_err(|error| {
                        tidb_planner_core_base::PlanError::internal(format!(
                            "LIST COLUMNS range detachment failed: {error:?}"
                        ))
                    })?;
                let location = tidb_planner_rule::list_column_location_for_ranges(
                    &detached.ranges,
                    values,
                    *default_partition,
                    field_types,
                    column_index,
                )
                .map_err(|error| {
                    tidb_planner_core_base::PlanError::internal(format!(
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
    partition: &tidb_table::partition_routing::PartitionSpec,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<ListColumnsLocated, tidb_planner_core_base::PlanError> {
    let mut location = None;
    for condition in conditions {
        match locate_list_columns_condition(partition, condition, columns, context)? {
            ListColumnsLocated::Full => {}
            ListColumnsLocated::Location(found) => {
                if let Some(current) = &mut location {
                    tidb_planner_rule::intersect_list_partition_location(current, &found);
                } else {
                    location = Some(found);
                }
            }
        }
    }
    Ok(location.map_or(ListColumnsLocated::Full, ListColumnsLocated::Location))
}

fn locate_list_columns_dnf(
    partition: &tidb_table::partition_routing::PartitionSpec,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<ListColumnsLocated, tidb_planner_core_base::PlanError> {
    if conditions.is_empty() {
        return Ok(ListColumnsLocated::Full);
    }
    let mut location = tidb_planner_rule::ListPartitionLocation::new();
    for condition in conditions {
        match locate_list_columns_condition(partition, condition, columns, context)? {
            ListColumnsLocated::Full => return Ok(ListColumnsLocated::Full),
            ListColumnsLocated::Location(found) => {
                tidb_planner_rule::union_list_partition_location(&mut location, found);
            }
        }
    }
    Ok(ListColumnsLocated::Location(location))
}

fn list_columns_pruned_ids(
    partition: &tidb_table::partition_routing::PartitionSpec,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<Option<Vec<i64>>, tidb_planner_core_base::PlanError> {
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
    partition: &tidb_table::partition_routing::PartitionSpec,
    source: &tidb_planner_core_operator_logicalop::DataSource,
    builder: &RealFunctionBuilder<'_, crate::StmtContext>,
    context: &crate::StmtContext,
) -> Result<Vec<usize>, tidb_planner_core_base::PlanError> {
    let mut surviving = (0..partition.definitions.len()).collect::<Vec<_>>();
    // Go's hash pruning has no empty-condition shortcut (only the list pruner
    // does): a full range still reaches the BIT-column branch, which drops
    // the hash values the column's width cannot produce.
    let hash = matches!(
        partition.kind,
        tidb_table::partition_routing::PartitionKind::Hash
    );
    if (source.all_conds.is_empty() && !hash) || partition.dependencies.is_empty() {
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
        tidb_table::partition_routing::PartitionKind::ListColumns { .. }
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
    // Go `PartitionRangeForCNFExpr` prunes a single-column RANGE COLUMNS
    // table by its predicates, and only a wider one through the ranger.
    if let ([column], tidb_table::partition_routing::PartitionKind::RangeColumns { .. }) =
        (columns.as_slice(), &partition.kind)
    {
        if let Some(ids) = tidb_planner_rule::single_range_columns_pruned_ids(
            partition,
            &conditions,
            column,
            builder,
            context,
        ) {
            surviving.retain(|index| ids.contains(&partition.definitions[*index].id));
        }
        return Ok(remap_partition_indices(
            partition,
            &source.partition_names,
            surviving,
        ));
    }
    // Go `PruneRangePartition` prunes `PARTITION BY RANGE (expr)` by its
    // predicates too (`PartitionRangeForCNFExpr`), never through the ranger.
    if matches!(partition.kind, tidb_table::partition_routing::PartitionKind::Range { .. }) {
        if let [column] = columns.as_slice() {
            if let Some(ids) =
                tidb_planner_rule::scalar_range_pruned_ids(partition, &conditions, column, context)
            {
                surviving.retain(|index| ids.contains(&partition.definitions[*index].id));
            }
        }
        return Ok(remap_partition_indices(
            partition,
            &source.partition_names,
            surviving,
        ));
    }
    let lengths = vec![tidb_datatype::UNSPECIFIED_LENGTH; columns.len()];
    let Ok(detached) = tidb_ranger::detacher::detach_partition_range_with_fallback_handler(
        &conditions,
        &columns,
        &lengths,
        context.range_max_size(),
        context.range_fallback_handler(),
        context.ranger_options(),
    ) else {
        return Ok(remap_partition_indices(
            partition,
            &source.partition_names,
            0..partition.definitions.len(),
        ));
    };
    let pruned =
        tidb_planner_rule::pruned_ids_from_ranger(partition, &detached.ranges, context)
            .map_err(|error| tidb_planner_core_base::PlanError::internal(format!("{error:?}")))?;
    if let Some(ids) = pruned {
        surviving.retain(|index| ids.contains(&partition.definitions[*index].id));
    }
    Ok(remap_partition_indices(
        partition,
        &source.partition_names,
        surviving,
    ))
}

#[cfg(test)]
mod list_columns_pruning_tests {
    use super::*;
    use tidb_table::partition_routing::{PartitionDef, PartitionKind, PartitionSpec};
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
                    storage_class: Default::default(),
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

        let location = tidb_planner_rule::ListPartitionLocation::from([
            (0, std::collections::BTreeSet::from([0, 1])),
            (1, std::collections::BTreeSet::from([2])),
        ]);
        assert_eq!(
            remap_list_columns_location(&spec, location),
            tidb_planner_rule::ListPartitionLocation::from([(
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
        let mut source = tidb_planner_core_operator_logicalop::DataSource::default();
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







pub(super) use tidb_planner::point_get_plan::is_cte;

pub(super) fn logical_from_plan(
    join: &tidb_ast::Join,
    with: Option<&tidb_ast::WithClause>,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    in_dml: bool,
) -> Result<LogicalPlan, tidb_planner_core_base::PlanError> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let prune_fallback = dynamic_prune_fallback(catalog, ctx);
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .with_mpp_warning_sink(ctx);
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
    builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
    builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
    builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_tiflash_engine_removed_due_to_strict_sql_mode(
        ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
    );
    builder.advanced_join_hint = ctx.advanced_join_hint();
    builder.in_explain_stmt = ctx.in_explain_stmt();
    builder.parser_config = ctx.parser_config();
    builder.set_partition_processor_enabled(ctx.static_partition_prune());
    builder.set_dynamic_prune_fallback(&prune_fallback);
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
    builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
    builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
    builder.is_for_update_read = in_dml;
    builder.in_update_or_delete_stmt = in_dml;
    // Go `buildUpdate`/`buildDelete` build the statement's `WITH` before its
    // table refs, so a CTE name in the FROM resolves to the CTE.
    if let Some(with) = with {
        let counts = tidb_planner::plan_builder::cte::cte_consumer_counts(
            &super::multi_dml::multi_dml_select(Some(with), &[], join, None, &[], None),
            with,
        );
        builder.build_with(with, &counts)?;
    }
    builder.build_join(join)
}

/// Go's `visitInfo` of an INSERT, UPDATE or DELETE, in the order `buildInsert` /
/// `buildUpdate` / `buildDelete` append it: what building the read and the SET expressions
/// visits, then `UpdatePriv` for each table an assignment writes or
/// `DeletePriv` for each table removed from. A source alias is only a lookup
/// name: authorization always uses the resolved base table identity.
///
/// The plan is discarded, so an uncorrelated subquery is not RUN (see
/// [`query_visit_info`]); a read that cannot be built contributes the visits it
/// made, and the statement's own planning raises the error.
pub fn dml_visit_info(
    statement: &tidb_ast::DmlStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_planner::plan_builder::visit_info::VisitInfo>, super::DriverError> {
    // Planning raises warnings (a folded cast, a hint); this pass plans the
    // statement a second time, so its warnings must not reach the statement's.
    let _no_folding = tidb_expr::constant_fold::suspend_constant_folding();
    let warning_bookmark = ctx.warning_count();
    let visits = dml_visit_info_inner(statement, catalog, current_db, ctx);
    ctx.truncate_warnings(warning_bookmark);
    visits
}

fn dml_visit_info_inner(
    statement: &tidb_ast::DmlStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_planner::plan_builder::visit_info::VisitInfo>, super::DriverError> {
    let source = catalog
        .planner_catalog(current_db, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let session_zone = ctx.session_zone();
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let evaluator = |inner: &LogicalPlan,
                     _kind: tidb_planner::plan_builder::SubqueryKind,
                     _opt_flag: u64| {
        let width = inner.schema().map_or(0, |schema| schema.len());
        Ok(tidb_planner::plan_builder::EvaluatedSubquery {
            column_ids: (0..width).map(|_| column_ids.alloc()).collect(),
            row: vec![tidb_datatype::Datum::Null; width],
            has_row: false,
        })
    };
    let defaults = default_evaluator(catalog, ctx);
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, session_zone)
        .with_subquery_evaluator(&evaluator)
        .with_default_evaluator(&defaults);
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
    builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
    builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
    builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
    builder.in_explain_stmt = ctx.in_explain_stmt();
    builder.parser_config = ctx.parser_config();
    builder.add_opt_flag(flags::PRUNE_COLUMNS);
    match statement {
        tidb_ast::DmlStmt::Update(update) => builder.update_visits(update),
        tidb_ast::DmlStmt::Delete(delete) => builder.delete_visits(delete),
        tidb_ast::DmlStmt::Insert(insert) => Ok(builder.insert_visits(insert)),
        _ => Ok(Vec::new()),
    }
    .map_err(super::planner_error_to_driver)
}

pub(super) fn update_target_columns(
    update: &tidb_ast::UpdateStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<(String, String, String)>, super::DriverError> {
    let source = catalog
        .planner_catalog(current_db, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let targets = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .resolve_update_targets(update)
        .map_err(super::planner_error_to_driver)?;
    Ok(targets
        .into_iter()
        .map(|target| (target.database, target.table, target.column))
        .collect())
}

/// Builds the name-resolution scope of one `FROM` node through Go's logical
/// `buildResultSetNode` path. Correlation discovery needs the logical schema
/// and output names, not an executor or a second AST-side join builder.
pub(crate) fn logical_from_scope(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<FromScope, tidb_planner_core_base::PlanError> {
    let plan = logical_from_plan(join, None, catalog, current_database, ctx, false)?;
    let schema = plan.schema().ok_or_else(|| {
        tidb_planner_core_base::PlanError::internal("FROM logical plan has no schema")
    })?;
    let names = plan.output_names();
    if schema.columns.len() != names.len() {
        return Err(tidb_planner_core_base::PlanError::internal(
            "FROM logical schema and output names have different widths",
        ));
    }

    let mut scope = FromScope::for_statement(ctx);
    for (column, name) in schema.columns.iter().zip(names) {
        if name.hidden || name.not_explicit_usable {
            continue;
        }
        let field_type = column.ret_type.clone().ok_or_else(|| {
            tidb_planner_core_base::PlanError::internal("FROM column has no field type")
        })?;
        let database = &name.names.database.original;
        let visible_table = &name.names.table.original;
        let column_name = &name.names.column.original;
        let offset = scope.width();
        let append = scope.tables.last_mut().filter(|table| {
            table.name.go_lower_eq(visible_table)
                && table
                    .database
                    .as_deref()
                    .unwrap_or_default()
                    .go_lower_eq(database)
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
    error: Option<tidb_planner_core_base::PlanError>,
    catalog: &'a Catalog,
    /// Go's statistics demand for this statement (`statistics_demand`), once
    /// `CollectPredicateColumnsPoint` has seen the plan: the columns and
    /// indexes whose payloads the synchronous load makes available. `None`
    /// before that point.
    demanded: Option<&'a std::collections::HashMap<tidb_model::TableItemID, bool>>,
    context: &'a crate::StmtContext,
    /// Whether this walk records the statement's used statistics (Go
    /// `initStats`' `RecordUsedInfo` and `recordUsedItemStatsStatus`). Go
    /// initializes a source when its statistics are derived, after the
    /// partition processor; the walk this port runs before the logical rules
    /// would record a statically pruned table's logical id, so it does not.
    record_used_stats: bool,
}

/// Go `recordUsedItemStatsStatus` (`cardinality/trace.go`) over the table
/// `GetStatsTable` answered with. `statistics` is `None` for a fresh
/// `PseudoTable`, whose pseudo column and index objects carry a zero
/// `StatsLoadedStatus` -- `unInitialized` -- for every public, non-hidden
/// column and public index (`in_pseudo_table`), while anything else is a nil
/// object its all-false existence map reports `missing`. The table's ledger
/// entry carries what Go's `initStats` records for it: the pseudo version
/// (zero) and the `PseudoTable` counts.
pub(crate) fn record_used_item_stats_status(
    context: &crate::StmtContext,
    table_name: &str,
    physical_table_id: i64,
    statistics: Option<&tidb_stats::Table>,
    in_pseudo_table: bool,
    item_id: i64,
    is_index: bool,
) {
    // Go `recordUsedItemStatsStatus`: `_tidb_rowid` (id -1) and other internal
    // pseudo columns have no statistics to report.
    if item_id <= 0 {
        return;
    }
    let Some(statistics) = statistics else {
        context.record_used_stats_status(
            physical_table_id,
            table_name,
            tidb_stats::PSEUDO_VERSION,
            tidb_stats::PSEUDO_ROW_COUNT,
            0,
            item_id,
            is_index,
            if in_pseudo_table {
                tidb_stats::StatsLoadedStatus::default().status_to_string()
            } else {
                "missing"
            },
        );
        return;
    };
    let loaded_status = if is_index {
        statistics
            .hist_coll
            .get_index(item_id)
            .map(|index| read(&index).stats_loaded_status)
    } else {
        statistics
            .hist_coll
            .get_column(item_id)
            .map(|column| read(&column).stats_loaded_status)
    };
    let analyzed = statistics
        .existence_map
        .as_ref()
        .is_some_and(|map| read(map).has_analyzed(item_id, is_index));
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
        if statistics.hist_coll.pseudo {
            0
        } else {
            statistics.version
        },
        statistics.hist_coll.realtime_count,
        statistics.hist_coll.modify_count,
        item_id,
        is_index,
        status,
    );
}



fn read<T>(lock: &std::sync::RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl InitStats<'_> {
    /// The `(column ids, index ids)` Go's lite statistics initialization
    /// leaves loaded for this source: what `CollectPredicateColumnsPoint`'s
    /// demand (read off the plan after predicate pushdown) names for its
    /// physical table and the cache holds in full.
    ///
    /// `None` before that demand exists or when it names no column of this
    /// source, which leaves the caller on the cache's fully loaded subset.
    fn predicate_loaded_items(
        &self,
        source: &tidb_planner_core_operator_logicalop::DataSource,
        statistics: Option<&tidb_stats::Table>,
    ) -> Option<(BTreeSet<i64>, BTreeSet<i64>)> {
        statistics?;
        let demanded = self.demanded?;
        let mut columns = BTreeSet::new();
        let mut indexes = BTreeSet::new();
        for (item, full_load) in demanded {
            if item.table_id != source.physical_table_id {
                continue;
            }
            if item.is_index {
                indexes.insert(item.id);
            } else if *full_load {
                // Go `addPredicateColumnsFromExpressions(..., false)`: a join
                // key asks for the column's essential statistics only, so its
                // histogram stays evicted.
                columns.insert(item.id);
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
                .and_then(|statistics| statistics.hist_coll.get_column(*id))
                .is_some_and(|column| read(&column).is_full_load())
        });
        indexes.retain(|id| {
            statistics
                .and_then(|statistics| statistics.hist_coll.get_index(*id))
                .is_some_and(|index| read(&index).is_full_load())
        });
        if columns.is_empty() {
            return None;
        }
        Some((columns, indexes))
    }
}

impl InitStats<'_> {
    fn initialize_source(
        &mut self,
        node: &mut LogicalPlan,
    ) -> Result<Descend<(), ()>, tidb_planner_core_base::PlanError> {
        let LogicalPlan::DataSource(source) = node else {
            return Ok(Descend::Children(vec![(); node.children().len()]));
        };
        source.base.base.set_stats(None);
        source.derived_access_paths = None;
        source.derived_index_paths.retain(|_, path| {
            path.row_estimate = None;
            path.filled = None;
            path.is_single_scan = None;
            path.declared_columns.is_some()
        });
        source.access_path_min_selectivity = 1.0;
        // Go `deriveStats4DataSource`: `EliminateNoPrecisionLossCast` over the
        // pushed conditions before any index path is filled, so `cast(a AS
        // SIGNED) = 1` on an INT key -- a UNION branch's widening cast --
        // builds a point range.
        let builder = tidb_expr::expr_util::RealFunctionBuilder::new(self.context);
        for condition in &mut source.pushed_down_conds {
            *condition =
                tidb_expr::expr_util::eliminate_no_precision_loss_cast(condition, &builder);
        }
        // Go `initStats` calls `GetStatsTable(..., ds.PhysicalTableID)`: a
        // static-pruning child owns one physical partition's statistics,
        // while an ordinary/dynamic source keeps the logical table ID here.
        // `None` is the fresh `PseudoTable` -- a table without statistics,
        // and rule 2's answer to a zero `RealtimeCount`.
        let statistics = crate::access_cost::get_stats_table(
            self.context,
            self.catalog,
            source.physical_table_id,
        );
        let statistics = statistics.as_deref();
        let row_count = crate::access_cost::realtime_row_count(statistics);
        // Go `buildDataSource`: a table with an available TiFlash replica
        // marks the statement as reading TiFlash whenever MPP is allowed.
        if source.has_tiflash_replica && self.context.optimizer_cost_env().session.mpp_allowed {
            self.context.mark_is_tiflash();
        }
        // Go `initStats`: the statement's `UsedStatsInfo` entry for this
        // source's physical table, `PseudoVersion` (0) for a pseudo table.
        if self.record_used_stats {
            self.context.record_used_stats_info(
                source.physical_table_id,
                self.catalog.used_stats_table_name(source.physical_table_id),
                statistics
                    .filter(|statistics| !statistics.hist_coll.pseudo)
                    .map_or(0, |statistics| statistics.version),
                row_count as i64,
                statistics.map_or(0, |statistics| statistics.hist_coll.modify_count),
            );
        }
        // Retain Go's loaded subset, including empty and TopN-only payloads.
        // The cache's explicit status excludes evicted metadata from NDV
        // scaling and histogram estimates without inferring state from buckets.
        let predicate_loaded = self.predicate_loaded_items(source, statistics);
        // Go `ds.TableInfo`.
        let table_info = source
            .table_info
            .clone()
            .expect("a planned data source carries its TableInfo");
        tidb_planner::stats::init_stats::init_stats(
            source,
            &table_info.read(),
            statistics,
            row_count,
            predicate_loaded,
        );
        // Go `deriveStats`' `StmtCtx.SetIndexForce()` (`stats.go:165`): the
        // flag is statement-wide, so a hint on ANY occurrence records it here
        // and the pass below stamps every source once the walk completes.
        if !source.forced_index_ids.is_empty() {
            self.context.set_index_force();
        }
        tidb_planner_core_rule::rule_collect_plan_stats::refresh_source_group_ndvs_with_scale_ratio(
            source, self.context.optimizer_cost_env().session.scale_ndv_skew_ratio,
        );
        Ok(Descend::Stop(()))
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
        // Clear EVERY node's cached derived statistics, DataSources included.
        // The sources' cached `stats_info` was derived from whatever histogram
        // snapshot was loaded when the first derive ran -- commonly BEFORE
        // `SyncWaitStatsLoadPoint` finishes the synchronous load (Go derives
        // lazily at physical time, after the wait, so it never caches a cold
        // estimate). Preserving the source's cached estimate here made every
        // downstream derive early-return the cold value: TPC-DS q7's
        // `eq(cd_education_status, 'Primary')` estimated 1.00 instead of
        // 276934.43 and collapsed the whole plan's row counts.
        node.set_stats(None);
        (node, ())
    }
}

fn planner_physical_select(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    let query = tidb_ast::QueryStmt::Select(Box::new(select.clone()));
    let (logical, plan_ids, column_ids) = planner_optimized_query(
        &query,
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
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
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
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
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
    tidb_planner_core_base::PlanError,
> {
    let registry = ScalarSubqueryRegistry::default();
    let logical = planner_optimized_query_with_allocators(
        query,
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
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    physical_plan_for_logical_with_update_refs(
        logical,
        plan_ids,
        column_ids,
        ctx,
        &std::collections::BTreeSet::new(),
    )
}

/// [`physical_plan_for_logical`] for an UPDATE's read plan: Go's
/// `StmtCtx.ColRefFromUpdatePlan`, the columns its assignments read, keeps
/// the projection that names them.
pub(crate) fn physical_plan_for_logical_with_update_refs(
    logical: &LogicalPlan,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    ctx: &crate::StmtContext,
    col_ref_from_update_plan: &std::collections::BTreeSet<i64>,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    tidb_planner::optimize::optimize_physical(
        logical,
        plan_ids,
        column_ids,
        ctx,
        col_ref_from_update_plan,
    )
}

/// Go `utilfuncp.DoOptimize` as `LogicalCTE.DeriveStats` calls it
/// (`logical_cte.go:172`): the CTE class is optimized lazily, the first time a
/// reference's statistics are derived, with the DNF of every predicate its
/// references have recorded by then. It is never re-optimized.
struct DriverCteOptimizer<'a> {
    catalog: &'a Catalog,
    ctx: &'a crate::StmtContext,
    plan_ids: &'a PlanIdAllocator,
    column_ids: &'a ColumnIdAllocator,
    /// Classes being optimized right now; a class reaching itself is a cycle.
    visiting: RefCell<HashSet<usize>>,
    parallel_apply_allowed: std::cell::Cell<bool>,
}

impl tidb_planner_core_operator_logicalop::plan_context::CteOptimizer for DriverCteOptimizer<'_> {
    fn optimize_cte(
        &self,
        class: &Rc<RefCell<CteClass>>,
        seed_stat: Option<&Rc<RefCell<StatsInfo>>>,
        rule_context: &RuleContext<'_>,
    ) -> Result<(), tidb_planner_core_base::PlanError> {
        if class.borrow().seed_part_physical_plan.is_some() {
            return Ok(());
        }
        let identity = Rc::as_ptr(class) as usize;
        if !self.visiting.borrow_mut().insert(identity) {
            return Err(tidb_planner_core_base::PlanError::internal(
                "LogicalCTE.DeriveStats: cyclic CTE class optimization",
            ));
        }
        let result = self.optimize_class(class, seed_stat, rule_context);
        self.visiting.borrow_mut().remove(&identity);
        result
    }
}

impl DriverCteOptimizer<'_> {
    fn optimize_class(
        &self,
        class: &Rc<RefCell<CteClass>>,
        seed_stat: Option<&Rc<RefCell<StatsInfo>>>,
        rule_context: &RuleContext<'_>,
    ) -> Result<(), tidb_planner_core_base::PlanError> {
        let (mut seed, mut opt_flag, predicates) = {
            let class = class.borrow();
            let seed = class
                .seed_part_logical_plan
                .as_deref()
                .ok_or_else(|| {
                    tidb_planner_core_base::PlanError::internal(
                        "LogicalCTE.DeriveStats: seed logical plan is nil",
                    )
                })?
                .deep_clone();
            (seed, class.opt_flag, class.push_down_predicates.clone())
        };
        // Go composes the predicates recorded by every reference as one DNF,
        // extracts common conjuncts, and puts that Selection above the seed
        // before running the CTE's own optimizer pass.
        if let Some(dnf) = compose_dnf_condition(predicates) {
            let conditions = extract_filters_from_dnfs(vec![dnf]);
            let query_block_offset = seed.base().base.query_block_offset();
            let mut selection = LogicalSelection::new(
                BaseLogicalPlan::new(self.plan_ids, LogicalSelection::TYPE, query_block_offset),
                conditions,
            );
            selection.base.set_children(vec![seed]);
            seed = LogicalPlan::Selection(selection);
            opt_flag = tidb_planner_core_rule::set_predicate_push_down_flag(opt_flag);
            class.borrow_mut().opt_flag = opt_flag;
        }
        let (seed_logical, seed_physical) = self.optimize_tree(seed, opt_flag, rule_context)?;
        // Go `*p.SeedStat = *resStat` precedes the recursive DoOptimize.
        if let (Some(seed_stat), Some(stats)) = (seed_stat, seed_physical.stats_info()) {
            *seed_stat.borrow_mut() = stats.clone();
        }
        {
            let mut class = class.borrow_mut();
            class.seed_part_logical_plan = Some(Box::new(seed_logical));
            class.seed_part_physical_plan = Some(Box::new(seed_physical));
        }
        let recursive = {
            let class = class.borrow();
            match (
                class.recursive_part_logical_plan.as_deref(),
                class.recursive_part_physical_plan.is_none(),
            ) {
                (Some(recursive), true) => Some(recursive.deep_clone()),
                _ => None,
            }
        };
        if let Some(recursive) = recursive {
            // Go temporarily disables parallel Apply throughout recursive
            // body optimization, including lazily optimized nested CTE parts.
            let previous = self.parallel_apply_allowed.replace(false);
            let optimized = self.optimize_tree(recursive, opt_flag, rule_context);
            self.parallel_apply_allowed.set(previous);
            let (logical, physical) = optimized?;
            let mut class = class.borrow_mut();
            class.recursive_part_logical_plan = Some(Box::new(logical));
            class.recursive_part_physical_plan = Some(Box::new(physical));
        }
        Ok(())
    }

    /// Go `DoOptimize` over one CTE part: logical rules, statistics (which
    /// optimize nested CTE classes lazily through the same hook), and the
    /// physical search.
    fn optimize_tree(
        &self,
        plan: LogicalPlan,
        opt_flag: u64,
        rule_context: &RuleContext<'_>,
    ) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
        let context = self
            .ctx
            .clone()
            .with_parallel_apply(self.parallel_apply_allowed.get());
        let ctx = &context;
        // Go DataSource.DeriveStats initializes base statistics even when
        // called from a logical rule; CTE roots need the same preinitialization
        // as the outer query before join reorder can inspect their sources.
        let mut initializer = InitStats {
            error: None,
            catalog: self.catalog,
            demanded: None,
            context: ctx,
            record_used_stats: true,
        };
        let (plan, ()) = fold_owned(&mut initializer, plan, ());
        if let Some(error) = initializer.error {
            return Err(error);
        }
        let opt_flag = tidb_planner::optimize::adjust_optimization_flags(opt_flag, ctx);
        // Go's CTE `DoOptimize` runs `logicalOptimize` and `physicalOptimize`,
        // which set the session's optimizer clocks like any other pass.
        let begin_logical_opt = std::time::Instant::now();
        let logical_result = logical_optimize(rule_context, opt_flag, plan);
        ctx.phase_durations()
            .set_logical_opt(begin_logical_opt.elapsed());
        let optimized = logical_result.map_err(|(_, error)| error)?.plan;
        let mut optimized = tidb_planner::optimize::check_partial_index_paths(
            optimized,
            ctx,
            rule_context.use_plan_cache,
        );
        let begin_physical_opt = std::time::Instant::now();
        optimized.recursive_derive_stats_with_context(&[], rule_context)?;
        ctx.phase_durations()
            .finish_stats_derive(begin_physical_opt);
        let logical = prepare_possible_properties(optimized).0;
        let physical = physical_plan_for_logical(&logical, self.plan_ids, self.column_ids, ctx)?;
        Ok((logical, physical))
    }
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
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
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
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
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
) -> Result<(PhysicalPlan, Vec<RegisteredScalarSubquery>), tidb_planner_core_base::PlanError> {
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
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
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
    /// Go `ScalarSubqueryEvalCtx.ID()`: `Init` allocates it from the plan id
    /// counter once the child has been optimized, and EXPLAIN prints it.
    pub plan_id: i32,
}

/// The per-statement list of [`RegisteredScalarSubquery`]s, Go's
/// `SessionVars.MapScalarSubQ` narrowed to one statement.
pub(crate) type ScalarSubqueryRegistry = Rc<RefCell<Vec<RegisteredScalarSubquery>>>;

/// Carries an executor failure back across the planner boundary.
///
/// Go's `EvalSubqueryFirstRow` returns a plain error, so the typed identities
/// the driver layer owns have to survive the round trip through
/// [`tidb_planner_core_base::PlanError`]; `planner_error_to_driver` maps them
/// back when the statement unwinds.
fn driver_error_to_plan(error: crate::DriverError) -> tidb_planner_core_base::PlanError {
    match error {
        // Go's `EvalSubqueryFirstRow` returns the executor's error as it is.
        crate::DriverError::Mysql(error) => tidb_planner_core_base::PlanError::from(error),
        crate::DriverError::MemTableRetrievalNeeded => {
            tidb_planner_core_base::PlanError::mem_table_retrieval_needed()
        }
        crate::DriverError::Exec(crate::ExecError::Eval(eval)) => {
            tidb_planner_core_base::PlanError::eval(eval)
        }
        other => tidb_planner_core_base::PlanError::internal(other.to_string()),
    }
}

/// Go `evalFieldDefaultValue` (`expression_rewriter.go:3303`) for the
/// planner's [`tidb_planner::plan_builder::DefaultEvaluator`]: the public
/// column's `table.GetColDefaultValue` under the statement's context -- a
/// CURRENT_TIMESTAMP default reads the statement clock, a column with no
/// default is NULL or 1364 -- as a constant of the column's type.
fn default_evaluator<'a>(
    catalog: &'a Catalog,
    ctx: &'a crate::StmtContext,
) -> impl Fn(&str, &str, &str) -> Result<Expression, tidb_exprctx::EvalError> + 'a {
    default_evaluator_using(catalog, ctx, super::dml::DefaultUse::Expression)
}

/// [`default_evaluator`] for a given use of the default: an INSERT's own
/// `DEFAULT` (Go `getDefaultValueForInsert`) answers differently from
/// `DEFAULT(col)` inside an expression.
fn default_evaluator_using<'a>(
    catalog: &'a Catalog,
    ctx: &'a crate::StmtContext,
    default_use: super::dml::DefaultUse,
) -> impl Fn(&str, &str, &str) -> Result<Expression, tidb_exprctx::EvalError> + 'a {
    move |database, table, column| {
        let entry = catalog.table_in(database, table).ok_or_else(|| {
            // Go `is.TableByName` failing.
            tidb_exprctx::EvalError::Conversion(tidb_terror::TerrorError::compatible(
                tidb_terror::TerrorCode::new(1146),
                format!("Table '{database}.{table}' doesn't exist"),
            ))
        })?;
        let meta = super::dml::column_metadata(entry)
            .into_iter()
            .find(|meta| meta.name.go_lower_eq(column))
            .ok_or_else(|| {
                // Go `ErrUnknownColumn.GenWithStackByArgs(colName, "field_list")`.
                tidb_exprctx::EvalError::UnknownColumnInClause(column.go_to_lower(), "field_list")
            })?;
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        let value =
            super::dml::materialize_column_default(&meta, default_use, ctx, chunk.get_row(0))
                .map_err(|error| match error {
                    crate::DriverError::Exec(crate::ExecError::Eval(error)) => error,
                    // Go `table.ErrNoDefaultValue.GenWithStackByArgs(colName)`.
                    error if error.equal(&tidb_table::table_errors::ERR_NO_DEFAULT_VALUE) => {
                        tidb_exprctx::EvalError::Conversion(
                            tidb_table::table_errors::ERR_NO_DEFAULT_VALUE
                                .generate(error.to_string()),
                        )
                    }
                    // Any other refusal (a default whose own conversion truncates, for
                    // instance) keeps the code the client would have seen.
                    error => {
                        let error = error.to_mysql_error();
                        tidb_exprctx::EvalError::Conversion(
                            tidb_terror::TerrorError::registered(
                                tidb_terror::TerrorClass::Types,
                                tidb_terror::TerrorCode::new(error.code as isize),
                                error.message,
                            ),
                        )
                    }
                })?;
        Ok(Expression::Constant(tidb_expr::constant::Constant::new(
            value,
            meta.field_type,
        )))
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
    session_zone: &'a tidb_datatype::SessionTimeZone,
    registry: &ScalarSubqueryRegistry,
) -> impl Fn(
    &LogicalPlan,
    tidb_planner::plan_builder::SubqueryKind,
    u64,
)
    -> Result<tidb_planner::plan_builder::EvaluatedSubquery, tidb_planner_core_base::PlanError>
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
        let plan_id = plan_ids.alloc();
        // Go `EvalSubqueryFirstRow`'s deferred `PreprocessSubQueries++` and
        // `DurationPreprocessSubQuery += time.Since(begin)`, error or not.
        let begin_evaluation = std::time::Instant::now();
        // Go `EvalSubqueryFirstRow`'s region: the subquery's executors run
        // under it.
        let region = tidb_tracing::start_thread_region_ex("executor.EvalSubQuery");
        let evaluated =
            crate::driver::physical_builder::execute_first_row(&mut physical, catalog, ctx);
        region.end();
        ctx.phase_durations()
            .add_preprocess_sub_query(begin_evaluation.elapsed());
        let (row, has_row) = evaluated.map_err(driver_error_to_plan)?;
        registry.borrow_mut().push(RegisteredScalarSubquery {
            output_col_ids: output_col_ids.clone(),
            physical,
            plan_id,
        });
        Ok(tidb_planner::plan_builder::EvaluatedSubquery {
            column_ids: output_col_ids,
            row,
            has_row,
        })
    }
}

/// Go `buildDataSource`'s dynamic-mode check for a partitioned table
/// (`logical_plan_builder.go:4997`): a table whose global statistics were
/// never analyzed (`Table.IsAnalyzed`) turns this statement's pruning static
/// -- `UseDynamicPruneMode = false` and the 1105 warning -- unless missing
/// partition statistics may be skipped (`tidb_skip_missing_partition_stats`,
/// which fix control 44262 overrides).
fn dynamic_prune_fallback<'a>(
    catalog: &'a Catalog,
    ctx: &'a crate::StmtContext,
) -> impl Fn(i64, &str) -> bool + 'a {
    move |table_id, table_name| {
        let global_stats_ready = crate::access_cost::table_statistics(ctx, catalog, table_id)
            .is_some_and(|statistics| statistics.last_analyze_version > 0);
        let allow_dynamic_without_stats = ctx.optimizer_fix_control().get_bool_with_default(
            tidb_fixcontrol::FIX_44262,
            ctx.skip_missing_partition_stats(),
        );
        if global_stats_ready || allow_dynamic_without_stats {
            return false;
        }
        ctx.disable_dynamic_prune();
        tidb_exprctx::Columns::append_warning(
            ctx,
            1105,
            &format!("disable dynamic pruning due to {table_name} has no global stats"),
        );
        true
    }
}

fn planner_optimized_query(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PlanIdAllocator, ColumnIdAllocator), tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let logical = planner_optimized_query_with_allocators(
        query,
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
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    registry: &ScalarSubqueryRegistry,
) -> Result<LogicalPlan, tidb_planner_core_base::PlanError> {
    // Go `buildLogicalPlan`: `RewritePhaseInfo.Reset()`, then
    // `DurationRewrite` is the time `builder.Build` took.
    ctx.phase_durations().reset_rewrite_phase_info();
    let begin_rewrite = std::time::Instant::now();
    let (plan, opt_flag) = planner_built_query_with_allocators(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        registry,
        None,
    )?;
    ctx.phase_durations()
        .set_duration_rewrite(begin_rewrite.elapsed());
    // Go `buildAndOptimizeLogicalPlanRound`: the optimization clock starts
    // before the first `DoOptimize` of a logical plan.
    ctx.phase_durations().begin_optimize();
    optimize_built_logical(
        plan,
        opt_flag,
        catalog,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        &ctx.session_zone(),
    )
}

/// Go `planner.Optimize` for a `ShowStmt`: `PlanBuilder.Build`'s
/// `buildShow` over the preprocessed statement, then the logical and
/// physical optimization every plan goes through.
pub(crate) fn physical_show_plan(
    show: &tidb_ast::ShowStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner::common_plans::Plan,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    let (plan, opt_flag, visits) = parts.build(None, None, |builder| {
        let plan = builder.build_show(show)?;
        Ok((plan, builder.get_opt_flag(), builder.visit_info.clone()))
    })?;
    let tidb_planner::common_plans::Plan::Logical(plan) = plan else {
        return Ok((plan, visits));
    };
    let logical = optimize_built_logical(
        plan,
        opt_flag,
        catalog,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        &ctx.session_zone(),
    )?;
    physical_plan_for_logical(&logical, &plan_ids, &column_ids, ctx)
        .map(|physical| (tidb_planner::common_plans::Plan::Physical(physical), visits))
}

/// Go `buildSet` for a `SET` statement: the plan and its visit infos.
pub(crate) fn set_plan(
    set: &tidb_ast::SetStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner::common_plans::Set,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    parts.build(None, None, |builder| {
        let tidb_planner::common_plans::Plan::Set(plan) = builder.build_set(set)? else {
            unreachable!("buildSet answers a Set plan");
        };
        Ok((plan, builder.visit_info.clone()))
    })
}

/// Go `PlanBuilder.Build` for a statement that is not a query or DML: the
/// common plan and its visit infos.
pub(crate) fn statement_plan(
    stmt: &tidb_ast::Stmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    session_ctx: &dyn tidb_planner::plan_builder::stmt_build::SessionPlanContext,
) -> Result<
    (
        tidb_planner::common_plans::Plan,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: Some(session_ctx),
    };
    parts.build(None, None, |builder| {
        let plan = builder.build_stmt(stmt)?;
        Ok((plan, builder.visit_info.clone()))
    })
}

/// Go `planner.Optimize` for `ADMIN SHOW DDL JOBS [n] [WHERE ...]`:
/// `buildAdmin`'s `LogicalShowDDLJobs` under its selection, optimized.
pub(crate) fn physical_admin_show_ddl_jobs_plan(
    job_number: i64,
    where_clause: Option<&tidb_ast::Expr>,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    let (plan, opt_flag) = parts.build(None, None, |builder| {
        let plan = builder.build_admin_show_ddl_jobs(job_number, where_clause)?;
        Ok((plan, builder.get_opt_flag()))
    })?;
    let logical = optimize_built_logical(
        plan,
        opt_flag,
        catalog,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        &ctx.session_zone(),
    )?;
    physical_plan_for_logical(&logical, &plan_ids, &column_ids, ctx)
}

/// Go `PlanBuilder.Build` for a query: the logical plan before any
/// optimization, with the optimizer flags the build collected.
/// `building_view` names the view whose stored body this is, which Go builds
/// from `BuildDataSourceFromView` with the view on its building stack.
fn planner_built_query_with_allocators(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    registry: &ScalarSubqueryRegistry,
    building_view: Option<(&str, &str)>,
) -> Result<(LogicalPlan, u64), tidb_planner_core_base::PlanError> {
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        registry,
        session_ctx: None,
    };
    parts.build(Some(query), building_view, |builder| {
        let node = tidb_resolve::NodeW::new(query.clone());
        let plan = builder.build_query_node(&node, false)?;
        ctx.set_block_as_names(builder.statement_block_as_names());
        Ok((plan, builder.get_opt_flag()))
    })
}

/// Go `buildDDL`'s `*ast.CreateViewStmt` arm over this catalog: the body is
/// built, never run; `create.query` is left holding the expanded body
/// `ddl.BuildViewInfo` restores and `create.columns` the view's column names.
/// Returns the types of the body's output columns.
pub(crate) fn build_create_view(
    create: &mut tidb_ast::CreateViewStmt,
    view_schema: &str,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_datatype::FieldType>, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    let body = (*create.query).clone();
    parts.build(Some(&body), None, |builder| {
        let plan = builder.build_create_view(view_schema, create)?;
        Ok(logical_column_types(&plan))
    })
}

/// What [`QueryBuilderParts::build`] configures a query's `PlanBuilder` from.
struct QueryBuilderParts<'a> {
    catalog: &'a Catalog,
    current_database: &'a str,
    ctx: &'a crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &'a PlanIdAllocator,
    column_ids: &'a ColumnIdAllocator,
    registry: &'a ScalarSubqueryRegistry,
    /// Go `PlanContext`'s session-side reach for the statement builders.
    session_ctx: Option<&'a dyn tidb_planner::plan_builder::stmt_build::SessionPlanContext>,
}

impl QueryBuilderParts<'_> {
    /// Go `NewPlanBuilder().Init(sctx, is, ...)` for a query: a builder over
    /// this catalog with the session's planner variables, handed to `build`.
    /// `query` is the statement the build is for, when there is one, which
    /// decides static pruning's global-index exception.
    fn build<R>(
        &self,
        query: Option<&tidb_ast::QueryStmt>,
        building_view: Option<(&str, &str)>,
        build: impl FnOnce(
            &mut PlanBuilder<'_, tidb_infoschema::PlannerCatalog, crate::StmtContext>,
        ) -> Result<R, tidb_planner_core_base::PlanError>,
    ) -> Result<R, tidb_planner_core_base::PlanError> {
        let (catalog, current_database, ctx) = (self.catalog, self.current_database, self.ctx);
        let (plan_ids, column_ids) = (self.plan_ids, self.column_ids);
        let source = catalog
            .planner_catalog(current_database, ctx.latest_index_schema())
            .with_statement_hypo_state(ctx)
            .with_statement_dirty_tables(ctx)
            .with_statement_privilege_verifier(ctx)
            .with_statement_related_tables_for_mdl(ctx);
        let session_zone = ctx.session_zone();
        let evaluator = subquery_evaluator(
            catalog,
            ctx,
            plan_ids,
            column_ids,
            self.use_plan_cache,
            &session_zone,
            self.registry,
        );
        let prune_fallback = dynamic_prune_fallback(catalog, ctx);
        let defaults = default_evaluator(catalog, ctx);
        let mut builder =
            PlanBuilder::new(&source, ctx, plan_ids, column_ids, session_zone.clone())
                .with_subquery_evaluator(&evaluator)
                .with_default_evaluator(&defaults)
                .with_mpp_warning_sink(ctx);
        if let Some(session_ctx) = self.session_ctx {
            builder = builder.with_session_ctx(session_ctx);
        }
        builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
        builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
        builder.only_full_group_by = ctx.only_full_group_by();
        builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
        builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
        builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
        builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
        builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
        builder.set_isolation_read_engines(ctx.isolation_read_engines());
        builder.set_tiflash_engine_removed_due_to_strict_sql_mode(
            ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
        );
        builder.advanced_join_hint = ctx.advanced_join_hint();
        builder.in_explain_stmt = ctx.in_explain_stmt();
        builder.parser_config = ctx.parser_config();
        builder.set_partition_processor_enabled(
            ctx.static_partition_prune()
                && !query
                    .is_some_and(|query| static_global_index_point_lookup(query, &source, ctx)),
        );
        builder.set_dynamic_prune_fallback(&prune_fallback);
        // Go `PlanBuilder.Build`: `b.optFlag |= FlagPruneColumns` for every
        // statement, which each evaluated subquery's `DoOptimize` reads.
        builder.add_opt_flag(flags::PRUNE_COLUMNS);
        if let Some((database, view)) = building_view {
            builder.building_view_stack.insert((
                tidb_mysql::to_lowercase(database),
                tidb_mysql::to_lowercase(view),
            ));
        }
        builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
        builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
        builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
        builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
        builder.prefer_index_merge_by_fix_control = ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_52869, false);
        builder.index_merge_skip_range_scan_check = ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_52869, true);
        build(&mut builder)
    }
}

/// Go `tryFillViewColumnType`'s build: `BuildDataSourceFromView` over the
/// view `database`.`view_name`, whose body is parsed and built -- never run --
/// under the view's own recursion guard, privilege rule and `ErrViewInvalid`
/// mapping. Each of `columns` takes the static type of the output column of
/// its name (`expression.FindFieldNameIdxByColName`), `None` when there is
/// none.
pub(crate) fn view_column_types(
    database: &str,
    view_name: &str,
    columns: &[String],
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Vec<Option<tidb_datatype::FieldType>>, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database: database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    parts.build(None, None, |builder| {
        let view = builder
            .source
            .find_view(database, view_name)
            .cloned()
            .ok_or_else(|| {
                tidb_planner_core_base::PlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .fast_generate_by_args(&[database.into(), view_name.into()]),
                )
            })?;
        let plan = builder.build_data_source_from_view(&view)?;
        let types = logical_column_types(&plan);
        let names = plan.output_names();
        Ok(columns
            .iter()
            .map(|column| {
                names
                    .iter()
                    .position(|name| tidb_mysql::lowercase_equal(&name.names.column.lower, column))
                    .and_then(|index| types.get(index).cloned())
            })
            .collect())
    })
}

/// The static type of each output column of a built logical plan.
fn logical_column_types(plan: &LogicalPlan) -> Vec<tidb_datatype::FieldType> {
    plan.schema()
        .map(|schema| {
            schema
                .columns
                .iter()
                .map(|column| {
                    column.ret_type.clone().unwrap_or_else(|| {
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Go's `visitInfo` for a query: the privileges its plan needs, collected by
/// the plan builder while it resolves names and expands views (a DEFINER view's
/// body already verified against its definer, an INVOKER view's appended).
///
/// The query is only planned logically; nothing runs.
pub fn query_visit_info(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_planner::plan_builder::visit_info::VisitInfo>, super::DriverError> {
    // Planning raises warnings (a folded cast, a hint); this pass plans the
    // statement a second time, so its warnings must not reach the statement's.
    let _no_folding = tidb_expr::constant_fold::suspend_constant_folding();
    let warning_bookmark = ctx.warning_count();
    let planned = query_visit_info_inner(query, catalog, current_database, ctx);
    ctx.truncate_warnings(warning_bookmark);
    planned
}

fn query_visit_info_inner(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_planner::plan_builder::visit_info::VisitInfo>, super::DriverError> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let session_zone = ctx.session_zone();
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    // The plan is discarded, so an uncorrelated subquery is not RUN (the
    // catalog planned against is not the statement's execution image, whose
    // virtual tables are materialised per statement): the builder has
    // already visited its tables by the time it asks for a value, and a row of
    // NULLs lets the outer plan finish building.
    let evaluator = |inner: &LogicalPlan,
                     _kind: tidb_planner::plan_builder::SubqueryKind,
                     _opt_flag: u64| {
        let width = inner.schema().map_or(0, |schema| schema.len());
        Ok(tidb_planner::plan_builder::EvaluatedSubquery {
            column_ids: (0..width).map(|_| column_ids.alloc()).collect(),
            row: vec![tidb_datatype::Datum::Null; width],
            has_row: false,
        })
    };
    let defaults = default_evaluator(catalog, ctx);
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, session_zone.clone())
        .with_subquery_evaluator(&evaluator)
        .with_default_evaluator(&defaults);
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
    builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
    builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
    builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
    builder.in_explain_stmt = ctx.in_explain_stmt();
    builder.parser_config = ctx.parser_config();
    let node = tidb_resolve::NodeW::new(query.clone());
    builder
        .build_query_node(&node, false)
        .map_err(super::planner_error_to_driver)?;
    Ok(std::mem::take(&mut builder.visit_info))
}

/// Go `buildInsert`'s expressions: the VALUES lists over the target's schema,
/// the `INSERT ... SELECT` checks, and the ON DUPLICATE assignments, with every
/// uncorrelated scalar subquery evaluated now (`handleScalarSubquery`'s
/// `EvalSubqueryFirstRow`).
pub(crate) fn plan_insert_expressions(
    insert: &tidb_ast::InsertStmt,
    select: Option<(
        &tidb_expr::schema::Schema,
        &tidb_planner_core_operator_physicalop::InsertSelectOutput,
    )>,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner::plan_builder::insert_values::InsertValues,
        Vec<tidb_planner::plan_builder::insert_on_duplicate::OnDuplicateAssign>,
        Vec<Option<Expression>>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    let session_zone = ctx.session_zone();
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let evaluator = subquery_evaluator(
        catalog,
        ctx,
        &plan_ids,
        &column_ids,
        false,
        &session_zone,
        &registry,
    );
    let defaults = default_evaluator(catalog, ctx);
    let insert_defaults = default_evaluator_using(catalog, ctx, super::dml::DefaultUse::Insert);
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, session_zone.clone())
        .with_subquery_evaluator(&evaluator)
        .with_default_evaluator(&defaults)
        .with_insert_default_evaluator(&insert_defaults);
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
    builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
    builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
    builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    // Go `PlanBuilder.Build`'s `b.optFlag |= FlagPruneColumns`, which every
    // statement's build starts from; `DoOptimize` of each subquery reads it.
    builder.add_opt_flag(tidb_planner_core_rule::flags::PRUNE_COLUMNS);
    let target = builder.resolve_insert_target(insert)?;
    let values = if insert.source.is_none() {
        builder.build_insert_values(insert, &target)?
    } else {
        tidb_planner::plan_builder::insert_values::InsertValues::default()
    };
    if let Some((schema, output)) = select {
        // Go counts the SELECT's own outputs when it was extended.
        builder.check_insert_select(&target, output.actual_col_len.unwrap_or(schema.len()))?;
    }
    let on_duplicate = builder.build_on_duplicate(insert, &target, select)?;
    // Go `resolveGeneratedColumns`, after the assignments.
    let generated = builder.resolve_generated_columns(&target)?;
    Ok((values, on_duplicate, generated))
}

/// Go `buildSelectPlanOfInsert`'s extension of the source for the ON
/// DUPLICATE assignments, and the width the SELECT had before it.
pub(crate) fn insert_extended_source(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Option<(tidb_ast::QueryStmt, usize)> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone());
    let target = builder.resolve_insert_target(insert).ok()?;
    builder.extend_insert_select(insert, &target)
}

/// [`physical_query_plan_with_allocators`] for an `INSERT ... SELECT`'s source,
/// also answering the names of the source's outputs (Go
/// `selectPlan.OutputNames()`).
pub(crate) fn physical_query_plan_and_names_with_allocators(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<(PhysicalPlan, Vec<tidb_datatype::FieldName>), tidb_planner_core_base::PlanError> {
    planner_physical_query_with_allocators(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
    )
    .map(|(logical, physical)| (physical_names(&logical), physical))
    .map(|(names, physical)| (physical, names))
}

fn physical_names(logical: &LogicalPlan) -> Vec<tidb_datatype::FieldName> {
    logical.output_names().to_vec()
}

/// Go `buildUpdateLists`'s resolution of a single-table UPDATE's SET items.
pub(crate) fn single_update_assignments(
    update: &tidb_ast::UpdateStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    Vec<tidb_planner::plan_builder::single_update::SingleUpdateAssignment>,
    tidb_planner_core_base::PlanError,
> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .resolve_single_update_assignments(update, ctx.allow_write_row_id())
}

/// Go `buildOnDuplicateUpdateColumns`: the columns an INSERT's resolved ON
/// DUPLICATE assignments write.
pub(crate) fn insert_on_duplicate_columns(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<String>, tidb_planner_core_base::PlanError> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone());
    let target = builder.resolve_insert_target(insert)?;
    builder.on_duplicate_columns(insert, &target)
}

/// Go `getAffectCols` of `buildInsert`: the columns an INSERT writes.
pub(crate) fn insert_target(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    tidb_planner::plan_builder::insert_values::InsertTarget,
    tidb_planner_core_base::PlanError,
> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .resolve_insert_target(insert)
}

/// The shared DML-source build, following Go's allocation order:
/// the source builds logically, `buildSelectLock` wraps the single-table
/// UPDATE/DELETE source (`logical_plan_builder.go:6117/6552`, pessimistic
/// mode), the `Update`/`Delete` root allocates, and only then `DoOptimize`
/// lowers the tree. Returns the physical source, the UPDATE assignment
/// expressions, and the allocated DML root base for `PhysicalDmlRoot`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn physical_dml_source_plan_with_allocators(
    select: &tidb_ast::SelectStmt,
    update_assignment_values: Option<&[Option<tidb_ast::Expr>]>,
    multi_update: Option<&tidb_ast::UpdateStmt>,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    operator: &str,
    delete_prune_has_fk: Option<bool>,
    delete_multi: Option<tidb_planner::plan_builder::delete_pruning::DeleteMultiInput>,
) -> Result<DmlSourcePlan, tidb_planner_core_base::PlanError> {
    // Go builds an UPDATE/DELETE inside `builder.Build` (`buildUpdate` /
    // `buildDelete` run the read plan's `DoOptimize` themselves), so its
    // `DurationRewrite` covers the read plan's optimization too, and the
    // non-logical `Update`/`Delete` never starts `optimize`'s total clock.
    ctx.phase_durations().reset_rewrite_phase_info();
    let begin_rewrite = std::time::Instant::now();
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
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
    let prune_fallback = dynamic_prune_fallback(catalog, ctx);
    let defaults = default_evaluator(catalog, ctx);
    let mut builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, session_zone.clone())
        .with_subquery_evaluator(&evaluator)
        .with_default_evaluator(&defaults)
        .with_mpp_warning_sink(ctx);
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
    builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
    builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
    builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_tiflash_engine_removed_due_to_strict_sql_mode(
        ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
    );
    builder.advanced_join_hint = ctx.advanced_join_hint();
    builder.in_explain_stmt = ctx.in_explain_stmt();
    builder.parser_config = ctx.parser_config();
    builder.set_partition_processor_enabled(ctx.static_partition_prune());
    builder.set_dynamic_prune_fallback(&prune_fallback);
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
    builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
    builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
    builder.prefer_index_merge_by_fix_control = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_fixcontrol::FIX_52869, false);
    builder.index_merge_skip_range_scan_check = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_fixcontrol::FIX_52869, true);
    builder.add_opt_flag(flags::PRUNE_COLUMNS);
    builder.delete_prune_has_fk = delete_prune_has_fk;
    builder.delete_multi = delete_multi;
    builder.allocate_delete_root = operator.eq_ignore_ascii_case("Delete");
    // Go `buildUpdate`/`buildDelete` lock the read only for a single-table
    // source (`TableRefs.Right == nil` / `!IsMultiTable`); a multi-table
    // write locks the rows it writes instead.
    let single_source = select.from.as_ref().is_some_and(|from| {
        from.right.is_none() && matches!(from.left, tidb_ast::JoinNode::Table(_))
    });
    let wrap_lock = ctx.pessimistic_transaction() && single_source;
    let (plan, mut update_expressions, mut plan_flags) = match (update_assignment_values, multi_update)
    {
        (Some(values), _) => builder.build_update_dml_source(select, values, wrap_lock)?,
        (None, Some(update)) => {
            builder.build_multi_update_dml_source(select, update, ctx.allow_write_row_id())?
        }
        (None, None) => {
            let (plan, build_flags) = builder.build_dml_source(select, wrap_lock)?;
            (plan, Vec::new(), build_flags)
        }
    };
    ctx.set_block_as_names(builder.statement_block_as_names());
    // Go `buildUpdate`: "We cannot apply projection elimination when building
    // the subplan, because columns in orderedList cannot be resolved" -- the
    // assignments live outside the plan, so a rewrite that substitutes a
    // projected column never reaches them (`DoOptimize(..., b.optFlag &^
    // rule.FlagEliminateProjection, p)`).
    if operator.eq_ignore_ascii_case("Update") {
        plan_flags &= !tidb_planner_core_rule::flags::ELIMINATE_PROJECTION;
    }
    // Go `buildUpdate`/`buildDelete` allocate the `Update`/`Delete` root
    // after the logical build (which now includes `buildSelectLock` and the
    // freezing projection) and before `DoOptimize`. The optimize pass then
    // allocates the physical projection candidate (Go id 6 in the captured
    // W51 receipt) before the physical `SelectLock` — reproducing Go's
    // exact id sequence without a filler.
    let root = builder
        .dml_root
        .take()
        .unwrap_or_else(|| tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(plan_ids, operator, 0));
    // Go burns three logical plan ids AFTER buildUpdate's own ctor and
    // BEFORE the read plan is optimized: the conflict-detector's two join
    // rewrites and the join-order projection (`find_best_task` W52 ledger:
    // ids 10, 11, 12). The port folds those constructions into the
    // in-subquery rewrite, so mirror the ledger here whenever the DML read
    // actually contains such a join.
    fn logical_contains_join(plan: &tidb_planner_core_operator_logicalop::LogicalPlan) -> bool {
        if matches!(plan, tidb_planner_core_operator_logicalop::LogicalPlan::Join(_)) {
            return true;
        }
        plan.children().iter().any(logical_contains_join)
    }
    if logical_contains_join(&plan) {
        let _ = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            plan_ids,
            tidb_planner_core_operator_logicalop::LogicalJoin::TYPE,
            0,
        );
        let _ = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            plan_ids,
            tidb_planner_core_operator_logicalop::LogicalJoin::TYPE,
            0,
        );
        let _ = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            plan_ids,
            tidb_planner_core_operator_logicalop::LogicalProjection::TYPE,
            0,
        );
    }
    let logical = optimize_built_logical(
        plan,
        plan_flags,
        catalog,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        &session_zone,
    )?;
    // Go `buildUpdateLists`: the columns a non-constant assignment reads
    // join `StmtCtx.ColRefFromUpdatePlan`.
    let col_ref_from_update_plan: std::collections::BTreeSet<i64> = update_expressions
        .iter()
        .flatten()
        .flat_map(tidb_expr::simple_expr::extract_columns)
        .map(|column| column.unique_id)
        .collect();
    let physical = physical_plan_for_logical_with_update_refs(
        &logical,
        plan_ids,
        column_ids,
        ctx,
        &col_ref_from_update_plan,
    )?;
    let schema = physical.schema().ok_or_else(|| {
        tidb_planner_core_base::PlanError::internal("physical DML source has no schema")
    })?;
    for expression in update_expressions.iter_mut().flatten() {
        tidb_expr::simple_expr::resolve_indices_in_place(expression, schema).map_err(|_| {
            tidb_planner_core_base::PlanError::internal(
                "UPDATE assignment does not resolve in its physical source",
            )
        })?;
    }
    ctx.phase_durations()
        .set_duration_rewrite(begin_rewrite.elapsed());
    Ok(DmlSourcePlan {
        plan: physical,
        update_expressions,
        root,
        delete_col_pos: builder.delete_col_pos.take(),
        delete_tables: std::mem::take(&mut builder.delete_tables),
        update_tables: std::mem::take(&mut builder.update_tables),
        update_assigns: std::mem::take(&mut builder.update_assigns),
    })
}

/// What the shared DML-source build produces for the `Update`/`Delete` root.
pub(crate) struct DmlSourcePlan {
    /// The optimized physical read.
    pub plan: PhysicalPlan,
    /// The UPDATE assignments' expressions, resolved against the read.
    pub update_expressions: Vec<Option<tidb_expr::expression::Expression>>,
    /// The allocated root base.
    pub root: tidb_planner_core_operator_physicalop::BasePhysicalPlan,
    /// A pruned single-table DELETE's layout.
    pub delete_col_pos: Option<tidb_planner_core_operator_physicalop::DeleteColPos>,
    /// A multi-table DELETE's layouts.
    pub delete_tables: Vec<tidb_planner_core_operator_physicalop::DeleteTablePos>,
    /// A multi-table UPDATE's layouts.
    pub update_tables: Vec<tidb_planner_core_operator_physicalop::UpdateTablePos>,
    /// A multi-table UPDATE's assignments.
    pub update_assigns: Vec<tidb_planner_core_operator_physicalop::UpdateAssign>,
}

/// Go `buildOrderedList` for the fast point UPDATE (`tryUpdatePointPlan`): the
/// assignments rewritten over the point plan's schema and cast to their
/// target columns, resolved to that schema's row.
pub(crate) fn point_update_ordered_list(
    table_ref: &tidb_ast::TableRef,
    schema: &tidb_expr::schema::Schema,
    assignments: &[(Vec<String>, Option<tidb_ast::Expr>)],
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<Vec<Option<Expression>>, tidb_planner_core_base::PlanError> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let defaults = default_evaluator(catalog, ctx);
    let builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, ctx.session_zone())
        .with_default_evaluator(&defaults);
    let mut list = builder.build_point_ordered_list(table_ref, schema, assignments)?;
    for expression in list.iter_mut().flatten() {
        tidb_expr::simple_expr::resolve_indices_in_place(expression, schema).map_err(|_| {
            tidb_planner_core_base::PlanError::internal(
                "a fast UPDATE assignment does not resolve in its point plan",
            )
        })?;
    }
    Ok(list)
}

/// Go `buildDelete`'s resolution of a multi-table DELETE's targets.
pub(crate) fn delete_targets(
    delete: &tidb_ast::DeleteStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    Vec<tidb_planner::plan_builder::delete_pruning::DeleteTarget>,
    tidb_planner_core_base::PlanError,
> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .resolve_delete_targets(delete)
}

#[allow(clippy::too_many_arguments)]
fn optimize_built_logical(
    plan: LogicalPlan,
    flags: u64,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    _session_zone: &tidb_datatype::SessionTimeZone,
) -> Result<LogicalPlan, tidb_planner_core_base::PlanError> {
    struct PlannerStatisticsLoad<'a> {
        catalog: &'a Catalog,
        context: &'a crate::StmtContext,
        /// The statement's accumulated statistics demand; see
        /// [`InitStats::demanded`]. A CTE body optimized on its own adds its
        /// own demand.
        demanded: RefCell<Option<std::collections::HashMap<tidb_model::TableItemID, bool>>>,
        /// See [`InitStats::record_used_stats`].
        record_used_stats: std::cell::Cell<bool>,
        /// Whether the planner's own statistics derivation records the items
        /// it reads. Go records them when it derives a source, after the
        /// partition processor and the synchronous statistics load; the first
        /// initialization walk runs before both.
        records_used_items: std::cell::Cell<bool>,
    }

    impl tidb_planner_core_operator_logicalop::plan_context::UsedStatsRecorder for PlannerStatisticsLoad<'_> {
        fn record_used_item(
            &self,
            physical_table_id: i64,
            table_name: &str,
            in_pseudo_table: bool,
            item_id: i64,
            is_index: bool,
        ) {
            if !self.records_used_items.get() {
                return;
            }
            let statistics =
                crate::access_cost::get_stats_table(self.context, self.catalog, physical_table_id);
            record_used_item_stats_status(
                self.context,
                table_name,
                physical_table_id,
                statistics.as_deref(),
                in_pseudo_table,
                item_id,
                is_index,
            );
        }
    }

    impl tidb_planner_core_operator_logicalop::plan_context::StatisticsLoadRequester
        for PlannerStatisticsLoad<'_>
    {
        fn request(
            &self,
            usage: &tidb_stats::table_statistics::ColumnStatsUsage,
        ) -> Result<(), tidb_planner_core_base::PlanError> {
            self.context.set_operator_num(usage.operator_count);
            self.context
                .update_col_stats_usage(usage.predicate_columns.keys().copied());
            self.context.record_table_runtime_statistics(
                usage.visited_logical_table_ids.iter().copied(),
                |table_id| crate::access_cost::table_statistics(self.context, self.catalog, table_id),
            );
            self.demanded
                .borrow_mut()
                .get_or_insert_with(Default::default)
                .extend(self.catalog.statistics_demand(self.context.stats_handle(), usage));
            self.catalog.request_statistics_load(usage, self.context)
        }

        fn wait(&self) -> Result<(), tidb_planner_core_base::PlanError> {
            self.catalog.wait_statistics_load(self.context)
        }

        fn initialize(
            &self,
            plan: LogicalPlan,
        ) -> Result<LogicalPlan, (LogicalPlan, tidb_planner_core_base::PlanError)> {
            let demanded = self.demanded.borrow();
            let mut initializer = InitStats {
                error: None,
                catalog: self.catalog,
                demanded: demanded.as_ref(),
                context: self.context,
                record_used_stats: self.record_used_stats.get(),
            };
            let (plan, ()) = fold_owned(&mut initializer, plan, ());
            if let Some(error) = initializer.error {
                return Err((plan, error));
            }
            if self.record_used_stats.get() {
                self.records_used_items.set(true);
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
        plan_ids: &'a PlanIdAllocator,
    }

    impl tidb_planner_core_operator_logicalop::plan_context::PartitionPruning
        for PlannerPartitionPruning<'_>
    {
        fn partition_indices(
            &self,
            source: &tidb_planner_core_operator_logicalop::DataSource,
        ) -> Result<Vec<usize>, tidb_planner_core_base::PlanError> {
            let table = self
                .catalog
                .kv_table_by_id(source.table_id)
                .ok_or_else(|| {
                    tidb_planner_core_base::PlanError::internal(
                        "partitioned logical table is absent from the catalog",
                    )
                })?;
            let partition = table.partition().ok_or_else(|| {
                tidb_planner_core_base::PlanError::internal(
                    "partition processor received an unpartitioned table",
                )
            })?;
            partition_indices_for_spec(partition, source, self.builder, self.context)
        }

        // Go `generateHashPartitionExpr` and `MakePartitionByFnCol` each
        // `PlanID.Add(1)` before parsing the partition expression, "to make
        // sure some tests will pass" (their words): a HASH and a plain RANGE
        // table spend one plan id per statically pruned source.
        fn spend_partition_expression_plan_id(&self, source: &tidb_planner_core_operator_logicalop::DataSource) {
            let spends = self
                .catalog
                .kv_table_by_id(source.table_id)
                .and_then(|table| table.partition().map(|partition| partition.kind.clone()))
                .is_some_and(|kind| {
                    matches!(
                        kind,
                        tidb_table::partition_routing::PartitionKind::Hash
                            | tidb_table::partition_routing::PartitionKind::Range { .. }
                    )
                });
            if spends {
                self.plan_ids.alloc();
            }
        }
    }

    let function_builder = RealFunctionBuilder::new(ctx);
    let statistics_load = PlannerStatisticsLoad {
        catalog,
        context: ctx,
        demanded: RefCell::new(None),
        record_used_stats: std::cell::Cell::new(true),
        records_used_items: std::cell::Cell::new(false),
    };
    let partition_pruning = PlannerPartitionPruning {
        catalog,
        builder: &function_builder,
        context: ctx,
        plan_ids,
    };
    let cte_optimizer = DriverCteOptimizer {
        catalog,
        ctx,
        plan_ids,
        column_ids,
        visiting: RefCell::new(HashSet::new()),
        parallel_apply_allowed: std::cell::Cell::new(ctx.enable_parallel_apply()),
    };
    // The base statistics are attached before the rule list with used-item
    // recording suspended for that walk.
    let initialize_statistics = |plan: LogicalPlan| {
        statistics_load.record_used_stats.set(false);
        let initialized =
            tidb_planner_core_operator_logicalop::plan_context::StatisticsLoadRequester::initialize(
                &statistics_load,
                plan,
            );
        statistics_load.record_used_stats.set(true);
        initialized
    };
    let services = tidb_planner::optimize::OptimizeServices {
        statistics_load: &statistics_load,
        initialize_statistics: &initialize_statistics,
        used_stats_recorder: &statistics_load,
        partition_pruning: &partition_pruning,
        cte_optimizer: &cte_optimizer,
    };
    tidb_planner::optimize::optimize_logical(
        plan,
        flags,
        ctx,
        &services,
        use_plan_cache,
        plan_ids,
        column_ids,
    )
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
    /// Go `PlanCacheStmt.NormalizedPlan` / `PlanDigest`, shared by every
    /// execution of this entry.
    digest: std::sync::Arc<std::sync::OnceLock<(String, String)>>,
}

impl CachedSelectPlan {
    pub(super) fn clone_for_execution(&self) -> Self {
        Self {
            statement: self.statement.clone(),
            physical: self.physical.deep_clone(),
            generation: self.generation,
            digest: std::sync::Arc::clone(&self.digest),
        }
    }

    pub(super) fn memory_usage(&self) -> i64 {
        tidb_planner::physical_plan_cache::cached_plan_memory_usage(&self.physical) as i64
    }

    pub(crate) fn bind(
        &mut self,
        values: &[tidb_datatype::Datum],
        ctx: Option<&crate::StmtContext>,
    ) -> Result<u64, CachedPlanBindFailure> {
        super::bind_prepared_statement_in_place(&mut self.statement, values)
            .map_err(|_| CachedPlanBindFailure::unbound())?;
        // Rebuild through the current execute parameters, never the datum
        // cached when the marker-bearing expression was first planned.
        let needs_statement = std::sync::atomic::AtomicBool::new(false);
        let statement = deferred_rebuild_context(ctx, values);
        let parameters = tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values);
        let evaluator = |expression: &tidb_expr::expression::Expression| {
            rebuild_evaluate(
                expression,
                statement.as_ref(),
                &parameters,
                &needs_statement,
            )
        };
        let rebuilt = self.physical.rebuild_plan_for_cache_in_place(
            &tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values)
                .with_deferred_evaluator(&evaluator),
        );
        if needs_statement.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(CachedPlanBindFailure::NeedsStatement);
        }
        rebuilt.map_err(CachedPlanBindFailure::rebuild_failed)?;
        self.generation = self.generation.wrapping_add(1);
        Ok(self.generation)
    }

    pub(crate) fn digest(&self) -> std::sync::Arc<std::sync::OnceLock<(String, String)>> {
        std::sync::Arc::clone(&self.digest)
    }

    pub(crate) fn execution_mut(
        &mut self,
        generation: u64,
    ) -> Option<(&tidb_ast::Stmt, &mut PhysicalPlan)> {
        (self.generation == generation).then_some((&self.statement, &mut self.physical))
    }
}

/// Why a cached plan could not be rebuilt for an execute.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CachedPlanBindFailure {
    /// The rebuild reached a deferred expression (`NOW()`) and was given no
    /// statement to evaluate it in: the entry stays, and a bind with the
    /// executing statement rebuilds it.
    NeedsStatement,
    /// Go rejects an entry whose in-place rebuild fails.
    Rejected {
        /// Go `RebuildPlan4CachedPlan`'s `rebuildRange` error, which it
        /// reports as "skip plan-cache: plan rebuild failed, %s".
        rebuild_error: Option<String>,
        /// The reason Go's ranger passed to `SetSkipPlanCache` while
        /// rebuilding: the hit is refused and the replanned statement is not
        /// cached either.
        skip_plan_cache: Option<String>,
    },
}

impl CachedPlanBindFailure {
    pub(crate) fn rebuild_failed(
        error: tidb_planner::physical_plan_cache::PlanCacheRebuildError,
    ) -> Self {
        match error {
            tidb_planner::physical_plan_cache::PlanCacheRebuildError::SkipPlanCache(reason) => {
                Self::Rejected {
                    rebuild_error: None,
                    skip_plan_cache: Some(reason),
                }
            }
            tidb_planner::physical_plan_cache::PlanCacheRebuildError::SkippedThenFailed {
                reason,
                error,
            } => Self::Rejected {
                rebuild_error: Some(error.to_string()),
                skip_plan_cache: Some(reason),
            },
            error => Self::Rejected {
                rebuild_error: Some(error.to_string()),
                skip_plan_cache: None,
            },
        }
    }

    /// The statement-binding failure, which carries no rebuild reason.
    pub(crate) const fn unbound() -> Self {
        Self::Rejected {
            rebuild_error: None,
            skip_plan_cache: None,
        }
    }

    /// Go `RebuildPlan4CachedPlan` warns and `generateNewPlan` replans. The
    /// entry stays: a cacheable new plan replaces it under the same key, an
    /// uncacheable one leaves it for the next execute. A context-free probe
    /// answers `false`, so the bind that carries the statement replans.
    pub(crate) fn replan(&self, ctx: Option<&crate::StmtContext>) -> bool {
        let (
            Self::Rejected {
                rebuild_error,
                skip_plan_cache,
            },
            Some(ctx),
        ) = (self, ctx)
        else {
            return false;
        };
        if let Some(reason) = skip_plan_cache {
            ctx.start_prepared_range_tracking();
            ctx.set_skip_plan_cache(reason.as_str());
        }
        if let Some(error) = rebuild_error {
            ctx.append_warning_parts(
                1105,
                &format!("skip plan-cache: plan rebuild failed, {error}"),
            );
        }
        true
    }
}

/// One evaluation during a cached plan's rebuild. A bound parameter or a
/// literal is its own value, so a context-free probe rebuilds the same
/// ranges the statement would. Anything else -- a deferred expression
/// (`NOW()`) or a function of a parameter (`FROM_UNIXTIME(?)`, which reads
/// the session time zone) -- is evaluated in the executing statement, and
/// the probe reports `NeedsStatement` instead of building from a value
/// computed without it.
pub(super) fn rebuild_evaluate(
    expression: &tidb_expr::expression::Expression,
    statement: Option<&crate::StmtContext>,
    parameters: &tidb_planner::physical_plan_cache::CachedPlanRebuildContext<'_>,
    needs_statement: &std::sync::atomic::AtomicBool,
) -> Result<tidb_datatype::Datum, tidb_exprctx::EvalError> {
    if let Some(statement) = statement {
        return tidb_expr::eval_expression_once(expression, statement);
    }
    match expression {
        tidb_expr::expression::Expression::Constant(constant)
            if constant.deferred_expr.is_none() =>
        {
            tidb_expr::eval_expression_once(expression, parameters)
        }
        _ => {
            needs_statement.store(true, std::sync::atomic::Ordering::Relaxed);
            Err(tidb_exprctx::EvalError::Unsupported(
                "the expression needs the executing statement",
            ))
        }
    }
}

/// The context a cached plan's rebuild evaluates a deferred expression in:
/// Go re-evaluates `NOW()` and its kin under the executing statement's own
/// session context (clock, time zone, variables) with this execute's
/// parameters. Without a statement there is no clock to read.
pub(super) fn deferred_rebuild_context(
    ctx: Option<&crate::StmtContext>,
    values: &[tidb_datatype::Datum],
) -> Option<crate::StmtContext> {
    ctx.map(|ctx| ctx.clone().with_prepared_params(std::sync::Arc::from(values)))
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
            digest: std::sync::Arc::default(),
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
    // Go `optimize` tries `TryFastPlan` before the optimizer, for a cached
    // statement too; the point plan keeps its parameter markers
    // (`HandleConstant` / `IndexConstants`) so a hit rebuilds its keys.
    let fast = match query {
        tidb_ast::QueryStmt::Select(select) => {
            super::point_get_plan::try_fast_point_physical_plan(select, catalog, current_database, ctx)
                .ok()?
        }
        tidb_ast::QueryStmt::SetOpr(_) => None,
    };
    let physical = match fast {
        Some(physical) => physical,
        None => {
            planner_physical_query(query, catalog, current_database, ctx, true)
                .ok()?
                .1
        }
    };
    if ctx.skip_plan_cache() {
        return Some((physical, false));
    }
    // Go `generateNewPlan`: an uncacheable physical plan still executes, and
    // `SetSkipPlanCache` says why.
    if let Err(reason) = tidb_planner::physical_plan_cache::plan_cacheable(&physical, cacheability) {
        ctx.set_skip_plan_cache(reason);
        return Some((physical, false));
    }
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
        tidb_stats::table_statistics::ColumnStatsUsage,
        tidb_stats::table_statistics::ColumnStatsUsage,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let session_zone = ctx.session_zone();
    let prune_fallback = dynamic_prune_fallback(catalog, ctx);
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, session_zone.clone());
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
    builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
    builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
    builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_tiflash_engine_removed_due_to_strict_sql_mode(
        ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
    );
    builder.advanced_join_hint = ctx.advanced_join_hint();
    builder.in_explain_stmt = ctx.in_explain_stmt();
    builder.parser_config = ctx.parser_config();
    builder.set_partition_processor_enabled(ctx.static_partition_prune());
    builder.set_dynamic_prune_fallback(&prune_fallback);
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
    builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
    builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
    let node = tidb_resolve::NodeW::new(query.clone());
    let plan = builder.build_query_node(&node, false)?;
    let flags = builder.get_opt_flag();
    let (plan, before) = tidb_planner_core_rule::rule_collect_plan_stats::collect_column_stats_usage(
        plan,
        ctx.opt_index_prune_threshold(),
    );
    let optimized = optimize_built_logical(
        plan,
        flags,
        catalog,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        &session_zone,
    )?;
    let (_, after) = tidb_planner_core_rule::rule_collect_plan_stats::collect_column_stats_usage(
        optimized,
        ctx.opt_index_prune_threshold(),
    );
    Ok((before, after))
}

#[cfg(test)]
mod predicate_column_tests {
    use super::*;
    use tidb_planner_core_operator_logicalop::DataSource;
    use tidb_model::TableItemID;

    /// The loaded set follows `CollectPredicateColumnsPoint`'s demand over the
    /// plan: a column the source filters on loads, and a column only a join
    /// key asked for (Go's `needFullStats` false) stays evicted, so it borrows
    /// a loaded index's analyzed row count.
    #[test]
    fn filter_columns_load_and_join_keys_do_not() {
        let mut source = DataSource::new(Default::default(), 7, "history");
        source.physical_table_id = 7;
        let statistics = tidb_stats::Table::from_analyzed_items(
            7,
            0,
            0,
            [1, 2].map(|id| (id, tidb_stats::column::Column::default())),
            [],
        );
        let item = |id| TableItemID {
            table_id: 7,
            id,
            is_index: false,
            is_sync_load_failed: false,
        };
        let demanded = std::collections::HashMap::from([(item(1), true), (item(2), false)]);
        let catalog = tidb_infoschema_perfschema::mock_catalog();
        let context = crate::StmtContext::default();
        let initializer = InitStats {
            error: None,
            catalog: &catalog,
            demanded: Some(&demanded),
            context: &context,
            record_used_stats: false,
        };
        let (columns, indexes) = initializer
            .predicate_loaded_items(&source, Some(&statistics))
            .expect("the demand names a column of the source");
        assert_eq!(columns, BTreeSet::from([1]));
        assert!(indexes.is_empty());
        // Before the demand exists, the cache's fully loaded subset stands.
        let initializer = InitStats {
            demanded: None,
            ..initializer
        };
        assert!(initializer
            .predicate_loaded_items(&source, Some(&statistics))
            .is_none());
    }
}

#[cfg(test)]
mod statistics_initialization_tests {
    use super::*;
    use tidb_planner_property::stats_info::HistCollExt as _;
    use tidb_table::kv_table::KvTable;
    use tidb_table::kv_table::KvIndex;
    use tidb_table::kv_table::KvColumn;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::expression::Expression;
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_expr::schema::Schema;
    use tidb_planner_core_operator_logicalop::DataSource;

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
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let table_info = table.meta();
        catalog.register_kv("t", table);
        let context = crate::StmtContext::default();
        context.stats_handle().set_table_statistics(
            7,
            std::sync::Arc::new(tidb_stats::Table::from_analyzed_items(7, 1000, 0, [], [])),
        );
        let mut source = DataSource::new(Default::default(), 7, "t");
        // Go `ds.TableInfo`.
        source.table_info = Some(table_info);
        source.db_name = "test".to_owned();
        source.physical_table_id = 7;
        source.derived_index_paths.insert(
            99,
            tidb_planner_util::path::IndexPathState {
                row_estimate: Some(
                    tidb_stats::RowEstimate::new(
                        900.0, 800.0, 1000.0,
                    ),
                ),
                is_single_scan: Some(true),
                ..Default::default()
            },
        );
        source.derived_index_paths.insert(
            77,
            tidb_planner_util::path::IndexPathState {
                declared_columns: Some(vec![Some((
                    Column::new(1, FieldType::new(FieldTypeCode::LongLong)),
                    -1,
                ))]),
                row_estimate: Some(
                    tidb_stats::RowEstimate::new(
                        900.0, 800.0, 1000.0,
                    ),
                ),
                is_single_scan: Some(true),
                ..Default::default()
            },
        );
        source.derived_access_paths = Some(tidb_planner_util::path::DerivedAccessPaths {
            table_path: None,
            paths: Vec::new(),
            min_selectivity: 0.01,
            index_merge_max_count: None,
        });

        let mut initializer = InitStats {
            error: None,
            catalog: &catalog,
            demanded: None,
            context: &context,
            record_used_stats: true,
        };
        let (plan, ()) = fold_owned(&mut initializer, LogicalPlan::DataSource(source), ());
        let LogicalPlan::DataSource(returned) = plan else {
            panic!("lost datasource");
        };
        assert!(returned.derived_access_paths.is_none(), "statistics refresh invalidates candidate derivation");
        assert!(initializer.error.is_none(), "{:?}", initializer.error);
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
                primary: false,
            },
            false,
        );
        for id in [10, 11] {
            let mut index = table.indexes()[0].clone();
            index.id = id;
            index.name = format!("ia_{id}");
            table.add_index(index, false);
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let table_info = table.meta();
        catalog.register_kv("t", table);
        let mut source = DataSource::new(Default::default(), 7, "t");
        // Go `ds.TableInfo`.
        source.table_info = Some(table_info.clone());
        source.db_name = "test".to_owned();
        source.columns = vec![tidb_model::GoShared::new({
                let info = tidb_model::ColumnInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("a"),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                };
                info
            })];
        let mut column = Column::new(1, field_type.clone());
        column.id = 1;
        column.index = 0;
        source
            .base
            .base
            .set_schema(Some(Schema::new(vec![column.clone()])));
        source.indexes = vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
            id: 9,
            name: tidb_ast::CiString::new("ia"),
            columns: vec![tidb_model::IndexColumn {
                name: tidb_ast::CiString::new("a"),
                offset: 0,
                length: -1,
                ..tidb_model::IndexColumn::default()
            }].into(),
            state: tidb_model::SchemaState::PUBLIC,
            ..tidb_model::IndexInfo::default()
        })];

        for id in [10, 11] {
            let index = tidb_model::GoShared::new(source.indexes[0].read().clone_like_go());
            index.write().id = id;
            index.write().name = tidb_ast::CiString::new(format!("ia_{id}"));
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
        let column_stats = tidb_stats::column::Column {
            histogram: histogram.clone(),
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            ..Default::default()
        };
        let index_stats = tidb_stats::index::Index {
            histogram: tidb_stats::Histogram { id: 9, ..histogram },
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                columns: (vec![String::new(); 1])
                    .into_iter()
                    .map(|name: String| tidb_model::IndexColumn {
                        name: tidb_ast::CiString::new(name),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>()
                    .into(),
                ..tidb_model::IndexInfo::default()
            })),
            ..Default::default()
        };
        let mut empty_index = index_stats.clone();
        empty_index.histogram = tidb_stats::Histogram { id: 11, ..Default::default() };
        let mut statistics = tidb_stats::Table::from_analyzed_items(
            7,
            100,
            0,
            [(1, column_stats)],
            [(9, index_stats), (11, empty_index)],
        );
        let evicted = tidb_stats::StatsLoadedStatus::all_evicted();
        statistics.hist_coll.get_column(1).unwrap().write().unwrap().stats_loaded_status = evicted;
        statistics.hist_coll.get_index(9).unwrap().write().unwrap().stats_loaded_status = evicted;
        // Index 11 is an unloaded placeholder that storage never recorded.
        statistics.hist_coll.get_index(11).unwrap().write().unwrap().stats_loaded_status =
            tidb_stats::StatsLoadedStatus::default();
        statistics.existence_map().write().unwrap().delete_index_not_found(11);
        for ndv in [2, 0] {
            let published = statistics.copy_as(tidb_stats::CopyIntent::AllDataWritable);
            let cache_usable = {
                let column = published.hist_coll.get_column(1).unwrap();
                let mut column = column.write().unwrap();
                column.histogram.ndv = ndv;
                // Go `ColumnStatsIsInvalid`'s estimation gate.
                let essential = column.stats_loaded_status.is_essential_stats_loaded();
                column.is_valid_for_estimation(published.hist_coll.pseudo, essential)
            };
            statistics = published;
            let context = crate::StmtContext::default();
            context
                .stats_handle()
                .set_table_statistics(7, std::sync::Arc::new(statistics.copy_as(
                    tidb_stats::CopyIntent::MetaOnly,
                )));
            let mut initializer = InitStats {
                error: None,
                catalog: &catalog,
                demanded: None,
                context: &context,
                record_used_stats: true,
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
            assert_eq!(
                hist.index_histogram(11).unwrap().read().unwrap().total_row_count(),
                0.0
            );
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
                tidb_planner_cardinality::row_count_estimator::get_row_count_by_column_ranges(
                    hist.histogram_for_estimation(column.unique_id)
                        .as_ref()
                        .map(|column| column.read().unwrap())
                        .as_deref(),
                    &[tidb_planner_cardinality::row_count_estimator::ColumnRange::point(Datum::Null)],
                    tidb_charset::Collation::Binary,
                    hist.realtime_count,
                    hist.modify_count,
                    false,
                    &Default::default(),
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
                primary: false,
            },
            false,
        );
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let table_info = table.meta();
        catalog.register_kv("t", table);
        let mut source = DataSource::new(Default::default(), 7, "t");
        // Go `ds.TableInfo`.
        source.table_info = Some(table_info.clone());
        source.db_name = "test".to_owned();
        source.columns = vec![tidb_model::GoShared::new({
                let info = tidb_model::ColumnInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("a"),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                };
                info
            })];
        let mut column = Column::new(1, field_type.clone());
        column.id = 1;
        column.index = 0;
        source
            .base
            .base
            .set_schema(Some(Schema::new(vec![column.clone()])));
        source.indexes = vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
            id: 9,
            name: tidb_ast::CiString::new("ia"),
            columns: vec![tidb_model::IndexColumn {
                name: tidb_ast::CiString::new("a"),
                offset: 0,
                length: -1,
                ..tidb_model::IndexColumn::default()
            }].into(),
            state: tidb_model::SchemaState::PUBLIC,
            ..tidb_model::IndexInfo::default()
        })];
        source.enumerated_paths = vec![tidb_planner_util::path::PossiblePath::Index { index: 0 }];
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
                    let comparison_ranges = tidb_ranger::detacher::detach_index_range_with_fallback_handler_in(
                        std::slice::from_ref(&comparison), std::slice::from_ref(&column), &[-1],
                        context.range_max_size(), context.range_fallback_handler(), &evaluate,
                        context.ranger_options(),
                    ).unwrap();
                    assert!(comparison_ranges.ranges.is_empty());
                }
                let direct =
                    tidb_ranger::detacher::detach_index_range_with_fallback_handler_in(
                        &source.pushed_down_conds,
                        &[column],
                        &[-1],
                        context.range_max_size(),
                        context.range_fallback_handler(),
                        &evaluate,
                        context.ranger_options(),
                    );
                assert_eq!(direct.is_err(), !bound, "direct result: {direct:?}");
                let expected_error = direct.err().map(|error| match error {
                    tidb_ranger::points::PointBuilderError::Eval(error) => error.into(),
                    tidb_ranger::points::PointBuilderError::Unsupported(message) => {
                        assert!(message.ends_with("is not evaluated"));
                        tidb_planner_core_base::PlanError::from(
                            tidb_plannererrors::ERR_UNSUPPORTED_TYPE
                                .generate_with_stack(message),
                        )
                    }
                    other => panic!("unexpected error: {other:?}"),
                });
                // Go builds the ranges and the row count in `fillIndexPath`,
                // which the planner's statistics derivation runs.
                let plan_ids = PlanIdAllocator::new();
                let column_ids = ColumnIdAllocator::new();
                let optimized = optimize_built_logical(
                    LogicalPlan::DataSource(source.clone()),
                    0,
                    &catalog,
                    &context,
                    false,
                    &plan_ids,
                    &column_ids,
                    &tidb_datatype::SessionTimeZone::utc(),
                );
                if bound {
                    let plan = optimized.expect("bound parameter plans");
                    let LogicalPlan::DataSource(returned) = plan else {
                        panic!("lost datasource");
                    };
                    assert_eq!(returned.table_id, 7);
                    assert!(returned.derived_index_paths.get(&9).and_then(|path| path.count_after_access()).is_some());
                } else {
                    assert_eq!(optimized.err(), expected_error.clone());
                    if let Some(error) = expected_error {
                        if error.equal(&tidb_plannererrors::ERR_UNSUPPORTED_TYPE) {
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
                }
            }
        }
    }
}



#[cfg(test)]
mod subquery_error_identity_tests {
    use super::driver_error_to_plan;

    /// Go's `EvalSubqueryFirstRow` returns the executor's error as it is, so
    /// any Go error a planning-time subquery raises -- here a recursive CTE
    /// that never stops -- reaches the client with its own code, not 1105.
    #[test]
    fn a_planning_time_subquery_keeps_the_go_error_it_raised() {
        let raised = tidb_exeerrors::ERR_CTE_MAX_RECURSION_DEPTH
            .generate_with_stack_by_args(&[1000_i64.into()]);
        let planned = driver_error_to_plan(crate::DriverError::from(raised.clone()));
        assert!(planned.equal(&tidb_exeerrors::ERR_CTE_MAX_RECURSION_DEPTH));
        let crate::DriverError::Mysql(back) = crate::driver::planner_error_to_driver(planned)
        else {
            panic!("the Go error was flattened");
        };
        assert_eq!(back.code, 3636);
        assert_eq!(
            back.message,
            tidb_terror::MysqlError::from_terror(&raised).message
        );
    }
}
