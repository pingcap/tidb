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
use std::collections::HashSet;
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
            "and" => locate_list_columns_cnf(partition, function.get_args(), columns),
            "or" => locate_list_columns_dnf(partition, function.get_args(), columns),
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
                    tidb_planner::ranger::detacher::detach_cond_and_build_range_for_partition(
                        std::slice::from_ref(condition),
                        std::slice::from_ref(&columns[column_index]),
                        &[tidb_datatype::UNSPECIFIED_LENGTH],
                        0,
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
) -> Result<ListColumnsLocated, tidb_planner::plan_base::PlanError> {
    let mut location = None;
    for condition in conditions {
        match locate_list_columns_condition(partition, condition, columns)? {
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
) -> Result<ListColumnsLocated, tidb_planner::plan_base::PlanError> {
    if conditions.is_empty() {
        return Ok(ListColumnsLocated::Full);
    }
    let mut location = crate::partition_pruning::ListPartitionLocation::new();
    for condition in conditions {
        match locate_list_columns_condition(partition, condition, columns)? {
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
) -> Result<Option<Vec<i64>>, tidb_planner::plan_base::PlanError> {
    Ok(
        match locate_list_columns_cnf(partition, conditions, columns)? {
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
        if let Some(ids) = list_columns_pruned_ids(partition, &conditions, &columns)? {
            surviving.retain(|index| ids.contains(&partition.definitions[*index].id));
        }
        return Ok(remap_partition_indices(
            partition,
            &source.partition_names,
            surviving,
        ));
    }
    let lengths = vec![tidb_datatype::UNSPECIFIED_LENGTH; columns.len()];
    let Ok(detached) = tidb_planner::ranger::detacher::detach_cond_and_build_range_for_partition(
        &conditions,
        &columns,
        &lengths,
        0,
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

        assert_eq!(
            list_columns_pruned_ids(&spec, &[equals(&columns[1], 7)], &columns).unwrap(),
            Some(vec![502, 503]),
            "a predicate on the second partition column is prunable; Go also retains DEFAULT"
        );
        assert_eq!(
            list_columns_pruned_ids(
                &spec,
                &[equals(&columns[0], 1), equals(&columns[1], 9)],
                &columns,
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

struct InitStats<'a> {
    catalog: &'a Catalog,
    select: Option<&'a tidb_ast::SelectStmt>,
    default_string_match_selectivity: f64,
    enable_pseudo_for_outdated_stats: bool,
    zone: &'a tidb_datatype::SessionTimeZone,
}

impl OwnedRewrite for InitStats<'_> {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): Self::Down) -> Descend<Self::Down, Self::Up> {
        let LogicalPlan::DataSource(source) = node else {
            return Descend::Children(vec![(); node.children().len()]);
        };
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
        let row_count = crate::access_cost::realtime_row_count(statistics);
        let loaded_columns = statistics
            .map(|statistics| statistics.columns.keys().copied().collect())
            .unwrap_or_default();
        let loaded_indexes = statistics
            .map(|statistics| statistics.indexes.keys().copied().collect())
            .unwrap_or_default();
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
        source.table_stats = Some(
            StatsInfo::new(row_count, ndvs)
                .with_hist_coll(HistColl::new(
                    statistics.is_none_or(|statistics| statistics.pseudo),
                    row_count as i64,
                    row_size_columns,
                ))
                .with_stats_version(statistics.map_or(tidb_stats::PSEUDO_VERSION, |s| s.version)),
        );
        if let (Some(_), Some(predicate), Some(TableEntry::Kv(table)), Some(table_stats)) = (
            self.select,
            self.select.and_then(|select| select.where_clause.as_ref()),
            self.catalog.get_in(&source.db_name, &source.table_name),
            source.table_stats.clone(),
        ) {
            source.table_path_count_after_access =
                crate::handle_range::build_handle_ranges(table, predicate, self.zone)
                    .map(|built| {
                        crate::handle_range::handle_range_row_count(
                            table,
                            &built.ranges,
                            statistics,
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
                    &columns, predicate, self.zone,
                ) else {
                    continue;
                };
                source.index_path_count_after_access.insert(
                    index.id,
                    crate::access_cost::index_range_row_count(
                        index,
                        table,
                        &built.ranges,
                        statistics,
                        row_count,
                    ),
                );
            }
            let visible = source
                .table_as_name
                .as_deref()
                .unwrap_or(&source.table_name);
            let scope = super::from::single_table_scope(
                visible,
                Some(source.db_name.clone()),
                table
                    .visible_columns()
                    .iter()
                    .map(|column| (column.name.clone(), column.field_type.clone()))
                    .collect(),
            );
            let selectivity = crate::access_cost::selectivity_with_default_string_match_selectivity(
                predicate,
                table,
                &crate::driver::from::scope_resolver(&scope),
                statistics,
                self.default_string_match_selectivity,
            );
            source.base.base.set_stats(Some(table_stats.scale(
                selectivity,
                tidb_planner::cardinality::derive_stats::DEF_SCALE_NDV_SKEW_RATIO,
            )));
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
        Descend::Stop(())
    }

    fn ascend(&mut self, node: LogicalPlan, _children: Vec<Self::Up>) -> (LogicalPlan, Self::Up) {
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
    let select_hint = match query {
        tidb_ast::QueryStmt::Select(select) => Some(select.as_ref()),
        tidb_ast::QueryStmt::SetOpr(_) => None,
    };
    let logical = planner_optimized_query_with_allocators(
        query,
        select_hint,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
    )?;
    let physical = physical_plan_for_logical(&logical, plan_ids, column_ids, ctx)?;
    Ok((logical, physical))
}

pub(crate) fn physical_plan_for_logical(
    logical: &LogicalPlan,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    ctx: &crate::StmtContext,
) -> Result<PhysicalPlan, tidb_planner::plan_base::PlanError> {
    let coster = Ver2Coster::from_env(ctx.optimizer_cost_env());
    let mut dispatch = DispatchContext::new(plan_ids, &coster, 1.0)
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
        .with_column_ids(column_ids);
    let task = find_best_task(logical, &PhysicalProperty::default(), &mut dispatch)?;
    let physical = task.plan().cloned().ok_or_else(|| {
        tidb_planner::plan_base::PlanError::internal("physical planning produced no plan")
    })?;
    let mut physical = tidb_planner::physical::eliminate_physical_projection(physical);
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
            catalog,
            select: None,
            default_string_match_selectivity: ctx.default_string_match_selectivity(),
            enable_pseudo_for_outdated_stats: ctx.enable_pseudo_for_outdated_stats(),
            zone,
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
    optimized.recursive_derive_stats(&[])?;
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
    let logical = planner_optimized_query_with_allocators(
        query,
        select_hint,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        &plan_ids,
        &column_ids,
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
) -> Result<LogicalPlan, tidb_planner::plan_base::PlanError> {
    let source = catalog.planner_catalog(current_database, ctx.latest_index_schema());
    let session_zone = ctx.session_zone();
    let mut builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, session_zone.clone());
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
        plan_cache_marker: Some(ctx),
        allow_derive_topn: true,
        disabled_rules: DisabledLogicalRules::default(),
        statistics_load: Some(&statistics_load),
        partition_pruning: Some(&partition_pruning),
        opt_index_prune_threshold: ctx.opt_index_prune_threshold(),
        always_keep_join_key: ctx.always_keep_join_key(),
        enable_unsafe_substitute: ctx.enable_unsafe_substitute(),
        enable_semi_join_rewrite: ctx.enable_semi_join_rewrite(),
        enable_no_decorrelate_in_select: ctx.enable_no_decorrelate_in_select(),
        join_reorder_threshold: ctx.join_reorder_threshold(),
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
    let (plan, ()) = fold_owned(
        &mut InitStats {
            catalog,
            select: (source_count == 1).then_some(select_hint).flatten(),
            default_string_match_selectivity: ctx.default_string_match_selectivity(),
            enable_pseudo_for_outdated_stats: ctx.enable_pseudo_for_outdated_stats(),
            zone: session_zone,
        },
        plan,
        (),
    );
    let mut optimized = logical_optimize(&rule_context, flags, plan)
        .map_err(|(_, error)| error)?
        .plan;
    optimized = check_partial_index_paths(optimized, ctx, use_plan_cache);
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
    optimized.recursive_derive_stats(&[])?;
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
        self.physical
            .rebuild_plan_for_cache_in_place(
                &tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values),
            )
            .ok()?;
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
) -> Option<CachedSelectPlan> {
    let physical = cached_physical_query_plan(query, catalog, current_database, ctx, cacheability)?;
    Some(CachedSelectPlan {
        statement: tidb_ast::Stmt::Query(tidb_ast::NodeBox::new(query.clone())),
        physical,
        generation: 0,
    })
}

/// Builds and admits the physical source tree shared by cached queries and
/// cached DML roots.
pub(crate) fn cached_physical_query_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    cacheability: tidb_planner::physical_plan_cache::PlanCacheabilityContext,
) -> Option<PhysicalPlan> {
    if matches!(query, tidb_ast::QueryStmt::Select(select) if select.rollup) {
        return None;
    }
    let (_, physical) = planner_physical_query(query, catalog, current_database, ctx, true).ok()?;
    if ctx.skip_plan_cache() {
        return None;
    }
    tidb_planner::physical_plan_cache::plan_cacheable(&physical, cacheability).ok()?;
    Some(physical)
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
