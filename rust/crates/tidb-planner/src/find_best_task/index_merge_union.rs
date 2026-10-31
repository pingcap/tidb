// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Physical property convergence and cop-task conversion for ordinary OR
//! alternatives derived by `access_path::index_merge`.

use crate::access_path::index_merge::compare_alternatives;

use tidb_planner_util::path::{Partial, UnionIndexMergePath};
use crate::find_best_task::dispatch::DispatchContext;
use tidb_planner_core_operator_logicalop::DataSource;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_physicalop::task::Task;
use tidb_expr::expression::Expression;
use tidb_planner_cardinality::derive_stats::StatsInfoScale;

#[cfg(test)]
/// Go `generateIndexMergePath`'s union candidate for a DataSource whose
/// pushed conditions are exactly one top-level OR. Returns `None` when the
/// form does not apply or any disjunct lacks a range-producing partial.
pub fn build_union_index_merge_task(
    ds: &DataSource,
    ctx: &mut DispatchContext<'_>,
) -> Result<Option<Task>, PlanError> {
    crate::access_path::index_merge::prepare_union_index_merge_path(ds, &ctx.access_path_derivation_context())?
        .map(|path| {
            build_prepared_union_index_merge_task(
                ds,
                &path,
                &tidb_planner_property::PhysicalProperty::default(),
                ctx,
            )
        })
        .transpose()
}

/// Go `matchProperty` over one partial path: `None` when it cannot deliver
/// the order, else the group-by columns of its merge sort (empty when the
/// path is ordered outright).
fn partial_matches_order(
    ds: &DataSource,
    partial: &Partial,
    items: &[tidb_planner_property::SortItem],
) -> Option<Vec<usize>> {
    let first = items.first()?;
    if items.iter().any(|item| item.desc != first.desc) {
        return None;
    }
    // Go `matchProperty` reads the partial's plan-time ranges. A partial
    // whose own access conditions are correlated has its ranges rebuilt per
    // outer row (in Go they are full ranges at plan time), so it keeps its
    // order only outright; a correlated condition elsewhere (a table filter,
    // an index filter on top of constant ranges) leaves the range groups
    // valid, as in Go.
    let allow_groups = |detached: &tidb_ranger::detacher::DetachRangeResult| {
        !tidb_expr::expr_util::predicates::contain_correlated_column(&detached.access_conds)
    };
    match partial {
        // Go `isMatchPropForIndexMerge`: every MV partial path must match,
        // its equality on the array value fixing that key position.
        Partial::MvIndex {
            index_pos, paths, ..
        } => paths
            .iter()
            .all(|path| {
                index_partial_matches_order(ds, *index_pos, path.filled(), items, false)
                    .is_some()
            })
            .then(Vec::new),
        // Go `matchProperty` over a table path: the integer handle alone, or
        // a clustered common handle's key columns (its `IdxCols`) after the
        // prefix its ranges pin.
        Partial::Table { .. } if ds.handle_is_int => (items.len() == 1
            && ds
                .handle_cols
                .first()
                .is_some_and(|column| column.unique_id == first.col.unique_id))
        .then(Vec::new),
        Partial::Table { filled, .. } => {
            if !ds.is_common_handle || ds.force_no_keep_order_table_path {
                return None;
            }
            key_columns_match_order(
                &filled.common_columns,
                &filled.detached.ranges,
                items,
                allow_groups(&filled.detached),
            )
        }
        Partial::Index {
            index_pos, filled, ..
        } => index_partial_matches_order(
            ds,
            *index_pos,
            filled,
            items,
            allow_groups(&filled.detached),
        ),
    }
}

/// Go `isMatchProp` over one index partial path: the sort columns follow
/// the key after a prefix every range pins to one value.
fn index_partial_matches_order(
    ds: &DataSource,
    index_pos: usize,
    filled: &tidb_planner_util::path::FilledIndexPath,
    items: &[tidb_planner_property::SortItem],
    allow_groups: bool,
) -> Option<Vec<usize>> {
    let index = &ds.indexes[index_pos];
    if ds.force_no_keep_order_index_ids.contains(&index.read().id) {
        return None;
    }
    // Match the same normalized key layout used by ordinary ranges,
    // including eligible appended handles after fixed declared keys.
    key_columns_match_order(&filled.columns, &filled.detached.ranges, items, allow_groups)
}

/// Go `isMatchProp` over one partial path's key columns: each sort column
/// is a full-length key column reached after columns every range pins to a
/// single value (Case 2) or to one value per range (Case 3, the group-by
/// columns of a merge sort over the range groups).
fn key_columns_match_order(
    columns: &[(tidb_expr::column::Column, i64)],
    ranges: &[tidb_ranger::types::Range],
    items: &[tidb_planner_property::SortItem],
    allow_groups: bool,
) -> Option<Vec<usize>> {
    let mut offset = 0;
    let mut group_by_col_idxs = Vec::new();
    for item in items {
        let mut found = false;
        while let Some((column, length)) = columns.get(offset) {
            let position = offset;
            offset += 1;
            let full_length = *length < 0
                || column
                    .ret_type
                    .as_ref()
                    .is_some_and(|ty| ty.flen() == *length);
            if full_length && column.unique_id == item.col.unique_id {
                found = true;
                break;
            }
            let fixed = ranges.first().and_then(|range| range.low_val.get(position));
            if fixed.is_some_and(|value| {
                ranges.iter().all(|range| {
                    range.low_val.get(position) == Some(value)
                        && range.high_val.get(position) == Some(value)
                })
            }) {
                continue;
            }
            let point_per_range = !ranges.is_empty()
                && ranges.iter().all(|range| {
                    range.low_val.len() > position
                        && range.high_val.len() > position
                        && range.low_val[position]
                            .compare(&range.high_val[position], range.collators[position])
                            .is_ok_and(|order| order == std::cmp::Ordering::Equal)
                });
            if !(allow_groups && point_per_range) {
                return None;
            }
            group_by_col_idxs.push(position);
        }
        if !found {
            return None;
        }
    }
    if !group_by_col_idxs.is_empty()
        && super::dispatch::range_group_count(ranges, &group_by_col_idxs)? == 0
    {
        group_by_col_idxs.clear();
    }
    Some(group_by_col_idxs)
}

#[cfg(test)]
pub(super) fn build_prepared_union_index_merge_task(
    ds: &DataSource,
    path: &UnionIndexMergePath,
    prop: &tidb_planner_property::PhysicalProperty,
    ctx: &DispatchContext<'_>,
) -> Result<Task, PlanError> {
    let Some(path) = converge_union_index_merge_path(ds, path, prop, ctx) else {
        return Ok(Task::invalid_task());
    };
    build_converged_union_index_merge_task(ds, &path, ctx)
}

pub(super) fn converge_union_index_merge_path<'a>(
    ds: &DataSource,
    path: &'a UnionIndexMergePath,
    prop: &tidb_planner_property::PhysicalProperty,
    ctx: &DispatchContext<'_>,
) -> Option<ConvergedUnionPath<'a>> {
    use tidb_planner_property::PhysicalPropMatchResult;
    let advisory = prop.sort_items.is_empty()
        && !prop.advisory_sort_items.is_empty()
        && prop
            .advisory_sort_items
            .iter()
            .all(|item| item.desc == prop.advisory_sort_items[0].desc);
    let sort_items = if advisory {
        &prop.advisory_sort_items
    } else {
        &prop.sort_items
    };
    let mut partials = Vec::with_capacity(path.alternatives.len());
    let mut partial_matches = Vec::with_capacity(path.alternatives.len());
    let mut partial_groups = Vec::with_capacity(path.alternatives.len());
    for branch in &path.alternatives {
        let matched = branch
            .iter()
            .filter_map(|partial| {
                partial_matches_order(ds, partial, sort_items).map(|groups| (partial, groups))
            })
            .min_by(|left, right| compare_alternatives(ds, left.0, right.0, true));
        let (partial, result, groups) = if let Some((partial, groups)) = matched {
            let result = if groups.is_empty() {
                PhysicalPropMatchResult::Matched
            } else {
                PhysicalPropMatchResult::MatchedNeedMergeSort
            };
            (partial, result, groups)
        } else {
            if !prop.sort_items.is_empty() {
                return None;
            }
            (
                branch
                    .iter()
                    .min_by(|left, right| compare_alternatives(ds, left, right, true))
                    .expect("prepared branches are nonempty"),
                PhysicalPropMatchResult::NotMatched,
                Vec::new(),
            )
        };
        partials.push(partial);
        partial_matches.push(result);
        partial_groups.push(groups);
    }
    let desc = sort_items.first().is_some_and(|item| item.desc);
    let total_rows = crate::access_path::index_merge::estimate_union_access(
        ds,
        &partials,
        &ctx.access_path_derivation_context(),
    );
    let index_id = |partial: &Partial| match partial {
        Partial::Index { index_pos, .. } | Partial::MvIndex { index_pos, .. } => {
            ds.indexes[*index_pos].read().id
        }
        Partial::Table { .. } => -1,
    };
    if ds.index_merge_hints.is_empty()
        && partials.first().is_some_and(|first| {
            partials
                .iter()
                .all(|partial| index_id(partial) == index_id(first))
        })
    {
        return None;
    }
    let mut table_filters = path
        .table_filters
        .iter()
        .filter(|filter| {
            if tidb_expr::expr_util::maybe_over_optimized_4_plan_cache(
                path.use_plan_cache,
                std::slice::from_ref(*filter),
            ) {
                return true;
            }
            !partials.iter().all(|partial| match partial {
                Partial::Index {
                    index_pos, filled, ..
                } => {
                    tidb_planner_core_operator_logicalop::data_source::index_covers_condition(
                        ds,
                        &ds.indexes[*index_pos],
                        filter,
                        ctx.opt_prefix_index_single_scan,
                    ) && !filled.table_filters.iter().any(|condition| {
                        condition.canonical_hash_code() == filter.canonical_hash_code()
                    }) && filled.detached.access_conds.iter().any(|condition| {
                        condition.canonical_hash_code() == filter.canonical_hash_code()
                    })
                }
                // Go `indexMergePartialPathCoversFilter`: "Be conservative: only
                // remove a filter when a normal index partial path carries the
                // exact predicate in AccessConds. Filters handled by table/MV
                // paths ... still need the global Selection."
                Partial::MvIndex { .. } | Partial::Table { .. } => false,
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    if tidb_expr::expr_util::maybe_over_optimized_4_plan_cache(
        path.use_plan_cache,
        std::slice::from_ref(&path.source_filter),
    ) || partials.iter().any(|partial| {
        matches!(
            partial,
            Partial::Index {
                keep_source_filter: true,
                ..
            } | Partial::Table {
                keep_source_filter: true,
                ..
            } | Partial::MvIndex {
                keep_source_filter: true,
                ..
            }
        )
    }) {
        table_filters.push(path.source_filter.clone());
    }
    Some(ConvergedUnionPath {
        partials,
        partial_matches,
        partial_groups,
        advisory,
        desc,
        total_rows,
        table_filters,
        expected_cnt: prop.expected_cnt,
        by_items: prop
            .sort_items
            .iter()
            .map(|item| {
                let column = ds
                    .table_columns
                    .iter()
                    .find(|column| column.unique_id == item.col.unique_id)
                    .unwrap_or(&item.col);
                tidb_expression_aggregation::ByItems {
                    expr: Expression::Column(column.clone()),
                    desc: item.desc,
                }
            })
            .collect(),
    })
}

impl<'a> ConvergedUnionPath<'a> {
    /// The converged partial accesses, in OR-list order.
    pub(super) fn partials(&self) -> &[&'a Partial] {
        &self.partials
    }
}

pub(super) struct ConvergedUnionPath<'a> {
    partials: Vec<&'a Partial>,
    partial_matches: Vec<tidb_planner_property::PhysicalPropMatchResult>,
    /// Go `GroupByColIdxs` of each partial path, empty when it is ordered
    /// outright or unordered.
    partial_groups: Vec<Vec<usize>>,
    advisory: bool,
    desc: bool,
    total_rows: f64,
    table_filters: Vec<Expression>,
    expected_cnt: f64,
    by_items: Vec<tidb_expression_aggregation::ByItems>,
}

// Go's partial scans and merge table probe scale the original table profile,
// just like ordinary scans. Keeping HistColl is essential: its presence changes
// row-size costing even for pseudo statistics.
pub(super) fn merge_scan_stats(
    ds: &DataSource,
    rows: f64,
    ctx: &DispatchContext<'_>,
) -> tidb_planner_property::stats_info::StatsInfo {
    ds.table_stats
        .as_ref()
        .map(|stats| stats.scale_by_expect_cnt(rows, ctx.skew_ratio))
        .unwrap_or_else(|| tidb_planner_property::stats_info::StatsInfo::new(rows, []))
}

pub(super) fn build_converged_union_index_merge_task(
    ds: &DataSource,
    path: &ConvergedUnionPath<'_>,
    ctx: &DispatchContext<'_>,
) -> Result<Task, PlanError> {
    let ConvergedUnionPath {
        partials,
        partial_matches,
        partial_groups,
        advisory,
        desc,
        total_rows,
        table_filters,
        expected_cnt,
        by_items,
    } = path;
    let (advisory, desc, mut total_rows) = (*advisory, *desc, *total_rows);
    let datasource_rows = ds.base.base.stats_info().map(|stats| stats.row_count());
    if let Some(rows) = datasource_rows {
        if expected_cnt + tidb_planner_core_cost::TOLERANCE_FACTOR < rows {
            total_rows *= expected_cnt / rows;
        }
    }
    // Expected-count scaling uses the selected partial's retained filters,
    // just as ordinary index scans do.
    let partial_rows = |rows, matched: bool, table: bool, has_filters: bool| {
        let Some(datasource_rows) = datasource_rows else {
            return rows;
        };
        let min_selectivity = ds
            .derived_access_paths
            .as_ref()
            .map_or(ds.access_path_min_selectivity, |paths| {
                paths.min_selectivity
            });
        if !table
            && super::dispatch::ignore_index_scan_expected_count(
                ctx.ordering_index_selectivity_threshold,
                min_selectivity,
                has_filters,
            )
        {
            return rows;
        }
        let mut adjusted = super::dispatch::adjust_index_scan_count_by_expected(
            rows,
            *expected_cnt,
            datasource_rows,
            has_filters && !table,
            ctx.ordering_index_selectivity_ratio,
        );
        if table && matched && ctx.ordering_index_selectivity_ratio > 0.0 {
            adjusted += (rows - adjusted).max(0.0) * ctx.ordering_index_selectivity_ratio;
        }
        adjusted
    };
    let mut partial_plans_raw = Vec::with_capacity(partials.len());
    // Go `globalRemainingFilters`: each partial's unpushable conditions, in
    // partial order, ahead of the table scan's own.
    let mut partial_remaining = Vec::new();
    for ((partial, matched), groups) in partials
        .iter()
        .copied()
        .zip(partial_matches)
        .zip(partial_groups)
    {
        match partial {
            Partial::Index {
                index_pos,
                filled,
                rows,
                ..
            } => {
                let scan_rows =
                    partial_rows(*rows, matched.matched(), false, !filled.index_filters.is_empty());
                partial_remaining.extend(push_index_partial(
                    ds,
                    ctx,
                    *index_pos,
                    filled,
                    *rows,
                    scan_rows,
                    matched.matched(),
                    desc,
                    groups,
                    &mut partial_plans_raw,
                )?);
            }
            // Go builds one partial scan per path of the MV alternative.
            Partial::MvIndex {
                index_pos, paths, ..
            } => {
                for path in paths {
                    let filled = path.filled();
                    let scan_rows = partial_rows(
                        path.rows(),
                        matched.matched(),
                        false,
                        !filled.index_filters.is_empty(),
                    );
                    partial_remaining.extend(push_index_partial(
                        ds,
                        ctx,
                        *index_pos,
                        filled,
                        path.rows(),
                        scan_rows,
                        matched.matched(),
                        desc,
                        &[],
                        &mut partial_plans_raw,
                    )?);
                }
            }
            Partial::Table { filled, rows, .. } => {
                let detached = &filled.detached;
                let ranges = &detached.ranges;
                let mut schema_columns = partial_handle_columns(ds, ctx)?;
                // Go `convertToPartialTableScan`: the partition id rides
                // along an ordered partial of a dynamically pruned table.
                if matched.matched() && keeps_phys_tbl_id(ds) {
                    super::dispatch::add_extra_phys_tbl_id_column(&mut schema_columns, ctx);
                }
                // Go PhysicalTableScan.TP: IsFullScan holds for no ranges too.
                let unsigned_int_handle = ds.pk_is_handle && filled.handle_type.is_unsigned();
                let scan_kind = if ranges
                    .iter()
                    .all(|range| range.is_full_range(unsigned_int_handle))
                {
                    tidb_planner_core_operator_logicalop::data_source_paths::ResolvedTableScanKind::Full
                } else {
                    tidb_planner_core_operator_logicalop::data_source_paths::ResolvedTableScanKind::Range
                };
                let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                    ctx.allocator,
                    scan_kind.plan_type(),
                    ds.base.base.query_block_offset(),
                );
                base.base.set_stats(Some(merge_scan_stats(
                    ds,
                    partial_rows(
                        *rows,
                        matched.matched(),
                        true,
                        !detached.remained_conds.is_empty(),
                    ),
                    ctx,
                )));
                base.base
                    .set_schema(Some(tidb_expr::schema::Schema::new(schema_columns)));
                partial_plans_raw.push(PhysicalPlan::TableScan(
                    tidb_planner_core_operator_physicalop::PhysicalTableScan {
                        by_items: Vec::new(),
                        group_by_col_idxs: groups.clone(),
                        base,
                        table_id: ds.physical_table_id,
                        table_as_name: ds.table_as_name.clone(),
                        dynamic_partition_access: ds.dynamic_partition_access.clone(),
                        cost_columns: ds.table_columns.clone(),
                        store_type: tidb_kv::store_type::StoreType::TiKv,
                        keep_order: matched.matched(),
                        desc,
                        ranges: ranges.clone(),
                        range_rebuild: partial_table_range_rebuild(ds, filled),
                        table_scan_penalty: ds.table_scan_penalty,
                        tikv_pushdown: None,
                        db_name: ds.db_name.clone(),
                        table_name: ds.table_name.clone(),
                        handle_col_unique_id: ds.handle_cols.first().map(|column| column.unique_id),
                        is_temporary: ds.is_temporary,
                        prop: None,
                        plan_part_info: None,
                        filter_condition: Vec::new(),
                        resolved_descriptor: Some(
                            tidb_planner_core_operator_logicalop::data_source_paths::ResolvedTableDescriptor::new(
                                ds.physical_table_id,
                                !ds.common_handle_cols.is_empty(),
                                scan_kind,
                                tidb_planner_core_operator_logicalop::data_source_paths::TableScanExplainIdSuffix::IncludePlanId,
                            ),
                        ),
                    },
                ));
                let scan = partial_plans_raw
                    .pop()
                    .expect("partial scan was just built");
                // Go convertToPartialTableScan removes residuals referring
                // to columns lost when the partial schema becomes handles.
                let filters = detached
                    .remained_conds
                    .iter()
                    .filter(|condition| {
                        tidb_expr::simple_expr::extract_columns(condition)
                            .iter()
                            .all(|column| {
                                scan.schema().is_some_and(|schema| {
                                    schema
                                        .columns
                                        .iter()
                                        .any(|handle| handle.unique_id == column.unique_id)
                                })
                            })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if filters.is_empty() {
                    partial_plans_raw.push(scan);
                } else {
                    let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                        ctx.allocator,
                        "Selection",
                        ds.base.base.query_block_offset(),
                    );
                    base.base.set_schema(scan.schema().cloned());
                    let ratio = crate::access_path::ordinary::filter_selectivity(
                        ds,
                        &filters,
                        &ctx.access_path_derivation_context(),
                    );
                    base.base.set_stats(
                        scan.stats_info()
                            .map(|stats| stats.scale(ratio, ctx.skew_ratio)),
                    );
                    base.set_children(vec![scan]);
                    partial_plans_raw.push(PhysicalPlan::Selection(
                        tidb_planner_core_operator_physicalop::PhysicalSelection {
                            base,
                            conditions: filters,
                            from_data_source: true,
                        },
                    ));
                }
            }
        }
    }

    let table_plan = Some({
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            ctx.allocator,
            "TableRowIDScan",
            ds.base.base.query_block_offset(),
        );
        base.base
            .set_stats(Some(merge_scan_stats(ds, total_rows, ctx)));
        base.base.set_schema(Some(tidb_expr::schema::Schema::new(
            ds.base
                .base
                .schema()
                .map(|schema| schema.columns.clone())
                .unwrap_or_default(),
        )));
        Box::new(PhysicalPlan::TableScan(
            tidb_planner_core_operator_physicalop::PhysicalTableScan {
                by_items: Vec::new(),
                group_by_col_idxs: Vec::new(),
                base,
                table_id: ds.physical_table_id,
                table_as_name: ds.table_as_name.clone(),
                dynamic_partition_access: ds.dynamic_partition_access.clone(),
                cost_columns: ds.table_columns.clone(),
                store_type: tidb_kv::store_type::StoreType::TiKv,
                keep_order: false,
                desc: false,
                ranges: tidb_ranger::types::Ranges::new(),
                range_rebuild: None,
                table_scan_penalty: ds.table_scan_penalty,
                tikv_pushdown: None,
                db_name: ds.db_name.clone(),
                table_name: ds.table_name.clone(),
                handle_col_unique_id: ds.handle_cols.first().map(|column| column.unique_id),
                is_temporary: ds.is_temporary,
                prop: None,
                plan_part_info: None,
                filter_condition: Vec::new(),
                resolved_descriptor: Some(tidb_planner_core_operator_logicalop::data_source_paths::ResolvedTableDescriptor::new(
                    ds.physical_table_id,
                    !ds.common_handle_cols.is_empty(),
                    tidb_planner_core_operator_logicalop::data_source_paths::ResolvedTableScanKind::RowId,
                    tidb_planner_core_operator_logicalop::data_source_paths::TableScanExplainIdSuffix::IncludePlanId,
                )),
            },
        ))
    });

    let (pushed_filters, table_remaining) = tidb_planner_core_operator_physicalop::pushdown::split_index_merge_table_filters(
        table_filters.clone(),
        &ctx.expr_pushdown_blacklist,
    );
    let mut table_plan = table_plan;
    // Go `BuildIndexMergeTableScan`: an ordered merge reads the row handle
    // (and the partition id of a dynamically pruned table) the merge sorts
    // and routes by; any column it appends is projected away above the
    // reader (`NeedExtraProj`). A residual root filter returns before this.
    let extra_columns_added = !by_items.is_empty()
        && table_remaining.is_empty()
        && append_index_merge_scan_columns(ds, ctx, table_plan.as_deref_mut());
    let mut root_task_conds = partial_remaining;
    root_task_conds.extend(table_remaining);
    let mut table_plan = table_plan;
    let index_plan_finished = !pushed_filters.is_empty();
    if !pushed_filters.is_empty() {
        let scan = table_plan.take().expect("union table probe exists");
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            ctx.allocator,
            "Selection",
            ds.base.base.query_block_offset(),
        );
        base.base.set_schema(scan.schema().cloned());
        let selectivity = crate::access_path::ordinary::filter_selectivity(
            ds,
            &pushed_filters,
            &ctx.access_path_derivation_context(),
        );
        base.base.set_stats(
            scan.stats_info()
                .map(|stats| stats.scale(selectivity, ctx.skew_ratio)),
        );
        base.set_children(vec![*scan]);
        table_plan = Some(Box::new(PhysicalPlan::Selection(
            tidb_planner_core_operator_physicalop::PhysicalSelection {
                base,
                conditions: pushed_filters,
                from_data_source: true,
            },
        )));
    }

    Ok(Task::Cop(tidb_planner_core_operator_physicalop::task::CopTask {
        phys_plan_part_info: Some(tidb_planner_core_operator_physicalop::PhysPlanPartInfo::from_data_source(ds)),
        table_plan,
        idx_merge_part_plans: partial_plans_raw,
        keep_order: !by_items.is_empty(),
        need_extra_proj: extra_columns_added,
        origin_schema: extra_columns_added
            .then(|| ds.base.base.schema().cloned())
            .flatten(),
        idx_merge_order: Some(tidb_planner_core_operator_physicalop::task::IndexMergeOrder {
            advisory,
            partial_matches: partial_matches.clone(),
            by_items: by_items.clone(),
        }),
        stats_context: ctx.task_stats_context(ds, &root_task_conds),
        root_task_conds,
        index_plan_finished,
        common_handle_cols: ds.common_handle_cols.clone(),
        ..Default::default()
    }))
}

/// Go `BuildIndexMergeTableScan`'s ordered-scan columns: the handle the merge
/// orders by (absent from the schema when column pruning dropped it) and the
/// partition id. Reports whether any column was added.
fn append_index_merge_scan_columns(
    ds: &DataSource,
    ctx: &DispatchContext<'_>,
    scan: Option<&mut PhysicalPlan>,
) -> bool {
    let Some(scan) = scan else {
        return false;
    };
    let Some(shared) = scan.base_mut().base.schema_shared() else {
        return false;
    };
    let mut schema = shared.as_ref().clone();
    let mut added = false;
    let contains = |schema: &tidb_expr::schema::Schema, column: &tidb_expr::column::Column| {
        schema
            .columns
            .iter()
            .any(|existing| existing.unique_id == column.unique_id)
    };
    if ds.pk_is_handle {
        if let Some(pk) = ds.handle_cols.first() {
            if !contains(&schema, pk) {
                schema.columns.push(pk.clone());
                added = true;
            }
        }
    } else if !ds.common_handle_cols.is_empty() {
        for column in &ds.common_handle_cols {
            if !contains(&schema, column) {
                schema.columns.push(column.clone());
                added = true;
            }
        }
    } else if !schema
        .columns
        .iter()
        .any(|column| column.id == tidb_planner_core_operator_logicalop::data_source::EXTRA_HANDLE_ID)
    {
        let mut extra = tidb_expr::column::Column::new(
            ctx.column_ids.map(|ids| ids.alloc()).unwrap_or_default(),
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                .with_flags(
                    tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::PRI_KEY,
                ),
        );
        extra.id = tidb_planner_core_operator_logicalop::data_source::EXTRA_HANDLE_ID;
        extra.orig_name = "_tidb_rowid".to_owned();
        schema.columns.push(extra);
        added = true;
    }
    if keeps_phys_tbl_id(ds) {
        added |= super::dispatch::add_extra_phys_tbl_id_column(&mut schema.columns, ctx);
    }
    scan.base_mut().base.set_schema(Some(schema));
    added
}

/// Go `ts.Table.GetPartitionInfo() != nil && UseDynamicPartitionPrune()`.
fn keeps_phys_tbl_id(ds: &DataSource) -> bool {
    !ds.partition_definition_ids.is_empty() && ds.dynamic_partition_access.is_some()
}

/// Go `PhysicalIndexScan.AccessCondition` / `IdxCols` / `IdxColLens` of a
/// merge partial, which `rebuildRange` re-detaches on a plan-cache hit.
pub(crate) fn partial_index_range_rebuild(
    filled: &tidb_planner_util::path::FilledIndexPath,
) -> Option<tidb_planner_core_operator_physicalop::range_rebuild::IndexRangeRebuild> {
    if filled.detached.access_conds.is_empty() {
        return None;
    }
    let (columns, lengths) = filled.columns.iter().cloned().unzip();
    Some(tidb_planner_core_operator_physicalop::range_rebuild::IndexRangeRebuild::new(
        filled.detached.access_conds.clone(),
        columns,
        lengths,
    ))
}

/// Go `PhysicalTableScan.AccessCondition` of a merge partial over the
/// handle, which `buildRangeForTableScan` re-detaches on a plan-cache hit.
fn partial_table_range_rebuild(
    ds: &DataSource,
    filled: &tidb_planner_util::path::FilledTablePath,
) -> Option<tidb_planner_core_operator_physicalop::range_rebuild::TableRangeRebuild> {
    let access_conds = &filled.detached.access_conds;
    if access_conds.is_empty() {
        return None;
    }
    Some(if ds.common_handle_cols.is_empty() {
        tidb_planner_core_operator_physicalop::range_rebuild::TableRangeRebuild::int_handle(
            access_conds.clone(),
            filled.handle_type.clone(),
            ds.pk_is_handle && filled.handle_type.is_unsigned(),
        )
    } else {
        let (columns, lengths) = filled.common_columns.iter().cloned().unzip();
        tidb_planner_core_operator_physicalop::range_rebuild::TableRangeRebuild::common_handle(
            access_conds.clone(),
            columns,
            lengths,
        )
    })
}

/// One index partial of a union: its IndexRangeScan, under a Selection for
/// its index filters or a global index's partition filter.
#[allow(clippy::too_many_arguments)]
fn push_index_partial(
    ds: &DataSource,
    ctx: &DispatchContext<'_>,
    index_pos: usize,
    filled: &tidb_planner_util::path::FilledIndexPath,
    rows: f64,
    scan_rows: f64,
    matched: bool,
    desc: bool,
    group_by_col_idxs: &[usize],
    partial_plans_raw: &mut Vec<PhysicalPlan>,
) -> Result<Vec<tidb_expr::expression::Expression>, PlanError> {
    let ranges = &filled.detached.ranges;
    let source_index = &ds.indexes[index_pos];
    let mut schema_columns = partial_index_schema(ds, source_index, ctx)?;
    // Go `ConvertToPartialIndexScan`: an ordered partial of a dynamically
    // pruned partitioned table also reads each entry's partition id, unless
    // a global index already carries it.
    if matched && keeps_phys_tbl_id(ds) && !source_index.read().global {
        super::dispatch::add_extra_phys_tbl_id_column(&mut schema_columns, ctx);
    }
    // Go GetScanRowSize prices the physical index schema once.
    let cost_columns = schema_columns.clone();
    let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
        ctx.allocator,
        "IndexRangeScan",
        ds.base.base.query_block_offset(),
    );
    base.base.set_stats(Some(merge_scan_stats(ds, scan_rows, ctx)));
    base.base
        .set_schema(Some(tidb_expr::schema::Schema::new(schema_columns)));
    partial_plans_raw.push(PhysicalPlan::IndexScan(tidb_planner_core_operator_physicalop::PhysicalIndexScan {
        by_items: Vec::new(),
        group_by_col_idxs: group_by_col_idxs.to_vec(),
        base,
        table_id: ds.physical_table_id,
        table_as_name: ds.table_as_name.clone(),
        dynamic_partition_access: ds.dynamic_partition_access.clone(),
        cost_columns,
        data_source_schema: ds.base.base.schema().cloned().map(Box::new),
        index_id: source_index.read().id,
        index_name: source_index.read().name.original().to_owned(),
        keep_order: matched,
        desc,
        ranges: ranges.clone(),
        range_rebuild: partial_index_range_rebuild(filled),
        covering_ranges: Vec::new(),
        tikv_pushdown: None,
        db_name: ds.db_name.clone(),
        is_temporary: ds.is_temporary,
        // Go's partial scan keeps the data source's required property (the
        // default one for an unordered read) and the integer handle column.
        prop: Some(Box::new(tidb_planner_property::PhysicalProperty::default())),
        pk_is_handle_col: ds.pk_is_handle.then(|| ds.handle_cols.first().cloned()).flatten(),
    }));
    let global_partition_filter = super::dispatch::global_index_partition_filter(
        ds,
        source_index,
        partial_plans_raw.last().and_then(PhysicalPlan::schema),
    );
    // Go `ConvertToPartialIndexScan`: only the conditions TiKV can evaluate
    // stay on the partial (`extractFiltersForIndexMerge`); the Selection is
    // built even when none can, and the rest return to the root task.
    let mut remaining = Vec::new();
    if !filled.index_filters.is_empty() || global_partition_filter.is_some() {
        let (pushed, rest) = tidb_planner_core_operator_physicalop::pushdown::extract_filters_for_index_merge(
            filled
                .index_filters
                .iter()
                .cloned()
                .chain(global_partition_filter)
                .collect(),
            &ctx.expr_pushdown_blacklist,
        );
        remaining = rest;
        let scan = partial_plans_raw
            .pop()
            .expect("partial scan was just built");
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            ctx.allocator,
            "Selection",
            ds.base.base.query_block_offset(),
        );
        base.base.set_schema(scan.schema().cloned());
        let ratio = if rows > 0.0 {
            filled.count_after_index.unwrap_or(rows) / rows
        } else {
            0.0
        };
        base.base.set_stats(
            scan.stats_info()
                .map(|stats| stats.scale(ratio, ctx.skew_ratio)),
        );
        base.set_children(vec![scan]);
        partial_plans_raw.push(PhysicalPlan::Selection(tidb_planner_core_operator_physicalop::PhysicalSelection {
            base,
            conditions: pushed,
            from_data_source: true,
        }));
    }
    Ok(remaining)
}


/// Go PhysicalIndexScan.InitSchema. The usable range prefix and the physical
/// index row have different widths after pruning; restore the latter from
/// immutable table metadata and retain the handle suffix's exact order.
pub(super) fn partial_index_schema(
    ds: &DataSource,
    index: &tidb_infoschema::table_source::SourceIndex,
    ctx: &DispatchContext<'_>,
) -> Result<Vec<tidb_expr::column::Column>, PlanError> {
    index_scan_schema(ds, index, ctx, true)
}

/// Construct the physical index row, including handles required by lookup.
pub(super) fn index_scan_schema(
    ds: &DataSource,
    index: &tidb_infoschema::table_source::SourceIndex,
    ctx: &DispatchContext<'_>,
    double_read: bool,
) -> Result<Vec<tidb_expr::column::Column>, PlanError> {
    let mut columns = Vec::with_capacity(index.read().columns.len() + ds.common_handle_cols.len() + 2);
    for index_column in index.read().columns.iter_deref() {
        let column = if let Some(column) = ds.schema_column_for_index_column(&index_column.read()) {
            column.clone()
        } else {
            let mut column = ds
                .table_columns
                .get(index_column.read().offset as usize)
                .cloned()
                .ok_or_else(|| {
                    PlanError::internal("index column is absent from immutable table metadata")
                })?;
            column.unique_id = ctx
                .column_ids
                .ok_or_else(|| {
                    PlanError::internal("index scan schema requires the statement column allocator")
                })?
                .alloc();
            column
        };
        // Go `indexCol2Col`: a column the index holds only a prefix of is
        // flagged `IsPrefix` (a copy; the DataSource's own column stays
        // whole), which keeps `canPushToIndexPlan` from sorting or cutting by
        // a truncated key.
        let mut column = column;
        if index_column.read().length > 0
            && column
                .ret_type
                .as_ref()
                .is_some_and(|field_type| field_type.flen() > index_column.read().length)
        {
            column.is_prefix = true;
        }
        // Go `InitSchema(path.FullIdxCols)`: an MV partial's ARRAY column is
        // its element type, which is what the index key holds.
        columns.push(crate::access_path::mv_index::unwrap_array_column(column));
    }
    let schema = ds.base.base.schema();
    if ds.is_common_handle {
        // A prefix index can contain a truncated copy of a common-handle
        // column. Go still appends the full handle, even with the same ID.
        columns.extend(ds.common_handle_cols.iter().cloned());
    } else if let Some(handle) = schema.and_then(|schema| {
        schema.columns.iter().find(|column| {
            column.id == tidb_model::column::EXTRA_HANDLE_ID
                || (ds.pk_is_handle
                    && ds
                        .columns
                        .iter()
                        .any(|metadata| metadata.read().id == column.id && metadata.read().field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)))
        })
    }) {
        columns.push(handle.clone());
    } else if double_read || index.read().global {
        columns.push(extra_column(
            ctx,
            tidb_model::column::EXTRA_HANDLE_ID,
            tidb_model::column::EXTRA_HANDLE_NAME,
        )?);
    }
    if let Some(column) = schema.and_then(|schema| {
        schema
            .columns
            .iter()
            .find(|column| column.id == tidb_model::column::EXTRA_PHYS_TBL_ID)
    }) {
        columns.push(column.clone());
    } else if index.read().global {
        columns.push(extra_column(
            ctx,
            tidb_model::column::EXTRA_PHYS_TBL_ID,
            tidb_model::column::EXTRA_PHYS_TBL_ID_NAME,
        )?);
    }
    Ok(columns)
}

fn extra_column(
    ctx: &DispatchContext<'_>,
    id: i64,
    name: &str,
) -> Result<tidb_expr::column::Column, PlanError> {
    let ids = ctx.column_ids.ok_or_else(|| {
        PlanError::internal("index scan schema requires the statement column allocator")
    })?;
    let mut column = tidb_expr::column::Column::new(
        ids.alloc(),
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    );
    column.id = id;
    column.orig_name = name.to_owned();
    Ok(column)
}

/// Go `PhysicalIndexScan.InitSchema` and `overwritePartialTableScanSchema`:
/// a partial access must return a handle even after logical column pruning.
fn partial_handle_columns(
    ds: &DataSource,
    ctx: &DispatchContext<'_>,
) -> Result<Vec<tidb_expr::column::Column>, PlanError> {
    if !ds.handle_cols.is_empty() {
        return Ok(ds.handle_cols.clone());
    }
    if !ds.common_handle_cols.is_empty() {
        return Ok(ds.common_handle_cols.clone());
    }
    if ds.pk_is_handle {
        if let Some(column) = ds.table_columns.iter().find(|column| {
            column
                .ret_type
                .as_ref()
                .is_some_and(|ty| ty.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY))
        }) {
            return Ok(vec![column.clone()]);
        }
        return Err(PlanError::internal(
            "index merge has no primary handle column",
        ));
    }
    Ok(vec![extra_column(
        ctx,
        tidb_model::column::EXTRA_HANDLE_ID,
        tidb_model::column::EXTRA_HANDLE_NAME,
    )?])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_path::index_merge::{index_merge_hint_allows, index_merge_hint_specifies, prepare_union_index_merge_path};

    #[test]
    fn named_intersection_hint_does_not_make_other_merge_hints_wildcards() {
        use tidb_planner_core_operator_logicalop::data_source::DataSourceIndexMergeHint;

        let ds = DataSource {
            index_merge_hints: vec![
                DataSourceIndexMergeHint::default(),
                DataSourceIndexMergeHint {
                    index_names: vec!["idx_a".into()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(index_merge_hint_allows(&ds, "idx_b"));
        assert!(index_merge_hint_specifies(&ds, "IDX_A"));
        assert!(!index_merge_hint_specifies(&ds, "idx_b"));
    }

    /// Go `isInIndexMergeHints`/`isSpecifiedInIndexMergeHints` compare the
    /// index's `Name.L` with each hint name by `strings.EqualFold`: `SIDX`
    /// names `ſidx` (the long s folds with `s`), `ÉX` names `éx`, and `İDX`
    /// names nothing, since `İ` folds only with itself and `Name.L` of `İdx`
    /// is `idx`.
    #[test]
    fn index_merge_hint_names_compare_by_equal_fold() {
        use tidb_planner_core_operator_logicalop::data_source::DataSourceIndexMergeHint;

        let hinted = |name: &str| DataSource {
            index_merge_hints: vec![DataSourceIndexMergeHint {
                index_names: vec![name.into()],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(index_merge_hint_allows(&hinted("SIDX"), "ſidx"));
        assert!(index_merge_hint_specifies(&hinted("SIDX"), "ſidx"));
        assert!(index_merge_hint_specifies(&hinted("ÉX"), "éx"));
        assert!(!index_merge_hint_allows(&hinted("İDX"), "İdx"));
        assert!(!index_merge_hint_specifies(&hinted("İDX"), "İdx"));
    }

    /// Go PhysicalIndexScan.InitSchema, reached by the union candidates in
    /// TestIndexMergePathGeneration: pruning cannot change physical key layout.
    #[test]
    fn index_merge_restores_index_columns_and_handle_suffix() {
        use tidb_planner_core_operator_logicalop::{BaseLogicalPlan};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let ty = FieldType::new(FieldTypeCode::LongLong);
        let column = |id| {
            let mut col = Column::new(id + 100, ty.clone());
            col.id = id;
            col
        };
        let a = column(1);
        let b = column(2);
        let mut base = BaseLogicalPlan::with_id(1, "DataSource", 0);
        base.base.set_schema(Some(Schema::new(vec![a.clone()])));
        let eq = |value| {
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("eq"),
                ty.clone(),
                vec![
                    Expression::Column(a.clone()),
                    Expression::Constant(Constant::new(Datum::Int(value), ty.clone())),
                ],
            ))
        };
        let ds = DataSource {
            base,
            table_columns: vec![a.clone(), b],
            columns: vec![tidb_model::GoShared::new({
                    let mut info = tidb_model::ColumnInfo {
                        id: 1,
                        name: tidb_ast::CiString::new("a"),
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::ColumnInfo::default()
                    };
                    info.add_flag(u64::from(tidb_datatype::FieldTypeFlags::PRI_KEY));
                    info.add_flag(u64::from(tidb_datatype::FieldTypeFlags::NOT_NULL));
                    info
                })],
            enumerated_paths: vec![tidb_planner_util::path::PossiblePath::Index { index: 0 }],
            index_merge_hints: vec![Default::default()],
            common_handle_cols: vec![a.clone()],
            is_common_handle: true,
            indexes: vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
                id: 7,
                global: true,
                columns: vec![
                    tidb_model::IndexColumn {
                        name: tidb_ast::CiString::new("a"),
                        offset: 0,
                        length: -1,
                        ..tidb_model::IndexColumn::default()
                    },
                    tidb_model::IndexColumn {
                        name: tidb_ast::CiString::new("b"),
                        offset: 1,
                        length: -1,
                        ..tidb_model::IndexColumn::default()
                    },
                ].into(),
                ..tidb_model::IndexInfo::default()
            })],
            pushed_down_conds: vec![Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("or"),
                ty.clone(),
                vec![eq(1), eq(2)],
            ))],
            ..Default::default()
        };
        // Go's `AllConds` contains every pushed condition.
        let ds = DataSource {
            all_conds: ds.pushed_down_conds.clone(),
            ..ds
        };
        let allocator = tidb_planner_core_base::PlanIdAllocator::new();
        let derivation = crate::access_path::AccessPathDerivationContext {
            opt_prefix_index_single_scan: true,
            expr_pushdown_blacklist: &Default::default(),
            selectivity_factor: tidb_planner_core_cost::SELECTION_FACTOR,
            estimator_options: Default::default(),
            range_max_size: 64 * 1024 * 1024,
            range_fallback_handler: None,
            expression_evaluator: &tidb_ranger::points::evaluate_static,
            plan_cache_marker: None,
            used_stats_recorder: None,
        };
        let plan_id_before = allocator.current();
        // Go `buildIntoAccessPath`: a union reading through one index is no
        // index merge.
        assert!(prepare_union_index_merge_path(&ds, &derivation)
            .unwrap()
            .is_none());
        assert_eq!(
            allocator.current(),
            plan_id_before,
            "logical preparation must not allocate physical plans"
        );
        // Both (a,b) and (a) can serve each branch. Logical preparation must
        // retain both; an ordering property may need the longer alternative.
        let mut choices = ds.clone();
        let shorter = tidb_model::GoShared::new(choices.indexes[0].read().clone_like_go());
        {
            let mut info = shorter.write();
            info.id = 8;
            let len = info.columns.len();
            info.columns.delete_go(1, len);
        }
        choices.indexes.push(shorter);
        choices
            .enumerated_paths
            .push(tidb_planner_util::path::PossiblePath::Index { index: 1 });
        let alternatives = prepare_union_index_merge_path(&choices, &derivation)
            .unwrap()
            .unwrap();
        assert!(alternatives.count_after_access >= 0.0);
        assert_eq!(alternatives.alternatives.len(), 2);
        assert!(alternatives
            .alternatives
            .iter()
            .all(|branch| branch.len() == 2));
        assert_eq!(allocator.current(), plan_id_before);
        let b = choices.table_columns[1].clone();
        choices.columns.push(tidb_model::GoShared::new({
                let info = tidb_model::ColumnInfo {
                    id: b.id,
                    name: tidb_ast::CiString::new("b"),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                };
                info
            }));
        choices
            .base
            .base
            .set_schema(Some(Schema::new(choices.table_columns.clone())));
        let mut multiple_or = choices.clone();
        let eq_b = |value| Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("eq"), ty.clone(), vec![Expression::Column(b.clone()),
                Expression::Constant(Constant::new(Datum::Int(value), ty.clone()))],
        ));
        // Go `buildIntoAccessPath` needs two access objects: with no index
        // led by `b`, an OR over `b` builds no merge.
        let only_b = {
            let mut only_b = choices.clone();
            only_b.pushed_down_conds = vec![Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("or"), ty.clone(), vec![eq_b(1), eq_b(2)],
            ))];
            only_b.all_conds = only_b.pushed_down_conds.clone();
            only_b
        };
        assert!(crate::access_path::index_merge::prepare_union_index_merge_paths(&only_b, &derivation, false)
            .unwrap()
            .is_empty());
        // Two indexes led by `b` serve the second OR.
        for (id, columns) in [(9, vec!["b"]), (10, vec!["b", "a"])] {
            multiple_or.indexes.push(tidb_model::GoShared::new(tidb_model::IndexInfo {
                id: id,
                global: true,
                columns: (columns
                    .into_iter()
                    .map(|name| tidb_model::IndexColumn {
                        name: tidb_ast::CiString::new(name),
                        offset: (usize::from(name == "b")) as i64,
                        length: -1,
                        ..tidb_model::IndexColumn::default()
                    })
                    .collect::<Vec<_>>()).into(),
                ..tidb_model::IndexInfo::default()
            }));
            multiple_or
                .enumerated_paths
                .push(tidb_planner_util::path::PossiblePath::Index { index: multiple_or.indexes.len() - 1 });
        }
        multiple_or.pushed_down_conds.push(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("or"), ty.clone(), vec![eq_b(1), eq_b(2)],
        )));
        multiple_or.all_conds = multiple_or.pushed_down_conds.clone();
        let candidates = crate::access_path::index_merge::prepare_union_index_merge_paths(&multiple_or, &derivation, false).unwrap();
        assert_eq!(candidates.len(), 2, "each top-level OR retains its own candidate");
        assert_ne!(candidates[0].source_filter.canonical_hash_code(), candidates[1].source_filter.canonical_hash_code());
        let advisory = tidb_planner_property::PhysicalProperty {
            advisory_sort_items: vec![tidb_planner_property::SortItem::new(b.unique_id, false)],
            ..Default::default()
        };
        let mut logical_source = choices.clone();
        logical_source.table_stats = Some(
            tidb_planner_property::stats_info::StatsInfo::new(1000.0, [])
                .with_hist_coll(tidb_planner_property::stats_info::loaded_hist_coll_for_test(true, 1000, 0, [], [])),
        );
        logical_source
            .base
            .base
            .set_stats(Some(tidb_planner_property::stats_info::StatsInfo::new(2.0, [])));
        logical_source.index_merge_hints = vec![Default::default()];
        let mut logical = tidb_planner_core_operator_logicalop::LogicalPlan::DataSource(logical_source);
        let rule_context = crate::rule::rule_tests::test_context(&allocator);
        logical
            .recursive_derive_stats_with_context(&[], &rule_context)
            .unwrap();
        let tidb_planner_core_operator_logicalop::LogicalPlan::DataSource(derived_source) = &logical else {
            unreachable!()
        };
        assert!(
            derived_source
                .derived_access_paths
                .as_ref()
                .is_some_and(|paths| paths
                    .paths
                    .iter()
                    .all(|path| matches!(path, tidb_planner_util::path::DerivedAccessPath::Union(_)))
                    && !paths.paths.is_empty()),
            "logical derivation must retain hinted merge alternatives even with cached row statistics"
        );
        assert_eq!(
            allocator.current(),
            plan_id_before,
            "candidate derivation allocates no physical plans"
        );
        assert_eq!(derived_source.derived_access_paths.as_ref().unwrap().min_selectivity, 0.0,
            "Go reads unset CountAfterIndex for an unfinished union at this stage");
        let mut cloned_source = derived_source.clone_shallow();
        cloned_source
            .derived_access_paths
            .as_mut()
            .unwrap()
            .paths
            .clear();
        assert!(
            !derived_source
                .derived_access_paths
                .as_ref()
                .unwrap()
                .paths
                .is_empty(),
            "each datasource clone owns its candidates"
        );
        let mut rewritten_source = derived_source.clone_shallow();
        rewritten_source.predicate_push_down_local(vec![eq(3)], &Default::default(), None);
        assert!(
            rewritten_source.derived_access_paths.is_none(),
            "a new predicate set invalidates retained candidates"
        );
        let column_ids = tidb_planner_core_base::ColumnIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let mut unhinted = ds.clone();
        unhinted.index_merge_hints.clear();
        assert!(build_union_index_merge_task(&unhinted, &mut ctx)
                .unwrap()
                .is_none_or(|task| task.invalid()),
            "Go builds no union whose branches all use one index");
        let must_not_evaluate =
            |_: &Expression| -> Result<tidb_datatype::Datum, tidb_exprctx::EvalError> {
                panic!("physical property search must consume logical ranges")
            };
        // Go recomputes OR cardinality after choosing property-compatible
        // alternatives. That statistics pass can evaluate constants; it must
        // still retain the logical alternatives and allocate no physical plan.
        let physical_context = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids);
        let before_convergence = allocator.current();
        let prepared = super::super::candidate_preparation::prepare_access_paths(
            derived_source, &[], &advisory, &mut ctx, false,
        );
        assert_eq!(prepared.merges.len(), 1);
        assert_eq!(allocator.current(), before_convergence,
            "merge convergence must finish before physical allocation");
        let retained = &derived_source.derived_access_paths.as_ref().unwrap().paths;
        let tidb_planner_util::path::DerivedAccessPath::Union(retained_union) = &retained[0] else {
            unreachable!()
        };
        for property in [
            &advisory,
            &tidb_planner_property::PhysicalProperty::default(),
        ] {
            let task = build_prepared_union_index_merge_task(
                derived_source,
                retained_union,
                property,
                &physical_context,
            )
            .unwrap();
            let Task::Cop(cop) = task else {
                panic!("merge conversion must produce a cop task");
            };
            for scan in cop
                .idx_merge_part_plans
                .iter()
                .chain(cop.table_plan.as_deref())
            {
                assert_eq!(
                    scan.stats_info().unwrap().hist_coll().map(std::sync::Arc::as_ptr),
                    derived_source
                        .table_stats
                        .as_ref()
                        .unwrap()
                        .hist_coll()
                        .map(std::sync::Arc::as_ptr),
                    "merge scans retain the same table profile as ordinary scans"
                );
            }
        }
        assert!(
            retained_union
                .alternatives
                .iter()
                .all(|branch| branch.len() == 2),
            "physical convergence must not destroy another property's alternatives"
        );
        let mut ordinary_source = choices.clone();
        ordinary_source.table_stats = Some(tidb_planner_property::stats_info::StatsInfo::new(1000.0, []));
        ordinary_source
            .base
            .base
            .set_stats(Some(tidb_planner_property::stats_info::StatsInfo::new(2.0, [])));
        ordinary_source.pushed_down_conds = vec![eq(1)];
        ordinary_source.all_conds = ordinary_source.pushed_down_conds.clone();
        ordinary_source.index_merge_hints.clear();
        for index in &ordinary_source.indexes {
            ordinary_source
                .derived_index_paths
                .entry(index.read().id)
                .or_default()
                .row_estimate = Some(tidb_stats::RowEstimate::new(
                100.0, 0.0, 0.0,
            ));
        }
        let mut ordinary = tidb_planner_core_operator_logicalop::LogicalPlan::DataSource(ordinary_source);
        ordinary
            .recursive_derive_stats_with_context(&[], &rule_context)
            .unwrap();
        let mut ordinary_context = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_expression_evaluator(&must_not_evaluate);
        let before_ordinary = allocator.current();
        let ordinary_task = crate::find_best_task::dispatch::find_best_task(
            &ordinary,
            &tidb_planner_property::PhysicalProperty::default(),
            &mut ordinary_context,
        )
        .unwrap();
        assert!(
            !ordinary_task.invalid(),
            "ordinary property search must reuse logical ranges"
        );
        let ordinary_allocations = allocator.current() - before_ordinary;
        let mut surviving_only = ordinary.clone();
        let tidb_planner_core_operator_logicalop::LogicalPlan::DataSource(surviving_source) = &mut surviving_only else {
            unreachable!()
        };
        surviving_source
            .derived_access_paths
            .as_mut()
            .unwrap()
            .paths
            .retain(|path| {
                matches!(
                    path,
                    tidb_planner_util::path::DerivedAccessPath::Ordinary(
                        tidb_planner_util::path::PossiblePath::Index { index: 0 }
                    )
                )
            });
        let before_survivor = allocator.current();
        let mut survivor_context = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_expression_evaluator(&must_not_evaluate);
        let survivor_task = crate::find_best_task::dispatch::find_best_task(
            &surviving_only,
            &tidb_planner_property::PhysicalProperty::default(),
            &mut survivor_context,
        )
        .unwrap();
        assert!(!survivor_task.invalid());
        assert_eq!(
            ordinary_allocations,
            allocator.current() - before_survivor,
            "a skyline-dominated index must not allocate a physical task"
        );
        let mut table_source = choices.clone();
        table_source.table_stats = Some(tidb_planner_property::stats_info::StatsInfo::new(1000.0, []));
        table_source.pushed_down_conds = vec![eq(1)];
        table_source.all_conds = table_source.pushed_down_conds.clone();
        table_source.index_merge_hints.clear();
        let detached = tidb_model::GoShared::new(table_source.indexes[0].read().clone_like_go());
        table_source.indexes[0] = detached;
        table_source.indexes[0].write().primary = true;
        table_source.indexes[0].write().unique = true;
        table_source.enumerated_paths = vec![tidb_planner_util::path::PossiblePath::Table {
            is_int_handle: false,
            primary_index: Some(0),
        }];
        let mut table = tidb_planner_core_operator_logicalop::LogicalPlan::DataSource(table_source);
        table
            .recursive_derive_stats_with_context(&[], &rule_context)
            .unwrap();
        let mut table_context = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_expression_evaluator(&must_not_evaluate);
        let table_task = crate::find_best_task::dispatch::find_best_task(
            &table,
            &tidb_planner_property::PhysicalProperty::default(),
            &mut table_context,
        )
        .unwrap();
        assert!(
            !table_task.invalid(),
            "table property search must reuse logical handle ranges"
        );
        // Restoring the datasource schema changes the derivation inputs.
        // Rebuild candidates instead of reading a later schema during costing.
        let alternatives = prepare_union_index_merge_path(&choices, &derivation)
            .unwrap()
            .unwrap();
        let ordered =
            build_prepared_union_index_merge_task(&choices, &alternatives, &advisory, &ctx)
                .unwrap();
        let Task::Cop(cop) = ordered else {
            panic!("merge must remain a cop task");
        };
        assert!(
            cop.idx_merge_order
                .as_ref()
                .unwrap()
                .partial_matches
                .iter()
                .all(|matched| matched.matched())
        );
        assert!(cop.idx_merge_part_plans.iter().all(|plan| matches!(plan, PhysicalPlan::IndexScan(scan) if scan.index_id == 7 && scan.keep_order)));
        let mut unavailable_index = ds.clone();
        unavailable_index.enumerated_paths.clear();
        assert!(
            build_union_index_merge_task(&unavailable_index, &mut ctx)
                .unwrap()
                .is_none()
        );
        let mut local_temporary = ds.clone();
        local_temporary.is_local_temporary = true;
        assert!(build_union_index_merge_task(&local_temporary, &mut ctx)
            .unwrap()
            .is_none());
        // A global index over a common handle: the pruned `b` comes back as
        // a fresh column, then the handle and the physical-table id.
        let columns = partial_index_schema(&ds, &ds.indexes[0], &ctx).unwrap();
        assert_eq!(
            columns.iter().map(|column| column.id).collect::<Vec<_>>(),
            vec![1, 2, 1, tidb_model::column::EXTRA_PHYS_TBL_ID]
        );
        assert_ne!(columns[1].unique_id, ds.table_columns[1].unique_id);
        let mut ds = ds;
        ds.is_common_handle = false;
        ds.common_handle_cols.clear();
        let detached = tidb_model::GoShared::new(ds.indexes[0].read().clone_like_go());
        ds.indexes[0] = detached;
        ds.indexes[0].write().global = false;
        for (pk_is_handle, expected) in [(false, vec![1, 2, -1]), (true, vec![1, 2, 1])] {
            ds.pk_is_handle = pk_is_handle;
            let columns = partial_index_schema(&ds, &ds.indexes[0], &ctx).unwrap();
            assert_eq!(
                columns.iter().map(|column| column.id).collect::<Vec<_>>(),
                expected
            );
        }
        ds.columns.clear();
        let inherited = extra_column(
            &ctx,
            tidb_model::column::EXTRA_PHYS_TBL_ID,
            tidb_model::column::EXTRA_PHYS_TBL_ID_NAME,
        )
        .unwrap();
        ds.base
            .base
            .set_schema(Some(Schema::new(vec![inherited.clone()])));
        ds.indexes[0].write().global = true;
        let columns = partial_index_schema(&ds, &ds.indexes[0], &ctx).unwrap();
        assert_eq!(
            columns.iter().map(|column| column.id).collect::<Vec<_>>(),
            vec![1, 2, -1, -3]
        );
        assert_eq!(columns.last().unwrap().unique_id, inherited.unique_id);
    }
}
