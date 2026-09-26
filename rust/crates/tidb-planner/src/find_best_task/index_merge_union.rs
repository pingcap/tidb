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

use crate::access_path::index_merge::{
    compare_alternatives, Partial, UnionIndexMergePath,
};
use crate::find_best_task::dispatch::DispatchContext;
use crate::logical::DataSource;
use crate::physical::PhysicalPlan;
use crate::plan_base::PlanError;
use crate::task::Task;
use tidb_expr::expression::Expression;

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
                &crate::physical_property::PhysicalProperty::default(),
                ctx,
            )
        })
        .transpose()
}

fn partial_matches_order(
    ds: &DataSource,
    partial: &Partial,
    items: &[crate::physical_property::SortItem],
) -> bool {
    let Some(first) = items.first() else {
        return false;
    };
    if items.iter().any(|item| item.desc != first.desc) {
        return false;
    }
    match partial {
        Partial::Table { .. } => {
            ds.handle_is_int
                && items.len() == 1
                && ds
                    .handle_cols
                    .first()
                    .is_some_and(|column| column.unique_id == first.col.unique_id)
        }
        Partial::Index {
            index_pos, filled, ..
        } => {
            let ranges = &filled.detached.ranges;
            let index = &ds.indexes[*index_pos];
            if ds.force_no_keep_order_index_ids.contains(&index.id) {
                return false;
            }
            // Match the same normalized key layout used by ordinary ranges,
            // including eligible appended handles after fixed declared keys.
            let columns = &filled.columns;
            let mut offset = 0;
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
                    if !fixed.is_some_and(|value| {
                        ranges.iter().all(|range| {
                            range.low_val.get(position) == Some(value)
                                && range.high_val.get(position) == Some(value)
                        })
                    }) {
                        return false;
                    }
                }
                if !found {
                    return false;
                }
            }
            true
        }
    }
}

#[cfg(test)]
pub(super) fn build_prepared_union_index_merge_task(
    ds: &DataSource,
    path: &UnionIndexMergePath,
    prop: &crate::physical_property::PhysicalProperty,
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
    prop: &crate::physical_property::PhysicalProperty,
    ctx: &DispatchContext<'_>,
) -> Option<ConvergedUnionPath<'a>> {
    use crate::physical_property::PhysicalPropMatchResult;
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
    for branch in &path.alternatives {
        let matched = branch
            .iter()
            .filter(|partial| partial_matches_order(ds, partial, sort_items))
            .min_by(|left, right| compare_alternatives(ds, left, right, true));
        let (partial, matches) = if let Some(partial) = matched {
            (partial, true)
        } else {
            if !prop.sort_items.is_empty() {
                return None;
            }
            (
                branch
                    .iter()
                    .min_by(|left, right| compare_alternatives(ds, left, right, true))
                    .expect("prepared branches are nonempty"),
                false,
            )
        };
        partials.push(partial);
        partial_matches.push(if matches {
            PhysicalPropMatchResult::Matched
        } else {
            PhysicalPropMatchResult::NotMatched
        });
    }
    let desc = sort_items.first().is_some_and(|item| item.desc);
    let total_rows = crate::access_path::index_merge::estimate_union_access(
        ds,
        &partials,
        &ctx.access_path_derivation_context(),
    );
    let index_id = |partial: &Partial| match partial {
        Partial::Index { index_pos, .. } => ds.indexes[*index_pos].id,
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
                    crate::logical::data_source::index_covers_condition(
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
                Partial::Table { .. } => false,
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
            }
        )
    }) {
        table_filters.push(path.source_filter.clone());
    }
    Some(ConvergedUnionPath {
        partials,
        partial_matches,
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
                tidb_expr::aggregation::ByItems {
                    expr: Expression::Column(column.clone()),
                    desc: item.desc,
                }
            })
            .collect(),
    })
}

pub(super) struct ConvergedUnionPath<'a> {
    partials: Vec<&'a Partial>,
    partial_matches: Vec<crate::physical_property::PhysicalPropMatchResult>,
    advisory: bool,
    desc: bool,
    total_rows: f64,
    table_filters: Vec<Expression>,
    expected_cnt: f64,
    by_items: Vec<tidb_expr::aggregation::ByItems>,
}

// Go's partial scans and merge table probe scale the original table profile,
// just like ordinary scans. Keeping HistColl is essential: its presence changes
// row-size costing even for pseudo statistics.
pub(super) fn merge_scan_stats(
    ds: &DataSource,
    rows: f64,
    ctx: &DispatchContext<'_>,
) -> crate::stats_info::StatsInfo {
    ds.table_stats
        .as_ref()
        .map(|stats| stats.scale_by_expect_cnt(rows, ctx.skew_ratio))
        .unwrap_or_else(|| crate::stats_info::StatsInfo::new(rows, []))
}

pub(super) fn build_converged_union_index_merge_task(
    ds: &DataSource,
    path: &ConvergedUnionPath<'_>,
    ctx: &DispatchContext<'_>,
) -> Result<Task, PlanError> {
    let ConvergedUnionPath {
        partials,
        partial_matches,
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
        if expected_cnt + crate::cost_factors::TOLERANCE_FACTOR < rows {
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
    for (partial, matched) in partials.iter().copied().zip(partial_matches) {
        match partial {
            Partial::Index {
                index_pos,
                filled,
                rows,
                ..
            } => {
                let ranges = &filled.detached.ranges;
                let source_index = &ds.indexes[*index_pos];
                let schema_columns = partial_index_schema(ds, source_index, ctx)?;
                // Go GetScanRowSize prices the physical index schema once.
                let cost_columns = schema_columns.clone();
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "IndexRangeScan",
                    ds.base.base.query_block_offset(),
                );
                base.base.set_stats(Some(merge_scan_stats(
                    ds,
                    partial_rows(
                        *rows,
                        matched.matched(),
                        false,
                        !filled.index_filters.is_empty(),
                    ),
                    ctx,
                )));
                base.base
                    .set_schema(Some(tidb_expr::schema::Schema::new(schema_columns)));
                partial_plans_raw.push(PhysicalPlan::IndexScan(
                    crate::physical::PhysicalIndexScan {
                        base,
                        table_id: ds.physical_table_id,
                        table_as_name: ds.table_as_name.clone(),
                        dynamic_partition_access: ds.dynamic_partition_access.clone(),
                        cost_columns,
                        data_source_schema: ds.base.base.schema().cloned().map(Box::new),
                        index_id: source_index.id,
                        index_name: source_index.name.clone(),
                        keep_order: matched.matched(),
                        desc,
                        ranges: ranges.clone(),
                        range_rebuild: None,
                        covering_ranges: Vec::new(),
                        tikv_pushdown: None,
                    },
                ));
                if !filled.index_filters.is_empty() {
                    let scan = partial_plans_raw
                        .pop()
                        .expect("partial scan was just built");
                    let mut base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "Selection",
                        ds.base.base.query_block_offset(),
                    );
                    base.base.set_schema(scan.schema().cloned());
                    let ratio = if *rows > 0.0 {
                        filled.count_after_index.unwrap_or(*rows) / rows
                    } else {
                        0.0
                    };
                    base.base.set_stats(
                        scan.stats_info()
                            .map(|stats| stats.scale(ratio, ctx.skew_ratio)),
                    );
                    base.set_children(vec![scan]);
                    partial_plans_raw.push(PhysicalPlan::Selection(
                        crate::physical::PhysicalSelection {
                            base,
                            conditions: filled.index_filters.clone(),
                            from_data_source: true,
                        },
                    ));
                }
            }
            Partial::Table { filled, rows, .. } => {
                let detached = &filled.detached;
                let ranges = &detached.ranges;
                let schema_columns = partial_handle_columns(ds, ctx)?;
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "TableRangeScan",
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
                    crate::physical::PhysicalTableScan {
                        base,
                        table_id: ds.physical_table_id,
                        table_as_name: ds.table_as_name.clone(),
                        dynamic_partition_access: ds.dynamic_partition_access.clone(),
                        cost_columns: ds.table_columns.clone(),
                        store_type: crate::physical_table_reader::StoreType::TiKv,
                        keep_order: matched.matched(),
                        desc,
                        ranges: ranges.clone(),
                        range_rebuild: None,
                        table_scan_penalty: ds.table_scan_penalty,
                        tikv_pushdown: None,
                        resolved_descriptor: Some(
                            crate::access_path::ResolvedTableDescriptor::new(
                                ds.physical_table_id,
                                !ds.common_handle_cols.is_empty(),
                                crate::access_path::ResolvedTableScanKind::Range,
                                crate::access_path::TableScanExplainIdSuffix::IncludePlanId,
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
                    let mut base = crate::physical::BasePhysicalPlan::new(
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
                        crate::physical::PhysicalSelection {
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
        let mut base = crate::physical::BasePhysicalPlan::new(
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
            crate::physical::PhysicalTableScan {
                base,
                table_id: ds.physical_table_id,
                table_as_name: ds.table_as_name.clone(),
                dynamic_partition_access: ds.dynamic_partition_access.clone(),
                cost_columns: ds.table_columns.clone(),
                store_type: crate::physical_table_reader::StoreType::TiKv,
                keep_order: false,
                desc: false,
                ranges: crate::ranger::types::Ranges::new(),
                range_rebuild: None,
                table_scan_penalty: ds.table_scan_penalty,
                tikv_pushdown: None,
                resolved_descriptor: Some(crate::access_path::ResolvedTableDescriptor::new(
                    ds.physical_table_id,
                    !ds.common_handle_cols.is_empty(),
                    crate::access_path::ResolvedTableScanKind::RowId,
                    crate::access_path::TableScanExplainIdSuffix::IncludePlanId,
                )),
            },
        ))
    });

    let (pushed_filters, root_task_conds) = crate::pushdown::split_scan_filters(
        table_filters.clone(),
        tidb_expr::infer_pushdown::PushDownStore::TiKv,
        &ctx.expr_pushdown_blacklist,
    );
    let mut table_plan = table_plan;
    let index_plan_finished = !pushed_filters.is_empty();
    if !pushed_filters.is_empty() {
        let scan = table_plan.take().expect("union table probe exists");
        let mut base = crate::physical::BasePhysicalPlan::new(
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
            crate::physical::PhysicalSelection {
                base,
                conditions: pushed_filters,
                from_data_source: true,
            },
        )));
    }

    Ok(Task::Cop(crate::task::CopTask {
        table_plan,
        idx_merge_part_plans: partial_plans_raw,
        keep_order: !by_items.is_empty(),
        idx_merge_order: Some(crate::task::IndexMergeOrder {
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

/// Go PhysicalIndexScan.InitSchema. The usable range prefix and the physical
/// index row have different widths after pruning; restore the latter from
/// immutable table metadata and retain the handle suffix's exact order.
pub(super) fn partial_index_schema(
    ds: &DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    ctx: &DispatchContext<'_>,
) -> Result<Vec<tidb_expr::column::Column>, PlanError> {
    index_scan_schema(ds, index, ctx, true)
}

/// Construct the physical index row, including handles required by lookup.
pub(super) fn index_scan_schema(
    ds: &DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    ctx: &DispatchContext<'_>,
    double_read: bool,
) -> Result<Vec<tidb_expr::column::Column>, PlanError> {
    let mut columns = Vec::with_capacity(index.columns.len() + ds.common_handle_cols.len() + 2);
    for index_column in &index.columns {
        let column = if let Some(column) = ds.schema_column_for_index_column(index_column) {
            column.clone()
        } else {
            let mut column = ds
                .table_columns
                .get(index_column.offset)
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
        columns.push(column);
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
                        .any(|metadata| metadata.id == column.id && metadata.is_primary_key))
        })
    }) {
        columns.push(handle.clone());
    } else if double_read || index.global {
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
    } else if index.global {
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
        use crate::logical::data_source::DataSourceIndexMergeHint;

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

    /// Go PhysicalIndexScan.InitSchema, reached by the union candidates in
    /// TestIndexMergePathGeneration: pruning cannot change physical key layout.
    #[test]
    fn index_merge_restores_index_columns_and_handle_suffix() {
        use crate::logical::{data_source::DataSourceColumn, BaseLogicalPlan};
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::{
            column::Column, constant::Constant, scalar_function::ScalarFunction, schema::Schema,
        };

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
            columns: vec![DataSourceColumn {
                id: 1,
                name: "a".into(),
                is_primary_key: true,
                is_not_null: true,
            }],
            enumerated_paths: vec![crate::access_path::PossiblePath::Index { index: 0 }],
            index_merge_hints: vec![Default::default()],
            common_handle_cols: vec![a.clone()],
            is_common_handle: true,
            indexes: vec![SourceIndex {
                id: 7,
                global: true,
                columns: vec![
                    SourceIndexColumn {
                        name: "a".into(),
                        offset: 0,
                        length: -1,
                    },
                    SourceIndexColumn {
                        name: "b".into(),
                        offset: 1,
                        length: -1,
                    },
                ],
                ..Default::default()
            }],
            pushed_down_conds: vec![Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("or"),
                ty.clone(),
                vec![eq(1), eq(2)],
            ))],
            ..Default::default()
        };
        let allocator = crate::plan_base::PlanIdAllocator::new();
        let derivation = crate::access_path::AccessPathDerivationContext {
            opt_prefix_index_single_scan: true,
            expr_pushdown_blacklist: &Default::default(),
            selectivity_factor: crate::cost_factors::SELECTION_FACTOR,
            estimator_options: Default::default(),
            range_max_size: 64 * 1024 * 1024,
            range_fallback_handler: None,
            expression_evaluator: &crate::ranger::points::evaluate_static,
        };
        let plan_id_before = allocator.current();
        let prepared = prepare_union_index_merge_path(&ds, &derivation)
            .unwrap()
            .unwrap();
        assert!(prepared.count_after_access >= 0.0);
        assert_eq!(
            allocator.current(),
            plan_id_before,
            "logical preparation must not allocate physical plans"
        );
        // Both (a,b) and (a) can serve each branch. Logical preparation must
        // retain both; an ordering property may need the longer alternative.
        let mut choices = ds.clone();
        let mut shorter = choices.indexes[0].clone();
        shorter.id = 8;
        shorter.columns.truncate(1);
        choices.indexes.push(shorter);
        choices
            .enumerated_paths
            .push(crate::access_path::PossiblePath::Index { index: 1 });
        let alternatives = prepare_union_index_merge_path(&choices, &derivation)
            .unwrap()
            .unwrap();
        assert_eq!(alternatives.alternatives.len(), 2);
        assert!(alternatives
            .alternatives
            .iter()
            .all(|branch| branch.len() == 2));
        assert_eq!(allocator.current(), plan_id_before);
        let b = choices.table_columns[1].clone();
        choices.columns.push(DataSourceColumn {
            id: b.id,
            name: "b".into(),
            is_primary_key: false,
            is_not_null: false,
        });
        choices
            .base
            .base
            .set_schema(Some(Schema::new(choices.table_columns.clone())));
        let mut multiple_or = choices.clone();
        let eq_b = |value| Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("eq"), ty.clone(), vec![Expression::Column(b.clone()),
                Expression::Constant(Constant::new(Datum::Int(value), ty.clone()))],
        ));
        multiple_or.pushed_down_conds.push(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("or"), ty.clone(), vec![eq_b(1), eq_b(2)],
        )));
        let candidates = crate::access_path::index_merge::prepare_union_index_merge_paths(&multiple_or, &derivation, false).unwrap();
        assert_eq!(candidates.len(), 2, "each top-level OR retains its own candidate");
        assert_ne!(candidates[0].source_filter.canonical_hash_code(), candidates[1].source_filter.canonical_hash_code());
        let advisory = crate::physical_property::PhysicalProperty {
            advisory_sort_items: vec![crate::physical_property::SortItem::new(b.unique_id, false)],
            ..Default::default()
        };
        let mut logical_source = choices.clone();
        logical_source.table_stats = Some(
            crate::stats_info::StatsInfo::new(1000.0, [])
                .with_hist_coll(crate::stats_info::HistColl::new(true, 1000, [])),
        );
        logical_source
            .base
            .base
            .set_stats(Some(crate::stats_info::StatsInfo::new(2.0, [])));
        logical_source.index_merge_hints = vec![Default::default()];
        let mut logical = crate::logical::LogicalPlan::DataSource(logical_source);
        let rule_context = crate::logical::rule_tests::test_context(&allocator);
        logical
            .recursive_derive_stats_with_context(&[], &rule_context)
            .unwrap();
        let crate::logical::LogicalPlan::DataSource(derived_source) = &logical else {
            unreachable!()
        };
        assert!(
            derived_source
                .derived_access_paths
                .as_ref()
                .is_some_and(|paths| paths
                    .paths
                    .iter()
                    .all(|path| matches!(path, crate::access_path::DerivedAccessPath::Union(_)))
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
        rewritten_source.predicate_push_down_local(vec![eq(3)], &Default::default());
        assert!(
            rewritten_source.derived_access_paths.is_none(),
            "a new predicate set invalidates retained candidates"
        );
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let mut unhinted = ds.clone();
        unhinted.index_merge_hints.clear();
        assert!(build_union_index_merge_task(&unhinted, &mut ctx).unwrap().unwrap().invalid(),
            "Go rejects an unhinted union whose chosen branches all use one index");
        let must_not_evaluate =
            |_: &Expression| -> Result<tidb_datatype::Datum, tidb_expr::EvalError> {
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
        let crate::access_path::DerivedAccessPath::Union(retained_union) = &retained[0] else {
            unreachable!()
        };
        for property in [
            &advisory,
            &crate::physical_property::PhysicalProperty::default(),
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
                    scan.stats_info().unwrap().hist_coll(),
                    derived_source.table_stats.as_ref().unwrap().hist_coll(),
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
        ordinary_source.table_stats = Some(crate::stats_info::StatsInfo::new(1000.0, []));
        ordinary_source
            .base
            .base
            .set_stats(Some(crate::stats_info::StatsInfo::new(2.0, [])));
        ordinary_source.pushed_down_conds = vec![eq(1)];
        ordinary_source.index_merge_hints.clear();
        for index in &ordinary_source.indexes {
            ordinary_source
                .derived_index_paths
                .entry(index.id)
                .or_default()
                .row_estimate = Some(crate::cardinality::row_count_column::RowEstimate::new(
                100.0, 0.0, 0.0,
            ));
        }
        let mut ordinary = crate::logical::LogicalPlan::DataSource(ordinary_source);
        ordinary
            .recursive_derive_stats_with_context(&[], &rule_context)
            .unwrap();
        let mut ordinary_context = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_expression_evaluator(&must_not_evaluate);
        let before_ordinary = allocator.current();
        let ordinary_task = crate::find_best_task::dispatch::find_best_task(
            &ordinary,
            &crate::physical_property::PhysicalProperty::default(),
            &mut ordinary_context,
        )
        .unwrap();
        assert!(
            !ordinary_task.invalid(),
            "ordinary property search must reuse logical ranges"
        );
        let ordinary_allocations = allocator.current() - before_ordinary;
        let mut surviving_only = ordinary.clone();
        let crate::logical::LogicalPlan::DataSource(surviving_source) = &mut surviving_only else {
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
                    crate::access_path::DerivedAccessPath::Ordinary(
                        crate::access_path::PossiblePath::Index { index: 0 }
                    )
                )
            });
        let before_survivor = allocator.current();
        let mut survivor_context = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_expression_evaluator(&must_not_evaluate);
        let survivor_task = crate::find_best_task::dispatch::find_best_task(
            &surviving_only,
            &crate::physical_property::PhysicalProperty::default(),
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
        table_source.table_stats = Some(crate::stats_info::StatsInfo::new(1000.0, []));
        table_source.pushed_down_conds = vec![eq(1)];
        table_source.index_merge_hints.clear();
        table_source.indexes[0].primary = true;
        table_source.indexes[0].unique = true;
        table_source.enumerated_paths = vec![crate::access_path::PossiblePath::Table {
            is_int_handle: false,
            primary_index: Some(0),
        }];
        let mut table = crate::logical::LogicalPlan::DataSource(table_source);
        table
            .recursive_derive_stats_with_context(&[], &rule_context)
            .unwrap();
        let mut table_context = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_expression_evaluator(&must_not_evaluate);
        let table_task = crate::find_best_task::dispatch::find_best_task(
            &table,
            &crate::physical_property::PhysicalProperty::default(),
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
        let task = build_union_index_merge_task(&ds, &mut ctx)
            .unwrap()
            .unwrap();
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
        assert!(matches!(&task, Task::Cop(cop) if !cop.index_plan_finished));
        let task = task.into_root_task(&allocator).unwrap();
        let PhysicalPlan::IndexMergeReader(reader) = task.plan().unwrap() else {
            panic!("expected index merge")
        };
        for partial in &reader.partial_plans_raw {
            let ids: Vec<_> = partial
                .schema()
                .unwrap()
                .columns
                .iter()
                .map(|column| column.id)
                .collect();
            assert_eq!(ids, vec![1, 2, 1, tidb_model::column::EXTRA_PHYS_TBL_ID]);
            assert_ne!(
                partial.schema().unwrap().columns[1].unique_id,
                ds.table_columns[1].unique_id
            );
        }
        let mut ds = ds;
        ds.is_common_handle = false;
        ds.common_handle_cols.clear();
        ds.indexes[0].global = false;
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
        ds.indexes[0].global = true;
        let columns = partial_index_schema(&ds, &ds.indexes[0], &ctx).unwrap();
        assert_eq!(
            columns.iter().map(|column| column.id).collect::<Vec<_>>(),
            vec![1, 2, -1, -3]
        );
        assert_eq!(columns.last().unwrap().unique_id, inherited.unique_id);
    }
}
