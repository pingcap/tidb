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

//! Go `generateIndexMergePath`'s UNION slice (`indexmerge_path.go`): split
//! the pushed conditions' top-level OR and give every disjunct its own
//! partial access — a range scan over a usable index prefix, or a handle
//! range scan when the disjunct binds the integer handle. Every disjunct
//! must be fully covered by its partial (no residual conditions left) and
//! the OR must be the WHOLE pushed set, so no sibling conjunct can be lost.

use crate::find_best_task::dispatch::DispatchContext;
use crate::logical::DataSource;
use crate::physical::PhysicalPlan;
use crate::plan_base::PlanError;
use crate::task::Task;
use tidb_expr::expression::Expression;

/// One viable partial access for a single disjunct.
enum Partial {
    /// A secondary-index range scan; rows are (index cols..., handle...).
    Index {
        index_pos: usize,
        ranges: crate::ranger::types::Ranges,
        rows: f64,
    },
    /// A table (handle) range scan; rows carry the whole row.
    Table {
        ranges: crate::ranger::types::Ranges,
        rows: f64,
    },
}

/// Go `generateIndexMergePath`'s union candidate for a DataSource whose
/// pushed conditions are exactly one top-level OR. Returns `None` when the
/// form does not apply or any disjunct lacks a fully-covering partial.
pub fn build_union_index_merge_task(
    ds: &DataSource,
    ctx: &mut DispatchContext<'_>,
) -> Result<Option<Task>, PlanError> {
    if ds.pushed_down_conds.len() != 1 {
        return Ok(None);
    }
    let Expression::ScalarFunction(or_func) = &ds.pushed_down_conds[0] else {
        return Ok(None);
    };
    if or_func.func_name.lowercase() != "or" {
        return Ok(None);
    }
    let disjuncts = tidb_expr::expr_util::normal_form::flatten_dnf_conditions(or_func);
    if disjuncts.len() < 2 {
        return Ok(None);
    }

    let mut partials: Vec<Partial> = Vec::with_capacity(disjuncts.len());
    let mut any_index_partial = false;
    for disjunct in &disjuncts {
        let disjunct_conds = std::slice::from_ref(disjunct);
        let mut chosen: Option<Partial> = None;
        // Index partials first, in the source's own index order, then the
        // handle fallback. Live Go picks the same shapes for the recorded
        // fixtures (ia/ib index partials; a c_d_e index partial next to an
        // a-handle table partial).
        for (index_pos, source_index) in ds.indexes.iter().enumerate() {
            if source_index.is_columnar {
                continue;
            }
            let resolved = source_index
                .columns
                .iter()
                .map_while(|index_column| {
                    ds.schema_column_for_index_column(index_column)
                        .cloned()
                        .map(|column| (column, index_column.length))
                })
                .collect::<Vec<_>>();
            if resolved.is_empty() {
                continue;
            }
            let (cols, lengths): (Vec<_>, Vec<_>) = resolved.into_iter().unzip();
            let Ok(result) = ctx.detach_index_range(disjunct_conds, &cols, &lengths) else {
                continue;
            };
            if result.ranges.is_empty()
                || result.ranges.iter().any(|range| range.is_full_range(false))
                || !result.remained_conds.is_empty()
            {
                continue;
            }
            let rows = if ds.table_scan_penalty.pseudo_stats {
                crate::ranger::stats_bridge::pseudo_count_by_index_ranges(
                    &result.ranges,
                    ds.table_stats
                        .as_ref()
                        .map_or(0.0, |stats| stats.row_count()),
                    None,
                )
            } else {
                ds.index_path_row_estimates
                    .get(&source_index.id)
                    .map(|estimate| estimate.est)
                    .unwrap_or(result.ranges.len() as f64)
            };
            chosen = Some(Partial::Index {
                index_pos,
                ranges: result.ranges.clone(),
                rows,
            });
            break;
        }
        if chosen.is_none() && ds.handle_is_int && !ds.handle_cols.is_empty() {
            let lengths = vec![tidb_datatype::UNSPECIFIED_LENGTH; ds.handle_cols.len()];
            if let Ok(result) = ctx.detach_index_range(disjunct_conds, &ds.handle_cols, &lengths) {
                if !result.ranges.is_empty()
                    && !result.ranges.iter().any(|range| range.is_full_range(false))
                    && result.remained_conds.is_empty()
                {
                    chosen = Some(Partial::Table {
                        ranges: result.ranges.clone(),
                        rows: ds
                            .table_path_count_after_access
                            .unwrap_or(result.ranges.len() as f64),
                    });
                }
            }
        }
        match chosen {
            Some(partial) => {
                if matches!(partial, Partial::Index { .. }) {
                    any_index_partial = true;
                }
                partials.push(partial);
            }
            // One uncovered disjunct makes the whole union unusable.
            None => return Ok(None),
        }
    }

    // A union whose partials include an index scan must fetch rows by
    // handle; all-table partials need no final row fetch — and add no
    // value over the ordinary handle-range path, so leave those to it.
    if !any_index_partial {
        return Ok(None);
    }

    let mut partial_plans_raw = Vec::with_capacity(partials.len());
    let mut total_rows = 0.0;
    for partial in &partials {
        match partial {
            Partial::Index {
                index_pos,
                ranges,
                rows,
            } => {
                total_rows += rows;
                let source_index = &ds.indexes[*index_pos];
                let resolved = source_index
                    .columns
                    .iter()
                    .map_while(|index_column| {
                        ds.schema_column_for_index_column(index_column)
                            .cloned()
                            .map(|column| (column, index_column.length))
                    })
                    .collect::<Vec<_>>();
                // Go `InitSchema` (`physical_index_scan.go:363`): the partial
                // reads the index columns plus the handle, and the executor's
                // handle extraction resolves against exactly these columns.
                let mut schema_columns: Vec<tidb_expr::column::Column> =
                    resolved.iter().map(|(column, _)| column.clone()).collect();
                let handles = partial_handle_columns(ds, ctx)?;
                for handle in &handles {
                    if !schema_columns
                        .iter()
                        .any(|column| column.unique_id == handle.unique_id)
                    {
                        schema_columns.push(handle.clone());
                    }
                }
                let mut cost_columns = source_index
                    .columns
                    .iter()
                    .filter_map(|column| ds.table_columns.get(column.offset).cloned())
                    .collect::<Vec<_>>();
                cost_columns.extend(schema_columns.iter().cloned());
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "IndexRangeScan",
                    ds.base.base.query_block_offset(),
                );
                base.base
                    .set_stats(Some(crate::stats_info::StatsInfo::new(*rows, [])));
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
                        keep_order: false,
                        desc: false,
                        ranges: ranges.clone(),
                        range_rebuild: None,
                        covering_ranges: Vec::new(),
                        tikv_pushdown: None,
                    },
                ));
            }
            Partial::Table { ranges, rows } => {
                total_rows += rows;
                let schema_columns = ds.handle_cols.clone();
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "TableRangeScan",
                    ds.base.base.query_block_offset(),
                );
                base.base
                    .set_stats(Some(crate::stats_info::StatsInfo::new(*rows, [])));
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
                        keep_order: false,
                        desc: false,
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
            }
        }
    }

    // A union whose partials include an index scan must fetch rows by
    // handle; all-table partials need no final row fetch — and add no
    // value over the ordinary handle-range path, so leave those to it.
    if !any_index_partial {
        return Ok(None);
    }

    let table_plan = any_index_partial.then(|| {
        let mut base = crate::physical::BasePhysicalPlan::new(
            ctx.allocator,
            "TableRowIDScan",
            ds.base.base.query_block_offset(),
        );
        base.base
            .set_stats(Some(crate::stats_info::StatsInfo::new(total_rows, [])));
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

    let mut base = crate::physical::BasePhysicalPlan::new(
        ctx.allocator,
        "IndexMergeReader",
        ds.base.base.query_block_offset(),
    );
    base.base
        .set_stats(Some(crate::stats_info::StatsInfo::new(total_rows, [])));
    base.base.set_schema(ds.base.base.schema().cloned());
    let reader = PhysicalPlan::IndexMergeReader(crate::physical::PhysicalIndexMergeReader {
        base,
        partial_plans_raw,
        table_plan,
        is_intersection_type: false,
        access_mv_index: false,
        pushed_limit: None,
        by_items: Vec::new(),
        keep_order: false,
    });
    let mut root = crate::task::RootTask::default();
    root.set_plan(reader);
    Ok(Some(Task::Root(root)))
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
            column.ret_type.as_ref().is_some_and(|ty| {
                ty.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
            })
        }) {
            return Ok(vec![column.clone()]);
        }
        return Err(PlanError::internal("index merge has no primary handle column"));
    }
    let ids = ctx.column_ids.ok_or_else(|| {
        PlanError::internal("index merge requires the statement column allocator")
    })?;
    let mut handle = tidb_expr::column::Column::new(
        ids.alloc(),
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    );
    handle.id = tidb_model::column::EXTRA_HANDLE_ID;
    handle.orig_name = "_tidb_rowid".to_owned();
    Ok(vec![handle])
}
