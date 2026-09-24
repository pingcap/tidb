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
            // Go models an int-clustered table's PRIMARY key AS the handle,
            // never as a secondary index partial; the handle fallback below
            // builds that disjunct's TableRangeScan instead.
            if ds.handle_is_int && source_index.primary {
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
                    // Go `getDatasetRowCnt`/partial stats price a table
                    // partial by its own range estimate: under pseudo
                    // statistics the int-handle estimator (`pseudo.go`'s
                    // signed/unsigned range counters), not the raw
                    // table-path count, which is the UNACCESSed row count
                    // and once made a `[2,2]` partial price the whole table.
                    let rows = if ds.table_scan_penalty.pseudo_stats {
                        crate::ranger::stats_bridge::pseudo_count_by_int_ranges(
                            &result.ranges,
                            ds.table_stats
                                .as_ref()
                                .map_or(0.0, |stats| stats.row_count()),
                            ds.handle_cols
                                .first()
                                .and_then(|handle| handle.ret_type.as_ref())
                                .is_some_and(tidb_datatype::FieldType::is_unsigned),
                        )
                    } else {
                        ds.table_path_count_after_access
                            .unwrap_or(result.ranges.len() as f64)
                    };
                    chosen = Some(Partial::Table {
                        ranges: result.ranges.clone(),
                        rows,
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
                let schema_columns = partial_index_schema(ds, source_index, ctx)?;
                // Go GetScanRowSize prices the physical index schema once.
                let cost_columns = schema_columns.clone();
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
                let schema_columns = partial_handle_columns(ds, ctx)?;
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
        // Go EXPLAIN prints this operator as `IndexMerge` (the physical
        // type name), not the executor's `IndexMergeReader` spelling.
        "IndexMerge",
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

/// Go PhysicalIndexScan.InitSchema. The usable range prefix and the physical
/// index row have different widths after pruning; restore the latter from
/// immutable table metadata and retain the handle suffix's exact order.
fn partial_index_schema(
    ds: &DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    ctx: &DispatchContext<'_>,
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
                    PlanError::internal("index merge requires the statement column allocator")
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
    } else {
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
        PlanError::internal("index merge requires the statement column allocator")
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
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let task = build_union_index_merge_task(&ds, &mut ctx)
            .unwrap()
            .unwrap();
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
