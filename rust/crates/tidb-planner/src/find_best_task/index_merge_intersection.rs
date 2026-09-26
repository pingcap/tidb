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

//! The ordinary-index AND/intersection slice of Go `generateIndexMergePath`.

use crate::find_best_task::dispatch::DispatchContext;
use crate::find_best_task::index_merge_union::merge_scan_stats;
use crate::logical::DataSource;
use crate::physical::PhysicalPlan;
use crate::plan_base::PlanError;
use crate::task::Task;

/// Builds Go's hinted normal-index AND IndexMerge for usable ranges on at
/// least two named indexes. Any conditions no partial range enforces remain
/// as a Selection on the final table lookup.
pub(super) fn build_prepared_intersection_index_merge_task(
    ds: &DataSource,
    path: &crate::access_path::index_merge::IntersectionIndexMergePath,
    ctx: &DispatchContext<'_>,
) -> Result<Task, PlanError> {
    let partials = &path.partials;
    let mut partial_plans_raw = Vec::with_capacity(partials.len());
    let output_rows = path.count_after_access;
    for partial in partials {
        let source_index = &ds.indexes[partial.index_pos];
        let filled = partial
            .path
            .filled
            .as_ref()
            .expect("intersection partials are filled logical paths");
        let rows = partial
            .path
            .count_after_access()
            .unwrap_or(filled.detached.ranges.len() as f64);
        let schema_columns =
            crate::find_best_task::index_merge_union::partial_index_schema(ds, source_index, ctx)?;
        let mut base = crate::physical::BasePhysicalPlan::new(
            ctx.allocator,
            "IndexRangeScan",
            ds.base.base.query_block_offset(),
        );
        base.base.set_stats(Some(merge_scan_stats(ds, rows, ctx)));
        base.base
            .set_schema(Some(tidb_expr::schema::Schema::new(schema_columns.clone())));
        let mut partial_plan = PhysicalPlan::IndexScan(crate::physical::PhysicalIndexScan {
            base,
            table_id: ds.physical_table_id,
            table_as_name: ds.table_as_name.clone(),
            dynamic_partition_access: ds.dynamic_partition_access.clone(),
            cost_columns: schema_columns,
            data_source_schema: ds.base.base.schema().cloned().map(Box::new),
            index_id: source_index.id,
            index_name: source_index.name.clone(),
            keep_order: false,
            desc: false,
            ranges: filled.detached.ranges.clone(),
            range_rebuild: None,
            covering_ranges: Vec::new(),
            tikv_pushdown: None,
        });
        if !filled.index_filters.is_empty() {
            let mut selection = crate::physical::BasePhysicalPlan::new(
                ctx.allocator,
                "Selection",
                ds.base.base.query_block_offset(),
            );
            selection.base.set_schema(partial_plan.schema().cloned());
            selection
                .base
                .set_stats(Some(crate::stats_info::StatsInfo::new(
                    filled
                        .count_after_index
                        .unwrap_or(rows * crate::cost_factors::SELECTION_FACTOR),
                    [],
                )));
            selection.set_children(vec![partial_plan]);
            partial_plan = PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                base: selection,
                conditions: filled.index_filters.clone(),
                from_data_source: true,
            });
        }
        partial_plans_raw.push(partial_plan);
    }

    let schema = ds.base.base.schema().cloned().unwrap_or_default();
    let mut table_scan_base = crate::physical::BasePhysicalPlan::new(
        ctx.allocator,
        "TableRowIDScan",
        ds.base.base.query_block_offset(),
    );
    table_scan_base
        .base
        .set_stats(Some(merge_scan_stats(ds, output_rows, ctx)));
    table_scan_base.base.set_schema(Some(schema.clone()));
    let table_scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
        base: table_scan_base,
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
    });
    // Go addPushedDownSelectionToTableScan separates root conditions before
    // sealing the cop table phase. Rejected residuals must survive conversion.
    let (table_filters, root_task_conds) = crate::pushdown::split_scan_filters(
        path.table_filters.clone(),
        tidb_expr::infer_pushdown::PushDownStore::TiKv,
        &ctx.expr_pushdown_blacklist,
    );
    let table_plan = if table_filters.is_empty() {
        table_scan
    } else {
        let mut selection_base = crate::physical::BasePhysicalPlan::new(
            ctx.allocator,
            "Selection",
            ds.base.base.query_block_offset(),
        );
        selection_base
            .base
            .set_stats(Some(merge_scan_stats(ds, output_rows, ctx)));
        selection_base.base.set_schema(Some(schema));
        selection_base.set_children(vec![table_scan]);
        PhysicalPlan::Selection(crate::physical::PhysicalSelection {
            base: selection_base,
            conditions: table_filters,
            from_data_source: true,
        })
    };

    // Go seals the index phase when table-side filtering has begun.
    let index_plan_finished = !matches!(&table_plan, PhysicalPlan::TableScan(_));
    Ok(Task::Cop(crate::task::CopTask {
        table_plan: Some(Box::new(table_plan)),
        idx_merge_part_plans: partial_plans_raw,
        idx_merge_is_intersection: true,
        index_plan_finished,
        stats_context: ctx.task_stats_context(ds, &root_task_conds),
        root_task_conds,
        common_handle_cols: ds.common_handle_cols.clone(),
        ..Default::default()
    }))
}
