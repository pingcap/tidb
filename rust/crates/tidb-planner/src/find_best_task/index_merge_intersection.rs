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

//! Go `convertToIndexMergeScan` for a finished index-merge path: the hinted
//! ordinary-index intersection and the multi-valued index paths.

use crate::find_best_task::dispatch::DispatchContext;
use crate::find_best_task::index_merge_union::merge_scan_stats;
use tidb_planner_core_operator_logicalop::DataSource;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_physicalop::task::Task;
use tidb_planner_cardinality::derive_stats::StatsInfoScale;

/// Builds the IndexMerge of a finished path: one partial plan per partial
/// path, then the table lookup. Any conditions no partial range enforces
/// remain as a Selection on the final table lookup.
pub(super) fn build_prepared_index_merge_task(
    ds: &DataSource,
    path: &tidb_planner_util::path::IndexMergePath,
    ctx: &DispatchContext<'_>,
) -> Result<Task, PlanError> {
    let partials = &path.partials;
    let mut partial_plans_raw = Vec::with_capacity(partials.len());
    // Go `globalRemainingFilters`: each partial's unpushable conditions, in
    // partial order, ahead of the table lookup's own.
    let mut partial_remaining = Vec::new();
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
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            ctx.allocator,
            "IndexRangeScan",
            ds.base.base.query_block_offset(),
        );
        base.base.set_stats(Some(merge_scan_stats(ds, rows, ctx)));
        base.base
            .set_schema(Some(tidb_expr::schema::Schema::new(schema_columns.clone())));
        if let Some(reason) = &path.noncacheable_reason {
            base.base.set_noncacheable_reason(reason);
        }
        let mut partial_plan = PhysicalPlan::IndexScan(tidb_planner_core_operator_physicalop::PhysicalIndexScan {
            by_items: Vec::new(),
            group_by_col_idxs: Vec::new(),
            base,
            table_id: ds.physical_table_id,
            table_as_name: ds.table_as_name.clone(),
            dynamic_partition_access: ds.dynamic_partition_access.clone(),
            cost_columns: schema_columns,
            data_source_schema: ds.base.base.schema().cloned().map(Box::new),
            index_id: source_index.read().id,
            index_name: source_index.read().name.original().to_owned(),
            keep_order: false,
            desc: false,
            ranges: filled.detached.ranges.clone(),
            range_rebuild: crate::find_best_task::index_merge_union::partial_index_range_rebuild(
                filled,
            ),
            covering_ranges: Vec::new(),
            tikv_pushdown: None,
            db_name: ds.db_name.clone(),
            is_temporary: ds.is_temporary,
            prop: Some(Box::new(tidb_planner_property::PhysicalProperty::default())),
            pk_is_handle_col: ds.pk_is_handle.then(|| ds.handle_cols.first().cloned()).flatten(),
        });
        if !filled.index_filters.is_empty() {
            // Go `ConvertToPartialIndexScan`: only what TiKV can evaluate
            // stays on the partial; the Selection is kept even if empty.
            let (pushed, rest) = tidb_planner_core_operator_physicalop::pushdown::extract_filters_for_index_merge(
                filled.index_filters.clone(),
                &ctx.expr_pushdown_blacklist,
            );
            partial_remaining.extend(rest);
            let mut selection = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                ctx.allocator,
                "Selection",
                ds.base.base.query_block_offset(),
            );
            selection.base.set_schema(partial_plan.schema().cloned());
            selection
                .base
                .set_stats(Some(tidb_planner_property::stats_info::StatsInfo::new(
                    filled
                        .count_after_index
                        .unwrap_or(rows * tidb_planner_core_cost::SELECTION_FACTOR),
                    [],
                )));
            selection.set_children(vec![partial_plan]);
            partial_plan = PhysicalPlan::Selection(tidb_planner_core_operator_physicalop::PhysicalSelection {
                base: selection,
                conditions: pushed,
                from_data_source: true,
            });
        }
        partial_plans_raw.push(partial_plan);
    }

    let schema = ds.base.base.schema().cloned().unwrap_or_default();
    let mut table_scan_base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
        ctx.allocator,
        "TableRowIDScan",
        ds.base.base.query_block_offset(),
    );
    table_scan_base
        .base
        .set_stats(Some(merge_scan_stats(ds, output_rows, ctx)));
    table_scan_base.base.set_schema(Some(schema.clone()));
    let table_scan = PhysicalPlan::TableScan(tidb_planner_core_operator_physicalop::PhysicalTableScan {
        by_items: Vec::new(),
        group_by_col_idxs: Vec::new(),
        base: table_scan_base,
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
    });
    // Go `BuildIndexMergeTableScan` separates root conditions before sealing
    // the cop table phase.
    let (table_filters, table_remaining) = tidb_planner_core_operator_physicalop::pushdown::split_index_merge_table_filters(
        path.table_filters.clone(),
        &ctx.expr_pushdown_blacklist,
    );
    let mut root_task_conds = partial_remaining;
    root_task_conds.extend(table_remaining);
    let table_plan = if table_filters.is_empty() {
        table_scan
    } else {
        let mut selection_base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            ctx.allocator,
            "Selection",
            ds.base.base.query_block_offset(),
        );
        // Go `BuildIndexMergeTableScan`: the pushed filters' selectivity
        // scales the lookup's rows.
        let selectivity = crate::access_path::ordinary::filter_selectivity(
            ds,
            &table_filters,
            &ctx.access_path_derivation_context(),
        );
        selection_base.base.set_stats(
            table_scan
                .stats_info()
                .map(|stats| stats.scale_by_expect_cnt(selectivity * output_rows, ctx.skew_ratio)),
        );
        selection_base.base.set_schema(Some(schema));
        selection_base.set_children(vec![table_scan]);
        PhysicalPlan::Selection(tidb_planner_core_operator_physicalop::PhysicalSelection {
            base: selection_base,
            conditions: table_filters,
            from_data_source: true,
        })
    };

    // Go seals the index phase when table-side filtering has begun.
    let index_plan_finished = !matches!(&table_plan, PhysicalPlan::TableScan(_));
    Ok(Task::Cop(tidb_planner_core_operator_physicalop::task::CopTask {
        phys_plan_part_info: Some(tidb_planner_core_operator_physicalop::PhysPlanPartInfo::from_data_source(ds)),
        table_plan: Some(Box::new(table_plan)),
        idx_merge_part_plans: partial_plans_raw,
        idx_merge_is_intersection: path.is_intersection,
        idx_merge_access_mv_index: path.access_mv_index,
        index_plan_finished,
        stats_context: ctx.task_stats_context(ds, &root_task_conds),
        root_task_conds,
        common_handle_cols: ds.common_handle_cols.clone(),
        ..Default::default()
    }))
}
