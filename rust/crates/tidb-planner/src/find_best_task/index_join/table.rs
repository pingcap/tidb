// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go buildDataSource2TableScanByIndexJoinProp / constructDS2TableScanTask.

use super::path::IndexJoinPathRangeBuilder;
use crate::{
    access_path::PossiblePath,
    find_best_task::dispatch::DispatchContext,
    logical::DataSource,
    physical::{
        BasePhysicalPlan, IndexJoinInfo, PhysicalPlan, PhysicalSelection, PhysicalTableScan,
    },
    physical_property::PhysicalProperty,
    plan_base::PlanError,
    stats_info::StatsInfo,
    task::{CopTask, Task},
};
use std::sync::Arc;
use tidb_expr::{expression::Expression, schema::Schema};

pub(super) fn build_table_task(
    ds: &DataSource,
    prop: &PhysicalProperty,
    ctx: &DispatchContext<'_>,
) -> Result<Task, PlanError> {
    let common = !ds.common_handle_cols.is_empty();
    if !ds.enumerated_paths.iter().any(|path| {
        matches!(path, PossiblePath::Table { is_int_handle, .. } if *is_int_handle != common)
    }) {
        return Ok(Task::invalid_task());
    }
    let schema = ds
        .base
        .base
        .schema()
        .ok_or_else(|| PlanError::internal("IndexJoin datasource schema missing"))?;
    let lookup = prop.index_join.as_ref().expect("IndexJoin property");
    let (ranges, filters, access, info, pk_column, max_one_row, keep_order) = if common {
        let settings = &ctx.index_join_ranger;
        let record_fallback = |quota| settings.range_fallbacks.borrow_mut().push(quota);
        let builder = IndexJoinPathRangeBuilder {
            columns: &ds.common_handle_cols,
            lengths: &ds.common_handle_lens,
            lookup,
            inner_schema: schema,
            pushed_conditions: &ds.pushed_down_conds,
            eval_constant: settings.eval_constant,
            range_max_size: settings.range_max_size,
            record_range_fallback: &record_fallback,
            regard_null_as_point: settings.regard_null_as_point,
            opt_prefix_index_single_scan: settings.opt_prefix_index_single_scan,
        };
        let result = if settings.use_plan_cache {
            builder.build_for_plan_cache()
        } else {
            builder.build(false)
        };
        let (result, empty) = match result {
            Ok(result) => result,
            Err(error) => {
                // Go records the failed path and continues the candidate loop.
                settings.range_errors.borrow_mut().push(error);
                return Ok(Task::invalid_task());
            }
        };
        let Some(result) = result.filter(|result| !empty && !result.ranges.is_empty()) else {
            return Ok(Task::invalid_task());
        };
        let max_one_row = result.max_one_row(true, ds.common_handle_cols.len());
        // The native order matcher currently represents the full-length key
        // prefix. Const-column skipping and merge-sort matching still belong
        // to the complete ordinary access-path metadata integration.
        let keep_order = !prop.is_sort_item_empty()
            && prop.all_same_order().0
            && prop.sort_items.len() <= ds.common_handle_cols.len()
            && prop
                .sort_items
                .iter()
                .zip(ds.common_handle_cols.iter().zip(&ds.common_handle_lens))
                .all(|(item, (column, length))| *length < 0 && item.col == column.unique_id);
        let info = IndexJoinInfo {
            key_off_to_idx_off: result.key_to_index(lookup.inner_join_keys().len()),
            idx_col_lens: ds.common_handle_lens.clone(),
            ranges: result.ranges.clone(),
            range_template: result.range_template,
            compare_filters: result.compare_filters,
        };
        (
            result.ranges,
            result.remained,
            result.accesses,
            info,
            None,
            max_one_row,
            keep_order,
        )
    } else {
        let Some(pk) = ds.get_pk_is_handle_col(schema) else {
            return Ok(Task::invalid_task());
        };
        let offsets = lookup
            .inner_join_keys()
            .iter()
            .map(|key| if key.unique_id == pk.unique_id { 0 } else { -1 })
            .collect::<Vec<_>>();
        if offsets.iter().all(|offset| *offset < 0) {
            return Ok(Task::invalid_task());
        }
        (
            crate::ranger::points::full_int_range(
                pk.ret_type.as_ref().is_some_and(|ty| ty.is_unsigned()),
            ),
            ds.pushed_down_conds.clone(),
            Vec::new(),
            IndexJoinInfo {
                key_off_to_idx_off: offsets,
                ..Default::default()
            },
            Some(pk.clone()),
            true,
            !prop.is_sort_item_empty()
                && crate::find_best_task::dispatch::table_path_matches_order(ds, prop),
        )
    };
    if keep_order && !ds.partition_definition_names.is_empty() {
        return Ok(Task::invalid_task());
    }
    // Selectivity applies to residuals only, not to the access predicates
    // subsequently restored as explicit probe-side filters by Go.
    let selectivity = if filters.is_empty() {
        1.0
    } else {
        let selectivity = ctx.index_join_selectivity.ok_or_else(|| {
            PlanError::internal(
                "IndexJoin filtered inner scan requires table-statistics selectivity",
            )
        })?;
        match selectivity(ds, &filters) {
            Ok(value) if value > 0.0 => value,
            _ => crate::cost_factors::SELECTION_FACTOR,
        }
    };
    let rows = if lookup.avg_inner_row_count() <= 0.0 {
        1.0
    } else {
        lookup.avg_inner_row_count()
    };
    let scan_rows = rows / selectivity;
    let scan_rows = if max_one_row {
        scan_rows.min(1.0)
    } else {
        scan_rows
    };
    let mut base = base(ds, schema, ctx, "TableScan", scan_rows);
    let (mut pushed, root_filters): (Vec<_>, Vec<_>) = filters
        .into_iter()
        .partition(|filter| !contains_large_in_list(filter, 10_000));
    let inner_access = access
        .iter()
        .filter(|condition| tidb_expr::expr_util::expr_from_schema(condition, schema))
        .cloned()
        .collect::<Vec<_>>();
    pushed = crate::ranger::detacher::append_conditions_if_not_exist(pushed, &inner_access);
    let mut scan = PhysicalPlan::TableScan(PhysicalTableScan {
        base,
        table_id: ds.physical_table_id,
        keep_order,
        desc: keep_order && prop.sort_items[0].desc,
        ranges,
        access_conditions: access,
        pk_column,
        common_handle_cols: ds.common_handle_cols.clone(),
        common_handle_lens: ds.common_handle_lens.clone(),
        ..Default::default()
    });
    if !pushed.is_empty() {
        base = self::base(ds, schema, ctx, "Selection", scan_rows * selectivity);
        base.set_children(vec![scan]);
        scan = PhysicalPlan::Selection(PhysicalSelection {
            base,
            conditions: pushed,
            from_data_source: true,
        });
    }
    Ok(Task::Cop(CopTask {
        table_plan: Some(Box::new(scan)),
        index_plan_finished: true,
        keep_order,
        root_task_conds: root_filters,
        index_join_info: Some(Arc::new(info)),
        ..Default::default()
    }))
}

fn base(
    ds: &DataSource,
    schema: &Schema,
    ctx: &DispatchContext<'_>,
    name: &str,
    rows: f64,
) -> BasePhysicalPlan {
    let mut base = BasePhysicalPlan::new(ctx.allocator, name, ds.base.base.query_block_offset());
    base.base.set_schema(Some(schema.clone()));
    base.base.set_stats(Some(StatsInfo::new(rows, [])));
    base
}

/// Go containsLargeInList; NOT IN is the recursive NOT(IN(...)) case.
fn contains_large_in_list(expression: &Expression, threshold: usize) -> bool {
    let Expression::ScalarFunction(function) = expression else {
        return false;
    };
    (function.func_name.lowercase() == "in" && function.args.len().saturating_sub(1) > threshold)
        || function
            .args
            .iter()
            .any(|arg| contains_large_in_list(arg, threshold))
}
