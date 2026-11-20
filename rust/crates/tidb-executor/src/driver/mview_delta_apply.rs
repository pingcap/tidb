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

//! Go master `MViewCompleteDeltaApplyExec`
//! (`pkg/executor/mview_complete_delta_apply.go`) and its builder
//! (`mview_complete_delta_apply_bulder.go`): the COMPLETE DELTA APPLY diff
//! source, optimized with FULL OUTER JOIN on and the cascades planner off as
//! `buildRefreshMaterializedViewImplement` does, read chunk by chunk, each
//! row inserted into, deleted from or updated in the view (and its MV log,
//! when it has one) by its diff op.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_planner::common_plans::MViewCompleteDeltaApply;
use tidb_planner::mview::{DIFF_OP_DELETE, DIFF_OP_INSERT, DIFF_OP_UPDATE};
use tidb_table::tables::TableHandle;

use super::{DriverError, SessionExtendedInfoSchema, TableEntry};
use crate::WithStatementStore as _;

/// Go master `mviewCompleteDeltaApplyRuntimeStats`: the writer's time and
/// counts, under the operator's id in the statement's runtime stats.
#[derive(Clone, Copy, Debug, Default)]
struct DeltaApplyRuntimeStats {
    writer_time: std::time::Duration,
    writer: DeltaApplyWriterStats,
}

impl tidb_execdetails::RuntimeStats for DeltaApplyRuntimeStats {
    /// Go `mviewCompleteDeltaApplyRuntimeStats.String`.
    fn string(&self) -> String {
        format!(
            "mview_complete_delta_apply:{{writer:{{time:{}, chunks:{}, row_ops:{}, rows:{{insert:{}, update:{}, delete:{}}}}}}}",
            tidb_execdetails::format_duration(self.writer_time),
            self.writer.chunks,
            self.writer.row_ops,
            self.writer.insert_rows,
            self.writer.update_rows,
            self.writer.delete_rows
        )
    }

    fn merge(&mut self, other: &dyn tidb_execdetails::RuntimeStats) {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self.writer_time += other.writer_time;
            self.writer.chunks += other.writer.chunks;
            self.writer.row_ops += other.writer.row_ops;
            self.writer.insert_rows += other.writer.insert_rows;
            self.writer.update_rows += other.writer.update_rows;
            self.writer.delete_rows += other.writer.delete_rows;
        }
    }

    fn clone_box(&self) -> Box<dyn tidb_execdetails::RuntimeStats> {
        Box::new(*self)
    }

    fn tp(&self) -> i32 {
        tidb_execdetails::TP_MVIEW_COMPLETE_DELTA_APPLY_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Go master `mviewCompleteDeltaApplyWriterStats`: the statement's write
/// counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeltaApplyWriterStats {
    /// The diff chunks applied.
    pub chunks: i64,
    /// The diff rows applied.
    pub row_ops: i64,
    /// Rows inserted into the view.
    pub insert_rows: i64,
    /// Rows updated in the view.
    pub update_rows: i64,
    /// Rows deleted from the view.
    pub delete_rows: i64,
}

impl DeltaApplyWriterStats {
    /// Go `affectedRows`.
    #[must_use]
    pub fn affected_rows(self) -> u64 {
        u64::try_from(self.insert_rows + self.update_rows + self.delete_rows).unwrap_or_default()
    }

    /// Go `stmtMessage`.
    #[must_use]
    pub fn stmt_message(self) -> String {
        format!(
            "Rows inserted: {}  Updated: {}  Deleted: {}",
            self.insert_rows, self.update_rows, self.delete_rows
        )
    }
}

fn plain(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message.into()))
}

/// Go master `MarkTouchedRowsByColumn` for one row: whether the current and
/// recomputed values differ -- NULL-aware, strings compared by bytes (the
/// binary collator), numbers by value, decimals by `Compare`.
#[must_use]
pub fn values_differ(old: &Datum, new: &Datum) -> bool {
    match (old, new) {
        (Datum::Null, Datum::Null) => false,
        (Datum::Null, _) | (_, Datum::Null) => true,
        (Datum::Decimal(old), Datum::Decimal(new)) => old.cmp(new) != std::cmp::Ordering::Equal,
        (Datum::String(old), Datum::String(new)) => old.bytes() != new.bytes(),
        (Datum::Bytes(old), Datum::Bytes(new)) => old != new,
        (Datum::Int(old), Datum::UInt(new)) | (Datum::UInt(new), Datum::Int(old)) => *old as u64 != *new,
        (old, new) => old != new,
    }
}

/// Go `buildRefreshMaterializedViewImplement`: the diff source optimized
/// with FULL OUTER JOIN enabled and the cascades planner off, its layout
/// checked against the view's.
fn diff_source_physical_plan(
    plan: &MViewCompleteDeltaApply,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    let plan_ctx = ctx.clone().with_full_outer_join(true).with_cascades_planner(false);
    let physical = super::planner_bridge::physical_select_plan(&plan.diff.diff_source_select, catalog, current_db, &plan_ctx)
        .map_err(super::planner_error_to_driver)?;
    let source_len = physical.schema().map_or(0, |schema| schema.columns.len());
    plan.diff.validate_source_layout(source_len).map_err(plain)?;
    Ok(physical)
}

/// Go master `formatMViewDeltaMergeOffsets`.
fn format_offsets(offsets: &[usize]) -> String {
    format!("[{}]", offsets.iter().map(usize::to_string).collect::<Vec<_>>().join(","))
}

/// Go master `MViewCompleteDeltaApply.ExplainInfo`.
fn explain_info(diff: &tidb_planner::mview::CompleteDiffBuildResult) -> String {
    let at = |offsets: &[usize], mv_offset: usize| offsets.get(mv_offset).map_or(-1, |&offset| offset as i64);
    let at_all = |offsets: &[usize]| {
        let picked: Vec<String> = diff.group_key_mv_offsets.iter().map(|&mv| at(offsets, mv).to_string()).collect();
        format!("[{}]", picked.join(","))
    };
    let handle_cols = &diff.current_handle_cols;
    let handle =
        format_offsets(&(0..handle_cols.num_cols()).map(|i| handle_cols.get_col(i).index as usize).collect::<Vec<_>>());
    format!(
        "op_offset:{}, current_marker_offset:{}, recomputed_marker_offset:{}, current_group_keys_offset:{}, recomputed_group_keys_offset:{}, current_handle_offset:{}, current_row_offset:{}, recomputed_row_offset:{}",
        diff.op_col_offset,
        at(&diff.current_row_offsets, diff.marker_mv_offset),
        at(&diff.recomputed_row_offsets, diff.marker_mv_offset),
        at_all(&diff.current_row_offsets),
        at_all(&diff.recomputed_row_offsets),
        handle,
        format_offsets(&diff.current_row_offsets),
        format_offsets(&diff.recomputed_row_offsets),
    )
}

/// Go master's `MViewCompleteDeltaApply` physical plan: the operator over
/// its optimized diff source, as EXPLAIN shows it.
///
/// # Errors
///
/// The diff source fails to plan, or its layout disagrees with the view.
pub fn plan_mview_complete_delta_apply(
    plan: &MViewCompleteDeltaApply,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    let source = diff_source_physical_plan(plan, catalog, current_db, ctx)?;
    let plan_ids = tidb_planner_core_base::PlanIdAllocator::new();
    Ok(tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(tidb_planner_core_operator_physicalop::PhysicalDmlRoot {
        base: tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(&plan_ids, "MViewCompleteDeltaApply", 0),
        go_operator: "MViewCompleteDeltaApply".to_owned(),
        explain_info: Some(explain_info(&plan.diff)),
        select_plan: Some(Box::new(source)),
        ..Default::default()
    }))
}

/// Go master `buildMViewCompleteDeltaApply` + `MViewCompleteDeltaApplyExec`:
/// applies the view's diff rows and answers the write counts.
///
/// # Errors
///
/// The diff source fails to plan or run, its layout disagrees with the view,
/// or a row write fails.
pub fn run_mview_complete_delta_apply(
    plan: &MViewCompleteDeltaApply,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<DeltaApplyWriterStats, DriverError> {
    let diff = &plan.diff;
    // Go `ExecStmt`: the statement's plan is the operator over its diff
    // source, published with the runtime stats its executors collect.
    let mut root = plan_mview_complete_delta_apply(plan, catalog, current_db, ctx)?;
    crate::explain::publish_physical_process_info(ctx, &root, catalog)?;
    let dml = super::physical_builder::DmlExec::new(&root, ctx);
    let root_id = i64::from(root.id());
    let tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(root_plan) = &mut root else {
        unreachable!("an MViewCompleteDeltaApply plan is a DML root")
    };
    let physical = root_plan.select_plan.as_deref_mut().expect("the diff source");

    // Go `buildMViewCompleteDeltaApply`'s checks over the target.
    let (public_len, writable_len, target_types) = match catalog.get_in(&plan.database, &plan.name) {
        Some(TableEntry::Kv(kv)) => {
            let meta = kv.meta();
            let meta = meta.read();
            if meta.id != diff.mv_table_id {
                return Err(plain(format!(
                    "MViewCompleteDeltaApply target table id {} not found in infoschema",
                    diff.mv_table_id
                )));
            }
            let types: Vec<_> = meta.columns.iter_deref().map(|column| column.read().field_type.clone()).collect();
            (meta.columns.len(), kv.writable_columns().len(), types)
        }
        _ => {
            return Err(plain(format!(
                "MViewCompleteDeltaApply target table id {} not found in infoschema",
                diff.mv_table_id
            )))
        }
    };
    if public_len != diff.mv_column_count {
        return Err(plain(format!(
            "MViewCompleteDeltaApply target public column count {public_len} != plan MV column count {}",
            diff.mv_column_count
        )));
    }
    if public_len != writable_len {
        return Err(plain(
            "MViewCompleteDeltaApply does not support target table with non-public writable columns",
        ));
    }
    let mut source = super::DmlSourceCursor::open(
        super::physical_builder::build_dml_source(physical, catalog, ctx)?,
        ctx.statement_memory(),
        None,
        ctx.chunk_allocator(),
    )?;
    // Go `validateMViewCompleteDeltaWritableInputColTypes`, both row images.
    let source_types = source.field_types().to_vec();
    for offsets in [&diff.current_row_offsets, &diff.recomputed_row_offsets] {
        for (index, &offset) in offsets.iter().enumerate() {
            let input = &source_types[offset];
            if !target_types[index].equal(input) {
                return Err(plain(format!(
                    "MViewCompleteDeltaApply writable input col id {offset} type mismatch, target col expects {} but input is {}",
                    target_types[index], input
                )));
            }
        }
    }
    // Go `buildMViewCompleteDeltaCompareMappings`: every non-group-key
    // column is compared for an UPDATE.
    let compare: Vec<usize> = (0..diff.mv_column_count)
        .filter(|offset| !diff.group_key_mv_offsets.contains(offset))
        .collect();
    // Go `wrapTableWithMLogIfExists(target, tables.MLogSourceRefresh)`.
    let mlog = super::dml::wrap_table_with_mlog_if_exists(
        catalog,
        &plan.database,
        &plan.name,
        tidb_table::tables::mview_log::MLogSourceStmt::Refresh,
    )?;
    let zone = tidb_exprctx::Columns::time_zone(ctx);

    let mut stats = DeltaApplyWriterStats::default();
    let mut writer_time = std::time::Duration::ZERO;
    // Go `MViewCompleteDeltaApplyExec.Next`: one call applying every chunk.
    let result = dml.next(|| {
        while let Some((rows, _)) = source.next_chunk()? {
            // Go `applyChunk`'s writer stats delta.
            stats.chunks += 1;
            stats.row_ops += i64::try_from(rows.len()).unwrap_or(i64::MAX);
            let write_start = std::time::Instant::now();
            for (row_index, row) in rows.iter().enumerate() {
                let op = match row.get(diff.op_col_offset) {
                    Some(Datum::Int(op)) => *op,
                    Some(Datum::UInt(op)) => *op as i64,
                    other => {
                        return Err(plain(format!(
                            "MViewCompleteDeltaApply invalid diff op {other:?} at row {row_index}"
                        )))
                    }
                };
                let image = |offsets: &[usize]| offsets.iter().map(|&offset| row[offset].clone()).collect::<Vec<_>>();
                let Some(TableEntry::Kv(kv)) = catalog.get_rows_mut_in(&plan.database, &plan.name) else {
                    return Err(plain("table storage changed during COMPLETE DELTA APPLY"));
                };
                let kv = Arc::make_mut(kv);
                let handle = |kv: &mut tidb_table::tables::TableCommon| -> Result<TableHandle, DriverError> {
                    match &diff.current_handle_cols {
                        tidb_planner_util::handle_cols::HandleCols::Int(int) => {
                            match &row[int.col.index as usize] {
                                Datum::Int(value) => Ok(TableHandle::Int(*value)),
                                Datum::UInt(value) => Ok(TableHandle::Int(*value as i64)),
                                other => Err(plain(format!("MViewCompleteDeltaApply invalid handle {other:?}"))),
                            }
                        }
                        tidb_planner_util::handle_cols::HandleCols::Common(common) => {
                            let values: Vec<Datum> = common.columns.iter().map(|col| row[col.index as usize].clone()).collect();
                            kv.common_handle_from_values(&values, &zone).map_err(super::dml::kv_write_error)
                        }
                    }
                };
                match op {
                    DIFF_OP_INSERT => {
                        stats.insert_rows += 1;
                        let new = image(&diff.recomputed_row_offsets);
                        // Go `AddRecord` without a reservation hint.
                    let reserve_auto_id = 0;
                        kv.with_statement_store(ctx, |table, store| {
                            table.insert_row_with_row_id_checked(store, &new, None, reserve_auto_id, ctx, true)
                        })
                        .map_err(super::dml::kv_write_error)?;
                        if let Some(mlog) = &mlog {
                            let mut store = crate::statement_store(ctx);
                            let mut mlog = mlog.borrow_mut();
                            let had_removed_conflict = mlog.take_removed_conflict();
                            mlog.add_record(&mut *store, &new, false, had_removed_conflict, true, ctx).map_err(super::dml::kv_write_error)?;
                        }
                    }
                    DIFF_OP_DELETE => {
                        stats.delete_rows += 1;
                        let old = image(&diff.current_row_offsets);
                        let handle = handle(kv)?;
                        kv.with_statement_store(ctx, |table, store| table.delete_row_with_old_context(store, &handle, &old, ctx))
                            .map_err(super::dml::kv_write_error)?;
                        if let Some(mlog) = &mlog {
                            let mut store = crate::statement_store(ctx);
                            mlog.borrow_mut().remove_record(&mut *store, &old, ctx).map_err(super::dml::kv_write_error)?;
                        }
                    }
                    DIFF_OP_UPDATE => {
                        let old = image(&diff.current_row_offsets);
                        let recomputed = image(&diff.recomputed_row_offsets);
                        let touched: Vec<usize> =
                            compare.iter().copied().filter(|&offset| values_differ(&old[offset], &recomputed[offset])).collect();
                        if touched.is_empty() {
                            continue;
                        }
                        stats.update_rows += 1;
                        let mut new = old.clone();
                        for &offset in &touched {
                            new[offset] = recomputed[offset].clone();
                        }
                        let handle = handle(kv)?;
                        kv.with_statement_store(ctx, |table, store| {
                            table.update_row_with_old_context(store, &handle, Some(&old), &new, ctx)
                        })
                        .map_err(super::dml::kv_write_error)?;
                        if let Some(mlog) = &mlog {
                            let mut store = crate::statement_store(ctx);
                            mlog.borrow_mut()
                                .update_record(&mut *store, &old, &new, &|offset| touched.contains(&offset), true, ctx)
                                .map_err(super::dml::kv_write_error)?;
                        }
                    }
                    other => {
                        return Err(plain(format!(
                            "MViewCompleteDeltaApply invalid diff op {other} at row {row_index}"
                        )))
                    }
                }
            }
            writer_time += write_start.elapsed();
        }
        Ok(())
    });
    let closed = dml.close(|| source.close());
    // Go `defer RuntimeStatsColl.RegisterStats(e.ID(), e.runtimeStats)`.
    if let Some(runtime) = ctx.executor_runtime_stats_source() {
        runtime.register_stats(root_id, Box::new(DeltaApplyRuntimeStats { writer_time, writer: stats }));
    }
    result?;
    closed?;
    Ok(stats)
}
