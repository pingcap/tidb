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
use tidb_planner::mview::{DiffHandleCols, DIFF_OP_DELETE, DIFF_OP_INSERT, DIFF_OP_UPDATE};
use tidb_table::tables::TableHandle;

use super::{DriverError, SessionExtendedInfoSchema, TableEntry};
use crate::WithStatementStore as _;

/// Go master `mviewCompleteDeltaApplyWriterStats`: the statement's write
/// counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeltaApplyWriterStats {
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
fn values_differ(old: &Datum, new: &Datum) -> bool {
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
    // Go `buildRefreshMaterializedViewImplement`: the diff source is
    // optimized with FULL OUTER JOIN enabled and the cascades planner off.
    let plan_ctx = ctx.clone().with_full_outer_join(true).with_cascades_planner(false);
    let mut physical = super::planner_bridge::physical_select_plan(&diff.diff_source_select, catalog, current_db, &plan_ctx)
        .map_err(super::planner_error_to_driver)?;
    let source_len = physical.schema().map_or(0, |schema| schema.columns.len());
    diff.validate_source_layout(source_len).map_err(plain)?;

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
        super::physical_builder::build_dml_source(&mut physical, catalog, ctx)?,
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
    let result = (|| {
        while let Some((rows, _)) = source.next_chunk()? {
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
                        DiffHandleCols::IntPk { source_offset } | DiffHandleCols::RowId { source_offset } => {
                            match &row[*source_offset] {
                                Datum::Int(value) => Ok(TableHandle::Int(*value)),
                                Datum::UInt(value) => Ok(TableHandle::Int(*value as i64)),
                                other => Err(plain(format!("MViewCompleteDeltaApply invalid handle {other:?}"))),
                            }
                        }
                        DiffHandleCols::Common { source_offsets } => {
                            let values: Vec<Datum> = source_offsets.iter().map(|&offset| row[offset].clone()).collect();
                            kv.common_handle_from_values(&values, &zone).map_err(super::dml::kv_write_error)
                        }
                    }
                };
                match op {
                    DIFF_OP_INSERT => {
                        stats.insert_rows += 1;
                        let new = image(&diff.recomputed_row_offsets);
                        let shard = if kv.shard_row_id_bits() > 0 { ctx.next_row_id_shard(1) as i64 } else { 0 };
                        kv.with_statement_store(ctx, |table, store| {
                            table.insert_row_with_row_id_checked(store, &new, None, shard, ctx, true)
                        })
                        .map_err(super::dml::kv_write_error)?;
                        if let Some(mlog) = &mlog {
                            let mut store = crate::statement_store(ctx);
                            mlog.borrow_mut().add_record(&mut *store, &new, false, true, ctx).map_err(super::dml::kv_write_error)?;
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
        }
        Ok(())
    })();
    let closed = source.close();
    result?;
    closed?;
    Ok(stats)
}
