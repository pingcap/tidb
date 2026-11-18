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

//! Go master `MViewDeltaMergeAggExec` (`pkg/executor/mviewdeltamergeagg`)
//! and its builder (`mview_delta_merge_agg_builder.go`): FAST refresh's
//! merge source -- the log window's per-group deltas left-joined to the
//! view -- read chunk by chunk, each aggregate merged in dependency order
//! (COUNT(*) first), each group inserted into, updated in or deleted from the
//! view (and its MV log, when it has one) by its new COUNT(*).
//!
//! Go merges on a reader / workers / writer pipeline; this merges and writes
//! each chunk in turn, which writes the same rows.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_planner::common_plans::MViewDeltaMerge;
use tidb_planner::mview::{AggInfo, AggKind, BuildResult, DiffHandleCols};
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_table::tables::TableHandle;

use super::{DriverError, SessionExtendedInfoSchema, TableEntry};
use crate::WithStatementStore as _;

fn plain(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message.into()))
}

/// Go master `mergeWriterStats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeltaMergeWriterStats {
    /// Chunks written.
    pub chunks: i64,
    /// Row operations planned.
    pub row_ops: i64,
    /// Updates that changed nothing.
    pub noop_rows: i64,
    /// Rows inserted into the view.
    pub insert_rows: i64,
    /// Rows updated in the view.
    pub update_rows: i64,
    /// Rows deleted from the view.
    pub delete_rows: i64,
}

impl DeltaMergeWriterStats {
    /// Go `affectedRows`.
    #[must_use]
    pub fn affected_rows(self) -> u64 {
        u64::try_from(self.insert_rows + self.update_rows + self.delete_rows).unwrap_or_default()
    }

    /// Go `stmtMessage`.
    #[must_use]
    pub fn stmt_message(self) -> String {
        format!("Rows inserted: {}  Updated: {}  Deleted: {}", self.insert_rows, self.update_rows, self.delete_rows)
    }
}

/// Go master `mergeRuntimeStats`, for this single merge worker.
#[derive(Clone, Copy, Debug, Default)]
struct DeltaMergeRuntimeStats {
    reader_time: std::time::Duration,
    worker_time: std::time::Duration,
    writer_time: std::time::Duration,
    writer: DeltaMergeWriterStats,
}

impl tidb_execdetails::RuntimeStats for DeltaMergeRuntimeStats {
    /// Go `mergeRuntimeStats.String`.
    fn string(&self) -> String {
        let format = tidb_execdetails::format_duration;
        let active = usize::from(!self.worker_time.is_zero());
        let workers = if active > 0 {
            format!(
                ", min:{}, max:{}, avg:{}",
                format(self.worker_time),
                format(self.worker_time),
                format(self.worker_time)
            )
        } else {
            String::new()
        };
        format!(
            "mview_delta_merge_agg:{{merge_worker:{{total:1, active:{active}{workers}}}, reader:{}, writer:{{time:{}, chunks:{}, row_ops:{}, rows:{{insert:{}, update:{}, delete:{}, noop:{}}}}}}}",
            format(self.reader_time),
            format(self.writer_time),
            self.writer.chunks,
            self.writer.row_ops,
            self.writer.insert_rows,
            self.writer.update_rows,
            self.writer.delete_rows,
            self.writer.noop_rows
        )
    }

    fn merge(&mut self, other: &dyn tidb_execdetails::RuntimeStats) {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self.reader_time += other.reader_time;
            self.worker_time += other.worker_time;
            self.writer_time += other.writer_time;
            self.writer.chunks += other.writer.chunks;
            self.writer.row_ops += other.writer.row_ops;
            self.writer.noop_rows += other.writer.noop_rows;
            self.writer.insert_rows += other.writer.insert_rows;
            self.writer.update_rows += other.writer.update_rows;
            self.writer.delete_rows += other.writer.delete_rows;
        }
    }

    fn clone_box(&self) -> Box<dyn tidb_execdetails::RuntimeStats> {
        Box::new(*self)
    }

    fn tp(&self) -> i32 {
        tidb_execdetails::TP_MVIEW_DELTA_MERGE_AGG_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Go `mview.Build` over the plan's window, with the catalog's view, base
/// table and log.
///
/// # Errors
///
/// The view is missing or malformed for FAST refresh.
pub fn build_merge_source(
    plan: &MViewDeltaMerge,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<BuildResult, DriverError> {
    let Some((_, mv)) = catalog.table_info_by_name(&plan.database, &plan.name) else {
        return Err(DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[(&plan.database).into(), (&plan.name).into()]),
        ));
    };
    let mv = mv.read().clone_like_go();
    let table_by_id = |id: i64| catalog.table_info_by_id(id).map(|table| table.read().clone_like_go());
    let parse = |sql: &str| ctx.parse(sql).map_err(|error| error.to_string());
    tidb_planner::mview::build(&mv, &plan.database, &table_by_id, plan.options, &parse).map_err(plain)
}

/// Go master `formatMViewDeltaMergeAggDependency`.
fn format_agg_dependency(info: &AggInfo) -> String {
    let name = match info.kind {
        AggKind::CountStar => "count(*)".to_owned(),
        kind => {
            let kind = match kind {
                AggKind::Count => "count",
                AggKind::Sum => "sum",
                AggKind::Min => "min",
                AggKind::Max => "max",
                AggKind::CountStar => unreachable!(),
            };
            if info.arg_col_name.is_empty() {
                kind.to_owned()
            } else {
                format!("{kind}({})", info.arg_col_name)
            }
        }
    };
    let deps = info.dependencies.iter().map(usize::to_string).collect::<Vec<_>>().join(",");
    format!("{name}@{}->[{deps}]", info.mv_offset)
}

/// Go master `MViewDeltaMerge.ExplainInfo`.
fn explain_info(build: &BuildResult) -> String {
    format!("agg_deps:[{}]", build.agg_infos.iter().map(format_agg_dependency).collect::<Vec<_>>().join(", "))
}

/// Go master `mviewRefreshMLogIsEmpty`: the log side of the merge source
/// (projections over the LEFT JOIN's left child, the grouped aggregation of
/// the log) planned to a zero-row dual.
fn mlog_is_empty(plan: &PhysicalPlan) -> bool {
    let mut plan = plan;
    loop {
        let children = plan.children();
        match plan {
            PhysicalPlan::TableDual(dual) => return dual.row_count == 0,
            PhysicalPlan::HashJoin(join) => {
                if join.join_type != tidb_planner_core_base::LogicalJoinType::LeftOuter || children.len() != 2 {
                    return false;
                }
                plan = &children[0];
                continue;
            }
            PhysicalPlan::HashAgg(agg) if agg.group_by_items.is_empty() => return false,
            PhysicalPlan::StreamAgg(agg) if agg.group_by_items.is_empty() => return false,
            PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_) | PhysicalPlan::Projection(_) | PhysicalPlan::Selection(_) => {}
            _ => return false,
        }
        if children.len() != 1 {
            return false;
        }
        plan = &children[0];
    }
}

/// Go master's `MViewDeltaMerge` physical plan: the operator over its
/// optimized merge source, as EXPLAIN shows it, and whether the log side is
/// empty.
///
/// # Errors
///
/// The merge source fails to plan or has an unexpected width.
pub fn plan_mview_delta_merge(
    plan: &MViewDeltaMerge,
    build: &BuildResult,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    retained_upper_tso: u64,
) -> Result<(PhysicalPlan, bool), DriverError> {
    // Go master `WithMLogCommitTSEstimation` around the merge source's
    // optimization: the retained log window the commit-ts filters are
    // estimated over.
    let plan_ctx = ctx.clone().with_mlog_commit_ts_estimation(tidb_exprctx::MLogCommitTSEstimation {
        mlog_table_id: build.mlog_table_id,
        retained_lower_tso: plan.mlog_retained_lower_tso,
        retained_upper_tso,
    });
    let source = super::planner_bridge::physical_select_plan(&build.merge_source_select, catalog, current_db, &plan_ctx)
        .map_err(super::planner_error_to_driver)?;
    let width = source.schema().map_or(0, |schema| schema.columns.len());
    if width != build.source_column_count {
        return Err(plain(format!(
            "unexpected merge-source schema length: got {width}, expected {}",
            build.source_column_count
        )));
    }
    let empty = mlog_is_empty(&source);
    let plan_ids = tidb_planner_core_base::PlanIdAllocator::new();
    Ok((
        PhysicalPlan::Dml(tidb_planner_core_operator_physicalop::PhysicalDmlRoot {
            base: tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(&plan_ids, "MViewDeltaMerge", 0),
            go_operator: "MViewDeltaMerge".to_owned(),
            explain_info: Some(explain_info(build)),
            select_plan: Some(Box::new(source)),
            ..Default::default()
        }),
        empty,
    ))
}

/// Go master `buildMViewDeltaMergeAggMappings`' order: COUNT(*) first, then
/// every aggregate after the ones whose merged value it reads (Kahn).
fn merge_order(build: &BuildResult) -> Result<Vec<usize>, DriverError> {
    let infos = &build.agg_infos;
    let delta = build.delta_column_count;
    let count_star = infos
        .iter()
        .position(|info| info.kind == AggKind::CountStar)
        .ok_or_else(|| plain("MViewDeltaMerge aggregate mappings require COUNT(*)"))?;
    let output_of = |index: usize| delta + infos[index].mv_offset;
    let mut indegree = vec![0usize; infos.len()];
    let mut adjacent = vec![Vec::new(); infos.len()];
    for (to, info) in infos.iter().enumerate() {
        let mut from_seen = Vec::new();
        for &dep in &info.dependencies {
            if dep < delta {
                continue;
            }
            let from = (0..infos.len()).find(|&from| output_of(from) == dep).ok_or_else(|| {
                plain(format!(
                    "MViewDeltaMerge dependency col id {dep} for mapping {to} (output cols [{}]) does not map to any aggregate output",
                    output_of(to)
                ))
            })?;
            if !from_seen.contains(&from) {
                from_seen.push(from);
                adjacent[from].push(to);
                indegree[to] += 1;
            }
        }
    }
    if indegree[count_star] != 0 {
        return Err(plain(format!(
            "MViewDeltaMerge COUNT(*) mapping at index {count_star} has non-zero indegree {}",
            indegree[count_star]
        )));
    }
    let mut queue = vec![count_star];
    queue.extend((0..infos.len()).filter(|&index| index != count_star && indegree[index] == 0));
    let mut head = 0;
    while head < queue.len() {
        let current = queue[head];
        head += 1;
        for &to in &adjacent[current] {
            indegree[to] -= 1;
            if indegree[to] == 0 {
                queue.push(to);
            }
        }
    }
    if queue.len() != infos.len() {
        let mut unresolved: Vec<usize> = (0..infos.len()).filter(|index| !queue.contains(index)).map(output_of).collect();
        unresolved.sort_unstable();
        return Err(plain(format!(
            "MViewDeltaMerge aggregate dependencies are unresolved or cyclic, unresolved output column ids: {unresolved:?}"
        )));
    }
    Ok(queue)
}

fn datum_i64(datum: &Datum) -> Option<i64> {
    match datum {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn overflow(error: tidb_datatype::OverflowError) -> DriverError {
    // Go `types.AddInt64`'s `ErrOverflow` (1690) with the operands.
    DriverError::Mysql(tidb_terror::MysqlError::from_terror(
        &tidb_datatype::types_errors::ERR_OVERFLOW.gen_with_stack(error.to_string()),
    ))
}

/// Go master's `sum*Merger`: `old + delta`, typed by the view column.
fn add_sum(old: &Datum, delta: &Datum) -> Result<Datum, DriverError> {
    Ok(match (old, delta) {
        (Datum::Int(old), Datum::Int(delta)) => Datum::Int(tidb_datatype::add_int64(*old, *delta).map_err(overflow)?),
        (Datum::UInt(old), Datum::Int(delta)) => Datum::UInt(tidb_datatype::add_integer(*old, *delta).map_err(overflow)?),
        (Datum::UInt(old), Datum::UInt(delta)) => Datum::UInt(tidb_datatype::add_uint64(*old, *delta).map_err(overflow)?),
        (Datum::Real(old), Datum::Real(delta)) => Datum::Real(old + delta),
        (Datum::Decimal(old), Datum::Decimal(delta)) => Datum::Decimal(old.add(delta)),
        (old, delta) => return Err(plain(format!("SUM merge cannot add {delta:?} to {old:?}"))),
    })
}

/// A delta added to a view row with no SUM yet (Go's `oldIsNull` arms).
fn sum_from_delta(delta: &Datum, view_unsigned: bool) -> Result<Datum, DriverError> {
    match delta {
        Datum::Int(value) if view_unsigned => Ok(Datum::UInt(tidb_datatype::add_integer(0, *value).map_err(overflow)?)),
        other => Ok(other.clone()),
    }
}

/// Go master `countMerger` / `sum*Merger.mergeChunk` for one row: the
/// aggregate's merged value. `merged` holds the values merged before it.
fn merge_value(
    info: &AggInfo,
    row: &[Datum],
    merged: &std::collections::HashMap<usize, Datum>,
    output: usize,
    view_unsigned: bool,
) -> Result<Datum, DriverError> {
    let old = &row[output];
    let delta = &row[info.dependencies[0]];
    match info.kind {
        AggKind::CountStar | AggKind::Count => {
            let Some(delta) = datum_i64(delta) else {
                return Err(plain("count delta contains null"));
            };
            let old = datum_i64(old).unwrap_or(0);
            let new = tidb_datatype::add_int64(old, delta).map_err(overflow)?;
            if new < 0 {
                return Err(plain(format!("count becomes negative ({new})")));
            }
            Ok(Datum::Int(new))
        }
        AggKind::Sum => {
            if info.dependencies.len() == 2 {
                let count_column = info.dependencies[1];
                let count = merged.get(&count_column).and_then(datum_i64);
                let Some(count) = count else {
                    return Err(plain("sum count dependency contains null"));
                };
                if count < 0 {
                    return Err(plain(format!("sum count dependency becomes negative ({count})")));
                }
                if count == 0 {
                    return Ok(Datum::Null);
                }
                return match (old, delta) {
                    (Datum::Null, Datum::Null) => Ok(Datum::Null),
                    (Datum::Null, delta) => sum_from_delta(delta, view_unsigned),
                    (old, Datum::Null) => Ok(old.clone()),
                    (old, delta) => add_sum(old, delta),
                };
            }
            match old {
                Datum::Null => sum_from_delta(delta, view_unsigned),
                old => add_sum(old, delta),
            }
        }
        AggKind::Min | AggKind::Max => Err(plain("MIN / MAX merge is not supported on this node yet")),
    }
}

/// Go master `MViewDeltaMergeAggExec`: merges the window into the view and
/// answers the write counts.
///
/// # Errors
///
/// The merge source fails to plan or run, a merge fails, or a write fails.
pub fn run_mview_delta_merge(
    plan: &MViewDeltaMerge,
    build: &BuildResult,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    retained_upper_tso: u64,
) -> Result<DeltaMergeWriterStats, DriverError> {
    let (mut root, mlog_empty) = plan_mview_delta_merge(plan, build, catalog, current_db, ctx, retained_upper_tso)?;
    crate::explain::publish_physical_process_info(ctx, &root, catalog)?;
    let dml = super::physical_builder::DmlExec::new(&root, ctx);
    let root_id = i64::from(root.id());
    let mut stats = DeltaMergeRuntimeStats::default();
    if mlog_empty {
        // Go `Open` with `MLogEmpty`: nothing to merge.
        if let Some(runtime) = ctx.executor_runtime_stats_source() {
            runtime.register_stats(root_id, Box::new(stats));
        }
        return Ok(stats.writer);
    }
    let order = merge_order(build)?;
    let delta = build.delta_column_count;
    // Go `buildTableResultWriter`'s checks over the view.
    let (target_types, unsigned) = match catalog.get_in(&plan.database, &plan.name) {
        Some(TableEntry::Kv(kv)) => {
            let meta = kv.meta();
            let meta = meta.read();
            if kv.writable_columns().len() != meta.columns.len() {
                return Err(plain("Exec does not support target table with non-public writable columns"));
            }
            let types: Vec<_> = meta.columns.iter_deref().map(|column| column.read().field_type.clone()).collect();
            let unsigned: Vec<bool> =
                types.iter().map(|tp| tidb_mysql::types::has_unsigned_flag(tp.flags() as usize)).collect();
            (types, unsigned)
        }
        _ => return Err(plain(format!("MViewDeltaMerge target table id {} not found in infoschema", build.mv_table_id))),
    };
    let tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(root_plan) = &mut root else {
        unreachable!("an MViewDeltaMerge plan is a DML root")
    };
    let physical = root_plan.select_plan.as_deref_mut().expect("the merge source");
    let mut source = super::DmlSourceCursor::open(
        super::physical_builder::build_dml_source(physical, catalog, ctx)?,
        ctx.statement_memory(),
        None,
        ctx.chunk_allocator(),
    )?;
    let source_types = source.field_types().to_vec();
    for (index, target) in target_types.iter().enumerate() {
        if !target.equal(&source_types[delta + index]) {
            return Err(plain(format!(
                "target writable mapping[{index}]={} type mismatch, target col expects {target} but input is {}",
                delta + index,
                source_types[delta + index]
            )));
        }
    }
    // Go `wrapTableWithMLogIfExists(target, tables.MLogSourceRefresh)`.
    let mlogs = super::dml::MLogTables::wrap(
        catalog,
        [(plan.database.as_str(), plan.name.as_str())],
        tidb_table::tables::mview_log::MLogSourceStmt::Refresh,
    )?;
    let mlog = mlogs.get(&plan.database, &plan.name);
    let zone = tidb_exprctx::Columns::time_zone(ctx);
    let agg_outputs: Vec<usize> = order.iter().map(|&index| build.agg_infos[index].mv_offset).collect();

    let result = dml.next(|| {
        loop {
            let read_start = std::time::Instant::now();
            let Some((rows, _)) = source.next_chunk()? else {
                stats.reader_time += read_start.elapsed();
                break;
            };
            stats.reader_time += read_start.elapsed();
            stats.writer.chunks += 1;
            for row in rows.iter() {
                // Go `mergeOneChunk`: each aggregate in order.
                let work_start = std::time::Instant::now();
                let mut merged = std::collections::HashMap::new();
                for &index in &order {
                    let info = &build.agg_infos[index];
                    let output = delta + info.mv_offset;
                    let value = merge_value(info, row, &merged, output, unsigned[info.mv_offset])?;
                    merged.insert(output, value);
                }
                stats.worker_time += work_start.elapsed();
                // Go `buildRowOps`.
                let count_star = delta + build.count_star_mv_offset;
                let new_count = merged.get(&count_star).and_then(datum_i64).unwrap_or(0);
                let old_exists = !matches!(row[count_star], Datum::Null);
                let view_row = |offset: usize| row[delta + offset].clone();
                let old: Vec<Datum> = (0..build.mv_column_count).map(view_row).collect();
                let mut new = old.clone();
                for &mv_offset in &agg_outputs {
                    new[mv_offset] = merged[&(delta + mv_offset)].clone();
                }
                let write_start = std::time::Instant::now();
                let Some(TableEntry::Kv(kv)) = catalog.get_rows_mut_in(&plan.database, &plan.name) else {
                    return Err(plain("table storage changed during FAST refresh"));
                };
                let kv = Arc::make_mut(kv);
                let handle = |kv: &mut tidb_table::tables::TableCommon| -> Result<TableHandle, DriverError> {
                    match &build.mv_table_pk_cols {
                        DiffHandleCols::IntPk { source_offset } | DiffHandleCols::RowId { source_offset } => {
                            match &row[*source_offset] {
                                Datum::Int(value) => Ok(TableHandle::Int(*value)),
                                Datum::UInt(value) => Ok(TableHandle::Int(*value as i64)),
                                other => Err(plain(format!("MViewDeltaMerge invalid handle {other:?}"))),
                            }
                        }
                        DiffHandleCols::Common { source_offsets } => {
                            let values: Vec<Datum> = source_offsets.iter().map(|&offset| row[offset].clone()).collect();
                            kv.common_handle_from_values(&values, &zone).map_err(super::dml::kv_write_error)
                        }
                    }
                };
                if new_count == 0 {
                    if old_exists {
                        stats.writer.row_ops += 1;
                        stats.writer.delete_rows += 1;
                        let handle = handle(kv)?;
                        kv.with_statement_store(ctx, |table, store| table.delete_row_with_old_context(store, &handle, &old, ctx))
                            .map_err(super::dml::kv_write_error)?;
                        if let Some(mlog) = mlog {
                            let mut store = crate::statement_store(ctx);
                            mlog.borrow_mut().remove_record(&mut *store, &old, ctx).map_err(super::dml::kv_write_error)?;
                        }
                    }
                } else if !old_exists {
                    stats.writer.row_ops += 1;
                    stats.writer.insert_rows += 1;
                    let shard = if kv.shard_row_id_bits() > 0 { ctx.next_row_id_shard(1) as i64 } else { 0 };
                    kv.with_statement_store(ctx, |table, store| {
                        table.insert_row_with_row_id_checked(store, &new, None, shard, ctx, true)
                    })
                    .map_err(super::dml::kv_write_error)?;
                    if let Some(mlog) = mlog {
                        let mut store = crate::statement_store(ctx);
                        mlog.borrow_mut().add_record(&mut *store, &new, false, true, ctx).map_err(super::dml::kv_write_error)?;
                    }
                } else {
                    stats.writer.row_ops += 1;
                    // Go `MarkTouchedRowsByColumn` over the aggregate outputs.
                    let touched: Vec<usize> = agg_outputs
                        .iter()
                        .copied()
                        .filter(|&offset| super::mview_delta_apply::values_differ(&old[offset], &new[offset]))
                        .collect();
                    if touched.is_empty() {
                        stats.writer.noop_rows += 1;
                    } else {
                        stats.writer.update_rows += 1;
                        let handle = handle(kv)?;
                        kv.with_statement_store(ctx, |table, store| {
                            table.update_row_with_old_context(store, &handle, Some(&old), &new, ctx)
                        })
                        .map_err(super::dml::kv_write_error)?;
                        if let Some(mlog) = mlog {
                            let mut store = crate::statement_store(ctx);
                            mlog.borrow_mut()
                                .update_record(&mut *store, &old, &new, &|offset| touched.contains(&offset), true, ctx)
                                .map_err(super::dml::kv_write_error)?;
                        }
                    }
                }
                stats.writer_time += write_start.elapsed();
            }
        }
        Ok(())
    });
    let closed = dml.close(|| source.close());
    // Go `defer RuntimeStatsColl.RegisterStats(e.ID(), e.runtimeStats)`.
    if let Some(runtime) = ctx.executor_runtime_stats_source() {
        runtime.register_stats(root_id, Box::new(stats));
    }
    result?;
    closed?;
    Ok(stats.writer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(kind: AggKind, mv_offset: usize, dependencies: Vec<usize>) -> AggInfo {
        AggInfo { kind, mv_offset, arg_col_name: String::new(), arg_not_null: false, dependencies }
    }

    #[test]
    fn explain_info_as_go() {
        let mut sum = info(AggKind::Sum, 1, vec![1]);
        sum.arg_col_name = "b".to_owned();
        assert_eq!(format_agg_dependency(&sum), "sum(b)@1->[1]");
        assert_eq!(format_agg_dependency(&info(AggKind::CountStar, 2, vec![0])), "count(*)@2->[0]");
    }

    #[test]
    fn counts_and_nullable_sums_merge_as_go() {
        let merged = std::collections::HashMap::new();
        // [cnt_star_delta, sum_delta, cnt_delta | a, s, c, cnt]
        let row = vec![Datum::Int(-1), Datum::Null, Datum::Int(-1), Datum::Int(1), Datum::Int(5), Datum::Int(1), Datum::Int(2)];
        let count = info(AggKind::Count, 2, vec![2]);
        assert_eq!(merge_value(&count, &row, &merged, 5, false).unwrap(), Datum::Int(0));
        let mut merged = std::collections::HashMap::new();
        merged.insert(5, Datum::Int(0));
        let sum = info(AggKind::Sum, 1, vec![1, 5]);
        assert_eq!(merge_value(&sum, &row, &merged, 4, false).unwrap(), Datum::Null);
        let negative = vec![Datum::Int(-3), Datum::Null, Datum::Null, Datum::Int(1), Datum::Null, Datum::Null, Datum::Int(2)];
        let count_star = info(AggKind::CountStar, 3, vec![0]);
        assert_eq!(
            merge_value(&count_star, &negative, &merged, 6, false).unwrap_err().to_string(),
            "count becomes negative (-1)"
        );
    }
}
