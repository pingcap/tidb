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

//! Go master `pkg/executor/mview_delta_merge_agg_builder.go`: the
//! `mviewdeltamergeagg.Exec` of a FAST refresh, built from the planner's
//! merge plan, with its MIN/MAX recompute and the view table it writes.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_datatype::{Datum, FieldType, FieldTypeFlags, SessionTimeZone};
use tidb_driver_error::ExecError;
use tidb_executor_mviewdeltamergeagg::{
    Mapping, MinMaxBatchBuildRequest, MinMaxBatchExecBuilder, MinMaxRecomputeExec, MinMaxRecomputeSpec,
    MinMaxRecomputeStrategy, TargetColumn, TargetTable,
};
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expression_aggregation::{names, AggFuncDesc, AggFunctionMode, BaseFuncDesc};
use tidb_planner::mview::{AggKind, BuildResult};

use tidb_planner::common_plans::MViewFullUpdateLookup as FullUpdateLookup;
use super::DriverError;
use crate::TableEntry;
use crate::WithStatementStore as _;
use tidb_infoschema::SessionExtendedInfoSchema;

fn plain(message: impl Into<String>) -> DriverError {
    DriverError::Exec(ExecError::internal(message.into()))
}

/// Go `mviewDeltaMergeAggFuncName`.
fn mview_delta_merge_agg_func_name(kind: AggKind) -> &'static str {
    match kind {
        AggKind::CountStar | AggKind::Count => names::COUNT,
        AggKind::Sum => names::SUM,
        AggKind::Min => names::MIN,
        AggKind::Max => names::MAX,
    }
}

/// Go `mviewDeltaMergeAggArgExpr`.
fn mview_delta_merge_agg_arg_expr(
    info: &tidb_planner::mview::AggInfo,
    source_field_types: &[FieldType],
) -> Result<Expression, DriverError> {
    if info.kind == AggKind::CountStar {
        return Ok(Expression::Constant(Constant::new_one()));
    }
    let Some(&dep) = info.dependencies.first() else {
        return Err(plain(format!(
            "MViewDeltaMerge aggregate {:?} at mview offset {} has empty dependencies",
            info.kind, info.mv_offset
        )));
    };
    let Some(ret_type) = source_field_types.get(dep) else {
        return Err(plain(format!(
            "MViewDeltaMerge aggregate {:?} at mview offset {} has invalid dependency col id {dep}",
            info.kind, info.mv_offset
        )));
    };
    let mut ret_type = ret_type.clone();
    if matches!(info.kind, AggKind::Min | AggKind::Max) {
        if info.dependencies.len() != 4 && info.dependencies.len() != 5 {
            return Err(plain(format!(
                "MViewDeltaMerge aggregate {:?} at mview offset {} expects 4 or 5 dependencies, got {}",
                info.kind,
                info.mv_offset,
                info.dependencies.len()
            )));
        }
        // MIN/MAX nullability should follow the original aggregate argument nullability instead of
        // nullable delta payload columns.
        if info.arg_not_null {
            ret_type.add_flags(FieldTypeFlags::NOT_NULL);
        } else {
            ret_type.del_flag(FieldTypeFlags::NOT_NULL);
        }
    }
    let mut column = tidb_expr::column::Column::new(dep as i64 + 1, ret_type);
    column.index = dep as i64;
    Ok(Expression::Column(column))
}

/// Go `buildMViewDeltaMergeAggMappings`: one mapping per aggregate in Go's
/// dependency order (COUNT(*) first).
pub(crate) fn build_mview_delta_merge_agg_mappings(
    build: &BuildResult,
    source_field_types: &[FieldType],
) -> Result<Vec<Mapping>, DriverError> {
    let delta = build.delta_column_count;
    super::mview_delta_merge::merge_order(build)?
        .into_iter()
        .map(|index| {
            let info = &build.agg_infos[index];
            let name = mview_delta_merge_agg_func_name(info.kind);
            let arg = mview_delta_merge_agg_arg_expr(info, source_field_types)?;
            let ret_type = match info.kind {
                AggKind::CountStar | AggKind::Count => FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                _ => source_field_types[delta + info.mv_offset].clone(),
            };
            Ok(Mapping {
                agg_func: Some(AggFuncDesc {
                    base: BaseFuncDesc { name: name.to_owned(), args: vec![arg], ret_type },
                    mode: AggFunctionMode::Complete,
                    has_distinct: false,
                    order_by_items: Vec::new(),
                    grouping_id: 0,
                }),
                col_id: vec![(delta + info.mv_offset) as i64],
                dependency_col_id: info.dependencies.iter().map(|&dep| dep as i64).collect(),
                min_max_recompute: None,
            })
        })
        .collect()
}

/// Go `buildMViewDeltaMergeMinMaxResultColByMViewOffset`.
fn build_mview_delta_merge_min_max_result_col_by_mview_offset(
    build: &BuildResult,
    lookup: Option<&FullUpdateLookup>,
) -> Result<HashMap<usize, Option<usize>>, DriverError> {
    let mut result_col_by_offset = HashMap::new();
    for info in &build.agg_infos {
        if info.mv_offset >= build.mv_column_count {
            return Err(plain(format!(
                "MViewDeltaMerge agg mview offset {} out of range [0,{})",
                info.mv_offset, build.mv_column_count
            )));
        }
        if !matches!(info.kind, AggKind::Min | AggKind::Max) {
            continue;
        }
        if result_col_by_offset.insert(info.mv_offset, None).is_some() {
            return Err(plain(format!("MViewDeltaMerge has duplicate min/max agg mview offset {}", info.mv_offset)));
        }
    }
    if result_col_by_offset.is_empty() {
        return Ok(result_col_by_offset);
    }
    for (result_col_idx, &mview_offset) in lookup.map_or(&[][..], |lookup| &lookup.output_mv_offsets).iter().enumerate() {
        if mview_offset >= build.mv_column_count {
            return Err(plain(format!(
                "MViewDeltaMerge full-update output mview offset {mview_offset} at result col {result_col_idx} out of range [0,{})",
                build.mv_column_count
            )));
        }
        let Some(slot) = result_col_by_offset.get_mut(&mview_offset) else {
            continue;
        };
        if slot.is_some() {
            return Err(plain(format!(
                "MViewDeltaMerge full-update output has duplicate min/max mview offset {mview_offset}"
            )));
        }
        *slot = Some(result_col_idx);
    }
    if let Some((&mview_offset, _)) = result_col_by_offset.iter().find(|(_, idx)| idx.is_none()) {
        return Err(plain(format!(
            "MViewDeltaMerge min/max result layout mismatch: missing full-update output for mview offset {mview_offset}"
        )));
    }
    Ok(result_col_by_offset)
}

/// Go `mviewDeltaMergeMinMaxBatchExecBuilder`: the full-update inner
/// reader over the batch's lookup keys.
struct MViewDeltaMergeMinMaxBatchExecBuilder {
    reader: Arc<dyn tidb_executor_join::index_lookup::IndexJoinReaderBuilder>,
    key_cnt: usize,
}

impl MinMaxBatchExecBuilder for MViewDeltaMergeMinMaxBatchExecBuilder {
    fn build(&self, req: &MinMaxBatchBuildRequest) -> Result<Box<dyn tidb_executor_internal_exec::Executor>, ExecError> {
        if req.lookup_keys.is_empty() {
            return Err(ExecError::internal("MViewDeltaMerge min/max batch builder requires non-empty lookup keys"));
        }
        for (i, key) in req.lookup_keys.iter().enumerate() {
            if key.keys.len() != self.key_cnt {
                return Err(ExecError::internal(format!(
                    "MViewDeltaMerge min/max batch builder lookup key {i} count mismatch: got={} expected={}",
                    key.keys.len(),
                    self.key_cnt
                )));
            }
        }
        self.reader.build_executor_for_index_join(tidb_executor_join::index_lookup::IndexJoinProbes {
            keys: req.lookup_keys.iter().map(|key| key.keys.clone()).collect(),
            ..Default::default()
        })
    }
}

/// Go `buildMViewDeltaMergeMinMaxRecompute`: batch recompute through the
/// full-update inner source, its result layout set on the MIN/MAX mappings.
pub(crate) fn build_mview_delta_merge_min_max_recompute(
    build: &BuildResult,
    lookup: Option<&FullUpdateLookup>,
    reader: Option<Arc<dyn tidb_executor_join::index_lookup::IndexJoinReaderBuilder>>,
    agg_mappings: &mut [Mapping],
) -> Result<Option<MinMaxRecomputeExec>, DriverError> {
    if agg_mappings.is_empty() {
        return Ok(None);
    }
    let result_col_by_offset = build_mview_delta_merge_min_max_result_col_by_mview_offset(build, lookup)?;
    if result_col_by_offset.is_empty() {
        return Ok(None);
    }
    let (Some(lookup), Some(reader)) = (lookup, reader) else {
        return Err(plain("MViewDeltaMerge min/max recompute requires full-update inner source"));
    };
    if lookup.inner_column_count() == 0 {
        return Err(plain("MViewDeltaMerge min/max recompute requires positive full-update inner column count"));
    }
    if build.group_key_mv_offsets.is_empty() {
        return Err(plain("MViewDeltaMerge min/max recompute requires non-empty group key offsets"));
    }
    if lookup.key_off2_idx_off().len() != build.group_key_mv_offsets.len() {
        return Err(plain(format!(
            "MViewDeltaMerge min/max recompute key mapping length mismatch: keyOff2IdxOff={} groupKeys={}",
            lookup.key_off2_idx_off().len(),
            build.group_key_mv_offsets.len()
        )));
    }
    if lookup.output_mv_offsets.len() != lookup.inner_column_count() {
        return Err(plain(format!(
            "MViewDeltaMerge min/max recompute output mview-offset mapping length mismatch: got={} expected={}",
            lookup.output_mv_offsets.len(),
            lookup.inner_column_count()
        )));
    }
    let mut key_input_col_ids = Vec::with_capacity(build.group_key_mv_offsets.len());
    for &mview_offset in &build.group_key_mv_offsets {
        if mview_offset >= build.mv_column_count {
            return Err(plain(format!(
                "MViewDeltaMerge group key mview offset {mview_offset} out of range [0,{})",
                build.mv_column_count
            )));
        }
        key_input_col_ids.push((build.delta_column_count + mview_offset) as i64);
    }
    if lookup.key_result_col_idxes.len() != build.group_key_mv_offsets.len() {
        return Err(plain(format!(
            "MViewDeltaMerge min/max recompute key-result mapping length mismatch: keyResult={} groupKeys={}",
            lookup.key_result_col_idxes.len(),
            build.group_key_mv_offsets.len()
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for (i, &key_result_col_idx) in lookup.key_result_col_idxes.iter().enumerate() {
        if key_result_col_idx >= lookup.inner_column_count() {
            return Err(plain(format!(
                "MViewDeltaMerge min/max recompute key-result col idx {key_result_col_idx} at position {i} out of range [0,{})",
                lookup.inner_column_count()
            )));
        }
        if !seen.insert(key_result_col_idx) {
            return Err(plain(format!("MViewDeltaMerge min/max recompute duplicate key-result col idx {key_result_col_idx}")));
        }
    }
    let mut mapped_min_max_cnt = 0;
    for (mapping_idx, mapping) in agg_mappings.iter_mut().enumerate() {
        if mapping.col_id.len() != 1 {
            return Err(plain(format!(
                "MViewDeltaMerge min/max recompute expects one output column for mapping {mapping_idx}, got {}",
                mapping.col_id.len()
            )));
        }
        let mview_offset = mapping.col_id[0] as usize - build.delta_column_count;
        let Some(Some(batch_result_col_idx)) = result_col_by_offset.get(&mview_offset) else {
            continue;
        };
        mapped_min_max_cnt += 1;
        mapping.min_max_recompute = Some(MinMaxRecomputeSpec {
            strategy: MinMaxRecomputeStrategy::Batch,
            single_row: None,
            batch_result_col_idxes: vec![*batch_result_col_idx as i64],
        });
    }
    if mapped_min_max_cnt != result_col_by_offset.len() {
        return Err(plain(format!(
            "MViewDeltaMerge min/max recompute mapping mismatch: mapped={mapped_min_max_cnt} expected={}",
            result_col_by_offset.len()
        )));
    }
    Ok(Some(MinMaxRecomputeExec {
        key_input_col_ids,
        key_result_col_idxes: lookup.key_result_col_idxes.iter().map(|&idx| idx as i64).collect(),
        batch_builder: Some(Box::new(MViewDeltaMergeMinMaxBatchExecBuilder {
            reader,
            key_cnt: build.group_key_mv_offsets.len(),
        })),
    }))
}

/// Go `table.Table` of the view, wrapped with its log (Go
/// `wrapTableWithMLogIfExists(target, tables.MLogSourceRefresh)`), writing in
/// the refresh statement.
pub(crate) struct SessionTargetTable<'a> {
    catalog: &'a mut SessionExtendedInfoSchema,
    database: &'a str,
    name: &'a str,
    ctx: &'a crate::StmtContext,
    mlog: Option<&'a std::cell::RefCell<tidb_table::tables::mview_log::MLogTable>>,
    zone: SessionTimeZone,
}

impl<'a> SessionTargetTable<'a> {
    pub(crate) fn new(
        catalog: &'a mut SessionExtendedInfoSchema,
        database: &'a str,
        name: &'a str,
        ctx: &'a crate::StmtContext,
        mlog: Option<&'a std::cell::RefCell<tidb_table::tables::mview_log::MLogTable>>,
    ) -> Self {
        let zone = tidb_exprctx::Columns::time_zone(ctx);
        Self { catalog, database, name, ctx, mlog, zone }
    }

    fn kv(&mut self) -> Result<&mut tidb_table::tables::TableCommon, ExecError> {
        match self.catalog.get_rows_mut_in(self.database, self.name) {
            Some(TableEntry::Kv(kv)) => Ok(Arc::make_mut(kv)),
            _ => Err(ExecError::internal("table storage changed during FAST refresh")),
        }
    }
}

fn write_error(error: impl std::fmt::Debug) -> ExecError {
    ExecError::internal(format!("{error:?}"))
}

/// The table layer's handle of a `kv.Handle`.
fn table_handle(handle: &tidb_txnkv::Handle) -> Result<tidb_table::tables::TableHandle, ExecError> {
    match handle {
        tidb_txnkv::Handle::Int(_) => Ok(tidb_table::tables::TableHandle::Int(handle.int_value().expect("an int handle"))),
        tidb_txnkv::Handle::Common(common) => Ok(tidb_table::tables::TableHandle::Common(common.encoded().to_vec())),
        tidb_txnkv::Handle::Partition(_) => Err(ExecError::internal("MViewDeltaMerge does not write partition handles")),
    }
}

impl TargetTable for SessionTargetTable<'_> {
    fn writable_cols(&self) -> Vec<TargetColumn> {
        match self.catalog.get_in(self.database, self.name) {
            Some(TableEntry::Kv(kv)) => kv
                .writable_columns()
                .map(|(_, column)| {
                    let info = column.info.read();
                    TargetColumn {
                        name: info.name.original().to_owned(),
                        offset: info.offset as usize,
                        field_type: info.field_type.clone(),
                    }
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn public_col_count(&self) -> usize {
        match self.catalog.get_in(self.database, self.name) {
            Some(TableEntry::Kv(kv)) => kv.meta().read().columns.len(),
            _ => 0,
        }
    }

    fn add_record(&mut self, row: &[Datum], reserve_auto_id_hint: usize) -> Result<(), ExecError> {
        let ctx = self.ctx;
        self.kv()?
            .with_statement_store(ctx, |table, store| {
                table.insert_row_with_row_id_checked(store, row, None, reserve_auto_id_hint as u64, ctx, true)
            })
            .map_err(write_error)?;
        if let Some(mlog) = self.mlog {
            let mut store = crate::statement_store(ctx);
            let mut mlog = mlog.borrow_mut();
            let had_removed_conflict = mlog.take_removed_conflict();
            mlog.add_record(&mut *store, row, false, had_removed_conflict, true, ctx).map_err(write_error)?;
        }
        Ok(())
    }

    fn update_record(
        &mut self,
        handle: &tidb_txnkv::Handle,
        old_row: &[Datum],
        new_row: &[Datum],
        touched: &[bool],
    ) -> Result<(), ExecError> {
        let ctx = self.ctx;
        let handle = table_handle(handle)?;
        self.kv()?
            .with_statement_store(ctx, |table, store| table.update_row_with_old_context(store, &handle, Some(old_row), new_row, ctx))
            .map_err(write_error)?;
        if let Some(mlog) = self.mlog {
            let mut store = crate::statement_store(ctx);
            mlog.borrow_mut()
                .update_record(&mut *store, old_row, new_row, &|offset| touched.get(offset).copied().unwrap_or(false), true, ctx)
                .map_err(write_error)?;
        }
        Ok(())
    }

    fn remove_record(&mut self, handle: &tidb_txnkv::Handle, old_row: &[Datum]) -> Result<(), ExecError> {
        let ctx = self.ctx;
        let handle = table_handle(handle)?;
        self.kv()?
            .with_statement_store(ctx, |table, store| table.delete_row_with_old_context(store, &handle, old_row, ctx))
            .map_err(write_error)?;
        if let Some(mlog) = self.mlog {
            let mut store = crate::statement_store(ctx);
            mlog.borrow_mut().remove_record(&mut *store, old_row, ctx).map_err(write_error)?;
        }
        Ok(())
    }

    fn may_flush(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn shard_allocate_step(&self) -> i64 {
        self.ctx.shard_allocate_step() as i64
    }

    fn time_zone(&self) -> SessionTimeZone {
        self.zone.clone()
    }
}
