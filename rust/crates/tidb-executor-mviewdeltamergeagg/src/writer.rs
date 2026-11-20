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

//! Go `writer.go`: the result writer over the MView table.

use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_driver_error::ExecError;
use tidb_planner_util::handle_cols::HandleCols;
use tidb_txnkv::Handle;

use crate::exec::{ChunkResult, MergeWriterStats, ResultWriter, RowOpType};
use crate::merge::err;

/// Go `table.Column` as the writer reads it.
#[derive(Clone, Debug)]
pub struct TargetColumn {
    /// Go `Name.O`.
    pub name: String,
    /// Go `Offset`.
    pub offset: usize,
    /// Go `FieldType`.
    pub field_type: FieldType,
}

/// Go `table.Table` with the session's transaction and table context, as
/// `tableResultWriter` writes through them.
pub trait TargetTable {
    /// Go `WritableCols()`.
    fn writable_cols(&self) -> Vec<TargetColumn>;
    /// Go `len(Cols())`: the public columns.
    fn public_col_count(&self) -> usize;
    /// Go `AddRecord(tableCtx, txn, row, [table.WithReserveAutoIDHint(hint)],
    /// table.DupKeyCheckLazy)`; `reserve_auto_id_hint == 0` passes no hint.
    ///
    /// # Errors
    /// The write failed.
    fn add_record(&mut self, row: &[Datum], reserve_auto_id_hint: usize) -> Result<(), ExecError>;
    /// Go `UpdateRecord(tableCtx, txn, handle, oldRow, newRow, touched)`.
    ///
    /// # Errors
    /// The write failed.
    fn update_record(&mut self, handle: &Handle, old_row: &[Datum], new_row: &[Datum], touched: &[bool]) -> Result<(), ExecError>;
    /// Go `RemoveRecord(tableCtx, txn, handle, oldRow)`.
    ///
    /// # Errors
    /// The write failed.
    fn remove_record(&mut self, handle: &Handle, old_row: &[Datum]) -> Result<(), ExecError>;
    /// Go `txn.MayFlush()`.
    ///
    /// # Errors
    /// The flush failed.
    fn may_flush(&mut self) -> Result<(), ExecError>;
    /// Go `sessVars.ShardAllocateStep`.
    fn shard_allocate_step(&self) -> i64;
    /// Go `stmtCtx` as `TargetHandleCols.BuildHandle` reads it: its zone.
    fn time_zone(&self) -> SessionTimeZone;
}

/// Go `tableResultWriter`.
pub(crate) struct TableResultWriter<'a> {
    table: Box<dyn TargetTable + 'a>,
    handle_cols: HandleCols,
    writable_col_ids: Vec<usize>,
    writable_field_types: Vec<FieldType>,
    non_agg_writable_ids: Vec<usize>,
    pub(crate) agg_writable_ids: Vec<usize>,
    old_row: Vec<Datum>,
    new_row: Vec<Datum>,
    pub(crate) touched: Vec<bool>,
}

/// Go `buildTableResultWriter`.
pub(crate) fn build_table_result_writer<'a>(
    table: Box<dyn TargetTable + 'a>,
    handle_cols: HandleCols,
    child_types: &[FieldType],
    delta_agg_col_count: i64,
    agg_output_col_ids: &[i64],
) -> Result<TableResultWriter<'a>, ExecError> {
    let writable_cols = table.writable_cols();
    if writable_cols.is_empty() {
        return Err(err("target table has no writable columns"));
    }
    if table.public_col_count() != writable_cols.len() {
        return Err(err("Exec does not support target table with non-public writable columns"));
    }
    let mut col_ids = Vec::with_capacity(writable_cols.len());
    // Row buffers and touched flags use writable ordinals as physical column offsets.
    for (i, col) in writable_cols.iter().enumerate() {
        if col.offset != i {
            return Err(err(format!(
                "target writable column `{}` offset {} != writable ordinal {i}",
                col.name, col.offset
            )));
        }
        col_ids.push(delta_agg_col_count + col.offset as i64);
    }
    for i in 0..handle_cols.num_cols() {
        let handle_input_idx = handle_cols.get_col(i).index;
        if handle_input_idx < 0 || handle_input_idx >= child_types.len() as i64 {
            return Err(err(format!(
                "TargetHandleCols col index {handle_input_idx} out of input range [0,{})",
                child_types.len()
            )));
        }
    }

    let mut output_to_writable = vec![-1_i64; child_types.len()];
    for (writable_idx, &col_id) in col_ids.iter().enumerate() {
        if col_id < 0 || col_id >= child_types.len() as i64 {
            return Err(err(format!(
                "target writable mapping[{writable_idx}]={col_id} out of range [0,{})",
                child_types.len()
            )));
        }
        let input_tp = &child_types[col_id as usize];
        let target_tp = &writable_cols[writable_idx].field_type;
        if !target_tp.equal(input_tp) {
            return Err(err(format!(
                "target writable mapping[{writable_idx}]={col_id} type mismatch, target col `{}` expects {target_tp} but input is {input_tp}",
                writable_cols[writable_idx].name
            )));
        }
        output_to_writable[col_id as usize] = writable_idx as i64;
    }

    let mut agg_writable_ids = Vec::with_capacity(agg_output_col_ids.len());
    let mut seen_writable = vec![false; writable_cols.len()];
    for &output_col_id in agg_output_col_ids {
        if output_col_id < 0 || output_col_id >= output_to_writable.len() as i64 {
            return Err(err(format!(
                "agg output col id {output_col_id} out of range [0,{})",
                output_to_writable.len()
            )));
        }
        let writable_idx = output_to_writable[output_col_id as usize];
        if writable_idx < 0 {
            return Err(err(format!("agg output col id {output_col_id} does not map to target writable columns")));
        }
        if !seen_writable[writable_idx as usize] {
            seen_writable[writable_idx as usize] = true;
            agg_writable_ids.push(writable_idx as usize);
        }
    }
    let writable_field_types: Vec<FieldType> = writable_cols.iter().map(|col| col.field_type.clone()).collect();
    let non_agg_writable_ids = (0..writable_cols.len()).filter(|&idx| !seen_writable[idx]).collect();
    let n = writable_cols.len();
    Ok(TableResultWriter {
        table,
        handle_cols,
        writable_col_ids: col_ids.into_iter().map(|id| id as usize).collect(),
        writable_field_types,
        non_agg_writable_ids,
        agg_writable_ids,
        old_row: vec![Datum::Null; n],
        new_row: vec![Datum::Null; n],
        touched: vec![false; n],
    })
}

impl ResultWriter for TableResultWriter<'_> {
    /// Go `WriteChunk`.
    fn write_chunk(&mut self, result: &ChunkResult, stats: &mut MergeWriterStats) -> Result<(), ExecError> {
        stats.chunks += 1;
        stats.row_ops += result.row_ops.len() as i64;
        if result.row_ops.is_empty() {
            return Ok(());
        }
        self.validate_chunk_result(result)?;

        let insert_size_hint_step = self.table.shard_allocate_step().max(1) as usize;
        let mut insert_remain = result.row_ops.iter().filter(|op| op.tp == RowOpType::Insert).count();
        let mut insert_ordinal = 0;
        let zone = self.table.time_zone();

        for op in &result.row_ops {
            match op.tp {
                RowOpType::Insert => {
                    stats.insert_rows += 1;
                    self.build_insert_row(result, op.row_idx);
                    let size_hint = if insert_ordinal % insert_size_hint_step == 0 {
                        insert_size_hint_step.min(insert_remain)
                    } else {
                        0
                    };
                    insert_ordinal += 1;
                    insert_remain -= 1;
                    self.table.add_record(&self.new_row, size_hint)?;
                }
                RowOpType::Update => {
                    let changed = self.build_touched_from_bitmap(
                        &result.update_touched_bitmap,
                        result.update_touched_stride,
                        op.update_ordinal as usize,
                    );
                    if !changed {
                        stats.noop_rows += 1;
                        continue;
                    }
                    stats.update_rows += 1;
                    self.build_update_rows(result, op.row_idx);
                    let handle = self
                        .handle_cols
                        .build_handle(&zone, result.input.get_row(op.row_idx))
                        .map_err(|e| err(format!("{e:?}")))?;
                    self.table.update_record(&handle, &self.old_row, &self.new_row, &self.touched)?;
                }
                RowOpType::Delete => {
                    stats.delete_rows += 1;
                    self.build_delete_row(result, op.row_idx);
                    let handle = self
                        .handle_cols
                        .build_handle(&zone, result.input.get_row(op.row_idx))
                        .map_err(|e| err(format!("{e:?}")))?;
                    self.table.remove_record(&handle, &self.old_row)?;
                }
            }
        }
        self.table.may_flush()
    }
}

impl TableResultWriter<'_> {
    /// Go's test literal `&tableResultWriter{aggWritableIDs: ids, touched: ...}`.
    #[cfg(test)]
    pub(crate) fn for_touched_test(agg_writable_ids: Vec<usize>) -> Self {
        struct NoTable;
        impl TargetTable for NoTable {
            fn writable_cols(&self) -> Vec<TargetColumn> {
                Vec::new()
            }
            fn public_col_count(&self) -> usize {
                0
            }
            fn add_record(&mut self, _: &[Datum], _: usize) -> Result<(), ExecError> {
                Ok(())
            }
            fn update_record(&mut self, _: &Handle, _: &[Datum], _: &[Datum], _: &[bool]) -> Result<(), ExecError> {
                Ok(())
            }
            fn remove_record(&mut self, _: &Handle, _: &[Datum]) -> Result<(), ExecError> {
                Ok(())
            }
            fn may_flush(&mut self) -> Result<(), ExecError> {
                Ok(())
            }
            fn shard_allocate_step(&self) -> i64 {
                1
            }
            fn time_zone(&self) -> SessionTimeZone {
                SessionTimeZone::utc()
            }
        }
        let n = agg_writable_ids.len();
        Self {
            table: Box::new(NoTable),
            handle_cols: tidb_planner_util::handle_cols::new_int_handle_cols(tidb_expr::column::Column::default()),
            writable_col_ids: Vec::new(),
            writable_field_types: Vec::new(),
            non_agg_writable_ids: Vec::new(),
            agg_writable_ids,
            old_row: Vec::new(),
            new_row: Vec::new(),
            touched: vec![false; n],
        }
    }

    /// Go `validateChunkResult`.
    fn validate_chunk_result(&self, result: &ChunkResult) -> Result<(), ExecError> {
        if result.update_touched_bit_cnt != self.agg_writable_ids.len() {
            return Err(err(format!(
                "update touched bit count mismatch, result={} writer={}",
                result.update_touched_bit_cnt,
                self.agg_writable_ids.len()
            )));
        }
        let expected_stride = (result.update_touched_bit_cnt + 7) >> 3;
        if result.update_touched_stride != expected_stride {
            return Err(err(format!(
                "update touched stride mismatch, result={} expected={expected_stride}",
                result.update_touched_stride
            )));
        }
        let mut update_candidate_cnt = 0;
        for op in &result.row_ops {
            if op.tp == RowOpType::Update {
                if op.update_ordinal as usize != update_candidate_cnt {
                    return Err(err(format!(
                        "update touched ordinal mismatch, expected={update_candidate_cnt} got={}",
                        op.update_ordinal
                    )));
                }
                update_candidate_cnt += 1;
            }
        }
        if result.update_touched_bitmap.len() != update_candidate_cnt * result.update_touched_stride {
            return Err(err(format!(
                "update touched bitmap size mismatch, bitmap_len={} expected={}",
                result.update_touched_bitmap.len(),
                update_candidate_cnt * result.update_touched_stride
            )));
        }
        for &writable_idx in &self.agg_writable_ids {
            let col_id = self.writable_col_ids[writable_idx];
            if col_id >= result.computed_cols.len() {
                return Err(err(format!(
                    "agg computed col id {col_id} out of range [0,{})",
                    result.computed_cols.len()
                )));
            }
            if result.computed_cols[col_id].is_none() {
                return Err(err(format!("agg computed col {col_id} is nil in chunk result")));
            }
        }
        Ok(())
    }

    /// Go `buildDeleteRow`.
    fn build_delete_row(&mut self, result: &ChunkResult, row_idx: usize) {
        let row = result.input.get_row(row_idx);
        for (writable_idx, &col_id) in self.writable_col_ids.iter().enumerate() {
            row.datum_with_buffer(col_id, &self.writable_field_types[writable_idx], &mut self.old_row[writable_idx]);
        }
    }

    /// Go `buildInsertRow`.
    fn build_insert_row(&mut self, result: &ChunkResult, row_idx: usize) {
        let row = result.input.get_row(row_idx);
        for &writable_idx in &self.non_agg_writable_ids {
            let col_id = self.writable_col_ids[writable_idx];
            row.datum_with_buffer(col_id, &self.writable_field_types[writable_idx], &mut self.new_row[writable_idx]);
        }
        for &writable_idx in &self.agg_writable_ids {
            let col_id = self.writable_col_ids[writable_idx];
            let computed = result.computed_cols[col_id].as_ref().expect("validated");
            computed.datum_with_buffer(row_idx, &self.writable_field_types[writable_idx], &mut self.new_row[writable_idx]);
        }
    }

    /// Go `buildUpdateRows`.
    fn build_update_rows(&mut self, result: &ChunkResult, row_idx: usize) {
        let row = result.input.get_row(row_idx);
        for (writable_idx, &col_id) in self.writable_col_ids.iter().enumerate() {
            row.datum_with_buffer(col_id, &self.writable_field_types[writable_idx], &mut self.old_row[writable_idx]);
        }
        for &writable_idx in &self.non_agg_writable_ids {
            self.new_row[writable_idx] = self.old_row[writable_idx].clone();
        }
        for &writable_idx in &self.agg_writable_ids {
            let col_id = self.writable_col_ids[writable_idx];
            let computed = result.computed_cols[col_id].as_ref().expect("validated");
            computed.datum_with_buffer(row_idx, &self.writable_field_types[writable_idx], &mut self.new_row[writable_idx]);
        }
    }

    /// Go `buildTouchedFromBitmap`.
    pub(crate) fn build_touched_from_bitmap(&mut self, bitmap: &[u8], stride: usize, update_ordinal: usize) -> bool {
        self.touched.iter_mut().for_each(|touched| *touched = false);
        if stride == 0 {
            return false;
        }
        let offset = update_ordinal * stride;
        let mut changed = false;
        for (byte_idx, &byte) in bitmap[offset..offset + stride].iter().enumerate() {
            let mut b = byte;
            while b != 0 {
                let bit_pos = (byte_idx << 3) + b.trailing_zeros() as usize;
                self.touched[self.agg_writable_ids[bit_pos]] = true;
                changed = true;
                b &= b - 1;
            }
        }
        changed
    }
}
