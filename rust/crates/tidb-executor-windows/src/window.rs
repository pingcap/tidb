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

//! Go `pkg/executor/windows/window.go`: `WindowExec` and its processors.

use std::collections::VecDeque;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::FieldType;
use tidb_driver_error::ExecError;
use tidb_executor_aggfuncs::agg_func::{AggFunc, AggFuncUpdateContext, PartialResult};
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_executor_internal_vecgroupchecker::VecGroupChecker;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_exprctx::Columns;
use tidb_planner_core_operator_logicalop::window::{BoundType, FrameBound};

/// Go `chunk.Row`: a row of a retained child chunk.
pub(crate) type RowRef = (Arc<Chunk>, usize);

/// The `chunk.Row` a retained row names.
pub(crate) fn row(row: &RowRef) -> Row<'_> {
    row.0.get_row(row.1)
}

/// Go's `rows[start:end]` handed to `UpdatePartialResult`.
pub(crate) fn rows_of<'a>(rows: impl IntoIterator<Item = &'a RowRef>) -> Vec<Row<'a>> {
    rows.into_iter().map(row).collect()
}

/// The error Go's executor recovers a runtime panic into
/// (`util.GetRecoverError`): `runtime error: <message>`.
pub(crate) fn go_runtime_error(message: String) -> ExecError {
    ExecError::internal(format!("runtime error: {message}"))
}

/// Go `rows[u]` over a partition's `[]chunk.Row`, whose out-of-range index is
/// the runtime panic Go recovers.
pub(crate) fn index_row(rows: &[RowRef], u: u64) -> Result<Row<'_>, ExecError> {
    usize::try_from(u)
        .ok()
        .and_then(|index| rows.get(index))
        .map(row)
        .ok_or_else(|| go_runtime_error(format!("index out of range [{u}] with length {}", rows.len())))
}

/// Go `WindowExec`: the executor for window functions.
pub struct WindowExec<C: Columns> {
    pub(crate) meta: ExecutorMeta,
    pub(crate) ctx: C,
    pub(crate) child: Box<dyn Executor>,

    pub(crate) group_checker: VecGroupChecker,
    /// Go `childResult`: the child chunk.
    pub(crate) child_result: Arc<Chunk>,
    /// Go `executed`: the child executor is drained or something unexpected
    /// happened.
    pub(crate) executed: bool,
    /// Go `resultChunks`: the chunks to return.
    pub(crate) result_chunks: VecDeque<Chunk>,
    /// Go `remainingRowsInChunk`: how many rows of `result_chunks[i]` are
    /// not prepared.
    pub(crate) remaining_rows_in_chunk: VecDeque<usize>,

    pub(crate) num_window_funcs: usize,
    pub(crate) processor: Box<dyn WindowProcessor>,
}

impl<C: Columns + 'static> WindowExec<C> {
    fn prepared_chunk_available(&self) -> bool {
        self.result_chunks.front().is_some() && self.remaining_rows_in_chunk[0] == 0
    }

    fn consume_one_group(&mut self) -> Result<(), ExecError> {
        let mut group_rows: Vec<RowRef> = Vec::new();
        if self.group_checker.is_exhausted() {
            let eof = self.fetch_child()?;
            if eof {
                self.executed = true;
                return self.consume_group_rows(group_rows);
            }
            self.group_checker.split_into_groups(&self.ctx, &self.child_result)?;
        }
        let (begin, mut end) = self.group_checker.get_next_group();
        for i in begin..end {
            group_rows.push((Arc::clone(&self.child_result), i));
        }

        let mut meet_last_group = end == self.child_result.num_rows();
        while meet_last_group {
            meet_last_group = false;
            let eof = self.fetch_child()?;
            if eof {
                self.executed = true;
                return self.consume_group_rows(group_rows);
            }

            let is_first_group_same_as_prev =
                self.group_checker.split_into_groups(&self.ctx, &self.child_result)?;

            if is_first_group_same_as_prev {
                let begin;
                (begin, end) = self.group_checker.get_next_group();
                for i in begin..end {
                    group_rows.push((Arc::clone(&self.child_result), i));
                }
                meet_last_group = end == self.child_result.num_rows();
            }
        }
        self.consume_group_rows(group_rows)
    }

    fn consume_group_rows(&mut self, mut group_rows: Vec<RowRef>) -> Result<(), ExecError> {
        let mut remaining_rows_in_group = group_rows.len();
        if remaining_rows_in_group == 0 {
            return Ok(());
        }
        for i in 0..self.result_chunks.len() {
            let remained = self.remaining_rows_in_chunk[i].min(remaining_rows_in_group);
            self.remaining_rows_in_chunk[i] -= remained;
            remaining_rows_in_group -= remained;

            // Go: "The old implementation needs the processor has these
            // three methods but now it does not have to."
            self.processor.consume_group_rows(&self.ctx, &mut group_rows)?;
            self.processor
                .append_result_2_chunk(&self.ctx, &group_rows, &mut self.result_chunks[i], remained)?;
            if remaining_rows_in_group == 0 {
                self.processor.reset_partial_result();
                break;
            }
        }
        Ok(())
    }

    /// Go `fetchChild`: whether the child is drained.
    fn fetch_child(&mut self) -> Result<bool, ExecError> {
        let mut child_result = self.child.new_chunk();
        self.child.next(&mut child_result)?;
        // No more data.
        let num_rows = child_result.num_rows();
        if num_rows == 0 {
            return Ok(true);
        }

        let mut result_chk = self.meta.new_chunk_with_capacity(self.meta.ret_field_types(), 0, num_rows);
        copy_chk(&self.meta, self.num_window_funcs, &mut child_result, &mut result_chk)?;
        self.result_chunks.push_back(result_chk);
        self.remaining_rows_in_chunk.push_back(num_rows);

        self.child_result = Arc::new(child_result);
        Ok(false)
    }
}

/// Go `copyChk`: the output's passthrough columns reference the child's.
pub(crate) fn copy_chk(
    meta: &ExecutorMeta,
    num_window_funcs: usize,
    src: &mut Chunk,
    dst: &mut Chunk,
) -> Result<(), ExecError> {
    let columns = &meta.schema().columns[..meta.schema().columns.len() - num_window_funcs];
    for (i, col) in columns.iter().enumerate() {
        let index = usize::try_from(col.index)
            .map_err(|_| ExecError::internal("a window output column has no child index"))?;
        dst.make_ref_to(i, src, index).map_err(ExecError::internal)?;
    }
    Ok(())
}

impl<C: Columns + Send + 'static> Executor for WindowExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.executed = false;
        self.group_checker.reset();
        self.result_chunks.clear();
        self.remaining_rows_in_chunk.clear();
        self.processor.reset_partial_result();
        self.child.open()
    }

    fn next(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        chk.reset();
        while !self.executed && !self.prepared_chunk_available() {
            if let Err(error) = self.consume_one_group() {
                self.executed = true;
                return Err(error);
            }
        }
        if let Some(mut result) = self.result_chunks.pop_front() {
            chk.swap_columns(&mut result);
            self.remaining_rows_in_chunk.pop_front();
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.child.close()
    }

    fn schema(&self) -> &tidb_expr::schema::Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Go `windowProcessor`: processing for the different kinds of windows.
pub(crate) trait WindowProcessor: Send {
    /// Go `consumeGroupRows`: updates the result for a window function using
    /// the input rows which belong to the same partition.
    fn consume_group_rows(&mut self, ctx: &AggFuncUpdateContext, rows: &mut Vec<RowRef>) -> Result<(), ExecError>;
    /// Go `appendResult2Chunk`: appends the final results to chunk. It is
    /// called when there are no more rows in current partition.
    fn append_result_2_chunk(
        &mut self,
        ctx: &AggFuncUpdateContext,
        rows: &[RowRef],
        chk: &mut Chunk,
        remained: usize,
    ) -> Result<(), ExecError>;
    /// Go `resetPartialResult`: resets the partial result to the original
    /// state for a specific window function.
    fn reset_partial_result(&mut self);
}

/// Go `aggWindowProcessor`.
pub(crate) struct AggWindowProcessor {
    pub(crate) window_funcs: Vec<Box<dyn AggFunc>>,
    pub(crate) partial_results: Vec<PartialResult>,
}

impl WindowProcessor for AggWindowProcessor {
    fn consume_group_rows(&mut self, ctx: &AggFuncUpdateContext, rows: &mut Vec<RowRef>) -> Result<(), ExecError> {
        let group = rows_of(rows.iter());
        for (i, window_func) in self.window_funcs.iter().enumerate() {
            window_func.update_partial_result(ctx, &group, &mut self.partial_results[i])?;
        }
        drop(group);
        rows.clear();
        Ok(())
    }

    fn append_result_2_chunk(
        &mut self,
        ctx: &AggFuncUpdateContext,
        _rows: &[RowRef],
        chk: &mut Chunk,
        mut remained: usize,
    ) -> Result<(), ExecError> {
        while remained > 0 {
            for (i, window_func) in self.window_funcs.iter().enumerate() {
                window_func.append_final_result_2_chunk(ctx, &mut self.partial_results[i], chk)?;
            }
            remained -= 1;
        }
        Ok(())
    }

    fn reset_partial_result(&mut self) {
        for (i, window_func) in self.window_funcs.iter().enumerate() {
            window_func.reset_partial_result(&mut self.partial_results[i]);
        }
    }
}

/// Go `rowFrameWindowProcessor`.
pub(crate) struct RowFrameWindowProcessor {
    pub(crate) window_funcs: Vec<Box<dyn AggFunc>>,
    pub(crate) partial_results: Vec<PartialResult>,
    pub(crate) start: FrameBound,
    pub(crate) end: FrameBound,
    pub(crate) cur_row_idx: u64,
}

impl RowFrameWindowProcessor {
    fn get_start_offset(&self, num_rows: u64) -> u64 {
        if self.start.unbounded {
            return 0;
        }
        match self.start.bound_type {
            BoundType::Preceding => self.cur_row_idx.checked_sub(self.start.num).unwrap_or(0),
            BoundType::Following => {
                let offset = self.cur_row_idx.wrapping_add(self.start.num);
                if offset >= num_rows {
                    return num_rows;
                }
                offset
            }
            BoundType::CurrentRow => self.cur_row_idx,
        }
    }

    fn get_end_offset(&self, num_rows: u64) -> u64 {
        if self.end.unbounded {
            return num_rows;
        }
        match self.end.bound_type {
            BoundType::Preceding => {
                if self.cur_row_idx >= self.end.num {
                    return self.cur_row_idx - self.end.num + 1;
                }
                0
            }
            BoundType::Following => {
                let offset = self.cur_row_idx.wrapping_add(self.end.num);
                if offset >= num_rows {
                    return num_rows;
                }
                offset + 1
            }
            BoundType::CurrentRow => self.cur_row_idx + 1,
        }
    }
}

impl WindowProcessor for RowFrameWindowProcessor {
    fn consume_group_rows(&mut self, _ctx: &AggFuncUpdateContext, _rows: &mut Vec<RowRef>) -> Result<(), ExecError> {
        Ok(())
    }

    fn append_result_2_chunk(
        &mut self,
        ctx: &AggFuncUpdateContext,
        rows: &[RowRef],
        chk: &mut Chunk,
        remained: usize,
    ) -> Result<(), ExecError> {
        let num_rows = rows.len() as u64;
        let window_funcs = std::mem::take(&mut self.window_funcs);
        let mut partial_results = std::mem::take(&mut self.partial_results);
        let result = append_frame_results(&window_funcs, &mut partial_results, ctx, rows, chk, remained, |_| {
            let start = self.get_start_offset(num_rows);
            let end = self.get_end_offset(num_rows);
            self.cur_row_idx += 1;
            Ok((start, end))
        });
        self.window_funcs = window_funcs;
        self.partial_results = partial_results;
        result
    }

    fn reset_partial_result(&mut self) {
        self.cur_row_idx = 0;
    }
}

/// Go `rangeFrameWindowProcessor`.
pub(crate) struct RangeFrameWindowProcessor {
    pub(crate) window_funcs: Vec<Box<dyn AggFunc>>,
    pub(crate) partial_results: Vec<PartialResult>,
    pub(crate) start: FrameBound,
    pub(crate) end: FrameBound,
    pub(crate) cur_row_idx: u64,
    pub(crate) last_start_offset: u64,
    pub(crate) last_end_offset: u64,
    pub(crate) order_by_cols: Vec<Column>,
    /// Go `expectedCmpResult`: decides if one value is included in the
    /// frame.
    pub(crate) expected_cmp_result: i64,
}

impl RangeFrameWindowProcessor {
    fn get_start_offset(&mut self, ctx: &AggFuncUpdateContext, rows: &[RowRef]) -> Result<u64, ExecError> {
        if self.start.unbounded {
            return Ok(0);
        }
        let num_rows = rows.len() as u64;
        while self.last_start_offset < num_rows {
            let mut res = 0;
            for i in 0..self.order_by_cols.len() {
                res = cmp_func(
                    ctx,
                    &self.start.compare_cols[i],
                    &self.start.calc_funcs[i],
                    index_row(rows, self.last_start_offset)?,
                    index_row(rows, self.cur_row_idx)?,
                )?;
                if res != 0 {
                    break;
                }
            }
            // For asc, break when the current value is greater or equal to the
            // calculated result; for desc, break when the current value is
            // less or equal to the calculated result.
            if res != self.expected_cmp_result {
                break;
            }
            self.last_start_offset += 1;
        }
        Ok(self.last_start_offset)
    }

    fn get_end_offset(&mut self, ctx: &AggFuncUpdateContext, rows: &[RowRef]) -> Result<u64, ExecError> {
        let num_rows = rows.len() as u64;
        if self.end.unbounded {
            return Ok(num_rows);
        }
        while self.last_end_offset < num_rows {
            let mut res = 0;
            for i in 0..self.order_by_cols.len() {
                res = cmp_func(
                    ctx,
                    &self.end.calc_funcs[i],
                    &self.end.compare_cols[i],
                    index_row(rows, self.cur_row_idx)?,
                    index_row(rows, self.last_end_offset)?,
                )?;
                if res != 0 {
                    break;
                }
            }
            // For asc, break when the calculated result is greater than the
            // current value; for desc, break when the calculated result is
            // less than the current value.
            if res == self.expected_cmp_result {
                break;
            }
            self.last_end_offset += 1;
        }
        Ok(self.last_end_offset)
    }
}

impl WindowProcessor for RangeFrameWindowProcessor {
    fn consume_group_rows(&mut self, _ctx: &AggFuncUpdateContext, _rows: &mut Vec<RowRef>) -> Result<(), ExecError> {
        Ok(())
    }

    fn append_result_2_chunk(
        &mut self,
        ctx: &AggFuncUpdateContext,
        rows: &[RowRef],
        chk: &mut Chunk,
        remained: usize,
    ) -> Result<(), ExecError> {
        let window_funcs = std::mem::take(&mut self.window_funcs);
        let mut partial_results = std::mem::take(&mut self.partial_results);
        let result = append_frame_results(&window_funcs, &mut partial_results, ctx, rows, chk, remained, |_| {
            let start = self.get_start_offset(ctx, rows)?;
            let end = self.get_end_offset(ctx, rows)?;
            self.cur_row_idx += 1;
            Ok((start, end))
        });
        self.window_funcs = window_funcs;
        self.partial_results = partial_results;
        result
    }

    fn reset_partial_result(&mut self) {
        self.cur_row_idx = 0;
        self.last_start_offset = 0;
        self.last_end_offset = 0;
    }
}

/// The shared body of Go's `rowFrameWindowProcessor.appendResult2Chunk` and
/// `rangeFrameWindowProcessor.appendResult2Chunk`; `next_frame` is each
/// one's `getStartOffset` / `getEndOffset` followed by `p.curRowIdx++`.
fn append_frame_results(
    window_funcs: &[Box<dyn AggFunc>],
    partial_results: &mut [PartialResult],
    ctx: &AggFuncUpdateContext,
    rows: &[RowRef],
    chk: &mut Chunk,
    mut remained: usize,
    mut next_frame: impl FnMut(&[RowRef]) -> Result<(u64, u64), ExecError>,
) -> Result<(), ExecError> {
    let mut initialized_sliding_window = false;
    let (mut last_start, mut last_end) = (0_u64, 0_u64);
    let get_row = |u: u64| index_row(rows, u);
    while remained > 0 {
        let (start, end) = next_frame(rows)?;
        remained -= 1;
        let shift_start = start.wrapping_sub(last_start);
        let shift_end = end.wrapping_sub(last_end);
        if start >= end {
            for (i, window_func) in window_funcs.iter().enumerate() {
                if let Some(sliding) = window_func.as_sliding_window().filter(|_| initialized_sliding_window) {
                    sliding.slide(ctx, &get_row, last_start, last_end, shift_start, shift_end, &mut partial_results[i])?;
                }
                window_func.append_final_result_2_chunk(ctx, &mut partial_results[i], chk)?;
            }
            (last_start, last_end) = (start, end);
            continue;
        }

        for (i, window_func) in window_funcs.iter().enumerate() {
            let sliding = window_func.as_sliding_window();
            match sliding.filter(|_| initialized_sliding_window) {
                Some(sliding) => {
                    sliding.slide(ctx, &get_row, last_start, last_end, shift_start, shift_end, &mut partial_results[i])?;
                }
                None => {
                    // For MinMaxSlidingWindowAggFuncs, it needs the absolute
                    // value of each start of window, to compare whether
                    // elements inside deque are out of current window.
                    if let Some(min_max) = window_func.as_max_min_sliding_window() {
                        // Store start inside MaxMinSlidingWindowAggFunc.windowInfo.
                        min_max.set_window_start(start);
                    }
                    let frame = rows_of(&rows[start as usize..end as usize]);
                    window_func.update_partial_result(ctx, &frame, &mut partial_results[i])?;
                }
            }
            window_func.append_final_result_2_chunk(ctx, &mut partial_results[i], chk)?;
            if sliding.is_none() {
                window_func.reset_partial_result(&mut partial_results[i]);
            }
        }
        initialized_sliding_window = true;
        (last_start, last_end) = (start, end);
    }
    for (i, window_func) in window_funcs.iter().enumerate() {
        window_func.reset_partial_result(&mut partial_results[i]);
    }
    Ok(())
}

/// Go `expression.CompareFunc` of a RANGE frame bound (`CmpFuncs[i]`):
/// evaluates `lhs` over `lhs_row` and `rhs` over `rhs_row` and compares them
/// in the bound's comparison type, NULL first (`compareNull`).
pub(crate) fn cmp_func(
    ctx: &AggFuncUpdateContext,
    lhs: &Expression,
    rhs: &Expression,
    lhs_row: Row<'_>,
    rhs_row: Row<'_>,
) -> Result<i64, ExecError> {
    let lhs_value = lhs.eval(ctx, lhs_row)?;
    let rhs_value = rhs.eval(ctx, rhs_row)?;
    let order = tidb_expr::compare_datums_with_collation(
        &lhs_value,
        &rhs_value,
        tidb_expr::collation_derive::collation_of_node(lhs),
    )?;
    Ok(order as i64)
}
