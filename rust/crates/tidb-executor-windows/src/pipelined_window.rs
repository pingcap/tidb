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

//! Go `pkg/executor/windows/pipelined_window.go`: `PipelinedWindowExec` and
//! `OrderedWindowExec`.

use std::collections::VecDeque;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_driver_error::ExecError;
use tidb_executor_aggfuncs::agg_func::{AggFunc, PartialResult};
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_executor_internal_vecgroupchecker::VecGroupChecker;
use tidb_expr::column::Column;
use tidb_exprctx::Columns;
use tidb_planner_core_operator_logicalop::window::{BoundType, FrameBound};

use crate::window::{cmp_func, copy_chk, go_runtime_error, row, rows_of, RowRef};

/// Go `dataInfo`.
pub(crate) struct DataInfo {
    chk: Chunk,
    remaining: u64,
    accumulated: u64,
}

/// Go `PipelinedWindowExec`: the executor for window functions.
pub struct PipelinedWindowExec<C: Columns> {
    pub(crate) meta: ExecutorMeta,
    pub(crate) ctx: C,
    pub(crate) child: Box<dyn Executor>,
    pub(crate) num_window_funcs: usize,
    pub(crate) window_funcs: Vec<Box<dyn AggFunc>>,
    pub(crate) partial_results: Vec<PartialResult>,
    pub(crate) start: FrameBound,
    pub(crate) end: FrameBound,
    pub(crate) group_checker: VecGroupChecker,

    /// Go `childResult`. Even if `remaining` is 0, `rows` might still
    /// reference rows in `data[0].chk` after it was returned to the upper
    /// executor, so `data_idx`, `accumulated` and `dropped` ensure a chunk is
    /// returned only once no row references it.
    pub(crate) child_result: Arc<Chunk>,
    pub(crate) data: VecDeque<DataInfo>,
    pub(crate) data_idx: usize,

    /// Go `done`: the child executor is drained or something unexpected
    /// happened.
    pub(crate) done: bool,
    pub(crate) accumulated: u64,
    pub(crate) dropped: u64,
    pub(crate) row_to_consume: u64,
    pub(crate) new_partition: bool,

    pub(crate) cur_row_idx: u64,
    /// Go `lastStartRow` / `lastEndRow`: the current frame range.
    pub(crate) last_start_row: u64,
    pub(crate) last_end_row: u64,
    pub(crate) staged_start_row: u64,
    pub(crate) staged_end_row: u64,
    pub(crate) row_start: u64,
    pub(crate) order_by_cols: Vec<Column>,
    /// Go `expectedCmpResult`: decides if one value is included in the
    /// frame.
    pub(crate) expected_cmp_result: i64,

    /// Go `rows`: the rows starting from `row_start`.
    pub(crate) rows: VecDeque<RowRef>,
    pub(crate) row_cnt: u64,
    pub(crate) whole: bool,
    pub(crate) is_range_frame: bool,
    pub(crate) empty_frame: bool,
    pub(crate) initialized_sliding_window: bool,
}

impl<C: Columns + 'static> PipelinedWindowExec<C> {
    /// Go `OpenSelf`: initializes the executor state without opening
    /// children.
    pub fn open_self(&mut self) {
        (self.done, self.new_partition, self.whole, self.initialized_sliding_window) = (false, false, false, false);
        (self.data_idx, self.cur_row_idx, self.dropped, self.row_to_consume, self.accumulated) = (0, 0, 0, 0, 0);
        (self.last_start_row, self.last_end_row, self.staged_start_row, self.staged_end_row) = (0, 0, 0, 0);
        (self.row_start, self.row_cnt) = (0, 0);
        (self.rows, self.data) = (VecDeque::new(), VecDeque::new());
        // A reopened executor starts from a fresh partition.
        self.empty_frame = false;
        self.group_checker.reset();
        for (i, window_func) in self.window_funcs.iter().enumerate() {
            window_func.reset_partial_result(&mut self.partial_results[i]);
        }
    }

    fn first_result_chunk_not_ready(&self) -> bool {
        if !self.done && self.data.is_empty() {
            return true;
        }
        // A chunk can't be ready unless 1. all of the rows in the chunk are
        // filled, 2. `rows` doesn't contain rows in the chunk.
        self.data
            .front()
            .is_some_and(|data| data.remaining != 0 || data.accumulated > self.dropped)
    }

    fn get_rows_in_partition(&mut self) -> Result<(), ExecError> {
        self.new_partition = true;
        if self.rows.is_empty() {
            // The first call is not a new partition.
            self.new_partition = false;
        }

        if self.group_checker.is_exhausted() {
            let drained = self.fetch_child()?;
            // A true new_partition with 0 row_to_consume says the data source
            // is drained.
            if drained {
                self.done = true;
                return Ok(());
            }
            let same_partition = self.group_checker.split_into_groups(&self.ctx, &self.child_result)?;
            if same_partition {
                // The only case where this is not a new partition.
                self.new_partition = false;
            }
        }
        let (begin, end) = self.group_checker.get_next_group();
        self.row_to_consume += (end - begin) as u64;
        for i in begin..end {
            self.rows.push_back((Arc::clone(&self.child_result), i));
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
        self.accumulated += num_rows as u64;
        self.data.push_back(DataInfo { chk: result_chk, remaining: num_rows as u64, accumulated: self.accumulated });

        self.child_result = Arc::new(child_result);
        Ok(false)
    }

    /// Go `finish`: a whole partition is consumed.
    fn finish(&mut self) {
        self.whole = true;
    }

    fn get_start(&mut self) -> Result<u64, ExecError> {
        if self.start.unbounded {
            return Ok(0);
        }
        if self.is_range_frame {
            let mut start = self.last_start_row.max(self.staged_start_row);
            while start < self.row_cnt {
                let mut res = 0;
                for i in 0..self.order_by_cols.len() {
                    res = cmp_func(
                        &self.ctx,
                        &self.start.compare_cols[i],
                        &self.start.calc_funcs[i],
                        self.get_row(start)?,
                        self.get_row(self.cur_row_idx)?,
                    )?;
                    if res != 0 {
                        break;
                    }
                }
                // For asc, break when the calculated result is greater than
                // the current value; for desc, when it is less.
                if res != self.expected_cmp_result {
                    break;
                }
                start += 1;
            }
            self.staged_start_row = start;
            return Ok(start);
        }
        Ok(match self.start.bound_type {
            BoundType::Preceding => {
                if self.cur_row_idx > self.start.num {
                    self.cur_row_idx - self.start.num
                } else {
                    0
                }
            }
            BoundType::Following => self.cur_row_idx.wrapping_add(self.start.num),
            BoundType::CurrentRow => self.cur_row_idx,
        })
    }

    fn get_end(&mut self) -> Result<u64, ExecError> {
        if self.end.unbounded {
            return Ok(self.row_cnt);
        }
        if self.is_range_frame {
            let mut end = self.last_end_row.max(self.staged_end_row);
            while end < self.row_cnt {
                let mut res = 0;
                for i in 0..self.order_by_cols.len() {
                    res = cmp_func(
                        &self.ctx,
                        &self.end.calc_funcs[i],
                        &self.end.compare_cols[i],
                        self.get_row(self.cur_row_idx)?,
                        self.get_row(end)?,
                    )?;
                    if res != 0 {
                        break;
                    }
                }
                // For asc, break when the calculated result is greater than
                // the current value; for desc, when it is less.
                if res == self.expected_cmp_result {
                    break;
                }
                end += 1;
            }
            self.staged_end_row = end;
            return Ok(end);
        }
        Ok(match self.end.bound_type {
            BoundType::Preceding => {
                if self.cur_row_idx >= self.end.num {
                    self.cur_row_idx - self.end.num + 1
                } else {
                    0
                }
            }
            BoundType::Following => self.cur_row_idx.wrapping_add(self.end.num).wrapping_add(1),
            BoundType::CurrentRow => self.cur_row_idx + 1,
        })
    }

    fn get_row(&self, i: u64) -> Result<tidb_chunk::row::Row<'_>, ExecError> {
        get_row(&self.rows, self.row_start, i)
    }

    /// Go `produce`: appends rows to `chk`, answering how many it appended.
    fn produce(&mut self, chk: &mut Chunk, mut remained: u64) -> Result<u64, ExecError> {
        let mut produced = 0;
        while remained > 0 {
            if !self.enough_to_produce()? {
                break;
            }
            let mut start = self.get_start()?;
            let mut end = self.get_end()?;
            if end > self.row_cnt {
                end = self.row_cnt;
            }
            if start >= self.row_cnt {
                start = self.row_cnt;
            }
            // A start at or past the end answers a default value, and resets
            // the frame to empty.
            if start >= end {
                for (i, wf) in self.window_funcs.iter().enumerate() {
                    if !self.empty_frame {
                        wf.reset_partial_result(&mut self.partial_results[i]);
                    }
                    wf.append_final_result_2_chunk(&self.ctx, &mut self.partial_results[i], chk)?;
                }
                if !self.empty_frame {
                    self.empty_frame = true;
                    self.initialized_sliding_window = false;
                }
            } else {
                self.empty_frame = false;
                let (rows, row_start) = (&self.rows, self.row_start);
                let get_row = |u: u64| get_row(rows, row_start, u);
                for (i, wf) in self.window_funcs.iter().enumerate() {
                    if self.last_start_row != start || self.last_end_row != end {
                        match wf.as_sliding_window().filter(|_| self.initialized_sliding_window) {
                            Some(sliding) => sliding.slide(
                                &self.ctx,
                                &get_row,
                                self.last_start_row,
                                self.last_end_row,
                                start.wrapping_sub(self.last_start_row),
                                end.wrapping_sub(self.last_end_row),
                                &mut self.partial_results[i],
                            )?,
                            None => {
                                // For MinMaxSlidingWindowAggFuncs, it needs the
                                // absolute value of each start of window, to
                                // compare whether elements inside deque are out
                                // of current window.
                                if let Some(min_max) = wf.as_max_min_sliding_window() {
                                    // Store start inside MaxMinSlidingWindowAggFunc.windowInfo.
                                    min_max.set_window_start(start);
                                }
                                wf.reset_partial_result(&mut self.partial_results[i]);
                                let frame = get_rows(rows, row_start, start, end)?;
                                wf.update_partial_result(&self.ctx, &frame, &mut self.partial_results[i])?;
                            }
                        }
                    }
                    wf.append_final_result_2_chunk(&self.ctx, &mut self.partial_results[i], chk)?;
                }
                self.initialized_sliding_window = true;
            }
            self.cur_row_idx += 1;
            (self.last_start_row, self.last_end_row) = (start, end);

            produced += 1;
            remained -= 1;
        }
        let extend = self.cur_row_idx.min(self.last_end_row).min(self.last_start_row);
        if extend > self.row_start {
            let num_drop = extend - self.row_start;
            self.dropped += num_drop;
            self.rows.drain(..num_drop as usize);
            self.row_start = extend;
        }
        Ok(produced)
    }

    fn enough_to_produce(&mut self) -> Result<bool, ExecError> {
        if self.cur_row_idx >= self.row_cnt {
            return Ok(false);
        }
        if self.whole {
            return Ok(true);
        }
        let start = self.get_start()?;
        let end = self.get_end()?;
        Ok(end < self.row_cnt && start < self.row_cnt)
    }

    /// Go `reset`: resets the processor.
    fn reset(&mut self) {
        self.last_start_row = 0;
        self.last_end_row = 0;
        self.staged_start_row = 0;
        self.staged_end_row = 0;
        self.empty_frame = false;
        self.cur_row_idx = 0;
        self.whole = false;
        let num_drop = self.row_cnt - self.row_start;
        self.dropped += num_drop;
        self.rows.drain(..num_drop as usize);
        self.row_start = 0;
        self.row_cnt = 0;
        self.initialized_sliding_window = false;
        for (i, window_func) in self.window_funcs.iter().enumerate() {
            window_func.reset_partial_result(&mut self.partial_results[i]);
        }
    }
}

impl<C: Columns + Send + 'static> Executor for PipelinedWindowExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.open_self();
        Ok(())
    }

    fn next(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        chk.reset();

        while self.first_result_chunk_not_ready() {
            // Gather enough rows and consume them until able to produce. An
            // unbounded frame consumes the whole partition first.
            let mut enough = self.enough_to_produce()?;
            if !enough {
                if !self.done && self.row_to_consume == 0 {
                    self.get_rows_in_partition()?;
                }
                if self.done || self.new_partition {
                    self.finish();
                    // A continue leaves the rows unconsumed, so the next
                    // round consumes them instead of fetching more.
                    enough = self.enough_to_produce()?;
                    if enough {
                        continue;
                    }
                    self.new_partition = false;
                    self.reset();
                    if self.row_to_consume == 0 {
                        // No more data.
                        break;
                    }
                }
                self.row_cnt += self.row_to_consume;
                self.row_to_consume = 0;
            }

            // Ready to produce data.
            if self.data.len() > self.data_idx && self.data[self.data_idx].remaining != 0 {
                let remaining = self.data[self.data_idx].remaining;
                let mut out = std::mem::replace(&mut self.data[self.data_idx].chk, Chunk::new(&[], 0, 0));
                let produced = self.produce(&mut out, remaining);
                self.data[self.data_idx].chk = out;
                let produced = produced?;
                self.data[self.data_idx].remaining -= produced;
                if self.data[self.data_idx].remaining == 0 {
                    self.data_idx += 1;
                }
            }
        }
        if let Some(mut data) = self.data.pop_front() {
            chk.swap_columns(&mut data.chk);
            self.data_idx -= 1;
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

/// Go `OrderedWindowExec`: executes windows whose input already satisfies
/// the required partition/order property.
pub struct OrderedWindowExec<C: Columns>(pub PipelinedWindowExec<C>);

impl<C: Columns + Send + 'static> Executor for OrderedWindowExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.0.open()
    }

    fn next(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        self.0.next(chk)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.0.close()
    }

    fn schema(&self) -> &tidb_expr::schema::Schema {
        self.0.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.0.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.0.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.0.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.0.new_chunk()
    }
}

/// Go `getRow`: `e.rows[i-e.rowStart]`, a `uint64` subtraction that wraps,
/// whose out-of-range index is the runtime panic Go recovers.
fn get_row(rows: &VecDeque<RowRef>, row_start: u64, i: u64) -> Result<tidb_chunk::row::Row<'_>, ExecError> {
    let index = i.wrapping_sub(row_start);
    usize::try_from(index)
        .ok()
        .and_then(|index| rows.get(index))
        .map(row)
        .ok_or_else(|| go_runtime_error(format!("index out of range [{index}] with length {}", rows.len())))
}

/// Go `getRows`: `e.rows[start-e.rowStart : end-e.rowStart]`, whose bounds
/// wrap as `uint64` and fail as Go's slice expression does.
fn get_rows(
    rows: &VecDeque<RowRef>,
    row_start: u64,
    start: u64,
    end: u64,
) -> Result<Vec<tidb_chunk::row::Row<'_>>, ExecError> {
    let (low, high) = (start.wrapping_sub(row_start), end.wrapping_sub(row_start));
    let len = rows.len() as u64;
    if high > len {
        return Err(go_runtime_error(format!("slice bounds out of range [:{high}] with capacity {len}")));
    }
    if low > high {
        return Err(go_runtime_error(format!("slice bounds out of range [{low}:{high}]")));
    }
    Ok(rows_of(rows.range(low as usize..high as usize)))
}
