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


//! Go `pkg/executor/aggregate/agg_stream_executor.go`: `StreamAggExec`,
//! which aggregates input already sorted by its group-by key, one group at
//! a time, over Go's `aggfuncs` partial results.
//!
//! Go keeps the group's `chunk.Row`s; every one is consumed before the
//! child result is refetched, so this executor keeps their row indices into
//! that one child chunk.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_driver_error::ExecError;
use tidb_executor_aggfuncs::agg_func::{AggFunc, PartialResult};
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_executor_internal_vecgroupchecker::VecGroupChecker;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;
use tidb_datatype::FieldType;
use tidb_memory::Tracker;

/// Go `streamAggMemDeltaFlushThreshold` (1KB).
const STREAM_AGG_MEM_DELTA_FLUSH_THRESHOLD: i64 = 1 << 10;

/// Go `StreamAggExec`.
pub struct StreamAggExec<C: Columns + 'static> {
    meta: ExecutorMeta,
    child: Box<dyn Executor>,
    ctx: C,

    executed: bool,
    /// Go `IsChildReturnEmpty`: whether the child returned no input.
    is_child_return_empty: bool,
    /// Go `DefaultVal`: the row a final global aggregate over no input
    /// answers.
    default_val: Option<Chunk>,
    /// Go `GroupChecker`.
    group_checker: VecGroupChecker,
    agg_funcs: Vec<Box<dyn AggFunc>>,
    partial_results: Vec<PartialResult>,
    /// Go `groupRows`, as indices into `child_result`.
    group_rows: Vec<usize>,
    child_result: Chunk,

    /// Go `memTracker`.
    mem_tracker: Arc<Tracker>,
    /// Go `StmtCtx.MemTracker`, which `OpenSelf` attaches `mem_tracker` to
    /// when `tidb_track_aggregate_memory_usage` is on.
    stmt_mem_tracker: Option<Arc<Tracker>>,
    /// Go `memUsageOfInitialPartialResult`: the partial results' size after
    /// initialization, which each group's reset returns to.
    mem_usage_of_initial_partial_result: i64,
    /// Go `pendingMemDelta`: deltas buffered across groups, flushed past
    /// `streamAggMemDeltaFlushThreshold`.
    pending_mem_delta: i64,
}

impl<C: Columns + 'static> StreamAggExec<C> {
    /// Go `buildStreamAggFromChildExec`'s executor: `default_val` is the
    /// row of each function's `GetDefaultValue` for a final global
    /// aggregate that is not all `FIRSTROW`, `mem_tracker` is the
    /// executor's tracker, and `stmt_mem_tracker` the statement's it attaches
    /// to (`None` where `tidb_track_aggregate_memory_usage` is off).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(meta: ExecutorMeta, child: Box<dyn Executor>, ctx: C, group_checker: VecGroupChecker, agg_funcs: Vec<Box<dyn AggFunc>>, default_val: Option<Chunk>, mem_tracker: Arc<Tracker>, stmt_mem_tracker: Option<Arc<Tracker>>) -> Self {
        // Go's `childResult` is nil until `OpenSelf` takes it from the pool.
        let child_result = Chunk::default();
        Self {
            meta,
            child,
            ctx,
            executed: false,
            is_child_return_empty: true,
            default_val,
            group_checker,
            agg_funcs,
            partial_results: Vec::new(),
            group_rows: Vec::new(),
            child_result,
            mem_tracker,
            stmt_mem_tracker,
            mem_usage_of_initial_partial_result: 0,
            pending_mem_delta: 0,
        }
    }

    /// Go `OpenSelf`.
    fn open_self(&mut self) {
        self.child_result = self.child.new_chunk();
        self.executed = false;
        self.is_child_return_empty = true;
        self.partial_results = Vec::with_capacity(self.agg_funcs.len());
        for agg_func in &self.agg_funcs {
            let (partial_result, mem_delta) = agg_func.alloc_partial_result();
            self.partial_results.push(partial_result);
            self.mem_usage_of_initial_partial_result += mem_delta;
        }
        self.mem_tracker.reset();
        if let Some(stmt_mem_tracker) = &self.stmt_mem_tracker {
            self.mem_tracker.attach_to(stmt_mem_tracker);
        }
        self.mem_tracker.consume(self.child_result.memory_usage() + self.mem_usage_of_initial_partial_result);
    }

    fn consume_one_group(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        if self.group_checker.is_exhausted() {
            self.consume_cur_group_rows_and_fetch_child(chk)?;
            if self.executed {
                return Ok(());
            }
            self.group_checker.split_into_groups(&self.ctx, &self.child_result).map_err(ExecError::Eval)?;
        }
        let (begin, end) = self.group_checker.get_next_group();
        self.group_rows.extend(begin..end);

        let mut meet_last_group = end == self.child_result.num_rows();
        while meet_last_group {
            meet_last_group = false;
            self.consume_cur_group_rows_and_fetch_child(chk)?;
            if self.executed {
                return Ok(());
            }
            let is_first_group_same_as_prev = self.group_checker.split_into_groups(&self.ctx, &self.child_result).map_err(ExecError::Eval)?;
            if is_first_group_same_as_prev {
                let (begin, end) = self.group_checker.get_next_group();
                self.group_rows.extend(begin..end);
                meet_last_group = end == self.child_result.num_rows();
            }
        }

        self.consume_group_rows()?;
        self.append_result_2_chunk(chk)
    }

    /// Go `consumeGroupRows`.
    fn consume_group_rows(&mut self) -> Result<(), ExecError> {
        if self.group_rows.is_empty() {
            return Ok(());
        }
        let rows: Vec<_> = self.group_rows.iter().map(|&i| self.child_result.get_row(i)).collect();
        let mut all_mem_delta = 0;
        for (agg_func, partial_result) in self.agg_funcs.iter().zip(self.partial_results.iter_mut()) {
            all_mem_delta += agg_func.update_partial_result(&self.ctx, &rows, partial_result)?;
        }
        if all_mem_delta != 0 {
            self.pending_mem_delta += all_mem_delta;
            if self.pending_mem_delta >= STREAM_AGG_MEM_DELTA_FLUSH_THRESHOLD {
                self.mem_tracker.consume(self.pending_mem_delta);
                self.pending_mem_delta = 0;
            }
        }
        self.group_rows.clear();
        Ok(())
    }

    /// Go `consumeCurGroupRowsAndFetchChild`.
    fn consume_cur_group_rows_and_fetch_child(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        // Before fetching a new batch of input, consume the last group.
        self.consume_group_rows()?;

        let m_size = self.child_result.memory_usage();
        let result = self.child.next(&mut self.child_result);
        self.mem_tracker.consume(self.child_result.memory_usage() - m_size);
        result?;

        // No more data.
        if self.child_result.num_rows() == 0 {
            let mut result = Ok(());
            if !self.is_child_return_empty {
                result = self.append_result_2_chunk(chk);
            } else if let Some(default_val) = &self.default_val {
                chk.append_row(default_val.get_row(0));
            }
            self.executed = true;
            return result;
        }
        self.is_child_return_empty = false;
        Ok(())
    }

    /// Go `appendResult2Chunk`: every function's result, then every partial
    /// result reset and the tracker back at the initial baseline.
    fn append_result_2_chunk(&mut self, chk: &mut Chunk) -> Result<(), ExecError> {
        for (agg_func, partial_result) in self.agg_funcs.iter().zip(self.partial_results.iter_mut()) {
            agg_func.append_final_result_2_chunk(&self.ctx, partial_result, chk)?;
            agg_func.reset_partial_result(partial_result);
        }
        self.pending_mem_delta = 0;
        self.mem_tracker.replace_bytes_used(self.child_result.memory_usage() + self.mem_usage_of_initial_partial_result);
        if self.agg_funcs.is_empty() {
            chk.set_num_virtual_rows(chk.num_rows() + 1);
        }
        Ok(())
    }
}

impl<C: Columns + Send + 'static> Executor for StreamAggExec<C> {
    fn agg_tree_input_empty(&self) -> bool {
        self.is_child_return_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.open_self();
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while !self.executed && !req.is_full() {
            if let Err(error) = self.consume_one_group(req) {
                self.executed = true;
                return Err(error);
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.mem_tracker.consume(-self.child_result.memory_usage() - self.mem_usage_of_initial_partial_result);
        self.mem_usage_of_initial_partial_result = 0;
        self.partial_results.clear();
        self.group_rows.clear();
        self.group_checker.reset();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
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
