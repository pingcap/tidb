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


//! `pkg/executor/aggregate` `HashAggExec`: Go's serial `unparallelExec` path
//! and partial/final-worker `parallelExec` path.
//!
//! Aggregates ported (from `pkg/executor/aggfuncs`): `COUNT` (NULL inputs
//! skipped; `COUNT(*)` counts rows), `SUM` (NULL inputs skipped; an all-NULL /
//! empty group sums to NULL), and `FIRST_ROW` (the planner's carrier for
//! group-by columns in the output). `COUNT(a, b, ...)` (only reachable as
//! `COUNT(DISTINCT a, b, ...)` -- the parser rejects the non-DISTINCT form,
//! `pkg/parser` `expr_func_parser.go`'s `parseAggregateFuncCall`) counts a row
//! only when every argument is non-NULL, and its own DISTINCT dedupes over
//! the whole argument tuple rather than a single column (Go
//! `count4MultiArgs`). Groups emit in first-seen order, matching
//! Go's `groupKeys` insertion order. The no-group-by/no-data case emits ONE
//! empty group (`SELECT COUNT(c) FROM t` on empty `t` is `[0]`), while
//! group-by/no-data emits none -- exactly Go's `unparallelExec` rule.
//!
//! `MIN`/`MAX` compare with the shared datum ordering and skip NULL inputs
//! (Go `maxMin4*.UpdatePartialResult`); `AVG` keeps Go's sum/count pair and
//! divides at finalize, returning DECIMAL for integer/decimal inputs and
//! DOUBLE for real ones (Go `typeInfer4Avg` + `baseAvg*.AppendFinalResult2Chunk`),
//! with the same `div_precision_increment` the `/` operator uses. `DISTINCT`
//! de-duplicates a function's inputs per group on the datum hash key, as Go's
//! `*4Distinct*` variants do with their `valueSet`.
//!
//! `BIT_AND`/`BIT_OR`/`BIT_XOR` fold in the unsigned 64-bit domain and seed
//! with the operator's IDENTITY, so an empty or all-NULL group yields that
//! identity rather than NULL (Go `func_bitfuncs.go`); the variance family
//! (`VAR_POP`/`VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP`) shares Go's one
//! incremental accumulator (`func_varpop.go`'s `calculateIntermediate`),
//! reproduced operation-for-operation so the floating-point result matches.
//!
//! `JSON_ARRAYAGG`/`JSON_OBJECTAGG` build a JSON document per group (NULL
//! inputs kept as JSON `null`, a repeated object key overwritten by the last
//! row), `APPROX_COUNT_DISTINCT` counts distinct encoded argument tuples and
//! `APPROX_PERCENTILE` ranks the group's values -- see each [`AggKind`]
//! variant for the exact Go rule and its captured edges.
//!
//! The Go parallel partial/final worker pipeline is transcreated in
//! [`parallel`] for every aggregate without an aggregate-local `ORDER BY`.
//! DISTINCT uses worker-local sets and final-worker set union, as Go does;
//! parallel spill writes 256 partitions of partial results and restores them
//! through the same merge path. A context that cannot be shared across Rust
//! worker threads remains serial for memory safety.
//!
//! `APPROX_COUNT_DISTINCT` ports Go's `BJKST` sketch
//! (`func_count_distinct.go`'s `partialResult4ApproxCountDistinct`, see
//! [`crate::approx_count_distinct`]) over the FarmHash `Hash64` of each
//! row's encoded argument tuple (`func_count_distinct.go`'s
//! `evalAndEncode`/`appendInt64`/etc, ported in
//! [`crate::farmhash`]), so results match Go's exactly, including above the
//! 65536-distinct-value threshold where the sketch stops being exact and
//! starts extrapolating. The concrete encoders cover Go's INT/REAL/DECIMAL,
//! string/binary, temporal, duration, JSON, and vector datum paths; unknown
//! sentinels are the only values rejected by this layer.


pub mod agg_spill;
use tidb_datatype::CollationCollator;
use crate::agg_spill::{AggSpillDiskAction, ParallelAggSpillDiskAction};

mod group_key;
pub mod parallel;
mod spill;
pub(crate) mod stats;

use group_key::GroupKeyBuffer;

use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::fast_bytes_map::FastBytesMap;
use tidb_variable::mem_quota::StatementMemory;
use spill::new_group_bytes;

#[doc(inline)]
pub use parallel::HashAggContext;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_codec::{encode_bytes, encode_compact_bytes, encode_varint, NIL_FLAG, VARINT_FLAG};
use tidb_goruntime::GoString;
use tidb_datatype::{BinaryJSON, BinaryJSONValue, Datum, Decimal, EvalType, FieldType, FieldTypeCode, TimeType, MAX_DECIMAL_SCALE, UNSPECIFIED_LENGTH};
use tidb_charset::Collation;
use tidb_expr::compare_datums;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;
use tidb_disk as disk;
use tidb_memory::{ActionOnExceed, ArcAction, Tracker};
use tidb_selection::{select, Selectable};
use tidb_set::{
    Float64SetWithMemoryUsage, Int64SetWithMemoryUsage, StringSetWithMemoryUsage,
};

use tidb_executor_aggfuncs::aggfuncs::*;
use tidb_executor_aggfuncs::approx_count_distinct::ApproxCountDistinctSketch;
use tidb_executor_aggfuncs::aggfuncs::input::AggInputMode;

/// Go `StreamAggExec` for a global aggregate (an empty group-by list).
///
/// A global aggregate is already one ordered group, so no hash table or row
/// materialization is needed: each child row is folded into the one vector of
/// partial results and that vector emits exactly once, including for empty
/// input. The driver selects this executor only for Go plan shapes that choose
/// StreamAgg; grouped input continues through [`HashAggExec`].
pub struct StreamAggExec<C: Columns> {
    meta: ExecutorMeta,
    /// Go builder's DefaultVal, absent for partial or FIRST_ROW-only plans.
    emit_default_row: bool,
    agg_funcs: Vec<AggFunc>,
    input_modes: Vec<AggInputMode>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    states: Vec<AggState>,
    truncated: Vec<bool>,
    emitted: bool,
    child_returned_empty: bool,
}

impl<C: Columns> StreamAggExec<C> {

    /// Builds a one-group streaming aggregation over `child`.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        agg_funcs: Vec<AggFunc>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        let child_chunk = child.new_chunk();
        let states = agg_funcs.iter().map(AggState::new).collect();
        let truncated = vec![false; agg_funcs.len()];
        let input_modes = agg_funcs.iter().map(AggInputMode::new).collect();
        Self {
            emit_default_row: !agg_funcs
                .iter()
                .all(|func| matches!(func.kind, AggKind::FirstRow)),
            meta,
            agg_funcs,
            input_modes,
            child,
            ctx,
            child_chunk,
            states,
            truncated,
            emitted: false,
            child_returned_empty: true,
        }
    }
}

impl<C: Columns + Send> Executor for StreamAggExec<C> {
    fn agg_tree_input_empty(&self) -> bool {
        self.child_returned_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.states = self.agg_funcs.iter().map(AggState::new).collect();
        self.truncated.fill(false);
        self.emitted = false;
        self.child_returned_empty = true;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted {
            return Ok(());
        }
        loop {
            self.child.next(&mut self.child_chunk)?;
            let rows = self.child_chunk.num_rows();
            if rows == 0 {
                break;
            }
            self.child_returned_empty = false;
            let inputs = input::bind_inputs(&self.input_modes, &self.child_chunk);
            for row_index in 0..rows {
                input::update_row(
                    &inputs,
                    &self.agg_funcs,
                    &self.ctx,
                    &mut self.states,
                    self.child_chunk.get_row(row_index),
                )?;
            }
            drop(inputs);
            self.child_chunk.reset();
        }
        if self.child_returned_empty && !self.emit_default_row {
            self.emitted = true;
            return Ok(());
        }
        if self.agg_funcs.is_empty() {
            req.set_num_virtual_rows(1);
        } else {
            for index in 0..self.agg_funcs.len() {
                append_finished_agg_value(
                    &mut self.states[index],
                    &self.agg_funcs[index],
                    &self.meta.ret_field_types()[index],
                    &self.ctx,
                    &mut self.truncated[index],
                    req,
                    index,
                )?;
            }
        }
        self.emitted = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.states.clear();
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

/// Go `StreamAggExec` for input already ordered by every `GROUP BY` item.
///
/// Unlike [`HashAggExec`], this operator holds one group at a time. Go's
/// `VecGroupChecker` determines both adjacent groups and continuation across
/// chunks. The driver requires input ordered by every grouping expression.
pub struct GroupedStreamAggExec<C: Columns> {
    meta: ExecutorMeta,
    group_by: Vec<Expression>,
    agg_funcs: Vec<AggFunc>,
    input_modes: Vec<AggInputMode>,
    /// Output column for each aggregate state. This separates TiKV's
    /// function-first physical state order from the aggregation schema order
    /// Go exposes without an extra Projection.
    output_positions: Vec<usize>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    child_at: usize,
    states: Vec<AggState>,
    truncated: Vec<bool>,
    group_checker: tidb_executor_internal_vecgroupchecker::VecGroupChecker,
    group_end: usize,
    /// A group's rows have been folded and its result is still owed.
    group_open: bool,
    /// The current group's key datums, kept only when the output schema
    /// carries trailing group-by columns (a cop partial aggregation; see
    /// [`HashAggExec::output_group_keys`]).
    current_group_values: Vec<Datum>,
    output_group_keys: bool,
    child_done: bool,
    child_returned_empty: bool,
}

impl<C: Columns> GroupedStreamAggExec<C> {
    /// Builds a grouped streaming aggregation over an already ordered child.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        group_by: Vec<Expression>,
        agg_funcs: Vec<AggFunc>,
        output_positions: Vec<usize>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        debug_assert!(!group_by.is_empty());
        debug_assert_eq!(agg_funcs.len(), output_positions.len());
        debug_assert!(output_positions
            .iter()
            .copied()
            .all(|position| position < agg_funcs.len()));
        debug_assert!((0..agg_funcs.len()).all(|position| output_positions.contains(&position)));
        let child_chunk = child.new_chunk();
        let states = agg_funcs.iter().map(AggState::new).collect();
        let truncated = vec![false; agg_funcs.len()];
        let input_modes = agg_funcs.iter().map(AggInputMode::new).collect();
        let output_group_keys = meta.schema().len() > agg_funcs.len();
        Self {
            meta,
            group_checker: tidb_executor_internal_vecgroupchecker::VecGroupChecker::new(group_by.clone()),
            group_by,
            agg_funcs,
            input_modes,
            output_positions,
            child,
            ctx,
            child_chunk,
            child_at: 0,
            states,
            truncated,
            group_end: 0,
            group_open: false,
            current_group_values: Vec::new(),
            output_group_keys,
            child_done: false,
            child_returned_empty: true,
        }
    }

    fn group_values(&self, row: tidb_chunk::row::Row<'_>) -> Result<Vec<Datum>, ExecError> {
        if !self.output_group_keys {
            return Ok(Vec::new());
        }
        self.group_by
            .iter()
            .map(|expr| expr.eval(&self.ctx, row).map_err(ExecError::from))
            .collect()
    }

    /// Go `appendResult2Chunk`: the open group's result row, then every
    /// state reset for the next group.
    fn finish_group(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.emit_current(req)?;
        for (state, func) in self.states.iter_mut().zip(&self.agg_funcs) {
            state.reset(func);
        }
        self.group_open = false;
        Ok(())
    }

    fn emit_current(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        for index in 0..self.states.len() {
            let output_position = self.output_positions[index];
            append_finished_agg_value(
                &mut self.states[index],
                &self.agg_funcs[index],
                &self.meta.ret_field_types()[output_position],
                &self.ctx,
                &mut self.truncated[index],
                req,
                output_position,
            )?;
        }
        if self.output_group_keys {
            let trailing = self.meta.schema().len() - self.agg_funcs.len();
            for (offset, datum) in self.current_group_values[..trailing].iter().enumerate() {
                req.append_datum(self.agg_funcs.len() + offset, datum);
            }
        }
        Ok(())
    }
}

impl<C: Columns + Send> Executor for GroupedStreamAggExec<C> {
    fn agg_tree_input_empty(&self) -> bool {
        self.child_returned_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.child_at = 0;
        self.states = self.agg_funcs.iter().map(AggState::new).collect();
        self.truncated.fill(false);
        self.group_checker = tidb_executor_internal_vecgroupchecker::VecGroupChecker::new(self.group_by.clone());
        self.group_end = 0;
        self.group_open = false;
        self.current_group_values.clear();
        self.child_done = false;
        self.child_returned_empty = true;
        Ok(())
    }

    /// Go `StreamAggExec.Next`: `for !e.executed && !req.IsFull()`, one
    /// group (or the rest of one) per `consumeOneGroup`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while !self.child_done && !req.is_full() {
            if self.child_at >= self.child_chunk.num_rows() {
                // Go `consumeCurGroupRowsAndFetchChild`.
                self.child_chunk.reset();
                self.child.next(&mut self.child_chunk)?;
                self.child_at = 0;
                if self.child_chunk.num_rows() == 0 {
                    self.child_done = true;
                    if self.group_open {
                        self.finish_group(req)?;
                    }
                    return Ok(());
                }
                self.child_returned_empty = false;
                let same_as_prev = self
                    .group_checker
                    .split_into_groups(&self.ctx, &self.child_chunk)?;
                self.group_end = self.group_checker.get_next_group().1;
                if self.group_open && !same_as_prev {
                    self.finish_group(req)?;
                }
                if req.is_full() {
                    return Ok(());
                }
            }
            if !self.group_open {
                self.current_group_values =
                    self.group_values(self.child_chunk.get_row(self.child_at))?;
                self.group_open = true;
            }
            let end = self.group_end;
            let inputs = input::bind_inputs(&self.input_modes, &self.child_chunk);
            for row_index in self.child_at..end {
                let row = self.child_chunk.get_row(row_index);
                input::update_row(&inputs, &self.agg_funcs, &self.ctx, &mut self.states, row)?;
            }
            drop(inputs);
            self.child_at = end;
            if end < self.child_chunk.num_rows() {
                // The group ends inside this chunk; the next starts at `end`.
                self.finish_group(req)?;
                self.group_end = self.group_checker.get_next_group().1;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.states.clear();
        self.group_checker = tidb_executor_internal_vecgroupchecker::VecGroupChecker::new(self.group_by.clone());
        self.group_open = false;
        self.current_group_values.clear();
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

/// Go `HashAggExec` (serial `unparallelExec`): hash aggregation over the
/// child's rows, in ROUNDS when the statement's memory quota forces a spill.
///
/// # The round structure, which is Go's and only visible under spill
///
/// Without a spill there is exactly one round: drain the child, fold every row
/// into its group, emit the groups. Under a spill (see [`crate::agg_spill`]
/// for the trigger) a round STOPS OPENING NEW GROUPS -- each row whose group
/// this round does not already hold is written back to a spill file untouched
/// -- finishes and emits the groups it does hold, releases them, and starts
/// the next round by re-reading the rows it deferred. A group is therefore
/// opened, completed and emitted inside ONE round, which is why this arm never
/// has to merge two partial results for the same key.
///
/// The observable consequence, and it is Go's: the groups of a spilled
/// aggregation come out in a different ORDER than the same aggregation
/// unspilled (rounds, then first-seen within a round). A `GROUP BY` without an
/// `ORDER BY` guarantees no order in either engine.
/// The `HashAggContext` bound carries the parallel-pipeline capability
/// declaration every context must make (see [`hash_agg::parallel`]).
pub struct HashAggExec<C: HashAggContext> {
    meta: ExecutorMeta,
    /// Go builder's DefaultVal, absent for partial or FIRST_ROW-only plans.
    emit_default_row: bool,
    group_by: Vec<Expression>,
    agg_funcs: Vec<AggFunc>,
    input_modes: Vec<AggInputMode>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    /// Go `HashAggExec.IsChildReturnEmpty`: whether the last run saw NO input
    /// row at all -- read back through [`Executor::agg_tree_input_empty`].
    child_returned_empty: bool,

    // --- Go's `unparallelExec` state machine ---
    /// Group key -> index into `ordered`. Go's `groupSet` + `partialResultMap`.
    groups: FastBytesMap<usize>,
    /// Go GetGroupKey's reusable per-row buffers, filled column-wise before
    /// updating any aggregate state.
    group_key_buffer: GroupKeyBuffer,
    /// The group-by key datums of every open group, flattened by
    /// `group_by.len()`. A cop partial aggregation outputs its group-by
    /// columns after the aggregate columns (see [`super::final_mode_agg`]), so
    /// the executor has to retain the evaluated keys -- the encoded
    /// `group_key_buffer` alone cannot be turned back into datums.
    group_key_values: Vec<Datum>,
    /// Whether the output schema carries trailing group-by columns. Root
    /// aggregations report every group-by value through a `firstrow()`
    /// aggregate, so their schema is exactly as wide as `agg_funcs`; a cop
    /// partial omits those `firstrow()`s (Go: "group by items are outputted by
    /// group by schema") and is wider instead.
    output_group_keys: bool,
    /// The open groups' states, in first-seen order (Go's `groupKeys`). Group
    /// `g` occupies `g * agg_funcs.len()..(g + 1) * agg_funcs.len()` so the
    /// hot path does not allocate one inner `Vec` per group.
    ordered: Vec<AggState>,
    /// Each aggregate's argument collation, derived once here rather than
    /// for every new group's states.
    state_collations: Vec<tidb_charset::Collation>,
    group_count: usize,
    /// Go `cursor4GroupKey`: how many of `ordered` this round has emitted.
    cursor: usize,
    /// Go `prepared`: the current round's groups are complete and emitting.
    prepared: bool,
    /// Go `executed`: every row, including every spilled one, is accounted for.
    executed: bool,
    /// The per-GROUP_CONCAT "already warned" sentinel. It lives here rather
    /// than in a round because MySQL emits exactly ONE 1260 per function per
    /// STATEMENT, and a spilled aggregation emits its groups over several
    /// rounds (`func_group_concat.go:56-60`).
    truncated: Vec<bool>,

    // --- spill (`agg_spill.go`'s `AggSpillDiskAction` arm) ---
    /// The statement's budget: this operator's tracker hangs off it and its
    /// quota is what the spill action and the cancellation both watch.
    memory: StatementMemory,
    /// Go `HashAggExec.memTracker`.
    tracker: Arc<Tracker>,
    /// Go `HashAggExec.diskTracker`.
    disk_tracker: Arc<disk::Tracker>,
    /// Go `HashAggExec.inSpillMode`, raised by the action.
    in_spill_mode: Arc<AtomicBool>,
    /// The action registered on the session tracker's SOFT-limit slot, kept so
    /// `close` can finish it (Go `Close`: `e.spillAction.SetFinished()`).
    spill_action: Option<Arc<AggSpillDiskAction>>,
    /// Go `HashAggExec.parallelAggSpillAction`.
    parallel_spill_action: Option<Arc<ParallelAggSpillDiskAction>>,
    /// Go `parallelHashAggSpillHelper.status == needSpill`.
    parallel_spill_requested: Arc<AtomicBool>,
    /// Go `HashAggExec.dataInDisk` for the parallel arm: the 256 spill
    /// partitions stay open (and on disk) until `Close` drops them.
    parallel_spilled: Option<parallel::ParallelSpillPartitions>,
    /// Go `HashAggExec.dataInDisk`, created on the first spill.
    data_in_disk: Option<DataInDiskByChunks>,
    /// Go `HashAggExec.tmpChkForSpill`.
    tmp_chk_for_spill: Chunk,
    /// Go `numOfSpilledChks`: chunks spilled as of the last round boundary.
    num_of_spilled_chks: usize,
    /// Go `offsetOfSpilledChks`: the next spilled chunk to re-read.
    offset_of_spilled_chks: usize,
    /// Go `isChildDrained`.
    is_child_drained: bool,
    /// Go finalOutputCh and reusable final-result holders, owned until EOF
    /// or Close. Final workers cannot run ahead of their returned chunks.
    parallel_output: Option<parallel::FinalOutput>,
    /// The parallel partial/final worker pipeline is engaged for this Open
    /// (Go `parallelExecValid`). Decided once per Open; `execute` never
    /// re-decides mid-run.
    pipeline_mode: bool,
    /// Resolved worker counts for the current Open.
    pipeline_partial_concurrency: usize,
    pipeline_final_concurrency: usize,
    /// Go `StmtCtx.RuntimeStatsColl` and the plan Close registers
    /// `HashAggRuntimeStats` on.
    runtime_stats: Option<(Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>, i64)>,
    /// Go `HashAggExec.stats`, created by `initForParallelExec`.
    pub(crate) hash_agg_stats: Option<Arc<stats::HashAggRuntimeStats>>,
    /// Test/diagnostic override standing in for SET concurrency variables.
    #[cfg(test)]
    pipeline_concurrency_override: Option<(usize, usize)>,
    /// Unit-test observations shared with pipeline workers.
    #[cfg(test)]
    pipeline_stats: Option<Arc<parallel::PipelineStats>>,
}

impl<C: HashAggContext> HashAggExec<C> {

    /// Builds a hash aggregation of `agg_funcs` over `child`, grouped by
    /// `group_by` (empty for a global aggregate).
    ///
    /// `memory` is the statement's budget, required for the same reason
    /// [`tidb_executor_sortexec::SortExec::new`] requires it: a call site must not be
    /// able to build an UNACCOUNTED group table by omitting it.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        group_by: Vec<Expression>,
        agg_funcs: Vec<AggFunc>,
        child: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let child_chunk = child.new_chunk();
        let tmp_chk_for_spill = child.new_chunk();
        let tracker = memory.operator_tracker(meta.id());
        let disk_tracker = memory.operator_disk_tracker(meta.id());
        let truncated = vec![false; agg_funcs.len()];
        let input_modes = agg_funcs.iter().map(AggInputMode::new).collect();
        let state_collations = agg_funcs.iter().map(state_collation).collect();
        // A cop partial aggregation's schema appends the group-by columns after
        // the aggregate columns; a root aggregation reaches the same width
        // through its `firstrow()` aggregates and has no trailing columns.
        let output_group_keys = meta.schema().len() > agg_funcs.len();
        HashAggExec {
            emit_default_row: !agg_funcs
                .iter()
                .all(|func| matches!(func.kind, AggKind::FirstRow)),
            meta,
            group_by,
            agg_funcs,
            input_modes,
            child,
            ctx,
            child_chunk,
            child_returned_empty: true,
            groups: FastBytesMap::default(),
            group_key_buffer: GroupKeyBuffer::default(),
            group_key_values: Vec::new(),
            output_group_keys,
            ordered: Vec::new(),
            state_collations,
            group_count: 0,
            cursor: 0,
            prepared: false,
            executed: false,
            truncated,
            memory,
            tracker,
            disk_tracker,
            in_spill_mode: Arc::new(AtomicBool::new(false)),
            spill_action: None,
            parallel_spill_action: None,
            parallel_spill_requested: Arc::new(AtomicBool::new(false)),
            parallel_spilled: None,
            data_in_disk: None,
            tmp_chk_for_spill,
            num_of_spilled_chks: 0,
            offset_of_spilled_chks: 0,
            is_child_drained: false,
            parallel_output: None,
            pipeline_mode: false,
            pipeline_partial_concurrency: 1,
            pipeline_final_concurrency: 1,
            runtime_stats: None,
            hash_agg_stats: None,
            #[cfg(test)]
            pipeline_concurrency_override: None,
            #[cfg(test)]
            pipeline_stats: None,
        }
    }

    /// Go `e.RuntimeStats() != nil`: a parallel aggregation collects and
    /// registers `HashAggRuntimeStats` on `plan_id`.
    pub fn set_runtime_stats(
        &mut self,
        source: Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>>,
        plan_id: i64,
    ) {
        self.runtime_stats = source.map(|source| (source, plan_id));
    }

    /// Go's inner loop of `execute`: fold `chunk`'s rows into their groups,
    /// returning the bytes the group table grew by and the rows this round
    /// refused to open a group for (Go's `sel`).
    fn fold_chunk(&mut self, chunk: &Chunk, rows: usize) -> Result<Vec<usize>, ExecError> {
        let mut sel: Vec<usize> = Vec::new();
        debug_assert_eq!(rows, chunk.num_rows());
        self.group_key_buffer
            .prepare(&self.ctx, chunk, &self.group_by, self.output_group_keys)?;
        let inputs = input::bind_inputs(&self.input_modes, chunk);
        let mut state_memory_delta = 0;
        for r in 0..rows {
            let row = chunk.get_row(r);
            let key = &self.group_key_buffer.encoded[r];
            let idx = match self.groups.get(key) {
                Some(&idx) => idx,
                None => {
                    // Go: a round in spill mode opens no new group -- but it
                    // must open the FIRST one, or a round could make no
                    // progress at all and the aggregation would not terminate.
                    if self.in_spill_mode.load(SeqCst) && self.group_count != 0 {
                        sel.push(r);
                        continue;
                    }
                    let idx = self.group_count;
                    let key_len = key.len();
                    self.groups.insert(key.clone(), idx);
                    self.ordered.extend(
                        self.agg_funcs
                            .iter()
                            .zip(&self.state_collations)
                            .map(|(func, collation)| AggState::with_collation(func, *collation)),
                    );
                    self.group_count += 1;
                    if self.output_group_keys {
                        self.group_key_buffer
                            .append_values(r, &mut self.group_key_values);
                    }
                    // Consumed HERE, not at the end of the chunk: Go consumes
                    // inside `getPartialResults`, per group, so the spill
                    // action fires PART WAY THROUGH a chunk and the rest of
                    // that chunk is already deferred. Accumulating a whole
                    // chunk's groups into one `Consume` would let a single
                    // call jump clean over the quota between the soft limit
                    // that spills and the hard limit that cancels.
                    self.tracker
                        .consume(new_group_bytes(key_len, &self.agg_funcs));
                    idx
                }
            };
            let width = self.agg_funcs.len();
            let delta = input::update_row(
                &inputs,
                &self.agg_funcs,
                &self.ctx,
                &mut self.ordered[idx * width..(idx + 1) * width],
                row,
            )?;
            state_memory_delta += delta;
        }
        // Go execute accounts aggregate-state growth once per input chunk;
        // allocations for newly opened groups remain charged inside the loop.
        self.tracker.consume(state_memory_delta);
        Ok(sel)
    }

    /// Appends group `idx`'s final values to `req`.
    fn emit_group(&mut self, idx: usize, req: &mut Chunk) -> Result<(), ExecError> {
        let group_offset = idx * self.agg_funcs.len();
        for c in 0..self.agg_funcs.len() {
            append_finished_agg_value(
                &mut self.ordered[group_offset + c],
                &self.agg_funcs[c],
                &self.meta.ret_field_types()[c],
                &self.ctx,
                &mut self.truncated[c],
                req,
                c,
            )?;
        }
        if self.output_group_keys {
            let trailing = self.meta.schema().len() - self.agg_funcs.len();
            let base = idx * self.group_by.len();
            for (offset, datum) in self.group_key_values[base..base + trailing]
                .iter()
                .enumerate()
            {
                req.append_datum(self.agg_funcs.len() + offset, datum);
            }
        }
        Ok(())
    }
}

impl<C: HashAggContext> Executor for HashAggExec<C> {
    /// Go `aggExecutorTreeInputEmpty`'s one true answer: the walk exists to
    /// find an aggregation and read its `IsChildReturnEmpty`.
    fn agg_tree_input_empty(&self) -> bool {
        self.child_returned_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.parallel_output = None;
        self.child.open()?;
        self.child_chunk.reset();
        self.tmp_chk_for_spill.reset();
        self.child_returned_empty = true;
        self.groups.clear();
        self.group_key_buffer = GroupKeyBuffer::default();
        self.group_key_values.clear();
        self.ordered.clear();
        self.group_count = 0;
        self.cursor = 0;
        self.prepared = false;
        self.executed = false;
        for flag in &mut self.truncated {
            *flag = false;
        }
        if let Some(in_disk) = &mut self.data_in_disk {
            in_disk.close();
        }
        self.data_in_disk = None;
        self.parallel_spilled = None;
        self.num_of_spilled_chks = 0;
        self.offset_of_spilled_chks = 0;
        self.is_child_drained = false;
        self.pipeline_mode = false;
        #[cfg(test)]
        {
            self.pipeline_stats = None;
        }
        self.in_spill_mode.store(false, SeqCst);
        self.parallel_spill_requested.store(false, SeqCst);
        // Go `HashAggExec.Open` -> `e.memTracker.Reset()`: an aggregation
        // re-opened by an Apply's inner side must not keep charging for the
        // groups it has just dropped.
        self.tracker.replace_bytes_used(0);
        if let Some(action) = self.spill_action.take() {
            action.set_finished();
        }
        if let Some(action) = self.parallel_spill_action.take() {
            action.set_finished();
        }
        let pipeline_counts = self.pipeline_eligibility();
        if let Some((partial_concurrency, final_concurrency)) = pipeline_counts {
            if <C as HashAggContext>::PARALLEL_WORKERS_MAY_EVAL {
                // Go `initForParallelExec`: worker counts resolved from the
                // session variables; the pipeline takes this aggregation over.
                #[cfg(test)]
                {
                    self.pipeline_stats = Some(Arc::new(parallel::PipelineStats::new(
                        partial_concurrency,
                        final_concurrency,
                    )));
                }
                self.pipeline_mode = true;
                self.pipeline_partial_concurrency = partial_concurrency;
                self.pipeline_final_concurrency = final_concurrency;
                // Go `initRuntimeStats`.
                self.hash_agg_stats = self.runtime_stats.as_ref().map(|_| {
                    Arc::new(stats::HashAggRuntimeStats::new(
                        partial_concurrency,
                        final_concurrency,
                    ))
                });
                if self.parallel_spill_enabled() {
                    let (action, requested) = ParallelAggSpillDiskAction::new(&self.tracker);
                    self.parallel_spill_requested = requested;
                    self.memory
                        .session_tracker()
                        .fallback_old_and_set_new_action_for_soft_limit(
                            Arc::clone(&action) as ArcAction
                        );
                    self.parallel_spill_action = Some(action);
                }
                return Ok(());
            }
        }
        // Go `initForUnparallelExec`: a FRESH action per Open (so `spillTimes`
        // starts over), registered on the SOFT-limit slot, and only when
        // `tidb_enable_tmp_storage_on_oom` is on -- with it off an overrun goes
        // straight to the 8175 cancellation on the hard-limit slot.
        if self.memory.tmp_storage_on_oom() {
            let (action, in_spill_mode) = AggSpillDiskAction::new(&self.tracker);
            self.in_spill_mode = in_spill_mode;
            self.memory
                .session_tracker()
                .fallback_old_and_set_new_action_for_soft_limit(Arc::clone(&action) as ArcAction);
            self.spill_action = Some(action);
        }
        Ok(())
    }

    /// Go `unparallelExec`: emit the current round's groups until the chunk is
    /// full, and when they run out, run the next round.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        loop {
            if let Some(output) = &mut self.parallel_output {
                let result = output.next(req, &self.memory);
                if matches!(result, Ok(true)) {
                    return Ok(());
                }
                output.close(&mut self.truncated);
                self.parallel_output = None;
                result?;
                continue;
            }
            if self.prepared {
                while self.cursor < self.group_count {
                    if self.agg_funcs.is_empty() {
                        // Go: `chk.SetNumVirtualRows(chk.NumRows() + 1)` -- a
                        // group with no aggregate columns is still a row.
                        req.set_num_virtual_rows(req.num_rows() + 1);
                    }
                    self.emit_group(self.cursor, req)?;
                    self.cursor += 1;
                    if req.is_full() {
                        return Ok(());
                    }
                }
                self.reset_spill_mode();
            }
            if self.executed {
                return Ok(());
            }
            self.execute()?;
            // No group-by and no data: one empty group, so a global COUNT is 0.
            // (The pipeline synthesizes its own defaults row; its output is
            // already staged in `parallel_output`.)
            if self.group_count == 0
                && self.emit_default_row
                && self.group_by.is_empty()
                && !self.pipeline_mode
            {
                self.ordered
                    .extend(self.agg_funcs.iter().map(AggState::new));
                self.group_count = 1;
            }
            self.prepared = true;
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(mut output) = self.parallel_output.take() {
            output.close(&mut self.truncated);
        }
        // Go `HashAggExec.Close`'s deferred `RegisterStats(e.ID(), e.stats)`.
        if let (Some((source, plan_id)), Some(stats)) =
            (&self.runtime_stats, self.hash_agg_stats.take())
        {
            source.register_stats(*plan_id, Box::new(stats.snapshot()));
        }
        self.groups.clear();
        self.group_key_values.clear();
        self.group_key_buffer = GroupKeyBuffer::default();
        self.ordered.clear();
        self.group_count = 0;
        self.pipeline_mode = false;
        #[cfg(test)]
        {
            self.pipeline_stats = None;
        }
        if let Some(in_disk) = &mut self.data_in_disk {
            in_disk.close();
        }
        self.data_in_disk = None;
        if let Some(action) = self.spill_action.take() {
            action.set_finished();
        }
        if let Some(action) = self.parallel_spill_action.take() {
            action.set_finished();
        }
        // Go `HashAggExec.Close`: `e.dataInDisk.Close()` removes the parallel
        // spill files as well.
        self.parallel_spilled = None;
        self.tracker.replace_bytes_used(0);
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


#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_exprctx::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    /// A test-only source emitting one prebuilt chunk then EOF.
    struct OneChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
    }
    impl Executor for OneChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
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

    /// A test-only source that keeps chunk boundaries visible to the aggregate
    /// worker-window implementation.
    struct MultiChunkSource {
        meta: ExecutorMeta,
        chunks: Vec<Chunk>,
        cursor: usize,
    }
    impl Executor for MultiChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(chunk) = self.chunks.get(self.cursor) {
                for row in 0..chunk.num_rows() {
                    req.append_row(chunk.get_row(row));
                }
                self.cursor += 1;
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
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

    fn col(index: i64) -> Expression {
        let mut c = Column::new(index + 1, long());
        c.index = index;
        Expression::Column(c)
    }

    fn typed_col(index: i64, field_type: FieldType) -> Expression {
        let mut c = Column::new(index + 1, field_type);
        c.index = index;
        Expression::Column(c)
    }

    /// A test source over the given rows, split into `MAX_CHUNK_SIZE`-row
    /// chunks like a real executor child (whose `next` fills at most one
    /// chunk). The parallel pipeline admits one lane per CHUNK, so a
    /// single-chunk source would exercise only one worker.
    fn source(rows: &[(i64, Option<i64>)]) -> Box<dyn Executor> {
        // Two long columns: group key, value (None = NULL).
        let fields = vec![long(), long()];
        let mut chunks = Vec::new();
        for batch in rows.chunks(1024) {
            let mut data = Chunk::new_with_capacity(&fields, batch.len().max(1));
            for (g, v) in batch {
                data.append_int64(0, *g);
                match v {
                    Some(v) => data.append_int64(1, *v),
                    None => data.append_null(1),
                }
            }
            chunks.push(data);
        }
        let mut cols = Vec::new();
        for i in 0..2 {
            let mut c = Column::new(i + 1, long());
            c.index = i;
            cols.push(c);
        }
        Box::new(MultiChunkSource {
            meta: ExecutorMeta::new(Schema::new(cols), 0, rows.len().max(1), 1024),
            chunks,
            cursor: 0,
        })
    }

    fn out_meta(n: usize) -> ExecutorMeta {
        let mut cols = Vec::new();
        for i in 0..n {
            let mut c = Column::new((i + 1) as i64, long());
            c.index = i as i64;
            cols.push(c);
        }
        ExecutorMeta::new(Schema::new(cols), 1, 4, 1024)
    }

    /// An output schema whose column types are given, so a decimal result
    /// lands in a decimal cell.
    fn out_meta_typed(types: &[FieldType]) -> ExecutorMeta {
        let mut cols = Vec::new();
        for (i, t) in types.iter().enumerate() {
            let mut c = Column::new((i + 1) as i64, t.clone());
            c.index = i as i64;
            cols.push(c);
        }
        ExecutorMeta::new(Schema::new(cols), 1, 4, 1024)
    }

    #[test]
    fn integer_group_key_fast_path_matches_go_hash_group_key_encoding() {
        let types = [long(), long().with_unsigned(true)];
        let mut chunk = Chunk::new_with_capacity(&types, 2);
        chunk.append_int64(0, -7);
        chunk.append_uint64(1, 7);
        chunk.append_null(0);
        chunk.append_null(1);

        for (column, field_type) in types.iter().enumerate() {
            let mut expr = Column::new(column as i64, field_type.clone());
            expr.index = column as i64;
            let mut keys = GroupKeyBuffer::default();
            keys.prepare(&NoColumns, &chunk, &[Expression::Column(expr)], false)
                .unwrap();
            for row_index in 0..chunk.num_rows() {
                let row = chunk.get_row(row_index);
                let expected =
                    tidb_codec::hash_group_key(&[row.get_datum(column, field_type)], field_type)
                        .unwrap()
                        .pop()
                        .unwrap();
                assert_eq!(
                    keys.encoded[row_index], expected,
                    "row {row_index}, column {column}"
                );
            }
        }
    }

    #[test]
    fn low_quota_does_not_change_go_parallel_admission() {
        let exec = HashAggExec::new(
            out_meta(1),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            source(&[(1, Some(1))]),
            NoColumns,
            StatementMemory::new(1 << 20, tidb_vardef::OomAction::Cancel, 42)
                .with_tmp_storage_on_oom(false),
        );

        assert!(exec.pipeline_eligibility().is_some());
    }

    fn decimal() -> FieldType {
        FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal)
    }

    fn binary_varchar() -> FieldType {
        FieldType::new(FieldTypeCode::Varchar)
            .with_flen(42)
            .with_collation(Collation::Binary)
    }

    fn binary_string_source(values: &[Option<&[u8]>]) -> Box<dyn Executor> {
        let field_type = binary_varchar();
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&field_type), values.len());
        for value in values {
            match value {
                Some(value) => data.append_bytes(0, value),
                None => data.append_null(0),
            }
        }
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        Box::new(OneChunkSource {
            meta: ExecutorMeta::new(Schema::new(vec![column]), 0, values.len(), 1024),
            data: Some(data),
        })
    }

    #[test]
    fn global_binary_count_distinct_uses_direct_bytes() {
        let field_type = binary_varchar();
        let agg = AggFunc {
            kind: AggKind::Count,
            arg: Some(typed_col(0, field_type)),
            extra_args: Vec::new(),
            distinct: true,
            order_by: Vec::new(),
            arg_orig_name: String::new(),
        };
        let exec = HashAggExec::new(
            out_meta(1),
            vec![],
            vec![agg],
            binary_string_source(&[Some(b"a"), Some(b"b"), Some(b"a"), None]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(exec), vec![vec![Datum::Int(2)]]);
    }

    /// Go's `countOriginalWithDistinct4Real` uses a float64 map: duplicate
    /// finite values collapse, signed zero aliases, and each NaN insertion is
    /// a distinct map key. The typed Rust state must keep those rules in both
    /// the serial and partial/final worker paths.
    #[test]
    fn real_count_distinct_uses_go_float_set_semantics() {
        let field_type = FieldType::new(FieldTypeCode::Double);
        let values = [
            Some(1.0),
            Some(1.0),
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            None,
        ];
        let make_source = || {
            let mut data =
                Chunk::new_with_capacity(std::slice::from_ref(&field_type), values.len());
            for value in values {
                match value {
                    Some(value) => data.append_float64(0, value),
                    None => data.append_null(0),
                }
            }
            let mut column = Column::new(1, field_type.clone());
            column.index = 0;
            Box::new(OneChunkSource {
                meta: ExecutorMeta::new(Schema::new(vec![column]), 0, values.len(), values.len()),
                data: Some(data),
            }) as Box<dyn Executor>
        };
        let make_agg = || {
            let mut agg = AggFunc::new(AggKind::Count, Some(typed_col(0, field_type.clone())));
            agg.distinct = true;
            agg
        };
        let output_types = [long()];
        let serial = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![],
            vec![make_agg()],
            make_source(),
            NoColumns,
            StatementMemory::default(),
        )
        .with_pipeline_concurrency_override(1, 1);
        let parallel = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![],
            vec![make_agg()],
            make_source(),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run_typed(serial, &output_types), vec![vec![Datum::Int(5)]]);
        assert_eq!(
            run_typed(parallel, &output_types),
            vec![vec![Datum::Int(5)]]
        );
    }

    #[test]
    fn max_min_count_hash_agg_returns_extreme_tie_counts() {
        let rows = [
            (0, Some(0)),
            (0, Some(0)),
            (0, Some(1)),
            (0, Some(4)),
            (0, Some(4)),
            (0, Some(4)),
            (0, None),
        ];
        let max = AggFunc::new(AggKind::MaxCount, Some(col(1)));
        let min = AggFunc::new(AggKind::MinCount, Some(col(1)));
        let exec = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![max, min],
            source(&rows),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(exec), vec![vec![Datum::Int(3), Datum::Int(2)]]);
    }

    #[test]
    fn grouped_binary_strings_use_go_parallel_hashagg_pipeline() {
        let field_type = binary_varchar();
        let output_types = [field_type.clone(), long()];
        let mut first_row = AggFunc::new(AggKind::FirstRow, Some(typed_col(0, field_type.clone())));
        first_row.distinct = true;
        let mut exec = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![typed_col(0, field_type.clone())],
            vec![first_row, AggFunc::new(AggKind::Count, None)],
            binary_string_source(&[Some(b"alpha"), Some(b"beta"), Some(b"alpha"), None]),
            NoColumns,
            StatementMemory::default(),
        );
        exec.open().unwrap();
        let mut output = exec.new_chunk();
        output.set_required_rows(1, exec.max_chunk_size());
        exec.next(&mut output).unwrap();
        let (_, _, dispatched, workers) = exec
            .pipeline_run_info()
            .expect("Go admits this DISTINCT HashAgg to its parallel pipeline");
        assert_eq!(dispatched, 1);
        // Go starts its configured workers; only one consumes this chunk.
        assert!(workers >= 1);
        let mut rows = Vec::new();
        loop {
            rows.extend((0..output.num_rows()).map(|index| {
                let row = output.get_row(index);
                vec![
                    row.get_datum(0, &output_types[0]),
                    row.get_datum(1, &output_types[1]),
                ]
            }));
            output.set_required_rows(exec.max_chunk_size() as isize, exec.max_chunk_size());
            exec.next(&mut output).unwrap();
            if output.num_rows() == 0 {
                break;
            }
        }
        assert_rows_unordered(
            rows,
            vec![
                vec![
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"alpha".to_vec(),
                        Collation::Binary,
                    )),
                    Datum::Int(2),
                ],
                vec![
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"beta".to_vec(),
                        Collation::Binary,
                    )),
                    Datum::Int(1),
                ],
                vec![Datum::Null, Datum::Int(1)],
            ],
        );
        exec.close().unwrap();
    }

    #[test]
    fn grouped_binary_strings_follow_go_parallel_hashagg_admission() {
        let field_type = binary_varchar();
        let output_types = [field_type.clone(), long()];
        let exec = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![typed_col(0, field_type.clone())],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(typed_col(0, field_type))),
                AggFunc::new(AggKind::Count, None),
            ],
            binary_string_source(&[Some(b"alpha"), Some(b"beta"), Some(b"alpha"), None]),
            NoColumns,
            StatementMemory::default(),
        );

        assert!(
            exec.pipeline_eligibility().is_some(),
            "Go admits this HashAgg to parallel execution; key representation is not a planner gate"
        );
    }

    #[test]
    fn grouped_binary_string_final_count_adds_partial_counts() {
        let group_type = binary_varchar();
        let sum_type = decimal_with_shape(20, 2);
        let count_type = long();
        let fields = [group_type.clone(), sum_type.clone(), count_type.clone()];
        let mut data = Chunk::new_with_capacity(&fields, 3);
        for (group, sum, count) in [
            (b"a".as_slice(), "1.00", 2),
            (b"a", "2.50", 3),
            (b"b", "3.00", 1),
        ] {
            data.append_bytes(0, group);
            data.append_datum(
                1,
                &Datum::Decimal(tidb_datatype::Decimal::from_literal(sum)),
            );
            data.append_int64(2, count);
        }
        let columns = fields
            .iter()
            .enumerate()
            .map(|(index, field_type)| {
                let mut column = Column::new((index + 1) as i64, field_type.clone());
                column.index = index as i64;
                column
            })
            .collect();
        let source = Box::new(OneChunkSource {
            meta: ExecutorMeta::new(Schema::new(columns), 0, 3, 1024),
            data: Some(data),
        });
        let output_types = [sum_type.clone(), count_type.clone(), group_type.clone()];
        let exec = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![typed_col(0, group_type.clone())],
            vec![
                AggFunc::new(AggKind::Sum, Some(typed_col(1, sum_type))),
                AggFunc::new(AggKind::FinalCount, Some(typed_col(2, count_type))),
                AggFunc::new(AggKind::FirstRow, Some(typed_col(0, group_type.clone()))),
            ],
            source,
            NoColumns,
            StatementMemory::default(),
        );
        assert_rows_unordered(
            run_typed(exec, &output_types),
            vec![
                vec![
                    Datum::Decimal(tidb_datatype::Decimal::from_literal("3.50")),
                    Datum::Int(5),
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"a".to_vec(),
                        Collation::Binary,
                    )),
                ],
                vec![
                    Datum::Decimal(tidb_datatype::Decimal::from_literal("3.00")),
                    Datum::Int(1),
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"b".to_vec(),
                        Collation::Binary,
                    )),
                ],
            ],
        );
    }

    #[test]
    fn grouped_binary_string_parallel_workers_merge_exact_decimal() {
        let group_type = binary_varchar();
        let sum_type = decimal_with_shape(20, 2);
        let count_type = long();
        let fields = [group_type.clone(), sum_type.clone(), count_type.clone()];
        let make_chunk = |rows: &[(&[u8], &str, i64)]| {
            let mut chunk = Chunk::new_with_capacity(&fields, rows.len());
            for (group, sum, count) in rows {
                chunk.append_bytes(0, group);
                chunk.append_datum(
                    1,
                    &Datum::Decimal(tidb_datatype::Decimal::from_literal(sum)),
                );
                chunk.append_int64(2, *count);
            }
            chunk
        };
        let columns = fields
            .iter()
            .enumerate()
            .map(|(index, field_type)| {
                let mut column = Column::new((index + 1) as i64, field_type.clone());
                column.index = index as i64;
                column
            })
            .collect();
        let source = Box::new(MultiChunkSource {
            meta: ExecutorMeta::new(Schema::new(columns), 0, 2, 1024),
            chunks: vec![
                make_chunk(&[(b"a", "1.00", 2), (b"b", "3.00", 1)]),
                make_chunk(&[(b"a", "2.50", 3), (b"c", "4.00", 1)]),
            ],
            cursor: 0,
        });
        let output_types = [sum_type.clone(), count_type.clone(), group_type.clone()];
        let mut exec = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![typed_col(0, group_type.clone())],
            vec![
                AggFunc::new(AggKind::Sum, Some(typed_col(1, sum_type))),
                AggFunc::new(AggKind::FinalCount, Some(typed_col(2, count_type))),
                AggFunc::new(AggKind::FirstRow, Some(typed_col(0, group_type))),
            ],
            source,
            NoColumns,
            StatementMemory::default(),
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut rows = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for row_index in 0..req.num_rows() {
                let row = req.get_row(row_index);
                rows.push(
                    (0..req.num_cols())
                        .map(|column| row.get_datum(column, &output_types[column]))
                        .collect::<Vec<_>>(),
                );
            }
        }
        assert_rows_unordered(
            rows,
            vec![
                vec![
                    Datum::Decimal(tidb_datatype::Decimal::from_literal("3.50")),
                    Datum::Int(5),
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"a".to_vec(),
                        Collation::Binary,
                    )),
                ],
                vec![
                    Datum::Decimal(tidb_datatype::Decimal::from_literal("3.00")),
                    Datum::Int(1),
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"b".to_vec(),
                        Collation::Binary,
                    )),
                ],
                vec![
                    Datum::Decimal(tidb_datatype::Decimal::from_literal("4.00")),
                    Datum::Int(1),
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"c".to_vec(),
                        Collation::Binary,
                    )),
                ],
            ],
        );
        let (_, _, dispatched, workers) = exec.pipeline_run_info().expect("pipeline ran");
        assert_eq!(dispatched, 2);
        assert!(workers > 1);
        exec.close().unwrap();
    }

    fn decimal_with_shape(flen: i64, scale: i64) -> FieldType {
        let mut field_type = decimal();
        field_type.set_flen(flen);
        field_type.set_decimal(scale);
        field_type
    }

    fn decimal_source(rows: &[Option<&str>], field_type: &FieldType) -> Box<dyn Executor> {
        let mut data =
            Chunk::new_with_capacity(std::slice::from_ref(field_type), rows.len().max(1));
        for value in rows {
            match value {
                Some(value) => data.append_datum(
                    0,
                    &Datum::Decimal(tidb_datatype::Decimal::from_literal(value)),
                ),
                None => data.append_null(0),
            }
        }
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        Box::new(OneChunkSource {
            meta: ExecutorMeta::new(Schema::new(vec![column]), 0, rows.len().max(1), 1024),
            data: Some(data),
        })
    }

    fn final_decimal_avg_source(groups: i64) -> Box<dyn Executor> {
        let decimal_type = decimal_with_shape(20, 2);
        let fields = vec![long(), long(), decimal_type.clone()];
        // Chunk like a real child so the parallel pipeline admits more than
        // one lane (one lane per chunk).
        let mut chunks = Vec::new();
        let mut data = Chunk::new_with_capacity(&fields, 1024);
        for group in 0..groups {
            for (count, value) in [(2, "3.00"), (1, "4.00")] {
                data.append_int64(0, group);
                data.append_int64(1, count);
                data.append_datum(
                    2,
                    &Datum::Decimal(tidb_datatype::Decimal::from_literal(value)),
                );
                if data.num_rows() >= 1024 {
                    chunks.push(std::mem::replace(
                        &mut data,
                        Chunk::new_with_capacity(&fields, 1024),
                    ));
                }
            }
        }
        if data.num_rows() > 0 {
            chunks.push(data);
        }
        let columns = fields
            .into_iter()
            .enumerate()
            .map(|(index, field_type)| {
                let mut column = Column::new((index + 1) as i64, field_type);
                column.index = index as i64;
                column
            })
            .collect();
        Box::new(MultiChunkSource {
            meta: ExecutorMeta::new(Schema::new(columns), 0, (groups as usize) * 2, 1024),
            chunks,
            cursor: 0,
        })
    }

    fn run_typed(mut exec: HashAggExec<NoColumns>, types: &[FieldType]) -> Vec<Vec<Datum>> {
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut out = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..req.num_cols())
                        .map(|c| row.get_datum(c, &types[c]))
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        out
    }

    fn sort_rows(rows: &mut [Vec<Datum>]) {
        rows.sort_by(|left, right| {
            for (left, right) in left.iter().zip(right) {
                let ordering = compare_datums(left, right).unwrap_or(Ordering::Equal);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
        });
    }

    fn assert_rows_unordered(mut actual: Vec<Vec<Datum>>, mut expected: Vec<Vec<Datum>>) {
        sort_rows(&mut actual);
        sort_rows(&mut expected);
        assert_eq!(actual, expected);
    }

    #[test]
    fn min_max_skip_nulls_and_report_null_for_an_all_null_group() {
        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc::new(AggKind::Min, Some(col(1))),
                AggFunc::new(AggKind::Max, Some(col(1))),
            ],
            source(&[(1, Some(30)), (1, None), (1, Some(10)), (1, Some(20))]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(agg), vec![vec![Datum::Int(10), Datum::Int(30)]]);

        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc::new(AggKind::Min, Some(col(1))),
                AggFunc::new(AggKind::Max, Some(col(1))),
            ],
            source(&[(1, None), (1, None)]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(agg), vec![vec![Datum::Null, Datum::Null]]);
    }

    fn grouped(
        group_by: Expression,
        funcs: Vec<AggFunc>,
        rows: &[(i64, Option<i64>)],
    ) -> GroupedStreamAggExec<NoColumns> {
        let positions = (0..funcs.len()).collect();
        GroupedStreamAggExec::new(
            out_meta(funcs.len()),
            vec![group_by],
            funcs,
            positions,
            source(rows),
            NoColumns,
        )
    }

    fn drain_grouped(
        exec: &mut GroupedStreamAggExec<NoColumns>,
        required_rows: usize,
    ) -> Vec<Vec<Option<i64>>> {
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut output = Vec::new();
        loop {
            req.set_required_rows(required_rows as isize, exec.max_chunk_size());
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            assert!(req.num_rows() <= required_rows);
            for row in 0..req.num_rows() {
                let row = req.get_row(row);
                output.push(
                    (0..req.num_cols())
                        .map(|col| (!row.is_null(col)).then(|| row.get_int64(col)))
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        output
    }

    /// Go `StreamAggExec`: a group whose rows straddle chunk boundaries is
    /// one group (`isFirstGroupSameAsPrev`), the last group emits at end of
    /// input, and `Next` fills only the requested rows.
    #[test]
    fn grouped_stream_agg_stops_at_required_rows_at_a_chunk_boundary() {
        let field = long();
        let chunks = [[1, 1], [2, 3]].map(|values| {
            let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 2);
            for value in values {
                chunk.append_int64(0, value);
            }
            chunk
        });
        let source = Box::new(MultiChunkSource {
            meta: out_meta(1),
            chunks: chunks.into(),
            cursor: 0,
        });
        let mut exec = GroupedStreamAggExec::new(
            out_meta(1),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, None)],
            vec![0],
            source,
            NoColumns,
        );
        assert_eq!(
            drain_grouped(&mut exec, 1),
            vec![vec![Some(2)], vec![Some(1)], vec![Some(1)]]
        );
    }

    #[test]
    fn grouped_stream_agg_carries_groups_across_chunk_boundaries() {
        let rows: Vec<(i64, Option<i64>)> = (0..3000).map(|i| (i / 700, Some(i))).collect();
        let mut exec = grouped(
            col(0),
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Max, Some(col(1))),
            ],
            &rows,
        );
        let expected: Vec<Vec<Option<i64>>> = (0..5)
            .map(|g| {
                let count = if g == 4 { 200 } else { 700 };
                vec![Some(g), Some(count), Some(g * 700 + count - 1)]
            })
            .collect();
        assert_eq!(drain_grouped(&mut exec, 2), expected);
        let mut exec = grouped(
            col(0),
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Max, Some(col(1))),
            ],
            &rows,
        );
        assert_eq!(drain_grouped(&mut exec, 1024), expected);
    }

    /// NULL keys form one group (Go's `ETInt` arm: two NULLs are the same
    /// group, a NULL and a value are not), across a chunk boundary too.
    #[test]
    fn grouped_stream_agg_groups_null_keys_together() {
        let mut rows: Vec<(i64, Option<i64>)> = (0..1500).map(|i| (i, None)).collect();
        rows.extend((1500..3000).map(|i| (i, Some(7))));
        rows.extend((3000..3002).map(|i| (i, Some(8))));
        let mut exec = grouped(
            col(1),
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(1))),
                AggFunc::new(AggKind::Count, Some(col(0))),
            ],
            &rows,
        );
        assert_eq!(
            drain_grouped(&mut exec, 3),
            vec![
                vec![None, Some(1500)],
                vec![Some(7), Some(1500)],
                vec![Some(8), Some(2)]
            ]
        );
    }

    /// Expression keys use the same typed comparison as bare columns.
    #[test]
    fn grouped_stream_agg_resolves_expression_keys() {
        let rows: Vec<(i64, Option<i64>)> = (0..2500).map(|i| (i / 1000, Some(1))).collect();
        let key = Expression::ScalarFunction(tidb_expr::expression::ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            long(),
            vec![
                col(0),
                Expression::Constant(tidb_expr::constant::Constant::new(Datum::Int(10), long())),
            ],
        ));
        let mut exec = grouped(
            key,
            vec![
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Min, Some(col(0))),
            ],
            &rows,
        );
        assert_eq!(
            drain_grouped(&mut exec, 4),
            vec![
                vec![Some(1000), Some(0)],
                vec![Some(1000), Some(1)],
                vec![Some(500), Some(2)]
            ]
        );
    }

    #[test]
    fn integer_count_agg_uses_parallel_worker_window() {
        let rows: Vec<(i64, Option<i64>)> = (0..10_000).map(|value| (value, Some(1))).collect();
        let mut exec = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
            ],
            source(&rows),
            NoColumns,
            StatementMemory::default(),
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut output = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for row in 0..req.num_rows() {
                output.push((req.get_row(row).get_int64(0), req.get_row(row).get_int64(1)));
            }
        }
        assert_eq!(output.len(), rows.len());
        let (_, _, dispatched, workers) = exec.pipeline_run_info().expect("pipeline ran");
        assert!(dispatched > 0);
        assert!(workers > 1);
        exec.close().unwrap();
    }

    #[test]
    fn final_decimal_avg_uses_parallel_worker_window() {
        let decimal_input = decimal_with_shape(20, 2);
        let decimal_output = decimal_with_shape(24, 6);
        let mut avg = AggFunc::new(AggKind::Avg, Some(typed_col(1, long())));
        avg.extra_args.push(typed_col(2, decimal_input));
        let output_types = [decimal_output, long()];
        let mut exec = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![typed_col(0, long())],
            vec![avg, AggFunc::new(AggKind::FirstRow, Some(col(0)))],
            final_decimal_avg_source(10_000),
            NoColumns,
            StatementMemory::default(),
        );

        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut group_ids = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for row_index in 0..req.num_rows() {
                let row = req.get_row(row_index);
                assert_eq!(
                    String::from_utf8(row.get_my_decimal(0).to_string_bytes()).unwrap(),
                    "2.333333"
                );
                group_ids.push(row.get_int64(1));
            }
        }
        group_ids.sort_unstable();
        assert_eq!(group_ids, (0..10_000).collect::<Vec<_>>());
        let (_, _, dispatched, workers) = exec.pipeline_run_info().expect("pipeline ran");
        assert!(dispatched > 0);
        assert!(workers > 1);
        let (partial, _, _, _) = exec.pipeline_run_info().expect("pipeline ran");
        assert_eq!(exec.pipeline_input_chunks(), Some(2 * partial));
        exec.close().unwrap();
    }

    /// A source that delivers one chunk and, while doing so, raises the
    /// query's memory kill signal: all queued and working chunk owners must
    /// release their tracker charges when the pipeline drains.
    struct KillAfterFirstChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
        killer: Arc<tidb_sqlkiller::SqlKiller>,
    }
    impl Executor for KillAfterFirstChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
                self.killer
                    .send_kill_signal(tidb_sqlkiller::KillSignal::QueryMemoryExceeded);
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
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

    #[test]
    fn cancellation_releases_reusable_input_chunk_charges() {
        let field_type = binary_varchar();
        // Values well past the variable-length column's per-element estimate,
        // so filling the source's chunk GROWS it: the charge the fetcher
        // updates include allocation growth as well as the initial buffers.
        let values: Vec<Vec<u8>> = (0..64u8).map(|i| vec![b'a' + i % 2; 40]).collect();
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&field_type), values.len());
        for value in &values {
            data.append_bytes(0, value);
        }
        {
            let mut probe =
                Chunk::new_with_capacity(std::slice::from_ref(&field_type), values.len());
            let before = probe.memory_usage();
            for r in 0..data.num_rows() {
                probe.append_row(data.get_row(r));
            }
            assert!(
                probe.memory_usage() > before,
                "the input chunk must grow for the release to be observable"
            );
        }
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        let memory = StatementMemory::new(1 << 20, tidb_vardef::OomAction::Cancel, 45);
        let killer = Arc::clone(memory.sql_killer());
        let mut first_row = AggFunc::new(AggKind::FirstRow, Some(typed_col(0, field_type.clone())));
        first_row.distinct = true;
        let output_types = [field_type.clone(), long()];
        let mut exec = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![typed_col(0, field_type)],
            vec![first_row, AggFunc::new(AggKind::Count, None)],
            Box::new(KillAfterFirstChunkSource {
                meta: ExecutorMeta::new(Schema::new(vec![column]), 0, values.len(), 1024),
                data: Some(data),
                killer,
            }),
            NoColumns,
            memory,
        );
        let mut output = exec.new_chunk();
        let outcome = exec.open().and_then(|()| exec.next(&mut output));
        assert!(
            matches!(outcome, Err(ExecError::MemoryExceedForQuery { .. })),
            "the kill signal raised while filling the first input must surface: {outcome:?}"
        );
        // All input and work buffers are released before Close detaches the tracker.
        assert_eq!(
            exec.tracker.bytes_consumed(),
            0,
            "all reusable chunk charges must be released"
        );
        exec.close().unwrap();
    }

    #[test]
    fn integer_count_agg_keeps_all_null_group() {
        let rows = vec![(0, None), (2, Some(7)), (0, None)];
        let agg = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
            ],
            source(&rows),
            NoColumns,
            StatementMemory::default(),
        );
        assert_rows_unordered(
            run(agg),
            vec![
                vec![Datum::Int(0), Datum::Int(0)],
                vec![Datum::Int(2), Datum::Int(1)],
            ],
        );
    }

    #[test]
    fn nested_integer_count_agg_keeps_zero_count_groups() {
        let inner = HashAggExec::new(
            out_meta(1),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            source(&[(0, None), (1, Some(7)), (2, None), (1, Some(8))]),
            NoColumns,
            StatementMemory::default(),
        );
        let outer = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::Count, None),
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
            ],
            Box::new(inner),
            NoColumns,
            StatementMemory::default(),
        );
        assert_rows_unordered(
            run(outer),
            vec![
                vec![Datum::Int(2), Datum::Int(0)],
                vec![Datum::Int(1), Datum::Int(2)],
            ],
        );
    }

    #[test]
    fn nested_integer_count_agg_keeps_late_zero_count_groups_across_windows() {
        let rows = (0..150_000i64)
            .map(|key| (key, (key < 100_000).then_some(1)))
            .collect::<Vec<_>>();
        let inner = HashAggExec::new(
            out_meta(1),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            source(&rows),
            NoColumns,
            StatementMemory::default(),
        );
        let outer = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::Count, None),
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
            ],
            Box::new(inner),
            NoColumns,
            StatementMemory::default(),
        );
        // Go's map order is unspecified, so the groups come back in any order.
        let mut got = run(outer);
        got.sort_by_key(|row| match row[1] {
            Datum::Int(value) => std::cmp::Reverse(value),
            _ => std::cmp::Reverse(i64::MIN),
        });
        assert_eq!(
            got,
            vec![
                vec![Datum::Int(100_000), Datum::Int(1)],
                vec![Datum::Int(50_000), Datum::Int(0)]
            ]
        );
    }

    /// Go divides AVG's exact sum by the count with div_precision_increment,
    /// so an integer average carries four fraction digits. The expectations
    /// are `types.DecimalDiv(sum, count, _, 4)` output from the Go
    /// implementation in this repository.
    #[test]
    fn avg_over_integers_is_decimal_scaled_by_the_precision_increment() {
        // Go `typeInfer4Avg` gives AVG(BIGINT) a DECIMAL return scale equal to
        // div_precision_increment, which is four in this test context.
        let types = [decimal_with_shape(15, 4)];
        for (values, want) in [
            (vec![1i64, 2, 3], "2.0000"),
            (vec![1, 2, 4], "2.3333"),
            (vec![1, 0, 0], "0.3333"),
        ] {
            let rows: Vec<(i64, Option<i64>)> = values.iter().map(|v| (1, Some(*v))).collect();
            let agg = HashAggExec::new(
                out_meta_typed(&types),
                vec![],
                vec![AggFunc::new(AggKind::Avg, Some(col(1)))],
                source(&rows),
                NoColumns,
                StatementMemory::default(),
            );
            assert_eq!(
                run_typed(agg, &types),
                vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
                    want
                ))]],
                "AVG of {values:?}"
            );
        }
    }

    #[test]
    fn avg_over_fixed_scale_decimals_uses_the_go_return_scale_and_skips_nulls() {
        let input_type = decimal_with_shape(15, 2);
        let output_type = decimal_with_shape(19, 6);
        let agg = HashAggExec::new(
            out_meta_typed(std::slice::from_ref(&output_type)),
            vec![],
            vec![AggFunc::new(
                AggKind::Avg,
                Some(typed_col(0, input_type.clone())),
            )],
            decimal_source(
                &[Some("1.00"), None, Some("2.00"), Some("4.00")],
                &input_type,
            ),
            NoColumns,
            StatementMemory::default(),
        );

        let result = run_typed(agg, std::slice::from_ref(&output_type));
        let Datum::Decimal(result) = &result[0][0] else {
            panic!("AVG(DECIMAL) must return DECIMAL");
        };
        assert_eq!(result.to_string(), "2.333333");
        assert_eq!(result.storage_string(), "2.333333");
        assert_eq!(result.declared_shape(), Some((19, 6)));
    }

    #[test]
    fn avg_of_an_all_null_group_is_null() {
        let types = [decimal()];
        let agg = HashAggExec::new(
            out_meta_typed(&types),
            vec![],
            vec![AggFunc::new(AggKind::Avg, Some(col(1)))],
            source(&[(1, None)]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run_typed(agg, &types), vec![vec![Datum::Null]]);
    }

    /// DISTINCT folds repeated inputs once per group, and the de-duplication
    /// is per group, not global.
    #[test]
    fn distinct_folds_repeats_within_each_group() {
        let mut count = AggFunc::new(AggKind::Count, Some(col(1)));
        count.distinct = true;
        let mut sum = AggFunc::new(AggKind::Sum, Some(col(1)));
        sum.distinct = true;
        // SUM lands in a decimal cell, which is the domain Go sums in.
        let agg = HashAggExec::new(
            out_meta_typed(&[long(), long(), decimal()]),
            vec![col(0)],
            vec![AggFunc::new(AggKind::FirstRow, Some(col(0))), count, sum],
            source(&[
                (1, Some(5)),
                (1, Some(5)),
                (1, Some(7)),
                (1, None),
                (2, Some(5)),
            ]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_rows_unordered(
            run_typed(agg, &[long(), long(), decimal()]),
            vec![
                // Group 1 sees 5,5,7,NULL: two distinct non-NULL values.
                vec![
                    Datum::Int(1),
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(12)),
                ],
                // Group 2's own 5 is not folded into group 1's.
                vec![
                    Datum::Int(2),
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(5)),
                ],
            ],
        );
    }

    /// The coefficient accumulator must publish exactly what the exact
    /// `Decimal` chain publishes -- value AND visible scale -- for every input
    /// shape that can seed it, and must hand back the shapes it cannot take.
    #[test]
    fn a_sum_folds_the_same_whether_it_seeds_fast_or_exact() {
        fn exact(inputs: &[Datum]) -> Option<Decimal> {
            let mut acc: Option<Decimal> = None;
            for input in inputs {
                let addend = match input {
                    Datum::Int(v) => Decimal::from_int(*v),
                    Datum::UInt(v) => Decimal::from_uint(*v),
                    Datum::Decimal(d) => d.clone(),
                    other => panic!("unexpected SUM input {other:?}"),
                };
                acc = Some(match acc.take() {
                    Some(sum) => sum.add(&addend),
                    None => addend,
                });
            }
            acc
        }

        let decimal = |text: &str| Datum::Decimal(Decimal::parse_mysql(text).0);
        // The last case exceeds i128 twice over, so it both declines the seed
        // and, once seeded by its smaller neighbours, overflows the fold and
        // has to materialize and replay.
        let cases: Vec<Vec<Datum>> = vec![
            vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)],
            vec![Datum::Int(-5), Datum::Int(5), Datum::Int(-7)],
            vec![Datum::UInt(u64::MAX), Datum::UInt(u64::MAX)],
            vec![Datum::Int(i64::MIN), Datum::Int(i64::MAX)],
            vec![decimal("1.25"), decimal("2.75"), decimal("-4.00")],
            vec![decimal("0.001"), decimal("0.002")],
            // Mixed scales: the fold declines a differing scale and the exact
            // path takes the row.
            vec![decimal("1.5"), decimal("2.25"), Datum::Int(3)],
            vec![
                decimal("170141183460469231731687303715884105727"),
                decimal("170141183460469231731687303715884105727"),
            ],
        ];

        for inputs in cases {
            let mut state = AggState::new(&AggFunc::new(AggKind::Sum, None));
            for input in &inputs {
                state
                    .update(Some(input.clone()), &[], Vec::new(), None)
                    .expect("SUM accepts the input");
            }
            let folded = state.partial.finish(&[], 4).expect("SUM finishes");
            let expected = exact(&inputs).expect("a non-empty group sums");
            let Datum::Decimal(folded) = folded else {
                panic!("SUM must finish as a DECIMAL, got {folded:?}");
            };
            assert_eq!(
                folded.to_string(),
                expected.to_string(),
                "SUM over {inputs:?} must publish the exact chain's value and scale"
            );
        }

        // An empty group is still NULL, and a NULL input still contributes
        // nothing -- neither may be swallowed by the seeding arm.
        let mut state = AggState::new(&AggFunc::new(AggKind::Sum, None));
        assert!(matches!(
            state.partial.finish(&[], 4).expect("an empty SUM finishes"),
            Datum::Null
        ));
        state
            .update(Some(Datum::Null), &[], Vec::new(), None)
            .expect("SUM accepts NULL");
        assert!(matches!(
            state
                .partial
                .finish(&[], 4)
                .expect("an all-NULL SUM finishes"),
            Datum::Null
        ));
    }

    /// The FINAL COUNT cell read must add exactly what the datum path adds,
    /// for every shape the column can take.
    #[test]
    fn a_final_count_adds_the_same_cell_the_datum_path_would() {
        use tidb_datatype::{FieldType, FieldTypeCode};

        let signed = FieldType::new(FieldTypeCode::LongLong);
        let unsigned = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        let text = FieldType::new(FieldTypeCode::Varchar);

        for (label, field_type, values) in [
            ("signed", signed.clone(), vec![Some(3_i64), None, Some(4)]),
            ("unsigned", unsigned.clone(), vec![Some(7_i64), Some(0)]),
            ("unsigned overflow", unsigned.clone(), vec![Some(-1_i64)]),
            ("all null", signed.clone(), vec![None, None]),
        ] {
            let mut chunk = Chunk::new_with_capacity(&[field_type.clone()], values.len());
            for value in &values {
                match value {
                    Some(value) => chunk.append_int64(0, *value),
                    None => chunk.append_null(0),
                }
            }

            let func = AggFunc::new(AggKind::FinalCount, Some(typed_col(0, field_type.clone())));
            let mut fast = AggState::new(&func);
            let mut exact = AggState::new(&func);
            let mode = AggInputMode::new(&func);
            assert!(matches!(mode, AggInputMode::FinalCount { .. }));
            // Selection order and repeated physical rows must not become
            // indices into the packed column themselves.
            chunk.set_sel(Some(vec![values.len() - 1, 0, values.len() - 1]));
            let bound = mode.bind(&chunk);
            for index in 0..chunk.num_rows() {
                let row = chunk.get_row(index);
                let actual = bound.update(&func, &NoColumns, &mut fast, row);
                // The datum path, as `Partial::update` sees it.
                let datum = if row.is_null(0) {
                    Datum::Null
                } else if field_type.is_unsigned() {
                    Datum::UInt(row.get_int64(0) as u64)
                } else {
                    Datum::Int(row.get_int64(0))
                };
                let expected = exact.update(Some(datum), &[], Vec::new(), None);
                assert_eq!(
                    actual.map_err(|error| format!("{error:?}")),
                    expected.map_err(|error| format!("{error:?}")),
                    "{label}: cell updates and overflow errors must agree"
                );
            }
            assert_eq!(
                fast.partial.finish(&[], 4).unwrap(),
                exact.partial.finish(&[], 4).unwrap(),
                "{label}: the two paths must agree"
            );
        }

        // A non-integer column keeps the complete path, so the unsupported
        // partial-result error is still raised rather than silently skipped.
        let func = AggFunc::new(AggKind::FinalCount, Some(typed_col(0, text.clone())));
        let mut chunk = Chunk::new_with_capacity(&[text], 1);
        chunk.append_bytes(0, b"3");
        assert!(
            matches!(AggInputMode::new(&func), AggInputMode::Expression),
            "a non-integer partial count must not be read as a cell"
        );
    }

    #[test]
    fn distinct_memory_delta_includes_value_set_growth() {
        let mut count = AggFunc::new(AggKind::Count, None);
        count.distinct = true;
        let mut state = AggState::new(&count);
        let mut retained_key_bytes = 0_i64;
        let mut total_delta = 0_i64;
        for byte in 0..128_u8 {
            let value = Datum::Bytes(vec![byte; 32]);
            retained_key_bytes +=
                i64::try_from(group_key_part(&tidb_charset::Collation::DEFAULT, &value).len())
                    .unwrap();
            total_delta += state.update(Some(value), &[], Vec::new(), None).unwrap();
        }
        assert!(
            total_delta > retained_key_bytes,
            "DISTINCT must charge both retained keys and value-set table growth"
        );
        assert_eq!(
            state
                .update(Some(Datum::Bytes(vec![127; 32])), &[], Vec::new(), None)
                .unwrap(),
            0,
            "a duplicate grows neither the retained payload nor the table"
        );
    }

    fn run(mut exec: HashAggExec<NoColumns>) -> Vec<Vec<Datum>> {
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut out = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..req.num_cols())
                        .map(|c| row.get_datum(c, &long()))
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        out
    }

    #[test]
    fn global_count_and_sum_with_nulls() {
        // Values: 10, NULL, 30 -> COUNT(v)=2 (NULL skipped), COUNT(*)=3, SUM=40.
        let agg = HashAggExec::new(
            out_meta_typed(&[long(), long(), decimal()]),
            vec![],
            vec![
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Count, None),
                AggFunc::new(AggKind::Sum, Some(col(1))),
            ],
            source(&[(1, Some(10)), (1, None), (1, Some(30))]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            run_typed(agg, &[long(), long(), decimal()]),
            vec![vec![
                Datum::Int(2),
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(40))
            ]]
        );
    }

    #[test]
    fn parallel_group_by_emits_every_group_without_an_order_contract() {
        // Go's parallel HashAgg iterates final-worker maps; FIRST_ROW carries
        // the key, but the groups themselves are unordered.
        let agg = HashAggExec::new(
            out_meta_typed(&[long(), decimal()]),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Sum, Some(col(1))),
            ],
            source(&[(2, Some(5)), (1, Some(7)), (2, Some(6))]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_rows_unordered(
            run_typed(agg, &[long(), decimal()]),
            vec![
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(11)),
                ],
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(7)),
                ],
            ],
        );
    }

    #[test]
    fn empty_input_rules() {
        // No group-by + no data: one row, COUNT 0, SUM NULL.
        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Sum, Some(col(1))),
            ],
            source(&[]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(agg), vec![vec![Datum::Int(0), Datum::Null]]);

        // Group-by + no data: empty result.
        let agg = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
            ],
            source(&[]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(agg), Vec::<Vec<Datum>>::new());
    }

    #[test]
    fn group_concat_stringifies_temporal_duration_and_json_values() {
        let time = Datum::Time(
            tidb_datatype::Time::new(
                tidb_datatype::CoreTime::from_date(2020, 1, 2, 3, 4, 5, 123_456),
                tidb_datatype::TimeType::DateTime,
                6,
            )
            .unwrap(),
        );
        let duration = Datum::Duration(
            tidb_datatype::MySqlDuration::from_nanoseconds(
                (3_600 + 2 * 60 + 3) * 1_000_000_000 + 400_000_000,
                6,
            )
            .unwrap(),
        );
        let json = Datum::Json(
            tidb_datatype::BinaryJSON::from_typed_value(&tidb_datatype::BinaryJSONValue::Object(
                std::collections::BTreeMap::from([(
                    "a".to_owned(),
                    tidb_datatype::BinaryJSONValue::Int64(1),
                )]),
            ))
            .unwrap(),
        );
        let bit = Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![5]));

        for value in [time, duration, json] {
            assert_eq!(
                group_concat_bytes(&value).unwrap(),
                value.sql_bytes().unwrap()
            );
        }
        assert_eq!(group_concat_bytes(&bit).unwrap(), b"0x05");
    }

    #[test]
    fn approx_percentile_uses_ordinal_selection() {
        let percentile = Partial::ApproxPercentile {
            values: vec![Datum::Int(9), Datum::Int(1), Datum::Int(5), Datum::Int(3)],
            percent: Some(50),
        };
        assert_eq!(percentile.finish(&[], 4).unwrap(), Datum::Int(3));

        let maximum = Partial::ApproxPercentile {
            values: vec![Datum::Int(2), Datum::Int(8), Datum::Int(4)],
            percent: Some(100),
        };
        assert_eq!(maximum.finish(&[], 4).unwrap(), Datum::Int(8));

        let empty = Partial::ApproxPercentile {
            values: Vec::new(),
            percent: Some(50),
        };
        assert_eq!(empty.finish(&[], 4).unwrap(), Datum::Null);

        // Go buildApproxPercentile ignores HasDistinct, including when the
        // caller constructs the typed descriptor directly instead of SQL.
        for distinct in [false, true] {
            let descriptor = tidb_expression_aggregation::AggFuncDesc::new(
                &NoColumns,
                "approx_percentile",
                vec![
                    col(1),
                    Expression::Constant(tidb_expr::constant::Constant::new(
                        Datum::Int(75),
                        long(),
                    )),
                ],
                distinct,
            )
            .unwrap();
            let function = AggFunc::from_descriptor(&descriptor, &NoColumns).unwrap();
            let exec = HashAggExec::new(
                out_meta(1),
                vec![],
                vec![function],
                source(&[(1, Some(1)), (1, Some(1)), (1, Some(1)), (1, Some(9))]),
                NoColumns,
                StatementMemory::default(),
            );
            assert_eq!(run(exec), vec![vec![Datum::Int(1)]], "DISTINCT={distinct}");
        }
    }

    /// `JSON_ARRAYAGG`/`JSON_OBJECTAGG` over a BINARY-charset value: `Opaque`
    /// wrapping tagged with the source column's own MySQL type code, and a
    /// BINARY-charset key failing with 3144. Every expected JSON text is
    /// captured verbatim from a real TiDB server (`zz_dump_opaque_test.go`,
    /// `TestZZDumpOpaque`).
    mod json_agg_opaque {
        use super::*;
        use tidb_datatype::FieldTypeCode;

        fn varbinary() -> FieldType {
            FieldType::new(FieldTypeCode::Varchar).with_collation(tidb_charset::Collation::Binary)
        }

        /// `SELECT JSON_ARRAYAGG(c_varbin) FROM t` over one row holding
        /// `"ab"` = `["base64:type15:YWI="]`.
        #[test]
        fn json_arrayagg_wraps_binary_charset_value_as_opaque() {
            let mut partial = Partial::JsonArrayAgg(Vec::new(), Box::new(varbinary()));
            partial
                .update(
                    Some(Datum::new_bytes(*b"ab")),
                    &[],
                    Vec::new(),
                    tidb_charset::Collation::DEFAULT,
                )
                .unwrap();
            let result = partial.finish(&[], 4).unwrap();
            let Datum::Json(json) = result else {
                panic!("expected a JSON datum, got {result:?}");
            };
            assert_eq!(json.to_string(), r#"["base64:type15:YWI="]"#);
        }

        /// `SELECT JSON_OBJECTAGG('k', c_varbin) FROM t` over one row
        /// holding `"ab"` = `{"k": "base64:type15:YWI="}`.
        #[test]
        fn json_objectagg_wraps_binary_charset_value_as_opaque() {
            let mut partial = Partial::JsonObjectAgg(BTreeMap::new(), Box::new(varbinary()), false);
            partial
                .update(
                    Some(Datum::new_string("k")),
                    &[Datum::new_bytes(*b"ab")],
                    Vec::new(),
                    tidb_charset::Collation::DEFAULT,
                )
                .unwrap();
            let result = partial.finish(&[], 4).unwrap();
            let Datum::Json(json) = result else {
                panic!("expected a JSON datum, got {result:?}");
            };
            assert_eq!(json.to_string(), r#"{"k": "base64:type15:YWI="}"#);
        }

        /// `SELECT JSON_OBJECTAGG(c_varbin, 1) FROM t`: a BINARY-charset KEY
        /// fails with 3144, captured message `Cannot create a JSON value
        /// from a string with CHARACTER SET 'binary'.`.
        #[test]
        fn json_objectagg_binary_charset_key_is_error_3144() {
            let mut partial = Partial::JsonObjectAgg(BTreeMap::new(), Box::new(long()), true);
            let err = partial
                .update(
                    Some(Datum::new_bytes(*b"ab")),
                    &[Datum::Int(1)],
                    Vec::new(),
                    tidb_charset::Collation::DEFAULT,
                )
                .unwrap_err();
            assert!(matches!(
                err,
                ExecError::Mysql(ref error)
                    if error.equal(&tidb_datatype::types_errors::ERR_INVALID_JSON_CHARSET)
                        && error.message.contains("'binary'")
            ));
        }
    }

    /// `APPROX_COUNT_DISTINCT` argument-encoding tests: golden counts
    /// captured from Go (`TestZZDumpApxEnc`,
    /// `pkg/executor/zz_dump_apxenc_test.go`) that exercise the TIME,
    /// DURATION, and JSON encodings ported into `approx_count_distinct_encode`.
    mod approx_count_distinct_encoding {
        use super::*;
        use tidb_datatype::{CoreTime, MySqlDuration, Time, TimeType};

        /// Feeds `values` through `approx_count_distinct_encode` and the
        /// BJKST sketch exactly as `Partial::ApproxCountDistinct` does, and
        /// returns the resulting distinct count.
        fn distinct_count(values: &[Datum]) -> u64 {
            let mut sketch = ApproxCountDistinctSketch::new();
            for value in values {
                let encoded = approx_count_distinct_encode(value, Collation::Binary).unwrap();
                sketch.insert(&encoded);
            }
            sketch.fixed_size()
        }

        fn date(y: u16, m: u8, d: u8) -> Datum {
            let core = CoreTime::from_date(y, m, d, 0, 0, 0, 0);
            Datum::Time(Time::new(core, TimeType::Date, 0).unwrap())
        }

        fn datetime_micros(base_micros: u32, offset: u32) -> Datum {
            // 2000-01-01 00:00:00 plus `offset` microseconds, matching the
            // Go capture's `date_add('2000-01-01 00:00:00', interval i
            // microsecond)` -- carried by hand into hour/minute/second so
            // CoreTime's fields never overflow their bit width.
            let total = base_micros as u64 + offset as u64;
            let micros = (total % 1_000_000) as u32;
            let total_seconds = total / 1_000_000;
            let second = (total_seconds % 60) as u8;
            let total_minutes = total_seconds / 60;
            let minute = (total_minutes % 60) as u8;
            let total_hours = total_minutes / 60;
            let hour = (total_hours % 24) as u8;
            let day = 1 + (total_hours / 24) as u8;
            let core = CoreTime::from_date(2000, 1, day, hour, minute, second, micros);
            Datum::Time(Time::new(core, TimeType::DateTime, 6).unwrap())
        }

        fn duration(nanoseconds: i64, fsp: i64) -> Datum {
            Datum::Duration(MySqlDuration::from_nanoseconds(nanoseconds, fsp).unwrap())
        }

        fn json(value: &BinaryJSONValue) -> Datum {
            Datum::Json(BinaryJSON::from_typed_value(value).unwrap())
        }

        #[test]
        fn string_encoding_uses_the_expression_collation() {
            let values = [
                Datum::String(tidb_datatype::StringDatum::new(
                    b"a".to_vec(),
                    Collation::Utf8Mb4GeneralCi,
                )),
                Datum::String(tidb_datatype::StringDatum::new(
                    b"A".to_vec(),
                    Collation::Utf8Mb4GeneralCi,
                )),
            ];
            let mut sketch = ApproxCountDistinctSketch::new();
            for value in &values {
                sketch.insert(
                    &approx_count_distinct_encode(value, Collation::Utf8Mb4GeneralCi).unwrap(),
                );
            }
            assert_eq!(sketch.fixed_size(), 1);
        }

        // Go: `insert into t_date values (1,'2020-01-01'),(2,'2020-01-01'),
        // (3,'2020-01-02')` -> ZZDUMP date_small = 2.
        #[test]
        fn date_dedup_matches_go() {
            let values = [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 2)];
            assert_eq!(distinct_count(&values), 2);
        }

        // Go: `insert into t_dur values (1,'01:02:03.400000'),
        // (2,'01:02:03.400000'),(3,'11:22:33.000001')` -> ZZDUMP
        // dur_small = 2.
        #[test]
        fn duration_dedup_matches_go() {
            let a = (3600 + 2 * 60 + 3) as i64 * 1_000_000_000 + 400_000_000;
            let b = (11 * 3600 + 22 * 60 + 33) as i64 * 1_000_000_000 + 1_000;
            let values = [duration(a, 6), duration(a, 6), duration(b, 6)];
            assert_eq!(distinct_count(&values), 2);
        }

        // Go: `insert into t_json values (1,'3'),(2,'3.0'),(3,'[1,2]'),
        // (4,'[1,2]'),(5,'{"a":1,"b":2}')` -> ZZDUMP json_small = 3 (the
        // integer and the equal-valued double collide, the array pair
        // collides, the object is its own distinct value).
        #[test]
        fn json_dedup_matches_go() {
            let array = || {
                BinaryJSONValue::Array(vec![BinaryJSONValue::Int64(1), BinaryJSONValue::Int64(2)])
            };
            let object = BinaryJSONValue::Object(BTreeMap::from([
                ("a".to_owned(), BinaryJSONValue::Int64(1)),
                ("b".to_owned(), BinaryJSONValue::Int64(2)),
            ]));
            let values = [
                json(&BinaryJSONValue::Int64(3)),
                json(&BinaryJSONValue::Float64(3.0)),
                json(&array()),
                json(&array()),
                json(&object),
            ];
            assert_eq!(distinct_count(&values), 3);
        }

        // Go: `insert into t_dt` with 75000 rows of
        // `date_add('2000-01-01 00:00:00', interval i microsecond)` for
        // `i` in `0..75000` -> ZZDUMP dt_large = 74710 (the BJKST sketch's
        // extrapolated estimate once the exact-count threshold is
        // exceeded). This is the encoding this module ports: the 16-byte
        // `appendTime` layout keeps the sketch byte-identical to Go.
        #[test]
        fn datetime_large_cardinality_matches_go_estimate() {
            let n: u32 = 75_000;
            let values: Vec<Datum> = (0..n).map(|i| datetime_micros(0, i)).collect();
            assert_eq!(distinct_count(&values), 74_710);
        }

        // A mixed-type multi-argument tuple: Go encodes each argument in
        // turn into one shared byte buffer per row before hashing, so two
        // rows with the same (INT, DATETIME, JSON) triple must encode to
        // the same bytes.
        //
        // Go: `insert into t_mixed values (1,1,'2020-01-01 00:00:00','1'),
        // (2,1,'2020-01-01 00:00:00','1'),(3,2,'2020-01-01 00:00:00','1')`
        // -> ZZDUMP mixed_small = 2.
        #[test]
        fn mixed_tuple_dedup_matches_go() {
            fn tuple_bytes(a: i64, b: Datum, c: Datum) -> Vec<u8> {
                let mut encoded =
                    approx_count_distinct_encode(&Datum::Int(a), Collation::Binary).unwrap();
                encoded.extend(approx_count_distinct_encode(&b, Collation::Binary).unwrap());
                encoded.extend(approx_count_distinct_encode(&c, Collation::Binary).unwrap());
                encoded
            }
            let dt = || datetime_micros(0, 0);
            let mut sketch = ApproxCountDistinctSketch::new();
            for row in [
                tuple_bytes(1, dt(), json(&BinaryJSONValue::Int64(1))),
                tuple_bytes(1, dt(), json(&BinaryJSONValue::Int64(1))),
                tuple_bytes(2, dt(), json(&BinaryJSONValue::Int64(1))),
            ] {
                sketch.insert(&row);
            }
            assert_eq!(sketch.fixed_size(), 2);
        }
    }
}

#[cfg(test)]
pub mod test_temp_storage {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use tidb_spill_storage::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

    /// A fresh scratch directory named after the test.
    pub fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tidb_rust_exec_spill_test_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch temp dir");
        dir
    }

    /// Opens one immutable authority over `path` for the test statement.
    pub fn storage(path: &Path) -> Arc<SpillStorage> {
        Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: path.to_owned(),
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("test spill storage"),
        )
    }
}
