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

//! `pkg/executor/join/index_lookup_join.go`: `IndexLookUpJoin` -- the
//! index-nested-loop join.
//!
//! # The algorithm, and where it lives
//!
//! Go's own doc comment (`index_lookup_join.go:51`) states the contract this
//! file has to preserve: *"It preserves the order of the outer table and
//! supports batch lookup."* Everything below is arranged around keeping that
//! true.
//!
//! One round of the join is a `lookUpJoinTask` (`:126`) and has four stages:
//!
//! 1. **collect a batch of outer rows** -- `outerWorker.buildTask` (`:439`),
//!    with the doubling batch size of `increaseBatchSize` (`:512`) and the
//!    outer-side `Filter` evaluated up front into `task.outerMatch`;
//! 2. **turn those rows into lookup keys** -- `innerWorker.constructLookupContent`
//!    (`:582`) over `constructDatumLookupKey` (`:654`), then
//!    `sortAndDedupLookUpContents` (`:703`);
//! 3. **read the inner side once for the whole batch** --
//!    `innerWorker.fetchInnerResults` (`:739`), which is the actual
//!    index-nested-loop: one index read serving N outer rows;
//! 4. **hash the inner rows and probe** -- `buildLookUpMap` (`:800`) plus
//!    `lookUpMatchedInners` (`:373`) and the probe loop in `Next` (`:283`).
//!
//! Stages 1, 2 and 4 are ported here in full. Stage 3 is a boundary: Go reaches
//! it through the `IndexJoinExecutorBuilder` interface (`:104`), which exists
//! in Go *"to avoid cycle import"* -- the inner reader is built by
//! `pkg/executor/builder.go`, not by this file. That interface is reproduced
//! here as [`IndexJoinExecutorBuilder`] for exactly the same reason, so this
//! module stays dependency-closed and the caller supplies the reader.
//!
//! # Reuse rather than restatement
//!
//! * [`crate::joiner::Joiner`] is Go's `Joiner`. Every per-join-type decision
//!   -- what an inner match emits, what a miss emits, whether one match
//!   settles the outer row -- is already written down there, once, across its
//!   nine strategies. This file makes exactly the same two calls Go's `Next`
//!   makes (`TryToMatchInners`, `OnMissMatch`) and derives nothing about join
//!   semantics itself.
//! * `tidb_util::mvmap::MVMap` is Go's `mvmap.MVMap`, already ported, already
//!   insertion-ordered on `get`. That ordering is load-bearing: it is what
//!   makes the inner rows of one outer key come out in inner-scan order.
//! * [`tidb_chunk::list::List`] is Go's `chunk.List`, and
//!   [`tidb_chunk::list::RowPtr`] its address type, so `task.outerResult` /
//!   `task.innerResult` are ported as-is rather than re-modelled.
//! * `LendingIterator::row_ptrs` is Go's `chunk.NewIterator4RowPtr`; the probe
//!   hands one to the joiner instead of materialising `task.matchedInners` as
//!   a `[]chunk.Row` (Go's `NewIterator4Slice`). Same rows, same order, no
//!   borrow of the list per row.
//! * [`crate::kv_table::IndexRange`] is Go's `ranger.Range`.
//! * `crate::joiner::eval_bool` is Go's `expression.EvalBool`, used for the
//!   outer-side `Filter`.
//!
//! Range *construction* per outer row is deliberately NOT here and not in
//! [`crate::index_range`] either: in Go it is `buildRangesForIndexJoin` /
//! `buildExecutorForIndexJoinInternal` in `pkg/executor/builder.go`, reached
//! only through the builder interface. [`crate::index_range`] ports
//! `pkg/util/ranger`'s point algebra over a `WHERE`, which is a different
//! input; it has no per-outer-row entry point, and inventing one here would be
//! restating builder.go in the wrong file.
//!
//! # Sequential here, worker-parallel there
//!
//! Go runs one `outerWorker` goroutine and `IndexLookupJoinConcurrency()`
//! `innerWorker` goroutines. The outer worker builds tasks and pushes each one
//! to `innerCh` and then to `resultCh` (`outerWorker.run` :385); the inner
//! workers pull from `innerCh`, fill in `innerResult`/`lookupMap`, and signal
//! `task.doneCh`; the main thread pulls from `resultCh` and blocks on
//! `task.doneCh` (`getFinishedTask` :341).
//!
//! **The order guarantee, and how it was determined.** `resultCh` is a single
//! FIFO channel written by a *single* producer, so tasks reach the main thread
//! in exactly the order the outer worker built them -- which is outer-scan
//! order. The N inner workers race only over *filling in* a task, never over
//! *ordering* it, and `getFinishedTask` waits on the head task's `doneCh`
//! rather than taking whichever task finished first. Within a task, `Next`
//! walks `task.cursor` forward one outer row at a time. So outer order is
//! total and unconditional here. (Contrast the hash variant,
//! `index_lookup_hash_join.go`, which carries an explicit `KeepOuterOrder`
//! field precisely because it does *not* preserve order by construction --
//! that is the counterexample proving this file's guarantee is structural,
//! not incidental.)
//!
//! Because order is producer-serial, running the pipeline sequentially --
//! build one task, fill it, drain it, build the next -- emits **the same rows
//! in the same order**. What is genuinely lost is throughput: Go overlaps the
//! inner reads of task K+1..K+concurrency with the probe of task K, and
//! prefetches up to `concurrency` outer batches. Nothing observable at the SQL
//! level depends on that overlap.
//!
//! What *is* observably narrowed by going sequential:
//!
//! * Go's `IsOuterJoin` back-pressure (`buildTask` :453) reads
//!   `lookup.requiredRows`, which the *main* thread stores on each `Next`,
//!   while the outer worker may already be several batches ahead. Here the
//!   value is always the current `Next`'s, so the batch size tracks the
//!   consumer exactly instead of approximately. Row set and order are
//!   unchanged; only how many outer rows are read ahead of a `LIMIT` changes,
//!   and in the direction of reading fewer.
//! * `Finished`/`cancelFunc`/`WorkerWg` have no counterpart: there is no
//!   worker to cancel, and an error propagates by `?` out of the single
//!   thread instead of through `task.doneCh`. Go's panic-to-`doneCh` recovery
//!   in `outerWorker.run`/`innerWorker.run` is likewise unnecessary.
//!
//! # Narrowings (each named at its site as well)
//!
//! * Runtime stats (`indexLookUpJoinRuntimeStats` :860, `innerWorkerRuntimeStats`
//!   :866) are not ported -- `Executor` has no stats surface in this workspace.
//! * Memory tracking (`memory.Tracker` throughout) is dropped; there is no
//!   task-scoped tracker to attach to.
//! * `physicalop.ColWithCmpFuncManager` (`LastColHelper` / `nextColCompareFilters`)
//!   is a planner type; the sort/dedup tie-break it provides is a
//!   [`LastColComparator`] hook the caller may supply.
//! * `failpoint.Inject` sites and `logutil` calls are dropped.
//! * Go stores a `chunk.RowPtr` into an 8-byte value via `unsafe.Pointer`;
//!   here the same 8 bytes are written and read explicitly.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::iterator::LendingIterator;
use tidb_chunk::list::{List, RowPtr};
use tidb_chunk::row::{OwnedRow, Row};
use tidb_datatype::{Collation, ConversionFlags, Datum, FieldType, FieldTypeCode};
use tidb_expr::expression::Expression;
use tidb_expr::Columns;
use tidb_util::mvmap::MVMap;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::joiner::{eval_bool, Joiner, NAAJType};
use crate::kv_table::IndexRange;

/// Go `types.UnspecifiedLength`: a column length that imposes no prefix cut.
pub const UNSPECIFIED_LENGTH: i64 = -1;

/// Go `SessionVars.IndexJoinBatchSize` default (`tidb_index_join_batch_size`).
pub const DEFAULT_INDEX_JOIN_BATCH_SIZE: usize = 25000;

// ---------------------------------------------------------------------------
// OuterCtx (`index_lookup_join.go:94`) / InnerCtx (`:110`)
// ---------------------------------------------------------------------------

/// Go `OuterCtx` (`index_lookup_join.go:94`): the outer side's column layout
/// and its pre-lookup filter.
#[derive(Clone, Debug, Default)]
pub struct OuterCtx {
    /// Go `RowTypes`: the outer child's output column types.
    pub row_types: Vec<FieldType>,
    /// Go `KeyCols`: outer offsets of the index-key columns.
    pub key_cols: Vec<usize>,
    /// Go `HashTypes`: types of the hash-key columns.
    pub hash_types: Vec<FieldType>,
    /// Go `HashCols`: outer offsets of the hash-key columns.
    pub hash_cols: Vec<usize>,
    /// Go `Filter`: the CNF applied to the outer row before it is looked up.
    /// An outer row that fails it still reaches the joiner as a miss, so an
    /// outer join keeps emitting it -- see `constructDatumLookupKey` (`:655`),
    /// which returns a nil key rather than dropping the row.
    pub filter: Vec<Expression>,
}

/// Go `InnerCtx` (`index_lookup_join.go:110`), minus `ReaderBuilder`, which is
/// passed separately here because it is the one non-`Clone` member.
#[derive(Clone, Debug, Default)]
pub struct InnerCtx {
    /// Go `RowTypes`: the inner reader's output column types.
    pub row_types: Vec<FieldType>,
    /// Go `KeyCols`: inner offsets of the index-key columns.
    pub key_cols: Vec<usize>,
    /// Go `KeyColIDs`: the key columns' original table IDs, carried through to
    /// the reader builder for dynamic partition pruning.
    pub key_col_ids: Vec<i64>,
    /// Go `KeyCollators`, aligned with `key_cols`.
    pub key_collators: Vec<Collation>,
    /// Go `HashTypes`.
    pub hash_types: Vec<FieldType>,
    /// Go `HashCols`: inner offsets of the hash-key columns.
    pub hash_cols: Vec<usize>,
    /// Go `HashCollators`, aligned with `hash_cols`.
    pub hash_collators: Vec<Collation>,
    /// Go `HashIsNullEQ`: which hash keys came from `<=>` and therefore must
    /// still be looked up when the outer value is NULL.
    pub hash_is_null_eq: Vec<bool>,
    /// Go `ColLens`: per-index-column prefix lengths, or
    /// [`UNSPECIFIED_LENGTH`].
    pub col_lens: Vec<i64>,
    /// Go `HasPrefixCol`: whether any entry of `col_lens` is a real prefix.
    pub has_prefix_col: bool,
}

/// Go `IndexJoinLookUpContent` (`index_lookup_join.go:550`): one deduplicated
/// probe the inner reader must serve.
#[derive(Clone, Debug)]
pub struct IndexJoinLookUpContent {
    /// Go `Keys`: the index-key datums, already converted to the inner
    /// column types.
    pub keys: Vec<Datum>,
    /// Go `Row`: the originating outer row, which the range builder needs for
    /// a `ColWithCmpFuncManager` bound.
    ///
    /// Go holds a `chunk.Row` borrowed from `task.outerResult`; an owned copy
    /// is taken here because the list is behind the task and Rust will not let
    /// the borrow outlive it.
    pub row: OwnedRow,
    /// Go `keyCols` (unexported): the inner key offsets, copied per content.
    pub key_cols: Vec<usize>,
    /// Go `KeyColIDs`.
    pub key_col_ids: Vec<i64>,
}

/// Go `IndexJoinExecutorBuilder` (`index_lookup_join.go:104`): builds the
/// inner-side reader for one batch of lookup contents.
///
/// Go declares this as an interface *"to avoid cycle import"*; it is an
/// interface here for the same reason plus one more -- the body it stands for
/// (`buildExecutorForIndexJoinInternal` in `pkg/executor/builder.go`, which
/// turns lookup contents into index ranges and dispatches a distsql request)
/// is not in this crate at all.
///
/// # boundary: `IndexJoinExecutorBuilder.BuildExecutorForIndexJoin`
/// Go additionally passes `cwc *physicalop.ColWithCmpFuncManager`, a
/// `*memory.Tracker` and the `interruptSignal *atomic.Value`. The first is a
/// planner type, the second has no counterpart, and the third exists only to
/// let a worker abort a coprocessor round-trip -- none of which survive the
/// sequential, tracker-free shape here.
pub trait IndexJoinExecutorBuilder {
    /// Go `BuildExecutorForIndexJoin`.
    ///
    /// # Errors
    /// Propagates reader-construction failure.
    fn build_executor_for_index_join(
        &mut self,
        lookup_contents: &[IndexJoinLookUpContent],
        index_ranges: &[IndexRange],
        key_off_to_idx_off: &[usize],
        can_reorder_handles: bool,
    ) -> Result<Box<dyn Executor>, ExecError>;
}

/// The tie-break Go's `physicalop.ColWithCmpFuncManager.CompareRow` supplies
/// to `sortAndDedupLookUpContents` (`:703`).
///
/// It exists for the `col > x_col AND col < x_col + 100` shape, where two
/// outer rows with equal *key* datums still need distinct ranges because the
/// last index column's bounds are functions of the outer row. Without it, the
/// dedup would collapse two probes that are not the same probe.
///
/// # boundary: `physicalop.ColWithCmpFuncManager`
/// The manager itself is a planner type holding `TmpConstant` scratch and the
/// compare functions; only its row comparison is needed here, so only that is
/// modelled.
pub trait LastColComparator {
    /// Go `ColWithCmpFuncManager.CompareRow`.
    fn compare_row(&self, left: Row<'_>, right: Row<'_>) -> std::cmp::Ordering;
}

// ---------------------------------------------------------------------------
// lookUpJoinTask (`index_lookup_join.go:126`)
// ---------------------------------------------------------------------------

/// Go `lookUpJoinTask` (`index_lookup_join.go:126`): one batch of outer rows
/// together with the inner rows fetched for it.
///
/// Go's `doneCh`, `innerExec` and `memTracker` are cross-worker plumbing and a
/// tracker; none survive the sequential shape. `cursor`, `hasMatch` and
/// `hasNull` live on the executor here rather than on the task, because Rust
/// cannot hold a `&mut` to the task's scalars at the same time as a `&` to its
/// row lists -- the split is a borrow-checker accommodation with no semantic
/// content, since only one task is ever current.
pub(crate) struct LookUpJoinTask {
    /// Go `outerResult`.
    pub(crate) outer_result: List,
    /// Go `outerMatch`: per chunk, per row, whether `OuterCtx::filter` held.
    pub(crate) outer_match: Option<Vec<Vec<bool>>>,
    /// Go `innerResult`.
    pub(crate) inner_result: List,
    /// Go `encodedLookUpKeys`: one single-BLOB-column chunk per outer chunk,
    /// row-aligned with it, holding the encoded hash key (or NULL for an
    /// outer row that must not be looked up).
    pub(crate) encoded_lookup_keys: Vec<Chunk>,
    /// Go `lookupMap`.
    lookup_map: MVMap,
    /// Go `innerExec`: the reader kept alive across a bounded fetch. `None`
    /// once the inner side is drained and the reader closed.
    pub(crate) inner_exec: Option<Box<dyn Executor>>,
}

impl LookUpJoinTask {
    /// Go's `lookUpJoinTask` zero value plus `newList` (`:433`).
    fn new(
        outer_types: &[FieldType],
        inner_types: &[FieldType],
        init_cap: usize,
        max: usize,
    ) -> Self {
        LookUpJoinTask {
            outer_result: List::new(outer_types, init_cap, max),
            outer_match: None,
            inner_result: List::new(inner_types, init_cap, max),
            encoded_lookup_keys: Vec::new(),
            lookup_map: MVMap::new(),
            inner_exec: None,
        }
    }
}

/// The single BLOB column Go builds `encodedLookUpKeys` over
/// (`buildTask` :493: `types.NewFieldType(mysql.TypeBlob)`).
fn encoded_key_field_type() -> FieldType {
    FieldType::new(FieldTypeCode::Blob)
}

/// Go's `*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr`
/// (`buildLookUpMap` :828), written explicitly.
fn encode_row_ptr(ptr: RowPtr) -> [u8; 8] {
    let mut buf = [0u8; 8];
    buf[..4].copy_from_slice(&ptr.chk_idx.to_ne_bytes());
    buf[4..].copy_from_slice(&ptr.row_idx.to_ne_bytes());
    buf
}

/// The read half of [`encode_row_ptr`] (`lookUpMatchedInners` :379).
fn decode_row_ptr(buf: &[u8]) -> Option<RowPtr> {
    if buf.len() < 8 {
        return None;
    }
    let chk_idx = u32::from_ne_bytes(buf[0..4].try_into().ok()?);
    let row_idx = u32::from_ne_bytes(buf[4..8].try_into().ok()?);
    Some(RowPtr { chk_idx, row_idx })
}

/// Go's `(dLookupKey, dHashKey)` pair from `constructDatumLookupKey` (`:654`).
///
/// Go returns two nil slices together for every skip reason, so the pair is
/// one `Option` here rather than two.
struct DatumLookupKey {
    /// Go `dLookupKey`: the index-key prefix, in inner column types.
    lookup: Vec<Datum>,
    /// Go `dHashKey`: the full probe key, which is what gets encoded.
    hash: Vec<Datum>,
}

/// Go `compareRow` (`index_lookup_join.go:725`): lexicographic datum-tuple
/// compare under per-column collations.
///
/// Go swallows the comparison error (`terror.Log`) on the stated grounds that
/// both sides have the same type; the same is done here, and an errored
/// column compares equal.
fn compare_row(left: &[Datum], right: &[Datum], collators: &[Collation]) -> std::cmp::Ordering {
    for idx in 0..left.len().min(right.len()) {
        let collation = collators.get(idx).copied().unwrap_or(Collation::Binary);
        match left[idx].compare(&right[idx], collation) {
            Ok(std::cmp::Ordering::Equal) | Err(_) => {}
            Ok(other) => return other,
        }
    }
    std::cmp::Ordering::Equal
}

// ---------------------------------------------------------------------------
// IndexLookUpJoin (`index_lookup_join.go:60`)
// ---------------------------------------------------------------------------

/// Go `IndexLookUpJoin` (`index_lookup_join.go:60`).
///
/// See the module header for the order guarantee and for what the missing
/// worker goroutines do and do not change.
pub struct IndexLookUpJoin<C: Columns> {
    pub(crate) meta: ExecutorMeta,
    /// Go `Children(0)`: the outer child.
    pub(crate) outer_exec: Box<dyn Executor>,
    /// Go `OuterCtx`.
    pub(crate) outer_ctx: OuterCtx,
    /// Go `InnerCtx`.
    pub(crate) inner_ctx: InnerCtx,
    /// Go `InnerCtx.ReaderBuilder`.
    pub(crate) reader_builder: Box<dyn IndexJoinExecutorBuilder>,
    /// Go `Joiner`.
    pub(crate) joiner: Box<dyn Joiner>,
    /// Go `IsOuterJoin`.
    pub(crate) is_outer_join: bool,
    /// Go `IndexRanges.Range()`; each inner worker clones it, so there is one
    /// copy here.
    pub(crate) index_ranges: Vec<IndexRange>,
    /// Go `KeyOff2IdxOff`.
    pub(crate) key_off_to_idx_off: Vec<usize>,
    /// Go `LastColHelper`, narrowed to its row comparison.
    pub(crate) last_col_comparator: Option<Box<dyn LastColComparator>>,
    /// The evaluation context for `OuterCtx::filter`.
    pub(crate) ctx: C,

    /// Go `task`.
    pub(crate) task: Option<LookUpJoinTask>,
    /// Go `lookUpJoinTask.cursor`.
    pub(crate) cursor: RowPtr,
    /// Go `lookUpJoinTask.hasMatch`.
    pub(crate) has_match: bool,
    /// Go `lookUpJoinTask.hasNull`.
    pub(crate) has_null: bool,
    /// Go `lookUpMatchedInners`' result, as addresses rather than
    /// `task.matchedInners []chunk.Row`.
    pub(crate) matched_ptrs: Vec<RowPtr>,
    /// How much of `matched_ptrs` the joiner has consumed; Go keeps this
    /// inside `e.innerIter`.
    pub(crate) inner_cursor: usize,
    /// Whether `matched_ptrs` still belongs to a previous outer row.
    pub(crate) needs_lookup: bool,

    /// Go `outerWorker.batchSize` / `maxBatchSize` (`:151`).
    pub(crate) batch_size: usize,
    pub(crate) max_batch_size: usize,
    /// Go `requiredRows` (`:79`).
    pub(crate) required_rows: usize,
    /// Go `prepared`.
    pub(crate) prepared: bool,
    /// Whether the outer child is drained.
    pub(crate) outer_done: bool,
    /// Go `innerWorker.maxFetchSize` (`:172`); `0` means unlimited.
    pub(crate) max_fetch_size: usize,
}

impl<C: Columns> IndexLookUpJoin<C> {
    /// Builds the join. Mirrors what Go's plan-to-executor builder fills into
    /// the struct literal, minus the worker plumbing.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        meta: ExecutorMeta,
        outer_exec: Box<dyn Executor>,
        outer_ctx: OuterCtx,
        inner_ctx: InnerCtx,
        reader_builder: Box<dyn IndexJoinExecutorBuilder>,
        joiner: Box<dyn Joiner>,
        is_outer_join: bool,
        index_ranges: Vec<IndexRange>,
        key_off_to_idx_off: Vec<usize>,
        ctx: C,
    ) -> Self {
        let max_chunk_size = meta.max_chunk_size();
        IndexLookUpJoin {
            meta,
            outer_exec,
            outer_ctx,
            inner_ctx,
            reader_builder,
            joiner,
            is_outer_join,
            index_ranges,
            key_off_to_idx_off,
            last_col_comparator: None,
            ctx,
            task: None,
            cursor: RowPtr::default(),
            has_match: false,
            has_null: false,
            matched_ptrs: Vec::new(),
            inner_cursor: 0,
            needs_lookup: true,
            batch_size: max_chunk_size,
            max_batch_size: DEFAULT_INDEX_JOIN_BATCH_SIZE,
            required_rows: 0,
            prepared: false,
            outer_done: false,
            max_fetch_size: 0,
        }
    }

    /// Go `LastColHelper` (`:82`), narrowed to its comparison.
    #[must_use]
    pub fn with_last_col_comparator(mut self, cmp: Box<dyn LastColComparator>) -> Self {
        self.last_col_comparator = Some(cmp);
        self
    }

    /// Go `SessionVars.IndexJoinBatchSize`.
    #[must_use]
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size.max(1);
        self
    }

    /// Go `innerWorker.maxFetchSize` (`:172`), used by the merge variant's
    /// bounded fetch; `0` is Go's unlimited default.
    #[must_use]
    pub fn with_max_fetch_size(mut self, size: usize) -> Self {
        self.max_fetch_size = size;
        self
    }

    /// Go `outerWorker.increaseBatchSize` (`index_lookup_join.go:512`).
    pub(crate) fn increase_batch_size(&mut self) {
        if self.batch_size < self.max_batch_size {
            self.batch_size *= 2;
        }
        if self.batch_size > self.max_batch_size {
            self.batch_size = self.max_batch_size;
        }
    }

    /// Go `outerWorker.buildTask` (`index_lookup_join.go:439`).
    ///
    /// Returns `None` for Go's `return nil, nil` -- the outer side produced no
    /// row, so there is no task at all.
    pub(crate) fn build_task(&mut self) -> Result<Option<LookUpJoinTask>, ExecError> {
        let max_chunk_size = self.outer_exec.max_chunk_size().max(1);
        let mut task = LookUpJoinTask::new(
            &self.outer_ctx.row_types,
            &self.inner_ctx.row_types,
            self.outer_exec.init_cap().max(1),
            max_chunk_size,
        );

        self.increase_batch_size();
        let mut required_rows = self.batch_size;
        if self.is_outer_join && self.required_rows != 0 {
            // Go `:453`. Sequentially this is always the *current* `Next`'s
            // value; see the module header.
            required_rows = self.required_rows;
        }

        let mut next_chunk_cap = self.outer_exec.init_cap().max(1);
        while task.outer_result.len() < required_rows {
            let remaining = required_rows - task.outer_result.len();
            let fetch_required_rows = remaining.min(max_chunk_size);
            let chk_cap = next_chunk_cap.min(fetch_required_rows).max(1);
            let mut chk = Chunk::new(&self.outer_ctx.row_types, chk_cap, max_chunk_size);
            chk.set_required_rows(
                isize::try_from(fetch_required_rows).unwrap_or(isize::MAX),
                max_chunk_size,
            );
            self.outer_exec.next(&mut chk)?;
            let rows = chk.num_rows();
            if rows == 0 {
                self.outer_done = true;
                break;
            }
            task.outer_result.add(chk);
            if rows >= chk_cap && next_chunk_cap < max_chunk_size {
                next_chunk_cap = (next_chunk_cap * 2).max(rows).min(max_chunk_size);
            }
        }
        if task.outer_result.is_empty() {
            return Ok(None);
        }

        let num_chunks = task.outer_result.num_chunks();
        if !self.outer_ctx.filter.is_empty() {
            // Go uses `expression.VectorizedFilter`; the row-based
            // `EvalBool` it falls back to is what `crate::joiner::eval_bool`
            // ports, and the two agree row for row.
            let mut outer_match = Vec::with_capacity(num_chunks);
            for i in 0..num_chunks {
                let chk = task.outer_result.get_chunk(i);
                let mut selected = Vec::with_capacity(chk.num_rows());
                for row_idx in 0..chk.num_rows() {
                    let (matched, _) =
                        eval_bool(&self.ctx, &self.outer_ctx.filter, chk.get_row(row_idx))?;
                    selected.push(matched);
                }
                outer_match.push(selected);
            }
            task.outer_match = Some(outer_match);
        }

        let key_types = [encoded_key_field_type()];
        task.encoded_lookup_keys = (0..num_chunks)
            .map(|i| {
                let rows = task.outer_result.get_chunk(i).num_rows();
                Chunk::new_with_capacity(&key_types, rows.max(1))
            })
            .collect();
        Ok(Some(task))
    }

    /// Go `innerWorker.constructDatumLookupKey` (`index_lookup_join.go:654`).
    ///
    /// Returns `(None, None)` for every reason Go returns a nil key: the outer
    /// filter rejected the row, a NULL under a non-null-safe equality, and the
    /// three conversion outcomes that prove no inner row can match.
    fn construct_datum_lookup_key(
        &self,
        task: &LookUpJoinTask,
        chk_idx: usize,
        row_idx: usize,
    ) -> Result<Option<DatumLookupKey>, ExecError> {
        if let Some(outer_match) = &task.outer_match {
            if !outer_match[chk_idx][row_idx] {
                return Ok(None);
            }
        }
        let outer_row = task.outer_result.get_chunk(chk_idx).get_row(row_idx);
        let key_len = self.inner_ctx.key_cols.len();
        let mut lookup_key = Vec::with_capacity(key_len);
        let mut hash_key = Vec::with_capacity(self.outer_ctx.hash_cols.len());

        for (i, &hash_col) in self.outer_ctx.hash_cols.iter().enumerate() {
            let outer_value = outer_row.get_datum(hash_col, &self.outer_ctx.row_types[hash_col]);
            let is_null_eq = self
                .inner_ctx
                .hash_is_null_eq
                .get(i)
                .copied()
                .unwrap_or(false);
            if outer_value.is_null() {
                if !is_null_eq {
                    return Ok(None);
                }
                if i < key_len {
                    lookup_key.push(outer_value.clone());
                }
                hash_key.push(outer_value);
                continue;
            }
            let inner_col_type = &self.inner_ctx.row_types[self.inner_ctx.hash_cols[i]];
            // Go passes `sc.TypeCtx()`, whose flags decide whether a truncation
            // is an error or a warning.
            // boundary: `stmtctx.StatementContext.TypeCtx`
            // The statement's conversion flags and session zone are not
            // reachable from `Columns`; the default (error-on-truncate off)
            // flags are used, which is Go's non-strict shape.
            let converted = match outer_value.convert_to(inner_col_type, ConversionFlags::default())
            {
                Ok(converted) => converted.value,
                Err(_) => {
                    // Go distinguishes overflow/out-of-range (skip the lookup)
                    // from a genuine error (propagate), and tolerates
                    // truncation into SET/ENUM. The datum layer here reports
                    // one error type for all of them; skipping is the shape
                    // that cannot produce a wrong extra row, and the outer row
                    // still reaches the joiner as a miss.
                    return Ok(None);
                }
            };
            let collation = self
                .inner_ctx
                .hash_collators
                .get(i)
                .copied()
                .unwrap_or(Collation::Binary);
            let cmp = outer_value
                .compare(&converted, collation)
                .map_err(|err| ExecError::internal(format!("index join key compare: {err:?}")))?;
            if cmp != std::cmp::Ordering::Equal {
                // The converted value is not the original one, so no inner row
                // can equal it (`:697`).
                return Ok(None);
            }
            if i < key_len {
                lookup_key.push(converted.clone());
            }
            hash_key.push(converted);
        }
        Ok(Some(DatumLookupKey {
            lookup: lookup_key,
            hash: hash_key,
        }))
    }

    /// Go `innerWorker.constructLookupContent` (`index_lookup_join.go:582`).
    pub(crate) fn construct_lookup_content(
        &self,
        task: &mut LookUpJoinTask,
    ) -> Result<Vec<IndexJoinLookUpContent>, ExecError> {
        let mut contents = Vec::with_capacity(task.outer_result.len());
        for chk_idx in 0..task.outer_result.num_chunks() {
            let num_rows = task.outer_result.get_chunk(chk_idx).num_rows();
            for row_idx in 0..num_rows {
                let key = self.construct_datum_lookup_key(task, chk_idx, row_idx)?;
                let Some(DatumLookupKey {
                    lookup: mut lookup_key,
                    hash: hash_key,
                }) = key
                else {
                    // Go appends NULL so `encodedLookUpKeys` stays row-aligned
                    // with `outerResult` (`:611`).
                    task.encoded_lookup_keys[chk_idx].append_null(0);
                    continue;
                };
                // boundary: `codec.EncodeKey(sc.TimeZone(), ...)`
                // The session zone is not reachable from `Columns`; the
                // zone-free encoder is used, which differs only for TIMESTAMP
                // keys.
                let encoded = tidb_codec::encode_key(&hash_key).map_err(|err| {
                    ExecError::internal(format!("index join encode lookup key: {err:?}"))
                })?;
                task.encoded_lookup_keys[chk_idx].append_bytes(0, &encoded);

                if self.inner_ctx.has_prefix_col {
                    for (i, &outer_offset) in self.key_off_to_idx_off.iter().enumerate() {
                        let prefix_len = self
                            .inner_ctx
                            .col_lens
                            .get(outer_offset)
                            .copied()
                            .unwrap_or(UNSPECIFIED_LENGTH);
                        if prefix_len != UNSPECIFIED_LENGTH {
                            if let Some(datum) = lookup_key.get_mut(i) {
                                cut_datum_by_prefix_len(datum, prefix_len);
                            }
                        }
                    }
                }
                contents.push(IndexJoinLookUpContent {
                    keys: lookup_key,
                    row: task
                        .outer_result
                        .get_chunk(chk_idx)
                        .get_row(row_idx)
                        .copy_construct(),
                    key_cols: self.inner_ctx.key_cols.clone(),
                    key_col_ids: self.inner_ctx.key_col_ids.clone(),
                });
            }
        }
        Ok(self.sort_and_dedup_lookup_contents(contents))
    }

    /// Go `innerWorker.sortAndDedupLookUpContents` (`index_lookup_join.go:703`).
    fn sort_and_dedup_lookup_contents(
        &self,
        mut contents: Vec<IndexJoinLookUpContent>,
    ) -> Vec<IndexJoinLookUpContent> {
        if contents.len() < 2 {
            return contents;
        }
        let collators = &self.inner_ctx.key_collators;
        contents.sort_by(|left, right| {
            let cmp = compare_row(&left.keys, &right.keys, collators);
            match (&self.last_col_comparator, cmp) {
                (Some(last), std::cmp::Ordering::Equal) => {
                    last.compare_row(left.row.as_row(), right.row.as_row())
                }
                _ => cmp,
            }
        });
        let mut deduped: Vec<IndexJoinLookUpContent> = Vec::with_capacity(contents.len());
        for content in contents {
            let distinct = match deduped.last() {
                None => true,
                Some(prev) => {
                    compare_row(&content.keys, &prev.keys, collators) != std::cmp::Ordering::Equal
                        || self.last_col_comparator.as_ref().is_some_and(|last| {
                            last.compare_row(content.row.as_row(), prev.row.as_row())
                                != std::cmp::Ordering::Equal
                        })
                }
            };
            if distinct {
                deduped.push(content);
            }
        }
        deduped
    }

    /// Go `innerWorker.fetchInnerResults` (`index_lookup_join.go:739`).
    ///
    /// The reader is built on the first call for a task and **kept** in
    /// `task.inner_exec` afterwards, exactly as Go does: with a
    /// `max_fetch_size`, a second call resumes the same reader where the first
    /// stopped rather than re-reading. `task.inner_exec` becoming `None` is
    /// Go's `needClose` signal that the inner side is drained -- the hash
    /// variant's incremental-lookup loop reads it that way.
    pub(crate) fn fetch_inner_results(
        &mut self,
        task: &mut LookUpJoinTask,
        contents: &[IndexJoinLookUpContent],
    ) -> Result<(), ExecError> {
        if task.inner_exec.is_none() {
            let mut inner_exec = self.reader_builder.build_executor_for_index_join(
                contents,
                &self.index_ranges,
                &self.key_off_to_idx_off,
                true,
            )?;
            task.inner_result = List::new(
                inner_exec.ret_field_types(),
                inner_exec.init_cap().max(1),
                inner_exec.max_chunk_size().max(1),
            );
            inner_exec.open()?;
            task.inner_exec = Some(inner_exec);
        } else {
            task.inner_result.reset();
        }

        let mut needs_close = false;
        let result = (|| -> Result<(), ExecError> {
            let inner_exec = task.inner_exec.as_mut().expect("just built");
            loop {
                let mut chk = task.inner_result.alloc_chunk();
                match inner_exec.next(&mut chk) {
                    Ok(()) => {}
                    Err(err) => {
                        needs_close = true;
                        return Err(err);
                    }
                }
                if chk.num_rows() == 0 {
                    needs_close = true;
                    return Ok(());
                }
                task.inner_result.add(chk);
                if self.max_fetch_size > 0 && task.inner_result.len() >= self.max_fetch_size {
                    return Ok(());
                }
            }
        })();
        if needs_close {
            if let Some(mut inner_exec) = task.inner_exec.take() {
                let close = inner_exec.close();
                return result.and(close);
            }
        }
        result
    }

    /// Go `innerWorker.hasNullInJoinKey` (`index_lookup_join.go:835`).
    fn has_null_in_join_key(&self, row: Row<'_>) -> bool {
        self.inner_ctx
            .hash_cols
            .iter()
            .enumerate()
            .any(|(i, &key_col)| {
                row.is_null(key_col)
                    && !self
                        .inner_ctx
                        .hash_is_null_eq
                        .get(i)
                        .copied()
                        .unwrap_or(false)
            })
    }

    /// Go `innerWorker.buildLookUpMap` (`index_lookup_join.go:800`).
    fn build_lookup_map(&self, task: &mut LookUpJoinTask) -> Result<(), ExecError> {
        let mut entries: Vec<(Vec<u8>, [u8; 8])> = Vec::new();
        for i in 0..task.inner_result.num_chunks() {
            let chk = task.inner_result.get_chunk(i);
            for j in 0..chk.num_rows() {
                let inner_row = chk.get_row(j);
                if self.has_null_in_join_key(inner_row) {
                    continue;
                }
                // Go encodes the key columns one datum at a time into one
                // buffer, which is the same bytes as encoding the tuple.
                let key_datums: Vec<Datum> = self
                    .inner_ctx
                    .hash_cols
                    .iter()
                    .map(|&key_col| {
                        inner_row.get_datum(key_col, &self.inner_ctx.row_types[key_col])
                    })
                    .collect();
                let key = tidb_codec::encode_key(&key_datums).map_err(|err| {
                    ExecError::internal(format!("index join encode inner key: {err:?}"))
                })?;
                let ptr = RowPtr {
                    chk_idx: u32::try_from(i).unwrap_or(u32::MAX),
                    row_idx: u32::try_from(j).unwrap_or(u32::MAX),
                };
                entries.push((key, encode_row_ptr(ptr)));
            }
        }
        for (key, value) in &entries {
            task.lookup_map.put(key, value);
        }
        Ok(())
    }

    /// Go `innerWorker.handleTask` (`index_lookup_join.go:557`).
    fn handle_task(&mut self, task: &mut LookUpJoinTask) -> Result<(), ExecError> {
        let contents = self.construct_lookup_content(task)?;
        self.fetch_inner_results(task, &contents)?;
        self.build_lookup_map(task)
    }

    /// Go `IndexLookUpJoin.getFinishedTask` (`index_lookup_join.go:341`),
    /// with the channel round-trip replaced by building the next task inline.
    ///
    /// `Ok(false)` is Go's `task == nil`: the join is exhausted.
    fn get_finished_task(&mut self) -> Result<bool, ExecError> {
        if let Some(task) = &self.task {
            if (self.cursor.chk_idx as usize) < task.outer_result.num_chunks() {
                return Ok(true);
            }
        }
        self.task = None;
        if self.outer_done {
            return Ok(false);
        }
        let Some(mut task) = self.build_task()? else {
            return Ok(false);
        };
        self.handle_task(&mut task)?;
        self.task = Some(task);
        self.cursor = RowPtr::default();
        self.has_match = false;
        self.has_null = false;
        self.matched_ptrs.clear();
        self.inner_cursor = 0;
        self.needs_lookup = true;
        Ok(true)
    }

    /// Go `IndexLookUpJoin.lookUpMatchedInners` (`index_lookup_join.go:373`).
    fn look_up_matched_inners(&mut self) {
        self.matched_ptrs.clear();
        self.inner_cursor = 0;
        let Some(task) = &self.task else { return };
        let key_chunk = &task.encoded_lookup_keys[self.cursor.chk_idx as usize];
        let key_row = key_chunk.get_row(self.cursor.row_idx as usize);
        // Go reads `GetBytes(0)` unconditionally; a NULL cell yields an empty
        // slice, which no encoded inner key can equal as long as there is at
        // least one hash column -- so the outer row correctly finds nothing.
        let key = key_row.get_bytes(0);
        for value in task.lookup_map.get(&key, Vec::new()) {
            if let Some(ptr) = decode_row_ptr(value) {
                self.matched_ptrs.push(ptr);
            }
        }
    }

    /// Advances `cursor` past the current outer row (Go `Next` :320-:327).
    fn advance_cursor(&mut self) {
        let Some(task) = &self.task else { return };
        self.cursor.row_idx += 1;
        let chunk_rows = task
            .outer_result
            .get_chunk(self.cursor.chk_idx as usize)
            .num_rows();
        if self.cursor.row_idx as usize == chunk_rows {
            self.cursor.chk_idx += 1;
            self.cursor.row_idx = 0;
        }
        self.has_match = false;
        self.has_null = false;
        self.needs_lookup = true;
    }
}

impl<C: Columns> Executor for IndexLookUpJoin<C> {
    /// Go `Open` (`index_lookup_join.go:178`).
    fn open(&mut self) -> Result<(), ExecError> {
        self.outer_exec.open()?;
        if self.inner_ctx.hash_is_null_eq.len() != self.inner_ctx.hash_cols.len() {
            return Err(ExecError::internal(
                "index lookup join: hash null-eq flags length must match hash cols length",
            ));
        }
        self.task = None;
        self.cursor = RowPtr::default();
        self.has_match = false;
        self.has_null = false;
        self.matched_ptrs.clear();
        self.inner_cursor = 0;
        self.needs_lookup = true;
        self.prepared = false;
        self.outer_done = false;
        Ok(())
    }

    /// Go `Next` (`index_lookup_join.go:283`).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if !self.prepared {
            // Go `startWorkers(ctx, req.RequiredRows())` seeds the outer
            // worker's batch size from the first requested row count.
            self.batch_size = req.required_rows().max(1).min(self.max_batch_size);
            self.prepared = true;
        }
        if self.is_outer_join {
            self.required_rows = req.required_rows();
        }
        req.reset();
        loop {
            if !self.get_finished_task()? {
                return Ok(());
            }
            if self.needs_lookup {
                self.look_up_matched_inners();
                self.needs_lookup = false;
            }

            let budget = req.required_rows().saturating_sub(req.num_rows());
            let remaining = self.matched_ptrs.len().saturating_sub(self.inner_cursor);
            if remaining > 0 && budget > 0 {
                let ptrs: Vec<RowPtr> = self.matched_ptrs[self.inner_cursor..].to_vec();
                // Disjoint field borrows: the joiner, the task's row lists and
                // the cursor scalars are separate fields of `self`.
                let task = self.task.as_ref().expect("task is current");
                let outer_row = task.outer_result.get_row(self.cursor);
                let mut inners = LendingIterator::row_ptrs(&task.inner_result, ptrs);
                inners.begin();
                let (matched, is_null) = self.joiner.try_to_match_inners(
                    outer_row,
                    &mut inners,
                    req,
                    NAAJType::Unknown,
                )?;
                // Go reads `innerIter.Current() == innerIter.End()`. `None`
                // means the joiner either drained the batch or called
                // `ReachEnd` (the semi family, which settles the outer row on
                // its first match) -- both mean this outer row is finished.
                // A non-`None` current can only come from the row joiner's
                // `budget` break, which consumes exactly `budget` rows.
                if inners.current().is_none() {
                    self.inner_cursor = self.matched_ptrs.len();
                } else {
                    self.inner_cursor += budget;
                }
                self.has_match = self.has_match || matched;
                self.has_null = self.has_null || is_null;
            }

            if self.inner_cursor >= self.matched_ptrs.len() {
                if !self.has_match {
                    let task = self.task.as_ref().expect("task is current");
                    let outer_row = task.outer_result.get_row(self.cursor);
                    self.joiner.on_miss_match(self.has_null, outer_row, req);
                }
                self.advance_cursor();
            }
            if req.is_full() {
                return Ok(());
            }
        }
    }

    /// Go `Close` (`index_lookup_join.go:845`).
    fn close(&mut self) -> Result<(), ExecError> {
        // Go's `fetchInnerResults` closes the reader on the drain path; a task
        // abandoned mid-fetch (an error, or a `LIMIT` above) still owns one.
        if let Some(mut inner_exec) = self.task.as_mut().and_then(|t| t.inner_exec.take()) {
            inner_exec.close()?;
        }
        self.task = None;
        self.matched_ptrs.clear();
        self.inner_cursor = 0;
        self.prepared = false;
        self.outer_done = false;
        self.outer_exec.close()
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

/// Go `ranger.CutDatumByPrefixLen` (`pkg/util/ranger/types.go`), narrowed to
/// the string/bytes cases an index prefix can apply to.
///
/// # boundary: `ranger.CutDatumByPrefixLen`
/// Go consults the column's charset to cut by *characters* for a non-binary
/// collation and by bytes otherwise. Only the byte cut is done here; a
/// multi-byte charset prefix index therefore keeps a longer key than Go's,
/// which can only make the probe MORE selective than the index entry, never
/// less -- so it never invents a row, but it can miss one. Named rather than
/// hidden.
fn cut_datum_by_prefix_len(datum: &mut Datum, prefix_len: i64) {
    let Ok(limit) = usize::try_from(prefix_len) else {
        return;
    };
    match datum {
        Datum::Bytes(bytes) if bytes.len() > limit => {
            bytes.truncate(limit);
        }
        _ => {}
    }
}

#[cfg(test)]
pub(crate) mod tests;
