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

//! `pkg/executor/join/index_lookup_merge_join.go`: `IndexLookUpMergeJoin` --
//! the merge variant of the index-nested-loop join.
//!
//! # Completeness
//!
//! This file **lands the complete join**: every Go symbol that shapes an
//! output row is ported -- `IndexLookUpMergeJoin`, `OuterMergeCtx`,
//! `InnerMergeCtx`, `lookUpMergeJoinTask`, `outerMergeWorker.buildTask` /
//! `increaseBatchSize`, `innerMergeWorker.handleTask` / `doMergeJoin` /
//! `fetchInnerRowsWithSameKey` / `compare` / `constructDatumLookupKeys` /
//! `constructDatumLookupKey` / `dedupDatumLookUpKeys` / `fetchNextInnerResult`,
//! and `Open` / `Next` / `Close`.
//!
//! The Go symbols that are NOT present are exactly the goroutine and channel
//! plumbing -- `startWorkers`, `newOuterWorker`, `newInnerMergeWorker`,
//! `loadFinishedTask`, `outerMergeWorker.run`, `pushToChan`,
//! `innerMergeWorker.run`, `fetchNewChunkWhenFull`, `indexMergeJoinResult` --
//! each named at its site below with what it stood for and why nothing
//! observable rides on it. Tests are WRITTEN, not transcreated: Go's coverage
//! for this executor is `pkg/executor/test/jointest/*` under `testkit` (real
//! session, real store, real SQL) and is not dependency-closed here.
//!
//! # What makes it the *merge* variant
//!
//! [`crate::index_lookup_join`] materialises the whole inner side of a batch
//! into a hash table and probes it per outer row. This file never builds a
//! hash table: it relies on **both sides already being sorted on the join
//! keys** and walks them together, keeping only the run of inner rows sharing
//! the current key (`task.sameKeyInnerRows`, `:111`). That is why the inner
//! reader is built with `canReorderHandles == false` (`handleTask` :490) --
//! the plain lookup join passes `true`, because it does not care what order
//! the inner rows arrive in, and this one does.
//!
//! # The ordering contract (the crux of this file)
//!
//! Determined structurally, from four places in the Go source:
//!
//! 1. `doMergeJoin` (`:555`) emits by iterating `task.outerOrderIdx` **in
//!    slice order**, one outer row at a time, appending to the result chunk.
//!    So a task's output order is exactly `outerOrderIdx`'s order.
//! 2. `outerOrderIdx` is built (`:440`-`:445`) as `(chkIdx, rowIdx)` in outer
//!    chunk-then-row order, i.e. outer-scan order -- and is then re-sorted by
//!    join key **only if `OuterMergeCtx.NeedOuterSort`** (`:450`-`:476`).
//! 3. Across tasks, `resultCh` (`:182`) has a **single** producer, the one
//!    `outerMergeWorker` goroutine, which pushes each task to `innerCh` and
//!    then to `resultCh` (`run` :325-:331) in the order it built them. The
//!    main thread reads that FIFO in `loadFinishedTask` (`:290`).
//! 4. Within a task, `task.results` (`:115`) is a per-task channel written by
//!    the single inner worker that owns that task, and `Next` (`:268`) drains
//!    it to completion -- `!ok` (closed) is the only path to the next task.
//!    So no two tasks ever interleave, however many inner workers run.
//!
//! Therefore:
//!
//! * **`NeedOuterSort == false`: output is in outer-scan order, totally and
//!   unconditionally.** This is the documented contract at `:43`-`:44`
//!   (*"It preserves the order of the outer table and support batch lookup"*).
//! * **`NeedOuterSort == true`: output is in join-key order within each
//!   batch**, batches themselves still in outer-scan order. This is a real,
//!   observable reordering relative to the outer child, and it is what the
//!   flag means: the outer side's property items could not guarantee join-key
//!   order, and merge join requires it, so the executor imposes it (`:447`-
//!   `:449`). The batch boundary is `outerMergeWorker.buildTask`'s row count,
//!   so the sort is per-batch, NOT global -- a fact that matters, and that
//!   `merge_join_sorts_only_within_a_batch` pins down.
//!
//! Because the order is producer-serial in both cases, running the pipeline
//! sequentially -- build one batch, look its keys up, merge it, drain it,
//! build the next -- emits **the same rows in the same order**. What is lost
//! is only overlap: Go reads the inner side of task K+1 while merging task K.
//!
//! The one genuinely observable consequence of going sequential is the
//! `IsOuterJoin` back-pressure at `buildTask` (`:356`): Go reads
//! `lookup.requiredRows`, which the main thread stores on each `Next` (`:260`)
//! while the outer worker may already be several batches ahead. Here the value
//! is always the current `Next`'s, so the batch tracks the consumer exactly
//! rather than approximately. Row set and order are unchanged; only how many
//! outer rows are read ahead of a `LIMIT` changes, in the direction of reading
//! fewer.
//!
//! # Reuse rather than restatement
//!
//! * [`crate::joiner::Joiner`] is Go's `Joiner`. `doMergeJoin` makes exactly
//!   two calls -- `TryToMatchInners` (`:584`) and `OnMissMatch` (`:597`) -- and
//!   this file makes the same two. No join-type semantics are re-derived here.
//! * [`crate::index_lookup_join::IndexJoinLookUpContent`],
//!   [`crate::index_lookup_join::IndexJoinExecutorBuilder`] and
//!   [`crate::index_lookup_join::LastColComparator`] are the same Go types
//!   this file's `handleTask` uses, already ported next door.
//! * [`tidb_chunk::list::List`] / [`tidb_chunk::list::RowPtr`] are Go's
//!   `chunk.List` / `chunk.RowPtr`.
//! * `LendingIterator::row_ptrs` stands in for Go's `chunk.NewIterator4Slice`
//!   over `task.sameKeyInnerRows`: same rows, same order, addressed rather
//!   than borrowed.
//!
//! # boundary: `innerMergeWorker.fetchNextInnerResult` (`:719`) -- chunk retention
//! Go replaces `task.innerResult` with a *fresh* chunk on every fetch and lets
//! the GC keep the previous one alive through the `[]chunk.Row` values sitting
//! in `task.sameKeyInnerRows`. Rust has no such back-reference, so the fetched
//! chunks are **appended** to one [`List`] for the life of the task and
//! `sameKeyInnerRows` holds [`RowPtr`]s into it. Row content and order are
//! identical; the difference is that a task holds its whole inner result
//! instead of one chunk plus whatever is still referenced. That is the same
//! footprint [`crate::index_lookup_join`] already has, and it cannot change a
//! row.
//!
//! This is also why [`crate::index_lookup_join::IndexLookUpJoin::fetch_inner_results`]
//! is not reused despite being resumable: verified against its body, it calls
//! `task.inner_result.reset()` on every resumed call, which would invalidate
//! exactly the [`RowPtr`]s `sameKeyInnerRows` is made of. Its *resumability*
//! (keep the reader in the task, resume where the last call stopped) is real
//! and is reproduced here; its buffer management is not what this variant
//! needs.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::iterator::LendingIterator;
use tidb_chunk::list::{List, RowPtr};
use tidb_datatype::{Collation, ConversionFlags, Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::index_lookup_join::{
    IndexJoinExecutorBuilder, IndexJoinLookUpContent, LastColComparator,
    DEFAULT_INDEX_JOIN_BATCH_SIZE,
};
use crate::joiner::{eval_bool, Joiner, NAAJType};
use crate::kv_table::IndexRange;

/// Go `outerMergeWorker.batchSize`'s literal seed (`newOuterWorker` :210).
///
/// The merge variant seeds its doubling batch at a constant 32 rather than at
/// the first `req.RequiredRows()` the way `index_lookup_join.go` does.
pub const INITIAL_MERGE_BATCH_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// OuterMergeCtx (`index_lookup_merge_join.go:80`) / InnerMergeCtx (`:90`)
// ---------------------------------------------------------------------------

/// Go `OuterMergeCtx` (`index_lookup_merge_join.go:80`).
///
/// # boundary: `OuterMergeCtx.JoinKeys []*expression.Column` and `CompareFuncs`
/// Go carries both the join-key *expressions* and the `expression.CompareFunc`
/// built from them, then calls `CompareFuncs[i](ctx, joinKey, joinKey, rowI,
/// rowJ)`. The keys are declared `*expression.Column`, so the function reduces
/// to "read column `JoinKeys[i].Index` from each row and compare"; that is what
/// [`OuterMergeCtx::key_cols`] plus the inner side's collators express here.
/// A non-column join key cannot occur in this struct's Go type, so nothing is
/// dropped -- but the coercion `expression.GetCmpFunction` would install for
/// two *differently typed* key columns is narrowed to
/// [`tidb_datatype::Datum`]'s own cross-type comparison.
#[derive(Clone, Debug, Default)]
pub struct OuterMergeCtx {
    /// Go `RowTypes`: the outer child's output column types.
    pub row_types: Vec<FieldType>,
    /// Go `KeyCols`: outer offsets of the join-key columns, aligned with Go's
    /// `JoinKeys`.
    pub key_cols: Vec<usize>,
    /// Go `Filter`: the CNF applied to the outer row before it is looked up.
    /// A row that fails it is never looked up (`constructDatumLookupKey`
    /// :659) but still reaches the joiner as a miss (`doMergeJoin` :558), so
    /// an outer join keeps emitting it.
    pub filter: Vec<Expression>,
    /// Go `NeedOuterSort`: the outer side's order does not guarantee join-key
    /// order, so the batch must be sorted before merging. See the module
    /// header -- this flag IS the ordering contract's only variable.
    pub need_outer_sort: bool,
}

/// Go `InnerMergeCtx` (`index_lookup_merge_join.go:90`), minus `ReaderBuilder`,
/// which is passed separately because it is the one non-`Clone` member.
#[derive(Clone, Debug, Default)]
pub struct InnerMergeCtx {
    /// Go `RowTypes`: the inner reader's output column types.
    pub row_types: Vec<FieldType>,
    /// Go `KeyCols`: inner offsets of the join-key columns.
    pub key_cols: Vec<usize>,
    /// Go `KeyColIDs`: the key columns' table IDs, carried to the reader
    /// builder for dynamic partition pruning.
    pub key_col_ids: Vec<i64>,
    /// Go `KeyCollators`, aligned with `key_cols`.
    pub key_collators: Vec<Collation>,
    /// Go `ColLens`: per-index-column prefix lengths.
    ///
    /// # boundary: `InnerMergeCtx.ColLens`
    /// Go stores them but this file never reads them: unlike
    /// `index_lookup_join.go`, `index_lookup_merge_join.go` has no prefix cut
    /// -- `constructDatumLookupKey` (`:658`) does not touch `ColLens`. The
    /// field is kept so the struct matches Go's and the reader builder can be
    /// handed the same information.
    pub col_lens: Vec<i64>,
    /// Go `Desc`: the merge runs in descending key order.
    pub desc: bool,
    /// Go `KeyOff2KeyOffOrderByIdx`: the permutation taking a join-key offset
    /// to its position in the index's ORDER BY, i.e. the order the keys must
    /// be compared in for the merge to be a merge.
    pub key_off_to_key_off_order_by_idx: Vec<usize>,
}

// ---------------------------------------------------------------------------
// lookUpMergeJoinTask (`index_lookup_merge_join.go:103`)
// ---------------------------------------------------------------------------

/// Go `lookUpMergeJoinTask` (`index_lookup_merge_join.go:103`): one batch of
/// outer rows plus the merge cursor over the inner side fetched for it.
///
/// Go's `doneErr`, `results` and `memTracker` do not survive: an error
/// propagates by `?` out of the single thread instead of through the task, and
/// there is no tracker to attach to.
struct LookUpMergeJoinTask {
    /// Go `outerResult`.
    outer_result: List,
    /// Go `outerMatch`: per chunk, per row, whether `OuterMergeCtx::filter`
    /// held.
    outer_match: Option<Vec<Vec<bool>>>,
    /// Go `outerOrderIdx`: the order `doMergeJoin` walks the outer rows in.
    /// See the module header.
    outer_order_idx: Vec<RowPtr>,

    /// Go `innerResult`, accumulated rather than replaced -- see the module
    /// header's chunk-retention boundary.
    inner_result: List,
    /// Go `innerIter`'s position: the address of `innerIter.Current()`.
    inner_ptr: RowPtr,
    /// Go's `task.innerResult.NumRows() == 0` predicate, which is what
    /// `fetchInnerRowsWithSameKey` returns as `noneInnerRows` (`:626`): true
    /// once a fetch came back empty.
    inner_drained: bool,
    /// Go `innerExec` (a `defer`red close in `handleTask` :492), kept in the
    /// task so a fetch can resume the same reader.
    inner_exec: Option<Box<dyn Executor>>,

    /// Go `sameKeyInnerRows` (`:111`), as addresses into `inner_result`.
    same_key_inner_rows: Vec<RowPtr>,
    /// Go `sameKeyIter`'s position (`:112`).
    same_key_at: usize,
}

impl LookUpMergeJoinTask {
    /// Go's `lookUpMergeJoinTask` literal in `buildTask` (`:347`).
    fn new(outer_types: &[FieldType], init_cap: usize, max_chunk_size: usize) -> Self {
        LookUpMergeJoinTask {
            outer_result: List::new(outer_types, init_cap, max_chunk_size),
            outer_match: None,
            outer_order_idx: Vec::new(),
            // Replaced with the reader's own field types in `handle_task`;
            // Go likewise only learns them from `imw.innerExec` (`:720`).
            inner_result: List::new(&[], 1, max_chunk_size),
            inner_ptr: RowPtr::default(),
            inner_drained: true,
            inner_exec: None,
            same_key_inner_rows: Vec::new(),
            same_key_at: 0,
        }
    }

    /// Go `task.innerIter.Current()`, `None` for `innerIter.End()`.
    fn inner_current(&self) -> Option<RowPtr> {
        let chk_idx = self.inner_ptr.chk_idx as usize;
        if chk_idx >= self.inner_result.num_chunks() {
            return None;
        }
        if self.inner_ptr.row_idx as usize >= self.inner_result.num_rows_of_chunk(chk_idx) {
            return None;
        }
        Some(self.inner_ptr)
    }

    /// Go `task.innerIter.Next()`: advance within the current chunk. Returns
    /// false when the iterator ran off the end and a fetch is due.
    fn advance_inner(&mut self) -> bool {
        self.inner_ptr.row_idx += 1;
        self.inner_current().is_some()
    }
}

/// Go `compareRow` (`index_lookup_join.go:725`), used here by
/// `dedupDatumLookUpKeys` (`:710`).
///
/// The sibling port has the same function, module-private; it is restated
/// rather than shared because this file may not edit that one. Go swallows the
/// comparison error (`terror.Log`) on the grounds that both sides have the
/// same type, so an errored column compares equal here too.
fn compare_datum_row(left: &[Datum], right: &[Datum], collators: &[Collation]) -> Ordering {
    for idx in 0..left.len().min(right.len()) {
        let collation = collators.get(idx).copied().unwrap_or(Collation::Binary);
        match left[idx].compare(&right[idx], collation) {
            Ok(Ordering::Equal) | Err(_) => {}
            Ok(other) => return other,
        }
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// IndexLookUpMergeJoin (`index_lookup_merge_join.go:51`)
// ---------------------------------------------------------------------------

/// Where the merge loop is inside the current outer row.
///
/// Go needs no such state: `doMergeJoin` runs an outer row to completion on
/// its own goroutine and pushes full chunks through `task.results`
/// (`fetchNewChunkWhenFull` :505). Here the loop must suspend the moment the
/// caller's `req` fills, so its position is explicit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowPhase {
    /// The pre-loop of `doMergeJoin`'s body (`:556`-`:581`) has not run for
    /// this outer row yet.
    NotStarted,
    /// Inside `for task.sameKeyIter.Current() != task.sameKeyIter.End()`
    /// (`:583`).
    Joining,
    /// Go's `goto missMatch` (`:559`, `:564`, `:575`): the join loop is
    /// skipped entirely for this outer row.
    Missed,
}

/// Go `IndexLookUpMergeJoin` (`index_lookup_merge_join.go:51`).
///
/// See the module header for the ordering contract and for what the missing
/// worker goroutines do and do not change.
pub struct IndexLookUpMergeJoin<C: Columns> {
    meta: ExecutorMeta,
    /// Go `Children(0)`: the outer child.
    outer_exec: Box<dyn Executor>,
    /// Go `OuterMergeCtx`.
    outer_ctx: OuterMergeCtx,
    /// Go `InnerMergeCtx`.
    inner_ctx: InnerMergeCtx,
    /// Go `InnerMergeCtx.ReaderBuilder`.
    reader_builder: Box<dyn IndexJoinExecutorBuilder>,
    /// Go `Joiners []Joiner` (`:61`), one per inner worker. One worker, one
    /// joiner.
    joiner: Box<dyn Joiner>,
    /// Go `IsOuterJoin` (`:63`).
    is_outer_join: bool,
    /// Go `IndexRanges.Range()`; Go clones it per inner worker (`:223`) purely
    /// to avoid a data race, so one copy suffices here.
    index_ranges: Vec<IndexRange>,
    /// Go `KeyOff2IdxOff` (`:70`).
    key_off_to_idx_off: Vec<usize>,
    /// Go `LastColHelper` / `innerMergeWorker.nextColCompareFilters` (`:73`),
    /// narrowed to its row comparison.
    last_col_comparator: Option<Box<dyn LastColComparator>>,
    /// The evaluation context for `OuterMergeCtx::filter`.
    ctx: C,

    /// Go `task` (`:67`).
    task: Option<LookUpMergeJoinTask>,
    /// How far `doMergeJoin`'s `for _, outerIdx := range task.outerOrderIdx`
    /// (`:555`) has got.
    outer_at: usize,
    phase: RowPhase,
    /// Go `hasMatch` / `hasNull`, the two locals of `doMergeJoin`'s body
    /// (`:557`).
    has_match: bool,
    has_null: bool,

    /// Go `outerMergeWorker.batchSize` / `maxBatchSize` (`:128`).
    batch_size: usize,
    max_batch_size: usize,
    /// Go `requiredRows` (`:65`).
    required_rows: usize,
    /// Go `prepared` (`:76`).
    prepared: bool,
    /// Whether the outer child is drained.
    outer_done: bool,
}

impl<C: Columns> IndexLookUpMergeJoin<C> {
    /// Builds the join. Mirrors what Go's plan-to-executor builder fills into
    /// the struct literal plus `newOuterWorker` / `newInnerMergeWorker`
    /// (`:202`, `:221`), minus the worker plumbing.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        meta: ExecutorMeta,
        outer_exec: Box<dyn Executor>,
        outer_ctx: OuterMergeCtx,
        inner_ctx: InnerMergeCtx,
        reader_builder: Box<dyn IndexJoinExecutorBuilder>,
        joiner: Box<dyn Joiner>,
        is_outer_join: bool,
        index_ranges: Vec<IndexRange>,
        key_off_to_idx_off: Vec<usize>,
        ctx: C,
    ) -> Self {
        IndexLookUpMergeJoin {
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
            outer_at: 0,
            phase: RowPhase::NotStarted,
            has_match: false,
            has_null: false,
            batch_size: INITIAL_MERGE_BATCH_SIZE,
            max_batch_size: DEFAULT_INDEX_JOIN_BATCH_SIZE,
            required_rows: 0,
            prepared: false,
            outer_done: false,
        }
    }

    /// Go `LastColHelper` (`:73`), narrowed to its comparison.
    #[must_use]
    pub fn with_last_col_comparator(mut self, cmp: Box<dyn LastColComparator>) -> Self {
        self.last_col_comparator = Some(cmp);
        self
    }

    /// Go `SessionVars.IndexJoinBatchSize` (`newOuterWorker` :211).
    #[must_use]
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size.max(1);
        self
    }

    /// Go `outerMergeWorker.increaseBatchSize` (`index_lookup_merge_join.go:384`).
    fn increase_batch_size(&mut self) {
        if self.batch_size < self.max_batch_size {
            self.batch_size *= 2;
        }
        if self.batch_size > self.max_batch_size {
            self.batch_size = self.max_batch_size;
        }
    }

    /// Go `innerMergeWorker.compare` (`index_lookup_merge_join.go:630`).
    ///
    /// Compares one outer row against one inner row on the join keys, in
    /// `KeyOff2KeyOffOrderByIdx` order, stopping at the first difference. Go
    /// discards the `isNull` flag of `CompareFunc` (`cmp, _, err`), so NULL
    /// takes whatever position [`Datum`]'s own ordering gives it, on both
    /// sides alike.
    fn compare(&self, outer: RowPtr, inner: RowPtr, task: &LookUpMergeJoinTask) -> Ordering {
        let outer_row = task.outer_result.get_row(outer);
        let inner_row = task.inner_result.get_row(inner);
        for &key_off in &self.inner_ctx.key_off_to_key_off_order_by_idx {
            let outer_col = self.outer_ctx.key_cols[key_off];
            let inner_col = self.inner_ctx.key_cols[key_off];
            let outer_value = outer_row.get_datum(outer_col, &self.outer_ctx.row_types[outer_col]);
            let inner_value = inner_row.get_datum(inner_col, &self.inner_ctx.row_types[inner_col]);
            let collation = self
                .inner_ctx
                .key_collators
                .get(key_off)
                .copied()
                .unwrap_or(Collation::Binary);
            // Go returns `(int(cmp), err)` and its callers treat an error as a
            // loop terminator; here an unorderable pair compares equal, which
            // can only merge two rows into the same key run -- never invent a
            // row outside it.
            match outer_value.compare(&inner_value, collation) {
                Ok(Ordering::Equal) | Err(_) => {}
                Ok(other) => return other,
            }
        }
        Ordering::Equal
    }

    /// Go's outer-side sort comparator inside `handleTask` (`:452`-`:475`).
    fn compare_outer(&self, left: RowPtr, right: RowPtr, task: &LookUpMergeJoinTask) -> Ordering {
        let left_row = task.outer_result.get_row(left);
        let right_row = task.outer_result.get_row(right);
        let mut cmp = Ordering::Equal;
        for &key_off in &self.inner_ctx.key_off_to_key_off_order_by_idx {
            let col = self.outer_ctx.key_cols[key_off];
            let field_type = &self.outer_ctx.row_types[col];
            let left_value = left_row.get_datum(col, field_type);
            let right_value = right_row.get_datum(col, field_type);
            let collation = self
                .inner_ctx
                .key_collators
                .get(key_off)
                .copied()
                .unwrap_or(Collation::Binary);
            // Go `terror.Log(err)` then keeps `c` at its previous value; an
            // errored column therefore compares equal, as here.
            cmp = left_value
                .compare(&right_value, collation)
                .unwrap_or(Ordering::Equal);
            if cmp != Ordering::Equal {
                break;
            }
        }
        if cmp != Ordering::Equal || self.last_col_comparator.is_none() {
            return if self.inner_ctx.desc {
                cmp.reverse()
            } else {
                cmp
            };
        }
        let cmp = self
            .last_col_comparator
            .as_ref()
            .expect("checked")
            .compare_row(left_row, right_row);
        if self.inner_ctx.desc {
            cmp.reverse()
        } else {
            cmp
        }
    }

    /// Go `outerMergeWorker.buildTask` (`index_lookup_merge_join.go:346`).
    ///
    /// Returns `None` for Go's `return nil, nil` -- the outer side produced no
    /// row, so there is no task at all. Go's `(task, err)` pair with a non-nil
    /// task exists only to carry the error to the main thread over `resultCh`
    /// (`run` :315); `?` carries it here.
    fn build_task(&mut self) -> Result<Option<LookUpMergeJoinTask>, ExecError> {
        let max_chunk_size = self.outer_exec.max_chunk_size().max(1);
        let init_cap = self.outer_exec.init_cap().max(1);
        let mut task =
            LookUpMergeJoinTask::new(&self.outer_ctx.row_types, init_cap, max_chunk_size);

        self.increase_batch_size();
        let mut required_rows = self.batch_size;
        if self.is_outer_join {
            // Go `:357`. Sequentially this is always the *current* `Next`'s
            // value; see the module header.
            required_rows = self.required_rows;
        }
        if required_rows == 0 || required_rows > self.max_batch_size {
            // Go `:359`: `requiredRows <= 0 || requiredRows > maxBatchSize`.
            required_rows = self.max_batch_size;
        }

        while required_rows > 0 {
            // Go `exec.TryNewCacheChunk(omw.executor)`; unlike
            // `index_lookup_join.go:466` the merge variant sets no
            // `RequiredRows` on it, so a full chunk may overshoot the batch --
            // reproduced, since it changes which rows land in which batch and
            // therefore (with `NeedOuterSort`) the output order.
            let mut chk = Chunk::new(&self.outer_ctx.row_types, init_cap, max_chunk_size);
            self.outer_exec.next(&mut chk)?;
            let rows = chk.num_rows();
            if rows == 0 {
                self.outer_done = true;
                break;
            }
            task.outer_result.add(chk);
            required_rows = required_rows.saturating_sub(rows);
        }

        if task.outer_result.is_empty() {
            return Ok(None);
        }
        Ok(Some(task))
    }

    /// Go `innerMergeWorker.constructDatumLookupKey`
    /// (`index_lookup_merge_join.go:658`).
    ///
    /// Returns `None` for every reason Go returns a nil key: the outer filter
    /// rejected the row, a NULL join key (which an equi-join can never match),
    /// and the conversion outcomes that prove no inner row can match.
    fn construct_datum_lookup_key(
        &self,
        task: &LookUpMergeJoinTask,
        idx: RowPtr,
    ) -> Result<Option<IndexJoinLookUpContent>, ExecError> {
        if let Some(outer_match) = &task.outer_match {
            if !outer_match[idx.chk_idx as usize][idx.row_idx as usize] {
                return Ok(None);
            }
        }
        let outer_row = task.outer_result.get_row(idx);
        let key_len = self.inner_ctx.key_cols.len();
        let mut lookup_key = Vec::with_capacity(key_len);
        for (i, &key_col) in self.outer_ctx.key_cols.iter().enumerate() {
            let outer_value = outer_row.get_datum(key_col, &self.outer_ctx.row_types[key_col]);
            // Go `:671`: the join-on condition is an equality, so a NULL outer
            // value matches nothing and need not be looked up.
            if outer_value.is_null() {
                return Ok(None);
            }
            let inner_col_type = &self.inner_ctx.row_types[self.inner_ctx.key_cols[i]];
            // boundary: `stmtctx.StatementContext.TypeCtx`
            // Go passes `sc.TypeCtx()`, whose flags and zone decide whether a
            // truncation is an error or a warning. Neither is reachable from
            // `Columns`; the default flags are used, which is Go's non-strict
            // shape.
            let converted = match outer_value.convert_to(inner_col_type, ConversionFlags::default())
            {
                Ok(converted) => converted.value,
                Err(_) => {
                    // Go distinguishes `ErrOverflow` / `ErrWarnDataOutOfRange`
                    // (skip the lookup, `:679`) and `ErrTruncated` into
                    // SET/ENUM (skip, `:681`) from a genuine error (propagate,
                    // `:684`). The datum layer here reports one error type for
                    // all of them; skipping is the shape that cannot produce a
                    // wrong extra row, and the outer row still reaches the
                    // joiner as a miss.
                    return Ok(None);
                }
            };
            let collation = self
                .inner_ctx
                .key_collators
                .get(i)
                .copied()
                .unwrap_or(Collation::Binary);
            let cmp = outer_value.compare(&converted, collation).map_err(|err| {
                ExecError::internal(format!("index merge join key compare: {err:?}"))
            })?;
            if cmp != Ordering::Equal {
                // Go `:690`: the converted value is not the original one, so
                // no inner row can equal it.
                return Ok(None);
            }
            lookup_key.push(converted);
        }
        Ok(Some(IndexJoinLookUpContent {
            keys: lookup_key,
            row: outer_row.copy_construct(),
            key_cols: self.inner_ctx.key_cols.clone(),
            key_col_ids: self.inner_ctx.key_col_ids.clone(),
        }))
    }

    /// Go `innerMergeWorker.constructDatumLookupKeys`
    /// (`index_lookup_merge_join.go:641`): over `outerOrderIdx`, in its order.
    fn construct_datum_lookup_keys(
        &self,
        task: &LookUpMergeJoinTask,
    ) -> Result<Vec<IndexJoinLookUpContent>, ExecError> {
        let mut contents = Vec::with_capacity(task.outer_order_idx.len());
        for &idx in &task.outer_order_idx {
            if let Some(content) = self.construct_datum_lookup_key(task, idx)? {
                contents.push(content);
            }
        }
        Ok(contents)
    }

    /// Go `innerMergeWorker.dedupDatumLookUpKeys`
    /// (`index_lookup_merge_join.go:703`).
    ///
    /// Note there is no sort here, unlike `index_lookup_join.go:703`: the
    /// contents are already in key order, because `outerOrderIdx` either came
    /// from an outer side the plan proved sorted or was just sorted by
    /// `handleTask`. Dedup therefore only has to compare **adjacent** entries.
    fn dedup_datum_lookup_keys(
        &self,
        contents: Vec<IndexJoinLookUpContent>,
    ) -> Vec<IndexJoinLookUpContent> {
        if contents.len() < 2 {
            return contents;
        }
        let collators = &self.inner_ctx.key_collators;
        let mut deduped: Vec<IndexJoinLookUpContent> = Vec::with_capacity(contents.len());
        for content in contents {
            let distinct = match deduped.last() {
                None => true,
                Some(prev) => {
                    compare_datum_row(&content.keys, &prev.keys, collators) != Ordering::Equal
                        || self.last_col_comparator.as_ref().is_some_and(|last| {
                            last.compare_row(content.row.as_row(), prev.row.as_row())
                                != Ordering::Equal
                        })
                }
            };
            if distinct {
                deduped.push(content);
            }
        }
        deduped
    }

    /// Go `innerMergeWorker.fetchNextInnerResult`
    /// (`index_lookup_merge_join.go:719`).
    ///
    /// Appends rather than replaces -- see the module header's chunk-retention
    /// boundary. `inner_drained` is Go's `task.innerResult.NumRows() == 0`.
    fn fetch_next_inner_result(&self, task: &mut LookUpMergeJoinTask) -> Result<(), ExecError> {
        let Some(inner_exec) = task.inner_exec.as_mut() else {
            task.inner_drained = true;
            return Ok(());
        };
        let mut chk = task.inner_result.alloc_chunk();
        inner_exec.next(&mut chk)?;
        if chk.num_rows() == 0 {
            task.inner_drained = true;
            return Ok(());
        }
        task.inner_drained = false;
        task.inner_ptr = RowPtr {
            chk_idx: u32::try_from(task.inner_result.num_chunks()).unwrap_or(u32::MAX),
            row_idx: 0,
        };
        task.inner_result.add(chk);
        Ok(())
    }

    /// Go `innerMergeWorker.handleTask` (`index_lookup_merge_join.go:424`)
    /// down to but not including `doMergeJoin` (`:501`), which is the
    /// suspendable loop below.
    fn handle_task(&mut self, task: &mut LookUpMergeJoinTask) -> Result<(), ExecError> {
        let num_outer_chunks = task.outer_result.num_chunks();

        // `:426`-`:437`. Go uses `expression.VectorizedFilter`; the row-based
        // `EvalBool` it falls back to is what `crate::joiner::eval_bool`
        // ports, and the two agree row for row.
        if !self.outer_ctx.filter.is_empty() {
            let mut outer_match = Vec::with_capacity(num_outer_chunks);
            for i in 0..num_outer_chunks {
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

        // `:439`-`:445`: outer-scan order.
        task.outer_order_idx = Vec::with_capacity(task.outer_result.len());
        for i in 0..num_outer_chunks {
            for j in 0..task.outer_result.get_chunk(i).num_rows() {
                task.outer_order_idx.push(RowPtr {
                    chk_idx: u32::try_from(i).unwrap_or(u32::MAX),
                    row_idx: u32::try_from(j).unwrap_or(u32::MAX),
                });
            }
        }

        // `:450`-`:476`: the one place the emission order stops being
        // outer-scan order. See the module header.
        if self.outer_ctx.need_outer_sort {
            let mut order = std::mem::take(&mut task.outer_order_idx);
            // Go uses `slices.SortFunc`, which is NOT stable. Rust's `sort_by`
            // is, so ties between outer rows with equal keys (and equal
            // `nextColCompareFilters` verdict) keep outer-scan order here
            // instead of an arbitrary one. That is one specific member of the
            // set of orders Go may produce, and the row multiset is identical.
            order.sort_by(|&left, &right| self.compare_outer(left, right, task));
            task.outer_order_idx = order;
        }

        // `:477`-`:481`.
        let contents = self.construct_datum_lookup_keys(task)?;
        let mut contents = self.dedup_datum_lookup_keys(contents);
        // `:484`-`:489`: the deduped contents are in descending order when
        // `Desc`, but a range must be built ascending, so they are reversed
        // for the reader only. `outerOrderIdx` is deliberately NOT reversed --
        // the emission order is untouched by this.
        if self.inner_ctx.desc {
            contents.reverse();
        }

        // `:490`. `canReorderHandles` is `false`: this join needs the inner
        // rows in index order, which is exactly what the plain lookup join
        // (which passes `true`) does not.
        let mut inner_exec = self.reader_builder.build_executor_for_index_join(
            &contents,
            &self.index_ranges,
            &self.key_off_to_idx_off,
            false,
        )?;
        task.inner_result = List::new(
            inner_exec.ret_field_types(),
            inner_exec.init_cap().max(1),
            inner_exec.max_chunk_size().max(1),
        );
        inner_exec.open()?;
        task.inner_exec = Some(inner_exec);

        // `:497`: the first fetch, which seats `innerIter` at `Begin()`.
        self.fetch_next_inner_result(task)
    }

    /// Go `innerMergeWorker.fetchInnerRowsWithSameKey`
    /// (`index_lookup_merge_join.go:608`).
    ///
    /// Collects the run of inner rows whose key equals the outer row's,
    /// advancing the inner cursor past every inner row that sorts at or before
    /// it. On return `task.inner_drained` is Go's `noneInnerRows`.
    fn fetch_inner_rows_with_same_key(
        &self,
        task: &mut LookUpMergeJoinTask,
        outer: RowPtr,
    ) -> Result<(), ExecError> {
        task.same_key_inner_rows.clear();
        let desc = self.inner_ctx.desc;
        while let Some(cur) = task.inner_current() {
            let cmp = self.compare(outer, cur, task);
            // Go `:612`: `(cmpRes >= 0 && !desc) || (cmpRes <= 0 && desc)`.
            let keep_going = if desc {
                cmp != Ordering::Greater
            } else {
                cmp != Ordering::Less
            };
            if !keep_going {
                break;
            }
            if cmp == Ordering::Equal {
                task.same_key_inner_rows.push(cur);
            }
            if !task.advance_inner() {
                self.fetch_next_inner_result(task)?;
                if task.inner_drained {
                    break;
                }
            }
        }
        // Go `:624`-`:625`: `NewIterator4Slice(...)` then `Begin()`.
        task.same_key_at = 0;
        Ok(())
    }

    /// One suspendable step of Go `doMergeJoin`'s body
    /// (`index_lookup_merge_join.go:555`-`:602`), for the outer row at
    /// `outer_at`.
    ///
    /// Go's chunk hand-off (`fetchNewChunkWhenFull` :505, which pushes a full
    /// chunk to `task.results` and takes a recycled one from
    /// `joinChkResourceCh`) has no counterpart: results are written straight
    /// into the caller's `req`, and the caller's next call resumes here.
    fn step_outer_row(
        &mut self,
        task: &mut LookUpMergeJoinTask,
        req: &mut Chunk,
    ) -> Result<(), ExecError> {
        let outer_idx = task.outer_order_idx[self.outer_at];
        let desc = self.inner_ctx.desc;

        if self.phase == RowPhase::NotStarted {
            self.has_match = false;
            self.has_null = false;
            // Go `:549`-`:552`: `initCmpResult` is 1, or -1 when descending.
            let mut cmp = if desc {
                Ordering::Less
            } else {
                Ordering::Greater
            };
            let filtered_out = task.outer_match.as_ref().is_some_and(|outer_match| {
                !outer_match[outer_idx.chk_idx as usize][outer_idx.row_idx as usize]
            });
            // Go `:558`: `goto missMatch`.
            let mut missed = filtered_out;
            // Go `:563`: it has iterated out all inner rows and holds no
            // same-key run, so this outer row can match nothing.
            if !missed && task.inner_drained && task.same_key_inner_rows.is_empty() {
                missed = true;
            }
            if !missed {
                if !task.same_key_inner_rows.is_empty() {
                    // Go `:567`: `compare(outerRow, task.sameKeyIter.Begin())`.
                    // `Begin()` REWINDS the iterator, which is what lets two
                    // consecutive outer rows with the same key each see the
                    // whole run.
                    let first = task.same_key_inner_rows[0];
                    cmp = self.compare(outer_idx, first, task);
                    task.same_key_at = 0;
                }
                // Go `:572`: the outer key is past the held run, so a new run
                // must be fetched.
                let needs_fetch = if desc {
                    cmp == Ordering::Less
                } else {
                    cmp == Ordering::Greater
                };
                if needs_fetch {
                    if task.inner_drained {
                        // Go `:574`.
                        task.same_key_inner_rows.clear();
                        task.same_key_at = 0;
                        missed = true;
                    } else {
                        self.fetch_inner_rows_with_same_key(task, outer_idx)?;
                    }
                }
            }
            self.phase = if missed {
                RowPhase::Missed
            } else {
                RowPhase::Joining
            };
        }

        if self.phase == RowPhase::Joining {
            // Go `:583`: `for task.sameKeyIter.Current() != task.sameKeyIter.End()`.
            let remaining = task.same_key_inner_rows.len() - task.same_key_at;
            let budget = req.required_rows().saturating_sub(req.num_rows());
            if remaining > 0 && budget > 0 {
                let ptrs: Vec<RowPtr> = task.same_key_inner_rows[task.same_key_at..].to_vec();
                let outer_row = task.outer_result.get_row(outer_idx);
                let mut inners = LendingIterator::row_ptrs(&task.inner_result, ptrs);
                inners.begin();
                let (matched, is_null) = self.joiner.try_to_match_inners(
                    outer_row,
                    &mut inners,
                    req,
                    NAAJType::Unknown,
                )?;
                // Same exhaustion rule as `crate::index_lookup_join`: a `None`
                // current means the joiner drained the batch or called
                // `ReachEnd` (the semi family settling the outer row on its
                // first match). A non-`None` current can only come from the
                // row joiner's `budget` break, which consumed exactly `budget`.
                if inners.current().is_none() {
                    task.same_key_at = task.same_key_inner_rows.len();
                } else {
                    task.same_key_at += budget;
                }
                self.has_match |= matched;
                self.has_null |= is_null;
            }
        }

        let row_done =
            self.phase == RowPhase::Missed || task.same_key_at >= task.same_key_inner_rows.len();
        if row_done {
            // Go `missMatch:` (`:595`).
            if !self.has_match {
                let outer_row = task.outer_result.get_row(outer_idx);
                self.joiner.on_miss_match(self.has_null, outer_row, req);
            }
            self.outer_at += 1;
            self.phase = RowPhase::NotStarted;
        }
        Ok(())
    }

    /// Releases the current task and its reader -- Go's `defer` in `handleTask`
    /// (`:492`).
    fn finish_task(&mut self) -> Result<(), ExecError> {
        let closed = match self.task.as_mut().and_then(|task| task.inner_exec.take()) {
            Some(mut inner_exec) => inner_exec.close(),
            None => Ok(()),
        };
        self.task = None;
        self.outer_at = 0;
        self.phase = RowPhase::NotStarted;
        self.has_match = false;
        self.has_null = false;
        closed
    }
}

impl<C: Columns> Executor for IndexLookUpMergeJoin<C> {
    /// Go `Open` (`index_lookup_merge_join.go:162`).
    ///
    /// Go's body is `exec.Open(child)` plus attaching the memory tracker; only
    /// the first survives. The length checks are local: they refuse a
    /// misconfigured join loudly instead of indexing out of bounds inside the
    /// merge loop.
    fn open(&mut self) -> Result<(), ExecError> {
        if self.outer_ctx.key_cols.len() != self.inner_ctx.key_cols.len() {
            return Err(ExecError::internal(
                "index lookup merge join: outer and inner key column counts must match",
            ));
        }
        if self
            .inner_ctx
            .key_off_to_key_off_order_by_idx
            .iter()
            .any(|&off| off >= self.inner_ctx.key_cols.len())
        {
            return Err(ExecError::internal(
                "index lookup merge join: key order permutation out of range",
            ));
        }
        self.outer_exec.open()?;
        self.task = None;
        self.outer_at = 0;
        self.phase = RowPhase::NotStarted;
        self.has_match = false;
        self.has_null = false;
        self.batch_size = INITIAL_MERGE_BATCH_SIZE;
        self.prepared = false;
        self.outer_done = false;
        Ok(())
    }

    /// Go `Next` (`index_lookup_merge_join.go:254`), with the `resultCh` /
    /// `task.results` round-trips replaced by building and merging the next
    /// task inline. See the module header for why that leaves the order alone.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if !self.prepared {
            // Go `startWorkers` (`:172`), narrowed to the part that has an
            // effect on rows: nothing. The batch seed is the literal 32 of
            // `newOuterWorker` (`:210`), already set.
            self.prepared = true;
        }
        if self.is_outer_join {
            // Go `:260`.
            self.required_rows = req.required_rows();
        }
        req.reset();
        loop {
            if self.task.is_none() {
                if self.outer_done {
                    return Ok(());
                }
                let Some(mut task) = self.build_task()? else {
                    return Ok(());
                };
                self.handle_task(&mut task)?;
                self.task = Some(task);
                self.outer_at = 0;
                self.phase = RowPhase::NotStarted;
                self.has_match = false;
                self.has_null = false;
            }

            let mut task = self.task.take().expect("just set");
            if self.outer_at >= task.outer_order_idx.len() {
                self.task = Some(task);
                self.finish_task()?;
                continue;
            }
            let stepped = self.step_outer_row(&mut task, req);
            self.task = Some(task);
            stepped?;

            if req.is_full() {
                return Ok(());
            }
        }
    }

    /// Go `Close` (`index_lookup_merge_join.go:728`).
    ///
    /// Go cancels the workers, drains `resultCh` and waits on `WorkerWg`; with
    /// no workers, closing the live inner reader and the outer child is all
    /// that is left.
    fn close(&mut self) -> Result<(), ExecError> {
        let closed = self.finish_task();
        self.prepared = false;
        self.outer_done = false;
        closed.and(self.outer_exec.close())
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
mod tests;
