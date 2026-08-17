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

//! `pkg/executor/join/index_lookup_hash_join.go`: `IndexNestedLoopHashJoin` --
//! the hash variant of the index-nested-loop join.
//!
//! # What makes it the *hash* variant
//!
//! [`crate::index_lookup_join`] builds its hash table on the **inner** rows and
//! probes it once per outer row. This file inverts that: it builds the table
//! on the **outer** batch (`buildHashTableForOuterResult` :631) and probes it
//! once per *inner* row. That inversion is the whole point -- it lets one pass
//! over the inner result settle every outer row, so the inner side can be
//! streamed in bounded chunks instead of being fully materialised.
//!
//! Go embeds `IndexLookUpJoin` (`:69`) and reuses its outer batching, lookup
//! key construction and inner fetch verbatim. So does this port: the
//! [`IndexLookUpJoin`] value in [`IndexNestedLoopHashJoin::base`] IS the
//! embedded struct, and `build_task` / `construct_lookup_content` /
//! `fetch_inner_results` are called on it rather than restated. Only the two
//! join loops are new.
//!
//! # The two modes, and the order guarantee
//!
//! Go's own doc comment (`:56`) says: *"The output order is not promised."*
//! That is the DEFAULT. The executor carries an explicit `KeepOuterOrder`
//! field, and `Next` (`:246`) branches on it:
//!
//! * **`KeepOuterOrder == false`** -> `runUnordered` (`:281`), served by
//!   `doJoinUnordered` (`:780`). It walks the INNER rows and calls
//!   `TryToMatchOuters` for each, so output is grouped by inner row, and the
//!   unmatched outer rows are flushed only at the very end, after the inner
//!   side is drained. Even single-threaded this does not preserve outer order.
//!   With N inner workers, results additionally arrive on a shared `resultCh`
//!   in completion order, so tasks interleave too.
//! * **`KeepOuterOrder == true`** -> `runInOrder` (`:258`), served by
//!   `doJoinInOrder` (`:918`). It first collects, per outer row, the inner row
//!   pointers that matched (`collectMatchedInnerPtrs4OuterRows` :899), then
//!   walks the outer rows IN ORDER calling `TryToMatchInners`/`OnMissMatch`.
//!   Across tasks, order is held by `taskCh` (one producer, FIFO) plus a
//!   per-task `resultCh` that `runInOrder` drains to completion before taking
//!   the next task.
//!
//! So: outer order is preserved **iff `KeepOuterOrder`**, and this port
//! reproduces both shapes, including the unordered one's inner-major output
//! order. Determined by reading `Next` :246 and the two loops, not assumed --
//! and cross-checked against the fact that the plain `IndexLookUpJoin` needs
//! no such field precisely because its shape orders it for free.
//!
//! # Sequential here, worker-parallel there
//!
//! Go's topology is one `indexHashJoinOuterWorker` plus N
//! `indexHashJoinInnerWorker`s, and *within* one inner worker a further
//! goroutine that builds the outer hash table concurrently with the inner
//! fetch (`handleTask` :723: `go util.WithRecovery(... buildHashTableForOuterResult ...)`
//! then `constructLookupContent` + `fetchInnerResults`, then `wg.Wait()`).
//! Both are collapsed here: the hash table is built, then the inner fetched,
//! then the join run.
//!
//! Observable consequences:
//!
//! * **`KeepOuterOrder == true`: none.** The rows and their order are fully
//!   determined, and this port produces them.
//! * **`KeepOuterOrder == false`: the output order becomes deterministic** --
//!   task-major, then inner-major, then by outer row within an inner row.
//!   That is one specific member of the set of orders Go is allowed to
//!   produce (it is exactly what Go produces with concurrency 1), and the
//!   *row multiset* is identical. Since Go promises no order here, no
//!   guarantee is broken; a caller that depended on Go's interleaving was
//!   already depending on nothing.
//! * `joinChkResourceCh` (the recycled result-chunk pool), `resultCh`,
//!   `panicErr`, `finishJoinWorkers`, `wait4JoinWorkers` and the per-worker
//!   `Joiners []Joiner` slice all exist to make N workers safe. One joiner is
//!   enough here, and results are written straight into the caller's `req`.
//!
//! # Narrowings
//!
//! * `codec.HashChunkRow` + `codec.EqualChunkRow` (`buildHashTableForOuterResult`
//!   :663, `getMatchedOuterRows` :828) are replaced by an exact encoded-key
//!   map, reusing the `codec.EncodeKey` bytes that
//!   [`IndexLookUpJoin::construct_lookup_content`] already computes into
//!   `task.encoded_lookup_keys`. Go hashes the RAW outer datums and then
//!   verifies equality across the two sides' `HashTypes`; keying on the
//!   already-converted lookup key does the same job -- an entry is found only
//!   when the outer value converts to the inner type exactly, which is the
//!   condition `constructDatumLookupKey` (`:697`) already enforces. This makes
//!   the outer hash table and the plain lookup join's inner one the same
//!   construction, which is the point.
//! * `BaseHashTable`/`newUnsafeHashTable` become a [`HashMap`] of encoded key
//!   to outer [`RowPtr`]s, insertion-ordered per key.
//! * Runtime stats, memory tracking, failpoints and `SQLKiller`/`Finished`
//!   polling are dropped, as in [`crate::index_lookup_join`].

use std::collections::HashMap;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::iterator::LendingIterator;
use tidb_chunk::list::RowPtr;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

use crate::executor::{ExecError, Executor};
use crate::index_lookup_join::{IndexJoinLookUpContent, IndexLookUpJoin, LookUpJoinTask};
use crate::joiner::{JoinType, NAAJType, OuterRowStatusFlag};

/// Go `maxRowsPerFetch` (`index_lookup_hash_join.go:53`).
pub const MAX_ROWS_PER_FETCH: usize = 4096;

/// Where the join loop is inside the current task.
///
/// Go needs no such enum: each worker runs the loop to completion on its own
/// goroutine and pushes finished chunks. Here the loop must suspend whenever
/// the caller's `req` fills, so its position is explicit state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// No current task.
    Idle,
    /// Go `doJoinUnordered` (`:780`), first loop: walk the inner rows.
    UnorderedProbe,
    /// Go `doJoinUnordered` (`:797`), second loop: flush the outer rows that
    /// no inner row matched. Only reached once the inner side is drained.
    UnorderedMisses,
    /// Go `doJoinInOrder` (`:918`), second phase: walk the outer rows.
    OrderedJoin,
}

/// Go `IndexNestedLoopHashJoin` (`index_lookup_hash_join.go:68`).
pub struct IndexNestedLoopHashJoin<C: Columns> {
    /// Go's embedded `IndexLookUpJoin` (`:69`). Its outer batching, lookup
    /// content construction and inner fetch are used unchanged; its probe loop
    /// is not.
    base: IndexLookUpJoin<C>,
    /// Go `KeepOuterOrder` (`:74`).
    keep_outer_order: bool,

    /// Go `curTask` (`:75`), plus the per-task state the workers hold.
    task: Option<LookUpJoinTask>,
    /// Go `indexHashJoinTask.lookupMap` (`:126`): the OUTER hash table.
    outer_hash: HashMap<Vec<u8>, Vec<RowPtr>>,
    /// Go `indexHashJoinTask.outerRowStatus` (`:122`).
    outer_row_status: Vec<Vec<OuterRowStatusFlag>>,
    /// Go `indexHashJoinTask.matchedInnerRowPtrs` (`:136`); only built when
    /// [`Self::keep_outer_order`].
    matched_inner_row_ptrs: Vec<Vec<Vec<RowPtr>>>,
    /// The lookup contents of the current task, kept for Go's incremental
    /// re-fetch (`handleTask` :765).
    lookup_contents: Vec<IndexJoinLookUpContent>,

    phase: Phase,
    /// `UnorderedProbe`: the inner row being probed.
    inner_cursor: RowPtr,
    /// `UnorderedProbe`: the outer rows matched by that inner row, and how
    /// many of them the joiner has consumed (Go `matchedOuterRowPtr` + the
    /// `cursor` of `joinMatchedInnerRow2Chunk` :869).
    matched_outer: Vec<RowPtr>,
    matched_outer_at: usize,
    /// `UnorderedMisses` / `OrderedJoin`: the outer row being flushed.
    outer_cursor: RowPtr,
    /// `OrderedJoin`: how many of the current outer row's matched inners the
    /// joiner has consumed, and its accumulated verdict.
    ordered_inner_at: usize,
    has_match: bool,
    has_null: bool,
    /// Scratch for `Joiner::try_to_match_outers`.
    outer_row_status_buf: Vec<OuterRowStatusFlag>,
}

impl<C: Columns> IndexNestedLoopHashJoin<C> {
    /// Builds the hash variant over an already-configured [`IndexLookUpJoin`],
    /// which is Go's embedding (`:69`).
    #[must_use]
    pub fn new(base: IndexLookUpJoin<C>, keep_outer_order: bool) -> Self {
        let mut join = IndexNestedLoopHashJoin {
            base,
            keep_outer_order,
            task: None,
            outer_hash: HashMap::new(),
            outer_row_status: Vec::new(),
            matched_inner_row_ptrs: Vec::new(),
            lookup_contents: Vec::new(),
            phase: Phase::Idle,
            inner_cursor: RowPtr::default(),
            matched_outer: Vec::new(),
            matched_outer_at: 0,
            outer_cursor: RowPtr::default(),
            ordered_inner_at: 0,
            has_match: false,
            has_null: false,
            outer_row_status_buf: Vec::new(),
        };
        join.base.max_fetch_size = if join.support_incremental_lookup() {
            MAX_ROWS_PER_FETCH
        } else {
            0
        };
        join
    }

    /// Go `supportIncrementalLookUp` (`index_lookup_hash_join.go:476`).
    ///
    /// Only these four join types may see the inner side in pieces: each of
    /// them decides an outer row from the inner rows it has seen SO FAR
    /// monotonically (a match can only be added, never retracted), so a
    /// partial inner batch cannot produce a wrong verdict. The semi and
    /// left-outer-semi families are excluded because their answer depends on
    /// having seen the whole inner side.
    fn support_incremental_lookup(&self) -> bool {
        !self.keep_outer_order
            && matches!(
                self.base.joiner.join_type(),
                JoinType::Inner
                    | JoinType::LeftOuter
                    | JoinType::RightOuter
                    | JoinType::AntiSemiJoin
            )
    }

    /// Go `indexHashJoinInnerWorker.buildHashTableForOuterResult` (`:631`),
    /// keyed on the encoded lookup key rather than a 64-bit hash; see the
    /// module header.
    ///
    /// The NULL and `outerMatch` skips Go performs here are already folded
    /// into `encoded_lookup_keys`: `constructDatumLookupKey` (`:655`, `:665`)
    /// appends a NULL cell for exactly those rows.
    fn build_hash_table_for_outer_result(&mut self, task: &LookUpJoinTask) {
        self.outer_hash.clear();
        self.outer_row_status.clear();
        for chk_idx in 0..task.outer_result.num_chunks() {
            let num_rows = task.outer_result.get_chunk(chk_idx).num_rows();
            self.outer_row_status
                .push(vec![OuterRowStatusFlag::Unmatched; num_rows]);
            let keys = &task.encoded_lookup_keys[chk_idx];
            for row_idx in 0..num_rows {
                let key_row = keys.get_row(row_idx);
                if key_row.is_null(0) {
                    continue;
                }
                let ptr = RowPtr {
                    chk_idx: u32::try_from(chk_idx).unwrap_or(u32::MAX),
                    row_idx: u32::try_from(row_idx).unwrap_or(u32::MAX),
                };
                self.outer_hash
                    .entry(key_row.get_bytes(0).to_vec())
                    .or_default()
                    .push(ptr);
            }
        }
    }

    /// Go `indexHashJoinInnerWorker.getMatchedOuterRows` (`:822`).
    ///
    /// Go's null check, hash, and `EqualChunkRow` verification all collapse
    /// into one encoded-key lookup. The semi-join skip (`:849`) is kept
    /// verbatim: an outer row a semi join has already settled must not be
    /// offered a second matching inner row.
    fn matched_outer_rows(&self, task: &LookUpJoinTask, inner_row_idx: RowPtr) -> Vec<RowPtr> {
        let chk = task.inner_result.get_chunk(inner_row_idx.chk_idx as usize);
        let inner_row = chk.get_row(inner_row_idx.row_idx as usize);
        for (i, &col_idx) in self.base.inner_ctx.hash_cols.iter().enumerate() {
            if inner_row.is_null(col_idx)
                && !self
                    .base
                    .inner_ctx
                    .hash_is_null_eq
                    .get(i)
                    .copied()
                    .unwrap_or(false)
            {
                return Vec::new();
            }
        }
        let key_datums: Vec<_> = self
            .base
            .inner_ctx
            .hash_cols
            .iter()
            .map(|&col| inner_row.get_datum(col, &self.base.inner_ctx.row_types[col]))
            .collect();
        let Ok(key) = tidb_codec::encode_key(&key_datums) else {
            return Vec::new();
        };
        let Some(candidates) = self.outer_hash.get(&key) else {
            return Vec::new();
        };
        let is_semi_join = matches!(
            self.base.joiner.join_type(),
            JoinType::SemiJoin
                | JoinType::AntiSemiJoin
                | JoinType::LeftOuterSemiJoin
                | JoinType::AntiLeftOuterSemiJoin
        );
        candidates
            .iter()
            .copied()
            .filter(|ptr| {
                !(is_semi_join
                    && self.outer_row_status[ptr.chk_idx as usize][ptr.row_idx as usize]
                        == OuterRowStatusFlag::Matched)
            })
            .collect()
    }

    /// Go's per-task setup: `buildHashTableForOuterResult` +
    /// `constructLookupContent` + `fetchInnerResults` (`handleTask` :723-:747),
    /// which Go overlaps on two goroutines.
    fn start_task(&mut self) -> Result<bool, ExecError> {
        let Some(mut task) = self.base.build_task()? else {
            return Ok(false);
        };
        self.lookup_contents = self.base.construct_lookup_content(&mut task)?;
        self.build_hash_table_for_outer_result(&task);
        let contents = std::mem::take(&mut self.lookup_contents);
        let fetched = self.base.fetch_inner_results(&mut task, &contents);
        self.lookup_contents = contents;
        fetched?;

        if self.keep_outer_order {
            self.matched_inner_row_ptrs = (0..task.outer_result.num_chunks())
                .map(|i| vec![Vec::new(); task.outer_result.get_chunk(i).num_rows()])
                .collect();
            self.collect_matched_inner_ptrs(&task);
            self.phase = Phase::OrderedJoin;
        } else {
            self.phase = Phase::UnorderedProbe;
        }
        self.task = Some(task);
        self.inner_cursor = RowPtr::default();
        self.outer_cursor = RowPtr::default();
        self.matched_outer.clear();
        self.matched_outer_at = 0;
        self.ordered_inner_at = 0;
        self.has_match = false;
        self.has_null = false;
        Ok(true)
    }

    /// Go `collectMatchedInnerPtrs4OuterRows` (`:899`) over the whole inner
    /// result -- the first phase of `doJoinInOrder` (`:930`).
    fn collect_matched_inner_ptrs(&mut self, task: &LookUpJoinTask) {
        for i in 0..task.inner_result.num_chunks() {
            for j in 0..task.inner_result.get_chunk(i).num_rows() {
                let inner_ptr = RowPtr {
                    chk_idx: u32::try_from(i).unwrap_or(u32::MAX),
                    row_idx: u32::try_from(j).unwrap_or(u32::MAX),
                };
                for outer_ptr in self.matched_outer_rows(task, inner_ptr) {
                    self.matched_inner_row_ptrs[outer_ptr.chk_idx as usize]
                        [outer_ptr.row_idx as usize]
                        .push(inner_ptr);
                }
            }
        }
    }

    /// Whether `outer_cursor` has walked past the last outer row.
    fn outer_exhausted(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(|task| self.outer_cursor.chk_idx as usize >= task.outer_result.num_chunks())
    }

    /// Advances `outer_cursor` one row (Go's `range` over the two nested
    /// slices, made explicit so it can suspend).
    fn advance_outer(&mut self) {
        let Some(task) = &self.task else { return };
        self.outer_cursor.row_idx += 1;
        let rows = task
            .outer_result
            .get_chunk(self.outer_cursor.chk_idx as usize)
            .num_rows();
        if self.outer_cursor.row_idx as usize >= rows {
            self.outer_cursor.chk_idx += 1;
            self.outer_cursor.row_idx = 0;
        }
    }

    /// Advances `inner_cursor` one row.
    fn advance_inner(&mut self) {
        let Some(task) = &self.task else { return };
        self.inner_cursor.row_idx += 1;
        let rows = task
            .inner_result
            .get_chunk(self.inner_cursor.chk_idx as usize)
            .num_rows();
        if self.inner_cursor.row_idx as usize >= rows {
            self.inner_cursor.chk_idx += 1;
            self.inner_cursor.row_idx = 0;
        }
    }

    fn inner_exhausted(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(|task| self.inner_cursor.chk_idx as usize >= task.inner_result.num_chunks())
    }

    /// Go `joinMatchedInnerRow2Chunk` (`:855`), one suspendable step: offers
    /// the current inner row's still-unconsumed matched outer rows to the
    /// joiner.
    fn step_unordered_probe(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.matched_outer_at >= self.matched_outer.len() {
            // Move to the next inner row and collect its matches.
            if self.matched_outer_at > 0 {
                self.advance_inner();
            }
            loop {
                if self.inner_exhausted() {
                    return Ok(());
                }
                let task = self.task.as_ref().expect("task is current");
                self.matched_outer = self.matched_outer_rows(task, self.inner_cursor);
                self.matched_outer_at = 0;
                if !self.matched_outer.is_empty() {
                    break;
                }
                self.advance_inner();
            }
        }

        let ptrs: Vec<RowPtr> = self.matched_outer[self.matched_outer_at..].to_vec();
        let task = self.task.as_ref().expect("task is current");
        let inner_chk = task
            .inner_result
            .get_chunk(self.inner_cursor.chk_idx as usize);
        let inner_row = inner_chk.get_row(self.inner_cursor.row_idx as usize);
        let mut outers = LendingIterator::row_ptrs(&task.outer_result, ptrs);
        outers.begin();
        self.base.joiner.try_to_match_outers(
            &mut outers,
            inner_row,
            req,
            &mut self.outer_row_status_buf,
        )?;
        // Go reads how far it got from the length of `outerRowStatus` (`:874`).
        for (offset, status) in self.outer_row_status_buf.iter().enumerate() {
            let ptr = self.matched_outer[self.matched_outer_at + offset];
            let current = self.outer_row_status[ptr.chk_idx as usize][ptr.row_idx as usize];
            if *status == OuterRowStatusFlag::Matched || current == OuterRowStatusFlag::Unmatched {
                self.outer_row_status[ptr.chk_idx as usize][ptr.row_idx as usize] = *status;
            }
        }
        self.matched_outer_at += self.outer_row_status_buf.len();
        if self.outer_row_status_buf.is_empty() {
            // `req` was not full on entry, so the joiner had a non-zero budget
            // and must have consumed at least one outer row. Anything else
            // would spin this loop forever, so it is refused rather than
            // absorbed.
            return Err(ExecError::internal(
                "index nested loop hash join: joiner consumed no outer row",
            ));
        }
        Ok(())
    }

    /// Go `doJoinInOrder`'s second phase (`:940`), one suspendable step.
    fn step_ordered_join(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let chk_idx = self.outer_cursor.chk_idx as usize;
        let row_idx = self.outer_cursor.row_idx as usize;
        let remaining = self.matched_inner_row_ptrs[chk_idx][row_idx].len() - self.ordered_inner_at;
        let budget = req.required_rows().saturating_sub(req.num_rows());
        if remaining > 0 && budget > 0 {
            let ptrs: Vec<RowPtr> =
                self.matched_inner_row_ptrs[chk_idx][row_idx][self.ordered_inner_at..].to_vec();
            let task = self.task.as_ref().expect("task is current");
            let outer_row = task.outer_result.get_chunk(chk_idx).get_row(row_idx);
            let mut inners = LendingIterator::row_ptrs(&task.inner_result, ptrs);
            inners.begin();
            let (matched, is_null) = self.base.joiner.try_to_match_inners(
                outer_row,
                &mut inners,
                req,
                NAAJType::Unknown,
            )?;
            // Same exhaustion rule as `crate::index_lookup_join`: a `None`
            // current means the joiner drained the batch or reached its end.
            if inners.current().is_none() {
                self.ordered_inner_at = self.matched_inner_row_ptrs[chk_idx][row_idx].len();
            } else {
                self.ordered_inner_at += budget;
            }
            self.has_match |= matched;
            self.has_null |= is_null;
        }
        if self.ordered_inner_at >= self.matched_inner_row_ptrs[chk_idx][row_idx].len() {
            if !self.has_match {
                let task = self.task.as_ref().expect("task is current");
                let outer_row = task.outer_result.get_chunk(chk_idx).get_row(row_idx);
                self.base
                    .joiner
                    .on_miss_match(self.has_null, outer_row, req);
            }
            self.advance_outer();
            self.ordered_inner_at = 0;
            self.has_match = false;
            self.has_null = false;
        }
        Ok(())
    }

    /// Go `doJoinUnordered`'s trailing loop (`:797`): `OnMissMatch` for every
    /// outer row no inner row settled.
    fn step_unordered_misses(&mut self, req: &mut Chunk) {
        let chk_idx = self.outer_cursor.chk_idx as usize;
        let row_idx = self.outer_cursor.row_idx as usize;
        let status = self.outer_row_status[chk_idx][row_idx];
        if status != OuterRowStatusFlag::Matched {
            let task = self.task.as_ref().expect("task is current");
            let outer_row = task.outer_result.get_chunk(chk_idx).get_row(row_idx);
            self.base
                .joiner
                .on_miss_match(status == OuterRowStatusFlag::HasNull, outer_row, req);
        }
        self.advance_outer();
    }

    /// Go's incremental re-fetch (`handleTask` :765): the inner reader was not
    /// drained, so read the next bounded batch and probe again.
    fn refetch_inner(&mut self) -> Result<bool, ExecError> {
        let Some(task) = &self.task else {
            return Ok(false);
        };
        if task.inner_exec.is_none() {
            return Ok(false);
        }
        let mut task = self.task.take().expect("checked");
        let contents = std::mem::take(&mut self.lookup_contents);
        let fetched = self.base.fetch_inner_results(&mut task, &contents);
        self.lookup_contents = contents;
        let empty = task.inner_result.is_empty();
        self.task = Some(task);
        fetched?;
        self.inner_cursor = RowPtr::default();
        self.matched_outer.clear();
        self.matched_outer_at = 0;
        Ok(!empty)
    }

    /// Releases the current task and its reader.
    fn finish_task(&mut self) -> Result<(), ExecError> {
        if let Some(mut inner_exec) = self.task.as_mut().and_then(|t| t.inner_exec.take()) {
            inner_exec.close()?;
        }
        self.task = None;
        self.phase = Phase::Idle;
        self.outer_hash.clear();
        self.outer_row_status.clear();
        self.matched_inner_row_ptrs.clear();
        self.lookup_contents.clear();
        Ok(())
    }
}

impl<C: Columns> Executor for IndexNestedLoopHashJoin<C> {
    /// Go `Open` (`index_lookup_hash_join.go:138`).
    fn open(&mut self) -> Result<(), ExecError> {
        self.base.open()?;
        self.finish_task()
    }

    /// Go `Next` (`index_lookup_hash_join.go:246`) plus `runInOrder` (`:258`)
    /// / `runUnordered` (`:281`), with the channel hand-offs replaced by
    /// writing into `req` directly.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if !self.base.prepared {
            self.base.batch_size = req.required_rows().max(1).min(self.base.max_batch_size);
            self.base.prepared = true;
        }
        req.reset();
        loop {
            if self.phase == Phase::Idle {
                if self.base.outer_done {
                    return Ok(());
                }
                if !self.start_task()? {
                    return Ok(());
                }
            }
            match self.phase {
                Phase::Idle => return Ok(()),
                Phase::UnorderedProbe => {
                    if self.inner_exhausted() && self.matched_outer_at >= self.matched_outer.len() {
                        // Go only flushes the misses once `innerExec` is nil,
                        // i.e. the inner side is genuinely drained (`:794`).
                        if self.refetch_inner()? {
                            continue;
                        }
                        self.phase = Phase::UnorderedMisses;
                        self.outer_cursor = RowPtr::default();
                        continue;
                    }
                    self.step_unordered_probe(req)?;
                }
                Phase::UnorderedMisses => {
                    if self.outer_exhausted() {
                        self.finish_task()?;
                        continue;
                    }
                    self.step_unordered_misses(req);
                }
                Phase::OrderedJoin => {
                    if self.outer_exhausted() {
                        self.finish_task()?;
                        continue;
                    }
                    self.step_ordered_join(req)?;
                }
            }
            if req.is_full() {
                return Ok(());
            }
        }
    }

    /// Go `Close` (`index_lookup_hash_join.go:349`).
    fn close(&mut self) -> Result<(), ExecError> {
        self.finish_task()?;
        self.base.close()
    }

    fn schema(&self) -> &Schema {
        self.base.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.base.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.base.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.base.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.base.new_chunk()
    }
}

#[cfg(test)]
mod tests;
