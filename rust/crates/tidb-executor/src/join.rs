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

//! Two-way joins: inner, left outer, and right outer.
//!
//! The output row is the left row's cells followed by the right row's, which
//! is the schema Go's `LogicalJoin` builds and what the column resolver
//! addresses. The `ON` condition filters MATCHES, not rows: an outer row that
//! matches nothing still emits once, padded with NULLs on the other side
//! (Go `HashJoinExec`'s `onMissMatch` path), while a `WHERE` predicate applies
//! above the join and can remove those padded rows -- the distinction that
//! makes `LEFT JOIN ... WHERE right.c IS NULL` an anti-join.
//!
//! A `RIGHT JOIN` is the same algorithm with the sides exchanged: the right
//! table is the outer one, and its unmatched rows are padded on the left. Go's
//! planner rewrites right joins into left joins for the same reason.
//!
//! # Algorithm: hash, with the nested loop as the stated fallback
//!
//! When the `ON` clause carries at least one `col = col` conjunct this module
//! can index (see [`crate::hash_join`] for the encoding and the classes it
//! refuses), the join builds a hash table on the INNER side and streams the
//! OUTER side through it: O(build + probe x fanout) instead of
//! O(build x probe). Everything else -- a cross join, a join whose only
//! conditions are inequalities, a key whose two sides compare in different
//! domains -- keeps the nested loop, which materializes both inputs and
//! compares every pair.
//!
//! Which path a join takes is therefore decided before it gets here, by what
//! is in its condition list -- and for a comma join that is decided by
//! [`crate::driver::predicate_push_down`], whose whole purpose is to put the
//! `WHERE` equality where this dispatch can see it. Without it a tree of
//! comma joins is a tree of nested loops and its cost is the product of every
//! input's row count.
//!
//! The two paths are not two implementations of `ON`. The hash table only
//! CHOOSES the candidate pairs; each candidate is then handed to the same
//! [`JoinExec::matches`] the nested loop calls on every pair. So the hash
//! path can only ever emit a subset of the loop's output, and the key
//! encoding's one obligation -- never separate two rows that satisfy `eq` --
//! is what makes that subset the whole of it.
//!
//! # Build side
//!
//! The build side is the INNER side: the right child for `INNER`/`LEFT`, the
//! left child for `RIGHT`. That is what Go's stats-less enumeration reaches
//! first (`getHashJoins`, `pkg/planner/core/exhaust_physical_plans.go`, which
//! calls `getHashJoin(ge, p, prop, 1, false)` first for `InnerJoin` and
//! `LeftOuterJoin` and uses `innerIdx = 0` for `RightOuterJoin`), and it is
//! what a captured `EXPLAIN` over statistics-free tables actually prints:
//! `hj2` is `(Build)` for `hj1 JOIN hj2` and for `hj1 LEFT JOIN hj2`, `hj1`
//! is `(Build)` for `hj1 RIGHT JOIN hj2`. Go re-picks by row count once
//! statistics exist; this tier has none, so it takes the stats-less choice
//! rather than inventing an estimate.
//!
//! Choosing the inner side is also what keeps the outer semantics
//! single-pass: the side whose unmatched rows must still be emitted is the
//! side being streamed, so a probe row that finds no bucket emits its
//! NULL-padded row immediately. No second sweep over a matched-flag array,
//! and no need to hold the preserved side in memory.
//!
//! # Memory, and the build side's spill to disk
//!
//! The build side is materialized -- that is inherent to hashing -- into a
//! `tidb_chunk::RowContainer`, which is Go v1's `hashRowContainer`: the row
//! DATA lives in the container and the hash table holds only `RowPtr`s into
//! it. The probe side is NOT materialized: it is pulled one chunk at a time
//! and each row is dropped as soon as its output is emitted. The nested-loop
//! fallback still materializes both sides.
//!
//! That split is what makes spilling cheap. When the build side outgrows
//! `tidb_mem_quota_query`, Go's `SpillDiskAction` -- registered on the
//! SESSION tracker by `BuildWorkerV1.BuildHashTableForList` via
//! `FallbackOldAndSetNewAction` -- moves the container's chunks to a file and
//! releases their memory; the hash table stays put, so a probe still finds
//! its bucket in memory and only the DEREFERENCE of a pointer becomes a disk
//! read. The gate is `tidb_enable_tmp_storage_on_oom`: with it off no spill
//! action is registered at all, so the tracker's existing cancellation fires
//! and the statement ends with errno 8175, exactly as it did before spilling
//! existed. With it on and the container still over quota after the spill,
//! the action's FALLBACK is that same cancellation -- Go chains them, and so
//! does this.
//!
//! WHICH VERSION. TiDB ships two hash joins, selected by
//! `tidb_hash_join_version`, and their spill mechanisms are not variants of
//! one design. v2 (`optimized`, the shipped DEFAULT for an equi-join with no
//! NullEQ and no null-aware key) partitions BOTH sides into its own
//! serialized row-table format and replays whole partitions in restore
//! rounds. v1 (`legacy`, and the only path for a cross join, a NullEQ key or
//! a null-aware anti-join) is the container-plus-pointers design above. This
//! executor is v1-SHAPED -- one build container, an in-memory key-to-pointer
//! map, a single-threaded probe loop -- so v1's spill is the one ported here.
//! Building v2's partitioned machinery onto it would be a rewrite, not a
//! port, and is NOT started.
//!
//! Still deferred relative to Go's `HashJoinExec`: the parallel build/probe
//! worker pipeline, v2's partitioned spill, and the semi/anti/outer-apply
//! variants. Hash-aggregate spill, TopN spill, parallel-sort spill and
//! `SortedRowContainer` are other operators' surfaces and are untouched.
//!
//! # Memory accounting (`tidb_mem_quota_query`)
//!
//! A cross join is the one plan whose materialization is bounded by nothing:
//! `t t1, .., t t6` over a doubled `t` drains the 5-way join underneath the
//! top one, which is |t|^5 rows held at once. Go cancels that on
//! `tidb_mem_quota_query` with errno 8175 (`executor/jointest/join` sets the
//! quota to `1 << 18` and asserts `--error 8175`); unaccounted, this port ran
//! until the OS killed the process, so the corpus HUNG instead of diverging.
//!
//! Two call sites now consume against [`crate::mem_quota::StatementMemory`],
//! each followed immediately by `check()`, exactly as
//! [`crate::sort::SortExec::fetch_and_sort`] does:
//!
//! - [`JoinExec::drain`], per chunk, for both sides of the nested-loop
//!   fallback (the hash build side accounts through its container instead).
//! - [`JoinExec::next_nested`], per outer row, for the OUTPUT it accumulates,
//!   because that path emits the whole result into one `req` in a single
//!   call.
//!
//! In a CHAIN of comma joins the two sites overlap, and measurably so: a
//! mutation probe that neutered either one alone still cancelled
//! `t t1, t t2, t t3`, and only neutering BOTH let it run to completion. The
//! reason is that an intermediate join's entire result is what its parent's
//! `drain` pulls, so at every level but one the drain site sees the
//! explosion first. The output site is what covers the level the drain site
//! cannot: the TOPMOST join, whose result no parent drains. Neither site is
//! redundant, and neither is the whole story on its own.
//!
//! The hash path's output is deliberately NOT accounted. It fills `req` per
//! probe chunk and the caller drains it between calls, so nothing
//! accumulates; charging a streamed result would cancel legal queries. Go
//! draws the same line -- `hashJoinExec` tracks its `rowContainer` build
//! side, not the rows it hands back.
//!
//! The hash path's BUILD side no longer goes through [`JoinExec::drain`]: its
//! container's own tracker accounts each chunk as it lands, which is Go's
//! accounting for that side and is also what the spill action fires from.
//! `drain` still serves the nested-loop fallback, whose two sides it counts
//! per row.
//!
//! The check fires INSIDE both loops, never after: after the loop there is
//! nothing left to save.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_join::{row_key, BuildError, BuildTable, EquiKey, KeyError};
use crate::mem_quota::StatementMemory;
use std::cell::Cell;
use std::cmp::Ordering;
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::{ArcAction, Tracker};

/// Which side, if any, keeps rows that match nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    /// `JOIN` / `INNER JOIN` / `CROSS JOIN` / comma join: only matches.
    Inner,
    /// `LEFT [OUTER] JOIN`: every left row survives, padded with NULLs.
    Left,
    /// `RIGHT [OUTER] JOIN`: every right row survives, padded with NULLs.
    Right,
}

/// The hash path's live state; absent until the first `next()`, and never
/// created on the nested-loop fallback.
struct HashState {
    /// The materialized, indexed inner side.
    table: BuildTable,
    /// The build side's column types, needed to read a row back out of the
    /// container (which stores bytes, not `Datum`s).
    build_types: Vec<FieldType>,
    /// Go `hashRowContainer.chkBuf`: the landing chunk a spilled build row is
    /// read back into. Reused across probes so a disk-backed join does not
    /// allocate per matched row.
    build_buf: Chunk,
    /// The chunk the outer child streams into, and how far it is consumed.
    probe_chunk: Chunk,
    probe_row: usize,
    probe_done: bool,
}

/// One side of the merge strategy: the chunk it streams into, how far that
/// chunk is consumed, and the group of equal-keyed rows currently held.
///
/// Go's `MergeJoinTable`. The group is `Vec<Vec<Datum>>` rather than Go's
/// `chunk.RowContainer` because this tier has no spill container; the bound
/// is the statement budget, polled after each group lands, exactly as the
/// nested path polls it.
struct MergeSide {
    chunk: Chunk,
    row: usize,
    /// Whether the child has returned its final (empty) chunk.
    done: bool,
    /// The rows of the current group -- all with the same join key.
    group: Vec<Vec<Datum>>,
    /// The key of `group`, empty when `group` is.
    key: Vec<Datum>,
}

impl MergeSide {
    fn new(chunk: Chunk) -> Self {
        MergeSide {
            chunk,
            row: 0,
            done: false,
            group: Vec::new(),
            key: Vec::new(),
        }
    }
}

/// Go `DefIndexJoinBatchSize` / `tidb_index_join_batch_size`: the largest
/// number of outer rows one index-join probe batch may hold.
///
/// RESIDUE: the session variable is not read here yet, so a `SET
/// tidb_index_join_batch_size` changes nothing. The batch size is a
/// PERFORMANCE decision -- every batch boundary produces the same rows in the
/// same order, which `index_join_batch_boundary_does_not_change_the_result`
/// pins -- so reading the default is a smaller claim, not a wrong answer.
pub(crate) const INDEX_JOIN_BATCH_SIZE: usize = 25000;

/// The index-join strategy: which child is LOOKED UP once per distinct outer
/// key, and over which object.
///
/// Go's `IndexLookUpJoin`. The outer child streams in batches; each batch's
/// distinct keys decide the ranges the inner side reads
/// (`range: decided by [...]` in `EXPLAIN`), and the rows that come back are
/// indexed by the join's OWN equality keys, so this strategy and the hash
/// strategy answer "does this pair match?" with the same bytes.
pub(crate) struct IndexLookupPlan {
    /// Whether the LOOKED-UP side is this join's left child.
    ///
    /// It is never the outer-join-preserved side: a `LEFT JOIN` may only look
    /// up its right child and a `RIGHT JOIN` only its left, which is what
    /// lets [`JoinExec::outer_is_left`] answer for both strategies at once.
    pub(crate) lookup_is_left: bool,
    /// Offsets into [`JoinExec::keys`] of the equalities that decide the
    /// probe range, in the looked-up object's own key-column order.
    ///
    /// A subset, not the whole list: Go's `KeyCols` are the index prefix the
    /// range is built from while `HashCols` are every equality, and a join
    /// whose index covers only the first of two keys still probes on one
    /// column and matches on both.
    pub(crate) probe_keys: Vec<usize>,
    /// The re-seedable inner source.
    pub(crate) source: crate::access_path::IndexJoinLookupExec,
}

/// The index strategy's live state: one outer batch and the inner rows its
/// probes found.
struct IndexLookupState {
    /// The current batch's outer rows, in the order the child produced them
    /// -- which is the order the result preserves.
    outer: Vec<Vec<Datum>>,
    /// How far `outer` is consumed.
    cursor: usize,
    /// The inner rows the batch's probes read, in read order.
    inner: Vec<Vec<Datum>>,
    /// Every equality key's encoding to the `inner` positions carrying it.
    matched: std::collections::HashMap<Vec<u8>, Vec<usize>>,
    /// The chunk the outer child streams into, and how far it is consumed.
    outer_chunk: Chunk,
    outer_row: usize,
    /// Whether the outer child has returned its final (empty) chunk.
    outer_done: bool,
    /// The next batch's size, doubling to [`INDEX_JOIN_BATCH_SIZE`] as Go's
    /// `increaseBatchSize` does.
    batch_size: usize,
}

/// The merge strategy's live state: one [`MergeSide`] per child.
struct MergeState {
    left: MergeSide,
    right: MergeSide,
}

/// A join of two children, hashing its equal conditions when it can and
/// falling back to a nested loop when it cannot (see the module doc).
pub struct JoinExec<C: Columns> {
    meta: ExecutorMeta,
    kind: JoinKind,
    conditions: Vec<Expression>,
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    ctx: C,
    /// The indexable `col = col` conjuncts; empty means the nested loop.
    keys: Vec<EquiKey>,
    /// Nested loop only: whether its single all-at-once batch was emitted.
    emitted: bool,
    hash: Option<HashState>,
    /// The merge strategy's key pairs and direction, set by the planner when
    /// BOTH children already produce rows in the join keys' order. `None` is
    /// every other join, and is what keeps this a fail-closed opt-in: a
    /// strategy that assumes an order it was not promised would silently drop
    /// rows.
    merge: Option<crate::merge_join_plan::MergeJoinPlan>,
    /// The merge strategy's live state; absent until the first `next()`.
    merge_state: Option<MergeState>,
    /// The index strategy's plan, set by the planner when one side is a
    /// single table whose index the join keys can probe. `None` is every
    /// other join, and -- as with `merge` above -- the opt-in is fail-closed:
    /// a lookup over the wrong object silently loses rows.
    index_lookup: Option<IndexLookupPlan>,
    /// The index strategy's live state; absent until the first `next()`.
    index_state: Option<IndexLookupState>,
    /// How many times the `ON` clause has been evaluated. This is the cost
    /// the hash table exists to remove, so it is the number a scaling test
    /// asserts on directly instead of timing the machine.
    condition_evals: Cell<u64>,
    /// The statement's budget, polled right after every `consume` below.
    memory: StatementMemory,
    /// This operator's accountant, hanging off the statement tracker.
    tracker: Arc<Tracker>,
    /// Go `HashJoinCtxV1.diskTracker`: what the build side has written to
    /// spill files.
    disk_tracker: Arc<tidb_util::disk::Tracker>,
    /// The spill action registered on the session tracker, kept so `close`
    /// can unbind it -- Go's
    /// `MemTracker.UnbindActionFromHardLimit(e.RowContainer.ActionSpill())`.
    registered_action: Option<ArcAction>,
    /// Whether the build side reached disk, latched when the build finishes
    /// (see [`JoinExec::build_table`]).
    build_spilled: bool,
    /// What the build side wrote to spill files, latched with the flag above.
    spilled_bytes: i64,
}

impl<C: Columns> JoinExec<C> {
    /// Builds a join of `left` and `right` filtered by `conditions` (the `ON`
    /// clause, empty for a Cartesian product).
    ///
    /// `memory` is the statement's budget, required rather than optional for
    /// the same reason `SortExec::new` requires it: a cross join is the one
    /// plan whose materialization is bounded by nothing, so a call site that
    /// could omit the budget could reintroduce the unbounded drain.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        kind: JoinKind,
        conditions: Vec<Expression>,
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let keys = crate::hash_join::split_equi(&conditions, left.ret_field_types().len()).keys;
        let tracker = memory.operator_tracker(meta.id());
        JoinExec {
            meta,
            kind,
            conditions,
            left,
            right,
            ctx,
            keys,
            emitted: false,
            hash: None,
            merge: None,
            merge_state: None,
            index_lookup: None,
            index_state: None,
            condition_evals: Cell::new(0),
            memory,
            tracker,
            disk_tracker: tidb_util::disk::new_tracker(-1, -1),
            registered_action: None,
            build_spilled: false,
            spilled_bytes: 0,
        }
    }

    /// Whether the build side has moved to a spill file (Go
    /// `hashRowContainer.AlreadySpilledSafeForTest`). For tests and
    /// diagnostics.
    #[must_use]
    pub fn build_side_spilled(&self) -> bool {
        self.build_spilled
    }

    /// The spill action this join put on the session tracker, if any. For
    /// tests: `close` must take it back off, and identity is the only way to
    /// tell "the chain still has SOME action" from "the chain still has THIS
    /// one".
    #[must_use]
    pub fn registered_spill_action(&self) -> Option<ArcAction> {
        self.registered_action.clone()
    }

    /// How many rows the reusable read-back buffer (Go
    /// `hashRowContainer.chkBuf`) is holding. It is reset per matched row, so
    /// this is 1 while probing and 0 before the build finishes -- a growing
    /// value would mean the buffer accumulates every row the join ever read
    /// back from disk.
    #[must_use]
    pub fn build_buf_rows(&self) -> usize {
        self.hash
            .as_ref()
            .map_or(0, |hash| hash.build_buf.num_rows())
    }

    /// Bytes the build side has written to spill files (Go
    /// `HashJoinCtxV1.diskTracker`).
    #[must_use]
    pub fn spilled_bytes(&self) -> i64 {
        self.spilled_bytes
    }

    /// Whether this join hashes its equal conditions rather than looping.
    #[must_use]
    pub fn is_hash_join(&self) -> bool {
        !self.keys.is_empty()
    }

    /// How many times the `ON` clause has been evaluated so far.
    #[must_use]
    pub fn condition_evals(&self) -> u64 {
        self.condition_evals.get()
    }

    /// Forces the nested-loop path on a join that would otherwise hash.
    ///
    /// The nested loop is this unit's stated reference: the hash path is
    /// correct exactly insofar as it reproduces the loop's output row for
    /// row. Proving that needs BOTH paths over the SAME data, which is what
    /// this exists for -- nothing outside the differential test uses it.
    #[cfg(test)]
    fn force_nested_loop(&mut self) {
        self.keys.clear();
    }

    /// The outer side is the one whose unmatched rows survive; the inner
    /// side is the other, and is the one the hash path builds its table on.
    /// `true` means the LEFT child is the outer one.
    fn outer_is_left(&self) -> bool {
        match &self.index_lookup {
            // The index strategy DRIVES from the side it does not look up,
            // whichever side that is -- an inner join whose left child is the
            // indexed table streams its right child. For an outer join the
            // two answers coincide, because the decision refuses to look up
            // the preserved side (see [`IndexLookupPlan::lookup_is_left`]).
            Some(plan) => !plan.lookup_is_left,
            None => self.kind != JoinKind::Right,
        }
    }

    /// Drains a child into rows of `Datum`s, accounting each chunk's worth
    /// against the statement's budget as it lands.
    ///
    /// Mirrors `SortExec::fetch_and_sort`: consume, then `check()`, INSIDE
    /// the loop. What is counted is what this `Vec` retains -- the datum rows
    /// -- not the chunk the child filled, because the chunk is reused and the
    /// rows are what the process actually holds.
    fn drain(
        child: &mut dyn Executor,
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        let types: Vec<FieldType> = child.ret_field_types().to_vec();
        let mut chunk = child.new_chunk();
        let mut rows = Vec::new();
        loop {
            child.next(&mut chunk)?;
            let n = chunk.num_rows();
            if n == 0 {
                break;
            }
            let mut bytes = 0i64;
            for r in 0..n {
                let row = datum_row(&chunk, r, &types);
                bytes += row_bytes(&row);
                rows.push(row);
            }
            tracker.consume(bytes);
            memory.check()?;
        }
        Ok(rows)
    }

    /// Whether the `ON` conditions all hold for one joined row.
    fn matches(&self, joined: &[Datum]) -> Result<bool, ExecError> {
        if self.conditions.is_empty() {
            return Ok(true);
        }
        self.condition_evals.set(self.condition_evals.get() + 1);
        let types = self.meta.ret_field_types().to_vec();
        let mut chunk = Chunk::new_with_capacity(&types, 1);
        for (i, value) in joined.iter().enumerate() {
            chunk.append_datum(i, value);
        }
        let row = chunk.get_row(0);
        for condition in &self.conditions {
            let value = condition.eval(&self.ctx, row)?;
            if !truthy(&value)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Concatenates an outer and an inner row back into left-then-right
    /// order, which is the join's own schema.
    fn join_rows(&self, outer_row: &[Datum], inner_row: &[Datum]) -> Vec<Datum> {
        if self.outer_is_left() {
            outer_row.iter().chain(inner_row).cloned().collect()
        } else {
            inner_row.iter().chain(outer_row).cloned().collect()
        }
    }

    /// The row an outer row that matched nothing emits: itself, padded with
    /// NULLs on the inner side.
    fn padded_row(&self, outer_row: &[Datum]) -> Vec<Datum> {
        let padding = if self.outer_is_left() {
            self.right.ret_field_types().len()
        } else {
            self.left.ret_field_types().len()
        };
        let nulls = std::iter::repeat_n(Datum::Null, padding);
        if self.outer_is_left() {
            outer_row.iter().cloned().chain(nulls).collect()
        } else {
            nulls.chain(outer_row.iter().cloned()).collect()
        }
    }

    fn append(req: &mut Chunk, joined: &[Datum]) {
        for (c, value) in joined.iter().enumerate() {
            req.append_datum(c, value);
        }
    }

    /// Emits every output row one outer row produces, given the inner rows
    /// it may match. Shared by both paths so the outer-join padding rule and
    /// the output column order have exactly one implementation.
    fn emit_outer_row(
        &self,
        req: &mut Chunk,
        outer_row: &[Datum],
        candidates: impl Iterator<Item = Vec<Datum>>,
    ) -> Result<(), ExecError> {
        let mut matched = false;
        for inner_row in candidates {
            let joined = self.join_rows(outer_row, &inner_row);
            if !self.matches(&joined)? {
                continue;
            }
            matched = true;
            Self::append(req, &joined);
        }
        if !matched && self.kind != JoinKind::Inner {
            Self::append(req, &self.padded_row(outer_row));
        }
        Ok(())
    }

    /// Declares that this join looks its inner side up per outer batch.
    ///
    /// As with [`Self::set_merge_plan`] the promise is the caller's: only
    /// `driver::index_join_decision` makes it, and only after checking that
    /// the probed object's key columns ARE the join's own equality columns.
    pub(crate) fn set_index_lookup_plan(&mut self, plan: IndexLookupPlan) {
        self.index_lookup = Some(plan);
    }

    /// Whether this join looks its inner side up per outer batch.
    #[must_use]
    pub fn is_index_join(&self) -> bool {
        self.index_lookup.is_some()
    }

    /// The index strategy: stream the outer side in batches, and read only
    /// the inner rows each batch's keys can reach.
    ///
    /// Go's `IndexLookUpJoin.Next` over the tasks its outer worker builds.
    /// One call consumes batches until it produces at least one output row,
    /// for the same reason [`Self::next_hashed`] does: an empty `req` is the
    /// caller's EOF signal.
    fn next_index_lookup(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        loop {
            if !self.fill_index_batch()? {
                return Ok(());
            }
            self.drain_index_batch(req)?;
            if req.num_rows() > 0 {
                return Ok(());
            }
        }
    }

    /// Makes sure a batch with unconsumed outer rows is loaded, returning
    /// `false` once the outer child is exhausted.
    fn fill_index_batch(&mut self) -> Result<bool, ExecError> {
        if self.index_state.is_none() {
            let outer_chunk = if self.outer_is_left() {
                self.left.new_chunk()
            } else {
                self.right.new_chunk()
            };
            self.index_state = Some(IndexLookupState {
                outer: Vec::new(),
                cursor: 0,
                inner: Vec::new(),
                matched: std::collections::HashMap::new(),
                outer_chunk,
                outer_row: 0,
                outer_done: false,
                // Go's `startWorkers(ctx, req.RequiredRows())`: the first
                // batch is what the caller asked for, capped by the maximum.
                batch_size: self.meta.max_chunk_size().min(INDEX_JOIN_BATCH_SIZE),
            });
        }
        loop {
            let state = self.index_state.as_ref().expect("just installed above");
            if state.cursor < state.outer.len() {
                return Ok(true);
            }
            if state.outer_done && state.outer_row >= state.outer_chunk.num_rows() {
                return Ok(false);
            }
            self.load_index_batch()?;
            let state = self.index_state.as_ref().expect("still installed");
            if state.outer.is_empty() && state.outer_done {
                return Ok(false);
            }
        }
    }

    /// Pulls the next batch of outer rows, probes the inner side with their
    /// distinct keys, and indexes what comes back.
    ///
    /// Go's `outerWorker.buildTask` + `innerWorker.handleTask`, in that
    /// order and with the same three steps: build the lookup contents, fetch
    /// the inner results, build the lookup map.
    fn load_index_batch(&mut self) -> Result<(), ExecError> {
        let outer_is_left = self.outer_is_left();
        // Disjoint field borrows: the batch below reads the outer CHILD while
        // it writes the state and re-seeds the source, and those are three
        // different fields of this executor.
        let JoinExec {
            left,
            right,
            keys,
            tracker,
            memory,
            index_lookup,
            index_state,
            ..
        } = self;
        let outer_child = if outer_is_left {
            left.as_mut()
        } else {
            right.as_mut()
        };
        let outer_types: Vec<FieldType> = outer_child.ret_field_types().to_vec();
        let outer_offset = |key: &EquiKey| if outer_is_left { key.left } else { key.right };
        let inner_offset = |key: &EquiKey| if outer_is_left { key.right } else { key.left };
        let plan = index_lookup
            .as_mut()
            .expect("this path runs only with a plan");
        let state = index_state.as_mut().expect("fill_index_batch installed it");

        // 1. The outer batch, up to `batch_size` rows.
        state.outer.clear();
        state.cursor = 0;
        let mut bytes = 0i64;
        while state.outer.len() < state.batch_size {
            if state.outer_row >= state.outer_chunk.num_rows() {
                if state.outer_done {
                    break;
                }
                outer_child.next(&mut state.outer_chunk)?;
                state.outer_row = 0;
                if state.outer_chunk.num_rows() == 0 {
                    state.outer_done = true;
                    break;
                }
            }
            let row = datum_row(&state.outer_chunk, state.outer_row, &outer_types);
            state.outer_row += 1;
            bytes += row_bytes(&row);
            state.outer.push(row);
        }
        tracker.consume(bytes);
        memory.check()?;
        // Go `increaseBatchSize`, applied to the NEXT batch.
        state.batch_size = state
            .batch_size
            .saturating_mul(2)
            .min(INDEX_JOIN_BATCH_SIZE);

        // 2. The batch's distinct probe tuples. A NULL key part contributes
        //    no probe: `key_part` refuses NULL too, so such an outer row
        //    matches nothing either way -- Go's `constructDatumLookupKey`
        //    returning nil.
        //
        //    Go DEDUPES by sorting (`sortAndDedupLookUpContents`); this
        //    dedupes by the same encoding the match map is keyed on and
        //    keeps first-seen order. Sorting is what makes Go's ranges walk
        //    the index forwards, which is a cost, not an answer -- and
        //    deduping by the key encoding cannot merge two probes the join
        //    would have distinguished, which a comparison that swallowed its
        //    own error could.
        let probe_encoding: Vec<EquiKey> = plan
            .probe_keys
            .iter()
            .enumerate()
            .map(|(at, key)| EquiKey {
                left: at,
                right: at,
                class: keys[*key].class,
            })
            .collect();
        let mut seen = std::collections::HashSet::new();
        let mut probes: Vec<Vec<Datum>> = Vec::new();
        for row in &state.outer {
            let probe: Option<Vec<Datum>> = plan
                .probe_keys
                .iter()
                .map(|at| {
                    let value = row[outer_offset(&keys[*at])].clone();
                    (!matches!(value, Datum::Null)).then_some(value)
                })
                .collect();
            let Some(probe) = probe else {
                continue;
            };
            let encoded =
                row_key(&probe_encoding, &probe, |key| key.left).map_err(|_: KeyError| {
                    ExecError::unsupported("a join key column has no comparable encoding")
                })?;
            if let Some(encoded) = encoded {
                if seen.insert(encoded) {
                    probes.push(probe);
                }
            }
        }

        // 3. The inner rows those probes reach.
        plan.source.set_probes(probes);
        let inner_types: Vec<FieldType> = plan.source.ret_field_types().to_vec();
        state.inner.clear();
        state.matched.clear();
        let mut chunk = plan.source.new_chunk();
        loop {
            plan.source.next(&mut chunk)?;
            let n = chunk.num_rows();
            if n == 0 {
                break;
            }
            let mut bytes = 0i64;
            for r in 0..n {
                let row = datum_row(&chunk, r, &inner_types);
                bytes += row_bytes(&row);
                state.inner.push(row);
            }
            tracker.consume(bytes);
            memory.check()?;
        }

        // 4. Go `buildLookUpMap`, keyed by the join's OWN equalities so this
        //    strategy and the hash strategy cannot disagree about a match.
        for (at, row) in state.inner.iter().enumerate() {
            let key = row_key(keys, row, inner_offset).map_err(|_: KeyError| {
                ExecError::unsupported("a join key column has no comparable encoding")
            })?;
            if let Some(key) = key {
                state.matched.entry(key).or_default().push(at);
            }
        }
        Ok(())
    }

    /// Emits the loaded batch's outer rows, in the order the child produced
    /// them, until `req` is full or the batch runs out.
    fn drain_index_batch(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let keys = self.keys.clone();
        let outer_is_left = self.outer_is_left();
        let outer_offset = |key: &EquiKey| if outer_is_left { key.left } else { key.right };
        let cap = self.meta.max_chunk_size();
        loop {
            let state = self
                .index_state
                .as_ref()
                .expect("this path runs only with a batch");
            if state.cursor >= state.outer.len() || req.num_rows() >= cap {
                return Ok(());
            }
            let outer_row = state.outer[state.cursor].clone();
            let key = row_key(&keys, &outer_row, outer_offset).map_err(|_: KeyError| {
                ExecError::unsupported("a join key column has no comparable encoding")
            })?;
            let candidates: Vec<Vec<Datum>> = key
                .and_then(|key| state.matched.get(&key))
                .map(|positions| {
                    positions
                        .iter()
                        .map(|at| state.inner[*at].clone())
                        .collect()
                })
                .unwrap_or_default();
            self.emit_outer_row(req, &outer_row, candidates.into_iter())?;
            let state = self.index_state.as_mut().expect("still installed");
            state.cursor += 1;
        }
    }

    /// Declares that both children produce rows in `plan`'s key order, and
    /// that this join may therefore merge them.
    ///
    /// The promise is the caller's: only `driver::from`'s merge-join decision
    /// (see [`crate::merge_join_plan`]) makes it, and only after checking that
    /// both sides' access paths ALREADY provide the order. A wrong promise
    /// here loses rows silently, which is why nothing else may make it.
    pub(crate) fn set_merge_plan(&mut self, plan: crate::merge_join_plan::MergeJoinPlan) {
        self.merge = Some(plan);
    }

    /// Whether this join merges its two sorted children.
    #[must_use]
    pub fn is_merge_join(&self) -> bool {
        self.merge.is_some()
    }

    /// Pulls the next group of equal-keyed rows into `side`, leaving it empty
    /// when the child is exhausted.
    ///
    /// Go's `fetchNextInnerGroup`/`fetchNextOuterGroup`, collapsed into one:
    /// Go splits them because its inner group spans chunks through a spill
    /// container while its outer group stops at a chunk boundary, an
    /// asymmetry that exists to bound memory. Here BOTH sides collect a whole
    /// group, so the outer side's group is never split across calls and the
    /// duplicate-key cross product below is complete on both sides -- which is
    /// the property the asymmetric Go pair also has, reached by its own
    /// `MultiIterator`.
    fn fetch_group(
        side: &mut MergeSide,
        child: &mut dyn Executor,
        key_offsets: &[usize],
        types: &[FieldType],
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
    ) -> Result<(), ExecError> {
        side.group.clear();
        side.key.clear();
        loop {
            if side.row >= side.chunk.num_rows() {
                if side.done {
                    break;
                }
                child.next(&mut side.chunk)?;
                side.row = 0;
                if side.chunk.num_rows() == 0 {
                    side.done = true;
                    break;
                }
            }
            let row = datum_row(&side.chunk, side.row, types);
            let key: Vec<Datum> = key_offsets.iter().map(|&at| row[at].clone()).collect();
            if side.group.is_empty() {
                side.key = key;
            } else if merge_key_cmp(&side.key, &key, false)? != Ordering::Equal {
                // The next group starts here; leave `row` unconsumed.
                break;
            }
            tracker.consume(row_bytes(&row));
            side.group.push(row);
            side.row += 1;
        }
        memory.check()?;
        Ok(())
    }

    /// The merge path: advance the side whose key falls behind, and emit the
    /// cross product of every pair of groups whose keys are equal.
    ///
    /// Go's `MergeJoinExec.Next`. The three arms are Go's three: the inner
    /// group behind (drop it), the outer group behind (emit its misses), and
    /// the keys equal (join the two groups). This spelling drives both sides
    /// symmetrically and lets [`Self::emit_outer_row`] -- shared with the
    /// nested and hash paths -- apply the residual `ON` conditions and the
    /// outer-join padding, so the three strategies cannot disagree about what
    /// a row is.
    fn next_merged(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let plan = self.merge.clone().expect("next_merged needs a merge plan");
        let desc = plan.desc;
        let left_keys: Vec<usize> = plan.keys.iter().map(|key| key.left).collect();
        let right_keys: Vec<usize> = plan.keys.iter().map(|key| key.right).collect();
        let left_types: Vec<FieldType> = self.left.ret_field_types().to_vec();
        let right_types: Vec<FieldType> = self.right.ret_field_types().to_vec();
        if self.merge_state.is_none() {
            self.merge_state = Some(MergeState {
                left: MergeSide::new(self.left.new_chunk()),
                right: MergeSide::new(self.right.new_chunk()),
            });
        }
        let tracker = Arc::clone(&self.tracker);
        let memory = self.memory.clone();
        let outer_is_left = self.outer_is_left();
        loop {
            let state = self.merge_state.as_mut().expect("just created");
            if state.left.group.is_empty() {
                Self::fetch_group(
                    &mut state.left,
                    self.left.as_mut(),
                    &left_keys,
                    &left_types,
                    &tracker,
                    &memory,
                )?;
            }
            let state = self.merge_state.as_mut().expect("just created");
            if state.right.group.is_empty() {
                Self::fetch_group(
                    &mut state.right,
                    self.right.as_mut(),
                    &right_keys,
                    &right_types,
                    &tracker,
                    &memory,
                )?;
            }
            let state = self.merge_state.as_mut().expect("just created");
            let (left_empty, right_empty) =
                (state.left.group.is_empty(), state.right.group.is_empty());
            if left_empty && right_empty {
                return Ok(());
            }
            // A side that ran out makes the other side's remaining groups all
            // unmatched, which only an OUTER join still emits.
            let order = if left_empty {
                Ordering::Greater
            } else if right_empty {
                Ordering::Less
            } else {
                merge_key_cmp(&state.left.key, &state.right.key, desc)?
            };
            // A NULL join key needs NO special arm here. Go's
            // `hasNullInJoinKey` drops a NULL inner group because Go MOVED the
            // used equal conditions OUT of the condition list
            // (`moveEqualToOtherConditions`), so nothing downstream would
            // reject the pair. This tier keeps every `ON` conjunct in
            // `conditions` -- the merge keys are DERIVED from them, not
            // removed -- so `matches` evaluates `NULL = NULL`, gets NULL, and
            // rejects the pair, after which the outer padding rule emits
            // exactly the rows Go's skip emits. Two NULL groups are therefore
            // allowed to meet and produce nothing, which is the same answer by
            // the normal path instead of by a special case.
            match order {
                Ordering::Equal => {
                    let left_group = std::mem::take(&mut state.left.group);
                    let right_group = std::mem::take(&mut state.right.group);
                    let (outer, inner) = if outer_is_left {
                        (&left_group, &right_group)
                    } else {
                        (&right_group, &left_group)
                    };
                    for outer_row in outer {
                        self.emit_outer_row(req, outer_row, inner.iter().cloned())?;
                    }
                }
                // The left group is behind, or the right side is spent.
                Ordering::Less => {
                    let group = std::mem::take(&mut state.left.group);
                    if outer_is_left {
                        for row in &group {
                            self.emit_outer_row(req, row, std::iter::empty())?;
                        }
                    }
                }
                Ordering::Greater => {
                    let group = std::mem::take(&mut state.right.group);
                    if !outer_is_left {
                        for row in &group {
                            self.emit_outer_row(req, row, std::iter::empty())?;
                        }
                    }
                }
            }
            self.memory.check()?;
            // An empty `req` is the caller's EOF signal, so a call that
            // dropped only unmatched groups must keep going rather than
            // report exhaustion.
            if req.num_rows() > 0 {
                return Ok(());
            }
        }
    }

    /// The fallback: materialize both sides and compare every pair.
    fn next_nested(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.emitted {
            return Ok(());
        }
        let tracker = Arc::clone(&self.tracker);
        let memory = self.memory.clone();
        let left_rows = Self::drain(self.left.as_mut(), &tracker, &memory)?;
        let right_rows = Self::drain(self.right.as_mut(), &tracker, &memory)?;
        let (outer, inner) = if self.outer_is_left() {
            (&left_rows, &right_rows)
        } else {
            (&right_rows, &left_rows)
        };
        // The OUTPUT is accounted here and only here. This path emits the
        // whole result into one `req` in a single call, so that chunk is
        // live all at once and is the cross join's real cost -- |outer| *
        // |inner| rows, bounded by nothing. The hash path deliberately does
        // NOT account its output: it fills `req` per probe chunk and the
        // caller drains it between calls, so nothing accumulates, and
        // charging a streamed result would cancel legal queries. Go draws
        // the same line -- `hashJoinExec` tracks its `rowContainer` build
        // side, not the rows it hands back.
        for outer_row in outer {
            let before_rows = req.num_rows();
            let before_bytes = req.memory_usage();
            self.emit_outer_row(req, outer_row, inner.iter().cloned())?;
            let produced = i64::try_from(req.num_rows() - before_rows).unwrap_or(i64::MAX);
            let grew = (req.memory_usage() - before_bytes).max(0);
            self.tracker
                .consume(grew + tidb_chunk::row::ROW_SIZE * produced);
            self.memory.check()?;
        }
        self.emitted = true;
        Ok(())
    }

    /// The hash path: build once on the inner side, then stream the outer.
    ///
    /// One call consumes probe chunks until it has produced at least one
    /// output row, because an empty `req` is the caller's EOF signal and a
    /// probe chunk whose rows all miss (an inner join) produces none.
    fn next_hashed(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.build_table()?;
        loop {
            if self.hash.as_ref().is_some_and(|hash| hash.probe_done) {
                return Ok(());
            }
            self.fill_probe_chunk()?;
            self.drain_probe_chunk(req)?;
            if req.num_rows() > 0 {
                return Ok(());
            }
        }
    }

    /// Materializes and indexes the inner side, once per `open()`.
    ///
    /// Go `BuildWorkerV1.BuildHashTableForList`, in its order: hang the
    /// container's trackers off this operator's, register the spill action on
    /// the SESSION tracker when `tidb_enable_tmp_storage_on_oom` allows it,
    /// then feed the child's chunks in.
    fn build_table(&mut self) -> Result<(), ExecError> {
        if self.hash.is_some() {
            return Ok(());
        }
        let build_is_left = !self.outer_is_left();
        let build_types: Vec<FieldType> = if build_is_left {
            self.left.ret_field_types().to_vec()
        } else {
            self.right.ret_field_types().to_vec()
        };
        let mut table = BuildTable::new(&build_types, self.meta.max_chunk_size());
        table.mem_tracker().attach_to(&self.tracker);
        table.disk_tracker().attach_to(&self.disk_tracker);
        // Go: `if vardef.EnableTmpStorageOnOOM.Load() { ...
        // MemTracker.FallbackOldAndSetNewAction(actionSpill) }`. With the
        // variable OFF no spill action exists, so an overrun goes straight to
        // the cancellation that was already registered -- errno 8175, which
        // is exactly what this executor did before spilling existed.
        if self.memory.tmp_storage_on_oom() {
            let action: ArcAction = table.action_spill();
            self.memory
                .session_tracker()
                .fallback_old_and_set_new_action(Arc::clone(&action));
            self.registered_action = Some(action);
        }
        let build: &mut dyn Executor = if build_is_left {
            self.left.as_mut()
        } else {
            self.right.as_mut()
        };
        loop {
            let mut chunk = build.new_chunk();
            build.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            // The container's own tracker accounts the chunk as it lands,
            // which is Go's accounting for this side; the spill action fires
            // from that consume. `check` right after is what turns a
            // still-exceeding budget -- the action's FALLBACK, the
            // cancellation -- into the statement's error.
            table
                .index_chunk(chunk, &self.keys, &build_types, build_is_left)
                .map_err(build_error)?;
            self.memory.check()?;
        }
        let probe_chunk = if build_is_left {
            self.right.new_chunk()
        } else {
            self.left.new_chunk()
        };
        // The container's live state is only readable while it is open --
        // `close` deletes the spill file and detaches the disk tracker -- so
        // what the build side DID is latched here, at the moment the build
        // finishes and nothing more can change it.
        self.build_spilled = table.already_spilled();
        self.spilled_bytes = self.disk_tracker.bytes_consumed();
        let build_buf = Chunk::new_with_capacity(&build_types, 1);
        self.hash = Some(HashState {
            table,
            build_types,
            build_buf,
            probe_chunk,
            probe_row: 0,
            probe_done: false,
        });
        Ok(())
    }

    /// Pulls the next probe chunk when the current one is spent.
    fn fill_probe_chunk(&mut self) -> Result<(), ExecError> {
        let Some(hash) = self.hash.as_mut() else {
            return Ok(());
        };
        if hash.probe_row < hash.probe_chunk.num_rows() {
            return Ok(());
        }
        // Move the chunk out so the probe child can be borrowed mutably
        // alongside it; it goes straight back below.
        let mut chunk = std::mem::replace(&mut hash.probe_chunk, Chunk::new_with_capacity(&[], 0));
        let probe: &mut dyn Executor = if self.outer_is_left() {
            self.left.as_mut()
        } else {
            self.right.as_mut()
        };
        let result = probe.next(&mut chunk);
        let hash = self.hash.as_mut().expect("hash state exists in this arm");
        hash.probe_done = chunk.num_rows() == 0;
        hash.probe_chunk = chunk;
        hash.probe_row = 0;
        result
    }

    /// Emits every output row the current probe chunk produces.
    fn drain_probe_chunk(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let probe_is_left = self.outer_is_left();
        let probe_types: Vec<FieldType> = if probe_is_left {
            self.left.ret_field_types().to_vec()
        } else {
            self.right.ret_field_types().to_vec()
        };
        let offset = |key: &EquiKey| if probe_is_left { key.left } else { key.right };
        loop {
            let Some(hash) = self.hash.as_mut() else {
                return Ok(());
            };
            if hash.probe_row >= hash.probe_chunk.num_rows() {
                return Ok(());
            }
            let probe_row = datum_row(&hash.probe_chunk, hash.probe_row, &probe_types);
            let key = row_key(&self.keys, &probe_row, offset).map_err(key_error)?;
            // A probe row whose key holds a NULL matches nothing, so it never
            // touches the table -- and, on an outer join, pads immediately.
            //
            // Go `GetMatchedRowsAndPtrs`: walk the bucket's pointers in order
            // and dereference each one through the container, which is where
            // a spilled build side becomes a read from the spill file.
            let candidates: Vec<Vec<Datum>> = match &key {
                Some(key) => {
                    let ptrs = hash.table.probe(key).to_vec();
                    let mut rows = Vec::with_capacity(ptrs.len());
                    for ptr in ptrs {
                        let HashState {
                            table,
                            build_buf,
                            build_types,
                            ..
                        } = hash;
                        rows.push(
                            table
                                .row(ptr, build_buf, build_types)
                                .map_err(|error| ExecError::SpillFailed(error.to_string()))?,
                        );
                    }
                    rows
                }
                None => Vec::new(),
            };
            self.emit_outer_row(req, &probe_row, candidates.into_iter())?;
            self.hash
                .as_mut()
                .expect("hash state exists in this arm")
                .probe_row += 1;
        }
    }
}

/// What one materialized row costs: the `Vec` header plus each datum's own
/// estimate, which is Go `Datum.MemUsage` summed the same way.
pub(crate) fn row_bytes(row: &[Datum]) -> i64 {
    let mut bytes = i64::try_from(size_of::<Vec<Datum>>()).unwrap_or(i64::MAX);
    for datum in row {
        bytes += i64::try_from(datum.estimated_mem_usage()).unwrap_or(i64::MAX);
    }
    bytes
}

/// One chunk row as owned `Datum`s.
fn datum_row(chunk: &Chunk, index: usize, types: &[FieldType]) -> Vec<Datum> {
    let row = chunk.get_row(index);
    types
        .iter()
        .enumerate()
        .map(|(c, ft)| row.get_datum(c, ft))
        .collect()
}

/// A key datum outside its column's statically determined class. The class
/// comes from the key columns' own field types, so this is a type-metadata
/// inconsistency rather than a data condition -- it is reported instead of
/// guessed at, because a guess here silently drops rows.
fn key_error(_: KeyError) -> ExecError {
    ExecError::unsupported("join key value outside its column's comparison domain")
}

/// Indexing one build chunk failed: either the key error above, or the spill
/// file rejecting the write (Go returns the `RowContainer.Add` error to the
/// build worker, which fails the statement).
fn build_error(error: BuildError) -> ExecError {
    match error {
        BuildError::Key => key_error(KeyError),
        BuildError::Disk(message) => ExecError::SpillFailed(message),
    }
}

/// Go's condition truth test (`Datum.ToBool` via `expression.EvalBool`):
/// Compares two merge-join keys column by column, reversing for a descending
/// merge.
///
/// Go's `MergeJoinExec.compare` runs `CompareFuncs[i]` -- `GetCmpFunction` on
/// the two key columns -- and returns at the first non-equal column. The
/// shared `compare_datums` is that function's answer for the comparable types
/// a merge join is offered, and is the same one [`crate::sort`] orders by, so
/// the order the merge ASSUMES and the order a sort would PRODUCE are one
/// implementation.
fn merge_key_cmp(left: &[Datum], right: &[Datum], desc: bool) -> Result<Ordering, ExecError> {
    for (a, b) in left.iter().zip(right) {
        let mut cmp = tidb_expr::compare_datums(a, b)?;
        if desc {
            cmp = cmp.reverse();
        }
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

/// NULL and zero are false, and a string takes its numeric prefix.
fn truthy(value: &Datum) -> Result<bool, ExecError> {
    Ok(tidb_expr::truthy_of(value)? == Some(true))
}

impl<C: Columns> Executor for JoinExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.left.open()?;
        self.right.open()?;
        self.emitted = false;
        self.hash = None;
        self.build_spilled = false;
        self.spilled_bytes = 0;
        self.merge_state = None;
        self.index_state = None;
        if let Some(plan) = self.index_lookup.as_mut() {
            plan.source.open()?;
        }
        self.condition_evals.set(0);
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.index_lookup.is_some() {
            return self.next_index_lookup(req);
        }
        if self.merge.is_some() {
            return self.next_merged(req);
        }
        if self.keys.is_empty() {
            return self.next_nested(req);
        }
        self.next_hashed(req)
    }

    /// Go `HashJoinV1Exec.Close`: close the container (which deletes the
    /// spill file), then take the spill action back off the session tracker
    /// so the next statement is not left with a dangling one.
    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(hash) = self.hash.as_mut() {
            hash.table.close();
        }
        if let Some(action) = self.registered_action.take() {
            self.memory
                .session_tracker()
                .unbind_action_from_hard_limit(&action);
        }
        if let Some(plan) = self.index_lookup.as_mut() {
            plan.source.close()?;
        }
        self.left.close()?;
        self.right.close()
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
    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;
    use tidb_expr::scalar_function::ScalarFunction;
    pub(super) use tidb_expr::NoColumns;

    const CHUNK: usize = 1024;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn schema_of(width: usize) -> Schema {
        Schema::new(
            (0..width)
                .map(|i| {
                    let mut column = Column::new(i as i64 + 1, long());
                    column.index = i as i64;
                    column
                })
                .collect(),
        )
    }

    /// A source that hands out prebuilt rows in `max_chunk_size` batches, so
    /// the probe side really is pulled incrementally rather than in one go.
    struct RowSource {
        meta: ExecutorMeta,
        rows: Vec<Vec<Datum>>,
        cursor: usize,
    }

    impl RowSource {
        fn new(rows: Vec<Vec<Datum>>, width: usize) -> Self {
            RowSource {
                meta: ExecutorMeta::new(schema_of(width), 0, CHUNK, CHUNK),
                rows,
                cursor: 0,
            }
        }
    }

    impl Executor for RowSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.cursor = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.cursor + CHUNK).min(self.rows.len());
            for row in &self.rows[self.cursor..end] {
                for (c, value) in row.iter().enumerate() {
                    req.append_datum(c, value);
                }
            }
            self.cursor = end;
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

    /// `left.<lhs> = right.<rhs>`, addressed against the joined schema.
    pub(super) fn eq_on(lhs: usize, rhs: usize, left_width: usize) -> Expression {
        let column = |index: usize| {
            let mut column = Column::new(index as i64 + 1, long());
            column.index = index as i64;
            Expression::Column(column)
        };
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            long(),
            vec![column(lhs), column(left_width + rhs)],
        ))
    }

    pub(super) fn join_of(
        kind: JoinKind,
        conditions: Vec<Expression>,
        left: Vec<Vec<Datum>>,
        right: Vec<Vec<Datum>>,
        width: usize,
    ) -> JoinExec<NoColumns> {
        join_with_memory(
            kind,
            conditions,
            left,
            right,
            width,
            StatementMemory::default(),
        )
    }

    pub(super) fn join_with_memory(
        kind: JoinKind,
        conditions: Vec<Expression>,
        left: Vec<Vec<Datum>>,
        right: Vec<Vec<Datum>>,
        width: usize,
        memory: StatementMemory,
    ) -> JoinExec<NoColumns> {
        JoinExec::new(
            ExecutorMeta::new(schema_of(2 * width), 1, CHUNK, CHUNK),
            kind,
            conditions,
            Box::new(RowSource::new(left, width)),
            Box::new(RowSource::new(right, width)),
            NoColumns,
            memory,
        )
    }

    /// Drains a join to completion, exactly as a caller does: repeated
    /// `next()` until an empty chunk.
    pub(super) fn run(join: &mut JoinExec<NoColumns>) -> Vec<Vec<i64>> {
        join.open().unwrap();
        let types = join.ret_field_types().to_vec();
        let mut out = Vec::new();
        let mut req = join.new_chunk();
        loop {
            join.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..types.len())
                        .map(|c| match row.get_datum(c, &types[c]) {
                            Datum::Int(value) => value,
                            // NULL padding, distinguishable from any test
                            // value because every fixture value is >= 0.
                            Datum::Null => -1,
                            other => panic!("unexpected datum {other:?}"),
                        })
                        .collect(),
                );
            }
        }
        join.close().unwrap();
        out
    }

    /// Left rows: key `i % 7` (so keys repeat and both sides fan out), value
    /// `i`, with every 11th key NULL. Right rows: key `i % 5`, so some keys
    /// match nothing on either side.
    fn fixture(n: i64, modulus: i64) -> Vec<Vec<Datum>> {
        (0..n)
            .map(|i| {
                let key = if i % 11 == 10 {
                    Datum::Null
                } else {
                    Datum::Int(i % modulus)
                };
                vec![key, Datum::Int(i)]
            })
            .collect()
    }

    /// The hash path must reproduce the nested loop ROW FOR ROW -- same
    /// rows, same order -- for every join kind, over data with duplicate
    /// keys, unmatched keys on both sides, and NULL keys on both sides.
    ///
    /// The NULL rows are the point of the fixture: a NULL key matches
    /// nothing (not even another NULL), so an inner join must drop those
    /// rows and an outer join must still emit them NULL-padded. Getting that
    /// wrong is exactly the failure a bucket-based key can introduce.
    #[test]
    fn hash_path_matches_the_nested_loop_row_for_row() {
        for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Right] {
            let left = fixture(200, 7);
            let right = fixture(200, 5);
            let mut hashed = join_of(kind, vec![eq_on(0, 0, 2)], left.clone(), right.clone(), 2);
            assert!(hashed.is_hash_join());
            let mut looped = join_of(kind, vec![eq_on(0, 0, 2)], left, right, 2);
            looped.force_nested_loop();
            assert_eq!(run(&mut hashed), run(&mut looped), "{kind:?}");
        }
    }

    /// The same, with a non-equi conjunct riding along: the hash table
    /// selects candidates on the equal condition, and the residue still has
    /// to reject the pairs it rejects.
    #[test]
    fn residual_conditions_still_filter_hashed_candidates() {
        let left = fixture(150, 7);
        let right = fixture(150, 5);
        // `l.key = r.key AND l.value = r.value` -- the second conjunct is
        // also an equal condition, so both become keys; the composite key is
        // what must not let one column borrow the other's bytes.
        let conditions = vec![eq_on(0, 0, 2), eq_on(1, 1, 2)];
        let mut hashed = join_of(
            JoinKind::Left,
            conditions.clone(),
            left.clone(),
            right.clone(),
            2,
        );
        let mut looped = join_of(JoinKind::Left, conditions, left, right, 2);
        looped.force_nested_loop();
        assert_eq!(run(&mut hashed), run(&mut looped));
    }

    /// A join with no equal condition keeps the nested loop, as documented.
    #[test]
    fn cross_join_falls_back_to_the_nested_loop() {
        let mut join = join_of(JoinKind::Inner, Vec::new(), fixture(4, 7), fixture(4, 5), 2);
        assert!(!join.is_hash_join());
        assert_eq!(run(&mut join).len(), 16);
    }

    /// The scaling claim, asserted on the cost the hash table exists to
    /// remove rather than on the wall clock.
    ///
    /// 10k x 10k over 10k distinct keys: the nested loop would evaluate the
    /// `ON` clause 100_000_000 times. The hash join evaluates it once per
    /// candidate pair a bucket produces -- here exactly once per matching
    /// row, because the keys are distinct.
    #[test]
    fn ten_thousand_by_ten_thousand_is_linear_not_quadratic() {
        let rows = 10_000i64;
        let side: Vec<Vec<Datum>> = (0..rows)
            .map(|i| vec![Datum::Int(i), Datum::Int(i * 2)])
            .collect();
        let mut join = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], side.clone(), side, 2);
        assert!(join.is_hash_join());
        let out = run(&mut join);
        assert_eq!(out.len(), rows as usize);
        // Every output row is the key joined to itself.
        assert_eq!(out[0], vec![0, 0, 0, 0]);
        assert_eq!(out[9_999], vec![9_999, 19_998, 9_999, 19_998]);

        let evals = join.condition_evals();
        let nested_loop_evals = (rows * rows) as u64;
        assert_eq!(evals, rows as u64, "one candidate pair per probe row");
        // Stated as a ratio so the assertion says what it means: at least
        // four orders of magnitude fewer, not a tuned constant.
        assert!(
            evals * 10_000 <= nested_loop_evals,
            "{evals} evaluations vs the nested loop's {nested_loop_evals}"
        );
    }
}

#[cfg(test)]
mod merge_path_tests {
    use super::tests::{eq_on, join_of, run};
    use super::JoinKind;
    use crate::merge_join_plan::{MergeJoinKey, MergeJoinPlan};
    use tidb_datatype::Datum;

    /// The same fixture shape as the hash differential test -- duplicate keys
    /// on both sides, keys present on one side only, and NULL keys -- but
    /// SORTED, because that is the promise a merge join is given.
    ///
    /// NULLs sort first, which is where a key-ordered read puts them, so this
    /// is a stream a real ordered scan could produce.
    fn sorted_fixture(n: i64, modulus: i64, nulls: bool) -> Vec<Vec<Datum>> {
        let mut rows: Vec<Vec<Datum>> = (0..n)
            .map(|i| {
                let key = if nulls && i % 11 == 10 {
                    Datum::Null
                } else {
                    Datum::Int(i % modulus)
                };
                vec![key, Datum::Int(i)]
            })
            .collect();
        rows.sort_by_key(|row| match row[0] {
            Datum::Null => (0, 0),
            Datum::Int(key) => (1, key),
            _ => unreachable!("the fixture builds only NULLs and ints"),
        });
        rows
    }

    /// A multiset comparison: the merge path emits in KEY order and the hash
    /// path in OUTER-ROW order, so the two agree on rows without agreeing on
    /// their sequence. That difference is the algorithm, not a bug -- Go's
    /// merge join reorders the result the same way, which is why its plans
    /// still carry a `Sort` above when the query asked for one.
    pub(super) fn as_multiset(mut rows: Vec<Vec<i64>>) -> Vec<Vec<i64>> {
        rows.sort_unstable();
        rows
    }

    /// The merge path must produce the same ROWS as the hash path for every
    /// join kind, over sorted data with duplicate keys on BOTH sides (the
    /// group-by-group cross product), keys matched on neither side, and NULL
    /// keys (which must match nothing, not even each other).
    #[test]
    fn merge_path_matches_the_hash_path_row_for_row() {
        for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Right] {
            let left = sorted_fixture(200, 7, true);
            let right = sorted_fixture(200, 5, true);
            let mut merged = join_of(kind, vec![eq_on(0, 0, 2)], left.clone(), right.clone(), 2);
            merged.set_merge_plan(MergeJoinPlan {
                keys: vec![MergeJoinKey { left: 0, right: 0 }],
                desc: false,
            });
            assert!(merged.is_merge_join());
            let mut hashed = join_of(kind, vec![eq_on(0, 0, 2)], left, right, 2);
            assert!(hashed.is_hash_join());
            assert_eq!(
                as_multiset(run(&mut merged)),
                as_multiset(run(&mut hashed)),
                "{kind:?}"
            );
        }
    }

    /// A residual conjunct still filters the pairs a matched group produces,
    /// and an outer row every pair rejects is still emitted NULL-padded --
    /// the rule `emit_outer_row` owns for all three strategies.
    #[test]
    fn residual_conditions_still_filter_merged_groups() {
        let left = sorted_fixture(150, 7, false);
        let right = sorted_fixture(150, 5, false);
        let conditions = vec![eq_on(0, 0, 2), eq_on(1, 1, 2)];
        let mut merged = join_of(
            JoinKind::Left,
            conditions.clone(),
            left.clone(),
            right.clone(),
            2,
        );
        merged.set_merge_plan(MergeJoinPlan {
            keys: vec![MergeJoinKey { left: 0, right: 0 }],
            desc: false,
        });
        let mut hashed = join_of(JoinKind::Left, conditions, left, right, 2);
        assert_eq!(as_multiset(run(&mut merged)), as_multiset(run(&mut hashed)));
    }

    /// A DESCENDING merge reads both sides high to low, and must find the
    /// same matches: `PhysicalMergeJoin.Desc` reverses the comparison, not
    /// the semantics.
    #[test]
    fn a_descending_merge_finds_the_same_matches() {
        let mut left = sorted_fixture(120, 7, false);
        let mut right = sorted_fixture(120, 5, false);
        left.reverse();
        right.reverse();
        let mut merged = join_of(
            JoinKind::Inner,
            vec![eq_on(0, 0, 2)],
            left.clone(),
            right.clone(),
            2,
        );
        merged.set_merge_plan(MergeJoinPlan {
            keys: vec![MergeJoinKey { left: 0, right: 0 }],
            desc: true,
        });
        let mut hashed = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], left, right, 2);
        assert_eq!(as_multiset(run(&mut merged)), as_multiset(run(&mut hashed)));
    }

    /// One empty side: an inner join produces nothing, and an outer join
    /// still emits every preserved row NULL-padded. This is the arm where a
    /// merge loop most easily stops early.
    #[test]
    fn an_empty_side_still_emits_the_preserved_rows() {
        for (kind, expected) in [
            (JoinKind::Inner, 0),
            (JoinKind::Left, 30),
            (JoinKind::Right, 0),
        ] {
            let left = sorted_fixture(30, 7, false);
            let mut merged = join_of(kind, vec![eq_on(0, 0, 2)], left, Vec::new(), 2);
            merged.set_merge_plan(MergeJoinPlan {
                keys: vec![MergeJoinKey { left: 0, right: 0 }],
                desc: false,
            });
            assert_eq!(run(&mut merged).len(), expected, "{kind:?}");
        }
    }

    /// A group larger than one chunk must still be one group: the merge
    /// collects a whole run of equal keys before joining it, so a 3000-row
    /// group spanning several source chunks fans out completely.
    #[test]
    fn a_group_spanning_chunks_is_still_one_group() {
        let left: Vec<Vec<Datum>> = (0..3000)
            .map(|i| vec![Datum::Int(1), Datum::Int(i)])
            .collect();
        let right = vec![vec![Datum::Int(1), Datum::Int(0)]; 3];
        let mut merged = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], left, right, 2);
        merged.set_merge_plan(MergeJoinPlan {
            keys: vec![MergeJoinKey { left: 0, right: 0 }],
            desc: false,
        });
        assert_eq!(run(&mut merged).len(), 9000);
    }
}

/// The build side's spill-to-disk, ported from Go's hash join v1
/// (`BuildWorkerV1.BuildHashTableForList` + `chunk.RowContainer`).
///
/// The claim under test is the one that makes spilling worth having: a join
/// whose build side does not fit answers THE SAME ROWS, IN THE SAME ORDER,
/// as the same join with room to spare. Every test below therefore compares
/// against an unspilled run of the identical data rather than against a
/// hand-written expectation, and asserts that the spilled run really did go
/// to disk -- otherwise the comparison would pass trivially.
#[cfg(test)]
mod spill_tests {
    use super::merge_path_tests::as_multiset;
    use super::tests::{eq_on, join_with_memory, run, NoColumns};
    use super::*;
    use crate::mem_quota::{OomAction, StatementMemory};

    /// A build side big enough that its chunks outweigh any quota worth
    /// setting, with duplicate keys so each probe row walks a multi-entry
    /// bucket -- the case where reading rows back in the wrong order would
    /// show up.
    fn fixture(rows: i64, modulus: i64) -> Vec<Vec<Datum>> {
        (0..rows)
            .map(|i| vec![Datum::Int(i % modulus), Datum::Int(i)])
            .collect()
    }

    /// The build side is the RIGHT child for an inner join. 5000 rows at a
    /// 1024-row chunk is five chunks, and `i % 1000` puts each key's five
    /// duplicates in five DIFFERENT chunks -- so a bucket walk after a spill
    /// touches every chunk of the file, in build order.
    const BUILD_ROWS: i64 = 5000;
    const BUILD_KEYS: i64 = 1000;
    const PROBE_ROWS: i64 = 200;

    fn inner_join(memory: StatementMemory) -> JoinExec<NoColumns> {
        join_with_memory(
            JoinKind::Inner,
            vec![eq_on(0, 0, 2)],
            fixture(PROBE_ROWS, BUILD_KEYS),
            fixture(BUILD_ROWS, BUILD_KEYS),
            2,
            memory,
        )
    }

    /// A quota the build side cannot fit in, but which is still far larger
    /// than a single chunk -- so the spill has something to release and the
    /// read-path cancellation #289 describes is not what is being measured.
    fn tight_quota() -> StatementMemory {
        StatementMemory::new(64 * 1024, OomAction::Cancel, 1)
    }

    /// What one build chunk costs, measured rather than assumed, so the
    /// quota above can be stated as a MULTIPLE of it -- the regime where a
    /// spill has something to release. (#289: at quotas below one chunk Go
    /// cancels on read-path accounting before any spill can help, and that
    /// is deliberately not what these tests exercise.)
    #[test]
    fn the_tight_quota_is_several_chunks_not_a_fraction_of_one() {
        // Measured with room to spare, so nothing is released mid-build, and
        // read BEFORE `close` detaches the tracker.
        let mut join = inner_join(StatementMemory::default());
        join.open().unwrap();
        let mut req = join.new_chunk();
        join.next(&mut req).unwrap();
        let chunks = i64::try_from(BUILD_ROWS as usize).unwrap() / CHUNK_ROWS as i64 + 1;
        let one_chunk = join.tracker.bytes_consumed() / chunks;
        assert!(one_chunk > 0, "the build side must account something");
        assert!(
            64 * 1024 > 2 * one_chunk,
            "quota 65536 must be several chunks, one chunk is {one_chunk}"
        );
        assert!(
            join.tracker.bytes_consumed() > 64 * 1024,
            "the build side must not fit in the quota the spill tests use"
        );
    }

    const CHUNK_ROWS: usize = 1024;

    /// The read-back buffer must not accumulate. Go reuses one `chkBuf` per
    /// probe and lets the disk reader recycle it; here it is reset before
    /// every row, so after a join that read 1000 rows back from disk it holds
    /// exactly the last one. Without the reset this grows by one row per
    /// matched pair, which on a large spilled join is the whole build side
    /// pulled back into memory -- the precise thing the spill exists to
    /// prevent.
    #[test]
    fn the_read_back_buffer_does_not_accumulate_across_a_spilled_probe() {
        let mut join = inner_join(tight_quota());
        join.open().unwrap();
        let mut req = join.new_chunk();
        let mut seen = 0;
        loop {
            join.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            seen += req.num_rows();
            assert!(
                join.build_buf_rows() <= 1,
                "the read-back buffer holds {} rows after {seen} output rows",
                join.build_buf_rows()
            );
        }
        assert!(join.build_side_spilled());
        assert_eq!(seen, 1000);
    }

    /// The end-to-end claim: spilled and unspilled produce identical output.
    #[test]
    fn a_spilled_build_side_answers_exactly_the_unspilled_rows() {
        let mut roomy = inner_join(StatementMemory::default());
        let expected = run(&mut roomy);
        assert!(
            !roomy.build_side_spilled(),
            "the control run must NOT spill, or it proves nothing"
        );
        assert!(!expected.is_empty());

        // An INDEPENDENT oracle, not just the unspilled hash run: the nested
        // loop shares no build-side addressing with the hash path, so a
        // pointer bug that corrupts both hash runs identically still shows
        // up here. (A mutation probe that shifted the chunk index by one
        // survived the spilled-vs-unspilled comparison alone.)
        let mut looped = inner_join(StatementMemory::default());
        looped.force_nested_loop();
        assert_eq!(
            as_multiset(run(&mut looped)),
            as_multiset(expected.clone()),
            "the hash path must agree with the nested loop it replaces"
        );

        let mut tight = inner_join(tight_quota());
        let spilled = run(&mut tight);
        assert!(
            tight.build_side_spilled(),
            "the build side must actually reach disk"
        );
        assert!(
            tight.spilled_bytes() > 0,
            "a spilled build side must have written bytes"
        );
        // Row for row and in order, not merely as a set: a bucket read back
        // from disk out of build order would still match as a multiset.
        assert_eq!(spilled, expected);
    }

    /// The same claim for a LEFT join, where the probe side is preserved and
    /// a build row that fails to come back from disk would look like a
    /// legitimate NULL pad rather than an error.
    #[test]
    fn a_spilled_outer_join_pads_exactly_where_the_unspilled_one_does() {
        let build = |memory| {
            join_with_memory(
                JoinKind::Left,
                vec![eq_on(0, 0, 2)],
                fixture(PROBE_ROWS, BUILD_KEYS),
                fixture(BUILD_ROWS, 97),
                2,
                memory,
            )
        };
        let mut roomy = build(StatementMemory::default());
        let expected = run(&mut roomy);
        assert!(!roomy.build_side_spilled());

        let mut tight = build(tight_quota());
        let spilled = run(&mut tight);
        assert!(tight.build_side_spilled());
        assert_eq!(spilled, expected);
    }

    /// The gate. Go registers the spill action only when
    /// `tidb_enable_tmp_storage_on_oom` is on; with it off the memory action
    /// is still the cancellation, so the statement fails with 8175 instead of
    /// spilling. This is the behaviour that existed before this unit, and it
    /// must survive unchanged.
    #[test]
    fn with_tmp_storage_off_an_over_quota_build_side_is_cancelled_not_spilled() {
        let memory = tight_quota().with_tmp_storage_on_oom(false);
        let mut join = inner_join(memory);
        join.open().unwrap();
        let mut req = join.new_chunk();
        let error = loop {
            match join.next(&mut req) {
                Err(error) => break error,
                Ok(()) if req.num_rows() == 0 => panic!("the quota must be enforced"),
                Ok(()) => {}
            }
        };
        assert!(
            !join.build_side_spilled(),
            "with the gate off nothing may reach disk"
        );
        assert!(matches!(error, ExecError::MemoryExceedForQuery { .. }));
    }

    /// A spill that fires must not leave the action bound to the session
    /// tracker: Go's `Close` calls `UnbindActionFromHardLimit`, and a
    /// statement that inherited a closed join's action would spill into a
    /// container that no longer exists.
    #[test]
    fn close_unbinds_the_spill_action_from_the_session_tracker() {
        let memory = tight_quota();
        let mut join = inner_join(memory.clone());
        join.open().unwrap();
        let mut req = join.new_chunk();
        join.next(&mut req).unwrap();
        let spill = join
            .registered_spill_action()
            .expect("the gate is on, so an action was registered");
        // Registered: the spill action is at the head, ahead of the
        // cancellation it pushed down as its fallback.
        let head = memory
            .session_tracker()
            .get_fallback_for_test(false)
            .expect("the session tracker always has an action");
        assert!(Arc::ptr_eq(&head, &spill), "the spill action must be bound");

        join.close().unwrap();

        // Unbound: the chain still ACTS -- the cancellation is back at the
        // head -- but this join's action, whose container is now closed, is
        // gone from it.
        let mut current = memory.session_tracker().get_fallback_for_test(false);
        let mut found_any = false;
        while let Some(action) = current {
            found_any = true;
            assert!(
                !Arc::ptr_eq(&action, &spill),
                "a closed join's spill action must not stay in the chain"
            );
            current = action.get_fallback();
        }
        assert!(
            found_any,
            "unbinding must restore the fallback, not clear the chain"
        );
    }

    /// The container is the only thing that moves: the hash TABLE stays in
    /// memory, so a spilled join still answers a miss without touching disk
    /// and a NULL key still matches nothing.
    #[test]
    fn a_spilled_build_side_keeps_null_and_miss_semantics() {
        let mut build = fixture(BUILD_ROWS, BUILD_KEYS);
        build.push(vec![Datum::Null, Datum::Int(-1)]);
        let probe = vec![
            vec![Datum::Int(7), Datum::Int(0)],
            vec![Datum::Null, Datum::Int(1)],
            vec![Datum::Int(9999), Datum::Int(2)],
        ];
        let make = |memory| {
            join_with_memory(
                JoinKind::Inner,
                vec![eq_on(0, 0, 2)],
                probe.clone(),
                build.clone(),
                2,
                memory,
            )
        };
        let mut roomy = make(StatementMemory::default());
        let expected = run(&mut roomy);
        assert!(!roomy.build_side_spilled());

        let mut tight = make(tight_quota());
        let spilled = run(&mut tight);
        assert!(tight.build_side_spilled());
        assert_eq!(spilled, expected);
        // The NULL-keyed build row and the 9999 probe row match nothing, and
        // the NULL probe row matches nothing either.
        assert!(spilled.iter().all(|row| row[0] == 7));
    }

    /// A cross join has no equal conditions, so it never reaches the hash
    /// path and never gets a container -- Go's v1 spill covers the build side
    /// of a hash join only. The nested loop's existing 8175 cancellation is
    /// what still bounds it, gate or no gate.
    #[test]
    fn a_cross_join_still_cancels_rather_than_spilling() {
        let mut join = join_with_memory(
            JoinKind::Inner,
            Vec::new(),
            fixture(PROBE_ROWS, BUILD_KEYS),
            fixture(BUILD_ROWS, BUILD_KEYS),
            2,
            tight_quota(),
        );
        assert!(!join.is_hash_join());
        join.open().unwrap();
        let mut req = join.new_chunk();
        let error = join.next(&mut req).expect_err("the quota must be enforced");
        assert!(matches!(error, ExecError::MemoryExceedForQuery { .. }));
        assert!(!join.build_side_spilled());
    }
}
