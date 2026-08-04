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
//! # Memory
//!
//! The build side is materialized -- that is inherent to hashing -- as
//! `build rows x row width` datums plus one `u32` per row in its bucket. The
//! probe side is NOT: it is pulled one chunk at a time and each row is
//! dropped as soon as its output is emitted. The nested-loop fallback still
//! materializes both sides.
//!
//! Still deferred relative to Go's `HashJoinExec`: the parallel build/probe
//! worker pipeline, spill-to-disk, and the semi/anti/outer-apply variants.
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
//! - [`JoinExec::drain`], per chunk, for every side it materializes -- the
//!   hash build side, and both sides on the nested-loop fallback.
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
//! The check fires INSIDE both loops, never after: after the loop there is
//! nothing left to save.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_join::{row_key, BuildTable, EquiKey, KeyError};
use crate::mem_quota::StatementMemory;
use std::cell::Cell;
use std::cmp::Ordering;
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::Tracker;

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
    /// How many times the `ON` clause has been evaluated. This is the cost
    /// the hash table exists to remove, so it is the number a scaling test
    /// asserts on directly instead of timing the machine.
    condition_evals: Cell<u64>,
    /// The statement's budget, polled right after every `consume` below.
    memory: StatementMemory,
    /// This operator's accountant, hanging off the statement tracker.
    tracker: Arc<Tracker>,
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
            condition_evals: Cell::new(0),
            memory,
            tracker,
        }
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
        self.kind != JoinKind::Right
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
    fn build_table(&mut self) -> Result<(), ExecError> {
        if self.hash.is_some() {
            return Ok(());
        }
        let build_is_left = !self.outer_is_left();
        let tracker = Arc::clone(&self.tracker);
        let memory = self.memory.clone();
        let build_rows = if build_is_left {
            Self::drain(self.left.as_mut(), &tracker, &memory)?
        } else {
            Self::drain(self.right.as_mut(), &tracker, &memory)?
        };
        let probe_chunk = if build_is_left {
            self.right.new_chunk()
        } else {
            self.left.new_chunk()
        };
        let table = BuildTable::build(build_rows, &self.keys, build_is_left).map_err(key_error)?;
        self.hash = Some(HashState {
            table,
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
            let Some(hash) = self.hash.as_ref() else {
                return Ok(());
            };
            if hash.probe_row >= hash.probe_chunk.num_rows() {
                return Ok(());
            }
            let probe_row = datum_row(&hash.probe_chunk, hash.probe_row, &probe_types);
            let key = row_key(&self.keys, &probe_row, offset).map_err(key_error)?;
            // A probe row whose key holds a NULL matches nothing, so it never
            // touches the table -- and, on an outer join, pads immediately.
            let candidates: Vec<Vec<Datum>> = match &key {
                Some(key) => hash
                    .table
                    .probe(key)
                    .iter()
                    .map(|index| hash.table.rows[*index as usize].clone())
                    .collect(),
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
        self.merge_state = None;
        self.condition_evals.set(0);
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.merge.is_some() {
            return self.next_merged(req);
        }
        if self.keys.is_empty() {
            return self.next_nested(req);
        }
        self.next_hashed(req)
    }

    fn close(&mut self) -> Result<(), ExecError> {
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
    use tidb_expr::NoColumns;

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
        JoinExec::new(
            ExecutorMeta::new(schema_of(2 * width), 1, CHUNK, CHUNK),
            kind,
            conditions,
            Box::new(RowSource::new(left, width)),
            Box::new(RowSource::new(right, width)),
            NoColumns,
            StatementMemory::default(),
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
    fn as_multiset(mut rows: Vec<Vec<i64>>) -> Vec<Vec<i64>> {
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
