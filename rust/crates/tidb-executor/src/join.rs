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
use tidb_chunk::list::RowPtr;
use tidb_chunk::row_container::RowContainer;
use tidb_datatype::{Collation, Datum, Decimal, FieldType};
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
    /// `EXISTS`: emit the left row once when at least one right row matches.
    Semi,
    /// `NOT EXISTS`: emit the left row once when no right row matches.
    AntiSemi,
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
/// chunk is consumed, and the current equal-key group metadata.
///
/// Only the OUTER side owns `group` datum rows. The INNER side sets
/// `group_len` and writes its rows to [`MergeInnerGroup`], which is the
/// spillable authority accepted TiDB uses for a run crossing chunk bounds.
struct MergeSide {
    chunk: Chunk,
    /// The exact live capacity charge for `chunk`.
    chunk_bytes: i64,
    row: usize,
    /// Whether the child has returned its final (empty) chunk.
    done: bool,
    /// The current OUTER row. The INNER side stores no datum rows here.
    ///
    /// TiDB's merge join retains one equal-key INNER group, but streams the
    /// OUTER side through it. Keeping this vector bounded to one row is what
    /// prevents a duplicate-key run on the preserved side from becoming a
    /// second, non-spillable build side.
    group: Vec<Vec<Datum>>,
    /// Accounted bytes retained by `group`; released before the next group.
    group_bytes: i64,
    /// Number of rows in the current group. The inner group is not stored in
    /// `group`, so emptiness cannot be derived from the vector.
    group_len: usize,
    /// The key of `group`, empty when `group` is.
    key: Vec<Datum>,
}

impl MergeSide {
    fn new(chunk: Chunk) -> Self {
        let chunk_bytes = chunk.memory_usage();
        MergeSide {
            chunk,
            chunk_bytes,
            row: 0,
            done: false,
            group: Vec::new(),
            group_bytes: 0,
            group_len: 0,
            key: Vec::new(),
        }
    }
}

/// The merge INNER side's current equal-key group.
///
/// `staging` is the child-sized chunk currently being filled. Its memory is
/// charged directly to the join tracker until ownership is transferred to
/// `rows`; `RowContainer::add` charges the same chunk before that direct
/// charge is released, matching TiDB's child-chunk-to-container handoff.
struct MergeInnerGroup {
    rows: RowContainer,
    staging: Chunk,
    types: Vec<FieldType>,
}

impl MergeInnerGroup {
    fn new(
        types: Vec<FieldType>,
        chunk_size: usize,
        memory: &StatementMemory,
        tracker: &Arc<Tracker>,
        disk_tracker: &Arc<tidb_util::disk::Tracker>,
    ) -> Self {
        let mut rows = RowContainer::new(&types, chunk_size, memory.spill_storage());
        rows.mem_tracker().attach_to(tracker);
        rows.disk_tracker().attach_to(disk_tracker);
        let staging = rows.alloc_chunk();
        Self {
            rows,
            staging,
            types,
        }
    }

    fn reset(&mut self) {
        debug_assert_eq!(self.staging.num_rows(), 0);
        self.rows.reset();
    }

    fn append(
        &mut self,
        row: tidb_chunk::row::Row<'_>,
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
    ) -> Result<(), ExecError> {
        let before = self.staging.memory_usage();
        self.staging.append_row(row);
        tracker.consume(self.staging.memory_usage() - before);
        if self.staging.is_full() {
            self.flush(tracker)?;
        }
        memory.check()
    }

    fn finish_group(&mut self, tracker: &Arc<Tracker>) -> Result<(), ExecError> {
        if self.staging.num_rows() != 0 {
            self.flush(tracker)?;
        }
        Ok(())
    }

    fn flush(&mut self, tracker: &Arc<Tracker>) -> Result<(), ExecError> {
        let chunk = std::mem::take(&mut self.staging);
        let chunk_bytes = chunk.memory_usage();
        let result = self
            .rows
            .add(chunk)
            .map_err(|error| ExecError::SpillFailed(error.to_string()));
        tracker.consume(-chunk_bytes);
        result?;
        self.staging = self.rows.alloc_chunk();
        tracker.consume(self.staging.memory_usage());
        Ok(())
    }

    fn close(&mut self, tracker: &Arc<Tracker>) {
        tracker.consume(-self.staging.memory_usage());
        self.staging = Chunk::default();
        self.rows.close();
    }

    fn first_ptr(&self) -> Option<RowPtr> {
        (self.rows.num_chunks() != 0).then_some(RowPtr::new(0, 0))
    }

    fn next_ptr(&self, ptr: RowPtr) -> Option<RowPtr> {
        let chunk_index = ptr.chk_idx as usize;
        let next_row = ptr.row_idx as usize + 1;
        if next_row < self.rows.num_rows_of_chunk(chunk_index) {
            return Some(RowPtr::new(ptr.chk_idx, next_row as u32));
        }
        let next_chunk = chunk_index + 1;
        (next_chunk < self.rows.num_chunks()).then_some(RowPtr::new(next_chunk as u32, 0))
    }

    fn datum_row(&mut self, ptr: RowPtr) -> Result<Vec<Datum>, ExecError> {
        self.staging.reset();
        let row = {
            let loaded = self
                .rows
                .get_row_and_append_to_chunk_if_in_disk(ptr, &mut self.staging)
                .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
            loaded.row(&self.staging).get_datum_row(&self.types)
        };
        self.staging.reset();
        Ok(row)
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

/// One output of an aggregation rebuilt below an index join's lookup side.
///
/// Go rebuilds the complete inner physical task for every outer batch.  Most
/// inner tasks in this port are a bare table reader, but a grouped derived
/// table can retain its aggregation above that reader.  These are the two
/// output shapes needed by TPCC's grouped probes. Aggregate inputs retain
/// their source offsets so access-path selection can prove index coverage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IndexLookupAggregateOutput {
    Column(usize),
    Count(Option<usize>),
    Max { offset: usize, collation: Collation },
    DecimalSum(usize),
}

/// The executable aggregation retained above an index join's re-seeded table
/// reader.
///
/// Group keys are currently restricted by the planner to non-null integers.
/// Aggregate semantics match the ordinary hash aggregation: COUNT skips NULL
/// arguments, MAX compares under the input field's collation, and decimal SUM
/// uses the exact `Decimal::add` fold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexLookupAggregation {
    pub(crate) group_offsets: Vec<usize>,
    pub(crate) input_offsets: Vec<usize>,
    pub(crate) outputs: Vec<IndexLookupAggregateOutput>,
    /// `LogicalAggregation.PruneColumns` removed the last explicit aggregate
    /// and appended its synthetic COUNT(1) row-count carrier.
    pub(crate) pruned_row_count: bool,
}

impl IndexLookupAggregation {
    fn apply(
        &self,
        rows: Vec<Vec<Datum>>,
        stream_ordered: bool,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        enum Partial {
            Column,
            Count(i64),
            Max(Option<Datum>),
            DecimalSum(Option<Decimal>),
        }

        struct Group {
            first: Vec<Datum>,
            partials: Vec<Partial>,
        }

        let new_partials = || {
            self.outputs
                .iter()
                .map(|output| match output {
                    IndexLookupAggregateOutput::Column(_) => Partial::Column,
                    IndexLookupAggregateOutput::Count(_) => Partial::Count(0),
                    IndexLookupAggregateOutput::Max { .. } => Partial::Max(None),
                    IndexLookupAggregateOutput::DecimalSum(_) => Partial::DecimalSum(None),
                })
                .collect()
        };
        let key_of = |row: &[Datum]| {
            let mut key = Vec::new();
            for offset in &self.group_offsets {
                let value = row.get(*offset).ok_or_else(|| {
                    ExecError::unsupported("an index lookup aggregation group offset is absent")
                })?;
                key.extend_from_slice(&tidb_codec::hash_code(value));
                key.push(0xff);
            }
            Ok::<_, ExecError>(key)
        };
        let update = |group: &mut Group, row: &[Datum]| -> Result<(), ExecError> {
            for (output, partial) in self.outputs.iter().zip(&mut group.partials) {
                match (output, partial) {
                    (IndexLookupAggregateOutput::Column(_), Partial::Column) => {}
                    (IndexLookupAggregateOutput::Count(offset), Partial::Count(count)) => {
                        let present = match offset {
                            None => true,
                            Some(offset) => !matches!(
                                row.get(*offset).ok_or_else(|| ExecError::unsupported(
                                    "an index lookup COUNT input offset is absent"
                                ))?,
                                Datum::Null
                            ),
                        };
                        if present {
                            *count += 1;
                        }
                    }
                    (
                        IndexLookupAggregateOutput::Max { offset, collation },
                        Partial::Max(current),
                    ) => {
                        let input = row.get(*offset).ok_or_else(|| {
                            ExecError::unsupported("an index lookup MAX input offset is absent")
                        })?;
                        if !matches!(input, Datum::Null)
                            && current.as_ref().is_none_or(|value| {
                                tidb_expr::compare_datums_with_collation(input, value, *collation)
                                    .is_ok_and(|order| order == Ordering::Greater)
                            })
                        {
                            *current = Some(input.clone());
                        } else if let Some(value) = current.as_ref() {
                            tidb_expr::compare_datums_with_collation(input, value, *collation)?;
                        }
                    }
                    (IndexLookupAggregateOutput::DecimalSum(offset), Partial::DecimalSum(sum)) => {
                        match row.get(*offset) {
                            Some(Datum::Null) => {}
                            Some(Datum::Decimal(value)) => {
                                *sum = Some(match sum.take() {
                                    Some(sum) => sum.add(value),
                                    None => value.clone(),
                                });
                            }
                            Some(_) => {
                                return Err(ExecError::unsupported(
                                    "an index lookup decimal SUM received a non-decimal value",
                                ))
                            }
                            None => {
                                return Err(ExecError::unsupported(
                                    "an index lookup SUM input offset is absent",
                                ))
                            }
                        }
                    }
                    _ => unreachable!("aggregate output and partial state are built together"),
                }
            }
            Ok(())
        };
        let finish = |group: Group| {
            self.outputs
                .iter()
                .zip(group.partials)
                .map(|(output, partial)| match (output, partial) {
                    (IndexLookupAggregateOutput::Column(offset), Partial::Column) => {
                        group.first.get(*offset).cloned().ok_or_else(|| {
                            ExecError::unsupported(
                                "an index lookup aggregation output offset is absent",
                            )
                        })
                    }
                    (IndexLookupAggregateOutput::Count(_), Partial::Count(count)) => {
                        Ok(Datum::Int(count))
                    }
                    (IndexLookupAggregateOutput::Max { .. }, Partial::Max(value)) => {
                        Ok(value.unwrap_or(Datum::Null))
                    }
                    (IndexLookupAggregateOutput::DecimalSum(_), Partial::DecimalSum(sum)) => {
                        Ok(sum.map_or(Datum::Null, Datum::Decimal))
                    }
                    _ => unreachable!("aggregate output and partial state are built together"),
                })
                .collect::<Result<Vec<_>, ExecError>>()
        };
        if stream_ordered {
            let mut output = Vec::new();
            let mut current: Option<(Vec<u8>, Group)> = None;
            for row in rows {
                let key = key_of(&row)?;
                if current.as_ref().is_some_and(|(group, _)| *group != key) {
                    output.push(finish(current.take().expect("a different group exists").1)?);
                }
                let group = &mut current
                    .get_or_insert_with(|| {
                        (
                            key,
                            Group {
                                first: row.clone(),
                                partials: new_partials(),
                            },
                        )
                    })
                    .1;
                update(group, &row)?;
            }
            if let Some((_, group)) = current {
                output.push(finish(group)?);
            }
            return Ok(output);
        }

        let mut positions = std::collections::HashMap::<Vec<u8>, usize>::new();
        let mut groups = Vec::<Group>::new();
        for row in rows {
            let key = key_of(&row)?;
            let position = match positions.get(&key).copied() {
                Some(position) => position,
                None => {
                    let position = groups.len();
                    positions.insert(key, position);
                    groups.push(Group {
                        first: row.clone(),
                        partials: new_partials(),
                    });
                    position
                }
            };
            update(&mut groups[position], &row)?;
        }
        groups.into_iter().map(finish).collect()
    }
}

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
    /// An aggregation retained by a grouped derived lookup side.  A bare
    /// table lookup has no transformation.
    pub(crate) aggregation: Option<IndexLookupAggregation>,
    /// Whether the lookup key order makes equal aggregate groups contiguous.
    pub(crate) aggregation_stream_ordered: bool,
    /// Outer-child columns a null-rejecting join predicate proves non-NULL.
    pub(crate) outer_not_null: Vec<usize>,
    /// Lookup-result columns the same predicate proves non-NULL, evaluated
    /// after a retained derived aggregation.
    pub(crate) inner_not_null: Vec<usize>,
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
    inner_is_left: bool,
    inner_group: Option<MergeInnerGroup>,
    pending: Option<MergePendingOutput>,
}

/// Output cursor for a merge group whose cross product or outer misses span
/// several requested chunks.
enum MergePendingOutput {
    Matched {
        outer: Vec<Vec<Datum>>,
        retained_bytes: i64,
        outer_index: usize,
        inner_ptr: Option<RowPtr>,
        matched_current_outer: bool,
    },
    Unmatched {
        outer: Vec<Vec<Datum>>,
        retained_bytes: i64,
        outer_index: usize,
    },
}

impl MergePendingOutput {
    fn retained_bytes(&self) -> i64 {
        match self {
            Self::Matched { retained_bytes, .. } | Self::Unmatched { retained_bytes, .. } => {
                *retained_bytes
            }
        }
    }
}

/// A join of two children, hashing its equal conditions when it can and
/// falling back to a nested loop when it cannot (see the module doc).
pub struct JoinExec<C: Columns> {
    meta: ExecutorMeta,
    kind: JoinKind,
    conditions: Vec<Expression>,
    /// The joined left-then-right row types the conditions read. Semi joins
    /// return only their left child, so this cannot be derived from `meta`.
    condition_types: Vec<FieldType>,
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    ctx: C,
    /// The indexable `col = col` conjuncts; empty means the nested loop.
    keys: Vec<EquiKey>,
    /// Nested loop only: whether its single all-at-once batch was emitted.
    emitted: bool,
    hash: Option<HashState>,
    /// The costed build side for an inner hash join. Outer joins keep their
    /// preserved side as the probe because unmatched build rows require a
    /// different executor contract.
    hash_build_is_left: Option<bool>,
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
    /// True only when the committed join strategy installed every leaf-local
    /// filter and every inter-leaf equality from the written `WHERE`.
    consumes_where: bool,
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
        let condition_types = left
            .ret_field_types()
            .iter()
            .chain(right.ret_field_types())
            .cloned()
            .collect();
        let tracker = memory.operator_tracker(meta.id());
        let disk_tracker = memory.operator_disk_tracker(meta.id());
        JoinExec {
            meta,
            kind,
            conditions,
            condition_types,
            left,
            right,
            ctx,
            keys,
            emitted: false,
            hash: None,
            hash_build_is_left: None,
            merge: None,
            merge_state: None,
            index_lookup: None,
            index_state: None,
            consumes_where: false,
            condition_evals: Cell::new(0),
            memory,
            tracker,
            disk_tracker,
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
    /// `hashRowContainer.chkBuf`) is holding. It stays empty for an in-memory
    /// build and holds exactly one decoded row after a spilled probe -- a
    /// growing value would mean the buffer accumulates every row the join ever
    /// read back from disk.
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

    /// Commits the build orientation chosen by the physical cost search.
    /// Both orientations are semantically interchangeable only for an inner
    /// join, which is also the only case accepted here.
    pub(crate) fn set_hash_build_is_left(&mut self, build_is_left: bool) {
        if self.kind == JoinKind::Inner {
            self.hash_build_is_left = Some(build_is_left);
        }
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
            None => self
                .hash_build_is_left
                .map_or(self.kind != JoinKind::Right, |build_is_left| !build_is_left),
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
        let mut chunk = Chunk::new_with_capacity(&self.condition_types, 1);
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
            match self.kind {
                JoinKind::Inner | JoinKind::Left | JoinKind::Right => {
                    Self::append(req, &joined);
                }
                JoinKind::Semi => {
                    Self::append(req, outer_row);
                    break;
                }
                JoinKind::AntiSemi => break,
            }
        }
        if !matched {
            match self.kind {
                JoinKind::Left | JoinKind::Right => {
                    Self::append(req, &self.padded_row(outer_row));
                }
                JoinKind::AntiSemi => Self::append(req, outer_row),
                JoinKind::Inner | JoinKind::Semi => {}
            }
        }
        Ok(())
    }

    /// Declares that this join looks its inner side up per outer batch.
    ///
    /// As with [`Self::set_merge_plan`] the promise is the caller's: only
    /// `driver::index_join_decision` makes it, and only after checking that
    /// the probed object's key columns ARE the join's own equality columns.
    pub(crate) fn set_index_lookup_plan(&mut self, plan: IndexLookupPlan) {
        if matches!(
            self.kind,
            JoinKind::Inner | JoinKind::Left | JoinKind::Right
        ) {
            self.index_lookup = Some(plan);
        }
    }

    /// Records that this committed join tree enforces the complete `WHERE`.
    pub(crate) fn set_consumes_where(&mut self, consumes_where: bool) {
        self.consumes_where = consumes_where;
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
        let retained = state
            .outer
            .iter()
            .chain(&state.inner)
            .map(|row| row_bytes(row))
            .sum::<i64>();
        tracker.consume(-retained);
        state.outer.clear();
        state.inner.clear();
        state.matched.clear();
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
            if !row_non_null_at(&row, &plan.outer_not_null)? {
                continue;
            }
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
                null_safe: false,
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
        if let Some(aggregation) = &plan.aggregation {
            let raw_bytes = state.inner.iter().map(|row| row_bytes(row)).sum::<i64>();
            let aggregated = aggregation.apply(
                std::mem::take(&mut state.inner),
                plan.aggregation_stream_ordered,
            )?;
            let aggregated_bytes = aggregated.iter().map(|row| row_bytes(row)).sum::<i64>();
            tracker.consume(aggregated_bytes - raw_bytes);
            state.inner = aggregated;
            memory.check()?;
        }
        if !plan.inner_not_null.is_empty() {
            let before = state.inner.iter().map(|row| row_bytes(row)).sum::<i64>();
            let mut retained = Vec::with_capacity(state.inner.len());
            for row in std::mem::take(&mut state.inner) {
                if row_non_null_at(&row, &plan.inner_not_null)? {
                    retained.push(row);
                }
            }
            let after = retained.iter().map(|row| row_bytes(row)).sum::<i64>();
            tracker.consume(after - before);
            state.inner = retained;
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
        if matches!(
            self.kind,
            JoinKind::Inner | JoinKind::Left | JoinKind::Right
        ) {
            self.merge = Some(plan);
        }
    }

    /// Whether this join merges its two sorted children.
    #[must_use]
    pub fn is_merge_join(&self) -> bool {
        self.merge.is_some()
    }

    /// Pulls one OUTER row into owned datums.
    ///
    /// The equal-key INNER run remains installed across calls, so adjacent
    /// OUTER rows with the same key reuse it. This is the bounded streaming
    /// half of Go's merge join: only the INNER run is materialized and
    /// spillable.
    fn fetch_outer_group(
        side: &mut MergeSide,
        child: &mut dyn Executor,
        key_offsets: &[usize],
        types: &[FieldType],
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
    ) -> Result<(), ExecError> {
        tracker.consume(-side.group_bytes);
        side.group.clear();
        side.group_bytes = 0;
        side.group_len = 0;
        side.key.clear();
        while side.row >= side.chunk.num_rows() {
            if side.done {
                return Ok(());
            }
            let result = child.next(&mut side.chunk);
            let current_bytes = side.chunk.memory_usage();
            tracker.consume(current_bytes - side.chunk_bytes);
            side.chunk_bytes = current_bytes;
            result?;
            memory.check()?;
            side.row = 0;
            if side.chunk.num_rows() == 0 {
                side.done = true;
                return Ok(());
            }
        }

        let row = datum_row(&side.chunk, side.row, types);
        side.key = key_offsets.iter().map(|&at| row[at].clone()).collect();
        let bytes = row_bytes(&row);
        tracker.consume(bytes);
        side.group_bytes = bytes;
        side.group.push(row);
        side.group_len = 1;
        side.row += 1;
        memory.check()
    }

    /// Pulls the next INNER group into the spillable row container. Only the
    /// key is materialized as datums; complete rows remain chunk encoded.
    fn fetch_inner_group(
        side: &mut MergeSide,
        child: &mut dyn Executor,
        key_offsets: &[usize],
        types: &[FieldType],
        group: &mut MergeInnerGroup,
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
    ) -> Result<(), ExecError> {
        group.reset();
        side.group.clear();
        side.group_bytes = 0;
        side.group_len = 0;
        side.key.clear();
        loop {
            if side.row >= side.chunk.num_rows() {
                if side.done {
                    break;
                }
                let result = child.next(&mut side.chunk);
                let current_bytes = side.chunk.memory_usage();
                tracker.consume(current_bytes - side.chunk_bytes);
                side.chunk_bytes = current_bytes;
                result?;
                memory.check()?;
                side.row = 0;
                if side.chunk.num_rows() == 0 {
                    side.done = true;
                    break;
                }
            }
            let row = side.chunk.get_row(side.row);
            let key: Vec<Datum> = key_offsets
                .iter()
                .map(|&at| row.get_datum(at, &types[at]))
                .collect();
            if side.group_len == 0 {
                side.key = key;
            } else if merge_key_cmp(&side.key, &key, false)? != Ordering::Equal {
                break;
            }
            group.append(row, tracker, memory)?;
            side.group_len += 1;
            side.row += 1;
        }
        group.finish_group(tracker)?;
        memory.check()
    }

    /// Drains as much of the current merge cross product as the caller's
    /// requested chunk can hold. The cursor is restored to `MergeState` even
    /// when expression evaluation or a spill read fails, so `close` can still
    /// release the container and action.
    fn drain_merge_pending(&mut self, req: &mut Chunk) -> Result<bool, ExecError> {
        let (mut pending, mut inner) = {
            let state = self.merge_state.as_mut().expect("merge state exists");
            (
                state.pending.take().expect("pending output exists"),
                state.inner_group.take().expect("inner group installed"),
            )
        };
        let mut error = None;
        match &mut pending {
            MergePendingOutput::Matched {
                outer,
                retained_bytes: _,
                outer_index,
                inner_ptr,
                matched_current_outer,
            } => {
                while *outer_index < outer.len() && !req.is_full() {
                    if let Some(ptr) = *inner_ptr {
                        let next = inner.next_ptr(ptr);
                        let inner_row = match inner.datum_row(ptr) {
                            Ok(row) => row,
                            Err(current) => {
                                error = Some(current);
                                break;
                            }
                        };
                        *inner_ptr = next;
                        let joined = self.join_rows(&outer[*outer_index], &inner_row);
                        match self.matches(&joined) {
                            Ok(true) => {
                                *matched_current_outer = true;
                                Self::append(req, &joined);
                            }
                            Ok(false) => {}
                            Err(current) => {
                                error = Some(current);
                                break;
                            }
                        }
                        continue;
                    }

                    if !*matched_current_outer && self.kind != JoinKind::Inner {
                        Self::append(req, &self.padded_row(&outer[*outer_index]));
                    }
                    *outer_index += 1;
                    *matched_current_outer = false;
                    if *outer_index < outer.len() {
                        *inner_ptr = inner.first_ptr();
                    }
                }
            }
            MergePendingOutput::Unmatched {
                outer,
                retained_bytes: _,
                outer_index,
            } => {
                while *outer_index < outer.len() && !req.is_full() {
                    Self::append(req, &self.padded_row(&outer[*outer_index]));
                    *outer_index += 1;
                }
            }
        }
        let done = match &pending {
            MergePendingOutput::Matched {
                outer, outer_index, ..
            }
            | MergePendingOutput::Unmatched {
                outer, outer_index, ..
            } => *outer_index == outer.len(),
        };
        if done {
            self.tracker.consume(-pending.retained_bytes());
        }
        let state = self.merge_state.as_mut().expect("merge state exists");
        state.inner_group = Some(inner);
        if !done {
            state.pending = Some(pending);
        }
        if let Some(error) = error {
            return Err(error);
        }
        Ok(done)
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
        let outer_is_left = self.outer_is_left();
        if self.merge_state.is_none() {
            let inner_is_left = !outer_is_left;
            let inner_types = if inner_is_left {
                left_types.clone()
            } else {
                right_types.clone()
            };
            let mut inner_group = MergeInnerGroup::new(
                inner_types,
                self.meta.max_chunk_size(),
                &self.memory,
                &self.tracker,
                &self.disk_tracker,
            );
            if self.memory.tmp_storage_on_oom() {
                let action: ArcAction = inner_group.rows.action_spill();
                self.memory
                    .session_tracker()
                    .fallback_old_and_set_new_action(Arc::clone(&action));
                self.registered_action = Some(action);
            }
            let staging_bytes = inner_group.staging.memory_usage();
            let left = MergeSide::new(self.left.new_chunk());
            let right = MergeSide::new(self.right.new_chunk());
            let input_bytes = left.chunk_bytes + right.chunk_bytes;
            self.merge_state = Some(MergeState {
                left,
                right,
                inner_is_left,
                inner_group: Some(inner_group),
                pending: None,
            });
            self.tracker.consume(staging_bytes + input_bytes);
            self.memory.check()?;
        }
        let tracker = Arc::clone(&self.tracker);
        let memory = self.memory.clone();
        loop {
            if self
                .merge_state
                .as_ref()
                .is_some_and(|state| state.pending.is_some())
            {
                self.drain_merge_pending(req)?;
                self.memory.check()?;
                if req.num_rows() > 0 {
                    return Ok(());
                }
                continue;
            }
            let (already_spilled, disk_bytes) = {
                let state = self.merge_state.as_ref().expect("just created");
                let inner = state.inner_group.as_ref().expect("inner group installed");
                (
                    inner.rows.already_spilled(),
                    inner.rows.disk_tracker().bytes_consumed(),
                )
            };
            self.build_spilled |= already_spilled;
            self.spilled_bytes = self.spilled_bytes.max(disk_bytes);

            let state = self.merge_state.as_mut().expect("just created");
            if state.left.group_len == 0 {
                if state.inner_is_left {
                    let MergeState {
                        left, inner_group, ..
                    } = state;
                    Self::fetch_inner_group(
                        left,
                        self.left.as_mut(),
                        &left_keys,
                        &left_types,
                        inner_group.as_mut().expect("inner group installed"),
                        &tracker,
                        &memory,
                    )?;
                } else {
                    Self::fetch_outer_group(
                        &mut state.left,
                        self.left.as_mut(),
                        &left_keys,
                        &left_types,
                        &tracker,
                        &memory,
                    )?;
                }
            }
            let state = self.merge_state.as_mut().expect("just created");
            if state.right.group_len == 0 {
                if state.inner_is_left {
                    Self::fetch_outer_group(
                        &mut state.right,
                        self.right.as_mut(),
                        &right_keys,
                        &right_types,
                        &tracker,
                        &memory,
                    )?;
                } else {
                    let MergeState {
                        right, inner_group, ..
                    } = state;
                    Self::fetch_inner_group(
                        right,
                        self.right.as_mut(),
                        &right_keys,
                        &right_types,
                        inner_group.as_mut().expect("inner group installed"),
                        &tracker,
                        &memory,
                    )?;
                }
            }
            let state = self.merge_state.as_mut().expect("just created");
            let (left_empty, right_empty) = (state.left.group_len == 0, state.right.group_len == 0);
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
                    let outer_bytes = {
                        let state = self.merge_state.as_mut().expect("state exists");
                        let (outer, bytes) = if outer_is_left {
                            state.left.group_len = 0;
                            let bytes = std::mem::take(&mut state.left.group_bytes);
                            (std::mem::take(&mut state.left.group), bytes)
                        } else {
                            state.right.group_len = 0;
                            let bytes = std::mem::take(&mut state.right.group_bytes);
                            (std::mem::take(&mut state.right.group), bytes)
                        };
                        let first = state
                            .inner_group
                            .as_ref()
                            .expect("inner group installed")
                            .first_ptr();
                        state.pending = Some(MergePendingOutput::Matched {
                            outer,
                            retained_bytes: bytes,
                            outer_index: 0,
                            inner_ptr: first,
                            matched_current_outer: false,
                        });
                        bytes
                    };
                    debug_assert!(outer_bytes >= 0);
                    self.drain_merge_pending(req)?;
                }
                // The left group is behind, or the right side is spent.
                Ordering::Less => {
                    let (group, bytes, is_outer) = {
                        let state = self.merge_state.as_mut().expect("state exists");
                        state.left.group_len = 0;
                        let group = std::mem::take(&mut state.left.group);
                        let bytes = std::mem::take(&mut state.left.group_bytes);
                        (group, bytes, outer_is_left)
                    };
                    if is_outer && self.kind != JoinKind::Inner {
                        self.merge_state.as_mut().expect("state exists").pending =
                            Some(MergePendingOutput::Unmatched {
                                outer: group,
                                retained_bytes: bytes,
                                outer_index: 0,
                            });
                        self.drain_merge_pending(req)?;
                    } else {
                        tracker.consume(-bytes);
                    }
                }
                Ordering::Greater => {
                    let (group, bytes, is_outer) = {
                        let state = self.merge_state.as_mut().expect("state exists");
                        state.right.group_len = 0;
                        let group = std::mem::take(&mut state.right.group);
                        let bytes = std::mem::take(&mut state.right.group_bytes);
                        (group, bytes, !outer_is_left)
                    };
                    if is_outer && self.kind != JoinKind::Inner {
                        self.merge_state.as_mut().expect("state exists").pending =
                            Some(MergePendingOutput::Unmatched {
                                outer: group,
                                retained_bytes: bytes,
                                outer_index: 0,
                            });
                        self.drain_merge_pending(req)?;
                    } else {
                        tracker.consume(-bytes);
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
        let mut table = BuildTable::new(
            &build_types,
            self.meta.max_chunk_size(),
            self.memory.spill_storage(),
        );
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
fn row_non_null_at(row: &[Datum], offsets: &[usize]) -> Result<bool, ExecError> {
    for offset in offsets {
        let value = row.get(*offset).ok_or_else(|| {
            ExecError::unsupported("an index join null-rejection offset is absent")
        })?;
        if matches!(value, Datum::Null) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn row_bytes(row: &[Datum]) -> i64 {
    let mut bytes = i64::try_from(size_of::<Vec<Datum>>()).unwrap_or(i64::MAX);
    for datum in row {
        bytes += i64::try_from(datum.estimated_mem_usage()).unwrap_or(i64::MAX);
    }
    bytes
}

/// One chunk row as owned `Datum`s.
fn datum_row(chunk: &Chunk, index: usize, types: &[FieldType]) -> Vec<Datum> {
    chunk.get_row(index).get_datum_row(types)
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
        if self.index_lookup.is_some() {
            if self.outer_is_left() {
                self.left.open()?;
            } else {
                self.right.open()?;
            }
        } else {
            self.left.open()?;
            self.right.open()?;
        }
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
        if let Some(mut state) = self.merge_state.take() {
            self.tracker.consume(-state.left.group_bytes);
            self.tracker.consume(-state.right.group_bytes);
            self.tracker.consume(-state.left.chunk_bytes);
            self.tracker.consume(-state.right.chunk_bytes);
            if let Some(pending) = state.pending.take() {
                self.tracker.consume(-pending.retained_bytes());
            }
            if let Some(inner) = state.inner_group.as_mut() {
                self.build_spilled |= inner.rows.already_spilled();
                self.spilled_bytes = self
                    .spilled_bytes
                    .max(inner.rows.disk_tracker().bytes_consumed());
                inner.close(&self.tracker);
            }
        }
        if let Some(action) = self.registered_action.take() {
            self.memory
                .session_tracker()
                .unbind_action_from_hard_limit(&action);
        }
        if let Some(plan) = self.index_lookup.as_mut() {
            plan.source.close()?;
        }
        if self.index_lookup.is_some() {
            if self.outer_is_left() {
                self.left.close()
            } else {
                self.right.close()
            }
        } else {
            self.left.close()?;
            self.right.close()
        }
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

    fn consumes_where(&self) -> bool {
        self.consumes_where
    }
}

#[cfg(test)]
#[path = "join_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "join_merge_path_tests.rs"]
mod merge_path_tests;

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
#[path = "join_spill_tests.rs"]
mod spill_tests;
