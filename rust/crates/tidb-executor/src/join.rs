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
//! refuses), the join builds a hash table on the costed build side and streams
//! the other side through it: O(build + probe x fanout) instead of
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
//! Go `getHashJoins` enumerates both build orientations for inner, left outer,
//! and right outer joins. Its first stats-less candidate builds the right
//! child for `INNER`/`LEFT` and the left child for `RIGHT`; physical-plan cost
//! may pick the other orientation when statistics make that cheaper.
//!
//! Building the non-preserved side keeps outer semantics single-pass: an
//! unmatched preserved probe row is padded immediately. Building the
//! preserved side follows Go v2 `outerJoinProbe`: a candidate is marked only
//! after every ON condition succeeds, then a post-probe scan emits each
//! unmarked build row with NULL padding. The matched bitmap also covers NULL
//! keys, which are stored but never inserted into a hash bucket.
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
//! executor is v1-SHAPED -- one build container and an in-memory
//! key-to-pointer map -- so v1's spill is the one ported here.
//! Building v2's partitioned machinery onto it would be a rewrite, not a
//! port, and is NOT started.
//!
//! The unique single-integer slice uses Go's default five-way probe boundary:
//! the session thread fetches a bounded window, workers share the immutable
//! build table and own their chunks, and matched-bitmap writes return to the
//! session thread. Pure equality and q17's proven DECIMAL residual shape use
//! that boundary. General residual conditions, parallel build, semi/anti
//! preserved-build variants, v2's partitioned spill, and outer-apply remain
//! deferred. Hash-aggregate spill, TopN spill, parallel-sort spill and
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
use crate::hash_join::{
    equi_keys_equal_chunk_rows, equi_keys_equal_row, exact_int_key_chunk, row_hash, row_hash_chunk,
    row_key, row_key_by, BuildError, BuildTable, EquiKey, FastBytesMap, KeyClass, KeyError,
};
use crate::mem_quota::StatementMemory;
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_chunk::list::List;
use tidb_chunk::list::RowPtr;
use tidb_chunk::row::Row;
use tidb_chunk::row_container::RowContainer;
use tidb_datatype::{Collation, Datum, Decimal, EvalType, FieldType, MyDecimal};
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
    /// The materialized, indexed build side.
    table: BuildTable,
    /// The build side's column types, needed to read a row back out of the
    /// container (which stores bytes, not `Datum`s).
    build_types: Vec<FieldType>,
    /// Go `hashRowContainer.chkBuf`: the landing chunk a spilled build row is
    /// read back into. Reused across probes so a disk-backed join does not
    /// allocate per matched row.
    build_buf: Chunk,
    /// The chunk the probe child streams into, and how far it is consumed.
    probe_chunk: Chunk,
    probe_row: usize,
    probe_done: bool,
    /// Products of a constant DECIMAL factor and a build-side DECIMAL column,
    /// keyed by the stable build-row address. `Some(None)` caches SQL NULL.
    /// This avoids repeating q17's `0.2 * AVG(...)` for every probe candidate.
    decimal_mul_products: HashMap<RowPtr, Option<MyDecimal>>,
    /// Cursor for Go hash join's post-probe scan when the preserved side was
    /// built. `None` means the scan is complete (or was never needed).
    unmatched_build_scan: Option<RowPtr>,
    /// Results produced by the bounded exact-integer probe workers, kept in
    /// probe-input order so this path preserves the serial executor's row
    /// order as well as its SQL result.
    parallel_probe_pending: VecDeque<Chunk>,
    /// Go returns consumed probe/result chunks to the fetcher/workers through
    /// resource channels. These two pools are the same ownership loop without
    /// exposing the non-`Send` executor tree to worker threads.
    parallel_probe_input_reuse: Vec<Chunk>,
    parallel_probe_output_reuse: Vec<Chunk>,
    /// True only for the first bounded parallel slice: one ordinary integer
    /// equality key and a unique exact build bucket.
    parallel_exact_int_enabled: bool,
    /// Number of bounded probe windows executed by the parallel exact-integer
    /// path. Kept as an execution-path receipt for focused regression tests.
    parallel_probe_windows: usize,
}

/// One worker's complete result for one source chunk. Pure equality over a
/// unique build key produces at most one joined row per probe row, so this
/// stays bounded to one input and one output chunk like Go's worker channel.
struct ParallelProbeResult {
    input: Chunk,
    output: Chunk,
    matched_build_rows: Vec<RowPtr>,
    condition_evals: u64,
}

/// Go resolves the default hash-join concurrency through
/// `tidb_executor_concurrency`, whose default is five probe workers.
const HASH_JOIN_CONCURRENCY: usize = 5;

/// Number of source chunks one scoped worker consumes before the join pays
/// the cost of creating the worker threads again. Go keeps its hash-join
/// goroutines alive for the complete probe; a bounded multi-chunk lane gives
/// the scoped Rust implementation the same amortization without allowing
/// probe/output memory to grow with the complete input.
const PARALLEL_PROBE_CHUNKS_PER_WORKER: usize = 8;

/// A residual DECIMAL comparison whose operands can be read directly from
/// the two input rows. TPC-H q17's `l_quantity < 0.2 * avg(l_quantity)` is the
/// hot instance. Keeping this as a narrowly proven shape avoids copying the
/// complete joined row into `condition_chunk` for every hash candidate while
/// leaving every other expression on the general evaluator path.
#[derive(Clone, Debug)]
struct DecimalMulLtFastPath {
    left_column: usize,
    right_column: usize,
    factor: Decimal,
}

/// One side of the merge strategy: the chunk it streams into, how far that
/// chunk is consumed, and the current equal-key group metadata.
///
/// The OUTER side retains a range in its current child chunk. The INNER side
/// sets `group_len` and writes its rows to [`MergeInnerGroup`], which is the
/// spillable authority accepted TiDB uses for a run crossing chunk bounds.
struct MergeSide {
    chunk: Chunk,
    /// The exact live capacity charge for `chunk`.
    chunk_bytes: i64,
    row: usize,
    /// Whether the child has returned its final (empty) chunk.
    done: bool,
    /// The first row in the current OUTER equal-key range.
    group_start: usize,
    /// One-past-the-end row in the current OUTER equal-key range.
    group_end: usize,
    /// Number of rows in the current group. The inner group is not stored in
    /// `group`, so emptiness cannot be derived from the vector.
    group_len: usize,
    /// The key of the current group.
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
            group_start: 0,
            group_end: 0,
            group_len: 0,
            key: Vec::new(),
        }
    }
}

#[derive(Clone, Copy)]
struct MergeOuterRange {
    side_left: bool,
    start: usize,
    end: usize,
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
    read_back: Chunk,
    types: Vec<FieldType>,
}

#[derive(Clone, Copy)]
enum MergeInnerPtr {
    Stored(RowPtr),
    Staging(usize),
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
        let read_back = Chunk::new_with_capacity(&types, 1);
        Self {
            rows,
            staging,
            read_back,
            types,
        }
    }

    fn reset(&mut self) {
        self.staging.reset();
        self.read_back.reset();
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
        tracker.consume(-self.staging.memory_usage() - self.read_back.memory_usage());
        self.staging = Chunk::default();
        self.read_back = Chunk::default();
        self.rows.close();
    }

    fn first_ptr(&self) -> Option<MergeInnerPtr> {
        if self.rows.num_chunks() != 0 {
            return Some(MergeInnerPtr::Stored(RowPtr::new(0, 0)));
        }
        (self.staging.num_rows() != 0).then_some(MergeInnerPtr::Staging(0))
    }

    fn next_ptr(&self, ptr: MergeInnerPtr) -> Option<MergeInnerPtr> {
        match ptr {
            MergeInnerPtr::Staging(row) => {
                (row + 1 < self.staging.num_rows()).then_some(MergeInnerPtr::Staging(row + 1))
            }
            MergeInnerPtr::Stored(ptr) => {
                let chunk_index = ptr.chk_idx as usize;
                let next_row = ptr.row_idx as usize + 1;
                if next_row < self.rows.num_rows_of_chunk(chunk_index) {
                    return Some(MergeInnerPtr::Stored(RowPtr::new(
                        ptr.chk_idx,
                        next_row as u32,
                    )));
                }
                let next_chunk = chunk_index + 1;
                if next_chunk < self.rows.num_chunks() {
                    return Some(MergeInnerPtr::Stored(RowPtr::new(next_chunk as u32, 0)));
                }
                (self.staging.num_rows() != 0).then_some(MergeInnerPtr::Staging(0))
            }
        }
    }

    fn datum_row(&mut self, ptr: MergeInnerPtr) -> Result<Vec<Datum>, ExecError> {
        match ptr {
            MergeInnerPtr::Staging(row) => Ok(self.staging.get_row(row).get_datum_row(&self.types)),
            MergeInnerPtr::Stored(ptr) => {
                self.read_back.reset();
                let row = {
                    let loaded = self
                        .rows
                        .get_row_and_append_to_chunk_if_in_disk(ptr, &mut self.read_back)
                        .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
                    loaded.row(&self.read_back).get_datum_row(&self.types)
                };
                self.read_back.reset();
                Ok(row)
            }
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

/// Go resolves the unset `tidb_index_lookup_join_concurrency` through
/// `tidb_executor_concurrency`, whose default is five inner workers.
const INDEX_LOOKUP_JOIN_CONCURRENCY: usize = 5;

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
    /// The field types of one aggregated output row, in output order: every
    /// FIRST_ROW/MAX/SUM carrier keeps its source column's type and COUNT is
    /// Go's `count(1)` INT64. These are the types of the rows [`Self::apply`]
    /// returns -- the physical lookup layout only describes its INPUTS.
    fn output_types(&self, source_types: &[FieldType]) -> Vec<FieldType> {
        self.outputs
            .iter()
            .map(|output| match output {
                IndexLookupAggregateOutput::Column(offset)
                | IndexLookupAggregateOutput::Max { offset, .. }
                | IndexLookupAggregateOutput::DecimalSum(offset) => source_types
                    .get(*offset)
                    .cloned()
                    .expect("an aggregate output names one of the lookup's own columns"),
                IndexLookupAggregateOutput::Count(_) => {
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                }
            })
            .collect()
    }

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
pub(crate) enum IndexLookupSource {
    /// The lookup side is one base-table leaf.
    Leaf(crate::access_path::IndexJoinLookupExec),
    /// The lookup side is a full subtree whose target leaf consumes the
    /// shared probe channel published by the enclosing join.
    Composite {
        exec: Box<dyn Executor>,
        probes: std::rc::Rc<std::cell::RefCell<crate::access_path::SharedIndexJoinProbes>>,
    },
}

impl IndexLookupSource {
    fn fork_prefetched_common_handle(
        &self,
        probes: Vec<Vec<Datum>>,
    ) -> Result<Option<Self>, ExecError> {
        match self {
            Self::Leaf(source) => source
                .fork_prefetched_common_handle(probes)
                .map(|source| source.map(Self::Leaf)),
            Self::Composite { .. } => Ok(None),
        }
    }

    fn set_probes(&mut self, probes: Vec<Vec<Datum>>) -> Result<(), ExecError> {
        match self {
            Self::Leaf(source) => {
                source.set_probes(probes);
                Ok(())
            }
            Self::Composite {
                exec,
                probes: shared,
            } => {
                exec.close()?;
                shared.borrow_mut().publish(probes);
                exec.open()
            }
        }
    }

    fn open(&mut self) -> Result<(), ExecError> {
        match self {
            Self::Leaf(source) => source.open(),
            Self::Composite { exec, .. } => exec.open(),
        }
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        match self {
            Self::Leaf(source) => source.next(req),
            Self::Composite { exec, .. } => exec.next(req),
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        match self {
            Self::Leaf(source) => source.close(),
            Self::Composite { exec, .. } => exec.close(),
        }
    }

    fn ret_field_types(&self) -> &[FieldType] {
        match self {
            Self::Leaf(source) => source.ret_field_types(),
            Self::Composite { exec, .. } => exec.ret_field_types(),
        }
    }

    fn new_chunk(&self) -> Chunk {
        match self {
            Self::Leaf(source) => source.new_chunk(),
            Self::Composite { exec, .. } => exec.new_chunk(),
        }
    }
}

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
    pub(crate) source: IndexLookupSource,
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
    inner: List,
    /// Bytes charged for the current inner list, released at the next batch.
    inner_bytes: i64,
    /// Every equality key's encoding to the `inner` positions carrying it.
    matched: FastBytesMap<Vec<RowPtr>>,
    /// The chunk the outer child streams into, and how far it is consumed.
    outer_chunk: Chunk,
    outer_row: usize,
    /// Whether the outer child has returned its final (empty) chunk.
    outer_done: bool,
    /// The next batch's size, doubling to [`INDEX_JOIN_BATCH_SIZE`] as Go's
    /// `increaseBatchSize` does.
    batch_size: usize,
    /// Inner tasks whose remote readers have already been opened. They remain
    /// in outer-task order even when a later TiKV request finishes first.
    pending: std::collections::VecDeque<PendingIndexLookupTask>,
    /// Once a lookup shape refuses remote prefetch, retain the synchronous
    /// path for the rest of this executor.
    prefetch_disabled: bool,
}

struct PendingIndexLookupTask {
    outer: Vec<Vec<Datum>>,
    outer_bytes: i64,
    source: PendingIndexLookupSource,
}

enum PendingIndexLookupSource {
    Prefetched(IndexLookupSource),
    Synchronous(Vec<Vec<Datum>>),
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
        outer: MergeOuterRange,
        retained_bytes: i64,
        outer_index: usize,
        inner_ptr: Option<MergeInnerPtr>,
        matched_current_outer: bool,
    },
    Unmatched {
        outer: MergeOuterRange,
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
    /// The complete logical `ON` clause. The nested-loop reference path must
    /// retain every condition, including equality keys.
    conditions: Vec<Expression>,
    /// The non-equality `otherCond` expressions that still need evaluation
    /// after the hash/merge/index key has matched. Go's hash join removes
    /// equal conditions from this list before probing; retaining them here
    /// would duplicate work and can rebuild a condition chunk with a schema
    /// that no longer describes a projected join row.
    residual_conditions: Vec<Expression>,
    /// The joined left-then-right row types the residual conditions read. Semi joins
    /// return only their left child, so this cannot be derived from `meta`.
    condition_types: Vec<FieldType>,
    /// Reused one-row input for residual predicates. Go evaluates join
    /// conditions over chunk rows; retaining this chunk avoids rebuilding its
    /// columns for every hash-table candidate.
    condition_chunk: Chunk,
    /// A structurally proven DECIMAL residual predicate that can bypass the
    /// one-row condition chunk. `None` keeps the complete expression path.
    residual_decimal_mul_lt: Option<DecimalMulLtFastPath>,
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    ctx: C,
    /// The indexable `col = col` conjuncts; empty means the nested loop.
    keys: Vec<EquiKey>,
    /// Nested loop only: whether its single all-at-once batch was emitted.
    emitted: bool,
    hash: Option<HashState>,
    /// The costed build side for a hash join. Go hash join v2 enumerates both
    /// orientations for inner, left outer, and right outer joins.
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
        let split = crate::hash_join::split_equi(&conditions, left.ret_field_types().len());
        let flattened = conditions
            .iter()
            .flat_map(crate::hash_join::split_conjuncts)
            .collect::<Vec<_>>();
        let residual_conditions: Vec<Expression> = flattened
            .into_iter()
            .zip(split.equal_mask)
            .filter_map(|(condition, is_equal)| (!is_equal).then_some(condition.clone()))
            .collect();
        let keys = split.keys;
        let condition_types: Vec<FieldType> = left
            .ret_field_types()
            .iter()
            .chain(right.ret_field_types())
            .cloned()
            .collect();
        let condition_chunk = Chunk::new_with_capacity(&condition_types, 1);
        let residual_decimal_mul_lt = residual_decimal_mul_lt(&residual_conditions);
        let tracker = memory.operator_tracker(meta.id());
        let disk_tracker = memory.operator_disk_tracker(meta.id());
        JoinExec {
            meta,
            kind,
            conditions,
            residual_conditions,
            condition_types,
            condition_chunk,
            residual_decimal_mul_lt,
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
    /// Outer and semi joins accept both orientations because the hash path
    /// tracks matched preserved build rows and emits the appropriate matched
    /// or unmatched rows after probe.
    pub(crate) fn set_hash_build_is_left(&mut self, build_is_left: bool) {
        if matches!(
            self.kind,
            JoinKind::Inner
                | JoinKind::Left
                | JoinKind::Right
                | JoinKind::Semi
                | JoinKind::AntiSemi
        ) {
            self.hash_build_is_left = Some(build_is_left);
        }
    }

    /// How many times the `ON` clause has been evaluated so far.
    #[must_use]
    pub fn condition_evals(&self) -> u64 {
        self.condition_evals.get()
    }

    /// How many bounded exact-integer probe windows used multiple workers.
    ///
    /// Wall-clock assertions are machine-dependent; the focused performance
    /// regression instead pins the Go-shaped worker path itself.
    #[cfg(test)]
    fn parallel_probe_windows(&self) -> usize {
        self.hash
            .as_ref()
            .map_or(0, |hash| hash.parallel_probe_windows)
    }

    #[cfg(test)]
    fn parallel_exact_int_enabled(&self) -> bool {
        self.hash
            .as_ref()
            .is_some_and(|hash| hash.parallel_exact_int_enabled)
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

    /// The logical outer side: the side whose unmatched rows survive. For an
    /// inner/semi join this is the streamed or index-driving side.
    fn outer_is_left(&self) -> bool {
        match self.kind {
            JoinKind::Left => return true,
            JoinKind::Right => return false,
            // Semi joins always preserve and return the logical left rows,
            // including when HashJoin v2 builds that side and probes right.
            JoinKind::Semi | JoinKind::AntiSemi => return true,
            JoinKind::Inner => {}
        }
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

    fn hash_build_is_left(&self) -> bool {
        self.hash_build_is_left
            .unwrap_or(self.kind == JoinKind::Right)
    }

    fn hash_builds_preserved_side(&self) -> bool {
        matches!(
            (self.kind, self.hash_build_is_left()),
            (JoinKind::Left | JoinKind::Semi | JoinKind::AntiSemi, true) | (JoinKind::Right, false)
        )
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
        let conditions = if self.keys.is_empty() {
            &self.conditions
        } else {
            &self.residual_conditions
        };
        if conditions.is_empty() {
            return Ok(true);
        }
        self.condition_evals.set(self.condition_evals.get() + 1);
        let mut chunk = Chunk::new_with_capacity(&self.condition_types, 1);
        for (i, value) in joined.iter().enumerate() {
            chunk.append_datum(i, value);
        }
        let row = chunk.get_row(0);
        for condition in conditions {
            let value = condition.eval(&self.ctx, row)?;
            if !truthy(&value)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn matches_chunk_rows(
        ctx: &C,
        conditions: &[Expression],
        condition_evals: &Cell<u64>,
        scratch: &mut Chunk,
        left: Row<'_>,
        right: Row<'_>,
    ) -> Result<bool, ExecError> {
        if conditions.is_empty() {
            return Ok(true);
        }
        condition_evals.set(condition_evals.get() + 1);
        scratch.reset();
        scratch.append_partial_row(0, left);
        scratch.append_partial_row(left.len(), right);
        let row = scratch.get_row(0);
        for condition in conditions {
            if !truthy(&condition.eval(ctx, row)?)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Evaluates the one residual shape admitted by
    /// [`residual_decimal_mul_lt`]. The column access remains typed and the
    /// decimal arithmetic uses the same exact value layer as the expression
    /// evaluator, so this is an allocation reduction rather than a numeric
    /// approximation.
    fn matches_decimal_mul_lt(
        condition_evals: &Cell<u64>,
        fast: &DecimalMulLtFastPath,
        left: Row<'_>,
        left_types: &[FieldType],
        right: Row<'_>,
        right_types: &[FieldType],
        cached_product: Option<Option<&MyDecimal>>,
    ) -> Result<bool, ExecError> {
        condition_evals.set(condition_evals.get().saturating_add(1));
        if let Some(product) = cached_product {
            let Some(product) = product else {
                return Ok(false);
            };
            let (row, types, column) = if fast.left_column < left_types.len() {
                (left, left_types, fast.left_column)
            } else {
                (right, right_types, fast.left_column - left_types.len())
            };
            let Some(field_type) = types.get(column) else {
                return Err(ExecError::unsupported(
                    "fast residual predicate left column is outside the join row",
                ));
            };
            if row.is_null(column) {
                return Ok(false);
            }
            if field_type.eval_type() != EvalType::Decimal {
                return Err(ExecError::unsupported(
                    "fast residual predicate left column is not DECIMAL",
                ));
            }
            return Ok(row.get_my_decimal(column).compare(product) == Ordering::Less);
        }
        let datum_at = |index: usize| {
            if index < left_types.len() {
                left_types
                    .get(index)
                    .map(|field_type| left.get_datum(index, field_type))
            } else {
                let right_index = index - left_types.len();
                right_types
                    .get(right_index)
                    .map(|field_type| right.get_datum(right_index, field_type))
            }
        };
        let Some(left_value) = datum_at(fast.left_column) else {
            return Err(ExecError::unsupported(
                "fast residual predicate left column is outside the join row",
            ));
        };
        let Some(right_value) = datum_at(fast.right_column) else {
            return Err(ExecError::unsupported(
                "fast residual predicate right column is outside the join row",
            ));
        };
        let (Datum::Decimal(left_value), Datum::Decimal(right_value)) = (left_value, right_value)
        else {
            // DECIMAL comparisons involving NULL are not TRUE. Any other
            // runtime type would contradict the statically proven shape and
            // must fail closed instead of changing SQL comparison rules.
            return Ok(false);
        };
        let comparison = decimal_mul_lt_mysql(&left_value, &fast.factor, &right_value);
        if comparison == Err(tidb_datatype::DecimalCodecWarning::Overflow) {
            return Err(ExecError::Eval(tidb_expr::EvalError::DecimalOverflow));
        }
        Ok(comparison.expect("decimal multiplication only reports overflow"))
    }

    fn decimal_mul_product(
        fast: &DecimalMulLtFastPath,
        build_row: Row<'_>,
        build_types: &[FieldType],
        column: usize,
    ) -> Result<Option<MyDecimal>, ExecError> {
        let Some(field_type) = build_types.get(column) else {
            return Err(ExecError::unsupported(
                "fast residual predicate right column is outside the build row",
            ));
        };
        if build_row.is_null(column) {
            return Ok(None);
        }
        if field_type.eval_type() != EvalType::Decimal {
            return Err(ExecError::unsupported(
                "fast residual predicate right column is not DECIMAL",
            ));
        }
        let right = Decimal::from_my_decimal(&build_row.get_my_decimal(column));
        let (product, warning) = fast.factor.mul_mysql(&right);
        if warning == Some(tidb_datatype::DecimalCodecWarning::Overflow) {
            return Err(ExecError::Eval(tidb_expr::EvalError::DecimalOverflow));
        }
        product.to_my_decimal().map(Some).map_err(|_| {
            ExecError::unsupported("fast residual DECIMAL product does not fit a chunk cell")
        })
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

    fn append(&self, req: &mut Chunk, joined: &[Datum]) {
        for (c, value) in joined.iter().enumerate() {
            req.append_datum(c, value);
        }
    }

    /// Appends a matched pair without allocating the concatenated row. The
    /// index-lookup path already proved the equality keys while building its
    /// lookup map, so allocating `outer ++ inner` for every result row would
    /// only duplicate the datums before copying them into the output chunk.
    fn append_joined_parts(&self, req: &mut Chunk, outer_row: &[Datum], inner_row: &[Datum]) {
        if self.outer_is_left() {
            for (column, value) in outer_row.iter().chain(inner_row).enumerate() {
                req.append_datum(column, value);
            }
        } else {
            for (column, value) in inner_row.iter().chain(outer_row).enumerate() {
                req.append_datum(column, value);
            }
        }
    }

    /// Appends a matched pair while the lookup side is still in its source
    /// chunk. This is the steady-state index-join path: `append_partial_row`
    /// copies the column cells directly instead of decoding the inner row to
    /// a temporary `Vec<Datum>` first.
    fn append_joined_chunk_row(&self, req: &mut Chunk, outer_row: &[Datum], inner_row: Row<'_>) {
        Self::append_joined_chunk_row_order(req, self.outer_is_left(), outer_row, inner_row);
    }

    fn append_joined_chunk_row_order(
        req: &mut Chunk,
        outer_is_left: bool,
        outer_row: &[Datum],
        inner_row: Row<'_>,
    ) {
        if outer_is_left {
            for (column, value) in outer_row.iter().enumerate() {
                req.append_datum(column, value);
            }
            req.append_partial_row(outer_row.len(), inner_row);
        } else {
            req.append_partial_row(0, inner_row);
            for (column, value) in outer_row.iter().enumerate() {
                req.append_datum(inner_row.len() + column, value);
            }
        }
    }

    /// Appends two chunk-backed hash-join rows in logical left-then-right
    /// order. Unlike the index-join helper above, neither side needs a
    /// temporary `Vec<Datum>`.
    fn append_joined_chunk_rows_order(
        req: &mut Chunk,
        probe_is_left: bool,
        probe_row: Row<'_>,
        build_row: Row<'_>,
    ) {
        if probe_is_left {
            req.append_partial_row(0, probe_row);
            req.append_partial_row(probe_row.len(), build_row);
        } else {
            req.append_partial_row(0, build_row);
            req.append_partial_row(build_row.len(), probe_row);
        }
    }

    fn append_joined_outer_chunk_row(
        req: &mut Chunk,
        outer_is_left: bool,
        outer_row: Row<'_>,
        inner_row: &[Datum],
    ) {
        if outer_is_left {
            req.append_partial_row(0, outer_row);
            for (column, value) in inner_row.iter().enumerate() {
                req.append_datum(outer_row.len() + column, value);
            }
        } else {
            for (column, value) in inner_row.iter().enumerate() {
                req.append_datum(column, value);
            }
            req.append_partial_row(inner_row.len(), outer_row);
        }
    }

    /// Emits a preserved probe row that found no match, without decoding the
    /// row out of its source chunk.
    fn append_unmatched_probe_chunk_row(
        req: &mut Chunk,
        probe_is_left: bool,
        probe_row: Row<'_>,
        build_width: usize,
    ) {
        if probe_is_left {
            req.append_partial_row(0, probe_row);
            for column in probe_row.len()..probe_row.len() + build_width {
                req.append_null(column);
            }
        } else {
            for column in 0..build_width {
                req.append_null(column);
            }
            req.append_partial_row(build_width, probe_row);
        }
    }

    /// Chunk-backed counterpart of [`Self::emit_outer_row`]. Residual
    /// predicates still materialize only the candidates that need evaluation;
    /// a pure equality lookup stays zero-copy through output assembly.
    fn emit_outer_chunk_rows<'a, I>(
        &self,
        req: &mut Chunk,
        outer_row: &[Datum],
        candidates: I,
        inner_types: &[FieldType],
    ) -> Result<(), ExecError>
    where
        I: Iterator<Item = Row<'a>>,
    {
        let evaluate_residual = self.keys.is_empty() || !self.residual_conditions.is_empty();
        let mut matched = false;
        for inner_row in candidates {
            if evaluate_residual {
                let inner_values = inner_row.get_datum_row(inner_types);
                let joined = self.join_rows(outer_row, &inner_values);
                if !self.matches(&joined)? {
                    continue;
                }
            }
            matched = true;
            match self.kind {
                JoinKind::Inner | JoinKind::Left | JoinKind::Right => {
                    self.append_joined_chunk_row(req, outer_row, inner_row);
                }
                JoinKind::Semi => {
                    self.append(req, outer_row);
                    break;
                }
                JoinKind::AntiSemi => break,
            }
        }
        if !matched {
            match self.kind {
                JoinKind::Left | JoinKind::Right => {
                    self.append(req, &self.padded_row(outer_row));
                }
                JoinKind::AntiSemi => self.append(req, outer_row),
                JoinKind::Inner | JoinKind::Semi => {}
            }
        }
        Ok(())
    }

    /// Emits every output row one outer row produces, given the inner rows
    /// it may match. Shared by both paths so the outer-join padding rule and
    /// the output column order have exactly one implementation.
    fn emit_outer_row<'a, I>(
        &self,
        req: &mut Chunk,
        outer_row: &[Datum],
        candidates: I,
    ) -> Result<(), ExecError>
    where
        I: Iterator<Item = &'a [Datum]>,
    {
        let mut matched = false;
        for inner_row in candidates {
            // Equal-key lookup/hash buckets have already established the key
            // match. Only build a joined row when a residual condition still
            // needs evaluation.
            let matches = if self.keys.is_empty() || !self.residual_conditions.is_empty() {
                let joined = self.join_rows(outer_row, inner_row);
                self.matches(&joined)?
            } else {
                true
            };
            if !matches {
                continue;
            }
            matched = true;
            match self.kind {
                JoinKind::Inner | JoinKind::Left | JoinKind::Right => {
                    self.append_joined_parts(req, outer_row, inner_row);
                }
                JoinKind::Semi => {
                    self.append(req, outer_row);
                    break;
                }
                JoinKind::AntiSemi => break,
            }
        }
        if !matched {
            match self.kind {
                JoinKind::Left | JoinKind::Right => {
                    self.append(req, &self.padded_row(outer_row));
                }
                JoinKind::AntiSemi => self.append(req, outer_row),
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
            JoinKind::Inner
                | JoinKind::Left
                | JoinKind::Right
                | JoinKind::Semi
                | JoinKind::AntiSemi
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
            let inner_types = self
                .index_lookup
                .as_ref()
                .expect("this path runs only with a plan")
                .source
                .ret_field_types()
                .to_vec();
            self.index_state = Some(IndexLookupState {
                outer: Vec::new(),
                cursor: 0,
                inner: List::new(
                    &inner_types,
                    self.meta.init_cap(),
                    self.meta.max_chunk_size(),
                ),
                inner_bytes: 0,
                matched: FastBytesMap::default(),
                outer_chunk,
                outer_row: 0,
                outer_done: false,
                // Go's `startWorkers(ctx, req.RequiredRows())`: the first
                // batch is what the caller asked for, capped by the maximum.
                batch_size: self.meta.max_chunk_size().min(INDEX_JOIN_BATCH_SIZE),
                pending: std::collections::VecDeque::new(),
                prefetch_disabled: false,
            });
        }
        loop {
            let state = self.index_state.as_ref().expect("just installed above");
            if state.cursor < state.outer.len() {
                return Ok(true);
            }
            if state.pending.is_empty()
                && state.outer_done
                && state.outer_row >= state.outer_chunk.num_rows()
            {
                return Ok(false);
            }
            self.load_index_batch()?;
            let state = self.index_state.as_ref().expect("still installed");
            if state.outer.is_empty() && state.outer_done {
                return Ok(false);
            }
        }
    }

    /// Pulls the next ordered outer task after starting a bounded number of
    /// later common-handle tasks. Go uses one outer worker and N inner workers;
    /// here opening N DistSQL-backed cursors provides the same request overlap
    /// while result materialization remains on the executor thread.
    fn load_index_batch(&mut self) -> Result<(), ExecError> {
        let outer_is_left = self.outer_is_left();
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
        let plan = index_lookup
            .as_mut()
            .expect("this path runs only with a plan");
        let state = index_state.as_mut().expect("fill_index_batch installed it");

        // Release the task just drained. Pending outer rows were charged when
        // their tasks were built, not when they became current.
        let retained =
            state.outer.iter().map(|row| row_bytes(row)).sum::<i64>() + state.inner_bytes;
        tracker.consume(-retained);
        state.outer.clear();
        state.inner.clear();
        state.inner_bytes = 0;
        state.matched.clear();
        state.cursor = 0;

        if state.pending.is_empty() {
            Self::fill_index_task_queue(
                outer_child,
                keys,
                plan,
                state,
                outer_is_left,
                tracker,
                memory,
                INDEX_LOOKUP_JOIN_CONCURRENCY,
            )?;
        }
        let Some(task) = state.pending.pop_front() else {
            return Ok(());
        };
        debug_assert_eq!(
            task.outer_bytes,
            task.outer.iter().map(|row| row_bytes(row)).sum::<i64>()
        );
        state.outer = task.outer;

        // Keep N-1 later requests live while this task is materialized and
        // drained, matching Go's N bounded inner-worker slots.
        Self::fill_index_task_queue(
            outer_child,
            keys,
            plan,
            state,
            outer_is_left,
            tracker,
            memory,
            INDEX_LOOKUP_JOIN_CONCURRENCY.saturating_sub(1),
        )?;

        let aggregation = plan.aggregation.clone();
        let aggregation_stream_ordered = plan.aggregation_stream_ordered;
        let inner_not_null = plan.inner_not_null.clone();
        match task.source {
            PendingIndexLookupSource::Prefetched(mut source) => Self::materialize_index_inner(
                &mut source,
                state,
                keys,
                outer_is_left,
                aggregation.as_ref(),
                aggregation_stream_ordered,
                &inner_not_null,
                tracker,
                memory,
            ),
            PendingIndexLookupSource::Synchronous(probes) => {
                plan.source.set_probes(probes)?;
                Self::materialize_index_inner(
                    &mut plan.source,
                    state,
                    keys,
                    outer_is_left,
                    aggregation.as_ref(),
                    aggregation_stream_ordered,
                    &inner_not_null,
                    tracker,
                    memory,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fill_index_task_queue(
        outer_child: &mut dyn Executor,
        keys: &[EquiKey],
        plan: &IndexLookupPlan,
        state: &mut IndexLookupState,
        outer_is_left: bool,
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
        target: usize,
    ) -> Result<(), ExecError> {
        let outer_types = outer_child.ret_field_types().to_vec();
        while state.pending.len() < target {
            let (outer, outer_bytes) = Self::read_index_outer_task(
                outer_child,
                &outer_types,
                &plan.outer_not_null,
                state,
            )?;
            if outer.is_empty() {
                if state.outer_done {
                    break;
                }
                continue;
            }
            let probes = Self::index_task_probes(keys, plan, &outer, outer_is_left)?;
            let source = if state.prefetch_disabled {
                PendingIndexLookupSource::Synchronous(probes)
            } else if let Some(source) =
                plan.source.fork_prefetched_common_handle(probes.clone())?
            {
                PendingIndexLookupSource::Prefetched(source)
            } else {
                state.prefetch_disabled = true;
                PendingIndexLookupSource::Synchronous(probes)
            };
            let synchronous = matches!(source, PendingIndexLookupSource::Synchronous(_));
            tracker.consume(outer_bytes);
            state.pending.push_back(PendingIndexLookupTask {
                outer,
                outer_bytes,
                source,
            });
            memory.check()?;
            if synchronous {
                break;
            }
        }
        Ok(())
    }

    fn read_index_outer_task(
        outer_child: &mut dyn Executor,
        outer_types: &[FieldType],
        outer_not_null: &[usize],
        state: &mut IndexLookupState,
    ) -> Result<(Vec<Vec<Datum>>, i64), ExecError> {
        let mut outer = Vec::with_capacity(state.batch_size);
        let mut bytes = 0i64;
        while outer.len() < state.batch_size {
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
            let row = datum_row(&state.outer_chunk, state.outer_row, outer_types);
            state.outer_row += 1;
            if !row_non_null_at(&row, outer_not_null)? {
                continue;
            }
            bytes += row_bytes(&row);
            outer.push(row);
        }
        state.batch_size = state
            .batch_size
            .saturating_mul(2)
            .min(INDEX_JOIN_BATCH_SIZE);
        Ok((outer, bytes))
    }

    fn index_task_probes(
        keys: &[EquiKey],
        plan: &IndexLookupPlan,
        outer: &[Vec<Datum>],
        outer_is_left: bool,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        let outer_offset = |key: &EquiKey| if outer_is_left { key.left } else { key.right };
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
        let mut probes_by_key = std::collections::BTreeMap::new();
        for row in outer {
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
                probes_by_key.entry(encoded).or_insert(probe);
            }
        }
        Ok(probes_by_key.into_values().collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_index_inner(
        source: &mut IndexLookupSource,
        state: &mut IndexLookupState,
        keys: &[EquiKey],
        outer_is_left: bool,
        aggregation: Option<&IndexLookupAggregation>,
        aggregation_stream_ordered: bool,
        inner_not_null: &[usize],
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
    ) -> Result<(), ExecError> {
        let inner_offset = |key: &EquiKey| if outer_is_left { key.right } else { key.left };
        let inner_types = source.ret_field_types().to_vec();
        let mut chunk = source.new_chunk();
        loop {
            source.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            let bytes = chunk.memory_usage();
            state.inner_bytes += bytes;
            state
                .inner
                .add(std::mem::replace(&mut chunk, source.new_chunk()));
            tracker.consume(bytes);
            memory.check()?;
        }
        // Once the retained aggregation has run, the inner rows are its own
        // OUTPUT layout -- one column per aggregate carrier -- and no longer
        // the physical lookup width. Everything after this point (the
        // non-NULL filter, the join-key extraction, and the emit path in
        // `drain_index_batch`) must read them with those types.
        let mut materialized_types = inner_types.clone();
        if let Some(aggregation) = aggregation {
            let rows = list_datum_rows(&state.inner, &inner_types);
            let raw_bytes = state.inner_bytes;
            let aggregated = aggregation.apply(rows, aggregation_stream_ordered)?;
            materialized_types = aggregation.output_types(&inner_types);
            let aggregated_bytes = aggregated.iter().map(|row| row_bytes(row)).sum::<i64>();
            replace_list_with_rows(&mut state.inner, &materialized_types, aggregated);
            state.inner_bytes = aggregated_bytes;
            tracker.consume(aggregated_bytes - raw_bytes);
            memory.check()?;
        }
        if !inner_not_null.is_empty() {
            let before = state.inner_bytes;
            let mut retained = Vec::with_capacity(state.inner.len());
            for row in list_datum_rows(&state.inner, &materialized_types) {
                if row_non_null_at(&row, inner_not_null)? {
                    retained.push(row);
                }
            }
            let after = retained.iter().map(|row| row_bytes(row)).sum::<i64>();
            tracker.consume(after - before);
            replace_list_with_rows(&mut state.inner, &materialized_types, retained);
            state.inner_bytes = after;
            memory.check()?;
        }

        for chk_idx in 0..state.inner.num_chunks() {
            let num_rows = state.inner.num_rows_of_chunk(chk_idx);
            for row_idx in 0..num_rows {
                let ptr = RowPtr::new(chk_idx as u32, row_idx as u32);
                let row = state.inner.get_row(ptr);
                let key = row_key_by(keys, |key| {
                    let offset = inner_offset(key);
                    row.get_datum(offset, &materialized_types[offset])
                })
                .map_err(|_: KeyError| {
                    ExecError::unsupported("a join key column has no comparable encoding")
                })?;
                if let Some(key) = key {
                    state.matched.entry(key).or_default().push(ptr);
                }
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
            let outer_row = &state.outer[state.cursor];
            let key = row_key(&keys, outer_row, outer_offset).map_err(|_: KeyError| {
                ExecError::unsupported("a join key column has no comparable encoding")
            })?;
            if let Some(positions) = key.and_then(|key| state.matched.get(&key)) {
                // The stored rows are the retained aggregation's OUTPUT layout
                // when one ran (see `materialize_index_inner`), so the emit
                // path must convert them with those types, not the physical
                // lookup width.
                let plan = self
                    .index_lookup
                    .as_ref()
                    .expect("this path runs only with a plan");
                let mut inner_types = plan.source.ret_field_types().to_vec();
                if let Some(aggregation) = &plan.aggregation {
                    inner_types = aggregation.output_types(&inner_types);
                }
                self.emit_outer_chunk_rows(
                    req,
                    outer_row,
                    positions.iter().map(|ptr| state.inner.get_row(*ptr)),
                    &inner_types,
                )?;
            } else {
                self.emit_outer_row(req, outer_row, std::iter::empty())?;
            }
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

    #[cfg(test)]
    pub(super) fn merge_inner_container_chunks(&self) -> usize {
        self.merge_state
            .as_ref()
            .and_then(|state| state.inner_group.as_ref())
            .map_or(0, |group| group.rows.num_chunks())
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
        side.group_len = 0;
        side.group_start = 0;
        side.group_end = 0;
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

        let first = side.row;
        let first_row = side.chunk.get_row(first);
        side.row += 1;
        while side.row < side.chunk.num_rows() {
            let row = side.chunk.get_row(side.row);
            if merge_rows_cmp(first_row, row, key_offsets, types, false)? != Ordering::Equal {
                break;
            }
            side.row += 1;
        }
        side.group_start = first;
        side.group_end = side.row;
        side.group_len = side.row - first;
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
                let outer_types = if outer.side_left {
                    self.left.ret_field_types().to_vec()
                } else {
                    self.right.ret_field_types().to_vec()
                };
                while *outer_index < outer.end && !req.is_full() {
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
                        let outer_row = {
                            let state = self.merge_state.as_ref().expect("merge state exists");
                            if outer.side_left {
                                state.left.chunk.get_row(*outer_index)
                            } else {
                                state.right.chunk.get_row(*outer_index)
                            }
                        };
                        let keys_match = match equi_keys_equal_row(
                            &self.keys,
                            &inner_row,
                            !outer.side_left,
                            outer_row,
                            &outer_types,
                        ) {
                            Ok(value) => value,
                            Err(current) => {
                                error = Some(key_error(current));
                                break;
                            }
                        };
                        let accepted = if keys_match {
                            if self.residual_conditions.is_empty() {
                                true
                            } else {
                                let outer_values = outer_row.get_datum_row(&outer_types);
                                let joined = if outer.side_left {
                                    self.join_rows(&outer_values, &inner_row)
                                } else {
                                    self.join_rows(&outer_values, &inner_row)
                                };
                                match self.matches(&joined) {
                                    Ok(value) => value,
                                    Err(current) => {
                                        error = Some(current);
                                        break;
                                    }
                                }
                            }
                        } else {
                            false
                        };
                        match accepted {
                            true => {
                                *matched_current_outer = true;
                                if self.residual_conditions.is_empty() {
                                    Self::append_joined_outer_chunk_row(
                                        req,
                                        outer.side_left,
                                        outer_row,
                                        &inner_row,
                                    );
                                } else {
                                    let outer_values = outer_row.get_datum_row(&outer_types);
                                    let joined = self.join_rows(&outer_values, &inner_row);
                                    self.append(req, &joined);
                                }
                            }
                            false => {}
                        }
                        continue;
                    }

                    if !*matched_current_outer && self.kind != JoinKind::Inner {
                        let state = self.merge_state.as_ref().expect("merge state exists");
                        let outer_row = if outer.side_left {
                            state.left.chunk.get_row(*outer_index)
                        } else {
                            state.right.chunk.get_row(*outer_index)
                        };
                        Self::append_unmatched_probe_chunk_row(
                            req,
                            outer.side_left,
                            outer_row,
                            if outer.side_left {
                                self.right.ret_field_types().len()
                            } else {
                                self.left.ret_field_types().len()
                            },
                        );
                    }
                    *outer_index += 1;
                    *matched_current_outer = false;
                    if *outer_index < outer.end {
                        *inner_ptr = inner.first_ptr();
                    }
                }
            }
            MergePendingOutput::Unmatched {
                outer,
                retained_bytes: _,
                outer_index,
            } => {
                while *outer_index < outer.end && !req.is_full() {
                    let state = self.merge_state.as_ref().expect("merge state exists");
                    let outer_row = if outer.side_left {
                        state.left.chunk.get_row(*outer_index)
                    } else {
                        state.right.chunk.get_row(*outer_index)
                    };
                    Self::append_unmatched_probe_chunk_row(
                        req,
                        outer.side_left,
                        outer_row,
                        if outer.side_left {
                            self.right.ret_field_types().len()
                        } else {
                            self.left.ret_field_types().len()
                        },
                    );
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
            } => *outer_index == outer.end,
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
            let inner_scratch_bytes =
                inner_group.staging.memory_usage() + inner_group.read_back.memory_usage();
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
            self.tracker.consume(inner_scratch_bytes + input_bytes);
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
            } else if outer_is_left {
                merge_row_key_cmp(
                    state.left.chunk.get_row(state.left.group_start),
                    &left_types,
                    &left_keys,
                    &state.right.key,
                    desc,
                )?
            } else {
                merge_key_cmp_row(
                    &state.left.key,
                    state.right.chunk.get_row(state.right.group_start),
                    &right_keys,
                    &right_types,
                    desc,
                )?
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
                    {
                        let state = self.merge_state.as_mut().expect("state exists");
                        let outer = if outer_is_left {
                            state.left.group_len = 0;
                            MergeOuterRange {
                                side_left: true,
                                start: state.left.group_start,
                                end: state.left.group_end,
                            }
                        } else {
                            state.right.group_len = 0;
                            MergeOuterRange {
                                side_left: false,
                                start: state.right.group_start,
                                end: state.right.group_end,
                            }
                        };
                        let first = state
                            .inner_group
                            .as_ref()
                            .expect("inner group installed")
                            .first_ptr();
                        state.pending = Some(MergePendingOutput::Matched {
                            outer,
                            retained_bytes: 0,
                            outer_index: outer.start,
                            inner_ptr: first,
                            matched_current_outer: false,
                        });
                    }
                    self.drain_merge_pending(req)?;
                }
                // The left group is behind, or the right side is spent.
                Ordering::Less => {
                    let (group, is_outer) = {
                        let state = self.merge_state.as_mut().expect("state exists");
                        state.left.group_len = 0;
                        (
                            MergeOuterRange {
                                side_left: true,
                                start: state.left.group_start,
                                end: state.left.group_end,
                            },
                            outer_is_left,
                        )
                    };
                    if is_outer && self.kind != JoinKind::Inner {
                        self.merge_state.as_mut().expect("state exists").pending =
                            Some(MergePendingOutput::Unmatched {
                                outer: group,
                                retained_bytes: 0,
                                outer_index: group.start,
                            });
                        self.drain_merge_pending(req)?;
                    }
                }
                Ordering::Greater => {
                    let (group, is_outer) = {
                        let state = self.merge_state.as_mut().expect("state exists");
                        state.right.group_len = 0;
                        (
                            MergeOuterRange {
                                side_left: false,
                                start: state.right.group_start,
                                end: state.right.group_end,
                            },
                            !outer_is_left,
                        )
                    };
                    if is_outer && self.kind != JoinKind::Inner {
                        self.merge_state.as_mut().expect("state exists").pending =
                            Some(MergePendingOutput::Unmatched {
                                outer: group,
                                retained_bytes: 0,
                                outer_index: group.start,
                            });
                        self.drain_merge_pending(req)?;
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
            self.emit_outer_row(req, outer_row, inner.iter().map(Vec::as_slice))?;
            let produced = i64::try_from(req.num_rows() - before_rows).unwrap_or(i64::MAX);
            let grew = (req.memory_usage() - before_bytes).max(0);
            self.tracker
                .consume(grew + tidb_chunk::row::ROW_SIZE * produced);
            self.memory.check()?;
        }
        self.emitted = true;
        Ok(())
    }

    /// The hash path: build once on the costed side, then stream the other.
    ///
    /// One call consumes probe chunks until it has produced at least one
    /// output row, because an empty `req` is the caller's EOF signal and a
    /// probe chunk whose rows all miss (an inner join) produces none.
    fn next_hashed(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.build_table()?;
        if self.can_parallelize_exact_int_probe() {
            self.prepare_parallel_decimal_products()?;
            return self.next_parallel_exact_int_hashed(req);
        }
        loop {
            if self.hash.as_ref().is_some_and(|hash| hash.probe_done) {
                if self.hash_builds_preserved_side() {
                    self.drain_preserved_build_rows(req)?;
                }
                return Ok(());
            }
            self.fill_probe_chunk()?;
            self.drain_probe_chunk(req)?;
            if req.num_rows() > 0 {
                return Ok(());
            }
        }
    }

    /// Whether this join can use the bounded worker path without moving the
    /// executor tree or expression context across threads.
    ///
    /// The first slice is deliberately narrow: one ordinary integer equality
    /// key, no residual predicate, and at most one build match per key. That
    /// is enough for primary/unique-key dimension joins while proving each
    /// worker can retain no more than one output chunk.
    fn can_parallelize_exact_int_probe(&self) -> bool {
        let [key] = self.keys.as_slice() else {
            return false;
        };
        let residual_supported = self.residual_conditions.is_empty()
            || (self.kind == JoinKind::Inner
                && self.residual_decimal_mul_lt.is_some()
                && self.parallel_decimal_product_build_column().is_some());
        matches!(
            self.kind,
            JoinKind::Inner
                | JoinKind::Left
                | JoinKind::Right
                | JoinKind::Semi
                | JoinKind::AntiSemi
        ) && residual_supported
            && key.class == KeyClass::Int
            && !key.null_safe
            && self
                .hash
                .as_ref()
                .is_some_and(|hash| hash.parallel_exact_int_enabled && hash.table.has_exact_int())
    }

    /// Build-side column holding the right operand of q17's cached decimal
    /// product. Returning `None` keeps a residual whose product lives on the
    /// probe side out of the bounded worker path.
    fn parallel_decimal_product_build_column(&self) -> Option<usize> {
        let fast = self.residual_decimal_mul_lt.as_ref()?;
        let left_width = self.left.ret_field_types().len();
        let product_is_left = fast.right_column < left_width;
        if product_is_left != self.hash_build_is_left() {
            return None;
        }
        Some(if product_is_left {
            fast.right_column
        } else {
            fast.right_column - left_width
        })
    }

    /// Computes q17's `0.2 * AVG(...)` once per unique build row before any
    /// worker borrows the table. The resulting map is immutable throughout
    /// all probe windows and therefore crosses no mutable session boundary.
    fn prepare_parallel_decimal_products(&mut self) -> Result<(), ExecError> {
        let Some(fast) = self.residual_decimal_mul_lt.clone() else {
            return Ok(());
        };
        let column = self
            .parallel_decimal_product_build_column()
            .expect("parallel decimal residual requires a build-side product");
        let hash = self.hash.as_mut().expect("hash table was built");
        if !hash.decimal_mul_products.is_empty() {
            return Ok(());
        }
        let mut ptr = hash.table.first_ptr();
        while let Some(current) = ptr {
            ptr = hash.table.next_ptr(current);
            let product = {
                let table = &hash.table;
                let build_types = &hash.build_types;
                let build_buf = &mut hash.build_buf;
                table
                    .with_row(current, build_buf, |build_row| {
                        Self::decimal_mul_product(&fast, build_row, build_types, column)
                    })
                    .map_err(|error| ExecError::SpillFailed(error.to_string()))??
            };
            hash.decimal_mul_products.insert(current, product);
        }
        Ok(())
    }

    /// Parallel counterpart of [`Self::next_hashed`] for the proven exact
    /// integer slice. The session thread remains the sole child fetcher and
    /// sole owner of matched-bitmap writes; workers borrow only the immutable
    /// build table and own their scratch/input/output chunks.
    fn next_parallel_exact_int_hashed(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        loop {
            if self.take_parallel_probe_output(req) {
                return Ok(());
            }
            if self.hash.as_ref().is_some_and(|hash| hash.probe_done) {
                if self.hash_builds_preserved_side() {
                    self.drain_preserved_build_rows(req)?;
                }
                return Ok(());
            }
            self.fill_parallel_exact_int_probe_window()?;
        }
    }

    /// Moves the next worker result into the caller chunk without copying its
    /// cells, then returns the caller's emptied column allocation to the
    /// worker pool -- Go's `req.SwapColumns(result.chk); result.src <- chk`.
    fn take_parallel_probe_output(&mut self, req: &mut Chunk) -> bool {
        let output = self
            .hash
            .as_mut()
            .and_then(|hash| hash.parallel_probe_pending.pop_front());
        let Some(mut output) = output else {
            return false;
        };
        req.swap_columns(&mut output);
        output.reset();
        // A parent may hand a zero-column scratch chunk to an executor whose
        // runtime join output is wider. It is valid as the receiving buffer,
        // but cannot be reused as a worker output.
        if output.num_cols() == req.num_cols() {
            self.hash
                .as_mut()
                .expect("parallel output requires hash state")
                .parallel_probe_output_reuse
                .push(output);
        }
        true
    }

    /// Fetches one bounded Go-shaped probe window and evaluates its chunks on
    /// up to five workers. Thread lifetimes end before this method mutates the
    /// build-side matched bitmap, so no shared mutable join state crosses the
    /// worker boundary.
    fn fill_parallel_exact_int_probe_window(&mut self) -> Result<(), ExecError> {
        debug_assert!(self.can_parallelize_exact_int_probe());
        debug_assert!(self
            .hash
            .as_ref()
            .is_some_and(|hash| hash.parallel_probe_pending.is_empty()));

        let probe_is_left = !self.hash_build_is_left();
        let probe_types = if probe_is_left {
            self.left.ret_field_types().to_vec()
        } else {
            self.right.ret_field_types().to_vec()
        };
        let build_types = self
            .hash
            .as_ref()
            .expect("parallel probe requires hash state")
            .build_types
            .clone();
        // A semi/anti join's output carries only the preserved LEFT columns;
        // every other family emits the joined left-then-right row.
        let output_types = if matches!(self.kind, JoinKind::Semi | JoinKind::AntiSemi) {
            self.left.ret_field_types().to_vec()
        } else if probe_is_left {
            probe_types
                .iter()
                .chain(&build_types)
                .cloned()
                .collect::<Vec<_>>()
        } else {
            build_types
                .iter()
                .chain(&probe_types)
                .cloned()
                .collect::<Vec<_>>()
        };
        let key = self.keys[0];
        let key_offset = if probe_is_left { key.left } else { key.right };
        let kind = self.kind;
        let builds_preserved = self.hash_builds_preserved_side();
        let decimal_mul_lt = self.residual_decimal_mul_lt.as_ref();

        // Keep a bounded lane of reusable chunks per default Go worker. The
        // child is intentionally fetched only here on the session thread:
        // Executor and StmtContext retain their single-threaded ownership
        // contract, while each scoped worker amortizes its startup over the
        // chunks in one lane.
        let window_chunks = HASH_JOIN_CONCURRENCY * PARALLEL_PROBE_CHUNKS_PER_WORKER;
        let mut inputs = Vec::with_capacity(window_chunks);
        for _ in 0..window_chunks {
            let reused = self
                .hash
                .as_mut()
                .expect("parallel probe requires hash state")
                .parallel_probe_input_reuse
                .pop();
            let mut input = reused.unwrap_or_else(|| {
                if probe_is_left {
                    self.left.new_chunk()
                } else {
                    self.right.new_chunk()
                }
            });
            let result = if probe_is_left {
                self.left.next(&mut input)
            } else {
                self.right.next(&mut input)
            };
            if let Err(error) = result {
                input.reset();
                self.hash
                    .as_mut()
                    .expect("parallel probe requires hash state")
                    .parallel_probe_input_reuse
                    .push(input);
                return Err(error);
            }
            if input.num_rows() == 0 {
                input.reset();
                let hash = self
                    .hash
                    .as_mut()
                    .expect("parallel probe requires hash state");
                hash.parallel_probe_input_reuse.push(input);
                hash.probe_done = true;
                break;
            }
            inputs.push(input);
        }
        if inputs.is_empty() {
            return Ok(());
        }

        let mut work = Vec::with_capacity(inputs.len());
        for input in inputs {
            let output = self
                .hash
                .as_mut()
                .expect("parallel probe requires hash state")
                .parallel_probe_output_reuse
                .pop()
                .filter(|output| output.num_cols() == output_types.len())
                .unwrap_or_else(|| {
                    Chunk::new(
                        &output_types,
                        self.meta.init_cap(),
                        self.meta.max_chunk_size(),
                    )
                });
            work.push((input, output));
        }
        let worker_count = work.len().min(HASH_JOIN_CONCURRENCY);

        let outcomes = {
            let hash = self
                .hash
                .as_ref()
                .expect("parallel probe requires hash state");
            let table = &hash.table;
            let build_types = hash.build_types.as_slice();
            let probe_types = probe_types.as_slice();
            let decimal_products = &hash.decimal_mul_products;
            if worker_count == 1 {
                let (input, output) = work.pop().expect("one worker item");
                vec![(
                    0,
                    Self::probe_unique_exact_int_chunk(
                        table,
                        build_types,
                        probe_types,
                        input,
                        output,
                        key_offset,
                        probe_is_left,
                        kind,
                        builds_preserved,
                        decimal_mul_lt,
                        decimal_products,
                    ),
                )]
            } else {
                let mut lanes = (0..worker_count)
                    .map(|_| Vec::with_capacity(PARALLEL_PROBE_CHUNKS_PER_WORKER))
                    .collect::<Vec<_>>();
                for (index, item) in work.into_iter().enumerate() {
                    lanes[index % worker_count].push((index, item));
                }
                std::thread::scope(|scope| {
                    let handles = lanes
                        .into_iter()
                        .map(|lane| {
                            scope.spawn(move || {
                                lane.into_iter()
                                    .map(|(index, (input, output))| {
                                        (
                                            index,
                                            Self::probe_unique_exact_int_chunk(
                                                table,
                                                build_types,
                                                probe_types,
                                                input,
                                                output,
                                                key_offset,
                                                probe_is_left,
                                                kind,
                                                builds_preserved,
                                                decimal_mul_lt,
                                                decimal_products,
                                            ),
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            })
                        })
                        .collect::<Vec<_>>();
                    let mut outcomes = Vec::with_capacity(PARALLEL_PROBE_CHUNKS_PER_WORKER);
                    for handle in handles {
                        let mut lane = handle.join().unwrap_or_else(|_| {
                            vec![(
                                0,
                                Err(ExecError::internal("hash join probe worker panicked")),
                            )]
                        });
                        outcomes.append(&mut lane);
                    }
                    outcomes
                })
            }
        };

        // Join every worker before observing an error. Only a completely
        // successful window may update the preserved-side match bitmap. Lane
        // assignment is round-robin, so restore source-chunk order before
        // handing results to the parent executor.
        let mut outcomes = outcomes;
        outcomes.sort_unstable_by_key(|(index, _)| *index);
        let results = outcomes
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Result<Vec<_>, _>>()?;
        let condition_evals = results.iter().fold(0u64, |total, result| {
            total.saturating_add(result.condition_evals)
        });
        self.condition_evals
            .set(self.condition_evals.get().saturating_add(condition_evals));
        let hash = self
            .hash
            .as_mut()
            .expect("parallel probe requires hash state");
        if worker_count > 1 {
            hash.parallel_probe_windows = hash.parallel_probe_windows.saturating_add(1);
        }
        for mut result in results {
            result.input.reset();
            hash.parallel_probe_input_reuse.push(result.input);
            for ptr in result.matched_build_rows {
                hash.table.mark_matched(ptr);
            }
            if result.output.num_rows() == 0 {
                result.output.reset();
                hash.parallel_probe_output_reuse.push(result.output);
            } else {
                hash.parallel_probe_pending.push_back(result.output);
            }
        }
        Ok(())
    }

    /// Evaluates one probe chunk against a unique exact-integer build table.
    /// All arguments are `Send`/`Sync` data; notably neither the executor tree
    /// nor the expression context is present in this worker contract.
    #[allow(clippy::too_many_arguments)]
    fn probe_unique_exact_int_chunk(
        table: &BuildTable,
        build_types: &[FieldType],
        probe_types: &[FieldType],
        input: Chunk,
        mut output: Chunk,
        key_offset: usize,
        probe_is_left: bool,
        kind: JoinKind,
        builds_preserved: bool,
        decimal_mul_lt: Option<&DecimalMulLtFastPath>,
        decimal_products: &HashMap<RowPtr, Option<MyDecimal>>,
    ) -> Result<ParallelProbeResult, ExecError> {
        output.reset();
        // A semi/anti join emits only the preserved LEFT columns; the other
        // two-column families emit the joined left-then-right row.
        let preserved_only = matches!(kind, JoinKind::Semi | JoinKind::AntiSemi);
        // A semi/anti join emits only the preserved LEFT columns, whichever
        // side was built.
        let required_columns = if preserved_only {
            if probe_is_left {
                probe_types.len()
            } else {
                build_types.len()
            }
        } else {
            probe_types.len() + build_types.len()
        };
        if output.num_cols() < required_columns {
            return Err(ExecError::internal(format!(
                "parallel hash join output has {} columns, needs {} (probe {}, build {})",
                output.num_cols(),
                required_columns,
                probe_types.len(),
                build_types.len()
            )));
        }
        if input.num_cols() < probe_types.len() {
            return Err(ExecError::internal(format!(
                "parallel hash join probe has {} columns, needs {}",
                input.num_cols(),
                probe_types.len()
            )));
        }
        let mut build_buf = Chunk::new_with_capacity(build_types, 1);
        let condition_evals = Cell::new(0u64);
        let mut matched_build_rows = if builds_preserved {
            Vec::with_capacity(input.num_rows())
        } else {
            Vec::new()
        };
        // Probe chunks produced by the coprocessor scan normally have no
        // selection vector. Keep the key column borrowed once in that shape;
        // the generic selected-chunk path below still uses the logical Row
        // accessor so selection semantics remain unchanged.
        // A HYBRID key column (Go `FieldType.Hybrid()`) is variable-length in
        // the chunk even though it compares as an integer, so the borrowed
        // fixed-width accessors below cannot read it. Those keys take the
        // generic row path, which goes through `Row::get_datum` exactly as
        // Go's `Column.EvalInt` does.
        let probe_key_values = (input.sel().is_none() && !probe_types[key_offset].is_hybrid())
            .then(|| input.column(key_offset));
        let exact_key_at = |row_index: usize| {
            if let Some(values) = probe_key_values.as_ref() {
                if values.is_null(row_index) {
                    None
                } else if probe_types[key_offset].is_unsigned() {
                    Some(i128::from(values.get_uint64(row_index)))
                } else {
                    Some(i128::from(values.get_int64(row_index)))
                }
            } else {
                let probe_row = input.get_row(row_index);
                exact_int_key_chunk(probe_row, key_offset, &probe_types[key_offset])
            }
        };

        // The common unique-key dimension join has one build candidate for
        // every probe row. Preflight that shape once, then retain the build
        // container's records read lock across the whole window instead of
        // reacquiring it for every joined row. The decimal residual form is
        // included here: q17 has one unique part row per probe key, and the
        // residual still runs for every pair while the source lock is held.
        if !builds_preserved && input.num_rows() > 0 {
            let mut batch_ptrs = Vec::with_capacity(input.num_rows());
            let mut all_matched = true;
            for probe_index in 0..input.num_rows() {
                let exact_key = exact_key_at(probe_index);
                let candidates = exact_key.map_or(&[][..], |key| table.probe_exact_int(key));
                if candidates.len() != 1 {
                    all_matched = false;
                    break;
                }
                batch_ptrs.push(candidates[0]);
            }
            if all_matched {
                // Semi/anti have no joined row to assemble: a preserved
                // build side only records the matches for the post-probe
                // scan, and a probe-side semi join emits each preserved row
                // once, in bulk, exactly because every row matched.
                if preserved_only {
                    if matches!(kind, JoinKind::Semi) && !builds_preserved {
                        let probe_compact = input.copy_construct_sel();
                        output.append_partial_range_from(
                            0,
                            &probe_compact,
                            0,
                            probe_compact.num_rows(),
                        );
                    }
                    if builds_preserved {
                        matched_build_rows.extend(batch_ptrs.iter().copied());
                    }
                    drop(probe_key_values);
                    return Ok(ParallelProbeResult {
                        input,
                        output,
                        matched_build_rows,
                        condition_evals: condition_evals.get(),
                    });
                }
                if let Some(fast) = decimal_mul_lt {
                    let mut probe_index = 0;
                    table
                        .with_rows(&batch_ptrs, &mut build_buf, |build_row| {
                            let current_probe_index = probe_index;
                            probe_index += 1;
                            // `get_row` maps through the selection vector, so
                            // q17's residual can borrow the worker-owned input
                            // directly. Deep-copying all probe columns here
                            // doubled the 6M-row scan traffic before emitting
                            // the small residual-selected result.
                            let probe_row = input.get_row(current_probe_index);
                            let ptr = batch_ptrs[current_probe_index];
                            let (left, left_types, right, right_types) = if probe_is_left {
                                (probe_row, probe_types, build_row, build_types)
                            } else {
                                (build_row, build_types, probe_row, probe_types)
                            };
                            if Self::matches_decimal_mul_lt(
                                &condition_evals,
                                fast,
                                left,
                                left_types,
                                right,
                                right_types,
                                decimal_products.get(&ptr).map(Option::as_ref),
                            )? {
                                Self::append_joined_chunk_rows_order(
                                    &mut output,
                                    probe_is_left,
                                    probe_row,
                                    build_row,
                                );
                            }
                            Ok::<(), ExecError>(())
                        })
                        .map_err(|error| ExecError::SpillFailed(error.to_string()))??;
                    drop(probe_key_values);
                    return Ok(ParallelProbeResult {
                        input,
                        output,
                        matched_build_rows,
                        condition_evals: condition_evals.get(),
                    });
                }
                // Bulk appends index physical row ranges and intentionally do
                // not consult a source selection vector. Compact only this
                // pure-equality arm; the residual arm above emits logical
                // rows individually and needs no copy.
                let probe = input.copy_construct_sel();
                let build_key_from_probe = build_types.len() == 1
                    && build_types[0].eval_type() == tidb_datatype::EvalType::Int
                    && probe_types[key_offset].eval_type() == tidb_datatype::EvalType::Int
                    && build_types[0].is_unsigned() == probe_types[key_offset].is_unsigned()
                    && output
                        .column(if probe_is_left { probe_types.len() } else { 0 })
                        .type_size()
                        == probe.column(key_offset).type_size();
                if probe_is_left {
                    output.append_partial_range_from(0, &probe, 0, probe.num_rows());
                }
                if build_key_from_probe {
                    output.append_column_range_from(
                        if probe_is_left { probe_types.len() } else { 0 },
                        &probe,
                        key_offset,
                        0,
                        probe.num_rows(),
                    );
                } else {
                    table
                        .with_rows(&batch_ptrs, &mut build_buf, |build_row| {
                            output.append_partial_row(
                                if probe_is_left { probe_types.len() } else { 0 },
                                build_row,
                            );
                            Ok::<(), ExecError>(())
                        })
                        .map_err(|error| ExecError::SpillFailed(error.to_string()))??;
                }
                // A selection-bearing probe chunk is compacted once. The
                // output then receives the probe columns in bulk and the
                // build suffix under one retained source lock, preserving
                // row order while avoiding one source cell copy for every
                // probe column on every joined row.
                if !probe_is_left {
                    output.append_partial_range_from(
                        build_types.len(),
                        &probe,
                        0,
                        probe.num_rows(),
                    );
                }
                drop(probe_key_values);
                return Ok(ParallelProbeResult {
                    input,
                    output,
                    matched_build_rows,
                    condition_evals: condition_evals.get(),
                });
            }
        }
        for probe_index in 0..input.num_rows() {
            let probe_row = input.get_row(probe_index);
            let exact_key = exact_key_at(probe_index);
            let candidates: &[RowPtr] = exact_key.map_or(&[], |key| table.probe_exact_int(key));
            debug_assert!(candidates.len() <= 1);
            // Semi/anti emit the preserved LEFT row once per match decision;
            // with the preserved side built they only collect matches for the
            // post-probe scan.
            if preserved_only {
                let matched = !candidates.is_empty();
                if matches!(kind, JoinKind::Semi) && matched && !builds_preserved {
                    output.append_partial_row(0, probe_row);
                }
                if builds_preserved {
                    if let Some(&ptr) = candidates.first() {
                        matched_build_rows.push(ptr);
                    }
                }
                if matches!(kind, JoinKind::AntiSemi) && !matched && !builds_preserved {
                    output.append_partial_row(0, probe_row);
                }
                continue;
            }
            let mut matched = false;
            for &ptr in candidates {
                let emitted = table
                    .with_row(ptr, &mut build_buf, |build_row| {
                        if let Some(fast) = decimal_mul_lt {
                            let (left, left_types, right, right_types) = if probe_is_left {
                                (probe_row, probe_types, build_row, build_types)
                            } else {
                                (build_row, build_types, probe_row, probe_types)
                            };
                            if !Self::matches_decimal_mul_lt(
                                &condition_evals,
                                fast,
                                left,
                                left_types,
                                right,
                                right_types,
                                decimal_products.get(&ptr).map(Option::as_ref),
                            )? {
                                return Ok::<bool, ExecError>(false);
                            }
                        }
                        Self::append_joined_chunk_rows_order(
                            &mut output,
                            probe_is_left,
                            probe_row,
                            build_row,
                        );
                        Ok::<bool, ExecError>(true)
                    })
                    .map_err(|error| ExecError::SpillFailed(error.to_string()))??;
                matched |= emitted;
                if emitted && builds_preserved {
                    matched_build_rows.push(ptr);
                }
            }
            if !matched && !builds_preserved && matches!(kind, JoinKind::Left | JoinKind::Right) {
                Self::append_unmatched_probe_chunk_row(
                    &mut output,
                    probe_is_left,
                    probe_row,
                    build_types.len(),
                );
            }
        }
        drop(probe_key_values);
        Ok(ParallelProbeResult {
            input,
            output,
            matched_build_rows,
            condition_evals: condition_evals.get(),
        })
    }

    /// Materializes and indexes the chosen build side, once per `open()`.
    ///
    /// Go `BuildWorkerV1.BuildHashTableForList`, in its order: hang the
    /// container's trackers off this operator's, register the spill action on
    /// the SESSION tracker when `tidb_enable_tmp_storage_on_oom` allows it,
    /// then feed the child's chunks in.
    fn build_table(&mut self) -> Result<(), ExecError> {
        if self.hash.is_some() {
            return Ok(());
        }
        let build_is_left = self.hash_build_is_left();
        let track_matches = self.hash_builds_preserved_side();
        let build_types: Vec<FieldType> = if build_is_left {
            self.left.ret_field_types().to_vec()
        } else {
            self.right.ret_field_types().to_vec()
        };
        let mut table = BuildTable::new(
            &build_types,
            self.meta.max_chunk_size(),
            self.memory.spill_storage(),
            track_matches,
            self.kind == JoinKind::Inner
                || (matches!(
                    self.kind,
                    JoinKind::Left | JoinKind::Right | JoinKind::Semi | JoinKind::AntiSemi
                ) && self.residual_conditions.is_empty()),
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
        let probe: &dyn Executor = if build_is_left {
            self.right.as_ref()
        } else {
            self.left.as_ref()
        };
        let probe_chunk = probe.new_chunk();
        // The container's live state is only readable while it is open --
        // `close` deletes the spill file and detaches the disk tracker -- so
        // what the build side DID is latched here, at the moment the build
        // finishes and nothing more can change it.
        self.build_spilled = table.already_spilled();
        self.spilled_bytes = self.disk_tracker.bytes_consumed();
        let parallel_exact_int_enabled = table.exact_int_is_unique();
        let build_buf = Chunk::new_with_capacity(&build_types, 1);
        let unmatched_build_scan = track_matches.then(|| table.first_ptr()).flatten();
        self.hash = Some(HashState {
            table,
            build_types,
            build_buf,
            probe_chunk,
            probe_row: 0,
            probe_done: false,
            decimal_mul_products: HashMap::new(),
            unmatched_build_scan,
            parallel_probe_pending: VecDeque::new(),
            parallel_probe_input_reuse: Vec::new(),
            parallel_probe_output_reuse: Vec::new(),
            parallel_exact_int_enabled,
            parallel_probe_windows: 0,
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
        let probe: &mut dyn Executor = if !self.hash_build_is_left() {
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
        let probe_is_left = !self.hash_build_is_left();
        let probe_types: Vec<FieldType> = if probe_is_left {
            self.left.ret_field_types().to_vec()
        } else {
            self.right.ret_field_types().to_vec()
        };
        // Keep pure-equality inputs chunk-backed from hash calculation through
        // output assembly. This is the common TPC-H path and avoids
        // materializing every wide probe row as `Vec<Datum>`. Semi and anti
        // joins qualify too: their match arms below are the same row-at-a-time
        // decisions Go's semiJoiner/antiSemiJoiner make, and routing them here
        // keeps `EXISTS`/`NOT EXISTS` off the per-row `Vec<Datum>` path.
        if matches!(
            self.kind,
            JoinKind::Inner
                | JoinKind::Left
                | JoinKind::Right
                | JoinKind::Semi
                | JoinKind::AntiSemi
        ) && self.residual_conditions.is_empty()
        {
            return self.drain_chunk_backed_probe(req, probe_is_left, &probe_types);
        }
        if self.kind == JoinKind::Inner {
            return self.drain_chunk_backed_residual_probe(req, probe_is_left, &probe_types);
        }
        let offset = |key: &EquiKey| if probe_is_left { key.left } else { key.right };
        loop {
            let Some(hash) = self.hash.as_mut() else {
                return Ok(());
            };
            if hash.probe_row >= hash.probe_chunk.num_rows() {
                return Ok(());
            }
            let probe_row = datum_row(&hash.probe_chunk, hash.probe_row, &probe_types);
            let key = row_hash(&self.keys, &probe_row, offset).map_err(key_error)?;
            // A probe row whose key holds a NULL matches nothing, so it never
            // touches the table -- and, on an outer join, pads immediately.
            //
            // Go `GetMatchedRowsAndPtrs`: walk the bucket's pointers in order
            // and dereference each one through the container, which is where
            // a spilled build side becomes a read from the spill file.
            let candidates: Vec<RowPtr> = match key {
                Some(key) => {
                    let ptrs = hash.table.probe(key).to_vec();
                    let mut rows = Vec::with_capacity(ptrs.len());
                    for ptr in ptrs {
                        let key_matches = {
                            let HashState {
                                table,
                                build_buf,
                                build_types,
                                ..
                            } = hash;
                            table
                                .with_row(ptr, build_buf, |build_row| {
                                    equi_keys_equal_row(
                                        &self.keys,
                                        &probe_row,
                                        probe_is_left,
                                        build_row,
                                        build_types,
                                    )
                                })
                                .map_err(|error| ExecError::SpillFailed(error.to_string()))?
                                .map_err(key_error)?
                        };
                        if !key_matches {
                            continue;
                        }
                        rows.push(ptr);
                    }
                    rows
                }
                None => Vec::new(),
            };
            if self.hash_builds_preserved_side() || !self.residual_conditions.is_empty() {
                if self.hash_builds_preserved_side() {
                    for ptr in candidates {
                        let build_row = {
                            let HashState {
                                table,
                                build_buf,
                                build_types,
                                ..
                            } = self.hash.as_mut().expect("hash state exists");
                            table
                                .row(ptr, build_buf, build_types)
                                .map_err(|error| ExecError::SpillFailed(error.to_string()))?
                        };
                        let joined = self.join_rows(&build_row, &probe_row);
                        if self.matches(&joined)? {
                            if matches!(self.kind, JoinKind::Left | JoinKind::Right) {
                                self.append(req, &joined);
                            }
                            let hash = self.hash.as_mut().expect("hash state exists in this arm");
                            hash.table.mark_matched(ptr);
                        }
                    }
                } else {
                    let mut matched = false;
                    for ptr in candidates {
                        let build_row = {
                            let HashState {
                                table,
                                build_buf,
                                build_types,
                                ..
                            } = self.hash.as_mut().expect("hash state exists");
                            table
                                .row(ptr, build_buf, build_types)
                                .map_err(|error| ExecError::SpillFailed(error.to_string()))?
                        };
                        let joined = self.join_rows(&probe_row, &build_row);
                        if !self.matches(&joined)? {
                            continue;
                        }
                        matched = true;
                        match self.kind {
                            JoinKind::Inner | JoinKind::Left | JoinKind::Right => {
                                self.append(req, &joined);
                            }
                            JoinKind::Semi => {
                                self.append(req, &probe_row);
                                break;
                            }
                            JoinKind::AntiSemi => break,
                        }
                    }
                    if !matched {
                        match self.kind {
                            JoinKind::Left | JoinKind::Right => {
                                self.append(req, &self.padded_row(&probe_row));
                            }
                            JoinKind::AntiSemi => self.append(req, &probe_row),
                            JoinKind::Inner | JoinKind::Semi => {}
                        }
                    }
                }
            } else {
                let outer_is_left = self.outer_is_left();
                let mut matched = false;
                for ptr in candidates {
                    let result = {
                        let HashState {
                            table,
                            build_buf,
                            build_types: _,
                            ..
                        } = self.hash.as_mut().expect("hash state exists");
                        table
                            .with_row(ptr, build_buf, |build_row| {
                                matched = true;
                                match self.kind {
                                    JoinKind::Inner | JoinKind::Left | JoinKind::Right => {
                                        Self::append_joined_chunk_row_order(
                                            req,
                                            outer_is_left,
                                            &probe_row,
                                            build_row,
                                        );
                                    }
                                    JoinKind::Semi => {
                                        for (column, value) in probe_row.iter().enumerate() {
                                            req.append_datum(column, value);
                                        }
                                    }
                                    JoinKind::AntiSemi => {}
                                }
                            })
                            .map_err(|error| ExecError::SpillFailed(error.to_string()))
                    };
                    result?;
                    if matches!(self.kind, JoinKind::Semi) && matched {
                        break;
                    }
                }
                if !matched {
                    match self.kind {
                        JoinKind::Left | JoinKind::Right => {
                            self.append(req, &self.padded_row(&probe_row));
                        }
                        JoinKind::AntiSemi => self.append(req, &probe_row),
                        JoinKind::Inner | JoinKind::Semi => {}
                    }
                }
            }
            self.hash
                .as_mut()
                .expect("hash state exists in this arm")
                .probe_row += 1;
        }
    }

    /// Chunk-backed steady state for a pure equality hash join. When the
    /// preserved side is built, matched bits are marked here and unmatched
    /// preserved rows are emitted by the post-probe scan.
    fn drain_chunk_backed_probe(
        &mut self,
        req: &mut Chunk,
        probe_is_left: bool,
        probe_types: &[FieldType],
    ) -> Result<(), ExecError> {
        let keys = self.keys.clone();
        let kind = self.kind;
        let builds_preserved = self.hash_builds_preserved_side();
        let offset = |key: &EquiKey| if probe_is_left { key.left } else { key.right };
        let use_exact_int = self
            .hash
            .as_ref()
            .is_some_and(|hash| hash.table.has_exact_int());
        let exact_int = keys.first().filter(|key| {
            use_exact_int && keys.len() == 1 && key.class == KeyClass::Int && !key.null_safe
        });
        loop {
            let Some(hash) = self.hash.as_mut() else {
                return Ok(());
            };
            if hash.probe_row >= hash.probe_chunk.num_rows() || req.is_full() {
                return Ok(());
            }
            let probe_index = hash.probe_row;
            let probe_row = hash.probe_chunk.get_row(probe_index);
            let exact_key = exact_int.and_then(|key| {
                exact_int_key_chunk(probe_row, offset(key), &probe_types[offset(key)])
            });
            // The exact integer index uses the signed comparison-domain key
            // directly. Avoid encoding the same value through the generic
            // FNV path as well; this is the common single-column TPC-H join
            // shape.
            let key = if exact_int.is_some() {
                None
            } else {
                row_hash_chunk(&keys, probe_row, probe_types, offset).map_err(key_error)?
            };
            let mut matched = false;
            {
                let HashState {
                    table,
                    build_types,
                    build_buf,
                    probe_chunk,
                    ..
                } = hash;
                let probe_row = probe_chunk.get_row(probe_index);
                let candidates: &[RowPtr] = if exact_int.is_some() {
                    exact_key.map_or(&[], |key| table.probe_exact_int(key))
                } else {
                    key.map_or(&[], |key| table.probe(key))
                };
                // Marking a preserved build row mutates the same table that
                // owns `candidates`, so defer those bitmap writes until the
                // immutable slice is no longer borrowed. A unique build key
                // (TPC-H q13's customer key) stays entirely on the stack;
                // only a true one-to-many bucket allocates overflow storage.
                let mut first_matched_ptr = None;
                let mut additional_matched_ptrs = Vec::new();
                for &ptr in candidates {
                    let accepted = table
                        .with_row(ptr, build_buf, |build_row| {
                            let (left, left_types, right, right_types) = if probe_is_left {
                                (probe_row, probe_types, build_row, build_types.as_slice())
                            } else {
                                (build_row, build_types.as_slice(), probe_row, probe_types)
                            };
                            if exact_int.is_none()
                                && !equi_keys_equal_chunk_rows(
                                    &keys,
                                    left,
                                    left_types,
                                    right,
                                    right_types,
                                )?
                            {
                                return Ok(false);
                            }
                            match kind {
                                JoinKind::Inner | JoinKind::Left | JoinKind::Right => {
                                    Self::append_joined_chunk_rows_order(
                                        req,
                                        probe_is_left,
                                        probe_row,
                                        build_row,
                                    );
                                }
                                // With the preserved side built, emission
                                // belongs to the post-probe build scan; the
                                // probe pass only marks matches.
                                JoinKind::Semi if !builds_preserved => {
                                    req.append_partial_row(0, probe_row);
                                }
                                JoinKind::Semi | JoinKind::AntiSemi => {}
                            }
                            Ok(true)
                        })
                        .map_err(|error| ExecError::SpillFailed(error.to_string()))?
                        .map_err(key_error)?;
                    if builds_preserved && accepted {
                        if first_matched_ptr.is_none() {
                            first_matched_ptr = Some(ptr);
                        } else {
                            additional_matched_ptrs.push(ptr);
                        }
                    }
                    matched |= accepted;
                    // First match settles a probe-side semi join, but when the
                    // preserved side was built every matching build row must
                    // still be marked for the post-probe scan.
                    if matches!(kind, JoinKind::Semi) && matched && !builds_preserved {
                        break;
                    }
                }
                if let Some(ptr) = first_matched_ptr {
                    table.mark_matched(ptr);
                }
                for ptr in additional_matched_ptrs {
                    table.mark_matched(ptr);
                }
            }
            if !matched && !builds_preserved {
                let HashState {
                    build_types,
                    probe_chunk,
                    ..
                } = hash;
                let probe_row = probe_chunk.get_row(probe_index);
                match kind {
                    JoinKind::Left | JoinKind::Right => Self::append_unmatched_probe_chunk_row(
                        req,
                        probe_is_left,
                        probe_row,
                        build_types.len(),
                    ),
                    JoinKind::AntiSemi => req.append_partial_row(0, probe_row),
                    JoinKind::Inner | JoinKind::Semi => {}
                }
            }
            hash.probe_row += 1;
        }
    }

    /// Chunk-backed hash probe for an inner join with non-equality ON
    /// predicates. The complete equality key is still checked after a bucket
    /// hit; only the temporary `Vec<Datum>` rows and per-candidate condition
    /// chunk allocation are removed.
    fn drain_chunk_backed_residual_probe(
        &mut self,
        req: &mut Chunk,
        probe_is_left: bool,
        probe_types: &[FieldType],
    ) -> Result<(), ExecError> {
        let keys = self.keys.clone();
        let offset = |key: &EquiKey| if probe_is_left { key.left } else { key.right };
        let use_exact_int = self
            .hash
            .as_ref()
            .is_some_and(|hash| hash.table.has_exact_int());
        let exact_int = keys.first().filter(|key| {
            use_exact_int && keys.len() == 1 && key.class == KeyClass::Int && !key.null_safe
        });
        let conditions = &self.residual_conditions;
        let decimal_mul_lt = self.residual_decimal_mul_lt.as_ref();
        let product_build_column = decimal_mul_lt.and_then(|fast| {
            let left_width = if probe_is_left {
                probe_types.len()
            } else {
                self.left.ret_field_types().len()
            };
            let product_is_left = fast.right_column < left_width;
            let build_is_left = !probe_is_left;
            (product_is_left == build_is_left).then_some(if product_is_left {
                fast.right_column
            } else {
                fast.right_column - left_width
            })
        });
        let ctx = &self.ctx;
        let condition_evals = &self.condition_evals;
        let condition_chunk = &mut self.condition_chunk;
        loop {
            let Some(hash) = self.hash.as_mut() else {
                return Ok(());
            };
            if hash.probe_row >= hash.probe_chunk.num_rows() || req.is_full() {
                return Ok(());
            }
            let probe_index = hash.probe_row;
            let probe_row = hash.probe_chunk.get_row(probe_index);
            let exact_key = exact_int.and_then(|key| {
                exact_int_key_chunk(probe_row, offset(key), &probe_types[offset(key)])
            });
            let key = if exact_int.is_some() {
                None
            } else {
                row_hash_chunk(&keys, probe_row, probe_types, offset).map_err(key_error)?
            };
            let candidates: &[RowPtr] = if exact_int.is_some() {
                exact_key.map_or(&[], |key| hash.table.probe_exact_int(key))
            } else {
                key.map_or(&[], |key| hash.table.probe(key))
            };
            for &ptr in candidates {
                let cached_product = match (decimal_mul_lt, product_build_column) {
                    (Some(fast), Some(column)) => {
                        if !hash.decimal_mul_products.contains_key(&ptr) {
                            let product = {
                                let table = &hash.table;
                                let build_types = &hash.build_types;
                                let build_buf = &mut hash.build_buf;
                                table
                                    .with_row(ptr, build_buf, |build_row| {
                                        Self::decimal_mul_product(
                                            fast,
                                            build_row,
                                            build_types,
                                            column,
                                        )
                                    })
                                    .map_err(|error| ExecError::SpillFailed(error.to_string()))??
                            };
                            hash.decimal_mul_products.insert(ptr, product);
                        }
                        Some(hash.decimal_mul_products.get(&ptr).copied().flatten())
                    }
                    _ => None,
                };
                let table = &hash.table;
                let build_types = &hash.build_types;
                let build_buf = &mut hash.build_buf;
                table
                    .with_row(ptr, build_buf, |build_row| {
                        let (left, left_types, right, right_types) = if probe_is_left {
                            (probe_row, probe_types, build_row, build_types.as_slice())
                        } else {
                            (build_row, build_types.as_slice(), probe_row, probe_types)
                        };
                        if exact_int.is_none()
                            && !equi_keys_equal_chunk_rows(
                                &keys,
                                left,
                                left_types,
                                right,
                                right_types,
                            )
                            .map_err(key_error)?
                            || !(match decimal_mul_lt {
                                Some(fast) => Self::matches_decimal_mul_lt(
                                    condition_evals,
                                    fast,
                                    left,
                                    left_types,
                                    right,
                                    right_types,
                                    cached_product.as_ref().map(Option::as_ref),
                                )?,
                                None => Self::matches_chunk_rows(
                                    ctx,
                                    conditions,
                                    condition_evals,
                                    condition_chunk,
                                    left,
                                    right,
                                )?,
                            })
                        {
                            return Ok::<(), ExecError>(());
                        }
                        Self::append_joined_chunk_rows_order(
                            req,
                            probe_is_left,
                            probe_row,
                            build_row,
                        );
                        Ok::<(), ExecError>(())
                    })
                    .map_err(|error| ExecError::SpillFailed(error.to_string()))??;
                if req.is_full() {
                    break;
                }
            }
            hash.probe_row += 1;
        }
    }

    /// Emits preserved build rows after every probe row is done. Outer joins
    /// emit unmatched rows with NULL padding; semi joins emit matched rows,
    /// and anti-semi joins emit unmatched rows. This is Go v2 `ScanRowTable`,
    /// including build rows with NULL keys that were never in a hash bucket.
    fn drain_preserved_build_rows(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        while !req.is_full() {
            let row = {
                let Some(hash) = self.hash.as_mut() else {
                    return Ok(());
                };
                let Some(ptr) = hash.unmatched_build_scan else {
                    return Ok(());
                };
                hash.unmatched_build_scan = hash.table.next_ptr(ptr);
                let matched = hash.table.is_matched(ptr);
                let emit = match self.kind {
                    JoinKind::Left | JoinKind::Right | JoinKind::AntiSemi => !matched,
                    JoinKind::Semi => matched,
                    JoinKind::Inner => false,
                };
                if !emit {
                    None
                } else {
                    Some(
                        hash.table
                            .row(ptr, &mut hash.build_buf, &hash.build_types)
                            .map_err(|error| ExecError::SpillFailed(error.to_string()))?,
                    )
                }
            };
            if let Some(row) = row {
                if matches!(self.kind, JoinKind::Left | JoinKind::Right) {
                    self.append(req, &self.padded_row(&row));
                } else {
                    self.append(req, &row);
                }
            }
        }
        Ok(())
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

/// Materializes a chunk list only for index-join paths that still need to
/// apply a derived aggregation or a post-lookup non-NULL filter. The normal
/// lookup path keeps source chunks and addresses rows by [`RowPtr`].
fn list_datum_rows(list: &List, field_types: &[FieldType]) -> Vec<Vec<Datum>> {
    let mut rows = Vec::with_capacity(list.len());
    list.walk(|row| {
        rows.push(row.get_datum_row(field_types));
        Ok::<(), ()>(())
    })
    .expect("a List walk with an infallible callback cannot fail");
    rows
}

/// Rebuilds a list after a transformation that changes its row set. These
/// transformations are uncommon; using one wide chunk keeps the conversion
/// bounded and leaves the steady-state lookup path chunk-backed.
fn replace_list_with_rows(list: &mut List, field_types: &[FieldType], rows: Vec<Vec<Datum>>) {
    list.clear();
    if rows.is_empty() {
        return;
    }
    let mut chunk = Chunk::new_with_capacity(field_types, rows.len());
    for row in &rows {
        for (column, datum) in row.iter().enumerate() {
            chunk.append_datum(column, datum);
        }
    }
    list.add(chunk);
}

/// One chunk row as owned `Datum`s.
fn datum_row(chunk: &Chunk, index: usize, types: &[FieldType]) -> Vec<Datum> {
    chunk.get_row(index).get_datum_row(types)
}

/// Recognizes the exact DECIMAL residual shape used by TPC-H q17. The
/// expression must be one `LT`, its left operand a DECIMAL column, and its
/// right operand a multiplication of a strict DECIMAL constant and another
/// DECIMAL column. Any broader recognition would risk changing MySQL's type
/// promotion or NULL behavior, so all other shapes use the normal evaluator.
fn residual_decimal_mul_lt(conditions: &[Expression]) -> Option<DecimalMulLtFastPath> {
    let [Expression::ScalarFunction(lt)] = conditions else {
        return None;
    };
    if lt.func_name.lowercase() != "lt" || lt.args.len() != 2 {
        return None;
    }
    let (Expression::Column(left), Expression::ScalarFunction(mul)) = (&lt.args[0], &lt.args[1])
    else {
        return None;
    };
    if mul.func_name.lowercase() != "mul" || mul.args.len() != 2 {
        return None;
    }
    let (Expression::Constant(factor), Expression::Column(right)) = (&mul.args[0], &mul.args[1])
    else {
        return None;
    };
    if factor.param_marker.is_some() || factor.deferred_expr.is_some() {
        return None;
    }
    let Datum::Decimal(factor) = &factor.value else {
        return None;
    };
    let decimal_column = |column: &tidb_expr::expression::Column| {
        column
            .ret_type
            .as_ref()
            .is_some_and(|field_type| field_type.eval_type() == EvalType::Decimal)
    };
    if !decimal_column(left) || !decimal_column(right) {
        return None;
    }
    let left_column = usize::try_from(left.index).ok()?;
    let right_column = usize::try_from(right.index).ok()?;
    Some(DecimalMulLtFastPath {
        left_column,
        right_column,
        factor: factor.clone(),
    })
}

fn decimal_mul_lt_mysql(
    left: &Decimal,
    factor: &Decimal,
    right: &Decimal,
) -> Result<bool, tidb_datatype::DecimalCodecWarning> {
    let (product, warning) = factor.mul_mysql(right);
    match warning {
        Some(tidb_datatype::DecimalCodecWarning::Overflow) => Err(warning.unwrap()),
        Some(tidb_datatype::DecimalCodecWarning::Truncated) | None => Ok(left < &product),
    }
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

fn merge_key_cmp_row(
    left: &[Datum],
    right: Row<'_>,
    key_offsets: &[usize],
    types: &[FieldType],
    desc: bool,
) -> Result<Ordering, ExecError> {
    for (left, &offset) in left.iter().zip(key_offsets) {
        let mut cmp = tidb_expr::compare_datums(left, &right.get_datum(offset, &types[offset]))?;
        if desc {
            cmp = cmp.reverse();
        }
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

fn merge_rows_cmp(
    left: Row<'_>,
    right: Row<'_>,
    key_offsets: &[usize],
    types: &[FieldType],
    desc: bool,
) -> Result<Ordering, ExecError> {
    for &offset in key_offsets {
        let mut cmp = tidb_expr::compare_datums(
            &left.get_datum(offset, &types[offset]),
            &right.get_datum(offset, &types[offset]),
        )?;
        if desc {
            cmp = cmp.reverse();
        }
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

fn merge_row_key_cmp(
    row: Row<'_>,
    types: &[FieldType],
    key_offsets: &[usize],
    key: &[Datum],
    desc: bool,
) -> Result<Ordering, ExecError> {
    for (&offset, key) in key_offsets.iter().zip(key) {
        let mut cmp = tidb_expr::compare_datums(&row.get_datum(offset, &types[offset]), key)?;
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
        if let Some(state) = self.index_state.take() {
            let current =
                state.outer.iter().map(|row| row_bytes(row)).sum::<i64>() + state.inner_bytes;
            let pending = state
                .pending
                .iter()
                .map(|task| task.outer_bytes)
                .sum::<i64>();
            self.tracker.consume(-(current + pending));
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
