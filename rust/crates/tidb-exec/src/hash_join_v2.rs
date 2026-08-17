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

//! `pkg/executor/join/hash_join_v2.go`: the hash join v2 executor -- the
//! driver that owns the partitioned row tables, merges them into the hash
//! table, and then runs build and probe over them.
//!
//! This is the *top* of hash join v2. Everything it stands on is already
//! ported and is reused here rather than restated:
//!
//! * [`crate::join_row_table`]: [`RowTable`], [`RowTableSegment`], the row
//!   layout, and `initTaggedBits`.
//! * [`crate::hash_table_v2`]: [`HashTableV2`], [`SubTable`], `newSubTable`,
//!   `build`, `lookup`, [`get_hash_table_length_by_row_table`],
//!   [`get_hash_table_memory_usage`], [`MINIMAL_HASH_TABLE_LEN`] (Go declares
//!   `minimalHashTableLen` at the top of *this* file and uses it from both;
//!   one Rust constant serves both).
//! * [`crate::row_table_builder`]: [`RowTableBuilder`] (`createRowTableBuilder`,
//!   `processOneChunk`), and the three helpers Go also declares in this file
//!   but which the build side already needed -- [`gen_hash_join_partition_number`],
//!   [`get_partition_mask_offset`], [`generate_partition_index`], [`rehash`],
//!   and `fakeSel`/[`FAKE_SEL_LENGTH`].
//! * [`crate::tagged_ptr`]: [`TagPtrHelper`], [`MAX_TAGGED_BITS`].
//! * [`crate::base_join_probe`]: [`ProbeContext`] and [`BaseJoinProbe`]. The
//!   probe half is *not* re-derived here; this file only drives it.
//! * `tidb_util::memory::Tracker`: Go `memory.Tracker`, including
//!   `AttachTo`/`Detach`/`Consume`, so the hash-table memory accounting is
//!   real rather than mocked.
//! * `tidb_executor::joiner::JoinType`: Go `plannerbase.JoinType`.
//!
//! ## Sequential here, worker-parallel there
//!
//! Go's `HashJoinV2Exec` is a goroutine topology, and reproducing that
//! topology is explicitly *not* the goal; reproducing what the topology
//! computes is. Go runs, per round:
//!
//! 1. one build-side fetcher goroutine feeding `srcChkCh`,
//! 2. `Concurrency` build workers, each calling
//!    `rowTableBuilder.processOneChunk` and appending finished segments into
//!    its **own** row-table row (`hashTableContext.rowTables[workerID]`),
//! 3. a single-threaded merge (`mergeRowTablesToHashTable`) that concatenates
//!    every worker's per-partition tables into one table per partition and
//!    allocates the sub tables,
//! 4. one task-creation goroutine feeding `buildTaskCh`, and `Concurrency`
//!    hash-table build workers consuming it,
//! 5. one probe-side fetcher goroutine and `Concurrency` probe workers, each
//!    with its own `ProbeV2`, all reading the finished hash table,
//! 6. finally, if the join needs it, `Concurrency` row-table scanners.
//!
//! Every one of those stages is a *partitioned* computation with no
//! cross-worker mutable state on the path that determines which rows come
//! out:
//!
//! * Stage 2's workers write disjoint `rowTables[workerID]` rows. Which
//!   worker gets which chunk is nondeterministic in Go, and it is observable
//!   -- but only as segment *order within a partition*, which feeds only the
//!   bucket-chain order, which Go already does not promise.
//! * Stage 4's workers take disjoint `[segStartIdx, segEndIdx)` ranges of one
//!   partition. [`SubTable::build`] already carries the atomic/plain
//!   distinction that makes this safe, selected by the same
//!   whole-range-versus-partial-range test; the *set* of chain links built is
//!   the same either way, only their order within a bucket differs.
//! * Stage 5's workers each own their `BaseJoinProbe` and share only the
//!   immutable hash table (plus the used-flag bit, which is set with an
//!   atomic OR by the outer/semi probes, and whose *final* value after all
//!   workers finish is order-independent).
//!
//! So this port drives all six stages sequentially, and what changes is only
//! what Go already leaves unspecified:
//!
//! * **Same rows out.** Build rows are packed identically, land in the same
//!   partition (the partition index is a pure function of the hash value),
//!   and chain into the same buckets.
//! * **Order within one probe chunk is identical**, which is all Go promises.
//!   Hash join v2 is unordered *across* chunks -- chunks go to whichever
//!   worker is free and results are merged in completion order. This is the
//!   same finding [`crate::base_join_probe`]'s header records, and a
//!   sequential driver simply picks one of the orderings Go already permits.
//! * **Bucket-chain order** is one of the orderings Go permits, for the same
//!   reason: `mergeRowTablesToHashTable` concatenates worker tables in worker
//!   order but the workers' *contents* raced, and stage 4's ranges are handed
//!   out round-robin across partitions.
//! * [`create_tasks`] is ported literally, returning the task list instead of
//!   pushing it down a channel, because the *partitioning* of segments into
//!   tasks (and hence which segments a given build call links) is a real
//!   computation and worth pinning, even though its consumption is
//!   sequential here.
//!
//! ### What is genuinely lost
//!
//! * **Cancellation.** `closeCh`, `finished`, `SQLKiller`, the `select`
//!   arms in `runJoinWorker`/`getNewJoinResult`/`controlWorkersForRestore`,
//!   and `handleProbeWorkerPanic`/`handleJoinWorkerPanic` have no sequential
//!   counterpart: a sequential driver has no other goroutine to be cancelled
//!   *by*. Observably this only removes the ability to stop early; a run that
//!   is allowed to finish produces the same rows.
//! * **Spill.** The whole restore loop (`startBuildAndProbe`'s outer `for`,
//!   `inRestore`, `restoredBuildInDisk`/`restoredProbeInDisk`,
//!   `prepareForRestoring`, `spillHelper.stack`) is out of reach, so this
//!   port is the single-round, memory-resident case. Observably, spill does
//!   not change the result set -- it changes only when memory is released --
//!   *except* that a spilled round re-partitions with a rehash, which again
//!   only reorders.
//! * **Runtime stats.** `hashJoinRuntimeStatsV2` and every
//!   `atomic.AddInt64`/`setMaxValue` around it are timing measurements of the
//!   concurrency this port does not have.
//!
//! ## Narrowings (every one named)
//!
//! * **`hashJoinSpillHelper`.** Blocking symbol for: `tryToSpill`,
//!   `mergeRowTablesToHashTable`'s `spillHelper` argument,
//!   `collectSpillStats`, `initMaxSpillRound`'s consumers, `releaseDisk`,
//!   `restoreAndProbe`, `processOneRestoredProbeChunk`,
//!   `splitPartitionAndAppendToRowTableForRestore`,
//!   `controlWorkersForRestore`, `getRestoredBuildChunkNum`,
//!   `getProbeSpillChunkFieldTypes`, and `HashJoinV2Exec.reset`'s
//!   `setCanSpillFlag`. [`HashTableContext::merge_row_tables_to_hash_table`]
//!   ports the `spillHelper == nil` arm, which Go itself documents as the
//!   unit-test path ("spillHelper may be nil in ut").
//!   [`HashJoinV2Exec::init_max_spill_round`] *is* ported: it is pure
//!   arithmetic and pins the round budget the spill path would use.
//! * **`hashJoinRuntimeStatsV2`.** Blocking symbol: `hashJoinRuntimeStatsV2`
//!   (`hash_join.go`), plus `setMaxValue`. All `stats` field updates are
//!   dropped.
//! * **`exec.Executor` / `exec.BaseExecutor` / session context.** `Open`,
//!   `Close`, `Next`, `OpenSelf`, `AllocPool`, `memTracker`/`diskTracker`
//!   attachment to `StmtCtx`, and `RuntimeStatsColl.RegisterStats` are not
//!   ported. Blocking symbols: `exec.BaseExecutor`,
//!   `sessionctx.Context.GetSessionVars`, `disk.Tracker`.
//!   [`HashJoinCtxV2`] carries exactly the fields this file reads.
//! * **`hashJoinCtxBase` / `probeSideTupleFetcherBase` / `probeWorkerBase` /
//!   `buildWorkerBase`.** These live in `hash_join_base.go` (channels,
//!   `joinResultCh`, `probeChkResourceCh`, `fetchProbeSideChunks`). Not
//!   ported; the sequential driver takes chunks directly.
//!   [`HashJoinV2Exec::run_probe`] takes a chunk-allocation closure where Go
//!   takes `joinChkResourceCh`.
//! * **`ProbeV2`.** Go declares the interface in `join_probe.go`. Only the
//!   methods *this* file calls are declared here, as [`ProbeV2`]; the
//!   per-join-type implementations (`innerJoinProbe`, `outerJoinProbe`, ...)
//!   are the same symbols [`crate::base_join_probe`] stops at.
//! * **`NewJoinBuildWorkerV2`'s nullability test.** Go reads
//!   `mysql.HasNotNullFlag(buildTypes[idx].GetFlag())`. This takes the
//!   already-evaluated flags as `build_column_not_null`, matching the
//!   `probe_key_nullable` seam [`crate::base_join_probe`]'s `new_join_probe`
//!   established.
//! * **`checkBalance`'s `math.Abs`.** Ported as a symmetric integer
//!   difference; the operands are both integers, so the float round-trip is
//!   not observable.
//! * **`triggerIntest` / `issue59377Intest` / `failpoint.Inject`.** Dropped:
//!   they inject random panics and errors under a failpoint and have no
//!   behavior outside one. Blocking symbol: `github.com/pingcap/failpoint`.
//! * **`EnableHashJoinV2` / `DisableHashJoinV2` / `HashJoinV2Strings`.**
//!   Test-only SQL strings; [`crate::hash_join_version`] already holds the
//!   variable values they concatenate.
//! * **`isAllMemoryClearedForTest` / `FileNamePrefixForTest` /
//!   `spillTriggeredBeforeBuildingHashTableForTest` and friends.** Test hooks
//!   on the spill path.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_executor::joiner::JoinType;
use tidb_util::memory::Tracker;

use crate::base_join_probe::ProbeError;
use crate::hash_table_v2::{
    get_hash_table_length_by_row_table, get_hash_table_memory_usage, HashTableV2, SubTable,
};
use crate::join_row_table::{RowTable, RowTableSegment};
use crate::row_table_builder::{BuildChunk, BuildContext, RowTableBuildError, RowTableBuilder};
use crate::tagged_ptr::{TagPtrHelper, MAX_TAGGED_BITS};

/// Go `memory.LabelForHashTableInHashJoinV2` (`util/memory/tracker.go:934`).
pub const LABEL_FOR_HASH_TABLE_IN_HASH_JOIN_V2: i64 = -32;

/// Go's cap on `genHashJoinPartitionNumber`'s doubling loop.
///
/// [`crate::row_table_builder::gen_hash_join_partition_number`] already
/// encodes it; the constant is named here because `checkBalance` and
/// `initMaxSpillRound` both reason about the same ceiling.
pub const MAX_PARTITION_NUMBER: usize = 16;

/// Go `buildTask`: one contiguous run of segments of one partition, to be
/// linked into that partition's hash table by a single build call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildTask {
    /// Go `partitionIdx`.
    pub partition_idx: usize,
    /// Go `segStartIdx`, inclusive.
    pub seg_start_idx: usize,
    /// Go `segEndIdx`, exclusive.
    pub seg_end_idx: usize,
}

/// Errors the sequential driver surfaces.
///
/// Go returns `error` through `errCh`/`buildFinished`/`joinResult.err`; the
/// channels are not ported, so the two error sources this file can actually
/// reach are named directly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HashJoinV2Error {
    /// A build chunk could not be packed into row-table bytes.
    Build(RowTableBuildError),
    /// A probe chunk could not be prepared or probed.
    Probe(ProbeError),
}

impl std::fmt::Display for HashJoinV2Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build(error) => write!(formatter, "hash join v2 build failed: {error}"),
            Self::Probe(error) => write!(formatter, "hash join v2 probe failed: {error}"),
        }
    }
}

impl std::error::Error for HashJoinV2Error {}

impl From<RowTableBuildError> for HashJoinV2Error {
    fn from(error: RowTableBuildError) -> Self {
        Self::Build(error)
    }
}

impl From<ProbeError> for HashJoinV2Error {
    fn from(error: ProbeError) -> Self {
        Self::Probe(error)
    }
}

// ---------------------------------------------------------------------------
// hashTableContext (`hash_join_v2.go:70`)
// ---------------------------------------------------------------------------

/// Go `hashTableContext`: the per-worker row tables during the split stage,
/// and the merged hash table afterwards.
///
/// Not `Debug`: it owns a `memory.Tracker`, which is not `Debug` either.
pub struct HashTableContext {
    /// Go `rowTables`, indexed `[workerID][partitionID]`. `None` is Go's
    /// `nil` entry, which `appendRowSegment` fills lazily.
    pub row_tables: Vec<Vec<Option<RowTable>>>,
    /// Go `hashTable`. Its per-partition sub tables stay `None` until
    /// [`Self::merge_row_tables_to_hash_table`] allocates them, exactly as
    /// Go's `make([]*subTable, partitionNumber)` leaves them nil.
    pub hash_table: HashTableV2,
    /// Go `tagHelper`, initialized by the merge from the narrowest segment.
    pub tag_helper: TagPtrHelper,
    /// Go `memoryTracker`.
    pub memory_tracker: Arc<Tracker>,
}

impl HashTableContext {
    /// Go `(*HashJoinCtxV2).initHashTableContext`.
    #[must_use]
    pub fn new(concurrency: usize, partition_number: usize) -> Self {
        Self {
            row_tables: (0..concurrency)
                .map(|_| (0..partition_number).map(|_| None).collect())
                .collect(),
            hash_table: HashTableV2 {
                tables: (0..partition_number).map(|_| None).collect(),
                partition_number: partition_number as u64,
            },
            tag_helper: TagPtrHelper::default(),
            memory_tracker: Tracker::new(LABEL_FOR_HASH_TABLE_IN_HASH_JOIN_V2, -1),
        }
    }

    /// Go `reset`.
    ///
    /// Go nils `rowTables`, `hashTable` and `tagHelper`; emptying them is the
    /// same release, and keeps the struct usable rather than half-nil.
    pub fn reset(&mut self) {
        self.row_tables.clear();
        self.hash_table.tables.clear();
        self.hash_table.partition_number = 0;
        self.tag_helper = TagPtrHelper::default();
        self.memory_tracker.detach();
    }

    /// Go `getAllMemoryUsageInHashTable`.
    #[must_use]
    pub fn get_all_memory_usage_in_hash_table(&self) -> i64 {
        (0..self.hash_table.tables.len())
            .map(|part_id| self.hash_table.get_partition_memory_usage(part_id))
            .sum()
    }

    /// Go `clearHashTable`.
    pub fn clear_hash_table(&mut self) {
        for part_id in 0..self.hash_table.tables.len() {
            self.hash_table.clear_partition_segments(part_id);
        }
    }

    /// Go `getPartitionMemoryUsage`: one partition's bytes, summed over every
    /// worker's row table.
    #[must_use]
    pub fn get_partition_memory_usage(&self, part_id: usize) -> i64 {
        self.row_tables
            .iter()
            .filter_map(|tables| tables.get(part_id).and_then(Option::as_ref))
            .map(RowTable::get_total_memory_usage)
            .sum()
    }

    /// Go `getSegmentsInRowTable`; the empty slice stands in for Go's `nil`.
    #[must_use]
    pub fn get_segments_in_row_table(
        &self,
        worker_id: usize,
        partition_id: usize,
    ) -> &[RowTableSegment] {
        self.row_tables[worker_id][partition_id]
            .as_ref()
            .map_or(&[], RowTable::get_segments)
    }

    /// Go `getAllSegmentsMemoryUsageInRowTable`.
    #[must_use]
    pub fn get_all_segments_memory_usage_in_row_table(&self) -> i64 {
        self.row_tables
            .iter()
            .flatten()
            .flatten()
            .map(RowTable::get_total_memory_usage)
            .sum()
    }

    /// Go `clearAllSegmentsInRowTable`.
    pub fn clear_all_segments_in_row_table(&mut self) {
        for table in self.row_tables.iter_mut().flatten().flatten() {
            table.clear_segments();
        }
    }

    /// Go `clearSegmentsInRowTable`.
    pub fn clear_segments_in_row_table(&mut self, worker_id: usize, partition_id: usize) {
        if let Some(table) = self.row_tables[worker_id][partition_id].as_mut() {
            table.clear_segments();
        }
    }

    /// Go `build`: link one task's segment range into its partition.
    ///
    /// # Panics
    ///
    /// Panics when the partition has no sub table, matching Go's nil
    /// dereference; `mergeRowTablesToHashTable` must have run.
    pub fn build(&mut self, task: &BuildTask) {
        let Self {
            hash_table,
            tag_helper,
            ..
        } = self;
        hash_table.tables[task.partition_idx]
            .as_mut()
            .expect("sub table of a merged partition")
            .build(task.seg_start_idx, task.seg_end_idx, tag_helper);
    }

    /// Go `lookup`: the bucket head for `hash_value` in one partition.
    #[must_use]
    pub fn lookup(&self, partition_index: usize, hash_value: u64) -> usize {
        self.hash_table
            .sub_table(partition_index)
            .lookup(hash_value, &self.tag_helper)
    }

    /// Go `appendRowSegment`.
    ///
    /// Drops empty segments, creates the worker's row table on first use, and
    /// derives the segment's tag width before it is stored -- the three steps
    /// Go performs in that order.
    pub fn append_row_segment(
        &mut self,
        worker_id: usize,
        partition_id: usize,
        mut segment: RowTableSegment,
    ) {
        if segment.hash_values.is_empty() {
            return;
        }
        segment.init_tagged_bits();
        self.row_tables[worker_id][partition_id]
            .get_or_insert_with(RowTable::new)
            .segments
            .push(segment);
    }

    /// Go `calculateHashTableMemoryUsage`: the total, and the per-partition
    /// split the spill path needs.
    #[must_use]
    pub fn calculate_hash_table_memory_usage(row_tables: &[RowTable]) -> (i64, Vec<i64>) {
        let per_partition: Vec<i64> = row_tables
            .iter()
            .map(|table| get_hash_table_memory_usage(get_hash_table_length_by_row_table(table)))
            .collect();
        (per_partition.iter().sum(), per_partition)
    }

    /// Go `mergeRowTablesToHashTable`, `spillHelper == nil` arm.
    ///
    /// Concatenates every worker's tables into one table per partition,
    /// allocates the sub tables, and initializes [`Self::tag_helper`] from the
    /// narrowest segment. Returns Go's `totalSegmentCnt`.
    ///
    /// The pre-consume of the hash tables' memory that Go performs inside
    /// `tryToSpill` is done here instead, because it happens on the
    /// `spillHelper == nil` path too -- Go reaches it through `tryToSpill`'s
    /// unconditional `memoryTracker.Consume(totalMemoryUsage)`, which runs
    /// before the `spillHelper != nil` test.
    ///
    /// boundary: `hashJoinSpillHelper` (`hash_join_spill.go`) for the
    /// `spillHelper != nil` arm -- `isSpillNeeded`, `spillRowTable`,
    /// `getSpilledPartitions`, `setCanSpillFlag`.
    pub fn merge_row_tables_to_hash_table(&mut self, partition_number: usize) -> usize {
        let mut row_tables: Vec<RowTable> =
            (0..partition_number).map(|_| RowTable::new()).collect();

        let mut total_segment_cnt = 0;
        for row_tables_per_worker in &mut self.row_tables {
            for (part_idx, table) in row_tables_per_worker.iter_mut().enumerate() {
                let Some(table) = table.take() else { continue };
                total_segment_cnt += table.segments.len();
                // Go merges the pointers and then nils the worker's slice in
                // `clearAllSegmentsInRowTable`; taking the table does both.
                row_tables[part_idx].merge(table);
            }
        }

        let (total_memory_usage, _per_partition) =
            Self::calculate_hash_table_memory_usage(&row_tables);
        self.memory_tracker.consume(total_memory_usage);

        let mut tagged_bits = MAX_TAGGED_BITS;
        for (part_idx, table) in row_tables.into_iter().enumerate() {
            for segment in table.get_segments() {
                tagged_bits = tagged_bits.min(segment.tagged_bits());
            }
            self.hash_table.tables[part_idx] = Some(SubTable::new(table));
        }

        self.tag_helper = TagPtrHelper::default();
        self.tag_helper.init(tagged_bits);
        total_segment_cnt
    }
}

// ---------------------------------------------------------------------------
// HashJoinCtxV2 (`hash_join_v2.go:262`)
// ---------------------------------------------------------------------------

/// Go `HashJoinCtxV2`, narrowed to the fields this file and the probe read.
///
/// Go embeds `hashJoinCtxBase` (channels, `SessCtx`, `Concurrency`,
/// `JoinType`, `finished`) and holds `expression.CNFExprs` for the three
/// filters. Filters appear here only as the booleans the layout and the probe
/// branch on; the expressions themselves are seams on the build and probe
/// sides ([`crate::row_table_builder::BuildFilter`],
/// [`crate::base_join_probe::ProbeFilter`]).
#[derive(Clone, Debug)]
pub struct HashJoinCtxV2 {
    /// Go `hashJoinCtxBase.Concurrency`.
    pub concurrency: usize,
    /// Go `hashJoinCtxBase.JoinType`.
    pub join_type: JoinType,
    /// Go `partitionNumber`, always a power of two.
    pub partition_number: usize,
    /// Go `partitionMaskOffset`.
    pub partition_mask_offset: usize,
    /// Go `RightAsBuildSide`.
    pub right_as_build_side: bool,
    /// Go `BuildFilter != nil`.
    pub has_build_filter: bool,
    /// Go `OtherCondition != nil`, i.e. `hasOtherCondition()`.
    pub has_other_condition: bool,
    /// Go `needScanRowTableAfterProbeDone`.
    pub need_scan_row_table_after_probe_done: bool,
    /// Go `maxSpillRound`.
    pub max_spill_round: usize,
}

impl HashJoinCtxV2 {
    /// A context with Go's post-`OpenSelf` defaults for one concurrency.
    ///
    /// Go builds this across `Open`/`OpenSelf`/`SetupPartitionInfo`; the
    /// pieces that survive the narrowing are exactly these.
    #[must_use]
    pub fn new(concurrency: usize, join_type: JoinType, right_as_build_side: bool) -> Self {
        let mut ctx = Self {
            concurrency,
            join_type,
            partition_number: 1,
            partition_mask_offset: 64,
            right_as_build_side,
            has_build_filter: false,
            has_other_condition: false,
            need_scan_row_table_after_probe_done: false,
            // Go `OpenSelf` sets `maxSpillRound = 1` unconditionally and only
            // raises it when temporary storage is enabled.
            max_spill_round: 1,
        };
        ctx.setup_partition_info();
        ctx
    }

    /// Go `SetupPartitionInfo`.
    pub fn setup_partition_info(&mut self) {
        self.partition_number =
            crate::row_table_builder::gen_hash_join_partition_number(self.concurrency);
        self.partition_mask_offset =
            crate::row_table_builder::get_partition_mask_offset(self.partition_number);
    }

    /// Go `initHashTableContext`.
    #[must_use]
    pub fn init_hash_table_context(&self) -> HashTableContext {
        HashTableContext::new(self.concurrency, self.partition_number)
    }

    /// Go `resetHashTableContextForRestore`.
    ///
    /// Go's `intest.InTest` panic ("All rowTables in hashTableContext should
    /// be cleared") is kept as a debug assertion: it states an invariant of
    /// the merge, not a test-only behavior.
    ///
    /// # Panics
    ///
    /// In debug builds, when any per-worker row table still holds segments.
    pub fn reset_hash_table_context_for_restore(context: &mut HashTableContext) {
        debug_assert_eq!(
            context.get_all_segments_memory_usage_in_row_table(),
            0,
            "All rowTables in hashTableContext should be cleared"
        );
        let memory_usage = context.get_all_memory_usage_in_hash_table();
        context.clear_hash_table();
        context.memory_tracker.consume(-memory_usage);
    }

    /// Go `initMaxSpillRound`: how many re-partition rounds it takes for the
    /// partition count to exceed 1024.
    pub fn init_max_spill_round(&mut self) {
        if self.partition_number > 1024 {
            self.max_spill_round = 1;
            return;
        }
        self.max_spill_round =
            (f64::from(1024_u16).ln() / (self.partition_number as f64).ln()) as usize;
    }

    /// Go `(*ProbeSideTupleFetcherV2).shouldLimitProbeFetchSize`.
    ///
    /// True exactly when the probe side is the outer side of an outer join,
    /// so the fetcher can stop as soon as the required row count is met.
    #[must_use]
    pub const fn should_limit_probe_fetch_size(&self) -> bool {
        match self.join_type {
            JoinType::LeftOuter => self.right_as_build_side,
            JoinType::RightOuter => !self.right_as_build_side,
            _ => false,
        }
    }

    /// Go `canSkipProbeIfHashTableIsEmpty`.
    #[must_use]
    pub const fn can_skip_probe_if_hash_table_is_empty(&self) -> bool {
        match self.join_type {
            JoinType::Inner => true,
            JoinType::LeftOuter => !self.right_as_build_side,
            JoinType::RightOuter | JoinType::SemiJoin => self.right_as_build_side,
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// BuildWorkerV2 (`hash_join_v2.go:415`)
// ---------------------------------------------------------------------------

/// Go `BuildWorkerV2`, narrowed to its build-side state.
///
/// Go's `buildWorkerBase` (`BuildSideExec`, `fetchBuildSideRows`) and
/// `restoredChkBuf` are not ported; chunks arrive directly.
#[derive(Clone, Debug)]
pub struct BuildWorkerV2 {
    /// Go `WorkerID`.
    pub worker_id: usize,
    /// Go `buildWorkerBase.BuildKeyColIdx`.
    pub build_key_col_idx: Vec<usize>,
    /// Go `HasNullableKey`.
    pub has_nullable_key: bool,
    /// Go `builder`; `None` until [`Self::create_builder`] runs, as Go leaves
    /// it nil until `fetchAndBuildHashTableImpl`.
    pub builder: Option<RowTableBuilder>,
}

/// Go `NewJoinBuildWorkerV2`.
///
/// `build_column_not_null[i]` is Go's
/// `mysql.HasNotNullFlag(buildTypes[i].GetFlag())`.
#[must_use]
pub fn new_join_build_worker_v2(
    worker_id: usize,
    build_key_col_idx: Vec<usize>,
    build_column_not_null: &[bool],
) -> BuildWorkerV2 {
    let has_nullable_key = build_key_col_idx
        .iter()
        .any(|&idx| !build_column_not_null[idx]);
    BuildWorkerV2 {
        worker_id,
        build_key_col_idx,
        has_nullable_key,
        builder: None,
    }
}

impl BuildWorkerV2 {
    /// Go's `createRowTableBuilder` call in `fetchAndBuildHashTableImpl`.
    pub fn create_builder(&mut self, ctx: &HashJoinCtxV2, null_map_length: usize) {
        self.builder = Some(RowTableBuilder::new(
            self.build_key_col_idx.clone(),
            ctx.partition_number,
            self.has_nullable_key,
            ctx.has_build_filter,
            ctx.need_scan_row_table_after_probe_done,
            null_map_length,
        ));
    }

    /// Go `processOneChunk`: pack one build chunk and hand every partition's
    /// segment to the shared context.
    ///
    /// Go's builder appends into `hashTableContext` from inside
    /// `rowTableBuilder.appendToRowTable`; the ported builder returns the
    /// segments instead (one per partition, in partition order), so the
    /// append happens here.
    ///
    /// # Errors
    ///
    /// [`RowTableBuildError`] when a column element or a serialized join key
    /// exceeds its 4-byte size prefix.
    ///
    /// # Panics
    ///
    /// Panics when [`Self::create_builder`] has not run, matching Go's nil
    /// `builder` dereference.
    pub fn process_one_chunk(
        &mut self,
        chunk: &BuildChunk,
        build_context: &mut BuildContext<'_>,
        table_context: &mut HashTableContext,
    ) -> Result<(), RowTableBuildError> {
        let builder = self.builder.as_mut().expect("builder created before use");
        let segments = builder.process_one_chunk(chunk, build_context)?;
        for (partition_id, segment) in segments.into_iter().enumerate() {
            table_context.append_row_segment(self.worker_id, partition_id, segment);
        }
        Ok(())
    }

    /// Go `splitPartitionAndAppendToRowTable`, over this worker's share of
    /// the build chunks.
    ///
    /// Go's worker pulls from `srcChkCh` until it closes; which chunks a
    /// given worker sees is a race. This takes the share explicitly, which is
    /// the same computation with the race decided by the caller.
    ///
    /// # Errors
    ///
    /// Propagates the first [`RowTableBuildError`]. Go instead sets `hasErr`,
    /// forwards to `errCh`, and keeps draining `srcChkCh` purely so
    /// `fetcherAndWorkerSyncer.Done()` still runs for every chunk -- there is
    /// no counter to keep balanced here, so it stops at the first error, and
    /// Go reports that same first error.
    pub fn split_partition_and_append_to_row_table(
        &mut self,
        chunks: &[BuildChunk],
        build_context: &mut BuildContext<'_>,
        table_context: &mut HashTableContext,
    ) -> Result<(), RowTableBuildError> {
        for chunk in chunks {
            self.process_one_chunk(chunk, build_context, table_context)?;
        }
        Ok(())
    }

    /// Go `buildHashTable`: consume build tasks and link their segments.
    pub fn build_hash_table(tasks: &[BuildTask], table_context: &mut HashTableContext) {
        for task in tasks {
            table_context.build(task);
        }
    }
}

// ---------------------------------------------------------------------------
// ProbeV2 (declared in `join_probe.go`; only this file's callers are here)
// ---------------------------------------------------------------------------

/// The slice of Go's `ProbeV2` interface that `hash_join_v2.go` calls.
///
/// boundary: `ProbeV2` (`pkg/executor/join/join_probe.go`) and its
/// implementations `innerJoinProbe`, `outerJoinProbe`, `semiJoinProbe`,
/// `antiSemiJoinProbe`, `leftOuterSemiJoinProbe` -- the same symbols
/// [`crate::base_join_probe`]'s `new_join_probe` stops at. Go's
/// `SetRestoredChunkForProbe` and `SpillRemainingProbeChunks` are omitted:
/// both are spill-only, and [`crate::base_join_probe`] already records them
/// as blocked on `hashJoinSpillHelper`.
pub trait ProbeV2 {
    /// Go `SetChunkForProbe`.
    ///
    /// # Errors
    ///
    /// When the previous chunk is unfinished, or a filter/serializer fails.
    fn set_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError>;

    /// Go `IsCurrentChunkProbeDone`.
    fn is_current_chunk_probe_done(&self) -> bool;

    /// Go `Probe`: append as many output rows as fit into `joined_chk`.
    ///
    /// # Errors
    ///
    /// Propagates the other-condition evaluation failure Go returns in
    /// `joinResult.err`.
    fn probe(&mut self, joined_chk: &mut Chunk) -> Result<(), ProbeError>;

    /// Go `NeedScanRowTable`.
    fn need_scan_row_table(&self) -> bool;

    /// Go `InitForScanRowTable`.
    fn init_for_scan_row_table(&mut self);

    /// Go `IsScanRowTableDone`.
    fn is_scan_row_table_done(&self) -> bool;

    /// Go `ScanRowTable`: append unmatched build rows.
    ///
    /// # Errors
    ///
    /// Propagates what Go puts in `joinResult.err`.
    fn scan_row_table(&mut self, joined_chk: &mut Chunk) -> Result<(), ProbeError>;

    /// Go `ResetProbe`.
    fn reset_probe(&mut self);

    /// Go `ResetProbeCollision`.
    fn reset_probe_collision(&mut self);

    /// Go `GetProbeCollision`.
    fn get_probe_collision(&self) -> u64;
}

// ---------------------------------------------------------------------------
// HashJoinV2Exec (`hash_join_v2.go:608`)
// ---------------------------------------------------------------------------

/// Go `HashJoinV2Exec`, as a sequential driver.
///
/// Go's `Open`/`Next`/`Close` lifecycle, the goroutine wiring, and the
/// restore loop are all out of scope (see the module header); what remains is
/// the computation the goroutines perform, in the order the barriers between
/// them already force.
pub struct HashJoinV2Exec {
    /// Go's embedded `*HashJoinCtxV2`.
    pub ctx: HashJoinCtxV2,
    /// Go `BuildWorkers`.
    pub build_workers: Vec<BuildWorkerV2>,
    /// Go `hashTableContext`.
    pub hash_table_context: HashTableContext,
}

impl HashJoinV2Exec {
    /// Creates the executor and its `Concurrency` build workers.
    ///
    /// `build_column_not_null` is Go's per-build-column
    /// `mysql.HasNotNullFlag`; every worker shares it, as they share
    /// `BuildTypes`.
    #[must_use]
    pub fn new(
        ctx: HashJoinCtxV2,
        build_key_col_idx: &[usize],
        build_column_not_null: &[bool],
    ) -> Self {
        let build_workers = (0..ctx.concurrency)
            .map(|worker_id| {
                new_join_build_worker_v2(
                    worker_id,
                    build_key_col_idx.to_vec(),
                    build_column_not_null,
                )
            })
            .collect();
        let hash_table_context = ctx.init_hash_table_context();
        Self {
            ctx,
            build_workers,
            hash_table_context,
        }
    }

    /// Go `initMaxSpillRound`, forwarded to [`HashJoinCtxV2`].
    pub fn init_max_spill_round(&mut self) {
        self.ctx.init_max_spill_round();
    }

    /// Go `checkBalance`: whether every partition holds roughly the same
    /// number of segments, in which case each partition becomes one task.
    ///
    /// # Panics
    ///
    /// Panics when a partition has no sub table, i.e. before the merge.
    #[must_use]
    pub fn check_balance(&self, total_segment_cnt: usize) -> bool {
        if self.ctx.concurrency != self.ctx.partition_number {
            return false;
        }
        let avg_seg_cnt = total_segment_cnt / self.ctx.partition_number;
        // Go: `int(float64(avgSegCnt) * 0.8)`, i.e. truncated.
        let balance_threshold = (avg_seg_cnt as f64 * 0.8) as usize;
        (0..self.hash_table_context.hash_table.tables.len()).all(|part_id| {
            let segment_count = self
                .hash_table_context
                .hash_table
                .sub_table(part_id)
                .row_data
                .segments
                .len();
            // Go takes `math.Abs` of the difference; both operands are
            // integers, so a symmetric integer difference is the same test.
            segment_count.abs_diff(avg_seg_cnt) <= balance_threshold
        })
    }

    /// Go `createTasks`, returning the task list instead of feeding
    /// `buildTaskCh`.
    ///
    /// The balanced case emits one whole-partition task per partition. The
    /// unbalanced case walks the partitions round-robin in `segStep` slices,
    /// so consecutive tasks touch different partitions -- Go's comment says
    /// this is deliberate, so concurrent builders contend less. The
    /// round-robin is preserved because it determines which segments share a
    /// build call, and hence which build calls take the atomic path in
    /// [`SubTable::build`].
    ///
    /// # Panics
    ///
    /// Panics when a partition has no sub table, i.e. before the merge.
    #[must_use]
    pub fn create_tasks(&self, total_segment_cnt: usize) -> Vec<BuildTask> {
        let is_balanced = self.check_balance(total_segment_cnt);
        let seg_step = 1.max(total_segment_cnt / self.ctx.concurrency);
        let partition_count = self.hash_table_context.hash_table.tables.len();
        let segment_lengths: Vec<usize> = (0..partition_count)
            .map(|part_id| {
                self.hash_table_context
                    .hash_table
                    .sub_table(part_id)
                    .row_data
                    .segments
                    .len()
            })
            .collect();

        let mut tasks = Vec::new();
        if is_balanced {
            for (part_idx, &segments_len) in segment_lengths.iter().enumerate() {
                tasks.push(BuildTask {
                    partition_idx: part_idx,
                    seg_start_idx: 0,
                    seg_end_idx: segments_len,
                });
            }
            return tasks;
        }

        let mut partition_start_index = vec![0_usize; partition_count];
        loop {
            let mut has_new_task = false;
            for part_idx in 0..partition_count {
                if partition_start_index[part_idx] < segment_lengths[part_idx] {
                    let start_index = partition_start_index[part_idx];
                    let end_index = (start_index + seg_step).min(segment_lengths[part_idx]);
                    tasks.push(BuildTask {
                        partition_idx: part_idx,
                        seg_start_idx: start_index,
                        seg_end_idx: end_index,
                    });
                    partition_start_index[part_idx] = end_index;
                    has_new_task = true;
                }
            }
            if !has_new_task {
                break;
            }
        }
        tasks
    }

    /// Go `fetchAndBuildHashTableImpl`, sequentially.
    ///
    /// `chunks_per_worker[i]` is the share Go's `srcChkCh` would have handed
    /// worker `i`. The four stages run in the order Go's `waitJobDone`
    /// barriers already force: split, merge, create tasks, build.
    ///
    /// Returns Go's `totalSegmentCnt`.
    ///
    /// # Errors
    ///
    /// Propagates the first build error, which Go forwards to
    /// `buildFinished`.
    ///
    /// # Panics
    ///
    /// Panics when `chunks_per_worker` is not one share per build worker.
    pub fn fetch_and_build_hash_table(
        &mut self,
        chunks_per_worker: &[Vec<BuildChunk>],
        build_context: &mut BuildContext<'_>,
        null_map_length: usize,
    ) -> Result<usize, HashJoinV2Error> {
        assert_eq!(
            chunks_per_worker.len(),
            self.build_workers.len(),
            "one chunk share per build worker"
        );
        for worker in &mut self.build_workers {
            worker.create_builder(&self.ctx, null_map_length);
        }
        // Go's `rowTableBuilder.appendToRowTable` charges each segment to
        // `hashTableContext.memoryTracker` as it is filled. The ported builder
        // accumulates the same total in `BuildContext::consumed_memory`
        // instead, so the charge is forwarded here, once per split stage.
        let consumed_before = build_context.consumed_memory;
        for (worker, chunks) in self.build_workers.iter_mut().zip(chunks_per_worker) {
            worker.split_partition_and_append_to_row_table(
                chunks,
                build_context,
                &mut self.hash_table_context,
            )?;
        }
        self.hash_table_context
            .memory_tracker
            .consume(build_context.consumed_memory - consumed_before);

        let total_segment_cnt = self
            .hash_table_context
            .merge_row_tables_to_hash_table(self.ctx.partition_number);

        let tasks = self.create_tasks(total_segment_cnt);
        BuildWorkerV2::build_hash_table(&tasks, &mut self.hash_table_context);
        Ok(total_segment_cnt)
    }

    /// Go `(*ProbeWorkerV2).runJoinWorker` plus `processOneProbeChunk` and
    /// `probeAndSendResult`, for one probe worker over its chunk share.
    ///
    /// `new_result_chunk` stands in for Go's `joinChkResourceCh`: Go recycles
    /// output chunks through a channel, and takes a fresh one whenever the
    /// current one fills. Result chunks are returned in production order,
    /// which is Go's per-worker order; the *cross*-worker interleaving Go's
    /// `joinResultCh` produces is exactly the ordering hash join v2 does not
    /// promise.
    ///
    /// # Errors
    ///
    /// Propagates what Go stores in `joinResult.err`.
    pub fn run_join_worker(
        probe: &mut dyn ProbeV2,
        chunks: Vec<Chunk>,
        new_result_chunk: &dyn Fn() -> Chunk,
    ) -> Result<Vec<Chunk>, HashJoinV2Error> {
        let mut results = Vec::new();
        let mut joined = new_result_chunk();
        for chunk in chunks {
            probe.set_chunk_for_probe(chunk)?;
            while !probe.is_current_chunk_probe_done() {
                probe.probe(&mut joined)?;
                if joined.is_full() {
                    results.push(std::mem::replace(&mut joined, new_result_chunk()));
                }
            }
        }
        if joined.num_rows() > 0 {
            results.push(joined);
        }
        Ok(results)
    }

    /// Go `(*ProbeWorkerV2).scanRowTableAfterProbeDone`.
    ///
    /// Go runs this on `Concurrency` workers after every probe worker has
    /// finished, each scanning the slice of the row table
    /// `commonInitForScanRowTable` gave it. That barrier is what makes the
    /// used flags final, and it is preserved here by the call order.
    ///
    /// # Errors
    ///
    /// Propagates what Go stores in `joinResult.err`.
    pub fn scan_row_table_after_probe_done(
        probe: &mut dyn ProbeV2,
        new_result_chunk: &dyn Fn() -> Chunk,
    ) -> Result<Vec<Chunk>, HashJoinV2Error> {
        probe.init_for_scan_row_table();
        let mut results = Vec::new();
        let mut joined = new_result_chunk();
        while !probe.is_scan_row_table_done() {
            probe.scan_row_table(&mut joined)?;
            if joined.is_full() {
                results.push(std::mem::replace(&mut joined, new_result_chunk()));
            }
        }
        if joined.num_rows() > 0 {
            results.push(joined);
        }
        Ok(results)
    }

    /// Go `resetProbeStatus`.
    pub fn reset_probe_status(probes: &mut [&mut dyn ProbeV2]) {
        for probe in probes {
            probe.reset_probe();
        }
    }
}
