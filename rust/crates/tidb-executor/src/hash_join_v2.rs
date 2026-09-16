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

//! Go hash-join V2 row-table build stages and probe state machines.
//!
//! Build workers consume native chunks incrementally. After the build barrier,
//! row tables are merged and linked before probing starts. Each `ProbeV2` fills
//! a caller-owned result chunk; input/output collection belongs only to tests.
//!
//! Probe workers provide bounded transport, input/output reuse, statement
//! cancellation and an explicit post-probe scan barrier. Native build admission
//! and parallel linking consume an opened child. The owning Executor facade
//! opens/closes children and shares the built table with those workers. SQL
//! builder selection, full probe accounting and spill restoration remain open.

use std::sync::Arc;

pub mod build_worker;
pub mod executor;
pub mod probe_stage;
pub mod probe_worker;
mod semi_probe;
pub mod spill;
pub use semi_probe::{
    AntiLeftOuterSemiJoinProbe, AntiSemiJoinProbe, LeftOuterSemiJoinProbe, SemiJoinProbe,
};

use crate::joiner::JoinType;
use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_util::copy_selected_rows_with_row_id_func;
use tidb_codec::JoinKeyColumns;
use tidb_util::memory::Tracker;
use tidb_util::sqlkiller::SqlKiller;

use crate::base_join_probe::{
    common_init_for_scan_row_table, is_key_matched, is_key_matched_mode, new_join_probe,
    BaseJoinProbe, ProbeContext, ProbeError, ProbeFilter,
};
use crate::hash_table_v2::{
    get_hash_table_length_by_row_table, get_hash_table_memory_usage, HashTableV2, RowIter, SubTable,
};
use crate::join_row_table::{RowTable, RowTableSegment};
use crate::row_table_builder::{BuildContext, RowTableBuildError, RowTableBuilder};
use crate::tagged_ptr::TagPtrHelper;

fn check_probe_killed(killer: &SqlKiller) -> Result<(), ProbeError> {
    killer.handle_signal().map_or(Ok(()), |error| {
        Err(ProbeError::Killed(error.to_sql_error()))
    })
}

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

/// Errors of the incremental build/probe component API. Native worker queues
/// convert them into typed SQL executor errors, as Go's result channels do.
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
            hash_table: HashTableV2::new_empty(partition_number),
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
        segment: RowTableSegment,
    ) {
        append_row_segment(&mut self.row_tables[worker_id][partition_id], segment);
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
    /// [`Self::merge_row_tables_with_spill`] supplies the spill-aware
    /// preallocation path used by the native build coordinator.
    pub fn merge_row_tables_to_hash_table(&mut self, partition_number: usize) -> usize {
        self.merge_row_tables_alloc(partition_number, true)
    }

    /// Go tryToSpill: account bucket arrays before allocating them, then spill
    /// row partitions and their projected buckets together. Rust computes the
    /// merged valid-key counts without copying/moving segments before the IO
    /// barrier, so each worker can still hand its own segments to its disk lane.
    pub fn merge_row_tables_with_spill(
        &mut self,
        partition_number: usize,
        spill: &mut spill::HashJoinSpill,
    ) -> Result<usize, crate::ExecError> {
        let hash_bytes: Vec<_> = (0..partition_number)
            .map(|id| {
                let count = self
                    .row_tables
                    .iter()
                    .filter_map(|tables| tables[id].as_ref())
                    .map(RowTable::valid_key_count)
                    .sum();
                get_hash_table_memory_usage(crate::hash_table_v2::get_hash_table_length_by_row_len(
                    count,
                ))
            })
            .collect();
        self.memory_tracker.consume(hash_bytes.iter().sum());
        if spill.action.is_spill_needed() {
            let mut tables: Vec<_> = self.row_tables.iter_mut().map(Vec::as_mut_slice).collect();
            spill.spill_rows(&mut tables, Some(&hash_bytes), false)?;
            let empty_buckets =
                get_hash_table_memory_usage(crate::hash_table_v2::MINIMAL_HASH_TABLE_LEN);
            self.memory_tracker.consume(
                empty_buckets
                    * spill
                        .spilled_partitions()
                        .iter()
                        .filter(|&&spilled| spilled)
                        .count() as i64,
            );
        }
        spill.action.set_can_spill(false);
        Ok(self.merge_row_tables_alloc(partition_number, false))
    }

    fn merge_row_tables_alloc(&mut self, partition_number: usize, charge_buckets: bool) -> usize {
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

        if charge_buckets {
            let (total_memory_usage, _per_partition) =
                Self::calculate_hash_table_memory_usage(&row_tables);
            self.memory_tracker.consume(total_memory_usage);
        }

        for (part_idx, table) in row_tables.into_iter().enumerate() {
            self.hash_table.tables[part_idx] = Some(SubTable::new(table));
        }

        let tagged_bits = self.hash_table.bind_row_addresses();
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

fn append_row_segment(table: &mut Option<RowTable>, mut segment: RowTableSegment) {
    if segment.hash_values.is_empty() {
        return;
    }
    segment.init_tagged_bits();
    table
        .get_or_insert_with(RowTable::new)
        .segments
        .push(segment);
}

/// Go `BuildWorkerV2`, narrowed to its build-side state.
///
/// Child fetching stays with the coordinator; restored file reads run in each
/// build worker using the same bounded admission and spill barrier.
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

    /// Go `processOneChunk`: pack one build chunk into this worker's partition
    /// slots. Disjoint mutable slices allow parallel workers without a global lock.
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
        chunk: &Chunk,
        build_context: &mut BuildContext<'_>,
        row_tables: &mut [Option<RowTable>],
    ) -> Result<(), RowTableBuildError> {
        let builder = self.builder.as_mut().expect("builder created before use");
        let segments = builder.process_one_chunk(chunk, build_context)?;
        for (partition_id, segment) in segments.into_iter().enumerate() {
            append_row_segment(&mut row_tables[partition_id], segment);
        }
        Ok(())
    }

    /// Append restored segments without recomputing SQL expressions or keys.
    pub fn process_one_restored_chunk(
        &mut self,
        chunk: &Chunk,
        context: &mut BuildContext<'_>,
        row_tables: &mut [Option<RowTable>],
    ) -> Result<(), RowTableBuildError> {
        let segments = self
            .builder
            .as_mut()
            .expect("builder created before restore")
            .process_one_restored_chunk(chunk, context)?;
        for (partition, segment) in segments.into_iter().enumerate() {
            append_row_segment(&mut row_tables[partition], segment);
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

/// Go `join_probe.go`: normal/restored chunk preparation, probing, spill flush,
/// and the post-probe scan. Implementations retain worker-local scratch.
pub trait ProbeV2 {
    /// Go `SetChunkForProbe`.
    ///
    /// # Errors
    ///
    /// When the previous chunk is unfinished, or a filter/serializer fails.
    fn set_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError>;

    /// Restore saved hashes, serialized keys and probe columns without reevaluation.
    fn set_restored_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError>;

    /// Flush each worker's partial partition chunks before completing the probe stage.
    fn spill_remaining_probe_chunks(&mut self) -> Result<(), ProbeError>;

    /// Go `IsCurrentChunkProbeDone`.
    fn is_current_chunk_probe_done(&self) -> bool;

    /// Returns the consumed input for reuse after probing completes.
    fn take_probe_chunk(&mut self) -> Option<Chunk>;

    /// Go `Probe`: append as many output rows as fit into `joined_chk`.
    ///
    /// # Errors
    ///
    /// Propagates the other-condition evaluation failure Go returns in
    /// `joinResult.err`.
    fn probe(&mut self, joined_chk: &mut Chunk, killer: &SqlKiller) -> Result<(), ProbeError>;

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
    fn scan_row_table(
        &mut self,
        joined_chk: &mut Chunk,
        killer: &SqlKiller,
    ) -> Result<(), ProbeError>;

    /// Go `ResetProbe`.
    fn reset_probe(&mut self);

    /// Go `ResetProbeCollision`.
    fn reset_probe_collision(&mut self);

    /// Go `GetProbeCollision`.
    fn get_probe_collision(&self) -> u64;
}

/// Go outerJoinProbe: batched matched rows, persistent unmatched state,
/// and a post-probe row-table scan when the preserved side is built.
pub struct OuterJoinProbe<'a> {
    base: BaseJoinProbe,
    ctx: ProbeContext<'a>,
    key_serializer: JoinKeyColumns,
    filter: Option<ProbeFilter<'a>>,
    other_condition: Option<JoinOtherCondition<'a>>,
    outer_side_build: bool,
    is_not_matched: Vec<bool>,
    row_iter: Option<RowIter<'a>>,
}

impl<'a> OuterJoinProbe<'a> {
    /// Constructs a left/right outer probe with either build orientation.
    #[must_use]
    pub fn new(
        ctx: ProbeContext<'a>,
        work_id: usize,
        join_type: JoinType,
        key_index: Vec<usize>,
        probe_key_nullable: &[bool],
        right_as_build_side: bool,
        key_serializer: JoinKeyColumns,
        filter: Option<ProbeFilter<'a>>,
        other_condition: Option<JoinOtherCondition<'a>>,
    ) -> Self {
        assert!(matches!(
            join_type,
            JoinType::LeftOuter | JoinType::RightOuter
        ));
        assert_eq!(
            ctx.has_other_condition,
            other_condition.is_some(),
            "outer join residual context and evaluator must agree"
        );
        let outer_side_build = matches!(
            (join_type, right_as_build_side),
            (JoinType::LeftOuter, false) | (JoinType::RightOuter, true)
        );
        Self {
            base: new_join_probe(
                &ctx,
                work_id,
                join_type,
                key_index,
                probe_key_nullable,
                right_as_build_side,
            ),
            ctx,
            key_serializer,
            filter,
            other_condition,
            outer_side_build,
            is_not_matched: Vec::new(),
            row_iter: None,
        }
    }

    fn append_unmatched_probe_rows(&self, output: &mut Chunk, start: usize) {
        let end = self.base.current_probe_row();
        let null_rows = self.is_not_matched[start..end]
            .iter()
            .filter(|&&unmatched| unmatched)
            .count();
        let before = output.num_rows();
        let (probe_used, probe_offset, build_used, build_offset) = if self.ctx.right_as_build_side {
            (&self.ctx.l_used, 0, &self.ctx.r_used, self.ctx.l_used.len())
        } else {
            (&self.ctx.r_used, self.ctx.l_used.len(), &self.ctx.l_used, 0)
        };
        let input = self.base.current_chunk().expect("probe chunk is set");
        for (index, &column) in probe_used.iter().enumerate() {
            let source = input.column(column);
            let mut destination = output.column_mut(probe_offset + index);
            copy_selected_rows_with_row_id_func(
                &mut destination,
                &source,
                &self.is_not_matched,
                start,
                end,
                |row| self.base.used_rows()[row],
            );
        }
        for index in 0..build_used.len() {
            output
                .column_mut(build_offset + index)
                .append_n_nulls(null_rows);
        }
        output.set_num_virtual_rows(before + null_rows);
    }
}

impl ProbeV2 for OuterJoinProbe<'_> {
    fn take_probe_chunk(&mut self) -> Option<Chunk> {
        self.base.take_probe_chunk()
    }

    fn set_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError> {
        self.base
            .set_chunk_for_probe(&self.ctx, chunk, self.filter, &self.key_serializer)?;
        if !self.outer_side_build {
            self.is_not_matched.resize(self.base.chunk_rows(), true);
            self.is_not_matched.fill(true);
            for &row in self.base.spilled_indices() {
                self.is_not_matched[row] = false;
            }
        }
        Ok(())
    }

    fn set_restored_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError> {
        self.base.set_restored_chunk_for_probe(&self.ctx, chunk)?;
        if !self.outer_side_build {
            self.is_not_matched.resize(self.base.chunk_rows(), true);
            self.is_not_matched.fill(true);
            for &row in self.base.spilled_indices() {
                self.is_not_matched[row] = false;
            }
        }
        Ok(())
    }

    fn spill_remaining_probe_chunks(&mut self) -> Result<(), ProbeError> {
        self.base.spill_remaining_probe_chunks(&self.ctx)
    }

    fn is_current_chunk_probe_done(&self) -> bool {
        self.base.is_current_chunk_probe_done()
    }

    fn probe(&mut self, output: &mut Chunk, killer: &SqlKiller) -> Result<(), ProbeError> {
        if output.is_full() {
            return Ok(());
        }
        let (_, remain_cap) = self.base.prepare_for_probe(&self.ctx, output);
        let start = self.base.current_probe_row();
        let outer_side_build = self.outer_side_build;
        if let Some(condition) = &mut self.other_condition {
            condition.chunk.reset();
            if outer_side_build {
                collect_outer_join_candidates::<true, true>(
                    &mut self.base,
                    &self.ctx,
                    &mut self.is_not_matched,
                    &mut condition.chunk,
                    remain_cap,
                    killer,
                )?;
            } else {
                collect_outer_join_candidates::<false, true>(
                    &mut self.base,
                    &self.ctx,
                    &mut self.is_not_matched,
                    &mut condition.chunk,
                    remain_cap,
                    killer,
                )?;
            }
            if condition.chunk.num_rows() > 0 {
                let selected = std::mem::take(self.base.selected_mut());
                *self.base.selected_mut() =
                    (condition.evaluate)(&condition.chunk, selected, Vec::new(), false)?.0;
                for index in 0..self.base.row_index_infos().len() {
                    if self.base.selected_mut()[index] {
                        let info = self.base.row_index_infos()[index];
                        if self.outer_side_build {
                            if let Some(location) = self.base.row_index_locations()[index] {
                                self.ctx
                                    .hash_table
                                    .mark_build_row_matched_at_location(location);
                            } else {
                                self.ctx
                                    .hash_table
                                    .mark_build_row_matched(info.build_row_start);
                            }
                        } else {
                            self.is_not_matched[info.probe_row_index] = false;
                        }
                    }
                }
                self.base.build_result_after_other_condition(
                    &self.ctx,
                    self.ctx.hash_table,
                    output,
                    &condition.chunk,
                );
            }
        } else if outer_side_build {
            collect_outer_join_candidates::<true, false>(
                &mut self.base,
                &self.ctx,
                &mut self.is_not_matched,
                output,
                remain_cap,
                killer,
            )?;
        } else {
            collect_outer_join_candidates::<false, false>(
                &mut self.base,
                &self.ctx,
                &mut self.is_not_matched,
                output,
                remain_cap,
                killer,
            )?;
        }
        if !outer_side_build {
            self.append_unmatched_probe_rows(output, start);
        }
        Ok(())
    }

    fn need_scan_row_table(&self) -> bool {
        self.outer_side_build
    }

    fn init_for_scan_row_table(&mut self) {
        assert!(
            self.outer_side_build,
            "probe-preserved outer join does not scan the build row table"
        );
        self.row_iter = Some(common_init_for_scan_row_table(
            self.ctx.hash_table,
            self.base.work_id(),
            self.ctx.concurrency,
        ));
    }

    fn is_scan_row_table_done(&self) -> bool {
        self.row_iter
            .as_ref()
            .expect("scan row table before init")
            .is_end()
    }

    fn scan_row_table(&mut self, output: &mut Chunk, killer: &SqlKiller) -> Result<(), ProbeError> {
        assert!(
            self.outer_side_build,
            "probe-preserved outer join does not scan the build row table"
        );
        if output.is_full() {
            return Ok(());
        }
        let (_, remain_cap) = self.base.prepare_for_probe(&self.ctx, output);
        let before = output.num_rows();
        let mut inserted = 0;
        let iter = self.row_iter.as_mut().expect("scan row table before init");
        while inserted < remain_cap && !iter.is_end() {
            let location = iter.current_row_location();
            if !self
                .ctx
                .hash_table
                .is_build_row_matched_at_location(location)
            {
                self.base
                    .append_build_row_to_cached_build_rows_v1_with_location(
                        &self.ctx,
                        self.ctx.hash_table,
                        0,
                        0,
                        Some(location),
                        output,
                        0,
                        false,
                    );
                inserted += 1;
            }
            iter.next();
        }
        check_probe_killed(killer)?;
        if self.base.next_cached_build_row_index() > 0 {
            self.base
                .batch_construct_build_rows(&self.ctx, self.ctx.hash_table, output, 0, false);
        }
        let (probe_used, offset) = if self.ctx.right_as_build_side {
            (&self.ctx.l_used, 0)
        } else {
            (&self.ctx.r_used, self.ctx.l_used.len())
        };
        for index in 0..probe_used.len() {
            output.column_mut(offset + index).append_n_nulls(inserted);
        }
        output.set_num_virtual_rows(before + inserted);
        Ok(())
    }

    fn reset_probe(&mut self) {
        self.row_iter = None;
        self.base.reset_probe(&self.ctx);
    }
    fn reset_probe_collision(&mut self) {
        self.base.reset_probe_collision();
    }
    fn get_probe_collision(&self) -> u64 {
        self.base.get_probe_collision()
    }
}

// Go budgets every lookup/advance on a preserved probe side, but only matches
// when the preserved side is built. That leaves room for deferred NULL rows.
// Const-specialize the two Go probe methods, their residual/no-residual
// callers, and the table's physical key representation so those mode checks do
// not run for every chain candidate.
#[inline(always)]
fn collect_outer_join_candidates<const OUTER_SIDE_BUILD: bool, const RESIDUAL: bool>(
    base: &mut BaseJoinProbe,
    ctx: &ProbeContext<'_>,
    is_not_matched: &mut [bool],
    output: &mut Chunk,
    remain_cap: usize,
    killer: &SqlKiller,
) -> Result<(), ProbeError> {
    match ctx.meta.key_mode {
        crate::join_table_meta::KeyMode::OneInt64 => {
            collect_outer_join_candidates_mode::<OUTER_SIDE_BUILD, RESIDUAL, true, false>(
                base,
                ctx,
                is_not_matched,
                output,
                remain_cap,
                killer,
            )
        }
        crate::join_table_meta::KeyMode::FixedSerializedKey => {
            collect_outer_join_candidates_mode::<OUTER_SIDE_BUILD, RESIDUAL, false, false>(
                base,
                ctx,
                is_not_matched,
                output,
                remain_cap,
                killer,
            )
        }
        crate::join_table_meta::KeyMode::VariableSerializedKey => {
            collect_outer_join_candidates_mode::<OUTER_SIDE_BUILD, RESIDUAL, false, true>(
                base,
                ctx,
                is_not_matched,
                output,
                remain_cap,
                killer,
            )
        }
    }
}

#[inline(always)]
fn collect_outer_join_candidates_mode<
    const OUTER_SIDE_BUILD: bool,
    const RESIDUAL: bool,
    const INTEGER_KEY: bool,
    const VARIABLE_KEY: bool,
>(
    base: &mut BaseJoinProbe,
    ctx: &ProbeContext<'_>,
    is_not_matched: &mut [bool],
    output: &mut Chunk,
    mut remain_cap: usize,
    killer: &SqlKiller,
) -> Result<(), ProbeError> {
    let was_incomplete = output.is_incomplete_chunk();
    output.set_num_virtual_rows(output.num_rows());
    output.set_incomplete_chunk(true);
    while remain_cap > 0 && !base.is_current_chunk_probe_done() {
        let probe_row = base.current_probe_row();
        let header = base.matched_rows_headers()[probe_row];
        if header == 0 {
            base.finish_lookup_current_probe_row();
            base.set_current_probe_row(probe_row + 1);
        } else {
            let hash = base.matched_rows_hash_value()[probe_row];
            let partition =
                crate::row_table_builder::generate_partition_index(hash, ctx.partition_mask_offset)
                    as usize;
            let table = ctx.hash_table.sub_table(partition);
            let address = crate::hash_table_v2::row_address_of(&ctx.tag_helper, header);
            let (build_row, next, location) = ctx.hash_table.row_bytes_and_next_in_sub_table(
                table,
                partition,
                address,
                &ctx.tag_helper,
                hash,
            );
            if is_key_matched_mode::<INTEGER_KEY, VARIABLE_KEY>(
                &base.serialized_keys()[probe_row],
                build_row,
                ctx.meta,
            ) {
                base.append_build_row_to_cached_build_rows_v1_with_location(
                    ctx,
                    ctx.hash_table,
                    probe_row,
                    address,
                    Some(location),
                    output,
                    0,
                    RESIDUAL,
                );
                if !RESIDUAL {
                    if OUTER_SIDE_BUILD {
                        ctx.hash_table.mark_build_row_matched_at_location(location);
                    } else {
                        is_not_matched[probe_row] = false;
                    }
                }
                base.record_matched_row_for_current_probe_row();
                if OUTER_SIDE_BUILD {
                    remain_cap -= 1;
                }
            } else {
                base.record_probe_collision();
            }
            base.set_matched_rows_header(probe_row, next);
        }
        if !OUTER_SIDE_BUILD {
            remain_cap -= 1;
        }
    }
    let result = check_probe_killed(killer);
    if result.is_ok() {
        base.finish_current_lookup_loop(ctx, ctx.hash_table, output);
    }
    output.set_incomplete_chunk(was_incomplete);
    result
}

/// Go's partial joined chunk and vectorized residual evaluator.
/// The scratch schema keeps original left columns followed by right columns.
pub struct JoinOtherCondition<'a> {
    chunk: Chunk,
    evaluate: Box<
        dyn Fn(&Chunk, Vec<bool>, Vec<bool>, bool) -> Result<(Vec<bool>, Vec<bool>), ProbeError>
            + 'a,
    >,
}

impl<'a> JoinOtherCondition<'a> {
    /// Binds the statement expression context once per probe worker.
    pub fn new<C: tidb_expr::Columns + 'a>(
        context: C,
        predicates: Vec<tidb_expr::expression::Expression>,
        joined_types: &[tidb_datatype::FieldType],
        max_chunk_size: usize,
        vectorized: bool,
    ) -> Self {
        let mut chunk = Chunk::new(
            joined_types,
            tidb_chunk::chunk::INITIAL_CAPACITY,
            max_chunk_size,
        );
        chunk.set_incomplete_chunk(true);
        Self {
            chunk,
            evaluate: Box::new(move |chunk, selected, nulls, consider_null| {
                if consider_null {
                    tidb_expr::evaluator::vec_eval_bool(
                        &context,
                        vectorized,
                        &predicates,
                        chunk,
                        selected,
                        nulls,
                    )
                } else {
                    tidb_expr::evaluator::vectorized_filter_consider_null(
                        &context,
                        vectorized,
                        &predicates,
                        chunk,
                        selected,
                        nulls,
                    )
                }
                .map_err(ProbeError::Expression)
            }),
        }
    }

    /// Retained scratch-column storage for the owning worker's memory tracker.
    pub fn memory_usage(&self) -> i64 {
        self.chunk.memory_usage()
    }
}

/// V2 inner join: equality candidates, batch residual evaluation, late output reconstruction.
pub struct InnerJoinProbe<'a> {
    base: BaseJoinProbe,
    ctx: ProbeContext<'a>,
    key_serializer: JoinKeyColumns,
    filter: Option<ProbeFilter<'a>>,
    other_condition: Option<JoinOtherCondition<'a>>,
}

impl<'a> InnerJoinProbe<'a> {
    /// Constructs a probe for one worker of an inner v2 join.
    #[must_use]
    pub fn new(
        ctx: ProbeContext<'a>,
        work_id: usize,
        key_index: Vec<usize>,
        probe_key_nullable: &[bool],
        right_as_build_side: bool,
        key_serializer: JoinKeyColumns,
        filter: Option<ProbeFilter<'a>>,
        other_condition: Option<JoinOtherCondition<'a>>,
    ) -> Self {
        assert_eq!(
            ctx.has_other_condition,
            other_condition.is_some(),
            "inner join residual context and evaluator must agree"
        );
        let base = new_join_probe(
            &ctx,
            work_id,
            JoinType::Inner,
            key_index,
            probe_key_nullable,
            right_as_build_side,
        );
        Self {
            base,
            ctx,
            key_serializer,
            filter,
            other_condition,
        }
    }

    /// Provides read-only access to the shared base for probe diagnostics.
    #[must_use]
    pub const fn base(&self) -> &BaseJoinProbe {
        &self.base
    }
}

impl ProbeV2 for InnerJoinProbe<'_> {
    fn set_restored_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError> {
        self.base.set_restored_chunk_for_probe(&self.ctx, chunk)
    }

    fn spill_remaining_probe_chunks(&mut self) -> Result<(), ProbeError> {
        self.base.spill_remaining_probe_chunks(&self.ctx)
    }

    fn take_probe_chunk(&mut self) -> Option<Chunk> {
        self.base.take_probe_chunk()
    }

    fn set_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError> {
        self.base
            .set_chunk_for_probe(&self.ctx, chunk, self.filter, &self.key_serializer)
    }

    fn is_current_chunk_probe_done(&self) -> bool {
        self.base.is_current_chunk_probe_done()
    }

    fn probe(&mut self, joined_chk: &mut Chunk, killer: &SqlKiller) -> Result<(), ProbeError> {
        if joined_chk.is_full() {
            return Ok(());
        }
        let (_, remain_cap) = self.base.prepare_for_probe(&self.ctx, joined_chk);
        if let Some(condition) = &mut self.other_condition {
            condition.chunk.reset();
            collect_inner_join_candidates(
                &mut self.base,
                &self.ctx,
                &mut condition.chunk,
                remain_cap,
                true,
                killer,
            )?;
            if condition.chunk.num_rows() > 0 {
                let selected = std::mem::take(self.base.selected_mut());
                *self.base.selected_mut() =
                    (condition.evaluate)(&condition.chunk, selected, Vec::new(), false)?.0;
                self.base.build_result_after_other_condition(
                    &self.ctx,
                    self.ctx.hash_table,
                    joined_chk,
                    &condition.chunk,
                );
            }
        } else {
            collect_inner_join_candidates(
                &mut self.base,
                &self.ctx,
                joined_chk,
                remain_cap,
                false,
                killer,
            )?;
        }
        Ok(())
    }

    fn need_scan_row_table(&self) -> bool {
        false
    }

    fn init_for_scan_row_table(&mut self) {
        panic!("inner join does not scan the build row table")
    }

    fn is_scan_row_table_done(&self) -> bool {
        panic!("inner join does not scan the build row table")
    }

    fn scan_row_table(
        &mut self,
        _joined_chk: &mut Chunk,
        _killer: &SqlKiller,
    ) -> Result<(), ProbeError> {
        panic!("inner join does not scan the build row table")
    }

    fn reset_probe(&mut self) {
        self.base.reset_probe(&self.ctx);
    }

    fn reset_probe_collision(&mut self) {
        self.base.reset_probe_collision();
    }

    fn get_probe_collision(&self) -> u64 {
        self.base.get_probe_collision()
    }
}

// ---------------------------------------------------------------------------
// HashJoinV2Exec (`hash_join_v2.go:608`)
// ---------------------------------------------------------------------------

/// V2 build state shared with the probe workers after its build barrier.
/// The owning Executor facade supplies its lifecycle and recursive restore;
/// SQL builder selection remains separate integration work.
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

    /// Initializes the per-worker builders before accepting the first chunk.
    pub fn begin_build(&mut self, null_map_length: usize) {
        for worker in &mut self.build_workers {
            worker.create_builder(&self.ctx, null_map_length);
        }
    }

    /// Consumes one worker's input chunk without retaining the input.
    /// Charge its row-table allocation before the fetcher admits another chunk.
    pub fn append_build_chunk(
        &mut self,
        worker_id: usize,
        chunk: &Chunk,
        build_context: &mut BuildContext<'_>,
    ) -> Result<(), HashJoinV2Error> {
        let consumed_before = build_context.consumed_memory;
        let result = self.build_workers[worker_id].process_one_chunk(
            chunk,
            build_context,
            &mut self.hash_table_context.row_tables[worker_id],
        );
        if build_context.memory_tracker.is_none() {
            self.hash_table_context
                .memory_tracker
                .consume(build_context.consumed_memory - consumed_before);
        }
        result.map_err(HashJoinV2Error::from)
    }

    /// Runs the post-build merge/link stages after every input worker finishes.
    /// Returns Go's total segment count; no probe may start before this barrier.
    pub fn finish_build(&mut self) -> usize {
        let total_segment_cnt = self
            .hash_table_context
            .merge_row_tables_to_hash_table(self.ctx.partition_number);
        let tasks = self.create_tasks(total_segment_cnt);
        BuildWorkerV2::build_hash_table(&tasks, &mut self.hash_table_context);
        total_segment_cnt
    }

    /// Go `resetProbeStatus`.
    pub fn reset_probe_status(probes: &mut [&mut dyn ProbeV2]) {
        for probe in probes {
            probe.reset_probe();
        }
    }
}

fn collect_inner_join_candidates(
    base: &mut BaseJoinProbe,
    ctx: &ProbeContext<'_>,
    joined_chk: &mut Chunk,
    mut remain_cap: usize,
    for_other_condition: bool,
    killer: &SqlKiller,
) -> Result<(), ProbeError> {
    let was_incomplete = joined_chk.is_incomplete_chunk();
    joined_chk.set_num_virtual_rows(joined_chk.num_rows());
    joined_chk.set_incomplete_chunk(true);

    while remain_cap > 0 && !base.is_current_chunk_probe_done() {
        remain_cap -= base.collect_inner_candidate_batch(ctx, remain_cap);
        if base.next_cached_build_row_index() == crate::base_join_probe::BATCH_BUILD_ROW_SIZE {
            base.batch_construct_build_rows(
                ctx,
                ctx.hash_table,
                joined_chk,
                0,
                for_other_condition,
            );
        }
    }

    let result = check_probe_killed(killer);
    if result.is_ok() {
        base.finish_current_lookup_loop(ctx, ctx.hash_table, joined_chk);
    }
    joined_chk.set_incomplete_chunk(was_incomplete);
    result
}
