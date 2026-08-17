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

//! `pkg/executor/join/base_join_probe.go`: the probe-side half of hash join
//! v2, shared by every join type.
//!
//! This is the *base*. Go splits the probe into a common `baseJoinProbe` that
//! prepares a probe chunk (filter, key serialization, hashing, partitioning,
//! bucket lookup) and reconstructs output rows from packed build-row bytes,
//! plus one small per-join-type probe (`innerJoinProbe`,
//! `outerJoinProbe`, `semiJoinProbe`, `antiSemiJoinProbe`,
//! `leftOuterSemiJoinProbe`) that decides which matches become rows. Only the
//! base lives here; the per-type probes live in their own Go files and are not
//! part of this port. [`new_join_probe`] therefore validates and records the
//! join type exactly as Go's `NewJoinProbe` does, and stops at the dispatch.
//!
//! ## What is reused rather than restated
//!
//! * [`crate::join_row_table`]: row layout, null map, key bytes, chain links
//!   ([`RowLayoutMeta`], [`next_row_address`]).
//! * [`crate::hash_table_v2`]: sub-tables, bucket lookup, row iteration
//!   ([`HashTableV2`], [`RowIter`], [`row_address_of`]).
//! * [`crate::join_table_meta`]: [`KeyMode`] -- Go declares `keyMode` twice,
//!   once in `join_table_meta.go` and again at the top of this file with the
//!   same three constants; one Rust enum serves both.
//! * [`crate::row_table_builder`]: [`fnv64`] (Go `hash/fnv`),
//!   [`generate_partition_index`], [`FAKE_SEL_LENGTH`].
//! * [`crate::tagged_ptr`]'s [`TagPtrHelper`].
//! * `tidb_chunk`: the real `chunk.Chunk`/`chunk.Column`, including
//!   `AppendCellFromRawData`, `AppendCellNTimes`, the `Reserve` pre-sizing
//!   deltas, and `CopySelectedRows*`.
//! * `tidb_executor::joiner::JoinType`: Go `plannerbase.JoinType`. The
//!   per-join-type match/miss semantics live there and are not re-derived.
//!
//! ## Sequential here, worker-parallel there
//!
//! Go runs `Concurrency` probe workers, each owning its own `baseJoinProbe`
//! (`workID`), all reading one shared, already-built hash table. Nothing in
//! this file is shared mutable state between workers: `matchedRowsHeaders`,
//! `serializedKeys`, `hashValues`, `cachedBuildRows`, `offsetAndLengthArray`
//! and the scratch chunk are all per-worker. The only cross-worker
//! interaction is the used-flag bit in the build row's null map, which the
//! outer/semi probes set with an atomic OR -- and that is in *those* files,
//! not this one.
//!
//! So this port is a single [`BaseJoinProbe`] driven sequentially, and that
//! changes nothing observable about which rows are produced:
//!
//! * Row content is identical -- every reconstruction here reads immutable
//!   build bytes and the worker's own probe chunk.
//! * Row *order* is identical **within one probe chunk**, which is all Go
//!   promises: hash join v2 is unordered across chunks because chunks are
//!   handed to whichever worker is free, and the result chunks are merged in
//!   completion order. A sequential driver simply picks one of the orderings
//!   Go already permits.
//! * [`common_init_for_scan_row_table`] is ported with its `work_id` /
//!   `concurrency` arguments intact, because the *partition* of the row table
//!   across workers is observable in each worker's output even though the
//!   union is not. Driving it with `concurrency == 1` scans the whole table,
//!   which is the sequential equivalent.
//!
//! What is *not* reproduced: the `SQLKiller` cancellation checkpoints
//! (`checkSQLKiller`) that Go interleaves into the probe loop, and the
//! spill-to-disk path, both of which only exist because probing is a
//! long-running concurrent activity. Both are narrowed below.
//!
//! ## Narrowings (every one named)
//!
//! * **Spill.** `SetRestoredChunkForProbe`, `preAllocForSetRestoredChunkForProbe`
//!   and `SpillRemainingProbeChunks` are `todo!()`; `SetChunkForProbe` omits
//!   the `spillHelper` branches. Blocking symbol:
//!   `hashJoinSpillHelper` (`pkg/executor/join/hash_join_spill.go`), together
//!   with `HashJoinCtxV2.spillHelper`, `spillProbeChk`, `isPartitionSpilled`,
//!   `areAllPartitionsSpilled` and `spillChunkSize`.
//! * **`SQLKiller`.** Go `checkSQLKiller` is not ported. Blocking symbol:
//!   `util/sqlkiller.SQLKiller` plus `failpoint.Inject` and
//!   `exeerrors.ErrQueryInterrupted`.
//! * **`HashJoinCtxV2`.** Not ported (it lives in `hash_join_v2.go`, which is
//!   explicitly out of scope). [`ProbeContext`] carries exactly the fields
//!   this file reads, and nothing else.
//! * **Probe filter and key serialization.** Go calls
//!   `expression.VectorizedFilter` and `codec.SerializeKeys`. Both are seams
//!   here ([`ProbeFilter`], [`ProbeKeySerializer`]), mirroring the seams
//!   [`crate::row_table_builder`] already established for the build side.
//! * **`isReadNullMapThreadSafe`.** Go picks between `isColumnNull` and
//!   `isColumnNullThreadSafe`; the two differ only in whether the null-map
//!   word is read atomically, which is invisible to a single reader. One
//!   implementation ([`RowLayoutMeta::is_column_null`]) serves both arms.
//! * **`intest.InTest` assertions.** Go's in-test capacity checks (that
//!   `Reserve` pre-sized exactly, and that restored serialized-key buffers
//!   never grow) are dropped; they assert about Go slice capacity growth,
//!   which has no Rust counterpart.
//! * **Per-join-type dispatch.** [`new_join_probe`] returns the validated
//!   base. Blocking symbols: `innerJoinProbe`, `newOuterJoinProbe`,
//!   `newSemiJoinProbe`, `newAntiSemiJoinProbe`, `newLeftOuterSemiJoinProbe`.
//! * **`mockJoinProbe`.** Not ported: it is a test-only shell whose every
//!   method is `panic("not supported")`, and it exists to feed
//!   `hash_join_v2_test.go`, which is out of scope.

use std::collections::HashMap;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_util::{copy_selected_rows, copy_selected_rows_with_row_id_func};
use tidb_chunk::column::{append_cell_from_raw_data, Column};
use tidb_executor::joiner::JoinType;

use crate::hash_table_v2::{row_address_of, HashTableV2, RowIter};
use crate::join_row_table::{next_row_address, RowLayoutMeta, SIZE_OF_ELEMENT_SIZE};
use crate::join_table_meta::KeyMode;
use crate::row_table_builder::{fnv64, generate_partition_index, FAKE_SEL_LENGTH};
use crate::tagged_ptr::TagPtrHelper;

/// Go `batchBuildRowSize`: how many matched build rows are reconstructed in
/// one pass over the row layout.
pub const BATCH_BUILD_ROW_SIZE: usize = 32;

/// Go `chunk.InitialCapacity`, the capacity every per-worker scratch buffer
/// starts at.
pub const INITIAL_CAPACITY: usize = 32;

/// Go `offsetAndLength`: a run of `length` output rows all copied from the
/// same physical probe row `offset`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OffsetAndLength {
    /// Physical probe-chunk row index.
    pub offset: usize,
    /// How many times that row is repeated.
    pub length: usize,
}

/// Go `matchedRowInfo`: one probe-row/build-row pairing awaiting
/// reconstruction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MatchedRowInfo {
    /// Logical probe-side row index of this match.
    pub probe_row_index: usize,
    /// Address of the matched build row.
    ///
    /// Go holds a `uintptr` reinterpreted as `unsafe.Pointer`; the ported row
    /// tables hand out synthetic addresses in the same role
    /// ([`crate::join_row_table::allocate_row_address_range`]).
    pub build_row_start: usize,
    /// How far into the build row the next column to reconstruct sits.
    ///
    /// `0` means "not yet advanced past the header", which is what
    /// [`BaseJoinProbe::advance_to_row_data`] keys on, exactly as Go's
    /// `advanceToRowData` does.
    pub build_row_offset: usize,
}

/// Go `posAndHashValue`: a probe row's hash value bucketed by partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PosAndHashValue {
    /// FNV-1 hash of the row's serialized key.
    pub hash_value: u64,
    /// Logical probe-chunk row index.
    pub pos: usize,
}

/// Errors this file's ported entry points return.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProbeError {
    /// Go: `errors.New("Previous chunk is not probed yet")`.
    PreviousChunkNotProbed,
    /// A [`ProbeKeySerializer`] or [`ProbeFilter`] failed.
    Seam(String),
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreviousChunkNotProbed => f.write_str("Previous chunk is not probed yet"),
            Self::Seam(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ProbeError {}

/// Reads the packed bytes of build rows by address.
///
/// Go dereferences `unsafe.Pointer`s straight into the row table's backing
/// array. The ported row tables address rows synthetically, so the probe asks
/// this source instead. It is a trait rather than a concrete
/// [`HashTableV2`] borrow so that reconstruction can be exercised against
/// hand-built rows without standing up a whole build side.
pub trait BuildRowSource {
    /// Bytes of the row starting at `address`, running to the end of its
    /// segment.
    ///
    /// # Panics
    ///
    /// Implementations panic on an unknown address, matching Go's behavior on
    /// a bad pointer.
    fn row_bytes(&self, address: usize) -> &[u8];

    /// The raw, still-tagged word stored in the row's `next_row_ptr`.
    fn raw_next_row_address(&self, address: usize) -> usize;
}

impl BuildRowSource for HashTableV2 {
    fn row_bytes(&self, address: usize) -> &[u8] {
        // Linear over partitions and segments. Go indexes by raw pointer, so
        // it has no equivalent cost; the ranges are disjoint, so the answer is
        // the same either way.
        self.tables
            .iter()
            .flatten()
            .find_map(|sub| sub.row_data.row_bytes_at(address))
            .expect("build row address belongs to some segment")
    }

    fn raw_next_row_address(&self, address: usize) -> usize {
        for sub in self.tables.iter().flatten() {
            if let Some((segment_index, offset)) = sub.row_data.segment_of_address(address) {
                return sub.row_data.segments[segment_index].raw_next_row_address(offset);
            }
        }
        panic!("build row address belongs to some segment")
    }
}

/// A [`BuildRowSource`] over hand-built rows, keyed by address.
#[derive(Clone, Debug, Default)]
pub struct RowBytesMap {
    rows: HashMap<usize, Vec<u8>>,
}

impl RowBytesMap {
    /// An empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one row's bytes at `address`.
    pub fn insert(&mut self, address: usize, bytes: Vec<u8>) {
        self.rows.insert(address, bytes);
    }
}

impl BuildRowSource for RowBytesMap {
    fn row_bytes(&self, address: usize) -> &[u8] {
        self.rows.get(&address).expect("registered row address")
    }

    fn raw_next_row_address(&self, address: usize) -> usize {
        let bytes = self.row_bytes(address);
        let mut raw = [0_u8; 8];
        raw.copy_from_slice(&bytes[..8]);
        u64::from_le_bytes(raw) as usize
    }
}

/// Go `expression.VectorizedFilter` over `HashJoinCtxV2.ProbeFilter`,
/// evaluated per **physical** row -- Go indexes `filterVector` physically
/// because that is what `VectorizedFilter` returns.
pub type ProbeFilter<'a> = &'a dyn Fn(&Chunk, usize) -> bool;

/// Go `codec.SerializeKeys` for the probe side.
///
/// Returns `None` when any key column of the row is NULL, which is how Go's
/// `SerializeKeys` reports through `nullKeyVector`.
pub trait ProbeKeySerializer {
    /// Serializes the join key of one logical probe row.
    fn serialize(&self, chunk: &Chunk, logical_row_index: usize) -> Option<Vec<u8>>;
}

impl<F: Fn(&Chunk, usize) -> Option<Vec<u8>>> ProbeKeySerializer for F {
    fn serialize(&self, chunk: &Chunk, logical_row_index: usize) -> Option<Vec<u8>> {
        self(chunk, logical_row_index)
    }
}

/// The slice of Go `HashJoinCtxV2` that `base_join_probe.go` actually reads.
///
/// `HashJoinCtxV2` itself lives in `hash_join_v2.go` and is out of scope; this
/// is the narrowed boundary, carrying one field per Go access site.
pub struct ProbeContext<'a> {
    /// Go `hashTableContext.hashTable`.
    pub hash_table: &'a HashTableV2,
    /// Go `hashTableMeta`, row-layout half.
    pub meta: &'a RowLayoutMeta,
    /// Go `hashTableMeta.columnCountNeededForOtherCondition`.
    pub column_count_needed_for_other_condition: usize,
    /// Go `hashTableMeta.totalColumnNumber`.
    pub total_column_number: usize,
    /// Tag helper shared by every sub-table.
    pub tag_helper: TagPtrHelper,
    /// Go `partitionNumber`.
    pub partition_number: usize,
    /// Go `partitionMaskOffset`.
    pub partition_mask_offset: usize,
    /// Go `hasOtherCondition()`, i.e. `OtherCondition != nil`.
    pub has_other_condition: bool,
    /// Go `RightAsBuildSide`.
    pub right_as_build_side: bool,
    /// Go `LUsed`: left-child columns the parent uses. Never nil; empty means
    /// no left column is used.
    pub l_used: Vec<usize>,
    /// Go `RUsed`.
    pub r_used: Vec<usize>,
    /// Go `LUsedInOtherCondition`.
    pub l_used_in_other_condition: Vec<usize>,
    /// Go `RUsedInOtherCondition`.
    pub r_used_in_other_condition: Vec<usize>,
    /// Go `Concurrency`, the probe worker count.
    pub concurrency: usize,
    /// Go `SessCtx.GetSessionVars().MaxChunkSize`.
    pub max_chunk_size: usize,
}

impl ProbeContext<'_> {
    /// Go `(*HashJoinCtxV2).hasOtherCondition`.
    #[must_use]
    pub const fn has_other_condition(&self) -> bool {
        self.has_other_condition
    }
}

/// Go `baseJoinProbe`: everything one probe worker keeps across chunks.
pub struct BaseJoinProbe {
    /// Go `workID`.
    work_id: usize,
    /// Go `currentChunk`. Owned here; Go holds a borrowed pointer into the
    /// worker's chunk queue.
    current_chunk: Option<Chunk>,
    /// Go `selRows`, the identity selection built when the chunk has none.
    sel_rows: Vec<usize>,
    /// Go `usedRows`: logical row index -> physical row index.
    used_rows: Vec<usize>,
    /// Go `matchedRowsHeaders`, indexed by logical row.
    matched_rows_headers: Vec<usize>,
    /// Go `matchedRowsHashValue`, indexed by logical row.
    matched_rows_hash_value: Vec<u64>,
    /// Go `serializedKeys`, indexed by logical row.
    serialized_keys: Vec<Vec<u8>>,
    /// Go `filterVector`, indexed by **physical** row.
    filter_vector: Option<Vec<bool>>,
    /// Go `nullKeyVector`, indexed by **physical** row.
    null_key_vector: Option<Vec<bool>>,
    /// Go `hashValues`, one bucket list per partition.
    hash_values: Vec<Vec<PosAndHashValue>>,
    /// Go `currentProbeRow`.
    current_probe_row: usize,
    /// Go `matchedRowsForCurrentProbeRow`.
    matched_rows_for_current_probe_row: usize,
    /// Go `chunkRows`, the logical row count of the current chunk.
    chunk_rows: usize,
    /// Go `cachedBuildRows`, a fixed [`BATCH_BUILD_ROW_SIZE`] staging array.
    cached_build_rows: Vec<MatchedRowInfo>,
    /// Go `nextCachedBuildRowIndex`, how much of it is live.
    next_cached_build_row_index: usize,
    /// Go `keyIndex`.
    key_index: Vec<usize>,
    /// Go `hasNullableKey`.
    has_nullable_key: bool,
    /// Go `maxChunkSize`.
    max_chunk_size: usize,
    /// Go `rightAsBuildSide`.
    right_as_build_side: bool,
    /// Go `offsetAndLengthArray`.
    offset_and_length_array: Vec<OffsetAndLength>,
    /// Go `rowIndexInfos`, only used when the join has an other condition.
    row_index_infos: Vec<MatchedRowInfo>,
    /// Go `selected`, the other-condition verdict per joined row.
    selected: Vec<bool>,
    /// Go `probeCollision`.
    probe_collision: u64,
    /// The join type Go's `NewJoinProbe` dispatched on.
    join_type: JoinType,
}

impl BaseJoinProbe {
    /// Go `GetProbeCollision`.
    #[must_use]
    pub const fn get_probe_collision(&self) -> u64 {
        self.probe_collision
    }

    /// Go `ResetProbeCollision`.
    pub const fn reset_probe_collision(&mut self) {
        self.probe_collision = 0;
    }

    /// Records one hash collision, i.e. a bucket hit whose key did not match.
    ///
    /// Go's per-join-type probes do `j.probeCollision++` inline; the counter
    /// lives on the base, so its mutator does too.
    pub const fn record_probe_collision(&mut self) {
        self.probe_collision += 1;
    }

    /// The join type this probe was constructed for.
    #[must_use]
    pub const fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Go `workID`.
    #[must_use]
    pub const fn work_id(&self) -> usize {
        self.work_id
    }

    /// Go `maxChunkSize`.
    #[must_use]
    pub const fn max_chunk_size(&self) -> usize {
        self.max_chunk_size
    }

    /// Go `keyIndex`.
    #[must_use]
    pub fn key_index(&self) -> &[usize] {
        &self.key_index
    }

    /// Go `hasNullableKey`.
    #[must_use]
    pub const fn has_nullable_key(&self) -> bool {
        self.has_nullable_key
    }

    /// Go `chunkRows`.
    #[must_use]
    pub const fn chunk_rows(&self) -> usize {
        self.chunk_rows
    }

    /// Go `currentProbeRow`.
    #[must_use]
    pub const fn current_probe_row(&self) -> usize {
        self.current_probe_row
    }

    /// Sets Go `currentProbeRow`; the per-join-type probes advance it.
    pub const fn set_current_probe_row(&mut self, row: usize) {
        self.current_probe_row = row;
    }

    /// Go `usedRows`.
    #[must_use]
    pub fn used_rows(&self) -> &[usize] {
        &self.used_rows
    }

    /// Go `matchedRowsHeaders`.
    #[must_use]
    pub fn matched_rows_headers(&self) -> &[usize] {
        &self.matched_rows_headers
    }

    /// Sets one entry of Go `matchedRowsHeaders`, as the per-join-type probes
    /// do while walking a bucket chain.
    pub fn set_matched_rows_header(&mut self, logical_row: usize, header: usize) {
        self.matched_rows_headers[logical_row] = header;
    }

    /// Go `matchedRowsHashValue`.
    #[must_use]
    pub fn matched_rows_hash_value(&self) -> &[u64] {
        &self.matched_rows_hash_value
    }

    /// Go `serializedKeys`.
    #[must_use]
    pub fn serialized_keys(&self) -> &[Vec<u8>] {
        &self.serialized_keys
    }

    /// Go `offsetAndLengthArray`.
    #[must_use]
    pub fn offset_and_length_array(&self) -> &[OffsetAndLength] {
        &self.offset_and_length_array
    }

    /// Go `rowIndexInfos`.
    #[must_use]
    pub fn row_index_infos(&self) -> &[MatchedRowInfo] {
        &self.row_index_infos
    }

    /// Go `selected`; the other-condition filter writes it.
    #[must_use]
    pub fn selected_mut(&mut self) -> &mut Vec<bool> {
        &mut self.selected
    }

    /// Go `currentChunk`.
    #[must_use]
    pub const fn current_chunk(&self) -> Option<&Chunk> {
        self.current_chunk.as_ref()
    }

    /// Go `IsCurrentChunkProbeDone`.
    #[must_use]
    pub fn is_current_chunk_probe_done(&self) -> bool {
        self.current_chunk.is_none() || self.current_probe_row >= self.chunk_rows
    }

    // -----------------------------------------------------------------
    // SetChunkForProbe (`base_join_probe.go:179`)
    // -----------------------------------------------------------------

    /// Go `SetChunkForProbe`: prepare one probe chunk.
    ///
    /// Filters, serializes keys, hashes, buckets by partition and resolves
    /// every logical row's bucket head into `matchedRowsHeaders`.
    ///
    /// # Errors
    ///
    /// [`ProbeError::PreviousChunkNotProbed`] when the previous chunk still
    /// has unprobed rows, or a seam failure from the filter/serializer.
    pub fn set_chunk_for_probe(
        &mut self,
        ctx: &ProbeContext<'_>,
        chunk: Chunk,
        filter: Option<ProbeFilter<'_>>,
        key_serializer: &dyn ProbeKeySerializer,
    ) -> Result<(), ProbeError> {
        // boundary: Go's `defer` here forces `currentProbeRow = chunkRows`
        // when `spillHelper.areAllPartitionsSpilled()`. No spill helper is
        // ported, so no partition is ever spilled and the defer is a no-op.
        if self.current_chunk.is_some() && self.current_probe_row < self.chunk_rows {
            return Err(ProbeError::PreviousChunkNotProbed);
        }

        let logical_rows = chunk.num_rows();
        // Go: `chk.Column(0).Rows()` -- physical rows, which differ from
        // logical rows exactly when the chunk carries a selection vector.
        let physical_rows = if chunk.num_cols() == 0 {
            logical_rows
        } else {
            chunk.column(0).rows()
        };

        match chunk.sel() {
            Some(sel) => self.used_rows = sel.to_vec(),
            None => {
                // Go reuses a shared `fakeSel` prefix below FAKE_SEL_LENGTH
                // and allocates above it; both produce the identity mapping,
                // which is all that is observable.
                if self.sel_rows.len() < logical_rows || logical_rows <= FAKE_SEL_LENGTH {
                    self.sel_rows = (0..logical_rows).collect();
                } else {
                    self.sel_rows.truncate(logical_rows);
                }
                self.used_rows.clone_from(&self.sel_rows);
            }
        }

        self.chunk_rows = logical_rows;
        self.matched_rows_headers.clear();
        self.matched_rows_headers.resize(logical_rows, 0);
        self.matched_rows_hash_value.clear();
        self.matched_rows_hash_value.resize(logical_rows, 0);
        for bucket in &mut self.hash_values {
            bucket.clear();
        }
        if filter.is_some() {
            self.filter_vector = Some(vec![false; physical_rows]);
        }
        if self.has_nullable_key {
            self.null_key_vector = Some(vec![false; physical_rows]);
        }
        self.serialized_keys.clear();
        self.serialized_keys.resize(logical_rows, Vec::new());

        if let Some(filter) = filter {
            let vector = self
                .filter_vector
                .as_mut()
                .expect("filter vector allocated alongside the filter");
            for (physical_row, slot) in vector.iter_mut().enumerate() {
                *slot = filter(&chunk, physical_row);
            }
        }

        // Go: one `codec.SerializeKeys` call fills `serializedKeys` and
        // `nullKeyVector` together, skipping rows the filter already rejected.
        for logical_row in 0..logical_rows {
            let physical_row = self.used_rows[logical_row];
            if self
                .filter_vector
                .as_ref()
                .is_some_and(|vector| !vector[physical_row])
            {
                continue;
            }
            match key_serializer.serialize(&chunk, logical_row) {
                Some(key) => self.serialized_keys[logical_row] = key,
                None => {
                    if let Some(vector) = self.null_key_vector.as_mut() {
                        vector[physical_row] = true;
                    }
                }
            }
        }

        self.current_chunk = Some(chunk);

        for logical_row in 0..logical_rows {
            let physical_row = self.used_rows[logical_row];
            let filtered_out = self
                .filter_vector
                .as_ref()
                .is_some_and(|vector| !vector[physical_row]);
            let null_key = self
                .null_key_vector
                .as_ref()
                .is_some_and(|vector| vector[physical_row]);
            if filtered_out || null_key {
                // Go explicitly zeroes both, so a stale header from a previous
                // chunk can never be walked.
                self.matched_rows_headers[logical_row] = 0;
                self.matched_rows_hash_value[logical_row] = 0;
                continue;
            }

            let hash_value = fnv64(&self.serialized_keys[logical_row]);
            self.matched_rows_hash_value[logical_row] = hash_value;
            let part_index =
                generate_partition_index(hash_value, ctx.partition_mask_offset) as usize;
            // boundary: Go routes a spilled partition's rows to
            // `spillTmpChk[partIndex]` and zeroes the header instead.
            self.hash_values[part_index].push(PosAndHashValue {
                hash_value,
                pos: logical_row,
            });
        }

        self.current_probe_row = 0;
        for part_index in 0..ctx.partition_number {
            for entry in &self.hash_values[part_index] {
                self.matched_rows_headers[entry.pos] = ctx
                    .hash_table
                    .sub_table(part_index)
                    .lookup(entry.hash_value, &ctx.tag_helper);
            }
        }
        Ok(())
    }

    /// Go `SetRestoredChunkForProbe`.
    ///
    /// # Panics
    ///
    /// Always. boundary: `hashJoinSpillHelper` (`hash_join_spill.go`) --
    /// restored chunks only exist once probe-side spilling has run, and their
    /// layout (`hashValue`, `serializedKey`, then the pruned probe columns) is
    /// defined by `spillHelper.probeSpillFieldTypes`. Go's companion
    /// `preAllocForSetRestoredChunkForProbe` and its `rehash` loop are blocked
    /// on the same symbol.
    pub fn set_restored_chunk_for_probe(&mut self, _chunk: Chunk) -> Result<(), ProbeError> {
        todo!("boundary: hashJoinSpillHelper / probeSpillFieldTypes (hash_join_spill.go)")
    }

    /// Go `SpillRemainingProbeChunks`.
    ///
    /// # Panics
    ///
    /// Always. boundary: `hashJoinSpillHelper.spillProbeChk`
    /// (`hash_join_spill.go`).
    pub fn spill_remaining_probe_chunks(&mut self) -> Result<(), ProbeError> {
        todo!("boundary: hashJoinSpillHelper.spillProbeChk (hash_join_spill.go)")
    }

    // -----------------------------------------------------------------
    // Probe-loop bookkeeping (`base_join_probe.go:505`, `:574`)
    // -----------------------------------------------------------------

    /// Go `finishLookupCurrentProbeRow`: close the run of output rows that
    /// share the current probe row.
    pub fn finish_lookup_current_probe_row(&mut self) {
        if self.matched_rows_for_current_probe_row > 0 {
            self.offset_and_length_array.push(OffsetAndLength {
                offset: self.used_rows[self.current_probe_row],
                length: self.matched_rows_for_current_probe_row,
            });
        }
        self.matched_rows_for_current_probe_row = 0;
    }

    /// Counts one more output row for the current probe row, Go's
    /// `matchedRowsForCurrentProbeRow++`.
    pub const fn record_matched_row_for_current_probe_row(&mut self) {
        self.matched_rows_for_current_probe_row += 1;
    }

    /// Go `matchedRowsForCurrentProbeRow`.
    #[must_use]
    pub const fn matched_rows_for_current_probe_row(&self) -> usize {
        self.matched_rows_for_current_probe_row
    }

    /// Go `finishCurrentLookupLoop`: flush cached build rows, close the
    /// current probe row's run, and copy the probe columns in.
    pub fn finish_current_lookup_loop(
        &mut self,
        ctx: &ProbeContext<'_>,
        rows: &dyn BuildRowSource,
        joined_chk: &mut Chunk,
    ) {
        if self.next_cached_build_row_index > 0 {
            self.batch_construct_build_rows(ctx, rows, joined_chk, 0, ctx.has_other_condition());
        }
        self.finish_lookup_current_probe_row();
        let probe_chunk = self
            .current_chunk
            .take()
            .expect("finishCurrentLookupLoop runs with a probe chunk set");
        self.append_probe_row_to_chunk(ctx, joined_chk, &probe_chunk);
        self.current_chunk = Some(probe_chunk);
    }

    /// Go `ResetProbe`.
    ///
    /// Go reallocates `cachedBuildRows` (and `rowIndexInfos`) rather than
    /// truncating, with a comment saying the GC otherwise errors and that they
    /// cannot explain why. Rust has no such hazard; clearing is equivalent,
    /// and the comment's uncertainty is not a behavior worth transcribing.
    pub fn reset_probe(&mut self, ctx: &ProbeContext<'_>) {
        self.cached_build_rows = vec![MatchedRowInfo::default(); BATCH_BUILD_ROW_SIZE];
        self.next_cached_build_row_index = 0;
        if ctx.has_other_condition() {
            self.row_index_infos = Vec::with_capacity(INITIAL_CAPACITY);
        }
    }

    /// Go `prepareForProbe`: reset the per-call scratch and report how many
    /// more rows the output chunk wants.
    ///
    /// Returns whether the caller should build into a scratch chunk. Go
    /// returns `joinedChk`, which is `j.tmpChk` when the join has an other
    /// condition and the caller's `chk` otherwise; a Rust method cannot hand
    /// back a borrow of one of two chunks the caller owns, so the choice is
    /// returned instead and the caller selects.
    ///
    /// # Errors
    ///
    /// Never; the `error` return exists in Go for signature uniformity and is
    /// kept so call sites read the same.
    pub fn prepare_for_probe(&mut self, ctx: &ProbeContext<'_>, chk: &Chunk) -> (bool, usize) {
        self.offset_and_length_array.clear();
        self.next_cached_build_row_index = 0;
        self.matched_rows_for_current_probe_row = 0;
        let use_scratch = ctx.has_other_condition();
        if use_scratch {
            self.row_index_infos.clear();
            self.selected.clear();
        }
        (
            use_scratch,
            chk.required_rows().saturating_sub(chk.num_rows()),
        )
    }

    // -----------------------------------------------------------------
    // Build-row staging (`base_join_probe.go:531`, `:540`, `:551`)
    // -----------------------------------------------------------------

    /// Go `appendBuildRowToCachedBuildRowsV2`: stage an already-formed
    /// [`MatchedRowInfo`], flushing at [`BATCH_BUILD_ROW_SIZE`].
    pub fn append_build_row_to_cached_build_rows_v2(
        &mut self,
        ctx: &ProbeContext<'_>,
        rows: &dyn BuildRowSource,
        row_info: MatchedRowInfo,
        chk: &mut Chunk,
        current_column_index_in_row: usize,
        for_other_condition: bool,
    ) {
        self.cached_build_rows[self.next_cached_build_row_index] = row_info;
        self.next_cached_build_row_index += 1;
        if self.next_cached_build_row_index == BATCH_BUILD_ROW_SIZE {
            self.batch_construct_build_rows(
                ctx,
                rows,
                chk,
                current_column_index_in_row,
                for_other_condition,
            );
        }
    }

    /// Go `appendBuildRowToCachedBuildRowsV1`: stage a fresh match by probe
    /// row and build-row address, with the offset reset to `0`.
    // Go's `appendBuildRowToCachedBuildRowsV1` takes the same six arguments
    // plus the receiver; splitting them into a struct would hide which call
    // site passes which of Go's parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn append_build_row_to_cached_build_rows_v1(
        &mut self,
        ctx: &ProbeContext<'_>,
        rows: &dyn BuildRowSource,
        probe_row_index: usize,
        build_row_start: usize,
        chk: &mut Chunk,
        current_column_index_in_row: usize,
        for_other_condition: bool,
    ) {
        self.append_build_row_to_cached_build_rows_v2(
            ctx,
            rows,
            MatchedRowInfo {
                probe_row_index,
                build_row_start,
                build_row_offset: 0,
            },
            chk,
            current_column_index_in_row,
            for_other_condition,
        );
    }

    /// Go `batchConstructBuildRows`: reconstruct the staged rows, remember
    /// them when an other condition still has to see them, and empty the
    /// staging array.
    pub fn batch_construct_build_rows(
        &mut self,
        ctx: &ProbeContext<'_>,
        rows: &dyn BuildRowSource,
        chk: &mut Chunk,
        current_column_index_in_row: usize,
        for_other_condition: bool,
    ) {
        self.append_build_row_to_chunk(
            ctx,
            rows,
            chk,
            current_column_index_in_row,
            for_other_condition,
        );
        if for_other_condition {
            self.row_index_infos
                .extend_from_slice(&self.cached_build_rows[..self.next_cached_build_row_index]);
        }
        self.next_cached_build_row_index = 0;
    }

    // -----------------------------------------------------------------
    // Build-side reconstruction (`base_join_probe.go:583`, `:599`)
    // -----------------------------------------------------------------

    /// Go `joinTableMeta.advanceToRowData`: skip a row's header so that
    /// `build_row_offset` points at the first stored column.
    fn advance_to_row_data(meta: &RowLayoutMeta, info: &mut MatchedRowInfo, row: &[u8]) {
        info.build_row_offset = meta.row_data_offset(row);
    }

    /// Go `appendBuildRowToChunk`: pick which used-column list and which
    /// destination offset apply, from the build side and the caller's phase.
    pub fn append_build_row_to_chunk(
        &mut self,
        ctx: &ProbeContext<'_>,
        rows: &dyn BuildRowSource,
        chk: &mut Chunk,
        current_column_index_in_row: usize,
        for_other_condition: bool,
    ) {
        let probe_num_cols = self
            .current_chunk
            .as_ref()
            .map_or(0, tidb_chunk::chunk::Chunk::num_cols);
        let (used_cols, col_offset) = if self.right_as_build_side {
            if for_other_condition {
                (ctx.r_used_in_other_condition.clone(), probe_num_cols)
            } else {
                (ctx.r_used.clone(), ctx.l_used.len())
            }
        } else if for_other_condition {
            (ctx.l_used_in_other_condition.clone(), 0)
        } else {
            (ctx.l_used.clone(), 0)
        };
        self.append_build_row_to_chunk_internal(
            ctx,
            rows,
            chk,
            &used_cols,
            for_other_condition,
            col_offset,
            current_column_index_in_row,
        );
    }

    /// Go `appendBuildRowToChunkInternal`: walk the row layout once, and for
    /// each stored column either append it to its destination column or step
    /// past it.
    #[allow(clippy::too_many_arguments)]
    fn append_build_row_to_chunk_internal(
        &mut self,
        ctx: &ProbeContext<'_>,
        rows: &dyn BuildRowSource,
        chk: &mut Chunk,
        used_cols: &[usize],
        for_other_condition: bool,
        col_offset: usize,
        current_column_in_row: usize,
    ) {
        let chk_rows = chk.num_rows();
        let need_update_virtual_row = current_column_in_row == 0;
        let live = self.next_cached_build_row_index;
        if used_cols.is_empty() || live == 0 {
            if need_update_virtual_row {
                chk.set_num_virtual_rows(chk_rows + live);
            }
            return;
        }

        let meta = ctx.meta;
        for index in 0..live {
            if self.cached_build_rows[index].build_row_offset == 0 {
                let row = rows.row_bytes(self.cached_build_rows[index].build_row_start);
                Self::advance_to_row_data(meta, &mut self.cached_build_rows[index], row);
            }
        }

        // Go's `colIndexMap`: build column index -> destination chunk column.
        // For the other-condition phase the destination keeps the *source*
        // column index (the scratch chunk is laid out probe-then-build in
        // original order); otherwise it is packed into `used_cols` order.
        let mut col_index_map: HashMap<usize, usize> = HashMap::new();
        for (index, &value) in used_cols.iter().enumerate() {
            if for_other_condition {
                col_index_map.insert(value, value + col_offset);
            } else {
                col_index_map.insert(value, index + col_offset);
            }
        }
        let mut columns_to_append = meta.row_columns_order.len();
        if for_other_condition {
            columns_to_append = ctx.column_count_needed_for_other_condition;
            let also = if ctx.right_as_build_side {
                &ctx.r_used
            } else {
                &ctx.l_used
            };
            for &value in also {
                col_index_map.insert(value, value + col_offset);
            }
        }

        let last_column = meta.row_columns_order.len().min(columns_to_append);
        for column_index in current_column_in_row..last_column {
            let source_column = meta.row_columns_order[column_index];
            if let Some(&destination_index) = col_index_map.get(&source_column) {
                for index in 0..live {
                    let address = self.cached_build_rows[index].build_row_start;
                    let offset = self.cached_build_rows[index].build_row_offset;
                    let row = rows.row_bytes(address);
                    // Narrowing: Go picks `isColumnNull` or
                    // `isColumnNullThreadSafe` from
                    // `meta.isReadNullMapThreadSafe(columnIndex)`; the two
                    // differ only in atomicity of the null-map read.
                    let not_null = !meta.is_column_null(row, column_index);
                    let mut destination = chk.column_mut(destination_index);
                    destination.append_null_bitmap(not_null);
                    let new_offset = append_cell_from_raw_data(&mut destination, row, offset);
                    drop(destination);
                    self.cached_build_rows[index].build_row_offset = new_offset;
                }
            } else {
                // Not used downstream, so nothing is appended -- but the row
                // cursor still has to step over the column's bytes.
                match meta.columns_size[column_index] {
                    Some(size) => {
                        for index in 0..live {
                            self.cached_build_rows[index].build_row_offset += size;
                        }
                    }
                    None => {
                        for index in 0..live {
                            let address = self.cached_build_rows[index].build_row_start;
                            let offset = self.cached_build_rows[index].build_row_offset;
                            let row = rows.row_bytes(address);
                            let mut size_bytes = [0_u8; SIZE_OF_ELEMENT_SIZE];
                            size_bytes.copy_from_slice(&row[offset..offset + SIZE_OF_ELEMENT_SIZE]);
                            let size = u32::from_ne_bytes(size_bytes) as usize;
                            self.cached_build_rows[index].build_row_offset +=
                                SIZE_OF_ELEMENT_SIZE + size;
                        }
                    }
                }
            }
        }
        if need_update_virtual_row {
            chk.set_num_virtual_rows(chk_rows + live);
        }
    }

    // -----------------------------------------------------------------
    // Probe-side reconstruction (`base_join_probe.go:680`, `:696`)
    // -----------------------------------------------------------------

    /// Go `appendProbeRowToChunk`.
    pub fn append_probe_row_to_chunk(
        &self,
        ctx: &ProbeContext<'_>,
        chk: &mut Chunk,
        probe_chk: &Chunk,
    ) {
        let (used, col_offset, for_other_condition) = if self.right_as_build_side {
            if ctx.has_other_condition() {
                (&ctx.l_used_in_other_condition, 0, true)
            } else {
                (&ctx.l_used, 0, false)
            }
        } else if ctx.has_other_condition() {
            (
                &ctx.r_used_in_other_condition,
                ctx.total_column_number,
                true,
            )
        } else {
            (&ctx.r_used, ctx.l_used.len(), false)
        };
        Self::append_probe_row_to_chunk_internal(
            &self.offset_and_length_array,
            chk,
            probe_chk,
            used,
            col_offset,
            for_other_condition,
        );
    }

    /// Go `appendProbeRowToChunkInternal`: replay each `offsetAndLength` run
    /// as `AppendCellNTimes`, after reserving exactly the space it needs.
    fn append_probe_row_to_chunk_internal(
        runs: &[OffsetAndLength],
        chk: &mut Chunk,
        probe_chk: &Chunk,
        used: &[usize],
        col_offset: usize,
        for_other_condition: bool,
    ) {
        if used.is_empty() || runs.is_empty() {
            return;
        }
        let total_times: usize = runs.iter().map(|run| run.length).sum();

        // Go's `preAllocMemForCol` closure: size the destination exactly once,
        // so the `AppendCellNTimes` loop below never reallocates. Go asserts
        // that in test builds; that assertion is dropped (see module header).
        let pre_alloc = |source: &Column, destination: &mut Column| {
            let null_bitmap_delta =
                destination.null_bitmap_len_delta_for_append_cell_n_times(total_times);
            let (data_delta, offset_delta) = if destination.is_fixed() {
                (
                    Column::fixed_len_delta_for_append_cell_n_times(source, total_times),
                    0,
                )
            } else {
                let data = runs
                    .iter()
                    .map(|run| {
                        Column::var_len_delta_for_append_cell_n_times(
                            source, run.offset, run.length,
                        )
                    })
                    .sum();
                (data, total_times)
            };
            destination.reserve(null_bitmap_delta, data_delta, offset_delta);
        };

        if for_other_condition {
            // Go dedupes with `usedColumnMap`, because a column can appear in
            // the other-condition list more than once and each cell must be
            // appended exactly once.
            let mut seen: Vec<usize> = Vec::with_capacity(used.len());
            for &col_index in used {
                if seen.contains(&col_index) {
                    continue;
                }
                seen.push(col_index);
                let source = probe_chk.column(col_index).clone();
                let mut destination = chk.column_mut(col_index + col_offset);
                pre_alloc(&source, &mut destination);
                for run in runs {
                    destination.append_cell_n_times(&source, run.offset, run.length);
                }
            }
        } else {
            for (index, &col_index) in used.iter().enumerate() {
                let source = probe_chk.column(col_index).clone();
                let mut destination = chk.column_mut(index + col_offset);
                pre_alloc(&source, &mut destination);
                for run in runs {
                    destination.append_cell_n_times(&source, run.offset, run.length);
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // buildResultAfterOtherCondition (`base_join_probe.go:774`)
    // -----------------------------------------------------------------

    /// Go `buildResultAfterOtherCondition`: turn the scratch chunk plus the
    /// `selected` verdicts into the real output chunk.
    ///
    /// Go's own comment enumerates the three kinds of column involved:
    /// already in `joinedChk`; from the build side but not in it; from the
    /// probe side but not in it.
    pub fn build_result_after_other_condition(
        &mut self,
        ctx: &ProbeContext<'_>,
        rows: &dyn BuildRowSource,
        chk: &mut Chunk,
        joined_chk: &Chunk,
    ) {
        let row_count = chk.num_rows();
        let probe_chunk = self
            .current_chunk
            .take()
            .expect("buildResultAfterOtherCondition runs with a probe chunk set");

        let (probe_used_columns, probe_col_offset, probe_col_offset_in_joined_chk) =
            if self.right_as_build_side {
                (ctx.l_used.clone(), 0, 0)
            } else {
                (
                    ctx.r_used.clone(),
                    ctx.l_used.len(),
                    ctx.total_column_number,
                )
            };

        for (index, &col_index) in probe_used_columns.iter().enumerate() {
            let joined_index = col_index + probe_col_offset_in_joined_chk;
            if joined_chk.column(joined_index).rows() > 0 {
                let source = joined_chk.column(joined_index).clone();
                let mut destination = chk.column_mut(index + probe_col_offset);
                copy_selected_rows(&mut destination, &source, &self.selected);
            } else {
                let source = probe_chunk.column(col_index).clone();
                let selected_len = self.selected.len();
                let mut destination = chk.column_mut(index + probe_col_offset);
                copy_selected_rows_with_row_id_func(
                    &mut destination,
                    &source,
                    &self.selected,
                    0,
                    selected_len,
                    |i| self.used_rows[self.row_index_infos[i].probe_row_index],
                );
            }
        }

        let (build_used_columns, build_col_offset, build_col_offset_in_joined_chk) =
            if self.right_as_build_side {
                (ctx.r_used.clone(), ctx.l_used.len(), probe_chunk.num_cols())
            } else {
                (ctx.l_used.clone(), 0, 0)
            };
        let mut has_remain_cols = false;
        for (index, &col_index) in build_used_columns.iter().enumerate() {
            let joined_index = col_index + build_col_offset_in_joined_chk;
            let source = joined_chk.column(joined_index).clone();
            if source.rows() > 0 {
                let mut destination = chk.column_mut(index + build_col_offset);
                copy_selected_rows(&mut destination, &source, &self.selected);
            } else {
                has_remain_cols = true;
            }
        }

        self.current_chunk = Some(probe_chunk);

        if has_remain_cols {
            self.next_cached_build_row_index = 0;
            let column_count = ctx.column_count_needed_for_other_condition;
            for index in 0..self.selected.len() {
                if self.selected[index] {
                    let info = self.row_index_infos[index];
                    self.append_build_row_to_cached_build_rows_v2(
                        ctx,
                        rows,
                        info,
                        chk,
                        column_count,
                        false,
                    );
                }
            }
            if self.next_cached_build_row_index > 0 {
                self.batch_construct_build_rows(ctx, rows, chk, column_count, false);
            }
        }

        let rows_added = self.selected.iter().filter(|selected| **selected).count();
        chk.set_num_virtual_rows(row_count + rows_added);
    }

    // -----------------------------------------------------------------
    // Bucket-chain walking helpers
    // -----------------------------------------------------------------

    /// Go `getNextRowAddress` as reached from the probe: follow one link of
    /// the current probe row's chain, honoring the tag short-circuit.
    #[must_use]
    pub fn next_matched_row(
        rows: &dyn BuildRowSource,
        tag_helper: &TagPtrHelper,
        current: usize,
        hash_value: u64,
    ) -> usize {
        let untagged = row_address_of(tag_helper, current);
        if untagged == 0 {
            return 0;
        }
        next_row_address(rows.raw_next_row_address(untagged), tag_helper, hash_value)
    }
}

/// Go `isKeyMatched`: compare a probe row's serialized key against the key
/// stored in a build row.
///
/// The three key modes differ only in how the stored key's extent is found,
/// which [`RowLayoutMeta::get_key_bytes`] already encodes; Go's `OneInt64` arm
/// compares two `int64`s, which is byte equality over the same eight bytes.
#[must_use]
pub fn is_key_matched(
    key_mode: KeyMode,
    serialized_key: &[u8],
    row: &[u8],
    meta: &RowLayoutMeta,
) -> bool {
    debug_assert_eq!(
        key_mode, meta.key_mode,
        "key mode must agree with the layout"
    );
    match key_mode {
        KeyMode::OneInt64 | KeyMode::FixedSerializedKey | KeyMode::VariableSerializedKey => {
            serialized_key == meta.get_key_bytes(row)
        }
    }
}

/// Go `commonInitForScanRowTable`: the slice of the row table this worker
/// scans for unmatched build rows.
///
/// Go divides by worker id, and the last worker takes the remainder. That
/// division is observable per worker, so it is ported literally even though
/// this port drives one worker at a time; `concurrency == 1` yields the whole
/// table.
///
/// # Panics
///
/// Panics when `concurrency` is zero, which Go would divide by.
#[must_use]
pub fn common_init_for_scan_row_table<'a>(
    hash_table: &'a HashTableV2,
    work_id: usize,
    concurrency: usize,
) -> RowIter<'a> {
    assert!(concurrency > 0, "probe concurrency must be positive");
    let total_row_count = hash_table.total_row_count();
    let work_id = work_id as u64;
    let avg_row_per_worker = total_row_count / concurrency as u64;
    let start_index = work_id * avg_row_per_worker;
    let mut end_index = (work_id + 1) * avg_row_per_worker;
    if work_id == concurrency as u64 - 1 {
        end_index = total_row_count;
    }
    if end_index > total_row_count {
        end_index = total_row_count;
    }
    hash_table.create_row_iter(start_index, end_index)
}

/// Go `NewJoinProbe`.
///
/// Builds the shared base and applies every validation Go's factory performs,
/// then stops at the dispatch. The per-join-type wrappers Go returns
/// (`innerJoinProbe`, `newOuterJoinProbe`, `newSemiJoinProbe`,
/// `newAntiSemiJoinProbe`, `newLeftOuterSemiJoinProbe`) live in other files
/// and are not part of this port; their match/miss semantics are already
/// modeled by `tidb_executor::joiner`.
///
/// `probe_key_nullable` mirrors Go's `!mysql.HasNotNullFlag(flag)` test over
/// `probeKeyTypes`, one entry per key.
///
/// # Panics
///
/// Panics with Go's messages: on a semi-family join whose `RUsed` is
/// non-empty, on a left-outer-semi or anti-left-outer-semi join built on the
/// left side, and on an unsupported join type.
#[must_use]
pub fn new_join_probe(
    ctx: &ProbeContext<'_>,
    work_id: usize,
    join_type: JoinType,
    key_index: Vec<usize>,
    probe_key_nullable: &[bool],
    right_as_build_side: bool,
) -> BaseJoinProbe {
    match join_type {
        JoinType::SemiJoin => assert!(ctx.r_used.is_empty(), "len(base.rUsed) != 0 for semi join"),
        JoinType::AntiSemiJoin => assert!(
            ctx.r_used.is_empty(),
            "len(base.rUsed) != 0 for anti semi join"
        ),
        JoinType::LeftOuterSemiJoin => {
            assert!(
                ctx.r_used.is_empty(),
                "len(base.rUsed) != 0 for left outer semi join"
            );
            assert!(right_as_build_side, "unsupported join type");
        }
        JoinType::AntiLeftOuterSemiJoin => {
            assert!(
                ctx.r_used.is_empty(),
                "len(base.rUsed) != 0 for left outer anti semi join"
            );
            assert!(right_as_build_side, "unsupported join type");
        }
        JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter => {}
    }

    let has_nullable_key = probe_key_nullable.iter().take(key_index.len()).any(|n| *n);

    BaseJoinProbe {
        work_id,
        current_chunk: None,
        sel_rows: (0..INITIAL_CAPACITY).collect(),
        used_rows: Vec::new(),
        matched_rows_headers: Vec::with_capacity(INITIAL_CAPACITY),
        matched_rows_hash_value: Vec::with_capacity(INITIAL_CAPACITY),
        serialized_keys: Vec::with_capacity(INITIAL_CAPACITY),
        filter_vector: None,
        null_key_vector: None,
        hash_values: (0..ctx.partition_number)
            .map(|_| Vec::with_capacity(INITIAL_CAPACITY))
            .collect(),
        current_probe_row: 0,
        matched_rows_for_current_probe_row: 0,
        chunk_rows: 0,
        cached_build_rows: vec![MatchedRowInfo::default(); BATCH_BUILD_ROW_SIZE],
        next_cached_build_row_index: 0,
        key_index,
        has_nullable_key,
        max_chunk_size: ctx.max_chunk_size,
        right_as_build_side,
        offset_and_length_array: Vec::new(),
        row_index_infos: if ctx.has_other_condition() {
            Vec::with_capacity(INITIAL_CAPACITY)
        } else {
            Vec::new()
        },
        selected: if ctx.has_other_condition() {
            Vec::with_capacity(INITIAL_CAPACITY)
        } else {
            Vec::new()
        },
        probe_collision: 0,
        join_type,
    }
}
