// Copyright 2024 PingCAP, Inc.
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

//! Go `pkg/executor/join` chunk-to-row conversion, covering
//! `row_table_builder.go`.
//!
//! SEED of `pkg/executor/join`: see [`crate::join_row_table`] for the ported
//! and unported file list. This module also carries the three partition
//! helpers the builder consumes from `hash_join_v2.go`
//! (`genHashJoinPartitionNumber`, `getPartitionMaskOffset`,
//! `generatePartitionIndex`) plus `rehash`, because they decide which
//! partition a row lands in and nothing else in that file is ported.
//!
//! What is LAYOUT-IDENTICAL to Go, byte for byte on little-endian targets:
//!
//! * the emitted row bytes: 8-byte `next_row_ptr` placeholder, null-map
//!   bytes with bit `1 << (7 - i % 8)` at byte `i / 8`, the little-endian
//!   4-byte `key_length`, the serialized key or the fixed-width fake key, and
//!   `row_data` where a fixed column is raw bytes and a variable column is a
//!   little-endian 4-byte length followed by its raw bytes;
//! * the trailing zero padding that rounds each row up to 8 bytes, so every
//!   row start is 8-byte aligned;
//! * `hashValues`, `rowStartOffset`, and `validJoinKeyPos` contents, and the
//!   round-robin partition assignment given to filtered rows;
//! * the FNV-1/64 hash of the serialized key and the `hash >> maskOffset`
//!   partition index, including the `maskOffset == 64` case that Go's shift
//!   defines as zero.
//!
//! What is only OBSERVABLY EQUIVALENT:
//!
//! * join keys use the codec's native column-wide serializer; build filters
//!   bind the shared expression evaluator once per statement context.
//! * `hashJoinCtx.hashTableContext.memoryTracker.Consume` charges the native
//!   stage's shared tracker before row-segment allocation; component fixtures
//!   can use [`BuildContext::consumed_memory`] alone. The native stage supplies
//!   the statement killer for Go's cancellation checkpoints.
//! * `resizeSlice` is not ported; `Vec::resize` has the same reuse-or-grow
//!   behavior.
//! * Spill restore rehashes saved hashes and copies native raw rows unchanged.

use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_codec::{JoinKeyColumns, SerializedJoinKeys};
use tidb_util::serialization::{INT_LEN, UINT64_LEN};
use tidb_util::{memory::Tracker, sqlkiller::SqlKiller};

use crate::join_row_table::{
    RowLayoutMeta, RowTableSegment, FAKE_ADDR_PLACE_HOLDER, FAKE_ADDR_PLACE_HOLDER_LEN,
    SIZE_OF_ELEMENT_SIZE,
};

const FNV64_OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
const FNV64_PRIME: u64 = 1_099_511_628_211;

/// Go `hash/fnv`'s 64-bit FNV-1, the hash the source builds join keys with.
#[must_use]
pub fn fnv64(data: &[u8]) -> u64 {
    let mut hash = FNV64_OFFSET_BASIS;
    for &byte in data {
        hash = hash.wrapping_mul(FNV64_PRIME);
        hash ^= u64::from(byte);
    }
    hash
}

/// Rehashes a spilled row's hash value, as the source's `rehash` does.
#[must_use]
pub fn rehash(old_hash_value: u64) -> u64 {
    fnv64(&old_hash_value.to_le_bytes())
}

/// Partition geometry derived from the join's build concurrency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionInfo {
    /// Number of build partitions, a power of two capped at 16.
    pub partition_number: usize,
    /// Right shift that turns a hash value into a partition index.
    pub partition_mask_offset: usize,
}

impl PartitionInfo {
    /// `HashJoinCtxV2.SetupPartitionInfo` for a given concurrency.
    #[must_use]
    pub const fn new(concurrency: usize) -> Self {
        let partition_number = gen_hash_join_partition_number(concurrency);
        Self {
            partition_number,
            partition_mask_offset: get_partition_mask_offset(partition_number),
        }
    }

    /// Partition index of a hash value.
    #[must_use]
    pub const fn partition_index(&self, hash_value: u64) -> usize {
        generate_partition_index(hash_value, self.partition_mask_offset) as usize
    }
}

/// Rounds a concurrency hint up to a power of two, capped at 16.
#[must_use]
pub const fn gen_hash_join_partition_number(partition_hint: usize) -> usize {
    let mut partition_number = 1_usize;
    while partition_number < partition_hint && partition_number < 16 {
        partition_number <<= 1;
    }
    partition_number
}

/// Shift that leaves only the partition bits of a hash value.
#[must_use]
pub const fn get_partition_mask_offset(partition_number: usize) -> usize {
    64 - (partition_number as u64).trailing_zeros() as usize
}

/// Extracts the partition index from a hash value.
///
/// A single partition yields an offset of 64; Go's shift is defined to give
/// zero there, while Rust's would panic, so the shift is checked.
#[must_use]
pub const fn generate_partition_index(hash_value: u64, partition_mask_offset: usize) -> u64 {
    match hash_value.checked_shr(partition_mask_offset as u32) {
        Some(value) => value,
        None => 0,
    }
}

/// Go `HashJoinCtxV2.BuildFilter`, evaluated with VectorizedFilter.
pub type BuildFilter<'a> = &'a crate::base_join_probe::JoinFilter<'a>;

/// Everything outside the builder that one `processOneChunk` call reads.
pub struct BuildContext<'a> {
    /// Row layout the emitted bytes must follow.
    pub meta: &'a RowLayoutMeta,
    /// Partition geometry for this join.
    pub partition: PartitionInfo,
    /// Serializer for join keys.
    pub key_serializer: &'a JoinKeyColumns,
    /// Build expressions, evaluated chunk-wide; `None` keeps every row.
    pub build_filter: Option<BuildFilter<'a>>,
    /// Running total of the source's hash-table memory tracker.
    pub consumed_memory: i64,
    /// Shared tracker and killer used by the native build stage.
    pub memory_tracker: Option<&'a Arc<Tracker>>,
    pub sql_killer: Option<&'a SqlKiller>,
}

impl<'a> BuildContext<'a> {
    /// Creates a context with an empty memory tracker and no filter.
    #[must_use]
    pub fn new(
        meta: &'a RowLayoutMeta,
        partition: PartitionInfo,
        key_serializer: &'a JoinKeyColumns,
    ) -> Self {
        Self {
            meta,
            partition,
            key_serializer,
            build_filter: None,
            consumed_memory: 0,
            memory_tracker: None,
            sql_killer: None,
        }
    }

    fn check_killed(&self) -> Result<(), RowTableBuildError> {
        if let Some(error) = self.sql_killer.and_then(SqlKiller::handle_signal) {
            return Err(RowTableBuildError::Killed(error.to_sql_error()));
        }
        Ok(())
    }

    fn consume(&mut self, delta: i64) {
        self.consumed_memory += delta;
        if let Some(tracker) = self.memory_tracker {
            tracker.consume(delta);
        }
    }
}

/// Errors the source returns when an element cannot be length-prefixed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowTableBuildError {
    /// Original expression error from the build-side filter.
    Expression(tidb_expr::EvalError),
    /// Canonical SQL cancellation, including quota exhaustion before allocation.
    Killed(tidb_error::mysql::SqlError),
    /// Codec failure while constructing the chunk's join keys.
    KeyEncoding(String),
    /// A stored column holds an element wider than a `u32` size prefix.
    ColumnElementTooLarge {
        /// Index of the offending build column.
        column_index: usize,
    },
    /// A serialized join key is wider than a `u32` size prefix.
    JoinKeyTooLarge,
}

impl std::fmt::Display for RowTableBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expression(error) => write!(formatter, "{error:?}"),
            Self::Killed(error) => write!(formatter, "{error:?}"),
            Self::KeyEncoding(message) => formatter.write_str(message),
            Self::ColumnElementTooLarge { column_index } => write!(
                formatter,
                "row table build failed: column contains element larger than 4GB, column index: {column_index}"
            ),
            Self::JoinKeyTooLarge => write!(
                formatter,
                "row table build failed: join key contains element larger than 4GB"
            ),
        }
    }
}

impl std::error::Error for RowTableBuildError {}

/// Per-partition pre-allocation totals for one chunk.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreAllocHelper {
    /// Rows that will be written to this partition.
    pub total_row_num: i64,
    /// Rows with a valid join key.
    pub valid_row_num: i64,
    /// Bytes those rows will occupy.
    pub raw_data_len: i64,
}

impl PreAllocHelper {
    const fn reset(&mut self) {
        self.total_row_num = 0;
        self.valid_row_num = 0;
        self.raw_data_len = 0;
    }
}

/// Converts build-side chunks into hash-join row segments.
#[derive(Clone, Debug)]
pub struct RowTableBuilder {
    /// Build-side column indices that form the join key.
    pub build_key_index: Vec<usize>,
    /// Whether any join key column is nullable.
    pub has_nullable_key: bool,
    /// Whether a build filter runs before conversion.
    pub has_filter: bool,
    /// Whether rows rejected by filter or null key are still stored.
    pub keep_filtered_rows: bool,
    /// Number of build partitions.
    pub partition_number: usize,
    /// Serialized key of each logical row; empty for rejected rows.
    pub serialized_key_vector_buffer: SerializedJoinKeys,
    /// Partition index of each logical row.
    pub part_idx_vector: Vec<usize>,
    /// Physical row index of each logical row.
    pub used_rows: Vec<usize>,
    /// Whether `used_rows` currently contains the identity mapping. Go keeps
    /// this mapping in the reusable `fakeSel` backing slice, so consecutive
    /// unselected chunks only adjust the visible length.
    used_rows_are_identity: bool,
    /// Hash value of each logical row.
    pub hash_value: Vec<u64>,
    /// Row-count hint for the first segment of the chunk.
    pub first_seg_row_size_hint: usize,
    /// Filter result per physical row, when a filter runs.
    pub filter_vector: Option<Vec<bool>>,
    /// Null-key flag per physical row, when the key is nullable.
    pub null_key_vector: Option<Vec<bool>>,
    null_map: Vec<u8>,
    helpers: Vec<PreAllocHelper>,
}

impl RowTableBuilder {
    /// `createRowTableBuilder`.
    #[must_use]
    pub fn new(
        build_key_index: Vec<usize>,
        partition_number: usize,
        has_nullable_key: bool,
        has_filter: bool,
        keep_filtered_rows: bool,
        null_map_length: usize,
    ) -> Self {
        Self {
            build_key_index,
            has_nullable_key,
            has_filter,
            keep_filtered_rows,
            partition_number,
            serialized_key_vector_buffer: SerializedJoinKeys::default(),
            part_idx_vector: Vec::new(),
            used_rows: Vec::new(),
            used_rows_are_identity: false,
            hash_value: Vec::new(),
            first_seg_row_size_hint: 0,
            filter_vector: None,
            null_key_vector: None,
            null_map: vec![0_u8; null_map_length],
            helpers: vec![PreAllocHelper::default(); partition_number],
        }
    }

    /// Per-partition pre-allocation totals from the last chunk.
    #[must_use]
    pub fn helpers(&self) -> &[PreAllocHelper] {
        &self.helpers
    }

    /// `ResetBuffer`: re-points the per-chunk vectors at this chunk's shape.
    pub fn reset_buffer(&mut self, chunk: &Chunk) {
        let logical_rows = chunk.num_rows();
        let physical_rows = chunk.physical_rows();
        match chunk.sel() {
            Some(sel) => {
                self.used_rows.clear();
                self.used_rows.extend_from_slice(sel);
                self.used_rows_are_identity = false;
            }
            None => {
                if !self.used_rows_are_identity {
                    self.used_rows.clear();
                    self.used_rows_are_identity = true;
                }
                if self.used_rows.len() < logical_rows {
                    let start = self.used_rows.len();
                    self.used_rows.extend(start..logical_rows);
                } else {
                    self.used_rows.truncate(logical_rows);
                }
            }
        }
        self.part_idx_vector.resize(logical_rows, 0);
        self.hash_value.resize(logical_rows, 0);
        self.hash_value.truncate(logical_rows);
        if self.has_filter {
            self.filter_vector
                .get_or_insert_with(Vec::new)
                .resize(physical_rows, false);
        }
    }

    /// `checkMaxElementSize`.
    fn check_max_element_size(&self, chunk: &Chunk, meta: &RowLayoutMeta) -> Option<usize> {
        self.build_key_index
            .iter()
            .chain(meta.row_columns_order.iter())
            .find(|&&column_index| chunk.column(column_index).contains_very_large_element())
            .copied()
    }

    /// Whether a physical row survives the filter and has a non-null key.
    #[must_use]
    pub fn has_valid_key(&self, physical_row_index: usize) -> bool {
        let passes_filter = self
            .filter_vector
            .as_ref()
            .is_none_or(|vector| vector[physical_row_index]);
        let key_not_null = self
            .null_key_vector
            .as_ref()
            .is_none_or(|vector| !vector[physical_row_index]);
        passes_filter && key_not_null
    }

    /// `initHashValueAndPartIndexForOneChunk`.
    pub fn init_hash_value_and_part_index_for_one_chunk(&mut self, partition: PartitionInfo) {
        let mut fake_part_index = 0_u64;
        for logical_row_index in 0..self.used_rows.len() {
            let physical_row_index = self.used_rows[logical_row_index];
            if !self.has_valid_key(physical_row_index) {
                self.hash_value[logical_row_index] = fake_part_index;
                self.part_idx_vector[logical_row_index] =
                    usize::try_from(fake_part_index).expect("fake partition index");
                fake_part_index = (fake_part_index + 1) % partition.partition_number as u64;
                continue;
            }
            let hash = fnv64(&self.serialized_key_vector_buffer[logical_row_index]);
            self.hash_value[logical_row_index] = hash;
            self.part_idx_vector[logical_row_index] = partition.partition_index(hash);
        }
    }

    /// `processOneChunk`: converts one chunk into one segment per partition.
    ///
    /// # Errors
    ///
    /// Returns [`RowTableBuildError`] when a column element or a serialized
    /// join key exceeds the 4-byte size prefix.
    pub fn process_one_chunk(
        &mut self,
        chunk: &Chunk,
        context: &mut BuildContext<'_>,
    ) -> Result<Vec<RowTableSegment>, RowTableBuildError> {
        if let Some(column_index) = self.check_max_element_size(chunk, context.meta) {
            return Err(RowTableBuildError::ColumnElementTooLarge { column_index });
        }
        self.reset_buffer(chunk);
        if self.used_rows.is_empty() {
            return Ok(Vec::new());
        }
        self.first_seg_row_size_hint = 1.max(
            (self.used_rows.len() as f64 / context.partition.partition_number as f64 * 1.2)
                as usize,
        );

        if let Some(filter) = context.build_filter {
            self.filter_vector = Some(
                filter
                    .evaluate(chunk, self.filter_vector.take().unwrap_or_default())
                    .map_err(RowTableBuildError::Expression)?,
            );
        }
        context.check_killed()?;

        let result = if self.has_nullable_key {
            context.key_serializer.serialize(
                chunk,
                &self.used_rows,
                self.filter_vector.as_deref(),
                self.null_key_vector.get_or_insert_with(Vec::new),
                &mut self.serialized_key_vector_buffer,
            )
        } else {
            context.key_serializer.serialize_without_nulls(
                chunk,
                &self.used_rows,
                self.filter_vector.as_deref(),
                &mut self.serialized_key_vector_buffer,
            )
        };
        // Go charges row segments and buckets, not reusable key scratch. In
        // particular, spill selection must count the storage it can release.
        result.map_err(|error| RowTableBuildError::KeyEncoding(error.to_string()))?;
        if self
            .serialized_key_vector_buffer
            .iter()
            .any(|key| key.len() > u32::MAX as usize)
        {
            return Err(RowTableBuildError::JoinKeyTooLarge);
        }
        context.check_killed()?;

        self.init_hash_value_and_part_index_for_one_chunk(context.partition);
        self.append_to_row_table(chunk, context)
    }

    /// Go `processOneRestoredChunk`: preallocate once, rehash, then copy saved rows.
    pub fn process_one_restored_chunk(
        &mut self,
        chunk: &Chunk,
        context: &mut BuildContext<'_>,
    ) -> Result<Vec<RowTableSegment>, RowTableBuildError> {
        assert!(
            chunk.sel().is_none(),
            "restored build chunk has no selection"
        );
        let rows = chunk.num_rows();
        self.part_idx_vector.resize(rows, 0);
        self.hash_value.resize(rows, 0);
        for helper in &mut self.helpers {
            helper.reset();
        }
        let mut fake_part = 0;
        for index in 0..rows {
            if index % 200 == 0 {
                context.check_killed()?;
            }
            let row = chunk.get_row(index);
            let valid = row.get_bytes(1)[0] != 0;
            let (hash, part) = if valid {
                let hash = rehash(row.get_uint64(0));
                (hash, context.partition.partition_index(hash))
            } else {
                let part = fake_part;
                fake_part = (fake_part + 1) % self.partition_number;
                (part as u64, part)
            };
            self.part_idx_vector[index] = part;
            self.hash_value[index] = hash;
            let helper = &mut self.helpers[part];
            helper.total_row_num += 1;
            helper.valid_row_num += i64::from(valid);
            helper.raw_data_len += row.get_bytes(2).len() as i64;
        }
        context.consume(
            self.helpers
                .iter()
                .map(|helper| {
                    helper.raw_data_len
                        + helper.total_row_num * 2 * UINT64_LEN as i64
                        + helper.valid_row_num * INT_LEN as i64
                        + helper.total_row_num * context.meta.col_offset_in_null_map as i64
                })
                .sum(),
        );
        let mut segments: Vec<_> = self
            .helpers
            .iter()
            .map(|helper| {
                let mut segment = RowTableSegment::new();
                segment.raw_data = Vec::with_capacity(helper.raw_data_len as usize);
                segment.hash_values = Vec::with_capacity(helper.total_row_num as usize);
                segment.row_start_offset = Vec::with_capacity(helper.total_row_num as usize);
                segment.valid_join_key_pos = Vec::with_capacity(helper.valid_row_num as usize);
                if context.meta.col_offset_in_null_map != 0 {
                    segment.allocate_used_flags(helper.total_row_num as usize);
                }
                segment
            })
            .collect();
        for index in 0..rows {
            if index % 200 == 0 {
                context.check_killed()?;
            }
            let row = chunk.get_row(index);
            let segment = &mut segments[self.part_idx_vector[index]];
            if row.get_bytes(1)[0] != 0 {
                segment.valid_join_key_pos.push(segment.hash_values.len());
            }
            segment.hash_values.push(self.hash_value[index]);
            segment.row_start_offset.push(segment.raw_data.len() as u64);
            segment.raw_data.extend_from_slice(&row.get_bytes(2));
        }
        for segment in &mut segments {
            segment.finalize();
        }
        Ok(segments)
    }

    /// `calculateSerializedKeyAndKeyLength`.
    fn calculate_serialized_key_and_key_length(
        &self,
        meta: &RowLayoutMeta,
        has_valid_key: bool,
        logical_row_index: usize,
    ) -> i64 {
        let mut append_row_length = 0_i64;
        if !meta.is_join_keys_fixed_length {
            append_row_length += SIZE_OF_ELEMENT_SIZE as i64;
        }
        if !meta.is_join_keys_inlined {
            if has_valid_key {
                append_row_length +=
                    self.serialized_key_vector_buffer[logical_row_index].len() as i64;
            } else if meta.is_join_keys_fixed_length {
                append_row_length += meta.join_keys_length as i64;
            }
        }
        append_row_length
    }

    /// `fillSerializedKeyAndKeyLengthIfNeeded`.
    fn fill_serialized_key_and_key_length_if_needed(
        &self,
        meta: &RowLayoutMeta,
        has_valid_key: bool,
        logical_row_index: usize,
        segment: &mut RowTableSegment,
    ) -> i64 {
        let mut append_row_length = 0_i64;
        if !meta.is_join_keys_fixed_length {
            let length = if has_valid_key {
                self.serialized_key_vector_buffer[logical_row_index].len() as u32
            } else {
                0
            };
            segment.raw_data.extend_from_slice(&length.to_le_bytes());
            append_row_length += SIZE_OF_ELEMENT_SIZE as i64;
        }
        if !meta.is_join_keys_inlined {
            if has_valid_key {
                let key = &self.serialized_key_vector_buffer[logical_row_index];
                segment.raw_data.extend_from_slice(key);
                append_row_length += key.len() as i64;
            } else if meta.is_join_keys_fixed_length {
                segment.raw_data.extend_from_slice(&meta.fake_key_byte);
                append_row_length += meta.join_keys_length as i64;
            }
        }
        append_row_length
    }

    /// `preAllocForSegments`.
    fn pre_alloc_for_segments(
        &mut self,
        segments: &mut [RowTableSegment],
        chunk: &Chunk,
        context: &mut BuildContext<'_>,
    ) -> Result<(), RowTableBuildError> {
        for helper in &mut self.helpers {
            helper.reset();
        }
        let meta = context.meta;
        for logical_row_index in 0..self.used_rows.len() {
            let physical_row_index = self.used_rows[logical_row_index];
            let has_valid_key = self.has_valid_key(physical_row_index);
            if !has_valid_key && !self.keep_filtered_rows {
                continue;
            }
            let part_idx = self.part_idx_vector[logical_row_index];
            self.helpers[part_idx].total_row_num += 1;
            if has_valid_key {
                self.helpers[part_idx].valid_row_num += 1;
            }
            let mut row_length = FAKE_ADDR_PLACE_HOLDER_LEN as i64 + meta.null_map_length as i64;
            row_length += self.calculate_serialized_key_and_key_length(
                meta,
                has_valid_key,
                logical_row_index,
            );
            row_length += calculate_row_data_length(meta, chunk, logical_row_index);
            row_length += calculate_fake_length(row_length);
            self.helpers[part_idx].raw_data_len += row_length;
        }

        let mut total_mem_usage = 0_i64;
        for helper in &self.helpers {
            total_mem_usage += helper.raw_data_len
                + (helper.total_row_num + helper.total_row_num) * UINT64_LEN as i64
                + helper.valid_row_num * INT_LEN as i64
                + helper.total_row_num * meta.col_offset_in_null_map as i64;
        }
        context.consume(total_mem_usage);
        context.check_killed()?;

        for (part_idx, segment) in segments.iter_mut().enumerate() {
            let helper = self.helpers[part_idx];
            segment.raw_data = Vec::with_capacity(helper.raw_data_len as usize);
            segment.hash_values = Vec::with_capacity(helper.total_row_num as usize);
            segment.row_start_offset = Vec::with_capacity(helper.total_row_num as usize);
            segment.valid_join_key_pos = Vec::with_capacity(helper.valid_row_num as usize);
            if meta.col_offset_in_null_map != 0 {
                segment.allocate_used_flags(helper.total_row_num as usize);
            }
        }
        Ok(())
    }

    /// `appendToRowTable`: writes every kept row of the chunk.
    fn append_to_row_table(
        &mut self,
        chunk: &Chunk,
        context: &mut BuildContext<'_>,
    ) -> Result<Vec<RowTableSegment>, RowTableBuildError> {
        let mut segments: Vec<RowTableSegment> = (0..self.partition_number)
            .map(|_| RowTableSegment::new())
            .collect();
        self.pre_alloc_for_segments(&mut segments, chunk, context)?;

        let meta = context.meta;
        for logical_row_index in 0..self.used_rows.len() {
            // Go appendToRowTable checks every ten rows and the final row.
            if logical_row_index % 10 == 0 || logical_row_index + 1 == self.used_rows.len() {
                context.check_killed()?;
            }
            let physical_row_index = self.used_rows[logical_row_index];
            let has_valid_key = self.has_valid_key(physical_row_index);
            if !has_valid_key && !self.keep_filtered_rows {
                continue;
            }
            let part_idx = self.part_idx_vector[logical_row_index];
            let segment = &mut segments[part_idx];

            if has_valid_key {
                segment.valid_join_key_pos.push(segment.hash_values.len());
            }
            segment.hash_values.push(self.hash_value[logical_row_index]);
            segment.row_start_offset.push(segment.raw_data.len() as u64);

            let mut row_length = 0_i64;
            row_length += fill_next_row_ptr(segment) as i64;
            row_length +=
                fill_null_map(meta, chunk, logical_row_index, segment, &mut self.null_map) as i64;
            row_length += self.fill_serialized_key_and_key_length_if_needed(
                meta,
                has_valid_key,
                logical_row_index,
                segment,
            );
            row_length += fill_row_data(meta, chunk, logical_row_index, segment);
            if row_length % 8 != 0 {
                let padding = 8 - (row_length % 8) as usize;
                segment
                    .raw_data
                    .extend_from_slice(&FAKE_ADDR_PLACE_HOLDER[..padding]);
            }
        }
        for segment in &mut segments {
            segment.finalize();
        }
        Ok(segments)
    }
}

/// `fillNextRowPtr`: reserves the 8-byte chain slot at the row start.
fn fill_next_row_ptr(segment: &mut RowTableSegment) -> usize {
    segment.raw_data.extend_from_slice(&FAKE_ADDR_PLACE_HOLDER);
    FAKE_ADDR_PLACE_HOLDER_LEN
}

/// `fillNullMap`: writes one bit per stored column, MSB first inside a byte.
fn fill_null_map(
    meta: &RowLayoutMeta,
    chunk: &Chunk,
    logical_row_index: usize,
    segment: &mut RowTableSegment,
    bitmap: &mut [u8],
) -> usize {
    let null_map_length = meta.null_map_length;
    if null_map_length == 0 {
        return 0;
    }
    bitmap[..null_map_length].fill(0);
    for (col_index_in_row_table, &col_index_in_row) in meta.row_columns_order.iter().enumerate() {
        let col_index_in_bitmap = col_index_in_row_table + meta.col_offset_in_null_map;
        if chunk.get_row(logical_row_index).is_null(col_index_in_row) {
            bitmap[col_index_in_bitmap / 8] |= 1 << (7 - col_index_in_bitmap % 8);
        }
    }
    segment
        .raw_data
        .extend_from_slice(&bitmap[..null_map_length]);
    null_map_length
}

/// `fillRowData`: fixed columns raw, variable columns length-prefixed.
fn fill_row_data(
    meta: &RowLayoutMeta,
    chunk: &Chunk,
    logical_row_index: usize,
    segment: &mut RowTableSegment,
) -> i64 {
    let mut append_row_length = 0_i64;
    for (index, &col_idx) in meta.row_columns_order.iter().enumerate() {
        let raw = chunk.get_row(logical_row_index).get_raw(col_idx);
        if let Some(size) = meta.columns_size[index] {
            segment.raw_data.extend_from_slice(&raw);
            append_row_length += size as i64;
        } else {
            let length = raw.len() as u32;
            segment.raw_data.extend_from_slice(&length.to_le_bytes());
            segment.raw_data.extend_from_slice(&raw);
            append_row_length += i64::from(length) + SIZE_OF_ELEMENT_SIZE as i64;
        }
    }
    append_row_length
}

/// `calculateRowDataLength`.
fn calculate_row_data_length(meta: &RowLayoutMeta, chunk: &Chunk, logical_row_index: usize) -> i64 {
    let mut append_row_length = 0_i64;
    for (index, &col_idx) in meta.row_columns_order.iter().enumerate() {
        if let Some(size) = meta.columns_size[index] {
            append_row_length += size as i64;
        } else {
            append_row_length += chunk.get_row(logical_row_index).get_raw(col_idx).len() as i64
                + SIZE_OF_ELEMENT_SIZE as i64;
        }
    }
    append_row_length
}

/// `calculateFakeLength`: padding that rounds a row up to 8 bytes.
#[must_use]
pub const fn calculate_fake_length(row_length: i64) -> i64 {
    (8 - row_length % 8) % 8
}
