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
//! * [`BuildChunk`] stands in for `util/chunk.Chunk`. It keeps the parts the
//!   builder reads -- a `sel` vector, per-column fixed width, per-row raw
//!   bytes, and per-row nullness -- but stores nullness as `Vec<bool>`
//!   instead of chunk's packed null bitmap, and is not `tidb-chunk`.
//! * serialized join keys arrive from a [`KeySerializer`] rather than
//!   `util/codec.SerializeKeys`, and the build filter from a closure rather
//!   than `expression.VectorizedFilter`. Both live in other Go packages; the
//!   builder's own contract is where their bytes land in a row.
//! * `hashJoinCtx.hashTableContext.memoryTracker.Consume` becomes
//!   [`BuildContext::consumed_memory`], and `checkSQLKiller` is dropped: it
//!   is cancellation, not row layout.
//! * `resizeSlice` is not ported; `Vec::resize` has the same reuse-or-grow
//!   behavior.
//! * Spill restore (`preAllocForSegmentsInSpill`,
//!   `processOneRestoredChunk`) is not ported, because it reads back chunks
//!   written by `hash_join_spill*.go`, which is outside this seed.

use tidb_util::serialization::{INT_LEN, UINT64_LEN};

use crate::join_row_table::{
    RowLayoutMeta, RowTableSegment, FAKE_ADDR_PLACE_HOLDER, FAKE_ADDR_PLACE_HOLDER_LEN,
    SIZE_OF_ELEMENT_SIZE,
};

/// Default `sel` length the source pre-builds for chunks without one.
pub const FAKE_SEL_LENGTH: usize = 4096;

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

/// One build-side column, modeling `util/chunk.Column`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildColumn {
    fixed_size: Option<usize>,
    data: Vec<u8>,
    offsets: Vec<usize>,
    nulls: Vec<bool>,
}

impl BuildColumn {
    /// Creates a fixed-width column from equally sized per-row values.
    ///
    /// # Panics
    ///
    /// Panics when a value's width differs from `fixed_size`.
    #[must_use]
    pub fn fixed(fixed_size: usize, values: &[(Vec<u8>, bool)]) -> Self {
        let mut data = Vec::with_capacity(fixed_size * values.len());
        let mut nulls = Vec::with_capacity(values.len());
        for (value, is_null) in values {
            assert_eq!(value.len(), fixed_size, "fixed column width mismatch");
            data.extend_from_slice(value);
            nulls.push(*is_null);
        }
        Self {
            fixed_size: Some(fixed_size),
            data,
            offsets: Vec::new(),
            nulls,
        }
    }

    /// Creates a variable-width column from per-row values.
    #[must_use]
    pub fn variable(values: &[(Vec<u8>, bool)]) -> Self {
        let mut data = Vec::new();
        let mut offsets = Vec::with_capacity(values.len() + 1);
        let mut nulls = Vec::with_capacity(values.len());
        offsets.push(0);
        for (value, is_null) in values {
            data.extend_from_slice(value);
            offsets.push(data.len());
            nulls.push(*is_null);
        }
        Self {
            fixed_size: None,
            data,
            offsets,
            nulls,
        }
    }

    /// Fixed width of this column, `None` when values vary in width.
    #[must_use]
    pub const fn fixed_size(&self) -> Option<usize> {
        self.fixed_size
    }

    /// Number of physical rows.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.nulls.len()
    }

    /// Raw bytes of a physical row.
    #[must_use]
    pub fn get_raw(&self, row: usize) -> &[u8] {
        match self.fixed_size {
            Some(size) => &self.data[row * size..(row + 1) * size],
            None => &self.data[self.offsets[row]..self.offsets[row + 1]],
        }
    }

    /// Byte length of a physical row's value.
    #[must_use]
    pub fn get_raw_len(&self, row: usize) -> usize {
        self.get_raw(row).len()
    }

    /// Whether a physical row is null.
    #[must_use]
    pub fn is_null(&self, row: usize) -> bool {
        self.nulls[row]
    }

    /// Whether any element is too large for the 4-byte size prefix.
    #[must_use]
    pub fn contains_very_large_element(&self) -> bool {
        if self.fixed_size.is_some() {
            return false;
        }
        (0..self.rows()).any(|row| self.get_raw_len(row) > u32::MAX as usize)
    }
}

/// A build-side chunk, modeling `util/chunk.Chunk`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildChunk {
    columns: Vec<BuildColumn>,
    sel: Option<Vec<usize>>,
}

impl BuildChunk {
    /// Creates a chunk from its columns, with no selection vector.
    #[must_use]
    pub fn new(columns: Vec<BuildColumn>) -> Self {
        Self { columns, sel: None }
    }

    /// Installs a selection vector of physical row indices.
    pub fn set_sel(&mut self, sel: Vec<usize>) {
        self.sel = Some(sel);
    }

    /// Borrows the selection vector.
    #[must_use]
    pub fn sel(&self) -> Option<&[usize]> {
        self.sel.as_deref()
    }

    /// Borrows one column.
    #[must_use]
    pub fn column(&self, index: usize) -> &BuildColumn {
        &self.columns[index]
    }

    /// Number of columns.
    #[must_use]
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    /// Number of logical rows, honoring the selection vector.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        match &self.sel {
            Some(sel) => sel.len(),
            None => self.columns.first().map_or(0, BuildColumn::rows),
        }
    }

    /// Physical row index behind a logical one.
    #[must_use]
    pub fn physical_row(&self, logical_row: usize) -> usize {
        match &self.sel {
            Some(sel) => sel[logical_row],
            None => logical_row,
        }
    }

    /// Raw bytes of a column in a logical row.
    #[must_use]
    pub fn get_raw(&self, logical_row: usize, column_index: usize) -> &[u8] {
        self.columns[column_index].get_raw(self.physical_row(logical_row))
    }

    /// Whether a column is null in a logical row.
    #[must_use]
    pub fn is_null(&self, logical_row: usize, column_index: usize) -> bool {
        self.columns[column_index].is_null(self.physical_row(logical_row))
    }
}

/// Produces the serialized join key of a logical row.
///
/// The source gets these from `util/codec.SerializeKeys`, which belongs to
/// another Go package; this seam keeps its output without pulling it in.
pub trait KeySerializer {
    /// Serializes the join key of one logical row.
    fn serialize(&self, chunk: &BuildChunk, logical_row_index: usize) -> Vec<u8>;
}

impl<F: Fn(&BuildChunk, usize) -> Vec<u8>> KeySerializer for F {
    fn serialize(&self, chunk: &BuildChunk, logical_row_index: usize) -> Vec<u8> {
        self(chunk, logical_row_index)
    }
}

/// Build filter over one physical row, standing in for
/// `expression.VectorizedFilter`'s per-row result.
pub type BuildFilter<'a> = &'a dyn Fn(&BuildChunk, usize) -> bool;

/// Everything outside the builder that one `processOneChunk` call reads.
pub struct BuildContext<'a> {
    /// Row layout the emitted bytes must follow.
    pub meta: &'a RowLayoutMeta,
    /// Partition geometry for this join.
    pub partition: PartitionInfo,
    /// Serializer for join keys.
    pub key_serializer: &'a dyn KeySerializer,
    /// Build filter, evaluated per physical row; `None` keeps every row.
    pub build_filter: Option<BuildFilter<'a>>,
    /// Running total of the source's hash-table memory tracker.
    pub consumed_memory: i64,
}

impl<'a> BuildContext<'a> {
    /// Creates a context with an empty memory tracker and no filter.
    #[must_use]
    pub fn new(
        meta: &'a RowLayoutMeta,
        partition: PartitionInfo,
        key_serializer: &'a dyn KeySerializer,
    ) -> Self {
        Self {
            meta,
            partition,
            key_serializer,
            build_filter: None,
            consumed_memory: 0,
        }
    }
}

/// Errors the source returns when an element cannot be length-prefixed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowTableBuildError {
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
    pub serialized_key_vector_buffer: Vec<Vec<u8>>,
    /// Partition index of each logical row.
    pub part_idx_vector: Vec<usize>,
    /// Physical row index of each logical row.
    pub used_rows: Vec<usize>,
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
            serialized_key_vector_buffer: Vec::new(),
            part_idx_vector: Vec::new(),
            used_rows: Vec::new(),
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
    pub fn reset_buffer(&mut self, chunk: &BuildChunk) {
        self.used_rows = match chunk.sel() {
            Some(sel) => sel.to_vec(),
            None => (0..chunk.num_rows()).collect(),
        };
        let logical_rows = chunk.num_rows();
        let physical_rows = chunk.column(0).rows();

        self.part_idx_vector.resize(logical_rows, 0);
        self.part_idx_vector.truncate(logical_rows);
        self.hash_value.resize(logical_rows, 0);
        self.hash_value.truncate(logical_rows);
        if self.has_filter {
            self.filter_vector = Some(vec![false; physical_rows]);
        }
        if self.has_nullable_key {
            self.null_key_vector = Some(vec![false; physical_rows]);
        }
        self.serialized_key_vector_buffer.clear();
        self.serialized_key_vector_buffer
            .resize(logical_rows, Vec::new());
    }

    /// `checkMaxElementSize`.
    fn check_max_element_size(&self, chunk: &BuildChunk, meta: &RowLayoutMeta) -> Option<usize> {
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
        chunk: &BuildChunk,
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
            let vector = self
                .filter_vector
                .as_mut()
                .expect("filter vector exists when a filter runs");
            for (physical_row_index, kept) in vector.iter_mut().enumerate() {
                *kept = filter(chunk, physical_row_index);
            }
        }

        // `codec.SerializeKeys` reports null keys through `nullKeyVector` and
        // leaves the serialized key of a rejected row empty.
        if self.has_nullable_key {
            let mut vector = self
                .null_key_vector
                .take()
                .expect("null key vector exists for a nullable key");
            for &physical_row_index in &self.used_rows {
                vector[physical_row_index] = self
                    .build_key_index
                    .iter()
                    .any(|&column_index| chunk.column(column_index).is_null(physical_row_index));
            }
            self.null_key_vector = Some(vector);
        }
        for logical_row_index in 0..self.used_rows.len() {
            if !self.has_valid_key(self.used_rows[logical_row_index]) {
                continue;
            }
            self.serialized_key_vector_buffer[logical_row_index] =
                context.key_serializer.serialize(chunk, logical_row_index);
        }
        if self
            .serialized_key_vector_buffer
            .iter()
            .any(|key| key.len() > u32::MAX as usize)
        {
            return Err(RowTableBuildError::JoinKeyTooLarge);
        }

        self.init_hash_value_and_part_index_for_one_chunk(context.partition);
        Ok(self.append_to_row_table(chunk, context))
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
        chunk: &BuildChunk,
        context: &mut BuildContext<'_>,
    ) {
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
                + helper.valid_row_num * INT_LEN as i64;
        }
        context.consumed_memory += total_mem_usage;

        for (part_idx, segment) in segments.iter_mut().enumerate() {
            let helper = self.helpers[part_idx];
            segment.raw_data = Vec::with_capacity(helper.raw_data_len as usize);
            segment.hash_values = Vec::with_capacity(helper.total_row_num as usize);
            segment.row_start_offset = Vec::with_capacity(helper.total_row_num as usize);
            segment.valid_join_key_pos = Vec::with_capacity(helper.valid_row_num as usize);
        }
    }

    /// `appendToRowTable`: writes every kept row of the chunk.
    fn append_to_row_table(
        &mut self,
        chunk: &BuildChunk,
        context: &mut BuildContext<'_>,
    ) -> Vec<RowTableSegment> {
        let mut segments: Vec<RowTableSegment> = (0..self.partition_number)
            .map(|_| RowTableSegment::new())
            .collect();
        self.pre_alloc_for_segments(&mut segments, chunk, context);

        let meta = context.meta;
        for logical_row_index in 0..self.used_rows.len() {
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
        segments
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
    chunk: &BuildChunk,
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
        if chunk.is_null(logical_row_index, col_index_in_row) {
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
    chunk: &BuildChunk,
    logical_row_index: usize,
    segment: &mut RowTableSegment,
) -> i64 {
    let mut append_row_length = 0_i64;
    for (index, &col_idx) in meta.row_columns_order.iter().enumerate() {
        let raw = chunk.get_raw(logical_row_index, col_idx);
        if let Some(size) = meta.columns_size[index] {
            segment.raw_data.extend_from_slice(raw);
            append_row_length += size as i64;
        } else {
            let length = raw.len() as u32;
            segment.raw_data.extend_from_slice(&length.to_le_bytes());
            segment.raw_data.extend_from_slice(raw);
            append_row_length += i64::from(length) + SIZE_OF_ELEMENT_SIZE as i64;
        }
    }
    append_row_length
}

/// `calculateRowDataLength`.
fn calculate_row_data_length(
    meta: &RowLayoutMeta,
    chunk: &BuildChunk,
    logical_row_index: usize,
) -> i64 {
    let mut append_row_length = 0_i64;
    for (index, &col_idx) in meta.row_columns_order.iter().enumerate() {
        if let Some(size) = meta.columns_size[index] {
            append_row_length += size as i64;
        } else {
            append_row_length += chunk.get_raw(logical_row_index, col_idx).len() as i64
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
