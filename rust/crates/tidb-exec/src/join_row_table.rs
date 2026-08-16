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

//! Go `pkg/executor/join` hash-join row storage, covering
//! `join_row_table.go`.
//!
//! SEED of `pkg/executor/join`: this file, [`crate::row_table_builder`]
//! (`row_table_builder.go`), [`crate::hash_table_v2`] (`hash_table_v2.go`),
//! [`crate::tagged_ptr`] (`tagged_ptr.go`), [`crate::concurrent_entry_map`]
//! (`concurrent_map.go`), and [`crate::join_table_meta`]
//! (`join_table_meta.go`) are ported; the probe side (`base_join_probe.go`,
//! `*_join_probe.go`), spill (`hash_join_spill*.go`), and the executors
//! (`hash_join_v1.go`, `hash_join_v2.go`, `merge_join.go`,
//! `index_lookup_*.go`) are not.
//!
//! ## Row layout
//!
//! Byte-for-byte the source layout, on little-endian 64-bit targets:
//!
//! ```text
//! |  next_row_ptr  |  null_map  |  key_length / serialized_key  |  row_data  |
//! |     8 bytes    |  optional  |           optional            |  variable  |
//! ```
//!
//! What is LAYOUT-IDENTICAL to Go:
//!
//! * every offset and width above: [`SIZE_OF_NEXT_PTR`] = 8,
//!   [`SIZE_OF_ELEMENT_SIZE`] = 4, null-map bytes, the 4-byte little-endian
//!   `key_length` prefix, the per-column `length + raw` encoding of variable
//!   columns, and the 8-byte row alignment padding;
//! * the null-map bit order (`bit = 1 << (7 - index % 8)` inside byte
//!   `index / 8`) and the used-flag bit;
//! * [`BIT_MASK_IN_UINT32`], derived by the same endianness-dependent
//!   formula the source runs in `init`;
//! * the `next_row_ptr` slot: 8 little-endian bytes holding a tagged address,
//!   read and written exactly where Go reads and writes a `taggedPtr`.
//!
//! What is only OBSERVABLY EQUIVALENT (the workspace forbids `unsafe`):
//!
//! * a row "pointer" is a `usize` drawn from a process-wide synthetic address
//!   space ([`allocate_row_address_range`]) instead of a real heap address.
//!   Addresses are unique, 8-byte aligned, monotone within a segment, and
//!   have enough leading zeros to carry a 24-bit tag -- the four properties
//!   the source relies on. They cannot be dereferenced; reads go through
//!   [`RowTable::segment_of_address`].
//! * `totalUsedBytes` uses Rust `Vec::capacity`, not Go slice `cap`, so the
//!   accounting is the same formula over a different allocator's growth.
//! * Go's `runtime.heapObjectsCanMove` guard has no analogue: `Vec` contents
//!   never move without `&mut`, and no address here outlives its table.

use std::sync::atomic::{AtomicUsize, Ordering};

use tidb_util::serialization::{INT_LEN, UINT64_LEN};

use crate::join_table_meta::{JoinTableMeta, KeyMode};
use crate::tagged_ptr::{get_tagged_bits_from_usize, TagPtrHelper};

/// Width of the `next_row_ptr` field at the start of every row.
pub const SIZE_OF_NEXT_PTR: usize = size_of::<u64>();
/// Width of a variable-length element's size prefix.
pub const SIZE_OF_ELEMENT_SIZE: usize = size_of::<u32>();
/// Width of Go's `unsafe.Pointer`, i.e. this target's pointer width.
pub const SIZE_OF_UNSAFE_POINTER: usize = size_of::<usize>();
/// Width of Go's `uintptr`, i.e. this target's address width.
pub const SIZE_OF_UINTPTR: usize = size_of::<usize>();

/// Placeholder bytes written where a row address will later be stored.
pub const FAKE_ADDR_PLACE_HOLDER: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
/// Length of [`FAKE_ADDR_PLACE_HOLDER`].
pub const FAKE_ADDR_PLACE_HOLDER_LEN: usize = FAKE_ADDR_PLACE_HOLDER.len();

/// Builds the per-bit masks used to reach a null-map bit through a `u32`.
///
/// The source picks between these two arrangements at `init` time from a
/// runtime endianness probe; here the target's endianness is known at compile
/// time, so [`BIT_MASK_IN_UINT32`] selects the same arm statically.
#[must_use]
pub const fn initialize_bit_masks(is_little_endian: bool) -> [u32; 32] {
    let mut masks = [0_u32; 32];
    let mut index = 0_usize;
    while index < 32 {
        let shift = if is_little_endian {
            7 - (index % 8) + (index / 8) * 8
        } else {
            31 - index
        };
        masks[index] = 1_u32 << shift;
        index += 1;
    }
    masks
}

/// Mask reaching null-map bit `i` when the map is loaded as a `u32`.
pub const BIT_MASK_IN_UINT32: [u32; 32] = initialize_bit_masks(cfg!(target_endian = "little"));

/// Mask of the used flag, which occupies null-map bit 0.
pub const USED_FLAG_MASK: u32 = BIT_MASK_IN_UINT32[0];

/// First address of the synthetic row address space.
///
/// Nonzero so that a zero address keeps its source meaning of "no row", and
/// 8-byte aligned so that row addresses inherit the source's alignment.
pub const ROW_ADDRESS_SPACE_BASE: usize = 0x1_0000;

static NEXT_ROW_ADDRESS: AtomicUsize = AtomicUsize::new(ROW_ADDRESS_SPACE_BASE);

/// Reserves `length` bytes of synthetic address space for one segment.
///
/// Ranges never overlap and never repeat within a process, which is what lets
/// tests key a set on row addresses the way the source keys on
/// `unsafe.Pointer`.
#[must_use]
pub fn allocate_row_address_range(length: usize) -> usize {
    let size = length.max(1).next_multiple_of(8);
    NEXT_ROW_ADDRESS.fetch_add(size, Ordering::Relaxed)
}

/// One contiguous chunk of hash-join rows.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowTableSegment {
    /// The chunk of memory that stores the rows.
    pub raw_data: Vec<u8>,
    /// Hash value of every row, indexed by row position in this segment.
    pub hash_values: Vec<u64>,
    /// Start offset of every row inside [`Self::raw_data`].
    pub row_start_offset: Vec<u64>,
    /// Positions of the rows that carry a valid join key.
    pub valid_join_key_pos: Vec<usize>,
    tagged_bits: u8,
    base_address: usize,
}

impl RowTableSegment {
    /// Creates an empty segment, as the source's `newRowTableSegment` does.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Assigns this segment's address range and derives its tag width.
    ///
    /// Called once, after the segment stops growing. The source does the same
    /// two steps when a finished segment is handed to the hash-table context.
    pub fn finalize(&mut self) {
        self.base_address = allocate_row_address_range(self.raw_data.len());
        self.init_tagged_bits();
    }

    /// Recomputes the tag width from this segment's first and last row.
    pub fn init_tagged_bits(&mut self) {
        if self.row_start_offset.is_empty() {
            self.tagged_bits = 0;
            return;
        }
        let start_ptr = self.get_row_pointer(0);
        let end_ptr = self.get_row_pointer(self.row_start_offset.len() - 1);
        self.tagged_bits = get_tagged_bits_from_usize(end_ptr | start_ptr);
    }

    /// Number of high bits usable as a tag for every row in this segment.
    #[must_use]
    pub const fn tagged_bits(&self) -> u8 {
        self.tagged_bits
    }

    /// Base address of this segment's synthetic address range.
    #[must_use]
    pub const fn base_address(&self) -> usize {
        self.base_address
    }

    /// Bytes retained by this segment's four buffers.
    #[must_use]
    pub fn total_used_bytes(&self) -> i64 {
        let mut ret = self.raw_data.capacity() as i64;
        ret += (self.hash_values.capacity() * UINT64_LEN) as i64;
        ret += (self.row_start_offset.capacity() * UINT64_LEN) as i64;
        ret += (self.valid_join_key_pos.capacity() * INT_LEN) as i64;
        ret
    }

    /// Address of row `index`.
    ///
    /// # Panics
    ///
    /// Panics when `index` is out of range, matching the source's slice
    /// bounds check.
    #[must_use]
    pub fn get_row_pointer(&self, index: usize) -> usize {
        self.base_address + usize::try_from(self.row_start_offset[index]).expect("row offset")
    }

    /// Number of rows that have a start offset.
    #[must_use]
    pub fn row_count(&self) -> i64 {
        self.row_start_offset.len() as i64
    }

    /// Number of rows with a valid join key.
    #[must_use]
    pub fn valid_key_count(&self) -> u64 {
        self.valid_join_key_pos.len() as u64
    }

    /// Number of hashed rows.
    #[must_use]
    pub fn get_row_num(&self) -> usize {
        self.hash_values.len()
    }

    /// Bytes of row `idx`, running to the next row or to the segment end.
    #[must_use]
    pub fn get_row_bytes(&self, idx: usize) -> &[u8] {
        let row_num = self.get_row_num();
        let start = usize::try_from(self.row_start_offset[idx]).expect("row offset");
        if idx == row_num - 1 {
            &self.raw_data[start..]
        } else {
            let end = usize::try_from(self.row_start_offset[idx + 1]).expect("row offset");
            &self.raw_data[start..end]
        }
    }

    /// Offset of `address` inside this segment, when it lands here.
    #[must_use]
    pub fn offset_of_address(&self, address: usize) -> Option<usize> {
        if address < self.base_address || address >= self.base_address + self.raw_data.len() {
            return None;
        }
        Some(address - self.base_address)
    }

    /// Writes the tagged address of the next row in this row's chain.
    ///
    /// Layout-identical to the source's `setNextRowAddress`: 8 little-endian
    /// bytes at the very start of the row.
    pub fn set_next_row_address(&mut self, row_offset: usize, next_row_address: usize) {
        self.raw_data[row_offset..row_offset + SIZE_OF_NEXT_PTR]
            .copy_from_slice(&(next_row_address as u64).to_le_bytes());
    }

    /// Reads the raw tagged address stored in this row's `next_row_ptr`.
    #[must_use]
    pub fn raw_next_row_address(&self, row_offset: usize) -> usize {
        let mut bytes = [0_u8; SIZE_OF_NEXT_PTR];
        bytes.copy_from_slice(&self.raw_data[row_offset..row_offset + SIZE_OF_NEXT_PTR]);
        u64::from_le_bytes(bytes) as usize
    }
}

/// Reads the next row in a chain, honoring the source's tag short-circuit.
///
/// Returns `0` when the stored tag cannot match `hash_value`, exactly as the
/// source's `getNextRowAddress` does.
#[must_use]
pub fn next_row_address(raw: usize, tag_helper: &TagPtrHelper, hash_value: u64) -> usize {
    let hash_tag_value = tag_helper.get_tagged_value(hash_value);
    if (raw as u64) & hash_tag_value != hash_tag_value {
        return 0;
    }
    raw
}

/// A hash-join build-side row table, a list of segments.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowTable {
    /// The segments that make up this table, in append order.
    pub segments: Vec<RowTableSegment>,
}

impl RowTable {
    /// Creates an empty row table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes retained by every segment.
    #[must_use]
    pub fn get_total_memory_usage(&self) -> i64 {
        self.segments
            .iter()
            .map(RowTableSegment::total_used_bytes)
            .sum()
    }

    /// Borrows the segments.
    #[must_use]
    pub fn get_segments(&self) -> &[RowTableSegment] {
        &self.segments
    }

    /// Drops every segment, releasing the row memory.
    pub fn clear_segments(&mut self) {
        self.segments.clear();
    }

    /// Appends another table's segments to this one.
    pub fn merge(&mut self, other: Self) {
        self.segments.extend(other.segments);
    }

    /// Total number of rows across all segments.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.row_count() as u64)
            .sum()
    }

    /// Total number of rows with a valid join key.
    #[must_use]
    pub fn valid_key_count(&self) -> u64 {
        self.segments
            .iter()
            .map(RowTableSegment::valid_key_count)
            .sum()
    }

    /// Address of the `row_index`-th row across segments, used by tests.
    ///
    /// `None` stands in for the source's `nil` return.
    #[must_use]
    pub fn get_row_pointer(&self, row_index: usize) -> Option<usize> {
        let mut row_index = row_index;
        for segment in &self.segments {
            if row_index < segment.row_start_offset.len() {
                return Some(segment.get_row_pointer(row_index));
            }
            row_index -= segment.row_start_offset.len();
        }
        None
    }

    /// Row position of the `row_index`-th valid join key, or `-1`.
    #[must_use]
    pub fn get_valid_join_key_pos(&self, row_index: usize) -> isize {
        let mut row_index = row_index;
        let mut start_offset = 0_usize;
        for segment in &self.segments {
            if row_index < segment.valid_join_key_pos.len() {
                return (start_offset + segment.valid_join_key_pos[row_index]) as isize;
            }
            row_index -= segment.valid_join_key_pos.len();
            start_offset += segment.row_start_offset.len();
        }
        -1
    }

    /// Locates the segment and in-segment offset that own `address`.
    #[must_use]
    pub fn segment_of_address(&self, address: usize) -> Option<(usize, usize)> {
        self.segments
            .iter()
            .enumerate()
            .find_map(|(index, segment)| {
                segment
                    .offset_of_address(address)
                    .map(|offset| (index, offset))
            })
    }

    /// Bytes of the row that starts at `address`, to the end of its segment.
    #[must_use]
    pub fn row_bytes_at(&self, address: usize) -> Option<&[u8]> {
        let (segment_index, offset) = self.segment_of_address(address)?;
        Some(&self.segments[segment_index].raw_data[offset..])
    }

    /// Follows one link of a row chain starting at `address`.
    #[must_use]
    pub fn next_row_address(
        &self,
        address: usize,
        tag_helper: &TagPtrHelper,
        hash_value: u64,
    ) -> usize {
        let Some((segment_index, offset)) = self.segment_of_address(address) else {
            return 0;
        };
        next_row_address(
            self.segments[segment_index].raw_next_row_address(offset),
            tag_helper,
            hash_value,
        )
    }
}

/// The row-layout half of `join_table_meta.go`'s `joinTableMeta`.
///
/// [`JoinTableMeta`] models the metadata *decisions*; this carries the fields
/// a row reader or writer needs to walk the bytes those decisions produce.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowLayoutMeta {
    /// Bytes reserved for the null map, zero when it is not needed.
    pub null_map_length: usize,
    /// Null-map bit offset of column 0: `1` with a used flag, else `0`.
    pub col_offset_in_null_map: usize,
    /// Build-column indices in row-storage order.
    pub row_columns_order: Vec<usize>,
    /// Fixed width of each saved column, `None` for variable width.
    pub columns_size: Vec<Option<usize>>,
    /// Whether the join key is stored inside `row_data`.
    pub is_join_keys_inlined: bool,
    /// Whether serialized join keys have a fixed width.
    pub is_join_keys_fixed_length: bool,
    /// Serialized key width when fixed.
    pub join_keys_length: usize,
    /// Filler key written for invalid-key rows with a fixed-width key.
    pub fake_key_byte: Vec<u8>,
    /// Physical key representation.
    pub key_mode: KeyMode,
}

impl RowLayoutMeta {
    /// Derives the row layout from ported metadata plus per-column widths.
    ///
    /// `columns_size` is indexed like [`JoinTableMeta::row_columns_order`],
    /// mirroring the source's `meta.columnsSize`.
    #[must_use]
    pub fn from_join_table_meta(meta: &JoinTableMeta, columns_size: Vec<Option<usize>>) -> Self {
        let join_keys_length = usize::try_from(meta.join_keys_length).unwrap_or(0);
        Self {
            null_map_length: meta.null_map_length,
            col_offset_in_null_map: meta.col_offset_in_null_map,
            row_columns_order: meta.row_columns_order.clone(),
            columns_size,
            is_join_keys_inlined: meta.is_join_keys_inlined,
            is_join_keys_fixed_length: meta.is_join_keys_fixed_length,
            join_keys_length,
            fake_key_byte: if meta.is_join_keys_fixed_length {
                vec![0_u8; join_keys_length]
            } else {
                Vec::new()
            },
            key_mode: meta.key_mode,
        }
    }

    /// Offset of the serialized-key length prefix, when the row has one.
    #[must_use]
    pub const fn key_length_offset(&self) -> usize {
        SIZE_OF_NEXT_PTR + self.null_map_length
    }

    /// Reads the 4-byte serialized-key length prefix.
    #[must_use]
    pub fn get_serialized_key_length(&self, row: &[u8]) -> u32 {
        let start = self.key_length_offset();
        let mut bytes = [0_u8; SIZE_OF_ELEMENT_SIZE];
        bytes.copy_from_slice(&row[start..start + SIZE_OF_ELEMENT_SIZE]);
        u32::from_le_bytes(bytes)
    }

    /// Returns the key bytes of a row, as the source's `getKeyBytes` does.
    #[must_use]
    pub fn get_key_bytes<'a>(&self, row: &'a [u8]) -> &'a [u8] {
        let start = SIZE_OF_NEXT_PTR + self.null_map_length;
        match self.key_mode {
            KeyMode::OneInt64 => &row[start..start + UINT64_LEN],
            KeyMode::FixedSerializedKey => &row[start..start + self.join_keys_length],
            KeyMode::VariableSerializedKey => {
                let length = self.get_serialized_key_length(row) as usize;
                let data = start + SIZE_OF_ELEMENT_SIZE;
                &row[data..data + length]
            }
        }
    }

    /// Reports whether the given build column is null in this row.
    #[must_use]
    pub fn is_column_null(&self, row: &[u8], column_index: usize) -> bool {
        let bit = column_index + self.col_offset_in_null_map;
        row[SIZE_OF_NEXT_PTR + bit / 8] & (1_u8 << (7 - bit % 8)) != 0
    }

    /// Reports whether the used flag is set on this row.
    #[must_use]
    pub fn is_used_flag_set(&self, row: &[u8]) -> bool {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(&row[SIZE_OF_NEXT_PTR..SIZE_OF_NEXT_PTR + 4]);
        u32::from_ne_bytes(bytes) & USED_FLAG_MASK != 0
    }

    /// Offset where `row_data` starts, mirroring `meta.rowDataOffset`.
    #[must_use]
    pub fn row_data_offset(&self, row: &[u8]) -> usize {
        let base = SIZE_OF_NEXT_PTR + self.null_map_length;
        match (self.is_join_keys_inlined, self.is_join_keys_fixed_length) {
            (true, true) => base,
            (true, false) => base + SIZE_OF_ELEMENT_SIZE,
            (false, true) => base + self.join_keys_length,
            (false, false) => {
                base + SIZE_OF_ELEMENT_SIZE + self.get_serialized_key_length(row) as usize
            }
        }
    }

    /// Decodes the saved columns of a row back into per-column bytes.
    ///
    /// This is the read side of `row_table_builder.go`'s `fillRowData`, and
    /// covers what the source's probe path reconstructs from a build row.
    #[must_use]
    pub fn read_row_columns(&self, row: &[u8]) -> Vec<RowColumn> {
        let mut offset = self.row_data_offset(row);
        let mut columns = Vec::with_capacity(self.row_columns_order.len());
        for (index, &column_index) in self.row_columns_order.iter().enumerate() {
            let bytes = if let Some(size) = self.columns_size[index] {
                let value = row[offset..offset + size].to_vec();
                offset += size;
                value
            } else {
                let mut length_bytes = [0_u8; SIZE_OF_ELEMENT_SIZE];
                length_bytes.copy_from_slice(&row[offset..offset + SIZE_OF_ELEMENT_SIZE]);
                offset += SIZE_OF_ELEMENT_SIZE;
                let length = u32::from_le_bytes(length_bytes) as usize;
                let value = row[offset..offset + length].to_vec();
                offset += length;
                value
            };
            columns.push(RowColumn {
                column_index,
                is_null: self.is_column_null(row, index),
                bytes,
            });
        }
        columns
    }
}

/// One decoded column of a stored row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowColumn {
    /// Index of this column in the build-side chunk.
    pub column_index: usize,
    /// Whether the null map marks this column null.
    pub is_null: bool,
    /// Stored bytes; still present, but meaningless, when null.
    pub bytes: Vec<u8>,
}
