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

//! Go `pkg/executor/join`, covering `hash_table_v2.go`: the per-partition
//! hash table built over the row tables, its concurrent build, its lookup,
//! and the row iterator that scans a whole `hashTableV2`.
//!
//! SEED of `pkg/executor/join`: see [`crate::join_row_table`] for the ported
//! and unported file list. This module covers the source's `subTable`,
//! `hashTableV2`, `rowPos`, `rowIter`, and the four length/memory helpers at
//! the end of the file. It does NOT cover the callers that live in
//! `hash_join_v2.go` (`hashTableContext`, `buildHashTableForList`, spill
//! bookkeeping) beyond the one constant `minimalHashTableLen` this file
//! reads, nor the probe side that consumes [`SubTable::lookup`].
//!
//! What is LAYOUT-IDENTICAL to Go:
//!
//! * the hash-table sizing rule -- `max(nextPowerOfTwo(validKeyCount), 32)`
//!   entries, `posMask = length - 1`, bucket `hashValue & posMask` -- so the
//!   bucket a row lands in matches the source for the same input;
//!   [`TAGGED_POINTER_LEN`] and [`get_hash_table_memory_usage`] are the same
//!   `uintptr`-width accounting;
//! * the tagged bucket word: high bits from
//!   [`TagPtrHelper::get_tagged_value`] over `hashValue | previousSlot`, low
//!   bits the row address, written back into the bucket and into the row's
//!   8-byte `next_row_ptr` prefix exactly where Go writes them;
//! * the chain order -- each new row becomes the bucket head and points at
//!   the previous head -- and the lookup tag short-circuit that returns "no
//!   row" when the bucket tag misses the probe hash;
//! * `rowIter`'s traversal: `createRowPos`'s two subtraction loops, `next`'s
//!   segment/sub-table advance, and `isEnd`'s disjunction, all reproduced
//!   statement for statement, so rows come out in the same order and each
//!   `[start, end)` split covers the same rows.
//!
//! What is only OBSERVABLY EQUIVALENT (the workspace forbids `unsafe`):
//!
//! * a bucket holds a tagged synthetic row address (`usize`, see
//!   [`crate::join_row_table`]) in an [`AtomicUsize`], not a Go `taggedPtr`
//!   over a real heap pointer. Untagging goes through
//!   [`row_address_of`] instead of `tagHelper.toUnsafePointer`, and a row is
//!   reached through its owning [`RowTableSegment`] instead of by
//!   dereferencing.
//! * Go writes a row's `next_row_ptr` through the same raw pointer it stores
//!   in the bucket. Here the writer needs `&mut` on the segment, so a
//!   concurrent build is expressed as
//!   [`SubTable::split_for_concurrent_build`]: one shared
//!   [`HashTableSlots`] plus disjoint mutable segment slices, which is the
//!   sharing the source's `build(start, end)` already assumes -- buckets
//!   shared and CAS-updated, each row written only by the thread owning its
//!   segment.
//! * the source's non-atomic `updateHashValue` and atomic
//!   `atomicUpdateHashValue` both appear, selected by the same
//!   whole-range-versus-partial-range test in [`SubTable::build`]; relaxed
//!   loads and stores stand in for Go's plain slice access, which is
//!   indistinguishable in the single-writer case the branch guards.
//! * `clearPartitionSegments` empties the bucket vector rather than setting
//!   it to `nil`; both leave a zero-length hash table behind.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::join_row_table::{RowTable, RowTableSegment};
use crate::tagged_ptr::TagPtrHelper;

/// Smallest hash table the source ever allocates, `minimalHashTableLen`.
pub const MINIMAL_HASH_TABLE_LEN: u64 = 32;

/// Width of one bucket, the source's `taggedPointerLen`.
pub const TAGGED_POINTER_LEN: i64 = size_of::<usize>() as i64;

/// Smallest power of two strictly greater than `value`.
///
/// # Panics
///
/// Panics with the source's message when `value` needs more than 64 rounds,
/// i.e. when no power of two above it fits a `u64`.
#[must_use]
pub fn next_power_of_two(value: u64) -> u64 {
    let mut ret = 2_u64;
    let mut round = 1;
    while ret <= value && round <= 64 {
        round += 1;
        ret <<= 1;
    }
    assert!(round <= 64, "input value is too large");
    ret
}

/// Bucket count for a row table, from its valid join keys.
#[must_use]
pub fn get_hash_table_length_by_row_table(table: &RowTable) -> u64 {
    get_hash_table_length_by_row_len(table.valid_key_count())
}

/// Bucket count for a given number of rows with a valid join key.
#[must_use]
pub fn get_hash_table_length_by_row_len(row_len: u64) -> u64 {
    next_power_of_two(row_len).max(MINIMAL_HASH_TABLE_LEN)
}

/// Bytes a hash table of `hash_table_length` buckets occupies.
#[must_use]
pub const fn get_hash_table_memory_usage(hash_table_length: u64) -> i64 {
    hash_table_length as i64 * TAGGED_POINTER_LEN
}

/// Strips the tag from a bucket word, the source's `toUnsafePointer`.
///
/// Buckets are stored as raw `usize` words so they can be atomic, so the
/// helper's `TaggedPtr` is reconstructed here before the mask is cleared.
#[must_use]
pub fn row_address_of(tag_helper: &TagPtrHelper, tagged: usize) -> usize {
    tag_helper.to_raw_pointer(tag_helper.to_tagged_ptr(0, tagged))
}

/// The bucket array of one [`SubTable`], shareable across build threads.
#[derive(Clone, Copy, Debug)]
pub struct HashTableSlots<'a> {
    slots: &'a [AtomicUsize],
    pos_mask: u64,
}

impl HashTableSlots<'_> {
    /// Number of buckets.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the bucket array has been cleared away.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Mask that turns a hash value into a bucket index.
    #[must_use]
    pub const fn pos_mask(&self) -> u64 {
        self.pos_mask
    }

    /// `updateHashValue`: makes `row_address` the head of its bucket.
    ///
    /// Returns the previous head, which the caller stores in the row's
    /// `next_row_ptr`.
    fn update_hash_value(
        &self,
        hash_value: u64,
        row_address: usize,
        tag_helper: &TagPtrHelper,
    ) -> usize {
        let pos = (hash_value & self.pos_mask) as usize;
        let prev = self.slots[pos].load(Ordering::Relaxed);
        let tag_value = tag_helper.get_tagged_value(hash_value | prev as u64);
        let tagged_address = tag_helper.to_tagged_ptr(tag_value, row_address);
        self.slots[pos].store(tagged_address.raw(), Ordering::Relaxed);
        prev
    }

    /// `atomicUpdateHashValue`: the same, under a compare-and-swap loop.
    fn atomic_update_hash_value(
        &self,
        hash_value: u64,
        row_address: usize,
        tag_helper: &TagPtrHelper,
    ) -> usize {
        let pos = (hash_value & self.pos_mask) as usize;
        loop {
            let prev = self.slots[pos].load(Ordering::Acquire);
            let tag_value = tag_helper.get_tagged_value(hash_value | prev as u64);
            let tagged_address = tag_helper.to_tagged_ptr(tag_value, row_address);
            if self.slots[pos]
                .compare_exchange_weak(
                    prev,
                    tagged_address.raw(),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return prev;
            }
        }
    }

    /// Reads one bucket.
    #[must_use]
    pub fn slot(&self, index: usize) -> usize {
        self.slots[index].load(Ordering::Acquire)
    }

    /// `lookup`: the bucket head for `hash_value`, or `0` when its tag misses.
    #[must_use]
    pub fn lookup(&self, hash_value: u64, tag_helper: &TagPtrHelper) -> usize {
        let ret = self.slot((hash_value & self.pos_mask) as usize);
        let hash_tag_value = tag_helper.get_tagged_value(hash_value);
        if ret as u64 & hash_tag_value != hash_tag_value {
            // if tag value not match, the key will not be matched
            return 0;
        }
        ret
    }

    /// Builds `segments` into these buckets with the atomic update path.
    ///
    /// This is the `startSegmentIndex != 0 || endSegmentIndex != len` arm of
    /// the source's `build`, exposed so several threads can each own a
    /// disjoint slice of segments while sharing one bucket array.
    pub fn build_segments(&self, segments: &mut [RowTableSegment], tag_helper: &TagPtrHelper) {
        for segment in segments {
            build_one_segment(*self, segment, tag_helper, true);
        }
    }
}

/// Inserts every valid-key row of one segment into the bucket array.
fn build_one_segment(
    slots: HashTableSlots<'_>,
    segment: &mut RowTableSegment,
    tag_helper: &TagPtrHelper,
    atomic: bool,
) {
    for valid_index in 0..segment.valid_join_key_pos.len() {
        let index = segment.valid_join_key_pos[valid_index];
        let row_address = segment.get_row_pointer(index);
        let hash_value = segment.hash_values[index];
        let prev = if atomic {
            slots.atomic_update_hash_value(hash_value, row_address, tag_helper)
        } else {
            slots.update_hash_value(hash_value, row_address, tag_helper)
        };
        let row_offset = usize::try_from(segment.row_start_offset[index]).expect("row offset");
        segment.set_next_row_address(row_offset, prev);
    }
}

/// One partition's row table plus the hash table built over it.
#[derive(Debug)]
pub struct SubTable {
    /// The rows this hash table indexes.
    pub row_data: RowTable,
    hash_table: Vec<AtomicUsize>,
    pos_mask: u64,
    is_row_table_empty: bool,
    is_hash_table_empty: bool,
}

impl SubTable {
    /// `newSubTable`: sizes the bucket array from the table's valid keys.
    #[must_use]
    pub fn new(table: RowTable) -> Self {
        let is_row_table_empty = table.row_count() == 0;
        let is_hash_table_empty = table.valid_key_count() == 0;
        let hash_table_length = next_power_of_two(table.valid_key_count()).max(32);
        Self {
            row_data: table,
            hash_table: (0..hash_table_length)
                .map(|_| AtomicUsize::new(0))
                .collect(),
            pos_mask: hash_table_length - 1,
            is_row_table_empty,
            is_hash_table_empty,
        }
    }

    /// Whether the row table holds no rows at all.
    #[must_use]
    pub const fn is_row_table_empty(&self) -> bool {
        self.is_row_table_empty
    }

    /// Whether no row carries a valid join key.
    #[must_use]
    pub const fn is_hash_table_empty(&self) -> bool {
        self.is_hash_table_empty
    }

    /// Number of buckets.
    #[must_use]
    pub fn hash_table_len(&self) -> usize {
        self.hash_table.len()
    }

    /// Mask that turns a hash value into a bucket index.
    #[must_use]
    pub const fn pos_mask(&self) -> u64 {
        self.pos_mask
    }

    /// Borrows the bucket array.
    #[must_use]
    pub fn slots(&self) -> HashTableSlots<'_> {
        HashTableSlots {
            slots: &self.hash_table,
            pos_mask: self.pos_mask,
        }
    }

    /// `getTotalMemoryUsage`: rows plus buckets.
    #[must_use]
    pub fn get_total_memory_usage(&self) -> i64 {
        self.row_data.get_total_memory_usage()
            + get_hash_table_memory_usage(self.hash_table.len() as u64)
    }

    /// `lookup`: the bucket head for `hash_value`, or `0` when its tag misses.
    #[must_use]
    pub fn lookup(&self, hash_value: u64, tag_helper: &TagPtrHelper) -> usize {
        self.slots().lookup(hash_value, tag_helper)
    }

    /// `build`: inserts segments `[start, end)` into the hash table.
    ///
    /// The whole range uses the plain update, any partial range the atomic
    /// one, exactly as the source branches.
    pub fn build(
        &mut self,
        start_segment_index: usize,
        end_segment_index: usize,
        tag_helper: &TagPtrHelper,
    ) {
        let Self {
            row_data,
            hash_table,
            pos_mask,
            ..
        } = self;
        let slots = HashTableSlots {
            slots: hash_table,
            pos_mask: *pos_mask,
        };
        let atomic = !(start_segment_index == 0 && end_segment_index == row_data.segments.len());
        for segment in &mut row_data.segments[start_segment_index..end_segment_index] {
            build_one_segment(slots, segment, tag_helper, atomic);
        }
    }

    /// Splits this table into shared buckets and mutable segments.
    ///
    /// The caller hands each build thread a disjoint segment slice (for
    /// example through `split_at_mut`) and the shared [`HashTableSlots`],
    /// then calls [`HashTableSlots::build_segments`] on each side. That is
    /// the sharing the source's concurrent `build` performs through raw
    /// pointers.
    pub fn split_for_concurrent_build(&mut self) -> (HashTableSlots<'_>, &mut [RowTableSegment]) {
        let Self {
            row_data,
            hash_table,
            pos_mask,
            ..
        } = self;
        (
            HashTableSlots {
                slots: hash_table,
                pos_mask: *pos_mask,
            },
            &mut row_data.segments,
        )
    }

    /// Drops the rows and the buckets, as `clearPartitionSegments` does.
    pub fn clear_segments(&mut self) {
        self.row_data.clear_segments();
        self.hash_table = Vec::new();
    }
}

/// The whole build side: one [`SubTable`] per partition.
#[derive(Debug)]
pub struct HashTableV2 {
    /// Per-partition sub tables; `None` is the source's `nil` entry.
    pub tables: Vec<Option<SubTable>>,
    /// Number of partitions, kept alongside `tables` as the source does.
    pub partition_number: u64,
}

impl HashTableV2 {
    /// `newJoinHashTableForTest`: one sub table per partitioned row table.
    #[must_use]
    pub fn new_for_test(partitioned_row_tables: Vec<RowTable>) -> Self {
        let partition_number = partitioned_row_tables.len() as u64;
        Self {
            tables: partitioned_row_tables
                .into_iter()
                .map(|table| Some(SubTable::new(table)))
                .collect(),
            partition_number,
        }
    }

    /// `getPartitionMemoryUsage`, zero for a partition that was never built.
    #[must_use]
    pub fn get_partition_memory_usage(&self, part_id: usize) -> i64 {
        self.tables[part_id]
            .as_ref()
            .map_or(0, SubTable::get_total_memory_usage)
    }

    /// `clearPartitionSegments`.
    pub fn clear_partition_segments(&mut self, part_id: usize) {
        if let Some(table) = self.tables[part_id].as_mut() {
            table.clear_segments();
        }
    }

    /// Borrows one partition's sub table.
    ///
    /// # Panics
    ///
    /// Panics when the partition is `None`, matching the source's nil
    /// dereference in the paths that assume a built partition.
    #[must_use]
    pub fn sub_table(&self, part_id: usize) -> &SubTable {
        self.tables[part_id]
            .as_ref()
            .expect("sub table of a built partition")
    }

    /// `isHashTableEmpty`: true only when no partition has a valid key.
    #[must_use]
    pub fn is_hash_table_empty(&self) -> bool {
        (0..self.tables.len()).all(|part_id| self.sub_table(part_id).is_hash_table_empty())
    }

    /// `totalRowCount`.
    #[must_use]
    pub fn total_row_count(&self) -> u64 {
        (0..self.tables.len())
            .map(|part_id| self.sub_table(part_id).row_data.row_count())
            .sum()
    }

    /// `createRowPos`: locates the `pos`-th row of the whole table.
    ///
    /// # Panics
    ///
    /// Panics when `pos` is above the total row count, as the source does.
    #[must_use]
    pub fn create_row_pos(&self, pos: u64) -> RowPos {
        let total_row_count = self.total_row_count();
        assert!(
            pos <= total_row_count,
            "invalid call to createRowPos, the input pos should be in [0, totalRowCount]"
        );
        if pos == total_row_count {
            return RowPos {
                sub_table_index: self.tables.len(),
                row_segment_index: 0,
                row_index: 0,
            };
        }
        let mut pos = pos;
        let mut sub_table_index = 0_usize;
        while pos >= self.sub_table(sub_table_index).row_data.row_count() {
            pos -= self.sub_table(sub_table_index).row_data.row_count();
            sub_table_index += 1;
        }
        let mut row_segment_index = 0_usize;
        while pos >= self.segment_row_count(sub_table_index, row_segment_index) {
            pos -= self.segment_row_count(sub_table_index, row_segment_index);
            row_segment_index += 1;
        }
        RowPos {
            sub_table_index,
            row_segment_index,
            row_index: pos,
        }
    }

    /// `createRowIter` over the half-open row range `[start, end)`.
    #[must_use]
    pub fn create_row_iter(&self, start: u64, end: u64) -> RowIter<'_> {
        let start = start.min(end);
        RowIter {
            table: self,
            current_pos: self.create_row_pos(start),
            end_pos: self.create_row_pos(end),
        }
    }

    fn segments(&self, sub_table_index: usize) -> &[RowTableSegment] {
        &self.sub_table(sub_table_index).row_data.segments
    }

    fn segment_row_count(&self, sub_table_index: usize, row_segment_index: usize) -> u64 {
        self.segments(sub_table_index)[row_segment_index].row_count() as u64
    }
}

/// A position inside a [`HashTableV2`], the source's `rowPos`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowPos {
    /// Which partition the row lives in.
    pub sub_table_index: usize,
    /// Which segment of that partition's row table.
    pub row_segment_index: usize,
    /// Row index inside that segment.
    pub row_index: u64,
}

/// Scans the rows of a [`HashTableV2`] in storage order, the source's
/// `rowIter`.
#[derive(Clone, Copy, Debug)]
pub struct RowIter<'a> {
    table: &'a HashTableV2,
    current_pos: RowPos,
    end_pos: RowPos,
}

impl RowIter<'_> {
    /// Current position.
    #[must_use]
    pub const fn current_pos(&self) -> RowPos {
        self.current_pos
    }

    /// End position.
    #[must_use]
    pub const fn end_pos(&self) -> RowPos {
        self.end_pos
    }

    /// `getValue`: the address of the row at the current position.
    #[must_use]
    pub fn get_value(&self) -> usize {
        self.table.segments(self.current_pos.sub_table_index)[self.current_pos.row_segment_index]
            .get_row_pointer(usize::try_from(self.current_pos.row_index).expect("row index"))
    }

    /// `next`: advances one row, crossing segments and partitions.
    pub fn next(&mut self) {
        self.current_pos.row_index += 1;
        if self.current_pos.row_index
            == self.table.segment_row_count(
                self.current_pos.sub_table_index,
                self.current_pos.row_segment_index,
            )
        {
            self.current_pos.row_segment_index += 1;
            self.current_pos.row_index = 0;
            while self.current_pos.row_segment_index
                == self.table.segments(self.current_pos.sub_table_index).len()
            {
                self.current_pos.sub_table_index += 1;
                self.current_pos.row_segment_index = 0;
                if self.current_pos.sub_table_index
                    == usize::try_from(self.table.partition_number).expect("partition number")
                {
                    break;
                }
            }
        }
    }

    /// `isEnd`.
    #[must_use]
    pub const fn is_end(&self) -> bool {
        !(self.current_pos.sub_table_index < self.end_pos.sub_table_index
            || self.current_pos.row_segment_index < self.end_pos.row_segment_index
            || self.current_pos.row_index < self.end_pos.row_index)
    }
}
