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

//! Source-backed tests for the hash-join hash table.
//!
//! Ported from `pkg/executor/join/hash_table_v2_test.go`, which holds exactly
//! five test functions: `TestHashTableSize`, `TestBuild`,
//! `TestConcurrentBuild`, `TestLookup`, and `TestRowIter`. All five are
//! ported; nothing is skipped.
//!
//! Fixture differences, all of which the Go fixtures leave free:
//!
//! * `createRowTable` drives `HashJoinCtxV2` and `testutil.GenRandomChunks`;
//!   here the same shape -- one not-null integer join key, one partition --
//!   is driven through [`RowTableBuilder`] with a deterministic xorshift
//!   generator, so runs are reproducible.
//! * Go's single `GenRandomChunks` call yields one segment, which makes
//!   `TestConcurrentBuild`'s three-way split degenerate to one whole-range
//!   build. The fixture here feeds the builder several chunks, so the split
//!   is real and the atomic build path is actually exercised.
//! * row addresses are the synthetic `usize` values of
//!   `join_row_table.rs`, so the Go tests' `map[unsafe.Pointer]struct{}`
//!   becomes a `HashMap<usize, _>` that also remembers where each address
//!   lives -- the port cannot dereference an address, it looks the row up in
//!   its segment.

use std::collections::HashMap;

use tidb_exec::hash_table_v2::{
    get_hash_table_length_by_row_len, get_hash_table_length_by_row_table,
    get_hash_table_memory_usage, next_power_of_two, row_address_of, HashTableV2, SubTable,
    MINIMAL_HASH_TABLE_LEN, TAGGED_POINTER_LEN,
};
use tidb_exec::join_row_table::{next_row_address, RowLayoutMeta, RowTable, RowTableSegment};
use tidb_exec::join_table_meta::{ColumnType, JoinTableMeta};
use tidb_exec::row_table_builder::{
    BuildChunk, BuildColumn, BuildContext, PartitionInfo, RowTableBuilder,
};
use tidb_exec::tagged_ptr::TagPtrHelper;

/// Deterministic stand-in for `math/rand`, which the Go fixtures use only to
/// vary segment sizes and key values.
struct Xorshift(u64);

impl Xorshift {
    const fn next_u64(&mut self) -> u64 {
        let mut state = self.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0 = state;
        state
    }

    /// `rand.Int31n(bound)`: a value in `[0, bound)`.
    const fn next_below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// `createMockRowTable` (hash_table_v2_test.go:31).
fn create_mock_row_table(
    max_rows_per_seg: usize,
    segment_count: usize,
    fixed_size: bool,
    rng: &mut Xorshift,
) -> RowTable {
    let mut ret = RowTable::new();
    for _ in 0..segment_count {
        // no empty segment is allowed
        let rows = if fixed_size {
            max_rows_per_seg
        } else {
            rng.next_below(max_rows_per_seg) + 1
        };
        let mut row_seg = RowTableSegment::new();
        row_seg.raw_data = vec![0_u8; rows];
        for j in 0..rows {
            row_seg.row_start_offset.push(j as u64);
            row_seg.valid_join_key_pos.push(j);
        }
        row_seg.finalize();
        ret.segments.push(row_seg);
    }
    ret
}

/// `createRowTable` (hash_table_v2_test.go:50): one not-null integer join key
/// in one partition, converted through the row-table builder.
fn create_row_table(rows: usize) -> (RowTable, u8) {
    let build_key_index = vec![0_usize];
    let build_types = [ColumnType::Int];
    let meta = JoinTableMeta::new(
        &build_key_index,
        &build_types,
        &build_types,
        &build_types,
        None,
        Some(&[]),
        false,
    );
    // The single integer key is inlined, so it is the one saved column.
    let layout = RowLayoutMeta::from_join_table_meta(&meta, vec![Some(8)]);
    let partition = PartitionInfo::new(1);
    // The key column is not null, so the builder keeps every row.
    let mut builder = RowTableBuilder::new(
        build_key_index,
        partition.partition_number,
        false,
        false,
        false,
        layout.null_map_length,
    );
    let key_serializer =
        |chunk: &BuildChunk, row: usize| -> Vec<u8> { chunk.get_raw(row, 0).to_vec() };
    let mut context = BuildContext::new(&layout, partition, &key_serializer);

    let mut table = RowTable::new();
    let mut rng = Xorshift(0x2545_f491_4f6c_dd1d);
    let chunk_size = 4096;
    let mut remaining = rows;
    while remaining > 0 {
        let chunk_rows = remaining.min(chunk_size);
        remaining -= chunk_rows;
        let values: Vec<(Vec<u8>, bool)> = (0..chunk_rows)
            .map(|_| (rng.next_u64().to_le_bytes().to_vec(), false))
            .collect();
        let chunk = BuildChunk::new(vec![BuildColumn::fixed(8, &values)]);
        let segments = builder
            .process_one_chunk(&chunk, &mut context)
            .expect("row table build");
        for segment in segments {
            if segment.row_count() > 0 {
                table.segments.push(segment);
            }
        }
    }
    let tagged_bits = table
        .segments
        .iter()
        .map(RowTableSegment::tagged_bits)
        .min()
        .expect("at least one segment");
    (table, tagged_bits)
}

/// Address -> (segment index, in-segment offset) for every row of a table.
///
/// This is the Go tests' `rowSet`, plus the location that replaces pointer
/// dereference.
fn collect_row_set(table: &RowTable) -> HashMap<usize, (usize, usize)> {
    let mut row_set = HashMap::with_capacity(table.row_count() as usize);
    for (segment_index, segment) in table.segments.iter().enumerate() {
        for index in 0..segment.row_start_offset.len() {
            let location = segment.get_row_pointer(index);
            let offset = segment.row_start_offset[index] as usize;
            assert!(
                row_set.insert(location, (segment_index, offset)).is_none(),
                "row address must be unique"
            );
        }
    }
    row_set
}

/// `getNextRowAddress(loc, tagHelper, hashValue)` over a located row.
fn next_in_chain(
    table: &RowTable,
    location: (usize, usize),
    tag_helper: &TagPtrHelper,
    hash_value: u64,
) -> usize {
    let (segment_index, offset) = location;
    next_row_address(
        table.segments[segment_index].raw_next_row_address(offset),
        tag_helper,
        hash_value,
    )
}

#[test]
fn hash_table_size_rounds_up_to_a_power_of_two() {
    // Source: pkg/executor/join/hash_table_v2.go:55-83 (nextPowerOfTwo,
    // newSubTable).
    // Direct Go coverage: pkg/executor/join/hash_table_v2_test.go:93
    // (TestHashTableSize).
    let mut rng = Xorshift(1);
    let row_table = create_mock_row_table(2, 5, true, &mut rng);
    let sub_table = SubTable::new(row_table);
    // min hash table size is 32
    assert_eq!(sub_table.hash_table_len(), 32);
    let sub_table = SubTable::new(create_mock_row_table(32, 1, true, &mut rng));
    assert_eq!(sub_table.hash_table_len(), 64);
    let sub_table = SubTable::new(create_mock_row_table(33, 1, true, &mut rng));
    assert_eq!(sub_table.hash_table_len(), 64);
    let sub_table = SubTable::new(create_mock_row_table(64, 1, true, &mut rng));
    assert_eq!(sub_table.hash_table_len(), 128);
    let sub_table = SubTable::new(create_mock_row_table(65, 1, true, &mut rng));
    assert_eq!(sub_table.hash_table_len(), 128);
    assert_eq!(sub_table.pos_mask(), 127);
}

#[test]
fn hash_table_length_and_memory_helpers_match_the_source() {
    // Source: pkg/executor/join/hash_table_v2.go:248-258. No Go test covers
    // these three helpers directly; they are the sizing rule TestHashTableSize
    // observes through newSubTable.
    assert_eq!(MINIMAL_HASH_TABLE_LEN, 32);
    assert_eq!(TAGGED_POINTER_LEN, 8);
    assert_eq!(next_power_of_two(0), 2);
    assert_eq!(next_power_of_two(1), 2);
    assert_eq!(next_power_of_two(2), 4);
    assert_eq!(get_hash_table_length_by_row_len(0), 32);
    assert_eq!(get_hash_table_length_by_row_len(31), 32);
    assert_eq!(get_hash_table_length_by_row_len(32), 64);
    assert_eq!(get_hash_table_memory_usage(32), 256);
    let mut rng = Xorshift(7);
    let row_table = create_mock_row_table(10, 4, true, &mut rng);
    assert_eq!(get_hash_table_length_by_row_table(&row_table), 64);
    let sub_table = SubTable::new(row_table);
    assert_eq!(
        sub_table.get_total_memory_usage(),
        sub_table.row_data.get_total_memory_usage() + 64 * TAGGED_POINTER_LEN
    );
    assert!(!sub_table.is_row_table_empty());
    assert!(!sub_table.is_hash_table_empty());
    let empty = SubTable::new(RowTable::new());
    assert!(empty.is_row_table_empty());
    assert!(empty.is_hash_table_empty());
}

#[test]
fn build_chains_every_row_exactly_once() {
    // Source: pkg/executor/join/hash_table_v2.go:85-125 (updateHashValue,
    // build).
    // Direct Go coverage: pkg/executor/join/hash_table_v2_test.go:112
    // (TestBuild).
    let (row_table, tagged_bits) = create_row_table(1_000_000);
    let mut tag_helper = TagPtrHelper::default();
    tag_helper.init(tagged_bits);
    let mut sub_table = SubTable::new(row_table);
    // single thread build
    let segment_count = sub_table.row_data.segments.len();
    sub_table.build(0, segment_count, &tag_helper);

    let mut row_set = collect_row_set(&sub_table.row_data);
    let expected_row_count = sub_table.row_data.row_count();
    let mut row_count = 0_u64;
    for index in 0..sub_table.hash_table_len() {
        let mut loc_holder = sub_table.slots().slot(index);
        while loc_holder != 0 {
            row_count += 1;
            let location = row_address_of(&tag_helper, loc_holder);
            let entry = row_set.remove(&location).expect("row must be a known row");
            // use 0 as hashvalue so getNextRowAddress won't exit early
            loc_holder = next_in_chain(&sub_table.row_data, entry, &tag_helper, 0);
        }
    }
    assert_eq!(row_set.len(), 0);
    assert_eq!(expected_row_count, row_count);
}

#[test]
fn concurrent_build_chains_every_row_exactly_once() {
    // Source: pkg/executor/join/hash_table_v2.go:94-125
    // (atomicUpdateHashValue, build).
    // Direct Go coverage: pkg/executor/join/hash_table_v2_test.go:145
    // (TestConcurrentBuild).
    let (row_table, tag_bits) = create_row_table(3_000_000);
    let mut sub_table = SubTable::new(row_table);
    let segment_count = sub_table.row_data.segments.len();
    let build_threads = 3;
    let mut tag_helper = TagPtrHelper::default();
    tag_helper.init(tag_bits);

    let (slots, mut rest) = sub_table.split_for_concurrent_build();
    let mut parts = Vec::with_capacity(build_threads);
    for i in 0..build_threads {
        let segment_start = segment_count / build_threads * i;
        let segment_end = if i == build_threads - 1 {
            segment_count
        } else {
            segment_count / build_threads * (i + 1)
        };
        let (head, tail) = rest.split_at_mut(segment_end - segment_start);
        parts.push(head);
        rest = tail;
    }
    let helper = &tag_helper;
    std::thread::scope(|scope| {
        for part in parts {
            scope.spawn(move || slots.build_segments(part, helper));
        }
    });

    let mut row_set = collect_row_set(&sub_table.row_data);
    for index in 0..sub_table.hash_table_len() {
        let mut loc_holder = sub_table.slots().slot(index);
        while loc_holder != 0 {
            let location = row_address_of(&tag_helper, loc_holder);
            let entry = row_set.remove(&location).expect("row must be a known row");
            loc_holder = next_in_chain(&sub_table.row_data, entry, &tag_helper, 0);
        }
    }
    assert_eq!(row_set.len(), 0);
}

#[test]
fn lookup_finds_every_built_row_in_its_bucket() {
    // Source: pkg/executor/join/hash_table_v2.go:45-53 (lookup).
    // Direct Go coverage: pkg/executor/join/hash_table_v2_test.go:186
    // (TestLookup).
    let (row_table, tag_bits) = create_row_table(200_000);
    let mut tag_helper = TagPtrHelper::default();
    tag_helper.init(tag_bits);
    let mut sub_table = SubTable::new(row_table);
    // single thread build
    let segment_count = sub_table.row_data.segments.len();
    sub_table.build(0, segment_count, &tag_helper);

    let row_set = collect_row_set(&sub_table.row_data);
    for segment in &sub_table.row_data.segments {
        for index in 0..segment.row_start_offset.len() {
            let hash_value = segment.hash_values[index];
            let mut candidate = sub_table.lookup(hash_value, &tag_helper);
            let location = segment.get_row_pointer(index);
            let mut found = false;
            while candidate != 0 {
                let candidate_address = row_address_of(&tag_helper, candidate);
                if candidate_address == location {
                    found = true;
                    break;
                }
                let entry = row_set[&candidate_address];
                candidate = next_in_chain(&sub_table.row_data, entry, &tag_helper, hash_value);
            }
            assert!(found, "row {index} not reachable from its bucket");
        }
    }
}

/// `checkRowIter` (hash_table_v2_test.go:214).
fn check_row_iter(table: &HashTableV2, scan_concurrency: u64) {
    // first create a map containing all the row locations
    let total_row_count = table.total_row_count();
    let mut row_set: HashMap<usize, ()> = HashMap::with_capacity(total_row_count as usize);
    for part_id in 0..table.tables.len() {
        for segment in &table.sub_table(part_id).row_data.segments {
            for index in 0..segment.row_start_offset.len() {
                assert!(
                    row_set.insert(segment.get_row_pointer(index), ()).is_none(),
                    "row address must be unique"
                );
            }
        }
    }
    // create row iters
    let mut row_iters = Vec::with_capacity(scan_concurrency as usize);
    let row_per_scan = total_row_count / scan_concurrency;
    for i in 0..scan_concurrency {
        let start_index = row_per_scan * i;
        let end_index = if i == scan_concurrency - 1 {
            total_row_count
        } else {
            row_per_scan * (i + 1)
        };
        row_iters.push(table.create_row_iter(start_index, end_index));
    }

    let mut loc_count = 0_u64;
    for iter in &mut row_iters {
        while !iter.is_end() {
            loc_count += 1;
            let location = iter.get_value();
            assert!(
                row_set.remove(&location).is_some(),
                "iterated row must be a known row"
            );
            iter.next();
        }
    }
    assert_eq!(table.total_row_count(), loc_count);
    assert_eq!(row_set.len(), 0);
}

#[test]
fn row_iter_covers_every_row_once() {
    // Source: pkg/executor/join/hash_table_v2.go:146-229 (rowPos, rowIter,
    // createRowPos, createRowIter).
    // Direct Go coverage: pkg/executor/join/hash_table_v2_test.go:255
    // (TestRowIter).
    let partition_numbers = [1_usize, 4, 8];
    let mut rng = Xorshift(0x9e37_79b9_7f4a_7c15);
    // normal case
    for &partition_number in &partition_numbers {
        // create row tables
        let row_tables: Vec<RowTable> = (0..partition_number)
            .map(|_| create_mock_row_table(1024, 16, false, &mut rng))
            .collect();
        let joined_hash_table = HashTableV2::new_for_test(row_tables);
        check_row_iter(&joined_hash_table, partition_number as u64);
    }
    // case with empty row table
    for &partition_number in &partition_numbers {
        for i in 0..partition_number {
            // the i-th row table is an empty row table
            let row_tables: Vec<RowTable> = (0..partition_number)
                .map(|j| {
                    if i == j {
                        create_mock_row_table(0, 0, true, &mut rng)
                    } else {
                        create_mock_row_table(1024, 16, false, &mut rng)
                    }
                })
                .collect();
            let joined_hash_table = HashTableV2::new_for_test(row_tables);
            check_row_iter(&joined_hash_table, partition_number as u64);
        }
    }
}

#[test]
fn partition_helpers_report_and_clear_built_partitions() {
    // Source: pkg/executor/join/hash_table_v2.go:132-144, 231-246
    // (getPartitionMemoryUsage, clearPartitionSegments, isHashTableEmpty,
    // totalRowCount). No Go test covers these directly; they are the
    // partition-level accessors around the structures TestRowIter walks.
    let mut rng = Xorshift(11);
    let mut table = HashTableV2::new_for_test(vec![
        create_mock_row_table(8, 2, true, &mut rng),
        create_mock_row_table(0, 0, true, &mut rng),
    ]);
    assert_eq!(table.partition_number, 2);
    assert_eq!(table.total_row_count(), 16);
    assert!(!table.is_hash_table_empty());
    assert!(table.sub_table(1).is_hash_table_empty());
    assert!(table.get_partition_memory_usage(0) > 0);
    table.clear_partition_segments(0);
    assert_eq!(table.total_row_count(), 0);
    assert_eq!(table.get_partition_memory_usage(0), 0);
    table.tables[0] = None;
    assert_eq!(table.get_partition_memory_usage(0), 0);
}
