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

//! Source-backed tests for hash-join row storage.
//!
//! Ported from `pkg/executor/join/join_row_table_test.go`.
//!
//! SKIPPED, both because they assert properties of the GO RUNTIME rather
//! than of TiDB, and neither property exists to be observed here:
//!
//! * `TestHeapObjectCanMove` (join_row_table_test.go:25) requires
//!   `runtime.heapObjectsCanMove() == false`, the guard that lets Go stash an
//!   `unsafe.Pointer` in a `uintptr` across a GC. Rust has no moving
//!   collector, the port stores no real pointers (row addresses are synthetic
//!   `usize` values from `allocate_row_address_range`), and `unsafe` is
//!   forbidden workspace-wide, so there is nothing to assert.
//! * `TestUintptrCanHoldPointer` (join_row_table_test.go:45) requires
//!   `unsafe.Sizeof(uintptr(0)) >= unsafe.Sizeof(unsafe.Pointer(nil))`. In
//!   Rust `usize` is *defined* as pointer-width, so the analogous comparison
//!   is a tautology the compiler settles, not a runtime fact about the
//!   target.

use tidb_exec::join_row_table::{
    initialize_bit_masks, RowLayoutMeta, RowTable, RowTableSegment, BIT_MASK_IN_UINT32,
    FAKE_ADDR_PLACE_HOLDER_LEN, SIZE_OF_ELEMENT_SIZE, SIZE_OF_NEXT_PTR, SIZE_OF_UINTPTR,
    SIZE_OF_UNSAFE_POINTER, USED_FLAG_MASK,
};
use tidb_exec::join_table_meta::KeyMode;
use tidb_exec::tagged_ptr::TagPtrHelper;

#[test]
fn fixed_offset_in_row_layout_matches_source() {
    // Source: pkg/executor/join/join_row_table.go:23-26.
    // Direct Go coverage: pkg/executor/join/join_row_table_test.go:29
    // (TestFixedOffsetInRowLayout).
    assert_eq!(SIZE_OF_NEXT_PTR, 8);
    assert_eq!(SIZE_OF_ELEMENT_SIZE, 4);
    assert_eq!(FAKE_ADDR_PLACE_HOLDER_LEN, 8);
    // Not a ported assertion, just the ambient fact the skipped
    // TestUintptrCanHoldPointer states about Go: see the module header.
    assert_eq!(SIZE_OF_UINTPTR, SIZE_OF_UNSAFE_POINTER);
}

#[test]
fn bit_mask_in_uint32_reaches_every_null_map_bit() {
    // Source: pkg/executor/join/join_row_table.go:34-75.
    // Direct Go coverage: pkg/executor/join/join_row_table_test.go:34
    // (TestBitMaskInUint32). The Go test writes one null-map bit through a
    // byte slice and reads it back through an atomic uint32 load; the native
    // -endian load here is the same reinterpretation.
    let mut test_data = [0_u8; 4];
    for index in 0..32 {
        test_data[index / 8] = 1 << (7 - index % 8);
        let test_uint32 = u32::from_ne_bytes(test_data);
        assert_ne!(
            test_uint32 & BIT_MASK_IN_UINT32[index],
            0,
            "bit {index} unreachable through its uint32 mask"
        );
        test_data[index / 8] = 0;
    }
}

#[test]
fn bit_masks_follow_the_source_formula_on_both_endiannesses() {
    // Source: pkg/executor/join/join_row_table.go:66-75
    // (initializeBitMasks). Go picks the arm at init from a runtime probe;
    // the port picks it at compile time, so both arms are checked here.
    let little = initialize_bit_masks(true);
    let big = initialize_bit_masks(false);
    for index in 0..32_usize {
        assert_eq!(little[index], 1_u32 << (7 - (index % 8) + (index / 8) * 8));
        assert_eq!(big[index], 1_u32 << (31 - index));
    }
    assert_eq!(USED_FLAG_MASK, BIT_MASK_IN_UINT32[0]);
}

#[test]
fn row_addresses_are_unique_aligned_and_taggable() {
    // Source: pkg/executor/join/join_row_table.go:118-128
    // (getRowPointer, initTaggedBits). The four properties the source needs
    // from a real heap address, restated over the synthetic address space.
    let mut table = RowTable::new();
    for _ in 0..4 {
        let mut segment = RowTableSegment::new();
        segment.raw_data = vec![0_u8; 64];
        segment.row_start_offset = (0..8).map(|row| row * 8).collect();
        segment.hash_values = vec![0; 8];
        segment.finalize();
        table.segments.push(segment);
    }
    let mut seen = std::collections::BTreeSet::new();
    for segment in &table.segments {
        assert_eq!(segment.tagged_bits(), 24);
        for index in 0..8 {
            let address = segment.get_row_pointer(index);
            assert_eq!(address % 8, 0, "row address must be 8 byte aligned");
            assert!(seen.insert(address), "row address must be unique");
            assert!(table.segment_of_address(address).is_some());
        }
    }
    assert_eq!(table.row_count(), 32);
    assert_eq!(table.get_row_pointer(32), None);
}

#[test]
fn next_row_pointer_round_trips_through_the_row_prefix() {
    // Source: pkg/executor/join/join_row_table.go:155-166
    // (setNextRowAddress, getNextRowAddress).
    let mut segment = RowTableSegment::new();
    segment.raw_data = vec![0_u8; 32];
    segment.row_start_offset = vec![0, 16];
    segment.hash_values = vec![0, 0];
    segment.finalize();
    let mut table = RowTable::new();
    table.segments.push(segment);

    let mut helper = TagPtrHelper::default();
    helper.init(24);
    let second = table.segments[0].get_row_pointer(1);
    let hash_value = u64::MAX;
    let tagged = helper.to_tagged_ptr(helper.get_tagged_value(hash_value), second);
    table.segments[0].set_next_row_address(0, tagged.raw());

    let first = table.segments[0].get_row_pointer(0);
    assert_eq!(table.next_row_address(first, &helper, hash_value), tagged.raw());
    // A hash value whose tag bits are absent short-circuits to "no row".
    assert_eq!(table.next_row_address(first, &helper, 0), tagged.raw());
    let mut other = TagPtrHelper::default();
    other.init(24);
    let missing_tag = helper.to_tagged_ptr(0, second);
    table.segments[0].set_next_row_address(0, missing_tag.raw());
    assert_eq!(table.next_row_address(first, &other, hash_value), 0);
}

#[test]
fn row_layout_reads_null_map_and_key_bytes() {
    // Source: pkg/executor/join/join_table_meta.go:69-107 (row readers) over
    // the layout `pkg/executor/join/join_row_table.go:79-105` documents.
    let meta = RowLayoutMeta {
        null_map_length: 4,
        col_offset_in_null_map: 1,
        row_columns_order: vec![0, 1],
        columns_size: vec![Some(8), None],
        is_join_keys_inlined: false,
        is_join_keys_fixed_length: false,
        join_keys_length: 0,
        fake_key_byte: Vec::new(),
        key_mode: KeyMode::VariableSerializedKey,
    };
    let mut row = vec![0_u8; SIZE_OF_NEXT_PTR];
    // Null map: used flag set (bit 0) and build column 1 null (bit 2).
    row.extend_from_slice(&[0b1010_0000, 0, 0, 0]);
    row.extend_from_slice(&3_u32.to_le_bytes());
    row.extend_from_slice(b"key");
    row.extend_from_slice(&7_i64.to_le_bytes());
    row.extend_from_slice(&2_u32.to_le_bytes());
    row.extend_from_slice(b"ab");

    assert_eq!(meta.get_serialized_key_length(&row), 3);
    assert_eq!(meta.get_key_bytes(&row), b"key");
    assert!(meta.is_used_flag_set(&row));
    assert!(!meta.is_column_null(&row, 0));
    assert!(meta.is_column_null(&row, 1));
    let columns = meta.read_row_columns(&row);
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].column_index, 0);
    assert!(!columns[0].is_null);
    assert_eq!(columns[0].bytes, 7_i64.to_le_bytes());
    assert_eq!(columns[1].column_index, 1);
    assert!(columns[1].is_null);
    assert_eq!(columns[1].bytes, b"ab");
}
