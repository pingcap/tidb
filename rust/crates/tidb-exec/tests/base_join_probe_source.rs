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

//! Tests for [`tidb_exec::base_join_probe`], the port of
//! `pkg/executor/join/base_join_probe.go`.
//!
//! **These tests are WRITTEN, not ported.** Go has no
//! `base_join_probe_test.go`; the base probe is covered indirectly from
//! `join_probe_test.go` and `hash_join_v2_test.go`, both of which drive whole
//! joins through `HashJoinCtxV2`, `testutil.GenRandomChunks` and the
//! per-join-type probes -- none of which is in this port's dependency
//! closure. Each test below therefore pins one semantic the Go source states
//! directly, and names the Go function it is pinning.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode};

use tidb_exec::base_join_probe::{
    BATCH_BUILD_ROW_SIZE, BaseJoinProbe, BuildRowSource, MatchedRowInfo, OffsetAndLength,
    ProbeContext, ProbeError, RowBytesMap, common_init_for_scan_row_table, is_key_matched,
    new_join_probe,
};
use tidb_exec::hash_table_v2::HashTableV2;
use tidb_exec::join_row_table::{
    RowLayoutMeta, RowTable, RowTableSegment, SIZE_OF_NEXT_PTR,
};
use tidb_exec::join_table_meta::KeyMode;
use tidb_exec::row_table_builder::{
    BuildChunk, BuildColumn, BuildContext, PartitionInfo, RowTableBuilder, fnv64,
};
use tidb_exec::tagged_ptr::TagPtrHelper;
use tidb_executor::joiner::JoinType;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn int_fields(width: usize) -> Vec<FieldType> {
    vec![FieldType::new(FieldTypeCode::LongLong); width]
}

/// A row layout with one inlined fixed-width integer key and no null map.
fn one_int_key_layout() -> RowLayoutMeta {
    RowLayoutMeta {
        null_map_length: 0,
        col_offset_in_null_map: 0,
        row_columns_order: vec![0],
        columns_size: vec![Some(8)],
        is_join_keys_inlined: true,
        is_join_keys_fixed_length: true,
        join_keys_length: 8,
        fake_key_byte: vec![0_u8; 8],
        key_mode: KeyMode::OneInt64,
    }
}

/// Builds a single-partition row table over `keys`, plus the tag width every
/// segment agrees on.
fn build_side(keys: &[i64], layout: &RowLayoutMeta) -> (RowTable, u8) {
    let partition = PartitionInfo::new(1);
    let mut builder = RowTableBuilder::new(
        vec![0],
        partition.partition_number,
        false,
        false,
        false,
        layout.null_map_length,
    );
    let key_serializer =
        |chunk: &BuildChunk, row: usize| -> Vec<u8> { chunk.get_raw(row, 0).to_vec() };
    let mut context = BuildContext::new(layout, partition, &key_serializer);

    let values: Vec<(Vec<u8>, bool)> = keys
        .iter()
        .map(|key| (key.to_le_bytes().to_vec(), false))
        .collect();
    let chunk = BuildChunk::new(vec![BuildColumn::fixed(8, &values)]);

    let mut table = RowTable::new();
    builder.reset_buffer(&chunk);
    builder.init_hash_value_and_part_index_for_one_chunk(partition);
    let segments = builder
        .process_one_chunk(&chunk, &mut context)
        .expect("row table build");
    for segment in segments {
        table.segments.push(segment);
    }
    let tagged_bits = table
        .segments
        .iter()
        .map(RowTableSegment::tagged_bits)
        .min()
        .unwrap_or(0);
    (table, tagged_bits)
}

struct Fixture {
    hash_table: HashTableV2,
    layout: RowLayoutMeta,
    tag_helper: TagPtrHelper,
}

fn fixture(keys: &[i64]) -> Fixture {
    let layout = one_int_key_layout();
    let (table, tagged_bits) = build_side(keys, &layout);
    let mut tag_helper = TagPtrHelper::default();
    tag_helper.init(tagged_bits);
    let mut hash_table = HashTableV2::new_for_test(vec![table]);
    let segment_count = hash_table.sub_table(0).row_data.segments.len();
    hash_table.tables[0]
        .as_mut()
        .expect("partition zero")
        .build(0, segment_count, &tag_helper);
    Fixture {
        hash_table,
        layout,
        tag_helper,
    }
}

fn context<'a>(fixture: &'a Fixture, right_as_build_side: bool) -> ProbeContext<'a> {
    ProbeContext {
        hash_table: &fixture.hash_table,
        meta: &fixture.layout,
        column_count_needed_for_other_condition: 0,
        total_column_number: 1,
        tag_helper: fixture.tag_helper,
        partition_number: 1,
        partition_mask_offset: 64,
        has_other_condition: false,
        right_as_build_side,
        l_used: vec![0],
        r_used: vec![0],
        l_used_in_other_condition: Vec::new(),
        r_used_in_other_condition: Vec::new(),
        concurrency: 1,
        max_chunk_size: 1024,
    }
}

fn probe_chunk(keys: &[i64]) -> Chunk {
    let mut chunk = Chunk::new(&int_fields(1), keys.len().max(1), 1024);
    for &key in keys {
        chunk.append_int64(0, key);
    }
    chunk
}

/// The probe-side counterpart of the build fixture's key serializer.
fn key_of(chunk: &Chunk, row: usize) -> Option<Vec<u8>> {
    if chunk.column(0).is_null(row) {
        return None;
    }
    Some(chunk.column(0).get_int64(row).to_le_bytes().to_vec())
}

// ---------------------------------------------------------------------------
// isKeyMatched (`base_join_probe.go:911`)
// ---------------------------------------------------------------------------

/// One row header plus `key` laid out as the given mode stores it.
fn row_with_key(mode: KeyMode, key: &[u8]) -> Vec<u8> {
    let mut row = vec![0_u8; SIZE_OF_NEXT_PTR];
    if mode == KeyMode::VariableSerializedKey {
        row.extend_from_slice(&(key.len() as u32).to_le_bytes());
    }
    row.extend_from_slice(key);
    row
}

fn layout_for(mode: KeyMode, key_length: usize) -> RowLayoutMeta {
    RowLayoutMeta {
        null_map_length: 0,
        col_offset_in_null_map: 0,
        row_columns_order: Vec::new(),
        columns_size: Vec::new(),
        is_join_keys_inlined: true,
        is_join_keys_fixed_length: mode != KeyMode::VariableSerializedKey,
        join_keys_length: key_length,
        fake_key_byte: Vec::new(),
        key_mode: mode,
    }
}

#[test]
fn key_match_compares_the_stored_key_in_every_key_mode() {
    // OneInt64: Go dereferences both sides as int64, i.e. the same 8 bytes.
    let key = 42_i64.to_le_bytes();
    let layout = layout_for(KeyMode::OneInt64, 8);
    let row = row_with_key(KeyMode::OneInt64, &key);
    assert!(is_key_matched(KeyMode::OneInt64, &key, &row, &layout));
    assert!(!is_key_matched(
        KeyMode::OneInt64,
        &43_i64.to_le_bytes(),
        &row,
        &layout
    ));

    // FixedSerializedKey: bytes.Equal over meta.joinKeysLength bytes.
    let key = [1_u8, 2, 3, 4, 5];
    let layout = layout_for(KeyMode::FixedSerializedKey, 5);
    let row = row_with_key(KeyMode::FixedSerializedKey, &key);
    assert!(is_key_matched(
        KeyMode::FixedSerializedKey,
        &key,
        &row,
        &layout
    ));
    assert!(!is_key_matched(
        KeyMode::FixedSerializedKey,
        &[1_u8, 2, 3, 4, 9],
        &row,
        &layout
    ));

    // VariableSerializedKey: the stored length prefix decides the extent, so
    // a prefix of the stored key must NOT match.
    let key = b"abcdef";
    let layout = layout_for(KeyMode::VariableSerializedKey, 0);
    let row = row_with_key(KeyMode::VariableSerializedKey, key);
    assert!(is_key_matched(
        KeyMode::VariableSerializedKey,
        key,
        &row,
        &layout
    ));
    assert!(!is_key_matched(
        KeyMode::VariableSerializedKey,
        b"abc",
        &row,
        &layout
    ));
}

// ---------------------------------------------------------------------------
// SetChunkForProbe (`base_join_probe.go:179`)
// ---------------------------------------------------------------------------

#[test]
fn set_chunk_for_probe_hashes_buckets_and_resolves_headers() {
    let fixture = fixture(&[10, 20, 30]);
    let ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);

    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[20, 999]), None, &key_of)
        .expect("first chunk");

    assert_eq!(probe.chunk_rows(), 2);
    assert_eq!(probe.current_probe_row(), 0);
    assert_eq!(probe.used_rows(), &[0, 1]);
    assert_eq!(probe.serialized_keys()[0], 20_i64.to_le_bytes());
    assert_eq!(
        probe.matched_rows_hash_value()[0],
        fnv64(&20_i64.to_le_bytes())
    );

    // Key 20 is in the build side, so its bucket head must be a real row and
    // that row's stored key must be 20.
    let head = probe.matched_rows_headers()[0];
    assert_ne!(head, 0, "an existing key must resolve to a bucket head");
    let address = tidb_exec::hash_table_v2::row_address_of(&fixture.tag_helper, head);
    let row = fixture.hash_table.row_bytes(address);
    assert!(is_key_matched(
        KeyMode::OneInt64,
        &20_i64.to_le_bytes(),
        row,
        &fixture.layout
    ));
}

#[test]
fn set_chunk_for_probe_zeroes_headers_for_filtered_and_null_key_rows() {
    let fixture = fixture(&[10, 20]);
    let ctx = context(&fixture, true);

    // hasNullableKey is on, so nullKeyVector is allocated and a NULL key row
    // is excluded from hashing entirely.
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[true], true);
    let mut chunk = probe_chunk(&[10, 0, 20]);
    chunk.column_mut(0).set_null(1, true);

    // ProbeFilter rejects the last physical row.
    let filter = |_: &Chunk, physical_row: usize| physical_row != 2;
    probe
        .set_chunk_for_probe(&ctx, chunk, Some(&filter), &key_of)
        .expect("chunk");

    assert_ne!(probe.matched_rows_headers()[0], 0);
    assert_eq!(probe.matched_rows_headers()[1], 0, "NULL key -> no match");
    assert_eq!(probe.matched_rows_hash_value()[1], 0);
    assert_eq!(probe.matched_rows_headers()[2], 0, "filtered -> no match");
    assert_eq!(probe.matched_rows_hash_value()[2], 0);
}

#[test]
fn set_chunk_for_probe_rejects_a_chunk_while_the_previous_one_is_unprobed() {
    let fixture = fixture(&[1]);
    let ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);

    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[1, 2]), None, &key_of)
        .expect("first chunk");
    assert!(!probe.is_current_chunk_probe_done());
    assert_eq!(
        probe.set_chunk_for_probe(&ctx, probe_chunk(&[3]), None, &key_of),
        Err(ProbeError::PreviousChunkNotProbed)
    );

    // Once the worker has walked every row, the next chunk is accepted.
    probe.set_current_probe_row(probe.chunk_rows());
    assert!(probe.is_current_chunk_probe_done());
    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[3]), None, &key_of)
        .expect("second chunk");
}

#[test]
fn set_chunk_for_probe_honors_a_selection_vector() {
    let fixture = fixture(&[7, 8, 9]);
    let ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);

    let mut chunk = probe_chunk(&[7, 8, 9]);
    chunk.set_sel(Some(vec![2, 0]));
    probe
        .set_chunk_for_probe(&ctx, chunk, None, &key_of)
        .expect("chunk");

    // usedRows maps logical -> physical, and chunkRows counts logical rows.
    assert_eq!(probe.chunk_rows(), 2);
    assert_eq!(probe.used_rows(), &[2, 0]);
}

// ---------------------------------------------------------------------------
// finishLookupCurrentProbeRow / appendProbeRowToChunk
// (`base_join_probe.go:505`, `:680`)
// ---------------------------------------------------------------------------

#[test]
fn probe_row_runs_are_replayed_once_per_match() {
    let fixture = fixture(&[5]);
    let ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);
    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[100, 200, 300]), None, &key_of)
        .expect("chunk");

    // Row 0 matched three build rows, row 1 matched none, row 2 matched one.
    probe.set_current_probe_row(0);
    for _ in 0..3 {
        probe.record_matched_row_for_current_probe_row();
    }
    probe.finish_lookup_current_probe_row();
    probe.set_current_probe_row(1);
    probe.finish_lookup_current_probe_row();
    probe.set_current_probe_row(2);
    probe.record_matched_row_for_current_probe_row();
    probe.finish_lookup_current_probe_row();

    // A zero-match row contributes no run at all, which is what keeps an
    // inner join from emitting it.
    assert_eq!(
        probe.offset_and_length_array(),
        &[
            OffsetAndLength {
                offset: 0,
                length: 3
            },
            OffsetAndLength {
                offset: 2,
                length: 1
            },
        ]
    );

    let mut out = Chunk::new(&int_fields(1), 8, 1024);
    let source = probe.current_chunk().expect("probe chunk").copy_construct();
    probe.append_probe_row_to_chunk(&ctx, &mut out, &source);

    assert_eq!(out.column(0).rows(), 4);
    let values: Vec<i64> = (0..4).map(|row| out.column(0).get_int64(row)).collect();
    assert_eq!(values, vec![100, 100, 100, 300]);
}

// ---------------------------------------------------------------------------
// appendBuildRowToChunk / batchConstructBuildRows
// (`base_join_probe.go:583`, `:551`)
// ---------------------------------------------------------------------------

#[test]
fn build_rows_are_reconstructed_from_their_packed_bytes() {
    let keys: Vec<i64> = (0..5).map(|i| 1000 + i).collect();
    let fixture = fixture(&keys);
    let ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);
    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[1000]), None, &key_of)
        .expect("chunk");

    let segment = &fixture.hash_table.sub_table(0).row_data.segments[0];
    let addresses: Vec<usize> = (0..keys.len()).map(|i| segment.get_row_pointer(i)).collect();

    // rightAsBuildSide with no other condition puts the build columns after
    // the probe columns, i.e. at offset len(lUsed).
    let mut out = Chunk::new(&int_fields(2), 8, 1024);
    for &address in &addresses {
        probe.append_build_row_to_cached_build_rows_v1(
            &ctx,
            &fixture.hash_table,
            0,
            address,
            &mut out,
            0,
            false,
        );
    }
    // Fewer than BATCH_BUILD_ROW_SIZE staged rows, so nothing has flushed yet.
    assert_eq!(out.column(1).rows(), 0);
    probe.batch_construct_build_rows(&ctx, &fixture.hash_table, &mut out, 0, false);

    let values: Vec<i64> = (0..keys.len())
        .map(|row| out.column(1).get_int64(row))
        .collect();
    assert_eq!(values, keys);
    // appendBuildRowToChunkInternal owns the virtual row count when it starts
    // at column 0.
    assert_eq!(out.num_virtual_rows(), keys.len());
}

#[test]
fn staged_build_rows_flush_at_the_batch_size() {
    let keys: Vec<i64> = (0..BATCH_BUILD_ROW_SIZE as i64).collect();
    let fixture = fixture(&keys);
    let ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);
    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[0]), None, &key_of)
        .expect("chunk");

    let segment = &fixture.hash_table.sub_table(0).row_data.segments[0];
    let mut out = Chunk::new(&int_fields(2), 64, 1024);
    for index in 0..keys.len() {
        assert_eq!(
            out.column(1).rows(),
            0,
            "no flush before the batch is full (row {index})"
        );
        probe.append_build_row_to_cached_build_rows_v1(
            &ctx,
            &fixture.hash_table,
            0,
            segment.get_row_pointer(index),
            &mut out,
            0,
            false,
        );
    }
    assert_eq!(
        out.column(1).rows(),
        BATCH_BUILD_ROW_SIZE,
        "the 32nd staged row flushes the batch"
    );
}

#[test]
fn an_unused_build_column_is_stepped_over_rather_than_appended() {
    // lUsed empty means the parent uses no build column: Go returns early
    // from appendBuildRowToChunkInternal but still fixes the virtual rows.
    let fixture = fixture(&[11, 22]);
    let mut ctx = context(&fixture, true);
    ctx.l_used = Vec::new();
    ctx.r_used = Vec::new();
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);
    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[11]), None, &key_of)
        .expect("chunk");

    let segment = &fixture.hash_table.sub_table(0).row_data.segments[0];
    let mut out = Chunk::new(&int_fields(1), 8, 1024);
    probe.append_build_row_to_cached_build_rows_v1(
        &ctx,
        &fixture.hash_table,
        0,
        segment.get_row_pointer(0),
        &mut out,
        0,
        false,
    );
    probe.batch_construct_build_rows(&ctx, &fixture.hash_table, &mut out, 0, false);

    assert_eq!(out.column(0).rows(), 0, "no column data is appended");
    assert_eq!(out.num_virtual_rows(), 1, "but the row still counts");
}

// ---------------------------------------------------------------------------
// Bucket-chain walking
// ---------------------------------------------------------------------------

#[test]
fn a_bucket_chain_walks_every_row_that_shares_a_hash_bucket() {
    // Three build rows carry the same key, so they form one chain.
    let fixture = fixture(&[77, 77, 77]);
    let ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);
    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[77]), None, &key_of)
        .expect("chunk");

    let hash_value = probe.matched_rows_hash_value()[0];
    let mut current = probe.matched_rows_headers()[0];
    let mut matched = 0;
    while current != 0 {
        let address = tidb_exec::hash_table_v2::row_address_of(&fixture.tag_helper, current);
        let row = fixture.hash_table.row_bytes(address);
        if is_key_matched(
            KeyMode::OneInt64,
            &77_i64.to_le_bytes(),
            row,
            &fixture.layout,
        ) {
            matched += 1;
        } else {
            probe.record_probe_collision();
        }
        current = BaseJoinProbe::next_matched_row(
            &fixture.hash_table,
            &fixture.tag_helper,
            current,
            hash_value,
        );
    }
    assert_eq!(matched, 3, "every duplicate key is reachable from the head");
    assert_eq!(probe.get_probe_collision(), 0);
    probe.record_probe_collision();
    assert_eq!(probe.get_probe_collision(), 1);
    probe.reset_probe_collision();
    assert_eq!(probe.get_probe_collision(), 0);
}

// ---------------------------------------------------------------------------
// commonInitForScanRowTable (`base_join_probe.go:927`)
// ---------------------------------------------------------------------------

#[test]
fn scan_row_table_splits_the_table_across_workers_with_the_remainder_last() {
    let keys: Vec<i64> = (0..10).collect();
    let fixture = fixture(&keys);
    assert_eq!(fixture.hash_table.total_row_count(), 10);

    // avgRowPerWorker = 10 / 3 = 3, and the last worker takes 6..10.
    let expected = [(0_u64, 3_u64), (3, 6), (6, 10)];
    for (work_id, &(start, end)) in expected.iter().enumerate() {
        let iter = common_init_for_scan_row_table(&fixture.hash_table, work_id, 3);
        assert_eq!(iter.current_pos(), fixture.hash_table.create_row_pos(start));
        assert_eq!(iter.end_pos(), fixture.hash_table.create_row_pos(end));
    }

    // The sequential driver: one worker sees the whole table.
    let mut iter = common_init_for_scan_row_table(&fixture.hash_table, 0, 1);
    let mut seen = 0;
    while !iter.is_end() {
        seen += 1;
        iter.next();
    }
    assert_eq!(seen, 10);
}

// ---------------------------------------------------------------------------
// prepareForProbe (`base_join_probe.go:565`)
// ---------------------------------------------------------------------------

#[test]
fn prepare_for_probe_reports_the_remaining_capacity_and_the_scratch_choice() {
    let fixture = fixture(&[1]);
    let mut ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);

    let mut chk = Chunk::new(&int_fields(1), 8, 16);
    chk.set_required_rows(10, 16);
    chk.append_int64(0, 1);
    chk.append_int64(0, 2);
    let (use_scratch, remain) = probe.prepare_for_probe(&ctx, &chk);
    assert!(!use_scratch, "no other condition -> build straight into chk");
    assert_eq!(remain, 8);

    ctx.has_other_condition = true;
    let (use_scratch, _) = probe.prepare_for_probe(&ctx, &chk);
    assert!(use_scratch, "an other condition needs the scratch chunk");
}

// ---------------------------------------------------------------------------
// advanceToRowData through a hand-built row (`join_table_meta.go`)
// ---------------------------------------------------------------------------

#[test]
fn reconstruction_reads_rows_from_any_build_row_source() {
    // The reconstruction path only needs bytes at an address, so a hand-built
    // row exercises it without a whole build side.
    let fixture = fixture(&[1]);
    let ctx = context(&fixture, true);
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);
    probe
        .set_chunk_for_probe(&ctx, probe_chunk(&[1]), None, &key_of)
        .expect("chunk");

    let mut rows = RowBytesMap::new();
    for (index, value) in [4242_i64, -7_i64].into_iter().enumerate() {
        let mut row = vec![0_u8; SIZE_OF_NEXT_PTR];
        row.extend_from_slice(&value.to_le_bytes());
        rows.insert(0x2000 + index, row);
    }

    let mut out = Chunk::new(&int_fields(2), 8, 1024);
    for index in 0..2 {
        probe.append_build_row_to_cached_build_rows_v2(
            &ctx,
            &rows,
            MatchedRowInfo {
                probe_row_index: 0,
                build_row_start: 0x2000 + index,
                build_row_offset: 0,
            },
            &mut out,
            0,
            false,
        );
    }
    probe.batch_construct_build_rows(&ctx, &rows, &mut out, 0, false);

    assert_eq!(out.column(1).get_int64(0), 4242);
    assert_eq!(out.column(1).get_int64(1), -7);
}

// ---------------------------------------------------------------------------
// NewJoinProbe validation (`base_join_probe.go:940`)
// ---------------------------------------------------------------------------

#[test]
fn new_join_probe_records_the_join_type_and_nullable_key_flag() {
    let fixture = fixture(&[1]);
    let ctx = context(&fixture, true);
    let probe = new_join_probe(&ctx, 3, JoinType::LeftOuter, vec![0], &[false], true);
    assert_eq!(probe.join_type(), JoinType::LeftOuter);
    assert_eq!(probe.work_id(), 3);
    assert_eq!(probe.key_index(), &[0]);
    assert!(!probe.has_nullable_key());
    assert_eq!(probe.max_chunk_size(), 1024);

    let probe = new_join_probe(&ctx, 0, JoinType::LeftOuter, vec![0], &[true], true);
    assert!(probe.has_nullable_key(), "a nullable probe key sets the flag");
}

#[test]
#[should_panic(expected = "len(base.rUsed) != 0 for semi join")]
fn new_join_probe_rejects_a_semi_join_that_uses_build_columns() {
    let fixture = fixture(&[1]);
    let ctx = context(&fixture, true);
    let _ = new_join_probe(&ctx, 0, JoinType::SemiJoin, vec![0], &[false], true);
}

#[test]
#[should_panic(expected = "unsupported join type")]
fn new_join_probe_rejects_a_left_outer_semi_join_built_on_the_left() {
    let fixture = fixture(&[1]);
    let mut ctx = context(&fixture, true);
    ctx.r_used = Vec::new();
    let _ = new_join_probe(&ctx, 0, JoinType::LeftOuterSemiJoin, vec![0], &[false], false);
}
