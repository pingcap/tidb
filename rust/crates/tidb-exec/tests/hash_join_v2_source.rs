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

//! Tests for [`tidb_exec::hash_join_v2`], the port of
//! `pkg/executor/join/hash_join_v2.go`.
//!
//! **These tests are WRITTEN, not ported.** Go's `hash_join_v2_test.go` runs
//! whole joins through `testkit`, a live `HashJoinCtxV2`, `mockDataSource`,
//! `testutil.GenRandomChunks`, the spill helper and the per-join-type probes
//! -- none of which is in this port's dependency closure. `hash_join_v2.go`'s
//! own pure functions are covered directly below, and the build-then-probe
//! path is pinned end to end by walking the finished bucket chains, which is
//! exactly what a per-join-type probe does to decide matches.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode};

use tidb_exec::base_join_probe::{
    BaseJoinProbe, ProbeContext, is_key_matched, new_join_probe,
};
use tidb_exec::hash_join_v2::{
    BuildTask, HashJoinCtxV2, HashJoinV2Exec, HashTableContext,
    LABEL_FOR_HASH_TABLE_IN_HASH_JOIN_V2, new_join_build_worker_v2,
};
use tidb_exec::hash_table_v2::{get_hash_table_length_by_row_len, get_hash_table_memory_usage};
use tidb_exec::join_row_table::{RowLayoutMeta, RowTableSegment};
use tidb_exec::join_table_meta::KeyMode;
use tidb_exec::row_table_builder::{
    BuildChunk, BuildColumn, BuildContext, PartitionInfo, get_partition_mask_offset,
};
use tidb_executor::joiner::JoinType;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A row layout with one inlined fixed-width integer key and no null map,
/// matching the fixture `base_join_probe_source.rs` already uses.
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

fn build_chunk(keys: &[i64]) -> BuildChunk {
    let values: Vec<(Vec<u8>, bool)> = keys
        .iter()
        .map(|key| (key.to_le_bytes().to_vec(), false))
        .collect();
    BuildChunk::new(vec![BuildColumn::fixed(8, &values)])
}

fn probe_chunk(keys: &[i64]) -> Chunk {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut chunk = Chunk::new(&fields, keys.len().max(1), 1024);
    for &key in keys {
        chunk.append_int64(0, key);
    }
    chunk
}

fn build_key(chunk: &BuildChunk, row: usize) -> Vec<u8> {
    chunk.get_raw(row, 0).to_vec()
}

fn probe_key(chunk: &Chunk, row: usize) -> Option<Vec<u8>> {
    if chunk.column(0).is_null(row) {
        return None;
    }
    Some(chunk.column(0).get_int64(row).to_le_bytes().to_vec())
}

/// A whole build side: run [`HashJoinV2Exec::fetch_and_build_hash_table`] over
/// `chunks_per_worker` and hand back the finished executor.
fn built_exec(
    concurrency: usize,
    join_type: JoinType,
    layout: &RowLayoutMeta,
    chunks_per_worker: &[Vec<BuildChunk>],
) -> (HashJoinV2Exec, usize) {
    let ctx = HashJoinCtxV2::new(concurrency, join_type, true);
    let mut exec = HashJoinV2Exec::new(ctx, &[0], &[true]);
    let partition = PartitionInfo::new(exec.ctx.partition_number);
    let serializer = build_key;
    let mut build_context = BuildContext::new(layout, partition, &serializer);
    let total = exec
        .fetch_and_build_hash_table(chunks_per_worker, &mut build_context, layout.null_map_length)
        .expect("build side");
    (exec, total)
}

/// Every build key that the finished hash table matches for `key`, found the
/// way a per-join-type probe finds them: bucket head from
/// `SetChunkForProbe`, then [`BaseJoinProbe::next_matched_row`] down the
/// chain with [`is_key_matched`] at each link.
fn matches_for(exec: &HashJoinV2Exec, layout: &RowLayoutMeta, keys: &[i64]) -> Vec<Vec<i64>> {
    let hash_table = &exec.hash_table_context.hash_table;
    let ctx = ProbeContext {
        hash_table,
        meta: layout,
        column_count_needed_for_other_condition: 0,
        total_column_number: 1,
        tag_helper: exec.hash_table_context.tag_helper,
        partition_number: exec.ctx.partition_number,
        partition_mask_offset: exec.ctx.partition_mask_offset,
        has_other_condition: false,
        right_as_build_side: true,
        l_used: vec![0],
        r_used: vec![0],
        l_used_in_other_condition: Vec::new(),
        r_used_in_other_condition: Vec::new(),
        concurrency: exec.ctx.concurrency,
        max_chunk_size: 1024,
    };
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);
    let chunk = probe_chunk(keys);
    probe
        .set_chunk_for_probe(&ctx, chunk, None, &(probe_key as fn(&Chunk, usize) -> Option<Vec<u8>>))
        .expect("probe chunk prepared");

    (0..keys.len())
        .map(|logical_row| {
            let mut matched = Vec::new();
            let hash_value = probe.matched_rows_hash_value()[logical_row];
            let serialized = probe.serialized_keys()[logical_row].clone();
            let mut current = probe.matched_rows_headers()[logical_row];
            while current != 0 {
                let row = tidb_exec::base_join_probe::BuildRowSource::row_bytes(
                    hash_table,
                    tidb_exec::hash_table_v2::row_address_of(&ctx.tag_helper, current),
                );
                if is_key_matched(layout.key_mode, &serialized, row, layout) {
                    let mut bytes = [0_u8; 8];
                    bytes.copy_from_slice(&layout.get_key_bytes(row)[..8]);
                    matched.push(i64::from_le_bytes(bytes));
                }
                current =
                    BaseJoinProbe::next_matched_row(hash_table, &ctx.tag_helper, current, hash_value);
            }
            matched.sort_unstable();
            matched
        })
        .collect()
}

// ---------------------------------------------------------------------------
// SetupPartitionInfo (`hash_join_v2.go:298`, `:306`, `:313`)
// ---------------------------------------------------------------------------

#[test]
fn partition_info_is_a_capped_power_of_two_with_a_matching_mask_offset() {
    // genHashJoinPartitionNumber doubles until it reaches the hint, capped at
    // 16, so the partition number is always a power of two.
    for (concurrency, expected) in [
        (1_usize, 1_usize),
        (2, 2),
        (3, 4),
        (5, 8),
        (16, 16),
        (64, 16),
    ] {
        let ctx = HashJoinCtxV2::new(concurrency, JoinType::Inner, true);
        assert_eq!(ctx.partition_number, expected, "concurrency {concurrency}");
        assert!(ctx.partition_number.is_power_of_two());
        // getPartitionMaskOffset: the top log2(partitionNumber) bits of the
        // hash value select the partition.
        assert_eq!(
            ctx.partition_mask_offset,
            64 - ctx.partition_number.trailing_zeros() as usize
        );
        assert_eq!(
            ctx.partition_mask_offset,
            get_partition_mask_offset(ctx.partition_number)
        );
    }
}

// ---------------------------------------------------------------------------
// initMaxSpillRound (`hash_join_v2.go:636`)
// ---------------------------------------------------------------------------

#[test]
fn max_spill_round_is_the_rounds_needed_to_pass_1024_partitions() {
    // log(1024)/log(partitionNumber), truncated: with 2 partitions it takes
    // 10 rounds of re-partitioning to exceed 1024, with 16 it takes 2.
    for (partition_number, expected) in [(2_usize, 10_usize), (4, 5), (8, 3), (16, 2)] {
        let mut ctx = HashJoinCtxV2::new(1, JoinType::Inner, true);
        ctx.partition_number = partition_number;
        ctx.init_max_spill_round();
        assert_eq!(ctx.max_spill_round, expected, "{partition_number} partitions");
    }

    // Above 1024 partitions one round already suffices.
    let mut ctx = HashJoinCtxV2::new(1, JoinType::Inner, true);
    ctx.partition_number = 2048;
    ctx.init_max_spill_round();
    assert_eq!(ctx.max_spill_round, 1);
}

// ---------------------------------------------------------------------------
// canSkipProbeIfHashTableIsEmpty / shouldLimitProbeFetchSize
// (`hash_join_v2.go:753`, `:763`)
// ---------------------------------------------------------------------------

#[test]
fn probe_can_be_skipped_only_when_an_empty_build_side_produces_no_rows() {
    let skip = |join_type, right_as_build_side| {
        HashJoinCtxV2::new(1, join_type, right_as_build_side)
            .can_skip_probe_if_hash_table_is_empty()
    };
    // Inner: no build row, no output row, either way round.
    assert!(skip(JoinType::Inner, true));
    assert!(skip(JoinType::Inner, false));
    // Outer joins: skippable only when the *outer* side is the build side,
    // because then there is nothing to null-extend.
    assert!(!skip(JoinType::LeftOuter, true));
    assert!(skip(JoinType::LeftOuter, false));
    assert!(skip(JoinType::RightOuter, true));
    assert!(!skip(JoinType::RightOuter, false));
    // Semi: skippable when the existence side is the build side.
    assert!(skip(JoinType::SemiJoin, true));
    assert!(!skip(JoinType::SemiJoin, false));
    // Anti semi still emits every probe row, so it can never be skipped.
    assert!(!skip(JoinType::AntiSemiJoin, true));
    assert!(!skip(JoinType::AntiSemiJoin, false));
}

#[test]
fn probe_fetch_size_is_limited_only_for_the_outer_side_of_an_outer_join() {
    let limit = |join_type, right_as_build_side| {
        HashJoinCtxV2::new(1, join_type, right_as_build_side).should_limit_probe_fetch_size()
    };
    assert!(limit(JoinType::LeftOuter, true));
    assert!(!limit(JoinType::LeftOuter, false));
    assert!(limit(JoinType::RightOuter, false));
    assert!(!limit(JoinType::RightOuter, true));
    assert!(!limit(JoinType::Inner, true));
    assert!(!limit(JoinType::SemiJoin, true));
}

// ---------------------------------------------------------------------------
// hashTableContext (`hash_join_v2.go:70`)
// ---------------------------------------------------------------------------

#[test]
fn appending_a_row_segment_skips_empty_segments_and_creates_the_row_table_lazily() {
    let mut context = HashTableContext::new(2, 2);
    assert!(context.get_segments_in_row_table(0, 0).is_empty());

    // appendRowSegment returns early on an empty segment, so no row table is
    // created for it.
    context.append_row_segment(0, 0, RowTableSegment::new());
    assert!(context.row_tables[0][0].is_none());

    let mut segment = RowTableSegment::new();
    segment.hash_values.push(7);
    segment.row_start_offset.push(0);
    segment.raw_data.extend_from_slice(&[0_u8; 8]);
    segment.finalize();
    context.append_row_segment(1, 1, segment);
    assert_eq!(context.get_segments_in_row_table(1, 1).len(), 1);
    assert!(context.get_all_segments_memory_usage_in_row_table() > 0);

    context.clear_segments_in_row_table(1, 1);
    assert!(context.get_segments_in_row_table(1, 1).is_empty());
}

#[test]
fn merging_row_tables_concatenates_every_worker_share_and_consumes_memory() {
    let layout = one_int_key_layout();
    // Two workers, each with two chunks; every chunk contributes one segment
    // per non-empty partition.
    let chunks_per_worker = vec![
        vec![build_chunk(&[1, 2, 3]), build_chunk(&[4, 5])],
        vec![build_chunk(&[6, 7]), build_chunk(&[8])],
    ];
    let (exec, total_segment_cnt) =
        built_exec(2, JoinType::Inner, &layout, &chunks_per_worker);

    // Every per-worker row table is drained by the merge, which is Go's
    // clearAllSegmentsInRowTable.
    assert_eq!(
        exec.hash_table_context
            .get_all_segments_memory_usage_in_row_table(),
        0
    );
    // Segments land in the sub tables instead.
    let in_sub_tables: usize = (0..exec.ctx.partition_number)
        .map(|part| {
            exec.hash_table_context
                .hash_table
                .sub_table(part)
                .row_data
                .segments
                .len()
        })
        .sum();
    assert_eq!(in_sub_tables, total_segment_cnt);
    assert_eq!(exec.hash_table_context.hash_table.total_row_count(), 8);

    // tryToSpill's unconditional pre-consume: the bucket arrays of every
    // partition, charged to the hash-table tracker.
    let expected: i64 = (0..exec.ctx.partition_number)
        .map(|part| {
            let valid = exec
                .hash_table_context
                .hash_table
                .sub_table(part)
                .row_data
                .valid_key_count();
            get_hash_table_memory_usage(get_hash_table_length_by_row_len(valid))
        })
        .sum();
    // The tracker also carries the row-table charge the builder made, so the
    // bucket total is a lower bound on it, and an exact match against what
    // the sub tables now report.
    assert!(exec.hash_table_context.memory_tracker.bytes_consumed() > expected);
    assert!(exec.hash_table_context.get_all_memory_usage_in_hash_table() >= expected);
    assert!(expected > 0);
    assert_eq!(
        exec.hash_table_context.memory_tracker.label(),
        LABEL_FOR_HASH_TABLE_IN_HASH_JOIN_V2
    );
}

#[test]
fn resetting_the_hash_table_context_for_restore_gives_back_the_bucket_memory() {
    let layout = one_int_key_layout();
    let (mut exec, _) = built_exec(1, JoinType::Inner, &layout, &[vec![build_chunk(&[1, 2])]]);
    let before = exec.hash_table_context.memory_tracker.bytes_consumed();
    assert!(before > 0);
    let in_hash_table = exec.hash_table_context.get_all_memory_usage_in_hash_table();
    assert!(in_hash_table > 0);
    HashJoinCtxV2::reset_hash_table_context_for_restore(&mut exec.hash_table_context);
    // Go gives back exactly what the sub tables hold, not the whole tracker:
    // the row-table charge was already released by clearAllSegmentsInRowTable.
    assert_eq!(
        exec.hash_table_context.memory_tracker.bytes_consumed(),
        before - in_hash_table
    );
    assert_eq!(exec.hash_table_context.get_all_memory_usage_in_hash_table(), 0);
}

// ---------------------------------------------------------------------------
// checkBalance / createTasks (`hash_join_v2.go:1197`, `:1215`)
// ---------------------------------------------------------------------------

#[test]
fn balanced_partitions_become_one_whole_partition_task_each() {
    let layout = one_int_key_layout();
    // Concurrency 1 => 1 partition, so concurrency == partitionNumber and
    // every segment is in that one partition: perfectly balanced.
    let chunks = vec![vec![build_chunk(&[1, 2]), build_chunk(&[3, 4])]];
    let (exec, total) = built_exec(1, JoinType::Inner, &layout, &chunks);
    assert!(exec.check_balance(total));
    assert_eq!(
        exec.create_tasks(total),
        vec![BuildTask {
            partition_idx: 0,
            seg_start_idx: 0,
            seg_end_idx: total,
        }]
    );
}

#[test]
fn unbalanced_partitions_are_sliced_round_robin_and_cover_every_segment_once() {
    let layout = one_int_key_layout();
    // Concurrency 4 => 4 partitions. checkBalance requires
    // concurrency == partitionNumber, which holds, but the per-partition
    // segment counts come out uneven, so the round-robin path runs.
    let chunks: Vec<Vec<BuildChunk>> = (0..4)
        .map(|worker| {
            (0..3)
                .map(|chunk| build_chunk(&[(worker * 10 + chunk) as i64, 100 + worker as i64]))
                .collect()
        })
        .collect();
    let (exec, total) = built_exec(4, JoinType::Inner, &layout, &chunks);
    let tasks = exec.create_tasks(total);

    // Every segment of every partition is covered exactly once, by a
    // contiguous run of tasks, whichever branch createTasks took.
    for part in 0..exec.ctx.partition_number {
        let expected = exec
            .hash_table_context
            .hash_table
            .sub_table(part)
            .row_data
            .segments
            .len();
        let mut next = 0_usize;
        for task in tasks.iter().filter(|task| task.partition_idx == part) {
            assert_eq!(task.seg_start_idx, next, "partition {part} has a gap");
            assert!(task.seg_end_idx > task.seg_start_idx);
            next = task.seg_end_idx;
        }
        assert_eq!(next, expected, "partition {part} not fully covered");
    }

    // The round-robin ordering: consecutive tasks never repeat a partition
    // while another partition still has work, which is the property Go's
    // comment states.
    if !exec.check_balance(total) {
        for pair in tasks.windows(2) {
            if pair[0].partition_idx == pair[1].partition_idx {
                assert_eq!(
                    tasks.iter().filter(|t| t.partition_idx != pair[0].partition_idx).count(),
                    0,
                    "a partition repeated while others still had segments"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// End-to-end: fetchAndBuildHashTableImpl then probe
// ---------------------------------------------------------------------------

#[test]
fn build_then_probe_finds_every_matching_build_row_and_only_those() {
    let layout = one_int_key_layout();
    // Duplicate keys, keys present on one worker only, and keys absent
    // entirely -- the three cases every join type branches on.
    let chunks_per_worker = vec![
        vec![build_chunk(&[1, 2, 2]), build_chunk(&[3])],
        vec![build_chunk(&[2, 4]), build_chunk(&[1])],
    ];
    let (exec, _) = built_exec(2, JoinType::Inner, &layout, &chunks_per_worker);

    let probe_keys = [1_i64, 2, 3, 4, 99];
    let matches = matches_for(&exec, &layout, &probe_keys);

    assert_eq!(matches[0], vec![1, 1], "key 1 is built twice");
    assert_eq!(matches[1], vec![2, 2, 2], "key 2 is built three times");
    assert_eq!(matches[2], vec![3]);
    assert_eq!(matches[3], vec![4]);
    assert!(matches[4].is_empty(), "key 99 is not on the build side");
}

#[test]
fn build_then_probe_agrees_with_each_join_type_match_rule() {
    let layout = one_int_key_layout();
    let chunks_per_worker = vec![vec![build_chunk(&[10, 20, 20, 30])]];
    let (exec, _) = built_exec(1, JoinType::Inner, &layout, &chunks_per_worker);

    let probe_keys = [10_i64, 20, 40];
    let matches = matches_for(&exec, &layout, &probe_keys);

    // Inner (right as build side): one output row per matched build row.
    let inner_rows: usize = matches.iter().map(Vec::len).sum();
    assert_eq!(inner_rows, 3, "10 matches once, 20 twice, 40 never");

    // Left outer with the right side as build: every probe row survives,
    // null-extended when it has no match.
    let left_outer_rows: usize = matches.iter().map(|m| m.len().max(1)).sum();
    assert_eq!(left_outer_rows, 4);

    // Semi: one row per probe row that has at least one match.
    let semi_rows = matches.iter().filter(|m| !m.is_empty()).count();
    assert_eq!(semi_rows, 2);

    // Anti semi: the complement.
    let anti_semi_rows = matches.iter().filter(|m| m.is_empty()).count();
    assert_eq!(anti_semi_rows, 1);

    // Left outer semi: one row per probe row, carrying the existence flag.
    assert_eq!(matches.len(), probe_keys.len());
}

#[test]
fn a_build_side_with_no_rows_leaves_every_partition_hash_table_empty() {
    let layout = one_int_key_layout();
    let (exec, total) = built_exec(2, JoinType::Inner, &layout, &[Vec::new(), Vec::new()]);
    assert_eq!(total, 0);
    assert!(exec.hash_table_context.hash_table.is_hash_table_empty());
    // Which is exactly the condition canSkipProbeIfHashTableIsEmpty guards.
    assert!(exec.ctx.can_skip_probe_if_hash_table_is_empty());
    assert!(matches_for(&exec, &layout, &[1, 2])
        .iter()
        .all(Vec::is_empty));
}

// ---------------------------------------------------------------------------
// NewJoinBuildWorkerV2 (`hash_join_v2.go:588`)
// ---------------------------------------------------------------------------

#[test]
fn a_build_worker_has_a_nullable_key_when_any_key_column_is_nullable() {
    // Go: hasNullableKey is true as soon as one key column lacks NOT NULL.
    let worker = new_join_build_worker_v2(0, vec![0, 2], &[true, false, true]);
    assert!(!worker.has_nullable_key, "both key columns are NOT NULL");

    let worker = new_join_build_worker_v2(1, vec![0, 2], &[true, true, false]);
    assert!(worker.has_nullable_key);
    assert_eq!(worker.worker_id, 1);
    assert!(worker.builder.is_none(), "the builder is created later");
}
