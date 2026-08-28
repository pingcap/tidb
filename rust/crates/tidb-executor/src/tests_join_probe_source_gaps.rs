// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 license (see the License file at the crate root).

//! Gap tests for `pkg/executor/join`'s probe-driver, hash-table-v1, and
//! join-runtime-stats tests.
//!
//! The probe/spill tests build a whole `HashJoinV2Exec`
//! (`pkg/executor/join/hash_join_v2.go`) from
//! `pkg/executor/internal/testutil.MockDataSource` fixtures and a
//! `hashJoinInfo` plan mock (`pkg/executor/join/hash_join_test_util.go:53
//! buildHashJoinV2Exec`), then compare against nested-loop reference
//! generators. This workspace ports the probe MECHANICS
//! (`tidb-exec/src/base_join_probe.rs`, `hash_join_v2.rs`, including
//! `AntiSemiJoinProbe`), but the executor fixture layer (MockDataSource,
//! `testutil.GenRandomChunks`, physical-plan mocks, `slowWorkers`
//! failpoint) is not in any crate's dependency closure, and the probe
//! packages live one dependency edge below `tidb-executor`
//! (`tidb-exec` -> `tidb-executor`), so no test here can drive them. The
//! written-not-ported equivalents that pin the shared probe mechanics live
//! in `tidb-exec/tests/base_join_probe_source.rs` and
//! `tidb-exec/tests/hash_table_v2_source.rs`.
//!
//! The v1 hash table and runtime-stats tests pin structs that are simply
//! absent from the Rust workspace; each entry names them.

// --- anti semi join probe (anti_semi_join_probe_test.go) -------------------

/// Go `pkg/executor/join/anti_semi_join_probe_test.go:285::TestAntiSemiJoinBasic`:
/// anti-semi join over 10k-row data sources in all four orientations (left
/// build / right build x with/without an `eq` other-condition), compared
/// against the nested-loop reference `genAntiSemiJoinResult`
/// (`anti_semi_join_probe_test.go:33`): a left row emits only when NO right
/// row matches, and a NULL in either join key makes the pair skip.
#[test]
#[ignore = "go-parity-gap: the HashJoinV2Exec probe driver (hash_join_test_util.go:53 buildHashJoinV2Exec + testutil.MockDataSource) is outside every crate's dependency closure"]
fn anti_semi_join_basic_matches_nested_loop_reference() {}

/// Go `pkg/executor/join/anti_semi_join_probe_test.go:292::TestAntiSemiJoinDuplicateKeys`:
/// the same four orientations with DUPLICATE build-side keys -- an anti-semi
/// probe row must be suppressed if any of the duplicates matches, and the
/// duplicates themselves never reach the output.
#[test]
#[ignore = "go-parity-gap: the HashJoinV2Exec probe driver (hash_join_test_util.go:53) is outside every crate's dependency closure"]
fn anti_semi_join_duplicate_keys_match_nested_loop_reference() {}

/// Go `pkg/executor/join/anti_semi_join_probe_test.go:300::TestNotInWithAntiSemi`
/// (`select * from t1 where col1 not in (select col1 from t2 where
/// t1.col0 = t2.col0)`): the `IN` operand columns ride the other condition
/// as `InOperand` eq conjuncts (`anti_semi_join_probe_test.go:246-252`), and
/// a NULL anywhere yields no output row, checked against a 10k-row
/// expectation built row by row.
#[test]
#[ignore = "go-parity-gap: the HashJoinV2Exec probe driver + InOperand other-condition fixtures are outside every crate's dependency closure"]
fn not_in_with_anti_semi_matches_nested_loop_reference() {}

/// Go `pkg/executor/join/anti_semi_join_probe_test.go:305::TestAntiSemiJoinProbeBasic`:
/// the `testJoinProbe` matrix over anti-semi joins -- used-column subsets
/// (empty, partial, full), int-vs-uint key typing, multiple join keys, both
/// build sides, 4-way partitioned build, nullable and not-null variants, 200
/// input rows.
#[test]
#[ignore = "go-parity-gap: the testJoinProbe fixture matrix (inner_join_probe_test.go:228) needs MockDataSource + planner mocks that no crate owns"]
fn anti_semi_join_probe_used_columns_and_key_typing_matrix() {}

/// Go `pkg/executor/join/anti_semi_join_probe_test.go:357::TestAntiSemiJoinProbeAllJoinKeys`:
/// every supported key type (tiny/int/uint/year/duration/enum/set/bit/json/
/// float/double/string/datetime/decimal/timestamp/date/binaryString) as a
/// single anti-semi join key, plus the four composite-key layout classes
/// (fixed-inlined, variable-inlined, fixed-overflow, variable-overflow),
/// nullable variants included.
#[test]
#[ignore = "go-parity-gap: the testJoinProbe fixture matrix (inner_join_probe_test.go:228) needs MockDataSource + planner mocks that no crate owns"]
fn anti_semi_join_probe_all_join_key_types() {}

/// Go `pkg/executor/join/anti_semi_join_probe_test.go:446::TestAntiSemiJoinJoinProbeWithSel`:
/// anti-semi probe with a `gt` other condition over nullable columns while
/// input chunks carry a selection vector, for both build sides.
#[test]
#[ignore = "go-parity-gap: the testJoinProbe fixture matrix with sel=True needs MockDataSource + planner mocks that no crate owns"]
fn anti_semi_join_probe_with_sel_and_other_condition() {}

// --- hash join v1 (hash_table_v1_test.go) -----------------------------------

/// Go `pkg/executor/join/hash_table_v1_test.go:86::TestHashRowContainer`:
/// `newHashRowContainer` (`pkg/executor/join/hash_table_v1.go:111`) accepts
/// two 10-row chunks on key columns (1,2); `GetMatchedRowsAndPtrs`
/// (:261) must return both stored rows for row 1 of the probe chunk in
/// insertion order; `ShallowCopy` (:129) must preserve `stat.probeCollision`
/// and `stat.buildTableElapse`; a collision-forcing hash (`Sum64() == 0`)
/// must drive `probeCollision` above 0; and with a 1-byte memory limit the
/// container must spill (`AlreadySpilledSafeForTest`) with disk-tracker
/// bytes above 0. None of the v1 container surface (stats, shallow copy,
/// spill actions, memory trackers) is ported.
#[test]
#[ignore = "go-parity-gap: hashRowContainer's stat/ShallowCopy/spill surface (hash_table_v1.go:92-261) is unported; the Rust v1 hash join has no probe-collision stats or spill actions"]
fn hash_row_container_matches_rows_and_tracks_collisions() {}

/// Go `pkg/executor/join/hash_table_v1_test.go:191::TestConcurrentMapHashTableMemoryUsage`:
/// `NewConcurrentMapHashTable` (`pkg/executor/join/hash_table_v1.go:668`)
/// grows its entryStore in doubling slices (64, 128, ..., 4096) over 6656
/// puts; the tracked memory usage must equal `memDelta`, dominate 75% of the
/// `RealBytes()` estimate, and reset to 0 through `GetAndCleanMemoryDelta`.
/// The entryStore memory accounting ABI has no Rust counterpart.
#[test]
#[ignore = "go-parity-gap: concurrentMapHashTable's entryStore growth and MemAware memory accounting (hash_table_v1.go:668) are unported; tidb-exec's concurrent_entry_map keeps only the shard/chain behavior"]
fn concurrent_map_hash_table_memory_usage_tracks_entry_store_growth() {}

// --- inner join probe (inner_join_probe_test.go) -----------------------------

/// Go `pkg/executor/join/inner_join_probe_test.go:494::TestInnerJoinProbeBasic`:
/// the inner-join `testJoinProbe` matrix -- used-column subsets, int-vs-uint
/// keys, multi-key joins, both build sides, 4-way partitions, nullable
/// variants -- against the nested-loop reference `genInnerJoinResult`
/// (`inner_join_probe_test.go:62`).
#[test]
#[ignore = "go-parity-gap: the testJoinProbe fixture matrix (inner_join_probe_test.go:228) needs MockDataSource + planner mocks that no crate owns"]
fn inner_join_probe_used_columns_and_key_typing_matrix() {}

/// Go `pkg/executor/join/inner_join_probe_test.go:543::TestInnerJoinProbeAllJoinKeys`:
/// every key type and the four composite-key layout classes for INNER joins,
/// both build sides, nullable variants.
#[test]
#[ignore = "go-parity-gap: the testJoinProbe fixture matrix (inner_join_probe_test.go:228) needs MockDataSource + planner mocks that no crate owns"]
fn inner_join_probe_all_join_key_types() {}

/// Go `pkg/executor/join/inner_join_probe_test.go:632::TestInnerJoinProbeOtherCondition`:
/// inner join with a `gt` other condition referencing one nullable left and
/// one right column; only rows passing it may emit, for both build sides.
#[test]
#[ignore = "go-parity-gap: the testJoinProbe fixture matrix needs MockDataSource + planner mocks that no crate owns"]
fn inner_join_probe_other_condition_filters_matches() {}

/// Go `pkg/executor/join/inner_join_probe_test.go:664::TestInnerJoinProbeWithSel`:
/// the other-condition matrix run with selection vectors on the input
/// chunks (`withSel = true`), 500 input rows.
#[test]
#[ignore = "go-parity-gap: the testJoinProbe fixture matrix with sel=True needs MockDataSource + planner mocks that no crate owns"]
fn inner_join_probe_with_sel_and_other_condition() {}

// --- inner join spill (inner_join_spill_test.go) -----------------------------

/// Go `pkg/executor/join/inner_join_spill_test.go:251::TestInnerJoinSpillBasic`:
/// six spill parameters (spill on/off x used-column subsets) drive an inner
/// join whose build side exceeds a lowered memory quota; every run must
/// return the unspilled join result with `spillChunkSize = 100` and the
/// `slowWorkers` failpoint slowing workers, and leave no leaked spill files
/// (`util.CheckNoLeakFiles`). The v2 spill driver, quota injection, and
/// spill-directory lifecycle checks are unported.
#[test]
#[ignore = "go-parity-gap: the HashJoinV2Exec spill test driver (memory-limit injection, slowWorkers failpoint, CheckNoLeakFiles) is outside every crate's dependency closure"]
fn inner_join_spill_preserves_results_across_memory_limits() {}

/// Go `pkg/executor/join/inner_join_spill_test.go:302::TestInnerJoinSpillWithOtherCondition`:
/// the same spill harness with a `gt` other condition over nullable columns
/// and per-side used columns restricted to the condition operands.
#[test]
#[ignore = "go-parity-gap: the HashJoinV2Exec spill test driver with other conditions is outside every crate's dependency closure"]
fn inner_join_spill_with_other_condition_preserves_results() {}

/// Go `pkg/executor/join/inner_join_spill_test.go:356::TestInnerJoinUnderApplyExec`:
/// the hash join executor is repeatedly closed and reopened under an Apply
/// operator while spilling (`testUnderApplyExec`), pinning that spilled
/// state is rebuilt per open. Needs the Apply operator wiring plus the spill
/// driver.
#[test]
#[ignore = "go-parity-gap: the under-Apply reopen harness needs the Apply operator + HashJoinV2Exec spill driver, unported"]
fn inner_join_under_apply_exec_reopens_spilled_state() {}

// --- join runtime stats (join_stats_test.go) ---------------------------------

/// Go `pkg/executor/join/join_stats_test.go:24::TestHashJoinRuntimeStats`:
/// `hashJoinRuntimeStats` (`pkg/executor/join/hash_join_stats.go:49`) with
/// `hashStatistic` (:122) formats as
/// `build_hash_table:{total:2s, fetch:1.9s, build:100ms}, probe:{concurrency:4, total:5s, max:2s, probe:4s, fetch and wait:1s, probe_collision:1}`,
/// `Clone()` preserves it, and `Merge(Clone())` doubles the totals while
/// keeping max and concurrency. No Rust struct ports this stats surface.
#[test]
#[ignore = "go-parity-gap: hashJoinRuntimeStats/hashStatistic (hash_join_stats.go:49/:122) are unported; Executor carries no runtime-stats surface in this workspace"]
fn hash_join_runtime_stats_format_clone_and_merge() {}

/// Go `pkg/executor/join/join_stats_test.go:42::TestIndexJoinRuntimeStats`:
/// `indexLookUpJoinRuntimeStats` with `innerWorkerRuntimeStats`
/// (`pkg/executor/join/index_lookup_join.go:860/:866`) formats as
/// `inner:{total:5s, concurrency:5, task:16, construct:100ms, fetch:300ms, build:250ms, join:150ms}, probe:1s`,
/// clones unchanged, and merges to doubled totals/task counts. Explicitly
/// named as not ported by `tidb-executor/src/index_lookup_join.rs`'s
/// narrowings list.
#[test]
#[ignore = "go-parity-gap: indexLookUpJoinRuntimeStats/innerWorkerRuntimeStats (index_lookup_join.go:860/:866) are unported; Executor carries no runtime-stats surface in this workspace"]
fn index_join_runtime_stats_format_clone_and_merge() {}
