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

//! Ports of `pkg/statistics/handle/handletest/statstest/stats_test.go` from
//! `origin/master` (17 Go test functions).
//!
//! Every Go test in that file drives the full statistics-handle runtime
//! through `testkit.CreateMockStoreAndDomain`: SQL sessions run DDL/DML and
//! `ANALYZE`, a background-ish `StatsHandle` loads stats via
//! `GetPhysicalTableStats` / `InitStats` / `Update`, and several tests flip
//! session/global config or enable failpoints inside the handle's cache and
//! load paths. `tidb-stats` owns only the dependency-closed leaf primitives
//! (Table/Column/Index shapes, existence maps, cache bookkeeping); there is no
//! session, domain, mock store, ANALYZE executor, or failpoint machinery on
//! the Rust side yet.
//!
//! Each Go test is therefore pinned as an `#[ignore]`d marker whose intent was
//! re-derived from the Go source it exercises. None of the asserted behavior
//! is approximated; when the handle runtime lands, these markers become the
//! porting checklist for the real tests.

// stats_test.go::TestStatsCacheProcess — after ANALYZE, GetPhysicalTableStats
// returns non-pseudo stats with a non-zero version while MaxTableStatsVersion
// stays unchanged ("analyze should not move forward the stats cache version"),
// whereas flushing deltas + Handle.Update does move it forward.
#[test]
#[ignore = "go-parity-gap: requires testkit session/domain plus StatsHandle runtime (GetPhysicalTableStats, MaxTableStatsVersion, GetNextCheckVersionWithOffset) not present in tidb-stats"]
fn stats_cache_process() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestStatsCache — table stats stay usable (non-pseudo) across
// schema changes: a newly added index without fresh stats yields no idx entry
// (GetIdx == nil) but keeps the table non-pseudo; dropping and adding columns
// followed by Clear + Update also keep the table stats working.
#[test]
#[ignore = "go-parity-gap: requires testkit session/domain plus StatsHandle runtime (analyze, create index, alter table drop/add column, Clear+Update cycle) not present in tidb-stats"]
fn stats_cache() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestStatsCacheMemTracker — same schema-change flow as
// TestStatsCache but additionally asserts MemoryUsage().TotalMemUsage: zero
// for pseudo stats, > 0 after analyze + reload, while the table remains
// non-pseudo throughout.
#[test]
#[ignore = "go-parity-gap: requires testkit session/domain plus StatsHandle runtime with memory-usage accounting through analyze/reload cycles not present in tidb-stats"]
fn stats_cache_mem_tracker() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestStatsStoreAndLoad — stats written by ANALYZE and then
// reloaded from storage after Handle.Clear + Update must be AssertTableEqual
// to the in-memory copy (same RealtimeCount = 1000 rows).
#[test]
#[ignore = "go-parity-gap: requires testkit session/domain plus StatsHandle store/load round-trip and internal.AssertTableEqual over full Table payloads not present in tidb-stats"]
fn stats_store_and_load() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsMemTraceWithLite — InitStats over 9 analyzed
// tables consumes exactly the sum of the loaded tables' MemoryUsage
// TotalMemUsage (h.MemConsumed() == memCostTot), with LiteInitStats=true.
#[test]
#[ignore = "go-parity-gap: requires StatsHandle.InitStats runtime over mysql.stats_* tables plus global config (Performance.LiteInitStats) and MemConsumed tracking not present in tidb-stats"]
fn init_stats_mem_trace_with_lite() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsMemTraceWithoutLite — same memory-trace
// assertion as the lite variant, with LiteInitStats=false.
#[test]
#[ignore = "go-parity-gap: requires StatsHandle.InitStats runtime over mysql.stats_* tables plus global config (Performance.LiteInitStats) and MemConsumed tracking not present in tidb-stats"]
fn init_stats_mem_trace_without_lite() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsMemTraceWithConcurrentLite — same memory-trace
// assertion under the concurrent-init-stats path with lite loading enabled.
#[test]
#[ignore = "go-parity-gap: requires concurrent StatsHandle.InitStats runtime plus global config toggles and MemConsumed tracking not present in tidb-stats"]
fn init_stats_mem_trace_with_concurrent_lite() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsMemTraceWithoutConcurrentLite — same memory-trace
// assertion under the concurrent-init-stats path with lite loading disabled.
#[test]
#[ignore = "go-parity-gap: requires concurrent StatsHandle.InitStats runtime plus global config toggles and MemConsumed tracking not present in tidb-stats"]
fn init_stats_mem_trace_without_concurrent_lite() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStats — InitStats after `analyze table t all columns
// with 2 topn, 2 buckets` yields: analyzed-table meta (ModifyCount=0,
// RealtimeCount=6, StatsVer=Version2), per-index TopN count=2 /
// TotalRowCount=6 / hist len=2 fully loaded, and all columns evicted-but-
// initialized with NDV=6; a second un-analyzed table yields Version0 meta with
// empty/uninitialized indexes and columns; a third analyzed with `predicate
// columns` leaves non-predicate column c uninitialized while predicate
// columns are analyzed-and-evicted.
#[test]
#[ignore = "go-parity-gap: requires testkit ANALYZE execution, statstestutil.HandleNextDDLEventWithTxn, and StatsHandle.InitStats runtime not present in tidb-stats"]
fn init_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsForPartitionedTable — same three-table scenario
// as TestInitStats but range-partitioned with a global index: global-level and
// per-partition stats must each satisfy the analyzed/non-analyzed/predicate-
// column shape checks with partition-scoped row counts (3 rows per partition).
#[test]
#[ignore = "go-parity-gap: requires testkit ANALYZE of partitioned tables with global indexes plus StatsHandle.InitStats runtime not present in tidb-stats"]
fn init_stats_for_partitioned_table() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsWithoutHandlingDDLEvent — with stats_meta rows
// flushed but no histogram meta and no DDL event handled, InitStats produces
// non-pseudo, non-analyzed stats (Version0, ModifyCount=RealtimeCount=6) whose
// existence map contains neither the index nor any column.
#[test]
#[ignore = "go-parity-gap: requires testkit flush-stats-delta plus StatsHandle.InitStats runtime over partially populated metadata not present in tidb-stats"]
fn init_stats_without_handling_ddl_event() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsVer2 — after add-column DDL handling and
// predicate-columns ANALYZE at version 2, InitStats (with lease-based load by
// need disabled via LiteInitStats=false) yields 5 initialized/evicted columns
// where the new column e is initialized and old column d is not; repeating
// Clear + InitStats reproduces an AssertTableEqual table.
#[test]
#[ignore = "go-parity-gap: requires testkit ANALYZE + DDL event handling plus StatsHandle lease/SetLease and InitStats runtime not present in tidb-stats"]
fn init_stats_ver2() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStats51358 — primary-key columns load without a TopN
// (is_index=false cannot load topn) while every column reports IsFullLoad ==
// false, exercised under the StatsCacheGetNil failpoint so every cache hit
// misses.
#[test]
#[ignore = "go-parity-gap: requires StatsHandle lease/failpoint-driven cache-miss path (StatsCacheGetNil) and InitStats runtime not present in tidb-stats"]
fn init_stats_51358() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsIssue41938 — InitStats must succeed for a
// timestamp-primary-key table analyzed with 0 topn (no panic/error from
// timestamp-encoded keys during initialization).
#[test]
#[ignore = "go-parity-gap: requires testkit ANALYZE of a timestamp-primary-key table plus StatsHandle.InitStats runtime not present in tidb-stats"]
fn init_stats_issue_41938() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestDumpStatsDeltaInBatch — flushing two tables' deltas in
// one statement writes one stats_meta row per table with modify_count=count=3
// and identical versions (both dumped inside one transaction).
#[test]
#[ignore = "go-parity-gap: requires testkit session with FLUSH STATS_DELTA statement execution and mysql.stats_meta query surface not present in tidb-stats"]
fn dump_stats_delta_in_batch() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsForTableWithTopNButNoBuckets — default ANALYZE
// of a small table stores only TopN (idx.TopN.TotalCount == 6, hist len 0,
// fully loaded, marked analyzed) and InitStats restores exactly that shape.
#[test]
#[ignore = "go-parity-gap: requires testkit ANALYZE plus StatsHandle.InitStats runtime over topn-only histograms not present in tidb-stats"]
fn init_stats_for_table_with_top_n_but_no_buckets() {
    unreachable!("gated by go-parity-gap ignore")
}

// stats_test.go::TestInitStatsMemoryFullBlocksBucketsButKeepsTopN — with the
// mockBucketsLoadMemoryLimit failpoint simulating memory exhaustion after
// TopN, InitStats loads the index TopN (non-empty count, populated total row
// count) but leaves the histogram buckets blocked (IsFullLoad false, hist len
// 0).
#[test]
#[ignore = "go-parity-gap: requires the mockBucketsLoadMemoryLimit failpoint inside the bucket-load path plus StatsHandle.InitStats runtime not present in tidb-stats"]
fn init_stats_memory_full_blocks_buckets_but_keeps_top_n() {
    unreachable!("gated by go-parity-gap ignore")
}
