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

//! Gap tests for Go `pkg/executor/distsql_test.go`. The Go cases cover the
//! TiKV coprocessor client, index consistency checks, index-lookup runtime
//! stats, paging/batching, replica-read metrics, and failpoint-driven request
//! recovery. The Rust executor has no Go-compatible `GetLackHandles`
//! (`pkg/executor/distsql.go:2259`), `IndexLookUpRunTimeStats`
//! (`pkg/executor/distsql.go:1939`), or session/TiKV coprocessor test seam.

/// Go `pkg/executor/distsql_test.go:68::TestCopClientSend` is intentionally
/// skipped by Go at `:69` as unstable; no Rust test is generated for it.

/// Go `pkg/executor/distsql_test.go:140::TestGetLackHandles`.
/// `GetLackHandles` (`pkg/executor/distsql.go:2259`) removes obtained handles
/// from the map and returns expected handles that were not observed.
#[test]
#[ignore = "go-parity-gap: Go kv.HandleMap/GetLackHandles is a private storage helper with no Rust counterpart"]
fn get_lack_handles_returns_unobserved_handles() {}

/// Go `pkg/executor/distsql_test.go:166::TestInconsistentIndex`.
/// The Go index-reader path detects an index count that exceeds the record
/// count and emits executor error 8133; it deliberately does not check the
/// query with an additional residual predicate (`pkg/executor/distsql_test.go:200-204`).
#[test]
#[ignore = "go-parity-gap: direct index corruption, TiKV transactions, and executor error 8133 are unported"]
fn inconsistent_index_is_reported_only_for_the_unfiltered_index_read() {}

/// Go `pkg/executor/distsql_test.go:218::TestPartitionTableRandomlyIndexLookUpReader`.
/// Partitioned index lookup results must match a normal table across 256
/// randomized range predicates (`pkg/executor/distsql_test.go:224-251`).
#[test]
#[ignore = "go-parity-gap: partitioned TiKV index-lookup execution and randomized storage comparison are unported"]
fn partition_table_randomly_index_lookup_matches_normal_table() {}

/// Go `pkg/executor/distsql_test.go:254::TestIndexLookUpStats`.
/// `IndexLookUpRunTimeStats::String`, `Clone`, and `Merge`
/// (`pkg/executor/distsql.go:1939-2006`) must render and double the timing,
/// task-count, and wait fields as asserted by the Go test.
#[test]
#[ignore = "go-parity-gap: IndexLookUpRunTimeStats has no Rust runtime-stat carrier"]
fn index_lookup_stats_clone_merge_and_render() {}

/// Go `pkg/executor/distsql_test.go:276::TestPartitionTableIndexJoinIndexLookUp`.
/// Dynamic hash-partition index lookup joins must match the unpartitioned
/// reference query over randomized predicates (`pkg/executor/distsql_test.go:280-304`).
#[test]
#[ignore = "go-parity-gap: dynamic partitioned index-lookup joins over TiKV are unported"]
fn partition_table_index_join_index_lookup_matches_reference() {}

/// Go `pkg/executor/distsql_test.go:307::TestCoprocessorPagingSize`.
/// Changing `tidb_min_paging_size` changes the coprocessor RPC count in
/// `EXPLAIN ANALYZE` (`pkg/executor/distsql_test.go:351-362`).
#[test]
#[ignore = "go-parity-gap: coprocessor paging protocol, session variables, and RPC execution counters are unported"]
fn coprocessor_paging_size_changes_rpc_count() {}

/// Go `pkg/executor/distsql_test.go:365::TestAdaptiveClosestRead`.
/// The classic Go kernel compares estimated reader cost with
/// `tidb_adaptive_closest_read_threshold` and checks hit/miss metrics across
/// table, index, lookup, partition, and index-merge readers
/// (`pkg/executor/distsql_test.go:369-468`).
#[test]
#[ignore = "go-parity-gap: adaptive closest-replica selection, statistics, and DistSQL metrics are unported"]
fn adaptive_closest_read_updates_hit_and_miss_metrics() {}

/// Go `pkg/executor/distsql_test.go:470::TestCoprocessorPagingReqKeyRangeSorted`.
/// The `checkKeyRangeSortedForPaging` failpoint exercises prepared statements
/// whose coprocessor paging key ranges must remain sorted
/// (`pkg/executor/distsql_test.go:474-528`).
#[test]
#[ignore = "go-parity-gap: coprocessor paging key-range validation and failpoint hooks are unported"]
fn coprocessor_paging_keeps_prepared_key_ranges_sorted() {}

/// Go `pkg/executor/distsql_test.go:530::TestCoprocessorBatchByStore`.
/// Batching ranges by store must preserve ordered and unordered query results
/// while region-error fallback retries every batch (`pkg/executor/distsql_test.go:545-585`).
#[test]
#[ignore = "go-parity-gap: TiKV region batching, paging, and coprocessor region-error fallback are unported"]
fn coprocessor_batch_by_store_preserves_rows_after_region_fallback() {}

/// Go `pkg/executor/distsql_test.go:588::TestCoprCacheWithoutExecutionInfo`.
/// A coprocessor-cache hit must still return rows when execution-info
/// collection is disabled between the first and second request
/// (`pkg/executor/distsql_test.go:597-607`).
#[test]
#[ignore = "go-parity-gap: unistore coprocessor-cache failpoint and execution-info request hook are unported"]
fn copr_cache_without_execution_info_still_returns_rows() {}

/// Go `pkg/executor/distsql_test.go:610::TestIndexLookUpPushDownCopTask`.
/// Index-lookup pushdown disables paging and coprocessor cache for its request
/// and reports a `LocalIndexLookUp` plan (`pkg/executor/distsql_test.go:625-656`).
#[test]
#[ignore = "go-parity-gap: index-lookup pushdown, coprocessor request hooks, and EXPLAIN ANALYZE are unported"]
fn index_lookup_pushdown_disables_paging_and_cop_cache() {}

/// Go `pkg/executor/distsql_test.go:659::TestPartitionIndexLookUpMergeWithSkewedPartitions`.
/// Skewed 100-partition index lookup must issue multiple concurrent index
/// scans, avoid table scans, and return 3,000 ordered-limit rows
/// (`pkg/executor/distsql_test.go:670-728`).
#[test]
#[ignore = "go-parity-gap: concurrent partitioned index-lookup pushdown and request inspection are unported"]
fn partition_index_lookup_merge_handles_skewed_partitions() {}
