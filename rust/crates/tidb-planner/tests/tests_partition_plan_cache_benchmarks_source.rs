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

//! Documentary gap ports for `pkg/planner/core/tests/partition/bench_test.go`
//! (`pkg/planner.part15` items 886–900 on `origin/master`, package
//! `session`). All fifteen items are execution benchmarks: each prepares a
//! mock-store session (`prepareBenchSession` :72) with the per-scheme table,
//! runs `preparePointGet` /
//! `prepareIndexLookup` / `prepareTableScan` warmups, then loops b.N over a
//! point select whose EXPLAIN must still show the expected operator
//! (`runPointSelect` :122: Point_Get / Batch_Point_Get / IndexLookUp /
//! TableReader; dispatcher `runBenchmark` :294). Names keep Go's `Benchmark`
//! shape so the batch gate filter
//! `not test(/bench/)` skips them exactly as `go test` skips Benchmarks;
//! every body is a recorded gap because prepared-plan-cache execution is far
//! outside this crate's ported surface.
//!
//! | Go function (`bench_test.go`) | Rust test |
//! | --- | --- |
//! | `:312 BenchmarkNonPartitionPointGetPlanCacheOn` | [`benchmark_non_partition_point_get_plan_cache_on`] |
//! | `:316 BenchmarkNonPartitionPointGetPlanCacheOff` | [`benchmark_non_partition_point_get_plan_cache_off`] |
//! | `:320 BenchmarkNonPartitionBatchPointGetPlanCacheOn` | [`benchmark_non_partition_batch_point_get_plan_cache_on`] |
//! | `:324 BenchmarkNonPartitionBatchPointGetPlanCacheOff` | [`benchmark_non_partition_batch_point_get_plan_cache_off`] |
//! | `:328 BenchmarkNonPartitionIndexLookupPlanCacheOn` | [`benchmark_non_partition_index_lookup_plan_cache_on`] |
//! | `:332 BenchmarkNonPartitionIndexLookupPlanCacheOff` | [`benchmark_non_partition_index_lookup_plan_cache_off`] |
//! | `:336 BenchmarkNonPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_non_partition_batch_index_lookup_plan_cache_on`] |
//! | `:340 BenchmarkNonPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_non_partition_batch_index_lookup_plan_cache_off`] |
//! | `:343 BenchmarkNonPartitionTableScanPlanCacheOn` | [`benchmark_non_partition_table_scan_plan_cache_on`] |
//! | `:347 BenchmarkNonPartitionTableScanPlanCacheOff` | [`benchmark_non_partition_table_scan_plan_cache_off`] |
//! | `:351 BenchmarkNonPartitionBatchTableScanPlanCacheOn` | [`benchmark_non_partition_batch_table_scan_plan_cache_on`] |
//! | `:355 BenchmarkNonPartitionBatchTableScanPlanCacheOff` | [`benchmark_non_partition_batch_table_scan_plan_cache_off`] |
//! | `:359 BenchmarkNonPartition` | [`benchmark_non_partition_all_access_kinds_plain`] |
//! | `:363 BenchmarkHashPartitionPointGetPlanCacheOn` | [`benchmark_hash_partition_point_get_plan_cache_on`] |
//! | `:367 BenchmarkHashPartitionPointGetPlanCacheOff` | [`benchmark_hash_partition_point_get_plan_cache_off`] |

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_non_partition_batch_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: runs all four access-kind benchmark shapes against a non-partitioned table"]
fn benchmark_non_partition_all_access_kinds_plain() {}

#[test]
#[ignore = "go-parity-gap: hash-partition variants prune via partitionByHash=hash(id) partitions 7"]
fn benchmark_hash_partition_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: hash-partition variants prune via partitionByHash=hash(id) partitions 7"]
fn benchmark_hash_partition_point_get_plan_cache_off() {}
