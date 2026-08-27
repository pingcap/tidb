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
//! (`pkg/planner.part16`, items 901–960 on `origin/master`, package
//! `session`). All sixty items are execution benchmarks driven by
//! `runBenchmark` (`bench_test.go:294`) over a mock-store session
//! (`prepareBenchSession` :72): per-scheme partitioned DDL + fixture inserts +
//! `analyze table t` warmup (`preparePointGet` :196, `prepareIndexLookup`
//! :218, `prepareTableScan` :224 feeding `insert1kRows` :203), then
//! `runPointSelect` :122 verifies `explain <query>`'s first row starts with the
//! expected operator and counts `FoundInPlanCache` hits across the `b.N`
//! loop. Names keep Go's `Benchmark` shape so the batch gate filter
//! `not test(/bench/)` skips them exactly as `go test` skips Benchmarks;
//! every body stays empty because session/executor/plan-cache runtime does not
//! exist in this crate (no `FoundInPlanCache`, no fast-plan gate — the local
//! `access_path::PointGetAdmission` enum is a pass-through marker, not Go's
//! decision).
//!
//! Behaviors these ports document (re-derived from `origin/master`):
//! - schemes: `partitionByHash` :51 = `partition by hash(id) partitions 7`,
//!   `partitionByHashExpr` :52 = `partition by hash(floor(id*0.5))
//!   partitions 7`, `partitionByKey` :53 = `partition by key(id)
//!   partitions 7`, List/ListExpr families generate their DDL through
//!   `getListPartitionDef` :516 — ranges start at {1, 5000, 10000, 99900},
//!   each holding 256 consecutive values, and because every call site passes
//!   a non-empty `expr`, the branch `expr != "" && i == 1` also appends value
//!   `0` to partition p1 for both `"id"` and `"floor(id*0.5)*2"` variants
//!   (:528-531).
//! - queries: `pointQuery` = `select * from t where id = 1`;
//!   `batchPointQuery` = `select * from t where id = 1 or id = 5000 or id =
//!   2 or id = 100000` (:46) whose comment :45 notes IN-or shapes route
//!   through TryFastPlan and never hit the non-prepared plan cache.
//! - expected EXPLAIN first-row prefixes (`strings.HasPrefix` :166):
//!   `Point_Get` / `IndexLookUp` / `TableReader`; for EVERY partitioned
//!   scheme the batch query's expectation is demoted from `Batch_Point_Get`
//!   to `TableReader` — `benchmarkPointGetPlanCache` :250: "Batch_Point_Get
//!   is not yet enabled for partitioned tables!".
//! - plan-cache flag: PlanCacheOn runs the flush ritual (`set
//!   tidb_session_plan_cache_size = 0`, create/drop `tTemp`, two probe
//!   shapes, restore default size, :127-151); `expectHits` :160 counts only
//!   IndexLookUp/TableReader rows, never [Batch]PointGet.
//!
//! | Go function (`bench_test.go`) | Rust test |
//! | --- | --- |
//! | `:371 BenchmarkHashPartitionBatchPointGetPlanCacheOn` | [`benchmark_hash_partition_batch_point_get_plan_cache_on`] |
//! | `:375 BenchmarkHashPartitionBatchPointGetPlanCacheOff` | [`benchmark_hash_partition_batch_point_get_plan_cache_off`] |
//! | `:379 BenchmarkHashPartitionIndexLookupPlanCacheOn` | [`benchmark_hash_partition_index_lookup_plan_cache_on`] |
//! | `:383 BenchmarkHashPartitionIndexLookupPlanCacheOff` | [`benchmark_hash_partition_index_lookup_plan_cache_off`] |
//! | `:387 BenchmarkHashPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_hash_partition_batch_index_lookup_plan_cache_on`] |
//! | `:391 BenchmarkHashPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_hash_partition_batch_index_lookup_plan_cache_off`] |
//! | `:394 BenchmarkHashPartitionTableScanPlanCacheOn` | [`benchmark_hash_partition_table_scan_plan_cache_on`] |
//! | `:398 BenchmarkHashPartitionTableScanPlanCacheOff` | [`benchmark_hash_partition_table_scan_plan_cache_off`] |
//! | `:402 BenchmarkHashPartitionBatchTableScanPlanCacheOn` | [`benchmark_hash_partition_batch_table_scan_plan_cache_on`] |
//! | `:406 BenchmarkHashPartitionBatchTableScanPlanCacheOff` | [`benchmark_hash_partition_batch_table_scan_plan_cache_off`] |
//! | `:410 BenchmarkHashPartition` | [`benchmark_hash_partition_all_access_kinds`] |
//! | `:414 BenchmarkHashExprPartitionPointGetPlanCacheOn` | [`benchmark_hash_expr_partition_point_get_plan_cache_on`] |
//! | `:418 BenchmarkHashExprPartitionPointGetPlanCacheOff` | [`benchmark_hash_expr_partition_point_get_plan_cache_off`] |
//! | `:422 BenchmarkHashExprPartitionBatchPointGetPlanCacheOn` | [`benchmark_hash_expr_partition_batch_point_get_plan_cache_on`] |
//! | `:426 BenchmarkHashExprPartitionBatchPointGetPlanCacheOff` | [`benchmark_hash_expr_partition_batch_point_get_plan_cache_off`] |
//! | `:430 BenchmarkHashExprPartitionIndexLookupPlanCacheOn` | [`benchmark_hash_expr_partition_index_lookup_plan_cache_on`] |
//! | `:434 BenchmarkHashExprPartitionIndexLookupPlanCacheOff` | [`benchmark_hash_expr_partition_index_lookup_plan_cache_off`] |
//! | `:438 BenchmarkHashExprPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_hash_expr_partition_batch_index_lookup_plan_cache_on`] |
//! | `:442 BenchmarkHashExprPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_hash_expr_partition_batch_index_lookup_plan_cache_off`] |
//! | `:445 BenchmarkHashExprPartitionTableScanPlanCacheOn` | [`benchmark_hash_expr_partition_table_scan_plan_cache_on`] |
//! | `:449 BenchmarkHashExprPartitionTableScanPlanCacheOff` | [`benchmark_hash_expr_partition_table_scan_plan_cache_off`] |
//! | `:453 BenchmarkHashExprPartitionBatchTableScanPlanCacheOn` | [`benchmark_hash_expr_partition_batch_table_scan_plan_cache_on`] |
//! | `:457 BenchmarkHashExprPartitionBatchTableScanPlanCacheOff` | [`benchmark_hash_expr_partition_batch_table_scan_plan_cache_off`] |
//! | `:461 BenchmarkHashExprPartition` | [`benchmark_hash_expr_partition_all_access_kinds`] |
//! | `:465 BenchmarkKeyPartitionPointGetPlanCacheOn` | [`benchmark_key_partition_point_get_plan_cache_on`] |
//! | `:469 BenchmarkKeyPartitionPointGetPlanCacheOff` | [`benchmark_key_partition_point_get_plan_cache_off`] |
//! | `:473 BenchmarkKeyPartitionBatchPointGetPlanCacheOn` | [`benchmark_key_partition_batch_point_get_plan_cache_on`] |
//! | `:477 BenchmarkKeyPartitionBatchPointGetPlanCacheOff` | [`benchmark_key_partition_batch_point_get_plan_cache_off`] |
//! | `:481 BenchmarkKeyPartitionIndexLookupPlanCacheOn` | [`benchmark_key_partition_index_lookup_plan_cache_on`] |
//! | `:485 BenchmarkKeyPartitionIndexLookupPlanCacheOff` | [`benchmark_key_partition_index_lookup_plan_cache_off`] |
//! | `:489 BenchmarkKeyPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_key_partition_batch_index_lookup_plan_cache_on`] |
//! | `:493 BenchmarkKeyPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_key_partition_batch_index_lookup_plan_cache_off`] |
//! | `:496 BenchmarkKeyPartitionTableScanPlanCacheOn` | [`benchmark_key_partition_table_scan_plan_cache_on`] |
//! | `:500 BenchmarkKeyPartitionTableScanPlanCacheOff` | [`benchmark_key_partition_table_scan_plan_cache_off`] |
//! | `:504 BenchmarkKeyPartitionBatchTableScanPlanCacheOn` | [`benchmark_key_partition_batch_table_scan_plan_cache_on`] |
//! | `:508 BenchmarkKeyPartitionBatchTableScanPlanCacheOff` | [`benchmark_key_partition_batch_table_scan_plan_cache_off`] |
//! | `:512 BenchmarkKeyPartition` | [`benchmark_key_partition_all_access_kinds`] |
//! | `:542 BenchmarkListPartitionPointGetPlanCacheOn` | [`benchmark_list_partition_point_get_plan_cache_on`] |
//! | `:546 BenchmarkListPartitionPointGetPlanCacheOff` | [`benchmark_list_partition_point_get_plan_cache_off`] |
//! | `:550 BenchmarkListPartitionBatchPointGetPlanCacheOn` | [`benchmark_list_partition_batch_point_get_plan_cache_on`] |
//! | `:554 BenchmarkListPartitionBatchPointGetPlanCacheOff` | [`benchmark_list_partition_batch_point_get_plan_cache_off`] |
//! | `:558 BenchmarkListPartitionIndexLookupPlanCacheOn` | [`benchmark_list_partition_index_lookup_plan_cache_on`] |
//! | `:562 BenchmarkListPartitionIndexLookupPlanCacheOff` | [`benchmark_list_partition_index_lookup_plan_cache_off`] |
//! | `:566 BenchmarkListPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_list_partition_batch_index_lookup_plan_cache_on`] |
//! | `:570 BenchmarkListPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_list_partition_batch_index_lookup_plan_cache_off`] |
//! | `:573 BenchmarkListPartitionTableScanPlanCacheOn` | [`benchmark_list_partition_table_scan_plan_cache_on`] |
//! | `:577 BenchmarkListPartitionTableScanPlanCacheOff` | [`benchmark_list_partition_table_scan_plan_cache_off`] |
//! | `:581 BenchmarkListPartitionBatchTableScanPlanCacheOn` | [`benchmark_list_partition_batch_table_scan_plan_cache_on`] |
//! | `:585 BenchmarkListPartitionBatchTableScanPlanCacheOff` | [`benchmark_list_partition_batch_table_scan_plan_cache_off`] |
//! | `:589 BenchmarkListPartition` | [`benchmark_list_partition_all_access_kinds`] |
//! | `:593 BenchmarkListExprPartitionPointGetPlanCacheOn` | [`benchmark_list_expr_partition_point_get_plan_cache_on`] |
//! | `:597 BenchmarkListExprPartitionPointGetPlanCacheOff` | [`benchmark_list_expr_partition_point_get_plan_cache_off`] |
//! | `:601 BenchmarkListExprPartitionBatchPointGetPlanCacheOn` | [`benchmark_list_expr_partition_batch_point_get_plan_cache_on`] |
//! | `:605 BenchmarkListExprPartitionBatchPointGetPlanCacheOff` | [`benchmark_list_expr_partition_batch_point_get_plan_cache_off`] |
//! | `:609 BenchmarkListExprPartitionIndexLookupPlanCacheOn` | [`benchmark_list_expr_partition_index_lookup_plan_cache_on`] |
//! | `:613 BenchmarkListExprPartitionIndexLookupPlanCacheOff` | [`benchmark_list_expr_partition_index_lookup_plan_cache_off`] |
//! | `:617 BenchmarkListExprPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_list_expr_partition_batch_index_lookup_plan_cache_on`] |
//! | `:621 BenchmarkListExprPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_list_expr_partition_batch_index_lookup_plan_cache_off`] |
//! | `:624 BenchmarkListExprPartitionTableScanPlanCacheOn` | [`benchmark_list_expr_partition_table_scan_plan_cache_on`] |
//! | `:628 BenchmarkListExprPartitionTableScanPlanCacheOff` | [`benchmark_list_expr_partition_table_scan_plan_cache_off`] |

// --- HashPartition family: `partitionByHash` :51 -------------------------------

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByHash, batchPointQuery, "TableReader", pointGet, true)
    // — TableReader because "Batch_Point_Get is not yet enabled for partitioned tables!" (:250).
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByHash, batchPointQuery, "TableReader", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByHash, pointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByHash, pointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByHash, batchPointQuery, "IndexLookUp", indexLookup, true);
    // expectHits (:160) applies — only IndexLookUp/TableReader rows count hits.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByHash, batchPointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByHash, pointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByHash, pointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByHash, batchPointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByHash, batchPointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, partitionByHash) :230 — twelve b.Run leaves;
    // its batch arm pins the TableReader demotion (:248-252).
}

// --- HashExprPartition family: `partitionByHashExpr` :52 -----------------------

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByHashExpr, pointQuery, "Point_Get", pointGet, true)
    // — expectHits false: Point_Get never counts plan-cache hits (:160).
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByHashExpr, pointQuery, "Point_Get", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByHashExpr, batchPointQuery, "TableReader", pointGet, true) — :250 demotion.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByHashExpr, batchPointQuery, "TableReader", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByHashExpr, pointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByHashExpr, pointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByHashExpr, batchPointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByHashExpr, batchPointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByHashExpr, pointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByHashExpr, pointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByHashExpr, batchPointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByHashExpr, batchPointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_hash_expr_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, partitionByHashExpr).
}

// --- KeyPartition family: `partitionByKey` :53 ---------------------------------

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByKey, pointQuery, "Point_Get", pointGet, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByKey, pointQuery, "Point_Get", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByKey, batchPointQuery, "TableReader", pointGet, true) — :250 demotion.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByKey, batchPointQuery, "TableReader", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByKey, pointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByKey, pointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByKey, batchPointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByKey, batchPointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByKey, pointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByKey, pointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByKey, batchPointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByKey, batchPointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_key_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, partitionByKey).
}

// --- ListPartition family: getListPartitionDef("id", false) :516 ---------------

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_point_get_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", false), pointQuery, "Point_Get", pointGet, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_point_get_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", false), pointQuery, "Point_Get", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, "TableReader", pointGet, true) — :250 demotion.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, "TableReader", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", false), pointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", false), pointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", false), pointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", false), pointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", false), batchPointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, getListPartitionDef("id", false)).
}

// --- ListExprPartition family: getListPartitionDef("floor(id*0.5)*2", false) ---

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_point_get_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, "Point_Get", pointGet, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_point_get_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, "Point_Get", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, "TableReader", pointGet, true) — :250 demotion.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, "TableReader", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), pointQuery, "TableReader", tableScan, false)
    // Last item of part16: item 961 (:632 BatchTableScan On) belongs to the next part.
}
