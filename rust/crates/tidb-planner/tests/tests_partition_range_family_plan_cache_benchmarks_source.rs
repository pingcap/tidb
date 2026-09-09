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
//! (`pkg/planner.part17`, items 961–1020 of the 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line; package `session`). The range starts at
//! `:632 BenchmarkListExprPartitionBatchTableScanPlanCacheOn` — right after
//! part16 ended at `:628 BenchmarkListExprPartitionTableScanPlanCacheOff` —
//! and ends at `:992 BenchmarkNonPartitionPreparedIndexLookupPlanCacheOn`;
//! part18 continues at `:996 …PlanCacheOff`.
//!
//! Shared shape re-derived from the Go source: the non-prepared entries call
//! `runBenchmark` (`bench_test.go:294`) over a mock-store session
//! (`prepareBenchSession` :72): per-scheme partitioned DDL + fixture inserts +
//! `analyze table t` (`preparePointGet` :196, `prepareIndexLookup` :218 /
//! `prepareTableScan` :224 both feeding the doubling inserts of `insert1kRows`
//! :203 → 1024 rows), then `runPointSelect` :122 asserts `explain <query>`'s
//! first row starts with the expected operator (`strings.HasPrefix` :166) and
//! counts `GetSessionVars().FoundInPlanCache` hits (:182-183) across the b.N
//! loop. The five `…Prepared…` tail entries instead call `runBenchmarkPrepared`
//! :958 → `runPreparedPointSelect` :852: flip `tidb_enable_prepared_plan_cache`
//! (:855-858), `PrepareStmt` (:859) then loop `ExecutePreparedStmt` (:866)
//! counting `FoundInPlanCache` — unconditionally at :866-868 and a second time
//! per iteration at :877-880 when `enablePlanCache && i > 0` — with a
//! log-only (non-failing) starvation check `hits < b.N/2` (:892-894); its TODO
//! :856-857 records that the prepared path never verifies the EXPLAIN shape.
//! Names keep Go's `Benchmark` shape so the batch gate filter
//! `not test(/bench/)` skips them exactly as `go test` skips Benchmarks; every
//! body stays empty because session/executor/plan-cache runtime does not exist
//! in this crate (no `FoundInPlanCache`, no fast-plan gate).
//!
//! Behaviors these ports document (re-derived from `origin/master`):
//! - range schemes: `partitionByRange` :54 = `partition by range(id)
//!   (partition p0 values less than (10), partition p1 values less than
//!   (1000), partition p3 values less than (100000), partition pMax values
//!   less than (maxvalue))`; `partitionByRangeExpr` :55 = the same boundaries
//!   over `range(floor(id*0.5))`; `partitionByRangeColumns` :56 = `range
//!   columns (id)` over the same boundaries. Note the partition names skip
//!   `p2` (p0, p1, p3, pMax) in all three strings.
//! - list schemes for this part come from `getListPartitionDef` :516-538:
//!   ranges start at {1, 5000, 10000, 99900}, each partition holding 256
//!   consecutive values; because every call site in this part passes a
//!   non-empty `expr`, the branch `expr != "" && i == 1` (:527-531) also
//!   appends value `0` to partition `p0` (the loop binds `i` to the range
//!   *value* 1 whose `partID` is 0, so the extra value lands in `p0`, not
//!   `p1` — withdrawing part16's doc-comment claim of "p1" for this branch).
//!   It fires for BOTH call sites: `getListPartitionDef("floor(id*0.5)*2",
//!   false)` (:632-642) and `getListPartitionDef("id", true)` (:645-694).
//! - queries: `pointQuery` :42 = `select * from t where id = 1`;
//!   `batchPointQuery` :46 = `select * from t where id = 1 or id = 5000 or
//!   id = 2 or id = 100000` whose comment :45 notes IN-or shapes route through
//!   TryFastPlan and never hit the non-prepared plan cache.
//! - expected EXPLAIN first-row prefixes: `Point_Get` (:44) /
//!   `IndexLookUp` (:49) / `TableReader` (:50). For every partitioned scheme
//!   the batch query's expectation is `TableReader` — statically in these
//!   standalone benchmarks, and dynamically in the umbrellas via
//!   `benchmarkPointGetPlanCache` :248-252: "Batch_Point_Get is not yet
//!   enabled for partitioned tables!".
//! - plan-cache flag: PlanCacheOn runs the flush ritual (`set
//!   tidb_session_plan_cache_size = 0`, create/drop `tTemp`, two probe
//!   shapes, restore default size, :127-151); `expectHits` :159-160 counts
//!   only IndexLookUp/TableReader rows, never [Batch]PointGet ("already using
//!   the FastPlan" :159).
//! - prepared tail: `pointQueryPrepared` :43 = `select * from t where id = ?`
//!   with `pointArgs = 1` (:57); `batchPointQueryPrepared` :47 = `select *
//!   from t where id IN (?,?,?)` with `batchArgs = (2, 10000, 1)` (:58),
//!   wrapped by `expression.Args2Expressions4Test` (:863).
//!
//! | Go function (`bench_test.go`) | Rust test |
//! | --- | --- |
//! | `:632 BenchmarkListExprPartitionBatchTableScanPlanCacheOn` | [`benchmark_list_expr_partition_batch_table_scan_plan_cache_on`] |
//! | `:636 BenchmarkListExprPartitionBatchTableScanPlanCacheOff` | [`benchmark_list_expr_partition_batch_table_scan_plan_cache_off`] |
//! | `:640 BenchmarkListExprPartition` | [`benchmark_list_expr_partition_all_access_kinds`] |
//! | `:645 BenchmarkListColumnsPartitionPointGetPlanCacheOn` | [`benchmark_list_columns_partition_point_get_plan_cache_on`] |
//! | `:649 BenchmarkListColumnsPartitionPointGetPlanCacheOff` | [`benchmark_list_columns_partition_point_get_plan_cache_off`] |
//! | `:653 BenchmarkListColumnsPartitionBatchPointGetPlanCacheOn` | [`benchmark_list_columns_partition_batch_point_get_plan_cache_on`] |
//! | `:657 BenchmarkListColumnsPartitionBatchPointGetPlanCacheOff` | [`benchmark_list_columns_partition_batch_point_get_plan_cache_off`] |
//! | `:661 BenchmarkListColumnsPartitionIndexLookupPlanCacheOn` | [`benchmark_list_columns_partition_index_lookup_plan_cache_on`] |
//! | `:665 BenchmarkListColumnsPartitionIndexLookupPlanCacheOff` | [`benchmark_list_columns_partition_index_lookup_plan_cache_off`] |
//! | `:669 BenchmarkListColumnsPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_list_columns_partition_batch_index_lookup_plan_cache_on`] |
//! | `:673 BenchmarkListColumnsPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_list_columns_partition_batch_index_lookup_plan_cache_off`] |
//! | `:676 BenchmarkListColumnsPartitionTableScanPlanCacheOn` | [`benchmark_list_columns_partition_table_scan_plan_cache_on`] |
//! | `:680 BenchmarkListColumnsPartitionTableScanPlanCacheOff` | [`benchmark_list_columns_partition_table_scan_plan_cache_off`] |
//! | `:684 BenchmarkListColumnsPartitionBatchTableScanPlanCacheOn` | [`benchmark_list_columns_partition_batch_table_scan_plan_cache_on`] |
//! | `:688 BenchmarkListColumnsPartitionBatchTableScanPlanCacheOff` | [`benchmark_list_columns_partition_batch_table_scan_plan_cache_off`] |
//! | `:692 BenchmarkListColumnsPartition` | [`benchmark_list_columns_partition_all_access_kinds`] |
//! | `:697 BenchmarkRangePartitionPointGetPlanCacheOn` | [`benchmark_range_partition_point_get_plan_cache_on`] |
//! | `:701 BenchmarkRangePartitionPointGetPlanCacheOff` | [`benchmark_range_partition_point_get_plan_cache_off`] |
//! | `:705 BenchmarkRangePartitionBatchPointGetPlanCacheOn` | [`benchmark_range_partition_batch_point_get_plan_cache_on`] |
//! | `:709 BenchmarkRangePartitionBatchPointGetPlanCacheOff` | [`benchmark_range_partition_batch_point_get_plan_cache_off`] |
//! | `:713 BenchmarkRangePartitionIndexLookupPlanCacheOn` | [`benchmark_range_partition_index_lookup_plan_cache_on`] |
//! | `:717 BenchmarkRangePartitionIndexLookupPlanCacheOff` | [`benchmark_range_partition_index_lookup_plan_cache_off`] |
//! | `:721 BenchmarkRangePartitionBatchIndexLookupPlanCacheOn` | [`benchmark_range_partition_batch_index_lookup_plan_cache_on`] |
//! | `:725 BenchmarkRangePartitionBatchIndexLookupPlanCacheOff` | [`benchmark_range_partition_batch_index_lookup_plan_cache_off`] |
//! | `:728 BenchmarkRangePartitionTableScanPlanCacheOn` | [`benchmark_range_partition_table_scan_plan_cache_on`] |
//! | `:732 BenchmarkRangePartitionTableScanPlanCacheOff` | [`benchmark_range_partition_table_scan_plan_cache_off`] |
//! | `:736 BenchmarkRangePartitionBatchTableScanPlanCacheOn` | [`benchmark_range_partition_batch_table_scan_plan_cache_on`] |
//! | `:740 BenchmarkRangePartitionBatchTableScanPlanCacheOff` | [`benchmark_range_partition_batch_table_scan_plan_cache_off`] |
//! | `:744 BenchmarkRangePartition` | [`benchmark_range_partition_all_access_kinds`] |
//! | `:748 BenchmarkRangeExprPartitionPointGetPlanCacheOn` | [`benchmark_range_expr_partition_point_get_plan_cache_on`] |
//! | `:752 BenchmarkRangeExprPartitionPointGetPlanCacheOff` | [`benchmark_range_expr_partition_point_get_plan_cache_off`] |
//! | `:756 BenchmarkRangeExprPartitionBatchPointGetPlanCacheOn` | [`benchmark_range_expr_partition_batch_point_get_plan_cache_on`] |
//! | `:760 BenchmarkRangeExprPartitionBatchPointGetPlanCacheOff` | [`benchmark_range_expr_partition_batch_point_get_plan_cache_off`] |
//! | `:764 BenchmarkRangeExprPartitionIndexLookupPlanCacheOn` | [`benchmark_range_expr_partition_index_lookup_plan_cache_on`] |
//! | `:768 BenchmarkRangeExprPartitionIndexLookupPlanCacheOff` | [`benchmark_range_expr_partition_index_lookup_plan_cache_off`] |
//! | `:772 BenchmarkRangeExprPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_range_expr_partition_batch_index_lookup_plan_cache_on`] |
//! | `:776 BenchmarkRangeExprPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_range_expr_partition_batch_index_lookup_plan_cache_off`] |
//! | `:779 BenchmarkRangeExprPartitionTableScanPlanCacheOn` | [`benchmark_range_expr_partition_table_scan_plan_cache_on`] |
//! | `:783 BenchmarkRangeExprPartitionTableScanPlanCacheOff` | [`benchmark_range_expr_partition_table_scan_plan_cache_off`] |
//! | `:787 BenchmarkRangeExprPartitionBatchTableScanPlanCacheOn` | [`benchmark_range_expr_partition_batch_table_scan_plan_cache_on`] |
//! | `:791 BenchmarkRangeExprPartitionBatchTableScanPlanCacheOff` | [`benchmark_range_expr_partition_batch_table_scan_plan_cache_off`] |
//! | `:795 BenchmarkRangeExprPartition` | [`benchmark_range_expr_partition_all_access_kinds`] |
//! | `:799 BenchmarkRangeColumnsPartitionPointGetPlanCacheOn` | [`benchmark_range_columns_partition_point_get_plan_cache_on`] |
//! | `:803 BenchmarkRangeColumnsPartitionPointGetPlanCacheOff` | [`benchmark_range_columns_partition_point_get_plan_cache_off`] |
//! | `:807 BenchmarkRangeColumnsPartitionBatchPointGetPlanCacheOn` | [`benchmark_range_columns_partition_batch_point_get_plan_cache_on`] |
//! | `:811 BenchmarkRangeColumnsPartitionBatchPointGetPlanCacheOff` | [`benchmark_range_columns_partition_batch_point_get_plan_cache_off`] |
//! | `:815 BenchmarkRangeColumnsPartitionIndexLookupPlanCacheOn` | [`benchmark_range_columns_partition_index_lookup_plan_cache_on`] |
//! | `:819 BenchmarkRangeColumnsPartitionIndexLookupPlanCacheOff` | [`benchmark_range_columns_partition_index_lookup_plan_cache_off`] |
//! | `:823 BenchmarkRangeColumnsPartitionBatchIndexLookupPlanCacheOn` | [`benchmark_range_columns_partition_batch_index_lookup_plan_cache_on`] |
//! | `:827 BenchmarkRangeColumnsPartitionBatchIndexLookupPlanCacheOff` | [`benchmark_range_columns_partition_batch_index_lookup_plan_cache_off`] |
//! | `:830 BenchmarkRangeColumnsPartitionTableScanPlanCacheOn` | [`benchmark_range_columns_partition_table_scan_plan_cache_on`] |
//! | `:834 BenchmarkRangeColumnsPartitionTableScanPlanCacheOff` | [`benchmark_range_columns_partition_table_scan_plan_cache_off`] |
//! | `:838 BenchmarkRangeColumnsPartitionBatchTableScanPlanCacheOn` | [`benchmark_range_columns_partition_batch_table_scan_plan_cache_on`] |
//! | `:842 BenchmarkRangeColumnsPartitionBatchTableScanPlanCacheOff` | [`benchmark_range_columns_partition_batch_table_scan_plan_cache_off`] |
//! | `:846 BenchmarkRangeColumnsPartition` | [`benchmark_range_columns_partition_all_access_kinds`] |
//! | `:976 BenchmarkNonPartitionPreparedPointGetPlanCacheOn` | [`benchmark_non_partition_prepared_point_get_plan_cache_on`] |
//! | `:980 BenchmarkNonPartitionPreparedPointGetPlanCacheOff` | [`benchmark_non_partition_prepared_point_get_plan_cache_off`] |
//! | `:984 BenchmarkNonPartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_non_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:988 BenchmarkNonPartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_non_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:992 BenchmarkNonPartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_non_partition_prepared_index_lookup_plan_cache_on`] |

// --- ListExprPartition tail: getListPartitionDef("floor(id*0.5)*2", false) :516

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery,
    // expectedTableScanPlan, tableScan, true) — TableReader because
    // "Batch_Point_Get is not yet enabled for partitioned tables!" (:248-252).
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("floor(id*0.5)*2", false), batchPointQuery,
    // expectedTableScanPlan, tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_expr_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, getListPartitionDef("floor(id*0.5)*2", false))
    // — 12 sub-runs (:230-292); its batch arm applies the :250 runtime demotion
    // and `prepareIndexLookup`/drop-idx_id phases re-prepare between quartets.
}

// --- ListColumnsPartition family: getListPartitionDef("id", true) :516 --------

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_point_get_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", true), pointQuery, "Point_Get", pointGet, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_point_get_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", true), pointQuery, "Point_Get", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, "TableReader",
    // pointGet, true) — :248-252 demotion, expected statically here.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, "TableReader",
    // pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", true), pointQuery, "IndexLookUp",
    // indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", true), pointQuery, "IndexLookUp",
    // indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, "IndexLookUp",
    // indexLookup, true); expectHits (:159-160) applies — IndexLookUp rows count hits.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, "IndexLookUp",
    // indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", true), pointQuery, "TableReader",
    // tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", true), pointQuery, "TableReader",
    // tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, "TableReader",
    // tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, getListPartitionDef("id", true), batchPointQuery, "TableReader",
    // tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_list_columns_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, getListPartitionDef("id", true)) — 12 sub-runs
    // (:230-292) with the :250 batch demotion for this partitioned scheme.
}

// --- RangePartition family: partitionByRange :54 -------------------------------

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByRange, pointQuery, "Point_Get", pointGet, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByRange, pointQuery, "Point_Get", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByRange, batchPointQuery, "TableReader", pointGet, true)
    // — :248-252 demotion, expected statically here.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByRange, batchPointQuery, "TableReader", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByRange, pointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByRange, pointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByRange, batchPointQuery, "IndexLookUp", indexLookup, true);
    // expectHits (:159-160) applies — IndexLookUp rows count hits.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByRange, batchPointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByRange, pointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByRange, pointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByRange, batchPointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByRange, batchPointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, partitionByRange) — 12 sub-runs (:230-292)
    // with the :250 batch demotion for this partitioned scheme.
}

// --- RangeExprPartition family: partitionByRangeExpr :55 -----------------------

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByRangeExpr, pointQuery, "Point_Get", pointGet, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByRangeExpr, pointQuery, "Point_Get", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByRangeExpr, batchPointQuery, "TableReader", pointGet, true)
    // — :248-252 demotion, expected statically here.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByRangeExpr, batchPointQuery, "TableReader", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByRangeExpr, pointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByRangeExpr, pointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByRangeExpr, batchPointQuery, "IndexLookUp",
    // indexLookup, true); expectHits (:159-160) applies.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByRangeExpr, batchPointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByRangeExpr, pointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByRangeExpr, pointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByRangeExpr, batchPointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByRangeExpr, batchPointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_expr_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, partitionByRangeExpr) — 12 sub-runs (:230-292)
    // with the :250 batch demotion for this partitioned scheme.
}

// --- RangeColumnsPartition family: partitionByRangeColumns :56 -----------------

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByRangeColumns, pointQuery, "Point_Get", pointGet, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByRangeColumns, pointQuery, "Point_Get", pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_batch_point_get_plan_cache_on() {
    // runBenchmark(b, partitionByRangeColumns, batchPointQuery, "TableReader",
    // pointGet, true) — :248-252 demotion, expected statically here.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_batch_point_get_plan_cache_off() {
    // runBenchmark(b, partitionByRangeColumns, batchPointQuery, "TableReader",
    // pointGet, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByRangeColumns, pointQuery, "IndexLookUp", indexLookup, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByRangeColumns, pointQuery, "IndexLookUp", indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_batch_index_lookup_plan_cache_on() {
    // runBenchmark(b, partitionByRangeColumns, batchPointQuery, "IndexLookUp",
    // indexLookup, true); expectHits (:159-160) applies.
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_batch_index_lookup_plan_cache_off() {
    // runBenchmark(b, partitionByRangeColumns, batchPointQuery, "IndexLookUp",
    // indexLookup, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByRangeColumns, pointQuery, "TableReader", tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByRangeColumns, pointQuery, "TableReader", tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_batch_table_scan_plan_cache_on() {
    // runBenchmark(b, partitionByRangeColumns, batchPointQuery, "TableReader",
    // tableScan, true)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_batch_table_scan_plan_cache_off() {
    // runBenchmark(b, partitionByRangeColumns, batchPointQuery, "TableReader",
    // tableScan, false)
}

#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache execution benchmarks need session/executor"]
fn benchmark_range_columns_partition_all_access_kinds() {
    // benchmarkPointGetPlanCache(b, partitionByRangeColumns) — 12 sub-runs
    // (:230-292) with the :250 batch demotion for this partitioned scheme.
}

// --- NonPartitionPrepared tail: runBenchmarkPrepared :958 (part ends at :992) --

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_point_get_plan_cache_on() {
    // runBenchmarkPrepared(b, "", pointQueryPrepared, pointGet, true, pointArgs)
    // — runPreparedPointSelect :852 counts FoundInPlanCache twice per iteration
    // once i > 0 (:866-868 and :877-880); starvation check hits < b.N/2 is
    // log-only (:892-894).
}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_point_get_plan_cache_off() {
    // runBenchmarkPrepared(b, "", pointQueryPrepared, pointGet, false, pointArgs)
}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_batch_point_get_plan_cache_on() {
    // runBenchmarkPrepared(b, "", batchPointQueryPrepared, pointGet, true, batchArgs...)
    // — no EXPLAIN check on the prepared path (TODO :856-857), so no :250-style
    // plan expectation is asserted for the batch shape here.
}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_batch_point_get_plan_cache_off() {
    // runBenchmarkPrepared(b, "", batchPointQueryPrepared, pointGet, false, batchArgs...)
}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_index_lookup_plan_cache_on() {
    // runBenchmarkPrepared(b, "", pointQueryPrepared, indexLookup, true, pointArgs)
    // — prepareIndexLookup :218 (idx_id via insert1kRows :203) backs this shape.
}
