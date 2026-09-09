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

//! Documentary gap ports for the tail of
//! `pkg/planner/core/tests/partition/bench_test.go` — the `{LIST|RANGE}
//! COLUMNS` prepared-plan-cache benchmark families plus
//! `BenchmarkHashPartitionMultiPointSelect` and `TestBenchDaily`
//! (`pkg/planner.part19`, items 1081–1135 of all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line; package `session`). The range starts at
//! `:1233 BenchmarkListColumnsPartitionPreparedPointGetPlanCacheOn`, right
//! after part18 ended at `:1228 BenchmarkListExprPartitionPrepared`.
//!
//! Shared shape re-derived from the Go source (same harness as part18, cited
//! again from origin/master): every entry prepares a live mock-store session
//! (`prepareBenchSession` :72) and a per-scheme table (`preparePointGet` :196
//! / `prepareIndexLookup` :218 / `prepareTableScan` :224, the latter two via
//! the doubling inserts of `insert1kRows` :203 plus `analyze table t`), then
//! drives `runPreparedPointSelect` :852: flip session variable
//! `tidb_enable_prepared_plan_cache`, `PrepareStmt` either
//! `pointQueryPrepared` (:43 `select * from t where id = ?`, arg
//! `pointArgs = 1` :57) or `batchPointQueryPrepared` (:47
//! `select * from t where id IN (?,?,?)`, args `(2,10000,1)` :58), loop b.N
//! times over `ExecutePreparedStmt` with `expression.Args2Expressions4Test`
//! params counting `GetSessionVars().FoundInPlanCache` hits (double-counted
//! for i > 0 when enabled :880-888), draining every record set, and log-error
//! if hits < b.N/2 when the cache was enabled (:892-894). Single access-path
//! shapes go through `runBenchmarkPrepared` :958; the four plain
//! `*PartitionPrepared` entries run `benchPreparedPointGet` :898 instead,
//! which executes all twelve sub-shapes (Point/Batch/Index/IndexBatch/
//! TableScan/TableScanBatch, each Off-then-On) against one session,
//! re-preparing after dropping `idx_id` for the table-scan quartet (:945-951).
//! Partition schemes of this tail:
//!
//! * LIST COLUMNS over `id`: `getListPartitionDef("id", true)` (:516) builds
//!   `partition by list columns(id)` with four partitions of 256 consecutive
//!   values `{1..256}`, `{5000..5255}`, `{10000..10255}`, `{99900..100155}`;
//!   Go's entry point at :1280 is misspelled `BenchmarkListColumnPartitionPrepared`
//!   (no `s`).
//! * RANGE prepared: `partitionByRangePrep` :59 `range (id)` bounds 10/63/100/
//!   maxvalue — deliberately narrower than the non-prepared `partitionByRange`
//!   :54 so batch args `(2,10000,1)` land in different partitions than they do
//!   against the 10/1000/100000 bounds.
//! * RANGE EXPR prepared: `partitionByRangeExprPrep` :60 `range (floor(id*0.5)*2)`,
//!   same 10/63/100/maxvalue bounds.
//! * RANGE COLUMNS prepared: `partitionByRangeColumnsPrep` :61 `range columns (id)`,
//!   same bounds.
//!
//! `BenchmarkHashPartitionMultiPointSelect` (:1440) is NOT prepared and does
//! not flip any plan-cache variable: it creates
//! `t (id int primary key, dt datetime) partition by hash(id) partitions 64`
//! once and loops three non-prepared selects per iteration (`id = 2330`,
//! `id = 1233 or id = 1512`, `id in (117, 1233, 15678)` :1457/:1462/:1467),
//! relying on TryFastPlan. `TestBenchDaily` (:1482-1717) has no assertion at
//! all: it registers a selected subset of the NON-prepared families above with
//! `benchdaily.Run` for the nightly CI job (active entries: NonPartition and
//! HashPartition non-prepared shapes, plus the RangePartition On-twins;
//! everything else is commented out). Names keep Go's `Benchmark` shape so the
//! batch gate filter `not test(/bench/)` skips them exactly as `go test`
//! skips Benchmarks; every body is a recorded gap because prepared-plan-cache
//! execution needs the session/executor stack far outside this crate's ported
//! surface.
//!
//! | Go function (`bench_test.go`) | Rust test |
//! | --- | --- |
//! | `:1233 BenchmarkListColumnsPartitionPreparedPointGetPlanCacheOn` | [`benchmark_list_columns_partition_prepared_point_get_plan_cache_on`] |
//! | `:1237 BenchmarkListColumnsPartitionPreparedPointGetPlanCacheOff` | [`benchmark_list_columns_partition_prepared_point_get_plan_cache_off`] |
//! | `:1241 BenchmarkListColumnsPartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_list_columns_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:1245 BenchmarkListColumnsPartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_list_columns_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:1249 BenchmarkListColumnsPartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_list_columns_partition_prepared_index_lookup_plan_cache_on`] |
//! | `:1253 BenchmarkListColumnsPartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_list_columns_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1257 BenchmarkListColumnsPartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_list_columns_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1261 BenchmarkListColumnsPartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_list_columns_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1264 BenchmarkListColumnsPartitionPreparedTableScanPlanCacheOn` | [`benchmark_list_columns_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1268 BenchmarkListColumnsPartitionPreparedTableScanPlanCacheOff` | [`benchmark_list_columns_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1272 BenchmarkListColumnsPartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_list_columns_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1276 BenchmarkListColumnsPartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_list_columns_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1280 BenchmarkListColumnPartitionPrepared` | [`benchmark_list_column_partition_prepared_all_shapes`] |
//! | `:1285 BenchmarkRangePartitionPreparedPointGetPlanCacheOn` | [`benchmark_range_partition_prepared_point_get_plan_cache_on`] |
//! | `:1289 BenchmarkRangePartitionPreparedPointGetPlanCacheOff` | [`benchmark_range_partition_prepared_point_get_plan_cache_off`] |
//! | `:1293 BenchmarkRangePartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_range_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:1297 BenchmarkRangePartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_range_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:1301 BenchmarkRangePartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_range_partition_prepared_index_lookup_plan_cache_on`] |
//! | `:1305 BenchmarkRangePartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_range_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1309 BenchmarkRangePartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_range_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1313 BenchmarkRangePartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_range_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1316 BenchmarkRangePartitionPreparedTableScanPlanCacheOn` | [`benchmark_range_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1320 BenchmarkRangePartitionPreparedTableScanPlanCacheOff` | [`benchmark_range_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1324 BenchmarkRangePartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_range_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1328 BenchmarkRangePartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_range_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1332 BenchmarkRangePartitionPrepared` | [`benchmark_range_partition_prepared_all_shapes`] |
//! | `:1336 BenchmarkRangeExprPartitionPreparedPointGetPlanCacheOn` | [`benchmark_range_expr_partition_prepared_point_get_plan_cache_on`] |
//! | `:1340 BenchmarkRangeExprPartitionPreparedPointGetPlanCacheOff` | [`benchmark_range_expr_partition_prepared_point_get_plan_cache_off`] |
//! | `:1344 BenchmarkRangeExprPartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_range_expr_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:1348 BenchmarkRangeExprPartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_range_expr_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:1352 BenchmarkRangeExprPartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_range_expr_partition_prepared_index_lookup_plan_cache_on`] |
//! | `:1356 BenchmarkRangeExprPartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_range_expr_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1360 BenchmarkRangeExprPartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_range_expr_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1364 BenchmarkRangeExprPartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_range_expr_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1367 BenchmarkRangeExprPartitionPreparedTableScanPlanCacheOn` | [`benchmark_range_expr_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1371 BenchmarkRangeExprPartitionPreparedTableScanPlanCacheOff` | [`benchmark_range_expr_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1375 BenchmarkRangeExprPartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_range_expr_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1379 BenchmarkRangeExprPartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_range_expr_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1383 BenchmarkRangeExprPartitionPrepared` | [`benchmark_range_expr_partition_prepared_all_shapes`] |
//! | `:1387 BenchmarkRangeColumnsPartitionPreparedPointGetPlanCacheOn` | [`benchmark_range_columns_partition_prepared_point_get_plan_cache_on`] |
//! | `:1391 BenchmarkRangeColumnsPartitionPreparedPointGetPlanCacheOff` | [`benchmark_range_columns_partition_prepared_point_get_plan_cache_off`] |
//! | `:1395 BenchmarkRangeColumnsPartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_range_columns_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:1399 BenchmarkRangeColumnsPartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_range_columns_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:1403 BenchmarkRangeColumnsPartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_range_columns_partition_prepared_index_lookup_plan_cache_on`] |
//! | `:1407 BenchmarkRangeColumnsPartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_range_columns_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1411 BenchmarkRangeColumnsPartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_range_columns_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1415 BenchmarkRangeColumnsPartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_range_columns_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1418 BenchmarkRangeColumnsPartitionPreparedTableScanPlanCacheOn` | [`benchmark_range_columns_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1422 BenchmarkRangeColumnsPartitionPreparedTableScanPlanCacheOff` | [`benchmark_range_columns_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1426 BenchmarkRangeColumnsPartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_range_columns_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1430 BenchmarkRangeColumnsPartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_range_columns_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1434 BenchmarkRangeColumnPartitionPrepared` | [`benchmark_range_column_partition_prepared_all_shapes`] |
//! | `:1440 BenchmarkHashPartitionMultiPointSelect` | [`benchmark_hash_partition_multi_point_select_tryfast_trio_over_hash_64`] |
//! | `:1482 TestBenchDaily` | [`bench_daily_registry_selects_nonprepared_families`] |

// ***** LIST COLUMNS prepared (`getListPartitionDef("id", true)`) *****

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_columns_partition_prepared_batch_table_scan_plan_cache_off() {}

/// Go `:1280 BenchmarkListColumnPartitionPrepared` keeps its singular spelling.
#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_list_column_partition_prepared_all_shapes() {}

// ***** RANGE prepared (`partitionByRangePrep` :59) *****

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_batch_table_scan_plan_cache_off() {}

/// Go `:1332 BenchmarkRangePartitionPrepared`.
#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_partition_prepared_all_shapes() {}

// ***** RANGE EXPR prepared (`partitionByRangeExprPrep` :60) *****

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_batch_table_scan_plan_cache_off() {}

/// Go `:1383 BenchmarkRangeExprPartitionPrepared`.
#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_expr_partition_prepared_all_shapes() {}

// ***** RANGE COLUMNS prepared (`partitionByRangeColumnsPrep` :61) *****

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_columns_partition_prepared_batch_table_scan_plan_cache_off() {}

/// Go `:1434 BenchmarkRangeColumnPartitionPrepared`.
#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_range_column_partition_prepared_all_shapes() {}

// ***** Multi-point select and the nightly registry *****

/// Go `:1440 BenchmarkHashPartitionMultiPointSelect`: hash(id) partitions 64
/// primary-key table, three TryFastPlan-driven selects per iteration — single
/// equality, two-way OR, three-way IN — each drained before the next.
#[test]
#[ignore = "go-parity-gap: looped multi-statement execution over a mock-store session table needs the executor stack"]
fn benchmark_hash_partition_multi_point_select_tryfast_trio_over_hash_64() {}

/// Go `:1482 TestBenchDaily` registers the nightly subset of the NON-prepared
/// families with `benchdaily.Run` (NonPartition + HashPartition shapes, Range
/// On-twins); no assertion exists to pin beyond the registry itself, which
/// only matters when `go test -run TestBenchDaily` executes on hardware.
#[test]
#[ignore = "go-parity-gap: benchdaily.Run registry has no Rust counterpart; the registered benchmarks are themselves unexecuted gaps"]
fn bench_daily_registry_selects_nonprepared_families() {}
