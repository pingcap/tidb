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
//! prepared-statement benchmark families (`pkg/planner.part18`, items 1021–1080
//! of all 1278 `Test*`/`Benchmark*` declarations under `pkg/planner/` on
//! `origin/master`, sorted by file path then line; package `session`). The
//! range starts mid-family at `:996 BenchmarkNonPartitionPreparedIndexLookupPlanCacheOff`
//! because its On-twin (`:992`) and earlier items belong to the neighbouring
//! part; it ends at `:1228 BenchmarkListExprPartitionPrepared`.
//!
//! Shared shape re-derived from the Go source: every entry prepares a live
//! mock-store session (`prepareBenchSession` :72) and a per-scheme table
//! (`preparePointGet` :196 / `prepareIndexLookup` :218 / `prepareTableScan`
//! :224, the latter two via the doubling inserts of `insert1kRows` :203 plus
//! `analyze table t`), then drives `runPreparedPointSelect` :852: flip session
//! variable `tidb_enable_prepared_plan_cache`, `PrepareStmt` either
//! `pointQueryPrepared` (:43 `select * from t where id = ?`, arg `1`) or
//! `batchPointQueryPrepared` (:47 `select * from t where id IN (?,?,?)`, args
//! `(2,10000,1)` :58), loop b.N times over `ExecutePreparedStmt` with
//! `expression.Args2Expressions4Test` params counting
//! `GetSessionVars().FoundInPlanCache` hits, draining every record set, and
//! log-error if hits < b.N/2 when the cache was enabled (:892-894). Single
//! access-path shapes go through `runBenchmarkPrepared` :958; the five plain
//! `*Prepared` entries run `benchPreparedPointGet` :898 instead, which executes
//! all twelve sub-shapes (Point/Batch/Index/IndexBatch/TableScan/TableScanBatch,
//! each Off-then-On) against one session, re-preparing after dropping `idx_id`
//! for the table-scan quartet (:949-951). Partition schemes for this batch:
//! non-partitioned (`""`), `partitionByHash` (:41 `hash(id) partitions 7`),
//! `partitionByHashExpr` (:42 `hash(floor(id*0.5)) partitions 7`), and the
//! LIST defs built by `getListPartitionDef` :516: four partitions of 256
//! consecutive values `{1..256}`, `{5000..5255}`, `{10000..10255}`,
//! `{99900..100155}` over `id`, or over `floor(id*0.5)*2` with value `0`
//! appended to p0 for the floor expression (:528-530). Names keep Go's
//! `Benchmark` shape so the batch gate filter `not test(/bench/)` skips them
//! exactly as `go test` skips Benchmarks; every body is a recorded gap because
//! prepared-plan-cache execution needs the session/executor stack far outside
//! this crate's ported surface.
//!
//! | Go function (`bench_test.go`) | Rust test |
//! | --- | --- |
//! | `:996 BenchmarkNonPartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_non_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1000 BenchmarkNonPartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_non_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1004 BenchmarkNonPartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_non_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1007 BenchmarkNonPartitionPreparedTableScanPlanCacheOn` | [`benchmark_non_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1011 BenchmarkNonPartitionPreparedTableScanPlanCacheOff` | [`benchmark_non_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1015 BenchmarkNonPartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_non_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1019 BenchmarkNonPartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_non_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1023 BenchmarkNonPartitionPrepared` | [`benchmark_non_partition_prepared_all_shapes`] |
//! | `:1027 BenchmarkHashPartitionPreparedPointGetPlanCacheOn` | [`benchmark_hash_partition_prepared_point_get_plan_cache_on`] |
//! | `:1031 BenchmarkHashPartitionPreparedPointGetPlanCacheOff` | [`benchmark_hash_partition_prepared_point_get_plan_cache_off`] |
//! | `:1035 BenchmarkHashPartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_hash_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:1039 BenchmarkHashPartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_hash_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:1043 BenchmarkHashPartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_hash_partition_prepared_index_lookup_plan_cache_on`] |
//! | `:1047 BenchmarkHashPartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_hash_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1051 BenchmarkHashPartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_hash_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1055 BenchmarkHashPartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_hash_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1058 BenchmarkHashPartitionPreparedTableScanPlanCacheOn` | [`benchmark_hash_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1062 BenchmarkHashPartitionPreparedTableScanPlanCacheOff` | [`benchmark_hash_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1066 BenchmarkHashPartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_hash_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1070 BenchmarkHashPartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_hash_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1074 BenchmarkHashPartitionPrepared` | [`benchmark_hash_partition_prepared_all_shapes`] |
//! | `:1078 BenchmarkHashExprPartitionPreparedPointGetPlanCacheOn` | [`benchmark_hash_expr_partition_prepared_point_get_plan_cache_on`] |
//! | `:1082 BenchmarkHashExprPartitionPreparedPointGetPlanCacheOff` | [`benchmark_hash_expr_partition_prepared_point_get_plan_cache_off`] |
//! | `:1086 BenchmarkHashExprPartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_hash_expr_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:1090 BenchmarkHashExprPartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_hash_expr_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:1094 BenchmarkHashExprPartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_hash_expr_partition_prepared_index_lookup_plan_cache_on`] |
//! | `:1098 BenchmarkHashExprPartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_hash_expr_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1102 BenchmarkHashExprPartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_hash_expr_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1106 BenchmarkHashExprPartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_hash_expr_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1109 BenchmarkHashExprPartitionPreparedTableScanPlanCacheOn` | [`benchmark_hash_expr_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1113 BenchmarkHashExprPartitionPreparedTableScanPlanCacheOff` | [`benchmark_hash_expr_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1117 BenchmarkHashExprPartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_hash_expr_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1121 BenchmarkHashExprPartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_hash_expr_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1125 BenchmarkHashExprPartitionPrepared` | [`benchmark_hash_expr_partition_prepared_all_shapes`] |
//! | `:1129 BenchmarkListPartitionPreparedPointGetPlanCacheOn` | [`benchmark_list_partition_prepared_point_get_plan_cache_on`] |
//! | `:1133 BenchmarkListPartitionPreparedPointGetPlanCacheOff` | [`benchmark_list_partition_prepared_point_get_plan_cache_off`] |
//! | `:1137 BenchmarkListPartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_list_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:1141 BenchmarkListPartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_list_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:1145 BenchmarkListPartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_list_partition_prepared_index_lookup_plan_cache_on`] |
//! | `:1149 BenchmarkListPartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_list_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1153 BenchmarkListPartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_list_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1157 BenchmarkListPartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_list_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1160 BenchmarkListPartitionPreparedTableScanPlanCacheOn` | [`benchmark_list_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1164 BenchmarkListPartitionPreparedTableScanPlanCacheOff` | [`benchmark_list_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1168 BenchmarkListPartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_list_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1172 BenchmarkListPartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_list_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1176 BenchmarkListPartitionPrepared` | [`benchmark_list_partition_prepared_all_shapes`] |
//! | `:1181 BenchmarkListExprPartitionPreparedPointGetPlanCacheOn` | [`benchmark_list_expr_partition_prepared_point_get_plan_cache_on`] |
//! | `:1185 BenchmarkListExprPartitionPreparedPointGetPlanCacheOff` | [`benchmark_list_expr_partition_prepared_point_get_plan_cache_off`] |
//! | `:1189 BenchmarkListExprPartitionPreparedBatchPointGetPlanCacheOn` | [`benchmark_list_expr_partition_prepared_batch_point_get_plan_cache_on`] |
//! | `:1193 BenchmarkListExprPartitionPreparedBatchPointGetPlanCacheOff` | [`benchmark_list_expr_partition_prepared_batch_point_get_plan_cache_off`] |
//! | `:1197 BenchmarkListExprPartitionPreparedIndexLookupPlanCacheOn` | [`benchmark_list_expr_partition_prepared_index_lookup_plan_cache_on`] |
//! | `:1201 BenchmarkListExprPartitionPreparedIndexLookupPlanCacheOff` | [`benchmark_list_expr_partition_prepared_index_lookup_plan_cache_off`] |
//! | `:1205 BenchmarkListExprPartitionPreparedBatchIndexLookupPlanCacheOn` | [`benchmark_list_expr_partition_prepared_batch_index_lookup_plan_cache_on`] |
//! | `:1209 BenchmarkListExprPartitionPreparedBatchIndexLookupPlanCacheOff` | [`benchmark_list_expr_partition_prepared_batch_index_lookup_plan_cache_off`] |
//! | `:1212 BenchmarkListExprPartitionPreparedTableScanPlanCacheOn` | [`benchmark_list_expr_partition_prepared_table_scan_plan_cache_on`] |
//! | `:1216 BenchmarkListExprPartitionPreparedTableScanPlanCacheOff` | [`benchmark_list_expr_partition_prepared_table_scan_plan_cache_off`] |
//! | `:1220 BenchmarkListExprPartitionPreparedBatchTableScanPlanCacheOn` | [`benchmark_list_expr_partition_prepared_batch_table_scan_plan_cache_on`] |
//! | `:1224 BenchmarkListExprPartitionPreparedBatchTableScanPlanCacheOff` | [`benchmark_list_expr_partition_prepared_batch_table_scan_plan_cache_off`] |
//! | `:1228 BenchmarkListExprPartitionPrepared` | [`benchmark_list_expr_partition_prepared_all_shapes`] |

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache execution needs PrepareStmt/ExecutePreparedStmt over a live session"]
fn benchmark_non_partition_prepared_batch_table_scan_plan_cache_off() {}

/// GO PORT of `bench_test.go:1023 BenchmarkNonPartitionPrepared`: runs all
/// twelve `benchPreparedPointGet` (:898) sub-shapes against an unpartitioned
/// table.
#[test]
#[ignore = "go-parity-gap: runs all twelve prepared plan-cache sub-shapes; needs the session/executor stack"]
fn benchmark_non_partition_prepared_all_shapes() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(id) partitions 7 needs the session/executor stack"]
fn benchmark_hash_partition_prepared_batch_table_scan_plan_cache_off() {}

/// GO PORT of `bench_test.go:1074 BenchmarkHashPartitionPrepared`: all twelve
/// `benchPreparedPointGet` (:898) sub-shapes over `partitionByHash` (:41).
#[test]
#[ignore = "go-parity-gap: runs all twelve prepared sub-shapes over hash(id) partitions 7; needs the session/executor stack"]
fn benchmark_hash_partition_prepared_all_shapes() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over hash(floor(id*0.5)) partitions 7 needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_batch_table_scan_plan_cache_off() {}

/// GO PORT of `bench_test.go:1125 BenchmarkHashExprPartitionPrepared`: all
/// twelve `benchPreparedPointGet` (:898) sub-shapes over `partitionByHashExpr`
/// (:42).
#[test]
#[ignore = "go-parity-gap: runs all twelve prepared sub-shapes over hash(floor(id*0.5)) partitions 7; needs the session/executor stack"]
fn benchmark_hash_expr_partition_prepared_all_shapes() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(id) 256-value buckets needs the session/executor stack"]
fn benchmark_list_partition_prepared_batch_table_scan_plan_cache_off() {}

/// GO PORT of `bench_test.go:1176 BenchmarkListPartitionPrepared`: all twelve
/// `benchPreparedPointGet` (:898) sub-shapes over `getListPartitionDef("id",
/// false)` (:516).
#[test]
#[ignore = "go-parity-gap: runs all twelve prepared sub-shapes over list(id) 256-value buckets; needs the session/executor stack"]
fn benchmark_list_partition_prepared_all_shapes() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_batch_point_get_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_batch_point_get_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_batch_index_lookup_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_batch_index_lookup_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_table_scan_plan_cache_off() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_batch_table_scan_plan_cache_on() {}

#[test]
#[ignore = "go-parity-gap: prepared plan-cache loop over list(floor(id*0.5)*2) incl. value 0 needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_batch_table_scan_plan_cache_off() {}

/// GO PORT of `bench_test.go:1228 BenchmarkListExprPartitionPrepared`: all
/// twelve `benchPreparedPointGet` (:898) sub-shapes over
/// `getListPartitionDef("floor(id*0.5)*2", false)` (:516, extra `0` in p0 at
/// :528-530).
#[test]
#[ignore = "go-parity-gap: runs all twelve prepared sub-shapes over list(floor(id*0.5)*2); needs the session/executor stack"]
fn benchmark_list_expr_partition_prepared_all_shapes() {}
