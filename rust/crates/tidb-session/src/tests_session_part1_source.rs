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

//! Source-backed carriers for `pkg/session.part1`.
//!
//! This is the first 60 declarations in the deterministic `Test*` /
//! `Benchmark*` inventory for `pkg/session`: the session benchmarks, the
//! beginning of `bootstrap_test.go`, and its upgrade tests through version
//! 145. The benchmark declarations and the bootstrap/domain upgrade tests are
//! retained as explicit ignored carriers because this crate does not expose
//! Go's benchmark driver, TiKV-backed storage bootstrap, Domain lifecycle,
//! or versioned upgrade machinery.

#![cfg(test)]

/// The Go benchmark driver owns the timing loop and benchmark fixtures; this
/// Rust crate has no equivalent `testing.B` surface for these declarations.
macro_rules! ignored_benchmark {
    ($name:ident, $source:literal) => {
        #[doc = $source]
        #[test]
        #[ignore = "skipped-reason: Go testing.B benchmark; the assigned test gate excludes benchmarks"]
        fn $name() {}
    };
}

ignored_benchmark!(
    benchmark_basic,
    "Go `pkg/session/bench_test.go:151::BenchmarkBasic`."
);
ignored_benchmark!(
    benchmark_table_scan,
    "Go `pkg/session/bench_test.go:170::BenchmarkTableScan`."
);
ignored_benchmark!(
    benchmark_explain_table_scan,
    "Go `pkg/session/bench_test.go:190::BenchmarkExplainTableScan`."
);
ignored_benchmark!(
    benchmark_table_lookup,
    "Go `pkg/session/bench_test.go:210::BenchmarkTableLookup`."
);
ignored_benchmark!(
    benchmark_explain_table_lookup,
    "Go `pkg/session/bench_test.go:230::BenchmarkExplainTableLookup`."
);
ignored_benchmark!(
    benchmark_string_index_scan,
    "Go `pkg/session/bench_test.go:250::BenchmarkStringIndexScan`."
);
ignored_benchmark!(
    benchmark_explain_string_index_scan,
    "Go `pkg/session/bench_test.go:270::BenchmarkExplainStringIndexScan`."
);
ignored_benchmark!(
    benchmark_point_get,
    "Go `pkg/session/bench_test.go:290::BenchmarkPointGet`."
);
ignored_benchmark!(
    benchmark_batch_point_get,
    "Go `pkg/session/bench_test.go:317::BenchmarkBatchPointGet`."
);
ignored_benchmark!(
    benchmark_prepared_point_get,
    "Go `pkg/session/bench_test.go:343::BenchmarkPreparedPointGet`."
);
ignored_benchmark!(
    benchmark_string_index_lookup,
    "Go `pkg/session/bench_test.go:376::BenchmarkStringIndexLookup`."
);
ignored_benchmark!(
    benchmark_integer_index_scan,
    "Go `pkg/session/bench_test.go:398::BenchmarkIntegerIndexScan`."
);
ignored_benchmark!(
    benchmark_integer_index_lookup,
    "Go `pkg/session/bench_test.go:418::BenchmarkIntegerIndexLookup`."
);
ignored_benchmark!(
    benchmark_decimal_index_scan,
    "Go `pkg/session/bench_test.go:440::BenchmarkDecimalIndexScan`."
);
ignored_benchmark!(
    benchmark_decimal_index_lookup,
    "Go `pkg/session/bench_test.go:460::BenchmarkDecimalIndexLookup`."
);
ignored_benchmark!(
    benchmark_insert_with_index,
    "Go `pkg/session/bench_test.go:482::BenchmarkInsertWithIndex`."
);
ignored_benchmark!(
    benchmark_insert_no_index,
    "Go `pkg/session/bench_test.go:499::BenchmarkInsertNoIndex`."
);
ignored_benchmark!(
    benchmark_sort,
    "Go `pkg/session/bench_test.go:515::BenchmarkSort`."
);
ignored_benchmark!(
    benchmark_sort2,
    "Go `pkg/session/bench_test.go:535::BenchmarkSort2`."
);
ignored_benchmark!(
    benchmark_join,
    "Go `pkg/session/bench_test.go:555::BenchmarkJoin`."
);
ignored_benchmark!(
    benchmark_join_limit,
    "Go `pkg/session/bench_test.go:575::BenchmarkJoinLimit`."
);
ignored_benchmark!(
    benchmark_partition_pruning,
    "Go `pkg/session/bench_test.go:595::BenchmarkPartitionPruning`."
);
ignored_benchmark!(
    benchmark_range_column_partition_pruning,
    "Go `pkg/session/bench_test.go:1652::BenchmarkRangeColumnPartitionPruning`."
);
ignored_benchmark!(
    benchmark_hash_partition_pruning_point_select,
    "Go `pkg/session/bench_test.go:1690::BenchmarkHashPartitionPruningPointSelect`."
);
ignored_benchmark!(
    benchmark_hash_partition_pruning_multi_select,
    "Go `pkg/session/bench_test.go:1716::BenchmarkHashPartitionPruningMultiSelect`."
);
ignored_benchmark!(
    benchmark_insert_into_select,
    "Go `pkg/session/bench_test.go:1758::BenchmarkInsertIntoSelect`."
);
ignored_benchmark!(
    benchmark_compile_stmt,
    "Go `pkg/session/bench_test.go:1784::BenchmarkCompileStmt`."
);
ignored_benchmark!(
    benchmark_auto_increment,
    "Go `pkg/session/bench_test.go:1898::BenchmarkAutoIncrement`."
);

/// Go `pkg/session/bench_test.go:1919::TestBenchDaily`.
#[test]
#[ignore = "skipped-reason: Go benchdaily registration harness; benchmark execution is outside the unit-test gate"]
fn test_bench_daily() {}

ignored_benchmark!(
    benchmark_pipelined_simple_insert,
    "Go `pkg/session/bench_test.go:1953::BenchmarkPipelinedSimpleInsert`."
);
ignored_benchmark!(
    benchmark_pipelined_insert_ignore_no_duplicates,
    "Go `pkg/session/bench_test.go:1983::BenchmarkPipelinedInsertIgnoreNoDuplicates`."
);
ignored_benchmark!(
    benchmark_pipelined_insert_on_duplicate,
    "Go `pkg/session/bench_test.go:2014::BenchmarkPipelinedInsertOnDuplicate`."
);
ignored_benchmark!(
    benchmark_pipelined_delete,
    "Go `pkg/session/bench_test.go:2048::BenchmarkPipelinedDelete`."
);
ignored_benchmark!(
    benchmark_pipelined_replace_no_duplicates,
    "Go `pkg/session/bench_test.go:2080::BenchmarkPipelinedReplaceNoDuplicates`."
);
ignored_benchmark!(
    benchmark_pipelined_update,
    "Go `pkg/session/bench_test.go:2111::BenchmarkPipelinedUpdate`."
);

/// Go `pkg/session/bootstrap_test.go:55::TestMySQLDBTables`.
#[test]
#[ignore = "go-parity-gap: complete versioned bootstrap catalog and reserved-ID metadata are not transcreated"]
fn test_mysql_db_tables() {}

/// Go `pkg/session/bootstrap_test.go:89::TestBootstrap`.
#[test]
#[ignore = "go-parity-gap: Go Domain, privilege-table, global-variable, and storage bootstrap lifecycle is not transcreated"]
fn test_bootstrap() {}

/// Go `pkg/session/bootstrap_test.go:188::TestBootstrapWithError`.
#[test]
#[ignore = "go-parity-gap: interrupted DDL bootstrap recovery needs Go Domain and storage lifecycle"]
fn test_bootstrap_with_error() {}

/// Go `pkg/session/bootstrap_test.go:287::TestDDLTableCreateBackfillTable`.
#[test]
#[ignore = "go-parity-gap: DDL table-version metadata and bootstrap backfill DDL are not transcreated"]
fn test_ddl_table_create_backfill_table() {}

/// Go `pkg/session/bootstrap_test.go:329::TestUpgrade`.
#[test]
#[ignore = "go-parity-gap: versioned bootstrap rollback, DDL upgrade, and persistent global variables are not transcreated"]
fn test_upgrade() {}

/// Go `pkg/session/bootstrap_test.go:428::TestOldPasswordUpgrade`.
/// The Go helper decodes the old stage-one SHA-1 hex and hashes those bytes
/// once more. This pure cryptographic contract is already exposed by the
/// parser auth module, so it can be checked without pretending to port the
/// surrounding bootstrap machinery.
#[test]
fn test_old_password_upgrade() {
    let stage_one = tidb_parser::auth::sha1_hash(b"abc");
    let stage_two = tidb_parser::auth::sha1_hash(&stage_one);
    let mut upgraded = String::from("*");
    for byte in stage_two {
        use std::fmt::Write as _;
        write!(&mut upgraded, "{byte:02X}").unwrap();
    }
    assert_eq!(upgraded, "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E");
}

/// Go `pkg/session/bootstrap_test.go:436::TestBootstrapInitExpensiveQueryHandle`.
#[test]
#[ignore = "go-parity-gap: ExpensiveQueryHandle is owned by the unported Go Domain bootstrap"]
fn test_bootstrap_init_expensive_query_handle() {}

/// Go `pkg/session/bootstrap_test.go:449::TestForIssue23387`.
#[test]
#[ignore = "go-parity-gap: cross-version bootstrap and privilege preservation require Go Domain/storage upgrades"]
fn test_for_issue23387() {}

/// Go `pkg/session/bootstrap_test.go:483::TestIndexMergeInNewCluster`.
#[test]
#[ignore = "go-parity-gap: fresh-cluster persisted global-variable bootstrap is not transcreated"]
fn test_index_merge_in_new_cluster() {}

/// Go `pkg/session/bootstrap_test.go:509::TestTiDBOptAdvancedJoinHintInNewCluster`.
#[test]
#[ignore = "go-parity-gap: fresh-cluster persisted global-variable bootstrap is not transcreated"]
fn test_tidb_opt_advanced_join_hint_in_new_cluster() {}

/// Go `pkg/session/bootstrap_test.go:535::TestTiDBCostModelInNewCluster`.
#[test]
#[ignore = "go-parity-gap: fresh-cluster persisted global-variable bootstrap is not transcreated"]
fn test_tidb_cost_model_in_new_cluster() {}

/// Go `pkg/session/bootstrap_test.go:561::TestTiDBGCAwareUpgradeFrom630To650`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable upgrade through BootstrapSession is not transcreated"]
fn test_tidb_gc_aware_upgrade_from630_to650() {}

/// Go `pkg/session/bootstrap_test.go:619::TestTiDBServerMemoryLimitUpgradeTo651_1`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable upgrade through BootstrapSession is not transcreated"]
fn test_tidb_server_memory_limit_upgrade_to651_1() {}

/// Go `pkg/session/bootstrap_test.go:677::TestTiDBServerMemoryLimitUpgradeTo651_2`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable upgrade through BootstrapSession is not transcreated"]
fn test_tidb_server_memory_limit_upgrade_to651_2() {}

/// Go `pkg/session/bootstrap_test.go:735::TestTiDBGlobalVariablesDefaultValueUpgradeFrom630To660`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable default preservation requires BootstrapSession and storage"]
fn test_tidb_global_variables_default_value_upgrade_from630_to660() {}

/// Go `pkg/session/bootstrap_test.go:803::TestTiDBStoreBatchSizeUpgradeFrom650To660`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable upgrade and persisted override are not transcreated"]
fn test_tidb_store_batch_size_upgrade_from650_to660() {}

/// Go `pkg/session/bootstrap_test.go:877::TestTiDBUpgradeToVer136`.
#[test]
#[ignore = "go-parity-gap: versioned DDL upgrade and reorg failpoint are not transcreated"]
fn test_tidb_upgrade_to_ver136() {}

/// Go `pkg/session/bootstrap_test.go:920::TestTiDBUpgradeToVer140`.
#[test]
#[ignore = "go-parity-gap: versioned DDL schema upgrade and Domain restart are not transcreated"]
fn test_tidb_upgrade_to_ver140() {}

/// Go `pkg/session/bootstrap_test.go:976::TestTiDBNonPrepPlanCacheUpgradeFrom540To700`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable and plan-cache upgrade are not transcreated"]
fn test_tidb_non_prep_plan_cache_upgrade_from540_to700() {}

/// Go `pkg/session/bootstrap_test.go:1040::TestTiDBStatsLoadPseudoTimeoutUpgradeFrom610To650`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable upgrade through BootstrapSession is not transcreated"]
fn test_tidb_stats_load_pseudo_timeout_upgrade_from610_to650() {}

/// Go `pkg/session/bootstrap_test.go:1098::TestTiDBTiDBOptTiDBOptimizerEnableNAAJWhenUpgradingToVer138`.
#[test]
#[ignore = "go-parity-gap: versioned optimizer-variable upgrade through BootstrapSession is not transcreated"]
fn test_tidb_optimizer_enable_naaj_when_upgrading_to_ver138() {}

/// Go `pkg/session/bootstrap_test.go:1153::TestTiDBUpgradeToVer143`.
#[test]
#[ignore = "go-parity-gap: versioned bootstrap upgrade and Domain restart are not transcreated"]
fn test_tidb_upgrade_to_ver143() {}

/// Go `pkg/session/bootstrap_test.go:1188::TestTiDBLoadBasedReplicaReadThresholdUpgradingToVer141`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable upgrade through BootstrapSession is not transcreated"]
fn test_tidb_load_based_replica_read_threshold_upgrading_to_ver141() {}

/// Go `pkg/session/bootstrap_test.go:1246::TestTiDBPlanCacheInvalidationOnFreshStatsWhenUpgradingToVer144`.
#[test]
#[ignore = "go-parity-gap: versioned global-variable initialization and plan-cache state are not transcreated"]
fn test_tidb_plan_cache_invalidation_on_fresh_stats_when_upgrading_to_ver144() {}

/// Go `pkg/session/bootstrap_test.go:1298::TestTiDBUpgradeToVer145`.
#[test]
#[ignore = "go-parity-gap: versioned bootstrap upgrade and Domain restart are not transcreated"]
fn test_tidb_upgrade_to_ver145() {}
