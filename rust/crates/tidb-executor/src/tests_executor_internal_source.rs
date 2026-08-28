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

//! Source-backed ports for the Go `pkg/executor/internal` unit tests.
//!
//! The apply-cache and vector-group-checker packages have Rust behavior
//! carriers in this crate. The remaining internal packages require Go's live
//! session/domain, PD, resource-control, index-usage, runaway-query, or MPP
//! infrastructure and are retained below as explicit parity-gap tests.

use std::sync::Arc;
use std::thread;

use tidb_datatype::{BinaryJSON, Collation, Datum, Decimal, FieldType, FieldTypeCode};
use tidb_expr::{NoColumns, column::Column, expression::Expression};

use crate::apply_cache::{ApplyCache, apply_cache_kv_mem};
use crate::vec_group_checker::VecGroupChecker;
use tidb_chunk::chunk::Chunk;

fn grouping_column(index: usize, field_type: FieldType) -> Expression {
    let mut column = Column::new(index as i64 + 1, field_type);
    column.index = index as i64;
    Expression::Column(column)
}

fn ranges(checker: &mut VecGroupChecker) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    while !checker.is_exhausted() {
        ranges.push(checker.get_next_group());
    }
    ranges
}

/// Go `pkg/executor/internal/applycache/apply_cache_test.go:30::TestApplyCache`:
/// `applyCacheKVMem` (`pkg/executor/internal/applycache/apply_cache.go:41`) charges
/// key bytes plus value memory, and `ApplyCache.Set` (`:87-101`) evicts the
/// oldest entries until a new item fits the configured quota.
#[test]
fn apply_cache_admits_values_and_evicts_oldest_entries() {
    let cache = ApplyCache::<Vec<i64>>::new(100);
    let keys = [vec![b'0'; 100], vec![b'1'; 100], vec![b'2'; 100]];
    let values = [vec![0], vec![1], vec![2]];

    for key in &keys {
        assert_eq!(apply_cache_kv_mem(key, 0), 100);
    }

    assert!(cache.set(keys[0].clone(), values[0].clone(), 0));
    assert_eq!(cache.get(&keys[0]).as_deref(), Some(&values[0]));
    assert!(cache.set(keys[1].clone(), values[1].clone(), 0));
    assert_eq!(cache.get(&keys[1]).as_deref(), Some(&values[1]));
    assert!(cache.set(keys[2].clone(), values[2].clone(), 0));
    assert_eq!(cache.get(&keys[2]).as_deref(), Some(&values[2]));

    assert!(cache.get(&keys[0]).is_none());
    assert!(cache.get(&keys[1]).is_none());
}

/// Go `pkg/executor/internal/applycache/apply_cache_test.go:82::TestApplyCacheConcurrent`:
/// the Go cache protects `get`/`put`/`removeOldest` with its mutex
/// (`pkg/executor/internal/applycache/apply_cache.go:58-74`); alternating two
/// quota-sized entries from concurrent goroutines must not race or corrupt the
/// cache.
#[test]
fn apply_cache_concurrent_get_and_set_is_safe() {
    let cache = Arc::new(ApplyCache::<Vec<i64>>::new(100));
    let key0 = vec![b'0'; 100];
    let key1 = vec![b'1'; 100];
    assert!(cache.set(key0.clone(), vec![0], 0));

    let first_cache = Arc::clone(&cache);
    let first_key = key0.clone();
    let second_key = key1.clone();
    let first = thread::spawn(move || {
        for _ in 0..100 {
            loop {
                if first_cache.get(&first_key).is_some() {
                    assert!(first_cache.set(second_key.clone(), vec![1], 0));
                    break;
                }
            }
        }
    });

    let second_cache = Arc::clone(&cache);
    let first_key = key0.clone();
    let second_key = key1.clone();
    let second = thread::spawn(move || {
        for _ in 0..100 {
            loop {
                if second_cache.get(&second_key).is_some() {
                    assert!(second_cache.set(first_key.clone(), vec![0], 0));
                    break;
                }
            }
        }
    });

    first.join().expect("first cache worker");
    second.join().expect("second cache worker");
    assert!(cache.set(key0.clone(), vec![0], 0));
    assert_eq!(cache.get(&key0).as_deref(), Some(&vec![0]));
}

/// Go `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:30::TestVecGroupCheckerDATARACE`:
/// the Go checker copies first/last evaluated datums before the temporary
/// vector column is reused (`pkg/executor/internal/vecgroupchecker/vec_group_checker.go:160-224`).
/// Rust stores an owned encoded boundary key, so replacing the caller's
/// variable-length, decimal, or JSON value cannot change the remembered key.
#[test]
fn vec_group_checker_datarace_owns_variable_length_and_complex_values() {
    let cases = [
        (
            Datum::new_string("abc"),
            Datum::new_string("replacement that grows the source"),
        ),
        (
            Datum::Decimal(Decimal::from_int(123)),
            Datum::Decimal(Decimal::from_int(456)),
        ),
        (
            Datum::Json(BinaryJSON::parse(r#"{"123":123}"#).expect("JSON")),
            Datum::Json(BinaryJSON::parse(r#"{"456":456}"#).expect("JSON")),
        ),
    ];

    for (original, replacement) in cases {
        let mut checker = VecGroupChecker::new(Vec::new());
        let mut source = vec![vec![original.clone()]];
        checker
            .split_evaluated(&source, &[Collation::Binary])
            .expect("first key evaluates");
        source[0][0] = replacement;
        assert!(
            checker
                .split_evaluated(&[vec![original]], &[Collation::Binary])
                .expect("second key evaluates")
        );
    }
}

/// Go `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:141::TestVecGroupChecker4GroupCount`:
/// `SplitIntoGroups` (`pkg/executor/internal/vecgroupchecker/vec_group_checker.go:80-157`)
/// reports the expected group count and whether the first group continues the
/// previous chunk for each chunk-size/same-value matrix.
#[test]
fn vec_group_checker_four_group_count_matrix_matches_go() {
    let cases = [
        (&[1024, 1][..], 1, 1025, &[false, false][..]),
        (&[1024, 1][..], 1025, 1, &[false, true][..]),
        (&[1, 1][..], 2, 1, &[false, true][..]),
        (&[1, 1][..], 1, 2, &[false, false][..]),
        (&[2, 2][..], 2, 2, &[false, false][..]),
        (&[2, 2][..], 4, 1, &[false, true][..]),
    ];
    let field = FieldType::new(FieldTypeCode::LongLong);

    for (chunk_rows, same_rows, expected_groups, expected_flags) in cases {
        let mut checker = VecGroupChecker::new(vec![grouping_column(0, field.clone())]);
        let mut global_row = 0usize;
        let mut group_count = 0usize;
        for (chunk_index, rows) in chunk_rows.iter().copied().enumerate() {
            let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), rows);
            for _ in 0..rows {
                chunk.append_int64(0, (global_row / same_rows) as i64);
                global_row += 1;
            }
            let continues = checker
                .split_into_groups(&NoColumns, &chunk)
                .expect("grouping succeeds");
            assert_eq!(continues, expected_flags[chunk_index]);
            group_count += checker.group_count() - usize::from(continues);
        }
        assert_eq!(group_count, expected_groups);
    }
}

/// Go `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:205::TestVecGroupChecker`:
/// adjacent equality uses the field collation and CHAR/VARCHAR padding rules
/// (`pkg/executor/internal/vecgroupchecker/vec_group_checker.go:350-520`),
/// producing six binary groups, three case-insensitive groups, and one padded
/// UTF-8 binary group.
#[test]
fn vec_group_checker_matches_collation_and_padding() {
    let keys = ["aaa", "AAA", "😜", "😃", "À", "A"]
        .into_iter()
        .map(|value| vec![Datum::new_string(value)])
        .collect::<Vec<_>>();

    let mut checker = VecGroupChecker::new(Vec::new());
    checker
        .split_evaluated(&keys, &[Collation::Binary])
        .expect("binary grouping");
    assert_eq!(ranges(&mut checker).len(), 6);

    checker = VecGroupChecker::new(Vec::new());
    checker
        .split_evaluated(&keys, &[Collation::Utf8Mb4GeneralCi])
        .expect("general-ci grouping");
    assert_eq!(ranges(&mut checker), [(0, 2), (2, 4), (4, 6)]);

    checker = VecGroupChecker::new(Vec::new());
    checker
        .split_evaluated(&keys, &[Collation::Utf8Mb4UnicodeCi])
        .expect("unicode-ci grouping");
    assert_eq!(ranges(&mut checker), [(0, 2), (2, 4), (4, 6)]);

    let padded = ["a", "a  ", "a    "]
        .into_iter()
        .map(|value| vec![Datum::new_string(value)])
        .collect::<Vec<_>>();
    checker = VecGroupChecker::new(Vec::new());
    checker
        .split_evaluated(&padded, &[Collation::Utf8Mb4Bin])
        .expect("padded grouping");
    assert_eq!(ranges(&mut checker), [(0, 3)]);
}

/// Go `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:272::TestIssue53867`:
/// `Reset` clears the group offsets and consumption cursor
/// (`pkg/executor/internal/vecgroupchecker/vec_group_checker.go:540-561`), so a
/// partially consumed checker is exhausted after reset.
#[test]
fn issue_53867_reset_discards_unconsumed_groups() {
    let mut checker = VecGroupChecker::new(Vec::new());
    checker
        .split_evaluated(
            &[vec![Datum::Int(1)], vec![Datum::Int(2)]],
            &[Collation::Binary],
        )
        .expect("grouping succeeds");
    assert!(!checker.is_exhausted());
    assert_eq!(checker.get_next_group(), (0, 1));
    assert!(!checker.is_exhausted());
    checker.reset();
    assert!(checker.is_exhausted());
}

/// Go `pkg/executor/internal/applycache/main_test.go:27::TestMain` only
/// installs the Go test process harness; it has no behavior to reproduce.
#[test]
#[ignore = "skipped-reason: Go TestMain is process bootstrap/goleak setup, not a behavior test"]
fn apply_cache_test_main_is_bootstrap_only() {}

/// Go `pkg/executor/internal/calibrateresource/main_test.go:27::TestMain` only
/// installs the Go test process harness; it has no behavior to reproduce.
#[test]
#[ignore = "skipped-reason: Go TestMain is process bootstrap/goleak setup, not a behavior test"]
fn calibrate_resource_test_main_is_bootstrap_only() {}

/// Go `pkg/executor/internal/pdhelper/main_test.go:27::TestMain` only installs
/// the Go test process harness; it has no behavior to reproduce.
#[test]
#[ignore = "skipped-reason: Go TestMain is process bootstrap/goleak setup, not a behavior test"]
fn pd_helper_test_main_is_bootstrap_only() {}

/// Go `pkg/executor/internal/querywatch/main_test.go:27::TestMain` only installs
/// the Go test process harness; it has no behavior to reproduce.
#[test]
#[ignore = "skipped-reason: Go TestMain is process bootstrap/goleak setup, not a behavior test"]
fn query_watch_test_main_is_bootstrap_only() {}

/// Go `pkg/executor/internal/vecgroupchecker/main_test.go:27::TestMain` only
/// installs the Go test process harness; it has no behavior to reproduce.
#[test]
#[ignore = "skipped-reason: Go TestMain is process bootstrap/goleak setup, not a behavior test"]
fn vec_group_checker_test_main_is_bootstrap_only() {}

/// Go `pkg/executor/internal/calibrateresource/calibrate_resource_test.go:40::TestCalibrateResource`
/// drives `calibrateresource.Executor.Next` (`calibrate_resource.go:133-264`)
/// through session SQL, failpoint metrics, PD resource control, and cluster
/// metadata, none of which exists in the Rust executor crate.
#[test]
#[ignore = "go-parity-gap: CALIBRATE RESOURCE needs Go session SQL, PD resource control, failpoint metrics, and cluster metadata (pkg/executor/internal/calibrateresource/calibrate_resource.go:133)"]
fn calibrate_resource_matches_session_and_pd_resource_control() {}

/// Go `pkg/executor/internal/exec/executor_test.go:35::TestNextIOAccAddInputCountsRowsWithZeroCols`
/// covers the private `nextIOAcc` accumulator (`exec/executor.go:42-89`),
/// including reuse and allocation behavior; no equivalent Rust accumulator is
/// exposed by `tidb-executor`.
#[test]
#[ignore = "go-parity-gap: private nextIOAcc reuse and RUV2 child accounting (pkg/executor/internal/exec/executor.go:42) have no Rust test surface"]
fn next_io_acc_add_input_counts_rows_with_zero_cols() {}

/// Go `pkg/executor/internal/exec/executor_test.go:73::TestRUV2ExecutorMetricByTypeIncludesConcreteExecutorTypes`
/// pins the private concrete-type mapping in `exec/executor.go:91-160`; Rust
/// does not expose the Go runtime-type/RUV2 metric registry.
#[test]
#[ignore = "go-parity-gap: Go concrete executor type to RUV2 metric mapping (pkg/executor/internal/exec/executor.go:91) is not modeled in Rust"]
fn ruv2_executor_metric_by_type_includes_concrete_executor_types() {}

/// Go `pkg/executor/internal/exec/indexusage_test.go:39::TestIndexUsageReporter`
/// exercises `IndexUsageReporter` (`exec/indexusage.go:29-132`) through a live
/// domain's asynchronous statistics reporter; that domain/index-usage path is
/// absent from this crate.
#[test]
#[ignore = "go-parity-gap: live domain statistics and asynchronous IndexUsageReporter (pkg/executor/internal/exec/indexusage.go:29) are unported"]
fn index_usage_reporter_records_point_get_and_cop_usage() {}

/// Go `pkg/executor/internal/exec/indexusage_test.go:217::TestIndexUsageReporterWithRealData`
/// verifies SQL plan selection and sampled index usage through the Go testkit;
/// Rust has no equivalent index-usage collector or analyze-backed domain.
#[test]
#[ignore = "go-parity-gap: SQL-driven index usage collection and analyze statistics (pkg/executor/internal/exec/indexusage_test.go:217) are unported"]
fn index_usage_reporter_with_real_data() {}

/// Go `pkg/executor/internal/exec/indexusage_test.go:288::TestIndexUsageReporterWithPartitionTable`
/// verifies partition physical-table samples through `IndexUsageReporter`
/// (`indexusage.go:73-115`), requiring Go partition metadata and statistics.
#[test]
#[ignore = "go-parity-gap: partition index-usage reporting through the Go domain/statistics handle (pkg/executor/internal/exec/indexusage.go:73) is unported"]
fn index_usage_reporter_with_partition_table() {}

/// Go `pkg/executor/internal/exec/indexusage_test.go:359::TestIndexUsageReporterWithGlobalIndex`
/// verifies global-index point/batch usage through the same reporter and
/// physical/global index metadata (`indexusage.go:49-115`).
#[test]
#[ignore = "go-parity-gap: global-index usage metadata and domain reporter (pkg/executor/internal/exec/indexusage.go:49) are unported"]
fn index_usage_reporter_with_global_index() {}

/// Go `pkg/executor/internal/exec/indexusage_test.go:409::TestDisableIndexUsageReporter`
/// verifies `tidb_enable_collect_execution_info` suppresses later samples via
/// the session/domain collector (`indexusage.go:29-47`), which Rust does not
/// implement.
#[test]
#[ignore = "go-parity-gap: session execution-info switch and index usage collector (pkg/executor/internal/exec/indexusage_test.go:409) are unported"]
fn disable_index_usage_reporter() {}

/// Go `pkg/executor/internal/exec/indexusage_test.go:447::TestIndexUsageReporterWithClusterIndex`
/// verifies clustered, common-handle, nonclustered-PK, and index-merge samples;
/// the required Go index metadata/statistics path is absent.
#[test]
#[ignore = "go-parity-gap: clustered-index usage reporting needs Go table metadata and statistics handles (pkg/executor/internal/exec/indexusage_test.go:447)"]
fn index_usage_reporter_with_cluster_index() {}

/// Go `pkg/executor/internal/mpp/local_mpp_coordinator_test.go:25::TestNeedReportExecutionSummary`
/// tests `needReportExecutionSummary` (`local_mpp_coordinator.go:461-486`) over
/// physical-plan/exchange nodes; Rust has no local MPP coordinator plan tree.
#[test]
#[ignore = "go-parity-gap: local MPP physical-plan exchange coordinator (pkg/executor/internal/mpp/local_mpp_coordinator.go:461) is unported"]
fn need_report_execution_summary() {}

/// Go `pkg/executor/internal/mpp/local_mpp_coordinator_test.go:72::TestZoneHelperTryQuickFill`
/// tests `taskZoneInfoHelper.tryQuickFillWithUncertainZones`
/// (`local_mpp_coordinator.go:319-350`) over TiFlash task metadata; that
/// coordinator/store metadata surface is absent from Rust.
#[test]
#[ignore = "go-parity-gap: TiFlash task-zone metadata and local MPP coordinator (pkg/executor/internal/mpp/local_mpp_coordinator.go:319) are unported"]
fn zone_helper_try_quick_fill() {}

/// Go `pkg/executor/internal/pdhelper/pd_test.go:42::TestTTLCache` verifies
/// `PDHelper.GetApproximateTableCountFromStorage` (`pd.go:38-87`) caches by
/// table identity with capacity and TTL; Rust has no PD helper or TTL cache
/// seam.
#[test]
#[ignore = "go-parity-gap: PDHelper approximate-count storage callback and TTL cache (pkg/executor/internal/pdhelper/pd.go:38) are unported"]
fn ttl_cache() {}

/// Go `pkg/executor/internal/querywatch/query_watch_test.go:33::TestQueryWatch`
/// drives `AddExecutor.Next` (`query_watch.go:165-199`) through resource-group
/// runaway watches, failpoints, asynchronous GC, and information-schema rows.
#[test]
#[ignore = "go-parity-gap: runaway query-watch SQL, resource groups, failpoints, and session/domain tables (pkg/executor/internal/querywatch/query_watch.go:165) are unported"]
fn query_watch() {}

/// Go `pkg/executor/internal/querywatch/query_watch_test.go:207::TestQueryWatchIssue56897`
/// checks query-watch matching across `USE` statements through the same
/// runaway-watch executor (`query_watch.go:165-199`), which Rust does not have.
#[test]
#[ignore = "go-parity-gap: query-watch matching across USE statements (pkg/executor/internal/querywatch/query_watch.go:165) is unported"]
fn query_watch_issue_56897() {}
