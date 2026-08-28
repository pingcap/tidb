// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 license (see the License file at the crate root).

//! Gap tests for `pkg/executor/inspection_result_test.go` and
//! `pkg/executor/inspection_summary_test.go`.
//!
//! All of these drive `information_schema.inspection_result` /
//! `inspection_summary` reads whose data path is
//! `inspectionResultRetriever::retrieve` (`pkg/executor/inspection_result.go:117`)
//! over the cluster-config/version/node-load inspectors (:213 `configInspection`,
//! :461 `versionInspection`, :487 `nodeLoadInspection`), backed by
//! `TableSnapshot` mocks of `cluster_config`/`cluster_load`/`metrics_schema`
//! tables and the `mockMetricsTableData` failpoint
//! (`pkg/executor/metrics_reader.go:60`). None of the inspection retrievers,
//! the metric-table registry (`pkg/infoschema/metric_table_def.go:19
//! MetricTableMap`), or those failpoints exist on this tier.
//!
//! The one leaf Go helper these tests exercise that IS ported --
//! `configInspection.convertReadableSizeToByteSize`
//! (`pkg/executor/inspection_result.go:437`) -- is pinned (pre-existing) by
//! `tidb-exec/tests/readable_size_source.rs::readable_size_preserves_source_suffixes_and_parse_boundaries`;
//! the retriever around it is the gap below.

/// Go `pkg/executor/inspection_result_test.go:39::TestInspectionResult`:
/// `select * from information_schema.inspection_result` over mocked
/// `cluster_config` snapshots must report inconsistent `ddl.lease`,
/// `raftstore.sync-log`, `advertise-client-urls`, and slow-threshold rows as
/// `critical`/`warning` results with the expected item/issue text.
#[test]
#[ignore = "go-parity-gap: inspectionResultRetriever (inspection_result.go:117) and the cluster_config TableSnapshot seam are unported"]
fn inspection_result_reports_mock_cluster_config_inconsistencies() {}

/// Go `pkg/executor/inspection_result_test.go:248::TestThresholdCheckInspection`:
/// the `threshold-check` rule (`inspection_result.go:97`) over mocked
/// `node_hardware`/`node_load`/`pd_schedule`/`read_throughput`/`write_throughput`
/// metrics rows evaluates each rule table with its thresholds and emits
/// `timeout|warning|critical` inspection results in a stable order.
#[test]
#[ignore = "go-parity-gap: thresholdCheckInspection (inspection_result.go:97) and the metrics_schema table mocks are unported"]
fn threshold_check_inspection_evaluates_mock_metric_rules() {}

/// Go `pkg/executor/inspection_result_test.go:339::TestThresholdCheckInspection2`:
/// additional threshold-check rules whose value comparison shapes differ
/// (byte-size, duration, and percentage columns from the same mocked metric
/// tables).
#[test]
#[ignore = "go-parity-gap: thresholdCheckInspection (inspection_result.go:97) and the metrics_schema table mocks are unported"]
fn threshold_check_inspection_second_rule_set() {}

/// Go `pkg/executor/inspection_result_test.go:422::TestThresholdCheckInspection3`:
/// the threshold-check arm whose rows must be grouped per instance before
/// the threshold comparison, again over mocked metrics rows.
#[test]
#[ignore = "go-parity-gap: thresholdCheckInspection (inspection_result.go:97) and the metrics_schema table mocks are unported"]
fn threshold_check_inspection_third_rule_set() {}

/// Go `pkg/executor/inspection_result_test.go:508::TestCriticalErrorInspection`:
/// the `critical-error` rule (`inspection_result.go:94`) maps mocked
/// `metrics_schema.tidb_critical_error` rows into inspection results with
/// their occurrence counts and instance list.
#[test]
#[ignore = "go-parity-gap: criticalErrorInspection (inspection_result.go:94) and the metrics_schema table mocks are unported"]
fn critical_error_inspection_lists_mock_errors() {}

/// Go `pkg/executor/inspection_result_test.go:629::TestNodeLoadInspection`:
/// the node-load inspector (`inspection_result.go:487 nodeLoadInspection`)
/// aggregates mocked `node_load` metric rows into per-instance cpu/memory
/// usage summaries.
#[test]
#[ignore = "go-parity-gap: nodeLoadInspection (inspection_result.go:487) and the cluster_load table mocks are unported"]
fn node_load_inspection_sums_mock_load_rows() {}

/// Go `pkg/executor/inspection_result_test.go:705::TestConfigCheckOfStorageBlockCacheSize`:
/// `configInspection.checkTiKVBlockCacheSizeConfig`
/// (`inspection_result.go:375`) sums each TiKV instance's
/// `storage.block-cache.capacity` (parsed by
/// `convertReadableSizeToByteSize`, :437), compares it against 80% of the
/// instance's total memory from `metrics_schema.node_total_memory`, and
/// warns for oversized caches. The size PARSER half is already pinned by
/// `tidb-exec/tests/readable_size_source.rs`; the retriever + metrics join
/// are the gap.
#[test]
#[ignore = "go-parity-gap: checkTiKVBlockCacheSizeConfig (inspection_result.go:375) needs the ExecRestrictedSQL metrics join and cluster_config mocks; only its size parser is ported (tidb-exec/tests/readable_size_source.rs)"]
fn config_check_of_storage_block_cache_size_compares_against_memory() {}

/// Go `pkg/executor/inspection_summary_test.go:31::TestValidInspectionSummaryRules`:
/// every table named in `executor.InspectionSummaryRules`
/// (`pkg/executor/inspection_summary.go:42`) must exist in
/// `infoschema.MetricTableMap` (`pkg/infoschema/metric_table_def.go:19`)
/// and no rule may name a table twice. Neither registry is ported.
#[test]
#[ignore = "go-parity-gap: inspectionSummaryRules (inspection_summary.go:42) and the MetricTableMap registry (metric_table_def.go:19) are unported"]
fn valid_inspection_summary_rules_reference_defined_metric_tables() {}

/// Go `pkg/executor/inspection_summary_test.go:44::TestInspectionSummary`:
/// `select * from information_schema.inspection_summary where rule =
/// 'query-summary'` over the `mockMetricsTableData` failpoint
/// (`pkg/executor/metrics_reader.go:60`) must aggregate mocked `tidb_qps`
/// and `tidb_query_duration` rows into per-instance per-quantile summary
/// rows (count/max/min value columns) with zero warnings.
#[test]
#[ignore = "go-parity-gap: the inspection_summary retriever and the mockMetricsTableData failpoint (metrics_reader.go:60) are unported"]
fn inspection_summary_aggregates_mocked_metric_tables() {}
