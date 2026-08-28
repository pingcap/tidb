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

//! Ports of the uncovered `pkg/executor.part14` items 832–840:
//! `TestDynamicExpandMustForceAllColumns`, the six `analyzetest/columns`
//! tests, and the two package `TestMain` bootstraps.
//!
//! The b132 receipt reuses the aggregate/analyze carriers already present in
//! this crate for items 781–831. These final items exercise SQL/session
//! surfaces that are deliberately outside this crate's in-process statistics
//! engine: persisted `mysql.analyze_options`, stats-version rewrites,
//! predicate-column tracking, column-scoped ANALYZE, and goleak suite setup.

/// Go `analyze_test.go:2217::TestDynamicExpandMustForceAllColumns`.
///
/// The Go contract persists a `PREDICATE` column choice, downgrades one
/// partition's histograms to v1, and requires a dynamic partition analyze to
/// force every column back to v2. This tier has no `mysql.analyze_options`
/// or `mysql.stats_histograms` persistence layer, no predicate-column usage
/// collector, and no stats-version rewrite path; its in-process ANALYZE
/// always analyzes the complete visible schema instead.
#[test]
#[ignore = "go-parity-gap: dynamic partition mustAllColumns behavior depends on mysql.analyze_options/stats_histograms persistence, predicate-column tracking, and v1/v2 rewrite surfaces unported"]
fn dynamic_expand_forces_all_columns() {}

/// Go `columns/analyze_columns_with_test.go:28::TestAnalyzeColumnsWithPrimaryKey`.
///
/// Both the explicit column-list and predicate-column arms require the Go
/// `mysql.stats_*`/`mysql.column_stats_usage` persistence and warning rows.
/// `lower_analyze_admin` rejects both column-scoped targets before execution
/// on this tier, so this is retained as an explicit gap rather than a weaker
/// whole-table ANALYZE assertion.
#[test]
#[ignore = "go-parity-gap: column-list/predicate-column ANALYZE and column_stats_usage persistence are unported; lower_analyze_admin rejects these targets"]
fn analyze_columns_with_primary_key() {}

/// Go `columns/analyze_columns_with_test.go:96::TestAnalyzeColumnsWithIndex`.
#[test]
#[ignore = "go-parity-gap: column-list/predicate-column ANALYZE and mysql.stats_* persistence are unported; lower_analyze_admin rejects these targets"]
fn analyze_columns_with_index() {}

/// Go `columns/analyze_columns_with_test.go:173::TestAnalyzeColumnsWithClusteredIndex`.
#[test]
#[ignore = "go-parity-gap: column-list/predicate-column ANALYZE and mysql.stats_* persistence are unported; lower_analyze_admin rejects these targets"]
fn analyze_columns_with_clustered_index() {}

/// Go `columns/analyze_columns_with_test.go:250::TestAnalyzeColumnsWithDynamicPartitionTable`.
#[test]
#[ignore = "go-parity-gap: dynamic partition column-scope ANALYZE, predicate tracking, and global/partition stats persistence are unported"]
fn analyze_columns_with_dynamic_partition_table() {}

/// Go `columns/analyze_columns_with_test.go:376::TestAnalyzeColumnsWithStaticPartitionTable`.
#[test]
#[ignore = "go-parity-gap: static partition column-scope ANALYZE and mysql.stats_* persistence are unported"]
fn analyze_columns_with_static_partition_table() {}

/// Go `columns/analyze_columns_with_test.go:487::TestAnalyzeColumnsWithVirtualColumnIndex`.
#[test]
#[ignore = "go-parity-gap: virtual-column column-scope ANALYZE and column_stats_usage persistence are unported"]
fn analyze_columns_with_virtual_column_index() {}

/// Go `analyzetest/columns/main_test.go:24::TestMain` and
/// `analyzetest/main_test.go:24::TestMain` are suite bootstrap only:
/// they configure a global stats-cache quota and wrap the package in goleak.
/// There is no per-test SQL or statistics behavior to port; Rust tests set up
/// their own catalog and do not use Go's package TestMain hook.
#[test]
#[ignore = "skipped-reason: analyzetest and analyzetest/columns TestMain only configure global state and goleak; no behavior to pin"]
fn analyzetest_suite_mains_are_bootstrap() {}
