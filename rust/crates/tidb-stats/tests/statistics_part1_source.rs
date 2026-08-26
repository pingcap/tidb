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

//! Remaining `pkg/statistics` (master) unit tests for testport batch b042
//! that are portable against the current `tidb-stats` surface, plus
//! `#[ignore]` markers for the go-parity gaps that are not.

use tidb_stats::ColAndIdxExistenceMap;

/// table_test.go::TestCloneColAndIdxExistenceMap.
#[test]
fn source_clone_col_and_idx_existence_map() {
    let mut m = ColAndIdxExistenceMap::new_without_size();
    m.insert_column(1, true);
    m.insert_index(1, true);

    let m2 = m.deep_clone();
    assert!(m.is_equal(&m2));
    assert_eq!(m, m2);
}

/// analyze_version_policy: table_test.go::
/// TestResolveAnalyzeVersionOnTableKeepsRequestedVersion.
///
/// A table whose stored stats version (Version1) differs from the requested
/// analyze version (Version2) is a mismatch even though it was analyzed
/// (`LastAnalyzeVersion > 0`).
#[test]
fn source_resolve_analyze_version_on_table_keeps_requested_version() {
    // Go fixture: StatsVer: Version1, LastAnalyzeVersion: 1.
    let matches = tidb_stats::analyze_version_matches(Some(1), false, 2);
    assert!(!matches);
}

// -------------------------------------------------------------------------
// go-parity-gap markers. Each `#[ignore]`d test names one Go test from
// origin/master `pkg/statistics` whose behavior has no Rust surface yet.
// -------------------------------------------------------------------------

// histogram_test.go::TestValueToString4InvalidKey
#[test]
#[ignore = "go-parity-gap: statistics.ValueToString (formatted multi-column datum rendering) is not ported to tidb-stats"]
fn source_value_to_string4_invalid_key() {
    unreachable!("gated by go-parity-gap ignore")
}

// histogram_test.go::TestIndexQueryBytes
#[test]
#[ignore = "go-parity-gap: Rust Index has no Bounds chunk / PreCalculateScalar bucket-count lookup; Index::query_bytes takes the histogram fallback count as a caller-supplied scalar"]
fn source_index_query_bytes() {
    unreachable!("gated by go-parity-gap ignore")
}

// histogram_test.go::TestNewPseudoHistogramReuseChunk
#[test]
#[ignore = "go-parity-gap: NewPseudoHistogram (and its shared Bounds-chunk reuse) is not ported to tidb-stats"]
fn source_new_pseudo_histogram_reuse_chunk() {
    unreachable!("gated by go-parity-gap ignore")
}

// statistics_test.go::TestPruneTopN
#[test]
#[ignore = "go-parity-gap: Rust prune_topn_item is module-private in tidb_stats::builder and cannot be driven directly without a visibility change outside this batch's edit scope; its pruning behavior is exercised indirectly by builder_source tests"]
fn source_prune_topn() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_test.go::TestMergePartTopNAndHistToGlobal (+ the declarative
// cases in merge_global_cases_test.go)
#[test]
#[ignore = "go-parity-gap: MergePartTopNAndHistToGlobal (combined partition TopN+histogram global merge) is not ported to tidb-stats; only the TopN-only merge lives in global_topn.rs"]
fn source_merge_part_topn_and_hist_to_global() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestExpBackoffEstimation
#[test]
#[ignore = "go-parity-gap: needs a session context plus loaded column stats through the statistics handle; no storage/session surface exists inside tidb-stats"]
fn source_exp_backoff_estimation() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestNULLOnFullSampling
#[test]
#[ignore = "go-parity-gap: needs a session context and ANALYZE through the statistics handle against a mock store"]
fn source_null_on_full_sampling() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestAnalyzeSnapshot
#[test]
#[ignore = "go-parity-gap: drives ANALYZE/queries through a session on a mock store; outside tidb-stats' dependency-closed surface"]
fn source_analyze_snapshot() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestOutdatedStatsCheck
#[test]
#[ignore = "go-parity-gap: requires the statistics handle with storage-backed table stats; not representable inside tidb-stats"]
fn source_outdated_stats_check() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestShowHistogramsLoadStatus
#[test]
#[ignore = "go-parity-gap: asserts on SHOW statements executed through a testkit session; no SQL layer inside tidb-stats"]
fn source_show_histograms_load_status() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestSingleColumnIndexNDV
#[test]
#[ignore = "go-parity-gap: runs ANALYZE through a session and reads index NDV via the handle; outside tidb-stats"]
fn source_single_column_index_ndv() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestColumnStatsLazyLoad
#[test]
#[ignore = "go-parity-gap: exercises lazy column loading through the handle's storage read path; not ported into tidb-stats"]
fn source_column_stats_lazy_load() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestUpdateNotLoadIndexFMSketch
#[test]
#[ignore = "go-parity-gap: needs handle/storage-backed index FM-sketch maintenance; outside tidb-stats"]
fn source_update_not_load_index_fm_sketch() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestIssue44369
#[test]
#[ignore = "go-parity-gap: regression test driving partition ANALYZE through a session/handle; outside tidb-stats"]
fn source_issue44369() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestTableLastAnalyzeVersion
#[test]
#[ignore = "go-parity-gap: mixes DDL, ANALYZE sessions, and handle state; only the pure policy slice is covered by analysis_policy/table_source tests"]
fn source_table_last_analyze_version() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestGlobalIndexWithHistoricalStats
#[test]
#[ignore = "go-parity-gap: needs historical-stats reads through the handle over a mock store; outside tidb-stats"]
fn source_global_index_with_historical_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestLastAnalyzeVersionNotChangedWithAsyncStatsLoad
#[test]
#[ignore = "go-parity-gap: async stats load through the handle/session; outside tidb-stats"]
fn source_last_analyze_version_not_changed_with_async_stats_load() {
    unreachable!("gated by go-parity-gap ignore")
}

// integration_test.go::TestSaveMetaToStorage
#[test]
#[ignore = "go-parity-gap: writes stats meta through the storage-backed handle; outside tidb-stats"]
fn source_save_meta_to_storage() {
    unreachable!("gated by go-parity-gap ignore")
}
