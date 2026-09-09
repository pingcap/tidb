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

//! Documentary gap ports for `pkg/planner/core/casetest/cbotest`
//! (`pkg/planner.part3` items 135–154 on `origin/master`).
//!
//! All 19 tests are EXPLAIN-golden casetests against a live TiDB
//! (`RunTestUnderCascadesWithDomain`): they create tables, feed stats
//! through the statistics handle (`statstestutil.HandleNextDDLEventWithTxn`,
//! `flush stats_delta`, `testkit.LoadTableStats(<json>)` or ANALYZE) and
//! compare whole plan outputs from the `analyze_suite` book, some with
//! warning columns. The Rust workspace has neither the session/executor
//! stack nor the statistics-handle update pipeline these pin, so every
//! port is recorded as an explicit gap. Bootstrap
//! (`main_test.go:29 TestMain`) loads only the suite; skipped-reason in
//! the receipt.

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:40
/// TestCBOWithoutAnalyze`.
///
/// Pins plans chosen when tables only have count info (6 rows inserted,
/// delta flushed via `HandleNextDDLEventWithTxn` + `Update`, no ANALYZE);
/// the analyze_suite goldens for those inputs must stay stable.
#[test]
#[ignore = "go-parity-gap: needs live planner cost decisions fed by realtime-count-only stats; no session/executor or stats handle on the Rust side"]
fn cbo_without_analyze_count_only_stats_plans_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:73
/// TestAnalyzeSuiteRegression`.
///
/// Three regressions in one body: issue:62438 (`objects` PK + idx over
/// metastore_uuid/securable_id with loaded `issue62438.json`), issue:61389
/// (enum-column unique-key join whose plan AND warnings are pinned), and
/// issue:61792 (executor/hash-join/distsql concurrency lowered to fixed
/// values before planning a cardcore-statement lookup). Each sub-block
/// re-loads its own named book section via `LoadTestCasesByName`.
#[test]
#[ignore = "go-parity-gap: three golden plan(+warning) suites run through the live server with LoadTableStats json fixtures; no equivalent planner/statistics plumbing exists here"]
fn analyze_suite_regression_issue_62438_61389_61792() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:268
/// TestTop2SeedGreedyJoinReorderWithLoadedStats`.
///
/// Anchored by a sanitized replayer stats fixture in database gjo_stats:
/// with `tidb_opt_enable_advanced_join_reorder=1` and threshold 0, the
/// root join key must become `gjo_pie.t_id = gjo_dim.id` (the top-2-seed
/// fix, Go sha 375e6a1), not the pre-fix `gjo_p.id = gjo_pi.p_id`; planRows
/// assertions also require `outer key:gjo_stats.gjo_pi.id,
/// inner key:gjo_stats.gjo_pie.pi_id` at row 3 and
/// `outer key:gjo_stats.gjo_p.id, inner key:gjo_stats.gjo_pi.p_id` at row 4.
#[test]
#[ignore = "go-parity-gap: advanced join reorder (top-2 seeds) with loaded replayer stats requires the live optimizer + stats loading path"]
fn top2_seed_greedy_join_reorder_with_loaded_stats() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:345
/// TestStraightJoin`.
///
/// For t1..t4 `(a int)` with only DDL-event counts handled, the
/// straight-join ordered outputs from analyze_suite must be reproduced
/// verbatim by the live planner.
#[test]
#[ignore = "go-parity-gap: live-server plan output comparison; nothing else to say -- tidb-planner cannot execute or format plans end to end"]
fn straight_join_order_pinned_by_analyze_suite() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:367
/// TestTableDual`.
///
/// t(a int) filled with 1..10 and flushed like count-only stats; the suite
/// pins that impossible ranges degrade to TableDual plans.
#[test]
#[ignore = "go-parity-gap: TableDual selection is part of live findBestTask costing over real ranges + stats"]
fn table_dual_chosen_when_range_is_impossible() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:395
/// TestEstimation`.
///
/// Runs with `statistics.RatioOfPseudoEstimate` bumped to 10.0 (restored
/// to 0.7 after) so pseudo estimates survive larger-NDV cases; analyze_suite
/// plans must match under that override.
#[test]
#[ignore = "go-parity-gap: RatioOfPseudoEstimate behavior sits inside unported stats derivation feeding live plan choices"]
fn estimation_respects_ratio_of_pseudo_estimate() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:432
/// TestIndexRead`.
///
/// Wide single-table fixture (b/d/e/ts/b_c indexes, datetime/timestamp
/// defaults) plus t1 with loaded `analyzesSuiteTestIndexReadT.json`: pins
/// index-vs-table-scan/read choices including concurrency settings.
#[test]
#[ignore = "go-parity-gap: index-read choice goldens need the live cost model over histograms loaded from json; unported here"]
fn index_read_choice_with_loaded_stats_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:483
/// TestEmptyTable`.
///
/// Empty-table variants of the CBO outputs must match analyze_suite exactly.
#[test]
#[ignore = "go-parity-gap: live planner outputs for empty stats; no execution environment in this crate"]
fn empty_table_row_counts_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:514
/// TestAnalyze`.
///
/// General ANALYZE-behavior plan goldens from analyze_suite.
#[test]
#[ignore = "go-parity-gap: same live-testkit dependency as the rest of this file"]
fn analyze_output_and_limits_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:627
/// TestNullCount`.
///
/// Pins NULL-count-based estimation over `index idx(a)` pairs (query,
/// expected) through the live planner.
#[test]
#[ignore = "go-parity-gap: null-count histogram fields are read by unported estimation inside live planning"]
fn null_count_index_estimation_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:656
/// TestCorrelatedEstimation`.
///
/// Correlated-column estimation through `t(a int, b int, c int, index
/// idx(c,b,a))`: outer-side filtering must refine inner scan estimates.
#[test]
#[ignore = "go-parity-gap: correlated apply estimation happens during live physical optimization; not ported"]
fn correlated_estimation_over_index_prefix_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:709
/// TestInconsistentEstimation`.
///
/// Wrapper over shared helper `testInconsistentEstimation` (cbo_test.go:709)
/// exercising t(a,b,c) with indexes ab(a,b)/ac(a,c); the case rows show one
/// index giving inconsistent estimate vs the other while both appear as
/// candidates in the pinned plan text.
#[test]
#[ignore = "go-parity-gap: relies on live cost model disagreement between two indexes; cannot pin without execution"]
fn inconsistent_estimation_between_indexes_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:713
/// TestLimitCrossEstimation`.
///
/// `t(a int primary key, b int not null, c int not null default 0, index
/// idx_bc(b, c))`: limit-cross estimation refines inner-side cardinality
/// through the limit and keeps joined plans matching the book.
#[test]
#[ignore = "go-parity-gap: limit-cross estimation logic runs during live optimize; not ported into tidb-planner"]
fn limit_cross_estimation_refines_join_side() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:747
/// TestLowSelIndexGreedySearch`.
///
/// Four-column varchar fixture with keys idx1(d,a)/idx2(a,c)/idx3(c,b)/
/// idx4(e) and loaded json stats: greedy search must still pick the
/// low-selectivity combo the book records.
#[test]
#[ignore = "go-parity-gap: greedy index search priced over loaded histograms in the live optimizer; unported"]
fn low_sel_index_greedy_search_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:774
/// TestIndexChoiceByNDV`.
///
/// ts/h tables sharing column shape with k/k1/k2 composite keys; uses
/// TIDB_INLJ hints (issue:63869) plus MustUseIndex probes, asserting NDV
/// drives which composite prefix the index join probes.
#[test]
#[ignore = "go-parity-gap: NDV-driven index choice and hint interplay need the live planner hint handling"]
fn index_choice_by_ndv_tidb_inlj() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:798
/// TestTiFlashCostModel`.
///
/// t(a,b,c) with a hacked AVAILABLE TiFlash replica on its meta; every
/// input row is a STATEMENT LIST executed via MustExec except the final
/// entry whose full query output must equal the recorded rows — so costs
/// with a tiflash candidate are pinned through plain result checks.
#[test]
#[ignore = "go-parity-gap: TiFlash replica costing exists only in the live costmodel; no tiflash surfaces here"]
fn tiflash_cost_model_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:830
/// TestIndexEqualUnknown`.
///
/// Loaded `analyzeSuiteTestIndexEqualUnknownT.json` pins plan choice when
/// the index's equal-condition NDV is unknown in the histogram bookkeeping.
#[test]
#[ignore = "go-parity-gap: depends on live estimation over partially-loaded histograms"]
fn index_equal_unknown_stats_golden() {}

/// GO PORT of `pkg/planner/core/casetest/cbotest/cbo_test.go:854
/// TestIndexJoinPreferIndexCoversMoreJoinKeyCols`.
///
/// mp(col1..col7, PK col1, idx_1(col2,col6,col7), idx_2(col3,col5,col6,col4))
/// joined against ab whose loaded `ab.simplified.json`/`mp.simplified.json`
/// stats encode strictly reverse-correlated columns; the book pins that
/// among index-join candidates the planner prefers the one covering more
/// join-key columns even when reverse correlation makes the other tempting.
#[test]
#[ignore = "go-parity-gap: index-join candidate ranking runs inside the live cost loop; not ported"]
fn index_join_prefers_index_covering_more_join_key_cols() {}
