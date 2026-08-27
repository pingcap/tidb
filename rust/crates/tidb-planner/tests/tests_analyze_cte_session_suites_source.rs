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

//! Documentary gap ports for `pkg/planner/core/tests/{analyze,cte}` session
//! suites (`pkg/planner.part15` items 872–876 on `origin/master`).
//!
//! Both families bootstrap through `main_test.go::TestMain` (analyze :25,
//! cte :25) which registers testkit books / zeroes flaky configuration; those
//! bootstrap-only harnesses have no behavior to port (skipped-reason in the
//! batch receipt).
//!
//! | Go function | Rust test |
//! | --- | --- |
//! | `tests/analyze/analyze_test.go:28 TestAnalyzeVirtualColumns` | [`analyze_virtual_columns_all_columns_succeeds`] |
//! | `tests/analyze/analyze_test.go:45 TestAutoAnalyzeForMissingPartition` | [`auto_analyze_missing_partition_fills_skipped_stats`] |
//! | `tests/analyze/main_test.go:25 TestMain` | — skipped-reason |
//! | `tests/cte/cte_test.go:23 TestCTEWithDifferentSchema` | [`cte_with_different_schema_view_plans_to_cte_full_scan`] |
//! | `tests/cte/main_test.go:25 TestMain` | — skipped-reason |

/// GO PORT of `pkg/planner/core/tests/analyze/analyze_test.go:28
/// TestAnalyzeVirtualColumns`.
///
/// Re-derived contract: table `t1` mixes real columns with VIRTUAL generated
/// columns built from json_extract (:33-38) and `vec_l2_distance` over a
/// `vector(3)` column (:39); `ANALYZE TABLE t1 ALL COLUMNS` must succeed
/// without treating virtual/generated or vector columns as problematic stats
/// targets (:41).
#[test]
#[ignore = "go-parity-gap: ANALYZE execution lives far outside tidb-planner's ported surface"]
fn analyze_virtual_columns_all_columns_succeeds() {}

/// GO PORT of `pkg/planner/core/tests/analyze/analyze_test.go:45
/// TestAutoAnalyzeForMissingPartition`.
///
/// Re-derived contract with `tidb_skip_missing_partition_stats=1`,
/// dynamic pruning and `AutoAnalyzeMinCnt=0` (:51-60): a range-partitioned
/// table gets ONLY p1 analyzed while p0/p2 stay unanalyzed; then
/// `StatsHandle.HandleAutoAnalyze()` must run auto-analyze WITHOUT missing
/// partition stats errors, filling pseudo/absent partitions' statistics as
/// observed via `GetPhysicalTableStats(...).Pseudo` flags afterwards
/// (:61-103+). Pins that skipped-partition stats never block auto-analyze.
#[test]
#[ignore = "go-parity-gap: needs domain StatsHandle/auto-analyze worker absent from this crate"]
fn auto_analyze_missing_partition_fills_skipped_stats() {}

/// GO PORT of `pkg/planner/core/tests/cte/cte_test.go:23
/// TestCTEWithDifferentSchema`.
///
/// Re-derived contract: users `db_a`/`db_b`, definer-rights view
/// `db_a.view_test_v1` containing `WITH rs1 AS (SELECT otn.* FROM
/// tmp_table1)`; EXPLAINing it FROM db_b yields the four-row
/// plan_tree golden "CTEFullScan root CTE:rs1 AS ojt data:CTE_0 /
/// CTE_0 root Non-Recursive CTE / └─TableReader(Seed Part)…
/// data:TableFullScan / └─TableFullScan cop[tikv] …stats:pseudo"
/// (:49-54) — a view executed under another user's schema still resolves the
/// non-recursive CTE to seed TableReader + CTEFullScan.
#[test]
#[ignore = "go-parity-gap: definer-rights views + CTE planning/explain rendering unported"]
fn cte_with_different_schema_view_plans_to_cte_full_scan() {}
