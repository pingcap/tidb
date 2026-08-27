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

//! `pkg/planner.part14` DOCUMENTED GAP ports for the cost-model-ver2
//! session suite, `pkg/planner/core/plan_cost_ver2_test.go` (13 items:
//! tests at :42, :83, :167, :211, :664, :676, :694, :746, :770, :799, :837,
//! :892 and the :134 benchmark).
//!
//! Every one of these Go tests drives `EXPLAIN` (formats `verbose`,
//! `cost_trace`, `true_card_cost`, `plan_tree`) through a testkit session
//! with analyzed tables, cost-factor session variables and (for the TiFlash
//! cases) a virtual replica. The Rust rewrite transcreated the ver2 cost
//! BODIES — `tidb_planner::plan_cost_ver2` with its golden table in
//! `src/plan_cost_ver2/golden_tests.rs` and the factor constants in
//! `tidb_planner::cost_factors` — but has no session, no optimizer loop that
//! picks plans from analyzed statistics, and no explain plumbing, so these
//! end-to-end cost observations cannot run here. They are recorded as
//! `#[ignore]` gap ports with their contracts re-derived from origin/master;
//! nothing is approximated. (The benchmark keeps Go's Benchmark name shape
//! so the batch gate filter `not test(/bench/)` skips it exactly like
//! `go test` skips Benchmarks.)

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:42
/// TestCostModelVer2ScanRowSize`.
///
/// go-parity-gap: needs `explain analyze format=true_card_cost` over a
/// session. Go pins ten scan row-size formulas: an index scan's formula
/// equals that index's row size (`logrowsize(32)` for `idx_ab`,
/// `logrowsize(48)` for `idx_abc`) regardless of the projected columns,
/// while a table scan always costs `logrowsize(80)` plus the
/// `1000*logrowsize(80)` point/range second term; and the `plan_tree`
/// explain prefers the smallest-row-size index automatically.
#[test]
#[ignore = "go-parity-gap: needs EXPLAIN ANALYZE true_card_cost sessions; formula printing pipeline unported"]
fn cost_model_ver2_scan_row_size_formula_pins_index_and_table_scans() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:83
/// TestCostModelTraceVer2`.
///
/// go-parity-gap: needs the `factor costs: ` warning JSON emitted under
/// `explain analyze format='true_card_cost'`. Go plans ten queries over an
/// analyzed 10-row table (full scan, range scans, index scans, order/limit,
/// group-by aggregates, cross/eq/index-lookup joins) and requires the
/// factor-cost map from the warning to sum to the plan cost within
/// `absDiff < 5` or relative 1%.
#[test]
#[ignore = "go-parity-gap: needs true_card_cost trace warnings over session-optimized plans"]
fn cost_model_trace_ver2_factor_costs_sum_to_plan_cost() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:134
/// BenchmarkGetPlanCost`.
///
/// go-parity-gap: benchmark over `core.GetPlanCost` with
/// `CostFlagRecalculate` on a session-optimized five-way-join aggregate; the
/// Rust side has no session optimizer to produce the plan.
#[test]
#[ignore = "go-parity-gap: benchmark over GetPlanCost(CostFlagRecalculate) on a session-optimized plan"]
fn benchmark_get_plan_cost() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:167
/// TestTableScanCostWithForce`.
///
/// go-parity-gap: needs `explain format=verbose` costs over an analyzed
/// table. Go pins that a `FORCE INDEX(PRIMARY)` full scan is MORE expensive
/// than the unforced plan (the force penalty), while for a RANGE scan
/// (`where a > 1`) forced and unforced costs are EQUAL (the penalty does
/// not apply to range scans).
#[test]
#[ignore = "go-parity-gap: needs verbose-explain cost comparison over analyzed session tables"]
fn table_scan_cost_force_penalty_applies_only_to_full_scans() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:211
/// TestOptimizerCostFactors`.
///
/// go-parity-gap: needs `explain format=verbose` plus the whole
/// `tidb_opt_*_cost_factor` session-variable matrix. Go walks every factor —
/// table_full_scan, table_reader, table_range_scan, index_scan, index_reader,
/// index_lookup, table_rowid_scan, limit, topn, stream_agg, hash_agg, sort,
/// index_join (plus index_join_max_scan_rows_ratio 0/0.8 keeping IndexJoin),
/// merge_join, hash_join, index_merge — requiring cost to rise when the
/// factor is raised to 10 and fall when lowered to 0.1, each isolated by
/// inflating competing factors. The factor NAMES and defaults themselves are
/// transcreated in `tidb_planner::cost_factors`; the session plumbing is not.
#[test]
#[ignore = "go-parity-gap: needs the tidb_opt_*_cost_factor session matrix over verbose explain"]
fn optimizer_cost_factors_raise_and_lower_each_operator_cost() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:664
/// TestIndexLookUpRowsLimit`.
///
/// go-parity-gap: needs `explain format='cost_trace'`. Go pins that an
/// index lookup under `limit 5 offset 100` scans only the LIMIT rows — the
/// scan formula is `(scan(5*logrowsize(48)*tikv_scan_factor(40.7)))*1.00` —
/// and `limit 20 offset 100` yields the 20-row formula; i.e. the cost model
/// folds limit-offset into the index scan row count.
#[test]
#[ignore = "go-parity-gap: needs cost_trace explain over a session-built index look-up plan"]
fn index_lookup_rows_limit_folds_limit_offset_into_scan_formula() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:676
/// TestMergeJoinCostWithOtherConds`.
///
/// go-parity-gap: needs `explain format='verbose'` over hinted merge joins.
/// Go pins that adding the non-index condition `t1.a>t2.a` to the join ON
/// clause strictly increases the total plan cost (residual conditions are
/// charged).
#[test]
#[ignore = "go-parity-gap: needs verbose-explain over hinted merge-join sessions"]
fn merge_join_cost_grows_with_other_conditions() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:694
/// TestTiFlashCostFactors`.
///
/// go-parity-gap: needs a virtual TiFlash replica plus
/// `tidb_allow_tiflash_cop` and the `tidb_opt_table_tiflash_scan_cost_factor`
/// session variable / `SET_VAR` hint. Go pins the raise-to-10 /
/// lower-to-0.1 cost monotonicity for TiFlash scans under
/// `READ_FROM_STORAGE(TIFLASH)`, and that `SET_VAR(tidb_opt_table_tiflash_scan_cost_factor=2)`
/// raises the cost above the factor-1 baseline.
#[test]
#[ignore = "go-parity-gap: needs TiFlash replica + tiflash cost-factor session vars + SET_VAR hint plumbing"]
fn tiflash_cost_factors_and_set_var_hint_raise_scan_cost() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:746
/// TestTrueCardCost`.
///
/// go-parity-gap: needs `explain analyze format=verbose` vs
/// `format=true_card_cost` over executed statements. Go pins that the
/// true-card cost format CHANGES the printed plan cost (activating
/// execution-info-based costing) for four query shapes: full scan, range
/// scan, range+limit, and an indexed group-by aggregate with order/limit.
#[test]
#[ignore = "go-parity-gap: needs EXPLAIN ANALYZE with both cost formats over executed plans"]
fn true_card_cost_format_changes_verbose_plan_cost() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:770
/// TestIssue36243`.
///
/// go-parity-gap: needs the expr-pushdown blacklist + `admin reload` pipeline
/// and verbose explain. With `>` blacklisted for tikv, the plan becomes
/// Selection(TableReader(TableScan)) and Go pins the ver2 ordering
/// `Selection cost > TableReader cost`.
#[test]
#[ignore = "go-parity-gap: needs expr_pushdown_blacklist reload + verbose-explain costs over the 3-node plan"]
fn issue_36243_selection_cost_exceeds_table_reader_cost() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:799
/// TestScanOnSmallTable`.
///
/// go-parity-gap: needs a virtual TiFlash replica over an analyzed 5-row
/// table and `explain` output. Go pins that the optimizer still chooses a
/// TiKV scan (`task` contains `tikv`) for the small table even though a
/// TiFlash replica is available — the small-table scan adjustment keeps
/// TiKV cheaper.
#[test]
#[ignore = "go-parity-gap: needs TiFlash replica metadata + plan-choice explain over an analyzed session table"]
fn scan_on_small_table_still_uses_tikv_despite_tiflash_replica() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:837
/// TestHashAggMemCostNotDividedByConcurrency`.
///
/// go-parity-gap: needs verbose-explain costs over an analyzed 1000-row
/// high-NDV table with a wide-row index plan. Go forces STREAM_AGG vs
/// HASH_AGG over `group by b` on an indexed table and requires the
/// StreamAgg plan to be CHEAPER — proving HashAgg's hash-table memory cost
/// is NOT divided by the executor concurrency (else parallel hashing would
/// always win).
#[test]
#[ignore = "go-parity-gap: needs verbose-explain costs over hinted aggregate plans on analyzed tables"]
fn hash_agg_mem_cost_not_divided_by_concurrency() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cost_ver2_test.go:892
/// TestHashAggMemCostGatedOnFreeOrdering`.
///
/// go-parity-gap: needs `explain format='cost_trace'` traces. Go pins the
/// GATING of HashAgg's memory penalty on the child providing free ordering
/// on the group-by keys: over an ordered index scan the HashAgg trace
/// matches `hashmem(<n>*<n>*<n>*tidb_mem_factor` — THREE numeric tokens
/// (concurrency folded in) — while over a hash-join output the penalty is
/// ABSENT (HashJoin's own term has only two tokens).
#[test]
#[ignore = "go-parity-gap: needs cost_trace explain of HashAgg memory terms over session plans"]
fn hash_agg_mem_cost_gated_on_free_ordering_in_cost_trace() {}
