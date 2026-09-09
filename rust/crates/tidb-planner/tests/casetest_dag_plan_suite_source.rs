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

//! Documentary gap ports for `pkg/planner/core/casetest/dag`
//! (`pkg/planner.part3` items 162–173 on `origin/master`).
//!
//! Every DAG test optimizes parsed SQL through `planner.Optimize` on a live
//! testkit session with `infoschema.MockInfoSchema([MockSignedTable(),
//! MockUnsignedTable()])` (dag_test.go:56, :77, ...) and compares
//! `core.ToString(plan)` (or plan-tree output) against `plan_suite`
//! goldens. The Rust workspace has neither a session to optimize over nor
//! the mock physical-table fixture factory nor `core.ToString`; these are
//! recorded gaps, not approximations. Bootstrap (`main_test.go:29
//! TestMain`) loads only `plan_suite`; skipped-reason in the receipt.

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:82
/// TestDAGPlanBuilderSimpleCase`.
///
/// Re-derived contract: shared helper `testDAGPlanBuilderSimpleCase`
/// (dag_test.go:53) sets `tidb_opt_limit_push_down_threshold=0`, starts a
/// txn per case (`sessiontxn.NewTxn`) and requires each book input's
/// `core.ToString(optimized)` golden to reproduce; enables the
/// SkipSystemTableCheck failpoint for the suite duration. This twin skips
/// itself on next-gen kernels.
#[test]
#[ignore = "go-parity-gap: planner.Optimize over a live session plus core.ToString rendering of the final plan is unported"]
fn dag_plan_builder_simple_case_classic_kernel() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:90
/// TestDAGPlanBuilderSimpleCaseForNextGen`.
///
/// The same helper under next-gen kernels only: skipped on classic kernels,
/// proving identical case coverage across kernel types.
#[test]
#[ignore = "go-parity-gap: same missing surface as dag_plan_builder_simple_case_classic_kernel (plus a kernel-type gate that has no Rust counterpart)"]
fn dag_plan_builder_simple_case_next_gen_kernel() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:98
/// TestDAGPlanBuilderJoin`.
///
/// Join planning goldens from plan_suite over MockSignedTable/MockUnsignedTable;
/// each input's `core.ToString` best-plan text must match byte-for-byte.
#[test]
#[ignore = "go-parity-gap: no live optimization pipeline or ToString renderer exists in tidb-planner"]
fn dag_plan_builder_join_golden() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:133
/// TestDAGPlanBuilderSubquery`.
///
/// Subquery lowering goldens; runs with
/// sql_mode='STRICT_TRANS_TABLES' so only-full-group-by relaxations apply.
#[test]
#[ignore = "go-parity-gap: subquery-to-join transformation happens inside the unported optimizer"]
fn dag_plan_builder_subquery_golden() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:170 TestDAGPlanTopN`.
///
/// TopN placement goldens from plan_suite via core.ToString.
#[test]
#[ignore = "go-parity-gap: TopN pushdown decisions live in the unported rule pipeline"]
fn dag_plan_top_n_golden() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:200
/// TestDAGPlanBuilderBasePhysicalPlan`.
///
/// Base physical-plan conversion goldens (reader/task conversions) pinned
/// through the same ToString loop.
#[test]
#[ignore = "go-parity-gap: task-to-plan conversion endgame needs the full findBestTask runtime"]
fn dag_plan_builder_base_physical_plan_golden() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:244
/// TestDAGPlanBuilderUnion`.
///
/// Union-all DAG shaping goldens from plan_suite.
#[test]
#[ignore = "go-parity-gap: union plan assembly is exercised through live Optimize only"]
fn dag_plan_builder_union_golden() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:274
/// TestDAGPlanBuilderUnionScan`.
///
/// UnionScan planning inside an explicit transaction: per case it does
/// `begin`, inserts `(2,2,2)`, builds+optimizes, checks the ToString golden,
/// then rolls back — pinning dirty-buffer reads appearing as UnionScan in
/// the DAG.
#[test]
#[ignore = "go-parity-gap: transaction dirty-buffer + UnionScan interplay needs a real executor session"]
fn dag_plan_builder_union_scan_over_txn_buffer() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:311
/// TestDAGPlanBuilderAgg`.
///
/// Aggregate DAG goldens; again STRICT_TRANS_TABLES to disable
/// only-full-group-by for the mixed inputs.
#[test]
#[ignore = "go-parity-gap: aggregate placement rules run in the unported logical/physical rules"]
fn dag_plan_builder_agg_golden() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:381
/// TestDAGPlanBuilderWindow`.
///
/// Window-function DAG goldens driven by shared `doTestDAGPlanBuilderWindow`
/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:348` helper
/// `doTestDAGPlanBuilderWindow`, shared by this test and its parallel twin;
/// it first applies the passed SET statements then compares ToString output
/// per case.
#[test]
#[ignore = "go-parity-gap: window operator planning lives behind the unported rule set"]
fn dag_plan_builder_window_golden() {}

/// GO PORT of `pkg/planner/core/casetest/dag/dag_test.go:395
/// TestDAGPlanBuilderWindowParallel`.
///
/// Same window harness with parallel-execution variables set; pins the
/// parallel-window (shuffle) variant of the DAG output.
#[test]
#[ignore = "go-parity-gap: parallel window shaping depends on live session vars and executor-level plan pieces"]
fn dag_plan_builder_window_parallel_golden() {}
