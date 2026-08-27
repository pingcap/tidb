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

//! Documentary gap ports for `pkg/planner/core/casetest/parallelapply`
//! (`pkg/planner.part6` items 331–333 on `origin/master`; family bootstrap is
//! `parallelapply/main_test.go:24 TestMain`, skipped-reason in the receipt).
//!
//! All three tests need a live mock store: either `testkit.CreateMockStore`
//! for DML round trips plus `EXPLAIN ANALYZE`, or `RunTestUnderCascades`
//! golden runs. The planner decisions they pin — whether the LATERAL apply
//! stays un-decorrelated, when `enableParallelApply`
//! (`pkg/planner/core/rule_generate_subquery.go`) chooses parallelism, its
//! `outerExpectedCnt` computation inside
//! `exhaustPhysicalPlans4LogicalApply`, and the ordered-apply KeepOrder
//! setting — have no Rust port; `tidb-planner/src/physical_apply.rs` carries
//! only PhysicalApply's Init/explain identity leaf.

/// GO PORT of `pkg/planner/core/casetest/parallelapply/
/// parallel_apply_test.go:36 TestLateralHierarchyParallelApply`.
///
/// Re-derived contract (three claims over one mock store):
/// 1. With `tidb_enable_parallel_apply=on` and concurrency 5, the recursive
///    CTE + LATERAL hierarchy query keeps Apply in the plan (not decorrelated).
/// 2. A flat LATERAL join's EXPLAIN ANALYZE reports `Concurrency:` > 1 on the
///    Apply operator.
/// 3. The recursive-CTE result set is identical with parallel_apply off and
///    on — the recursive body's Apply is intentionally serialized per
///    `logical_cte.go` because grandchildren would otherwise be dropped.
#[test]
#[ignore = "go-parity-gap: needs CreateMockStore session+executor for explain analyze rows and recursive-CTE execution; enableParallelApply decision logic is unported"]
fn lateral_hierarchy_keeps_apply_and_matches_serial_results() {}

/// GO PORT of `pkg/planner/core/casetest/parallelapply/
/// parallel_apply_test.go:113 TestParallelApplyWarnning`.
///
/// Re-derived contract: a scalar subquery joining t2,t3 via INL hash-join hint
/// under parallel apply emits NO warnings ("show warnings" empty), and the
/// issue 59863 shape (correlated count(*) with index join plan) plans the
/// recorded CARTESIAN Apply tree without warning output either.
#[test]
#[ignore = "go-parity-gap: RunTestUnderCascades live planning with hint-driven IndexJoin choice plus plan_tree printing and warning capture"]
fn parallel_apply_inl_hash_join_emits_no_warnings() {}

/// GO PORT of `pkg/planner/core/casetest/parallelapply/
/// parallel_apply_test.go:148 TestParallelApplyOrderedPlan`.
///
/// Re-derived contract: with parallel apply on, correlated ORDER BY queries
/// still produce Apply whose outer (Build) side scans `keep order:true` — the
/// KeepOrder branch of `enableParallelApply`; ORDER BY + LIMIT exercises the
/// `outerExpectedCnt` selectivity estimate; unordered cases keep
/// keep order:false; no "Parallel Apply rejects order properties" warning; and
/// parallel results equal serial ones.
#[test]
#[ignore = "go-parity-gap: outerExpectedCnt estimation and KeepOrder selection in exhaustPhysicalPlans4LogicalApply are unported; assertions also compare executor outputs"]
fn parallel_apply_ordered_plan_keeps_outer_order() {}
