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

//! Documentary gap ports for `pkg/planner/core/casetest/fulljoin`
//! (`pkg/planner.part4` items 189–198 on `origin/master`).
//!
//! All ten tests share one helper stack (full_join_test.go:45-119):
//! `createPlannerSuite` builds a mock store with `t(a int not null,
//! b int not null, key(a))`, `parseNode`/`buildLogicalPlanForTest`/
//! `optimizeWithPlanner` drive `core.Preprocess` + `BuildLogicalPlanForTest`
//! /`planner.Optimize`, and tree walkers find joins. The whole feature — a
//! `base.FullOuterJoin` join type, the `EnableFullOuterJoin` session switch,
//! nullable-schema relaxation, join-type simplification under WHERE, and a
//! tail-scan cost premium — is unported here (`find_best_task.rs`
//! LogicalJoinType carries no Full variant), so every item below is a
//! documented gap. The bootstrap `fulljoin/main_test.go:26 TestMain` is
//! skipped-reason: goleak/config boilerplate only.

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:161
/// TestFullOuterJoinFeatureSwitchDefaultOff`.
///
/// Re-derived contract: with `EnableFullOuterJoin` at its default OFF,
/// planning `select * from t t1 full outer join t t2 on t1.a = t2.a` must
/// fail in build with `plannererrors.ErrNotSupportedYet` whose message names
/// "FULL OUTER JOIN".
#[test]
#[ignore = "go-parity-gap: no FullOuterJoin syntax gate exists; the Rust parser->plan path never produces ErrNotSupportedYet(FULL OUTER JOIN)"]
fn full_outer_join_feature_switch_default_off_rejects() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:175
/// TestFullOuterJoinLogicalBuild`.
///
/// With the switch ON, the built logical join must carry JoinType
/// FullOuterJoin; every column of BOTH join.Schema() and join.FullSchema
/// (:101 in source — non-nil) must have lost the NotNull flag despite t's
/// NOT NULL columns; DoOptimize(FlagPredicatePushDown) keeps a physical full
/// outer join whose ExplainInfo mentions "left cond:" and "right cond:" and
/// whose plan string contains no Selection.
#[test]
#[ignore = "go-parity-gap: no full-outer logical/physical operator, FullSchema duality or explain-info split exists"]
fn full_outer_join_logical_build_relaxes_not_null_and_keeps_both_conditions() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:209
/// TestFullOuterJoinUnsupportedFormsFailFast`.
///
/// USING (`full outer join ... using (a)`), NATURAL, and LATERAL-derived
/// (`lateral (select 1 as a) as t2 on false`) forms each fail preprocess→
/// build with `ErrNotSupportedYet` naming FULL OUTER JOIN.
#[test]
#[ignore = "go-parity-gap: same missing full-outer support surface as full_outer_join_feature_switch_default_off_rejects"]
fn full_outer_join_unsupported_forms_fail_fast() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:231
/// TestFullOuterJoinCascadesFailFast`.
///
/// Even with EnableFullOuterJoin ON, enabling the cascades planner makes the
/// same query fail with `ErrNotSupportedYet` naming FULL OUTER JOIN.
#[test]
#[ignore = "go-parity-gap: cascades-planner variant of the missing rejection path"]
fn full_outer_join_cascades_planner_fail_fast() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:249
/// TestFullOuterJoinPhysicalPlanHashJoinOnly`.
///
/// After planner.Optimize the only physical join impl is a PhysicalHashJoin
/// with JoinType FullOuterJoin and UseOuterToBuild == false.
#[test]
#[ignore = "go-parity-gap: PhysicalHashJoin has no full-outer mode nor UseOuterToBuild knob on this crate"]
fn full_outer_join_physical_plan_is_hash_join_with_build_on_outer() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:262
/// TestFullOuterJoinUnsupportedJoinMethodHintsWarn`.
///
/// `/*+ MERGE_JOIN(t1, t2) */` and `/*+ INL_JOIN(t2) */` around a full outer
/// join still yield the hash-join full-outer plan, but the statement
/// warnings must contain both the hint name and "inapplicable".
#[test]
#[ignore = "go-parity-gap: hint-applicability warning pipeline over unsupported join methods is unported"]
fn full_outer_join_unsupported_join_method_hints_warn_inapplicable() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:300
/// TestFullOuterJoinSimplifyOuterJoin`.
///
/// WHERE-clause predicates decide the simplified join type after
/// FlagPredicatePushDown only: left-side conjunct → LeftOuterJoin, right-side
/// → RightOuterJoin, both sides → InnerJoin, an OR across sides stays
/// FullOuterJoin.
#[test]
#[ignore = "go-parity-gap: outer-join simplification rules operate on operators this crate has not transcreated"]
fn full_outer_join_where_predicate_simplifies_join_type() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:334
/// TestFullOuterJoinSkipJoinReOrder`.
///
/// Two chained full outer joins planned with
/// FlagPredicatePushDown|FlagJoinReOrder must survive reordering untouched:
/// exactly two FullOuterJoin physical joins remain among the collected
/// joins.
#[test]
#[ignore = "go-parity-gap: join-reorder skip logic for full-outer chains needs the full optimizer loop"]
fn full_outer_join_chain_skips_join_reorder_keeping_two_full_joins() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:352
/// TestFullOuterJoinTailScanCostVer1`.
///
/// Cost model version 1, HASH_JOIN-hinted identical shapes: the full outer
/// hash join's GetPlanCostVer1(RootTaskType) must exceed the inner join's —
/// Go charges a tail-scan premium (reading/building the unmatched tail).
#[test]
#[ignore = "go-parity-gap: GetPlanCostVer1 and its full-outer tail-scan premium are not transcreated (cost work here is ver2-only)"]
fn full_outer_join_tail_scan_cost_ver1_exceeds_inner_join_cost() {}

/// GO PORT of `pkg/planner/core/casetest/fulljoin/full_join_test.go:374
/// TestFullOuterJoinTailScanCostVer2`.
///
/// Same inequality through GetPlanCostVer2 with CostFlagRecalculate under
/// CostModelVersion=2: full-outer total strictly above inner total.
#[test]
#[ignore = "go-parity-gap: no PhysicalHashJoin costing node applies the full-outer tail-scan multiplier in plan_cost_ver2"]
fn full_outer_join_tail_scan_cost_ver2_exceeds_inner_join_cost() {}
