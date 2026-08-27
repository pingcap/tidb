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

//! Documentary gap ports for the `pkg/planner.part6` slice of
//! `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go`
//! (items 356–360 on `origin/master`: the first five test functions; family
//! bootstrap is `physicalplantest/main_test.go:29 TestMain`, skipped-reason in
//! the receipt).
//!
//! All five drive `planner.Optimize` against
//! `infoschema.MockInfoSchema([coretestsdk.MockSignedTable(),
//! coretestsdk.MockUnsignedTable()])` and compare either `core.ToString`
//! best-plan strings or stmt-context warnings against the `plan_suite` book.
//! The crate states plainly (`tidb-planner` lib.rs) that no optimizer driver
//! exists here — there is nothing to run these through yet.

/// GO PORT of `pkg/planner/core/casetest/physicalplantest/
/// physical_plan_test.go:64 TestRefine`.
///
/// Re-derived contract: every plan_suite input parses as one statement and,
/// after optimize on the signed/unsigned mock tables with truncate errors NOT
/// ignored, prints exactly its recorded Best string — pinning how index range
/// refinement shapes the final access path (`RangeRefine` inside
/// find_best_task's deriveStatsExpectTimes loop, e.g. point keys collapsing
/// IndexFullScan to IndexPointGet-style single ranges).
#[test]
#[ignore = "go-parity-gap: needs planner.Optimize over MockInfoSchema plus core.ToString of whole plans -- tidb-planner has neither driver nor printer"]
fn refine_index_range_shapes_match_plan_suite_golden() {}

/// GO PORT of `pkg/planner/core/casetest/physicalplantest/
/// physical_plan_test.go:95 TestAggEliminator`.
///
/// Re-derived contract: with `tidb_opt_limit_push_down_threshold=0` and strict
/// sql_mode (disabling ONLY_FULL_GROUP_BY), aggregation elimination turns
/// distinct/max-one-row aggregates over unique columns into plain projections;
/// plan_suite Best strings pin each output tree.
#[test]
#[ignore = "go-parity-gap: agg-elimination rule application happens in the unported logical-optimize driver"]
fn agg_eliminator_removes_redundant_first_row_aggregates() {}

/// GO PORT of `pkg/planner/core/casetest/physicalplantest/
/// physical_plan_test.go:128 TestRuleColumnPruningLogicalApply`, issue 45822.
///
/// Re-derived contract: under fix-control `45822:ON`, column pruning across a
/// LogicalApply must NOT strip referenced correlated outer schema columns;
/// each case asserts BOTH the ToString best plan AND the full plan_tree EXPLAIN
/// rows from the session executor path.
#[test]
#[ignore = "go-parity-gap: dual assertions need the optimize driver plus executor-side explain rows"]
fn column_pruning_logical_apply_fix_control_45822_keeps_outer_columns() {}

/// GO PORT of `pkg/planner/core/casetest/physicalplantest/
/// physical_plan_test.go:186 TestSemiJoinToInner`.
///
/// Re-derived contract: semi joins whose inner side provably yields at most one
/// matching row (unique-key equalities in the inner conditions) rewrite to
/// inner joins during optimization; plan_suite Best pins every converted shape.
#[test]
#[ignore = "go-parity-gap: semi-to-inner rewrite rule belongs to the unported rule pipeline"]
fn semi_join_to_inner_when_inner_side_is_unique() {}

/// GO PORT of `pkg/planner/core/casetest/physicalplantest/
/// physical_plan_test.go:215 TestUnmatchedTableInHint`.
///
/// Re-derived contract: each case optimizes WITHOUT error; if the recorded
/// Warning field is empty the statement must produce ZERO stmt-context
/// warnings, otherwise exactly ONE warning at level Warning whose error text
/// equals the record — the two known unmatched-hint sources being an unknown
/// table name and a table outside the current query block.
#[test]
#[ignore = "go-parity-gap: hint-resolution warning emission needs the optimize pipeline plus SessionVars.StmtCtx warning capture"]
fn unmatched_table_hints_warn_exactly_once_or_not_at_all() {}
