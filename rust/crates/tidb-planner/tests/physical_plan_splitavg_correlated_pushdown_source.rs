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

//! `pkg/planner.part14` DOCUMENTED GAP ports from
//! `pkg/planner/core/physical_plan_test.go` — the session-driven plan-shape
//! regressions. Each function below pins the INTENT of one Go test from
//! `origin/master` that this crate cannot yet exercise for real, so it is
//! `#[ignore]`d instead of approximated. (The batch's remaining
//! `physical_plan_test.go` items — memory trace, exchange-sender index
//! resolution — live in `physicalop_memory_trace_clone_stream_count_source.rs`.)
//!
//! All three Go tests drive `parse → core.Preprocess → planner.Optimize`
//! over a mock store / testkit session (`testkit.RunTestUnderCascades`); this
//! crate has neither a session stack nor `planner.Optimize`, so the
//! optimized-plan observations cannot run here.

/// GO PARITY GAP port of `pkg/planner/core/physical_plan_test.go:609
/// TestDAGPlanBuilderSplitAvg`.
///
/// go-parity-gap: needs `planner.Optimize` over a testkit session plus the
/// mock infoschema (`coretestsdk.MockSignedTable`/`MockUnsignedTable`). Go
/// pins that `select avg(a),avg(b),avg(c) from t` — with and without the
/// `HASH_AGG()` hint — plans to
/// `TableReader(Table(t)->HashAgg)->HashAgg`, and that every hash/stream agg
/// found under the reader (helper `testDAGPlanBuilderSplitAvg` :643-672)
/// has `agg.AggFuncs[i].RetTp` EQUAL to `agg.Schema().Columns[i].RetType` —
/// the avg split keeps the partial schema types aligned with the function
/// descriptors (the invariant `BuildFinalModeAggregation`'s avg arm
/// establishes, base_physical_agg.go:825-842).
#[test]
#[ignore = "go-parity-gap: needs session planner.Optimize + ToString plan rendering to reach the split aggregates through a real plan"]
fn dag_plan_builder_split_avg_keeps_agg_ret_types_aligned_with_schema() {}

/// GO PARITY GAP port of `pkg/planner/core/physical_plan_test.go:691
/// TestPhysicalTableScanExtractCorrelatedCols`.
///
/// go-parity-gap: needs TiFlash replica machinery (`SetTiFlashReplica`,
/// `UpdateTableReplicaInfo`), session `ShowProcess` plan retrieval, `ToPB`
/// over a BuildPB context, and the executor-level correlate extraction. Go
/// pins that, after manually moving the `client_no = c.company_no` equality
/// into `PhysicalTableScan.LateMaterializationFilterCondition` (the encoded
/// `EQString` scalar function), the encoded table scan carries exactly one
/// `PushedDownFilterConditions` whose first child is a `ColumnRef`, and
/// `ts.ExtractCorrelatedCols()` yields exactly ONE column printing as
/// `test.t2.company_no` — the outer query's column survives the push-down
/// as a correlated reference.
#[test]
#[ignore = "go-parity-gap: needs TiFlash replica + ShowProcess + ToPB pipeline; LateMaterializationFilterCondition/ExtractCorrelatedCols unported on this crate's scan leaves"]
fn physical_table_scan_extract_correlated_cols_after_late_materialization_pushdown() {}

/// GO PARITY GAP port of `pkg/planner/core/physical_plan_test.go:771
/// TestAvoidColumnEvaluatorForProjBelowUnion`.
///
/// go-parity-gap: needs session plan execution traces (`ShowProcess`) over
/// UNION ALL / window queries. Go pins that every `PhysicalProjection`
/// DIRECTLY below a `PhysicalUnionAll` carries
/// `AvoidColumnEvaluator == true` (so the union consumes chunks without
/// per-column evaluators), while every other projection in the tree —
/// including a projection at the root — carries `false`; exercised with a
/// distinct+window UNION ALL and a `union select` derived table.
#[test]
#[ignore = "go-parity-gap: needs session pipeline to build real union/window physical plans; AvoidColumnEvaluator flag unported"]
fn avoid_column_evaluator_flags_projs_below_union_only() {}
