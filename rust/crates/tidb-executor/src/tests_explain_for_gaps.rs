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

//! `pkg/executor/explain_unit_test.go` and `pkg/executor/explainfor_test.go`
//! surfaces this tier cannot pin. Both Go files drive full SQL sessions
//! (process registries, plan caches, RU metrics) this rewrite does not carry;
//! each ignored test names its Go test and the missing seam. Go itself skips
//! `TestExplainForConnPlanCache` (`t.Skip("unstable")`,
//! pkg/executor/explainfor_test.go:259), so it has no Rust counterpart.

/// Go `pkg/executor/explain_unit_test.go:50::TestInsertRowsColMultiplyRUV2Metrics`.
// go-parity-gap: `InsertValues.rowsColMultiply` and
// `recordRowsColMultiply2RUV2Metrics` (`pkg/executor/insert.go`) feed
// `execdetails.RUV2Metrics.ExecutorL5InsertRows` (rows x columns, idempotent
// per statement); this tier ports no RUV2 metrics collector.
#[test]
#[ignore]
fn insert_rows_col_multiply_records_ruv2_metrics_once() {}

/// Go `pkg/executor/explain_unit_test.go:135::TestExplainAnalyzeInvokeNextAndClose`
/// (including its three subtests).
// go-parity-gap: pins `ExplainExec.generateExplainInfo`'s contract that
// `Close` runs even when `Next` errors or panics and the errors JOIN
// ("next error, close error"), plus the statement RU snapshot/drain ordering
// (`staticrecordset`, `RUV2MetricsFromContext`) around
// `executeAnalyzeExec`; this tier's EXPLAIN ANALYZE runs the traced plan in
// `crate::explain` with no ExplainExec analyze seam, no panic-join, and no
// RU context plumbing.
#[test]
#[ignore]
fn explain_analyze_invokes_next_and_close_and_joins_errors() {}

/// Go `pkg/executor/explainfor_test.go:36::TestExplainFor`.
// go-parity-gap: `EXPLAIN FOR CONNECTION` across sessions requires the
// session manager (`MockSessionManager`, `ShowProcess`), permission checks
// (`ErrAccessDenied`/`ErrNoSuchThread`), live-vs-binary runtime stats
// substitution, and non-prepared/prepared plan-cache process records.
#[test]
#[ignore]
fn explain_for_connection_reports_target_session_plan() {}

/// Go `pkg/executor/explainfor_test.go:182::TestExplainForVerbose`.
// go-parity-gap: compares `EXPLAIN FORMAT = 'verbose'` columns between the
// own and a target connection (6 vs 10 columns with execution info); needs
// the cross-session process registry and verbose execution-info columns.
#[test]
#[ignore]
fn explain_for_verbose_matches_own_connection_plan_prefix() {}

/// Go `pkg/executor/explainfor_test.go:230::TestIssue11124`.
// go-parity-gap: pins that `EXPLAIN FORMAT = 'brief' FOR CONNECTION` shows
// the same CASE-pruned plan as the live statement; requires the
// cross-session explain surface.
#[test]
#[ignore]
fn issue11124_explain_for_connection_keeps_case_projection() {}

/// Go `pkg/executor/explainfor_test.go:318::TestExplainDotForExplainPlan`.
// go-parity-gap: `EXPLAIN FORMAT = 'dot' FOR CONNECTION` must fail with
// "explain format 'dot' for connection is not supported now"
// (`pkg/executor/explainfor_test.go:335`); the dot format and the
// for-connection dispatcher are unported.
#[test]
#[ignore]
fn explain_dot_for_connection_is_unsupported() {}

/// Go `pkg/executor/explainfor_test.go:339::TestExplainDotForQuery`.
// go-parity-gap: pins the exact `digraph Projection_N {...}` DOT rendering
// for `EXPLAIN FORMAT = 'dot'` plus the for-connection refusal; the dot
// renderer is unported.
#[test]
#[ignore]
fn explain_dot_for_query_renders_digraph() {}

/// Go `pkg/executor/explainfor_test.go:360::TestPointGetUserVarPlanCache`.
// go-parity-gap: prepared plan-cache replay of a parameterized point-get
// join, verified through `EXPLAIN ... FOR CONNECTION` plans (Point_Get with
// `range:[1,1]`); requires the prepared plan cache and process registry.
#[test]
#[ignore]
fn point_get_user_var_plan_cache_keeps_merge_join_shape() {}

/// Go `pkg/executor/explainfor_test.go:411::TestExpressionIndexPreparePlanCache`.
// go-parity-gap: expression-index (`key ((a+b))`) plan-cache replays viewed
// through `EXPLAIN FOR CONNECTION` (`expression_index` access object with
// the `[123,123]` range); needs the plan cache and connection explain.
#[test]
#[ignore]
fn expression_index_prepare_plan_cache_replays_ranges() {}

/// Go `pkg/executor/explainfor_test.go:440::TestIssue28259`.
// go-parity-gap: plan-cache-sensitive index-range plans (IndexRangeScan vs
// IndexReader shapes per parameter set) read through `EXPLAIN FOR
// CONNECTION`; requires prepared plan cache plus the cross-session explain
// dispatcher.
#[test]
#[ignore]
fn issue28259_plan_cache_parameter_sets_switch_index_shapes() {}

/// Go `pkg/executor/explainfor_test.go:629::TestIssue28696`.
// go-parity-gap: same cross-session explain surface over outer-join
// parameterized plans (`pkg/executor/explainfor_test.go:629`); unported dispatcher.
#[test]
#[ignore]
fn issue28696_explain_for_connection_survives_outer_join_params() {}

/// Go `pkg/executor/explainfor_test.go:662::TestIndexMerge4PlanCache`.
// go-parity-gap: IndexMerge plans through the prepared plan cache across
// parameter sets, read via `EXPLAIN FOR CONNECTION`; needs the plan cache,
// IndexMerge explain rendering, and the process registry.
#[test]
#[ignore]
fn index_merge_plan_cache_keeps_merge_shape_across_params() {}

/// Go `pkg/executor/explainfor_test.go:818::TestSPM4PlanCache`.
// go-parity-gap: SQL plan management bindings (`SPM`) applied to cached
// plans and inspected through `EXPLAIN FOR CONNECTION` (`pkg/executor/explainfor_test.go
// :818`); the binding/SPM layer and connection explain are unported.
#[test]
#[ignore]
fn spm_bindings_rewrite_plan_cache_explain_output() {}
