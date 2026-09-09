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

//! Documentary gap ports for `pkg/planner/core/casetest/scalarsubquery`
//! (`pkg/planner.part9` items 509-511 on `origin/master`).
//!
//! Both tests load `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite.json`
//! (its `main_test.go:29` TestMain registers exactly that book) and drive SQL
//! through a live cascades-mode session; the Rust side cannot execute or
//! explain SQL yet. Bootstrap TestMain is recorded as skipped-reason.

/// GO PORT of
/// `pkg/planner/core/casetest/scalarsubquery/cases_test.go:27
/// TestExplainNonEvaledSubquery`.
///
/// With `@@tidb_opt_enable_non_eval_scalar_subquery=true`, plain tables
/// t1/t2(int triple) and t3(varchar) run every plan-suite entry: entries
/// marked HasErr must FAIL when executed (`ExecToErr`, :64/:78) with their recorded
/// error string; otherwise rows are compared after trimming unstable
/// EXPLAIN ANALYZE columns (execution-info/memory/disk cut via the closure at
/// :50). Pins that non-evaled
/// scalar subqueries error during execution instead of being evaluated.
#[test]
#[ignore = "go-parity-gap: scalar-subquery execution-time errors need the executor + EXPLAIN ANALYZE renderer"]
fn explain_non_evaled_scalar_subquery_error_cases() {}

/// GO PORT of
/// `pkg/planner/core/casetest/scalarsubquery/cases_test.go:95
/// TestSubqueryInExplainAnalyze`.
///
/// Four wide tables spanning nearly every MySQL type family (t1-t4 created
/// :113-116 and seeded :121-125, incl. decimal(20,5)/json/bit/enum/set/binary)
/// so EXPLAIN ANALYZE output above scalar subqueries is exercised across type
/// conversions; suite plans compare after cutting unstable columns exactly as
/// in the sibling test.
#[test]
#[ignore = "go-parity-gap: needs live EXPLAIN ANALYZE over executed subqueries"]
fn subquery_in_explain_analyze_all_data_types() {}
