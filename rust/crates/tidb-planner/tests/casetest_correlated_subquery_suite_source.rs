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

//! Documentary gap ports for `pkg/planner/core/casetest/correlated`
//! (`pkg/planner.part3` items 158–161 on `origin/master`).
//!
//! All three tests execute correlated-subquery SQL against a live TiDB and
//! pin either exact result rows or `correlated_subquery_suite` plan+result
//! goldens under both planner modes (`RunTestUnderCascades`). The Rust
//! planner crate has no executor to run SQL on, so these are documented
//! gaps. Bootstrap (`main_test.go:29 TestMain`) loads only the suite;
//! skipped-reason in the receipt.

/// GO PORT of `pkg/planner/core/casetest/correlated/correlated_test.go:28
/// TestCorrelatedSubquery`.
///
/// Re-derived contract: two identical hash-partitioned clustered tables
/// tlc07c2a51/tc4cf4a6b (json/varbinary/bit/bigint columns) get fixed data;
/// `SELECT 1 FROM tlc07c2a51 WHERE NOT (col_1 >= (SELECT GROUP_CONCAT(...)
/// ... HAVING col_6>1951988))` must return ZERO rows (scalar subquery with
/// HAVING per row), while the `any (...) ... group by col_6 HAVING col_6>0`
/// variant must return exactly ten rows of `1`.
#[test]
#[ignore = "go-parity-gap: correlated scalar/ANY subquery execution requires the unported session+executor; grouping and PARTITION BY HASH handling likewise"]
fn correlated_subquery_group_concat_having_result() {
    // Restore: build the two partitioned tables, run the NOT(...) query
    // expecting empty result and the any(...) variant expecting ten 1s.
}

/// GO PORT of `pkg/planner/core/casetest/correlated/correlated_test.go:79
/// TestNaturalJoinWithCorrelatedSubquery`.
///
/// Re-derived contract: `t(a int)` with duplicated values and a NULL —
/// deliberately kept to pin multiplicity and NULL semantics of the
/// correlated EXISTS predicate — drives each `correlated_subquery_suite`
/// input through both an explain-plan golden AND an execution-result
/// golden; selected inputs are also expected to fail via
/// `QueryToErr`, after which `StmtCtx.AlternativeLogicalPlanDecorrelatedApply`
/// and `AlternativeLogicalPlanSameOrderIndexJoin` must BOTH be true (the
/// alternative-plan fallback flags, asserted at correlated_test.go:162-167).
#[test]
#[ignore = "go-parity-gap: needs live SQL execution plus the stmtCtx alternative-decorrelate flag plumbing; neither exists here"]
fn natural_join_with_correlated_exists_multiplicity() {
    // Restore: run every suite input twice -- once for the plan_tree golden,
    // once for results -- and assert the two StmtCtx flags after expected
    // errors.
}

/// GO PORT of `pkg/planner/core/casetest/correlated/correlated_test.go:170
/// TestWrongDecorrelate`.
///
/// Re-derived contract (regression): with t1(amount decimal(65,20),
/// segment1 varchar(50)) holding three specific rows, the select-list
/// subquery `(SELECT IF(substr(dd.segment1,1,3)='600','X','') FROM dual
/// WHERE dd.amount<>0)` over ordered output must print exactly:
/// "<nil> 0.00000000000000000000 60021022342",
/// "' ' 30025... 60121022342" (empty string c1), and
/// "X 6.23... 60021022342" — i.e. decorrelation must keep per-row
/// WHERE-side NULL behaviour instead of hoisting it wrong.
#[test]
#[ignore = "go-parity-gap: decimal(65,20) formatting + FROM-dual correlated execution are part of the unported executor"]
fn wrong_decorrelate_select_list_case() {
    // Restore: insert the three rows, run the SELECT with ORDER BY 1,2,3,
    // compare testkit.Rows exactly.
}
