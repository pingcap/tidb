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

//! Ports of Go `pkg/planner/funcdep/extract_fd_test.go`.
//!
//! All three tests in that file are END-TO-END planner tests: each spins up a
//! mock store (`testkit.CreateMockStore`), creates tables, parses a SQL
//! statement, runs `plannercore.Preprocess` + plan building +
//! `LogicalOptimizeTest`, then calls `LogicalPlan.ExtractFD()` and compares
//! `FDToString(p)` against a golden FD string. Pinning those goldens here
//! would need the whole pipeline (session/catalog, logical plan construction,
//! optimization, per-operator `ExtractFD`); on the Rust side
//! `tidb_planner::logical`'s `extract_fd` is still an explicit stub. They are
//! therefore recorded as ignored go-parity gaps rather than approximated:
//! hand-constructing FdSet inputs to "reproduce" the golden strings would
//! assert our derivation, not Go's pipeline.

/// Go `TestFDSet_ExtractFD`
/// (`pkg/planner/funcdep/extract_fd_test.go`): 27 table-driven cases over
/// t1/x1/x2/x3 covering projection-extended columns, aggregation FDs, unique
/// keys with/without NOT NULL, equivalence derived from `WHERE c = d`, scalar
/// subqueries, and one expected error case
/// ("contains nonaggregated column 'test.x3.a'").
#[test]
#[ignore = "go-parity-gap: needs plan-build + LogicalOptimize + per-operator ExtractFD pipeline; tidb-planner's extract_fd is still a stub"]
fn fd_set_extract_fd() {
    // Golden cases live verbatim in Go extract_fd_test.go; see module doc.
}

/// Go `TestFDSet_ExtractFDForApplyAndUnion`
/// (`pkg/planner/funcdep/extract_fd_test.go`): semi-join/Apply FD derivation
/// for `EXISTS` subqueries (outer-side FDs survive because semi join keeps
/// all/part outer rows) plus `UNION ALL` FDs built from
/// `FindCommonEquivClasses`.
#[test]
#[ignore = "go-parity-gap: needs plan-build + LogicalOptimize + Apply/UnionAll ExtractFD; not present outside tidb-planner stubs"]
fn fd_set_extract_fd_for_apply_and_union() {
    // Golden cases live verbatim in Go extract_fd_test.go; see module doc.
}

/// Go `TestFDSet_MakeOuterJoin` (`pkg/planner/funcdep/extract_fd_test.go`):
/// `X LEFT OUTER JOIN (SELECT *, p+q FROM Y) ON true` — after outer-join
/// elimination the join operator's FD set is
/// `{(1)-->(2-5), (7,8)-->(9,10,13), (9,10)-->(13), (1,7,8)-->(2-5,9,10,13)}`.
///
/// The graph-level algorithm itself is ported as
/// [`crate::FdSet::make_outer_join`] and exercised by the planner when it
/// lands; this test pins the full SQL-to-FD pipeline, which does not exist yet.
#[test]
#[ignore = "go-parity-gap: golden depends on datasource-FD extraction + outer-join elimination invoking FdSet::make_outer_join within an optimized plan"]
fn fd_set_make_outer_join() {
    // Golden case lives verbatim in Go extract_fd_test.go; see module doc.
}
