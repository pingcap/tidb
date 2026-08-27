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

//! Batch b077 ports of `pkg/expression.part12` (`func Test*` / `func
//! Benchmark*` items 661–664 on `origin/master`, sorted by file path then
//! line). All four functions live in `pkg/expression/util_test.go`:
//!
//! | Go function (`util_test.go`) | Rust counterpart |
//! | --- | --- |
//! | `TestSQLDigestTextRetriever` (:421) | [`sql_digest_text_retriever_mock_query_partitioning`] (ignored gap) |
//! | `TestProjectionBenefitsFromPushedDown` (:483) | pre-existing
//!   `expr_util::tests::go_projection_benefits_from_pushed_down`, verified
//!   row-by-row against master in this batch |
//! | `BenchmarkExtractColumns` (:520) | skipped: Go `testing.B` microbenchmark,
//!   excluded by this batch's gate; its five-condition fixture is pinned as an
//!   assertion by the pre-existing
//!   `simple_expr::tests::compose_cnf_condition_and_extract_columns` |
//! | `BenchmarkExprFromSchema` (:540) | skipped: same benchmark reason; the
//!   exercised switch (`schema.go:134` port at
//!   `expr_util/normal_form.rs:115`) is pinned by the pre-existing
//!   `expr_util::tests::expr_from_schema_covers_constants_and_correlated_columns` |
//!
//! Each mapping re-derives its intent from the Go source it exercises, not
//! from comments in existing Rust code.

/// GO PORT of `pkg/expression/util_test.go:421 TestSQLDigestTextRetriever`.
///
/// The test exercises only the MOCK half of `SQLDigestTextRetriever`
/// (`util.go:1883`–`:2049`): with `mockLocalData` / `mockGlobalData` set,
/// `runFetchDigestQuery` short-circuits to `runMockQuery` (`util.go:1922`)
/// and never touches the `expropt.SQLExecutor` argument — which is why the
/// Go test passes `nil` for it — so the asserted contract is purely about
/// map merging:
///
/// 1. `RetrieveLocal(ctx, nil)` over `{digest1..digest5}` all empty fills
///    only the digests present in `mockLocalData`: digest1→text1,
///    digest2→text2; digest3/4/5 stay ""; entries of the mock that were
///    never requested (digest6) never appear.
/// 2. `RetrieveGlobal(ctx, nil)` runs `RetrieveLocal` first, then queries
///    only the still-empty digests through `mockGlobalData`: digest3→text3,
///    digest4→text4 are added; digest5 stays "" and the unrequested digest7
///    stays absent.
/// 3. With `fetchAllLimit = 1` both calls produce the SAME results: the
///    "too many digests, fetch all" branch passes empty `inValues`, and
///    `runMockQuery` returns its whole data table unchanged (`util.go:1906`),
///    after which `updateDigestInfo` (`util.go:1956`) still only overwrites
///    previously-empty entries.
#[test]
#[ignore = "go-parity-gap: SQLDigestTextRetriever is deliberately unported in tidb-expr -- it is a SQL client over expropt::SQLExecutor against information_schema.(cluster_)statements_summary[_history], documented 'Not ported' in expr_util/mod.rs as belonging to the expropt unit, not an expression utility"]
fn sql_digest_text_retriever_mock_query_partitioning() {
    // Restore the four assertions above once the retriever lands:
    // retrieve_local fills requested digests from mock-local data and leaves
    // unknown entries "", retrieve_global layers mock-global data on top,
    // and fetchAllLimit=1 cannot change either outcome because runMockQuery
    // with empty inValues returns the full table for updateDigestInfo to
    // merge into the previously-empty slots.
}
