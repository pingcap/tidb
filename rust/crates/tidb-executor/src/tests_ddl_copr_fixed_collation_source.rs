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

//! Port of Go `pkg/ddl/copr/copr_ctx_test.go` (part2 slice: the one master
//! test this branch's stale snapshot predates,
//! `TestCopContextConditionUsesFixedCollation`, added at line 134).
//!
//! The other three master tests of the file — `TestNewCopContextSingleIndex`
//! (line 32), `TestResolveIndicesForHandle` (line 199), and
//! `TestCollectVirtualColumnOffsetsAndTypes` (line 242) — are already ported
//! inline in [`crate::ddl_copr`]'s `tests` module
//! (`test_new_cop_context_single_index`, `test_resolve_indices_for_handle`,
//! `test_collect_virtual_column_offsets_and_types`), each verified against
//! the `origin/master` body.

/// `pkg/ddl/copr/copr_ctx_test.go::TestCopContextConditionUsesFixedCollation`
/// (line 134): `NewCopContextSingleIndex` must build the index condition
/// expression under the collation mode FIXED INTO THE EXPR CONTEXT — the
/// production caller applies `exprstatic.WithNewCollationEnabled(false)` —
/// never under the process-wide `collate.NewCollationEnabled()` setting,
/// which the test switches ON for the duration. The observable is every
/// `expression.BuildSimpleExpr` call reporting `ctx.NewCollationEnabled() ==
/// false` (via a `BuildSimpleExpr` hook), while `GetCondition()` still
/// returns a condition for the `ConditionExprString: "1"` index.
// go-parity-gap: the assertion needs to observe which collation flag reaches
// expression building; `tidb_expr::simple_expr` has no BuildSimpleExpr
// injection point and derives collation process-wide (documented as the
// `WithUseNewCollate` boundary in `crate::ddl_copr`'s module header), so
// "fixed-collation plumbing" has no observable here yet.
#[test]
#[ignore = "go-parity-gap: no BuildSimpleExpr hook exists to observe the fixed collation flag"]
fn cop_context_condition_uses_fixed_collation() {}
