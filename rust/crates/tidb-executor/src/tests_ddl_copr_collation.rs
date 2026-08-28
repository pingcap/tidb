// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, 2.0 (the "License");
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

//! Ports of all four functions in `pkg/ddl/copr/copr_ctx_test.go` from the
//! authoritative `origin/master` snapshot. The current Rust branch removed the
//! `ddl_copr` production facade, so each behavior is recorded as an explicit
//! gap rather than approximated through an unrelated API.

/// `pkg/ddl/copr/copr_ctx_test.go:32::TestNewCopContextSingleIndex`.
#[test]
#[ignore = "go-parity-gap: the Rust branch has no ddl_copr::NewCopContextSingleIndex carrier for row-id, PK-handle, or common-handle column selection"]
fn new_cop_context_single_index_selects_columns_for_each_handle_kind() {
    // Go constructs six columns and checks the selected names for row-id,
    // integer-PK-handle, and common-handle tables: c1 + _tidb_rowid; c0 + c1;
    // and c1 + c2 + c4 respectively. Source: pkg/ddl/copr/copr_ctx.go:177.
}

/// `pkg/ddl/copr/copr_ctx_test.go:134::TestCopContextConditionUsesFixedCollation`.
#[test]
#[ignore = "go-parity-gap: the Rust branch has no ddl_copr context or expression-build observation seam for the fixed-collation contract"]
fn cop_context_condition_builds_under_the_context_collation_mode() {
    // Go forces new-collation OFF, builds a virtual lower(c0) column and an
    // index condition, and asserts every BuildSimpleExpr call sees false while
    // GetCondition succeeds. Source: pkg/ddl/copr/copr_ctx.go:177-238.
}

/// `pkg/ddl/copr/copr_ctx_test.go:199::TestResolveIndicesForHandle`.
#[test]
#[ignore = "go-parity-gap: the Rust branch has no ddl_copr::resolveIndicesForHandle helper to exercise"]
fn resolve_indices_for_handle_preserves_requested_handle_order() {
    // Go maps handle IDs [2], [3,2,1], and [1,3] over columns [1,2,3] to
    // offsets [1], [2,1,0], and [0,2]. Source: pkg/ddl/copr/copr_ctx.go:384.
}

/// `pkg/ddl/copr/copr_ctx_test.go:242::TestCollectVirtualColumnOffsetsAndTypes`.
#[test]
#[ignore = "go-parity-gap: the Rust branch has no ddl_copr::collectVirtualColumnOffsetsAndTypes helper to exercise"]
fn collect_virtual_column_offsets_and_types_reports_virtual_columns() {
    // Go reports virtual-column offsets [0,2] and types [1,2] in one fixture,
    // and [1] and [1] in the other. Source: pkg/ddl/copr/copr_ctx.go:397.
}
