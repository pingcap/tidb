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

//! Documentary gap ports for `pkg/planner/core/fts_resolve_index_test.go`
//! (`pkg/planner.part10` items 574-577 on `origin/master`) and
//! `pkg/planner/core/fulltext_to_like_test.go` items 578-579. The resolve-index
//! tests all require STARTER deployment mode (`setStarterDeployModeForFTSTest`
//! skips on classic kernels), mock TiFlash replicas and sessions; the two
//! fulltext-to-like helpers are pure planner predicates exposed by
//! [`tidb_planner::fulltext`].

/// GO PORT of `pkg/planner/core/fts_resolve_index_test.go:32
/// TestFTSRequiresStarterMode`.
///
/// Contract (:32-49): creating a table WITH a fulltext key, ALTER-adding a
/// fulltext index and evaluating `fts_match_word('hello', title)` each fail
/// with "… only supported in starter deployment mode" when not in starter
/// mode; plain DDL/DML over fts_t stays legal.
#[test]
#[ignore = "go-parity-gap: kernel/deployment-mode gates and session error surfacing are outside this crate"]
fn fts_requires_starter_mode_messages() {}

/// GO PORT of `pkg/planner/core/fts_resolve_index_test.go:50
/// TestTiFlashFTSMatchWordPushDown`.
///
/// Contract (:50-94): over fts_t with a public fulltext index and TiFlash
/// replica, suite queries' plan_tree goldens pin index-resolution for
/// FTS_MATCH_WORD; additionally EXPLAIN surfaces exact errors (:86-91) —
/// SELECT-only match needs a WHERE twin (:86), wrapped SELECT matches must
/// stay bare (:87), literal arguments must agree between SELECT and WHERE
/// (:88), one MATCH per query (:89), a matching FTS index is required (:90)
/// and ORDER BY match without LIMIT is rejected (:91).
#[test]
#[ignore = "go-parity-gap: FTS index resolution over executed plans needs TiFlash + session tier"]
fn tiflash_fts_match_word_push_down_resolves_and_rejects() {}

/// GO PORT of `pkg/planner/core/fts_resolve_index_test.go:95
/// TestTiFlashFTSMatchWordPreparedPlanCache`.
///
/// Contract (:95-114): executing the prepared constant-match statement twice
/// NEVER caches (`@@last_plan_from_cache` stays 0); preparing with a `?`
/// parameter fails with "match against a non-constant string".
#[test]
#[ignore = "go-parity-gap: prepared plan-cache interplay needs session plumbing"]
fn tiflash_fts_match_word_prepared_plan_never_caches() {}

/// GO PORT of `pkg/planner/core/fts_resolve_index_test.go:115
/// TestTiFlashFTSMatchWordDirtyTxn`.
///
/// Contract (:115-128): the pre-insert FTS query returns no rows; inside a
/// transaction carrying an uncommitted insert, the same query fails with
/// "cannot be used in a transaction with uncommitted changes"; rollback
/// restores normal behavior.
#[test]
#[ignore = "go-parity-gap: dirty-transaction detection spans executor state"]
fn tiflash_fts_match_word_dirty_txn_errors() {}

/// GO PORT of `pkg/planner/core/fulltext_to_like_test.go:25
/// TestFTSModifierAllowsNativePushdown`.
///
/// Contract (:25-52): `ftsModifierAllowsNativePushdown`
/// (`expression_rewriter.go:2553-2561`: `!modifier.IsBooleanMode() &&
/// !modifier.WithQueryExpansion()`) accepts ONLY the default natural-language
/// modifier — boolean mode and natural-language+WITH QUERY EXPANSION are both
/// refused because tipb does not serialize the modifier.
#[test]
fn fts_modifier_allows_native_pushdown_truth_table() {
    use tidb_ast::MatchModifier;
    use tidb_planner::fulltext::fts_modifier_allows_native_pushdown;

    assert!(fts_modifier_allows_native_pushdown(MatchModifier::None));
    assert!(!fts_modifier_allows_native_pushdown(
        MatchModifier::BooleanMode
    ));
    assert!(!fts_modifier_allows_native_pushdown(
        MatchModifier::QueryExpansion
    ));
}

/// GO PORT of `pkg/planner/core/fulltext_to_like_test.go:55
/// TestTableHasPublicFTSIndexOnColumn`.
///
/// Contract (:55-131): `tableHasPublicFTSIndexOnColumn`
/// (`expression_rewriter.go:2563-2572`) scans tblInfo.Indices for an index
/// that is PUBLIC and carries FullTextInfo, matching the requested lowercase
/// column via FindColumnByName — nil indices answer false; only non-FTS or
/// non-public FTS answers false; a public FTS on another column stays false;
/// one public single-column FTS makes it true.
#[test]
fn table_has_public_fts_index_on_column_truth_table() {
    use tidb_ast::CiString;
    use tidb_model::go_runtime::GoShared;
    use tidb_model::{FullTextIndexInfo, IndexColumn, IndexInfo, SchemaState, TableInfo};
    use tidb_planner::fulltext::table_has_public_fts_index_on_column;

    let fts_idx = |name: &str, column: &str, state| IndexInfo {
        name: CiString::new(name),
        state,
        columns: vec![IndexColumn {
            name: CiString::new(column),
            ..Default::default()
        }]
        .into(),
        full_text_info: Some(GoShared::new(FullTextIndexInfo::default())),
        ..Default::default()
    };
    let plain_idx = |name: &str, column: &str| IndexInfo {
        name: CiString::new(name),
        state: SchemaState::PUBLIC,
        columns: vec![IndexColumn {
            name: CiString::new(column),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };

    let no_indices = TableInfo::default();
    assert!(!table_has_public_fts_index_on_column(&no_indices, "title"));

    let plain = TableInfo {
        indices: vec![plain_idx("idx_title", "title")].into(),
        ..Default::default()
    };
    assert!(!table_has_public_fts_index_on_column(&plain, "title"));

    let non_public = TableInfo {
        indices: vec![fts_idx(
            "ft_title",
            "title",
            SchemaState::WRITE_REORGANIZATION,
        )]
        .into(),
        ..Default::default()
    };
    assert!(!table_has_public_fts_index_on_column(&non_public, "title"));

    let different_column = TableInfo {
        indices: vec![fts_idx("ft_body", "body", SchemaState::PUBLIC)].into(),
        ..Default::default()
    };
    assert!(!table_has_public_fts_index_on_column(
        &different_column,
        "title"
    ));

    let public = TableInfo {
        indices: vec![
            plain_idx("idx_id", "id"),
            fts_idx("ft_body", "body", SchemaState::PUBLIC),
            fts_idx("ft_title", "Title", SchemaState::PUBLIC),
        ]
        .into(),
        ..Default::default()
    };
    assert!(table_has_public_fts_index_on_column(&public, "title"));
}
