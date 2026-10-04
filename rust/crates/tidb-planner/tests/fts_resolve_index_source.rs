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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

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
