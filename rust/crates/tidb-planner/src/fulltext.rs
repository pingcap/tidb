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

//! Full-text pushdown admission predicates from Go's
//! `pkg/planner/core/expression_rewriter.go`.
//!
//! The TiDB protobuf shape currently carries no full-text search modifier, so
//! only natural-language mode without query expansion can use the native
//! TiFlash builtin. Index admission likewise requires a public full-text
//! index on every referenced column; keeping these predicates here makes the
//! safety decision reusable by the planner's MATCH...AGAINST rewrite.

use tidb_ast::MatchModifier;
use tidb_model::TableInfo;

/// Go `ftsModifierAllowsNativePushdown` (`expression_rewriter.go:2559`).
///
/// Non-default modifiers are refused because the pushdown protocol does not
/// serialize their semantics; allowing them would silently execute a
/// boolean/query-expansion search as natural language.
#[must_use]
pub const fn fts_modifier_allows_native_pushdown(modifier: MatchModifier) -> bool {
    !modifier.is_boolean_mode() && !modifier.with_query_expansion()
}

/// Go `tableHasPublicFTSIndexOnColumn` (`expression_rewriter.go:2567`).
///
/// TiDB's native full-text index is single-column, so this predicate is
/// intentionally evaluated once for each column in `MATCH(...)`.
#[must_use]
pub fn table_has_public_fts_index_on_column(table: &TableInfo, column_name_l: &str) -> bool {
    table.indices.iter_deref().any(|index| {
        let index = index.read();
        index.full_text_info.is_some()
            && index.is_public()
            && index.find_column_by_name(column_name_l).is_some()
    })
}
