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

//! Self-contained pieces of `pkg/meta/model/index.go`: the distance-metric /
//! full-text-parser / columnar-index-type enums, the global-index-v1 flag,
//! and `IndexColumn`.
//!
//! DEFERRED: the `IndexInfo` struct and its methods, most of which take the
//! unported `TableInfo` or parse condition/vector expressions via the parser
//! AST (`ExprNode`/`SelectStmt`), plus `VectorIndexInfo`/`InvertedIndexInfo`
//! and the `FieldTypeToInvertedIndexInfo` mapping.

use std::sync::atomic::{AtomicBool, Ordering};

use tidb_ast::CiString;

/// Distance-metric values for a vector index (Go `DistanceMetric`, a string).
pub mod distance_metric {
    /// L2 (Euclidean) distance.
    pub const L2: &str = "L2";
    /// Cosine distance.
    pub const COSINE: &str = "COSINE";
    /// Inner-product distance.
    pub const INNER_PRODUCT: &str = "INNER_PRODUCT";
}

/// Go `changingIndexPrefix`: prefixes the temporary name of an index being
/// modified (mirrors the column changing prefix).
pub const CHANGING_INDEX_PREFIX: &str = "_Idx$_";

/// Go `GlobalIndexVersionLegacy`.
pub const GLOBAL_INDEX_VERSION_LEGACY: u8 = 0;
/// Go `GlobalIndexVersionV1`.
pub const GLOBAL_INDEX_VERSION_V1: u8 = 1;
/// Go `GlobalIndexVersionV2`.
pub const GLOBAL_INDEX_VERSION_V2: u8 = 2;

static GLOBAL_INDEX_V1_SUPPORTED: AtomicBool = AtomicBool::new(false);

/// Go `SetGlobalIndexV1Supported`.
pub fn set_global_index_v1_supported(supported: bool) {
    GLOBAL_INDEX_V1_SUPPORTED.store(supported, Ordering::SeqCst);
}

/// Go `GetGlobalIndexV1Supported`.
#[must_use]
pub fn get_global_index_v1_supported() -> bool {
    GLOBAL_INDEX_V1_SUPPORTED.load(Ordering::SeqCst)
}

/// Full-text-parser-type values (Go `FullTextParserType`, a string).
pub mod full_text_parser_type {
    /// An invalid/unknown parser type.
    pub const INVALID: &str = "INVALID";
    /// The standard v1 parser.
    pub const STANDARD_V1: &str = "STANDARD_V1";
    /// The multilingual v1 parser.
    pub const MULTILINGUAL_V1: &str = "MULTILINGUAL_V1";
}

/// Go `FullTextParserType.SQLName`: the SQL keyword for a parser type.
#[must_use]
pub fn full_text_parser_sql_name(parser_type: &str) -> &'static str {
    match parser_type {
        full_text_parser_type::STANDARD_V1 => "STANDARD",
        full_text_parser_type::MULTILINGUAL_V1 => "MULTILINGUAL",
        _ => "INVALID",
    }
}

/// Go `GetFullTextParserTypeBySQLName`: the parser type for a SQL keyword
/// (case-insensitive; unknown -> `INVALID`).
#[must_use]
pub fn get_full_text_parser_type_by_sql_name(name: &str) -> &'static str {
    match name.to_uppercase().as_str() {
        "STANDARD" => full_text_parser_type::STANDARD_V1,
        "MULTILINGUAL" => full_text_parser_type::MULTILINGUAL_V1,
        _ => full_text_parser_type::INVALID,
    }
}

/// Go `ColumnarIndexType` (a `uint8`): the kind of columnar index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ColumnarIndexType(pub u8);

impl ColumnarIndexType {
    /// Not a columnar index (Go `ColumnarIndexTypeNA`, the zero value).
    pub const NA: ColumnarIndexType = ColumnarIndexType(0);
    /// An inverted index (Go `ColumnarIndexTypeInverted`).
    pub const INVERTED: ColumnarIndexType = ColumnarIndexType(1);
    /// A vector index (Go `ColumnarIndexTypeVector`).
    pub const VECTOR: ColumnarIndexType = ColumnarIndexType(2);
    /// A full-text index (Go `ColumnarIndexTypeFulltext`).
    pub const FULLTEXT: ColumnarIndexType = ColumnarIndexType(3);

    /// Go `ColumnarIndexType.SQLName`.
    #[must_use]
    pub fn sql_name(self) -> &'static str {
        match self {
            ColumnarIndexType::VECTOR => "vector index",
            ColumnarIndexType::INVERTED => "inverted index",
            ColumnarIndexType::FULLTEXT => "fulltext index",
            _ => "columnar index",
        }
    }
}

/// Go `RegionSplitPolicy`: a table's region-split policy (defined in
/// `index.go`, referenced by `TableInfo.TableSplitPolicy`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegionSplitPolicy {
    /// The lower-bound split points.
    pub lower: Vec<String>,
    /// The upper-bound split points.
    pub upper: Vec<String>,
    /// The number of regions.
    pub regions: i64,
}

/// Go `IndexColumn`: one column referenced by an index.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexColumn {
    /// The index column name.
    pub name: CiString,
    /// The column's offset in `TableInfo.Columns`.
    pub offset: i32,
    /// The prefix length (`UnspecifiedLength` when not a prefix index).
    pub length: i32,
    /// Whether the column uses the changing type.
    pub use_changing_type: bool,
}

/// Go `FindIndexColumnByName`: the position and column matching `name_l`
/// (already lower-cased), or `None`.
#[must_use]
pub fn find_index_column_by_name<'a>(
    index_cols: &'a [IndexColumn],
    name_l: &str,
) -> Option<(usize, &'a IndexColumn)> {
    index_cols
        .iter()
        .enumerate()
        .find(|(_, ic)| ic.name.lowercase() == name_l)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_index_v1_flag() {
        let prev = get_global_index_v1_supported();
        set_global_index_v1_supported(true);
        assert!(get_global_index_v1_supported());
        set_global_index_v1_supported(false);
        assert!(!get_global_index_v1_supported());
        set_global_index_v1_supported(prev);
    }

    #[test]
    fn full_text_parser_names() {
        assert_eq!(
            full_text_parser_sql_name(full_text_parser_type::STANDARD_V1),
            "STANDARD"
        );
        assert_eq!(
            full_text_parser_sql_name(full_text_parser_type::MULTILINGUAL_V1),
            "MULTILINGUAL"
        );
        assert_eq!(full_text_parser_sql_name("bogus"), "INVALID");

        // Reverse, case-insensitive.
        assert_eq!(
            get_full_text_parser_type_by_sql_name("standard"),
            full_text_parser_type::STANDARD_V1
        );
        assert_eq!(
            get_full_text_parser_type_by_sql_name("MULTILINGUAL"),
            full_text_parser_type::MULTILINGUAL_V1
        );
        assert_eq!(
            get_full_text_parser_type_by_sql_name("x"),
            full_text_parser_type::INVALID
        );
    }

    #[test]
    fn columnar_index_sql_name() {
        assert_eq!(ColumnarIndexType::VECTOR.sql_name(), "vector index");
        assert_eq!(ColumnarIndexType::INVERTED.sql_name(), "inverted index");
        assert_eq!(ColumnarIndexType::FULLTEXT.sql_name(), "fulltext index");
        assert_eq!(ColumnarIndexType::NA.sql_name(), "columnar index");
        assert_eq!(ColumnarIndexType::default(), ColumnarIndexType::NA);
    }

    #[test]
    fn find_index_column() {
        let cols = vec![
            IndexColumn {
                name: CiString::new("Foo"),
                ..Default::default()
            },
            IndexColumn {
                name: CiString::new("Bar"),
                ..Default::default()
            },
        ];
        let (i, ic) = find_index_column_by_name(&cols, "bar").unwrap();
        assert_eq!(i, 1);
        assert_eq!(ic.name.original(), "Bar");
        assert!(find_index_column_by_name(&cols, "baz").is_none());
    }
}
