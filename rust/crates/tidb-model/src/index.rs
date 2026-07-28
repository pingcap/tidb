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

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tidb_ast::{CiString, IndexType};

use crate::column::{removing_origin_name, REMOVING_OBJ_PREFIX};
use crate::reorg::BackfillState;
use crate::schema_state::SchemaState;

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

/// Go `n == 0` for the `omitempty` check on a `uint8` field.
fn is_zero_u8(value: &u8) -> bool {
    *value == 0
}

/// Go `ast.IndexType` is an `int`, so `encoding/json` writes it as a number.
/// `IndexType` lives in `tidb-ast` without serde impls, so the numeric mapping
/// is applied per field. The discriminants must match the Go `iota` order in
/// `pkg/parser/ast/model.go` because they are persisted in `TableInfo`.
fn index_type_to_i64(tp: IndexType) -> i64 {
    match tp {
        IndexType::Invalid => 0,
        IndexType::Btree => 1,
        IndexType::Hash => 2,
        IndexType::Rtree => 3,
        IndexType::Hypo => 4,
        IndexType::Vector => 5,
        IndexType::Inverted => 6,
        IndexType::Hnsw => 7,
        IndexType::Fulltext => 8,
    }
}

/// Inverse of [`index_type_to_i64`]; an unknown number decodes as `Invalid`,
/// matching Go's unnamed `IndexType` values falling through `String()`.
fn index_type_from_i64(value: i64) -> IndexType {
    match value {
        1 => IndexType::Btree,
        2 => IndexType::Hash,
        3 => IndexType::Rtree,
        4 => IndexType::Hypo,
        5 => IndexType::Vector,
        6 => IndexType::Inverted,
        7 => IndexType::Hnsw,
        8 => IndexType::Fulltext,
        _ => IndexType::Invalid,
    }
}

fn serialize_index_type<S: Serializer>(tp: &IndexType, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_i64(index_type_to_i64(*tp))
}

fn deserialize_index_type<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<IndexType, D::Error> {
    Ok(index_type_from_i64(
        Option::<i64>::deserialize(deserializer)?.unwrap_or_default(),
    ))
}

/// Go `BackfillState` is a `byte`, so `encoding/json` writes it as a number.
fn serialize_backfill_state<S: Serializer>(
    state: &BackfillState,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_u8(state.0)
}

fn deserialize_backfill_state<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BackfillState, D::Error> {
    Ok(BackfillState(
        Option::<u8>::deserialize(deserializer)?.unwrap_or_default(),
    ))
}

/// Go `ColumnarIndexType` (a `uint8`): the kind of columnar index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
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
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionSplitPolicy {
    /// The lower-bound split points.
    #[serde(
        rename = "lower",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub lower: Vec<String>,
    /// The upper-bound split points.
    #[serde(
        rename = "upper",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub upper: Vec<String>,
    /// The number of regions.
    #[serde(rename = "regions", default)]
    pub regions: i64,
}

/// Go `IndexColumn`: one column referenced by an index.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexColumn {
    /// The index column name.
    #[serde(rename = "name", default)]
    pub name: CiString,
    /// The column's offset in `TableInfo.Columns`.
    #[serde(rename = "offset", default)]
    pub offset: i32,
    /// The prefix length (`UnspecifiedLength` when not a prefix index).
    #[serde(rename = "length", default)]
    pub length: i32,
    /// Whether the column uses the changing type.
    #[serde(
        rename = "using_changing_type",
        default,
        skip_serializing_if = "crate::serde_helpers::is_false"
    )]
    pub use_changing_type: bool,
}

/// Go `VectorIndexInfo`: a vector index's parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorIndexInfo {
    /// The vector dimension.
    #[serde(rename = "dimension", default)]
    pub dimension: u64,
    /// The distance metric (see [`distance_metric`]).
    #[serde(
        rename = "distance_metric",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub distance_metric: String,
}

/// Go `InvertedIndexInfo`: an inverted index's parameters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvertedIndexInfo {
    /// The indexed column ID.
    #[serde(rename = "column_id", default)]
    pub column_id: i64,
    /// Whether the column is signed.
    #[serde(rename = "is_signed", default)]
    pub is_signed: bool,
    /// The column's byte size.
    #[serde(rename = "type_size", default)]
    pub type_size: u8,
}

/// Go `FullTextIndexInfo`: a full-text index's parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextIndexInfo {
    /// The parser type (see [`full_text_parser_type`]).
    #[serde(
        rename = "parser_type",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub parser_type: String,
}

/// Go `IndexInfo`: metadata describing a table index.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexInfo {
    /// The index ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The index name.
    #[serde(rename = "idx_name", default)]
    pub name: CiString,
    /// The table name.
    #[serde(rename = "tbl_name", default)]
    pub table: CiString,
    /// The index columns.
    #[serde(
        rename = "idx_cols",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub columns: Vec<IndexColumn>,
    /// The online-DDL state.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
    /// The backfill-merge state.
    #[serde(
        rename = "backfill_state",
        default,
        serialize_with = "serialize_backfill_state",
        deserialize_with = "deserialize_backfill_state"
    )]
    pub backfill_state: BackfillState,
    /// The index comment.
    #[serde(
        rename = "comment",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub comment: String,
    /// The index type (Btree/Hash/...).
    #[serde(
        rename = "index_type",
        default,
        serialize_with = "serialize_index_type",
        deserialize_with = "deserialize_index_type"
    )]
    pub tp: IndexType,
    /// Whether the index is unique.
    #[serde(rename = "is_unique", default)]
    pub unique: bool,
    /// Whether the index is the primary key.
    #[serde(rename = "is_primary", default)]
    pub primary: bool,
    /// Whether the index is invisible.
    #[serde(rename = "is_invisible", default)]
    pub invisible: bool,
    /// Whether the index is global.
    #[serde(rename = "is_global", default)]
    pub global: bool,
    /// Whether the index is multi-valued.
    #[serde(rename = "mv_index", default)]
    pub mv_index: bool,
    /// Vector-index parameters, if any.
    #[serde(rename = "vector_index", default)]
    pub vector_info: Option<VectorIndexInfo>,
    /// Inverted-index parameters, if any.
    #[serde(rename = "inverted_index", default)]
    pub inverted_info: Option<InvertedIndexInfo>,
    /// Full-text-index parameters, if any.
    #[serde(rename = "full_text_index", default)]
    pub full_text_info: Option<FullTextIndexInfo>,
    /// The partial-index condition expression string.
    #[serde(
        rename = "condition_expr_string",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub condition_expr_string: String,
    /// The columns the index affects.
    #[serde(
        rename = "affect_column",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_vec",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub affect_column: Vec<IndexColumn>,
    /// The global-index version.
    #[serde(
        rename = "global_index_version",
        default,
        skip_serializing_if = "is_zero_u8"
    )]
    pub global_index_version: u8,
    /// The persistent region-split policy.
    #[serde(
        rename = "region_split_policy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub region_split_policy: Option<RegionSplitPolicy>,
}

impl IndexInfo {
    /// Go `IsChanging`: whether this is a modify-index temporary index.
    #[must_use]
    pub fn is_changing(&self) -> bool {
        self.name.original().starts_with(CHANGING_INDEX_PREFIX)
    }

    /// Go `IsRemoving`: whether this is a removing (tombstone) index.
    #[must_use]
    pub fn is_removing(&self) -> bool {
        self.name.original().starts_with(REMOVING_OBJ_PREFIX)
    }

    /// Go `GetRemovingOriginName`: the original name of a removing index.
    #[must_use]
    pub fn get_removing_origin_name(&self) -> String {
        removing_origin_name(self.name.original())
    }

    /// Go `GetChangingOriginName`: the original name of a changing index
    /// (strips the index changing prefix and the trailing `_<n>`).
    #[must_use]
    pub fn get_changing_origin_name(&self) -> String {
        let idx_name = self
            .name
            .original()
            .strip_prefix(CHANGING_INDEX_PREFIX)
            .unwrap_or(self.name.original());
        match idx_name.rfind('_') {
            None => idx_name.to_owned(),
            Some(pos) => idx_name[..pos].to_owned(),
        }
    }

    /// Go `HasPrefixIndex`: whether any column uses a prefix length.
    #[must_use]
    pub fn has_prefix_index(&self) -> bool {
        // Go compares against types.UnspecifiedLength (-1).
        self.columns.iter().any(|ic| ic.length != -1)
    }

    /// Go `FindColumnByName`: the index column named `name_l` (lower-cased).
    #[must_use]
    pub fn find_column_by_name(&self, name_l: &str) -> Option<&IndexColumn> {
        find_index_column_by_name(&self.columns, name_l).map(|(_, ic)| ic)
    }

    /// Go `IsPublic`: whether the index is in the public state.
    #[must_use]
    pub fn is_public(&self) -> bool {
        self.state == SchemaState::PUBLIC
    }

    /// Go `IsColumnarIndex`: whether this is a vector/inverted/full-text index.
    #[must_use]
    pub fn is_columnar_index(&self) -> bool {
        self.vector_info.is_some() || self.inverted_info.is_some() || self.full_text_info.is_some()
    }

    /// Go `GetColumnarIndexType`: the columnar-index kind (or `NA`).
    #[must_use]
    pub fn get_columnar_index_type(&self) -> ColumnarIndexType {
        if self.vector_info.is_some() {
            ColumnarIndexType::VECTOR
        } else if self.inverted_info.is_some() {
            ColumnarIndexType::INVERTED
        } else if self.full_text_info.is_some() {
            ColumnarIndexType::FULLTEXT
        } else {
            ColumnarIndexType::NA
        }
    }
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
    fn index_info_methods() {
        let mut idx = IndexInfo {
            name: CiString::new("_Idx$_myidx_0"),
            columns: vec![
                IndexColumn {
                    name: CiString::new("a"),
                    length: -1,
                    ..Default::default()
                },
                IndexColumn {
                    name: CiString::new("B"),
                    length: 10,
                    ..Default::default()
                },
            ],
            state: SchemaState::PUBLIC,
            ..Default::default()
        };
        assert!(idx.is_changing());
        assert_eq!(idx.get_changing_origin_name(), "myidx");
        assert!(idx.has_prefix_index()); // column B has length 10
        assert!(idx.is_public());
        assert!(idx.find_column_by_name("b").is_some());
        assert!(idx.find_column_by_name("z").is_none());

        // Removing name.
        idx.name = CiString::new("_Tombstone$_orig");
        assert!(idx.is_removing());
        assert_eq!(idx.get_removing_origin_name(), "orig");

        // Columnar-index type detection.
        assert!(!idx.is_columnar_index());
        assert_eq!(idx.get_columnar_index_type(), ColumnarIndexType::NA);
        idx.vector_info = Some(VectorIndexInfo {
            dimension: 128,
            distance_metric: distance_metric::COSINE.to_owned(),
        });
        assert!(idx.is_columnar_index());
        assert_eq!(idx.get_columnar_index_type(), ColumnarIndexType::VECTOR);
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
