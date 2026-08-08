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

//! `pkg/meta/model/index.go`: persisted index metadata and its lookup,
//! prefix-coverage, partial-index, and columnar-index rules.

use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tidb_ast::{CiString, Expr, IndexType, IsTarget, QueryStmt, SelectField, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode};

use crate::column::{removing_origin_name, REMOVING_OBJ_PREFIX};
use crate::reorg::BackfillState;
use crate::schema_state::SchemaState;
use crate::table_info::TableInfo;

/// Distance-metric values for a vector index (Go `DistanceMetric`, a string).
pub mod distance_metric {
    /// L2 (Euclidean) distance.
    pub const L2: &str = "L2";
    /// Cosine distance.
    pub const COSINE: &str = "COSINE";
    /// Inner-product distance.
    pub const INNER_PRODUCT: &str = "INNER_PRODUCT";
}

/// Go `ast.VecCosineDistance`, used by the bidirectional indexable-function
/// maps in `index.go`.
pub const VEC_COSINE_DISTANCE_FN: &str = "vec_cosine_distance";
/// Go `ast.VecL2Distance`, used by the bidirectional indexable-function maps
/// in `index.go`.
pub const VEC_L2_DISTANCE_FN: &str = "vec_l2_distance";

/// Go `IndexableFnNameToDistanceMetric`.
#[must_use]
pub fn indexable_fn_name_to_distance_metric(name: &str) -> Option<&'static str> {
    match name {
        VEC_COSINE_DISTANCE_FN => Some(distance_metric::COSINE),
        VEC_L2_DISTANCE_FN => Some(distance_metric::L2),
        _ => None,
    }
}

/// Go `IndexableDistanceMetricToFnName`.
#[must_use]
pub fn indexable_distance_metric_to_fn_name(metric: &str) -> Option<&'static str> {
    match metric {
        distance_metric::COSINE => Some(VEC_COSINE_DISTANCE_FN),
        distance_metric::L2 => Some(VEC_L2_DISTANCE_FN),
        _ => None,
    }
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

/// Go `GenUniqueChangingIndexName`: generates the first unused temporary
/// changing-index name, comparing the candidate case-insensitively.
#[must_use]
pub fn gen_unique_changing_index_name(table: &TableInfo, index: &IndexInfo) -> String {
    let used: std::collections::HashSet<&str> = table
        .indices
        .iter()
        .map(|candidate| candidate.name.lowercase())
        .collect();
    let mut suffix = 0_u64;
    loop {
        let candidate = format!(
            "{CHANGING_INDEX_PREFIX}{}_{}",
            index.name.original(),
            suffix
        );
        if !used.contains(tidb_mysql::to_lowercase(&candidate).as_str()) {
            return candidate;
        }
        suffix += 1;
    }
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
    match tidb_mysql::to_uppercase(name).as_str() {
        "STANDARD" => full_text_parser_type::STANDARD_V1,
        "MULTILINGUAL" => full_text_parser_type::MULTILINGUAL_V1,
        _ => full_text_parser_type::INVALID,
    }
}

/// Go `n == 0` for the `omitempty` check on a `uint8` field.
fn is_zero_u8(value: &u8) -> bool {
    *value == 0
}

/// Go `ast.IndexType` is an `int`, so `encoding/json` writes it as a number and
/// accepts any number back. `IndexType` lives in `tidb-ast` without serde
/// impls, so the conversion is applied per field. It carries the raw integer
/// through: Go's declaration warns the value "may come from a previous version
/// persisted in TableInfo", and folding an unnamed value to `INVALID` would
/// rewrite another version's index type to 0 on the first write here.
fn serialize_index_type<S: Serializer>(tp: &IndexType, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_i64(tp.0)
}

fn deserialize_index_type<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<IndexType, D::Error> {
    Ok(IndexType(
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

/// Go `FieldTypeToInvertedIndexInfo`: returns the fixed-width physical
/// representation used by an inverted index, or `None` for unsupported
/// source types.
#[must_use]
pub fn field_type_to_inverted_index_info(
    field_type: &FieldType,
    column_id: i64,
) -> Option<InvertedIndexInfo> {
    let (type_size, is_signed) = match field_type.code() {
        FieldTypeCode::Tiny => (1, !field_type.is_unsigned()),
        FieldTypeCode::Short => (2, !field_type.is_unsigned()),
        FieldTypeCode::Int24 | FieldTypeCode::Long => (4, !field_type.is_unsigned()),
        FieldTypeCode::LongLong => (8, !field_type.is_unsigned()),
        FieldTypeCode::Year | FieldTypeCode::Enum => (2, false),
        FieldTypeCode::Set
        | FieldTypeCode::Datetime
        | FieldTypeCode::Date
        | FieldTypeCode::Timestamp => (8, false),
        FieldTypeCode::Duration => (8, true),
        _ => return None,
    };
    Some(InvertedIndexInfo {
        column_id,
        is_signed,
        type_size,
    })
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
    /// Go `Hash64`/`Equals` use only the persisted index ID. Rust's standard
    /// equality follows that exact identity contract.
    #[must_use]
    pub fn equals_id(&self, other: &Self) -> bool {
        self.id == other.id
    }

    /// Go `Hash64`: feed only the persisted index ID to the supplied hasher.
    pub fn hash_id<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.id, state);
    }

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

    /// Go `HasColumnInIndexColumns`: whether an index column resolves to the
    /// requested table-column ID. As in Go, an invalid offset is a metadata
    /// invariant violation and indexes the table slice directly.
    #[must_use]
    pub fn has_column_in_index_columns(&self, table: &TableInfo, column_id: i64) -> bool {
        self.columns
            .iter()
            .any(|column| table.columns[column.offset as usize].id == column_id)
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

    /// Go `HasCondition`.
    #[must_use]
    pub fn has_condition(&self) -> bool {
        !self.condition_expr_string.is_empty()
    }

    /// Go `ConditionExpr`: parses the stored predicate as the sole field in a
    /// synthetic SELECT and returns that field expression.
    pub fn condition_expr(&self) -> Result<Expr, tidb_parser::ParseError> {
        let sql = format!("select {}", self.condition_expr_string);
        let mut statements = tidb_parser::parse_multi(&sql)?;
        let statement = statements.remove(0);
        if let Stmt::Query(query) = statement {
            if let QueryStmt::Select(select) = &*query {
                if let Some(SelectField::Expr { expr, .. }) = select.fields.fields().first() {
                    return Ok(expr.clone());
                }
            }
        }
        Err(tidb_parser::ParseError {
            message: "partial-index condition did not produce one SELECT expression".to_owned(),
            offset: sql.len(),
            near_offset: sql.len(),
        })
    }
}

/// Go `FindIndexByColumns`: first index whose leading columns cover `columns`.
#[must_use]
pub fn find_index_by_columns<'a>(
    table: &TableInfo,
    indices: &'a [IndexInfo],
    columns: &[CiString],
) -> Option<&'a IndexInfo> {
    indices
        .iter()
        .find(|index| is_index_prefix_covered(table, index, columns))
}

/// Go `IsIndexPrefixCovered`.
#[must_use]
pub fn is_index_prefix_covered(table: &TableInfo, index: &IndexInfo, columns: &[CiString]) -> bool {
    if index.columns.len() < columns.len() {
        return false;
    }
    columns.iter().enumerate().all(|(position, column)| {
        let index_column = &index.columns[position];
        if column.lowercase() != index_column.name.lowercase()
            || index_column.offset >= table.columns.len() as i32
        {
            return false;
        }
        let table_column = &table.columns[index_column.offset as usize];
        index_column.length == -1 || i64::from(index_column.length) >= table_column.get_flen()
    })
}

/// Go `FindIndexByColumnsForForeignKey`.
#[must_use]
pub fn find_index_by_columns_for_foreign_key<'a>(
    table: &TableInfo,
    indices: &'a [IndexInfo],
    columns: &[CiString],
) -> Option<&'a IndexInfo> {
    indices
        .iter()
        .find(|index| is_index_prefix_covered_for_foreign_key(table, index, columns))
}

/// Go `IsIndexPrefixCoveredForForeignKey`.
#[must_use]
pub fn is_index_prefix_covered_for_foreign_key(
    table: &TableInfo,
    index: &IndexInfo,
    columns: &[CiString],
) -> bool {
    is_index_prefix_covered(table, index, columns)
        && is_index_condition_covered_by_foreign_key_columns(index, columns)
}

fn is_index_condition_covered_by_foreign_key_columns(
    index: &IndexInfo,
    columns: &[CiString],
) -> bool {
    if !index.has_condition() {
        return true;
    }
    let Ok(Expr::Is { expr, target, not }) = index.condition_expr() else {
        return false;
    };
    if target != IsTarget::Null || !not {
        return false;
    }
    let Expr::Column(path) = *expr else {
        return false;
    };
    let Some(name) = path.last() else {
        return false;
    };
    columns
        .iter()
        .any(|column| tidb_mysql::to_lowercase(name) == column.lowercase())
}

/// Go `FindIndexInfoByID`.
#[must_use]
pub fn find_index_info_by_id(indices: &[IndexInfo], id: i64) -> Option<&IndexInfo> {
    indices.iter().find(|index| index.id == id)
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
    use std::hash::Hasher;

    // Go's `ast.IndexType` is a plain `int`, and its declaration warns that a
    // value "may come from a previous version persisted in TableInfo. So you
    // must keep it compatible when modifying it." An `index_type` this build
    // has no constant for must therefore survive a decode/encode cycle byte
    // for byte, not become 0.
    #[test]
    fn unknown_index_type_survives_round_trip() {
        let go =
            r#"{"id":1,"idx_name":{"O":"i","L":"i"},"tbl_name":{"O":"t","L":"t"},"index_type":9}"#;
        let idx: IndexInfo = serde_json::from_str(go).unwrap();
        assert_eq!(idx.tp, IndexType(9));
        // Go `IndexType.String` returns "" for an unnamed value, so
        // SHOW CREATE TABLE omits the USING clause rather than inventing one.
        assert_eq!(idx.tp.sql(), "");

        let encoded = serde_json::to_string(&idx).unwrap();
        assert!(
            encoded.contains(r#""index_type":9"#),
            "index_type collapsed: {encoded}"
        );
        // Byte-identical on every further cycle: nothing about the value is
        // normalized away.
        let again: IndexInfo = serde_json::from_str(&encoded).unwrap();
        assert_eq!(serde_json::to_string(&again).unwrap(), encoded);
    }

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
        assert_eq!(
            get_full_text_parser_type_by_sql_name("\u{fb06}andard"),
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
    fn inverted_index_field_type_boundaries() {
        let signed = FieldType::new(FieldTypeCode::Long);
        assert_eq!(
            field_type_to_inverted_index_info(&signed, 7),
            Some(InvertedIndexInfo {
                column_id: 7,
                is_signed: true,
                type_size: 4,
            })
        );
        let unsigned = FieldType::new(FieldTypeCode::Long).with_unsigned(true);
        assert!(
            !field_type_to_inverted_index_info(&unsigned, 7)
                .unwrap()
                .is_signed
        );
        assert_eq!(
            field_type_to_inverted_index_info(&FieldType::new(FieldTypeCode::Year), 1)
                .unwrap()
                .type_size,
            2
        );
        assert!(
            field_type_to_inverted_index_info(&FieldType::new(FieldTypeCode::Varchar), 1).is_none()
        );
    }

    #[test]
    fn index_identity_hash_and_equality_use_only_id() {
        let left = IndexInfo {
            id: 9,
            name: CiString::new("left"),
            ..Default::default()
        };
        let right = IndexInfo {
            id: 9,
            name: CiString::new("right"),
            ..Default::default()
        };
        let other = IndexInfo {
            id: 10,
            name: CiString::new("left"),
            ..Default::default()
        };
        assert!(left.equals_id(&right));
        assert!(!left.equals_id(&other));
        let hash = |index: &IndexInfo| {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            index.hash_id(&mut state);
            state.finish()
        };
        assert_eq!(hash(&left), hash(&right));
    }

    #[test]
    fn indexable_distance_function_maps_are_exact_and_bidirectional() {
        assert_eq!(
            indexable_fn_name_to_distance_metric(VEC_COSINE_DISTANCE_FN),
            Some(distance_metric::COSINE)
        );
        assert_eq!(
            indexable_fn_name_to_distance_metric(VEC_L2_DISTANCE_FN),
            Some(distance_metric::L2)
        );
        assert_eq!(
            indexable_distance_metric_to_fn_name(distance_metric::COSINE),
            Some(VEC_COSINE_DISTANCE_FN)
        );
        assert_eq!(
            indexable_distance_metric_to_fn_name(distance_metric::L2),
            Some(VEC_L2_DISTANCE_FN)
        );
        assert_eq!(
            indexable_fn_name_to_distance_metric("VEC_L2_DISTANCE"),
            None
        );
        assert_eq!(
            indexable_distance_metric_to_fn_name(distance_metric::INNER_PRODUCT),
            None
        );
    }

    #[test]
    fn unique_changing_name_and_prefix_coverage_boundaries() {
        let mut table = TableInfo::default();
        table.columns = vec![
            crate::column::ColumnInfo {
                id: 10,
                name: CiString::new("a"),
                offset: 0,
                field_type: FieldType::new(FieldTypeCode::Varchar).with_flen(8),
                ..Default::default()
            },
            crate::column::ColumnInfo {
                id: 11,
                name: CiString::new("b"),
                offset: 1,
                ..Default::default()
            },
        ];
        table.indices = vec![IndexInfo {
            name: CiString::new("_idx$_Key_0"),
            ..Default::default()
        }];
        let source = IndexInfo {
            name: CiString::new("Key"),
            columns: vec![IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: 8,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            gen_unique_changing_index_name(&table, &source),
            "_Idx$_Key_1"
        );
        table.indices = vec![IndexInfo {
            name: CiString::new("_Idx$_i_0"),
            ..Default::default()
        }];
        let unicode_source = IndexInfo {
            name: CiString::new("\u{130}"),
            ..Default::default()
        };
        assert_eq!(
            gen_unique_changing_index_name(&table, &unicode_source),
            "_Idx$_\u{130}_1"
        );
        assert!(is_index_prefix_covered(
            &table,
            &source,
            &[CiString::new("A")]
        ));
        let mut short = source.clone();
        short.columns[0].length = 7;
        assert!(!is_index_prefix_covered(
            &table,
            &short,
            &[CiString::new("a")]
        ));
        assert!(source.has_column_in_index_columns(&table, 10));
        assert!(!source.has_column_in_index_columns(&table, 11));

        let mut out_of_range = source.clone();
        out_of_range.columns[0].offset = table.columns.len() as i32;
        assert!(!is_index_prefix_covered(
            &table,
            &out_of_range,
            &[CiString::new("a")]
        ));
        out_of_range.columns[0].offset = -1;
        assert!(std::panic::catch_unwind(|| {
            is_index_prefix_covered(&table, &out_of_range, &[CiString::new("a")])
        })
        .is_err());
    }

    #[test]
    fn foreign_key_partial_index_condition_boundaries() {
        let table = TableInfo {
            columns: vec![crate::column::ColumnInfo {
                name: CiString::new("a"),
                offset: 0,
                ..Default::default()
            }],
            ..Default::default()
        };
        let base = IndexInfo {
            columns: vec![IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: -1,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(is_index_prefix_covered_for_foreign_key(
            &table,
            &base,
            &[CiString::new("a")]
        ));
        let mut safe = base.clone();
        safe.condition_expr_string = "a is not null".to_owned();
        assert!(is_index_prefix_covered_for_foreign_key(
            &table,
            &safe,
            &[CiString::new("a")]
        ));
        safe.condition_expr_string = "a is null".to_owned();
        assert!(!is_index_prefix_covered_for_foreign_key(
            &table,
            &safe,
            &[CiString::new("a")]
        ));
        safe.condition_expr_string = "b is not null".to_owned();
        assert!(!is_index_prefix_covered_for_foreign_key(
            &table,
            &safe,
            &[CiString::new("a")]
        ));
        safe.condition_expr_string = "a > 0".to_owned();
        assert!(!is_index_prefix_covered_for_foreign_key(
            &table,
            &safe,
            &[CiString::new("a")]
        ));
        safe.condition_expr_string = "a is not null; select 1".to_owned();
        assert!(is_index_prefix_covered_for_foreign_key(
            &table,
            &safe,
            &[CiString::new("a")]
        ));
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

    /// Go `encoding/json` output for `model.IndexInfo` at each
    /// `GlobalIndexVersion`, captured from a program that marshals the real Go
    /// struct against this worktree's `pkg/meta/model`.
    ///
    /// The version is what tells a reader whether the partition id is part of
    /// the index KEY (`GenIndexKey`'s `GlobalIndexVersionV1+` arm). Dropping it
    /// on read makes a V1 global index read back as legacy and produce keys
    /// that collide across partitions, so the round trip is pinned here.
    const GO_INDEX_INFO_JSON: [(u8, &str); 3] = [
        (
            0,
            r#"{"id":3,"idx_name":{"O":"g","L":"g"},"tbl_name":{"O":"t","L":"t"},"idx_cols":null,"state":0,"backfill_state":0,"comment":"","index_type":0,"is_unique":true,"is_primary":false,"is_invisible":false,"is_global":false,"mv_index":false,"vector_index":null,"inverted_index":null,"full_text_index":null,"condition_expr_string":""}"#,
        ),
        (
            1,
            r#"{"id":3,"idx_name":{"O":"g","L":"g"},"tbl_name":{"O":"t","L":"t"},"idx_cols":null,"state":0,"backfill_state":0,"comment":"","index_type":0,"is_unique":true,"is_primary":false,"is_invisible":false,"is_global":true,"mv_index":false,"vector_index":null,"inverted_index":null,"full_text_index":null,"condition_expr_string":"","global_index_version":1}"#,
        ),
        (
            2,
            r#"{"id":3,"idx_name":{"O":"g","L":"g"},"tbl_name":{"O":"t","L":"t"},"idx_cols":null,"state":0,"backfill_state":0,"comment":"","index_type":0,"is_unique":true,"is_primary":false,"is_invisible":false,"is_global":true,"mv_index":false,"vector_index":null,"inverted_index":null,"full_text_index":null,"condition_expr_string":"","global_index_version":2}"#,
        ),
    ];

    #[test]
    fn go_global_index_version_survives_the_round_trip() {
        for (version, json) in GO_INDEX_INFO_JSON {
            let index: IndexInfo = serde_json::from_str(json).unwrap();
            assert_eq!(index.global_index_version, version);
            assert_eq!(index.global, version != 0);
            assert!(!index.mv_index);
            // Go's tag is `global_index_version,omitempty`, so version 0 must
            // not appear in the re-encoded form and any other version must.
            let encoded = serde_json::to_string(&index).unwrap();
            assert_eq!(
                encoded.contains("global_index_version"),
                version != 0,
                "omitempty parity for version {version}"
            );
            let reparsed: IndexInfo = serde_json::from_str(&encoded).unwrap();
            assert_eq!(reparsed.global_index_version, version);
        }
    }
}
