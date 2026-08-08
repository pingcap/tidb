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

use serde::de::DeserializeSeed;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tidb_ast::{CiString, Expr, IndexType, IsTarget, QueryStmt, SelectField, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode};

use crate::cascades_hash::HashInt64;
use crate::column::{removing_origin_name, REMOVING_OBJ_PREFIX};
use crate::go_runtime::{
    GoNullClonePolicy, GoPointerAny, GoShared, GoSharedPointerSlice, GoSharedSlice,
};
use crate::reorg::BackfillState;
use crate::schema_state::SchemaState;
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, impl_go_json_deserialize, impl_go_json_merge_object,
    FatalSeed, NullNoopSeed, OptionSharedMergeSeed, SharedPointerSliceSeed, SharedStringSliceSeed,
    ValueMergeSeed,
};
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
    let used: std::collections::HashSet<String> = table
        .indices
        .iter_deref()
        .map(|candidate| candidate.read().name.lowercase().to_owned())
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

struct IndexTypeSeed<'a>(&'a mut IndexType);

impl<'de> DeserializeSeed<'de> for IndexTypeSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if let Some(value) = Option::<i64>::deserialize(deserializer)? {
            *self.0 = IndexType(value);
        }
        Ok(())
    }
}

/// Go `BackfillState` is a `byte`, so `encoding/json` writes it as a number.
fn serialize_backfill_state<S: Serializer>(
    state: &BackfillState,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_u8(state.0)
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
#[derive(Debug, Default, Serialize)]
pub struct RegionSplitPolicy {
    /// The lower-bound split points.
    #[serde(rename = "lower", default)]
    pub lower: GoSharedSlice<String>,
    /// The upper-bound split points.
    #[serde(rename = "upper", default)]
    pub upper: GoSharedSlice<String>,
    /// The number of regions.
    #[serde(rename = "regions", default)]
    pub regions: i64,
}

impl_go_json_merge_object!(RegionSplitPolicy, destination, map, key, {
    if go_json_field_matches(&key, "lower") {
        map.next_value_seed(SharedStringSliceSeed(&mut destination.lower))?;
    } else if go_json_field_matches(&key, "upper") {
        map.next_value_seed(SharedStringSliceSeed(&mut destination.upper))?;
    } else if go_json_field_matches(&key, "regions") {
        map.next_value_seed(NullNoopSeed(&mut destination.regions))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(RegionSplitPolicy);

impl Clone for RegionSplitPolicy {
    fn clone(&self) -> Self {
        fn clone_bound(values: &GoSharedSlice<String>) -> GoSharedSlice<String> {
            if values.is_empty() {
                return values.clone();
            }
            // Go source uses make(len)+copy, so cap is exactly len.
            GoSharedSlice::from_vec_with_capacity(values.snapshot(), values.len())
        }

        Self {
            lower: clone_bound(&self.lower),
            upper: clone_bound(&self.upper),
            regions: self.regions,
        }
    }
}

/// Go `IndexColumn`: one column referenced by an index.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct IndexColumn {
    /// The index column name.
    #[serde(rename = "name", default)]
    pub name: CiString,
    /// The column's offset in `TableInfo.Columns`.
    #[serde(rename = "offset", default)]
    pub offset: i64,
    /// The prefix length (`UnspecifiedLength` when not a prefix index).
    #[serde(rename = "length", default)]
    pub length: i64,
    /// Whether the column uses the changing type.
    #[serde(
        rename = "using_changing_type",
        default,
        skip_serializing_if = "crate::serde_helpers::is_false"
    )]
    pub use_changing_type: bool,
}

impl_go_json_merge_object!(IndexColumn, destination, map, key, {
    if go_json_field_matches(&key, "name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.name)))?;
    } else if go_json_field_matches(&key, "offset") {
        map.next_value_seed(NullNoopSeed(&mut destination.offset))?;
    } else if go_json_field_matches(&key, "length") {
        map.next_value_seed(NullNoopSeed(&mut destination.length))?;
    } else if go_json_field_matches(&key, "using_changing_type") {
        map.next_value_seed(NullNoopSeed(&mut destination.use_changing_type))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(IndexColumn);

/// Go `VectorIndexInfo`: a vector index's parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct VectorIndexInfo {
    /// The vector dimension.
    #[serde(rename = "dimension", default)]
    pub dimension: u64,
    /// The distance metric (see [`distance_metric`]).
    #[serde(rename = "distance_metric", default)]
    pub distance_metric: String,
}

impl_go_json_merge_object!(VectorIndexInfo, destination, map, key, {
    if go_json_field_matches(&key, "dimension") {
        map.next_value_seed(NullNoopSeed(&mut destination.dimension))?;
    } else if go_json_field_matches(&key, "distance_metric") {
        map.next_value_seed(NullNoopSeed(&mut destination.distance_metric))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(VectorIndexInfo);

/// Go `InvertedIndexInfo`: an inverted index's parameters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
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

impl_go_json_merge_object!(InvertedIndexInfo, destination, map, key, {
    if go_json_field_matches(&key, "column_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.column_id))?;
    } else if go_json_field_matches(&key, "is_signed") {
        map.next_value_seed(NullNoopSeed(&mut destination.is_signed))?;
    } else if go_json_field_matches(&key, "type_size") {
        map.next_value_seed(NullNoopSeed(&mut destination.type_size))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(InvertedIndexInfo);

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
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct FullTextIndexInfo {
    /// The parser type (see [`full_text_parser_type`]).
    #[serde(rename = "parser_type", default)]
    pub parser_type: String,
}

impl_go_json_merge_object!(FullTextIndexInfo, destination, map, key, {
    if go_json_field_matches(&key, "parser_type") {
        map.next_value_seed(NullNoopSeed(&mut destination.parser_type))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(FullTextIndexInfo);

/// Go `IndexInfo`: metadata describing a table index.
#[derive(Debug, Default, Serialize)]
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
    #[serde(rename = "idx_cols", default)]
    pub columns: GoSharedPointerSlice<IndexColumn>,
    /// The online-DDL state.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
    /// The backfill-merge state.
    #[serde(
        rename = "backfill_state",
        default,
        serialize_with = "serialize_backfill_state"
    )]
    pub backfill_state: BackfillState,
    /// The index comment.
    #[serde(rename = "comment", default)]
    pub comment: String,
    /// The index type (Btree/Hash/...).
    #[serde(
        rename = "index_type",
        default,
        serialize_with = "serialize_index_type"
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
    pub vector_info: Option<GoShared<VectorIndexInfo>>,
    /// Inverted-index parameters, if any.
    #[serde(rename = "inverted_index", default)]
    pub inverted_info: Option<GoShared<InvertedIndexInfo>>,
    /// Full-text-index parameters, if any.
    #[serde(rename = "full_text_index", default)]
    pub full_text_info: Option<GoShared<FullTextIndexInfo>>,
    /// The partial-index condition expression string.
    #[serde(rename = "condition_expr_string", default)]
    pub condition_expr_string: String,
    /// The columns the index affects.
    #[serde(
        rename = "affect_column",
        default,
        skip_serializing_if = "GoSharedPointerSlice::is_empty"
    )]
    pub affect_column: GoSharedPointerSlice<IndexColumn>,
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
    pub region_split_policy: Option<GoShared<RegionSplitPolicy>>,
}

impl Clone for IndexInfo {
    /// Go `IndexInfo.Clone`: the column pointer slices and region policy are
    /// copied deeply, while vector/inverted/full-text pointers remain shared.
    /// A nil Columns header becomes an allocated-empty slice; AffectColumn
    /// remains nil when the source header is nil. Nil elements panic when the
    /// source invokes `IndexColumn.Clone`.
    fn clone(&self) -> Self {
        let columns = self
            .columns
            .map_clone_with(GoNullClonePolicy::Panic, Clone::clone);
        let affect_column = if self.affect_column.is_allocated() {
            self.affect_column
                .map_clone_with(GoNullClonePolicy::Panic, Clone::clone)
        } else {
            GoSharedPointerSlice::default()
        };
        Self {
            id: self.id,
            name: self.name.clone(),
            table: self.table.clone(),
            columns,
            state: self.state,
            backfill_state: self.backfill_state,
            comment: self.comment.clone(),
            tp: self.tp,
            unique: self.unique,
            primary: self.primary,
            invisible: self.invisible,
            global: self.global,
            mv_index: self.mv_index,
            vector_info: self.vector_info.clone(),
            inverted_info: self.inverted_info.clone(),
            full_text_info: self.full_text_info.clone(),
            condition_expr_string: self.condition_expr_string.clone(),
            affect_column,
            global_index_version: self.global_index_version,
            region_split_policy: self
                .region_split_policy
                .as_ref()
                .map(|policy| GoShared::new(policy.read().clone())),
        }
    }
}

impl_go_json_merge_object!(IndexInfo, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "idx_name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.name)))?;
    } else if go_json_field_matches(&key, "tbl_name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.table)))?;
    } else if go_json_field_matches(&key, "idx_cols") {
        map.next_value_seed(SharedPointerSliceSeed(&mut destination.columns))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else if go_json_field_matches(&key, "backfill_state") {
        map.next_value_seed(NullNoopSeed(&mut destination.backfill_state))?;
    } else if go_json_field_matches(&key, "comment") {
        map.next_value_seed(NullNoopSeed(&mut destination.comment))?;
    } else if go_json_field_matches(&key, "index_type") {
        map.next_value_seed(IndexTypeSeed(&mut destination.tp))?;
    } else if go_json_field_matches(&key, "is_unique") {
        map.next_value_seed(NullNoopSeed(&mut destination.unique))?;
    } else if go_json_field_matches(&key, "is_primary") {
        map.next_value_seed(NullNoopSeed(&mut destination.primary))?;
    } else if go_json_field_matches(&key, "is_invisible") {
        map.next_value_seed(NullNoopSeed(&mut destination.invisible))?;
    } else if go_json_field_matches(&key, "is_global") {
        map.next_value_seed(NullNoopSeed(&mut destination.global))?;
    } else if go_json_field_matches(&key, "mv_index") {
        map.next_value_seed(NullNoopSeed(&mut destination.mv_index))?;
    } else if go_json_field_matches(&key, "vector_index") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.vector_info))?;
    } else if go_json_field_matches(&key, "inverted_index") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.inverted_info))?;
    } else if go_json_field_matches(&key, "full_text_index") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.full_text_info))?;
    } else if go_json_field_matches(&key, "condition_expr_string") {
        map.next_value_seed(NullNoopSeed(&mut destination.condition_expr_string))?;
    } else if go_json_field_matches(&key, "affect_column") {
        map.next_value_seed(SharedPointerSliceSeed(&mut destination.affect_column))?;
    } else if go_json_field_matches(&key, "global_index_version") {
        map.next_value_seed(NullNoopSeed(&mut destination.global_index_version))?;
    } else if go_json_field_matches(&key, "region_split_policy") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.region_split_policy))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(IndexInfo);

impl IndexInfo {
    /// Nil-receiver-capable Go `IndexInfo.Clone` boundary.
    #[must_use]
    pub fn clone_pointer(index: Option<&Self>) -> Option<GoShared<Self>> {
        index.map(|value| GoShared::new(value.clone()))
    }

    /// Go `Hash64`/`Equals` use only the persisted index ID. Rust's standard
    /// equality follows that exact identity contract.
    #[must_use]
    pub fn equals_id(&self, other: &Self) -> bool {
        self.id == other.id
    }

    /// Exact Go `Equals(any)`, including wrong dynamic types and typed-nil
    /// pointer interfaces. `receiver` represents Go's possibly nil method
    /// receiver.
    #[must_use]
    pub fn equals(receiver: Option<&Self>, other: GoPointerAny<'_, Self>) -> bool {
        let GoPointerAny::Typed(other) = other else {
            return false;
        };
        match (receiver, other) {
            (None, None) => true,
            (Some(left), Some(right)) => left.id == right.id,
            _ => false,
        }
    }

    /// Go `Hash64`: hash the persisted index ID as one whole cascades
    /// `HashInt64` step.
    pub fn hash64<H: HashInt64>(&self, state: &mut H) {
        state.hash_int64(self.id);
    }

    /// Nil-receiver-capable Go `Hash64` call boundary. A nil `*IndexInfo`
    /// panics when Go evaluates `index.ID`.
    pub fn hash64_pointer<H: HashInt64>(index: Option<&Self>, state: &mut H) {
        index.expect("nil *IndexInfo").hash64(state);
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
        self.columns
            .iter_deref()
            .any(|column| column.read().length != -1)
    }

    /// Go `HasColumnInIndexColumns`: whether an index column resolves to the
    /// requested table-column ID. As in Go, an invalid offset is a metadata
    /// invariant violation and indexes the table slice directly.
    #[must_use]
    pub fn has_column_in_index_columns(&self, table: &TableInfo, column_id: i64) -> bool {
        self.columns.iter_deref().any(|column| {
            let offset = column.read().offset;
            table
                .columns
                .get(offset as usize)
                .expect("nil *ColumnInfo in TableInfo.Columns")
                .read()
                .id
                == column_id
        })
    }

    /// Go `FindColumnByName`: the index column named `name_l` (lower-cased).
    #[must_use]
    pub fn find_column_by_name(&self, name_l: &str) -> Option<GoShared<IndexColumn>> {
        self.columns
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name_l)
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
pub fn find_index_by_columns(
    table: &TableInfo,
    indices: &GoSharedPointerSlice<IndexInfo>,
    columns: &[CiString],
) -> Option<GoShared<IndexInfo>> {
    indices
        .iter_deref()
        .find(|index| is_index_prefix_covered(table, &index.read(), columns))
}

/// Go `IsIndexPrefixCovered`.
#[must_use]
pub fn is_index_prefix_covered(table: &TableInfo, index: &IndexInfo, columns: &[CiString]) -> bool {
    if index.columns.len() < columns.len() {
        return false;
    }
    columns.iter().enumerate().all(|(position, column)| {
        let index_column = index
            .columns
            .get(position)
            .expect("nil *IndexColumn in IndexInfo.Columns");
        let index_column = index_column.read();
        if column.lowercase() != index_column.name.lowercase()
            || index_column.offset >= table.columns.len() as i64
        {
            return false;
        }
        let table_column = table.columns.get(index_column.offset as usize);
        index_column.length == -1
            || index_column.length
                >= table_column
                    .expect("nil *ColumnInfo in TableInfo.Columns")
                    .read()
                    .get_flen()
    })
}

/// Go `FindIndexByColumnsForForeignKey`.
#[must_use]
pub fn find_index_by_columns_for_foreign_key(
    table: &TableInfo,
    indices: &GoSharedPointerSlice<IndexInfo>,
    columns: &[CiString],
) -> Option<GoShared<IndexInfo>> {
    indices
        .iter_deref()
        .find(|index| is_index_prefix_covered_for_foreign_key(table, &index.read(), columns))
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
pub fn find_index_info_by_id(
    indices: &GoSharedPointerSlice<IndexInfo>,
    id: i64,
) -> Option<GoShared<IndexInfo>> {
    indices.iter_deref().find(|index| index.read().id == id)
}

/// Go `FindIndexColumnByName`: the position and column matching `name_l`
/// (already lower-cased), or `None`.
#[must_use]
pub fn find_index_column_by_name(
    index_cols: &GoSharedPointerSlice<IndexColumn>,
    name_l: &str,
) -> Option<(usize, GoShared<IndexColumn>)> {
    index_cols
        .iter_deref()
        .enumerate()
        .find(|(_, column)| column.read().name.lowercase() == name_l)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn index_json_preserves_pointer_slice_and_merge_states() {
        use crate::serde_helpers::GoJsonMerge;

        let nil: IndexInfo = serde_json::from_str(r#"{"idx_cols":null}"#).unwrap();
        let empty: IndexInfo = serde_json::from_str(r#"{"idx_cols":[]}"#).unwrap();
        assert!(!nil.columns.is_allocated());
        assert!(empty.columns.is_allocated());
        assert_eq!(
            serde_json::to_value(nil).unwrap()["idx_cols"],
            serde_json::Value::Null
        );
        assert_eq!(
            serde_json::to_value(empty).unwrap()["idx_cols"],
            serde_json::json!([])
        );

        let nullable: IndexInfo =
            serde_json::from_str(r#"{"idx_cols":[null,{"offset":2}]}"#).unwrap();
        assert!(nullable.columns.get(0).is_none());
        assert_eq!(nullable.columns.get(1).unwrap().read().offset, 2);
        assert!(std::panic::catch_unwind(|| nullable.columns.iter_deref().next()).is_err());

        let mut index = IndexInfo {
            id: 7,
            tp: IndexType(9),
            vector_info: Some(GoShared::new(VectorIndexInfo {
                dimension: 3,
                distance_metric: "old".to_owned(),
            })),
            ..Default::default()
        };
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{
                "id":null,
                "index_type":"bad",
                "VECTOR_INDEX":{"distance_metric":"later"},
                "COMMENT":"after-error",
                "idx_cols":[null]
            }"#,
        );
        assert!(index.go_json_merge(&mut decoder).is_err());
        assert_eq!(index.id, 7);
        assert_eq!(index.tp, IndexType(9));
        assert_eq!(index.vector_info.as_ref().unwrap().read().dimension, 3);
        assert_eq!(
            index.vector_info.as_ref().unwrap().read().distance_metric,
            "later"
        );
        assert_eq!(index.comment, "after-error");
        assert!(index.columns.get(0).is_none());

        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"idx_cols":[],"IDX_COLS":null,"affect_column":[]}"#,
        );
        index.go_json_merge(&mut decoder).unwrap();
        assert!(!index.columns.is_allocated());
        assert!(index.affect_column.is_allocated());
        // `omitempty` suppresses both nil and allocated-empty pointer slices.
        assert!(serde_json::to_value(&index)
            .unwrap()
            .get("affect_column")
            .is_none());

        let region: RegionSplitPolicy =
            serde_json::from_str(r#"{"lower":null,"upper":[]}"#).unwrap();
        assert!(!region.lower.is_allocated());
        assert!(region.upper.is_allocated());

        let allocated_empty = GoSharedSlice::<String>::from_vec_with_capacity(Vec::new(), 4);
        let empty_region = RegionSplitPolicy {
            lower: allocated_empty.clone(),
            ..Default::default()
        };
        let empty_clone = empty_region.clone();
        assert!(empty_clone.lower.backing_ptr_eq(&allocated_empty));
        assert_eq!(empty_clone.lower.capacity(), 4);

        let bound = GoSharedSlice::from_vec_with_capacity(vec!["a".to_owned()], 4);
        let bounded_region = RegionSplitPolicy {
            lower: bound.clone(),
            ..Default::default()
        };
        let bounded_clone = bounded_region.clone();
        assert!(!bounded_clone.lower.backing_ptr_eq(&bound));
        assert_eq!(bounded_clone.lower.capacity(), 1);
        bounded_clone.lower.set(0, "clone".to_owned());
        assert_eq!(bound.get(0), "a");

        let mut receiving = RegionSplitPolicy {
            lower: GoSharedSlice::from_vec_with_capacity(
                vec!["old".to_owned(), "tail".to_owned()],
                3,
            ),
            ..Default::default()
        };
        let receiving_alias = receiving.lower.clone();
        let mut decoder =
            serde_json::Deserializer::from_str(r#"{"lower":[null,7,"later"],"regions":4}"#);
        assert!(receiving.go_json_merge(&mut decoder).is_err());
        assert!(receiving.lower.backing_ptr_eq(&receiving_alias));
        assert_eq!(receiving.lower.len(), 3);
        assert_eq!(receiving.lower.get(0), "old");
        assert_eq!(receiving.lower.get(1), "tail");
        assert_eq!(receiving.lower.get(2), "later");
        assert_eq!(receiving.regions, 4);

        let mut decoder = serde_json::Deserializer::from_str(r#"{"lower":null}"#);
        receiving.go_json_merge(&mut decoder).unwrap();
        assert!(!receiving.lower.is_allocated());
        assert!(receiving_alias.is_allocated());

        index.name = CiString::new("OldIndex");
        index.table = CiString::new("OldTable");
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{
                "idx_name":{"O":"First"},
                "IDX_NAME":{"L":"folded"},
                "tbl_name":{"L":"table-lower"}
            }"#,
        );
        index.go_json_merge(&mut decoder).unwrap();
        assert_eq!(index.name.original(), "First");
        assert_eq!(index.name.lowercase(), "folded");
        assert_eq!(index.table.original(), "OldTable");
        assert_eq!(index.table.lowercase(), "table-lower");

        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"idx_name":"SingleIndex","tbl_name":"SingleTable","comment":"after"}"#,
        );
        index.go_json_merge(&mut decoder).unwrap();
        assert_eq!(index.name.original(), "SingleIndex");
        assert_eq!(index.name.lowercase(), "singleindex");
        assert_eq!(index.table.original(), "SingleTable");
        assert_eq!(index.table.lowercase(), "singletable");
        assert_eq!(index.comment, "after");

        index.name = serde_json::from_str(r#"{"O":"First","L":"folded"}"#).unwrap();
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"idx_name":{"O":1,"L":"partial"},"comment":"unreached"}"#,
        );
        assert!(index.go_json_merge(&mut decoder).is_err());
        assert_eq!(index.name.original(), "First");
        assert_eq!(index.name.lowercase(), "partial");
        assert_eq!(index.comment, "after");

        let mut index_column = IndexColumn {
            name: CiString::new("OldColumn"),
            ..Default::default()
        };
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"name":{"O":"NewColumn"},"NAME":{"L":"new-column"}}"#,
        );
        index_column.go_json_merge(&mut decoder).unwrap();
        assert_eq!(index_column.name.original(), "NewColumn");
        assert_eq!(index_column.name.lowercase(), "new-column");
    }

    #[test]
    fn index_clone_preserves_source_pointer_policies() {
        assert!(IndexInfo::clone_pointer(None).is_none());

        let vector = GoShared::new(VectorIndexInfo {
            dimension: 3,
            distance_metric: distance_metric::L2.to_owned(),
        });
        let inverted = GoShared::new(InvertedIndexInfo {
            column_id: 7,
            is_signed: true,
            type_size: 8,
        });
        let full_text = GoShared::new(FullTextIndexInfo {
            parser_type: "standard".to_owned(),
        });
        let region = GoShared::new(RegionSplitPolicy {
            lower: GoSharedSlice::from_vec(vec!["a".to_owned()]),
            upper: GoSharedSlice::from_vec(Vec::new()),
            regions: 2,
        });
        let column = GoShared::new(IndexColumn {
            name: CiString::new("a"),
            offset: 1,
            length: -1,
            ..Default::default()
        });
        let affected = GoShared::new(IndexColumn {
            name: CiString::new("b"),
            ..Default::default()
        });
        let source = IndexInfo {
            columns: GoSharedPointerSlice::from_handles(vec![Some(column.clone())]),
            affect_column: GoSharedPointerSlice::from_handles(vec![Some(affected.clone())]),
            vector_info: Some(vector.clone()),
            inverted_info: Some(inverted.clone()),
            full_text_info: Some(full_text.clone()),
            region_split_policy: Some(region.clone()),
            ..Default::default()
        };
        let cloned = source.clone();

        assert!(cloned.columns.is_allocated());
        assert_eq!(cloned.columns.capacity(), source.columns.len());
        assert!(!cloned.columns.backing_ptr_eq(&source.columns));
        assert!(!cloned.columns.get(0).unwrap().ptr_eq(&column));
        cloned.columns.get(0).unwrap().write().offset = 9;
        assert_eq!(column.read().offset, 1);

        assert!(cloned.affect_column.is_allocated());
        assert_eq!(cloned.affect_column.capacity(), source.affect_column.len());
        assert!(!cloned.affect_column.backing_ptr_eq(&source.affect_column));
        assert!(!cloned.affect_column.get(0).unwrap().ptr_eq(&affected));

        assert!(cloned.vector_info.as_ref().unwrap().ptr_eq(&vector));
        assert!(cloned.inverted_info.as_ref().unwrap().ptr_eq(&inverted));
        assert!(cloned.full_text_info.as_ref().unwrap().ptr_eq(&full_text));
        let cloned_region = cloned.region_split_policy.as_ref().unwrap();
        assert!(!cloned_region.ptr_eq(&region));
        cloned_region.write().regions = 8;
        assert_eq!(region.read().regions, 2);

        let nil_columns = IndexInfo::default().clone();
        assert!(nil_columns.columns.is_allocated());
        assert!(!nil_columns.affect_column.is_allocated());

        let empty_affect = IndexInfo {
            affect_column: GoSharedPointerSlice::from_nullable(Vec::new()),
            ..Default::default()
        };
        let empty_affect_clone = empty_affect.clone();
        assert!(empty_affect_clone.affect_column.is_allocated());
        assert!(!empty_affect_clone
            .affect_column
            .backing_ptr_eq(&empty_affect.affect_column));

        let null_column = IndexInfo {
            columns: GoSharedPointerSlice::from_nullable(vec![None]),
            ..Default::default()
        };
        assert!(std::panic::catch_unwind(|| null_column.clone()).is_err());
        let null_affect = IndexInfo {
            affect_column: GoSharedPointerSlice::from_nullable(vec![None]),
            ..Default::default()
        };
        assert!(std::panic::catch_unwind(|| null_affect.clone()).is_err());
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
            ]
            .into(),
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
        idx.vector_info = Some(GoShared::new(VectorIndexInfo {
            dimension: 128,
            distance_metric: distance_metric::COSINE.to_owned(),
        }));
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
        assert!(IndexInfo::equals(
            Some(&left),
            GoPointerAny::typed(Some(&right))
        ));
        assert!(!IndexInfo::equals(Some(&left), GoPointerAny::Other));
        assert!(!IndexInfo::equals(Some(&left), GoPointerAny::typed(None)));
        assert!(IndexInfo::equals(None, GoPointerAny::typed(None)));
        assert!(!IndexInfo::equals(None, GoPointerAny::typed(Some(&right))));
        let hash = |index: &IndexInfo| {
            let mut state = crate::cascades_hash::CascadesHasher::new();
            index.hash64(&mut state);
            state.sum64()
        };
        assert_eq!(hash(&left), hash(&right));
        assert!(std::panic::catch_unwind(|| {
            let mut state = crate::cascades_hash::CascadesHasher::new();
            IndexInfo::hash64_pointer(None, &mut state);
        })
        .is_err());
        for (id, expected) in [
            (0, 12_638_153_115_695_167_455),
            (-1, 5_808_589_858_502_755_950),
            (i64::MIN, 3_414_781_078_840_391_647),
            (i64::MAX, 15_031_961_895_357_531_758),
        ] {
            let index = IndexInfo {
                id,
                ..Default::default()
            };
            assert_eq!(hash(&index), expected);
        }
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
        let mut table = TableInfo {
            columns: vec![
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
            ]
            .into(),
            indices: vec![IndexInfo {
                name: CiString::new("_idx$_Key_0"),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        let source = IndexInfo {
            name: CiString::new("Key"),
            columns: vec![IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: 8,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        assert_eq!(
            gen_unique_changing_index_name(&table, &source),
            "_Idx$_Key_1"
        );
        table.indices = vec![IndexInfo {
            name: CiString::new("_Idx$_i_0"),
            ..Default::default()
        }]
        .into();
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
        short.columns.get(0).unwrap().write().length = 7;
        assert!(!is_index_prefix_covered(
            &table,
            &short,
            &[CiString::new("a")]
        ));
        assert!(source.has_column_in_index_columns(&table, 10));
        assert!(!source.has_column_in_index_columns(&table, 11));

        let mut out_of_range = source.clone();
        out_of_range.columns.get(0).unwrap().write().offset = table.columns.len() as i64;
        assert!(!is_index_prefix_covered(
            &table,
            &out_of_range,
            &[CiString::new("a")]
        ));
        out_of_range.columns.get(0).unwrap().write().offset = -1;
        assert!(std::panic::catch_unwind(|| {
            is_index_prefix_covered(&table, &out_of_range, &[CiString::new("a")])
        })
        .is_err());

        let nil_column_table = TableInfo {
            columns: GoSharedPointerSlice::from_nullable(vec![None]),
            ..Default::default()
        };
        let mut nil_column_index = IndexInfo {
            columns: vec![IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: -1,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        assert!(is_index_prefix_covered(
            &nil_column_table,
            &nil_column_index,
            &[CiString::new("a")]
        ));
        nil_column_index.columns.get(0).unwrap().write().length = 0;
        assert!(std::panic::catch_unwind(|| {
            is_index_prefix_covered(&nil_column_table, &nil_column_index, &[CiString::new("a")])
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
            }]
            .into(),
            ..Default::default()
        };
        let base = IndexInfo {
            columns: vec![IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: -1,
                ..Default::default()
            }]
            .into(),
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
    fn pointer_slice_index_finders_return_source_handles_and_panic_on_nil() {
        let table = TableInfo {
            columns: vec![crate::column::ColumnInfo {
                id: 1,
                name: CiString::new("a"),
                offset: 0,
                field_type: FieldType::new(FieldTypeCode::Varchar).with_flen(8),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        let indices: GoSharedPointerSlice<IndexInfo> = vec![IndexInfo {
            id: 9,
            columns: vec![IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: -1,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        }]
        .into();

        let by_columns = find_index_by_columns(&table, &indices, &[CiString::new("a")]).unwrap();
        let by_id = find_index_info_by_id(&indices, 9).unwrap();
        assert!(by_columns.ptr_eq(&indices.get(0).unwrap()));
        assert!(by_id.ptr_eq(&indices.get(0).unwrap()));

        let nullable = GoSharedPointerSlice::from_nullable(vec![
            None,
            Some(IndexInfo {
                id: 9,
                ..Default::default()
            }),
        ]);
        assert!(std::panic::catch_unwind(|| {
            find_index_by_columns(&table, &nullable, &[CiString::new("a")])
        })
        .is_err());
        assert!(std::panic::catch_unwind(|| find_index_info_by_id(&nullable, 9)).is_err());
    }

    #[test]
    fn find_index_column() {
        let cols: GoSharedPointerSlice<_> = vec![
            IndexColumn {
                name: CiString::new("Foo"),
                ..Default::default()
            },
            IndexColumn {
                name: CiString::new("Bar"),
                ..Default::default()
            },
        ]
        .into();
        let (i, ic) = find_index_column_by_name(&cols, "bar").unwrap();
        assert_eq!(i, 1);
        assert_eq!(ic.read().name.original(), "Bar");
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
