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

//! Source-shaped result-field metadata conversion.
//!
//! This leaf ports the metadata-only part of Go's
//! `pkg/server/internal/column/convert.go::ConvertColumnInfo`.  It deliberately
//! does not know how an executor resolves a `ResultField`; the caller supplies
//! the already resolved display/original names and the source field type.
//! Wiring this leaf into the connected server path belongs to the executor
//! steward after typed expression metadata is available.

use tidb_datatype::{Collation, FieldTypeCode};
pub use tidb_datatype::{FieldNameMetadata, IdentifierMetadata};
use tidb_protocol::{ColumnDefault, ColumnInfo, TYPE_VAR_STRING};

/// The source's MySQL `NotFixedDec` marker.
pub const NOT_FIXED_DEC: u8 = 31;

/// MySQL's unsigned flag (`mysql.UnsignedFlag`).
pub const UNSIGNED_FLAG: u16 = 1 << 5;

/// MySQL's not-null flag (`mysql.NotNullFlag`).
///
/// The seed catalog currently stores nullable-by-default column declarations,
/// but planner-owned fields may already carry this flag. Outer-join metadata
/// must clear it on the null-extended side instead of retaining an impossible
/// NOT NULL wire declaration.
pub const NOT_NULL_FLAG: u16 = 1;

/// Source metadata required by `ConvertColumnInfo`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultFieldTypeMetadata {
    /// MySQL/TiDB field type code.
    pub code: FieldTypeCode,
    /// Raw MySQL field flags (for example [`UNSIGNED_FLAG`]).
    pub flags: u16,
    /// Declared display width; `None` is Go's `types.UnspecifiedLength`.
    pub flen: Option<u32>,
    /// Declared decimal scale; `None` is Go's `types.UnspecifiedLength`.
    pub decimal: Option<u8>,
    /// Registered collation used to derive the protocol charset ID and the
    /// byte-width multiplier for character fields.
    pub collation: Collation,
}

/// Source-shaped resolved result field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultFieldMetadata {
    /// Database/schema display name.
    pub schema: String,
    /// Table alias display name.
    pub table: String,
    /// Original table name. An unresolved table is represented by an empty
    /// string, matching Go's `ResultField.Table == nil` branch.
    pub org_table: String,
    /// Display name, including a SELECT-list alias when present.
    pub name: String,
    /// Original column name from the underlying `ColumnInfo`.
    pub org_name: String,
    /// Go's expression-only `EmptyOrgName` bit.
    pub empty_org_name: bool,
    /// Optional default value for a `COM_FIELD_LIST` response.
    pub default_value: Option<ColumnDefault>,
    /// Resolved field type metadata.
    pub field_type: ResultFieldTypeMetadata,
}

/// Output shape produced by Go's `colNames2ResultFields` adapter helper.
///
/// This keeps the two CIStr spellings and the source `ColumnInfo` name/type
/// separate from [`ResultFieldMetadata`], whose string fields are the wire
/// conversion view. Use [`AdaptedResultField::as_result_field`] when the
/// caller intentionally projects this shape into that view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdaptedResultField {
    /// Source `ColumnInfo.Name` after expression fallback.
    pub column_name: IdentifierMetadata,
    /// Source `ResultField.ColumnAsName`, truncated to the compatibility
    /// limit independently for `O` and `L`.
    pub column_as_name: IdentifierMetadata,
    /// Whether the source expression had no original column name.
    pub empty_org_name: bool,
    /// Source `TableInfo.Name` (`OrigTblName`).
    pub original_table: IdentifierMetadata,
    /// Source `ResultField.TableAsName`.
    pub table: IdentifierMetadata,
    /// Source `ResultField.DBName`, including default database fallback.
    pub database: IdentifierMetadata,
    /// Return field type copied from the matching schema column.
    pub field_type: ResultFieldTypeMetadata,
}

/// MySQL's maximum alias identifier length (`mysql.MaxAliasIdentifierLen`).
pub const MAX_ALIAS_IDENTIFIER_LEN: usize = 256;

impl AdaptedResultField {
    /// Projects the adapter output into the metadata view consumed by the
    /// protocol column conversion leaf.
    pub fn as_result_field(&self) -> ResultFieldMetadata {
        ResultFieldMetadata {
            schema: self.database.original.clone(),
            table: self.table.original.clone(),
            org_table: self.original_table.original.clone(),
            name: self.column_as_name.original.clone(),
            org_name: self.column_name.original.clone(),
            empty_org_name: self.empty_org_name,
            default_value: None,
            field_type: self.field_type.clone(),
        }
    }
}

/// Ports `pkg/executor/adapter.go::colNames2ResultFields` for source-shaped
/// names and return types.
///
/// Go's helper assumes that the schema and output-name slices have identical
/// lengths; this function preserves that contract with an assertion rather
/// than introducing a new recoverable error path.
pub fn col_names_to_result_fields(
    schema: &[ResultFieldTypeMetadata],
    names: &[FieldNameMetadata],
    default_db: &str,
) -> Vec<AdaptedResultField> {
    assert_eq!(schema.len(), names.len());
    let default_database = IdentifierMetadata::new(default_db);
    schema
        .iter()
        .zip(names)
        .map(|(field_type, name)| {
            let database = if name.database.lower.is_empty() && !name.table.lower.is_empty() {
                default_database.clone()
            } else {
                name.database.clone()
            };
            let (column_name, empty_org_name) = if name.original_column.lower.is_empty() {
                (name.column.clone(), true)
            } else {
                (name.original_column.clone(), false)
            };
            AdaptedResultField {
                column_name,
                column_as_name: truncate_alias(&name.column),
                empty_org_name,
                original_table: name.original_table.clone(),
                table: name.table.clone(),
                database,
                field_type: field_type.clone(),
            }
        })
        .collect()
}

fn truncate_alias(identifier: &IdentifierMetadata) -> IdentifierMetadata {
    IdentifierMetadata {
        original: truncate_alias_part(&identifier.original),
        lower: truncate_alias_part(&identifier.lower),
    }
}

fn truncate_alias_part(value: &str) -> String {
    if value.len() <= MAX_ALIAS_IDENTIFIER_LEN {
        return value.to_owned();
    }
    // Go slices the identifier at a byte boundary. Rust strings must remain
    // valid UTF-8, so keep the largest valid prefix no longer than the same
    // 256-byte protocol limit. ASCII aliases (the source compatibility case)
    // therefore have byte-identical behavior.
    let mut end = MAX_ALIAS_IDENTIFIER_LEN;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

/// Converts one resolved result field to protocol column metadata.
///
/// The arithmetic and type remapping mirror `ConvertColumnInfo` exactly for
/// the dependency-closed type/collation registry currently present in the
/// Rust workspace. Typed Datum formatting, session charset conversion, and
/// executor-side field resolution remain outside this leaf.
pub fn convert_result_field(field: &ResultFieldMetadata) -> ColumnInfo {
    let code = field.field_type.code;
    let type_code = if code == FieldTypeCode::Varchar {
        // Keep old clients compatible, as Go's server conversion does.
        TYPE_VAR_STRING
    } else {
        code.mysql_type()
    };
    let org_name = if field.empty_org_name {
        String::new()
    } else {
        field.org_name.clone()
    };

    ColumnInfo {
        schema: field.schema.clone(),
        table: field.table.clone(),
        org_table: field.org_table.clone(),
        name: field.name.clone(),
        org_name,
        column_length: column_length(&field.field_type),
        charset: collation_id(field.field_type.collation),
        flag: field.field_type.flags,
        decimal: decimal_places(&field.field_type),
        type_code,
        default_value: field.default_value.clone(),
    }
}

/// Converts adapter output into the column vector accepted by
/// `Session::execute_framed_query_text_result_set`.
///
/// This is the narrow connection between source-shaped executor fields and
/// the existing framed result-set path. It does not execute SQL, infer a
/// schema, or derive metadata from `Datum` values; callers still own the
/// resolved field list and protocol options.
pub fn columns_from_adapted_fields(fields: &[AdaptedResultField]) -> Vec<ColumnInfo> {
    fields
        .iter()
        .map(|field| convert_result_field(&field.as_result_field()))
        .collect()
}

fn column_length(field: &ResultFieldTypeMetadata) -> u32 {
    let code = field.code;
    let Some(flen) = field.flen else {
        // Go converts the `-1` returned for an unknown type to uint32.  The
        // known source table below covers every type currently represented by
        // `FieldTypeCode`; retaining MAX for an unknown value keeps that cast
        // observable instead of inventing a new zero-length protocol field.
        return default_field_length(code);
    };

    let mut length = flen;
    if code == FieldTypeCode::NewDecimal {
        // `ConvertColumnInfo` reserves one byte for a sign and another byte
        // when the decimal has a fractional part.
        length = length.wrapping_add(1);
        if field.decimal.is_some_and(|decimal| decimal > 0) {
            length = length.wrapping_add(1);
        }
    } else if code.is_string() || matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) {
        length = length.wrapping_mul(max_bytes_per_character(field.collation));
    }
    length
}

fn decimal_places(field: &ResultFieldTypeMetadata) -> u8 {
    match field.decimal {
        Some(decimal) => decimal,
        None if field.code == FieldTypeCode::Duration => 0,
        None => NOT_FIXED_DEC,
    }
}

fn max_bytes_per_character(collation: Collation) -> u32 {
    match collation.charset() {
        tidb_datatype::Charset::Binary
        | tidb_datatype::Charset::Ascii
        | tidb_datatype::Charset::Latin1 => 1,
        tidb_datatype::Charset::Utf8 => 3,
        tidb_datatype::Charset::Utf8Mb4 => 4,
        tidb_datatype::Charset::Gbk => 2,
        tidb_datatype::Charset::Gb18030 => 4,
    }
}

fn collation_id(collation: Collation) -> u16 {
    u16::try_from(collation.id()).expect("supported collation IDs fit u16")
}

fn default_field_length(code: FieldTypeCode) -> u32 {
    match code {
        FieldTypeCode::Bit => 1,
        FieldTypeCode::Tiny => 4,
        FieldTypeCode::Short => 6,
        FieldTypeCode::Int24 => 9,
        FieldTypeCode::Long => 11,
        FieldTypeCode::LongLong => 20,
        FieldTypeCode::Double => 22,
        FieldTypeCode::Float => 12,
        FieldTypeCode::NewDecimal => 10,
        FieldTypeCode::Duration => 10,
        FieldTypeCode::Date => 10,
        FieldTypeCode::Timestamp | FieldTypeCode::Datetime => 19,
        FieldTypeCode::Year => 4,
        FieldTypeCode::String => 1,
        FieldTypeCode::Varchar | FieldTypeCode::VarString => 5,
        FieldTypeCode::TinyBlob => 255,
        FieldTypeCode::Blob => 65_535,
        FieldTypeCode::MediumBlob => 16_777_215,
        FieldTypeCode::LongBlob | FieldTypeCode::Json => u32::MAX,
        // The Go lookup returns -1 for these entries and ConvertColumnInfo
        // casts that value to uint32.
        FieldTypeCode::Unspecified
        | FieldTypeCode::Null
        | FieldTypeCode::NewDate
        | FieldTypeCode::Enum
        | FieldTypeCode::Set
        | FieldTypeCode::Geometry
        | FieldTypeCode::VectorFloat32
        | FieldTypeCode::Unknown(_) => u32::MAX,
    }
}
