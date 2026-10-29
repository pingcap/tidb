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

//! Go `pkg/server/internal/column/convert.go`: `ConvertColumnInfo`, the wire
//! column definition of a resolved result field.

use tidb_datatype::FieldTypeCode;
use tidb_charset::Charset;
use tidb_charset::Collation;
use tidb_executor::adapter::{ResultFieldMetadata, ResultFieldTypeMetadata, NOT_FIXED_DEC};
use tidb_protocol::{ColumnInfo, TYPE_VAR_STRING};

/// Converts one resolved result field to protocol column metadata.
///
/// The arithmetic and type remapping mirror `ConvertColumnInfo` exactly for
/// the dependency-closed type/collation registry currently present in the
/// Rust workspace. Typed Datum formatting, session charset conversion, and
/// executor-side field resolution remain outside this leaf.
pub fn convert_column_info(field: &ResultFieldMetadata) -> ColumnInfo {
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
        charset: charset_default_collation_id(field.field_type.collation),
        flag: field.field_type.flags,
        decimal: decimal_places(&field.field_type),
        type_code,
        // Go `ConvertColumnInfo` also carries `fld.Column.GetDefaultValue()` for
        // `COM_FIELD_LIST`; the result fields do not carry the column's default yet.
        default_value: None,
    }
}

/// Go `tidbResultSet.Columns()`: [`convert_column_info`] over the record
/// set's fields.
pub fn convert_column_infos(fields: &[ResultFieldMetadata]) -> Vec<ColumnInfo> {
    fields.iter().map(convert_column_info).collect()
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
        tidb_charset::Charset::Binary
        | tidb_charset::Charset::Ascii
        | tidb_charset::Charset::Latin1 => 1,
        tidb_charset::Charset::Utf8 => 3,
        tidb_charset::Charset::Utf8Mb4 => 4,
        tidb_charset::Charset::Gbk => 2,
        tidb_charset::Charset::Gb18030 => 4,
    }
}

/// Returns the protocol ID Go emits for a field's character set.
///
/// `ConvertColumnInfo` calls `mysql.CharsetNameToID`, so a column declared
/// with `utf8mb4_general_ci` still carries `utf8mb4_bin` (46) in the wire
/// column definition. The protocol field is the charset's default collation,
/// not the column's selected collation. Keep this mapping independent of the
/// new-collation compatibility switch: Go's charset constants do not change
/// when that switch changes.
fn charset_default_collation_id(collation: Collation) -> u16 {
    match collation.charset() {
        Charset::Binary => 63,
        Charset::Ascii => 65,
        Charset::Latin1 => 47,
        Charset::Utf8 => 83,
        Charset::Utf8Mb4 => 46,
        Charset::Gbk => 28,
        Charset::Gb18030 => 248,
    }
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
        FieldTypeCode::Null => 0,
        // The Go lookup returns -1 for these entries and ConvertColumnInfo
        // casts that value to uint32.
        FieldTypeCode::Unspecified
        | FieldTypeCode::NewDate
        | FieldTypeCode::Enum
        | FieldTypeCode::Set
        | FieldTypeCode::Geometry
        | FieldTypeCode::VectorFloat32
        | FieldTypeCode::Unknown(_) => u32::MAX,
    }
}

#[cfg(test)]
mod tests {
    use tidb_datatype::FieldTypeCode;
use tidb_charset::Collation;
    use tidb_executor::adapter::{ResultFieldMetadata, ResultFieldTypeMetadata, NOT_FIXED_DEC, UNSIGNED_FLAG};
    use tidb_protocol::TYPE_VAR_STRING;

    use super::convert_column_info;

    fn field(code: FieldTypeCode) -> ResultFieldMetadata {
        ResultFieldMetadata {
            schema: "test".to_owned(),
            table: "alias".to_owned(),
            org_table: "t".to_owned(),
            name: "display".to_owned(),
            org_name: "original".to_owned(),
            empty_org_name: false,
            field_type: ResultFieldTypeMetadata {
                code,
                flags: UNSIGNED_FLAG,
                flen: None,
                decimal: None,
                collation: Collation::Utf8Mb4Bin,
            },
        }
    }

    #[test]
    fn conversion_preserves_names_flags_and_varchar_compatibility() {
        let mut input = field(FieldTypeCode::Varchar);
        input.field_type.flen = Some(10);
        let output = convert_column_info(&input);

        assert_eq!(output.schema, "test");
        assert_eq!(output.table, "alias");
        assert_eq!(output.org_table, "t");
        assert_eq!(output.name, "display");
        assert_eq!(output.org_name, "original");
        assert_eq!(output.flag, UNSIGNED_FLAG);
        assert_eq!(output.charset, 46);
        assert_eq!(output.type_code, TYPE_VAR_STRING);
        assert_eq!(output.column_length, 40);
        assert_eq!(output.decimal, NOT_FIXED_DEC);

        input.empty_org_name = true;
        assert!(convert_column_info(&input).org_name.is_empty());
    }

    #[test]
    fn conversion_applies_decimal_charset_and_default_length_rules() {
        let mut decimal = field(FieldTypeCode::NewDecimal);
        decimal.field_type.flen = Some(10);
        decimal.field_type.decimal = Some(2);
        assert_eq!(convert_column_info(&decimal).column_length, 12);
        decimal.field_type.decimal = Some(0);
        assert_eq!(convert_column_info(&decimal).column_length, 11);

        let mut gbk_string = field(FieldTypeCode::Varchar);
        gbk_string.field_type.flen = Some(10);
        gbk_string.field_type.collation = Collation::GbkBin;
        assert_eq!(convert_column_info(&gbk_string).column_length, 20);

        let duration = convert_column_info(&field(FieldTypeCode::Duration));
        assert_eq!(duration.column_length, 10);
        assert_eq!(duration.decimal, 0);

        let integer = convert_column_info(&field(FieldTypeCode::Long));
        assert_eq!(integer.column_length, 11);
        assert_eq!(integer.decimal, NOT_FIXED_DEC);

        assert_eq!(
            convert_column_info(&field(FieldTypeCode::Null)).column_length,
            0,
            "mysql.GetDefaultFieldLengthAndDecimal(TypeNull) returns zero"
        );
    }

    #[test]
    fn conversion_uses_charset_default_collation_for_nondefault_column_collation() {
        let mut input = field(FieldTypeCode::Varchar);
        input.field_type.flen = Some(10);
        input.field_type.collation = Collation::Utf8Mb4GeneralCi;

        // Go's ConvertColumnInfo calls CharsetNameToID("utf8mb4"), so the wire
        // charset number remains utf8mb4_bin (46), not the column collation (45).
        assert_eq!(convert_column_info(&input).charset, 46);
    }
}
