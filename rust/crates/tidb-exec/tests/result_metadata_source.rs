// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use tidb_datatype::{Collation, FieldTypeCode};
use tidb_exec::{
    convert_result_field, ResultFieldMetadata, ResultFieldTypeMetadata, NOT_FIXED_DEC,
    UNSIGNED_FLAG,
};
use tidb_protocol::{ColumnDefault, TYPE_VAR_STRING};

fn field(code: FieldTypeCode) -> ResultFieldMetadata {
    ResultFieldMetadata {
        schema: "test".to_owned(),
        table: "alias".to_owned(),
        org_table: "t".to_owned(),
        name: "display".to_owned(),
        org_name: "original".to_owned(),
        empty_org_name: false,
        default_value: Some(ColumnDefault::Text("default".to_owned())),
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
fn conversion_preserves_names_defaults_flags_and_varchar_compatibility() {
    let mut input = field(FieldTypeCode::Varchar);
    input.field_type.flen = Some(10);
    let output = convert_result_field(&input);

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
    assert_eq!(
        output.default_value,
        Some(ColumnDefault::Text("default".to_owned()))
    );

    input.empty_org_name = true;
    assert!(convert_result_field(&input).org_name.is_empty());
}

#[test]
fn conversion_applies_decimal_charset_and_default_length_rules() {
    let mut decimal = field(FieldTypeCode::NewDecimal);
    decimal.field_type.flen = Some(10);
    decimal.field_type.decimal = Some(2);
    assert_eq!(convert_result_field(&decimal).column_length, 12);
    decimal.field_type.decimal = Some(0);
    assert_eq!(convert_result_field(&decimal).column_length, 11);

    let mut gbk_string = field(FieldTypeCode::Varchar);
    gbk_string.field_type.flen = Some(10);
    gbk_string.field_type.collation = Collation::GbkBin;
    assert_eq!(convert_result_field(&gbk_string).column_length, 20);

    let duration = convert_result_field(&field(FieldTypeCode::Duration));
    assert_eq!(duration.column_length, 10);
    assert_eq!(duration.decimal, 0);

    let integer = convert_result_field(&field(FieldTypeCode::Long));
    assert_eq!(integer.column_length, 11);
    assert_eq!(integer.decimal, NOT_FIXED_DEC);

    assert_eq!(
        convert_result_field(&field(FieldTypeCode::Null)).column_length,
        0,
        "mysql.GetDefaultFieldLengthAndDecimal(TypeNull) returns zero"
    );
}
