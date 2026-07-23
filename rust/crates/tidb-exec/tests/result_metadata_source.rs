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

// aggregate-test: standalone

#![allow(missing_docs)]

#[path = "../src/result_metadata.rs"]
mod result_metadata;

use result_metadata::{
    col_names_to_result_fields, columns_from_adapted_fields, convert_result_field,
    FieldNameMetadata, IdentifierMetadata, ResultFieldMetadata, ResultFieldTypeMetadata,
};
use tidb_datatype::{Collation, FieldTypeCode};
use tidb_protocol::{ColumnDefault, TYPE_VAR_STRING};

fn field(code: FieldTypeCode, flen: Option<u32>) -> ResultFieldMetadata {
    ResultFieldMetadata {
        schema: "test".to_owned(),
        table: "dual".to_owned(),
        org_table: String::new(),
        name: "a".to_owned(),
        org_name: "a".to_owned(),
        empty_org_name: false,
        default_value: None,
        field_type: ResultFieldTypeMetadata {
            code,
            flags: 0,
            flen,
            decimal: None,
            collation: Collation::Utf8GeneralCi,
        },
    }
}

#[test]
fn convert_column_info_preserves_source_names_flags_and_type_remap() {
    let mut bit = field(FieldTypeCode::Bit, Some(1));
    bit.field_type.flags = result_metadata::UNSIGNED_FLAG;
    let got = convert_result_field(&bit);
    assert_eq!(got.schema, "test");
    assert_eq!(got.table, "dual");
    assert_eq!(got.org_table, "");
    assert_eq!(got.name, "a");
    assert_eq!(got.org_name, "a");
    assert_eq!(got.column_length, 1);
    assert_eq!(got.charset, 33);
    assert_eq!(got.flag, result_metadata::UNSIGNED_FLAG);
    assert_eq!(got.type_code, FieldTypeCode::Bit.mysql_type());

    let mut not_null = field(FieldTypeCode::Long, Some(11));
    not_null.field_type.flags = result_metadata::NOT_NULL_FLAG;
    assert_eq!(
        convert_result_field(&not_null).flag,
        result_metadata::NOT_NULL_FLAG
    );

    let tiny = convert_result_field(&field(FieldTypeCode::Tiny, Some(1)));
    assert_eq!(tiny.column_length, 1);

    let mut varchar = field(FieldTypeCode::Varchar, Some(5));
    varchar.empty_org_name = true;
    varchar.default_value = Some(ColumnDefault::Text("x".to_owned()));
    let varchar = convert_result_field(&varchar);
    assert_eq!(varchar.type_code, TYPE_VAR_STRING);
    assert_eq!(varchar.org_name, "");
    assert_eq!(
        varchar.default_value,
        Some(ColumnDefault::Text("x".to_owned()))
    );
}

#[test]
fn convert_column_info_uses_default_lengths_for_unspecified_fields() {
    let cases = [
        (FieldTypeCode::Bit, 1),
        (FieldTypeCode::Tiny, 4),
        (FieldTypeCode::Short, 6),
        (FieldTypeCode::Int24, 9),
        (FieldTypeCode::Long, 11),
        (FieldTypeCode::LongLong, 20),
        (FieldTypeCode::Float, 12),
        (FieldTypeCode::Double, 22),
        (FieldTypeCode::NewDecimal, 10),
        (FieldTypeCode::Duration, 10),
        (FieldTypeCode::Date, 10),
        (FieldTypeCode::Timestamp, 19),
        (FieldTypeCode::Datetime, 19),
        (FieldTypeCode::Year, 4),
        (FieldTypeCode::String, 1),
        (FieldTypeCode::Varchar, 5),
        (FieldTypeCode::VarString, 5),
        (FieldTypeCode::TinyBlob, 255),
        (FieldTypeCode::Blob, 65_535),
        (FieldTypeCode::MediumBlob, 16_777_215),
        (FieldTypeCode::LongBlob, u32::MAX),
        (FieldTypeCode::Json, u32::MAX),
    ];
    for (code, expected) in cases {
        let got = convert_result_field(&field(code, None));
        assert_eq!(got.column_length, expected, "code={code:?}");
    }
}

#[test]
fn convert_column_info_matches_decimal_and_character_width_rules() {
    let mut decimal = field(FieldTypeCode::NewDecimal, Some(20));
    decimal.field_type.decimal = Some(4);
    assert_eq!(convert_result_field(&decimal).column_length, 22);

    let mut integer_decimal = field(FieldTypeCode::Long, Some(11));
    integer_decimal.field_type.decimal = Some(4);
    assert_eq!(convert_result_field(&integer_decimal).column_length, 11);

    let mut utf8mb4 = field(FieldTypeCode::VarString, Some(20));
    utf8mb4.field_type.collation = Collation::Utf8Mb4Bin;
    assert_eq!(convert_result_field(&utf8mb4).column_length, 80);

    let mut latin1_enum = field(FieldTypeCode::Enum, Some(20));
    latin1_enum.field_type.collation = Collation::Latin1Bin;
    assert_eq!(convert_result_field(&latin1_enum).column_length, 20);

    let mut duration = field(FieldTypeCode::Duration, Some(10));
    assert_eq!(convert_result_field(&duration).decimal, 0);
    duration.field_type.decimal = Some(3);
    assert_eq!(convert_result_field(&duration).decimal, 3);
}

#[test]
fn adapter_applies_default_database_and_preserves_original_names() {
    let return_type = ResultFieldTypeMetadata {
        code: FieldTypeCode::LongLong,
        flags: result_metadata::UNSIGNED_FLAG,
        flen: Some(20),
        decimal: Some(0),
        collation: Collation::Binary,
    };
    let names = [FieldNameMetadata {
        original_table: IdentifierMetadata::new("base_table"),
        original_column: IdentifierMetadata::new("base_col"),
        database: IdentifierMetadata::new(""),
        table: IdentifierMetadata::new("alias_t"),
        column: IdentifierMetadata::new("display_col"),
    }];

    let fields =
        col_names_to_result_fields(std::slice::from_ref(&return_type), &names, "default_db");
    assert_eq!(fields.len(), 1);
    let field = &fields[0];
    assert_eq!(field.database, IdentifierMetadata::new("default_db"));
    assert_eq!(field.original_table, IdentifierMetadata::new("base_table"));
    assert_eq!(field.column_name, IdentifierMetadata::new("base_col"));
    assert_eq!(field.column_as_name, IdentifierMetadata::new("display_col"));
    assert!(!field.empty_org_name);
    assert_eq!(field.field_type, return_type);

    let projected = field.as_result_field();
    assert_eq!(projected.schema, "default_db");
    assert_eq!(projected.table, "alias_t");
    assert_eq!(projected.org_table, "base_table");
    assert_eq!(projected.org_name, "base_col");
    assert_eq!(projected.name, "display_col");
    assert_eq!(projected.field_type, return_type);
}

#[test]
fn adapter_falls_back_to_display_name_for_expression_and_truncates_aliases() {
    let return_type = ResultFieldTypeMetadata {
        code: FieldTypeCode::VarString,
        flags: 0,
        flen: Some(10),
        decimal: None,
        collation: Collation::Utf8Mb4Bin,
    };
    let long_original = "A".repeat(result_metadata::MAX_ALIAS_IDENTIFIER_LEN + 10);
    let long_lower = "a".repeat(result_metadata::MAX_ALIAS_IDENTIFIER_LEN + 10);
    let names = [FieldNameMetadata {
        original_table: IdentifierMetadata::new(""),
        original_column: IdentifierMetadata::from_parts("", ""),
        database: IdentifierMetadata::new("explicit_db"),
        table: IdentifierMetadata::new(""),
        column: IdentifierMetadata::from_parts(long_original.clone(), long_lower.clone()),
    }];

    let fields =
        col_names_to_result_fields(std::slice::from_ref(&return_type), &names, "ignored_db");
    let field = &fields[0];
    assert_eq!(field.database, IdentifierMetadata::new("explicit_db"));
    assert_eq!(field.original_table, IdentifierMetadata::new(""));
    assert!(field.empty_org_name);
    assert_eq!(field.column_name, names[0].column);
    assert_eq!(field.column_as_name.original, "A".repeat(256));
    assert_eq!(field.column_as_name.lower, "a".repeat(256));
    assert_eq!(field.column_as_name.original.len(), 256);
    assert_eq!(field.column_as_name.lower.len(), 256);
    assert_eq!(field.field_type, return_type);

    let projected = field.as_result_field();
    assert_eq!(projected.org_name, long_original);
    assert!(projected.empty_org_name);
    assert_eq!(projected.name, "A".repeat(256));
}

#[test]
fn adapted_fields_feed_the_existing_framed_result_set_path() {
    let return_type = ResultFieldTypeMetadata {
        code: FieldTypeCode::LongLong,
        flags: 0,
        flen: Some(20),
        decimal: Some(0),
        collation: Collation::Binary,
    };
    let names = [FieldNameMetadata {
        original_table: IdentifierMetadata::new(""),
        original_column: IdentifierMetadata::new(""),
        database: IdentifierMetadata::new(""),
        table: IdentifierMetadata::new(""),
        column: IdentifierMetadata::new("answer"),
    }];
    let fields = col_names_to_result_fields(std::slice::from_ref(&return_type), &names, "");
    let columns = columns_from_adapted_fields(&fields);
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "answer");
    assert_eq!(columns[0].org_name, "");
    assert_eq!(columns[0].type_code, FieldTypeCode::LongLong.mysql_type());
    assert_eq!(columns[0].flag, 0);

    let mut session = tidb_exec::Cluster::new().session();
    let mut request = tidb_distsql::DistSqlContext::new();
    let mut framed = Vec::new();
    let mut writer = tidb_protocol::PacketWriter::new(&mut framed);
    writer
        .write_packet(b"\x03select 7")
        .expect("frame COM_QUERY");
    writer.flush().expect("flush COM_QUERY");
    let encoded = session
        .execute_framed_query_text_result_set(
            &framed,
            &mut request,
            &columns,
            tidb_protocol::ResultSetOptions {
                status_flags: 2,
                ..tidb_protocol::ResultSetOptions::default()
            },
        )
        .expect("encode result set using adapted fields");
    let mut reader = tidb_protocol::PacketReader::new(std::io::Cursor::new(encoded));
    assert_eq!(reader.read_packet().expect("column count"), vec![0x01]);
    let _metadata = reader.read_packet().expect("column metadata");
    assert_eq!(
        reader.read_packet().expect("metadata EOF"),
        vec![0xfe, 0x00, 0x00, 0x02, 0x00]
    );
    assert_eq!(reader.read_packet().expect("text row"), vec![0x01, b'7']);
    assert_eq!(
        reader.read_packet().expect("terminal EOF"),
        vec![0xfe, 0x00, 0x00, 0x02, 0x00]
    );
    assert_eq!(request.request.original_sql, "select 7");
}
