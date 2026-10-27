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


use crate::bad_null::*;
use tidb_datatype::{BinaryJSON, BinaryJSONValue, BinaryLiteral, CoreTime, Datum, Decimal, FieldType, FieldTypeCode, MySqlDuration, MysqlEnum, MysqlSet, StringDatum, Time, TimeType};

use crate::DriverError;
use tidb_datatype::{FieldTypeBuilder, FieldTypeFlags};
use tidb_model::column::check_column_infos_once;
use tidb_model::{ColumnInfo, GoShared, GoSharedPointerSlice};

fn field(code: FieldTypeCode, flen: i64, charset: &str, collation: &str) -> FieldType {
    FieldTypeBuilder::new()
        .with_code(code)
        .flen_set(flen)
        .charset_set(charset)
        .collation_set(collation)
        .build()
}

fn bytes_of(datum: &Datum) -> Vec<u8> {
    match datum {
        Datum::String(value) => value.bytes().to_vec(),
        other => panic!("expected a string datum, got {other:?}"),
    }
}

#[test]
fn test_handle_bad_null() {
    // pkg/table/column_test.go::TestHandleBadNull is the source table:
    // nullable NULL is untouched, an error-level NOT NULL violation
    // fails, and warning level substitutes the type's zero.
    let nullable = FieldType::new(FieldTypeCode::Long);
    let context = crate::StmtContext::default();
    let mut value = Datum::Null;
    assert!(!handle_bad_null(&mut value, &nullable, "a", NullLevel::Error, &context,).unwrap());
    assert_eq!(value, Datum::Null);

    let not_null = nullable.with_flags(NOT_NULL_FLAG);
    assert!(
        (handle_bad_null(&mut Datum::Null, &not_null, "a", NullLevel::Error, &context,)
            .map_err(DriverError::from))
        .is_err_and(
            |error| error.equal(&tidb_error::catalog::table::ERR_COLUMN_CANT_NULL)
                && error.to_string()
                    == DriverError::from(
                        tidb_error::catalog::table::ERR_COLUMN_CANT_NULL
                            .generate_with_stack_by_args(&["a".into()])
                    )
                    .to_string()
        )
    );

    let mut warned = Datum::Null;
    assert!(handle_bad_null(&mut warned, &not_null, "a", NullLevel::Warn, &context,).unwrap());
    assert_eq!(warned, Datum::Int(0));
    assert_eq!(context.take_warnings().len(), 1);
}

#[test]
fn test_check() {
    // Direct port of pkg/table/column_test.go::TestCheck.  The source
    // combines table.CheckOnce with its local checkNotNull loop, so this
    // test deliberately crosses the model/executor ownership boundary.
    let mut column = ColumnInfo::new(1, "a", FieldType::new(FieldTypeCode::Long));
    column.add_flag(u64::from(FieldTypeFlags::AUTO_INCREMENT));
    let column = GoShared::new(column);
    let duplicate =
        GoSharedPointerSlice::from_handles(vec![Some(column.clone()), Some(column.clone())]);
    assert!(check_column_infos_once(&duplicate).is_err());

    let columns = GoSharedPointerSlice::from_handles(vec![Some(column.clone())]);
    let context = crate::StmtContext::default();
    let mut value = Datum::Null;
    let nullable = columns.get(0).unwrap().read().field_type.clone();
    assert!(!handle_bad_null(&mut value, &nullable, "a", NullLevel::Error, &context,).unwrap());

    columns
        .get(0)
        .unwrap()
        .write()
        .add_flag(u64::from(FieldTypeFlags::NOT_NULL));
    let not_null = columns.get(0).unwrap().read().field_type.clone();
    assert!(
        handle_bad_null(&mut Datum::Null, &not_null, "a", NullLevel::Error, &context,).is_err()
    );

    assert!(check_column_infos_once(&GoSharedPointerSlice::default()).is_ok());
}

#[test]
fn test_get_zero_value() {
    // Direct port of pkg/table/column_test.go::TestGetZeroValue. Expected
    // string datums use each FieldType's own collation, matching Go's
    // collator-based comparison instead of comparing collation metadata.
    let unsigned = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
    let fixed_binary = field(FieldTypeCode::String, 2, "binary", "binary");
    let fixed_utf8 = field(FieldTypeCode::String, 2, "utf8mb4", "binary");

    let string_zero = |field_type: &FieldType, bytes: Vec<u8>| {
        Datum::String(StringDatum::new(bytes, field_type.collation()))
    };
    let time_zero = |kind, field_type: &FieldType| {
        Datum::new_time(
            Time::new(CoreTime::from_raw(0), kind, field_type.decimal())
                .or_else(|_| Time::new(CoreTime::from_raw(0), kind, 0))
                .unwrap(),
        )
    };

    let signed = FieldType::new(FieldTypeCode::Long);
    let float = FieldType::new(FieldTypeCode::Float);
    let double = FieldType::new(FieldTypeCode::Double);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal);
    let varchar = FieldType::new(FieldTypeCode::Varchar);
    let blob = FieldType::new(FieldTypeCode::Blob);
    let duration = FieldType::new(FieldTypeCode::Duration);
    let datetime = FieldType::new(FieldTypeCode::Datetime);
    let timestamp = FieldType::new(FieldTypeCode::Timestamp);
    let date = FieldType::new(FieldTypeCode::Date);
    let bit = FieldType::new(FieldTypeCode::Bit);
    let set = FieldType::new(FieldTypeCode::Set);
    let enumeration = FieldType::new(FieldTypeCode::Enum);
    let json = FieldType::new(FieldTypeCode::Json);
    let json_null = BinaryJSON::from_typed_value(&BinaryJSONValue::Null)
        .map(Datum::new_json)
        .unwrap();

    let cases = vec![
        (signed, Datum::Int(0)),
        (unsigned, Datum::UInt(0)),
        (float, Datum::Float32(0.0)),
        (double, Datum::Real(0.0)),
        (decimal, Datum::new_decimal(Decimal::from_int(0))),
        (varchar.clone(), string_zero(&varchar, Vec::new())),
        (blob.clone(), string_zero(&blob, Vec::new())),
        (
            duration,
            Datum::new_duration(MySqlDuration::from_nanoseconds(0, 0).unwrap()),
        ),
        (datetime.clone(), time_zero(TimeType::DateTime, &datetime)),
        (
            timestamp.clone(),
            time_zero(TimeType::Timestamp, &timestamp),
        ),
        (date.clone(), time_zero(TimeType::Date, &date)),
        (bit, Datum::Bit(BinaryLiteral::from_uint(0, None))),
        (
            set.clone(),
            Datum::new_set(MysqlSet::new(String::new(), 0), set.collation()),
        ),
        (
            enumeration.clone(),
            Datum::new_enum(MysqlEnum::new(String::new(), 0), enumeration.collation()),
        ),
        (fixed_binary.clone(), string_zero(&fixed_binary, vec![0, 0])),
        (fixed_utf8.clone(), string_zero(&fixed_utf8, Vec::new())),
        (json, json_null),
    ];

    for (field_type, expected) in cases {
        assert_eq!(zero_value(&field_type), expected, "{field_type:?}");
    }
}

/// Go `table.GetZeroValue`'s `mysql.TypeString` arm. Captured from TiDB:
///
/// ```text
/// alter table t add column a binary(8) not null;
/// alter table t add column b char(4) charset binary not null;
/// alter table t add column c char(4) not null;
/// alter table t add column d varbinary(8) not null;
/// select hex(a),length(a),hex(b),length(b),hex(c),length(c),hex(d),length(d);
///   -> 0000000000000000|8|00000000|4||0||0
/// ```
#[test]
fn binary_zero_value_is_flen_zero_bytes() {
    assert_eq!(
        bytes_of(&zero_value(&field(
            FieldTypeCode::String,
            8,
            "binary",
            "binary"
        ))),
        vec![0u8; 8]
    );
    assert_eq!(
        bytes_of(&zero_value(&field(
            FieldTypeCode::String,
            4,
            "binary",
            "binary"
        ))),
        vec![0u8; 4]
    );
}

/// The three shapes that share the code but NOT the zero fill: a non-binary
/// CHAR, a zero-width BINARY, and VARBINARY (a separate Go arm entirely).
#[test]
fn only_fixed_width_binary_zero_fills() {
    assert!(bytes_of(&zero_value(&field(
        FieldTypeCode::String,
        4,
        "utf8mb4",
        "utf8mb4_bin"
    )))
    .is_empty());
    assert!(bytes_of(&zero_value(&field(
        FieldTypeCode::String,
        0,
        "binary",
        "binary"
    )))
    .is_empty());
    assert!(bytes_of(&zero_value(&field(
        FieldTypeCode::VarString,
        8,
        "binary",
        "binary"
    )))
    .is_empty());
}

/// Go `column.go`'s `mysql.TypeTiDBVectorFloat32` arm sets
/// `types.ZeroVectorFloat32`, not a NULL datum.
#[test]
fn vector_zero_value_is_the_empty_vector() {
    let zero = zero_value(&field(FieldTypeCode::VectorFloat32, 0, "binary", "binary"));
    match zero {
        Datum::VectorFloat32(value) => assert_eq!(value.len(), 0),
        other => panic!("expected a vector datum, got {other:?}"),
    }
}
