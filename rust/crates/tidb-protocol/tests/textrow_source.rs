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

#![allow(missing_docs)]

use tidb_protocol::{
    append_format_float, format_text_value, TextColumn, TextFormatError, TextScalar, TYPE_DATE,
    TYPE_DATETIME, TYPE_DOUBLE, TYPE_DURATION, TYPE_ENUM, TYPE_FLOAT, TYPE_GEOMETRY, TYPE_JSON,
    TYPE_LONGLONG, TYPE_NEW_DECIMAL, TYPE_SET, TYPE_TIMESTAMP, TYPE_YEAR, UNSIGNED_FLAG,
};

#[test]
fn test_append_format_float() {
    // Exact vectors from pkg/format/textrow/textrow_test.go::TestAppendFormatFloat.
    let cases = [
        (99999999999999999999.0, "1e20", -1, 64),
        (1e15, "1e15", -1, 64),
        (9e14, "900000000000000", -1, 64),
        (-9999999999999999.0, "-1e16", -1, 64),
        (999999999999999.0, "999999999999999", -1, 64),
        (0.000000000000001, "0.000000000000001", -1, 64),
        (0.0000000000000009, "9e-16", -1, 64),
        (-0.0000000000000009, "-9e-16", -1, 64),
        (0.11111, "0.111", 3, 64),
        (0.111_111_111_111_111_1, "0.11111111", -1, 32),
        (0.111_111_111_111_111_1, "0.1111111111111111", -1, 64),
        (0.0000000000000009, "9e-16", 3, 64),
        (0.0, "0", -1, 64),
        (
            -340282346638528860000000000000000000000.0,
            "-3.40282e38",
            -1,
            32,
        ),
        (-34028236.0, "-34028236.00", 2, 32),
        (-17976921.34, "-17976921.34", 2, 64),
        (-3.402823466e38, "-3.40282e38", -1, 32),
        (-1.7976931348623157e308, "-1.7976931348623157e308", -1, 64),
        (10.0e20, "1e21", -1, 32),
        (1e20, "1e20", -1, 32),
        (10.0, "10", -1, 32),
        (999999986991104.0, "1e15", -1, 32),
        (1e15, "1e15", -1, 32),
        (f64::INFINITY, "0", -1, 64),
        (f64::NEG_INFINITY, "0", -1, 64),
        (1e14, "100000000000000", -1, 64),
        (1e308, "1e308", -1, 64),
    ];

    for (value, expected, precision, bit_size) in cases {
        let mut got = Vec::new();
        append_format_float(&mut got, value, precision, bit_size);
        assert_eq!(
            got,
            expected.as_bytes(),
            "value={value} precision={precision} bit_size={bit_size}"
        );
    }
}

#[test]
fn source_scalar_formatter_keeps_numeric_and_byte_boundaries() {
    for type_code in [1, 2, 9, 3] {
        assert_eq!(
            format_text_value(TextColumn::new(type_code), TextScalar::Signed(-10)).unwrap(),
            Some(b"-10".to_vec()),
            "signed integer type={type_code}"
        );
    }
    assert_eq!(
        format_text_value(TextColumn::new(TYPE_LONGLONG), TextScalar::Signed(-10)).unwrap(),
        Some(b"-10".to_vec())
    );
    assert_eq!(
        format_text_value(
            TextColumn {
                type_code: TYPE_LONGLONG,
                flag: UNSIGNED_FLAG,
                ..TextColumn::new(TYPE_LONGLONG)
            },
            TextScalar::Unsigned(11),
        )
        .unwrap(),
        Some(b"11".to_vec())
    );
    assert_eq!(
        format_text_value(TextColumn::new(TYPE_YEAR), TextScalar::Signed(0)).unwrap(),
        Some(b"0000".to_vec())
    );
    for (decimal, expected) in [
        (b"-1.2300".as_slice(), b"-1.2300".as_slice()),
        (b"0.00".as_slice(), b"0.00".as_slice()),
    ] {
        assert_eq!(
            format_text_value(
                TextColumn::new(TYPE_NEW_DECIMAL),
                TextScalar::Decimal(decimal)
            )
            .unwrap(),
            Some(expected.to_vec()),
            "decimal={decimal:?}"
        );
    }
    assert_eq!(
        format_text_value(
            TextColumn {
                type_code: TYPE_DOUBLE,
                decimal: 2,
                ..TextColumn::new(TYPE_DOUBLE)
            },
            TextScalar::Float {
                value: 2.2,
                bit_size: 64,
            },
        )
        .unwrap(),
        Some(b"2.20".to_vec())
    );
    assert_eq!(
        format_text_value(
            TextColumn::new(tidb_protocol::TYPE_VARCHAR),
            TextScalar::Bytes(&[0xff, 0]),
        )
        .unwrap(),
        Some(vec![0xff, 0])
    );
    assert_eq!(
        format_text_value(TextColumn::new(TYPE_FLOAT), TextScalar::Null).unwrap(),
        None
    );
}

#[test]
fn source_scalar_formatter_rejects_mismatched_and_unported_branches() {
    assert_eq!(
        format_text_value(TextColumn::new(TYPE_NEW_DECIMAL), TextScalar::Signed(1)).unwrap_err(),
        TextFormatError::ScalarTypeMismatch(TYPE_NEW_DECIMAL)
    );
    assert_eq!(
        format_text_value(
            TextColumn::new(tidb_protocol::TYPE_VARCHAR),
            TextScalar::Signed(1),
        )
        .unwrap_err(),
        TextFormatError::ScalarTypeMismatch(tidb_protocol::TYPE_VARCHAR)
    );

    for type_code in [
        TYPE_DATE,
        TYPE_DATETIME,
        TYPE_TIMESTAMP,
        TYPE_DURATION,
        TYPE_ENUM,
        TYPE_SET,
        TYPE_JSON,
    ] {
        assert_eq!(
            format_text_value(TextColumn::new(type_code), TextScalar::Bytes(b"not-ported"))
                .unwrap_err(),
            TextFormatError::UnsupportedType(type_code),
            "unported type={type_code}"
        );
    }
}

#[test]
fn test_format_value_text_invalid_type() {
    let error =
        format_text_value(TextColumn::new(TYPE_GEOMETRY), TextScalar::Signed(1)).unwrap_err();
    assert_eq!(error, TextFormatError::UnsupportedType(TYPE_GEOMETRY));
    assert!(error.to_string().contains("invalid column type"));
}
