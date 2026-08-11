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

//! Row-for-row translations of `pkg/types/parser_driver/value_expr_test.go`.

use tidb_datatype::{
    unwrap_from_single_quotes, wrap_in_single_quotes, BinaryLiteral, CoreTime, Datum, Decimal,
    MySqlDuration, Time, TimeType,
};

fn binary_literal() -> Datum {
    Datum::new_binary_literal(BinaryLiteral::from(b"test `s't\"r."))
}

#[test]
fn test_value_expr_restore() {
    let rows = [
        (Datum::Null, "NULL"),
        (Datum::new_int(1), "1"),
        (Datum::new_int(-1), "-1"),
        (Datum::new_uint(1), "1"),
        (Datum::new_float32_from_f64(1.1), "1.1e+00"),
        (Datum::new_real(1.1), "1.1e+00"),
        (
            Datum::new_string("test `s't\"r."),
            "'test `s''t\"r.'",
        ),
        (
            Datum::new_bytes(b"test `s't\"r.".to_vec()),
            "'test `s''t\"r.'",
        ),
        (
            binary_literal(),
            "b'11101000110010101110011011101000010000001100000011100110010011101110100001000100111001000101110'",
        ),
        (Datum::new_decimal(Decimal::from_literal("321")), "321"),
        (Datum::new_duration(MySqlDuration::default()), "'00:00:00'"),
        (
            Datum::new_time(
                Time::new(CoreTime::default(), TimeType::DateTime, 0)
                    .expect("zero datetime is valid"),
            ),
            "'0000-00-00 00:00:00'",
        ),
        (Datum::new_string("\\"), "'\\\\'"),
    ];

    for (datum, expected) in rows {
        assert_eq!(datum.restore_value_expr().unwrap(), expected.as_bytes());
    }
}

#[test]
fn test_value_expr_format() {
    let rows = [
        (Datum::Null, "NULL"),
        (Datum::new_int(1), "1"),
        (Datum::new_int(-1), "-1"),
        (Datum::new_uint(1), "1"),
        (Datum::new_float32_from_f64(1.1), "1.1e+00"),
        (Datum::new_real(1.1), "1.1e+00"),
        (
            Datum::new_string("test `s't\"r."),
            "'test `s''t\"r.'",
        ),
        (
            Datum::new_bytes(b"test `s't\"r.".to_vec()),
            "'test `s''t\"r.'",
        ),
        (
            binary_literal(),
            "b'11101000110010101110011011101000010000001100000011100110010011101110100001000100111001000101110'",
        ),
        (Datum::new_decimal(Decimal::from_literal("321")), "321"),
        (Datum::new_string("\\"), "'\\\\'"),
        (Datum::new_string("''"), "''''''"),
        (Datum::new_string("\\''\t\n"), "'\\\\''''\t\n'"),
    ];

    for (datum, expected) in rows {
        assert_eq!(datum.format_value_expr().unwrap(), expected.as_bytes());
    }
}

#[test]
fn single_quote_helpers_are_byte_preserving_inverses() {
    let rows: &[&[u8]] = &[
        b"plain",
        b"a'b",
        br"a\b",
        br"\''",
        b"\xff\0'\\",
    ];
    for value in rows {
        let wrapped = wrap_in_single_quotes(value);
        assert_eq!(unwrap_from_single_quotes(&wrapped), *value);
    }

    assert_eq!(wrap_in_single_quotes(br"a\'b"), br"'a\\''b'");
    assert_eq!(unwrap_from_single_quotes(b"not quoted"), b"not quoted");
    assert_eq!(unwrap_from_single_quotes(b"'unterminated"), b"'unterminated");
}
