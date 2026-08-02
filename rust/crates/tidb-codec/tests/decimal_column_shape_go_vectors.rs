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

//! Go-produced row-v2 bytes for decimals that carry a declared column shape.
//!
//! A round trip cannot see this bug. `11.99` encoded at its natural `(4, 2)`
//! decodes back to `11.99`, so all fifteen self-round-trip row tests passed
//! while every decimal column on disk differed from TiDB's bytes and every
//! row/TiCDC checksum computed over them disagreed
//! (`rust/docs/codec-divergence-inventory.md` F13). Only bytes Go wrote can
//! fail here, which is why this fixture exists.
//!
//! Fixture: `generate_decimal_column_rows.go` beside the `.hex`.

use tidb_codec::{decode_row_to_map, encode_row, ColumnInfo};
use tidb_datatype::{
    Datum, Decimal, FieldType, FieldTypeCode, SessionTimeZone, DEFAULT_STATEMENT_FLAGS,
};

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/decimal_column_rows.hex");

/// A `DECIMAL(M, D)` column value is stored at the DECLARED shape.
///
/// The datum is built through the same conversion the write path uses rather
/// than stamped by hand, so the assertion covers the stamping site too.
#[test]
fn declared_decimal_column_row_is_go_byte_exact() {
    let value = column_value("11.99", 10, 4);
    assert_eq!(value.as_decimal().unwrap().declared_shape(), Some((10, 4)));
    // The value itself never learns the column shape: converting to
    // `DECIMAL(10, 4)` rounds `11.99` to scale 4, so its OWN shape is `(6, 4)`
    // — still not the `(10, 4)` the bytes must be written under.
    assert_eq!(value.as_decimal().unwrap().precision_and_frac(), (6, 4));

    let encoded = encode(&[1], &[value]);
    assert_eq!(encoded, fixture("row_decimal_10_4_11_99"));
    // `07 00` value offset: a 7-byte payload, `0a 04` header plus five bytes of
    // `WriteBin(10, 4)`. The natural shape would have written four.
    assert_eq!(&encoded[encoded.len() - 7..], &[0x0a, 0x04, 0x80, 0x00, 0x0b, 0x26, 0xac]);

    assert_eq!(decode(&encoded, &[decimal_column(1, 10, 4)]), ["11.9900"]);
}

/// A decimal that no column produced — a computed expression result — keeps
/// its natural shape, which is Go's unset `Datum.length` reaching
/// `EncodeDecimal`'s `precision == 0` branch.
#[test]
fn natural_decimal_row_is_go_byte_exact() {
    let value = Decimal::from_literal("11.99");
    assert_eq!(value.declared_shape(), None);
    assert_eq!(value.storage_shape(), (0, 0));

    let encoded = encode(&[1], &[Datum::new_decimal(value)]);
    assert_eq!(encoded, fixture("row_decimal_natural_11_99"));
    assert_eq!(&encoded[encoded.len() - 4..], &[0x04, 0x02, 0x8b, 0x63]);

    // Read back through the same `DECIMAL(10, 4)` column: the scale that
    // survives is the one in the payload header, not the column's.
    assert_eq!(decode(&encoded, &[decimal_column(1, 10, 4)]), ["11.99"]);
}

/// Both paths inside one row, so a fix that stamps everything (or nothing)
/// fails here even if each single-column vector were made to pass.
#[test]
fn mixed_declared_and_natural_decimal_row_is_go_byte_exact() {
    let stamped = column_value("11.99", 10, 4);
    let natural = Datum::new_decimal(Decimal::from_literal("11.99"));
    assert_eq!(
        encode(&[1, 2], &[stamped, natural]),
        fixture("row_decimal_mixed_11_99")
    );
}

/// A second declared shape, so the pin is not a single point: `DECIMAL(20, 10)`
/// crosses into a two-word `WriteBin` payload and carries a sign.
#[test]
fn wide_negative_declared_decimal_row_is_go_byte_exact() {
    let value = column_value("-0.5", 20, 10);
    assert_eq!(
        encode(&[1], &[value]),
        fixture("row_decimal_20_10_neg_0_5")
    );
}

fn column_value(literal: &str, flen: i64, decimal: i64) -> Datum {
    Datum::new_decimal(Decimal::from_signed_literal(literal))
        .convert_to(&decimal_type(flen, decimal), DEFAULT_STATEMENT_FLAGS)
        .unwrap()
        .value
}

fn decimal_type(flen: i64, decimal: i64) -> FieldType {
    FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(flen)
        .with_decimal(decimal)
}

fn decimal_column(id: i64, flen: i64, decimal: i64) -> ColumnInfo {
    ColumnInfo {
        id,
        is_pk_handle: false,
        virtual_generated: false,
        field_type: decimal_type(flen, decimal),
    }
}

fn encode(ids: &[i64], values: &[Datum]) -> Vec<u8> {
    let utc = SessionTimeZone::utc();
    let mut buffer = Vec::new();
    encode_row(Some(&utc), ids, values, &mut buffer).unwrap();
    buffer
}

fn decode(row: &[u8], columns: &[ColumnInfo]) -> Vec<String> {
    let utc = SessionTimeZone::utc();
    let decoded = decode_row_to_map(row, columns, Some(&utc)).unwrap();
    decoded
        .values()
        .map(|datum| datum.as_decimal().unwrap().to_string())
        .collect()
}

fn fixture(name: &str) -> Vec<u8> {
    let prefix = format!("{name}=");
    let hex = FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"));
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect()
}

fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("non-hex fixture byte {byte:#x}"),
    }
}
