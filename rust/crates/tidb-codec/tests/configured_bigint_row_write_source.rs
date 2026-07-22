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

//! Go-byte-exact persisted rows for the configured clustered signed-`BIGINT`
//! table.
//!
//! Every vector below was produced by TiDB's own owners — `tablecodec`
//! `EncodeRowKeyWithHandle` and `rowcodec` `Encoder.Encode` — through
//! `generate_configured_rows.go`. This covers the exact obligations named by
//! `pkg/tablecodec/tablecodec_test.go` `TestRecordKey`/`TestRowCodec` and
//! `pkg/util/rowcodec/rowcodec_test.go` `TestTypesNewRowCodec`/
//! `TestVarintCompatibility`/`TestColumnEncode` for this one row shape; every
//! other type, index, checksum, NULL, default, and old-format branch remains a
//! named gap in the ledgers.

use tidb_codec::{
    decode_configured_row_bytes, decode_configured_row_int, encode_configured_mixed_row,
    encode_configured_row, encode_configured_row_value, encode_configured_row_value_typed,
    ConfiguredRowColumn, ConfiguredRowReadError, ConfiguredRowWriteError, ConfiguredValue,
};

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/configured_rows.hex");

const TABLE_ID: i64 = 114;
const BALANCE_COLUMN: i64 = 2;

#[test]
fn record_keys_match_go_across_the_signed_handle_domain() {
    for handle in [i64::MIN, -1, 0, 1, 10, 11, i64::MAX] {
        let (key, _) = encode_configured_row(
            TABLE_ID,
            handle,
            &[ConfiguredRowColumn::new(BALANCE_COLUMN, 1)],
        )
        .expect("configured row must encode");
        assert_eq!(key, fixture(&format!("key_{handle}")), "handle {handle}");
    }
}

#[test]
fn stored_column_values_match_go_at_every_compact_width_transition() {
    for balance in [
        i64::MIN,
        i64::from(i32::MIN) - 1,
        i64::from(i32::MIN),
        i64::from(i16::MIN) - 1,
        i64::from(i16::MIN),
        i64::from(i8::MIN) - 1,
        i64::from(i8::MIN),
        -1,
        0,
        1,
        100,
        i64::from(i8::MAX),
        i64::from(i8::MAX) + 1,
        i64::from(i16::MAX),
        i64::from(i16::MAX) + 1,
        i64::from(i32::MAX),
        i64::from(i32::MAX) + 1,
        i64::MAX,
    ] {
        let value =
            encode_configured_row_value(&[ConfiguredRowColumn::new(BALANCE_COLUMN, balance)])
                .expect("configured row value must encode");
        assert_eq!(
            value,
            fixture(&format!("value_balance_{balance}")),
            "balance {balance}"
        );
        assert_eq!(
            decode_configured_row_int(&value, BALANCE_COLUMN),
            Ok(balance)
        );
    }
}

#[test]
fn not_null_column_ids_are_sorted_exactly_as_go_sorts_them() {
    // Supplied out of ID order; Go sorts its not-null partition by column ID,
    // so a statement's column order can never change the persisted bytes.
    let value = encode_configured_row_value(&[
        ConfiguredRowColumn::new(BALANCE_COLUMN + 1, -7),
        ConfiguredRowColumn::new(BALANCE_COLUMN, 100),
    ])
    .expect("configured row value must encode");
    assert_eq!(value, fixture("value_two_columns_unsorted"));
    assert_eq!(decode_configured_row_int(&value, BALANCE_COLUMN), Ok(100));
    assert_eq!(
        decode_configured_row_int(&value, BALANCE_COLUMN + 1),
        Ok(-7)
    );
}

#[test]
fn a_column_id_above_one_byte_takes_gos_large_row_format() {
    let value = encode_configured_row_value(&[ConfiguredRowColumn::new(256, 100)])
        .expect("configured row value must encode");
    assert_eq!(value, fixture("value_large_column_id"));
    assert_eq!(decode_configured_row_int(&value, 256), Ok(100));
}

#[test]
fn the_clustered_handle_lives_only_in_the_key() {
    // Go's `tables.CanSkip` skips `col.IsPKHandleColumn`, so the row value for
    // the live campaign-28 fixture carries `balance` and nothing else.
    let (key, value) = encode_configured_row(
        TABLE_ID,
        10,
        &[ConfiguredRowColumn::new(BALANCE_COLUMN, 100)],
    )
    .expect("configured row must encode");
    assert_eq!(key, fixture("key_accounts_id10"));
    assert_eq!(value, fixture("value_accounts_id10_balance100"));

    // The handle's own column ID is absent from the value; asking for it is a
    // missing column rather than a silently decoded zero.
    assert_eq!(
        decode_configured_row_int(&value, 1),
        Err(ConfiguredRowReadError::MissingColumn(1))
    );
}

#[test]
fn invalid_configured_rows_never_produce_bytes() {
    assert_eq!(
        encode_configured_row_value(&[]),
        Err(ConfiguredRowWriteError::NoStoredColumns)
    );
    assert_eq!(
        encode_configured_row_value(&[
            ConfiguredRowColumn::new(BALANCE_COLUMN, 1),
            ConfiguredRowColumn::new(BALANCE_COLUMN, 2),
        ]),
        Err(ConfiguredRowWriteError::DuplicateColumnId(BALANCE_COLUMN))
    );
    assert_eq!(
        encode_configured_row_value(&[ConfiguredRowColumn::new(0, 1)]),
        Err(ConfiguredRowWriteError::ColumnIdOutOfRange(0))
    );
    assert_eq!(
        encode_configured_row_value(&[ConfiguredRowColumn::new(-1, 1)]),
        Err(ConfiguredRowWriteError::ColumnIdOutOfRange(-1))
    );
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
        other => panic!("fixture has a non-hex byte {other:#x}"),
    }
}

// -----------------------------------------------------------------------------
// String (CHAR) column values — no-restored-data (utf8mb4_bin) storage
// -----------------------------------------------------------------------------

const STRING_COLUMN: i64 = 5;

#[test]
fn string_column_values_match_go_no_restored_data_storage() {
    // Go stores a default-collation CHAR value as its raw bytes addressed by the
    // offset table: no length prefix, no restored-collation data, no trailing
    // space trimming.
    for (name, text) in [
        ("value_char_empty", ""),
        ("value_char_hello", "hello"),
        ("value_char_multibyte", "héllo😀"),
        ("value_char_spaces", "ab  "),
    ] {
        let value = encode_configured_row_value_typed(&[(
            STRING_COLUMN,
            ConfiguredValue::Bytes(text.as_bytes().to_vec()),
        )])
        .expect("string row value must encode");
        assert_eq!(value, fixture(name), "string vector {name}");
        assert_eq!(
            decode_configured_row_bytes(&value, STRING_COLUMN),
            Ok(text.as_bytes().to_vec()),
            "round-trip {name}"
        );
    }
}

#[test]
fn a_mixed_int_and_string_row_encodes_each_column_in_its_own_domain() {
    // A row with one INT-like signed column and one string column: the offset
    // table keeps them independent, and each decodes in its own domain.
    let (key, value) = encode_configured_mixed_row(
        114,
        10,
        &[
            (2, ConfiguredValue::Int(100)),
            (STRING_COLUMN, ConfiguredValue::Bytes(b"hello".to_vec())),
        ],
    )
    .expect("mixed row must encode");
    assert_eq!(key, fixture("key_accounts_id10"));
    assert_eq!(decode_configured_row_int(&value, 2), Ok(100));
    assert_eq!(
        decode_configured_row_bytes(&value, STRING_COLUMN),
        Ok(b"hello".to_vec())
    );
}

#[test]
fn an_empty_string_is_present_not_null_or_missing() {
    let value =
        encode_configured_row_value_typed(&[(STRING_COLUMN, ConfiguredValue::Bytes(Vec::new()))])
            .expect("empty string row value must encode");
    assert_eq!(value, fixture("value_char_empty"));
    assert_eq!(
        decode_configured_row_bytes(&value, STRING_COLUMN),
        Ok(Vec::new()),
        "an empty CHAR is a present zero-length value"
    );
}
