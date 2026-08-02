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

//! Go-authoritative row-v2 vectors (task #190).
//!
//! Every expected byte string in this file was produced by
//! `rust/difftests/transaction-tests/fixtures/generate_rowv2_vectors.go`
//! running against this repository's Go tree; each test names the Go function
//! that produced its bytes. The rest of the row-v2 suite only self-round-trips
//! (encode ours, decode ours), which cannot detect a symmetric encoder/decoder
//! divergence from TiDB.

use tidb_codec::table_key::{encode_index_seek_key, encode_row_key_with_handle, RecordHandle};
use tidb_codec::{
    decode_row_to_datums, encode_hash_datum, encode_key, encode_row, ColumnInfo, DecodeRowOptions,
    RawRowValue, RowDecoder, ROW_FLAG_LARGE,
};
use tidb_datatype::{
    BinaryJSON, BinaryLiteral, Collation, CoreTime, Datum, FieldType, FieldTypeCode, MySqlDuration,
    MysqlEnum, MysqlSet, SessionTimeZone, Time, TimeType,
};

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/rowv2_vectors.hex");

fn fixture(name: &str) -> Vec<u8> {
    let prefix = format!("{name}=");
    let hex = FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"));
    assert!(hex.len().is_multiple_of(2), "{name} is not whole bytes");
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect()
}

fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        other => panic!("fixture has non-hex byte {other:#x}"),
    }
}

fn column(id: i64, field_type: FieldType) -> ColumnInfo {
    ColumnInfo {
        id,
        is_pk_handle: false,
        virtual_generated: false,
        field_type,
    }
}

fn encode(ids: &[i64], values: &[Datum]) -> Vec<u8> {
    let utc = SessionTimeZone::utc();
    let mut encoded = Vec::new();
    encode_row(Some(&utc), ids, values, &mut encoded).unwrap();
    encoded
}

fn decode(row: &[u8], columns: &[ColumnInfo]) -> Vec<Datum> {
    let utc = SessionTimeZone::utc();
    decode_row_to_datums(
        row,
        columns,
        &DecodeRowOptions {
            timezone: Some(&utc),
            ..DecodeRowOptions::default()
        },
    )
    .unwrap()
    .values
}

/// Go source: `hashIntBoundary` / `emitHash`, over
/// `codec.EncodeHashChunkRowIdx` (pkg/util/codec/codec.go).
///
/// The three vectors sit exactly on the signed/unsigned 64-bit boundary that
/// the hash-int-tag fix (f4517b7a92) touched: an unsigned column whose stored
/// word reads negative as `i64` must still hash under `uvarintFlag`.
#[test]
fn hash_int_tag_matches_go_at_the_sign_boundary() {
    let unsigned = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
    let signed = FieldType::new(FieldTypeCode::LongLong);

    let (flag, bytes) = encode_hash_datum(&Datum::UInt(u64::MAX), &unsigned).unwrap();
    assert_eq!(vec![flag], fixture("hash_u64_max_unsigned_flag"));
    assert_eq!(bytes, fixture("hash_u64_max_unsigned_bytes"));

    let (flag, bytes) = encode_hash_datum(&Datum::UInt(1 << 63), &unsigned).unwrap();
    assert_eq!(vec![flag], fixture("hash_i64_max_plus_1_unsigned_flag"));
    assert_eq!(bytes, fixture("hash_i64_max_plus_1_unsigned_bytes"));

    // Go's chunk stores an unsigned column as a raw 64-bit word, so the same
    // value can reach the hash encoder as a negative `i64`. It must still
    // produce the unsigned vector above; this is the case f4517b7a92 fixed.
    let (flag, bytes) = encode_hash_datum(&Datum::Int(i64::MIN), &unsigned).unwrap();
    assert_eq!(vec![flag], fixture("hash_i64_max_plus_1_unsigned_flag"));
    assert_eq!(bytes, fixture("hash_i64_max_plus_1_unsigned_bytes"));

    let (flag, bytes) = encode_hash_datum(&Datum::Int(i64::MAX), &signed).unwrap();
    assert_eq!(vec![flag], fixture("hash_i64_max_signed_flag"));
    assert_eq!(bytes, fixture("hash_i64_max_signed_bytes"));
}

/// Go source: `nullBitmapSplit`, over `rowcodec.Encoder.Encode`
/// (pkg/util/rowcodec/encoder.go). One row carries a non-null column ID below
/// 256 and a null column ID above 255, so the large-ID promotion and the
/// null partition are exercised together.
#[test]
fn null_bitmap_with_small_and_large_column_ids_matches_go() {
    let ids = [5_i64, 300];
    let values = [Datum::Int(7), Datum::Null];
    let expected = fixture("row_null_bitmap_small_large");
    assert_eq!(encode(&ids, &values), expected);

    let columns = [
        column(5, FieldType::new(FieldTypeCode::LongLong)),
        column(300, FieldType::new(FieldTypeCode::LongLong)),
    ];
    assert_eq!(decode(&expected, &columns), values);
}

/// Go source: `bigRow`, over `rowcodec.Encoder.Encode`. 256 columns push the
/// row past Go's 255-column small-row cap and set the `isBig` row flag.
#[test]
fn two_hundred_fifty_six_column_row_sets_is_big_like_go() {
    let ids: Vec<i64> = (1..=256).collect();
    let values: Vec<Datum> = (0..256).map(Datum::Int).collect();
    let expected = fixture("row_256_columns_is_big");
    assert_eq!(encode(&ids, &values), expected);

    // Byte 1 is Go's row flags; `isBig` is the low bit.
    assert_eq!(expected[1] & ROW_FLAG_LARGE, ROW_FLAG_LARGE);

    let columns: Vec<ColumnInfo> = ids
        .iter()
        .map(|id| column(*id, FieldType::new(FieldTypeCode::LongLong)))
        .collect();
    assert_eq!(decode(&expected, &columns), values);
}

/// Go source: `fixedWidthTypes`, over `rowcodec.Encoder.Encode`. Every
/// fixed-width column kind once, including `-0.0`.
///
/// The generator also pins a refusal: TiDB has no validated path that stores a
/// NaN float in a row, so no NaN vector exists and none is asserted here.
#[test]
fn fixed_width_column_kinds_match_go() {
    let time = Time::new(
        CoreTime::from_date(2026, 8, 2, 12, 34, 56, 0),
        TimeType::DateTime,
        0,
    )
    .unwrap();
    let values = vec![
        Datum::Int(-42),
        Datum::UInt(42),
        Datum::Real(3.5),
        Datum::Real(-0.0),
        Datum::Duration(MySqlDuration::new(12, 34, 56, 0, 0).unwrap()),
        Datum::Time(time),
        Datum::Enum(MysqlEnum::new("b", 2), Collation::DEFAULT),
        Datum::Set(MysqlSet::new("x,y", 0b11), Collation::DEFAULT),
        Datum::Bit(BinaryLiteral::from(vec![0x05])),
        Datum::Json(BinaryJSON::parse(r#"{"a":1}"#).unwrap()),
    ];
    let ids: Vec<i64> = (1..=10).collect();
    let expected = fixture("row_fixed_width_all_types");
    assert_eq!(encode(&ids, &values), expected);

    let columns = vec![
        column(1, FieldType::new(FieldTypeCode::LongLong)),
        column(
            2,
            FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
        ),
        column(3, FieldType::new(FieldTypeCode::Double)),
        column(4, FieldType::new(FieldTypeCode::Double)),
        column(
            5,
            FieldType::new(FieldTypeCode::Duration).with_decimal(0),
        ),
        column(6, FieldType::new(FieldTypeCode::Datetime)),
        column(
            7,
            FieldType::new(FieldTypeCode::Enum)
                .with_elems(["a", "b"])
                .with_collation(Collation::DEFAULT),
        ),
        column(
            8,
            FieldType::new(FieldTypeCode::Set)
                .with_elems(["x", "y"])
                .with_collation(Collation::DEFAULT),
        ),
        column(9, FieldType::new(FieldTypeCode::Bit).with_flen(8)),
        column(10, FieldType::new(FieldTypeCode::Json)),
    ];
    let decoded = decode(&expected, &columns);
    assert_eq!(decoded, values);

    // `-0.0` is stored as `8000000000000000` and read back as `+0.0`: Go's
    // `encodeFloatToCmpUint64` takes the non-negative branch for `-0.0`
    // (`-0.0 < 0` is false) and leaves the sign bit set, then
    // `decodeCmpUintToFloat` clears that same bit. The sign is lost inside
    // TiDB itself, verified by round-tripping `-0.0` through
    // `rowcodec.Encoder.Encode` + `DatumMapDecoder.DecodeToDatumMap`. Pin the
    // loss rather than "fixing" it away from the source.
    let (decoder, _) = RowDecoder::parse(&expected).unwrap();
    let RawRowValue::NotNull { bytes, .. } = decoder.column(4).unwrap() else {
        panic!("column 4 must be present and non-null");
    };
    assert_eq!(bytes, 0x8000_0000_0000_0000_u64.to_be_bytes());
    assert!(!decoded[3].as_real().unwrap().is_sign_negative());
}

/// Go source: `commonHandlePadding`, over `kv.NewCommonHandle` (pkg/kv/key.go)
/// and `tablecodec.EncodeRowKeyWithHandle`. The short handle is the same
/// decimal-`1` encoding Go's own `TestPaddingHandle` uses; it is zero-padded to
/// 9 bytes, while a handle already at or above 9 bytes passes through.
#[test]
fn common_handle_padding_matches_go_on_both_sides_of_nine_bytes() {
    let short = fixture("common_handle_short_raw");
    assert!(short.len() < 9);
    let handle = RecordHandle::Common(short);
    assert_eq!(handle.encoded().len(), 9);
    assert_eq!(
        encode_row_key_with_handle(42, &handle),
        fixture("common_handle_short_padded_key")
    );

    let long = fixture("common_handle_long_raw");
    assert!(long.len() >= 9);
    let handle = RecordHandle::Common(long.clone());
    assert_eq!(handle.encoded(), long);
    assert_eq!(
        encode_row_key_with_handle(42, &handle),
        fixture("common_handle_long_key")
    );
}

/// Go source: `indexKeyWithRestoredData`, over `tablecodec.EncodeIndexSeekKey`
/// for the key and the `idxValNeedRestoredData` branch of
/// `genIndexValueVersion0` (pkg/tablecodec/tablecodec.go) for the value.
///
/// The Rust `decode_index_kv` equivalent has no production caller yet, so this
/// asserts the key encoder and decodes the restored-data payload directly
/// rather than wiring a new decode path.
#[test]
fn index_key_and_restored_data_value_match_go() {
    let encoded_values = encode_key(&[Datum::new_bytes(b"abc".to_vec())]).unwrap();
    assert_eq!(
        encode_index_seek_key(42, 7, &encoded_values),
        fixture("index_key_restored_data_key")
    );

    // Index value version 0 with restored data: one `tailLen` byte, then a
    // row-v2 payload of the restored columns, zero-padded to 10 bytes.
    let value = fixture("index_key_restored_data_value");
    let tail_len = usize::from(value[0]);
    let row = &value[1..value.len() - tail_len];
    let columns = [column(
        1,
        FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Binary),
    )];
    assert_eq!(
        decode(row, &columns),
        vec![Datum::new_collation_string(b"abc", Collation::Binary)]
    );
}
