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

//! Exact root-package test obligations from Go `pkg/tablecodec`.

use std::collections::BTreeMap;

use chrono_tz::UTC;
use tidb_tablecodec::table_key::{
    cut_index_prefix, cut_row_key_prefix, decode_index_id, decode_index_key, decode_key_head,
    decode_meta_key, decode_record_key, decode_row_key, encode_index_seek_key, encode_meta_key,
    encode_meta_key_prefix, encode_record_key, encode_row_key_with_handle,
    encode_table_index_prefix, encode_table_prefix, gen_table_index_prefix, gen_table_prefix,
    gen_table_record_prefix, get_table_handle_key_range, truncate_to_row_key_len, KeyHead,
    RecordHandle, META_PREFIX, RECORD_ROW_KEY_LEN, TABLE_PREFIX,
};
use tidb_codec::{
    cut_one, decode_decimal_with_fault, decode_int, decode_one, decode_table_id, encode_int,
    encode_key, encode_row, encode_value, Encoder,
};
use tidb_tablecodec::{
    cut_index_key, cut_index_key_by_ids, cut_table_row, decode_column_value,
    decode_handle_in_index_value, decode_index_handle, decode_table_row_into_map,
    decode_table_row_to_map, decode_temp_index_value, encode_handle_in_unique_index_value,
    encode_old_table_row, encode_table_row, encode_table_value,
    filter_overwritten_temp_index_values, generate_index_key, generate_index_value,
    get_table_index_key_range,
    index_key_to_temp_index_key, index_kv_is_unique, is_index_key, is_record_key, is_table_key,
    is_temp_index_key, is_untouched_index_kv, split_index_value, temp_index_key_to_index_key,
    temp_index_value_is_untouched, truncate_index_value, unflatten_datum, unflatten_datums,
    verify_table_ids_for_ranges, IndexColumn, IndexInfo, TableColumn, TableInfo, TableKeyRange,
    TempIndexValue, TempIndexValueElem, COMMON_HANDLE_FLAG, INDEX_ID_MASK, INDEX_VERSION_FLAG,
    PARTITION_ID_FLAG, UNCOMMITTED_INDEX_KV_FLAG,
};
use tidb_datatype::{
    BinaryLiteral, BinaryLiteralWidth, Collation, CoreTime, Datum, Decimal, FieldType,
    FieldTypeCode, MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType, UNSPECIFIED_LENGTH,
};
use tidb_txnkv::{CommonHandle, Handle, IntHandle, PartitionHandle};

fn int_handle(value: i64) -> Handle {
    IntHandle::new(value).into()
}

fn common_handle(encoded: Vec<u8>) -> Handle {
    CommonHandle::new(encoded).unwrap().into()
}

fn partition_handle(partition_id: i64, handle: Handle) -> Handle {
    PartitionHandle::new(partition_id, handle).into()
}

fn field(code: FieldTypeCode) -> FieldType {
    FieldType::new(code)
}

fn table_column(id: i64, offset: usize, code: FieldTypeCode) -> TableColumn {
    TableColumn {
        id,
        offset,
        field_type: field(code),
        primary_key: false,
        changing_field_type: None,
    }
}

/// Source: `main_test.go::TestMain`.
///
/// Go installs process-global collation state and leaktest around the package.
/// Rust carries collation mode in `Encoder`, owns allocations through RAII,
/// and the aggregate test process therefore needs no mutable setup/cleanup.
#[test]
fn test_main() {
    assert!(Encoder::new(true).use_new_collation());
    assert!(!Encoder::new(false).use_new_collation());
}

/// Source: `tablecodec_test.go::TestTableCodec`.
#[test]
fn test_table_codec() {
    let key = encode_row_key_with_handle(1, &RecordHandle::Int(2));
    assert_eq!(decode_row_key(&key).unwrap(), RecordHandle::Int(2));
    let mut encoded = Vec::new();
    encode_int(&mut encoded, 2);
    assert_eq!(tidb_codec::encode_row_key(1, &encoded), key);
}

/// Source: `tablecodec_test.go::TestTableCodecInvalid`.
#[test]
fn test_table_codec_invalid() {
    let mut handle = Vec::new();
    encode_int(&mut handle, -9_078_412_423_848_787_968);
    handle.push(b'0');
    let error = decode_row_key(&tidb_codec::encode_row_key(100, &handle)).unwrap_err();
    assert_eq!(error.mysql_error_code(), 8221);
}

/// Source: `tablecodec_test.go::TestRowCodec`.
#[test]
fn test_row_codec() {
    let columns = BTreeMap::from([
        (1, field(FieldTypeCode::LongLong)),
        (2, field(FieldTypeCode::Varchar)),
        (3, field(FieldTypeCode::NewDecimal)),
        (
            4,
            field(FieldTypeCode::Enum).with_elems(["a"].map(String::from)),
        ),
        (
            5,
            field(FieldTypeCode::Set).with_elems(["a"].map(String::from)),
        ),
        (6, field(FieldTypeCode::Bit).with_flen(8)),
    ]);
    let row = vec![
        Datum::Int(100),
        Datum::new_bytes(b"abc"),
        Datum::Decimal(Decimal::from_literal("1")),
        Datum::Enum(MysqlEnum::new("a", 1), Collation::Binary),
        Datum::Set(MysqlSet::new("a", 1), Collation::Binary),
        Datum::Bit(BinaryLiteral::from(vec![100])),
    ];
    let ids = [1, 2, 3, 4, 5, 6];
    for new_format in [false, true] {
        let encoded =
            encode_table_row(Some(&UTC), &row, &ids, new_format, None).unwrap();
        let decoded = decode_table_row_to_map(&encoded, &columns, Some(&UTC)).unwrap();
        assert_eq!(decoded.len(), row.len());
        assert_eq!(decoded[&1], row[0]);
        assert_eq!(decoded[&2].as_raw_bytes(), row[1].as_raw_bytes());
        assert_eq!(decoded[&3], row[2]);
        assert_eq!(decoded[&4], row[3]);
        assert_eq!(decoded[&5], row[4]);
        assert_eq!(decoded[&6], row[5]);

        let requested = BTreeMap::from([
            (1, field(FieldTypeCode::LongLong)),
            (2, field(FieldTypeCode::Varchar)),
            (99, field(FieldTypeCode::Float)),
        ]);
        let decoded = decode_table_row_to_map(&encoded, &requested, Some(&UTC)).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[&1], row[0]);
        assert_eq!(decoded[&2].as_raw_bytes(), row[1].as_raw_bytes());
        assert!(!decoded.contains_key(&99));
    }
    assert_eq!(
        encode_old_table_row(Some(&UTC), &[], &[]).unwrap(),
        [tidb_codec::NIL_FLAG]
    );
}

/// Source: `tablecodec_test.go::TestDecodeColumnValue`.
#[test]
fn test_decode_column_value() {
    let timestamp =
        Time::new(CoreTime::from_date(2026, 7, 23, 12, 34, 56, 0), TimeType::Timestamp, 0)
            .unwrap();
    let cases = [
        (
            Datum::Time(timestamp),
            field(FieldTypeCode::Timestamp),
        ),
        (
            Datum::Set(MysqlSet::new("a", 1), Collation::Binary),
            field(FieldTypeCode::Set).with_elems(["a", "b", "c"].map(String::from)),
        ),
        (
            Datum::Bit(BinaryLiteral::from_uint(
                3_223_600,
                Some(BinaryLiteralWidth::try_from(3_u8).unwrap()),
            )),
            field(FieldTypeCode::Bit).with_flen(24),
        ),
        (
            Datum::Enum(MysqlEnum::default(), Collation::Binary),
            field(FieldTypeCode::Enum),
        ),
    ];
    for (datum, field_type) in cases {
        let encoded = encode_table_value(Some(&UTC), &datum).unwrap();
        assert_eq!(
            decode_column_value(&encoded, &field_type, Some(&UTC)).unwrap(),
            datum
        );
    }
}

/// Source: `tablecodec_test.go::TestUnflattenDatums`.
#[test]
fn test_unflatten_datums() {
    let mut values = vec![Datum::Int(1)];
    unflatten_datums(
        &mut values,
        &[field(FieldTypeCode::LongLong)],
        Some(&UTC),
    )
    .unwrap();
    assert_eq!(values, [Datum::Int(1)]);

    let mut values = vec![Datum::new_bytes(b"aaa")];
    let blob = field(FieldTypeCode::Blob).with_collation(Collation::Utf8Mb4UnicodeCi);
    unflatten_datums(&mut values, &[blob], Some(&UTC)).unwrap();
    assert_eq!(values[0].collation(), Some(Collation::Utf8Mb4UnicodeCi));
}

/// Source: `tablecodec_test.go::TestTimeCodec`.
#[test]
fn test_time_codec() {
    let timestamp =
        Time::new(CoreTime::from_date(2016, 6, 23, 11, 30, 45, 0), TimeType::Timestamp, 0)
            .unwrap();
    let duration = MySqlDuration::new(12, 59, 59, 999_999, 6).unwrap();
    let row = [
        Datum::Int(100),
        Datum::new_bytes(b"abc"),
        Datum::Time(timestamp),
        Datum::Duration(duration),
    ];
    let columns = BTreeMap::from([
        (1, field(FieldTypeCode::LongLong)),
        (2, field(FieldTypeCode::Varchar)),
        (3, field(FieldTypeCode::Timestamp)),
        (4, field(FieldTypeCode::Duration).with_decimal(6)),
    ]);
    let encoded = encode_table_row(Some(&UTC), &row, &[1, 2, 3, 4], true, None).unwrap();
    let decoded = decode_table_row_to_map(&encoded, &columns, Some(&UTC)).unwrap();
    assert_eq!(decoded[&1], row[0]);
    assert_eq!(decoded[&2].as_raw_bytes(), row[1].as_raw_bytes());
    assert_eq!(decoded[&3], row[2]);
    assert_eq!(decoded[&4], row[3]);
}

/// Source: `tablecodec_test.go::TestCutRow`.
#[test]
fn test_cut_row() {
    let row = [
        Datum::Int(100),
        Datum::new_bytes(b"abc"),
        Datum::Decimal(Decimal::from_literal("1")),
    ];
    let encoded = encode_old_table_row(Some(&UTC), &row, &[1, 2, 3]).unwrap();
    let cut = cut_table_row(&encoded, &BTreeMap::from([(1, 0), (2, 1), (3, 2)])).unwrap();
    for (value, expected) in cut.into_iter().zip(row) {
        assert_eq!(decode_one(&value.unwrap()).unwrap().1, expected);
    }
    assert!(cut_table_row(&[tidb_codec::NIL_FLAG], &BTreeMap::new())
        .unwrap()
        .is_empty());
    assert!(cut_table_row(&[], &BTreeMap::new()).unwrap().is_empty());
}

fn encoded_index_key() -> (Vec<Datum>, Vec<u8>) {
    let values = vec![
        Datum::Int(1),
        Datum::new_bytes(b"abc"),
        Datum::Real(5.5),
        Datum::Int(100),
    ];
    let encoded = encode_key(&values).unwrap();
    (values, encode_index_seek_key(4, 5, &encoded))
}

/// Source: `tablecodec_test.go::TestCutKeyNew`.
#[test]
fn test_cut_key_new() {
    let (values, key) = encoded_index_key();
    let (cut, handle) = cut_index_key(&key, 3).unwrap();
    for (encoded, expected) in cut.iter().zip(&values) {
        assert_eq!(&decode_one(encoded).unwrap().1, expected);
    }
    assert_eq!(decode_one(handle).unwrap().1, Datum::Int(100));
}

/// Source: `tablecodec_test.go::TestCutKey`.
#[test]
fn test_cut_key() {
    let (values, key) = encoded_index_key();
    let (cut, handle) = cut_index_key_by_ids(&key, &[1, 2, 3]).unwrap();
    for (id, expected) in [1, 2, 3].iter().zip(&values) {
        assert_eq!(&decode_one(&cut[id]).unwrap().1, expected);
    }
    assert_eq!(decode_one(handle).unwrap().1, Datum::Int(100));
}

/// Source: `tablecodec_test.go::TestDecodeBadDecical`.
#[test]
fn test_decode_bad_decical() {
    assert!(decode_decimal_with_fault(&[1, 0, 0], true).is_err());
}

/// Source: `tablecodec_test.go::TestIndexKey`.
#[test]
fn test_index_key() {
    assert_eq!(
        decode_key_head(&encode_index_seek_key(4, 5, &[])).unwrap(),
        KeyHead::Index {
            table_id: 4,
            index_id: 5,
        }
    );
}

/// Source: `tablecodec_test.go::TestRecordKey`.
#[test]
fn test_record_key() {
    let handle = RecordHandle::Int(u32::MAX.into());
    let key = encode_row_key_with_handle(55, &handle);
    assert_eq!(decode_key_head(&key).unwrap(), KeyHead::Record { table_id: 55 });
    assert_eq!(decode_record_key(&key).unwrap(), (55, handle.clone()));
    assert_eq!(encode_record_key(&gen_table_record_prefix(55), &handle), key);
    assert!(decode_record_key(&[]).is_err());
    assert!(decode_record_key(b"abcdefghijklmnopqrstuvwxyz").is_err());
    assert_eq!(decode_table_id(&[]), 0);

    assert_eq!(
        encode_record_key(
            &gen_table_record_prefix(1),
            &RecordHandle::partition(42, RecordHandle::Int(9)),
        ),
        encode_row_key_with_handle(42, &RecordHandle::Int(9))
    );
}

/// Source: `tablecodec_test.go::TestPrefix`.
#[test]
fn test_prefix() {
    let key = encode_table_prefix(66);
    assert_eq!(decode_table_id(&key), 66);
    assert_eq!(TABLE_PREFIX, b"t");
    assert_eq!(META_PREFIX, b"m");
    assert_eq!(gen_table_prefix(66), key);
    let index = encode_table_index_prefix(66, u32::MAX.into());
    assert_eq!(
        decode_key_head(&index).unwrap(),
        KeyHead::Index {
            table_id: 66,
            index_id: u32::MAX.into(),
        }
    );
    assert_eq!(decode_index_id(&index).unwrap(), i64::from(u32::MAX));
    assert_eq!(decode_table_id(&gen_table_index_prefix(66)), 66);
    let mut extended = index;
    extended.extend_from_slice(b"xyz");
    assert_eq!(truncate_to_row_key_len(&extended).len(), RECORD_ROW_KEY_LEN);
    assert_eq!(truncate_to_row_key_len(&key).len(), key.len());
    assert!(is_record_key(&encode_row_key_with_handle(66, &RecordHandle::Int(1))));
    assert!(is_index_key(&encode_table_index_prefix(66, 1)));
    assert!(is_table_key(&key));
}

/// Source: `tablecodec_test.go::TestDecodeIndexKey`.
#[test]
fn test_decode_index_key() {
    let values = [
        Datum::Int(1),
        Datum::new_bytes(b"abc"),
        Datum::Real(123.45),
    ];
    let key = encode_index_seek_key(4, 5, &encode_key(&values).unwrap());
    assert_eq!(
        decode_index_key(&key).unwrap(),
        (4, 5, vec!["1".into(), "abc".into(), "123.45".into()])
    );
}

/// Source: `tablecodec_test.go::TestCutPrefix`.
#[test]
fn test_cut_prefix() {
    let key = encode_table_index_prefix(42, 666);
    assert_eq!(cut_row_key_prefix(&key), [0x80, 0, 0, 0, 0, 0, 2, 0x9a]);
    assert!(cut_index_prefix(&key).is_empty());
}

/// Source: `tablecodec_test.go::TestRange`.
#[test]
fn test_range() {
    let (start_22, end_22) = get_table_handle_key_range(22);
    let (start_23, end_23) = get_table_handle_key_range(23);
    assert!(start_22 < end_22 && end_22 < start_23 && start_23 < end_23);
    let (start_666, end_666) = get_table_index_key_range(42, 666);
    let (start_667, end_667) = get_table_index_key_range(42, 667);
    assert!(start_666 < end_666 && end_666 < start_667 && start_667 < end_667);
}

/// Source: `tablecodec_test.go::TestDecodeAutoIDMeta`.
#[test]
fn test_decode_auto_id_meta() {
    let encoded = [
        0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x31, 0x30, 0x38, 0x0, 0xfe,
    ];
    assert_eq!(
        decode_meta_key(&encoded).unwrap(),
        (b"DB:56".to_vec(), b"TID:108".to_vec())
    );
    assert_eq!(encode_meta_key(b"DB:56", b"TID:108"), encoded);
    assert!(encoded.starts_with(&encode_meta_key_prefix(b"DB:56")));
}

/// Source: `tablecodec_test.go::TestError`.
#[test]
fn test_error() {
    use tidb_tablecodec::table_key::TableKeyError;

    assert_eq!(TableKeyError::InvalidKey.mysql_error_code(), 8221);
    assert_eq!(TableKeyError::InvalidRecordKey.mysql_error_code(), 8045);
    assert_eq!(TableKeyError::InvalidIndexKey.mysql_error_code(), 8222);
}

/// Source: `tablecodec_test.go::TestUntouchedIndexKValue`.
#[test]
fn test_untouched_index_kvalue() {
    let mut key = encode_table_index_prefix(1, 1);
    let legacy = [0, 0, 0, 0, 0, 0, 0, 1, UNCOMMITTED_INDEX_KV_FLAG];
    assert!(is_untouched_index_kv(&key, &legacy));
    assert!(!is_untouched_index_kv(
        &key,
        &[0, INDEX_VERSION_FLAG, 1]
    ));
    assert!(is_untouched_index_kv(
        &key,
        &[1, INDEX_VERSION_FLAG, 1, UNCOMMITTED_INDEX_KV_FLAG]
    ));
    let marker_like = encode_handle_in_unique_index_value(
        &int_handle(0x017d_0100_0000_0031),
        false,
    );
    assert!(!is_untouched_index_kv(&key, &marker_like));
    index_key_to_temp_index_key(&mut key).unwrap();
    assert!(is_untouched_index_kv(&key, &legacy));
    let deleted = TempIndexValueElem {
        value: Vec::new(),
        handle: Some(int_handle(1)),
        key_version: b'b',
        delete: true,
        distinct: true,
        global: false,
    };
    let mut encoded = Vec::new();
    deleted.encode(&mut encoded).unwrap();
    assert!(!is_untouched_index_kv(&key, &encoded));
}

/// Source: `tablecodec_test.go::TestTempIndexKey`.
#[test]
fn test_temp_index_key() {
    let mut key = encode_index_seek_key(4, 5, &encode_key(&[Datum::Int(1)]).unwrap());
    index_key_to_temp_index_key(&mut key).unwrap();
    let KeyHead::Index { table_id, index_id } = decode_key_head(&key).unwrap() else {
        panic!("expected index");
    };
    assert_eq!(table_id, 4);
    assert_ne!(index_id, 5);
    assert_eq!(index_id & INDEX_ID_MASK, 5);
    assert_eq!(decode_index_id(&key).unwrap(), index_id);
    assert!(is_temp_index_key(&key));
    temp_index_key_to_index_key(&mut key).unwrap();
    assert_eq!(
        decode_key_head(&key).unwrap(),
        KeyHead::Index {
            table_id: 4,
            index_id: 5,
        }
    );
}

fn temp_element(
    value: Vec<u8>,
    handle: Option<Handle>,
    key_version: u8,
    delete: bool,
    distinct: bool,
) -> TempIndexValueElem {
    TempIndexValueElem {
        value,
        handle,
        key_version,
        delete,
        distinct,
        global: false,
    }
}

/// Source: `tablecodec_test.go::TestTempIndexValueCodec`.
#[test]
fn test_temp_index_value_codec() {
    let normal = temp_element(encode_value(&[Datum::Int(1)]).unwrap(), None, b'b', false, false);
    let distinct = temp_element(
        encode_handle_in_unique_index_value(&int_handle(100), false),
        None,
        b'm',
        false,
        true,
    );
    let deleted = temp_element(Vec::new(), None, b'b', true, false);
    let distinct_deleted = temp_element(Vec::new(), Some(int_handle(100)), b'b', true, true);

    for element in [&normal, &distinct, &deleted, &distinct_deleted] {
        let mut encoded = Vec::new();
        element.encode(&mut encoded).unwrap();
        let decoded = decode_temp_index_value(&encoded).unwrap();
        assert_eq!(decoded.as_slice(), std::slice::from_ref(element));
    }

    let first = temp_element(
        encode_handle_in_unique_index_value(&int_handle(100), false),
        Some(int_handle(100)),
        b'm',
        false,
        true,
    );
    let second = temp_element(Vec::new(), Some(int_handle(100)), b'm', true, true);
    let third = temp_element(
        encode_handle_in_unique_index_value(&int_handle(101), false),
        Some(int_handle(101)),
        b'm',
        false,
        true,
    );
    let mut encoded = Vec::new();
    first.encode(&mut encoded).unwrap();
    second.encode(&mut encoded).unwrap();
    third.encode(&mut encoded).unwrap();
    let decoded = decode_temp_index_value(&encoded).unwrap();
    assert_eq!(decoded.len(), 3);
    assert_eq!(
        filter_overwritten_temp_index_values(vec![first.clone(), second.clone(), third.clone()]),
        [second, third]
    );
    assert!(!index_kv_is_unique(
        &{
            let mut value = Vec::new();
            distinct_deleted.encode(&mut value).unwrap();
            value
        }
    ));

    let history = TempIndexValue {
        elements: vec![first],
    };
    assert!(!history.is_empty());
    assert_eq!(history.current().unwrap().key_version, b'm');
}

/// Source: `tablecodec_test.go::TestV2TableCodec`.
#[test]
fn test_v2_table_codec() {
    let mut key = vec![b'x', 0x04, 0x25, 0xd4];
    key.extend_from_slice(&encode_table_prefix(31_415_926));
    assert_eq!(decode_table_id(&key), 31_415_926);

    let mut row_key = vec![b'x', 0x04, 0x25, 0xd4];
    row_key.extend_from_slice(&encode_row_key_with_handle(
        31_415_926,
        &RecordHandle::Int(9),
    ));
    assert_eq!(decode_row_key(&row_key).unwrap(), RecordHandle::Int(9));

    assert_eq!(decode_table_id(b"x001HelloWorld"), 0);
    assert_eq!(decode_table_id(b"x001x001t123"), 0);
}

/// Source: `tablecodec_test.go::TestDecodeIndexHandleWithPartitionIDInKeyAndValue`.
#[test]
fn test_decode_index_handle_with_partition_id_in_key_and_value() {
    let mut key = encode_index_seek_key(100, 1, &encode_key(&[Datum::Int(123)]).unwrap());
    key.push(PARTITION_ID_FLAG);
    encode_int(&mut key, 42);
    key.push(tidb_codec::INT_FLAG);
    encode_int(&mut key, 999);

    let mut value = vec![0, PARTITION_ID_FLAG];
    encode_int(&mut value, 42);
    value.resize(10, 0);
    value[0] = 0;
    let handle = decode_index_handle(&key, &value, 1).unwrap();
    let expected = partition_handle(42, int_handle(999));
    assert!(handle.equal(&expected));
    let Handle::Partition(partition) = handle else {
        panic!("expected partition handle");
    };
    assert_eq!(partition.partition_id(), 42);
    assert!(matches!(partition.inner(), Handle::Int(_)));
}

fn global_index_metadata(version: u8) -> (TableInfo, IndexInfo) {
    let table = TableInfo {
        columns: vec![
            table_column(1, 0, FieldTypeCode::Long),
            table_column(2, 1, FieldTypeCode::Long),
        ],
        pk_is_handle: false,
        is_common_handle: false,
        common_handle_version: 0,
        indices: Vec::new(),
    };
    let index = IndexInfo {
        id: 1,
        columns: vec![IndexColumn {
            offset: 1,
            length: UNSPECIFIED_LENGTH,
            use_changing_type: false,
        }],
        unique: true,
        global: true,
        global_index_version: version,
        primary: false,
    };
    (table, index)
}

/// Source: `tablecodec_test.go::TestUniqueGlobalIndexKeyWithNullValues`.
#[test]
fn test_unique_global_index_key_with_null_values() {
    let (table, index) = global_index_metadata(1);
    let handle = partition_handle(42, int_handle(999));
    let mut non_null = [Datum::Int(123)];
    let (key, distinct) = generate_index_key(
        Encoder::new(true),
        Some(&UTC),
        &table,
        &index,
        100,
        &mut non_null,
        Some(&handle),
    )
    .unwrap();
    assert!(distinct);
    assert!(!key.contains(&PARTITION_ID_FLAG));

    let mut null = [Datum::Null];
    let (key, distinct) = generate_index_key(
        Encoder::new(true),
        Some(&UTC),
        &table,
        &index,
        100,
        &mut null,
        Some(&handle),
    )
    .unwrap();
    assert!(!distinct);
    assert!(key.contains(&PARTITION_ID_FLAG));

    let value = generate_index_value(
        true,
        Some(&UTC),
        &table,
        &index,
        false,
        true,
        false,
        &[Datum::Int(123)],
        &int_handle(999),
        42,
        &[],
    )
    .unwrap();
    assert!(value.contains(&PARTITION_ID_FLAG));
    let partition_id = split_index_value(&value).unwrap().partition_id.unwrap();
    assert_eq!(decode_int(&partition_id).unwrap(), (&[][..], 42));

    let (table, legacy) = global_index_metadata(0);
    let mut null = [Datum::Null];
    let (legacy_key, distinct) = generate_index_key(
        Encoder::new(true),
        Some(&UTC),
        &table,
        &legacy,
        100,
        &mut null,
        Some(&int_handle(999)),
    )
    .unwrap();
    assert!(!distinct);
    assert!(!legacy_key.contains(&PARTITION_ID_FLAG));
}

/// Source: `bench_test.go::TestBenchDaily`.
#[test]
fn test_bench_daily() {
    for _ in 0..100 {
        let key = encode_row_key_with_handle(100, &RecordHandle::Int(100));
        assert_eq!(decode_row_key(&key).unwrap(), RecordHandle::Int(100));
        assert!(is_record_key(&key));
        let value = encode_table_value(Some(&UTC), &Datum::Int(100)).unwrap();
        assert_eq!(decode_one(&value).unwrap().1, Datum::Int(100));
    }
}

/// Source support: table-range verification at the end of `tablecodec.go`.
#[test]
fn verify_table_id_for_ranges() {
    let partitions = vec![
        vec![
            TableKeyRange {
                start_key: encode_row_key_with_handle(11, &RecordHandle::Int(1)),
                end_key: encode_row_key_with_handle(11, &RecordHandle::Int(2)),
            },
            TableKeyRange {
                start_key: encode_row_key_with_handle(11, &RecordHandle::Int(3)),
                end_key: encode_row_key_with_handle(11, &RecordHandle::Int(4)),
            },
        ],
        vec![TableKeyRange {
            start_key: encode_row_key_with_handle(12, &RecordHandle::Int(1)),
            end_key: encode_row_key_with_handle(12, &RecordHandle::Int(2)),
        }],
    ];
    assert_eq!(verify_table_ids_for_ranges(&partitions).unwrap(), [11, 12]);

    let invalid = vec![vec![
        TableKeyRange {
            start_key: encode_row_key_with_handle(11, &RecordHandle::Int(1)),
            end_key: Vec::new(),
        },
        TableKeyRange {
            start_key: b"not-a-table-key".to_vec(),
            end_key: Vec::new(),
        },
    ]];
    assert_eq!(
        verify_table_ids_for_ranges(&invalid).unwrap_err().to_string(),
        "Incorrect keyRange is constrcuted"
    );

    let mixed = vec![vec![
        TableKeyRange {
            start_key: encode_row_key_with_handle(11, &RecordHandle::Int(1)),
            end_key: Vec::new(),
        },
        TableKeyRange {
            start_key: encode_row_key_with_handle(12, &RecordHandle::Int(1)),
            end_key: Vec::new(),
        },
    ]];
    assert_eq!(
        verify_table_ids_for_ranges(&mixed).unwrap_err().to_string(),
        "Using multi partition's ranges as single table's"
    );
}

/// Source support: prefix truncation from `TruncateIndexValue`.
#[test]
fn truncate_index_values_preserve_byte_and_character_domains() {
    let binary_column = TableColumn {
        id: 1,
        offset: 0,
        field_type: field(FieldTypeCode::Varchar).with_collation(Collation::Binary),
        primary_key: false,
        changing_field_type: None,
    };
    let utf8_column = TableColumn {
        field_type: field(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4Bin),
        ..binary_column.clone()
    };
    let index_column = IndexColumn {
        offset: 0,
        length: 2,
        use_changing_type: false,
    };
    let mut binary = Datum::new_bytes("你好".as_bytes());
    truncate_index_value(&mut binary, &index_column, &binary_column).unwrap();
    assert_eq!(binary.as_raw_bytes().unwrap(), &"你".as_bytes()[..2]);
    let mut utf8 = Datum::new_collation_string("你好世界", Collation::Utf8Mb4Bin);
    truncate_index_value(&mut utf8, &index_column, &utf8_column).unwrap();
    assert_eq!(utf8.as_raw_bytes().unwrap(), "你好".as_bytes());

    let invalid_utf8 = vec![0xf0, 0x28, 0x8c, 0x28];
    let mut invalid = Datum::new_bytes(invalid_utf8.clone());
    truncate_index_value(&mut invalid, &index_column, &utf8_column).unwrap();
    assert_eq!(
        invalid.as_raw_bytes().unwrap(),
        String::from_utf8_lossy(&invalid_utf8)
            .chars()
            .take(2)
            .collect::<String>()
            .as_bytes()
    );
    let no_truncation = IndexColumn {
        length: 8,
        ..index_column
    };
    let mut preserved = Datum::new_bytes(invalid_utf8.clone());
    truncate_index_value(&mut preserved, &no_truncation, &utf8_column).unwrap();
    assert_eq!(preserved.as_raw_bytes().unwrap(), invalid_utf8);
}

/// Source support: V1 uniqueness is carried only by a common handle.
#[test]
fn clustered_v1_uniqueness_requires_common_handle() {
    assert!(!index_kv_is_unique(&[0, INDEX_VERSION_FLAG, 1]));

    let mut v1_common_handle = vec![0, INDEX_VERSION_FLAG, 1, COMMON_HANDLE_FLAG];
    let common = encode_key(&[Datum::Int(123)]).unwrap();
    v1_common_handle.extend_from_slice(&(common.len() as u16).to_be_bytes());
    v1_common_handle.extend_from_slice(&common);
    assert!(index_kv_is_unique(&v1_common_handle));
}

/// Source support: `model.GetIdxChangingFieldType` selects the concurrent-DDL
/// type when deciding whether clustered V1 restored data is required.
#[test]
fn clustered_v1_uses_changing_index_field_type() {
    let table = TableInfo {
        columns: vec![TableColumn {
            id: 7,
            offset: 0,
            field_type: field(FieldTypeCode::LongLong),
            primary_key: false,
            changing_field_type: Some(
                field(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4GeneralCi),
            ),
        }],
        indices: Vec::new(),
        pk_is_handle: false,
        is_common_handle: true,
        common_handle_version: 1,
    };
    let mut index = IndexInfo {
        id: 9,
        columns: vec![IndexColumn {
            offset: 0,
            length: UNSPECIFIED_LENGTH,
            use_changing_type: true,
        }],
        unique: false,
        global: false,
        global_index_version: 0,
        primary: false,
    };
    let handle = common_handle(encode_key(&[Datum::Int(1)]).unwrap());
    let value = generate_index_value(
        true,
        Some(&UTC),
        &table,
        &index,
        true,
        false,
        false,
        &[Datum::new_collation_string(
            "value",
            Collation::Utf8Mb4GeneralCi,
        )],
        &handle,
        0,
        &[],
    )
    .unwrap();
    let restored = split_index_value(&value).unwrap().restored_values.unwrap();
    assert_eq!(
        decode_table_row_to_map(
            &restored,
            &BTreeMap::from([(
                7,
                field(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4GeneralCi),
            )]),
            Some(&UTC),
        )
        .unwrap()[&7]
            .as_raw_bytes()
            .unwrap(),
        b"value"
    );

    index.columns[0].use_changing_type = false;
    let value = generate_index_value(
        true,
        Some(&UTC),
        &table,
        &index,
        true,
        false,
        false,
        &[Datum::new_bytes(b"value")],
        &handle,
        0,
        &[],
    )
    .unwrap();
    assert!(split_index_value(&value).unwrap().restored_values.is_none());
}

/// Source support: temporary-index untouched suffix.
#[test]
fn temporary_index_untouched_suffix_is_exact() {
    assert!(temp_index_value_is_untouched(&[
        0,
        UNCOMMITTED_INDEX_KV_FLAG
    ]));
    assert!(!temp_index_value_is_untouched(&[]));
}

/// Source support: legacy integer handle raw bytes.
#[test]
fn unique_index_handle_raw_bytes_round_trip_signed_domain() {
    for value in [i64::MIN, -1, 0, 1, i64::MAX] {
        let encoded = encode_handle_in_unique_index_value(&int_handle(value), false);
        assert_eq!(
            decode_handle_in_index_value(&encoded)
                .unwrap()
                .int_value(),
            Some(value)
        );
    }
}

/// Source support: new row decoder can still consume explicitly constructed
/// rowcodec data through the tablecodec portal.
#[test]
fn new_row_portal_is_rowcodec_compatible() {
    let mut encoded = Vec::new();
    encode_row(Some(&UTC), &[1], &[Datum::Int(7)], &mut encoded).unwrap();
    assert_eq!(
        decode_table_row_to_map(
            &encoded,
            &BTreeMap::from([(1, field(FieldTypeCode::LongLong))]),
            Some(&UTC),
        )
        .unwrap(),
        BTreeMap::from([(1, Datum::Int(7))])
    );
}

/// Source support: both `DecodeRowWithMap` variants preserve caller-owned
/// entries and overwrite decoded requested columns.
#[test]
fn row_portals_decode_into_existing_maps() {
    let columns = BTreeMap::from([(1, field(FieldTypeCode::LongLong))]);
    let mut existing = BTreeMap::from([(1, Datum::Int(-1)), (99, Datum::Int(99))]);
    let old = encode_old_table_row(Some(&UTC), &[Datum::Int(7)], &[1]).unwrap();
    decode_table_row_into_map(&old, &columns, Some(&UTC), &mut existing).unwrap();
    assert_eq!(
        existing,
        BTreeMap::from([(1, Datum::Int(7)), (99, Datum::Int(99))])
    );

    let mut new = Vec::new();
    encode_row(Some(&UTC), &[1], &[Datum::Int(8)], &mut new).unwrap();
    decode_table_row_into_map(&new, &columns, Some(&UTC), &mut existing).unwrap();
    assert_eq!(
        existing,
        BTreeMap::from([(1, Datum::Int(8)), (99, Datum::Int(99))])
    );
    decode_table_row_into_map(&[], &columns, Some(&UTC), &mut existing).unwrap();
    assert_eq!(existing[&99], Datum::Int(99));
}

/// Source support: `DecodeHandleToDatumMap` does not decode common-handle
/// columns the caller did not request. Canonical `CommonHandle` construction
/// eliminates malformed encoded handles before they can reach tablecodec.
#[test]
fn unused_common_handle_columns_are_not_decoded() {
    let mut row = BTreeMap::new();
    let handle = common_handle(encode_key(&[Datum::Int(7)]).unwrap());
    tidb_tablecodec::decode_handle_to_datum_map(
        Some(&handle),
        &[1],
        &BTreeMap::new(),
        Some(&UTC),
        &mut row,
    )
    .unwrap();
    assert!(row.is_empty());
}

/// Source support: old-row value boundaries are independently cuttable.
#[test]
fn old_row_value_boundaries_are_exact() {
    let encoded =
        encode_old_table_row(Some(&UTC), &[Datum::Int(1)], &[9]).unwrap();
    let (column, remaining) = cut_one(&encoded).unwrap();
    assert_eq!(decode_one(column).unwrap().1, Datum::Int(9));
    assert_eq!(decode_one(remaining).unwrap().1, Datum::Int(1));
}

/// Source support: `unflatten` leaves SQL NULL unchanged for every type.
#[test]
fn unflatten_null_is_type_independent() {
    for code in [
        FieldTypeCode::LongLong,
        FieldTypeCode::Timestamp,
        FieldTypeCode::Duration,
        FieldTypeCode::Enum,
        FieldTypeCode::Set,
        FieldTypeCode::Bit,
    ] {
        assert_eq!(
            unflatten_datum(Datum::Null, &field(code), Some(&UTC)).unwrap(),
            Datum::Null
        );
    }
}

/// Source support: `flatten` uses Go's strict binary-literal conversion
/// context, so a non-zero payload wider than uint64 is an error.
#[test]
fn flatten_rejects_truncated_binary_literals() {
    let too_wide = Datum::BinaryLiteral(BinaryLiteral::from(vec![1; 9]));
    assert_eq!(
        encode_table_value(Some(&UTC), &too_wide)
            .unwrap_err()
            .to_string(),
        "invalid binary literal exceeds uint64 datum"
    );
}
