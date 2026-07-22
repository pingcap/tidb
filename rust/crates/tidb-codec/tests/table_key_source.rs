// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Source-derived coverage for TiDB table, record, index, and metadata keys.

use tidb_codec::table_key::*;
use tidb_codec::{encode_int, encode_key};
use tidb_datatype::Datum;

#[test]
fn test_table_codec() {
    let mut handle = Vec::new();
    encode_int(&mut handle, 2);
    let key = encode_row_key(1, &handle);
    assert_eq!(decode_row_key(&key), Ok(RecordHandle::Int(2)));
    assert_eq!(encode_row_key_with_handle(1, &RecordHandle::Int(2)), key);
}

#[test]
fn test_table_codec_invalid() {
    let mut handle = Vec::new();
    encode_int(&mut handle, -9_078_412_423_848_787_968);
    handle.push(b'0');
    assert!(decode_row_key(&encode_row_key(100, &handle)).is_err());
}

#[test]
fn test_index_key() {
    assert_eq!(
        decode_key_head(&encode_index_seek_key(4, 5, &[])),
        Ok(KeyHead::Index {
            table_id: 4,
            index_id: 5
        })
    );
}

#[test]
fn test_record_key() {
    let mut handle = Vec::new();
    encode_int(&mut handle, u32::MAX.into());
    let key = encode_row_key(55, &handle);
    assert_eq!(decode_key_head(&key), Ok(KeyHead::Record { table_id: 55 }));
    assert_eq!(
        decode_record_key(&key),
        Ok((55, RecordHandle::Int(u32::MAX.into())))
    );
    assert_eq!(
        encode_record_key(
            &gen_table_record_prefix(55),
            &RecordHandle::Int(u32::MAX.into())
        ),
        key
    );
    assert!(decode_record_key(&[]).is_err());
    assert!(decode_record_key(b"abcdefghijklmnopqrstuvwxyz").is_err());
    assert_eq!(decode_table_id(&[]), 0);
}

#[test]
fn test_prefix() {
    let key = encode_table_prefix(66);
    assert_eq!(decode_table_id(&key), 66);
    assert_eq!(TABLE_PREFIX, b"t");
    assert_eq!(gen_table_prefix(66), key);
    let index_prefix = encode_table_index_prefix(66, u32::MAX.into());
    assert_eq!(
        decode_key_head(&index_prefix),
        Ok(KeyHead::Index {
            table_id: 66,
            index_id: u32::MAX.into()
        })
    );
    assert_eq!(decode_table_id(&gen_table_index_prefix(66)), 66);
    let mut extended = index_prefix;
    extended.extend_from_slice(b"xyz");
    assert_eq!(truncate_to_row_key_len(&extended).len(), RECORD_ROW_KEY_LEN);
    assert_eq!(truncate_to_row_key_len(&key).len(), key.len());
}

#[test]
fn test_decode_index_key() {
    let values = vec![
        Datum::new_int(1),
        Datum::new_bytes(b"abc".to_vec()),
        Datum::new_real(123.45),
    ];
    let encoded = encode_key(&values).expect("encode key");
    assert_eq!(
        decode_index_key(&encode_index_seek_key(4, 5, &encoded)),
        Ok((4, 5, vec!["1".into(), "abc".into(), "123.45".into()]))
    );
}

#[test]
fn test_cut_prefix() {
    let key = encode_table_index_prefix(42, 666);
    assert_eq!(cut_row_key_prefix(&key), [0x80, 0, 0, 0, 0, 0, 2, 0x9a]);
    assert!(cut_index_prefix(&key).is_empty());
}

#[test]
fn non_unique_int_index_key_matches_go_gen_index_key() {
    // Byte fixtures from Go `tablecodec.GenIndexKey`'s non-unique int-handle
    // composition (`EncodeIndexSeekKey(tableID, idxID, EncodeKey(values...) +
    // IntHandleFlag + EncodeInt(handle))`): t + memcomp(tableID) + _i +
    // memcomp(idxID) + INT_FLAG + memcomp(k) + INT_FLAG + memcomp(handle).
    let cases: &[(i64, i64, i64, i64, &str)] = &[
        (
            100,
            1,
            42,
            7,
            "7480000000000000645f69800000000000000103800000000000002a038000000000000007",
        ),
        (
            100,
            1,
            -5,
            1,
            "7480000000000000645f698000000000000001037ffffffffffffffb038000000000000001",
        ),
        (
            256,
            2,
            0,
            9_223_372_036_854_775_807,
            "7480000000000001005f69800000000000000203800000000000000003ffffffffffffffff",
        ),
        (
            100,
            1,
            42,
            -3,
            "7480000000000000645f69800000000000000103800000000000002a037ffffffffffffffd",
        ),
    ];
    for &(table_id, index_id, k, handle, expected) in cases {
        let key = encode_non_unique_index_key(table_id, index_id, &[Datum::new_int(k)], handle)
            .expect("integer index key encodes");
        assert_eq!(
            hex(&key),
            expected,
            "table={table_id} idx={index_id} k={k} handle={handle}",
        );
    }
}

#[test]
fn non_unique_index_value_is_the_single_zero_byte() {
    // Go `genIndexValueVersion0` emits a single '0' (0x30) for a non-unique
    // integer-handle index with no restored data — confirmed against real
    // GenIndexValuePortal for several (k, handle) pairs, all yielding 0x30.
    assert_eq!(non_unique_index_value(), vec![0x30]);
}

#[test]
fn test_range() {
    // pkg/tablecodec/tablecodec_test.go:523 TestRange
    let (start_22, end_22) = get_table_handle_key_range(22);
    let (start_23, end_23) = get_table_handle_key_range(23);

    assert!(start_22 < end_22);
    assert!(end_22 < start_23);
    assert!(start_23 < end_23);
    assert_eq!(
        start_22,
        [b't', 0x80, 0, 0, 0, 0, 0, 0, 22, b'_', b'r', 0, 0, 0, 0, 0, 0, 0, 0,]
    );
    assert_eq!(
        end_22,
        [
            b't', 0x80, 0, 0, 0, 0, 0, 0, 22, b'_', b'r', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff,
        ]
    );
}

#[test]
fn test_decode_auto_id_meta() {
    let key = [
        0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0, 0, 0, 0xfc, 0, 0, 0, 0, 0, 0, 0, 0x68, 0x54, 0x49,
        0x44, 0x3a, 0x31, 0x30, 0x38, 0, 0xfe,
    ];
    assert_eq!(
        decode_meta_key(&key),
        Ok((b"DB:56".to_vec(), b"TID:108".to_vec()))
    );
    let mut with_remainder = key.to_vec();
    with_remainder.extend_from_slice(b"ignored");
    assert_eq!(
        decode_meta_key(&with_remainder),
        Ok((b"DB:56".to_vec(), b"TID:108".to_vec()))
    );
}
