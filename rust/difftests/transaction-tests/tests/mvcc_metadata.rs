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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct tests for the dependency-closed MVCC metadata boundary.
//!
//! The upstream `mvcc_test.go` file is an integration suite over Badger,
//! lockstore, kvproto, and the mock PD client. These tests intentionally pin
//! only the stable metadata bytes that can be translated without claiming any
//! of those storage or transaction services.

use tidb_txnkv::{
    decode_extra_txn_status_key, decode_key_ts, encode_extra_txn_status_key, encode_write_cf_value,
    parse_write_cf_value, DbUserMeta, LockType, MvccLockMetadata, MvccMetadataError, WriteType,
    FOR_UPDATE_PREFIX, LOCK_USER_META_DELETE, LOCK_USER_META_NONE, MIN_COMMIT_TS_PREFIX,
    SHORT_VALUE_MAX_LEN,
};

#[test]
fn write_cf_round_trip_preserves_source_remainder() {
    let encoded = encode_write_cf_value(WriteType::Put, 300, b"abc");
    assert_eq!(encoded, [b'P', 0xac, 0x02, b'v', 3, b'a', b'b', b'c']);

    let decoded = parse_write_cf_value(&encoded).expect("source record decodes");
    assert_eq!(decoded.write_type, WriteType::Put);
    assert_eq!(decoded.start_ts, 300);
    assert_eq!(decoded.short_value, [b'v', 3, b'a', b'b', b'c']);

    let no_value = encode_write_cf_value(WriteType::Rollback, u64::MAX, &[]);
    assert_eq!(
        parse_write_cf_value(&no_value)
            .expect("rollback decodes")
            .short_value,
        []
    );
}

#[test]
fn write_and_lock_type_bytes_match_tikv_source() {
    assert_eq!(u8::from(WriteType::Lock), b'L');
    assert_eq!(u8::from(WriteType::Rollback), b'R');
    assert_eq!(u8::from(WriteType::Delete), b'D');
    assert_eq!(u8::from(WriteType::Put), b'P');
    assert_eq!(u8::from(LockType::Put), b'P');
    assert_eq!(u8::from(LockType::Delete), b'D');
    assert_eq!(u8::from(LockType::Lock), b'L');
    assert_eq!(u8::from(LockType::Pessimistic), b'S');

    assert_eq!(SHORT_VALUE_MAX_LEN, 64);
    assert_eq!(FOR_UPDATE_PREFIX, b'f');
    assert_eq!(MIN_COMMIT_TS_PREFIX, b'm');
    assert_eq!(LOCK_USER_META_NONE, [0]);
    assert_eq!(LOCK_USER_META_DELETE, [2]);
}

#[test]
fn malformed_write_cf_and_timestamp_inputs_are_rejected() {
    assert_eq!(
        parse_write_cf_value(&[]),
        Err(MvccMetadataError::InvalidWriteCfValue)
    );
    assert_eq!(
        parse_write_cf_value(&[b'X', 1]),
        Err(MvccMetadataError::InvalidWriteCfValue)
    );
    assert!(matches!(
        parse_write_cf_value(&[b'P', 0x80]),
        Err(MvccMetadataError::Codec(_))
    ));
    assert!(matches!(
        decode_key_ts(b"short"),
        Err(MvccMetadataError::Codec(_))
    ));
}

#[test]
fn db_user_meta_is_fixed_little_endian_pair() {
    let meta = DbUserMeta::new(0x0102_0304_0506_0708, 0x1112_1314_1516_1718);
    assert_eq!(
        meta.as_bytes(),
        [8, 7, 6, 5, 4, 3, 2, 1, 24, 23, 22, 21, 20, 19, 18, 17]
    );
    assert_eq!(meta.start_ts(), 0x0102_0304_0506_0708);
    assert_eq!(meta.commit_ts(), 0x1112_1314_1516_1718);
    assert_eq!(DbUserMeta::try_from_bytes(&meta.as_bytes()), Ok(meta));
    assert_eq!(
        DbUserMeta::try_from_bytes(&[0; 15]),
        Err(MvccMetadataError::InvalidUserMetaLength(15))
    );
}

#[test]
fn extra_status_key_and_version_suffix_follow_source_byte_order() {
    let key = b"t123";
    let encoded = encode_extra_txn_status_key(key, 42).expect("non-empty key encodes");
    assert_eq!(&encoded[..key.len()], b"u123");
    assert_eq!(decode_key_ts(&encoded), Ok(42));
    assert_eq!(decode_extra_txn_status_key(&encoded), Some(key.to_vec()));

    assert_eq!(
        encode_extra_txn_status_key(&[], 42),
        Err(MvccMetadataError::EmptyKey)
    );
    assert_eq!(decode_extra_txn_status_key(&[0; 9]), None);
    assert_eq!(decode_extra_txn_status_key(&[0; 10]), Some(vec![255, 0]));
}

#[test]
fn lock_metadata_keeps_semantic_fields_without_wire_layout_claim() {
    let metadata = MvccLockMetadata {
        start_ts: 7,
        for_update_ts: 8,
        min_commit_ts: 9,
        ttl: 50,
        lock_type: LockType::Pessimistic,
        has_old_version: true,
        primary: b"p".to_vec(),
        use_async_commit: false,
        secondaries: vec![b"s1".to_vec(), b"s2".to_vec()],
        value: b"value".to_vec(),
    };
    assert_eq!(metadata.primary_len(), 1);
    assert_eq!(metadata.secondary_count(), 2);
    assert_eq!(metadata.lock_type, LockType::Pessimistic);
    assert!(metadata.has_old_version);
}
