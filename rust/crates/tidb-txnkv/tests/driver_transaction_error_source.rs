// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Source-derived transaction diagnostic and driver-error regressions.

use tidb_txnkv::driver::transaction_error::{pretty_lock_not_found_key, Redaction, WriteConflict};
use tidb_txnkv::{to_tidb_driver_error, StorageDriverError};

#[test]
fn test_lock_not_found_print() {
    let message = "Txn(Mvcc(TxnLockNotFound { start_ts: 408090278408224772, commit_ts: 408090279311835140, key: [116, 128, 0, 0, 0, 0, 0, 50, 137, 95, 105, 128, 0, 0, 0, 0,0 ,0, 1, 1, 67, 49, 57, 48, 57, 50, 57, 48, 255, 48, 48, 48, 48, 48, 52, 56, 54, 255, 50, 53, 53, 50, 51, 0, 0, 0, 252] }))";
    assert_eq!(
        pretty_lock_not_found_key(message),
        "{tableID=12937, indexID=1, indexValues={C19092900000048625523, }}"
    );
}

#[test]
fn test_write_conflict_pretty_format() {
    let index_key = vec![
        116, 128, 0, 0, 0, 0, 0, 1, 155, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 82, 87, 48, 49, 0,
        0, 0, 0, 251, 1, 55, 54, 56, 50, 50, 49, 49, 48, 255, 57, 0, 0, 0, 0, 0, 0, 0, 248, 1, 0,
        0, 0, 0, 0, 0, 0, 0, 247,
    ];
    let conflict = WriteConflict {
        start_ts: 399402937522847774,
        conflict_ts: 399402937719455772,
        conflict_commit_ts: 399402937719455773,
        key: index_key.clone(),
        primary: index_key,
        reason: "Unknown".into(),
    };
    let error = to_tidb_driver_error(&StorageDriverError::WriteConflict {
        conflict: Some(conflict),
        redaction: Redaction::Disabled,
    });
    assert_eq!(error.to_string(), "[kv:9007]Write conflict, txnStartTS=399402937522847774, conflictStartTS=399402937719455772, conflictCommitTS=399402937719455773, key={tableID=411, indexID=1, indexValues={RW01, 768221109, , }}, originalKey=74800000000000019b5f698000000000000001015257303100000000fb013736383232313130ff3900000000000000f8010000000000000000f7, primary={tableID=411, indexID=1, indexValues={RW01, 768221109, , }}, originalPrimaryKey=74800000000000019b5f698000000000000001015257303100000000fb013736383232313130ff3900000000000000f8010000000000000000f7, reason=Unknown [try again later]");

    let meta = vec![
        0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0, 0, 0, 0xfc, 0, 0, 0, 0, 0, 0, 0, 0x68, 0x54, 0x49,
        0x44, 0x3a, 0x31, 0x30, 0x38, 0, 0xfe,
    ];
    let conflict = WriteConflict {
        start_ts: 399402937522847774,
        conflict_ts: 399402937719455772,
        conflict_commit_ts: 399402937719455773,
        key: meta.clone(),
        primary: meta,
        reason: "Optimistic".into(),
    };
    let visible = to_tidb_driver_error(&StorageDriverError::WriteConflict {
        conflict: Some(conflict.clone()),
        redaction: Redaction::Disabled,
    });
    assert_eq!(visible.to_string(), "[kv:9007]Write conflict, txnStartTS=399402937522847774, conflictStartTS=399402937719455772, conflictCommitTS=399402937719455773, key={metaKey=true, key=DB:56, field=TID:108}, originalKey=6d44423a3536000000fc00000000000000685449443a31303800fe, primary={metaKey=true, key=DB:56, field=TID:108}, originalPrimaryKey=6d44423a3536000000fc00000000000000685449443a31303800fe, reason=Optimistic [try again later]");
    let enabled = to_tidb_driver_error(&StorageDriverError::WriteConflict {
        conflict: Some(conflict.clone()),
        redaction: Redaction::Enabled,
    });
    assert_eq!(enabled.to_string(), "[kv:9007]Write conflict, txnStartTS=399402937522847774, conflictStartTS=399402937719455772, conflictCommitTS=399402937719455773, key=????, reason=Optimistic [try again later]");
    let marker = to_tidb_driver_error(&StorageDriverError::WriteConflict {
        conflict: Some(conflict),
        redaction: Redaction::Marker,
    });
    assert_eq!(marker.to_string(), "[kv:9007]Write conflict, txnStartTS=399402937522847774, conflictStartTS=399402937719455772, conflictCommitTS=399402937719455773, key=‹›‹{metaKey=true, key=DB:56, field=TID:108}, originalKey=6d44423a3536000000fc00000000000000685449443a31303800fe, primary=›‹›‹{metaKey=true, key=DB:56, field=TID:108}, originalPrimaryKey=6d44423a3536000000fc00000000000000685449443a31303800fe›, reason=Optimistic [try again later]");
}

#[test]
fn retryable_routes_through_lock_key_diagnostics() {
    let message = "TxnLockNotFound key: [116,128,0,0,0,0,0,50,137,95,105,128,0,0,0,0,0,0,1,1,67,49,57,48,57,50,57,48,255,48,48,48,48,48,52,56,54,255,50,53,53,50,51,0,0,0,252]";
    let error = to_tidb_driver_error(&StorageDriverError::Retryable {
        message: message.into(),
    });
    assert!(error
        .to_string()
        .starts_with("[kv:8022]Error: KV error safe to retry"));
    assert!(error.to_string().contains("{tableID=12937, indexID=1"));
}

#[test]
fn empty_json_lock_key_is_valid_and_uses_the_go_byte_fallback() {
    assert_eq!(
        pretty_lock_not_found_key("TxnLockNotFound key: []"),
        "[]byte{}"
    );
}
