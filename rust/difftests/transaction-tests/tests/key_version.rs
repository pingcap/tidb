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

//! Direct translations of the representable key/version tests in `pkg/kv`.

use std::cmp::Ordering;
use std::time::SystemTime;

use tidb_txnkv::{
    gen_key_exists_err, get_cdc_write_source, get_lossy_ddl_reorg_source,
    get_min_inner_txn_start_ts, is_cdc_write_source_set, is_err_not_found,
    is_lossy_ddl_reorg_source_set, is_txn_retryable_error, set_cdc_write_source,
    set_lossy_ddl_reorg_source, ErrorClass, InnerTxnStartTsBox, Key, KeyRange, MysqlErrorCode,
    RequestTypeSupportedChecker, Version, ERR_ASSERTION_FAILED, ERR_CANNOT_SET_NIL_VALUE,
    ERR_ENTRY_TOO_LARGE, ERR_INVALID_TXN, ERR_KEY_EXISTS, ERR_KEY_TOO_LARGE, ERR_LOCK_EXPIRE,
    ERR_NOT_EXIST, ERR_NOT_IMPLEMENTED, ERR_TXN_RETRYABLE, ERR_TXN_TOO_LARGE, ERR_WRITE_CONFLICT,
    ERR_WRITE_CONFLICT_IN_TIDB, MAX_VERSION, MIN_VERSION, REQ_SUB_TYPE_ANALYZE_IDX,
    REQ_SUB_TYPE_DESC, REQ_SUB_TYPE_GROUP_BY, REQ_SUB_TYPE_SIGNATURE, REQ_TYPE_ANALYZE,
    REQ_TYPE_CHECKSUM, REQ_TYPE_DAG, REQ_TYPE_SELECT, TXN_RETRYABLE_MARK,
};

const PARTIAL_NEXT_FIXTURE: &str = include_str!("../fixtures/partial_next.hex");

fn decode_hex(input: &str) -> Vec<u8> {
    assert_eq!(input.len() % 2, 0, "hex fixture length must be even");
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| (decode_nibble(pair[0]) << 4) | decode_nibble(pair[1]))
        .collect()
}

fn decode_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("fixture contains non-hex byte {byte}"),
    }
}

fn fixture(name: &str) -> Key {
    let prefix = format!("{name}=");
    let encoded = PARTIAL_NEXT_FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"));
    Key::from_bytes(decode_hex(encoded))
}

/// Direct translation of `pkg/kv/key_test.go:38-61 TestPartialNext`.
///
/// TiDB's codec bytes come from the checked Go generator in `fixtures/`; this
/// test translates all three source comparisons without claiming a Rust codec
/// implementation.
#[test]
fn test_partial_next() {
    let key_a = fixture("key_a");
    let key_b = fixture("key_b");
    let seek_key = fixture("seek_key");

    let next_key = seek_key.next();
    assert_eq!(next_key.compare(&key_a), Ordering::Less);

    let next_partial_key = seek_key.prefix_next();
    assert_eq!(next_partial_key.compare(&key_a), Ordering::Greater);
    assert_eq!(next_partial_key.compare(&key_b), Ordering::Less);
}

/// Direct table translation of all seven rows in
/// `pkg/kv/key_test.go:63-113 TestIsPoint`.
#[test]
fn test_is_point() {
    let cases: &[(&[u8], &[u8], bool)] = &[
        (b"rowkey1", b"rowkey2", true),
        (b"rowkey1", b"rowkey3", false),
        (b"", &[0], true),
        (&[123, 123, 255, 255], &[123, 124, 0, 0], true),
        (&[123, 123, 255, 255], &[123, 124, 0, 1], false),
        (&[123, 123], &[123, 123, 0], true),
        (&[255], &[0], false),
    ];

    for (row, (start, end, expected)) in cases.iter().enumerate() {
        let range = KeyRange::new(Key::from_bytes(*start), Key::from_bytes(*end));
        assert_eq!(range.is_point(), *expected, "source row {row}");
    }
}

/// Direct translation of all four assertions in
/// `pkg/kv/version_test.go:25-34 TestVersion`.
#[test]
fn test_version() {
    let less = Version::new(42).cmp(&Version::new(43));
    let greater = Version::new(42).cmp(&Version::new(41));
    let equal = Version::new(42).cmp(&Version::new(42));

    assert!(less.is_lt());
    assert!(greater.is_gt());
    assert!(equal.is_eq());
    assert!(MIN_VERSION < MAX_VERSION);
}

/// Direct translation of `pkg/kv/error_test.go:25-43 TestError`, extended to
/// pin every error identity declared by `pkg/kv/error.go`.
#[test]
fn test_error_identity() {
    assert_eq!(ErrorClass::Kv.as_u8(), 8);
    assert_eq!(ErrorClass::TiKv.as_u8(), 24);

    let original_test_error_prototypes = [
        (&ERR_NOT_EXIST, 8021),
        (&ERR_TXN_RETRYABLE, 8022),
        (&ERR_CANNOT_SET_NIL_VALUE, 8023),
        (&ERR_INVALID_TXN, 8024),
        (&ERR_TXN_TOO_LARGE, 8004),
        (&ERR_ENTRY_TOO_LARGE, 8025),
        (&ERR_NOT_IMPLEMENTED, 8026),
        (&ERR_WRITE_CONFLICT, 9007),
        (&ERR_WRITE_CONFLICT_IN_TIDB, 8005),
    ];
    assert_eq!(original_test_error_prototypes.len(), 9);
    for (error, registered_code) in original_test_error_prototypes {
        let sql_error_code = error.mysql_code().as_u16();
        assert_ne!(sql_error_code, 1105);
        assert_eq!(sql_error_code, registered_code);
    }

    let source_error_rows = [
        (&ERR_NOT_EXIST, ErrorClass::Kv, MysqlErrorCode::NotExist, "Error: key not exist", &[][..]),
        (&ERR_TXN_RETRYABLE, ErrorClass::Kv, MysqlErrorCode::TxnRetryable, "Error: KV error safe to retry %s [try again later]", &[0][..]),
        (&ERR_CANNOT_SET_NIL_VALUE, ErrorClass::Kv, MysqlErrorCode::CannotSetNilValue, "can not set nil value", &[][..]),
        (&ERR_INVALID_TXN, ErrorClass::Kv, MysqlErrorCode::InvalidTxn, "invalid transaction", &[][..]),
        (&ERR_TXN_TOO_LARGE, ErrorClass::Kv, MysqlErrorCode::TxnTooLarge, "Transaction is too large, size: %d", &[][..]),
        (&ERR_ENTRY_TOO_LARGE, ErrorClass::Kv, MysqlErrorCode::EntryTooLarge, "entry too large, the max entry size is %d, the size of data is %d", &[][..]),
        (&ERR_KEY_TOO_LARGE, ErrorClass::Kv, MysqlErrorCode::KeyTooLarge, "key is too large, the size of given key is %d", &[][..]),
        (&ERR_KEY_EXISTS, ErrorClass::Kv, MysqlErrorCode::DupEntry, "Duplicate entry '%-.64s' for key '%-.192s'", &[0][..]),
        (&ERR_NOT_IMPLEMENTED, ErrorClass::Kv, MysqlErrorCode::NotImplemented, "not implemented", &[][..]),
        (&ERR_WRITE_CONFLICT, ErrorClass::Kv, MysqlErrorCode::WriteConflict, "Write conflict, txnStartTS=%d, conflictStartTS=%d, conflictCommitTS=%d, key=%s%s%s%s, reason=%s [try again later]", &[3, 4, 5, 6][..]),
        (&ERR_WRITE_CONFLICT_IN_TIDB, ErrorClass::Kv, MysqlErrorCode::WriteConflictInTiDb, "Write conflict, txnStartTS %d is stale [try again later]", &[][..]),
        (&ERR_LOCK_EXPIRE, ErrorClass::TiKv, MysqlErrorCode::LockExpire, "TTL manager has timed out, pessimistic locks may expire, please commit or rollback this transaction", &[][..]),
        (&ERR_ASSERTION_FAILED, ErrorClass::TiKv, MysqlErrorCode::AssertionFailed, "assertion failed: key: %s, assertion: %s, start_ts: %v, existing start ts: %v, existing commit ts: %v", &[0][..]),
    ];

    for (error, class, code, template, redact_arg_positions) in source_error_rows {
        assert_eq!(error.class(), class);
        assert_eq!(error.mysql_code(), code);
        assert_eq!(error.message_template(), template);
        assert_eq!(error.redact_arg_positions(), redact_arg_positions);
        assert_eq!(error.to_string(), template);
        assert_eq!(
            error.rfc_code(),
            format!("{}:{}", class.name(), code.as_u16())
        );
    }
}

/// Direct translation of `pkg/kv/key_test.go:115-119 TestBasicFunc`, including
/// the other two source identities accepted by the classifier.
#[test]
fn test_basic_func() {
    assert!(!is_txn_retryable_error(None));
    assert!(is_txn_retryable_error(Some(&ERR_TXN_RETRYABLE)));
    assert!(is_txn_retryable_error(Some(&ERR_WRITE_CONFLICT)));
    assert!(is_txn_retryable_error(Some(&ERR_WRITE_CONFLICT_IN_TIDB)));
    assert!(!is_txn_retryable_error(Some(&ERR_NOT_EXIST)));
    assert!(!is_txn_retryable_error(Some(&std::io::Error::other(
        "test"
    ))));

    assert!(is_err_not_found(Some(&ERR_NOT_EXIST)));
    assert!(!is_err_not_found(None));
    assert!(!is_err_not_found(Some(&ERR_TXN_RETRYABLE)));
}

/// Direct translation of `pkg/kv/error.go:97-101 GenKeyExistsErr` with the
/// original MySQL template's character-precision behavior.
#[test]
fn test_gen_key_exists_err() {
    let error = gen_key_exists_err(&["a", "b", "c"], "table.index");
    assert!(ERR_KEY_EXISTS.equal(&error));
    assert_eq!(error.mysql_code(), MysqlErrorCode::DupEntry);
    assert_eq!(
        error.to_string(),
        "Duplicate entry 'a-b-c' for key 'table.index'"
    );

    assert_eq!(
        gen_key_exists_err::<&str>(&[], "empty").to_string(),
        "Duplicate entry '' for key 'empty'"
    );

    let long_value = "好".repeat(65);
    let long_name = "索".repeat(193);
    let error = gen_key_exists_err(&[long_value.as_str()], &long_name);
    assert_eq!(
        error.to_string(),
        format!(
            "Duplicate entry '{}' for key '{}'",
            "好".repeat(64),
            "索".repeat(192)
        )
    );
    assert_eq!(TXN_RETRYABLE_MARK, "[try again later]");
}

/// Direct translation of every assertion in
/// `pkg/kv/checker_test.go:25-34 TestIsRequestTypeSupported`.
#[test]
fn test_is_request_type_supported() {
    const TIPB_EXPR_TYPE_SUM_INT: i64 = 3021;
    let checker = RequestTypeSupportedChecker;

    assert!(checker.is_request_type_supported(REQ_TYPE_SELECT, REQ_SUB_TYPE_GROUP_BY));
    assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_SIGNATURE));
    assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_DESC));
    assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_SIGNATURE));
    assert!(checker.is_request_type_supported(REQ_TYPE_SELECT, TIPB_EXPR_TYPE_SUM_INT));
    assert!(!checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_ANALYZE_IDX));
    assert!(checker.is_request_type_supported(REQ_TYPE_ANALYZE, 0));
    assert!(!checker.is_request_type_supported(REQ_TYPE_CHECKSUM, 0));
}

/// Direct translation of every row and assertion in
/// `pkg/kv/option_test.go:23-61 TestSetCDCWriteSource`.
#[test]
fn test_set_cdc_write_source() {
    let cases = [
        ("cdc write source is set", 1, true, 1, None),
        ("cdc write source is not set", 0, false, 0, None),
        (
            "cdc write source is not valid",
            16,
            false,
            0,
            Some("out of TiCDC write source range"),
        ),
    ];

    for (name, cdc_source, expected_set, expected_source, expected_error) in cases {
        let mut txn_source = 0;
        let result = set_cdc_write_source(&mut txn_source, cdc_source);
        if let Some(expected_error) = expected_error {
            assert!(
                result.expect_err(name).to_string().contains(expected_error),
                "source row {name}"
            );
            continue;
        }
        result.expect(name);
        assert_eq!(is_cdc_write_source_set(txn_source), expected_set, "{name}");
        assert_eq!(get_cdc_write_source(txn_source), expected_source, "{name}");
    }
}

/// Direct translation of every row and assertion in
/// `pkg/kv/option_test.go:63-111 TestSetLossyDDLReorgSource`.
#[test]
fn test_set_lossy_ddl_reorg_source() {
    let cases = [
        ("lossy ddl reorg source is set/empty", 0, 1, true, 1, None),
        ("lossy ddl reorg source is set/CDC", 12, 1, true, 1, None),
        ("lossy ddl reorg source is not set", 12, 0, false, 0, None),
        (
            "lossy ddl reorg source is not valid",
            12,
            256,
            false,
            0,
            Some("out of lossy DDL reorg source range"),
        ),
    ];

    for (name, mut current_source, lossy_source, expected_set, expected_source, expected_error) in
        cases
    {
        let result = set_lossy_ddl_reorg_source(&mut current_source, lossy_source);
        if let Some(expected_error) = expected_error {
            assert!(
                result.expect_err(name).to_string().contains(expected_error),
                "source row {name}"
            );
            continue;
        }
        result.expect(name);
        assert_eq!(
            is_lossy_ddl_reorg_source_set(current_source),
            expected_set,
            "{name}"
        );
        assert_eq!(
            get_lossy_ddl_reorg_source(current_source),
            expected_source,
            "{name}"
        );
    }
}

/// Direct translation of `pkg/kv/txn_test.go:70-104 TestInnerTxnStartTsBox`.
///
/// The source test uses the process-global box and an oracle-backed wall clock.
/// This test keeps the same store/delete/minimum assertions on an explicit box;
/// the server-global registry and long-running-transaction logging remain
/// outside the dependency-closed txnkv crate.
#[test]
fn test_inner_txn_start_ts_box() {
    let timestamps = InnerTxnStartTsBox::new();

    timestamps.store_inner_txn_ts(5);
    assert!(timestamps.contains(5));
    timestamps.delete_inner_txn_ts(5);
    assert!(!timestamps.contains(5));

    // Values stand in for the oracle timestamps in the source rows. Their
    // ordering is the only input to getMinStartTS's pure selection rule.
    let lower_limit = 100;
    let current_min = 500;
    for start_ts in [10, 200, 300, 400] {
        timestamps.store_inner_txn_ts(start_ts);
    }
    assert_eq!(
        get_min_inner_txn_start_ts(
            &timestamps,
            SystemTime::UNIX_EPOCH,
            lower_limit,
            current_min
        ),
        200
    );

    timestamps.delete_inner_txn_ts(10);
    timestamps.delete_inner_txn_ts(200);
    timestamps.delete_inner_txn_ts(300);
    timestamps.delete_inner_txn_ts(400);
    assert_eq!(
        get_min_inner_txn_start_ts(
            &timestamps,
            SystemTime::UNIX_EPOCH,
            lower_limit,
            current_min
        ),
        current_min
    );
    assert_eq!(
        get_min_inner_txn_start_ts(
            &timestamps,
            SystemTime::UNIX_EPOCH,
            current_min,
            lower_limit
        ),
        lower_limit
    );
}
