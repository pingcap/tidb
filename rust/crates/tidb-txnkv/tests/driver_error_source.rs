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

//! Direct storage-driver error-conversion obligations from TiDB's Go tests.

use tidb_error::terror::{TerrorClass, CODE_RESULT_UNDETERMINED};
use tidb_error::tidb;
use tidb_txnkv::{to_tidb_driver_error, ConvertedDriverError, MysqlErrorCode, StorageDriverError};

#[test]
fn test_convert_error_follows_every_source_wrapper_shape() {
    let errors = [
        StorageDriverError::ResultUndetermined,
        StorageDriverError::ResultUndetermined.context("trace"),
        StorageDriverError::ResultUndetermined.context("stack"),
        StorageDriverError::ResultUndetermined.context("dummy"),
    ];

    for error in errors {
        let ConvertedDriverError::Terror(converted) = to_tidb_driver_error(&error) else {
            panic!("result-undetermined must use the shared terror identity")
        };
        assert_eq!(converted.class(), TerrorClass::Global);
        assert_eq!(converted.code(), CODE_RESULT_UNDETERMINED);
        assert_eq!(converted.rfc_code(), "global:2");
    }
}

#[test]
fn test_mem_buffer_oversize_error_source_messages() {
    let cases = [
        (
            StorageDriverError::TxnTooLarge { size: 100 },
            MysqlErrorCode::TxnTooLarge,
            "Transaction is too large, size: 100",
        ),
        (
            StorageDriverError::EntryTooLarge {
                limit: 10,
                size: 20,
            },
            MysqlErrorCode::EntryTooLarge,
            "entry too large, the max entry size is 10, the size of data is 20",
        ),
        (
            StorageDriverError::KeyTooLarge {
                key_size: i64::from(u16::MAX) + 1,
            },
            MysqlErrorCode::KeyTooLarge,
            "key is too large, the size of given key is 65536",
        ),
    ];

    for (source, expected_code, expected_message) in cases {
        let converted = to_tidb_driver_error(&source);
        let ConvertedDriverError::Kv(error) = &converted else {
            panic!("source size errors must map to existing KV identities")
        };
        assert_eq!(error.mysql_code(), expected_code);
        assert!(converted.to_string().contains(expected_message));
    }
}

#[test]
fn immediate_read_path_identities_reuse_existing_kv_errors() {
    let cases = [
        (StorageDriverError::NotFound, MysqlErrorCode::NotExist),
        (
            StorageDriverError::CannotSetNilValue,
            MysqlErrorCode::CannotSetNilValue,
        ),
        (StorageDriverError::InvalidTxn, MysqlErrorCode::InvalidTxn),
    ];
    for (source, expected_code) in cases {
        let ConvertedDriverError::Kv(converted) = to_tidb_driver_error(&source) else {
            panic!("source identity must map to its existing KV error")
        };
        assert_eq!(converted.mysql_code(), expected_code);
    }
}

#[test]
fn latch_write_conflict_preserves_start_ts_through_nested_wrappers() {
    let source = StorageDriverError::WriteConflictInLatch {
        start_ts: 408_090_278_408_224_772,
    }
    .context("stack")
    .context("outer");

    let ConvertedDriverError::Kv(converted) = to_tidb_driver_error(&source) else {
        panic!("latch conflicts must map to ErrWriteConflictInTiDB")
    };
    assert_eq!(converted.mysql_code(), MysqlErrorCode::WriteConflictInTiDb);
    assert_eq!(
        converted.to_string(),
        "Write conflict, txnStartTS 408090278408224772 is stale [try again later]"
    );
}

#[test]
fn unrecognized_errors_preserve_outer_context() {
    let source = StorageDriverError::Other("client detail".to_owned()).context("outer");
    assert_eq!(
        to_tidb_driver_error(&source),
        ConvertedDriverError::Passthrough(source.clone())
    );
    assert_eq!(
        to_tidb_driver_error(&source).to_string(),
        "outer: client detail"
    );
}

#[test]
fn every_typed_client_branch_uses_its_source_catalog_identity() {
    let cases = vec![
        (
            StorageDriverError::TiKvServerTimeout,
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVServerTimeout,
            "TiKV server timeout",
        ),
        (
            StorageDriverError::PdServerTimeout {
                message: "request deadline exceeded".to_owned(),
            },
            TerrorClass::TiKv,
            tidb::errcode::ErrPDServerTimeout,
            "PD server timeout: request deadline exceeded",
        ),
        (
            StorageDriverError::TiFlashServerTimeout,
            TerrorClass::TiKv,
            tidb::errcode::ErrTiFlashServerTimeout,
            "TiFlash server timeout",
        ),
        (
            StorageDriverError::QueryInterrupted,
            TerrorClass::TiKv,
            tidb::errcode::ErrQueryInterrupted,
            "Query execution was interrupted",
        ),
        (
            StorageDriverError::MaxExecutionTimeExceeded,
            TerrorClass::Executor,
            tidb::errcode::ErrMaxExecTimeExceeded,
            "maximum statement execution time exceeded",
        ),
        (
            StorageDriverError::QueryMemoryExceeded,
            TerrorClass::Executor,
            tidb::errcode::ErrMemoryExceedForQuery,
            "[conn=-1]",
        ),
        (
            StorageDriverError::ServerMemoryExceeded,
            TerrorClass::Executor,
            tidb::errcode::ErrMemoryExceedForInstance,
            "[conn=-1]",
        ),
        (
            StorageDriverError::RunawayQueryExceeded,
            TerrorClass::Executor,
            tidb::errcode::ErrResourceGroupQueryRunawayInterrupted,
            "[exceed tidb side]",
        ),
        (
            StorageDriverError::TiKvServerBusy,
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVServerBusy,
            "TiKV server is busy",
        ),
        (
            StorageDriverError::TiFlashServerBusy,
            TerrorClass::TiKv,
            tidb::errcode::ErrTiFlashServerBusy,
            "TiFlash server is busy",
        ),
        (
            StorageDriverError::TiKvStaleCommand,
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVStaleCommand,
            "TiKV server reports stale command",
        ),
        (
            StorageDriverError::TiKvMaxTimestampNotSynced,
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVMaxTimestampNotSynced,
            "TiKV max timestamp is not synced",
        ),
        (
            StorageDriverError::LockAcquireFailAndNoWaitSet,
            TerrorClass::TiKv,
            tidb::errcode::ErrLockAcquireFailAndNoWaitSet,
            "NOWAIT is set",
        ),
        (
            StorageDriverError::ResolveLockTimeout,
            TerrorClass::TiKv,
            tidb::errcode::ErrResolveLockTimeout,
            "Resolve lock timeout",
        ),
        (
            StorageDriverError::LockWaitTimeout,
            TerrorClass::TiKv,
            tidb::errcode::ErrLockWaitTimeout,
            "Lock wait timeout exceeded",
        ),
        (
            StorageDriverError::RegionUnavailable,
            TerrorClass::TiKv,
            tidb::errcode::ErrRegionUnavailable,
            "Region is unavailable",
        ),
        (
            StorageDriverError::TokenLimit { store_id: 42 },
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVStoreLimit,
            "store id = 42",
        ),
        (
            StorageDriverError::Unknown,
            TerrorClass::TiKv,
            tidb::errcode::ErrUnknown,
            "Unknown error",
        ),
        (
            StorageDriverError::ResourceGroupNotExists {
                name: "latency-sensitive".to_owned(),
            },
            TerrorClass::TiKv,
            tidb::errcode::ErrResourceGroupNotExists,
            "latency-sensitive",
        ),
        (
            StorageDriverError::ResourceGroupConfigUnavailable,
            TerrorClass::TiKv,
            tidb::errcode::ErrResourceGroupConfigUnavailable,
            "Resource group configuration is unavailable",
        ),
        (
            StorageDriverError::ResourceGroupThrottled,
            TerrorClass::TiKv,
            tidb::errcode::ErrResourceGroupThrottled,
            "Exceeded resource group quota limitation",
        ),
    ];

    for (source, expected_class, expected_code, expected_message) in cases {
        let wrapped = source.context("outer client context");
        let ConvertedDriverError::Terror(converted) = to_tidb_driver_error(&wrapped) else {
            panic!("typed source branch must map to a catalog terror error")
        };
        assert_eq!(converted.class(), expected_class);
        assert_eq!(converted.code().value(), i32::from(expected_code));
        assert!(
            converted.message().contains(expected_message),
            "{} did not contain {expected_message:?}",
            converted.message()
        );
    }
}

#[test]
fn gc_abort_branches_preserve_current_and_legacy_argument_shapes() {
    let current = to_tidb_driver_error(&StorageDriverError::TxnAbortedByGc {
        txn_start_ts: 100,
        txn_start_ts_time: "start-time".to_owned(),
        txn_safe_point: 200,
        txn_safe_point_time: "safe-point-time".to_owned(),
    });
    let ConvertedDriverError::Terror(current) = current else {
        panic!("current GC abort must use the TiKV error catalog")
    };
    assert_eq!(current.class(), TerrorClass::TiKv);
    assert_eq!(
        current.code().value(),
        i32::from(tidb::errcode::ErrTxnAbortedByGC)
    );
    assert!(current.message().contains(
        "transaction start ts is 100 (start-time), txn safe point is 200 (safe-point-time)"
    ));

    let legacy = to_tidb_driver_error(&StorageDriverError::GcTooEarly {
        txn_start_ts_time: "legacy-start".to_owned(),
        gc_safe_point_time: "legacy-safe-point".to_owned(),
    });
    let ConvertedDriverError::Terror(legacy) = legacy else {
        panic!("legacy GC abort must use the current TiKV error identity")
    };
    assert_eq!(legacy.class(), TerrorClass::TiKv);
    assert_eq!(
        legacy.code().value(),
        i32::from(tidb::errcode::ErrTxnAbortedByGC)
    );
    assert!(legacy.message().contains(
        "transaction start ts is <unknown> (legacy-start), txn safe point is <unknown> (legacy-safe-point)"
    ));
}
