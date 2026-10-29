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

//! Go `pkg/store/driver/error`'s `ToTiDBErr`: a client-level storage error
//! ([`StorageDriverError`], client-go's `tikverr` set) as TiDB reports it.

use std::fmt;

use tidb_errno as tidb;
use tidb_mysql::errors::{ErrMessage, FormatArg};
use tidb_terror::{TerrorClass, TerrorCode, TerrorError, ERR_RESULT_UNDETERMINED};
use tidb_txnkv::{
    gen_entry_too_large_err, gen_key_too_large_err, gen_shared_lock_lost_err,
    gen_txn_too_large_err, gen_write_conflict_in_tidb_err, KvError, StorageDriverError,
    ERR_CANNOT_SET_NIL_VALUE, ERR_INVALID_TXN, ERR_NOT_EXIST, ERR_WRITE_CONFLICT,
};

fn catalog_error(
    class: TerrorClass,
    code: u16,
    message: ErrMessage,
    arguments: &[FormatArg],
) -> ConvertedDriverError {
    let prototype = TerrorError::registered_standard(
        class,
        TerrorCode::new(
            isize::try_from(code).expect("MySQL error code must fit the source int domain"),
        ),
        message,
    );
    let error = if arguments.is_empty() {
        prototype
    } else {
        prototype.fast_generate(message.raw, arguments)
    };
    ConvertedDriverError::Terror(error)
}


/// TiDB-side result of converting a storage-driver error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConvertedDriverError {
    /// Existing `pkg/kv/error.go` identity, optionally generated with arguments.
    Kv(KvError),
    /// Shared `pkg/parser/terror` class/code/RFC identity.
    Terror(TerrorError),
    /// Unrecognized errors retain their complete typed identity and chain.
    Passthrough(StorageDriverError),
}

impl fmt::Display for ConvertedDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kv(error) => error.fmt(formatter),
            Self::Terror(error) => error.fmt(formatter),
            Self::Passthrough(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ConvertedDriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Passthrough(error) => Some(error),
            _ => None,
        }
    }
}

/// Converts recognized client errors after following all context wrappers.
#[must_use]
pub fn to_tidb_driver_error(error: &StorageDriverError) -> ConvertedDriverError {
    match error.root_cause() {
        StorageDriverError::NotFound => ConvertedDriverError::Kv(ERR_NOT_EXIST.clone()),
        StorageDriverError::ResultUndetermined => {
            ConvertedDriverError::Terror(ERR_RESULT_UNDETERMINED.clone())
        }
        StorageDriverError::WriteConflictInLatch { start_ts } => {
            ConvertedDriverError::Kv(gen_write_conflict_in_tidb_err(*start_ts))
        }
        StorageDriverError::SharedLockLost { start_ts, key } => {
            ConvertedDriverError::Kv(gen_shared_lock_lost_err(*start_ts, key))
        }
        StorageDriverError::TxnTooLarge { size } => {
            ConvertedDriverError::Kv(gen_txn_too_large_err(*size))
        }
        StorageDriverError::EntryTooLarge { limit, size } => {
            ConvertedDriverError::Kv(gen_entry_too_large_err(*limit, *size))
        }
        StorageDriverError::KeyTooLarge { key_size } => {
            ConvertedDriverError::Kv(gen_key_too_large_err(*key_size))
        }
        StorageDriverError::CannotSetNilValue => {
            ConvertedDriverError::Kv(ERR_CANNOT_SET_NIL_VALUE.clone())
        }
        StorageDriverError::InvalidTxn => ConvertedDriverError::Kv(ERR_INVALID_TXN.clone()),
        StorageDriverError::TiKvServerTimeout => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVServerTimeout,
            tidb::errname::ErrTiKVServerTimeout,
            &[],
        ),
        StorageDriverError::PdServerTimeout { message } => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrPDServerTimeout,
            tidb::errname::ErrPDServerTimeout,
            &[message.as_str().into()],
        ),
        StorageDriverError::TiFlashServerTimeout => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTiFlashServerTimeout,
            tidb::errname::ErrTiFlashServerTimeout,
            &[],
        ),
        StorageDriverError::QueryInterrupted => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrQueryInterrupted,
            tidb::errname::ErrQueryInterrupted,
            &[],
        ),
        StorageDriverError::MaxExecutionTimeExceeded => catalog_error(
            TerrorClass::Executor,
            tidb::errcode::ErrMaxExecTimeExceeded,
            tidb::errname::ErrMaxExecTimeExceeded,
            &[],
        ),
        StorageDriverError::QueryMemoryExceeded => catalog_error(
            TerrorClass::Executor,
            tidb::errcode::ErrMemoryExceedForQuery,
            tidb::errname::ErrMemoryExceedForQuery,
            &[(-1_i64).into()],
        ),
        StorageDriverError::ServerMemoryExceeded => catalog_error(
            TerrorClass::Executor,
            tidb::errcode::ErrMemoryExceedForInstance,
            tidb::errname::ErrMemoryExceedForInstance,
            &[(-1_i64).into()],
        ),
        StorageDriverError::RunawayQueryExceeded => catalog_error(
            TerrorClass::Executor,
            tidb::errcode::ErrResourceGroupQueryRunawayInterrupted,
            tidb::errname::ErrResourceGroupQueryRunawayInterrupted,
            &["exceed tidb side".into()],
        ),
        StorageDriverError::TiKvServerBusy => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVServerBusy,
            tidb::errname::ErrTiKVServerBusy,
            &[],
        ),
        StorageDriverError::TiFlashServerBusy => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTiFlashServerBusy,
            tidb::errname::ErrTiFlashServerBusy,
            &[],
        ),
        StorageDriverError::TxnAbortedByGc {
            txn_start_ts,
            txn_start_ts_time,
            txn_safe_point,
            txn_safe_point_time,
        } => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTxnAbortedByGC,
            tidb::errname::ErrTxnAbortedByGC,
            &[
                (*txn_start_ts).into(),
                txn_start_ts_time.as_str().into(),
                (*txn_safe_point).into(),
                txn_safe_point_time.as_str().into(),
            ],
        ),
        StorageDriverError::GcTooEarly {
            txn_start_ts_time,
            gc_safe_point_time,
        } => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTxnAbortedByGC,
            tidb::errname::ErrTxnAbortedByGC,
            &[
                "<unknown>".into(),
                txn_start_ts_time.as_str().into(),
                "<unknown>".into(),
                gc_safe_point_time.as_str().into(),
            ],
        ),
        StorageDriverError::TiKvStaleCommand => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVStaleCommand,
            tidb::errname::ErrTiKVStaleCommand,
            &[],
        ),
        StorageDriverError::TiKvMaxTimestampNotSynced => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVMaxTimestampNotSynced,
            tidb::errname::ErrTiKVMaxTimestampNotSynced,
            &[],
        ),
        StorageDriverError::LockAcquireFailAndNoWaitSet => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrLockAcquireFailAndNoWaitSet,
            tidb::errname::ErrLockAcquireFailAndNoWaitSet,
            &[],
        ),
        StorageDriverError::ResolveLockTimeout => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrResolveLockTimeout,
            tidb::errname::ErrResolveLockTimeout,
            &[],
        ),
        StorageDriverError::LockWaitTimeout => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrLockWaitTimeout,
            tidb::errname::ErrLockWaitTimeout,
            &[],
        ),
        StorageDriverError::RegionUnavailable => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrRegionUnavailable,
            tidb::errname::ErrRegionUnavailable,
            &[],
        ),
        StorageDriverError::TokenLimit { store_id } => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrTiKVStoreLimit,
            tidb::errname::ErrTiKVStoreLimit,
            &[(*store_id).into()],
        ),
        StorageDriverError::Unknown => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrUnknown,
            tidb::errname::ErrUnknown,
            &[],
        ),
        StorageDriverError::ResourceGroupNotExists { name } => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrResourceGroupNotExists,
            tidb::errname::ErrResourceGroupNotExists,
            &[name.as_str().into()],
        ),
        StorageDriverError::ResourceGroupConfigUnavailable => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrResourceGroupConfigUnavailable,
            tidb::errname::ErrResourceGroupConfigUnavailable,
            &[],
        ),
        StorageDriverError::ResourceGroupThrottled => catalog_error(
            TerrorClass::TiKv,
            tidb::errcode::ErrResourceGroupThrottled,
            tidb::errname::ErrResourceGroupThrottled,
            &[],
        ),
        StorageDriverError::WriteConflict { conflict: None, .. } => {
            ConvertedDriverError::Kv(ERR_WRITE_CONFLICT.clone())
        }
        // Go `ToTiDBErr` keeps client-go's write conflict and retryable errors
        // as they are; `extractKeyErr` in the transaction driver decodes them.
        StorageDriverError::WriteConflict { .. } | StorageDriverError::Retryable { .. } => {
            ConvertedDriverError::Passthrough(error.clone())
        }
        StorageDriverError::Other(_) => ConvertedDriverError::Passthrough(error.clone()),
        StorageDriverError::Context { .. } => unreachable!("root_cause removes all context"),
    }
}
