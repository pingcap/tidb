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

//! Root-cause-aware storage-driver error conversion translated from
//! `pkg/store/driver/error/error.go`.

use std::fmt;

use tidb_error::mysql::FormatArg;
use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError, ERR_RESULT_UNDETERMINED};
use tidb_error::{tidb, ErrMessage};

use crate::driver::transaction_error::{Redaction, TransactionError, WriteConflict};

use crate::{
    gen_entry_too_large_err, gen_key_too_large_err, gen_txn_too_large_err,
    gen_write_conflict_in_tidb_err, KvError, ERR_CANNOT_SET_NIL_VALUE, ERR_INVALID_TXN,
    ERR_NOT_EXIST, ERR_WRITE_CONFLICT,
};

/// Typed client-side errors entering transaction read adapters.
///
/// The source accepts external client-go errors through `error`. This enum is
/// the Rust boundary until a TiKV client crate supplies those concrete types.
/// Its client-go and PD variants map one-for-one to source branches, while the
/// transaction-specific variants are retained for the adjacent transaction
/// diagnostic authority. `Context` models the linear wrappers exercised by
/// `error_test.go`; arbitrary Go `Is`/`As` implementations and multi-error
/// chains remain outside this typed boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageDriverError {
    /// client-go not-found identity.
    NotFound,
    /// client-go undetermined-result marker.
    ResultUndetermined,
    /// client-go latch write conflict.
    WriteConflictInLatch {
        /// Transaction start timestamp reported by the latch conflict.
        start_ts: u64,
    },
    /// client-go transaction buffer exceeds its total limit.
    TxnTooLarge {
        /// Transaction-buffer size reported by client-go.
        size: i64,
    },
    /// client-go entry exceeds its configured limit.
    EntryTooLarge {
        /// Configured maximum entry size.
        limit: u64,
        /// Actual entry size.
        size: u64,
    },
    /// client-go key exceeds its configured limit.
    KeyTooLarge {
        /// Actual encoded key size.
        key_size: i64,
    },
    /// client-go cannot store a nil value.
    CannotSetNilValue,
    /// client-go invalid transaction identity.
    InvalidTxn,
    /// client-go TiKV server timeout identity.
    TiKvServerTimeout,
    /// client-go PD timeout, whose concrete error text becomes the catalog argument.
    PdServerTimeout {
        /// Concrete PD timeout text used as the catalog argument.
        message: String,
    },
    /// client-go TiFlash server timeout identity.
    TiFlashServerTimeout,
    /// `sqlkiller.QueryInterrupted`.
    QueryInterrupted,
    /// `sqlkiller.MaxExecTimeExceeded`.
    MaxExecutionTimeExceeded,
    /// `sqlkiller.QueryMemoryExceeded`.
    QueryMemoryExceeded,
    /// `sqlkiller.ServerMemoryExceeded`.
    ServerMemoryExceeded,
    /// `sqlkiller.RunawayQueryExceeded`.
    RunawayQueryExceeded,
    /// client-go TiKV server-busy identity.
    TiKvServerBusy,
    /// client-go TiFlash server-busy identity.
    TiFlashServerBusy,
    /// client-go's current transaction-aborted-by-GC detail.
    TxnAbortedByGc {
        /// Transaction start timestamp.
        txn_start_ts: u64,
        /// Human-readable transaction start time.
        txn_start_ts_time: String,
        /// GC safe-point timestamp that aborted the transaction.
        txn_safe_point: u64,
        /// Human-readable GC safe-point time.
        txn_safe_point_time: String,
    },
    /// Legacy client-go GC-too-early detail, which lacks numeric timestamps.
    GcTooEarly {
        /// Human-readable transaction start time.
        txn_start_ts_time: String,
        /// Human-readable GC safe-point time.
        gc_safe_point_time: String,
    },
    /// client-go stale-command identity.
    TiKvStaleCommand,
    /// client-go max-timestamp-not-synced identity.
    TiKvMaxTimestampNotSynced,
    /// client-go NOWAIT lock-acquisition failure identity.
    LockAcquireFailAndNoWaitSet,
    /// client-go resolve-lock timeout identity.
    ResolveLockTimeout,
    /// client-go lock-wait timeout identity.
    LockWaitTimeout,
    /// client-go region-unavailable identity.
    RegionUnavailable,
    /// client-go store token limit detail.
    TokenLimit {
        /// Store whose request-token limit was reached.
        store_id: u64,
    },
    /// client-go unknown identity (distinct from an unrecognized error type).
    Unknown,
    /// PD reports that a named resource group does not exist.
    ResourceGroupNotExists {
        /// Missing resource-group name.
        name: String,
    },
    /// PD cannot provide resource-group configuration.
    ResourceGroupConfigUnavailable,
    /// PD throttled the request because the resource-group quota was exceeded.
    ResourceGroupThrottled,
    /// client-go transaction write conflict with optional TiKV detail.
    WriteConflict {
        /// TiKV conflict detail; nil in Go maps to `None`.
        conflict: Option<WriteConflict>,
        /// Process redaction mode captured at the conversion boundary.
        redaction: Redaction,
    },
    /// client-go retryable transaction error text.
    Retryable {
        /// Raw retry detail, including a possible serialized lock key.
        message: String,
    },
    /// An error type not recognized by the source conversion table.
    Other(String),
    /// `errors.Trace`, `errors.WithStack`, or `errors.Wrap` context.
    Context {
        /// Human-readable wrapping context; conversion intentionally ignores it
        /// for recognized root causes, as Go `errors.As`/`errors.Is` does.
        message: String,
        /// Wrapped source error.
        source: Box<Self>,
    },
}

impl StorageDriverError {
    /// Adds one error-chain layer.
    pub fn context(self, message: impl Into<String>) -> Self {
        Self::Context {
            message: message.into(),
            source: Box::new(self),
        }
    }

    fn root_cause(&self) -> &Self {
        let mut error = self;
        while let Self::Context { source, .. } = error {
            error = source;
        }
        error
    }
}

impl fmt::Display for StorageDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("key not exist"),
            Self::ResultUndetermined => formatter.write_str("result undetermined"),
            Self::WriteConflictInLatch { start_ts } => {
                write!(formatter, "write conflict in latch at {start_ts}")
            }
            Self::TxnTooLarge { size } => write!(formatter, "transaction too large: {size}"),
            Self::EntryTooLarge { limit, size } => {
                write!(formatter, "entry too large: limit {limit}, size {size}")
            }
            Self::KeyTooLarge { key_size } => write!(formatter, "key too large: {key_size}"),
            Self::CannotSetNilValue => formatter.write_str("cannot set nil value"),
            Self::InvalidTxn => formatter.write_str("invalid transaction"),
            Self::TiKvServerTimeout => formatter.write_str("TiKV server timeout"),
            Self::PdServerTimeout { message } => formatter.write_str(message),
            Self::TiFlashServerTimeout => formatter.write_str("TiFlash server timeout"),
            Self::QueryInterrupted => formatter.write_str("query interrupted"),
            Self::MaxExecutionTimeExceeded => {
                formatter.write_str("maximum execution time exceeded")
            }
            Self::QueryMemoryExceeded => formatter.write_str("query memory exceeded"),
            Self::ServerMemoryExceeded => formatter.write_str("server memory exceeded"),
            Self::RunawayQueryExceeded => formatter.write_str("runaway query exceeded"),
            Self::TiKvServerBusy => formatter.write_str("TiKV server busy"),
            Self::TiFlashServerBusy => formatter.write_str("TiFlash server busy"),
            Self::TxnAbortedByGc { .. } => formatter.write_str("transaction aborted by GC"),
            Self::GcTooEarly { .. } => formatter.write_str("GC too early"),
            Self::TiKvStaleCommand => formatter.write_str("TiKV stale command"),
            Self::TiKvMaxTimestampNotSynced => formatter.write_str("TiKV max timestamp not synced"),
            Self::LockAcquireFailAndNoWaitSet => {
                formatter.write_str("lock acquire failed with NOWAIT")
            }
            Self::ResolveLockTimeout => formatter.write_str("resolve lock timeout"),
            Self::LockWaitTimeout => formatter.write_str("lock wait timeout"),
            Self::RegionUnavailable => formatter.write_str("region unavailable"),
            Self::TokenLimit { store_id } => write!(formatter, "store token limit: {store_id}"),
            Self::Unknown => formatter.write_str("unknown client error"),
            Self::ResourceGroupNotExists { name } => {
                write!(formatter, "resource group not found: {name}")
            }
            Self::ResourceGroupConfigUnavailable => {
                formatter.write_str("resource group config unavailable")
            }
            Self::ResourceGroupThrottled => formatter.write_str("resource group throttled"),
            Self::WriteConflict { .. } => formatter.write_str("write conflict"),
            Self::Retryable { message } => formatter.write_str(message),
            Self::Other(message) => formatter.write_str(message),
            Self::Context { message, source } => write!(formatter, "{message}: {source}"),
        }
    }
}

fn catalog_error(
    class: TerrorClass,
    code: u16,
    message: ErrMessage,
    arguments: &[FormatArg],
) -> ConvertedDriverError {
    let prototype =
        TerrorError::registered_standard(class, TerrorCode::new(i32::from(code)), message);
    let error = if arguments.is_empty() {
        prototype
    } else {
        prototype.fast_generate(message.raw, arguments)
    };
    ConvertedDriverError::Terror(error)
}

impl std::error::Error for StorageDriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Context { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

/// TiDB-side result of converting a storage-driver error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConvertedDriverError {
    /// Existing `pkg/kv/error.go` identity, optionally generated with arguments.
    Kv(KvError),
    /// Shared `pkg/parser/terror` class/code/RFC identity.
    Terror(TerrorError),
    /// Transaction-specific error after source-exact key diagnostics.
    Transaction(TransactionError),
    /// Unrecognized errors retain their complete typed identity and chain.
    Passthrough(StorageDriverError),
}

impl fmt::Display for ConvertedDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kv(error) => error.fmt(formatter),
            Self::Terror(error) => error.fmt(formatter),
            Self::Transaction(error) => error.fmt(formatter),
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
        StorageDriverError::WriteConflict {
            conflict,
            redaction,
        } => ConvertedDriverError::Transaction(TransactionError::WriteConflict {
            conflict: conflict.clone(),
            redaction: *redaction,
        }),
        StorageDriverError::Retryable { message } => {
            ConvertedDriverError::Transaction(TransactionError::Retryable(message.clone()))
        }
        StorageDriverError::Other(_) => ConvertedDriverError::Passthrough(error.clone()),
        StorageDriverError::Context { .. } => unreachable!("root_cause removes all context"),
    }
}
