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

//! client-go's `tikverr` errors as the transaction read path carries them
//! (the input of Go `pkg/store/driver/error/error.go`'s `ToTiDBErr`, which is
//! `tidb_store_driver_error::driver_error`).

use std::fmt;

use crate::driver::transaction_error::{Redaction, WriteConflict};

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
    /// TiKV confirmed that shared-lock ownership was lost during an upgrade.
    /// The key is already rendered under the caller's redaction policy.
    SharedLockLost {
        /// Transaction start timestamp.
        start_ts: u64,
        /// Redacted key text used by the source message template.
        key: String,
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
    /// Classifies the longest effective client-go backoff when its budget
    /// is exhausted, using that category's source error identity or message.
    #[must_use]
    pub fn from_backoff(kind: crate::retry::RegionBackoffKind, detail: &str) -> Self {
        use crate::retry::RegionBackoffKind;
        match kind {
            RegionBackoffKind::TikvRpc => Self::TiKvServerTimeout,
            RegionBackoffKind::TiFlashRpc => Self::TiFlashServerTimeout,
            RegionBackoffKind::RegionMiss | RegionBackoffKind::RegionScheduling => {
                Self::RegionUnavailable
            }
            RegionBackoffKind::TikvServerBusy => Self::Other(detail.to_owned()),
            RegionBackoffKind::StaleCommand => Self::TiKvStaleCommand,
            RegionBackoffKind::MaxTimestampNotSynced => Self::TiKvMaxTimestampNotSynced,
            RegionBackoffKind::TxnLock
            | RegionBackoffKind::TxnLockFast
            | RegionBackoffKind::TxnNotFound => Self::ResolveLockTimeout,
            RegionBackoffKind::TikvDiskFull => Self::Other("tikv disk full".to_owned()),
            RegionBackoffKind::RegionRecoveryInProgress => {
                Self::Other("region is being online unsafe recovered".to_owned())
            }
            RegionBackoffKind::RegionNotInitialized => {
                Self::Other("region not Initialized".to_owned())
            }
            RegionBackoffKind::IsWitness => Self::Other("peer is witness".to_owned()),
            RegionBackoffKind::PdRpc => Self::PdServerTimeout {
                message: detail.to_owned(),
            },
        }
    }

    /// Adds one error-chain layer.
    pub fn context(self, message: impl Into<String>) -> Self {
        Self::Context {
            message: message.into(),
            source: Box::new(self),
        }
    }

    /// The innermost error under every context wrapper.
    #[must_use]
    pub fn root_cause(&self) -> &Self {
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
            Self::SharedLockLost { start_ts, key } => write!(
                formatter,
                "shared lock lost during upgrade: txnStartTS={start_ts}, key={key}"
            ),
            Self::Retryable { message } => formatter.write_str(message),
            Self::Other(message) => formatter.write_str(message),
            Self::Context { message, source } => write!(formatter, "{message}: {source}"),
        }
    }
}

impl std::error::Error for StorageDriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Context { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}
