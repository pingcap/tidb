// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use tidb_proto::KvrpcLockInfo;

const OP_PUT: i32 = 0;
const OP_DELETE: i32 = 1;
const OP_LOCK: i32 = 2;
const OP_INSERT: i32 = 4;
const OP_PESSIMISTIC_LOCK: i32 = 5;
const OP_CHECK_NOT_EXISTS: i32 = 6;
const OP_SHARED_LOCK: i32 = 7;
const OP_SHARED_PESSIMISTIC_LOCK: i32 = 8;

/// One source-shaped optimistic prewrite lock admitted by the bounded reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimisticLock {
    /// Locked secondary or primary key returned by TiKV.
    pub key: Vec<u8>,
    /// Transaction primary key.
    pub primary: Vec<u8>,
    /// Transaction start timestamp.
    pub txn_id: u64,
    /// Source lock TTL in milliseconds.
    pub ttl_ms: u64,
    /// Source transaction size hint.
    pub txn_size: u64,
    /// Exact optimistic mutation discriminant.
    pub lock_type: i32,
    /// Minimum commit timestamp retained for diagnostics.
    pub min_commit_ts: u64,
}

/// One source-shaped pessimistic lock observed by a blocked locking statement.
///
/// A pessimistic lock has no committed value behind it, so it is never
/// resolved by committing: it is either still alive (its owner keeps it alive
/// with TxnHeartBeat) or expired and rolled back key by key. That is why it
/// carries `for_update_ts`, which the cleanup command must echo, instead of the
/// optimistic lock's `min_commit_ts`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PessimisticLock {
    /// Locked key returned by TiKV.
    pub key: Vec<u8>,
    /// Transaction primary key.
    pub primary: Vec<u8>,
    /// Transaction start timestamp.
    pub txn_id: u64,
    /// Source lock TTL in milliseconds.
    pub ttl_ms: u64,
    /// Statement timestamp the lock was acquired under.
    pub for_update_ts: u64,
    /// Milliseconds since the owner last refreshed this lock, zero if unknown.
    pub duration_to_last_update_ms: u64,
    /// Exact pessimistic mutation discriminant.
    pub lock_type: i32,
}

/// One lock blocking a pessimistic locking statement.
///
/// The two variants need different cleanup protocols, so the discriminant is
/// resolved once at admission rather than re-derived at every decision point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockingLock {
    /// A prewrite lock cleaned by CheckTxnStatus plus ResolveLock.
    Optimistic(OptimisticLock),
    /// A pessimistic lock cleaned by CheckTxnStatus plus PessimisticRollback.
    Pessimistic(PessimisticLock),
}

impl BlockingLock {
    /// Locked key common to both protocols.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Optimistic(lock) => &lock.key,
            Self::Pessimistic(lock) => &lock.key,
        }
    }

    /// Owning transaction's start timestamp.
    #[must_use]
    pub const fn txn_id(&self) -> u64 {
        match self {
            Self::Optimistic(lock) => lock.txn_id,
            Self::Pessimistic(lock) => lock.txn_id,
        }
    }

    /// Milliseconds since the owner last refreshed the lock, zero if unknown.
    ///
    /// Only a pessimistic lock reports this; TiKV sets it when it wakes a
    /// waiter, and client-go uses it to skip resolving a lock whose owner is
    /// demonstrably alive.
    #[must_use]
    pub const fn duration_to_last_update_ms(&self) -> u64 {
        match self {
            Self::Optimistic(_) => 0,
            Self::Pessimistic(lock) => lock.duration_to_last_update_ms,
        }
    }
}

/// Fail-closed lock admission errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LockAdmissionError {
    /// A pessimistic lock requires a different protocol.
    Pessimistic(i32),
    /// An async-commit lock requires secondary-status recovery.
    AsyncCommit,
    /// A transaction-file lock requires file-aware cleanup.
    TransactionFile,
    /// The lock mutation is not an admitted optimistic prewrite operation.
    UnsupportedLockType(i32),
    /// A required transaction identity or key is absent.
    MissingIdentity,
}

impl fmt::Display for LockAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pessimistic(lock_type) => {
                write!(
                    formatter,
                    "pessimistic lock type {lock_type} is outside bounded recovery"
                )
            }
            Self::AsyncCommit => {
                formatter.write_str("async-commit lock is outside bounded recovery")
            }
            Self::TransactionFile => {
                formatter.write_str("transaction-file lock is outside bounded recovery")
            }
            Self::UnsupportedLockType(lock_type) => {
                write!(
                    formatter,
                    "lock type {lock_type} is outside bounded recovery"
                )
            }
            Self::MissingIdentity => formatter.write_str("lock is missing key, primary, or txn id"),
        }
    }
}

impl std::error::Error for LockAdmissionError {}

/// Expands TiDB's shared-lock response wrapper and maps every inner LockInfo.
///
/// This deliberately matches `coprocessor.handleLockErr`: the presence of
/// `shared_lock_infos`, rather than an outer discriminant, selects expansion.
pub fn decode_lock_observation(
    observation: &KvrpcLockInfo,
) -> Result<Vec<OptimisticLock>, LockAdmissionError> {
    if !observation.shared_lock_infos.is_empty() {
        return observation
            .shared_lock_infos
            .iter()
            .map(admit_lock)
            .collect();
    }
    Ok(vec![admit_lock(observation)?])
}

/// Expands the same shared-lock wrapper for a blocked pessimistic statement,
/// which — unlike a snapshot read — must also admit pessimistic lock types.
pub fn decode_blocking_lock_observation(
    observation: &KvrpcLockInfo,
) -> Result<Vec<BlockingLock>, LockAdmissionError> {
    if !observation.shared_lock_infos.is_empty() {
        return observation
            .shared_lock_infos
            .iter()
            .map(admit_blocking_lock)
            .collect();
    }
    Ok(vec![admit_blocking_lock(observation)?])
}

fn admit_blocking_lock(lock: &KvrpcLockInfo) -> Result<BlockingLock, LockAdmissionError> {
    match lock.lock_type {
        OP_PESSIMISTIC_LOCK | OP_SHARED_PESSIMISTIC_LOCK => {
            if lock.is_txn_file {
                return Err(LockAdmissionError::TransactionFile);
            }
            if lock.key.is_empty() || lock.primary_lock.is_empty() || lock.lock_version == 0 {
                return Err(LockAdmissionError::MissingIdentity);
            }
            Ok(BlockingLock::Pessimistic(PessimisticLock {
                key: lock.key.clone(),
                primary: lock.primary_lock.clone(),
                txn_id: lock.lock_version,
                ttl_ms: lock.lock_ttl,
                for_update_ts: lock.lock_for_update_ts,
                duration_to_last_update_ms: lock.duration_to_last_update_ms,
                lock_type: lock.lock_type,
            }))
        }
        _ => Ok(BlockingLock::Optimistic(admit_lock(lock)?)),
    }
}

fn admit_lock(lock: &KvrpcLockInfo) -> Result<OptimisticLock, LockAdmissionError> {
    if lock.use_async_commit {
        return Err(LockAdmissionError::AsyncCommit);
    }
    if lock.is_txn_file {
        return Err(LockAdmissionError::TransactionFile);
    }
    match lock.lock_type {
        OP_PESSIMISTIC_LOCK | OP_SHARED_PESSIMISTIC_LOCK => {
            return Err(LockAdmissionError::Pessimistic(lock.lock_type));
        }
        OP_PUT | OP_DELETE | OP_LOCK | OP_INSERT | OP_CHECK_NOT_EXISTS => {}
        OP_SHARED_LOCK => {
            return Err(LockAdmissionError::UnsupportedLockType(lock.lock_type));
        }
        other => return Err(LockAdmissionError::UnsupportedLockType(other)),
    }
    if lock.key.is_empty() || lock.primary_lock.is_empty() || lock.lock_version == 0 {
        return Err(LockAdmissionError::MissingIdentity);
    }
    Ok(OptimisticLock {
        key: lock.key.clone(),
        primary: lock.primary_lock.clone(),
        txn_id: lock.lock_version,
        ttl_ms: lock.lock_ttl,
        txn_size: lock.txn_size,
        lock_type: lock.lock_type,
        min_commit_ts: lock.min_commit_ts,
    })
}
