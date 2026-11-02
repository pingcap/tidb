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

//! Go `pkg/store/mockstore/unistore/tikv/kverrors`: the errors the in-process
//! TiKV answers a key request with, which its server converts to
//! `kvrpcpb.KeyError`.

use tidb_proto::KvrpcLockInfo;

/// The `kverrors` subset the optimistic slice raises, each Go error by name.
#[derive(Clone, Debug, PartialEq)]
pub enum KvError {
    /// `kverrors.ErrAlreadyRollback`.
    AlreadyRollback,
    /// `kverrors.ErrLockNotFound`.
    LockNotFound,
    /// `kverrors.ErrReplaced`.
    Replaced,
    /// `kverrors.ErrCommitExpire`.
    CommitExpire {
        /// `StartTs`.
        start_ts: u64,
        /// `CommitTs`.
        commit_ts: u64,
        /// `MinCommitTs`.
        min_commit_ts: u64,
        /// `Key`.
        key: Vec<u8>,
    },
    /// `kverrors.ErrConflict`, carrying Go's `kvrpcpb.WriteConflict` reason.
    Conflict {
        /// `Reason`, the proto's `write_conflict::Reason`.
        reason: tidb_proto::kvrpcpb::write_conflict::Reason,
        /// `StartTS`.
        start_ts: u64,
        /// `ConflictTS` — the conflicting version's START ts, Go's choice.
        conflict_ts: u64,
        /// `ConflictCommitTS`.
        conflict_commit_ts: u64,
        /// `Key`.
        key: Vec<u8>,
    },
    /// `kverrors.ErrKeyAlreadyExists`.
    KeyAlreadyExists {
        /// `Key`.
        key: Vec<u8>,
    },
    /// `kverrors.ErrAssertionFailed`.
    AssertionFailed {
        /// `StartTS`.
        start_ts: u64,
        /// `Key`.
        key: Vec<u8>,
        /// `Assertion`, the `kvrpcpb.Assertion` value that failed.
        assertion: i32,
        /// `ExistingStartTS` — zero when the key does not exist.
        existing_start_ts: u64,
        /// `ExistingCommitTS` — zero when the key does not exist.
        existing_commit_ts: u64,
    },
    /// `kverrors.ErrAlreadyCommitted(commitTS)`.
    AlreadyCommitted(u64),
    /// `kverrors.BuildLockErr(key, lock)`: the blocking lock, kvrpcpb-shaped.
    Locked(Box<KvrpcLockInfo>),
    /// `kverrors.ErrPrimaryMismatch`: the checked key carries a lock whose
    /// primary is a DIFFERENT key.
    PrimaryMismatch {
        /// `Key`.
        key: Vec<u8>,
        /// `Lock`, kvrpcpb-shaped.
        lock: Box<KvrpcLockInfo>,
    },
    /// `kverrors.ErrTxnNotFound`.
    TxnNotFound {
        /// `PrimaryKey`.
        primary_key: Vec<u8>,
        /// `StartTS`.
        start_ts: u64,
    },
    /// Go `kverrors.ErrInvalidOp`: `Op_CheckNotExists` inside a pessimistic
    /// prewrite.
    InvalidOp,
    /// Go's literal `errors.New("pessimistic lock not found")`.
    PessimisticLockNotFound,
    /// `kverrors.ErrDeadlock`: waiting for the lock would close a cycle.
    Deadlock {
        /// `LockKey`.
        lock_key: Vec<u8>,
        /// `LockTS`.
        lock_ts: u64,
        /// `DeadlockKeyHash`.
        deadlock_key_hash: u64,
        /// `WaitChain`.
        wait_chain: Vec<tidb_unistore_lockwaiter::WaitForEntry>,
    },
    /// Go's literal `errors.New("lock type not match")`.
    LockTypeNotMatch,
    /// A refusal: the named Go symbol is a later course of this port.
    Unported(&'static str),
    /// Go raises `errors.Errorf` with a formatted message; the rendered text
    /// rides here (e.g. the for-update-ts constraint range check,
    /// `mvcc.go:857`).
    Message(String),
}

/// Go's `hex.EncodeToString`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Go `mvcc.Lock.String()` over the lock's kvrpcpb shape.
fn lock_text(lock: &KvrpcLockInfo) -> String {
    let op = tidb_proto::KvrpcOp::try_from(lock.lock_type)
        .map_or_else(|_| lock.lock_type.to_string(), |op| op.as_str_name().to_owned());
    format!(
        "Lock {{ Type: {op}, StartTS: {},  ForUpdateTS: {}, Primary: {}, UseAsyncCommit: {} }}",
        lock.lock_version,
        lock.lock_for_update_ts,
        hex(&lock.primary_lock),
        lock.use_async_commit,
    )
}

/// Go's `Error()` text of each `kverrors` error, which the server and the
/// coprocessor answer with (`resp.OtherError = err.Error()`).
impl std::fmt::Display for KvError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // `ErrRetryable(...)`.
            Self::AlreadyRollback => formatter.write_str("retryable: already rollback"),
            Self::LockNotFound => formatter.write_str("retryable: lock not found"),
            Self::Replaced => formatter.write_str("retryable: replaced by another transaction"),
            Self::CommitExpire { .. } => formatter.write_str("commit expired"),
            Self::Conflict { .. } => formatter.write_str("write conflict"),
            Self::KeyAlreadyExists { .. } => formatter.write_str("key already exists"),
            Self::AssertionFailed {
                start_ts,
                key,
                assertion,
                existing_start_ts,
                existing_commit_ts,
            } => {
                let assertion = tidb_proto::kvrpcpb::Assertion::try_from(*assertion)
                    .map_or_else(|_| assertion.to_string(), |a| a.as_str_name().to_owned());
                write!(
                    formatter,
                    "AssertionFailed {{ StartTS: {start_ts}, Key: {}, Assertion: {assertion}, ExistingStartTS: {existing_start_ts}, ExistingCommitTS: {existing_commit_ts} }}",
                    hex(key)
                )
            }
            Self::AlreadyCommitted(_) => formatter.write_str("txn already committed"),
            Self::Locked(lock) => write!(
                formatter,
                "key is locked, key: {}, lock: {}",
                hex(&lock.key),
                lock_text(lock)
            ),
            Self::PrimaryMismatch { key, lock } => write!(
                formatter,
                "primary mismatch, key: {}, lock: {}",
                hex(key),
                lock_text(lock)
            ),
            Self::TxnNotFound { .. } => formatter.write_str("txn not found"),
            Self::InvalidOp => formatter.write_str("invalid op: CheckNotExists"),
            Self::PessimisticLockNotFound => formatter.write_str("pessimistic lock not found"),
            Self::Deadlock { .. } => formatter.write_str("deadlock"),
            Self::LockTypeNotMatch => formatter.write_str("lock type not match"),
            Self::Unported(name) => write!(formatter, "{name} is not ported"),
            Self::Message(message) => formatter.write_str(message),
        }
    }
}
