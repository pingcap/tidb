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

//! TiDB lock errors and statement contracts over client-rust.
use super::TransactionCause;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Wait budget for one locking statement, in client-go's exact encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockWaitTime {
    /// `SELECT ... FOR UPDATE NOWAIT`: fail the statement rather than queue.
    NoWait,
    /// Wait as long as the surrounding call allows.
    AlwaysWait,
    /// Wait at most this long, then fail with a lock-wait timeout.
    Timeout(Duration),
}

impl Default for LockWaitTime {
    fn default() -> Self {
        Self::Timeout(Duration::from_secs(
            tidb_vardef::defaults::DEF_INNODB_LOCK_WAIT_TIMEOUT as u64,
        ))
    }
}

/// Why one locking statement failed.
///
/// The first four variants are statement-scoped in TiDB: the transaction stays
/// usable and the SQL layer decides whether to retry the statement under a
/// newer `for_update_ts`. [`Self::Transaction`] is not — it ends the
/// transaction, exactly like the optimistic path's causes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PessimisticLockFailure {
    /// TiKV's deadlock detector proved a cycle. The statement must be aborted;
    /// retrying it would recreate the same cycle.
    Deadlock(tidb_client_go_error::DeadlockDetail),
    /// A newer version of a key was committed after this statement's
    /// `for_update_ts`. Retry the statement under a fresh one.
    WriteConflict {
        /// TiKV conflict diagnostic.
        detail: String,
    },
    /// `NOWAIT` was requested and the key is locked by a live transaction.
    LockAcquireFailAndNoWaitSet {
        /// Exact encoded key that is locked.
        key: Vec<u8>,
    },
    /// The statement's lock-wait budget elapsed while a live owner held the key.
    LockWaitTimeout {
        /// Exact encoded key that is locked.
        key: Vec<u8>,
    },
    /// Anything that ends the transaction rather than the statement.
    Transaction(TransactionCause),
}

impl fmt::Display for PessimisticLockFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadlock(detail) => write!(
                formatter,
                "deadlock detected on txn {} over key {:?}",
                detail.lock_ts, detail.lock_key
            ),
            Self::WriteConflict { detail } => {
                write!(formatter, "pessimistic write conflict: {detail}")
            }
            Self::LockAcquireFailAndNoWaitSet { .. } => {
                formatter.write_str("lock acquisition failed and NOWAIT is set")
            }
            Self::LockWaitTimeout { .. } => formatter.write_str("lock wait timeout exceeded"),
            Self::Transaction(cause) => cause.fmt(formatter),
        }
    }
}

impl std::error::Error for PessimisticLockFailure {}

impl PessimisticLockFailure {
    /// Whether the transaction survives this failure and the SQL layer may
    /// retry only the statement under a newer `for_update_ts`.
    #[must_use]
    pub const fn is_statement_scoped(&self) -> bool {
        !matches!(self, Self::Transaction(_))
    }
}

/// Locks one statement acquired, and the timestamp they were acquired under.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquiredLocks {
    /// Statement timestamp every lock in this batch carries.
    pub for_update_ts: u64,
    /// Exact encoded keys newly locked, in encoded-key order.
    pub keys: Vec<Vec<u8>>,
    /// Keys fair locking locked *despite* a conflict, with the commit
    /// timestamp of the version that beat this statement.
    ///
    /// Non-empty only under [`crate::transaction::RealTransaction::set_fair_locking`].
    /// Each entry says "the lock exists, but at this higher timestamp": the
    /// statement's own result is stale and must be recomputed, while the lock
    /// itself carries over to the retry. Go turns the same fact into
    /// `ErrWriteConflict{reason: "LockedWithConflict"}` in
    /// `pkg/store/driver/txn.generateWriteConflictForLockedWithConflict`.
    pub locked_with_conflict: Vec<(Vec<u8>, u64)>,
    /// Row values TiKV returned with the locks — Go `LockCtx.Values`, filled
    /// when the request carried `return_values`
    /// (`pkg/executor/point_get.go:614 InitReturnValues(1)`; the response
    /// lands in `TxnCtx.SetPessimisticLockCache`, and the executor's own
    /// `get` then reads from that cache instead of storage). Empty unless
    /// [`crate::transaction::RealTransaction::acquire_locks_returning_values`] was
    /// used. A key maps to `None` when TiKV reports it absent.
    pub values: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}
