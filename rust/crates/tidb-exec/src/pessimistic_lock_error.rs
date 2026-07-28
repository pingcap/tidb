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

//! Turning a TiKV pessimistic-lock failure into the error a SQL client sees.
//!
//! `tidb-txnkv` reports why a locking statement failed in TiKV's own terms.
//! Go converts those in two places: `pkg/store/driver/error.ToTiDBErr` maps
//! the client-go NOWAIT and lock-wait-timeout errors to `ClassTiKV`'s 3572 and
//! 1205, and `pkg/executor/adapter.go`'s deferred `errors.Cause(err).(*
//! tikverr.ErrDeadlock)` check replaces a proven deadlock with
//! `exeerrors.ErrDeadlock`, which is `ClassExecutor` over `mysql.
//! ErrLockDeadlock` (1213).
//!
//! Captured from TiDB's mock store, two sessions in `BEGIN PESSIMISTIC`:
//! `SELECT ... FOR UPDATE NOWAIT` against a locked row raises
//! `[tikv:3572]Statement aborted because lock(s) could not be acquired
//! immediately and NOWAIT is set.`, and the same statement without `NOWAIT`,
//! after `innodb_lock_wait_timeout`, raises `[tikv:1205]Lock wait timeout
//! exceeded; try restarting transaction`.

use tidb_txnkv::transaction::PessimisticLockFailure;

/// Go `errno.ErrLockWaitTimeout`.
pub const ERR_LOCK_WAIT_TIMEOUT: u16 = 1205;
/// Go `errno.ErrLockDeadlock`.
pub const ERR_LOCK_DEADLOCK: u16 = 1213;
/// Go `errno.ErrLockAcquireFailAndNoWaitSet`.
pub const ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET: u16 = 3572;
/// Go `errno.ErrWriteConflict`, the statement-scoped conflict a pessimistic
/// retry resolves.
pub const ERR_WRITE_CONFLICT: u16 = 9007;

/// Go `mysql.DefaultMySQLState`, used by every code absent from `state.go`.
const DEFAULT_SQL_STATE: [u8; 5] = *b"HY000";
/// Go `mysql.MySQLState[ErrLockDeadlock]`, the only one of these four with an
/// entry of its own.
const DEADLOCK_SQL_STATE: [u8; 5] = *b"40001";

/// One client-visible SQL error: the wire triple, with nothing else attached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockSqlError {
    /// MySQL error number.
    pub code: u16,
    /// Five-byte SQLSTATE.
    pub state: [u8; 5],
    /// The message Go renders, including its `[class:code]` prefix.
    pub message: String,
}

/// Maps a lock failure to the error TiDB reports for it.
///
/// [`PessimisticLockFailure::Transaction`] is not statement-scoped: it ends
/// the transaction, so it is reported with its own diagnostic under the
/// generic 1105 rather than being disguised as a lock error the client could
/// usefully retry.
#[must_use]
pub fn lock_failure_to_sql_error(failure: &PessimisticLockFailure) -> LockSqlError {
    match failure {
        PessimisticLockFailure::Deadlock(_) => LockSqlError {
            code: ERR_LOCK_DEADLOCK,
            state: DEADLOCK_SQL_STATE,
            message: "[executor:1213]Deadlock found when trying to get lock; try restarting \
                      transaction"
                .to_owned(),
        },
        PessimisticLockFailure::LockAcquireFailAndNoWaitSet { .. } => LockSqlError {
            code: ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET,
            state: DEFAULT_SQL_STATE,
            message: "[tikv:3572]Statement aborted because lock(s) could not be acquired \
                      immediately and NOWAIT is set."
                .to_owned(),
        },
        PessimisticLockFailure::LockWaitTimeout { .. } => LockSqlError {
            code: ERR_LOCK_WAIT_TIMEOUT,
            state: DEFAULT_SQL_STATE,
            message: "[tikv:1205]Lock wait timeout exceeded; try restarting transaction".to_owned(),
        },
        PessimisticLockFailure::WriteConflict { detail } => LockSqlError {
            code: ERR_WRITE_CONFLICT,
            state: DEFAULT_SQL_STATE,
            message: format!("[kv:9007]Write conflict, {detail} [try again later]"),
        },
        PessimisticLockFailure::Transaction(cause) => LockSqlError {
            code: 1105,
            state: DEFAULT_SQL_STATE,
            message: format!("[executor:1105]pessimistic transaction aborted: {cause}"),
        },
    }
}

/// Whether the SQL layer may retry only the statement, under a newer
/// `for_update_ts`, instead of ending the transaction.
///
/// Go retries a pessimistic statement after a write conflict but never after a
/// proven deadlock: `pkg/executor/adapter.go` aborts the statement outright,
/// because retrying would re-form the same cycle. NOWAIT and the lock-wait
/// timeout are the user's own explicit budgets, so they are reported too.
#[must_use]
pub const fn is_retryable_statement_failure(failure: &PessimisticLockFailure) -> bool {
    matches!(failure, PessimisticLockFailure::WriteConflict { .. })
}

#[cfg(test)]
mod tests {
    use super::{
        is_retryable_statement_failure, lock_failure_to_sql_error,
        ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET, ERR_LOCK_DEADLOCK, ERR_LOCK_WAIT_TIMEOUT,
        ERR_WRITE_CONFLICT,
    };
    use tidb_txnkv::transaction::{DeadlockDetail, PessimisticLockFailure};

    #[test]
    fn nowait_reports_3572_exactly_as_tidb_does() {
        let error =
            lock_failure_to_sql_error(&PessimisticLockFailure::LockAcquireFailAndNoWaitSet {
                key: b"k".to_vec(),
            });
        assert_eq!(error.code, ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET);
        assert_eq!(&error.state, b"HY000");
        assert_eq!(
            error.message,
            "[tikv:3572]Statement aborted because lock(s) could not be acquired immediately and \
             NOWAIT is set."
        );
    }

    #[test]
    fn a_timed_out_wait_reports_1205() {
        let error = lock_failure_to_sql_error(&PessimisticLockFailure::LockWaitTimeout {
            key: b"k".to_vec(),
        });
        assert_eq!(error.code, ERR_LOCK_WAIT_TIMEOUT);
        assert_eq!(
            error.message,
            "[tikv:1205]Lock wait timeout exceeded; try restarting transaction"
        );
    }

    #[test]
    fn a_proven_deadlock_reports_1213_with_its_own_sqlstate() {
        let error = lock_failure_to_sql_error(&PessimisticLockFailure::Deadlock(DeadlockDetail {
            lock_ts: 7,
            lock_key: b"k".to_vec(),
            deadlock_key_hash: 1,
            deadlock_key: b"j".to_vec(),
            wait_chain: vec![(7, 8)],
        }));
        assert_eq!(error.code, ERR_LOCK_DEADLOCK);
        assert_eq!(&error.state, b"40001");
    }

    #[test]
    fn only_a_write_conflict_is_worth_retrying_the_statement_for() {
        let conflict = PessimisticLockFailure::WriteConflict {
            detail: "txnStartTS=1".to_owned(),
        };
        assert_eq!(
            lock_failure_to_sql_error(&conflict).code,
            ERR_WRITE_CONFLICT
        );
        assert!(is_retryable_statement_failure(&conflict));
        assert!(!is_retryable_statement_failure(
            &PessimisticLockFailure::LockWaitTimeout { key: b"k".to_vec() }
        ));
        assert!(!is_retryable_statement_failure(
            &PessimisticLockFailure::Deadlock(DeadlockDetail {
                lock_ts: 1,
                lock_key: Vec::new(),
                deadlock_key_hash: 0,
                deadlock_key: Vec::new(),
                wait_chain: Vec::new(),
            })
        ));
    }
}
