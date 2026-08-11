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

use tidb_error::terror::ERR_RESULT_UNDETERMINED;
use tidb_txnkv::region::RegionBackoffKind;
use tidb_txnkv::transaction::{OptimisticCommitOutcome, PessimisticLockFailure, TransactionCause};
use tidb_txnkv::{to_tidb_driver_error, ConvertedDriverError, StorageDriverError};

/// Go `errno.ErrLockWaitTimeout`.
pub const ERR_LOCK_WAIT_TIMEOUT: u16 = 1205;
/// Go `errno.ErrLockDeadlock`.
pub const ERR_LOCK_DEADLOCK: u16 = 1213;
/// Go `errno.ErrLockAcquireFailAndNoWaitSet`.
pub const ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET: u16 = 3572;
/// Go `errno.ErrWriteConflict`, the statement-scoped conflict a pessimistic
/// retry resolves.
pub const ERR_WRITE_CONFLICT: u16 = 9007;
/// Go `errno.ErrRegionUnavailable`, a determinate routing failure that is not
/// one of `pkg/kv.IsTxnRetryableError`'s three retryable identities.
pub const ERR_REGION_UNAVAILABLE: u16 = 9005;

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
    /// The message Go renders: registered errors include their `[class:code]`
    /// prefix, while unregistered passthrough errors keep their raw source
    /// text.
    pub message: String,
}

impl LockSqlError {
    /// Whether this error carries Go's result-undetermined identity.
    #[must_use]
    pub(crate) fn is_result_undetermined(&self) -> bool {
        self.code == tidb_error::mysql::errcode::ErrUnknown
            && self.message == ERR_RESULT_UNDETERMINED.message()
    }
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
        PessimisticLockFailure::Transaction(cause @ TransactionCause::BackoffExhausted { .. }) => {
            transaction_cause_to_sql_error(cause)
        }
        PessimisticLockFailure::Transaction(cause) => LockSqlError {
            code: 1105,
            state: DEFAULT_SQL_STATE,
            message: format!("[executor:1105]pessimistic transaction aborted: {cause}"),
        },
    }
}

/// Maps a terminal commit outcome to what the client is told.
///
/// Only `Committed` may answer `COMMIT` with success. Every other outcome
/// means the writes are not durable, and the coordinator reports those as an
/// `Ok` value carrying the cause rather than as an `Err` -- so a caller that
/// only checks for `Err` tells the client its transaction committed when TiKV
/// rolled it back. SQL-facing commit consumers use this authority for that
/// reason; domain-specific retry loops must apply their own typed policy.
///
/// # Errors
///
/// Returns the client-visible error for any outcome other than `Committed`.
pub fn commit_outcome_to_sql_error(outcome: &OptimisticCommitOutcome) -> Result<(), LockSqlError> {
    match outcome {
        OptimisticCommitOutcome::Committed(_) => Ok(()),
        OptimisticCommitOutcome::RolledBack(rolled_back) => {
            Err(transaction_cause_to_sql_error(&rolled_back.cause))
        }
        OptimisticCommitOutcome::CleanupFailed(failed) => {
            Err(transaction_cause_to_sql_error(&failed.cause))
        }
        OptimisticCommitOutcome::Undetermined(_) => Err(LockSqlError {
            code: 1105,
            state: DEFAULT_SQL_STATE,
            message: ERR_RESULT_UNDETERMINED.message().to_owned(),
        }),
    }
}

/// Renders a transaction-ending cause as the error TiDB reports for it.
///
/// Write conflicts keep Go's retryable 9007 identity. A terminal effective
/// backoff keeps the concrete client-go sentinel selected by its longest-sleep
/// category; only `BoRegionMiss`/`BoRegionScheduling` become 9005. The excluded
/// ServerBusy cap returns its current ordinary error, while structural region
/// failures have no such identity and keep their exact diagnostic under 1105.
#[must_use]
pub fn transaction_cause_to_sql_error(cause: &TransactionCause) -> LockSqlError {
    match cause {
        TransactionCause::WriteConflict { detail } => LockSqlError {
            code: ERR_WRITE_CONFLICT,
            state: DEFAULT_SQL_STATE,
            message: format!("[kv:9007]Write conflict, {detail} [try again later]"),
        },
        TransactionCause::BackoffExhausted { kind, detail } => {
            backoff_exhausted_to_sql_error(*kind, detail)
        }
        other => LockSqlError {
            code: 1105,
            state: DEFAULT_SQL_STATE,
            message: format!("[kv:1105]transaction failed: {other}"),
        },
    }
}

fn backoff_exhausted_to_sql_error(kind: RegionBackoffKind, detail: &str) -> LockSqlError {
    let source = match kind {
        RegionBackoffKind::TikvRpc => StorageDriverError::TiKvServerTimeout,
        RegionBackoffKind::RegionMiss | RegionBackoffKind::RegionScheduling => {
            StorageDriverError::RegionUnavailable
        }
        RegionBackoffKind::TikvServerBusy => StorageDriverError::Other(detail.to_owned()),
        RegionBackoffKind::StaleCommand => StorageDriverError::TiKvStaleCommand,
        RegionBackoffKind::MaxTimestampNotSynced => StorageDriverError::TiKvMaxTimestampNotSynced,
        RegionBackoffKind::TxnLock
        | RegionBackoffKind::TxnLockFast
        | RegionBackoffKind::TxnNotFound => StorageDriverError::ResolveLockTimeout,
        RegionBackoffKind::TikvDiskFull => StorageDriverError::Other("tikv disk full".to_owned()),
        RegionBackoffKind::RegionRecoveryInProgress => {
            StorageDriverError::Other("region is being online unsafe recovered".to_owned())
        }
        RegionBackoffKind::RegionNotInitialized => {
            StorageDriverError::Other("region not Initialized".to_owned())
        }
        RegionBackoffKind::IsWitness => StorageDriverError::Other("peer is witness".to_owned()),
    };
    match to_tidb_driver_error(&source) {
        ConvertedDriverError::Terror(converted) => LockSqlError {
            code: u16::try_from(converted.code().value())
                .expect("registered TiDB error code fits the MySQL protocol"),
            state: DEFAULT_SQL_STATE,
            message: converted.to_string(),
        },
        ConvertedDriverError::Passthrough(source) => LockSqlError {
            code: 1105,
            state: DEFAULT_SQL_STATE,
            message: source.to_string(),
        },
        ConvertedDriverError::Kv(_) | ConvertedDriverError::Transaction(_) => {
            unreachable!("backoff source table has no kv or transaction conversion rows")
        }
    }
}

/// The write conflict a fair-locking `LockedWithConflict` result reports.
///
/// Go `pkg/store/driver/txn.generateWriteConflictForLockedWithConflict`: the
/// lock exists, but at a timestamp newer than the statement's, so the
/// statement's result was computed from a snapshot that has been overtaken.
/// Reporting it as `ErrWriteConflict` is what puts the statement on the same
/// retry path a plain conflict takes — the difference is that the lock is kept.
///
/// `conflict_key` is the key whose lock carries `conflict_commit_ts`; Go
/// renders it through `prettyWriteKey`, which decodes the table and index it
/// belongs to. This bounded owner has no such decoder here and prints the
/// encoded key instead, which names the same row.
#[must_use]
pub fn locked_with_conflict_error(
    start_ts: u64,
    conflict_commit_ts: u64,
    conflict_key: &[u8],
) -> LockSqlError {
    LockSqlError {
        code: ERR_WRITE_CONFLICT,
        state: DEFAULT_SQL_STATE,
        message: format!(
            "[kv:9007]Write conflict, txnStartTS={start_ts}, conflictStartTS=0, \
             conflictCommitTS={conflict_commit_ts}, key={conflict_key:?} primary=<unknown>, \
             reason=LockedWithConflict [try again later]"
        ),
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
    use super::{commit_outcome_to_sql_error, transaction_cause_to_sql_error};
    use super::{
        is_retryable_statement_failure, lock_failure_to_sql_error,
        ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET, ERR_LOCK_DEADLOCK, ERR_LOCK_WAIT_TIMEOUT,
        ERR_REGION_UNAVAILABLE, ERR_WRITE_CONFLICT,
    };
    use tidb_error::terror::ERR_RESULT_UNDETERMINED;
    use tidb_txnkv::region::RegionBackoffKind;
    use tidb_txnkv::transaction::{
        CommittedTransaction, DeadlockDetail, DeadlockWaitChainItem, OptimisticCommitOutcome,
        OptimisticTransactionReceipt, PessimisticLockFailure, RolledBackTransaction,
        TransactionCause, UndeterminedTransaction,
    };

    fn receipt() -> OptimisticTransactionReceipt {
        OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1)
    }

    /// The regression this exists for: the coordinator reports a 2PC TiKV
    /// refused as an `Ok` value carrying the cause. A commit path that reads
    /// that as success tells the client its transaction committed while the
    /// rows were rolled back -- the silent-wrong COMMIT.
    #[test]
    fn a_rolled_back_outcome_is_a_failure_with_its_own_code() {
        let refused = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: receipt(),
            cause: TransactionCause::WriteConflict {
                detail: "conflictStartTS=7".to_owned(),
            },
        });
        let error = commit_outcome_to_sql_error(&refused)
            .expect_err("a rolled-back commit can never answer OK");
        assert_eq!(error.code, ERR_WRITE_CONFLICT);
        assert!(error.message.contains("conflictStartTS=7"));
    }

    #[test]
    fn only_a_committed_outcome_answers_ok() {
        assert!(
            commit_outcome_to_sql_error(&OptimisticCommitOutcome::Committed(
                CommittedTransaction {
                    receipt: receipt(),
                    secondary_failures: Vec::new(),
                }
            ))
            .is_ok()
        );
        let undetermined = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
            receipt: receipt(),
            cause: TransactionCause::Transport {
                detail: "connection reset".to_owned(),
            },
        });
        let error = commit_outcome_to_sql_error(&undetermined)
            .expect_err("an undetermined commit is not a durable one");
        assert_eq!(error.code, 1105);
        assert_eq!(error.message, ERR_RESULT_UNDETERMINED.message());
        assert!(error.is_result_undetermined());
    }

    #[test]
    fn transaction_causes_keep_only_source_typed_tidb_codes() {
        assert_eq!(
            transaction_cause_to_sql_error(&TransactionCause::WriteConflict {
                detail: "d".to_owned()
            })
            .code,
            ERR_WRITE_CONFLICT
        );
        let region = transaction_cause_to_sql_error(&TransactionCause::Region {
            detail: "epoch not match".to_owned(),
        });
        assert_eq!(region.code, 1105);
        assert!(region.message.contains("epoch not match"));

        let lock_path = lock_failure_to_sql_error(&PessimisticLockFailure::Transaction(
            TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::RegionScheduling,
                detail: "region scheduling exhausted".to_owned(),
            },
        ));
        assert_eq!(lock_path.code, ERR_REGION_UNAVAILABLE);
    }

    #[test]
    fn exhausted_backoff_uses_client_go_config_error_identity() {
        use tidb_error::tidb::errcode;

        let cases = [
            (
                RegionBackoffKind::TikvRpc,
                Some(errcode::ErrTiKVServerTimeout),
                None,
            ),
            (
                RegionBackoffKind::RegionMiss,
                Some(ERR_REGION_UNAVAILABLE),
                None,
            ),
            (
                RegionBackoffKind::RegionScheduling,
                Some(ERR_REGION_UNAVAILABLE),
                None,
            ),
            (
                RegionBackoffKind::TikvServerBusy,
                None,
                Some("TikvServerBusy backoffer exhausted"),
            ),
            (
                RegionBackoffKind::TikvDiskFull,
                None,
                Some("tikv disk full"),
            ),
            (
                RegionBackoffKind::RegionRecoveryInProgress,
                None,
                Some("region is being online unsafe recovered"),
            ),
            (
                RegionBackoffKind::StaleCommand,
                Some(errcode::ErrTiKVStaleCommand),
                None,
            ),
            (
                RegionBackoffKind::MaxTimestampNotSynced,
                Some(errcode::ErrTiKVMaxTimestampNotSynced),
                None,
            ),
            (
                RegionBackoffKind::RegionNotInitialized,
                None,
                Some("region not Initialized"),
            ),
            (RegionBackoffKind::IsWitness, None, Some("peer is witness")),
            (
                RegionBackoffKind::TxnLock,
                Some(errcode::ErrResolveLockTimeout),
                None,
            ),
            (
                RegionBackoffKind::TxnLockFast,
                Some(errcode::ErrResolveLockTimeout),
                None,
            ),
            (
                RegionBackoffKind::TxnNotFound,
                Some(errcode::ErrResolveLockTimeout),
                None,
            ),
        ];

        for (kind, expected_code, expected_passthrough) in cases {
            let detail = format!("{kind:?} backoffer exhausted");
            let error = transaction_cause_to_sql_error(&TransactionCause::BackoffExhausted {
                kind,
                detail: detail.clone(),
            });
            match expected_code {
                Some(code) => assert_eq!(error.code, code, "{kind:?}: {}", error.message),
                None => {
                    assert_eq!(error.code, 1105, "{kind:?}: {}", error.message);
                    assert_eq!(
                        error.message,
                        expected_passthrough.expect("passthrough source text"),
                        "{kind:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn excluded_busy_cap_keeps_the_current_error_generic() {
        let mut budget = tidb_txnkv::region::RegionBackoffBudget::with_jitter_seed(
            std::time::Duration::from_secs(20),
            7,
        );
        let exhausted = loop {
            match budget.next_delay(RegionBackoffKind::TikvServerBusy) {
                Ok(_) => continue,
                Err(exhausted) => break exhausted,
            }
        };
        assert_eq!(exhausted.kind, RegionBackoffKind::TikvServerBusy);

        let error = transaction_cause_to_sql_error(&TransactionCause::BackoffExhausted {
            kind: exhausted.kind,
            detail: "server is busy".to_owned(),
        });
        assert_eq!(error.code, 1105);
        assert_eq!(error.message, "server is busy");
    }

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
            wait_chain: vec![DeadlockWaitChainItem {
                txn: 7,
                wait_for_txn: 8,
                key: Vec::new(),
                resource_group_tag: Vec::new(),
            }],
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
