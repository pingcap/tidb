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

//! One connection's open `BEGIN` ... `COMMIT` transaction over real TiKV.
//!
//! This is the executor-side owner of everything a multi-statement transaction
//! does between its two control statements. The connection holds exactly one of
//! these while a transaction is open; every statement in between runs *through*
//! it rather than opening a transaction of its own.
//!
//! Three contracts live here, all of them Go's:
//!
//! **Read-your-own-writes.** A statement inside the transaction observes the
//! transaction's own uncommitted writes. Go does this in `pkg/store/driver/txn`:
//! `tikvTxn.Get` consults the `MemBuffer` first and only falls back to the
//! snapshot at `start_ts` when the key is unstaged. [`Self::staged_row`] is that
//! membuffer lookup, keyed by clustered handle, and [`Self::read_at_snapshot`]
//! is the same overlay in the shape the write planner consumes — so an `UPDATE`
//! of a row the transaction already wrote computes its new row from the value it
//! is replacing, not from the stale snapshot. This is exactly what makes
//! [`TransactionMutationBuffer`]'s repeated-update coalescing correct.
//!
//! **Statement scope versus transaction scope.** A pessimistic lock failure ends
//! the *statement*: the transaction stays open and usable, matching
//! `pkg/executor/adapter.go`, which retries or reports a locking statement
//! without touching the transaction. A write conflict is the asymmetry between
//! the two modes: pessimistic detects it while locking, so it is statement-
//! scoped and retried under a fresh `for_update_ts`; optimistic detects it at
//! Prewrite, so it can only surface at `COMMIT` and it ends the transaction.
//!
//! **Nothing is published before `COMMIT`.** Buffered mutations reach TiKV only
//! in the final two-phase commit, so a concurrent reader sees the pre-transaction
//! value until then. A pessimistic transaction does take locks earlier, but a
//! TiKV pessimistic lock blocks writers, not readers.

use std::collections::BTreeSet;
use std::time::Duration;

use tidb_codec::table_key::{decode_record_key, encode_row_key_with_handle, RecordHandle};
use tidb_codec::{decode_configured_row_bytes, decode_configured_row_int};
use tidb_datatype::Datum;
use tidb_planner::prepared_dml::ConfiguredPreparedWrite;
use tidb_planner::read_only_scan::{
    ConfiguredColumnKind, ConfiguredScalarType, ConfiguredTable, ReadLockWait,
    ResolvedProjectionColumn,
};
use tidb_planner::signed_bigint_ranger::SignedBigIntRange;
use tidb_planner::txn_mode::SessionTxnMode;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    LockKeepAlive, LockWaitTime, OptimisticCommitOutcome, OptimisticCoordinatorError,
    OptimisticMutationKind, PessimisticLockFailure, ProductionOptimisticTransaction,
    ProductionPessimisticTransaction, RealOptimisticTransactionOpener, TransactionCause,
    TransactionMutationBuffer, MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::pessimistic_lock_error::{
    is_retryable_statement_failure, lock_failure_to_sql_error, LockSqlError, ERR_WRITE_CONFLICT,
};
use crate::real_tikv_dml::{
    plan_configured_write, ConfiguredWriteError, ConfiguredWritePlan, ConfiguredWriteReport,
    WritePlanningSnapshot,
};

/// A transaction's own uncommitted rows for one read, by clustered handle.
///
/// `None` is a row the transaction deleted; `Some(row)` is its value in the
/// read's own projection. A reader applies these over the snapshot's rows.
pub type StagedRowOverlay = Vec<(i64, Option<Vec<Datum>>)>;

/// How many times one locking statement re-acquires under a fresh
/// `for_update_ts` after a write conflict.
///
/// Go retries a pessimistic statement until `pessimistic-txn.max-retry-count`;
/// this bounded node keeps a small fixed budget so a statement that keeps losing
/// reports 9007 to the client instead of spinning against PD forever.
const MAX_LOCK_RETRIES: usize = 8;

/// The failure of one statement inside an open transaction.
///
/// The distinction is the whole point of the type: [`Self::Statement`] leaves
/// the transaction open, so the connection reports the error and keeps serving
/// the same transaction; [`Self::Transaction`] means the transaction is over.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionStatementError {
    /// The statement failed; the transaction survives and stays usable.
    Statement(LockSqlError),
    /// The transaction itself failed and must be abandoned. The connection
    /// closes it out and returns to autocommit.
    Transaction(LockSqlError),
}

impl TransactionStatementError {
    /// The client-visible error either way.
    #[must_use]
    pub const fn sql_error(&self) -> &LockSqlError {
        match self {
            Self::Statement(error) | Self::Transaction(error) => error,
        }
    }

    /// Whether the transaction stays open after this failure.
    #[must_use]
    pub const fn keeps_transaction_open(&self) -> bool {
        matches!(self, Self::Statement(_))
    }

    /// A failure of the bounded write path itself (a shape this node does not
    /// admit, an overflow, an encoding refusal). It aborts the statement and
    /// nothing else: no mutation was staged and no lock state changed.
    fn write(error: &ConfiguredWriteError) -> Self {
        Self::Statement(LockSqlError {
            code: 1105,
            state: *b"HY000",
            message: error.to_string(),
        })
    }

    /// A refusal this bounded transaction issues itself, before touching TiKV.
    fn refused(message: impl Into<String>) -> Self {
        Self::Statement(LockSqlError {
            code: 1105,
            state: *b"HY000",
            message: message.into(),
        })
    }

    fn from_lock_failure(failure: &PessimisticLockFailure) -> Self {
        let error = lock_failure_to_sql_error(failure);
        if failure.is_statement_scoped() {
            Self::Statement(error)
        } else {
            Self::Transaction(error)
        }
    }
}

impl std::fmt::Display for TransactionStatementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.sql_error().message)
    }
}

impl std::error::Error for TransactionStatementError {}

/// How one transaction ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionEnd {
    /// `COMMIT` published every buffered mutation, or the transaction had none.
    Committed,
    /// `ROLLBACK` discarded them, releasing anything the transaction held.
    RolledBack,
}

/// The two concrete transactions this node can hold open.
///
/// They are one type here because every statement path treats them alike except
/// where the mode genuinely differs: only the pessimistic one can lock, and only
/// the optimistic one can first learn of a conflict at `COMMIT`.
enum OpenTransaction {
    Optimistic(Box<ProductionOptimisticTransaction>),
    Pessimistic(Box<ProductionPessimisticTransaction>),
}

impl OpenTransaction {
    /// The two-phase commit coordinator underneath, which is what serves every
    /// snapshot read in either mode.
    fn two_pc(&mut self) -> &mut ProductionOptimisticTransaction {
        match self {
            Self::Optimistic(transaction) => transaction,
            Self::Pessimistic(transaction) => transaction.snapshot(),
        }
    }

    fn start_ts(&self) -> u64 {
        match self {
            Self::Optimistic(transaction) => transaction.start_ts(),
            Self::Pessimistic(transaction) => transaction.start_ts(),
        }
    }
}

/// One open explicit transaction, owned by exactly one connection.
pub struct MultiStatementTransaction {
    open: OpenTransaction,
    mode: SessionTxnMode,
    /// The one table this node serves, needed to turn a clustered handle into
    /// the record key the buffer is keyed by.
    table: ConfiguredTable,
    buffer: TransactionMutationBuffer,
    call: UnaryCallContext,
    /// Refreshes the primary lock's TTL for as long as a pessimistic
    /// transaction holds it; `None` until the first lock is taken.
    keep_alive: Option<LockKeepAlive>,
    opener: RealOptimisticTransactionOpener,
}

impl MultiStatementTransaction {
    /// Opens one explicit transaction in `mode` over the shared authorities.
    ///
    /// The publication budget is the transaction-size limit itself, because a
    /// multi-statement transaction cannot know its mutation set when `BEGIN`
    /// runs — unlike a single-statement write, whose exact bounds are computed
    /// before it spends a timestamp. The commit still enforces the same limits.
    pub fn begin(
        opener: &RealOptimisticTransactionOpener,
        mode: SessionTxnMode,
        table: ConfiguredTable,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let open = match mode {
            SessionTxnMode::Optimistic => OpenTransaction::Optimistic(Box::new(
                opener.begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)?,
            )),
            SessionTxnMode::Pessimistic => {
                OpenTransaction::Pessimistic(Box::new(opener.begin_pessimistic(
                    MAX_OPTIMISTIC_MUTATIONS,
                    MAX_OPTIMISTIC_TRANSACTION_BYTES,
                )?))
            }
        };
        Ok(Self {
            open,
            mode,
            table,
            buffer: TransactionMutationBuffer::new(),
            call: UnaryCallContext::with_timeout(timeout),
            keep_alive: None,
            opener: opener.clone(),
        })
    }

    /// The snapshot every read in this transaction runs at.
    #[must_use]
    pub fn start_ts(&self) -> u64 {
        self.open.start_ts()
    }

    /// The mode this transaction runs in.
    #[must_use]
    pub const fn mode(&self) -> SessionTxnMode {
        self.mode
    }

    /// Whether the transaction has buffered any write yet.
    #[must_use]
    pub fn has_staged_writes(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// The row image this transaction has staged for `handle`.
    ///
    /// `None` means the transaction has not written that row, so a read must
    /// take whatever the snapshot at `start_ts` holds. `Some(None)` means the
    /// transaction deleted it, and `Some(Some(row))` means the transaction's own
    /// replacement row — the read-your-own-writes overlay a reader applies over
    /// the snapshot's rows.
    #[must_use]
    pub fn staged_row(&self, handle: i64) -> Option<Option<&[u8]>> {
        let key = encode_row_key_with_handle(self.table.table_id(), &RecordHandle::Int(handle));
        let staged = self.buffer.staged(&key)?;
        match staged.kind() {
            OptimisticMutationKind::Delete => Some(None),
            OptimisticMutationKind::Insert | OptimisticMutationKind::PutExisting => {
                Some(Some(staged.value()))
            }
            // Index keys are never record keys, so this key cannot carry one.
            OptimisticMutationKind::IndexPut | OptimisticMutationKind::IndexDelete => None,
        }
    }

    /// The transaction's own uncommitted rows that fall inside `ranges`,
    /// decoded into the projection a read is about to return.
    ///
    /// This is the union-scan half of read-your-own-writes: the reader applies
    /// these over the rows the snapshot at `start_ts` produces, so a row this
    /// transaction rewrote comes back with its new value, a row it deleted does
    /// not come back, and a row it inserted comes back even though the snapshot
    /// has never seen it. Go builds the same overlay in `UnionScanExec` from the
    /// `MemBuffer` entries within the scanned key range.
    ///
    /// An entry is `None` when the transaction deleted the row.
    pub fn read_overlay(
        &self,
        projection: &[ResolvedProjectionColumn],
        ranges: &[SignedBigIntRange],
    ) -> Result<StagedRowOverlay, TransactionStatementError> {
        let mut overlay = Vec::new();
        for staged in self.buffer.staged_entries() {
            // Only record keys carry rows; a secondary-index entry changes no
            // row the projection can return.
            let Ok((table_id, RecordHandle::Int(handle))) = decode_record_key(staged.key()) else {
                continue;
            };
            if table_id != self.table.table_id()
                || !ranges
                    .iter()
                    .any(|range| range.start() <= handle && handle <= range.end())
            {
                continue;
            }
            let row = match staged.kind() {
                OptimisticMutationKind::Delete => None,
                OptimisticMutationKind::Insert | OptimisticMutationKind::PutExisting => Some(
                    decode_staged_projection(projection, handle, staged.value())
                        .map_err(|error| TransactionStatementError::write(&error))?,
                ),
                OptimisticMutationKind::IndexPut | OptimisticMutationKind::IndexDelete => continue,
            };
            overlay.push((handle, row));
        }
        Ok(overlay)
    }

    /// Plans one bound write against this transaction and buffers its mutations.
    ///
    /// Nothing reaches TiKV here. The write is planned over the read-your-own-
    /// writes overlay, so a row this transaction already wrote is re-read from
    /// the buffer, and the resulting mutations are staged for the commit.
    ///
    /// In pessimistic mode the rows the statement touches are locked first, as
    /// Go's `UPDATE`/`DELETE` executors do through `SelectLockExec`, so the
    /// conflict is detected now rather than at commit.
    pub fn execute_write(
        &mut self,
        write: &ConfiguredPreparedWrite,
    ) -> Result<ConfiguredWriteReport, TransactionStatementError> {
        if self.mode.is_pessimistic() {
            let handles = written_handles(write);
            if !handles.is_empty() {
                // A write blocks for the lock rather than failing fast: NOWAIT
                // is a locking-read clause, never a DML one.
                self.lock_handles(&handles, ReadLockWait::Blocking)?;
            }
        }
        let plan = plan_configured_write(self, write, &self.call.clone())
            .map_err(|error| TransactionStatementError::write(&error))?;
        match plan {
            ConfiguredWritePlan::Write {
                mutations,
                affected_rows,
            } => {
                for mutation in mutations {
                    self.buffer.stage(mutation).map_err(|error| {
                        TransactionStatementError::refused(format!(
                            "configured write staging: {error}"
                        ))
                    })?;
                }
                Ok(ConfiguredWriteReport {
                    affected_rows,
                    no_write: None,
                })
            }
            ConfiguredWritePlan::NoWrite { reason } => Ok(ConfiguredWriteReport {
                affected_rows: 0,
                no_write: Some(reason),
            }),
        }
    }

    /// Acquires the exclusive row locks a `SELECT ... FOR UPDATE` demands.
    ///
    /// A write conflict here costs only this statement: the locks are retried
    /// under a fresh `for_update_ts`, which is precisely what pessimistic mode
    /// buys. A statement that fails after locking part of its key set releases
    /// exactly the keys it added, so the next statement never blocks on this
    /// transaction's own abandoned locks.
    pub fn lock_handles(
        &mut self,
        handles: &[i64],
        wait: ReadLockWait,
    ) -> Result<(), TransactionStatementError> {
        if handles.is_empty() {
            return Ok(());
        }
        if !self.mode.is_pessimistic() {
            return Err(TransactionStatementError::refused(
                "a locking read requires a pessimistic transaction; optimistic transactions \
                 detect conflicts at COMMIT",
            ));
        }
        let keys = handles
            .iter()
            .map(|handle| {
                encode_row_key_with_handle(self.table.table_id(), &RecordHandle::Int(*handle))
            })
            .collect::<Vec<_>>();
        let wait = match wait {
            ReadLockWait::Blocking => LockWaitTime::AlwaysWait,
            ReadLockWait::NoWait => LockWaitTime::NoWait,
            ReadLockWait::Seconds(seconds) => LockWaitTime::Timeout(Duration::from_secs(seconds)),
        };
        let OpenTransaction::Pessimistic(transaction) = &mut self.open else {
            unreachable!("the pessimistic mode check above admits only a pessimistic transaction");
        };
        let call = self.call.clone();
        let held: BTreeSet<Vec<u8>> = transaction.locked_keys().into_iter().collect();
        let mut attempt = 0;
        let acquired = loop {
            // No key of a locking read is presumed absent: the rows already
            // exist, which is why the statement is locking them.
            match transaction.acquire_locks(&keys, &BTreeSet::new(), wait, &call) {
                Ok(acquired) => break acquired,
                Err(failure) => {
                    // Release only what this statement added; the transaction's
                    // earlier locks must survive its own failed statement.
                    let added = keys
                        .iter()
                        .filter(|key| !held.contains(*key))
                        .cloned()
                        .collect::<Vec<_>>();
                    if let Err(cause) = transaction.pessimistic_rollback(&added, &call) {
                        return Err(TransactionStatementError::Transaction(
                            transaction_cause_error(&cause),
                        ));
                    }
                    if !is_retryable_statement_failure(&failure) || attempt >= MAX_LOCK_RETRIES {
                        return Err(TransactionStatementError::from_lock_failure(&failure));
                    }
                    // A newer statement timestamp is what makes the retry see
                    // the committed version that beat this one.
                    transaction
                        .advance_for_update_ts()
                        .map_err(|error| TransactionStatementError::from_lock_failure(&error))?;
                    attempt += 1;
                }
            }
        };
        // The primary lock now has to survive every later statement, so its TTL
        // is refreshed from the moment it exists.
        if self.keep_alive.is_none() {
            let keep_alive = self
                .opener
                .start_lock_keep_alive(acquired.primary_key.clone(), transaction.start_ts())
                .map_err(|error| {
                    TransactionStatementError::Transaction(LockSqlError {
                        code: 1105,
                        state: *b"HY000",
                        message: format!(
                            "cannot keep the transaction's primary lock alive: {error}"
                        ),
                    })
                })?;
            self.keep_alive = Some(keep_alive);
        }
        Ok(())
    }

    /// Publishes every buffered mutation in one two-phase commit.
    ///
    /// A transaction that staged nothing commits trivially — there is nothing to
    /// publish and no commit timestamp to take — after releasing any lock it
    /// took. An optimistic transaction learns here, and only here, that another
    /// transaction beat it to a key: that is the 9007 the client sees.
    pub fn commit(self) -> Result<TransactionEnd, TransactionStatementError> {
        self.finish(true)
    }

    /// Discards every buffered mutation and releases every held lock.
    ///
    /// Nothing buffered was ever published, so a rollback publishes nothing
    /// either; the only physical work is returning the pessimistic locks.
    pub fn rollback(self) -> Result<TransactionEnd, TransactionStatementError> {
        self.finish(false)
    }

    fn finish(mut self, publish: bool) -> Result<TransactionEnd, TransactionStatementError> {
        let call = self.call.clone();
        let mutations = if publish {
            std::mem::take(&mut self.buffer).into_mutations()
        } else {
            Vec::new()
        };
        // The keep-alive thread must stop before the locks it refreshes are
        // resolved, so a heartbeat can never revive a lock the commit released.
        if let Some(keep_alive) = self.keep_alive.take() {
            keep_alive.close();
        }
        let end = if publish {
            TransactionEnd::Committed
        } else {
            TransactionEnd::RolledBack
        };
        match self.open {
            OpenTransaction::Optimistic(transaction) => {
                if mutations.is_empty() {
                    transaction
                        .finish_without_writes()
                        .map_err(coordinator_error)?;
                    return Ok(end);
                }
                let outcome = transaction
                    .commit(mutations, &call)
                    .map_err(coordinator_error)?;
                classify_commit_outcome(&outcome).map(|()| end)
            }
            OpenTransaction::Pessimistic(mut transaction) => {
                if mutations.is_empty() {
                    let held = transaction.locked_keys();
                    transaction
                        .pessimistic_rollback(&held, &call)
                        .map_err(|cause| {
                            TransactionStatementError::Transaction(transaction_cause_error(&cause))
                        })?;
                    transaction
                        .into_two_pc()
                        .finish_without_writes()
                        .map_err(coordinator_error)?;
                    return Ok(end);
                }
                let outcome = transaction
                    .commit(mutations, &call)
                    .map_err(coordinator_error)?;
                classify_commit_outcome(&outcome).map(|()| end)
            }
        }
    }
}

/// Read-your-own-writes for the write planner.
///
/// The transaction's own staged entry for `key` wins outright; only an unstaged
/// key falls through to the snapshot at `start_ts`. That is `tikvTxn.Get`'s
/// order, and it is what lets a second `UPDATE` of one row compute its new value
/// from the first one's.
impl WritePlanningSnapshot for MultiStatementTransaction {
    fn read_at_snapshot(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, ConfiguredWriteError> {
        if let Some(staged) = self.buffer.staged(key) {
            return Ok(match staged.kind() {
                OptimisticMutationKind::Delete | OptimisticMutationKind::IndexDelete => None,
                OptimisticMutationKind::Insert
                | OptimisticMutationKind::PutExisting
                | OptimisticMutationKind::IndexPut => Some(staged.value().to_vec()),
            });
        }
        Ok(self.open.two_pc().snapshot_get(key, call)?.value)
    }
}

/// Decodes one staged row into exactly the columns a read projects.
///
/// The clustered primary key is not stored in the row value — it *is* the record
/// key's handle — so it is filled from the handle, while every other projected
/// column is decoded from the row bytes at its own configured type.
fn decode_staged_projection(
    projection: &[ResolvedProjectionColumn],
    handle: i64,
    row: &[u8],
) -> Result<Vec<Datum>, ConfiguredWriteError> {
    projection
        .iter()
        .map(|column| match (column.kind(), column.scalar_type()) {
            (ConfiguredColumnKind::ClusteredPrimaryKey, _) => Ok(Datum::new_int(handle)),
            (_, ConfiguredScalarType::BigInt | ConfiguredScalarType::Int) => Ok(Datum::new_int(
                decode_configured_row_int(row, column.scan_column().column_id)?,
            )),
            (_, ConfiguredScalarType::Char { .. }) => Ok(Datum::new_string(
                decode_configured_row_bytes(row, column.scan_column().column_id)?,
            )),
            // The write path cannot produce a row with either type (see
            // `ConfiguredWriteError::UnsupportedScalarType`), so a staged row
            // never carries one; refusing keeps that fact checked rather than
            // assumed.
            (
                _,
                scalar_type @ (ConfiguredScalarType::UnsignedBigInt
                | ConfiguredScalarType::Double
                | ConfiguredScalarType::Varchar { .. }
                | ConfiguredScalarType::Decimal { .. }),
            ) => Err(ConfiguredWriteError::UnsupportedScalarType {
                column: column.source_name().to_owned(),
                scalar_type,
            }),
        })
        .collect()
}

/// The clustered handles one bound write touches, which are the rows a
/// pessimistic transaction locks before it plans the statement.
fn written_handles(write: &ConfiguredPreparedWrite) -> Vec<i64> {
    match write {
        ConfiguredPreparedWrite::InsertRows { .. } => {
            // An INSERT's row is new, so there is no existing row to lock; TiKV
            // enforces its absence through the Insert operation's assertion.
            Vec::new()
        }
        ConfiguredPreparedWrite::UpdatePoint { handle, .. }
        | ConfiguredPreparedWrite::DeletePoint { handle, .. } => vec![*handle],
    }
}

/// Maps a terminal commit outcome to what the client is told.
///
/// Only `Committed` may answer `COMMIT` with success: every other state means
/// the writes are not durable, and a write conflict among them is the 9007 an
/// optimistic transaction exists to report.
fn classify_commit_outcome(
    outcome: &OptimisticCommitOutcome,
) -> Result<(), TransactionStatementError> {
    match outcome {
        OptimisticCommitOutcome::Committed(_) => Ok(()),
        OptimisticCommitOutcome::RolledBack(rolled_back) => Err(
            TransactionStatementError::Transaction(transaction_cause_error(&rolled_back.cause)),
        ),
        OptimisticCommitOutcome::CleanupFailed(failed) => Err(
            TransactionStatementError::Transaction(transaction_cause_error(&failed.cause)),
        ),
        OptimisticCommitOutcome::Undetermined(_) => {
            Err(TransactionStatementError::Transaction(LockSqlError {
                code: 1105,
                state: *b"HY000",
                message: "[kv:8005]transaction result is undetermined; the commit was published \
                          but its outcome is unknown"
                    .to_owned(),
            }))
        }
    }
}

/// Renders a transaction-ending cause as the error TiDB reports for it.
///
/// A write conflict is the one cause with a code of its own: Go's
/// `kv.ErrWriteConflict` (9007) is what an optimistic transaction that lost the
/// race reports at `COMMIT`. Everything else keeps its exact diagnostic under
/// the generic 1105 rather than being disguised as a retryable conflict.
fn transaction_cause_error(cause: &TransactionCause) -> LockSqlError {
    match cause {
        TransactionCause::WriteConflict { detail } => LockSqlError {
            code: ERR_WRITE_CONFLICT,
            state: *b"HY000",
            message: format!("[kv:9007]Write conflict, {detail} [try again later]"),
        },
        other => LockSqlError {
            code: 1105,
            state: *b"HY000",
            message: format!("[kv:1105]transaction failed: {other}"),
        },
    }
}

fn coordinator_error(error: OptimisticCoordinatorError) -> TransactionStatementError {
    TransactionStatementError::Transaction(LockSqlError {
        code: 1105,
        state: *b"HY000",
        message: format!("[kv:1105]transaction failed: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        classify_commit_outcome, transaction_cause_error, written_handles,
        TransactionStatementError, ERR_WRITE_CONFLICT,
    };
    use tidb_planner::prepared_dml::{ConfiguredAssignment, ConfiguredPreparedWrite};
    use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};
    use tidb_txnkv::transaction::{
        OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction,
        TransactionCause,
    };

    fn table() -> ConfiguredTable {
        ConfiguredTable::new(
            "campaign",
            "accounts",
            42,
            [
                ConfiguredColumn::clustered_primary_key("id", 1),
                ConfiguredColumn::stored_not_null("balance", 2),
            ],
        )
    }

    fn rolled_back(cause: TransactionCause) -> OptimisticCommitOutcome {
        OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
            cause,
        })
    }

    #[test]
    fn an_optimistic_commit_that_lost_the_race_reports_9007_and_ends_the_transaction() {
        // The asymmetry this whole type exists for: an optimistic transaction
        // cannot learn of the conflict before COMMIT, and the conflict costs the
        // transaction rather than one statement.
        let outcome = rolled_back(TransactionCause::WriteConflict {
            detail: "conflictStartTS=7".to_owned(),
        });
        let error = classify_commit_outcome(&outcome).expect_err("a lost race cannot report OK");
        assert_eq!(error.sql_error().code, ERR_WRITE_CONFLICT);
        assert!(error.sql_error().message.contains("conflictStartTS=7"));
        assert!(
            !error.keeps_transaction_open(),
            "a failed commit ends the transaction"
        );
    }

    #[test]
    fn a_successful_commit_is_the_only_outcome_that_answers_ok() {
        for cause in [
            TransactionCause::Region {
                detail: "epoch not match".to_owned(),
            },
            TransactionCause::Transport {
                detail: "connection reset".to_owned(),
            },
        ] {
            let error = classify_commit_outcome(&rolled_back(cause))
                .expect_err("a non-commit never reports durable rows");
            assert_eq!(
                error.sql_error().code,
                1105,
                "only a write conflict earns 9007"
            );
            assert!(!error.keeps_transaction_open());
        }
        assert!(classify_commit_outcome(&OptimisticCommitOutcome::Committed(
            tidb_txnkv::transaction::CommittedTransaction {
                receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
                secondary_failures: Vec::new(),
            }
        ))
        .is_ok());
    }

    #[test]
    fn a_write_conflict_is_the_only_cause_with_a_code_of_its_own() {
        assert_eq!(
            transaction_cause_error(&TransactionCause::WriteConflict {
                detail: "d".to_owned()
            })
            .code,
            ERR_WRITE_CONFLICT
        );
        let other = transaction_cause_error(&TransactionCause::AlreadyExists {
            key: b"k".to_vec(),
            detail: "duplicate".to_owned(),
        });
        assert_eq!(other.code, 1105);
        assert!(
            other.message.contains("duplicate"),
            "the exact cause survives instead of being flattened"
        );
    }

    #[test]
    fn only_statement_scoped_failures_keep_the_transaction_open() {
        let statement = TransactionStatementError::refused("bounded refusal");
        assert!(statement.keeps_transaction_open());
        let ended = TransactionStatementError::Transaction(statement.sql_error().clone());
        assert!(!ended.keeps_transaction_open());
    }

    #[test]
    fn a_pessimistic_statement_locks_the_rows_it_rewrites_and_not_the_rows_it_creates() {
        // An UPDATE/DELETE must lock the existing row before planning it; an
        // INSERT has no existing row, and TiKV asserts its absence at Prewrite.
        let table = table();
        assert_eq!(
            written_handles(&ConfiguredPreparedWrite::UpdatePoint {
                table: table.clone(),
                handle: 9,
                column_index: 1,
                assignment: ConfiguredAssignment::Set(4),
            }),
            vec![9]
        );
        assert_eq!(
            written_handles(&ConfiguredPreparedWrite::DeletePoint {
                table: table.clone(),
                handle: -3,
            }),
            vec![-3]
        );
        assert!(written_handles(&ConfiguredPreparedWrite::InsertRows {
            table,
            rows: Vec::new(),
        })
        .is_empty());
    }
}
