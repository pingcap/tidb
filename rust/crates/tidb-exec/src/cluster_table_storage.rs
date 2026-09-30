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

//! The production side of the cluster table storage: a real transaction behind
//! the executor's [`ClusterSnapshot`], and the COMMIT that publishes the
//! session's staged buffer through the existing optimistic 2PC.
//!
//! # The transaction lifecycle this chooses
//!
//! There are two lifecycles here, and which one applies is exactly Go's rule.
//!
//! **Autocommit.** One statement opens one read-only transaction
//! ([`StatementSnapshot`]), reads every key it needs at that transaction's
//! single `start_ts`, and finishes without writes; the statement's writes stay
//! in the session's [`MutationBuffer`] and are published by
//! [`commit_staged_buffer`] as one transaction at the end of the statement.
//! Each autocommit statement that reads a cluster row therefore gets its own
//! fresh timestamp, which is what Go's autocommit does too: `BEGIN` is
//! implicit and ends with the statement. Two statements spend none. A
//! statement that reads no cluster row never activates this transaction at all:
//! the cluster session driver starts the open asynchronously after planning,
//! but only the first read waits for and exposes it. And a statement that
//! DECLARED its whole read is one point get on the clustered handle uses
//! [`MaxTsSnapshot`] instead, at `u64::MAX`, which is Go's
//! `AdviseOptimizeWithPlan` shortcut. That reader runs directly on the
//! connection worker and constructs no reusable transaction state; the
//! declaration is a statement-level fact and never inferred from a read,
//! because at this seam an `UPDATE`'s read-before-write is the same `get` on
//! the same key.
//!
//! **Explicit `BEGIN` ... `COMMIT`.** [`SessionTransaction`] opens *one*
//! transaction at `BEGIN` and keeps it open. Every statement in between reads
//! through [`SessionTransaction::snapshot`], which serves reads on that one
//! transaction at its original `start_ts`, and `COMMIT` prewrites the whole
//! staged buffer on that same transaction — so the prewrite carries the
//! `BEGIN` timestamp. That is what makes conflict detection faithful: TiKV
//! rejects a prewrite whose key has a commit newer than `start_ts`, so a writer
//! that raced this transaction between its read and its commit is reported as
//! `WriteConflict` (9007) rather than silently overwritten. It is also
//! repeatable read: a statement inside the transaction cannot see a commit made
//! after `BEGIN`, because there is no newer timestamp to see it at.
//!
//! The read path in both lifecycles is Go's `MemBuffer`-in-front-of-snapshot:
//! the session's staged writes win, and only an unstaged key reaches the
//! snapshot.
//!
//! [`ClusterSnapshot`]: tidb_executor::cluster_storage::ClusterSnapshot
//! [`MutationBuffer`]: tidb_executor::cluster_storage::MutationBuffer

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::multi_statement_transaction::TRANSACTION_END_TIMEOUT;
use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, DuplicateKeyHint, MutationBuffer, SnapshotPairs,
};
use tidb_executor::storage::StorageError;
use tidb_hack::GoToLower;
use tidb_pd_client::PdClient;
use tidb_txnkv::pd_capability::{CapabilityTimestampSource, TimestampFutureWait};
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext, UnaryCancellation};
use tidb_txnkv::transaction::{
    BufferMutation, CommitProtocol, LockWaitTime, OptimisticCommitOutcome,
    OptimisticCoordinatorError, PessimisticLockFailure, RealOptimisticTransaction,
    RealOptimisticTransactionOpener, RealPessimisticTransaction, SchemaLease, SchemaLeaseChecker,
    StorePdCapability, StoreWriteClient, StoreWriteLoader, TransactionCause,
};
use tidb_txnkv::Key;
use tidb_txnkv::PdRegionLoader;

use crate::pessimistic_lock_error::{
    commit_outcome_to_sql_error_with_hint, duplicate_cause, duplicate_key_sql_error,
    is_retryable_statement_failure, lock_failure_to_sql_error, transaction_cause_to_sql_error,
    LockSqlError, PessimisticStatementRetry,
};

/// What one statement's lock acquisition came to -- the session layer's
/// half of Go's `handlePessimisticDML` protocol.
#[derive(Debug)]
pub enum LockKeysOutcome {
    /// Every key is locked at `for_update_ts`; the statement stands.
    Locked {
        /// The statement timestamp the locks carry.
        for_update_ts: u64,
        /// The keys THIS call newly locked (already-held keys excluded), so
        /// the session can release exactly a failed statement's accumulation
        /// -- Go `OnPessimisticStmtEnd(isSuccessful=false)` ->
        /// `CancelFairLocking`.
        newly_locked: Vec<Vec<u8>>,
    },
    /// The locks are HELD, but a newer committed version beat the statement
    /// (fair locking's `locked_with_conflict`, or a write conflict during
    /// acquisition). The statement's effects must be rolled back and the
    /// statement RE-EXECUTED reading at this advanced `for_update_ts` --
    /// Go's `handlePessimisticLockError` -> `UpdateForUpdateTS` -> rebuild.
    RetryStatement {
        /// The advanced statement timestamp the retry reads at.
        for_update_ts: u64,
        /// The keys THIS call newly locked and RETAINED across the retry
        /// (fair locking's whole point); see [`LockKeysOutcome::Locked`].
        newly_locked: Vec<Vec<u8>>,
    },
    /// The statement fails with this error and its locks are released; the
    /// transaction stays open (Go's statement-scoped 1205/1213 family).
    StatementError(LockSqlError),
    /// The transaction itself is no longer usable.
    TransactionError(LockSqlError),
}

/// Failure while executing one restricted SQL statement in a pessimistic
/// transaction.
#[derive(Debug)]
pub enum PessimisticStatementTransactionError {
    /// Snapshot creation or statement planning failed before locking.
    Build(String),
    /// Locking or the final transaction commit produced a SQL error.
    Transaction(LockSqlError),
}

impl fmt::Display for PessimisticStatementTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Build(detail) => formatter.write_str(detail),
            Self::Transaction(error) => formatter.write_str(&error.message),
        }
    }
}

impl std::error::Error for PessimisticStatementTransactionError {}

/// Converts a direct system-table mutation plan into the session transaction
/// buffer used by the ordinary pessimistic statement path.
///
/// Statistics restricted SQL and user SQL produce the same TiKV mutation
/// kinds. Keeping this conversion here makes both paths apply the same lazy
/// INSERT assertions and lock-key classifier.
#[must_use]
pub fn mutation_buffer_from_mutations(
    mutations: Vec<BufferMutation>,
) -> Result<MutationBuffer, StorageError> {
    let buffer = MutationBuffer::new();
    stage_mutations(&buffer, mutations)?;
    Ok(buffer)
}

/// Stages direct mutation-plan output into an existing transaction buffer.
pub fn stage_mutations(
    buffer: &MutationBuffer,
    mutations: Vec<BufferMutation>,
) -> Result<(), StorageError> {
    use tidb_txnkv::transaction::BufferMutationOp;

    buffer.stage_owned_batch(mutations.into_iter().filter_map(|mutation| {
        let (op, raw_key, value, presume, assertion) = mutation.into_parts();
        let key = Key::from_bytes(raw_key);
        match op {
            BufferMutationOp::Delete => Some((key, None, presume, assertion)),
            BufferMutationOp::Set => Some((key, Some(value), presume, assertion)),
            BufferMutationOp::Lock => None,
        }
    }))
}
/// Go `handlePessimisticDML`'s lock half for one statement's keys: acquire at
/// the current `for_update_ts` with the session lock-wait timeout, and turn
/// every outcome into the session layer's next move.
///
/// The internal retry is only for a RETRYABLE DEADLOCK, mirroring
/// `multi_statement_transaction::lock_keys`; a write conflict or a
/// fair-locking grant-with-conflict is NOT retried here, because its remedy
/// is re-executing the statement at the advanced `for_update_ts`, which only
/// the session layer can do.
fn acquire_statement_locks<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    transaction: &mut RealPessimisticTransaction<C, L, CapabilityTimestampSource<P>>,
    lock_values: &mut PessimisticLockCache,
    keys: &[Vec<u8>],
    presume_not_exists: &BTreeSet<Vec<u8>>,
    duplicate_hints: &BTreeMap<Vec<u8>, DuplicateKeyHint>,
    return_values: bool,
    wait: LockWaitTime,
    call: &UnaryCallContext,
) -> LockKeysOutcome {
    // KVTxn owns held-key filtering, existence checks, rollback of partial
    // lock attempts, and fair-lock retry state. TiDB only classifies the SQL
    // result and asks its statement provider for a newer read timestamp.
    let result = if return_values {
        transaction.acquire_locks_returning_values(keys, presume_not_exists, wait, call)
    } else {
        transaction.acquire_locks(keys, presume_not_exists, wait, call)
    };
    match result {
        Ok(acquired) => {
            lock_values.current.extend(acquired.values);
            if acquired.locked_with_conflict.is_empty() {
                return LockKeysOutcome::Locked {
                    for_update_ts: acquired.for_update_ts,
                    newly_locked: acquired.keys,
                };
            }
            lock_values.current.clear();
            match transaction.advance_for_update_ts() {
                Ok(for_update_ts) => LockKeysOutcome::RetryStatement {
                    for_update_ts,
                    newly_locked: acquired.keys,
                },
                Err(error) => LockKeysOutcome::TransactionError(lock_failure_to_sql_error(&error)),
            }
        }
        Err(failure) => {
            if let PessimisticLockFailure::Deadlock(detail) = &failure {
                crate::deadlock_recording::record_deadlock(detail);
            }
            if let PessimisticLockFailure::Transaction(cause) = &failure {
                if duplicate_cause(cause) {
                    if let Some(hint) = duplicate_hints.get(cause.key()) {
                        return LockKeysOutcome::StatementError(duplicate_key_sql_error(hint));
                    }
                }
            }
            if is_retryable_statement_failure(&failure) {
                lock_values.current.clear();
                return match transaction.advance_for_update_ts() {
                    Ok(for_update_ts) => LockKeysOutcome::RetryStatement {
                        for_update_ts,
                        newly_locked: Vec::new(),
                    },
                    Err(error) => {
                        LockKeysOutcome::TransactionError(lock_failure_to_sql_error(&error))
                    }
                };
            }
            let error = lock_failure_to_sql_error(&failure);
            if failure.is_statement_scoped() {
                LockKeysOutcome::StatementError(error)
            } else {
                LockKeysOutcome::TransactionError(error)
            }
        }
    }
}

/// One statement's read snapshot: a real read-only transaction at one PD
/// timestamp, owned by the CONNECTION worker that opened it.
///
/// This is the autocommit shape. Inside an explicit transaction the session
/// reads through [`SessionTransaction::snapshot`] instead, so every statement
/// shares the one timestamp `BEGIN` took.
///
/// The transaction is held inline rather than behind a borrowed worker
/// thread. Go's autocommit read builds its `KVSnapshot` on the connection
/// goroutine and hands nothing to another thread, and there is nothing here
/// that needs one either: `SharedReadRuntime` is `Arc<Mutex<C>>` plus
/// `BackgroundRegionCache<L>`, so the transport is shared, not worker-local,
/// and `tests/transaction_send_source.rs` asserts as much. Sampling a 200-row
/// range put 192 of 4195 samples in the handshake this removes -- the whole
/// cost of shipping a purely local `begin_read_only_at` to another thread and
/// waiting for it to come back.
pub struct StatementSnapshot<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    /// `None` once the statement has finished; every read after that is
    /// refused because its transaction has already ended.
    transaction: Option<RealOptimisticTransaction<C, L, CapabilityTimestampSource<P>>>,
    start_ts: u64,
    timeout: Duration,
    /// Reused by all reads in this statement; see `SessionSnapshot`.
    cancellation: UnaryCancellation,
}

/// One statement snapshot whose ordinary PD timestamp request is in flight.
///
/// Unlike [`StatementSnapshot`], this owns only the in-flight timestamp. Go's
/// warmup stores only an oracle future; the connection-worker transaction is
/// opened when [`Self::wait`] activates the snapshot for the first storage read.
pub struct PreparedStatementSnapshot<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient>
where
    P: StorePdCapability,
{
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
    start_ts: P::TsFuture,
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>
    PreparedStatementSnapshot<C, L, P>
{
    /// Waits for the timestamp prepared after planning, then opens the
    /// read-only transaction HERE. `begin_read_only_at` spends no timestamp
    /// and sends no request -- it is local state over an already-shared
    /// transport -- so handing it to another thread only bought a round trip.
    pub fn wait(self) -> Result<StatementSnapshot<C, L, P>, OptimisticCoordinatorError> {
        let start_ts = self
            .start_ts
            .wait()
            .map_err(|error| OptimisticCoordinatorError::Timestamp(error.to_string()))?;
        let transaction = self.opener.begin_read_only_at(start_ts)?;
        Ok(StatementSnapshot {
            transaction: Some(transaction),
            start_ts,
            timeout: self.timeout,
            cancellation: UnaryCancellation::new(),
        })
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> fmt::Debug
    for StatementSnapshot<C, L, P>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatementSnapshot")
            .field("start_ts", &self.start_ts)
            .field("open", &self.transaction.is_some())
            .finish()
    }
}

impl StatementSnapshot {
    /// Starts fetching one ordinary read-only transaction's PD timestamp
    /// without opening the transaction itself.
    pub fn prepare<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
    ) -> Result<PreparedStatementSnapshot<C, L, P>, OptimisticCoordinatorError> {
        let start_ts = opener.prepare_read_only_start_ts()?;
        Ok(PreparedStatementSnapshot {
            opener,
            timeout,
            start_ts,
        })
    }

    /// Opens one read-only transaction on the CALLING thread, spending
    /// exactly one PD timestamp.
    pub fn open<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
    ) -> Result<StatementSnapshot<C, L, P>, OptimisticCoordinatorError> {
        StatementSnapshot::prepare(opener, timeout)?.wait()
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> StatementSnapshot<C, L, P> {
    /// The timestamp every read of this statement is served at.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.start_ts
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> StatementSnapshot<C, L, P> {
    /// Ends the statement's read transaction, leaving no locks behind.
    ///
    /// Calling it twice is a no-op: the statement is already finished.
    pub fn finish(&mut self) -> Result<(), StorageError> {
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };
        transaction
            .finish_without_writes()
            .map(|_| ())
            .map_err(|error| StorageError::Backend(error.to_string()))
    }

    /// One call context per read, never one for the snapshot.
    /// [`UnaryCallContext`] carries an ABSOLUTE deadline, so a context minted
    /// when the snapshot opened would charge a later read for the wall-clock
    /// time the statement spent between them.
    fn call(&self) -> UnaryCallContext {
        UnaryCallContext::with_deadline(Instant::now() + self.timeout, self.cancellation.clone())
    }

    fn reader(
        &mut self,
    ) -> Result<&mut RealOptimisticTransaction<C, L, CapabilityTimestampSource<P>>, StorageError>
    {
        self.transaction
            .as_mut()
            .ok_or_else(|| StorageError::Backend("the transaction is already finished".to_owned()))
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> Drop
    for StatementSnapshot<C, L, P>
{
    fn drop(&mut self) {
        // Go abandons an autocommit read snapshot after the statement: there
        // are no writes or locks whose cleanup the foreground must observe.
        // Ending it here costs nothing to wait for -- `finish_without_writes`
        // on a read-only transaction is a local state transition and sends no
        // request -- which is why this no longer has to be detached to keep
        // the next statement off its critical path.
        if let Some(transaction) = self.transaction.take() {
            let _ = transaction.finish_without_writes();
        }
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> ClusterSnapshot
    for StatementSnapshot<C, L, P>
{
    fn point_rpc_counts(&mut self) -> (u64, u64) {
        self.transaction
            .as_ref()
            .map_or((0, 0), RealOptimisticTransaction::snapshot_point_rpc_counts)
    }

    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        let call = self.call();
        let read_ts = self.start_ts;
        let bytes = key.as_bytes();
        self.reader()?
            .snapshot_get_at(bytes, read_ts, &call)
            .map(|result| result.value)
            .map_err(classify)
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        let call = self.call();
        let read_ts = self.start_ts;
        let keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        self.reader()?
            .snapshot_batch_get_at(&keys, read_ts, &call)
            .map_err(classify)
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        let call = self.call();
        let read_ts = self.start_ts;
        let start = start.as_bytes();
        let end = end.as_bytes();
        self.reader()?
            .snapshot_scan_at(start, end, limit, read_ts, &call)
            .map_err(classify)
    }

    fn start_ts(&self) -> u64 {
        self.start_ts
    }
}

/// One direct latest-committed point read with no writable transaction state.
///
/// The session creates this only after the root plan has declared Go's
/// autocommit point-get shape. A second read would not have snapshot isolation
/// at `u64::MAX`, so this handle consumes exactly one Get and refuses every
/// scan or later Get. Each accepted read opens only a shared read lease;
/// region retries, lock recovery, GC visibility, and the call deadline remain
/// those of the transaction snapshot reader.
pub struct MaxTsSnapshot<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient> {
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
    /// Reused for the one direct read (and retained for the defensive scan
    /// path) so MaxTS does not allocate a cancellation channel per operation.
    cancellation: UnaryCancellation,
    consumed: bool,
    get_rpc_count: u64,
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> MaxTsSnapshot<C, L, P> {
    /// Binds the direct read to the process transaction/read authority.
    #[must_use]
    pub fn new(opener: Arc<RealOptimisticTransactionOpener<C, L, P>>, timeout: Duration) -> Self {
        Self {
            opener,
            timeout,
            cancellation: UnaryCancellation::new(),
            consumed: false,
            get_rpc_count: 0,
        }
    }

    fn consume(&mut self) -> Result<(), StorageError> {
        if std::mem::replace(&mut self.consumed, true) {
            return Err(StorageError::Backend(
                "a MaxTS point snapshot cannot serve a second read".to_owned(),
            ));
        }
        Ok(())
    }
}

impl<C, L, P> fmt::Debug for MaxTsSnapshot<C, L, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaxTsSnapshot")
            .field("consumed", &self.consumed)
            .finish_non_exhaustive()
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> ClusterSnapshot
    for MaxTsSnapshot<C, L, P>
{
    fn point_rpc_counts(&mut self) -> (u64, u64) {
        (self.get_rpc_count, 0)
    }

    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.consume()?;
        let call = UnaryCallContext::with_deadline(
            Instant::now() + self.timeout,
            self.cancellation.clone(),
        );
        let (value, rpc_count) = self
            .opener
            .snapshot_get_at_max_ts(key.as_bytes(), &call)
            .map_err(classify)?;
        self.get_rpc_count = self.get_rpc_count.wrapping_add(rpc_count);
        Ok(value)
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.consume()?;
        // A bounded single-row statement still has a range-shaped plan. Keep
        // its MaxTS declaration, but use the direct range reader rather than
        // activating an ordinary timestamped transaction for each scan.
        let call = UnaryCallContext::with_deadline(
            Instant::now() + self.timeout,
            self.cancellation.clone(),
        );
        self.opener
            .snapshot_scan_at_max_ts(start.as_bytes(), end.as_bytes(), limit, &call)
            .map_err(classify)
    }

    fn start_ts(&self) -> u64 {
        u64::MAX
    }
}

/// One connection's open `BEGIN` ... `COMMIT`: a single transaction that every
/// statement in between reads through and that `COMMIT` prewrites on.
///
/// Holding one transaction is what makes conflict detection Go's. The prewrite
/// carries the timestamp `BEGIN` took, so TiKV refuses it when a key this
/// transaction touched was committed by someone else after that timestamp, and
/// every statement in between reads at that timestamp, which is repeatable
/// read.
pub struct SessionTransaction<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    state: Arc<Mutex<SessionTransactionState<C, L, P>>>,
    start_ts: u64,
    timeout: Duration,
    /// Wall clock of BEGIN, Go's
    /// `tidb_session_transaction_duration_seconds` start point.
    opened_at: std::time::Instant,
    /// Statements executed inside this transaction, Go's
    /// `tidb_session_transaction_statement_num` observation input.
    statement_count: std::cell::Cell<u64>,
    /// Whether this is a pessimistic transaction -- decided by the
    /// session's `tidb_txn_mode` at `BEGIN`, Go's `DefTiDBTxnMode`
    /// (pessimistic) being the default.
    pessimistic: bool,
    /// `@@tidb_pessimistic_txn_fair_locking` as it stood at `BEGIN`; reaches
    /// the locking transaction when the lazy pessimistic state is promoted
    /// (Go `OnPessimisticStmtStart` -> `KVTxn.StartFairLocking`).
    fair_locking: bool,
    /// The session's schema lease checker, carried into the commit together
    /// with the physical tables the commit writes (Go
    /// `SetOptionsBeforeCommit`, `base.go:560-616`).
    schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
}

/// Go TxnCtx's transaction cache and CurrentStmtPessimisticLockCache.
#[derive(Default)]
struct PessimisticLockCache {
    previous: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    current: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}
impl PessimisticLockCache {
    fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.current.get(key).or_else(|| self.previous.get(key))
    }
}

enum SessionTransactionState<C, L, P: StorePdCapability> {
    Optimistic(RealOptimisticTransaction<C, L, CapabilityTimestampSource<P>>),
    /// A pessimistic transaction before its first locking statement.
    ///
    /// Go keeps this state as an ordinary `KVTxn` and creates pessimistic
    /// committer state only when a statement asks for locks. Most read-only
    /// transactions never cross that boundary, so eagerly constructing and
    /// tearing down the pessimistic wrapper on every `BEGIN` is both needless
    /// work and a different lifecycle.
    PessimisticPending {
        transaction: RealOptimisticTransaction<C, L, CapabilityTimestampSource<P>>,
        opened_at: Instant,
    },
    Pessimistic {
        transaction: RealPessimisticTransaction<C, L, CapabilityTimestampSource<P>>,
        lock_values: PessimisticLockCache,
    },
    Finished,
}

/// Crosses Go's lazy pessimistic boundary exactly once, immediately before
/// the first statement that actually needs row locks.
fn promote_pessimistic_state<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    state: &mut SessionTransactionState<C, L, P>,
    fair_locking: bool,
) -> Result<(), StorageError> {
    if !matches!(state, SessionTransactionState::PessimisticPending { .. }) {
        return Ok(());
    }
    let pending = std::mem::replace(state, SessionTransactionState::Finished);
    let SessionTransactionState::PessimisticPending {
        transaction,
        opened_at,
    } = pending
    else {
        unreachable!("the lazy pessimistic state was checked before promotion")
    };
    let mut transaction = RealPessimisticTransaction::from_transaction(transaction, opened_at)
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    // Go arms fair locking per pessimistic statement
    // (`basePessimisticTxnContextProvider.OnPessimisticStmtStart`); here the
    // session-level switch reaches the transaction once, at promotion, and
    // `acquire_locks` applies Go's single-key `ForceLock` rule per statement.
    transaction.set_fair_locking(fair_locking);
    *state = SessionTransactionState::Pessimistic {
        transaction,
        lock_values: PessimisticLockCache::default(),
    };
    Ok(())
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> fmt::Debug
    for SessionTransaction<C, L, P>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let open = !matches!(&*state, SessionTransactionState::Finished);
        formatter
            .debug_struct("SessionTransaction")
            .field("start_ts", &self.start_ts)
            .field("open", &open)
            .finish()
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> SessionTransaction<C, L, P> {
    /// Bind executor staging to the client's own transaction buffer.
    pub fn bind_mutation_buffer(&self, buffer: &MutationBuffer) {
        let state = self.state.clone();
        buffer.bind_native(
            self.start_ts,
            Arc::new(move |visit| {
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &mut *state {
                    SessionTransactionState::Optimistic(transaction)
                    | SessionTransactionState::PessimisticPending { transaction, .. } => {
                        visit(transaction.mem_buffer())
                    }
                    SessionTransactionState::Pessimistic { transaction, .. } => {
                        visit(transaction.snapshot().mem_buffer())
                    }
                    SessionTransactionState::Finished => return false,
                }
                true
            }),
        );
    }

    /// Wall clock of BEGIN (Go's transaction-duration start point).
    pub fn opened_at(&self) -> std::time::Instant {
        self.opened_at
    }

    /// Statements executed so far in this transaction.
    pub fn statement_count(&self) -> u64 {
        self.statement_count.get()
    }

    /// Counts one statement against this transaction (Go's
    /// `tidb_session_transaction_statement_num` observation input).
    pub fn note_statement(&self) {
        self.statement_count.set(self.statement_count.get() + 1);
    }

    /// Opens a locking statement at a fresh for-update timestamp, as Go's
    /// pessimistic repeatable-read provider does for non-point locking reads.
    pub fn fresh_locking_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, StorageError> {
        let read_ts = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            promote_pessimistic_state(&mut state, self.fair_locking)?;
            match &mut *state {
                SessionTransactionState::Pessimistic { transaction, .. } => transaction
                    .advance_for_update_ts()
                    .map_err(|error| StorageError::Backend(error.to_string()))?,
                _ => {
                    return Err(StorageError::Backend(
                        "only a pessimistic transaction advances its locking snapshot".to_owned(),
                    ))
                }
            }
        };
        self.snapshot_at_for(read_ts, true)
    }

    /// Changes the resource group stamped on subsequent reads, locks, prewrite,
    /// commit, cleanup, and lock-resolution requests of this transaction.
    /// Go refreshes the transaction option from each statement context; the
    /// transaction remains open while this per-statement property changes.
    pub fn set_resource_group_name(&self, name: &str) -> Result<(), StorageError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            SessionTransactionState::Optimistic(transaction)
            | SessionTransactionState::PessimisticPending { transaction, .. } => {
                tidb_txnkv::set_txn_resource_group(transaction, name);
            }
            SessionTransactionState::Pessimistic { transaction, .. } => {
                tidb_txnkv::set_txn_resource_group(transaction, name);
            }
            SessionTransactionState::Finished => {
                return Err(StorageError::Backend(
                    "the transaction is already finished".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Opens the transaction `BEGIN` holds, spending exactly one PD timestamp.
    ///
    /// The native buffer validates actual writes against configured byte limits.
    pub fn begin(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
        commit_protocol: CommitProtocol,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let mut transaction = opener.begin()?;
        transaction.set_commit_protocol(commit_protocol);
        let start_ts = transaction.start_ts();
        Ok(Self {
            opened_at: std::time::Instant::now(),
            statement_count: std::cell::Cell::new(0),
            state: Arc::new(Mutex::new(SessionTransactionState::Optimistic(transaction))),
            start_ts,
            timeout,
            pessimistic: false,
            fair_locking: false,
            schema_lease_checker: None,
        })
    }

    /// [`Self::begin`] at a timestamp already obtained by
    /// [`RealOptimisticTransactionOpener::prepare_read_only_start_ts`], so the
    /// PD round trip can overlap the statement's own planning (Go's
    /// `txnFuture`: the timestamp request is dispatched at warm-up and waited
    /// for at first use).
    pub fn begin_at(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        start_ts: u64,
        timeout: Duration,
        commit_protocol: CommitProtocol,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let mut transaction = opener.begin_at(start_ts)?;
        transaction.set_commit_protocol(commit_protocol);
        Ok(Self {
            opened_at: std::time::Instant::now(),
            statement_count: std::cell::Cell::new(0),
            state: Arc::new(Mutex::new(SessionTransactionState::Optimistic(transaction))),
            start_ts,
            timeout,
            pessimistic: false,
            fair_locking: false,
            schema_lease_checker: None,
        })
    }

    /// [`Self::begin_pessimistic`] at a timestamp already obtained by
    /// [`RealOptimisticTransactionOpener::prepare_read_only_start_ts`].
    pub fn begin_pessimistic_at(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        start_ts: u64,
        timeout: Duration,
        commit_protocol: CommitProtocol,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let opened_at = Instant::now();
        let mut transaction = opener.begin_at(start_ts)?;
        transaction.set_commit_protocol(commit_protocol);
        Ok(Self {
            opened_at: std::time::Instant::now(),
            statement_count: std::cell::Cell::new(0),
            state: Arc::new(Mutex::new(SessionTransactionState::PessimisticPending {
                transaction,
                opened_at,
            })),
            start_ts,
            timeout,
            pessimistic: true,
            fair_locking: false,
            schema_lease_checker: None,
        })
    }

    /// Opens the pessimistic transaction `BEGIN` holds under Go's default
    /// `tidb_txn_mode = 'pessimistic'`: the same one-timestamp transaction,
    /// which additionally serves the statement-lock protocol
    /// ([`Self::lock_keys`]) and commits with pessimistic constraints.
    pub fn begin_pessimistic(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
        commit_protocol: CommitProtocol,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let opened_at = Instant::now();
        let mut transaction = opener.begin()?;
        transaction.set_commit_protocol(commit_protocol);
        let start_ts = transaction.start_ts();
        Ok(Self {
            opened_at: std::time::Instant::now(),
            statement_count: std::cell::Cell::new(0),
            state: Arc::new(Mutex::new(SessionTransactionState::PessimisticPending {
                transaction,
                opened_at,
            })),
            start_ts,
            timeout,
            pessimistic: true,
            fair_locking: false,
            schema_lease_checker: None,
        })
    }

    /// Whether this transaction locks per statement and commits with
    /// pessimistic constraints.
    #[must_use]
    pub const fn is_pessimistic(&self) -> bool {
        self.pessimistic
    }

    /// `@@tidb_pessimistic_txn_fair_locking` reaching this transaction. Only a
    /// pessimistic transaction locks, so only it can lock fairly; the flag is
    /// applied when the lazy pessimistic state is promoted, before the first
    /// lock request.
    pub const fn set_fair_locking(&mut self, enabled: bool) {
        self.fair_locking = enabled;
    }

    /// Whether the promoted locking transaction locks fairly -- Go
    /// `KVTxn.IsInFairLockingMode`. `false` before the first locking statement
    /// promotes the lazy pessimistic state, whatever the switch says.
    #[must_use]
    pub fn is_in_fair_locking_mode(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*state {
            SessionTransactionState::Pessimistic { transaction, .. } => {
                transaction.is_in_fair_locking_mode()
            }
            _ => false,
        }
    }

    /// Acquires pessimistic locks on one statement's written keys at the
    /// transaction's current `for_update_ts` -- Go `handlePessimisticDML`'s
    /// lock step. The outcome tells the session layer whether the statement
    /// stands, must be re-executed at an advanced timestamp, or failed.
    pub fn lock_keys(&self, keys: Vec<Vec<u8>>) -> Result<LockKeysOutcome, StorageError> {
        self.lock_keys_with_assertions(
            keys,
            BTreeSet::new(),
            BTreeMap::new(),
            false,
            LockWaitTime::default(),
        )
    }

    /// [`Self::lock_keys`], asking TiKV to answer each newly locked key's row
    /// WITH the lock and serving later reads of those keys from the answers —
    /// Go's point-write fold (`InitReturnValues` /
    /// `TxnCtx.SetPessimisticLockCache`, `pkg/executor/point_get.go:612-624`).
    pub fn lock_keys_with_values(
        &self,
        keys: Vec<Vec<u8>>,
        return_values: bool,
        wait: LockWaitTime,
    ) -> Result<LockKeysOutcome, StorageError> {
        self.lock_keys_with_assertions(keys, BTreeSet::new(), BTreeMap::new(), return_values, wait)
    }

    /// Acquires statement locks with the lazy INSERT assertions selected by
    /// Go's `getPessimisticLazyCheckMode`.
    pub fn lock_keys_with_assertions(
        &self,
        keys: Vec<Vec<u8>>,
        presume_not_exists: BTreeSet<Vec<u8>>,
        duplicate_hints: BTreeMap<Vec<u8>, DuplicateKeyHint>,
        return_values: bool,
        wait: LockWaitTime,
    ) -> Result<LockKeysOutcome, StorageError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        promote_pessimistic_state(&mut state, self.fair_locking)?;
        let call = UnaryCallContext::with_timeout(self.timeout);
        let lock_started = std::time::Instant::now();
        let outcome = match &mut *state {
            SessionTransactionState::Pessimistic {
                transaction,
                lock_values,
                ..
            } => acquire_statement_locks(
                transaction,
                lock_values,
                &keys,
                &presume_not_exists,
                &duplicate_hints,
                return_values,
                wait,
                &call,
            ),
            SessionTransactionState::Optimistic(_) => {
                LockKeysOutcome::TransactionError(LockSqlError {
                    code: 1105,
                    state: *b"HY000",
                    message: "a pessimistic lock requires a pessimistic transaction".to_owned(),
                })
            }
            SessionTransactionState::PessimisticPending { .. } => {
                unreachable!("pessimistic state was promoted before locking")
            }
            SessionTransactionState::Finished => {
                return Err(StorageError::Backend(
                    "the transaction is already finished".to_owned(),
                ));
            }
        };
        let lock_elapsed = lock_started.elapsed();
        if !matches!(outcome, LockKeysOutcome::TransactionError(_)) {
            // Go `LockKeysDetail.TotalTime` feeds the client-go
            // `pessimistic_lock_keys_duration` histogram (`adapter.go:588`),
            // charged per lock acquisition through the client's own registry.
            accumulate_lock_observation(keys.len() as u64, lock_elapsed);
            tidb_txnkv::client_go_metrics::observe_pessimistic_lock_keys_duration(
                lock_elapsed.as_secs_f64(),
            );
        }
        if matches!(outcome, LockKeysOutcome::TransactionError(_)) {
            let _ = finish_session_transaction(&mut state);
        }
        Ok(outcome)
    }

    /// Completes the client's fair-locking scope. Ordinary locks remain owned
    /// by the transaction when a statement fails, as Go's StmtRollback does.
    pub fn finish_statement(&self, successful: bool) -> Result<(), StorageError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let SessionTransactionState::Pessimistic {
            transaction,
            lock_values,
            ..
        } = &mut *state
        {
            transaction
                .finish_statement(successful)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            if successful {
                lock_values.previous.append(&mut lock_values.current);
            } else {
                lock_values.current.clear();
            }
        }
        Ok(())
    }

    /// The one timestamp every statement of this transaction reads at.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// A read handle onto this transaction, for one statement to bind.
    ///
    /// Dropping it ends the statement, not the transaction: that is the
    /// re-entry the shape exists for.
    pub fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, StorageError> {
        self.snapshot_for(false)
    }

    /// [`Self::snapshot`] told whether the statement it serves takes LOCKS --
    /// Go's `e.lock`, which is what admits the pessimistic lock cache
    /// (`pkg/executor/point_get.go:677`).
    pub fn snapshot_for(&self, locking: bool) -> Result<Box<dyn ClusterSnapshot>, StorageError> {
        Ok(Box::new(SessionSnapshot {
            state: Arc::clone(&self.state),
            cancellation: UnaryCancellation::new(),
            start_ts: self.start_ts,
            timeout: self.timeout,
            read_ts: None,
            locking,
        }))
    }

    /// A read handle whose reads happen at `read_ts` instead of `start_ts`:
    /// the retried pessimistic statement's view (Go rebuilds the retried
    /// executor reading at `forUpdateTS`).
    ///
    /// Refused for an optimistic transaction: only the pessimistic statement
    /// retry may read past `start_ts`, and an optimistic caller reaching here
    /// would silently break snapshot isolation with mixed-timestamp reads.
    pub fn snapshot_at(&self, read_ts: u64) -> Result<Box<dyn ClusterSnapshot>, StorageError> {
        self.snapshot_at_for(read_ts, false)
    }

    /// [`Self::snapshot_at`] told whether the statement takes locks.
    pub fn snapshot_at_for(
        &self,
        read_ts: u64,
        locking: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, StorageError> {
        if !self.pessimistic {
            return Err(StorageError::Backend(
                "only a pessimistic transaction reads at a statement timestamp".to_owned(),
            ));
        }
        Ok(Box::new(SessionSnapshot {
            state: Arc::clone(&self.state),
            cancellation: UnaryCancellation::new(),
            start_ts: self.start_ts,
            timeout: self.timeout,
            read_ts: Some(read_ts),
            locking,
        }))
    }

    /// Publishes every staged write of the transaction at its own `start_ts`.
    ///
    /// An empty buffer publishes nothing and takes no commit timestamp, as
    /// Go's `COMMIT` of a transaction that wrote nothing does.
    ///
    /// # Errors
    ///
    /// Returns the client-visible error of any 2PC that did not commit -- the
    /// 9007 of a lost race above all. The coordinator reports a rolled-back
    /// transaction as an `Ok` value carrying the cause, so the outcome is
    /// classified here rather than treated as success.
    pub fn commit(
        self,
        buffer: &MutationBuffer,
    ) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
        // The bound native transaction already owns the staged entries.
        // Commit consumes them directly without a second SQL mutation vector.
        self.commit_with(buffer, Vec::new())
    }

    /// Binds the session's schema lease checker to this transaction's commit
    /// (Go `txn.SetOption(kv.SchemaChecker, domain.NewSchemaChecker(...))`,
    /// `base.go:606-615`). The physical tables the checker is asked about are
    /// taken from the mutations at commit time.
    pub fn set_schema_lease_checker(&mut self, checker: Arc<dyn SchemaLeaseChecker>) {
        self.schema_lease_checker = Some(checker);
    }

    /// Publishes the staged writes together with `extra`, as one transaction at
    /// this transaction's own `start_ts`.
    ///
    /// The two sets are one commit because they are one change: an index
    /// change's meta keys say the index exists and its data keys are what it
    /// contains, and a reader that saw the first without the second would get
    /// the wrong rows with no error. Ordering between the sets is not the
    /// caller's business: the native client derives the complete mutation set
    /// from its MemDB before prewrite.
    ///
    /// # Errors
    ///
    /// Returns the client-visible error of any 2PC that did not commit.
    pub fn commit_with(
        self,
        buffer: &MutationBuffer,
        extra: Vec<BufferMutation>,
    ) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
        // Go commits the same MemDB used by SQL. Transfer an unbound buffer
        // intact, including tombstone flags and assertions; an empty SQL handle
        // attaches to existing native lock metadata without replacing it.
        self.bind_mutation_buffer(buffer);
        // Go invalidates LazyTxn on every commit exit, including read-only
        // transactions and failures. Consume the owner before detaching its
        // SQL handle so no later statement can use a finished transaction.
        let result = self.commit_bound_buffer(buffer, extra);
        buffer.reset();
        result
    }

    fn commit_bound_buffer(
        self,
        buffer: &MutationBuffer,
        extra: Vec<BufferMutation>,
    ) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
        let staged_keys = buffer.staged_keys();
        let schema_lease = schema_lease_for_keys(
            self.schema_lease_checker.clone(),
            staged_keys
                .iter()
                .map(Key::as_bytes)
                .chain(extra.iter().map(BufferMutation::key)),
        );
        if buffer.is_empty() && extra.is_empty() {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            finish_session_transaction(&mut state).map_err(storage_sql_error)?;
            return Ok(None);
        }
        let mutations = extra;
        let state = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::replace(&mut *state, SessionTransactionState::Finished)
        };
        let call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
        let outcome = match state {
            SessionTransactionState::Optimistic(mut transaction) => {
                if let Some(lease) = schema_lease {
                    transaction.set_schema_lease(lease);
                }
                transaction.commit(mutations, &call)
            }
            SessionTransactionState::PessimisticPending {
                transaction,
                opened_at,
                ..
            } => RealPessimisticTransaction::from_transaction(transaction, opened_at).and_then(
                |mut transaction| {
                    if let Some(lease) = schema_lease {
                        transaction.set_schema_lease(lease);
                    }
                    transaction.commit(mutations, &call)
                },
            ),
            SessionTransactionState::Pessimistic {
                mut transaction, ..
            } => {
                if let Some(lease) = schema_lease {
                    transaction.set_schema_lease(lease);
                }
                transaction.commit(mutations, &call)
            }
            SessionTransactionState::Finished => {
                return Err(engine_sql_error(
                    "the transaction is already finished".to_owned(),
                ));
            }
        }
        .map_err(|error| engine_sql_error(error.to_string()))?;
        let duplicate_hint = deferred_duplicate_hint(&outcome, buffer);
        commit_outcome_to_sql_error_with_hint(&outcome, duplicate_hint.as_ref())?;
        Ok(Some(outcome))
    }

    /// Ends the transaction without publishing anything.
    ///
    /// # Errors
    ///
    /// Returns the failure of ending the transaction's own read side.
    pub fn rollback(self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        finish_session_transaction(&mut state).map_err(|error| error.to_string())
    }
}

/// Executes and commits one restricted SQL statement inside the ordinary
/// pessimistic transaction path.
///
/// The statement is rebuilt after Go-equivalent pessimistic lock conflicts at
/// the advanced `for_update_ts`; its transaction `start_ts` remains stable and
/// is supplied separately for version columns. Callers with multiple Go SQL
/// statements must call [`lock_pessimistic_statement`] once per statement and
/// commit their combined mutations only after every statement succeeds.
pub fn commit_pessimistic_statement<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
    T,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    build: impl FnMut(Box<dyn ClusterSnapshot>, u64) -> Result<(T, Vec<BufferMutation>), String>,
) -> Result<T, PessimisticStatementTransactionError> {
    let transaction = SessionTransaction::begin_pessimistic(
        Arc::new(opener.clone()),
        timeout,
        opener.commit_protocol(),
    )
    .map_err(|error| PessimisticStatementTransactionError::Build(error.to_string()))?;
    let staged = MutationBuffer::new();
    let value = lock_pessimistic_statement(&transaction, &staged, build)?;
    transaction
        .commit(&staged)
        .map_err(PessimisticStatementTransactionError::Transaction)?;
    Ok(value)
}

struct MutationOverlaySnapshot {
    snapshot: Box<dyn ClusterSnapshot>,
    staged: MutationBuffer,
}

/// Gives one later statement in a transaction read-your-own-writes over the
/// mutations staged by earlier statements.
pub fn overlay_staged_mutations(
    snapshot: Box<dyn ClusterSnapshot>,
    staged: &MutationBuffer,
) -> Box<dyn ClusterSnapshot> {
    Box::new(MutationOverlaySnapshot::new(snapshot, staged.clone()))
}

impl MutationOverlaySnapshot {
    fn new(snapshot: Box<dyn ClusterSnapshot>, staged: MutationBuffer) -> Self {
        Self { snapshot, staged }
    }
}

impl fmt::Debug for MutationOverlaySnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationOverlaySnapshot")
            .field("start_ts", &self.snapshot.start_ts())
            .finish()
    }
}

impl ClusterSnapshot for MutationOverlaySnapshot {
    fn point_rpc_counts(&mut self) -> (u64, u64) {
        self.snapshot.point_rpc_counts()
    }

    fn prepare(&mut self) -> Result<(), StorageError> {
        self.snapshot.prepare()
    }

    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        match self.staged.get(key) {
            Some(value) => Ok(value),
            None => self.snapshot.get(key),
        }
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        let mut rows = self
            .snapshot
            .scan(start, end, None)?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for (key, value) in self.staged.range(start, end) {
            match value {
                Some(value) => {
                    rows.insert(key.as_bytes().to_vec(), value);
                }
                None => {
                    rows.remove(key.as_bytes());
                }
            }
        }
        Ok(rows.into_iter().take(limit.unwrap_or(usize::MAX)).collect())
    }

    fn start_ts(&self) -> u64 {
        self.snapshot.start_ts()
    }

    fn declare_autocommit_point_get(&mut self) -> bool {
        self.snapshot.declare_autocommit_point_get()
    }
}

/// Locks one Go SQL statement and returns its mutations for the enclosing
/// transaction's eventual commit.
pub fn lock_pessimistic_statement<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
    T,
>(
    transaction: &SessionTransaction<C, L, P>,
    staged: &MutationBuffer,
    build: impl FnMut(Box<dyn ClusterSnapshot>, u64) -> Result<(T, Vec<BufferMutation>), String>,
) -> Result<T, PessimisticStatementTransactionError> {
    transaction.bind_mutation_buffer(staged);
    let result = lock_pessimistic_statement_with(
        transaction.start_ts(),
        |read_ts| {
            match read_ts {
                Some(for_update_ts) => transaction.snapshot_at_for(for_update_ts, true),
                None => transaction.snapshot_for(true),
            }
            .map(|snapshot| overlay_staged_mutations(snapshot, staged))
            .map_err(|error| error.to_string())
        },
        |keys, presume_not_exists, duplicate_hints| {
            transaction
                .lock_keys_with_assertions(
                    keys,
                    presume_not_exists,
                    duplicate_hints,
                    false,
                    LockWaitTime::default(),
                )
                .map_err(|error| error.to_string())
        },
        build,
    );
    transaction
        .finish_statement(result.is_ok())
        .map_err(|e| PessimisticStatementTransactionError::Build(e.to_string()))?;
    let (value, mutations) = result?;
    stage_mutations(staged, mutations)
        .map_err(|error| PessimisticStatementTransactionError::Build(error.to_string()))?;
    Ok(value)
}

/// Shared Go pessimistic statement rebuild loop used by both the concrete
/// TiKV transaction and the server's testable transaction authority.
/// One pessimistic statement's lock observation, Go `LockKeysDetail`'s
/// dashboard counterpart: how many keys the statement locked, how many
/// pessimistic-statement retries it spent, and how long the lock acquisition
/// waited. Go charges these to `StatementLockKeysCount`,
/// `StatementPessimisticRetryCount`, and the client-go
/// `pessimistic_lock_keys_duration` histogram (`adapter.go:580-588`).
#[derive(Clone, Copy, Debug, Default)]
pub struct PessimisticLockObservation {
    /// Go `LockKeysDetail.LockKeys`.
    pub lock_keys: u64,
    /// Go `a.retryCount`.
    pub retries: usize,
    /// Go `LockKeysDetail.TotalTime`.
    pub lock_elapsed: std::time::Duration,
}

thread_local! {
    /// The lock activity of the statement CURRENTLY RUNNING on this worker,
    /// accumulated across every lock RPC the statement makes and drained at
    /// the statement boundary. The generic lock loop knows the numbers; the
    /// dashboard families live one crate up, so the server's statement seam
    /// reads them here.
    static LAST_LOCK_OBSERVATION: std::cell::Cell<PessimisticLockObservation> =
        std::cell::Cell::new(PessimisticLockObservation {
            lock_keys: 0,
            retries: 0,
            lock_elapsed: std::time::Duration::ZERO,
        });
}

/// Reads (and clears) the accumulated lock observation for this worker.
#[must_use]
pub fn take_last_pessimistic_lock_observation() -> Option<PessimisticLockObservation> {
    LAST_LOCK_OBSERVATION.with(|slot| {
        let observation = slot.replace(PessimisticLockObservation::default());
        (observation.lock_keys > 0 || observation.retries > 0).then_some(observation)
    })
}

fn accumulate_lock_observation(keys: u64, elapsed: std::time::Duration) {
    LAST_LOCK_OBSERVATION.with(|slot| {
        let mut observation = slot.get();
        observation.lock_keys += keys;
        observation.lock_elapsed += elapsed;
        slot.set(observation);
    });
}

fn record_lock_retries(retries: usize) {
    LAST_LOCK_OBSERVATION.with(|slot| {
        let mut observation = slot.get();
        observation.retries = retries;
        slot.set(observation);
    });
}

pub fn lock_pessimistic_statement_with<T>(
    start_ts: u64,
    mut snapshot: impl FnMut(Option<u64>) -> Result<Box<dyn ClusterSnapshot>, String>,
    mut lock: impl FnMut(
        Vec<Vec<u8>>,
        BTreeSet<Vec<u8>>,
        BTreeMap<Vec<u8>, DuplicateKeyHint>,
    ) -> Result<LockKeysOutcome, String>,
    mut build: impl FnMut(Box<dyn ClusterSnapshot>, u64) -> Result<(T, Vec<BufferMutation>), String>,
) -> Result<(T, Vec<BufferMutation>), PessimisticStatementTransactionError> {
    let mut retry_read_ts = None;
    let mut retries = PessimisticStatementRetry::default();
    loop {
        let snapshot =
            snapshot(retry_read_ts).map_err(PessimisticStatementTransactionError::Build)?;
        let (value, mutations) =
            build(snapshot, start_ts).map_err(PessimisticStatementTransactionError::Build)?;
        let mut keys = mutations
            .iter()
            .filter(|mutation| mutation.kind() == tidb_txnkv::transaction::BufferMutationOp::Lock)
            .map(|mutation| mutation.key().to_vec())
            .chain(mutations.iter().filter_map(|mutation| {
                let value = match mutation.kind() {
                    tidb_txnkv::transaction::BufferMutationOp::Delete => None,
                    tidb_txnkv::transaction::BufferMutationOp::Lock => return None,
                    _ => Some(mutation.value()),
                };
                let flags = if mutation.presume_not_exists() {
                    tikv_client::kv::apply_flags_ops(
                        Default::default(),
                        &[tikv_client::kv::FlagsOp::SetPresumeKeyNotExists],
                    )
                } else {
                    Default::default()
                };
                tidb_executor::cluster_storage::key_needs_pessimistic_lock(
                    mutation.key(),
                    value.unwrap_or_default(),
                    flags,
                )
                .then(|| mutation.key().to_vec())
            }))
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        if keys.is_empty() {
            return Ok((value, mutations));
        }
        let presume_not_exists = mutations
            .iter()
            .filter(|mutation| mutation.presume_not_exists())
            .map(|mutation| mutation.key().to_vec())
            .filter(|key| keys.binary_search(key).is_ok())
            .collect::<BTreeSet<_>>();
        let duplicate_hints = BTreeMap::new();
        match lock(keys, presume_not_exists, duplicate_hints)
            .map_err(PessimisticStatementTransactionError::Build)?
        {
            LockKeysOutcome::Locked { .. } => {
                record_lock_retries(retries.count());
                return Ok((value, mutations));
            }
            LockKeysOutcome::RetryStatement { for_update_ts, .. } => {
                retries
                    .retry()
                    .map_err(PessimisticStatementTransactionError::Transaction)?;
                retry_read_ts = Some(for_update_ts);
            }
            LockKeysOutcome::StatementError(error) | LockKeysOutcome::TransactionError(error) => {
                return Err(PessimisticStatementTransactionError::Transaction(error));
            }
        }
    }
}

fn finish_session_transaction<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    state: &mut SessionTransactionState<C, L, P>,
) -> Result<(), StorageError> {
    match std::mem::replace(state, SessionTransactionState::Finished) {
        SessionTransactionState::Optimistic(transaction) => transaction
            .finish_without_writes()
            .map(|_| ())
            .map_err(|error| StorageError::Backend(error.to_string())),
        SessionTransactionState::PessimisticPending { transaction, .. } => transaction
            .finish_without_writes()
            .map(|_| ())
            .map_err(|error| StorageError::Backend(error.to_string())),
        SessionTransactionState::Pessimistic { transaction, .. } => {
            let call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
            transaction
                .rollback(&call)
                .map(|_| ())
                .map_err(|error| StorageError::Backend(error.to_string()))
        }
        SessionTransactionState::Finished => Ok(()),
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> Drop
    for SessionTransaction<C, L, P>
{
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = finish_session_transaction(&mut state);
    }
}

/// One statement's view of an open session transaction.
///
/// It carries no ownership of the transaction: dropping it is the end of the
/// statement, and the transaction stays open for the next one.
struct SessionSnapshot<C, L, P: StorePdCapability> {
    state: Arc<Mutex<SessionTransactionState<C, L, P>>>,
    /// Reused by all reads in this statement.  The transport does not cancel
    /// this carrier on an ordinary deadline, so sharing it avoids allocating
    /// one watch channel/condvar pair per point Get.
    cancellation: UnaryCancellation,
    /// The timestamp the transaction opened at, which every statement of it
    /// reads at; a remote scan has to name it.
    start_ts: u64,
    timeout: Duration,
    /// `Some` overrides the read timestamp for this statement -- the
    /// pessimistic retry's advanced `for_update_ts`. `None` reads at
    /// `start_ts`.
    read_ts: Option<u64>,
    /// Whether this statement may use rows returned with pessimistic locks.
    locking: bool,
}

impl<C, L, P: StorePdCapability> fmt::Debug for SessionSnapshot<C, L, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionSnapshot")
            .field("start_ts", &self.start_ts)
            .finish()
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> ClusterSnapshot
    for SessionSnapshot<C, L, P>
{
    fn point_rpc_counts(&mut self) -> (u64, u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            SessionTransactionState::Optimistic(transaction)
            | SessionTransactionState::PessimisticPending { transaction, .. } => {
                transaction.snapshot_point_rpc_counts()
            }
            SessionTransactionState::Pessimistic { transaction, .. } => {
                transaction.snapshot().snapshot_point_rpc_counts()
            }
            SessionTransactionState::Finished => (0, 0),
        }
    }

    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        // `snapshot_get_at` owns the request bytes before it crosses the
        // BatchCommands boundary. Keep the borrowed key through dispatch so
        // a point read does not allocate a copy merely for lock-cache lookup.
        let bytes = key.as_bytes();
        let read_ts = self.read_ts.unwrap_or(self.start_ts);
        let call = UnaryCallContext::with_deadline(
            Instant::now() + self.timeout,
            self.cancellation.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            SessionTransactionState::Optimistic(transaction) => transaction
                .snapshot_get_at(&bytes, read_ts, &call)
                .map(|result| result.value)
                .map_err(classify),
            SessionTransactionState::PessimisticPending { transaction, .. } => transaction
                .snapshot_get_at(&bytes, read_ts, &call)
                .map(|result| result.value)
                .map_err(classify),
            SessionTransactionState::Pessimistic {
                transaction,
                lock_values,
                ..
            } => {
                if self.locking {
                    if let Some(cached) = lock_values.get(bytes) {
                        return Ok(cached.clone());
                    }
                }
                transaction
                    .snapshot()
                    .snapshot_get_at(&bytes, read_ts, &call)
                    .map(|result| result.value)
                    .map_err(classify)
            }
            SessionTransactionState::Finished => Err(StorageError::Backend(
                "the transaction is already finished".to_owned(),
            )),
        }
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        let keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let read_ts = self.read_ts.unwrap_or(self.start_ts);
        let call = UnaryCallContext::with_deadline(
            Instant::now() + self.timeout,
            self.cancellation.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            SessionTransactionState::Optimistic(transaction) => transaction
                .snapshot_batch_get_at(&keys, read_ts, &call)
                .map_err(classify),
            SessionTransactionState::PessimisticPending { transaction, .. } => transaction
                .snapshot_batch_get_at(&keys, read_ts, &call)
                .map_err(classify),
            SessionTransactionState::Pessimistic {
                transaction,
                lock_values,
                ..
            } => {
                let mut answered = Vec::new();
                let mut uncached = Vec::with_capacity(keys.len());
                for key in keys {
                    match lock_values.get(&key).filter(|_| self.locking) {
                        Some(Some(value)) => answered.push((key, value.clone())),
                        Some(None) => {}
                        None => uncached.push(key),
                    }
                }
                if uncached.is_empty() {
                    return Ok(answered);
                }
                transaction
                    .snapshot()
                    .snapshot_batch_get_at(&uncached, read_ts, &call)
                    .map(|mut pairs| {
                        pairs.extend(answered);
                        pairs
                    })
                    .map_err(classify)
            }
            SessionTransactionState::Finished => Err(StorageError::Backend(
                "the transaction is already finished".to_owned(),
            )),
        }
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        let read_ts = self.read_ts.unwrap_or(self.start_ts);
        let call = UnaryCallContext::with_deadline(
            Instant::now() + self.timeout,
            self.cancellation.clone(),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            SessionTransactionState::Optimistic(transaction) => transaction
                .snapshot_scan_at(start.as_bytes(), end.as_bytes(), limit, read_ts, &call)
                .map_err(classify),
            SessionTransactionState::PessimisticPending { transaction, .. } => transaction
                .snapshot_scan_at(start.as_bytes(), end.as_bytes(), limit, read_ts, &call)
                .map_err(classify),
            SessionTransactionState::Pessimistic { transaction, .. } => transaction
                .snapshot()
                .snapshot_scan_at(start.as_bytes(), end.as_bytes(), limit, read_ts, &call)
                .map_err(classify),
            SessionTransactionState::Finished => Err(StorageError::Backend(
                "the transaction is already finished".to_owned(),
            )),
        }
    }

    fn start_ts(&self) -> u64 {
        // The timestamp this snapshot READS at, which is the statement's when
        // it has one. Both callers stamp it into a request that names an MVCC
        // version -- `PushdownScanRequest::snapshot_ts`
        // (`tidb-executor/src/cluster_storage.rs`) -- so answering the
        // transaction's `start_ts` while the statement's point reads use its
        // advanced `for_update_ts` would read one statement at two
        // timestamps. Go re-executes a retried pessimistic statement wholly
        // at `forUpdateTS` (`handlePessimisticDML` -> `UpdateForUpdateTS`),
        // pushdown included.
        self.read_ts.unwrap_or(self.start_ts)
    }
}

/// Maps a coordinator failure onto the seam's error kinds.
///
/// Registered exhausted retry budgets keep their terminal SQL identity. The
/// remaining untyped diagnostics retain the existing statement-retry heuristic;
/// their text must not override a registered storage error.
fn classify(error: OptimisticCoordinatorError) -> StorageError {
    if matches!(
        error,
        OptimisticCoordinatorError::SnapshotBackoff { .. } | OptimisticCoordinatorError::Storage(_)
    ) {
        let sql = coordinator_sql_error(error);
        return StorageError::Sql(tidb_executor::MysqlError::new(sql.code, sql.message));
    }
    let message = error.to_string();
    let lowered = message.go_to_lower();
    let retryable = [
        "region", "epoch", "lock", "leader", "stale", "budget", "deadline",
    ]
    .iter()
    .any(|cause| lowered.contains(cause));
    if retryable {
        StorageError::Retryable(message)
    } else {
        StorageError::Backend(message)
    }
}

/// Builds one statement's cluster storage: a fresh snapshot in front of the
/// session's staged writes.
///
/// The returned handle is the statement's; finishing it ends the read
/// transaction. The buffer outlives it, because the session -- not the
/// statement -- owns the staged writes until COMMIT.
pub fn statement_storage<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    buffer: MutationBuffer,
    timeout: Duration,
) -> Result<(ClusterTableStorage, Arc<Mutex<StatementSnapshot<C, L, P>>>), OptimisticCoordinatorError>
{
    let snapshot = Arc::new(Mutex::new(StatementSnapshot::open(opener, timeout)?));
    let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
    Ok((ClusterTableStorage::new(buffer, handle), snapshot))
}

/// Finds the table/index text retained for client-go ErrKeyExist. Assertion
/// failures remain consistency errors even when the same key has a hint.
fn deferred_duplicate_hint(
    outcome: &OptimisticCommitOutcome,
    buffer: &MutationBuffer,
) -> Option<DuplicateKeyHint> {
    let key = match outcome {
        OptimisticCommitOutcome::RolledBack(result) => &result.cause,
        OptimisticCommitOutcome::CleanupFailed(result) => &result.cause,
        _ => return None,
    };
    match key {
        TransactionCause::AlreadyExists { key, .. } => buffer.duplicate_key_hint_for(key),
        _ => None,
    }
}

/// Publishes every staged write of one autocommit statement as its own
/// optimistic transaction, **at the timestamp the statement read at**.
///
/// `read_ts` is the whole correctness content of this function. Go's implicit
/// per-statement transaction spends ONE timestamp and both reads and prewrites
/// at it (`pkg/sessiontxn/isolation/optimistic.go:45-46` ->
/// `base.go:268` -> client-go `2pc.go:474` -> `prewrite.go:174`), which is what
/// makes TiKV's conflict check — a key's latest `commit_ts` against the
/// prewriting transaction's `start_ts` — sufficient. Publishing at a fresh,
/// later timestamp instead makes a commit that landed between the read and the
/// write invisible to that check, and the value computed from the stale read
/// overwrites it with no error and no warning. That was a real, measured
/// lost-update on this path.
///
/// `None` means the statement never read a cluster row, so there is no
/// timestamp to publish at and nothing a racing commit could have invalidated;
/// a fresh one is then both necessary and correct. It is NOT a fallback for a
/// statement that did read.
///
/// Inside `BEGIN` ... `COMMIT` the publication goes through
/// [`SessionTransaction::commit`] instead, at the timestamp `BEGIN` took. An
/// empty buffer commits nothing and consumes no timestamp.
pub fn commit_staged_buffer<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    buffer: &MutationBuffer,
    read_ts: Option<u64>,
    timeout: Duration,
    commit_protocol: CommitProtocol,
    schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
    if buffer.is_empty() {
        return Ok(None);
    }
    let lease = schema_lease_for_keys(
        schema_lease_checker,
        buffer.staged_keys().iter().map(Key::as_bytes),
    );
    let transaction = match read_ts {
        Some(start_ts) => opener.begin_at(start_ts),
        None => opener.begin(),
    }
    .map_err(coordinator_sql_error)?;
    let mut transaction = transaction;
    *transaction.mem_buffer() = buffer.take_native_buffer();
    // Go's autocommit committer checks `@@tidb_enable_async_commit` /
    // `@@tidb_enable_1pc` at execute time (`checkAsyncCommit` / `checkOnePC`);
    // the same eligibility decision then runs at commit.
    transaction.set_commit_protocol(commit_protocol);
    if let Some(lease) = lease {
        transaction.set_schema_lease(lease);
    }
    let call = UnaryCallContext::with_timeout(timeout.max(TRANSACTION_END_TIMEOUT));
    let outcome = transaction
        .commit(Vec::new(), &call)
        .map_err(coordinator_sql_error)?;
    let duplicate_hint = deferred_duplicate_hint(&outcome, buffer);
    commit_outcome_to_sql_error_with_hint(&outcome, duplicate_hint.as_ref())?;
    buffer.reset();
    Ok(Some(outcome))
}

/// Go `SetOptionsBeforeCommit` (`base.go:585-596`): the physical table (or
/// partition) IDs a commit writes, which is what the schema lease check is
/// asked about. Go reads them off `TxnCtx.TableDeltaMap`, filled per DML
/// statement with the physical ID of every table written; here the same
/// set is the table prefix of every mutation key, which is by construction
/// the physical ID. A key outside the table space (`decode_table_id` answers
/// `0`) is not a table and is skipped, as Go's map never held it.
pub fn physical_table_ids(mutations: &[BufferMutation]) -> Vec<i64> {
    let mut ids: Vec<i64> = mutations
        .iter()
        .map(|mutation| tidb_codec::decode_table_id(mutation.key()))
        .filter(|id| *id > 0)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Builds the same schema-lease table set from a native MemDB without
/// cloning its row/index values.
fn schema_lease_for_keys<'a>(
    checker: Option<Arc<dyn SchemaLeaseChecker>>,
    keys: impl Iterator<Item = &'a [u8]>,
) -> Option<SchemaLease> {
    checker.map(|checker| {
        let mut related_physical_table_ids: Vec<i64> = keys
            .map(tidb_codec::decode_table_id)
            .filter(|id| *id > 0)
            .collect();
        related_physical_table_ids.sort_unstable();
        related_physical_table_ids.dedup();
        SchemaLease {
            checker,
            related_physical_table_ids,
        }
    })
}

/// Preserve registered snapshot failures; other failures before a commit
/// verdict retain the existing generic transaction diagnostic.
pub(crate) fn coordinator_sql_error(error: OptimisticCoordinatorError) -> LockSqlError {
    match error {
        OptimisticCoordinatorError::Storage(error) => LockSqlError {
            code: error.mysql_code().as_u16(),
            state: *b"HY000",
            message: error.to_string(),
        },
        OptimisticCoordinatorError::SnapshotBackoff { kind, detail } => {
            transaction_cause_to_sql_error(&TransactionCause::BackoffExhausted { kind, detail })
        }
        other => engine_sql_error(other.to_string()),
    }
}

fn storage_sql_error(error: StorageError) -> LockSqlError {
    match error {
        StorageError::Sql(error) => LockSqlError {
            code: error.code,
            state: error.state,
            message: error.message,
        },
        other => engine_sql_error(other.to_string()),
    }
}

fn engine_sql_error(detail: impl fmt::Display) -> LockSqlError {
    LockSqlError {
        code: 1105,
        state: *b"HY000",
        message: format!("[kv:1105]transaction failed: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coprocessor_snapshot_timestamp_follows_the_statement_retry() {
        let mut snapshot = SessionSnapshot::<TonicCoprocessorClient, PdRegionLoader, PdClient> {
            state: Arc::new(Mutex::new(SessionTransactionState::Finished)),
            cancellation: UnaryCancellation::new(),
            start_ts: 10,
            timeout: Duration::from_secs(1),
            read_ts: None,
            locking: false,
        };
        assert_eq!(ClusterSnapshot::start_ts(&snapshot), 10);
        snapshot.read_ts = Some(20);
        assert_eq!(ClusterSnapshot::start_ts(&snapshot), 20);
        assert_eq!(
            snapshot.start_ts, 10,
            "transaction identity remains unchanged"
        );
    }

    #[derive(Debug)]
    struct MapSnapshot(BTreeMap<Vec<u8>, Vec<u8>>);

    impl ClusterSnapshot for MapSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.0.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .0
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    #[test]
    fn later_restricted_statements_read_the_transactions_staged_writes() {
        let staged = MutationBuffer::new();
        staged
            .set(Key::from_bytes(b"b".to_vec()), b"new".to_vec())
            .unwrap();
        staged.delete(Key::from_bytes(b"c".to_vec())).unwrap();
        let mut snapshot = MutationOverlaySnapshot::new(
            Box::new(MapSnapshot(BTreeMap::from([
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"old".to_vec()),
                (b"c".to_vec(), b"gone".to_vec()),
            ]))),
            staged,
        );

        assert_eq!(
            snapshot
                .get(&Key::from_bytes(b"b".to_vec()))
                .expect("overlay get"),
            Some(b"new".to_vec())
        );
        assert_eq!(
            snapshot
                .scan(
                    &Key::from_bytes(b"a".to_vec()),
                    &Key::from_bytes(b"d".to_vec()),
                    None,
                )
                .expect("overlay scan"),
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"new".to_vec()),
            ]
        );
    }

    #[test]
    /// A finished statement snapshot refuses further reads, and finishing it
    /// twice is a no-op.
    ///
    /// Owning the transaction inline means the lifecycle is enforced by the
    /// snapshot itself.
    fn a_finished_statement_snapshot_refuses_reads_and_finishes_once() {
        let mut snapshot: StatementSnapshot = StatementSnapshot {
            transaction: None,
            start_ts: 42,
            timeout: Duration::from_secs(1),
            cancellation: UnaryCancellation::new(),
        };
        assert_eq!(snapshot.start_ts(), 42);
        assert!(
            snapshot.finish().is_ok(),
            "finishing an already-finished snapshot is a no-op, not an error"
        );
        let refused = snapshot.get(&Key::from_bytes(b"k".to_vec()));
        assert!(
            matches!(refused, Err(StorageError::Backend(ref message))
                if message.contains("already finished")),
            "a read after finish must be refused: {refused:?}"
        );
    }

    #[test]
    fn snapshot_backoff_errors_keep_their_registered_sql_identity() {
        for (kind, code, message) in [
            (
                tidb_txnkv::region::RegionBackoffKind::TxnLockFast,
                9004,
                "[tikv:9004]Resolve lock timeout",
            ),
            (
                tidb_txnkv::region::RegionBackoffKind::RegionMiss,
                9005,
                "[tikv:9005]Region is unavailable",
            ),
        ] {
            let error = OptimisticCoordinatorError::SnapshotBackoff {
                kind,
                detail: "exhausted".to_owned(),
            };
            let storage = classify(error.clone());
            assert!(
                matches!(&storage, StorageError::Sql(e) if e.code == code),
                "{storage:?}"
            );
            for sql in [coordinator_sql_error(error), storage_sql_error(storage)] {
                assert_eq!(sql.code, code);
                assert_eq!(sql.state, *b"HY000");
                assert_eq!(sql.message, message);
            }
        }
    }

    #[test]
    fn topology_and_lock_causes_are_retryable() {
        assert!(matches!(
            classify(OptimisticCoordinatorError::SnapshotGet(
                "region epoch is stale".to_owned()
            )),
            StorageError::Retryable(_)
        ));
        assert!(matches!(
            classify(OptimisticCoordinatorError::SnapshotGet(
                "snapshot lock retry budget exhausted".to_owned()
            )),
            StorageError::Retryable(_)
        ));
        assert!(matches!(
            classify(OptimisticCoordinatorError::ZeroClusterId),
            StorageError::Backend(_)
        ));
        assert!(matches!(
            classify(OptimisticCoordinatorError::SnapshotGet(
                "encoded key is empty".to_owned()
            )),
            StorageError::Backend(_)
        ));
    }

    /// A snapshot serving a RETRIED pessimistic statement reports the
    /// statement's timestamp, not the transaction's.
    ///
    /// `ClusterSnapshot::start_ts` is stamped into
    /// `PushdownScanRequest::snapshot_ts`, which names the MVCC version the
    /// coprocessor reads. A retried statement's point reads already use the
    /// advanced `for_update_ts` (`SessionTransaction::snapshot_at`), so
    /// answering `start_ts` here would read ONE statement at TWO timestamps
    /// and recompute from the row the statement just lost the lock race on.
    /// Go re-executes the whole retried statement at `forUpdateTS`
    /// (`handlePessimisticDML` -> `UpdateForUpdateTS`).
    #[test]
    fn a_statement_snapshot_reports_the_timestamp_it_reads_at() {
        let state = Arc::new(Mutex::new(
            SessionTransactionState::<TonicCoprocessorClient, PdRegionLoader, PdClient>::Finished,
        ));
        let at_transaction = SessionSnapshot {
            state: Arc::clone(&state),
            cancellation: UnaryCancellation::new(),
            start_ts: 100,
            timeout: Duration::from_secs(1),
            read_ts: None,
            locking: false,
        };
        assert_eq!(
            at_transaction.start_ts(),
            100,
            "an ordinary statement reads at the transaction's own timestamp"
        );

        let retried = SessionSnapshot {
            state,
            cancellation: UnaryCancellation::new(),
            start_ts: 100,
            timeout: Duration::from_secs(1),
            read_ts: Some(200),
            locking: false,
        };
        assert_eq!(
            retried.start_ts(),
            200,
            "a retried statement reads at its advanced for_update_ts, and \
             every read of it must agree -- pushdown included"
        );
    }
}
