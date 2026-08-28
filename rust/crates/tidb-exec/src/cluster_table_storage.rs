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
use tidb_pd_client::PdClient;
use tidb_txnkv::pd_capability::{CapabilityTimestampSource, TimestampFutureWait};
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    CommitProtocol, LockKeepAlive, LockWaitTime, OptimisticCommitOutcome,
    OptimisticCoordinatorError, OptimisticMutation, PessimisticLockFailure,
    RealOptimisticTransaction, RealOptimisticTransactionOpener, RealPessimisticTransaction,
    StorePdCapability, StoreWriteClient, StoreWriteLoader, TransactionCause,
    MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::Key;
use tidb_txnkv::PdRegionLoader;

use crate::pessimistic_lock_error::{
    commit_outcome_to_sql_error_with_hint, duplicate_key_sql_error, is_retryable_statement_failure,
    lock_failure_to_sql_error, transaction_cause_to_sql_error, LockSqlError,
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

/// The keys one statement's staged writes owe pessimistic locks -- Go
/// `LazyTxn.KeysNeedToLock` over the statement's staging delta, filtered by
/// `KeyNeedToLock` (`pkg/session/txn.go`).
///
/// The delta is every buffer entry `after` holds that `before` did not, or
/// holds with a different value: exactly what this statement staged. The
/// filter is Go's, reduced to the flags this buffer carries (none):
///
/// * a non-table key always locks (Go's meta arm);
/// * a DELETE locks only RECORD keys (`len(v) == 0` ->
///   `tablecodec.IsRecordKey`);
/// * a record-key put locks;
/// * an index-key put locks only when the entry is UNIQUE, decided by the
///   SAME ported classifier Go reads (`tablecodec.IndexKVIsUnique` ->
///   [`tidb_tablecodec::index_kv_is_unique`]) -- a value-length
///   shortcut is NOT equivalent, because this tier's own writer already
///   emits multi-byte NON-unique entries (restored collation data, the v1
///   versioned encoding), which a length test would over-lock.
#[must_use]
pub fn pessimistic_lock_delta(
    before: &[(Key, Option<Vec<u8>>)],
    after: &[(Key, Option<Vec<u8>>)],
) -> Vec<Vec<u8>> {
    use std::collections::BTreeMap;
    let before: BTreeMap<&Key, &Option<Vec<u8>>> =
        before.iter().map(|(key, value)| (key, value)).collect();
    after
        .iter()
        .filter(|(key, value)| before.get(key) != Some(&value))
        .filter(|(key, value)| statement_key_needs_lock(key.as_bytes(), value.as_deref()))
        .map(|(key, _)| key.as_bytes().to_vec())
        .collect()
}

/// Go `KeyNeedToLock` (`pkg/session/txn.go`), reduced as
/// [`pessimistic_lock_delta`]'s doc describes, over the same ported
/// classifiers Go reads (`tablecodec.IsRecordKey` / `IsIndexKey` /
/// `IndexKVIsUnique`).
fn statement_key_needs_lock(key: &[u8], value: Option<&[u8]>) -> bool {
    if !tidb_tablecodec::is_record_key(key) && !tidb_tablecodec::is_index_key(key) {
        // Go's "meta key always need to lock".
        return true;
    }
    match value {
        None => tidb_tablecodec::is_record_key(key),
        Some(_) if tidb_tablecodec::is_record_key(key) => true,
        Some(value) => tidb_tablecodec::index_kv_is_unique(value),
    }
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
    opener: &Arc<RealOptimisticTransactionOpener<C, L, P>>,
    keep_alive: &mut Option<LockKeepAlive>,
    lock_values: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    keys: &[Vec<u8>],
    presume_not_exists: &BTreeSet<Vec<u8>>,
    duplicate_hints: &BTreeMap<Vec<u8>, DuplicateKeyHint>,
    return_values: bool,
    call: &UnaryCallContext,
) -> LockKeysOutcome {
    let held: BTreeSet<Vec<u8>> = transaction.locked_keys().into_iter().collect();
    // Go `KVTxn.LockKeys` filters keys this transaction already holds BEFORE
    // any RPC (client-go `kv.go`: a key already in `txn.locks` is reported as
    // `AlreadyLocked`, never re-sent): the lock pins the key against every
    // other writer, so re-acquiring it could discover nothing new and only
    // spends a round trip. A held key absent from the value cache (locked
    // earlier by a path that asked for no rows -- a locking read) simply has
    // no cached image: the statement's own read falls through to storage,
    // exactly Go's `getValueFromLockCtx` AlreadyLocked arm.
    let added: Vec<Vec<u8>> = keys
        .iter()
        .filter(|key| !held.contains(*key))
        .cloned()
        .collect();
    if added.is_empty() {
        return LockKeysOutcome::Locked {
            for_update_ts: transaction.for_update_ts(),
            newly_locked: Vec::new(),
        };
    }
    let presume_not_exists: BTreeSet<Vec<u8>> = presume_not_exists
        .iter()
        .filter(|key| !held.contains(*key))
        .cloned()
        .collect();
    /// Bound on deadlock-retryable re-acquisitions, the narrow driver's own.
    const MAX_LOCK_RETRIES: usize = 8;
    let mut attempt = 0usize;
    loop {
        // Go's `getPessimisticLazyCheckMode` selects whether a lazy INSERT's
        // NotExist assertion lands here (`DupKeyCheckInAcquireLock`) or is
        // retained for prewrite (`DupKeyCheckInPrewrite`).
        match if return_values {
            transaction.acquire_locks_returning_values(
                &added,
                &presume_not_exists,
                LockWaitTime::session_lock_wait_timeout(),
                call,
            )
        } else {
            transaction.acquire_locks(
                &added,
                &presume_not_exists,
                LockWaitTime::session_lock_wait_timeout(),
                call,
            )
        } {
            Ok(acquired) => {
                // Rows that rode back with the locks enter the cache now:
                // both exits below KEEP these locks (a clean grant, or fair
                // locking's grant-despite-conflict), so the images stay valid
                // either way. Conflict-granted keys answer no value — Go
                // recomputes such a statement from a newer snapshot, and so
                // does this one.
                lock_values.extend(
                    acquired
                        .values
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                );
                if keep_alive.is_none() {
                    match opener
                        .start_lock_keep_alive(acquired.primary_key.clone(), transaction.start_ts())
                    {
                        Ok(alive) => *keep_alive = Some(alive),
                        Err(error) => {
                            return LockKeysOutcome::TransactionError(LockSqlError {
                                code: 1105,
                                state: *b"HY000",
                                message: format!(
                                    "cannot keep the transaction's primary lock alive: {error}"
                                ),
                            });
                        }
                    }
                }
                if acquired.locked_with_conflict.is_empty() {
                    return LockKeysOutcome::Locked {
                        for_update_ts: acquired.for_update_ts,
                        newly_locked: added,
                    };
                }
                // Fair locking granted the locks despite a newer committed
                // version. The locks STAY -- that is the point -- but the
                // statement must be recomputed at a timestamp that sees it.
                return match transaction.advance_for_update_ts() {
                    Ok(for_update_ts) => LockKeysOutcome::RetryStatement {
                        for_update_ts,
                        newly_locked: added,
                    },
                    Err(failure) => {
                        LockKeysOutcome::TransactionError(lock_failure_to_sql_error(&failure))
                    }
                };
            }
            Err(failure) => {
                if let PessimisticLockFailure::Deadlock(detail) = &failure {
                    tidb_executor::deadlock_history::record_deadlock(detail);
                }
                // Release only what this statement added; earlier statements'
                // locks survive their successor's failure. The release runs on
                // the store's deadline, not this statement's: a lock attempt
                // that failed by TIMEOUT leaves `call` at zero, and cleaning up
                // on a spent context turned every such statement-scoped lock
                // failure into a transaction abort ("PessimisticRollback
                // failed: ... timed out after 0ms") under a multi-threaded
                // workload.
                let cleanup_call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
                if let Err(cause) = transaction.pessimistic_rollback(&added, &cleanup_call) {
                    return LockKeysOutcome::TransactionError(transaction_cause_to_sql_error(
                        &cause,
                    ));
                }
                if let PessimisticLockFailure::Transaction(TransactionCause::AlreadyExists {
                    key,
                    ..
                }) = &failure
                {
                    if let Some(hint) = duplicate_hints.get(key) {
                        // Go reports this assertion as a statement error: the
                        // INSERT is rolled back, while the explicit
                        // pessimistic transaction stays usable.
                        return LockKeysOutcome::StatementError(duplicate_key_sql_error(hint));
                    }
                }
                if !is_retryable_statement_failure(&failure) {
                    let error = lock_failure_to_sql_error(&failure);
                    return if failure.is_statement_scoped() {
                        LockKeysOutcome::StatementError(error)
                    } else {
                        LockKeysOutcome::TransactionError(error)
                    };
                }
                match &failure {
                    PessimisticLockFailure::Deadlock(detail) if detail.is_retryable => {
                        if attempt >= MAX_LOCK_RETRIES {
                            return LockKeysOutcome::StatementError(lock_failure_to_sql_error(
                                &failure,
                            ));
                        }
                        std::thread::sleep(Duration::from_millis(5));
                        if let Err(advance) = transaction.advance_for_update_ts() {
                            return LockKeysOutcome::TransactionError(lock_failure_to_sql_error(
                                &advance,
                            ));
                        }
                        attempt += 1;
                        continue;
                    }
                    // A write conflict's remedy is the statement retry at a
                    // newer `for_update_ts`. This arm rolled its additions
                    // back above, so the retry carries no new locks.
                    _ => {
                        return match transaction.advance_for_update_ts() {
                            Ok(for_update_ts) => LockKeysOutcome::RetryStatement {
                                for_update_ts,
                                newly_locked: Vec::new(),
                            },
                            Err(advance) => LockKeysOutcome::TransactionError(
                                lock_failure_to_sql_error(&advance),
                            ),
                        };
                    }
                }
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
        UnaryCallContext::with_timeout(self.timeout)
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
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        let call = self.call();
        let read_ts = self.start_ts;
        let bytes = key.as_bytes().to_vec();
        self.reader()?
            .snapshot_get_at(&bytes, read_ts, &call)
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
        let start = start.as_bytes().to_vec();
        let end = end.as_bytes().to_vec();
        self.reader()?
            .snapshot_scan_at(&start, &end, limit, read_ts, &call)
            .map_err(classify)
    }

    fn start_ts(&self) -> u64 {
        self.start_ts
    }
}

/// One direct latest-committed point read with no transaction-worker state.
///
/// The session creates this only after the root plan has declared Go's
/// autocommit point-get shape. A second read would not have snapshot isolation
/// at `u64::MAX`, so this handle consumes exactly one Get and refuses every
/// scan or later Get. Each accepted read opens only a thread-local read runtime;
/// region retries, lock recovery, GC visibility, and the call deadline remain
/// those of the transaction snapshot reader.
pub struct MaxTsSnapshot<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient> {
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
    consumed: bool,
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> MaxTsSnapshot<C, L, P> {
    /// Binds the direct read to the process transaction/read authority.
    #[must_use]
    pub fn new(opener: Arc<RealOptimisticTransactionOpener<C, L, P>>, timeout: Duration) -> Self {
        Self {
            opener,
            timeout,
            consumed: false,
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
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.consume()?;
        let call = UnaryCallContext::with_timeout(self.timeout);
        self.opener
            .snapshot_get_at_max_ts(key.as_bytes(), &call)
            .map_err(classify)
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
        // opening an ordinary timestamped transaction for every YCSB E operation.
        let call = UnaryCallContext::with_timeout(self.timeout);
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
    /// Whether this is a pessimistic transaction -- decided by the
    /// session's `tidb_txn_mode` at `BEGIN`, Go's `DefTiDBTxnMode`
    /// (pessimistic) being the default.
    pessimistic: bool,
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
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        opened_at: Instant,
    },
    Pessimistic {
        transaction: RealPessimisticTransaction<C, L, CapabilityTimestampSource<P>>,
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        keep_alive: Option<LockKeepAlive>,
        lock_values: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    },
    Finished,
}

/// Crosses Go's lazy pessimistic boundary exactly once, immediately before
/// the first statement that actually needs row locks.
fn promote_pessimistic_state<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    state: &mut SessionTransactionState<C, L, P>,
) -> Result<(), StorageError> {
    if !matches!(state, SessionTransactionState::PessimisticPending { .. }) {
        return Ok(());
    }
    let pending = std::mem::replace(state, SessionTransactionState::Finished);
    let SessionTransactionState::PessimisticPending {
        transaction,
        opener,
        opened_at,
    } = pending
    else {
        unreachable!("the lazy pessimistic state was checked before promotion")
    };
    let transaction = RealPessimisticTransaction::from_transaction(transaction, opened_at)
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    *state = SessionTransactionState::Pessimistic {
        transaction,
        opener,
        keep_alive: None,
        lock_values: BTreeMap::new(),
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
    /// The publication budget is the transaction-size limit itself, because a
    /// multi-statement transaction cannot know its mutation set at `BEGIN`; the
    /// commit still enforces the same limits against the buffer it publishes.
    pub fn begin(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
        commit_protocol: CommitProtocol,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let mut transaction =
            opener.begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)?;
        transaction.set_commit_protocol(commit_protocol);
        let start_ts = transaction.start_ts();
        Ok(Self {
            state: Arc::new(Mutex::new(SessionTransactionState::Optimistic(transaction))),
            start_ts,
            timeout,
            pessimistic: false,
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
        let mut transaction =
            opener.begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)?;
        transaction.set_commit_protocol(commit_protocol);
        let start_ts = transaction.start_ts();
        Ok(Self {
            state: Arc::new(Mutex::new(SessionTransactionState::PessimisticPending {
                transaction,
                opener,
                opened_at,
            })),
            start_ts,
            timeout,
            pessimistic: true,
        })
    }

    /// Whether this transaction locks per statement and commits with
    /// pessimistic constraints.
    #[must_use]
    pub const fn is_pessimistic(&self) -> bool {
        self.pessimistic
    }

    /// Acquires pessimistic locks on one statement's written keys at the
    /// transaction's current `for_update_ts` -- Go `handlePessimisticDML`'s
    /// lock step. The outcome tells the session layer whether the statement
    /// stands, must be re-executed at an advanced timestamp, or failed.
    pub fn lock_keys(&self, keys: Vec<Vec<u8>>) -> Result<LockKeysOutcome, StorageError> {
        self.lock_keys_with_assertions(keys, BTreeSet::new(), BTreeMap::new(), false)
    }

    /// [`Self::lock_keys`], asking TiKV to answer each newly locked key's row
    /// WITH the lock and serving later reads of those keys from the answers —
    /// Go's point-write fold (`InitReturnValues` /
    /// `TxnCtx.SetPessimisticLockCache`, `pkg/executor/point_get.go:612-624`).
    pub fn lock_keys_with_values(
        &self,
        keys: Vec<Vec<u8>>,
        return_values: bool,
    ) -> Result<LockKeysOutcome, StorageError> {
        self.lock_keys_with_assertions(keys, BTreeSet::new(), BTreeMap::new(), return_values)
    }

    /// Acquires statement locks with the lazy INSERT assertions selected by
    /// Go's `getPessimisticLazyCheckMode`.
    pub fn lock_keys_with_assertions(
        &self,
        keys: Vec<Vec<u8>>,
        presume_not_exists: BTreeSet<Vec<u8>>,
        duplicate_hints: BTreeMap<Vec<u8>, DuplicateKeyHint>,
        return_values: bool,
    ) -> Result<LockKeysOutcome, StorageError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        promote_pessimistic_state(&mut state)?;
        let call = UnaryCallContext::with_timeout(self.timeout);
        let outcome = match &mut *state {
            SessionTransactionState::Pessimistic {
                transaction,
                opener,
                keep_alive,
                lock_values,
            } => acquire_statement_locks(
                transaction,
                opener,
                keep_alive,
                lock_values,
                &keys,
                &presume_not_exists,
                &duplicate_hints,
                return_values,
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
        if matches!(outcome, LockKeysOutcome::TransactionError(_)) {
            let _ = finish_session_transaction(&mut state);
        }
        Ok(outcome)
    }

    /// Releases the locks a FAILED statement accumulated across its retry
    /// rounds -- Go `OnPessimisticStmtEnd(isSuccessful=false)` ->
    /// `CancelFairLocking` (`pkg/sessiontxn/isolation/base.go`): a contender
    /// must not keep blocking on keys a statement the client was told failed
    /// had fair-locked. An empty key set releases nothing.
    pub fn release_keys(&self, keys: Vec<Vec<u8>>) -> Result<(), StorageError> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            SessionTransactionState::Pessimistic {
                transaction,
                lock_values,
                ..
            } => {
                let call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
                transaction
                    .pessimistic_rollback(&keys, &call)
                    .map_err(|cause| StorageError::Backend(cause.to_string()))?;
                for key in &keys {
                    lock_values.remove(key);
                }
                Ok(())
            }
            SessionTransactionState::Optimistic(_) => Ok(()),
            SessionTransactionState::PessimisticPending { .. } => Ok(()),
            SessionTransactionState::Finished => Err(StorageError::Backend(
                "the transaction is already finished".to_owned(),
            )),
        }
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
        self.commit_with(buffer, Vec::new())
    }

    /// Publishes the staged writes together with `extra`, as one transaction at
    /// this transaction's own `start_ts`.
    ///
    /// The two sets are one commit because they are one change: an index
    /// change's meta keys say the index exists and its data keys are what it
    /// contains, and a reader that saw the first without the second would get
    /// the wrong rows with no error. Ordering between the sets is not the
    /// caller's business — the coordinator sorts and validates the whole
    /// mutation set before prewrite.
    ///
    /// # Errors
    ///
    /// Returns the client-visible error of any 2PC that did not commit.
    pub fn commit_with(
        self,
        buffer: &MutationBuffer,
        extra: Vec<OptimisticMutation>,
    ) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
        let (mut mutations, _) = staged_mutations(buffer).map_err(coordinator_sql_error)?;
        mutations.extend(extra);
        if mutations.is_empty() {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            finish_session_transaction(&mut state).map_err(storage_sql_error)?;
            return Ok(None);
        }
        let state = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::replace(&mut *state, SessionTransactionState::Finished)
        };
        let call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
        let outcome = match state {
            SessionTransactionState::Optimistic(transaction) => {
                transaction.commit(mutations, &call)
            }
            SessionTransactionState::PessimisticPending {
                transaction,
                opened_at,
                ..
            } => RealPessimisticTransaction::from_transaction(transaction, opened_at)
                .and_then(|transaction| transaction.commit(mutations, &call)),
            SessionTransactionState::Pessimistic { transaction, .. } => {
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
        buffer.reset();
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
        SessionTransactionState::Pessimistic {
            mut transaction, ..
        } => {
            let held = transaction.locked_keys();
            let call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
            let rolled_back = transaction
                .pessimistic_rollback(&held, &call)
                .map_err(|error| StorageError::Backend(error.to_string()));
            let finished = transaction
                .into_two_pc()
                .finish_without_writes()
                .map(|_| ())
                .map_err(|error| StorageError::Backend(error.to_string()));
            rolled_back.and(finished)
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
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        let bytes = key.as_bytes().to_vec();
        let read_ts = self.read_ts.unwrap_or(self.start_ts);
        let call = UnaryCallContext::with_timeout(self.timeout);
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
                    if let Some(cached) = lock_values.get(&bytes) {
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
        let call = UnaryCallContext::with_timeout(self.timeout);
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
        let call = UnaryCallContext::with_timeout(self.timeout);
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
/// The coordinator already retries region errors and resolvable locks
/// internally; what reaches here outlived that budget. A topology or lock cause
/// is still worth a *statement* retry at a fresh timestamp, which is the slot
/// [`StorageError::Retryable`] was reserved for; everything else is terminal
/// for the statement.
fn classify(error: OptimisticCoordinatorError) -> StorageError {
    let message = error.to_string();
    let lowered = message.to_lowercase();
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

/// The buffer's staged writes as one mutation set, with the bytes they carry.
///
/// The mutations carry no existence assertion (`Op_Put`/`Op_Del` only): the
/// buffer holds raw row and index keys whose prior state the storage seam does
/// not record, and asserting the wrong one would fail a correct commit.
fn staged_mutations(
    buffer: &MutationBuffer,
) -> Result<(Vec<OptimisticMutation>, usize), OptimisticCoordinatorError> {
    let staged = buffer.snapshot();
    staged_mutations_from_entries(buffer, staged)
}

/// Builds mutations from entries already detached from the session buffer.
/// The autocommit path uses this ownership-preserving form so row and index
/// bytes are handed to TiKV without a second clone; explicit transactions keep
/// [`staged_mutations`] because their buffer remains live after each statement.
fn staged_mutations_from_entries(
    buffer: &MutationBuffer,
    staged: Vec<(Key, Option<Vec<u8>>)>,
) -> Result<(Vec<OptimisticMutation>, usize), OptimisticCoordinatorError> {
    // Keys an INSERT staged presumed absent (`kv.SetPresumeKeyNotExists`)
    // prewrite as Go's own lazy inserts do: `Op_Insert`, which TiKV rejects
    // when a committed version of the key turns out to exist. This is the
    // deferred duplicate check landing -- Go `DupKeyCheckLazy` under a
    // pessimistic transaction reports 1062 from exactly this mechanism
    // (`twoPhaseCommitter.initKeysAndMutations` typing presume keys as
    // insert).
    let presumed_absent = buffer.take_presume_not_exists();
    let mut mutations = Vec::with_capacity(staged.len());
    let mut planned_bytes = 0usize;
    for (key, value) in staged {
        planned_bytes += key.as_bytes().len() + value.as_ref().map_or(0, Vec::len);
        let mutation = match value {
            Some(value) if presumed_absent.contains(&key) => {
                OptimisticMutation::insert(key.into_bytes(), value)
            }
            Some(value) => OptimisticMutation::index_put(key.into_bytes(), value),
            None => OptimisticMutation::index_delete(key.into_bytes()),
        }
        .map_err(OptimisticCoordinatorError::Mutations)?;
        mutations.push(mutation);
    }
    Ok((mutations, planned_bytes))
}

/// Finds the table/index text that Go retained when a deferred insert marked a
/// record key presumed absent. Only an `AlreadyExists` outcome can consume it;
/// all other transaction failures keep their normal typed mapping.
fn deferred_duplicate_hint(
    outcome: &OptimisticCommitOutcome,
    buffer: &MutationBuffer,
) -> Option<DuplicateKeyHint> {
    let key = match outcome {
        OptimisticCommitOutcome::RolledBack(result) => &result.cause,
        OptimisticCommitOutcome::CleanupFailed(result) => &result.cause,
        _ => return None,
    };
    let TransactionCause::AlreadyExists { key, .. } = key else {
        return None;
    };
    buffer.duplicate_key_hint_for(key)
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
) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
    // An autocommit buffer is no longer needed by the session after this
    // boundary. Move its entries into the mutation set, matching Go's
    // MemBuffer hand-off and avoiding a second copy of every inserted row.
    let (mutations, planned_bytes) = staged_mutations_from_entries(buffer, buffer.take_snapshot())
        .map_err(coordinator_sql_error)?;
    if mutations.is_empty() {
        return Ok(None);
    }
    let transaction = match read_ts {
        Some(start_ts) => opener.begin_at(start_ts, mutations.len(), planned_bytes),
        None => opener.begin(mutations.len(), planned_bytes),
    }
    .map_err(coordinator_sql_error)?;
    let mut transaction = transaction;
    // Go's autocommit committer checks `@@tidb_enable_async_commit` /
    // `@@tidb_enable_1pc` at execute time (`checkAsyncCommit` / `checkOnePC`);
    // the same eligibility decision then runs at commit.
    transaction.set_commit_protocol(commit_protocol);
    let call = UnaryCallContext::with_timeout(timeout.max(TRANSACTION_END_TIMEOUT));
    let outcome = transaction
        .commit(mutations, &call)
        .map_err(coordinator_sql_error)?;
    let duplicate_hint = deferred_duplicate_hint(&outcome, buffer);
    commit_outcome_to_sql_error_with_hint(&outcome, duplicate_hint.as_ref())?;
    buffer.reset();
    Ok(Some(outcome))
}

/// The generic client-visible failure of a commit that never reached TiKV's
/// verdict. Only an outcome TiKV returned can carry a code of its own.
fn coordinator_sql_error(error: OptimisticCoordinatorError) -> LockSqlError {
    engine_sql_error(error.to_string())
}

fn storage_sql_error(error: StorageError) -> LockSqlError {
    engine_sql_error(error.to_string())
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

    /// The handle is still `Send`, and now so is the transaction inside it.
    ///
    /// This used to hold because only a channel crossed threads while the
    /// transaction stayed pinned to a borrowed worker. It holds for a stronger
    /// reason now: `RealOptimisticTransaction` is itself `Send` over an
    /// `Arc<Mutex<_>>` transport, which is what let the worker go.
    fn assert_send<T: Send>() {}

    #[test]
    fn the_snapshot_handle_is_sendable() {
        assert_send::<StatementSnapshot>();
        assert_send::<ClusterTableStorage>();
    }

    /// A finished statement snapshot refuses further reads, and finishing it
    /// twice is a no-op.
    ///
    /// Owning the transaction inline means the lifecycle is enforced by the
    /// snapshot itself.
    #[test]
    fn a_finished_statement_snapshot_refuses_reads_and_finishes_once() {
        let mut snapshot: StatementSnapshot = StatementSnapshot {
            transaction: None,
            start_ts: 42,
            timeout: Duration::from_secs(1),
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
