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
//! statement that reads no cluster row never opens this transaction at all --
//! the cluster session driver opens it at the statement's first read, not when
//! it binds the slot. And a statement that DECLARED its whole read is one
//! point get on the clustered handle opens through
//! [`StatementSnapshot::open_at_max_ts`] instead, at `u64::MAX`, which is Go's
//! `AdviseOptimizeWithPlan` shortcut; the declaration is a statement-level
//! fact and never inferred from a read, because at this seam an `UPDATE`'s
//! read-before-write is the same `get` on the same key.
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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::multi_statement_transaction::{
    LockKeysOutcome, TransactionStatementError, TRANSACTION_END_TIMEOUT,
};
use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
};
use tidb_executor::storage::StorageError;
use tidb_planner::read_only_scan::ReadLockWait;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    CommitProtocol, LockKeepAlive, LockWaitTime, OptimisticCommitOutcome,
    OptimisticCoordinatorError, OptimisticMutation, ProductionOptimisticTransaction,
    ProductionPessimisticTransaction, RealOptimisticTransactionOpener, MAX_OPTIMISTIC_MUTATIONS,
    MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::Key;

use crate::pessimistic_lock_error::{
    commit_outcome_to_sql_error, is_retryable_statement_failure, lock_failure_to_sql_error,
    locked_with_conflict_error, transaction_cause_to_sql_error, LockSqlError,
};
use crate::pinned_thread_pool::PinnedThreadPool;

/// One request the transaction's own thread serves, with the channel its answer
/// goes back on.
enum TransactionRequest {
    Get {
        key: Vec<u8>,
        reply: Sender<Result<Option<Vec<u8>>, StorageError>>,
    },
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        /// At most this many pairs, so an incremental cursor pays for the
        /// batch it consumes rather than for its whole range.
        limit: Option<usize>,
        reply: Sender<Result<SnapshotPairs, StorageError>>,
    },
    LockKeys {
        keys: Vec<Vec<u8>>,
        presume_not_exists: BTreeSet<Vec<u8>>,
        wait: ReadLockWait,
        reply: Sender<Result<WorkerLockOutcome, TransactionStatementError>>,
    },
    /// Publishes `mutations` at the transaction's original `start_ts` and ends
    /// the thread, whatever the outcome.
    Commit {
        mutations: Vec<OptimisticMutation>,
        reply: Sender<Result<OptimisticCommitOutcome, String>>,
    },
    Finish {
        reply: Sender<Result<(), StorageError>>,
    },
}

/// One real transaction pinned to the thread it was opened on.
///
/// The production transport is deliberately worker-local (`Rc<RefCell<..>>`),
/// while `TableStorage` is `Send` because a `KvTable` lives in a catalog the
/// server shares between workers. Both constraints hold at once here: the
/// transaction is created, used and ended on one thread it has to itself, and
/// what crosses threads is this handle -- a channel and a timestamp. No borrow
/// of the transport ever leaves its thread.
///
/// The thread is borrowed from [`PinnedThreadPool`] rather than spawned, which
/// is why the transaction still owns a thread for its whole life without every
/// statement paying to create one. A statement that opens a snapshot and
/// finishes it costs one channel handshake instead of a `pthread_create` plus a
/// `join`; on the cluster path, where every statement -- `SELECT 1` included --
/// opens one, that is roughly 17 microseconds each.
struct TransactionThread {
    requests: Option<Sender<TransactionRequest>>,
    start_ts: u64,
    statement_read_ts: u64,
}

struct WorkerLockOutcome {
    outcome: LockKeysOutcome,
    statement_read_ts: u64,
}

/// Go client-go `KVSnapshot.Get`/`BatchGet` use a 20-second retry backoff
/// budget (`getMaxBackoff`/`batchGetMaxBackoff`) around their short TiKV RPCs.
const SNAPSHOT_READ_MAX_BACKOFF: Duration = Duration::from_secs(20);

fn transaction_request_timeout(request: &TransactionRequest, default: Duration) -> Duration {
    match request {
        TransactionRequest::Get { .. } | TransactionRequest::Scan { .. } => {
            default.max(SNAPSHOT_READ_MAX_BACKOFF)
        }
        TransactionRequest::LockKeys { wait, .. } => match wait {
            ReadLockWait::Blocking => default.max(Duration::from_secs(55)),
            ReadLockWait::NoWait => default,
            ReadLockWait::Seconds(seconds) => {
                default.max(Duration::from_secs(seconds.saturating_add(5)))
            }
        },
        _ => default,
    }
}

/// Which transaction a [`TransactionThread`] opens, and therefore what its
/// `start_ts` costs.
///
/// [`TransactionOpen::ReadOnlyAtMaxTs`] is the only one that spends no PD
/// timestamp, and it is reachable only from a statement that has DECLARED it
/// reads one row once; see
/// [`StatementSnapshot::open_at_max_ts`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionOpen {
    Writable,
    ReadOnly,
    ReadOnlyAtMaxTs,
    /// A multi-statement transaction whose statements may take pessimistic
    /// locks. It still publishes through the same two-phase coordinator.
    Pessimistic {
        fair_locking: bool,
        commit_protocol: CommitProtocol,
    },
}

enum OpenTransaction {
    Optimistic(ProductionOptimisticTransaction),
    Pessimistic(ProductionPessimisticTransaction),
}

impl OpenTransaction {
    fn start_ts(&self) -> u64 {
        match self {
            Self::Optimistic(transaction) => transaction.start_ts(),
            Self::Pessimistic(transaction) => transaction.start_ts(),
        }
    }
}

impl TransactionOpen {
    const fn writable(writable: bool) -> Self {
        if writable {
            TransactionOpen::Writable
        } else {
            TransactionOpen::ReadOnly
        }
    }
}

impl TransactionThread {
    /// Opens one transaction on a thread it owns until it ends, spending
    /// exactly one PD timestamp.
    ///
    /// `writable` decides the publication budget the coordinator opens with: a
    /// read-only transaction is opened with the tightest possible one (zero),
    /// so a later attempt to publish a mutation through it is refused rather
    /// than admitted by accident.
    ///
    /// The call returns only once the transaction exists, so `start_ts` is an
    /// allocated timestamp rather than a promise.
    fn open(
        opener: &Arc<RealOptimisticTransactionOpener>,
        timeout: Duration,
        writable: bool,
        name: &str,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Self::open_with(opener, timeout, TransactionOpen::writable(writable), name)
    }

    fn open_with(
        opener: &Arc<RealOptimisticTransactionOpener>,
        timeout: Duration,
        open: TransactionOpen,
        name: &str,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let (requests, incoming) = mpsc::channel::<TransactionRequest>();
        let (opened, opened_reply) = mpsc::channel::<Result<u64, OptimisticCoordinatorError>>();
        let opener = Arc::clone(opener);
        PinnedThreadPool::shared()
            .run(
                name,
                Box::new(move || {
                    let begun = match open {
                        TransactionOpen::Writable => opener
                            .begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
                            .map(OpenTransaction::Optimistic),
                        TransactionOpen::ReadOnly => {
                            opener.begin_read_only().map(OpenTransaction::Optimistic)
                        }
                        TransactionOpen::ReadOnlyAtMaxTs => opener
                            .begin_read_only_at_max_ts()
                            .map(OpenTransaction::Optimistic),
                        TransactionOpen::Pessimistic {
                            fair_locking,
                            commit_protocol,
                        } => opener
                            .begin_pessimistic(
                                MAX_OPTIMISTIC_MUTATIONS,
                                MAX_OPTIMISTIC_TRANSACTION_BYTES,
                            )
                            .map(|mut transaction| {
                                transaction.set_fair_locking(fair_locking);
                                transaction.set_commit_protocol(commit_protocol);
                                OpenTransaction::Pessimistic(transaction)
                            }),
                    };
                    let transaction = match begun {
                        Ok(transaction) => {
                            // A caller that stopped waiting leaves no lock
                            // behind: the transaction ends here instead.
                            if opened.send(Ok(transaction.start_ts())).is_err() {
                                finish_open_transaction(transaction);
                                return;
                            }
                            transaction
                        }
                        Err(error) => {
                            let _ = opened.send(Err(error));
                            return;
                        }
                    };
                    serve_transaction(transaction, &incoming, timeout, &opener);
                }),
            )
            .map_err(OptimisticCoordinatorError::SnapshotGet)?;
        let start_ts = opened_reply
            .recv()
            .map_err(|_| {
                OptimisticCoordinatorError::SnapshotGet(
                    "the transaction thread ended before opening a transaction".to_owned(),
                )
            })
            .and_then(|result| result)?;
        Ok(Self {
            requests: Some(requests),
            start_ts,
            statement_read_ts: start_ts,
        })
    }

    /// Ends the transaction without publishing anything, leaving no locks
    /// behind. Calling it twice is a no-op.
    ///
    /// The reply is what orders the cleanup: the worker sends it only after
    /// `finish_without_writes` returned, so a caller that has this answer knows
    /// the transaction is over. That is the same guarantee joining the thread
    /// used to give, without ending a thread to get it.
    fn finish(&mut self) -> Result<(), StorageError> {
        let Some(requests) = self.requests.take() else {
            return Ok(());
        };
        let (reply, answer) = mpsc::channel();
        match requests.send(TransactionRequest::Finish { reply }) {
            Ok(()) => answer.recv().unwrap_or(Ok(())),
            // The worker is already gone, which means it already finished the
            // transaction on its way out.
            Err(_) => Ok(()),
        }
    }

    /// Publishes `mutations` on this very transaction, so the prewrite carries
    /// the timestamp the transaction opened at.
    fn commit(
        &mut self,
        mutations: Vec<OptimisticMutation>,
    ) -> Result<OptimisticCommitOutcome, String> {
        let requests = self
            .requests
            .take()
            .ok_or_else(|| "the transaction is already finished".to_owned())?;
        let (reply, answer) = mpsc::channel();
        match requests.send(TransactionRequest::Commit { mutations, reply }) {
            Ok(()) => answer
                .recv()
                .unwrap_or_else(|_| Err("the transaction thread stopped mid-commit".to_owned())),
            Err(_) => Err("the transaction thread is gone".to_owned()),
        }
    }

    fn lock_keys_once(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &BTreeSet<Vec<u8>>,
        wait: ReadLockWait,
    ) -> Result<LockKeysOutcome, TransactionStatementError> {
        let requests = self.requests.as_ref().cloned().ok_or_else(|| {
            TransactionStatementError::Transaction(LockSqlError {
                code: 1105,
                state: *b"HY000",
                message: "the transaction is already finished".to_owned(),
            })
        })?;
        let (reply, answer) = mpsc::channel();
        requests
            .send(TransactionRequest::LockKeys {
                keys: keys.to_vec(),
                presume_not_exists: presume_not_exists.clone(),
                wait,
                reply,
            })
            .map_err(|_| {
                TransactionStatementError::Transaction(LockSqlError {
                    code: 1105,
                    state: *b"HY000",
                    message: "the transaction thread is gone".to_owned(),
                })
            })?;
        let answer = answer.recv().map_err(|_| {
            TransactionStatementError::Transaction(LockSqlError {
                code: 1105,
                state: *b"HY000",
                message: "the transaction thread stopped while locking keys".to_owned(),
            })
        })??;
        self.statement_read_ts = answer.statement_read_ts;
        Ok(answer.outcome)
    }

    fn sender(&self) -> Result<Sender<TransactionRequest>, StorageError> {
        self.requests
            .as_ref()
            .cloned()
            .ok_or_else(|| StorageError::Backend("the transaction is already finished".to_owned()))
    }
}

impl Drop for TransactionThread {
    fn drop(&mut self) {
        // An owner that dropped the handle without finishing still must not
        // leave a transaction open, and must not race ahead of its cleanup:
        // `finish` waits for the worker's answer.
        let _ = self.finish();
    }
}

/// Sends one request to a transaction's thread and waits for its answer.
fn ask<T>(
    requests: &Sender<TransactionRequest>,
    request: impl FnOnce(Sender<Result<T, StorageError>>) -> TransactionRequest,
) -> Result<T, StorageError> {
    let (reply, answer) = mpsc::channel();
    requests
        .send(request(reply))
        .map_err(|_| StorageError::Backend("the transaction thread is gone".to_owned()))?;
    answer
        .recv()
        .map_err(|_| StorageError::Backend("the transaction thread stopped mid-read".to_owned()))?
}

/// Serves the transaction on its own thread until it is committed, finished, or
/// its last handle goes away.
fn serve_transaction(
    mut transaction: OpenTransaction,
    incoming: &Receiver<TransactionRequest>,
    timeout: Duration,
    opener: &RealOptimisticTransactionOpener,
) {
    let mut keep_alive: Option<LockKeepAlive> = None;
    while let Ok(request) = incoming.recv() {
        // Minted per request, never once for the thread. `UnaryCallContext`
        // carries an ABSOLUTE deadline, so a single context made when the
        // transaction opened would charge every later statement — and the
        // commit — for the wall-clock time the client spent holding the
        // transaction, which is not work anything did.
        let call = UnaryCallContext::with_timeout(transaction_request_timeout(&request, timeout));
        let call = &call;
        match request {
            TransactionRequest::Get { key, reply } => {
                let answer = match &mut transaction {
                    OpenTransaction::Optimistic(transaction) => transaction
                        .snapshot_get(&key, call)
                        .map(|result| result.value)
                        .map_err(classify),
                    OpenTransaction::Pessimistic(transaction) => transaction
                        .statement_snapshot_get(&key, call)
                        .map(|result| result.value)
                        .map_err(classify),
                };
                let _ = reply.send(answer);
            }
            TransactionRequest::Scan {
                start,
                end,
                limit,
                reply,
            } => {
                let answer = match &mut transaction {
                    OpenTransaction::Optimistic(transaction) => transaction
                        .snapshot_scan(&start, &end, limit, call)
                        .map_err(classify),
                    OpenTransaction::Pessimistic(transaction) => transaction
                        .statement_snapshot_scan(&start, &end, limit, call)
                        .map_err(classify),
                };
                let _ = reply.send(answer);
            }
            TransactionRequest::LockKeys {
                keys,
                presume_not_exists,
                wait,
                reply,
            } => {
                let answer = match &mut transaction {
                    OpenTransaction::Optimistic(_) => {
                        Err(TransactionStatementError::Statement(LockSqlError {
                            code: 1105,
                            state: *b"HY000",
                            message: "an optimistic transaction cannot take pessimistic locks"
                                .to_owned(),
                        }))
                    }
                    OpenTransaction::Pessimistic(transaction) => {
                        let outcome = lock_pessimistic_keys_once(
                            transaction,
                            &keys,
                            &presume_not_exists,
                            wait,
                            call,
                        );
                        match outcome {
                            Ok(outcome) => {
                                let keep_alive_result = if keep_alive.is_none() {
                                    transaction.primary_key().map_or(Ok(()), |primary_key| {
                                        opener
                                            .start_lock_keep_alive(
                                                primary_key.to_vec(),
                                                transaction.start_ts(),
                                            )
                                            .map(|started| keep_alive = Some(started))
                                            .map_err(|error| {
                                                TransactionStatementError::Transaction(
                                                    LockSqlError {
                                                        code: 1105,
                                                        state: *b"HY000",
                                                        message: format!(
                                                            "cannot keep the transaction's primary lock alive: {error}"
                                                        ),
                                                    },
                                                )
                                            })
                                    })
                                } else {
                                    Ok(())
                                };
                                keep_alive_result.map(|()| WorkerLockOutcome {
                                    outcome,
                                    statement_read_ts: transaction.statement_read_ts(),
                                })
                            }
                            Err(error) => Err(error),
                        }
                    }
                };
                let _ = reply.send(answer);
            }
            TransactionRequest::Commit { mutations, reply } => {
                // The coordinator re-enters the write phase from the read
                // phase, so this prewrite carries the transaction's original
                // start timestamp -- the whole point of holding one open.
                // Ending the transaction is the store's work, not the last
                // statement's: client-go builds its commit and cleanup
                // backoffers on `c.store.Ctx()` with `cleanupMaxBackoff`,
                // deliberately decoupled from the statement's context.
                let end_call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
                if let Some(keep_alive) = keep_alive.take() {
                    keep_alive.close();
                }
                let answer = match transaction {
                    OpenTransaction::Optimistic(transaction) => transaction
                        .commit(mutations, &end_call)
                        .map_err(|error| error.to_string()),
                    OpenTransaction::Pessimistic(transaction) => transaction
                        .commit(mutations, &end_call)
                        .map_err(|error| error.to_string()),
                };
                let _ = reply.send(answer);
                return;
            }
            TransactionRequest::Finish { reply } => {
                if let Some(keep_alive) = keep_alive.take() {
                    keep_alive.close();
                }
                let answer = match transaction {
                    OpenTransaction::Optimistic(transaction) => transaction
                        .finish_without_writes()
                        .map(|_| ())
                        .map_err(|error| StorageError::Backend(error.to_string())),
                    OpenTransaction::Pessimistic(mut transaction) => {
                        let call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
                        let locked = transaction.locked_keys();
                        if let Err(error) = transaction.pessimistic_rollback(&locked, &call) {
                            Err(StorageError::Backend(error.to_string()))
                        } else {
                            transaction
                                .into_two_pc()
                                .finish_without_writes()
                                .map(|_| ())
                                .map_err(|error| StorageError::Backend(error.to_string()))
                        }
                    }
                };
                let _ = reply.send(answer);
                return;
            }
        }
    }
    finish_open_transaction(transaction);
}

fn lock_pessimistic_keys_once(
    transaction: &mut ProductionPessimisticTransaction,
    keys: &[Vec<u8>],
    presume_not_exists: &BTreeSet<Vec<u8>>,
    wait: ReadLockWait,
    call: &UnaryCallContext,
) -> Result<LockKeysOutcome, TransactionStatementError> {
    if keys.is_empty() {
        return Ok(LockKeysOutcome::Acquired);
    }
    let wait = match wait {
        ReadLockWait::Blocking => LockWaitTime::session_lock_wait_timeout(),
        ReadLockWait::NoWait => LockWaitTime::NoWait,
        ReadLockWait::Seconds(seconds) => LockWaitTime::Timeout(Duration::from_secs(seconds)),
    };
    let held: BTreeSet<Vec<u8>> = transaction.locked_keys().into_iter().collect();
    // Fair locking keeps a lock granted with a newer committed version while
    // the caller reruns the statement at the advanced `for_update_ts`. That
    // lock is already owned by this transaction on the retry; asking TiKV to
    // ForceLock it again reports the same `LockedWithConflict` forever and
    // exhausts the statement retry budget. Reacquire only keys this statement
    // has not already inherited from an earlier attempt or statement.
    let keys_to_lock = lock_keys_not_held(keys, &held);
    if keys_to_lock.is_empty() {
        return Ok(LockKeysOutcome::Acquired);
    }
    let presume_not_exists_to_lock = presume_not_exists
        .iter()
        .filter(|key| !held.contains(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    let retry_reason =
        match transaction.acquire_locks(&keys_to_lock, &presume_not_exists_to_lock, wait, call) {
            Ok(acquired) if acquired.locked_with_conflict.is_empty() => {
                return Ok(LockKeysOutcome::Acquired);
            }
            Ok(acquired) => {
                let (key, conflict_commit_ts) = acquired
                    .locked_with_conflict
                    .iter()
                    .max_by_key(|(_, conflict_ts)| *conflict_ts)
                    .expect("the non-empty branch admits a conflict");
                locked_with_conflict_error(transaction.start_ts(), *conflict_commit_ts, key)
            }
            Err(failure) => {
                let added = transaction
                    .locked_keys()
                    .into_iter()
                    .filter(|key| !held.contains(key))
                    .collect::<Vec<_>>();
                if let Err(cause) = transaction.pessimistic_rollback(&added, call) {
                    return Err(TransactionStatementError::Transaction(
                        transaction_cause_to_sql_error(&cause),
                    ));
                }
                if !is_retryable_statement_failure(&failure) {
                    let error = lock_failure_to_sql_error(&failure);
                    return Err(if failure.is_statement_scoped() {
                        TransactionStatementError::Statement(error)
                    } else {
                        TransactionStatementError::Transaction(error)
                    });
                }
                lock_failure_to_sql_error(&failure)
            }
        };
    transaction.advance_for_update_ts().map_err(|failure| {
        let error = lock_failure_to_sql_error(&failure);
        if failure.is_statement_scoped() {
            TransactionStatementError::Statement(error)
        } else {
            TransactionStatementError::Transaction(error)
        }
    })?;
    Ok(LockKeysOutcome::Retry(retry_reason))
}

fn lock_keys_not_held(keys: &[Vec<u8>], held: &BTreeSet<Vec<u8>>) -> Vec<Vec<u8>> {
    keys.iter()
        .filter(|key| !held.contains(*key))
        .cloned()
        .collect()
}

fn finish_open_transaction(transaction: OpenTransaction) {
    match transaction {
        OpenTransaction::Optimistic(transaction) => {
            let _ = transaction.finish_without_writes();
        }
        OpenTransaction::Pessimistic(mut transaction) => {
            let call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
            let locked = transaction.locked_keys();
            let _ = transaction.pessimistic_rollback(&locked, &call);
            let _ = transaction.into_two_pc().finish_without_writes();
        }
    }
}

/// One statement's read snapshot: a real read-only transaction at one PD
/// timestamp, owned by the thread that opened it.
///
/// This is the autocommit shape. Inside an explicit transaction the session
/// reads through [`SessionTransaction::snapshot`] instead, so every statement
/// shares the one timestamp `BEGIN` took.
pub struct StatementSnapshot {
    thread: TransactionThread,
}

impl fmt::Debug for StatementSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatementSnapshot")
            .field("start_ts", &self.thread.start_ts)
            .field("open", &self.thread.requests.is_some())
            .finish()
    }
}

impl StatementSnapshot {
    /// Opens one read-only transaction on its own thread, spending exactly one
    /// PD timestamp.
    pub fn open(
        opener: Arc<RealOptimisticTransactionOpener>,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Ok(Self {
            thread: TransactionThread::open(&opener, timeout, false, "cluster-statement-snapshot")?,
        })
    }

    /// Opens one read-only transaction at `u64::MAX` on its own thread,
    /// spending NO PD timestamp.
    ///
    /// Reachable only from a statement that declared, before its first read,
    /// that its whole read is one autocommit point get on the clustered handle
    /// — Go's `IsPointGetWithPKOrUniqueKeyByAutoCommit` plus
    /// `AdviseOptimizeWithPlan`. `MaxUint64` ignores snapshot isolation, so a
    /// statement with a second read would see two different snapshots through
    /// one handle; the declaration, not this method, is what forbids that.
    pub fn open_at_max_ts(
        opener: Arc<RealOptimisticTransactionOpener>,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Ok(Self {
            thread: TransactionThread::open_with(
                &opener,
                timeout,
                TransactionOpen::ReadOnlyAtMaxTs,
                "cluster-statement-snapshot-max-ts",
            )?,
        })
    }

    /// The timestamp every read of this statement is served at.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.thread.start_ts
    }

    /// The timestamp the next statement reads at.
    #[must_use]
    pub const fn statement_read_ts(&self) -> u64 {
        self.thread.statement_read_ts
    }

    /// Ends the statement's read transaction, leaving no locks behind.
    ///
    /// Calling it twice is a no-op: the statement is already finished.
    pub fn finish(&mut self) -> Result<(), StorageError> {
        self.thread.finish()
    }
}

impl ClusterSnapshot for StatementSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        let bytes = key.as_bytes().to_vec();
        ask(&self.thread.sender()?, |reply| TransactionRequest::Get {
            key: bytes,
            reply,
        })
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        let start = start.as_bytes().to_vec();
        let end = end.as_bytes().to_vec();
        ask(&self.thread.sender()?, |reply| TransactionRequest::Scan {
            start,
            end,
            limit,
            reply,
        })
    }

    fn start_ts(&self) -> u64 {
        self.thread.start_ts
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
pub struct SessionTransaction {
    thread: TransactionThread,
    mode: tidb_planner::txn_mode::SessionTxnMode,
}

impl fmt::Debug for SessionTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionTransaction")
            .field("start_ts", &self.thread.start_ts)
            .field("open", &self.thread.requests.is_some())
            .finish()
    }
}

impl SessionTransaction {
    /// Opens the transaction `BEGIN` holds, spending exactly one PD timestamp.
    ///
    /// The publication budget is the transaction-size limit itself, because a
    /// multi-statement transaction cannot know its mutation set at `BEGIN`; the
    /// commit still enforces the same limits against the buffer it publishes.
    pub fn begin(
        opener: Arc<RealOptimisticTransactionOpener>,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Self::begin_mode(
            opener,
            timeout,
            tidb_planner::txn_mode::SessionTxnMode::Optimistic,
            false,
            CommitProtocol::two_phase_only(),
        )
    }

    /// Opens a real multi-statement transaction in the requested TiDB mode.
    ///
    /// The wide SQL session uses this entry point with no configured table;
    /// reads are addressed by raw keys and therefore work for every loaded
    /// table in the connection's catalog.
    pub fn begin_mode(
        opener: Arc<RealOptimisticTransactionOpener>,
        timeout: Duration,
        mode: tidb_planner::txn_mode::SessionTxnMode,
        fair_locking: bool,
        commit_protocol: CommitProtocol,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let thread = match mode {
            tidb_planner::txn_mode::SessionTxnMode::Optimistic => {
                TransactionThread::open(&opener, timeout, true, "cluster-session-transaction")?
            }
            tidb_planner::txn_mode::SessionTxnMode::Pessimistic => TransactionThread::open_with(
                &opener,
                timeout,
                TransactionOpen::Pessimistic {
                    fair_locking,
                    commit_protocol,
                },
                "cluster-session-pessimistic-transaction",
            )?,
        };
        Ok(Self { thread, mode })
    }

    /// The mode selected when the transaction opened.
    #[must_use]
    pub const fn mode(&self) -> tidb_planner::txn_mode::SessionTxnMode {
        self.mode
    }

    /// The one timestamp every statement of this transaction reads at.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.thread.start_ts
    }

    /// The timestamp the next statement reads at.
    #[must_use]
    pub const fn statement_read_ts(&self) -> u64 {
        self.thread.statement_read_ts
    }

    /// A read handle onto this transaction, for one statement to bind.
    ///
    /// Dropping it ends the statement, not the transaction: that is the
    /// re-entry the shape exists for.
    pub fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, StorageError> {
        Ok(Box::new(SessionSnapshot {
            requests: self.thread.sender()?,
            start_ts: self.thread.statement_read_ts,
        }))
    }

    /// Attempts one raw-key pessimistic lock batch. A retry result means the
    /// caller must restore its statement buffer image and rerun the SQL at the
    /// transaction's advanced statement timestamp.
    pub fn lock_keys_once(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &BTreeSet<Vec<u8>>,
        wait: ReadLockWait,
    ) -> Result<LockKeysOutcome, TransactionStatementError> {
        self.thread.lock_keys_once(keys, presume_not_exists, wait)
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
        mut self,
        buffer: &MutationBuffer,
        extra: Vec<OptimisticMutation>,
    ) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
        let (mut mutations, _) = staged_mutations(buffer).map_err(coordinator_sql_error)?;
        mutations.extend(extra);
        if mutations.is_empty() {
            self.thread.finish().map_err(storage_sql_error)?;
            return Ok(None);
        }
        let outcome = self.thread.commit(mutations).map_err(engine_sql_error)?;
        commit_outcome_to_sql_error(&outcome)?;
        buffer.reset();
        Ok(Some(outcome))
    }

    /// Ends the transaction without publishing anything.
    ///
    /// # Errors
    ///
    /// Returns the failure of ending the transaction's own read side.
    pub fn rollback(mut self) -> Result<(), String> {
        self.thread.finish().map_err(|error| error.to_string())
    }
}

/// One statement's view of an open session transaction.
///
/// It carries no ownership of the transaction: dropping it is the end of the
/// statement, and the transaction stays open for the next one.
struct SessionSnapshot {
    requests: Sender<TransactionRequest>,
    /// The timestamp the transaction opened at, which every statement of it
    /// reads at; a remote scan has to name it.
    start_ts: u64,
}

impl fmt::Debug for SessionSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionSnapshot")
            .field("start_ts", &self.start_ts)
            .finish()
    }
}

impl ClusterSnapshot for SessionSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        let bytes = key.as_bytes().to_vec();
        ask(&self.requests, |reply| TransactionRequest::Get {
            key: bytes,
            reply,
        })
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        let start = start.as_bytes().to_vec();
        let end = end.as_bytes().to_vec();
        ask(&self.requests, |reply| TransactionRequest::Scan {
            start,
            end,
            limit,
            reply,
        })
    }

    fn start_ts(&self) -> u64 {
        self.start_ts
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
    let retryable = ["region", "epoch", "lock", "leader", "stale", "budget"]
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
pub fn statement_storage(
    opener: Arc<RealOptimisticTransactionOpener>,
    buffer: MutationBuffer,
    timeout: Duration,
) -> Result<(ClusterTableStorage, Arc<Mutex<StatementSnapshot>>), OptimisticCoordinatorError> {
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
    let staged = buffer.staged();
    let mut mutations = Vec::with_capacity(staged.len());
    let mut planned_bytes = 0usize;
    for (key, value) in staged {
        planned_bytes += key.as_bytes().len() + value.as_ref().map_or(0, Vec::len);
        let mutation = match value {
            Some(value) => OptimisticMutation::index_put(key.into_bytes(), value),
            None => OptimisticMutation::index_delete(key.into_bytes()),
        }
        .map_err(OptimisticCoordinatorError::Mutations)?;
        mutations.push(mutation);
    }
    Ok((mutations, planned_bytes))
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
pub fn commit_staged_buffer(
    opener: &RealOptimisticTransactionOpener,
    buffer: &MutationBuffer,
    read_ts: Option<u64>,
    timeout: Duration,
) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
    let (mutations, planned_bytes) = staged_mutations(buffer).map_err(coordinator_sql_error)?;
    if mutations.is_empty() {
        return Ok(None);
    }
    let transaction = match read_ts {
        Some(start_ts) => opener.begin_at(start_ts, mutations.len(), planned_bytes),
        None => opener.begin(mutations.len(), planned_bytes),
    }
    .map_err(coordinator_sql_error)?;
    let call = UnaryCallContext::with_timeout(timeout.max(TRANSACTION_END_TIMEOUT));
    let outcome = transaction
        .commit(mutations, &call)
        .map_err(coordinator_sql_error)?;
    commit_outcome_to_sql_error(&outcome)?;
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

    /// The handle crosses threads even though the transaction it drives never
    /// does; that is the whole reason for the thread-owned shape.
    ///
    /// It is what says the split is between the transaction and its handle --
    /// not between "a fresh thread" and "a reused one". Borrowing the thread
    /// from the pinned pool keeps the transaction on one thread for its whole
    /// life, so this assertion holds for exactly the reason it always did.
    fn assert_send<T: Send>() {}

    #[test]
    fn the_snapshot_handle_is_sendable() {
        assert_send::<StatementSnapshot>();
        assert_send::<ClusterTableStorage>();
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

    #[test]
    fn a_statement_retry_does_not_relock_fairly_acquired_keys() {
        let held = BTreeSet::from([b"already-locked".to_vec()]);
        let requested = vec![b"already-locked".to_vec(), b"new-key".to_vec()];

        assert_eq!(
            lock_keys_not_held(&requested, &held),
            vec![b"new-key".to_vec()]
        );
    }

    #[test]
    fn a_lock_request_deadline_covers_its_wait_budget() {
        let default = Duration::from_secs(5);
        let request = |wait| {
            let (reply, _) = mpsc::channel();
            TransactionRequest::LockKeys {
                keys: vec![b"key".to_vec()],
                presume_not_exists: BTreeSet::new(),
                wait,
                reply,
            }
        };

        assert_eq!(
            transaction_request_timeout(&request(ReadLockWait::NoWait), default),
            default
        );
        assert_eq!(
            transaction_request_timeout(&request(ReadLockWait::Blocking), default),
            Duration::from_secs(55)
        );
        assert_eq!(
            transaction_request_timeout(&request(ReadLockWait::Seconds(60)), default),
            Duration::from_secs(65)
        );
    }

    #[test]
    fn a_snapshot_request_deadline_covers_client_go_backoff() {
        let default = Duration::from_secs(5);
        let (get_reply, _) = mpsc::channel();
        let get = TransactionRequest::Get {
            key: b"key".to_vec(),
            reply: get_reply,
        };
        let (scan_reply, _) = mpsc::channel();
        let scan = TransactionRequest::Scan {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            limit: None,
            reply: scan_reply,
        };

        assert_eq!(
            transaction_request_timeout(&get, default),
            SNAPSHOT_READ_MAX_BACKOFF
        );
        assert_eq!(
            transaction_request_timeout(&scan, default),
            SNAPSHOT_READ_MAX_BACKOFF
        );
    }
}
