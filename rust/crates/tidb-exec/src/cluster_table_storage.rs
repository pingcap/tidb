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
//! connection worker and never opens a pinned transaction worker; the
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

use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::multi_statement_transaction::TRANSACTION_END_TIMEOUT;
use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
};
use tidb_executor::storage::StorageError;
use tidb_pd_client::PdClient;
use tidb_txnkv::pd_capability::{CapabilityTimestampSource, TimestampFutureWait};
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    LockKeepAlive, LockWaitTime, OptimisticCommitOutcome, OptimisticCoordinatorError,
    OptimisticMutation, PessimisticLockFailure, RealOptimisticTransaction,
    RealOptimisticTransactionOpener, RealPessimisticTransaction, StorePdCapability,
    StoreWriteClient, StoreWriteLoader, MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::Key;
use tidb_txnkv::PdRegionLoader;

use crate::pessimistic_lock_error::{
    commit_outcome_to_sql_error, is_retryable_statement_failure, lock_failure_to_sql_error,
    transaction_cause_to_sql_error, LockSqlError,
};
use crate::pinned_thread_pool::PinnedThreadPool;

/// One request the transaction's own thread serves, with the channel its answer
/// goes back on.
enum TransactionRequest {
    Get {
        key: Vec<u8>,
        /// `Some` reads at this statement timestamp instead of the
        /// transaction's `start_ts` -- a pessimistic statement retried after
        /// a lock conflict reads at its advanced `for_update_ts` (Go rebuilds
        /// the retried executor at `forUpdateTS`). `None` is every ordinary
        /// read.
        read_ts: Option<u64>,
        reply: Sender<Result<Option<Vec<u8>>, StorageError>>,
    },
    BatchGet {
        keys: Vec<Vec<u8>>,
        /// See [`TransactionRequest::Get::read_ts`].
        read_ts: Option<u64>,
        reply: Sender<Result<SnapshotPairs, StorageError>>,
    },
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        /// At most this many pairs, so an incremental cursor pays for the
        /// batch it consumes rather than for its whole range.
        limit: Option<usize>,
        /// See [`TransactionRequest::Get::read_ts`].
        read_ts: Option<u64>,
        reply: Sender<Result<SnapshotPairs, StorageError>>,
    },
    /// Acquires pessimistic locks on `keys` at the transaction's current
    /// `for_update_ts` -- Go's `KVTxn.LockKeys` for one DML statement's
    /// written keys. Served only by a pessimistic transaction; the
    /// optimistic worker refuses it.
    LockKeys {
        keys: Vec<Vec<u8>>,
        reply: Sender<LockKeysOutcome>,
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
    /// Ends a read-only statement snapshot without putting its caller on the
    /// worker's cleanup path.
    FinishDetached,
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
/// `join`; a statement that never reads storage keeps only its PD timestamp
/// future and does not borrow a worker.
struct TransactionThread {
    requests: Option<Sender<TransactionRequest>>,
    start_ts: u64,
}

/// A transaction thread whose worker-local open result has not been waited for.
struct PreparedTransactionThread {
    requests: Option<Sender<TransactionRequest>>,
    opened: Receiver<Result<u64, OptimisticCoordinatorError>>,
}

impl PreparedTransactionThread {
    fn wait(mut self) -> Result<TransactionThread, OptimisticCoordinatorError> {
        let start_ts = self
            .opened
            .recv()
            .map_err(|_| {
                OptimisticCoordinatorError::SnapshotGet(
                    "the transaction thread ended before opening a transaction".to_owned(),
                )
            })
            .and_then(|result| result)?;
        Ok(TransactionThread {
            requests: self.requests.take(),
            start_ts,
        })
    }
}

impl Drop for PreparedTransactionThread {
    fn drop(&mut self) {
        // Closing the request channel lets a transaction that already opened
        // finish itself; if opening is still in flight, the worker observes the
        // closed channel immediately after it publishes the result.
        self.requests.take();
    }
}

/// Which transaction a [`TransactionThread`] opens, and therefore what its
/// `start_ts` costs.
///
/// MaxTS point reads are deliberately absent: [`MaxTsSnapshot`] sends those
/// directly from the connection worker instead of opening a transaction
/// worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionOpen {
    Writable,
    /// An explicit pessimistic transaction: the same writable budget, plus
    /// the statement-lock protocol its worker serves.
    WritablePessimistic,
    ReadOnly,
    ReadOnlyAt(u64),
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
    fn open<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
        opener: &Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
        writable: bool,
        name: &str,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Self::open_with(opener, timeout, TransactionOpen::writable(writable), name)
    }

    fn open_with<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
        opener: &Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
        open: TransactionOpen,
        name: &str,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Self::prepare_with(opener, timeout, open, name)?.wait()
    }

    fn prepare_with<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
        opener: &Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
        open: TransactionOpen,
        name: &str,
    ) -> Result<PreparedTransactionThread, OptimisticCoordinatorError> {
        let (requests, incoming) = mpsc::channel::<TransactionRequest>();
        let (opened, opened_reply) = mpsc::channel::<Result<u64, OptimisticCoordinatorError>>();
        let opener = Arc::clone(opener);
        PinnedThreadPool::shared()
            .run(
                name,
                Box::new(move || {
                    if open == TransactionOpen::WritablePessimistic {
                        let transaction = match opener.begin_pessimistic(
                            MAX_OPTIMISTIC_MUTATIONS,
                            MAX_OPTIMISTIC_TRANSACTION_BYTES,
                        ) {
                            Ok(transaction) => {
                                if opened.send(Ok(transaction.start_ts())).is_err() {
                                    let _ = transaction.into_two_pc().finish_without_writes();
                                    return;
                                }
                                transaction
                            }
                            Err(error) => {
                                let _ = opened.send(Err(error));
                                return;
                            }
                        };
                        serve_pessimistic_transaction(transaction, &opener, &incoming, timeout);
                        return;
                    }
                    let begun = match open {
                        TransactionOpen::Writable => {
                            opener.begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
                        }
                        TransactionOpen::WritablePessimistic => {
                            unreachable!("the pessimistic arm above returned")
                        }
                        TransactionOpen::ReadOnly => opener.begin_read_only(),
                        TransactionOpen::ReadOnlyAt(start_ts) => {
                            opener.begin_read_only_at(start_ts)
                        }
                    };
                    let transaction = match begun {
                        Ok(transaction) => {
                            // A caller that stopped waiting leaves no lock
                            // behind: the transaction ends here instead.
                            if opened.send(Ok(transaction.start_ts())).is_err() {
                                let _ = transaction.finish_without_writes();
                                return;
                            }
                            transaction
                        }
                        Err(error) => {
                            let _ = opened.send(Err(error));
                            return;
                        }
                    };
                    serve_transaction(transaction, &incoming, timeout);
                }),
            )
            .map_err(OptimisticCoordinatorError::SnapshotGet)?;
        Ok(PreparedTransactionThread {
            requests: Some(requests),
            opened: opened_reply,
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

    /// Hands read-only cleanup to the transaction worker without waiting for
    /// its local state transition. This is reserved for statement snapshots:
    /// they cannot have mutations or pessimistic locks to clean up.
    fn finish_detached(&mut self) {
        let Some(requests) = self.requests.take() else {
            return;
        };
        let _ = requests.send(TransactionRequest::FinishDetached);
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
fn serve_transaction<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    mut transaction: RealOptimisticTransaction<C, L, CapabilityTimestampSource<P>>,
    incoming: &Receiver<TransactionRequest>,
    timeout: Duration,
) {
    while let Ok(request) = incoming.recv() {
        // Minted per request, never once for the thread. `UnaryCallContext`
        // carries an ABSOLUTE deadline, so a single context made when the
        // transaction opened would charge every later statement — and the
        // commit — for the wall-clock time the client spent holding the
        // transaction, which is not work anything did.
        let call = UnaryCallContext::with_timeout(timeout);
        let call = &call;
        match request {
            TransactionRequest::Get {
                key,
                read_ts,
                reply,
            } => {
                let read_ts = read_ts.unwrap_or_else(|| transaction.start_ts());
                let answer = transaction
                    .snapshot_get_at(&key, read_ts, call)
                    .map(|result| result.value)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::BatchGet {
                keys,
                read_ts,
                reply,
            } => {
                let read_ts = read_ts.unwrap_or_else(|| transaction.start_ts());
                let answer = transaction
                    .snapshot_batch_get_at(&keys, read_ts, call)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::Scan {
                start,
                end,
                limit,
                read_ts,
                reply,
            } => {
                let read_ts = read_ts.unwrap_or_else(|| transaction.start_ts());
                let answer = transaction
                    .snapshot_scan_at(&start, &end, limit, read_ts, call)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::LockKeys { reply, .. } => {
                // Fail closed: an optimistic transaction detects conflicts at
                // COMMIT and has no locks to grant. Reaching this arm is a
                // session-layer wiring fault, not a client-visible condition.
                let _ = reply.send(LockKeysOutcome::TransactionError(LockSqlError {
                    code: 1105,
                    state: *b"HY000",
                    message: "a pessimistic lock requires a pessimistic transaction".to_owned(),
                }));
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
                let _ = reply.send(
                    transaction
                        .commit(mutations, &end_call)
                        .map_err(|error| error.to_string()),
                );
                return;
            }
            TransactionRequest::Finish { reply } => {
                let _ = reply.send(
                    transaction
                        .finish_without_writes()
                        .map(|_| ())
                        .map_err(|error| StorageError::Backend(error.to_string())),
                );
                return;
            }
            TransactionRequest::FinishDetached => {
                let _ = transaction.finish_without_writes();
                return;
            }
        }
    }
    let _ = transaction.finish_without_writes();
}

/// What one statement's lock acquisition came to -- the session layer's
/// half of Go's `handlePessimisticDML` protocol.
#[derive(Debug)]
pub enum LockKeysOutcome {
    /// Every key is locked at `for_update_ts`; the statement stands.
    Locked {
        /// The statement timestamp the locks carry.
        for_update_ts: u64,
    },
    /// The locks are HELD, but a newer committed version beat the statement
    /// (fair locking's `locked_with_conflict`, or a write conflict during
    /// acquisition). The statement's effects must be rolled back and the
    /// statement RE-EXECUTED reading at this advanced `for_update_ts` --
    /// Go's `handlePessimisticLockError` -> `UpdateForUpdateTS` -> rebuild.
    RetryStatement {
        /// The advanced statement timestamp the retry reads at.
        for_update_ts: u64,
    },
    /// The statement fails with this error and its locks are released; the
    /// transaction stays open (Go's statement-scoped 1205/1213 family).
    StatementError(LockSqlError),
    /// The transaction itself is no longer usable.
    TransactionError(LockSqlError),
}

/// Serves one pessimistic explicit transaction on its own thread: the same
/// read/commit/finish protocol as [`serve_transaction`], plus
/// [`TransactionRequest::LockKeys`], the statement-lock step Go's
/// `handlePessimisticDML` runs after each DML.
fn serve_pessimistic_transaction<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    mut transaction: RealPessimisticTransaction<C, L, CapabilityTimestampSource<P>>,
    opener: &Arc<RealOptimisticTransactionOpener<C, L, P>>,
    incoming: &Receiver<TransactionRequest>,
    timeout: Duration,
) {
    // Refreshes the primary lock's TTL from the first lock on; `None` until
    // one exists. Ending the transaction drops it, which stops the heartbeat.
    let mut keep_alive: Option<LockKeepAlive> = None;
    while let Ok(request) = incoming.recv() {
        let call = UnaryCallContext::with_timeout(timeout);
        let call = &call;
        match request {
            TransactionRequest::Get {
                key,
                read_ts,
                reply,
            } => {
                let read_ts = read_ts.unwrap_or_else(|| transaction.start_ts());
                let answer = transaction
                    .snapshot()
                    .snapshot_get_at(&key, read_ts, call)
                    .map(|result| result.value)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::BatchGet {
                keys,
                read_ts,
                reply,
            } => {
                let read_ts = read_ts.unwrap_or_else(|| transaction.start_ts());
                let answer = transaction
                    .snapshot()
                    .snapshot_batch_get_at(&keys, read_ts, call)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::Scan {
                start,
                end,
                limit,
                read_ts,
                reply,
            } => {
                let read_ts = read_ts.unwrap_or_else(|| transaction.start_ts());
                let answer = transaction
                    .snapshot()
                    .snapshot_scan_at(&start, &end, limit, read_ts, call)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::LockKeys { keys, reply } => {
                let outcome =
                    acquire_statement_locks(&mut transaction, opener, &mut keep_alive, &keys, call);
                let fatal = matches!(outcome, LockKeysOutcome::TransactionError(_));
                let _ = reply.send(outcome);
                if fatal {
                    // The transaction is unusable; release what it holds and
                    // end truthfully, exactly as the Finish arm would.
                    let held = transaction.locked_keys();
                    let end_call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
                    let _ = transaction.pessimistic_rollback(&held, &end_call);
                    let _ = transaction.into_two_pc().finish_without_writes();
                    return;
                }
            }
            TransactionRequest::Commit { mutations, reply } => {
                let end_call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
                let _ = reply.send(
                    transaction
                        .commit(mutations, &end_call)
                        .map_err(|error| error.to_string()),
                );
                return;
            }
            TransactionRequest::Finish { reply } => {
                // A pessimistic transaction that publishes nothing still owes
                // its locks back -- `into_two_pc` documents exactly this
                // order.
                let held = transaction.locked_keys();
                let end_call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
                let rolled_back = transaction
                    .pessimistic_rollback(&held, &end_call)
                    .map_err(|cause| StorageError::Backend(cause.to_string()));
                let finished = transaction
                    .into_two_pc()
                    .finish_without_writes()
                    .map(|_| ())
                    .map_err(|error| StorageError::Backend(error.to_string()));
                let _ = reply.send(rolled_back.and(finished));
                return;
            }
            TransactionRequest::FinishDetached => {
                let held = transaction.locked_keys();
                let end_call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
                let _ = transaction.pessimistic_rollback(&held, &end_call);
                let _ = transaction.into_two_pc().finish_without_writes();
                return;
            }
        }
    }
    let held = transaction.locked_keys();
    let end_call = UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT);
    let _ = transaction.pessimistic_rollback(&held, &end_call);
    let _ = transaction.into_two_pc().finish_without_writes();
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
/// * an index-key put locks only when the entry is UNIQUE
///   (`tablecodec.IndexKVIsUnique`); this tier's non-unique entry value is
///   the single byte `'0'` ([`tidb_codec::table_key::non_unique_index_value`])
///   while a unique entry carries the row's handle, so the length is the
///   same discriminator Go reads.
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
/// [`pessimistic_lock_delta`]'s doc describes.
fn statement_key_needs_lock(key: &[u8], value: Option<&[u8]>) -> bool {
    let is_record = match tidb_codec::table_key::decode_key_head(key) {
        Ok(tidb_codec::table_key::KeyHead::Record { .. }) => true,
        Ok(tidb_codec::table_key::KeyHead::Index { .. }) => false,
        // Not a table key: Go's "meta key always need to lock".
        Err(_) => return true,
    };
    match value {
        None => is_record,
        Some(_) if is_record => true,
        Some(value) => value.len() != 1,
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
    keys: &[Vec<u8>],
    call: &UnaryCallContext,
) -> LockKeysOutcome {
    use std::collections::BTreeSet;
    let held: BTreeSet<Vec<u8>> = transaction.locked_keys().into_iter().collect();
    let added: Vec<Vec<u8>> = keys
        .iter()
        .filter(|key| !held.contains(*key))
        .cloned()
        .collect();
    /// Bound on deadlock-retryable re-acquisitions, the narrow driver's own.
    const MAX_LOCK_RETRIES: usize = 8;
    let mut attempt = 0usize;
    loop {
        // No absence presumption: DML locks target rows that exist (the
        // rewritten set); INSERT keeps its NotExist assertion at Prewrite.
        match transaction.acquire_locks(
            keys,
            &BTreeSet::new(),
            LockWaitTime::session_lock_wait_timeout(),
            call,
        ) {
            Ok(acquired) => {
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
                    };
                }
                // Fair locking granted the locks despite a newer committed
                // version. The locks STAY -- that is the point -- but the
                // statement must be recomputed at a timestamp that sees it.
                return match transaction.advance_for_update_ts() {
                    Ok(for_update_ts) => LockKeysOutcome::RetryStatement { for_update_ts },
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
                // locks survive their successor's failure.
                if let Err(cause) = transaction.pessimistic_rollback(&added, call) {
                    return LockKeysOutcome::TransactionError(transaction_cause_to_sql_error(
                        &cause,
                    ));
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
                    // newer `for_update_ts`.
                    _ => {
                        return match transaction.advance_for_update_ts() {
                            Ok(for_update_ts) => LockKeysOutcome::RetryStatement { for_update_ts },
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
/// timestamp, owned by the thread that opened it.
///
/// This is the autocommit shape. Inside an explicit transaction the session
/// reads through [`SessionTransaction::snapshot`] instead, so every statement
/// shares the one timestamp `BEGIN` took.
pub struct StatementSnapshot {
    thread: TransactionThread,
}

/// One statement snapshot whose ordinary PD timestamp request is in flight.
///
/// Unlike [`StatementSnapshot`], this owns no pinned transaction worker. Go's
/// warmup stores only an oracle future; the worker-local transaction is opened
/// when [`Self::wait`] activates the snapshot for the first storage read.
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
    /// Waits for the transaction prepared after planning.
    pub fn wait(self) -> Result<StatementSnapshot, OptimisticCoordinatorError> {
        let start_ts = self
            .start_ts
            .wait()
            .map_err(|error| OptimisticCoordinatorError::Timestamp(error.to_string()))?;
        Ok(StatementSnapshot {
            thread: TransactionThread::open_with(
                &self.opener,
                self.timeout,
                TransactionOpen::ReadOnlyAt(start_ts),
                "cluster-statement-snapshot",
            )?,
        })
    }
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
    /// Starts fetching one ordinary read-only transaction's PD timestamp
    /// without opening its worker-local transaction.
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

    /// Opens one read-only transaction on its own thread, spending exactly one
    /// PD timestamp.
    pub fn open<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Ok(Self {
            thread: TransactionThread::open(&opener, timeout, false, "cluster-statement-snapshot")?,
        })
    }

    /// The timestamp every read of this statement is served at.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.thread.start_ts
    }

    /// Ends the statement's read transaction, leaving no locks behind.
    ///
    /// Calling it twice is a no-op: the statement is already finished.
    pub fn finish(&mut self) -> Result<(), StorageError> {
        self.thread.finish()
    }
}

impl Drop for StatementSnapshot {
    fn drop(&mut self) {
        // Go abandons an autocommit read snapshot after the statement: there
        // are no writes or locks whose cleanup the foreground must observe.
        // Keep the worker-owned transaction lifecycle ordered, but do not put
        // the next statement behind its local read-only state transition.
        self.thread.finish_detached();
    }
}

impl ClusterSnapshot for StatementSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        let bytes = key.as_bytes().to_vec();
        ask(&self.thread.sender()?, |reply| TransactionRequest::Get {
            key: bytes,
            read_ts: None,
            reply,
        })
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        let keys = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        ask(&self.thread.sender()?, |reply| {
            TransactionRequest::BatchGet {
                keys,
                read_ts: None,
                reply,
            }
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
            read_ts: None,
            reply,
        })
    }

    fn start_ts(&self) -> u64 {
        self.thread.start_ts
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
        _start: &Key,
        _end: &Key,
        _limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.consume()?;
        Err(StorageError::Backend(
            "a MaxTS point snapshot cannot serve a range scan".to_owned(),
        ))
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
pub struct SessionTransaction {
    thread: TransactionThread,
    /// Whether the worker hosts a pessimistic transaction -- decided by the
    /// session's `tidb_txn_mode` at `BEGIN`, Go's `DefTiDBTxnMode`
    /// (pessimistic) being the default.
    pessimistic: bool,
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
    pub fn begin<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Ok(Self {
            thread: TransactionThread::open(&opener, timeout, true, "cluster-session-transaction")?,
            pessimistic: false,
        })
    }

    /// Opens the pessimistic transaction `BEGIN` holds under Go's default
    /// `tidb_txn_mode = 'pessimistic'`: the same one-timestamp transaction,
    /// whose worker additionally serves the statement-lock protocol
    /// ([`Self::lock_keys`]) and commits with pessimistic constraints.
    pub fn begin_pessimistic<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Ok(Self {
            thread: TransactionThread::open_with(
                &opener,
                timeout,
                TransactionOpen::WritablePessimistic,
                "cluster-session-pessimistic",
            )?,
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
        let requests = self.thread.sender()?;
        let (reply, answer) = mpsc::channel();
        requests
            .send(TransactionRequest::LockKeys { keys, reply })
            .map_err(|_| StorageError::Backend("the transaction thread is gone".to_owned()))?;
        answer.recv().map_err(|_| {
            StorageError::Backend("the transaction thread stopped mid-lock".to_owned())
        })
    }

    /// The one timestamp every statement of this transaction reads at.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.thread.start_ts
    }

    /// A read handle onto this transaction, for one statement to bind.
    ///
    /// Dropping it ends the statement, not the transaction: that is the
    /// re-entry the shape exists for.
    pub fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, StorageError> {
        Ok(Box::new(SessionSnapshot {
            requests: self.thread.sender()?,
            start_ts: self.thread.start_ts,
            read_ts: None,
        }))
    }

    /// A read handle whose reads happen at `read_ts` instead of `start_ts`:
    /// the retried pessimistic statement's view (Go rebuilds the retried
    /// executor reading at `forUpdateTS`).
    pub fn snapshot_at(&self, read_ts: u64) -> Result<Box<dyn ClusterSnapshot>, StorageError> {
        Ok(Box::new(SessionSnapshot {
            requests: self.thread.sender()?,
            start_ts: self.thread.start_ts,
            read_ts: Some(read_ts),
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
    /// `Some` overrides the read timestamp for this statement -- the
    /// pessimistic retry's advanced `for_update_ts`. `None` reads at
    /// `start_ts`.
    read_ts: Option<u64>,
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
        let read_ts = self.read_ts;
        ask(&self.requests, |reply| TransactionRequest::Get {
            key: bytes,
            read_ts,
            reply,
        })
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        let keys = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let read_ts = self.read_ts;
        ask(&self.requests, |reply| TransactionRequest::BatchGet {
            keys,
            read_ts,
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
        let read_ts = self.read_ts;
        ask(&self.requests, |reply| TransactionRequest::Scan {
            start,
            end,
            limit,
            read_ts,
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
pub fn statement_storage<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
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
pub fn commit_staged_buffer<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
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
    use std::sync::mpsc::RecvTimeoutError;
    use std::thread;

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
    fn dropping_a_statement_snapshot_does_not_wait_for_worker_cleanup() {
        let (requests, incoming) = mpsc::channel();
        let (dropped, drop_finished) = mpsc::channel();
        let dropper = thread::spawn(move || {
            drop(StatementSnapshot {
                thread: TransactionThread {
                    requests: Some(requests),
                    start_ts: 42,
                },
            });
            dropped.send(()).expect("report snapshot drop");
        });

        let synchronous_reply = match incoming
            .recv_timeout(Duration::from_secs(1))
            .expect("snapshot drop must ask the worker to finish")
        {
            TransactionRequest::Finish { reply } => Some(reply),
            TransactionRequest::FinishDetached => None,
            _ => panic!("snapshot drop sent a non-finish request"),
        };
        let returned_without_cleanup = match drop_finished.recv_timeout(Duration::from_millis(50)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("snapshot dropper stopped unexpectedly"),
        };

        if let Some(reply) = synchronous_reply {
            reply
                .send(Ok(()))
                .expect("release synchronous snapshot drop");
        }
        dropper.join().expect("snapshot dropper");
        assert!(
            returned_without_cleanup,
            "read-only statement cleanup blocked the foreground thread"
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
}
