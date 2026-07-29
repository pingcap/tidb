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
//! Each autocommit statement therefore gets its own fresh timestamp, which is
//! what Go's autocommit does too: `BEGIN` is implicit and ends with the
//! statement.
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
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
};
use tidb_executor::storage::StorageError;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticCoordinatorError, OptimisticMutation,
    ProductionOptimisticTransaction, RealOptimisticTransactionOpener, MAX_OPTIMISTIC_MUTATIONS,
    MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::Key;

use crate::pessimistic_lock_error::{commit_outcome_to_sql_error, LockSqlError};

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
        reply: Sender<Result<SnapshotPairs, StorageError>>,
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

/// One real transaction pinned to the thread that opened it.
///
/// The production transport is deliberately worker-local (`Rc<RefCell<..>>`),
/// while `TableStorage` is `Send` because a `KvTable` lives in a catalog the
/// server shares between workers. Both constraints hold at once here: the
/// transaction is created, used and ended on one dedicated thread, and what
/// crosses threads is this handle -- a channel and a timestamp. No borrow of
/// the transport ever leaves its thread.
struct TransactionThread {
    requests: Option<Sender<TransactionRequest>>,
    worker: Option<JoinHandle<()>>,
    start_ts: u64,
}

impl TransactionThread {
    /// Opens one transaction on its own thread, spending exactly one PD
    /// timestamp.
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
        let (requests, incoming) = mpsc::channel::<TransactionRequest>();
        let (opened, opened_reply) = mpsc::channel::<Result<u64, OptimisticCoordinatorError>>();
        let opener = Arc::clone(opener);
        let worker = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let begun = if writable {
                    opener.begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
                } else {
                    opener.begin_read_only()
                };
                let transaction = match begun {
                    Ok(transaction) => {
                        // A caller that stopped waiting leaves no lock behind:
                        // the transaction ends here instead.
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
                let call = UnaryCallContext::with_timeout(timeout);
                serve_transaction(transaction, &incoming, &call);
            })
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
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
            worker: Some(worker),
            start_ts,
        })
    }

    /// Ends the transaction without publishing anything, leaving no locks
    /// behind. Calling it twice is a no-op.
    fn finish(&mut self) -> Result<(), StorageError> {
        let Some(requests) = self.requests.take() else {
            return Ok(());
        };
        let (reply, answer) = mpsc::channel();
        let outcome = match requests.send(TransactionRequest::Finish { reply }) {
            Ok(()) => answer.recv().unwrap_or(Ok(())),
            // The thread is already gone, which means it already finished the
            // transaction on its way out.
            Err(_) => Ok(()),
        };
        drop(requests);
        self.join();
        outcome
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
        let outcome = match requests.send(TransactionRequest::Commit { mutations, reply }) {
            Ok(()) => answer
                .recv()
                .unwrap_or_else(|_| Err("the transaction thread stopped mid-commit".to_owned())),
            Err(_) => Err("the transaction thread is gone".to_owned()),
        };
        drop(requests);
        self.join();
        outcome
    }

    fn join(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
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
        // Dropping the request channel is what tells the thread to finish the
        // transaction; joining orders that cleanup before the handle's owner
        // moves on.
        self.requests = None;
        self.join();
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
    mut transaction: ProductionOptimisticTransaction,
    incoming: &Receiver<TransactionRequest>,
    call: &UnaryCallContext,
) {
    while let Ok(request) = incoming.recv() {
        match request {
            TransactionRequest::Get { key, reply } => {
                let answer = transaction
                    .snapshot_get(&key, call)
                    .map(|result| result.value)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::Scan { start, end, reply } => {
                let answer = transaction
                    .snapshot_scan(&start, &end, call)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::Commit { mutations, reply } => {
                // The coordinator re-enters the write phase from the read
                // phase, so this prewrite carries the transaction's original
                // start timestamp -- the whole point of holding one open.
                let _ = reply.send(
                    transaction
                        .commit(mutations, call)
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
        }
    }
    let _ = transaction.finish_without_writes();
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

impl ClusterSnapshot for StatementSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        let bytes = key.as_bytes().to_vec();
        ask(&self.thread.sender()?, |reply| TransactionRequest::Get {
            key: bytes,
            reply,
        })
    }

    fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
        let start = start.as_bytes().to_vec();
        let end = end.as_bytes().to_vec();
        ask(&self.thread.sender()?, |reply| TransactionRequest::Scan {
            start,
            end,
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
        Ok(Self {
            thread: TransactionThread::open(&opener, timeout, true, "cluster-session-transaction")?,
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
        mut self,
        buffer: &MutationBuffer,
    ) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
        let (mutations, _) = staged_mutations(buffer).map_err(coordinator_sql_error)?;
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

    fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
        let start = start.as_bytes().to_vec();
        let end = end.as_bytes().to_vec();
        ask(&self.requests, |reply| TransactionRequest::Scan {
            start,
            end,
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
/// optimistic transaction.
///
/// This is the autocommit path only: the statement's own read transaction has
/// already ended, so the publication takes a fresh timestamp, exactly as Go's
/// implicit per-statement transaction does. Inside `BEGIN` ... `COMMIT` the
/// publication goes through [`SessionTransaction::commit`] instead, at the
/// timestamp `BEGIN` took. An empty buffer commits nothing and consumes no
/// timestamp.
pub fn commit_staged_buffer(
    opener: &RealOptimisticTransactionOpener,
    buffer: &MutationBuffer,
    timeout: Duration,
) -> Result<Option<OptimisticCommitOutcome>, LockSqlError> {
    let (mutations, planned_bytes) = staged_mutations(buffer).map_err(coordinator_sql_error)?;
    if mutations.is_empty() {
        return Ok(None);
    }
    let transaction = opener
        .begin(mutations.len(), planned_bytes)
        .map_err(coordinator_sql_error)?;
    let call = UnaryCallContext::with_timeout(timeout);
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
}
