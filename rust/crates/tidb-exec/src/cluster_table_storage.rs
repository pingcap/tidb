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
    OptimisticCommitOutcome, OptimisticCoordinatorError, OptimisticMutation,
    RealOptimisticTransaction, RealOptimisticTransactionOpener, StorePdCapability,
    StoreWriteClient, StoreWriteLoader, MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::Key;
use tidb_txnkv::PdRegionLoader;

use crate::pessimistic_lock_error::{commit_outcome_to_sql_error, LockSqlError};
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
                    let begun = match open {
                        TransactionOpen::Writable => {
                            opener.begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
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
            TransactionRequest::Get { key, reply } => {
                let answer = transaction
                    .snapshot_get(&key, call)
                    .map(|result| result.value)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            TransactionRequest::Scan {
                start,
                end,
                limit,
                reply,
            } => {
                let answer = transaction
                    .snapshot_scan(&start, &end, limit, call)
                    .map_err(classify);
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

impl fmt::Debug for MaxTsSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaxTsSnapshot")
            .field("consumed", &self.consumed)
            .finish_non_exhaustive()
    }
}

impl ClusterSnapshot for MaxTsSnapshot {
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
