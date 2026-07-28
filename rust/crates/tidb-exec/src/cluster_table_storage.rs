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
//! One statement opens one read-only transaction, reads every key it needs at
//! that transaction's single `start_ts`, and finishes without writes; the
//! statement's writes stay in the session's [`MutationBuffer`]. COMMIT opens a
//! second, writing transaction and publishes the whole buffer as one mutation
//! set.
//!
//! That is Go's `MemBuffer`-in-front-of-snapshot read path exactly, with one
//! honest divergence: Go's session keeps *one* `kv.Transaction`, so its writes
//! prewrite at the same `start_ts` its reads used, and a concurrent writer is
//! caught by the write conflict check against that `start_ts`. Here the COMMIT
//! timestamp is newer than the statements' read timestamps, so a
//! read-then-write race is caught only by the mutation assertions, not by
//! `start_ts` conflict detection. Reads within a statement are still one
//! consistent snapshot, which is what the wide-SQL driver depends on. Closing
//! the gap means holding one transaction open across the whole session, which
//! the coordinator's state machine does not admit today: `commit` and
//! `finish_without_writes` both consume the transaction and reach a terminal
//! state that cannot re-enter `Reading`.
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
    ProductionOptimisticTransaction, RealOptimisticTransactionOpener,
};
use tidb_txnkv::Key;

/// One statement's read snapshot: a real read-only transaction at one PD
/// timestamp, owned by the thread that opened it.
///
/// The production transport is deliberately worker-local (`Rc<RefCell<..>>`),
/// while `TableStorage` is `Send` because a `KvTable` lives in a catalog the
/// server shares between workers. Both constraints hold at once here: the
/// transaction is created, used and finished on one dedicated thread, and what
/// crosses threads is this handle -- a channel and a timestamp. No borrow of
/// the transport ever leaves its thread.
pub struct StatementSnapshot {
    requests: Option<Sender<SnapshotRequest>>,
    worker: Option<JoinHandle<()>>,
    start_ts: u64,
}

/// One read the snapshot's thread performs, with the channel its answer goes
/// back on.
enum SnapshotRequest {
    Get {
        key: Vec<u8>,
        reply: Sender<Result<Option<Vec<u8>>, StorageError>>,
    },
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        reply: Sender<Result<SnapshotPairs, StorageError>>,
    },
    Finish {
        reply: Sender<Result<(), StorageError>>,
    },
}

impl fmt::Debug for StatementSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatementSnapshot")
            .field("start_ts", &self.start_ts)
            .field("open", &self.requests.is_some())
            .finish()
    }
}

impl StatementSnapshot {
    /// Opens one read-only transaction on its own thread, spending exactly one
    /// PD timestamp.
    ///
    /// The call returns only once the transaction exists, so `start_ts` is an
    /// allocated timestamp rather than a promise.
    pub fn open(
        opener: Arc<RealOptimisticTransactionOpener>,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let (requests, incoming) = mpsc::channel::<SnapshotRequest>();
        let (opened, opened_reply) = mpsc::channel::<Result<u64, OptimisticCoordinatorError>>();
        let worker = thread::Builder::new()
            .name("cluster-statement-snapshot".to_owned())
            .spawn(move || {
                let mut transaction = match opener.begin_read_only() {
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
                serve_snapshot(&mut transaction, &incoming, &call);
                let _ = transaction.finish_without_writes();
            })
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let start_ts = opened_reply
            .recv()
            .map_err(|_| {
                OptimisticCoordinatorError::SnapshotGet(
                    "the snapshot thread ended before opening a transaction".to_owned(),
                )
            })
            .and_then(|result| result)?;
        Ok(StatementSnapshot {
            requests: Some(requests),
            worker: Some(worker),
            start_ts,
        })
    }

    /// The timestamp every read of this statement is served at.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// Ends the statement's read transaction, leaving no locks behind.
    ///
    /// Calling it twice is a no-op: the statement is already finished.
    pub fn finish(&mut self) -> Result<(), StorageError> {
        let Some(requests) = self.requests.take() else {
            return Ok(());
        };
        let (reply, answer) = mpsc::channel();
        let outcome = match requests.send(SnapshotRequest::Finish { reply }) {
            Ok(()) => answer.recv().unwrap_or(Ok(())),
            // The thread is already gone, which means it already finished the
            // transaction on its way out.
            Err(_) => Ok(()),
        };
        drop(requests);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        outcome
    }

    fn ask<T>(
        &self,
        request: impl FnOnce(Sender<Result<T, StorageError>>) -> SnapshotRequest,
    ) -> Result<T, StorageError> {
        let requests = self.requests.as_ref().ok_or_else(|| {
            StorageError::Backend("the statement's read snapshot is already finished".to_owned())
        })?;
        let (reply, answer) = mpsc::channel();
        requests.send(request(reply)).map_err(|_| {
            StorageError::Backend("the statement's snapshot thread is gone".to_owned())
        })?;
        answer.recv().map_err(|_| {
            StorageError::Backend("the statement's snapshot thread stopped mid-read".to_owned())
        })?
    }
}

impl Drop for StatementSnapshot {
    fn drop(&mut self) {
        // Dropping the request channel is what tells the thread to finish the
        // transaction; joining orders that cleanup before the handle's owner
        // moves on.
        self.requests = None;
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Serves reads on the transaction's own thread until the handle goes away.
fn serve_snapshot(
    transaction: &mut ProductionOptimisticTransaction,
    incoming: &Receiver<SnapshotRequest>,
    call: &UnaryCallContext,
) {
    while let Ok(request) = incoming.recv() {
        match request {
            SnapshotRequest::Get { key, reply } => {
                let answer = transaction
                    .snapshot_get(&key, call)
                    .map(|result| result.value)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            SnapshotRequest::Scan { start, end, reply } => {
                let answer = transaction
                    .snapshot_scan(&start, &end, call)
                    .map_err(classify);
                let _ = reply.send(answer);
            }
            SnapshotRequest::Finish { reply } => {
                // The caller's return path finishes the transaction; the
                // acknowledgement here is what makes `finish` synchronous.
                let _ = reply.send(Ok(()));
                return;
            }
        }
    }
}

impl ClusterSnapshot for StatementSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        let bytes = key.as_bytes().to_vec();
        self.ask(|reply| SnapshotRequest::Get { key: bytes, reply })
    }

    fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
        let start = start.as_bytes().to_vec();
        let end = end.as_bytes().to_vec();
        self.ask(|reply| SnapshotRequest::Scan { start, end, reply })
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

/// Publishes every staged write of the session as one optimistic transaction.
///
/// The mutations carry no existence assertion (`Op_Put`/`Op_Del` only): the
/// buffer holds raw row and index keys whose prior state the storage seam does
/// not record, and asserting the wrong one would fail a correct commit. An
/// empty buffer commits nothing and consumes no timestamp, as Go's COMMIT of a
/// transaction that wrote nothing does.
pub fn commit_staged_buffer(
    opener: &RealOptimisticTransactionOpener,
    buffer: &MutationBuffer,
    timeout: Duration,
) -> Result<Option<OptimisticCommitOutcome>, OptimisticCoordinatorError> {
    let staged = buffer.staged();
    if staged.is_empty() {
        return Ok(None);
    }
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
    let transaction = opener.begin(mutations.len(), planned_bytes)?;
    let call = UnaryCallContext::with_timeout(timeout);
    let outcome = transaction.commit(mutations, &call)?;
    buffer.reset();
    Ok(Some(outcome))
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
