//! The committed cluster this node's statements are tested against: the
//! stored rows, the timestamp source, the snapshots a statement binds, and
//! the optimistic 2PC a COMMIT publishes through.
//!
//! Stands in for the tiers under
//! [`ClusterTransactions`](crate::cluster_session_node::ClusterTransactions) --
//! PD's timestamp oracle and TiKV's write-conflict rule in miniature -- so a
//! test can assert what Go's `pkg/session/session.go` transaction lifecycle
//! guarantees without a cluster. Every counter here exists because some
//! failure is only visible as a count: a leaked read handle, a second
//! publication, a snapshot taken twice.

use super::super::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tidb_exec::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_executor::cluster_storage::SnapshotPairs;
use tidb_executor::storage::StorageError;
use tidb_txnkv::transaction::{
    CommittedTransaction, OptimisticCommitOutcome, OptimisticTransactionReceipt,
    RolledBackTransaction, TransactionCause,
};
use tidb_txnkv::Key;

/// The committed cluster: what a statement's snapshot reads and what a
/// COMMIT publishes into. Nothing a statement stages may appear here
/// before its transaction commits, which is what most of these tests
/// assert.
#[derive(Debug, Default)]
pub(super) struct MockCluster {
    pub(super) committed: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    /// The timestamp of the last commit that touched each key, which is
    /// what a prewrite at `start_ts` is checked against -- TiKV's own
    /// write-conflict rule in miniature.
    pub(super) versions: Mutex<BTreeMap<Vec<u8>, u64>>,
    /// Stands in for PD: every transaction and every commit takes one.
    pub(super) clock: AtomicU64,
    /// Autocommit read transactions opened, so "one statement, one
    /// snapshot" stays countable.
    pub(super) opened: AtomicUsize,
    /// Autocommit read transactions opened at `u64::MAX` -- the ones that
    /// spent no timestamp. Counted apart from `opened` so a pin can say
    /// which branch a statement took, not merely that it read once.
    pub(super) opened_at_max_ts: AtomicUsize,
    /// Explicit transactions opened by `BEGIN`.
    pub(super) begun: AtomicUsize,
    /// Read handles still bound. A statement that leaks one leaves this
    /// above zero, which is the lock-left-behind failure in miniature.
    pub(super) live: AtomicUsize,
    /// Publications that actually carried mutations.
    pub(super) publications: AtomicUsize,
    pub(super) fail_commit: AtomicBool,
}

impl MockCluster {
    pub(super) fn rows(&self) -> usize {
        self.committed.lock().expect("committed").len()
    }

    pub(super) fn timestamp(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(super) fn snapshot(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        self.committed.lock().expect("committed").clone()
    }

    /// Publishes `staged` at `commit_ts`, refusing any key another
    /// transaction committed after `start_ts`.
    ///
    /// A refusal is returned the way the real coordinator returns one: as
    /// an `Ok` outcome carrying its cause, not as an `Err`. That is the
    /// shape a caller can mistake for success, so the mock reproduces it
    /// and lets the production classifier decide what the client is told.
    pub(super) fn publish(
        self: &Arc<Self>,
        staged: Vec<(Key, Option<Vec<u8>>)>,
        start_ts: u64,
    ) -> OptimisticCommitOutcome {
        let receipt = OptimisticTransactionReceipt::new(1, start_ts, b"primary".to_vec(), 1);
        let rolled_back = |cause| {
            OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
                receipt: OptimisticTransactionReceipt::new(1, start_ts, b"primary".to_vec(), 1),
                cause,
            })
        };
        if self.fail_commit.load(Ordering::Acquire) {
            return rolled_back(TransactionCause::Transport {
                detail: "the mock cluster refused this publication".to_owned(),
            });
        }
        let mut versions = self.versions.lock().expect("versions");
        for (key, _) in &staged {
            if versions
                .get(key.as_bytes())
                .is_some_and(|last| *last > start_ts)
            {
                return rolled_back(TransactionCause::WriteConflict {
                    detail: format!("txnStartTS={start_ts}"),
                });
            }
        }
        let commit_ts = self.timestamp();
        let mut committed = self.committed.lock().expect("committed");
        for (key, value) in staged {
            versions.insert(key.as_bytes().to_vec(), commit_ts);
            match value {
                Some(value) => committed.insert(key.into_bytes(), value),
                None => committed.remove(key.as_bytes()),
            };
        }
        drop(committed);
        drop(versions);
        self.publications.fetch_add(1, Ordering::AcqRel);
        OptimisticCommitOutcome::Committed(CommittedTransaction {
            receipt,
            secondary_failures: Vec::new(),
        })
    }
}

#[derive(Debug)]
pub(super) struct MockSnapshot {
    pub(super) data: BTreeMap<Vec<u8>, Vec<u8>>,
    pub(super) cluster: Arc<MockCluster>,
}

impl Drop for MockSnapshot {
    fn drop(&mut self) {
        self.cluster.live.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ClusterSnapshot for MockSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.data.get(key.as_bytes()).cloned())
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        Ok(self
            .data
            .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
            .take(limit.unwrap_or(usize::MAX))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

/// The transaction tier the session holds: an `Arc` so the test keeps its
/// own view of the committed store while the session writes through it.
#[derive(Debug)]
pub(super) struct MockTransactions(pub(super) Arc<MockCluster>);

impl ClusterTransactions for MockTransactions {
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.0.opened.fetch_add(1, Ordering::AcqRel);
        self.0.live.fetch_add(1, Ordering::AcqRel);
        let _ = self.0.timestamp();
        Ok(Box::new(MockSnapshot {
            data: self.0.snapshot(),
            cluster: Arc::clone(&self.0),
        }))
    }

    fn open_max_ts_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.0.opened_at_max_ts.fetch_add(1, Ordering::AcqRel);
        self.0.live.fetch_add(1, Ordering::AcqRel);
        // No `timestamp()` call: that absence IS what this branch buys, and
        // the clock the tests read is what proves it.
        Ok(Box::new(MockSnapshot {
            data: self.0.snapshot(),
            cluster: Arc::clone(&self.0),
        }))
    }

    fn commit(&self, buffer: &MutationBuffer) -> Result<(), SqlQueryError> {
        let staged = buffer.staged();
        if staged.is_empty() {
            return Ok(());
        }
        // Autocommit publishes at a fresh timestamp, so nothing committed
        // before it can conflict -- exactly what an implicit
        // single-statement transaction does.
        let start_ts = self.0.timestamp();
        let outcome = self.0.publish(staged, start_ts);
        commit_outcome_to_sql_error(&outcome).map_err(sql_error)?;
        buffer.reset();
        Ok(())
    }

    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String> {
        self.0.begun.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(MockSessionTransaction {
            start_ts: self.0.timestamp(),
            data: self.0.snapshot(),
            cluster: Arc::clone(&self.0),
        }))
    }
}

/// One `BEGIN` ... `COMMIT` over the mock cluster: the rows it saw at
/// `start_ts`, served to every statement, and a publication checked against
/// that same `start_ts`.
#[derive(Debug)]
pub(super) struct MockSessionTransaction {
    pub(super) start_ts: u64,
    pub(super) data: BTreeMap<Vec<u8>, Vec<u8>>,
    pub(super) cluster: Arc<MockCluster>,
}

impl OpenClusterTransaction for MockSessionTransaction {
    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.cluster.live.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(MockSnapshot {
            data: self.data.clone(),
            cluster: Arc::clone(&self.cluster),
        }))
    }

    fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), SqlQueryError> {
        let staged = buffer.staged();
        if staged.is_empty() {
            return Ok(());
        }
        let outcome = self.cluster.publish(staged, self.start_ts);
        commit_outcome_to_sql_error(&outcome).map_err(sql_error)?;
        buffer.reset();
        Ok(())
    }

    fn rollback(self: Box<Self>) -> Result<(), String> {
        Ok(())
    }
}
