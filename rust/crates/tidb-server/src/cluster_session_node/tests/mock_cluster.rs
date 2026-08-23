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

use super::super::transactions::PendingClusterSnapshot;
use super::super::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tidb_exec::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_executor::cluster_storage::SnapshotPairs;
use tidb_executor::storage::StorageError;
use tidb_txnkv::region::RegionBackoffKind;
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
    pub(super) advisory_locks: tidb_executor::advisory_lock_state::LocalAdvisoryLockService,
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
    /// Ordinary autocommit snapshot futures started after planning. A future
    /// spends a timestamp immediately but becomes an open snapshot only when a
    /// statement read waits for it.
    pub(super) prepared: AtomicUsize,
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
    /// One-shot failure carried by the next ordinary snapshot future. Merely
    /// preparing and dropping it is harmless; waiting for a read reports it.
    pub(super) fail_next_prepared_snapshot: AtomicBool,
    /// One-shot explicit `tikv:9005` at the SQL commit boundary. Unlike a
    /// write conflict, Go reports this error without replaying the statement.
    pub(super) fail_next_region_commit: AtomicBool,
    /// One-shot untyped region diagnostic returned by the transaction
    /// coordinator. Go preserves its detail as an ordinary error rather than
    /// manufacturing the explicit client-go 9005 sentinel.
    pub(super) fail_next_generic_region_commit: AtomicBool,
    /// One-shot: the NEXT autocommit read snapshot opened is followed
    /// immediately by another session's commit of the row that already exists.
    ///
    /// That is the window the lost-update bug lived in, and the only way to
    /// express it: the racing commit has to land strictly after the reading
    /// statement took its timestamp and strictly before that statement
    /// publishes. Nothing a test can do from SQL alone lands inside a single
    /// statement.
    pub(super) race_next_read: AtomicBool,
    /// Not one-shot: EVERY autocommit read snapshot is followed by another
    /// session's commit, so no replay can ever win.
    ///
    /// This is the only way to reach the exhaustion contract. An autocommit
    /// statement now retries a write conflict, so a one-shot race proves the
    /// retry and this proves its BOUND -- that the budget runs out and the
    /// client is told, rather than the node spinning forever against a key it
    /// will never get.
    pub(super) race_every_read: AtomicBool,
}

impl MockCluster {
    pub(super) fn rows(&self) -> usize {
        self.committed.lock().expect("committed").len()
    }

    pub(super) fn timestamp(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Commits, as some other session, a new value for whatever single row the
    /// store already holds, at a timestamp of its own.
    ///
    /// The bytes are the row's OWN, re-committed unchanged: what makes this a
    /// race is the new `commit_ts` on the key, not a new value. It has to stay
    /// a decodable row because a statement that retries the conflict READS it
    /// on its next attempt -- an undecodable value would end the replay with a
    /// decode error and hide whether the retry worked.
    pub(super) fn commit_from_another_session(self: &Arc<Self>) {
        let Some((key, value)) = self
            .committed
            .lock()
            .expect("committed")
            .iter()
            .next()
            .map(|(key, value)| (key.clone(), value.clone()))
        else {
            return;
        };
        let start_ts = self.timestamp();
        let outcome = self.publish(vec![(Key::from(key), Some(value))], start_ts);
        assert!(
            matches!(outcome, OptimisticCommitOutcome::Committed(_)),
            "the racing session's own commit must land: {outcome:?}"
        );
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
    /// The timestamp this read is served at, so a test can assert that the
    /// statement's publication carries the SAME one. The mock's stored bytes
    /// are a clone of the committed map and so cannot themselves express MVCC,
    /// but `versions` plus this number reproduce TiKV's conflict rule exactly,
    /// which is the half the lost-update bug lived in.
    pub(super) start_ts: u64,
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

    fn start_ts(&self) -> u64 {
        self.start_ts
    }
}

/// The transaction tier the session holds: an `Arc` so the test keeps its
/// own view of the committed store while the session writes through it.
#[derive(Debug)]
pub(super) struct MockTransactions(pub(super) Arc<MockCluster>);

struct MockPendingSnapshot {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    cluster: Arc<MockCluster>,
    start_ts: u64,
    failure: Option<String>,
}

impl PendingClusterSnapshot for MockPendingSnapshot {
    fn wait(self: Box<Self>) -> Result<Box<dyn ClusterSnapshot>, String> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        self.cluster.opened.fetch_add(1, Ordering::AcqRel);
        self.cluster.live.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(MockSnapshot {
            data: self.data,
            cluster: Arc::clone(&self.cluster),
            start_ts: self.start_ts,
        }))
    }
}

impl ClusterTransactions for MockTransactions {
    fn prepare_snapshot(&self) -> Result<Box<dyn PendingClusterSnapshot>, String> {
        self.0.prepared.fetch_add(1, Ordering::AcqRel);
        if self
            .0
            .fail_next_prepared_snapshot
            .swap(false, Ordering::AcqRel)
        {
            return Ok(Box::new(MockPendingSnapshot {
                data: BTreeMap::new(),
                cluster: Arc::clone(&self.0),
                start_ts: 0,
                failure: Some("mock prepared snapshot failed".to_owned()),
            }));
        }
        // Go's oracle future is requested after planning. Capture both the
        // timestamp and its MVCC image at that point; `wait` only exposes it.
        let data = self.0.snapshot();
        let start_ts = self.0.timestamp();
        if self.0.race_every_read.load(Ordering::Acquire)
            || self.0.race_next_read.swap(false, Ordering::AcqRel)
        {
            self.0.commit_from_another_session();
        }
        Ok(Box::new(MockPendingSnapshot {
            data,
            cluster: Arc::clone(&self.0),
            start_ts,
            failure: None,
        }))
    }

    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.0.opened.fetch_add(1, Ordering::AcqRel);
        self.0.live.fetch_add(1, Ordering::AcqRel);
        // The order is the whole point: this statement's rows and its
        // timestamp are taken FIRST, and only then does the racing session
        // commit -- so the race lands inside the statement.
        let data = self.0.snapshot();
        let start_ts = self.0.timestamp();
        if self.0.race_every_read.load(Ordering::Acquire)
            || self.0.race_next_read.swap(false, Ordering::AcqRel)
        {
            self.0.commit_from_another_session();
        }
        Ok(Box::new(MockSnapshot {
            data,
            cluster: Arc::clone(&self.0),
            start_ts,
        }))
    }

    fn acquire_advisory_lock(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<
        Box<dyn tidb_executor::advisory_lock_state::AdvisoryLockLease>,
        tidb_executor::advisory_lock_state::AdvisoryLockError,
    > {
        tidb_executor::advisory_lock_state::AdvisoryLockService::acquire(
            &self.0.advisory_locks,
            name,
            timeout,
        )
    }

    fn is_advisory_lock_used(&self, name: &str) -> bool {
        tidb_executor::advisory_lock_state::AdvisoryLockService::is_used(
            &self.0.advisory_locks,
            name,
        )
    }

    fn open_max_ts_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.0.opened_at_max_ts.fetch_add(1, Ordering::AcqRel);
        self.0.live.fetch_add(1, Ordering::AcqRel);
        // No `timestamp()` call: that absence IS what this branch buys, and
        // the clock the tests read is what proves it.
        Ok(Box::new(MockSnapshot {
            data: self.0.snapshot(),
            cluster: Arc::clone(&self.0),
            // The shortcut's marker, not a timestamp. A statement that took it
            // and then tried to write must fail closed, exactly as
            // `RealOptimisticTransactionOpener::begin_at` makes it.
            start_ts: u64::MAX,
        }))
    }

    fn commit(&self, buffer: &MutationBuffer, read_ts: Option<u64>) -> Result<(), SqlQueryError> {
        let staged = buffer.staged();
        if staged.is_empty() {
            return Ok(());
        }
        if self.0.fail_next_region_commit.swap(false, Ordering::AcqRel) {
            let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
                receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
                cause: TransactionCause::BackoffExhausted {
                    kind: RegionBackoffKind::RegionMiss,
                    detail: "regionMiss backoffer exhausted".to_owned(),
                },
            });
            return commit_outcome_to_sql_error(&outcome).map_err(sql_error);
        }
        if self
            .0
            .fail_next_generic_region_commit
            .swap(false, Ordering::AcqRel)
        {
            let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
                receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
                cause: TransactionCause::Region {
                    detail: "TiKV returned terminal region error: FlashbackInProgress region=42"
                        .to_owned(),
                },
            });
            return commit_outcome_to_sql_error(&outcome).map_err(sql_error);
        }
        // Autocommit publishes at the timestamp the statement READ at, which is
        // what puts a commit that landed in between inside TiKV's conflict
        // check. A statement that read nothing has none and takes a fresh one.
        let start_ts = match read_ts {
            Some(u64::MAX) => {
                buffer.reset();
                return Err(SqlQueryError::unknown(
                    "refusing to publish at the max-ts read marker".to_owned(),
                ));
            }
            Some(read_ts) => read_ts,
            None => self.0.timestamp(),
        };
        let outcome = self.0.publish(staged, start_ts);
        commit_outcome_to_sql_error(&outcome).map_err(sql_error)?;
        buffer.reset();
        Ok(())
    }

    fn begin(&self, _pessimistic: bool) -> Result<Box<dyn OpenClusterTransaction>, String> {
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
    fn start_ts(&self) -> u64 {
        self.start_ts
    }

    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.cluster.live.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(MockSnapshot {
            data: self.data.clone(),
            cluster: Arc::clone(&self.cluster),
            // Every statement of the transaction reads at what BEGIN took,
            // which is also what the eventual publication is checked against.
            start_ts: self.start_ts,
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
