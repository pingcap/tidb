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

//! The transaction seam: one fresh read snapshot per autocommit statement,
//! one publication of its staged writes, and the single transaction an
//! explicit `BEGIN` holds open. Split out of `cluster_session_node` because
//! it is one of the independent seams that accreted there; see that module's
//! doc comment for the statement lifecycle this seam is exercised by.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_exec::cluster_table_storage::{
    commit_staged_buffer, SessionTransaction, StatementSnapshot,
};
use tidb_exec::pessimistic_lock_error::LockSqlError;
use tidb_exec::real_tikv_read::RealOptimisticTransactionOpener;
use tidb_executor::cluster_storage::{ClusterSnapshot, MutationBuffer, SnapshotPairs};
use tidb_executor::storage::StorageError;
use tidb_txnkv::Key;

use crate::sql_node::SqlQueryError;

/// Carries a commit's own client-visible triple onto the wire, so a 9007 stays
/// a 9007 instead of collapsing into the generic 1105.
pub(crate) fn sql_error(error: LockSqlError) -> SqlQueryError {
    SqlQueryError::new(error.code, error.state, error.message)
}

/// Everything a connection needs from the cluster's transaction tier: one
/// fresh read snapshot per autocommit statement, one publication of its staged
/// writes, and the single transaction an explicit `BEGIN` holds open.
///
/// The seam exists so the statement lifecycle -- which is the correctness core
/// of this mode -- is exercised without a cluster. The production
/// implementation is [`RealClusterTransactions`]; the tests drive the same
/// lifecycle against an in-memory committed store.
pub trait ClusterTransactions: Send + Sync {
    /// Opens one autocommit statement's read snapshot at its own timestamp.
    ///
    /// Called at the statement's FIRST read rather than when it binds, so a
    /// statement that reads no cluster row never reaches here; see
    /// [`DeferredSnapshot`].
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Opens one autocommit statement's read snapshot at `u64::MAX` -- the
    /// latest committed version -- spending no PD timestamp.
    ///
    /// Reached only from a statement that DECLARED its whole read is one point
    /// get on the clustered handle; see
    /// [`ClusterSnapshot::declare_autocommit_point_get`].
    fn open_max_ts_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Publishes one autocommit statement's staged writes as its own
    /// transaction **at `read_ts`**, then empties the buffer. An empty buffer
    /// publishes nothing.
    ///
    /// `read_ts` is the timestamp the statement's own reads were served at, or
    /// `None` if the statement read no cluster row and therefore has none. It
    /// is not an optimisation: publishing a value computed from a read at `T`
    /// under any timestamp later than `T` puts every commit in between outside
    /// TiKV's conflict check, which is silent lost-update. See
    /// [`StatementReadTs`].
    ///
    /// The error is the client-visible one, because a publication TiKV refused
    /// has a code of its own: a lost race is 9007, not a generic failure.
    fn commit(&self, buffer: &MutationBuffer, read_ts: Option<u64>)
        -> Result<(), SqlQueryError>;

    /// Opens the one transaction an explicit `BEGIN` holds until `COMMIT` or
    /// `ROLLBACK`.
    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String>;
}

/// The transaction an explicit `BEGIN` holds open across its statements.
///
/// Every statement of the transaction reads through [`Self::snapshot`], so they
/// all share the timestamp `BEGIN` took, and [`Self::commit`] prewrites at that
/// same timestamp -- which is what makes a racing writer a write conflict
/// instead of a silent overwrite.
pub trait OpenClusterTransaction: Send {
    /// One statement's read handle. Dropping it ends the statement, never the
    /// transaction.
    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Publishes the staged writes at the transaction's own start timestamp and
    /// empties the buffer.
    ///
    /// The error is the client-visible one: a transaction whose prewrite lost
    /// the race against a newer commit reports 9007, as Go's does.
    fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), SqlQueryError>;

    /// Ends the transaction without publishing anything.
    fn rollback(self: Box<Self>) -> Result<(), String>;
}

/// One autocommit statement's read snapshot, opened at its FIRST read rather
/// than when the statement starts.
///
/// The session driver binds a statement's snapshot before the statement is
/// planned, because the slot has to be in place before the executor exists.
/// Opening the real read transaction there spends one PD timestamp
/// unconditionally -- including for statements that read no cluster row at all
/// (`SET`, a constant `SELECT`, a statement served entirely from the staged
/// buffer), and before any plan exists for a timestamp policy to look at.
/// Deferring the open to the first `get`/`scan` keeps the binding order
/// exactly as it was while making the timestamp the property of a read, which
/// is the only thing that needs one.
///
/// The deferral changes no read's timestamp: the first read still opens a
/// fresh transaction, so it is the same "latest committed at the moment the
/// statement reads" the eager open gave, only moved later inside the same
/// statement. Nothing between the two points reads through the slot -- the
/// statement's own execution is what does.
/// The timestamp one autocommit statement is at: written by the statement's
/// first read, read back by the statement's publication.
///
/// The two halves cannot share the read transaction itself, because the read
/// handle is unbound and dropped before the publication is decided — that
/// ordering is what keeps a read transaction from outliving its statement. So
/// the *number* outlives the handle, and this is where it lives.
///
/// `None` after the statement means it never read a cluster row. That is a
/// real, distinct case, not a missing value: an `INSERT ... VALUES` with no
/// read has nothing a racing commit could have made stale, so its publication
/// takes a fresh timestamp and is correct doing so.
///
/// A statement that read at the max-ts shortcut records `u64::MAX`, which is
/// not a timestamp; [`RealOptimisticTransactionOpener::begin_at`] refuses it,
/// so such a statement fails closed instead of publishing anywhere.
#[derive(Clone, Debug, Default)]
pub(crate) struct StatementReadTs(Arc<Mutex<Option<u64>>>);

impl StatementReadTs {
    fn record(&self, start_ts: u64) {
        *self.0.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(start_ts);
    }

    /// The timestamp the statement read at, or `None` if it never read.
    pub(crate) fn get(&self) -> Option<u64> {
        *self.0.lock().unwrap_or_else(|poison| poison.into_inner())
    }
}

struct DeferredSnapshot {
    transactions: Arc<dyn ClusterTransactions>,
    /// Where the open publishes the statement's timestamp, so the publication
    /// can find it after this handle is gone.
    read_ts: StatementReadTs,
    /// Behind one `Mutex` because `start_ts` takes `&self` and must answer
    /// with the timestamp of the same transaction the reads use -- and because
    /// the declaration below must be settled against the open atomically.
    state: Mutex<DeferredState>,
}

/// The statement's read transaction, and the shape it was declared with.
#[derive(Default)]
struct DeferredState {
    /// `None` until the first read.
    opened: Option<Box<dyn ClusterSnapshot>>,
    /// Whether the statement declared its whole read is one point get on the
    /// clustered handle, which is what decides WHICH transaction the first
    /// read opens.
    ///
    /// It is a property of the statement, taken once before any read, never of
    /// a read. Were it re-decided per read, one statement's two reads would
    /// land on two different latest-committed versions -- and the counters
    /// would still show a saving. Keeping it beside `opened` under one lock is
    /// what makes "declared before the first read" checkable rather than
    /// hoped for.
    max_ts: bool,
}

impl fmt::Debug for DeferredSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        formatter
            .debug_struct("DeferredSnapshot")
            .field("opened", &state.opened.is_some())
            .field("max_ts", &state.max_ts)
            .finish()
    }
}

impl DeferredSnapshot {
    fn new(transactions: Arc<dyn ClusterTransactions>, read_ts: StatementReadTs) -> Self {
        Self {
            transactions,
            read_ts,
            state: Mutex::new(DeferredState::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, DeferredState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Runs `use_snapshot` against the statement's read transaction, opening
    /// it first if this is the statement's first read.
    ///
    /// The open is where the declaration is spent: a declared statement opens
    /// at `u64::MAX` and pays no PD timestamp, and every later read of the
    /// same statement goes through the transaction this one opened -- so a
    /// statement never splits across two timestamps whichever branch it took.
    fn with_open<T>(
        &self,
        use_snapshot: impl FnOnce(&mut dyn ClusterSnapshot) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut guard = self.state();
        if guard.opened.is_none() {
            let opened = if guard.max_ts {
                self.transactions.open_max_ts_snapshot()
            } else {
                self.transactions.open_snapshot()
            };
            let opened = opened.map_err(StorageError::Backend)?;
            // Recorded at the open, under the same lock, so the timestamp the
            // statement publishes at is the one its reads are served at and
            // cannot be a later transaction's.
            self.read_ts.record(opened.start_ts());
            guard.opened = Some(opened);
        }
        use_snapshot(guard.opened.as_mut().expect("just opened").as_mut())
    }
}

impl ClusterSnapshot for DeferredSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_open(|snapshot| snapshot.get(key))
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.with_open(|snapshot| snapshot.scan(start, end, limit))
    }

    fn start_ts(&self) -> u64 {
        // A remote pushdown scan names the timestamp before issuing its
        // request, so asking IS the statement's first read: opening here is
        // what keeps the coprocessor request and any local read on one
        // transaction. `0` on failure is the "no MVCC timestamp" answer the
        // trait already defines, which refuses the remote scan and leaves the
        // error to be reported by the read that follows.
        self.with_open(|snapshot| Ok(snapshot.start_ts()))
            .unwrap_or(0)
    }

    /// Takes the declaration, but only while the statement still has no read
    /// transaction.
    ///
    /// A statement that has already read has already spent a timestamp, and
    /// switching now would put its remaining reads on a second snapshot. That
    /// is Go's `p.txn != nil` refusal in `AdviseOptimizeWithPlan` -- "the
    /// startTS has already been used" -- and it is checked under the same lock
    /// the open takes, so there is no window between the two.
    fn declare_autocommit_point_get(&mut self) -> bool {
        let mut state = self.state();
        if state.opened.is_some() {
            return false;
        }
        state.max_ts = true;
        true
    }
}

/// Binds one autocommit statement's snapshot without spending a timestamp.
///
/// `read_ts` is the statement's, and outlives the returned handle: whatever
/// timestamp the statement's first read opens at is written there, and the
/// statement's publication reads it back after this handle has been dropped.
pub(crate) fn deferred_snapshot(
    transactions: Arc<dyn ClusterTransactions>,
    read_ts: StatementReadTs,
) -> Box<dyn ClusterSnapshot> {
    Box::new(DeferredSnapshot::new(transactions, read_ts))
}

/// The production transaction tier: real read-only transactions and the
/// optimistic 2PC, both over the node's one process authority.
pub struct RealClusterTransactions {
    opener: Arc<RealOptimisticTransactionOpener>,
    timeout: Duration,
}

impl RealClusterTransactions {
    /// Binds the tier to an already-connected authority's write capability.
    #[must_use]
    pub fn new(opener: RealOptimisticTransactionOpener, timeout: Duration) -> Self {
        Self {
            opener: Arc::new(opener),
            timeout,
        }
    }
}

impl ClusterTransactions for RealClusterTransactions {
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        StatementSnapshot::open(Arc::clone(&self.opener), self.timeout)
            .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
            .map_err(|error| error.to_string())
    }

    fn open_max_ts_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        StatementSnapshot::open_at_max_ts(Arc::clone(&self.opener), self.timeout)
            .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
            .map_err(|error| error.to_string())
    }

    fn commit(
        &self,
        buffer: &MutationBuffer,
        read_ts: Option<u64>,
    ) -> Result<(), SqlQueryError> {
        commit_staged_buffer(&self.opener, buffer, read_ts, self.timeout)
            .map(|_| ())
            .map_err(sql_error)
    }

    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String> {
        SessionTransaction::begin(Arc::clone(&self.opener), self.timeout)
            .map(|transaction| Box::new(transaction) as Box<dyn OpenClusterTransaction>)
            .map_err(|error| error.to_string())
    }
}

impl OpenClusterTransaction for SessionTransaction {
    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::snapshot(self).map_err(|error| error.to_string())
    }

    fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), SqlQueryError> {
        SessionTransaction::commit(*self, buffer)
            .map(|_| ())
            .map_err(sql_error)
    }

    fn rollback(self: Box<Self>) -> Result<(), String> {
        SessionTransaction::rollback(*self)
    }
}
