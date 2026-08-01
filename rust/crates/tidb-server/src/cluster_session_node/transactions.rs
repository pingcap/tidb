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
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Publishes one autocommit statement's staged writes as its own
    /// transaction, then empties the buffer. An empty buffer publishes nothing.
    ///
    /// The error is the client-visible one, because a publication TiKV refused
    /// has a code of its own: a lost race is 9007, not a generic failure.
    fn commit(&self, buffer: &MutationBuffer) -> Result<(), SqlQueryError>;

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
struct DeferredSnapshot {
    transactions: Arc<dyn ClusterTransactions>,
    /// `None` until the first read. Behind a `Mutex` because `start_ts` takes
    /// `&self` and must answer with the timestamp of the same transaction the
    /// reads use.
    opened: Mutex<Option<Box<dyn ClusterSnapshot>>>,
}

impl fmt::Debug for DeferredSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredSnapshot")
            .field(
                "opened",
                &self
                    .opened
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .is_some(),
            )
            .finish()
    }
}

impl DeferredSnapshot {
    fn new(transactions: Arc<dyn ClusterTransactions>) -> Self {
        Self {
            transactions,
            opened: Mutex::new(None),
        }
    }

    /// Runs `use_snapshot` against the statement's read transaction, opening
    /// it first if this is the statement's first read.
    fn with_open<T>(
        &self,
        use_snapshot: impl FnOnce(&mut dyn ClusterSnapshot) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut guard = self
            .opened
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if guard.is_none() {
            *guard = Some(
                self.transactions
                    .open_snapshot()
                    .map_err(StorageError::Backend)?,
            );
        }
        use_snapshot(guard.as_mut().expect("just opened").as_mut())
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
}

/// Binds one autocommit statement's snapshot without spending a timestamp.
pub(crate) fn deferred_snapshot(
    transactions: Arc<dyn ClusterTransactions>,
) -> Box<dyn ClusterSnapshot> {
    Box::new(DeferredSnapshot::new(transactions))
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

    fn commit(&self, buffer: &MutationBuffer) -> Result<(), SqlQueryError> {
        commit_staged_buffer(&self.opener, buffer, self.timeout)
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
