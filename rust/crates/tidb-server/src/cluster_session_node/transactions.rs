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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tidb_pd_client::PdClient;
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};
use tidb_txnkv::PdRegionLoader;

use tidb_exec::cluster_table_storage::{
    commit_staged_buffer, MaxTsSnapshot, PreparedStatementSnapshot, SessionTransaction,
    StatementSnapshot,
};
use tidb_exec::pessimistic_lock_error::LockSqlError;
use tidb_exec::real_tikv_read::RealOptimisticTransactionOpener;
use tidb_executor::advisory_lock_state::{
    AdvisoryLockError, AdvisoryLockLease, AdvisoryLockService,
};
use tidb_executor::cluster_storage::{ClusterSnapshot, MutationBuffer, SnapshotPairs};
use tidb_executor::storage::StorageError;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    LockKeepAlive, LockWaitTime, PessimisticLockFailure, RealPessimisticTransaction,
};
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
    /// Starts preparing one ordinary autocommit snapshot without waiting for
    /// its timestamp. The first read consumes the returned future.
    fn prepare_snapshot(&self) -> Result<Box<dyn PendingClusterSnapshot>, String>;

    /// Synchronously opens one autocommit statement's read snapshot at its own
    /// timestamp. This is the fail-closed fallback when no prepared future was
    /// installed; ordinary statements use [`Self::prepare_snapshot`].
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
    fn commit(&self, buffer: &MutationBuffer, read_ts: Option<u64>) -> Result<(), SqlQueryError>;

    /// Opens the one transaction an explicit `BEGIN` holds until `COMMIT` or
    /// `ROLLBACK`.
    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String>;

    /// Acquires the TiKV pessimistic key backing one advisory lock.
    fn acquire_advisory_lock(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError>;

    /// Checks the same physical key without retaining it.
    fn is_advisory_lock_used(&self, name: &str) -> bool;
}

/// Adapts the cluster transaction authority to the expression/session lock
/// service without creating a second lock namespace.
pub struct ClusterAdvisoryLockService {
    transactions: Arc<dyn ClusterTransactions>,
}

impl ClusterAdvisoryLockService {
    #[must_use]
    pub fn new(transactions: Arc<dyn ClusterTransactions>) -> Self {
        Self { transactions }
    }
}

impl AdvisoryLockService for ClusterAdvisoryLockService {
    fn acquire(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError> {
        self.transactions.acquire_advisory_lock(name, timeout)
    }

    fn is_used(&self, name: &str) -> bool {
        self.transactions.is_advisory_lock_used(name)
    }
}

/// One ordinary autocommit snapshot whose timestamp request is in flight.
pub trait PendingClusterSnapshot: Send {
    /// Waits for preparation and returns the snapshot every statement read
    /// will share.
    fn wait(self: Box<Self>) -> Result<Box<dyn ClusterSnapshot>, String>;
}

/// The transaction an explicit `BEGIN` holds open across its statements.
///
/// Every statement of the transaction reads through [`Self::snapshot`], so they
/// all share the timestamp `BEGIN` took, and [`Self::commit`] prewrites at that
/// same timestamp -- which is what makes a racing writer a write conflict
/// instead of a silent overwrite.
pub trait OpenClusterTransaction: Send {
    /// The transaction timestamp shared by every statement until it ends.
    fn start_ts(&self) -> u64;

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

/// The timestamp one autocommit statement is at: written when the first read
/// waits for its prepared snapshot, read back by statement publication.
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
pub(crate) struct StatementReadTs {
    value: Arc<Mutex<Option<u64>>>,
    current_tso: tidb_executor::CurrentTso,
}

impl StatementReadTs {
    pub(crate) fn new(current_tso: tidb_executor::CurrentTso) -> Self {
        Self {
            value: Arc::default(),
            current_tso,
        }
    }

    fn record(&self, start_ts: u64) {
        *self
            .value
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(start_ts);
        self.current_tso.publish(start_ts);
    }

    /// The timestamp the statement read at, or `None` if it never read.
    pub(crate) fn get(&self) -> Option<u64> {
        *self
            .value
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// One autocommit statement's read snapshot, prepared after planning and
/// activated at its FIRST read.
///
/// The session driver binds a statement's snapshot before the statement is
/// planned, because the slot has to be in place before the executor exists.
/// Opening synchronously at bind time both precedes the plan-shape decision and
/// serializes PD latency with executor setup. Go instead installs an oracle
/// future after `AdviseOptimizeWithPlan`: planning chooses ordinary versus
/// MaxTS first, ordinary TSO work overlaps executor construction, and `Txn()`
/// waits only if execution needs a snapshot. `prepare_open` is that future;
/// `with_open` is the wait/activation boundary.
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
    /// An ordinary timestamp request started after planning and not yet waited.
    prepared: Option<Box<dyn PendingClusterSnapshot>>,
    /// `None` until the first read waits for the prepared snapshot.
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
            .field("prepared", &state.prepared.is_some())
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

    fn prepare_open(&self) -> Result<(), StorageError> {
        let mut state = self.state();
        if state.max_ts || state.prepared.is_some() || state.opened.is_some() {
            return Ok(());
        }
        state.prepared = Some(
            self.transactions
                .prepare_snapshot()
                .map_err(StorageError::Backend)?,
        );
        Ok(())
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
            } else if let Some(prepared) = guard.prepared.take() {
                prepared.wait()
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
    fn prepare(&mut self) -> Result<(), StorageError> {
        self.prepare_open()
    }

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
        // Go chooses the MaxTS constant future before warmup. If a caller ever
        // reverses those two operations, discard the ordinary future here.
        state.prepared.take();
        state.max_ts = true;
        true
    }
}

/// Binds one autocommit statement's snapshot without starting a timestamp.
///
/// `read_ts` is the statement's, and outlives the returned handle: whatever
/// timestamp the statement's first read waits for is written there, and the
/// statement's publication reads it back after this handle has been dropped.
pub(crate) fn deferred_snapshot(
    transactions: Arc<dyn ClusterTransactions>,
    read_ts: StatementReadTs,
) -> Box<dyn ClusterSnapshot> {
    Box::new(DeferredSnapshot::new(transactions, read_ts))
}

/// The production transaction tier: real read-only transactions and the
/// optimistic 2PC, both over the node's one process authority.
pub struct RealClusterTransactions<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
}

struct RealPendingSnapshot<C, L, P>(PreparedStatementSnapshot<C, L, P>)
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability;

impl<C, L, P> PendingClusterSnapshot for RealPendingSnapshot<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn wait(self: Box<Self>) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.0
            .wait()
            .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
            .map_err(|error| error.to_string())
    }
}

impl<C, L, P> RealClusterTransactions<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    /// Binds the tier to an already-connected authority's write capability.
    #[must_use]
    pub fn new(opener: RealOptimisticTransactionOpener<C, L, P>, timeout: Duration) -> Self {
        Self {
            opener: Arc::new(opener),
            timeout,
        }
    }

    fn acquire_advisory_lock_lease(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<RealAdvisoryLockLease<C, L, P>, AdvisoryLockError> {
        let key = tidb_exec::mysql_bootstrap::advisory_lock_key(name)
            .map_err(|error| AdvisoryLockError::Internal(error.to_string()))?;
        let mut transaction = self
            .opener
            .begin_pessimistic(0, 0)
            .map_err(|error| AdvisoryLockError::Internal(error.to_string()))?;
        let wait = if timeout.is_zero() {
            LockWaitTime::NoWait
        } else {
            LockWaitTime::Timeout(timeout)
        };
        let call = UnaryCallContext::with_timeout(timeout.saturating_add(self.timeout));
        let presume_not_exists = BTreeSet::from([key.clone()]);
        let acquired = match transaction.acquire_locks(
            std::slice::from_ref(&key),
            &presume_not_exists,
            wait,
            &call,
        ) {
            Ok(acquired) => acquired,
            Err(failure) => {
                finish_advisory_transaction(transaction, &[key], self.timeout);
                return Err(map_advisory_lock_failure(failure));
            }
        };
        let keep_alive = match self
            .opener
            .start_lock_keep_alive(acquired.primary_key, transaction.start_ts())
        {
            Ok(keep_alive) => keep_alive,
            Err(error) => {
                finish_advisory_transaction(transaction, &acquired.keys, self.timeout);
                return Err(AdvisoryLockError::Internal(error));
            }
        };
        Ok(RealAdvisoryLockLease {
            transaction: Some(transaction),
            keep_alive: Some(keep_alive),
            end_timeout: self.timeout,
        })
    }
}

struct RealAdvisoryLockLease<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    transaction: Option<
        RealPessimisticTransaction<C, L, tidb_txnkv::pd_capability::CapabilityTimestampSource<P>>,
    >,
    keep_alive: Option<LockKeepAlive>,
    end_timeout: Duration,
}

impl<C, L, P> RealAdvisoryLockLease<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn finish(&mut self) {
        if let Some(keep_alive) = self.keep_alive.take() {
            keep_alive.close();
        }
        let Some(transaction) = self.transaction.take() else {
            return;
        };
        let held = transaction.locked_keys();
        finish_advisory_transaction(transaction, &held, self.end_timeout);
    }
}

fn finish_advisory_transaction<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    mut transaction: RealPessimisticTransaction<
        C,
        L,
        tidb_txnkv::pd_capability::CapabilityTimestampSource<P>,
    >,
    keys: &[Vec<u8>],
    timeout: Duration,
) {
    let call = UnaryCallContext::with_timeout(timeout);
    let _ = transaction.pessimistic_rollback(keys, &call);
    let _ = transaction.into_two_pc().finish_without_writes();
}

impl<C, L, P> Drop for RealAdvisoryLockLease<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn drop(&mut self) {
        self.finish();
    }
}

impl<C, L, P> AdvisoryLockLease for RealAdvisoryLockLease<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn release(mut self: Box<Self>) {
        self.finish();
    }
}

fn map_advisory_lock_failure(failure: PessimisticLockFailure) -> AdvisoryLockError {
    match failure {
        PessimisticLockFailure::LockAcquireFailAndNoWaitSet { .. }
        | PessimisticLockFailure::LockWaitTimeout { .. } => AdvisoryLockError::Timeout,
        PessimisticLockFailure::Deadlock(_) => AdvisoryLockError::Deadlock,
        other => AdvisoryLockError::Internal(other.to_string()),
    }
}

impl<C, L, P> ClusterTransactions for RealClusterTransactions<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn prepare_snapshot(&self) -> Result<Box<dyn PendingClusterSnapshot>, String> {
        StatementSnapshot::prepare(Arc::clone(&self.opener), self.timeout)
            .map(|snapshot| {
                Box::new(RealPendingSnapshot(snapshot)) as Box<dyn PendingClusterSnapshot>
            })
            .map_err(|error| error.to_string())
    }

    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        StatementSnapshot::open(Arc::clone(&self.opener), self.timeout)
            .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
            .map_err(|error| error.to_string())
    }

    fn open_max_ts_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        Ok(Box::new(MaxTsSnapshot::new(
            Arc::clone(&self.opener),
            self.timeout,
        )))
    }

    fn commit(&self, buffer: &MutationBuffer, read_ts: Option<u64>) -> Result<(), SqlQueryError> {
        commit_staged_buffer(&self.opener, buffer, read_ts, self.timeout)
            .map(|_| ())
            .map_err(sql_error)
    }

    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String> {
        SessionTransaction::begin(Arc::clone(&self.opener), self.timeout)
            .map(|transaction| Box::new(transaction) as Box<dyn OpenClusterTransaction>)
            .map_err(|error| error.to_string())
    }

    fn acquire_advisory_lock(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError> {
        self.acquire_advisory_lock_lease(name, timeout)
            .map(|lease| Box::new(lease) as Box<dyn AdvisoryLockLease>)
    }

    fn is_advisory_lock_used(&self, name: &str) -> bool {
        match self.acquire_advisory_lock_lease(name, Duration::from_secs(1)) {
            Ok(mut lease) => {
                lease.finish();
                false
            }
            Err(_) => true,
        }
    }
}

impl OpenClusterTransaction for SessionTransaction {
    fn start_ts(&self) -> u64 {
        SessionTransaction::start_ts(self)
    }

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
