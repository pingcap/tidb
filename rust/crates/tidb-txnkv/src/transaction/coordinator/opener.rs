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

//! Deriving a native transaction from the already-running process authorities.
//!
//! Go boundary: client-go's `txn.go` — `KVStore.Begin` allocates the `start_ts`
//! from PD and hands back a committer bound to the store's region cache and
//! transport. The native transaction owns its heartbeat lifecycle.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_pd_client::PdClient;

use crate::gc_state::{GcStateCache, TxnSafePointLoader, TxnSafePointRefresher};
use crate::lock::TimestampSource;
use crate::rpc::TonicCoprocessorClient;
use crate::{PdRegionLoader, SharedReadOpener, SharedReadRuntime};

use super::{CommitProtocol, OptimisticCoordinatorError, RealOptimisticTransaction};

/// Process-level opener for concrete normal optimistic transactions.
///
/// It holds only a cloneable session opener and the cloneable capability for
/// the already-running PD worker. The unique RegionCache maintenance and TiKV
/// transport lifecycle owners remain with the process that supplied them.
pub struct RealOptimisticTransactionOpener<
    C = TonicCoprocessorClient,
    L = PdRegionLoader,
    P = PdClient,
> {
    opener: crate::SharedReadOpener<C, L>,
    pd: P,
    timeout: Duration,
    /// Keeps the shared txn safe point current for as long as any clone of this
    /// opener — and therefore any transaction it opened — can still read.
    gc_state: Arc<TxnSafePointRefresher>,
    protocol: CommitProtocol,
    /// Resource group every transaction and direct MaxTS snapshot opened
    /// through this capability attaches to its TiKV request contexts.
    resource_group_name: Option<Arc<str>>,
}

impl<C: Clone, L, P: Clone> Clone for RealOptimisticTransactionOpener<C, L, P> {
    fn clone(&self) -> Self {
        Self {
            opener: self.opener.clone(),
            pd: self.pd.clone(),
            timeout: self.timeout,
            gc_state: Arc::clone(&self.gc_state),
            protocol: self.protocol,
            resource_group_name: self.resource_group_name.clone(),
        }
    }
}

impl RealOptimisticTransactionOpener {
    /// Derives transaction-opening capability from the already-running shared
    /// read authority. This starts no PD, RegionCache, or transport worker.
    /// The PRODUCTION constructor: the gc refresher rides the PD+etcd loader,
    /// fallback included, exactly as before the opener went generic.
    pub fn from_process_capabilities(
        opener: SharedReadOpener<TonicCoprocessorClient, PdRegionLoader>,
        pd: PdClient,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        if pd.cluster_id() == 0 {
            return Err(OptimisticCoordinatorError::ZeroClusterId);
        }
        // client-go loads the txn safe point inside `NewKVStore` and fails
        // store construction if it cannot: a reader that does not know the
        // safe point cannot tell a valid snapshot from a collected one.
        let gc_state = TxnSafePointRefresher::start(TxnSafePointLoader::new(
            pd.clone(),
            // The null keyspace: keyspace-level GC is not a scope this client
            // reads under.
            None,
            timeout,
        ))
        .map_err(|error| OptimisticCoordinatorError::GcState(error.to_string()))?;
        Ok(Self {
            opener,
            pd,
            timeout,
            gc_state: Arc::new(gc_state),
            protocol: CommitProtocol::two_phase_only(),
            resource_group_name: None,
        })
    }
}

/// Everything the write path demands of a store client, under one name.
///
/// Go's `kv.Storage` is one interface and every layer above it is
/// store-agnostic; these three aliases are that interface's client half,
/// so a helper generic over a store writes one bound instead of restating
/// the five-trait bundle. Blanket-implemented: any type with the parts IS one.
pub trait StoreWriteClient:
    Clone
    + crate::transaction::TransactionCommandClient
    + crate::lock::LockRecoveryClient
    + crate::LockWaitInfoClient
    + Send
    + Sync
    + 'static
{
}
impl<T> StoreWriteClient for T where
    T: Clone
        + crate::transaction::TransactionCommandClient
        + crate::lock::LockRecoveryClient
        + crate::LockWaitInfoClient
        + Send
        + Sync
        + 'static
{
}

/// The region-routing half of a store, under one name (see [`StoreWriteClient`]).
pub trait StoreWriteLoader:
    crate::region::RegionRecoveryLoader + crate::region::RegionQueryLoader + Send + Sync + 'static
{
}
impl<T> StoreWriteLoader for T where
    T: crate::region::RegionRecoveryLoader
        + crate::region::RegionQueryLoader
        + Send
        + Sync
        + 'static
{
}

/// The control-plane half of a store, under one name (see [`StoreWriteClient`]).
pub trait StorePdCapability: crate::pd_capability::PdCapability + Send + Sync + 'static {}
impl<T> StorePdCapability for T where T: crate::pd_capability::PdCapability + Send + Sync + 'static {}

impl<C, L, P> RealOptimisticTransactionOpener<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    /// The generic constructor: the caller supplies the safe-point refresher
    /// — an embedded store starts one over its own capability
    /// (`TxnSafePointRefresher::start_with_source`), the production path
    /// keeps its loader through `from_process_capabilities`.
    /// The PD capability this opener routes through.
    pub fn pd(&self) -> &P {
        &self.pd
    }

    /// Opens one worker-local capability over the process-owned transport and
    /// region cache without starting another authority.
    pub fn open_read_runtime(&self) -> Result<SharedReadRuntime<C, L>, OptimisticCoordinatorError> {
        self.opener
            .open_session()
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))
    }

    pub fn from_capabilities(
        opener: crate::SharedReadOpener<C, L>,
        pd: P,
        timeout: Duration,
        gc_state: TxnSafePointRefresher,
    ) -> Result<Self, OptimisticCoordinatorError> {
        if pd.cluster_id() == 0 {
            return Err(OptimisticCoordinatorError::ZeroClusterId);
        }
        Ok(Self {
            opener,
            pd,
            timeout,
            gc_state: Arc::new(gc_state),
            protocol: CommitProtocol::two_phase_only(),
            resource_group_name: None,
        })
    }

    /// Lets every transaction opened from here attempt `protocol`.
    ///
    /// The node resolves `@@tidb_enable_async_commit` / `@@tidb_enable_1pc`
    /// once, exactly as it resolves `@@tidb_pessimistic_txn_fair_locking`, so
    /// this is a property of the opener rather than an argument threaded
    /// through every call site that begins a transaction.
    #[must_use]
    pub const fn with_commit_protocol(mut self, protocol: CommitProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Returns the commit protocol inherited by transactions opened through
    /// this capability.
    #[must_use]
    pub const fn commit_protocol(&self) -> CommitProtocol {
        self.protocol
    }

    /// Assigns the SQL resource group inherited by every TiKV request opened
    /// through this capability.
    ///
    /// The opener is cloned per owning service, so configuring the SQL
    /// transaction tier does not silently relabel unrelated internal clients.
    #[must_use]
    pub fn with_resource_group_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.resource_group_name = Some(name.into());
        self
    }

    /// The shared txn safe point every transaction from this opener reads
    /// against.
    #[must_use]
    pub fn gc_state_cache(&self) -> Arc<GcStateCache> {
        self.gc_state.cache()
    }

    /// Stable shared process authority identity.
    #[must_use]
    pub fn authority_id(&self) -> u64 {
        self.opener.authority_id()
    }

    /// The PD cluster this opener writes to, as PD itself names it.
    ///
    /// Never zero: [`Self::from_process_capabilities`] refuses a PD client that
    /// has not learned its cluster ID.
    #[must_use]
    pub fn cluster_id(&self) -> u64 {
        self.pd.cluster_id()
    }

    /// Opens a worker-local transaction over the existing process authorities.
    pub fn begin(
        &self,
    ) -> Result<
        RealOptimisticTransaction<C, L, crate::pd_capability::CapabilityTimestampSource<P>>,
        OptimisticCoordinatorError,
    > {
        self.open(false)
    }

    /// Opens a writable transaction at a timestamp that has ALREADY been spent
    /// — the one the statement's own read is at — spending none of its own.
    ///
    /// This is what makes an implicit single-statement transaction a single
    /// transaction. Go allocates one timestamp for an autocommit DML and uses
    /// it for both halves: `pkg/sessiontxn/isolation/optimistic.go:45-46` points
    /// `getStmtReadTSFunc` *and* `getStmtForUpdateTSFunc` at `getTxnStartTS`,
    /// and client-go's `2pc.go` sets the committer's `startTS: txn.StartTS()`,
    /// which `prewrite.go` sends as `StartVersion`. Prewriting at a LATER
    /// timestamp than the read is not a slower version of the same thing: it is
    /// silent lost-update, because TiKV's conflict check compares a key's
    /// latest `commit_ts` against the *prewriting* transaction's `start_ts`, so
    /// a commit landing between the read and a fresh write timestamp is not a
    /// conflict TiKV can see and the stale value overwrites it with no error.
    ///
    /// `u64::MAX` is refused. It is not a timestamp — it is the direct MaxTS
    /// snapshot reader's marker for "the latest committed version", correct
    /// only for a read that never writes. Refusing it here is
    /// what makes "a max-ts read must not publish" a property of the only
    /// function that can turn a read timestamp into a write one, rather than a
    /// comment somewhere upstream.
    pub fn begin_at(
        &self,
        start_ts: u64,
    ) -> Result<
        RealOptimisticTransaction<C, L, crate::pd_capability::CapabilityTimestampSource<P>>,
        OptimisticCoordinatorError,
    > {
        if start_ts == u64::MAX {
            return Err(OptimisticCoordinatorError::Timestamp(
                "refusing to publish at the max-ts read marker: u64::MAX is the latest-committed \
                 read version, not a start timestamp a write may carry"
                    .to_owned(),
            ));
        }
        self.open_at(Some(start_ts), false)
    }

    /// Opens a transaction that may only read.
    ///
    /// The native snapshot option enforces read-only access.
    pub fn begin_read_only(
        &self,
    ) -> Result<
        RealOptimisticTransaction<C, L, crate::pd_capability::CapabilityTimestampSource<P>>,
        OptimisticCoordinatorError,
    > {
        self.open(true)
    }

    /// Dispatches an ordinary autocommit transaction's timestamp request
    /// without opening a worker-local transaction.
    ///
    /// Go stores the corresponding `oracle.Future` during transaction warmup;
    /// a statement that never reaches storage can drop it without activating a
    /// transaction.
    pub fn prepare_read_only_start_ts(&self) -> Result<P::TsFuture, OptimisticCoordinatorError> {
        self.pd
            .timestamp_future()
            .map_err(OptimisticCoordinatorError::Timestamp)
    }

    /// Opens a read-only transaction at a timestamp already obtained by
    /// [`Self::prepare_read_only_start_ts`].
    ///
    /// The native snapshot option enforces read-only access.
    pub fn begin_read_only_at(
        &self,
        start_ts: u64,
    ) -> Result<
        RealOptimisticTransaction<C, L, crate::pd_capability::CapabilityTimestampSource<P>>,
        OptimisticCoordinatorError,
    > {
        self.open_at(Some(start_ts), true)
    }

    /// Reads one point key at `u64::MAX` without activating a transaction.
    ///
    /// Go's optimistic provider returns `math.MaxUint64` directly for this
    /// plan shape; it does not call `Txn()`. Keep that distinction here too:
    /// open a read-session lease and run the snapshot RPC directly.
    /// The snapshot reader still owns the normal region
    /// recovery, lock resolution, GC visibility, and call-deadline checks.
    pub fn snapshot_get_at_max_ts(
        &self,
        key: &[u8],
        call: &crate::rpc::UnaryCallContext,
    ) -> Result<(Option<Vec<u8>>, u64), OptimisticCoordinatorError> {
        let runtime = self.open_read_runtime()?;
        if runtime.cluster_id() != self.pd.cluster_id() {
            return Err(OptimisticCoordinatorError::ClusterMismatch {
                pd: self.pd.cluster_id(),
                region_cache: runtime.cluster_id(),
            });
        }
        let mut snapshot = RealOptimisticTransaction::new_opened(
            runtime,
            crate::pd_capability::CapabilityTimestampSource(self.pd.clone()),
            self.timeout,
            u64::MAX,
            Instant::now(),
            true,
            self.gc_state.cache(),
        )?;
        if let Some(name) = self.resource_group_name.as_deref() {
            crate::new_txn::TxnResourceGroup::set_resource_group_name(&mut snapshot, name);
        }
        let result = snapshot
            .snapshot_get(key, call)
            .map(|result| (result.value, result.rpc_count));
        snapshot.finish_without_writes()?;
        result
    }

    /// Reads a bounded range at `u64::MAX` without activating a transaction.
    ///
    /// This is the range-shaped companion to [`Self::snapshot_get_at_max_ts`]
    /// used by a statement that is proven to return at most one row.
    pub fn snapshot_scan_at_max_ts(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: Option<usize>,
        call: &crate::rpc::UnaryCallContext,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError> {
        let runtime = self.open_read_runtime()?;
        if runtime.cluster_id() != self.pd.cluster_id() {
            return Err(OptimisticCoordinatorError::ClusterMismatch {
                pd: self.pd.cluster_id(),
                region_cache: runtime.cluster_id(),
            });
        }
        let mut snapshot = RealOptimisticTransaction::new_opened(
            runtime,
            crate::pd_capability::CapabilityTimestampSource(self.pd.clone()),
            self.timeout,
            u64::MAX,
            Instant::now(),
            true,
            self.gc_state.cache(),
        )?;
        if let Some(name) = self.resource_group_name.as_deref() {
            crate::new_txn::TxnResourceGroup::set_resource_group_name(&mut snapshot, name);
        }
        let result = snapshot.snapshot_scan(start_key, end_key, limit, call);
        snapshot.finish_without_writes()?;
        result
    }

    fn open(
        &self,
        read_only: bool,
    ) -> Result<
        RealOptimisticTransaction<C, L, crate::pd_capability::CapabilityTimestampSource<P>>,
        OptimisticCoordinatorError,
    > {
        self.open_at(None, read_only)
    }

    /// `start_ts` of `None` spends one PD timestamp; `Some` uses the supplied
    /// one and spends none.
    fn open_at(
        &self,
        start_ts: Option<u64>,
        read_only: bool,
    ) -> Result<
        RealOptimisticTransaction<C, L, crate::pd_capability::CapabilityTimestampSource<P>>,
        OptimisticCoordinatorError,
    > {
        let opened_at = Instant::now();
        let runtime = self.open_read_runtime()?;
        if runtime.cluster_id() != self.pd.cluster_id() {
            return Err(OptimisticCoordinatorError::ClusterMismatch {
                pd: self.pd.cluster_id(),
                region_cache: runtime.cluster_id(),
            });
        }
        let start_ts = match start_ts {
            Some(start_ts) => start_ts,
            None => {
                // The real client's `get_timestamp` IS `get_timestamp_async +
                // wait`, so the seam changes nothing observable.
                use crate::pd_capability::TimestampFutureWait;
                self.pd
                    .timestamp_future()
                    .and_then(|future| future.wait())
                    .map_err(OptimisticCoordinatorError::Timestamp)?
            }
        };
        if start_ts == 0 {
            return Err(OptimisticCoordinatorError::Timestamp(
                "PD returned zero start timestamp".to_owned(),
            ));
        }
        let mut transaction = RealOptimisticTransaction::new_opened(
            runtime,
            crate::pd_capability::CapabilityTimestampSource(self.pd.clone()),
            self.timeout,
            start_ts,
            opened_at,
            read_only,
            self.gc_state.cache(),
        )?;
        transaction.set_commit_protocol(self.protocol);
        if let Some(resource_group_name) = self.resource_group_name.as_deref() {
            crate::new_txn::TxnResourceGroup::set_resource_group_name(
                &mut transaction,
                resource_group_name,
            );
        }
        Ok(transaction)
    }

    /// Opens a pessimistic transaction over the same process authorities.
    ///
    /// It shares the optimistic opener because a pessimistic transaction *is*
    /// an optimistic two-phase commit preceded by statement-level locking; only
    /// the conflict-detection point differs.
    pub fn begin_pessimistic(
        &self,
    ) -> Result<
        super::super::RealPessimisticTransaction<
            C,
            L,
            crate::pd_capability::CapabilityTimestampSource<P>,
        >,
        OptimisticCoordinatorError,
    > {
        let opened_at = Instant::now();
        let two_pc = self.begin()?;
        super::super::RealPessimisticTransaction::from_transaction(two_pc, opened_at)
    }

    /// Opens a pessimistic transaction that will LOCK but never publish —
    /// `GET_LOCK` holds its locks until rollback.
    pub fn begin_pessimistic_lock_only(
        &self,
    ) -> Result<
        super::super::RealPessimisticTransaction<
            C,
            L,
            crate::pd_capability::CapabilityTimestampSource<P>,
        >,
        OptimisticCoordinatorError,
    > {
        let opened_at = Instant::now();
        let two_pc = self.open(false)?;
        super::super::RealPessimisticTransaction::from_transaction(two_pc, opened_at)
    }
}

/// Real PD timestamp authority used by the one production transaction.
pub struct PdLockTimestampSource(PdClient);

impl fmt::Debug for PdLockTimestampSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PdLockTimestampSource")
            .finish_non_exhaustive()
    }
}

impl TimestampSource for PdLockTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        self.0.get_timestamp().map_err(|error| error.to_string())
    }
}
