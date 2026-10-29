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

use std::sync::Arc;
use std::time::Duration;

use tikv_client::PdRpcClient;

use tidb_txnkv::driver::tikv_store::TikvStore;

use super::{CommitProtocol, RealTransaction};
use tidb_txnkv::transaction::OptimisticCoordinatorError;

/// Process-level opener for TiDB transactions: Go `tikvStore.Begin`.
///
/// It holds the node's client-rust store, whose region cache, sender, lock
/// resolver, oracle, PD client and txn safe point every transaction it opens
/// shares.
pub struct RealOptimisticTransactionOpener<S: tikv_client::PdClient = PdRpcClient> {
    store: TikvStore<S>,
    timeout: Duration,
    protocol: CommitProtocol,
    /// Resource group every transaction and direct MaxTS snapshot opened
    /// through this capability attaches to its TiKV request contexts.
    resource_group_name: Option<Arc<str>>,
    /// The same store as the coprocessor reads through it (Go's copr over
    /// the `KVStore`), for the reads this opener's users issue themselves --
    /// ANALYZE's sampling.
    cop_store: Arc<dyn crate::driver::cop_store::NativeCopStore>,
}

impl<S: tikv_client::PdClient> Clone for RealOptimisticTransactionOpener<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            timeout: self.timeout,
            protocol: self.protocol,
            resource_group_name: self.resource_group_name.clone(),
            cop_store: Arc::clone(&self.cop_store),
        }
    }
}

impl<S: tikv_client::PdClient> RealOptimisticTransactionOpener<S> {
    /// The node's store for coprocessor reads.
    #[must_use]
    pub fn cop_store(&self) -> Option<&Arc<dyn crate::driver::cop_store::NativeCopStore>> {
        Some(&self.cop_store)
    }

    /// The node's client-rust store (Go `tikvStore`'s `*tikv.KVStore`).
    #[must_use]
    pub fn store(&self) -> &TikvStore<S> {
        &self.store
    }
}

impl RealOptimisticTransactionOpener {
    /// The production opener over the node's store.
    pub fn from_process_capabilities(
        store: TikvStore,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        Self::from_capabilities(store, timeout)
    }
}

/// The store a transaction opener runs on, under one name: any client-rust
/// PD client (a TiKV cluster's, or the embedded store's).
pub trait StoreClient: tikv_client::PdClient {}
impl<T: tikv_client::PdClient> StoreClient for T {}

impl<S> RealOptimisticTransactionOpener<S>
where
    S: StoreClient,
{
    /// The opener over `store`: timestamps from its oracle, PD through its
    /// PD client.
    pub fn from_capabilities(
        store: TikvStore<S>,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        if store.cluster_id() == 0 {
            return Err(OptimisticCoordinatorError::ZeroClusterId);
        }
        let cop_store: Arc<dyn crate::driver::cop_store::NativeCopStore> = Arc::new(store.clone());
        Ok(Self {
            store,
            timeout,
            protocol: CommitProtocol::two_phase_only(),
            resource_group_name: None,
            cop_store,
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

    /// Stable store identity in this process.
    #[must_use]
    pub fn authority_id(&self) -> u64 {
        self.store.authority_id()
    }

    /// The PD cluster this opener writes to, as PD itself names it.
    ///
    /// Never zero: [`Self::from_capabilities`] refuses a store whose PD
    /// client has not learned its cluster ID.
    #[must_use]
    pub fn cluster_id(&self) -> u64 {
        self.store.cluster_id()
    }

    /// Opens a transaction.
    pub fn begin(&self) -> Result<RealTransaction<S>, OptimisticCoordinatorError> {
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
    ) -> Result<RealTransaction<S>, OptimisticCoordinatorError> {
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
    pub fn begin_read_only(&self) -> Result<RealTransaction<S>, OptimisticCoordinatorError> {
        self.open(true)
    }

    /// Dispatches an ordinary autocommit transaction's timestamp request
    /// without opening a worker-local transaction.
    ///
    /// Go stores the corresponding `oracle.Future` during transaction warmup
    /// (`sessiontxn/isolation/base.go`: `GetStore().GetOracle()
    /// .GetTimestampAsync`); a statement that never reaches storage can drop
    /// it without activating a transaction.
    pub fn prepare_read_only_start_ts(
        &self,
    ) -> Result<tidb_txnkv::driver::tikv_store::StoreTimestampFuture, OptimisticCoordinatorError> {
        Ok(self.store.get_timestamp_async())
    }

    /// Opens a read-only transaction at a timestamp already obtained by
    /// [`Self::prepare_read_only_start_ts`].
    ///
    /// The native snapshot option enforces read-only access.
    pub fn begin_read_only_at(
        &self,
        start_ts: u64,
    ) -> Result<RealTransaction<S>, OptimisticCoordinatorError> {
        self.open_at(Some(start_ts), true)
    }

    /// Reads one point key at `u64::MAX` without activating a transaction.
    ///
    /// Go's optimistic provider returns `math.MaxUint64` directly for this
    /// plan shape; it does not call `Txn()`. The store's snapshot still owns
    /// region recovery, lock resolution, GC visibility and the call deadline.
    pub fn snapshot_get_at_max_ts(
        &self,
        key: &[u8],
        call: &tidb_txnkv::rpc::UnaryCallContext,
        options: &tidb_txnkv::SnapshotReadOptions,
    ) -> Result<(Option<Vec<u8>>, u64), OptimisticCoordinatorError> {
        let mut snapshot = self.max_ts_snapshot(options)?;
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
        call: &tidb_txnkv::rpc::UnaryCallContext,
        options: &tidb_txnkv::SnapshotReadOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError> {
        let mut snapshot = self.max_ts_snapshot(options)?;
        let result = snapshot.snapshot_scan(start_key, end_key, limit, call);
        snapshot.finish_without_writes()?;
        result
    }

    fn max_ts_snapshot(
        &self,
        options: &tidb_txnkv::SnapshotReadOptions,
    ) -> Result<RealTransaction<S>, OptimisticCoordinatorError> {
        let mut snapshot =
            RealTransaction::new_opened(&self.store, u64::MAX, true, self.authority_id())?;
        snapshot.set_snapshot_read_options(options);
        if let Some(name) = self.resource_group_name.as_deref() {
            tidb_txnkv::new_txn::TxnResourceGroup::set_resource_group_name(&mut snapshot, name);
        }
        Ok(snapshot)
    }

    fn open(&self, read_only: bool) -> Result<RealTransaction<S>, OptimisticCoordinatorError> {
        self.open_at(None, read_only)
    }

    /// `start_ts` of `None` spends one timestamp from the store's oracle, as
    /// Go's `tikvStore.Begin` does; `Some` uses the supplied one and spends
    /// none.
    fn open_at(
        &self,
        start_ts: Option<u64>,
        read_only: bool,
    ) -> Result<RealTransaction<S>, OptimisticCoordinatorError> {
        let start_ts = match start_ts {
            Some(start_ts) => start_ts,
            None => self
                .store
                .get_timestamp_async()
                .wait()
                .map_err(OptimisticCoordinatorError::Timestamp)?,
        };
        if start_ts == 0 {
            return Err(OptimisticCoordinatorError::Timestamp(
                "PD returned zero start timestamp".to_owned(),
            ));
        }
        let mut transaction =
            RealTransaction::new_opened(&self.store, start_ts, read_only, self.authority_id())?;
        transaction.set_commit_protocol(self.protocol);
        if let Some(resource_group_name) = self.resource_group_name.as_deref() {
            tidb_txnkv::new_txn::TxnResourceGroup::set_resource_group_name(
                &mut transaction,
                resource_group_name,
            );
        }
        Ok(transaction)
    }
}

/// Go `tikvStore` as `kv.Storage`.
impl<S> tidb_txnkv::kv_storage::Storage for RealOptimisticTransactionOpener<S>
where
    S: StoreClient + 'static,
{
    fn begin(
        &self,
    ) -> Result<Box<dyn tidb_txnkv::kv_storage::Transaction>, OptimisticCoordinatorError> {
        Ok(Box::new(RealOptimisticTransactionOpener::begin(self)?))
    }
}

/// Go `tikvTxn` as `kv.Transaction`.
impl<S> tidb_txnkv::kv_storage::Transaction for RealTransaction<S>
where
    S: StoreClient + 'static,
{
    fn get(
        &mut self,
        key: &[u8],
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, OptimisticCoordinatorError> {
        self.snapshot_get(key, call).map(|result| result.value)
    }

    fn commit(
        self: Box<Self>,
        mutations: Vec<tidb_txnkv::transaction::BufferMutation>,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_txnkv::transaction::OptimisticCommitOutcome, OptimisticCoordinatorError> {
        RealTransaction::commit(*self, mutations, call)
    }

    fn finish_without_writes(
        self: Box<Self>,
    ) -> Result<tidb_txnkv::transaction::ReadOnlyTransaction, OptimisticCoordinatorError> {
        RealTransaction::finish_without_writes(*self)
    }
}
