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

//! The production catalog load: one real transaction, one PD timestamp, one
//! consistent view of the cluster's schema.

use std::time::Duration;

use tidb_executor::cluster_storage::ClusterSnapshot;
use tidb_txnkv::lock::{LockRecoveryClient, TimestampSource};
use tidb_txnkv::region::RegionRecoveryLoader;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    RealOptimisticTransaction, RealOptimisticTransactionOpener, TransactionCommandClient,
};
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};
use tidb_txnkv::Key;

use crate::catalog_reload::{reload_cluster_catalog, ReloadedCatalog};
use crate::cluster_catalog::{
    load_cluster_catalog, prefix_scan_end, ClusterCatalog, ClusterCatalogError, MetaPairs,
    MetaSnapshot, PagedMetaSnapshot,
};

/// One live transaction seen as a meta-key snapshot.
///
/// Holding the transaction for the whole load is the point: every key the
/// loader reads is served at the same `start_ts`.
pub struct TransactionMetaSnapshot<'transaction, C, L, T> {
    transaction: &'transaction mut RealOptimisticTransaction<C, L, T>,
    /// One read request's deadline. Go bounds each storage request, not the
    /// statement issuing them: every cop task takes
    /// `TiKVClient.CoprReqTimeout` (default 120s,
    /// `pkg/store/copr/coprocessor.go:1791`) and client-go stamps its own
    /// read timeout on every kv RPC -- thousands of requests in one ANALYZE
    /// or catalog load race thousands of clocks, never one. So this is a
    /// *template*, stamped fresh per `get`/`scan_prefix`; holding one context
    /// for the whole load made the total budget equal to a single request's,
    /// and a catalog-plus-full-scan read of a real cluster blew it before its
    /// first page came back.
    timeout: Duration,
}

impl<'transaction, C, L, T> TransactionMetaSnapshot<'transaction, C, L, T> {
    /// Binds one transaction and the per-request deadline every read stamps.
    pub fn new(
        transaction: &'transaction mut RealOptimisticTransaction<C, L, T>,
        timeout: Duration,
    ) -> Self {
        Self {
            transaction,
            timeout,
        }
    }
}

impl<C, L, T> MetaSnapshot for TransactionMetaSnapshot<'_, C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        let call = UnaryCallContext::with_timeout(self.timeout);
        self.transaction
            .snapshot_get(key, &call)
            .map(|result| result.value)
            .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let Some(end) = prefix_scan_end(prefix) else {
            return Err(ClusterCatalogError::Snapshot(
                "catalog prefix has no finite scan end".to_owned(),
            ));
        };
        let call = UnaryCallContext::with_timeout(self.timeout);
        self.transaction
            .snapshot_scan(prefix, &end, None, &call)
            .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))
    }
}

impl<C, L, T> PagedMetaSnapshot for TransactionMetaSnapshot<'_, C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    fn scan_page(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<MetaPairs, ClusterCatalogError> {
        let call = UnaryCallContext::with_timeout(self.timeout);
        self.transaction
            .snapshot_scan(start, end, Some(limit), &call)
            .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))
    }
}

/// One open transaction's read handle seen as a meta-key snapshot.
///
/// [`TransactionMetaSnapshot`] borrows the transaction itself, which is right
/// for a load that owns its transaction outright. An index change cannot do
/// that: it has to read the catalog AND walk the table's rows through the
/// executor's own [`ClusterSnapshot`] seam, and both must be served at the one
/// timestamp whose write set they end up in. This adapter is what makes the
/// two the same read: a `SessionTransaction` hands out as many
/// [`ClusterSnapshot`] handles as are needed, all on the one transaction, and
/// this turns one of them back into the loader's interface.
pub struct SnapshotMetaSnapshot {
    snapshot: Box<dyn ClusterSnapshot>,
}

impl SnapshotMetaSnapshot {
    /// Binds one read handle.
    #[must_use]
    pub fn new(snapshot: Box<dyn ClusterSnapshot>) -> Self {
        Self { snapshot }
    }
}

impl MetaSnapshot for SnapshotMetaSnapshot {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        self.snapshot
            .get(&Key::from_bytes(key.to_vec()))
            .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let Some(end) = prefix_scan_end(prefix) else {
            return Err(ClusterCatalogError::Snapshot(
                "catalog prefix has no finite scan end".to_owned(),
            ));
        };
        self.snapshot
            .scan(
                &Key::from_bytes(prefix.to_vec()),
                &Key::from_bytes(end),
                None,
            )
            .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))
    }
}

impl PagedMetaSnapshot for SnapshotMetaSnapshot {
    fn scan_page(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<MetaPairs, ClusterCatalogError> {
        self.snapshot
            .scan(
                &Key::from_bytes(start.to_vec()),
                &Key::from_bytes(end.to_vec()),
                Some(limit),
            )
            .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))
    }
}

/// Loads the whole cluster catalog through one fresh transaction.
///
/// The transaction is opened for reading only and finished without writes, so
/// the load leaves no locks behind and consumes exactly one PD timestamp.
pub fn load_catalog_from_cluster<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
) -> Result<ClusterCatalog, ClusterCatalogError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))?;
    let catalog = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        load_cluster_catalog(&mut snapshot)?
    };
    transaction
        .finish_without_writes()
        .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))?;
    Ok(catalog)
}

/// Brings one loaded catalog up to date through one fresh transaction.
///
/// One PD timestamp per pass, exactly like the startup load: the version, the
/// diffs, and any object a diff points at are all read at that one timestamp,
/// so a pass never publishes a blend of two schema versions. The transaction
/// is read-only and finished without writes, leaving no locks behind.
pub fn reload_catalog_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    current: &ClusterCatalog,
) -> Result<ReloadedCatalog, ClusterCatalogError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))?;
    let reloaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        reload_cluster_catalog(&mut snapshot, current)?
    };
    transaction
        .finish_without_writes()
        .map_err(|error| ClusterCatalogError::Snapshot(error.to_string()))?;
    Ok(reloaded)
}
