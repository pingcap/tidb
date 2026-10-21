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
use tidb_executor::storage::StorageError;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{OptimisticCoordinatorError, StoreClient};
use tidb_txnkv::transaction::{RealOptimisticTransactionOpener, RealTransaction};
use tidb_txnkv::Key;

use crate::catalog_reload::{reload_cluster_catalog, ReloadedCatalog};
use crate::cluster_catalog::{
    load_cluster_catalog, prefix_scan_end, ClusterCatalog, ClusterCatalogError, MetaPairs,
    MetaSnapshot, PagedMetaSnapshot,
};

/// A transaction's read failure as a catalog load reports it: a storage
/// error Go's `ToTiDBErr` gives a SQL identity (`ErrTxnAbortedByGC`, an
/// exhausted backoff) keeps it, the rest stay diagnostics.
fn coordinator_catalog_error(error: OptimisticCoordinatorError) -> ClusterCatalogError {
    match error {
        OptimisticCoordinatorError::SnapshotBackoff { .. }
        | OptimisticCoordinatorError::Storage(_)
        | OptimisticCoordinatorError::Visibility(_) => {
            let sql = crate::cluster_table_storage::coordinator_sql_error(error);
            ClusterCatalogError::Sql(tidb_executor::MysqlError::from_parts(
                sql.code,
                sql.state,
                sql.message,
            ))
        }
        other => ClusterCatalogError::Snapshot(other.to_string()),
    }
}

/// [`coordinator_catalog_error`] for a read through the executor's
/// [`ClusterSnapshot`] seam, whose typed failures are already SQL errors.
fn storage_catalog_error(error: StorageError) -> ClusterCatalogError {
    match error {
        StorageError::Sql(error) => ClusterCatalogError::Sql(error),
        other => ClusterCatalogError::Snapshot(other.to_string()),
    }
}

/// One live transaction seen as a meta-key snapshot.
///
/// Holding the transaction for the whole load is the point: every key the
/// loader reads is served at the same `start_ts`.
pub struct TransactionMetaSnapshot<'transaction, S: tidb_txnkv::transaction::StoreClient> {
    transaction: &'transaction mut RealTransaction<S>,
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
    /// The node's live row-ID allocators, for a writer that inserts.
    row_ids: Option<std::sync::Arc<dyn crate::table_from_meta::TableAutoIds>>,
}

impl<'transaction, S: tidb_txnkv::transaction::StoreClient>
    TransactionMetaSnapshot<'transaction, S>
{
    /// Binds one transaction and the per-request deadline every read stamps.
    pub fn new(transaction: &'transaction mut RealTransaction<S>, timeout: Duration) -> Self {
        Self {
            transaction,
            timeout,
            row_ids: None,
        }
    }

    /// Lets a writer allocate new rows' ids from `ids` (see
    /// [`MetaSnapshot::row_ids`]).
    #[must_use]
    pub fn with_row_ids(
        mut self,
        ids: std::sync::Arc<dyn crate::table_from_meta::TableAutoIds>,
    ) -> Self {
        self.row_ids = Some(ids);
        self
    }

    /// [`Self::with_row_ids`] when `ids` is given.
    #[must_use]
    pub fn with_row_ids_opt(
        mut self,
        ids: Option<&std::sync::Arc<dyn crate::table_from_meta::TableAutoIds>>,
    ) -> Self {
        self.row_ids = ids.cloned();
        self
    }
}

impl<S> MetaSnapshot for TransactionMetaSnapshot<'_, S>
where
    S: tidb_txnkv::transaction::StoreClient,
{
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        let call = UnaryCallContext::with_timeout(self.timeout);
        self.transaction
            .snapshot_get(key, &call)
            .map(|result| result.value)
            .map_err(coordinator_catalog_error)
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
            .map_err(coordinator_catalog_error)
    }

    fn scan_range(&mut self, start: &[u8], end: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let call = UnaryCallContext::with_timeout(self.timeout);
        self.transaction
            .snapshot_scan(start, end, None, &call)
            .map_err(coordinator_catalog_error)
    }

    fn row_ids(&self) -> Option<&dyn crate::table_from_meta::TableAutoIds> {
        self.row_ids.as_deref()
    }
}

impl<S> PagedMetaSnapshot for TransactionMetaSnapshot<'_, S>
where
    S: tidb_txnkv::transaction::StoreClient,
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
            .map_err(coordinator_catalog_error)
    }
}

impl<S> crate::cluster_catalog::RegionPagedMetaSnapshot for TransactionMetaSnapshot<'_, S>
where
    S: tidb_txnkv::transaction::StoreClient,
{
    fn scan_regions(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<tidb_txnkv::transaction::SnapshotScanRegion>, ClusterCatalogError> {
        let call = UnaryCallContext::with_timeout(self.timeout);
        self.transaction
            .snapshot_scan_regions(start, end, &call)
            .map_err(coordinator_catalog_error)
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
    /// The node's live row-ID allocators, for a writer that inserts.
    row_ids: Option<std::sync::Arc<dyn crate::table_from_meta::TableAutoIds>>,
    /// The node's auto-ID service client.
    auto_ids: Option<std::sync::Arc<crate::auto_id_client::AutoIdClient>>,
}

impl SnapshotMetaSnapshot {
    /// Returns the same snapshot after reading metadata, so rows share its timestamp.
    pub fn into_snapshot(self) -> Box<dyn ClusterSnapshot> {
        self.snapshot
    }

    /// Binds one read handle.
    #[must_use]
    pub fn new(snapshot: Box<dyn ClusterSnapshot>) -> Self {
        Self {
            snapshot,
            row_ids: None,
            auto_ids: None,
        }
    }

    /// Lets a job reach the auto-ID service (see
    /// [`MetaSnapshot::auto_id_client`]).
    #[must_use]
    pub fn with_auto_ids_opt(
        mut self,
        auto_ids: Option<&std::sync::Arc<crate::auto_id_client::AutoIdClient>>,
    ) -> Self {
        self.auto_ids = auto_ids.cloned();
        self
    }

    /// Lets a writer allocate new rows' ids from `ids` (see
    /// [`MetaSnapshot::row_ids`]).
    #[must_use]
    pub fn with_row_ids(
        mut self,
        ids: std::sync::Arc<dyn crate::table_from_meta::TableAutoIds>,
    ) -> Self {
        self.row_ids = Some(ids);
        self
    }

    /// [`Self::with_row_ids`] when `ids` is given.
    #[must_use]
    pub fn with_row_ids_opt(
        mut self,
        ids: Option<&std::sync::Arc<dyn crate::table_from_meta::TableAutoIds>>,
    ) -> Self {
        self.row_ids = ids.cloned();
        self
    }
}

impl MetaSnapshot for SnapshotMetaSnapshot {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        self.snapshot
            .get(&Key::from_bytes(key.to_vec()))
            .map_err(storage_catalog_error)
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
            .map_err(storage_catalog_error)
    }

    fn scan_range(&mut self, start: &[u8], end: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        self.snapshot
            .scan(
                &Key::from_bytes(start.to_vec()),
                &Key::from_bytes(end.to_vec()),
                None,
            )
            .map_err(storage_catalog_error)
    }

    fn row_ids(&self) -> Option<&dyn crate::table_from_meta::TableAutoIds> {
        self.row_ids.as_deref()
    }

    fn auto_id_client(&self) -> Option<&std::sync::Arc<crate::auto_id_client::AutoIdClient>> {
        self.auto_ids.as_ref()
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
            .map_err(storage_catalog_error)
    }
}

/// Loads the whole cluster catalog through one fresh transaction.
///
/// The transaction is opened for reading only and finished without writes, so
/// the load leaves no locks behind and consumes exactly one PD timestamp.
pub fn load_catalog_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<ClusterCatalog, ClusterCatalogError> {
    load_catalog_from_cluster_at(opener, timeout).map(|(catalog, _)| catalog)
}

/// [`load_catalog_from_cluster`] together with the PD timestamp the load read
/// at: Go's `version` in the domain's startup `Syncer.Reload`
/// (`issyncer/syncer.go:420-427`), which grants the schema validator its
/// first lease (`schemaValidator.Update(version, ...)`, `:494`) before the
/// node serves any statement.
pub fn load_catalog_from_cluster_at<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<(ClusterCatalog, u64), ClusterCatalogError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(coordinator_catalog_error)?;
    let lease_grant_ts = transaction.start_ts();
    let catalog = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        load_cluster_catalog(&mut snapshot)?
    };
    transaction
        .finish_without_writes()
        .map_err(coordinator_catalog_error)?;
    Ok((catalog, lease_grant_ts))
}

/// Brings one loaded catalog up to date through one fresh transaction.
///
/// One PD timestamp per pass, exactly like the startup load: the version, the
/// diffs, and any object a diff points at are all read at that one timestamp,
/// so a pass never publishes a blend of two schema versions. The transaction
/// is read-only and finished without writes, leaving no locks behind.
pub fn reload_catalog_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    current: &ClusterCatalog,
) -> Result<ReloadedCatalogAt, ClusterCatalogError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(coordinator_catalog_error)?;
    let lease_grant_ts = transaction.start_ts();
    let reloaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        reload_cluster_catalog(&mut snapshot, current)?
    };
    transaction
        .finish_without_writes()
        .map_err(coordinator_catalog_error)?;
    Ok(ReloadedCatalogAt {
        lease_grant_ts,
        reloaded,
    })
}

/// One reload pass's result together with the timestamp it was read at.
#[derive(Clone, Debug)]
pub struct ReloadedCatalogAt {
    /// The PD timestamp the pass read at: Go's `version` in
    /// `Syncer.Reload` (`issyncer/syncer.go:420-425`), which is also the
    /// schema validator's lease grant (`schemaValidator.Update(version, ...)`,
    /// `:494`).
    pub lease_grant_ts: u64,
    /// What the pass did.
    pub reloaded: ReloadedCatalog,
}

/// What Go's `Syncer.Reload` does with a load's result
/// (`issyncer/syncer.go:436-495`): a full load resets the validator ("full
/// load and reset schema validator"); every pass, hit or not, renews the
/// lease at the timestamp the pass read at
/// (`schemaValidator.Update(version, oldSchemaVersion, newVersion, changes)`),
/// recording the diffs' changed tables when the version moved. Go's cache
/// hit reports `oldSchemaVersion` as 0 (`loader.go:196`), and so does an
/// unchanged pass here.
pub fn note_reload(
    validator: &crate::schema_validator::SchemaValidator,
    current_version: i64,
    at: ReloadedCatalogAt,
) -> crate::catalog_watch::CatalogReloadPass {
    use crate::schema_validator::Validator as _;
    let ReloadedCatalogAt {
        lease_grant_ts,
        reloaded,
    } = at;
    match reloaded {
        ReloadedCatalog::Unchanged { version } => {
            validator.update(lease_grant_ts, 0, version, None);
            crate::catalog_watch::CatalogReloadPass::Unchanged
        }
        ReloadedCatalog::Diffs {
            catalog, changes, ..
        } => {
            validator.update(
                lease_grant_ts,
                current_version,
                catalog.schema_version,
                Some(&changes),
            );
            crate::catalog_watch::CatalogReloadPass::Diffs(catalog)
        }
        ReloadedCatalog::Full { catalog, reason } => {
            // Go `issyncer.Syncer.Reload`'s line for a full load.
            tidb_util::logutil::bg_logger().info("full load and reset schema validator", &[]);
            // Not log content: rust/scripts/run-realtikv-ddl.sh reads this
            // JSON line from stderr.
            eprintln!(
                "{{\"event\":\"catalog_full_reload\",\"schema_version\":{},\"reason\":\"{reason}\"}}",
                catalog.schema_version
            );
            validator.reset();
            validator.update(
                lease_grant_ts,
                current_version,
                catalog.schema_version,
                None,
            );
            crate::catalog_watch::CatalogReloadPass::Full(catalog)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `GetSnapshotInfoSchema` reads the meta at the snapshot through the
    /// store driver, whose `ToTiDBErr` turns client-go's `ErrTxnAbortedByGC`
    /// into `ErrTxnAbortedByGC` (9006); the catalog load must hand that error
    /// on unwrapped rather than as a 1105 diagnostic.
    #[test]
    fn a_gc_aborted_meta_read_keeps_its_sql_identity() {
        let aborted = tidb_txnkv::VisibilityError::TxnAbortedByGc {
            txn_start_ts: 449,
            txn_safe_point: 450,
        };
        let expected = aborted.to_string();
        let ClusterCatalogError::Sql(error) =
            coordinator_catalog_error(OptimisticCoordinatorError::Visibility(aborted))
        else {
            panic!("a GC-aborted meta read lost its SQL identity")
        };
        assert_eq!(
            (error.code, error.message.as_str()),
            (9006, expected.as_str())
        );
        assert_eq!(error.state, *b"HY000");

        let statement_error = tidb_executor::MysqlError::sql_error(9006, expected);
        assert_eq!(
            storage_catalog_error(StorageError::Sql(statement_error.clone())),
            ClusterCatalogError::Sql(statement_error)
        );
        assert_eq!(
            storage_catalog_error(StorageError::Backend("boom".to_owned())),
            ClusterCatalogError::Snapshot("storage error: boom".to_owned())
        );
    }
}
