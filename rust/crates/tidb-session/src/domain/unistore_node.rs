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

//! The `--store unistore` run path: the same SQL node over the embedded store.
//!
//! Go boundary: `cmd/tidb-server` registers the unistore driver
//! (`session.RegisterStore("unistore", mockstore.EmbedUnistoreDriver{})`) and
//! everything above `kv.Storage` runs unchanged. This module is that
//! registration's Rust half: it builds the in-process capability triple
//! (client, region plane, TSO) from `tidb-unistore`, derives the SAME
//! ordinary session/catalog lifecycle used by TiKV, with the embedded store
//! supplying the same storage capabilities. Bootstrap and catalog reloads run
//! against that store; no PD or etcd connection is needed.

use crate::domain::cluster_session_node::DomainLoginPolicy as _;
use std::sync::Arc;

use tidb_distsql::cop_paging::CopRuntimeConfig;
use tidb_cluster::real_tikv_read::RealTiKvSessionTransportFactory;
use tidb_txnkv::driver::tikv_store::TikvStore;
#[cfg(test)]
use tidb_unistore::kv_handler::KvHandler;
#[cfg(test)]
use tidb_unistore::mvcc_store::MvccStore;
#[cfg(test)]
use tidb_unistore::tso::InProcessPd;

pub use tidb_ddl::embedded_store::{in_process_stack, InProcessStack, IN_PROCESS_TIMEOUT};
#[cfg(test)]
pub use tidb_ddl::embedded_store::{
    embedded_tikv_store, in_process_write_stack, InProcessOpener,
};
#[cfg(test)]
use tidb_pd_client::ClusterSecurity;

use crate::domain::session_api::SqlQueryError;


/// The transport an in-process session reads through: Go's coprocessor over
/// the node's client-rust store, whose region cache and sender reach the
/// embedded store through unistore's PD and RPC clients.
pub type InProcessReadTransport =
    tidb_distsql::cop_paging::NativeCopTransport<TikvStore<tidb_unistore::mock::StorePdClient>>;

/// The read-session factory over the embedded store, mirror of
/// `ProductionReadSessionFactory`: cloneable handles only, no lifecycle
/// ownership, no second worker per session.
pub struct InProcessReadSessionFactory {
    tikv_store: Arc<TikvStore<tidb_unistore::mock::StorePdClient>>,
}

impl RealTiKvSessionTransportFactory for InProcessReadSessionFactory {
    type Transport = InProcessReadTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        Ok(tidb_distsql::cop_paging::NativeCopTransport::new(
            Arc::clone(&self.tikv_store),
            CopRuntimeConfig {
                default_timeout: IN_PROCESS_TIMEOUT,
                ..CopRuntimeConfig::default()
            },
        ))
    }
}

#[cfg(test)]
mod transaction_buffer_tests {
    use super::*;
    use tidb_cluster::cluster_table_storage::SessionTransaction;
    use tidb_executor::cluster_storage::MutationBuffer;
    use tidb_txnkv::{transaction::BufferMutation, AssertionOp, Key, UnaryCallContext};

    /// Go `mockstore`'s `tikv.NewKVStore` over unistore: the embedded node's
    /// store loads its txn safe point from the mock PD (nothing GCed yet, so
    /// every start ts reads) and answers `CheckVisibility` from client-rust's
    /// cache, so a read below a safe point is Go's GC abort, not a pass.
    #[test]
    fn the_embedded_store_checks_visibility_against_its_txn_safe_point() {
        let pd = InProcessPd::new();
        let regions = Arc::new(tidb_unistore::region_manager::InProcessRegionManager::new());
        let handler = Arc::new(std::sync::Mutex::new(KvHandler::new(
            MvccStore::with_pd(pd.oracle()),
            Arc::clone(&regions),
        )));
        let (store, _rpc) =
            embedded_tikv_store(handler, regions, &pd, &ClusterSecurity::plaintext()).unwrap();
        let visibility = store.visibility();
        assert_eq!(visibility(1), Ok(()));

        store
            .store()
            .update_txn_safe_point_cache(500, std::time::SystemTime::now());
        assert_eq!(visibility(500), Ok(()));
        assert_eq!(
            visibility(499),
            Err(tidb_txnkv::VisibilityError::TxnAbortedByGc {
                txn_start_ts: 499,
                txn_safe_point: 500,
            })
        );
    }

    #[test]
    fn unbound_session_commit_preserves_deleted_insert_constraint() {
        let (_pd, opener) = in_process_write_stack().unwrap();
        let call = UnaryCallContext::with_timeout(IN_PROCESS_TIMEOUT);
        let key = Key::from_bytes(b"existing".to_vec());
        let seed = opener.begin().unwrap();
        seed.commit(
            vec![BufferMutation::set(key.as_bytes(), b"original").unwrap()],
            &call,
        )
        .unwrap();

        let transaction = SessionTransaction::begin(
            Arc::new(opener.clone()),
            IN_PROCESS_TIMEOUT,
            tidb_cluster::session_commit_protocol::bootstrap_commit_protocol(),
        )
        .unwrap();
        let buffer = MutationBuffer::new();
        buffer
            .stage_owned_batch([
                (
                    key.clone(),
                    Some(b"replacement".to_vec()),
                    true,
                    AssertionOp::AssertUnknown,
                ),
                (key.clone(), None, false, AssertionOp::AssertNone),
            ])
            .unwrap();
        buffer.mark_presume_key_not_exists_with_hint(&key, "existing", "PRIMARY");
        let error = transaction
            .commit(&buffer)
            .expect_err("deleting a lazy insert must retain its native CheckNotExists mutation");
        assert_eq!(error.code, 1062, "{error:?}");
        assert_eq!(buffer.native_owner(), None);
        assert!(buffer.is_empty());
        let mut reader = opener.begin().unwrap();
        assert_eq!(
            reader.snapshot_get(key.as_bytes(), &call).unwrap().value,
            Some(b"original".to_vec())
        );
        reader.finish_without_writes().unwrap();
    }

    #[test]
    fn bound_session_commit_checks_tables_from_appended_mutations() {
        use tidb_txnkv::transaction::{SchemaLeaseChecker, SchemaLeaseError};
        #[derive(Default)]
        struct CheckedTables(std::sync::Mutex<Vec<Vec<i64>>>);
        impl SchemaLeaseChecker for CheckedTables {
            fn check_by_schema_ver(&self, _: u64, tables: &[i64]) -> Result<(), SchemaLeaseError> {
                self.0.lock().unwrap().push(tables.to_vec());
                Ok(())
            }
        }
        let (_pd, opener) = in_process_write_stack().unwrap();
        let mut transaction = SessionTransaction::begin(
            Arc::new(opener),
            IN_PROCESS_TIMEOUT,
            tidb_cluster::session_commit_protocol::bootstrap_commit_protocol(),
        )
        .unwrap();
        let checked = Arc::new(CheckedTables::default());
        transaction.set_schema_lease_checker(checked.clone());
        let buffer = MutationBuffer::new();
        transaction.bind_mutation_buffer(&buffer);
        let table_key = |id| {
            tidb_tablecodec::table_key::encode_row_key_with_handle(
                id,
                &tidb_tablecodec::table_key::RecordHandle::Int(1),
            )
        };
        buffer
            .set(Key::from_bytes(table_key(10)), b"row".to_vec())
            .unwrap();
        transaction
            .commit_with(
                &buffer,
                vec![BufferMutation::set(table_key(20), b"row").unwrap()],
            )
            .unwrap();
        let tables = checked.0.lock().unwrap();
        assert!(!tables.is_empty(), "commit must check the schema lease");
        assert!(tables.iter().all(|ids| ids == &[10, 20]), "{tables:?}");
    }

    #[test]
    fn restricted_session_commit_keeps_locks_when_attaching_an_empty_sql_handle() {
        let (_pd, opener) = in_process_write_stack().unwrap();
        let transaction = SessionTransaction::begin_pessimistic(
            Arc::new(opener.clone()),
            IN_PROCESS_TIMEOUT,
            tidb_cluster::session_commit_protocol::bootstrap_commit_protocol(),
        )
        .unwrap();
        let key = b"locked".to_vec();
        assert!(matches!(
            transaction.lock_keys(vec![key.clone()]).unwrap(),
            tidb_cluster::cluster_table_storage::LockKeysOutcome::Locked { .. }
        ));
        transaction
            .commit_with(
                &MutationBuffer::new(),
                vec![BufferMutation::set(key.clone(), b"value").unwrap()],
            )
            .unwrap();
        let mut reader = opener.begin().unwrap();
        let call = UnaryCallContext::with_timeout(IN_PROCESS_TIMEOUT);
        assert_eq!(
            reader.snapshot_get(&key, &call).unwrap().value,
            Some(b"value".to_vec())
        );
        reader.finish_without_writes().unwrap();
    }
}


/// The cluster-session factory over the embedded store, plus the guards
/// that keep its store and reload threads alive. One build, two callers:
/// the running node and the in-tree tests that pin coprocessor-backed
/// execution -- a test over this stack exercises the SAME bootstrap,
/// catalog, and coprocessor client the live `--store unistore
/// --cluster-session` process serves.
pub struct UnistoreClusterStack {
    pub factory: Arc<crate::domain::cluster_session_node::Domain>,
    pub schema_version: i64,
    pub stats: Arc<tidb_exec::stats_watch::SharedStats>,
    pub _stats_maintenance:
        Option<crate::domain::cluster_session_node::stats_maintenance::StatsMaintenanceWorker>,
    // Guards, dropped in declaration order: reload threads first, then the
    // store they read from.
    pub _reloader: tidb_cluster::catalog_watch::CatalogReloader,
    pub _sysvar_reloader: crate::domain::cluster_sysvar_seam::SysvarReloader,
    pub _stats_reloader: tidb_exec::stats_watch::StatsReloader,
    pub _binding_reloader: crate::domain::cluster_binding_seam::BindingReloader,
    pub _async_stats_loader: tidb_exec::stats_watch::AsyncStatsLoader,
    /// See [`InProcessStack::rpc`].
    pub rpc: tidb_unistore::rpc::RpcClient,
    /// Go `initResourceGroupsController` over unistore's mock PD client.
    pub _resource_groups: EmbeddedResourceGroups,
}

/// The embedded store's resource-groups controller, stopped when the stack
/// drops (Go `Domain.Close`).
pub struct EmbeddedResourceGroups {
    store: Arc<TikvStore<tidb_unistore::mock::StorePdClient>>,
    controller: Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>,
}

impl Drop for EmbeddedResourceGroups {
    fn drop(&mut self) {
        crate::domain::cluster_stack::stop_resource_groups_controller(
            &self.store,
            &self.controller,
        );
    }
}

/// Builds the wide cluster-session factory over the embedded store:
/// bootstrap publish, account seeding, catalog/stats/sysvar followers, and
/// the in-process coprocessor.
pub fn unistore_cluster_session_stack(
    config: &crate::domain::node_config::NodeConfig,
    users: &Arc<crate::domain::configured_user_store::ConfiguredUserStore>,
    spill_storage: Option<Arc<tidb_util::spill_storage::SpillStorage>>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<UnistoreClusterStack, crate::domain::real_tikv_node::RunConfiguredNodeError> {
    unistore_cluster_session_stack_from(config, users, spill_storage, memory_arbitrator, None, 0)
}

/// Go `mockstore.WithMockTiFlash(nodes)`: `nodes` stores labelled
/// `engine=tiflash` (`tiflash0`, `tiflash1`, ...), each with a peer on every
/// region (Go adds it to the only region before the first split).
fn with_mock_tiflash(regions: &tidb_unistore::region_manager::InProcessRegionManager, nodes: usize) {
    for index in 0..nodes {
        let store_id = regions.alloc_id();
        regions.add_store(
            store_id,
            &format!("tiflash{index}"),
            vec![tidb_proto::metapb::StoreLabel {
                key: "engine".to_owned(),
                value: "tiflash".to_owned(),
            }],
        );
        for (region, _) in regions.scan_regions(b"", b"", 0) {
            regions.add_peer(region.id, store_id, regions.alloc_id());
        }
    }
}

/// Go testkit's bootstrapped store image (`tryMakeImage`): a store's
/// committed data right after `session.BootstrapSession`, plus what the
/// bootstrap did outside the store, so a later store starts bootstrapped
/// instead of running the bootstrap again.
#[derive(Clone, Debug)]
pub struct BootstrapImage {
    /// [`tidb_unistore::mvcc_store::MvccStore::data_image`] of the store.
    pub data: Vec<u8>,
    /// The schema version the bootstrap published.
    pub schema_version: i64,
    /// The region splits the bootstrap's tables owe.
    pub splits: tidb_exec::mysql_bootstrap::BootstrapSplits,
    /// The resource manager write the bootstrap made.
    pub resource_group_update: tidb_ddl::cluster_ddl::ResourceGroupUpdate,
}

const BOOTSTRAP_IMAGE_MAGIC: &[u8; 8] = b"TIDBIMG1";

impl BootstrapImage {
    /// Bootstraps a fresh embedded store and takes its image.
    pub fn make(security: &tidb_pd_client::ClusterSecurity) -> Result<Self, String> {
        let stack = in_process_stack(security).map_err(|error| error.message)?;
        // The bootstrap's resource group write lands in the mock resource
        // manager, as the stack's own boot initializes it.
        tidb_domain::resource_manager_client::init_resource_manager_client(None);
        let published = tidb_exec::bootstrap_publish::publish_bootstrap_with_effects(
            &stack.opener,
            IN_PROCESS_TIMEOUT,
        )?;
        tidb_exec::bootstrap_publish::notify_committed_bootstrap(
            &published.outcome,
            published.schema_version,
            None,
        )?;
        // Go's image is a copy of a CLOSED store: `KVStore.Close` waits for
        // the async-commit secondaries the bootstrap's commits left running,
        // so no committed transaction's lock is in it. A lock carried into
        // the image would park the first pessimistic lock on its key for the
        // store's whole lock-wait timeout before the resolver ran.
        let drained_by = std::time::Instant::now() + IN_PROCESS_TIMEOUT;
        while stack
            .handler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .store
            .has_locks()
            && std::time::Instant::now() < drained_by
        {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let data = stack
            .handler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .store
            .data_image();
        Ok(Self {
            data,
            schema_version: published.schema_version,
            splits: published.splits,
            resource_group_update: published.resource_group_update,
        })
    }

    /// The image as bytes, for a file other processes start from.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        use prost::Message as _;
        use tidb_ddl::cluster_ddl::ResourceGroupUpdate;
        fn put(out: &mut Vec<u8>, bytes: &[u8]) {
            out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        fn put_ids(out: &mut Vec<u8>, ids: &[i64]) {
            out.extend_from_slice(&(ids.len() as u64).to_le_bytes());
            for id in ids {
                out.extend_from_slice(&id.to_le_bytes());
            }
        }
        let mut out = BOOTSTRAP_IMAGE_MAGIC.to_vec();
        out.extend_from_slice(&self.schema_version.to_le_bytes());
        out.extend_from_slice(&(self.splits.global_batches.len() as u64).to_le_bytes());
        for batch in &self.splits.global_batches {
            put_ids(&mut out, batch);
        }
        put_ids(&mut out, &self.splits.table_ids);
        match &self.resource_group_update {
            ResourceGroupUpdate::Add(group) => {
                out.push(0);
                put(&mut out, &group.encode_to_vec());
            }
            ResourceGroupUpdate::Modify(group) => {
                out.push(1);
                put(&mut out, &group.encode_to_vec());
            }
            ResourceGroupUpdate::Delete(name) => {
                out.push(2);
                put(&mut out, name.as_bytes());
            }
        }
        put(&mut out, &self.data);
        out
    }

    /// Reads [`Self::to_bytes`] back.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        use prost::Message as _;
        use tidb_ddl::cluster_ddl::ResourceGroupUpdate;
        struct Reader<'a>(&'a [u8]);
        impl<'a> Reader<'a> {
            fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
                if self.0.len() < n {
                    return Err("truncated bootstrap image".to_owned());
                }
                let (head, rest) = self.0.split_at(n);
                self.0 = rest;
                Ok(head)
            }
            fn u64(&mut self) -> Result<u64, String> {
                Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
            }
            fn len(&mut self) -> Result<usize, String> {
                usize::try_from(self.u64()?).map_err(|error| error.to_string())
            }
            fn bytes(&mut self) -> Result<&'a [u8], String> {
                let len = self.len()?;
                self.take(len)
            }
            fn ids(&mut self) -> Result<Vec<i64>, String> {
                let len = self.len()?;
                (0..len).map(|_| self.u64().map(|id| id as i64)).collect()
            }
        }
        let mut reader = Reader(bytes);
        if reader.take(BOOTSTRAP_IMAGE_MAGIC.len())? != BOOTSTRAP_IMAGE_MAGIC {
            return Err("not a bootstrap image".to_owned());
        }
        let schema_version = reader.u64()? as i64;
        let batches = reader.len()?;
        let global_batches = (0..batches)
            .map(|_| reader.ids())
            .collect::<Result<Vec<_>, _>>()?;
        let table_ids = reader.ids()?;
        let tag = reader.take(1)?[0];
        let payload = reader.bytes()?;
        let decode = |payload: &[u8]| {
            tidb_proto::resource_manager::ResourceGroup::decode(payload)
                .map(Box::new)
                .map_err(|error| error.to_string())
        };
        let resource_group_update = match tag {
            0 => ResourceGroupUpdate::Add(decode(payload)?),
            1 => ResourceGroupUpdate::Modify(decode(payload)?),
            2 => ResourceGroupUpdate::Delete(
                String::from_utf8(payload.to_vec()).map_err(|error| error.to_string())?,
            ),
            other => return Err(format!("unknown resource group update {other}")),
        };
        let data = reader.bytes()?.to_vec();
        Ok(Self {
            data,
            schema_version,
            splits: tidb_exec::mysql_bootstrap::BootstrapSplits {
                global_batches,
                table_ids,
            },
            resource_group_update,
        })
    }
}

/// [`unistore_cluster_session_stack`] over a store that starts from `image`
/// when one is given, skipping the bootstrap it already holds.
pub fn unistore_cluster_session_stack_from(
    config: &crate::domain::node_config::NodeConfig,
    users: &Arc<crate::domain::configured_user_store::ConfiguredUserStore>,
    spill_storage: Option<Arc<tidb_util::spill_storage::SpillStorage>>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
    image: Option<&BootstrapImage>,
    mock_tiflash: usize,
) -> Result<UnistoreClusterStack, crate::domain::real_tikv_node::RunConfiguredNodeError> {
    use crate::domain::cluster_session_node::{Domain, RealClusterDdl, RealClusterTransactions};
    use crate::domain::real_tikv_node::RunConfiguredNodeError;

    let engine = RunConfiguredNodeError::Engine;
    let InProcessStack {
        pd: _pd,
        opener,
        regions,
        tikv_store,
        rpc,
        handler: _handler,
    } = tidb_ddl::embedded_store::in_process_stack_from_image(
        &config.cluster_security,
        image.map(|image| image.data.as_slice()),
    )
    .map_err(engine)?;
    with_mock_tiflash(&regions, mock_tiflash);
    // The store's regions as `SPLIT`, `SHOW TABLE ... REGIONS` and
    // `TABLESAMPLE REGIONS()` reach them: client-go's `SplitRegions` and
    // `LoadRegionsInKeyRange` over the store, as Go's mockstore runs them.
    let region_store: Arc<dyn tidb_kv::region_map::RegionStore> = Arc::new(
        tidb_exec::tikv_region_store::TikvRegionStore::new((*tikv_store).clone(), Vec::new()),
    );
    // Go `affinity.InitManager(nil)`: unistore has no PD HTTP client, so the
    // affinity groups live in the mock manager.
    tidb_domain::affinity::init_manager(None);
    // Go `initLabelRuleManager` without PD: a fresh `mockLabelManager`.
    tidb_domain::label_manager::init_label_rule_manager(None);
    // Go `initResourceManagerClient(nil)`: the mock resource manager.
    tidb_domain::resource_manager_client::init_resource_manager_client(None);
    // Go `initDomainSysVars` registering `setGlobalResourceControl` for every
    // store, before any global variable loads: `tidb_enable_resource_control`
    // turns client-rust's resource-control dispatch on and off.
    *crate::vars::SET_GLOBAL_RESOURCE_CONTROL
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(|enable| {
        if enable {
            tikv_client::enable_resource_control();
        } else {
            tikv_client::disable_resource_control();
        }
    }));
    // Go `initResourceGroupsController(ctx, store.GetPDClient(), serverID)`:
    // unistore's PD client embeds that mock, and the controller over it
    // prices every request and response into the statement's RU details.
    let resource_groups = EmbeddedResourceGroups {
        controller: crate::domain::cluster_stack::start_resource_groups_controller_over(
            &tikv_store,
            0,
            Arc::new(
                crate::domain::embedded_resource_groups::EmbeddedResourceGroupProvider::default(),
            ),
        )?,
        store: Arc::clone(&tikv_store),
    };

    // The bootstrap: every boot, because the store is empty every boot --
    // unless it starts from a bootstrapped image, whose resource manager
    // write is replayed instead.
    let (schema_version, bootstrap_splits) = match image {
        None => {
            let bootstrap_started = std::time::Instant::now();
            let (outcome, schema_version, bootstrap_splits) =
                tidb_exec::bootstrap_publish::publish_bootstrap_with_splits(
                    &opener,
                    IN_PROCESS_TIMEOUT,
                )
                .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;
            let schema_version = tidb_exec::bootstrap_publish::notify_committed_bootstrap(
                &outcome,
                schema_version,
                None,
            )
            .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;
            tidb_exec::bootstrap_publish::log_bootstrap_successful(bootstrap_started);
            (schema_version, bootstrap_splits)
        }
        Some(image) => {
            image
                .resource_group_update
                .apply()
                .map_err(|error| engine(SqlQueryError::unknown(error)))?;
            (image.schema_version, image.splits.clone())
        }
    };
    // Go `EnableSplitTableRegion` (config `split-table`): the bootstrap cut
    // every system table's region on the fresh store.
    if tidb_config::config_tree::config::get_global_config().split_table {
        tidb_exec::pre_split::split_bootstrap_tables(region_store.as_ref(), &bootstrap_splits);
    }

    // Go's bootstrap INSERTs `root@%` into `mysql.user`; this node provisions
    // its startup identity from `--auth-file` instead, so it has to become a
    // real row before any account statement rewrites the set -- see
    // `cluster_account_seam::seed_cluster_accounts` for what happened when it
    // did not.
    // Go's root INSERT in `doDMLWorks` logs nothing.
    crate::domain::cluster_account_seam::seed_cluster_accounts(
        &opener,
        &users.accounts(),
        IN_PROCESS_TIMEOUT,
    )
    .map_err(engine)?;

    let (startup, startup_lease_grant_ts) =
        tidb_cluster::real_tikv_catalog::load_catalog_from_cluster_at(&opener, IN_PROCESS_TIMEOUT)
            .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;

    // The bootstrap persisted the system time zone and the global-variable
    // rows this instant; reading them back through the same seam is the
    // production boot's own order.
    crate::domain::real_tikv_node::load_cluster_startup_variables(&users, &opener)?;

    // One registry for the whole node: sessions' INSERTs, the DDL owner's and
    // the statistics writers' `mysql.*` rows, and the reload's rebases.
    let node_auto_ids: Arc<dyn tidb_cluster::table_from_meta::TableAutoIds> = Arc::new(
        crate::domain::cluster_auto_id_seam::ClusterTableAutoIds::new(opener.clone(), IN_PROCESS_TIMEOUT),
    );
    let (catalog, reloader, schema_validator) =
        crate::domain::real_tikv_node::spawn_catalog_reloader(
            startup,
            startup_lease_grant_ts,
            opener.clone(),
            config.schema_lease,
            Arc::clone(&node_auto_ids),
        )
        .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;
    let (stats, stats_reloader, async_stats_loader) =
        crate::domain::real_tikv_node::spawn_node_stats(
            Arc::clone(&catalog),
            opener.clone(),
            config.stats_lease,
            IN_PROCESS_TIMEOUT,
        )
        .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;

    let sysvar_publication_fence = crate::domain::cluster_sysvar_seam::SysvarPublicationFence::default();
    let sysvar_reloader = crate::domain::cluster_sysvar_seam::SysvarReloader::spawn(
        users.global_vars(),
        opener.clone(),
        crate::domain::real_tikv_node::sysvar_reload_interval(config.schema_lease),
        IN_PROCESS_TIMEOUT,
        sysvar_publication_fence.clone(),
    )
    .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;

    let peer_store = (*tikv_store).clone();
    let transport_factory = Arc::new(InProcessReadSessionFactory { tikv_store });
    // This node's own server-info record: the id Go mints with `uuid.New()`
    // for its DDL owner, and the address/ports/labels a peer would read.
    // With no etcd client the syncer publishes nothing and answers reads
    // with this node alone -- Go's `etcdCli == nil` path, and exactly what
    // `information_schema.TIDB_SERVERS_INFO` shows on a single node.
    let server_info = Arc::new(
        tidb_domain::serverinfo_syncer::Syncer::new_with_status_endpoint_claim(
            crate::domain::serverinfo_etcd::node_server_info(config),
            None,
            config.report_status,
        ),
    );
    let stats_owner: Arc<dyn tidb_owner::Manager> = Arc::new(tidb_owner::MockManager::new(
        tidb_owner::Context::background(),
        server_info.local_server_info().static_info.id.clone(),
        // Go Domain.NewOwnerManager supplies store.UUID(). Opener clones
        // share this store authority, while independent embedded stores do not.
        Some(&format!("embedded-authority-{}", opener.authority_id())),
        crate::domain::cluster_session_node::STATS_OWNER_KEY,
    ));
    let kv_client: Arc<dyn tidb_txnkv::Client> = Arc::new(
        tidb_exec::cop_client::CopClient::new(transport_factory),
    );
    let (bindings, binding_reloader) = crate::domain::cluster_binding_seam::start_binding_cache(
        opener.clone(),
        Arc::clone(&catalog),
        users.global_vars(),
        IN_PROCESS_TIMEOUT,
        Arc::new(tidb_owner::MockManager::new(
            tidb_owner::Context::background(),
            server_info.local_server_info().static_info.id.clone(),
            Some(&format!("embedded-authority-{}", opener.authority_id())),
            "/tidb/bindinfo/owner",
        )),
    )
    .map_err(|error| engine(SqlQueryError::unknown(error)))?;
    let cluster_ddl = Arc::new(
        RealClusterDdl::new(
            opener.clone(),
            Arc::clone(&catalog),
            IN_PROCESS_TIMEOUT,
            // No etcd: schema changes announce themselves to nobody, and the
            // reload tick above is the only follower -- correct for one node.
            None,
            Arc::clone(&server_info),
            None,
            Arc::clone(&schema_validator),
            config.run_ddl,
            Arc::new(tidb_exec::ddl_session_pool::ExecutorDdlSessionPool),
            Arc::clone(&node_auto_ids),
        )
        .map_err(|error| {
            crate::domain::real_tikv_node::RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
                "campaign DDL owner failed: {error}"
            )))
        })?
        // Go's mockstore reports no delete-range support, so the DDL layer
        // emulates it.
        .with_delete_range_emulator()
        .map_err(|error| {
            crate::domain::real_tikv_node::RunConfiguredNodeError::Engine(SqlQueryError::unknown(error))
        })?,
    );

    let factory = Domain::new(
        Arc::new(RealClusterTransactions::new(
            opener.clone(),
            IN_PROCESS_TIMEOUT,
        )),
        cluster_ddl,
        Arc::new(crate::domain::cluster_account_seam::RealClusterAccountWriter::new(
            Arc::new(opener.clone()),
            users.accounts(),
            IN_PROCESS_TIMEOUT,
            None,
        )),
        Arc::new(crate::domain::cluster_sysvar_seam::RealClusterSysvarWriter::new(
            Arc::new(opener.clone()),
            users.global_vars(),
            IN_PROCESS_TIMEOUT,
            None,
            sysvar_publication_fence,
        )),
        Arc::new(
            crate::domain::cluster_analyze_seam::RealClusterAnalyze::new(
                Arc::new(opener.clone()),
                Arc::clone(&stats),
                IN_PROCESS_TIMEOUT,
                config.stats_lease.slow_save_interval(),
                Arc::clone(&node_auto_ids),
            ),
        ),
        Arc::new(crate::domain::cluster_stats_lock_seam::RealClusterStatsLock::new(
            Arc::new(opener.clone()),
            IN_PROCESS_TIMEOUT,
        )),
        catalog,
        users.accounts(),
        users.global_vars(),
        Arc::clone(&stats),
        Arc::clone(&node_auto_ids),
    )
    .with_kv_client(kv_client)
    .with_region_store(region_store)
    .with_server_info(server_info)
    // No etcd: the standalone server ID, as Go's Domain allocator names.
    .with_connection_ids(crate::domain::session_api::new_connection_id_allocator(
        &config.global_config,
        None,
    ))
    .with_cluster_peer_client(Arc::new(tidb_exec::cluster_peer::ClusterPeerClient::new(
        peer_store,
    )))
    .with_cluster_config_client(Arc::new(
        tidb_exec::cluster_config::ClusterConfigClient::new(&config.cluster_security)
            .map_err(|error| engine(SqlQueryError::unknown(error)))?,
    ))
    .with_cluster_diagnostics_client(Arc::new(
        tidb_exec::cluster_diagnostics::ClusterDiagnosticsClient::new(&config.cluster_security)
            .map_err(|error| engine(SqlQueryError::unknown(error)))?,
    ))
    .with_stats_owner(stats_owner);
    let factory = match spill_storage {
        Some(storage) => factory.with_spill_storage(storage),
        None => factory,
    };
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = factory.with_bindings(bindings);
    factory.attach_login_storage(&users);
    factory.install_bdr_role_store(Arc::new(opener), IN_PROCESS_TIMEOUT);
    factory.load_signing_cert_loop(
        &config.global_config.security.session_token_signing_cert,
        &config.global_config.security.session_token_signing_key,
    );

    let stats_maintenance =
        crate::domain::cluster_session_node::stats_maintenance::StatsMaintenanceWorker::start(
            &factory,
            config.stats_lease,
            config.schema_lease,
            async_stats_loader.initialization(),
        )
        .map_err(|error| engine(SqlQueryError::unknown(error)))?;

    Ok(UnistoreClusterStack {
        factory,
        schema_version,
        stats,
        _stats_maintenance: stats_maintenance,
        _reloader: reloader,
        _sysvar_reloader: sysvar_reloader,
        _stats_reloader: stats_reloader,
        _binding_reloader: binding_reloader,
        _async_stats_loader: async_stats_loader,
        rpc,
        _resource_groups: resource_groups,
    })
}

/// [`tidb_exec::meta_txn`] against real (embedded) storage: Go's
/// `kv.Transaction` memory-buffer contract, `meta.NewMutator(txn)`, and
/// `kv.RunInNewTxn`'s retry on a write conflict.
#[cfg(test)]
mod meta_txn_tests {
    use super::*;
    use tidb_exec::meta_txn::MetaTxnStorage;
    use tidb_meta::transaction::{Mutator, RawTransaction};
    use tidb_txnkv::{run_in_new_txn, NewTxnStorage, NewTxnTransaction, RunInNewTxnContext};

    /// The embedded stack: the PD oracle and the opener over the store.
    struct Stack {
        _pd: InProcessPd,
        opener: InProcessOpener,
    }

    fn stack() -> Stack {
        let (pd, opener) = in_process_write_stack().expect("embedded write stack");
        Stack { _pd: pd, opener }
    }

    /// A write is visible to the same transaction before commit, absent from
    /// every other transaction until then, and durable after it -- Go's memory
    /// buffer in front of the snapshot.
    #[test]
    fn writes_read_back_before_commit_and_persist_after_it() {
        let stack = stack();
        let opener = &stack.opener;
        let mut storage = MetaTxnStorage::new(opener, IN_PROCESS_TIMEOUT);

        let mut writer = storage.begin().unwrap();
        writer.set(b"mt/a".to_vec(), b"1".to_vec()).unwrap();
        assert_eq!(writer.get(b"mt/a").unwrap(), Some(b"1".to_vec()));

        let mut peer = storage.begin().unwrap();
        assert_eq!(
            peer.get(b"mt/a").unwrap(),
            None,
            "uncommitted writes stay private"
        );
        NewTxnTransaction::rollback(&mut peer).unwrap();

        NewTxnTransaction::commit(&mut writer).unwrap();
        let mut reader = storage.begin().unwrap();
        assert_eq!(reader.get(b"mt/a").unwrap(), Some(b"1".to_vec()));
        NewTxnTransaction::rollback(&mut reader).unwrap();
    }

    /// A scan merges the buffer over the snapshot: a buffered set appears, a
    /// buffered delete hides a committed key, and the order is byte order.
    #[test]
    fn scans_merge_the_buffer_over_the_snapshot() {
        let stack = stack();
        let opener = &stack.opener;
        let mut storage = MetaTxnStorage::new(opener, IN_PROCESS_TIMEOUT);
        let mut seed = storage.begin().unwrap();
        seed.set(b"ms/1".to_vec(), b"one".to_vec()).unwrap();
        seed.set(b"ms/3".to_vec(), b"three".to_vec()).unwrap();
        NewTxnTransaction::commit(&mut seed).unwrap();

        let mut txn = storage.begin().unwrap();
        txn.set(b"ms/2".to_vec(), b"two".to_vec()).unwrap();
        txn.delete(b"ms/3").unwrap();
        assert_eq!(
            txn.scan_prefix(b"ms/").unwrap(),
            vec![
                (b"ms/1".to_vec(), b"one".to_vec()),
                (b"ms/2".to_vec(), b"two".to_vec()),
            ]
        );
        NewTxnTransaction::rollback(&mut txn).unwrap();

        // The rollback discarded both buffered changes.
        let mut after = storage.begin().unwrap();
        assert_eq!(
            after.scan_prefix(b"ms/").unwrap(),
            vec![
                (b"ms/1".to_vec(), b"one".to_vec()),
                (b"ms/3".to_vec(), b"three".to_vec()),
            ]
        );
        NewTxnTransaction::rollback(&mut after).unwrap();
    }

    /// The point of the adapter: Go's ported `meta.Mutator` round-trips a meta
    /// structure through real storage. `SetRUStats` then `GetRUStats` is the
    /// pair the RU statistics writer depends on.
    #[test]
    fn the_meta_mutator_round_trips_ru_stats_through_real_storage() {
        use tidb_meta::transaction::{DailyRuStats, GroupRuStats, RuConsumption, RuStats};
        let stack = stack();
        let opener = &stack.opener;
        let mut storage = MetaTxnStorage::new(opener, IN_PROCESS_TIMEOUT);
        let stats = RuStats {
            latest: Some(Box::new(DailyRuStats {
                end_time: chrono::DateTime::parse_from_rfc3339("2026-10-08T00:00:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc),
                stats: Some(vec![GroupRuStats {
                    id: 3,
                    name: "rg1".to_owned(),
                    ru_consumption: Some(RuConsumption::default()),
                }]),
            })),
            previous: None,
        };
        run_in_new_txn(&RunInNewTxnContext::default(), &mut storage, true, |txn| {
            Mutator::new(&mut *txn).set_ru_stats(Some(&stats))?;
            Ok(())
        })
        .unwrap();

        let mut read = storage.begin().unwrap();
        let loaded = Mutator::new(&mut read).ru_stats().unwrap();
        assert_eq!(loaded, Some(stats));
        NewTxnTransaction::rollback(&mut read).unwrap();
    }

    /// Go `kv.RunInNewTxn` retries a write conflict and only that: a peer that
    /// commits the same key between this attempt's read and its commit makes
    /// the first attempt lose, and the retry re-reads and wins.
    #[test]
    fn run_in_new_txn_retries_a_write_conflict() {
        let stack = stack();
        let opener = &stack.opener;
        let mut storage = MetaTxnStorage::new(opener, IN_PROCESS_TIMEOUT);
        let mut seed = storage.begin().unwrap();
        seed.set(b"mc/k".to_vec(), b"0".to_vec()).unwrap();
        NewTxnTransaction::commit(&mut seed).unwrap();

        let mut attempts = 0;
        let peer_opener = opener;
        run_in_new_txn(&RunInNewTxnContext::default(), &mut storage, true, |txn| {
            attempts += 1;
            let current = txn.get(b"mc/k")?.unwrap_or_default();
            if attempts == 1 {
                // A peer commits the same key after this attempt read it.
                let mut peer_storage = MetaTxnStorage::new(peer_opener, IN_PROCESS_TIMEOUT);
                let mut peer = peer_storage.begin().unwrap();
                peer.set(b"mc/k".to_vec(), b"peer".to_vec()).unwrap();
                NewTxnTransaction::commit(&mut peer).unwrap();
            }
            let mut next = current.clone();
            next.extend_from_slice(b"+");
            txn.set(b"mc/k".to_vec(), next)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(attempts, 2, "the conflicted attempt retried once");

        let mut read = storage.begin().unwrap();
        assert_eq!(
            read.get(b"mc/k").unwrap(),
            Some(b"peer+".to_vec()),
            "the retry re-read the peer's value before writing"
        );
        NewTxnTransaction::rollback(&mut read).unwrap();
    }
}

/// The schema validator's state between boot and the reload loop's first
/// tick, over real (embedded) storage.
#[cfg(test)]
mod startup_schema_lease_tests {
    use super::*;
    use tidb_cluster::schema_validator::{Result as ValidatorResult, Validator as _};

    /// Go `Domain.Init` runs `isSyncer.Reload()` before the reload loop and
    /// before any session, so the startup load's timestamp is the validator's
    /// first lease (`schemaValidator.Update`, `issyncer/syncer.go:494`). A
    /// commit right after boot is `ResultSucc`; without that lease it was
    /// `ResultUnknown` until the loop's first tick `schemaLease / 2` later,
    /// and the retries ran out as 8027 on the first COMMIT after boot.
    #[test]
    fn the_startup_load_grants_the_schema_validator_its_first_lease() {
        let (_pd, opener) = in_process_write_stack().expect("embedded write stack");
        tidb_exec::bootstrap_publish::publish_bootstrap(&opener, IN_PROCESS_TIMEOUT)
            .expect("bootstrap publish");
        let (startup, lease_grant_ts) =
            tidb_cluster::real_tikv_catalog::load_catalog_from_cluster_at(&opener, IN_PROCESS_TIMEOUT)
                .expect("startup catalog");
        let version = startup.schema_version;
        assert!(lease_grant_ts > 0, "the load read at a real timestamp");
        // A lease long enough that the reload thread never ticks during the
        // test: only the startup grant can make the check succeed.
        let (_catalog, _reloader, validator) =
            crate::domain::real_tikv_node::spawn_catalog_reloader(
                startup,
                lease_grant_ts,
                opener.clone(),
                std::time::Duration::from_secs(45),
                Arc::new(tidb_cluster::table_from_meta::LocalTableAutoIds::default()),
            )
            .expect("catalog reloader");
        let transaction = opener.begin_read_only().expect("commit-time timestamp");
        let commit_ts = transaction.start_ts();
        transaction.finish_without_writes().expect("finish");
        let (_, result) = validator.check(commit_ts, version, Some(&[]), false);
        assert_eq!(result, ValidatorResult::Succ);
    }

    /// Go `doDDLWorks`' `ALTER RESOURCE GROUP default
    /// BACKGROUND=(TASK_TYPES='stats')` is a real DDL job: its group lands in
    /// meta, so a node loading the published bootstrap reads the background
    /// setting from storage rather than from an in-memory default.
    #[test]
    fn the_published_bootstrap_stores_the_default_group_background() {
        let (_pd, opener) = in_process_write_stack().expect("embedded write stack");
        tidb_exec::bootstrap_publish::publish_bootstrap(&opener, IN_PROCESS_TIMEOUT)
            .expect("bootstrap publish");
        let (startup, _) =
            tidb_cluster::real_tikv_catalog::load_catalog_from_cluster_at(&opener, IN_PROCESS_TIMEOUT)
                .expect("startup catalog");
        let default = startup
            .resource_groups
            .iter()
            .find(|group| group.name.lowercase() == tidb_model::DEFAULT_RESOURCE_GROUP_NAME)
            .expect("the default group");
        let job_types = default
            .settings
            .as_ref()
            .and_then(|settings| settings.background.as_ref())
            .and_then(|background| background.read().job_types.clone());
        assert_eq!(job_types, Some(vec!["stats".to_owned()]));
    }
}
