//! The TiKV store the IMPORT INTO tasks of this instance import into: what
//! Go's `pkg/dxf/importinto` reaches through `kv.Storage` (its PD client,
//! codec and allocators) and `infosync`.

use std::sync::Arc;

use tikv_client::PdClient as _;

use tidb_domain_core::domain::Domain;
use tidb_lightning_backend_kv::AllocatorType;
use tidb_txnkv::driver::tikv_store::TikvStore;

/// [`tidb_dxf_importinto::ImportStore`] over the node's TiKV store.
pub(crate) struct TikvImportStore {
    store: TikvStore,
    domain: Arc<Domain>,
}

impl TikvImportStore {
    pub(crate) fn new(store: TikvStore, domain: Arc<Domain>) -> Self {
        Self { store, domain }
    }
}

impl tidb_dxf_importinto::ImportStore for TikvImportStore {
    fn import_client(&self) -> Result<Arc<dyn tidb_ingestor_ingestctrl::region_job::ImportClient>, String> {
        let security = tidb_config::config_tree::config::get_global_config().security.clone();
        let security = tidb_pd_client::ClusterSecurity::new(
            security.cluster_ssl_ca,
            security.cluster_ssl_cert,
            security.cluster_ssl_key,
            security.cluster_verify_cn,
        );
        Ok(Arc::new(tidb_ingestor_ingestctrl::region_job::RemoteImportClient::new(self.store.clone(), security)))
    }

    /// Go `codecV2.GetKeyspace`: `'x'` and the 3-byte keyspace ID; empty
    /// under API V1.
    fn keyspace(&self) -> Vec<u8> {
        match self.store.store().keyspace() {
            tikv_client::request::Keyspace::Enable { keyspace_id: id } => {
                vec![b'x', (id >> 16) as u8, (id >> 8) as u8, id as u8]
            }
            _ => Vec::new(),
        }
    }

    fn rebase_allocator(
        &self,
        db_id: i64,
        table: &tidb_model::TableInfo,
        tp: AllocatorType,
        base: i64,
    ) -> Result<(), String> {
        let auto_ids = &self.domain.auto_ids;
        let allocator = match tp {
            AllocatorType::RowId => auto_ids.row_allocator_for(db_id, table),
            AllocatorType::AutoIncrement => auto_ids.allocator_for(db_id, table),
            AllocatorType::AutoRandom => auto_ids.random_allocator_for(db_id, table),
        };
        // Go `Rebase(ctx, base, false)` takes the int64 base as is.
        allocator.0.rebase(base as u64).map_err(|e| e.to_string())
    }

    fn etcd(&self) -> Option<Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>> {
        self.domain
            .etcd_client()
            .map(|client| Arc::new(tidb_ddl::etcd_ops::EtcdClientOps::new(client)) as Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>)
    }

    fn local_server_info(&self) -> Option<serde_json::Value> {
        let info = self.domain.server_info()?.get_local_server_info();
        serde_json::to_value(info).ok()
    }

    fn cluster_id(&self) -> Option<u64> {
        Some(self.store.cluster_id())
    }

    fn table_is_empty(&self, table: &tidb_model::TableInfo) -> Result<bool, String> {
        let mut ids = vec![table.id];
        if let Some(partition) = &table.partition {
            ids = partition.read().definitions.snapshot().iter().map(|d| d.id).collect();
        }
        let storage = self.domain.transactions.kv_storage();
        let mut snapshot = storage.begin_read_only().map_err(|e| e.to_string())?;
        let call = tidb_txnkv::UnaryCallContext::with_timeout(std::time::Duration::from_secs(60));
        for id in ids {
            let start = tidb_tablecodec::table_key::gen_table_record_prefix(id);
            let end = tidb_txnkv::Key::from(start.clone()).prefix_next().into_bytes();
            if !snapshot.scan(&start, &end, Some(1), &call).map_err(|e| e.to_string())?.is_empty() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Go `importer.GetRegionSplitSizeKeys` (`ingestctrl.GetRegionSplitSizeKeys`):
    /// the `coprocessor` config of the first non-TiFlash, non-tombstone
    /// store with a status address that answers `GET /config`.
    fn region_split_size_keys(&self) -> Result<(i64, i64), String> {
        let pd = self.store.store().pd_client();
        let stores = self.store.block_on(async move { pd.get_all_stores().await }).map_err(|e| e.to_string())?;
        let security = &tidb_config::config_tree::config::get_global_config().security;
        let tls = !security.cluster_ssl_ca.is_empty();
        for store in stores {
            let tiflash = store.labels.iter().any(|l| l.key == "engine" && (l.value == "tiflash" || l.value == "tiflash_compute"));
            if store.status_address.is_empty()
                || tiflash
                || store.state == tidb_proto::metapb::StoreState::Tombstone as i32
            {
                continue;
            }
            match get_split_conf_from_store(&store.status_address, tls) {
                Ok(conf) => return Ok(conf),
                Err(error) => tidb_logutil::bg_logger().warn(
                    "get region split size and keys failed",
                    &[
                        tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                        tidb_log::Field::new("store", tidb_log::Value::Str(store.status_address.clone())),
                    ],
                ),
            }
        }
        Err("get region split size and keys failed".to_owned())
    }

    fn kv_storage(&self) -> Option<Arc<dyn tidb_txnkv::kv_storage::Storage>> {
        Some(self.domain.transactions.kv_storage())
    }

    fn max_node_count_by_stores(&self) -> usize {
        let pd = self.store.store().pd_client();
        match self.store.block_on(async move { pd.get_all_stores().await }) {
            Ok(stores) => (stores.len() / 3).max(3),
            Err(error) => {
                tidb_logutil::bg_logger().warn(
                    "failed to get all stores for calculating max node count",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                );
                0
            }
        }
    }
}

/// Go `getSplitConfFromStore`: `coprocessor.region-split-size` (a go-units
/// decimal size) and `region-split-keys` from the store's `/config`.
fn get_split_conf_from_store(host: &str, tls: bool) -> Result<(i64, i64), String> {
    let scheme = if tls { "https" } else { "http" };
    let body: serde_json::Value = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?
        .get(format!("{scheme}://{host}/config"))
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|e| e.to_string())?
        .json()
        .map_err(|e| e.to_string())?;
    let coprocessor = &body["coprocessor"];
    let size = tidb_config_configtypes::from_human_size(coprocessor["region-split-size"].as_str().unwrap_or_default())?;
    Ok((size, coprocessor["region-split-keys"].as_i64().unwrap_or_default()))
}
