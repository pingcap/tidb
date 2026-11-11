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

//! Go `infosync/tiflash_manager.go` and the `infosync` entry points over it:
//! the TiFlash placement rules DDL jobs set (`ConfigureTiFlashPDForTable`,
//! `ConfigureTiFlashPDForPartitions`), the GC worker deletes
//! (`DeleteTiFlashPlacementRules`) and the replica poller's group config
//! (`SetTiFlashGroupConfig`).

use std::sync::{Arc, Mutex, PoisonError};

use tidb_log::{Field, Value};
use tikv_client::pd_http;

/// Go `TiFlashReplicaManager`, the operations this node calls.
pub trait TiFlashReplicaManager: Send + Sync {
    /// Go `SetTiFlashGroupConfig`.
    fn set_tiflash_group_config(&self) -> Result<(), String>;
    /// Go `SetPlacementRule`.
    fn set_placement_rule(&self, rule: &pd_http::Rule) -> Result<(), String>;
    /// Go `SetPlacementRuleBatch`.
    fn set_placement_rule_batch(&self, rules: &[pd_http::Rule]) -> Result<(), String>;
    /// Go `GetPlacementRule`: the table's TiFlash rule; PD's zero rule (an
    /// empty id) when it has none.
    fn get_placement_rule(&self, table_id: i64) -> Result<pd_http::Rule, String>;
    /// Go `PostAccelerateScheduleBatch`.
    fn post_accelerate_schedule_batch(&self, table_ids: &[i64]) -> Result<(), String>;
    /// Go `UpdateTiFlashProgressCache` updates tiflashProgressCache.
    fn update_tiflash_progress_cache(&self, table_id: i64, progress: f64);
    /// Go `GetTiFlashProgressFromCache` gets tiflash replica progress from
    /// tiflashProgressCache.
    fn get_tiflash_progress_from_cache(&self, table_id: i64) -> Option<f64>;
    /// Go `DeleteTiFlashProgressFromCache` delete tiflash replica progress
    /// from tiflashProgressCache.
    fn delete_tiflash_progress_from_cache(&self, table_id: i64);
    /// Go `CleanTiFlashProgressCache` clean progress cache.
    fn clean_tiflash_progress_cache(&self);
    /// Go `GetStoresStat` gets the TiKV store information by accessing PD's
    /// api.
    fn get_stores_stat(&self) -> Result<pd_http::StoresInfo, String>;
    /// Go `SyncTiFlashTableSchema` syncs the table's schema to TiFlash.
    fn sync_tiflash_table_schema(&self, table_id: i64, stores: &[pd_http::StoreInfo]) -> Result<(), String>;
    /// Go `SetMockTiFlash`'s type assertion: only the mock manager takes a
    /// mock TiFlash.
    fn set_mock_tiflash(&self, _tiflash: Arc<MockTiFlash>) {}
    /// The cluster security Go's `util.InternalHTTPClient` is built from.
    fn security(&self) -> tidb_pd_client::ClusterSecurity {
        tidb_pd_client::ClusterSecurity::default()
    }
}

/// Go `tiflashProgressCache` with the four methods both of Go's managers
/// implement the same way over it.
#[derive(Default)]
pub struct TiFlashProgressCache(std::sync::RwLock<std::collections::HashMap<i64, f64>>);

impl TiFlashProgressCache {
    /// Go `UpdateTiFlashProgressCache`.
    pub fn update(&self, table_id: i64, progress: f64) {
        self.0.write().unwrap_or_else(PoisonError::into_inner).insert(table_id, progress);
    }

    /// Go `GetTiFlashProgressFromCache`.
    pub fn get(&self, table_id: i64) -> Option<f64> {
        self.0.read().unwrap_or_else(PoisonError::into_inner).get(&table_id).copied()
    }

    /// Go `DeleteTiFlashProgressFromCache`.
    pub fn delete(&self, table_id: i64) {
        self.0.write().unwrap_or_else(PoisonError::into_inner).remove(&table_id);
    }

    /// Go `CleanTiFlashProgressCache`.
    pub fn clean(&self) {
        self.0.write().unwrap_or_else(PoisonError::into_inner).clear();
    }
}

/// Go `TiFlashReplicaManagerCtx` over the info syncer's PD HTTP client.
pub struct TiFlashReplicaManagerCtx {
    pd_http_client: pd_http::Client,
    security: tidb_pd_client::ClusterSecurity,
    tiflash_progress_cache: TiFlashProgressCache,
}

impl TiFlashReplicaManagerCtx {
    /// Over the store's PD HTTP client, as the info syncer derives it, and
    /// the cluster security Go's internal HTTP client is built from.
    pub fn new(pd_http_client: &pd_http::Client, security: &tidb_pd_client::ClusterSecurity) -> Self {
        Self {
            pd_http_client: crate::pd_http::info_syncer_pd_http_client(pd_http_client),
            security: security.clone(),
            tiflash_progress_cache: TiFlashProgressCache::default(),
        }
    }

    /// Go `helper.SyncTableSchemaToTiFlash`: only a failed request is an
    /// error; the response is dropped. This node serves the null keyspace
    /// (Go `m.codec.GetKeyspaceID()`).
    fn sync_table_schema_to_tiflash(&self, status_address: &str, table_id: i64) -> Result<(), String> {
        let schema = if self.security.is_tls_enabled() { "https" } else { "http" };
        let stat_url = format!(
            "{schema}://{status_address}/tiflash/sync-schema/keyspace/{}/table/{table_id}",
            tikv_client::tikv::NULLSPACE_ID,
        );
        let client = tidb_store_driver_txn::cluster_http::internal_http_client(&self.security)?;
        client.get(stat_url).send().map(drop).map_err(|error| error.to_string())
    }
}

fn block_on<T: Send>(request: impl std::future::Future<Output = tikv_client::Result<T>> + Send) -> Result<T, String> {
    tidb_store_driver_txn::cluster_http::block_on_pd_http(request)
}

impl TiFlashReplicaManager for TiFlashReplicaManagerCtx {
    fn set_tiflash_group_config(&self) -> Result<(), String> {
        let group_config = block_on(
            self.pd_http_client
                .get_placement_rule_group_by_id(tidb_placement::TIFLASH_RULE_GROUP_ID),
        )?;
        if group_config.index == tidb_placement::RULE_INDEX_TIFLASH && !group_config.r#override {
            return Ok(());
        }
        block_on(self.pd_http_client.set_placement_rule_group(&pd_http::RuleGroup {
            id: tidb_placement::TIFLASH_RULE_GROUP_ID.to_owned(),
            index: tidb_placement::RULE_INDEX_TIFLASH,
            r#override: false,
        }))
    }

    /// Go `SetPlacementRule` -> `doSetPlacementRule`: the group config, then
    /// the rule; a zero-count rule is deleted.
    fn set_placement_rule(&self, rule: &pd_http::Rule) -> Result<(), String> {
        self.set_tiflash_group_config()?;
        if rule.count == 0 {
            return block_on(self.pd_http_client.delete_placement_rule(&rule.group_id, &rule.id));
        }
        block_on(self.pd_http_client.set_placement_rule(rule))
    }

    /// Go `doSetPlacementRuleBatch`: the group config, then one batch where
    /// a zero-count rule is a delete.
    fn set_placement_rule_batch(&self, rules: &[pd_http::Rule]) -> Result<(), String> {
        self.set_tiflash_group_config()?;
        let rule_ops: Vec<_> = rules
            .iter()
            .map(|rule| pd_http::RuleOp {
                rule: rule.clone(),
                action: if rule.count == 0 {
                    pd_http::RULE_OP_DEL
                } else {
                    pd_http::RULE_OP_ADD
                },
                delete_by_id_prefix: false,
            })
            .collect();
        block_on(self.pd_http_client.set_placement_rule_in_batch(&rule_ops))
    }

    fn get_placement_rule(&self, table_id: i64) -> Result<pd_http::Rule, String> {
        block_on(
            self.pd_http_client
                .get_placement_rule(tidb_placement::TIFLASH_RULE_GROUP_ID, &make_rule_id(table_id)),
        )
    }

    fn post_accelerate_schedule_batch(&self, table_ids: &[i64]) -> Result<(), String> {
        if table_ids.is_empty() {
            return Ok(());
        }
        let key_ranges: Vec<_> = table_ids
            .iter()
            .map(|&table_id| {
                let (start, end) = table_region_range(table_id);
                pd_http::new_key_range(start, end)
            })
            .collect();
        block_on(self.pd_http_client.accelerate_schedule_in_batch(&key_ranges))
    }

    fn update_tiflash_progress_cache(&self, table_id: i64, progress: f64) {
        self.tiflash_progress_cache.update(table_id, progress);
    }

    fn get_tiflash_progress_from_cache(&self, table_id: i64) -> Option<f64> {
        self.tiflash_progress_cache.get(table_id)
    }

    fn delete_tiflash_progress_from_cache(&self, table_id: i64) {
        self.tiflash_progress_cache.delete(table_id);
    }

    fn clean_tiflash_progress_cache(&self) {
        self.tiflash_progress_cache.clean();
    }

    fn get_stores_stat(&self) -> Result<pd_http::StoresInfo, String> {
        block_on(self.pd_http_client.get_stores())
    }

    fn sync_tiflash_table_schema(&self, table_id: i64, stores: &[pd_http::StoreInfo]) -> Result<(), String> {
        for store in stores {
            self.sync_table_schema_to_tiflash(&store.store.status_address, table_id)?;
        }
        Ok(())
    }

    fn security(&self) -> tidb_pd_client::ClusterSecurity {
        self.security.clone()
    }
}

/// Go `MockTiFlash`, the part DDL jobs read: the one TiFlash store its
/// `HandleGetStoresStat` reports.
pub struct MockTiFlash {
    status_addr: String,
    start_time: i64,
}

impl MockTiFlash {
    /// Go `NewMockTiFlash`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            status_addr: String::new(),
            start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)),
        }
    }

    /// Go `HandleGetStoresStat`.
    #[must_use]
    pub fn handle_get_stores_stat(&self) -> pd_http::StoresInfo {
        pd_http::StoresInfo {
            count: 1,
            stores: vec![pd_http::StoreInfo {
                store: pd_http::MetaStore {
                    id: 1,
                    address: "127.0.0.1:3930".to_owned(),
                    state: 0,
                    state_name: "Up".to_owned(),
                    version: "4.0.0-alpha".to_owned(),
                    status_address: self.status_addr.clone(),
                    git_hash: "mock-tikv-githash".to_owned(),
                    start_timestamp: self.start_time,
                    labels: vec![pd_http::StoreLabel {
                        key: "engine".to_owned(),
                        value: "tiflash".to_owned(),
                    }],
                },
                ..Default::default()
            }],
        }
    }
}

impl Default for MockTiFlash {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `mockTiFlashReplicaManagerCtx`: every write is a no-op, and the stores
/// are its mock TiFlash's. Its record of the calls is what tests read.
#[derive(Default)]
pub struct MockTiFlashReplicaManagerCtx {
    calls: Mutex<Vec<String>>,
    tiflash: Mutex<Option<Arc<MockTiFlash>>>,
    tiflash_progress_cache: TiFlashProgressCache,
}

impl MockTiFlashReplicaManagerCtx {
    /// The calls made so far, as `set <rule id> <count>`, `accel <ids>`.
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn record(&self, call: String) {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner).push(call);
    }
}

impl TiFlashReplicaManager for MockTiFlashReplicaManagerCtx {
    fn set_tiflash_group_config(&self) -> Result<(), String> {
        Ok(())
    }

    fn set_placement_rule(&self, rule: &pd_http::Rule) -> Result<(), String> {
        self.record(format!("set {} {}", rule.id, rule.count));
        Ok(())
    }

    fn set_placement_rule_batch(&self, rules: &[pd_http::Rule]) -> Result<(), String> {
        for rule in rules {
            self.record(format!("set {} {}", rule.id, rule.count));
        }
        Ok(())
    }

    fn post_accelerate_schedule_batch(&self, table_ids: &[i64]) -> Result<(), String> {
        self.record(format!("accel {table_ids:?}"));
        Ok(())
    }

    /// Go's mock: `errors.New("not implemented")`.
    fn get_placement_rule(&self, _table_id: i64) -> Result<pd_http::Rule, String> {
        Err("not implemented".to_owned())
    }

    fn update_tiflash_progress_cache(&self, table_id: i64, progress: f64) {
        self.tiflash_progress_cache.update(table_id, progress);
    }

    fn get_tiflash_progress_from_cache(&self, table_id: i64) -> Option<f64> {
        self.tiflash_progress_cache.get(table_id)
    }

    fn delete_tiflash_progress_from_cache(&self, table_id: i64) {
        self.tiflash_progress_cache.delete(table_id);
    }

    fn clean_tiflash_progress_cache(&self) {
        self.tiflash_progress_cache.clean();
    }

    /// Go's mock: `&MockTiFlashError{"MockTiFlash is not accessible"}` with
    /// no mock TiFlash.
    fn get_stores_stat(&self) -> Result<pd_http::StoresInfo, String> {
        self.tiflash
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|tiflash| tiflash.handle_get_stores_stat())
            .ok_or_else(|| "MockTiFlash is not accessible".to_owned())
    }

    fn sync_tiflash_table_schema(&self, _table_id: i64, _stores: &[pd_http::StoreInfo]) -> Result<(), String> {
        Ok(())
    }

    fn set_mock_tiflash(&self, tiflash: Arc<MockTiFlash>) {
        *self.tiflash.lock().unwrap_or_else(PoisonError::into_inner) = Some(tiflash);
    }
}

/// The info syncer's `tiflashReplicaManager`.
static TIFLASH_REPLICA_MANAGER: Mutex<Option<Arc<dyn TiFlashReplicaManager>>> = Mutex::new(None);

/// Go `InfoSyncer.initTiFlashReplicaManager`: PD's manager, or a mock when
/// the node has no PD.
pub fn init_tiflash_replica_manager(manager: Option<Arc<dyn TiFlashReplicaManager>>) {
    *TIFLASH_REPLICA_MANAGER
        .lock()
        .unwrap_or_else(PoisonError::into_inner) =
        Some(manager.unwrap_or_else(|| Arc::new(MockTiFlashReplicaManagerCtx::default())));
}

/// The node's TiFlash replica manager; a node that installed none gets a mock.
pub fn tiflash_replica_manager() -> Arc<dyn TiFlashReplicaManager> {
    Arc::clone(
        TIFLASH_REPLICA_MANAGER
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| Arc::new(MockTiFlashReplicaManagerCtx::default())),
    )
}

fn tiflash_enabled() -> bool {
    tidb_config::config_tree::config::get_global_config().cse.is_tiflash_enabled()
}

fn info(message: &str, fields: &[Field]) {
    tidb_logutil::bg_logger().info(message, fields);
}

/// Go `infosync.SetMockTiFlash`: the mock manager's TiFlash.
pub fn set_mock_tiflash(tiflash: Arc<MockTiFlash>) {
    tiflash_replica_manager().set_mock_tiflash(tiflash);
}

/// Go `infosync.GetTiFlashStoresStat`.
pub fn get_tiflash_stores_stat() -> Result<pd_http::StoresInfo, String> {
    tiflash_replica_manager().get_stores_stat()
}

/// Go `infosync.SyncTiFlashTableSchema`: the table's schema synced to every
/// store labelled `engine=tiflash`.
pub fn sync_tiflash_table_schema(table_id: i64) -> Result<(), String> {
    let manager = tiflash_replica_manager();
    let tikv_stats = manager.get_stores_stat()?;
    let tiflash_stores: Vec<pd_http::StoreInfo> = tikv_stats
        .stores
        .into_iter()
        .filter(|store| {
            store
                .store
                .labels
                .iter()
                .any(|label| label.key == "engine" && label.value == "tiflash")
        })
        .collect();
    manager.sync_tiflash_table_schema(table_id, &tiflash_stores)
}

/// Go `helper.ColumnarStatusResp`: the response from the TiKV's status API.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ColumnarStatusResp {
    /// `ready`.
    pub ready: u64,
    /// `vector-index-ready`.
    pub vector_index_ready: u64,
    /// `total`.
    pub total: u64,
}

/// Go `helper.CollectColumnarStatus`: collects the columnar status from the
/// TiKV status API. This node serves the null keyspace (Go
/// `is.tikvCodec.GetKeyspaceID()`).
pub fn collect_columnar_status(
    security: &tidb_pd_client::ClusterSecurity,
    status_address: &str,
    table_id: i64,
    index_id: Option<i64>,
) -> Result<ColumnarStatusResp, String> {
    let schema = if security.is_tls_enabled() { "https" } else { "http" };
    let mut stat_url = format!(
        "{schema}://{status_address}/kvengine/columnar_status?keyspace_id={}&table_id={table_id}",
        tikv_client::tikv::NULLSPACE_ID,
    );
    if let Some(index_id) = index_id {
        stat_url.push_str(&format!("&index_id={index_id}"));
    }
    let client = tidb_store_driver_txn::cluster_http::internal_http_client(security)?;
    let resp = client.get(stat_url).send().map_err(|error| error.to_string())?;
    let status = resp.status().as_u16();
    let body = resp.text().map_err(|error| error.to_string())?;
    if status != 200 {
        return Err(format!("TiKV columnar status API returned status {status}: {body}"));
    }
    let value: serde_json::Value = serde_json::from_str(&body).map_err(|error| error.to_string())?;
    let field = |name: &str| match value.get(name) {
        None | Some(serde_json::Value::Null) => Ok(0),
        Some(count) => count
            .as_u64()
            .ok_or_else(|| format!("json: cannot unmarshal number {count} into Go struct field ColumnarStatusResp.{name} of type uint")),
    };
    let columnar_status = ColumnarStatusResp {
        ready: field("ready")?,
        vector_index_ready: field("vector-index-ready")?,
        total: field("total")?,
    };
    if columnar_status.ready != columnar_status.total {
        info(
            "columnar status not ready",
            &[
                Field::new("ready", Value::U64(columnar_status.ready)),
                Field::new("total", Value::U64(columnar_status.total)),
            ],
        );
    }
    Ok(columnar_status)
}

/// The loop Go's `calculateColumnarProgressWithCtx` and
/// `CalculateColumnarIndexProgress` share: the `ready_of` count of every
/// TiKV store over their total, a failed store an error unless it is a
/// tombstone.
fn columnar_progress(
    table_id: i64,
    index_id: Option<i64>,
    tikv_stores: &[pd_http::StoreInfo],
    ready_of: fn(&ColumnarStatusResp) -> u64,
) -> Result<f64, String> {
    let security = tiflash_replica_manager().security();
    let mut ready = 0;
    let mut total = 0;
    for store_stat in tikv_stores {
        let addr = &store_stat.store.status_address;
        let Ok(columnar_status) = collect_columnar_status(&security, addr, table_id, index_id) else {
            tidb_logutil::bg_logger().error(
                "Fail to get columnar status from TiKV.",
                &[
                    Field::new("tableID", Value::I64(table_id)),
                    Field::new("addr", Value::Str(addr.clone())),
                ],
            );
            // If the store is not tombstone, return error
            if store_stat.store.state_name != "Tombstone" {
                return Err(format!(
                    "Failed to get columnar status from TiKV, store {addr} is {}",
                    store_stat.store.state_name
                ));
            }
            continue;
        };
        ready += ready_of(&columnar_status);
        total += columnar_status.total;
    }
    if total == 0 {
        return Ok(0.0);
    }
    Ok(ready as f64 / total as f64)
}

/// Go `infosync.CalculateColumnarProgress` calculates columnar progress.
pub fn calculate_columnar_progress(table_id: i64, tikv_stores: &[pd_http::StoreInfo]) -> Result<f64, String> {
    columnar_progress(table_id, None, tikv_stores, |status| status.ready)
}

/// Go `infosync.CalculateColumnarIndexProgress` calculates columnar index
/// progress.
pub fn calculate_columnar_index_progress(
    table_id: i64,
    index_id: i64,
    tikv_stores: &[pd_http::StoreInfo],
) -> Result<f64, String> {
    columnar_progress(table_id, Some(index_id), tikv_stores, |status| status.vector_index_ready)
}

/// Go `engine.IsTiFlashHTTPResp` tests whether the store is a TiFlash node
/// from a PD HTTP response.
pub fn is_tiflash_http_resp(store: &pd_http::MetaStore) -> bool {
    store
        .labels
        .iter()
        .any(|label| label.key == "engine" && (label.value == "tiflash_compute" || label.value == "tiflash"))
}

/// Go `infosync.UpdateTiFlashProgressCache`.
pub fn update_tiflash_progress_cache(table_id: i64, progress: f64) {
    tiflash_replica_manager().update_tiflash_progress_cache(table_id, progress);
}

/// Go `infosync.GetTiFlashProgressFromCache`.
pub fn get_tiflash_progress_from_cache(table_id: i64) -> Option<f64> {
    tiflash_replica_manager().get_tiflash_progress_from_cache(table_id)
}

/// Go `infosync.DeleteTiFlashProgressFromCache`.
pub fn delete_tiflash_progress_from_cache(table_id: i64) {
    tiflash_replica_manager().delete_tiflash_progress_from_cache(table_id);
}

/// Go `infosync.CleanTiFlashProgressCache`.
pub fn clean_tiflash_progress_cache() {
    tiflash_replica_manager().clean_tiflash_progress_cache();
}

/// Go `infosync.DeleteTiFlashTableSyncProgress`: drops the cached progress
/// of the table, or of each of its partitions.
pub fn delete_tiflash_table_sync_progress(table_info: &tidb_model::table_info::TableInfo) {
    let manager = tiflash_replica_manager();
    if let Some(pi) = table_info.get_partition_info() {
        for p in pi.read().definitions.snapshot() {
            manager.delete_tiflash_progress_from_cache(p.id);
        }
    } else {
        manager.delete_tiflash_progress_from_cache(table_info.id);
    }
}

/// Go `infosync.SetTiFlashGroupConfig`.
pub fn set_tiflash_group_config() -> Result<(), String> {
    info("SetTiFlashGroupConfig", &[]);
    tiflash_replica_manager().set_tiflash_group_config()
}

/// Go `infosync.ConfigureTiFlashPDForTable`.
pub fn configure_tiflash_pd_for_table(id: i64, count: u64, location_labels: &[String]) -> Result<(), String> {
    if !tiflash_enabled() {
        return Ok(());
    }
    info(
        "ConfigureTiFlashPDForTable",
        &[Field::new("tableID", Value::I64(id)), Field::new("count", Value::U64(count))],
    );
    tiflash_replica_manager().set_placement_rule(&make_new_rule(id, count, location_labels))
}

/// Go `infosync.ConfigureTiFlashPDForPartitions`: one rule per partition in
/// one batch, then, when `accel`, the accelerated scheduling of them.
pub fn configure_tiflash_pd_for_partitions(
    accel: bool,
    partition_ids: &[i64],
    count: u64,
    location_labels: &[String],
    table_id: i64,
) -> Result<(), String> {
    if !tiflash_enabled() {
        return Ok(());
    }
    let rules: Vec<_> = partition_ids
        .iter()
        .map(|&id| {
            info(
                "ConfigureTiFlashPDForPartitions",
                &[
                    Field::new("tableID", Value::I64(table_id)),
                    Field::new("partID", Value::I64(id)),
                    Field::new("accel", Value::Bool(accel)),
                    Field::new("count", Value::U64(count)),
                ],
            );
            make_new_rule(id, count, location_labels)
        })
        .collect();
    let manager = tiflash_replica_manager();
    manager.set_placement_rule_batch(&rules)?;
    if accel {
        manager.post_accelerate_schedule_batch(partition_ids)?;
    }
    Ok(())
}

/// Go `infosync.DeleteTiFlashPlacementRules`: a zero-count rule per table,
/// which the batch deletes.
pub fn delete_tiflash_placement_rules(physical_table_ids: &[i64]) -> Result<(), String> {
    info("DeleteTiFlashPlacementRules", &[]);
    let rules: Vec<_> = physical_table_ids
        .iter()
        .map(|&id| make_new_rule(id, 0, &[]))
        .collect();
    tiflash_replica_manager().set_placement_rule_batch(&rules)
}

/// Go `MakeRuleID`: the rule ID for TiFlash with the given table ID.
pub fn make_rule_id(id: i64) -> String {
    format!("table-{id}-r")
}

/// Go `MakeNewRule` (over `makeBaseRule`): the TiFlash learner rule of one
/// physical table. The raw keys are the record range; the classic (API V1)
/// codec's `EncodeRange` leaves them as they are, and the rule's JSON carries
/// them memcomparable-encoded in hex.
pub fn make_new_rule(id: i64, count: u64, location_labels: &[String]) -> pd_http::Rule {
    pd_http::Rule {
        group_id: tidb_placement::TIFLASH_RULE_GROUP_ID.to_owned(),
        id: make_rule_id(id),
        index: tidb_placement::RULE_INDEX_TIFLASH,
        r#override: false,
        start_key: tidb_tablecodec::table_key::gen_table_record_prefix(id),
        end_key: tidb_tablecodec::table_key::encode_table_prefix(id + 1),
        role: pd_http::LEARNER,
        count: count as i64,
        label_constraints: vec![pd_http::LabelConstraint {
            key: tidb_placement::ENGINE_LABEL_KEY.to_owned(),
            op: pd_http::IN,
            values: vec![tidb_placement::ENGINE_LABEL_TIFLASH.to_owned()],
        }],
        location_labels: location_labels.to_vec(),
        ..pd_http::Rule::default()
    }
}

/// The record range of `table_id` as PD's region APIs take it:
/// `EncodeRegionRange` over `GenTableRecordPrefix` .. `EncodeTablePrefix(id+1)`.
pub fn table_region_range(table_id: i64) -> (Vec<u8>, Vec<u8>) {
    let mut start = Vec::new();
    let mut end = Vec::new();
    tidb_codec::encode_bytes(&mut start, &tidb_tablecodec::table_key::gen_table_record_prefix(table_id));
    tidb_codec::encode_bytes(&mut end, &tidb_tablecodec::table_key::encode_table_prefix(table_id + 1));
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tiflash_rule_delete_is_go_make_new_rule_with_a_zero_count() {
        let rule = serde_json::to_value(make_new_rule(100, 0, &[])).unwrap();
        assert_eq!("tiflash", rule["group_id"]);
        assert_eq!("table-100-r", rule["id"]);
        assert_eq!(120, rule["index"]);
        assert_eq!(0, rule["count"]);
        // `t\x80\0\0\0\0\0\0\x64_r` and `t\x80\0\0\0\0\0\0\x65`, memcomparable.
        assert_eq!("7480000000000000ff645f720000000000fa", rule["start_key"]);
        assert_eq!("7480000000000000ff6500000000000000f8", rule["end_key"]);
    }
}
