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
    /// Go `GetRegionCountFromPD`: PD's `/stats/region` count over the
    /// table's record range.
    fn get_region_count_from_pd(&self, table_id: i64) -> Result<i64, String>;
    /// Go `CalculateTiFlashProgress`: `(full replica progress, one replica
    /// progress)`. This node serves the null keyspace.
    fn calculate_tiflash_progress(
        &self,
        table_id: i64,
        replica_count: u64,
        tiflash_stores: &[pd_http::StoreInfo],
    ) -> Result<(f64, f64), String> {
        let security = self.security();
        calculate_tiflash_progress(
            self.get_region_count_from_pd(table_id),
            table_id,
            replica_count,
            tiflash_stores,
            |addr| collect_tiflash_status(&security, addr, table_id),
        )
    }
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

    fn get_region_count_from_pd(&self, table_id: i64) -> Result<i64, String> {
        let (start, end) = table_region_range(table_id);
        block_on(
            self.pd_http_client
                .get_region_status_by_key_range(&pd_http::new_key_range(start, end), true),
        )
        .map(|stats| stats.count)
    }

    fn security(&self) -> tidb_pd_client::ClusterSecurity {
        self.security.clone()
    }
}

/// Go `MockTiFlash`, the part DDL jobs read: the one TiFlash store its
/// `HandleGetStoresStat` reports, and the placement rules its manager sets
/// (`GlobalTiFlashPlacementRules`).
pub struct MockTiFlash {
    status_addr: String,
    start_time: i64,
    global_tiflash_placement_rules: Mutex<std::collections::HashMap<String, pd_http::Rule>>,
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
            global_tiflash_placement_rules: Mutex::default(),
        }
    }

    /// Go `HandleSetPlacementRule`: a rule of count zero is deleted. (Go's
    /// mock also marks the table's regions synced, which nothing here reads.)
    ///
    /// # Errors
    ///
    /// Go's `Can't parse rule` for an ID that is not `table-<id>-r`.
    pub fn handle_set_placement_rule(&self, rule: &pd_http::Rule) -> Result<(), String> {
        let mut rules = self.global_tiflash_placement_rules.lock().unwrap_or_else(PoisonError::into_inner);
        if rule.count == 0 {
            rules.remove(&rule.id);
        } else {
            rules.insert(rule.id.clone(), rule.clone());
        }
        let parsed = rule
            .id
            .strip_prefix("table-")
            .and_then(|rest| rest.strip_suffix("-r"))
            .is_some_and(|id| id.parse::<i64>().is_ok());
        if !parsed {
            return Err("Can't parse rule".to_owned());
        }
        Ok(())
    }

    /// Go `HandleDeletePlacementRule`.
    pub fn handle_delete_placement_rule(&self, rule_id: &str) {
        self.global_tiflash_placement_rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(rule_id);
    }

    /// Go `MockTiFlash.GetPlacementRule`.
    #[must_use]
    pub fn get_placement_rule(&self, rule_id: &str) -> Option<pd_http::Rule> {
        self.global_tiflash_placement_rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(rule_id)
            .cloned()
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

    /// Go's mock: its mock TiFlash's `HandleSetPlacementRule`, nothing
    /// without one.
    fn set_placement_rule(&self, rule: &pd_http::Rule) -> Result<(), String> {
        self.record(format!("set {} {}", rule.id, rule.count));
        match self.tiflash.lock().unwrap_or_else(PoisonError::into_inner).as_ref() {
            Some(tiflash) => tiflash.handle_set_placement_rule(rule),
            None => Ok(()),
        }
    }

    /// Go's mock: `HandleSetPlacementRuleBatch`.
    fn set_placement_rule_batch(&self, rules: &[pd_http::Rule]) -> Result<(), String> {
        for rule in rules {
            self.record(format!("set {} {}", rule.id, rule.count));
        }
        if let Some(tiflash) = self.tiflash.lock().unwrap_or_else(PoisonError::into_inner).as_ref() {
            for rule in rules {
                tiflash.handle_set_placement_rule(rule)?;
            }
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

    /// Go's mock: one region (`HandleGetPDRegionRecordStats`), none without
    /// a mock TiFlash.
    fn get_region_count_from_pd(&self, _table_id: i64) -> Result<i64, String> {
        Ok(i64::from(
            self.tiflash
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_some(),
        ))
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
    /// `fts-index-ready`.
    pub fts_index_ready: u64,
    /// Whether the payload holds `fts-index-ready` (an older TiKV does not).
    pub has_fts_index_ready: bool,
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
    let has_fts_index_ready = !matches!(value.get("fts-index-ready"), None | Some(serde_json::Value::Null));
    let columnar_status = ColumnarStatusResp {
        ready: field("ready")?,
        vector_index_ready: field("vector-index-ready")?,
        fts_index_ready: field("fts-index-ready")?,
        has_fts_index_ready,
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
    ready_of: &dyn Fn(&ColumnarStatusResp, &pd_http::StoreInfo) -> Result<u64, String>,
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
        ready += ready_of(&columnar_status, store_stat)?;
        total += columnar_status.total;
    }
    if total == 0 {
        return Ok(0.0);
    }
    Ok(ready as f64 / total as f64)
}

/// Go `infosync.CalculateColumnarProgress` calculates columnar progress.
pub fn calculate_columnar_progress(table_id: i64, tikv_stores: &[pd_http::StoreInfo]) -> Result<f64, String> {
    columnar_progress(table_id, None, tikv_stores, &|status, _| Ok(status.ready))
}

/// Go `infosync.CalculateColumnarIndexProgress` calculates columnar index
/// progress.
pub fn calculate_columnar_index_progress(
    table_id: i64,
    index_id: i64,
    columnar_index_type: tidb_model::ColumnarIndexType,
    tikv_stores: &[pd_http::StoreInfo],
) -> Result<f64, String> {
    columnar_progress(table_id, Some(index_id), tikv_stores, &|status, store_stat| {
        if columnar_index_type == tidb_model::ColumnarIndexType::FULLTEXT {
            if !status.has_fts_index_ready {
                return Err(format!(
                    "fts-index-ready not found in TiKV columnar_status response from {} (store {}); please check TiKV version",
                    store_stat.store.status_address, store_stat.store.id
                ));
            }
            return Ok(status.fts_index_ready);
        }
        Ok(status.vector_index_ready)
    })
}

/// Go `engine.IsTiFlashHTTPResp` tests whether the store is a TiFlash node
/// from a PD HTTP response.
pub fn is_tiflash_http_resp(store: &pd_http::MetaStore) -> bool {
    store
        .labels
        .iter()
        .any(|label| label.key == "engine" && (label.value == "tiflash_compute" || label.value == "tiflash"))
}

/// Go `helper.ComputeTiFlashStatus`: a count line followed by
/// space-separated region IDs.
///
/// # Errors
/// A missing line or an unparsable number.
pub fn compute_tiflash_status(body: &str) -> Result<Vec<i64>, String> {
    let mut lines = body.split_inclusive('\n');
    let mut next_line = || {
        lines
            .next()
            .filter(|line| line.ends_with('\n'))
            .map(|line| line.trim_matches(['\r', '\n', '\t']))
            .ok_or_else(|| "incomplete TiFlash sync-status response".to_owned())
    };
    let expected: i64 = next_line()?
        .parse()
        .map_err(|error| format!("TiFlash region count: {error}"))?;
    let regions: Vec<i64> = next_line()?
        .split(' ')
        .filter(|id| !id.is_empty())
        .map(|id| {
            id.parse()
                .map_err(|error| format!("TiFlash region ID: {error}"))
        })
        .collect::<Result<_, _>>()?;
    // Go warns on count disagreement but accepts the successfully parsed IDs.
    if expected != regions.len() as i64 {
        tidb_logutil::bg_logger().warn(
            "ComputeTiFlashStatus count check failed",
            &[
                Field::new("claim", Value::I64(expected)),
                Field::new("real", Value::I64(regions.len() as i64)),
            ],
        );
    }
    Ok(regions)
}

/// Go `helper.CollectTiFlashStatusWithCtx`: the regions of the table one
/// TiFlash store holds, from its `sync-status` API in the null keyspace.
/// The body is parsed whatever the HTTP status.
///
/// # Errors
/// The request or parse error.
pub fn collect_tiflash_status(
    security: &tidb_pd_client::ClusterSecurity,
    status_address: &str,
    table_id: i64,
) -> Result<Vec<i64>, String> {
    let schema = if security.is_tls_enabled() {
        "https"
    } else {
        "http"
    };
    let stat_url = format!(
        "{schema}://{status_address}/tiflash/sync-status/keyspace/{}/table/{table_id}",
        tikv_client::tikv::NULLSPACE_ID,
    );
    let client = tidb_store_driver_txn::cluster_http::internal_http_client(security)?;
    let body = client
        .get(stat_url)
        .send()
        .and_then(|response| response.text())
        .map_err(|error| error.to_string())?;
    compute_tiflash_status(&body)
}

/// Go `calculateTiFlashProgress` over `region_count` (Go
/// `GetTiFlashRegionCountFromPD`) and `collect` (Go
/// `CollectTiFlashStatusWithCtx` by status address): `(full replica
/// progress, one replica progress)`. A store that fails is skipped unless
/// it is up or disconnected (Go `getTiFlashPeerWithoutLagCount`).
///
/// # Errors
/// PD's error, a zero region count, or a live store's error.
pub fn calculate_tiflash_progress(
    region_count: Result<i64, String>,
    table_id: i64,
    replica_count: u64,
    tiflash_stores: &[pd_http::StoreInfo],
    collect: impl Fn(&str) -> Result<Vec<i64>, String>,
) -> Result<(f64, f64), String> {
    let region_count = region_count.inspect_err(|_| {
        tidb_logutil::bg_logger().warn(
            "Fail to get regionCount from PD.",
            &[Field::new("tableID", Value::I64(table_id))],
        );
    })?;
    if region_count <= 0 {
        tidb_logutil::bg_logger().warn(
            "region count getting from PD is 0.",
            &[Field::new("tableID", Value::I64(table_id))],
        );
        return Err("region count getting from PD is 0".to_owned());
    }
    // A region counts once even when several stores hold it.
    let mut regions_with_peer = std::collections::HashSet::new();
    let mut peer_count = 0usize;
    for store in tiflash_stores {
        let store_regions: std::collections::HashSet<_> = match collect(&store.store.status_address)
        {
            Ok(regions) => regions.into_iter().collect(),
            // Down, offline or tombstone stores are skipped: PD migrates
            // their regions.
            Err(error)
                if store.store.state_name == "Up" || store.store.state_name == "Disconnected" =>
            {
                tidb_logutil::bg_logger().warn(
                    "Fail to get peer count from TiFlash.",
                    &[
                        Field::new("tableID", Value::I64(table_id)),
                        Field::new("storeAddr", Value::Str(store.store.status_address.clone())),
                        Field::new("error", Value::Str(error.clone())),
                    ],
                );
                return Err(error);
            }
            Err(_) => continue,
        };
        peer_count += store_regions.len();
        regions_with_peer.extend(store_regions);
    }
    let region_count = region_count as f64;
    // Balancing between TiFlash stores may leave more than `replica_count`
    // peers on some regions.
    let full_replica_progress =
        (peer_count as f64 / (region_count * replica_count as f64)).min(1.0);
    let one_replica_progress = regions_with_peer.len() as f64 / region_count;
    Ok((full_replica_progress, one_replica_progress))
}

/// Go `infosync.GetTiFlashProgressStores`: the TiFlash write stores and the
/// TiKV stores, TiFlash compute nodes left out.
///
/// # Errors
/// PD's error.
pub fn get_tiflash_progress_stores(
) -> Result<(Vec<pd_http::StoreInfo>, Vec<pd_http::StoreInfo>), String> {
    let stats = get_tiflash_stores_stat()?;
    let (mut tiflash_stores, mut tikv_stores) = (Vec::new(), Vec::new());
    for store in stats.stores {
        if is_tiflash_http_resp(&store.store) {
            if is_tiflash_write_http_resp(&store.store) {
                tiflash_stores.push(store);
            }
            continue;
        }
        tikv_stores.push(store);
    }
    Ok((tiflash_stores, tikv_stores))
}

/// Go `engine.IsTiFlashWriteHTTPResp`: a TiFlash node under the classic
/// kernel or a TiFlash write node under the next-gen one.
#[must_use]
pub fn is_tiflash_write_http_resp(store: &pd_http::MetaStore) -> bool {
    store
        .labels
        .iter()
        .any(|label| label.key == "engine" && label.value == "tiflash")
}

/// Go `infosync.MustGetTiFlashProgress`: the cached progress, or the
/// lower of the TiFlash progress (with TiFlash stores) and the columnar
/// progress (with TiKV stores), cached.
///
/// # Errors
/// The calculation's error.
pub fn must_get_tiflash_progress(
    table_id: i64,
    replica_count: u64,
    tiflash_stores: &[pd_http::StoreInfo],
    tikv_stores: &[pd_http::StoreInfo],
) -> Result<f64, String> {
    let manager = tiflash_replica_manager();
    if let Some(progress) = manager.get_tiflash_progress_from_cache(table_id) {
        return Ok(progress);
    }
    let mut tiflash_progress = 1.0;
    let mut columnar_progress = 1.0;
    if !tiflash_stores.is_empty() {
        tiflash_progress = manager
            .calculate_tiflash_progress(table_id, replica_count, tiflash_stores)?
            .0;
    }
    if !tikv_stores.is_empty() {
        columnar_progress = calculate_columnar_progress(table_id, tikv_stores)?;
    }
    let progress = f64::min(tiflash_progress, columnar_progress);
    manager.update_tiflash_progress_cache(table_id, progress);
    Ok(progress)
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

    fn store(state: &str, labels: &[(&str, &str)], addr: &str) -> pd_http::StoreInfo {
        pd_http::StoreInfo {
            store: pd_http::MetaStore {
                state_name: state.to_owned(),
                status_address: addr.to_owned(),
                labels: labels
                    .iter()
                    .map(|(key, value)| pd_http::StoreLabel {
                        key: (*key).to_owned(),
                        value: (*value).to_owned(),
                    })
                    .collect(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Go `calculateTiFlashProgress`: a region counts once for the one
    /// replica progress, peers count toward the full one (capped at 1), and
    /// only a live store's failure is an error.
    #[test]
    fn tiflash_progress_counts_regions_and_peers_like_go() {
        let stores = [
            store("Up", &[], "a"),
            store("Up", &[], "b"),
            store("Down", &[], "c"),
        ];
        let collect = |addr: &str| match addr {
            "a" => Ok(vec![1, 2]),
            "b" => Ok(vec![2]),
            _ => Err("down".to_owned()),
        };
        assert_eq!(
            calculate_tiflash_progress(Ok(4), 7, 2, &stores, collect).unwrap(),
            (3.0 / 8.0, 0.5)
        );
        // More peers than replicas while PD balances: capped.
        assert_eq!(
            calculate_tiflash_progress(Ok(1), 7, 1, &stores, collect).unwrap(),
            (1.0, 2.0)
        );
        assert_eq!(
            calculate_tiflash_progress(Ok(0), 7, 1, &stores, collect).unwrap_err(),
            "region count getting from PD is 0"
        );
        calculate_tiflash_progress(Err("pd".to_owned()), 7, 1, &stores, collect).unwrap_err();
        let live_failure = [store("Disconnected", &[], "c")];
        assert_eq!(
            calculate_tiflash_progress(Ok(1), 7, 1, &live_failure, collect).unwrap_err(),
            "down"
        );
    }

    /// Go `ComputeTiFlashStatus`: the count line, then the IDs.
    #[test]
    fn compute_tiflash_status_reads_the_region_ids() {
        assert_eq!(
            compute_tiflash_status("2\n1009 1010 \n").unwrap(),
            vec![1009, 1010]
        );
        compute_tiflash_status("2\n1009").unwrap_err();
    }

    /// Go `engine.IsTiFlashWriteHTTPResp`: a compute node is not a write
    /// store.
    #[test]
    fn tiflash_write_stores_exclude_compute_nodes() {
        assert!(is_tiflash_write_http_resp(
            &store("Up", &[("engine", "tiflash")], "").store
        ));
        assert!(!is_tiflash_write_http_resp(
            &store("Up", &[("engine", "tiflash_compute")], "").store
        ));
        assert!(is_tiflash_http_resp(
            &store("Up", &[("engine", "tiflash_compute")], "").store
        ));
    }
}
