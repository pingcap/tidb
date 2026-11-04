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
}

/// Go `TiFlashReplicaManagerCtx` over the info syncer's PD HTTP client.
pub struct TiFlashReplicaManagerCtx {
    pd_http_client: pd_http::Client,
}

impl TiFlashReplicaManagerCtx {
    /// Over the store's PD HTTP client, as the info syncer derives it.
    pub fn new(pd_http_client: &pd_http::Client) -> Self {
        Self {
            pd_http_client: crate::pd_http::info_syncer_pd_http_client(pd_http_client),
        }
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
}

/// Go `mockTiFlashReplicaManagerCtx` with no mock TiFlash: every write is a
/// no-op. Its record of the calls is what tests read.
#[derive(Default)]
pub struct MockTiFlashReplicaManager {
    calls: Mutex<Vec<String>>,
}

impl MockTiFlashReplicaManager {
    /// The calls made so far, as `set <rule id> <count>`, `accel <ids>`.
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn record(&self, call: String) {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner).push(call);
    }
}

impl TiFlashReplicaManager for MockTiFlashReplicaManager {
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
}

/// The info syncer's `tiflashReplicaManager`.
static TIFLASH_REPLICA_MANAGER: Mutex<Option<Arc<dyn TiFlashReplicaManager>>> = Mutex::new(None);

/// Go `InfoSyncer.initTiFlashReplicaManager`: PD's manager, or a mock when
/// the node has no PD.
pub fn init_tiflash_replica_manager(manager: Option<Arc<dyn TiFlashReplicaManager>>) {
    *TIFLASH_REPLICA_MANAGER
        .lock()
        .unwrap_or_else(PoisonError::into_inner) =
        Some(manager.unwrap_or_else(|| Arc::new(MockTiFlashReplicaManager::default())));
}

/// The node's TiFlash replica manager; a node that installed none gets a mock.
pub fn tiflash_replica_manager() -> Arc<dyn TiFlashReplicaManager> {
    Arc::clone(
        TIFLASH_REPLICA_MANAGER
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| Arc::new(MockTiFlashReplicaManager::default())),
    )
}

fn tiflash_enabled() -> bool {
    tidb_config::config_tree::config::get_global_config().cse.is_tiflash_enabled()
}

fn info(message: &str, fields: &[Field]) {
    tidb_logutil::bg_logger().info(message, fields);
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
