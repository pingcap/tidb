// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `interface.go`: every method of the PD HTTP `Client` interface. Request
//! bodies are encoded as Go's `encoding/json` would: struct fields in
//! declaration order and `map` keys sorted.

use std::collections::{BTreeMap, HashMap};

use reqwest::Method;
use serde_derive::{Deserialize, Serialize};

use super::api::*;
use super::client::{with_allow_follower_handle, Client};
use super::request_info::*;
use super::types::*;
use crate::proto::keyspacepb;
use crate::proto::metapb;
use crate::proto::pdpb;
use crate::{Error, Result};

// Go `json.Marshal`, whose errors are traced into the call's result.
fn marshal<T: serde::Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| Error::StringError(error.to_string()))
}

// Go encodes a map with its keys sorted.
fn sorted<V>(map: &HashMap<String, V>) -> BTreeMap<&String, &V> {
    map.iter().collect()
}

#[derive(Default)]
struct CreateAffinityGroupsOptions {
    skip_exist_check: bool,
}

/// Go `CreateAffinityGroupsOption`: customizes `create_affinity_groups` behavior.
pub struct CreateAffinityGroupsOption(Box<dyn Fn(&mut CreateAffinityGroupsOptions) + Send + Sync>);

/// Go `WithSkipExistCheck`: skips existing groups and creates the rest.
pub fn with_skip_exist_check() -> CreateAffinityGroupsOption {
    CreateAffinityGroupsOption(Box::new(|opts: &mut CreateAffinityGroupsOptions| {
        opts.skip_exist_check = true
    }))
}

impl Client {
    /* Member-related interfaces */

    /// Go `GetMembers`: the members info of PD cluster.
    pub async fn get_members(&self) -> Result<MembersInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_MEMBERS_NAME)
                .with_uri(MEMBERS_PREFIX)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetLeader`: the leader of PD cluster.
    pub async fn get_leader(&self) -> Result<pdpb::Member> {
        let leader: PbMember = self
            .request_resp(
                new_request_info()
                    .with_name(GET_LEADER_NAME)
                    .with_uri(LEADER_PREFIX)
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        Ok(leader.0)
    }

    /// Go `TransferLeader`: transfers the PD leader.
    pub async fn transfer_leader(&self, new_leader: &str) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(TRANSFER_LEADER_NAME)
                .with_uri(transfer_leader_by_id(new_leader))
                .with_method(Method::POST),
            &[],
        )
        .await
    }

    /* Meta-related interfaces */

    /// Go `GetRegionByID`: the region info by ID.
    pub async fn get_region_by_id(&self, region_id: u64) -> Result<RegionInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REGION_BY_ID_NAME)
                .with_uri(region_by_id(region_id))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetRegionByKey`: the region info by key.
    pub async fn get_region_by_key(&self, key: &[u8]) -> Result<RegionInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REGION_BY_KEY_NAME)
                .with_uri(region_by_key(key))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetRegions`: the regions info.
    pub async fn get_regions(&self) -> Result<RegionsInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REGIONS_NAME)
                .with_uri(REGIONS)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetRegionsByKeyRange`: the regions info by key range. If the limit
    /// is -1, it will return all regions within the range. The keys in the key
    /// range should be encoded in the UTF-8 bytes format.
    pub async fn get_regions_by_key_range(
        &self,
        key_range: &KeyRange,
        limit: i64,
    ) -> Result<RegionsInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REGIONS_BY_KEY_RANGE_NAME)
                .with_uri(regions_by_key_range(key_range, limit))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetRegionsByStoreID`: the regions info by store ID.
    pub async fn get_regions_by_store_id(&self, store_id: u64) -> Result<RegionsInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REGIONS_BY_STORE_ID_NAME)
                .with_uri(regions_by_store_id(store_id))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetEmptyRegions`: the empty regions info.
    pub async fn get_empty_regions(&self) -> Result<RegionsInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_EMPTY_REGIONS_NAME)
                .with_uri(EMPTY_REGIONS)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetRegionsReplicatedStateByKeyRange`: the regions replicated state
    /// info by key range. The keys in the key range should be encoded in the
    /// hex bytes format (without encoding to the UTF-8 bytes).
    pub async fn get_regions_replicated_state_by_key_range(
        &self,
        key_range: &KeyRange,
    ) -> Result<String> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REGIONS_REPLICATED_STATE_BY_KEY_RANGE_NAME)
                .with_uri(regions_replicated_by_key_range(key_range))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetRegionSiblingsByID`: the regions sibling info by ID.
    pub async fn get_region_siblings_by_id(&self, region_id: u64) -> Result<RegionsInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REGION_SIBLINGS_BY_ID)
                .with_uri(region_siblings_by_id(region_id))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetHotReadRegions`: the hot read region statistics info.
    pub async fn get_hot_read_regions(&self) -> Result<StoreHotPeersInfos> {
        self.request_resp(
            new_request_info()
                .with_name(GET_HOT_READ_REGIONS_NAME)
                .with_uri(HOT_READ)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetHotWriteRegions`: the hot write region statistics info.
    pub async fn get_hot_write_regions(&self) -> Result<StoreHotPeersInfos> {
        self.request_resp(
            new_request_info()
                .with_name(GET_HOT_WRITE_REGIONS_NAME)
                .with_uri(HOT_WRITE)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetHistoryHotRegions`: the history hot region statistics info. A
    /// PD follower may serve it.
    pub async fn get_history_hot_regions(
        &self,
        req: &HistoryHotRegionsRequest,
    ) -> Result<HistoryHotRegions> {
        let req_json = marshal(req)?;
        self.request_resp(
            new_request_info()
                .with_name(GET_HISTORY_HOT_REGIONS_NAME)
                .with_uri(HOT_HISTORY)
                .with_method(Method::GET)
                .with_body(req_json),
            &[with_allow_follower_handle()],
        )
        .await
    }

    /// Go `GetRegionDistributionByKeyRange`: the region distribution by key
    /// range, one entry per store in `store_peer_count` (unordered, as Go's map
    /// iteration is).
    pub async fn get_region_distribution_by_key_range(
        &self,
        key_range: &KeyRange,
        engine: &str,
    ) -> Result<RegionDistributions> {
        let region_stats: RegionStats = self
            .request_resp(
                new_request_info()
                    .with_name(GET_REGION_DISTRIBUTIONS_BY_KEY_RANGE_NAME)
                    .with_uri(region_distributions_by_key_range(key_range, engine))
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        fn get<V: Copy + Default>(map: &HashMap<u64, V>, store_id: &u64) -> V {
            map.get(store_id).copied().unwrap_or_default()
        }
        let mut distributions = Vec::with_capacity(region_stats.store_peer_count.len());
        for (store_id, region) in &region_stats.store_peer_count {
            distributions.push(RegionDistribution {
                store_id: *store_id,
                engine_type: region_stats
                    .store_engine
                    .get(store_id)
                    .cloned()
                    .unwrap_or_default(),
                region_leader_count: get(&region_stats.store_leader_count, store_id),
                region_peer_count: *region,
                approximate_size: get(&region_stats.store_peer_size, store_id),
                approximate_keys: get(&region_stats.store_peer_keys, store_id),
                region_write_bytes: get(&region_stats.store_write_bytes, store_id),
                region_write_keys: get(&region_stats.store_write_keys, store_id),
                region_write_query: get(&region_stats.store_write_query, store_id),
                region_leader_read_bytes: get(&region_stats.store_leader_read_bytes, store_id),
                region_leader_read_keys: get(&region_stats.store_leader_read_keys, store_id),
                region_leader_read_query: get(&region_stats.store_leader_read_query, store_id),
                region_peer_read_bytes: get(&region_stats.store_peer_read_bytes, store_id),
                region_peer_read_keys: get(&region_stats.store_peer_read_keys, store_id),
                region_peer_read_query: get(&region_stats.store_peer_read_query, store_id),
            });
        }
        Ok(RegionDistributions {
            region_distributions: distributions,
        })
    }

    /// Go `GetRegionStatusByKeyRange`: the region status by key range. If the
    /// `only_count` flag is true, the result will only include the count of
    /// regions. The keys in the key range should be encoded in the UTF-8 bytes
    /// format.
    pub async fn get_region_status_by_key_range(
        &self,
        key_range: &KeyRange,
        only_count: bool,
    ) -> Result<RegionStats> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REGION_STATUS_BY_KEY_RANGE_NAME)
                .with_uri(region_stats_by_key_range(key_range, only_count))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `SetStoreLabels`: sets the labels of a store.
    pub async fn set_store_labels(
        &self,
        store_id: i64,
        store_labels: &HashMap<String, String>,
    ) -> Result<()> {
        let json_input = marshal(&sorted(store_labels))?;
        self.request_no_resp(
            new_request_info()
                .with_name(SET_STORE_LABELS_NAME)
                .with_uri(label_by_store_id(store_id))
                .with_method(Method::POST)
                .with_body(json_input),
            &[],
        )
        .await
    }

    /// Go `DeleteStoreLabel`: deletes the labels of a store.
    pub async fn delete_store_label(&self, store_id: i64, label_key: &str) -> Result<()> {
        let json_input = marshal(label_key)?;
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_STORE_LABEL_NAME)
                .with_uri(label_by_store_id(store_id))
                .with_method(Method::DELETE)
                .with_body(json_input),
            &[],
        )
        .await
    }

    /// Go `GetHealthStatus`: the health status of the cluster.
    pub async fn get_health_status(&self) -> Result<Vec<Health>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_HEALTH_STATUS_NAME)
                .with_uri(HEALTH)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /* Config-related interfaces */

    /// Go `GetConfig`: the configurations.
    pub async fn get_config(&self) -> Result<HashMap<String, serde_json::Value>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_CONFIG_NAME)
                .with_uri(CONFIG)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `SetConfig`: sets the configurations. `ttl_second` is Go's optional
    /// variadic argument.
    pub async fn set_config(
        &self,
        config: &HashMap<String, serde_json::Value>,
        ttl_second: Option<f64>,
    ) -> Result<()> {
        let config_json = marshal(&sorted(config))?;
        let uri = match ttl_second {
            Some(ttl_second) => config_with_ttl_seconds(ttl_second),
            None => CONFIG.to_owned(),
        };
        self.request_no_resp(
            new_request_info()
                .with_name(SET_CONFIG_NAME)
                .with_uri(uri)
                .with_method(Method::POST)
                .with_body(config_json),
            &[],
        )
        .await
    }

    /// Go `GetScheduleConfig`: the schedule configurations.
    pub async fn get_schedule_config(&self) -> Result<HashMap<String, serde_json::Value>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_SCHEDULE_CONFIG_NAME)
                .with_uri(SCHEDULE_CONFIG)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `SetScheduleConfig`: sets the schedule configurations.
    pub async fn set_schedule_config(
        &self,
        config: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let config_json = marshal(&sorted(config))?;
        self.request_no_resp(
            new_request_info()
                .with_name(SET_SCHEDULE_CONFIG_NAME)
                .with_uri(SCHEDULE_CONFIG)
                .with_method(Method::POST)
                .with_body(config_json),
            &[],
        )
        .await
    }

    /// Go `GetStores`: the stores info.
    pub async fn get_stores(&self) -> Result<StoresInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_STORES_NAME)
                .with_uri(STORES)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetStore`: the store info by ID.
    pub async fn get_store(&self, store_id: u64) -> Result<StoreInfo> {
        self.request_resp(
            new_request_info()
                .with_name(GET_STORE_NAME)
                .with_uri(store_by_id(store_id))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `DeleteStore`: deletes the store by ID.
    pub async fn delete_store(&self, store_id: u64) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_STORE_NAME)
                .with_uri(store_by_id(store_id))
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /// Go `GetClusterVersion`: the cluster version.
    pub async fn get_cluster_version(&self) -> Result<String> {
        self.request_resp(
            new_request_info()
                .with_name(GET_CLUSTER_VERSION_NAME)
                .with_uri(CLUSTER_VERSION)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetCluster`: the cluster meta information; `None` when PD answers
    /// JSON `null`, as Go's result pointer is then nil.
    pub async fn get_cluster(&self) -> Result<Option<metapb::Cluster>> {
        let cluster: Option<ClusterJson> = self
            .request_resp(
                new_request_info()
                    .with_name(GET_CLUSTER_NAME)
                    .with_uri(CLUSTER)
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        Ok(cluster.map(ClusterJson::into_pb))
    }

    /// Go `GetClusterStatus`: the cluster status; `None` for a JSON `null`.
    /// Go labels this request with `GetCluster`'s name, and so does this port.
    pub async fn get_cluster_status(&self) -> Result<Option<ClusterState>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_CLUSTER_NAME)
                .with_uri(CLUSTER_STATUS)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetStatus`: the status of PD; `None` for a JSON `null`. A PD
    /// follower may serve it.
    pub async fn get_status(&self) -> Result<Option<State>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_STATUS_NAME)
                .with_uri(STATUS)
                .with_method(Method::GET),
            &[with_allow_follower_handle()],
        )
        .await
    }

    /// Go `GetReplicateConfig`: the replication configurations.
    pub async fn get_replicate_config(&self) -> Result<HashMap<String, serde_json::Value>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_REPLICATE_CONFIG_NAME)
                .with_uri(REPLICATE_CONFIG)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /* Rule-related interfaces */

    /// Go `GetAllPlacementRuleBundles`: all placement rules bundles.
    pub async fn get_all_placement_rule_bundles(&self) -> Result<Vec<GroupBundle>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_ALL_PLACEMENT_RULE_BUNDLES_NAME)
                .with_uri(PLACEMENT_RULE_BUNDLE)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetPlacementRuleBundleByGroup`: the placement rules bundle by group.
    pub async fn get_placement_rule_bundle_by_group(&self, group: &str) -> Result<GroupBundle> {
        self.request_resp(
            new_request_info()
                .with_name(GET_PLACEMENT_RULE_BUNDLE_BY_GROUP_NAME)
                .with_uri(placement_rule_bundle_by_group(group))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetPlacementRulesByGroup`: the placement rules by group.
    pub async fn get_placement_rules_by_group(&self, group: &str) -> Result<Vec<Rule>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_PLACEMENT_RULES_BY_GROUP_NAME)
                .with_uri(placement_rules_by_group(group))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetPlacementRule`: the placement rule by group and ID.
    pub async fn get_placement_rule(&self, group: &str, id: &str) -> Result<Rule> {
        self.request_resp(
            new_request_info()
                .with_name(GET_PLACEMENT_RULE_NAME)
                .with_uri(placement_rule_by_group_and_id(group, id))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `SetPlacementRule`: sets the placement rule.
    pub async fn set_placement_rule(&self, rule: &Rule) -> Result<()> {
        let rule_json = marshal(rule)?;
        self.request_no_resp(
            new_request_info()
                .with_name(SET_PLACEMENT_RULE_NAME)
                .with_uri(PLACEMENT_RULE)
                .with_method(Method::POST)
                .with_body(rule_json),
            &[],
        )
        .await
    }

    /// Go `SetPlacementRuleInBatch`: sets the placement rules in batch.
    pub async fn set_placement_rule_in_batch(&self, rule_ops: &[RuleOp]) -> Result<()> {
        let rule_ops_json = marshal(rule_ops)?;
        self.request_no_resp(
            new_request_info()
                .with_name(SET_PLACEMENT_RULE_IN_BATCH_NAME)
                .with_uri(PLACEMENT_RULES_IN_BATCH)
                .with_method(Method::POST)
                .with_body(rule_ops_json),
            &[],
        )
        .await
    }

    /// Go `SetPlacementRuleBundles`: sets the placement rule bundles. If
    /// `partial` is false, all old configurations will be over-written and
    /// dropped.
    pub async fn set_placement_rule_bundles(
        &self,
        bundles: &[GroupBundle],
        partial: bool,
    ) -> Result<()> {
        let bundles_json = marshal(bundles)?;
        self.request_no_resp(
            new_request_info()
                .with_name(SET_PLACEMENT_RULE_BUNDLES_NAME)
                .with_uri(placement_rule_bundle_with_partial_parameter(partial))
                .with_method(Method::POST)
                .with_body(bundles_json),
            &[],
        )
        .await
    }

    /// Go `DeletePlacementRule`: deletes the placement rule.
    pub async fn delete_placement_rule(&self, group: &str, id: &str) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_PLACEMENT_RULE_NAME)
                .with_uri(placement_rule_by_group_and_id(group, id))
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /// Go `DeletePlacementRuleBundleByGroup`: deletes the placement rule bundle
    /// by group.
    pub async fn delete_placement_rule_bundle_by_group(&self, group: &str) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_PLACEMENT_RULE_BUNDLE_BY_GROUP_NAME)
                .with_uri(placement_rule_bundle_by_group(group))
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /// Go `GetAllPlacementRuleGroups`: all placement rule groups.
    pub async fn get_all_placement_rule_groups(&self) -> Result<Vec<RuleGroup>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_ALL_PLACEMENT_RULE_GROUPS_NAME)
                .with_uri(PLACEMENT_RULE_GROUPS)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetPlacementRuleGroupByID`: the placement rule group by ID.
    pub async fn get_placement_rule_group_by_id(&self, id: &str) -> Result<RuleGroup> {
        self.request_resp(
            new_request_info()
                .with_name(GET_PLACEMENT_RULE_GROUP_BY_ID_NAME)
                .with_uri(placement_rule_group_by_id(id))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `SetPlacementRuleGroup`: sets the placement rule group.
    pub async fn set_placement_rule_group(&self, rule_group: &RuleGroup) -> Result<()> {
        let rule_group_json = marshal(rule_group)?;
        self.request_no_resp(
            new_request_info()
                .with_name(SET_PLACEMENT_RULE_GROUP_NAME)
                .with_uri(PLACEMENT_RULE_GROUP)
                .with_method(Method::POST)
                .with_body(rule_group_json),
            &[],
        )
        .await
    }

    /// Go `DeletePlacementRuleGroupByID`: deletes the placement rule group by ID.
    pub async fn delete_placement_rule_group_by_id(&self, id: &str) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_PLACEMENT_RULE_GROUP_BY_ID_NAME)
                .with_uri(placement_rule_group_by_id(id))
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /// Go `GetAllRegionLabelRules`: all region label rules.
    pub async fn get_all_region_label_rules(&self) -> Result<Vec<LabelRule>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_ALL_REGION_LABEL_RULES_NAME)
                .with_uri(REGION_LABEL_RULES)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetRegionLabelRulesByIDs`: the region label rules by IDs.
    pub async fn get_region_label_rules_by_ids(
        &self,
        rule_ids: &[String],
    ) -> Result<Vec<LabelRule>> {
        let ids_json = marshal(rule_ids)?;
        self.request_resp(
            new_request_info()
                .with_name(GET_REGION_LABEL_RULES_BY_IDS_NAME)
                .with_uri(REGION_LABEL_RULES_BY_IDS)
                .with_method(Method::GET)
                .with_body(ids_json),
            &[],
        )
        .await
    }

    /// Go `SetRegionLabelRule`: sets the label rule for a region. When a label
    /// rule (deny scheduler) is set,
    ///  1. All schedulers will be disabled except for the evict-leader-scheduler.
    ///  2. The merge-checker will be disabled, preventing these regions from being merged.
    pub async fn set_region_label_rule(&self, label_rule: &LabelRule) -> Result<()> {
        let label_rule_json = marshal(label_rule)?;
        self.request_no_resp(
            new_request_info()
                .with_name(SET_REGION_LABEL_RULE_NAME)
                .with_uri(REGION_LABEL_RULE)
                .with_method(Method::POST)
                .with_body(label_rule_json),
            &[],
        )
        .await
    }

    /// Go `PatchRegionLabelRules`: patches the region label rules.
    pub async fn patch_region_label_rules(&self, label_rule_patch: &LabelRulePatch) -> Result<()> {
        let label_rule_patch_json = marshal(label_rule_patch)?;
        self.request_no_resp(
            new_request_info()
                .with_name(PATCH_REGION_LABEL_RULES_NAME)
                .with_uri(REGION_LABEL_RULES)
                .with_method(Method::PATCH)
                .with_body(label_rule_patch_json),
            &[],
        )
        .await
    }

    /* Scheduler-related interfaces */

    /// Go `GetSchedulers`: the schedulers from PD cluster.
    pub async fn get_schedulers(&self) -> Result<Vec<String>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_SCHEDULERS_NAME)
                .with_uri(SCHEDULERS)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetSchedulerConfig`: the configuration of the specified scheduler
    /// for pd cluster (Go `any`; `Null` when absent).
    pub async fn get_scheduler_config(&self, name: &str) -> Result<serde_json::Value> {
        self.request_resp(
            new_request_info()
                .with_name(GET_SCHEDULER_CONFIG)
                .with_uri(get_scheduler_config_uri_by_name(name))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `CancelSchedulerJob`: cancels the specified scheduler job.
    pub async fn cancel_scheduler_job(&self, name: &str, job_id: u64) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(CANCEL_SCHEDULER_JOB_NAME)
                .with_uri(get_cancel_scheduler_job_uri_by_name_and_job_id(
                    name, job_id,
                ))
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /// Go `CreateSchedulerWithInput`: creates a scheduler with the specified
    /// input. As in Go, `name` is written into the caller's input map.
    pub async fn create_scheduler_with_input(
        &self,
        name: &str,
        input: &mut HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        input.insert(
            "name".to_owned(),
            serde_json::Value::String(name.to_owned()),
        );
        let input_json = marshal(&sorted(input))?;
        self.request_no_resp(
            new_request_info()
                .with_name(CREATE_SCHEDULER_NAME_WITH_INPUT)
                .with_uri(SCHEDULERS)
                .with_method(Method::POST)
                .with_body(input_json),
            &[],
        )
        .await
    }

    /// Go `CreateScheduler`: creates a scheduler to PD cluster.
    pub async fn create_scheduler(&self, name: &str, store_id: u64) -> Result<()> {
        // Go marshals a map, so the keys are sorted.
        #[derive(Serialize)]
        struct Input<'a> {
            name: &'a str,
            store_id: u64,
        }
        let input_json = marshal(&Input { name, store_id })?;
        self.request_no_resp(
            new_request_info()
                .with_name(CREATE_SCHEDULER_NAME)
                .with_uri(SCHEDULERS)
                .with_method(Method::POST)
                .with_body(input_json),
            &[],
        )
        .await
    }

    /// Go `DeleteScheduler`: deletes a scheduler from PD cluster.
    pub async fn delete_scheduler(&self, name: &str) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_SCHEDULER_NAME)
                .with_uri(scheduler_by_name(name))
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /* Scheduling-related interfaces */

    /// Go `AccelerateSchedule`: accelerates the scheduling of the regions
    /// within the given key range. The keys in the key range should be encoded
    /// in the hex bytes format (without encoding to the UTF-8 bytes).
    pub async fn accelerate_schedule(&self, key_range: &KeyRange) -> Result<()> {
        let (start_key, end_key) = key_range.escape_as_hex_str();
        let input_json = marshal(&KeyRangeInput {
            end_key: &end_key,
            start_key: &start_key,
        })?;
        self.request_no_resp(
            new_request_info()
                .with_name(ACCELERATE_SCHEDULE_NAME)
                .with_uri(ACCELERATE_SCHEDULE)
                .with_method(Method::POST)
                .with_body(input_json),
            &[],
        )
        .await
    }

    /// Go `AccelerateScheduleInBatch`: accelerates the scheduling of the
    /// regions within the given key ranges in batch. The keys in the key
    /// ranges should be encoded in the hex bytes format (without encoding to
    /// the UTF-8 bytes).
    pub async fn accelerate_schedule_in_batch(&self, key_ranges: &[KeyRange]) -> Result<()> {
        let escaped: Vec<(String, String)> = key_ranges
            .iter()
            .map(|key_range| key_range.escape_as_hex_str())
            .collect();
        let input: Vec<KeyRangeInput<'_>> = escaped
            .iter()
            .map(|(start_key, end_key)| KeyRangeInput { end_key, start_key })
            .collect();
        let input_json = marshal(&input)?;
        self.request_no_resp(
            new_request_info()
                .with_name(ACCELERATE_SCHEDULE_IN_BATCH_NAME)
                .with_uri(ACCELERATE_SCHEDULE_IN_BATCH)
                .with_method(Method::POST)
                .with_body(input_json),
            &[],
        )
        .await
    }

    /* Admin-related interfaces */

    /// Go `ResetTS`: resets the PD's TS.
    pub async fn reset_ts(&self, ts: u64, force_use_larger: bool) -> Result<()> {
        #[derive(Serialize)]
        struct ResetTsRequest {
            #[serde(rename = "tso")]
            tso: String,
            #[serde(rename = "force-use-larger")]
            force_use_larger: bool,
        }
        let req_data = marshal(&ResetTsRequest {
            tso: ts.to_string(),
            force_use_larger,
        })?;
        self.request_no_resp(
            new_request_info()
                .with_name(RESET_TS_NAME)
                .with_uri(RESET_TS)
                .with_method(Method::POST)
                .with_body(req_data),
            &[],
        )
        .await
    }

    /// Go `ResetBaseAllocID`: resets the PD's base alloc ID.
    pub async fn reset_base_alloc_id(&self, id: u64) -> Result<()> {
        #[derive(Serialize)]
        struct ResetBaseAllocIdRequest {
            #[serde(rename = "id")]
            id: String,
        }
        let req_data = marshal(&ResetBaseAllocIdRequest { id: id.to_string() })?;
        self.request_no_resp(
            new_request_info()
                .with_name(RESET_BASE_ALLOC_ID_NAME)
                .with_uri(BASE_ALLOC_ID)
                .with_method(Method::POST)
                .with_body(req_data),
            &[],
        )
        .await
    }

    /// Go `SetSnapshotRecoveringMark`: sets the snapshot recovering mark.
    pub async fn set_snapshot_recovering_mark(&self) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(SET_SNAPSHOT_RECOVERING_MARK_NAME)
                .with_uri(SNAPSHOT_RECOVERING_MARK)
                .with_method(Method::POST),
            &[],
        )
        .await
    }

    /// Go `DeleteSnapshotRecoveringMark`: deletes the snapshot recovering mark.
    pub async fn delete_snapshot_recovering_mark(&self) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_SNAPSHOT_RECOVERING_MARK_NAME)
                .with_uri(SNAPSHOT_RECOVERING_MARK)
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /// Go `SetPitrRestoreModeMark`: sets the pitr restore mode mark.
    pub async fn set_pitr_restore_mode_mark(&self) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(SET_PITR_RESTORE_MODE_MARK_NAME)
                .with_uri(PITR_RESTORE_MODE_MARK)
                .with_method(Method::POST),
            &[],
        )
        .await
    }

    /// Go `DeletePitrRestoreModeMark`: deletes the pitr restore mode mark.
    pub async fn delete_pitr_restore_mode_mark(&self) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_PITR_RESTORE_MODE_MARK_NAME)
                .with_uri(PITR_RESTORE_MODE_MARK)
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /// Go `SetSchedulerDelay`: sets the delay of given scheduler.
    pub async fn set_scheduler_delay(&self, scheduler: &str, delay_sec: i64) -> Result<()> {
        #[derive(Serialize)]
        struct DelayInput {
            delay: i64,
        }
        let input_json = marshal(&DelayInput { delay: delay_sec })?;
        self.request_no_resp(
            new_request_info()
                .with_name(SET_SCHEDULER_DELAY_NAME)
                .with_uri(scheduler_by_name(scheduler))
                .with_method(Method::POST)
                .with_body(input_json),
            &[],
        )
        .await
    }

    /* Other interfaces */

    /// Go `GetMinResolvedTSByStoresIDs`: min-resolved-ts by stores IDs.
    /// - When `store_ids` has zero length, it will return (cluster-level's
    ///   min_resolved_ts, None) when no error.
    /// - When `store_ids` is {"cluster"}, it will return (cluster-level's
    ///   min_resolved_ts, stores_min_resolved_ts) when no error.
    /// - When `store_ids` is specified to ID lists, it will return
    ///   (min_resolved_ts of given stores, stores_min_resolved_ts) when no error.
    pub async fn get_min_resolved_ts_by_stores_ids(
        &self,
        store_ids: &[u64],
    ) -> Result<(u64, Option<HashMap<u64, u64>>)> {
        let mut uri = MIN_RESOLVED_TS_PREFIX.to_owned();
        // scope is an optional parameter, it can be `cluster` or specified
        // store IDs.
        // - When no scope is given, cluster-level's min_resolved_ts will be
        //   returned and storesMinResolvedTS will be nil.
        // - When scope is `cluster`, cluster-level's min_resolved_ts will be
        //   returned and storesMinResolvedTS will be filled.
        // - When scope given a list of stores, min_resolved_ts will be provided
        //   for each store and the scope-specific min_resolved_ts will be
        //   returned.
        if !store_ids.is_empty() {
            let store_id_strs: Vec<String> = store_ids.iter().map(u64::to_string).collect();
            uri = format!("{uri}?scope={}", store_id_strs.join(","));
        }
        #[derive(Default, Deserialize)]
        #[serde(default)]
        struct MinResolvedTsResponse {
            #[serde(rename = "min_resolved_ts")]
            min_resolved_ts: u64,
            #[serde(rename = "is_real_time")]
            is_real_time: bool,
            #[serde(rename = "stores_min_resolved_ts")]
            stores_min_resolved_ts: Option<HashMap<u64, u64>>,
        }
        let resp: MinResolvedTsResponse = self
            .request_resp(
                new_request_info()
                    .with_name(GET_MIN_RESOLVED_TS_BY_STORES_IDS_NAME)
                    .with_uri(uri)
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        if !resp.is_real_time {
            return Err(Error::StringError(
                "min resolved ts is not enabled".to_owned(),
            ));
        }
        Ok((resp.min_resolved_ts, resp.stores_min_resolved_ts))
    }

    /// Go `GetPDVersion`: the release version of the PD binary.
    pub async fn get_pd_version(&self) -> Result<String> {
        #[derive(Default, Deserialize)]
        #[serde(default)]
        struct Ver {
            #[serde(rename = "version")]
            version: String,
        }
        let ver: Ver = self
            .request_resp(
                new_request_info()
                    .with_name(GET_PD_VERSION_NAME)
                    .with_uri(VERSION)
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        Ok(ver.version)
    }

    /// Go `GetGCSafePoint`: the GC safe point list.
    pub async fn get_gc_safe_point(&self) -> Result<ListServiceGCSafepoint> {
        self.request_resp(
            new_request_info()
                .with_name(GET_GC_SAFE_POINT_NAME)
                .with_uri(SAFEPOINT)
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `DeleteGCSafePoint`: deletes a GC safe point with the given service ID.
    pub async fn delete_gc_safe_point(&self, service_id: &str) -> Result<String> {
        self.request_resp(
            new_request_info()
                .with_name(DELETE_GC_SAFE_POINT_NAME)
                .with_uri(get_delete_safe_point_uri(service_id))
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /* Microservice interfaces */

    /// Go `GetMicroserviceMembers`: the members of the microservice.
    pub async fn get_microservice_members(&self, service: &str) -> Result<Vec<MicroserviceMember>> {
        self.request_resp(
            new_request_info()
                .with_name(GET_MICROSERVICE_MEMBERS_NAME)
                .with_uri(microservice_members(service))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetMicroservicePrimary`: the primary of the microservice.
    pub async fn get_microservice_primary(&self, service: &str) -> Result<String> {
        self.request_resp(
            new_request_info()
                .with_name(GET_MICROSERVICE_PRIMARY_NAME)
                .with_uri(microservice_primary(service))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `CreateOperators`: creates the operators.
    pub async fn create_operators(&self, input: &HashMap<String, serde_json::Value>) -> Result<()> {
        let input_json = marshal(&sorted(input))?;
        self.request_no_resp(
            new_request_info()
                .with_name(CREATE_OPERATORS)
                .with_uri(OPERATORS)
                .with_method(Method::POST)
                .with_body(input_json),
            &[],
        )
        .await
    }

    /// Go `DeleteOperators`: deletes the running operators.
    pub async fn delete_operators(&self) -> Result<()> {
        self.request_no_resp(
            new_request_info()
                .with_name(DELETE_OPERATORS)
                .with_uri(OPERATORS)
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /* Keyspace interface */

    // Go `patchKeyspaceConfig`.
    async fn patch_keyspace_config<P: serde::Serialize + Sync>(
        &self,
        req_name: &str,
        keyspace_name: &str,
        params: &P,
    ) -> Result<keyspacepb::KeyspaceMeta> {
        let keyspace_config_patch_json = marshal(params)?;
        let temp_meta: TempKeyspaceMeta = self
            .request_resp(
                new_request_info()
                    .with_name(req_name)
                    .with_uri(get_update_keyspace_config_url(keyspace_name))
                    .with_method(Method::PATCH)
                    .with_body(keyspace_config_patch_json),
                &[],
            )
            .await?;
        temp_meta.to_pb()
    }

    /// Go `UpdateKeyspaceConfig`: patches the keyspace config and returns the
    /// updated keyspace meta. A 409 Conflict is not retried on another member.
    pub async fn update_keyspace_config(
        &self,
        keyspace_name: &str,
        params: &UpdateKeyspaceConfigParams,
    ) -> Result<keyspacepb::KeyspaceMeta> {
        self.patch_keyspace_config(UPDATE_KEYSPACE_CONFIG_NAME, keyspace_name, params)
            .await
    }

    /// Go `UpdateKeyspaceGCManagementType`: updates the `gc_management_type` in
    /// keyspace meta config. If `gc_management_type` is `global_gc`, it means
    /// the current keyspace requires a tidb without 'keyspace-name' configured
    /// to run a global gc worker to calculate a global gc safe point. If
    /// `gc_management_type` is `keyspace_level_gc` it means the current
    /// keyspace can calculate gc safe point by its own.
    pub async fn update_keyspace_gc_management_type(
        &self,
        keyspace_name: &str,
        keyspace_gc_management_type: &KeyspaceGCManagementTypeConfig,
    ) -> Result<()> {
        self.patch_keyspace_config(
            UPDATE_KEYSPACE_GC_MANAGEMENT_TYPE_NAME,
            keyspace_name,
            keyspace_gc_management_type,
        )
        .await
        .map(|_| ())
    }

    /// Go `GetKeyspaceMetaByName`: the given keyspace meta.
    pub async fn get_keyspace_meta_by_name(
        &self,
        keyspace_name: &str,
    ) -> Result<keyspacepb::KeyspaceMeta> {
        let temp_keyspace_meta: TempKeyspaceMeta = self
            .request_resp(
                new_request_info()
                    .with_name(GET_KEYSPACE_META_BY_NAME_NAME)
                    .with_uri(get_keyspace_meta_by_name_url(keyspace_name))
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        temp_keyspace_meta.to_pb()
    }

    /// Go `GetKeyspaceMetaByID`: the given keyspace meta.
    pub async fn get_keyspace_meta_by_id(
        &self,
        keyspace_id: u32,
    ) -> Result<keyspacepb::KeyspaceMeta> {
        let temp_keyspace_meta: TempKeyspaceMeta = self
            .request_resp(
                new_request_info()
                    .with_name(GET_KEYSPACE_META_BY_ID_NAME)
                    .with_uri(get_keyspace_meta_by_id_url(keyspace_id))
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        temp_keyspace_meta.to_pb()
    }

    /* Affinity group interfaces */

    /// Go `CreateAffinityGroups`: creates one or more affinity groups with key
    /// ranges. The `affinity_groups` parameter is a map from group ID to a list
    /// of key ranges. Use options to customize behavior (for example, skipping
    /// existing groups).
    pub async fn create_affinity_groups(
        &self,
        affinity_groups: &HashMap<String, Vec<AffinityGroupKeyRange>>,
        opts: &[CreateAffinityGroupsOption],
    ) -> Result<HashMap<String, AffinityGroupState>> {
        let mut options = CreateAffinityGroupsOptions::default();
        for opt in opts {
            (opt.0)(&mut options);
        }

        let req_groups: HashMap<String, CreateAffinityGroupInput> = affinity_groups
            .iter()
            .map(|(group_id, ranges)| {
                (
                    group_id.clone(),
                    CreateAffinityGroupInput {
                        ranges: ranges.clone(),
                    },
                )
            })
            .collect();
        let req = CreateAffinityGroupsRequest {
            affinity_groups: req_groups,
        };
        let req_json = marshal(&SortedCreateAffinityGroupsRequest::new(&req))?;
        let uri = if options.skip_exist_check {
            format!("{AFFINITY_GROUPS}?skip_exist_check=true")
        } else {
            AFFINITY_GROUPS.to_owned()
        };
        let resp: AffinityGroupsResponse = self
            .request_resp(
                new_request_info()
                    .with_name("CreateAffinityGroups")
                    .with_uri(uri)
                    .with_method(Method::POST)
                    .with_body(req_json),
                &[],
            )
            .await?;
        Ok(resp.affinity_groups)
    }

    /// Go `GetAffinityGroup`: an affinity group by group ID.
    pub async fn get_affinity_group(&self, group_id: &str) -> Result<AffinityGroupState> {
        self.request_resp(
            new_request_info()
                .with_name("GetAffinityGroup")
                .with_uri(affinity_group_by_id(group_id))
                .with_method(Method::GET),
            &[],
        )
        .await
    }

    /// Go `GetAffinityGroups`: affinity groups by group IDs. When `group_ids`
    /// is empty, it returns all affinity groups.
    pub async fn get_affinity_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, AffinityGroupState>> {
        if group_ids.is_empty() {
            return self.get_all_affinity_groups().await;
        }

        let query = group_ids
            .iter()
            .map(|id| format!("ids={}", super::codec::query_escape(id.as_bytes())))
            .collect::<Vec<_>>()
            .join("&");
        let resp: AffinityGroupsResponse = self
            .request_resp(
                new_request_info()
                    .with_name("GetAffinityGroups")
                    .with_uri(format!("{AFFINITY_GROUPS}?{query}"))
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        Ok(resp.affinity_groups)
    }

    /// Go `GetAllAffinityGroups`: all affinity groups.
    pub async fn get_all_affinity_groups(&self) -> Result<HashMap<String, AffinityGroupState>> {
        let resp: AffinityGroupsResponse = self
            .request_resp(
                new_request_info()
                    .with_name("GetAllAffinityGroups")
                    .with_uri(AFFINITY_GROUPS)
                    .with_method(Method::GET),
                &[],
            )
            .await?;
        Ok(resp.affinity_groups)
    }

    /// Go `UpdateAffinityGroupPeers`: updates the leader and voter stores of an
    /// affinity group.
    pub async fn update_affinity_group_peers(
        &self,
        group_id: &str,
        leader_store_id: u64,
        voter_store_ids: &[u64],
    ) -> Result<AffinityGroupState> {
        let req = UpdateAffinityGroupPeersRequest {
            leader_store_id,
            voter_store_ids: voter_store_ids.to_vec(),
        };
        let req_json = marshal(&req)?;
        self.request_resp(
            new_request_info()
                .with_name("UpdateAffinityGroupPeers")
                .with_uri(affinity_group_by_id(group_id))
                .with_method(Method::PUT)
                .with_body(req_json),
            &[],
        )
        .await
    }

    /// Go `DeleteAffinityGroup`: deletes an affinity group by group ID.
    pub async fn delete_affinity_group(&self, group_id: &str, force: bool) -> Result<()> {
        let mut uri = affinity_group_by_id(group_id);
        if force {
            uri = format!("{uri}?force=true");
        }
        self.request_no_resp(
            new_request_info()
                .with_name("DeleteAffinityGroup")
                .with_uri(uri)
                .with_method(Method::DELETE),
            &[],
        )
        .await
    }

    /// Go `BatchDeleteAffinityGroups`: deletes multiple affinity groups in batch.
    pub async fn batch_delete_affinity_groups(
        &self,
        group_ids: &[String],
        force: bool,
    ) -> Result<()> {
        let req = BatchDeleteAffinityGroupsRequest {
            ids: group_ids.to_vec(),
            force,
        };
        let req_json = marshal(&req)?;
        // Use POST with ?delete query parameter for batch deletion
        let url = format!("{AFFINITY_GROUPS}?delete");
        self.request_no_resp(
            new_request_info()
                .with_name("BatchDeleteAffinityGroups")
                .with_uri(url)
                .with_method(Method::POST)
                .with_body(req_json),
            &[],
        )
        .await
    }

    /// Go `AddAffinityGroupKeyRanges`: adds key ranges to affinity groups.
    pub async fn add_affinity_group_key_ranges(
        &self,
        group_key_ranges: &HashMap<String, Vec<AffinityGroupKeyRange>>,
    ) -> Result<HashMap<String, AffinityGroupState>> {
        // Convert to the request format
        let add = group_ranges_modifications(group_key_ranges);
        let req = BatchModifyAffinityGroupsRequest {
            add,
            ..Default::default()
        };
        let req_json = marshal(&req)?;
        let resp: AffinityGroupsResponse = self
            .request_resp(
                new_request_info()
                    .with_name("AddAffinityGroupKeyRanges")
                    .with_uri(AFFINITY_GROUPS)
                    .with_method(Method::PATCH)
                    .with_body(req_json),
                &[],
            )
            .await?;
        Ok(resp.affinity_groups)
    }

    /// Go `RemoveAffinityGroupKeyRanges`: removes key ranges from affinity groups.
    pub async fn remove_affinity_group_key_ranges(
        &self,
        group_key_ranges: &HashMap<String, Vec<AffinityGroupKeyRange>>,
    ) -> Result<HashMap<String, AffinityGroupState>> {
        // Convert to the request format
        let remove = group_ranges_modifications(group_key_ranges);
        let req = BatchModifyAffinityGroupsRequest {
            remove,
            ..Default::default()
        };
        let req_json = marshal(&req)?;
        let resp: AffinityGroupsResponse = self
            .request_resp(
                new_request_info()
                    .with_name("RemoveAffinityGroupKeyRanges")
                    .with_uri(AFFINITY_GROUPS)
                    .with_method(Method::PATCH)
                    .with_body(req_json),
                &[],
            )
            .await?;
        Ok(resp.affinity_groups)
    }
}

// The `{"start_key", "end_key"}` map Go builds for accelerate-schedule, with
// its keys in Go's sorted order.
#[derive(Serialize)]
struct KeyRangeInput<'a> {
    end_key: &'a str,
    start_key: &'a str,
}

// `CreateAffinityGroupsRequest` with its map keys sorted, as Go encodes it.
#[derive(Serialize)]
struct SortedCreateAffinityGroupsRequest<'a> {
    #[serde(rename = "affinity_groups")]
    affinity_groups: BTreeMap<&'a String, &'a CreateAffinityGroupInput>,
}

impl<'a> SortedCreateAffinityGroupsRequest<'a> {
    fn new(req: &'a CreateAffinityGroupsRequest) -> Self {
        SortedCreateAffinityGroupsRequest {
            affinity_groups: sorted(&req.affinity_groups),
        }
    }
}

// Go builds one modification per map entry, in map order.
fn group_ranges_modifications(
    group_key_ranges: &HashMap<String, Vec<AffinityGroupKeyRange>>,
) -> Vec<GroupRangesModification> {
    group_key_ranges
        .iter()
        .map(|(group_id, ranges)| GroupRangesModification {
            id: group_id.clone(),
            ranges: ranges.clone(),
        })
        .collect()
}
