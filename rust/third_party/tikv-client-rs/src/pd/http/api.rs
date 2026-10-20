// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `api.go`: the paths of the PD HTTP APIs and their parameterized builders.
//! Constants keep Go's values byte for byte, including the `%s`/`%d` verbs of
//! the keyspace and affinity templates; the builders substitute them.

use std::time::Duration;

use super::codec::query_escape;
use super::types::KeyRange;

// Metadata
pub const HOT_READ: &str = "/pd/api/v1/hotspot/regions/read";
pub const HOT_WRITE: &str = "/pd/api/v1/hotspot/regions/write";
pub const HOT_HISTORY: &str = "/pd/api/v1/hotspot/regions/history";
pub const REGION_BY_ID_PREFIX: &str = "/pd/api/v1/region/id";
pub(super) const REGION_BY_KEY: &str = "/pd/api/v1/region/key";
pub const REGIONS: &str = "/pd/api/v1/regions";
pub(super) const REGIONS_BY_KEY: &str = "/pd/api/v1/regions/key";
pub const REGIONS_BY_STORE_ID_PREFIX: &str = "/pd/api/v1/regions/store";
pub(super) const REGIONS_REPLICATED: &str = "/pd/api/v1/regions/replicated";
pub(super) const REGIONS_SIBLINGS: &str = "/pd/api/v1/regions/sibling";
pub const EMPTY_REGIONS: &str = "/pd/api/v1/regions/check/empty-region";
pub const ACCELERATE_SCHEDULE: &str = "/pd/api/v1/regions/accelerate-schedule";
pub const ACCELERATE_SCHEDULE_IN_BATCH: &str = "/pd/api/v1/regions/accelerate-schedule/batch";
pub(super) const STORE: &str = "/pd/api/v1/store";
pub const STORES: &str = "/pd/api/v1/stores";
pub const STATS_REGION: &str = "/pd/api/v1/stats/region";
pub(super) const MEMBERS_PREFIX: &str = "/pd/api/v1/members";
pub(super) const LEADER_PREFIX: &str = "/pd/api/v1/leader";
pub(super) const TRANSFER_LEADER: &str = "/pd/api/v1/leader/transfer";
pub(super) const HEALTH: &str = "/pd/api/v1/health";
// Config
pub const CONFIG: &str = "/pd/api/v1/config";
pub const CLUSTER_VERSION: &str = "/pd/api/v1/config/cluster-version";
pub const SCHEDULE_CONFIG: &str = "/pd/api/v1/config/schedule";
pub const REPLICATE_CONFIG: &str = "/pd/api/v1/config/replicate";
// Rule
pub const PLACEMENT_RULE: &str = "/pd/api/v1/config/rule";
pub const PLACEMENT_RULES: &str = "/pd/api/v1/config/rules";
pub const PLACEMENT_RULES_IN_BATCH: &str = "/pd/api/v1/config/rules/batch";
pub(super) const PLACEMENT_RULES_BY_GROUP: &str = "/pd/api/v1/config/rules/group";
pub const PLACEMENT_RULE_BUNDLE: &str = "/pd/api/v1/config/placement-rule";
pub(super) const PLACEMENT_RULE_GROUP: &str = "/pd/api/v1/config/rule_group";
pub(super) const PLACEMENT_RULE_GROUPS: &str = "/pd/api/v1/config/rule_groups";
pub const REGION_LABEL_RULE: &str = "/pd/api/v1/config/region-label/rule";
pub const REGION_LABEL_RULES: &str = "/pd/api/v1/config/region-label/rules";
pub const REGION_LABEL_RULES_BY_IDS: &str = "/pd/api/v1/config/region-label/rules/ids";
// Scheduler
pub const SCHEDULERS: &str = "/pd/api/v1/schedulers";
pub const SCHEDULER_CONFIG: &str = "/pd/api/v1/scheduler-config";
pub(super) const SCATTER_RANGE_SCHEDULER: &str = "/pd/api/v1/schedulers/scatter-range-scheduler-";
// Admin
pub const RESET_TS: &str = "/pd/api/v1/admin/reset-ts";
pub const BASE_ALLOC_ID: &str = "/pd/api/v1/admin/base-alloc-id";
pub const SNAPSHOT_RECOVERING_MARK: &str = "/pd/api/v1/admin/cluster/markers/snapshot-recovering";
pub const PITR_RESTORE_MODE_MARK: &str = "/pd/api/v1/admin/cluster/markers/pitr-restore-mode";
// Debug
pub const PPROF_PROFILE: &str = "/pd/api/v1/debug/pprof/profile";
pub const PPROF_HEAP: &str = "/pd/api/v1/debug/pprof/heap";
pub const PPROF_MUTEX: &str = "/pd/api/v1/debug/pprof/mutex";
pub const PPROF_ALLOCS: &str = "/pd/api/v1/debug/pprof/allocs";
pub const PPROF_BLOCK: &str = "/pd/api/v1/debug/pprof/block";
pub const PPROF_GOROUTINE: &str = "/pd/api/v1/debug/pprof/goroutine";
// Others
pub const MIN_RESOLVED_TS_PREFIX: &str = "/pd/api/v1/min-resolved-ts";
pub const CLUSTER: &str = "/pd/api/v1/cluster";
pub const CLUSTER_STATUS: &str = "/pd/api/v1/cluster/status";
pub const STATUS: &str = "/pd/api/v1/status";
pub const VERSION: &str = "/pd/api/v1/version";
pub(super) const OPERATORS: &str = "/pd/api/v1/operators";
pub(super) const SAFEPOINT: &str = "/pd/api/v1/gc/safepoint";
// Microservice
pub(super) const MICROSERVICE_PREFIX: &str = "/pd/api/v2/ms";
// Keyspace
pub const KEYSPACE_CONFIG: &str = "/pd/api/v2/keyspaces/%s/config";
pub const GET_KEYSPACE_META_BY_NAME: &str = "/pd/api/v2/keyspaces/%s";
pub const GET_KEYSPACE_META_BY_ID: &str = "/pd/api/v2/keyspaces/id/%d";
// Affinity
pub const AFFINITY_GROUPS: &str = "/pd/api/v2/affinity-groups";
pub const AFFINITY_GROUP_BY_ID: &str = "/pd/api/v2/affinity-groups/%s";

/// Go `RegionByID`: the path of PD HTTP API to get region by ID.
pub fn region_by_id(region_id: u64) -> String {
    format!("{REGION_BY_ID_PREFIX}/{region_id}")
}

/// Go `RegionByKey`: the path of PD HTTP API to get region by key.
pub fn region_by_key(key: &[u8]) -> String {
    format!("{REGION_BY_KEY}/{}", query_escape(key))
}

/// Go `RegionsByKeyRange`: the path of PD HTTP API to scan regions with given
/// start key, end key and limit parameters.
pub fn regions_by_key_range(key_range: &KeyRange, limit: i64) -> String {
    let (start_key_str, end_key_str) = key_range.escape_as_utf8_str();
    format!("{REGIONS_BY_KEY}?key={start_key_str}&end_key={end_key_str}&limit={limit}")
}

/// Go `RegionsByStoreID`: the path of PD HTTP API to get regions by store ID.
pub fn regions_by_store_id(store_id: u64) -> String {
    format!("{REGIONS_BY_STORE_ID_PREFIX}/{store_id}")
}

/// Go `RegionsReplicatedByKeyRange`: the path of PD HTTP API to get replicated
/// regions with given start key and end key.
pub fn regions_replicated_by_key_range(key_range: &KeyRange) -> String {
    let (start_key_str, end_key_str) = key_range.escape_as_hex_str();
    format!("{REGIONS_REPLICATED}?startKey={start_key_str}&endKey={end_key_str}")
}

/// Go `RegionSiblingsByID`: the path of PD HTTP API to get sibling regions by ID.
pub fn region_siblings_by_id(region_id: u64) -> String {
    format!("{REGIONS_SIBLINGS}/{region_id}")
}

/// Go `RegionStatsByKeyRange`: the path of PD HTTP API to get region stats by
/// start key and end key.
pub fn region_stats_by_key_range(key_range: &KeyRange, only_count: bool) -> String {
    let (start_key_str, end_key_str) = key_range.escape_as_utf8_str();
    if only_count {
        return format!("{STATS_REGION}?start_key={start_key_str}&end_key={end_key_str}&count");
    }
    format!("{STATS_REGION}?start_key={start_key_str}&end_key={end_key_str}")
}

/// Go `RegionDistributionsByKeyRange`: the path of PD HTTP API to get region
/// distribution by start key and end key.
pub fn region_distributions_by_key_range(key_range: &KeyRange, engine: &str) -> String {
    let (start_key_str, end_key_str) = key_range.escape_as_utf8_str();
    format!(
        "{STATS_REGION}?use_hot&start_key={start_key_str}&end_key={end_key_str}&engine={engine}"
    )
}

/// Go `StoreByID`: the store API with store ID parameter.
pub fn store_by_id(id: u64) -> String {
    format!("{STORE}/{id}")
}

/// Go `StoreLabelByID`: the store label API with store ID parameter.
pub fn store_label_by_id(id: u64) -> String {
    format!("{STORE}/{id}/label")
}

/// Go `LabelByStoreID`: the path of PD HTTP API to set store label.
pub fn label_by_store_id(store_id: i64) -> String {
    format!("{STORE}/{store_id}/label")
}

/// Go `TransferLeaderByID`: the path of PD HTTP API to transfer leader by ID.
pub fn transfer_leader_by_id(leader_id: &str) -> String {
    format!("{TRANSFER_LEADER}/{leader_id}")
}

/// Go `ConfigWithTTLSeconds`: the config API with the TTL seconds parameter
/// (`%.0f`; Rust's precision formatting also rounds exact ties to even).
pub fn config_with_ttl_seconds(ttl_seconds: f64) -> String {
    format!("{CONFIG}?ttlSecond={ttl_seconds:.0}")
}

/// Go `PlacementRulesByGroup`: the path of PD HTTP API to get placement rules by group.
pub fn placement_rules_by_group(group: &str) -> String {
    format!("{PLACEMENT_RULES_BY_GROUP}/{group}")
}

/// Go `PlacementRuleByGroupAndID`: the path of PD HTTP API to get placement
/// rule by group and ID.
pub fn placement_rule_by_group_and_id(group: &str, id: &str) -> String {
    format!("{PLACEMENT_RULE}/{group}/{id}")
}

/// Go `PlacementRuleBundleByGroup`: the path of PD HTTP API to get placement
/// rule bundle by group.
pub fn placement_rule_bundle_by_group(group: &str) -> String {
    format!("{PLACEMENT_RULE_BUNDLE}/{group}")
}

/// Go `PlacementRuleBundleWithPartialParameter`: the path of PD HTTP API to get
/// placement rule bundle with partial parameter.
pub fn placement_rule_bundle_with_partial_parameter(partial: bool) -> String {
    format!("{PLACEMENT_RULE_BUNDLE}?partial={partial}")
}

/// Go `PlacementRuleGroupByID`: the path of PD HTTP API to get placement rule
/// group by ID.
pub fn placement_rule_group_by_id(id: &str) -> String {
    format!("{PLACEMENT_RULE_GROUP}/{id}")
}

/// Go `GetSchedulerConfigURIByName`: the path of PD HTTP API to get
/// configuration of the given scheduler (`path.Join`, so the name is cleaned).
pub fn get_scheduler_config_uri_by_name(name: &str) -> String {
    path_join(&[SCHEDULER_CONFIG, name, "list"])
}

/// Go `GetCancelSchedulerJobURIByNameAndJobID`: the path of PD HTTP API to
/// cancel the job of the given scheduler.
pub fn get_cancel_scheduler_job_uri_by_name_and_job_id(name: &str, job_id: u64) -> String {
    format!("{SCHEDULER_CONFIG}/{name}/job?job-id={job_id}")
}

/// Go `SchedulerByName`: the scheduler API with the given scheduler name.
pub fn scheduler_by_name(name: &str) -> String {
    format!("{SCHEDULERS}/{name}")
}

/// Go `ScatterRangeSchedulerWithName`: the scatter range scheduler API with
/// name parameter.
pub fn scatter_range_scheduler_with_name(name: &str) -> String {
    format!("{SCATTER_RANGE_SCHEDULER}{name}")
}

/// Go `PProfProfileAPIWithInterval`: the pprof profile API with interval
/// parameter, in whole seconds.
pub fn pprof_profile_api_with_interval(interval: Duration) -> String {
    format!("{PPROF_PROFILE}?seconds={}", interval.as_secs())
}

/// Go `PProfGoroutineWithDebugLevel`: the pprof goroutine API with debug level
/// parameter.
pub fn pprof_goroutine_with_debug_level(level: i64) -> String {
    format!("{PPROF_GOROUTINE}?debug={level}")
}

/// Go `MicroserviceMembers`: the path of PD HTTP API to get the members of
/// microservice.
pub fn microservice_members(service: &str) -> String {
    format!("{MICROSERVICE_PREFIX}/members/{service}")
}

/// Go `MicroservicePrimary`: the path of PD HTTP API to get the primary of
/// microservice.
pub fn microservice_primary(service: &str) -> String {
    format!("{MICROSERVICE_PREFIX}/primary/{service}")
}

/// Go `GetUpdateKeyspaceConfigURL`: the path of PD HTTP API to update keyspace config.
pub fn get_update_keyspace_config_url(keyspace_name: &str) -> String {
    KEYSPACE_CONFIG.replacen("%s", keyspace_name, 1)
}

/// Go `GetKeyspaceMetaByNameURL`: the path of PD HTTP API to get keyspace meta
/// by keyspace name.
pub fn get_keyspace_meta_by_name_url(keyspace_name: &str) -> String {
    GET_KEYSPACE_META_BY_NAME.replacen("%s", keyspace_name, 1)
}

/// Go `GetKeyspaceMetaByIDURL`: the path of PD HTTP API to get keyspace meta
/// by keyspace id.
pub fn get_keyspace_meta_by_id_url(id: u32) -> String {
    GET_KEYSPACE_META_BY_ID.replacen("%d", &id.to_string(), 1)
}

/// Go `GetDeleteSafePointURI`: the URI for delete safepoint service.
pub fn get_delete_safe_point_uri(service_id: &str) -> String {
    format!("{SAFEPOINT}/{service_id}")
}

/// `fmt.Sprintf(AffinityGroupByID, groupID)`, written inline in Go's client.
pub(super) fn affinity_group_by_id(group_id: &str) -> String {
    AFFINITY_GROUP_BY_ID.replacen("%s", group_id, 1)
}

// Go `path.Join`: join the non-empty elements with '/', then `path.Clean`.
fn path_join(elems: &[&str]) -> String {
    let joined = elems
        .iter()
        .copied()
        .filter(|elem| !elem.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if joined.is_empty() {
        return String::new();
    }
    path_clean(&joined)
}

// Go `path.Clean`.
fn path_clean(path: &str) -> String {
    let rooted = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|last| *last != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            part => out.push(part),
        }
    }
    let body = out.join("/");
    if rooted {
        format!("/{body}")
    } else if body.is_empty() {
        ".".to_owned()
    } else {
        body
    }
}
