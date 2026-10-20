// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `types.go`: the JSON shapes of the PD HTTP APIs. Field names are the
//! snake-cased Go names; JSON keys are Go's `json` tags. Every struct decodes
//! with container-level `default`, as Go's decoder leaves absent fields at
//! their zero value, and `omitempty` tags skip the zero value on encode.
//! Go `int` is `i64`.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, FixedOffset, TimeZone, Timelike};
use serde_derive::{Deserialize, Serialize};

use super::codec::{hex_encode, key_hex_str_to_raw_key, query_escape, raw_key_to_key_hex_str};
use crate::proto::encryptionpb;
use crate::proto::keyspacepb;
use crate::proto::pdpb;
use crate::Error;

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Go `time.Time` as `encoding/json` carries it: RFC 3339 text with
/// nanosecond precision. The default is Go's zero time,
/// `0001-01-01T00:00:00Z`, and a JSON `null` decodes to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Time(pub DateTime<FixedOffset>);

fn go_zero_time() -> DateTime<FixedOffset> {
    FixedOffset::east_opt(0)
        .expect("zero offset")
        .with_ymd_and_hms(1, 1, 1, 0, 0, 0)
        .single()
        .expect("Go zero time")
}

impl Default for Time {
    fn default() -> Self {
        Time(go_zero_time())
    }
}

impl Time {
    /// Go `Time.IsZero`.
    pub fn is_zero(&self) -> bool {
        self.0 == go_zero_time()
    }

    /// Go `time.RFC3339Nano`: trailing fractional zeros are trimmed and a zero
    /// offset is written as `Z`.
    pub fn format_rfc3339_nano(&self) -> String {
        let time = self.0;
        let mut out = time.format("%Y-%m-%dT%H:%M:%S").to_string();
        let nanos = time.nanosecond() % 1_000_000_000;
        if nanos != 0 {
            let fraction = format!("{nanos:09}");
            out.push('.');
            out.push_str(fraction.trim_end_matches('0'));
        }
        let offset = time.offset().local_minus_utc();
        if offset == 0 {
            out.push('Z');
        } else {
            let sign = if offset < 0 { '-' } else { '+' };
            let offset = offset.abs();
            out.push_str(&format!(
                "{sign}{:02}:{:02}",
                offset / 3600,
                (offset % 3600) / 60
            ));
        }
        out
    }
}

impl serde::Serialize for Time {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.format_rfc3339_nano())
    }
}

impl<'de> serde::Deserialize<'de> for Time {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match <Option<String> as serde::Deserialize>::deserialize(deserializer)? {
            None => Ok(Time::default()),
            Some(text) => DateTime::parse_from_rfc3339(&text)
                .map(Time)
                .map_err(serde::de::Error::custom),
        }
    }
}

macro_rules! go_string_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Cow<'static, str>);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl From<&'static str> for $name {
            fn from(value: &'static str) -> Self {
                $name(Cow::Borrowed(value))
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                $name(Cow::Owned(value))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

/// Go `ServiceSafePoint`: the safepoint for a specific service.
/// NOTE: This type is in sync with pd/pkg/storage/endpoint/gc_safe_point.go
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceSafePoint {
    #[serde(rename = "service_id")]
    pub service_id: String,
    #[serde(rename = "expired_at")]
    pub expired_at: i64,
    #[serde(rename = "safe_point")]
    pub safe_point: u64,
}

/// Go `ListServiceGCSafepoint`: the response for list service GC safepoint.
/// NOTE: This type is in sync with pd/server/api/service_gc_safepoint.go
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ListServiceGCSafepoint {
    #[serde(rename = "service_gc_safe_points")]
    pub service_gc_safepoints: Vec<ServiceSafePoint>,
    #[serde(
        rename = "min_service_gc_safe_point",
        skip_serializing_if = "is_zero_u64"
    )]
    pub min_service_gc_safepoint: u64,
    #[serde(rename = "gc_safe_point")]
    pub gc_safe_point: u64,
}

/// Go `ClusterState`: some cluster state information.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterState {
    // Go `omitempty` has no effect on a struct-typed field.
    #[serde(rename = "raft_bootstrap_time")]
    pub raft_bootstrap_time: Time,
    #[serde(rename = "is_initialized")]
    pub is_initialized: bool,
    #[serde(rename = "replication_status")]
    pub replication_status: String,
}

/// Go `State`: the status of PD server.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct State {
    #[serde(rename = "build_ts")]
    pub build_ts: String,
    #[serde(rename = "version")]
    pub version: String,
    #[serde(rename = "git_hash")]
    pub git_hash: String,
    #[serde(rename = "start_timestamp")]
    pub start_timestamp: i64,
    #[serde(rename = "kernel_type")]
    pub kernel_type: String,
}

/// Go `KeyRange` (an alias of PD's `router.KeyRange`): a range of keys in bytes.
///
/// The actual encoding of the key range is not specified here. It should be
/// either UTF-8 or hex:
///   - UTF-8 means the key has already been encoded into a string with UTF-8
///     encoding, and is converted to the URL with
///     [`escape_as_utf8_str`](Self::escape_as_utf8_str).
///   - Hex means the key is just raw bytes without encoding to a UTF-8
///     string, and is converted with [`escape_as_hex_str`](Self::escape_as_hex_str).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct KeyRange {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl KeyRange {
    /// Go `NewKeyRange`.
    pub fn new(start_key: impl Into<Vec<u8>>, end_key: impl Into<Vec<u8>>) -> KeyRange {
        KeyRange {
            start_key: start_key.into(),
            end_key: end_key.into(),
        }
    }

    /// Go `EscapeAsUTF8Str`: the URL escaped key strings as they are UTF-8 encoded.
    pub fn escape_as_utf8_str(&self) -> (String, String) {
        (query_escape(&self.start_key), query_escape(&self.end_key))
    }

    /// Go `EscapeAsHexStr`: the URL escaped key strings as they are hex encoded.
    pub fn escape_as_hex_str(&self) -> (String, String) {
        (
            query_escape(hex_encode(&self.start_key).as_bytes()),
            query_escape(hex_encode(&self.end_key).as_bytes()),
        )
    }
}

/// Go `NewKeyRange`: a new key range with the given start key and end key bytes.
pub fn new_key_range(start_key: impl Into<Vec<u8>>, end_key: impl Into<Vec<u8>>) -> KeyRange {
    KeyRange::new(start_key, end_key)
}

// NOTICE: the structures below are copied from the PD API definitions.
// Please make sure the consistency if any change happens to the PD API.

/// Go `RegionInfo`: the information of one region.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionInfo {
    #[serde(rename = "id")]
    pub id: i64,
    /// hex-encoded
    #[serde(rename = "start_key")]
    pub start_key: String,
    /// hex-encoded
    #[serde(rename = "end_key")]
    pub end_key: String,
    #[serde(rename = "epoch")]
    pub epoch: RegionEpoch,
    #[serde(rename = "peers")]
    pub peers: Vec<RegionPeer>,
    #[serde(rename = "leader")]
    pub leader: RegionPeer,
    #[serde(rename = "down_peers")]
    pub down_peers: Vec<RegionPeerStat>,
    #[serde(rename = "pending_peers")]
    pub pending_peers: Vec<RegionPeer>,
    #[serde(rename = "written_bytes")]
    pub written_bytes: u64,
    #[serde(rename = "read_bytes")]
    pub read_bytes: u64,
    #[serde(rename = "written_keys")]
    pub written_keys: u64,
    #[serde(rename = "read_keys")]
    pub read_keys: u64,
    #[serde(rename = "approximate_size")]
    pub approximate_size: i64,
    #[serde(rename = "approximate_kv_size")]
    pub approximate_kv_size: i64,
    #[serde(rename = "approximate_keys")]
    pub approximate_keys: i64,

    #[serde(rename = "replication_status", skip_serializing_if = "Option::is_none")]
    pub replication_status: Option<ReplicationStatus>,
}

impl RegionInfo {
    /// Go `GetStartKey`.
    pub fn get_start_key(&self) -> &str {
        &self.start_key
    }

    /// Go `GetEndKey`.
    pub fn get_end_key(&self) -> &str {
        &self.end_key
    }
}

/// Go `RegionEpoch`: the information about its epoch.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionEpoch {
    #[serde(rename = "conf_ver")]
    pub conf_ver: i64,
    #[serde(rename = "version")]
    pub version: i64,
}

/// Go `RegionPeer`: information of one peer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionPeer {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "store_id")]
    pub store_id: i64,
    #[serde(rename = "is_learner")]
    pub is_learner: bool,
}

/// Go `RegionPeerStat`: one field `DownSec` which indicates how long it's down
/// than `RegionPeer`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionPeerStat {
    #[serde(rename = "peer")]
    pub peer: RegionPeer,
    #[serde(rename = "down_seconds")]
    pub down_sec: i64,
}

/// Go `ReplicationStatus`: the replication mode status of the region.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationStatus {
    #[serde(rename = "state")]
    pub state: String,
    #[serde(rename = "state_id")]
    pub state_id: i64,
}

/// Go `RegionsInfo`: the information of regions.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionsInfo {
    #[serde(rename = "count")]
    pub count: i64,
    #[serde(rename = "regions")]
    pub regions: Vec<RegionInfo>,
}

pub(super) fn new_regions_info(count: i64) -> RegionsInfo {
    RegionsInfo {
        count,
        regions: Vec::with_capacity(count.max(0) as usize),
    }
}

/// Go `(*RegionsInfo).Merge`, including its nil receiver and nil argument.
pub(super) fn merge_regions_info(
    ri: Option<&RegionsInfo>,
    other: Option<&RegionsInfo>,
) -> RegionsInfo {
    let empty = new_regions_info(0);
    let ri = ri.unwrap_or(&empty);
    let other = other.unwrap_or(&empty);
    let mut merged = new_regions_info(ri.count + other.count);
    let mut m: HashMap<i64, RegionInfo> =
        HashMap::with_capacity((ri.count + other.count).max(0) as usize);
    for region in &ri.regions {
        m.insert(region.id, region.clone());
    }
    for region in &other.regions {
        m.insert(region.id, region.clone());
    }
    merged.regions.extend(m.into_values());
    merged.count = merged.regions.len() as i64;
    merged
}

impl RegionsInfo {
    /// Go `Merge`: merges two RegionsInfo together and returns a new one. A
    /// region present in both keeps `other`'s copy; the order is unspecified,
    /// as Go's map iteration is.
    pub fn merge(&self, other: Option<&RegionsInfo>) -> RegionsInfo {
        merge_regions_info(Some(self), other)
    }
}

/// Go `StoreHotPeersInfos`: used to get human-readable description for hot regions.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StoreHotPeersInfos {
    #[serde(rename = "as_peer")]
    pub as_peer: StoreHotPeersStat,
    #[serde(rename = "as_leader")]
    pub as_leader: StoreHotPeersStat,
}

/// Go `StoreHotPeersStat`: the hot region statistics group by store.
pub type StoreHotPeersStat = HashMap<u64, HotPeersStat>;

/// Go `HotPeersStat`: all hot regions statistics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HotPeersStat {
    #[serde(rename = "store_bytes")]
    pub store_byte_rate: f64,
    #[serde(rename = "store_keys")]
    pub store_key_rate: f64,
    #[serde(rename = "store_query")]
    pub store_query_rate: f64,
    #[serde(rename = "store_cpu")]
    pub store_cpu_rate: f64,
    #[serde(rename = "total_flow_bytes")]
    pub total_bytes_rate: f64,
    #[serde(rename = "total_flow_keys")]
    pub total_keys_rate: f64,
    #[serde(rename = "total_flow_query")]
    pub total_query_rate: f64,
    #[serde(rename = "total_flow_cpu")]
    pub total_cpu_rate: f64,
    #[serde(rename = "regions_count")]
    pub count: i64,
    #[serde(rename = "statistics")]
    pub stats: Vec<HotPeerStatShow>,
}

/// Go `HotPeerStatShow`: the hot region statistics for output.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HotPeerStatShow {
    #[serde(rename = "store_id")]
    pub store_id: u64,
    #[serde(rename = "stores")]
    pub stores: Vec<u64>,
    #[serde(rename = "is_leader")]
    pub is_leader: bool,
    #[serde(rename = "is_learner")]
    pub is_learner: bool,
    #[serde(rename = "region_id")]
    pub region_id: u64,
    #[serde(rename = "hot_degree")]
    pub hot_degree: i64,
    #[serde(rename = "flow_bytes")]
    pub byte_rate: f64,
    #[serde(rename = "flow_keys")]
    pub key_rate: f64,
    #[serde(rename = "flow_query")]
    pub query_rate: f64,
    #[serde(rename = "flow_cpu")]
    pub cpu_rate: f64,
    #[serde(rename = "anti_count")]
    pub anti_count: i64,
    // Go `omitempty` has no effect on a struct-typed field.
    #[serde(rename = "last_update_time")]
    pub last_update_time: Time,
}

/// Go `HistoryHotRegionsRequest`: wrap the request conditions.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryHotRegionsRequest {
    #[serde(rename = "start_time", skip_serializing_if = "is_zero_i64")]
    pub start_time: i64,
    #[serde(rename = "end_time", skip_serializing_if = "is_zero_i64")]
    pub end_time: i64,
    #[serde(rename = "region_ids", skip_serializing_if = "Vec::is_empty")]
    pub region_ids: Vec<u64>,
    #[serde(rename = "store_ids", skip_serializing_if = "Vec::is_empty")]
    pub store_ids: Vec<u64>,
    #[serde(rename = "peer_ids", skip_serializing_if = "Vec::is_empty")]
    pub peer_ids: Vec<u64>,
    #[serde(rename = "is_learners", skip_serializing_if = "Vec::is_empty")]
    pub is_learners: Vec<bool>,
    #[serde(rename = "is_leaders", skip_serializing_if = "Vec::is_empty")]
    pub is_leaders: Vec<bool>,
    #[serde(rename = "hot_region_type", skip_serializing_if = "Vec::is_empty")]
    pub hot_region_types: Vec<String>,
}

/// Go `RegionDistributions`: wraps region distribution info.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionDistributions {
    #[serde(rename = "region_distribution")]
    pub region_distributions: Vec<RegionDistribution>,
}

/// Go `RegionDistribution`: wraps region distribution info; it is the storage
/// format of region_distribution_storage.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionDistribution {
    #[serde(rename = "store_id")]
    pub store_id: u64,
    #[serde(rename = "engine_type")]
    pub engine_type: String,
    #[serde(rename = "region_leader_count")]
    pub region_leader_count: i64,
    #[serde(rename = "region_peer_count")]
    pub region_peer_count: i64,
    #[serde(rename = "approximate_size")]
    pub approximate_size: i64,
    #[serde(rename = "approximate_keys")]
    pub approximate_keys: i64,
    // write
    #[serde(rename = "region_write_bytes")]
    pub region_write_bytes: u64,
    #[serde(rename = "region_write_keys")]
    pub region_write_keys: u64,
    #[serde(rename = "region_write_query")]
    pub region_write_query: u64,
    // leader read
    #[serde(rename = "region_leader_read_bytes")]
    pub region_leader_read_bytes: u64,
    #[serde(rename = "region_leader_read_keys")]
    pub region_leader_read_keys: u64,
    #[serde(rename = "region_leader_read_query")]
    pub region_leader_read_query: u64,
    // peer read
    #[serde(rename = "region_peer_read_bytes")]
    pub region_peer_read_bytes: u64,
    #[serde(rename = "region_peer_read_keys")]
    pub region_peer_read_keys: u64,
    #[serde(rename = "region_peer_read_query")]
    pub region_peer_read_query: u64,
}

/// Go `HistoryHotRegions`: wraps historyHotRegion.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryHotRegions {
    #[serde(rename = "history_hot_region")]
    pub history_hot_region: Vec<HistoryHotRegion>,
}

/// Go `HistoryHotRegion`: wraps hot region info; it is the storage format of
/// hot_region_storage.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryHotRegion {
    #[serde(rename = "update_time")]
    pub update_time: i64,
    #[serde(rename = "region_id")]
    pub region_id: u64,
    #[serde(rename = "peer_id")]
    pub peer_id: u64,
    #[serde(rename = "store_id")]
    pub store_id: u64,
    #[serde(rename = "is_leader")]
    pub is_leader: bool,
    #[serde(rename = "is_learner")]
    pub is_learner: bool,
    #[serde(rename = "hot_region_type")]
    pub hot_region_type: String,
    #[serde(rename = "hot_degree")]
    pub hot_degree: i64,
    #[serde(rename = "flow_bytes")]
    pub flow_bytes: f64,
    #[serde(rename = "flow_cpu")]
    pub flow_cpu: f64,
    #[serde(rename = "key_rate")]
    pub key_rate: f64,
    #[serde(rename = "query_rate")]
    pub query_rate: f64,
    #[serde(rename = "start_key")]
    pub start_key: String,
    #[serde(rename = "end_key")]
    pub end_key: String,
    /// Encryption metadata for start_key and end_key. encryption_meta.iv is IV
    /// for start_key. IV for end_key is calculated from
    /// (encryption_meta.iv + len(start_key)). The field is only used by PD and
    /// should be ignored otherwise. If encryption_meta is empty (i.e. None), it
    /// means start_key and end_key are unencrypted.
    #[serde(
        rename = "encryption_meta",
        skip_serializing_if = "Option::is_none",
        with = "pb_json::opt_encryption_meta"
    )]
    pub encryption_meta: Option<encryptionpb::EncryptionMeta>,
}

/// Go `StoresInfo`: the information of all TiKV/TiFlash stores.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StoresInfo {
    #[serde(rename = "count")]
    pub count: i64,
    #[serde(rename = "stores")]
    pub stores: Vec<StoreInfo>,
}

/// Go `StoreInfo`: the information of one TiKV/TiFlash store.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StoreInfo {
    #[serde(rename = "store")]
    pub store: MetaStore,
    #[serde(rename = "status")]
    pub status: StoreStatus,
}

/// Go `MetaStore`: the meta information of one store.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetaStore {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "address")]
    pub address: String,
    #[serde(rename = "state")]
    pub state: i64,
    #[serde(rename = "state_name")]
    pub state_name: String,
    #[serde(rename = "version")]
    pub version: String,
    #[serde(rename = "labels")]
    pub labels: Vec<StoreLabel>,
    #[serde(rename = "status_address")]
    pub status_address: String,
    #[serde(rename = "git_hash")]
    pub git_hash: String,
    #[serde(rename = "start_timestamp")]
    pub start_timestamp: i64,
}

/// Go `StoreLabel`: the information of one store label.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StoreLabel {
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "value")]
    pub value: String,
}

/// Go `StoreStatus`: the detail information of one store.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StoreStatus {
    #[serde(rename = "capacity")]
    pub capacity: String,
    #[serde(rename = "available")]
    pub available: String,
    #[serde(rename = "leader_count")]
    pub leader_count: i64,
    #[serde(rename = "leader_weight")]
    pub leader_weight: f64,
    #[serde(rename = "leader_score")]
    pub leader_score: f64,
    #[serde(rename = "leader_size")]
    pub leader_size: i64,
    #[serde(rename = "region_count")]
    pub region_count: i64,
    #[serde(rename = "region_weight")]
    pub region_weight: f64,
    #[serde(rename = "region_score")]
    pub region_score: f64,
    #[serde(rename = "region_size")]
    pub region_size: i64,
    #[serde(rename = "start_ts")]
    pub start_ts: Time,
    #[serde(rename = "last_heartbeat_ts")]
    pub last_heartbeat_ts: Time,
    #[serde(rename = "uptime")]
    pub uptime: String,
}

/// Go `RegionStats`: the statistics of regions.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionStats {
    #[serde(rename = "count")]
    pub count: i64,
    #[serde(rename = "empty_count")]
    pub empty_count: i64,
    #[serde(rename = "storage_size")]
    pub storage_size: i64,
    #[serde(rename = "storage_keys")]
    pub storage_keys: i64,
    #[serde(rename = "store_leader_count")]
    pub store_leader_count: HashMap<u64, i64>,
    #[serde(rename = "store_peer_count")]
    pub store_peer_count: HashMap<u64, i64>,
    #[serde(rename = "store_peer_size", skip_serializing_if = "HashMap::is_empty")]
    pub store_peer_size: HashMap<u64, i64>,
    #[serde(rename = "store_peer_keys", skip_serializing_if = "HashMap::is_empty")]
    pub store_peer_keys: HashMap<u64, i64>,
    #[serde(
        rename = "store_write_bytes",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub store_write_bytes: HashMap<u64, u64>,
    #[serde(rename = "store_write_keys", skip_serializing_if = "HashMap::is_empty")]
    pub store_write_keys: HashMap<u64, u64>,
    #[serde(
        rename = "store_write_query",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub store_write_query: HashMap<u64, u64>,
    #[serde(
        rename = "store_leader_read_bytes",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub store_leader_read_bytes: HashMap<u64, u64>,
    #[serde(
        rename = "store_leader_read_keys",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub store_leader_read_keys: HashMap<u64, u64>,
    #[serde(
        rename = "store_leader_read_query",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub store_leader_read_query: HashMap<u64, u64>,
    #[serde(
        rename = "store_peer_read_bytes",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub store_peer_read_bytes: HashMap<u64, u64>,
    #[serde(
        rename = "store_peer_read_keys",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub store_peer_read_keys: HashMap<u64, u64>,
    #[serde(
        rename = "store_peer_read_query",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub store_peer_read_query: HashMap<u64, u64>,
    #[serde(rename = "store_engine", skip_serializing_if = "HashMap::is_empty")]
    pub store_engine: HashMap<u64, String>,
}

go_string_type! {
    /// Go `PeerRoleType`: the expected peer type of the placement rule.
    PeerRoleType
}

/// Go `Voter`: can either match a leader peer or follower peer.
pub const VOTER: PeerRoleType = PeerRoleType(Cow::Borrowed("voter"));
/// Go `Leader`: matches a leader.
pub const LEADER: PeerRoleType = PeerRoleType(Cow::Borrowed("leader"));
/// Go `Follower`: matches a follower.
pub const FOLLOWER: PeerRoleType = PeerRoleType(Cow::Borrowed("follower"));
/// Go `Learner`: matches a learner.
pub const LEARNER: PeerRoleType = PeerRoleType(Cow::Borrowed("learner"));

/// Go `LabelConstraint`: used to filter store when trying to place peer of a region.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LabelConstraint {
    #[serde(rename = "key", skip_serializing_if = "String::is_empty")]
    pub key: String,
    #[serde(rename = "op", skip_serializing_if = "LabelConstraintOp::is_empty")]
    pub op: LabelConstraintOp,
    #[serde(rename = "values", skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
}

go_string_type! {
    /// Go `LabelConstraintOp`: how a LabelConstraint matches a store. It can be
    /// one of 'in', 'notIn', 'exists', or 'notExists'.
    LabelConstraintOp
}

/// Go `In`: restricts the store label value should in the value list.
/// If label does not exist, `in` is always false.
pub const IN: LabelConstraintOp = LabelConstraintOp(Cow::Borrowed("in"));
/// Go `NotIn`: restricts the store label value should not in the value list.
/// If label does not exist, `notIn` is always true.
pub const NOT_IN: LabelConstraintOp = LabelConstraintOp(Cow::Borrowed("notIn"));
/// Go `Exists`: restricts the store should have the label.
pub const EXISTS: LabelConstraintOp = LabelConstraintOp(Cow::Borrowed("exists"));
/// Go `NotExists`: restricts the store should not have the label.
pub const NOT_EXISTS: LabelConstraintOp = LabelConstraintOp(Cow::Borrowed("notExists"));

/// Go `Rule`: the placement rule that can be checked against a region. When
/// applying rules (apply means schedule regions to match selected rules), the
/// apply order is defined by the tuple [GroupIndex, GroupID, Index, ID].
///
/// Its JSON goes through Go's private `rule` helper: `start_key`/`end_key`
/// carry the hex fields (derived from the raw keys when the hex is empty),
/// decoding fills the raw keys from the hex, and `version` and
/// `create_timestamp` are neither encoded nor decoded.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Rule {
    /// mark the source that add the rule
    pub group_id: String,
    /// unique ID within a group
    pub id: String,
    /// rule apply order in a group, rule with less ID is applied first when indexes are equal
    pub index: i64,
    /// when it is true, all rules with less indexes are disabled
    pub r#override: bool,
    /// range start key
    pub start_key: Vec<u8>,
    /// hex format start key, for marshal/unmarshal
    pub start_key_hex: String,
    /// range end key
    pub end_key: Vec<u8>,
    /// hex format end key, for marshal/unmarshal
    pub end_key_hex: String,
    /// expected role of the peers
    pub role: PeerRoleType,
    /// when it is true, it means the role is also a witness
    pub is_witness: bool,
    /// expected count of the peers
    pub count: i64,
    /// used to select stores to place peers
    pub label_constraints: Vec<LabelConstraint>,
    /// used to make peers isolated physically
    pub location_labels: Vec<String>,
    /// used to isolate replicas explicitly and forcibly
    pub isolation_level: String,
    /// only set at runtime, add 1 each time rules updated, begin from 0.
    pub version: u64,
    /// only set at runtime, recorded rule create timestamp
    pub create_timestamp: u64,
}

/// Go `(*Rule).String`: the JSON text, or empty when encoding fails.
impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&serde_json::to_string(self).unwrap_or_default())
    }
}

/// Go `(*Rule).Clone`: a JSON round trip (so `version` and `create_timestamp`
/// are dropped, and an undecodable hex key leaves every JSON field at its
/// zero value), followed by independent copies of the raw keys.
impl Clone for Rule {
    fn clone(&self) -> Self {
        let mut clone: Rule = serde_json::from_str(&self.to_string()).unwrap_or_default();
        clone.start_key = self.start_key.clone();
        clone.end_key = self.end_key.clone();
        clone
    }
}

// Go's private `rule`: the helper struct used to customize the JSON
// marshal/unmarshal methods of `Rule`.
#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct RuleJson {
    #[serde(rename = "group_id")]
    group_id: String,
    #[serde(rename = "id")]
    id: String,
    #[serde(rename = "index", skip_serializing_if = "is_zero_i64")]
    index: i64,
    #[serde(rename = "override", skip_serializing_if = "is_false")]
    r#override: bool,
    #[serde(rename = "start_key")]
    start_key_hex: String,
    #[serde(rename = "end_key")]
    end_key_hex: String,
    #[serde(rename = "role")]
    role: PeerRoleType,
    #[serde(rename = "is_witness")]
    is_witness: bool,
    #[serde(rename = "count")]
    count: i64,
    #[serde(rename = "label_constraints", skip_serializing_if = "Vec::is_empty")]
    label_constraints: Vec<LabelConstraint>,
    #[serde(rename = "location_labels", skip_serializing_if = "Vec::is_empty")]
    location_labels: Vec<String>,
    #[serde(rename = "isolation_level", skip_serializing_if = "String::is_empty")]
    isolation_level: String,
}

impl RuleJson {
    // Go `MarshalJSON`: converts the start/end key to hex format if the
    // corresponding hex field is empty.
    fn from_rule(r: &Rule) -> RuleJson {
        let mut temp = RuleJson {
            group_id: r.group_id.clone(),
            id: r.id.clone(),
            index: r.index,
            r#override: r.r#override,
            start_key_hex: r.start_key_hex.clone(),
            end_key_hex: r.end_key_hex.clone(),
            role: r.role.clone(),
            is_witness: r.is_witness,
            count: r.count,
            label_constraints: r.label_constraints.clone(),
            location_labels: r.location_labels.clone(),
            isolation_level: r.isolation_level.clone(),
        };
        if !r.start_key.is_empty() && r.start_key_hex.is_empty() {
            temp.start_key_hex = raw_key_to_key_hex_str(&r.start_key);
        }
        if !r.end_key.is_empty() && r.end_key_hex.is_empty() {
            temp.end_key_hex = raw_key_to_key_hex_str(&r.end_key);
        }
        temp
    }

    // Go `UnmarshalJSON`: the raw keys are decoded from the hex fields.
    fn into_rule(self) -> crate::Result<Rule> {
        let start_key = key_hex_str_to_raw_key(&self.start_key_hex)?;
        let end_key = key_hex_str_to_raw_key(&self.end_key_hex)?;
        Ok(Rule {
            group_id: self.group_id,
            id: self.id,
            index: self.index,
            r#override: self.r#override,
            start_key,
            start_key_hex: self.start_key_hex,
            end_key,
            end_key_hex: self.end_key_hex,
            role: self.role,
            is_witness: self.is_witness,
            count: self.count,
            label_constraints: self.label_constraints,
            location_labels: self.location_labels,
            isolation_level: self.isolation_level,
            version: 0,
            create_timestamp: 0,
        })
    }
}

impl serde::Serialize for Rule {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&RuleJson::from_rule(self), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Rule {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let temp = <RuleJson as serde::Deserialize>::deserialize(deserializer)?;
        temp.into_rule().map_err(serde::de::Error::custom)
    }
}

go_string_type! {
    /// Go `RuleOpType`: the operation type.
    RuleOpType
}

/// Go `RuleOpAdd`: a placement rule, only need to specify the field *Rule.
pub const RULE_OP_ADD: RuleOpType = RuleOpType(Cow::Borrowed("add"));
/// Go `RuleOpDel`: a placement rule, only need to specify the field `GroupID`,
/// `ID`, `MatchID`.
pub const RULE_OP_DEL: RuleOpType = RuleOpType(Cow::Borrowed("del"));

/// Go `RuleOp`: for batching placement rule actions. The action type is
/// distinguished by the field `action`. Go embeds `*Rule`; here the rule is the
/// `rule` field, also reachable through `Deref`. Its JSON flattens the rule's
/// helper fields, as Go's private `ruleOp` does.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuleOp {
    /// information of the placement rule to add/delete the operation type
    pub rule: Rule,
    pub action: RuleOpType,
    /// if action == delete, delete by the prefix of id
    pub delete_by_id_prefix: bool,
}

impl std::ops::Deref for RuleOp {
    type Target = Rule;

    fn deref(&self) -> &Rule {
        &self.rule
    }
}

impl std::ops::DerefMut for RuleOp {
    fn deref_mut(&mut self) -> &mut Rule {
        &mut self.rule
    }
}

/// Go `(*RuleOp).String`.
impl fmt::Display for RuleOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&serde_json::to_string(self).unwrap_or_default())
    }
}

// Go's private `ruleOp`.
#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct RuleOpJson {
    #[serde(flatten)]
    rule: RuleJson,
    #[serde(rename = "action")]
    action: RuleOpType,
    #[serde(rename = "delete_by_id_prefix")]
    delete_by_id_prefix: bool,
}

impl serde::Serialize for RuleOp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let temp = RuleOpJson {
            rule: RuleJson::from_rule(&self.rule),
            action: self.action.clone(),
            delete_by_id_prefix: self.delete_by_id_prefix,
        };
        serde::Serialize::serialize(&temp, serializer)
    }
}

impl<'de> serde::Deserialize<'de> for RuleOp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let temp = <RuleOpJson as serde::Deserialize>::deserialize(deserializer)?;
        Ok(RuleOp {
            rule: temp
                .rule
                .into_rule()
                .map_err(<D::Error as serde::de::Error>::custom)?,
            action: temp.action,
            delete_by_id_prefix: temp.delete_by_id_prefix,
        })
    }
}

/// Go `RuleGroup`: properties of a rule group.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RuleGroup {
    #[serde(rename = "id", skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(rename = "index", skip_serializing_if = "is_zero_i64")]
    pub index: i64,
    #[serde(rename = "override", skip_serializing_if = "is_false")]
    pub r#override: bool,
}

/// Go `(*RuleGroup).String`.
impl fmt::Display for RuleGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&serde_json::to_string(self).unwrap_or_default())
    }
}

/// Go `GroupBundle`: a rule group and all rules belong to the group.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GroupBundle {
    #[serde(rename = "group_id")]
    pub id: String,
    #[serde(rename = "group_index")]
    pub index: i64,
    #[serde(rename = "group_override")]
    pub r#override: bool,
    #[serde(rename = "rules")]
    pub rules: Vec<Rule>,
}

/// Go `RegionLabel`: the label of a region.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionLabel {
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "value")]
    pub value: String,
    #[serde(rename = "ttl", skip_serializing_if = "String::is_empty")]
    pub ttl: String,
    #[serde(rename = "start_at", skip_serializing_if = "String::is_empty")]
    pub start_at: String,
}

/// Go `LabelRule`: the rule to assign labels to a region. `data` is Go's `any`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LabelRule {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "index")]
    pub index: i64,
    #[serde(rename = "labels")]
    pub labels: Vec<RegionLabel>,
    #[serde(rename = "rule_type")]
    pub rule_type: String,
    #[serde(rename = "data")]
    pub data: serde_json::Value,
}

/// Go `LabelRulePatch`: the patch to update the label rules.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LabelRulePatch {
    #[serde(rename = "sets")]
    pub set_rules: Vec<LabelRule>,
    #[serde(rename = "deletes")]
    pub delete_rules: Vec<String>,
}

/// Go `MembersInfo`: PD members info returned from PD RESTful interface. The
/// protobuf members decode from Go's `encoding/json` form of the gogoproto
/// structs (snake-case keys, every field `omitempty`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MembersInfo {
    #[serde(
        rename = "header",
        skip_serializing_if = "Option::is_none",
        with = "pb_json::opt_response_header"
    )]
    pub header: Option<pdpb::ResponseHeader>,
    #[serde(
        rename = "members",
        skip_serializing_if = "Vec::is_empty",
        with = "pb_json::vec_member"
    )]
    pub members: Vec<pdpb::Member>,
    #[serde(
        rename = "leader",
        skip_serializing_if = "Option::is_none",
        with = "pb_json::opt_member"
    )]
    pub leader: Option<pdpb::Member>,
    #[serde(
        rename = "etcd_leader",
        skip_serializing_if = "Option::is_none",
        with = "pb_json::opt_member"
    )]
    pub etcd_leader: Option<pdpb::Member>,
}

/// Go `MicroserviceMember`: the member info of a microservice.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MicroserviceMember {
    #[serde(rename = "service-addr")]
    pub service_addr: String,
    #[serde(rename = "version")]
    pub version: String,
    #[serde(rename = "git-hash")]
    pub git_hash: String,
    #[serde(rename = "deploy-path")]
    pub deploy_path: String,
    #[serde(rename = "start-timestamp")]
    pub start_timestamp: i64,
}

/// Go `KeyspaceGCManagementType`: parameters needed to modify the gc management
/// type. If `gc_management_type` is `global_gc`, it means the current keyspace
/// requires a tidb without 'keyspace-name' configured to run a global gc worker
/// to calculate a global gc safe point. If `gc_management_type` is
/// `keyspace_level_gc` it means the current keyspace can calculate gc safe
/// point by its own.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct KeyspaceGCManagementType {
    #[serde(
        rename = "gc_management_type",
        skip_serializing_if = "String::is_empty"
    )]
    pub gc_management_type: String,
}

/// Go `KeyspaceGCManagementTypeConfig`: parameters needed to modify target
/// keyspace's configs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct KeyspaceGCManagementTypeConfig {
    #[serde(rename = "config")]
    pub config: KeyspaceGCManagementType,
}

/// Go `UpdateKeyspaceConfigParams`: parameters needed to modify target keyspace
/// configs. A map of string to optional string differentiates between JSON
/// null and "".
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UpdateKeyspaceConfigParams {
    #[serde(rename = "config")]
    pub config: HashMap<String, Option<String>>,
    /// Preconditions specifies prerequisites for updating config, using a
    /// JSON-merge-patch-like encoding:
    /// - key -> None means the key must be absent.
    /// - key -> Some("value") means the key must exist and equal "value".
    #[serde(rename = "preconditions", skip_serializing_if = "HashMap::is_empty")]
    pub preconditions: HashMap<String, Option<String>>,
}

// Go `tempKeyspaceMeta`: the keyspace meta struct that returned from the http
// interface.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct TempKeyspaceMeta {
    #[serde(rename = "id")]
    pub(super) id: u32,
    #[serde(rename = "name")]
    pub(super) name: String,
    #[serde(rename = "state")]
    pub(super) state: String,
    #[serde(rename = "created_at")]
    pub(super) created_at: i64,
    #[serde(rename = "state_changed_at")]
    pub(super) state_changed_at: i64,
    #[serde(rename = "config")]
    pub(super) config: HashMap<String, String>,
}

impl TempKeyspaceMeta {
    /// Go `toPB`.
    pub(super) fn to_pb(&self) -> crate::Result<keyspacepb::KeyspaceMeta> {
        let keyspace_state = string_to_keyspace_state(&self.state)?;
        let mut meta = keyspacepb::KeyspaceMeta::default();
        meta.name = self.name.clone();
        meta.keyspace = Some(keyspacepb::keyspace_meta::Keyspace::Id(self.id));
        meta.config = self.config.clone();
        meta.created_at = self.created_at;
        meta.state_changed_at = self.state_changed_at;
        meta.state = keyspace_state as i32;
        Ok(meta)
    }
}

fn string_to_keyspace_state(text: &str) -> crate::Result<keyspacepb::KeyspaceState> {
    match text {
        "ENABLED" => Ok(keyspacepb::KeyspaceState::Enabled),
        "DISABLED" => Ok(keyspacepb::KeyspaceState::Disabled),
        "ARCHIVED" => Ok(keyspacepb::KeyspaceState::Archived),
        "TOMBSTONE" => Ok(keyspacepb::KeyspaceState::Tombstone),
        _ => Err(Error::StringError(format!(
            "invalid KeyspaceState string: {text}"
        ))),
    }
}

/// Go `Health`: the cluster's health.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Health {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "member_id")]
    pub member_id: u64,
    #[serde(rename = "client_urls")]
    pub client_urls: Vec<String>,
    #[serde(rename = "health")]
    pub health: bool,
}

/// Go `AffinityGroupKeyRange`: a key range for affinity group operations. The
/// keys are Go `[]byte`, so their JSON is standard base64 text.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AffinityGroupKeyRange {
    #[serde(rename = "start_key", with = "go_bytes")]
    pub start_key: Vec<u8>,
    #[serde(rename = "end_key", with = "go_bytes")]
    pub end_key: Vec<u8>,
}

/// Go `CreateAffinityGroupInput`: the input for a single group in the creation request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CreateAffinityGroupInput {
    #[serde(rename = "ranges")]
    pub ranges: Vec<AffinityGroupKeyRange>,
}

/// Go `CreateAffinityGroupsRequest`: the body for the POST request to create
/// affinity groups.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CreateAffinityGroupsRequest {
    #[serde(rename = "affinity_groups")]
    pub affinity_groups: HashMap<String, CreateAffinityGroupInput>,
}

/// Go `AffinityGroup`: an affinity group.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AffinityGroup {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "create_timestamp")]
    pub create_timestamp: u64,
    #[serde(rename = "leader_store_id", skip_serializing_if = "is_zero_u64")]
    pub leader_store_id: u64,
    #[serde(rename = "voter_store_ids", skip_serializing_if = "Vec::is_empty")]
    pub voter_store_ids: Vec<u64>,
}

/// Go `AffinityGroupState`: the runtime state of an affinity group. Go embeds
/// `AffinityGroup`; its fields are flattened into the same JSON object.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AffinityGroupState {
    #[serde(flatten)]
    pub affinity_group: AffinityGroup,
    #[serde(rename = "phase")]
    pub phase: String,
    #[serde(rename = "range_count")]
    pub range_count: i64,
    #[serde(rename = "region_count")]
    pub region_count: i64,
    #[serde(rename = "affinity_region_count")]
    pub affinity_region_count: i64,
}

impl std::ops::Deref for AffinityGroupState {
    type Target = AffinityGroup;

    fn deref(&self) -> &AffinityGroup {
        &self.affinity_group
    }
}

impl AffinityGroupState {
    /// Go `IsPending`: the Group is still determining the StoreIDs. If the
    /// Group has no KeyRanges, it remains in pending forever.
    pub fn is_pending(&self) -> bool {
        self.phase == "pending"
    }

    /// Go `IsPreparing`: the Group is scheduling Regions according to the
    /// required Peers.
    pub fn is_preparing(&self) -> bool {
        self.phase == "preparing"
    }

    /// Go `IsStable`: the Group has completed the required scheduling and is
    /// currently in a stable state.
    pub fn is_stable(&self) -> bool {
        self.phase == "stable"
    }
}

/// Go `AffinityGroupsResponse`: the success response for affinity group operations.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AffinityGroupsResponse {
    #[serde(rename = "affinity_groups")]
    pub affinity_groups: HashMap<String, AffinityGroupState>,
}

/// Go `BatchDeleteAffinityGroupsRequest`: the body for batch delete request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchDeleteAffinityGroupsRequest {
    #[serde(rename = "ids")]
    pub ids: Vec<String>,
    #[serde(rename = "force", skip_serializing_if = "is_false")]
    pub force: bool,
}

/// Go `GroupRangesModification`: add or remove operations for a specific group.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GroupRangesModification {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "ranges")]
    pub ranges: Vec<AffinityGroupKeyRange>,
}

/// Go `BatchModifyAffinityGroupsRequest`: the body for batch modify request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchModifyAffinityGroupsRequest {
    #[serde(rename = "add", skip_serializing_if = "Vec::is_empty")]
    pub add: Vec<GroupRangesModification>,
    #[serde(rename = "remove", skip_serializing_if = "Vec::is_empty")]
    pub remove: Vec<GroupRangesModification>,
}

/// Go `UpdateAffinityGroupPeersRequest`: the body for updating peer
/// distribution of an affinity group.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UpdateAffinityGroupPeersRequest {
    #[serde(rename = "leader_store_id")]
    pub leader_store_id: u64,
    #[serde(rename = "voter_store_ids")]
    pub voter_store_ids: Vec<u64>,
}

// Go `encoding/json` for `[]byte`: standard base64 text; `null` decodes to
// an empty key.
mod go_bytes {
    use super::super::codec::{base64_decode, base64_encode};

    pub fn serialize<S: serde::Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64_encode(value))
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        match <Option<String> as serde::Deserialize>::deserialize(deserializer)? {
            None => Ok(Vec::new()),
            Some(text) => base64_decode(&text).map_err(serde::de::Error::custom),
        }
    }
}

// Go `encoding/json` forms of the gogoproto messages this package carries.
// gogoproto tags every field `json:"<snake_name>,omitempty"`; enums are numbers
// and `[]byte` is base64.
mod pb_json {
    use serde_derive::{Deserialize, Serialize};

    use super::super::codec::{base64_decode, base64_encode};
    use super::{is_zero_u64, pdpb};
    use crate::proto::encryptionpb;

    fn is_zero_i32(value: &i32) -> bool {
        *value == 0
    }

    fn is_zero_u32(value: &u32) -> bool {
        *value == 0
    }

    #[derive(Default, Serialize, Deserialize)]
    #[serde(default)]
    pub(super) struct MemberJson {
        #[serde(rename = "name", skip_serializing_if = "String::is_empty")]
        name: String,
        #[serde(rename = "member_id", skip_serializing_if = "is_zero_u64")]
        member_id: u64,
        #[serde(rename = "peer_urls", skip_serializing_if = "Vec::is_empty")]
        peer_urls: Vec<String>,
        #[serde(rename = "client_urls", skip_serializing_if = "Vec::is_empty")]
        client_urls: Vec<String>,
        #[serde(rename = "leader_priority", skip_serializing_if = "is_zero_i32")]
        leader_priority: i32,
        #[serde(rename = "deploy_path", skip_serializing_if = "String::is_empty")]
        deploy_path: String,
        #[serde(rename = "binary_version", skip_serializing_if = "String::is_empty")]
        binary_version: String,
        #[serde(rename = "git_hash", skip_serializing_if = "String::is_empty")]
        git_hash: String,
        #[serde(rename = "dc_location", skip_serializing_if = "String::is_empty")]
        dc_location: String,
    }

    impl MemberJson {
        pub(super) fn from_pb(member: &pdpb::Member) -> MemberJson {
            MemberJson {
                name: member.name.clone(),
                member_id: member.member_id,
                peer_urls: member.peer_urls.clone(),
                client_urls: member.client_urls.clone(),
                leader_priority: member.leader_priority,
                deploy_path: member.deploy_path.clone(),
                binary_version: member.binary_version.clone(),
                git_hash: member.git_hash.clone(),
                dc_location: member.dc_location.clone(),
            }
        }

        pub(super) fn into_pb(self) -> pdpb::Member {
            let mut member = pdpb::Member::default();
            member.name = self.name;
            member.member_id = self.member_id;
            member.peer_urls = self.peer_urls;
            member.client_urls = self.client_urls;
            member.leader_priority = self.leader_priority;
            member.deploy_path = self.deploy_path;
            member.binary_version = self.binary_version;
            member.git_hash = self.git_hash;
            member.dc_location = self.dc_location;
            member
        }
    }

    #[derive(Default, Serialize, Deserialize)]
    #[serde(default)]
    struct ErrorJson {
        #[serde(rename = "type", skip_serializing_if = "is_zero_i32")]
        r#type: i32,
        #[serde(rename = "message", skip_serializing_if = "String::is_empty")]
        message: String,
    }

    #[derive(Default, Serialize, Deserialize)]
    #[serde(default)]
    struct ResponseHeaderJson {
        #[serde(rename = "cluster_id", skip_serializing_if = "is_zero_u64")]
        cluster_id: u64,
        #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
        error: Option<ErrorJson>,
    }

    impl ResponseHeaderJson {
        fn from_pb(header: &pdpb::ResponseHeader) -> ResponseHeaderJson {
            ResponseHeaderJson {
                cluster_id: header.cluster_id,
                error: header.error.as_ref().map(|error| ErrorJson {
                    r#type: error.r#type,
                    message: error.message.clone(),
                }),
            }
        }

        fn into_pb(self) -> pdpb::ResponseHeader {
            let mut header = pdpb::ResponseHeader::default();
            header.cluster_id = self.cluster_id;
            header.error = self.error.map(|error| {
                let mut pb = pdpb::Error::default();
                pb.r#type = error.r#type;
                pb.message = error.message;
                pb
            });
            header
        }
    }

    /// Go `encoding/json` of `metapb.Cluster`.
    #[derive(Default, Serialize, Deserialize)]
    #[serde(default)]
    pub(in super::super) struct ClusterJson {
        #[serde(rename = "id", skip_serializing_if = "is_zero_u64")]
        id: u64,
        #[serde(rename = "max_peer_count", skip_serializing_if = "is_zero_u32")]
        max_peer_count: u32,
    }

    impl ClusterJson {
        pub(in super::super) fn into_pb(self) -> crate::proto::metapb::Cluster {
            let mut cluster = crate::proto::metapb::Cluster::default();
            cluster.id = self.id;
            cluster.max_peer_count = self.max_peer_count;
            cluster
        }
    }

    #[derive(Default, Serialize, Deserialize)]
    #[serde(default)]
    struct EncryptionMetaJson {
        #[serde(rename = "key_id", skip_serializing_if = "is_zero_u64")]
        key_id: u64,
        #[serde(rename = "iv", skip_serializing_if = "String::is_empty")]
        iv: String,
    }

    pub(super) mod opt_member {
        use super::MemberJson;
        use crate::proto::pdpb;

        pub fn serialize<S: serde::Serializer>(
            value: &Option<pdpb::Member>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            serde::Serialize::serialize(&value.as_ref().map(MemberJson::from_pb), serializer)
        }

        pub fn deserialize<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<pdpb::Member>, D::Error> {
            let value = <Option<MemberJson> as serde::Deserialize>::deserialize(deserializer)?;
            Ok(value.map(MemberJson::into_pb))
        }
    }

    pub(super) mod vec_member {
        use super::MemberJson;
        use crate::proto::pdpb;

        pub fn serialize<S: serde::Serializer>(
            value: &[pdpb::Member],
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            let members: Vec<MemberJson> = value.iter().map(MemberJson::from_pb).collect();
            serde::Serialize::serialize(&members, serializer)
        }

        pub fn deserialize<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Vec<pdpb::Member>, D::Error> {
            let value = <Option<Vec<MemberJson>> as serde::Deserialize>::deserialize(deserializer)?;
            Ok(value
                .unwrap_or_default()
                .into_iter()
                .map(MemberJson::into_pb)
                .collect())
        }
    }

    pub(super) mod opt_response_header {
        use super::ResponseHeaderJson;
        use crate::proto::pdpb;

        pub fn serialize<S: serde::Serializer>(
            value: &Option<pdpb::ResponseHeader>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            serde::Serialize::serialize(
                &value.as_ref().map(ResponseHeaderJson::from_pb),
                serializer,
            )
        }

        pub fn deserialize<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<pdpb::ResponseHeader>, D::Error> {
            let value =
                <Option<ResponseHeaderJson> as serde::Deserialize>::deserialize(deserializer)?;
            Ok(value.map(ResponseHeaderJson::into_pb))
        }
    }

    pub(super) mod opt_encryption_meta {
        use super::{base64_decode, base64_encode, encryptionpb, EncryptionMetaJson};

        pub fn serialize<S: serde::Serializer>(
            value: &Option<encryptionpb::EncryptionMeta>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            let value = value.as_ref().map(|meta| EncryptionMetaJson {
                key_id: meta.key_id,
                iv: base64_encode(&meta.iv),
            });
            serde::Serialize::serialize(&value, serializer)
        }

        pub fn deserialize<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<encryptionpb::EncryptionMeta>, D::Error> {
            let value =
                <Option<EncryptionMetaJson> as serde::Deserialize>::deserialize(deserializer)?;
            value
                .map(|json| {
                    let mut meta = encryptionpb::EncryptionMeta::default();
                    meta.key_id = json.key_id;
                    meta.iv =
                        base64_decode(&json.iv).map_err(<D::Error as serde::de::Error>::custom)?;
                    Ok(meta)
                })
                .transpose()
        }
    }
}

pub(super) use pb_json::ClusterJson;

/// A `pdpb.Member` decoded from Go's JSON form, as `get_leader` reads it; a
/// JSON `null` leaves the zero member, like Go's decoder.
#[derive(Default)]
pub(super) struct PbMember(pub(super) pdpb::Member);

impl<'de> serde::Deserialize<'de> for PbMember {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(PbMember(
            pb_json::opt_member::deserialize(deserializer)?.unwrap_or_default(),
        ))
    }
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
