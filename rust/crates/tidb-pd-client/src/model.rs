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

/// One validated source peer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdPeer {
    /// Peer identity.
    pub id: u64,
    /// Referenced store identity.
    pub store_id: u64,
    /// Raw source role discriminant. Protobuf enums are forward-extensible.
    pub role: i32,
    /// Source witness flag.
    pub is_witness: bool,
}

/// One validated foreground PD membership snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdMemberSet {
    /// Nonzero cluster identity shared by every accepted response.
    pub cluster_id: u64,
    /// Normalized plaintext client URL reported for the PD leader.
    pub leader_url: String,
    /// Sorted, deduplicated normalized plaintext URLs for all PD members.
    pub member_urls: Vec<String>,
}

/// Versioned region epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PdRegionEpoch {
    /// Membership configuration version.
    pub conf_ver: u64,
    /// Split/merge version.
    pub version: u64,
}

/// Exact bucket activity arrays carried by pinned kvproto.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdBucketStats {
    /// Total read bytes per bucket.
    pub read_bytes: Vec<u64>,
    /// Total written bytes per bucket.
    pub write_bytes: Vec<u64>,
    /// Read QPS per bucket.
    pub read_qps: Vec<u64>,
    /// Write QPS per bucket.
    pub write_qps: Vec<u64>,
    /// Read keys per bucket.
    pub read_keys: Vec<u64>,
    /// Written keys per bucket.
    pub write_keys: Vec<u64>,
}

/// Ordered bucket topology in PD's wire key domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdBuckets {
    /// Region owning these buckets.
    pub region_id: u64,
    /// Monotonic bucket boundary version.
    pub version: u64,
    /// Ordered boundaries, including the region boundaries when complete.
    pub keys: Vec<Vec<u8>>,
    /// Optional activity arrays returned by PD.
    pub stats: Option<PdBucketStats>,
    /// Source collection period in milliseconds.
    pub period_in_ms: u64,
}

/// One already encoded PD batch-scan range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdKeyRange {
    /// Inclusive start key.
    pub start_key: Vec<u8>,
    /// Exclusive end key; empty means positive infinity.
    pub end_key: Vec<u8>,
}

/// Region metadata in PD's wire key domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdRegion {
    /// Region identity.
    pub id: u64,
    /// Inclusive wire-encoded start key.
    pub start_key: Vec<u8>,
    /// Exclusive wire-encoded end key; empty means positive infinity.
    pub end_key: Vec<u8>,
    /// Region epoch.
    pub epoch: PdRegionEpoch,
    /// Peers in PD metadata order.
    pub peers: Vec<PdPeer>,
    /// Leader returned by PD. Scans preserve a missing/zero leader as `None`.
    pub leader: Option<PdPeer>,
    /// Exact peers reported down by the leader, in response order.
    pub down_peers: Vec<PdPeer>,
    /// Pending peers in response order.
    pub pending_peers: Vec<PdPeer>,
    /// Optional bucket topology in PD's wire key domain.
    pub buckets: Option<PdBuckets>,
}

/// Legacy store lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PdStoreState {
    /// Online according to the legacy field.
    Up,
    /// Being removed according to the legacy field.
    Offline,
}

/// Replacement store lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PdNodeState {
    /// Joining but not yet fully serving.
    Preparing,
    /// Serving requests.
    Serving,
    /// Being removed.
    Removing,
}

/// One usable TiKV store returned by PD.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdStore {
    /// Store identity.
    pub id: u64,
    /// TiKV client address.
    pub address: String,
    /// Legacy source state.
    pub state: PdStoreState,
    /// Replacement source state.
    pub node_state: PdNodeState,
    /// Store labels in PD metadata order.
    pub labels: Vec<(String, String)>,
}

/// PD's current GC state for one keyspace scope.
///
/// Only `txn_safe_point` gates client reads: it is the timestamp below which PD
/// has promised no transaction may still be reading, so a snapshot pinned under
/// it may already have lost the MVCC versions it needs. `gc_safe_point` is
/// projected for diagnostics only — advancing either point, and deleting the
/// versions below them, belongs to the GC owner and not to a reading client.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PdGcState {
    /// Whether GC for this scope is managed at the keyspace level.
    pub is_keyspace_level_gc: bool,
    /// Timestamp below which transactions are no longer safe to continue.
    pub txn_safe_point: u64,
    /// Timestamp below which obsolete MVCC versions may already be deleted.
    pub gc_safe_point: u64,
}
