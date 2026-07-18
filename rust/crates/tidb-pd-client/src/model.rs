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
    /// Leader returned by PD.
    pub leader: PdPeer,
    /// Exact peer identities reported down by the leader.
    pub down_peer_ids: Vec<u64>,
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
}
