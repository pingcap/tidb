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

use super::RegionVerId;

/// Half-open encoded TiKV key range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyRange {
    /// Inclusive start key.
    pub start: Vec<u8>,
    /// Exclusive end key. Empty means positive infinity.
    pub end: Vec<u8>,
}

impl KeyRange {
    /// Creates a validated non-empty half-open range.
    #[must_use]
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.end.is_empty() || self.start < self.end
    }
}

/// Peer role needed by the leader-only route.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PeerRole {
    /// Normal voting replica.
    #[default]
    Voter,
    /// Learner replica.
    Learner,
}

/// Immutable region peer metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Peer {
    /// Peer identifier.
    pub id: u64,
    /// Owning store identifier.
    pub store_id: u64,
    /// Raft role.
    pub role: PeerRole,
    /// Store epoch captured when this region snapshot was loaded.
    pub store_epoch: u64,
}

/// Resolved TiKV store metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Store {
    /// Store identifier.
    pub id: u64,
    /// Current TiKV address.
    pub address: String,
    /// Current resolve/failure epoch.
    pub epoch: u64,
}

/// One immutable region snapshot returned by the injected loader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionLocation {
    /// Versioned region identity.
    pub region: RegionVerId,
    /// Inclusive start key.
    pub start_key: Vec<u8>,
    /// Exclusive end key. Empty means positive infinity.
    pub end_key: Vec<u8>,
    /// Region peers in metadata order.
    pub peers: Vec<Peer>,
    /// Leader peer selected by PD.
    pub leader_peer_id: Option<u64>,
    /// Stores referenced by peers.
    pub stores: Vec<Store>,
}

impl RegionLocation {
    /// Whether this location contains one key under Go's `[start,end)` rule.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.start_key.as_slice() <= key
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }

    /// Whether the complete request is contained by this one region.
    #[must_use]
    pub fn contains_range(&self, range: &KeyRange) -> bool {
        if !range.is_valid() || !self.contains_key(&range.start) {
            return false;
        }
        if self.end_key.is_empty() {
            return true;
        }
        !range.end.is_empty() && range.end <= self.end_key
    }
}
