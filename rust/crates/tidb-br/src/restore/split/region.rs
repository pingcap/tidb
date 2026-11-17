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

//! Go `br/pkg/restore/split/region.go`.

use tidb_proto::metapb;

/// Go `RegionInfo`: a region and its leader, as PD reports them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RegionInfo {
    /// Go `Region`.
    pub region: metapb::Region,
    /// Go `Leader`.
    pub leader: Option<metapb::Peer>,
    /// Go `PendingPeers`.
    pub pending_peers: Vec<metapb::Peer>,
    /// Go `DownPeers`.
    pub down_peers: Vec<metapb::Peer>,
}

impl RegionInfo {
    /// Go `ContainsInterior`: `key` is inside the region and not its start.
    #[must_use]
    pub fn contains_interior(&self, key: &[u8]) -> bool {
        key > self.region.start_key.as_slice() && before_end(key, &self.region.end_key)
    }
}

/// Go `beforeEnd`.
fn before_end(key: &[u8], end: &[u8]) -> bool {
    key < end || end.is_empty()
}

impl From<crate::pdclient::PdRegion> for RegionInfo {
    fn from(r: crate::pdclient::PdRegion) -> Self {
        Self {
            region: r.meta,
            leader: r.leader,
            pending_peers: r.pending_peers,
            down_peers: r.down_peers,
        }
    }
}
