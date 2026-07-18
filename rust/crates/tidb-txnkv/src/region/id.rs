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

/// Raft region epoch copied from PD metadata.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RegionEpoch {
    /// Peer configuration version.
    pub conf_ver: u64,
    /// Key-range version.
    pub version: u64,
}

impl RegionEpoch {
    /// Returns true when this epoch is strictly older in either source field.
    #[must_use]
    pub const fn is_older_than(self, other: Self) -> bool {
        self.conf_ver < other.conf_ver || self.version < other.version
    }
}

/// Identity of one region at one exact epoch.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RegionVerId {
    /// Region identifier.
    pub id: u64,
    /// Exact epoch.
    pub epoch: RegionEpoch,
}

impl RegionVerId {
    /// Creates one source-shaped versioned region identity.
    #[must_use]
    pub const fn new(id: u64, conf_ver: u64, version: u64) -> Self {
        Self {
            id,
            epoch: RegionEpoch { conf_ver, version },
        }
    }
}
