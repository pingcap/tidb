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

use std::collections::BTreeMap;
use std::time::Duration;

use super::{PeerRole, ReadPolicy, RegionAttempt, RegionVerId};

/// Pinned client-go's maximum attempts against one leader generation.
pub const MAX_REPLICA_ATTEMPTS: u8 = 10;

/// Pinned client-go's maximum accumulated attempt time for one leader.
pub const MAX_REPLICA_ATTEMPT_TIME: Duration = Duration::from_secs(50);

/// One immutable leader-semantics request target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderRequest {
    /// Exact region/store route observed by this request.
    pub attempt: RegionAttempt,
    /// Raft role copied from the immutable region snapshot.
    pub role: PeerRole,
    /// Whether source metadata marks this peer as a witness.
    pub is_witness: bool,
    /// Leader requests remain non-replica reads even on an alternate peer.
    pub replica_read: bool,
    /// Leader requests never become stale reads while probing peers.
    pub stale_read: bool,
    /// Whether this peer was the cached leader when selected.
    pub cached_leader: bool,
}

/// Result of one request-scoped selection step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestSelection {
    /// Dispatch this immutable attempt.
    Attempt(LeaderRequest),
    /// Every admissible peer was exhausted; reload the exact region.
    ReloadRegion {
        /// Invalidated region identity.
        region: RegionVerId,
    },
}

/// Request-scoped leader-first selector state.
///
/// The cache remains the store/leader authority. This object owns only attempt
/// history, so every selection is revalidated against current canonical state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestSelector {
    pub(crate) region: RegionVerId,
    pub(crate) policy: ReadPolicy,
    pub(crate) attempts_by_peer: BTreeMap<u64, u8>,
}

impl RequestSelector {
    pub(crate) fn new(region: RegionVerId, policy: ReadPolicy) -> Self {
        Self {
            region,
            policy,
            attempts_by_peer: BTreeMap::new(),
        }
    }

    /// Exact region bound to this selector.
    #[must_use]
    pub const fn region(&self) -> RegionVerId {
        self.region
    }

    /// Excludes a peer which returned `NotLeader` without a known leader.
    pub fn reject_peer(&mut self, peer_id: u64) {
        self.attempts_by_peer.insert(peer_id, MAX_REPLICA_ATTEMPTS);
    }

    pub(crate) fn attempts_for(&self, peer_id: u64) -> u8 {
        self.attempts_by_peer.get(&peer_id).copied().unwrap_or(0)
    }

    pub(crate) fn record_attempt(&mut self, peer_id: u64) {
        let attempts = self.attempts_by_peer.entry(peer_id).or_default();
        *attempts = attempts.saturating_add(1);
    }
}
