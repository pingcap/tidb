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

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::{PeerRole, ReadPolicy, RegionAttempt, RegionVerId};

/// Attempt count and completed RPC time for one peer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PeerAttemptState {
    attempts: u8,
    attempted_time: Duration,
}

/// Pinned client-go's maximum attempts against one leader generation.
pub const MAX_REPLICA_ATTEMPTS: u8 = 10;

/// Pinned client-go's maximum accumulated attempt time for one leader.
pub const MAX_REPLICA_ATTEMPT_TIME: Duration = Duration::from_secs(50);

/// One immutable request target selected from the cached region.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderRequest {
    /// Exact region/store route observed by this request.
    pub attempt: RegionAttempt,
    /// Raft role copied from the immutable region snapshot.
    pub role: PeerRole,
    /// Whether source metadata marks this peer as a witness.
    pub is_witness: bool,
    /// Whether TiKV must execute this as a replica read.
    pub replica_read: bool,
    /// Whether TiKV must execute this as a stale read.
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

/// Request-scoped replica selector state.
///
/// The cache remains the store/leader authority. This object owns only attempt
/// history, so every selection is revalidated against current canonical state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestSelector {
    pub(crate) region: RegionVerId,
    pub(crate) policy: ReadPolicy,
    pub(crate) leader_peer_id: Option<u64>,
    pub(crate) attempts_by_peer: BTreeMap<u64, PeerAttemptState>,
    pub(crate) pending_attempt: Option<RegionAttempt>,
    pub(crate) completed_attempt: Option<RegionAttempt>,
    pub(crate) data_not_ready_peers: BTreeSet<u64>,
    pub(crate) dispatches: u32,
}

impl RequestSelector {
    pub(crate) fn new(
        region: RegionVerId,
        policy: ReadPolicy,
        leader_peer_id: Option<u64>,
    ) -> Self {
        Self {
            region,
            policy,
            leader_peer_id,
            attempts_by_peer: BTreeMap::new(),
            pending_attempt: None,
            completed_attempt: None,
            data_not_ready_peers: BTreeSet::new(),
            dispatches: 0,
        }
    }

    /// Exact region bound to this selector.
    #[must_use]
    pub const fn region(&self) -> RegionVerId {
        self.region
    }

    /// Excludes a peer which returned `NotLeader` without a known leader.
    pub fn reject_peer(&mut self, peer_id: u64) {
        self.attempts_by_peer.entry(peer_id).or_default().attempts = MAX_REPLICA_ATTEMPTS;
    }

    pub(crate) fn attempts_for(&self, peer_id: u64) -> u8 {
        self.attempts_by_peer
            .get(&peer_id)
            .map_or(0, |state| state.attempts)
    }

    pub(crate) fn attempted_time_for(&self, peer_id: u64) -> Duration {
        self.attempts_by_peer
            .get(&peer_id)
            .map_or(Duration::ZERO, |state| state.attempted_time)
    }

    pub(crate) fn observe_leader(&mut self, leader_peer_id: Option<u64>) {
        if self.leader_peer_id == leader_peer_id {
            return;
        }
        self.leader_peer_id = leader_peer_id;
        let Some(peer_id) = leader_peer_id else {
            return;
        };
        let state = self.attempts_by_peer.entry(peer_id).or_default();
        if state.attempts >= MAX_REPLICA_ATTEMPTS
            || state.attempted_time >= MAX_REPLICA_ATTEMPT_TIME
        {
            state.attempts = MAX_REPLICA_ATTEMPTS - 1;
            state.attempted_time = Duration::ZERO;
        }
    }

    pub(crate) fn record_dispatch(&mut self, attempt: RegionAttempt) {
        let state = self.attempts_by_peer.entry(attempt.peer_id).or_default();
        state.attempts = state.attempts.saturating_add(1);
        self.pending_attempt = Some(attempt);
        self.completed_attempt = None;
        self.dispatches = self.dispatches.saturating_add(1);
    }

    /// Records the duration of the exact outstanding RPC attempt.
    ///
    /// A stale, duplicated, or unrelated completion returns `false` without
    /// changing selector state.
    #[must_use]
    pub fn record_attempt_result(&mut self, attempt: &RegionAttempt, duration: Duration) -> bool {
        if self.pending_attempt.as_ref() != Some(attempt) {
            return false;
        }
        let state = self.attempts_by_peer.entry(attempt.peer_id).or_default();
        state.attempted_time = state.attempted_time.saturating_add(duration);
        self.completed_attempt = self.pending_attempt.take();
        true
    }

    /// Marks an exact completed stale-read attempt as `DataIsNotReady`.
    ///
    /// Pinned client-go permits that nonleader peer one later ordinary
    /// replica-read attempt. Stale, duplicated, or still-pending observations
    /// cannot change the access path.
    #[must_use]
    pub fn record_data_not_ready(&mut self, attempt: &RegionAttempt) -> bool {
        if !self.policy.stale_read
            || self.completed_attempt.as_ref() != Some(attempt)
            || self.leader_peer_id == Some(attempt.peer_id)
        {
            return false;
        }
        self.completed_attempt = None;
        self.data_not_ready_peers.insert(attempt.peer_id)
    }

    pub(crate) fn may_retry_data_not_ready(&self, peer_id: u64) -> bool {
        self.data_not_ready_peers.contains(&peer_id)
    }
}
