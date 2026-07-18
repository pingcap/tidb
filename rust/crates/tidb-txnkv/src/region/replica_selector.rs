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

use super::{Peer, RegionLocation, RegionRouteError, Store};

/// client-go replica-read policy discriminants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReplicaReadMode {
    /// Ordinary leader read.
    #[default]
    Leader,
    /// Follower read.
    Follower,
    /// Mixed leader/follower read.
    Mixed,
    /// Learner read.
    Learner,
    /// Prefer leader with fallback.
    PreferLeader,
}

/// Read and forwarding policy presented to replica selection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadPolicy {
    /// Replica selection mode.
    pub mode: ReplicaReadMode,
    /// Whether stale-read semantics are active.
    pub stale_read: bool,
    /// Whether request forwarding/proxy selection is enabled.
    pub forwarding: bool,
}

/// Selected immutable leader route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderRoute<'a> {
    /// Selected peer.
    pub peer: &'a Peer,
    /// Selected store.
    pub store: &'a Store,
}

/// Bounded leader-only replica selector.
pub struct ReplicaSelector;

impl ReplicaSelector {
    /// Selects a valid leader and rejects every unsupported replica path.
    pub fn select_leader<'a>(
        location: &'a RegionLocation,
        policy: ReadPolicy,
    ) -> Result<LeaderRoute<'a>, RegionRouteError> {
        if policy.mode != ReplicaReadMode::Leader || policy.stale_read || policy.forwarding {
            return Err(RegionRouteError::UnsupportedReadPolicy);
        }
        let leader_id = location
            .leader_peer_id
            .ok_or(RegionRouteError::MissingLeader)?;
        let peer = location
            .peers
            .iter()
            .find(|peer| peer.id == leader_id)
            .ok_or(RegionRouteError::MissingLeader)?;
        let store = location
            .stores
            .iter()
            .find(|store| store.id == peer.store_id)
            .ok_or(RegionRouteError::MissingStore(peer.store_id))?;
        if store.address.is_empty() {
            return Err(RegionRouteError::MissingAddress(store.id));
        }
        if peer.store_epoch != store.epoch {
            return Err(RegionRouteError::StaleStoreEpoch {
                store_id: store.id,
                expected: peer.store_epoch,
                actual: store.epoch,
            });
        }
        Ok(LeaderRoute { peer, store })
    }
}
