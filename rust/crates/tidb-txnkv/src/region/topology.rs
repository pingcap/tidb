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

use std::ops::{Deref, DerefMut};

use super::{PeerRole, RegionAttempt, RegionVerId, StoreState};

/// One immutable peer/store topology view used by request-local policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutePeer {
    attempt: RegionAttempt,
    role: PeerRole,
    is_witness: bool,
    cached_leader: bool,
    labels: Vec<(String, String)>,
}

impl RoutePeer {
    pub(crate) fn new(
        attempt: RegionAttempt,
        role: PeerRole,
        is_witness: bool,
        cached_leader: bool,
        labels: Vec<(String, String)>,
    ) -> Self {
        Self {
            attempt,
            role,
            is_witness,
            cached_leader,
            labels,
        }
    }

    /// Exact peer and store generation captured by this snapshot.
    #[must_use]
    pub const fn attempt(&self) -> &RegionAttempt {
        &self.attempt
    }

    /// Raft role copied from the region metadata.
    #[must_use]
    pub const fn role(&self) -> PeerRole {
        self.role
    }

    /// Whether this peer is read/write prohibited unless it is the leader.
    #[must_use]
    pub const fn is_witness(&self) -> bool {
        self.is_witness
    }

    /// Whether this peer was the cached leader when the snapshot was built.
    #[must_use]
    pub const fn cached_leader(&self) -> bool {
        self.cached_leader
    }

    /// Exact PD labels attached to the canonical store metadata.
    #[must_use]
    pub fn labels(&self) -> &[(String, String)] {
        &self.labels
    }

    /// Pinned client-go subset matching: every requested key/value pair must
    /// occur in the store labels; an empty request matches every store.
    #[must_use]
    pub fn labels_match(&self, requested: &[(String, String)]) -> bool {
        requested.iter().all(|target| self.labels.contains(target))
    }
}

/// Immutable topology facts for one exact versioned region.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteSnapshot {
    region: RegionVerId,
    peers: Vec<RoutePeer>,
    preferred_proxy: Option<RegionAttempt>,
}

impl RouteSnapshot {
    pub(crate) fn new(
        region: RegionVerId,
        peers: Vec<RoutePeer>,
        preferred_proxy: Option<RegionAttempt>,
    ) -> Self {
        Self {
            region,
            peers,
            preferred_proxy,
        }
    }

    /// Exact versioned region captured by this snapshot.
    #[must_use]
    pub const fn region(&self) -> RegionVerId {
        self.region
    }

    /// Peers in PD metadata order.
    #[must_use]
    pub fn peers(&self) -> &[RoutePeer] {
        &self.peers
    }

    /// Preferred physical proxy previously proven for this region.
    #[must_use]
    pub const fn preferred_proxy(&self) -> Option<&RegionAttempt> {
        self.preferred_proxy.as_ref()
    }
}

/// Canonical store metadata owned by `RegionCache`.
///
/// The existing store failure state and PD labels live in one record so a
/// selector never has to join two independently mutable store authorities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegionStoreTopology {
    state: StoreState,
    labels: Vec<(String, String)>,
}

impl RegionStoreTopology {
    pub(crate) fn new(state: StoreState, labels: Vec<(String, String)>) -> Self {
        Self { state, labels }
    }

    pub(crate) const fn state(&self) -> &StoreState {
        &self.state
    }

    pub(crate) fn replace_labels(&mut self, labels: Vec<(String, String)>) {
        self.labels = labels;
    }

    pub(crate) fn labels(&self) -> &[(String, String)] {
        &self.labels
    }
}

impl Deref for RegionStoreTopology {
    type Target = StoreState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for RegionStoreTopology {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

/// Result of applying one generation-checked route observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteFeedbackApplication {
    /// A successful forwarded route became the region preference.
    ProxyPublished,
    /// A failed proxy or successful direct route removed the preference.
    ProxyCleared,
    /// The observation did not change the canonical preference.
    Unchanged,
}
