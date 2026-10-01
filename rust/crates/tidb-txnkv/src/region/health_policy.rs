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

use std::time::Duration;

use super::store_health::{HealthInstant, StoreHealthDetail, StoreLoad};
use crate::StoreLabel;
use tikv_client::kv::ReplicaReadType;
use tikv_client::tikv::{MixedReplicaSelection, ReplicaCandidate, ReplicaLiveness};

/// Exact five-bit store-selection score from pinned client-go.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct StoreSelectionScore(u8);

impl StoreSelectionScore {
    /// Not yet attempted, least-significant preference.
    pub const NOT_ATTEMPTED: Self = Self(1);
    /// Peer role is normal for the requested mode.
    pub const NORMAL_PEER: Self = Self(2);
    /// Leader preference.
    pub const PREFER_LEADER: Self = Self(4);
    /// Store and label constraints match.
    pub const LABEL_MATCHES: Self = Self(8);
    /// Store is not slow, most-significant preference.
    pub const NOT_SLOW: Self = Self(16);

    /// Raw source-shaped bitset.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

/// Pure policy applied to immutable replica facts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaHealthPolicy {
    /// Mixed read may select the leader.
    pub try_leader: bool,
    /// Prefer a healthy leader over followers.
    pub prefer_leader: bool,
    /// Only learners receive the normal-peer bit.
    pub learner_only: bool,
    /// Required labels. Empty means every store matches.
    pub labels: Vec<StoreLabel>,
    /// Allowed store IDs. Empty means every store matches.
    pub stores: Vec<u64>,
    /// Positive threshold used only for idle-replica diversion.
    pub busy_threshold: Duration,
}

/// Immutable facts for one replica candidate.
#[derive(Clone, Copy, Debug)]
pub struct ReplicaHealthFacts<'a> {
    /// Candidate's store ID.
    pub store_id: u64,
    /// Candidate labels.
    pub labels: &'a [StoreLabel],
    /// Whether this peer is the cached leader.
    pub is_leader: bool,
    /// Whether this peer is a learner.
    pub is_learner: bool,
    /// Number of request-local attempts.
    pub attempts: u8,
    /// One replica-read retry is allowed after DataIsNotReady.
    pub data_is_not_ready: bool,
    /// Whether this request observed `ServerIsBusy` from the peer.
    pub reported_busy: bool,
    /// Store-owned health detail.
    pub health: StoreHealthDetail,
    /// Store-owned decaying load.
    pub load: StoreLoad,
}

impl ReplicaHealthPolicy {
    /// Chooses among the highest-scored eligible replicas with client-go's
    /// per-selection random tie break.
    #[must_use]
    pub fn select(&self, replicas: &[ReplicaHealthFacts<'_>], now: HealthInstant) -> Option<usize> {
        let candidates: Vec<_> = replicas
            .iter()
            .copied()
            .enumerate()
            .map(|(index, facts)| self.candidate(index as u64, facts, now))
            .collect();
        self.selection()
            .choose(&candidates)
            .map(|candidate| candidate.peer_id as usize)
    }

    /// Delegates candidate eligibility to the native client owner.
    #[must_use]
    pub fn is_candidate(&self, facts: ReplicaHealthFacts<'_>, now: HealthInstant) -> bool {
        self.selection()
            .is_candidate(&self.candidate(0, facts, now))
    }

    /// Delegates the five-bit score to the native client owner.
    #[must_use]
    pub fn score(&self, facts: ReplicaHealthFacts<'_>) -> StoreSelectionScore {
        StoreSelectionScore(self.selection().calculate_score(&self.candidate(
            0,
            facts,
            HealthInstant::now(),
        )))
    }

    pub(crate) fn selection(&self) -> MixedReplicaSelection {
        MixedReplicaSelection {
            read_type: if self.learner_only {
                ReplicaReadType::Learner
            } else if self.try_leader {
                ReplicaReadType::Mixed
            } else {
                ReplicaReadType::Follower
            },
            leader_only: false,
            prefer_leader: self.prefer_leader,
            labels_requested: !self.labels.is_empty(),
            busy_threshold: self.busy_threshold,
        }
    }

    pub(crate) fn candidate(
        &self,
        peer_id: u64,
        facts: ReplicaHealthFacts<'_>,
        now: HealthInstant,
    ) -> ReplicaCandidate {
        ReplicaCandidate {
            peer_id,
            is_leader: facts.is_leader,
            is_learner: facts.is_learner,
            label_matches: self.matches_store(facts.store_id) && self.matches_labels(facts.labels),
            is_slow: facts.health.is_slow(),
            // Cache metadata validation has already excluded unreachable peers.
            liveness: ReplicaLiveness::Reachable,
            attempts: facts.attempts,
            data_is_not_ready: facts.data_is_not_ready,
            reported_busy: facts.reported_busy,
            estimated_wait: facts.load.estimated_wait(now),
        }
    }

    fn matches_store(&self, store_id: u64) -> bool {
        self.stores.is_empty() || self.stores.contains(&store_id)
    }

    fn matches_labels(&self, labels: &[StoreLabel]) -> bool {
        self.labels.iter().all(|required| {
            labels
                .iter()
                .any(|label| label.key == required.key && label.value == required.value)
        })
    }
}
