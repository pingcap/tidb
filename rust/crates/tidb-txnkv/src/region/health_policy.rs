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

    fn insert(&mut self, flag: Self) {
        self.0 |= flag.0;
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
    /// Whether this request observed `ServerIsBusy` from the peer.
    pub reported_busy: bool,
    /// Store-owned health detail.
    pub health: StoreHealthDetail,
    /// Store-owned decaying load.
    pub load: StoreLoad,
}

impl ReplicaHealthPolicy {
    /// Selects the highest-scoring candidate with deterministic seeded ties.
    ///
    /// The returned index refers to the immutable route snapshot supplied by
    /// RegionCache. An empty result under a positive busy threshold means the
    /// caller must clear that request-owned threshold and retry the leader
    /// without invalidating the region.
    #[must_use]
    pub fn select(
        &self,
        replicas: &[ReplicaHealthFacts<'_>],
        now: HealthInstant,
        selection_seed: u32,
    ) -> Option<usize> {
        let mut best_score = None;
        let mut best = Vec::new();
        for (index, facts) in replicas.iter().copied().enumerate() {
            if !self.is_candidate(facts, now) {
                continue;
            }
            let score = self.score(facts);
            match best_score {
                None => {
                    best_score = Some(score);
                    best.push(index);
                }
                Some(current) if score > current => {
                    best_score = Some(score);
                    best.clear();
                    best.push(index);
                }
                Some(current) if score == current => best.push(index),
                Some(_) => {}
            }
        }
        (!best.is_empty()).then(|| best[selection_seed as usize % best.len()])
    }

    /// Whether the candidate survives busy/slow compatibility filters.
    #[must_use]
    pub fn is_candidate(&self, facts: ReplicaHealthFacts<'_>, now: HealthInstant) -> bool {
        if !self.busy_threshold.is_zero()
            && (facts.load.estimated_wait(now) > self.busy_threshold
                || facts.reported_busy
                || facts.is_leader)
        {
            return false;
        }
        !(self.prefer_leader && facts.health.is_slow() && !facts.is_leader)
    }

    /// Calculates the exact ordered five-bit source score.
    #[must_use]
    pub fn score(&self, facts: ReplicaHealthFacts<'_>) -> StoreSelectionScore {
        let mut score = StoreSelectionScore::default();
        if self.matches_store(facts.store_id) && self.matches_labels(facts.labels) {
            score.insert(StoreSelectionScore::LABEL_MATCHES);
        }
        if facts.is_leader {
            if self.prefer_leader {
                score.insert(if facts.health.is_slow() {
                    StoreSelectionScore::NORMAL_PEER
                } else {
                    StoreSelectionScore::PREFER_LEADER
                });
            } else if self.try_leader {
                score.insert(if self.labels.is_empty() {
                    StoreSelectionScore::NORMAL_PEER
                } else {
                    StoreSelectionScore::PREFER_LEADER
                });
            }
        } else if !self.learner_only || facts.is_learner {
            score.insert(StoreSelectionScore::NORMAL_PEER);
        }
        if !facts.health.is_slow() {
            score.insert(StoreSelectionScore::NOT_SLOW);
        }
        if facts.attempts == 0 {
            score.insert(StoreSelectionScore::NOT_ATTEMPTED);
        }
        score
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
