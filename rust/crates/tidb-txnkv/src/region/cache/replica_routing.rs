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
//! Which peer of a region serves the next request, and how each observed
//! failure moves that choice.
//!
//! Go boundary: client-go's `region_request.go` replica selector — leader-first
//! semantics, forwarding through a proxy when the leader is busy, replica-read
//! candidate filtering by label and health, and the `ServerIsBusy` /
//! send-failure feedback that demotes a peer. Every decision here is validated
//! against the cache's current epoch, so a route built from a stale attempt is
//! rejected rather than retried.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::super::{
    HealthInstant, LeaderRequest, Peer, PeerRole, ReadPolicy, RegionAttempt,
    RegionAttemptObservation, RegionLocation, RegionRecoveryError, RegionRouteError, RegionVerId,
    ReplicaHealthFacts, ReplicaReadMode, RequestSelection, RequestSelector, RouteFeedback,
    RouteFeedbackApplication, RouteOutcome, RoutePeer, RouteSnapshot, ServerBusyAction,
    StoreFailureOutcome, StoreLabel, StoreLiveness, StoreResolveState, MAX_REPLICA_ATTEMPTS,
    MAX_REPLICA_ATTEMPT_TIME,
};
use super::RegionCache;

impl<L> RegionCache<L> {
    /// Returns the currently reusable proxy for one exact region.
    #[must_use]
    pub fn preferred_proxy(&self, region: RegionVerId) -> Option<&RegionAttempt> {
        self.preferred_proxies
            .get(&region)
            .filter(|proxy| self.validate_attempt(proxy).is_ok())
    }

    /// Copies one immutable routing view from the sole mutable cache authority.
    pub fn route_snapshot(&self, region: RegionVerId) -> Result<RouteSnapshot, RegionRouteError> {
        let location = self
            .regions
            .iter()
            .find(|location| location.region == region)
            .ok_or(RegionRouteError::MissingLeader)?;
        let peers = location
            .peers
            .iter()
            .map(|peer| {
                let store = self
                    .stores
                    .get(&peer.store_id)
                    .ok_or(RegionRouteError::MissingStore(peer.store_id))?;
                if peer.store_epoch != store.epoch {
                    return Err(RegionRouteError::StaleStoreEpoch {
                        store_id: peer.store_id,
                        expected: peer.store_epoch,
                        actual: store.epoch,
                    });
                }
                Ok(RoutePeer::new(
                    RegionAttempt {
                        region,
                        peer_id: peer.id,
                        store_id: peer.store_id,
                        address: store.address.clone(),
                        store_epoch: store.epoch,
                    },
                    peer.role,
                    peer.is_witness,
                    location.leader_peer_id == Some(peer.id),
                    store.labels().to_vec(),
                ))
            })
            .collect::<Result<Vec<_>, RegionRouteError>>()?;
        let preferred_proxy = self.preferred_proxy(region).cloned();
        Ok(RouteSnapshot::new(region, peers, preferred_proxy))
    }

    /// Applies a transport result only when both captured generations still
    /// belong to this exact region topology.
    pub fn apply_route_feedback(
        &mut self,
        feedback: &RouteFeedback,
    ) -> Result<RouteFeedbackApplication, RegionRecoveryError> {
        self.validate_attempt(feedback.target())?;
        let target_is_leader = self.regions.iter().any(|location| {
            location.region == feedback.target().region
                && location.leader_peer_id == Some(feedback.target().peer_id)
        });
        if !target_is_leader {
            return Err(RegionRecoveryError::StaleObservation(
                feedback.target().clone(),
            ));
        }
        if let Some(proxy) = feedback.proxy() {
            self.validate_attempt(proxy)?;
            if proxy.region != feedback.target().region
                || proxy.peer_id == feedback.target().peer_id
                || proxy.store_id == feedback.target().store_id
            {
                return Err(RegionRecoveryError::StaleObservation(proxy.clone()));
            }
        }

        let region = feedback.target().region;
        match (feedback.proxy(), feedback.outcome()) {
            (Some(proxy), RouteOutcome::Success) => {
                if self.preferred_proxies.get(&region) == Some(proxy) {
                    Ok(RouteFeedbackApplication::Unchanged)
                } else {
                    self.preferred_proxies.insert(region, proxy.clone());
                    Ok(RouteFeedbackApplication::ProxyPublished)
                }
            }
            (Some(proxy), RouteOutcome::Failure) => {
                if self.preferred_proxies.get(&region) == Some(proxy) {
                    self.preferred_proxies.remove(&region);
                    Ok(RouteFeedbackApplication::ProxyCleared)
                } else {
                    Ok(RouteFeedbackApplication::Unchanged)
                }
            }
            (None, RouteOutcome::Success) => {
                if self.preferred_proxies.remove(&region).is_some() {
                    Ok(RouteFeedbackApplication::ProxyCleared)
                } else {
                    Ok(RouteFeedbackApplication::Unchanged)
                }
            }
            (None, RouteOutcome::Failure) => Ok(RouteFeedbackApplication::Unchanged),
        }
    }

    /// Applies one exact foreground send-failure observation.
    ///
    /// A delayed address or epoch cannot mutate a newer generation. Both
    /// `Unreachable` and `Unknown` fail closed: client-go probes another peer
    /// instead of treating an inconclusive health request as proof of health.
    pub fn on_send_failure(
        &mut self,
        attempt: &RegionAttempt,
        liveness: StoreLiveness,
    ) -> Result<StoreFailureOutcome, RegionRecoveryError> {
        self.validate_attempt(attempt)?;
        let store = self
            .stores
            .get_mut(&attempt.store_id)
            .expect("validated attempt has a canonical store");
        store.liveness = liveness;
        if liveness == StoreLiveness::Reachable {
            return Ok(StoreFailureOutcome::Reachable { epoch: store.epoch });
        }
        let previous_epoch = store.epoch;
        store.epoch = store.epoch.saturating_add(1);
        store.resolve_state = StoreResolveState::NeedCheck;
        let current_epoch = store.epoch;
        self.advance_store_revision();
        self.preferred_proxies.retain(|_, proxy| {
            proxy.store_id != attempt.store_id || proxy.store_epoch != previous_epoch
        });
        Ok(StoreFailureOutcome::Invalidated {
            previous_epoch,
            current_epoch,
        })
    }

    /// Issues an opaque dispatch observation over the selectable peer vector.
    pub fn observe_attempt(
        &self,
        attempt: &RegionAttempt,
    ) -> Result<RegionAttemptObservation, RegionRecoveryError> {
        self.validate_attempt(attempt)?;
        let location = self
            .regions
            .iter()
            .find(|location| location.region == attempt.region)
            .expect("validated attempt has a canonical region");
        Ok(RegionAttemptObservation::new(
            attempt.clone(),
            selectable_peer_count(location),
        ))
    }

    /// Applies a send failure only when the cache-issued selectable peer-vector
    /// width still matches the topology observed at dispatch.
    pub fn on_send_failure_observed(
        &mut self,
        observation: &RegionAttemptObservation,
        liveness: StoreLiveness,
    ) -> Result<StoreFailureOutcome, RegionRecoveryError> {
        self.validate_attempt_observation(observation)?;
        self.on_send_failure(observation.attempt(), liveness)
    }

    fn validate_attempt_observation(
        &self,
        observation: &RegionAttemptObservation,
    ) -> Result<(), RegionRecoveryError> {
        let attempt = observation.attempt();
        self.validate_attempt(attempt)?;
        let location = self
            .regions
            .iter()
            .find(|location| location.region == attempt.region)
            .expect("validated attempt has a canonical region");
        if selectable_peer_count(location) != observation.selectable_peer_count() {
            return Err(RegionRecoveryError::StaleObservation(attempt.clone()));
        }
        Ok(())
    }

    /// Validates a route observation without mutating cache or retry state.
    pub fn validate_route_observation(
        &self,
        request: &LeaderRequest,
        observation: &RegionAttemptObservation,
    ) -> Result<(), RegionRecoveryError> {
        if observation.attempt() != request.dispatch_attempt() {
            return Err(RegionRecoveryError::StaleObservation(
                observation.attempt().clone(),
            ));
        }
        self.validate_attempt_observation(observation)
    }

    /// Applies one request-scoped busy observation to the canonical store.
    pub fn on_server_busy(
        &mut self,
        selector: &mut RequestSelector,
        attempt: &RegionAttempt,
        estimated_wait_ms: u32,
        now: HealthInstant,
    ) -> Result<ServerBusyAction, RegionRecoveryError> {
        self.validate_attempt(attempt)?;
        if selector.region != attempt.region || selector.completed_attempt.as_ref() != Some(attempt)
        {
            return Err(RegionRecoveryError::StaleObservation(attempt.clone()));
        }
        let action = selector.record_server_busy(attempt.peer_id, estimated_wait_ms);
        self.stores
            .get_mut(&attempt.store_id)
            .expect("validated attempt has a canonical store")
            .routing_health
            .observe_server_busy(estimated_wait_ms, now);
        Ok(action)
    }

    /// Applies failure to the physical dispatch while preserving a failed
    /// leader generation long enough to route that same target through a
    /// healthy proxy.
    pub fn on_route_send_failure(
        &mut self,
        request: &LeaderRequest,
        liveness: StoreLiveness,
    ) -> Result<StoreFailureOutcome, RegionRecoveryError> {
        let feedback = RouteFeedback::from_request(request, RouteOutcome::Failure);
        if request.proxy().is_some()
            || (request.cached_leader && request.read_mode == ReplicaReadMode::Leader)
        {
            self.apply_route_feedback(&feedback)?;
        } else {
            self.validate_attempt(feedback.target())?;
        }
        if request.proxy().is_none()
            && request.forwarding
            && request.cached_leader
            && request.read_mode == ReplicaReadMode::Leader
            && liveness != StoreLiveness::Reachable
            && self.has_forwarding_proxy(request.target())?
        {
            let store = self
                .stores
                .get_mut(&request.target().store_id)
                .expect("validated route target has a canonical store");
            store.liveness = liveness;
            return Ok(StoreFailureOutcome::ForwardingRequired { epoch: store.epoch });
        }
        self.on_send_failure(feedback.dispatch_attempt(), liveness)
    }

    /// Applies one production route failure only when its selection-time peer
    /// vector still describes the canonical region. Validation precedes proxy,
    /// store, leader, reload, and liveness mutation.
    pub fn on_route_send_failure_observed(
        &mut self,
        request: &LeaderRequest,
        observation: &RegionAttemptObservation,
        liveness: StoreLiveness,
    ) -> Result<StoreFailureOutcome, RegionRecoveryError> {
        self.validate_route_observation(request, observation)?;
        self.on_route_send_failure(request, liveness)
    }

    /// Publishes one usable route and marks its physical dispatch reachable.
    pub fn on_route_success(
        &mut self,
        request: &LeaderRequest,
    ) -> Result<RouteFeedbackApplication, RegionRecoveryError> {
        let feedback = RouteFeedback::from_request(request, RouteOutcome::Success);
        let application = if request.proxy().is_some()
            || (request.cached_leader && request.read_mode == ReplicaReadMode::Leader)
        {
            self.apply_route_feedback(&feedback)?
        } else {
            self.validate_attempt(feedback.target())?;
            RouteFeedbackApplication::Unchanged
        };
        self.stores
            .get_mut(&feedback.dispatch_attempt().store_id)
            .expect("validated dispatch has a canonical store")
            .liveness = StoreLiveness::Reachable;
        Ok(application)
    }

    /// Creates a request-scoped selector over one exact cached region.
    pub fn request_selector(
        &self,
        region: RegionVerId,
        policy: ReadPolicy,
    ) -> Result<RequestSelector, RegionRouteError> {
        if policy.stale_read && policy.mode != ReplicaReadMode::Mixed {
            return Err(RegionRouteError::UnsupportedReadPolicy);
        }
        let Some(location) = self
            .regions
            .iter()
            .find(|location| location.region == region)
        else {
            return Err(RegionRouteError::MissingLeader);
        };
        Ok(RequestSelector::new(
            region,
            policy,
            location.leader_peer_id,
        ))
    }

    /// Selects the next source-shaped replica and invalidates on exhaustion.
    pub fn select_request(
        &mut self,
        selector: &mut RequestSelector,
    ) -> Result<RequestSelection, RegionRouteError> {
        self.select_request_at(selector, health_now())
    }

    /// Selects using an injected monotonic health instant.
    pub fn select_request_at(
        &mut self,
        selector: &mut RequestSelector,
        now: HealthInstant,
    ) -> Result<RequestSelection, RegionRouteError> {
        if selector.policy.stale_read && selector.policy.mode != ReplicaReadMode::Mixed {
            return Err(RegionRouteError::UnsupportedReadPolicy);
        }
        if let Some(pending) = &selector.pending_attempt {
            return Err(RegionRouteError::AttemptStillPending {
                region: pending.region,
                peer_id: pending.peer_id,
            });
        }
        if selector.policy.mode != ReplicaReadMode::Leader
            && self.region_has_stale_candidate_store(selector.region)
        {
            self.mark_delayed_reload(selector.region);
        }
        let Some(location) = self
            .regions
            .iter()
            .find(|location| location.region == selector.region)
        else {
            return Ok(RequestSelection::ReloadRegion {
                region: selector.region,
            });
        };

        let leader_peer_id = location.leader_peer_id;
        selector.observe_leader(leader_peer_id);
        let (selected, proxy) = if selector.policy.mode == ReplicaReadMode::Leader {
            let selected = if selector.policy.forwarding {
                self.select_forwarding_leader(selector, location, leader_peer_id, now)?
            } else {
                self.select_leader_semantics(selector, location, leader_peer_id, now)?
                    .map(|peer| (peer, None))
            };
            match selected {
                Some((peer, proxy)) => (Some(peer), proxy),
                None => (None, None),
            }
        } else {
            (
                self.select_replica_read(selector, location, leader_peer_id, now)?,
                None,
            )
        };

        let Some(peer) = selected else {
            let region = selector.region;
            self.invalidate(region);
            return Ok(RequestSelection::ReloadRegion { region });
        };
        let store = self
            .stores
            .get(&peer.store_id)
            .ok_or(RegionRouteError::MissingStore(peer.store_id))?;
        let attempt = RegionAttempt {
            region: selector.region,
            peer_id: peer.id,
            store_id: peer.store_id,
            address: store.address.clone(),
            store_epoch: store.epoch,
        };
        let cached_leader = Some(peer.id) == leader_peer_id;
        let (replica_read, stale_read) = request_flags(selector, cached_leader);
        selector.record_route_dispatch(attempt.clone(), proxy.as_ref());
        Ok(RequestSelection::Attempt(LeaderRequest {
            attempt,
            proxy,
            role: peer.role,
            is_witness: peer.is_witness,
            replica_read,
            stale_read,
            cached_leader,
            forwarding: selector.policy.forwarding,
            read_mode: selector.policy.mode,
        }))
    }

    /// Promotes an alternate peer only after a successful leader-semantics RPC.
    pub fn promote_successful_request(
        &mut self,
        request: &LeaderRequest,
    ) -> Result<bool, RegionRecoveryError> {
        if request.replica_read || request.stale_read {
            return Ok(false);
        }
        self.validate_attempt(&request.attempt)?;
        if request.cached_leader {
            return Ok(false);
        }
        Ok(self.update_leader(
            request.attempt.region,
            request.attempt.peer_id,
            request.attempt.store_id,
        ))
    }

    fn select_leader_semantics(
        &self,
        selector: &mut RequestSelector,
        location: &RegionLocation,
        leader_peer_id: Option<u64>,
        now: HealthInstant,
    ) -> Result<Option<Peer>, RegionRouteError> {
        let leader = leader_peer_id
            .and_then(|peer_id| location.peers.iter().find(|peer| peer.id == peer_id));
        if let Some(peer) = leader {
            if selector.attempts_for(peer.id) < MAX_REPLICA_ATTEMPTS
                && selector.attempted_time_for(peer.id) < MAX_REPLICA_ATTEMPT_TIME
                && self.peer_is_candidate(peer, true, false)?
            {
                if !self.leader_is_busy(selector, peer, now)? {
                    return Ok(Some(peer.clone()));
                }
                if let Some(idle) =
                    self.select_replica_read(selector, location, leader_peer_id, now)?
                {
                    return Ok(Some(idle));
                }
                let cleared = selector.clear_busy_threshold_for_leader_fallback();
                debug_assert!(cleared);
                return Ok(Some(peer.clone()));
            }
        }
        for peer in &location.peers {
            if Some(peer.id) != leader_peer_id
                && selector.attempts_for(peer.id) == 0
                && self.peer_is_candidate(peer, false, false)?
            {
                return Ok(Some(peer.clone()));
            }
        }
        Ok(None)
    }

    fn select_forwarding_leader(
        &self,
        selector: &mut RequestSelector,
        location: &RegionLocation,
        leader_peer_id: Option<u64>,
        now: HealthInstant,
    ) -> Result<Option<(Peer, Option<RegionAttempt>)>, RegionRouteError> {
        let Some(leader) = leader_peer_id
            .and_then(|peer_id| location.peers.iter().find(|peer| peer.id == peer_id))
        else {
            return Ok(None);
        };
        let target_store = self
            .stores
            .get(&leader.store_id)
            .ok_or(RegionRouteError::MissingStore(leader.store_id))?;
        if target_store.resolve_state != StoreResolveState::Resolved
            || target_store.address.is_empty()
            || leader.store_epoch != target_store.epoch
        {
            return Ok(None);
        }
        if target_store.liveness == StoreLiveness::Reachable {
            return self
                .select_leader_semantics(selector, location, leader_peer_id, now)
                .map(|peer| peer.map(|peer| (peer, None)));
        }

        let mut proxy_peer = None;
        if let Some(preferred) = self.preferred_proxies.get(&location.region) {
            if let Some(peer) = location.peers.iter().find(|peer| {
                peer.id == preferred.peer_id
                    && peer.store_id == preferred.store_id
                    && peer.store_epoch == preferred.store_epoch
            }) {
                if selector.attempts_for(peer.id) == 0
                    && self.peer_is_candidate(peer, false, true)?
                {
                    proxy_peer = Some(peer);
                }
            }
        }
        if proxy_peer.is_none() {
            for peer in &location.peers {
                if peer.id != leader.id
                    && selector.attempts_for(peer.id) == 0
                    && self.peer_is_candidate(peer, false, true)?
                {
                    proxy_peer = Some(peer);
                    break;
                }
            }
        }
        let Some(proxy_peer) = proxy_peer else {
            return Ok(None);
        };
        let proxy_store = self
            .stores
            .get(&proxy_peer.store_id)
            .ok_or(RegionRouteError::MissingStore(proxy_peer.store_id))?;
        Ok(Some((
            leader.clone(),
            Some(RegionAttempt {
                region: location.region,
                peer_id: proxy_peer.id,
                store_id: proxy_peer.store_id,
                address: proxy_store.address.clone(),
                store_epoch: proxy_store.epoch,
            }),
        )))
    }

    fn leader_is_busy(
        &self,
        selector: &RequestSelector,
        leader: &Peer,
        now: HealthInstant,
    ) -> Result<bool, RegionRouteError> {
        let threshold = selector.busy_threshold();
        if threshold.is_zero() {
            return Ok(false);
        }
        let store = self
            .stores
            .get(&leader.store_id)
            .ok_or(RegionRouteError::MissingStore(leader.store_id))?;
        Ok(store.routing_health.load.estimated_wait(now) > threshold
            || selector.peer_reported_busy(leader.id))
    }

    fn has_forwarding_proxy(&self, target: &RegionAttempt) -> Result<bool, RegionRecoveryError> {
        let Some(location) = self
            .regions
            .iter()
            .find(|location| location.region == target.region)
        else {
            return Ok(false);
        };
        for peer in &location.peers {
            if peer.id != target.peer_id && self.peer_is_candidate(peer, false, true)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn select_replica_read(
        &self,
        selector: &RequestSelector,
        location: &RegionLocation,
        leader_peer_id: Option<u64>,
        now: HealthInstant,
    ) -> Result<Option<Peer>, RegionRouteError> {
        if selector.policy.stale_read && selector.dispatches == 1 {
            if let Some(leader) = leader_peer_id
                .and_then(|peer_id| location.peers.iter().find(|peer| peer.id == peer_id))
            {
                if selector.attempts_for(leader.id) == 0
                    && self.peer_is_candidate(leader, true, false)?
                {
                    return Ok(Some(leader.clone()));
                }
            }
        }

        let mut best_score = None;
        let mut best = Vec::new();
        for peer in &location.peers {
            let is_leader = Some(peer.id) == leader_peer_id;
            if !self.peer_is_candidate(peer, is_leader, true)? {
                continue;
            }
            let max_attempts = if !is_leader && selector.may_retry_data_not_ready(peer.id) {
                2
            } else {
                1
            };
            if selector.attempts_for(peer.id) >= max_attempts {
                continue;
            }
            let store = self
                .stores
                .get(&peer.store_id)
                .ok_or(RegionRouteError::MissingStore(peer.store_id))?;
            let labels = store
                .labels()
                .iter()
                .map(|(key, value)| StoreLabel {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            let facts = ReplicaHealthFacts {
                store_id: peer.store_id,
                labels: &labels,
                is_leader,
                is_learner: peer.role == PeerRole::Learner,
                attempts: selector.attempts_for(peer.id),
                reported_busy: selector.peer_reported_busy(peer.id),
                health: store.routing_health.health.detail(),
                load: store.routing_health.load,
            };
            if !selector.health_policy.is_candidate(facts, now) {
                continue;
            }
            let score = selector.health_policy.score(facts);
            match best_score {
                None => {
                    best_score = Some(score);
                    best.push(peer.clone());
                }
                Some(current) if score > current => {
                    best_score = Some(score);
                    best.clear();
                    best.push(peer.clone());
                }
                Some(current) if score == current => best.push(peer.clone()),
                Some(_) => {}
            }
        }
        if best.is_empty() {
            return Ok(None);
        }
        let index = selector.policy.selection_seed as usize % best.len();
        Ok(Some(best.swap_remove(index)))
    }

    fn peer_is_candidate(
        &self,
        peer: &Peer,
        cached_leader: bool,
        replica_policy: bool,
    ) -> Result<bool, RegionRouteError> {
        if peer.is_witness {
            return Ok(false);
        }
        let voter = matches!(
            peer.role,
            PeerRole::Voter | PeerRole::IncomingVoter | PeerRole::DemotingVoter
        );
        if (!replica_policy && !voter)
            || (replica_policy && !voter && peer.role != PeerRole::Learner)
        {
            return Ok(false);
        }
        if cached_leader && !voter {
            return Ok(false);
        }
        let store = self
            .stores
            .get(&peer.store_id)
            .ok_or(RegionRouteError::MissingStore(peer.store_id))?;
        if store.resolve_state != StoreResolveState::Resolved
            || store.liveness == StoreLiveness::Unreachable
        {
            return Ok(false);
        }
        if store.address.is_empty() {
            return Err(RegionRouteError::MissingAddress(store.id));
        }
        if peer.store_epoch != store.epoch {
            return Ok(false);
        }
        Ok(true)
    }

    fn region_has_stale_candidate_store(&self, region: RegionVerId) -> bool {
        self.regions
            .iter()
            .find(|location| location.region == region)
            .is_some_and(|location| {
                location.peers.iter().any(|peer| {
                    self.stores.get(&peer.store_id).is_some_and(|store| {
                        peer.store_epoch != store.epoch
                            && ((store.liveness == StoreLiveness::Reachable
                                && store.resolve_state == StoreResolveState::Resolved)
                                || location.leader_peer_id == Some(peer.id))
                    })
                })
            })
    }
}

pub(super) fn health_now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
}

pub(super) fn selectable_peer_count(location: &RegionLocation) -> usize {
    location
        .peers
        .iter()
        .filter(|peer| {
            !location.down_peer_ids.contains(&peer.id)
                && (!peer.is_witness || location.leader_peer_id == Some(peer.id))
        })
        .count()
}

pub(super) fn request_flags(selector: &RequestSelector, cached_leader: bool) -> (bool, bool) {
    if selector.policy.mode == ReplicaReadMode::Leader {
        return (
            !cached_leader && !selector.busy_threshold().is_zero(),
            false,
        );
    }
    if !selector.policy.stale_read {
        return (!cached_leader, false);
    }
    if selector.dispatches == 0 {
        return (false, true);
    }
    if cached_leader
        && selector
            .leader_peer_id
            .is_some_and(|leader| selector.attempts_for(leader) == 0)
    {
        return (false, false);
    }
    if selector
        .leader_peer_id
        .is_some_and(|leader| selector.attempts_for(leader) > 0)
    {
        return (true, false);
    }
    (false, true)
}
