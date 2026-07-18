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

use super::{
    KeyRange, LeaderRequest, OwnedLeaderRoute, Peer, PeerRole, ReadPolicy, RegionAttempt,
    RegionLoadError, RegionLocation, RegionMetadata, RegionRebuildAction, RegionRecoveryError,
    RegionRouteError, RegionVerId, ReplicaReadMode, RequestSelection, RequestSelector, Store,
    StoreFailureOutcome, StoreLiveness, StoreResolveState, StoreState, MAX_REPLICA_ATTEMPTS,
    MAX_REPLICA_ATTEMPT_TIME,
};

/// Injected PD-shaped region metadata loader.
pub trait RegionLoader {
    /// Returns the cluster identity attached to requests routed by this loader.
    fn cluster_id(&self) -> u64;

    /// Loads the region containing `key` without prescribing any network API.
    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError>;
}

/// Region loader with the required current-region store hydration capability.
pub trait RegionRecoveryLoader: RegionLoader {
    /// Resolves the stores referenced by TiKV-provided current-region metadata.
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError>;
}

/// Ordered cache for versioned region snapshots.
pub struct RegionCache<L> {
    pub(super) loader: L,
    pub(super) regions: Vec<RegionLocation>,
    pub(super) stores: BTreeMap<u64, StoreState>,
}

impl<L> RegionCache<L> {
    /// Creates an empty cache over an injected loader.
    #[must_use]
    pub const fn new(loader: L) -> Self {
        Self {
            loader,
            regions: Vec::new(),
            stores: BTreeMap::new(),
        }
    }

    /// Returns the number of cached region snapshots.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.regions.len()
    }

    /// Returns whether the cache is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    /// Returns the cluster identity supplied by the metadata loader.
    pub fn cluster_id(&self) -> u64
    where
        L: RegionLoader,
    {
        self.loader.cluster_id()
    }

    /// Invalidates only the exact versioned region identity.
    pub fn invalidate(&mut self, region: RegionVerId) -> bool {
        let original_len = self.regions.len();
        self.regions.retain(|cached| cached.region != region);
        self.regions.len() != original_len
    }

    pub(super) fn validate_attempt(
        &self,
        attempt: &RegionAttempt,
    ) -> Result<(), RegionRecoveryError> {
        let Some(location) = self
            .regions
            .iter()
            .find(|location| location.region == attempt.region)
        else {
            return Err(RegionRecoveryError::StaleObservation(attempt.clone()));
        };
        let peer_matches = location.peers.iter().any(|peer| {
            peer.id == attempt.peer_id
                && peer.store_id == attempt.store_id
                && peer.store_epoch == attempt.store_epoch
        });
        let store_matches = self.stores.get(&attempt.store_id).is_some_and(|store| {
            store.address == attempt.address && store.epoch == attempt.store_epoch
        });
        if !peer_matches || !store_matches {
            return Err(RegionRecoveryError::StaleObservation(attempt.clone()));
        }
        Ok(())
    }

    /// Returns one immutable view of the canonical store authority.
    #[must_use]
    pub fn store_state(&self, store_id: u64) -> Option<&StoreState> {
        self.stores.get(&store_id)
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
        Ok(StoreFailureOutcome::Invalidated {
            previous_epoch,
            current_epoch: store.epoch,
        })
    }

    /// Creates a request-scoped selector over one exact cached region.
    pub fn request_selector(
        &self,
        region: RegionVerId,
        policy: ReadPolicy,
    ) -> Result<RequestSelector, RegionRouteError> {
        if policy.forwarding || (policy.stale_read && policy.mode != ReplicaReadMode::Mixed) {
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
        if selector.policy.forwarding
            || (selector.policy.stale_read && selector.policy.mode != ReplicaReadMode::Mixed)
        {
            return Err(RegionRouteError::UnsupportedReadPolicy);
        }
        if let Some(pending) = &selector.pending_attempt {
            return Err(RegionRouteError::AttemptStillPending {
                region: pending.region,
                peer_id: pending.peer_id,
            });
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
        let selected = if selector.policy.mode == ReplicaReadMode::Leader {
            self.select_leader_semantics(selector, location, leader_peer_id)?
        } else {
            self.select_replica_read(selector, location, leader_peer_id)?
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
        selector.record_dispatch(attempt.clone());
        Ok(RequestSelection::Attempt(LeaderRequest {
            attempt,
            proxy: None,
            role: peer.role,
            is_witness: peer.is_witness,
            replica_read,
            stale_read,
            cached_leader,
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
        selector: &RequestSelector,
        location: &RegionLocation,
        leader_peer_id: Option<u64>,
    ) -> Result<Option<Peer>, RegionRouteError> {
        let leader = leader_peer_id
            .and_then(|peer_id| location.peers.iter().find(|peer| peer.id == peer_id));
        if let Some(peer) = leader {
            if selector.attempts_for(peer.id) < MAX_REPLICA_ATTEMPTS
                && selector.attempted_time_for(peer.id) < MAX_REPLICA_ATTEMPT_TIME
                && self.peer_is_candidate(peer, true, false)?
            {
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

    fn select_replica_read(
        &self,
        selector: &RequestSelector,
        location: &RegionLocation,
        leader_peer_id: Option<u64>,
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
            let score = replica_score(selector, peer, is_leader);
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

    pub(super) fn update_leader(
        &mut self,
        region: RegionVerId,
        peer_id: u64,
        store_id: u64,
    ) -> bool {
        let Some(location) = self
            .regions
            .iter_mut()
            .find(|location| location.region == region)
        else {
            return false;
        };
        let usable = location
            .peers
            .iter()
            .any(|peer| peer.id == peer_id && peer.store_id == store_id)
            && self.stores.get(&store_id).is_some_and(|store| {
                !store.address.is_empty()
                    && store.resolve_state == StoreResolveState::Resolved
                    && location.peers.iter().any(|peer| {
                        peer.id == peer_id
                            && peer.store_id == store.id
                            && peer.store_epoch == store.epoch
                    })
            });
        if usable {
            location.leader_peer_id = Some(peer_id);
        }
        usable
    }

    pub(super) fn owned_leader_route(
        &self,
        region: RegionVerId,
    ) -> Result<OwnedLeaderRoute, RegionRouteError> {
        let location = self
            .regions
            .iter()
            .find(|location| location.region == region)
            .ok_or(RegionRouteError::MissingLeader)?;
        let peer_id = location
            .leader_peer_id
            .ok_or(RegionRouteError::MissingLeader)?;
        let peer = location
            .peers
            .iter()
            .find(|peer| peer.id == peer_id)
            .ok_or(RegionRouteError::MissingLeader)?;
        let store = self
            .stores
            .get(&peer.store_id)
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
        Ok(OwnedLeaderRoute {
            region,
            peer_id,
            store_id: store.id,
            address: store.address.clone(),
            store_epoch: store.epoch,
        })
    }

    pub(super) fn replace_regions_atomically(
        &mut self,
        observed: RegionVerId,
        mut replacements: Vec<RegionLocation>,
    ) -> Result<(), RegionRouteError> {
        replacements.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        for replacement in &replacements {
            if !replacement.end_key.is_empty() && replacement.start_key >= replacement.end_key {
                return Err(RegionRouteError::InvalidRegionBounds {
                    region: replacement.region,
                });
            }
        }
        for (index, left) in replacements.iter().enumerate() {
            if replacements[index + 1..]
                .iter()
                .any(|right| right.region.id == left.region.id)
            {
                return Err(RegionRouteError::DuplicateReplacementRegion {
                    region: left.region,
                });
            }
        }
        let mut next = self.regions.clone();
        let mut next_stores = self.stores.clone();
        next.retain(|location| location.region != observed);
        for mut replacement in replacements {
            normalize_loaded(&mut next_stores, &mut replacement);
            insert_loaded_into(&mut next, replacement)?;
        }
        self.regions = next;
        self.stores = next_stores;
        Ok(())
    }

    /// Applies the topology mutation intentionally deferred until after sleep.
    pub fn apply_rebuild_action(
        &mut self,
        action: RegionRebuildAction,
    ) -> Result<(), RegionRecoveryError> {
        match action {
            RegionRebuildAction::CacheReady => Ok(()),
        }
    }

    /// Finds one key, loading and inserting on a miss.
    pub fn locate_key(&mut self, key: &[u8]) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        self.locate_key_with_boundary(key, false)
    }

    fn locate_key_with_boundary(
        &mut self,
        key: &[u8],
        require_exact_start: bool,
    ) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        if let Some(index) = self.find_key(key) {
            if require_exact_start && self.regions[index].start_key != key {
                return Err(RegionRouteError::DiscontinuousRegion {
                    region: self.regions[index].region,
                });
            }
            return Ok(&self.regions[index]);
        }
        let loaded = self
            .loader
            .load_region(key)
            .map_err(RegionRouteError::Loader)?;
        if !loaded.end_key.is_empty() && loaded.start_key >= loaded.end_key {
            return Err(RegionRouteError::InvalidRegionBounds {
                region: loaded.region,
            });
        }
        if require_exact_start && loaded.start_key != key {
            return Err(RegionRouteError::DiscontinuousRegion {
                region: loaded.region,
            });
        }
        if !loaded.contains_key(key) {
            return Err(RegionRouteError::LoadedRegionDoesNotContainKey {
                region: loaded.region,
            });
        }
        let index = self.insert_loaded(loaded)?;
        Ok(&self.regions[index])
    }

    /// Locates a range and rejects any cross-region request.
    pub fn locate_range(&mut self, range: &KeyRange) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        if !range.is_valid() {
            return Err(RegionRouteError::InvalidRange);
        }
        let location = self.locate_key(&range.start)?;
        if !location.contains_range(range) {
            return Err(RegionRouteError::MultiRegion);
        }
        Ok(location)
    }

    /// Resolves every region intersecting the supplied half-open ranges.
    ///
    /// Returned snapshots are unique by exact versioned identity and sorted by
    /// region start key. Overlapping caller ranges therefore reuse the cache
    /// instead of loading or dispatching the same region twice.
    pub fn locate_ranges(
        &mut self,
        ranges: &[KeyRange],
    ) -> Result<Vec<RegionLocation>, RegionRouteError>
    where
        L: RegionLoader,
    {
        let mut located = BTreeMap::<RegionVerId, RegionLocation>::new();
        for range in ranges {
            if !range.is_valid() {
                return Err(RegionRouteError::InvalidRange);
            }
            let mut cursor = range.start.clone();
            let mut first_fragment = true;
            loop {
                let location = self
                    .locate_key_with_boundary(&cursor, !first_fragment)?
                    .clone();
                let region = location.region;
                let region_end = location.end_key.clone();
                located.entry(region).or_insert(location);

                let request_is_covered = if range.end.is_empty() {
                    region_end.is_empty()
                } else {
                    region_end.is_empty() || range.end <= region_end
                };
                if request_is_covered {
                    break;
                }
                if region_end <= cursor {
                    return Err(RegionRouteError::NonProgressingRegion { region });
                }
                cursor = region_end;
                first_fragment = false;
            }
        }

        let mut regions: Vec<_> = located.into_values().collect();
        regions.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        Ok(regions)
    }

    fn find_key(&self, key: &[u8]) -> Option<usize> {
        self.regions
            .binary_search_by(|region| {
                if region.contains_key(key) {
                    std::cmp::Ordering::Equal
                } else if region.start_key.as_slice() > key {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .ok()
    }

    fn insert_loaded(&mut self, mut loaded: RegionLocation) -> Result<usize, RegionRouteError> {
        let mut next_regions = self.regions.clone();
        let mut next_stores = self.stores.clone();
        normalize_loaded(&mut next_stores, &mut loaded);
        let index = insert_loaded_into(&mut next_regions, loaded)?;
        self.regions = next_regions;
        self.stores = next_stores;
        Ok(index)
    }
}

fn replica_score(selector: &RequestSelector, peer: &Peer, is_leader: bool) -> u8 {
    const NOT_ATTEMPTED: u8 = 1;
    const NORMAL_PEER: u8 = 1 << 1;
    const PREFER_LEADER: u8 = 1 << 2;

    let mut score = 0;
    if selector.attempts_for(peer.id) == 0 {
        score |= NOT_ATTEMPTED;
    }
    if is_leader {
        match selector.policy.mode {
            ReplicaReadMode::Mixed => score |= NORMAL_PEER,
            ReplicaReadMode::PreferLeader => score |= PREFER_LEADER,
            ReplicaReadMode::Leader | ReplicaReadMode::Follower | ReplicaReadMode::Learner => {}
        }
    } else if selector.policy.mode != ReplicaReadMode::Learner || peer.role == PeerRole::Learner {
        score |= NORMAL_PEER;
    }
    score
}

fn request_flags(selector: &RequestSelector, cached_leader: bool) -> (bool, bool) {
    if selector.policy.mode == ReplicaReadMode::Leader {
        return (false, false);
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

fn normalize_loaded(stores: &mut BTreeMap<u64, StoreState>, loaded: &mut RegionLocation) {
    for supplied in &loaded.stores {
        match stores.get_mut(&supplied.id) {
            None => {
                stores.insert(
                    supplied.id,
                    StoreState {
                        id: supplied.id,
                        address: supplied.address.clone(),
                        epoch: supplied.epoch,
                        resolve_state: StoreResolveState::Resolved,
                        liveness: StoreLiveness::Reachable,
                    },
                );
            }
            Some(canonical) => {
                let address_changed = canonical.address != supplied.address;
                if address_changed && canonical.resolve_state == StoreResolveState::Resolved {
                    canonical.epoch = canonical.epoch.saturating_add(1);
                }
                canonical.address.clone_from(&supplied.address);
                canonical.resolve_state = StoreResolveState::Resolved;
            }
        }
    }

    for peer in &mut loaded.peers {
        if let Some(store) = stores.get(&peer.store_id) {
            peer.store_epoch = store.epoch;
        }
    }
    loaded.stores = loaded
        .peers
        .iter()
        .filter_map(|peer| stores.get(&peer.store_id))
        .map(|store| Store {
            id: store.id,
            address: store.address.clone(),
            epoch: store.epoch,
        })
        .fold(Vec::new(), |mut snapshots, store| {
            if !snapshots
                .iter()
                .any(|current: &Store| current.id == store.id)
            {
                snapshots.push(store);
            }
            snapshots
        });
}

fn insert_loaded_into(
    regions: &mut Vec<RegionLocation>,
    loaded: RegionLocation,
) -> Result<usize, RegionRouteError> {
    if let Some(current) = regions
        .iter()
        .find(|region| region.region.id == loaded.region.id)
    {
        if loaded.region.epoch.is_older_than(current.region.epoch) {
            return Err(RegionRouteError::StaleRegionEpoch {
                loaded: loaded.region,
                cached: current.region,
            });
        }
    }
    if let Some(current) = regions.iter().find(|current| {
        ranges_intersect(current, &loaded)
            && current.region.epoch.version > loaded.region.epoch.version
    }) {
        return Err(RegionRouteError::StaleRegionEpoch {
            loaded: loaded.region,
            cached: current.region,
        });
    }

    regions.retain(|current| {
        current.region.id != loaded.region.id && !ranges_intersect(current, &loaded)
    });
    let index = regions
        .binary_search_by(|region| region.start_key.cmp(&loaded.start_key))
        .unwrap_or_else(|index| index);
    regions.insert(index, loaded);
    Ok(index)
}

fn ranges_intersect(left: &RegionLocation, right: &RegionLocation) -> bool {
    let left_before_right = !left.end_key.is_empty() && left.end_key <= right.start_key;
    let right_before_left = !right.end_key.is_empty() && right.end_key <= left.start_key;
    !left_before_right && !right_before_left
}
