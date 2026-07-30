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

//! The ordered region cache itself: the entries, their TTL, and the atomic
//! topology replacement every lookup and every route decision reads through.
//!
//! Go boundary: client-go's `region_cache.go` — the `RegionCache` struct, its
//! `invalidate` / reload-state marks, the rotating GC that
//! `asyncCheckAndResolveLoop` drives, and `updateLeader`. The work each lookup
//! or route decision does on top of that lives in its own module:
//!
//! | module | subject | Go boundary |
//! | --- | --- | --- |
//! | [`loader`] | what the cache asks PD for, and the plan/apply split that lets a background thread do the asking | `region_cache.go` PD-client calls |
//! | [`lookup`] | resolving one region — by key, by end key, by range, by ID | `LocateKey`, `LocateEndKey`, `LocateRegionByID`, `insertRegionToCache` |
//! | [`scan`] | filling the cache in bulk from PD range scans | `scanRegions`, `BatchLocateKeyRanges` |
//! | [`replica_routing`] | which peer serves a request, and how a failure moves it | `region_request.go` replica selector |
//! | [`store_metadata`] | keeping the canonical store set and its liveness current | `region_cache.go` store reload / `checkUntilHealth` |

mod loader;
mod lookup;
mod replica_routing;
mod scan;
mod store_metadata;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{
    CacheEntryState, CacheReloadState, OwnedLeaderRoute, RegionAttempt, RegionLocation,
    RegionRebuildAction, RegionRecoveryError, RegionRouteError, RegionStoreTopology, RegionVerId,
    StoreLiveness, StoreResolveState,
};

use lookup::{apply_observed_buckets, insert_loaded_into, normalize_loaded};

pub use loader::{
    BatchLoadOptions, BatchRegionLoader, BatchScanBackoff, BatchScanRetryReason, RegionLoader,
    RegionQuery, RegionQueryBackoff, RegionQueryLoader, RegionQueryOptions, RegionQueryRetryReason,
    RegionQueryRoute, RegionRecoveryLoader, StoreMetadata,
};

pub(super) use loader::{
    RegionLookupApplication, RegionLookupPlan, RegionLookupResult, RegionLookupSelection,
    SharedRegionLoader, StoreLabels, StoreLivenessApplication, StoreLivenessPlan,
    StoreLivenessResult, StoreRefreshApplication, StoreRefreshPlan, StoreRefreshResult,
};

/// Result of one bounded rotating cache-GC round.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionGcRound {
    /// Entries inspected in this round.
    pub scanned: usize,
    /// Expired entries removed.
    pub expired: usize,
    /// Delayed reloads made visible to foreground lookup.
    pub delayed_reloads_released: usize,
    /// Whether the next round continues before wrapping to the beginning.
    pub has_more: bool,
}

/// Aggregate result of one canonical store-maintenance pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreMaintenanceRound {
    /// Stores selected for refresh.
    pub attempted: usize,
    /// Existing records refreshed in place.
    pub refreshed: usize,
    /// Existing records marked removed.
    pub removed: usize,
    /// Bounded loader failures deferred to a later round.
    pub failed: usize,
    /// Results discarded because the canonical store changed during metadata
    /// or liveness I/O.
    pub stale_discarded: usize,
}

/// Ordered cache for versioned region snapshots.
pub struct RegionCache<L> {
    pub(super) loader: SharedRegionLoader<L>,
    pub(super) regions: Vec<RegionLocation>,
    pub(super) stores: BTreeMap<u64, RegionStoreTopology>,
    store_revision: u64,
    topology_revision: u64,
    preferred_proxies: BTreeMap<RegionVerId, RegionAttempt>,
    entry_states: BTreeMap<RegionVerId, CacheEntryState>,
    base_ttl_seconds: u64,
    ttl_jitter_seconds: u64,
    gc_cursor: usize,
}

impl<L> RegionCache<L> {
    /// Creates an empty cache over an injected loader.
    #[must_use]
    pub fn new(loader: L) -> Self {
        Self {
            loader: SharedRegionLoader::new(loader),
            regions: Vec::new(),
            stores: BTreeMap::new(),
            store_revision: 0,
            topology_revision: 0,
            preferred_proxies: BTreeMap::new(),
            entry_states: BTreeMap::new(),
            base_ttl_seconds: 600,
            ttl_jitter_seconds: 60,
            gc_cursor: 0,
        }
    }

    /// Creates an empty cache with deterministic source-shaped TTL settings.
    #[must_use]
    pub fn with_ttl(loader: L, base_ttl_seconds: u64, ttl_jitter_seconds: u64) -> Self {
        Self {
            loader: SharedRegionLoader::new(loader),
            regions: Vec::new(),
            stores: BTreeMap::new(),
            store_revision: 0,
            topology_revision: 0,
            preferred_proxies: BTreeMap::new(),
            entry_states: BTreeMap::new(),
            base_ttl_seconds,
            ttl_jitter_seconds,
            gc_cursor: 0,
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
        self.with_loader(|loader| loader.cluster_id())
    }

    pub(super) fn loader_handle(&self) -> SharedRegionLoader<L> {
        self.loader.clone()
    }

    pub(super) fn with_loader<R>(&self, operation: impl FnOnce(&mut L) -> R) -> R {
        self.loader.with_loader(operation)
    }

    pub(super) const fn topology_revision(&self) -> u64 {
        self.topology_revision
    }

    fn advance_store_revision(&mut self) {
        self.store_revision = self.store_revision.saturating_add(1);
        self.advance_topology_revision();
    }

    fn advance_topology_revision(&mut self) {
        self.topology_revision = self.topology_revision.saturating_add(1);
    }

    /// Invalidates only the exact versioned region identity.
    pub fn invalidate(&mut self, region: RegionVerId) -> bool {
        let original_len = self.regions.len();
        self.regions.retain(|cached| cached.region != region);
        self.preferred_proxies.remove(&region);
        self.entry_states.remove(&region);
        let removed = self.regions.len() != original_len;
        if removed {
            self.advance_topology_revision();
        }
        removed
    }

    /// Forces this exact entry to reload on its next foreground access.
    pub fn mark_reload_on_access(&mut self, region: RegionVerId) -> bool {
        self.entry_states.get_mut(&region).is_some_and(|state| {
            state.mark(CacheReloadState::ReloadOnAccess);
            true
        })
    }

    /// Preserves the current expiry even when the entry is repeatedly read.
    pub fn mark_expire_after_ttl(&mut self, region: RegionVerId) -> bool {
        self.entry_states.get_mut(&region).is_some_and(|state| {
            state.mark(CacheReloadState::ExpireAfterTtl);
            true
        })
    }

    /// Defers this exact entry's reload until one cache-maintenance scan has
    /// observed it, matching client-go's delayed split-reload state machine.
    pub fn mark_delayed_reload(&mut self, region: RegionVerId) -> bool {
        self.entry_states.get_mut(&region).is_some_and(|state| {
            state.mark(CacheReloadState::DelayedReloadPending);
            true
        })
    }

    /// Runs one cache-maintenance scan at the current wall clock.
    ///
    /// Expired entries are removed and delayed reloads become visible to the
    /// next foreground lookup. Scheduling this hook remains the caller's job.
    pub fn maintain_entries(&mut self) -> usize {
        self.maintain_entries_at(cache_now_seconds())
    }

    /// Runs one bounded rotating cache-GC round at the current wall clock.
    pub fn maintain_entries_bounded(&mut self, limit: usize) -> RegionGcRound {
        self.maintain_entries_bounded_at(cache_now_seconds(), limit)
    }

    /// Deterministic-clock form of [`Self::maintain_entries`]. Returns the
    /// number of delayed reloads released by this scan.
    pub fn maintain_entries_at(&mut self, now_seconds: u64) -> usize {
        let limit = self.regions.len().max(1);
        self.gc_cursor = 0;
        self.maintain_entries_bounded_at(now_seconds, limit)
            .delayed_reloads_released
    }

    /// Inspects at most `limit` entries and continues from the prior round.
    pub fn maintain_entries_bounded_at(&mut self, now_seconds: u64, limit: usize) -> RegionGcRound {
        let limit = limit.max(1);
        if self.regions.is_empty() {
            self.gc_cursor = 0;
            return RegionGcRound::default();
        }
        self.gc_cursor = self.gc_cursor.min(self.regions.len() - 1);
        let end = self.gc_cursor.saturating_add(limit).min(self.regions.len());
        let selected = self.regions[self.gc_cursor..end]
            .iter()
            .map(|location| location.region)
            .collect::<Vec<_>>();
        let next_region = self.regions.get(end).map(|location| location.region);
        let mut expired = Vec::new();
        let mut released = 0;
        for region in &selected {
            let Some(state) = self.entry_states.get_mut(region) else {
                continue;
            };
            if now_seconds > state.expires_at_seconds() {
                expired.push(*region);
                continue;
            }
            if state.is_marked(CacheReloadState::DelayedReloadReady) {
                continue;
            }
            if state.release_delayed_reload() {
                released += 1;
                continue;
            }
            if state.is_marked(CacheReloadState::ExpireAfterTtl) {
                continue;
            }
            let stale_or_unreachable = self
                .regions
                .iter()
                .find(|cached| cached.region == *region)
                .is_some_and(|cached| {
                    cached.peers.iter().any(|peer| {
                        self.stores.get(&peer.store_id).is_some_and(|store| {
                            peer.store_epoch != store.epoch
                                || store.liveness != StoreLiveness::Reachable
                        })
                    })
                });
            if stale_or_unreachable {
                state.mark(CacheReloadState::ExpireAfterTtl);
            }
        }
        let expired_count = expired.len();
        for region in expired {
            self.remove_cached_region(region);
        }
        self.gc_cursor = next_region
            .and_then(|next| {
                self.regions
                    .iter()
                    .position(|location| location.region == next)
            })
            .unwrap_or(0);
        RegionGcRound {
            scanned: selected.len(),
            expired: expired_count,
            delayed_reloads_released: released,
            has_more: next_region.is_some(),
        }
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
        if usable && location.leader_peer_id != Some(peer_id) {
            location.leader_peer_id = Some(peer_id);
            self.preferred_proxies.remove(&region);
            self.advance_topology_revision();
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
        mut replacements: Vec<(RegionLocation, StoreLabels)>,
    ) -> Result<(), RegionRouteError>
    where
        L: RegionLoader,
    {
        replacements.sort_by(|left, right| left.0.start_key.cmp(&right.0.start_key));
        for (replacement, _) in &replacements {
            if !replacement.end_key.is_empty() && replacement.start_key >= replacement.end_key {
                return Err(RegionRouteError::InvalidRegionBounds {
                    region: replacement.region,
                });
            }
        }
        for (index, (left, _)) in replacements.iter().enumerate() {
            if replacements[index + 1..]
                .iter()
                .any(|(right, _)| right.region.id == left.region.id)
            {
                return Err(RegionRouteError::DuplicateReplacementRegion {
                    region: left.region,
                });
            }
        }
        let mut next = self.regions.clone();
        let observed_buckets = self
            .regions
            .iter()
            .find(|location| location.region == observed)
            .and_then(|location| location.buckets.clone());
        let mut next_stores = self.stores.clone();
        let mut next_states = self.entry_states.clone();
        next.retain(|location| location.region != observed);
        next_states.remove(&observed);
        let now_seconds = cache_now_seconds();
        for (mut replacement, labels) in replacements {
            apply_observed_buckets(observed_buckets.as_ref(), &mut replacement);
            normalize_loaded(&mut next_stores, &mut replacement, &labels);
            let region = replacement.region;
            let expire_after_ttl = !replacement.down_peer_ids.is_empty();
            insert_loaded_into(&mut next, replacement)?;
            let mut state = CacheEntryState::new(self.next_expiry_at(now_seconds, region));
            if expire_after_ttl {
                state.mark(CacheReloadState::ExpireAfterTtl);
            }
            next_states.insert(region, state);
        }
        self.regions = next;
        self.stores = next_stores;
        self.advance_store_revision();
        next_states.retain(|region, _| self.regions.iter().any(|cached| cached.region == *region));
        self.entry_states = next_states;
        self.preferred_proxies.remove(&observed);
        self.preferred_proxies
            .retain(|region, _| self.regions.iter().any(|cached| cached.region == *region));
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
}

fn cache_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}
