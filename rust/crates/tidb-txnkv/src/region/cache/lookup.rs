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
//! Resolving one region: by key, by end key, by covering range, or by region
//! ID — and inserting what PD answered into the ordered cache.
//!
//! Go boundary: client-go's `region_cache.go` — `LocateKey`, `LocateEndKey`,
//! `LocateRegionByID`, `loadRegion`, and `insertRegionToCache`, including its
//! rule that a freshly loaded region never loses bucket metadata the cache
//! already holds at a newer version.

use std::collections::{BTreeMap, BTreeSet};

use super::super::{
    CacheEntryState, CacheReloadState, KeyRange, RegionLoadError, RegionLocation, RegionRouteError,
    RegionStoreTopology, RegionVerId, Store, StoreLiveness, StoreResolveState, StoreState,
};
use super::{
    cache_now_seconds, RegionCache, RegionLoader, RegionLookupApplication, RegionLookupPlan,
    RegionLookupResult, RegionLookupSelection, RegionQuery, RegionQueryLoader, RegionQueryOptions,
    StoreLabels,
};

impl<L> RegionCache<L>
where
    L: RegionQueryLoader,
{
    /// Loads one region identity directly from the control plane without
    /// publishing it into the cache.
    pub fn locate_region_by_id_from_source(
        &mut self,
        region_id: u64,
    ) -> Result<RegionLocation, RegionRouteError> {
        let loaded = self
            .with_loader(|loader| {
                loader.query_region(RegionQuery::Id(region_id), RegionQueryOptions::default())
            })
            .map_err(RegionRouteError::Loader)?;
        ensure_region_id(region_id, &loaded)?;
        Ok(loaded)
    }

    /// Finds one region identity in cache or loads and publishes it on a miss.
    pub fn locate_region_by_id(
        &mut self,
        region_id: u64,
    ) -> Result<RegionLocation, RegionRouteError> {
        self.locate_region_by_id_at(region_id, cache_now_seconds())
    }

    /// Deterministic-clock form of [`Self::locate_region_by_id`].
    pub fn locate_region_by_id_at(
        &mut self,
        region_id: u64,
        now_seconds: u64,
    ) -> Result<RegionLocation, RegionRouteError> {
        if let Some(index) = self
            .regions
            .iter()
            .position(|region| region.region.id == region_id)
        {
            let region = self.regions[index].region;
            let next_expiry = self.next_expiry_at(now_seconds, region);
            let valid = self.entry_states.get_mut(&region).is_some_and(|state| {
                state.check_and_renew(now_seconds, self.base_ttl_seconds, next_expiry)
            });
            if valid {
                return Ok(self.regions[index].clone());
            }
        }
        let loaded = self
            .with_loader(|loader| {
                loader.query_region(RegionQuery::Id(region_id), RegionQueryOptions::default())
            })
            .map_err(RegionRouteError::Loader)?;
        ensure_region_id(region_id, &loaded)?;
        let index = self.insert_loaded_at(loaded, now_seconds)?;
        Ok(self.regions[index].clone())
    }
}

impl<L> RegionCache<L> {
    pub(in crate::region) fn select_region_lookup(
        &mut self,
        key: &[u8],
        require_exact_start: bool,
    ) -> Result<RegionLookupSelection, RegionRouteError> {
        self.select_region_lookup_at(key, require_exact_start, cache_now_seconds())
    }

    fn select_region_lookup_at(
        &mut self,
        key: &[u8],
        require_exact_start: bool,
        now_seconds: u64,
    ) -> Result<RegionLookupSelection, RegionRouteError> {
        let observed_location = self.find_key(key).map(|index| self.regions[index].clone());
        if let Some(location) = &observed_location {
            let next_expiry = self.next_expiry_at(now_seconds, location.region);
            let valid = self
                .entry_states
                .get_mut(&location.region)
                .is_some_and(|state| {
                    state.check_and_renew(now_seconds, self.base_ttl_seconds, next_expiry)
                });
            if valid {
                if require_exact_start && location.start_key != key {
                    return Err(RegionRouteError::DiscontinuousRegion {
                        region: location.region,
                    });
                }
                return Ok(RegionLookupSelection::Hit(location.clone()));
            }
            self.preferred_proxies.remove(&location.region);
        }
        Ok(RegionLookupSelection::Load(RegionLookupPlan {
            key: key.to_vec(),
            require_exact_start,
            observed_location,
            observed_store_revision: self.store_revision,
        }))
    }

    pub(in crate::region) fn publish_region_lookup(
        &mut self,
        result: RegionLookupResult,
    ) -> Result<RegionLookupApplication, RegionRouteError> {
        let RegionLookupResult { plan, loaded } = result;
        match self.select_region_lookup(&plan.key, plan.require_exact_start)? {
            RegionLookupSelection::Hit(location) => {
                return Ok(RegionLookupApplication::Published(Box::new(location)));
            }
            RegionLookupSelection::Load(current)
                if current.observed_location != plan.observed_location
                    || current.observed_store_revision != plan.observed_store_revision =>
            {
                return Ok(RegionLookupApplication::Retry);
            }
            RegionLookupSelection::Load(_) => {}
        }

        let (loaded, labels) = loaded.map_err(RegionRouteError::Loader)?;
        if !loaded.end_key.is_empty() && loaded.start_key >= loaded.end_key {
            return Err(RegionRouteError::InvalidRegionBounds {
                region: loaded.region,
            });
        }
        if plan.require_exact_start && loaded.start_key != plan.key {
            return Err(RegionRouteError::DiscontinuousRegion {
                region: loaded.region,
            });
        }
        if !loaded.contains_key(&plan.key) {
            return Err(RegionRouteError::LoadedRegionDoesNotContainKey {
                region: loaded.region,
            });
        }

        let index = self.insert_loaded_with_labels_at(loaded, labels, cache_now_seconds())?;
        Ok(RegionLookupApplication::Published(Box::new(
            self.regions[index].clone(),
        )))
    }

    /// Finds one key, loading and inserting on a miss.
    pub fn locate_key(&mut self, key: &[u8]) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        self.locate_key_at(key, cache_now_seconds())
    }

    /// Deterministic-clock form of [`Self::locate_key`] used by source tests.
    pub fn locate_key_at(
        &mut self,
        key: &[u8],
        now_seconds: u64,
    ) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        self.locate_key_with_boundary_at(key, false, now_seconds)
    }

    /// Finds the region containing an inclusive range end.
    pub fn locate_end_key(&mut self, key: &[u8]) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        self.locate_end_key_at(key, cache_now_seconds())
    }

    /// Deterministic-clock form of [`Self::locate_end_key`].
    pub fn locate_end_key_at(
        &mut self,
        key: &[u8],
        now_seconds: u64,
    ) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        if let Some(index) = self.find_end_key(key) {
            let region = self.regions[index].region;
            let next_expiry = self.next_expiry_at(now_seconds, region);
            let valid = self.entry_states.get_mut(&region).is_some_and(|state| {
                state.check_and_renew(now_seconds, self.base_ttl_seconds, next_expiry)
            });
            if valid {
                return Ok(&self.regions[index]);
            }
            self.preferred_proxies.remove(&region);
        }
        let loaded = self
            .with_loader(|loader| loader.load_region_by_end_key(key))
            .map_err(RegionRouteError::Loader)?;
        if !loaded.contains_end_key(key) {
            return Err(RegionRouteError::LoadedRegionDoesNotContainKey {
                region: loaded.region,
            });
        }
        let index = self.insert_loaded_at(loaded, now_seconds)?;
        Ok(&self.regions[index])
    }

    pub(super) fn locate_key_with_boundary(
        &mut self,
        key: &[u8],
        require_exact_start: bool,
    ) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        self.locate_key_with_boundary_at(key, require_exact_start, cache_now_seconds())
    }

    pub(super) fn locate_key_with_boundary_at(
        &mut self,
        key: &[u8],
        require_exact_start: bool,
        now_seconds: u64,
    ) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        if let Some(index) = self.find_key(key) {
            let region = self.regions[index].region;
            let next_expiry = self.next_expiry_at(now_seconds, region);
            let valid = self.entry_states.get_mut(&region).is_some_and(|state| {
                state.check_and_renew(now_seconds, self.base_ttl_seconds, next_expiry)
            });
            if valid {
                if require_exact_start && self.regions[index].start_key != key {
                    return Err(RegionRouteError::DiscontinuousRegion { region });
                }
                return Ok(&self.regions[index]);
            }
            self.preferred_proxies.remove(&region);
        }
        let loaded = self
            .with_loader(|loader| loader.load_region(key))
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
        let index = self.insert_loaded_at(loaded, now_seconds)?;
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

    /// Lists cached/loaded region IDs from `start_key` through the region
    /// containing inclusive `end_key`, matching client-go's cache helper.
    pub fn list_region_ids(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<u64>, RegionRouteError>
    where
        L: RegionLoader,
    {
        if !end_key.is_empty() && start_key > end_key {
            return Err(RegionRouteError::InvalidRange);
        }
        let mut ids = Vec::new();
        let mut cursor = start_key.to_vec();
        loop {
            let location = self.locate_key(&cursor)?;
            ids.push(location.region.id);
            let region = location.region;
            let next = location.end_key.clone();
            if next.is_empty() || (!end_key.is_empty() && next.as_slice() > end_key) {
                break;
            }
            if next <= cursor {
                return Err(RegionRouteError::NonProgressingRegion { region });
            }
            cursor = next;
        }
        Ok(ids)
    }

    pub(super) fn find_key(&self, key: &[u8]) -> Option<usize> {
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

    pub(super) fn find_end_key(&self, key: &[u8]) -> Option<usize> {
        self.regions
            .binary_search_by(|region| {
                if region.contains_end_key(key) {
                    std::cmp::Ordering::Equal
                } else if key.is_empty() || region.start_key.as_slice() >= key {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .ok()
    }

    pub(super) fn refresh_traversed_entries(
        &mut self,
        ranges: &[KeyRange],
        now_seconds: u64,
    ) -> Result<BTreeSet<RegionVerId>, RegionRouteError> {
        let mut unavailable = BTreeSet::new();
        for range in ranges {
            let mut cursor = range.start.clone();
            while let Some(index) = self.find_key(&cursor) {
                let region = self.regions[index].region;
                if unavailable.contains(&region) {
                    break;
                }
                let next_expiry = self.next_expiry_at(now_seconds, region);
                let valid = self.entry_states.get_mut(&region).is_some_and(|state| {
                    state.check_and_renew(now_seconds, self.base_ttl_seconds, next_expiry)
                });
                if !valid {
                    unavailable.insert(region);
                    break;
                }

                let region_end = self.regions[index].end_key.clone();
                let covered = if range.end.is_empty() {
                    region_end.is_empty()
                } else {
                    region_end.is_empty() || region_end >= range.end
                };
                if covered {
                    break;
                }
                if region_end.is_empty() || region_end <= cursor {
                    return Err(RegionRouteError::NonProgressingRegion { region });
                }
                cursor = region_end;
            }
        }
        Ok(unavailable)
    }

    pub(super) fn remove_cached_region(&mut self, region: RegionVerId) {
        let original_len = self.regions.len();
        self.regions.retain(|cached| cached.region != region);
        self.entry_states.remove(&region);
        self.preferred_proxies.remove(&region);
        if self.regions.len() != original_len {
            self.advance_topology_revision();
        }
    }

    pub(super) fn insert_loaded_at(
        &mut self,
        loaded: RegionLocation,
        now_seconds: u64,
    ) -> Result<usize, RegionRouteError>
    where
        L: RegionLoader,
    {
        let labels = self.with_loader(|loader| labels_for_location(loader, &loaded));
        self.insert_loaded_with_labels_at(loaded, labels, now_seconds)
    }

    pub(super) fn insert_loaded_with_labels_at(
        &mut self,
        mut loaded: RegionLocation,
        labels: StoreLabels,
        now_seconds: u64,
    ) -> Result<usize, RegionRouteError> {
        let mut next_regions = self.regions.clone();
        let mut next_stores = self.stores.clone();
        preserve_newer_buckets(&self.regions, &mut loaded);
        normalize_loaded(&mut next_stores, &mut loaded, &labels);
        let region = loaded.region;
        let expire_after_ttl = !loaded.down_peer_ids.is_empty();
        let index = insert_loaded_into(&mut next_regions, loaded)?;
        self.regions = next_regions;
        self.stores = next_stores;
        self.advance_store_revision();
        self.entry_states
            .retain(|cached, _| self.regions.iter().any(|region| region.region == *cached));
        let mut state = CacheEntryState::new(self.next_expiry_at(now_seconds, region));
        if expire_after_ttl {
            state.mark(CacheReloadState::ExpireAfterTtl);
        }
        self.entry_states.insert(region, state);
        self.preferred_proxies
            .retain(|region, _| self.regions.iter().any(|cached| cached.region == *region));
        Ok(index)
    }

    pub(super) fn next_expiry_at(&self, now_seconds: u64, region: RegionVerId) -> u64 {
        let jitter = if self.ttl_jitter_seconds == 0 {
            0
        } else {
            region.id % self.ttl_jitter_seconds
        };
        now_seconds
            .saturating_add(self.base_ttl_seconds)
            .saturating_add(jitter)
    }
}

pub(in crate::region) fn preserve_newer_buckets(
    cached: &[RegionLocation],
    loaded: &mut RegionLocation,
) {
    let Some(current) = cached
        .iter()
        .find(|current| ranges_intersect(current, loaded))
    else {
        return;
    };
    let current_version = current.bucket_version();
    let loaded_version = loaded.bucket_version();
    if current_version > 0 && (loaded_version == 0 || loaded_version < current_version) {
        loaded.buckets.clone_from(&current.buckets);
    }
}

fn ensure_region_id(expected: u64, loaded: &RegionLocation) -> Result<(), RegionRouteError> {
    if loaded.region.id == expected {
        return Ok(());
    }
    Err(RegionRouteError::Loader(RegionLoadError::new(
        "region-id-mismatch",
        format!(
            "control plane returned region {}, expected {expected}",
            loaded.region.id
        ),
    )))
}

pub(super) fn apply_observed_buckets(
    current: Option<&super::super::BucketMetadata>,
    loaded: &mut RegionLocation,
) {
    let Some(current) = current else {
        return;
    };
    loaded.buckets = Some(current.clone());
}

pub(in crate::region) fn cache_misses(
    cached: &[RegionLocation],
    ranges: &[KeyRange],
    unavailable: &BTreeSet<RegionVerId>,
) -> Result<Vec<KeyRange>, RegionRouteError> {
    let mut misses = Vec::new();
    for range in ranges {
        let mut cursor = range.start.clone();
        loop {
            let current = cached.iter().find(|region| {
                !unavailable.contains(&region.region) && region.contains_key(&cursor)
            });
            let Some(current) = current else {
                misses.push(KeyRange::new(cursor, range.end.clone()));
                break;
            };
            let covered = if range.end.is_empty() {
                current.end_key.is_empty()
            } else {
                current.end_key.is_empty() || current.end_key >= range.end
            };
            if covered {
                break;
            }
            if current.end_key.is_empty() || current.end_key <= cursor {
                return Err(RegionRouteError::NonProgressingRegion {
                    region: current.region,
                });
            }
            cursor.clone_from(&current.end_key);
        }
    }
    Ok(misses)
}

pub(in crate::region) fn labels_for_location<L: RegionLoader>(
    loader: &L,
    loaded: &RegionLocation,
) -> StoreLabels {
    loaded
        .stores
        .iter()
        .map(|store| (store.id, loader.store_labels(store.id).to_vec()))
        .collect()
}

pub(super) fn normalize_loaded(
    stores: &mut BTreeMap<u64, RegionStoreTopology>,
    loaded: &mut RegionLocation,
    labels: &StoreLabels,
) {
    for supplied in &loaded.stores {
        let supplied_labels = labels.get(&supplied.id).cloned().unwrap_or_default();
        match stores.get_mut(&supplied.id) {
            None => {
                stores.insert(
                    supplied.id,
                    RegionStoreTopology::new(
                        StoreState {
                            id: supplied.id,
                            address: supplied.address.clone(),
                            epoch: supplied.epoch,
                            resolve_state: StoreResolveState::Resolved,
                            liveness: StoreLiveness::Reachable,
                            routing_health: super::super::StoreRoutingHealth::default(),
                        },
                        supplied_labels,
                    ),
                );
            }
            Some(canonical) => {
                let address_changed = canonical.address != supplied.address;
                if address_changed && canonical.resolve_state == StoreResolveState::Resolved {
                    canonical.epoch = canonical.epoch.saturating_add(1);
                }
                canonical.address.clone_from(&supplied.address);
                canonical.resolve_state = StoreResolveState::Resolved;
                canonical.replace_labels(supplied_labels);
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

pub(in crate::region) fn insert_loaded_into(
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
