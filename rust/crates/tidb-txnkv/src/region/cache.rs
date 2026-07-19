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
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{
    merge_loaded_and_cached, ranges_after_key, regions_have_gap, regions_intersecting_ranges,
    CacheEntryState, CacheReloadState, HealthInstant, KeyRange, LeaderRequest, OwnedLeaderRoute,
    Peer, PeerRole, ReadPolicy, RegionAttempt, RegionAttemptObservation, RegionLoadError,
    RegionLocation, RegionMetadata, RegionRebuildAction, RegionRecoveryError, RegionRouteError,
    RegionStoreTopology, RegionVerId, ReplicaHealthFacts, ReplicaReadMode, RequestSelection,
    RequestSelector, RouteFeedback, RouteFeedbackApplication, RouteOutcome, RoutePeer,
    RouteSnapshot, ServerBusyAction, Store, StoreFailureOutcome, StoreLabel, StoreLiveness,
    StoreRefreshOutcome, StoreResolveState, StoreState, DEFAULT_REGIONS_PER_BATCH,
    MAX_RANGES_PER_BATCH, MAX_REPLICA_ATTEMPTS, MAX_REPLICA_ATTEMPT_TIME,
};

/// Caller-selected options for one PD batch-region attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchLoadOptions {
    /// Request bucket metadata with each region.
    pub need_buckets: bool,
    /// Filter leaderless metadata and retry when none remains.
    pub need_leader: bool,
}

/// Why a batch scan must be retried before any reply is published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchScanRetryReason {
    /// PD returned no regions.
    EmptyReply,
    /// PD returned a prefix or interior coverage gap.
    CoverageGap,
    /// The caller required leaders and every returned region was leaderless.
    MissingLeader,
}

/// Source-shaped retry context owned by the batch-scan caller.
pub trait BatchScanBackoff {
    /// Waits, cancels, or exhausts the caller's PD-RPC retry budget.
    fn backoff(&mut self, reason: BatchScanRetryReason) -> Result<(), RegionRouteError>;
}

/// One source-shaped foreground region query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegionQuery<'a> {
    /// Region containing an ordinary key.
    Key(&'a [u8]),
    /// Region containing an inclusive end key.
    EndKey(&'a [u8]),
    /// Region with one exact identity.
    Id(u64),
}

/// Per-attempt PD routing options. Retry policy remains in RegionCache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionQueryOptions {
    /// Request bucket metadata.
    pub need_buckets: bool,
    /// Exact endpoint class permitted for this attempt.
    pub route: RegionQueryRoute,
}

/// Mutually exclusive PD endpoint policy for one request attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegionQueryRoute {
    /// Permit the active follower or router-service path.
    AllowFollowerOrRouter,
    /// Send only to the discovered PD leader.
    LeaderOnly,
}

impl Default for RegionQueryOptions {
    fn default() -> Self {
        Self {
            need_buckets: true,
            route: RegionQueryRoute::LeaderOnly,
        }
    }
}

/// Why a unary/legacy scan retries through its caller-owned backoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegionQueryRetryReason {
    /// PD returned no metadata.
    EmptyReply,
    /// PD returned a prefix or interior gap.
    CoverageGap,
    /// Every returned region was leaderless.
    MissingLeader,
}

/// Caller-owned cancellation, sleep, and retry budget.
pub trait RegionQueryBackoff {
    /// Applies one source-shaped PD-RPC backoff.
    fn backoff(&mut self, reason: RegionQueryRetryReason) -> Result<(), RegionRouteError>;
}

/// One projected store metadata observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreMetadata {
    /// Store identity.
    pub id: u64,
    /// Current TiKV address.
    pub address: String,
    /// Current labels.
    pub labels: Vec<(String, String)>,
}

/// Request-shaped control-plane capability above one-attempt transports.
pub trait RegionQueryLoader: RegionLoader {
    /// Executes exactly one key/end-key/ID query.
    fn query_region(
        &mut self,
        query: RegionQuery<'_>,
        options: RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError>;

    /// Executes exactly one deprecated contiguous scan.
    fn scan_regions_once(
        &mut self,
        range: &KeyRange,
        limit: usize,
        options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError>;

    /// Reloads one store. None means removed/tombstone.
    fn load_store(&mut self, store_id: u64) -> Result<Option<StoreMetadata>, RegionLoadError>;
}

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

pub(super) struct SharedRegionLoader<L> {
    inner: Arc<Mutex<L>>,
}

impl<L> Clone for SharedRegionLoader<L> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<L> SharedRegionLoader<L> {
    fn new(loader: L) -> Self {
        Self {
            inner: Arc::new(Mutex::new(loader)),
        }
    }

    fn with_loader<R>(&self, operation: impl FnOnce(&mut L) -> R) -> R {
        let mut loader = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        operation(&mut loader)
    }
}

impl<L: RegionQueryLoader> SharedRegionLoader<L> {
    pub(super) fn load_store(&self, plan: StoreRefreshPlan) -> StoreRefreshResult {
        let metadata = self.with_loader(|loader| loader.load_store(plan.store_id));
        StoreRefreshResult { plan, metadata }
    }
}

impl<L: RegionLoader> SharedRegionLoader<L> {
    pub(super) fn load_region(&self, plan: RegionLookupPlan) -> RegionLookupResult {
        let loaded = self.with_loader(|loader| {
            let location = loader.load_region(&plan.key)?;
            let labels = labels_for_location(loader, &location);
            Ok((location, labels))
        });
        RegionLookupResult { plan, loaded }
    }
}

impl<L: RegionRecoveryLoader> SharedRegionLoader<L> {
    pub(super) fn hydrate_regions(
        &self,
        metadata: &[RegionMetadata],
        leader_store_id: u64,
    ) -> Result<Vec<(RegionLocation, StoreLabels)>, RegionRecoveryError> {
        self.with_loader(|loader| {
            metadata
                .iter()
                .map(|metadata| {
                    let hydrated = loader
                        .hydrate_region(metadata, leader_store_id)
                        .map_err(RegionRecoveryError::Loader)?;
                    if hydrated.region != metadata.region {
                        return Err(RegionRecoveryError::HydratedRegionMismatch {
                            expected: metadata.region,
                            actual: hydrated.region,
                        });
                    }
                    let labels = labels_for_location(loader, &hydrated);
                    Ok((hydrated, labels))
                })
                .collect()
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RegionLookupPlan {
    key: Vec<u8>,
    require_exact_start: bool,
    observed_location: Option<RegionLocation>,
    observed_store_revision: u64,
}

pub(super) type StoreLabels = BTreeMap<u64, Vec<(String, String)>>;
type LoadedRegion = Result<(RegionLocation, StoreLabels), RegionLoadError>;

pub(super) struct RegionLookupResult {
    plan: RegionLookupPlan,
    loaded: LoadedRegion,
}

pub(super) enum RegionLookupSelection {
    Hit(RegionLocation),
    Load(RegionLookupPlan),
}

pub(super) enum RegionLookupApplication {
    Published(Box<RegionLocation>),
    Retry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoreRefreshPlan {
    store_id: u64,
    observed_epoch: u64,
    observed_resolve_state: StoreResolveState,
    observed_address: String,
    observed_labels: Vec<(String, String)>,
}

pub(super) struct StoreRefreshResult {
    plan: StoreRefreshPlan,
    metadata: Result<Option<StoreMetadata>, RegionLoadError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoreLivenessPlan {
    pub(super) store_id: u64,
    pub(super) observed_epoch: u64,
    pub(super) observed_resolve_state: StoreResolveState,
    pub(super) observed_liveness: StoreLiveness,
    pub(super) address: String,
}

pub(super) struct StoreLivenessResult {
    pub(super) plan: StoreLivenessPlan,
    pub(super) liveness: StoreLiveness,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StoreRefreshApplication {
    Unchanged,
    Refreshed,
    Removed,
    Failed,
    StaleDiscarded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StoreLivenessApplication {
    Unchanged,
    Updated,
    StaleDiscarded,
}

/// Injected PD-shaped region metadata loader.
pub trait RegionLoader {
    /// Returns the cluster identity attached to requests routed by this loader.
    fn cluster_id(&self) -> u64;

    /// Loads the region containing `key` without prescribing any network API.
    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError>;

    /// Loads the region containing an inclusive range end. Loaders backed by
    /// a control plane with a previous-region API should override this
    /// boundary; the default preserves compatibility for cache-only loaders.
    fn load_region_by_end_key(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.load_region(key)
    }

    /// Returns the most recently resolved PD labels for one store.
    ///
    /// Loaders without a label-bearing control plane retain source-compatible
    /// empty-label matching through this default.
    fn store_labels(&self, _store_id: u64) -> &[(String, String)] {
        &[]
    }
}

impl<L> RegionCache<L>
where
    L: BatchRegionLoader,
{
    /// Resolves ordered key ranges through valid cache entries first and the
    /// exact PD batch-scan boundary second.
    pub fn batch_locate_key_ranges(
        &mut self,
        ranges: &[KeyRange],
        options: BatchLoadOptions,
        backoff: &mut impl BatchScanBackoff,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        self.batch_locate_key_ranges_at(ranges, options, backoff, cache_now_seconds())
    }

    /// Deterministic-clock form of [`Self::batch_locate_key_ranges`] used by
    /// source-transition tests.
    pub fn batch_locate_key_ranges_at(
        &mut self,
        ranges: &[KeyRange],
        options: BatchLoadOptions,
        backoff: &mut impl BatchScanBackoff,
        now_seconds: u64,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        if ranges.iter().any(|range| !range.is_valid()) {
            return Err(RegionRouteError::InvalidRange);
        }
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let unavailable = self.refresh_traversed_entries(ranges, now_seconds)?;
        let cached_before_load = self
            .regions
            .iter()
            .filter(|region| !unavailable.contains(&region.region))
            .cloned()
            .collect::<Vec<_>>();

        let mut misses = cache_misses(&self.regions, ranges, &unavailable)?;
        let mut fresh = Vec::new();
        while !misses.is_empty() {
            let batch_len = misses.len().min(MAX_RANGES_PER_BATCH);
            let request = &misses[..batch_len];
            let publishable = loop {
                let loaded = self
                    .with_loader(|loader| {
                        loader.batch_load_regions(request, DEFAULT_REGIONS_PER_BATCH, options)
                    })
                    .map_err(RegionRouteError::Loader)?;
                let retry = if loaded.is_empty() {
                    Some(BatchScanRetryReason::EmptyReply)
                } else if regions_have_gap(request, &loaded, DEFAULT_REGIONS_PER_BATCH) {
                    Some(BatchScanRetryReason::CoverageGap)
                } else {
                    None
                };
                if let Some(reason) = retry {
                    backoff.backoff(reason)?;
                    continue;
                }
                let publishable = if options.need_leader {
                    loaded
                        .iter()
                        .filter(|region| region.leader_peer_id.is_some())
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    loaded.clone()
                };
                if publishable.is_empty() {
                    backoff.backoff(BatchScanRetryReason::MissingLeader)?;
                    continue;
                }
                break publishable;
            };
            let split_key = publishable
                .last()
                .expect("validated publishable batch reply is nonempty")
                .end_key
                .clone();
            fresh.extend(publishable);
            if split_key.is_empty() {
                misses.clear();
            } else {
                let remaining_batch = ranges_after_key(request, &split_key);
                let mut remaining = remaining_batch;
                remaining.extend_from_slice(&misses[batch_len..]);
                if remaining == misses {
                    return Err(RegionRouteError::NonProgressingBatchScan { split_key });
                }
                misses = remaining;
            }
        }

        let merged = merge_loaded_and_cached(&cached_before_load, &fresh);
        let result = regions_intersecting_ranges(&merged, ranges);
        if regions_have_gap(ranges, &result, 0) {
            return Err(RegionRouteError::BatchScanGap);
        }

        // Validate the complete canonical insertion sequence against a clone
        // before publishing the first region. This keeps terminal failures
        // from leaving a partially updated cache.
        let mut preview = self.regions.clone();
        for mut region in fresh.iter().cloned() {
            preserve_newer_buckets(&preview, &mut region);
            insert_loaded_into(&mut preview, region)?;
        }
        for region in fresh {
            self.insert_loaded_at(region, now_seconds)?;
        }
        Ok(result)
    }
}

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

    /// Executes the pinned contiguous ScanRegions contract without publishing.
    pub fn scan_regions(
        &mut self,
        range: &KeyRange,
        limit: usize,
        backoff: &mut impl RegionQueryBackoff,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        if !range.is_valid() {
            return Err(RegionRouteError::InvalidRange);
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut options = RegionQueryOptions {
            need_buckets: false,
            route: RegionQueryRoute::AllowFollowerOrRouter,
        };
        loop {
            let loaded = self
                .with_loader(|loader| loader.scan_regions_once(range, limit, options))
                .map_err(RegionRouteError::Loader)?;
            let retry = if loaded.is_empty() {
                Some(RegionQueryRetryReason::EmptyReply)
            } else if regions_have_gap(std::slice::from_ref(range), &loaded, limit) {
                Some(RegionQueryRetryReason::CoverageGap)
            } else {
                None
            };
            if let Some(reason) = retry {
                backoff.backoff(reason)?;
                options.route = RegionQueryRoute::LeaderOnly;
                continue;
            }
            let leaderful = loaded
                .into_iter()
                .filter(|region| region.leader_peer_id.is_some())
                .collect::<Vec<_>>();
            if leaderful.is_empty() {
                backoff.backoff(RegionQueryRetryReason::MissingLeader)?;
                options.route = RegionQueryRoute::LeaderOnly;
                continue;
            }
            return Ok(leaderful);
        }
    }

    /// Scans and atomically publishes the returned regions.
    pub fn load_regions_with_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        backoff: &mut impl RegionQueryBackoff,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        let loaded = self.scan_regions(range, limit, backoff)?;
        let mut preview = self.regions.clone();
        for mut region in loaded.iter().cloned() {
            preserve_newer_buckets(&preview, &mut region);
            insert_loaded_into(&mut preview, region)?;
        }
        let now_seconds = cache_now_seconds();
        for region in loaded.iter().cloned() {
            self.insert_loaded_at(region, now_seconds)?;
        }
        Ok(loaded)
    }

    /// Selects immutable store refresh plans under the canonical cache lock.
    pub(super) fn plan_store_refreshes(&self, need_check_only: bool) -> Vec<StoreRefreshPlan> {
        self.stores
            .values()
            .filter(|store| {
                if need_check_only {
                    store.resolve_state == StoreResolveState::NeedCheck
                } else {
                    store.resolve_state != StoreResolveState::Removed
                }
            })
            .map(|store| StoreRefreshPlan {
                store_id: store.id,
                observed_epoch: store.epoch,
                observed_resolve_state: store.resolve_state,
                observed_address: store.address.clone(),
                observed_labels: store.labels().to_vec(),
            })
            .collect()
    }

    /// Selects immutable health-check inputs without lending canonical stores
    /// across transport I/O.
    pub(super) fn plan_store_liveness_checks(&self) -> Vec<StoreLivenessPlan> {
        self.stores
            .values()
            .filter(|store| {
                store.resolve_state == StoreResolveState::Resolved
                    && store.liveness != StoreLiveness::Reachable
            })
            .map(|store| StoreLivenessPlan {
                store_id: store.id,
                observed_epoch: store.epoch,
                observed_resolve_state: store.resolve_state,
                observed_liveness: store.liveness,
                address: store.address.clone(),
            })
            .collect()
    }

    /// Publishes one health result only onto the exact store generation that
    /// was probed. Delayed success can never revive a replaced address or a
    /// newer failure generation.
    pub(super) fn publish_store_liveness(
        &mut self,
        result: StoreLivenessResult,
    ) -> StoreLivenessApplication {
        let StoreLivenessResult { plan, liveness } = result;
        let Some(store) = self.stores.get_mut(&plan.store_id) else {
            return StoreLivenessApplication::StaleDiscarded;
        };
        if store.epoch != plan.observed_epoch
            || store.resolve_state != plan.observed_resolve_state
            || store.liveness != plan.observed_liveness
            || store.address != plan.address
        {
            return StoreLivenessApplication::StaleDiscarded;
        }
        // Foreground failure handling owns degradation. A periodic probe may
        // only restore a store after an explicit serving response; timeout,
        // transport failure, and health `Unknown` must not turn a known-dead
        // store into a selector candidate.
        if liveness != StoreLiveness::Reachable || store.liveness == liveness {
            return StoreLivenessApplication::Unchanged;
        }
        store.liveness = liveness;
        self.advance_store_revision();
        StoreLivenessApplication::Updated
    }

    /// Publishes one PD observation only if its complete selection snapshot is current.
    pub(super) fn publish_store_refresh(
        &mut self,
        result: StoreRefreshResult,
    ) -> StoreRefreshApplication {
        let StoreRefreshResult { plan, metadata } = result;
        let Some(current) = self.stores.get(&plan.store_id) else {
            return StoreRefreshApplication::StaleDiscarded;
        };
        if current.epoch != plan.observed_epoch
            || current.resolve_state != plan.observed_resolve_state
            || current.address != plan.observed_address
            || current.labels() != plan.observed_labels.as_slice()
        {
            return StoreRefreshApplication::StaleDiscarded;
        }
        let metadata = match metadata {
            Ok(metadata) => metadata,
            Err(_) => return StoreRefreshApplication::Failed,
        };
        if let Some(metadata) = &metadata {
            if metadata.id != plan.store_id {
                return StoreRefreshApplication::Failed;
            }
            if metadata.address.is_empty() {
                return StoreRefreshApplication::Failed;
            }
        }
        let store = self
            .stores
            .get_mut(&plan.store_id)
            .expect("refresh plan was revalidated against the canonical store");
        let previous_epoch = store.epoch;
        let outcome = match metadata {
            None => {
                if store.resolve_state == StoreResolveState::Removed {
                    StoreRefreshOutcome::Unchanged
                } else {
                    store.epoch = store.epoch.saturating_add(1);
                    store.resolve_state = StoreResolveState::Removed;
                    store.liveness = StoreLiveness::Unreachable;
                    store.replace_labels(Vec::new());
                    StoreRefreshOutcome::Removed
                }
            }
            Some(metadata) => {
                let changed = store.address != metadata.address
                    || store.labels() != metadata.labels
                    || store.resolve_state != StoreResolveState::Resolved;
                if store.address != metadata.address {
                    store.epoch = store.epoch.saturating_add(1);
                }
                store.address = metadata.address;
                store.replace_labels(metadata.labels);
                store.resolve_state = StoreResolveState::Resolved;
                if changed {
                    StoreRefreshOutcome::Refreshed
                } else {
                    StoreRefreshOutcome::Unchanged
                }
            }
        };
        let removed = outcome == StoreRefreshOutcome::Removed;
        if store.epoch != previous_epoch {
            self.preferred_proxies.retain(|_, proxy| {
                proxy.store_id != plan.store_id || proxy.store_epoch != previous_epoch
            });
        }
        if removed {
            for location in &self.regions {
                if location
                    .peers
                    .iter()
                    .any(|peer| peer.store_id == plan.store_id)
                {
                    if let Some(state) = self.entry_states.get_mut(&location.region) {
                        state.mark(CacheReloadState::ExpireAfterTtl);
                    }
                }
            }
        }
        if outcome != StoreRefreshOutcome::Unchanged {
            self.advance_store_revision();
        }
        match outcome {
            StoreRefreshOutcome::Unchanged => StoreRefreshApplication::Unchanged,
            StoreRefreshOutcome::Refreshed => StoreRefreshApplication::Refreshed,
            StoreRefreshOutcome::Removed => StoreRefreshApplication::Removed,
        }
    }
}

/// PD-shaped ordered batch-region loader used only after cache misses are
/// identified. Implementations own source-specific fallback from an exact
/// Unimplemented response; RegionCache never silently replaces other errors.
pub trait BatchRegionLoader: RegionLoader {
    /// Loads at most `limit` regions intersecting ordered key ranges.
    fn batch_load_regions(
        &mut self,
        ranges: &[KeyRange],
        limit: usize,
        options: BatchLoadOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError>;
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

    pub(super) fn select_region_lookup(
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

    pub(super) fn publish_region_lookup(
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

    /// Returns one immutable view of the canonical store authority.
    #[must_use]
    pub fn store_state(&self, store_id: u64) -> Option<&StoreState> {
        self.stores.get(&store_id).map(RegionStoreTopology::state)
    }

    /// Returns one exact PD label from the canonical store authority.
    #[must_use]
    pub fn store_label(&self, store_id: u64, key: &str) -> Option<&str> {
        self.stores.get(&store_id).and_then(|store| {
            store
                .labels()
                .iter()
                .find_map(|(label_key, value)| (label_key == key).then_some(value.as_str()))
        })
    }

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

    fn locate_key_with_boundary(
        &mut self,
        key: &[u8],
        require_exact_start: bool,
    ) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        self.locate_key_with_boundary_at(key, require_exact_start, cache_now_seconds())
    }

    fn locate_key_with_boundary_at(
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

    fn find_end_key(&self, key: &[u8]) -> Option<usize> {
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

    fn refresh_traversed_entries(
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

    fn remove_cached_region(&mut self, region: RegionVerId) {
        let original_len = self.regions.len();
        self.regions.retain(|cached| cached.region != region);
        self.entry_states.remove(&region);
        self.preferred_proxies.remove(&region);
        if self.regions.len() != original_len {
            self.advance_topology_revision();
        }
    }

    fn insert_loaded_at(
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

    fn insert_loaded_with_labels_at(
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

    fn next_expiry_at(&self, now_seconds: u64, region: RegionVerId) -> u64 {
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

fn health_now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
}

fn cache_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

fn preserve_newer_buckets(cached: &[RegionLocation], loaded: &mut RegionLocation) {
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

fn selectable_peer_count(location: &RegionLocation) -> usize {
    location
        .peers
        .iter()
        .filter(|peer| {
            !location.down_peer_ids.contains(&peer.id)
                && (!peer.is_witness || location.leader_peer_id == Some(peer.id))
        })
        .count()
}

fn apply_observed_buckets(current: Option<&super::BucketMetadata>, loaded: &mut RegionLocation) {
    let Some(current) = current else {
        return;
    };
    loaded.buckets = Some(current.clone());
}

fn cache_misses(
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

fn request_flags(selector: &RequestSelector, cached_leader: bool) -> (bool, bool) {
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

fn labels_for_location<L: RegionLoader>(loader: &L, loaded: &RegionLocation) -> StoreLabels {
    loaded
        .stores
        .iter()
        .map(|store| (store.id, loader.store_labels(store.id).to_vec()))
        .collect()
}

fn normalize_loaded(
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
                            routing_health: super::StoreRoutingHealth::default(),
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
