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
//! What the region cache asks PD for, and the plan/apply split that lets
//! someone else do the asking.
//!
//! Go boundary: the PD-client calls `region_cache.go` makes — `GetRegion`,
//! `GetRegionByID`, `ScanRegions`, `BatchScanRegions`, `GetStore` — expressed
//! here as loader traits so the cache never owns a connection.
//!
//! Every I/O in this crate is split into a *plan* selected under the cache
//! lock, the load performed with the lock released, and an *application* that
//! is discarded if the canonical cache moved underneath it. That is what lets
//! [`super::super::background`] refresh regions and stores without holding the
//! lock across a PD round trip, and it is why the plan/result/application types
//! live next to the traits they carry data for.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::super::{
    KeyRange, RegionLoadError, RegionLocation, RegionMetadata, RegionRecoveryError,
    RegionRouteError, StoreLiveness, StoreResolveState,
};
use super::lookup::labels_for_location;

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

pub(in crate::region) struct SharedRegionLoader<L> {
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
    pub(super) fn new(loader: L) -> Self {
        Self {
            inner: Arc::new(Mutex::new(loader)),
        }
    }

    pub(super) fn with_loader<R>(&self, operation: impl FnOnce(&mut L) -> R) -> R {
        let mut loader = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        operation(&mut loader)
    }
}

impl<L: RegionQueryLoader> SharedRegionLoader<L> {
    pub(in crate::region) fn load_store(&self, plan: StoreRefreshPlan) -> StoreRefreshResult {
        let metadata = self.with_loader(|loader| loader.load_store(plan.store_id));
        StoreRefreshResult { plan, metadata }
    }
}

impl<L: RegionLoader> SharedRegionLoader<L> {
    pub(in crate::region) fn load_region(&self, plan: RegionLookupPlan) -> RegionLookupResult {
        let loaded = self.with_loader(|loader| {
            let location = loader.load_region(&plan.key)?;
            let labels = labels_for_location(loader, &location);
            Ok((location, labels))
        });
        RegionLookupResult { plan, loaded }
    }
}

impl<L: RegionRecoveryLoader> SharedRegionLoader<L> {
    pub(in crate::region) fn hydrate_regions(
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
pub(in crate::region) struct RegionLookupPlan {
    pub(super) key: Vec<u8>,
    pub(super) require_exact_start: bool,
    pub(super) observed_location: Option<RegionLocation>,
    pub(super) observed_store_revision: u64,
}

pub(in crate::region) type StoreLabels = BTreeMap<u64, Vec<(String, String)>>;
type LoadedRegion = Result<(RegionLocation, StoreLabels), RegionLoadError>;

pub(in crate::region) struct RegionLookupResult {
    pub(super) plan: RegionLookupPlan,
    pub(super) loaded: LoadedRegion,
}

pub(in crate::region) enum RegionLookupSelection {
    Hit(RegionLocation),
    Load(RegionLookupPlan),
}

pub(in crate::region) enum RegionLookupApplication {
    Published(Box<RegionLocation>),
    Retry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::region) struct StoreRefreshPlan {
    pub(super) store_id: u64,
    pub(super) observed_epoch: u64,
    pub(super) observed_resolve_state: StoreResolveState,
    pub(super) observed_address: String,
    pub(super) observed_labels: Vec<(String, String)>,
}

pub(in crate::region) struct StoreRefreshResult {
    pub(super) plan: StoreRefreshPlan,
    pub(super) metadata: Result<Option<StoreMetadata>, RegionLoadError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::region) struct StoreLivenessPlan {
    pub(in crate::region) store_id: u64,
    pub(in crate::region) observed_epoch: u64,
    pub(in crate::region) observed_resolve_state: StoreResolveState,
    pub(in crate::region) observed_liveness: StoreLiveness,
    pub(in crate::region) address: String,
}

pub(in crate::region) struct StoreLivenessResult {
    pub(in crate::region) plan: StoreLivenessPlan,
    pub(in crate::region) liveness: StoreLiveness,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::region) enum StoreRefreshApplication {
    Unchanged,
    Refreshed,
    Removed,
    Failed,
    StaleDiscarded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::region) enum StoreLivenessApplication {
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
