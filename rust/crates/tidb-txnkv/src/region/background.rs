// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use super::cache::{
    RegionLookupApplication, RegionLookupSelection, SharedRegionLoader, StoreRefreshApplication,
};
use super::recovery::RegionErrorRecoveryPlan;
use super::{
    KeyRange, RegionAttempt, RegionBackoffBudget, RegionCache, RegionErrorDisposition,
    RegionGcRound, RegionLoader, RegionLocation, RegionQueryLoader, RegionRecoveryError,
    RegionRecoveryLoader, RegionRouteError, RegionVerId, StoreMaintenanceRound,
};

/// One complete pass performed by the single maintenance driver.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackgroundMaintenanceRound {
    /// Whether a coalesced need-check notification caused this pass.
    pub triggered: bool,
    /// Bounded rotating region-cache work.
    pub regions: RegionGcRound,
    /// Sequential in-place store refresh work.
    pub stores: StoreMaintenanceRound,
}

/// Construction or synchronization failure from the background owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackgroundRegionCacheError {
    /// A periodic interval of zero cannot make progress.
    ZeroInterval,
    /// A zero GC limit cannot inspect an entry.
    ZeroGcLimit,
    /// The sole maintenance thread could not be created.
    Spawn(String),
    /// A previous panic poisoned the canonical cache lock.
    CachePoisoned,
    /// A previous panic poisoned the driver-state lock.
    DriverPoisoned,
    /// The maintenance thread panicked during shutdown.
    WorkerPanicked,
    /// Explicit shutdown requires the last handle to the shared authority.
    SharedOwners {
        /// Number of live handles observed by the consumed shutdown handle.
        owners: usize,
    },
}

impl std::fmt::Display for BackgroundRegionCacheError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroInterval => formatter.write_str("background interval must be nonzero"),
            Self::ZeroGcLimit => formatter.write_str("background GC limit must be nonzero"),
            Self::Spawn(message) => {
                write!(formatter, "failed to spawn maintenance driver: {message}")
            }
            Self::CachePoisoned => formatter.write_str("canonical region cache lock is poisoned"),
            Self::DriverPoisoned => formatter.write_str("maintenance driver lock is poisoned"),
            Self::WorkerPanicked => formatter.write_str("maintenance driver panicked"),
            Self::SharedOwners { owners } => write!(
                formatter,
                "explicit shutdown requires unique ownership; observed {owners} live handles"
            ),
        }
    }
}

impl std::error::Error for BackgroundRegionCacheError {}

#[derive(Default)]
struct DriverState {
    shutdown: bool,
    triggered: bool,
    closed: bool,
    completed_rounds: u64,
    last_round: Option<BackgroundMaintenanceRound>,
}

/// Sole synchronized owner and cancellable maintenance task for RegionCache.
struct BackgroundRegionCacheInner<L> {
    cache: Arc<Mutex<RegionCache<L>>>,
    loader: SharedRegionLoader<L>,
    driver: Arc<(Mutex<DriverState>, Condvar)>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// Cloneable handle to the sole synchronized cache and maintenance task.
pub struct BackgroundRegionCache<L> {
    inner: Arc<BackgroundRegionCacheInner<L>>,
}

impl<L> Clone for BackgroundRegionCache<L> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<L> BackgroundRegionCache<L> {
    /// Wraps one canonical cache without starting a worker.
    ///
    /// This is the injection seam for non-Send test loaders. Production
    /// runtimes use [`Self::start`] and therefore cannot bypass maintenance.
    #[must_use]
    pub(crate) fn without_worker(cache: RegionCache<L>) -> Self {
        let loader = cache.loader_handle();
        Self {
            inner: Arc::new(BackgroundRegionCacheInner {
                cache: Arc::new(Mutex::new(cache)),
                loader,
                driver: Arc::new((
                    Mutex::new(DriverState {
                        shutdown: true,
                        closed: true,
                        ..DriverState::default()
                    }),
                    Condvar::new(),
                )),
                worker: Mutex::new(None),
            }),
        }
    }

    /// Moves the canonical cache under one lock and starts bounded cache GC.
    pub fn start_gc(
        cache: RegionCache<L>,
        interval: Duration,
        gc_limit: usize,
    ) -> Result<Self, BackgroundRegionCacheError>
    where
        L: RegionLoader + Send + 'static,
    {
        Self::start_with_round(cache, interval, gc_limit, |shared, triggered, gc_limit| {
            let mut cache = shared.lock().ok()?;
            Some(BackgroundMaintenanceRound {
                triggered,
                regions: cache.maintain_entries_bounded(gc_limit),
                stores: StoreMaintenanceRound::default(),
            })
        })
    }

    fn start_with_round<F>(
        cache: RegionCache<L>,
        interval: Duration,
        gc_limit: usize,
        round: F,
    ) -> Result<Self, BackgroundRegionCacheError>
    where
        L: RegionLoader + Send + 'static,
        F: FnMut(&Arc<Mutex<RegionCache<L>>>, bool, usize) -> Option<BackgroundMaintenanceRound>
            + Send
            + 'static,
    {
        if interval.is_zero() {
            return Err(BackgroundRegionCacheError::ZeroInterval);
        }
        if gc_limit == 0 {
            return Err(BackgroundRegionCacheError::ZeroGcLimit);
        }
        let loader = cache.loader_handle();
        let cache = Arc::new(Mutex::new(cache));
        let driver = Arc::new((Mutex::new(DriverState::default()), Condvar::new()));
        let worker_cache = Arc::clone(&cache);
        let worker_driver = Arc::clone(&driver);
        let worker = std::thread::Builder::new()
            .name("tidb-region-maintenance".to_owned())
            .spawn(move || maintenance_loop(worker_cache, worker_driver, interval, gc_limit, round))
            .map_err(|error| BackgroundRegionCacheError::Spawn(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(BackgroundRegionCacheInner {
                cache,
                loader,
                driver,
                worker: Mutex::new(Some(worker)),
            }),
        })
    }

    /// Finds one key while keeping loader I/O outside the canonical cache lock.
    pub fn locate_key(
        &self,
        key: &[u8],
    ) -> Result<Result<RegionLocation, RegionRouteError>, BackgroundRegionCacheError>
    where
        L: RegionLoader,
    {
        self.locate_key_with_boundary(key, false)
    }

    /// Resolves every requested range from one stable canonical topology revision.
    pub fn locate_ranges(
        &self,
        ranges: &[KeyRange],
    ) -> Result<Result<Vec<RegionLocation>, RegionRouteError>, BackgroundRegionCacheError>
    where
        L: RegionLoader,
    {
        for range in ranges {
            if !range.is_valid() {
                return Ok(Err(RegionRouteError::InvalidRange));
            }
        }
        loop {
            let revision = self.topology_revision()?;
            let mut located = BTreeMap::<RegionVerId, RegionLocation>::new();
            for range in ranges {
                let mut cursor = range.start.clone();
                let mut first_fragment = true;
                loop {
                    let location = match self.locate_key_with_boundary(&cursor, !first_fragment)? {
                        Ok(location) => location,
                        Err(error) => return Ok(Err(error)),
                    };
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
                        return Ok(Err(RegionRouteError::NonProgressingRegion { region }));
                    }
                    cursor = region_end;
                    first_fragment = false;
                }
            }
            if self.topology_revision()? == revision {
                let mut regions = located.into_values().collect::<Vec<_>>();
                regions.sort_by(|left, right| left.start_key.cmp(&right.start_key));
                return Ok(Ok(regions));
            }
        }
    }

    fn topology_revision(&self) -> Result<u64, BackgroundRegionCacheError> {
        self.inner
            .cache
            .lock()
            .map(|cache| cache.topology_revision())
            .map_err(|_| BackgroundRegionCacheError::CachePoisoned)
    }

    fn locate_key_with_boundary(
        &self,
        key: &[u8],
        require_exact_start: bool,
    ) -> Result<Result<RegionLocation, RegionRouteError>, BackgroundRegionCacheError>
    where
        L: RegionLoader,
    {
        loop {
            let selection = {
                let mut cache = self
                    .inner
                    .cache
                    .lock()
                    .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
                cache.select_region_lookup(key, require_exact_start)
            };
            let plan = match selection {
                Ok(RegionLookupSelection::Hit(location)) => return Ok(Ok(location)),
                Ok(RegionLookupSelection::Load(plan)) => plan,
                Err(error) => return Ok(Err(error)),
            };
            let loaded = self.inner.loader.load_region(plan);
            let publication = {
                let mut cache = self
                    .inner
                    .cache
                    .lock()
                    .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
                cache.publish_region_lookup(loaded)
            };
            match publication {
                Ok(RegionLookupApplication::Published(location)) => return Ok(Ok(*location)),
                Ok(RegionLookupApplication::Retry) => {}
                Err(error) => return Ok(Err(error)),
            }
        }
    }

    /// Runs a foreground operation against the same canonical cache authority.
    pub fn with_cache<R>(
        &self,
        operation: impl FnOnce(&mut RegionCache<L>) -> R,
    ) -> Result<R, BackgroundRegionCacheError> {
        let mut cache = self
            .inner
            .cache
            .lock()
            .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
        Ok(operation(&mut cache))
    }

    /// Coalesces any number of pending store-check wakeups into one pass.
    pub fn trigger_store_check(&self) -> Result<bool, BackgroundRegionCacheError> {
        let (state, wake) = &*self.inner.driver;
        let mut state = state
            .lock()
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)?;
        if state.shutdown || state.closed {
            return Ok(false);
        }
        let newly_scheduled = !state.triggered;
        state.triggered = true;
        wake.notify_one();
        Ok(newly_scheduled)
    }

    /// Returns the number of fully completed maintenance passes.
    pub fn completed_rounds(&self) -> Result<u64, BackgroundRegionCacheError> {
        let (state, _) = &*self.inner.driver;
        state
            .lock()
            .map(|state| state.completed_rounds)
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)
    }

    /// Returns the last fully completed pass, if any.
    pub fn last_round(
        &self,
    ) -> Result<Option<BackgroundMaintenanceRound>, BackgroundRegionCacheError> {
        let (state, _) = &*self.inner.driver;
        state
            .lock()
            .map(|state| state.last_round)
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)
    }

    /// Consumes the unique owner, cancels the sole worker, and joins it once.
    ///
    /// Dropping the last handle performs the same shutdown automatically. A
    /// shared handle cannot terminate the worker while another owner uses it.
    pub fn shutdown(self) -> Result<(), BackgroundRegionCacheError> {
        let inner = Arc::try_unwrap(self.inner).map_err(|inner| {
            let owners = Arc::strong_count(&inner);
            BackgroundRegionCacheError::SharedOwners { owners }
        })?;
        shutdown_inner(&inner)
    }

    /// Whether shutdown has completed and no worker remains.
    pub fn is_closed(&self) -> Result<bool, BackgroundRegionCacheError> {
        let (state, _) = &*self.inner.driver;
        state
            .lock()
            .map(|state| state.closed)
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)
    }
}

impl<L: RegionRecoveryLoader> BackgroundRegionCache<L> {
    /// Applies one region error while keeping EpochNotMatch store hydration
    /// outside the canonical cache lock.
    pub fn on_region_error(
        &self,
        error: &tidb_proto::errorpb::Error,
        attempt: RegionAttempt,
        backoff: &mut RegionBackoffBudget,
    ) -> Result<Result<RegionErrorDisposition, RegionRecoveryError>, BackgroundRegionCacheError>
    {
        let recovery = {
            let mut cache = self
                .inner
                .cache
                .lock()
                .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
            cache.plan_region_error(error, attempt, backoff)
        };
        let recovery = match recovery {
            Ok(recovery) => recovery,
            Err(error) => return Ok(Err(error)),
        };
        let plan = match recovery {
            RegionErrorRecoveryPlan::Complete(disposition) => return Ok(Ok(disposition)),
            RegionErrorRecoveryPlan::HydrateEpochNotMatch(plan) => plan,
        };
        let replacements = match self
            .inner
            .loader
            .hydrate_regions(&plan.metadata, plan.attempt.store_id)
        {
            Ok(replacements) => replacements,
            Err(error) => return Ok(Err(error)),
        };
        let mut cache = self
            .inner
            .cache
            .lock()
            .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
        Ok(cache.publish_epoch_not_match(plan, replacements))
    }
}

impl<L> BackgroundRegionCache<L>
where
    L: RegionQueryLoader + Send + 'static,
{
    /// Starts full store refresh plus bounded cache GC over one cache allocation.
    pub fn start(
        cache: RegionCache<L>,
        interval: Duration,
        gc_limit: usize,
    ) -> Result<Self, BackgroundRegionCacheError> {
        let loader = cache.loader_handle();
        Self::start_with_round(
            cache,
            interval,
            gc_limit,
            move |shared, triggered, gc_limit| {
                let (plans, regions) = {
                    let mut cache = shared.lock().ok()?;
                    (
                        cache.plan_store_refreshes(triggered),
                        cache.maintain_entries_bounded(gc_limit),
                    )
                };
                let attempted = plans.len();
                let observations = plans
                    .into_iter()
                    .map(|plan| loader.load_store(plan))
                    .collect::<Vec<_>>();
                let mut stores = StoreMaintenanceRound {
                    attempted,
                    ..StoreMaintenanceRound::default()
                };
                let mut cache = shared.lock().ok()?;
                for observation in observations {
                    match cache.publish_store_refresh(observation) {
                        StoreRefreshApplication::Unchanged => {}
                        StoreRefreshApplication::Refreshed => stores.refreshed += 1,
                        StoreRefreshApplication::Removed => stores.removed += 1,
                        StoreRefreshApplication::Failed => stores.failed += 1,
                        StoreRefreshApplication::StaleDiscarded => stores.stale_discarded += 1,
                    }
                }
                Some(BackgroundMaintenanceRound {
                    triggered,
                    regions,
                    stores,
                })
            },
        )
    }
}

impl<L> Drop for BackgroundRegionCacheInner<L> {
    fn drop(&mut self) {
        let (state, wake) = &*self.driver;
        if let Ok(mut state) = state.lock() {
            state.shutdown = true;
            wake.notify_one();
        }
        let worker = match self.worker.get_mut() {
            Ok(worker) => worker,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(worker) = worker.take() {
            let _ = worker.join();
        }
    }
}

fn shutdown_inner<L>(
    inner: &BackgroundRegionCacheInner<L>,
) -> Result<(), BackgroundRegionCacheError> {
    let (state, wake) = &*inner.driver;
    {
        let mut state = state
            .lock()
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)?;
        state.shutdown = true;
        wake.notify_one();
    }
    let mut worker = inner
        .worker
        .lock()
        .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)?;
    if let Some(worker) = worker.take() {
        worker
            .join()
            .map_err(|_| BackgroundRegionCacheError::WorkerPanicked)?;
    }
    Ok(())
}

fn maintenance_loop<L, F>(
    cache: Arc<Mutex<RegionCache<L>>>,
    driver: Arc<(Mutex<DriverState>, Condvar)>,
    interval: Duration,
    gc_limit: usize,
    mut round: F,
) where
    L: RegionLoader + Send + 'static,
    F: FnMut(&Arc<Mutex<RegionCache<L>>>, bool, usize) -> Option<BackgroundMaintenanceRound>,
{
    loop {
        let (state, wake) = &*driver;
        let Ok(state_guard) = state.lock() else {
            return;
        };
        let Ok((mut state_guard, _)) = wake.wait_timeout_while(state_guard, interval, |state| {
            !state.shutdown && !state.triggered
        }) else {
            return;
        };
        if state_guard.shutdown {
            state_guard.closed = true;
            return;
        }
        let triggered = std::mem::take(&mut state_guard.triggered);
        drop(state_guard);

        let Some(completed) = round(&cache, triggered, gc_limit) else {
            if let Ok(mut state) = state.lock() {
                state.closed = true;
            }
            return;
        };

        let Ok(mut state) = state.lock() else {
            return;
        };
        state.completed_rounds = state.completed_rounds.saturating_add(1);
        state.last_round = Some(completed);
    }
}
