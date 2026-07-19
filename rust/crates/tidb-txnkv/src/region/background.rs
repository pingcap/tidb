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
    RegionLookupApplication, RegionLookupSelection, SharedRegionLoader, StoreLivenessApplication,
    StoreLivenessResult, StoreRefreshApplication,
};
use super::recovery::RegionErrorRecoveryPlan;
use super::{
    KeyRange, RegionAttempt, RegionBackoffBudget, RegionCache, RegionErrorDisposition,
    RegionGcRound, RegionLoader, RegionLocation, RegionQueryLoader, RegionRecoveryError,
    RegionRecoveryLoader, RegionRouteError, RegionVerId, StoreMaintenanceRound,
};

/// One-shot TiKV health capability used by the bounded maintenance worker.
///
/// The worker plans immutable address/generation observations under the cache
/// lock, releases the lock for this call, and stale-checks the result before
/// publishing it back to the canonical store.
pub trait StoreLivenessProbe: Send + 'static {
    /// Checks one resolved TiKV address without mutating RegionCache state.
    fn probe(&self, address: &str, timeout: Duration) -> super::StoreLiveness;
}

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
    /// A zero health-check timeout would classify every store as unavailable.
    ZeroLivenessTimeout,
    /// The sole maintenance thread could not be created.
    Spawn(String),
    /// A previous panic poisoned the canonical cache lock.
    CachePoisoned,
    /// A previous panic poisoned the driver-state lock.
    DriverPoisoned,
    /// The foreground lease admission state was poisoned.
    LeaseStatePoisoned,
    /// The unique owner already closed foreground lease admission.
    LeaseAdmissionClosed,
    /// The foreground lease counter cannot represent another session.
    LeaseLimit,
    /// The maintenance thread panicked during shutdown.
    WorkerPanicked,
    /// Explicit shutdown requires the last handle to the shared authority.
    SharedOwners {
        /// Number of live foreground leases observed by the unique owner.
        owners: usize,
    },
    /// More than one independent shutdown action failed.
    Multiple(Vec<BackgroundRegionCacheError>),
}

impl std::fmt::Display for BackgroundRegionCacheError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroInterval => formatter.write_str("background interval must be nonzero"),
            Self::ZeroGcLimit => formatter.write_str("background GC limit must be nonzero"),
            Self::ZeroLivenessTimeout => {
                formatter.write_str("background liveness timeout must be nonzero")
            }
            Self::Spawn(message) => {
                write!(formatter, "failed to spawn maintenance driver: {message}")
            }
            Self::CachePoisoned => formatter.write_str("canonical region cache lock is poisoned"),
            Self::DriverPoisoned => formatter.write_str("maintenance driver lock is poisoned"),
            Self::LeaseStatePoisoned => {
                formatter.write_str("background cache lease state is poisoned")
            }
            Self::LeaseAdmissionClosed => {
                formatter.write_str("background cache lease admission is closed")
            }
            Self::LeaseLimit => formatter.write_str("background cache lease limit reached"),
            Self::WorkerPanicked => formatter.write_str("maintenance driver panicked"),
            Self::SharedOwners { owners } => write!(
                formatter,
                "explicit shutdown requires unique ownership; observed {owners} live handles"
            ),
            Self::Multiple(failures) => {
                formatter.write_str("multiple background cache shutdown failures")?;
                for failure in failures {
                    write!(formatter, "; {failure}")?;
                }
                Ok(())
            }
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

/// Shared cache state borrowed by foreground session leases and the worker.
struct BackgroundRegionCacheShared<L> {
    cache: Arc<Mutex<RegionCache<L>>>,
    loader: SharedRegionLoader<L>,
    driver: Arc<(Mutex<DriverState>, Condvar)>,
    leases: Mutex<CacheLeaseAdmission>,
}

struct CacheLeaseAdmission {
    accepting: bool,
    active: usize,
}

/// Cloneable foreground lease over the sole synchronized cache.
pub struct BackgroundRegionCache<L> {
    shared: Arc<BackgroundRegionCacheShared<L>>,
    counted_lease: bool,
}

enum MaintenanceWorker {
    Running(JoinHandle<Result<(), BackgroundRegionCacheError>>),
    Joined,
}

/// Unique shutdown and join authority for one background cache worker.
///
/// Foreground code receives only [`BackgroundRegionCache`] leases. Keeping the
/// join handle here makes explicit shutdown independent of `Arc` uniqueness.
pub struct BackgroundRegionCacheOwner<L> {
    handle: BackgroundRegionCache<L>,
    worker: Mutex<MaintenanceWorker>,
}

impl<L> Clone for BackgroundRegionCache<L> {
    fn clone(&self) -> Self {
        self.open_lease()
            .expect("cannot clone a background cache lease after admission closes")
    }
}

impl<L> Drop for BackgroundRegionCache<L> {
    fn drop(&mut self) {
        if self.counted_lease {
            let mut leases = match self.shared.leases.lock() {
                Ok(leases) => leases,
                Err(poisoned) => poisoned.into_inner(),
            };
            leases.active = leases
                .active
                .checked_sub(1)
                .expect("background cache lease count underflow");
        }
    }
}

impl<L> std::ops::Deref for BackgroundRegionCacheOwner<L> {
    type Target = BackgroundRegionCache<L>;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<L> BackgroundRegionCache<L> {
    pub(crate) fn open_lease(&self) -> Result<Self, BackgroundRegionCacheError> {
        let mut leases = self
            .shared
            .leases
            .lock()
            .map_err(|_| BackgroundRegionCacheError::LeaseStatePoisoned)?;
        if !leases.accepting {
            return Err(BackgroundRegionCacheError::LeaseAdmissionClosed);
        }
        leases.active = leases
            .active
            .checked_add(1)
            .ok_or(BackgroundRegionCacheError::LeaseLimit)?;
        drop(leases);
        Ok(Self {
            shared: Arc::clone(&self.shared),
            counted_lease: true,
        })
    }

    pub(crate) fn clone_opener(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            counted_lease: false,
        }
    }

    /// Wraps one canonical cache without starting a worker.
    ///
    /// This is the injection seam for non-Send test loaders. Production
    /// runtimes use [`Self::start`] and therefore cannot bypass maintenance.
    #[must_use]
    pub(crate) fn without_worker(cache: RegionCache<L>) -> Self {
        let loader = cache.loader_handle();
        Self {
            shared: Arc::new(BackgroundRegionCacheShared {
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
                leases: Mutex::new(CacheLeaseAdmission {
                    accepting: true,
                    active: 0,
                }),
            }),
            counted_lease: false,
        }
    }

    /// Moves the canonical cache under one lock and starts bounded cache GC.
    pub fn start_gc(
        cache: RegionCache<L>,
        interval: Duration,
        gc_limit: usize,
    ) -> Result<BackgroundRegionCacheOwner<L>, BackgroundRegionCacheError>
    where
        L: RegionLoader + Send + 'static,
    {
        Self::start_with_round(cache, interval, gc_limit, |shared, triggered, gc_limit| {
            let mut cache = shared
                .lock()
                .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
            Ok(BackgroundMaintenanceRound {
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
    ) -> Result<BackgroundRegionCacheOwner<L>, BackgroundRegionCacheError>
    where
        L: RegionLoader + Send + 'static,
        F: FnMut(
                &Arc<Mutex<RegionCache<L>>>,
                bool,
                usize,
            ) -> Result<BackgroundMaintenanceRound, BackgroundRegionCacheError>
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
        let shared = Arc::new(BackgroundRegionCacheShared {
            cache,
            loader,
            driver,
            leases: Mutex::new(CacheLeaseAdmission {
                accepting: true,
                active: 0,
            }),
        });
        Ok(BackgroundRegionCacheOwner {
            handle: Self {
                shared,
                counted_lease: false,
            },
            worker: Mutex::new(MaintenanceWorker::Running(worker)),
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
        self.shared
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
                    .shared
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
            let loaded = self.shared.loader.load_region(plan);
            let publication = {
                let mut cache = self
                    .shared
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
            .shared
            .cache
            .lock()
            .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
        Ok(operation(&mut cache))
    }

    /// Coalesces any number of pending store-check wakeups into one pass.
    pub fn trigger_store_check(&self) -> Result<bool, BackgroundRegionCacheError> {
        let (state, wake) = &*self.shared.driver;
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
        let (state, _) = &*self.shared.driver;
        state
            .lock()
            .map(|state| state.completed_rounds)
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)
    }

    /// Returns the last fully completed pass, if any.
    pub fn last_round(
        &self,
    ) -> Result<Option<BackgroundMaintenanceRound>, BackgroundRegionCacheError> {
        let (state, _) = &*self.shared.driver;
        state
            .lock()
            .map(|state| state.last_round)
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)
    }

    /// Whether shutdown has completed and no worker remains.
    pub fn is_closed(&self) -> Result<bool, BackgroundRegionCacheError> {
        let (state, _) = &*self.shared.driver;
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
                .shared
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
            RegionErrorRecoveryPlan::HydrateEpochNotMatch(plan) => *plan,
        };
        let replacements = match self
            .shared
            .loader
            .hydrate_regions(&plan.metadata, plan.attempt.store_id)
        {
            Ok(replacements) => replacements,
            Err(error) => return Ok(Err(error)),
        };
        let mut cache = self
            .shared
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
    ) -> Result<BackgroundRegionCacheOwner<L>, BackgroundRegionCacheError> {
        Self::start_store_maintenance(cache, interval, gc_limit, None)
    }

    /// Starts metadata refresh, stale-safe TiKV health recovery, and bounded
    /// cache GC over one cache allocation.
    pub fn start_with_liveness<P>(
        cache: RegionCache<L>,
        probe: P,
        interval: Duration,
        gc_limit: usize,
        liveness_timeout: Duration,
    ) -> Result<BackgroundRegionCacheOwner<L>, BackgroundRegionCacheError>
    where
        P: StoreLivenessProbe,
    {
        if liveness_timeout.is_zero() {
            return Err(BackgroundRegionCacheError::ZeroLivenessTimeout);
        }
        Self::start_store_maintenance(
            cache,
            interval,
            gc_limit,
            Some((Box::new(probe), liveness_timeout)),
        )
    }

    fn start_store_maintenance(
        cache: RegionCache<L>,
        interval: Duration,
        gc_limit: usize,
        liveness: Option<(Box<dyn StoreLivenessProbe>, Duration)>,
    ) -> Result<BackgroundRegionCacheOwner<L>, BackgroundRegionCacheError> {
        let loader = cache.loader_handle();
        Self::start_with_round(
            cache,
            interval,
            gc_limit,
            move |shared, triggered, gc_limit| {
                let (plans, regions) = {
                    let mut cache = shared
                        .lock()
                        .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
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
                let (mut stores, liveness_plans) = {
                    let mut stores = StoreMaintenanceRound {
                        attempted,
                        ..StoreMaintenanceRound::default()
                    };
                    let mut cache = shared
                        .lock()
                        .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
                    for observation in observations {
                        match cache.publish_store_refresh(observation) {
                            StoreRefreshApplication::Unchanged => {}
                            StoreRefreshApplication::Refreshed => stores.refreshed += 1,
                            StoreRefreshApplication::Removed => stores.removed += 1,
                            StoreRefreshApplication::Failed => stores.failed += 1,
                            StoreRefreshApplication::StaleDiscarded => {
                                stores.stale_discarded += 1;
                            }
                        }
                    }
                    let liveness_plans = if liveness.is_some() {
                        cache.plan_store_liveness_checks()
                    } else {
                        Vec::new()
                    };
                    (stores, liveness_plans)
                };
                let liveness_results = liveness_plans
                    .into_iter()
                    .map(|plan| {
                        let (probe, timeout) = liveness
                            .as_ref()
                            .expect("liveness plans require an enabled probe");
                        let observed = probe.probe(&plan.address, *timeout);
                        StoreLivenessResult {
                            plan,
                            liveness: observed,
                        }
                    })
                    .collect::<Vec<_>>();
                let mut cache = shared
                    .lock()
                    .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
                for result in liveness_results {
                    if cache.publish_store_liveness(result)
                        == StoreLivenessApplication::StaleDiscarded
                    {
                        stores.stale_discarded += 1;
                    }
                }
                Ok(BackgroundMaintenanceRound {
                    triggered,
                    regions,
                    stores,
                })
            },
        )
    }
}

impl<L> BackgroundRegionCacheOwner<L> {
    /// Returns a foreground lease that cannot stop or join the worker.
    pub fn handle(&self) -> Result<BackgroundRegionCache<L>, BackgroundRegionCacheError> {
        self.handle.open_lease()
    }

    pub(crate) fn opener_handle(&self) -> BackgroundRegionCache<L> {
        self.handle.clone_opener()
    }

    /// Number of foreground cache leases that must drain before shutdown.
    #[must_use]
    pub fn active_leases(&self) -> usize {
        match self.handle.shared.leases.lock() {
            Ok(leases) => leases.active,
            Err(poisoned) => poisoned.into_inner().active,
        }
    }

    /// Cancels and explicitly joins the maintenance worker exactly once.
    pub fn shutdown(&self) -> Result<(), BackgroundRegionCacheError> {
        {
            let mut leases = self
                .handle
                .shared
                .leases
                .lock()
                .map_err(|_| BackgroundRegionCacheError::LeaseStatePoisoned)?;
            leases.accepting = false;
            if leases.active != 0 {
                let owners = leases.active;
                leases.accepting = true;
                return Err(BackgroundRegionCacheError::SharedOwners { owners });
            }
        }
        shutdown_worker(&self.handle.shared, &self.worker)
    }
}

impl<L> Drop for BackgroundRegionCacheOwner<L> {
    fn drop(&mut self) {
        let mut leases = match self.handle.shared.leases.lock() {
            Ok(leases) => leases,
            Err(poisoned) => poisoned.into_inner(),
        };
        leases.accepting = false;
        drop(leases);
        let _ = shutdown_worker(&self.handle.shared, &self.worker);
    }
}

fn shutdown_worker<L>(
    shared: &BackgroundRegionCacheShared<L>,
    worker: &Mutex<MaintenanceWorker>,
) -> Result<(), BackgroundRegionCacheError> {
    let mut failures = Vec::new();
    let (state, wake) = &*shared.driver;
    {
        let mut state = match state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                failures.push(BackgroundRegionCacheError::DriverPoisoned);
                poisoned.into_inner()
            }
        };
        state.shutdown = true;
        wake.notify_one();
    }
    let mut worker = match worker.lock() {
        Ok(worker) => worker,
        Err(poisoned) => {
            failures.push(BackgroundRegionCacheError::DriverPoisoned);
            poisoned.into_inner()
        }
    };
    let owned = std::mem::replace(&mut *worker, MaintenanceWorker::Joined);
    if let MaintenanceWorker::Running(worker) = owned {
        match worker.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(error),
            Err(_) => failures.push(BackgroundRegionCacheError::WorkerPanicked),
        }
    }
    match failures.len() {
        0 => Ok(()),
        1 => Err(failures.pop().expect("one shutdown failure")),
        _ => Err(BackgroundRegionCacheError::Multiple(failures)),
    }
}

fn maintenance_loop<L, F>(
    cache: Arc<Mutex<RegionCache<L>>>,
    driver: Arc<(Mutex<DriverState>, Condvar)>,
    interval: Duration,
    gc_limit: usize,
    mut round: F,
) -> Result<(), BackgroundRegionCacheError>
where
    L: RegionLoader + Send + 'static,
    F: FnMut(
        &Arc<Mutex<RegionCache<L>>>,
        bool,
        usize,
    ) -> Result<BackgroundMaintenanceRound, BackgroundRegionCacheError>,
{
    loop {
        let (state, wake) = &*driver;
        let state_guard = state
            .lock()
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)?;
        let (mut state_guard, _) = wake
            .wait_timeout_while(state_guard, interval, |state| {
                !state.shutdown && !state.triggered
            })
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)?;
        if state_guard.shutdown {
            state_guard.closed = true;
            return Ok(());
        }
        let triggered = std::mem::take(&mut state_guard.triggered);
        drop(state_guard);

        let completed = match round(&cache, triggered, gc_limit) {
            Ok(completed) => completed,
            Err(error) => {
                let mut state = state
                    .lock()
                    .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)?;
                state.closed = true;
                return Err(error);
            }
        };

        let mut state = state
            .lock()
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)?;
        state.completed_rounds = state.completed_rounds.saturating_add(1);
        state.last_round = Some(completed);
    }
}
