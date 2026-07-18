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

use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use super::{RegionCache, RegionGcRound, RegionQueryLoader, StoreMaintenanceRound};

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
pub struct BackgroundRegionCache<L> {
    cache: Arc<Mutex<RegionCache<L>>>,
    driver: Arc<(Mutex<DriverState>, Condvar)>,
    worker: Option<JoinHandle<()>>,
}

impl<L> BackgroundRegionCache<L>
where
    L: RegionQueryLoader + Send + 'static,
{
    /// Moves the canonical cache under one lock and starts exactly one worker.
    pub fn start(
        cache: RegionCache<L>,
        interval: Duration,
        gc_limit: usize,
    ) -> Result<Self, BackgroundRegionCacheError> {
        if interval.is_zero() {
            return Err(BackgroundRegionCacheError::ZeroInterval);
        }
        if gc_limit == 0 {
            return Err(BackgroundRegionCacheError::ZeroGcLimit);
        }
        let cache = Arc::new(Mutex::new(cache));
        let driver = Arc::new((Mutex::new(DriverState::default()), Condvar::new()));
        let worker_cache = Arc::clone(&cache);
        let worker_driver = Arc::clone(&driver);
        let worker = std::thread::Builder::new()
            .name("tidb-region-maintenance".to_owned())
            .spawn(move || maintenance_loop(worker_cache, worker_driver, interval, gc_limit))
            .map_err(|error| BackgroundRegionCacheError::Spawn(error.to_string()))?;
        Ok(Self {
            cache,
            driver,
            worker: Some(worker),
        })
    }

    /// Runs a foreground operation against the same canonical cache authority.
    pub fn with_cache<R>(
        &self,
        operation: impl FnOnce(&mut RegionCache<L>) -> R,
    ) -> Result<R, BackgroundRegionCacheError> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| BackgroundRegionCacheError::CachePoisoned)?;
        Ok(operation(&mut cache))
    }

    /// Coalesces any number of pending store-check wakeups into one pass.
    pub fn trigger_store_check(&self) -> Result<bool, BackgroundRegionCacheError> {
        let (state, wake) = &*self.driver;
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
        let (state, _) = &*self.driver;
        state
            .lock()
            .map(|state| state.completed_rounds)
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)
    }

    /// Returns the last fully completed pass, if any.
    pub fn last_round(
        &self,
    ) -> Result<Option<BackgroundMaintenanceRound>, BackgroundRegionCacheError> {
        let (state, _) = &*self.driver;
        state
            .lock()
            .map(|state| state.last_round)
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)
    }

    /// Cancels the sole worker and waits for its termination.
    pub fn shutdown(&mut self) -> Result<(), BackgroundRegionCacheError> {
        let (state, wake) = &*self.driver;
        {
            let mut state = state
                .lock()
                .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)?;
            state.shutdown = true;
            wake.notify_one();
        }
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| BackgroundRegionCacheError::WorkerPanicked)?;
        }
        Ok(())
    }

    /// Whether shutdown has completed and no worker remains.
    pub fn is_closed(&self) -> Result<bool, BackgroundRegionCacheError> {
        let (state, _) = &*self.driver;
        state
            .lock()
            .map(|state| state.closed)
            .map_err(|_| BackgroundRegionCacheError::DriverPoisoned)
    }
}

impl<L> Drop for BackgroundRegionCache<L> {
    fn drop(&mut self) {
        let (state, wake) = &*self.driver;
        if let Ok(mut state) = state.lock() {
            state.shutdown = true;
            wake.notify_one();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn maintenance_loop<L>(
    cache: Arc<Mutex<RegionCache<L>>>,
    driver: Arc<(Mutex<DriverState>, Condvar)>,
    interval: Duration,
    gc_limit: usize,
) where
    L: RegionQueryLoader + Send + 'static,
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

        let Ok(mut cache) = cache.lock() else {
            if let Ok(mut state) = state.lock() {
                state.closed = true;
            }
            return;
        };
        let stores = cache.maintain_stores(triggered);
        let regions = cache.maintain_entries_bounded(gc_limit);
        drop(cache);

        let Ok(mut state) = state.lock() else {
            return;
        };
        state.completed_rounds = state.completed_rounds.saturating_add(1);
        state.last_round = Some(BackgroundMaintenanceRound {
            triggered,
            regions,
            stores,
        });
    }
}
