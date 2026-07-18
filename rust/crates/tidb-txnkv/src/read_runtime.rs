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

//! Shared ownership boundary for the retained TiKV read path.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use crate::region::{
    BackgroundRegionCache, BackgroundRegionCacheError, RegionCache, RegionLoader, RegionQueryLoader,
};

const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_GC_LIMIT: usize = 50;

/// One client handle and one region-cache handle shared by read-path policies.
///
/// Cloning this value clones only the handles. It cannot create another
/// client, channel pool, region cache, topology map, or retry authority.
pub struct SharedReadRuntime<C, L> {
    client: Rc<RefCell<C>>,
    region_cache: BackgroundRegionCache<L>,
    cluster_id: u64,
    maintained: bool,
}

impl<C, L> Clone for SharedReadRuntime<C, L> {
    fn clone(&self) -> Self {
        Self {
            client: Rc::clone(&self.client),
            region_cache: self.region_cache.clone(),
            cluster_id: self.cluster_id,
            maintained: self.maintained,
        }
    }
}

impl<C, L: RegionLoader> SharedReadRuntime<C, L> {
    /// Creates the synchronized cache authority for an injected runtime.
    #[must_use]
    pub fn new_injected(client: C, region_cache: RegionCache<L>) -> Self {
        let cluster_id = region_cache.cluster_id();
        Self {
            client: Rc::new(RefCell::new(client)),
            region_cache: BackgroundRegionCache::without_worker(region_cache),
            cluster_id,
            maintained: false,
        }
    }

    /// Returns a handle to the same client authority.
    #[must_use]
    pub fn client_handle(&self) -> Rc<RefCell<C>> {
        Rc::clone(&self.client)
    }

    /// Borrows the same client cell without cloning a handle.
    #[must_use]
    pub fn client(&self) -> &RefCell<C> {
        self.client.as_ref()
    }

    /// Returns a handle to the same region-cache authority.
    #[must_use]
    pub fn region_cache_handle(&self) -> BackgroundRegionCache<L> {
        self.region_cache.clone()
    }

    /// Runs one bounded foreground cache operation under the canonical lock.
    pub fn with_region_cache<R>(
        &self,
        operation: impl FnOnce(&mut RegionCache<L>) -> R,
    ) -> Result<R, BackgroundRegionCacheError> {
        self.region_cache.with_cache(operation)
    }

    /// Coalesces a store-check request into the sole maintenance worker.
    pub fn trigger_store_check(&self) -> Result<bool, BackgroundRegionCacheError> {
        self.region_cache.trigger_store_check()
    }

    /// Cancels and joins the sole maintenance worker exactly once.
    pub fn shutdown(self) -> Result<(), BackgroundRegionCacheError> {
        self.region_cache.shutdown()
    }

    /// Whether this runtime owns the production maintenance worker.
    #[must_use]
    pub const fn is_maintained(&self) -> bool {
        self.maintained
    }

    /// Cluster identity owned by the sole region cache.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }
}

impl<C, L> SharedReadRuntime<C, L>
where
    L: RegionQueryLoader + Send + 'static,
{
    /// Creates the production cache authority with store refresh and cache GC.
    pub fn new_with_maintenance(
        client: C,
        region_cache: RegionCache<L>,
    ) -> Result<Self, BackgroundRegionCacheError> {
        let cluster_id = region_cache.cluster_id();
        let region_cache = BackgroundRegionCache::start(
            region_cache,
            DEFAULT_MAINTENANCE_INTERVAL,
            DEFAULT_GC_LIMIT,
        )?;
        Ok(Self {
            client: Rc::new(RefCell::new(client)),
            region_cache,
            cluster_id,
            maintained: true,
        })
    }
}
