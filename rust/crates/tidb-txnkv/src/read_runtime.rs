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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::region::{
    BackgroundRegionCache, BackgroundRegionCacheError, KeyRange, RegionCache, RegionLoader,
    RegionLocation, RegionQueryLoader, RegionRouteError,
};

const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_GC_LIMIT: usize = 50;
static NEXT_READ_AUTHORITY_ID: AtomicU64 = AtomicU64::new(1);

fn next_read_authority_id() -> u64 {
    NEXT_READ_AUTHORITY_ID.fetch_add(1, Ordering::Relaxed)
}

/// Process-owned region-cache and TiKV-client capability authority.
///
/// This value is intentionally not `Clone`: it owns the lifetime of the sole
/// maintenance worker. A server shares it behind `Arc` and calls
/// [`Self::open_session`] inside each connection worker. The returned session
/// runtime clones only the cheap client capability and cache handle, so it
/// cannot create or terminate a process worker.
pub struct SharedReadAuthority<C, L> {
    client: C,
    region_cache: BackgroundRegionCache<L>,
    cluster_id: u64,
    authority_id: u64,
}

impl<C, L> SharedReadAuthority<C, L>
where
    C: Clone,
    L: RegionQueryLoader + Send + 'static,
{
    /// Starts the one production maintenance worker over the canonical cache.
    pub fn start(
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
            client,
            region_cache,
            cluster_id,
            authority_id: next_read_authority_id(),
        })
    }

    /// Creates one thread-local session from process-owned capabilities.
    ///
    /// The `Rc<RefCell<_>>` is allocated here, on the calling worker. It is
    /// never stored in the process authority or moved between workers.
    pub fn open_session(&self) -> Result<SharedReadRuntime<C, L>, BackgroundRegionCacheError> {
        SharedReadRuntime::from_shared_authorities(
            self.client.clone(),
            self.region_cache.clone(),
            self.authority_id,
        )
    }

    /// Cluster identity owned by the canonical region cache.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Stable process authority identity for lifecycle evidence.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }

    /// Stops and joins the maintenance worker after every session is drained.
    pub fn shutdown(self) -> Result<(), BackgroundRegionCacheError> {
        self.region_cache.shutdown()
    }
}

/// One client handle and one region-cache handle shared by read-path policies.
///
/// Cloning this value clones only the handles. It cannot create another
/// client, channel pool, region cache, topology map, or retry authority.
pub struct SharedReadRuntime<C, L> {
    client: Rc<RefCell<C>>,
    region_cache: BackgroundRegionCache<L>,
    cluster_id: u64,
    authority_id: u64,
}

impl<C, L> Clone for SharedReadRuntime<C, L> {
    fn clone(&self) -> Self {
        Self {
            client: Rc::clone(&self.client),
            region_cache: self.region_cache.clone(),
            cluster_id: self.cluster_id,
            authority_id: self.authority_id,
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
            authority_id: next_read_authority_id(),
        }
    }

    /// Creates a worker-local session over already-owned process authorities.
    fn from_shared_authorities(
        client: C,
        region_cache: BackgroundRegionCache<L>,
        authority_id: u64,
    ) -> Result<Self, BackgroundRegionCacheError> {
        let cluster_id = region_cache.with_cache(|cache| cache.cluster_id())?;
        Ok(Self {
            client: Rc::new(RefCell::new(client)),
            region_cache,
            cluster_id,
            authority_id,
        })
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

    /// Finds one key without holding the canonical cache lock across loader I/O.
    pub fn locate_key(
        &self,
        key: &[u8],
    ) -> Result<Result<RegionLocation, RegionRouteError>, BackgroundRegionCacheError> {
        self.region_cache.locate_key(key)
    }

    /// Resolves ranges without holding the canonical cache lock across loader I/O.
    pub fn locate_ranges(
        &self,
        ranges: &[KeyRange],
    ) -> Result<Result<Vec<RegionLocation>, RegionRouteError>, BackgroundRegionCacheError> {
        self.region_cache.locate_ranges(ranges)
    }

    /// Coalesces a store-check request into the sole maintenance worker.
    pub fn trigger_store_check(&self) -> Result<bool, BackgroundRegionCacheError> {
        self.region_cache.trigger_store_check()
    }

    /// Cluster identity owned by the sole region cache.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Stable identity of the process authority that opened this session.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }
}
