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

use crate::region::{RegionCache, RegionLoader};

/// One client handle and one region-cache handle shared by read-path policies.
///
/// Cloning this value clones only the handles. It cannot create another
/// client, channel pool, region cache, topology map, or retry authority.
pub struct SharedReadRuntime<C, L> {
    client: Rc<RefCell<C>>,
    region_cache: Rc<RefCell<RegionCache<L>>>,
}

impl<C, L> Clone for SharedReadRuntime<C, L> {
    fn clone(&self) -> Self {
        Self {
            client: Rc::clone(&self.client),
            region_cache: Rc::clone(&self.region_cache),
        }
    }
}

impl<C, L: RegionLoader> SharedReadRuntime<C, L> {
    /// Creates the sole client and region-cache handles for a read runtime.
    #[must_use]
    pub fn new(client: C, region_cache: RegionCache<L>) -> Self {
        Self {
            client: Rc::new(RefCell::new(client)),
            region_cache: Rc::new(RefCell::new(region_cache)),
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
    pub fn region_cache_handle(&self) -> Rc<RefCell<RegionCache<L>>> {
        Rc::clone(&self.region_cache)
    }

    /// Borrows the same region-cache cell without cloning a handle.
    #[must_use]
    pub fn region_cache(&self) -> &RefCell<RegionCache<L>> {
        self.region_cache.as_ref()
    }

    /// Cluster identity owned by the sole region cache.
    #[must_use]
    pub fn cluster_id(&self) -> u64 {
        self.region_cache.borrow().cluster_id()
    }
}
