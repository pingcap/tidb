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

//! Source-shaped RegionCache TTL and reload state.

use std::sync::atomic::{AtomicU64, Ordering};

const RELOAD_ON_ACCESS: u8 = 1;
const EXPIRE_AFTER_TTL: u8 = 1 << 1;
const DELAYED_RELOAD_PENDING: u8 = 1 << 2;
const DELAYED_RELOAD_READY: u8 = 1 << 3;

/// Reload state stored beside one canonical cached region.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheReloadState {
    /// Ordinary valid cache entry.
    Current,
    /// Reload on the next foreground access.
    ReloadOnAccess,
    /// Expire at the existing TTL even when accessed repeatedly.
    ExpireAfterTtl,
    /// Wait for one background scan before a reload-by-ID is allowed.
    DelayedReloadPending,
    /// The background scan released the delayed reload.
    DelayedReloadReady,
}

/// TTL and synchronization flags for one cached snapshot.
#[derive(Debug)]
pub struct CacheEntryState {
    expires_at_seconds: AtomicU64,
    flags: u8,
}

impl Clone for CacheEntryState {
    fn clone(&self) -> Self {
        Self {
            expires_at_seconds: AtomicU64::new(self.expires_at_seconds()),
            flags: self.flags,
        }
    }
}

impl PartialEq for CacheEntryState {
    fn eq(&self, other: &Self) -> bool {
        self.expires_at_seconds() == other.expires_at_seconds() && self.flags == other.flags
    }
}

impl Eq for CacheEntryState {}

impl CacheEntryState {
    /// Creates an accessed entry with an absolute expiry.
    #[must_use]
    pub const fn new(expires_at_seconds: u64) -> Self {
        Self {
            expires_at_seconds: AtomicU64::new(expires_at_seconds),
            flags: 0,
        }
    }

    /// Exact absolute expiry used by deterministic source tests.
    #[must_use]
    pub fn expires_at_seconds(&self) -> u64 {
        self.expires_at_seconds.load(Ordering::Relaxed)
    }

    /// Adds the source synchronization flag without replacing sibling flags.
    pub const fn mark(&mut self, state: CacheReloadState) {
        self.flags |= state.flag();
    }

    /// Whether one source synchronization flag is currently set.
    #[must_use]
    pub const fn is_marked(&self, state: CacheReloadState) -> bool {
        let flag = state.flag();
        flag != 0 && self.flags & flag != 0
    }

    /// Advances Pending to Ready in one background-GC transition.
    pub const fn release_delayed_reload(&mut self) -> bool {
        if self.flags & DELAYED_RELOAD_PENDING == 0 {
            return false;
        }
        self.flags &= !DELAYED_RELOAD_PENDING;
        self.flags |= DELAYED_RELOAD_READY;
        true
    }

    /// Implements client-go's strict `now > ttl` expiry and near-boundary
    /// renewal. `next_expiry` is injected so jitter remains deterministic.
    pub fn check_and_renew(
        &self,
        now_seconds: u64,
        base_ttl_seconds: u64,
        next_expiry: u64,
    ) -> bool {
        // Cache topology/reload flags stay protected by the cache's RwLock.
        // Readers may renew the TTL concurrently, like Go checkRegionCacheTTL.
        let mut expiry = self.expires_at_seconds();
        loop {
            if self.flags & (RELOAD_ON_ACCESS | DELAYED_RELOAD_READY) != 0 || now_seconds > expiry {
                return false;
            }
            if self.flags & EXPIRE_AFTER_TTL != 0
                || expiry > now_seconds.saturating_add(base_ttl_seconds)
                || next_expiry <= expiry
            {
                return true;
            }
            match self.expires_at_seconds.compare_exchange_weak(
                expiry,
                next_expiry,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => expiry = current,
            }
        }
    }
}

impl CacheReloadState {
    const fn flag(self) -> u8 {
        match self {
            Self::Current => 0,
            Self::ReloadOnAccess => RELOAD_ON_ACCESS,
            Self::ExpireAfterTtl => EXPIRE_AFTER_TTL,
            Self::DelayedReloadPending => DELAYED_RELOAD_PENDING,
            Self::DelayedReloadReady => DELAYED_RELOAD_READY,
        }
    }
}
