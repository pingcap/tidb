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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheEntryState {
    expires_at_seconds: u64,
    flags: u8,
}

impl CacheEntryState {
    /// Creates an accessed entry with an absolute expiry.
    #[must_use]
    pub const fn new(expires_at_seconds: u64) -> Self {
        Self {
            expires_at_seconds,
            flags: 0,
        }
    }

    /// Exact absolute expiry used by deterministic source tests.
    #[must_use]
    pub const fn expires_at_seconds(self) -> u64 {
        self.expires_at_seconds
    }

    /// Adds the source synchronization flag without replacing sibling flags.
    pub const fn mark(&mut self, state: CacheReloadState) {
        self.flags |= state.flag();
    }

    /// Whether one source synchronization flag is currently set.
    #[must_use]
    pub const fn is_marked(self, state: CacheReloadState) -> bool {
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
    pub const fn check_and_renew(
        &mut self,
        now_seconds: u64,
        base_ttl_seconds: u64,
        next_expiry: u64,
    ) -> bool {
        if self.flags & (RELOAD_ON_ACCESS | DELAYED_RELOAD_READY) != 0
            || now_seconds > self.expires_at_seconds
        {
            return false;
        }
        if self.flags & EXPIRE_AFTER_TTL != 0
            || self.expires_at_seconds > now_seconds.saturating_add(base_ttl_seconds)
        {
            return true;
        }
        if next_expiry > self.expires_at_seconds {
            self.expires_at_seconds = next_expiry;
        }
        true
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
