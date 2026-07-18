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

use super::StoreRoutingHealth;

/// Foreground TiKV liveness result shared by transport and region routing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StoreLiveness {
    /// The foreground health request reached a serving TiKV.
    #[default]
    Reachable,
    /// The target is known not to be serving or reachable.
    Unreachable,
    /// Liveness could not be decided without treating the store as healthy.
    Unknown,
}

/// Whether the canonical store address may be used by a new request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StoreResolveState {
    /// The address was resolved by the metadata loader.
    #[default]
    Resolved,
    /// A failed generation must be re-resolved before reuse.
    NeedCheck,
}

/// Immutable view of the sole RegionCache-owned store authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreState {
    pub(crate) id: u64,
    pub(crate) address: String,
    pub(crate) epoch: u64,
    pub(crate) resolve_state: StoreResolveState,
    pub(crate) liveness: StoreLiveness,
    pub(crate) routing_health: StoreRoutingHealth,
}

impl StoreState {
    /// Store identifier.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Current resolved TiKV address.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Monotonically increasing local failure generation.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Current address resolution state.
    #[must_use]
    pub const fn resolve_state(&self) -> StoreResolveState {
        self.resolve_state
    }

    /// Latest foreground liveness observation.
    #[must_use]
    pub const fn liveness(&self) -> StoreLiveness {
        self.liveness
    }

    /// Immutable load and slow-health facts used by replica policy.
    #[must_use]
    pub const fn routing_health(&self) -> &StoreRoutingHealth {
        &self.routing_health
    }
}

/// Exact result of applying one foreground send-failure observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreFailureOutcome {
    /// The health request proved the observed store generation is reachable.
    Reachable {
        /// Unchanged canonical store epoch.
        epoch: u64,
    },
    /// A failed direct leader remains the logical target while a healthy
    /// follower can serve as its physical proxy.
    ForwardingRequired {
        /// Preserved canonical leader generation.
        epoch: u64,
    },
    /// A non-reachable result invalidated exactly the observed generation.
    Invalidated {
        /// Epoch carried by the failed request.
        previous_epoch: u64,
        /// New canonical epoch.
        current_epoch: u64,
    },
}
