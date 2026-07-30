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
//! Keeping the canonical store set current: which stores need a metadata
//! reload, which need a liveness probe, and how a completed probe is applied
//! without overwriting a store the cache has since replaced.
//!
//! Go boundary: client-go's `region_cache.go` store handling — `reloadStore`,
//! the `checkUntilHealth` liveness loop, and the resolve-state transitions that
//! decide whether a store is still a routing candidate.

use super::super::{
    CacheReloadState, RegionStoreTopology, StoreLiveness, StoreRefreshOutcome, StoreResolveState,
    StoreState,
};
use super::{
    RegionCache, RegionQueryLoader, StoreLivenessApplication, StoreLivenessPlan,
    StoreLivenessResult, StoreRefreshApplication, StoreRefreshPlan, StoreRefreshResult,
};

impl<L> RegionCache<L>
where
    L: RegionQueryLoader,
{
    /// Selects immutable store refresh plans under the canonical cache lock.
    pub(in crate::region) fn plan_store_refreshes(
        &self,
        need_check_only: bool,
    ) -> Vec<StoreRefreshPlan> {
        self.stores
            .values()
            .filter(|store| {
                if need_check_only {
                    store.resolve_state == StoreResolveState::NeedCheck
                } else {
                    store.resolve_state != StoreResolveState::Removed
                }
            })
            .map(|store| StoreRefreshPlan {
                store_id: store.id,
                observed_epoch: store.epoch,
                observed_resolve_state: store.resolve_state,
                observed_address: store.address.clone(),
                observed_labels: store.labels().to_vec(),
            })
            .collect()
    }

    /// Selects immutable health-check inputs without lending canonical stores
    /// across transport I/O.
    pub(in crate::region) fn plan_store_liveness_checks(&self) -> Vec<StoreLivenessPlan> {
        self.stores
            .values()
            .filter(|store| {
                store.resolve_state == StoreResolveState::Resolved
                    && store.liveness != StoreLiveness::Reachable
            })
            .map(|store| StoreLivenessPlan {
                store_id: store.id,
                observed_epoch: store.epoch,
                observed_resolve_state: store.resolve_state,
                observed_liveness: store.liveness,
                address: store.address.clone(),
            })
            .collect()
    }

    /// Publishes one health result only onto the exact store generation that
    /// was probed. Delayed success can never revive a replaced address or a
    /// newer failure generation.
    pub(in crate::region) fn publish_store_liveness(
        &mut self,
        result: StoreLivenessResult,
    ) -> StoreLivenessApplication {
        let StoreLivenessResult { plan, liveness } = result;
        let Some(store) = self.stores.get_mut(&plan.store_id) else {
            return StoreLivenessApplication::StaleDiscarded;
        };
        if store.epoch != plan.observed_epoch
            || store.resolve_state != plan.observed_resolve_state
            || store.liveness != plan.observed_liveness
            || store.address != plan.address
        {
            return StoreLivenessApplication::StaleDiscarded;
        }
        // Foreground failure handling owns degradation. A periodic probe may
        // only restore a store after an explicit serving response; timeout,
        // transport failure, and health `Unknown` must not turn a known-dead
        // store into a selector candidate.
        if liveness != StoreLiveness::Reachable || store.liveness == liveness {
            return StoreLivenessApplication::Unchanged;
        }
        store.liveness = liveness;
        self.advance_store_revision();
        StoreLivenessApplication::Updated
    }

    /// Publishes one PD observation only if its complete selection snapshot is current.
    pub(in crate::region) fn publish_store_refresh(
        &mut self,
        result: StoreRefreshResult,
    ) -> StoreRefreshApplication {
        let StoreRefreshResult { plan, metadata } = result;
        let Some(current) = self.stores.get(&plan.store_id) else {
            return StoreRefreshApplication::StaleDiscarded;
        };
        if current.epoch != plan.observed_epoch
            || current.resolve_state != plan.observed_resolve_state
            || current.address != plan.observed_address
            || current.labels() != plan.observed_labels.as_slice()
        {
            return StoreRefreshApplication::StaleDiscarded;
        }
        let metadata = match metadata {
            Ok(metadata) => metadata,
            Err(_) => return StoreRefreshApplication::Failed,
        };
        if let Some(metadata) = &metadata {
            if metadata.id != plan.store_id {
                return StoreRefreshApplication::Failed;
            }
            if metadata.address.is_empty() {
                return StoreRefreshApplication::Failed;
            }
        }
        let store = self
            .stores
            .get_mut(&plan.store_id)
            .expect("refresh plan was revalidated against the canonical store");
        let previous_epoch = store.epoch;
        let outcome = match metadata {
            None => {
                if store.resolve_state == StoreResolveState::Removed {
                    StoreRefreshOutcome::Unchanged
                } else {
                    store.epoch = store.epoch.saturating_add(1);
                    store.resolve_state = StoreResolveState::Removed;
                    store.liveness = StoreLiveness::Unreachable;
                    store.replace_labels(Vec::new());
                    StoreRefreshOutcome::Removed
                }
            }
            Some(metadata) => {
                let changed = store.address != metadata.address
                    || store.labels() != metadata.labels
                    || store.resolve_state != StoreResolveState::Resolved;
                if store.address != metadata.address {
                    store.epoch = store.epoch.saturating_add(1);
                }
                store.address = metadata.address;
                store.replace_labels(metadata.labels);
                store.resolve_state = StoreResolveState::Resolved;
                if changed {
                    StoreRefreshOutcome::Refreshed
                } else {
                    StoreRefreshOutcome::Unchanged
                }
            }
        };
        let removed = outcome == StoreRefreshOutcome::Removed;
        if store.epoch != previous_epoch {
            self.preferred_proxies.retain(|_, proxy| {
                proxy.store_id != plan.store_id || proxy.store_epoch != previous_epoch
            });
        }
        if removed {
            for location in &self.regions {
                if location
                    .peers
                    .iter()
                    .any(|peer| peer.store_id == plan.store_id)
                {
                    if let Some(state) = self.entry_states.get_mut(&location.region) {
                        state.mark(CacheReloadState::ExpireAfterTtl);
                    }
                }
            }
        }
        if outcome != StoreRefreshOutcome::Unchanged {
            self.advance_store_revision();
        }
        match outcome {
            StoreRefreshOutcome::Unchanged => StoreRefreshApplication::Unchanged,
            StoreRefreshOutcome::Refreshed => StoreRefreshApplication::Refreshed,
            StoreRefreshOutcome::Removed => StoreRefreshApplication::Removed,
        }
    }
}

impl<L> RegionCache<L> {
    /// Returns one immutable view of the canonical store authority.
    #[must_use]
    pub fn store_state(&self, store_id: u64) -> Option<&StoreState> {
        self.stores.get(&store_id).map(RegionStoreTopology::state)
    }

    /// Returns one exact PD label from the canonical store authority.
    #[must_use]
    pub fn store_label(&self, store_id: u64, key: &str) -> Option<&str> {
        self.stores.get(&store_id).and_then(|store| {
            store
                .labels()
                .iter()
                .find_map(|(label_key, value)| (label_key == key).then_some(value.as_str()))
        })
    }
}
