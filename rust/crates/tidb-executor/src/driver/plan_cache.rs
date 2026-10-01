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

//! The session's physical-entry owner, shared by prepared/non-prepared SELECT
//! and DML. Prepared definitions retain syntax and key metadata; executions may keep an
//! entry alive after eviction, but cannot make it a cache hit again.

use super::access::{
    prepared_parameter_types_compatible, PreparedParameterType, PreparedPlanCacheEnvironment,
};
use super::{dml::CachedDmlPlan, planner_bridge::CachedSelectPlan};
use std::sync::{Arc, Mutex};
use tidb_planner::plan_cache_lru::{LruPlanCache, PlanCacheKey, PlanCacheValue};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct PhysicalPlanCacheKey {
    pub statement: String,
    pub schema_version: u64,
    pub stats_version_hash: u64,
    pub environment: PreparedPlanCacheEnvironment,
    pub limit_values: Vec<u64>,
}

impl PlanCacheKey for PhysicalPlanCacheKey {
    fn memory_usage(&self) -> i64 {
        (std::mem::size_of::<Self>() + self.statement.capacity() + self.environment.memory_usage()
            - std::mem::size_of::<PreparedPlanCacheEnvironment>()
            + self.limit_values.capacity() * std::mem::size_of::<u64>()) as i64
    }
}

#[derive(Clone)]
pub(super) enum CachedPhysicalPlan {
    Select(Arc<Mutex<CachedSelectPlan>>),
    Dml(Arc<Mutex<CachedDmlPlan>>),
}

#[derive(Clone)]
struct CachedPlanValue {
    parameter_types: Arc<[PreparedParameterType]>,
    plan: CachedPhysicalPlan,
    memory: i64,
}

impl PlanCacheValue for CachedPlanValue {
    type ParamTypes = Arc<[PreparedParameterType]>;
    fn param_types(&self) -> &Self::ParamTypes {
        &self.parameter_types
    }
    fn parameter_types_compatible(cached: &Self::ParamTypes, requested: &Self::ParamTypes) -> bool {
        prepared_parameter_types_compatible(cached, requested)
    }
    fn memory_usage(&self) -> i64 {
        self.memory
    }
}

struct CacheState {
    plans: LruPlanCache<CachedPlanValue, PhysicalPlanCacheKey>,
    monitor_memory: bool,
    epoch: u64,
}

impl CacheState {
    fn account(&self, previous_size: usize, previous_memory: i64) {
        if self.plans.size() != previous_size {
            tidb_planner::metrics::plan_cache_instance_num_counter(false)
                .add(self.plans.size() as f64 - previous_size as f64);
        }
        if self.monitor_memory && self.plans.memory_usage() != previous_memory {
            tidb_planner::metrics::plan_cache_instance_memory_usage(false)
                .add((self.plans.memory_usage() - previous_memory) as f64);
        }
    }
}

impl Drop for CacheState {
    fn drop(&mut self) {
        let size = self.plans.size();
        let memory = self.plans.memory_usage();
        self.plans.close();
        self.account(size, memory);
    }
}

/// Domain-owned invalidation state for ADMIN FLUSH INSTANCE PLAN_CACHE.
/// An epoch represents Go's expiry timestamp without depending on clock precision.
#[derive(Default, Debug)]
pub struct PlanCacheInvalidation(std::sync::atomic::AtomicU64);

impl PlanCacheInvalidation {
    /// The expiry generation sessions last observed before using their cache.
    pub fn epoch(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Expire existing physical entries in every session sharing this owner.
    pub fn expire(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// One session-scoped LRU, corresponding to Go GetSessionPlanCache.
/// The mutex supports immutable session planning APIs; a plan's own mutex
/// separately guards the execute-time in-place range rebuild.
#[derive(Default)]
pub struct SessionPlanCache {
    state: Mutex<Option<CacheState>>,
}

impl std::fmt::Debug for SessionPlanCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionPlanCache")
            .field("size", &self.size())
            .finish()
    }
}

impl SessionPlanCache {
    /// Apply session capacity and memory monitoring and the domain's flush
    /// generation. Physical entries expire; prepared statement metadata stays.
    pub fn configure(&self, capacity: usize, monitor_memory: bool, epoch: u64, guard: f64) {
        let mut state = self.state.lock().expect("session plan cache poisoned");
        let state = state.get_or_insert_with(|| {
            let configured = tidb_config::config_tree::config::get_global_config()
                .performance
                .server_memory_quota;
            let total = tidb_util::memory::mem_total().unwrap_or(u64::MAX);
            let quota = if configured == 0 {
                total
            } else {
                configured.min(total)
            };
            let mut plans = LruPlanCache::new(capacity, quota, guard);
            plans.set_memory_used(Box::new(|| {
                tidb_util::memory::read_mem_stats().heap_alloc.max(0) as u64
            }));
            CacheState {
                plans,
                monitor_memory,
                epoch,
            }
        });
        let size = state.plans.size();
        let memory = state.plans.memory_usage();
        if state.epoch != epoch {
            state.plans.delete_all();
            state.epoch = epoch;
        }
        // Go captures capacity when GetSessionPlanCache first constructs the LRU.
        state.account(size, memory);
        if state.monitor_memory != monitor_memory {
            tidb_planner::metrics::plan_cache_instance_memory_usage(false)
                .add(state.plans.memory_usage() as f64 * if monitor_memory { 1.0 } else { -1.0 });
            state.monitor_memory = monitor_memory;
        }
    }

    /// Drop every cached physical entry, preserving prepared definitions.
    pub fn clear(&self) {
        let mut state = self.state.lock().expect("session plan cache poisoned");
        if let Some(state) = state.as_mut() {
            let size = state.plans.size();
            let memory = state.plans.memory_usage();
            state.plans.delete_all();
            state.account(size, memory);
        }
    }

    /// Number of retained physical entries (parameter variants count separately).
    pub fn size(&self) -> usize {
        self.state
            .lock()
            .expect("session plan cache poisoned")
            .as_ref()
            .map_or(0, |state| state.plans.size())
    }

    /// Accounted key/plan storage owned by this cache.
    pub fn memory_usage(&self) -> i64 {
        self.state
            .lock()
            .expect("session plan cache poisoned")
            .as_ref()
            .map_or(0, |state| state.plans.memory_usage())
    }

    fn mutate<R>(
        &self,
        action: impl FnOnce(&mut LruPlanCache<CachedPlanValue, PhysicalPlanCacheKey>) -> R,
    ) -> R {
        let mut state = self.state.lock().expect("session plan cache poisoned");
        // Direct executor callers get the same default session cache. SQL
        // sessions configure the owner before their first lookup.
        let state = state.get_or_insert_with(|| CacheState {
            plans: LruPlanCache::new(100, 0, 0.0),
            monitor_memory: false,
            epoch: 0,
        });
        let size = state.plans.size();
        let memory = state.plans.memory_usage();
        let result = action(&mut state.plans);
        state.account(size, memory);
        result
    }

    pub(super) fn get(
        &self,
        key: &PhysicalPlanCacheKey,
        parameter_types: &Arc<[PreparedParameterType]>,
    ) -> Option<CachedPhysicalPlan> {
        self.mutate(|plans| plans.get(key, parameter_types).map(|entry| entry.plan))
    }

    pub(super) fn delete(&self, key: &PhysicalPlanCacheKey) {
        self.mutate(|plans| plans.delete(key));
    }

    pub(super) fn put(
        &self,
        key: PhysicalPlanCacheKey,
        parameter_types: Arc<[PreparedParameterType]>,
        plan: CachedPhysicalPlan,
    ) {
        let memory = match &plan {
            CachedPhysicalPlan::Select(plan) => {
                plan.lock().expect("cached SELECT poisoned").memory_usage()
            }
            CachedPhysicalPlan::Dml(plan) => {
                plan.lock().expect("cached DML poisoned").memory_usage()
            }
        } + (std::mem::size_of::<CachedPlanValue>()
            + parameter_types.len() * std::mem::size_of::<PreparedParameterType>())
            as i64;
        self.mutate(|plans| {
            plans.put(
                key,
                Arc::clone(&parameter_types),
                CachedPlanValue {
                    parameter_types,
                    plan,
                    memory,
                },
            )
        });
    }
}

pub(super) fn statement_key(database: &str, sql: &str) -> String {
    // Length-prefixing prevents database/SQL boundary collisions.
    format!("{}:{database}{sql}", database.len())
}
