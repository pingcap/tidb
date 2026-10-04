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

//! Shared instance cache policy from Go core/plan_cache_instance.go.
//!
//! Values are immutable snapshots; execution cloning belongs to the caller.
//! A Rust mutex serializes admission and eviction rather than reproducing
//! Go's unsafe linked-node CAS. Compatible duplicate admission still fails,
//! hard limits reject rather than evict, and periodic eviction uses Go's
//! average-cost last-use threshold (not the session LRU's count policy).

use crate::plan_cache_lru::{PlanCacheKey, PlanCacheValue};
use std::{collections::HashMap, sync::Mutex, time::Instant};

struct Entry<V> {
    value: V,
    last_used: Instant,
}
struct State<K, V> {
    heads: HashMap<K, Vec<Entry<V>>>,
    cost: i64,
    count: usize,
    soft: i64,
    hard: i64,
}

/// Domain-owned, parameter-compatible immutable physical plan snapshots.
pub struct InstancePlanCache<K: PlanCacheKey, V: PlanCacheValue> {
    state: Mutex<State<K, V>>,
}
impl<K: PlanCacheKey, V: PlanCacheValue> InstancePlanCache<K, V> {
    /// Go NewInstancePlanCache.
    pub fn new(soft: i64, hard: i64) -> Self {
        Self {
            state: Mutex::new(State {
                heads: HashMap::new(),
                cost: 0,
                count: 0,
                soft,
                hard,
            }),
        }
    }
    /// Go Get: update last use and return an immutable snapshot handle.
    pub fn get(&self, key: &K, types: &V::ParamTypes) -> Option<V> {
        let mut state = self.state.lock().expect("instance plan cache poisoned");
        let entry = state
            .heads
            .get_mut(key)?
            .iter_mut()
            .find(|entry| V::parameter_types_compatible(entry.value.param_types(), types))?;
        entry.last_used = Instant::now();
        Some(entry.value.clone())
    }
    /// Go Put: do not replace compatible entries or exceed the hard limit.
    pub fn put(&self, key: K, value: V) -> bool {
        let mut state = self.state.lock().expect("instance plan cache poisoned");
        let memory = value.memory_usage();
        if state.cost.saturating_add(memory) > state.hard {
            return false;
        }
        if let Some(entries) = state.heads.get_mut(&key) {
            if let Some(entry) = entries.iter_mut().find(|entry| {
                V::parameter_types_compatible(entry.value.param_types(), value.param_types())
            }) {
                entry.last_used = Instant::now();
                return false;
            }
        }
        state.cost += memory;
        state.count += 1;
        state.heads.entry(key).or_default().insert(
            0,
            Entry {
                value,
                last_used: Instant::now(),
            },
        );
        true
    }
    /// Go All: values remain read-only.
    pub fn all(&self) -> Vec<V> {
        self.state
            .lock()
            .expect("instance plan cache poisoned")
            .heads
            .values()
            .flatten()
            .map(|entry| entry.value.clone())
            .collect()
    }
    /// Go Evict: whole-cache retirement or estimated least-recent-use threshold.
    pub fn evict(&self, all: bool) -> usize {
        let mut state = self.state.lock().expect("instance plan cache poisoned");
        if !all && state.cost < state.soft {
            return 0;
        }
        let mut uses: Vec<_> = state
            .heads
            .values()
            .flatten()
            .map(|entry| entry.last_used)
            .collect();
        let threshold = if all {
            Some(Instant::now())
        } else if uses.is_empty() {
            None
        } else {
            let average = state.cost / uses.len() as i64;
            if average <= 0 {
                None
            } else {
                let count = (state.cost - state.soft + average - 1) / average;
                if count <= 0 || count as usize > uses.len() {
                    None
                } else {
                    uses.sort_unstable();
                    Some(uses[count as usize - 1])
                }
            }
        };
        let Some(threshold) = threshold else {
            return 0;
        };
        let mut removed_cost = 0;
        let mut removed = 0;
        state.heads.retain(|_, entries| {
            entries.retain(|entry| {
                if entry.last_used <= threshold {
                    removed += 1;
                    removed_cost += entry.value.memory_usage();
                    false
                } else {
                    true
                }
            });
            !entries.is_empty()
        });
        state.cost -= removed_cost;
        state.count -= removed;
        removed
    }
    /// Go MemUsage.
    pub fn memory_usage(&self) -> i64 {
        self.state
            .lock()
            .expect("instance plan cache poisoned")
            .cost
    }
    /// Go Size.
    pub fn size(&self) -> usize {
        self.state
            .lock()
            .expect("instance plan cache poisoned")
            .count
    }
    /// Go GetLimits.
    pub fn limits(&self) -> (i64, i64) {
        let state = self.state.lock().expect("instance plan cache poisoned");
        (state.soft, state.hard)
    }
    /// Go SetLimits; changing limits does not synchronously evict.
    pub fn set_limits(&self, soft: i64, hard: i64) {
        let mut state = self.state.lock().expect("instance plan cache poisoned");
        state.soft = soft;
        state.hard = hard;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Clone)]
    struct Value {
        types: u8,
        memory: i64,
    }
    impl PlanCacheValue for Value {
        type ParamTypes = u8;
        fn param_types(&self) -> &u8 {
            &self.types
        }
        fn memory_usage(&self) -> i64 {
            self.memory
        }
    }
    #[test]
    fn admission_variants_and_limit_changes_preserve_existing_values() {
        let cache = InstancePlanCache::new(20, 30);
        assert!(cache.put(
            "q".to_owned(),
            Value {
                types: 0,
                memory: 10
            }
        ));
        assert!(!cache.put(
            "q".to_owned(),
            Value {
                types: 0,
                memory: 1
            }
        ));
        assert!(cache.put(
            "q".to_owned(),
            Value {
                types: 1,
                memory: 20
            }
        ));
        assert!(!cache.put(
            "other".to_owned(),
            Value {
                types: 0,
                memory: 1
            }
        ));
        assert_eq!(cache.size(), 2);
        assert_eq!(cache.memory_usage(), 30);
        assert_eq!(cache.get(&"q".to_owned(), &0).unwrap().memory, 10);
        cache.set_limits(5, 10);
        assert_eq!(cache.limits(), (5, 10));
        assert_eq!(cache.all().len(), 2);
        assert_eq!(cache.evict(false), 2);
        assert_eq!(cache.size(), 0);
        assert_eq!(cache.memory_usage(), 0);
    }
    #[test]
    fn eviction_refreshes_hits_and_disable_clears_all_variants() {
        let cache = InstancePlanCache::new(20, 40);
        for key in ["a", "b", "c"] {
            assert!(cache.put(
                key.to_owned(),
                Value {
                    types: 0,
                    memory: 10
                }
            ));
        }
        cache.get(&"a".to_owned(), &0).unwrap();
        assert_eq!(cache.evict(false), 1);
        assert!(cache.get(&"b".to_owned(), &0).is_none());
        assert!(cache.get(&"a".to_owned(), &0).is_some());
        assert_eq!(cache.evict(false), 0); // exactly soft: Go evicts none.
        assert_eq!(cache.evict(true), 2);
    }
    #[test]
    fn concurrent_duplicate_admission_has_one_owner() {
        let cache = std::sync::Arc::new(InstancePlanCache::new(20, 40));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let cache = cache.clone();
                scope.spawn(move || {
                    cache.put(
                        "q".to_owned(),
                        Value {
                            types: 0,
                            memory: 10,
                        },
                    );
                });
            }
        });
        assert_eq!(cache.size(), 1);
        assert_eq!(cache.memory_usage(), 10);
    }
}
