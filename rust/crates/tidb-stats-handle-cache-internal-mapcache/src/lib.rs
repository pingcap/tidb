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

//! Go `pkg/statistics/handle/cache/internal/mapcache`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use arc_swap::ArcSwapOption;
use foldhash::fast::RandomState;
use tidb_stats::Table;
use tidb_stats_handle_cache_internal::StatsCacheInner;

#[derive(Clone)]
struct CacheItem {
    value: Arc<Table>,
    cost: i64,
}

#[derive(Clone, Default)]
struct State {
    tables: HashMap<i64, CacheItem, RandomState>,
    mem_usage: i64,
}

impl State {
    fn insert(&mut self, table_id: i64, table: Arc<Table>, cost: i64) {
        let old_cost = self
            .tables
            .insert(table_id, CacheItem { value: table, cost })
            .map_or(0, |item| item.cost);
        self.mem_usage = self.mem_usage.wrapping_add(cost.wrapping_sub(old_cost));
    }

    fn remove(&mut self, table_id: i64) {
        if let Some(item) = self.tables.remove(&table_id) {
            self.mem_usage = self.mem_usage.wrapping_sub(item.cost);
        }
    }
}

/// Go `MapCache`.
pub struct MapCache {
    state: RwLock<Arc<State>>,
    // A copy used only for writes never registers an ArcSwap snapshot.
    published: OnceLock<ArcSwapOption<State>>,
}

impl MapCache {
    /// Go `NewMapCache`.
    #[allow(clippy::new_without_default)] // Pinned Go has NewMapCache, not a zero-value constructor.
    pub fn new() -> Self {
        Self::from_state(Arc::new(State::default()))
    }

    fn from_state(state: Arc<State>) -> Self {
        Self {
            published: OnceLock::new(),
            state: RwLock::new(state),
        }
    }

    #[inline]
    fn read<R>(&self, publish: bool, read: impl FnOnce(&Arc<State>) -> R) -> R {
        if let Some(published) = self.published.get() {
            let snapshot = published.load();
            if let Some(state) = snapshot.as_ref() {
                return read(state);
            }
        }
        self.read_unpublished(publish, read)
    }

    #[inline(never)]
    fn read_unpublished<R>(&self, publish: bool, read: impl FnOnce(&Arc<State>) -> R) -> R {
        // Build a snapshot only when a lookup needs one. Bulk cache updates
        // can keep editing the writable state until the first lookup.
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if publish {
            self.published
                .get_or_init(ArcSwapOption::empty)
                .compare_and_swap(&None::<Arc<State>>, Some(Arc::clone(&state)));
        }
        read(&state)
    }

    fn write(&self, update: impl FnOnce(&mut State)) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // With no old readers or copies, make_mut can edit the map in place.
        // Readers already holding the snapshot keep seeing its stable state.
        if let Some(published) = self.published.get() {
            if published.load().is_some() {
                published.store(None);
            }
        }
        update(Arc::make_mut(&mut state));
    }

    /// Go `Keys`.
    pub fn keys(&self) -> Vec<i64> {
        self.read(false, |state| state.tables.keys().copied().collect())
    }
}

impl StatsCacheInner for MapCache {
    fn get(&self, table_id: i64) -> Option<Arc<Table>> {
        self.read(true, |state| {
            state.tables.get(&table_id).map(|item| Arc::clone(&item.value))
        })
    }

    fn contains_table(&self, table_id: i64) -> bool {
        self.read(true, |state| state.tables.contains_key(&table_id))
    }

    fn put(&self, table_id: i64, table: Arc<Table>) -> bool {
        let cost = table.memory_usage().total_mem_usage;
        self.write(|state| state.insert(table_id, table, cost));
        true
    }

    fn del(&self, table_id: i64) {
        self.write(|state| state.remove(table_id));
    }

    fn cost(&self) -> i64 {
        self.read(false, |state| state.mem_usage)
    }

    fn values(&self) -> Vec<Arc<Table>> {
        self.read(false, |state| {
            state
                .tables
                .values()
                .map(|item| Arc::clone(&item.value))
                .collect()
        })
    }

    fn len(&self) -> usize {
        self.read(false, |state| state.tables.len())
    }

    fn copy(&self) -> Box<dyn StatsCacheInner> {
        Box::new(Self::from_state(self.read(false, Arc::clone)))
    }

    fn copy_and_update(&self, tables: &[Arc<Table>], deleted_ids: &[i64]) -> Box<dyn StatsCacheInner> {
        if tables.is_empty() && deleted_ids.is_empty() {
            return self.copy();
        }

        // The copy is private until the caller publishes it, so the batch
        // does not need to lock the copy or retire snapshots between edits.
        let snapshot = self.read(false, Arc::clone);
        let mut state = (*snapshot).clone();
        for table in tables {
            state.insert(
                table.hist_coll.physical_id,
                Arc::clone(table),
                table.memory_usage().total_mem_usage,
            );
        }
        for &id in deleted_ids {
            state.remove(id);
        }
        Box::new(Self::from_state(Arc::new(state)))
    }

    fn set_capacity(&self, _capacity: i64) {}

    fn close(&self) {}

    fn trigger_evict(&self) {}

    fn wait_for_async_updates(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_stats_handle_cache_internal_testutil::new_mock_statistics_table;

    #[test]
    fn source_put_replace_delete_and_copy() {
        let cache = MapCache::new();
        let first = new_mock_statistics_table(1, 1, true, false, false);
        let first_cost = first.memory_usage().total_mem_usage;
        assert!(cache.put(1, first));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.cost(), first_cost);

        let replacement = new_mock_statistics_table(2, 1, true, false, false);
        let replacement_cost = replacement.memory_usage().total_mem_usage;
        assert!(cache.put(1, replacement));
        assert_eq!(cache.cost(), replacement_cost);

        let copy = cache.copy();
        cache.del(1);
        assert_eq!(cache.len(), 0);
        assert_eq!(copy.len(), 1);
        assert_eq!(copy.cost(), replacement_cost);
    }

    #[test]
    fn published_state_survives_a_concurrent_write_and_copy() {
        let cache = MapCache::new();
        let old = new_mock_statistics_table(1, 1, true, false, false);
        let new = new_mock_statistics_table(2, 1, true, false, false);
        cache.put(1, Arc::clone(&old));
        assert_eq!(cache.cost(), old.memory_usage().total_mem_usage);
        assert!(cache.published.get().is_none());

        assert!(Arc::ptr_eq(&cache.get(1).unwrap(), &old));
        let snapshot = cache.published.get().unwrap().load();
        let copy = cache.copy();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                cache.put(1, Arc::clone(&new));
                cache.put(2, Arc::clone(&new));
            });
        });

        let snapshot = snapshot.as_ref().unwrap();
        assert!(Arc::ptr_eq(&snapshot.tables[&1].value, &old));
        assert_eq!(snapshot.tables.len(), 1);
        assert_eq!(copy.len(), 1);
        assert!(Arc::ptr_eq(&copy.get(1).unwrap(), &old));
        assert!(Arc::ptr_eq(&cache.get(1).unwrap(), &new));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.cost(), 2 * new.memory_usage().total_mem_usage);
    }

    #[test]
    fn shared_reads_are_safe() {
        let cache = Arc::new(MapCache::new());
        cache.put(1, new_mock_statistics_table(1, 1, true, false, false));
        std::thread::scope(|scope| {
            for _ in 0..32 {
                let cache = Arc::clone(&cache);
                scope.spawn(move || {
                    assert!(cache.get(1).is_some());
                    assert_eq!(cache.values().len(), 1);
                });
            }
        });
    }
}
