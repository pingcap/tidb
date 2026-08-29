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

use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

use tidb_stats::Table;
use tidb_stats_handle_cache_internal::StatsCacheInner;

#[derive(Clone)]
struct CacheItem {
    value: Arc<Table>,
    key: i64,
    cost: i64,
}

impl CacheItem {
    fn copy(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            key: self.key,
            cost: self.cost,
        }
    }
}

struct State {
    tables: HashMap<i64, CacheItem>,
    mem_usage: i64,
}

/// Go `MapCache`.
pub struct MapCache {
    state: RefCell<State>,
}

impl MapCache {
    /// Go `NewMapCache`.
    #[must_use]
    #[allow(clippy::new_without_default)] // Pinned Go has NewMapCache, not a zero-value constructor.
    pub fn new() -> Self {
        Self {
            state: RefCell::new(State {
                tables: HashMap::new(),
                mem_usage: 0,
            }),
        }
    }

    fn state(&self) -> Ref<'_, State> {
        self.state.borrow()
    }

    /// Go `Keys`.
    #[must_use]
    pub fn keys(&self) -> Vec<i64> {
        self.state().tables.keys().copied().collect()
    }
}

impl StatsCacheInner for MapCache {
    fn get(&self, table_id: i64) -> Option<Arc<Table>> {
        self.state()
            .tables
            .get(&table_id)
            .map(|item| Arc::clone(&item.value))
    }

    fn put(&self, table_id: i64, table: Arc<Table>) -> bool {
        let cost = table.memory_usage().total_mem_usage;
        let mut state = self.state.borrow_mut();
        if let Some(item) = state.tables.get_mut(&table_id) {
            let old_cost = item.cost;
            item.value = table;
            item.cost = cost;
            state.mem_usage = state.mem_usage.wrapping_add(cost.wrapping_sub(old_cost));
            return true;
        }
        state.tables.insert(
            table_id,
            CacheItem {
                value: table,
                key: table_id,
                cost,
            },
        );
        state.mem_usage = state.mem_usage.wrapping_add(cost);
        true
    }

    fn del(&self, table_id: i64) {
        let mut state = self.state.borrow_mut();
        if let Some(item) = state.tables.remove(&table_id) {
            state.mem_usage = state.mem_usage.wrapping_sub(item.cost);
        }
    }

    fn cost(&self) -> i64 {
        self.state().mem_usage
    }

    fn values(&self) -> Vec<Arc<Table>> {
        self.state()
            .tables
            .values()
            .map(|item| Arc::clone(&item.value))
            .collect()
    }

    fn len(&self) -> usize {
        self.state().tables.len()
    }

    fn copy(&self) -> Box<dyn StatsCacheInner> {
        let state = self.state();
        Box::new(Self {
            state: RefCell::new(State {
                tables: state
                    .tables
                    .iter()
                    .map(|(&key, item)| (key, item.copy()))
                    .collect(),
                mem_usage: state.mem_usage,
            }),
        })
    }

    fn set_capacity(&self, _capacity: i64) {}

    fn close(&self) {}

    fn trigger_evict(&self) {}

    fn wait_for_async_updates(&self) {}
}
