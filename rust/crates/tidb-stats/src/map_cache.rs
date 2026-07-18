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

//! Map-backed statistics-cache state from
//! `pkg/statistics/handle/cache/internal/mapcache/map_cache.go`.
//!
//! The source stores `*statistics.Table` values and derives each item's cost
//! from `Table.MemoryUsage`. This leaf keeps that integration explicit by
//! accepting caller-owned values and tracking costs supplied at insertion;
//! map replacement, deletion, enumeration, and copy state remain source-shaped.

use std::collections::HashMap;

#[derive(Clone, Debug)]
struct CacheItem<V> {
    key: i64,
    value: V,
    cost: i64,
}

impl<V: Clone> CacheItem<V> {
    fn copy(&self) -> Self {
        Self {
            key: self.key,
            value: self.value.clone(),
            cost: self.cost,
        }
    }
}

/// A map-backed cache with source-compatible memory-cost accounting.
#[derive(Debug)]
pub struct MapCache<V> {
    tables: HashMap<i64, CacheItem<V>>,
    mem_usage: i64,
}

impl<V> Default for MapCache<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> MapCache<V> {
    /// Creates an empty map cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            mem_usage: 0,
        }
    }

    /// Returns a cached value by table ID.
    #[must_use]
    pub fn get(&self, key: i64) -> Option<&V> {
        self.tables.get(&key).map(|item| &item.value)
    }

    /// Inserts or replaces a value and its caller-owned memory cost.
    ///
    /// The source map cache always accepts the insertion, so this method
    /// returns `true` for interface parity.
    pub fn put(&mut self, key: i64, value: V, cost: i64) -> bool {
        if let Some(item) = self.tables.get_mut(&key) {
            let old_cost = item.cost;
            item.value = value;
            item.cost = cost;
            self.mem_usage = self.mem_usage.wrapping_add(cost.wrapping_sub(old_cost));
            return true;
        }

        self.tables.insert(key, CacheItem { key, value, cost });
        self.mem_usage = self.mem_usage.wrapping_add(cost);
        true
    }

    /// Deletes a value if present and subtracts its tracked cost.
    pub fn del(&mut self, key: i64) {
        let Some(item) = self.tables.remove(&key) else {
            return;
        };
        self.mem_usage = self.mem_usage.wrapping_sub(item.cost);
    }

    /// Returns the tracked aggregate memory cost.
    #[must_use]
    pub const fn cost(&self) -> i64 {
        self.mem_usage
    }

    /// Returns all table IDs in unspecified map order.
    #[must_use]
    pub fn keys(&self) -> Vec<i64> {
        self.tables.keys().copied().collect()
    }

    /// Returns all cached values in unspecified map order.
    #[must_use]
    pub fn values(&self) -> Vec<&V> {
        self.tables.values().map(|item| &item.value).collect()
    }

    /// Returns the number of cached values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Returns whether the cache has no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Copies map entries and aggregate cost.
    #[must_use]
    pub fn copy(&self) -> Self
    where
        V: Clone,
    {
        Self {
            tables: self
                .tables
                .iter()
                .map(|(&key, item)| (key, item.copy()))
                .collect(),
            mem_usage: self.mem_usage,
        }
    }

    /// Preserves the source no-op capacity hook.
    pub const fn set_capacity(&self, _capacity: i64) {}

    /// Preserves the source no-op close hook.
    pub const fn close(&self) {}

    /// Preserves the source no-op eviction hook.
    pub const fn trigger_evict(&self) {}

    /// Preserves the source synchronous map-cache hook.
    pub const fn wait_for_async_updates(&self) {}
}
