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

//! Thread-safe statistics-cache key metadata from
//! `pkg/statistics/handle/cache/internal/lfu/key_set.go`.
//!
//! The source stores `*statistics.Table` values and derives removal cost from
//! their memory usage. This leaf accepts that already-derived cost so it can
//! preserve replacement/removal semantics without importing table, CMSketch,
//! or LFU admission machinery.

use std::collections::HashMap;
use std::sync::RwLock;

/// Thread-safe key→tracking-cost metadata for a statistics cache.
#[derive(Debug, Default)]
pub struct StatsKeySet {
    values: RwLock<HashMap<i64, i64>>,
}

impl StatsKeySet {
    /// Creates an empty key set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a key with its caller-derived tracking cost.
    pub fn add_key_value(&self, key: i64, tracking_cost: i64) {
        self.values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key, tracking_cost);
    }

    /// Removes a key and returns its previous cost, or zero when absent.
    pub fn remove(&self, key: i64) -> i64 {
        self.values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&key)
            .unwrap_or(0)
    }

    /// Returns a key's cost and presence.
    #[must_use]
    pub fn get(&self, key: i64) -> Option<i64> {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .copied()
    }

    /// Returns all keys in unspecified map order.
    #[must_use]
    pub fn keys(&self) -> Vec<i64> {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .copied()
            .collect()
    }

    /// Returns the number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Returns whether the set contains no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
    }

    /// Removes all entries.
    pub fn clear(&self) {
        self.values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}
