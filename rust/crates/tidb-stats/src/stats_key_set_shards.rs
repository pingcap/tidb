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

//! Sharded statistics-cache key metadata from
//! `pkg/statistics/handle/cache/internal/lfu/key_set_shard.go`.
//!
//! The source routes each table ID to one of 256 key sets. Values are already
//! reduced to caller-owned tracking costs; LFU admission, Ristretto state, and
//! statistics-table memory accounting remain outside this leaf.

use crate::StatsKeySet;

/// Number of source key-set shards.
pub const KEY_SET_SHARD_COUNT: usize = 256;

/// Fixed-shard key metadata for statistics-cache fallback state.
#[derive(Debug)]
pub struct StatsKeySetShards {
    shards: Vec<StatsKeySet>,
}

impl Default for StatsKeySetShards {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsKeySetShards {
    /// Creates all source shards.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: (0..KEY_SET_SHARD_COUNT)
                .map(|_| StatsKeySet::new())
                .collect(),
        }
    }

    /// Adds or replaces one key/value cost in its shard.
    pub fn add_key_value(&self, key: i64, tracking_cost: i64) {
        self.shards[Self::shard_index(key)].add_key_value(key, tracking_cost);
    }

    /// Returns one key/value cost from its shard.
    #[must_use]
    pub fn get(&self, key: i64) -> Option<i64> {
        self.shards[Self::shard_index(key)].get(key)
    }

    /// Removes one key from its shard.
    pub fn remove(&self, key: i64) {
        self.shards[Self::shard_index(key)].remove(key);
    }

    /// Returns all keys in shard/map order.
    #[must_use]
    pub fn keys(&self) -> Vec<i64> {
        self.shards.iter().flat_map(StatsKeySet::keys).collect()
    }

    /// Returns the aggregate number of keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards.iter().map(StatsKeySet::len).sum()
    }

    /// Returns whether every shard contains no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(StatsKeySet::is_empty)
    }

    /// Clears every shard.
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }

    fn shard_index(key: i64) -> usize {
        key.rem_euclid(KEY_SET_SHARD_COUNT as i64) as usize
    }
}
