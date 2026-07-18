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

//! Sharded pending-statistics metadata from `pkg/statistics/asyncload/async_load.go`.
//!
//! The leaf owns only the source's thread-safe set of table columns/indexes
//! waiting for histogram loads.  Statistics-handle scheduling, storage reads,
//! schema-drop cleanup, and the process-global queue remain future owners.

use std::collections::HashMap;
use std::sync::RwLock;

/// Number of shards in the source pending-statistics map.
pub const SHARD_COUNT: usize = 128;

/// Identifies one table column or index whose statistics may be loaded.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TableItemId {
    /// Physical table identifier.
    pub table_id: i64,
    /// Column or index identifier within the table.
    pub id: i64,
    /// Whether `id` identifies an index rather than a column.
    pub is_index: bool,
    /// Whether the synchronous loader previously failed for this item.
    pub is_sync_load_failed: bool,
}

/// One pending statistics-load request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StatsLoadItem {
    /// Table/column/index identity.
    pub table_item_id: TableItemId,
    /// Whether the request needs a full histogram load.
    pub full_load: bool,
}

#[derive(Debug, Default)]
struct NeededStatsInternalMap {
    items: RwLock<HashMap<TableItemId, bool>>,
}

impl NeededStatsInternalMap {
    fn all_items(&self) -> Vec<StatsLoadItem> {
        let items = self
            .items
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        items
            .iter()
            .map(|(&table_item_id, &full_load)| StatsLoadItem {
                table_item_id,
                full_load,
            })
            .collect()
    }

    fn insert(&self, item: TableItemId, full_load: bool) {
        let mut items = self
            .items
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if items.get(&item).copied().unwrap_or(false) {
            // Once a full load is requested, a later partial request cannot
            // weaken it.
            return;
        }
        items.insert(item, full_load);
    }

    fn delete(&self, item: TableItemId) {
        self.items
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&item);
    }

    fn len(&self) -> usize {
        self.items
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

/// Sharded, thread-safe pending histogram-load requests.
#[derive(Debug)]
pub struct NeededStatsMap {
    items: [NeededStatsInternalMap; SHARD_COUNT],
}

impl Default for NeededStatsMap {
    fn default() -> Self {
        Self::new()
    }
}

impl NeededStatsMap {
    /// Creates an empty map with the source's 128 shards.
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: std::array::from_fn(|_| NeededStatsInternalMap::default()),
        }
    }

    fn shard(item: TableItemId) -> usize {
        item.id.wrapping_abs() as usize % SHARD_COUNT
    }

    /// Returns all pending requests, grouped by shard as in the source.
    #[must_use]
    pub fn all_items(&self) -> Vec<StatsLoadItem> {
        let mut result = Vec::new();
        for item in &self.items {
            result.extend(item.all_items());
        }
        result
    }

    /// Inserts or upgrades one request.
    ///
    /// A previous full-load request wins over a later partial request; a
    /// previous partial request can be upgraded to full load.
    pub fn insert(&self, item: TableItemId, full_load: bool) {
        self.items[Self::shard(item)].insert(item, full_load);
    }

    /// Removes one request if present.
    pub fn delete(&self, item: TableItemId) {
        self.items[Self::shard(item)].delete(item);
    }

    /// Returns the number of pending requests.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.iter().map(NeededStatsInternalMap::len).sum()
    }

    /// Returns whether no requests are pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
