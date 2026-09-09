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

//! Go `pkg/statistics/asyncload`: the process-wide set of column and index
//! statistics whose histogram payload is still needed.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

use tidb_model::{StatsLoadItem, TableItemID};

const SHARD_COUNT: usize = 128;

/// The process-wide needed-item set used by asynchronous statistics loading.
pub static ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS: LazyLock<NeededStatsMap> =
    LazyLock::new(NeededStatsMap::new);

/// Go's `neededStatsMap`, sharded by the absolute column-or-index ID.
pub struct NeededStatsMap {
    shards: [RwLock<HashMap<TableItemID, bool>>; SHARD_COUNT],
}

impl NeededStatsMap {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(HashMap::new())),
        }
    }

    /// Go `AllItems`: returns one snapshot across all 128 shards.
    #[must_use]
    pub fn all_items(&self) -> Vec<StatsLoadItem> {
        let mut result = Vec::with_capacity(SHARD_COUNT);
        for shard in &self.shards {
            result.extend(
                shard
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .map(|(&table_item_id, &full_load)| StatsLoadItem {
                        table_item_id,
                        full_load,
                    }),
            );
        }
        result
    }

    /// Go `Insert`: inserts a request and only permits an upgrade to full load.
    pub fn insert(&self, item: TableItemID, full_load: bool) {
        let mut shard = self.shards[shard_index(item)]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if shard.get(&item).copied().unwrap_or(false) {
            return;
        }
        shard.insert(item, full_load);
    }

    /// Go `Delete`.
    pub fn delete(&self, item: TableItemID) {
        self.shards[shard_index(item)]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&item);
    }

    /// Go `Length`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len()
            })
            .sum()
    }
}

fn shard_index(item: TableItemID) -> usize {
    (item.id.unsigned_abs() % SHARD_COUNT as u64) as usize
}
