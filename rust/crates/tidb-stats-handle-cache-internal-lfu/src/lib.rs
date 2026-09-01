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

//! Go `pkg/statistics/handle/cache/internal/lfu`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use stretto::{
    Cache, CacheBuilder, CacheCallback, DefaultCoster, DefaultUpdateValidator, Item,
    TransparentKeyBuilder,
};
use tidb_stats::{CopyIntent, HistColl, Table};
use tidb_stats_handle_cache_internal::StatsCacheInner;
use tidb_stats_handle_cache_metrics as metrics;

const KEY_SET_COUNT: usize = 256;

struct KeySetShard {
    shards: Vec<RwLock<HashMap<i64, Arc<Table>>>>,
}

impl KeySetShard {
    fn new() -> Self {
        Self {
            shards: (0..KEY_SET_COUNT)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        }
    }

    fn shard(&self, key: i64) -> &RwLock<HashMap<i64, Arc<Table>>> {
        &self.shards[(key as usize) % KEY_SET_COUNT]
    }

    fn get(&self, key: i64) -> Option<Arc<Table>> {
        self.shard(key)
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .cloned()
    }

    fn put(&self, key: i64, table: Arc<Table>) {
        self.shard(key)
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, table);
    }

    fn remove(&self, key: i64) {
        self.shard(key)
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
    }

    fn values(&self) -> Vec<Arc<Table>> {
        self.shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn len(&self) -> usize {
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

    fn clear(&self) {
        for shard in &self.shards {
            shard
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
    }
}

type Primary = Cache<
    i64,
    Arc<Table>,
    TransparentKeyBuilder<i64>,
    DefaultCoster<Arc<Table>>,
    DefaultUpdateValidator<Arc<Table>>,
    Callbacks,
>;

struct State {
    tables: KeySetShard,
    cost: AtomicI64,
    closed: AtomicBool,
    fake_key: AtomicU64,
    primary: OnceLock<Weak<Primary>>,
}

impl State {
    fn add_cost(&self, value: i64) {
        let cost = self
            .cost
            .fetch_add(value, Ordering::AcqRel)
            .wrapping_add(value);
        metrics::cost_gauge().set(cost as f64);
    }

    fn trigger_evict(&self) {
        let Some(cache) = self.primary.get().and_then(Weak::upgrade) else {
            return;
        };
        if self.cost.load(Ordering::Acquire) > cache.max_cost() {
            let key = -(self.fake_key.fetch_add(1, Ordering::Relaxed) as i64).wrapping_sub(1);
            cache.insert(key, Arc::new(empty_table()), 0);
        }
    }

    fn drop_memory(&self, item: &Item<Arc<Table>>) {
        if (item.index as i64) < 0 {
            return;
        }
        let Some(table) = item.val.as_ref() else {
            return;
        };
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let table = Arc::new(table.copy_as(CopyIntent::AllDataWritable));
        table.hist_coll.drop_evicted();
        self.tables.put(item.index as i64, Arc::clone(&table));
        self.add_cost(table.memory_usage().total_tracking_mem_usage());
        self.trigger_evict();
    }
}

fn empty_table() -> Table {
    Table {
        existence_map: None,
        hist_coll: HistColl::new(0, 0, 0, 0, 0),
        version: 0,
        last_analyze_version: 0,
        last_stats_hist_version: 0,
        table_info_update_ts: 0,
        is_pk_handle: false,
    }
}

#[derive(Clone)]
struct Callbacks(Arc<State>);

impl CacheCallback for Callbacks {
    type Value = Arc<Table>;

    fn on_exit(&self, value: Option<Self::Value>) {
        let Some(table) = value else { return };
        if self.0.closed.load(Ordering::Acquire) {
            return;
        }
        self.0.trigger_evict();
        self.0
            .add_cost(-table.memory_usage().total_tracking_mem_usage());
    }

    fn on_evict(&self, item: Item<Self::Value>) {
        self.0.drop_memory(&item);
        self.on_exit(item.val);
        metrics::evict_counter().inc();
    }

    fn on_reject(&self, item: Item<Self::Value>) {
        self.0.drop_memory(&item);
        self.on_exit(item.val);
        metrics::reject_counter().inc();
    }
}

/// Go `LFU`.
pub struct Lfu {
    state: Arc<State>,
    cache: Arc<Mutex<Option<Arc<Primary>>>>,
}

impl Lfu {
    /// Go `NewLFU`.
    pub fn new(total_mem_cost: i64) -> Result<Self, String> {
        Self::with_internal_cost(total_mem_cost, false)
    }

    fn with_internal_cost(total_mem_cost: i64, ignore_internal_cost: bool) -> Result<Self, String> {
        let cost = adjust_mem_cost(total_mem_cost)?;
        metrics::capacity_gauge().set(cost as f64);
        let state = Arc::new(State {
            tables: KeySetShard::new(),
            cost: AtomicI64::new(0),
            closed: AtomicBool::new(false),
            fake_key: AtomicU64::new(0),
            primary: OnceLock::new(),
        });
        let cache = Arc::new(
            CacheBuilder::new_with_key_builder(
                usize::try_from((cost / 128).clamp(10, 1_000_000)).unwrap_or(10),
                cost,
                TransparentKeyBuilder::default(),
            )
            .set_buffer_items(64)
            .set_ignore_internal_cost(ignore_internal_cost)
            .set_callback(Callbacks(Arc::clone(&state)))
            .finalize()
            .map_err(|error| error.to_string())?,
        );
        state
            .primary
            .set(Arc::downgrade(&cache))
            .map_err(|_| "primary cache already initialized".to_owned())?;
        Ok(Self {
            state,
            cache: Arc::new(Mutex::new(Some(cache))),
        })
    }

    #[cfg(test)]
    fn new_for_test(total_mem_cost: i64) -> Result<Self, String> {
        Self::with_internal_cost(total_mem_cost, true)
    }

    fn primary(&self) -> Option<Arc<Primary>> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

fn adjust_mem_cost(total_mem_cost: i64) -> Result<i64, String> {
    if total_mem_cost != 0 {
        return Ok(total_mem_cost);
    }
    tidb_util::memory::mem_total()
        .map(|total| (total / 5).min(i64::MAX as u64) as i64)
        .map_err(|error| error.to_string())
}

impl StatsCacheInner for Lfu {
    fn get(&self, table_id: i64) -> Option<Arc<Table>> {
        self.primary()
            .and_then(|cache| cache.get(&table_id).map(|value| Arc::clone(value.value())))
            .or_else(|| self.state.tables.get(table_id))
    }

    fn put(&self, table_id: i64, table: Arc<Table>) -> bool {
        let cost = table.memory_usage().total_tracking_mem_usage();
        self.state.tables.put(table_id, Arc::clone(&table));
        self.state.add_cost(cost);
        self.primary()
            .is_some_and(|cache| cache.insert(table_id, table, cost))
    }

    fn del(&self, table_id: i64) {
        if let Some(cache) = self.primary() {
            cache.remove(&table_id);
        }
        self.state.tables.remove(table_id);
    }

    fn cost(&self) -> i64 {
        self.state.cost.load(Ordering::Acquire)
    }
    fn values(&self) -> Vec<Arc<Table>> {
        self.state.tables.values()
    }
    fn len(&self) -> usize {
        self.state.tables.len()
    }
    fn copy(&self) -> Box<dyn StatsCacheInner> {
        Box::new(Self {
            state: Arc::clone(&self.state),
            cache: Arc::clone(&self.cache),
        })
    }
    fn set_capacity(&self, capacity: i64) {
        if let Ok(capacity) = adjust_mem_cost(capacity) {
            if let Some(cache) = self.primary() {
                cache.update_max_cost(capacity);
            }
            self.state.trigger_evict();
            metrics::capacity_gauge().set(capacity as f64);
            metrics::cost_gauge().set(self.cost() as f64);
        }
    }
    fn close(&self) {
        if self.state.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.state.tables.clear();
        if let Some(cache) = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = cache.clear();
            let _ = cache.wait();
        }
    }
    fn trigger_evict(&self) {
        self.state.trigger_evict();
    }
    fn wait_for_async_updates(&self) {
        if let Some(cache) = self.primary() {
            let _ = cache.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_stats_handle_cache_internal_testutil::new_mock_statistics_table;

    #[test]
    fn put_get_delete_preserves_go_visibility() {
        let cache = Lfu::new_for_test(100).expect("LFU");
        let table = new_mock_statistics_table(1, 1, true, false, false);
        assert!(cache.put(1, Arc::clone(&table)));
        cache.wait_for_async_updates();
        assert!(cache.get(1).is_some());
        cache.del(1);
        assert!(cache.get(1).is_none());
        cache.wait_for_async_updates();
        assert!(cache.values().is_empty());
    }

    #[test]
    fn rejected_payload_remains_as_evicted_metadata() {
        let cache = Lfu::new_for_test(1).expect("LFU");
        let table = new_mock_statistics_table(1, 1, true, true, true);
        assert!(cache.put(1, table));
        cache.wait_for_async_updates();

        let table = cache.get(1).expect("secondary metadata table");
        assert_eq!(cache.len(), 1);
        table.hist_coll.for_each_column(|_, column| {
            assert!(column.is_all_evicted());
            false
        });
        table.hist_coll.for_each_index(|_, index| {
            assert!(index.is_all_evicted());
            false
        });
    }

    #[test]
    fn copy_is_the_same_lfu_instance() {
        let cache = Lfu::new_for_test(100).expect("LFU");
        let copy = cache.copy();
        copy.put(7, new_mock_statistics_table(1, 1, true, false, false));
        copy.wait_for_async_updates();
        assert!(cache.get(7).is_some());
    }

    #[test]
    fn replacement_cost_follows_live_payload() {
        let cache = Lfu::new_for_test(10_000).expect("LFU");
        let first = new_mock_statistics_table(1, 1, true, false, false);
        let first_cost = first.memory_usage().total_tracking_mem_usage();
        cache.put(1, first);
        cache.wait_for_async_updates();
        assert_eq!(cache.cost(), first_cost);

        let replacement = new_mock_statistics_table(2, 1, true, false, false);
        let replacement_cost = replacement.memory_usage().total_tracking_mem_usage();
        cache.put(1, replacement);
        cache.wait_for_async_updates();
        assert_eq!(cache.cost(), replacement_cost);
    }

    #[test]
    fn concurrent_puts_remain_enumerable() {
        let cache = Arc::new(Lfu::new_for_test(1_000_000).expect("LFU"));
        std::thread::scope(|scope| {
            for id in 0..128_i64 {
                let cache = Arc::clone(&cache);
                scope.spawn(move || {
                    cache.put(id, new_mock_statistics_table(1, 1, true, false, false));
                    let _ = cache.get(id);
                });
            }
        });
        cache.wait_for_async_updates();
        assert_eq!(cache.len(), 128);
        assert_eq!(cache.values().len(), 128);
    }

    #[test]
    fn capacity_reduction_evicts_payload_but_keeps_tables() {
        let cache = Lfu::new_for_test(10_000).expect("LFU");
        let table = new_mock_statistics_table(2, 1, true, false, false);
        let one_table_cost = table.memory_usage().total_tracking_mem_usage();
        for id in 1..=3 {
            cache.put(id, Arc::clone(&table));
        }
        cache.wait_for_async_updates();
        cache.set_capacity(one_table_cost);
        cache.wait_for_async_updates();

        assert_eq!(cache.cost(), one_table_cost);
        assert_eq!(cache.len(), 3);
    }
}
