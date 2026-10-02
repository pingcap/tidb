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
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock, RwLock, RwLockReadGuard, Weak};

use stretto::{
    Cache, CacheBuilder, CacheCallback, DefaultCoster, DefaultUpdateValidator, Item,
    TransparentKeyBuilder,
};
use tidb_log::{Field, Value};
use tidb_stats::{CopyIntent, Table};
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
        // `rem_euclid` keeps every i64 key in `[0, 256)`: identical to the
        // signed remainder for non-negative keys, and a valid shard for the
        // negative ids the cluster allocator can hand the stats cache (a
        // bare `%` wraps `as usize` into an out-of-bounds index — the
        // g-admin2 analyze panic).
        let index = key.rem_euclid(KEY_SET_COUNT as i64) as usize;
        &self.shards[index]
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
            *shard
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = HashMap::new();
        }
    }
}

// A nil value is Go's eviction trigger, not a statistics table. The outer
// Option on Stretto callbacks independently represents an already-removed item.
type CachedTable = Option<Arc<Table>>;
type Primary = Cache<
    i64,
    CachedTable,
    TransparentKeyBuilder<i64>,
    DefaultCoster<CachedTable>,
    DefaultUpdateValidator<CachedTable>,
    Callbacks,
>;

struct State {
    tables: KeySetShard,
    cost: AtomicI64,
    closed: AtomicBool,
    primary: OnceLock<Weak<Primary>>,
    // Pause a live primary handle at the precise shutdown race in tests.
    #[cfg(test)]
    before_trigger: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
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
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let Some(cache) = self.primary.get().and_then(Weak::upgrade) else {
            return;
        };
        #[cfg(test)]
        {
            let hook = self.before_trigger.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        if self.cost.load(Ordering::Acquire) > cache.max_cost() {
            let key = -((rand::random::<u64>() & i64::MAX as u64) as i64);
            cache.insert(key, None, 0);
        }
    }

    fn drop_memory(&self, item: &Item<CachedTable>) {
        let Some(table) = item.val.as_ref().and_then(Option::as_ref) else {
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

#[derive(Clone)]
struct Callbacks(Arc<State>);

// Go recovers each callback separately so a bad eviction cannot kill the
// cache processor, and onExit still runs after a recovered onEvict/onReject.
fn recover_callback(name: &str, action: impl FnOnce()) {
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic");
        tidb_util::logutil::bg_logger().warn(
            &format!("panic in {name}"),
            &[
                Field::new("error", Value::Str(message.to_owned())),
                Field::new(
                    "stack",
                    Value::Str(std::backtrace::Backtrace::force_capture().to_string()),
                ),
            ],
        );
    }
}

impl CacheCallback for Callbacks {
    type Value = CachedTable;

    fn on_exit(&self, value: Option<Self::Value>) {
        recover_callback("onExit", || {
            let Some(table) = value.flatten() else { return };
            if self.0.closed.load(Ordering::Acquire) {
                return;
            }
            self.0.trigger_evict();
            self.0
                .add_cost(-table.memory_usage().total_tracking_mem_usage());
        });
    }

    fn on_evict(&self, item: Item<Self::Value>) {
        recover_callback("onEvict", || {
            self.0.drop_memory(&item);
            metrics::evict_counter().inc();
        });
        self.on_exit(item.val);
    }

    fn on_reject(&self, item: Item<Self::Value>) {
        recover_callback("onReject", || {
            self.0.drop_memory(&item);
            metrics::reject_counter().inc();
        });
        self.on_exit(item.val);
    }
}

/// Go `LFU`.
pub struct Lfu {
    state: Arc<State>,
    // Reads share the lifetime gate. Close takes it exclusively through the
    // final primary drop/join, so every concurrent closer observes completion.
    // Callbacks use State's weak primary reference and never acquire this gate.
    cache: Arc<RwLock<Option<Arc<Primary>>>>,
}

impl Lfu {
    /// Go `NewLFU`.
    pub fn new(total_mem_cost: i64) -> Result<Self, String> {
        Self::with_internal_cost(total_mem_cost, false)
    }

    fn with_internal_cost(total_mem_cost: i64, ignore_internal_cost: bool) -> Result<Self, String> {
        let mut cost = adjust_mem_cost(total_mem_cost)?;
        // Go's intest.InTest path avoids an oversized TinyLFU sketch when the
        // caller asks for the default quota. The test-only constructor passes
        // this same mode through `ignore_internal_cost`.
        if ignore_internal_cost && total_mem_cost == 0 {
            cost = 5_000_000;
        }
        metrics::capacity_gauge().set(cost as f64);
        let state = Arc::new(State {
            tables: KeySetShard::new(),
            cost: AtomicI64::new(0),
            closed: AtomicBool::new(false),
            primary: OnceLock::new(),
            #[cfg(test)]
            before_trigger: std::sync::Mutex::new(None),
        });
        let cache = Arc::new(
            CacheBuilder::new_with_key_builder(
                usize::try_from((cost / 128).clamp(10, 1_000_000)).unwrap_or(10),
                cost,
                TransparentKeyBuilder::default(),
            )
            .set_buffer_items(64)
            .set_ignore_internal_cost(ignore_internal_cost)
            .set_metrics(ignore_internal_cost)
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
            cache: Arc::new(RwLock::new(Some(cache))),
        })
    }

    #[cfg(test)]
    fn new_for_test(total_mem_cost: i64) -> Result<Self, String> {
        Self::with_internal_cost(total_mem_cost, true)
    }

    fn primary(&self) -> RwLockReadGuard<'_, Option<Arc<Primary>>> {
        self.cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `Clear`: remove cached data while retaining the reusable cache owner.
    pub fn clear(&self) {
        if let Some(cache) = self.primary().as_ref() {
            // Go clears the primary first because its callbacks can repopulate
            // the fallback metadata. Clear the fallback only after they finish.
            cache.clear().expect("LFU primary clear");
        }
        self.state.tables.clear();
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
            .as_ref()
            .and_then(|cache| {
                cache.get(&table_id).map(|value| {
                    Arc::clone(
                        value
                            .value()
                            .as_ref()
                            .expect("LFU eviction trigger is not a table"),
                    )
                })
            })
            .or_else(|| self.state.tables.get(table_id))
    }

    fn put(&self, table_id: i64, table: Arc<Table>) -> bool {
        let primary = self.primary();
        let cost = table.memory_usage().total_tracking_mem_usage();
        self.state.tables.put(table_id, Arc::clone(&table));
        self.state.add_cost(cost);
        primary
            .as_ref()
            .is_some_and(|cache| cache.insert(table_id, Some(table), cost))
    }

    fn del(&self, table_id: i64) {
        if let Some(cache) = self.primary().as_ref() {
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
        let capacity = match adjust_mem_cost(capacity) {
            Ok(capacity) => capacity,
            Err(error) => {
                tidb_util::logutil::bg_logger().warn(
                    "adjustMemCost failed",
                    &[Field::new("error", Value::Str(error))],
                );
                return;
            }
        };
        let primary = self.primary();
        if let Some(cache) = primary.as_ref() {
            cache.update_max_cost(capacity);
        }
        self.state.trigger_evict();
        metrics::capacity_gauge().set(capacity as f64);
        metrics::cost_gauge().set(self.cost() as f64);
    }
    fn close(&self) {
        let mut primary = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.state.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(cache) = primary.take() {
            cache.clear().expect("LFU primary clear during close");
            self.state.tables.clear();
            cache.wait().expect("LFU primary wait during close");
            // Stretto joins its worker on final drop. Keep the lifetime gate
            // until that completes, equivalent to Go's sync.Once Close body.
            drop(cache);
        }
    }
    fn trigger_evict(&self) {
        // State's Weak upgrade must not outlive a public operation's guard.
        // Callbacks call State directly while Close drains their processor.
        let _primary = self.primary();
        self.state.trigger_evict();
    }
    fn wait_for_async_updates(&self) {
        if let Some(cache) = self.primary().as_ref() {
            cache.wait().expect("LFU primary wait");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_stats_handle_cache_internal_testutil::new_mock_statistics_table;

    #[test]
    fn public_eviction_operations_cannot_outlive_close() {
        use std::sync::mpsc;
        use std::time::Duration;

        for set_capacity in [false, true] {
            let cache = Arc::new(Lfu::new_for_test(100).unwrap());
            let owner = Arc::downgrade(cache.primary().as_ref().unwrap());
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            *cache.state.before_trigger.lock().unwrap() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }));
            let operation = {
                let cache = Arc::clone(&cache);
                std::thread::spawn(move || {
                    if set_capacity {
                        cache.set_capacity(50);
                    } else {
                        cache.trigger_evict();
                    }
                })
            };
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let (closed_tx, closed_rx) = mpsc::channel();
            let closer = {
                let cache = Arc::clone(&cache);
                std::thread::spawn(move || {
                    cache.close();
                    closed_tx.send(()).unwrap();
                })
            };
            let premature = closed_rx.recv_timeout(Duration::from_millis(100)).is_ok();
            release_tx.send(()).unwrap();
            operation.join().unwrap();
            closer.join().unwrap();
            assert!(
                !premature,
                "Close returned with a live eviction handle (SetCapacity={set_capacity})"
            );
            assert!(owner.upgrade().is_none());
        }
    }

    #[test]
    fn close_waits_for_active_primary_operations_and_other_closers() {
        use std::sync::mpsc;
        use std::time::Duration;

        let cache = Arc::new(Lfu::new_for_test(100).unwrap());
        let primary_operation = cache.primary();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let started = started_tx.clone();
                let done = done_tx.clone();
                std::thread::spawn(move || {
                    started.send(()).unwrap();
                    cache.close();
                    done.send(()).unwrap();
                })
            })
            .collect();
        for _ in 0..2 {
            started_rx.recv().unwrap();
        }
        let premature = done_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        drop(primary_operation);
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(
            !premature,
            "Close must wait for an active primary operation and the closing owner"
        );
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn negative_shard_zero_is_a_real_table_not_an_eviction_trigger() {
        let cache = Lfu::new_for_test(1).unwrap();
        let table = new_mock_statistics_table(1, 1, true, true, true);
        // Go -256 % 256 == 0: a valid shard, unlike -1.
        assert!(cache.put(-256, table));
        cache.wait_for_async_updates();
        let retained = cache.get(-256).unwrap();
        assert_eq!(retained.memory_usage().total_tracking_mem_usage(), 0);
        retained.hist_coll.for_each_column(|_, column| {
            assert!(column.is_all_evicted());
            false
        });
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.cost(), 0);
    }

    #[test]
    fn clear_reuses_the_shared_owner_and_close_releases_it() {
        let cache = Lfu::new_for_test(100).unwrap();
        let alias = cache.copy();
        let table = new_mock_statistics_table(1, 1, true, false, false);
        cache.put(1, Arc::clone(&table));
        cache.wait_for_async_updates();
        let owner = Arc::downgrade(cache.primary().as_ref().unwrap());
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.cost(), 0);
        assert!(owner.upgrade().is_some());
        assert!(alias.put(2, Arc::clone(&table)));
        alias.wait_for_async_updates();
        assert!(Arc::ptr_eq(&cache.get(2).unwrap(), &table));
        let cost_before_close = cache.cost();
        cache.close();
        alias.close();
        assert_eq!(cache.cost(), cost_before_close);
        assert!(
            owner.upgrade().is_none(),
            "Close releases and joins the primary owner"
        );
        assert!(alias.get(2).is_none());
        // Go publishes fallback metadata even when Set on the closed primary
        // returns false; Clear must still remove this fallback after Close.
        assert!(!alias.put(3, table));
        alias.close();
        assert_eq!(cache.len(), 1);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn rejected_callback_recovers_before_exit_and_the_processor_continues() {
        let cache = Lfu::new_for_test(1).unwrap();
        let table = new_mock_statistics_table(1, 1, true, false, false);
        let cost = table.memory_usage().total_tracking_mem_usage();
        // Inject an invalid shard directly into the primary's callback path:
        // dropMemory panics, Go recovers, then Ristretto still invokes onExit.
        cache.state.add_cost(cost);
        cache
            .primary()
            .as_ref()
            .unwrap()
            .insert(-1, Some(table), cost);
        cache.wait_for_async_updates();
        assert_eq!(cache.cost(), 0);
        assert!(cache.put(1, new_mock_statistics_table(1, 1, true, false, false)));
        cache.wait_for_async_updates();
        assert_eq!(cache.cost(), 0);
        assert_eq!(cache.len(), 1);

        let callbacks = Callbacks(Arc::clone(&cache.state));
        callbacks.on_exit(None);
        callbacks.on_exit(Some(None));
        assert_eq!(cache.cost(), 0);
        cache.close();
        callbacks.on_exit(Some(Some(new_mock_statistics_table(
            1, 1, true, false, false,
        ))));
        assert_eq!(cache.cost(), 0);
    }

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

    #[test]
    #[should_panic]
    fn negative_table_id_matches_go_shard_indexing() {
        let cache = Lfu::new_for_test(100).expect("LFU");
        cache.put(-1, new_mock_statistics_table(1, 1, true, false, false));
    }

    #[test]
    fn test_mode_zero_capacity_uses_go_override() {
        let cache = Lfu::new_for_test(0).expect("LFU");
        assert_eq!(
            cache.primary().as_ref().expect("primary").max_cost(),
            5_000_000
        );
    }
}

#[cfg(test)]
mod source_tests;
