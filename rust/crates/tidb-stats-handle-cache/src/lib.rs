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

//! Go `pkg/statistics/handle/cache` full-table cache core.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_stats::Table;
use tidb_stats_handle_cache_internal::StatsCacheInner;
use tidb_stats_handle_cache_internal_lfu::Lfu;
use tidb_stats_handle_cache_internal_mapcache::MapCache;
use tidb_stats_handle_cache_metrics as metrics;

/// Go `StatsCache`: the full-table cache and its lifecycle maximum version.
pub struct StatsCache {
    inner: Box<dyn StatsCacheInner>,
    max_table_stats_version: AtomicU64,
}

impl StatsCache {
    /// Go `NewStatsCache`.
    pub fn new() -> Result<Self, String> {
        let config = tidb_config::config_tree::config::get_global_config();
        let inner: Box<dyn StatsCacheInner> = if config.performance.enable_stats_cache_mem_quota {
            Box::new(Lfu::new(
                tidb_vardef::STATS_CACHE_MEM_QUOTA.load(Ordering::SeqCst),
            )?)
        } else {
            Box::new(MapCache::new())
        };
        Ok(Self::from_inner(inner))
    }

    fn from_inner(inner: Box<dyn StatsCacheInner>) -> Self {
        Self {
            inner,
            max_table_stats_version: AtomicU64::new(0),
        }
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Go `Get`.
    #[must_use]
    pub fn get(&self, id: i64) -> Option<Arc<Table>> {
        let result = self.inner.get(id);
        if result.is_some() {
            metrics::hit_counter().inc();
        } else {
            metrics::miss_counter().inc();
        }
        result
    }

    /// Go `Put`, including buffered-cache retry and monotonic version movement.
    pub fn put(&self, id: i64, table: Arc<Table>) {
        loop {
            metrics::update_counter().inc();
            #[cfg(test)]
            assert!(
                table.existence_map.is_some(),
                "ColAndIdxExistenceMap should not be nil"
            );
            if self.inner.put(id, Arc::clone(&table)) {
                self.max_table_stats_version
                    .fetch_max(table.version, Ordering::AcqRel);
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Go `Values`.
    #[must_use]
    pub fn values(&self) -> Vec<Arc<Table>> {
        self.inner.values()
    }

    /// Go `Cost`.
    #[must_use]
    pub fn cost(&self) -> i64 {
        self.inner.cost()
    }

    /// Go `SetCapacity`.
    pub fn set_capacity(&self, capacity: i64) {
        self.inner.set_capacity(capacity);
    }

    /// Go `Close`.
    pub fn close(&self) {
        self.inner.close();
    }

    /// Go `Version`.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.max_table_stats_version.load(Ordering::Acquire)
    }

    /// Go `CopyAndUpdate`, used by copy-on-write mode.
    #[must_use]
    pub fn copy_and_update(&self, tables: &[Arc<Table>], deleted_ids: &[i64]) -> Self {
        let result = Self::from_inner(self.inner.copy());
        result
            .max_table_stats_version
            .store(self.version(), Ordering::Release);
        for table in tables {
            result
                .inner
                .put(table.hist_coll.physical_id, Arc::clone(table));
        }
        for id in deleted_ids {
            result.inner.del(*id);
        }
        for table in tables {
            result
                .max_table_stats_version
                .fetch_max(table.version, Ordering::AcqRel);
        }
        result
    }

    /// Go `Update`, used by quota mode.
    pub fn update(&self, tables: &[Arc<Table>], deleted_ids: &[i64], skip_move_forward: bool) {
        for table in tables {
            metrics::update_counter().inc();
            self.inner
                .put(table.hist_coll.physical_id, Arc::clone(table));
        }
        for id in deleted_ids {
            metrics::del_counter().inc();
            self.inner.del(*id);
        }
        if !skip_move_forward {
            for table in tables {
                self.max_table_stats_version
                    .fetch_max(table.version, Ordering::AcqRel);
            }
        }
    }

    /// Go `TriggerEvict`.
    pub fn trigger_evict(&self) {
        self.inner.trigger_evict();
    }

    /// Go `WaitForAsyncUpdates`.
    pub fn wait_for_async_updates(&self) {
        self.inner.wait_for_async_updates();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use tidb_stats::ColAndIdxExistenceMap;
    use tidb_stats_handle_cache_internal_testutil::new_mock_statistics_table;

    use super::*;

    fn table(id: i64, version: u64) -> Arc<Table> {
        let source = new_mock_statistics_table(1, 1, true, false, false);
        let mut table = (*source).clone();
        table.hist_coll.physical_id = id;
        table.version = version;
        table.existence_map = Some(Arc::new(RwLock::new(ColAndIdxExistenceMap::default())));
        Arc::new(table)
    }

    #[test]
    fn source_get_put_and_version() {
        let cache = StatsCache::from_inner(Box::new(MapCache::new()));
        assert!(cache.get(1).is_none());
        cache.put(1, table(1, 2));
        cache.put(2, table(2, 1));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.version(), 2);
        assert_eq!(cache.get(1).expect("table").version, 2);
    }

    #[test]
    fn source_copy_and_update_is_independent() {
        let cache = StatsCache::from_inner(Box::new(MapCache::new()));
        cache.put(1, table(1, 2));
        let copied = cache.copy_and_update(&[table(2, 3)], &[1]);
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none());
        assert!(copied.get(1).is_none());
        assert!(copied.get(2).is_some());
        assert_eq!(copied.version(), 3);
    }

    #[test]
    fn source_update_can_skip_version_movement() {
        let cache = StatsCache::from_inner(Box::new(MapCache::new()));
        cache.update(&[table(1, 4)], &[], true);
        assert_eq!(cache.version(), 0);
        cache.update(&[table(2, 3)], &[1], false);
        assert_eq!(cache.version(), 3);
        assert!(cache.get(1).is_none());
    }
}
