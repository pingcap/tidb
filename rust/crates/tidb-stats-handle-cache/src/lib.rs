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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use tidb_stats::{CopyIntent, Table};
use tidb_stats_handle_cache_internal::StatsCacheInner;
use tidb_stats_handle_cache_internal_lfu::Lfu;
use tidb_stats_handle_cache_internal_mapcache::MapCache;
use tidb_stats_handle_cache_metrics as metrics;
use tidb_stats_handle_metrics as handle_metrics;

/// Go `LeaseOffset`.
pub const LEASE_OFFSET: u32 = 5;

/// Go `types.CacheUpdate` reduced to the parent cache's owned fields.
#[derive(Clone, Debug, Default)]
pub struct CacheUpdate {
    /// Full table values to insert or replace.
    pub updated: Vec<Arc<Table>>,
    /// Physical table IDs to delete.
    pub deleted: Vec<i64>,
    /// Whether this targeted update must leave the lifecycle max version unchanged.
    pub skip_move_forward: bool,
}

/// One ordered row read by Go `StatsCacheImpl.Update` from `mysql.stats_meta`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsMetaRow {
    /// Statistics meta version.
    pub version: u64,
    /// Physical table ID.
    pub physical_id: i64,
    /// Modified rows since analysis.
    pub modify_count: i64,
    /// Current row count.
    pub count: i64,
    /// Analyze snapshot version.
    pub snapshot: u64,
    /// Latest histogram update version, or zero when SQL NULL.
    pub latest_histogram_version: u64,
}

/// Storage and schema operations consumed by Go `StatsCacheImpl.Update`.
pub trait StatsRefreshSource {
    /// Source error type.
    type Error;

    /// Go `StatsHandle.Lease`.
    fn lease(&self) -> Duration;

    /// Reads rows with `version > after_version`, ordered by version. A
    /// non-empty `physical_ids` slice applies Go's targeted `IN` predicate.
    fn stats_meta_rows(
        &self,
        after_version: u64,
        physical_ids: &[i64],
    ) -> Result<Vec<StatsMetaRow>, Self::Error>;

    /// Returns the current table metadata update TSO, or `None` after DDL removal.
    fn table_info_update_ts(&self, physical_id: i64) -> Option<u64>;

    /// Go `TableStatsFromStorage(tableInfo, physicalID, false, 0)`.
    fn table_stats_from_storage(&self, physical_id: i64)
        -> Result<Option<Arc<Table>>, Self::Error>;
}

/// Failures Go `StatsCacheImpl.Update` returns to its caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateError<E> {
    /// Reading the ordered `stats_meta` rows failed.
    Source(E),
    /// The caller's context was cancelled while rows were being processed.
    Cancelled,
}

/// One `mysql.stats_meta` row consumed by Go `getRowCountTables`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableRowCount {
    /// Physical table ID.
    pub table_id: i64,
    /// Persisted row count.
    pub count: u64,
}

/// One non-index `mysql.stats_histograms` row consumed by Go `getColLengthTables`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnLength {
    /// Physical table ID.
    pub table_id: i64,
    /// Column histogram ID.
    pub histogram_id: i64,
    /// Persisted total column size. Negative values are clamped to zero.
    pub total_size: i64,
}

/// Restricted-SQL boundary used by Go `StatsTableRowCache.UpdateByID`.
pub trait StatsTableRowSource {
    /// Source error type.
    type Error;

    /// Reads `table_id, count` from `mysql.stats_meta` for the requested IDs.
    fn table_row_counts(&self, table_ids: &[i64]) -> Result<Vec<TableRowCount>, Self::Error>;

    /// Reads non-index `table_id, hist_id, tot_col_size` rows.
    fn column_lengths(&self, table_ids: &[i64]) -> Result<Vec<ColumnLength>, Self::Error>;
}

#[derive(Default)]
struct StatsTableRowCacheState {
    table_rows: HashMap<i64, u64>,
    column_lengths: HashMap<(i64, i64), u64>,
}

/// Go `StatsTableRowCache`, the process-wide information-schema size cache.
#[derive(Default)]
pub struct StatsTableRowCache {
    state: RwLock<StatsTableRowCacheState>,
}

/// Go `TableRowStatsCache`.
pub static TABLE_ROW_STATS_CACHE: LazyLock<StatsTableRowCache> =
    LazyLock::new(StatsTableRowCache::default);

impl StatsTableRowCache {
    /// Go `GetTableRows`.
    #[must_use]
    pub fn get_table_rows(&self, table_id: i64) -> u64 {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .table_rows
            .get(&table_id)
            .copied()
            .unwrap_or_default()
    }

    /// Go `GetColLength`.
    #[must_use]
    pub fn get_column_length(&self, table_id: i64, histogram_id: i64) -> u64 {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .column_lengths
            .get(&(table_id, histogram_id))
            .copied()
            .unwrap_or_default()
    }

    /// Go `UpdateByID`. Neither map changes unless both restricted reads succeed.
    pub fn update_by_id<S>(&self, source: &S, table_ids: &[i64]) -> Result<(), S::Error>
    where
        S: StatsTableRowSource,
    {
        let table_rows = source.table_row_counts(table_ids)?;
        let column_lengths = source.column_lengths(table_ids)?;
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .table_rows
            .extend(table_rows.into_iter().map(|row| (row.table_id, row.count)));
        state
            .column_lengths
            .extend(column_lengths.into_iter().map(|row| {
                (
                    (row.table_id, row.histogram_id),
                    u64::try_from(row.total_size.max(0)).expect("nonnegative i64 fits u64"),
                )
            }));
        Ok(())
    }

    /// Go `EstimateDataLength`.
    #[must_use]
    pub fn estimate_data_length(&self, table: &tidb_model::TableInfo) -> (u64, u64, u64, u64) {
        let mut row_count = self.get_table_rows(table.id);
        let (mut data_length, mut index_length) =
            self.get_data_and_index_length(table, table.id, row_count);
        if let Some(partition) = table.get_partition_info() {
            row_count = 0;
            data_length = 0;
            for definition in partition.read().definitions.snapshot() {
                let partition_rows = self.get_table_rows(definition.id);
                row_count = row_count.wrapping_add(partition_rows);
                let (partition_data, partition_index) =
                    self.get_data_and_index_length(table, definition.id, partition_rows);
                data_length = data_length.wrapping_add(partition_data);
                index_length = index_length.wrapping_add(partition_index);
            }
        }
        let average_row_length = if row_count == 0 {
            0
        } else {
            data_length / row_count
        };
        if table.is_sequence() {
            row_count = 1;
        }
        (row_count, average_row_length, data_length, index_length)
    }

    /// Go `GetDataAndIndexLength`.
    #[must_use]
    pub fn get_data_and_index_length(
        &self,
        table: &tidb_model::TableInfo,
        physical_id: i64,
        row_count: u64,
    ) -> (u64, u64) {
        let mut column_lengths = vec![0_u64; table.columns.len()];
        let mut data_length = 0_u64;
        for (offset, column) in table.columns.iter_deref().enumerate() {
            let column = column.read();
            if column.state != tidb_model::SchemaState::PUBLIC {
                continue;
            }
            let storage_length = column.field_type.storage_length();
            let length = if storage_length == tidb_datatype::VAR_STORAGE_LEN {
                self.get_column_length(physical_id, column.id)
            } else {
                row_count.wrapping_mul(storage_length as u64)
            };
            data_length = data_length.wrapping_add(length);
            column_lengths[offset] = length;
        }

        let partitioned = table.get_partition_info().is_some();
        let mut index_length = 0_u64;
        for index in table.indices.iter_deref() {
            let index = index.read();
            if index.state != tidb_model::SchemaState::PUBLIC {
                continue;
            }
            if partitioned {
                if index.global && table.id != physical_id {
                    continue;
                }
                if !index.global && table.id == physical_id {
                    continue;
                }
            }
            for index_column in index.columns.iter_deref() {
                let index_column = index_column.read();
                let length = if index_column.length == tidb_datatype::UNSPECIFIED_LENGTH {
                    column_lengths[index_column.offset as usize]
                } else {
                    row_count.wrapping_mul(index_column.length as u64)
                };
                index_length = index_length.wrapping_add(length);
            }
        }
        (data_length, index_length)
    }
}

/// Go `buildInTableIDsString`.
#[must_use]
pub fn build_in_table_ids_string(table_ids: &[i64]) -> String {
    let ids = table_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!("table_id in ({ids})")
}

/// Go `StatsCache`: the full-table cache and its lifecycle maximum version.
pub struct StatsCache {
    inner: Box<dyn StatsCacheInner>,
    max_table_stats_version: AtomicU64,
}

/// Go `StatsCacheImpl`: an atomically replaceable full-table cache.
pub struct StatsCacheImpl {
    cache: RwLock<Arc<StatsCache>>,
}

impl StatsCacheImpl {
    /// Go `NewStatsCacheImpl`.
    pub fn new() -> Result<Self, String> {
        Ok(Self::with_cache(Arc::new(StatsCache::new()?)))
    }

    fn with_cache(cache: Arc<StatsCache>) -> Self {
        Self {
            cache: RwLock::new(cache),
        }
    }

    fn load(&self) -> Arc<StatsCache> {
        Arc::clone(
            &self
                .cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn replace_cache(&self, cache: Arc<StatsCache>) {
        let old = std::mem::replace(
            &mut *self
                .cache
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            cache,
        );
        old.close();
        metrics::cost_gauge().set(self.load().cost() as f64);
    }

    /// Go `Replace`.
    pub fn replace(&self, replacement: &Self) {
        self.replace_cache(replacement.load());
    }

    /// Go `UpdateStatsCache`, preserving quota-mode in-place updates and map-mode COW.
    pub fn update_stats_cache(&self, update: CacheUpdate) {
        if tidb_config::config_tree::config::get_global_config()
            .performance
            .enable_stats_cache_mem_quota
        {
            self.load()
                .update(&update.updated, &update.deleted, update.skip_move_forward);
        } else {
            let cache = self
                .load()
                .copy_and_update(&update.updated, &update.deleted);
            self.replace_cache(Arc::new(cache));
        }
    }

    /// Go `GetNextCheckVersionWithOffset`, with the handle-owned lease explicit.
    #[must_use]
    pub fn next_check_version_with_offset(&self, lease: Duration) -> u64 {
        let nanos = i64::try_from(lease.as_nanos()).unwrap_or(i64::MAX);
        let offset =
            tidb_stats_handle_util::duration_to_ts(nanos.saturating_mul(i64::from(LEASE_OFFSET)));
        self.max_table_stats_version().saturating_sub(offset)
    }

    /// Go `StatsCacheImpl.Update` over the source-owned SQL/schema boundary.
    pub fn update_from_source<S>(
        &self,
        source: &S,
        mut physical_ids: Vec<i64>,
        is_cancelled: impl Fn() -> bool,
    ) -> Result<(), UpdateError<S::Error>>
    where
        S: StatsRefreshSource,
    {
        let targeted = !physical_ids.is_empty();
        if targeted {
            physical_ids.sort_unstable();
            physical_ids.dedup();
        }
        let mut rows = source
            .stats_meta_rows(
                self.next_check_version_with_offset(source.lease()),
                &physical_ids,
            )
            .map_err(UpdateError::Source)?;
        rows.sort_by_key(|row| row.version);

        let mut batch = BatchUpdate::new(10, |updated: &[Arc<Table>], deleted: &[i64]| {
            self.update_stats_cache(CacheUpdate {
                updated: updated.to_vec(),
                deleted: deleted.to_vec(),
                skip_move_forward: targeted,
            });
        });
        for row in rows {
            if is_cancelled() {
                return Err(UpdateError::Cancelled);
            }
            let Some(table_info_update_ts) = source.table_info_update_ts(row.physical_id) else {
                batch.add_deleted(row.physical_id);
                continue;
            };
            let old = self.get(row.physical_id);
            if old.as_ref().is_some_and(|old| {
                old.version >= row.version && old.table_info_update_ts == table_info_update_ts
            }) {
                continue;
            }

            let mut table = if let Some(old) = old.as_ref().filter(|old| {
                row.latest_histogram_version > 0
                    && old.last_stats_hist_version >= row.latest_histogram_version
            }) {
                old.copy_as(CopyIntent::MetaOnly)
            } else {
                let loaded = match source.table_stats_from_storage(row.physical_id) {
                    Ok(loaded) => loaded,
                    // Go logs this per-table error and continues the refresh.
                    Err(_) => continue,
                };
                let Some(loaded) = loaded else {
                    batch.add_deleted(row.physical_id);
                    continue;
                };
                loaded.as_ref().clone()
            };
            table.version = row.version;
            table.last_stats_hist_version = row.latest_histogram_version;
            table.hist_coll.realtime_count = row.count;
            table.hist_coll.modify_count = row.modify_count;
            table.table_info_update_ts = table_info_update_ts;
            if table.last_analyze_version == 0 && row.snapshot != 0 {
                table.last_analyze_version = row.snapshot;
            }
            batch.add_updated(Arc::new(table));
        }
        batch.flush();
        Ok(())
    }

    /// Go `Close`.
    pub fn close(&self) {
        self.load().close();
    }

    /// Go `Clear`. Construction failure leaves the current cache untouched.
    pub fn clear(&self) {
        if let Ok(cache) = StatsCache::new() {
            self.replace_cache(Arc::new(cache));
        }
    }

    /// Go `MemConsumed`.
    #[must_use]
    pub fn mem_consumed(&self) -> i64 {
        self.load().cost()
    }

    /// Go `Get`.
    #[must_use]
    pub fn get(&self, table_id: i64) -> Option<Arc<Table>> {
        self.load().get(table_id)
    }

    /// Go `Put`.
    pub fn put(&self, id: i64, table: Arc<Table>) {
        self.load().put(id, table);
    }

    /// Go `TriggerEvict`.
    pub fn trigger_evict(&self) {
        self.load().trigger_evict();
    }

    /// Go `WaitForAsyncUpdates`.
    pub fn wait_for_async_updates(&self) {
        self.load().wait_for_async_updates();
    }

    /// Go `MaxTableStatsVersion`.
    #[must_use]
    pub fn max_table_stats_version(&self) -> u64 {
        self.load().version()
    }

    /// Go `Values`.
    #[must_use]
    pub fn values(&self) -> Vec<Arc<Table>> {
        self.load().values()
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.load().len()
    }

    /// Go `SetStatsCacheCapacity`.
    pub fn set_stats_cache_capacity(&self, capacity: i64) {
        self.load().set_capacity(capacity);
    }

    /// Go `UpdateStatsHealthyMetrics`.
    pub fn update_stats_healthy_metrics(&self) {
        let mut buckets = [0_i64; handle_metrics::STATS_HEALTHY_BUCKET_COUNT];
        for table in self.values() {
            buckets[handle_metrics::STATS_HEALTHY_BUCKET_TOTAL] += 1;
            if table.hist_coll.pseudo {
                buckets[handle_metrics::STATS_HEALTHY_BUCKET_PSEUDO] += 1;
                continue;
            }
            if !table.meets_auto_analyze_min_count(tidb_stats::DEFAULT_AUTO_ANALYZE_MIN_COUNT)
                && !table.is_analyzed()
            {
                buckets[handle_metrics::STATS_HEALTHY_BUCKET_UNNEEDED_ANALYZE] += 1;
                continue;
            }
            let (healthy, available) = table.stats_healthy();
            if available {
                buckets[stats_healthy_bucket_index(healthy)] += 1;
            }
        }
        for (index, gauge) in handle_metrics::stats_healthy_gauges()
            .into_iter()
            .enumerate()
        {
            gauge.set(buckets[index] as f64);
        }
    }
}

fn stats_healthy_bucket_index(healthy: i64) -> usize {
    debug_assert!((0..=100).contains(&healthy));
    handle_metrics::HEALTHY_BUCKET_CONFIGS
        .iter()
        .find(|config| config.upper_bound > 0 && healthy < config.upper_bound)
        .map_or(handle_metrics::STATS_HEALTHY_BUCKET_100_TO_100, |config| {
            config.index
        })
}

struct BatchUpdate<F> {
    operation: F,
    updated: Vec<Arc<Table>>,
    deleted: Vec<i64>,
    batch_size: usize,
}

impl<F> BatchUpdate<F>
where
    F: FnMut(&[Arc<Table>], &[i64]),
{
    fn new(batch_size: usize, operation: F) -> Self {
        Self {
            operation,
            updated: Vec::with_capacity(batch_size),
            deleted: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    fn flush_internal(&mut self) {
        (self.operation)(&self.updated, &self.deleted);
        self.updated.clear();
        self.deleted.clear();
    }

    fn add_updated(&mut self, table: Arc<Table>) {
        if self.updated.len() == self.batch_size {
            self.flush_internal();
        }
        self.updated.push(table);
    }

    fn add_deleted(&mut self, id: i64) {
        if self.deleted.len() == self.batch_size {
            self.flush_internal();
        }
        self.deleted.push(id);
    }

    fn flush(&mut self) {
        if !self.updated.is_empty() || !self.deleted.is_empty() {
            self.flush_internal();
        }
    }
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
    use std::collections::HashMap;
    use std::sync::Mutex;

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

    #[test]
    fn source_batch_update_flushes_each_side_at_its_limit() {
        let mut flushed = Vec::new();
        {
            let mut batch = BatchUpdate::new(3, |updated: &[Arc<Table>], deleted: &[i64]| {
                flushed.push((
                    updated
                        .iter()
                        .map(|table| table.hist_coll.physical_id)
                        .collect::<Vec<_>>(),
                    deleted.to_vec(),
                ));
            });
            batch.add_updated(table(1, 0));
            batch.add_updated(table(2, 0));
            batch.add_updated(table(3, 0));
            batch.add_updated(table(4, 0));
            batch.add_deleted(5);
            batch.add_deleted(6);
            batch.add_deleted(7);
            batch.add_updated(table(8, 0));
            batch.add_deleted(9);
            batch.flush();
        }
        assert_eq!(
            flushed,
            vec![
                (vec![1, 2, 3], vec![]),
                (vec![4, 8], vec![5, 6, 7]),
                (vec![], vec![9]),
            ]
        );
    }

    #[test]
    fn source_healthy_metrics_use_exact_buckets() {
        for gauge in handle_metrics::stats_healthy_gauges() {
            gauge.set(0.0);
        }
        let cache = StatsCache::from_inner(Box::new(MapCache::new()));
        for (id, pseudo, count, modify, analyzed) in [
            (0, false, 2_000, 1_000, 0),
            (1, false, 2_000, 1_100, 1),
            (2, false, 2_000, 920, 1),
            (3, false, 2_000, 200, 1),
            (4, false, 2_000, 0, 1),
            (5, true, 10_000, 0, 0),
            (6, false, 800, 500, 1),
            (7, false, 800, 500, 0),
        ] {
            let mut table = (*table(id, 0)).clone();
            table.hist_coll.pseudo = pseudo;
            table.hist_coll.realtime_count = count;
            table.hist_coll.modify_count = modify;
            table.last_analyze_version = analyzed;
            cache.inner.put(id, Arc::new(table));
        }
        let cache = StatsCacheImpl::with_cache(Arc::new(cache));
        cache.update_stats_healthy_metrics();
        assert_eq!(
            handle_metrics::stats_healthy_gauges().len(),
            handle_metrics::STATS_HEALTHY_BUCKET_COUNT
        );
        assert_eq!(
            handle_metrics::stats_healthy_gauges()
                .into_iter()
                .map(|gauge| gauge.get())
                .collect::<Vec<_>>(),
            vec![3.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 8.0, 1.0, 1.0]
        );
    }

    struct RefreshSource {
        rows: Vec<StatsMetaRow>,
        metadata: HashMap<i64, u64>,
        loaded: HashMap<i64, Result<Option<Arc<Table>>, &'static str>>,
        requested: Mutex<Vec<Vec<i64>>>,
        loaded_ids: Mutex<Vec<i64>>,
    }

    impl StatsRefreshSource for RefreshSource {
        type Error = &'static str;

        fn lease(&self) -> Duration {
            Duration::ZERO
        }

        fn stats_meta_rows(
            &self,
            _after_version: u64,
            physical_ids: &[i64],
        ) -> Result<Vec<StatsMetaRow>, Self::Error> {
            self.requested.lock().unwrap().push(physical_ids.to_vec());
            Ok(self.rows.clone())
        }

        fn table_info_update_ts(&self, physical_id: i64) -> Option<u64> {
            self.metadata.get(&physical_id).copied()
        }

        fn table_stats_from_storage(
            &self,
            physical_id: i64,
        ) -> Result<Option<Arc<Table>>, Self::Error> {
            self.loaded_ids.lock().unwrap().push(physical_id);
            self.loaded.get(&physical_id).cloned().unwrap_or(Ok(None))
        }
    }

    #[test]
    fn source_refresh_reuses_payload_deletes_unknown_and_skips_load_errors() {
        let cache = StatsCache::from_inner(Box::new(MapCache::new()));
        let mut old = (*table(1, 10)).clone();
        old.last_stats_hist_version = 100;
        old.table_info_update_ts = 1;
        cache.put(1, Arc::new(old));
        cache.put(2, table(2, 5));
        let cache = StatsCacheImpl::with_cache(Arc::new(cache));
        let source = RefreshSource {
            // Deliberately out of order: Update orders by version.
            rows: vec![
                StatsMetaRow {
                    version: 12,
                    physical_id: 4,
                    count: 40,
                    snapshot: 77,
                    latest_histogram_version: 12,
                    ..StatsMetaRow::default()
                },
                StatsMetaRow {
                    version: 11,
                    physical_id: 1,
                    modify_count: 3,
                    count: 30,
                    latest_histogram_version: 90,
                    ..StatsMetaRow::default()
                },
                StatsMetaRow {
                    version: 13,
                    physical_id: 2,
                    ..StatsMetaRow::default()
                },
                StatsMetaRow {
                    version: 14,
                    physical_id: 3,
                    ..StatsMetaRow::default()
                },
            ],
            metadata: HashMap::from([(1, 1), (3, 1), (4, 2)]),
            loaded: HashMap::from([(3, Err("ddl changed")), (4, Ok(Some(table(4, 0))))]),
            requested: Mutex::new(Vec::new()),
            loaded_ids: Mutex::new(Vec::new()),
        };

        cache
            .update_from_source(&source, vec![4, 1, 4], || false)
            .unwrap();

        assert_eq!(*source.requested.lock().unwrap(), vec![vec![1, 4]]);
        assert_eq!(*source.loaded_ids.lock().unwrap(), vec![4, 3]);
        assert!(cache.get(2).is_none());
        assert!(cache.get(3).is_none());
        let reused = cache.get(1).unwrap();
        assert_eq!(reused.version, 11);
        assert_eq!(reused.hist_coll.realtime_count, 30);
        assert_eq!(reused.hist_coll.modify_count, 3);
        assert_eq!(reused.last_stats_hist_version, 90);
        let loaded = cache.get(4).unwrap();
        assert_eq!(loaded.version, 12);
        assert_eq!(loaded.last_analyze_version, 77);
        assert_eq!(loaded.table_info_update_ts, 2);
        // A targeted refresh must not move the cache-wide scan watermark.
        assert_eq!(cache.max_table_stats_version(), 10);
    }

    #[test]
    fn source_refresh_cancellation_discards_the_pending_batch() {
        let cache =
            StatsCacheImpl::with_cache(Arc::new(StatsCache::from_inner(Box::new(MapCache::new()))));
        let source = RefreshSource {
            rows: vec![StatsMetaRow {
                version: 1,
                physical_id: 1,
                ..StatsMetaRow::default()
            }],
            metadata: HashMap::from([(1, 1)]),
            loaded: HashMap::from([(1, Ok(Some(table(1, 0))))]),
            requested: Mutex::new(Vec::new()),
            loaded_ids: Mutex::new(Vec::new()),
        };
        assert_eq!(
            cache.update_from_source(&source, Vec::new(), || true),
            Err(UpdateError::Cancelled)
        );
        assert_eq!(cache.len(), 0);
    }

    struct TableRowSource {
        rows: Result<Vec<TableRowCount>, &'static str>,
        lengths: Result<Vec<ColumnLength>, &'static str>,
    }

    impl StatsTableRowSource for TableRowSource {
        type Error = &'static str;

        fn table_row_counts(&self, _table_ids: &[i64]) -> Result<Vec<TableRowCount>, Self::Error> {
            self.rows.clone()
        }

        fn column_lengths(&self, _table_ids: &[i64]) -> Result<Vec<ColumnLength>, Self::Error> {
            self.lengths.clone()
        }
    }

    fn table_info_with_indexes() -> tidb_model::TableInfo {
        use tidb_datatype::{FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
        use tidb_model::go_runtime::{GoShared, GoSharedPointerSlice};
        use tidb_model::{ColumnInfo, IndexColumn, IndexInfo, SchemaState, TableInfo};

        let fixed = GoShared::new(ColumnInfo {
            id: 1,
            offset: 0,
            field_type: FieldType::parser(FieldTypeCode::LongLong),
            state: SchemaState::PUBLIC,
            ..ColumnInfo::default()
        });
        let variable = GoShared::new(ColumnInfo {
            id: 2,
            offset: 1,
            field_type: FieldType::parser(FieldTypeCode::Varchar),
            state: SchemaState::PUBLIC,
            ..ColumnInfo::default()
        });
        let index_column = || {
            GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(IndexColumn {
                offset: 0,
                length: UNSPECIFIED_LENGTH,
                ..IndexColumn::default()
            }))])
        };
        let local = GoShared::new(IndexInfo {
            columns: index_column(),
            state: SchemaState::PUBLIC,
            ..IndexInfo::default()
        });
        let global = GoShared::new(IndexInfo {
            columns: index_column(),
            state: SchemaState::PUBLIC,
            global: true,
            ..IndexInfo::default()
        });
        TableInfo {
            id: 100,
            columns: GoSharedPointerSlice::from_handles(vec![Some(fixed), Some(variable)]),
            indices: GoSharedPointerSlice::from_handles(vec![Some(local), Some(global)]),
            ..TableInfo::default()
        }
    }

    #[test]
    fn source_table_row_cache_updates_atomically_and_clamps_column_length() {
        let cache = StatsTableRowCache::default();
        cache
            .update_by_id(
                &TableRowSource {
                    rows: Ok(vec![TableRowCount {
                        table_id: 1,
                        count: 7,
                    }]),
                    lengths: Ok(vec![ColumnLength {
                        table_id: 1,
                        histogram_id: 2,
                        total_size: -9,
                    }]),
                },
                &[1],
            )
            .unwrap();
        assert_eq!(cache.get_table_rows(1), 7);
        assert_eq!(cache.get_column_length(1, 2), 0);

        assert_eq!(
            cache.update_by_id(
                &TableRowSource {
                    rows: Ok(vec![TableRowCount {
                        table_id: 1,
                        count: 99,
                    }]),
                    lengths: Err("histogram read failed"),
                },
                &[1],
            ),
            Err("histogram read failed")
        );
        assert_eq!(cache.get_table_rows(1), 7);
        assert_eq!(
            build_in_table_ids_string(&[3, -2, 9]),
            "table_id in (3,-2,9)"
        );
    }

    #[test]
    fn source_table_row_cache_estimates_partition_and_global_indexes() {
        use tidb_model::go_runtime::{GoShared, GoSharedSlice};
        use tidb_model::{PartitionDefinition, PartitionInfo};

        let cache = StatsTableRowCache::default();
        cache
            .update_by_id(
                &TableRowSource {
                    rows: Ok(vec![
                        TableRowCount {
                            table_id: 100,
                            count: 7,
                        },
                        TableRowCount {
                            table_id: 101,
                            count: 2,
                        },
                        TableRowCount {
                            table_id: 102,
                            count: 3,
                        },
                    ]),
                    lengths: Ok(vec![
                        ColumnLength {
                            table_id: 101,
                            histogram_id: 2,
                            total_size: 3,
                        },
                        ColumnLength {
                            table_id: 102,
                            histogram_id: 2,
                            total_size: 4,
                        },
                    ]),
                },
                &[],
            )
            .unwrap();
        let mut table = table_info_with_indexes();
        table.partition = Some(GoShared::new(PartitionInfo {
            enable: true,
            definitions: GoSharedSlice::from_vec(vec![
                PartitionDefinition {
                    id: 101,
                    ..PartitionDefinition::default()
                },
                PartitionDefinition {
                    id: 102,
                    ..PartitionDefinition::default()
                },
            ]),
            ..PartitionInfo::default()
        }));

        // Data: (2 * 8 + 3) + (3 * 8 + 4) = 47. Local index: 40.
        // Global index is calculated once at table level from 7 rows: 56.
        assert_eq!(cache.estimate_data_length(&table), (5, 9, 47, 96));
    }
}
