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

//! Go `pkg/statistics/handle/usage`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tidb_model::TableItemID;
use tidb_stats_handle_usage_indexusage::{Collector as IndexUsageCollector, Sample};

const DEFAULT_DUMP_STATS_DELTA_RATIO: f64 = 1.0 / 10_000.0;
const DEFAULT_DUMP_STATS_MAX_DURATION: Duration = Duration::from_secs(60 * 60);
static DUMP_STATS_DELTA_RATIO: AtomicU64 = AtomicU64::new(DEFAULT_DUMP_STATS_DELTA_RATIO.to_bits());
static DUMP_STATS_MAX_DURATION: Mutex<Duration> = Mutex::new(DEFAULT_DUMP_STATS_MAX_DURATION);
/// Go `colStatsUsageLastUsedThrottleInterval`.
pub const COL_STATS_USAGE_LAST_USED_THROTTLE_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(12 * 60 * 60);
/// Go `batchInsertSize`.
pub const BATCH_INSERT_SIZE: usize = 2_048;
/// Go `dumpDeltaBatchSize`.
pub const DUMP_DELTA_BATCH_SIZE: usize = 100_000;

/// Go `variable.TableDelta`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TableDelta {
    /// The table row-count change.
    pub delta: i64,
    /// The number of modified rows.
    pub count: i64,
    /// The first time represented by this accumulated delta.
    pub init_time: Option<SystemTime>,
}

/// Go `storage.DeltaUpdate`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeltaUpdate {
    /// Physical or logical table ID to update.
    pub table_id: i64,
    /// Accumulated row and modification deltas.
    pub delta: TableDelta,
    /// Whether the delta is stashed in `mysql.stats_table_locked`.
    pub is_locked: bool,
}

/// Go `statsUsageImpl.dumpStatsDeltaToKV`'s lock and partition expansion.
#[must_use]
pub fn prepare_delta_updates(
    mut updates: Vec<DeltaUpdate>,
    parent_table_id: impl Fn(i64) -> Option<i64>,
    locked_tables: &HashSet<i64>,
) -> Vec<DeltaUpdate> {
    let original_len = updates.len();
    for index in 0..original_len {
        if updates[index].delta.count == 0 {
            continue;
        }
        if let Some(parent_id) = parent_table_id(updates[index].table_id) {
            let table_locked = locked_tables.contains(&parent_id);
            let partition_locked = locked_tables.contains(&updates[index].table_id);
            updates[index].is_locked = table_locked || partition_locked;
            if !table_locked && !partition_locked {
                updates.push(DeltaUpdate {
                    table_id: parent_id,
                    delta: updates[index].delta,
                    is_locked: false,
                });
            }
        } else {
            updates[index].is_locked = locked_tables.contains(&updates[index].table_id);
        }
    }
    if updates.len() > original_len {
        updates.sort_unstable_by_key(|update| update.table_id);
    }
    updates
}

impl TableDelta {
    /// Go `TableDelta.MergeFrom`.
    pub fn merge_from(&mut self, incoming: Self) {
        self.delta = self.delta.wrapping_add(incoming.delta);
        self.count = self.count.wrapping_add(incoming.count);
        if self.init_time.is_none()
            || incoming
                .init_time
                .is_some_and(|incoming| self.init_time.is_some_and(|current| incoming < current))
        {
            self.init_time = incoming.init_time;
        }
    }
}

/// Go `TableDeltaMap`.
#[derive(Default)]
pub struct TableDeltaMap {
    delta: Mutex<HashMap<i64, TableDelta>>,
}

impl TableDeltaMap {
    /// Go `NewTableDeltaMap`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `TableDeltaMap.Reset`.
    pub fn reset(&self) {
        self.delta
            .lock()
            .expect("table delta lock poisoned")
            .clear();
    }

    /// Go `TableDeltaMap.GetDeltaAndReset`.
    #[must_use]
    pub fn get_delta_and_reset(&self) -> HashMap<i64, TableDelta> {
        std::mem::take(&mut *self.delta.lock().expect("table delta lock poisoned"))
    }

    /// Go `TableDeltaMap.Update`.
    pub fn update(&self, id: i64, delta: i64, count: i64) {
        debug_assert!(id > 0, "table ID should be greater than 0");
        let mut values = self.delta.lock().expect("table delta lock poisoned");
        let item = values.entry(id).or_default();
        item.delta = item.delta.wrapping_add(delta);
        item.count = item.count.wrapping_add(count);
    }

    /// Go `TableDeltaMap.Merge`.
    pub fn merge(&self, incoming: HashMap<i64, TableDelta>) {
        if incoming.is_empty() {
            return;
        }
        let mut values = self.delta.lock().expect("table delta lock poisoned");
        for (id, incoming) in incoming {
            values.entry(id).or_default().merge_from(incoming);
        }
    }

    /// Go `TransactionContext.GetCurrentSavepoint`'s table-delta clone.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<i64, TableDelta> {
        self.delta
            .lock()
            .expect("table delta lock poisoned")
            .clone()
    }

    /// Go `TransactionContext.RestoreBySavepoint`'s table-delta restore.
    pub fn restore(&self, snapshot: HashMap<i64, TableDelta>) {
        *self.delta.lock().expect("table delta lock poisoned") = snapshot;
    }
}

/// Go `StatsUsage`.
#[derive(Default)]
pub struct StatsUsage {
    usage: Mutex<HashMap<TableItemID, SystemTime>>,
}

impl StatsUsage {
    /// Go `NewStatsUsage`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `StatsUsage.Reset`.
    pub fn reset(&self) {
        self.usage
            .lock()
            .expect("column stats usage lock poisoned")
            .clear();
    }

    /// Go `StatsUsage.GetUsageAndReset`.
    #[must_use]
    pub fn get_usage_and_reset(&self) -> HashMap<TableItemID, SystemTime> {
        std::mem::take(&mut *self.usage.lock().expect("column stats usage lock poisoned"))
    }

    /// Go `StatsUsage.Merge`.
    pub fn merge(&self, incoming: HashMap<TableItemID, SystemTime>) {
        if incoming.is_empty() {
            return;
        }
        let mut values = self.usage.lock().expect("column stats usage lock poisoned");
        for (id, incoming) in incoming {
            let current = values.entry(id).or_insert(incoming);
            if *current < incoming {
                *current = incoming;
            }
        }
    }

    /// Go `StatsUsage.MergeRawData`.
    pub fn merge_raw_data(
        &self,
        items: impl IntoIterator<Item = TableItemID>,
        update_time: SystemTime,
    ) {
        let mut values = self.usage.lock().expect("column stats usage lock poisoned");
        for item in items {
            debug_assert!(
                !item.is_index,
                "predicate column should only be table column"
            );
            let current = values.entry(item).or_insert(update_time);
            if *current < update_time {
                *current = update_time;
            }
        }
    }
}

/// Go `SessionStatsItem`.
pub struct SessionStatsItem {
    state: Mutex<SessionStatsItemState>,
}

struct SessionStatsItemState {
    mapper: HashMap<i64, TableDelta>,
    stats_usage: HashMap<TableItemID, SystemTime>,
    deleted: bool,
}

impl SessionStatsItem {
    fn new() -> Self {
        Self {
            state: Mutex::new(SessionStatsItemState {
                mapper: HashMap::new(),
                stats_usage: HashMap::new(),
                deleted: false,
            }),
        }
    }

    /// Go `SessionStatsItem.Delete`.
    pub fn delete(&self) {
        self.state
            .lock()
            .expect("session stats lock poisoned")
            .deleted = true;
    }

    /// Go `SessionStatsItem.Update`.
    pub fn update(&self, id: i64, delta: i64, count: i64) {
        debug_assert!(id > 0, "table ID should be greater than 0");
        let mut state = self.state.lock().expect("session stats lock poisoned");
        let item = state.mapper.entry(id).or_default();
        item.delta = item.delta.wrapping_add(delta);
        item.count = item.count.wrapping_add(count);
    }

    /// Go `SessionStatsItem.UpdateColStatsUsage`.
    pub fn update_col_stats_usage(
        &self,
        items: impl IntoIterator<Item = TableItemID>,
        update_time: SystemTime,
    ) {
        let mut state = self.state.lock().expect("session stats lock poisoned");
        for item in items {
            debug_assert!(
                !item.is_index,
                "predicate column should only be table column"
            );
            let current = state.stats_usage.entry(item).or_insert(update_time);
            if *current < update_time {
                *current = update_time;
            }
        }
    }

    fn sweep_into(&self, delta: &TableDeltaMap, usage: &StatsUsage) -> bool {
        let (mapper, stats_usage, deleted) = {
            let mut state = self.state.lock().expect("session stats lock poisoned");
            (
                std::mem::take(&mut state.mapper),
                std::mem::take(&mut state.stats_usage),
                state.deleted,
            )
        };
        delta.merge(mapper);
        usage.merge(stats_usage);
        deleted
    }
}

/// Go `SessionStatsList`.
pub struct SessionStatsList {
    table_delta: TableDeltaMap,
    stats_usage: StatsUsage,
    sessions: Mutex<Vec<Arc<SessionStatsItem>>>,
}

impl SessionStatsList {
    /// Go `NewSessionStatsList`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            table_delta: TableDeltaMap::new(),
            stats_usage: StatsUsage::new(),
            sessions: Mutex::new(Vec::new()),
        }
    }

    /// Go `SessionStatsList.NewSessionStatsItem`.
    #[must_use]
    pub fn new_session_stats_item(&self) -> Arc<SessionStatsItem> {
        let item = Arc::new(SessionStatsItem::new());
        self.sessions
            .lock()
            .expect("session stats list lock poisoned")
            .push(Arc::clone(&item));
        item
    }

    /// Go `SessionStatsList.SweepSessionStatsList`.
    pub fn sweep_session_stats_list(&self) {
        self.sessions
            .lock()
            .expect("session stats list lock poisoned")
            .retain(|item| !item.sweep_into(&self.table_delta, &self.stats_usage));
    }

    /// Go `SessionStatsList.SessionTableDelta`.
    #[must_use]
    pub const fn session_table_delta(&self) -> &TableDeltaMap {
        &self.table_delta
    }

    /// Go `SessionStatsList.SessionStatsUsage`.
    #[must_use]
    pub const fn session_stats_usage(&self) -> &StatsUsage {
        &self.stats_usage
    }

    /// Go `SessionStatsList.ResetSessionStatsList`.
    pub fn reset_session_stats_list(&self) {
        self.sessions
            .lock()
            .expect("session stats list lock poisoned")
            .clear();
        self.table_delta.reset();
        self.stats_usage.reset();
    }

    /// Starts Go `DumpColStatsUsageToKV`'s sweep-and-reset phase.
    #[must_use]
    pub fn begin_column_stats_usage_dump(&self) -> ColumnStatsUsageDump<'_> {
        self.sweep_session_stats_list();
        ColumnStatsUsageDump {
            target: &self.stats_usage,
            usage: self.stats_usage.get_usage_and_reset(),
        }
    }

    /// Starts Go `DumpStatsDeltaToKV`'s sweep-and-reset phase.
    #[must_use]
    pub fn begin_table_delta_dump(&self) -> TableDeltaDump<'_> {
        self.sweep_session_stats_list();
        TableDeltaDump {
            target: &self.table_delta,
            delta: self.table_delta.get_delta_and_reset(),
        }
    }
}

impl Default for SessionStatsList {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `DumpColStatsUsageToKV`'s reset map and deferred merge.
pub struct ColumnStatsUsageDump<'a> {
    target: &'a StatsUsage,
    usage: HashMap<TableItemID, SystemTime>,
}

impl ColumnStatsUsageDump<'_> {
    /// The pending rows, sorted like Go `DumpColStatsUsageEntries`.
    #[must_use]
    pub fn entries(&self) -> Vec<(TableItemID, SystemTime)> {
        let mut entries = self
            .usage
            .iter()
            .map(|(item, time)| (*item, *time))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(item, _)| (item.table_id, item.id));
        entries
    }

    /// Removes a successfully persisted batch before the deferred merge.
    pub fn mark_persisted(&mut self, items: impl IntoIterator<Item = TableItemID>) {
        for item in items {
            self.usage.remove(&item);
        }
    }
}

impl Drop for ColumnStatsUsageDump<'_> {
    fn drop(&mut self) {
        self.target.merge(std::mem::take(&mut self.usage));
    }
}

/// Go `DumpStatsDeltaToKV`'s reset map and deferred merge.
pub struct TableDeltaDump<'a> {
    target: &'a TableDeltaMap,
    delta: HashMap<i64, TableDelta>,
}

impl TableDeltaDump<'_> {
    /// Go `collectPendingStatsDeltaTableIDs`.
    #[must_use]
    pub fn pending_table_ids(&self, target_table_ids: &[i64]) -> Vec<i64> {
        let mut table_ids = if target_table_ids.is_empty() {
            self.delta.keys().copied().collect::<Vec<_>>()
        } else {
            let mut seen = std::collections::HashSet::with_capacity(target_table_ids.len());
            target_table_ids
                .iter()
                .copied()
                .filter(|id| seen.insert(*id) && self.delta.contains_key(id))
                .collect()
        };
        table_ids.sort_unstable();
        table_ids
    }

    /// One pending table's delta.
    #[must_use]
    pub fn get(&self, table_id: i64) -> Option<TableDelta> {
        self.delta.get(&table_id).copied()
    }

    /// Initializes Go's zero `InitTime` at eligibility evaluation.
    pub fn initialize_time(&mut self, table_id: i64, now: SystemTime) {
        if let Some(item) = self.delta.get_mut(&table_id) {
            if item.init_time.is_none() {
                item.init_time = Some(now);
            }
        }
    }

    /// Removes one successfully persisted table before the deferred merge.
    pub fn mark_persisted(&mut self, table_id: i64) {
        self.delta.remove(&table_id);
    }
}

impl Drop for TableDeltaDump<'_> {
    fn drop(&mut self) {
        self.target.merge(std::mem::take(&mut self.delta));
    }
}

/// Go `statsUsageImpl.needDumpStatsDelta` after table/schema exclusions.
#[must_use]
pub fn need_dump_stats_delta(
    force_dump: bool,
    item: TableDelta,
    current_time: SystemTime,
    realtime_count: Option<i64>,
) -> bool {
    if force_dump {
        return true;
    }
    if item.init_time.is_some_and(|init_time| {
        current_time
            .duration_since(init_time)
            .is_ok_and(|elapsed| elapsed > dump_stats_max_duration())
    }) {
        return true;
    }
    realtime_count.is_none_or(|count| {
        count == 0 || item.count as f64 / count as f64 > dump_stats_delta_ratio()
    })
}

#[must_use]
fn dump_stats_delta_ratio() -> f64 {
    f64::from_bits(DUMP_STATS_DELTA_RATIO.load(Ordering::Relaxed))
}

#[cfg(test)]
fn set_dump_stats_delta_ratio(value: f64) {
    DUMP_STATS_DELTA_RATIO.store(value.to_bits(), Ordering::Relaxed);
}

#[must_use]
fn dump_stats_max_duration() -> Duration {
    *DUMP_STATS_MAX_DURATION
        .lock()
        .expect("dump stats duration lock poisoned")
}

#[cfg(test)]
fn set_dump_stats_max_duration(value: Duration) {
    *DUMP_STATS_MAX_DURATION
        .lock()
        .expect("dump stats duration lock poisoned") = value;
}

/// Go `statsUsageImpl`'s package-owned collectors.
pub struct StatsUsageHandle {
    index_usage: Arc<IndexUsageCollector>,
    sessions: SessionStatsList,
}

impl StatsUsageHandle {
    /// Go `NewStatsUsageImpl`'s collector construction.
    #[must_use]
    pub fn new() -> Self {
        Self {
            index_usage: Arc::new(IndexUsageCollector::new()),
            sessions: SessionStatsList::new(),
        }
    }

    /// Go `statsUsageImpl.NewSessionStatsItem`.
    #[must_use]
    pub fn new_session_stats_item(&self) -> Arc<SessionStatsItem> {
        self.sessions.new_session_stats_item()
    }

    /// Go `statsUsageImpl.NewSessionIndexUsageCollector`.
    #[must_use]
    pub fn new_session_index_usage_collector(
        &self,
    ) -> tidb_stats_handle_usage_indexusage::SessionIndexUsageCollector {
        self.index_usage.spawn_session_collector()
    }

    /// Go `statsUsageImpl.StartWorker`.
    pub fn start_worker(&self) {
        self.index_usage.start_worker();
    }

    /// Go `statsUsageImpl.Close`.
    pub fn close(&self) {
        self.index_usage.close();
    }

    /// Go `statsUsageImpl.GetIndexUsage`.
    #[must_use]
    pub fn get_index_usage(&self, table_id: i64, index_id: i64) -> Sample {
        self.index_usage.get_index_usage(table_id, index_id)
    }

    /// Go `statsUsageImpl.SessionStatsList`.
    #[must_use]
    pub const fn session_stats_list(&self) -> &SessionStatsList {
        &self.sessions
    }

    /// The index collector used by Go `GCIndexUsage` and information schema.
    #[must_use]
    pub fn index_usage_collector(&self) -> Arc<IndexUsageCollector> {
        Arc::clone(&self.index_usage)
    }

    /// Go `statsUsageImpl.GCIndexUsage` after its restricted-session schema lookup.
    pub fn gc_index_usage(
        &self,
        table_meta_lookup: impl Fn(i64) -> Option<Arc<tidb_model::TableInfo>>,
    ) {
        self.index_usage.gc_index_usage(table_meta_lookup);
    }
}

impl Drop for StatsUsageHandle {
    fn drop(&mut self) {
        self.index_usage.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_model::{IndexInfo, TableInfo};

    fn column(table_id: i64, column_id: i64) -> TableItemID {
        TableItemID {
            table_id,
            id: column_id,
            is_index: false,
            is_sync_load_failed: false,
        }
    }

    #[test]
    fn session_sweep_merges_deltas_and_latest_column_usage() {
        let sessions = SessionStatsList::new();
        let first = sessions.new_session_stats_item();
        let second = sessions.new_session_stats_item();
        first.update(7, 1, 2);
        second.update(7, -1, 3);

        let old = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let new = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        first.update_col_stats_usage([column(7, 1)], old);
        second.update_col_stats_usage([column(7, 1)], new);

        sessions.sweep_session_stats_list();
        assert_eq!(
            sessions.session_table_delta().get_delta_and_reset()[&7],
            TableDelta {
                delta: 0,
                count: 5,
                init_time: None,
            }
        );
        assert_eq!(
            sessions.session_stats_usage().get_usage_and_reset()[&column(7, 1)],
            new
        );
    }

    #[test]
    fn deleted_session_is_swept_once() {
        let sessions = SessionStatsList::new();
        let item = sessions.new_session_stats_item();
        item.update(9, 2, 2);
        item.delete();
        sessions.sweep_session_stats_list();
        assert_eq!(sessions.sessions.lock().unwrap().len(), 0);
        assert_eq!(
            sessions.session_table_delta().get_delta_and_reset()[&9].delta,
            2
        );
        sessions.sweep_session_stats_list();
        assert!(sessions
            .session_table_delta()
            .get_delta_and_reset()
            .is_empty());
    }

    #[test]
    fn failed_dump_merges_remaining_data_with_earliest_init_time() {
        let deltas = TableDeltaMap::new();
        let first = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let later = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        deltas.merge(HashMap::from([(
            11,
            TableDelta {
                delta: 1,
                count: 1,
                init_time: Some(first),
            },
        )]));
        let mut pending = TableDeltaDump {
            target: &deltas,
            delta: deltas.get_delta_and_reset(),
        };
        deltas.merge(HashMap::from([(
            11,
            TableDelta {
                delta: 1,
                count: 1,
                init_time: Some(later),
            },
        )]));
        pending.initialize_time(11, later);
        drop(pending);

        assert_eq!(
            deltas.get_delta_and_reset()[&11],
            TableDelta {
                delta: 2,
                count: 2,
                init_time: Some(first),
            }
        );
    }

    #[test]
    fn dump_eligibility_uses_ratio_and_elapsed_duration() {
        static CONFIG: Mutex<()> = Mutex::new(());
        let _guard = CONFIG.lock().unwrap();
        let old_ratio = dump_stats_delta_ratio();
        let old_duration = dump_stats_max_duration();
        set_dump_stats_delta_ratio(0.5);
        set_dump_stats_max_duration(Duration::from_millis(50));

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let recent = TableDelta {
            delta: 1,
            count: 1,
            init_time: Some(now - Duration::from_millis(10)),
        };
        assert!(!need_dump_stats_delta(false, recent, now, Some(10)));
        assert!(need_dump_stats_delta(false, recent, now, Some(1)));
        let old = TableDelta {
            init_time: Some(now - Duration::from_millis(51)),
            ..recent
        };
        assert!(need_dump_stats_delta(false, old, now, Some(10)));

        set_dump_stats_delta_ratio(old_ratio);
        set_dump_stats_max_duration(old_duration);
    }

    #[test]
    fn target_table_ids_are_deduplicated_filtered_and_sorted() {
        let deltas = TableDeltaMap::new();
        deltas.update(3, 1, 1);
        deltas.update(1, 1, 1);
        let pending = TableDeltaDump {
            target: &deltas,
            delta: deltas.get_delta_and_reset(),
        };
        assert_eq!(pending.pending_table_ids(&[3, 2, 3, 1]), vec![1, 3]);
    }

    #[test]
    fn partition_updates_follow_go_lock_and_global_rules() {
        let delta = TableDelta {
            delta: 1,
            count: 1,
            init_time: None,
        };
        let unlocked = prepare_delta_updates(
            vec![DeltaUpdate {
                table_id: 11,
                delta,
                is_locked: false,
            }],
            |id| (id == 11).then_some(10),
            &HashSet::new(),
        );
        assert_eq!(
            unlocked,
            vec![
                DeltaUpdate {
                    table_id: 10,
                    delta,
                    is_locked: false,
                },
                DeltaUpdate {
                    table_id: 11,
                    delta,
                    is_locked: false,
                },
            ]
        );
        let partition_locked = prepare_delta_updates(
            vec![DeltaUpdate {
                table_id: 11,
                delta,
                is_locked: false,
            }],
            |id| (id == 11).then_some(10),
            &HashSet::from([11]),
        );
        assert_eq!(
            partition_locked,
            vec![DeltaUpdate {
                table_id: 11,
                delta,
                is_locked: true,
            }]
        );

        // A partition added after LOCK STATS has no lock row yet. Go treats
        // the parent's logical lock as authoritative, so its first delta
        // creates the physical partition lock row instead of updating global
        // stats_meta. This is how a newly added partition inherits the lock.
        let parent_locked = prepare_delta_updates(
            vec![DeltaUpdate {
                table_id: 12,
                delta,
                is_locked: false,
            }],
            |id| (id == 12).then_some(10),
            &HashSet::from([10]),
        );
        assert_eq!(
            parent_locked,
            vec![DeltaUpdate {
                table_id: 12,
                delta,
                is_locked: true,
            }]
        );
    }

    #[test]
    fn source_index_usage_integration_test_gc_index_usage() {
        const TABLE_COUNT: i64 = 10;
        const INDEX_COUNT: i64 = 10;

        let usage = StatsUsageHandle::new();
        usage.start_worker();
        let mut session = usage.new_session_index_usage_collector();
        let mut tables = HashMap::new();
        for table_id in 0..TABLE_COUNT {
            let indices = (0..INDEX_COUNT)
                .map(|index_id| IndexInfo {
                    id: index_id,
                    ..IndexInfo::default()
                })
                .collect::<Vec<_>>()
                .into();
            tables.insert(
                table_id,
                Arc::new(TableInfo {
                    id: table_id,
                    indices,
                    ..TableInfo::default()
                }),
            );
            for index_id in 0..INDEX_COUNT {
                session.update(
                    table_id,
                    index_id,
                    tidb_stats_handle_usage_indexusage::new_sample(1, 2, 3, 4),
                );
            }
        }
        session.flush();
        usage.close();

        let verify = |table_limit: i64, index_limit: i64| {
            for table_id in 0..TABLE_COUNT {
                for index_id in 0..INDEX_COUNT {
                    let actual = usage.get_index_usage(table_id, index_id);
                    if table_id < table_limit && index_id < index_limit {
                        assert_eq!(actual.query_total, 1);
                        assert_eq!(actual.kv_req_total, 2);
                        assert_eq!(actual.row_access_total, 3);
                        assert_eq!(actual.percentage_access, [0, 0, 0, 0, 0, 1, 0]);
                    } else {
                        assert_eq!(actual, Sample::default());
                    }
                }
            }
        };

        verify(TABLE_COUNT, INDEX_COUNT);

        for table in tables.values_mut() {
            let retained = table
                .indices
                .iter_deref()
                .filter_map(|index| {
                    let index = index.read();
                    (index.id < 5).then(|| index.clone())
                })
                .collect::<Vec<_>>();
            Arc::make_mut(table).indices = retained.into();
        }
        usage.gc_index_usage(|table_id| tables.get(&table_id).cloned());
        verify(TABLE_COUNT, 5);

        tables.retain(|table_id, _| *table_id < 5);
        usage.gc_index_usage(|table_id| tables.get(&table_id).cloned());
        verify(5, 5);
    }
}
