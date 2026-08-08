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

//! Aggregate `HistColl` and `Table` ownership from `pkg/statistics/table.go`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    CmsSketch, ColAndIdxExistenceMap, Column, ColumnInfo, ColumnMemUsage, FmSketch, Histogram,
    Index, IndexInfo, IndexMemUsage, StatsLoadedStatus, TopN,
};

pub const PSEUDO_VERSION: u64 = 0;
pub const PSEUDO_ROW_COUNT: i64 = 10_000;

pub type SharedColumn = Arc<RwLock<Column>>;
pub type SharedIndex = Arc<RwLock<Index>>;

/// Per-table component memory accounting from `table.go`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableMemoryUsage {
    pub columns_mem_usage: HashMap<i64, ColumnMemUsage>,
    pub indices_mem_usage: HashMap<i64, IndexMemUsage>,
    pub table_id: i64,
    pub total_mem_usage: i64,
}

impl TableMemoryUsage {
    #[must_use]
    pub fn total_index_tracking_mem_usage(&self) -> i64 {
        self.indices_mem_usage.values().fold(0_i64, |sum, usage| {
            sum.wrapping_add(usage.tracking_mem_usage())
        })
    }

    #[must_use]
    pub fn total_column_tracking_mem_usage(&self) -> i64 {
        self.columns_mem_usage.values().fold(0_i64, |sum, usage| {
            sum.wrapping_add(usage.tracking_mem_usage())
        })
    }

    #[must_use]
    pub fn total_tracking_mem_usage(&self) -> i64 {
        self.total_index_tracking_mem_usage()
            .wrapping_add(self.total_column_tracking_mem_usage())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct OwnedStatsInfo {
    histogram: Histogram,
    cmsketch: Option<CmsSketch>,
    top_n: Option<TopN>,
    fm_sketch: Option<FmSketch>,
}

/// The successful result of Go `GetStatsInfo`. Shared variants retain the
/// source cache item; `Owned` is the independent `needCopy` result.
#[derive(Clone, Debug)]
pub enum StatsInfo {
    SharedColumn(SharedColumn),
    SharedIndex(SharedIndex),
    Owned(Arc<RwLock<OwnedStatsInfo>>),
}

impl StatsInfo {
    pub fn with_components<R>(
        &self,
        visit: impl FnOnce(&Histogram, Option<&CmsSketch>, Option<&TopN>, Option<&FmSketch>) -> R,
    ) -> R {
        match self {
            Self::SharedColumn(column) => {
                let column = read(column);
                visit(
                    &column.histogram,
                    column.cmsketch.as_ref(),
                    column.top_n.as_ref(),
                    column.fm_sketch.as_ref(),
                )
            }
            Self::SharedIndex(index) => {
                let index = read(index);
                visit(
                    &index.histogram,
                    index.cmsketch.as_ref(),
                    index.top_n.as_ref(),
                    index.fm_sketch.as_ref(),
                )
            }
            Self::Owned(info) => {
                let info = read(info);
                visit(
                    &info.histogram,
                    info.cmsketch.as_ref(),
                    info.top_n.as_ref(),
                    info.fm_sketch.as_ref(),
                )
            }
        }
    }

    pub fn with_components_mut<R>(
        &self,
        visit: impl FnOnce(
            &mut Histogram,
            &mut Option<CmsSketch>,
            &mut Option<TopN>,
            &mut Option<FmSketch>,
        ) -> R,
    ) -> R {
        match self {
            Self::SharedColumn(column) => {
                let mut column = write(column);
                let Column {
                    histogram,
                    cmsketch,
                    top_n,
                    fm_sketch,
                    ..
                } = &mut *column;
                visit(histogram, cmsketch, top_n, fm_sketch)
            }
            Self::SharedIndex(index) => {
                let mut index = write(index);
                let Index {
                    histogram,
                    cmsketch,
                    top_n,
                    fm_sketch,
                    ..
                } = &mut *index;
                visit(histogram, cmsketch, top_n, fm_sketch)
            }
            Self::Owned(info) => {
                let mut info = write(info);
                let OwnedStatsInfo {
                    histogram,
                    cmsketch,
                    top_n,
                    fm_sketch,
                } = &mut *info;
                visit(histogram, cmsketch, top_n, fm_sketch)
            }
        }
    }
}

fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyIntent {
    MetaOnly,
    ColumnMapWritable,
    IndexMapWritable,
    BothMapsWritable,
    AllDataWritable,
}

/// Schema inputs consumed by Go `PseudoTable`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PseudoColumnInfo {
    pub info: ColumnInfo,
    pub public: bool,
    pub hidden: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PseudoIndexInfo {
    pub info: IndexInfo,
    pub public: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PseudoTableInfo {
    pub id: i64,
    pub pk_is_handle: bool,
    pub columns: Vec<PseudoColumnInfo>,
    pub indices: Vec<PseudoIndexInfo>,
}

/// Planner column identity consumed by `ID2UniqueID` and
/// `GenerateHistCollFromColumnInfo`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryColumn {
    pub id: i64,
    pub unique_id: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryIndexInfo {
    pub id: i64,
    pub column_offsets: Vec<usize>,
    pub mv_index: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryTableInfo {
    /// Metadata column IDs in offset order.
    pub column_ids: Vec<i64>,
    pub indices: Vec<QueryIndexInfo>,
}

#[derive(Clone, Debug)]
pub struct HistColl {
    columns: Arc<RwLock<HashMap<i64, SharedColumn>>>,
    indices: Arc<RwLock<HashMap<i64, SharedIndex>>>,
    pub physical_id: i64,
    pub realtime_count: i64,
    pub modify_count: i64,
    pub stats_version: i32,
    pub pseudo: bool,
    pub cannot_trigger_load: bool,
    pub idx_to_col_unique_ids: HashMap<i64, Vec<i64>>,
    pub col_unique_id_to_idx_ids: HashMap<i64, Vec<i64>>,
    pub unique_id_to_col_info_id: HashMap<i64, i64>,
    pub mv_idx_to_columns: HashMap<i64, Vec<i64>>,
}

impl HistColl {
    #[must_use]
    pub fn new(
        physical_id: i64,
        realtime_count: i64,
        modify_count: i64,
        column_capacity: usize,
        index_capacity: usize,
    ) -> Self {
        Self {
            columns: Arc::new(RwLock::new(HashMap::with_capacity(column_capacity))),
            indices: Arc::new(RwLock::new(HashMap::with_capacity(index_capacity))),
            physical_id,
            realtime_count,
            modify_count,
            stats_version: 0,
            pseudo: false,
            cannot_trigger_load: false,
            idx_to_col_unique_ids: HashMap::new(),
            col_unique_id_to_idx_ids: HashMap::new(),
            unique_id_to_col_info_id: HashMap::new(),
            mv_idx_to_columns: HashMap::new(),
        }
    }

    pub fn set_column(&self, id: i64, column: Column) {
        write(&self.columns).insert(id, Arc::new(RwLock::new(column)));
    }

    pub fn set_index(&self, id: i64, index: Index) {
        write(&self.indices).insert(id, Arc::new(RwLock::new(index)));
    }

    #[must_use]
    pub fn get_column(&self, id: i64) -> Option<SharedColumn> {
        read(&self.columns).get(&id).cloned()
    }

    #[must_use]
    pub fn get_index(&self, id: i64) -> Option<SharedIndex> {
        read(&self.indices).get(&id).cloned()
    }

    #[must_use]
    pub fn column_count(&self) -> usize {
        read(&self.columns).len()
    }

    #[must_use]
    pub fn index_count(&self) -> usize {
        read(&self.indices).len()
    }

    pub fn for_each_column(&self, mut visit: impl FnMut(i64, &Column) -> bool) {
        for (id, column) in read(&self.columns).iter() {
            if visit(*id, &read(column)) {
                return;
            }
        }
    }

    pub fn for_each_index(&self, mut visit: impl FnMut(i64, &Index) -> bool) {
        for (id, index) in read(&self.indices).iter() {
            if visit(*id, &read(index)) {
                return;
            }
        }
    }

    #[must_use]
    pub fn stable_columns(&self) -> Vec<SharedColumn> {
        let mut columns: Vec<_> = read(&self.columns).values().cloned().collect();
        columns.sort_by_key(|column| read(column).histogram.id);
        columns
    }

    #[must_use]
    pub fn stable_indices(&self) -> Vec<SharedIndex> {
        let mut indices: Vec<_> = read(&self.indices).values().cloned().collect();
        indices.sort_by_key(|index| read(index).histogram.id);
        indices
    }

    pub fn set_all_indices_full_load_for_bootstrap(&self) {
        for index in read(&self.indices).values() {
            write(index).stats_loaded_status = StatsLoadedStatus::full_load();
        }
    }

    pub fn calculate_pre_scalar_counts(&self) {
        for index in read(&self.indices).values() {
            let mut index = write(index);
            for position in 1..index.histogram.buckets.len() {
                let previous = index.histogram.buckets[position - 1].count;
                index.histogram.buckets[position].count = index.histogram.buckets[position]
                    .count
                    .wrapping_add(previous);
            }
        }
        for column in read(&self.columns).values() {
            let mut column = write(column);
            for position in 1..column.histogram.buckets.len() {
                let previous = column.histogram.buckets[position - 1].count;
                column.histogram.buckets[position].count = column.histogram.buckets[position]
                    .count
                    .wrapping_add(previous);
            }
        }
    }

    pub fn drop_evicted(&self) {
        for column in read(&self.columns).values() {
            let mut column = write(column);
            if column.is_stats_initialized() && !column.is_all_evicted() {
                column.drop_unnecessary_data();
            }
        }
        for index in read(&self.indices).values() {
            let mut index = write(index);
            if index.stats_loaded_status.stats_initialized() && !index.is_all_evicted() {
                index.drop_unnecessary_data();
            }
        }
    }

    /// Stable column-first, index-second source order.
    #[must_use]
    pub fn analyze_row_count(&self) -> f64 {
        for column in self.stable_columns() {
            let column = read(&column);
            if column.is_full_load() {
                return column.total_row_count();
            }
        }
        for index in self.stable_indices() {
            let index = read(&index);
            if index.info.as_ref().is_some_and(|info| info.mv_index) {
                continue;
            }
            if index.is_full_load() {
                return index.total_row_count();
            }
        }
        -1.0
    }

    #[must_use]
    pub fn scaled_realtime_and_modify_count(&self, index: Option<&Index>) -> (i64, i64) {
        let Some(index) = index else {
            return (self.realtime_count, self.modify_count);
        };
        if !index.info.as_ref().is_some_and(|info| info.mv_index) || !index.is_full_load() {
            return (self.realtime_count, self.modify_count);
        }
        let analyzed = self.analyze_row_count();
        let index_total = index.total_row_count();
        if analyzed <= 0.0 || index_total <= 0.0 {
            return (self.realtime_count, self.modify_count);
        }
        let scale = index_total / analyzed;
        (
            (self.realtime_count as f64 * scale) as i64,
            (self.modify_count as f64 * scale) as i64,
        )
    }

    /// Go `ID2UniqueID`. Statistics payloads remain shared; only the column
    /// map and its keys are rebuilt.
    #[must_use]
    pub fn id_to_unique_id(&self, columns: &[QueryColumn]) -> Self {
        let source_columns = read(&self.columns);
        let mapped = columns
            .iter()
            .filter_map(|column| {
                source_columns
                    .get(&column.id)
                    .map(|stats| (column.unique_id, Arc::clone(stats)))
            })
            .collect();
        let mut result = Self::new(
            self.physical_id,
            self.realtime_count,
            self.modify_count,
            0,
            0,
        );
        result.pseudo = self.pseudo;
        result.columns = Arc::new(RwLock::new(mapped));
        result
    }

    /// Go `GenerateHistCollFromColumnInfo`. `prepare_mv_columns` is the
    /// planner-owned `PrepareCols4MVIndex` callback reduced to the unique IDs
    /// retained by this crate's query map.
    #[must_use]
    pub fn generate_from_column_info(
        &self,
        table_info: &QueryTableInfo,
        columns: &[QueryColumn],
        mut prepare_mv_columns: impl FnMut(&QueryIndexInfo, &[QueryColumn]) -> Option<Vec<i64>>,
    ) -> Self {
        let id_to_column: HashMap<_, _> =
            columns.iter().map(|column| (column.id, *column)).collect();
        let id_to_unique: HashMap<_, _> = columns
            .iter()
            .map(|column| (column.id, column.unique_id))
            .collect();
        let unique_to_id = columns
            .iter()
            .map(|column| (column.unique_id, column.id))
            .collect();

        let source_columns = read(&self.columns);
        let mapped_columns = source_columns
            .iter()
            .filter_map(|(id, stats)| {
                id_to_unique
                    .get(id)
                    .map(|unique_id| (*unique_id, Arc::clone(stats)))
            })
            .collect();
        drop(source_columns);

        let index_info: HashMap<_, _> = table_info
            .indices
            .iter()
            .map(|index| (index.id, index))
            .collect();
        let mut mapped_indices = HashMap::new();
        let mut index_to_columns = HashMap::new();
        let mut column_to_indices: HashMap<i64, Vec<i64>> = HashMap::new();
        let mut mv_index_to_columns = HashMap::new();
        for (id, stats) in read(&self.indices).iter() {
            let Some(info) = index_info.get(id) else {
                continue;
            };
            let mut unique_ids = Vec::with_capacity(info.column_offsets.len());
            for offset in &info.column_offsets {
                let Some(column_id) = table_info.column_ids.get(*offset) else {
                    break;
                };
                let Some(unique_id) = id_to_unique.get(column_id) else {
                    break;
                };
                unique_ids.push(*unique_id);
            }
            if unique_ids.is_empty() {
                continue;
            }
            let stats_id = read(stats).histogram.id;
            column_to_indices
                .entry(unique_ids[0])
                .or_default()
                .push(stats_id);
            mapped_indices.insert(stats_id, Arc::clone(stats));
            index_to_columns.insert(stats_id, unique_ids);
            if info.mv_index {
                let planner_columns: Vec<_> = id_to_column.values().copied().collect();
                if let Some(prepared) = prepare_mv_columns(info, &planner_columns) {
                    mv_index_to_columns.insert(*id, prepared);
                }
            }
        }
        for indices in column_to_indices.values_mut() {
            indices.sort_unstable();
        }

        let mut result = Self::new(
            self.physical_id,
            self.realtime_count,
            self.modify_count,
            0,
            0,
        );
        result.pseudo = self.pseudo;
        result.columns = Arc::new(RwLock::new(mapped_columns));
        result.indices = Arc::new(RwLock::new(mapped_indices));
        result.col_unique_id_to_idx_ids = column_to_indices;
        result.idx_to_col_unique_ids = index_to_columns;
        result.unique_id_to_col_info_id = unique_to_id;
        result.mv_idx_to_columns = mv_index_to_columns;
        result
    }

    fn shallow_clone_columns(&self) -> Arc<RwLock<HashMap<i64, SharedColumn>>> {
        Arc::new(RwLock::new(read(&self.columns).clone()))
    }

    fn shallow_clone_indices(&self) -> Arc<RwLock<HashMap<i64, SharedIndex>>> {
        Arc::new(RwLock::new(read(&self.indices).clone()))
    }

    fn deep_clone_columns(&self) -> Arc<RwLock<HashMap<i64, SharedColumn>>> {
        Arc::new(RwLock::new(
            read(&self.columns)
                .iter()
                .map(|(id, column)| (*id, Arc::new(RwLock::new(read(column).copy()))))
                .collect(),
        ))
    }

    fn deep_clone_indices(&self) -> Arc<RwLock<HashMap<i64, SharedIndex>>> {
        Arc::new(RwLock::new(
            read(&self.indices)
                .iter()
                .map(|(id, index)| (*id, Arc::new(RwLock::new(read(index).copy()))))
                .collect(),
        ))
    }
}

/// Go `PseudoHistColl`.
#[must_use]
pub fn pseudo_hist_coll(physical_id: i64, allow_trigger_loading: bool) -> HistColl {
    let mut coll = HistColl::new(physical_id, PSEUDO_ROW_COUNT, 0, 0, 0);
    coll.pseudo = true;
    coll.cannot_trigger_load = !allow_trigger_loading;
    coll
}

#[derive(Clone, Debug)]
pub struct Table {
    pub existence_map: Option<Arc<RwLock<ColAndIdxExistenceMap>>>,
    pub hist_coll: HistColl,
    pub version: u64,
    pub last_analyze_version: u64,
    pub last_stats_hist_version: u64,
    pub table_info_update_ts: u64,
    pub is_pk_handle: bool,
}

impl Table {
    /// Go `(*Table).MemoryUsage`; only column and index statistics payloads
    /// contribute, while table metadata is intentionally excluded.
    #[must_use]
    pub fn memory_usage(&self) -> TableMemoryUsage {
        let mut result = TableMemoryUsage {
            table_id: self.hist_coll.physical_id,
            ..TableMemoryUsage::default()
        };
        for column in read(&self.hist_coll.columns).values() {
            let usage = read(column).memory_usage();
            result.total_mem_usage = result
                .total_mem_usage
                .wrapping_add(usage.total_memory_usage());
            result.columns_mem_usage.insert(usage.item_id(), usage);
        }
        for index in read(&self.hist_coll.indices).values() {
            let usage = read(index).memory_usage();
            result.total_mem_usage = result
                .total_mem_usage
                .wrapping_add(usage.total_memory_usage());
            result.indices_mem_usage.insert(usage.item_id(), usage);
        }
        result
    }

    pub fn delete_column(&self, id: i64) {
        write(&self.hist_coll.columns).remove(&id);
        if let Some(map) = &self.existence_map {
            write(map).delete_column_not_found(id);
        }
    }

    pub fn delete_index(&self, id: i64) {
        write(&self.hist_coll.indices).remove(&id);
        if let Some(map) = &self.existence_map {
            write(map).delete_index_not_found(id);
        }
    }

    #[must_use]
    pub fn copy_as(&self, intent: CopyIntent) -> Self {
        let (columns, indices) = match intent {
            CopyIntent::MetaOnly => (
                Arc::clone(&self.hist_coll.columns),
                Arc::clone(&self.hist_coll.indices),
            ),
            CopyIntent::ColumnMapWritable => (
                self.hist_coll.shallow_clone_columns(),
                Arc::clone(&self.hist_coll.indices),
            ),
            CopyIntent::IndexMapWritable => (
                Arc::clone(&self.hist_coll.columns),
                self.hist_coll.shallow_clone_indices(),
            ),
            CopyIntent::BothMapsWritable => (
                self.hist_coll.shallow_clone_columns(),
                self.hist_coll.shallow_clone_indices(),
            ),
            CopyIntent::AllDataWritable => (
                self.hist_coll.deep_clone_columns(),
                self.hist_coll.deep_clone_indices(),
            ),
        };
        let existence_map = match intent {
            CopyIntent::MetaOnly => self.existence_map.clone(),
            _ => self
                .existence_map
                .as_ref()
                .map(|map| Arc::new(RwLock::new(read(map).deep_clone()))),
        };
        Self {
            existence_map,
            hist_coll: HistColl {
                columns,
                indices,
                physical_id: self.hist_coll.physical_id,
                realtime_count: self.hist_coll.realtime_count,
                modify_count: self.hist_coll.modify_count,
                stats_version: self.hist_coll.stats_version,
                pseudo: self.hist_coll.pseudo,
                cannot_trigger_load: false,
                idx_to_col_unique_ids: HashMap::new(),
                col_unique_id_to_idx_ids: HashMap::new(),
                unique_id_to_col_info_id: HashMap::new(),
                mv_idx_to_columns: HashMap::new(),
            },
            version: self.version,
            last_analyze_version: self.last_analyze_version,
            last_stats_hist_version: self.last_stats_hist_version,
            table_info_update_ts: self.table_info_update_ts,
            is_pk_handle: false,
        }
    }

    #[must_use]
    pub const fn is_analyzed(&self) -> bool {
        self.last_analyze_version > 0
    }

    #[must_use]
    pub fn meets_auto_analyze_min_count(&self, threshold: i64) -> bool {
        self.hist_coll.realtime_count >= threshold
    }

    #[must_use]
    pub fn is_eligible_for_analysis(&self, threshold: i64) -> bool {
        self.meets_auto_analyze_min_count(threshold) && !self.hist_coll.pseudo
    }

    #[must_use]
    pub fn stats_healthy(&self) -> (i64, bool) {
        if self.hist_coll.pseudo {
            return (0, false);
        }
        if !self.is_analyzed() {
            return (0, true);
        }
        let analyzed = self.hist_coll.analyze_row_count();
        let count = if analyzed > 0.0 {
            analyzed
        } else {
            self.hist_coll.realtime_count as f64
        };
        let healthy = if (self.hist_coll.modify_count as f64) < count {
            ((1.0 - self.hist_coll.modify_count as f64 / count) * 100.0) as i64
        } else if self.hist_coll.modify_count == 0 {
            100
        } else {
            0
        };
        (healthy, true)
    }

    #[must_use]
    pub fn column_load_needed(
        &self,
        id: i64,
        full_load: bool,
    ) -> (Option<SharedColumn>, bool, bool) {
        if self.hist_coll.pseudo {
            return (None, false, false);
        }
        let map = self
            .existence_map
            .as_ref()
            .expect("table has no existence map");
        let map = read(map);
        let analyzed = map.has_analyzed(id, false);
        let column = self.hist_coll.get_column(id);
        let Some(column) = column else {
            return (None, map.has(id, false), analyzed);
        };
        if !analyzed {
            return (None, false, false);
        }
        let needed = {
            let value = read(&column);
            (full_load && !value.is_full_load()) || (!full_load && !value.is_stats_initialized())
        };
        (Some(column), needed, true)
    }

    #[must_use]
    pub fn index_load_needed(&self, id: i64) -> (Option<SharedIndex>, bool) {
        let index = self.hist_coll.get_index(id);
        let map = self
            .existence_map
            .as_ref()
            .expect("table has no existence map");
        if index.is_none() && read(map).has_analyzed(id, true) {
            return (None, true);
        }
        let needed = index.as_ref().is_some_and(|index| {
            let index = read(index);
            index.is_analyzed() && !index.is_full_load()
        });
        (index, needed)
    }

    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.hist_coll
            .stable_columns()
            .iter()
            .any(|column| read(column).is_stats_initialized())
            || self
                .hist_coll
                .stable_indices()
                .iter()
                .any(|index| read(index).stats_loaded_status.stats_initialized())
    }

    #[must_use]
    pub fn is_outdated(&self, ratio: f64) -> bool {
        let analyzed = self.hist_coll.analyze_row_count();
        let row_count = if analyzed < 0.0 {
            self.hist_coll.realtime_count as f64
        } else {
            analyzed
        };
        row_count > 0.0 && self.hist_coll.modify_count as f64 / row_count > ratio
    }

    /// Go `GetStatsInfo`. `need_copy == false` returns an alias to the cache
    /// item; `true` deep-copies every returned component.
    #[must_use]
    pub fn stats_info(&self, id: i64, is_index: bool, need_copy: bool) -> Option<StatsInfo> {
        if is_index {
            let index = self.hist_coll.get_index(id)?;
            if !need_copy {
                return Some(StatsInfo::SharedIndex(index));
            }
            let index = read(&index);
            return Some(StatsInfo::Owned(Arc::new(RwLock::new(OwnedStatsInfo {
                histogram: index.histogram.clone(),
                cmsketch: index.cmsketch.clone(),
                top_n: index.top_n.clone(),
                fm_sketch: index.fm_sketch.clone(),
            }))));
        }
        let column = self.hist_coll.get_column(id)?;
        if !need_copy {
            return Some(StatsInfo::SharedColumn(column));
        }
        let column = read(&column);
        Some(StatsInfo::Owned(Arc::new(RwLock::new(OwnedStatsInfo {
            histogram: column.histogram.clone(),
            cmsketch: column.cmsketch.clone(),
            top_n: column.top_n.clone(),
            fm_sketch: column.fm_sketch.clone(),
        }))))
    }

    #[must_use]
    pub fn index_starting_with_column(&self, name: &str) -> Option<SharedIndex> {
        read(&self.hist_coll.indices).values().find_map(|shared| {
            let index = read(shared);
            let info = index.info.as_ref().expect("index has no metadata");
            (info.columns[0] == name).then(|| Arc::clone(shared))
        })
    }

    #[must_use]
    pub fn column_by_name(&self, name: &str) -> Option<SharedColumn> {
        read(&self.hist_coll.columns).values().find_map(|shared| {
            let column = read(shared);
            let info = column.info.as_ref().expect("column has no metadata");
            (info.name == name).then(|| Arc::clone(shared))
        })
    }
}

/// Go `PseudoTable`, including public/hidden schema filtering and the option
/// to omit histogram metadata while retaining the existence map.
#[must_use]
pub fn pseudo_table(
    table_info: &PseudoTableInfo,
    allow_trigger_loading: bool,
    allow_fill_hist_meta: bool,
) -> Table {
    let coll = pseudo_hist_coll(table_info.id, allow_trigger_loading);
    let mut existence =
        ColAndIdxExistenceMap::new(table_info.columns.len(), table_info.indices.len());
    for column in &table_info.columns {
        if !column.public || column.hidden {
            continue;
        }
        existence.insert_column(column.info.id, false);
        if allow_fill_hist_meta {
            coll.set_column(
                column.info.id,
                Column {
                    physical_id: table_info.id,
                    info: Some(column.info.clone()),
                    is_handle: table_info.pk_is_handle && column.info.primary_key,
                    histogram: Histogram {
                        id: column.info.id,
                        ..Histogram::default()
                    },
                    ..Column::default()
                },
            );
        }
    }
    for index in &table_info.indices {
        if !index.public {
            continue;
        }
        existence.insert_index(index.info.id, false);
        if allow_fill_hist_meta {
            coll.set_index(
                index.info.id,
                Index {
                    physical_id: table_info.id,
                    info: Some(index.info.clone()),
                    histogram: Histogram {
                        id: index.info.id,
                        ..Histogram::default()
                    },
                    ..Index::default()
                },
            );
        }
    }
    Table {
        existence_map: Some(Arc::new(RwLock::new(existence))),
        hist_coll: coll,
        version: PSEUDO_VERSION,
        last_analyze_version: 0,
        last_stats_hist_version: 0,
        table_info_update_ts: 0,
        is_pk_handle: false,
    }
}
