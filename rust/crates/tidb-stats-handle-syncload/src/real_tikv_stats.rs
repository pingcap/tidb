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

//! The production statistics load: one real transaction, one PD timestamp, one
//! consistent view of a table's `mysql.stats_*` rows.
//!
//! `mysql.stats_meta`, `mysql.stats_histograms`, `mysql.stats_buckets`, and
//! `mysql.stats_top_n` are four separate tables that one `ANALYZE` writes in
//! one transaction. Reading them at four timestamps could therefore assemble a
//! histogram whose buckets came from one `ANALYZE` and whose TopN came from
//! the next — a combination that never existed and whose row counts would not
//! add up. Everything below is read at a single `start_ts`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::FieldType;
use tidb_model::table_info::TableInfo;
use tidb_stats_handle_cache::{StatsMetaRow, StatsRefreshSource, UpdateError};

use tidb_stats_handle_storage::cluster_stats_load::ClusterStatsItem;
use tidb_syssession::mysql_system_tables::SystemTableError;
use crate::stats_watch::{SharedStats, TableStatsState};

/// One cache-refresh target: the complete schema object Go passes through
/// `TableStatsFromStorage`, plus the derived bound-decoding map.
#[derive(Clone, Debug)]
pub struct StatsTarget {
    /// Logical table ID or physical partition ID whose statistics are read.
    pub physical_id: i64,
    /// The current parent-table schema metadata Go resolves for this physical ID.
    pub table: TableInfo,
    /// Declared column types used to decode stored histogram bounds.
    pub column_types: BTreeMap<i64, FieldType>,
}

impl StatsTarget {
    /// Expands one logical table into the IDs Go's statistics cache tracks:
    /// the logical/global ID and every physical partition ID.
    #[must_use]
    pub fn for_table(table: &TableInfo) -> Vec<Self> {
        let column_types = tidb_stats_handle_storage::cluster_stats_load::column_types_of(table);
        let mut targets = Vec::with_capacity(
            1 + table
                .partition
                .as_ref()
                .map_or(0, |partition| partition.read().definitions.len()),
        );
        targets.push(Self {
            physical_id: table.id,
            table: table.clone(),
            column_types: column_types.clone(),
        });
        if let Some(partition) = &table.partition {
            for definition in partition.read().definitions.snapshot() {
                targets.push(Self {
                    physical_id: definition.id,
                    table: table.clone(),
                    column_types: column_types.clone(),
                });
            }
        }
        targets
    }
}


/// Go `storage.LoadNeededHistograms`: drain the process-wide asynchronous
/// statistics demand against the latest schema and shared statistics cache.
/// Every item is removed after its one attempt, including storage failures.
pub fn load_needed_histograms(
    pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    tables: &[StatsTarget],
    shared: &SharedStats,
) -> Result<(), SystemTableError> {
    // Go `statsReadWriter.LoadNeededHistograms`: one session, one wrapped
    // transaction, for the whole pass.
    pool.call_with_sctx(
        &mut |sctx| load_needed_histograms_on(sctx, tables, shared),
        &[tidb_stats_handle_util::FLAG_WRAP_TXN],
    )
    .map_err(|error| SystemTableError::Snapshot(error.message))
}

fn load_needed_histograms_on(
    sctx: &dyn tidb_stats_handle_util::StatsSessionContext,
    tables: &[StatsTarget],
    shared: &SharedStats,
) -> Result<(), tidb_sqlexec::SqlExecError> {
    let needed = &tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
    for requested in needed.all_items() {
        let item = requested.table_item_id;
        let result: Result<(), tidb_sqlexec::SqlExecError> = (|| {
            if skips_internal_column(&item) {
                return Ok(());
            }
            let current = shared.load();
            let Some(current) = current
                .get(&item.table_id)
                .and_then(TableStatsState::loaded)
            else {
                return Ok(());
            };
            let Some(target) = tables
                .iter()
                .find(|target| target.physical_id == item.table_id)
            else {
                return Ok(());
            };
            if item.is_index {
                // Go `loadNeededIndexHistograms`: an index is always fully
                // loaded.
                if !current.index_load_needed(item.id).1
                    || !target
                        .table
                        .indices
                        .iter_deref()
                        .any(|index| index.read().id == item.id)
                {
                    return Ok(());
                }
                if let Some(loaded) =
                    read_stats_for_one_item(sctx, item.table_id, true, item.id, None, true)?
                {
                    shared.update_item(item.table_id, loaded, &target.table);
                }
                return Ok(());
            }
            let Some(column_type) = target.column_types.get(&item.id) else {
                return Ok(());
            };
            let (_, load_needed, analyzed) = current.column_load_needed(item.id, true);
            if !load_needed {
                return Ok(());
            }
            if !analyzed {
                let empty = ClusterStatsItem {
                    id: item.id,
                    is_index: false,
                    stats_ver: 0,
                    flag: 0,
                    load_status: tidb_stats::StatsLoadedStatus::default(),
                    histogram: tidb_stats::Histogram {
                        id: item.id,
                        ..tidb_stats::Histogram::default()
                    },
                    topn: None,
                    cms: None,
                    fm_sketch: None,
                };
                shared.update_item(item.table_id, empty, &target.table);
                return Ok(());
            }
            if let Some(loaded) = read_stats_for_one_item(
                sctx,
                item.table_id,
                false,
                item.id,
                Some(column_type),
                requested.full_load,
            )? {
                shared.update_item(item.table_id, loaded, &target.table);
            }
            Ok(())
        })();
        // Go: the item leaves the queue whether or not its load succeeded.
        needed.delete(item);
        result?;
    }
    Ok(())
}

fn skips_internal_column(item: &tidb_model::TableItemID) -> bool {
    !item.is_index && item.id <= 0
}

/// Go `statsSyncLoad.readStatsForOneItem` on a statistics session: the
/// item's histogram metadata, and on a full load its buckets, CM sketch and
/// TopN, at high priority. `None` is Go's `errGetHistMeta` (no row), which
/// the caller skips. `column_type` is the column's type for a column item.
///
/// # Errors
///
/// A read that failed.
pub fn read_stats_for_one_item<C: tidb_stats_handle_util::StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    is_index: bool,
    id: i64,
    column_type: Option<&FieldType>,
    full_load: bool,
) -> Result<Option<ClusterStatsItem>, tidb_sqlexec::SqlExecError> {
    use tidb_stats_handle_storage::read;
    let Some((mut histogram, stats_ver)) =
        read::hist_meta_from_storage_with_high_priority(sctx, table_id, is_index, id, column_type)?
    else {
        return Ok(None);
    };
    let (mut cms, mut topn) = (None, None);
    if full_load {
        let blob = FieldType::new(tidb_datatype::FieldTypeCode::Blob);
        let tp = if is_index { &blob } else { column_type.unwrap_or(&blob) };
        histogram = read::histogram_from_storage_with_priority(
            sctx,
            table_id,
            id,
            tp,
            histogram.ndv,
            is_index,
            histogram.last_update_version,
            histogram.null_count,
            histogram.tot_col_size,
            histogram.correlation,
            read::Priority::High,
        )?;
        (cms, topn) =
            read::cm_sketch_and_topn_from_storage_with_high_priority(sctx, table_id, is_index, id, stats_ver)?;
    }
    // Go: an index with a nonzero `stats_ver`, a column whose
    // `StatsAvailable()`, is loaded (fully or with its payload evicted).
    let available = if is_index {
        stats_ver != 0
    } else {
        stats_ver != 0 || histogram.ndv > 0 || histogram.null_count > 0
    };
    let load_status = match (available, full_load) {
        (false, _) => tidb_stats::StatsLoadedStatus::default(),
        (true, true) => tidb_stats::StatsLoadedStatus::full_load(),
        (true, false) => tidb_stats::StatsLoadedStatus::all_evicted(),
    };
    Ok(Some(ClusterStatsItem {
        id,
        is_index,
        stats_ver,
        flag: 0,
        load_status,
        histogram,
        topn,
        cms,
        fm_sketch: None,
    }))
}

/// Go `StatsCacheImpl.Update`'s storage reads on the statistics handle's
/// session pool (`util.CallWithSCtx(s.statsHandle.SPool(), ...)`): the
/// ordered `mysql.stats_meta` SELECT, and `statsReadWriter.TableStatsFromStorage`
/// over `storage/read.rs`.
struct SessionStatsRefreshSource<'a> {
    pool: &'a dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    targets: BTreeMap<i64, &'a StatsTarget>,
    shared: &'a SharedStats,
    lease: Duration,
}

impl StatsRefreshSource for SessionStatsRefreshSource<'_> {
    type Error = SystemTableError;

    fn lease(&self) -> Duration {
        self.lease
    }

    fn stats_meta_rows(
        &self,
        after_version: u64,
        physical_ids: &[i64],
    ) -> Result<Vec<StatsMetaRow>, Self::Error> {
        let mut query = "SELECT version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta where version > %? ".to_owned();
        // Go converts the table IDs to strings: the SQL executor only accepts
        // string arrays for IN clauses.
        let ids: Vec<String> = physical_ids.iter().map(i64::to_string).collect();
        let id_bytes: Vec<&[u8]> = ids.iter().map(|id| id.as_bytes()).collect();
        let mut args = vec![tidb_sqlescape::SqlArg::Unsigned(after_version)];
        if !physical_ids.is_empty() {
            query.push_str("and table_id in (%?) ");
            args.push(tidb_sqlescape::SqlArg::Strings(&id_bytes));
        }
        query.push_str("order by version");
        let mut rows = Vec::new();
        self.pool
            .call_with_sctx(
                &mut |sctx| {
                    (rows, _) = tidb_stats_handle_util::exec_rows(sctx, &query, &args)?;
                    Ok(())
                },
                &[],
            )
            .map_err(|error| SystemTableError::Snapshot(error.message))?;
        // Go `row.GetInt64`/`row.GetUint64`: one eight-byte slot either way.
        let int = |row: &[tidb_datatype::Datum], index: usize| match row.get(index) {
            Some(tidb_datatype::Datum::Int(value)) => *value,
            Some(tidb_datatype::Datum::UInt(value)) => *value as i64,
            _ => 0,
        };
        Ok(rows
            .iter()
            .map(|row| StatsMetaRow {
                version: int(row, 0) as u64,
                physical_id: int(row, 1),
                modify_count: int(row, 2),
                count: int(row, 3),
                snapshot: int(row, 4) as u64,
                latest_histogram_version: int(row, 5) as u64,
            })
            .collect())
    }

    fn table_info_update_ts(&self, physical_id: i64) -> Option<u64> {
        self.targets
            .get(&physical_id)
            .map(|target| target.table.update_ts)
    }

    fn table_stats_from_storage(
        &self,
        physical_id: i64,
    ) -> Result<Option<Arc<tidb_stats::Table>>, Self::Error> {
        let Some(target) = self.targets.get(&physical_id) else {
            return Ok(None);
        };
        // Go `statsReadWriter.TableStatsFromStorage`: the cached table
        // (`statsHandler.Get`), a cached `PseudoTable` included, under the
        // handle's lease, in one wrapped transaction.
        let cached = self.shared.get(physical_id);
        let mut loaded = None;
        self.pool
            .call_with_sctx(
                &mut |sctx| {
                    loaded = tidb_stats_handle_storage::read::table_stats_from_storage(
                        sctx,
                        0,
                        &target.table,
                        physical_id,
                        false,
                        self.lease,
                        cached.as_deref(),
                    )?;
                    Ok(())
                },
                &[tidb_stats_handle_util::FLAG_WRAP_TXN],
            )
            .map_err(|error| SystemTableError::Snapshot(error.message))?;
        Ok(loaded.map(Arc::new))
    }
}

/// Go `StatsCacheImpl.Update` on the statistics handle's session pool. The
/// cache's five-lease watermark, ordered rows, per-table version/schema skip,
/// metadata-only reuse, and monotonic maximum all come from the shared
/// `tidb-stats-handle-cache` implementation.
pub fn update_stats_cache(
    pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    tables: &[StatsTarget],
    shared: &SharedStats,
    lease: Duration,
    physical_ids: Vec<i64>,
) -> Result<bool, SystemTableError> {
    let targets = tables
        .iter()
        .map(|target| (target.physical_id, target))
        .collect();
    let tracked_ids = tables
        .iter()
        .map(|target| target.physical_id)
        .collect::<Vec<_>>();
    let source = SessionStatsRefreshSource {
        pool,
        targets,
        shared,
        lease,
    };
    shared
        .update_from_source(&source, physical_ids, &tracked_ids, || false)
        .map_err(|error| match error {
            UpdateError::Source(error) => error,
            UpdateError::Cancelled => {
                SystemTableError::Snapshot("statistics update was cancelled".to_owned())
            }
        })
}

#[cfg(test)]
mod tests {
    use tidb_model::go_runtime::GoShared;
    use tidb_model::partition::{PartitionDefinition, PartitionInfo};

    use super::*;

    #[test]
    fn partition_stats_target_keeps_parent_table_info_like_go() {
        let table = TableInfo {
            id: 10,
            update_ts: 99,
            partition: Some(GoShared::new(PartitionInfo {
                definitions: vec![PartitionDefinition {
                    id: 11,
                    ..PartitionDefinition::default()
                }]
                .into(),
                ..PartitionInfo::default()
            })),
            ..TableInfo::default()
        };

        let targets = StatsTarget::for_table(&table);
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].physical_id, 10);
        assert_eq!(targets[0].table.id, 10);
        assert_eq!(targets[1].physical_id, 11);
        assert_eq!(targets[1].table.id, 10);
        assert_eq!(targets[1].table.update_ts, 99);
    }

    #[test]
    fn needed_histogram_loading_skips_only_internal_columns() {
        let item = |id, is_index| tidb_model::TableItemID {
            table_id: 10,
            id,
            is_index,
            is_sync_load_failed: false,
        };
        assert!(skips_internal_column(&item(-1, false)));
        assert!(skips_internal_column(&item(0, false)));
        assert!(!skips_internal_column(&item(1, false)));
        assert!(!skips_internal_column(&item(-1, true)));
    }
}
