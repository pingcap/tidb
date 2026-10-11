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

//! Go's statistics storage writes (`statistics/handle/storage/save.go`) and
//! analyze job rows (`statistics/handle/autoanalyze`), issued as the internal
//! SQL Go runs against the `mysql` tables. The planner keeps reading this
//! tier's in-memory statistics; these rows are what `mysql.stats_*` and
//! `mysql.analyze_jobs` readers see.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_exec::cluster_stats_load::{
    histogram_row_item, ClusterStatsItem, ClusterTableStats, StatsTableSchema,
};
use tidb_executor::{DriverError, KvTable, SchemaErrorKind, TableEntry};
use tidb_util::sqlescape::{must_escape_sql_text, SqlArg};

use crate::{Session, StmtOutput};

/// One analyze task's `mysql.analyze_jobs` identity (Go `AnalyzeJob`).
#[derive(Clone, Debug)]
pub(crate) struct AnalyzeJob {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) partition: String,
    pub(crate) job_info: String,
}

/// Go `statistics.AnalyzePending` and its siblings.
const ANALYZE_PENDING: &str = "pending";
const ANALYZE_RUNNING: &str = "running";
const ANALYZE_FINISHED: &str = "finished";
const ANALYZE_FAILED: &str = "failed";

/// Go `types.TimeFormat` of the UTC wall clock.
fn utc_now_text() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn storage_error(error: impl std::fmt::Display) -> DriverError {
    DriverError::unsupported(format!("statistics storage write failed: {error}"))
}

fn datum_u64(value: Option<&Datum>) -> u64 {
    match value {
        Some(Datum::UInt(value)) => *value,
        Some(Datum::Int(value)) => *value as u64,
        _ => 0,
    }
}

impl Session {
    /// Go `insertAnalyzeJob` then `startAnalyzeJob` then `finishAnalyzeJob`
    /// for one table task: the job row as Go leaves it, `finished` with the
    /// rows the task processed, or `failed` with its reason. Like Go
    /// (`AddNewAnalyzeJob`, `StartAnalyzeJob`, `FinishAnalyzeJob`), a failed
    /// job-row write never fails the `ANALYZE`: Go logs it, and a job whose
    /// insert failed has no id, so its start and finish are skipped.
    pub(crate) fn record_analyze_job(
        &mut self,
        job: &AnalyzeJob,
        processed_rows: i64,
        failure: Option<&str>,
    ) {
        if !self.stats_storage_present() {
            return;
        }
        // Go truncates job_info and fail_reason to TEXT's 65535 bytes.
        let truncate = |text: &str| -> String {
            let mut end = text.len().min(65535);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text[..end].to_owned()
        };
        let config = tidb_config::config_tree::config::get_global_config();
        let instance = format!("{}:{}", config.host, config.port);
        let job_info = truncate(&job.job_info);
        let id = self.run_internal_write(&must_escape_sql_text(
            "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, state, instance, process_id) VALUES (%?, %?, %?, %?, %?, %?, %?)",
            &[
                SqlArg::String(job.schema.as_bytes()),
                SqlArg::String(job.table.as_bytes()),
                SqlArg::String(job.partition.as_bytes()),
                SqlArg::String(job_info.as_bytes()),
                SqlArg::String(ANALYZE_PENDING.as_bytes()),
                SqlArg::String(instance.as_bytes()),
                SqlArg::Unsigned(self.connection_id()),
            ],
        ));
        let Ok(Some(id)) = id else {
            return;
        };
        let start = utc_now_text();
        let _ = self.run_internal_write(&must_escape_sql_text(
            "UPDATE mysql.analyze_jobs SET start_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %? WHERE id = %?",
            &[
                SqlArg::String(start.as_bytes()),
                SqlArg::String(ANALYZE_RUNNING.as_bytes()),
                SqlArg::Unsigned(id),
            ],
        ));
        let end = utc_now_text();
        let sql = match failure {
            Some(reason) => {
                let reason = truncate(reason);
                must_escape_sql_text(
                    "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?",
                    &[
                        SqlArg::Signed(processed_rows),
                        SqlArg::String(end.as_bytes()),
                        SqlArg::String(ANALYZE_FAILED.as_bytes()),
                        SqlArg::String(reason.as_bytes()),
                        SqlArg::Unsigned(id),
                    ],
                )
            }
            None => must_escape_sql_text(
                "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?",
                &[
                    SqlArg::Signed(processed_rows),
                    SqlArg::String(end.as_bytes()),
                    SqlArg::String(ANALYZE_FINISHED.as_bytes()),
                    SqlArg::Unsigned(id),
                ],
            ),
        };
        let _ = self.run_internal_write(&sql);
    }

    /// Go `SaveAnalyzeResultToStorage` for one analyzed physical table:
    /// the `stats_meta` decision (a newer stored snapshot wins; a first row
    /// is replaced in; an existing one is updated), then each histogram's
    /// statements. `dump_fm_sketch` is Go's `needDumpFMS` (the table is a
    /// partition).
    pub(crate) fn save_analyze_result_to_storage(
        &mut self,
        table_id: i64,
        count: i64,
        version: u64,
        stats_ver: i64,
        items: &[ClusterStatsItem],
        dump_fm_sketch: bool,
    ) -> Result<(), DriverError> {
        if !self.stats_storage_present() {
            return Ok(());
        }
        let snapshot = version;
        let rows = self.user_table_rows(&must_escape_sql_text(
            "select snapshot, count, modify_count from mysql.stats_meta where table_id in (%?, %?)",
            &[SqlArg::Signed(-table_id), SqlArg::Signed(table_id)],
        ))?;
        if let Some(row) = rows.first() {
            if datum_u64(row.first()) >= snapshot && stats_ver == 2 {
                return Ok(());
            }
        }
        if rows.is_empty() || stats_ver != 2 {
            self.run_internal_write(&must_escape_sql_text(
                "replace into mysql.stats_meta (version, table_id, count, snapshot, last_stats_histograms_version) values (%?, %?, %?, %?, %?)",
                &[
                    SqlArg::Unsigned(version),
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(count),
                    SqlArg::Unsigned(snapshot),
                    SqlArg::Unsigned(version),
                ],
            ))?;
        } else {
            // Go stores `max(curModifyCnt - results.BaseModifyCnt, 0)`, the
            // changes that landed while the analyze ran. This tier reads,
            // analyzes and saves with no other writer in between, so the
            // stored count is the base itself and the difference is zero.
            let modify_count = 0_i64;
            self.run_internal_write(&must_escape_sql_text(
                "update mysql.stats_meta set version=%?, modify_count=%?, count=%?, snapshot=%?, last_stats_histograms_version=%? where table_id=%?",
                &[
                    SqlArg::Unsigned(version),
                    SqlArg::Signed(modify_count),
                    SqlArg::Signed(count.max(0)),
                    SqlArg::Unsigned(snapshot),
                    SqlArg::Unsigned(version),
                    SqlArg::Signed(table_id),
                ],
            ))?;
        }
        for item in items {
            for statement in tidb_exec::cluster_stats_write::save_stats_item_sql(
                table_id,
                item,
                version,
                stats_ver,
                dump_fm_sketch,
                true,
            )
            .map_err(storage_error)?
            {
                self.run_internal_write(&statement)?;
            }
        }
        Ok(())
    }

    /// Go `WriteGlobalStatsToStorage`: `SaveColOrIdxStatsToStorage` for each
    /// merged global histogram, each replacing the `stats_meta` row first.
    pub(crate) fn save_global_stats_to_storage(
        &mut self,
        table_id: i64,
        count: i64,
        modify_count: i64,
        version: u64,
        stats_ver: i64,
        items: &[ClusterStatsItem],
    ) -> Result<(), DriverError> {
        if !self.stats_storage_present() {
            return Ok(());
        }
        for item in items {
            self.run_internal_write(&must_escape_sql_text(
                "replace into mysql.stats_meta (version, table_id, count, modify_count, last_stats_histograms_version) values (%?, %?, %?, %?, %?)",
                &[
                    SqlArg::Unsigned(version),
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(count),
                    SqlArg::Signed(modify_count),
                    SqlArg::Unsigned(version),
                ],
            ))?;
            for statement in tidb_exec::cluster_stats_write::save_stats_item_sql(
                table_id, item, version, stats_ver, false, false,
            )
            .map_err(storage_error)?
            {
                self.run_internal_write(&statement)?;
            }
        }
        Ok(())
    }

    /// Whether this store has the statistics tables (a fresh in-process
    /// store creates them at bootstrap).
    /// Go `SimpleExec.executeDropStats`: the statistics ids the statement
    /// names (every partition and the table itself; or, through the
    /// deprecated scopes, the table's global id or the named partitions),
    /// soft-deleted from storage (`DeleteTableStatsFromKV(ids, true)`), then
    /// the statistics cache refreshed from what storage now holds
    /// (`Handle.Update`).
    pub(crate) fn drop_stats_stmt(
        &mut self,
        stmt: &tidb_ast::DropStatsStmt,
    ) -> Result<StmtOutput, DriverError> {
        let mut tables = Vec::with_capacity(stmt.tables.len());
        for path in &stmt.tables {
            let (database, name) = match path.as_slice() {
                [name] => (self.current_db.clone(), name.clone()),
                [database, name] => (database.clone(), name.clone()),
                _ => {
                    return Err(DriverError::unsupported(
                        "DROP STATS of a malformed table name",
                    ))
                }
            };
            if database.is_empty() {
                return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
            }
            let table =
                self.with_catalog_mut(|catalog| match catalog.table_in(&database, &name) {
                    Some(TableEntry::Kv(table)) => Ok(Arc::clone(table)),
                    _ => Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                        "{database}.{name}"
                    )))),
                })?;
            tables.push(table);
        }
        let mut stats_ids = Vec::new();
        if stmt.global {
            stats_ids.push(tables[0].table_id);
        } else if stmt.partitions.is_empty() {
            for table in &tables {
                stats_ids.extend(physical_ids_and_partition_names(table, &[])?);
                stats_ids.push(table.table_id);
            }
        } else {
            stats_ids = physical_ids_and_partition_names(&tables[0], &stmt.partitions)?;
        }
        if !self.stats_storage_present() {
            return Err(DriverError::unsupported(
                "DROP STATS of a cluster node's statistics storage is not ported",
            ));
        }
        let start_ts = self.with_catalog_mut(|catalog| Ok(catalog.allocate_tso()))?;
        self.delete_table_stats_from_kv(&stats_ids, start_ts)?;
        for table in &tables {
            for physical_id in table_stats_ids(table) {
                if stats_ids.contains(&physical_id) {
                    self.update_table_stats_from_storage(table, physical_id)?;
                }
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Go `DeleteTableStatsFromKV(statsIDs, soft = true)`: each id's meta
    /// row re-versioned so readers reload it, its histogram rows reset in
    /// place, and its buckets, TopN, FM sketches, predicate-column usage,
    /// saved analyze options and statistics lock removed.
    fn delete_table_stats_from_kv(
        &mut self,
        stats_ids: &[i64],
        start_ts: u64,
    ) -> Result<(), DriverError> {
        for &stats_id in stats_ids {
            self.run_internal_write(&must_escape_sql_text(
                "update mysql.stats_meta set version = %?, last_stats_histograms_version = %? where table_id = %? ",
                &[
                    SqlArg::Unsigned(start_ts),
                    SqlArg::Unsigned(start_ts),
                    SqlArg::Signed(stats_id),
                ],
            ))?;
            self.run_internal_write(&must_escape_sql_text(
                "update mysql.stats_histograms set distinct_count = 0, null_count = 0, tot_col_size = 0, modify_count = 0, version = %?,cm_sketch = null, stats_ver = 0, flag = 0, correlation = 0, last_analyze_pos = null where table_id = %?",
                &[SqlArg::Unsigned(start_ts), SqlArg::Signed(stats_id)],
            ))?;
            for sql in [
                "delete from mysql.stats_buckets where table_id = %?",
                "delete from mysql.stats_top_n where table_id = %?",
                "delete from mysql.stats_fm_sketch where table_id = %?",
                "delete from mysql.column_stats_usage where table_id = %?",
                "delete from mysql.analyze_options where table_id = %?",
                // Go `lockstats.DeleteLockSQL`.
                "DELETE FROM mysql.stats_table_locked WHERE table_id = %?",
            ] {
                self.run_internal_write(&must_escape_sql_text(sql, &[SqlArg::Signed(stats_id)]))?;
            }
            self.with_catalog_mut(|catalog| {
                catalog.delete_analyze_options(stats_id);
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Go `StatsCache.Update` for one physical table whose meta row moved:
    /// `TableStatsFromStorage(.., loadAll = false)` over the stored rows,
    /// installed with the meta row's versions and counts. A table with no
    /// meta row keeps whatever the cache holds, as Go's update never visits
    /// it.
    fn update_table_stats_from_storage(
        &mut self,
        table: &KvTable,
        physical_id: i64,
    ) -> Result<(), DriverError> {
        let meta = self.user_table_rows(&must_escape_sql_text(
            "select version, snapshot, modify_count, count, last_stats_histograms_version from mysql.stats_meta where table_id = %?",
            &[SqlArg::Signed(physical_id)],
        ))?;
        let Some(meta) = meta.first() else {
            return Ok(());
        };
        let histograms = self.user_table_rows(&must_escape_sql_text(
            "select hist_id, is_index, stats_ver, flag, distinct_count, null_count, version, tot_col_size, correlation from mysql.stats_histograms where table_id = %?",
            &[SqlArg::Signed(physical_id)],
        ))?;
        let snapshot = datum_u64(meta.get(1));
        let mut stats = ClusterTableStats {
            table_id: physical_id,
            version: datum_u64(meta.first()),
            snapshot,
            modify_count: datum_i64(meta.get(2)),
            row_count: datum_u64(meta.get(3)),
            last_analyze_version: snapshot,
            last_stats_hist_version: datum_u64(meta.get(4)),
            columns: Vec::new(),
            indexes: Vec::new(),
        };
        for row in &histograms {
            let is_index = datum_i64(row.get(1)) != 0;
            let item = histogram_row_item(
                datum_i64(row.first()),
                is_index,
                datum_i64(row.get(2)),
                datum_i64(row.get(3)),
                datum_i64(row.get(4)),
                datum_i64(row.get(5)),
                datum_u64(row.get(6)),
                datum_i64(row.get(7)),
                datum_f64(row.get(8)),
            );
            if is_index {
                stats.indexes.push(item);
            } else {
                stats.columns.push(item);
            }
        }
        stats.columns.sort_by_key(|item| item.id);
        stats.indexes.sort_by_key(|item| item.id);
        let statistics_table =
            stats.to_updated_statistics_table_for_schema(&StatsTableSchema::of_kv_table(table));
        let statistics =
            tidb_executor::load_stats::table_statistics_from_table(&statistics_table, table);
        self.with_catalog_mut(|catalog| {
            catalog.set_table_statistics(physical_id, Arc::new(statistics));
            Ok(())
        })
    }

    /// Whether this session writes Go's statistics storage itself: its own
    /// catalog holds the `mysql` statistics tables. A cluster node's are the
    /// cluster's TiKV tables, behind the node's statement savepoints, and its
    /// statistics writer persists them in its own transactions.
    fn stats_storage_present(&mut self) -> bool {
        self.with_catalog_mut(|catalog| {
            Ok(["stats_meta", "stats_histograms", "analyze_jobs"]
                .iter()
                .all(|table| {
                    catalog.table_in("mysql", table).is_some()
                        && !catalog.table_has_external_statement_rollback("mysql", table)
                }))
        })
        .unwrap_or(false)
    }
}

/// Go `GetPhysicalIDsAndPartitionNames`' ids: a non-partitioned table is its
/// own id (and names no partition), a partitioned one every partition or the
/// named ones.
fn physical_ids_and_partition_names(
    table: &KvTable,
    partition_names: &[String],
) -> Result<Vec<i64>, DriverError> {
    let Some(partition) = table.partition() else {
        if !partition_names.is_empty() {
            return Err(DriverError::PartitionManagementOnNonpartitioned);
        }
        return Ok(vec![table.table_id]);
    };
    if partition_names.is_empty() {
        return Ok(partition
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect());
    }
    partition_names
        .iter()
        .map(|name| {
            partition
                .definitions
                .iter()
                .find(|definition| definition.name.eq_ignore_ascii_case(name))
                .map(|definition| definition.id)
                .ok_or_else(|| {
                    DriverError::unsupported(format!(
                        "can not found the specified partition name {name} in the table definition"
                    ))
                })
        })
        .collect()
}

/// Every statistics id one table owns: its partitions and its own id.
fn table_stats_ids(table: &KvTable) -> Vec<i64> {
    let mut ids: Vec<i64> = table
        .partition()
        .map(|partition| {
            partition
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect()
        })
        .unwrap_or_default();
    ids.push(table.table_id);
    ids
}

fn datum_i64(value: Option<&Datum>) -> i64 {
    match value {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}

fn datum_f64(value: Option<&Datum>) -> f64 {
    match value {
        Some(Datum::Real(value)) => *value,
        Some(Datum::Int(value)) => *value as f64,
        _ => 0.0,
    }
}
