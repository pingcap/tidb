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

//! The `mysql.analyze_jobs` writers of Go `autoanalyze.go`: `InsertAnalyzeJob`,
//! `StartAnalyzeJob`, `UpdateAnalyzeJobProgress`, `FinishAnalyzeJob`,
//! `DeleteAnalyzeJobs` and the corrupted-job cleanups, plus the
//! `dataForAnalyzeStatusHelper` read.

use std::collections::HashSet;

use tidb_datatype::{
    Datum, Time,
};
use tidb_model::table_info::TableInfo;

use tidb_infoschema::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use tidb_syssession::mysql_system_tables::{
    scan_system_table, HandleLayout,
    SystemRow, SystemTableError, SystemTableView,
};
use tidb_syssession::system_row_write::{defaults_row, delete_clustered_row, delete_row, RowValues};
use tidb_table::origin_default::RowEncodeError;
use tidb_syssession::stats_table_rows::*;

/// One persisted `mysql.analyze_jobs` identifier.
pub type AnalyzeJobId = u64;

/// Pinned Go `BatchUpdateAnalyzeJobSQL`'s failure reason.
pub const CORRUPTED_ANALYZE_JOB_FAILURE: &str =
    "The TiDB Server has either shut down or the analyze query was terminated during the analyze job execution";

/// Pinned Go `dataForAnalyzeStatusHelper`'s restricted query: order all jobs
/// by `update_time DESC` and retain at most thirty rows. Privilege filtering
/// deliberately happens later in the session, so invisible rows still occupy
/// the SQL limit just as they do in Go.
pub fn load_analyze_status_jobs<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
) -> Result<Vec<tidb_stats::AnalyzeStatusJob>, StatsWriteError> {
    const MAX_ANALYZE_JOBS: usize = 30;
    const COLUMNS: &[&str] = &[
        "table_schema",
        "table_name",
        "partition_name",
        "job_info",
        "processed_rows",
        "start_time",
        "end_time",
        "state",
        "fail_reason",
        "instance",
        "process_id",
        "update_time",
    ];

    let view = SystemTableView::locate(catalog, "analyze_jobs", COLUMNS)?;
    let timezone = tidb_datatype::SessionTimeZone::utc();
    let mut rows = Vec::new();
    for (key, value) in scan_system_table(snapshot, &view)? {
        let row = SystemRow::parse_in_timezone(&view, &key, &value, Some(&timezone))?;
        let time = |column: &str| -> Result<Option<Time>, StatsWriteError> {
            match row.datum(column)? {
                None => Ok(None),
                Some(Datum::Time(value)) => Ok(Some(*value)),
                Some(value) => Err(StatsWriteError::Read(
                    SystemTableError::UnexpectedColumnValue {
                        name: view.name().to_owned(),
                        column: column.to_owned(),
                        wanted: "a time",
                        stored: format!("{value:?}"),
                    },
                )),
            }
        };
        rows.push((
            time("update_time")?,
            tidb_stats::AnalyzeStatusJob {
                table_schema: row.text("table_schema")?.unwrap_or_default(),
                table_name: row.text("table_name")?.unwrap_or_default(),
                partition_name: row.text("partition_name")?.unwrap_or_default(),
                job_info: row.text("job_info")?.unwrap_or_default(),
                processed_rows: row.i64("processed_rows")?.unwrap_or_default(),
                start_time: time("start_time")?,
                end_time: time("end_time")?,
                state: row.enum_label("state")?.unwrap_or_default(),
                fail_reason: row.text("fail_reason")?,
                instance: row.text("instance")?.unwrap_or_default(),
                process_id: row.u64("process_id")?,
            },
        ));
    }
    rows.sort_by(|left, right| match (left.0, right.0) {
        (Some(left), Some(right)) => right.compare(left),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    rows.truncate(MAX_ANALYZE_JOBS);
    Ok(rows.into_iter().map(|(_, job)| job).collect())
}

/// Plans pinned Go `InsertAnalyzeJob`: one pending row and the shared
/// auto-increment watermark it consumed.
pub fn plan_insert_analyze_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    schema: &str,
    table_name: &str,
    partition_name: &str,
    job_info: &[u8],
    instance: &str,
    process_id: u64,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(AnalyzeJobId, StatsWritePlan), StatsWriteError> {
    let table = locate(catalog, "analyze_jobs")?;
    let job_id = allocate_row_id(catalog, table, row_ids)?;
    let job_id = u64::try_from(job_id).map_err(|_| {
        StatsWriteError::Encode(RowEncodeError(
            "mysql.analyze_jobs auto-increment id is negative".to_owned(),
        ))
    })?;
    let mut values = defaults_row(table, now)?;
    set(table, &mut values, "id", Datum::UInt(job_id));
    set(table, &mut values, "update_time", Datum::Time(now));
    set(
        table,
        &mut values,
        "table_schema",
        Datum::Bytes(schema.as_bytes().to_vec()),
    );
    set(
        table,
        &mut values,
        "table_name",
        Datum::Bytes(table_name.as_bytes().to_vec()),
    );
    set(
        table,
        &mut values,
        "partition_name",
        Datum::Bytes(partition_name.as_bytes().to_vec()),
    );
    set(
        table,
        &mut values,
        "job_info",
        Datum::Bytes(job_info[..job_info.len().min(65_535)].to_vec()),
    );
    set(
        table,
        &mut values,
        "state",
        Datum::Bytes(tidb_stats::ANALYZE_PENDING.as_bytes().to_vec()),
    );
    set(
        table,
        &mut values,
        "instance",
        Datum::Bytes(instance.as_bytes().to_vec()),
    );
    set(table, &mut values, "process_id", Datum::UInt(process_id));
    let mut rows = StatsRows::open_all(snapshot, table, &["id"])?;
    let mut plan = StatsWritePlan::default();
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
    Ok((job_id, plan))
}

fn plan_update_analyze_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    job_id: AnalyzeJobId,
    now: Time,
    update: impl FnOnce(&TableInfo, &mut RowValues),
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let table = locate(catalog, "analyze_jobs")?;
    let id_column = column_id(table, "id")?;
    let existing = read_rows_with_keys(snapshot, table, None)?
        .into_iter()
        .find_map(|(_, values)| {
            let matches = match values.get(&id_column) {
                Some(Datum::UInt(value)) => *value == job_id,
                Some(Datum::Int(value)) => u64::try_from(*value).ok() == Some(job_id),
                _ => false,
            };
            matches.then_some(values)
        });
    let Some(mut values) = existing else {
        return Ok(StatsWritePlan::default());
    };
    let mut rows = StatsRows::open_all(snapshot, table, &["id"])?;
    set(table, &mut values, "update_time", Datum::Time(now));
    update(table, &mut values);
    let mut plan = StatsWritePlan::default();
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
    Ok(plan)
}

/// Plans pinned Go `StartAnalyzeJob`.
pub fn plan_start_analyze_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    job_id: AnalyzeJobId,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_update_analyze_job(
        snapshot,
        catalog,
        job_id,
        now,
        |table, values| {
            set(table, values, "start_time", Datum::Time(now));
            set(
                table,
                values,
                "state",
                Datum::Bytes(tidb_stats::ANALYZE_RUNNING.as_bytes().to_vec()),
            );
        },
        row_ids,
    )
}

/// Plans pinned Go `UpdateAnalyzeJobProgress`: add one already-throttled
/// progress delta to `mysql.analyze_jobs.processed_rows`.
///
/// The 10,000,000-row/five-second throttle belongs to the executor-side
/// `AnalyzeProgress`; storage receives only the delta that passed it.
pub fn plan_update_analyze_job_progress<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    job_id: AnalyzeJobId,
    processed_rows: i64,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_update_analyze_job(
        snapshot,
        catalog,
        job_id,
        now,
        |table, values| {
            let processed = column_id(table, "processed_rows")
                .ok()
                .and_then(|id| values.get(&id))
                .and_then(|value| match value {
                    Datum::Int(value) => Some(*value),
                    Datum::UInt(value) => i64::try_from(*value).ok(),
                    _ => None,
                })
                .unwrap_or_default()
                .saturating_add(processed_rows);
            set(
                table,
                values,
                "processed_rows",
                Datum::UInt(u64::try_from(processed).unwrap_or_default()),
            );
        },
        row_ids,
    )
}

/// Plans pinned Go `FinishAnalyzeJob` for a table-analysis job.
pub fn plan_finish_analyze_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    job_id: AnalyzeJobId,
    processed_rows: i64,
    failure: Option<&str>,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_update_analyze_job(
        snapshot,
        catalog,
        job_id,
        now,
        |table, values| {
            let processed = column_id(table, "processed_rows")
                .ok()
                .and_then(|id| values.get(&id))
                .and_then(|value| match value {
                    Datum::Int(value) => Some(*value),
                    Datum::UInt(value) => i64::try_from(*value).ok(),
                    _ => None,
                })
                .unwrap_or_default()
                .saturating_add(processed_rows);
            set(
                table,
                values,
                "processed_rows",
                Datum::UInt(u64::try_from(processed).unwrap_or_default()),
            );
            set(table, values, "end_time", Datum::Time(now));
            let state = if failure.is_some() {
                tidb_stats::ANALYZE_FAILED
            } else {
                tidb_stats::ANALYZE_FINISHED
            };
            set(
                table,
                values,
                "state",
                Datum::Bytes(state.as_bytes().to_vec()),
            );
            set(
                table,
                values,
                "fail_reason",
                failure.map_or(Datum::Null, |failure| {
                    let bytes = failure.as_bytes();
                    Datum::Bytes(bytes[..bytes.len().min(65_535)].to_vec())
                }),
            );
            set(table, values, "process_id", Datum::Null);
        },
        row_ids,
    )
}

/// Plans pinned Go `DeleteAnalyzeJobs`' timestamp predicate.
pub fn plan_delete_analyze_jobs<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    cutoff: &Time,
) -> Result<StatsWritePlan, StatsWriteError> {
    let table = locate(catalog, "analyze_jobs")?;
    let update_time = column_id(table, "update_time")?;
    let mut plan = StatsWritePlan::default();
    for (key, values) in read_rows_with_keys(snapshot, table, None)? {
        let Some(Datum::Time(update_time)) = values.get(&update_time) else {
            continue;
        };
        if update_time.compare(*cutoff) != std::cmp::Ordering::Less {
            continue;
        }
        if matches!(HandleLayout::of(table), HandleLayout::RowId) {
            plan.mutations.extend(delete_row(table, &key, &values)?);
        } else {
            plan.mutations.extend(delete_clustered_row(table, &values)?);
        }
    }
    Ok(plan)
}

/// Plans pinned Go `CleanupCorruptedAnalyzeJobsOnCurrentInstance`.
///
/// The caller supplies only process IDs whose current SQL normalizes to an
/// `ANALYZE TABLE` statement. A NULL persisted process ID is deliberately
/// ignored, matching the otherwise-surprising guard in Go's row loop.
pub fn plan_cleanup_corrupted_analyze_jobs_on_current_instance<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    instance: &str,
    current_analyze_process_ids: &HashSet<u64>,
    cutoff: &Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_cleanup_corrupted_analyze_jobs(
        snapshot,
        catalog,
        cutoff,
        |table, values| {
            Ok(
                datum_bytes(values.get(&column_id(table, "instance")?))
                    == Some(instance.as_bytes())
                    && datum_u64(values.get(&column_id(table, "process_id")?)).is_some_and(
                        |process_id| !current_analyze_process_ids.contains(&process_id),
                    ),
            )
        },
        row_ids,
    )
}

/// Plans pinned Go `CleanupCorruptedAnalyzeJobsOnDeadInstances`.
pub fn plan_cleanup_corrupted_analyze_jobs_on_dead_instances<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    alive_instances: &HashSet<String>,
    cutoff: &Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    plan_cleanup_corrupted_analyze_jobs(
        snapshot,
        catalog,
        cutoff,
        |table, values| {
            Ok(
                datum_bytes(values.get(&column_id(table, "instance")?)).is_some_and(|instance| {
                    std::str::from_utf8(instance)
                        .map_or(true, |instance| !alive_instances.contains(instance))
                }),
            )
        },
        row_ids,
    )
}

fn plan_cleanup_corrupted_analyze_jobs<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    cutoff: &Time,
    select: impl Fn(&TableInfo, &RowValues) -> Result<bool, StatsWriteError>,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let table = locate(catalog, "analyze_jobs")?;
    let state = column_id(table, "state")?;
    let update_time = column_id(table, "update_time")?;
    let mut selected = Vec::new();
    for (_, values) in read_rows_with_keys(snapshot, table, None)? {
        let active = datum_bytes(values.get(&state)).is_some_and(|state| {
            state.eq_ignore_ascii_case(tidb_stats::ANALYZE_PENDING.as_bytes())
                || state.eq_ignore_ascii_case(tidb_stats::ANALYZE_RUNNING.as_bytes())
        });
        let stale = matches!(values.get(&update_time), Some(Datum::Time(time)) if time.compare(*cutoff) == std::cmp::Ordering::Less);
        if active && stale && select(table, &values)? {
            selected.push(values);
        }
    }

    let mut rows = StatsRows::open_all(snapshot, table, &["id"])?;
    let mut plan = StatsWritePlan::default();
    for mut values in selected {
        set(
            table,
            &mut values,
            "state",
            Datum::Bytes(tidb_stats::ANALYZE_FAILED.as_bytes().to_vec()),
        );
        set(
            table,
            &mut values,
            "fail_reason",
            Datum::Bytes(CORRUPTED_ANALYZE_JOB_FAILURE.as_bytes().to_vec()),
        );
        set(table, &mut values, "process_id", Datum::Null);
        rows.store_row(catalog, &values, &mut plan, row_ids)?;
    }
    Ok(plan)
}

fn datum_bytes(value: Option<&Datum>) -> Option<&[u8]> {
    match value {
        Some(Datum::Bytes(value)) => Some(value),
        Some(Datum::String(value)) => Some(value.bytes()),
        Some(Datum::Enum(value, _)) => Some(value.name().as_bytes()),
        _ => None,
    }
}

fn datum_u64(value: Option<&Datum>) -> Option<u64> {
    match value {
        Some(Datum::UInt(value)) => Some(*value),
        Some(Datum::Int(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

/// Storage-backed reader used by pinned Go `SHOW ANALYZE STATUS`.
pub trait AnalyzeStatusProvider: Send + Sync {
    /// Runs Go's fresh restricted read of the thirty newest persisted jobs.
    fn load_analyze_status(
        &self,
        resource_group: &str,
    ) -> Result<Vec<tidb_stats::AnalyzeStatusJob>, String>;

    /// Go `GlobalPDHelper.GetApproximateTableCountFromStorage`. Errors and a
    /// backend without PD both produce zero, as its caller ignores `hasPD`.
    fn approximate_table_count(
        &self,
        resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> i64;
}
