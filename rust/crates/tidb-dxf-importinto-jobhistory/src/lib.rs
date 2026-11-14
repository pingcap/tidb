// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/jobhistory`: history-only IMPORT INTO diagnostics.
use std::collections::BTreeMap;
use tidb_datatype::Datum;
use tidb_dxf::*;
use tidb_dxf_importinto_taskkey as taskkey;
use tidb_dxf_storage::{Result, StorageError, TaskManager};
use tidb_sqlescape::SqlArg;

/// Go `Duration`.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Duration {
    pub total: String,
    pub encode: String,
    pub merge_sort: String,
    pub ingest: String,
    pub collect_conflicts: String,
    pub resolve_conflicts: String,
    pub post_process: String,
}
/// Go `Info`.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Info {
    pub job_id: i64,
    pub keyspace: String,
    pub task_id: i64,
    pub state: String,
    pub concurrency: isize,
    pub max_node_count: isize,
    pub distsql_scan_concurrency: isize,
    pub index_count: isize,
    pub column_count: isize,
    pub file_size: String,
    pub data_kv_size: String,
    pub index_kv_size: String,
    pub per_core_speed: String,
    pub overall_speed: String,
    pub row_count: i64,
    pub row_length: i64,
    pub duration: Duration,
}

/// Go `chunk.Row.GetInt64`, including NULL's zero value.
fn int(row: &[Datum], index: usize) -> i64 {
    match row.get(index) {
        Some(Datum::Int(v)) => *v,
        Some(Datum::UInt(v)) => *v as i64,
        _ => 0,
    }
}
/// Go `chunk.Row.IsNull`.
fn is_null(row: &[Datum], index: usize) -> bool {
    matches!(row.get(index), None | Some(Datum::Null))
}
/// Go `chunk.Row.GetString`.
fn string(row: &[Datum], index: usize) -> String {
    row.get(index)
        .filter(|d| !matches!(d, Datum::Null))
        .map_or_else(String::new, |d| d.sql_string().unwrap_or_default())
}

/// Go `GetFromHistory`.
pub fn get_from_history(
    ctx: &tidb_gocontext::Context,
    mgr: &TaskManager,
    keyspace: &str,
    job_id: i64,
) -> Result<Info> {
    // Go `injectfailpoint.DXFRandomErrorWithOnePercent` at the entry point.
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/util/injectfailpoint/DXFRandomError")
        .is_some()
        && rand::random::<f64>() < 0.01
    {
        return Err(StorageError::Other("injected random error, caller: github.com/pingcap/tidb/pkg/dxf/importinto/jobhistory.GetFromHistory".into()));
    }
    let task_key = taskkey::for_job_in_keyspace(keyspace, job_id);
    let tp = IMPORT_INTO.to_string();
    let rows = mgr.execute_sql_with_new_session(
        ctx,
        TASK_QUERY,
        &[
            SqlArg::String(task_key.as_bytes()),
            SqlArg::String(tp.as_bytes()),
        ],
    )?;
    let row = rows.first().ok_or_else(|| {
        StorageError::annotate(
            StorageError::TaskNotFound,
            format!("import-into job {job_id} in keyspace {keyspace} not found in history"),
        )
    })?;
    let mut info = Info {
        job_id,
        keyspace: keyspace.into(),
        task_id: int(row, 0),
        state: string(row, 1),
        concurrency: int(row, 2) as isize,
        max_node_count: int(row, 3) as isize,
        distsql_scan_concurrency: int(row, 4) as isize,
        index_count: int(row, 5) as isize,
        column_count: int(row, 6) as isize,
        row_count: int(row, 8),
        ..Info::default()
    };
    let total_file_bytes = int(row, 7);
    if !is_null(row, 7) {
        info.file_size = format_bytes(total_file_bytes);
    }
    if info.row_count > 0 {
        info.row_length = (total_file_bytes as f64 / info.row_count as f64).round() as i64;
    }
    let step_rows =
        mgr.execute_sql_with_new_session(ctx, STEP_QUERY, &[SqlArg::Signed(info.task_id)])?;
    let (mut data_bytes, mut index_bytes) = (None::<i64>, None::<i64>);
    let mut total_bounds = None::<(i64, i64)>;
    let mut step_durations = BTreeMap::<Step, (i64, i64)>::new();
    for row in step_rows {
        let step = Step(int(&row, 0));
        if step == IMPORT_STEP_WRITE_AND_INGEST && !is_null(&row, 2) {
            let bytes = if string(&row, 1) == "data" {
                &mut data_bytes
            } else {
                &mut index_bytes
            };
            *bytes = Some(bytes.unwrap_or(0).wrapping_add(int(&row, 2)));
        }
        if is_null(&row, 3) || is_null(&row, 4) {
            continue;
        }
        let (start, end) = (int(&row, 3), int(&row, 4));
        total_bounds = Some(total_bounds.map_or((start, end), |(s, e)| (s.min(start), e.max(end))));
        let bounds = step_durations.entry(step).or_insert((start, end));
        bounds.0 = bounds.0.min(start);
        bounds.1 = bounds.1.max(end);
    }
    if let Some(bytes) = data_bytes {
        info.data_kv_size = format_bytes(bytes);
    }
    if let Some(bytes) = index_bytes {
        info.index_kv_size = format_bytes(bytes);
    }
    let total_seconds = total_bounds.map_or(0, |(start, end)| end.wrapping_sub(start).max(0));
    if total_bounds.is_some() {
        info.duration.total = format_duration(total_seconds);
    }
    info.per_core_speed = format_bytes_per_core_hour(
        total_file_bytes,
        total_seconds,
        info.max_node_count,
        info.concurrency,
    );
    info.overall_speed = format_bytes_per_hour(total_file_bytes, total_seconds);
    for (step, (start, end)) in step_durations {
        let duration = format_duration(end.wrapping_sub(start).max(0));
        match step {
            IMPORT_STEP_ENCODE_AND_SORT => info.duration.encode = duration,
            IMPORT_STEP_MERGE_SORT => info.duration.merge_sort = duration,
            IMPORT_STEP_WRITE_AND_INGEST => info.duration.ingest = duration,
            IMPORT_STEP_COLLECT_CONFLICTS => info.duration.collect_conflicts = duration,
            IMPORT_STEP_CONFLICT_RESOLUTION => info.duration.resolve_conflicts = duration,
            IMPORT_STEP_POST_PROCESS => info.duration.post_process = duration,
            _ => {}
        }
    }
    Ok(info)
}
/// Go `formatDuration`.
fn format_duration(seconds: i64) -> String {
    if seconds < 0 {
        return String::new();
    }
    tidb_config_configtypes::format_go_duration(seconds.wrapping_mul(1_000_000_000))
}
/// Go `formatBytes`.
fn format_bytes(size: i64) -> String {
    if size < 0 {
        return String::new();
    }
    tidb_config_configtypes::bytes_size(size as f64)
}
/// Go `formatBytesPerHour`.
fn format_bytes_per_hour(bytes: i64, seconds: i64) -> String {
    if bytes < 0 || seconds <= 0 {
        return String::new();
    }
    format!(
        "{}/hour",
        tidb_config_configtypes::bytes_size(bytes as f64 * 3600.0 / seconds as f64)
    )
}
/// Go `formatBytesPerCoreHour`.
fn format_bytes_per_core_hour(
    bytes: i64,
    seconds: i64,
    nodes: isize,
    concurrency: isize,
) -> String {
    if bytes < 0 || seconds <= 0 || nodes <= 0 || concurrency <= 0 {
        return String::new();
    }
    format!(
        "{}/core/hour",
        tidb_config_configtypes::bytes_size(
            bytes as f64 * 3600.0 / seconds as f64 / nodes.wrapping_mul(concurrency) as f64
        )
    )
}

const TASK_QUERY: &str = r#"
		select
			t.id,
			t.state,
			t.concurrency,
			t.max_node_count,
			cast(json_extract(cast(cast(t.meta as char) as json), '$.Plan.DistSQLScanConcurrency') as signed) as distsql_scan_concurrency,
			cast(json_length(json_extract(cast(cast(t.meta as char) as json), '$.Plan.DesiredTableInfo.index_info')) as signed) as index_count,
			cast(json_length(json_extract(cast(cast(t.meta as char) as json), '$.Plan.DesiredTableInfo.cols')) as signed) as column_count,
			cast(json_extract(cast(cast(t.meta as char) as json), '$.Plan.TotalFileSize') as signed) as file_size_bytes,
			cast(json_extract(cast(cast(t.meta as char) as json), '$.Summary."row-count"') as signed) as row_count
		from mysql.tidb_global_task_history t
		where t.task_key = %? and t.type = %?"#;
const STEP_QUERY: &str = r#"
		select
			step,
			json_unquote(json_extract(cast(meta as char), '$."kv-group"')) as kv_group,
			cast(sum(cast(json_extract(summary, '$.bytes') as signed)) as signed) as bytes,
			min(case when start_time > 0 then start_time else null end) as min_start_time,
			max(case when state_update_time > 0 then state_update_time else null end) as max_state_update_time
		from mysql.tidb_background_subtask_history
		where task_key = %?
		group by step, kv_group"#;
