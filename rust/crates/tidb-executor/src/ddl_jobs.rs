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


//! Go `ShowDDLJobsExec.appendJobToChunk` and `showCommentsFromJob`: the
//! rows `ADMIN SHOW DDL JOBS` and `information_schema.DDL_JOBS` share.

use tidb_datatype::{Datum, Time, TimeType};
use tidb_model::{ActionType, Job, JobState, SchemaState};

use crate::Catalog;

/// Go `ts2Time` over a TSO: its physical milliseconds in the process's
/// local zone.
pub fn tso_local_time(timestamp: u64) -> Time {
    use chrono::TimeZone;
    let milliseconds = i64::try_from(timestamp >> 18).unwrap_or(0);
    let utc = chrono::Utc
        .timestamp_millis_opt(milliseconds)
        .single()
        .unwrap_or_else(chrono::Utc::now);
    let core = tidb_datatype::core_time_from_datetime(utc.with_timezone(&chrono::Local));
    Time::new(core, TimeType::DateTime, 6)
        .or_else(|_| Time::new(core, TimeType::DateTime, 0))
        .expect("a TSO's time is a valid DATETIME")
}

/// One row of Go `DDLJobRetriever.appendJobToChunk`: the job's columns, and
/// for a multi-schema change one more per sub-job. `in_show_stmt` selects
/// ADMIN SHOW DDL JOBS' COMMENTS over DDL_JOBS' QUERY in the last column.
///
/// Go appends that last column after the sub-job rows, so a multi-schema
/// change's rows are shifted by one there: the parent row carries the first
/// sub-job's value and the last sub-job row the parent's. DDL_JOBS repeats
/// the query in every row, where the shift cannot be seen.
///
/// A job that records neither name gets them from `catalog`, the session's
/// info schema, by its schema and table IDs.
pub fn ddl_job_rows(job: &Job, in_show_stmt: bool, catalog: &Catalog) -> Vec<Vec<Datum>> {
    let history = job.binlog_info.as_ref().map(|info| info.read().clone());
    let mut schema_name = job.schema_name.to_utf8_lossy_go();
    let mut table_name = String::new();
    let mut finish_ts = 0;
    if let Some(history) = &history {
        finish_ts = history.finished_ts;
        if let Some(info) = &history.table_info {
            table_name = info.read().name.lowercase().to_owned();
        } else if job.type_ == ActionType::ACTION_RENAME_TABLE {
            // A running rename has no table info yet: its new name, from the
            // arguments, keeps the output consistent.
            if let Ok(Some(args)) = tidb_model::get_rename_table_args(&mut job.clone()) {
                let new_name = args.read().new_table_name.lowercase().to_owned();
                if !new_name.is_empty() {
                    table_name = new_name;
                }
            }
        }
        let multiple = history
            .multiple_table_infos
            .iter_deref()
            .map(|info| info.read().name.lowercase().to_owned())
            .collect::<Vec<_>>();
        if !multiple.is_empty() {
            table_name = multiple.join(",");
        }
        if schema_name.is_empty() {
            if let Some(info) = &history.db_info {
                schema_name = info.read().name.lowercase().to_owned();
            }
        }
    }
    if table_name.is_empty() {
        table_name = job.table_name.to_utf8_lossy_go();
    }
    // Go: an older job stored neither name.
    if schema_name.is_empty() {
        schema_name = catalog
            .database_name_by_id(job.schema_id)
            .unwrap_or_default();
    }
    if table_name.is_empty() {
        table_name = catalog
            .table_names_by_id(job.table_id)
            .map(|(_, table)| table)
            .unwrap_or_default();
    }
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let time_or_null = |ts: u64| {
        if ts > 0 {
            Datum::Time(tso_local_time(ts))
        } else {
            Datum::Null
        }
    };
    let create_time = Datum::Time(tso_local_time(job.start_ts));
    let finish_time = time_or_null(finish_ts);
    let last_column = |comment: String| {
        if in_show_stmt {
            text(&comment)
        } else {
            text(&job.query.to_utf8_lossy_go())
        }
    };
    let row = |job_type: String,
               schema_state: SchemaState,
               row_count: i64,
               real_start_ts: u64,
               state: JobState| {
        vec![
            Datum::Int(job.id),
            text(&schema_name),
            text(&table_name),
            text(&job_type),
            text(&schema_state.to_string()),
            Datum::Int(job.schema_id),
            Datum::Int(job.table_id),
            Datum::Int(row_count),
            create_time.clone(),
            time_or_null(real_start_ts),
            finish_time.clone(),
            text(&state.to_string()),
        ]
    };
    let mut rows = vec![row(
        job.type_.to_string(),
        job.schema_state,
        job.row_count,
        job.real_start_ts,
        job.state,
    )];
    let mut last_values = Vec::new();
    if job.type_ == ActionType::ACTION_MULTI_SCHEMA_CHANGE {
        let (use_dxf, use_cloud) = job.reorg_meta.as_ref().map_or((false, false), |meta| {
            let meta = meta.read();
            (meta.is_dist_reorg, meta.use_cloud_storage)
        });
        if let Some(info) = &job.multi_schema_info {
            for sub_job in info.read().sub_jobs.iter_deref() {
                let sub_job = sub_job.read();
                rows.push(row(
                    format!("{} /* subjob */", sub_job.type_),
                    sub_job.schema_state,
                    sub_job.row_count,
                    sub_job.real_start_ts,
                    sub_job.state,
                ));
                last_values.push(last_column(show_comments_from_subjob(
                    sub_job.reorg_type,
                    use_dxf,
                    use_cloud,
                )));
            }
        }
    }
    last_values.push(last_column(show_comments_from_job(job)));
    for (row, last) in rows.iter_mut().zip(last_values) {
        row.push(last);
    }
    rows
}

/// Go `showCommentsFromJob`: the job's reorganization labels.
fn show_comments_from_job(job: &Job) -> String {
    use tidb_model::reorg::analyze_state;
    use tidb_model::ReorgType;
    let Some(meta) = job.reorg_meta.as_ref() else {
        return String::new();
    };
    let meta = meta.read();
    let mut labels: Vec<String> = Vec::new();
    match meta.analyze_state {
        analyze_state::RUNNING => labels.push("analyzing".to_owned()),
        analyze_state::FAILED => labels.push("analyze_failed".to_owned()),
        analyze_state::TIMEOUT => labels.push("analyze_timeout".to_owned()),
        _ => {}
    }
    let is_adding_index = job.type_ == ActionType::ACTION_ADD_INDEX
        || job.type_ == ActionType::ACTION_ADD_PRIMARY_KEY;
    if is_adding_index && tidb_config_kerneltype::is_next_gen() {
        // The parameters are determined automatically in next-gen.
        return labels.join(", ");
    }
    if is_adding_index {
        match meta.reorg_type {
            ReorgType::TXN => labels.push(ReorgType::TXN.to_string()),
            ReorgType::INGEST => {
                labels.push(ReorgType::INGEST.to_string());
                if meta.is_dist_reorg {
                    labels.push("DXF".to_owned());
                }
                if meta.use_cloud_storage {
                    labels.push("cloud".to_owned());
                }
            }
            ReorgType::TXN_MERGE => labels.push(ReorgType::TXN_MERGE.to_string()),
            _ => {}
        }
    }
    if job.may_need_reorg() {
        use tidb_vardef::defaults::{
            DEF_TIDB_DDL_REORG_BATCH_SIZE, DEF_TIDB_DDL_REORG_MAX_WRITE_SPEED,
            DEF_TIDB_DDL_REORG_WORKER_COUNT,
        };
        let defaults = tidb_model::DDLReorgProcessDefaults::new(
            || DEF_TIDB_DDL_REORG_WORKER_COUNT,
            || DEF_TIDB_DDL_REORG_BATCH_SIZE,
        );
        let concurrency = meta.get_concurrency(defaults);
        let batch_size = meta.get_batch_size(defaults);
        let max_write_speed = meta.get_max_write_speed();
        if concurrency != DEF_TIDB_DDL_REORG_WORKER_COUNT {
            labels.push(format!("thread={concurrency}"));
        }
        if batch_size != DEF_TIDB_DDL_REORG_BATCH_SIZE {
            labels.push(format!("batch_size={batch_size}"));
        }
        if max_write_speed != DEF_TIDB_DDL_REORG_MAX_WRITE_SPEED {
            labels.push(format!("max_write_speed={max_write_speed}"));
        }
        if !meta.target_scope.is_empty() {
            labels.push(format!("service_scope={}", meta.target_scope));
        }
        if meta.max_node_count != 0 {
            labels.push(format!("max_node_count={}", meta.max_node_count));
        }
    }
    labels.join(", ")
}

/// Go `showCommentsFromSubjob`.
fn show_comments_from_subjob(
    reorg_type: tidb_model::ReorgType,
    use_dxf: bool,
    use_cloud: bool,
) -> String {
    if tidb_config_kerneltype::is_next_gen() || reorg_type == tidb_model::ReorgType::NONE {
        return String::new();
    }
    let mut labels = vec![reorg_type.to_string()];
    if use_dxf {
        labels.push("DXF".to_owned());
    }
    if use_dxf && use_cloud {
        labels.push("cloud".to_owned());
    }
    labels.join(", ")
}
