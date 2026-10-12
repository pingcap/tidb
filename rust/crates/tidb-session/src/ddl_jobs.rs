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

//! The DDL history of this synchronous owner: the jobs a statement submitted
//! (`tidb_executor::ddl::job_history`) finished the way Go's job worker
//! leaves them and filed by `AddHistoryDDLJob`, and Go's
//! `DDLJobRetriever.appendJobToChunk`, which renders them for both
//! `ADMIN SHOW DDL JOBS` and `information_schema.DDL_JOBS`.

use tidb_datatype::{Datum, Time, TimeType};
use tidb_executor::ddl::job_history::{done_schema_state, rolls_back, SubmittedDdlJob};
use tidb_executor::{Catalog, DriverError};
use tidb_model::go_runtime::GoShared;
use tidb_model::{ActionType, Job, JobState, SchemaState};

use crate::Session;

impl Session {
    /// Go's job worker finishing the statement's submitted jobs and
    /// `AddHistoryDDLJob` filing each: the meta history every reader uses,
    /// then Go's best-effort copy into `mysql.tidb_ddl_history`. A statement
    /// that failed after submitting leaves its jobs rolled back (a job that
    /// reorganizes data) or cancelled.
    pub(crate) fn finish_submitted_ddl_jobs(&mut self, sql: &str, succeeded: bool) {
        let submitted = self.ddl_job_sink.take();
        if submitted.is_empty() {
            return;
        }
        let context = self.ddl_statement_context();
        let filed = self.with_persistent_catalog_mut(|catalog| {
            let mut filed = Vec::with_capacity(submitted.len());
            for submitted in submitted {
                let mut job = finished_job(catalog, &context, sql, submitted, succeeded);
                catalog.add_history_ddl_job(&mut job)?;
                filed.push(job);
            }
            Ok(filed)
        });
        let Ok(filed) = filed else {
            return;
        };
        for mut job in filed {
            // Go `addHistoryDDLJob2Table` only logs a failure.
            let _ = self.insert_ddl_history_row(&mut job);
        }
    }

    /// Go `addHistoryDDLJob2Table`.
    fn insert_ddl_history_row(&mut self, job: &mut Job) -> Result<(), DriverError> {
        let encoded = job.encode(true).map_err(|error| DriverError::DdlCoded {
            errno: 1105,
            message: error.to_string(),
        })?;
        let hex = encoded
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let quote = |text: &str| format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"));
        let create_time = tso_local_time(job.start_ts).to_string();
        let sql = format!(
            "insert ignore into mysql.tidb_ddl_history(job_id, job_meta, db_name, table_name, \
             schema_ids, table_ids, create_time) values ({}, 0x{hex}, {}, {}, {}, {}, {})",
            job.id,
            quote(&job.schema_name.to_utf8_lossy_go()),
            quote(&job.table_name.to_utf8_lossy_go()),
            quote(&job.schema_id.to_string()),
            quote(&job.table_id.to_string()),
            quote(&create_time),
        );
        self.run_internal_write(&sql).map(|_| ())
    }

    /// Go `DDLJobRetriever.initial`: the running jobs (`GetAllDDLJobs`),
    /// then the meta history newest first (`GetLastHistoryDDLJobsIterator`),
    /// from the cluster's store when one is installed, else this catalog's
    /// own history (which has no running job: its DDL runs to completion).
    pub(crate) fn all_ddl_jobs(&mut self) -> Result<(Vec<Job>, Vec<Job>), DriverError> {
        if let Some(store) = self.ddl_job_history_store.clone() {
            return store
                .all_ddl_jobs()
                .map_err(|message| DriverError::DdlCoded {
                    errno: 1105,
                    message,
                });
        }
        let history =
            self.with_persistent_catalog_mut(|catalog| catalog.last_history_ddl_jobs())?;
        Ok((Vec::new(), history))
    }
}

/// The job Go's worker leaves when it finishes `submitted`.
fn finished_job(
    catalog: &mut Catalog,
    context: &tidb_executor::StmtContext,
    sql: &str,
    submitted: SubmittedDdlJob,
    succeeded: bool,
) -> Job {
    let action = submitted.action;
    let schema_id = submitted
        .schema_id
        .or_else(|| catalog.database_id(&submitted.schema_name))
        .or_else(|| {
            catalog
                .policy(&submitted.schema_name)
                .map(|policy| policy.id)
        })
        .unwrap_or(0);
    let table_id = submitted
        .table_id
        .or_else(|| {
            (!submitted.table_name.is_empty())
                .then(|| catalog.stored_table_id(&submitted.schema_name, &submitted.table_name))
                .flatten()
        })
        .unwrap_or(0);
    let (state, schema_state) = if succeeded {
        (JobState::SYNCED, done_schema_state(action))
    } else if rolls_back(action) {
        (JobState::ROLLBACK_DONE, SchemaState::NONE)
    } else {
        (JobState::CANCELLED, SchemaState::NONE)
    };
    let mut job = Job::default();
    job.version = tidb_model::get_job_ver_in_use();
    // Go `GenGlobalID`: the job takes the next ID of the sequence its
    // table IDs come from.
    job.id = catalog.allocate_table_id();
    job.type_ = action;
    job.schema_id = schema_id;
    job.table_id = table_id;
    job.schema_name = submitted.schema_name.clone().into();
    job.table_name = submitted.table_name.clone().into();
    job.state = state;
    job.schema_state = schema_state;
    if succeeded {
        job.row_count = submitted.row_count;
    }
    job.query =
        tidb_executor::ddl::job_history::rewritten_query(sql, submitted.query_rewrite.as_ref())
            .into();
    job.start_ts = catalog.allocate_tso();
    job.real_start_ts = catalog.allocate_tso();
    job.sql_mode = context.ddl_sql_mode();
    job.cdc_write_source = context.ddl_cdc_write_source();
    let finished_ts = catalog.allocate_tso();
    let mut history = tidb_model::HistoryInfo {
        schema_version: i64::try_from(catalog.schema_version()).unwrap_or(i64::MAX),
        finished_ts,
        ..tidb_model::HistoryInfo::default()
    };
    // Go `FinishTableJob` / `FinishMultipleTableJob` / `FinishDBJob`: the
    // finished table (by its final name), tables, or schema.
    let table_info = |id: i64| {
        catalog.table_names_by_id(id).map(|(_, name)| {
            let mut info = tidb_model::TableInfo::default();
            info.id = id;
            info.name = tidb_ast::CiString::new(name);
            GoShared::new(info)
        })
    };
    if !submitted.multiple_table_ids.is_empty() {
        let infos = submitted
            .multiple_table_ids
            .iter()
            .filter_map(|id| table_info(*id))
            .collect::<Vec<_>>();
        history.table_info = infos.last().cloned();
        history.multiple_table_infos = tidb_model::go_runtime::GoSharedPointerSlice::from_handles(
            infos.into_iter().map(Some).collect(),
        );
    } else if table_id != 0 {
        history.table_info = table_info(table_id);
    } else if submitted.table_name.is_empty() && schema_id != 0 {
        let mut info = tidb_model::DBInfo::default();
        info.id = schema_id;
        info.name = tidb_ast::CiString::new(submitted.schema_name.clone());
        history.db_info = Some(GoShared::new(info));
    }
    job.binlog_info = Some(GoShared::new(history));
    if !submitted.sub_jobs.is_empty() {
        let sub_jobs = submitted
            .sub_jobs
            .iter()
            .map(|sub_action| {
                let mut sub_job = tidb_model::SubJob::default();
                sub_job.type_ = *sub_action;
                if succeeded {
                    sub_job.state = JobState::DONE;
                    sub_job.schema_state = done_schema_state(*sub_action);
                } else {
                    sub_job.state = state;
                }
                Some(GoShared::new(sub_job))
            })
            .collect::<Vec<_>>();
        let info = tidb_model::MultiSchemaInfo {
            sub_jobs: tidb_model::go_runtime::GoSharedPointerSlice::from_handles(sub_jobs),
            ..tidb_model::MultiSchemaInfo::default()
        };
        job.multi_schema_info = Some(GoShared::new(info));
    }
    job
}

/// Go `ts2Time` over a TSO: its physical milliseconds in the process's
/// local zone.
pub(crate) fn tso_local_time(timestamp: u64) -> Time {
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
pub(crate) fn ddl_job_rows(job: &Job, in_show_stmt: bool) -> Vec<Vec<Datum>> {
    let history = job.binlog_info.as_ref().map(|info| info.read().clone());
    let mut schema_name = job.schema_name.to_utf8_lossy_go();
    let mut table_name = String::new();
    let mut finish_ts = 0;
    if let Some(history) = &history {
        finish_ts = history.finished_ts;
        if let Some(info) = &history.table_info {
            table_name = info.read().name.lowercase().to_owned();
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
                last_values.push(last_column(String::new()));
            }
        }
    }
    last_values.push(last_column(String::new()));
    for (row, last) in rows.iter_mut().zip(last_values) {
        row.push(last);
    }
    rows
}
