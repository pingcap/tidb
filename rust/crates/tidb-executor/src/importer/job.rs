//! Go `pkg/executor/importer/job.go`: IMPORT INTO jobs in
//! `mysql.tidb_import_jobs`.

use crate::memtable_reader::sql_exec_driver_error;
use crate::DriverError;
use tidb_datatype::{Datum, Time};
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::{ExecutionContext, SqlExecutor};

/// Go `jobStatusPending`.
pub const JOB_STATUS_PENDING: &str = "pending";
/// Go `JobStatusRunning`.
pub const JOB_STATUS_RUNNING: &str = "running";
/// Go `jogStatusCancelled`.
pub const JOB_STATUS_CANCELLED: &str = "cancelled";
/// Go `jobStatusFailed`.
pub const JOB_STATUS_FAILED: &str = "failed";
/// Go `JobStatusFinished`.
pub const JOB_STATUS_FINISHED: &str = "finished";
/// Go `jobStepNone`.
pub const JOB_STEP_NONE: &str = "";
/// Go `JobStepPreparing`.
pub const JOB_STEP_PREPARING: &str = "preparing";
/// Go `JobStepGlobalSorting`.
pub const JOB_STEP_GLOBAL_SORTING: &str = "global-sorting";
/// Go `JobStepImporting`.
pub const JOB_STEP_IMPORTING: &str = "importing";
/// Go `JobStepResolvingConflicts`.
pub const JOB_STEP_RESOLVING_CONFLICTS: &str = "resolving-conflicts";
/// Go `JobStepValidating`.
pub const JOB_STEP_VALIDATING: &str = "validating";

/// Go `baseQuerySQL`.
const BASE_QUERY_SQL: &str = "SELECT
					id, create_time, start_time, update_time, end_time,
					table_schema, table_name, table_id, created_by, parameters, source_file_size,
					status, step, summary, error_message, group_key
				FROM mysql.tidb_import_jobs";

/// Go `ImportParameters`: what SHOW IMPORT JOB displays of the statement.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ImportParameters {
    /// Go `ColumnsAndVars`.
    #[serde(rename = "columns-and-vars", default, skip_serializing_if = "String::is_empty")]
    pub columns_and_vars: String,
    /// Go `SetClause`.
    #[serde(rename = "set-clause", default, skip_serializing_if = "String::is_empty")]
    pub set_clause: String,
    /// Go `FileLocation`, redacted.
    #[serde(rename = "file-location", default)]
    pub file_location: String,
    /// Go `Format`.
    #[serde(default)]
    pub format: String,
    /// Go `Options` (`map[string]any`): each option's value text, or `null`
    /// for a flag; other JSON values survive a round trip.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub options: std::collections::BTreeMap<String, serde_json::Value>,
}

impl std::fmt::Display for ImportParameters {
    /// Go `ImportParameters.String`: its JSON.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&tidb_stringutil::go_json_marshal(self))
    }
}

/// Go `StepSummary`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StepSummary {
    /// Go `Bytes`.
    #[serde(rename = "input-bytes", default, skip_serializing_if = "is_zero")]
    pub bytes: i64,
    /// Go `RowCnt`.
    #[serde(rename = "input-rows", default, skip_serializing_if = "is_zero")]
    pub row_cnt: i64,
}

fn is_zero<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

/// Go `Summary`: a job's summary column. `omitempty` does not omit Go's
/// struct fields, so every step summary is always written.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    /// Go `EncodeSummary`.
    #[serde(rename = "encode-summary", default)]
    pub encode_summary: StepSummary,
    /// Go `MergeSummary`.
    #[serde(rename = "merge-summary", default)]
    pub merge_summary: StepSummary,
    /// Go `IngestSummary`.
    #[serde(rename = "ingest-summary", default)]
    pub ingest_summary: StepSummary,
    /// Go `CollectConflictsSummary`.
    #[serde(rename = "collect-conflicts-summary", default)]
    pub collect_conflicts_summary: StepSummary,
    /// Go `ResolveConflictsSummary`.
    #[serde(rename = "resolve-conflicts-summary", default)]
    pub resolve_conflicts_summary: StepSummary,
    /// Go `ImportedRows`.
    #[serde(rename = "row-count", default, skip_serializing_if = "is_zero")]
    pub imported_rows: i64,
    /// Go `ConflictRowCnt`.
    #[serde(rename = "conflict-row-count", default, skip_serializing_if = "is_zero")]
    pub conflict_row_cnt: u64,
    /// Go `TooManyConflicts`.
    #[serde(rename = "too-many-conflicts", default, skip_serializing_if = "is_zero")]
    pub too_many_conflicts: bool,
}

/// Go `JobInfo`. Go's zero times are `None`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JobInfo {
    pub id: i64,
    pub create_time: Option<Time>,
    pub start_time: Option<Time>,
    pub update_time: Option<Time>,
    pub end_time: Option<Time>,
    pub table_schema: String,
    pub table_name: String,
    pub table_id: i64,
    pub created_by: String,
    pub parameters: ImportParameters,
    pub source_file_size: i64,
    pub status: String,
    pub step: String,
    pub summary: Option<Summary>,
    pub error_message: String,
    pub group_key: String,
}

impl JobInfo {
    /// Go `CanCancel`.
    #[must_use]
    pub fn can_cancel(&self) -> bool {
        self.status == JOB_STATUS_PENDING || self.status == JOB_STATUS_RUNNING
    }
    /// Go `IsCancelled`.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.status == JOB_STATUS_CANCELLED
    }
    /// Go `IsSuccess`.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.status == JOB_STATUS_FINISHED
    }
    /// Go `IsSourceFileSizeUnknown`.
    #[must_use]
    pub fn is_source_file_size_unknown(&self) -> bool {
        if self.source_file_size > 0 {
            return false;
        }
        self.status == JOB_STATUS_PENDING || (self.status == JOB_STATUS_RUNNING && self.step == JOB_STEP_PREPARING)
    }
}

fn exec(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    sql: &str,
    args: &[SqlArg<'_>],
) -> Result<Vec<Vec<Datum>>, DriverError> {
    tidb_sqlexec::execute_sql(ctx, conn, sql, args).map_err(sql_exec_driver_error)
}

fn row_int(row: &[Datum], i: usize) -> i64 {
    match row.get(i) {
        Some(Datum::Int(v)) => *v,
        Some(Datum::UInt(v)) => *v as i64,
        _ => 0,
    }
}

fn row_string(row: &[Datum], i: usize) -> String {
    match row.get(i) {
        None | Some(Datum::Null) => String::new(),
        Some(d) => d.sql_string().unwrap_or_default(),
    }
}

fn row_time(row: &[Datum], i: usize) -> Option<Time> {
    match row.get(i) {
        Some(Datum::Time(t)) if !t.is_zero() => Some(t.clone()),
        _ => None,
    }
}

fn json_error(e: &serde_json::Error) -> DriverError {
    super::import::plain(e.to_string())
}

/// Go `convert2JobInfo`.
fn convert_2_job_info(row: &[Datum]) -> Result<JobInfo, DriverError> {
    let parameters: ImportParameters = serde_json::from_str(&row_string(row, 9)).map_err(|e| json_error(&e))?;
    let summary_str = row_string(row, 13);
    let summary = if summary_str.is_empty() {
        None
    } else {
        Some(serde_json::from_str(&summary_str).map_err(|e| json_error(&e))?)
    };
    Ok(JobInfo {
        id: row_int(row, 0),
        create_time: row_time(row, 1),
        start_time: row_time(row, 2),
        update_time: row_time(row, 3),
        end_time: row_time(row, 4),
        table_schema: row_string(row, 5),
        table_name: row_string(row, 6),
        table_id: row_int(row, 7),
        created_by: row_string(row, 8),
        parameters,
        source_file_size: row_int(row, 10),
        status: row_string(row, 11),
        step: row_string(row, 12),
        summary,
        error_message: row_string(row, 14),
        group_key: row_string(row, 15),
    })
}

/// Go `GetJob`: the job, if `user` created it or has SUPER.
pub fn get_job(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    job_id: i64,
    user: &str,
    has_super_priv: bool,
) -> Result<JobInfo, DriverError> {
    let sql = format!("{BASE_QUERY_SQL} WHERE id = %?");
    let rows = exec(ctx, conn, &sql, &[SqlArg::Signed(job_id)])?;
    if rows.len() != 1 {
        return Err(DriverError::from(
            tidb_exeerrors::ERR_LOAD_DATA_JOB_NOT_FOUND.gen_with_stack_by_args(&[job_id.into()]),
        ));
    }
    let info = convert_2_job_info(&rows[0])?;
    if !has_super_priv && info.created_by != user {
        return Err(DriverError::from(
            tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED.gen_with_stack_by_args(&["SUPER".into()]),
        ));
    }
    Ok(info)
}

/// Go `GetActiveJobCnt`: pending or running jobs on the table.
pub fn get_active_job_cnt(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    table_schema: &str,
    table_name: &str,
) -> Result<i64, DriverError> {
    let sql = "select count(1) from mysql.tidb_import_jobs
			where status in (%?, %?)
				and table_schema = %? and table_name = %?;
			";
    let rows = exec(
        ctx,
        conn,
        sql,
        &[
            SqlArg::String(JOB_STATUS_PENDING.as_bytes()),
            SqlArg::String(JOB_STATUS_RUNNING.as_bytes()),
            SqlArg::String(table_schema.as_bytes()),
            SqlArg::String(table_name.as_bytes()),
        ],
    )?;
    Ok(rows.first().map_or(0, |row| row_int(row, 0)))
}

/// Go `CreateJob`: inserts a pending job and returns its ID.
#[allow(clippy::too_many_arguments)]
pub fn create_job(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    db: &str,
    table: &str,
    table_id: i64,
    user: &str,
    group_key: &str,
    parameters: &ImportParameters,
    source_file_size: i64,
) -> Result<i64, DriverError> {
    let bytes = tidb_stringutil::go_json_marshal(parameters).into_bytes();
    exec(
        ctx,
        conn,
        "INSERT INTO mysql.tidb_import_jobs
		(table_schema, table_name, table_id, group_key, created_by, parameters, source_file_size, status, step)
		VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?);",
        &[
            SqlArg::String(db.as_bytes()),
            SqlArg::String(table.as_bytes()),
            SqlArg::Signed(table_id),
            SqlArg::String(group_key.as_bytes()),
            SqlArg::String(user.as_bytes()),
            SqlArg::Bytes(Some(&bytes)),
            SqlArg::Signed(source_file_size),
            SqlArg::String(JOB_STATUS_PENDING.as_bytes()),
            SqlArg::String(JOB_STEP_NONE.as_bytes()),
        ],
    )?;
    let rows = exec(ctx, conn, "SELECT LAST_INSERT_ID();", &[])?;
    if rows.len() != 1 {
        return Err(super::import::plain(format!("unexpected result length: {}", rows.len())));
    }
    Ok(row_int(&rows[0], 0))
}

/// Go `StartJob`: a pending job starts running at `step`.
pub fn start_job(ctx: &dyn ExecutionContext, conn: &dyn SqlExecutor, job_id: i64, step: &str) -> Result<(), DriverError> {
    exec(
        ctx,
        conn,
        "UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), start_time = CURRENT_TIMESTAMP(6), status = %?, step = %?
		WHERE id = %? AND status = %?;",
        &[SqlArg::String(JOB_STATUS_RUNNING.as_bytes()), SqlArg::String(step.as_bytes()), SqlArg::Signed(job_id), SqlArg::String(JOB_STATUS_PENDING.as_bytes())],
    )
    .map(|_| ())
}

/// Go `Job2Step`.
pub fn job_2_step(ctx: &dyn ExecutionContext, conn: &dyn SqlExecutor, job_id: i64, step: &str) -> Result<(), DriverError> {
    exec(
        ctx,
        conn,
        "UPDATE mysql.tidb_import_jobs
			SET update_time = CURRENT_TIMESTAMP(6), step = %?
			WHERE id = %? AND status = %?;",
        &[SqlArg::String(step.as_bytes()), SqlArg::Signed(job_id), SqlArg::String(JOB_STATUS_RUNNING.as_bytes())],
    )
    .map(|_| ())
}

/// Go `UpdateJobPreparedInfo`: the source size and detected format, once
/// an asynchronously prepared job has listed its files.
pub fn update_job_prepared_info(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    job_id: i64,
    source_file_size: i64,
    format: &str,
) -> Result<(), DriverError> {
    let rows = exec(
        ctx,
        conn,
        "SELECT parameters FROM mysql.tidb_import_jobs
		WHERE id = %? AND status = %?;",
        &[SqlArg::Signed(job_id), SqlArg::String(JOB_STATUS_RUNNING.as_bytes())],
    )?;
    let Some(row) = rows.first() else { return Ok(()) };
    let text = row_string(row, 0);
    let mut parameters = if text.is_empty() {
        ImportParameters::default()
    } else {
        serde_json::from_str(&text).map_err(|e| json_error(&e))?
    };
    if !format.is_empty() {
        parameters.format = format.to_owned();
    }
    let bytes = tidb_stringutil::go_json_marshal(&parameters).into_bytes();
    exec(
        ctx,
        conn,
        "UPDATE mysql.tidb_import_jobs
				SET update_time = CURRENT_TIMESTAMP(6), source_file_size = %?, parameters = %?
				WHERE id = %? AND status = %?;",
        &[
            SqlArg::Signed(source_file_size),
            SqlArg::Bytes(Some(&bytes)),
            SqlArg::Signed(job_id),
            SqlArg::String(JOB_STATUS_RUNNING.as_bytes()),
        ],
    )
    .map(|_| ())
}

fn summary_text(summary: Option<&Summary>) -> String {
    summary.map_or_else(|| "{}".to_owned(), tidb_stringutil::go_json_marshal)
}

/// Go `FinishJob`.
pub fn finish_job(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    job_id: i64,
    summary: Option<&Summary>,
) -> Result<(), DriverError> {
    let summary = summary_text(summary);
    exec(
        ctx,
        conn,
        "UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), end_time = CURRENT_TIMESTAMP(6), status = %?, step = %?, summary = %?
		WHERE id = %? AND status = %?;",
        &[
            SqlArg::String(JOB_STATUS_FINISHED.as_bytes()),
            SqlArg::String(JOB_STEP_NONE.as_bytes()),
            SqlArg::String(summary.as_bytes()),
            SqlArg::Signed(job_id),
            SqlArg::String(JOB_STATUS_RUNNING.as_bytes()),
        ],
    )
    .map(|_| ())
}

/// Go `FailJob`: a pending or running job fails.
pub fn fail_job(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    job_id: i64,
    error_msg: &str,
    summary: Option<&Summary>,
) -> Result<(), DriverError> {
    let summary = summary_text(summary);
    exec(
        ctx,
        conn,
        "UPDATE mysql.tidb_import_jobs
			SET update_time = CURRENT_TIMESTAMP(6), end_time = CURRENT_TIMESTAMP(6), status = %?, error_message = %?, summary = %?
			WHERE id = %? AND status IN (%?, %?);",
        &[
            SqlArg::String(JOB_STATUS_FAILED.as_bytes()),
            SqlArg::String(error_msg.as_bytes()),
            SqlArg::String(summary.as_bytes()),
            SqlArg::Signed(job_id),
            SqlArg::String(JOB_STATUS_PENDING.as_bytes()),
            SqlArg::String(JOB_STATUS_RUNNING.as_bytes()),
        ],
    )
    .map(|_| ())
}

/// Go `getJobInfoFromSQL`.
fn get_job_info_from_sql(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    sql: &str,
    args: &[SqlArg<'_>],
) -> Result<Vec<JobInfo>, DriverError> {
    exec(ctx, conn, sql, args)?.iter().map(|row| convert_2_job_info(row)).collect()
}

/// Go `GetJobsByGroupKey`: the jobs of a group, or of every group when
/// `group_key` is empty.
pub fn get_jobs_by_group_key(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    user: &str,
    group_key: &str,
    has_super_priv: bool,
) -> Result<Vec<JobInfo>, DriverError> {
    let mut clauses = Vec::new();
    let mut args = Vec::new();
    if !has_super_priv {
        clauses.push("created_by = %?");
        args.push(SqlArg::String(user.as_bytes()));
    }
    if group_key.is_empty() {
        clauses.push("GROUP_KEY != ''");
    } else {
        clauses.push("GROUP_KEY = %?");
        args.push(SqlArg::String(group_key.as_bytes()));
    }
    let sql = format!("{BASE_QUERY_SQL} WHERE {}", clauses.join(" AND "));
    get_job_info_from_sql(ctx, conn, &sql, &args)
}

/// Go `GetAllViewableJobs`.
pub fn get_all_viewable_jobs(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    user: &str,
    has_super_priv: bool,
) -> Result<Vec<JobInfo>, DriverError> {
    if has_super_priv {
        get_job_info_from_sql(ctx, conn, BASE_QUERY_SQL, &[])
    } else {
        let sql = format!("{BASE_QUERY_SQL} WHERE created_by = %?");
        get_job_info_from_sql(ctx, conn, &sql, &[SqlArg::String(user.as_bytes())])
    }
}

/// Go `CancelJob`: a pending or running job is cancelled.
pub fn cancel_job(ctx: &dyn ExecutionContext, conn: &dyn SqlExecutor, job_id: i64) -> Result<(), DriverError> {
    cancel_job_in_state(ctx, conn, job_id, &[JOB_STATUS_PENDING, JOB_STATUS_RUNNING])
}

/// Go `CancelPendingJob`.
pub fn cancel_pending_job(ctx: &dyn ExecutionContext, conn: &dyn SqlExecutor, job_id: i64) -> Result<(), DriverError> {
    cancel_job_in_state(ctx, conn, job_id, &[JOB_STATUS_PENDING])
}

/// Go `cancelJobInState`.
fn cancel_job_in_state(
    ctx: &dyn ExecutionContext,
    conn: &dyn SqlExecutor,
    job_id: i64,
    states: &[&str],
) -> Result<(), DriverError> {
    let markers = vec!["%?"; states.len()].join(",");
    let sql = format!(
        "UPDATE mysql.tidb_import_jobs
			SET update_time = CURRENT_TIMESTAMP(6), status = %?, error_message = 'cancelled by user'
			WHERE id = %? AND status IN ({markers});"
    );
    let mut args = vec![SqlArg::String(JOB_STATUS_CANCELLED.as_bytes()), SqlArg::Signed(job_id)];
    args.extend(states.iter().map(|s| SqlArg::String(s.as_bytes())));
    exec(ctx, conn, &sql, &args).map(|_| ())
}
