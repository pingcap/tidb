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

//! Go `pkg/domain/extract.go`: the extract handle that dumps the plans of
//! the statements a statement summary recorded in a time range, with their
//! schemas, statistics, variables and bindings, into the extract task
//! directory (which the dump-file GC also walks).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Mutex;

use base64::engine::general_purpose::URL_SAFE;
use base64::Engine as _;
use chrono::{DateTime, FixedOffset};
use serde::Serialize;
use tidb_ast::Visitable as _;

use crate::plan_replayer::{
    build_config_toml, build_meta_txt, build_tiflash_replica_file, build_variables_records,
    dump_file, dump_global_bindings, dump_schemas, dump_stats, flat_toml, DumpArchiveWriter,
    ExtractSchemaSource, ExtractViewParser, PlanReplayerError, PlanReplayerSessionVars,
    PlanReplayerSessionContext, StatsDumpSource, TableNameExtractor, TableNamePair,
    TiFlashReplicaSource, VariablesDumpSource,
};

/// Go `ExtractMetaFile` indicates meta file for extract.
pub const EXTRACT_META_FILE: &str = "extract_meta.txt";

/// Go `ExtractTaskType` indicates type of extract task.
pub const EXTRACT_TASK_TYPE: &str = "taskType";
/// Go `ExtractPlanTaskSkipStats` indicates skip stats for extract plan task.
pub const EXTRACT_PLAN_TASK_SKIP_STATS: &str = "SkipStats";
/// Go `ExtractTaskDirName` indicates directory name for extract task.
pub const EXTRACT_TASK_DIR_NAME: &str = "extract";

/// Go `ExtractType` indicates type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExtractType {
    /// Go `ExtractPlanType` indicates extract plan task.
    #[default]
    Plan,
}

fn task_type_to_string(t: ExtractType) -> &'static str {
    match t {
        ExtractType::Plan => "Plan",
    }
}

/// Go `ExtractTask` indicates task.
#[derive(Clone, Debug)]
pub struct ExtractTask {
    /// Go `ExtractType`.
    pub extract_type: ExtractType,
    /// Go `IsBackgroundJob`.
    pub is_background_job: bool,
    // Param for Extract Plan
    /// Go `SkipStats`.
    pub skip_stats: bool,
    /// Go `UseHistoryView`.
    pub use_history_view: bool,
    // variables for plan task type
    /// Go's `time.Time`, formatted in its own location.
    pub begin: DateTime<FixedOffset>,
    /// Go `End`.
    pub end: DateTime<FixedOffset>,
}

/// Go `NewExtractPlanTask` returns extract plan task.
#[must_use]
pub fn new_extract_plan_task(
    begin: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
) -> ExtractTask {
    ExtractTask {
        extract_type: ExtractType::Plan,
        is_background_job: false,
        skip_stats: false,
        use_history_view: false,
        begin,
        end,
    }
}

/// What the extract worker reaches through its session: Go's
/// `sctx.GetRestrictedSQLExecutor()`, `GetDomain(sctx).InfoSchema()`,
/// `StatsHandle()` and `GetSessionVars()`, gathered as the plan replayer's
/// dump seams.
pub struct ExtractSeams<'a> {
    /// Go `sctx.GetRestrictedSQLExecutor()` / `GetSQLExecutor()`.
    pub sql: &'a dyn PlanReplayerSessionContext,
    /// Go `GetDomain(sctx).InfoSchema()`.
    pub extract_schema: &'a dyn ExtractSchemaSource,
    /// Go `ParseWithParams` for a view's SELECT.
    pub view_parser: &'a dyn ExtractViewParser,
    /// Go `GetDomain(sctx).StatsHandle()`.
    pub stats: &'a dyn StatsDumpSource,
    /// Go `dumpTiFlashReplica`'s info schema.
    pub tiflash: &'a dyn TiFlashReplicaSource,
    /// Go `variable.GetSysVars()` for the session.
    pub variables: &'a dyn VariablesDumpSource,
    /// Go `sctx.GetSessionVars()`.
    pub session_vars: PlanReplayerSessionVars,
}

/// Go `extractWorker.sctx`: the session the worker runs on, which lends the
/// seams over its latest schema for one task.
pub trait ExtractSessionContext {
    /// Runs `run` over this session's seams.
    ///
    /// # Errors
    /// `run`'s error, or the session's failure to lend its seams.
    fn with_seams(
        &mut self,
        run: &mut dyn FnMut(&ExtractSeams<'_>) -> Result<String, PlanReplayerError>,
    ) -> Result<String, PlanReplayerError>;
}

/// Go `ExtractHandle` handles the extractWorker to run extract the
/// information task like Plan or any others.
pub struct ExtractHandle {
    worker: ExtractWorker,
}

impl ExtractHandle {
    /// Go `newExtractHandler`.
    #[must_use]
    pub fn new(sctx: Box<dyn ExtractSessionContext + Send>) -> Self {
        Self {
            worker: ExtractWorker::new(sctx, false),
        }
    }

    /// Go `ExtractHandle.ExtractTask` extract tasks.
    ///
    /// # Errors
    /// The extract task's error.
    pub fn extract_task(&self, task: &ExtractTask) -> Result<String, PlanReplayerError> {
        // TODO: support background job later
        if task.is_background_job {
            return Ok(String::new());
        }
        self.worker.extract_task(task)
    }
}

struct ExtractWorker {
    sctx: Mutex<Box<dyn ExtractSessionContext + Send>>,
    #[allow(dead_code)]
    is_background_worker: bool,
}

/// Go `stmtSummaryHistoryKey`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct StmtSummaryHistoryKey {
    digest: String,
    plan_digest: String,
}

/// Go `stmtSummaryHistoryRecord`.
#[derive(Clone, Debug, Default)]
struct StmtSummaryHistoryRecord {
    stmt_type: String,
    schema_name: String,
    tables: Vec<TableNamePair>,
    digest: String,
    plan_digest: String,
    sql: String,
    binary_plan: String,
    user_name: String,

    plan: String,
    skip: bool,
}

/// Go `extractPlanPackage`.
struct ExtractPlanPackage {
    tables: BTreeSet<TableNamePair>,
    records: HashMap<StmtSummaryHistoryKey, StmtSummaryHistoryRecord>,
}

impl ExtractWorker {
    fn new(sctx: Box<dyn ExtractSessionContext + Send>, is_background_worker: bool) -> Self {
        Self {
            sctx: Mutex::new(sctx),
            is_background_worker,
        }
    }

    fn extract_task(&self, task: &ExtractTask) -> Result<String, PlanReplayerError> {
        match task.extract_type {
            ExtractType::Plan => self.extract_plan_task(task),
        }
    }

    fn extract_plan_task(&self, task: &ExtractTask) -> Result<String, PlanReplayerError> {
        if task.use_history_view
            && !tidb_config::config_tree::config::get_global_config()
                .instance
                .stmt_summary_enable_persistent
        {
            return Err(PlanReplayerError::Other(
                "tidb_stmt_summary_enable_persistent should be enabled for extract task".to_owned(),
            ));
        }
        let mut sctx = self
            .sctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        sctx.with_seams(&mut |sctx| {
            let records = collect_records(sctx, task).inspect_err(|error| {
                tidb_logutil::bg_logger().error(
                    "collect stmt summary records failed for extract plan task",
                    &[tidb_logutil::zap_error(error.to_string())],
                );
            })?;
            let p = package_extract_plan_records(sctx, records).inspect_err(|error| {
                tidb_logutil::bg_logger().error(
                    "package stmt summary records failed for extract plan task",
                    &[tidb_logutil::zap_error(error.to_string())],
                );
            })?;
            dump_extract_plan_package(sctx, task, &p)
        })
    }
}

fn collect_records(
    sctx: &ExtractSeams<'_>,
    task: &ExtractTask,
) -> Result<HashMap<StmtSummaryHistoryKey, StmtSummaryHistoryRecord>, PlanReplayerError> {
    let source_table = if task.use_history_view {
        "STATEMENTS_SUMMARY_HISTORY"
    } else {
        "STATEMENTS_SUMMARY"
    };
    let (rows, _) = sctx
        .sql
        .get_restricted_sql_executor()
        .exec_restricted_sql(
            &crate::plan_replayer::plan_replayer_ctx(),
            &[],
            &format!(
                "SELECT STMT_TYPE, DIGEST, PLAN_DIGEST,QUERY_SAMPLE_TEXT, BINARY_PLAN, TABLE_NAMES, SAMPLE_USER FROM INFORMATION_SCHEMA.{source_table} WHERE SUMMARY_END_TIME > '{}' AND SUMMARY_BEGIN_TIME < '{}'",
                task.begin.format(TIME_FORMAT),
                task.end.format(TIME_FORMAT)
            ),
            &[],
        )
        .map_err(crate::plan_replayer::sql_exec_error)?;
    let mut collect_map = HashMap::new();
    for row in rows {
        let get = |i: usize| get_string(row.get(i));
        let mut record = StmtSummaryHistoryRecord {
            stmt_type: get(0),
            digest: get(1),
            plan_digest: get(2),
            sql: get(3),
            binary_plan: get(4),
            user_name: get(6),
            ..StmtSummaryHistoryRecord::default()
        };
        let table_names = get(5);
        let key = StmtSummaryHistoryKey {
            digest: record.digest.clone(),
            plan_digest: record.plan_digest.clone(),
        };
        let set_record = handle_table_names(sctx, &table_names, &mut record);
        if set_record && check_record_valid(&record) {
            collect_map.insert(key, record);
        }
    }
    Ok(collect_map)
}

/// Go `types.TimeFormat`.
const TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

fn handle_table_names(
    sctx: &ExtractSeams<'_>,
    table_names: &str,
    record: &mut StmtSummaryHistoryRecord,
) -> bool {
    let is = sctx.extract_schema;
    for t in table_names.split(',') {
        let names: Vec<&str> = t.split('.').collect();
        if names.len() != 2 {
            return false;
        }
        let (db_name, tbl_name) = (names[0], names[1]);
        record.schema_name = db_name.to_owned();
        // skip internal schema record
        match record.schema_name.to_lowercase().as_str() {
            "performance_schema" | "information_schema" | "metrics_schema" | "mysql" => {
                return false
            }
            _ => {}
        }
        if !is.table_exists(db_name, tbl_name) {
            return false;
        }
        record.tables.push(TableNamePair {
            db_name: db_name.to_owned(),
            table_name: tbl_name.to_owned(),
            is_view: is.is_view(db_name, tbl_name),
        });
    }
    true
}

fn check_record_valid(r: &StmtSummaryHistoryRecord) -> bool {
    if r.stmt_type != "Select" {
        return false;
    }
    if r.schema_name.is_empty() {
        return false;
    }
    if r.plan_digest.is_empty() {
        return false;
    }
    true
}

fn package_extract_plan_records(
    sctx: &ExtractSeams<'_>,
    mut records: HashMap<StmtSummaryHistoryKey, StmtSummaryHistoryRecord>,
) -> Result<ExtractPlanPackage, PlanReplayerError> {
    let mut tables = BTreeSet::new();
    for record in records.values_mut() {
        // skip the sql which has been cut off
        if record.sql.contains("(len:") {
            record.skip = true;
            continue;
        }
        record.plan = decode_binary_plan(sctx, &record.binary_plan)?;
        for tbl in &record.tables {
            tables.insert(tbl.clone());
        }
    }
    let mut p = ExtractPlanPackage { tables, records };
    handle_is_view(sctx, &mut p)?;
    Ok(p)
}

fn handle_is_view(
    sctx: &ExtractSeams<'_>,
    p: &mut ExtractPlanPackage,
) -> Result<(), PlanReplayerError> {
    let is = sctx.extract_schema;
    let mut tne = TableNameExtractor {
        schema: is,
        parser: sctx.view_parser,
        cur_db: String::new(),
        names: BTreeSet::new(),
        cte_names: HashSet::new(),
        err: None,
    };
    for v in &p.tables {
        if v.is_view {
            let Some(sql) = is.view_select_stmt(&v.db_name, &v.table_name) else {
                return Err(PlanReplayerError::Other(format!(
                    "[schema:1146]Table '{}.{}' doesn't exist",
                    v.db_name, v.table_name
                )));
            };
            let mut node = sctx.view_parser.parse(&sql)?;
            node.accept(&mut tne);
        }
    }
    if let Some(error) = tne.err.take() {
        return Err(error);
    }
    let r = tne.get_tables_and_views()?;
    p.tables.extend(r);
    Ok(())
}

/// Go `row.GetString(i)`: a NULL reads as the empty string.
fn get_string(datum: Option<&tidb_datatype::Datum>) -> String {
    match datum {
        None | Some(tidb_datatype::Datum::Null) => String::new(),
        Some(datum) => datum.sql_string().unwrap_or_default(),
    }
}

fn decode_binary_plan(sctx: &ExtractSeams<'_>, b_plan: &str) -> Result<String, PlanReplayerError> {
    let (rows, _) = sctx
        .sql
        .get_restricted_sql_executor()
        .exec_restricted_sql(
            &crate::plan_replayer::plan_replayer_ctx(),
            &[],
            &format!("SELECT tidb_decode_binary_plan('{b_plan}')"),
            &[],
        )
        .map_err(crate::plan_replayer::sql_exec_error)?;
    let plan = get_string(rows.first().and_then(|row| row.first()));
    Ok(plan.trim_matches('\n').to_owned())
}

/// The archive the extract task writes, buffered until Go's deferred close.
struct ArchiveWriter(tidb_replayer::zip::ZipWriter<Vec<u8>>);

impl DumpArchiveWriter for ArchiveWriter {
    fn create_file(&mut self, name: &str, contents: &[u8]) -> Result<(), PlanReplayerError> {
        self.0
            .create_file(name, contents)
            .map_err(|error| PlanReplayerError::Other(error.to_string()))
    }
}

/// Go `dumpExtractPlanPackage` will dump the information about sqls
/// collected in stmt_summary_history.
fn dump_extract_plan_package(
    sctx: &ExtractSeams<'_>,
    task: &ExtractTask,
    p: &ExtractPlanPackage,
) -> Result<String, PlanReplayerError> {
    let (mut f, name) = generate_extract_file()?;
    let mut zw = ArchiveWriter(tidb_replayer::zip::ZipWriter::new(Vec::new()));
    let result = (|| -> Result<(), PlanReplayerError> {
        // Dump config
        zw.create_file(
            dump_file::PLAN_REPLAYER_CONFIG_FILE,
            build_config_toml().unwrap_or_default().as_bytes(),
        )?;
        // Dump meta
        zw.create_file(
            dump_file::PLAN_REPLAYER_META_FILE,
            build_meta_txt().as_bytes(),
        )?;
        // dump extract plan task meta
        dump_extract_meta(task, &mut zw)?;
        // Dump Schema and View
        dump_schemas(&p.tables, &mut zw, sctx.sql)?;
        // Dump tables tiflash replicas
        zw.create_file(
            dump_file::PLAN_REPLAYER_TIFLASH_REPLICAS_FILE,
            build_tiflash_replica_file(sctx.tiflash, &p.tables).as_bytes(),
        )?;
        // Dump variables
        let variables = build_variables_records(sctx.variables, &sctx.session_vars)
            .map_err(PlanReplayerError::Other)?;
        zw.create_file(
            dump_file::PLAN_REPLAYER_VARIABLES_FILE,
            flat_toml(&variables).as_bytes(),
        )?;
        // Dump global bindings
        dump_global_bindings(sctx.sql, &mut zw)?;
        // Dump stats
        if !task.skip_stats {
            dump_stats(sctx.stats, &p.tables, 0, &mut zw)?;
        }
        // Dump sqls and plan
        dump_sql_records(&p.records, &mut zw)
    })();
    if let Err(error) = &result {
        tidb_logutil::bg_logger().error(
            "dump extract plan task failed",
            &[tidb_logutil::zap_error(error.to_string())],
        );
    }
    // Go's deferred `zw.Close` and `f.Close`.
    let mut bytes = Vec::new();
    match zw.0.close() {
        Ok(()) => bytes = zw.0.into_inner(),
        Err(error) => tidb_logutil::bg_logger().warn(
            "close zip writer failed",
            &[
                tidb_log::Field::new("file", tidb_log::Value::Str(name.clone())),
                tidb_logutil::zap_error(error.to_string()),
            ],
        ),
    }
    if let Err(error) = f.write(&bytes).and_then(|_| f.close()) {
        tidb_logutil::bg_logger().warn(
            "close file failed",
            &[
                tidb_log::Field::new("file", tidb_log::Value::Str(name.clone())),
                tidb_logutil::zap_error(error.to_string()),
            ],
        );
    }
    result.map(|()| name)
}

fn dump_sql_records(
    records: &HashMap<StmtSummaryHistoryKey, StmtSummaryHistoryRecord>,
    zw: &mut dyn DumpArchiveWriter,
) -> Result<(), PlanReplayerError> {
    for (key, record) in records {
        if record.skip {
            dump_sql_record(record, &format!("skippedSQLs/{}.json", key.digest), zw)?;
        } else {
            dump_sql_record(record, &format!("SQLs/{}.json", key.digest), zw)?;
        }
    }
    Ok(())
}

/// Go `singleSQLRecord`.
#[derive(Serialize)]
struct SingleSqlRecord<'a> {
    schema: &'a str,
    plan: &'a str,
    sql: &'a str,
    digest: &'a str,
    #[serde(rename = "binaryPlan")]
    binary_plan: &'a str,
    #[serde(rename = "userName")]
    user_name: &'a str,
}

/// Go `dumpSQLRecord` dumps sql records into one file for each record, the
/// format is in json.
fn dump_sql_record(
    record: &StmtSummaryHistoryRecord,
    path: &str,
    zw: &mut dyn DumpArchiveWriter,
) -> Result<(), PlanReplayerError> {
    let single_sql_record = SingleSqlRecord {
        schema: &record.schema_name,
        plan: &record.plan,
        sql: &record.sql,
        digest: &record.digest,
        binary_plan: &record.binary_plan,
        user_name: &record.user_name,
    };
    let content = serde_json::to_string(&single_sql_record)
        .map_err(|error| PlanReplayerError::Other(error.to_string()))?;
    // Go `json.Marshal`'s HTML escaping.
    zw.create_file(
        path,
        tidb_stringutil::go_json_html_escape(&content).as_bytes(),
    )
}

fn dump_extract_meta(
    task: &ExtractTask,
    zw: &mut dyn DumpArchiveWriter,
) -> Result<(), PlanReplayerError> {
    let mut var_map = BTreeMap::new();
    var_map.insert(
        EXTRACT_TASK_TYPE.to_owned(),
        task_type_to_string(task.extract_type).to_owned(),
    );
    if task.extract_type == ExtractType::Plan {
        var_map.insert(
            EXTRACT_PLAN_TASK_SKIP_STATS.to_owned(),
            task.skip_stats.to_string(),
        );
    }
    zw.create_file(EXTRACT_META_FILE, flat_toml(&var_map).as_bytes())
}

/// Go `GenerateExtractFile` generates extract stmt file.
fn generate_extract_file(
) -> Result<(Box<dyn tidb_replayer::WriteCloser>, String), PlanReplayerError> {
    let path = get_extract_task_dir_name();
    let file_name = generate_extract_stmt_file()?;
    let storage =
        tidb_planner_extstore::get_global_ext_storage().map_err(PlanReplayerError::Other)?;
    let writer = tidb_replayer::Storage::create(
        storage.as_ref(),
        &std::path::Path::new(path).join(&file_name),
    )
    .map_err(|error| PlanReplayerError::Other(error.to_string()))?;
    let zf = tidb_replayer::new_file_writer(writer);
    Ok((Box::new(zf), file_name))
}

fn generate_extract_stmt_file() -> Result<String, PlanReplayerError> {
    // Generate key and create zip file
    let time = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as i64,
        Err(error) => -(error.duration().as_nanos() as i64),
    };
    let mut b = [0_u8; 16];
    getrandom::fill(&mut b).map_err(|error| PlanReplayerError::Other(error.to_string()))?;
    let key = URL_SAFE.encode(b);
    Ok(format!("extract_{key}_{time}.zip"))
}

/// Go `GetExtractTaskDirName` get extract dir name.
#[must_use]
pub const fn get_extract_task_dir_name() -> &'static str {
    EXTRACT_TASK_DIR_NAME
}
