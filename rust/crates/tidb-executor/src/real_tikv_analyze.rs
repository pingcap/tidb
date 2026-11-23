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

//! Cluster ANALYZE sampling and statistics persistence.
//!
//! SessionExtendedInfoSchema selection and previous statistics are read at the sampling TSO.
//! Coprocessor sampling follows Go's analyze snapshot setting: snapshot reads
//! use that TSO, while the default reads use RC at the latest version.
//! Statistics are saved in a separate transaction; its version and the
//! sampling snapshot serve distinct purposes in concurrent-ANALYZE checks.

use std::collections::HashSet;
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tidb_mysql::errors::SqlError;
use tidb_stats::row_sample_collector::get_adjusted_sample_rate;
use tidb_stats_handle_globalstats::global_stats::GlobalStatsInfo;
use tidb_store_driver_txn::transaction::StoreClient;
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;
use tidb_sqlkiller::SqlKiller;

use crate::cluster_analyze::{analyze_independent_index_with_progress, AnalyzeError, AnalyzeStatement};
use tidb_planner::plan_builder::analyze::{
    Analyze, AnalyzeColumnsTask, AnalyzeIndexTask, AnalyzeOptionType, AnalyzeOpts, AnalyzeTableID,
};
use tidb_stats_handle_storage::save::{AnalyzeResults as AnalyzeResults_, STATS_VERSION_2};
use tidb_stats_handle_storage::stats_read_writer::{self, StatsSessionPool};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_sqlescape::SqlArg;
use tidb_store_driver_error::pessimistic_lock_error::LockSqlError;
pub use crate::analyze::sample_rate_reason;
use tidb_pdhelper::ApproximateTableCountProvider;

const ANALYZE_JOB_MAX_DELTA: i64 = 10_000_000;
const ANALYZE_JOB_DUMP_INTERVAL: Duration = Duration::from_secs(5);


/// One row Go publishes in `mysql.analyze_jobs` before dispatching the
/// corresponding physical analyze task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeJobSpec {
    /// Database name stored in `table_schema`.
    pub schema: String,
    /// Logical table name stored in `table_name`.
    pub table: String,
    /// Physical partition name, or empty for a nonpartition/global job.
    pub partition: String,
    /// Go-compatible human-readable task description.
    pub job_info: String,
}

/// Whether finishing a job also flushes its residual processed-row count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyzeJobKind {
    /// A table, partition, or independent-index analysis.
    Table,
    /// A partition-to-global statistics merge.
    GlobalStatsMerge,
}

/// Storage boundary used by the analyzer for Go's job lifecycle. Persistence
/// errors are deliberately swallowed by implementations: Go logs them and
/// never changes the result of `ANALYZE`.
pub trait AnalyzeJobLifecycle: Send + Sync {
    /// Inserts a pending job and returns its generated ID when bookkeeping succeeds.
    fn insert(&self, spec: &AnalyzeJobSpec) -> Option<u64>;
    /// Marks a pending job running.
    fn start(&self, job_id: u64);
    /// Persists an executor-throttled processed-row delta.
    fn update_progress(&self, job_id: u64, processed_rows: i64);
    /// Finishes or fails a job, flushing residual table-job progress.
    fn finish(&self, job_id: u64, processed_rows: i64, failure: Option<&str>, kind: AnalyzeJobKind);
}

/// Go `statistics.AnalyzeProgress`: progress writes are throttled by both a
/// row threshold and elapsed time, while finish consumes the residual count.
struct AnalyzeJobProgress {
    state: Mutex<AnalyzeJobProgressState>,
}

struct AnalyzeJobProgressState {
    last_dump: Instant,
    delta: i64,
}

impl AnalyzeJobProgress {
    fn started() -> Self {
        Self {
            state: Mutex::new(AnalyzeJobProgressState {
                last_dump: Instant::now(),
                delta: 0,
            }),
        }
    }

    fn update(&self, row_count: i64) -> i64 {
        let mut state = self.state.lock().expect("analyze job progress lock");
        state.delta = state.delta.saturating_add(row_count);
        let now = Instant::now();
        if state.delta > ANALYZE_JOB_MAX_DELTA
            && now.duration_since(state.last_dump) > ANALYZE_JOB_DUMP_INTERVAL
        {
            let delta = state.delta;
            state.delta = 0;
            state.last_dump = now;
            delta
        } else {
            0
        }
    }

    fn start(&self) {
        self.state
            .lock()
            .expect("analyze job progress lock")
            .last_dump = Instant::now();
    }

    fn residual(&self) -> i64 {
        self.state.lock().expect("analyze job progress lock").delta
    }
}

pub(crate) struct AnalyzeJobHandle {
    id: Option<u64>,
    progress: AnalyzeJobProgress,
}

impl AnalyzeJobHandle {
    pub(crate) fn pending(jobs: &dyn AnalyzeJobLifecycle, spec: &AnalyzeJobSpec) -> Self {
        Self {
            id: jobs.insert(spec),
            progress: AnalyzeJobProgress::started(),
        }
    }

    pub(crate) fn start(&self, jobs: &dyn AnalyzeJobLifecycle) {
        self.progress.start();
        if let Some(id) = self.id {
            jobs.start(id);
        }
    }

    fn update(&self, jobs: &dyn AnalyzeJobLifecycle, row_count: i64) {
        let delta = self.progress.update(row_count);
        if delta == 0 {
            return;
        }
        if let Some(id) = self.id {
            jobs.update_progress(id, delta);
        }
    }

    pub(crate) fn finish(&self, jobs: &dyn AnalyzeJobLifecycle, failure: Option<&str>, kind: AnalyzeJobKind) {
        if let Some(id) = self.id {
            jobs.finish(id, self.progress.residual(), failure, kind);
        }
    }
}

/// Go `collectStatsDeltaFlushObjectsForAnalyze`'s input: the
/// `(DBName, TableName)` of each column task, whose deltas are flushed before
/// the tasks read `mysql.stats_meta`.
#[must_use]
pub fn analyze_col_task_tables(plan: &Analyze) -> Vec<(String, String)> {
    plan.col_tasks
        .iter()
        .map(|task| (task.analyze_info.db_name.clone(), task.analyze_info.table_name.clone()))
        .collect()
}

/// Go `executorBuilder.buildAnalyze`: every column task
/// (`buildAnalyzeSamplingPushdown`), then every index task
/// (`buildAnalyzeIndexPushdown`). The sample-rate notes are appended to
/// `notes`.
#[allow(clippy::too_many_arguments)]
fn build_analyze<'a>(
    plan: &'a Analyze,
    vars: &AnalyzeStatement,
    stats: &tidb_stats_handle_syncload::stats_watch::SharedStats,
    approximate_counts: &dyn ApproximateTableCountProvider,
    resource_group: &str,
    stats_pool: &dyn StatsSessionPool,
    notes: &mut Vec<(bool, u16, String)>,
) -> Result<Vec<AnalyzeTask<'a>>, ClusterAnalyzeError> {
    let mut tasks = Vec::with_capacity(plan.col_tasks.len() + plan.idx_tasks.len());
    for task in &plan.col_tasks {
        tasks.push(build_analyze_sampling_pushdown(
            plan,
            task,
            vars,
            stats,
            approximate_counts,
            resource_group,
            stats_pool,
            notes,
        )?);
    }
    for task in &plan.idx_tasks {
        tasks.push(build_analyze_index_pushdown(plan, task, vars));
    }
    Ok(tasks)
}

/// The executor's options from Go's filled option map.
fn analyze_options_of(opts: &AnalyzeOpts) -> crate::analyze::AnalyzeOptions {
    let get = |opt| opts.get(&opt).copied().unwrap_or_default();
    let num_samples = get(AnalyzeOptionType::NumSamples);
    let sample_rate = f64::from_bits(get(AnalyzeOptionType::SampleRate));
    crate::analyze::AnalyzeOptions {
        num_buckets: get(AnalyzeOptionType::NumBuckets) as isize,
        num_topn: get(AnalyzeOptionType::NumTopN) as isize,
        num_samples: num_samples as usize,
        sample_rate: (num_samples == 0 && sample_rate >= 0.0).then_some(sample_rate),
    }
}

/// Go `buildAnalyzeSamplingPushdown`.
#[allow(clippy::too_many_arguments)]
fn build_analyze_sampling_pushdown<'a>(
    plan: &'a Analyze,
    task: &'a AnalyzeColumnsTask,
    vars: &AnalyzeStatement,
    stats: &tidb_stats_handle_syncload::stats_watch::SharedStats,
    approximate_counts: &dyn ApproximateTableCountProvider,
    resource_group: &str,
    stats_pool: &dyn StatsSessionPool,
    notes: &mut Vec<(bool, u16, String)>,
) -> Result<AnalyzeTask<'a>, ClusterAnalyzeError> {
    let info = &task.analyze_info;
    let opts = info.v2_options.as_ref().map_or(&plan.opts, |options| &options.filled_opts);
    let (count, modify_count, no_meta) = stats_meta_count_and_modify_count(stats_pool, info.table_id.get_statistics_id())?;
    let mut options = analyze_options_of(opts);
    if options.num_samples == 0 && options.sample_rate.is_none() {
        // Go `getAdjustedSampleRate`.
        let tbl_info = task.tbl_info.read();
        let physical_id = info.table_id.get_statistics_id();
        let pseudo_cacheable = tbl_info.temp_table_type == tidb_model::TempTableType::NONE
            && !tidb_filter::is_system_schema(&tidb_hack::go_to_lower(&info.db_name));
        let realtime_count = stats
            .get_physical_table_stats(physical_id, tbl_info.partition.is_some(), pseudo_cacheable, || {
                tidb_stats::pseudo_table(&tbl_info, false, true)
            })
            .map_or(tidb_stats::PSEUDO_ROW_COUNT, |cached| cached.hist_coll.realtime_count);
        let (rate, reason) = automatic_sample_rate(
            Some(realtime_count),
            approximate_counts,
            resource_group,
            physical_id,
            &info.db_name,
            &info.table_name,
            &info.partition_name,
        );
        options.sample_rate = Some(rate);
        let message = if info.partition_name.is_empty() {
            format!(
                "Analyze use auto adjusted sample rate {rate:.6} for table {}.{}, reason to use this rate is \"{reason}\"",
                info.db_name, info.table_name
            )
        } else {
            format!(
                "Analyze use auto adjusted sample rate {rate:.6} for table {}.{}'s partition {}, reason to use this rate is \"{reason}\"",
                info.db_name, info.table_name, info.partition_name
            )
        };
        notes.push((true, 1105, message));
    }
    let mut statement = vars.clone();
    statement.schema = info.db_name.clone();
    statement.table = info.table_name.clone();
    statement.options = options;
    let selected_columns =
        (!task.all_columns).then(|| task.cols_info.iter().map(|column| column.read().id).collect::<HashSet<_>>());
    let job = AnalyzeJobSpec {
        schema: info.db_name.clone(),
        table: info.table_name.clone(),
        partition: info.partition_name.clone(),
        job_info: prepare_analyze_columns_job_info(vars.auto_analyze, task, &options),
    };
    Ok(AnalyzeTask {
        kind: AnalyzeTaskKind::Col(task),
        table_id: info.table_id,
        job,
        statement,
        selected_columns,
        base_count: count,
        base_modify_count: modify_count,
        realtime_count: (!no_meta).then_some(count),
    })
}

/// Go `buildAnalyzeIndexPushdown`.
fn build_analyze_index_pushdown<'a>(plan: &'a Analyze, task: &'a AnalyzeIndexTask, vars: &AnalyzeStatement) -> AnalyzeTask<'a> {
    let info = &task.analyze_info;
    let auto_analyze = if vars.auto_analyze { "auto " } else { "" };
    let mut statement = vars.clone();
    statement.schema = info.db_name.clone();
    statement.table = info.table_name.clone();
    statement.options = analyze_options_of(&plan.opts);
    AnalyzeTask {
        kind: AnalyzeTaskKind::Idx(task),
        table_id: info.table_id,
        job: AnalyzeJobSpec {
            schema: info.db_name.clone(),
            table: info.table_name.clone(),
            partition: info.partition_name.clone(),
            job_info: format!("{auto_analyze}analyze index {}", task.index_info.read().name.original()),
        },
        statement,
        selected_columns: None,
        base_count: 0,
        base_modify_count: 0,
        realtime_count: None,
    }
}

/// Go `prepareAnalyzeColumnsJobInfo` with `prepareIndexes` and
/// `prepareColumns`.
fn prepare_analyze_columns_job_info(
    auto_analyze: bool,
    task: &AnalyzeColumnsTask,
    options: &crate::analyze::AnalyzeOptions,
) -> String {
    let tbl_info = task.tbl_info.read();
    let mut b = String::new();
    if auto_analyze {
        b.push_str("auto ");
    }
    b.push_str("analyze table");
    // Go `prepareIndexes`.
    let indexes = &task.indexes;
    if !indexes.is_empty() {
        if indexes.len() < tbl_info.indices.len() {
            b.push_str(if indexes.len() > 1 { " indexes " } else { " index " });
            b.push_str(
                &indexes
                    .iter()
                    .map(|index| index.read().name.original().to_owned())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        } else {
            b.push_str(" all indexes");
        }
    }
    let cols = task
        .cols_info
        .iter()
        .filter(|col| col.read().id != tidb_model::column::EXTRA_HANDLE_ID)
        .collect::<Vec<_>>();
    if !indexes.is_empty() && !task.cols_info.is_empty() {
        b.push(',');
    }
    // Go `prepareColumns`.
    if !cols.is_empty() {
        let filtered_cols = cols
            .iter()
            .filter(|col| {
                let col = col.read();
                !col.is_changing() && !col.is_removing()
            })
            .collect::<Vec<_>>();
        if filtered_cols.len() < tbl_info.get_non_temp_columns().len() {
            b.push_str(if cols.len() > 1 { " columns " } else { " column " });
            b.push_str(
                &filtered_cols
                    .iter()
                    .map(|col| col.read().name.original().to_owned())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        } else {
            b.push_str(" all columns");
        }
    }
    b.push_str(&format!(" with {} buckets, {} topn, ", options.num_buckets, options.num_topn));
    if options.num_samples != 0 {
        b.push_str(&format!("{} samples", options.num_samples));
    } else {
        b.push_str(&format!("{} samplerate", options.sample_rate.unwrap_or(1.0)));
    }
    b
}

fn automatic_sample_rate(
    realtime_count: Option<i64>,
    approximate_counts: &dyn ApproximateTableCountProvider,
    resource_group: &str,
    physical_id: i64,
    database: &str,
    table: &str,
    partition: &str,
) -> (f64, String) {
    let (approximate_count, has_pd) = approximate_counts.approximate_table_count(
        resource_group,
        physical_id,
        database,
        table,
        partition,
    );
    let rate = get_adjusted_sample_rate(realtime_count, has_pd.then_some(approximate_count));
    let reason = sample_rate_reason(realtime_count, has_pd.then_some(approximate_count), rate);
    (rate, reason)
}

/// Go `AnalyzeExec.saveAnalyzeOptions`: one `REPLACE INTO
/// mysql.analyze_options` of the statement's options (in dynamic prune mode
/// only the tables').
///
/// # Errors
///
/// The statement's failure, which Go reports as a warning.
pub fn save_analyze_options(
    stats_pool: &dyn StatsSessionPool,
    vars: &AnalyzeStatement,
    options_map: &std::collections::BTreeMap<i64, tidb_planner::plan_builder::analyze::V2AnalyzeOptions>,
) -> Result<(), SqlQueryError> {
    if !vars.persist_options || options_map.is_empty() {
        return Ok(());
    }
    // Only to save table options if dynamic prune mode.
    let dynamic_prune = vars.dynamic_partition_prune;
    let to_save = options_map
        .values()
        .filter(|opts| !opts.is_partition || !dynamic_prune)
        .collect::<Vec<_>>();
    let mut sql = String::from(
        "REPLACE INTO mysql.analyze_options (table_id,sample_num,sample_rate,buckets,topn,column_choice,column_ids) VALUES ",
    );
    let col_ids = to_save
        .iter()
        .map(|opts| {
            opts.column_list
                .iter()
                .map(|col| col.read().id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>();
    let mut args = Vec::with_capacity(to_save.len() * 7);
    for (idx, opts) in to_save.iter().enumerate() {
        let raw = &opts.raw_opts;
        let sample_num = raw.get(&AnalyzeOptionType::NumSamples).copied().unwrap_or_default();
        let sample_rate = raw.get(&AnalyzeOptionType::SampleRate).map_or(0.0, |value| f64::from_bits(*value));
        let buckets = raw.get(&AnalyzeOptionType::NumBuckets).copied().unwrap_or_default();
        let topn = raw.get(&AnalyzeOptionType::NumTopN).map_or(-1, |value| *value as i64);
        sql.push_str("(%?,%?,%?,%?,%?,%?,%?)");
        if idx < to_save.len() - 1 {
            sql.push(',');
        }
        args.push(SqlArg::Signed(opts.phy_table_id));
        args.push(SqlArg::Unsigned(sample_num));
        args.push(SqlArg::Float64(sample_rate));
        args.push(SqlArg::Unsigned(buckets));
        args.push(SqlArg::Signed(topn));
        args.push(SqlArg::from(opts.col_choice.as_str()));
        args.push(SqlArg::from(col_ids[idx].as_str()));
    }
    stats_pool.call_with_sctx(
        &mut |sctx| tidb_stats_handle_util::exec_rows(sctx, &sql, &args).map(|_| ()),
        &[],
    )
}

/// Why an `ANALYZE TABLE` transaction did not produce a committed report.
#[derive(Debug)]
pub enum ClusterAnalyzeError {
    /// A sampling/computation failure retaining its execution error owner.
    Analyze(AnalyzeError),
    /// The commit may have landed, so the server must not report failure.
    Undetermined(String),
    /// A determinate commit failure with its driver error code and SQLSTATE.
    Commit(LockSqlError),
    /// A statement kill retaining its MySQL error identity.
    Killed(SqlError),
    /// A determinate failure with its existing diagnostic.
    Other(String),
    /// The error value Go's ANALYZE raises as it is: `ErrTableNotExists`,
    /// `ErrAnalyzeMissIndex`, `ErrPartitionMgmtOnNonpartitioned`, and the
    /// global-stats `ErrPartitionStatsMissing` /
    /// `ErrPartitionColumnStatsMissing` that ANALYZE demotes to a warning.
    Mysql(tidb_terror::MysqlError),
    /// A statistics-session statement's error, as the statement raised it.
    Sql(SqlQueryError),
    /// A statement error inside a statistics-session callback, returned
    /// through the callback so the pool keeps its MySQL code.
    StatsRead(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for ClusterAnalyzeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Analyze(error) => write!(formatter, "{error}"),
            Self::Undetermined(detail) => {
                write!(formatter, "execution result undetermined: {detail}")
            }
            Self::Commit(error) => formatter.write_str(&error.message),
            Self::Killed(error) => formatter.write_str(&error.message),
            Self::Other(detail) => formatter.write_str(detail),
            Self::Mysql(error) => formatter.write_str(&error.message),
            Self::Sql(error) => formatter.write_str(&error.message),
            Self::StatsRead(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ClusterAnalyzeError {}

/// A Go error raised as ANALYZE's failure.
impl From<tidb_terror::TerrorError> for ClusterAnalyzeError {
    fn from(error: tidb_terror::TerrorError) -> Self {
        Self::Mysql(tidb_terror::MysqlError::from_terror(&error))
    }
}

/// Go `errAnalyzeWorkerPanic`.
pub(crate) const ERR_ANALYZE_WORKER_PANIC: &str = "analyze worker panic";

/// Go `taskType` with the task its executor runs.
enum AnalyzeTaskKind<'a> {
    /// Go `colTask`: `AnalyzeColumnsExec`'s full sampling of one physical
    /// table.
    Col(&'a AnalyzeColumnsTask),
    /// Go `idxTask`: `AnalyzeIndexExec` over one index.
    Idx(&'a AnalyzeIndexTask),
}

/// Go `analyzeTask` with what `buildAnalyzeSamplingPushdown` /
/// `buildAnalyzeIndexPushdown` put in its executor.
struct AnalyzeTask<'a> {
    kind: AnalyzeTaskKind<'a>,
    table_id: AnalyzeTableID,
    job: AnalyzeJobSpec,
    /// The session's analyze variables with this task's table and options.
    statement: AnalyzeStatement,
    /// Go `colsInfo` when not every column is analyzed.
    selected_columns: Option<HashSet<i64>>,
    /// Go `baseCount` / `baseModifyCnt`.
    base_count: i64,
    base_modify_count: i64,
    /// The `mysql.stats_meta` count, `None` without a row.
    realtime_count: Option<i64>,
}

/// Go `statistics.AnalyzeResults`, from a worker to the savers.
pub(crate) struct AnalyzeResults {
    pub(crate) table_id: AnalyzeTableID,
    /// The task, whose job this is (Go `Job`).
    task: usize,
    /// Go `Err`.
    err: Option<ClusterAnalyzeError>,
    stats: Option<tidb_stats_handle_storage::cluster_stats_load::ClusterTableStats>,
    base_count: i64,
    base_modify_count: i64,
    /// Go `ForMVIndexOrGlobalIndex`: an index task's results.
    for_mv_index_or_global_index: bool,
}

/// What one ANALYZE statement's executor leaves for its session.
#[derive(Debug, Default)]
pub struct AnalyzeExecReport {
    /// What the executor builder appends: the sample-rate notes, as
    /// `(is_note, code, message)`.
    pub build_diagnostics: Vec<(bool, u16, String)>,
    /// What `AnalyzeExec.Next` appends.
    pub exec_warnings: Vec<(u16, String)>,
    /// Go `statsHandle.Update(ctx, is, tableAndPartitionIDs...)`; `None`
    /// when no task ran, where Go returns before the update.
    pub table_and_partition_ids: Option<Vec<i64>>,
}

/// Go `filterAndCollectTasks`: drops the tasks of locked tables and
/// partitions, and names each locked physical table once.
fn filter_and_collect_tasks<'a>(
    tasks: Vec<AnalyzeTask<'a>>,
    stats_pool: &dyn StatsSessionPool,
) -> Result<(Vec<AnalyzeTask<'a>>, usize, Vec<String>), ClusterAnalyzeError> {
    let locked = get_locked_table_and_partition_ids(stats_pool, &tasks)?;
    let mut filtered = Vec::new();
    let mut skipped_tables = Vec::new();
    let mut need_analyze_table_cnt = 0;
    let mut tid_and_pids = HashSet::new();
    for task in tasks {
        let table_id = task.table_id;
        let mut is_locked = locked.contains(&table_id.table_id);
        // If the whole table is not locked, check whether the partition is.
        if !is_locked && table_id.is_partition_table() {
            is_locked = locked.contains(&table_id.partition_id);
        }
        let physical_table_id = table_id.get_statistics_id();
        if tid_and_pids.insert(physical_table_id) {
            if is_locked {
                let table_name = match &task.kind {
                    AnalyzeTaskKind::Col(col) => col.tbl_info.read().name.original().to_owned(),
                    AnalyzeTaskKind::Idx(idx) => idx.tbl_info.read().name.original().to_owned(),
                };
                if table_id.is_partition_table() {
                    skipped_tables.push(format!(
                        "{}.{table_name} partition ({})",
                        task.statement.schema, task.job.partition
                    ));
                } else {
                    skipped_tables.push(format!("{}.{table_name}", task.statement.schema));
                }
            } else {
                need_analyze_table_cnt += 1;
            }
        }
        if !is_locked {
            filtered.push(task);
        }
    }
    Ok((filtered, need_analyze_table_cnt, skipped_tables))
}

/// Go `getLockedTableAndPartitionIDs` (`statsHandle.GetLockedTables`).
fn get_locked_table_and_partition_ids(
    stats_pool: &dyn StatsSessionPool,
    tasks: &[AnalyzeTask<'_>],
) -> Result<std::collections::BTreeSet<i64>, ClusterAnalyzeError> {
    let tid_and_pids = tasks
        .iter()
        .flat_map(|task| {
            std::iter::once(task.table_id.table_id)
                .chain(task.table_id.is_partition_table().then_some(task.table_id.partition_id))
        })
        .collect::<Vec<_>>();
    let mut locked = std::collections::BTreeSet::new();
    stats_pool
        .call_with_sctx(
            &mut |sctx| {
                locked = tidb_stats_handle_lockstats::session_stats_lock::query_locked_tables(sctx)?;
                Ok(())
            },
            &[],
        )
        .map_err(ClusterAnalyzeError::Sql)?;
    Ok(tidb_stats_handle_lockstats::lock_stats::get_locked_tables(&locked, tid_and_pids))
}

/// Go `warnLockedTableMsg`.
fn warn_locked_table_msg(warnings: &mut Vec<(u16, String)>, need_analyze_table_cnt: usize, skipped_tables: &[String]) {
    if skipped_tables.is_empty() {
        return;
    }
    let tables = skipped_tables.join(", ");
    let msg = if skipped_tables.len() > 1 {
        if need_analyze_table_cnt > 0 {
            format!("skip analyze locked tables: {tables}, other tables will be analyzed")
        } else {
            format!("skip analyze locked tables: {tables}")
        }
    } else {
        format!("skip analyze locked table: {tables}")
    };
    warnings.push((1105, msg));
}

/// Go `errors.New(err.Error())` for a job's failure text.
fn interrupted() -> ClusterAnalyzeError {
    ClusterAnalyzeError::Mysql(tidb_terror::MysqlError::from_terror(&tidb_exeerrors::ERR_QUERY_INTERRUPTED))
}

/// What one statement's `AnalyzeExec` shares with its workers.
struct AnalyzeExec<'a, S: StoreClient> {
    opener: &'a RealOptimisticTransactionOpener<S>,
    stats_pool: &'a dyn StatsSessionPool,
    jobs: &'a dyn AnalyzeJobLifecycle,
    killer: &'a SqlKiller,
    resource_group: &'a str,
    timeout: Duration,
    stats_lease: Duration,
    tasks: Vec<AnalyzeTask<'a>>,
    handles: Vec<AnalyzeJobHandle>,
}

/// Go `AnalyzeExec.Next` over the plan's tasks, after `buildAnalyze`: the
/// locked tables are skipped, `tidb_build_stats_concurrency` workers analyze
/// the tasks, the result handler hands them to
/// `tidb_analyze_partition_concurrency` save workers (a failed save does not
/// stop the others), then the global stats of dynamically pruned partitioned
/// tables are merged and the options are saved.
#[allow(clippy::too_many_arguments)]
pub fn analyze_exec_next<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    plan: &Analyze,
    vars: &AnalyzeStatement,
    stats: &tidb_stats_handle_syncload::stats_watch::SharedStats,
    approximate_counts: &dyn ApproximateTableCountProvider,
    resource_group: &str,
    timeout: Duration,
    stats_lease: Duration,
    killer: &SqlKiller,
    jobs: &dyn AnalyzeJobLifecycle,
    stats_pool: &dyn StatsSessionPool,
) -> Result<AnalyzeExecReport, ClusterAnalyzeError> {
    let mut report = AnalyzeExecReport::default();
    let tasks = build_analyze(
        plan,
        vars,
        stats,
        approximate_counts,
        resource_group,
        stats_pool,
        &mut report.build_diagnostics,
    )?;
    // Filter the locked tables.
    let (tasks, need_analyze_table_cnt, skipped_tables) = filter_and_collect_tasks(tasks, stats_pool)?;
    warn_locked_table_msg(&mut report.exec_warnings, need_analyze_table_cnt, &skipped_tables);
    if tasks.is_empty() {
        return Ok(report);
    }
    let mut table_and_partition_ids = Vec::with_capacity(tasks.len());
    for task in &tasks {
        table_and_partition_ids.push(task.table_id.table_id);
        if task.table_id.is_partition_table() {
            table_and_partition_ids.push(task.table_id.partition_id);
        }
    }
    // Go `AddNewAnalyzeJob`, once the workers wait for the tasks.
    let handles = tasks.iter().map(|task| AnalyzeJobHandle::pending(jobs, &task.job)).collect();
    let exec = AnalyzeExec {
        opener,
        stats_pool,
        jobs,
        killer,
        resource_group,
        timeout,
        stats_lease,
        tasks,
        handles,
    };
    let global_stats_map = exec.run(vars)?;
    // Go: "If we enabled dynamic prune mode, then we need to generate global
    // stats here for partition tables."
    if vars.dynamic_partition_prune {
        crate::analyze_global_stats::handle_global_stats(
            stats_pool,
            jobs,
            plan,
            vars,
            &global_stats_map,
            &mut report.exec_warnings,
            stats_lease,
            killer,
        );
    }
    // Update analyze options to mysql.analyze_options for auto analyze.
    if let Err(error) = save_analyze_options(stats_pool, vars, &plan.options_map) {
        report.exec_warnings.push((1105, error.message));
    }
    report.table_and_partition_ids = Some(table_and_partition_ids);
    Ok(report)
}

impl<S: StoreClient> AnalyzeExec<'_, S> {
    fn finish(&self, task: usize, failure: Option<&str>) {
        let job = &self.tasks[task].job;
        match failure {
            Some(error) => tidb_stats_handle_logutil::stats_logger().warn(
                &format!("analyze table `{}`.`{}` has failed", job.schema, job.table),
                &[
                    tidb_log::Field::new("partition", tidb_log::Value::Str(job.partition.clone())),
                    tidb_log::Field::new("job info", tidb_log::Value::Str(job.job_info.clone())),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_owned())),
                ],
            ),
            None => tidb_stats_handle_logutil::stats_logger().info(
                &format!("analyze table `{}`.`{}` has finished", job.schema, job.table),
                &[
                    tidb_log::Field::new("partition", tidb_log::Value::Str(job.partition.clone())),
                    tidb_log::Field::new("job info", tidb_log::Value::Str(job.job_info.clone())),
                ],
            ),
        }
        self.handles[task].finish(self.jobs, failure, AnalyzeJobKind::Table);
    }

    /// The task loop, the workers and the result handler of Go's `Next`;
    /// returns Go's `globalStatsMap`.
    fn run(
        &self,
        session: &AnalyzeStatement,
    ) -> Result<std::collections::BTreeMap<(i64, i64), GlobalStatsInfo>, ClusterAnalyzeError> {
        let build_stats_concurrency = session.build_stats_concurrency.clamp(1, self.tasks.len());
        let (task_tx, task_rx) = crossbeam_channel::bounded::<usize>(build_stats_concurrency);
        let (results_tx, results_rx) = crossbeam_channel::bounded::<AnalyzeResults>(1);
        // Go `errExitCh`: dropping the sender closes it.
        let (err_exit_tx, err_exit_rx) = crossbeam_channel::bounded::<()>(0);
        let err_exit_tx = Mutex::new(Some(err_exit_tx));
        let taken = (0..self.tasks.len()).map(|_| std::sync::atomic::AtomicBool::new(false)).collect::<Vec<_>>();
        let mut global_stats_map = std::collections::BTreeMap::new();
        let handled = std::thread::scope(|scope| {
            let workers = (0..build_stats_concurrency)
                .map(|_| {
                    let task_rx = task_rx.clone();
                    let results_tx = results_tx.clone();
                    let err_exit_rx = err_exit_rx.clone();
                    let taken = &taken;
                    scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                        self.analyze_worker(&task_rx, &results_tx, &err_exit_rx, taken);
                    }))
                })
                .collect::<Vec<_>>();
            drop(results_tx);
            let handler = scope.spawn(tidb_cpuprofile::labels::inherit(|| {
                let result = self.handle_results_error(
                    build_stats_concurrency,
                    session,
                    &mut global_stats_map,
                    &results_rx,
                );
                if result.is_err() {
                    // Go `waitFinish`: a failed handler closes `errExitCh`.
                    drop(err_exit_tx.lock().expect("errExitCh").take());
                }
                result
            }));
            for task in 0..self.tasks.len() {
                crossbeam_channel::select! {
                    send(task_tx, task) -> sent => if sent.is_err() { break },
                    recv(err_exit_rx) -> _ => break,
                }
            }
            drop(task_tx);
            let handled = handler
                .join()
                .unwrap_or_else(|_| Err(ClusterAnalyzeError::Other(ERR_ANALYZE_WORKER_PANIC.to_owned())));
            for worker in workers {
                let _ = worker.join();
            }
            handled
        });
        // Go `Next`: "Preserve the original cancellation cause" when the
        // workers stopped on a kill the handler did not see.
        let handled = handled.and_then(|()| {
            self.killer
                .handle_signal()
                .map_or(Ok(()), |error| Err(ClusterAnalyzeError::Killed(error.to_sql_error())))
        });
        if let Err(error) = handled {
            let failure = error.to_string();
            // The tasks no worker took, and the results the handler never
            // read.
            while let Ok(task) = task_rx.try_recv() {
                taken[task].store(true, std::sync::atomic::Ordering::SeqCst);
                self.finish(task, Some(&failure));
            }
            for (task, taken) in taken.iter().enumerate() {
                if !taken.load(std::sync::atomic::Ordering::SeqCst) {
                    self.finish(task, Some(&failure));
                }
            }
            while let Ok(result) = results_rx.try_recv() {
                self.finish(result.task, Some(&failure));
            }
            return Err(error);
        }
        Ok(global_stats_map)
    }

    /// Go `analyzeWorker`. A panic sends `errAnalyzeWorkerPanic` as the
    /// task's results and ends the worker, as Go's recovered goroutine does.
    fn analyze_worker(
        &self,
        task_rx: &crossbeam_channel::Receiver<usize>,
        results_tx: &crossbeam_channel::Sender<AnalyzeResults>,
        err_exit_rx: &crossbeam_channel::Receiver<()>,
        taken: &[std::sync::atomic::AtomicBool],
    ) {
        while let Ok(task) = task_rx.recv() {
            taken[task].store(true, std::sync::atomic::Ordering::SeqCst);
            // Go `analyzeWorkerExitErr`.
            if let Some(error) = self.killer.handle_signal() {
                self.finish(task, Some(&error.to_sql_error().message));
                return;
            }
            if matches!(err_exit_rx.try_recv(), Err(crossbeam_channel::TryRecvError::Disconnected)) {
                self.finish(task, Some(&interrupted().to_string()));
                return;
            }
            let outcome = crate::analyze::panic_recovery::recover_analyze_panic(|| {
                tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/handleAnalyzeWorkerPanic");
                self.handles[task].start(self.jobs);
                self.analyze_task(task)
            });
            let (result, exit) = match outcome {
                Ok(result) => (result, false),
                Err(panic) => {
                    tidb_stats_handle_logutil::stats_logger().warn("analyze worker panicked", &[]);
                    let table_id = self.tasks[task].table_id;
                    (
                        AnalyzeResults {
                            table_id,
                            task,
                            err: Some(ClusterAnalyzeError::Other(panic.rendered_message().to_owned())),
                            stats: None,
                            base_count: 0,
                            base_modify_count: 0,
                            for_mv_index_or_global_index: false,
                        },
                        true,
                    )
                }
            };
            self.try_send_analyze_result(results_tx, err_exit_rx, result);
            if exit {
                return;
            }
        }
    }

    /// Go `trySendAnalyzeResult`: a result the aborting statement does not
    /// take finishes its job here.
    fn try_send_analyze_result(
        &self,
        results_tx: &crossbeam_channel::Sender<AnalyzeResults>,
        err_exit_rx: &crossbeam_channel::Receiver<()>,
        result: AnalyzeResults,
    ) {
        let dropped = crossbeam_channel::select! {
            send(results_tx, result) -> sent => match sent {
                Ok(()) => {
                    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/executor/analyzeResultSent", &mut ());
                    None
                }
                Err(crossbeam_channel::SendError(result)) => Some(result),
            },
            recv(err_exit_rx) -> _ => None,
        };
        if let Some(result) = dropped {
            let failure = result.err.as_ref().map_or_else(|| interrupted().to_string(), ToString::to_string);
            self.finish(result.task, Some(&failure));
        }
    }

    /// Go `handleResultsError`.
    fn handle_results_error(
        &self,
        build_stats_concurrency: usize,
        session: &AnalyzeStatement,
        global_stats_map: &mut std::collections::BTreeMap<(i64, i64), GlobalStatsInfo>,
        results_rx: &crossbeam_channel::Receiver<AnalyzeResults>,
    ) -> Result<(), ClusterAnalyzeError> {
        // The concurrency of saving partition-level stats does not exceed the
        // number of tasks.
        let save_stats_concurrency = session.analyze_partition_concurrency.clamp(1, self.tasks.len());
        crate::analyze::panic_recovery::recover_analyze_panic(|| {
            if save_stats_concurrency <= 1 {
                tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/handleResultsErrorSingleThreadPanic");
            }
            self.handle_results_error_with_concurrency(
                build_stats_concurrency,
                save_stats_concurrency,
                session,
                global_stats_map,
                results_rx,
            )
        })
        .unwrap_or_else(|panic| {
            tidb_stats_handle_logutil::stats_logger().error("analyze save stats panic", &[]);
            Err(ClusterAnalyzeError::Other(panic.rendered_message().to_owned()))
        })
    }

    /// Go `handleResultsErrorWithConcurrency`.
    fn handle_results_error_with_concurrency(
        &self,
        _build_stats_concurrency: usize,
        save_stats_concurrency: usize,
        session: &AnalyzeStatement,
        global_stats_map: &mut std::collections::BTreeMap<(i64, i64), GlobalStatsInfo>,
        results_rx: &crossbeam_channel::Receiver<AnalyzeResults>,
    ) -> Result<(), ClusterAnalyzeError> {
        let (save_tx, save_rx) = crossbeam_channel::bounded::<AnalyzeResults>(save_stats_concurrency);
        let (err_tx, err_rx) = crossbeam_channel::bounded::<String>(save_stats_concurrency);
        let analyze_snapshot = session.analyze_snapshot;
        let need_global_stats = session.dynamic_partition_prune;
        std::thread::scope(|scope| {
            let savers = (0..save_stats_concurrency)
                .map(|_| {
                    let worker = crate::analyze_worker::AnalyzeSaveStatsWorker::new(save_rx.clone(), err_tx.clone(), self.killer);
                    scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                        worker.run(
                            |results: &AnalyzeResults| self.save_analyze_result_to_storage(results, analyze_snapshot),
                            |results: &AnalyzeResults, failure| self.finish(results.task, failure),
                        );
                    }))
                })
                .collect::<Vec<_>>();
            drop(save_rx);
            drop(err_tx);
            let mut err = None;
            loop {
                if let Some(error) = self.killer.handle_signal() {
                    // Go returns before reading the next result; `Next`
                    // finishes the jobs of those left in the channel.
                    drop(save_tx);
                    return Err(ClusterAnalyzeError::Killed(error.to_sql_error()));
                }
                let Ok(mut results) = results_rx.recv() else {
                    break;
                };
                if let Some(error) = results.err.take() {
                    self.finish(results.task, Some(&error.to_string()));
                    err = Some(error);
                    continue;
                }
                handle_global_stats_map(need_global_stats, global_stats_map, &results);
                tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/executor/analyzeBeforeSendToSaveResults", &mut ());
                if let Err(crossbeam_channel::SendError(results)) = save_tx.send(results) {
                    self.finish(results.task, Some(&interrupted().to_string()));
                }
            }
            drop(save_tx);
            for saver in savers {
                let _ = saver.join();
            }
            let mut err_set = err_rx.try_iter().collect::<Vec<_>>();
            if !err_set.is_empty() {
                err_set.sort();
                err_set.dedup();
                return Err(ClusterAnalyzeError::Other(err_set.join(",")));
            }
            err.map_or(Ok(()), Err)
        })
    }

    /// Go `statsHandle.SaveAnalyzeResultToStorage`.
    fn save_analyze_result_to_storage(&self, results: &AnalyzeResults, analyze_snapshot: bool) -> Result<(), String> {
        let Some(stats) = &results.stats else {
            return Ok(());
        };
        stats_read_writer::save_analyze_result_to_storage(
            self.stats_pool,
            self.stats_lease,
            &AnalyzeResults_ {
                stats,
                is_partition: results.table_id.is_partition_table(),
                base_count: results.base_count,
                base_modify_count: results.base_modify_count,
                stats_ver: STATS_VERSION_2,
                for_mv_index_or_global_index: results.for_mv_index_or_global_index,
            },
            analyze_snapshot,
        )
        .map(|_| ())
        .map_err(|error| error.message)
    }

    /// Go `analyzeColumnsPushDown` / `analyzeIndexPushdown` for one task.
    fn analyze_task(&self, task: usize) -> AnalyzeResults {
        let table_id = self.tasks[task].table_id;
        let progress = |count| self.handles[task].update(self.jobs, count);
        let outcome = match &self.tasks[task].kind {
            AnalyzeTaskKind::Col(col) => self.analyze_columns(&self.tasks[task], col, &progress),
            AnalyzeTaskKind::Idx(idx) => self.analyze_index(&self.tasks[task], idx, &progress).map(|stats| (stats, 0, 0)),
        };
        let for_mv_index_or_global_index = matches!(self.tasks[task].kind, AnalyzeTaskKind::Idx(_));
        match outcome {
            Ok((stats, base_count, base_modify_count)) => AnalyzeResults {
                table_id,
                task,
                err: None,
                stats: Some(stats),
                base_count,
                base_modify_count,
                for_mv_index_or_global_index,
            },
            Err(error) => AnalyzeResults {
                table_id,
                task,
                err: Some(error),
                stats: None,
                base_count: 0,
                base_modify_count: 0,
                for_mv_index_or_global_index,
            },
        }
    }

    /// Go `AnalyzeColumnsExec.analyzeColumnsPushDown`: samples one physical
    /// table at its own snapshot.
    fn analyze_columns(
        &self,
        task: &AnalyzeTask<'_>,
        col: &AnalyzeColumnsTask,
        progress: &(dyn Fn(i64) + Sync),
    ) -> Result<(tidb_stats_handle_storage::cluster_stats_load::ClusterTableStats, i64, i64), ClusterAnalyzeError> {
        let statement = &task.statement;
        let sample_transaction = self
            .opener
            .begin()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let sample_ts = sample_transaction.start_ts();
        let report = {
            let table = col.tbl_info.read();
            crate::cluster_analyze::sampling::analyze_table(
                self.opener,
                &table,
                task.table_id.get_statistics_id(),
                statement,
                task.realtime_count,
                sample_ts,
                task.selected_columns.as_ref(),
                crate::cluster_analyze::sampling::adaptive_anlayze_dist_sql_concurrency(
                    statement.scan_concurrency,
                    self.opener.store().pd_http_client(),
                    self.timeout,
                ),
                self.timeout,
                self.killer,
                self.resource_group,
                progress,
                Some((self.jobs, &task.job)),
            )
            .map_err(ClusterAnalyzeError::Analyze)?
        };
        sample_transaction
            .finish_without_writes()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        Ok((report.stats, task.base_count, task.base_modify_count))
    }

    /// Go `analyzeIndexPushdown` for a multi-valued index of one physical
    /// table or a special global index of the logical table.
    fn analyze_index(
        &self,
        task: &AnalyzeTask<'_>,
        idx: &AnalyzeIndexTask,
        progress: &(dyn Fn(i64) + Sync),
    ) -> Result<tidb_stats_handle_storage::cluster_stats_load::ClusterTableStats, ClusterAnalyzeError> {
        let statement = &task.statement;
        let transaction = self
            .opener
            .begin_read_only()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let start_ts = transaction.start_ts();
        let index = idx.index_info.read().clone();
        let report = analyze_independent_index_with_progress(
            self.opener,
            task.table_id.get_statistics_id(),
            &index,
            statement,
            &statement.options,
            start_ts,
            crate::cluster_analyze::sampling::adaptive_anlayze_dist_sql_concurrency(
                statement.scan_concurrency,
                self.opener.store().pd_http_client(),
                self.timeout,
            ),
            self.timeout,
            self.killer,
            self.resource_group,
            progress,
        )
        .map_err(ClusterAnalyzeError::Analyze)?;
        transaction
            .finish_without_writes()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        Ok(report.stats)
    }
}

/// Go `handleGlobalStats` (analyze.go): the global merges a dynamically
/// pruned partition's results call for, keyed by `(tableID, indexID)` with
/// `-1` for the columns.
fn handle_global_stats_map(
    need_global_stats: bool,
    global_stats_map: &mut std::collections::BTreeMap<(i64, i64), GlobalStatsInfo>,
    results: &AnalyzeResults,
) {
    if !(results.table_id.is_partition_table() && need_global_stats) {
        return;
    }
    let Some(stats) = &results.stats else {
        return;
    };
    let table_id = results.table_id.table_id;
    if !results.for_mv_index_or_global_index {
        // The columns' result, whose virtual columns have no histogram.
        global_stats_map.insert(
            (table_id, -1),
            GlobalStatsInfo {
                is_index: false,
                hist_ids: stats.columns.iter().map(|item| item.id).collect(),
                stats_version: STATS_VERSION_2,
            },
        );
    }
    for index in &stats.indexes {
        global_stats_map.insert(
            (table_id, index.id),
            GlobalStatsInfo { is_index: true, hist_ids: vec![index.id], stats_version: STATS_VERSION_2 },
        );
    }
}

/// Go `statsReadWriter.StatsMetaCountAndModifyCount`: `(count,
/// modify_count, no row)` in a wrapped transaction on a statistics session.
fn stats_meta_count_and_modify_count(
    stats_pool: &dyn StatsSessionPool,
    table_id: i64,
) -> Result<(i64, i64, bool), ClusterAnalyzeError> {
    let mut counts = (0, 0, true);
    stats_pool
        .call_with_sctx(
            &mut |sctx| {
                counts = tidb_stats_handle_storage::read::stats_meta_count_and_modify_count(
                    sctx, table_id, false,
                )?;
                Ok(())
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .map_err(ClusterAnalyzeError::Sql)?;
    Ok(counts)
}

/// The session-visible error of a cluster ANALYZE.
pub fn cluster_analyze_error(error: ClusterAnalyzeError) -> tidb_store_driver_error::sql_query_error::SqlQueryError {
    match error {
        ClusterAnalyzeError::Analyze(error) => {
            let error = crate::DriverError::from(error).to_mysql_error();
            tidb_store_driver_error::sql_query_error::SqlQueryError::from(error)
        }
        ClusterAnalyzeError::Undetermined(_) => tidb_store_driver_error::sql_query_error::SqlQueryError::result_undetermined(),
        ClusterAnalyzeError::Commit(error) => tidb_store_driver_error::sql_query_error::lock_sql_error(&error),
        ClusterAnalyzeError::Killed(error) => {
            tidb_store_driver_error::sql_query_error::SqlQueryError::from(tidb_terror::MysqlError::from_sql_error(error))
        }
        ClusterAnalyzeError::Other(detail) => tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(detail),
        ClusterAnalyzeError::Mysql(error) => tidb_store_driver_error::sql_query_error::SqlQueryError::from(error),
        ClusterAnalyzeError::Sql(error) => error,
        // Returned through its callback before it gets here; the pool's own
        // conversion of an error it does not recognize.
        ClusterAnalyzeError::StatsRead(error) => {
            tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        automatic_sample_rate,
        AnalyzeJobKind, AnalyzeJobLifecycle,
        AnalyzeJobProgress, AnalyzeJobSpec, ApproximateTableCountProvider,
        ANALYZE_JOB_MAX_DELTA,
    };
    use std::thread;
    use std::time::Duration;

    #[derive(Default)]
    struct RecordingAnalyzeJobs {
        next_id: std::sync::atomic::AtomicU64,
        events: std::sync::Mutex<Vec<String>>,
    }

    struct RecordingApproximateCount {
        call: std::sync::Mutex<Option<(String, i64, String, String, String)>>,
    }

    impl ApproximateTableCountProvider for RecordingApproximateCount {
        fn approximate_table_count(
            &self,
            resource_group: &str,
            physical_id: i64,
            database: &str,
            table: &str,
            partition: &str,
        ) -> (f64, bool) {
            *self.call.lock().unwrap() = Some((
                resource_group.to_owned(),
                physical_id,
                database.to_owned(),
                table.to_owned(),
                partition.to_owned(),
            ));
            (1_000_000.0, true)
        }
    }

    #[test]
    fn automatic_sample_rate_uses_pdhelper_identity_and_stale_count() {
        let provider = RecordingApproximateCount {
            call: std::sync::Mutex::new(None),
        };
        assert_eq!(
            automatic_sample_rate(Some(10_000), &provider, "oltp", 42, "test", "orders", "p0",).0,
            0.15
        );
        assert_eq!(
            provider.call.into_inner().unwrap(),
            Some((
                "oltp".to_owned(),
                42,
                "test".to_owned(),
                "orders".to_owned(),
                "p0".to_owned(),
            ))
        );
    }

    impl AnalyzeJobLifecycle for RecordingAnalyzeJobs {
        fn insert(&self, spec: &AnalyzeJobSpec) -> Option<u64> {
            let id = self
                .next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            self.events
                .lock()
                .unwrap()
                .push(format!("insert:{id}:{}", spec.partition));
            Some(id)
        }

        fn start(&self, job_id: u64) {
            self.events.lock().unwrap().push(format!("start:{job_id}"));
        }

        fn update_progress(&self, job_id: u64, processed_rows: i64) {
            self.events
                .lock()
                .unwrap()
                .push(format!("progress:{job_id}:{processed_rows}"));
        }

        fn finish(
            &self,
            job_id: u64,
            processed_rows: i64,
            failure: Option<&str>,
            kind: AnalyzeJobKind,
        ) {
            self.events.lock().unwrap().push(format!(
                "finish:{job_id}:{processed_rows}:{}:{kind:?}",
                failure.unwrap_or_default()
            ));
        }
    }

    #[test]
    fn analyze_job_progress_uses_go_row_and_time_gates() {
        let progress = AnalyzeJobProgress::started();
        progress.state.lock().unwrap().last_dump =
            std::time::Instant::now() - Duration::from_secs(6);

        assert_eq!(progress.update(ANALYZE_JOB_MAX_DELTA), 0);
        assert_eq!(progress.update(1), ANALYZE_JOB_MAX_DELTA + 1);
        assert_eq!(progress.residual(), 0);
        assert_eq!(progress.update(7), 0);
        assert_eq!(progress.residual(), 7);
    }

    use tidb_txnkv::rpc::UnaryCallContext;

    #[test]
    fn the_commit_budget_is_minted_after_the_scan_not_before_it() {
        // Regression for the ANALYZE commit that reached prewrite with
        // {bt}timed out after 0ms{bt}: the commit used to inherit one
        // {bt}UnaryCallContext{bt} stamped at function entry, so a
        // table-sized sample consumed the whole statement budget before the
        // commit was ever submitted -- on a 5.5M-row table the scan alone ran
        // past the 300s deadline and every commit arrived expired. The
        // commit must mint its budget where it is spent, the way
        // {bt}MultiStatementTransaction::transaction_end_call{bt} already
        // does for sessions.
        let statement_budget = Duration::from_millis(50);
        let inherited = UnaryCallContext::with_timeout(statement_budget);
        // The sample of a deep table: far longer than one request's budget.
        thread::sleep(statement_budget * 3);
        assert!(
            inherited.timeout().is_zero(),
            "a context minted before the scan is exhausted by it"
        );
        let commit_call = UnaryCallContext::with_timeout(statement_budget);
        assert!(
            commit_call.timeout() > Duration::ZERO,
            "the commit mints its own full budget at commit time"
        );
    }
}
