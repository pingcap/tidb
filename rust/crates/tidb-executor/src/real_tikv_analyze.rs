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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::sync_channel;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::TimeZone;
use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone, UNSPECIFIED_LENGTH};
use tidb_mysql::errors::SqlError;
use tidb_stats::histogram::Histogram;
use tidb_stats::row_sample_collector::get_adjusted_sample_rate;
use tidb_stats::fm_sketch_ndv;
use tidb_stats::merge_fm_sketch;
use tidb_stats_handle_globalstats::merge_partition_histogram_topn;
use tidb_stats_handle_globalstats::merge_partition_stats_item;
use tidb_stats::CmsSketch;
use tidb_stats::FmSketch;
use tidb_stats_handle_globalstats::GlobalStatsItem;
use tidb_stats_handle_globalstats::GlobalStatsMergeError;
use tidb_stats_handle_globalstats::GlobalStatsMergeMode;
use tidb_stats_handle_globalstats::PartitionStatsItem;
use tidb_stats::StatsLoadedStatus;
use tidb_stats::TopN;
use tidb_store_driver_txn::transaction::StoreClient;
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;
use tidb_sqlkiller::SqlKiller;

use crate::cluster_analyze::{
    analyze_independent_index_with_progress, lower_analyze, resolve_analyze_options,
    AnalyzeColumnChoice, AnalyzeError, AnalyzeStatement,
};
use tidb_infoschema::cluster_catalog::load_info_schema;
use tidb_stats_handle_storage::cluster_stats_load::{
    column_types_of, ClusterStatsItem,
};
use tidb_stats_handle_storage::save::{AnalyzeResults, STATS_VERSION_2};
use tidb_stats_handle_storage::stats_read_writer::{self, StatsSessionPool};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_stats_handle_history::history_stats::record_historical_stats_meta_on;
use tidb_stats_handle_usage_predicatecolumn::predicate_column::get_predicate_columns;
use tidb_sqlescape::SqlArg;
use tidb_store_driver_error::pessimistic_lock_error::LockSqlError;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_hack::GoToLower;
use tidb_datatype::Datum;
use crate::analyze::{AnalyzeOptionOverrides, SavedAnalyzeOptions};
use tidb_model::table_info::TableInfo;
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

struct AnalyzeJobHandle {
    id: Option<u64>,
    progress: AnalyzeJobProgress,
}

impl AnalyzeJobHandle {
    fn pending(jobs: &dyn AnalyzeJobLifecycle, spec: &AnalyzeJobSpec) -> Self {
        Self {
            id: jobs.insert(spec),
            progress: AnalyzeJobProgress::started(),
        }
    }

    fn start(&self, jobs: &dyn AnalyzeJobLifecycle) {
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

    fn finish(&self, jobs: &dyn AnalyzeJobLifecycle, failure: Option<&str>, kind: AnalyzeJobKind) {
        if let Some(id) = self.id {
            jobs.finish(id, self.progress.residual(), failure, kind);
        }
    }
}

fn fail_analyze_jobs(jobs: &dyn AnalyzeJobLifecycle, pending: &[AnalyzeJobHandle], failure: &str) {
    for job in pending {
        job.finish(jobs, Some(failure), AnalyzeJobKind::Table);
    }
}

/// Parses and admits one text-protocol statement as an `ANALYZE TABLE`.
///
/// `None` means the statement is not one -- including when it does not parse
/// at all, so an unparsable statement is still reported by the query path
/// that owns the same text. This mirrors
/// [`tidb_ddl::real_tikv_ddl::prepare_cluster_ddl_with_context`]: the server never parses SQL
/// itself, so the dependency direction stays
/// `tidb-server -> tidb-exec -> tidb-txnkv`.
pub fn prepare_cluster_analyze(
    sql: &str,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    let Ok(statement) = tidb_parser::parse(sql) else {
        return Ok(None);
    };
    prepare_cluster_analyze_parsed(&statement, default_schema)
}

/// [`prepare_cluster_analyze`] over a statement the caller already parsed.
pub fn prepare_cluster_analyze_parsed(
    statement: &tidb_ast::Stmt,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    lower_analyze(statement, default_schema)
}

pub use crate::analyze::ClusterAnalyzeReport;

/// The pinned planner's physical ANALYZE targets and merged options.
#[derive(Clone, Debug)]
pub struct ResolvedClusterAnalyze {
    statement: AnalyzeStatement,
    table: tidb_model::table_info::TableInfo,
    physical_options: Vec<crate::analyze::PhysicalAnalyzeOptions>,
    partition_names: HashMap<i64, String>,
    selected_columns: HashMap<i64, Option<HashSet<i64>>>,
    predicate_columns_empty: HashMap<i64, bool>,
    partitioned: bool,
    ignored_partition_overrides: bool,
    run_full_sampling: bool,
    independent_index_ids: Vec<i64>,
    /// See [`ClusterAnalyzeReport::build_diagnostics`].
    build_diagnostics: Vec<(bool, u16, String)>,
}

impl ResolvedClusterAnalyze {
    /// The logical table whose global statistics are written.
    #[must_use]
    pub fn logical_table_id(&self) -> i64 {
        self.physical_options
            .first()
            .expect("the logical table option is always present")
            .physical_id
    }

    fn partition_name(&self, physical_id: i64) -> String {
        self.partition_names
            .get(&physical_id)
            .cloned()
            .unwrap_or_default()
    }

    fn table_job_spec(
        &self,
        options: &crate::analyze::PhysicalAnalyzeOptions,
    ) -> AnalyzeJobSpec {
        let indexes = self
            .table
            .indices
            .iter_deref()
            .filter_map(|index| {
                let index = index.read();
                // The column task's own indexes (Go `AnalyzeColumnsTask.
                // Indexes`): independent and columnar ones are not among them.
                (index.state == tidb_model::SchemaState::PUBLIC
                    && !is_special_global_index(&self.table, &index)
                    && !index.mv_index
                    && !crate::cluster_analyze::is_columnar_index(&index))
                .then(|| index.name.original().to_owned())
            })
            .collect::<Vec<_>>();
        let public_index_count = self
            .table
            .indices
            .iter_deref()
            .filter(|index| index.read().state == tidb_model::SchemaState::PUBLIC)
            .count();
        let columns = self
            .selected_columns
            .get(&options.physical_id)
            .and_then(|selected| selected.as_ref())
            .map(|selected| {
                self.table
                    .cols()
                    .iter_deref()
                    .filter_map(|column| {
                        let column = column.read();
                        (selected.contains(&column.id)
                            && !column.is_changing()
                            && !column.is_removing())
                        .then(|| column.name.original().to_owned())
                    })
                    .collect::<Vec<_>>()
            });
        let job_info = analyze_table_job_info(
            self.statement.auto_analyze,
            &indexes,
            public_index_count,
            columns.as_deref(),
            self.table.get_non_temp_columns().len(),
            options,
        );
        AnalyzeJobSpec {
            schema: self.statement.schema.clone(),
            table: self.statement.table.clone(),
            partition: self.partition_name(options.physical_id),
            job_info,
        }
    }

    /// The public multi-valued indexes, which Go analyzes as independent
    /// index tasks per physical table.
    fn mv_index_ids(&self) -> Vec<i64> {
        self.table
            .indices
            .iter_deref()
            .filter_map(|index| {
                let index = index.read();
                (index.state == tidb_model::SchemaState::PUBLIC && index.mv_index)
                    .then_some(index.id)
            })
            .collect()
    }

    /// Go's `AnalyzeIndexTask` job for one physical table's index.
    fn partition_index_job_spec(&self, physical_id: i64, index_id: i64) -> AnalyzeJobSpec {
        AnalyzeJobSpec {
            partition: self.partition_name(physical_id),
            ..self.independent_index_job_spec(index_id)
        }
    }

    fn independent_index_job_spec(&self, index_id: i64) -> AnalyzeJobSpec {
        let name = self
            .table
            .indices
            .iter_deref()
            .find_map(|index| {
                let index = index.read();
                (index.id == index_id).then(|| index.name.original().to_owned())
            })
            .unwrap_or_else(|| index_id.to_string());
        AnalyzeJobSpec {
            schema: self.statement.schema.clone(),
            table: self.statement.table.clone(),
            partition: String::new(),
            job_info: if self.statement.auto_analyze {
                format!("auto analyze index {name}")
            } else {
                format!("analyze index {name}")
            },
        }
    }

    fn global_job_spec(&self, index_id: Option<i64>) -> AnalyzeJobSpec {
        let job_info = match index_id {
            None => format!(
                "merge global stats for {}.{} columns",
                self.statement.schema, self.statement.table
            ),
            Some(index_id) => {
                let name = self
                    .table
                    .indices
                    .iter_deref()
                    .find_map(|index| {
                        let index = index.read();
                        (index.id == index_id).then(|| index.name.original().to_owned())
                    })
                    .unwrap_or_else(|| index_id.to_string());
                format!(
                    "merge global stats for {}.{}'s index {name}",
                    self.statement.schema, self.statement.table
                )
            }
        };
        AnalyzeJobSpec {
            schema: self.statement.schema.clone(),
            table: self.statement.table.clone(),
            partition: String::new(),
            job_info,
        }
    }
}

/// Reads and merges the saved table-level ANALYZE options before execution.
pub fn resolve_cluster_analyze_statement<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    statement: &AnalyzeStatement,
    timeout: Duration,
    resource_group: &str,
    approximate_counts: &dyn ApproximateTableCountProvider,
    stats: &tidb_stats_handle_syncload::stats_watch::SharedStats,
    stats_pool: &dyn StatsSessionPool,
) -> Result<ResolvedClusterAnalyze, ClusterAnalyzeError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let (table, partition_ids, partition_names, saved) = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_info_schema(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, statement)?;
        let table = table.read();
        let partition_ids = analyze_partition_ids(&table, &statement.partitions)?;
        let partition_names = table
            .partition
            .as_ref()
            .map(|partition| {
                partition
                    .read()
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| (definition.id, definition.name.original().to_owned()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let mut saved = std::collections::HashMap::new();
        if statement.persist_options {
            for physical_id in std::iter::once(table.id).chain(partition_ids.iter().copied()) {
                if let Some(options) = get_saved_analyze_opts(stats_pool, &table, physical_id)
                    .map_err(ClusterAnalyzeError::Sql)?
                {
                    saved.insert(physical_id, options);
                }
            }
        }
        (table.clone(), partition_ids, partition_names, saved)
    };
    transaction
        .finish_without_writes()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;

    let index_tasks = select_index_tasks(&table, statement)?;

    let mut resolution = resolve_analyze_options(
        table.id,
        &partition_ids,
        statement.raw_options,
        &statement.columns,
        &saved,
        !statement.persist_options || statement.partitions.is_empty(),
        statement.dynamic_partition_prune,
    );
    let mut build_diagnostics = Vec::new();
    let pseudo_info = &table;
    // Go `Handle.isSystemTable`: `filter.IsSystemSchema(db.Name.L)`.
    let pseudo_cacheable = table.temp_table_type == tidb_model::TempTableType::NONE
        && !tidb_filter::is_system_schema(&tidb_hack::go_to_lower(&statement.schema));
    for options in &mut resolution.physical {
        let (columns, missing) = final_column_choice(&table, &options.columns)?;
        options.columns = columns;
        // Go's planner warns once, while it builds the column list.
        if let Some(missing) = missing {
            if build_diagnostics.is_empty() {
                build_diagnostics.push((
                    false,
                    1105,
                    format!(
                        "Columns {missing} are missing in ANALYZE but their stats are needed \
                         for calculating stats for indexes/primary key/extended stats"
                    ),
                ));
            }
        }
        if options.effective.num_samples == 0 && options.effective.sample_rate.is_none() {
            let partition = partition_names
                .get(&options.physical_id)
                .map_or("", String::as_str);
            // go `getAdjustedSampleRate` reads `GetPhysicalTableStats`: the
            // stats CACHE's table, whose RealtimeCount is the
            // `stats_meta.count` its last load saw, and on a cache miss
            // `PseudoTable`, whose RealtimeCount is PseudoRowCount=10000 and
            // which that miss caches (with `stats-lease = 0` nothing loads a
            // new table before its first ANALYZE, which is why Go's
            // integration recordings answer `use min(1, 110000/10000)`).
            let realtime_count = stats
                .get_physical_table_stats(
                    options.physical_id,
                    table.partition.is_some(),
                    pseudo_cacheable,
                    || tidb_stats::pseudo_table(pseudo_info, false, true),
                )
                .map_or(tidb_stats::PSEUDO_ROW_COUNT, |cached| {
                    cached.hist_coll.realtime_count
                });
            let (rate, reason) = automatic_sample_rate(
                Some(realtime_count),
                approximate_counts,
                resource_group,
                options.physical_id,
                &statement.schema,
                &statement.table,
                partition,
            );
            options.effective.sample_rate = Some(rate);
            // Go appends this NOTE per sampling task at build time
            // (`builder.go:3185`/`3194`, `sc.AppendNote`, generic 1105). A
            // partitioned table's logical entry is no task of its own, and a
            // partition's task names the partition.
            if table.partition.is_some() && options.physical_id == table.id {
                continue;
            }
            let message = if partition.is_empty() {
                format!(
                    "Analyze use auto adjusted sample rate {:.6} for table {}.{}, \
                     reason to use this rate is \"{}\"",
                    rate, statement.schema, statement.table, reason
                )
            } else {
                format!(
                    "Analyze use auto adjusted sample rate {:.6} for table {}.{}'s partition {}, \
                     reason to use this rate is \"{}\"",
                    rate, statement.schema, statement.table, partition, reason
                )
            };
            build_diagnostics.push((true, 1105, message));
        }
    }
    let mut selected_columns_by_id = HashMap::new();
    let mut predicate_columns_empty = HashMap::new();
    for options in &resolution.physical {
        let mut target_statement = statement.clone();
        target_statement.columns = options.columns.clone();
        let (mut selected, empty) = selected_columns(stats_pool, &table, &target_statement)?;
        if let Some(selected) = selected.as_mut() {
            let predicate = match &target_statement.columns {
                AnalyzeColumnChoice::Predicate => true,
                AnalyzeColumnChoice::Default => {
                    matches!(
                        target_statement.default_columns,
                        AnalyzeColumnChoice::Predicate
                    )
                }
                _ => false,
            };
            add_mandatory_columns(&table, selected, predicate);
        }
        selected =
            filter_skipped_column_types(&table, selected, &target_statement.skip_column_types);
        selected_columns_by_id.insert(options.physical_id, selected);
        predicate_columns_empty.insert(options.physical_id, empty);
    }
    let mut resolved = statement.clone();
    let logical = resolution
        .physical
        .first()
        .expect("the logical table option is always present");
    resolved.raw_options = logical.raw;
    resolved.columns = logical.columns.clone();
    resolved.options = logical.effective;
    resolved.options.memory_quota = statement.options.memory_quota;
    let partitioned = table.partition.is_some();
    Ok(ResolvedClusterAnalyze {
        statement: resolved,
        table,
        physical_options: resolution.physical,
        partition_names,
        selected_columns: selected_columns_by_id,
        predicate_columns_empty,
        partitioned,
        ignored_partition_overrides: resolution.ignored_partition_overrides,
        run_full_sampling: index_tasks.run_full_sampling,
        independent_index_ids: index_tasks.independent_index_ids,
        build_diagnostics,
    })
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

#[derive(Debug)]
struct IndexTaskSelection {
    run_full_sampling: bool,
    independent_index_ids: Vec<i64>,
}

fn is_special_global_index(
    table: &tidb_model::table_info::TableInfo,
    index: &tidb_model::index::IndexInfo,
) -> bool {
    index.global
        && index.columns.iter_deref().any(|column| {
            let column = column.read();
            column.length != UNSPECIFIED_LENGTH
                || table
                    .cols()
                    .get(column.offset as usize)
                    .is_some_and(|column| column.read().is_virtual_generated())
        })
}

fn select_index_tasks(
    table: &tidb_model::table_info::TableInfo,
    statement: &AnalyzeStatement,
) -> Result<IndexTaskSelection, ClusterAnalyzeError> {
    let Some(names) = &statement.index_names else {
        return Ok(IndexTaskSelection {
            run_full_sampling: true,
            independent_index_ids: if statement.partitions.is_empty() {
                table
                    .indices
                    .iter_deref()
                    .filter_map(|index| {
                        let index = index.read();
                        (index.state == tidb_model::SchemaState::PUBLIC
                            && is_special_global_index(table, &index))
                        .then_some(index.id)
                    })
                    .collect()
            } else {
                Vec::new()
            },
        });
    };
    let indexes = table.indices.iter_deref().filter_map(|index| {
        let index = index.read();
        (index.state == tidb_model::SchemaState::PUBLIC).then(|| index.clone())
    });
    let selected = if names.is_empty() {
        indexes.collect::<Vec<_>>()
    } else {
        names
            .iter()
            .map(|name| {
                table
                    .indices
                    .iter_deref()
                    .find_map(|index| {
                        let index = index.read();
                        (index.state == tidb_model::SchemaState::PUBLIC
                            && index.name.lowercase() == name.go_to_lower())
                        .then(|| index.clone())
                    })
                    .ok_or_else(|| {
                        ClusterAnalyzeError::from(
                            tidb_plannererrors::ERR_ANALYZE_MISS_INDEX
                                .generate_with_stack_by_args(&[
                                    name.as_str().into(),
                                    table.name.original().into(),
                                ]),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    for index in &selected {
        if !is_special_global_index(table, index) {
            return Ok(IndexTaskSelection {
                run_full_sampling: true,
                independent_index_ids: if statement.partitions.is_empty() {
                    table
                        .indices
                        .iter_deref()
                        .filter_map(|index| {
                            let index = index.read();
                            (index.state == tidb_model::SchemaState::PUBLIC
                                && is_special_global_index(table, &index))
                            .then_some(index.id)
                        })
                        .collect()
                } else {
                    Vec::new()
                },
            });
        }
        if !statement.partitions.is_empty() {
            return Err(ClusterAnalyzeError::Other(format!(
                "Analyze global index '{}' can't work with analyze specified partitions",
                index.name.original()
            )));
        }
    }
    Ok(IndexTaskSelection {
        run_full_sampling: false,
        // Pinned Go deliberately iterates the explicitly named list in this
        // branch. Therefore `ANALYZE TABLE t INDEX` with no names and only
        // special global indexes creates no independent task.
        independent_index_ids: names
            .iter()
            .filter_map(|name| {
                selected
                    .iter()
                    .find(|index| index.name.lowercase() == name.go_to_lower())
                    .map(|index| index.id)
            })
            .collect(),
    })
}

/// Go `buildAnalyze`'s answer for one statement: whether it builds a column
/// (full-sampling) task, the only kind `collectStatsDeltaFlushObjectsForAnalyze`
/// flushes deltas for. An index-only selection of special global indexes
/// builds independent index tasks alone. A statement whose table or index is
/// missing fails later in its own way; it keeps the flush.
#[must_use]
pub fn analyze_builds_column_task(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    statement: &AnalyzeStatement,
) -> bool {
    let Ok(table) = find_statement_table(catalog, statement) else {
        return true;
    };
    let table = table.read();
    let runs_full_sampling =
        select_index_tasks(&table, statement).map_or(true, |selection| selection.run_full_sampling);
    runs_full_sampling
}

fn analyze_partition_ids(
    table: &tidb_model::table_info::TableInfo,
    requested: &[String],
) -> Result<Vec<i64>, ClusterAnalyzeError> {
    let Some(partition) = &table.partition else {
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        return Err(tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
            .generate_with_stack_by_args(&[])
            .into());
    };
    let partition = partition.read();
    let definitions = partition.definitions.snapshot();
    if requested.is_empty() {
        return Ok(definitions.iter().map(|definition| definition.id).collect());
    }
    requested
        .iter()
        .map(|requested_name| {
            definitions
                .iter()
                .find(|definition| {
                    definition.name.lowercase() == requested_name.go_to_lower()
                })
                .map(|definition| definition.id)
                .ok_or_else(|| {
                    ClusterAnalyzeError::Other(format!(
                        "can not found the specified partition name {requested_name} in the table definition"
                    ))
                })
        })
        .collect()
}

/// The explicit column list plus the columns an index, the handle or an
/// extended statistic needs; the second value names those added columns in
/// table order (Go `getMustAnalyzedColumns`' warning), when there are any.
fn final_column_choice(
    table: &tidb_model::table_info::TableInfo,
    columns: &AnalyzeColumnChoice,
) -> Result<(AnalyzeColumnChoice, Option<String>), ClusterAnalyzeError> {
    let AnalyzeColumnChoice::Explicit(names) = columns else {
        return Ok((columns.clone(), None));
    };
    let mut selected = HashSet::new();
    for name in names {
        let column = table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name.go_to_lower())
            .ok_or_else(|| {
                ClusterAnalyzeError::Other(format!(
                    "column `{name}` does not exist in `{}`",
                    table.name.original()
                ))
            })?;
        selected.insert(column.read().id);
    }
    let explicit = selected.clone();
    add_mandatory_columns(table, &mut selected, false);
    let missing: Vec<String> = table
        .cols()
        .iter_deref()
        .filter_map(|column| {
            let column = column.read();
            (selected.contains(&column.id) && !explicit.contains(&column.id))
                .then(|| column.name.original().to_owned())
        })
        .collect();
    let choice = AnalyzeColumnChoice::Explicit(
        table
            .cols()
            .iter_deref()
            .filter_map(|column| {
                let column = column.read();
                selected
                    .contains(&column.id)
                    .then(|| column.name.original().to_owned())
            })
            .collect(),
    );
    Ok((choice, (!missing.is_empty()).then(|| missing.join(","))))
}

/// Go `AnalyzeExec.saveAnalyzeOptions`: one `REPLACE INTO
/// mysql.analyze_options` on a restricted session. Callers treat failure as a
/// statement warning.
pub fn save_analyze_options(
    stats_pool: &dyn StatsSessionPool,
    resolved: &ResolvedClusterAnalyze,
) -> Result<(), SqlQueryError> {
    if !resolved.statement.persist_options || resolved.physical_options.is_empty() {
        return Ok(());
    }
    // Only to save table options if dynamic prune mode.
    let dynamic_prune = resolved.statement.dynamic_partition_prune;
    let to_save = resolved
        .physical_options
        .iter()
        .filter(|options| !options.is_partition || !dynamic_prune)
        .map(|options| {
            let (col_choice, col_ids) = match &options.columns {
                AnalyzeColumnChoice::Default => ("DEFAULT", String::new()),
                AnalyzeColumnChoice::All => ("ALL", String::new()),
                AnalyzeColumnChoice::Predicate => ("PREDICATE", String::new()),
                AnalyzeColumnChoice::Explicit(names) => {
                    let ids = names
                        .iter()
                        .filter_map(|name| {
                            resolved
                                .table
                                .cols()
                                .iter_deref()
                                .find(|column| column.read().name.lowercase() == name.go_to_lower())
                                .map(|column| column.read().id.to_string())
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    ("LIST", ids)
                }
            };
            (options, col_choice, col_ids)
        })
        .collect::<Vec<_>>();
    let mut sql = String::from(
        "REPLACE INTO mysql.analyze_options (table_id,sample_num,sample_rate,buckets,topn,column_choice,column_ids) VALUES ",
    );
    let mut args = Vec::with_capacity(to_save.len() * 7);
    for (index, (options, col_choice, col_ids)) in to_save.iter().enumerate() {
        sql.push_str("(%?,%?,%?,%?,%?,%?,%?)");
        if index < to_save.len() - 1 {
            sql.push(',');
        }
        let raw = options.raw;
        args.push(SqlArg::Signed(options.physical_id));
        args.push(SqlArg::Unsigned(raw.num_samples.map_or(0, |value| value as u64)));
        args.push(SqlArg::Float64(raw.sample_rate.unwrap_or(0.0)));
        args.push(SqlArg::Unsigned(raw.num_buckets.map_or(0, |value| value as u64)));
        args.push(SqlArg::Signed(raw.num_topn.map_or(-1, |value| value as i64)));
        args.push(SqlArg::from(*col_choice));
        args.push(SqlArg::from(col_ids.as_str()));
    }
    stats_pool.call_with_sctx(
        &mut |sctx| tidb_stats_handle_util::exec_rows(sctx, &sql, &args).map(|_| ()),
        &[],
    )
}

fn find_statement_table<'catalog>(
    catalog: &'catalog tidb_infoschema::SessionExtendedInfoSchema,
    statement: &AnalyzeStatement,
) -> Result<tidb_model::GoShared<tidb_model::table_info::TableInfo>, ClusterAnalyzeError> {
    catalog
        .stored_schemas()
        .iter()
        .find(|database| database.read().name.lowercase() == statement.schema.go_to_lower().as_str())
        .and_then(|database| {
            database
                .read().deprecated_tables
                .iter_deref()
                .find(|stored| stored.read().name.lowercase() == statement.table.go_to_lower().as_str())
        })
        .ok_or_else(|| {
            ClusterAnalyzeError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.generate_with_stack_by_args(
                    &[
                        statement.schema.as_str().into(),
                        statement.table.as_str().into(),
                    ],
                ),
            )
        })
        .and_then(|table| {
            // Go `buildAnalyzeTable` refuses both by the name as written.
            if table.read().is_view() {
                return Err(ClusterAnalyzeError::Other(format!(
                    "analyze view {} is not supported now",
                    statement.table
                )));
            }
            if table.read().is_sequence() {
                return Err(ClusterAnalyzeError::Other(format!(
                    "analyze sequence {} is not supported now",
                    statement.table
                )));
            }
            Ok(table)
        })
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

fn global_stats_merge_error(error: GlobalStatsMergeError) -> ClusterAnalyzeError {
    match error {
        GlobalStatsMergeError::Killed(error) => ClusterAnalyzeError::Killed(error),
        other => ClusterAnalyzeError::Other(other.to_string()),
    }
}

/// Go's non-enforced historical-meta gate for one global-statistics save.
#[must_use]
pub fn record_global_history_enabled(enabled: bool, initialized: bool) -> bool {
    enabled && initialized
}

/// Runs and commits one `ANALYZE TABLE`.
pub fn commit_cluster_analyze<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    resolved: &ResolvedClusterAnalyze,
    resource_group: &str,
    timeout: Duration,
    stats_lease: Duration,
    killer: &SqlKiller,
    record_history: &dyn Fn() -> bool,
    record_global_history: &dyn Fn() -> bool,
    jobs: &dyn AnalyzeJobLifecycle,
    stats_pool: &dyn StatsSessionPool,
) -> Result<ClusterAnalyzeReport, ClusterAnalyzeError> {
    let targets = if !resolved.run_full_sampling {
        Vec::new()
    } else if resolved.partitioned {
        resolved
            .physical_options
            .iter()
            .filter(|options| options.is_partition)
            .collect::<Vec<_>>()
    } else {
        resolved.physical_options.iter().take(1).collect::<Vec<_>>()
    };
    let target_ids = targets
        .iter()
        .map(|options| options.physical_id)
        .collect::<Vec<_>>();
    let cache_update_physical_ids = collect_cache_update_physical_ids(
        resolved.logical_table_id(),
        resolved.partitioned,
        &target_ids,
        resolved.independent_index_ids.len(),
    );
    // Go appends a multi-valued index's task inside its per-physical-table
    // loop, after that table's column task, and runs every column task
    // before every index task; the special global indexes follow.
    let mv_index_tasks = targets
        .iter()
        .flat_map(|options| {
            resolved
                .mv_index_ids()
                .into_iter()
                .map(move |index_id| (options.physical_id, index_id))
        })
        .collect::<Vec<_>>();
    let task_jobs = targets
        .iter()
        .map(|options| AnalyzeJobHandle::pending(jobs, &resolved.table_job_spec(options)))
        .chain(mv_index_tasks.iter().map(|(physical_id, index_id)| {
            AnalyzeJobHandle::pending(
                jobs,
                &resolved.partition_index_job_spec(*physical_id, *index_id),
            )
        }))
        .chain(resolved.independent_index_ids.iter().map(|index_id| {
            AnalyzeJobHandle::pending(jobs, &resolved.independent_index_job_spec(*index_id))
        }))
        .collect::<Vec<_>>();
    let mut reports = Vec::with_capacity(targets.len());
    for (position, options) in targets.iter().enumerate() {
        let job = &task_jobs[position];
        job.start(jobs);
        let selected_columns = resolved
            .selected_columns
            .get(&options.physical_id)
            .and_then(|selected| selected.as_ref());
        let predicate_columns_empty = resolved
            .predicate_columns_empty
            .get(&options.physical_id)
            .copied()
            .unwrap_or(false);
        let task_result = catch_unwind(AssertUnwindSafe(|| {
            commit_cluster_analyze_target(
                opener,
                stats_pool,
                &resolved.statement,
                options,
                timeout,
                stats_lease,
                record_history,
                &|count| job.update(jobs, count),
                selected_columns,
                predicate_columns_empty,
                killer,
                resource_group,
                &resolved.build_diagnostics,
            )
        }));
        let task_result = match task_result {
            Ok(result) => result,
            Err(panic) => {
                fail_analyze_jobs(jobs, &task_jobs[position..], "analyze worker panic");
                std::panic::resume_unwind(panic);
            }
        };
        match task_result {
            Ok(report) => {
                job.finish(jobs, None, AnalyzeJobKind::Table);
                reports.push(report);
            }
            Err(error) => {
                let failure = error.to_string();
                fail_analyze_jobs(jobs, &task_jobs[position..], &failure);
                return Err(error);
            }
        }
    }
    let mut reports = reports.into_iter();
    let mut report = reports.next().unwrap_or_else(|| ClusterAnalyzeReport {
        table_id: resolved.logical_table_id(),
        version: 0,
        scanned_rows: 0,
        sampled_rows: 0,
        sample_rate: 1.0,
        histogram_count: 0,
        bucket_count: 0,
        topn_count: 0,
        predicate_columns_empty: false,
        option_save_warning: None,
        global_stats_warnings: Vec::new(),
        ignored_partition_overrides: false,
        collected_all_for_index_target: false,
        skipped_columnar_indexes: Vec::new(),
        analyzed_column_ids: Vec::new(),
        analyzed_index_ids: Vec::new(),
        historical_stats_table_ids: Vec::new(),
        cache_update_physical_ids: Vec::new(),
        build_diagnostics: Vec::new(),
    });
    for next in reports {
        merge_analyze_report(&mut report, next);
    }
    // Go splits the indexes inside its per-physical-table loop, so each
    // analyzed physical table warns once per columnar index.
    for _ in &targets {
        report
            .skipped_columnar_indexes
            .extend(resolved.table.indices.iter_deref().filter_map(|index| {
                let index = index.read();
                (index.state == tidb_model::SchemaState::PUBLIC
                    && crate::cluster_analyze::is_columnar_index(&index))
                .then(|| index.name.lowercase().to_owned())
            }));
    }
    if resolved.partitioned {
        let table_id = resolved
            .physical_options
            .first()
            .expect("the logical table option is present")
            .physical_id;
        report.table_id = table_id;
    }
    let independent_options = &resolved
        .physical_options
        .first()
        .expect("the logical table option is present")
        .effective;
    for (offset, (physical_id, index_id)) in mv_index_tasks.iter().enumerate() {
        let position = targets.len() + offset;
        let job = &task_jobs[position];
        job.start(jobs);
        let task_result = catch_unwind(AssertUnwindSafe(|| {
            commit_cluster_independent_index(
                opener,
                stats_pool,
                &resolved.statement,
                Some(*physical_id),
                *index_id,
                independent_options,
                timeout,
                stats_lease,
                record_history,
                &|count| job.update(jobs, count),
            )
        }));
        let task_result = match task_result {
            Ok(result) => result,
            Err(panic) => {
                fail_analyze_jobs(jobs, &task_jobs[position..], "analyze worker panic");
                std::panic::resume_unwind(panic);
            }
        };
        match task_result {
            Ok(next) => {
                job.finish(jobs, None, AnalyzeJobKind::Table);
                merge_analyze_report(&mut report, next);
            }
            Err(error) => {
                fail_analyze_jobs(jobs, &task_jobs[position..], &error.to_string());
                return Err(error);
            }
        }
    }
    for (offset, index_id) in resolved.independent_index_ids.iter().enumerate() {
        let position = targets.len() + mv_index_tasks.len() + offset;
        let job = &task_jobs[position];
        job.start(jobs);
        let task_result = catch_unwind(AssertUnwindSafe(|| {
            commit_cluster_independent_index(
                opener,
                stats_pool,
                &resolved.statement,
                None,
                *index_id,
                independent_options,
                timeout,
                stats_lease,
                record_history,
                &|count| job.update(jobs, count),
            )
        }));
        let task_result = match task_result {
            Ok(result) => result,
            Err(panic) => {
                fail_analyze_jobs(jobs, &task_jobs[position..], "analyze worker panic");
                std::panic::resume_unwind(panic);
            }
        };
        match task_result {
            Ok(next) => {
                job.finish(jobs, None, AnalyzeJobKind::Table);
                merge_analyze_report(&mut report, next);
            }
            Err(error) => {
                let failure = error.to_string();
                fail_analyze_jobs(jobs, &task_jobs[position..], &failure);
                return Err(error);
            }
        }
    }
    if resolved.run_full_sampling
        && resolved.partitioned
        && resolved.statement.dynamic_partition_prune
    {
        let column_ids = report.analyzed_column_ids.clone();
        let index_ids = report.analyzed_index_ids.clone();
        if (!column_ids.is_empty() || !index_ids.is_empty())
            && !report
                .historical_stats_table_ids
                .contains(&resolved.logical_table_id())
        {
            report
                .historical_stats_table_ids
                .push(resolved.logical_table_id());
        }
        if !column_ids.is_empty() {
            let job = AnalyzeJobHandle::pending(jobs, &resolved.global_job_spec(None));
            job.start(jobs);
            let result = commit_cluster_global_stats(
                opener,
                stats_pool,
                resolved,
                &mut report,
                &column_ids,
                &[],
                timeout,
                stats_lease,
                killer,
                record_global_history,
            );
            job.finish(
                jobs,
                result.as_ref().err().map(ToString::to_string).as_deref(),
                AnalyzeJobKind::GlobalStatsMerge,
            );
            record_global_stats_result(&mut report.global_stats_warnings, result);
        }
        for index_id in index_ids {
            let job = AnalyzeJobHandle::pending(jobs, &resolved.global_job_spec(Some(index_id)));
            job.start(jobs);
            let result = commit_cluster_global_stats(
                opener,
                stats_pool,
                resolved,
                &mut report,
                &[],
                &[index_id],
                timeout,
                stats_lease,
                killer,
                record_global_history,
            );
            job.finish(
                jobs,
                result.as_ref().err().map(ToString::to_string).as_deref(),
                AnalyzeJobKind::GlobalStatsMerge,
            );
            record_global_stats_result(&mut report.global_stats_warnings, result);
        }
    }
    report.ignored_partition_overrides = resolved.ignored_partition_overrides;
    report.cache_update_physical_ids = cache_update_physical_ids;
    Ok(report)
}

fn collect_cache_update_physical_ids(
    logical_id: i64,
    partitioned: bool,
    physical_target_ids: &[i64],
    independent_index_task_count: usize,
) -> Vec<i64> {
    let mut ids = Vec::with_capacity(
        physical_target_ids.len() * if partitioned { 2 } else { 1 } + independent_index_task_count,
    );
    for physical_id in physical_target_ids {
        if partitioned {
            ids.push(logical_id);
        }
        ids.push(*physical_id);
    }
    ids.extend(std::iter::repeat_n(
        logical_id,
        independent_index_task_count,
    ));
    ids
}

fn record_global_stats_result(
    warnings: &mut Vec<(u16, String)>,
    result: Result<(), ClusterAnalyzeError>,
) {
    let Err(error) = result else {
        return;
    };
    match error {
        // Go `globalstats`: ANALYZE records the failed merge job and
        // continues; the missing-stats errors become statement warnings.
        ClusterAnalyzeError::Mysql(error)
            if error.equal(&tidb_datatype::types_errors::ERR_PARTITION_STATS_MISSING)
                || error.equal(&tidb_datatype::types_errors::ERR_PARTITION_COLUMN_STATS_MISSING) =>
        {
            warnings.push((error.code, error.message));
        }
        // The ANALYZE statement itself still succeeds.
        _ => {}
    }
}

fn merge_analyze_report(report: &mut ClusterAnalyzeReport, next: ClusterAnalyzeReport) {
    report.version = report.version.max(next.version);
    report.scanned_rows = report.scanned_rows.saturating_add(next.scanned_rows);
    report.sampled_rows = report.sampled_rows.saturating_add(next.sampled_rows);
    report.histogram_count += next.histogram_count;
    report.bucket_count += next.bucket_count;
    report.topn_count += next.topn_count;
    report.predicate_columns_empty |= next.predicate_columns_empty;
    for id in next.analyzed_column_ids {
        if !report.analyzed_column_ids.contains(&id) {
            report.analyzed_column_ids.push(id);
        }
    }
    for id in next.analyzed_index_ids {
        if !report.analyzed_index_ids.contains(&id) {
            report.analyzed_index_ids.push(id);
        }
    }
    for id in next.historical_stats_table_ids {
        if !report.historical_stats_table_ids.contains(&id) {
            report.historical_stats_table_ids.push(id);
        }
    }
}

fn commit_cluster_global_stats<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    stats_pool: &dyn StatsSessionPool,
    resolved: &ResolvedClusterAnalyze,
    report: &mut ClusterAnalyzeReport,
    column_ids: &[i64],
    index_ids: &[i64],
    timeout: Duration,
    stats_lease: Duration,
    killer: &SqlKiller,
    record_history: &dyn Fn() -> bool,
) -> Result<(), ClusterAnalyzeError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let (logical_id, total_count, modify_count, items) = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_info_schema(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, &resolved.statement)?;
        let table = table.read();
        let definitions = table
            .partition
            .as_ref()
            .expect("a dynamic target is partitioned")
            .read()
            .definitions
            .snapshot();
        let column_types = column_types_of(&table);
        let column_names = table
            .cols()
            .iter_deref()
            .map(|column| {
                let column = column.read();
                (column.id, column.name.original().to_owned())
            })
            .collect();
        let index_names = table
            .indices
            .iter_deref()
            .map(|index| {
                let index = index.read();
                (index.id, index.name.original().to_owned())
            })
            .collect();
        let mode = if resolved.statement.enable_async_merge_global_stats {
            GlobalStatsMergeMode::Async
        } else {
            GlobalStatsMergeMode::Blocking
        };
        if mode == GlobalStatsMergeMode::Async {
            // Go `AsyncMergePartitionStats2GlobalStats.MergePartitionStats2GlobalStats`:
            // `prepare` and the IO worker on one statistics session.
            let mut merged = Err(ClusterAnalyzeError::Other("global stats merge did not run".to_owned()));
            stats_pool
                .call_with_sctx(
                    &mut |sctx| {
                        merged = match merge_async_on(
                            sctx,
                            &table,
                            &definitions,
                            column_ids,
                            index_ids,
                            &column_types,
                            &column_names,
                            &index_names,
                            &resolved.statement,
                            killer,
                        ) {
                            Err(ClusterAnalyzeError::StatsRead(error)) => return Err(error),
                            result => result,
                        };
                        Ok(())
                    },
                    &[],
                )
                .map_err(ClusterAnalyzeError::Sql)?;
            let (total_count, modify_count, items) = merged?;
            (table.id, total_count, modify_count, items)
        } else {
            let mut partitions = Vec::with_capacity(definitions.len());
            for definition in definitions.iter() {
                let stats = load_table_partition_stats(stats_pool, &table, definition.id, stats_lease)?;
                partitions.push((definition.name.original().to_owned(), stats));
            }
            let total_count = partitions.iter().try_fold(0_i64, |total, (_, stats)| {
                let count = stats.as_ref().map_or(0, |stats| stats.hist_coll.realtime_count);
                Ok::<_, ClusterAnalyzeError>(total.wrapping_add(count))
            })?;
            let modify_count = partitions
                .iter()
                .filter_map(|(_, stats)| stats.as_ref())
                .fold(0_i64, |total, stats| total.wrapping_add(stats.hist_coll.modify_count));
            let items = match &resolved.statement.eval_context.session_zone() {
                SessionTimeZone::Local => merge_global_items(
                    &table,
                    Some(&chrono::Local),
                    &partitions,
                    column_ids,
                    index_ids,
                    &column_types,
                    &column_names,
                    &index_names,
                    &resolved.statement,
                    total_count,
                    mode,
                    killer,
                ),
                SessionTimeZone::Named(zone) => merge_global_items(
                    &table,
                    Some(zone),
                    &partitions,
                    column_ids,
                    index_ids,
                    &column_types,
                    &column_names,
                    &index_names,
                    &resolved.statement,
                    total_count,
                    mode,
                    killer,
                ),
                SessionTimeZone::Fixed { offset_secs, .. } => {
                    let zone = chrono::FixedOffset::east_opt(*offset_secs).ok_or_else(|| {
                        ClusterAnalyzeError::Other("invalid session timezone offset".to_owned())
                    })?;
                    merge_global_items(
                        &table,
                        Some(&zone),
                        &partitions,
                        column_ids,
                        index_ids,
                        &column_types,
                        &column_names,
                        &index_names,
                        &resolved.statement,
                        total_count,
                        mode,
                        killer,
                    )
                }
            }?;
            (table.id, total_count, modify_count, items)
        }
    };
    transaction
        .finish_without_writes()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;

    write_global_stats_items(
        items,
        |item| {
            commit_global_stats_item(stats_pool, logical_id, total_count, modify_count, item, stats_lease)
        },
        |write| {
            if record_history() {
                record_historical_stats_meta_on(stats_pool, write.0, ANALYZE_HISTORY_SOURCE, &[logical_id]);
            }
            report.version = report.version.max(write.0);
            report.histogram_count += write.1;
            report.bucket_count += write.2;
            report.topn_count += write.3;
        },
    )?;
    report.table_id = logical_id;
    Ok(())
}

/// The async global-stats merge on one statistics session: Go `prepare`,
/// then the IO and CPU workers.
#[allow(clippy::too_many_arguments)]
fn merge_async_on(
    sctx: &dyn tidb_stats_handle_util::StatsSessionContext,
    table: &tidb_model::TableInfo,
    definitions: &[tidb_model::PartitionDefinition],
    column_ids: &[i64],
    index_ids: &[i64],
    column_types: &std::collections::BTreeMap<i64, FieldType>,
    column_names: &std::collections::BTreeMap<i64, String>,
    index_names: &std::collections::BTreeMap<i64, String>,
    statement: &AnalyzeStatement,
    killer: &SqlKiller,
) -> Result<(i64, i64, Vec<ClusterStatsItem>), ClusterAnalyzeError> {
    let partitions = prepare_async_global_partitions(
        sctx,
        definitions
            .iter()
            .map(|definition| (definition.id, definition.name.original().to_owned())),
        !column_ids.is_empty(),
        !index_ids.is_empty(),
    )?;
    let total_count = partitions.iter().fold(0_i64, |total, partition| {
        total.wrapping_add(partition.row_count)
    });
    let modify_count = partitions.iter().fold(0_i64, |total, partition| {
        total.wrapping_add(partition.modify_count)
    });
    let items = match &statement.eval_context.session_zone() {
        SessionTimeZone::Local => merge_global_items_async(
            table,
            sctx,
            Some(&chrono::Local),
            &partitions,
            column_ids,
            index_ids,
            column_types,
            column_names,
            index_names,
            statement,
            total_count,
            killer,
        ),
        SessionTimeZone::Named(zone) => merge_global_items_async(
            table,
            sctx,
            Some(zone),
            &partitions,
            column_ids,
            index_ids,
            column_types,
            column_names,
            index_names,
            statement,
            total_count,
            killer,
        ),
        SessionTimeZone::Fixed { offset_secs, .. } => {
            let zone = chrono::FixedOffset::east_opt(*offset_secs).ok_or_else(|| {
                ClusterAnalyzeError::Other("invalid session timezone offset".to_owned())
            })?;
            merge_global_items_async(
                table,
                sctx,
                Some(&zone),
                &partitions,
                column_ids,
                index_ids,
                column_types,
                column_names,
                index_names,
                statement,
                total_count,
                killer,
            )
        }
    }?;
    Ok((total_count, modify_count, items))
}

/// A statistics-session read failure inside the merge's callback.
fn sql_exec_error(error: Box<dyn std::error::Error + Send + Sync>) -> ClusterAnalyzeError {
    ClusterAnalyzeError::StatsRead(error)
}

/// Go `statsReadWriter.LoadTablePartitionStats`: the partition's statistics
/// with every payload (`TableStatsFromStorage(loadAll)`), `None` without a
/// `stats_meta` row (Go `ErrPartitionStatsMissing`).
fn load_table_partition_stats(
    stats_pool: &dyn StatsSessionPool,
    table: &tidb_model::TableInfo,
    partition_id: i64,
    stats_lease: Duration,
) -> Result<Option<tidb_stats::Table>, ClusterAnalyzeError> {
    let mut loaded = None;
    stats_pool
        .call_with_sctx(
            &mut |sctx| {
                loaded = tidb_stats_handle_storage::read::table_stats_from_storage(
                    sctx,
                    0,
                    table,
                    partition_id,
                    true,
                    stats_lease,
                    None,
                )?;
                Ok(())
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .map_err(ClusterAnalyzeError::Sql)?;
    Ok(loaded)
}

/// Go `Table.GetStatsInfo`: one item's payload, `None` when the partition
/// has no such item or it is not analyzed.
fn partition_stats_info(table: &tidb_stats::Table, id: i64, is_index: bool) -> Option<PartitionStatsItem> {
    if is_index {
        let index = table.hist_coll.get_index(id)?;
        let index = index.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        (index.stats_version != 0).then(|| PartitionStatsItem {
            histogram: index.histogram.clone(),
            cmsketch: index.cmsketch.clone(),
            topn: index.top_n.clone(),
            fm_sketch: index.fm_sketch.clone(),
        })
    } else {
        let column = table.hist_coll.get_column(id)?;
        let column = column.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        (column.stats_version != 0).then(|| PartitionStatsItem {
            histogram: column.histogram.clone(),
            cmsketch: column.cmsketch.clone(),
            topn: column.top_n.clone(),
            fm_sketch: column.fm_sketch.clone(),
        })
    }
}

fn write_global_stats_items<T, E>(
    items: impl IntoIterator<Item = T>,
    mut write: impl FnMut(T) -> Result<(u64, usize, usize, usize), E>,
    mut record: impl FnMut((u64, usize, usize, usize)),
) -> Result<(), E> {
    let mut result = Ok(());
    for item in items {
        result = match write(item) {
            Ok(receipt) => {
                record(receipt);
                Ok(())
            }
            Err(error) => Err(error),
        };
    }
    result
}

#[derive(Clone, Debug)]
struct AsyncGlobalPartition {
    id: i64,
    name: String,
    row_count: i64,
    modify_count: i64,
    has_columns: bool,
    has_indexes: bool,
}

struct AsyncGlobalItem<'a> {
    id: i64,
    is_index: bool,
    field_type: FieldType,
    partitions: Vec<&'a AsyncGlobalPartition>,
}

/// Go `AsyncMergePartitionStats2GlobalStats.prepare` on a statistics session.
fn prepare_async_global_partitions(
    sctx: &dyn tidb_stats_handle_util::StatsSessionContext,
    definitions: impl IntoIterator<Item = (i64, String)>,
    merge_columns: bool,
    merge_indexes: bool,
) -> Result<Vec<AsyncGlobalPartition>, ClusterAnalyzeError> {
    let mut partitions = Vec::new();
    for (id, name) in definitions {
        let (count, modify, is_null) =
            tidb_stats_handle_storage::read::stats_meta_count_and_modify_count(sctx, id, false)
                .map_err(sql_exec_error)?;
        // Go: a partition counts once, unless it has no `stats_meta` row.
        let (modify_count, row_count) = if is_null { (0, 0) } else { (modify, count) };
        let has_columns = merge_columns
            && tidb_stats_handle_storage::read::check_skip_partition(sctx, id, false)
                .map_err(sql_exec_error)?;
        let has_indexes = merge_indexes
            && tidb_stats_handle_storage::read::check_skip_partition(sctx, id, true)
                .map_err(sql_exec_error)?;
        partitions.push(AsyncGlobalPartition {
            id,
            name,
            row_count,
            modify_count,
            has_columns,
            has_indexes,
        });
    }
    Ok(partitions)
}

#[allow(clippy::too_many_arguments)]
fn merge_global_items_async<TZ>(
    table: &tidb_model::TableInfo,
    sctx: &dyn tidb_stats_handle_util::StatsSessionContext,
    timezone: Option<&TZ>,
    partitions: &[AsyncGlobalPartition],
    column_ids: &[i64],
    index_ids: &[i64],
    column_types: &std::collections::BTreeMap<i64, FieldType>,
    column_names: &std::collections::BTreeMap<i64, String>,
    index_names: &std::collections::BTreeMap<i64, String>,
    statement: &AnalyzeStatement,
    total_count: i64,
    killer: &SqlKiller,
) -> Result<Vec<ClusterStatsItem>, ClusterAnalyzeError>
where
    TZ: TimeZone + Sync,
{
    let mut items = Vec::with_capacity(column_ids.len() + index_ids.len());
    for (is_index, ids) in [(false, column_ids), (true, index_ids)] {
        for &id in ids {
            let field_type = if is_index {
                FieldType::new(FieldTypeCode::Blob)
            } else {
                column_types.get(&id).cloned().ok_or_else(|| {
                    ClusterAnalyzeError::Other(format!("unknown analyzed column ID {id}"))
                })?
            };
            let mut active = Vec::with_capacity(partitions.len());
            for partition in partitions {
                if if is_index {
                    !partition.has_indexes
                } else {
                    !partition.has_columns
                } {
                    continue;
                }
                // Go `skipColumnPartition` (`CheckSkipColumnPartiion`).
                let exists =
                    tidb_stats_handle_storage::read::check_skip_column_partition(sctx, partition.id, is_index, id)
                        .map_err(sql_exec_error)?;
                if !exists {
                    if statement.skip_missing_partition_stats {
                        continue;
                    }
                    let name = if is_index {
                        index_names.get(&id)
                    } else {
                        column_names.get(&id)
                    }
                    .map_or("", String::as_str);
                    return Err(
                        tidb_datatype::types_errors::ERR_PARTITION_COLUMN_STATS_MISSING
                            .generate_with_stack_by_args(&[format!(
                                "table `{}` partition `{}` {} `{name}`",
                                statement.table,
                                partition.name,
                                if is_index { "index" } else { "column" }
                            )
                            .into()])
                            .into(),
                    );
                }
                active.push(partition);
            }
            items.push(AsyncGlobalItem {
                id,
                is_index,
                field_type,
                partitions: active,
            });
        }
    }

    type SketchMessage<T> = (usize, Option<T>);
    type HistogramMessage = (usize, Vec<Histogram>, Vec<Option<TopN>>);
    let merged = std::thread::scope(|scope| {
        // These capacities are part of pinned Go's worker contract.
        let (fm_sender, fm_receiver) = sync_channel::<SketchMessage<FmSketch>>(5);
        let (cms_sender, cms_receiver) = sync_channel::<SketchMessage<CmsSketch>>(5);
        let (histogram_sender, histogram_receiver) = sync_channel::<HistogramMessage>(0);
        let worker_items = &items;
        let requested_topn = statement.options.num_topn as u32;
        let expected_buckets = statement.options.num_buckets as usize;
        let merge_concurrency = statement.partition_merge_concurrency;
        let cpu_worker = scope.spawn(
            move || -> Result<Vec<Option<GlobalStatsItem>>, ClusterAnalyzeError> {
                let mut fm_sketches: Vec<Option<FmSketch>> = vec![None; worker_items.len()];
                while let Ok((index, source)) = fm_receiver.recv() {
                    if let Some(source) = source {
                        if let Some(destination) = &mut fm_sketches[index] {
                            merge_fm_sketch(Some(destination), Some(&source));
                        } else {
                            fm_sketches[index] = Some(source);
                        }
                    }
                }
                let global_ndvs = fm_sketches
                    .iter()
                    .map(|sketch| fm_sketch_ndv(sketch.as_ref()).min(total_count))
                    .collect::<Vec<_>>();
                drop(fm_sketches);

                let mut cmsketches: Vec<Option<CmsSketch>> = vec![None; worker_items.len()];
                while let Ok((index, source)) = cms_receiver.recv() {
                    if let Some(source) = source {
                        if let Some(destination) = &mut cmsketches[index] {
                            destination
                                .merge(&source)
                                .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
                        } else {
                            cmsketches[index] = Some(source);
                        }
                    }
                }

                let mut merged = vec![None; worker_items.len()];
                while let Ok((index, histograms, topns)) = histogram_receiver.recv() {
                    let item = &worker_items[index];
                    let ndv =
                        if tidb_stats_handle_globalstats::is_local_unique(table, item.is_index, item.id)
                        {
                            tidb_stats_handle_globalstats::unique_global_ndv(&histograms, total_count)
                        } else {
                            global_ndvs[index]
                        };
                    merged[index] = Some(
                        merge_partition_histogram_topn(
                            timezone,
                            2,
                            requested_topn,
                            expected_buckets,
                            &item.field_type,
                            item.is_index,
                            merge_concurrency,
                            ndv,
                            cmsketches[index].take(),
                            histograms,
                            topns,
                            killer,
                        )
                        .map_err(global_stats_merge_error)?,
                    );
                }
                Ok(merged)
            },
        );

        let mut fm_sender = Some(fm_sender);
        let mut cms_sender = Some(cms_sender);
        let mut histogram_sender = Some(histogram_sender);
        let io_result = catch_unwind(AssertUnwindSafe(|| -> Result<(), ClusterAnalyzeError> {
            for (index, item) in items.iter().enumerate() {
                for partition in &item.partitions {
                    let sketch = tidb_stats_handle_storage::read::fm_sketch_from_storage(
                        sctx,
                        partition.id,
                        item.is_index,
                        item.id,
                    )
                    .map_err(sql_exec_error)?;
                    if fm_sender
                        .as_ref()
                        .expect("FM sender remains open during the FM phase")
                        .send((index, sketch))
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            drop(fm_sender.take());

            for (index, item) in items.iter().enumerate() {
                for partition in &item.partitions {
                    let sketch = tidb_stats_handle_storage::read::cm_sketch_from_storage(
                        sctx,
                        partition.id,
                        item.is_index,
                        item.id,
                    )
                    .map_err(sql_exec_error)?;
                    if cms_sender
                        .as_ref()
                        .expect("CMS sender remains open during the CMS phase")
                        .send((index, sketch))
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            drop(cms_sender.take());

            for (index, item) in items.iter().enumerate() {
                let mut histograms = Vec::with_capacity(item.partitions.len());
                let mut topns = Vec::with_capacity(item.partitions.len());
                for partition in &item.partitions {
                    // Go `loadHistogramAndTopN`: `LoadHistogram`, then
                    // `TopNFromStorage`.
                    let histogram = tidb_stats_handle_storage::read::load_histogram(
                        sctx,
                        partition.id,
                        item.is_index,
                        item.id,
                        table,
                    )
                    .map_err(sql_exec_error)?
                    .ok_or_else(|| {
                        ClusterAnalyzeError::Other(format!(
                            "histogram {} disappeared from partition {}",
                            item.id, partition.id
                        ))
                    })?;
                    histograms.push(histogram);
                    topns.push(
                        tidb_stats_handle_storage::read::topn_from_storage(
                            sctx,
                            partition.id,
                            item.is_index,
                            item.id,
                        )
                        .map_err(sql_exec_error)?,
                    );
                }
                if !histograms.is_empty() || !topns.is_empty() {
                    if histogram_sender
                        .as_ref()
                        .expect("histogram sender remains open during its phase")
                        .send((index, histograms, topns))
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            drop(histogram_sender.take());
            Ok(())
        }))
        .map_err(|panic| {
            let detail = panic
                .downcast_ref::<&str>()
                .map_or_else(
                    || {
                        panic
                            .downcast_ref::<String>()
                            .map_or("unknown panic", String::as_str)
                    },
                    |message| *message,
                )
                .to_owned();
            ClusterAnalyzeError::Other(detail)
        })
        .and_then(|result| result);
        drop(fm_sender);
        drop(cms_sender);
        drop(histogram_sender);

        let cpu_result = cpu_worker
            .join()
            .map_err(|panic| {
                let detail = panic
                    .downcast_ref::<&str>()
                    .map_or_else(
                        || {
                            panic
                                .downcast_ref::<String>()
                                .map_or("unknown panic", String::as_str)
                        },
                        |message| *message,
                    )
                    .to_owned();
                ClusterAnalyzeError::Other(detail)
            })
            .and_then(|result| result);
        join_async_global_worker_results(io_result, cpu_result)
    })?;

    Ok(items
        .iter()
        .zip(merged)
        .filter_map(|(item, merged)| {
            let merged = merged?;
            Some(ClusterStatsItem {
                id: item.id,
                is_index: item.is_index,
                stats_ver: 2,
                flag: 0,
                load_status: StatsLoadedStatus::full_load(),
                histogram: merged.histogram?,
                topn: merged.topn,
                cms: merged.cmsketch,
                fm_sketch: None,
            })
        })
        .collect())
}

fn join_async_global_worker_results<T>(
    io_result: Result<(), ClusterAnalyzeError>,
    cpu_result: Result<T, ClusterAnalyzeError>,
) -> Result<T, ClusterAnalyzeError> {
    match (io_result, cpu_result) {
        (Ok(()), result) => result,
        (Err(io), Ok(_)) => Err(io),
        (Err(io), Err(cpu)) => Err(ClusterAnalyzeError::Other(format!("{io}\n{cpu}"))),
    }
}

fn merge_global_items<TZ: TimeZone + Sync>(
    table: &tidb_model::TableInfo,
    timezone: Option<&TZ>,
    partitions: &[(String, Option<tidb_stats::Table>)],
    column_ids: &[i64],
    index_ids: &[i64],
    column_types: &std::collections::BTreeMap<i64, FieldType>,
    column_names: &std::collections::BTreeMap<i64, String>,
    index_names: &std::collections::BTreeMap<i64, String>,
    statement: &AnalyzeStatement,
    total_count: i64,
    mode: GlobalStatsMergeMode,
    killer: &SqlKiller,
) -> Result<Vec<ClusterStatsItem>, ClusterAnalyzeError> {
    let mut result = Vec::with_capacity(column_ids.len() + index_ids.len());
    for (is_index, ids) in [(false, column_ids), (true, index_ids)] {
        for &id in ids {
            let mut inputs = Vec::with_capacity(partitions.len());
            for (partition_name, stats) in partitions {
                let Some(stats) = stats else {
                    if mode == GlobalStatsMergeMode::Async {
                        continue;
                    }
                    if statement.skip_missing_partition_stats {
                        continue;
                    }
                    return Err(tidb_datatype::types_errors::ERR_PARTITION_STATS_MISSING
                        .generate_with_stack_by_args(&[format!(
                            "table `{}` partition `{partition_name}`",
                            statement.table
                        )
                        .into()])
                        .into());
                };
                if mode == GlobalStatsMergeMode::Async
                    && if is_index {
                        stats.hist_coll.index_count() == 0
                    } else {
                        stats.hist_coll.column_count() == 0
                    }
                {
                    continue;
                }
                let item = partition_stats_info(stats, id, is_index);
                let Some(item) = item else {
                    if statement.skip_missing_partition_stats {
                        continue;
                    }
                    let name = if is_index {
                        index_names.get(&id)
                    } else {
                        column_names.get(&id)
                    }
                    .map_or("", String::as_str);
                    let detail = format!(
                        "table `{}` partition `{partition_name}` {} `{name}`",
                        statement.table,
                        if is_index { "index" } else { "column" }
                    );
                    let proto = if mode == GlobalStatsMergeMode::Async {
                        &tidb_datatype::types_errors::ERR_PARTITION_COLUMN_STATS_MISSING
                    } else {
                        &tidb_datatype::types_errors::ERR_PARTITION_STATS_MISSING
                    };
                    return Err(proto.generate_with_stack_by_args(&[detail.into()]).into());
                };
                let topn_count = item.topn.as_ref().map_or(0, |topn| topn.total_count());
                if mode == GlobalStatsMergeMode::Blocking
                    && stats.hist_coll.realtime_count > 0
                    && item.histogram.total_row_count() <= 0.0
                    && topn_count == 0
                {
                    if statement.skip_missing_partition_stats {
                        continue;
                    }
                    let name = if is_index {
                        index_names.get(&id)
                    } else {
                        column_names.get(&id)
                    }
                    .map_or("", String::as_str);
                    return Err(
                        tidb_datatype::types_errors::ERR_PARTITION_COLUMN_STATS_MISSING
                            .generate_with_stack_by_args(&[format!(
                                "table `{}` partition `{partition_name}` {} `{name}`",
                                statement.table,
                                if is_index { "index" } else { "column" }
                            )
                            .into()])
                            .into(),
                    );
                }
                inputs.push(item);
            }
            let field_type = if is_index {
                FieldType::new(FieldTypeCode::Blob)
            } else {
                column_types.get(&id).cloned().ok_or_else(|| {
                    ClusterAnalyzeError::Other(format!("unknown analyzed column ID {id}"))
                })?
            };
            let unique_ndv =
                tidb_stats_handle_globalstats::is_local_unique(table, is_index, id).then(|| {
                    tidb_stats_handle_globalstats::unique_global_ndv(
                        inputs.iter().map(|item| &item.histogram),
                        total_count,
                    )
                });
            let merged = merge_partition_stats_item(
                timezone,
                2,
                statement.options.num_topn as u32,
                statement.options.num_buckets as usize,
                total_count,
                &field_type,
                is_index,
                mode,
                statement.partition_merge_concurrency,
                inputs,
                killer,
            )
            .map_err(global_stats_merge_error)?;
            if let Some(mut histogram) = merged.histogram {
                if let Some(ndv) = unique_ndv {
                    histogram.ndv = ndv;
                }
                result.push(ClusterStatsItem {
                    id,
                    is_index,
                    stats_ver: 2,
                    flag: 0,
                    load_status: StatsLoadedStatus::full_load(),
                    histogram,
                    topn: merged.topn,
                    cms: merged.cmsketch,
                    fm_sketch: None,
                });
            }
        }
    }
    Ok(result)
}

/// Go `statsReadWriter.SaveColOrIdxStatsToStorage` for one merged global
/// histogram, with its `handleSlowStatsSaving`.
fn commit_global_stats_item(
    stats_pool: &dyn StatsSessionPool,
    table_id: i64,
    row_count: i64,
    modify_count: i64,
    item: ClusterStatsItem,
    stats_lease: Duration,
) -> Result<(u64, usize, usize, usize), ClusterAnalyzeError> {
    let (histogram_count, bucket_count, topn_count) = saved_item_counts([&item]);
    let version = stats_read_writer::save_col_or_idx_stats_to_storage(
        stats_pool,
        stats_lease,
        table_id,
        row_count,
        modify_count,
        &item,
        STATS_VERSION_2,
        true,
    )
    .map_err(ClusterAnalyzeError::Sql)?;
    Ok((version, histogram_count, bucket_count, topn_count))
}

/// The histogram, bucket and TopN rows a save of `items` writes.
fn saved_item_counts<'a>(
    items: impl IntoIterator<Item = &'a ClusterStatsItem>,
) -> (usize, usize, usize) {
    items.into_iter().fold((0, 0, 0), |(histograms, buckets, topn), item| {
        (
            histograms + 1,
            buckets + item.histogram.buckets.len(),
            topn + item.topn.as_ref().map_or(0, |topn| topn.entries().len()),
        )
    })
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

fn commit_cluster_analyze_target<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    stats_pool: &dyn StatsSessionPool,
    statement: &AnalyzeStatement,
    options: &crate::analyze::PhysicalAnalyzeOptions,
    timeout: Duration,
    stats_lease: Duration,
    record_history: &dyn Fn() -> bool,
    progress: &dyn Fn(i64),
    selected_columns_for_task: Option<&HashSet<i64>>,
    predicate_columns_empty_for_task: bool,
    killer: &SqlKiller,
    resource_group: &str,
    build_diagnostics: &[(bool, u16, String)],
) -> Result<ClusterAnalyzeReport, ClusterAnalyzeError> {
    let mut sample_transaction = opener
        .begin()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let sample_ts = sample_transaction.start_ts();
    let sampled = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut sample_transaction, timeout);
        let catalog = load_info_schema(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, statement)?.clone();
        let table = table.read();
        // Go `buildAnalyzeSamplingPushdown`:
        // `statsHandle.StatsMetaCountAndModifyCount` on the statistics
        // handle's sessions, the counts the save reconciles against.
        let (base_count, base_modify_count, no_meta) =
            stats_meta_count_and_modify_count(stats_pool, options.physical_id)?;
        let realtime_count = (!no_meta).then_some(base_count);
        let mut target_statement = statement.clone();
        target_statement.columns = options.columns.clone();
        target_statement.raw_options = options.raw;
        target_statement.options = options.effective;
        target_statement.options.memory_quota = statement.options.memory_quota;
        let report = crate::cluster_analyze::sampling::analyze_table(
            opener,
            &table,
            options.physical_id,
            &target_statement,
            realtime_count,
            sample_ts,
            selected_columns_for_task,
            crate::cluster_analyze::sampling::scan_concurrency(
                statement.scan_concurrency,
                opener.store().pd_http_client(),
                timeout,
            ),
            timeout,
            killer,
            resource_group,
            progress,
        )
        .map_err(ClusterAnalyzeError::Analyze)?;
        (report, base_count, base_modify_count)
    };
    let (report, base_count, base_modify_count) = sampled;

    sample_transaction
        .finish_without_writes()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;

    // Go samples at `AnalyzeResults.Snapshot`, then saves through the
    // statistics handle's later transaction, which reconciles the deltas
    // committed after sampling against BaseCount/BaseModifyCnt.
    let stats_ver = stats_read_writer::save_analyze_result_to_storage(
        stats_pool,
        stats_lease,
        &AnalyzeResults {
                stats: &report.stats,
                is_partition: options.is_partition,
                base_count,
                base_modify_count,
                stats_ver: STATS_VERSION_2,
                for_mv_index_or_global_index: false,
        },
        statement.analyze_snapshot,
    )
    .map_err(ClusterAnalyzeError::Sql)?;
    // A zero `statsVer`: a newer analysis was saved first, and Go skipped
    // this one.
    let saved = stats_ver != 0;
    let (histogram_count, bucket_count, topn_count) = if saved {
        saved_item_counts(report.stats.columns.iter().chain(&report.stats.indexes))
    } else {
        (0, 0, 0)
    };
    let receipt = ClusterAnalyzeReport {
        table_id: report.stats.table_id,
        version: stats_ver,
        scanned_rows: report.scanned_rows,
        sampled_rows: report.sampled_rows,
        sample_rate: report.sample_rate,
        histogram_count,
        bucket_count,
        topn_count,
        predicate_columns_empty: predicate_columns_empty_for_task,
        option_save_warning: None,
        global_stats_warnings: Vec::new(),
        ignored_partition_overrides: false,
        collected_all_for_index_target: statement.index_names.is_some(),
        skipped_columnar_indexes: Vec::new(),
        analyzed_column_ids: report.stats.columns.iter().map(|item| item.id).collect(),
        analyzed_index_ids: report.stats.indexes.iter().map(|item| item.id).collect(),
        historical_stats_table_ids: if saved {
            vec![report.stats.table_id]
        } else {
            Vec::new()
        },
        cache_update_physical_ids: Vec::new(),
        build_diagnostics: build_diagnostics.to_vec(),
    };
    if saved {
        record_analyze_history_if(stats_pool, receipt.table_id, stats_ver, record_history);
    }
    Ok(receipt)
}

fn commit_cluster_independent_index<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    stats_pool: &dyn StatsSessionPool,
    statement: &AnalyzeStatement,
    physical_id: Option<i64>,
    index_id: i64,
    options: &crate::analyze::AnalyzeOptions,
    timeout: Duration,
    stats_lease: Duration,
    record_history: &dyn Fn() -> bool,
    progress: &dyn Fn(i64),
) -> Result<ClusterAnalyzeReport, ClusterAnalyzeError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let start_ts = transaction.start_ts();
    let (report, is_partition) = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_info_schema(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, statement)?.clone();
        let table = table.read();
        let index = table
            .indices
            .iter_deref()
            .find_map(|index| {
                let index = index.read();
                (index.id == index_id && index.state == tidb_model::SchemaState::PUBLIC)
                    .then(|| index.clone())
            })
            .ok_or_else(|| {
                ClusterAnalyzeError::Other(format!(
                    "independent ANALYZE index {index_id} no longer exists on table '{}'",
                    table.name.original()
                ))
            })?;
        let report = analyze_independent_index_with_progress(
            &mut snapshot,
            physical_id.unwrap_or(table.id),
            &index,
            options,
            start_ts,
            progress,
        )
        .map_err(ClusterAnalyzeError::Analyze)?;
        (report, physical_id.is_some_and(|id| id != table.id))
    };
    transaction
        .finish_without_writes()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    // Go's special multi-valued / global index task: saved as
    // `ForMVIndexOrGlobalIndex`, which moves only the versions of an
    // existing `stats_meta` row (and then reports no `statsVer`).
    let stats_ver = stats_read_writer::save_analyze_result_to_storage(
        stats_pool,
        stats_lease,
        &AnalyzeResults {
                stats: &report.stats,
                is_partition,
                base_count: 0,
                base_modify_count: 0,
                stats_ver: STATS_VERSION_2,
                for_mv_index_or_global_index: true,
        },
        statement.analyze_snapshot,
    )
    .map_err(ClusterAnalyzeError::Sql)?;
    let (histogram_count, bucket_count, topn_count) =
        saved_item_counts(report.stats.columns.iter().chain(&report.stats.indexes));
    let receipt = ClusterAnalyzeReport {
        build_diagnostics: Vec::new(),
        table_id: report.stats.table_id,
        version: stats_ver,
        scanned_rows: report.scanned_rows,
        sampled_rows: report.sampled_rows,
        sample_rate: report.sample_rate,
        histogram_count,
        bucket_count,
        topn_count,
        predicate_columns_empty: false,
        option_save_warning: None,
        global_stats_warnings: Vec::new(),
        ignored_partition_overrides: false,
        collected_all_for_index_target: false,
        skipped_columnar_indexes: Vec::new(),
        analyzed_column_ids: Vec::new(),
        analyzed_index_ids: vec![index_id],
        historical_stats_table_ids: vec![report.stats.table_id],
        cache_update_physical_ids: Vec::new(),
    };
    if stats_ver != 0 {
        record_analyze_history_if(stats_pool, receipt.table_id, stats_ver, record_history);
    }
    Ok(receipt)
}

/// Go's `RecordHistoricalStatsMeta(statsVer, "analyze", ...)` after a
/// versioned save.
fn record_analyze_history_if(
    stats_pool: &dyn StatsSessionPool,
    table_id: i64,
    version: u64,
    record_history: &dyn Fn() -> bool,
) {
    if record_history() {
        record_historical_stats_meta_on(stats_pool, version, ANALYZE_HISTORY_SOURCE, &[table_id]);
    }
}

/// Go `util.StatsMetaHistorySourceAnalyze`.
const ANALYZE_HISTORY_SOURCE: &str = "analyze";

fn selected_columns(
    stats_pool: &dyn StatsSessionPool,
    table: &tidb_model::table_info::TableInfo,
    statement: &AnalyzeStatement,
) -> Result<(Option<HashSet<i64>>, bool), ClusterAnalyzeError> {
    match &statement.columns {
        AnalyzeColumnChoice::All => Ok((None, false)),
        AnalyzeColumnChoice::Default => {
            selected_columns_for_choice(stats_pool, table, &statement.default_columns)
        }
        AnalyzeColumnChoice::Predicate => {
            // Go `statsHandle.GetPredicateColumns`, in a wrapped transaction
            // of its own.
            let mut columns = Vec::new();
            stats_pool
                .call_with_sctx(
                    &mut |sctx| {
                        columns = get_predicate_columns(sctx, table.id)?;
                        Ok(())
                    },
                    &[tidb_stats_handle_util::FLAG_WRAP_TXN],
                )
                .map_err(ClusterAnalyzeError::Sql)?;
            let empty = columns.is_empty();
            Ok((Some(columns.into_iter().collect()), empty))
        }
        AnalyzeColumnChoice::Explicit(names) => {
            let mut selected = HashSet::new();
            for name in names {
                let column = table
                    .cols()
                    .iter_deref()
                    .find(|column| column.read().name.lowercase() == name.go_to_lower())
                    .ok_or_else(|| {
                        ClusterAnalyzeError::Other(
                            tidb_syssession::stats_table_rows::StatsWriteError::MissingTable(
                                format!(
                                    "column `{name}` does not exist in `{}`",
                                    table.name.original()
                                ),
                            )
                            .to_string(),
                        )
                    })?;
                selected.insert(column.read().id);
            }
            Ok((Some(selected), false))
        }
    }
}

fn selected_columns_for_choice(
    stats_pool: &dyn StatsSessionPool,
    table: &tidb_model::table_info::TableInfo,
    choice: &AnalyzeColumnChoice,
) -> Result<(Option<HashSet<i64>>, bool), ClusterAnalyzeError> {
    let statement = AnalyzeStatement {
        schema: String::new(),
        table: String::new(),
        auto_analyze: false,
        skip_column_types: std::collections::BTreeSet::new(),
        partitions: Vec::new(),
        index_names: None,
        columns: choice.clone(),
        raw_options: Default::default(),
        persist_options: false,
        default_columns: AnalyzeColumnChoice::All,
        dynamic_partition_prune: true,
        skip_missing_partition_stats: true,
        analyze_snapshot: false,
        enable_async_merge_global_stats: true,
        partition_merge_concurrency: 1,
        scan_concurrency: tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY,
        store_batch_size: tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE,
        eval_context: crate::StmtContext::for_query().into(),
        options: Default::default(),
    };
    selected_columns(stats_pool, table, &statement)
}

/// Go `getMustAnalyzedColumns` (and, for the PREDICATE choice,
/// `getPredicateColumns`) folded into `selected`: ordinary index columns and
/// the handle, plus the base columns a VIRTUAL generated column among them
/// reads, transitively (`addColumnsWithVirtualExprs` follows `VirtualExpr`,
/// which only virtual generated columns carry -- a stored generated column
/// is decoded from storage, so its base columns stay skippable).
/// `expand_selected` also follows the columns already in `selected` (the
/// predicate columns); an explicit column list is not expanded.
fn add_mandatory_columns(
    table: &tidb_model::table_info::TableInfo,
    selected: &mut HashSet<i64>,
    expand_selected: bool,
) {
    // Columns whose base columns are followed: Go's `cols.data` before the
    // virtual-expression walk.
    let mut roots = if expand_selected {
        selected.clone()
    } else {
        HashSet::new()
    };
    for index in table.indices.iter_deref() {
        let index = index.read();
        if index.state != tidb_model::SchemaState::PUBLIC || index.mv_index {
            continue;
        }
        for indexed in index.columns.iter_deref() {
            let offset = indexed.read().offset;
            if let Some(column) = table.cols().get(offset as usize) {
                let id = column.read().id;
                selected.insert(id);
                roots.insert(id);
            }
        }
    }
    if table.pk_is_handle {
        for column in table.cols().iter_deref() {
            let column = column.read();
            if column.get_flag() & u64::from(tidb_datatype::FieldTypeFlags::PRI_KEY) != 0 {
                selected.insert(column.id);
            }
        }
    }
    loop {
        let before = roots.len();
        for column in table.cols().iter_deref() {
            let column = column.read();
            if !roots.contains(&column.id) || !column.is_generated() || column.generated_stored {
                continue;
            }
            for dependency in column.dependences.snapshot() {
                if let Some(base) = table.cols().iter_deref().find(|candidate| {
                    dependency.as_utf8().is_ok_and(|dependency| {
                        candidate.read().name.lowercase() == dependency.go_to_lower()
                    })
                }) {
                    let id = base.read().id;
                    selected.insert(id);
                    roots.insert(id);
                }
            }
        }
        if roots.len() == before {
            break;
        }
    }
}

/// Go `PlanBuilder.filterSkipColumnTypes`: skipped scalar types are omitted
/// unless an ordinary index or handle requires them; vector columns are
/// always omitted, and virtual generated columns depending on an omitted
/// base column are omitted as well.
fn filter_skipped_column_types(
    table: &tidb_model::table_info::TableInfo,
    selected: Option<HashSet<i64>>,
    skip_types: &std::collections::BTreeSet<String>,
) -> Option<HashSet<i64>> {
    let columns = table.cols().iter_deref().collect::<Vec<_>>();
    let mut selected = selected.unwrap_or_else(|| {
        columns
            .iter()
            .map(|column| column.read().id)
            .collect::<HashSet<_>>()
    });
    let mut mandatory = HashSet::new();
    add_mandatory_columns(table, &mut mandatory, false);
    let mut skipped_names = HashSet::new();
    for column in &columns {
        let column = column.read();
        let vector = column.field_type.code() == tidb_datatype::FieldTypeCode::VectorFloat32;
        let skipped = skip_types.contains(tidb_datatype::type_to_str(
            column.field_type.code(),
            column.field_type.charset_name(),
        ));
        if vector || (skipped && !mandatory.contains(&column.id)) {
            selected.remove(&column.id);
            skipped_names.insert(column.name.lowercase().to_owned());
        }
    }
    for column in &columns {
        let column = column.read();
        if !column.is_generated() || (column.generated_stored && mandatory.contains(&column.id)) {
            continue;
        }
        if column.dependences.snapshot().iter().any(|dependency| {
            dependency
                .as_utf8()
                .is_ok_and(|dependency| skipped_names.contains(&dependency.to_ascii_lowercase()))
        }) {
            selected.remove(&column.id);
        }
    }
    (selected.len() != columns.len()).then_some(selected)
}

/// Go's analyze `JobInfo` for a table task: `[auto ]analyze table`, then
/// `all indexes` / `index[es] <names>` over the public indexes, then
/// `all columns` or the selected columns, then the bucket, TopN and sample
/// settings.
#[must_use]
pub fn analyze_table_job_info(
    auto_analyze: bool,
    indexes: &[String],
    public_index_count: usize,
    columns: Option<&[String]>,
    non_temp_columns: usize,
    options: &crate::analyze::PhysicalAnalyzeOptions,
) -> String {
    let mut job_info = if auto_analyze {
        "auto analyze table".to_owned()
    } else {
        "analyze table".to_owned()
    };
    if !indexes.is_empty() {
        if indexes.len() == public_index_count {
            job_info.push_str(" all indexes");
        } else {
            job_info.push_str(if indexes.len() == 1 {
                " index "
            } else {
                " indexes "
            });
            job_info.push_str(&indexes.join(", "));
        }
        job_info.push(',');
    }
    match columns {
        Some(columns) if columns.len() < non_temp_columns => {
            job_info.push_str(if columns.len() == 1 {
                " column "
            } else {
                " columns "
            });
            job_info.push_str(&columns.join(", "));
        }
        _ => job_info.push_str(" all columns"),
    }
    job_info.push_str(&format!(
        " with {} buckets, {} topn, ",
        options.effective.num_buckets, options.effective.num_topn
    ));
    if options.effective.num_samples > 0 {
        job_info.push_str(&format!("{} samples", options.effective.num_samples));
    } else {
        job_info.push_str(&format!(
            "{} samplerate",
            options.effective.sample_rate.unwrap_or(1.0)
        ));
    }
    job_info
}

/// Go `PlanBuilder.getSavedAnalyzeOpts`: one physical table's persisted
/// options, read on a restricted session.
pub fn get_saved_analyze_opts(
    stats_pool: &dyn StatsSessionPool,
    table: &TableInfo,
    physical_id: i64,
) -> Result<Option<SavedAnalyzeOptions>, SqlQueryError> {
    let mut rows = Vec::new();
    stats_pool.call_with_sctx(
        &mut |sctx| {
            rows = tidb_stats_handle_util::exec_rows(
                sctx,
                "select sample_num,sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id = %?",
                &[SqlArg::Signed(physical_id)],
            )?
            .0;
            Ok(())
        },
        &[],
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let int64 = |index: usize| row.get(index).map_or(0, Datum::get_int64);
    let sample_num = Some(int64(0)).filter(|value| *value > 0);
    let sample_rate = match row.get(1) {
        Some(Datum::Real(value)) => Some(*value),
        _ => None,
    }
    .filter(|value| *value > 0.0);
    let buckets = Some(int64(2)).filter(|value| *value > 0);
    let topn = Some(int64(3)).filter(|value| *value >= 0);
    let raw = AnalyzeOptionOverrides {
        num_buckets: buckets.and_then(|value| isize::try_from(value).ok()),
        num_topn: topn.and_then(|value| isize::try_from(value).ok()),
        num_samples: sample_num.and_then(|value| usize::try_from(value).ok()),
        sample_rate,
    };
    let col_type: &[u8] = match row.get(4) {
        Some(Datum::Enum(value, _)) => value.name().as_bytes(),
        _ => b"",
    };
    let columns = match col_type {
        b"ALL" => AnalyzeColumnChoice::All,
        b"LIST" => {
            let col_id_strs = row.get(5).and_then(Datum::as_raw_bytes).unwrap_or_default();
            let mut names = Vec::new();
            for id in String::from_utf8_lossy(col_id_strs)
                .split(',')
                .filter_map(|id| id.parse::<i64>().ok())
            {
                if let Some(column) = table
                    .cols()
                    .iter_deref()
                    .find(|column| column.read().id == id)
                {
                    names.push(column.read().name.original().to_owned());
                }
            }
            AnalyzeColumnChoice::Explicit(names)
        }
        b"PREDICATE" => AnalyzeColumnChoice::Predicate,
        _ => AnalyzeColumnChoice::Default,
    };
    Ok(Some(SavedAnalyzeOptions { raw, columns }))
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
        analyze_partition_ids, automatic_sample_rate,
        collect_cache_update_physical_ids, fail_analyze_jobs, filter_skipped_column_types,
        final_column_choice, merge_global_items, record_global_history_enabled,
        select_index_tasks,
        write_global_stats_items, AnalyzeJobHandle, AnalyzeJobKind, AnalyzeJobLifecycle,
        AnalyzeJobProgress, AnalyzeJobSpec, ApproximateTableCountProvider, ClusterAnalyzeError,
        ANALYZE_JOB_MAX_DELTA,
    };
    use std::collections::{BTreeMap, BTreeSet, HashSet};
    use std::thread;
    use std::time::Duration;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
    use crate::analyze::{AnalyzeColumnChoice, AnalyzeStatement};
    use tidb_model::column::ColumnInfo;
    use tidb_model::go_runtime::GoShared;
    use tidb_model::index::{IndexColumn, IndexInfo};
    use tidb_model::partition::{PartitionDefinition, PartitionInfo};
    use tidb_model::table_info::TableInfo;

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

    #[test]
    fn analyze_failure_finishes_running_and_undispatched_jobs() {
        let jobs = RecordingAnalyzeJobs::default();
        let spec = |partition: &str| AnalyzeJobSpec {
            schema: "test".to_owned(),
            table: "t".to_owned(),
            partition: partition.to_owned(),
            job_info: "analyze table all columns".to_owned(),
        };
        let handles = [
            AnalyzeJobHandle::pending(&jobs, &spec("p0")),
            AnalyzeJobHandle::pending(&jobs, &spec("p1")),
        ];
        handles[0].start(&jobs);
        fail_analyze_jobs(&jobs, &handles, "sampling failed");

        assert_eq!(
            *jobs.events.lock().unwrap(),
            [
                "insert:1:p0",
                "insert:2:p1",
                "start:1",
                "finish:1:0:sampling failed:Table",
                "finish:2:0:sampling failed:Table",
            ]
        );
    }
    use tidb_model::SchemaState;
    use tidb_stats::histogram::{Bucket, Histogram};
    use tidb_stats::FmSketch;
    use tidb_stats_handle_globalstats::GlobalStatsMergeMode;
    use tidb_stats::MAX_SKETCH_SIZE;
    use tidb_txnkv::rpc::UnaryCallContext;
    use tidb_sqlkiller::SqlKiller;

    fn global_partition_item(id: i64, count: i64) -> (i64, tidb_stats::Column) {
        (id, tidb_stats::Column {
            stats_version: 2,
            histogram: Histogram {
                id,
                ndv: 2,
                null_count: 0,
                last_update_version: 7,
                tot_col_size: count,
                correlation: 0.0,
                buckets: vec![Bucket {
                    count,
                    repeat: 1,
                    ndv: 2,
                    lower_bound: tidb_datatype::Datum::Int(1),
                    upper_bound: tidb_datatype::Datum::Int(4),
                }],
                ..Default::default()
            },
            fm_sketch: Some(FmSketch::from_raw_parts(
                0,
                MAX_SKETCH_SIZE,
                [id as u64, id as u64 + 1],
            )),
            ..Default::default()
        })
    }

    fn global_partition_stats(item: Option<(i64, tidb_stats::Column)>) -> tidb_stats::Table {
        tidb_stats::Table::from_analyzed_items(11, 5, 1, item, [])
    }

    fn global_statement(skip_missing: bool) -> AnalyzeStatement {
        AnalyzeStatement {
            schema: "test".to_owned(),
            table: "t".to_owned(),
            auto_analyze: false,
            skip_column_types: std::collections::BTreeSet::new(),
            partitions: Vec::new(),
            index_names: None,
            columns: AnalyzeColumnChoice::All,
            raw_options: Default::default(),
            persist_options: true,
            default_columns: AnalyzeColumnChoice::All,
            dynamic_partition_prune: true,
            skip_missing_partition_stats: skip_missing,
            analyze_snapshot: false,
            enable_async_merge_global_stats: true,
            partition_merge_concurrency: 1,
            scan_concurrency: tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY,
            store_batch_size: tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE,
            eval_context: crate::StmtContext::for_query().into(),
            options: Default::default(),
        }
    }

    fn index_task_table() -> TableInfo {
        let mut a = ColumnInfo::new(1, "a", FieldType::new(FieldTypeCode::Varchar));
        a.offset = 0;
        TableInfo {
            id: 42,
            name: CiString::new("t"),
            columns: vec![a].into(),
            indices: vec![
                IndexInfo {
                    id: 7,
                    name: CiString::new("special"),
                    state: SchemaState::PUBLIC,
                    global: true,
                    columns: vec![IndexColumn {
                        name: CiString::new("a"),
                        offset: 0,
                        length: 3,
                        ..IndexColumn::default()
                    }]
                    .into(),
                    ..IndexInfo::default()
                },
                IndexInfo {
                    id: 8,
                    name: CiString::new("ordinary"),
                    state: SchemaState::PUBLIC,
                    columns: vec![IndexColumn {
                        name: CiString::new("a"),
                        offset: 0,
                        ..IndexColumn::default()
                    }]
                    .into(),
                    ..IndexInfo::default()
                },
            ]
            .into(),
            ..TableInfo::default()
        }
    }

    #[test]
    fn special_global_index_tasks_follow_pinned_go_selection() {
        let table = index_task_table();
        let mut statement = global_statement(true);
        let tasks = select_index_tasks(&table, &statement).unwrap();
        assert!(tasks.run_full_sampling);
        assert_eq!(tasks.independent_index_ids, vec![7]);

        statement.index_names = Some(vec!["special".to_owned()]);
        let tasks = select_index_tasks(&table, &statement).unwrap();
        assert!(!tasks.run_full_sampling);
        assert_eq!(tasks.independent_index_ids, vec![7]);

        statement.index_names = Some(vec!["ordinary".to_owned()]);
        let tasks = select_index_tasks(&table, &statement).unwrap();
        assert!(tasks.run_full_sampling);
        assert_eq!(tasks.independent_index_ids, vec![7]);
    }

    #[test]
    fn skipped_analyze_types_keep_index_columns_and_always_drop_vectors() {
        let mut plain = ColumnInfo::new(1, "plain", FieldType::new(FieldTypeCode::Long));
        plain.offset = 0;
        let mut json = ColumnInfo::new(2, "json_col", FieldType::new(FieldTypeCode::Json));
        json.offset = 1;
        let mut indexed_blob =
            ColumnInfo::new(3, "indexed_blob", FieldType::new(FieldTypeCode::Blob));
        indexed_blob.offset = 2;
        let mut vector = ColumnInfo::new(
            4,
            "vector_col",
            FieldType::new(FieldTypeCode::VectorFloat32),
        );
        vector.offset = 3;
        let table = TableInfo {
            columns: vec![plain, json, indexed_blob, vector].into(),
            indices: vec![IndexInfo {
                id: 7,
                state: tidb_model::SchemaState::PUBLIC,
                columns: vec![IndexColumn {
                    name: CiString::new("indexed_blob"),
                    offset: 2,
                    ..IndexColumn::default()
                }]
                .into(),
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        };

        assert_eq!(
            filter_skipped_column_types(
                &table,
                None,
                &BTreeSet::from(["json".to_owned(), "blob".to_owned()]),
            ),
            Some(HashSet::from([1, 3]))
        );
    }

    #[test]
    fn special_global_index_partition_error_and_empty_name_branch_match_go() {
        let mut table = index_task_table();
        table.indices = vec![table
            .indices
            .iter_deref()
            .next()
            .expect("the special index exists")
            .read()
            .clone()]
        .into();
        let mut statement = global_statement(true);
        statement.index_names = Some(Vec::new());
        let tasks = select_index_tasks(&table, &statement).unwrap();
        assert!(!tasks.run_full_sampling);
        assert!(tasks.independent_index_ids.is_empty());

        statement.index_names = Some(vec!["special".to_owned()]);
        statement.partitions = vec!["p0".to_owned()];
        assert_eq!(
            select_index_tasks(&table, &statement)
                .unwrap_err()
                .to_string(),
            "Analyze global index 'special' can't work with analyze specified partitions"
        );
    }

    #[test]
    fn stats_ndv_batch_local_unique_global_ndv_sums_partition_counts() {
        let table = TableInfo {
            columns: vec![ColumnInfo::new(
                1,
                "a",
                FieldType::new(FieldTypeCode::LongLong),
            )]
            .into(),
            indices: vec![IndexInfo {
                id: 2,
                unique: true,
                state: SchemaState::PUBLIC,
                columns: vec![IndexColumn {
                    offset: 0,
                    length: -1,
                    ..IndexColumn::default()
                }]
                .into(),
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        };
        let partitions = vec![
            (
                "p0".to_owned(),
                Some(global_partition_stats(Some(global_partition_item(1, 5)))),
            ),
            (
                "p1".to_owned(),
                Some(global_partition_stats(Some(global_partition_item(1, 5)))),
            ),
        ];
        for mode in [GlobalStatsMergeMode::Blocking, GlobalStatsMergeMode::Async] {
            let merged = merge_global_items(
                &table,
                Some(&chrono::Utc),
                &partitions,
                &[1],
                &[],
                &BTreeMap::from([(1, FieldType::new(FieldTypeCode::LongLong))]),
                &BTreeMap::new(),
                &BTreeMap::new(),
                &global_statement(false),
                10,
                mode,
                &SqlKiller::default(),
            )
            .unwrap();
            assert_eq!(merged[0].histogram.ndv, 4);
        }
    }

    #[test]
    fn dynamic_global_merge_uses_all_partitions_and_skip_policy() {
        let killer = SqlKiller::default();
        let column_types = BTreeMap::from([(1, FieldType::new(FieldTypeCode::LongLong))]);
        let column_names = BTreeMap::from([(1, "a".to_owned())]);
        let partitions = vec![
            (
                "p0".to_owned(),
                Some(global_partition_stats(Some(global_partition_item(1, 5)))),
            ),
            ("p1".to_owned(), None),
        ];
        let error = merge_global_items(
            &TableInfo::default(),
            Some(&chrono::Utc),
            &partitions,
            &[1],
            &[],
            &column_types,
            &column_names,
            &BTreeMap::new(),
            &global_statement(false),
            5,
            GlobalStatsMergeMode::Blocking,
            &killer,
        )
        .expect_err("missing partition stats are reported when skipping is disabled");
        assert_eq!(
            error.to_string(),
            "Build global-level stats failed due to missing partition-level stats: table `t` partition `p1`"
        );

        let merged = merge_global_items(
            &TableInfo::default(),
            Some(&chrono::Utc),
            &partitions,
            &[1],
            &[],
            &column_types,
            &column_names,
            &BTreeMap::new(),
            &global_statement(true),
            5,
            GlobalStatsMergeMode::Async,
            &killer,
        )
        .expect("the configured missing partition is skipped");
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].histogram.ndv, 2);

        let merged = merge_global_items(
            &TableInfo::default(),
            Some(&chrono::Utc),
            &partitions,
            &[1],
            &[],
            &column_types,
            &column_names,
            &BTreeMap::new(),
            &global_statement(false),
            5,
            GlobalStatsMergeMode::Async,
            &killer,
        )
        .expect("the async worker always skips a partition with no histogram rows");
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn dynamic_global_merge_distinguishes_missing_item_payload() {
        let killer = SqlKiller::default();
        let column_types = BTreeMap::from([(1, FieldType::new(FieldTypeCode::LongLong))]);
        let column_names = BTreeMap::from([(1, "a".to_owned())]);
        let mut empty = global_partition_item(1, 0);
        empty.1.histogram.buckets.clear();
        empty.1.top_n = None;
        let partitions = vec![("p0".to_owned(), Some(global_partition_stats(Some(empty))))];
        let error = merge_global_items(
            &TableInfo::default(),
            Some(&chrono::Utc),
            &partitions,
            &[1],
            &[],
            &column_types,
            &column_names,
            &BTreeMap::new(),
            &global_statement(false),
            5,
            GlobalStatsMergeMode::Blocking,
            &killer,
        )
        .expect_err("an empty item on a non-empty partition is reported");
        assert_eq!(
            error.to_string(),
            "Build global-level stats failed due to missing partition-level column stats: table `t` partition `p0` column `a`, please run analyze table to refresh columns of all partitions"
        );

        let merged = merge_global_items(
            &TableInfo::default(),
            Some(&chrono::Utc),
            &partitions,
            &[1],
            &[],
            &column_types,
            &column_names,
            &BTreeMap::new(),
            &global_statement(false),
            5,
            GlobalStatsMergeMode::Async,
            &killer,
        )
        .expect("the async worker only checks whether the histogram row exists");
        assert_eq!(merged.len(), 1);
        assert!(merged[0].histogram.buckets.is_empty());

        let partitions = vec![(
            "p0".to_owned(),
            Some(global_partition_stats(Some(global_partition_item(2, 5)))),
        )];
        let error = merge_global_items(
            &TableInfo::default(),
            Some(&chrono::Utc),
            &partitions,
            &[1],
            &[],
            &column_types,
            &column_names,
            &BTreeMap::new(),
            &global_statement(false),
            5,
            GlobalStatsMergeMode::Async,
            &killer,
        )
        .expect_err("the async worker reports a missing item when sibling rows exist");
        assert_eq!(
            error.to_string(),
            "Build global-level stats failed due to missing partition-level column stats: table `t` partition `p0` column `a`, please run analyze table to refresh columns of all partitions"
        );
    }

    #[test]
    fn global_stats_storage_attempts_every_item_and_returns_the_final_attempt_result() {
        let mut attempted = Vec::new();
        let mut recorded = Vec::new();
        write_global_stats_items(
            [1_u64, 2, 3, 4],
            |item| {
                attempted.push(item);
                match item {
                    1 => Err("first"),
                    3 => Err("last"),
                    _ => Ok((item, 1, 2, 3)),
                }
            },
            |receipt| recorded.push(receipt),
        )
        .expect("the final successful item clears an earlier storage error in Go");

        assert_eq!(attempted, vec![1, 2, 3, 4]);
        assert_eq!(recorded, vec![(2, 1, 2, 3), (4, 1, 2, 3)]);

        let error = write_global_stats_items(
            [1_u64, 2, 3],
            |item| match item {
                1 => Err("first"),
                3 => Err("last"),
                _ => Ok((item, 1, 2, 3)),
            },
            |_| {},
        )
        .expect_err("Go returns an error when the final attempted storage write fails");
        assert_eq!(error, "last");
    }

    #[test]
    fn global_history_requires_both_the_live_switch_and_initialized_cache() {
        assert!(!record_global_history_enabled(false, false));
        assert!(!record_global_history_enabled(false, true));
        assert!(!record_global_history_enabled(true, false));
        assert!(record_global_history_enabled(true, true));
    }

    #[test]
    fn partition_targets_follow_go_definition_and_requested_order() {
        let table = TableInfo {
            name: CiString::new("t"),
            partition: Some(GoShared::new(PartitionInfo {
                definitions: vec![
                    PartitionDefinition {
                        id: 11,
                        name: CiString::new("p0"),
                        ..PartitionDefinition::default()
                    },
                    PartitionDefinition {
                        id: 12,
                        name: CiString::new("p1"),
                        ..PartitionDefinition::default()
                    },
                ]
                .into(),
                ..PartitionInfo::default()
            })),
            ..TableInfo::default()
        };

        assert_eq!(analyze_partition_ids(&table, &[]).unwrap(), vec![11, 12]);
        assert_eq!(
            analyze_partition_ids(&table, &["P1".to_owned(), "p0".to_owned()]).unwrap(),
            vec![12, 11]
        );
        assert_eq!(
            analyze_partition_ids(&table, &["missing".to_owned()])
                .unwrap_err()
                .to_string(),
            "can not found the specified partition name missing in the table definition"
        );

        let plain = TableInfo {
            name: CiString::new("plain"),
            ..TableInfo::default()
        };
        assert!(analyze_partition_ids(&plain, &[]).unwrap().is_empty());
        assert_eq!(
            analyze_partition_ids(&plain, &["p0".to_owned()])
                .unwrap_err()
                .to_string(),
            "Partition management on a not partitioned table is not possible"
        );
    }

    #[test]
    fn post_analyze_cache_ids_come_from_tasks_not_history_targets() {
        // Pinned AnalyzeExec appends the logical ID and partition ID for each
        // partition task, including static mode where no global-history
        // target is generated.
        assert_eq!(
            collect_cache_update_physical_ids(10, true, &[11, 12], 0),
            vec![10, 11, 10, 12]
        );
        // Independent index tasks each carry the logical table ID. The cache
        // update itself sorts and deduplicates this list like Go.
        assert_eq!(
            collect_cache_update_physical_ids(10, false, &[10], 2),
            vec![10, 10, 10]
        );
        assert_eq!(
            collect_cache_update_physical_ids(10, true, &[], 1),
            vec![10]
        );
    }

    #[test]
    fn explicit_column_choice_is_saved_after_mandatory_columns_in_schema_order() {
        let mut primary_type = FieldType::new(FieldTypeCode::LongLong);
        primary_type.add_flags(FieldTypeFlags::PRI_KEY);
        let mut a = ColumnInfo::new(1, "a", primary_type);
        a.offset = 0;
        let mut b = ColumnInfo::new(2, "b", FieldType::new(FieldTypeCode::LongLong));
        b.offset = 1;
        let mut c = ColumnInfo::new(3, "c", FieldType::new(FieldTypeCode::LongLong));
        c.offset = 2;
        let table = TableInfo {
            name: CiString::new("t"),
            pk_is_handle: true,
            columns: vec![a, b, c].into(),
            indices: vec![IndexInfo {
                name: CiString::new("kb"),
                state: SchemaState::PUBLIC,
                columns: vec![IndexColumn {
                    name: CiString::new("b"),
                    offset: 1,
                    ..IndexColumn::default()
                }]
                .into(),
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        };

        assert_eq!(
            final_column_choice(&table, &AnalyzeColumnChoice::Explicit(vec!["c".to_owned()]))
                .expect("the final LIST is valid"),
            (
                AnalyzeColumnChoice::Explicit(
                    vec!["a".to_owned(), "b".to_owned(), "c".to_owned(),]
                ),
                Some("a,b".to_owned())
            )
        );
    }

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

    #[test]
    fn simultaneous_async_global_worker_failures_are_joined_in_go_order() {
        let error = super::join_async_global_worker_results::<()>(
            Err(ClusterAnalyzeError::Other("io failure".to_owned())),
            Err(ClusterAnalyzeError::Other("cpu failure".to_owned())),
        )
        .expect_err("both worker errors are returned");
        assert_eq!(error.to_string(), "io failure\ncpu failure");
    }

    #[test]
    fn global_merge_failures_do_not_fail_analyze_and_missing_stats_accumulate_warnings() {
        let mut warnings = Vec::new();
        super::record_global_stats_result(
            &mut warnings,
            Err(ClusterAnalyzeError::Other("storage failed".to_owned())),
        );
        assert!(warnings.is_empty(), "ordinary merge errors stay job-local");

        super::record_global_stats_result(
            &mut warnings,
            Err(tidb_datatype::types_errors::ERR_PARTITION_STATS_MISSING
                .generate_with_stack_by_args(&["p0".into()])
                .into()),
        );
        super::record_global_stats_result(
            &mut warnings,
            Err(
                tidb_datatype::types_errors::ERR_PARTITION_COLUMN_STATS_MISSING
                    .generate_with_stack_by_args(&["p1 index i".into()])
                    .into(),
            ),
        );
        assert_eq!(
            warnings.iter().map(|warning| warning.0).collect::<Vec<_>>(),
            vec![8131, 8244]
        );
    }
}
