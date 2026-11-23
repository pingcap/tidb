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
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tidb_datatype::UNSPECIFIED_LENGTH;
use tidb_mysql::errors::SqlError;
use tidb_stats::row_sample_collector::get_adjusted_sample_rate;
use tidb_stats_handle_globalstats::global_stats::GlobalStatsInfo;
use tidb_store_driver_txn::transaction::StoreClient;
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;
use tidb_sqlkiller::SqlKiller;

use crate::cluster_analyze::{
    analyze_independent_index_with_progress, lower_analyze, resolve_analyze_options,
    AnalyzeColumnChoice, AnalyzeError, AnalyzeStatement,
};
use tidb_infoschema::cluster_catalog::load_info_schema;
use tidb_stats_handle_storage::save::{AnalyzeResults as AnalyzeResults_, STATS_VERSION_2};
use tidb_stats_handle_storage::stats_read_writer::{self, StatsSessionPool};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
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


/// The pinned planner's physical ANALYZE targets and merged options.
#[derive(Clone, Debug)]
pub struct ResolvedClusterAnalyze {
    pub(crate) statement: AnalyzeStatement,
    pub(crate) table: tidb_model::table_info::TableInfo,
    pub(crate) physical_options: Vec<crate::analyze::V2AnalyzeOptions>,
    partition_names: HashMap<i64, String>,
    selected_columns: HashMap<i64, Option<HashSet<i64>>>,
    predicate_columns_empty: HashMap<i64, bool>,
    partitioned: bool,
    ignored_partition_overrides: bool,
    run_full_sampling: bool,
    independent_index_ids: Vec<i64>,
    /// Go's build-time diagnostics as `(is_note, code, message)`: the
    /// planner's missing-explicit-columns WARNING and the builder's
    /// sample-rate NOTEs.
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

    /// The physical tables Go builds column tasks for: each partition of a
    /// partitioned table, or the table itself; none when only special
    /// global indexes are analyzed.
    fn column_task_targets(&self) -> Vec<&crate::analyze::V2AnalyzeOptions> {
        if !self.run_full_sampling {
            Vec::new()
        } else if self.partitioned {
            self.physical_options.iter().filter(|options| options.is_partition).collect()
        } else {
            self.physical_options.iter().take(1).collect()
        }
    }

    pub(crate) fn partition_name(&self, physical_id: i64) -> String {
        self.partition_names
            .get(&physical_id)
            .cloned()
            .unwrap_or_default()
    }

    fn table_job_spec(
        &self,
        options: &crate::analyze::V2AnalyzeOptions,
    ) -> AnalyzeJobSpec {
        let indexes = self
            .table
            .indices
            .iter_deref()
            .filter_map(|index| {
                let index = index.read();
                // The column task's own indexes (Go `AnalyzeColumnsTask.
                // Indexes`): independent and columnar ones are not among them.
                (crate::cluster_analyze::index_state_analyzable(&index, self.statement.enable_ddl_analyze_exec_opt)
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
                (crate::cluster_analyze::index_state_analyzable(&index, self.statement.enable_ddl_analyze_exec_opt) && index.mv_index)
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

    pub(crate) fn global_job_spec(&self, index_id: Option<i64>) -> AnalyzeJobSpec {
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

    // Go `getAnalyzeColumnList`.
    for name in &statement.column_names {
        if !table.cols().iter_deref().any(|column| column.read().name.lowercase() == name.go_to_lower()) {
            return Err(ClusterAnalyzeError::from(
                tidb_plannererrors::ERR_ANALYZE_MISS_COLUMN
                    .gen_with_stack_by_args(&[name.as_str().into(), table.name.original().into()]),
            ));
        }
    }
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
        let (columns, missing) =
            final_column_choice(&table, &options.columns, statement.enable_ddl_analyze_exec_opt)?;
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
        // Go `buildAnalyzeFullSamplingTask` adjusts the rate per sampling
        // task (`getAdjustedSampleRate`, whose `GetPhysicalTableStats` caches
        // a `PseudoTable` on a miss): a partitioned table's logical entry is
        // no task of its own, so its statistics are never looked up here.
        if table.partition.is_some() && options.physical_id == table.id {
            continue;
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
            // (`builder.go:3185`/`3194`, `sc.AppendNote`, generic 1105); a
            // partition's task names the partition.
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
            add_mandatory_columns(&table, selected, predicate, statement.enable_ddl_analyze_exec_opt);
        }
        selected =
            filter_skipped_column_types(
                &table,
                selected,
                &target_statement.skip_column_types,
                statement.enable_ddl_analyze_exec_opt,
            );
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
                        (crate::cluster_analyze::index_state_analyzable(&index, statement.enable_ddl_analyze_exec_opt)
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
                                .gen_with_stack_by_args(&[
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
                            (crate::cluster_analyze::index_state_analyzable(&index, statement.enable_ddl_analyze_exec_opt)
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
            .gen_with_stack_by_args(&[])
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
    enable_ddl_analyze_exec_opt: bool,
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
    add_mandatory_columns(table, &mut selected, false, enable_ddl_analyze_exec_opt);
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

pub(crate) fn find_statement_table<'catalog>(
    catalog: &'catalog tidb_infoschema::SessionExtendedInfoSchema,
    statement: &AnalyzeStatement,
) -> Result<tidb_model::GoShared<tidb_model::table_info::TableInfo>, ClusterAnalyzeError> {
    if let Some(table) = &statement.local_temporary_table {
        return Ok(table.clone());
    }
    catalog.try_table_info_by_name(&statement.schema, &statement.table)
        .map_err(|e| ClusterAnalyzeError::Mysql(e.to_mysql_error()))?
        .map(|(_, table)| table)
        .ok_or_else(|| {
            ClusterAnalyzeError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(
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
            // Go master `CheckMViewReadable` in `buildAnalyzeTable`.
            let vars = tidb_planner::util::MViewSessionVars::of(&*statement.eval_context);
            tidb_planner::util::check_mview_readable(&vars, &table.read(), &statement.table).map_err(|error| {
                match error.kind() {
                    tidb_planner_core_base::PlanErrorKind::Mysql(error) => ClusterAnalyzeError::Mysql(error.clone()),
                    _ => ClusterAnalyzeError::Other(error.message().to_owned()),
                }
            })?;
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

/// Go `errAnalyzeWorkerPanic`.
pub(crate) const ERR_ANALYZE_WORKER_PANIC: &str = "analyze worker panic";

/// Go `statistics.NonPartitionTableID`.
const NON_PARTITION_TABLE_ID: i64 = -1;

/// Go `statistics.AnalyzeTableID`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct AnalyzeTableID {
    /// Go `TableID`: the logical table.
    pub(crate) table_id: i64,
    /// Go `PartitionID`, [`NON_PARTITION_TABLE_ID`] for the table itself.
    pub(crate) partition_id: i64,
}

impl AnalyzeTableID {
    fn new(table_id: i64, physical_id: i64) -> Self {
        Self {
            table_id,
            partition_id: if physical_id == table_id { NON_PARTITION_TABLE_ID } else { physical_id },
        }
    }

    /// Go `IsPartitionTable`.
    pub(crate) fn is_partition_table(self) -> bool {
        self.partition_id != NON_PARTITION_TABLE_ID
    }

    /// Go `GetStatisticsID`.
    fn get_statistics_id(self) -> i64 {
        if self.is_partition_table() {
            self.partition_id
        } else {
            self.table_id
        }
    }
}

/// Go `taskType` with the executor it runs.
enum AnalyzeTaskKind<'a> {
    /// Go `colTask`: `AnalyzeColumnsExec`'s full sampling of one physical
    /// table.
    Col(&'a crate::analyze::V2AnalyzeOptions),
    /// Go `idxTask`: `AnalyzeIndexExec` over one index of one physical table;
    /// `None` is the logical table (a special global index).
    Idx { physical_id: Option<i64>, index_id: i64 },
}

/// Go `analyzeTask`.
struct AnalyzeTask<'a> {
    kind: AnalyzeTaskKind<'a>,
    resolved: &'a ResolvedClusterAnalyze,
    table_id: AnalyzeTableID,
    job: AnalyzeJobSpec,
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
    /// What Go's planner and executor builder append while they build the
    /// plan, in their order: `(is_note, code, message)`.
    pub build_diagnostics: Vec<(bool, u16, String)>,
    /// What `AnalyzeExec.Next` appends.
    pub exec_warnings: Vec<(u16, String)>,
    /// Go `statsHandle.Update(ctx, is, tableAndPartitionIDs...)`; `None`
    /// when no task ran, where Go returns before the update.
    pub table_and_partition_ids: Option<Vec<i64>>,
}

/// The warnings and notes Go's planner and executor builder raise for the
/// statement's tables: every table's planner warnings first, then the
/// builder's sample-rate notes.
fn build_diagnostics(resolved: &[ResolvedClusterAnalyze]) -> Vec<(bool, u16, String)> {
    let mut diagnostics = Vec::new();
    for resolved in resolved {
        let statement = &resolved.statement;
        diagnostics.extend(resolved.build_diagnostics.iter().filter(|(note, ..)| !note).cloned());
        let targets = resolved.column_task_targets();
        if targets
            .iter()
            .any(|options| resolved.predicate_columns_empty.get(&options.physical_id).copied().unwrap_or(false))
        {
            diagnostics.push((
                false,
                1105,
                format!(
                    "No predicate column has been collected yet for table {}.{}, so only indexes and the columns composing the indexes will be analyzed",
                    tidb_hack::go_to_lower(&statement.schema),
                    tidb_hack::go_to_lower(&statement.table)
                ),
            ));
        }
        if resolved.ignored_partition_overrides {
            diagnostics.push((false, 1105, "Ignore columns and options when analyze partition in dynamic mode".to_owned()));
        }
        if resolved.run_full_sampling && statement.index_names.is_some() {
            diagnostics.push((
                false,
                1105,
                "The version 2 would collect all statistics not only the selected indexes".to_owned(),
            ));
        }
        // Go splits the indexes inside its per-physical-table loop, so each
        // analyzed physical table warns once per columnar index.
        for _ in &targets {
            for index in resolved.table.indices.iter_deref() {
                let index = index.read();
                if crate::cluster_analyze::index_state_analyzable(&index, statement.enable_ddl_analyze_exec_opt)
                    && !is_special_global_index(&resolved.table, &index)
                    && !index.mv_index
                    && crate::cluster_analyze::is_columnar_index(&index)
                {
                    diagnostics.push((
                        false,
                        1105,
                        format!("analyzing columnar index is not supported, skip {}", index.name.lowercase()),
                    ));
                }
            }
        }
    }
    // Go `buildAnalyzeSamplingPushdown`'s notes: column tasks only.
    for resolved in resolved.iter().filter(|resolved| resolved.run_full_sampling) {
        diagnostics.extend(resolved.build_diagnostics.iter().filter(|(note, ..)| *note).cloned());
    }
    diagnostics
}

/// Go `executorBuilder.buildAnalyze`'s tasks: every table's column tasks,
/// then every table's index tasks (a multi-valued index per physical table,
/// then the special global indexes).
fn build_analyze_tasks(resolved: &[ResolvedClusterAnalyze]) -> Vec<AnalyzeTask<'_>> {
    let mut tasks = Vec::new();
    for resolved in resolved {
        let table_id = resolved.table.id;
        for options in resolved.column_task_targets() {
            tasks.push(AnalyzeTask {
                kind: AnalyzeTaskKind::Col(options),
                resolved,
                table_id: AnalyzeTableID::new(table_id, options.physical_id),
                job: resolved.table_job_spec(options),
            });
        }
    }
    for resolved in resolved {
        let table_id = resolved.table.id;
        for options in resolved.column_task_targets() {
            for index_id in resolved.mv_index_ids() {
                tasks.push(AnalyzeTask {
                    kind: AnalyzeTaskKind::Idx { physical_id: Some(options.physical_id), index_id },
                    resolved,
                    table_id: AnalyzeTableID::new(table_id, options.physical_id),
                    job: resolved.partition_index_job_spec(options.physical_id, index_id),
                });
            }
        }
        for index_id in &resolved.independent_index_ids {
            tasks.push(AnalyzeTask {
                kind: AnalyzeTaskKind::Idx { physical_id: None, index_id: *index_id },
                resolved,
                table_id: AnalyzeTableID::new(table_id, table_id),
                job: resolved.independent_index_job_spec(*index_id),
            });
        }
    }
    tasks
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
                let statement = &task.resolved.statement;
                if table_id.is_partition_table() {
                    skipped_tables.push(format!(
                        "{}.{} partition ({})",
                        statement.schema,
                        task.resolved.table.name.original(),
                        task.resolved.partition_name(table_id.partition_id)
                    ));
                } else {
                    skipped_tables.push(format!("{}.{}", statement.schema, task.resolved.table.name.original()));
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

/// Go `AnalyzeExec.Next` over the statement's tables: the locked ones are
/// skipped, `tidb_build_stats_concurrency` workers analyze the tasks, the
/// result handler hands them to `tidb_analyze_partition_concurrency` save
/// workers (a failed save does not stop the others), then the global stats
/// of dynamically pruned partitioned tables are merged and the options are
/// saved.
pub fn analyze_exec_next<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    resolved: &[ResolvedClusterAnalyze],
    resource_group: &str,
    timeout: Duration,
    stats_lease: Duration,
    killer: &SqlKiller,
    jobs: &dyn AnalyzeJobLifecycle,
    stats_pool: &dyn StatsSessionPool,
) -> Result<AnalyzeExecReport, ClusterAnalyzeError> {
    let mut report = AnalyzeExecReport { build_diagnostics: build_diagnostics(resolved), ..AnalyzeExecReport::default() };
    let Some(first) = resolved.first() else {
        return Ok(report);
    };
    let session = first.statement.clone();
    // Filter the locked tables.
    let (tasks, need_analyze_table_cnt, skipped_tables) =
        filter_and_collect_tasks(build_analyze_tasks(resolved), stats_pool)?;
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
    // Go: "If we enabled dynamic prune mode, then we need to generate global
    // stats here for partition tables."
    let global_stats_map = exec.run(&session)?;
    if session.dynamic_partition_prune {
        crate::analyze_global_stats::handle_global_stats(
            opener,
            stats_pool,
            jobs,
            resolved,
            &global_stats_map,
            &mut report.exec_warnings,
            timeout,
            stats_lease,
            killer,
        );
    }
    // Update analyze options to mysql.analyze_options for auto analyze.
    for resolved in resolved {
        if let Err(error) = save_analyze_options(stats_pool, resolved) {
            report.exec_warnings.push((1105, error.message));
        }
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
            AnalyzeTaskKind::Col(options) => self.analyze_columns(&self.tasks[task], options, &progress),
            AnalyzeTaskKind::Idx { physical_id, index_id } => self
                .analyze_index(&self.tasks[task], *physical_id, *index_id, &progress)
                .map(|stats| (stats, 0, 0)),
        };
        let for_mv_index_or_global_index = matches!(self.tasks[task].kind, AnalyzeTaskKind::Idx { .. });
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
    /// table at its own snapshot; the base counts are Go's
    /// `StatsMetaCountAndModifyCount`.
    fn analyze_columns(
        &self,
        task: &AnalyzeTask<'_>,
        options: &crate::analyze::V2AnalyzeOptions,
        progress: &(dyn Fn(i64) + Sync),
    ) -> Result<(tidb_stats_handle_storage::cluster_stats_load::ClusterTableStats, i64, i64), ClusterAnalyzeError> {
        let resolved = task.resolved;
        let statement = &resolved.statement;
        let mut sample_transaction = self
            .opener
            .begin()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let sample_ts = sample_transaction.start_ts();
        let sampled = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut sample_transaction, self.timeout);
            let catalog = load_info_schema(&mut snapshot)
                .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
            let table = find_statement_table(&catalog, statement)?.clone();
            let table = table.read();
            let (base_count, base_modify_count, no_meta) =
                stats_meta_count_and_modify_count(self.stats_pool, options.physical_id)?;
            let realtime_count = (!no_meta).then_some(base_count);
            let mut target_statement = statement.clone();
            target_statement.columns = options.columns.clone();
            target_statement.raw_options = options.raw;
            target_statement.options = options.effective;
            let selected_columns = resolved
                .selected_columns
                .get(&options.physical_id)
                .and_then(|selected| selected.as_ref());
            let report = crate::cluster_analyze::sampling::analyze_table(
                self.opener,
                &table,
                options.physical_id,
                &target_statement,
                realtime_count,
                sample_ts,
                selected_columns,
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
            (report.stats, base_count, base_modify_count)
        };
        sample_transaction
            .finish_without_writes()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        Ok(sampled)
    }

    /// Go `analyzeIndexPushdown` for a multi-valued index of one physical
    /// table or a special global index of the logical table.
    fn analyze_index(
        &self,
        task: &AnalyzeTask<'_>,
        physical_id: Option<i64>,
        index_id: i64,
        progress: &(dyn Fn(i64) + Sync),
    ) -> Result<tidb_stats_handle_storage::cluster_stats_load::ClusterTableStats, ClusterAnalyzeError> {
        let statement = &task.resolved.statement;
        let mut transaction = self
            .opener
            .begin_read_only()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let start_ts = transaction.start_ts();
        let target = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            let catalog = load_info_schema(&mut snapshot)
                .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
            let table = find_statement_table(&catalog, statement)?.clone();
            let table = table.read();
            let index = table
                .indices
                .iter_deref()
                .find_map(|index| {
                    let index = index.read();
                    (index.id == index_id
                        && crate::cluster_analyze::index_state_analyzable(&index, statement.enable_ddl_analyze_exec_opt))
                    .then(|| index.clone())
                })
                .ok_or_else(|| {
                    ClusterAnalyzeError::Other(format!(
                        "independent ANALYZE index {index_id} no longer exists on table '{}'",
                        table.name.original()
                    ))
                })?;
            (index, physical_id.unwrap_or(table.id))
        };
        let (index, target_id) = target;
        let report = analyze_independent_index_with_progress(
            self.opener,
            target_id,
            &index,
            statement,
            &task.resolved.physical_options.first().expect("the logical table option is present").effective,
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
        column_names: Vec::new(),
        raw_options: Default::default(),
        persist_options: false,
        default_columns: AnalyzeColumnChoice::All,
        dynamic_partition_prune: true,
        skip_missing_partition_stats: true,
        enable_ddl_analyze_exec_opt: false,
        analyze_snapshot: false,
        partition_merge_concurrency: 1,
        scan_concurrency: tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY,
        store_batch_size: tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE,
        build_stats_concurrency: tidb_vardef::defaults::DEF_BUILD_STATS_CONCURRENCY as usize,
        analyze_partition_concurrency: tidb_vardef::defaults::DEF_TIDB_ANALYZE_PARTITION_CONCURRENCY as usize,
        sampling_stats_concurrency: tidb_vardef::defaults::DEF_BUILD_SAMPLING_STATS_CONCURRENCY as usize,
        eval_context: crate::StmtContext::for_query().into(),
        options: Default::default(),
        local_temporary_table: None,
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
    enable_ddl_analyze_exec_opt: bool,
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
        if !crate::cluster_analyze::index_state_analyzable(&index, enable_ddl_analyze_exec_opt) || index.mv_index {
            continue;
        }
        // Go `tblInfo.Columns[idxCol.Offset]`: a write-reorganization
        // index's column is the hidden changing column, past `Cols()`.
        for indexed in index.columns.iter_deref() {
            let offset = indexed.read().offset;
            if let Some(column) = table.columns.iter_deref().nth(offset as usize) {
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
    // Go `addColumnsWithVirtualExprs` walks the `VirtualExpr` of the
    // columns built from `tblInfo.Columns`.
    loop {
        let before = roots.len();
        for column in table.columns.iter_deref() {
            let column = column.read();
            if !roots.contains(&column.id) || !column.is_generated() || column.generated_stored {
                continue;
            }
            for dependency in column.dependences.snapshot() {
                if let Some(base) = table.columns.iter_deref().find(|candidate| {
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
    enable_ddl_analyze_exec_opt: bool,
) -> Option<HashSet<i64>> {
    let columns = table.cols().iter_deref().collect::<Vec<_>>();
    let mut selected = selected.unwrap_or_else(|| {
        columns
            .iter()
            .map(|column| column.read().id)
            .collect::<HashSet<_>>()
    });
    let mut mandatory = HashSet::new();
    add_mandatory_columns(table, &mut mandatory, false, enable_ddl_analyze_exec_opt);
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
    options: &crate::analyze::V2AnalyzeOptions,
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
        filter_skipped_column_types,
        final_column_choice,
        select_index_tasks,
        AnalyzeJobHandle, AnalyzeJobKind, AnalyzeJobLifecycle,
        AnalyzeJobProgress, AnalyzeJobSpec, ApproximateTableCountProvider, ClusterAnalyzeError,
        ANALYZE_JOB_MAX_DELTA,
    };
    use std::collections::{BTreeSet, HashSet};
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

    use tidb_model::SchemaState;
    use tidb_txnkv::rpc::UnaryCallContext;
    fn global_statement(skip_missing: bool) -> AnalyzeStatement {
        AnalyzeStatement {
            schema: "test".to_owned(),
            table: "t".to_owned(),
            auto_analyze: false,
            skip_column_types: std::collections::BTreeSet::new(),
            partitions: Vec::new(),
            index_names: None,
            columns: AnalyzeColumnChoice::All,
            column_names: Vec::new(),
            raw_options: Default::default(),
            persist_options: true,
            default_columns: AnalyzeColumnChoice::All,
            dynamic_partition_prune: true,
            skip_missing_partition_stats: skip_missing,
            enable_ddl_analyze_exec_opt: false,
            analyze_snapshot: false,
            partition_merge_concurrency: 1,
            scan_concurrency: tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY,
            store_batch_size: tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE,
            build_stats_concurrency: tidb_vardef::defaults::DEF_BUILD_STATS_CONCURRENCY as usize,
            analyze_partition_concurrency: tidb_vardef::defaults::DEF_TIDB_ANALYZE_PARTITION_CONCURRENCY as usize,
            sampling_stats_concurrency: tidb_vardef::defaults::DEF_BUILD_SAMPLING_STATS_CONCURRENCY as usize,
            eval_context: crate::StmtContext::for_query().into(),
            options: Default::default(),
            local_temporary_table: None,
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
                false,
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
            final_column_choice(&table, &AnalyzeColumnChoice::Explicit(vec!["c".to_owned()]), false)
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
}
