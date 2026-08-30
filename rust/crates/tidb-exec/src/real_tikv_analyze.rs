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

//! One `ANALYZE TABLE`, as one real transaction.
//!
//! Everything the statement does happens at one `start_ts`: the catalog it
//! resolves the table in, the previous statistics it reads its sample rate
//! from, the rows it samples, and the `mysql.stats_*` rows it stores. That is
//! not tidiness -- it is the correctness argument. `mysql.stats_meta.count`
//! and the histograms must describe the *same* rows, and a second timestamp
//! anywhere in between would let a concurrent `INSERT` land between the count
//! and the buckets, giving the planner a row count its histogram cannot
//! account for.
//!
//! The stamp on every stored row is that same `start_ts`, which is also what
//! makes a Go TiDB's concurrent `ANALYZE` a plain write conflict at prewrite:
//! both write the same keys, and exactly one of them commits.

use std::collections::HashSet;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};

use chrono::TimeZone;
use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone, UNSPECIFIED_LENGTH};
use tidb_error::mysql::SqlError;
use tidb_stats::histogram::Histogram;
use tidb_stats::{
    fm_sketch_ndv, merge_fm_sketch, merge_partition_histogram_topn, merge_partition_stats_item,
    CmsSketch, FmSketch, GlobalStatsItem, GlobalStatsMergeError, GlobalStatsMergeMode,
    PartitionStatsItem, StatsLoadedStatus, TopN,
};
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, RealOptimisticTransactionOpener, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};
use tidb_util::sqlkiller::SqlKiller;

use crate::cluster_analyze::{
    analyze_independent_index, analyze_physical_table, lower_analyze, resolve_analyze_options,
    AnalyzeColumnChoice, AnalyzeError, AnalyzeStatement,
};
use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_stats_load::{
    column_types_of, ClusterStatsItem, ClusterStatsLoader, ClusterTableStats,
};
use crate::cluster_stats_write::{
    load_analyze_options, plan_analyze_options_write, plan_get_predicate_columns,
    plan_historical_stats_meta_write, plan_independent_index_stats_write,
    plan_partial_partition_stats_write, plan_partial_stats_write, plan_partition_stats_write,
    plan_stats_meta_version_refresh, plan_stats_write, StatsWritePlan,
};
use crate::mysql_bootstrap::utc_now_timestamp;
use crate::mysql_system_tables::SystemTableError;
use crate::pessimistic_lock_error::{commit_outcome_to_sql_error, LockSqlError};
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// The mutation-count budget one `ANALYZE TABLE` declares.
///
/// Go saves one table's whole analyze result in ONE transaction --
/// `pkg/statistics/handle/storage/stats_read_writer.go:141` wraps
/// `SaveAnalyzeResultToStorage` in `util.FlagWrapTxn`, which is a single
/// `BEGIN PESSIMISTIC` ... `COMMIT` around every `stats_meta`,
/// `stats_histograms`, `stats_buckets` and `stats_top_n` write for the table
/// -- and places no bound on how many rows that is. Its batching
/// (`save.go:42`'s `batchInsertSize = 10`) groups rows into *statements*
/// inside that one transaction, not into separate transactions.
///
/// So this path keeps its single transaction, and states the count budget
/// Go's has: none. The bound that remains is the byte one
/// ([`MAX_OPTIMISTIC_TRANSACTION_BYTES`], this path's stand-in for Go's
/// `txn-total-size-limit`), which is the bound Go is actually held to.
///
/// The generic [`tidb_txnkv::transaction::MAX_OPTIMISTIC_MUTATIONS`] is the
/// wrong budget here and was
/// the defect: the default `ANALYZE` builds 256 buckets and 100 TopN entries
/// per histogram, so a table with four columns and two indexes plans ~4300
/// mutations -- over that budget. It worked on toy tables and hard-failed on
/// real ones.
pub const ANALYZE_MAX_MUTATIONS: usize = usize::MAX;

/// Parses and admits one text-protocol statement as an `ANALYZE TABLE`.
///
/// `None` means the statement is not one -- including when it does not parse
/// at all, so an unparsable statement is still reported by the query path
/// that owns the same text. This mirrors
/// [`crate::real_tikv_ddl::prepare_cluster_ddl`]: the server never parses SQL
/// itself, so the dependency direction stays
/// `tidb-server -> tidb-exec -> tidb-txnkv`.
pub fn prepare_cluster_analyze(
    sql: &str,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    let Ok(statement) = tidb_parser::parse(sql) else {
        return Ok(None);
    };
    lower_analyze(&statement, default_schema)
}

/// What one committed `ANALYZE TABLE` did.
#[derive(Clone, Debug)]
pub struct ClusterAnalyzeReport {
    /// The physical table the statistics belong to.
    pub table_id: i64,
    /// The TSO every stored row is stamped with, and the transaction's
    /// `start_ts`.
    pub version: u64,
    /// Rows read from the table.
    pub scanned_rows: i64,
    /// Rows the sampler kept.
    pub sampled_rows: i64,
    /// The Bernoulli sample rate in force.
    pub sample_rate: f64,
    /// Histograms stored, across columns and indexes.
    pub histogram_count: usize,
    /// Buckets stored.
    pub bucket_count: usize,
    /// TopN entries stored.
    pub topn_count: usize,
    /// Whether predicate-column selection found no collected column.
    pub predicate_columns_empty: bool,
    /// Nonfatal pinned-Go `saveAnalyzeOptions` failure, if any.
    pub option_save_warning: Option<String>,
    /// Nonfatal pinned-Go global-statistics merge warning, if any.
    pub global_stats_warning: Option<(u16, String)>,
    /// Whether Go ignores explicit partition options in dynamic mode.
    pub ignored_partition_overrides: bool,
    /// Whether stats v2 expanded an index-targeted statement to the normal
    /// full statistics collection path.
    pub collected_all_for_index_target: bool,
    analyzed_column_ids: Vec<i64>,
    analyzed_index_ids: Vec<i64>,
}

/// The pinned planner's physical ANALYZE targets and merged options.
#[derive(Clone, Debug)]
pub struct ResolvedClusterAnalyze {
    statement: AnalyzeStatement,
    physical_options: Vec<tidb_executor::analyze::PhysicalAnalyzeOptions>,
    partitioned: bool,
    ignored_partition_overrides: bool,
    run_full_sampling: bool,
    independent_index_ids: Vec<i64>,
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
}

/// Reads and merges the saved table-level ANALYZE options before execution.
pub fn resolve_cluster_analyze_statement<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    statement: &AnalyzeStatement,
    timeout: Duration,
) -> Result<ResolvedClusterAnalyze, ClusterAnalyzeError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let (table, partition_ids, saved) = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, statement)?;
        let partition_ids = analyze_partition_ids(table, &statement.partitions)?;
        let mut saved = std::collections::HashMap::new();
        if statement.persist_options {
            for physical_id in std::iter::once(table.id).chain(partition_ids.iter().copied()) {
                if let Some(options) =
                    load_analyze_options(&mut snapshot, &catalog, table, physical_id)
                        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?
                {
                    saved.insert(physical_id, options);
                }
            }
        }
        (table.clone(), partition_ids, saved)
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
    for options in &mut resolution.physical {
        options.columns = final_column_choice(&table, &options.columns)?;
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
    Ok(ResolvedClusterAnalyze {
        statement: resolved,
        physical_options: resolution.physical,
        partitioned: table.partition.is_some(),
        ignored_partition_overrides: resolution.ignored_partition_overrides,
        run_full_sampling: index_tasks.run_full_sampling,
        independent_index_ids: index_tasks.independent_index_ids,
    })
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
                            && index.name.lowercase().eq_ignore_ascii_case(name))
                        .then(|| index.clone())
                    })
                    .ok_or_else(|| {
                        ClusterAnalyzeError::Other(format!(
                            "Index '{name}' in field list does not exist in table '{}'",
                            table.name.original()
                        ))
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
                    .find(|index| index.name.lowercase().eq_ignore_ascii_case(name))
                    .map(|index| index.id)
            })
            .collect(),
    })
}

fn analyze_partition_ids(
    table: &tidb_model::table_info::TableInfo,
    requested: &[String],
) -> Result<Vec<i64>, ClusterAnalyzeError> {
    let Some(partition) = &table.partition else {
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        return Err(ClusterAnalyzeError::Other(
            "Partition management on a not partitioned table is not possible".to_owned(),
        ));
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
                    definition
                        .name
                        .lowercase()
                        .eq_ignore_ascii_case(requested_name)
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

fn final_column_choice(
    table: &tidb_model::table_info::TableInfo,
    columns: &AnalyzeColumnChoice,
) -> Result<AnalyzeColumnChoice, ClusterAnalyzeError> {
    let AnalyzeColumnChoice::Explicit(names) = columns else {
        return Ok(columns.clone());
    };
    let mut selected = HashSet::new();
    for name in names {
        let column = table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name.to_lowercase())
            .ok_or_else(|| {
                ClusterAnalyzeError::Other(format!(
                    "column `{name}` does not exist in `{}`",
                    table.name.original()
                ))
            })?;
        selected.insert(column.read().id);
    }
    add_mandatory_columns(table, &mut selected);
    Ok(AnalyzeColumnChoice::Explicit(
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
    ))
}

/// Saves the merged raw options in Go's later, independent restricted
/// transaction. Callers treat failure as a statement warning.
pub fn save_cluster_analyze_options<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    resolved: &ResolvedClusterAnalyze,
    timeout: Duration,
) -> Result<(), ClusterAnalyzeError> {
    if !resolved.statement.persist_options {
        return Ok(());
    }
    let mut transaction = opener
        // Go emits one REPLACE containing every logical/partition option row
        // and applies no row-count bound to that restricted transaction.
        .begin(ANALYZE_MAX_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let plan = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, &resolved.statement)?;
        let mut combined = StatsWritePlan::default();
        for options in &resolved.physical_options {
            if options.is_partition && resolved.statement.dynamic_partition_prune {
                continue;
            }
            let write = plan_analyze_options_write(
                &mut snapshot,
                &catalog,
                table,
                options.physical_id,
                options.raw,
                &options.columns,
                utc_now_timestamp(),
            )
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
            combined.mutations.extend(write.mutations);
        }
        combined
    };
    let outcome = transaction
        .commit(plan.mutations, &UnaryCallContext::with_timeout(timeout))
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    classify_commit_outcome(&outcome)
}

fn find_statement_table<'catalog>(
    catalog: &'catalog crate::cluster_catalog::ClusterCatalog,
    statement: &AnalyzeStatement,
) -> Result<&'catalog tidb_model::table_info::TableInfo, ClusterAnalyzeError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == statement.schema.to_lowercase().as_str())
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|stored| stored.name.lowercase() == statement.table.to_lowercase().as_str())
        })
        .ok_or_else(|| {
            ClusterAnalyzeError::Other(
                SystemTableError::Missing {
                    name: format!("{}.{}", statement.schema, statement.table),
                }
                .to_string(),
            )
        })
}

/// Why an `ANALYZE TABLE` transaction did not produce a committed report.
#[derive(Debug)]
pub enum ClusterAnalyzeError {
    /// The commit may have landed, so the server must not report failure.
    Undetermined(String),
    /// A determinate commit failure with its driver error code and SQLSTATE.
    Commit(LockSqlError),
    /// A statement kill retaining its MySQL error identity.
    Killed(SqlError),
    /// Pinned error 8243, demoted to a statement warning by ANALYZE.
    MissingPartitionStats(String),
    /// Pinned error 8244, demoted to a statement warning by ANALYZE.
    MissingPartitionItemStats(String),
    /// A determinate failure with its existing diagnostic.
    Other(String),
}

impl fmt::Display for ClusterAnalyzeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undetermined(detail) => {
                write!(formatter, "execution result undetermined: {detail}")
            }
            Self::Commit(error) => formatter.write_str(&error.message),
            Self::Killed(error) => formatter.write_str(&error.message),
            Self::MissingPartitionStats(detail) => write!(
                formatter,
                "Build global-level stats failed due to missing partition-level stats: {detail}"
            ),
            Self::MissingPartitionItemStats(detail) => write!(
                formatter,
                "Build global-level stats failed due to missing partition-level column stats: {detail}, please run analyze table to refresh columns of all partitions"
            ),
            Self::Other(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for ClusterAnalyzeError {}

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

/// `mysql.stats_meta.count` as the sample-rate rule reads it.
///
/// Go's `RealtimeCount` is an `int64`, so a stored count past `i64::MAX` is
/// not a number Go can have written and not one this node can believe. It
/// reads as *no usable count*, which `getAdjustedSampleRate` answers with
/// `1.0` -- read every row. Saturating to `i64::MAX` instead would drive the
/// rate to ~1e-14: zero rows kept, yet the NDVs still taken from the
/// full-scan FM sketch, producing a table the loader accepts as non-pseudo
/// with empty histograms.
fn realtime_count_of(row_count: u64) -> i64 {
    i64::try_from(row_count).unwrap_or(0)
}

/// Runs and commits one `ANALYZE TABLE`.
pub fn commit_cluster_analyze<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    resolved: &ResolvedClusterAnalyze,
    timeout: Duration,
    stats_lease: Duration,
    killer: &SqlKiller,
    record_history: &dyn Fn() -> bool,
    record_global_history: &dyn Fn() -> bool,
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
    let mut reports = Vec::with_capacity(targets.len());
    for options in targets {
        reports.push(commit_cluster_analyze_target(
            opener,
            &resolved.statement,
            options,
            timeout,
            stats_lease,
            record_history,
        )?);
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
        global_stats_warning: None,
        ignored_partition_overrides: false,
        collected_all_for_index_target: false,
        analyzed_column_ids: Vec::new(),
        analyzed_index_ids: Vec::new(),
    });
    for next in reports {
        merge_analyze_report(&mut report, next);
    }
    if resolved.partitioned {
        let table_id = resolved
            .physical_options
            .first()
            .expect("the logical table option is present")
            .physical_id;
        report.table_id = table_id;
    }
    if resolved.run_full_sampling
        && resolved.partitioned
        && resolved.statement.dynamic_partition_prune
    {
        if let Err(error) = commit_cluster_global_stats(
            opener,
            resolved,
            &mut report,
            timeout,
            stats_lease,
            killer,
            record_global_history,
        ) {
            match error {
                ClusterAnalyzeError::MissingPartitionStats(_) => {
                    report.global_stats_warning = Some((8243, error.to_string()));
                }
                ClusterAnalyzeError::MissingPartitionItemStats(_) => {
                    report.global_stats_warning = Some((8244, error.to_string()));
                }
                _ => return Err(error),
            }
        }
    }
    let independent_options = &resolved
        .physical_options
        .first()
        .expect("the logical table option is present")
        .effective;
    for index_id in &resolved.independent_index_ids {
        let next = commit_cluster_independent_index(
            opener,
            &resolved.statement,
            *index_id,
            independent_options,
            timeout,
            stats_lease,
            record_history,
        )?;
        merge_analyze_report(&mut report, next);
    }
    report.ignored_partition_overrides = resolved.ignored_partition_overrides;
    Ok(report)
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
}

fn commit_cluster_global_stats<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    resolved: &ResolvedClusterAnalyze,
    report: &mut ClusterAnalyzeReport,
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
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, &resolved.statement)?;
        let definitions = table
            .partition
            .as_ref()
            .expect("a dynamic target is partitioned")
            .read()
            .definitions
            .snapshot();
        let loader = ClusterStatsLoader::locate(&catalog)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let column_types = column_types_of(table);
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
            let partitions = prepare_async_global_partitions(
                &mut snapshot,
                &loader,
                definitions
                    .iter()
                    .map(|definition| (definition.id, definition.name.original().to_owned())),
                !report.analyzed_column_ids.is_empty(),
                !report.analyzed_index_ids.is_empty(),
            )?;
            let total_count = partitions.iter().fold(0_i64, |total, partition| {
                total.wrapping_add(partition.row_count)
            });
            let modify_count = partitions.iter().fold(0_i64, |total, partition| {
                total.wrapping_add(partition.modify_count)
            });
            let items = match &resolved.statement.time_zone {
                SessionTimeZone::Local => merge_global_items_async(
                    &mut snapshot,
                    &loader,
                    Some(&chrono::Local),
                    &partitions,
                    &report.analyzed_column_ids,
                    &report.analyzed_index_ids,
                    &column_types,
                    &column_names,
                    &index_names,
                    &resolved.statement,
                    total_count,
                    killer,
                ),
                SessionTimeZone::Named(zone) => merge_global_items_async(
                    &mut snapshot,
                    &loader,
                    Some(zone),
                    &partitions,
                    &report.analyzed_column_ids,
                    &report.analyzed_index_ids,
                    &column_types,
                    &column_names,
                    &index_names,
                    &resolved.statement,
                    total_count,
                    killer,
                ),
                SessionTimeZone::Fixed { offset_secs, .. } => {
                    let zone = chrono::FixedOffset::east_opt(*offset_secs).ok_or_else(|| {
                        ClusterAnalyzeError::Other("invalid session timezone offset".to_owned())
                    })?;
                    merge_global_items_async(
                        &mut snapshot,
                        &loader,
                        Some(&zone),
                        &partitions,
                        &report.analyzed_column_ids,
                        &report.analyzed_index_ids,
                        &column_types,
                        &column_names,
                        &index_names,
                        &resolved.statement,
                        total_count,
                        killer,
                    )
                }
            }?;
            (table.id, total_count, modify_count, items)
        } else {
            let mut partitions = Vec::with_capacity(definitions.len());
            for definition in definitions.iter() {
                let stats = loader
                    .load_table_with_fm(&mut snapshot, definition.id, &column_types)
                    .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
                partitions.push((definition.name.original().to_owned(), stats));
            }
            let total_count = partitions.iter().try_fold(0_i64, |total, (_, stats)| {
                let count = stats
                    .as_ref()
                    .map_or(Ok(0), |stats| i64::try_from(stats.row_count))
                    .map_err(|_| {
                        ClusterAnalyzeError::Other(
                            "partition statistics count exceeds int64".to_owned(),
                        )
                    })?;
                Ok::<_, ClusterAnalyzeError>(total.wrapping_add(count))
            })?;
            let modify_count = partitions
                .iter()
                .filter_map(|(_, stats)| stats.as_ref())
                .fold(0_i64, |total, stats| total.wrapping_add(stats.modify_count));
            let items = match &resolved.statement.time_zone {
                SessionTimeZone::Local => merge_global_items(
                    Some(&chrono::Local),
                    &partitions,
                    &report.analyzed_column_ids,
                    &report.analyzed_index_ids,
                    &column_types,
                    &column_names,
                    &index_names,
                    &resolved.statement,
                    total_count,
                    mode,
                    killer,
                ),
                SessionTimeZone::Named(zone) => merge_global_items(
                    Some(zone),
                    &partitions,
                    &report.analyzed_column_ids,
                    &report.analyzed_index_ids,
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
                        Some(&zone),
                        &partitions,
                        &report.analyzed_column_ids,
                        &report.analyzed_index_ids,
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
            commit_global_stats_item(
                opener,
                logical_id,
                total_count,
                modify_count,
                item,
                timeout,
                stats_lease,
            )
        },
        |write| {
            if record_history() {
                record_analyze_history(opener, logical_id, write.0, timeout);
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

fn record_analyze_history<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table_id: i64,
    version: u64,
    timeout: Duration,
) {
    let result = (|| -> Result<(), ClusterAnalyzeError> {
        let mut transaction = opener
            .begin(ANALYZE_MAX_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let plan = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
            let catalog = load_cluster_catalog(&mut snapshot)
                .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
            plan_historical_stats_meta_write(
                &mut snapshot,
                &catalog,
                table_id,
                version,
                "analyze",
                utc_now_timestamp(),
            )
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?
        };
        let outcome = transaction
            .commit(plan.mutations, &UnaryCallContext::with_timeout(timeout))
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        classify_commit_outcome(&outcome)
    })();
    let _: Result<(), ClusterAnalyzeError> = result;
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

fn prepare_async_global_partitions<S: crate::cluster_catalog::MetaSnapshot>(
    snapshot: &mut S,
    loader: &ClusterStatsLoader,
    definitions: impl IntoIterator<Item = (i64, String)>,
    merge_columns: bool,
    merge_indexes: bool,
) -> Result<Vec<AsyncGlobalPartition>, ClusterAnalyzeError> {
    let mut partitions = Vec::new();
    for (id, name) in definitions {
        let meta = loader
            .load_meta(snapshot, id)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let (modify_count, row_count) = match meta {
            Some((_, _, modify_count, row_count, _)) => (
                modify_count,
                i64::try_from(row_count).map_err(|_| {
                    ClusterAnalyzeError::Other(
                        "partition statistics count exceeds int64".to_owned(),
                    )
                })?,
            ),
            None => (0, 0),
        };
        let has_columns = if merge_columns {
            loader
                .has_histogram_rows(snapshot, id, false)
                .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?
        } else {
            false
        };
        let has_indexes = if merge_indexes {
            loader
                .has_histogram_rows(snapshot, id, true)
                .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?
        } else {
            false
        };
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
fn merge_global_items_async<S, TZ>(
    snapshot: &mut S,
    loader: &ClusterStatsLoader,
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
    S: crate::cluster_catalog::MetaSnapshot,
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
                let column_type = (!is_index).then_some(&field_type);
                let exists = loader
                    .load_item(snapshot, partition.id, is_index, id, column_type, false)
                    .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?
                    .is_some();
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
                    return Err(ClusterAnalyzeError::MissingPartitionItemStats(format!(
                        "table `{}` partition `{}` {} `{name}`",
                        statement.table,
                        partition.name,
                        if is_index { "index" } else { "column" }
                    )));
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
                    merged[index] = Some(
                        merge_partition_histogram_topn(
                            timezone,
                            2,
                            requested_topn,
                            expected_buckets,
                            &item.field_type,
                            item.is_index,
                            merge_concurrency,
                            global_ndvs[index],
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
                    let sketch = loader
                        .load_fm_sketch(snapshot, partition.id, item.is_index, item.id)
                        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
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
                    let sketch = loader
                        .load_item_cmsketch(snapshot, partition.id, item.is_index, item.id)
                        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
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
                    let histogram = loader
                        .load_item_histogram(
                            snapshot,
                            partition.id,
                            item.is_index,
                            item.id,
                            (!item.is_index).then_some(&item.field_type),
                        )
                        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?
                        .ok_or_else(|| {
                            ClusterAnalyzeError::Other(format!(
                                "histogram {} disappeared from partition {} in one snapshot",
                                item.id, partition.id
                            ))
                        })?;
                    histograms.push(histogram);
                    topns.push(
                        loader
                            .load_item_topn(snapshot, partition.id, item.is_index, item.id)
                            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?,
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
    timezone: Option<&TZ>,
    partitions: &[(String, Option<ClusterTableStats>)],
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
                    return Err(ClusterAnalyzeError::MissingPartitionStats(format!(
                        "table `{}` partition `{partition_name}`",
                        statement.table
                    )));
                };
                if mode == GlobalStatsMergeMode::Async
                    && if is_index {
                        stats.indexes.is_empty()
                    } else {
                        stats.columns.is_empty()
                    }
                {
                    continue;
                }
                let item = if is_index {
                    stats.index(id)
                } else {
                    stats.column(id)
                };
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
                    return Err(if mode == GlobalStatsMergeMode::Async {
                        ClusterAnalyzeError::MissingPartitionItemStats(detail)
                    } else {
                        ClusterAnalyzeError::MissingPartitionStats(detail)
                    });
                };
                let topn_count = item.topn.as_ref().map_or(0, |topn| topn.total_count());
                if mode == GlobalStatsMergeMode::Blocking
                    && stats.row_count > 0
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
                    return Err(ClusterAnalyzeError::MissingPartitionItemStats(format!(
                        "table `{}` partition `{partition_name}` {} `{name}`",
                        statement.table,
                        if is_index { "index" } else { "column" }
                    )));
                }
                inputs.push(PartitionStatsItem {
                    histogram: item.histogram.clone(),
                    cmsketch: item.cms.clone(),
                    topn: item.topn.clone(),
                    fm_sketch: item.fm_sketch.clone(),
                });
            }
            let field_type = if is_index {
                FieldType::new(FieldTypeCode::Blob)
            } else {
                column_types.get(&id).cloned().ok_or_else(|| {
                    ClusterAnalyzeError::Other(format!("unknown analyzed column ID {id}"))
                })?
            };
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
            if let Some(histogram) = merged.histogram {
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

fn commit_global_stats_item<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table_id: i64,
    row_count: i64,
    modify_count: i64,
    item: ClusterStatsItem,
    timeout: Duration,
    stats_lease: Duration,
) -> Result<(u64, usize, usize, usize), ClusterAnalyzeError> {
    let started = Instant::now();
    let mut transaction = opener
        .begin(ANALYZE_MAX_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let mut version = transaction.start_ts();
    let write = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let is_index = item.is_index;
        let stats = ClusterTableStats {
            table_id,
            version,
            snapshot: version,
            last_analyze_version: version,
            last_stats_hist_version: version,
            modify_count,
            row_count: u64::try_from(row_count).map_err(|_| {
                ClusterAnalyzeError::Other("negative global statistics count".to_owned())
            })?,
            columns: if is_index {
                Vec::new()
            } else {
                vec![item.clone()]
            },
            indexes: if is_index { vec![item] } else { Vec::new() },
        };
        plan_partial_stats_write(&mut snapshot, &catalog, &stats, utc_now_timestamp())
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?
    };
    let counts = (write.histogram_count, write.bucket_count, write.topn_count);
    let outcome = transaction
        .commit(write.mutations, &UnaryCallContext::with_timeout(timeout))
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    classify_commit_outcome(&outcome)?;
    if slow_stats_save_needs_version_refresh(stats_lease, started.elapsed()) {
        version = refresh_stats_meta_version(opener, table_id, timeout).map_err(|_| {
            ClusterAnalyzeError::Other(
                "failed to update stats meta version during analyze result save. The system may be too busy. Please retry the operation later".to_owned(),
            )
        })?;
    }
    Ok((version, counts.0, counts.1, counts.2))
}

fn slow_stats_save_needs_version_refresh(stats_lease: Duration, elapsed: Duration) -> bool {
    stats_lease > Duration::ZERO && elapsed >= stats_lease.saturating_mul(5)
}

fn refresh_stats_meta_version<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table_id: i64,
    timeout: Duration,
) -> Result<u64, ClusterAnalyzeError> {
    let mut transaction = opener
        .begin(ANALYZE_MAX_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let version = transaction.start_ts();
    let plan = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        plan_stats_meta_version_refresh(&mut snapshot, &catalog, table_id, version)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?
    };
    let outcome = transaction
        .commit(plan.mutations, &UnaryCallContext::with_timeout(timeout))
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    classify_commit_outcome(&outcome)?;
    Ok(version)
}

fn commit_cluster_analyze_target<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    statement: &AnalyzeStatement,
    options: &tidb_executor::analyze::PhysicalAnalyzeOptions,
    timeout: Duration,
    stats_lease: Duration,
    record_history: &dyn Fn() -> bool,
) -> Result<ClusterAnalyzeReport, ClusterAnalyzeError> {
    let started = Instant::now();
    let mut transaction = opener
        .begin(ANALYZE_MAX_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let start_ts = transaction.start_ts();
    let planned = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, statement)?.clone();
        // The sample rate Go derives from `RealtimeCount`: what the table's
        // previous `ANALYZE` (or delta flush) said it holds.
        let realtime_count = ClusterStatsLoader::locate(&catalog)
            .ok()
            .and_then(|loader| {
                loader
                    .load_table(
                        &mut snapshot,
                        options.physical_id,
                        &crate::cluster_stats_load::column_types_of(&table),
                    )
                    .ok()
                    .flatten()
            })
            .map(|stats| realtime_count_of(stats.row_count));
        let mut target_statement = statement.clone();
        target_statement.columns = options.columns.clone();
        target_statement.raw_options = options.raw;
        target_statement.options = options.effective;
        target_statement.options.memory_quota = statement.options.memory_quota;
        let (mut selected, cleanup, predicate_columns_empty) =
            selected_columns(&mut snapshot, &catalog, &table, &target_statement)
                .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        if let Some(selected) = selected.as_mut() {
            add_mandatory_columns(&table, selected);
        }
        let report = analyze_physical_table(
            &mut snapshot,
            &table,
            options.physical_id,
            &target_statement.options,
            realtime_count,
            start_ts,
            selected.as_ref(),
        )
        .map_err(|error: AnalyzeError| ClusterAnalyzeError::Other(error.to_string()))?;
        let mut write = if options.is_partition && selected.is_some() {
            plan_partial_partition_stats_write(
                &mut snapshot,
                &catalog,
                &report.stats,
                utc_now_timestamp(),
            )
        } else if options.is_partition {
            plan_partition_stats_write(&mut snapshot, &catalog, &report.stats, utc_now_timestamp())
        } else if selected.is_some() {
            plan_partial_stats_write(&mut snapshot, &catalog, &report.stats, utc_now_timestamp())
        } else {
            plan_stats_write(&mut snapshot, &catalog, &report.stats, utc_now_timestamp())
        }
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        write.mutations.splice(0..0, cleanup.mutations);
        (report, write, predicate_columns_empty)
    };
    let (report, write, predicate_columns_empty) = planned;

    if write.is_empty() {
        transaction
            .finish_without_writes()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        return Err(ClusterAnalyzeError::Other(
            "this ANALYZE produced no statistics to store".to_owned(),
        ));
    }
    let mut receipt = ClusterAnalyzeReport {
        table_id: report.stats.table_id,
        version: start_ts,
        scanned_rows: report.scanned_rows,
        sampled_rows: report.sampled_rows,
        sample_rate: report.sample_rate,
        histogram_count: write.histogram_count,
        bucket_count: write.bucket_count,
        topn_count: write.topn_count,
        predicate_columns_empty,
        option_save_warning: None,
        global_stats_warning: None,
        ignored_partition_overrides: false,
        collected_all_for_index_target: statement.index_names.is_some(),
        analyzed_column_ids: report.stats.columns.iter().map(|item| item.id).collect(),
        analyzed_index_ids: report.stats.indexes.iter().map(|item| item.id).collect(),
    };
    // The commit mints its own deadline here rather than inheriting one from
    // function entry: everything above it -- the catalog read, the previous
    // statistics read and the row sample -- runs on per-request budgets
    // (`TransactionMetaSnapshot`), but it lasts as long as the table is
    // deep. A context stamped before that work shares ONE absolute deadline
    // with the whole statement, so on a real table the prewrite arrived after
    // its budget had already been spent by scanning -- `timed out after
    // 0ms` at commit admission. Go never couples these:
    // `SaveAnalyzeResultToStorage` commits through the two-phase
    // committer, whose every action builds its context when it runs
    // (`twoPhaseCommitter.execute`, pkg/store/tikv/2pc.go), not when
    // the statement began.
    let outcome = transaction
        .commit(write.mutations, &UnaryCallContext::with_timeout(timeout))
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    classify_commit_outcome(&outcome)?;
    receipt.version = finish_analyze_save(
        opener,
        receipt.table_id,
        receipt.version,
        started,
        timeout,
        stats_lease,
        record_history,
    )?;
    Ok(receipt)
}

fn commit_cluster_independent_index<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    statement: &AnalyzeStatement,
    index_id: i64,
    options: &tidb_executor::analyze::AnalyzeOptions,
    timeout: Duration,
    stats_lease: Duration,
    record_history: &dyn Fn() -> bool,
) -> Result<ClusterAnalyzeReport, ClusterAnalyzeError> {
    let started = Instant::now();
    let mut transaction = opener
        .begin(ANALYZE_MAX_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let start_ts = transaction.start_ts();
    let (report, write, inserted_meta) = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, statement)?.clone();
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
        let report = analyze_independent_index(&mut snapshot, &table, &index, options, start_ts)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let (write, inserted_meta) = plan_independent_index_stats_write(
            &mut snapshot,
            &catalog,
            &report.stats,
            utc_now_timestamp(),
        )
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        (report, write, inserted_meta)
    };
    let mut receipt = ClusterAnalyzeReport {
        table_id: report.stats.table_id,
        version: start_ts,
        scanned_rows: report.scanned_rows,
        sampled_rows: report.sampled_rows,
        sample_rate: report.sample_rate,
        histogram_count: write.histogram_count,
        bucket_count: write.bucket_count,
        topn_count: write.topn_count,
        predicate_columns_empty: false,
        option_save_warning: None,
        global_stats_warning: None,
        ignored_partition_overrides: false,
        collected_all_for_index_target: false,
        analyzed_column_ids: Vec::new(),
        analyzed_index_ids: vec![index_id],
    };
    let outcome = transaction
        .commit(write.mutations, &UnaryCallContext::with_timeout(timeout))
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    classify_commit_outcome(&outcome)?;
    if inserted_meta {
        receipt.version = finish_analyze_save(
            opener,
            receipt.table_id,
            receipt.version,
            started,
            timeout,
            stats_lease,
            record_history,
        )?;
    }
    Ok(receipt)
}

fn finish_analyze_save<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table_id: i64,
    mut version: u64,
    started: Instant,
    timeout: Duration,
    stats_lease: Duration,
    record_history: &dyn Fn() -> bool,
) -> Result<u64, ClusterAnalyzeError> {
    if slow_stats_save_needs_version_refresh(stats_lease, started.elapsed()) {
        version = refresh_stats_meta_version(opener, table_id, timeout).map_err(|_| {
            ClusterAnalyzeError::Other(
                "failed to update stats meta version during analyze result save. The system may be too busy. Please retry the operation later".to_owned(),
            )
        })?;
    }
    if record_history() {
        record_analyze_history(opener, table_id, version, timeout);
    }
    Ok(version)
}

fn selected_columns<S: crate::cluster_catalog::MetaSnapshot>(
    snapshot: &mut S,
    catalog: &crate::cluster_catalog::ClusterCatalog,
    table: &tidb_model::table_info::TableInfo,
    statement: &AnalyzeStatement,
) -> Result<(Option<HashSet<i64>>, StatsWritePlan, bool), crate::cluster_stats_write::StatsWriteError>
{
    match &statement.columns {
        AnalyzeColumnChoice::All => Ok((None, StatsWritePlan::default(), false)),
        AnalyzeColumnChoice::Default => {
            selected_columns_for_choice(snapshot, catalog, table, &statement.default_columns)
        }
        AnalyzeColumnChoice::Predicate => {
            let current = table
                .cols()
                .iter_deref()
                .map(|column| column.read().id)
                .collect::<Vec<_>>();
            let (columns, cleanup) =
                plan_get_predicate_columns(snapshot, catalog, table.id, &current)?;
            let empty = columns.is_empty();
            Ok((Some(columns.into_iter().collect()), cleanup, empty))
        }
        AnalyzeColumnChoice::Explicit(names) => {
            let mut selected = HashSet::new();
            for name in names {
                let column = table
                    .cols()
                    .iter_deref()
                    .find(|column| column.read().name.lowercase() == name.to_lowercase())
                    .ok_or_else(|| {
                        crate::cluster_stats_write::StatsWriteError::MissingTable(format!(
                            "column `{name}` does not exist in `{}`",
                            table.name.original()
                        ))
                    })?;
                selected.insert(column.read().id);
            }
            Ok((Some(selected), StatsWritePlan::default(), false))
        }
    }
}

fn selected_columns_for_choice<S: crate::cluster_catalog::MetaSnapshot>(
    snapshot: &mut S,
    catalog: &crate::cluster_catalog::ClusterCatalog,
    table: &tidb_model::table_info::TableInfo,
    choice: &AnalyzeColumnChoice,
) -> Result<(Option<HashSet<i64>>, StatsWritePlan, bool), crate::cluster_stats_write::StatsWriteError>
{
    let statement = AnalyzeStatement {
        schema: String::new(),
        table: String::new(),
        partitions: Vec::new(),
        index_names: None,
        columns: choice.clone(),
        raw_options: Default::default(),
        persist_options: false,
        default_columns: AnalyzeColumnChoice::All,
        dynamic_partition_prune: true,
        skip_missing_partition_stats: true,
        enable_async_merge_global_stats: true,
        partition_merge_concurrency: 1,
        time_zone: tidb_datatype::SessionTimeZone::utc(),
        options: Default::default(),
    };
    selected_columns(snapshot, catalog, table, &statement)
}

fn add_mandatory_columns(table: &tidb_model::table_info::TableInfo, selected: &mut HashSet<i64>) {
    for index in table.indices.iter_deref() {
        let index = index.read();
        if index.state != tidb_model::SchemaState::PUBLIC || index.mv_index {
            continue;
        }
        for indexed in index.columns.iter_deref() {
            let offset = indexed.read().offset;
            if let Some(column) = table.cols().get(offset as usize) {
                selected.insert(column.read().id);
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
        let before = selected.len();
        for column in table.cols().iter_deref() {
            let column = column.read();
            if !selected.contains(&column.id) {
                continue;
            }
            for dependency in column.dependences.snapshot() {
                if let Some(base) = table.cols().iter_deref().find(|candidate| {
                    dependency.as_utf8().is_ok_and(|dependency| {
                        candidate
                            .read()
                            .name
                            .lowercase()
                            .eq_ignore_ascii_case(dependency)
                    })
                }) {
                    selected.insert(base.read().id);
                }
            }
        }
        if selected.len() == before {
            break;
        }
    }
}

fn classify_commit_outcome(outcome: &OptimisticCommitOutcome) -> Result<(), ClusterAnalyzeError> {
    match commit_outcome_to_sql_error(outcome) {
        Ok(()) => Ok(()),
        Err(error) if error.is_result_undetermined() => Err(ClusterAnalyzeError::Undetermined(
            "the ANALYZE transaction returned no commit verdict".to_owned(),
        )),
        Err(error) => Err(ClusterAnalyzeError::Commit(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        analyze_partition_ids, classify_commit_outcome, final_column_choice, merge_global_items,
        realtime_count_of, record_global_history_enabled, select_index_tasks,
        slow_stats_save_needs_version_refresh, write_global_stats_items, ClusterAnalyzeError,
    };
    use std::collections::BTreeMap;
    use std::thread;
    use std::time::Duration;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
    use tidb_executor::analyze::{AnalyzeColumnChoice, AnalyzeStatement};
    use tidb_model::column::ColumnInfo;
    use tidb_model::go_runtime::GoShared;
    use tidb_model::index::{IndexColumn, IndexInfo};
    use tidb_model::partition::{PartitionDefinition, PartitionInfo};
    use tidb_model::table_info::TableInfo;
    use tidb_model::SchemaState;
    use tidb_stats::histogram::{Bucket, Histogram};
    use tidb_stats::row_sample_collector::adjusted_sample_rate;
    use tidb_stats::{FmSketch, GlobalStatsMergeMode, StatsLoadedStatus, MAX_SKETCH_SIZE};
    use tidb_txnkv::region::RegionBackoffKind;
    use tidb_txnkv::rpc::UnaryCallContext;
    use tidb_txnkv::transaction::{
        OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction,
        TransactionCause,
    };
    use tidb_util::sqlkiller::SqlKiller;

    fn global_partition_item(id: i64, count: i64) -> super::ClusterStatsItem {
        super::ClusterStatsItem {
            id,
            is_index: false,
            stats_ver: 2,
            flag: 0,
            load_status: StatsLoadedStatus::full_load(),
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
            },
            topn: None,
            cms: None,
            fm_sketch: Some(FmSketch::from_raw_parts(
                0,
                MAX_SKETCH_SIZE,
                [id as u64, id as u64 + 1],
            )),
        }
    }

    fn global_partition_stats(item: Option<super::ClusterStatsItem>) -> super::ClusterTableStats {
        super::ClusterTableStats {
            table_id: 11,
            version: 7,
            snapshot: 7,
            last_analyze_version: 7,
            last_stats_hist_version: 7,
            modify_count: 1,
            row_count: 5,
            columns: item.into_iter().collect(),
            indexes: Vec::new(),
        }
    }

    fn global_statement(skip_missing: bool) -> AnalyzeStatement {
        AnalyzeStatement {
            schema: "test".to_owned(),
            table: "t".to_owned(),
            partitions: Vec::new(),
            index_names: None,
            columns: AnalyzeColumnChoice::All,
            raw_options: Default::default(),
            persist_options: true,
            default_columns: AnalyzeColumnChoice::All,
            dynamic_partition_prune: true,
            skip_missing_partition_stats: skip_missing,
            enable_async_merge_global_stats: true,
            partition_merge_concurrency: 1,
            time_zone: tidb_datatype::SessionTimeZone::utc(),
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
        empty.histogram.buckets.clear();
        empty.topn = None;
        let partitions = vec![("p0".to_owned(), Some(global_partition_stats(Some(empty))))];
        let error = merge_global_items(
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
            AnalyzeColumnChoice::Explicit(vec!["a".to_owned(), "b".to_owned(), "c".to_owned(),])
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
    fn slow_global_stats_save_uses_go_five_lease_boundary() {
        let lease = Duration::from_secs(3);
        assert!(!slow_stats_save_needs_version_refresh(
            Duration::ZERO,
            Duration::from_secs(60)
        ));
        assert!(!slow_stats_save_needs_version_refresh(
            lease,
            Duration::from_secs(14)
        ));
        assert!(slow_stats_save_needs_version_refresh(
            lease,
            Duration::from_secs(15)
        ));
    }

    #[test]
    fn an_unbelievable_stored_row_count_reads_every_row_rather_than_none() {
        assert_eq!(realtime_count_of(10_000), 10_000);
        assert_eq!(realtime_count_of(u64::MAX), 0);
        // The failure this is about: a saturating read would sample at
        // 110000/i64::MAX and keep nothing, while the FM sketch still reported
        // NDVs from the full scan.
        assert!(
            adjusted_sample_rate(Some(realtime_count_of(u64::MAX)), None)
                > adjusted_sample_rate(Some(i64::MAX), None)
        );
        assert!(
            (adjusted_sample_rate(Some(realtime_count_of(u64::MAX)), None) - 1.0).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn analyze_backoff_exhaustion_keeps_its_driver_error_identity() {
        let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
            cause: TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::StaleCommand,
                detail: "staleCommand backoffer exhausted".to_owned(),
            },
        });
        assert!(matches!(
            classify_commit_outcome(&outcome),
            Err(ClusterAnalyzeError::Commit(error))
                if error.code == tidb_error::tidb::errcode::ErrTiKVStaleCommand
        ));
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
}
