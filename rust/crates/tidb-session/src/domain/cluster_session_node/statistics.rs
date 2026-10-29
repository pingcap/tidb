//! `ANALYZE TABLE` on the convergence node: who may run one, how much memory
//! its sampling may use, and what each analysed table reports.
//!
//! The statement is not answered by the session driver at all -- statistics
//! are stored cluster state, so it is routed to
//! [`ClusterAnalyze`](tidb_domain_core::cluster_analyze_seam::ClusterAnalyze), one
//! transaction per named table. Mirrors Go's split across
//! `pkg/planner/core/planbuilder.go` (`requireInsertAndSelectPriv`, the
//! privilege gate below) and `pkg/executor/analyze.go` with
//! `variable.SetMemQuotaAnalyze` (`pkg/executor/select.go:141`, the quota
//! below).

use std::sync::Arc;
use tidb_executor::cluster_analyze::AnalyzeStatement;
use tidb_stats_handle_lockstats::lock_target::ClusterStatsLockStatement;
use tidb_stats_handle_storage::cluster_stats_write::StatsWritePlan;
use tidb_datatype::current_time::utc_now_timestamp;
use tidb_executor::real_tikv_analyze::{AnalyzeJobKind, AnalyzeJobLifecycle, AnalyzeJobSpec};
use tidb_executor::analyze::exec::AnalyzeExec as _;
use tidb_executor::analyze::panic_recovery::recover_analyze_panic;
use tidb_executor::stats_exec::StatsExec as _;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;

use crate::domain::session_api::{SqlQueryError, WriteOutcome};

use super::{ClusterServerSession, ClusterTransactions, SharedClusterCatalog};
use crate::stats_statement_service::FlushStatsDeltaTargets;

/// Go answers the statistics statements with an OK packet carrying no rows.
const NO_ROWS: WriteOutcome = WriteOutcome {
    affected_rows: 0,
    last_insert_id: 0,
};

/// A cluster seam's error as the session's.
fn into_driver_error(error: SqlQueryError) -> tidb_executor::DriverError {
    tidb_executor::DriverError::Mysql(
        tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
            .with_identity_opt(error.identity),
    )
}

struct PersistedAnalyzeJobs {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
    instance: String,
    process_id: u64,
    global_vars: crate::GlobalSysvars,
    /// The node's live row-ID allocators (Go's internal INSERT allocates
    /// `mysql.analyze_jobs.id` from the table's AUTO_INCREMENT allocator).
    row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
}

impl PersistedAnalyzeJobs {
    fn commit_analyze_job_plan(
        &self,
        build: impl FnOnce(
            &mut super::SnapshotMetaSnapshot,
            &tidb_infoschema::cluster_catalog::ClusterCatalog,
        ) -> Result<StatsWritePlan, String>,
    ) -> Result<(), String> {
        let snapshot = self.transactions.open_snapshot("default")?;
        let read_ts = snapshot.start_ts();
        let plan = build(
            &mut super::SnapshotMetaSnapshot::new(snapshot),
            &self.catalog.load(),
        )?;
        self.transactions
            .commit_optimistic_mutations(
                plan.mutations,
                read_ts,
                "default",
                tidb_domain_core::session_transaction::restricted_transaction_options(&self.global_vars),
            )
            .map_err(|error| error.message)
    }
}

impl AnalyzeJobLifecycle for PersistedAnalyzeJobs {
    fn insert(&self, spec: &AnalyzeJobSpec) -> Option<u64> {
        let result = (|| {
            let catalog = self.catalog.load();
            let snapshot = self.transactions.open_snapshot("default")?;
            let read_ts = snapshot.start_ts();
            let mut snapshot = super::SnapshotMetaSnapshot::new(snapshot);
            let (job_id, plan) = tidb_stats_handle_storage::cluster_stats_write::plan_insert_analyze_job(
                &mut snapshot,
                &catalog,
                &spec.schema,
                &spec.table,
                &spec.partition,
                spec.job_info.as_bytes(),
                &self.instance,
                self.process_id,
                utc_now_timestamp(),
                Some(self.row_ids.as_ref()),
            )
            .map_err(|error| error.to_string())?;
            self.transactions
                .commit_optimistic_mutations(
                    plan.mutations,
                    read_ts,
                    "default",
                    tidb_domain_core::session_transaction::restricted_transaction_options(&self.global_vars),
                )
                .map_err(|error| error.message)?;
            Ok::<_, String>(job_id)
        })();
        match result {
            Ok(job_id) => Some(job_id),
            Err(error) => {
                // Go `AddNewAnalyzeJob`.
                tidb_stats_handle_logutil::stats_err_verbose_logger()
                    .error("failed to insert analyze job", &[crate::domain::zap_error(error)]);
                None
            }
        }
    }

    fn start(&self, job_id: u64) {
        if let Err(error) = self.commit_analyze_job_plan(|snapshot, catalog| {
            tidb_stats_handle_storage::cluster_stats_write::plan_start_analyze_job(
                snapshot,
                catalog,
                job_id,
                utc_now_timestamp(),
                Some(self.row_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        }) {
            // Go `autoanalyze.startAnalyzeJob`.
            log_update_analyze_job_failed(
                &tidb_stats_handle_logutil::stats_logger(),
                "pending->running".to_owned(),
                error,
            );
        }
    }

    fn update_progress(&self, job_id: u64, processed_rows: i64) {
        if let Err(error) = self.commit_analyze_job_plan(|snapshot, catalog| {
            tidb_stats_handle_storage::cluster_stats_write::plan_update_analyze_job_progress(
                snapshot,
                catalog,
                job_id,
                processed_rows,
                utc_now_timestamp(),
                Some(self.row_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        }) {
            // Go `autoanalyze.updateAnalyzeJobProgress`.
            log_update_analyze_job_failed(
                &tidb_stats_handle_logutil::stats_logger(),
                format!("process {processed_rows} rows"),
                error,
            );
        }
    }

    fn finish(
        &self,
        job_id: u64,
        processed_rows: i64,
        failure: Option<&str>,
        kind: AnalyzeJobKind,
    ) {
        let processed_rows = match kind {
            AnalyzeJobKind::Table => processed_rows,
            AnalyzeJobKind::GlobalStatsMerge => 0,
        };
        if let Err(error) = self.commit_analyze_job_plan(|snapshot, catalog| {
            tidb_stats_handle_storage::cluster_stats_write::plan_finish_analyze_job(
                snapshot,
                catalog,
                job_id,
                processed_rows,
                failure,
                utc_now_timestamp(),
                Some(self.row_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        }) {
            // Go `autoanalyze.finishAnalyzeJob`, through the plain logger.
            let state = if failure.is_some() {
                "failed"
            } else {
                "finished"
            };
            log_update_analyze_job_failed(
                &tidb_util::logutil::bg_logger(),
                format!("running->{state}"),
                error,
            );
        }
    }
}

fn log_update_analyze_job_failed(
    logger: &tidb_util::logutil::Logger,
    update: String,
    error: impl std::fmt::Display,
) {
    logger.warn(
        "failed to update analyze job",
        &[
            tidb_log::Field::new("update", tidb_log::Value::Str(update)),
            crate::domain::zap_error(error),
        ],
    );
}

impl<'s> ClusterServerSession<'s> {
    /// Runs Go `AutoAnalyze` through the same routed physical ANALYZE path as
    /// a client statement while retaining its analyze-job identity.
    pub(super) fn run_auto_analyze_sql(
        &mut self,
        sql: &str,
        analyze_snapshot: Option<bool>,
        partition_prune_mode: &str,
    ) -> Result<WriteOutcome, SqlQueryError> {
        let _process_statement = self.session.retain_process_statement(sql);
        self.rebuild_catalog_if_stale();
        let parsed = self
            .session
            .parse_statement(sql)
            .map_err(super::map_error)?;
        // Go runs it as internal SQL through `ExecuteInternalStmt`, whose
        // `Compile` builds the plan like a client statement's.
        let super::StatementRoute::Analyze(mut tables) = self.compile_route(&parsed)? else {
            return Err(SqlQueryError::unknown(
                "auto analyze generated a non-ANALYZE statement",
            ));
        };
        for statement in &mut tables {
            statement.auto_analyze = true;
            if let Some(analyze_snapshot) = analyze_snapshot {
                statement.analyze_snapshot = analyze_snapshot;
            }
            statement.dynamic_partition_prune =
                !partition_prune_mode.eq_ignore_ascii_case("static");
        }
        self.run_analyze(&tables)
    }

    /// The statistics handle the session's services hold, for the running
    /// statement.
    fn stats_statement_service(
        &self,
    ) -> Box<dyn crate::stats_statement_service::StatsStatementService> {
        let stats = Arc::clone(&self.domain_services().stats);
        stats(&self.session)
    }

    /// Applies client-transferred LOAD STATS bytes through the cluster handle.
    pub(super) fn run_load_stats(&mut self, data: &[u8]) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self
            .session
            .run_load_stats(data, service.as_ref())
            .map_err(super::map_error)?;
        Ok(NO_ROWS)
    }

    pub(super) fn run_flush_stats_delta(
        &mut self,
        targets: &FlushStatsDeltaTargets,
    ) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self
            .session
            .run_flush_stats_delta(targets, service.as_ref())
            .map_err(super::map_error)?;
        Ok(NO_ROWS)
    }

    pub(super) fn run_drop_stats(
        &mut self,
        physical_ids: &[i64],
    ) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self
            .session
            .run_drop_stats(physical_ids, service.as_ref())
            .map_err(super::map_error)?;
        Ok(NO_ROWS)
    }

    pub(super) fn run_stats_lock(
        &mut self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self
            .session
            .run_stats_lock(statement, service.as_ref())
            .map_err(super::map_error)?;
        Ok(NO_ROWS)
    }

    /// Performs one `ANALYZE TABLE`, one table at a time.
    ///
    /// Each table is its own transaction, which is what Go does too: an
    /// `ANALYZE TABLE t1, t2` is two analyses, and holding one transaction
    /// open across both would make the second table's row count describe the
    /// moment the first was read.
    ///
    /// An open transaction is committed first, for the same reason a DDL or
    /// an account statement commits one: MySQL and Go both commit implicitly
    /// before a statement that changes stored state outside it.
    pub(super) fn run_analyze(
        &mut self,
        tables: &[AnalyzeStatement],
    ) -> Result<WriteOutcome, SqlQueryError> {
        // Go `AnalyzeExec.Next` (`pkg/executor/analyze.go:300`) has no
        // `NewTxnInStmt`: the statistics are written by internal sessions and
        // the user's transaction stays open.
        let analyze = Arc::clone(&self.domain_services().analyze);
        let service = analyze(&self.session);
        self
            .session
            .run_analyze_statements(tables, service.as_ref())
            .map_err(super::map_error)?;
        // Go answers `ANALYZE TABLE` with an OK packet carrying no rows.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }
}

/// The cluster analyzer as the session's [`AnalyzeService`], for the
/// statement running on the session it is built from: its memory, its
/// active resource group and its analyze-job identity.
///
/// [`AnalyzeService`]: crate::analyze_service::AnalyzeService
pub(super) fn analyze_service(
    domain: &super::Domain,
) -> crate::domain_services::StatementService<dyn crate::analyze_service::AnalyzeService> {
    analyze_service_over(domain, domain.analyze())
}

/// [`analyze_service`] over `analyze`, the domain's analyzer.
pub(super) fn analyze_service_over(
    domain: &super::Domain,
    analyze: Arc<dyn tidb_domain_core::cluster_analyze_seam::ClusterAnalyze>,
) -> crate::domain_services::StatementService<dyn crate::analyze_service::AnalyzeService> {
    let stats_usage = Arc::clone(&domain.stats_usage);
    let transactions = Arc::clone(&domain.transactions);
    let catalog = Arc::clone(&domain.catalog);
    let stats = Arc::clone(&domain.stats);
    let global_vars = domain.global_vars.clone();
    let approximate_table_counts = Arc::clone(&domain.approximate_table_counts);
    let historical_stats_worker = Arc::clone(&domain.historical_stats_worker);
    let row_ids = Arc::clone(&domain.auto_ids);
    Arc::new(move |session: &crate::Session| {
        Box::new(ClusterAnalyzeService {
            statement_memory: session.routed_statement_memory(),
            resource_group: session.active_resource_group().to_owned(),
            instance: session.analyze_job_instance(),
            connection_id: session.connection_id(),
            stats_usage: Arc::clone(&stats_usage),
            transactions: Arc::clone(&transactions),
            catalog: Arc::clone(&catalog),
            stats: Arc::clone(&stats),
            global_vars: global_vars.clone(),
            analyze: Arc::clone(&analyze),
            approximate_table_counts: Arc::clone(&approximate_table_counts),
            historical_stats_worker: Arc::clone(&historical_stats_worker),
            row_ids: Arc::clone(&row_ids),
        }) as Box<dyn crate::analyze_service::AnalyzeService>
    })
}

/// The statistics handle as the session's [`StatsStatementService`], for
/// the statement running on the session it is built from (its resource
/// group).
///
/// [`StatsStatementService`]: crate::stats_statement_service::StatsStatementService
pub(super) fn stats_statement_service(
    domain: &super::Domain,
) -> crate::domain_services::StatementService<
    dyn crate::stats_statement_service::StatsStatementService,
> {
    let stats_usage = Arc::clone(&domain.stats_usage);
    let transactions = Arc::clone(&domain.transactions);
    let catalog = Arc::clone(&domain.catalog);
    let stats = Arc::clone(&domain.stats);
    let global_vars = domain.global_vars.clone();
    let analyze = domain.analyze();
    let stats_lock = Arc::clone(&domain.stats_lock);
    let row_ids = Arc::clone(&domain.auto_ids);
    Arc::new(move |session: &crate::Session| {
        Box::new(ClusterStatsStatementService {
            resource_group: session.current_resource_group().to_owned(),
            stats_usage: Arc::clone(&stats_usage),
            transactions: Arc::clone(&transactions),
            catalog: Arc::clone(&catalog),
            stats: Arc::clone(&stats),
            global_vars: global_vars.clone(),
            analyze: Arc::clone(&analyze),
            stats_lock: Arc::clone(&stats_lock),
            row_ids: Arc::clone(&row_ids),
        }) as Box<dyn crate::stats_statement_service::StatsStatementService>
    })
}
/// The cluster analyzer as the session's [`AnalyzeService`]: owned handles,
/// because the session runs the statement while the node is borrowed.
///
/// [`AnalyzeService`]: crate::analyze_service::AnalyzeService
struct ClusterAnalyzeService {
    statement_memory: tidb_executor::StatementMemory,
    resource_group: String,
    instance: String,
    connection_id: u64,
    stats_usage: Arc<tidb_stats_handle_usage::StatsUsageHandle>,
    transactions: Arc<dyn super::ClusterTransactions>,
    catalog: Arc<super::SharedClusterCatalog>,
    stats: Arc<super::SharedStats>,
    global_vars: crate::GlobalSysvars,
    analyze: Arc<dyn tidb_domain_core::cluster_analyze_seam::ClusterAnalyze>,
    approximate_table_counts: Arc<super::ClusterApproximateTableCountProvider>,
    historical_stats_worker: Arc<
        tidb_domain::historical_stats::HistoricalStatsWorker<super::ClusterHistoricalInfoSchema>,
    >,
    row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
}

impl crate::analyze_service::AnalyzeService for ClusterAnalyzeService {
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), tidb_executor::DriverError> {
        super::Domain::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.transactions.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            true,
            target_ids,
            &self.resource_group,
            &self.row_ids,
        )
        .map_err(tidb_executor::DriverError::unsupported)
    }

    fn builds_column_task(&self, statement: &AnalyzeStatement) -> bool {
        tidb_executor::real_tikv_analyze::analyze_builds_column_task(&self.catalog.load(), statement)
    }

    fn execute(
        &self,
        statement: &AnalyzeStatement,
        historical_stats_enabled: bool,
    ) -> Result<tidb_executor::real_tikv_analyze::ClusterAnalyzeReport, tidb_executor::DriverError>
    {
        let jobs = PersistedAnalyzeJobs {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            instance: self.instance.clone(),
            process_id: self.connection_id,
            global_vars: self.global_vars.clone(),
            row_ids: Arc::clone(&self.row_ids),
        };
        let report = recover_analyze_panic(|| {
            self.analyze.execute(
                statement,
                &self.resource_group,
                self.approximate_table_counts.as_ref(),
                self.statement_memory.sql_killer(),
                &|| historical_stats_enabled,
                &jobs,
            )
        })
        .map_err(|error| SqlQueryError::unknown(error.rendered_message()))
        .and_then(|result| result)
        .map_err(|error| {
            tidb_executor::DriverError::Mysql(
                tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
                    .with_identity_opt(error.identity),
            )
        })?;
        // Not log content (Go has no such per-statement summary):
        // rust/scripts/run-realtikv-analyze.sh reads this JSON line.
        eprintln!(
            "{{\"event\":\"cluster_table_analyzed\",\"schema\":{},\"table\":{},\
             \"table_id\":{},\"version\":{},\"scanned_rows\":{},\"sampled_rows\":{},\
             \"sample_rate\":{},\"histograms\":{},\"buckets\":{},\"topn\":{}}}",
            serde_json::to_string(&statement.schema).unwrap_or_else(|_| "\"\"".to_owned()),
            serde_json::to_string(&statement.table).unwrap_or_else(|_| "\"\"".to_owned()),
            report.table_id,
            report.version,
            report.scanned_rows,
            report.sampled_rows,
            report.sample_rate,
            report.histogram_count,
            report.bucket_count,
            report.topn_count,
        );
        Ok(report)
    }

    fn update_stats_cache(&self, physical_ids: &[i64]) -> Result<(), tidb_executor::DriverError> {
        self.analyze
            .update_stats_cache(physical_ids.to_vec())
            .map_err(into_driver_error)
    }

    fn publish_temporary_table_stats(&self, table_id: i64, statistics: Arc<tidb_stats::Table>) {
        self.stats.publish_analyzed_table(table_id, statistics);
    }

    fn dump_historical_stats(&self, table_id: i64) {
        self.historical_stats_worker
            .send_tbl_to_dump_historical_stats(table_id);
    }
}

/// The statistics handle as the session's [`StatsStatementService`]: owned
/// handles, because the session runs the statement while the node is
/// borrowed.
///
/// [`StatsStatementService`]: crate::stats_statement_service::StatsStatementService
struct ClusterStatsStatementService {
    resource_group: String,
    stats_usage: Arc<tidb_stats_handle_usage::StatsUsageHandle>,
    transactions: Arc<dyn super::ClusterTransactions>,
    catalog: Arc<super::SharedClusterCatalog>,
    stats: Arc<super::SharedStats>,
    global_vars: crate::GlobalSysvars,
    analyze: Arc<dyn tidb_domain_core::cluster_analyze_seam::ClusterAnalyze>,
    stats_lock: Arc<dyn tidb_domain_core::cluster_stats_lock_seam::ClusterStatsLock>,
    row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
}

impl crate::stats_statement_service::StatsStatementService for ClusterStatsStatementService {
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), tidb_executor::DriverError> {
        super::Domain::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.transactions.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            true,
            target_ids,
            &self.resource_group,
            &self.row_ids,
        )
        .map_err(tidb_executor::DriverError::unsupported)
    }

    fn drop_stats(&self, physical_ids: &[i64]) -> Result<(), tidb_executor::DriverError> {
        self.analyze
            .drop_stats(physical_ids)
            .map_err(into_driver_error)
    }

    fn lock_stats(
        &self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<String, tidb_executor::DriverError> {
        self.stats_lock
            .execute(statement)
            .map(|report| report.warning)
            .map_err(into_driver_error)
    }

    fn load_stats(
        &self,
        json: &tidb_stats_handle::load_stats::JsonTable,
        historical_stats_enabled: bool,
    ) -> Result<(), tidb_executor::DriverError> {
        // Go's LOAD STATS path logs nothing on success.
        self.analyze
            .load_stats(json, historical_stats_enabled)
            .map_err(into_driver_error)?;
        Ok(())
    }
}
