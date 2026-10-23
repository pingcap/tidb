//! `ANALYZE TABLE` on the convergence node: who may run one, how much memory
//! its sampling may use, and what each analysed table reports.
//!
//! The statement is not answered by the session driver at all -- statistics
//! are stored cluster state, so it is routed to
//! [`ClusterAnalyze`](crate::cluster_analyze_seam::ClusterAnalyze), one
//! transaction per named table. Mirrors Go's split across
//! `pkg/planner/core/planbuilder.go` (`requireInsertAndSelectPriv`, the
//! privilege gate below) and `pkg/executor/analyze.go` with
//! `variable.SetMemQuotaAnalyze` (`pkg/executor/select.go:141`, the quota
//! below).

use std::sync::Arc;
use tidb_exec::cluster_analyze::AnalyzeStatement;
use tidb_exec::cluster_stats_lock::ClusterStatsLockStatement;
use tidb_exec::cluster_stats_write::StatsWritePlan;
use tidb_exec::mysql_bootstrap::utc_now_timestamp;
use tidb_exec::real_tikv_analyze::{AnalyzeJobKind, AnalyzeJobLifecycle, AnalyzeJobSpec};
use tidb_executor::analyze::panic_recovery::recover_analyze_panic;

use crate::sql_node::{QuerySession, SqlQueryError, WriteOutcome};

use super::{ClusterServerSession, ClusterTransactions, SharedClusterCatalog};
use tidb_session::stats_statement_service::FlushStatsDeltaTargets;

/// Go answers the statistics statements with an OK packet carrying no rows.
const NO_ROWS: WriteOutcome = WriteOutcome {
    affected_rows: 0,
    last_insert_id: 0,
};

/// A cluster seam's error as the session's.
fn into_driver_error(error: SqlQueryError) -> tidb_executor::DriverError {
    tidb_executor::DriverError::Mysql(tidb_executor::MysqlError::from_parts(
        error.code,
        error.state,
        error.message,
    ))
}

struct PersistedAnalyzeJobs {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
    instance: String,
    process_id: u64,
    global_vars: tidb_session::GlobalSysvars,
    /// The node's live row-ID allocators (Go's internal INSERT allocates
    /// `mysql.analyze_jobs.id` from the table's AUTO_INCREMENT allocator).
    row_ids: Arc<dyn tidb_exec::table_from_meta::TableAutoIds>,
}

impl PersistedAnalyzeJobs {
    fn commit_analyze_job_plan(
        &self,
        build: impl FnOnce(
            &mut super::SnapshotMetaSnapshot,
            &tidb_exec::cluster_catalog::ClusterCatalog,
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
                crate::session_transaction::restricted_transaction_options(&self.global_vars),
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
            let mut snapshot =
                super::SnapshotMetaSnapshot::new(snapshot).with_row_ids(Arc::clone(&self.row_ids));
            let (job_id, plan) = tidb_exec::cluster_stats_write::plan_insert_analyze_job(
                &mut snapshot,
                &catalog,
                &spec.schema,
                &spec.table,
                &spec.partition,
                spec.job_info.as_bytes(),
                &self.instance,
                self.process_id,
                utc_now_timestamp(),
            )
            .map_err(|error| error.to_string())?;
            self.transactions
                .commit_optimistic_mutations(
                    plan.mutations,
                    read_ts,
                    "default",
                    crate::session_transaction::restricted_transaction_options(&self.global_vars),
                )
                .map_err(|error| error.message)?;
            Ok::<_, String>(job_id)
        })();
        match result {
            Ok(job_id) => Some(job_id),
            Err(error) => {
                // Go `AddNewAnalyzeJob`.
                tidb_stats_handle_logutil::stats_err_verbose_logger()
                    .error("failed to insert analyze job", &[crate::zap_error(error)]);
                None
            }
        }
    }

    fn start(&self, job_id: u64) {
        if let Err(error) = self.commit_analyze_job_plan(|snapshot, catalog| {
            tidb_exec::cluster_stats_write::plan_start_analyze_job(
                snapshot,
                catalog,
                job_id,
                utc_now_timestamp(),
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
            tidb_exec::cluster_stats_write::plan_update_analyze_job_progress(
                snapshot,
                catalog,
                job_id,
                processed_rows,
                utc_now_timestamp(),
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
            tidb_exec::cluster_stats_write::plan_finish_analyze_job(
                snapshot,
                catalog,
                job_id,
                processed_rows,
                failure,
                utc_now_timestamp(),
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
            crate::zap_error(error),
        ],
    );
}

impl ClusterServerSession {
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
        let super::StatementRoute::Analyze(mut tables) = self.schema_route(&parsed)? else {
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

    /// Applies client-transferred LOAD STATS bytes through the cluster handle.
    /// The statistics handle as the session's [`StatsStatementService`].
    ///
    /// [`StatsStatementService`]: tidb_session::stats_statement_service::StatsStatementService
    fn stats_statement_service(&self) -> ClusterStatsStatementService {
        ClusterStatsStatementService {
            resource_group: self.session.current_resource_group().to_owned(),
            stats_usage: Arc::clone(&self.stats_usage),
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            stats: Arc::clone(&self.stats),
            global_vars: self.global_vars.clone(),
            analyze: Arc::clone(&self.analyze),
            stats_lock: Arc::clone(&self.stats_lock),
            row_ids: Arc::clone(&self.auto_ids),
        }
    }

    pub(super) fn run_load_stats(&mut self, data: &[u8]) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self.session
            .run_load_stats(data, &service)
            .map_err(super::map_error)?;
        Ok(NO_ROWS)
    }

    pub(super) fn run_flush_stats_delta(
        &mut self,
        targets: &FlushStatsDeltaTargets,
    ) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self.session
            .run_flush_stats_delta(targets, &service)
            .map_err(super::map_error)?;
        Ok(NO_ROWS)
    }

    pub(super) fn run_drop_stats(
        &mut self,
        physical_ids: &[i64],
    ) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self.session
            .run_drop_stats(physical_ids, &service)
            .map_err(super::map_error)?;
        Ok(NO_ROWS)
    }

    pub(super) fn run_stats_lock(
        &mut self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self.session
            .run_stats_lock(statement, &service)
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
        let statement_memory = self.session.routed_statement_memory();
        if self.txn.explicit.is_some() || self.session.in_transaction() {
            self.control_transaction("COMMIT")?;
        }
        self.record_new_txn_in_stmt();
        let service = ClusterAnalyzeService {
            statement_memory,
            resource_group: self.session.active_resource_group().to_owned(),
            instance: self.session.analyze_job_instance(),
            connection_id: self.connection_id,
            stats_usage: Arc::clone(&self.stats_usage),
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            stats: Arc::clone(&self.stats),
            global_vars: self.global_vars.clone(),
            analyze: Arc::clone(&self.analyze),
            approximate_table_counts: Arc::clone(&self.approximate_table_counts),
            historical_stats_worker: Arc::clone(&self.historical_stats_worker),
            row_ids: Arc::clone(&self.auto_ids),
        };
        self.session
            .run_analyze_statements(tables, &service)
            .map_err(super::map_error)?;
        // Go answers `ANALYZE TABLE` with an OK packet carrying no rows.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }
}

/// The cluster analyzer as the session's [`AnalyzeService`]: owned handles,
/// because the session runs the statement while the node is borrowed.
///
/// [`AnalyzeService`]: tidb_session::analyze_service::AnalyzeService
struct ClusterAnalyzeService {
    statement_memory: tidb_executor::StatementMemory,
    resource_group: String,
    instance: String,
    connection_id: u64,
    stats_usage: Arc<tidb_stats_handle_usage::StatsUsageHandle>,
    transactions: Arc<dyn super::ClusterTransactions>,
    catalog: Arc<super::SharedClusterCatalog>,
    stats: Arc<super::SharedStats>,
    global_vars: tidb_session::GlobalSysvars,
    analyze: Arc<dyn crate::cluster_analyze_seam::ClusterAnalyze>,
    approximate_table_counts: Arc<super::ClusterApproximateTableCountProvider>,
    historical_stats_worker: Arc<
        tidb_domain::historical_stats::HistoricalStatsWorker<super::ClusterHistoricalInfoSchema>,
    >,
    row_ids: Arc<dyn tidb_exec::table_from_meta::TableAutoIds>,
}

impl tidb_session::analyze_service::AnalyzeService for ClusterAnalyzeService {
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), tidb_executor::DriverError> {
        super::ClusterSessionFactory::dump_stats_delta_to_kv_parts(
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

    fn execute(
        &self,
        statement: &AnalyzeStatement,
        historical_stats_enabled: bool,
    ) -> Result<tidb_exec::real_tikv_analyze::ClusterAnalyzeReport, tidb_executor::DriverError>
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
            tidb_executor::DriverError::Mysql(tidb_executor::MysqlError::from_parts(
                error.code,
                error.state,
                error.message,
            ))
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
/// [`StatsStatementService`]: tidb_session::stats_statement_service::StatsStatementService
struct ClusterStatsStatementService {
    resource_group: String,
    stats_usage: Arc<tidb_stats_handle_usage::StatsUsageHandle>,
    transactions: Arc<dyn super::ClusterTransactions>,
    catalog: Arc<super::SharedClusterCatalog>,
    stats: Arc<super::SharedStats>,
    global_vars: tidb_session::GlobalSysvars,
    analyze: Arc<dyn crate::cluster_analyze_seam::ClusterAnalyze>,
    stats_lock: Arc<dyn crate::cluster_stats_lock_seam::ClusterStatsLock>,
    row_ids: Arc<dyn tidb_exec::table_from_meta::TableAutoIds>,
}

impl tidb_session::stats_statement_service::StatsStatementService for ClusterStatsStatementService {
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), tidb_executor::DriverError> {
        super::ClusterSessionFactory::dump_stats_delta_to_kv_parts(
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
        json: &tidb_executor::load_stats::JsonTable,
        historical_stats_enabled: bool,
    ) -> Result<(), tidb_executor::DriverError> {
        // Go's LOAD STATS path logs nothing on success.
        self.analyze
            .load_stats(json, historical_stats_enabled)
            .map_err(into_driver_error)?;
        Ok(())
    }
}
