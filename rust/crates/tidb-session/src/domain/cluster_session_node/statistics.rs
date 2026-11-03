//! `ANALYZE TABLE` on the convergence node: who may run one, how much memory
//! its sampling may use, and what each analysed table reports.
//!
//! The statement is not answered by the session driver at all -- statistics
//! are stored cluster state, so it is routed to
//! [`ClusterAnalyze`](tidb_executor::cluster_analyze_seam::ClusterAnalyze), one
//! transaction per named table. Mirrors Go's split across
//! `pkg/planner/core/planbuilder.go` (`requireInsertAndSelectPriv`, the
//! privilege gate below) and `pkg/executor/analyze.go` with
//! `variable.SetMemQuotaAnalyze` (`pkg/executor/select.go:141`, the quota
//! below).

use std::sync::Arc;
use tidb_executor::cluster_analyze::AnalyzeStatement;
use tidb_executor_lockstats::ClusterStatsLockStatement;
use tidb_executor::real_tikv_analyze::{AnalyzeJobKind, AnalyzeJobLifecycle, AnalyzeJobSpec};
use tidb_executor::analyze::exec::AnalyzeExec as _;
use tidb_executor::analyze::panic_recovery::recover_analyze_panic;
use tidb_executor::stats_exec::StatsExec as _;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;

use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_executor::ddl_exec::WriteOutcome;

use super::ClusterServerSession;
use tidb_executor::stats_exec::FlushStatsDeltaTargets;

/// Go answers the statistics statements with an OK packet carrying no rows.
const NO_ROWS: WriteOutcome = WriteOutcome {
    affected_rows: 0,
    last_insert_id: 0,
};

/// A cluster seam's error as the session's.
/// A statistics session's error as the statement's own: Go returns the
/// statement's error unchanged.
use tidb_domain_core::sys_session::stats_session_sql_error;

fn into_driver_error(error: SqlQueryError) -> tidb_driver_error::DriverError {
    tidb_driver_error::DriverError::Mysql(
        tidb_terror::MysqlError::from_parts(error.code, error.state, error.message)
            .with_identity_opt(error.identity),
    )
}

/// Go `statsAnalyze`'s analyze-job lifecycle: Go's statements on the
/// statistics handle's session pool (`util.CallWithSCtx(sa.statsHandle.SPool(),
/// ...)`), each in its own autocommit transaction.
struct PersistedAnalyzeJobs {
    session_pool: Option<Arc<tidb_domain_core::sys_session::DomainSysSessionPool>>,
    instance: String,
    process_id: u64,
}

/// Go `statistics.AnalyzePending` / `AnalyzeRunning` / `AnalyzeFinished` /
/// `AnalyzeFailed`.
const ANALYZE_PENDING: &str = "pending";
const ANALYZE_RUNNING: &str = "running";
const ANALYZE_FINISHED: &str = "finished";
const ANALYZE_FAILED: &str = "failed";
/// Go's `textMaxLength` for `job_info` and `fail_reason`.
const TEXT_MAX_LENGTH: usize = 65535;

/// Go `time.Now().UTC().Format(types.TimeFormat)`.
fn utc_now_text() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

impl PersistedAnalyzeJobs {
    /// Go `statsutil.CallWithSCtx(pool, f)` without a wrapping transaction.
    fn call_with_sctx(
        &self,
        f: impl FnOnce(
            &dyn tidb_domain_core::sys_session::DomainSysSession,
        ) -> Result<(), tidb_sqlexec::SqlExecError>,
    ) -> Result<(), String> {
        let pool = self
            .session_pool
            .as_ref()
            .ok_or_else(|| "the node's domain is closed".to_owned())?;
        tidb_stats_handle_util::call_with_sctx(pool.as_ref(), |sctx| f(sctx), &[])
            .map_err(|error| error.to_string())
    }
}

impl AnalyzeJobLifecycle for PersistedAnalyzeJobs {
    /// Go `insertAnalyzeJob`.
    fn insert(&self, spec: &AnalyzeJobSpec) -> Option<u64> {
        let mut job_id = None;
        let result = self.call_with_sctx(|sctx| {
            use tidb_sqlescape::SqlArg;
            let job_info = spec.job_info.as_bytes();
            let job_info = &job_info[..job_info.len().min(TEXT_MAX_LENGTH)];
            tidb_stats_handle_util::exec_rows(
                sctx,
                "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, state, instance, process_id) VALUES (%?, %?, %?, %?, %?, %?, %?)",
                &[
                    SqlArg::from(spec.schema.as_str()),
                    SqlArg::from(spec.table.as_str()),
                    SqlArg::from(spec.partition.as_str()),
                    SqlArg::String(job_info),
                    SqlArg::from(ANALYZE_PENDING),
                    SqlArg::from(self.instance.as_str()),
                    SqlArg::Unsigned(self.process_id),
                ],
            )?;
            let (rows, _) = tidb_stats_handle_util::exec_rows(sctx, "SELECT LAST_INSERT_ID()", &[])?;
            job_id = match rows.first().and_then(|row| row.first()) {
                Some(tidb_datatype::Datum::UInt(id)) => Some(*id),
                Some(tidb_datatype::Datum::Int(id)) => Some(*id as u64),
                _ => Some(0),
            };
            Ok(())
        });
        match result {
            Ok(()) => job_id,
            Err(error) => {
                // Go `AddNewAnalyzeJob`.
                tidb_stats_handle_logutil::stats_err_verbose_logger()
                    .error("failed to insert analyze job", &[crate::domain::zap_error(error)]);
                None
            }
        }
    }

    /// Go `startAnalyzeJob`.
    fn start(&self, job_id: u64) {
        let result = self.call_with_sctx(|sctx| {
            use tidb_sqlescape::SqlArg;
            let start_time = utc_now_text();
            tidb_stats_handle_util::exec_rows(
                sctx,
                "UPDATE mysql.analyze_jobs SET start_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %? WHERE id = %?",
                &[
                    SqlArg::from(start_time.as_str()),
                    SqlArg::from(ANALYZE_RUNNING),
                    SqlArg::Unsigned(job_id),
                ],
            )?;
            Ok(())
        });
        if let Err(error) = result {
            log_update_analyze_job_failed(
                &tidb_stats_handle_logutil::stats_logger(),
                format!("{ANALYZE_PENDING}->{ANALYZE_RUNNING}"),
                error,
            );
        }
    }

    /// Go `updateAnalyzeJobProgress`, `processed_rows` being the delta.
    fn update_progress(&self, job_id: u64, processed_rows: i64) {
        let result = self.call_with_sctx(|sctx| {
            use tidb_sqlescape::SqlArg;
            tidb_stats_handle_util::exec_rows(
                sctx,
                "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %? WHERE id = %?",
                &[SqlArg::Signed(processed_rows), SqlArg::Unsigned(job_id)],
            )?;
            Ok(())
        });
        if let Err(error) = result {
            log_update_analyze_job_failed(
                &tidb_stats_handle_logutil::stats_logger(),
                format!("process {processed_rows} rows"),
                error,
            );
        }
    }

    /// Go `finishAnalyzeJob`.
    fn finish(
        &self,
        job_id: u64,
        processed_rows: i64,
        failure: Option<&str>,
        kind: AnalyzeJobKind,
    ) {
        let result = self.call_with_sctx(|sctx| {
            use tidb_sqlescape::SqlArg;
            let end_time = utc_now_text();
            let fail_reason = failure.map(|reason| {
                let reason = reason.as_bytes();
                &reason[..reason.len().min(TEXT_MAX_LENGTH)]
            });
            let table_job = kind == AnalyzeJobKind::Table;
            let (sql, args): (&str, Vec<SqlArg<'_>>) = match (fail_reason, table_job) {
                (Some(reason), true) => (
                    "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?",
                    vec![
                        SqlArg::Signed(processed_rows),
                        SqlArg::from(end_time.as_str()),
                        SqlArg::from(ANALYZE_FAILED),
                        SqlArg::String(reason),
                        SqlArg::Unsigned(job_id),
                    ],
                ),
                (Some(reason), false) => (
                    "UPDATE mysql.analyze_jobs SET end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?",
                    vec![
                        SqlArg::from(end_time.as_str()),
                        SqlArg::from(ANALYZE_FAILED),
                        SqlArg::String(reason),
                        SqlArg::Unsigned(job_id),
                    ],
                ),
                (None, true) => (
                    "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?",
                    vec![
                        SqlArg::Signed(processed_rows),
                        SqlArg::from(end_time.as_str()),
                        SqlArg::from(ANALYZE_FINISHED),
                        SqlArg::Unsigned(job_id),
                    ],
                ),
                (None, false) => (
                    "UPDATE mysql.analyze_jobs SET end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?",
                    vec![
                        SqlArg::from(end_time.as_str()),
                        SqlArg::from(ANALYZE_FINISHED),
                        SqlArg::Unsigned(job_id),
                    ],
                ),
            };
            tidb_stats_handle_util::exec_rows(sctx, sql, &args)?;
            Ok(())
        });
        if let Err(error) = result {
            // Go `autoanalyze.finishAnalyzeJob`, through the plain logger.
            let state = if failure.is_some() {
                ANALYZE_FAILED
            } else {
                ANALYZE_FINISHED
            };
            log_update_analyze_job_failed(
                &tidb_logutil::bg_logger(),
                format!("{ANALYZE_RUNNING}->{state}"),
                error,
            );
        }
    }
}

fn log_update_analyze_job_failed(
    logger: &tidb_logutil::Logger,
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
    ) -> Box<dyn tidb_executor::stats_exec::StatsStatementService> {
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
        // A temporary table is sampled in-process (see
        // `AnalyzeExec::with_temporary_table_snapshot`); the node, which
        // this routed statement holds, is where its snapshot binds.
        let bound_here =
            crate::temptable::bind_temporary_read_snapshot(&self.node.slot, &self.session);
        let analyzed = self
            .session
            .run_analyze_statements(tables, service.as_ref())
            .map_err(super::map_error);
        crate::temptable::finish_temporary_read_snapshot(&self.node.slot, bound_here);
        analyzed?;
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
) -> crate::domain_services::StatementService<dyn tidb_executor::analyze::exec::AnalyzeService> {
    analyze_service_over(domain, super::node_services(domain).analyze())
}

/// [`analyze_service`] over `analyze`, the domain's analyzer.
pub(super) fn analyze_service_over(
    domain: &super::Domain,
    analyze: Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze>,
) -> crate::domain_services::StatementService<dyn tidb_executor::analyze::exec::AnalyzeService> {
    let stats_usage = Arc::clone(&domain.stats_usage);
    let catalog = Arc::clone(&domain.catalog);
    let stats = Arc::clone(&domain.stats);
    let global_vars = domain.global_vars.clone();
    let approximate_table_counts = Arc::clone(&domain.approximate_table_counts);
    let historical_stats_worker = Arc::clone(&domain.historical_stats_worker);
    let session_pool = domain
        .binding_session_factory
        .upgrade()
        .map(|domain| domain.advanced_sys_session_pool());
    Arc::new(move |session: &crate::Session| {
        Box::new(ClusterAnalyzeService {
            statement_memory: session.routed_statement_memory(),
            resource_group: session.active_resource_group().to_owned(),
            instance: session.analyze_job_instance(),
            connection_id: session.connection_id(),
            stats_usage: Arc::clone(&stats_usage),
            catalog: Arc::clone(&catalog),
            stats: Arc::clone(&stats),
            global_vars: global_vars.clone(),
            analyze: Arc::clone(&analyze),
            approximate_table_counts: Arc::clone(&approximate_table_counts),
            historical_stats_worker: Arc::clone(&historical_stats_worker),
            session_pool: session_pool.clone(),
        }) as Box<dyn tidb_executor::analyze::exec::AnalyzeService>
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
    dyn tidb_executor::stats_exec::StatsStatementService,
> {
    let stats_usage = Arc::clone(&domain.stats_usage);
    let catalog = Arc::clone(&domain.catalog);
    let stats = Arc::clone(&domain.stats);
    let global_vars = domain.global_vars.clone();
    let analyze = super::node_services(domain).analyze();
    let session_pool = domain
        .binding_session_factory
        .upgrade()
        .map(|domain| domain.advanced_sys_session_pool());
    Arc::new(move |_session: &crate::Session| {
        Box::new(ClusterStatsStatementService {
            stats_usage: Arc::clone(&stats_usage),
            catalog: Arc::clone(&catalog),
            stats: Arc::clone(&stats),
            global_vars: global_vars.clone(),
            analyze: Arc::clone(&analyze),
            session_pool: session_pool.clone(),
        }) as Box<dyn tidb_executor::stats_exec::StatsStatementService>
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
    catalog: Arc<super::InfoCache>,
    stats: Arc<super::SharedStats>,
    global_vars: crate::GlobalSysvars,
    analyze: Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze>,
    approximate_table_counts: Arc<super::ClusterApproximateTableCountProvider>,
    historical_stats_worker: Arc<
        tidb_domain::historical_stats::HistoricalStatsWorker<super::ClusterHistoricalInfoSchema>,
    >,
    /// Go `statsHandle.SPool()`: the analyze jobs' statements run on it.
    session_pool: Option<Arc<tidb_domain_core::sys_session::DomainSysSessionPool>>,
}

impl tidb_executor::analyze::exec::AnalyzeService for ClusterAnalyzeService {
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), tidb_driver_error::DriverError> {
        super::Domain::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            &tidb_domain_core::sys_session::StatsPool {
                session_pool: self.session_pool.as_ref(),
            },
            true,
            target_ids,
        )
        .map_err(tidb_driver_error::DriverError::unsupported)
    }

    fn builds_column_task(&self, statement: &AnalyzeStatement) -> bool {
        tidb_executor::real_tikv_analyze::analyze_builds_column_task(&self.catalog.get_latest(), statement)
    }

    fn execute(
        &self,
        statement: &AnalyzeStatement,
        historical_stats_enabled: bool,
    ) -> Result<tidb_executor::real_tikv_analyze::ClusterAnalyzeReport, tidb_driver_error::DriverError>
    {
        let jobs = PersistedAnalyzeJobs {
            session_pool: self.session_pool.clone(),
            instance: self.instance.clone(),
            process_id: self.connection_id,
        };
        let report = recover_analyze_panic(|| {
            self.analyze.execute(
                statement,
                &self.resource_group,
                self.approximate_table_counts.as_ref(),
                self.statement_memory.sql_killer(),
                &|| historical_stats_enabled,
                &jobs,
                &tidb_domain_core::sys_session::StatsPool {
                    session_pool: self.session_pool.as_ref(),
                },
            )
        })
        .map_err(|error| SqlQueryError::unknown(error.rendered_message()))
        .and_then(|result| result)
        .map_err(|error| {
            tidb_driver_error::DriverError::Mysql(
                tidb_terror::MysqlError::from_parts(error.code, error.state, error.message)
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

    fn update_stats_cache(&self, physical_ids: &[i64]) -> Result<(), tidb_driver_error::DriverError> {
        self.analyze
            .update_stats_cache(
                physical_ids.to_vec(),
                &tidb_domain_core::sys_session::StatsPool {
                    session_pool: self.session_pool.as_ref(),
                },
            )
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
    stats_usage: Arc<tidb_stats_handle_usage::StatsUsageHandle>,
    catalog: Arc<super::InfoCache>,
    stats: Arc<super::SharedStats>,
    global_vars: crate::GlobalSysvars,
    analyze: Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze>,
    /// Go `statsHandle.SPool()`: the sessions `statsLockImpl` runs its
    /// statements on.
    session_pool: Option<Arc<tidb_domain_core::sys_session::DomainSysSessionPool>>,
}

impl tidb_executor::stats_exec::StatsStatementService for ClusterStatsStatementService {
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), tidb_driver_error::DriverError> {
        super::Domain::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            &tidb_domain_core::sys_session::StatsPool {
                session_pool: self.session_pool.as_ref(),
            },
            true,
            target_ids,
        )
        .map_err(tidb_driver_error::DriverError::unsupported)
    }

    fn drop_stats(&self, physical_ids: &[i64]) -> Result<(), tidb_driver_error::DriverError> {
        self.analyze
            .drop_stats(
                physical_ids,
                &tidb_domain_core::sys_session::StatsPool {
                    session_pool: self.session_pool.as_ref(),
                },
            )
            .map_err(into_driver_error)
    }

    fn lock_stats(
        &self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<String, tidb_driver_error::DriverError> {
        // Go `LockExec.Next`: the tables resolve against the domain's latest
        // schema (`do.InfoSchema()`) before the handle's transaction.
        let request = statement.resolve(&self.catalog.get_latest()).map_err(|error| {
            into_driver_error(
                tidb_stats_handle_lockstats::cluster_stats_lock::cluster_stats_lock_error(error),
            )
        })?;
        let session_pool = self.session_pool.as_ref().ok_or_else(|| {
            tidb_driver_error::DriverError::unsupported("the node's domain is closed")
        })?;
        tidb_stats_handle_lockstats::session_stats_lock::execute_stats_lock(
            session_pool.as_ref(),
            &request,
        )
        .map_err(|error| into_driver_error(stats_session_sql_error(error)))
    }

    fn load_stats(
        &self,
        json: &tidb_stats_util::JsonTable,
        historical_stats_enabled: bool,
    ) -> Result<(), tidb_driver_error::DriverError> {
        // Go's LOAD STATS path logs nothing on success.
        self.analyze
            .load_stats(
                json,
                historical_stats_enabled,
                &tidb_domain_core::sys_session::StatsPool {
                    session_pool: self.session_pool.as_ref(),
                },
            )
            .map_err(into_driver_error)?;
        Ok(())
    }
}
