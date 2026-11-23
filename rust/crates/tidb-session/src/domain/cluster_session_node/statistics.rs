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

impl<'s> ClusterServerSession<'s> {
    /// The statistics handle the session's services hold, for the running
    /// statement.
    pub(super) fn stats_statement_service(
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

    /// Go `executeRefreshStatsOnCurrentInstance`.
    pub(super) fn run_refresh_stats(
        &mut self,
        refresh: &tidb_ast::RefreshStatsStmt,
    ) -> Result<WriteOutcome, SqlQueryError> {
        let service = self.stats_statement_service();
        self
            .session
            .run_refresh_stats(refresh, service.as_ref())
            .map_err(super::map_error)?;
        Ok(NO_ROWS)
    }

    /// Go `broadcast`.
    pub(super) fn run_broadcast(&mut self, query: &str) -> Result<WriteOutcome, SqlQueryError> {
        self.session.broadcast(query).map_err(super::map_error)?;
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

    /// Go `AnalyzeExec` over the planned `Analyze`.
    pub(super) fn run_analyze(
        &mut self,
        plan: &tidb_planner::plan_builder::analyze::Analyze,
        build_ctx: Option<&tidb_sessionctx::StmtContext>,
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
            .run_analyze_statements(plan, build_ctx, service.as_ref())
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

/// The statistics handle as the planner's `buildAnalyze` reads it.
pub(super) struct ClusterAnalyzePlanService {
    stats: Arc<tidb_stats_handle_syncload::stats_watch::SharedStats>,
    session_pool: Option<Arc<tidb_domain_core::sys_session::DomainSysSessionPool>>,
}

/// The domain's [`ClusterAnalyzePlanService`].
pub(super) fn analyze_plan_service(domain: &super::Domain) -> Arc<dyn crate::domain_services::AnalyzePlanService + Send + Sync> {
    Arc::new(ClusterAnalyzePlanService {
        stats: Arc::clone(&domain.stats),
        session_pool: domain
            .binding_session_factory
            .upgrade()
            .map(|domain| domain.advanced_sys_session_pool()),
    })
}

impl crate::domain_services::AnalyzePlanService for ClusterAnalyzePlanService {
    fn get_predicate_columns(&self, table_id: i64) -> Result<Vec<i64>, tidb_driver_error::DriverError> {
        let mut columns = Vec::new();
        tidb_stats_handle_storage::stats_read_writer::StatsSessionPool::call_with_sctx(
            &tidb_domain_core::sys_session::StatsPool {
                session_pool: self.session_pool.as_ref(),
            },
            &mut |sctx| {
                columns = tidb_stats_handle_usage_predicatecolumn::predicate_column::get_predicate_columns(sctx, table_id)?;
                Ok(())
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .map_err(|error| {
            tidb_driver_error::DriverError::Mysql(
                tidb_terror::MysqlError::from_parts(error.code, error.state, error.message).with_identity_opt(error.identity),
            )
        })?;
        Ok(columns)
    }

    fn analyze_version_matches(&self, physical_id: i64, table: &tidb_model::TableInfo, version: i64) -> bool {
        // Go `GetPhysicalTableStats` caches a pseudo table on a miss.
        let cacheable = table.temp_table_type == tidb_model::TempTableType::NONE;
        let stats = self.stats.get_physical_table_stats(physical_id, table.partition.is_some(), cacheable, || {
            tidb_stats::pseudo_table(table, false, true)
        });
        tidb_stats::analyze_version_policy::analyze_version_matches(
            stats.as_ref().map(|stats| i64::from(stats.hist_coll.stats_version)),
            stats.as_ref().is_some_and(|stats| stats.hist_coll.pseudo),
            version,
        )
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
    let factory = domain.binding_session_factory.clone();
    Arc::new(move |_session: &crate::Session| {
        Box::new(ClusterStatsStatementService {
            domain: factory.clone(),
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

    fn execute(
        &self,
        plan: &tidb_planner::plan_builder::analyze::Analyze,
        vars: &AnalyzeStatement,
    ) -> Result<tidb_executor::real_tikv_analyze::AnalyzeExecReport, tidb_driver_error::DriverError> {
        // Go `statsHandle`'s `StatsAnalyze` analyze-job methods.
        let jobs = tidb_stats_handle_autoanalyze::analyze_jobs::StatsAnalyzeJobs::new(
            self.session_pool.clone().map(|pool| {
                pool as Arc<dyn tidb_syssession::Pool<dyn tidb_domain_core::sys_session::DomainSysSession>>
            }),
            self.instance.clone(),
            self.connection_id,
        );
        let report = recover_analyze_panic(|| {
            self.analyze.execute(
                plan,
                vars,
                &self.resource_group,
                self.approximate_table_counts.as_ref(),
                self.statement_memory.sql_killer(),
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
        Ok(report)
    }

    fn update_stats_cache(
        &self,
        physical_ids: &[i64],
        local_temporary_tables: &[tidb_model::GoShared<tidb_model::TableInfo>],
    ) -> Result<(), tidb_driver_error::DriverError> {
        self.analyze
            .update_stats_cache(
                physical_ids.to_vec(),
                local_temporary_tables,
                &tidb_domain_core::sys_session::StatsPool {
                    session_pool: self.session_pool.as_ref(),
                },
            )
            .map_err(into_driver_error)
    }
}

/// The statistics handle as the session's [`StatsStatementService`]: owned
/// handles, because the session runs the statement while the node is
/// borrowed.
///
/// [`StatsStatementService`]: crate::stats_statement_service::StatsStatementService
struct ClusterStatsStatementService {
    /// The domain whose `DumpStatsToJSON` / `DumpHistoricalStatsBySnapshot`
    /// the plan replayer reads.
    domain: std::sync::Weak<super::Domain>,
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
    fn plan_replayer_stats_json(
        &self,
        db: &str,
        table: &str,
    ) -> Result<Option<tidb_stats_util::JsonTable>, tidb_driver_error::DriverError> {
        let domain = self
            .domain
            .upgrade()
            .ok_or_else(|| tidb_driver_error::DriverError::unsupported("the node's domain is closed"))?;
        // Go `is.TableByName` on the domain's latest schema.
        let catalog = self.catalog.get_latest();
        let (_, info) = catalog.table_info_by_name(db, table).ok_or_else(|| {
            tidb_driver_error::DriverError::unsupported(format!("[schema:1146]Table '{db}.{table}' doesn't exist"))
        })?;
        let info = info.read().clone();
        drop(catalog);
        domain
            .dump_stats_to_json(db, &info, true)
            .map_err(tidb_driver_error::DriverError::unsupported)
    }

    fn plan_replayer_stats_mem(
        &self,
        db: &str,
        table: &str,
    ) -> Result<Option<(Vec<String>, Vec<String>)>, tidb_driver_error::DriverError> {
        let catalog = self.catalog.get_latest();
        let (_, info) = catalog.table_info_by_name(db, table).ok_or_else(|| {
            tidb_driver_error::DriverError::unsupported(format!("[schema:1146]Table '{db}.{table}' doesn't exist"))
        })?;
        let table_id = info.read().id;
        drop(catalog);
        // Go `GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())`.
        let Some(stats) = self.stats.get(table_id) else {
            return Ok(None);
        };
        let mut indices = Vec::new();
        stats.hist_coll.for_each_index(|_, index| {
            let name = index.info.as_ref().map(|info| info.read().name.original().to_owned()).unwrap_or_default();
            indices.push(format!("{name}={}", index.stats_loaded_status.status_to_string()));
            false
        });
        let mut columns = Vec::new();
        stats.hist_coll.for_each_column(|_, column| {
            let name = column.info.as_ref().map(|info| info.read().name.original().to_owned()).unwrap_or_default();
            columns.push(format!("{name}={}", column.stats_loaded_status.status_to_string()));
            false
        });
        Ok(Some((indices, columns)))
    }

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

    fn init_stats(
        &self,
        table_ids: &[i64],
        mode: tidb_stats_handle::bootstrap::InitialStatsLoad,
    ) -> Result<(), tidb_driver_error::DriverError> {
        // Go `h.Pool.SPool().WithForceBlockGCSession`.
        let pool = self.session_pool.as_ref().ok_or_else(|| {
            tidb_driver_error::DriverError::unsupported("this node has no statistics session pool")
        })?;
        let targets = crate::domain::real_tikv_node::schema_following::current_stats_targets(
            self.catalog.as_ref(),
        )
        .map_err(tidb_driver_error::DriverError::unsupported)?;
        let snapshot = tidb_stats_handle::bootstrap::init_stats(
            Arc::new(tidb_domain_core::sys_session::OwnedStatsPool(Arc::clone(pool))),
            &targets,
            table_ids,
            mode,
        )
        .map_err(tidb_driver_error::DriverError::unsupported)?;
        // "If tableIDs is empty, it means we load all the tables' stats meta
        // and histograms": `h.Replace(cache)`; else `h.Put` each table the
        // load found.
        if table_ids.is_empty() {
            self.stats.store(snapshot);
        } else {
            for (table_id, state) in snapshot {
                if let Some(table) = state.loaded() {
                    self.stats.publish_analyzed_table(table_id, Arc::clone(table));
                }
            }
        }
        Ok(())
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

    fn load_stats(&self, json: &tidb_stats_util::JsonTable) -> Result<(), tidb_driver_error::DriverError> {
        // Go's LOAD STATS path logs nothing on success.
        self.analyze
            .load_stats(
                json,
                &tidb_domain_core::sys_session::StatsPool {
                    session_pool: self.session_pool.as_ref(),
                },
            )
            .map_err(into_driver_error)?;
        Ok(())
    }
}
