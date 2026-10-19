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
use tidb_hack::GoToLower;

use super::{ClusterServerSession, ClusterTransactions, SharedClusterCatalog};

struct PersistedAnalyzeJobs {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
    instance: String,
    process_id: u64,
    global_vars: tidb_session::GlobalSysvars,
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
            let mut snapshot = super::SnapshotMetaSnapshot::new(snapshot);
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
                eprintln!("{{\"event\":\"insert_analyze_job_failed\",\"error\":{error:?}}}");
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
            eprintln!("{{\"event\":\"start_analyze_job_failed\",\"error\":{error:?}}}");
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
            eprintln!("{{\"event\":\"update_analyze_job_failed\",\"error\":{error:?}}}");
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
            eprintln!("{{\"event\":\"finish_analyze_job_failed\",\"error\":{error:?}}}");
        }
    }
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
    pub(super) fn run_load_stats(&mut self, data: &[u8]) -> Result<WriteOutcome, SqlQueryError> {
        if data.is_empty() {
            return Ok(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            });
        }
        let json = tidb_executor::load_stats::parse_stats_json(
            std::str::from_utf8(data).map_err(|error| SqlQueryError::unknown(error.to_string()))?,
        )
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        // Go `LoadStatsInfo.Update`: JSON `null` is a successful no-op.
        if json.table_name.is_empty() && json.version == 0 {
            return Ok(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            });
        }
        let historical_stats_enabled = self
            .session
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value));
        let report = self.analyze.load_stats(&json, historical_stats_enabled)?;
        eprintln!(
            "{{\"event\":\"cluster_stats_loaded\",\"tables\":{},\"items\":{}}}",
            report.table_count, report.item_count
        );
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// Go `SimpleExec.executeDropStats`' id resolution: the physical ids
    /// whose statistics the statement drops. `Ok(None)` is a statement that
    /// is not DROP STATS.
    ///
    /// Without a partition clause every named table drops its partitions and
    /// itself (a non-partitioned table's own id twice, as Go's loop yields
    /// it); `GLOBAL` drops the first table's own id; a partition list drops
    /// the first table's named partitions.
    pub(super) fn drop_stats_ids(
        &mut self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<Option<Vec<i64>>, SqlQueryError> {
        let tidb_ast::Stmt::Admin(admin) = stmt else {
            return Ok(None);
        };
        let tidb_ast::AdminStmt::DropStats(drop) = admin.as_ref() else {
            return Ok(None);
        };
        let current_database = self.session.current_database().to_owned();
        let catalog = self.catalog.load();
        let mut tables = Vec::with_capacity(drop.tables.len());
        for path in &drop.tables {
            let (schema, name) = match path.as_slice() {
                [name] => (current_database.clone(), name.clone()),
                [schema, name] => (schema.clone(), name.clone()),
                _ => {
                    return Err(SqlQueryError::unknown(
                        "DROP STATS of a malformed table name",
                    ))
                }
            };
            if schema.is_empty() {
                return Err(super::map_error(tidb_executor::DriverError::Schema(
                    tidb_executor::SchemaErrorKind::NoDatabaseSelected,
                )));
            }
            let Some((_, table)) = catalog.find_table(&schema, &name) else {
                return Err(super::map_error(tidb_executor::DriverError::Schema(
                    tidb_executor::SchemaErrorKind::UnknownTable(format!("{schema}.{name}")),
                )));
            };
            tables.push(table);
        }
        let partition_ids = |table: &tidb_model::table_info::TableInfo,
                             names: &[String]|
         -> Result<Vec<i64>, SqlQueryError> {
            let Some(partition) = table.get_partition_info() else {
                if !names.is_empty() {
                    return Err(super::map_error(
                        tidb_executor::DriverError::PartitionManagementOnNonpartitioned,
                    ));
                }
                return Ok(vec![table.id]);
            };
            let definitions = partition.read().definitions.snapshot();
            if names.is_empty() {
                return Ok(definitions.iter().map(|definition| definition.id).collect());
            }
            names
                .iter()
                .map(|name| {
                    definitions
                        .iter()
                        .find(|definition| definition.name.lowercase() == name.go_to_lower())
                        .map(|definition| definition.id)
                        .ok_or_else(|| {
                            SqlQueryError::unknown(format!(
                                "can not found the specified partition name {name} in the table definition"
                            ))
                        })
                })
                .collect()
        };
        let mut ids = Vec::new();
        if drop.global {
            ids.push(tables[0].id);
        } else if drop.partitions.is_empty() {
            for table in &tables {
                ids.extend(partition_ids(table, &[])?);
                ids.push(table.id);
            }
        } else {
            ids = partition_ids(tables[0], &drop.partitions)?;
        }
        // The soft delete is idempotent per id; plan each once.
        let mut seen = std::collections::BTreeSet::new();
        ids.retain(|id| seen.insert(*id));
        Ok(Some(ids))
    }

    /// Runs Go's `executeDropStats` write: the statistics handle commits its
    /// own transaction, so the user's is not moved.
    pub(super) fn run_drop_stats(
        &mut self,
        physical_ids: &[i64],
    ) -> Result<WriteOutcome, SqlQueryError> {
        self.analyze.drop_stats(physical_ids)?;
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// Runs one persisted statistics-lock operation in the internal
    /// transaction owned by the statistics handle.
    pub(super) fn run_stats_lock(
        &mut self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<WriteOutcome, SqlQueryError> {
        // Unlike DDL and ANALYZE, Go does not move the user's transaction:
        // the stats handle borrows a separate restricted session and commits
        // only its `mysql.stats_*` changes.
        self.session.begin_routed_statement_warnings();
        for target in &statement.targets {
            self.require_insert_and_select_privileges(&target.schema, &target.table)?;
        }
        let report = self.stats_lock.execute(statement)?;
        if !report.warning.is_empty() {
            self.session.append_routed_warning(1105, report.warning);
        }
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
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
        if self.explicit.is_some() || self.session.in_transaction() {
            self.control_transaction("COMMIT")?;
        }
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

    /// Go's privilege gate on `ANALYZE TABLE`: INSERT *and* SELECT on the
    /// table.
    ///
    /// `pkg/planner/core/planbuilder.go:3205` calls
    /// `requireInsertAndSelectPriv(as.TableNames)`, which appends
    /// `mysql.InsertPriv` and then `mysql.SelectPriv` for each table, each
    /// carrying its own `ErrTableaccessDenied`. INSERT is appended first, so
    /// an account holding neither is told about INSERT -- captured from a
    /// real TiDB, for a user with no privileges and for a SELECT-only user
    /// alike:
    ///
    /// ```text
    /// ERROR 1142 (42000): INSERT command denied to user 'zzlow'@'%' for table 'zzt'
    /// ```
    ///
    /// This is not a formality on a read: the TopN entries an `ANALYZE`
    /// writes into `mysql.stats_top_n` are ACTUAL COLUMN VALUES, readable by
    /// anyone who can read the statistics.
    fn require_insert_and_select_privileges(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<(), SqlQueryError> {
        self.session
            .require_insert_and_select_privileges(schema, table)
            .map_err(super::map_error)
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
}

impl tidb_session::analyze_service::AnalyzeService for ClusterAnalyzeService {
    fn flush_stats_delta(
        &self,
        tables: &[AnalyzeStatement],
    ) -> Result<(), tidb_executor::DriverError> {
        let catalog = self.catalog.load();
        let mut target_ids = Vec::new();
        for statement in tables {
            let Some((_, table)) = catalog.find_table(&statement.schema, &statement.table) else {
                continue;
            };
            match &table.partition {
                Some(partition) => {
                    target_ids.extend(
                        partition
                            .read()
                            .definitions
                            .snapshot()
                            .into_iter()
                            .filter(|definition| {
                                statement.partitions.is_empty()
                                    || statement.partitions.iter().any(|name| {
                                        name.eq_ignore_ascii_case(definition.name.original())
                                    })
                            })
                            .map(|definition| definition.id),
                    );
                }
                None => target_ids.push(table.id),
            }
        }
        drop(catalog);
        super::ClusterSessionFactory::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.transactions.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            true,
            &target_ids,
            &self.resource_group,
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

    fn publish_temporary_table_stats(&self, table_id: i64, statistics: Arc<tidb_stats::Table>) {
        let mut published = (*self.stats.load()).clone();
        published.insert(
            table_id,
            tidb_exec::stats_watch::TableStatsState::Loaded(statistics),
        );
        self.stats.store_after_analyze(published);
    }

    fn dump_historical_stats(&self, table_id: i64) {
        self.historical_stats_worker
            .send_tbl_to_dump_historical_stats(table_id);
    }
}
