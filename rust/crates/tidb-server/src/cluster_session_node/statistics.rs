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
use tidb_exec::cluster_analyze::{AnalyzeStatement, SampleMemoryQuota, MEM_QUOTA_ANALYZE_VARIABLE};
use tidb_exec::cluster_stats_lock::ClusterStatsLockStatement;
use tidb_exec::cluster_stats_write::StatsWritePlan;
use tidb_exec::mysql_bootstrap::utc_now_timestamp;
use tidb_exec::real_tikv_analyze::{AnalyzeJobKind, AnalyzeJobLifecycle, AnalyzeJobSpec};
use tidb_executor::analyze::panic_recovery::recover_analyze_panic;
use tidb_session::privilege::GlobalPriv;

use crate::sql_node::{QuerySession, SqlQueryError, WriteOutcome};

use super::{
    ClusterServerSession, ClusterTransactions, SharedClusterCatalog, ER_TABLEACCESS_DENIED_ERROR,
};

struct PersistedAnalyzeJobs {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
    instance: String,
    process_id: u64,
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
            .commit_optimistic_mutations(plan.mutations, read_ts, "default")
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
                .commit_optimistic_mutations(plan.mutations, read_ts, "default")
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
    ) -> Result<WriteOutcome, SqlQueryError> {
        self.rebuild_catalog_if_stale();
        let super::StatementRoute::Analyze(mut tables) = self.schema_route(sql)? else {
            return Err(SqlQueryError::unknown(
                "auto analyze generated a non-ANALYZE statement",
            ));
        };
        for statement in &mut tables {
            statement.auto_analyze = true;
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
            .is_ok_and(|value| tidb_exec::option_values::tidb_opt_on(&value));
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
        // Go checks the privileges of EVERY named table before running any
        // of them: `buildAnalyze` appends the visitInfo for each and
        // `CheckPrivilege` runs over the whole plan, so `ANALYZE TABLE ok, no`
        // stores nothing at all.
        for statement in tables {
            self.require_insert_and_select_privileges(&statement.schema, &statement.table)?;
        }
        // Go `AnalyzeExec.Next` broadcasts `FLUSH STATS_DELTA` for every
        // physical table named by the analyze plan before sampling. Persist
        // this session's pending deltas through the same statistics-handle
        // path so a static analyze still leaves the logical table's
        // realtime count available to dynamic pseudo estimation.
        self.session.publish_table_delta();
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
        let resource_group = self.session.active_resource_group().to_owned();
        super::ClusterSessionFactory::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.transactions.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            true,
            &target_ids,
            &resource_group,
        )
        .map_err(SqlQueryError::unknown)?;
        // Go's analyze memory quota is process-wide and read at execution:
        // `variable.SetMemQuotaAnalyze` drives one `GlobalAnalyzeMemoryTracker`
        // (`pkg/executor/select.go:141`), so the value in force is whatever
        // `SET GLOBAL tidb_mem_quota_analyze` last stored. Its default, `-1`,
        // is no bound.
        let memory_quota = self.analyze_memory_quota();
        self.session.begin_routed_statement_warnings();
        let mut successful_table_ids = std::collections::BTreeSet::new();
        for statement in tables {
            let mut statement = statement.clone();
            statement.persist_options = self
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_PERSIST_ANALYZE_OPTIONS)
                .is_ok_and(|value| tidb_exec::option_values::tidb_opt_on(&value));
            statement.default_columns = if self
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_COLUMN_OPTIONS)
                .is_ok_and(|value| value.eq_ignore_ascii_case("PREDICATE"))
            {
                tidb_exec::cluster_analyze::AnalyzeColumnChoice::Predicate
            } else {
                tidb_exec::cluster_analyze::AnalyzeColumnChoice::All
            };
            statement.skip_column_types = self
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES)
                .map(|value| tidb_session::varsutil::parse_analyze_skip_column_types(&value))
                .unwrap_or_default();
            statement.dynamic_partition_prune = !self
                .session
                .vars()
                .get_system("tidb_partition_prune_mode")
                .is_ok_and(|value| value.eq_ignore_ascii_case("static"));
            statement.skip_missing_partition_stats = self
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS)
                .is_ok_and(|value| tidb_exec::option_values::tidb_opt_on(&value));
            statement.enable_async_merge_global_stats = self
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS)
                .is_ok_and(|value| tidb_exec::option_values::tidb_opt_on(&value));
            statement.partition_merge_concurrency = self
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_MERGE_PARTITION_STATS_CONCURRENCY)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1);
            statement.time_zone = self.session.session_time_zone();
            statement.options.memory_quota = memory_quota;
            let statement = &statement;
            let historical_stats_enabled = || {
                self.session
                    .vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
                    .is_ok_and(|value| tidb_exec::option_values::tidb_opt_on(&value))
            };
            let jobs = PersistedAnalyzeJobs {
                transactions: Arc::clone(&self.transactions),
                catalog: Arc::clone(&self.catalog),
                instance: self.session.analyze_job_instance(),
                process_id: self.connection_id,
            };
            let analyzed = recover_analyze_panic(|| {
                self.analyze.execute(
                    statement,
                    &resource_group,
                    self.approximate_table_counts.as_ref(),
                    statement_memory.sql_killer(),
                    &historical_stats_enabled,
                    &jobs,
                )
            })
            .map_err(|error| SqlQueryError::unknown(error.rendered_message()))
            .and_then(|result| result);
            let report = analyzed?;
            if report.predicate_columns_empty {
                self.session.append_routed_warning(
                    1105,
                    format!(
                        "No predicate column has been collected yet for table {}.{}, so only indexes and the columns composing the indexes will be analyzed",
                        statement.schema.to_lowercase(),
                        statement.table.to_lowercase()
                    ),
                );
            }
            if report.ignored_partition_overrides {
                self.session.append_routed_warning(
                    1105,
                    "Ignore columns and options when analyze partition in dynamic mode".to_owned(),
                );
            }
            if report.collected_all_for_index_target {
                self.session.append_routed_warning(
                    1105,
                    "The version 2 would collect all statistics not only the selected indexes"
                        .to_owned(),
                );
            }
            if let Some(warning) = &report.option_save_warning {
                self.session.append_routed_warning(1105, warning.clone());
            }
            for (code, warning) in &report.global_stats_warnings {
                self.session.append_routed_warning(*code, warning.clone());
            }
            successful_table_ids.extend(report.historical_stats_table_ids());
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
        }
        // Go collects `results.TableID.GetStatisticsID()` only from successful
        // results and enqueues that set after every save worker has finished.
        if self
            .session
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .is_ok_and(|value| tidb_exec::option_values::tidb_opt_on(&value))
        {
            for table_id in successful_table_ids {
                self.historical_stats_worker
                    .send_tbl_to_dump_historical_stats(table_id);
            }
        }
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
        for required in [GlobalPriv::Insert, GlobalPriv::Select] {
            if self.session.has_table_privilege(schema, table, required) {
                continue;
            }
            let (user, host) = self.session.authenticated_identity().unwrap_or(("", ""));
            return Err(SqlQueryError::new(
                ER_TABLEACCESS_DENIED_ERROR,
                *b"42000",
                format!(
                    "{} command denied to user '{user}'@'{host}' for table '{}'",
                    required.print_name(),
                    table
                ),
            ));
        }
        Ok(())
    }

    /// `tidb_mem_quota_analyze` as this node currently holds it.
    ///
    /// A variable that is missing or unreadable is Go's default: no bound. It
    /// is not a reason to refuse an `ANALYZE`, since Go runs every one of them
    /// unbounded by default anyway.
    fn analyze_memory_quota(&self) -> SampleMemoryQuota {
        self.session
            .vars()
            .get_global(MEM_QUOTA_ANALYZE_VARIABLE)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .map_or_else(
                SampleMemoryQuota::unlimited,
                SampleMemoryQuota::from_setting,
            )
    }
}
