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

use tidb_exec::cluster_analyze::{AnalyzeStatement, SampleMemoryQuota, MEM_QUOTA_ANALYZE_VARIABLE};
use tidb_exec::cluster_stats_lock::ClusterStatsLockStatement;
use tidb_executor::analyze::panic_recovery::recover_analyze_panic;
use tidb_session::privilege::GlobalPriv;

use crate::sql_node::{QuerySession, SqlQueryError, WriteOutcome};

use super::{ClusterServerSession, ER_TABLEACCESS_DENIED_ERROR};

impl ClusterServerSession {
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
        // Go's analyze memory quota is process-wide and read at execution:
        // `variable.SetMemQuotaAnalyze` drives one `GlobalAnalyzeMemoryTracker`
        // (`pkg/executor/select.go:141`), so the value in force is whatever
        // `SET GLOBAL tidb_mem_quota_analyze` last stored. Its default, `-1`,
        // is no bound.
        let memory_quota = self.analyze_memory_quota();
        self.session.begin_routed_statement_warnings();
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
            let report = recover_analyze_panic(|| {
                self.analyze.execute(
                    statement,
                    statement_memory.sql_killer(),
                    &historical_stats_enabled,
                )
            })
            .map_err(|error| SqlQueryError::unknown(error.rendered_message()))??;
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
            if let Some(warning) = &report.option_save_warning {
                self.session.append_routed_warning(1105, warning.clone());
            }
            if let Some((code, warning)) = &report.global_stats_warning {
                self.session.append_routed_warning(*code, warning.clone());
            }
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
