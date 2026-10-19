// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `ANALYZE TABLE` behind Milestone 3's session seam: the session owns what
//! Go's planner and `AnalyzeExec` decide (privileges, the statement's
//! options from session variables, temporary tables, the warnings), and an
//! [`AnalyzeService`] the caller supplies samples and persists.

use std::collections::BTreeSet;
use std::sync::Arc;

use tidb_exec::cluster_analyze::AnalyzeStatement;
use tidb_exec::real_tikv_analyze::ClusterAnalyzeReport;
use tidb_util::stringutil::go_to_lower;

use crate::privilege::GlobalPriv;
use crate::*;

/// Samples and persists the statistics an `ANALYZE` builds.
pub trait AnalyzeService {
    /// Go `AnalyzeExec.Next`'s `FLUSH STATS_DELTA` for every physical table
    /// the statement names, before sampling.
    fn flush_stats_delta(&self, tables: &[AnalyzeStatement]) -> Result<(), DriverError>;
    /// Runs and persists one table's analysis.
    fn execute(
        &self,
        statement: &AnalyzeStatement,
        historical_stats_enabled: bool,
    ) -> Result<ClusterAnalyzeReport, DriverError>;
    /// Publishes a temporary table's statistics, which the session analyzed
    /// itself (its rows live in the session's overlay).
    fn publish_temporary_table_stats(&self, table_id: i64, statistics: Arc<tidb_stats::Table>);
    /// Queues one successful table for the historical-stats dump.
    fn dump_historical_stats(&self, table_id: i64);
}

impl Session {
    /// Runs `ANALYZE TABLE` over `tables` through `service`. The caller has
    /// committed any open transaction first, as Go's implicit commit does.
    pub fn run_analyze_statements(
        &mut self,
        tables: &[AnalyzeStatement],
        service: &dyn AnalyzeService,
    ) -> Result<(), DriverError> {
        // Go checks the privileges of EVERY named table before running any
        // of them: `buildAnalyze` appends the visitInfo for each and
        // `CheckPrivilege` runs over the whole plan, so `ANALYZE TABLE ok, no`
        // stores nothing at all.
        for statement in tables {
            self.require_insert_and_select_privileges(&statement.schema, &statement.table)?;
        }
        // Go `AnalyzeExec.Next` broadcasts `FLUSH STATS_DELTA` before
        // sampling; this session's pending deltas are published first.
        self.publish_table_delta();
        service.flush_stats_delta(tables)?;
        let memory_quota = self.analyze_memory_quota();
        self.begin_routed_statement_warnings();
        let mut successful_table_ids = BTreeSet::new();
        for statement in tables {
            let mut statement = self.analyze_statement_with_session_options(statement);
            statement.options.memory_quota = memory_quota;
            let statement = &statement;
            // Go resolves and samples temporary tables through the session's
            // temporary-table overlay, which the shared cluster analyzer
            // cannot see; the session analyzer runs them.
            if let Some((table_id, statistics)) = self.analyze_temporary_table(statement)? {
                service.publish_temporary_table_stats(table_id, statistics);
                continue;
            }
            let analyzed = service.execute(statement, self.historical_stats_enabled());
            self.drain_context_warnings(&statement.eval_context);
            let report = analyzed?;
            self.append_analyze_report_warnings(statement, &report);
            successful_table_ids.extend(report.historical_stats_table_ids());
        }
        // Go collects `results.TableID.GetStatisticsID()` only from successful
        // results and enqueues that set after every save worker has finished.
        if self.historical_stats_enabled() {
            for table_id in successful_table_ids {
                service.dump_historical_stats(table_id);
            }
        }
        Ok(())
    }

    /// Go's privilege gate on `ANALYZE TABLE` and the statistics statements:
    /// INSERT *and* SELECT on the table, INSERT first
    /// (`requireInsertAndSelectPriv`), each with its own 1142.
    pub fn require_insert_and_select_privileges(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<(), DriverError> {
        for required in [GlobalPriv::Insert, GlobalPriv::Select] {
            if self.has_table_privilege(schema, table, required) {
                continue;
            }
            let (user, host) = self.authenticated_identity().unwrap_or(("", ""));
            return Err(DriverError::Mysql(tidb_executor::MysqlError::new(
                1142,
                format!(
                    "{} command denied to user '{user}'@'{host}' for table '{}'",
                    required.print_name(),
                    table
                ),
            )));
        }
        Ok(())
    }

    fn historical_stats_enabled(&self) -> bool {
        self.vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

    /// The statement with the options Go reads from session variables when
    /// it builds the analyze plan.
    fn analyze_statement_with_session_options(
        &self,
        statement: &AnalyzeStatement,
    ) -> AnalyzeStatement {
        let mut statement = statement.clone();
        let on = |name: &str| {
            self.vars()
                .get_system(name)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
        };
        statement.persist_options = on(tidb_vardef::tidb_vars::TIDB_PERSIST_ANALYZE_OPTIONS);
        statement.default_columns = if self
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_COLUMN_OPTIONS)
            .is_ok_and(|value| value.eq_ignore_ascii_case("PREDICATE"))
        {
            tidb_exec::cluster_analyze::AnalyzeColumnChoice::Predicate
        } else {
            tidb_exec::cluster_analyze::AnalyzeColumnChoice::All
        };
        statement.skip_column_types = self
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES)
            .map(|value| crate::varsutil::parse_analyze_skip_column_types(&value))
            .unwrap_or_default();
        statement.dynamic_partition_prune = !self
            .vars()
            .get_system("tidb_partition_prune_mode")
            .is_ok_and(|value| value.eq_ignore_ascii_case("static"));
        statement.skip_missing_partition_stats =
            on(tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS);
        statement.analyze_snapshot = on(tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT);
        statement.enable_async_merge_global_stats =
            on(tidb_vardef::tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS);
        statement.partition_merge_concurrency = self
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_MERGE_PARTITION_STATS_CONCURRENCY)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1);
        statement.scan_concurrency = self
            .vars()
            .get_system(analyze_scan_concurrency_variable(statement.auto_analyze))
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY);
        statement.eval_context = self.analyze_statement_context().into();
        statement
    }

    /// The warnings and notes Go's planner and analyze executor raise for
    /// one finished table, in Go's order.
    fn append_analyze_report_warnings(
        &mut self,
        statement: &AnalyzeStatement,
        report: &ClusterAnalyzeReport,
    ) {
        if report.predicate_columns_empty {
            self.append_routed_warning(
                1105,
                format!(
                    "No predicate column has been collected yet for table {}.{}, so only indexes and the columns composing the indexes will be analyzed",
                    go_to_lower(&statement.schema),
                    go_to_lower(&statement.table)
                ),
            );
        }
        if report.ignored_partition_overrides {
            self.append_routed_warning(
                1105,
                "Ignore columns and options when analyze partition in dynamic mode".to_owned(),
            );
        }
        if report.collected_all_for_index_target {
            self.append_routed_warning(
                1105,
                "The version 2 would collect all statistics not only the selected indexes"
                    .to_owned(),
            );
        }
        for index in &report.skipped_columnar_indexes {
            self.append_routed_warning(
                1105,
                format!("analyzing columnar index is not supported, skip {index}"),
            );
        }
        if let Some(warning) = &report.option_save_warning {
            self.append_routed_warning(1105, warning.clone());
        }
        for (code, warning) in &report.global_stats_warnings {
            self.append_routed_warning(*code, warning.clone());
        }
        // Go `builder.go:3194`: the sample-rate notice is a NOTE.
        for (code, note) in &report.sample_rate_notes {
            self.append_routed_note(*code, note.clone());
        }
    }
}

/// The scan-concurrency variable an analyze reads: an auto-analyze uses the
/// system-process one.
#[must_use]
pub fn analyze_scan_concurrency_variable(auto_analyze: bool) -> &'static str {
    if auto_analyze {
        tidb_vardef::tidb_vars::TIDB_SYS_PROC_SCAN_CONCURRENCY
    } else {
        tidb_vardef::tidb_vars::TIDB_ANALYZE_DIST_SQL_SCAN_CONCURRENCY
    }
}
