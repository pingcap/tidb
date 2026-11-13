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

//! `ANALYZE TABLE`: the `AdminStmt::AnalyzeTable` arm of
//! `crate::admin::AdminExec::admin_next`.
//!
//! Every named table is analyzed by the cluster analyzer (Go's
//! `AnalyzeExec` over TiKV, [`crate::real_tikv_analyze`]); this session adds
//! what Go's plan carries from the session: its options, and the LOCAL
//! temporary table the name resolves to, which the TiKV catalog never holds.
//! A temporary table's requests go to TiKV like any other's, so they read
//! none of its rows, as Go's do.

use std::collections::BTreeSet;

use crate::analyze::{AnalyzeStatement, SampleMemoryQuota, MEM_QUOTA_ANALYZE_VARIABLE};
use crate::analyze::ClusterAnalyzeReport;
use crate::{DriverError, TableEntry};

use tidb_hack::go_to_lower;

/// Go `AnalyzeExec` for the tables this session's catalog holds.
pub trait AnalyzeExec: crate::show::exec::ShowExec {
    /// Go `pdhelper.GlobalPDHelper`.
    fn approximate_table_counts(
        &self,
    ) -> std::sync::Arc<dyn tidb_pdhelper::ApproximateTableCountProvider>;
    /// Moves the statement context's warnings onto the session.
    fn drain_context_warnings(&mut self, ctx: &crate::StmtContext);

    /// Go's session `InfoSchema` (`TemporaryTableAttachedInfoSchema`)
    /// resolving the statement's name: the LOCAL temporary table it names,
    /// which the TiKV catalog never holds.
    fn local_temporary_table(
        &mut self,
        statement: &AnalyzeStatement,
    ) -> Result<Option<tidb_model::GoShared<tidb_model::TableInfo>>, DriverError> {
        self.with_catalog_mut(|catalog| {
            Ok(match catalog.table_in(&statement.schema, &statement.table) {
                Some(TableEntry::Kv(table)) if table.temp_table_type() == tidb_model::TempTableType::LOCAL => {
                    Some(table.meta())
                }
                _ => None,
            })
        })
    }

    /// Go's analyze memory quota, as one `ANALYZE` reads it.
    ///
    /// `tidb_mem_quota_analyze` is process-wide and read at execution time
    /// (`variable.SetMemQuotaAnalyze` drives one `GlobalAnalyzeMemoryTracker`,
    /// `pkg/executor/select.go:141`), so the value in force is whatever the
    /// last `SET GLOBAL` stored. Its default, `-1`, is no bound.
    fn analyze_memory_quota(&self) -> SampleMemoryQuota {
        self.session_vars()
            .get_global(MEM_QUOTA_ANALYZE_VARIABLE)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .map_or_else(
                SampleMemoryQuota::unlimited,
                SampleMemoryQuota::from_setting,
            )
    }

    /// Go `AnalyzeExec.Next`'s `FLUSH STATS_DELTA` of this session's own
    /// pending deltas before sampling.
    fn publish_table_delta(&self);
    /// Starts the statement's own warnings.
    fn begin_routed_statement_warnings(&mut self);
    /// Appends one warning of a routed statement.
    fn append_routed_warning(&mut self, code: u16, message: String);
    /// Appends one note of a routed statement.
    fn append_routed_note(&mut self, code: u16, message: String);
    /// The session's catalog, read even after a panic poisoned its lock.
    fn catalog_ignoring_poison(&self) -> std::sync::MutexGuard<'_, crate::SessionExtendedInfoSchema>;

    /// Go `AnalyzeExec.Next`: runs `ANALYZE TABLE` over `tables` through
    /// `service`, after `Compile` checked their privileges. The user's
    /// transaction is left alone, as Go's executor leaves it.
    fn run_analyze_statements(
        &mut self,
        tables: &[AnalyzeStatement],
        service: &dyn AnalyzeService,
    ) -> Result<(), DriverError> {
        // `Compile` already checked every named table (`buildAnalyze`'s visit
        // infos), so `ANALYZE TABLE ok, no` stores nothing at all.
        // Go `AnalyzeExec.Next` broadcasts `FLUSH STATS_DELTA` before
        // sampling; this session's pending deltas are published first.
        self.publish_table_delta();
        let column_task_tables = tables
            .iter()
            .filter(|statement| service.builds_column_task(statement))
            .cloned()
            .collect::<Vec<_>>();
        let target_ids = self.analyze_stats_delta_target_ids(&column_task_tables);
        if !target_ids.is_empty() {
            service.flush_stats_delta(&target_ids)?;
        }
        let memory_quota = self.analyze_memory_quota();
        self.begin_routed_statement_warnings();
        let mut successful_table_ids = BTreeSet::new();
        let mut cache_update_physical_ids: Option<Vec<i64>> = None;
        let mut local_temporary_tables = Vec::new();
        for statement in tables {
            let mut statement = self.analyze_statement_with_session_options(statement);
            statement.options.memory_quota = memory_quota;
            // A temporary table is analyzed as any other: its requests go to
            // TiKV, which holds none of its rows.
            statement.local_temporary_table = self.local_temporary_table(&statement)?;
            local_temporary_tables.extend(statement.local_temporary_table.clone());
            let statement = &statement;
            let analyzed = service.execute(statement, self.historical_stats_enabled());
            self.drain_context_warnings(&statement.eval_context);
            let report = analyzed?;
            self.append_analyze_report_warnings(statement, &report);
            successful_table_ids.extend(report.historical_stats_table_ids());
            cache_update_physical_ids
                .get_or_insert_with(Vec::new)
                .extend_from_slice(report.cache_update_physical_ids());
        }
        // Go collects `results.TableID.GetStatisticsID()` only from successful
        // results and enqueues that set after every save worker has finished.
        if self.historical_stats_enabled() {
            for table_id in successful_table_ids {
                service.dump_historical_stats(table_id);
            }
        }
        // Go ends `AnalyzeExec.Next` with `return statsHandle.Update(...)`, so
        // a failed cache update fails the statement although every table's
        // statistics are already stored. Its InfoSchema is the session's, so
        // a LOCAL temporary table is reloaded too.
        if let Some(physical_ids) = cache_update_physical_ids {
            service.update_stats_cache(&physical_ids, &local_temporary_tables)?;
        }
        Ok(())
    }

    /// Go `collectStatsDeltaFlushObjectsForAnalyze`: one table-scope object
    /// per table with a column task, whatever partitions it names, resolved
    /// as `FLUSH STATS_DELTA` resolves it (the table and every partition).
    fn analyze_stats_delta_target_ids(&self, tables: &[AnalyzeStatement]) -> Vec<i64> {
        let catalog = self.catalog_ignoring_poison();
        let mut seen = BTreeSet::new();
        let mut ids = Vec::new();
        for statement in tables {
            if !seen.insert((
                go_to_lower(&statement.schema),
                go_to_lower(&statement.table),
            )) {
                continue;
            }
            let Some(crate::TableEntry::Kv(table)) =
                catalog.table_in(&statement.schema, &statement.table)
            else {
                continue;
            };
            ids.push(table.table_id());
            if let Some(pi) = table.meta().read().get_partition_info() {
                ids.extend(pi.read().definitions.map_visible(|definition| definition.id));
            }
        }
        ids
    }

    fn historical_stats_enabled(&self) -> bool {
        self.session_vars()
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
            self.session_vars()
                .get_system(name)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
        };
        // Go `buildAnalyze`: "auto " when the session is `InRestrictedSQL`.
        statement.auto_analyze = self.session_vars().restricted_sql;
        statement.persist_options = on(tidb_vardef::tidb_vars::TIDB_PERSIST_ANALYZE_OPTIONS);
        statement.default_columns = if self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_COLUMN_OPTIONS)
            .is_ok_and(|value| value.eq_ignore_ascii_case("PREDICATE"))
        {
            crate::analyze::AnalyzeColumnChoice::Predicate
        } else {
            crate::analyze::AnalyzeColumnChoice::All
        };
        statement.skip_column_types = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES)
            .map(|value| tidb_variable::varsutil::parse_analyze_skip_column_types(&value))
            .unwrap_or_default();
        statement.dynamic_partition_prune = !self
            .session_vars()
            .get_system("tidb_partition_prune_mode")
            .is_ok_and(|value| value.eq_ignore_ascii_case("static"));
        statement.skip_missing_partition_stats =
            on(tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS);
        statement.enable_ddl_analyze_exec_opt = self.session_vars().enable_ddl_analyze_exec_opt;
        statement.analyze_snapshot = on(tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT);
        statement.enable_async_merge_global_stats =
            on(tidb_vardef::tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS);
        statement.partition_merge_concurrency = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_MERGE_PARTITION_STATS_CONCURRENCY)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1);
        statement.scan_concurrency = self
            .session_vars()
            .get_system(analyze_scan_concurrency_variable(statement.auto_analyze))
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY);
        statement.store_batch_size = self.session_vars().analyze_store_batch_size();
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
        // Go's planner raises these while it builds the plan, before any
        // execution diagnostic.
        for (_, code, warning) in report.build_diagnostics.iter().filter(|(note, ..)| !note) {
            self.append_routed_warning(*code, warning.clone());
        }
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
        // Go `builder.go:3185`/`3194`: the sample-rate notice is a NOTE.
        for (_, code, note) in report.build_diagnostics.iter().filter(|(note, ..)| *note) {
            self.append_routed_note(*code, note.clone());
        }
    }
}

/// Samples and persists the statistics an `ANALYZE` builds.
pub trait AnalyzeService {
    /// Go `AnalyzeExec.Next`'s `FLUSH STATS_DELTA` before sampling, over the
    /// physical ids the session resolved (never empty: empty would mean
    /// every table).
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), DriverError>;
    /// Whether `statement` builds a column task, the tables Go flushes
    /// deltas for before sampling (`collectStatsDeltaFlushObjectsForAnalyze`).
    fn builds_column_task(&self, statement: &AnalyzeStatement) -> bool;
    /// Runs and persists one table's analysis.
    fn execute(
        &self,
        statement: &AnalyzeStatement,
        historical_stats_enabled: bool,
    ) -> Result<ClusterAnalyzeReport, DriverError>;
    /// Go `AnalyzeExec.Next`'s closing `statsHandle.Update(ctx, infoSchema,
    /// tableAndPartitionIDs...)` over every analyzed table's physical ids;
    /// its failure is the statement's error. `local_temporary_tables` are
    /// the analyzed tables the session's `InfoSchema` holds beside the
    /// stored ones.
    fn update_stats_cache(
        &self,
        physical_ids: &[i64],
        local_temporary_tables: &[tidb_model::GoShared<tidb_model::TableInfo>],
    ) -> Result<(), DriverError>;
    /// Queues one successful table for the historical-stats dump.
    fn dump_historical_stats(&self, table_id: i64);
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
