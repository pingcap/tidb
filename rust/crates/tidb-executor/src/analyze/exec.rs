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
    /// Go `broadcast` over the session's `kv.TiDB` client.
    fn broadcast(&mut self, sql: &str) -> Result<(), DriverError>;
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
        self.flush_stats_delta_for_analyze(&column_task_tables, service)?;
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
    /// Go `flushStatsDeltaForAnalyze`: flushes pending stats deltas for the
    /// tables whose column-analyze tasks will capture base count /
    /// modify_count from `mysql.stats_meta`. Without this, a stale
    /// pre-analyze delta can be applied later and double count rows or
    /// modifications.
    fn flush_stats_delta_for_analyze(
        &mut self,
        column_task_tables: &[AnalyzeStatement],
        service: &dyn AnalyzeService,
    ) -> Result<(), DriverError> {
        let flush_objects = collect_stats_delta_flush_objects_for_analyze(
            column_task_tables
                .iter()
                .map(|task| (task.schema.as_str(), task.table.as_str())),
        );
        if flush_objects.is_empty() {
            return Ok(());
        }
        // HACK: test binaries have no real RPC peer to broadcast to; dump
        // locally instead.
        if tidb_intest::IN_TEST
            && self.flush_analyze_stats_delta_for_test(column_task_tables, service)?
        {
            return Ok(());
        }
        let stmt = tidb_ast::Stmt::Admin(tidb_ast::NodeBox::new(tidb_ast::AdminStmt::Flush(
            Box::new(tidb_ast::FlushStmt {
                no_write_to_binlog: false,
                target: tidb_ast::FlushStmtType::StatsDelta {
                    objects: flush_objects,
                    cluster: true,
                },
            }),
        )));
        self.try_broadcast(&stmt.restore())
    }

    /// Go `tryBroadcast`: runs `FLUSH STATS_DELTA ... CLUSTER` to flush
    /// every TiDB's pending deltas before analyze. A peer on an older release
    /// that cannot decode the BroadcastQuery executor rejects it with "this
    /// exec type <n> doesn't support yet"; the pre-flush is best-effort, so
    /// that is a warning and the analyze proceeds. Other errors propagate.
    fn try_broadcast(&mut self, sql: &str) -> Result<(), DriverError> {
        let Err(error) = self.broadcast(sql) else {
            return Ok(());
        };
        if !is_unsupported_broadcast_query_err(&error) {
            return Err(error);
        }
        tidb_stats_handle_logutil::stats_logger().warn(
            "FLUSH STATS_DELTA CLUSTER broadcast rejected by a peer TiDB during analyze; \
             proceeding without the cluster-wide pre-analyze flush",
            &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
        );
        Ok(())
    }

    /// Go `flushAnalyzeStatsDeltaForTest`: when every registered TiDB has a
    /// reachable RPC endpoint the normal broadcast path runs, so RPC-backed
    /// tests still exercise the production behavior; otherwise this
    /// instance dumps the tables' deltas itself.
    fn flush_analyze_stats_delta_for_test(
        &mut self,
        column_task_tables: &[AnalyzeStatement],
        service: &dyn AnalyzeService,
    ) -> Result<bool, DriverError> {
        if self.can_broadcast_analyze_stats_delta_for_test() {
            return Ok(false);
        }
        let target_ids = self.analyze_stats_delta_target_ids(column_task_tables);
        if target_ids.is_empty() {
            return Ok(false);
        }
        service.flush_stats_delta(&target_ids)?;
        Ok(true)
    }

    /// Go `canBroadcastAnalyzeStatsDeltaForTest`: every listed TiDB but a
    /// placeholder must answer a TCP dial at its status address.
    fn can_broadcast_analyze_stats_delta_for_test(&self) -> bool {
        let Some(syncer) = self.viewer_state().server_info_syncer else {
            return false;
        };
        let Ok(servers) = syncer.get_all_server_info() else {
            return false;
        };
        let rpc_addrs: Vec<String> = servers
            .values()
            // Go `config.UnavailableIP`, which `buildTiDBMemCopTasks` skips too.
            .filter(|server| server.static_info.ip != "<nil>")
            .map(|server| {
                // In-process test domains can register server info without
                // starting a TiDB RPC listener.
                if server.static_info.ip.is_empty() {
                    String::new()
                } else {
                    tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
                        &server.static_info.ip,
                        server.static_info.status_port,
                    )
                }
            })
            .collect();
        can_broadcast_to_tidb_rpc_for_test(&rpc_addrs)
    }

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

/// Go `collectStatsDeltaFlushObjectsForAnalyze`: the database-qualified
/// tables whose stats deltas must be flushed before building column analyze
/// tasks, each once. A column task is its `(DBName, TableName)`.
fn collect_stats_delta_flush_objects_for_analyze<'a>(
    col_tasks: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<tidb_ast::StatsObject> {
    let mut seen_objects = BTreeSet::new();
    let mut flush_objects = Vec::new();
    for (db_name, table_name) in col_tasks {
        // Go: "analyze column task must have database-qualified table name".
        if db_name.is_empty() || table_name.is_empty() {
            continue;
        }
        if !seen_objects.insert((db_name, table_name)) {
            continue;
        }
        flush_objects.push(tidb_ast::StatsObject::Table {
            database: Some(db_name.to_owned()),
            table: table_name.to_owned(),
        });
    }
    flush_objects
}

/// Go `isUnsupportedBroadcastQueryErr`: a peer rejecting the BroadcastQuery
/// coprocessor executor with "this exec type <n> doesn't support yet".
fn is_unsupported_broadcast_query_err(error: &DriverError) -> bool {
    let msg = error.to_string();
    msg.contains("exec type") && msg.contains("doesn't support yet")
}

/// Go `canBroadcastToTiDBRPCForTest`.
fn can_broadcast_to_tidb_rpc_for_test(rpc_addrs: &[String]) -> bool {
    !rpc_addrs.is_empty() && rpc_addrs.iter().all(|addr| is_tidb_rpc_reachable_for_test(addr))
}

/// Go `isTiDBRPCReachableForTest`: a 50 ms TCP dial.
fn is_tidb_rpc_reachable_for_test(addr: &str) -> bool {
    use std::net::ToSocketAddrs;
    if addr.is_empty() {
        return false;
    }
    let Ok(addrs) = addr.to_socket_addrs() else {
        return false;
    };
    addrs.into_iter().any(|addr| {
        std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(50)).is_ok()
    })
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

/// Go `pkg/executor/analyze_utils_test.go`.
#[cfg(test)]
mod analyze_utils_test {
    use super::*;

    /// Go `TestIsUnsupportedBroadcastQueryErr`.
    #[test]
    fn test_is_unsupported_broadcast_query_err() {
        let err = |message: &str| DriverError::Mysql(crate::MysqlError::unknown(message));
        // An old peer rejects the BroadcastQuery executor during a rolling
        // upgrade; the message is produced by PBPlanBuilder.pbToPhysicalPlan.
        assert!(is_unsupported_broadcast_query_err(&err(
            "other error: this exec type 17 doesn't support yet"
        )));
        assert!(is_unsupported_broadcast_query_err(&err(
            "this exec type 17 doesn't support yet"
        )));

        // Unrelated errors must still propagate and fail analyze.
        assert!(!is_unsupported_broadcast_query_err(&err("context canceled")));
        assert!(!is_unsupported_broadcast_query_err(&err("region unavailable")));
    }

    /// Go `TestCollectStatsDeltaFlushObjectsForAnalyzeDottedNames`.
    #[test]
    fn test_collect_stats_delta_flush_objects_for_analyze_dotted_names() {
        let flush_objects = collect_stats_delta_flush_objects_for_analyze([
            // Quoted identifiers may contain dots. These first two targets
            // both stringify to "a.b.c" if db and table names are joined
            // with ".".
            ("a.b", "c"),
            ("a", "b.c"),
            // Keep the duplicate target deduped.
            ("a", "b.c"),
        ]);
        let targets: Vec<(String, String)> = flush_objects
            .into_iter()
            .map(|object| match object {
                tidb_ast::StatsObject::Table { database, table } => (database.unwrap(), table),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            targets,
            [("a.b".to_owned(), "c".to_owned()), ("a".to_owned(), "b.c".to_owned())]
        );
    }

    /// Go `TestCanBroadcastToTiDBRPCForTestRejectsInvalidEndpoints`.
    #[test]
    fn test_can_broadcast_to_tidb_rpc_for_test_rejects_invalid_endpoints() {
        // In-process domains can register multiple server infos with an
        // empty IP but no TiDB RPC listener. Such targets must not take the
        // broadcast path.
        assert!(!can_broadcast_to_tidb_rpc_for_test(&[String::new(), String::new()]));
    }
}
