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

//! Go's `memTableRetriever`s for the `information_schema` tables that read
//! instance-wide state through the viewing session's identity: the process
//! list, the transaction and statement summaries, the client-error summaries,
//! the memory-usage tables and the index-usage table.
//!
//! The executor owns these retrievers (a scan calls `retrieve` at its first
//! `Next`), so what they need of the viewing session is a [`ViewerState`]
//! the session snapshots when a scan first asks for one of them.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_executor::mem_table_retrieval::{MemTableRead, MemTableRetriever};
use tidb_executor::{Catalog, DriverError};
use tidb_hack::GoCaseCompare;

use crate::infoschema::SchemaVisibility;
use crate::process;

/// What the instance-wide tables read of the session viewing them.
#[derive(Clone)]
pub(crate) struct ViewerState {
    /// Go's `hasPriv(sctx, mysql.ProcessPriv)`, which `PROCESS` privileged
    /// viewers and internal sessions pass.
    pub(crate) has_process: bool,
    /// The instance's connection registry, absent for a session no server
    /// front end owns.
    pub(crate) registry: Option<process::ProcessRegistry>,
    /// This connection's id, user name, current schema, status text and
    /// resource group, which stand for it when there is no registry.
    pub(crate) connection_id: u64,
    /// The id the session's process-registry guard registered, absent when no
    /// server front end owns the session.
    pub(crate) own_process_id: Option<u64>,
    pub(crate) process_user: String,
    pub(crate) current_db: String,
    pub(crate) status: String,
    pub(crate) resource_group: String,
    /// The login (`user@host`) and the authenticated account.
    pub(crate) login_user: Option<String>,
    pub(crate) auth_identity: Option<(String, String)>,
    /// `@@tidb_distsql_scan_concurrency`, the statement-summary history
    /// reader's worker count.
    pub(crate) scan_concurrency: usize,
    pub(crate) visibility: SchemaVisibility,
    pub(crate) index_usage: Arc<tidb_stats_handle_usage_indexusage::Collector>,
    /// Go `RequestVerification("", "", "", mysql.ProcessPriv)`, which the
    /// metric-backed tables ask before they look at their extractor.
    pub(crate) process_scoped: bool,
    /// Go `GetRestrictedSQLExecutor`, which the inspection retrievers query
    /// the metrics tables through.
    pub(crate) restricted_sql: crate::restricted_sql::RestrictedSql,
    /// `@@tidb_slow_query_file`.
    pub(crate) slow_query_file: String,
    /// The DDL job store a node installs (Go reads the store's meta).
    pub(crate) ddl_job_history_store: Option<Arc<dyn crate::DdlJobHistoryStore>>,
    /// The analyze-job reader, absent for a session with no statistics.
    pub(crate) analyze_status: Option<Arc<dyn crate::AnalyzeStatusProvider>>,
    /// Go's SQL-digest text retriever over this node and its peers.
    pub(crate) digest_retriever: crate::process_arm::SqlDigestTextRetriever,
    pub(crate) data_lock_waits: Option<Arc<dyn crate::DataLockWaitsProvider>>,
    /// The cluster's discovery and diagnostics clients; absent for a session
    /// no server front end owns, which reads these tables as empty.
    pub(crate) cluster_topology: Option<Arc<tidb_domain::cluster_topology::ClusterTopology>>,
    pub(crate) cluster_config: Option<Arc<tidb_exec::cluster_config::ClusterConfigClient>>,
    pub(crate) cluster_diagnostics:
        Option<Arc<tidb_exec::cluster_diagnostics::ClusterDiagnosticsClient>>,
    pub(crate) cluster_peer: Option<Arc<tidb_exec::cluster_peer::ClusterPeerClient>>,
    pub(crate) server_info_syncer: Option<Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    /// Go `RequestVerification("", "", "", mysql.ConfigPriv)`.
    pub(crate) config_scoped: bool,
    /// Whether SEM hides the cluster metadata from the viewer.
    pub(crate) hides_cluster_metadata: bool,
}

impl ViewerState {
    /// Go `setDataForProcessList`: "If you have the PROCESS privilege, you
    /// can see all threads. Otherwise, you can see only your own threads";
    /// an internal session with no login user is not filtered at all.
    pub(crate) fn filter_process_rows(
        &self,
        rows: Vec<process::ProcessRow>,
    ) -> Vec<process::ProcessRow> {
        if self.has_process {
            return rows;
        }
        rows.into_iter()
            .filter(|row| row.user == self.process_user)
            .collect()
    }

    /// Every connection this session is allowed to see for `SHOW
    /// PROCESSLIST` / `information_schema.PROCESSLIST`.
    pub(crate) fn visible_process_rows(&self, full: bool) -> Vec<process::ProcessRow> {
        let rows: Vec<process::ProcessRow> = match &self.registry {
            Some(registry) => registry.snapshot(),
            None => vec![process::ProcessRow {
                id: self.connection_id,
                user: self.process_user.clone(),
                host: String::new(),
                db: self.current_db.clone(),
                command: "Query".to_owned(),
                time: 0,
                state: self.status.clone(),
                info: Some(if full {
                    "show full processlist".to_owned()
                } else {
                    "show processlist".to_owned()
                }),
                resource_group: self.resource_group.clone(),
                ..process::ProcessRow::default()
            }],
        };
        self.filter_process_rows(rows)
    }

    /// Go `setDataForTrxSummary`: the transaction history, for a viewer that
    /// holds PROCESS.
    pub(crate) fn transaction_summary_rows(
        &self,
        registry: Option<&process::ProcessRegistry>,
    ) -> Vec<Vec<Datum>> {
        if !self.has_process {
            return Vec::new();
        }
        registry.or(self.registry.as_ref()).map_or_else(
            || tidb_exec::txn_summary::RECORDER.rows(),
            |registry| registry.transaction_history_rows(),
        )
    }

    /// Go `slowQueryRetriever.retrieve`: SLOW_QUERY's rows from this
    /// instance's slow-log file. The viewer sees every user's entries with
    /// `PROCESS`, else only its own; the scans' extractors bound the entries'
    /// time (a statement scanning the table more than once reads the union
    /// of their ranges). Returns the rows and the warnings the read raised.
    pub(crate) fn slow_query_rows(
        &self,
        extractors: &[&tidb_planner::memtable_predicate_extractor::SlowQueryExtractor],
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<(Vec<Vec<Datum>>, Vec<String>), DriverError> {
        if !extractors.is_empty() && extractors.iter().all(|extractor| extractor.skip_request) {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut checker = tidb_executor::slow_query::SlowLogChecker {
            has_process_priv: self.has_process,
            user: self
                .login_user
                .as_deref()
                .map(|identity| identity.split_once('@').map_or(identity, |(user, _)| user))
                .map(str::to_owned),
            ..Default::default()
        };
        let live: Vec<_> = extractors
            .iter()
            .filter(|extractor| !extractor.skip_request)
            .collect();
        if !live.is_empty() && live.iter().all(|extractor| extractor.enable) {
            checker.enable_time_check = true;
            checker.time_ranges = live
                .iter()
                .flat_map(|extractor| extractor.time_ranges.iter())
                .map(|range| (range.start_time, range.end_time))
                .collect();
        }
        let columns =
            crate::infoschema::table_schema("SLOW_QUERY").expect("SLOW_QUERY is registered");
        let read = tidb_executor::slow_query::retrieve_slow_query_rows(
            &columns,
            &checker,
            zone,
            &self.slow_query_file,
        )
        .map_err(|error| DriverError::Mysql(tidb_executor::MysqlError::unknown(error)))?;
        Ok((read.rows, read.warnings))
    }

    /// Go `DDLJobRetriever.initial`: the running jobs, then the meta history
    /// newest first, read from the session's store.
    fn all_ddl_jobs(
        &self,
    ) -> Result<(Vec<tidb_model::Job>, Vec<tidb_model::Job>), DriverError> {
        self.ddl_job_history_store
            .as_ref()
            .ok_or_else(|| DriverError::unsupported("this session has no store to read DDL jobs from"))?
            .all_ddl_jobs()
            .map_err(|message| DriverError::unsupported(message))
    }

    /// Go `stmtSummaryRetriever`: the statement summary tables for this
    /// viewer, or every user's rows for an `internal` reader.
    pub(crate) fn statement_summary_rows(
        &self,
        table_name: &str,
        columns: &[(String, tidb_datatype::FieldType)],
        zone: &tidb_datatype::SessionTimeZone,
        internal: bool,
        digests: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_parser::auth::UserIdentity;
        let has_process = internal || self.has_process;
        let user = self
            .login_user
            .as_deref()
            .filter(|_| !internal)
            .map(|login| {
                let (username, hostname) = login.split_once('@').unwrap_or((login, ""));
                let (auth_username, auth_hostname) = self
                    .auth_identity
                    .as_ref()
                    .map_or(("", ""), |(user, host)| (user.as_str(), host.as_str()));
                UserIdentity {
                    username: username.to_owned(),
                    hostname: hostname.to_owned(),
                    current_user: false,
                    auth_username: auth_username.to_owned(),
                    auth_hostname: auth_hostname.to_owned(),
                    auth_plugin: String::new(),
                }
            });
        crate::dispatch::statement_summary_rows(
            table_name,
            columns,
            zone,
            user,
            has_process,
            digests,
            self.scan_concurrency,
        )
    }
}

/// The retriever for the tables [`ViewerState`] answers.
pub(crate) struct InstanceStateRetriever {
    viewer: ViewerState,
}

impl InstanceStateRetriever {
    pub(crate) fn new(viewer: ViewerState) -> Self {
        Self { viewer }
    }

    fn is_statement_summary(table: &str) -> bool {
        [
            "TIDB_STATEMENTS_STATS",
            "STATEMENTS_SUMMARY",
            "STATEMENTS_SUMMARY_HISTORY",
            "STATEMENTS_SUMMARY_EVICTED",
        ]
        .iter()
        .any(|name| table.go_lower_eq(name))
    }

    fn is_client_errors_summary(table: &str) -> bool {
        [
            "CLIENT_ERRORS_SUMMARY_GLOBAL",
            "CLIENT_ERRORS_SUMMARY_BY_USER",
            "CLIENT_ERRORS_SUMMARY_BY_HOST",
        ]
        .iter()
        .any(|name| table.go_lower_eq(name))
    }
}

impl MemTableRetriever for InstanceStateRetriever {
    fn serves(&self, read: &MemTableRead) -> bool {
        // The cluster tables (`CLUSTER_PROCESSLIST`, ...) fan out to peers and
        // are the session's.
        if read.is_cluster_scan
            || !read
                .db_name
                .go_lower_eq(tidb_executor::driver::infoschema_meta::INFORMATION_SCHEMA)
        {
            return false;
        }
        let table = read.table_name.as_str();
        [
            "PROCESSLIST",
            "TRX_SUMMARY",
            "MEMORY_USAGE",
            "MEMORY_USAGE_OPS_HISTORY",
            "TIDB_INDEX_USAGE",
            "SLOW_QUERY",
            "DDL_JOBS",
            "ANALYZE_STATUS",
            "INSPECTION_RULES",
            "INSPECTION_SUMMARY",
            "METRICS_SUMMARY",
            "METRICS_SUMMARY_BY_LABEL",
        ]
        .iter()
        .any(|name| table.go_lower_eq(name))
            || Self::is_statement_summary(table)
            || Self::is_client_errors_summary(table)
    }

    fn retrieve(
        &self,
        read: &MemTableRead,
        catalog: &Catalog,
        ctx: &tidb_executor::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_planner::memtable_predicate_extractor::MemTablePredicateExtractor as Extractor;
        let table = read.table_name.as_str();
        if table.go_lower_eq("SLOW_QUERY") {
            let extractors: Vec<_> = match &read.extractor {
                Some(Extractor::SlowQuery(extractor)) => vec![extractor],
                _ => Vec::new(),
            };
            let (rows, warnings) = self
                .viewer
                .slow_query_rows(&extractors, &ctx.session_zone())?;
            for warning in warnings {
                ctx.append_warning_parts(1105, &warning);
            }
            return Ok(rows);
        }
        if table.go_lower_eq("DDL_JOBS") {
            // Go `DDLJobsReaderExec`: the running jobs, then the history, over
            // the statement's own InfoSchema (the snapshot schema under
            // `tidb_snapshot`, the transaction's otherwise).
            let (running, history) = self.viewer.all_ddl_jobs()?;
            let jobs = running.into_iter().chain(history).collect::<Vec<_>>();
            return Ok(crate::infoschema::ddl_jobs_rows(
                &jobs,
                &self.viewer.visibility,
                catalog,
            ));
        }
        if table.go_lower_eq("ANALYZE_STATUS") {
            return crate::show::statistics::analyze_status_rows_of(
                self.viewer.analyze_status.as_deref(),
                &self.viewer.resource_group,
                &self.viewer.visibility,
                &ctx.session_zone(),
                ctx.stats_handle(),
                catalog,
            );
        }
        if table.go_lower_eq("INSPECTION_RULES") {
            return Ok(crate::inspection::inspection_rules_rows());
        }
        // Go's retrievers query the metrics tables through the restricted SQL
        // executor.
        let query_metrics = |sql: &str| {
            crate::restricted_sql::exec_restricted_sql_over(self.viewer.restricted_sql.as_deref(), sql)
        };
        if table.go_lower_eq("INSPECTION_SUMMARY") {
            let Some(Extractor::InspectionSummaryTable(extractor)) = &read.extractor else {
                return Ok(Vec::new());
            };
            return crate::inspection::inspection_summary_rows(
                extractor,
                &read.query_time_range,
                query_metrics,
                |warning| ctx.append_warning_parts(1105, &warning),
            );
        }
        if table.go_lower_eq("METRICS_SUMMARY") || table.go_lower_eq("METRICS_SUMMARY_BY_LABEL") {
            // Go checks PROCESS before it looks at the extractor.
            if !self.viewer.process_scoped {
                return Err(DriverError::from(
                    tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                        .generate_with_stack_by_args(&["PROCESS".into()]),
                ));
            }
            let Some(Extractor::MetricSummaryTable(extractor)) = &read.extractor else {
                return Ok(Vec::new());
            };
            return crate::memtable_retrievers::metrics_summary_rows(
                extractor,
                &read.query_time_range,
                table.go_lower_eq("METRICS_SUMMARY_BY_LABEL"),
                query_metrics,
            );
        }
        if table.go_lower_eq("PROCESSLIST") {
            return Ok(crate::process_arm::format_process_rows(
                self.viewer.visible_process_rows(true),
                &ctx.session_zone(),
            ));
        }
        if table.go_lower_eq("TRX_SUMMARY") {
            return Ok(self.viewer.transaction_summary_rows(None));
        }
        if table.go_lower_eq("MEMORY_USAGE") {
            return Ok(crate::dispatch::memory_usage_table_rows());
        }
        if table.go_lower_eq("MEMORY_USAGE_OPS_HISTORY") {
            return Ok(tidb_util::servermemorylimit::GLOBAL_MEMORY_OPS_HISTORY_MANAGER.get_rows());
        }
        if table.go_lower_eq("TIDB_INDEX_USAGE") {
            return Ok(crate::infoschema::tidb_index_usage_rows(
                catalog,
                &self.viewer.visibility,
                self.viewer.index_usage.as_ref(),
            ));
        }
        if Self::is_client_errors_summary(table) {
            return crate::Session::client_errors_summary_rows(
                table,
                self.viewer.has_process,
                self.viewer.login_user.as_deref(),
            );
        }
        let columns = crate::infoschema::table_schema(table).ok_or_else(|| {
            DriverError::unsupported("a statement summary table has no declared schema")
        })?;
        self.viewer
            .statement_summary_rows(table, &columns, &ctx.session_zone(), false, None)
    }
}
