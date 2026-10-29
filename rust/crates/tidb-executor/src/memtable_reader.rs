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

use crate::StatsStmtCtx as _;
use std::sync::Arc;

use tidb_datatype::{Datum, FieldType};
use crate::mem_table_retrieval::{MemTableRead, MemTableRetriever};
use crate::{Catalog, DriverError};
use tidb_hack::GoCaseCompare;

use crate::infoschema_reader::SchemaVisibility;
use crate::process;

/// What the instance-wide tables read of the session viewing them.
#[derive(Clone)]
pub struct ViewerState {
    /// Go's `hasPriv(sctx, mysql.ProcessPriv)`, which `PROCESS` privileged
    /// viewers and internal sessions pass.
    pub has_process: bool,
    /// The instance's connection registry, absent for a session no server
    /// front end owns.
    pub registry: Option<process::ProcessRegistry>,
    /// This connection's id, user name, current schema, status text and
    /// resource group, which stand for it when there is no registry.
    pub connection_id: u64,
    /// The id the session's process-registry guard registered, absent when no
    /// server front end owns the session.
    pub own_process_id: Option<u64>,
    pub process_user: String,
    pub current_db: String,
    pub status: String,
    pub resource_group: String,
    /// The login (`user@host`) and the authenticated account.
    pub login_user: Option<String>,
    pub auth_identity: Option<(String, String)>,
    /// `@@tidb_distsql_scan_concurrency`, the statement-summary history
    /// reader's worker count.
    pub scan_concurrency: usize,
    pub visibility: SchemaVisibility,
    pub index_usage: Arc<tidb_stats_handle_usage_indexusage::Collector>,
    /// Go `RequestVerification("", "", "", mysql.ProcessPriv)`, which the
    /// metric-backed tables ask before they look at their extractor.
    pub process_scoped: bool,
    /// Go `GetRestrictedSQLExecutor`, which the inspection retrievers query
    /// the metrics tables through.
    pub restricted_sql: RestrictedSql,
    /// `@@tidb_slow_query_file`.
    pub slow_query_file: String,
    /// The DDL job store a node installs (Go reads the store's meta).
    pub ddl_job_history_store: Option<Arc<dyn DdlJobHistoryStore>>,
    /// The analyze-job reader, absent for a session with no statistics.
    pub analyze_status: Option<Arc<dyn crate::show::stats::AnalyzeStatusProvider>>,
    /// Go's SQL-digest text retriever over this node and its peers.
    pub digest_retriever: SqlDigestTextRetriever,
    pub data_lock_waits: Option<Arc<dyn DataLockWaitsProvider>>,
    /// The cluster's discovery and diagnostics clients; absent for a session
    /// no server front end owns, which reads these tables as empty.
    pub cluster_topology: Option<Arc<tidb_domain::cluster_topology::ClusterTopology>>,
    pub cluster_config: Option<Arc<crate::cluster_config::ClusterConfigClient>>,
    pub cluster_diagnostics:
        Option<Arc<crate::cluster_diagnostics::ClusterDiagnosticsClient>>,
    pub cluster_peer: Option<Arc<crate::cluster_peer::ClusterPeerClient>>,
    pub server_info_syncer: Option<Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    /// Go `RequestVerification("", "", "", mysql.ConfigPriv)`.
    pub config_scoped: bool,
    /// Whether SEM hides the cluster metadata from the viewer.
    pub hides_cluster_metadata: bool,
}

impl ViewerState {
    /// Go `setDataForProcessList`: "If you have the PROCESS privilege, you
    /// can see all threads. Otherwise, you can see only your own threads";
    /// an internal session with no login user is not filtered at all.
    pub fn filter_process_rows(
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
    pub fn visible_process_rows(&self, full: bool) -> Vec<process::ProcessRow> {
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
    pub fn transaction_summary_rows(
        &self,
        registry: Option<&process::ProcessRegistry>,
    ) -> Vec<Vec<Datum>> {
        if !self.has_process {
            return Vec::new();
        }
        registry.or(self.registry.as_ref()).map_or_else(
            || crate::txn_summary::RECORDER.rows(),
            |registry| registry.transaction_history_rows(),
        )
    }

    /// Go `slowQueryRetriever.retrieve`: SLOW_QUERY's rows from this
    /// instance's slow-log file. The viewer sees every user's entries with
    /// `PROCESS`, else only its own; the scans' extractors bound the entries'
    /// time (a statement scanning the table more than once reads the union
    /// of their ranges). Returns the rows and the warnings the read raised.
    pub fn slow_query_rows(
        &self,
        extractors: &[&tidb_planner::memtable_predicate_extractor::SlowQueryExtractor],
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<(Vec<Vec<Datum>>, Vec<String>), DriverError> {
        if !extractors.is_empty() && extractors.iter().all(|extractor| extractor.skip_request) {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut checker = crate::slow_query::SlowLogChecker {
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
            crate::infoschema_reader::table_schema("SLOW_QUERY").expect("SLOW_QUERY is registered");
        let read = crate::slow_query::retrieve_slow_query_rows(
            &columns,
            &checker,
            zone,
            &self.slow_query_file,
        )
        .map_err(|error| DriverError::Mysql(crate::MysqlError::unknown(error)))?;
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
    pub fn statement_summary_rows(
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
        statement_summary_rows(
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
pub struct InstanceStateRetriever {
    viewer: ViewerState,
}

impl InstanceStateRetriever {
    pub fn new(viewer: ViewerState) -> Self {
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
                .go_lower_eq(tidb_infoschema::infoschema_meta::INFORMATION_SCHEMA)
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
        ctx: &crate::StmtContext,
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
            return Ok(crate::infoschema_reader::ddl_jobs_rows(
                &jobs,
                &self.viewer.visibility,
                catalog,
            ));
        }
        if table.go_lower_eq("ANALYZE_STATUS") {
            return crate::show::stats::analyze_status_rows_of(
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
            exec_restricted_sql_over(self.viewer.restricted_sql.as_deref(), sql)
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
            return Ok(format_process_rows(
                self.viewer.visible_process_rows(true),
                &ctx.session_zone(),
            ));
        }
        if table.go_lower_eq("TRX_SUMMARY") {
            return Ok(self.viewer.transaction_summary_rows(None));
        }
        if table.go_lower_eq("MEMORY_USAGE") {
            return Ok(memory_usage_table_rows());
        }
        if table.go_lower_eq("MEMORY_USAGE_OPS_HISTORY") {
            return Ok(tidb_util::servermemorylimit::GLOBAL_MEMORY_OPS_HISTORY_MANAGER.get_rows());
        }
        if table.go_lower_eq("TIDB_INDEX_USAGE") {
            return Ok(crate::infoschema_reader::tidb_index_usage_rows(
                catalog,
                &self.viewer.visibility,
                self.viewer.index_usage.as_ref(),
            ));
        }
        if Self::is_client_errors_summary(table) {
            return client_errors_summary_rows(
                table,
                self.viewer.has_process,
                self.viewer.login_user.as_deref(),
            );
        }
        let columns = crate::infoschema_reader::table_schema(table).ok_or_else(|| {
            DriverError::unsupported("a statement summary table has no declared schema")
        })?;
        self.viewer
            .statement_summary_rows(table, &columns, &ctx.session_zone(), false, None)
    }
}

/// One pessimistic wait-for edge returned by the active storage backend.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataLockWait {
    /// Transaction waiting for the lock.
    pub txn: u64,
    /// Transaction currently holding the lock.
    pub wait_for_txn: u64,
    /// Encoded locked key.
    pub key: Vec<u8>,
    /// Encoded TopSQL resource-group tag.
    pub resource_group_tag: Vec<u8>,
}


/// Storage boundary for `information_schema.DATA_LOCK_WAITS`.
pub trait DataLockWaitsProvider: Send + Sync {
    /// Reads the current wait-for edges from the backend.
    fn lock_waits(&self) -> Result<Vec<DataLockWait>, String>;
}


/// The cluster's DDL jobs, Go's `mysql.tidb_ddl_job` and meta
/// `DDLJobHistory`, where a front end keeps them in a shared store rather
/// than in its own catalog.
pub trait DdlJobHistoryStore: Send + Sync {
    /// Go `IterAllDDLJobs`' order: the running jobs (`GetAllDDLJobs`, by job
    /// ID), then the meta history newest first
    /// (`GetLastHistoryDDLJobsIterator`).
    fn all_ddl_jobs(&self) -> Result<(Vec<tidb_model::Job>, Vec<tidb_model::Job>), String>;
}


/// Go `RestrictedSQLExecutor.ExecRestrictedSQL` with no options: one
/// schema-qualified statement on an internal session, answering its rows.
pub trait RestrictedSqlExecutor: Send + Sync {
    /// Runs `sql` in an internal session's own transaction.
    fn exec_restricted_sql(&self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError>;
}

/// The shared handle a session and its retriever snapshots carry.
pub type RestrictedSql = Option<Arc<dyn RestrictedSqlExecutor>>;

/// Go `ExecRestrictedSQL` through the session's executor, which the node
/// that opened the session installs.
pub fn exec_restricted_sql_over(
    executor: Option<&dyn RestrictedSqlExecutor>,
    sql: &str,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    executor
        .ok_or_else(|| {
            DriverError::unsupported("restricted SQL runs on the session's node".to_owned())
        })?
        .exec_restricted_sql(sql)
}



pub fn memory_usage_table_rows() -> Vec<Vec<tidb_datatype::Datum>> {
    use std::sync::atomic::Ordering;

    use tidb_datatype::{core_time_from_datetime, CoreTime, Datum, Time, TimeType};

    let stats = tidb_util::memory::read_mem_stats();
    let current_ops = tidb_util::servermemorylimit::IS_KILLING
        .load(Ordering::SeqCst)
        .then(|| Datum::new_string("shrink"))
        .unwrap_or(Datum::Null);
    let session_kill_last = tidb_util::servermemorylimit::SESSION_KILL_LAST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .map(|value| {
            Datum::new_time(
                Time::new(core_time_from_datetime(value), TimeType::DateTime, 0)
                    .expect("fsp 0 is valid"),
            )
        })
        .unwrap_or(Datum::Null);
    let zero_datetime = Datum::new_time(
        Time::new(CoreTime::default(), TimeType::DateTime, 0).expect("fsp 0 is valid"),
    );
    vec![vec![
        Datum::new_int(
            tidb_util::memory::mem_total()
                .ok()
                .and_then(|value| i64::try_from(value).ok())
                .unwrap_or(0),
        ),
        Datum::new_int(
            i64::try_from(tidb_util::memory::SERVER_MEMORY_LIMIT.load(Ordering::SeqCst))
                .unwrap_or(i64::MAX),
        ),
        Datum::new_int(stats.heap_inuse),
        Datum::new_int(
            i64::try_from(tidb_util::servermemorylimit::MEMORY_MAX_USED.load(Ordering::SeqCst))
                .unwrap_or(i64::MAX),
        ),
        current_ops,
        session_kill_last,
        Datum::new_int(tidb_util::servermemorylimit::SESSION_KILL_TOTAL.load(Ordering::SeqCst)),
        zero_datetime,
        Datum::new_int(0),
        Datum::new_int(0),
        Datum::new_int(0),
    ]]
}

/// The rows of a statement-summary table for `user` (`None`: every user,
/// as Go's restricted SQL reads them), over the v1 map or the persistent v2
/// owner the instance configuration selects.
pub fn statement_summary_rows(
    table_name: &str,
    columns: &[(String, tidb_datatype::FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    user: Option<tidb_parser::auth::UserIdentity>,
    has_process: bool,
    digests: Option<&std::collections::HashSet<String>>,
    scan_concurrency: usize,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
    use tidb_ast::CiString;
    use tidb_model::ColumnInfo;
    use tidb_stmtsummary::reader::{StmtSummaryChecker, StmtSummaryReader};

    if table_name.go_lower_eq("STATEMENTS_SUMMARY_EVICTED") {
        if !has_process {
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["PROCESS".into()]),
            ));
        }
        let full: Vec<Vec<tidb_datatype::Datum>> = if tidb_config::config_tree::config::get_global_config()
            .instance
            .stmt_summary_enable_persistent
        {
            tidb_stmtsummary::v2::stmtsummary::global_stmt_summary()
                .map(|summary| summary.evicted())
                .filter(|row| !row.is_empty())
                .into_iter()
                .collect()
        } else {
            tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.to_evicted_count_datum()
        };
        // The caller asks for `columns` (a pruned scan reads only some of the
        // table's); the evicted row is produced at full width.
        let schema = crate::infoschema_reader::table_schema("STATEMENTS_SUMMARY_EVICTED")
            .unwrap_or_default();
        let positions: Vec<usize> = columns
            .iter()
            .filter_map(|(name, _)| schema.iter().position(|(have, _)| have.eq_ignore_ascii_case(name)))
            .collect();
        if positions.len() != columns.len() || positions.len() == schema.len() {
            return Ok(full);
        }
        return Ok(full
            .into_iter()
            .map(|row| positions.iter().map(|position| row[*position].clone()).collect())
            .collect());
    }

    let columns: Vec<ColumnInfo> = columns
        .iter()
        .enumerate()
        .map(|(offset, (name, _))| ColumnInfo {
            id: i64::try_from(offset).expect("information-schema column count fits i64"),
            name: CiString::new(name),
            offset: i64::try_from(offset).expect("information-schema column count fits i64"),
            ..ColumnInfo::default()
        })
        .collect();
    let persistent = tidb_config::config_tree::config::get_global_config()
        .instance
        .stmt_summary_enable_persistent;
    let cumulative = table_name.go_lower_eq("TIDB_STATEMENTS_STATS");
    let history = table_name.go_lower_eq("STATEMENTS_SUMMARY_HISTORY");
    if persistent {
        // Go refuses cumulative retrieval in v2; never silently substitute
        // an unrelated v1 map for the selected persistent owner.
        if cumulative {
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_NOT_SUPPORTED_YET.generate_with_stack_by_args(&[
                    "cumulative statement summary table with persistent mode (v2)".into(),
                ]),
            ));
        }
        let mut rows = tidb_stmtsummary::v2::reader::new_mem_reader(
            tidb_stmtsummary::v2::stmtsummary::global_stmt_summary(),
            &columns,
            String::new(),
            zone.clone(),
            user.clone(),
            has_process,
            digests.cloned(),
            Vec::new(),
        )
        .rows();
        if history {
            let mut reader = tidb_stmtsummary::v2::reader::HistoryReader::new(
                None,
                &columns,
                String::new(),
                zone.clone(),
                user,
                has_process,
                digests.cloned(),
                Vec::new(),
                scan_concurrency,
            )
            .map_err(|error| DriverError::Unsupported(error.into()))?;
            let result = (|| {
                while let Some(batch) = reader
                    .rows()
                    .map_err(|error| DriverError::Unsupported(error.into()))?
                {
                    rows.extend(batch);
                }
                Ok(rows)
            })();
            let _ = reader.close();
            return result;
        }
        return Ok(rows);
    }
    let mut reader =
        StmtSummaryReader::new(user, has_process, columns, String::new(), zone.clone());
    reader.set_checker(digests.cloned().map(StmtSummaryChecker::new));
    Ok(if cumulative {
        reader.get_stmt_summary_cumulative_rows()
    } else if history {
        reader.get_stmt_summary_history_rows()
    } else {
        reader.get_stmt_summary_current_rows()
    })
}

/// Pinned Go `memtableRetriever.setDataForClientErrorsSummary`.
pub fn client_errors_summary_rows(
    table_name: &str,
    has_process: bool,
    login_user: Option<&str>,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
    use chrono::{DateTime, Local};
    use tidb_datatype::{core_time_from_datetime, CoreTime, Datum, Time, TimeType};
    use tidb_error::tidb::infoschema::{self, ErrorStats};

    fn text(value: &str) -> Datum {
        Datum::new_string(value.as_bytes())
    }

    fn count(value: isize) -> Datum {
        Datum::Int(i64::try_from(value).expect("client error count fits i64"))
    }

    fn timestamp(value: Option<std::time::SystemTime>) -> Datum {
        let time = match value {
            Some(value) => {
                let local: DateTime<Local> = value.into();
                Time::new(core_time_from_datetime(local), TimeType::Timestamp, 0)
            }
            None => Time::new(CoreTime::from_raw(0), TimeType::Timestamp, 0),
        }
        .expect("fsp 0 is valid for client-error timestamps");
        Datum::new_time(time)
    }

    fn message(code: u16) -> &'static str {
        tidb_error::mysql::message_by_code(code)
            .or_else(|| tidb_error::tidb::message_by_code(code))
            .map_or("", |message| message.raw)
    }

    fn summary_cells(code: u16, summary: &infoschema::ErrorSummary) -> Vec<Datum> {
        vec![
            Datum::Int(i64::from(code)),
            text(message(code)),
            count(summary.error_count),
            count(summary.warning_count),
            timestamp(Some(summary.first_seen)),
            timestamp(summary.last_seen),
        ]
    }

    fn append_scoped(rows: &mut Vec<Vec<Datum>>, scope: &str, stats: ErrorStats) {
        for (code, summary) in stats {
            let mut row = Vec::with_capacity(7);
            row.push(text(scope));
            row.extend(summary_cells(code, &summary));
            rows.push(row);
        }
    }

    if !has_process
        && (table_name.go_lower_eq("CLIENT_ERRORS_SUMMARY_GLOBAL")
            || table_name.go_lower_eq("CLIENT_ERRORS_SUMMARY_BY_HOST"))
    {
        return Err(DriverError::from(
            tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                .generate_with_stack_by_args(&["PROCESS".into()]),
        ));
    }

    let mut rows = Vec::new();
    if table_name.go_lower_eq("CLIENT_ERRORS_SUMMARY_GLOBAL") {
        for (code, summary) in infoschema::global_stats() {
            rows.push(summary_cells(code, &summary));
        }
    } else if table_name.go_lower_eq("CLIENT_ERRORS_SUMMARY_BY_HOST") {
        for (host, stats) in infoschema::host_stats() {
            append_scoped(&mut rows, &host, stats);
        }
    } else {
        let login_username = login_user
            .and_then(|identity| identity.split_once('@').map(|(user, _)| user));
        for (user, stats) in infoschema::user_stats() {
            if !has_process && login_username.is_some_and(|login| login != user) {
                continue;
            }
            append_scoped(&mut rows, &user, stats);
        }
    }
    Ok(rows)
}

/// Go `expression.SQLDigestTextRetriever`: the normalized statement text of
/// each requested digest, read the way Go's `ExecRestrictedSQL` reads it --
/// from this node's `statements_summary` and its history, then, for
/// `RetrieveGlobal`, from the peers' cluster tables for the digests still
/// unknown. Owned, so `TIDB_DECODE_SQL_DIGESTS` can retrieve while it
/// evaluates.
#[derive(Clone)]
pub struct SqlDigestTextRetriever {
    pub zone: tidb_datatype::SessionTimeZone,
    pub scan_concurrency: usize,
    pub server_info_syncer: Option<std::sync::Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    pub cluster_peer: Option<std::sync::Arc<crate::cluster_peer::ClusterPeerClient>>,
}

impl SqlDigestTextRetriever {
    /// Go `RetrieveLocal`, then with `global` `RetrieveGlobal`'s peer pass.
    /// A digest the summaries do not know maps to the empty text.
    pub fn retrieve(
        &self,
        ctx: &crate::StmtContext,
        digests: impl IntoIterator<Item = String>,
        global: bool,
    ) -> Result<std::collections::HashMap<String, String>, DriverError> {
        use std::collections::{HashMap, HashSet};
        let mut texts: HashMap<String, String> = digests
            .into_iter()
            .map(|digest| (digest, String::new()))
            .collect();
        if texts.is_empty() {
            return Ok(texts);
        }
        let requested: HashSet<_> = texts.keys().cloned().collect();
        // Go limits the WHERE IN list to512 digests, otherwise fetching all.
        let filter = (requested.len() <= 512).then_some(&requested);
        let columns = crate::infoschema_reader::table_schema("STATEMENTS_SUMMARY")
            .expect("summary schema");
        let columns: Vec<_> = ["DIGEST", "DIGEST_TEXT"]
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .find(|(column, _)| column == name)
                    .expect("digest column")
                    .clone()
            })
            .collect();
        fn merge(texts: &mut HashMap<String, String>, rows: Vec<Vec<Datum>>) {
            for row in rows {
                let (Some(digest), Some(text)) = (row[0].as_raw_bytes(), row[1].as_raw_bytes())
                else {
                    continue;
                };
                if let Some(known) = texts.get_mut(String::from_utf8_lossy(digest).as_ref()) {
                    if known.is_empty() {
                        *known = String::from_utf8_lossy(text).into_owned();
                    }
                }
            }
        }
        for table in ["STATEMENTS_SUMMARY", "STATEMENTS_SUMMARY_HISTORY"] {
            merge(
                &mut texts,
                statement_summary_rows(
                    table,
                    &columns,
                    &self.zone,
                    None,
                    true,
                    filter,
                    self.scan_concurrency,
                )?,
            );
        }
        let unknown: HashSet<_> = texts
            .iter()
            .filter(|(_, text)| text.is_empty())
            .map(|(digest, _)| digest.clone())
            .collect();
        if !global || unknown.is_empty() {
            return Ok(texts);
        }
        let filter = (requested.len() <= 512).then_some(&unknown);
        for (table, source) in [
            ("CLUSTER_STATEMENTS_SUMMARY", "STATEMENTS_SUMMARY"),
            (
                "CLUSTER_STATEMENTS_SUMMARY_HISTORY",
                "STATEMENTS_SUMMARY_HISTORY",
            ),
        ] {
            let schema = crate::infoschema_reader::table_schema(table)
                .expect("cluster summary schema");
            let offsets: Vec<_> = ["DIGEST", "DIGEST_TEXT"]
                .iter()
                .map(|name| {
                    schema
                        .iter()
                        .position(|(column, _)| column == name)
                        .expect("digest column")
                })
                .collect();
            let local = statement_summary_rows(
                source,
                &columns,
                &self.zone,
                None,
                true,
                filter,
                self.scan_concurrency,
            )?;
            // Go reads the peers through `ExecRestrictedSQL`, whose warnings
            // stay on the internal session.
            let (rows, _) = peer_cluster_table_rows(
                self.server_info_syncer.as_deref(),
                self.cluster_peer.as_deref(),
                ctx,
                None,
                &self.zone,
                table,
                &schema,
                &offsets,
                local,
                filter,
            )?;
            merge(&mut texts, rows);
        }
        Ok(texts)
    }
}


/// Go `ProcessInfo.ToRow` for each row: the columns of
/// `information_schema.PROCESSLIST`.
pub fn format_process_rows(
    rows: Vec<process::ProcessRow>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<Vec<Datum>> {
    rows.into_iter()
        .map(|row| {
            let txn_start = if row.cur_txn_start_ts == 0 {
                String::new()
            } else {
                let time = tidb_expr::sessionexpr::get_time_from_ts(row.cur_txn_start_ts)
                    .with_timezone(zone);
                format!(
                    "{}({})",
                    time.format("%m-%d %H:%M:%S%.3f"),
                    row.cur_txn_start_ts
                )
            };
            vec![
                Datum::UInt(row.id),
                Datum::Bytes(row.user.into_bytes()),
                Datum::Bytes(row.host.into_bytes()),
                if row.db.is_empty() {
                    Datum::Null
                } else {
                    Datum::Bytes(row.db.into_bytes())
                },
                Datum::Bytes(row.command.into_bytes()),
                Datum::Int(i64::try_from(row.time).unwrap_or(i64::MAX)),
                if row.state.is_empty() {
                    Datum::Null
                } else {
                    Datum::Bytes(row.state.into_bytes())
                },
                match row.info {
                    Some(info) => {
                        Datum::Bytes(tidb_hack::raw_bytes::str_to_bytes(&info).into_owned())
                    }
                    None => Datum::Null,
                },
                Datum::Bytes(row.digest.into_bytes()),
                Datum::Int(row.mem_bytes),
                // MEM_ARBITRATION
                Datum::Null,
                // MEM_WAIT_ARBITRATE_START
                Datum::Null,
                // MEM_WAIT_ARBITRATE_BYTES
                Datum::Null,
                Datum::Int(row.disk_bytes),
                Datum::Bytes(txn_start.into_bytes()),
                Datum::Bytes(row.resource_group.into_bytes()),
                Datum::Bytes(row.session_alias.into_bytes()),
                row.affected_rows.map_or(Datum::Null, Datum::UInt),
                // TIDB_CPU
                Datum::Int(0),
                // TIKV_CPU
                Datum::Int(0),
            ]
        })
        .collect()
}

/// The rows the discovered peers answer for a cluster memory table, after
/// `rows` (this node's own, dropped when discovery does not list this node).
/// Without a server-info syncer there are no peers and `rows` is the answer.
/// The second value is the warnings the peers reported.
#[allow(clippy::too_many_arguments)]
pub fn peer_cluster_table_rows(
    syncer: Option<&tidb_domain::serverinfo_syncer::Syncer>,
    peer: Option<&crate::cluster_peer::ClusterPeerClient>,
    ctx: &crate::StmtContext,
    user: Option<(&str, &str)>,
    zone: &tidb_datatype::SessionTimeZone,
    table_name: &str,
    columns: &[(String, FieldType)],
    offsets: &[usize],
    mut rows: Vec<Vec<Datum>>,
    digests: Option<&std::collections::HashSet<String>>,
) -> Result<(Vec<Vec<Datum>>, Vec<(u16, String)>), DriverError> {
    let Some(syncer) = syncer else {
        return Ok((rows, Vec::new()));
    };
    let local_id = syncer.local_server_info().static_info.id;
    let discovered = syncer.all_server_info().map_err(DriverError::unsupported)?;
    if !discovered
        .values()
        .any(|server| server.static_info.id == local_id && server.static_info.ip != "<nil>")
    {
        rows.clear();
    }
    let servers: Vec<_> = discovered
        .into_values()
        .filter(|server| server.static_info.id != local_id && server.static_info.ip != "<nil>")
        .collect();
    if servers.is_empty() {
        return Ok((rows, Vec::new()));
    }
    let client =
        peer.ok_or_else(|| DriverError::unsupported("TiDB peer RPC client is not installed"))?;
    let result = client.scan_projected(
        &servers,
        crate::infoschema_reader::memory_table_id(table_name)
            .expect("registered memory table"),
        columns,
        offsets,
        user,
        ctx,
        zone,
        ctx.dist_sql_scan_concurrency() as usize,
        digests,
    )?;
    rows.extend(result.rows);
    Ok((rows, result.warnings))
}

/// Go `time.Duration.String()` for a whole number of seconds, which is what
/// `UPTIME` carries: `time.Since(startTime).String()` with the sub-second
/// part always zero here because the start timestamp has second granularity.
pub fn go_uptime_string(seconds: i64) -> String {
    let seconds = seconds.max(0);
    let (hours, minutes, seconds) = (seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    if hours > 0 {
        format!("{hours}h{minutes}m{seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds}s")
    } else {
        format!("{seconds}s")
    }
}
