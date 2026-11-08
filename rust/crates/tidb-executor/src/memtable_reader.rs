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

use tidb_ddl::ddl_history::DdlJobHistoryStore;
use tidb_kv::lock_waits::DataLockWaitsProvider;
use tidb_sessmgr as process;
use crate::StatsStmtCtx as _;
use std::sync::Arc;

use tidb_datatype::{Datum, FieldType};
use crate::mem_table_retrieval::{MemTableRead, MemTableRetriever};
use crate::SessionExtendedInfoSchema;
use crate::DriverError;
use tidb_hack::GoCaseCompare;

use crate::infoschema_reader::SchemaVisibility;

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
    pub analyze_status: Option<Arc<dyn tidb_stats_handle_autoanalyze::analyze_jobs::AnalyzeStatusProvider>>,
    /// Go's SQL-digest text retriever over this node and its peers.
    pub digest_retriever: SqlDigestTextRetriever,
    pub data_lock_waits: Option<Arc<dyn DataLockWaitsProvider>>,
    /// The store client the TiFlash system tables query TiFlash through.
    pub tiflash_system_table: Option<Arc<dyn tidb_kv::tiflash_system_table::TiFlashSystemTableClient>>,
    /// The cluster's discovery and diagnostics clients; absent for a session
    /// no server front end owns, which reads these tables as empty.
    pub cluster_topology: Option<Arc<tidb_domain::cluster_topology::ClusterTopology>>,
    pub cluster_config: Option<Arc<crate::cluster_config::ClusterConfigClient>>,
    pub cluster_diagnostics:
        Option<Arc<crate::cluster_diagnostics::ClusterDiagnosticsClient>>,
    pub cluster_peer: Option<Arc<crate::cluster_peer::ClusterPeerClient>>,
    pub server_info_syncer: Option<Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>>,
    /// Go `RequestVerification("", "", "", mysql.ConfigPriv)`.
    pub config_scoped: bool,
    /// Whether SEM hides the cluster metadata from the viewer.
    pub hides_cluster_metadata: bool,
}

impl ViewerState {
    /// Go `setDataForProcessList`: "If you have the PROCESS privilege, you
    /// can see all threads. Otherwise, you can see only your own threads";
    /// an internal session with no login user is not filtered at all.
    pub fn set_data_for_process_list(
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
        self.set_data_for_process_list(rows)
    }

    /// Go `setDataForTrxSummary`: the transaction history, for a viewer that
    /// holds PROCESS.
    pub fn set_data_for_trx_summary(
        &self,
        registry: Option<&process::ProcessRegistry>,
    ) -> Vec<Vec<Datum>> {
        if !self.has_process {
            return Vec::new();
        }
        registry.or(self.registry.as_ref()).map_or_else(
            || tidb_txninfo::RECORDER.rows(),
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
        mem_tracker: Option<&std::sync::Arc<tidb_memory::Tracker>>,
        concurrency: usize,
    ) -> Result<(Vec<Vec<Datum>>, Vec<String>, crate::slow_query::SlowQueryRuntimeStats), DriverError> {
        if !extractors.is_empty() && extractors.iter().all(|extractor| extractor.skip_request) {
            return Ok((Vec::new(), Vec::new(), crate::slow_query::SlowQueryRuntimeStats::default()));
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
            tidb_infoschema::infoschema_meta::table_schema("SLOW_QUERY").expect("SLOW_QUERY is registered");
        let read = crate::slow_query::retrieve_slow_query_rows(
            &columns,
            &checker,
            zone,
            &self.slow_query_file,
            mem_tracker,
            // Go's `limit` and `Desc` hints come from the one scan's
            // extractor; a union of scans reads every entry forward.
            match extractors {
                [extractor] => extractor.limit,
                _ => 0,
            },
            matches!(extractors, [extractor] if extractor.desc),
            concurrency,
        )
        .map_err(|error| DriverError::Mysql(crate::MysqlError::unknown(error)))?;
        Ok((read.rows, read.warnings, read.stats))
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
        time_ranges: Vec<tidb_stmtsummary::v2::reader::StmtTimeRange>,
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
            time_ranges,
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
            "TIFLASH_TABLES",
            "TIFLASH_SEGMENTS",
            "TIFLASH_INDEXES",
        ]
        .iter()
        .any(|name| table.go_lower_eq(name))
            || Self::is_statement_summary(table)
            || Self::is_client_errors_summary(table)
    }

    fn retrieve(
        &self,
        read: &MemTableRead,
        catalog: &SessionExtendedInfoSchema,
        ctx: &crate::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_planner::memtable_predicate_extractor::MemTableExtractor as Extractor;
        let table = read.table_name.as_str();
        if table.go_lower_eq("SLOW_QUERY") {
            let extractors: Vec<_> = match &read.extractor {
                Some(Extractor::SlowQuery(extractor)) => vec![extractor],
                _ => Vec::new(),
            };
            // Go `buildMemTable`'s `slowQueryRetriever.memTracker`: the
            // reader's own tracker under the statement's.
            let mem_tracker = ctx
                .statement_memory()
                .persistent_operator_tracker(i64::from(read.plan_id));
            let (rows, warnings, stats) = self.viewer.slow_query_rows(
                &extractors,
                &ctx.session_zone(),
                Some(&mem_tracker),
                usize::try_from(ctx.dist_sql_scan_concurrency()).unwrap_or(usize::MAX),
            )?;
            // Go `MemTableReaderExec.Close`: `RegisterStats(e.ID(),
            // retriever.getRuntimeStats())`.
            if let Some(source) = ctx.executor_runtime_stats_source() {
                source.register_stats(i64::from(read.plan_id), Box::new(stats));
            }
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
            return crate::show::stats::data_for_analyze_status_helper(
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
        if table.go_lower_eq("TIFLASH_TABLES")
            || table.go_lower_eq("TIFLASH_SEGMENTS")
            || table.go_lower_eq("TIFLASH_INDEXES")
        {
            let Some(Extractor::TiFlashSystemTable(extractor)) = &read.extractor else {
                return Ok(Vec::new());
            };
            // A session no server front end owns has no store topology and
            // reads these tables as empty.
            let Some(topology) = &self.viewer.cluster_topology else {
                return Ok(Vec::new());
            };
            let stores = topology
                .get_store_server_info()
                .map_err(DriverError::unsupported)?;
            return crate::infoschema_reader::tiflash_system_table_rows(
                table,
                &read.columns,
                extractor,
                &stores,
                self.viewer.tiflash_system_table.as_deref(),
                catalog,
            );
        }
        // Go's retrievers query the metrics tables through the restricted SQL
        // executor, under `kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)`.
        let query_metrics = |sql: &str| {
            exec_restricted_sql_over(
                self.viewer.restricted_sql.as_deref(),
                tidb_txnkv::INTERNAL_TXN_META,
                sql,
                &[],
            )
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
                    tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                        .gen_with_stack_by_args(&["PROCESS".into()]),
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
            return Ok(self.viewer.set_data_for_trx_summary(None));
        }
        if table.go_lower_eq("MEMORY_USAGE") {
            return Ok(memory_usage_table_rows());
        }
        if table.go_lower_eq("MEMORY_USAGE_OPS_HISTORY") {
            return Ok(tidb_servermemorylimit::GLOBAL_MEMORY_OPS_HISTORY_MANAGER.get_rows());
        }
        if table.go_lower_eq("TIDB_INDEX_USAGE") {
            return Ok(crate::infoschema_reader::set_data_from_index_usage(
                catalog,
                &self.viewer.visibility,
                self.viewer.index_usage.as_ref(),
            ));
        }
        if Self::is_client_errors_summary(table) {
            return set_data_for_client_errors_summary(
                table,
                self.viewer.has_process,
                self.viewer.login_user.as_deref(),
            );
        }
        let columns = tidb_infoschema::infoschema_meta::table_schema(table).ok_or_else(|| {
            DriverError::unsupported("a statement summary table has no declared schema")
        })?;
        self.viewer
            .statement_summary_rows(table, &columns, &ctx.session_zone(), false, None, Vec::new())
    }
}

/// Go `sctx.GetRestrictedSQLExecutor()`: the shared handle a session and its
/// retriever snapshots carry.
pub type RestrictedSql = Option<Arc<dyn tidb_sqlexec::RestrictedSqlExecutor>>;

/// Go `ExecRestrictedSQL(kv.WithInternalSourceType(ctx, source), nil, sql,
/// args...)` through the session's executor, which the node that opened the
/// session installs; the rows, as Go's callers read them.
pub fn exec_restricted_sql_over(
    executor: Option<&dyn tidb_sqlexec::RestrictedSqlExecutor>,
    source: &str,
    sql: &str,
    arguments: &[tidb_sqlescape::SqlArg<'_>],
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let executor = executor.ok_or_else(|| {
        DriverError::unsupported("restricted SQL runs on the session's node".to_owned())
    })?;
    let context = tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        source,
    );
    executor
        .exec_restricted_sql(&context, &[], sql, arguments)
        .map(|(rows, _)| rows)
        .map_err(sql_exec_driver_error)
}

/// A SQL executor's failure as the statement error it carries, or Go's
/// unknown error naming any other.
pub fn sql_exec_driver_error(error: tidb_sqlexec::SqlExecError) -> DriverError {
    DriverError::Mysql(tidb_store_driver_error::sql_query_error::mysql_error_of(error))
}

pub fn memory_usage_table_rows() -> Vec<Vec<tidb_datatype::Datum>> {
    use std::sync::atomic::Ordering;

    use tidb_datatype::{core_time_from_datetime, CoreTime, Datum, Time, TimeType};

    let stats = tidb_memory::read_mem_stats();
    let current_ops = tidb_servermemorylimit::IS_KILLING
        .load(Ordering::SeqCst)
        .then(|| Datum::new_string_datum("shrink"))
        .unwrap_or(Datum::Null);
    let session_kill_last = tidb_servermemorylimit::SESSION_KILL_LAST
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
            tidb_memory::mem_total()
                .ok()
                .and_then(|value| i64::try_from(value).ok())
                .unwrap_or(0),
        ),
        Datum::new_int(
            i64::try_from(tidb_memory::SERVER_MEMORY_LIMIT.load(Ordering::SeqCst))
                .unwrap_or(i64::MAX),
        ),
        Datum::new_int(stats.heap_inuse),
        Datum::new_int(
            i64::try_from(tidb_servermemorylimit::MEMORY_MAX_USED.load(Ordering::SeqCst))
                .unwrap_or(i64::MAX),
        ),
        current_ops,
        session_kill_last,
        Datum::new_int(tidb_servermemorylimit::SESSION_KILL_TOTAL.load(Ordering::SeqCst)),
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
    time_ranges: Vec<tidb_stmtsummary::v2::reader::StmtTimeRange>,
    scan_concurrency: usize,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
    use tidb_ast::CiString;
    use tidb_model::ColumnInfo;
    use tidb_stmtsummary::reader::{StmtSummaryChecker, StmtSummaryReader};

    if table_name.go_lower_eq("STATEMENTS_SUMMARY_EVICTED") {
        if !has_process {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .gen_with_stack_by_args(&["PROCESS".into()]),
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
        let schema = tidb_infoschema::infoschema_meta::table_schema("STATEMENTS_SUMMARY_EVICTED")
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
                tidb_plannererrors::ERR_NOT_SUPPORTED_YET.gen_with_stack_by_args(&[
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
            time_ranges.clone(),
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
                time_ranges,
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
pub fn set_data_for_client_errors_summary(
    table_name: &str,
    has_process: bool,
    login_user: Option<&str>,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
    use chrono::{DateTime, Local};
    use tidb_datatype::{core_time_from_datetime, CoreTime, Datum, Time, TimeType};
    use tidb_errno::infoschema::{self, ErrorStats};

    fn text(value: &str) -> Datum {
        Datum::new_string_datum(value.as_bytes())
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
        tidb_mysql::errors::message_by_code(code)
            .or_else(|| tidb_errno::message_by_code(code))
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
            tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                .gen_with_stack_by_args(&["PROCESS".into()]),
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

/// Go `SQLDigestTextRetriever` (`pkg/expression/util.go`): the SQL texts of
/// statement digests, from the statement summaries through the session's
/// restricted SQL executor.
#[derive(Clone)]
pub struct SqlDigestTextRetriever {
    /// Go `expropt.SQLExecutor`: the session's `ExecRestrictedSQL`.
    pub restricted_sql: RestrictedSql,
}

/// Go `SQLDigestTextRetriever.fetchAllLimit`: past this many digests the
/// query fetches every row instead of listing them.
const FETCH_ALL_LIMIT: usize = 512;

impl SqlDigestTextRetriever {
    /// Go `RetrieveLocal`, then, with `global`, `RetrieveGlobal`. A digest the
    /// summaries do not know maps to the empty text.
    pub fn retrieve(
        &self,
        _ctx: &crate::StmtContext,
        digests: impl IntoIterator<Item = String>,
        global: bool,
    ) -> Result<std::collections::HashMap<String, String>, DriverError> {
        let mut texts: std::collections::HashMap<String, String> = digests
            .into_iter()
            .map(|digest| (digest, String::new()))
            .collect();
        if texts.is_empty() {
            return Ok(texts);
        }
        // Go `RetrieveLocal`.
        if texts.len() <= FETCH_ALL_LIMIT {
            let in_values: Vec<String> = texts.keys().cloned().collect();
            let result = self.run_fetch_digest_query(false, &in_values)?;
            if result.len() == texts.len() {
                texts = result;
            } else {
                update_digest_info(&mut texts, &result);
            }
        } else {
            let result = self.run_fetch_digest_query(false, &[])?;
            update_digest_info(&mut texts, &result);
        }
        if !global {
            return Ok(texts);
        }
        // Go `RetrieveGlobal`'s pass over the whole cluster.
        let unknown: Vec<String> = texts
            .iter()
            .filter(|(_, text)| text.is_empty())
            .map(|(digest, _)| digest.clone())
            .collect();
        if unknown.is_empty() {
            return Ok(texts);
        }
        let result = if texts.len() <= FETCH_ALL_LIMIT {
            self.run_fetch_digest_query(true, &unknown)?
        } else {
            self.run_fetch_digest_query(true, &[])?
        };
        update_digest_info(&mut texts, &result);
        Ok(texts)
    }

    /// Go `runFetchDigestQuery`: `statements_summary` and its history (the
    /// `cluster_` tables when `query_global`), unioned so a digest moved to
    /// the history is still found, filtered to `in_values` when given.
    fn run_fetch_digest_query(
        &self,
        query_global: bool,
        in_values: &[String],
    ) -> Result<std::collections::HashMap<String, String>, DriverError> {
        let mut stmt = if query_global {
            "select digest, digest_text from information_schema.cluster_statements_summary union distinct \
             select digest, digest_text from information_schema.cluster_statements_summary_history"
                .to_owned()
        } else {
            "select digest, digest_text from information_schema.statements_summary union distinct \
             select digest, digest_text from information_schema.statements_summary_history"
                .to_owned()
        };
        if !in_values.is_empty() {
            stmt.push_str(" where digest in (");
            stmt.push_str(&vec!["%?"; in_values.len()].join(","));
            stmt.push(')');
        }
        let arguments: Vec<tidb_sqlescape::SqlArg<'_>> = in_values
            .iter()
            .map(|value| tidb_sqlescape::SqlArg::String(value.as_bytes()))
            .collect();
        let rows = exec_restricted_sql_over(
            self.restricted_sql.as_deref(),
            tidb_txnkv::INTERNAL_TXN_OTHERS,
            &stmt,
            &arguments,
        )?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let digest = row.first()?.as_raw_bytes()?;
                let text = row.get(1)?.as_raw_bytes()?;
                Some((
                    String::from_utf8_lossy(digest).into_owned(),
                    String::from_utf8_lossy(text).into_owned(),
                ))
            })
            .collect())
    }
}

/// Go `updateDigestInfo`: a digest whose text is still unknown takes the
/// query's.
fn update_digest_info(
    texts: &mut std::collections::HashMap<String, String>,
    query_result: &std::collections::HashMap<String, String>,
) {
    for (digest, text) in texts.iter_mut() {
        if !text.is_empty() {
            continue;
        }
        if let Some(sql_text) = query_result.get(digest) {
            text.clone_from(sql_text);
        }
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
                let time = tidb_sessionexpr::get_time_from_ts(row.cur_txn_start_ts)
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
                row.mem_arbitration.map_or(Datum::Null, Datum::Real),
                // MEM_WAIT_ARBITRATE_START: Go `ts.In(tz).Format("2006-01-02 15:04:05.999")`.
                row.mem_wait_arbitrate.map_or(Datum::Null, |(ts, _)| {
                    let time = chrono::DateTime::from_timestamp_nanos(ts).with_timezone(zone);
                    let millis = time.format("%3f").to_string();
                    let millis = millis.trim_end_matches('0');
                    let base = time.format("%Y-%m-%d %H:%M:%S").to_string();
                    Datum::Bytes(if millis.is_empty() { base } else { format!("{base}.{millis}") }.into_bytes())
                }),
                // MEM_WAIT_ARBITRATE_BYTES
                row.mem_wait_arbitrate.map_or(Datum::Null, |(_, size)| Datum::Int(size)),
                Datum::Int(row.disk_bytes),
                Datum::Bytes(txn_start.into_bytes()),
                Datum::Bytes(row.resource_group.into_bytes()),
                Datum::Bytes(row.session_alias.into_bytes()),
                row.affected_rows.map_or(Datum::Null, Datum::UInt),
                // TIDB_CPU
                Datum::Int(row.cpu_usages.tidb_cpu_time),
                // TIKV_CPU
                Datum::Int(row.cpu_usages.tikv_cpu_time),
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
    syncer: Option<&tidb_domain_serverinfo::serverinfo_syncer::Syncer>,
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
    let local_id = syncer.get_local_server_info().static_info.id;
    let discovered = syncer.get_all_server_info().map_err(DriverError::unsupported)?;
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
        tidb_infoschema::infoschema_meta::memory_table_id(table_name)
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

#[cfg(test)]
mod process_row_tests {
    use super::*;

    /// Go `ProcessInfo.ToRow`: MEM_ARBITRATION is the arbitration time in
    /// seconds, MEM_WAIT_ARBITRATE_START the waited grant's start in the
    /// session zone (`2006-01-02 15:04:05.999`), MEM_WAIT_ARBITRATE_BYTES its
    /// size; all NULL for a statement that never waited.
    #[test]
    fn process_rows_carry_the_memory_arbitration_columns() {
        let zone = tidb_datatype::SessionTimeZone::utc();
        let waited = process::ProcessRow {
            mem_arbitration: Some(1.5),
            // 2026-11-05 01:02:03.120 UTC.
            mem_wait_arbitrate: Some((1_793_840_523_120_000_000, 4096)),
            ..process::ProcessRow::default()
        };
        let row = &format_process_rows(vec![waited, process::ProcessRow::default()], &zone);
        assert_eq!(row[0][10], Datum::Real(1.5));
        assert_eq!(row[0][11], Datum::Bytes(b"2026-11-05 01:02:03.12".to_vec()));
        assert_eq!(row[0][12], Datum::Int(4096));
        assert_eq!(row[1][10..13], [Datum::Null, Datum::Null, Datum::Null]);
    }
}

#[cfg(test)]
mod restricted_sql_tests {
    use super::*;

    /// Answers the request source its context carries, or fails with the
    /// statement error it holds.
    struct SourceEcho(Option<tidb_store_driver_error::sql_query_error::SqlQueryError>);

    impl tidb_sqlexec::RestrictedSqlExecutor for SourceEcho {
        fn parse_with_params(
            &self,
            _context: &dyn tidb_sqlexec::ExecutionContext,
            sql: &str,
            _arguments: &[tidb_sqlescape::SqlArg<'_>],
        ) -> tidb_sqlexec::Result<tidb_ast::Stmt> {
            Err(format!("unparsed {sql}").into())
        }

        fn exec_restricted_stmt(
            &self,
            _context: &dyn tidb_sqlexec::ExecutionContext,
            _statement: &tidb_ast::Stmt,
            _options: &[tidb_sqlexec::OptionFuncAlias],
        ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
            Err("no statement".into())
        }

        fn exec_restricted_sql(
            &self,
            context: &dyn tidb_sqlexec::ExecutionContext,
            _options: &[tidb_sqlexec::OptionFuncAlias],
            sql: &str,
            arguments: &[tidb_sqlescape::SqlArg<'_>],
        ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
            if let Some(error) = &self.0 {
                return Err(Box::new(error.clone()));
            }
            let source = context
                .as_any()
                .downcast_ref::<tikv_client::trace::TraceContext>()
                .and_then(tikv_client::util::request_source_value)
                .expect("the caller's context carries a request source");
            Ok((
                vec![vec![
                    Datum::new_string_datum(source.source_type.as_str()),
                    Datum::Int(i64::from(source.internal)),
                    Datum::new_string_datum(tidb_sqlescape::escape_sql_text(sql, arguments)?),
                ]],
                Vec::new(),
            ))
        }
    }

    /// Go `ExecRestrictedSQL(kv.WithInternalSourceType(ctx, source), nil, sql,
    /// args...)`: the executor sees an internal request of the caller's
    /// source and the escaped arguments, and its statement error reaches the
    /// caller with its own code (Go returns the error unwrapped).
    #[test]
    fn restricted_sql_runs_under_the_callers_internal_source_and_keeps_its_error() {
        let rows = exec_restricted_sql_over(
            Some(&SourceEcho(None)),
            tidb_txnkv::INTERNAL_TXN_GC,
            "SELECT %?",
            &["it's".into()],
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Datum::new_string_datum(tidb_txnkv::INTERNAL_TXN_GC),
                Datum::Int(1),
                Datum::new_string_datum("SELECT 'it\\'s'"),
            ]]
        );

        let missing = tidb_store_driver_error::sql_query_error::SqlQueryError::new(
            1146,
            *b"42S02",
            "Table 'mysql.tidb' doesn't exist",
        );
        let error = exec_restricted_sql_over(
            Some(&SourceEcho(Some(missing))),
            tidb_txnkv::INTERNAL_TXN_GC,
            "SELECT 1",
            &[],
        )
        .unwrap_err()
        .to_mysql_error();
        assert_eq!(error.code, 1146);
        assert_eq!(error.message, "Table 'mysql.tidb' doesn't exist");
    }
}
