//! The `SHOW`/`ADMIN` arms Go answers from `SimpleExec` and `ShowDDLExec`
//! without touching a user table.
//!
//! Split out of `crate::show` when that file passed the repository's
//! 2200-line ceiling. Each function is the body of one arm, so what the
//! dispatcher does is still one line per statement.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::stats_statement_service::FlushStatsDeltaTargets;
use crate::{DriverError, StmtOutput};

/// Go `SimpleExec.executeFlush`.
///
/// Most targets are accepted and do nothing observable here: the counters and
/// caches they reset are per-instance, and this node re-reads accounts from
/// the cluster rather than holding the privilege cache Go notifies. The two
/// that are NOT no-ops keep Go's exact answers.
pub(crate) fn flush_stmt(
    flush: &tidb_ast::FlushStmt,
    catalog: &mut tidb_executor::Catalog,
    stats_delta_targets: Option<FlushStatsDeltaTargets>,
) -> Result<StmtOutput, DriverError> {
    match &flush.target {
        tidb_ast::FlushTarget::Tables { read_lock, .. } => {
            if *read_lock {
                // Go returns this as a plain error, double space and all.
                return Err(DriverError::unsupported(
                    "FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot",
                ));
            }
        }
        // Go `plugin.NotifyFlush` fails for a name no loaded plugin answers
        // to, and no plugin framework runs here, so every name fails.
        tidb_ast::FlushTarget::TiDbPlugins(plugins) => {
            if let Some(name) = plugins.first() {
                return Err(DriverError::unsupported(format!(
                    "plugin '{name}' not found"
                )));
            }
        }
        // Go dumps buffered statistics deltas to stats_meta. The embedded
        // tier derives the net count from its row image and applies the
        // committed modification count captured by DML.
        tidb_ast::FlushTarget::StatsDelta { .. } => match stats_delta_targets {
            Some(FlushStatsDeltaTargets::All) => catalog.flush_stats_delta(),
            Some(FlushStatsDeltaTargets::Tables(ids)) if !ids.is_empty() => {
                catalog.flush_stats_delta_for(&ids);
            }
            _ => {}
        },
        tidb_ast::FlushTarget::ClientErrorsSummary => {
            tidb_error::tidb::infoschema::flush_stats();
        }
        tidb_ast::FlushTarget::Status
        | tidb_ast::FlushTarget::Privileges
        | tidb_ast::FlushTarget::Hosts
        | tidb_ast::FlushTarget::Logs(_) => {}
    }
    Ok(StmtOutput::Done(true))
}

/// Go `ShowDDLExec`'s six columns. The rows come from the session, which owns
/// the node identity and the followed schema version.
pub(crate) fn show_ddl_output(rows: &[Vec<Datum>]) -> StmtOutput {
    let varchar = |size: i64| FieldType::new(FieldTypeCode::Varchar).with_flen(size);
    StmtOutput::Rows {
        columns: vec![
            (
                "SCHEMA_VER".to_owned(),
                FieldType::new(FieldTypeCode::LongLong).with_flen(4),
            ),
            ("OWNER_ID".to_owned(), varchar(64)),
            ("OWNER_ADDRESS".to_owned(), varchar(32)),
            ("RUNNING_JOBS".to_owned(), varchar(256)),
            ("SELF_ID".to_owned(), varchar(64)),
            ("QUERY".to_owned(), varchar(256)),
        ],
        rows: rows.to_vec(),
    }
}

/// Go `ShowExec.fetchShowMasterStatus`: one row naming TiDB's pseudo binlog
/// file and the CURRENT transaction's start timestamp as the position, with
/// the three replication columns empty. Tools that call this to fence a dump
/// read the position.
pub(crate) fn master_status_output(position: i64) -> StmtOutput {
    let varchar = || FieldType::new(FieldTypeCode::Varchar);
    StmtOutput::Rows {
        columns: vec![
            ("File".to_owned(), varchar()),
            (
                "Position".to_owned(),
                FieldType::new(FieldTypeCode::LongLong),
            ),
            ("Binlog_Do_DB".to_owned(), varchar()),
            ("Binlog_Ignore_DB".to_owned(), varchar()),
            ("Executed_Gtid_Set".to_owned(), varchar()),
        ],
        rows: vec![vec![
            Datum::Bytes(b"tidb-binlog".to_vec()),
            Datum::Int(position),
            Datum::Bytes(Vec::new()),
            Datum::Bytes(Vec::new()),
            Datum::Bytes(Vec::new()),
        ]],
    }
}

/// Go's two `SHOW` inspections that answer their column list and no rows:
/// `fetchShowPlugins` over an empty `plugin.GetAll()`, and `ShowProfiles`,
/// whose arm in Go is literally `// empty result`.
///
/// `None` means the kind is some other inspection, which the caller handles.
pub(crate) fn inspection_output(kind: tidb_ast::ShowInspectionKind) -> Option<StmtOutput> {
    match kind {
        tidb_ast::ShowInspectionKind::Plugins => Some(crate::show::text_columns_output(&[
            "Name", "Status", "Type", "Library", "License", "Version",
        ])),
        tidb_ast::ShowInspectionKind::Profiles => Some(StmtOutput::Rows {
            columns: vec![
                ("Query_ID".to_owned(), FieldType::new(FieldTypeCode::Long)),
                ("Duration".to_owned(), FieldType::new(FieldTypeCode::Double)),
                ("Query".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
            ],
            rows: Vec::new(),
        }),
        // Go `ShowExec.fetchShowTriggers`/`fetchShowEvents`/`fetchShowProcedureStatus`/
        // `fetchShowFunctionStatus` are literal `return nil` bodies — the schema
        // comes from the plan, the rows are always empty, and the client sees an
        // empty result set. Same for the BRIE/import job listings, which read
        // their system tables this build does not populate, and `SHOW REPLICA
        // STATUS`, which answers nothing without replication.
        tidb_ast::ShowInspectionKind::Triggers => Some(StmtOutput::Rows {
            columns: crate::show_admin::trigger_columns(),
            rows: Vec::new(),
        }),
        tidb_ast::ShowInspectionKind::ProcedureStatus | tidb_ast::ShowInspectionKind::FunctionStatus => {
            Some(StmtOutput::Rows {
                columns: crate::show_admin::routine_columns(),
                rows: Vec::new(),
            })
        }
        tidb_ast::ShowInspectionKind::Events => Some(StmtOutput::Rows {
            columns: crate::show_admin::event_columns(),
            rows: Vec::new(),
        }),
        tidb_ast::ShowInspectionKind::Backups | tidb_ast::ShowInspectionKind::Restores => {
            Some(StmtOutput::Rows {
                // Go `buildShowSchema` for `ShowBackups`/`ShowRestores`.
                columns: vec![
                    ("Id".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                    ("Destination".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                    ("State".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                    ("Progress".to_owned(), FieldType::new(FieldTypeCode::Double)),
                    ("Queue_time".to_owned(), FieldType::new(FieldTypeCode::Datetime)),
                    ("Execution_time".to_owned(), FieldType::new(FieldTypeCode::Datetime)),
                    ("Finish_time".to_owned(), FieldType::new(FieldTypeCode::Datetime)),
                    ("Connection".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                    ("Message".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                ],
                rows: Vec::new(),
            })
        }
        tidb_ast::ShowInspectionKind::Imports => Some(StmtOutput::Rows {
            columns: vec![
                ("Job_ID".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ("State".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
            ],
            rows: Vec::new(),
        }),
        _ => None,
    }
}

/// The empty-result column families: `SHOW TRIGGERS`' eleven MySQL-standard
/// columns, `SHOW PROCEDURE/FUNCTION STATUS`'s eleven, `SHOW EVENTS`' fifteen.
/// With no rows the wire never sends the names, but the shapes stay faithful
/// for `EXPLAIN`-level introspection of the result schema.
fn trigger_columns() -> Vec<(String, FieldType)> {
    let varchar = || FieldType::new(FieldTypeCode::Varchar);
    ["Trigger", "Event", "Table", "Statement", "Timing", "Created", "sql_mode", "Definer", "character_set_client", "collation_connection", "Database Collation"]
        .into_iter()
        .map(|name| (name.to_owned(), varchar()))
        .collect()
}

fn routine_columns() -> Vec<(String, FieldType)> {
    let varchar = || FieldType::new(FieldTypeCode::Varchar);
    ["Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client", "collation_connection", "Database Collation"]
        .into_iter()
        .map(|name| (name.to_owned(), varchar()))
        .collect()
}

fn event_columns() -> Vec<(String, FieldType)> {
    // Go `buildShowEventsSchema`.
    let varchar = || FieldType::new(FieldTypeCode::Varchar);
    let datetime = || FieldType::new(FieldTypeCode::Datetime);
    vec![
        ("Db".to_owned(), varchar()),
        ("Name".to_owned(), varchar()),
        ("Time zone".to_owned(), varchar()),
        ("Definer".to_owned(), varchar()),
        ("Type".to_owned(), varchar()),
        ("Execute At".to_owned(), datetime()),
        ("Interval Value".to_owned(), varchar()),
        ("Interval Field".to_owned(), varchar()),
        ("Starts".to_owned(), datetime()),
        ("Ends".to_owned(), datetime()),
        ("Status".to_owned(), varchar()),
        ("Originator".to_owned(), FieldType::new(FieldTypeCode::Int24)),
        ("character_set_client".to_owned(), varchar()),
        ("collation_connection".to_owned(), varchar()),
        ("Database Collation".to_owned(), varchar()),
    ]
}

/// Go `ShowExec.fetchShowPrivileges`: the static MySQL privilege table
/// (`executor/show.go:2037` onward) followed by every registered dynamic
/// privilege as `("<NAME>", "Server Admin", "")` — `privileges.GetDynamicPrivileges()`
/// over `pkg/privilege/privileges/privileges.go:60`'s list, in stored order.
pub(crate) fn show_privileges_output() -> StmtOutput {
    const STATIC_PRIVILEGES: &[(&str, &str, &str)] = &[
        ("Alter", "Tables", "To alter the table"),
        ("Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"),
        ("Config", "Server Admin", "To use SHOW CONFIG and SET CONFIG statements"),
        ("Create", "Databases,Tables,Indexes", "To create new databases and tables"),
        ("Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"),
        ("Create role", "Server Admin", "To create new roles"),
        ("Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"),
        ("Create view", "Tables", "To create new views"),
        ("Create user", "Server Admin", "To create new users"),
        ("Delete", "Tables", "To delete existing rows"),
        ("Drop", "Databases,Tables", "To drop databases, tables, and views"),
        ("Drop role", "Server Admin", "To drop roles"),
        ("Event", "Server Admin", "To create, alter, drop and execute events"),
        ("Execute", "Functions,Procedures", "To execute stored routines"),
        ("File", "File access on server", "To read and write files on the server"),
        ("Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"),
        ("Index", "Tables", "To create or drop indexes"),
        ("Insert", "Tables", "To insert data into tables"),
        ("Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"),
        ("Process", "Server Admin", "To view the plain text of currently executing queries"),
        ("Proxy", "Server Admin", "To make proxy user possible"),
        ("References", "Databases,Tables", "To have references on tables"),
        ("Reload", "Server Admin", "To reload or refresh tables, logs and privileges"),
        ("Replication client", "Server Admin", "To ask where the slave or master servers are"),
        ("Replication slave", "Server Admin", "To read binary log events from the master"),
        ("Select", "Tables", "To retrieve rows from table"),
        ("Show databases", "Server Admin", "To see all databases with SHOW DATABASES"),
        ("Show view", "Tables", "To see views with SHOW CREATE VIEW"),
        ("Shutdown", "Server Admin", "To shut down the server"),
        ("Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."),
        ("Trigger", "Tables", "To use triggers"),
        ("Create tablespace", "Server Admin", "To create/alter/drop tablespaces"),
        ("Update", "Tables", "To update existing rows"),
        ("Usage", "Server Admin", "No privileges - allow connect only"),
    ];
    const DYNAMIC_PRIVILEGES: &[&str] = &[
        "BACKUP_ADMIN",
        "RESTORE_ADMIN",
        "SYSTEM_USER",
        "SYSTEM_VARIABLES_ADMIN",
        "ROLE_ADMIN",
        "CONNECTION_ADMIN",
        "PLACEMENT_ADMIN",
        "DASHBOARD_CLIENT",
        "RESTRICTED_TABLES_ADMIN",
        "RESTRICTED_STATUS_ADMIN",
        "RESTRICTED_VARIABLES_ADMIN",
        "RESTRICTED_USER_ADMIN",
        "RESTRICTED_CONNECTION_ADMIN",
        "RESTRICTED_REPLICA_WRITER_ADMIN",
        "RESTRICTED_PRIV_ADMIN",
        "RESTRICTED_SQL_ADMIN",
        "RESOURCE_GROUP_ADMIN",
        "RESOURCE_GROUP_USER",
        "TRAFFIC_CAPTURE_ADMIN",
        "TRAFFIC_REPLAY_ADMIN",
        "APPLICATION_PASSWORD_ADMIN",
    ];
    let varchar = || FieldType::new(FieldTypeCode::Varchar);
    let mut rows: Vec<Vec<Datum>> = STATIC_PRIVILEGES
        .iter()
        .map(|(privilege, context, comment)| {
            vec![
                Datum::Bytes(privilege.as_bytes().to_vec()),
                Datum::Bytes(context.as_bytes().to_vec()),
                Datum::Bytes(comment.as_bytes().to_vec()),
            ]
        })
        .collect();
    rows.extend(DYNAMIC_PRIVILEGES.iter().map(|privilege| {
        vec![
            Datum::Bytes(privilege.as_bytes().to_vec()),
            Datum::Bytes(b"Server Admin".to_vec()),
            Datum::Bytes(Vec::new()),
        ]
    }));
    StmtOutput::Rows {
        columns: vec![
            ("Privilege".to_owned(), varchar()),
            ("Context".to_owned(), varchar()),
            ("Comment".to_owned(), varchar()),
        ],
        rows,
    }
}

/// The `SHOW TABLE STATUS` header, with the columns Go reports as numbers
/// marked.
pub(crate) const SHOW_TABLE_STATUS_COLUMNS: &[(&str, bool)] = &[
    ("Name", false),
    ("Engine", false),
    ("Version", true),
    ("Row_format", false),
    ("Rows", true),
    ("Avg_row_length", true),
    ("Data_length", true),
    ("Max_data_length", true),
    ("Index_length", true),
    ("Data_free", true),
    ("Auto_increment", true),
    ("Create_time", false),
    ("Update_time", false),
    ("Check_time", false),
    ("Collation", false),
    ("Checksum", false),
    ("Create_options", false),
    ("Comment", false),
];

/// The domain's slow-query memory `ADMIN SHOW SLOW` reads -- Go
/// `Domain.slowQuery` (`topn_slow_query.go`), sized by
/// `in-mem-slow-query-topn-num` / `in-mem-slow-query-recent-num` with a
/// seven-day period. `ExecStmt.LogSlowQuery` lands every statement its rules
/// select here (`domain.LogSlowQuery`).
struct DomainSlowQueries {
    queries: tidb_domain::topn_slow_query::TopNSlowQueries,
    /// When `topNSlowQueryLoop`'s ten-minute ticker last removed expired
    /// entries.
    last_expired: std::time::Instant,
}

static SLOW_QUERIES: std::sync::LazyLock<std::sync::Mutex<DomainSlowQueries>> =
    std::sync::LazyLock::new(|| {
        let config = tidb_config::config_tree::config::get_global_config();
        std::sync::Mutex::new(DomainSlowQueries {
            queries: tidb_domain::topn_slow_query::TopNSlowQueries::new(
                usize::try_from(config.in_mem_slow_query_topn_num).unwrap_or(0),
                std::time::Duration::from_secs(60 * 60 * 24 * 7),
                usize::try_from(config.in_mem_slow_query_recent_num).unwrap_or(0),
            ),
            last_expired: std::time::Instant::now(),
        })
    });

/// Go `topNSlowQueryLoop`'s ticker: every ten minutes the user and internal
/// heaps drop entries older than the period.
fn remove_expired_slow_queries(store: &mut DomainSlowQueries) {
    const TICK: std::time::Duration = std::time::Duration::from_secs(600);
    if store.last_expired.elapsed() >= TICK {
        store.last_expired = std::time::Instant::now();
        store.queries.remove_expired(std::time::SystemTime::now());
    }
}

/// Go `domain.LogSlowQuery`: hands the statement to the slow-query memory.
pub fn record_slow_query(info: tidb_domain::topn_slow_query::SlowQueryInfo) {
    let mut store = SLOW_QUERIES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    remove_expired_slow_queries(&mut store);
    store.queries.append(info);
}

/// Go `Domain.ShowSlowQuery`: `QueryTop` for `ADMIN SHOW SLOW TOP`,
/// `QueryRecent` for `ADMIN SHOW SLOW RECENT`.
fn slow_query_rows(
    mode: &tidb_ast::AdminShowSlowMode,
    count: u64,
) -> Vec<tidb_domain::topn_slow_query::SlowQueryInfo> {
    let mut store = SLOW_QUERIES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    remove_expired_slow_queries(&mut store);
    let count = usize::try_from(count).unwrap_or(usize::MAX);
    match mode {
        tidb_ast::AdminShowSlowMode::Recent => store.queries.query_recent(count),
        tidb_ast::AdminShowSlowMode::Top(kind) => store.queries.query_top(count, *kind),
    }
}

impl crate::Session {
    /// go `SplitTableRegionExec` / `SplitIndexRegionExec`
    /// (`pkg/executor/split.go`): one `(TOTAL_SPLIT_REGION,
    /// SCATTER_FINISH_RATIO)` row.
    pub(crate) fn split_region_stmt(
        &mut self,
        split: &tidb_ast::SplitRegionStmt,
    ) -> Result<StmtOutput, DriverError> {
        let (database, name) = self.split_table_path(&split.table)?;
        let ctx = self.statement_context(false);
        let wait_finish = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_WAIT_SPLIT_REGION_FINISH)
            .is_ok_and(|value| value.eq_ignore_ascii_case("ON") || value == "1");
        // Go `GetSplitRegionTimeout`: `tidb_wait_split_region_timeout` seconds.
        let timeout = std::time::Duration::from_secs(
            self.vars
                .get_system(tidb_vardef::tidb_vars::TIDB_WAIT_SPLIT_REGION_TIMEOUT)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(300),
        );
        let row = self.with_catalog_mut(|catalog| {
            let catalog = &*catalog;
            match catalog.table_in(&database, &name) {
                Some(tidb_executor::TableEntry::Kv(table)) => {
                    tidb_executor::split_region::split_region(
                        split,
                        table,
                        catalog,
                        &ctx,
                        wait_finish,
                        timeout,
                    )
                }
                Some(_) => Err(DriverError::unsupported(
                    "SPLIT names a table without regions",
                )),
                None => Err(DriverError::Schema(
                    tidb_executor::SchemaErrorKind::UnknownTable(format!("{database}.{name}")),
                )),
            }
        })?;
        Ok(StmtOutput::Rows {
            columns: tidb_executor::split_region::split_region_columns(),
            rows: vec![row],
        })
    }

    /// go `ChecksumTableExec` (`pkg/executor/checksum.go`), built by
    /// `buildChecksumTable`: one coprocessor checksum request per physical
    /// table (the table's own ID plus every partition) and per public index
    /// on each, all at the statement's snapshot, folded per table by
    /// `updateChecksumResponse` (see [`tidb_executor::checksum`]). One
    /// `(Db_name, Table_name, Checksum_crc64_xor, Total_kvs, Total_bytes)`
    /// row per distinct table, the schema of `buildChecksumTableSchema`.
    pub(crate) fn admin_checksum_stmt(
        &mut self,
        tables: &[Vec<String>],
    ) -> Result<StmtOutput, DriverError> {
        let mut paths = Vec::with_capacity(tables.len());
        for path in tables {
            paths.push(self.split_table_path(path)?);
        }
        let (contexts, client) = self.with_catalog_mut(|catalog| {
            let mut contexts = Vec::with_capacity(paths.len());
            let mut client = None;
            for (database, name) in &paths {
                match catalog.table_in(database, name) {
                    // Go `checkAdminCheckTableGrammar` (`preprocess.go:908`).
                    Some(tidb_executor::TableEntry::Kv(table)) if table.is_temporary() => {
                        return Err(DriverError::OptOnTemporaryTable("admin checksum table"));
                    }
                    Some(tidb_executor::TableEntry::Kv(table)) => {
                        // Every table of a session shares one storage, so
                        // one coprocessor client and one snapshot.
                        if client.is_none() {
                            client = table.coprocessor_client();
                        }
                        contexts.push(tidb_executor::checksum::ChecksumContext::new(
                            database, table,
                        ));
                    }
                    Some(_) => {
                        return Err(DriverError::unsupported(
                            "checksum reads stored tables only",
                        ))
                    }
                    None => {
                        return Err(DriverError::Schema(
                            tidb_executor::SchemaErrorKind::UnknownTable(format!(
                                "{database}.{name}"
                            )),
                        ))
                    }
                }
            }
            Ok((contexts, client))
        })?;
        // Go `getChecksumTableConcurrency`.
        let concurrency = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_CHECKSUM_TABLE_CONCURRENCY)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(4);
        let explicit_request_source_type = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_EXPLICIT_REQUEST_SOURCE_TYPE)
            .unwrap_or_default();
        let ctx = self.statement_context(false);
        let (client, start_ts) = match client {
            Some((client, start_ts)) => (Some(client), start_ts),
            None => (None, 0),
        };
        let tables = tidb_executor::checksum::ChecksumTableExec::new(
            contexts,
            client,
            start_ts,
            explicit_request_source_type,
            &ctx,
        )
        .run(usize::try_from(concurrency).unwrap_or(1))?;
        let rows = tables
            .into_iter()
            .map(|table| {
                vec![
                    Datum::Bytes(table.db_name.into_bytes()),
                    Datum::Bytes(table.table_name.into_bytes()),
                    Datum::UInt(table.response.checksum()),
                    Datum::UInt(table.response.total_kvs()),
                    Datum::UInt(table.response.total_bytes()),
                ]
            })
            .collect();
        // Go `buildColumnWithName` flags every non-string column unsigned.
        let ulonglong = || {
            FieldType::new(FieldTypeCode::LongLong)
                .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED)
        };
        Ok(StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                ("Table_name".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                ("Checksum_crc64_xor".to_owned(), ulonglong()),
                ("Total_kvs".to_owned(), ulonglong()),
                ("Total_bytes".to_owned(), ulonglong()),
            ],
            rows,
        })
    }

    /// go `execCommandOnDDLJobs` (`pkg/executor/admin.go`): one
    /// `(JOB_ID, RESULT)` row per requested id. Every id this node holds no
    /// active job for answers the not-found error string go's
    /// `GetDDLJobById` path produces — `error: [ddl:8224]DDL Job:%d not
    /// found` — which is the only reachable shape on a node whose DDL runs
    /// to completion synchronously (no job is ever mid-flight at query
    /// time).
    pub(crate) fn ddl_job_control_stmt(
        &mut self,
        control: &tidb_ast::AdminDdlJobControlStmt,
    ) -> Result<StmtOutput, DriverError> {
        let varchar = || FieldType::new(FieldTypeCode::Varchar);
        let rows = control
            .job_ids
            .iter()
            .map(|job_id| {
                vec![
                    Datum::Bytes(job_id.to_string().into_bytes()),
                    Datum::Bytes(
                        format!("error: [ddl:8224]DDL Job:{job_id} not found").into_bytes(),
                    ),
                ]
            })
            .collect();
        Ok(StmtOutput::Rows {
            columns: vec![
                ("JOB_ID".to_owned(), varchar()),
                ("RESULT".to_owned(), varchar()),
            ],
            rows,
        })
    }

    /// go `buildShowSlowSchema` (`pkg/planner/core/planbuilder.go:3340`)
    /// rendered over the recorded slow statements.
    pub(crate) fn admin_show_slow_stmt(
        &mut self,
        show: &tidb_ast::AdminShowSlowStmt,
    ) -> Result<StmtOutput, DriverError> {
        use tidb_datatype::{core_time_from_datetime, Time, TimeType};
        let records = slow_query_rows(&show.mode, show.count);
        let varchar = || FieldType::new(FieldTypeCode::Varchar);
        let rows = records
            .into_iter()
            .map(|record| {
                let start: chrono::DateTime<chrono::Local> = record
                    .start
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                    .into();
                let nanos = record.duration.subsec_nanos() as i64
                    + i64::try_from(record.duration.as_secs()).unwrap_or(0) * 1_000_000_000;
                vec![
                    // The text carries a byte that is not UTF-8 as its
                    // `raw_bytes` escape; Go's string holds the byte.
                    Datum::Bytes(tidb_hack::raw_bytes::str_to_bytes(&record.sql).into_owned()),
                    Datum::new_time(
                        Time::new(
                            core_time_from_datetime(start),
                            TimeType::Timestamp,
                            6,
                        )
                        .unwrap_or_else(|_| {
                            Time::new(
                                core_time_from_datetime(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH.with_timezone(&chrono::Local)),
                                TimeType::Timestamp,
                                0,
                            )
                            .expect("fsp 0 is valid")
                        }),
                    ),
                    Datum::new_duration(
                        tidb_datatype::MySqlDuration::from_nanoseconds(nanos, 6)
                            .unwrap_or_else(|_| {
                                tidb_datatype::MySqlDuration::from_nanoseconds(0, 0)
                                    .expect("fsp 0 is valid")
                            }),
                    ),
                    Datum::Bytes(record.detail.into_bytes()),
                    Datum::Int(i64::from(record.succ)),
                    Datum::UInt(record.conn_id),
                    Datum::UInt(record.txn_ts),
                    Datum::Bytes(record.user.into_bytes()),
                    Datum::Bytes(record.db.into_bytes()),
                    Datum::Bytes(record.table_ids.into_bytes()),
                    Datum::Bytes(record.index_names.into_bytes()),
                    Datum::Int(i64::from(record.internal)),
                    Datum::Bytes(record.digest.into_bytes()),
                    Datum::Bytes(record.sess_alias.into_bytes()),
                ]
            })
            .collect();
        Ok(StmtOutput::Rows {
            columns: vec![
                ("SQL".to_owned(), varchar()),
                (
                    "START".to_owned(),
                    FieldType::new(FieldTypeCode::Timestamp),
                ),
                ("DURATION".to_owned(), FieldType::new(FieldTypeCode::Duration)),
                ("DETAILS".to_owned(), varchar()),
                ("SUCC".to_owned(), FieldType::new(FieldTypeCode::Tiny)),
                ("CONN_ID".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                (
                    "TRANSACTION_TS".to_owned(),
                    FieldType::new(FieldTypeCode::LongLong),
                ),
                ("USER".to_owned(), varchar()),
                ("DB".to_owned(), varchar()),
                ("TABLE_IDS".to_owned(), varchar()),
                ("INDEX_IDS".to_owned(), varchar()),
                ("INTERNAL".to_owned(), FieldType::new(FieldTypeCode::Tiny)),
                ("DIGEST".to_owned(), varchar()),
                ("SESSION_ALIAS".to_owned(), varchar()),
            ],
            rows,
        })
    }

    /// Go `ShowDDLJobQueriesExec` / `ShowDDLJobQueriesWithRangeExec`: the
    /// queries of the named jobs, searched in the running jobs and the newest
    /// ten history jobs (the range form: the newest `offset + count`).
    pub(crate) fn admin_show_ddl_job_queries_stmt(
        &mut self,
        show: &tidb_ast::AdminShowDdlJobQueriesStmt,
    ) -> Result<StmtOutput, DriverError> {
        /// Go `ddl.DefNumHistoryJobs`.
        const DEF_NUM_HISTORY_JOBS: usize = 10;
        let history_limit = match show {
            tidb_ast::AdminShowDdlJobQueriesStmt::JobIds(_) => DEF_NUM_HISTORY_JOBS,
            tidb_ast::AdminShowDdlJobQueriesStmt::Limit { offset, count } => {
                usize::try_from(offset.saturating_add(*count)).unwrap_or(usize::MAX)
            }
        };
        let (running, history) = self.all_ddl_jobs()?;
        // Deduplicated by job id, running jobs first.
        let mut seen = std::collections::BTreeSet::new();
        let jobs = running
            .iter()
            .chain(history.iter().take(history_limit))
            .filter(|job| seen.insert(job.id))
            .collect::<Vec<_>>();
        let varchar = || FieldType::new(FieldTypeCode::Varchar);
        let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
        match show {
            tidb_ast::AdminShowDdlJobQueriesStmt::JobIds(ids) => {
                // Go's executor answers nothing once the named ids are at
                // least as many as the jobs it found.
                let rows = if ids.len() >= jobs.len() {
                    Vec::new()
                } else {
                    ids.iter()
                        .flat_map(|id| {
                            jobs.iter()
                                .filter(move |job| job.id == *id)
                                .map(|job| vec![text(&job.query.to_utf8_lossy_go())])
                        })
                        .collect()
                };
                Ok(StmtOutput::Rows {
                    columns: vec![("QUERY".to_owned(), varchar())],
                    rows,
                })
            }
            tidb_ast::AdminShowDdlJobQueriesStmt::Limit { offset, count } => {
                let offset = usize::try_from(*offset).unwrap_or(usize::MAX);
                let end = offset.saturating_add(usize::try_from(*count).unwrap_or(usize::MAX));
                let rows = if offset > jobs.len() {
                    Vec::new()
                } else {
                    jobs.iter()
                        .enumerate()
                        .skip(offset)
                        .take_while(|(index, _)| *index < end)
                        .map(|(_, job)| vec![text(&job.id.to_string()), text(&job.query.to_utf8_lossy_go())])
                        .collect()
                };
                Ok(StmtOutput::Rows {
                    columns: vec![
                        ("JOB_ID".to_owned(), varchar()),
                        ("QUERY".to_owned(), varchar()),
                    ],
                    rows,
                })
            }
        }
    }

    /// Go `ShowDDLJobsExec`: the newest `job_number` jobs of the DDL history
    /// (Go's `DefNumHistoryJobs`, ten, when the statement names none), each
    /// rendered by `appendJobToChunk` with its COMMENTS, a multi-schema
    /// change followed by its sub-jobs. The statement checks no table
    /// privilege.
    pub(crate) fn admin_show_ddl_jobs_stmt(
        &mut self,
        show: &tidb_ast::AdminShowDdlJobsStmt,
    ) -> Result<StmtOutput, DriverError> {
        /// Go `ddl.DefNumHistoryJobs`.
        const DEF_NUM_HISTORY_JOBS: usize = 10;
        let job_number = if show.job_number > 0 {
            usize::try_from(show.job_number).unwrap_or(usize::MAX)
        } else {
            DEF_NUM_HISTORY_JOBS
        };
        // Go `ShowDDLJobsExec.Next`: every running job, then at most
        // `job_number` history jobs.
        let (running, history) = self.all_ddl_jobs()?;
        let rows = self.with_planning_catalog(|catalog| {
            Ok(running
                .iter()
                .chain(history.iter().take(job_number))
                .flat_map(|job| crate::ddl_jobs::ddl_job_rows(job, true, catalog))
                .collect::<Vec<_>>())
        })?;
        let varchar = || FieldType::new(FieldTypeCode::Varchar);
        let longlong = || FieldType::new(FieldTypeCode::LongLong);
        let output = StmtOutput::Rows {
            columns: vec![
                ("JOB_ID".to_owned(), longlong()),
                ("DB_NAME".to_owned(), varchar()),
                ("TABLE_NAME".to_owned(), varchar()),
                ("JOB_TYPE".to_owned(), varchar()),
                ("SCHEMA_STATE".to_owned(), varchar()),
                ("SCHEMA_ID".to_owned(), longlong()),
                ("TABLE_ID".to_owned(), longlong()),
                ("ROW_COUNT".to_owned(), longlong()),
                (
                    "CREATE_TIME".to_owned(),
                    FieldType::new(FieldTypeCode::Datetime),
                ),
                (
                    "START_TIME".to_owned(),
                    FieldType::new(FieldTypeCode::Datetime),
                ),
                (
                    "END_TIME".to_owned(),
                    FieldType::new(FieldTypeCode::Datetime),
                ),
                ("STATE".to_owned(), varchar()),
                ("COMMENTS".to_owned(), varchar()),
            ],
            rows,
        };
        match &show.where_clause {
            None => Ok(output),
            Some(expr) => {
                let (like_pattern, where_clause) = (None, Some(expr));
                self.filter_show_output(output, like_pattern, where_clause)
            }
        }
    }
}

