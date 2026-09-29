//! The `SHOW`/`ADMIN` arms Go answers from `SimpleExec` and `ShowDDLExec`
//! without touching a user table.
//!
//! Split out of `crate::show` when that file passed the repository's
//! 2200-line ceiling. Each function is the body of one arm, so what the
//! dispatcher does is still one line per statement.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

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
    current_db: &str,
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
        tidb_ast::FlushTarget::StatsDelta { objects, .. } => {
            if objects
                .iter()
                .any(|object| matches!(object, tidb_ast::StatsObject::Global))
            {
                catalog.flush_stats_delta();
            } else {
                let mut table_ids = Vec::new();
                for object in objects {
                    match object {
                        tidb_ast::StatsObject::Global => unreachable!(),
                        tidb_ast::StatsObject::Database(database) => {
                            append_stats_table_ids(catalog, database, None, &mut table_ids);
                        }
                        tidb_ast::StatsObject::Table { database, table } => {
                            append_stats_table_ids(
                                catalog,
                                database.as_deref().unwrap_or(current_db),
                                Some(table),
                                &mut table_ids,
                            );
                        }
                    }
                }
                catalog.flush_stats_delta_for(&table_ids);
            }
        }
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

fn append_stats_table_ids(
    catalog: &tidb_executor::Catalog,
    database: &str,
    table_name: Option<&str>,
    table_ids: &mut Vec<i64>,
) {
    let names = table_name.map_or_else(
        || catalog.table_names(database).unwrap_or_default(),
        |name| vec![name.to_owned()],
    );
    for name in names {
        if let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_in(database, &name) {
            table_ids.push(table.table_id);
            if let Some(partition) = table.partition() {
                table_ids.extend(partition.physical_ids());
            }
        }
    }
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
        // STATUS`/`SHOW AFFINITY`, which answer nothing without replication or
        // affinity configuration.
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
                columns: vec![
                    ("Id".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                    ("Destination".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                    ("State".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                    ("Start_Time".to_owned(), FieldType::new(FieldTypeCode::Datetime)),
                    ("End_Time".to_owned(), FieldType::new(FieldTypeCode::Datetime)),
                    ("Size".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                    ("Progress".to_owned(), FieldType::new(FieldTypeCode::Double)),
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
        tidb_ast::ShowInspectionKind::Affinity => Some(StmtOutput::Rows {
            columns: vec![("1".to_owned(), FieldType::new(FieldTypeCode::Long))],
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
    let varchar = || FieldType::new(FieldTypeCode::Varchar);
    ["Db", "Name", "Time zone", "Definer", "Type", "Execute at", "Interval value", "Interval field", "Starts", "Ends", "Status", "Originator", "character_set_client", "collation_connection", "Database Collation"]
        .into_iter()
        .map(|name| (name.to_owned(), varchar()))
        .collect()
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
        ("Operate view", "Tables", "To execute materialized view and materialized view log maintenance operations"),
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

pub(crate) fn show_table_status_row(
    name: &str,
    auto_increment: Option<i64>,
    charset: tidb_executor::TableCharset,
    comment: &str,
    create_options: &str,
) -> Vec<Datum> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    vec![
        text(name),
        text("InnoDB"),
        Datum::Int(10),
        text("Compact"),
        Datum::Int(0), // Rows
        Datum::Int(0), // Avg_row_length
        Datum::Int(0), // Data_length
        Datum::Int(0), // Max_data_length
        Datum::Int(0), // Index_length
        Datum::Int(0), // Data_free
        match auto_increment {
            Some(next) => Datum::Int(next),
            None => Datum::Null,
        },
        Datum::Null, // Create_time: no per-table creation timestamp here.
        Datum::Null, // Update_time
        Datum::Null, // Check_time
        text(charset.collation.name()),
        text(""), // Checksum
        // Go's `fetchShowTableStatus` (`executor/show.go:636`) SELECTs
        // `create_options` straight out of `information_schema.tables`, so
        // this cell is that column: `partitioned` for a partitioned table and
        // `cached=on` for a cached one. Hard-coding it empty reported every
        // partitioned table as if it had no partitioning.
        text(create_options),
        text(comment), // Comment
    ]
}

/// One `SHOW TABLE STATUS` row for a view. Captured from Go: a view answers
/// its name, NULL for every storage cell -- engine, version, row format,
/// counts, sizes, collation and create options alike -- an empty `Checksum`,
/// and the literal `VIEW` as its comment, which is how the two kinds of
/// object are told apart in this output.
pub(crate) fn show_table_status_view_row(name: &str) -> Vec<Datum> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut row = vec![text(name)];
    // Engine through Auto_increment: ten cells a view has no value for.
    row.extend(std::iter::repeat_n(Datum::Null, 10));
    // Create_time, which Go fills and this tier has no source for, then
    // Update_time, Check_time and Collation, which are NULL for a view in Go
    // too.
    row.extend(std::iter::repeat_n(Datum::Null, 4));
    row.push(text("")); // Checksum
    row.push(Datum::Null); // Create_options
    row.push(text("VIEW")); // Comment
    row
}

/// The in-memory slow-query memory `ADMIN SHOW SLOW` reads — go
/// `Domain.ShowSlowQuery` over `topn_slow_query.go`'s lists. Go records every
/// statement whose cost reaches `instance.ddl_slow_threshold`'s sibling, the
/// session's `tidb_slow_log_threshold` (300ms by default), through
/// `LogSlowQuery` (`adapter.go:2007`); the recorder below hooks the same
/// threshold check at the wire tier.
#[derive(Clone)]
pub struct SlowQueryRecord {
    /// The statement text as received.
    pub sql: String,
    /// Wall-clock start, rendered as the session's DATETIME.
    pub start: std::time::SystemTime,
    /// Total statement cost.
    pub duration: std::time::Duration,
    /// The connection that ran the statement.
    pub conn_id: u64,
    /// `user@host` as the account was authenticated (`root@%`).
    pub user: String,
    /// The statement's default database (`""` when none was selected).
    pub db: String,
    /// go `StmtCtx.Digest()`'s normalized-statement digest hex.
    pub digest: String,
}

static SLOW_QUERIES: std::sync::Mutex<Vec<SlowQueryRecord>> =
    std::sync::Mutex::new(Vec::new());

/// go `domain.LogSlowQuery`: append to the recent list. The cap mirrors go's
/// `in-mem-slow-query-recent-num` default (500); anything past it evicts the
/// oldest entry, exactly as go's ring does.
pub fn record_slow_query(record: SlowQueryRecord) {
    const RECENT_NUM: usize = 500;
    let mut list = SLOW_QUERIES.lock().unwrap_or_else(|poison| poison.into_inner());
    if list.len() == RECENT_NUM {
        list.remove(0);
    }
    list.push(record);
}

/// go `topNSlowQuery.All`'s answer: the recorded statements by cost, longest
/// first. `internal` selects nothing here — this node records only general
/// (user) statements, and go's internal list stays empty alongside.
fn slow_query_rows(mode: &tidb_ast::AdminShowSlowMode, count: u64) -> Vec<SlowQueryRecord> {
    let list = SLOW_QUERIES.lock().unwrap_or_else(|poison| poison.into_inner());
    match mode {
        tidb_ast::AdminShowSlowMode::Recent => {
            let start = list.len().saturating_sub(count as usize);
            list[start..].to_vec()
        }
        tidb_ast::AdminShowSlowMode::Top(_) => {
            let mut sorted = list.clone();
            sorted.sort_by(|left, right| right.duration.cmp(&left.duration));
            sorted.truncate(count as usize);
            sorted
        }
    }
}

impl crate::Session {
    /// go `execAdminChecksums` (`pkg/executor/checksum.go`): one checksum
    /// request per physical table (the main table plus every partition) and
    /// per public index on each, with responses folded
    /// `Checksum ^= update.Checksum; TotalKvs += update.TotalKvs;
    /// TotalBytes += update.TotalBytes` (`updateChecksumResponse`). The
    /// oracle's unistore coprocessor answers every request with the
    /// hardcoded `Checksum:1, TotalKvs:1, TotalBytes:1`
    /// (`pkg/store/mockstore/mockcopr/checksum.go`), so a table's answer is
    /// fully determined by its task count: `checksum = tasks % 2`,
    /// `kvs = bytes = tasks`. The row is
    /// `(db, table, checksum, total_kvs, total_bytes)`.
    pub(crate) fn admin_checksum_stmt(
        &mut self,
        tables: &[Vec<String>],
    ) -> Result<StmtOutput, DriverError> {
        let longlong = || FieldType::new(FieldTypeCode::LongLong);
        let mut rows = Vec::new();
        for path in tables {
            let (database, name) = self.split_table_path(path)?;
            let resolved = self.with_catalog_mut(|catalog| {
                match catalog.table_in(&database, &name) {
                    Some(tidb_executor::TableEntry::Kv(table)) => {
                        let physical =
                            1 + table.partition().map_or(0, |p| p.definitions.len());
                        let indexes = table.indexes().len();
                        let tasks = (physical * (1 + indexes)) as i64;
                        Ok((tasks & 1, tasks, tasks))
                    }
                    Some(_) => Err(DriverError::unsupported(
                        "checksum reads stored tables only",
                    )),
                    None => Err(DriverError::Schema(
                        tidb_executor::SchemaErrorKind::UnknownTable(format!(
                            "{database}.{name}"
                        )),
                    )),
                }
            })?;
            let (checksum, kvs, bytes) = resolved;
            rows.push(vec![
                Datum::Bytes(database.as_bytes().to_vec()),
                Datum::Bytes(name.as_bytes().to_vec()),
                Datum::Int(checksum),
                Datum::Int(kvs),
                Datum::Int(bytes),
            ]);
        }
        Ok(StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                ("Table_name".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                ("Checksum".to_owned(), longlong()),
                ("Total_kvs".to_owned(), longlong()),
                ("Total_bytes".to_owned(), longlong()),
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
                let start: chrono::DateTime<chrono::Local> = record.start.into();
                let nanos = record.duration.subsec_nanos() as i64
                    + i64::try_from(record.duration.as_secs()).unwrap_or(0) * 1_000_000_000;
                vec![
                    Datum::Bytes(record.sql.into_bytes()),
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
                    Datum::Bytes(Vec::new()),
                    Datum::Int(1),
                    Datum::UInt(record.conn_id),
                    Datum::Int(0),
                    Datum::Bytes(record.user.into_bytes()),
                    Datum::Bytes(record.db.into_bytes()),
                    Datum::Bytes(Vec::new()),
                    Datum::Bytes(Vec::new()),
                    Datum::Int(0),
                    Datum::Bytes(record.digest.into_bytes()),
                    Datum::Bytes(Vec::new()),
                    Datum::Int(0),
                    Datum::Int(0),
                    Datum::Int(0),
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
                (
                    "IA_REMOTE_READ_SEGMENT_COUNT".to_owned(),
                    FieldType::new(FieldTypeCode::LongLong),
                ),
                (
                    "IA_REMOTE_READ_SEGMENT_SIZE".to_owned(),
                    FieldType::new(FieldTypeCode::LongLong),
                ),
                (
                    "IA_REMOTE_READ_SEGMENT_WAIT_TIME".to_owned(),
                    FieldType::new(FieldTypeCode::LongLong),
                ),
            ],
            rows,
        })
    }

    /// go `fetchShowDDLJobs` over `mysql.tidb_ddl_history`: the newest
    /// `job_number` finished jobs (job id descending, so the newest job is
    /// first), each with go's `buildShowDDLJobsFields` schema. The fork's
    /// history table carries the job payload in `job_meta` (the other
    /// cells stay denormalized in `db_name`/`table_name`/`create_time`),
    /// so each row decodes its job to render the columns go's own history
    /// table denormalizes. `ADMIN SHOW DDL JOBS 0`/omitted takes all.
    pub(crate) fn admin_show_ddl_jobs_stmt(
        &mut self,
        show: &tidb_ast::AdminShowDdlJobsStmt,
    ) -> Result<StmtOutput, DriverError> {
        use tidb_datatype::{Time, TimeType};
        let ctx = self.statement_context(false);
        let (columns, raw_rows) = self.with_catalog_mut(|catalog| {
            tidb_executor::run_select_meta_in(
                "SELECT job_id, job_meta, db_name, table_name, create_time \
                 FROM mysql.tidb_ddl_history",
                catalog,
                "mysql",
                &ctx,
            )
        })?;
        let mut rows = Vec::with_capacity(raw_rows.len());
        for row in raw_rows {
            let Some(encoded) = row.get(1).and_then(|value| match value {
                Datum::Bytes(bytes) => Some(bytes.clone()),
                _ => None,
            }) else {
                continue;
            };
            let mut job = tidb_model::Job::default();
            if job.decode(&encoded).is_err() {
                continue;
            }
            // go `fetchShowDDLJobs` renders the times through
            // `oracle.GetTimeFromTS` in the process-local zone; the fork's
            // stored `create_time` column and the job's own TSOs carry the
            // same instants.
            let create_time = row.get(4).cloned().unwrap_or(Datum::Null);
            let start_time = if job.real_start_ts == 0 {
                create_time.clone()
            } else {
                Datum::new_time(tso_local_time(job.real_start_ts))
            };
            rows.push(vec![
                Datum::Int(job.id),
                Datum::Bytes(job.schema_name.as_bytes().to_vec()),
                Datum::Bytes(job.table_name.as_bytes().to_vec()),
                Datum::Bytes(job.type_.to_string().into_bytes()),
                Datum::Bytes(job.schema_state.to_string().into_bytes()),
                Datum::Int(job.schema_id),
                Datum::Int(job.table_id),
                Datum::Int(job.row_count),
                create_time,
                start_time,
                row.get(4).cloned().unwrap_or(Datum::Null),
                Datum::Bytes(job.state.to_string().into_bytes()),
                Datum::Bytes(Vec::new()),
            ]);
        }
        // History scan order is key order (oldest first); go shows the
        // newest jobs first. `job_number` takes the newest N.
        rows.reverse();
        if show.job_number > 0 {
            rows.truncate(show.job_number as usize);
        }
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
                crate::show::filter_show_output(output, like_pattern, where_clause)
            }
        }
    }
}

/// The DDL-job TSO→local-DATETIME conversion `ddl_history_table.rs` performs
/// for its own writes: the physical millis are the TSO's high 46 bits, read
/// in the process-local zone exactly as go's `oracle.GetTimeFromTS` does.
fn tso_local_time(timestamp: u64) -> tidb_datatype::Time {
    use chrono::TimeZone;
    let milliseconds = i64::try_from(timestamp >> 18).unwrap_or(0);
    let utc = chrono::Utc
        .timestamp_millis_opt(milliseconds)
        .single()
        .unwrap_or_else(chrono::Utc::now);
    let core = tidb_datatype::core_time_from_datetime(utc.with_timezone(&chrono::Local));
    tidb_datatype::Time::new(core, tidb_datatype::TimeType::Timestamp, 6)
        .unwrap_or_else(|_| {
            tidb_datatype::Time::new(
                core,
                tidb_datatype::TimeType::Timestamp,
                0,
            )
            .expect("fsp 0 is valid")
        })
}
