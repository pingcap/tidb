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
        _ => None,
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
