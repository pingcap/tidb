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

//! The session: the single entry point that owns catalog state and runs SQL
//! statements through the wired parse -> plan -> execute pipeline.
//!
//! This is the seam of Go's `pkg/session` `session.ExecuteStmt`: one object a
//! client holds, dispatching each statement kind to its executor path.
//!
//! SEED SCOPE: [`Session::run`] dispatches `SELECT` (rows), `INSERT` (affected
//! count), and `CREATE TABLE` over the session's [`Catalog`]. DEFERRED
//! (documented): transactions (autocommit is implicit and immediate --
//! `BEGIN`/`COMMIT`/`ROLLBACK` land with the txnkv integration), session
//! variables (`SET`), prepared statements, the MySQL wire protocol, privileges,
//! and every other statement kind. Statements are currently parsed twice (once
//! here for dispatch, once in the driver's runner) -- a wiring simplification
//! to remove when the driver's runners take parsed statements.

use std::sync::{Arc, Mutex, MutexGuard};

use tidb_ast::{DdlStmt, DmlStmt, SessionStmt, Stmt};
use tidb_datatype::{Datum, FieldType};
use tidb_executor::{Catalog, DriverError};
use tidb_executor::{SchemaErrorKind, DEFAULT_DATABASE};

/// The result of running one statement.
#[derive(Debug, PartialEq)]
pub enum StmtResult {
    /// A query's result rows.
    Rows(Vec<Vec<Datum>>),
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// The result of running one statement, with wire-facing column metadata.
///
/// [`StmtResult::Rows`] loses column names/types; a server front end needs one
/// `(name, type)` per result column to build protocol column definitions, so
/// [`Session::run_with_columns`] returns this richer shape instead.
#[derive(Debug, PartialEq)]
pub enum StmtOutput {
    /// A query's result columns and rows.
    Rows {
        /// One `(display name, field type)` per output column.
        columns: Vec<(String, FieldType)>,
        /// The result rows (one `Datum` per column).
        rows: Vec<Vec<Datum>>,
    },
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// What kind of answer a statement produces, decided by parsing alone.
///
/// The MySQL text protocol answers a query with a result set and a write or
/// DDL with an OK packet, so a server front end must know which shape a
/// statement takes *before* running it (running it twice would duplicate the
/// write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtKind {
    /// A query: answers with rows.
    Query,
    /// A DML write or DDL: answers with an affected-row count.
    Write,
}

/// A process-wide catalog shared by every session, as Go's domain-owned
/// `infoschema` is shared by every session of a TiDB instance.
pub type SharedCatalog = Arc<Mutex<Catalog>>;

/// A session: runs statements against a catalog shared with its peers.
///
/// Go sessions borrow the process's schema state rather than owning private
/// copies, so a table one connection creates is visible to the others. This
/// mirrors that with a shared, mutex-guarded catalog; the statement-level lock
/// stands in for Go's schema-version/lease machinery, which is a separate
/// tier (documented deferral).
pub struct Session {
    catalog: SharedCatalog,
    /// The open transaction, if any.
    txn: Option<Transaction>,
    /// The session's system and user variables.
    vars: SessionVars,
    /// The warnings the last statement produced, which Go keeps in
    /// `StmtCtx.warnings` and `SHOW WARNINGS` reads.
    warnings: Vec<SqlWarning>,
    /// Go `SessionVars.PrevLastInsertID`: the id `LAST_INSERT_ID()` reports,
    /// which only a statement that ALLOCATED an auto value updates.
    last_insert_id: u64,
    /// The id the last statement allocated, which the OK packet carries and
    /// which is 0 for a statement that allocated nothing.
    statement_insert_id: u64,
    /// Go `SessionVars.CurrentDB`: the schema an unqualified name resolves in.
    /// Empty means no database is selected, which is Go's `ErrNoDB` case.
    current_db: String,
}

impl Default for Session {
    /// A session on its own empty catalog, with `test` selected as a fresh
    /// TiDB connection has.
    fn default() -> Self {
        Session {
            catalog: SharedCatalog::default(),
            txn: None,
            vars: SessionVars::new(),
            warnings: Vec::new(),
            last_insert_id: 0,
            statement_insert_id: 0,
            current_db: DEFAULT_DATABASE.to_owned(),
        }
    }
}

/// An open transaction's state.
///
/// Go stages a transaction's writes in a `kv.MemBuffer` over a read snapshot
/// and flushes them at commit; this stages them in a private copy of the
/// catalog taken at `BEGIN`, so the session reads its own writes while its
/// peers see nothing until commit.
///
/// `base_version` is the shared catalog's mutation counter at `BEGIN`. If it
/// moved by commit time, someone else wrote, and the commit is refused rather
/// than overwriting their work -- the outcome Go gets from TiKV's optimistic
/// conflict check, though Go compares the WRITTEN KEYS while this compares the
/// whole catalog, so this refuses some commits Go would allow (documented).
struct Transaction {
    working: Catalog,
    base_version: u64,
}

pub use tidb_executor::TxnErrorKind;

pub mod infoschema;
pub mod sysvar;
pub mod vars;
pub use vars::{SessionVars, VarError};

/// Maps a variable error onto the driver error the wire layer renders.
fn var_error(error: VarError) -> DriverError {
    DriverError::Var(match error {
        VarError::UnknownSystemVariable(name) => {
            tidb_executor::VarErrorKind::UnknownSystemVariable(name)
        }
        VarError::ReadOnlyVariable(name) => tidb_executor::VarErrorKind::ReadOnlyVariable(name),
        VarError::WrongTypeForVar(name) => tidb_executor::VarErrorKind::WrongTypeForVar(name),
        VarError::WrongValueForVar(name, value) => {
            tidb_executor::VarErrorKind::WrongValueForVar(name, value)
        }
    })
}

/// Go `mysql.DefaultCharset` and the collation `getDefaultCollate` returns for
/// it, which is what a table with no explicit charset reports.
const TABLE_CHARSET: &str = "utf8mb4";
const TABLE_COLLATE: &str = "utf8mb4_bin";

/// Go `stringutil.Escape` with a non-ANSI_QUOTES sql_mode: backtick-quoted,
/// with an embedded backtick doubled.
fn escape_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go `constructResultOfShowCreateTable`, over the metadata this seed keeps.
///
/// The shape is Go's line for line: the header, two-space-indented column
/// clauses separated by ",\n", the clustered primary key when the handle is
/// one, then the indexes, then the closing paren with the engine and charset.
///
/// NOT MODELLED (documented, and each one rejected at DDL time so no table can
/// carry it): generated columns, AUTO_INCREMENT, AUTO_RANDOM, ON UPDATE
/// CURRENT_TIMESTAMP, column and index comments, foreign keys, check
/// constraints, partitioning, temporary tables, views and sequences.
/// Per-column CHARACTER SET / COLLATE clauses are omitted because this seed
/// stores no per-column charset, so every column takes the table's.
fn show_create_table_text(name: &str, table: &tidb_executor::KvTable) -> String {
    let mut out = format!("CREATE TABLE {} (\n", escape_name(name));
    let mut clauses: Vec<String> = Vec::with_capacity(table.columns.len() + 1);

    for (offset, column) in table.columns.iter().enumerate() {
        let mut clause = format!(
            "  {} {}",
            escape_name(&column.name),
            column.field_type.compact_str(false)
        );
        let not_null = column.field_type.flags() & NOT_NULL_FLAG != 0;
        if table.auto_increment_offset() == Some(offset) {
            // Go writes the pair together for an auto column and prints no
            // default for it.
            clause.push_str(" NOT NULL AUTO_INCREMENT");
            clauses.push(clause);
            continue;
        }
        if not_null {
            clause.push_str(" NOT NULL");
        }
        // Go prints nothing for a column carrying NoDefaultValueFlag, which is
        // a NOT NULL column with no DEFAULT clause; a nullable column with no
        // DEFAULT reports DEFAULT NULL, as MySQL does.
        match &column.default_value {
            Some(Datum::Null) => clause.push_str(" DEFAULT NULL"),
            Some(value) => {
                // Go quotes every non-bit default, integers included.
                let text = datum_text(value).unwrap_or_default();
                clause.push_str(&format!(" DEFAULT '{text}'"));
            }
            None if !not_null => clause.push_str(" DEFAULT NULL"),
            None => {}
        }
        clauses.push(clause);
    }

    // Go emits a clustered primary key here, because a clustered key -- an
    // int handle or a common handle -- is not in the index list.
    let clustered: Vec<usize> = match table.pk_handle_offset() {
        Some(offset) => vec![offset],
        None => table.common_handle_offsets().to_vec(),
    };
    if !clustered.is_empty() {
        let columns = clustered
            .iter()
            .map(|offset| escape_name(&table.columns[*offset].name))
            .collect::<Vec<_>>()
            .join(",");
        clauses.push(format!(
            "  PRIMARY KEY ({columns}) /*T![clustered_index] CLUSTERED */"
        ));
    }

    for index in table.indexes() {
        let columns = index
            .column_offsets
            .iter()
            .map(|offset| escape_name(&table.columns[*offset].name))
            .collect::<Vec<_>>()
            .join(",");
        if index.name.eq_ignore_ascii_case("PRIMARY") {
            // A primary key that is not the handle is non-clustered here,
            // since this seed builds no clustered common handle.
            clauses.push(format!(
                "  PRIMARY KEY ({columns}) /*T![clustered_index] NONCLUSTERED */"
            ));
        } else if index.unique {
            clauses.push(format!(
                "  UNIQUE KEY {} ({columns})",
                escape_name(&index.name)
            ));
        } else {
            clauses.push(format!("  KEY {} ({columns})", escape_name(&index.name)));
        }
    }

    out.push_str(&clauses.join(",\n"));
    out.push_str(&format!(
        "\n) ENGINE=InnoDB DEFAULT CHARSET={TABLE_CHARSET} COLLATE={TABLE_COLLATE}"
    ));
    out
}

/// Go `table.ColDescFieldNames(false)`: the columns `SHOW COLUMNS` and
/// `DESCRIBE` produce.
const COL_DESC_FIELD_NAMES: &[&str] = &["Field", "Type", "Null", "Key", "Default", "Extra"];

/// Go `table.NewColDesc`, restricted to the facts this seed's metadata holds.
///
/// `Null` is NO when the column carries `NotNullFlag`; `Key` is PRI for a
/// primary-key column, UNI for a column that is the whole of a unique index,
/// and MUL for one that leads a non-unique index -- Go reads those from the
/// column's key flags, which the DDL sets from the same index definitions.
///
/// `Default` is the column's stored `DEFAULT`, or NULL when none was written.
///
/// `Extra` reports `auto_increment` for the auto column.
///
/// NOT MODELLED (documented): the other `Extra` values -- ON UPDATE
/// CURRENT_TIMESTAMP and the generated-column markers -- because those column
/// kinds are rejected at DDL time, so no column can carry them.
fn column_description(
    column: &tidb_executor::KvColumn,
    offset: usize,
    table: &tidb_executor::KvTable,
) -> Vec<Datum> {
    let null_flag = if column.field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    // Go `NewColDesc`: an auto-increment column reports auto_increment.
    let extra = if table.auto_increment_offset() == Some(offset) {
        "auto_increment"
    } else {
        ""
    };
    let key_flag = column_key_flag(table, offset);
    vec![
        Datum::Bytes(column.name.clone().into_bytes()),
        Datum::Bytes(column.field_type.compact_str(false).into_bytes()),
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(key_flag.into_bytes()),
        // Go prints the stored default; a column without one shows NULL.
        match &column.default_value {
            Some(value) => match datum_text(value) {
                Some(text) => Datum::Bytes(text.into_bytes()),
                None => Datum::Null,
            },
            None => Datum::Null,
        },
        Datum::Bytes(extra.as_bytes().to_vec()),
    ]
}

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;

/// Go `NewColDesc`'s key flag, shared by `SHOW COLUMNS` and
/// `information_schema.COLUMNS`: PRI for a primary key, UNI for a column that
/// is the whole of a unique index, MUL for one that leads a non-unique index.
pub(crate) fn column_key_flag(table: &tidb_executor::KvTable, offset: usize) -> String {
    let is_handle =
        table.pk_handle_offset() == Some(offset) || table.common_handle_offsets().contains(&offset);
    if is_handle
        || table.indexes().iter().any(|index| {
            index.name.eq_ignore_ascii_case("PRIMARY") && index.column_offsets == [offset]
        })
    {
        "PRI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.unique && index.column_offsets == [offset])
    {
        "UNI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.first() == Some(&offset))
    {
        "MUL".to_owned()
    } else {
        String::new()
    }
}

/// A one-column result set of strings, the shape SHOW DATABASES and SHOW
/// TABLES produce.
fn string_column_output(column: &str, values: Vec<String>) -> StmtOutput {
    let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
    StmtOutput::Rows {
        columns: vec![(column.to_owned(), field_type)],
        rows: values
            .into_iter()
            .map(|value| vec![Datum::Bytes(value.into_bytes())])
            .collect(),
    }
}

/// The text form a system variable stores for a datum (Go keeps every system
/// variable as a string).
fn datum_text(value: &Datum) -> Option<String> {
    match value {
        Datum::Null => None,
        Datum::Int(v) => Some(v.to_string()),
        Datum::UInt(v) => Some(v.to_string()),
        Datum::Real(v) => Some(v.to_string()),
        Datum::Decimal(d) => Some(d.to_string()),
        Datum::String(s) => Some(String::from_utf8_lossy(s.bytes()).into_owned()),
        Datum::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

impl Session {
    /// A fresh session with its own empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Session::default()
    }

    /// Go `SessionVars.CurrentDB`. Empty when no database is selected.
    #[must_use]
    pub fn current_database(&self) -> &str {
        &self.current_db
    }

    /// Applies `USE`, `CREATE DATABASE`, `DROP DATABASE`, `SHOW DATABASES`
    /// and `SHOW TABLES`.
    ///
    /// Returns `Some(output)` for those statements and `None` for anything
    /// else, so a caller can dispatch without re-parsing.
    pub fn apply_schema_statement(&mut self, sql: &str) -> Result<Option<StmtOutput>, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        match &stmt {
            Stmt::Session(session_stmt) => match &**session_stmt {
                SessionStmt::Use(name) => {
                    self.use_database(name)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                _ => Ok(None),
            },
            Stmt::Ddl(ddl) => match &**ddl {
                tidb_ast::DdlStmt::CreateDatabase {
                    if_not_exists,
                    name,
                    options,
                } => {
                    if !options.is_empty() {
                        return Err(DriverError::Unsupported(
                            "database charset and collation options are not supported yet",
                        ));
                    }
                    let created =
                        self.with_catalog_mut(|catalog| Ok(catalog.create_database(name)))?;
                    // Go raises ErrDBCreateExists unless IF NOT EXISTS.
                    if !created && !*if_not_exists {
                        return Err(DriverError::Schema(SchemaErrorKind::DatabaseExists(
                            name.clone(),
                        )));
                    }
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::DdlStmt::DropDatabase { if_exists, name } => {
                    let dropped =
                        self.with_catalog_mut(|catalog| Ok(catalog.drop_database(name)))?;
                    // Go raises ErrDBDropExists unless IF EXISTS.
                    if !dropped && !*if_exists {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                            name.clone(),
                        )));
                    }
                    // Dropping the current database leaves the session with
                    // none selected, which is Go's ErrNoDB state for the next
                    // unqualified statement.
                    if dropped && self.current_db.eq_ignore_ascii_case(name) {
                        self.current_db.clear();
                    }
                    Ok(Some(StmtOutput::Affected(0)))
                }
                _ => Ok(None),
            },
            Stmt::Admin(admin) => match &**admin {
                tidb_ast::AdminStmt::ShowDatabases(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW DATABASES filters are not supported yet",
                        ));
                    }
                    let names = self.with_catalog_mut(|catalog| Ok(catalog.database_names()))?;
                    Ok(Some(string_column_output("Database", names)))
                }
                // Go `ShowExec` with `ShowWarnings`/`ShowErrors`: the rows are
                // the statement-context warnings, whose `Level` column is
                // `Warning` or `Error`.
                //
                // DEFERRED (documented, and refused rather than ignored): the
                // optional filter Go's shared SHOW grammar accepts here.
                tidb_ast::AdminStmt::ShowWarnings(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW WARNINGS filters are not supported yet",
                        ));
                    }
                    Ok(Some(self.warning_output(show.count_only, false)))
                }
                tidb_ast::AdminStmt::ShowErrors(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW ERRORS filters are not supported yet",
                        ));
                    }
                    Ok(Some(self.warning_output(show.count_only, true)))
                }
                // Go `fetchShowCreateTable`.
                tidb_ast::AdminStmt::ShowCreate { kind, name, .. } => {
                    if *kind != tidb_ast::ShowCreateKind::Table {
                        return Ok(None);
                    }
                    let current = self.require_current_database()?.to_owned();
                    let (database, table_name) = match name.as_slice() {
                        [table] => (current, table.clone()),
                        [database, table] => (database.clone(), table.clone()),
                        _ => return Err(DriverError::Unsupported("empty table name")),
                    };
                    let (text, reported) = self.with_catalog_mut(|catalog| {
                        let Some(entry) = catalog.table_in(&database, &table_name) else {
                            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(
                                format!("{database}.{table_name}"),
                            )));
                        };
                        let tidb_executor::TableEntry::Kv(table) = entry else {
                            return Err(DriverError::Unsupported(
                                "SHOW CREATE TABLE needs a storage-backed table",
                            ));
                        };
                        Ok((
                            show_create_table_text(&table_name, table),
                            table_name.clone(),
                        ))
                    })?;
                    let field_type =
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            ("Table".to_owned(), field_type.clone()),
                            ("Create Table".to_owned(), field_type),
                        ],
                        rows: vec![vec![
                            Datum::Bytes(reported.into_bytes()),
                            Datum::Bytes(text.into_bytes()),
                        ]],
                    }))
                }
                // Go `fetchShowColumns`.
                tidb_ast::AdminStmt::ShowColumns(show) => {
                    if show.filter.is_some() || show.full || show.extended {
                        return Err(DriverError::Unsupported(
                            "SHOW FULL/EXTENDED COLUMNS and column filters are not supported yet",
                        ));
                    }
                    let database = match &show.database {
                        Some(name) => name.clone(),
                        None => self.require_current_database()?.to_owned(),
                    };
                    self.show_columns(&database, &show.table, None).map(Some)
                }
                // Go's parser rewrites `DESCRIBE tbl [col]` into a SHOW
                // COLUMNS statement; this parser keeps a node of its own, so
                // the same output is produced from it here.
                tidb_ast::AdminStmt::DescribeTable(describe) => {
                    let database = self.require_current_database()?.to_owned();
                    let column = describe.column.as_ref().and_then(|path| path.last());
                    self.show_columns(&database, &describe.table, column.map(String::as_str))
                        .map(Some)
                }
                tidb_ast::AdminStmt::ShowTables(show) => {
                    if show.filter.is_some() || show.full {
                        return Err(DriverError::Unsupported(
                            "SHOW FULL TABLES and SHOW TABLES filters are not supported yet",
                        ));
                    }
                    let database = match &show.database {
                        Some(name) => name.clone(),
                        None => self.require_current_database()?.to_owned(),
                    };
                    let names =
                        self.with_catalog_mut(|catalog| Ok(catalog.table_names(&database)))?;
                    let names = names.ok_or_else(|| {
                        DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                    })?;
                    // Go names the column after the schema being listed.
                    Ok(Some(string_column_output(
                        &format!("Tables_in_{database}"),
                        names,
                    )))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// Runs a `SELECT` whose `FROM` names an `information_schema` table.
    ///
    /// The virtual rows are materialized into a scratch catalog and then run
    /// through the ordinary plan, so `WHERE`, `ORDER BY`, `LIMIT`, expressions
    /// and aggregates all behave as they do over a stored table. Go reaches
    /// the same place differently -- its memory tables are real tables to the
    /// planner -- but the requirement is the same: a predicate over a virtual
    /// table must filter it.
    ///
    /// Returns `None` when the statement is an ordinary one, so the caller
    /// falls through to the storage path.
    ///
    /// DEFERRED (documented): a join between a virtual table and a stored one,
    /// because the scratch catalog holds only the virtual side. Such a
    /// statement is rejected rather than answered from half the data.
    fn run_information_schema_select(
        &mut self,
        select: &tidb_ast::SelectStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let Some(join) = &select.from else {
            return Ok(None);
        };
        let tidb_ast::JoinNode::Table(table_ref) = &join.left else {
            return Ok(None);
        };
        // `information_schema.X`, or a bare `X` while that schema is current.
        let (schema, table_name) = match table_ref.name.as_slice() {
            [name] => (self.current_db.clone(), name.clone()),
            [schema, name] => (schema.clone(), name.clone()),
            _ => return Ok(None),
        };
        if !infoschema::is_information_schema(&schema) {
            return Ok(None);
        }
        if join.right.is_some() {
            return Err(DriverError::Unsupported(
                "joining an information_schema table is not supported yet",
            ));
        }
        let Some(columns) = infoschema::table_schema(&table_name) else {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                "{schema}.{table_name}"
            ))));
        };
        let rows = self.with_catalog_mut(|catalog| {
            Ok(infoschema::table_rows(&table_name, catalog).unwrap_or_default())
        })?;

        // A scratch catalog holding just this table, so the ordinary plan runs
        // over it.
        let mut scratch = Catalog::default();
        scratch.register_mem_in(
            infoschema::INFORMATION_SCHEMA,
            &table_name,
            tidb_executor::MemTable { columns, rows },
        );
        let ctx = self.statement_context(false);
        let (columns, rows) = tidb_executor::run_select_meta_stmt(
            select,
            &scratch,
            infoschema::INFORMATION_SCHEMA,
            &ctx,
        )?;
        self.drain_eval_warnings(&ctx);
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }

    /// The `SHOW COLUMNS` / `DESCRIBE` result for one table, optionally
    /// narrowed to a single column as Go's `DESCRIBE tbl col` narrows it.
    fn show_columns(
        &mut self,
        database: &str,
        table_path: &[String],
        column: Option<&str>,
    ) -> Result<StmtOutput, DriverError> {
        // A `db.tbl` path names its own schema, as everywhere else.
        let (database, table_name) = match table_path {
            [name] => (database.to_owned(), name.clone()),
            [db, name] => (db.clone(), name.clone()),
            _ => return Err(DriverError::Unsupported("empty table name")),
        };
        let rows = self.with_catalog_mut(|catalog| {
            let Some(entry) = catalog.table_in(&database, &table_name) else {
                return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                    "{database}.{table_name}"
                ))));
            };
            let tidb_executor::TableEntry::Kv(table) = entry else {
                return Err(DriverError::Unsupported(
                    "SHOW COLUMNS needs a storage-backed table",
                ));
            };
            Ok(table
                .columns
                .iter()
                .enumerate()
                .filter(|(_, candidate)| {
                    column.is_none_or(|name| candidate.name.eq_ignore_ascii_case(name))
                })
                .map(|(offset, candidate)| column_description(candidate, offset, table))
                .collect::<Vec<_>>())
        })?;
        let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        Ok(StmtOutput::Rows {
            columns: COL_DESC_FIELD_NAMES
                .iter()
                .map(|name| ((*name).to_owned(), field_type.clone()))
                .collect(),
            rows,
        })
    }

    /// Go `executeUse`: an unknown schema is `ErrDatabaseNotExists`, and the
    /// switch also updates `collation_database`.
    fn use_database(&mut self, name: &str) -> Result<(), DriverError> {
        let exists = self.with_catalog_mut(|catalog| Ok(catalog.has_database(name)))?;
        if !exists {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                name.to_owned(),
            )));
        }
        self.current_db = name.to_owned();
        Ok(())
    }

    /// The current database, or Go's `ErrNoDB` when none is selected.
    fn require_current_database(&self) -> Result<&str, DriverError> {
        if self.current_db.is_empty() {
            return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
        }
        Ok(&self.current_db)
    }

    /// Go `LAST_INSERT_ID()`: the first id the most recent ALLOCATING
    /// statement handed out. A statement that allocated nothing -- an explicit
    /// auto value, a table with no auto column, an UPDATE -- leaves it as it
    /// was, which is what MySQL and TiDB both do.
    #[must_use]
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// The id the last statement allocated, which the OK packet reports and
    /// which is 0 when the statement allocated nothing.
    #[must_use]
    pub fn statement_insert_id(&self) -> u64 {
        self.statement_insert_id
    }

    /// The session's variables.
    #[must_use]
    pub fn vars(&self) -> &SessionVars {
        &self.vars
    }

    /// Applies a `SET` statement.
    ///
    /// Returns `Some(())` when the SQL is a `SET` this handles and `None`
    /// otherwise, so a caller can answer with an OK packet without
    /// re-parsing. Go's `SetExecutor` walks the assignments in source order
    /// and stops at the first error, which this reproduces.
    ///
    /// DEFERRED (documented): `SET GLOBAL` changes only this session here,
    /// because there is no persisted global tier yet; `SET PASSWORD`,
    /// resource groups, and the other non-variable `SET` forms stay
    /// unsupported.
    pub fn apply_set(&mut self, sql: &str) -> Result<Option<()>, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        let Stmt::Session(session_stmt) = &stmt else {
            return Ok(None);
        };
        match &**session_stmt {
            SessionStmt::Set(set) => {
                for assignment in &set.assignments {
                    self.apply_assignment(assignment)?;
                }
                Ok(Some(()))
            }
            SessionStmt::SetCharset {
                charset,
                collation,
                assignments,
                ..
            } => {
                self.apply_charset(charset.as_deref(), collation.as_deref())?;
                for assignment in assignments {
                    self.apply_assignment(assignment)?;
                }
                Ok(Some(()))
            }
            SessionStmt::SetMixed(items) => {
                for item in items {
                    match item {
                        tidb_ast::SetItem::System(assignment) => {
                            self.apply_assignment(assignment)?;
                        }
                        tidb_ast::SetItem::Charset {
                            charset, collation, ..
                        } => self.apply_charset(charset.as_deref(), collation.as_deref())?,
                    }
                }
                Ok(Some(()))
            }
            SessionStmt::SetUserVar(set) => {
                for assignment in &set.assignments {
                    let value = self.eval_literal(&assignment.value)?;
                    self.vars.set_user(&assignment.name, value);
                }
                Ok(Some(()))
            }
            _ => Ok(None),
        }
    }

    /// One `name = value` assignment.
    fn apply_assignment(
        &mut self,
        assignment: &tidb_ast::SystemVariableAssignment,
    ) -> Result<(), DriverError> {
        let value = match &assignment.value {
            // Go restores a variable to its registry default by clearing the
            // session override.
            tidb_ast::SetVariableValue::Default => {
                self.vars
                    .reset_system(&assignment.name)
                    .map_err(var_error)?;
                return Ok(());
            }
            tidb_ast::SetVariableValue::Expr(expr) => self.eval_literal(expr)?,
        };
        // Go stores every system variable as a string.
        self.vars
            .set_system(&assignment.name, value.unwrap_or_default())
            .map_err(var_error)
    }

    /// `SET NAMES` / `SET CHARACTER SET`.
    fn apply_charset(
        &mut self,
        charset: Option<&str>,
        collation: Option<&str>,
    ) -> Result<(), DriverError> {
        // `DEFAULT` restores the registry default, which is what the charset
        // variables already hold when nothing has overridden them.
        let charset = charset.unwrap_or("utf8mb4");
        self.vars.set_names(charset, collation).map_err(var_error)
    }

    /// Evaluates a `SET` right-hand side. Go runs it through the expression
    /// evaluator; this evaluates it as a constant expression, which covers the
    /// literals and simple arithmetic a `SET` carries.
    fn eval_literal(&mut self, expr: &tidb_ast::Expr) -> Result<Option<String>, DriverError> {
        // An unquoted identifier is a bare word value such as `SET sql_mode =
        // ANSI_QUOTES` or `SET autocommit = ON`, which MySQL takes literally.
        if let tidb_ast::Expr::Column(path) = expr {
            if let [word] = path.as_slice() {
                return Ok(Some(word.clone()));
            }
        }
        let sql = format!("SELECT {}", expr.restore());
        let ctx = self.statement_context(false);
        let rows =
            self.with_catalog_mut(|catalog| tidb_executor::run_select_on(&sql, catalog, &ctx))?;
        let value = rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(Datum::Null);
        Ok(datum_text(&value))
    }

    /// Replaces every variable reference in `sql` with the session's value,
    /// so the driver plans against ordinary literals.
    ///
    /// Go resolves `@@x` and `@x` in the expression rewriter using the
    /// session's variables; the values live in the session here, so the
    /// substitution happens here too. An unknown `@@x` is Go's 1193, while an
    /// unset `@x` is NULL rather than an error, as in MySQL.
    fn bind_variables(&self, stmt: &mut Stmt) -> Result<(), DriverError> {
        let Stmt::Query(query) = stmt else {
            return Ok(());
        };
        let tidb_ast::QueryStmt::Select(select) = &mut **query else {
            return Ok(());
        };
        for field in select.fields.fields_mut() {
            if let tidb_ast::SelectField::Expr { expr, .. } = field {
                *expr = self.bind_variables_in(expr)?;
            }
        }
        if let Some(where_clause) = &select.where_clause {
            select.where_clause = Some(self.bind_variables_in(where_clause)?);
        }
        if let Some(having) = &select.having {
            select.having = Some(self.bind_variables_in(having)?);
        }
        for item in &mut select.order_by {
            item.expr = self.bind_variables_in(&item.expr)?;
        }
        for item in &mut select.group_by {
            item.expr = self.bind_variables_in(&item.expr)?;
        }
        Ok(())
    }

    /// Substitutes variable references inside one expression.
    fn bind_variables_in(&self, expr: &tidb_ast::Expr) -> Result<tidb_ast::Expr, DriverError> {
        use tidb_ast::Expr;
        Ok(match expr {
            Expr::SysVar { name, .. } => {
                // A scope prefix does not change the value here: there is no
                // separate global tier yet (documented in `vars`).
                match self.vars.get_system(name) {
                    Ok(value) => Expr::String(value),
                    Err(error) => return Err(var_error(error)),
                }
            }
            // `LAST_INSERT_ID()` reads session state, so it binds here for
            // the same reason `@@x` does.
            Expr::Func { name, args, .. }
                if name.eq_ignore_ascii_case("LAST_INSERT_ID") && args.is_empty() =>
            {
                Expr::Int(self.last_insert_id.to_string())
            }
            Expr::UserVar(name) => match self.vars.get_user(name) {
                Some(value) => Expr::String(value),
                None => Expr::Null,
            },
            Expr::Paren(inner) => Expr::Paren(Box::new(self.bind_variables_in(inner)?)),
            Expr::Unary(op, inner) => Expr::Unary(*op, Box::new(self.bind_variables_in(inner)?)),
            Expr::Binary(op, lhs, rhs) => Expr::Binary(
                *op,
                Box::new(self.bind_variables_in(lhs)?),
                Box::new(self.bind_variables_in(rhs)?),
            ),
            Expr::Is { expr, target, not } => Expr::Is {
                expr: Box::new(self.bind_variables_in(expr)?),
                target: *target,
                not: *not,
            },
            Expr::In { expr, list, not } => Expr::In {
                expr: Box::new(self.bind_variables_in(expr)?),
                list: list
                    .iter()
                    .map(|item| self.bind_variables_in(item))
                    .collect::<Result<_, _>>()?,
                not: *not,
            },
            other => other.clone(),
        })
    }

    /// Whether a transaction is open (the wire's `SERVER_STATUS_IN_TRANS`).
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// Applies `BEGIN`/`START TRANSACTION`, `COMMIT`, or `ROLLBACK`.
    ///
    /// Returns `Some(in_transaction)` for those statements and `None` for
    /// anything else, so a caller can answer with an OK packet carrying the
    /// right status flag without re-parsing.
    ///
    /// Go's `BEGIN` inside an open transaction implicitly commits the current
    /// one before starting the new one, which this reproduces. `COMMIT` and
    /// `ROLLBACK` with no open transaction are no-ops, as in MySQL.
    pub fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        let Stmt::Session(session_stmt) = &stmt else {
            return Ok(None);
        };
        match &**session_stmt {
            SessionStmt::Begin(_) => {
                // An open transaction is committed first (Go's implicit commit).
                if self.txn.is_some() {
                    self.commit()?;
                }
                let (working, base_version) = {
                    let catalog = self.lock_catalog()?;
                    (catalog.clone(), catalog.version())
                };
                self.txn = Some(Transaction {
                    working,
                    base_version,
                });
                Ok(Some(true))
            }
            SessionStmt::Commit(_) => {
                self.commit()?;
                Ok(Some(false))
            }
            SessionStmt::Rollback { savepoint, .. } => {
                if savepoint.is_some() {
                    return Err(DriverError::Unsupported(
                        "ROLLBACK TO SAVEPOINT is not supported yet",
                    ));
                }
                // Dropping the staged copy discards every staged write.
                self.txn = None;
                Ok(Some(false))
            }
            _ => Ok(None),
        }
    }

    /// Publishes the open transaction's staged writes, or refuses when the
    /// shared catalog moved under it. A refused commit ends the transaction,
    /// as an aborted Go transaction does -- the staged writes are gone either
    /// way, so the caller must retry the statements, not just the COMMIT.
    fn commit(&mut self) -> Result<(), DriverError> {
        let Some(txn) = self.txn.take() else {
            // COMMIT with no open transaction is a no-op, as in MySQL.
            return Ok(());
        };
        let mut shared = self.lock_catalog()?;
        if shared.version() != txn.base_version {
            return Err(DriverError::Txn(TxnErrorKind::WriteConflict));
        }
        *shared = txn.working;
        Ok(())
    }

    /// A session sharing `catalog` with its peers.
    #[must_use]
    pub fn with_catalog(catalog: SharedCatalog) -> Self {
        Session {
            catalog,
            txn: None,
            vars: SessionVars::new(),
            warnings: Vec::new(),
            last_insert_id: 0,
            statement_insert_id: 0,
            current_db: DEFAULT_DATABASE.to_owned(),
        }
    }

    /// The shared catalog handle, for opening a peer session over the same
    /// schema state.
    #[must_use]
    pub fn shared_catalog(&self) -> SharedCatalog {
        Arc::clone(&self.catalog)
    }

    /// Borrows the shared catalog for one statement. The lock is held for the
    /// statement's duration only, which is the granularity Go's schema state
    /// is consumed at.
    fn lock_catalog(&self) -> Result<MutexGuard<'_, Catalog>, DriverError> {
        self.catalog
            .lock()
            .map_err(|_| DriverError::CatalogPoisoned)
    }

    /// Runs `body` over the catalog this statement sees: the transaction's
    /// staged copy when one is open (so it reads its own writes), otherwise
    /// the shared catalog directly (autocommit).
    fn with_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        match &mut self.txn {
            Some(txn) => body(&mut txn.working),
            None => {
                let mut catalog = self
                    .catalog
                    .lock()
                    .map_err(|_| DriverError::CatalogPoisoned)?;
                body(&mut catalog)
            }
        }
    }

    /// Classifies a statement by parsing alone (no execution), so a caller can
    /// choose the protocol answer shape before running it.
    pub fn statement_kind(&self, sql: &str) -> Result<StmtKind, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        match &stmt {
            Stmt::Query(_) => Ok(StmtKind::Query),
            Stmt::Dml(_) | Stmt::Ddl(_) => Ok(StmtKind::Write),
            _ => Err(DriverError::Unsupported(
                "this statement kind is not supported yet",
            )),
        }
    }

    /// Runs one SQL statement (Go `session.ExecuteStmt`): parses, dispatches by
    /// statement kind, and executes over the session catalog.
    pub fn run(&mut self, sql: &str) -> Result<StmtResult, DriverError> {
        Ok(match self.run_with_columns(sql)? {
            StmtOutput::Rows { rows, .. } => StmtResult::Rows(rows),
            StmtOutput::Affected(count) => StmtResult::Affected(count),
            StmtOutput::Done(created) => StmtResult::Done(created),
        })
    }

    /// Like [`Session::run`], but a query result also carries its column
    /// metadata (`(name, type)` per column) for wire-protocol fronts.
    ///
    /// Captured from TiDB: a statement that fails leaves its own error in the
    /// warning buffer as an `Error`-level row, so `SHOW WARNINGS` right after
    /// a failure reports it.
    pub fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        let result = self.execute_statement(sql);
        if let Err(error) = &result {
            let reported = error.clone().to_mysql_error();
            self.warnings.push(SqlWarning {
                level: WarningLevel::Error,
                code: reported.code,
                message: reported.message,
            });
        }
        result
    }

    fn execute_statement(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        // Go clears the warning buffer when a statement starts, so what
        // `SHOW WARNINGS` reports always belongs to the statement before it --
        // which is why those two statements must not clear it themselves.
        if !reports_warnings(sql) {
            self.warnings.clear();
        }
        // USE / CREATE DATABASE / DROP DATABASE / SHOW DATABASES / SHOW TABLES.
        if let Some(output) = self.apply_schema_statement(sql)? {
            return Ok(output);
        }
        let mut stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        // `@@x` / `@x` read the session's own state, so they are bound before
        // the statement reaches the driver.
        self.bind_variables(&mut stmt)?;
        // Only an allocating INSERT sets it; every other statement reports 0.
        self.statement_insert_id = 0;
        // Go's preprocessor runs before planning, so a gated clause is
        // refused before any table is touched.
        if let Stmt::Query(query) = &stmt {
            self.check_noop_functions(query)?;
            self.check_query_clauses(query)?;
        }
        // Go raises ErrNoDB where an unqualified NAME is resolved, not for
        // every statement: `SELECT 1` and `SELECT DATABASE()` both run with
        // no database selected (captured). The driver's own
        // `split_table_path` raises it at the resolution point, which is
        // where Go's does.
        match &stmt {
            Stmt::Query(query) => {
                let tidb_ast::QueryStmt::Select(select) = &**query else {
                    // A set operation runs through its own fold.
                    let tidb_ast::QueryStmt::SetOpr(set_opr) = &**query else {
                        unreachable!("a query is a SELECT or a set operation")
                    };
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(false);
                    let (columns, rows) = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_set_opr_stmt(set_opr, catalog, &current_db, &ctx)
                    })?;
                    self.drain_eval_warnings(&ctx);
                    return Ok(StmtOutput::Rows { columns, rows });
                };
                // An information_schema table is virtual: its rows are
                // computed from the catalog rather than read from storage.
                if let Some(output) = self.run_information_schema_select(select)? {
                    return Ok(output);
                }
                let current_db = self.current_db.clone();
                let ctx = self.statement_context(false);
                let (columns, rows) = self.with_catalog_mut(|catalog| {
                    tidb_executor::run_select_meta_stmt(select, catalog, &current_db, &ctx)
                })?;
                self.drain_eval_warnings(&ctx);
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Dml(dml) => match &**dml {
                DmlStmt::Insert(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let (affected, allocated) = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_insert_reporting(sql, catalog, &current_db, &ctx)
                    })?;
                    self.drain_eval_warnings(&ctx);
                    self.statement_insert_id = allocated.unwrap_or(0).max(0) as u64;
                    if let Some(allocated) = allocated {
                        self.last_insert_id = allocated.max(0) as u64;
                    }
                    Ok(StmtOutput::Affected(affected))
                }
                DmlStmt::Update(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let output = self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_update_in(
                            sql,
                            catalog,
                            &current_db,
                            &ctx,
                        )?))
                    });
                    self.drain_eval_warnings(&ctx);
                    output
                }
                DmlStmt::Delete(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let output = self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_delete_in(
                            sql,
                            catalog,
                            &current_db,
                            &ctx,
                        )?))
                    });
                    self.drain_eval_warnings(&ctx);
                    output
                }
                _ => Err(DriverError::Unsupported(
                    "this DML statement kind is not supported yet",
                )),
            },
            Stmt::Ddl(ddl) => match &**ddl {
                DdlStmt::RenameTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_rename_table_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::TruncateTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_truncate_table_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::CreateIndex(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_index_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropIndex(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_index_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::AlterTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_alter_table_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_table_in(sql, catalog, &current_db)?;
                        // MySQL answers DDL with a zero affected-row count.
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::CreateTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Done(tidb_executor::run_create_table_in(
                            sql,
                            catalog,
                            &current_db,
                        )?))
                    })
                }
                _ => Err(DriverError::Unsupported(
                    "this DDL statement kind is not supported yet",
                )),
            },
            _ => Err(DriverError::Unsupported(
                "this statement kind is not supported yet",
            )),
        }
    }
}

/// A statement warning, which Go keeps in `StmtCtx` and `SHOW WARNINGS`
/// reports as `Level | Code | Message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlWarning {
    /// Whether the statement survived it.
    pub level: WarningLevel,
    /// The MySQL error code the warning carries.
    pub code: u16,
    /// The message text.
    pub message: String,
}

/// A warning's `Level` column, which Go fills from
/// `StmtCtx.warnings[i].Level`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningLevel {
    /// The statement continued.
    Warning,
    /// The statement failed; Go records its error in the same buffer.
    Error,
}

impl WarningLevel {
    /// The text the `Level` column shows.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            WarningLevel::Warning => "Warning",
            WarningLevel::Error => "Error",
        }
    }
}

/// Go `variable.NoopFuncsMode`: how a clause TiDB only implements as a
/// no-op is treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NoopFuncsMode {
    /// `OFF` (the default): the statement is refused.
    Off,
    /// `ON`: the clause is accepted and does nothing.
    On,
    /// `WARN`: the clause is accepted with a warning.
    Warn,
}

impl Session {
    /// `SHOW WARNINGS` / `SHOW ERRORS` output: one row per buffered warning,
    /// or the count when the source wrote `SHOW COUNT(*) WARNINGS`.
    ///
    /// Captured from TiDB: the columns are `Level`, `Code`, `Message`; the
    /// count form returns a single `@@session.warning_count` column; and
    /// `SHOW ERRORS` shows only the `Error`-level rows.
    fn warning_output(&self, count_only: bool, errors_only: bool) -> StmtOutput {
        let reported = self
            .warnings
            .iter()
            .filter(|warning| !errors_only || warning.level == WarningLevel::Error);
        if count_only {
            let count = reported.count() as i64;
            let name = if errors_only {
                "@@session.error_count"
            } else {
                "@@session.warning_count"
            };
            return StmtOutput::Rows {
                columns: vec![(
                    name.to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )],
                rows: vec![vec![Datum::Int(count)]],
            };
        }
        let text = || FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let rows = reported
            .map(|warning| {
                vec![
                    Datum::Bytes(warning.level.as_str().as_bytes().to_vec()),
                    Datum::Int(i64::from(warning.code)),
                    Datum::Bytes(warning.message.clone().into_bytes()),
                ]
            })
            .collect();
        StmtOutput::Rows {
            columns: vec![
                ("Level".to_owned(), text()),
                (
                    "Code".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ),
                ("Message".to_owned(), text()),
            ],
            rows,
        }
    }

    /// Go `timeutil.ParseTimeZone`: `SYSTEM` is the host zone, a named zone
    /// comes from the zone database, and a `+HH:MM`/`-HH:MM` string is a
    /// fixed offset bounded to `[-12:59, +14:00]`.
    ///
    /// An unparseable value falls back to the host zone rather than failing
    /// the statement, because this tier accepts the variable without
    /// validating it at SET time -- Go validates there instead, and that
    /// check is the deferred half of this port.
    fn session_time_zone(&self) -> tidb_executor::SessionTimeZone {
        use tidb_executor::SessionTimeZone;
        let written = self
            .vars
            .get_system("time_zone")
            .unwrap_or_else(|_| "SYSTEM".to_owned());
        if !written.eq_ignore_ascii_case("SYSTEM") {
            if let Ok(zone) = written.parse::<chrono_tz::Tz>() {
                return SessionTimeZone::Named(zone);
            }
            if let Some(rest) = written.strip_prefix(['+', '-']) {
                let negative = written.starts_with('-');
                let mut parts = rest.split(':');
                let hours: i32 = parts.next().unwrap_or_default().parse().unwrap_or(-1);
                let minutes: i32 = parts.next().unwrap_or("0").parse().unwrap_or(-1);
                if hours >= 0 && (0..60).contains(&minutes) {
                    let offset = hours * 3600 + minutes * 60;
                    let bounded = if negative {
                        offset <= 12 * 3600 + 59 * 60
                    } else {
                        offset <= 14 * 3600
                    };
                    if bounded {
                        return SessionTimeZone::Fixed {
                            name: written.clone(),
                            offset_secs: if negative { -offset } else { offset },
                        };
                    }
                }
            }
        }
        // SYSTEM: the host's own zone, which is what Go's SystemLocation is.
        let local = chrono::Local::now();
        SessionTimeZone::Fixed {
            name: "System".to_owned(),
            offset_secs: chrono::Offset::fix(local.offset()).local_minus_utc(),
        }
    }

    /// The instant every `NOW()` in one statement shares, which Go fixes on
    /// the statement context.
    fn statement_clock(&self, zone: &tidb_executor::SessionTimeZone) -> (i64, u32, i32) {
        use tidb_executor::SessionTimeZone;
        let utc = chrono::Utc::now();
        let seconds = utc.timestamp();
        let nanos = utc.timestamp_subsec_nanos();
        let offset = match zone {
            SessionTimeZone::Fixed { offset_secs, .. } => *offset_secs,
            SessionTimeZone::Named(zone) => {
                use chrono::TimeZone;
                chrono::Offset::fix(&zone.offset_from_utc_datetime(&utc.naive_utc()))
                    .local_minus_utc()
            }
        };
        (seconds, nanos, offset)
    }

    /// The evaluation context for one statement, which is Go's
    /// `StatementContext`.
    ///
    /// The division-by-zero level is the only group modelled so far: Go warns
    /// for a query, and for a DML statement resolves it from `sql_mode` --
    /// without `ERROR_FOR_DIVISION_BY_ZERO` the condition is ignored, a
    /// non-strict mode warns, and the default strict mode fails the statement.
    fn statement_context(&self, is_dml: bool) -> tidb_executor::StmtContext {
        // Go hands the same `SessionVars` to every expression, which is where
        // `DATABASE()` and `VERSION()` read from.
        let current_db = if self.current_db.is_empty() {
            None
        } else {
            Some(self.current_db.clone())
        };
        let version = self.vars.get_system("version").ok();
        let zone = self.session_time_zone();
        let clock = self.statement_clock(&zone);
        if !is_dml {
            return tidb_executor::StmtContext::for_query()
                .with_session_state(current_db, version)
                .with_clock(clock, zone);
        }
        let mode = self
            .vars
            .get_system("sql_mode")
            .unwrap_or_default()
            .to_ascii_uppercase();
        let has = |flag: &str| mode.split(',').any(|part| part.trim() == flag);
        tidb_executor::StmtContext::for_dml(
            has("ERROR_FOR_DIVISION_BY_ZERO"),
            has("STRICT_TRANS_TABLES") || has("STRICT_ALL_TABLES"),
        )
        .with_session_state(current_db, version)
        .with_clock(clock, zone)
    }

    /// Moves what evaluation recorded into the statement's warning buffer.
    fn drain_eval_warnings(&mut self, ctx: &tidb_executor::StmtContext) {
        for (code, message) in ctx.take_warnings() {
            self.warnings.push(SqlWarning {
                level: WarningLevel::Warning,
                code,
                message,
            });
        }
    }

    /// The warnings the last statement produced.
    #[must_use]
    pub fn warnings(&self) -> &[SqlWarning] {
        &self.warnings
    }

    /// The query clauses this tier parses but cannot execute.
    ///
    /// `INTO OUTFILE` writes a server-side file, which this seed has no path
    /// for; Go returns an empty result set after writing the file, so
    /// executing the query and returning rows instead would be silently
    /// wrong. It is refused rather than ignored.
    ///
    /// ACCEPTED WITH A DEFERRAL (documented): `FOR UPDATE`. TiDB's default
    /// `tidb_txn_mode` is pessimistic, where the clause takes row locks at
    /// read time; this seed's transactions are optimistic, where TiDB itself
    /// takes no read-time lock and resolves the conflict at COMMIT -- which
    /// is exactly what this seed does. The rows returned therefore match;
    /// what is missing is the pessimistic lock, not the result. `OF t`,
    /// `NOWAIT`, `SKIP LOCKED` and `WAIT n` all only shape that missing
    /// lock's waiting behavior, so they are accepted for the same reason.
    fn check_query_clauses(&self, query: &tidb_ast::QueryStmt) -> Result<(), DriverError> {
        let into_outfile = match query {
            tidb_ast::QueryStmt::Select(select) => select.into_outfile.is_some(),
            tidb_ast::QueryStmt::SetOpr(_) => false,
        };
        if into_outfile {
            return Err(DriverError::Unsupported(
                "SELECT ... INTO OUTFILE is not supported yet",
            ));
        }
        Ok(())
    }

    /// Go `preprocessor.checkNoopFuncs` + `checkGroupBy`: refuses the clauses
    /// TiDB parses but only implements as no-ops, unless
    /// `tidb_enable_noop_functions` says otherwise.
    ///
    /// Captured from TiDB with the variable at its `OFF` default:
    /// `SELECT SQL_CALC_FOUND_ROWS ...`, `... FOR SHARE` and `... LOCK IN
    /// SHARE MODE` all raise 1235; `FOR UPDATE` does not.
    ///
    /// DEFERRED (documented): `tidb_enable_shared_lock_promotion`, which
    /// turns `FOR SHARE` into `FOR UPDATE` before this check, and the
    /// `ForShareLockEnabledByNoop` statement flag that only a real locking
    /// layer would read.
    fn check_noop_functions(&mut self, query: &tidb_ast::QueryStmt) -> Result<(), DriverError> {
        let mode = match self
            .vars
            .get_system("tidb_enable_noop_functions")
            .unwrap_or_else(|_| "OFF".to_owned())
            .to_ascii_uppercase()
            .as_str()
        {
            "ON" | "1" => NoopFuncsMode::On,
            "WARN" => NoopFuncsMode::Warn,
            _ => NoopFuncsMode::Off,
        };
        let mut gated: Vec<&'static str> = Vec::new();
        collect_noop_clauses(query, &mut gated);
        if gated.is_empty() || mode == NoopFuncsMode::On {
            return Ok(());
        }
        for clause in gated {
            let message = format!(
                "function {clause} has only noop implementation in tidb now, use \
                 tidb_enable_noop_functions to enable these functions"
            );
            if mode == NoopFuncsMode::Off {
                return Err(DriverError::FunctionsNoopImpl(clause));
            }
            self.warnings.push(SqlWarning {
                level: WarningLevel::Warning,
                code: 1235,
                message,
            });
        }
        Ok(())
    }
}

/// Names every gated clause the query uses, in the order Go's preprocessor
/// would reach them.
///
/// Go walks the whole statement tree, so a gated clause inside a derived
/// table, a CTE or a subquery counts too; this walk covers the same
/// containers.
fn collect_noop_clauses(query: &tidb_ast::QueryStmt, out: &mut Vec<&'static str>) {
    match query {
        tidb_ast::QueryStmt::Select(select) => collect_noop_in_select(select, out),
        tidb_ast::QueryStmt::SetOpr(set_opr) => collect_noop_in_set_opr(set_opr, out),
    }
}

fn collect_noop_in_set_opr(set_opr: &tidb_ast::SetOprStmt, out: &mut Vec<&'static str>) {
    if let Some(with) = &set_opr.with {
        for cte in &with.ctes {
            collect_noop_clauses(&cte.query, out);
        }
    }
    for term in &set_opr.terms {
        match &term.body {
            tidb_ast::SetOprTermBody::Select(select) => collect_noop_in_select(select, out),
            tidb_ast::SetOprTermBody::Nested(nested) => collect_noop_in_set_opr(nested, out),
        }
    }
    // A set operation carries its own trailing locking clause, which the
    // grammar attaches to the whole statement rather than the last term.
    if share_lock(&set_opr.lock) || share_lock(&set_opr.outer_lock) {
        out.push("LOCK IN SHARE MODE");
    }
}

/// Whether the statement reports the warning buffer, and so must not clear it
/// before running. Go decides this on the parsed node; parsing here would mean
/// parsing the statement twice, so this reads the leading keywords the same
/// way the dispatcher's own fast paths do.
fn reports_warnings(sql: &str) -> bool {
    let mut words = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .filter(|word| !word.is_empty());
    if !words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("SHOW"))
    {
        return false;
    }
    // `SHOW WARNINGS`, `SHOW ERRORS`, and the `SHOW COUNT(*) WARNINGS` form.
    words.any(|word| {
        let word = word.trim_end_matches(';');
        word.eq_ignore_ascii_case("WARNINGS") || word.eq_ignore_ascii_case("ERRORS")
    })
}

/// Whether a locking clause is the shared kind, which is the gated one --
/// `FOR UPDATE` is a real lock in TiDB and is never gated.
fn share_lock(lock: &Option<tidb_ast::SelectLock>) -> bool {
    matches!(
        lock,
        Some(tidb_ast::SelectLock {
            kind: tidb_ast::LockKind::Share,
            ..
        })
    )
}

fn collect_noop_in_select(select: &tidb_ast::SelectStmt, out: &mut Vec<&'static str>) {
    if select.calc_found_rows {
        out.push("SQL_CALC_FOUND_ROWS");
    }
    if share_lock(&select.lock) {
        out.push("LOCK IN SHARE MODE");
    }
    // Go's `checkGroupBy`: a written ASC/DESC on a GROUP BY item is a no-op,
    // because TiDB does not order groups.
    if select.group_by.iter().any(|item| item.desc.is_some()) {
        out.push("GROUP BY expr ASC|DESC");
    }
    if let Some(with) = &select.with {
        for cte in &with.ctes {
            collect_noop_clauses(&cte.query, out);
        }
    }
    if let Some(from) = &select.from {
        collect_noop_in_join(from, out);
    }
    for expr in select
        .where_clause
        .iter()
        .chain(select.having.iter())
        .chain(select.group_by.iter().map(|item| &item.expr))
        .chain(select.order_by.iter().map(|item| &item.expr))
    {
        collect_noop_in_expr(expr, out);
    }
}

/// The subqueries a `FROM` clause holds, which are derived tables.
fn collect_noop_in_join(join: &tidb_ast::Join, out: &mut Vec<&'static str>) {
    for node in std::iter::once(&join.left).chain(join.right.iter()) {
        match node {
            tidb_ast::JoinNode::Derived { subquery, .. } => collect_noop_clauses(subquery, out),
            tidb_ast::JoinNode::Join(nested) => collect_noop_in_join(nested, out),
            tidb_ast::JoinNode::Table(_) => {}
        }
    }
    if let Some(on) = &join.on {
        collect_noop_in_expr(on, out);
    }
}

/// The subqueries an expression holds.
fn collect_noop_in_expr(expr: &tidb_ast::Expr, out: &mut Vec<&'static str>) {
    match expr {
        tidb_ast::Expr::Subquery(query) => collect_noop_clauses(query, out),
        tidb_ast::Expr::Exists { subquery, .. } => collect_noop_clauses(subquery, out),
        tidb_ast::Expr::InSubquery { expr, subquery, .. } => {
            collect_noop_in_expr(expr, out);
            collect_noop_clauses(subquery, out);
        }
        tidb_ast::Expr::CompareSubquery { left, subquery, .. } => {
            collect_noop_in_expr(left, out);
            collect_noop_clauses(subquery, out);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A whole session lifecycle from SQL strings alone: DDL, writes, reads.
    #[test]
    fn session_runs_a_sql_lifecycle() {
        let mut session = Session::new();
        assert_eq!(
            session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap(),
            StmtResult::Done(true)
        );
        assert_eq!(
            session
                .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
                .unwrap(),
            StmtResult::Affected(3)
        );
        assert_eq!(
            session
                .run("SELECT a + b FROM t WHERE a >= 2 ORDER BY a DESC LIMIT 1")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(33)]])
        );
        // A second table coexists in the same catalog.
        session.run("CREATE TABLE u (x BIGINT)").unwrap();
        session.run("INSERT INTO u VALUES (42)").unwrap();
        assert_eq!(
            session.run("SELECT x FROM u").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(42)]])
        );
    }

    /// UPDATE and DELETE run through the session like any other write, and
    /// report their affected-row counts.
    #[test]
    fn update_and_delete_through_the_session() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1), (2), (3)").unwrap();
        assert_eq!(
            session.run("UPDATE t SET a = a * 10 WHERE a > 1").unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            session.run("DELETE FROM t WHERE a >= 20").unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        // Both are classified as writes, so the wire answers with an OK packet.
        assert_eq!(
            session.statement_kind("UPDATE t SET a = 1").unwrap(),
            StmtKind::Write
        );
        assert_eq!(
            session.statement_kind("DELETE FROM t").unwrap(),
            StmtKind::Write
        );
    }

    /// A transaction stages its writes: the session reads its own, a peer
    /// sharing the catalog sees nothing until COMMIT, and ROLLBACK discards.
    #[test]
    fn transaction_stages_writes_until_commit() {
        let mut writer = Session::new();
        writer.run("CREATE TABLE t (a BIGINT)").unwrap();
        writer.run("INSERT INTO t VALUES (1)").unwrap();
        let mut peer = Session::with_catalog(writer.shared_catalog());

        assert_eq!(writer.control_transaction("BEGIN").unwrap(), Some(true));
        assert!(writer.in_transaction());
        writer.run("INSERT INTO t VALUES (2)").unwrap();

        // The transaction reads its own write; the peer does not see it.
        assert_eq!(
            writer.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
        );
        assert_eq!(
            peer.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        assert_eq!(writer.control_transaction("COMMIT").unwrap(), Some(false));
        assert!(!writer.in_transaction());
        assert_eq!(
            peer.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
        );

        // ROLLBACK discards everything staged since BEGIN.
        writer.control_transaction("BEGIN").unwrap();
        writer.run("INSERT INTO t VALUES (3)").unwrap();
        writer.run("DELETE FROM t WHERE a = 1").unwrap();
        assert_eq!(writer.control_transaction("ROLLBACK").unwrap(), Some(false));
        assert_eq!(
            writer.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
        );
    }

    /// A commit that would discard a peer's writes is refused, rather than
    /// silently overwriting them. The refused transaction is over, so its
    /// staged writes are gone -- the statements must be retried, not the
    /// COMMIT alone.
    #[test]
    fn a_conflicting_commit_is_refused() {
        let mut first = Session::new();
        first.run("CREATE TABLE t (a BIGINT)").unwrap();
        let mut second = Session::with_catalog(first.shared_catalog());

        first.control_transaction("BEGIN").unwrap();
        first.run("INSERT INTO t VALUES (1)").unwrap();
        // The peer commits first, moving the shared catalog.
        second.run("INSERT INTO t VALUES (2)").unwrap();

        assert!(matches!(
            first.control_transaction("COMMIT"),
            Err(DriverError::Txn(TxnErrorKind::WriteConflict))
        ));
        assert!(!first.in_transaction(), "a refused commit ends the txn");
        // The peer's write survived; the refused one did not.
        assert_eq!(
            second.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(2)]])
        );
    }

    /// BEGIN inside an open transaction implicitly commits it, as in Go, and
    /// COMMIT/ROLLBACK outside one is a no-op, as in MySQL.
    #[test]
    fn nested_begin_commits_and_stray_commit_is_a_no_op() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        assert_eq!(session.control_transaction("COMMIT").unwrap(), Some(false));
        assert_eq!(
            session.control_transaction("ROLLBACK").unwrap(),
            Some(false)
        );

        session.control_transaction("BEGIN").unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();
        // The implicit commit publishes the first transaction's write.
        session.control_transaction("START TRANSACTION").unwrap();
        session.run("INSERT INTO t VALUES (2)").unwrap();
        session.control_transaction("ROLLBACK").unwrap();
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // A non-transaction statement is not claimed by the hook.
        assert_eq!(session.control_transaction("SELECT 1").unwrap(), None);
        assert!(session
            .control_transaction("ROLLBACK TO SAVEPOINT s")
            .is_err());
    }

    /// The single value a one-column, one-row query returns, as text.
    fn scalar_text(session: &mut Session, sql: &str) -> Option<String> {
        match session.run(sql).unwrap() {
            StmtResult::Rows(rows) => datum_text(&rows[0][0]),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// SET and the variable reads a connecting client performs.
    #[test]
    fn session_variables() {
        let mut session = Session::new();

        // A stock client's opening statements.
        assert_eq!(session.apply_set("SET NAMES utf8mb4").unwrap(), Some(()));
        assert_eq!(
            session.vars().get_system("character_set_client").unwrap(),
            "utf8mb4"
        );
        assert_eq!(session.apply_set("SET autocommit = 0").unwrap(), Some(()));
        // Go's checkBoolSystemVar canonicalizes 0/1 to OFF/ON.
        assert_eq!(session.vars().get_system("autocommit").unwrap(), "OFF");

        // Reading variables back through a query.
        assert_eq!(
            scalar_text(&mut session, "SELECT @@autocommit"),
            Some("OFF".to_owned())
        );
        let comment = scalar_text(&mut session, "SELECT @@version_comment").unwrap();
        assert!(
            comment.starts_with("TiDB Server (Apache License 2.0)"),
            "{comment}"
        );

        // DEFAULT restores the registry default.
        session.apply_set("SET autocommit = DEFAULT").unwrap();
        assert_eq!(session.vars().get_system("autocommit").unwrap(), "ON");

        // An unknown system variable is Go's 1193, on read and on write.
        assert!(matches!(
            session.apply_set("SET nonexistent_variable = 1"),
            Err(DriverError::Var(
                tidb_executor::VarErrorKind::UnknownSystemVariable(_)
            ))
        ));
        assert!(matches!(
            session.run("SELECT @@nonexistent_variable"),
            Err(DriverError::Var(
                tidb_executor::VarErrorKind::UnknownSystemVariable(_)
            ))
        ));
        // A read-only variable cannot be set.
        assert!(matches!(
            session.apply_set("SET version = '1'"),
            Err(DriverError::Var(
                tidb_executor::VarErrorKind::ReadOnlyVariable(_)
            ))
        ));

        // User variables: unset reads as NULL, never an error.
        assert_eq!(
            session.run("SELECT @nope").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null]])
        );
        session.apply_set("SET @x = 41 + 1").unwrap();
        assert_eq!(
            scalar_text(&mut session, "SELECT @x"),
            Some("42".to_owned())
        );

        // A non-SET statement is not claimed by the hook.
        assert_eq!(session.apply_set("SELECT 1").unwrap(), None);
    }

    /// Transcreated from Go `pkg/executor/test/ddl/ddl_test.go`
    /// `TestCreateDropDatabase`, case for case, minus the parts that need
    /// tiers this seed does not have yet.
    ///
    /// NOT PORTED from that Go test (documented): every `charset`/`collate`
    /// database option and its `SHOW CREATE DATABASE` output, which need the
    /// charset tier; the `drop database mysql` rejection, which needs the
    /// system schemas; and the privilege/role cases.
    #[test]
    fn create_drop_database() {
        let mut session = Session::new();

        // tk.MustExec("create database if not exists drop_test;")
        session
            .run("CREATE DATABASE IF NOT EXISTS drop_test")
            .unwrap();
        // tk.MustExec("drop database if exists drop_test;")
        session.run("DROP DATABASE IF EXISTS drop_test").unwrap();
        // tk.MustExec("create database drop_test;")
        session.run("CREATE DATABASE drop_test").unwrap();
        // tk.MustExec("use drop_test;")
        session.run("USE drop_test").unwrap();
        assert_eq!(session.current_database(), "drop_test");
        // tk.MustExec("drop database drop_test;")
        session.run("DROP DATABASE drop_test").unwrap();

        // tk.MustGetDBError("drop table t;", plannererrors.ErrNoDB)
        // tk.MustGetDBError("select * from t;", plannererrors.ErrNoDB)
        // Dropping the current database leaves none selected.
        assert_eq!(session.current_database(), "");
        assert!(matches!(
            session.run("SELECT * FROM t"),
            Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected))
        ));
        assert!(matches!(
            session.run("INSERT INTO t VALUES (1)"),
            Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected))
        ));

        // Creating a database that exists is Go's ErrDBCreateExists unless
        // IF NOT EXISTS was written.
        session.run("CREATE DATABASE drop_test").unwrap();
        assert!(matches!(
            session.run("CREATE DATABASE drop_test"),
            Err(DriverError::Schema(SchemaErrorKind::DatabaseExists(_)))
        ));
        session
            .run("CREATE DATABASE IF NOT EXISTS drop_test")
            .unwrap();
        // Dropping one that does not exist is ErrDBDropExists unless IF EXISTS.
        session.run("DROP DATABASE drop_test").unwrap();
        assert!(matches!(
            session.run("DROP DATABASE drop_test"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
        ));
        session.run("DROP DATABASE IF EXISTS drop_test").unwrap();

        // USE on an unknown schema is Go's ErrDatabaseNotExists.
        assert!(matches!(
            session.run("USE no_such_database"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
        ));
    }

    /// SHOW DATABASES and SHOW TABLES, with Go's column naming and ordering.
    #[test]
    fn show_databases_and_tables() {
        let mut session = Session::new();
        session.run("CREATE TABLE zeta (a BIGINT)").unwrap();
        session.run("CREATE TABLE alpha (a BIGINT)").unwrap();
        session.run("CREATE DATABASE other").unwrap();

        // Go's fetchShowDatabases sorts the names, then moves
        // information_schema to the front; the column is "Database".
        match session.run_with_columns("SHOW DATABASES").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "Database");
                assert_eq!(
                    rows.iter()
                        .map(|row| datum_text(&row[0]).unwrap())
                        .collect::<Vec<_>>(),
                    vec![
                        "INFORMATION_SCHEMA".to_owned(),
                        "other".to_owned(),
                        "test".to_owned()
                    ]
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // Go names the column Tables_in_<db> and sorts the table names.
        match session.run_with_columns("SHOW TABLES").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "Tables_in_test");
                assert_eq!(
                    rows.iter()
                        .map(|row| datum_text(&row[0]).unwrap())
                        .collect::<Vec<_>>(),
                    vec!["alpha".to_owned(), "zeta".to_owned()]
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // SHOW TABLES IN <db> reports that schema, and an empty one is empty.
        match session.run_with_columns("SHOW TABLES IN other").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "Tables_in_other");
                assert!(rows.is_empty());
            }
            other => panic!("expected rows, got {other:?}"),
        }
        // An unknown schema is Go's ErrBadDB.
        assert!(matches!(
            session.run("SHOW TABLES IN nope"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
        ));
    }

    /// A table in another schema is reachable by qualifying it, which is what
    /// makes the schema tier more than a listing.
    #[test]
    fn a_qualified_name_resolves_across_schemas() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(
            session.run("SELECT a FROM test.t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
    }

    /// USE changes where unqualified names resolve, which is the point of the
    /// schema tier: the same table name in two schemas is two tables.
    #[test]
    fn use_changes_unqualified_name_resolution() {
        let mut session = Session::new();
        session.run("CREATE DATABASE other").unwrap();

        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();

        session.run("USE other").unwrap();
        // `t` here is a different table, in the other schema.
        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (2)").unwrap();
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(2)]])
        );
        // The first schema's table is still reachable by qualifying it.
        assert_eq!(
            session.run("SELECT a FROM test.t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        assert_eq!(
            session.run_with_columns("SHOW TABLES").unwrap(),
            StmtOutput::Rows {
                columns: vec![(
                    "Tables_in_other".to_owned(),
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString)
                )],
                rows: vec![vec![Datum::Bytes(b"t".to_vec())]],
            }
        );

        session.run("USE test").unwrap();
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        // Writes follow the current schema too.
        session.run("UPDATE t SET a = 10").unwrap();
        assert_eq!(
            session.run("SELECT a FROM other.t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(2)]])
        );
    }

    /// SHOW COLUMNS / DESCRIBE, with Go's ColDesc field names and key flags.
    #[test]
    fn show_columns_and_describe() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, \
                 tag VARCHAR(4), v BIGINT, KEY tag_idx (tag))",
            )
            .unwrap();

        let describe = |session: &mut Session, sql: &str| match session
            .run_with_columns(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        {
            StmtOutput::Rows { columns, rows } => (
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                rows.into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| match value {
                                Datum::Null => "NULL".to_owned(),
                                other => datum_text(other).unwrap_or_default(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            ),
            other => panic!("expected rows, got {other:?}"),
        };

        let (names, rows) = describe(&mut session, "SHOW COLUMNS FROM t");
        assert_eq!(names, ["Field", "Type", "Null", "Key", "Default", "Extra"]);
        assert_eq!(
            rows,
            vec![
                // A handle primary key is NOT NULL and PRI, as Go marks it.
                vec!["id", "bigint(20)", "NO", "PRI", "NULL", ""],
                // A column that is the whole of a unique index is UNI.
                vec!["code", "varchar(8)", "YES", "UNI", "NULL", ""],
                // A column leading a non-unique index is MUL.
                vec!["tag", "varchar(4)", "YES", "MUL", "NULL", ""],
                // An unindexed column has no key flag.
                vec!["v", "bigint(20)", "YES", "", "NULL", ""],
            ]
        );

        // Go reports auto_increment in Extra; captured from TiDB's DESCRIBE:
        // [[id bigint(20) NO PRI <nil> auto_increment] [v bigint(20) YES  <nil> ]]
        session
            .run("CREATE TABLE ai (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
            .unwrap();
        assert_eq!(
            describe(&mut session, "DESCRIBE ai").1,
            vec![
                vec!["id", "bigint(20)", "NO", "PRI", "NULL", "auto_increment"],
                vec!["v", "bigint(20)", "YES", "", "NULL", ""],
            ]
        );

        // A column's stored DEFAULT shows in the Default column.
        session
            .run("CREATE TABLE withdef (a BIGINT DEFAULT 7, b VARCHAR(4) DEFAULT 'zz')")
            .unwrap();
        assert_eq!(
            describe(&mut session, "DESCRIBE withdef").1,
            vec![
                vec!["a", "bigint(20)", "YES", "", "7", ""],
                vec!["b", "varchar(4)", "YES", "", "zz", ""],
            ]
        );

        // DESCRIBE parses to the same node and answers identically.
        assert_eq!(describe(&mut session, "DESCRIBE t"), (names.clone(), rows));
        assert_eq!(describe(&mut session, "DESC t").0, names);

        // Another schema's table is reachable by qualifying the FROM.
        session.run("CREATE DATABASE other").unwrap();
        session.run("USE other").unwrap();
        assert_eq!(describe(&mut session, "SHOW COLUMNS FROM test.t").0, names);

        // Go's DESCRIBE takes an optional column, which narrows the output.
        session.run("USE test").unwrap();
        let (_, one) = describe(&mut session, "DESCRIBE t code");
        assert_eq!(
            one,
            vec![vec!["code", "varchar(8)", "YES", "UNI", "NULL", ""]]
        );

        // An unknown table is an error, not empty output.
        assert!(session.run("SHOW COLUMNS FROM nope").is_err());
    }

    /// SHOW CREATE TABLE, checked against output captured from real TiDB by
    /// running the same DDL through `pkg/executor/test/showtest` and printing
    /// `show create table`. Every expectation below is that captured text.
    #[test]
    fn show_create_table() {
        let mut session = Session::new();
        let create = |session: &mut Session, sql: &str, name: &str| {
            session.run(sql).unwrap();
            match session
                .run_with_columns(&format!("SHOW CREATE TABLE {name}"))
                .unwrap()
            {
                StmtOutput::Rows { columns, rows } => {
                    assert_eq!(
                        columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                        ["Table", "Create Table"]
                    );
                    assert_eq!(datum_text(&rows[0][0]).unwrap(), name);
                    datum_text(&rows[0][1]).unwrap()
                }
                other => panic!("expected rows, got {other:?}"),
            }
        };

        // Captured from TiDB verbatim.
        assert_eq!(
            create(
                &mut session,
                "create table t1 (id bigint primary key, code varchar(8) unique, \
                 tag varchar(4), v bigint, key tag_idx (tag))",
                "t1"
            ),
            "CREATE TABLE `t1` (\n  \
             `id` bigint(20) NOT NULL,\n  \
             `code` varchar(8) DEFAULT NULL,\n  \
             `tag` varchar(4) DEFAULT NULL,\n  \
             `v` bigint(20) DEFAULT NULL,\n  \
             PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n  \
             KEY `tag_idx` (`tag`),\n  \
             UNIQUE KEY `code` (`code`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        assert_eq!(
            create(
                &mut session,
                "create table t2 (a bigint default 7, b varchar(4) default 'zz', \
                 c bigint not null, d bigint)",
                "t2"
            ),
            "CREATE TABLE `t2` (\n  \
             `a` bigint(20) DEFAULT '7',\n  \
             `b` varchar(4) DEFAULT 'zz',\n  \
             `c` bigint(20) NOT NULL,\n  \
             `d` bigint(20) DEFAULT NULL\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        assert_eq!(
            create(
                &mut session,
                "create table t4 (a bigint, b bigint, key ab (a,b))",
                "t4"
            ),
            "CREATE TABLE `t4` (\n  \
             `a` bigint(20) DEFAULT NULL,\n  \
             `b` bigint(20) DEFAULT NULL,\n  \
             KEY `ab` (`a`,`b`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        // Index order: table constraints first, then inline ones in column
        // order -- also captured from TiDB.
        assert_eq!(
            create(
                &mut session,
                "create table x1 (a bigint unique, b bigint unique, key kb (b))",
                "x1"
            ),
            "CREATE TABLE `x1` (\n  \
             `a` bigint(20) DEFAULT NULL,\n  \
             `b` bigint(20) DEFAULT NULL,\n  \
             KEY `kb` (`b`),\n  \
             UNIQUE KEY `a` (`a`),\n  \
             UNIQUE KEY `b` (`b`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        // A string primary key is now a clustered common handle, so this
        // matches TiDB's captured output exactly. The previous commit
        // reported NONCLUSTERED, truthfully, because no common handle
        // existed then.
        assert_eq!(
            create(
                &mut session,
                "create table t3 (k varchar(10) primary key)",
                "t3"
            ),
            "CREATE TABLE `t3` (\n  \
             `k` varchar(10) NOT NULL,\n  \
             PRIMARY KEY (`k`) /*T![clustered_index] CLUSTERED */\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        // AUTO_INCREMENT, captured from TiDB verbatim.
        assert_eq!(
            create(
                &mut session,
                "create table a1 (id bigint auto_increment primary key, v bigint)",
                "a1"
            ),
            "CREATE TABLE `a1` (\n  \
             `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  \
             `v` bigint(20) DEFAULT NULL,\n  \
             PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        // An unknown table is an error, and another schema is reachable.
        assert!(session.run("SHOW CREATE TABLE nope").is_err());
        session.run("CREATE DATABASE other").unwrap();
        session.run("USE other").unwrap();
        assert!(session.run("SHOW CREATE TABLE test.t1").is_ok());
    }

    /// LAST_INSERT_ID, checked against a sequence captured from real TiDB:
    /// 0, 1, 2 (the FIRST id of a multi-row insert), unchanged by an explicit
    /// value, then 101 and 102, and unchanged by a non-allocating statement.
    #[test]
    fn last_insert_id() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE a (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
            .unwrap();
        let read = |session: &mut Session| match session.run("SELECT LAST_INSERT_ID()").unwrap() {
            StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };

        assert_eq!(read(&mut session), "0", "captured: start");
        session.run("INSERT INTO a (v) VALUES (10)").unwrap();
        assert_eq!(read(&mut session), "1", "captured: after single auto");
        session
            .run("INSERT INTO a (v) VALUES (20), (30), (40)")
            .unwrap();
        assert_eq!(
            read(&mut session),
            "2",
            "captured: a multi-row insert reports its FIRST id"
        );
        session.run("INSERT INTO a VALUES (100, 50)").unwrap();
        assert_eq!(
            read(&mut session),
            "2",
            "captured: an explicit value leaves it unchanged"
        );
        session.run("INSERT INTO a (v) VALUES (60)").unwrap();
        assert_eq!(read(&mut session), "101", "captured: after auto again");
        session.run("INSERT INTO a VALUES (NULL, 70)").unwrap();
        assert_eq!(read(&mut session), "102", "captured: NULL allocates");

        // A table with no auto column, and an UPDATE, both leave it alone.
        session
            .run("CREATE TABLE b (id BIGINT PRIMARY KEY)")
            .unwrap();
        session.run("INSERT INTO b VALUES (5)").unwrap();
        assert_eq!(read(&mut session), "102", "captured: non-auto insert");
        session.run("UPDATE a SET v = 0 WHERE id = 1").unwrap();
        assert_eq!(read(&mut session), "102", "captured: after update");

        // The OK packet's field is per statement, so it is 0 for a statement
        // that allocated nothing, unlike the sticky function value.
        session.run("INSERT INTO a (v) VALUES (80)").unwrap();
        assert_eq!(session.statement_insert_id(), 103);
        session.run("INSERT INTO b VALUES (6)").unwrap();
        assert_eq!(session.statement_insert_id(), 0);
        assert_eq!(session.last_insert_id(), 103);
    }

    /// information_schema, checked against output captured from a running
    /// TiDB: the column lists and the values for the same table definition.
    #[test]
    fn information_schema() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, \
                 code VARCHAR(8) UNIQUE, v BIGINT DEFAULT 7)",
            )
            .unwrap();

        let query = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap()
        {
            StmtOutput::Rows { columns, rows } => (
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                rows.into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| match value {
                                Datum::Null => "<nil>".to_owned(),
                                Datum::Int(v) => v.to_string(),
                                other => datum_text(other).unwrap_or_default(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            ),
            other => panic!("expected rows, got {other:?}"),
        };

        // SCHEMATA: captured column list, and a row per schema.
        let (names, rows) = query(&mut session, "SELECT * FROM information_schema.schemata");
        assert_eq!(
            names,
            [
                "CATALOG_NAME",
                "SCHEMA_NAME",
                "DEFAULT_CHARACTER_SET_NAME",
                "DEFAULT_COLLATION_NAME",
                "SQL_PATH",
                "TIDB_PLACEMENT_POLICY_NAME"
            ]
        );
        // Captured: [def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil> <nil>]
        assert_eq!(
            rows[0],
            vec![
                "def",
                "INFORMATION_SCHEMA",
                "utf8mb4",
                "utf8mb4_bin",
                "<nil>",
                "<nil>"
            ]
        );
        assert!(rows.iter().any(|row| row[1] == "test"));

        // TABLES: the captured 28-column list, and the captured values.
        let (names, rows) = query(&mut session, "SELECT * FROM information_schema.tables");
        assert_eq!(names.len(), 28, "the captured TABLES column count");
        assert_eq!(names[0], "TABLE_CATALOG");
        assert_eq!(names[27], "TIDB_STORAGE_CLASS");
        let row = rows.iter().find(|row| row[2] == "t").expect("table t");
        // Captured: def test t BASE TABLE InnoDB 10 Compact ...
        assert_eq!(
            &row[..7],
            ["def", "test", "t", "BASE TABLE", "InnoDB", "10", "Compact"]
        );
        assert_eq!(row[17], "utf8mb4_bin", "TABLE_COLLATION");
        assert_eq!(row[22], "NOT_SHARDED(PK_IS_HANDLE)");
        assert_eq!(row[23], "CLUSTERED");
        assert_eq!(row[25], "Normal");

        // COLUMNS: the captured 22-column list and per-column values.
        let (names, rows) = query(&mut session, "SELECT * FROM information_schema.columns");
        assert_eq!(names.len(), 22, "the captured COLUMNS column count");
        assert_eq!(names[4], "ORDINAL_POSITION");
        assert_eq!(names[21], "SRS_ID");
        let of = |name: &str| {
            rows.iter()
                .find(|row| row[2] == "t" && row[3] == name)
                .expect("column")
                .clone()
        };
        // Captured: def test t id 1 <nil> NO bigint <nil> <nil> 19 0 <nil>
        //           <nil> <nil> bigint(20) PRI auto_increment ...
        let id = of("id");
        assert_eq!(
            &id[..8],
            ["def", "test", "t", "id", "1", "<nil>", "NO", "bigint"]
        );
        assert_eq!(
            &id[8..15],
            ["<nil>", "<nil>", "19", "0", "<nil>", "<nil>", "<nil>"]
        );
        assert_eq!(
            &id[15..19],
            [
                "bigint(20)",
                "PRI",
                "auto_increment",
                "select,insert,update,references"
            ]
        );
        // Captured: code ... 8 32 <nil> <nil> <nil> utf8mb4 utf8mb4_bin
        //           varchar(8) UNI
        let code = of("code");
        assert_eq!(code[7], "varchar");
        assert_eq!(
            &code[8..16],
            [
                "8",
                "32",
                "<nil>",
                "<nil>",
                "<nil>",
                "utf8mb4",
                "utf8mb4_bin",
                "varchar(8)"
            ]
        );
        assert_eq!(code[16], "UNI");
        // Captured: v ... 7 YES bigint, no key
        let v = of("v");
        assert_eq!(v[5], "7", "COLUMN_DEFAULT");
        assert_eq!(v[6], "YES");
        assert_eq!(v[16], "");

        // A projected column is named as WRITTEN, which is captured TiDB
        // behavior: `select table_name ...` reports `table_name`, while
        // `select TABLE_NAME ...` reports `TABLE_NAME`.
        assert_eq!(
            query(
                &mut session,
                "SELECT table_name FROM information_schema.tables"
            )
            .0,
            ["table_name"]
        );
        assert_eq!(
            query(
                &mut session,
                "SELECT TABLE_NAME FROM information_schema.tables"
            )
            .0,
            ["TABLE_NAME"]
        );
        // A bare name works while that schema is current.
        session.run("USE information_schema").unwrap();
        assert_eq!(
            query(&mut session, "SELECT schema_name FROM schemata").0,
            ["schema_name"]
        );

        // An unimplemented information_schema table is an error, not empty
        // output that would look like a table with no rows.
        assert!(session
            .run("SELECT * FROM information_schema.statistics")
            .is_err());
    }

    /// DROP TABLE, checked against captured TiDB behavior: a missing name is
    /// 1051, IF EXISTS suppresses it, and a mixed list still drops the tables
    /// that exist BEFORE reporting the error.
    #[test]
    fn drop_table() {
        let mut session = Session::new();
        for name in ["d1", "d2", "d3"] {
            session
                .run(&format!("CREATE TABLE {name} (a BIGINT)"))
                .unwrap();
        }
        let tables = |session: &mut Session| match session.run_with_columns("SHOW TABLES").unwrap()
        {
            StmtOutput::Rows { rows, .. } => rows
                .into_iter()
                .map(|row| datum_text(&row[0]).unwrap())
                .collect::<Vec<_>>(),
            other => panic!("expected rows, got {other:?}"),
        };

        // Captured: [schema:1051]Unknown table 'test.nosuch'
        assert!(matches!(
            session.run("DROP TABLE nosuch"),
            Err(DriverError::Schema(SchemaErrorKind::BadTable(_)))
        ));
        // Captured: IF EXISTS is a no-op.
        session.run("DROP TABLE IF EXISTS nosuch").unwrap();

        // Captured: `drop table d1, nosuch` errors AND still drops d1.
        assert!(matches!(
            session.run("DROP TABLE d1, nosuch"),
            Err(DriverError::Schema(SchemaErrorKind::BadTable(_)))
        ));
        assert_eq!(tables(&mut session), vec!["d2".to_owned(), "d3".to_owned()]);

        // A multi-table drop removes them all.
        session.run("DROP TABLE d2, d3").unwrap();
        assert!(tables(&mut session).is_empty());

        // A dropped name can be recreated with a different shape, so the drop
        // removed the metadata rather than only the rows.
        session.run("CREATE TABLE d2 (b BIGINT)").unwrap();
        assert_eq!(
            session.run("SELECT b FROM d2").unwrap(),
            StmtResult::Rows(vec![])
        );
        // The rows are gone too: a recreated table starts empty.
        session.run("INSERT INTO d2 VALUES (1)").unwrap();
        session.run("DROP TABLE d2").unwrap();
        session.run("CREATE TABLE d2 (b BIGINT)").unwrap();
        assert_eq!(
            session.run("SELECT b FROM d2").unwrap(),
            StmtResult::Rows(vec![])
        );
    }

    /// ALTER TABLE ADD/DROP COLUMN, checked against captured TiDB behavior.
    /// The one that matters most: ADD COLUMN ... DEFAULT 7 makes rows written
    /// EARLIER read back 7, without rewriting them.
    #[test]
    fn alter_table_columns() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO a VALUES (1, 10), (2, 20)")
            .unwrap();

        // Captured: [[1 10 7] [2 20 7]] -- the existing rows take the default.
        session
            .run("ALTER TABLE a ADD COLUMN w BIGINT DEFAULT 7")
            .unwrap();
        assert_eq!(
            session.run("SELECT id, v, w FROM a").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1), Datum::Int(10), Datum::Int(7)],
                vec![Datum::Int(2), Datum::Int(20), Datum::Int(7)],
            ])
        );
        // Captured: without a default the existing rows read NULL.
        session.run("ALTER TABLE a ADD COLUMN x BIGINT").unwrap();
        assert_eq!(
            session.run("SELECT id, x FROM a").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1), Datum::Null],
                vec![Datum::Int(2), Datum::Null],
            ])
        );

        let columns = |session: &mut Session| match session.run_with_columns("DESCRIBE a").unwrap()
        {
            StmtOutput::Rows { rows, .. } => rows
                .into_iter()
                .map(|row| datum_text(&row[0]).unwrap())
                .collect::<Vec<_>>(),
            other => panic!("expected rows, got {other:?}"),
        };
        // Captured order after FIRST then AFTER v: y, id, v, z, w, x.
        session
            .run("ALTER TABLE a ADD COLUMN y BIGINT FIRST")
            .unwrap();
        session
            .run("ALTER TABLE a ADD COLUMN z BIGINT AFTER v")
            .unwrap();
        assert_eq!(columns(&mut session), ["y", "id", "v", "z", "w", "x"]);

        // A new column is written and read like any other, and the rows that
        // predate it still report their defaults.
        session
            .run("INSERT INTO a (id, v, w, x, y, z) VALUES (3, 30, 1, 2, 3, 4)")
            .unwrap();
        assert_eq!(
            session.run("SELECT w FROM a WHERE id = 3").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: DROP COLUMN removes it from the schema.
        session.run("ALTER TABLE a DROP COLUMN w").unwrap();
        assert_eq!(columns(&mut session), ["y", "id", "v", "z", "x"]);
        assert!(session.run("SELECT w FROM a").is_err());

        // Captured error codes.
        assert!(matches!(
            session.run("ALTER TABLE a ADD COLUMN v BIGINT"),
            Err(DriverError::DuplicateColumnName(_))
        ));
        assert!(matches!(
            session.run("ALTER TABLE a DROP COLUMN nosuch"),
            Err(DriverError::UnknownColumnInAlter(_))
        ));
        session.run("CREATE TABLE one (a BIGINT)").unwrap();
        assert!(matches!(
            session.run("ALTER TABLE one DROP COLUMN a"),
            Err(DriverError::CannotDropOnlyColumn { .. })
        ));
        assert!(matches!(
            session.run("ALTER TABLE a DROP COLUMN id"),
            Err(DriverError::UnsupportedDropIntegerPrimaryKey)
        ));

        // Captured: a SINGLE-column index is dropped along with its column,
        // while a COMPOSITE one refuses the drop with 8200.
        session
            .run("CREATE TABLE ix (a BIGINT, b BIGINT, KEY kb (b))")
            .unwrap();
        session.run("INSERT INTO ix VALUES (1, 2)").unwrap();
        session.run("ALTER TABLE ix DROP COLUMN b").unwrap();
        let create_ix = match session.run_with_columns("SHOW CREATE TABLE ix").unwrap() {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert!(!create_ix.contains("kb"), "the index went with the column");
        assert_eq!(
            session.run("SELECT a FROM ix").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // A unique single-column index behaves the same way.
        session
            .run("CREATE TABLE uq (a BIGINT, b BIGINT, UNIQUE KEY ua (a))")
            .unwrap();
        session.run("ALTER TABLE uq DROP COLUMN a").unwrap();

        // A composite index refuses it, and the table is unchanged.
        session
            .run("CREATE TABLE comp (a BIGINT, b BIGINT, c BIGINT, KEY kab (a, b))")
            .unwrap();
        assert!(matches!(
            session.run("ALTER TABLE comp DROP COLUMN a"),
            Err(DriverError::CannotDropColumnWithCompositeIndex(_))
        ));
        let create_comp = match session.run_with_columns("SHOW CREATE TABLE comp").unwrap() {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert!(create_comp.contains("KEY `kab` (`a`,`b`)"));

        // An unknown table is an error, and an action this tier does not
        // implement is still rejected rather than ignored. (RENAME TO used to
        // be this example; it is implemented now.)
        assert!(session
            .run("ALTER TABLE nosuch ADD COLUMN a BIGINT")
            .is_err());
        assert!(session.run("ALTER TABLE a ORDER BY v").is_err());
    }

    /// CREATE INDEX / DROP INDEX / ALTER TABLE ADD INDEX, checked against
    /// captured TiDB behavior -- including that CREATE INDEX backfills the
    /// rows that already exist.
    #[test]
    fn index_ddl() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE i1 (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO i1 VALUES (1, 10, 1), (2, 20, 1), (3, 10, 2)")
            .unwrap();

        // The index is backfilled, so it finds rows written before it existed.
        // Captured: select id from i1 where a = 10 -> [[1] [3]].
        session.run("CREATE INDEX ia ON i1 (a)").unwrap();
        assert_eq!(
            session.run("SELECT id FROM i1 WHERE a = 10").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(3)]])
        );
        // SHOW CREATE TABLE reports it, captured as KEY `ia` (`a`).
        let create =
            |session: &mut Session| match session.run_with_columns("SHOW CREATE TABLE i1").unwrap()
            {
                StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
                other => panic!("expected rows, got {other:?}"),
            };
        assert!(create(&mut session).contains("KEY `ia` (`a`)"));

        // Captured: a duplicate index name is 1061.
        assert!(matches!(
            session.run("CREATE INDEX ia ON i1 (b)"),
            Err(DriverError::DuplicateKeyName(_))
        ));
        // Captured: a unique index over data that already collides is 1062
        // naming table.index, and the index is NOT created.
        match session.run("CREATE UNIQUE INDEX ua ON i1 (a)") {
            Err(DriverError::DuplicateEntry { value, key }) => {
                assert_eq!(value, "10");
                assert_eq!(key, "i1.ua");
            }
            other => panic!("expected a duplicate-entry error, got {other:?}"),
        }
        assert!(!create(&mut session).contains("ua"));

        // A unique index over data that does not collide is created.
        session.run("CREATE UNIQUE INDEX ub ON i1 (b, a)").unwrap();
        assert!(create(&mut session).contains("UNIQUE KEY `ub` (`b`,`a`)"));
        // It is enforced from then on.
        assert!(session.run("INSERT INTO i1 VALUES (4, 10, 1)").is_err());

        // DROP INDEX removes it, and its entries with it: the same insert now
        // succeeds.
        session.run("DROP INDEX ub ON i1").unwrap();
        assert!(!create(&mut session).contains("ub"));
        session.run("INSERT INTO i1 VALUES (4, 10, 1)").unwrap();

        // Captured: dropping one that does not exist is 1091.
        assert!(matches!(
            session.run("DROP INDEX nosuch ON i1"),
            Err(DriverError::UnknownIndex(_))
        ));

        // ALTER TABLE ADD INDEX takes the same path.
        session.run("ALTER TABLE i1 ADD INDEX ic (b)").unwrap();
        assert!(create(&mut session).contains("KEY `ic` (`b`)"));
        session.run("ALTER TABLE i1 DROP INDEX ic").unwrap();
        assert!(!create(&mut session).contains("ic"));

        // An index over an unknown column is rejected.
        assert!(session.run("CREATE INDEX bad ON i1 (nosuch)").is_err());
    }

    /// TRUNCATE TABLE, checked against captured TiDB behavior: the rows go,
    /// the definition stays, and the auto-increment counter restarts.
    #[test]
    fn truncate_table() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT, KEY kv (v))")
            .unwrap();
        session
            .run("INSERT INTO t1 (v) VALUES (1), (2), (3)")
            .unwrap();
        assert_eq!(
            session.run("SELECT id FROM t1").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)]
            ])
        );

        session.run("TRUNCATE TABLE t1").unwrap();
        // Captured: no rows remain.
        assert_eq!(
            session.run("SELECT id FROM t1").unwrap(),
            StmtResult::Rows(vec![])
        );
        // Captured: the next auto-increment insert starts over at 1.
        session.run("INSERT INTO t1 (v) VALUES (9)").unwrap();
        assert_eq!(
            session.run("SELECT id FROM t1").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        // Captured: the definition, including the index, survives.
        let create = match session.run_with_columns("SHOW CREATE TABLE t1").unwrap() {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert!(create.contains("AUTO_INCREMENT"));
        assert!(create.contains("KEY `kv` (`v`)"));

        // The index entries went with the rows: a read through the index sees
        // only what was written after the truncate.
        assert_eq!(
            session.run("SELECT id FROM t1 WHERE v = 1").unwrap(),
            StmtResult::Rows(vec![])
        );
        assert_eq!(
            session.run("SELECT id FROM t1 WHERE v = 9").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: truncating a table that does not exist is 1146.
        assert!(matches!(
            session.run("TRUNCATE TABLE nosuch"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
        ));
    }

    /// RENAME TABLE, checked against captured TiDB behavior.
    /// A result's rows as text, so an assertion does not depend on which
    /// datum kind the codec hands back for a given column type.
    fn row_text(result: Result<StmtResult, DriverError>) -> Vec<Vec<String>> {
        match result.unwrap() {
            StmtResult::Rows(rows) => rows
                .into_iter()
                .map(|row| {
                    row.iter()
                        .map(|v| datum_text(v).unwrap_or_else(|| "NULL".to_owned()))
                        .collect()
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// The three conflict policies -- `REPLACE`, `INSERT IGNORE` and
    /// `ON DUPLICATE KEY UPDATE` -- checked against captured TiDB output,
    /// including the affected-row counts, which is how MySQL clients tell
    /// an insert from an update.
    #[test]
    fn insert_conflict_policies() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT, UNIQUE KEY ub (b))",
            )
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'p',10),(2,'q',20)")
            .unwrap();

        // Captured: an update that changes nothing affects no rows, and
        // raises no warning.
        assert_eq!(
            session
                .run("INSERT INTO t (a,b,c) VALUES (1,'p',10) ON DUPLICATE KEY UPDATE c = c")
                .unwrap(),
            StmtResult::Affected(0)
        );
        assert!(session.warnings().is_empty());

        // Captured: VALUES(c) is the value the insert would have written, and
        // a real update affects two rows.
        assert_eq!(
            session
                .run(
                    "INSERT INTO t (a,b,c) VALUES (1,'p',77) ON DUPLICATE KEY UPDATE c = VALUES(c)"
                )
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT c FROM t WHERE a = 1")),
            [["77"]]
        );

        // Captured: the conflict is found on a UNIQUE INDEX too, and the
        // assignment updates THAT row -- the candidate's own key is never
        // inserted.
        assert_eq!(
            session
                .run("INSERT INTO t (a,b,c) VALUES (9,'q',5) ON DUPLICATE KEY UPDATE c = 42")
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
            [["1", "p", "77"], ["2", "q", "42"]]
        );

        // Captured: the assignments read the EXISTING row.
        assert_eq!(
            session
                .run("INSERT INTO t (a,b,c) VALUES (1,'p',1000) ON DUPLICATE KEY UPDATE c = c + 1")
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT c FROM t WHERE a = 1")),
            [["78"]]
        );

        // Captured: INSERT IGNORE skips the conflicting row with a 1062
        // warning and inserts the rest.
        assert_eq!(
            session
                .run("INSERT IGNORE INTO t (a,b,c) VALUES (1,'zzz',1),(5,'five',5)")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1062);
        assert_eq!(
            session.warnings()[0].message,
            "Duplicate entry '1' for key 't.PRIMARY'"
        );

        // Captured: REPLACE deletes EVERY row it collides with -- here one on
        // the primary key and another on the unique key -- and the affected
        // count is one per deleted row plus one for the inserted row.
        assert_eq!(
            session
                .run("REPLACE INTO t (a,b,c) VALUES (2,'five',99)")
                .unwrap(),
            StmtResult::Affected(3)
        );
        assert_eq!(
            row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
            [["1", "p", "78"], ["2", "five", "99"]]
        );
        // Captured: a REPLACE with no conflict is a plain insert.
        assert_eq!(
            session
                .run("REPLACE INTO t (a,b,c) VALUES (77,'new',1)")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY a")),
            [["1"], ["2"], ["77"]]
        );
    }

    /// `INSERT ... SELECT` and the `ORDER BY`/`LIMIT` forms of UPDATE and
    /// DELETE, checked against captured TiDB output.
    ///
    /// STILL REFUSED, each recorded at its gate: `REPLACE INTO`,
    /// `INSERT IGNORE`, `ON DUPLICATE KEY UPDATE` (all three need
    /// conflict-time row replacement), the `SET` insert syntax, partitions
    /// and `RETURNING`.
    #[test]
    fn insert_select_and_ordered_dml() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT)")
            .unwrap();
        session
            .run("CREATE TABLE u (x BIGINT, y VARCHAR(10))")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'p',10),(2,'q',20),(3,'r',30)")
            .unwrap();
        session
            .run("INSERT INTO u VALUES (7,'seven'),(8,'eight')")
            .unwrap();

        // Captured: INSERT ... SELECT inserts the query's rows, and the
        // columns it does not name stay NULL.
        assert_eq!(
            session
                .run("INSERT INTO t (a,b) SELECT x, y FROM u")
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
            [
                ["1", "p", "10"],
                ["2", "q", "20"],
                ["3", "r", "30"],
                ["7", "seven", "NULL"],
                ["8", "eight", "NULL"],
            ]
        );

        // Captured: UPDATE ... ORDER BY ... LIMIT updates that many rows, in
        // that order -- here the largest `a`.
        assert_eq!(
            session
                .run("UPDATE t SET c = 99 ORDER BY a DESC LIMIT 1")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            row_text(session.run("SELECT a, c FROM t ORDER BY a")),
            [
                ["1", "10"],
                ["2", "20"],
                ["3", "30"],
                ["7", "NULL"],
                ["8", "99"],
            ]
        );

        // Captured: DELETE ... ORDER BY ... LIMIT, and the WHERE + LIMIT form
        // whose cap counts rows DELETED rather than rows examined.
        assert_eq!(
            session
                .run("DELETE FROM t ORDER BY a DESC LIMIT 1")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY a")),
            [["1"], ["2"], ["3"], ["7"]]
        );
        assert_eq!(
            session.run("DELETE FROM t WHERE c > 0 LIMIT 2").unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY a")),
            [["3"], ["7"]]
        );

        // The SET insert syntax is the shape still refused here; REPLACE and
        // ON DUPLICATE KEY UPDATE are implemented (see
        // `insert_conflict_policies`).
        assert!(session.run("INSERT INTO t SET a = 42").is_err());
    }

    /// `ORDER BY` resolved against the SELECT list, checked against captured
    /// TiDB output.
    ///
    /// A positional `ORDER BY 1` used to rewrite as a constant here, which
    /// silently produced UNSORTED rows -- the worst kind of divergence, and
    /// the reason this unit was picked.
    #[test]
    fn order_by_resolves_against_the_select_list() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
        session
            .run("INSERT INTO t VALUES (1,30),(2,20),(3,10)")
            .unwrap();

        // Captured: an alias names a projected expression.
        assert_eq!(
            row_text(session.run("SELECT a, a*2 AS twice FROM t ORDER BY twice DESC")),
            [["3", "6"], ["2", "4"], ["1", "2"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a AS z FROM t ORDER BY z DESC")),
            [["3"], ["2"], ["1"]]
        );
        // Captured: an expression BUILT on an alias resolves too.
        assert_eq!(
            row_text(session.run("SELECT a*2 AS twice FROM t ORDER BY twice+0 DESC")),
            [["6"], ["4"], ["2"]]
        );
        // Captured: a bare integer is a 1-based output position.
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY 1 DESC")),
            [["3"], ["2"], ["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a, b FROM t ORDER BY 2")),
            [["3", "10"], ["2", "20"], ["1", "30"]]
        );
        // Captured: an alias SHADOWS a real column of the same name.
        assert_eq!(
            row_text(session.run("SELECT b AS a FROM t ORDER BY a")),
            [["10"], ["20"], ["30"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a+0 AS a FROM t ORDER BY a DESC")),
            [["3"], ["2"], ["1"]]
        );
        // Captured: a source column that is not projected still sorts.
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY b DESC")),
            [["1"], ["2"], ["3"]]
        );

        // Captured: an unknown name and an out-of-range position are both
        // 1054 naming the order clause.
        for sql in [
            "SELECT a FROM t ORDER BY nosuch",
            "SELECT a FROM t ORDER BY 5",
        ] {
            match session.run(sql) {
                Err(error) => {
                    let reported = error.to_mysql_error();
                    assert_eq!(reported.code, 1054, "{sql}");
                    assert!(
                        reported.message.ends_with("in 'order clause'"),
                        "{sql}: {}",
                        reported.message
                    );
                }
                Ok(other) => panic!("expected 1054 from {sql}, got {other:?}"),
            }
        }
    }

    /// The aggregates over each numeric domain, checked against captured
    /// TiDB output.
    ///
    /// The type is the load-bearing part: `SUM` over a BIGINT column is a
    /// DECIMAL in MySQL (captured type 246), not a BIGINT, so it sums in the
    /// decimal domain the way Go's `sum4Decimal` does. Only a real argument
    /// makes it a DOUBLE.
    #[test]
    fn aggregates_over_numeric_domains() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT, d DECIMAL(10,2), r DOUBLE)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,1.5,1.5),(2,2.25,2.5),(3,3.25,3.5)")
            .unwrap();

        // Captured: SUM over each domain, with the decimal column keeping
        // its own scale.
        assert_eq!(
            row_text(session.run("SELECT SUM(a), SUM(d), SUM(r) FROM t")),
            [["6", "7.00", "7.5"]]
        );
        // Captured: an empty SUM is NULL, not zero.
        assert_eq!(
            row_text(session.run("SELECT SUM(a) FROM t WHERE a > 100")),
            [["NULL"]]
        );
        // Captured: AVG and MIN/MAX over a decimal column.
        assert_eq!(
            row_text(session.run("SELECT MIN(d), MAX(d) FROM t")),
            [["1.50", "3.25"]]
        );
        assert_eq!(
            row_text(session.run("SELECT COUNT(DISTINCT a), COUNT(*) FROM t")),
            [["3", "3"]]
        );
        // Captured: grouped SUM over a decimal column.
        assert_eq!(
            row_text(session.run("SELECT a, SUM(d) FROM t GROUP BY a ORDER BY a")),
            [["1", "1.50"], ["2", "2.25"], ["3", "3.25"]]
        );
    }

    /// The math, conditional and TRIM builtins through the chunk executor,
    /// checked against captured TiDB output -- including the result TYPES,
    /// which are what size a chunk cell.
    ///
    /// The types are the subtle part and were read off TiDB's own result
    /// fields: `ABS` and `MOD` keep the argument's domain, `CEIL`/`FLOOR`
    /// return an integer for an integer OR decimal argument but stay real
    /// for a real one, `ROUND`/`TRUNCATE` keep the decimal domain, and the
    /// transcendental functions are always real.
    #[test]
    fn math_and_conditional_builtins() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'x',10),(2,'y',20)")
            .unwrap();

        // Captured: ABS keeps the argument's domain.
        assert_eq!(
            row_text(session.run("SELECT ABS(-3), ABS(-3.5)")),
            [["3", "3.5"]]
        );
        // Captured: CEIL/FLOOR of a decimal are integers, and of an integer
        // are the integer itself.
        assert_eq!(
            row_text(session.run("SELECT CEIL(1.2), FLOOR(1.8), CEIL(3), FLOOR(3)")),
            [["2", "1", "3", "3"]]
        );
        // Captured: ROUND keeps the decimal domain and rounds half away from
        // zero; TRUNCATE cuts instead.
        assert_eq!(
            row_text(
                session.run("SELECT ROUND(1.55,1), ROUND(1.55), ROUND(2.5), TRUNCATE(1.999,2)")
            ),
            [["1.6", "2", "3", "1.99"]]
        );
        // Captured: MOD follows its arguments.
        assert_eq!(
            row_text(session.run("SELECT MOD(7,3), MOD(7.5,3)")),
            [["1", "1.5"]]
        );
        // Captured: the always-real family.
        assert_eq!(
            row_text(session.run("SELECT POW(2,3), SQRT(9), LOG10(100)")),
            [["8", "3", "2"]]
        );
        // Captured: SIGN, CONV and CRC32.
        assert_eq!(
            row_text(session.run("SELECT SIGN(-2), CONV(255,10,16), CRC32('a')")),
            [["-1", "FF", "3904355907"]]
        );

        // Captured: GREATEST/LEAST take the merged argument type, and work
        // over strings as well as numbers.
        assert_eq!(
            row_text(session.run("SELECT GREATEST(1,2,3), LEAST(1,2,3), GREATEST('a','b')")),
            [["3", "1", "b"]]
        );
        // Captured: IF picks one branch, and NULLIF is NULL only on equality.
        assert_eq!(
            row_text(session.run("SELECT IF(1,'big','small'), NULLIF(1,1), NULLIF(1,2)")),
            [["big", "NULL", "1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a, IF(c>15,'big','small') FROM t")),
            [["1", "small"], ["2", "big"]]
        );

        // Captured: TRIM's three directions, and its implicit space.
        assert_eq!(
            row_text(session.run("SELECT TRIM(' x '), TRIM(LEADING 'x' FROM 'xxa')")),
            [["x", "a"]]
        );
        assert_eq!(
            row_text(session.run("SELECT TRIM(TRAILING 'a' FROM 'xaa'), SUBSTRING('abc',1,2)")),
            [["x", "ab"]]
        );

        // IF is lazy, so the branch not taken never runs -- a division by
        // zero there would otherwise warn.
        session.run("SELECT IF(1, 1, 1/0)").unwrap();
        assert!(session.warnings().is_empty());
    }

    /// The date/time family through the chunk executor, checked against
    /// captured TiDB output with `time_zone = '+00:00'`.
    ///
    /// Go fixes the statement clock once, so every `NOW()` in one statement
    /// agrees; the context carries that instant and the resolved session
    /// zone (Go `timeutil.ParseTimeZone`).
    ///
    /// DOCUMENTED DIVERGENCE, the same one the temporal casts carry: this
    /// crate's date/time builtins produce formatted STRINGS, so the reported
    /// column type is `VarString` where TiDB says `DATETIME`. The values
    /// match.
    #[test]
    fn date_time_builtins() {
        let mut session = Session::new();
        session.apply_set("SET time_zone = '+00:00'").unwrap();
        session.run("CREATE TABLE t (d VARCHAR(30))").unwrap();
        session
            .run("INSERT INTO t VALUES ('2020-03-05 06:07:08')")
            .unwrap();

        // Captured: the field extractors.
        assert_eq!(
            row_text(session.run(
                "SELECT MONTH(d), DAY(d), YEAR(d), DAYOFWEEK(d), DAYOFYEAR(d), WEEKDAY(d), QUARTER(d) FROM t"
            )),
            [["3", "5", "2020", "5", "65", "3", "1"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT MONTHNAME(d), DAYNAME(d), LAST_DAY(d), TO_DAYS(d), TIME_TO_SEC(d) FROM t"
            )),
            [["March", "Thursday", "2020-03-31", "737854", "22028"]]
        );
        assert_eq!(
            row_text(session.run("SELECT WEEK(d), WEEKOFYEAR(d), YEARWEEK(d) FROM t")),
            [["9", "10", "202009"]]
        );
        assert_eq!(
            row_text(session.run("SELECT SEC_TO_TIME(3661), MAKEDATE(2020,10), MAKETIME(1,2,3)")),
            [["01:01:01", "2020-01-10", "01:02:03"]]
        );
        assert_eq!(
            row_text(session.run("SELECT PERIOD_ADD(202001, 2), PERIOD_DIFF(202003, 202001)")),
            [["202003", "2"]]
        );

        // Captured: the statement clock is fixed, so NOW() agrees with
        // itself and prints a full second-resolution datetime.
        assert_eq!(
            row_text(session.run("SELECT NOW() = NOW(), LENGTH(NOW()) = 19")),
            [["1", "1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CURDATE() = CURDATE(), LENGTH(CURDATE()) = 10")),
            [["1", "1"]]
        );

        // The session zone reaches the clock: UTC and a +10 offset differ by
        // ten hours in the hour NOW() reports for the same instant.
        let hour_at = |session: &mut Session, zone: &str| -> i64 {
            session
                .apply_set(&format!("SET time_zone = '{zone}'"))
                .unwrap();
            match session.run("SELECT HOUR(NOW())").unwrap() {
                StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap().parse().unwrap(),
                other => panic!("expected rows, got {other:?}"),
            }
        };
        let utc = hour_at(&mut session, "+00:00");
        let plus_ten = hour_at(&mut session, "+10:00");
        assert_eq!((utc + 10) % 24, plus_ten);
    }

    /// `CAST(expr AS type)` and its `CONVERT`/`BINARY` spellings through the
    /// chunk executor, checked against captured TiDB output.
    ///
    /// The target type IS the operation in Go (it picks a
    /// `builtinCast*As*Sig` from it), so the rewriter puts the target in the
    /// function's result type and evaluation reads it back from there.
    ///
    /// STILL REFUSED, for the reason `cast::eval_cast` already records:
    /// `TIME` and `JSON` targets have no value domain in this crate, and the
    /// `ARRAY` modifier is a JSON multi-valued index.
    #[test]
    fn cast_and_convert() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'12abc',10),(2,'zz',20)")
            .unwrap();

        // Captured: a number to CHAR, and the width truncating it.
        assert_eq!(
            row_text(session.run("SELECT CAST(c AS CHAR) FROM t")),
            [["10"], ["20"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST(c AS CHAR(1)) FROM t")),
            [["1"], ["2"]]
        );

        // Captured: a string to a number takes the leading digits, or zero.
        assert_eq!(
            row_text(session.run("SELECT CAST(b AS SIGNED) FROM t")),
            [["12"], ["0"]]
        );
        // Captured: the rounding asymmetry -- a string keeps only the integer
        // prefix while a decimal or a float rounds.
        assert_eq!(
            row_text(session.run("SELECT CAST('3.7' AS SIGNED), CAST(3.7 AS SIGNED)")),
            [["3", "4"]]
        );
        // Captured: UNSIGNED wraps a negative rather than clamping it.
        assert_eq!(
            row_text(session.run("SELECT CAST(-1 AS UNSIGNED)")),
            [["18446744073709551615"]]
        );

        // Captured: DECIMAL rounds to the written scale, and pads to it.
        assert_eq!(
            row_text(session.run("SELECT CAST('12.345' AS DECIMAL(6,2))")),
            [["12.35"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST(1 AS DECIMAL(6,2))")),
            [["1.00"]]
        );

        // Captured: the temporal targets.
        assert_eq!(
            row_text(session.run("SELECT CAST('2020-01-02' AS DATE)")),
            [["2020-01-02"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST('2020-1-2' AS DATE)")),
            [["2020-01-02"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST('2020-01-02 03:04:05' AS DATETIME)")),
            [["2020-01-02 03:04:05"]]
        );

        // Captured: BINARY(n) pads with NUL rather than truncating short.
        assert_eq!(
            row_text(session.run("SELECT CAST(b AS BINARY(3)) FROM t")),
            [["12a"], ["zz\u{0}"]]
        );

        // Captured: CONVERT and the BINARY operator are the same node.
        assert_eq!(
            row_text(session.run("SELECT CONVERT(c, CHAR), CONVERT('7', SIGNED) FROM t")),
            [["10", "7"], ["20", "7"]]
        );
        assert_eq!(
            row_text(session.run("SELECT BINARY b FROM t")),
            [["12abc"], ["zz"]]
        );

        // Captured: NULL casts to NULL, and a cast result is an ordinary
        // operand afterwards.
        assert_eq!(
            row_text(session.run("SELECT CAST(NULL AS SIGNED) IS NULL")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST(c AS DOUBLE)/2 FROM t")),
            [["5"], ["10"]]
        );

        // The refusals are refusals, not wrong answers.
        assert!(session.run("SELECT CAST(c AS TIME) FROM t").is_err());
        assert!(session.run("SELECT CAST(c AS JSON) FROM t").is_err());
    }

    /// LIKE, BETWEEN, CASE and the ordinary builtins through the chunk
    /// executor, checked against captured TiDB output.
    ///
    /// These forms all existed in `tidb_expr`'s AST evaluator already; what
    /// was missing was the rewriter building them for chunk evaluation, so a
    /// query using any of them failed outright.
    ///
    /// STILL REFUSED, each for its own reason recorded at
    /// `tidb_expr::rewriter::builtin_return_type`: the session-state
    /// functions (`DATABASE`, `VERSION`, `CURRENT_USER`, `NOW`) need a
    /// resolver carrying session state into the chunk path, `CAST`/`CONVERT`
    /// take a target type rather than a value, `GROUP_CONCAT` is an
    /// aggregate, and the `DATE_ADD` family takes an `Expr::Interval`.
    #[test]
    fn like_between_case_and_builtins() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT, KEY kb (b))")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'xy',10),(2,'Yz',20),(3,'z',30)")
            .unwrap();

        // Captured: LIKE's wildcards, its negation and its escape.
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b LIKE 'x%'")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b LIKE '%y%'")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b LIKE 'x_'")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b NOT LIKE 'x%'")),
            [["2"], ["3"]]
        );
        assert_eq!(row_text(session.run(r"SELECT 'a%b' LIKE 'a\%b'")), [["1"]]);
        assert_eq!(
            row_text(session.run("SELECT b FROM t WHERE b LIKE '%'")),
            [["xy"], ["Yz"], ["z"]]
        );

        // Captured: BETWEEN is inclusive, and its negation is the complement.
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE c BETWEEN 10 AND 20")),
            [["1"], ["2"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE c NOT BETWEEN 10 AND 20")),
            [["3"]]
        );

        // Captured: the searched CASE, the simple CASE, a NULL condition
        // (which is not a match), and a missing ELSE (which is NULL).
        assert_eq!(
            row_text(session.run("SELECT a, CASE WHEN c > 15 THEN 'hi' ELSE 'lo' END FROM t")),
            [["1", "lo"], ["2", "hi"], ["3", "hi"]]
        );
        assert_eq!(
            row_text(
                session.run("SELECT CASE c WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' END FROM t")
            ),
            [["ten"], ["twenty"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CASE WHEN NULL THEN 'x' ELSE 'y' END")),
            [["y"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CASE WHEN c > 100 THEN 'x' END FROM t")),
            [["NULL"], ["NULL"], ["NULL"]]
        );

        // Captured: the string builtins, including LENGTH counting bytes
        // while CHAR_LENGTH counts characters.
        assert_eq!(
            row_text(
                session.run(
                    "SELECT CONCAT(b,'!'), UPPER(b), LOWER(b), LENGTH(b), CHAR_LENGTH(b) FROM t"
                )
            ),
            [
                ["xy!", "XY", "xy", "2", "2"],
                ["Yz!", "YZ", "yz", "2", "2"],
                ["z!", "Z", "z", "1", "1"],
            ]
        );
        assert_eq!(
            row_text(session.run("SELECT LENGTH('héllo'), CHAR_LENGTH('héllo')")),
            [["6", "5"]]
        );

        // Captured: COALESCE and IFNULL over a column and a literal, whose
        // branch types Go merges to one string type.
        assert_eq!(
            row_text(
                session.run("SELECT COALESCE(NULL, b), IFNULL(b,'n'), IFNULL(NULL,'n') FROM t")
            ),
            [["xy", "xy", "n"], ["Yz", "Yz", "n"], ["z", "z", "n"],]
        );

        // Captured: DATABASE() and its SCHEMA() synonym report the current
        // database, and VERSION() reports the same string as @@version.
        assert_eq!(
            row_text(session.run("SELECT DATABASE(), SCHEMA()")),
            [["test", "test"]]
        );
        let version = match session.run("SELECT VERSION()").unwrap() {
            StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert_eq!(version, session.vars().get_system("version").unwrap());
        assert!(version.contains("TiDB"), "{version}");
        // Captured: with no database selected, DATABASE() is NULL.
        let mut fresh = Session::new();
        fresh.run("DROP DATABASE test").unwrap();
        assert_eq!(row_text(fresh.run("SELECT DATABASE()")), [["NULL"]]);

        // The refusals above are refusals, not wrong answers. (CAST used to
        // be this example; it is built now -- see `cast_and_convert`.)
        for sql in ["SELECT CURRENT_USER()", "SELECT GROUP_CONCAT(b) FROM t"] {
            assert!(session.run(sql).is_err(), "{sql} should still be refused");
        }
    }

    /// Go `getDefaultValue` + `checkDefaultValue`: a written DEFAULT is
    /// normalized and checked against the column's own type at DDL time,
    /// checked against captured TiDB output.
    ///
    /// NOT PORTED: the function-call defaults (`CURRENT_TIMESTAMP`), the
    /// ENUM/SET forms with their own index rules, and BIT columns -- each is
    /// its own arm of Go's `getDefaultValue` and none of those column types
    /// reaches this tier yet.
    #[test]
    fn column_default_is_normalized_and_checked() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (a BIGINT, d DECIMAL(10,3) DEFAULT 1.5, \
                 i INT DEFAULT 7.6, v VARCHAR(4) DEFAULT 'ab')",
            )
            .unwrap();

        // Captured: only the integer and float/double types normalize their
        // stored default, so SHOW CREATE reports 8 for the INT column while
        // the DECIMAL column keeps the literal as written.
        let created = show_create(&mut session, "t");
        assert!(
            created.contains("`d` decimal(10,3) DEFAULT '1.5'"),
            "{created}"
        );
        assert!(created.contains("`i` int(11) DEFAULT '8'"), "{created}");
        assert!(created.contains("`v` varchar(4) DEFAULT 'ab'"), "{created}");

        // Captured: a row that takes the defaults casts them to the column,
        // so the decimal reaches the column's own scale here.
        session.run("INSERT INTO t (a) VALUES (1)").unwrap();
        assert_eq!(
            row_text(session.run("SELECT a, d, i, v FROM t")),
            [["1", "1.500", "8", "ab"]]
        );

        // Captured: a default the column cannot hold is 1067 at DDL time.
        for sql in [
            "CREATE TABLE w (v VARCHAR(4) DEFAULT 'abcdefg')",
            "CREATE TABLE x (i INT DEFAULT 'zz')",
        ] {
            match session.run(sql) {
                Err(error) => {
                    let reported = error.to_mysql_error();
                    assert_eq!(reported.code, 1067, "{sql}");
                    assert!(
                        reported.message.starts_with("Invalid default value for "),
                        "{sql}: {}",
                        reported.message
                    );
                }
                Ok(other) => panic!("expected 1067 from {sql}, got {other:?}"),
            }
        }
        // A numeric string a column CAN hold is accepted and kept.
        session.run("CREATE TABLE y (i INT DEFAULT '12')").unwrap();
        session
            .run("INSERT INTO y (i) VALUES (DEFAULT)")
            .unwrap_or_else(|_| {
                // `VALUES (DEFAULT)` is not parsed at this tier; an omitted
                // column takes the same path.
                session.run("INSERT INTO y () VALUES ()").unwrap()
            });
        assert_eq!(row_text(session.run("SELECT i FROM y")), [["12"]]);

        // Captured: ALTER TABLE ADD COLUMN runs the same normalization and
        // check, and existing rows read the cast default.
        session
            .run("ALTER TABLE t ADD COLUMN e DECIMAL(6,2) DEFAULT 3.14159")
            .unwrap();
        let created = show_create(&mut session, "t");
        assert!(
            created.contains("`e` decimal(6,2) DEFAULT '3.14159'"),
            "{created}"
        );
        assert_eq!(row_text(session.run("SELECT e FROM t")), [["3.14"]]);
        assert!(matches!(
            session.run("ALTER TABLE t ADD COLUMN f VARCHAR(2) DEFAULT 'toolong'"),
            Err(DriverError::InvalidDefault(_))
        ));
    }

    /// The `Create Table` text of one table.
    fn show_create(session: &mut Session, table: &str) -> String {
        match session
            .run_with_columns(&format!("SHOW CREATE TABLE {table}"))
            .unwrap()
        {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// Go `table.CastValue`: a written value takes its column's type, checked
    /// against captured TiDB output.
    ///
    /// NOT PORTED from Go's own suites: the temporal columns (a DATE/DATETIME
    /// column's zero-date handling is its own error path), ENUM/SET, and the
    /// `INSERT IGNORE` form, which Go treats like a non-strict mode.
    #[test]
    fn insert_casts_to_column_type() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (d DECIMAL(10,3), i INT, v VARCHAR(4))")
            .unwrap();

        // Captured: a decimal rounds to the column's scale, a float rounds to
        // the integer column, and a numeric string parses.
        session
            .run("INSERT INTO t VALUES (1.23456, 7.6, 'ab')")
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT d, i, v FROM t")),
            [["1.235", "8", "ab"]]
        );
        assert!(session.warnings().is_empty());
        session.run("INSERT INTO t (i) VALUES ('12')").unwrap();
        assert_eq!(row_text(session.run("SELECT i FROM t")), [["8"], ["12"]]);

        // Captured: under the default strict mode a value that does not fit
        // fails the statement, and the row is not written.
        assert!(matches!(
            session.run("INSERT INTO t (v) VALUES ('abcdefg')"),
            Err(DriverError::DataTooLong { row: 1, .. })
        ));
        assert!(matches!(
            session.run("INSERT INTO t (i) VALUES ('x')"),
            Err(DriverError::IncorrectValue { row: 1, .. })
        ));
        assert_eq!(row_text(session.run("SELECT i FROM t")).len(), 2);
        // The failure is reported with Go's own message.
        match session.run("INSERT INTO t (i) VALUES ('x')") {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1366);
                assert_eq!(
                    reported.message,
                    "Incorrect int value: 'x' for column 'i' at row 1"
                );
            }
            Ok(other) => panic!("expected a failure, got {other:?}"),
        }

        // Captured: UPDATE casts an assigned value the same way.
        session.run("UPDATE t SET d = 9.87654 WHERE i = 8").unwrap();
        assert_eq!(
            row_text(session.run("SELECT d FROM t WHERE i = 8")),
            [["9.877"]]
        );
        assert!(matches!(
            session.run("UPDATE t SET v = 'abcdefg' WHERE i = 8"),
            Err(DriverError::DataTooLong { .. })
        ));

        // Captured: without a strict mode the converted value is stored and
        // the same message is a warning -- the string truncates to the
        // column's width and an unparseable number becomes zero.
        session.apply_set("SET sql_mode = ''").unwrap();
        session.run("INSERT INTO t (v) VALUES ('abcdefg')").unwrap();
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1406);
        assert_eq!(
            session.warnings()[0].message,
            "Data too long for column 'v' at row 1"
        );
        session.run("INSERT INTO t (i) VALUES ('x')").unwrap();
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1366);
        assert_eq!(
            row_text(session.run("SELECT v FROM t")),
            [["ab"], ["NULL"], ["abcd"], ["NULL"]]
        );
    }

    /// Decimal, hex and bit literals through the whole session path, checked
    /// against captured TiDB output.
    ///
    /// NOT PORTED: `-2.750` is one literal token in Go's parser, so its type
    /// carries the sign in its flen; this AST keeps the sign as a unary minus
    /// over the literal, so the sign shapes the value but not the literal's
    /// own flen. The printed value is the same.
    #[test]
    fn numeric_literals() {
        let mut session = Session::new();

        // Captured: a decimal literal keeps its written scale.
        assert_eq!(row_text(session.run("SELECT 1.5")), [["1.5"]]);
        assert_eq!(row_text(session.run("SELECT 0.10")), [["0.10"]]);
        assert_eq!(row_text(session.run("SELECT -2.750")), [["-2.750"]]);

        // Captured: decimal arithmetic keeps the wider scale, and division by
        // zero is still NULL plus a warning.
        assert_eq!(row_text(session.run("SELECT 1.5 + 1")), [["2.5"]]);
        assert_eq!(row_text(session.run("SELECT 1.5 * 2")), [["3.0"]]);
        assert_eq!(
            session.run("SELECT 1.5 / 0").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null]])
        );
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1365);

        // Captured: a decimal comparison against an integer.
        assert_eq!(row_text(session.run("SELECT 1.5 > 1")), [["1"]]);

        // Captured: DIV and MOD truncate toward zero.
        assert_eq!(
            row_text(session.run("SELECT 7 DIV 2, 7 MOD 2, -7 DIV 2")),
            [["3", "1", "-3"]]
        );

        // Captured: a hex or bit literal prints as its bytes.
        assert_eq!(row_text(session.run("SELECT 0x41")), [["A"]]);
        assert_eq!(row_text(session.run("SELECT x'4142'")), [["AB"]]);
        assert_eq!(row_text(session.run("SELECT b'1010'")), [["\n"]]);

        // Captured: and reads as a number in arithmetic.
        assert_eq!(row_text(session.run("SELECT 0x41 + 0")), [["65"]]);
        assert_eq!(row_text(session.run("SELECT b'1010' + 0")), [["10"]]);

        // A decimal literal reaches a stored decimal column and compares
        // against it.
        session.run("CREATE TABLE t (d DECIMAL(10,3))").unwrap();
        session.run("INSERT INTO t VALUES (1.5), (2.25)").unwrap();
        assert_eq!(
            row_text(session.run("SELECT d FROM t WHERE d > 1.4")),
            [["1.500"], ["2.250"]]
        );
    }

    /// Division by zero, checked against captured TiDB output.
    ///
    /// The value is `NULL` in every case; what the SQL mode decides is whether
    /// the statement also warns, fails, or stays silent.
    ///
    /// NOT PORTED from Go's own suites: the coprocessor's own warning
    /// merging. TiDB pushes a `WHERE a/0 IS NULL` filter down and reports ONE
    /// warning for all the rows a region produced, while three zero divisors
    /// in a projection give three warnings; this tier has no coprocessor
    /// boundary, so it reports one warning per evaluation everywhere.
    #[test]
    fn division_by_zero() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();

        // Captured: a query returns NULL and warns 1365.
        assert_eq!(
            session.run("SELECT 1 / 0").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null]])
        );
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1365);
        assert_eq!(session.warnings()[0].message, "Division by 0");
        assert_eq!(
            row_text(session.run("SHOW WARNINGS")),
            [[
                "Warning".to_owned(),
                "1365".to_owned(),
                "Division by 0".to_owned()
            ]]
        );

        // Captured: every zero divisor raises its own warning.
        assert_eq!(
            session.run("SELECT 1 / 0, 2 / 0").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null, Datum::Null]])
        );
        assert_eq!(session.warnings().len(), 2);
        // DEFERRED (pre-existing rewriter gaps, not this channel's): `DIV`,
        // `MOD` and a decimal literal operand reach the same zero-divisor
        // check in `ops.rs`, but the rewriter does not build those expression
        // forms yet, so they cannot be asserted through the session here.

        // Captured: a zero dividend is ordinary arithmetic, not this case.
        session.run("SELECT 0 / 1").unwrap();
        assert!(session.warnings().is_empty());

        // Captured: under the default SQL mode an INSERT fails with 1365 and
        // writes nothing.
        assert!(matches!(
            session.run("INSERT INTO t VALUES (1 / 0, 1)"),
            Err(DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::DivisionByZero
            )))
        ));
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![])
        );

        // The same holds for UPDATE and DELETE, which Go gives the same level.
        session.run("INSERT INTO t VALUES (1, 1)").unwrap();
        assert!(session.run("UPDATE t SET a = a / 0").is_err());
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        assert!(session.run("DELETE FROM t WHERE a = 1 / 0").is_err());
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: without ERROR_FOR_DIVISION_BY_ZERO the condition is
        // ignored entirely -- NULL is written, with no warning at all.
        session.apply_set("SET sql_mode = ''").unwrap();
        session.run("INSERT INTO t VALUES (1 / 0, 2)").unwrap();
        assert!(session.warnings().is_empty());
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Null]])
        );
        // Captured: a strict mode without that flag ignores it too.
        session
            .apply_set("SET sql_mode = 'STRICT_TRANS_TABLES'")
            .unwrap();
        session.run("INSERT INTO t VALUES (1 / 0, 3)").unwrap();
        assert!(session.warnings().is_empty());

        // Non-strict with the flag warns instead of failing.
        session
            .apply_set("SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO'")
            .unwrap();
        session.run("INSERT INTO t VALUES (1 / 0, 4)").unwrap();
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1365);

        // A query keeps warning whatever the SQL mode says.
        session.apply_set("SET sql_mode = ''").unwrap();
        session.run("SELECT 1 / 0").unwrap();
        assert_eq!(session.warnings().len(), 1);
    }

    /// SHOW WARNINGS / SHOW ERRORS, checked against captured TiDB output.
    ///
    /// NOT PORTED from Go's own suites: the warnings raised by evaluation
    /// (`1/0` is 1365 there) and by write-time truncation, because this tier
    /// does not yet produce those warnings -- only the preprocessor gate and
    /// the failed-statement error reach the buffer here. The filter forms of
    /// both statements are refused, not ignored.
    #[test]
    fn show_warnings() {
        let mut session = Session::new();
        session
            .apply_set("SET tidb_enable_noop_functions = 'WARN'")
            .unwrap();
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1, 1)").unwrap();

        // Captured: the warning the statement raised, as Level/Code/Message.
        session.run("SELECT a FROM t LOCK IN SHARE MODE").unwrap();
        let expected = vec![vec![
            "Warning".to_owned(),
            "1235".to_owned(),
            "function LOCK IN SHARE MODE has only noop implementation in tidb now, use \
             tidb_enable_noop_functions to enable these functions"
                .to_owned(),
        ]];
        assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
        // Captured: SHOW WARNINGS does not consume what it reports.
        assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
        match session.run_with_columns("SHOW WARNINGS").unwrap() {
            StmtOutput::Rows { columns, .. } => assert_eq!(
                columns
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                ["Level", "Code", "Message"]
            ),
            other => panic!("expected rows, got {other:?}"),
        }
        // Captured: a warning is not an error, so SHOW ERRORS is empty.
        assert!(row_text(session.run("SHOW ERRORS")).is_empty());

        // Captured: the buffer belongs to the last statement, so an ordinary
        // statement empties it.
        session.run("SELECT a FROM t").unwrap();
        assert!(row_text(session.run("SHOW WARNINGS")).is_empty());

        // Captured: a failed statement leaves its own error in the buffer,
        // which both SHOW WARNINGS and SHOW ERRORS report.
        session
            .apply_set("SET tidb_enable_noop_functions = 'OFF'")
            .unwrap();
        assert!(session.run("SELECT a FROM t LOCK IN SHARE MODE").is_err());
        let reported = row_text(session.run("SHOW WARNINGS"));
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0][0], "Error");
        assert_eq!(reported[0][1], "1235");
        assert_eq!(row_text(session.run("SHOW ERRORS")), reported);

        // Captured: the count form reports a single count column.
        match session.run_with_columns("SHOW COUNT(*) WARNINGS").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "@@session.warning_count");
                assert_eq!(rows, vec![vec![Datum::Int(1)]]);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // A filter would silently report the wrong rows, so it is refused.
        assert!(matches!(
            session.run("SHOW WARNINGS WHERE 1"),
            Err(DriverError::Unsupported(_)) | Err(DriverError::Parse(_))
        ));
    }

    /// The clauses TiDB parses but only implements as no-ops, checked
    /// against captured TiDB output with `tidb_enable_noop_functions` at its
    /// `OFF` default.
    ///
    /// NOT PORTED from Go's own suites: `tidb_enable_shared_lock_promotion`
    /// (no locking layer here to promote to) and the `READ ONLY` /
    /// `OFFLINE MODE` / `sql_auto_is_null` gates, which belong to variable
    /// and transaction surfaces this tier does not have.
    #[test]
    fn noop_function_gate() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1, 10), (2, 20)")
            .unwrap();

        // Captured: FOR UPDATE runs and returns the rows.
        assert_eq!(
            session
                .run("SELECT b FROM t WHERE a = 1 FOR UPDATE")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(10)]])
        );
        // Its waiting options only shape a lock this tier does not take.
        session.run("SELECT b FROM t FOR UPDATE NOWAIT").unwrap();
        session.run("SELECT b FROM t FOR UPDATE OF t").unwrap();

        // Captured: the shared lock and SQL_CALC_FOUND_ROWS are 1235.
        for sql in [
            "SELECT b FROM t FOR SHARE",
            "SELECT b FROM t LOCK IN SHARE MODE",
            "SELECT SQL_CALC_FOUND_ROWS b FROM t LIMIT 1",
            "SELECT b FROM t GROUP BY b DESC",
        ] {
            assert!(
                matches!(session.run(sql), Err(DriverError::FunctionsNoopImpl(_))),
                "expected a noop-function error from {sql}"
            );
        }
        // An explicit ASC is written too, so it is gated the same way.
        assert!(matches!(
            session.run("SELECT b FROM t GROUP BY b ASC"),
            Err(DriverError::FunctionsNoopImpl("GROUP BY expr ASC|DESC"))
        ));
        // A GROUP BY with no direction is not.
        session.run("SELECT b FROM t GROUP BY b").unwrap();

        // The gate reaches a subquery, a derived table and a set operation.
        assert!(matches!(
            session.run("SELECT b FROM t WHERE a IN (SELECT a FROM t LOCK IN SHARE MODE)"),
            Err(DriverError::FunctionsNoopImpl(_))
        ));
        assert!(matches!(
            session.run("SELECT x.b FROM (SELECT b FROM t LOCK IN SHARE MODE) x"),
            Err(DriverError::FunctionsNoopImpl(_))
        ));
        assert!(matches!(
            session.run("SELECT b FROM t UNION SELECT a FROM t LOCK IN SHARE MODE"),
            Err(DriverError::FunctionsNoopImpl(_))
        ));

        // ON: the clause is accepted and does nothing, with no warning.
        session
            .apply_set("SET tidb_enable_noop_functions = 'ON'")
            .unwrap();
        session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
        assert!(session.warnings().is_empty());

        // WARN: accepted, with the same message as a warning.
        session
            .apply_set("SET tidb_enable_noop_functions = 'WARN'")
            .unwrap();
        session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1235);
        assert!(session.warnings()[0].message.contains("LOCK IN SHARE MODE"));
        // The warnings belong to the last statement only.
        session.run("SELECT b FROM t").unwrap();
        assert!(session.warnings().is_empty());

        // INTO OUTFILE writes a server-side file, which this tier cannot do,
        // so it is refused rather than answered with rows.
        session
            .apply_set("SET tidb_enable_noop_functions = 'OFF'")
            .unwrap();
        assert!(matches!(
            session.run("SELECT b FROM t INTO OUTFILE '/tmp/x'"),
            Err(DriverError::Unsupported(_))
        ));
    }

    /// ALTER TABLE MODIFY / CHANGE COLUMN, checked against captured TiDB
    /// output (`alter table t modify column ...` on a mock store).
    ///
    /// NOT PORTED from Go's own DDL suites: the concurrent/rollback schema
    /// states (this tier applies a DDL atomically), reorg-worker batching,
    /// and the type changes needing a full index rebuild across a partitioned
    /// table -- none of those surfaces exist here.
    #[test]
    fn modify_column() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT NOT NULL DEFAULT 5, KEY kb (b))")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1, 'xx', 7), (2, 'yy', 8)")
            .unwrap();

        // Captured: widening keeps the rows, and the index still reads.
        session
            .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20)")
            .unwrap();
        assert_eq!(
            session.run("SELECT a FROM t WHERE b = 'xx'").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: CHANGE renames the column, and the rows survive.
        session
            .run("ALTER TABLE t CHANGE COLUMN c d BIGINT")
            .unwrap();
        assert_eq!(
            session.run("SELECT a, d FROM t").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1), Datum::Int(7)],
                vec![Datum::Int(2), Datum::Int(8)],
            ])
        );
        assert!(session.run("SELECT c FROM t").is_err());

        // Captured: an unknown column is 1054 naming the table, unless the
        // statement says IF EXISTS.
        assert!(matches!(
            session.run("ALTER TABLE t MODIFY COLUMN nosuch BIGINT"),
            Err(DriverError::UnknownColumnInTable { .. })
        ));
        assert!(matches!(
            session.run("ALTER TABLE t CHANGE COLUMN nosuch e BIGINT"),
            Err(DriverError::UnknownColumnInTable { .. })
        ));
        session
            .run("ALTER TABLE t MODIFY COLUMN IF EXISTS nosuch BIGINT")
            .unwrap();

        // Captured: a value the new type cannot read is 1292, and the table is
        // left untouched.
        assert!(matches!(
            session.run("ALTER TABLE t MODIFY COLUMN b BIGINT"),
            Err(DriverError::TruncatedIncorrectValue { kind: "DOUBLE", .. })
        ));
        assert_eq!(
            row_text(session.run("SELECT b FROM t WHERE a = 1")),
            [["xx"]]
        );

        // Captured: a clustered handle cannot leave the integer domain (8200),
        // but may change to another integer type.
        assert!(matches!(
            session.run("ALTER TABLE t MODIFY COLUMN a VARCHAR(10)"),
            Err(DriverError::UnsupportedModifyColumn(_))
        ));
        session.run("ALTER TABLE t MODIFY COLUMN a INT").unwrap();

        // Captured: an index cannot cover a full BLOB/TEXT column (1170).
        assert!(matches!(
            session.run("ALTER TABLE t MODIFY COLUMN b TEXT"),
            Err(DriverError::BlobKeyWithoutLength(_))
        ));

        // Captured: NOT NULL and DEFAULT come from the new definition.
        session
            .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20) NOT NULL")
            .unwrap();
        assert!(session.run("INSERT INTO t (a, d) VALUES (3, 1)").is_err());
        session
            .run("ALTER TABLE t MODIFY COLUMN d BIGINT DEFAULT 3")
            .unwrap();

        // Captured: FIRST and AFTER move the column, rows and index included.
        session
            .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20) NOT NULL FIRST")
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT * FROM t")),
            [["xx", "1", "7"], ["yy", "2", "8"]]
        );
        session
            .run("ALTER TABLE t CHANGE COLUMN b bb VARCHAR(20) NOT NULL AFTER d")
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT * FROM t")),
            [["1", "7", "xx"], ["2", "8", "yy"]]
        );
        // The renamed, moved column still reads through its index.
        assert_eq!(
            session.run("SELECT a FROM t WHERE bb = 'xx'").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: renaming onto an existing column is 1060; renaming a
        // column to its own name is allowed.
        assert!(matches!(
            session.run("ALTER TABLE t CHANGE COLUMN bb a VARCHAR(20)"),
            Err(DriverError::DuplicateColumnName(_))
        ));
        session
            .run("ALTER TABLE t CHANGE COLUMN d d BIGINT")
            .unwrap();

        // Captured: a stored NULL is rejected by a new NOT NULL, with the
        // row's position; a convertible string becomes the new type.
        let mut session = Session::new();
        session
            .run("CREATE TABLE u (a BIGINT, b VARCHAR(10), c BIGINT)")
            .unwrap();
        session.run("INSERT INTO u VALUES (1, '12', NULL)").unwrap();
        assert!(matches!(
            session.run("ALTER TABLE u MODIFY COLUMN c BIGINT NOT NULL"),
            Err(DriverError::DataTruncatedAtRow { row: 1, .. })
        ));
        session.run("ALTER TABLE u MODIFY COLUMN b BIGINT").unwrap();
        assert_eq!(
            session.run("SELECT b FROM u").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(12)]])
        );

        // Captured: a value too wide for the narrowed type is 1265, and the
        // table keeps its old definition.
        session
            .run("CREATE TABLE w (a BIGINT, b VARCHAR(10))")
            .unwrap();
        session.run("INSERT INTO w VALUES (1, 'xxxxxxxx')").unwrap();
        assert!(matches!(
            session.run("ALTER TABLE w MODIFY COLUMN b VARCHAR(3)"),
            Err(DriverError::DataTruncatedValue { .. })
        ));
        assert_eq!(row_text(session.run("SELECT b FROM w")), [["xxxxxxxx"]]);
    }

    #[test]
    fn rename_table() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t1 (id BIGINT PRIMARY KEY, v BIGINT, KEY kv (v))")
            .unwrap();
        session.run("INSERT INTO t1 VALUES (1, 9)").unwrap();

        // Captured: the table is renamed and keeps its rows.
        session.run("RENAME TABLE t1 TO t2").unwrap();
        assert_eq!(
            session.run("SELECT id, v FROM t2").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1), Datum::Int(9)]])
        );
        assert!(session.run("SELECT id FROM t1").is_err());
        // Its indexes come along, so a read through one still works.
        assert_eq!(
            session.run("SELECT id FROM t2 WHERE v = 9").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: renaming onto an existing name is 1050.
        session.run("CREATE TABLE t3 (a BIGINT)").unwrap();
        assert!(matches!(
            session.run("RENAME TABLE t2 TO t3"),
            Err(DriverError::Schema(SchemaErrorKind::TableExists(_)))
        ));
        // Captured: renaming a table that does not exist is 1146.
        assert!(matches!(
            session.run("RENAME TABLE nosuch TO t9"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
        ));

        // Captured: ALTER TABLE ... RENAME TO is the same operation.
        session.run("ALTER TABLE t2 RENAME TO t4").unwrap();
        match session.run_with_columns("SHOW TABLES").unwrap() {
            StmtOutput::Rows { rows, .. } => assert_eq!(
                rows.into_iter()
                    .map(|row| datum_text(&row[0]).unwrap())
                    .collect::<Vec<_>>(),
                vec!["t3".to_owned(), "t4".to_owned()]
            ),
            other => panic!("expected rows, got {other:?}"),
        }

        // A rename may move the table to another schema.
        session.run("CREATE DATABASE other").unwrap();
        session.run("RENAME TABLE t4 TO other.moved").unwrap();
        assert_eq!(
            session.run("SELECT id FROM other.moved").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // The renamed table reports its NEW name in a duplicate-key error,
        // which is the table.index form TiDB uses.
        session
            .run("CREATE TABLE dup (a BIGINT, UNIQUE KEY ua (a))")
            .unwrap();
        session.run("INSERT INTO dup VALUES (1)").unwrap();
        session.run("RENAME TABLE dup TO dup2").unwrap();
        match session.run("INSERT INTO dup2 VALUES (1)") {
            Err(DriverError::DuplicateEntry { key, .. }) => assert_eq!(key, "dup2.ua"),
            other => panic!("expected a duplicate-entry error, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_kinds_error() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a INT)").unwrap();
        // Shapes the write paths do not model yet. (ORDER BY and LIMIT used
        // to be the examples here; both work now -- see
        // `insert_select_and_ordered_dml`.)
        assert!(session.run("DELETE QUICK FROM t").is_err());
        assert!(session.run("INSERT INTO t SET a = 1").is_err());
    }
}
