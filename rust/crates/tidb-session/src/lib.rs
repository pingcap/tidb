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
        let (columns, rows) =
            tidb_executor::run_select_meta_stmt(select, &scratch, infoschema::INFORMATION_SCHEMA)?;
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
        let rows = self.with_catalog_mut(|catalog| tidb_executor::run_select_on(&sql, catalog))?;
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
    pub fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
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
        // Go raises ErrNoDB when a statement resolves an unqualified name and
        // no database is selected.
        if matches!(stmt, Stmt::Query(_) | Stmt::Dml(_) | Stmt::Ddl(_)) {
            self.require_current_database()?;
        }
        match &stmt {
            Stmt::Query(query) => {
                let tidb_ast::QueryStmt::Select(select) = &**query else {
                    // A set operation runs through its own fold.
                    let tidb_ast::QueryStmt::SetOpr(set_opr) = &**query else {
                        unreachable!("a query is a SELECT or a set operation")
                    };
                    let current_db = self.current_db.clone();
                    let (columns, rows) = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_set_opr_stmt(set_opr, catalog, &current_db)
                    })?;
                    return Ok(StmtOutput::Rows { columns, rows });
                };
                // An information_schema table is virtual: its rows are
                // computed from the catalog rather than read from storage.
                if let Some(output) = self.run_information_schema_select(select)? {
                    return Ok(output);
                }
                let current_db = self.current_db.clone();
                let (columns, rows) = self.with_catalog_mut(|catalog| {
                    tidb_executor::run_select_meta_stmt(select, catalog, &current_db)
                })?;
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Dml(dml) => match &**dml {
                DmlStmt::Insert(_) => {
                    let current_db = self.current_db.clone();
                    let (affected, allocated) = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_insert_reporting(sql, catalog, &current_db)
                    })?;
                    self.statement_insert_id = allocated.unwrap_or(0).max(0) as u64;
                    if let Some(allocated) = allocated {
                        self.last_insert_id = allocated.max(0) as u64;
                    }
                    Ok(StmtOutput::Affected(affected))
                }
                DmlStmt::Update(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_update_in(
                            sql,
                            catalog,
                            &current_db,
                        )?))
                    })
                }
                DmlStmt::Delete(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_delete_in(
                            sql,
                            catalog,
                            &current_db,
                        )?))
                    })
                }
                _ => Err(DriverError::Unsupported(
                    "this DML statement kind is not supported yet",
                )),
            },
            Stmt::Ddl(ddl) => match &**ddl {
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

    #[test]
    fn unsupported_kinds_error() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a INT)").unwrap();
        // Shapes the write paths do not model yet.
        assert!(session.run("DELETE FROM t ORDER BY a LIMIT 1").is_err());
        assert!(session.run("UPDATE t SET a = 1 LIMIT 1").is_err());
    }
}
