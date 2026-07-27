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
/// NOT MODELLED (documented): `Default` is always NULL because column
/// defaults are not stored yet, and `Extra` is always empty because
/// AUTO_INCREMENT, ON UPDATE CURRENT_TIMESTAMP and generated columns are not
/// supported. Both are stated here rather than filled with a guess.
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
    let is_handle = table.pk_handle_offset() == Some(offset);
    let key_flag = if is_handle
        || table.indexes().iter().any(|index| {
            index.name.eq_ignore_ascii_case("PRIMARY") && index.column_offsets == [offset]
        }) {
        "PRI"
    } else if table
        .indexes()
        .iter()
        .any(|index| index.unique && index.column_offsets == [offset])
    {
        "UNI"
    } else if table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.first() == Some(&offset))
    {
        "MUL"
    } else {
        ""
    };
    vec![
        Datum::Bytes(column.name.clone().into_bytes()),
        Datum::Bytes(column.field_type.compact_str(false).into_bytes()),
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(key_flag.as_bytes().to_vec()),
        // Column defaults are not stored yet (see the doc above).
        Datum::Null,
        Datum::Bytes(Vec::new()),
    ]
}

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;

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
        // Go raises ErrNoDB when a statement resolves an unqualified name and
        // no database is selected.
        if matches!(stmt, Stmt::Query(_) | Stmt::Dml(_) | Stmt::Ddl(_)) {
            self.require_current_database()?;
        }
        match &stmt {
            Stmt::Query(query) => {
                let tidb_ast::QueryStmt::Select(select) = &**query else {
                    return Err(DriverError::Unsupported("set operations are not supported"));
                };
                let current_db = self.current_db.clone();
                let (columns, rows) = self.with_catalog_mut(|catalog| {
                    tidb_executor::run_select_meta_stmt(select, catalog, &current_db)
                })?;
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Dml(dml) => match &**dml {
                DmlStmt::Insert(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_insert_in(
                            sql,
                            catalog,
                            &current_db,
                        )?))
                    })
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

        // Go's fetchShowDatabases sorts the names; the column is "Database".
        match session.run_with_columns("SHOW DATABASES").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "Database");
                assert_eq!(
                    rows.iter()
                        .map(|row| datum_text(&row[0]).unwrap())
                        .collect::<Vec<_>>(),
                    vec!["other".to_owned(), "test".to_owned()]
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

    #[test]
    fn unsupported_kinds_error() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a INT)").unwrap();
        assert!(session.run("DROP TABLE t").is_err());
        // Shapes the write paths do not model yet.
        assert!(session.run("DELETE FROM t ORDER BY a LIMIT 1").is_err());
        assert!(session.run("UPDATE t SET a = 1 LIMIT 1").is_err());
    }
}
