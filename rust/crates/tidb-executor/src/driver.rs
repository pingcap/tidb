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

//! A minimal query driver: parse a SQL string, rewrite its expressions, wire the
//! executors, and run it -- the first end-to-end parse -> plan -> execute of a
//! SQL string.
//!
//! SCOPE: `SELECT <exprs | *> [FROM <table>] [WHERE <pred>] [ORDER BY ...]
//! [LIMIT ...]` over a single in-memory [`Catalog`] table or the implicit dual
//! row. It parses via `tidb-parser`, resolves `FROM` against the catalog,
//! rewrites fields/predicates/by-items through
//! [`tidb_expr::rewriter::rewrite_expr_resolved`] (columns bound by the
//! [`TableResolver`]), and wires `MemTableSource|TableDual ->
//! [Selection] -> [Sort] -> Projection -> [Limit]`.
//!
//! DEFERRED (documented): joins and derived tables, `db.t` qualification
//! (single-schema catalog), ordering by select alias/position, and everything
//! the rewriter does not yet handle. The real storage-backed `TableReaderExec`
//! replaces [`MemTableSourceExec`] when storage/tablecodec integration lands.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, AggKind, HashAggExec};
use crate::join::{JoinExec, JoinKind};
use crate::kv_table::{IndexRange, KvTable, TableHandle, TableScanExec};
use crate::limit::LimitExec;
use crate::mem_table::MemTableSourceExec;
use crate::projection::ProjectionExec;
use crate::selection::SelectionExec;
use crate::sort::{SortByItem, SortExec};
use crate::table_dual::TableDualExec;
use std::collections::HashMap;
use tidb_ast::{JoinNode, QueryStmt, SelectField, Stmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver, NoResolver};
use tidb_expr::schema::Schema;

/// An in-memory table: named, typed columns plus row values.
#[derive(Clone, Debug, Default)]
pub struct MemTable {
    /// The columns, in row order: `(name, type)`.
    pub columns: Vec<(String, FieldType)>,
    /// The rows (one `Datum` per column).
    pub rows: Vec<Vec<Datum>>,
}

/// Splits a table reference into its schema and table names. A bare name
/// resolves in the default schema; `db.t` names its schema explicitly.
///
/// Splits a table path for another module in this crate.
pub(crate) fn split_table_path_pub<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    split_table_path(path, current_db)
}

/// `current_db` is the session's selected schema (Go `SessionVars.CurrentDB`);
/// an empty one is Go's `ErrNoDB`.
fn split_table_path<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    match path {
        [name] => {
            if current_db.is_empty() {
                return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
            }
            Ok((current_db, name))
        }
        [database, name] => Ok((database, name)),
        _ => Err(DriverError::Unsupported("empty table name")),
    }
}

/// Go's default schema: TiDB's bootstrap runs
/// `CREATE DATABASE IF NOT EXISTS test`, so a fresh server always has it and
/// a connection with no explicit database lands there.
pub const DEFAULT_DATABASE: &str = "test";

/// One schema: Go `model.DBInfo`, reduced to the name and its tables.
///
/// NOT MODELLED (documented): the schema's charset, collation, placement
/// policy and state, which live on Go's `DBInfo` and matter to DDL rather
/// than to resolving a name.
#[derive(Clone, Debug, Default)]
struct Database {
    /// The name as written, for `SHOW DATABASES` output.
    name: String,
    tables: HashMap<String, TableEntry>,
}

/// A catalog of databases and their tables, the position Go's `infoschema`
/// occupies. Database and table names are case-insensitive, as in MySQL.
#[derive(Clone, Debug)]
pub struct Catalog {
    databases: HashMap<String, Database>,
    next_table_id: i64,
    /// Bumped by every mutation, so a transaction can detect that the shared
    /// catalog moved under it (Go detects the same at commit through TiKV's
    /// optimistic conflict check on the written keys).
    version: u64,
}

impl Default for Catalog {
    /// A catalog holding only `test`, as a freshly bootstrapped TiDB does.
    ///
    /// `INFORMATION_SCHEMA` is present because its tables are implemented
    /// (see `tidb-session`'s `infoschema`), and holds no stored tables of its
    /// own -- its rows are computed at query time.
    ///
    /// DIVERGENCE (documented): real TiDB also exposes `mysql`,
    /// `performance_schema`, `sys` and `metrics_schema`. Those are system
    /// schemas whose tables this seed does not implement, and listing them
    /// empty would claim more than is true, so they stay absent until their
    /// contents are ported.
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_owned(),
            Database {
                name: DEFAULT_DATABASE.to_owned(),
                tables: HashMap::new(),
            },
        );
        databases.insert(
            "information_schema".to_owned(),
            Database {
                name: "INFORMATION_SCHEMA".to_owned(),
                tables: HashMap::new(),
            },
        );
        Catalog {
            databases,
            next_table_id: 0,
            version: 0,
        }
    }
}

/// A view: Go `model.TableInfo` whose `View` field is set. A view stores no
/// rows -- its `SELECT` is re-run whenever the name is read.
///
/// DIVERGENCE (documented): Go resolves the view's output columns afresh on
/// every read, so an incompatible change to a base table surfaces at read
/// time. Here the columns are resolved once, at `CREATE VIEW`, and cached:
/// `SHOW CREATE VIEW` and `DESCRIBE` therefore answer from the definition as
/// it was created. A read still runs the body, so a dropped base table is
/// still Go's `ErrViewInvalid` (1356).
///
/// NOT MODELLED (documented): `WITH CHECK OPTION` (this tier refuses writes
/// through a view outright, which is where the check option would apply).
#[derive(Clone, Debug)]
pub struct ViewDef {
    /// The view name as written, for `SHOW CREATE VIEW`.
    pub name: String,
    /// The output columns, resolved when the view was created. The names are
    /// the explicit `CREATE VIEW v (...)` list when one was written.
    pub columns: Vec<(String, FieldType)>,
    /// The view's `SELECT`, canonicalized as Go stores it: every field
    /// explicitly aliased, every table reference schema-qualified.
    pub select_sql: String,
    /// The definer's user name (empty when the session has no user, which is
    /// this tier's only case).
    pub definer_user: String,
    /// The definer's host name.
    pub definer_host: String,
    /// The `ALGORITHM` as written, defaulting to `UNDEFINED`.
    pub algorithm: String,
    /// The `SQL SECURITY` mode as written, defaulting to `DEFINER`.
    pub security: String,
}

/// A catalog table's backing store.
#[derive(Clone, Debug)]
pub enum TableEntry {
    /// A plain value matrix (the original mock backing).
    Mem(MemTable),
    /// Rows stored as real TiKV-format bytes (see [`crate::kv_table`]).
    Kv(KvTable),
    /// A view: a stored `SELECT` rather than stored rows.
    View(ViewDef),
}

impl TableEntry {
    /// The table's columns as `(name, type)` in schema order.
    pub(crate) fn column_list(&self) -> Vec<(String, FieldType)> {
        match self {
            TableEntry::Mem(mem) => mem.columns.clone(),
            TableEntry::Kv(kv) => kv
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.field_type.clone()))
                .collect(),
            TableEntry::View(view) => view.columns.clone(),
        }
    }

    /// Whether this entry is a view, which decides which of MySQL's two
    /// object kinds a statement is allowed to name.
    #[must_use]
    pub fn is_view(&self) -> bool {
        matches!(self, TableEntry::View(_))
    }
}

impl Catalog {
    /// Registers a matrix-backed `table` in the default database.
    pub fn register(&mut self, name: &str, table: MemTable) {
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Mem(table));
    }

    /// Registers a TiKV-format-byte-backed `table` in the default database.
    pub fn register_kv(&mut self, name: &str, table: KvTable) {
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Kv(table));
    }

    /// Registers `table` in `database`, which must exist.
    fn register_in(&mut self, database: &str, name: &str, table: TableEntry) {
        self.version += 1;
        if let Some(database) = self.databases.get_mut(&database.to_lowercase()) {
            database.tables.insert(name.to_lowercase(), table);
        }
    }

    /// Every database name, sorted, with `information_schema` first when it
    /// exists -- Go's `fetchShowDatabases` ordering.
    #[must_use]
    pub fn database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .databases
            .values()
            .map(|database| database.name.clone())
            .collect();
        names.sort();
        if let Some(position) = names
            .iter()
            .position(|name| name.eq_ignore_ascii_case("information_schema"))
        {
            let front = names.remove(position);
            names.insert(0, front);
        }
        names
    }

    /// Every table name in `database`, sorted as Go's `fetchShowTables` sorts
    /// them. `None` when the database does not exist.
    #[must_use]
    pub fn table_names(&self, database: &str) -> Option<Vec<String>> {
        let database = self.databases.get(&database.to_lowercase())?;
        let mut names: Vec<String> = database.tables.keys().cloned().collect();
        names.sort();
        Some(names)
    }

    /// Whether `database` exists (Go `is.SchemaExists`).
    #[must_use]
    pub fn has_database(&self, database: &str) -> bool {
        self.databases.contains_key(&database.to_lowercase())
    }

    /// Creates `database`, reporting whether it was new. Go raises
    /// `ErrDBCreateExists` (1007) unless `IF NOT EXISTS` was written.
    pub fn create_database(&mut self, database: &str) -> bool {
        self.version += 1;
        let key = database.to_lowercase();
        if self.databases.contains_key(&key) {
            return false;
        }
        self.databases.insert(
            key,
            Database {
                name: database.to_owned(),
                tables: HashMap::new(),
            },
        );
        true
    }

    /// Moves a table to a new schema and name, which is what RENAME does.
    /// Returns `false` when the source does not exist.
    pub fn rename_table(
        &mut self,
        from_database: &str,
        from_name: &str,
        to_database: &str,
        to_name: &str,
    ) -> bool {
        self.version += 1;
        let Some(source) = self
            .databases
            .get_mut(&from_database.to_lowercase())
            .and_then(|database| database.tables.remove(&from_name.to_lowercase()))
        else {
            return false;
        };
        // The table carries its own name for duplicate-key messages.
        let mut source = source;
        if let TableEntry::Kv(table) = &mut source {
            table.set_name(to_name);
        }
        if let Some(database) = self.databases.get_mut(&to_database.to_lowercase()) {
            database.tables.insert(to_name.to_lowercase(), source);
        }
        true
    }

    /// Drops one table, reporting whether it existed.
    pub fn drop_table_in(&mut self, database: &str, name: &str) -> bool {
        self.version += 1;
        match self.databases.get_mut(&database.to_lowercase()) {
            Some(database) => database.tables.remove(&name.to_lowercase()).is_some(),
            None => false,
        }
    }

    /// Drops `database` and its tables, reporting whether it existed. Go
    /// raises `ErrDBDropExists` (1008) unless `IF EXISTS` was written.
    pub fn drop_database(&mut self, database: &str) -> bool {
        self.version += 1;
        self.databases.remove(&database.to_lowercase()).is_some()
    }

    /// Resolves a table in `database`.
    pub(crate) fn get_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.databases
            .get(&database.to_lowercase())?
            .tables
            .get(&name.to_lowercase())
    }

    fn get(&self, name: &str) -> Option<&TableEntry> {
        self.get_in(DEFAULT_DATABASE, name)
    }

    /// A mutable handle on a table of `database`, for the write paths.
    ///
    /// Taking it bumps [`Catalog::version`], which is what a transaction's
    /// conflict check observes. The count is deliberately over-approximate:
    /// every write path goes through here, so a statement that ends up
    /// changing nothing still bumps it. That can refuse a commit Go would
    /// allow, never the reverse.
    fn get_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        self.version += 1;
        self.databases
            .get_mut(&database.to_ascii_lowercase())?
            .tables
            .get_mut(&name.to_ascii_lowercase())
    }

    /// The catalog's mutation counter.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// A mutable table of `database`, for the schema-changing statements.
    pub fn table_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        self.get_mut_in(database, name)
    }

    /// A table of `database`, for the metadata statements.
    #[must_use]
    pub fn table_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.get_in(database, name)
    }

    /// A table of the default database, for tests that inspect the entry.
    #[must_use]
    pub fn get_table_for_test(&self, name: &str) -> Option<&TableEntry> {
        self.get(name)
    }

    /// Whether `database` holds a table called `name`.
    #[must_use]
    pub fn contains_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some()
    }

    /// Registers a matrix-backed table in `database`, creating the schema
    /// when it does not exist. Used to materialize a virtual table before
    /// running an ordinary plan over it.
    pub fn register_mem_in(&mut self, database: &str, name: &str, table: MemTable) {
        let key = database.to_lowercase();
        self.databases.entry(key).or_insert_with(|| Database {
            name: database.to_owned(),
            tables: HashMap::new(),
        });
        self.register_in(database, name, TableEntry::Mem(table));
    }

    /// Registers a TiKV-format-byte-backed table in `database`.
    pub fn register_kv_in(&mut self, database: &str, name: &str, table: KvTable) {
        self.register_in(database, name, TableEntry::Kv(table));
    }

    /// Registers a view in `database`, replacing whatever the name held --
    /// which is what `CREATE OR REPLACE VIEW` means.
    pub fn register_view_in(&mut self, database: &str, name: &str, view: ViewDef) {
        self.register_in(database, name, TableEntry::View(view));
    }

    /// Whether `name` in `database` is a view.
    #[must_use]
    pub fn is_view_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some_and(TableEntry::is_view)
    }

    /// Whether a table with `name` exists in the default database.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Allocates the next table id (a monotone counter standing in for the
    /// global autoid allocator, like KvTable's handle counter).
    pub fn allocate_table_id(&mut self) -> i64 {
        self.next_table_id += 1;
        self.next_table_id
    }
}

/// Resolves unqualified/`t.`-qualified column names against one table's schema
/// (case-insensitive, as in MySQL).
struct TableResolver<'a> {
    table_name: &'a str,
    columns: &'a [(String, FieldType)],
}

impl ColumnResolver for TableResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (qualifier, name) = match path {
            [name] => (None, name),
            [table, name] => (Some(table), name),
            // db.t.a qualification waits on a multi-schema catalog.
            _ => return None,
        };
        if let Some(q) = qualifier {
            if !q.eq_ignore_ascii_case(self.table_name) {
                return None;
            }
        }
        self.columns
            .iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|i| (i, self.columns[i].1.clone(), (i + 1) as i64))
    }
}

/// A failure while running a SQL string through the driver.
#[derive(Debug, Clone)]
pub enum DriverError {
    /// The SQL failed to parse.
    Parse(String),
    /// The statement is not a supported `FROM`-less `SELECT`.
    Unsupported(&'static str),
    /// Rewriting an expression or executing failed.
    Exec(ExecError),
    /// The shared catalog is unusable because a statement panicked while
    /// holding it, so its schema state may be half-written.
    CatalogPoisoned,
    /// A transaction could not be committed.
    Txn(TxnErrorKind),
    /// A session-variable statement failed.
    Var(VarErrorKind),
    /// A schema statement failed.
    Schema(SchemaErrorKind),
    /// Go `ErrDupFieldName` (1060).
    DuplicateColumnName(String),
    /// Go `ErrDupKeyName` (1061).
    DuplicateKeyName(String),
    /// Go `ErrCantDropFieldOrKey` (1091), with the index-specific message.
    UnknownIndex(String),
    /// Go `ErrCantDropFieldOrKey` (1091).
    UnknownColumnInAlter(String),
    /// Go `ErrCantRemoveAllFields` (1090).
    CannotDropOnlyColumn {
        /// The column the statement named.
        column: String,
        /// The table it belongs to.
        table: String,
    },
    /// TiDB `ErrUnsupportedModifyColumn`-family (8200).
    UnsupportedDropIntegerPrimaryKey,
    /// Go `ErrFunctionsNoopImpl` (1235): a clause TiDB only implements as a
    /// no-op, refused unless `tidb_enable_noop_functions` allows it.
    FunctionsNoopImpl(&'static str),
    /// TiDB `ErrUnsupportedModifyColumn` (8200), carrying Go's reason text.
    UnsupportedModifyColumn(&'static str),
    /// Go `ErrBadField` (1054): the column is not in the table.
    UnknownColumnInTable {
        /// The column the statement named.
        column: String,
        /// The table it looked in.
        table: String,
    },
    /// Go `ErrBlobKeyWithoutLength` (1170).
    BlobKeyWithoutLength(String),
    /// Go `ErrTruncatedWrongValue` (1292).
    TruncatedIncorrectValue {
        /// The numeric domain Go names.
        kind: &'static str,
        /// The value it could not read.
        value: String,
    },
    /// Go `ErrTruncatedWrongValueForField` (1265), value form.
    DataTruncatedValue {
        /// The column being modified.
        column: String,
        /// The value that does not fit.
        value: String,
    },
    /// Go `ErrWrongParamCount` (1210).
    WrongParamCount,
    /// Go `plannererrors.ErrWrongArguments` (1210), carrying the function
    /// name the arguments were wrong for (`ntile`).
    WrongArguments(&'static str),
    /// Go `plannererrors.ErrWindowInvalidWindowFuncUse` (3593): a window
    /// function written outside the select list / `ORDER BY`, carrying its
    /// lowercased name.
    WindowInvalidWindowFuncUse(String),
    /// Go `plannererrors.ErrWindowNoSuchWindow` (3579): an `OVER` clause named
    /// a window the `WINDOW` clause does not define.
    WindowNoSuchWindow(String),
    /// Go `plannererrors.ErrWindowCircularityInWindowGraph` (3580): a named
    /// window's `base` chain loops back on itself.
    WindowCircularity,
    /// Go `plannererrors.ErrWindowNoChildPartitioning` (3581): a window that
    /// extends another defined its own `PARTITION BY`.
    WindowNoChildPartitioning,
    /// Go `plannererrors.ErrWindowNoRedefineOrderBy` (3583): a window that
    /// extends another added an `ORDER BY` the base already has, carrying the
    /// base's name.
    WindowNoRedefineOrderBy(String),
    /// Go `plannererrors.ErrWindowFrameStartIllegal` / `ErrWindowFrameIllegal`
    /// (3586): a frame bound whose offset is negative, NULL or non-integral,
    /// or a `start` bound that ranks AFTER its `end` bound.
    WindowFrameIllegal,
    /// Go `plannererrors.ErrWindowRangeFrameOrderType` (3587): a `RANGE` frame
    /// with an `N PRECEDING`/`N FOLLOWING` bound needs exactly one `ORDER BY`
    /// expression of numeric or temporal type.
    WindowRangeFrameOrderType,
    /// Go `plannererrors.ErrWindowRangeFrameTemporalType` (3588): a temporal
    /// `ORDER BY` key accepts only an `INTERVAL` bound value.
    WindowRangeFrameTemporalType,
    /// Go `plannererrors.ErrWindowRangeFrameNumericType` (3589): a numeric
    /// `ORDER BY` key rejects an `INTERVAL` bound value.
    WindowRangeFrameNumericType,
    /// Go `ErrUnknownColumn` (1054) naming the clause it was written in.
    UnknownColumnInClause {
        /// The name as written.
        column: String,
        /// The clause Go names, for example `order clause`.
        clause: String,
    },
    /// Go `types.ErrInvalidDefault` (1067).
    InvalidDefault(String),
    /// Go `ErrDataTooLong` (1406).
    DataTooLong {
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `ErrWarnDataOutOfRange` (1264).
    DataOutOfRange {
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `table.ErrTruncatedWrongValueForField` (1366).
    IncorrectValue {
        /// The column type's name, as Go `types.TypeStr` prints it.
        type_name: String,
        /// The rejected value.
        value: String,
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `ErrTruncatedWrongValueForField` (1265), row form.
    DataTruncatedAtRow {
        /// The column being modified.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// TiDB 8200: the column is covered by a composite index.
    CannotDropColumnWithCompositeIndex(String),
    /// Go `ErrWrongNumberOfColumnsInSelect` (1222).
    WrongNumberOfColumnsInSelect,
    /// Go `ErrWrongAutoKey` (1075): more than one auto column.
    WrongAutoKey,
    /// Go `ErrWrongFieldSpec` (1063): AUTO_INCREMENT on a non-integer column.
    WrongColumnSpecifier(String),
    /// Go `ErrColumnCantNull` (1048).
    ColumnCannotBeNull(String),
    /// Go `ErrNoDefaultForField` (1364).
    NoDefaultForField(String),
    /// Go `ErrDupEntry` (1062).
    DuplicateEntry {
        /// The rejected key value.
        value: String,
        /// The violated key's name.
        key: String,
    },
    /// Go `ER_SUBQUERY_NO_1_ROW` (1242): a scalar subquery produced more than
    /// one row.
    SubqueryReturnsMoreThanOneRow,
    /// Go `plannererrors.ErrInvalidGroupFuncUse` (1111): `GROUPING()` written
    /// in a query that has no `WITH ROLLUP`.
    InvalidGroupFuncUse,
    /// Go `plannererrors.ErrFieldInGroupingNotGroupBy` (3602): a `GROUPING()`
    /// argument is not one of the `GROUP BY` expressions. The number Go prints
    /// is the argument's 0-based position.
    FieldInGroupingNotGroupBy(usize),
    /// Go's plain `INSERT into view` refusal, which carries no error class:
    /// `insert into view %s is not supported now`.
    InsertIntoViewUnsupported(String),
    /// Go's plain `DELETE` refusal: `delete view %s is not supported now`.
    DeleteViewUnsupported(String),
    /// Go `plannererrors.ErrNonUpdatableTable` (1288), which is what an
    /// `UPDATE` through a view reports.
    TableNotUpdatable(String),
    /// Go `ErrViewWrongList` (1353): the `CREATE VIEW v (...)` column list
    /// and the body's select list have different widths.
    ViewWrongList,
    /// Go `ErrCannotUser` (1396): `CREATE USER` named an account that
    /// already exists. Go quotes the account as `'user'@'host'`.
    CreateUserAlreadyExists {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrCannotUser` (1396): `DROP USER` named one or more accounts that
    /// do not exist. Go collects every missing account across the statement,
    /// rolls back (nothing is dropped), and reports them comma-joined,
    /// unquoted `user@host` each (`auth.UserIdentity.String`,
    /// `strings.Join(failedUsers, ",")`).
    DropUserMissing {
        /// The missing accounts, already formatted and comma-joined.
        accounts: String,
    },
    /// Go's plain `errors.Errorf("Unknown user: %s", user)` (`REVOKE` on an
    /// account that does not exist), unquoted `user@host`.
    RevokeUnknownUser {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrCantCreateUserWithGrant` (1410): `GRANT` named an account that
    /// does not exist and TiDB refuses to implicitly create one.
    GrantToUnknownUser,
    /// Go `ErrDynamicPrivilegeNotRegistered` (3929): a `GRANT`/`REVOKE`
    /// privilege name is not one of the standard static privileges and is
    /// not a registered dynamic privilege either.
    DynamicPrivilegeNotRegistered(String),
    /// Go `ErrNonexistingGrant` (1141): `SHOW GRANTS FOR` an account with no
    /// grant row at all (also raised for an account that does not exist).
    NonexistingGrant {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrWrongUsage.GenWithStackByArgs("DB GRANT", "GLOBAL PRIVILEGES")`
    /// (1221): a DB-scope `GRANT`/`REVOKE` named a global-only privilege
    /// (`PROCESS`, `SUPER`, ...).
    DbGrantGlobalOnlyPriv,
    /// Go `ErrIllegalGrantForTable` (1144): a TABLE-scope `GRANT`/`REVOKE`
    /// named a privilege outside `mysql.AllTablePrivs`.
    IllegalGrantForTable,
    /// Go's plain `errors.Errorf("There is no such grant defined for user
    /// '%s' on host '%s' on database %s", ...)`: `REVOKE ... ON db.*` for an
    /// account with no `mysql.DB` row for that database at all.
    RevokeNoDbGrant {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
        /// The database named in the `REVOKE`, as written.
        database: String,
    },
    /// Go's plain `errors.Errorf("There is no such grant defined for user
    /// '%s' on host '%s' on table %s.%s", ...)`: `REVOKE ... ON db.t` for an
    /// account with no `mysql.Tables_priv` row for that table at all.
    RevokeNoTableGrant {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
        /// The database named in the `REVOKE`, as written.
        database: String,
        /// The table named in the `REVOKE`, as written.
        table: String,
    },
}

/// Why a schema statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaErrorKind {
    /// Go `infoschema.ErrDatabaseNotExists` / `ErrBadDB` (1049).
    UnknownDatabase(String),
    /// Go `infoschema.ErrTableNotExists` (1146): a statement read a table
    /// that does not exist.
    UnknownTable(String),
    /// Go `ErrTableExists` (1050).
    TableExists(String),
    /// Go `ErrBadTable` (1051): `DROP TABLE` named a table that does not
    /// exist. MySQL uses a different code and message here than for a read.
    BadTable(String),
    /// Go `ErrDBCreateExists` (1007).
    DatabaseExists(String),
    /// Go `plannererrors.ErrNoDB` (1046).
    NoDatabaseSelected,
    /// Go `ErrWrongObject` (1347): the name exists but is the other object
    /// kind -- `DROP VIEW t` / `SHOW CREATE VIEW t` on a base table. The
    /// string is the qualified name; the expected kind is always `VIEW`,
    /// since the reverse direction (a table statement naming a view) reports
    /// the name as simply unknown, as Go does.
    NotView(String),
    /// Go `plannererrors.ErrViewInvalid` (1356): the view's own query no
    /// longer runs, typically because a base table was dropped.
    ViewInvalid(String),
}

/// Why a session-variable statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarErrorKind {
    /// Go `ErrUnknownSystemVar` (1193).
    UnknownSystemVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238): the variable is read-only.
    ReadOnlyVariable(String),
    /// Go `ErrWrongTypeForVar` (1232).
    WrongTypeForVar(String),
    /// Go `ErrWrongValueForVar` (1231).
    WrongValueForVar(String, String),
}

/// Why a transaction statement failed (Go `kv.ErrWriteConflict` and friends).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnErrorKind {
    /// The catalog moved under the transaction, so committing would discard
    /// another session's writes.
    WriteConflict,
}

impl From<ExecError> for DriverError {
    fn from(err: ExecError) -> Self {
        match err {
            // The same statement-level error whichever layer raised it, so
            // callers match one variant.
            ExecError::SubqueryReturnsMoreThanOneRow => DriverError::SubqueryReturnsMoreThanOneRow,
            other => DriverError::Exec(other),
        }
    }
}

const INIT_CAP: usize = 1;
const MAX_CHUNK_SIZE: usize = 1024;

/// Parses and runs a `FROM`-less `SELECT`, returning its rows as `Datum`s.
pub fn run_select(sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_on(sql, &Catalog::default(), &crate::StmtContext::for_query())
}

/// Parses and runs a single-table (or `FROM`-less) `SELECT` against `catalog`,
/// returning its rows as `Datum`s.
pub fn run_select_on(
    sql: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_meta_on(sql, catalog, ctx).map(|(_, rows)| rows)
}

/// A `SELECT` result with metadata: the output columns as `(name, type)`, then
/// the rows.
pub type SelectMeta = (Vec<(String, FieldType)>, Vec<Vec<Datum>>);

/// Like [`run_select_on`], but also returns the result-column metadata the
/// wire protocol needs: one `(name, type)` per output column.
///
/// Naming follows Go's result-field resolution in spirit, simplified for the
/// seed driver: an `AS` alias wins; a plain column reference uses the column's
/// own name; any other expression uses its restored text (Go's
/// `RestoreString`); `*` expands to the table's column names.
pub fn run_select_meta_on(
    sql: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_meta_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_select_meta_on`] resolving unqualified names in `current_db`.
pub fn run_select_meta_in(
    sql: &str,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let select = match &stmt {
        Stmt::Query(query) => match &**query {
            QueryStmt::Select(select) => select,
            QueryStmt::SetOpr(set_opr) => {
                return run_set_opr_stmt(set_opr, catalog, current_db, ctx)
            }
        },
        _ => return Err(DriverError::Unsupported("only SELECT is supported")),
    };
    run_select_stmt(select, catalog, current_db, ctx)
}

/// Runs a set-operation statement: `UNION`, `EXCEPT` or `INTERSECT`.
///
/// Go plans the terms left to right and folds each into the accumulated
/// result (`buildSetOpr`), which is what this does over materialized rows.
/// The distinct forms deduplicate, the `ALL` forms keep multiplicity, and a
/// statement-level `ORDER BY`/`LIMIT` applies to the whole result rather than
/// to the last term.
///
/// Row order is unspecified for the deduplicating forms -- TiDB returns them
/// in hash order -- so only `UNION ALL` and an explicit `ORDER BY` have an
/// order worth relying on.
///
/// DEFERRED (documented): pushing the work into executors instead of
/// materializing each term, and the type unification Go performs across terms
/// (the column metadata here comes from the first term).
pub fn run_set_opr_stmt(
    stmt: &tidb_ast::SetOprStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    // A CTE prefix belongs to the whole statement, so it is materialized once
    // and every term sees it.
    let with_catalog;
    let catalog = match &stmt.with {
        Some(with) => {
            with_catalog = materialize_ctes(with, catalog, current_db, ctx)?;
            &with_catalog
        }
        None => catalog,
    };

    let mut columns: Option<Vec<(String, FieldType)>> = None;
    let mut accumulated: Vec<Vec<Datum>> = Vec::new();
    for (index, term) in stmt.terms.iter().enumerate() {
        let (term_columns, term_rows) = run_set_opr_term(term, catalog, current_db, ctx)?;
        match &mut columns {
            None => {
                columns = Some(term_columns);
                accumulated = term_rows;
            }
            Some(existing) => {
                // Go raises ErrWrongNumberOfColumnsInSelect for a term whose
                // width differs.
                if term_columns.len() != existing.len() {
                    return Err(DriverError::WrongNumberOfColumnsInSelect);
                }
                let Some(op) = term.op else {
                    return Err(DriverError::Unsupported(
                        "a set-operation term after the first needs an operator",
                    ));
                };
                accumulated = combine_set_opr(op, accumulated, term_rows)?;
            }
        }
        debug_assert!(index == 0 || columns.is_some());
    }
    let columns = columns.ok_or(DriverError::Unsupported("an empty set operation"))?;

    // The statement-level ORDER BY and LIMIT apply to the folded result.
    if !stmt.order_by.is_empty() {
        sort_rows_by_output(&mut accumulated, &columns, &stmt.order_by)?;
    }
    if let Some(limit) = &stmt.limit {
        let count = eval_limit_bound(&limit.count)? as usize;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)? as usize,
            None => 0,
        };
        accumulated = accumulated.into_iter().skip(offset).take(count).collect();
    }
    Ok((columns, accumulated))
}

/// One term of a set operation, which is a `SELECT` or a nested set operation.
fn run_set_opr_term(
    term: &tidb_ast::SetOprTerm,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    match &term.body {
        tidb_ast::SetOprTermBody::Select(select) => {
            run_select_stmt(select, catalog, current_db, ctx)
        }
        tidb_ast::SetOprTermBody::Nested(nested) => {
            run_set_opr_stmt(nested, catalog, current_db, ctx)
        }
    }
}

/// Folds one term into the accumulated rows.
fn combine_set_opr(
    op: tidb_ast::SetOp,
    left: Vec<Vec<Datum>>,
    right: Vec<Vec<Datum>>,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    use tidb_ast::SetOp;
    Ok(match op {
        SetOp::Union { all: true } => {
            let mut rows = left;
            rows.extend(right);
            rows
        }
        SetOp::Union { all: false } => {
            let mut rows = left;
            rows.extend(right);
            dedup_rows(rows)?
        }
        SetOp::Except { all } => {
            let mut remaining = row_counts(&right)?;
            let mut rows = Vec::new();
            for row in left {
                let key = row_key(&row)?;
                match remaining.get_mut(&key) {
                    // EXCEPT ALL removes one occurrence per matching right row.
                    Some(count) if *count > 0 && all => *count -= 1,
                    Some(count) if *count > 0 => {}
                    _ => rows.push(row),
                }
            }
            if all {
                rows
            } else {
                dedup_rows(rows)?
            }
        }
        SetOp::Intersect { all } => {
            let mut available = row_counts(&right)?;
            let mut rows = Vec::new();
            for row in left {
                let key = row_key(&row)?;
                if let Some(count) = available.get_mut(&key) {
                    if *count > 0 {
                        if all {
                            *count -= 1;
                        }
                        rows.push(row);
                    }
                }
            }
            if all {
                rows
            } else {
                dedup_rows(rows)?
            }
        }
    })
}

/// The key a row is compared by, which is the codec encoding its datums use
/// for grouping elsewhere.
fn row_key(row: &[Datum]) -> Result<Vec<u8>, DriverError> {
    let mut key = Vec::new();
    for value in row {
        key.extend_from_slice(
            &value
                .to_hash_key()
                .map_err(|_| DriverError::Unsupported("this datum kind cannot be deduplicated"))?,
        );
        key.push(0xff);
    }
    Ok(key)
}

/// How many times each row appears.
fn row_counts(rows: &[Vec<Datum>]) -> Result<HashMap<Vec<u8>, usize>, DriverError> {
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    for row in rows {
        *counts.entry(row_key(row)?).or_insert(0) += 1;
    }
    Ok(counts)
}

/// Keeps the first occurrence of each distinct row.
fn dedup_rows(rows: Vec<Vec<Datum>>) -> Result<Vec<Vec<Datum>>, DriverError> {
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if seen.insert(row_key(&row)?) {
            out.push(row);
        }
    }
    Ok(out)
}

/// Sorts the folded rows by a statement-level `ORDER BY`, whose items name
/// output columns rather than any term's source columns.
fn sort_rows_by_output(
    rows: &mut [Vec<Datum>],
    columns: &[(String, FieldType)],
    order_by: &[tidb_ast::OrderItem],
) -> Result<(), DriverError> {
    let mut keys = Vec::with_capacity(order_by.len());
    for item in order_by {
        let index = match &item.expr {
            tidb_ast::Expr::Column(path) => {
                let name = path
                    .last()
                    .ok_or(DriverError::Unsupported("empty ORDER BY column"))?;
                columns
                    .iter()
                    .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                    .ok_or(DriverError::Unsupported(
                        "a set operation's ORDER BY must name an output column",
                    ))?
            }
            // MySQL also allows ordering by output position.
            tidb_ast::Expr::Int(text) => {
                let position: usize = text
                    .parse()
                    .map_err(|_| DriverError::Unsupported("bad ORDER BY position"))?;
                if position == 0 || position > columns.len() {
                    return Err(DriverError::Unsupported("ORDER BY position out of range"));
                }
                position - 1
            }
            _ => {
                return Err(DriverError::Unsupported(
                    "a set operation's ORDER BY must name an output column",
                ))
            }
        };
        keys.push((index, item.desc));
    }
    let mut failure = None;
    rows.sort_by(|left, right| {
        for (index, desc) in &keys {
            let ordering = match tidb_expr::compare_datums(&left[*index], &right[*index]) {
                Ok(ordering) => ordering,
                Err(error) => {
                    failure = Some(error);
                    std::cmp::Ordering::Equal
                }
            };
            if ordering != std::cmp::Ordering::Equal {
                return if *desc { ordering.reverse() } else { ordering };
            }
        }
        std::cmp::Ordering::Equal
    });
    match failure {
        Some(error) => Err(DriverError::Exec(ExecError::Eval(error))),
        None => Ok(()),
    }
}

/// Materializes a `WITH` clause's CTEs into `catalog`, so the query that
/// follows resolves them like ordinary tables.
///
/// Go plans a non-recursive CTE as its own subtree the outer query reads from
/// (`buildWith`), and a later CTE may reference an earlier one; materializing
/// them in written order gives that.
///
/// DEFERRED (documented): `WITH RECURSIVE`, which needs the iterate-to-fixpoint
/// executor, and is rejected rather than run as if it were non-recursive --
/// that would silently return only the seed rows.
fn materialize_ctes(
    with: &tidb_ast::WithClause,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Catalog, DriverError> {
    if with.recursive {
        return Err(DriverError::Unsupported(
            "WITH RECURSIVE is not supported yet",
        ));
    }
    // The scratch catalog carries the real tables too, since the CTE bodies
    // and the outer query both read them.
    let mut scratch = catalog.clone();
    for cte in &with.ctes {
        let tidb_ast::QueryStmt::Select(select) = &*cte.query else {
            return Err(DriverError::Unsupported(
                "a set-operation CTE is not supported yet",
            ));
        };
        // Each CTE sees the ones already materialized, which is what lets a
        // later one reference an earlier one.
        let (mut columns, rows) = run_select_stmt(select, &scratch, current_db, ctx)?;
        if !cte.columns.is_empty() {
            if cte.columns.len() != columns.len() {
                return Err(DriverError::Unsupported(
                    "the CTE column list does not match its query's columns",
                ));
            }
            for (column, name) in columns.iter_mut().zip(&cte.columns) {
                column.0 = name.clone();
            }
        }
        scratch.register_mem_in(current_db, &cte.name, MemTable { columns, rows });
    }
    Ok(scratch)
}

/// Runs one parsed `SELECT` against the catalog, for a caller that has
/// already rewritten the statement (session-variable binding, for instance)
/// and must not go back through SQL text.
pub fn run_select_meta_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_stmt(select, catalog, current_db, ctx)
}

/// Runs one parsed `SELECT` against the catalog.
fn run_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    // A WITH clause's CTEs are materialized first, then the query runs against
    // a catalog that contains them.
    let with_catalog;
    let catalog = match &select.with {
        Some(with) => {
            with_catalog = materialize_ctes(with, catalog, current_db, ctx)?;
            &with_catalog
        }
        None => catalog,
    };
    // Uncorrelated subqueries are evaluated now and folded into literals, so
    // everything below plans against ordinary expressions (Go's
    // handleScalarSubquery for the non-Apply case).
    let folded;
    let select = if select_has_uncorrelated_subquery(select, catalog, current_db, ctx) {
        let outer = select_outer_scope(select, catalog, current_db, ctx);
        folded = fold_select_subqueries(select, &outer, catalog, current_db, ctx)?;
        &folded
    } else {
        select
    };

    // Resolve FROM: none -> table-dual; otherwise the (possibly joined) tables.
    let (mut from_source, scope): (Option<Box<dyn Executor>>, FromScope) = match &select.from {
        None => (None, FromScope::default()),
        Some(join) => {
            let (exec, scope) = build_join(join, catalog, current_db, ctx)?;
            (Some(exec), scope)
        }
    };

    // Go's TryFastPlan runs before the ordinary plan: a single-table SELECT
    // whose WHERE pins the handle or a whole unique index reads that one row
    // instead of scanning. The WHERE stays in the pipeline below, so an
    // unsatisfied extra condition still filters the row out -- the point get
    // narrows the source, it does not replace the filter.
    if let Some(table) = single_kv_table(&select.from, catalog, current_db) {
        let columns = scope.column_list();
        // Go tries the batch point get before the single one.
        if let Some(handles) = try_batch_point_get(select, &table, &columns)? {
            let mut table = table.clone();
            let mut rows = Vec::with_capacity(handles.len());
            for handle in &handles {
                if let Some(row) = table
                    .get_row_by_handle(handle)
                    .map_err(|e| DriverError::Parse(format!("batch point get failed: {e:?}")))?
                {
                    rows.push(row);
                }
            }
            let schema_columns: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            from_source = Some(Box::new(MemTableSourceExec::new(
                ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
                rows,
            )));
        } else
        // An index range scan, when no point get applies: the ranges replace
        // the full scan with the rows the index covers, and the WHERE stays
        // above to apply the conditions the ranges did not consume.
        if try_point_get(select, &table, &columns)?.is_none() {
            if let Some((index_id, ranges)) = try_index_ranges(select, &table, &columns) {
                let mut table = table.clone();
                let mut rows = Vec::new();
                for range in &ranges {
                    for handle in table
                        .scan_index_range(index_id, range)
                        .map_err(|e| DriverError::Parse(format!("index scan failed: {e:?}")))?
                    {
                        if let Some(row) = table
                            .get_row_by_handle(&handle)
                            .map_err(|e| DriverError::Parse(format!("row read failed: {e:?}")))?
                        {
                            rows.push(row);
                        }
                    }
                }
                let schema_columns: Vec<Column> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, (_, ft))| {
                        let mut col = Column::new((i + 1) as i64, ft.clone());
                        col.index = i as i64;
                        col
                    })
                    .collect();
                from_source = Some(Box::new(MemTableSourceExec::new(
                    ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
                    rows,
                )));
            }
        }
        if let Some(handle) = try_point_get(select, &table, &columns)? {
            let mut table = table;
            let rows = match handle {
                Some(handle) => table
                    .get_row_by_handle(&handle)
                    .map_err(|e| DriverError::Parse(format!("point get failed: {e:?}")))?
                    .map(|row| vec![row])
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            let schema_columns: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            from_source = Some(Box::new(MemTableSourceExec::new(
                ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
                rows,
            )));
        }
    }

    // The column resolver for this query's scope.
    let resolver = ScopeResolver { scope: &scope };

    // GROUPING() reads which grouping set produced a row, so it means nothing
    // without WITH ROLLUP: Go rejects it with ErrInvalidGroupFuncUse (1111),
    // whether or not the query groups at all.
    if !select.rollup && select_has_grouping(select) {
        return Err(DriverError::InvalidGroupFuncUse);
    }

    // A window function outside the select list / ORDER BY is Go's
    // ErrWindowInvalidWindowFuncUse (3593), whichever path runs below.
    crate::window::reject_windows_outside_select_list(select)?;

    // Aggregate path: GROUP BY, or any select field that is an aggregate call.
    let is_aggregate = !select.group_by.is_empty()
        || select.fields.fields().iter().any(|f| {
            matches!(
                f,
                SelectField::Expr {
                    expr: tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. },
                    ..
                }
            )
        });
    if is_aggregate {
        return run_aggregate_select(select, from_source, &resolver, catalog, current_db, ctx);
    }

    // Source: the table rows (matrix- or TiKV-byte-backed), or one virtual row
    // from a table-dual.
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match from_source {
        Some(exec) => {
            let schema = exec.schema().clone();
            (exec, schema)
        }
        None => (
            Box::new(TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
                1,
            )),
            Schema::new(vec![]),
        ),
    };

    // Optional WHERE: a selection over the source rows. A correlated
    // subquery in the predicate first becomes an Apply below the selection,
    // appending the column the rewritten predicate reads (Go's plan shape).
    // The scope the rows above the WHERE have: the FROM tables, plus the
    // column a correlated WHERE subquery's Apply appends.
    let mut current_scope = scope.clone();
    if let Some(predicate) = &select.where_clause {
        let mut correlated = None;
        let appended = scope.width();
        let predicate = extract_correlated_subquery(
            predicate,
            &scope,
            catalog,
            current_db,
            appended,
            &mut correlated,
            ctx,
        )?;
        let (predicate_resolver, predicate_scope);
        let mut source_schema = source_schema;
        if let Some(correlated) = correlated {
            // The Apply's schema is the source's columns plus the subquery's.
            let mut applied = scope.clone();
            let mut value_type = FieldType::new(FieldTypeCode::LongLong);
            if matches!(correlated.kind, SubqueryKind::Scalar) {
                value_type = subquery_result_type(&correlated, catalog, current_db, ctx)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            }
            applied.tables.push(FromTable {
                name: String::new(),
                database: None,
                columns: vec![(format!("__apply_{appended}"), value_type)],
                offset: appended,
            });
            let columns: Vec<Column> = applied
                .column_list()
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let apply_schema = Schema::new(columns);
            let inner_scope = scope.clone();
            // The apply callback outlives this borrow of the catalog, so it
            // owns a snapshot (see ApplyExec::new).
            let inner_catalog = catalog.clone();
            let inner_db = current_db.to_owned();
            // The statement context is a handle, so the callback shares the
            // one warning buffer the statement reports.
            let inner_ctx = ctx.clone();
            let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
                run_correlated_subquery(
                    &correlated,
                    values,
                    &inner_scope,
                    &inner_catalog,
                    &inner_db,
                    &inner_ctx,
                )
                .map_err(|e| match e {
                    DriverError::Exec(exec) => exec,
                    DriverError::SubqueryReturnsMoreThanOneRow => {
                        ExecError::SubqueryReturnsMoreThanOneRow
                    }
                    other => ExecError::Unsupported(driver_error_text(&other)),
                })
            });
            source = Box::new(crate::apply::ApplyExec::new(
                ExecutorMeta::new(apply_schema.clone(), 7, INIT_CAP, MAX_CHUNK_SIZE),
                source,
                runner,
            ));
            source_schema = apply_schema;
            current_scope = applied;
            predicate_scope = current_scope.clone();
        } else {
            predicate_scope = scope.clone();
        }
        predicate_resolver = ScopeResolver {
            scope: &predicate_scope,
        };
        let pred = rewrite_expr_resolved(&predicate, &predicate_resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        source = Box::new(SelectionExec::new(
            ExecutorMeta::new(source_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![pred],
            source,
            ctx.clone(),
        ));
    }

    // Window functions: the source rows are materialized here, each window
    // call is computed over them (see `crate::window`), and its values are
    // appended as one synthetic source column per call. Every `Expr::Window`
    // in the select list / ORDER BY is then rewritten to read that column, so
    // everything below -- projection, outer ORDER BY, DISTINCT, LIMIT -- runs
    // unchanged, and the outer ORDER BY sorts the already-computed values.
    let window_rewritten;
    let select = if crate::window::select_has_window(select) {
        let calls = crate::window::collect_window_calls(select)?;
        let source_types: Vec<FieldType> = current_scope
            .column_list()
            .into_iter()
            .map(|(_, field_type)| field_type)
            .collect();
        let rows = drain_executor_rows(source, &source_types)?;
        let (rows, scope_with_windows) =
            crate::window::compute_windows(&calls, rows, &current_scope, ctx)?;
        let columns: Vec<Column> = scope_with_windows
            .column_list()
            .iter()
            .enumerate()
            .map(|(i, (_, ft))| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        source = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(Schema::new(columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
            rows,
        ));
        current_scope = scope_with_windows;
        window_rewritten = crate::window::rewrite_windows(select, &calls);
        &window_rewritten
    } else {
        select
    };

    // A correlated subquery in the SELECT list becomes an Apply above the
    // WHERE's selection, appending the column the rewritten field reads --
    // the same shape the WHERE path builds, and Go's plan for
    // `handleScalarSubquery` when the subquery cannot be folded. It sits
    // ABOVE the filter, so the inner query runs only for the rows the WHERE
    // kept, as Go's plan does.
    let mut projected: Vec<(SelectField, Option<String>)> = Vec::new();
    for field in select.fields.fields() {
        let SelectField::Expr { expr, alias } = field else {
            projected.push((field.clone(), None));
            continue;
        };
        let name = match (alias, expr) {
            (Some(alias), _) => alias.clone(),
            (None, tidb_ast::Expr::Column(path)) => {
                path.last().cloned().unwrap_or_else(|| expr.restore())
            }
            (None, _) => expr.restore(),
        };
        let mut correlated = None;
        let appended = current_scope.width();
        let rewritten = extract_correlated_subquery(
            expr,
            &current_scope,
            catalog,
            current_db,
            appended,
            &mut correlated,
            ctx,
        )?;
        if let Some(correlated) = correlated {
            let mut value_type = FieldType::new(FieldTypeCode::LongLong);
            if matches!(correlated.kind, SubqueryKind::Scalar) {
                value_type = subquery_result_type(&correlated, catalog, current_db, ctx)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            }
            let inner_scope = current_scope.clone();
            current_scope.tables.push(FromTable {
                name: String::new(),
                database: None,
                columns: vec![(format!("__apply_{appended}"), value_type)],
                offset: appended,
            });
            let columns: Vec<Column> = current_scope
                .column_list()
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let apply_schema = Schema::new(columns);
            // The callback outlives this borrow of the catalog, so it owns a
            // snapshot (see ApplyExec::new); the context is a handle, so the
            // inner query's warnings reach the statement's one buffer.
            let inner_catalog = catalog.clone();
            let inner_db = current_db.to_owned();
            let inner_ctx = ctx.clone();
            let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
                run_correlated_subquery(
                    &correlated,
                    values,
                    &inner_scope,
                    &inner_catalog,
                    &inner_db,
                    &inner_ctx,
                )
                .map_err(|e| match e {
                    DriverError::Exec(exec) => exec,
                    DriverError::SubqueryReturnsMoreThanOneRow => {
                        ExecError::SubqueryReturnsMoreThanOneRow
                    }
                    other => ExecError::Unsupported(driver_error_text(&other)),
                })
            });
            source = Box::new(crate::apply::ApplyExec::new(
                ExecutorMeta::new(apply_schema, 7, INIT_CAP, MAX_CHUNK_SIZE),
                source,
                runner,
            ));
        }
        projected.push((
            SelectField::Expr {
                expr: rewritten,
                alias: alias.clone(),
            },
            Some(name),
        ));
    }
    let projected_fields: Vec<SelectField> =
        projected.iter().map(|(field, _)| field.clone()).collect();
    let resolver = ScopeResolver {
        scope: &current_scope,
    };

    // Rewrite each projected field into an evaluable expression; `*` expands to
    // every table column in order (Go's unfoldWildStar).
    let mut exprs: Vec<Expression> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for (field, name) in &projected {
        match field {
            SelectField::Expr { expr, .. } => {
                let rewritten = rewrite_expr_resolved(expr, &resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                exprs.push(rewritten);
                names.push(name.clone().unwrap_or_default());
            }
            SelectField::Wildcard(qualifier) => {
                if scope.tables.is_empty() {
                    return Err(DriverError::Unsupported(
                        "`*` is not supported in a FROM-less SELECT",
                    ));
                }
                // `*` expands to every column of every FROM table in order,
                // `t.*` to one table's (Go's unfoldWildStar).
                let selected: Vec<&FromTable> = match qualifier.last() {
                    None => scope.tables.iter().collect(),
                    Some(q) => {
                        let matching: Vec<&FromTable> = scope
                            .tables
                            .iter()
                            .filter(|t| t.name.eq_ignore_ascii_case(q))
                            .collect();
                        if matching.is_empty() {
                            return Err(DriverError::Unsupported(
                                "`t.*` qualifier does not match a FROM table",
                            ));
                        }
                        matching
                    }
                };
                for table in selected {
                    for (i, (name, ft)) in table.columns.iter().enumerate() {
                        let index = table.offset + i;
                        let mut col = Column::new((index + 1) as i64, ft.clone());
                        col.index = index as i64;
                        exprs.push(Expression::Column(col));
                        names.push(name.clone());
                    }
                }
            }
        }
    }

    // Output schema: one column per field, typed by the expression's static type.
    let out_columns: Vec<Column> = exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            let field_type = expr
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            let mut col = Column::new((i + 1) as i64, field_type);
            col.index = i as i64;
            col
        })
        .collect();
    let out_schema = Schema::new(out_columns);
    let ret_types: Vec<FieldType> = out_schema
        .columns
        .iter()
        .map(|c| c.ret_type.clone().expect("output column has a type"))
        .collect();

    // ORDER BY: a sort below the projection, with by-items resolved against
    // the SELECT list first and the SOURCE schema second -- Go's own
    // resolution order, which is why ordering by a column that is not
    // projected still works while an alias shadows one that is.
    if !select.order_by.is_empty() {
        let mut by_items = Vec::with_capacity(select.order_by.len());
        for item in &select.order_by {
            let resolved = substitute_output_aliases(&item.expr, &projected_fields, true)?;
            let expr = rewrite_expr_resolved(&resolved, &resolver).map_err(|e| {
                order_by_column_error(&resolved).unwrap_or(DriverError::Exec(ExecError::Eval(e)))
            })?;
            by_items.push(SortByItem {
                expr,
                desc: item.desc,
            });
        }
        let sort_schema = source.schema().clone();
        source = Box::new(SortExec::new(
            ExecutorMeta::new(sort_schema, 3, INIT_CAP, MAX_CHUNK_SIZE),
            by_items,
            source,
            ctx.clone(),
        ));
    }

    // Projection of the rewritten fields.
    let mut root: Box<dyn Executor> = Box::new(ProjectionExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        exprs,
        source,
        ctx.clone(),
    ));

    // SELECT DISTINCT: Go `buildDistinct` builds an aggregation grouping by
    // every projected column, with a FIRST_ROW aggregate per column, which is
    // exactly a deduplication. It sits above the projection and below LIMIT.
    if select.distinct {
        root = Box::new(distinct_over(root, &out_schema, ctx));
    }

    // LIMIT [offset,] count: both bounds must be non-negative integer literals
    // (as in SQL; Go validates the same in the planner).
    if let Some(limit) = &select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        let limit_schema = root.schema().clone();
        root = Box::new(LimitExec::new(
            ExecutorMeta::new(limit_schema, 4, INIT_CAP, MAX_CHUNK_SIZE),
            offset,
            count,
            root,
        ));
    }

    root.open()?;
    let mut req = root.new_chunk();
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    loop {
        root.next(&mut req)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            let row = req.get_row(r);
            let values = ret_types
                .iter()
                .enumerate()
                .map(|(c, ft)| row.get_datum(c, ft))
                .collect();
            rows.push(values);
        }
    }
    root.close()?;
    let columns = names.into_iter().zip(ret_types).collect();
    Ok((columns, rows))
}

/// Parses and runs a plain `INSERT INTO t [(cols)] VALUES (...), ...` against
/// `catalog`, returning the number of inserted rows.
///
/// The write half of the in-memory gateway (the storage-backed `InsertExec`
/// with autoid/defaults/constraints lands with real tables). Unsupported here
/// (rejected, documented): `REPLACE`, `IGNORE`, `ON DUPLICATE KEY UPDATE`,
/// `SET` syntax, `INSERT ... SELECT`, and partitions. A `RETURNING` clause is
/// parsed and silently ignored: Go's hand-written parser stores it on the AST
/// but the planner and executor never read it, so the write runs normally and
/// answers with a plain OK packet (verified against Go with a testkit probe).
/// Columns not
/// listed in an explicit column list are filled with NULL (column defaults
/// wait on ColumnInfo default-value wiring).
pub fn run_insert_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_insert_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_insert_on`] resolving unqualified names in `current_db`.
pub fn run_insert_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_insert_reporting(sql, catalog, current_db, ctx).map(|outcome| outcome.0)
}

/// [`run_insert_in`], also reporting the first auto-increment id the statement
/// allocated, which is what MySQL answers with as `LAST_INSERT_ID`.
///
/// `None` when the statement allocated nothing: an explicit auto value or a
/// table with no auto column leaves the session's value untouched, which is
/// the behavior captured from TiDB.
pub fn run_insert_reporting(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(u64, Option<i64>), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let insert = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Insert(insert) => insert,
            _ => return Err(DriverError::Unsupported("only INSERT is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only INSERT is supported here")),
    };

    if !insert.partitions.is_empty() || (insert.replace && !insert.on_duplicate.is_empty()) {
        return Err(DriverError::Unsupported("partitions are not supported yet"));
    }

    // `INSERT ... SELECT` runs its source query first, over the catalog as it
    // stands: Go materializes the SelectExec's rows and feeds them to the
    // insert, so a source that reads the target table sees the pre-insert
    // rows. The query runs before the table is borrowed mutably, which is the
    // same ordering.
    let source_rows: Option<Vec<Vec<Datum>>> = match &insert.source {
        Some(query) => Some(match &**query {
            tidb_ast::QueryStmt::Select(select) => {
                run_select_stmt(select, catalog, current_db, ctx)?.1
            }
            tidb_ast::QueryStmt::SetOpr(set_opr) => {
                run_set_opr_stmt(set_opr, catalog, current_db, ctx)?.1
            }
        }),
        None => None,
    };

    let (database, table_name) = split_table_path(&insert.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    let table = catalog
        .get_mut_in(&database, &table_name)
        .ok_or(DriverError::Unsupported("table not found in catalog"))?;
    // Go refuses a write through a view before planning anything.
    if table.is_view() {
        return Err(DriverError::InsertIntoViewUnsupported(table_name.clone()));
    }
    let column_list = table.column_list();

    // Map an explicit column list to table offsets; without one, values map to
    // every column in order.
    //
    // `INSERT ... SET a = 1, b = 2` is the same statement as
    // `INSERT (a, b) VALUES (1, 2)` -- Go normalizes its `Setlist` into
    // `Columns` + one `Lists` entry, and the parser here does the same, so
    // the assignment columns are simply another way to name the targets.
    let named_columns: Vec<String> = if insert.set_syntax {
        insert
            .set_columns
            .iter()
            .map(|path| path.last().cloned().unwrap_or_default())
            .collect()
    } else {
        insert.columns.clone()
    };
    let target_offsets: Vec<usize> = if insert.set_syntax || insert.columns_specified {
        named_columns
            .iter()
            .map(|name| {
                column_list
                    .iter()
                    .position(|(n, _)| n.eq_ignore_ascii_case(name))
                    .ok_or_else(|| DriverError::UnknownColumnInClause {
                        column: name.clone(),
                        clause: "field list".to_owned(),
                    })
            })
            .collect::<Result<_, _>>()?
    } else {
        (0..column_list.len()).collect()
    };

    // Evaluate each VALUES row (constant expressions over the dual row).
    let eval_chunk = {
        let mut c = tidb_chunk::chunk::Chunk::new_empty(&[]);
        c.set_num_virtual_rows(1);
        c
    };
    // The per-column metadata the default and NOT NULL rules read.
    let column_meta: Vec<(Option<Datum>, bool, String)> = match table {
        TableEntry::Kv(kv) => kv
            .columns
            .iter()
            .map(|c| {
                (
                    c.default_value.clone(),
                    c.field_type.flags() & 1 != 0,
                    c.name.clone(),
                )
            })
            .collect(),
        // A matrix-backed table carries no column metadata, so every column
        // is nullable with no default -- the original mock behavior.
        TableEntry::Mem(mem) => mem
            .columns
            .iter()
            .map(|(name, _)| (None, false, name.clone()))
            .collect(),
        TableEntry::View(_) => unreachable!("INSERT through a view is refused above"),
    };

    let auto_increment_offset = match table {
        TableEntry::Kv(kv) => kv.auto_increment_offset(),
        TableEntry::Mem(_) => None,
        TableEntry::View(_) => unreachable!("INSERT through a view is refused above"),
    };
    let mut auto_rows: Vec<usize> = Vec::new();
    let mut first_allocated: Option<i64> = None;

    let mut inserted = 0u64;
    // A source query supplies already-evaluated values; a VALUES list
    // supplies expressions. Both fill the same target offsets.
    let value_rows: Vec<Vec<Datum>> = match &source_rows {
        Some(rows) => rows.clone(),
        None => Vec::new(),
    };
    let row_count = source_rows.as_ref().map_or(insert.rows.len(), Vec::len);
    let mut new_rows: Vec<Vec<Datum>> = Vec::with_capacity(row_count);
    for index in 0..row_count {
        let width = match source_rows.as_ref() {
            Some(_) => value_rows.get(index).map_or(0, Vec::len),
            None => insert.rows[index].len(),
        };
        if width != target_offsets.len() {
            return Err(DriverError::Unsupported(
                "VALUES arity does not match the column list",
            ));
        }
        let mut row = vec![Datum::Null; column_list.len()];
        let mut assigned = vec![false; column_list.len()];
        for (position, &offset) in target_offsets.iter().enumerate() {
            let value = match source_rows.as_ref() {
                Some(_) => value_rows[index][position].clone(),
                None => {
                    let rewritten =
                        rewrite_expr_resolved(&insert.rows[index][position], &NoResolver)
                            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                    rewritten
                        .eval(ctx, eval_chunk.get_row(0))
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?
                }
            };
            row[offset] = value;
            assigned[offset] = true;
        }
        // Go fills the auto-increment column before the default and NOT NULL
        // rules run, so an omitted auto column never looks like a missing
        // value (`adjustAutoIncrementDatum` runs inside the row build).
        if let Some(offset) = auto_increment_offset {
            // An omitted or explicitly NULL auto column becomes the zero
            // marker, which allocation replaces; Go does this before the
            // NOT NULL check, so a NULL here is never a bad-null error.
            if !assigned[offset] || row[offset] == Datum::Null {
                row[offset] = Datum::Int(0);
            }
            assigned[offset] = true;
            auto_rows.push(new_rows.len());
        }
        // Only a column the statement omits takes its default, and only such
        // a column can raise ErrNoDefaultForField (Go `fillColValue`).
        for offset in 0..column_list.len() {
            if !assigned[offset] {
                row[offset] = column_default(&column_meta, offset)?;
            }
        }
        // Go `Column.CheckNotNull`: an explicit NULL in a NOT NULL column is
        // ErrColumnCantNull, which is a different error from omitting a
        // column that has no default.
        for (offset, value) in row.iter().enumerate() {
            if *value == Datum::Null && column_is_not_null(&column_meta, offset) && assigned[offset]
            {
                return Err(DriverError::ColumnCannotBeNull(
                    column_list[offset].0.clone(),
                ));
            }
        }
        // Go casts each value to its column's type before the row is
        // written, which is what rounds a decimal to the column's scale and
        // parses a numeric string.
        if let TableEntry::Kv(kv) = &*table {
            for (offset, value) in row.iter_mut().enumerate() {
                let column = &kv.columns[offset];
                *value = cast_value_for_column(
                    std::mem::replace(value, Datum::Null),
                    &column.field_type,
                    &column.name,
                    new_rows.len(),
                    ctx,
                )?;
            }
        }
        new_rows.push(row);
        inserted += 1;
    }
    match table {
        TableEntry::View(_) => unreachable!("INSERT through a view is refused above"),
        TableEntry::Mem(mem) => mem.rows.extend(new_rows),
        TableEntry::Kv(kv) => {
            // The allocator lives on the table, so the ids are handed out here
            // rather than while the rows were being built.
            for index in &auto_rows {
                if let Some(allocated) = kv.apply_auto_increment(&mut new_rows[*index]) {
                    // Go keeps the FIRST allocated id of the statement.
                    if first_allocated.is_none() {
                        first_allocated = Some(allocated);
                    }
                }
            }
            // Go resolves a conflict per row, before the row is written:
            // REPLACE deletes every row it collides with, ON DUPLICATE KEY
            // UPDATE applies its assignments to the first one, and IGNORE
            // skips the row with the duplicate reported as a warning.
            inserted = 0;
            for row in &new_rows {
                let conflicts = kv
                    .conflicting_handles(row)
                    .map_err(|e| DriverError::Parse(format!("conflict lookup failed: {e:?}")))?;
                if !conflicts.is_empty() {
                    if insert.replace {
                        // Captured: the affected count is one per deleted row
                        // plus one for the inserted row.
                        for handle in &conflicts {
                            kv.delete_row(handle).map_err(|e| {
                                DriverError::Parse(format!("row delete failed: {e:?}"))
                            })?;
                            inserted += 1;
                        }
                    } else if !insert.on_duplicate.is_empty() {
                        inserted += apply_on_duplicate(
                            kv,
                            &conflicts[0],
                            row,
                            &insert.on_duplicate,
                            &column_list,
                            ctx,
                        )?;
                        continue;
                    } else if insert.ignore {
                        let reported = kv.duplicate_entry_error(row).map_err(|e| {
                            DriverError::Parse(format!("conflict lookup failed: {e:?}"))
                        })?;
                        if let crate::kv_table::KvTableError::DuplicateEntry { value, key } =
                            reported
                        {
                            let warning =
                                DriverError::DuplicateEntry { value, key }.to_mysql_error();
                            ctx.append_warning_parts(warning.code, &warning.message);
                        }
                        continue;
                    }
                }
                kv.insert_row(row).map_err(|e| match e {
                    crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                        DriverError::DuplicateEntry { value, key }
                    }
                    other => DriverError::Parse(format!("row encode failed: {other:?}")),
                })?;
                inserted += 1;
            }
        }
    }
    Ok((inserted, first_allocated))
}

/// Whether a conversion event is one TiDB reports nothing for.
///
/// Rounding a NUMBER into a narrower decimal is the case: captured, both
/// `INSERT INTO t(d DECIMAL(10,3)) VALUES (1.23456)` and
/// `ALTER TABLE t ADD COLUMN e DECIMAL(6,2) DEFAULT 3.14159` are accepted in
/// silence, storing 1.235 and 3.14. Go reaches that through
/// `ProduceDecWithSpecifiedTp`, whose rounding notice never becomes a
/// statement error. A STRING source is a different case -- it may not be a
/// number at all -- so it is never silent.
pub(crate) fn conversion_event_is_silent(
    value: &Datum,
    field_type: &FieldType,
    event: &tidb_datatype::ScalarConversionEvent,
) -> bool {
    let numeric_source = matches!(
        value,
        Datum::Int(_) | Datum::UInt(_) | Datum::Real(_) | Datum::Float32(_) | Datum::Decimal(_)
    );
    numeric_source
        && matches!(field_type.eval_type(), tidb_datatype::EvalType::Decimal)
        && matches!(event, tidb_datatype::ScalarConversionEvent::Truncated)
}

/// Go `table.CastValue` + `completeInsertErr`: converts one written value into
/// the column's own type, and names the failure the way the insert path does.
///
/// The strict SQL mode makes a bad value fail the statement; without it the
/// converted (clamped or truncated) value is stored and the same message is a
/// warning, which is what `sql_mode = ''` produces in TiDB.
fn cast_value_for_column(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    if value.is_null() {
        return Ok(value);
    }
    let converted = value
        .convert_to(field_type, ctx.conversion_flags())
        .map_err(|_| DriverError::IncorrectValue {
            type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
            value: datum_error_text(&value),
            column: column.to_owned(),
            row: row_index + 1,
        })?;
    let Some(event) = converted.event else {
        return Ok(converted.value);
    };
    if conversion_event_is_silent(&value, field_type, &event) {
        return Ok(converted.value);
    }
    // Go picks the message from the conversion's own error kind: a string
    // that does not fit is ErrDataTooLong, a number outside the column's
    // range is ErrWarnDataOutOfRange, and anything else is the
    // "Incorrect <type> value" form.
    let error = match event {
        tidb_datatype::ScalarConversionEvent::Overflow(_) => DriverError::DataOutOfRange {
            column: column.to_owned(),
            row: row_index + 1,
        },
        tidb_datatype::ScalarConversionEvent::Truncated
            if matches!(field_type.eval_type(), tidb_datatype::EvalType::String) =>
        {
            DriverError::DataTooLong {
                column: column.to_owned(),
                row: row_index + 1,
            }
        }
        tidb_datatype::ScalarConversionEvent::Truncated => DriverError::IncorrectValue {
            type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
            value: datum_error_text(&value),
            column: column.to_owned(),
            row: row_index + 1,
        },
    };
    if ctx.strict() {
        return Err(error);
    }
    let reported = error.to_mysql_error();
    ctx.append_warning_parts(reported.code, &reported.message);
    Ok(converted.value)
}

/// A value as MySQL prints it inside a conversion error message.
fn datum_error_text(value: &Datum) -> String {
    match value {
        Datum::Int(v) => v.to_string(),
        Datum::UInt(v) => v.to_string(),
        Datum::Real(v) => v.to_string(),
        Datum::Decimal(v) => v.to_string(),
        Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
        other => format!("{other:?}"),
    }
}

/// Orders candidate rows the way a DML statement's own `ORDER BY` does, and
/// reports the row cap its `LIMIT` sets.
///
/// Go plans `UPDATE`/`DELETE ... ORDER BY ... LIMIT n` as a sort and a limit
/// over the rows to modify, so the cap counts rows actually MODIFIED, not
/// rows examined -- which is why the limit is applied by the caller as it
/// modifies rather than by truncating this list.
fn order_rows_for_dml<H>(
    rows: &mut [(H, Vec<Datum>)],
    order_by: &[tidb_ast::OrderItem],
    field_types: &[FieldType],
    resolver: &impl tidb_expr::rewriter::ColumnResolver,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if order_by.is_empty() {
        return Ok(());
    }
    let mut items = Vec::with_capacity(order_by.len());
    for item in order_by {
        let expr = rewrite_expr_resolved(&item.expr, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        items.push((expr, item.desc));
    }
    // Each row's sort key is computed once, so the comparison itself cannot
    // fail partway through and leave a partial order.
    let mut keyed = Vec::with_capacity(rows.len());
    for (index, (_, row)) in rows.iter().enumerate() {
        let chunk = row_chunk(row, field_types)?;
        let mut key = Vec::with_capacity(items.len());
        for (expr, _) in &items {
            key.push(
                expr.eval(ctx, chunk.get_row(0))
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
            );
        }
        keyed.push((index, key));
    }
    let mut failure = None;
    keyed.sort_by(|left, right| {
        for (position, (_, desc)) in items.iter().enumerate() {
            let ordering = match tidb_expr::compare_datums(&left.1[position], &right.1[position]) {
                Ok(ordering) => ordering,
                Err(error) => {
                    failure = Some(error);
                    std::cmp::Ordering::Equal
                }
            };
            if ordering != std::cmp::Ordering::Equal {
                return if *desc { ordering.reverse() } else { ordering };
            }
        }
        std::cmp::Ordering::Equal
    });
    if let Some(error) = failure {
        return Err(DriverError::Exec(ExecError::Eval(error)));
    }
    let order: Vec<usize> = keyed.into_iter().map(|(index, _)| index).collect();
    apply_permutation(rows, &order);
    Ok(())
}

/// Reorders `rows` so that position `i` holds what was at `order[i]`.
fn apply_permutation<T>(rows: &mut [T], order: &[usize]) {
    let mut done = vec![false; rows.len()];
    for start in 0..rows.len() {
        if done[start] || order[start] == start {
            done[start] = true;
            continue;
        }
        let mut current = start;
        loop {
            let next = order[current];
            done[current] = true;
            if next == start {
                break;
            }
            rows.swap(current, next);
            current = next;
        }
    }
}

/// The row cap a DML `LIMIT` sets, which Go requires to be a constant.
fn dml_row_limit(limit: &Option<tidb_ast::Limit>) -> Result<Option<u64>, DriverError> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    if limit.offset.is_some() {
        return Err(DriverError::Unsupported(
            "an UPDATE/DELETE LIMIT takes no offset",
        ));
    }
    Ok(Some(eval_limit_bound(&limit.count)?))
}

/// Go `ON DUPLICATE KEY UPDATE`: applies the assignments to the row already
/// stored, and reports what the statement counts as affected.
///
/// Captured from TiDB: the assignments read the EXISTING row (`c = c + 1` on
/// a stored 10 gives 11, not the rejected value plus one), `VALUES(col)`
/// reads the row that would have been inserted, an update that changes
/// nothing counts 0, and one that changes something counts 2.
fn apply_on_duplicate(
    table: &mut crate::KvTable,
    handle: &crate::kv_table::TableHandle,
    candidate: &[Datum],
    assignments: &[tidb_ast::Assignment],
    column_list: &[(String, FieldType)],
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let Some(existing) = table
        .get_row_by_handle(handle)
        .map_err(|e| DriverError::Parse(format!("row read failed: {e:?}")))?
    else {
        return Ok(0);
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let mut updated = existing.clone();
    for assignment in assignments {
        let name = assignment
            .col
            .last()
            .ok_or(DriverError::Unsupported("empty assignment column"))?;
        let offset = column_list
            .iter()
            .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .ok_or_else(|| DriverError::UnknownColumnInClause {
                column: name.clone(),
                clause: "field list".to_owned(),
            })?;
        // `VALUES(col)` is the value the insert would have written, which Go
        // resolves before evaluating the assignment.
        let bound = substitute_values_references(&assignment.value, candidate, column_list)?;
        let resolver = TableResolver {
            table_name: "",
            columns: column_list,
        };
        let expr = rewrite_expr_resolved(&bound, &resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        let chunk = row_chunk(&updated, &field_types)?;
        let value = expr
            .eval(ctx, chunk.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        updated[offset] =
            cast_value_for_column(value, &field_types[offset], &column_list[offset].0, 0, ctx)?;
    }
    if updated == existing {
        // Captured: an update that changes nothing affects no rows.
        return Ok(0);
    }
    table.update_row(handle, &updated).map_err(|e| match e {
        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
            DriverError::DuplicateEntry { value, key }
        }
        other => DriverError::Parse(format!("row encode failed: {other:?}")),
    })?;
    Ok(2)
}

/// Replaces every `VALUES(col)` in an `ON DUPLICATE KEY UPDATE` assignment
/// with the literal the insert would have written for that column.
fn substitute_values_references(
    expr: &tidb_ast::Expr,
    candidate: &[Datum],
    column_list: &[(String, FieldType)],
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match expr {
        Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("values") => {
            let Some(Expr::Column(path)) = args.first() else {
                return Err(DriverError::Unsupported("VALUES() takes a column name"));
            };
            let name = path
                .last()
                .ok_or(DriverError::Unsupported("VALUES() takes a column name"))?;
            let offset = column_list
                .iter()
                .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .ok_or_else(|| DriverError::UnknownColumnInClause {
                    column: name.clone(),
                    clause: "field list".to_owned(),
                })?;
            datum_to_literal(&candidate[offset])?
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(substitute_values_references(
            inner,
            candidate,
            column_list,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(substitute_values_references(inner, candidate, column_list)?),
        ),
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(substitute_values_references(left, candidate, column_list)?),
            Box::new(substitute_values_references(right, candidate, column_list)?),
        ),
        other => other.clone(),
    })
}

/// Binds a prepared statement's parameters, replacing every `?` marker with
/// the literal for its execute-time value.
///
/// Go keeps the parsed statement and installs the values on the markers
/// themselves; this tier reaches execution through SQL text, so the markers
/// become literals and the statement is restored. That round trip is exact
/// for every value kind `datum_to_literal` covers, and a byte string that is
/// not UTF-8 becomes a hex literal rather than a lossy conversion.
///
/// Returns the bound SQL, or `ErrWrongParamCount` when the count does not
/// match the markers the statement carries.
pub fn bind_parameters(sql: &str, values: &[Datum]) -> Result<String, DriverError> {
    let mut stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let mut bound = 0usize;
    bind_statement_markers(&mut stmt, values, &mut bound)?;
    if bound != values.len() {
        return Err(DriverError::WrongParamCount);
    }
    Ok(stmt.restore())
}

/// The number of `?` markers a statement carries, which `COM_STMT_PREPARE`
/// reports to the client.
pub fn parameter_count(sql: &str) -> Result<usize, DriverError> {
    let mut stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let mut counted = 0usize;
    // Counting binds nothing: every marker reports itself and stays put.
    count_statement_markers(&mut stmt, &mut counted);
    Ok(counted)
}

/// Walks a statement's expressions, applying `visit` to every marker.
fn walk_statement_markers(stmt: &mut Stmt, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
    let walk_expr = walk_expr_markers;
    match stmt {
        Stmt::Query(query) => walk_query_markers(query, visit),
        Stmt::Dml(dml) => match &mut **dml {
            tidb_ast::DmlStmt::Insert(insert) => {
                for row in &mut insert.rows {
                    for value in row {
                        walk_expr(value, visit);
                    }
                }
                for assignment in &mut insert.on_duplicate {
                    walk_expr(&mut assignment.value, visit);
                }
                if let Some(source) = &mut insert.source {
                    walk_query_markers(source, visit);
                }
            }
            tidb_ast::DmlStmt::Update(update) => {
                for assignment in &mut update.assignments {
                    walk_expr(&mut assignment.value, visit);
                }
                if let Some(where_clause) = &mut update.where_clause {
                    walk_expr(where_clause, visit);
                }
            }
            tidb_ast::DmlStmt::Delete(delete) => {
                if let Some(where_clause) = &mut delete.where_clause {
                    walk_expr(where_clause, visit);
                }
            }
            _ => {}
        },
        _ => {}
    }
}

/// The markers inside one query, including its set-operation terms.
fn walk_query_markers(query: &mut tidb_ast::QueryStmt, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
    match query {
        tidb_ast::QueryStmt::Select(select) => walk_select_markers(select, visit),
        tidb_ast::QueryStmt::SetOpr(set_opr) => walk_set_opr_markers(set_opr, visit),
    }
}

/// The markers inside one set operation and, recursively, its nested terms.
fn walk_set_opr_markers(
    set_opr: &mut tidb_ast::SetOprStmt,
    visit: &mut dyn FnMut(&mut tidb_ast::Expr),
) {
    for term in &mut set_opr.terms {
        match &mut term.body {
            tidb_ast::SetOprTermBody::Select(select) => walk_select_markers(select, visit),
            tidb_ast::SetOprTermBody::Nested(nested) => walk_set_opr_markers(nested, visit),
        }
    }
}

/// The markers inside one `SELECT`.
fn walk_select_markers(
    select: &mut tidb_ast::SelectStmt,
    visit: &mut dyn FnMut(&mut tidb_ast::Expr),
) {
    for field in select.fields.fields_mut() {
        if let tidb_ast::SelectField::Expr { expr, .. } = field {
            walk_expr_markers(expr, visit);
        }
    }
    if let Some(where_clause) = &mut select.where_clause {
        walk_expr_markers(where_clause, visit);
    }
    if let Some(having) = &mut select.having {
        walk_expr_markers(having, visit);
    }
    for item in &mut select.order_by {
        walk_expr_markers(&mut item.expr, visit);
    }
    for item in &mut select.group_by {
        walk_expr_markers(&mut item.expr, visit);
    }
    if let Some(limit) = &mut select.limit {
        walk_expr_markers(&mut limit.count, visit);
        if let Some(offset) = &mut limit.offset {
            walk_expr_markers(offset, visit);
        }
    }
}

/// The markers inside one expression tree.
fn walk_expr_markers(expr: &mut tidb_ast::Expr, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
    use tidb_ast::Expr;
    if matches!(expr, Expr::ParamMarker { .. }) {
        visit(expr);
        return;
    }
    match expr {
        Expr::Paren(inner) | Expr::Unary(_, inner) => walk_expr_markers(inner, visit),
        Expr::Binary(_, left, right) => {
            walk_expr_markers(left, visit);
            walk_expr_markers(right, visit);
        }
        Expr::Func { args, .. } => {
            for arg in args {
                walk_expr_markers(arg, visit);
            }
        }
        Expr::In { expr, list, .. } => {
            walk_expr_markers(expr, visit);
            for item in list {
                walk_expr_markers(item, visit);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            walk_expr_markers(expr, visit);
            walk_expr_markers(low, visit);
            walk_expr_markers(high, visit);
        }
        Expr::Like { expr, pattern, .. } => {
            walk_expr_markers(expr, visit);
            walk_expr_markers(pattern, visit);
        }
        Expr::Is { expr, .. } => walk_expr_markers(expr, visit),
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            if let Some(value) = value {
                walk_expr_markers(value, visit);
            }
            for (condition, result) in when_clauses {
                walk_expr_markers(condition, visit);
                walk_expr_markers(result, visit);
            }
            if let Some(else_clause) = else_clause {
                walk_expr_markers(else_clause, visit);
            }
        }
        Expr::Cast(cast) => walk_expr_markers(&mut cast.expr, visit),
        _ => {}
    }
}

/// Replaces each marker with its value, in the parser's own left-to-right
/// marker order.
fn bind_statement_markers(
    stmt: &mut Stmt,
    values: &[Datum],
    bound: &mut usize,
) -> Result<(), DriverError> {
    let mut failure = None;
    walk_statement_markers(stmt, &mut |expr| {
        let order = match expr {
            tidb_ast::Expr::ParamMarker { order, .. } => *order,
            _ => return,
        };
        match values.get(order) {
            Some(value) => match datum_to_literal(value) {
                Ok(literal) => {
                    *expr = literal;
                    *bound += 1;
                }
                Err(error) => failure = Some(error),
            },
            None => failure = Some(DriverError::WrongParamCount),
        }
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Counts the markers without changing them.
fn count_statement_markers(stmt: &mut Stmt, counted: &mut usize) {
    walk_statement_markers(stmt, &mut |_| *counted += 1);
}

/// Go `havingWindowAndOrderbyExprResolver`: an `ORDER BY` item is resolved
/// against the SELECT list first, so a select alias and an output position
/// both name a projected expression.
///
/// Go rewrites the reference into the projected expression itself, which is
/// what this does -- the sort then runs over the source rows with no plan
/// reshuffle, and an expression BUILT on an alias (`ORDER BY twice + 0`)
/// falls out for free.
///
/// Captured from TiDB: an alias SHADOWS a real column of the same name
/// (`SELECT b AS a FROM t ORDER BY a` sorts by `b`); a bare integer is a
/// 1-based output position, and only at the top level (`ORDER BY twice + 0`
/// is arithmetic, not position 1); an out-of-range position and an unknown
/// name are both `ErrUnknownColumn` naming the `order clause`.
fn substitute_output_aliases(
    expr: &tidb_ast::Expr,
    fields: &[SelectField],
    top_level: bool,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // A bare integer at the top of an ORDER BY item is an output position.
    if top_level {
        if let Expr::Int(text) = expr {
            let position: usize = text.parse().map_err(|_| unknown_order_column(text))?;
            let projected = fields
                .iter()
                .filter_map(|field| match field {
                    SelectField::Expr { expr, .. } => Some(expr),
                    SelectField::Wildcard(_) => None,
                })
                .nth(position.wrapping_sub(1))
                .ok_or_else(|| unknown_order_column(text))?;
            if position == 0 {
                return Err(unknown_order_column(text));
            }
            return Ok(projected.clone());
        }
    }
    Ok(match expr {
        // A one-segment name may be a select alias; a qualified one
        // (`t.a`) always addresses the source.
        Expr::Column(path) if path.len() == 1 => {
            let alias = fields.iter().find_map(|field| match field {
                SelectField::Expr {
                    expr,
                    alias: Some(alias),
                } if alias.eq_ignore_ascii_case(&path[0]) => Some(expr),
                _ => None,
            });
            match alias {
                Some(expr) => expr.clone(),
                None => expr.clone(),
            }
        }
        Expr::Paren(inner) => {
            Expr::Paren(Box::new(substitute_output_aliases(inner, fields, false)?))
        }
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(substitute_output_aliases(inner, fields, false)?),
        ),
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(substitute_output_aliases(left, fields, false)?),
            Box::new(substitute_output_aliases(right, fields, false)?),
        ),
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| substitute_output_aliases(arg, fields, false))
                .collect::<Result<_, _>>()?,
            origin_position: *origin_position,
        },
        other => other.clone(),
    })
}

/// The `ErrUnknownColumn` an unresolvable `ORDER BY` item reports, when the
/// item is a plain name -- anything else keeps the rewriter's own error.
fn order_by_column_error(expr: &tidb_ast::Expr) -> Option<DriverError> {
    match expr {
        tidb_ast::Expr::Column(path) => Some(unknown_order_column(&path.join("."))),
        _ => None,
    }
}

/// Go `ErrUnknownColumn` naming the `order clause`.
fn unknown_order_column(name: &str) -> DriverError {
    DriverError::UnknownColumnInClause {
        column: name.to_owned(),
        clause: "order clause".to_owned(),
    }
}

/// Go `aggregation.NewAggFuncDesc` + `baseFuncDesc.TypeInfer`: the aggregate
/// kind and the result type inferred for its argument.
pub(crate) fn agg_kind_and_type(
    name: &str,
    arg: &Expression,
) -> Result<(AggKind, FieldType), DriverError> {
    Ok(match name {
        // Go `typeInfer4Count`: a binary `BIGINT(21)` that never returns NULL
        // -- an empty group (and an empty window frame) counts 0.
        "COUNT" => {
            let mut t = FieldType::new(FieldTypeCode::LongLong);
            t.set_flen(21);
            t.set_decimal(0);
            t.add_flags(
                tidb_datatype::FieldTypeFlags::BINARY | tidb_datatype::FieldTypeFlags::NOT_NULL,
            );
            (AggKind::Count, t)
        }
        // Go `typeInfer4Sum`: DOUBLE for a real argument, DECIMAL for every
        // other numeric one -- `SUM` over a BIGINT column is a DECIMAL in
        // MySQL, not a BIGINT (captured: `sum(a)` reports type 246).
        "SUM" => {
            let real = arg
                .static_type()
                .is_some_and(|t| t.eval_type() == tidb_datatype::EvalType::Real);
            let t = if real {
                FieldType::new(FieldTypeCode::Double)
            } else {
                FieldType::new(FieldTypeCode::NewDecimal)
            };
            (AggKind::Sum, t)
        }
        // Go `typeInfer4MaxMin`: the result carries the argument's
        // own type (with NOT NULL dropped, which this seed does not
        // track on result columns).
        "MIN" | "MAX" => {
            let t = arg
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            let kind = if name == "MIN" {
                AggKind::Min
            } else {
                AggKind::Max
            };
            (kind, t)
        }
        // Go `typeInfer4Avg`: DOUBLE for real arguments, otherwise
        // DECIMAL. The decimal scale Go derives from
        // div_precision_increment is display metadata this seed
        // does not set on result columns (documented deferral).
        "AVG" => {
            let code = arg
                .static_type()
                .map_or(FieldTypeCode::NewDecimal, |t| match t.code() {
                    FieldTypeCode::Float | FieldTypeCode::Double => FieldTypeCode::Double,
                    _ => FieldTypeCode::NewDecimal,
                });
            (AggKind::Avg, FieldType::new(code))
        }
        _ => {
            return Err(DriverError::Unsupported(
                "this aggregate function is deferred",
            ))
        }
    })
}

/// The aggregation's output columns, addressed by name.
///
/// Go rewrites `HAVING`/`ORDER BY` to reference the aggregation's output
/// schema (`resolveHavingAndOrderBy` + `buildProjection`), so those clauses see
/// the aggregate results rather than the source rows. This resolver is that
/// output schema: a name is a select field's alias or column name, or an
/// aggregate's restored text.
struct AggOutputResolver {
    names: Vec<String>,
    types: Vec<FieldType>,
}

impl ColumnResolver for AggOutputResolver {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let index = self
            .names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))?;
        Some((index, self.types[index].clone(), (index + 1) as i64))
    }
}

/// Go `havingWindowAndOrderbyExprResolver`: rewrites a `HAVING`/`ORDER BY`
/// expression so every aggregate in it refers to an aggregation output column,
/// appending a hidden aggregate when the select list does not already compute
/// it.
///
/// The substitution is textual in the same sense Go's is structural: an
/// aggregate node becomes a column reference whose name is the aggregate's
/// restored text, which [`AggOutputResolver`] then binds to the output column.
///
/// Only the expression forms the expression rewriter itself supports are
/// walked (literals, parentheses, unary, binary, columns, aggregates); any
/// other form would fail to rewrite anyway and is returned unchanged.
fn substitute_aggregates(
    expr: &tidb_ast::Expr,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_names: &[String],
    resolver: &ScopeResolver<'_>,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // GROUPING() is hoisted the same way an aggregate is: the value is
    // computed by the rollup pass into an output column, and the clause reads
    // that column. A GROUPING() only HAVING or ORDER BY needs becomes a hidden
    // column and is trimmed by the final projection.
    if let Some(args) = grouping_call_args(expr) {
        let display = expr.restore();
        let name = add_grouping_column(
            args,
            display,
            agg_funcs,
            names,
            types,
            grouping_specs,
            group_by_names,
        )?;
        return Ok(Expr::Column(vec![name]));
    }
    Ok(match expr {
        // A column that HAVING/ORDER BY references but the select list does
        // not project: Go carries it out of the aggregation as a hidden
        // FIRST_ROW column, exactly as it does for a selected group column.
        // A column that is not grouped is rejected, which is what
        // ONLY_FULL_GROUP_BY reports in Go.
        // A hoisted window column is computed ABOVE the aggregation, so it is
        // neither grouped nor aggregated and must be left alone here; it
        // resolves once the window stage has appended it.
        Expr::Column(path)
            if path
                .last()
                .is_some_and(|name| crate::window::is_window_column(name)) =>
        {
            expr.clone()
        }
        Expr::Column(path) => {
            let name = path.last().cloned().unwrap_or_default();
            // `__apply_N` is not a real column: it is the placeholder a
            // correlated subquery's extraction left behind, standing in for
            // the column an Apply appends above the aggregation once the
            // subquery is bound and run. It carries no ONLY_FULL_GROUP_BY
            // obligation of its own, so it passes through untouched.
            if name.starts_with("__apply_") {
                return Ok(expr.clone());
            }
            if names
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&name))
            {
                return Ok(expr.clone());
            }
            if !group_by_names
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&name))
            {
                return Err(DriverError::Unsupported(
                    "this clause references a column that is neither grouped nor aggregated",
                ));
            }
            let carrier = rewrite_expr_resolved(expr, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            let ftype = carrier
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            agg_funcs.push(AggFunc {
                kind: AggKind::FirstRow,
                arg: Some(carrier),
                extra_args: Vec::new(),
                distinct: false,
                order_by: Vec::new(),
            });
            names.push(name.clone());
            types.push(ftype);
            Expr::Column(vec![name])
        }
        // GROUP_CONCAT is substituted the same way: the aggregate is hoisted
        // and the field becomes a reference to its output column.
        Expr::Aggregate { .. } | Expr::GroupConcat { .. } => {
            let text = expr.restore();
            if !names.iter().any(|name| name.eq_ignore_ascii_case(&text)) {
                let (func, ftype) = build_agg_func(expr, resolver)?;
                agg_funcs.push(func);
                names.push(text.clone());
                types.push(ftype);
            }
            Expr::Column(vec![text])
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(substitute_aggregates(
            inner,
            agg_funcs,
            names,
            types,
            grouping_specs,
            group_by_names,
            resolver,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(substitute_aggregates(
                inner,
                agg_funcs,
                names,
                types,
                grouping_specs,
                group_by_names,
                resolver,
            )?),
        ),
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(substitute_aggregates(
                lhs,
                agg_funcs,
                names,
                types,
                grouping_specs,
                group_by_names,
                resolver,
            )?),
            Box::new(substitute_aggregates(
                rhs,
                agg_funcs,
                names,
                types,
                grouping_specs,
                group_by_names,
                resolver,
            )?),
        ),
        other => other.clone(),
    })
}

/// The window-call index a select field IS, once
/// [`crate::window::hoist_windows`] has replaced the call with its computed
/// column.
fn hoisted_window_index(expr: &tidb_ast::Expr) -> Option<usize> {
    let tidb_ast::Expr::Column(path) = expr else {
        return None;
    };
    let name = path.last()?;
    crate::window::is_window_column(name)
        .then(|| crate::window::window_column_index(name))
        .flatten()
}

/// Whether `expr` reads a hoisted window column anywhere inside a larger
/// expression.
fn expr_has_hoisted_window(expr: &tidb_ast::Expr) -> bool {
    struct Finder {
        found: bool,
    }
    impl tidb_ast::Visitor for Finder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Column(path)) = node.downcast_ref::<tidb_ast::Expr>() {
                if path
                    .last()
                    .is_some_and(|name| crate::window::is_window_column(name))
                {
                    self.found = true;
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut finder = Finder { found: false };
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut finder);
    finder.found
}

/// Builds one aggregate function (and its Go-inferred result type) from an
/// `Expr::Aggregate` node.
fn build_agg_func(
    expr: &tidb_ast::Expr,
    resolver: &ScopeResolver<'_>,
) -> Result<(AggFunc, FieldType), DriverError> {
    // GROUP_CONCAT is its own AST shape: it carries a separator and its own
    // row ORDER BY rather than being a one-argument aggregate.
    if let tidb_ast::Expr::GroupConcat {
        distinct,
        args,
        order_by,
        separator,
    } = expr
    {
        // `GROUP_CONCAT(a, b, ...)` concatenates its arguments per row before
        // the rows are joined; the first argument rides `arg` and the rest
        // ride `extra_args`.
        let Some((first, rest)) = args.split_first() else {
            return Err(DriverError::Unsupported(
                "GROUP_CONCAT requires at least one argument",
            ));
        };
        let arg = rewrite_expr_resolved(first, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        let mut extra_args = Vec::with_capacity(rest.len());
        for extra in rest {
            extra_args.push(
                rewrite_expr_resolved(extra, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
            );
        }
        // The aggregate's own ORDER BY items resolve against the SOURCE row,
        // the same scope the concatenated argument does.
        let mut order_items = Vec::with_capacity(order_by.len());
        for item in order_by {
            let expr = rewrite_expr_resolved(&item.expr, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            order_items.push((expr, item.desc));
        }
        let mut ret_type = FieldType::new(FieldTypeCode::VarString);
        ret_type.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        return Ok((
            AggFunc {
                kind: AggKind::GroupConcat {
                    separator: separator.value.clone(),
                },
                arg: Some(arg),
                extra_args,
                distinct: *distinct,
                order_by: order_items,
            },
            ret_type,
        ));
    }
    let tidb_ast::Expr::Aggregate {
        name,
        distinct,
        args,
    } = expr
    else {
        return Err(DriverError::Unsupported("not an aggregate function"));
    };
    // `COUNT(DISTINCT a, b, ...)` is the one non-GROUP_CONCAT aggregate the
    // parser lets through with more than one argument (`parse_aggregate`
    // rejects a bare `COUNT(a, b)` and every multi-argument `SUM`/`AVG`/etc.
    // at parse time), so only COUNT needs an `extra_args`-carrying path here.
    let Some((first, rest)) = args.split_first() else {
        return Err(DriverError::Unsupported(
            "multi-argument aggregates are deferred",
        ));
    };
    if !rest.is_empty() && name != "COUNT" {
        return Err(DriverError::Unsupported(
            "multi-argument aggregates are deferred",
        ));
    }
    // A subquery inside an aggregate's own argument (`SUM((SELECT ...))`,
    // `SUM(CASE WHEN EXISTS(...) THEN v END)`) would need to run once per
    // SOURCE row, before the aggregate accumulates it -- an Apply BELOW the
    // aggregation, rather than the Apply above it this driver builds for a
    // select-field/HAVING/ORDER BY subquery (which reads the already-grouped
    // value). That per-row Apply is not built here; refuse precisely rather
    // than let the per-row rewriter reject it with its generic message.
    if expr_has_subquery(first) || rest.iter().any(expr_has_subquery) {
        return Err(DriverError::Unsupported(
            "a subquery inside an aggregate function's argument is not supported yet",
        ));
    }
    let arg = rewrite_expr_resolved(first, resolver)
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    let mut extra_args = Vec::with_capacity(rest.len());
    for extra in rest {
        extra_args.push(
            rewrite_expr_resolved(extra, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }
    let (kind, ftype) = agg_kind_and_type(name, &arg)?;
    Ok((
        AggFunc {
            kind,
            arg: Some(arg),
            extra_args,
            distinct: *distinct,
            order_by: Vec::new(),
        },
        ftype,
    ))
}

/// The type of the column an Apply appends for a correlated scalar subquery.
///
/// Go infers it statically from the subquery's select field; here the query is
/// planned with every correlated column bound to NULL, which reaches the same
/// field type without depending on any outer row -- and it must, because the
/// appended column's width is fixed before the first inner run (a `SUM` is a
/// 40-byte decimal, not an 8-byte integer). Falling back to `LongLong`
/// matches what the rest of the seed does for an uninferred expression.
fn subquery_result_type(
    correlated: &CorrelatedSubquery,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<FieldType> {
    let nulls: Vec<(Vec<String>, Datum)> = correlated
        .columns
        .iter()
        .map(|path| (path.clone(), Datum::Null))
        .collect();
    let typed = bind_subquery_columns(&correlated.select, &nulls).ok()?;
    run_select_stmt(&typed, catalog, current_db, ctx)
        .ok()
        .and_then(|(columns, _)| columns.first().map(|(_, ft)| ft.clone()))
}

/// A short description of a driver error, for the executor-level error the
/// apply callback must return.
fn driver_error_text(error: &DriverError) -> &'static str {
    match error {
        DriverError::SubqueryReturnsMoreThanOneRow => "Subquery returns more than 1 row",
        DriverError::Unsupported(text) => text,
        _ => "the correlated subquery failed",
    }
}

/// What the outer expression asks of a correlated subquery's result.
///
/// Go builds a different plan for each: `handleScalarSubquery` for a scalar
/// read, and a semi join (`LogicalJoin` with `SemiJoin`/`AntiSemiJoin`/
/// `LeftOuterSemiJoin`) for `EXISTS`, `IN` and `ANY`/`ALL`. Here they all ride
/// one Apply, because the join's answer for one outer row is exactly what
/// running the inner query for that row and folding the result yields.
enum SubqueryKind {
    /// A scalar read: the one value the subquery selects, NULL if no row.
    Scalar,
    /// `[NOT] EXISTS`.
    Exists { not: bool },
    /// `lhs [NOT] IN (subquery)`. `lhs` belongs to the OUTER scope and is
    /// evaluated per outer row against that row's inner result.
    In { lhs: tidb_ast::Expr, not: bool },
    /// `lhs <op> ANY|ALL (subquery)`.
    Compare {
        op: tidb_ast::BinaryOp,
        lhs: tidb_ast::Expr,
        all: bool,
    },
}

/// A correlated subquery found in an outer expression: the subquery itself and
/// what its result is asked for.
struct CorrelatedSubquery {
    select: tidb_ast::SelectStmt,
    kind: SubqueryKind,
    columns: Vec<Vec<String>>,
}

/// Whether `expr` references a column of the OUTER scope, which is what makes
/// a subquery correlated (Go's `ExtractCorrelatedCols4LogicalPlan`).
///
/// A reference is correlated when the inner query's own `FROM` cannot resolve
/// it but the outer scope can -- the same two-scope test Go's name resolver
/// applies when it binds a column to an outer plan's schema.
fn collect_correlated_columns(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    found: &mut Vec<Vec<String>>,
    ctx: &crate::StmtContext,
) {
    let inner = match &select.from {
        None => FromScope::default(),
        Some(join) => match build_join(join, catalog, current_db, ctx) {
            Ok((_, scope)) => scope,
            // An unresolvable inner FROM is reported by the inner run itself.
            Err(_) => FromScope::default(),
        },
    };
    let mut visit = |expr: &tidb_ast::Expr| {
        collect_outer_columns(expr, &inner, outer, found);
    };
    for field in select.fields.fields() {
        if let SelectField::Expr { expr, .. } = field {
            visit(expr);
        }
    }
    if let Some(where_clause) = &select.where_clause {
        visit(where_clause);
    }
    if let Some(having) = &select.having {
        visit(having);
    }
    for item in &select.group_by {
        visit(&item.expr);
    }
    for item in &select.order_by {
        visit(&item.expr);
    }
}

/// Records every column reference in `expr` that the inner scope cannot
/// resolve but the outer scope can.
fn collect_outer_columns(
    expr: &tidb_ast::Expr,
    inner: &FromScope,
    outer: &FromScope,
    found: &mut Vec<Vec<String>>,
) {
    use tidb_ast::Expr;
    match expr {
        Expr::Column(path) => {
            let inner_resolver = ScopeResolver { scope: inner };
            let outer_resolver = ScopeResolver { scope: outer };
            if inner_resolver.resolve(path).is_none()
                && outer_resolver.resolve(path).is_some()
                && !found.contains(path)
            {
                found.push(path.clone());
            }
        }
        Expr::Paren(inner_expr)
        | Expr::Unary(_, inner_expr)
        | Expr::Is {
            expr: inner_expr, ..
        } => {
            collect_outer_columns(inner_expr, inner, outer, found);
        }
        Expr::Binary(_, lhs, rhs) => {
            collect_outer_columns(lhs, inner, outer, found);
            collect_outer_columns(rhs, inner, outer, found);
        }
        Expr::In { expr, list, .. } => {
            collect_outer_columns(expr, inner, outer, found);
            for item in list {
                collect_outer_columns(item, inner, outer, found);
            }
        }
        Expr::Aggregate { args, .. } => {
            for arg in args {
                collect_outer_columns(arg, inner, outer, found);
            }
        }
        _ => {}
    }
}

/// Replaces each correlated column reference with the literal for the outer
/// row's value, which is this port's equivalent of Go's apply loop writing
/// `*col.Data` before re-running the inner plan.
fn bind_correlated_columns(
    expr: &tidb_ast::Expr,
    bindings: &[(Vec<String>, Datum)],
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match expr {
        Expr::Column(path) => match bindings.iter().find(|(bound, _)| paths_match(bound, path)) {
            Some((_, value)) => datum_to_literal(value)?,
            None => expr.clone(),
        },
        Expr::Paren(inner) => Expr::Paren(Box::new(bind_correlated_columns(inner, bindings)?)),
        Expr::Unary(op, inner) => {
            Expr::Unary(*op, Box::new(bind_correlated_columns(inner, bindings)?))
        }
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(bind_correlated_columns(expr, bindings)?),
            target: *target,
            not: *not,
        },
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(bind_correlated_columns(lhs, bindings)?),
            Box::new(bind_correlated_columns(rhs, bindings)?),
        ),
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(bind_correlated_columns(expr, bindings)?),
            list: list
                .iter()
                .map(|item| bind_correlated_columns(item, bindings))
                .collect::<Result<_, _>>()?,
            not: *not,
        },
        Expr::Aggregate {
            name,
            distinct,
            args,
        } => Expr::Aggregate {
            name: name.clone(),
            distinct: *distinct,
            args: args
                .iter()
                .map(|arg| bind_correlated_columns(arg, bindings))
                .collect::<Result<_, _>>()?,
        },
        other => other.clone(),
    })
}

/// Whether a bound path and a reference name the same column. A bare `a`
/// matches a bound `t.a`, since the inner reference may be unqualified.
fn paths_match(bound: &[String], candidate: &[String]) -> bool {
    if bound.len() == candidate.len() {
        return bound
            .iter()
            .zip(candidate)
            .all(|(a, b)| a.eq_ignore_ascii_case(b));
    }
    match (bound.last(), candidate.last()) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        _ => false,
    }
}

/// Substitutes `bindings` for the correlated column references in every clause
/// of `select`.
fn bind_subquery_columns(
    select: &tidb_ast::SelectStmt,
    bindings: &[(Vec<String>, Datum)],
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut bound = select.clone();
    for field in bound.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = bind_correlated_columns(expr, bindings)?;
        }
    }
    if let Some(where_clause) = &bound.where_clause {
        bound.where_clause = Some(bind_correlated_columns(where_clause, bindings)?);
    }
    if let Some(having) = &bound.having {
        bound.having = Some(bind_correlated_columns(having, bindings)?);
    }
    for item in &mut bound.group_by {
        item.expr = bind_correlated_columns(&item.expr, bindings)?;
    }
    for item in &mut bound.order_by {
        item.expr = bind_correlated_columns(&item.expr, bindings)?;
    }
    Ok(bound)
}

/// Binds every correlated column in `select` and runs it for one outer row.
fn run_correlated_subquery(
    correlated: &CorrelatedSubquery,
    outer_values: &[Datum],
    outer_scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let mut bindings = Vec::with_capacity(correlated.columns.len());
    for path in &correlated.columns {
        let resolver = ScopeResolver { scope: outer_scope };
        // The aggregation's output row has no table qualifiers of its own
        // (Go's post-aggregation schema is one flat row), so a qualified
        // correlated reference (`t.g`) falls back to a bare-name lookup
        // (`g`) when the qualified one does not resolve. The plain WHERE/
        // SELECT Apply paths bind against the real `FromScope`, where the
        // qualified lookup already succeeds and this fallback never fires.
        let (index, _, _) = resolver
            .resolve(path)
            .or_else(|| {
                let name = path.last()?;
                resolver.resolve(std::slice::from_ref(name))
            })
            .ok_or(DriverError::Unsupported("unresolved correlated column"))?;
        let value = outer_values
            .get(index)
            .cloned()
            .ok_or(DriverError::Unsupported("correlated column out of range"))?;
        bindings.push((path.clone(), value));
    }

    let bound = bind_subquery_columns(&correlated.select, &bindings)?;
    let (_, rows) = run_select_stmt(&bound, catalog, current_db, ctx)?;
    match &correlated.kind {
        // EXISTS folds to 1/0 per outer row.
        SubqueryKind::Exists { not } => Ok(Datum::Int(i64::from(!rows.is_empty() != *not))),
        SubqueryKind::Scalar => match rows.len() {
            0 => Ok(Datum::Null),
            1 => {
                let [value] = rows[0].as_slice() else {
                    return Err(DriverError::Unsupported(
                        "a scalar subquery selecting several columns is not supported yet",
                    ));
                };
                Ok(value.clone())
            }
            _ => Err(DriverError::SubqueryReturnsMoreThanOneRow),
        },
        // The semi-join shapes: this outer row's inner result becomes a value
        // list, and the test is evaluated over it exactly as the uncorrelated
        // fold evaluates its own folded list -- same `IN`, same comparisons,
        // so the same three-valued answers.
        SubqueryKind::In { lhs, not } => {
            let list = subquery_value_list(
                &rows,
                "an IN subquery selecting several columns is not supported yet",
            )?;
            let test = in_list_expr(lhs.clone(), list, *not);
            eval_expr_on_row(&test, outer_scope, outer_values, ctx)
        }
        SubqueryKind::Compare { op, lhs, all } => {
            let list = subquery_value_list(
                &rows,
                "an ANY/ALL subquery selecting several columns is not supported yet",
            )?;
            let test = any_all_expr(*op, lhs.clone(), *all, list);
            eval_expr_on_row(&test, outer_scope, outer_values, ctx)
        }
    }
}

/// A subquery result's single column, as the literals a value list needs.
fn subquery_value_list(
    rows: &[Vec<Datum>],
    several_columns: &'static str,
) -> Result<Vec<tidb_ast::Expr>, DriverError> {
    let mut list = Vec::with_capacity(rows.len());
    for row in rows {
        let [value] = row.as_slice() else {
            return Err(DriverError::Unsupported(several_columns));
        };
        list.push(datum_to_literal(value)?);
    }
    Ok(list)
}

/// `lhs [NOT] IN (list)`, with the empty list written as the constant it is.
///
/// `x IN ()` is not sayable in SQL: an empty subquery result makes `IN` false
/// and `NOT IN` true for every x INCLUDING NULL, because MySQL evaluates the
/// semi join, which finds no row to match. The non-empty case keeps the
/// ordinary `IN`, whose NULL rules are the three-valued ones (an unmatched x
/// against a list holding NULL is NULL, not false).
fn in_list_expr(lhs: tidb_ast::Expr, list: Vec<tidb_ast::Expr>, not: bool) -> tidb_ast::Expr {
    if list.is_empty() {
        return tidb_ast::Expr::Int(i64::from(not).to_string());
    }
    tidb_ast::Expr::In {
        expr: Box::new(lhs),
        list,
        not,
    }
}

/// `lhs <op> ANY|ALL (list)` as the OR/AND chain it is defined to be.
///
/// Go's `buildSemiApply` for a comparison subquery builds the same disjunction
/// (`ANY`) or conjunction (`ALL`) of per-value comparisons, which is where the
/// three-valued behaviour comes from: `20 > ANY (25, NULL)` is
/// `false OR NULL` = NULL, while `20 > ALL (25, NULL)` is `false AND NULL` =
/// false. An empty list has no comparison at all, so `ALL` is vacuously TRUE
/// and `ANY` is FALSE -- both for a NULL `lhs` too.
fn any_all_expr(
    op: tidb_ast::BinaryOp,
    lhs: tidb_ast::Expr,
    all: bool,
    list: Vec<tidb_ast::Expr>,
) -> tidb_ast::Expr {
    use tidb_ast::{BinaryOp, Expr};
    let compare = |value: Expr| Expr::Binary(op, Box::new(lhs.clone()), Box::new(value));
    let mut values = list.into_iter();
    let Some(first) = values.next() else {
        return Expr::Int(i64::from(all).to_string());
    };
    let combine = if all {
        BinaryOp::LogicAnd
    } else {
        BinaryOp::LogicOr
    };
    values.fold(compare(first), |acc, value| {
        Expr::Binary(combine, Box::new(acc), Box::new(compare(value)))
    })
}

/// Evaluates an expression over the OUTER scope's columns for one outer row.
///
/// The semi-join folds keep their left operand in the outer scope rather than
/// binding it to a literal, so the comparison runs through the very same
/// expression evaluator the uncorrelated path uses.
fn eval_expr_on_row(
    expr: &tidb_ast::Expr,
    scope: &FromScope,
    values: &[Datum],
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let types: Vec<FieldType> = scope.column_list().into_iter().map(|(_, ft)| ft).collect();
    let rewritten = rewrite_expr_resolved(expr, &ScopeResolver { scope })
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    let chunk = row_chunk(values, &types)?;
    rewritten
        .eval(ctx, chunk.get_row(0))
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))
}

/// Finds the one correlated subquery in `expr`, replacing it with a reference
/// to the column an [`ApplyExec`] will append at `index`.
///
/// Go's rewriter does the same substitution: after building the Apply, the
/// subquery expression becomes the Apply schema's last column.
fn extract_correlated_subquery(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    index: usize,
    found: &mut Option<CorrelatedSubquery>,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // The synthetic name the appended column answers to.
    let placeholder = |index: usize| Expr::Column(vec![format!("__apply_{index}")]);
    Ok(match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            let tidb_ast::QueryStmt::Select(select) = &**query else {
                return Err(DriverError::Unsupported(
                    "set-operation subqueries are not supported yet",
                ));
            };
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                // Uncorrelated: the folding pass handles it.
                return Ok(expr.clone());
            }
            if found.is_some() {
                return Err(DriverError::Unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            let kind = match expr {
                Expr::Exists { not, .. } => SubqueryKind::Exists { not: *not },
                _ => SubqueryKind::Scalar,
            };
            *found = Some(CorrelatedSubquery {
                select: (**select).clone(),
                kind,
                columns,
            });
            placeholder(index)
        }
        // Go turns a correlated IN / ANY / ALL into a semi join; the Apply
        // here answers the same question one outer row at a time, with the
        // tested left operand staying in the outer expression.
        Expr::InSubquery {
            expr: lhs,
            subquery,
            not,
        } => {
            let select = subquery_select(subquery)?;
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                return Ok(expr.clone());
            }
            if found.is_some() || expr_has_subquery(lhs) {
                return Err(DriverError::Unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            *found = Some(CorrelatedSubquery {
                select: select.clone(),
                kind: SubqueryKind::In {
                    lhs: (**lhs).clone(),
                    not: *not,
                },
                columns,
            });
            placeholder(index)
        }
        Expr::CompareSubquery {
            op,
            left,
            all,
            subquery,
        } => {
            let select = subquery_select(subquery)?;
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                return Ok(expr.clone());
            }
            if found.is_some() || expr_has_subquery(left) {
                return Err(DriverError::Unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            *found = Some(CorrelatedSubquery {
                select: select.clone(),
                kind: SubqueryKind::Compare {
                    op: *op,
                    lhs: (**left).clone(),
                    all: *all,
                },
                columns,
            });
            placeholder(index)
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(extract_correlated_subquery(
            inner, outer, catalog, current_db, index, found, ctx,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(extract_correlated_subquery(
                inner, outer, catalog, current_db, index, found, ctx,
            )?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(extract_correlated_subquery(
                expr, outer, catalog, current_db, index, found, ctx,
            )?),
            target: *target,
            not: *not,
        },
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(extract_correlated_subquery(
                lhs, outer, catalog, current_db, index, found, ctx,
            )?),
            Box::new(extract_correlated_subquery(
                rhs, outer, catalog, current_db, index, found, ctx,
            )?),
        ),
        other => other.clone(),
    })
}

/// The `SELECT` a subquery carries, rejecting the set-operation body.
fn subquery_select(query: &tidb_ast::QueryStmt) -> Result<&tidb_ast::SelectStmt, DriverError> {
    match query {
        tidb_ast::QueryStmt::Select(select) => Ok(select),
        _ => Err(DriverError::Unsupported(
            "set-operation subqueries are not supported yet",
        )),
    }
}

/// The scope a subquery inside `select` sees as its OUTER scope: `select`'s
/// own `FROM` tables. An unresolvable `FROM` yields an empty scope, and the
/// error surfaces when the query itself is built.
fn select_outer_scope(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> FromScope {
    match &select.from {
        None => FromScope::default(),
        Some(join) => match build_join(join, catalog, current_db, ctx) {
            Ok((_, scope)) => scope,
            Err(_) => FromScope::default(),
        },
    }
}

/// Whether any clause of `select` contains a subquery the folding pass should
/// run on. A correlated subquery in the `WHERE` is left for the Apply path.
fn select_has_uncorrelated_subquery(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    if let Some(where_clause) = &select.where_clause {
        if expr_has_subquery(where_clause) {
            let outer = select_outer_scope(select, catalog, current_db, ctx);
            let mut found = None;
            // A correlated WHERE subquery is the Apply path's job.
            if extract_correlated_subquery(
                where_clause,
                &outer,
                catalog,
                current_db,
                0,
                &mut found,
                ctx,
            )
            .is_ok()
                && found.is_some()
            {
                return false;
            }
        }
    }
    select_has_subquery(select)
}

/// Whether any clause of `select` contains a subquery, so the fold pass runs
/// only when it has something to do.
fn select_has_subquery(select: &tidb_ast::SelectStmt) -> bool {
    let fields = select.fields.fields().iter().any(|field| match field {
        SelectField::Expr { expr, .. } => expr_has_subquery(expr),
        SelectField::Wildcard(_) => false,
    });
    fields
        || select.where_clause.as_ref().is_some_and(expr_has_subquery)
        || select.having.as_ref().is_some_and(expr_has_subquery)
        || select
            .order_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
        || select
            .group_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
}

/// Whether `expr` contains a subquery in a position the fold pass walks.
fn expr_has_subquery(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. } => true,
        Expr::Paren(inner) | Expr::Unary(_, inner) | Expr::Is { expr: inner, .. } => {
            expr_has_subquery(inner)
        }
        Expr::Binary(_, lhs, rhs) => expr_has_subquery(lhs) || expr_has_subquery(rhs),
        Expr::In { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        _ => false,
    }
}

/// Folds every subquery in `select`'s clauses, returning the rewritten copy.
fn fold_select_subqueries(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut folded = select.clone();
    for field in folded.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = fold_subqueries(expr, outer, catalog, current_db, ctx)?;
        }
    }
    if let Some(where_clause) = &folded.where_clause {
        folded.where_clause = Some(fold_subqueries(
            where_clause,
            outer,
            catalog,
            current_db,
            ctx,
        )?);
    }
    if let Some(having) = &folded.having {
        folded.having = Some(fold_subqueries(having, outer, catalog, current_db, ctx)?);
    }
    for item in &mut folded.order_by {
        item.expr = fold_subqueries(&item.expr, outer, catalog, current_db, ctx)?;
    }
    for item in &mut folded.group_by {
        item.expr = fold_subqueries(&item.expr, outer, catalog, current_db, ctx)?;
    }
    Ok(folded)
}

/// Replaces every uncorrelated subquery in `expr` with the value it produces.
///
/// This is Go's `handleScalarSubquery` path for a subquery with no correlated
/// columns: the subquery is planned and run on the spot
/// (`EvalSubqueryFirstRow`) and its result folded into a `Constant`, so the
/// outer statement plans against ordinary literals. Go's `buildMaxOneRow`
/// wrapper is the "more than one row" check below; a subquery producing no
/// rows yields NULL.
///
/// `EXISTS` folds to 1 or 0, `x IN (subquery)` folds to `x IN (values)` and
/// `x <op> ANY|ALL (subquery)` to the OR/AND chain of comparisons, all of
/// which evaluate identically for an uncorrelated subquery -- including the
/// NULL rules, since the folded list is compared by the same code.
///
/// DEFERRED (documented): CORRELATED subqueries, which Go turns into an Apply
/// operator rather than folding, and which this leaves for the Apply path
/// rather than silently evaluating the inner query against the wrong row; and
/// row constructors (a subquery selecting several columns).
fn fold_subqueries(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // A subquery reading the OUTER query's columns has no single value to fold
    // to: it is the Apply path's job, one run per outer row.
    if let Expr::Subquery(query)
    | Expr::Exists {
        subquery: query, ..
    }
    | Expr::InSubquery {
        subquery: query, ..
    }
    | Expr::CompareSubquery {
        subquery: query, ..
    } = expr
    {
        if let tidb_ast::QueryStmt::Select(select) = &**query {
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if !columns.is_empty() {
                return Ok(expr.clone());
            }
        }
    }
    Ok(match expr {
        Expr::Subquery(query) => {
            let rows = run_subquery(query, catalog, current_db, ctx)?;
            match rows.len() {
                // Go: a scalar subquery with no rows is NULL.
                0 => Expr::Null,
                1 => {
                    let row = &rows[0];
                    let [value] = row.as_slice() else {
                        return Err(DriverError::Unsupported(
                            "a scalar subquery selecting several columns is not supported yet",
                        ));
                    };
                    datum_to_literal(value)?
                }
                // Go's buildMaxOneRow raises ER_SUBQUERY_NO_1_ROW here.
                _ => return Err(DriverError::SubqueryReturnsMoreThanOneRow),
            }
        }
        Expr::Exists { subquery, not } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let exists = !rows.is_empty();
            Expr::Int(i64::from(exists != *not).to_string())
        }
        Expr::InSubquery {
            expr,
            subquery,
            not,
        } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let list = subquery_value_list(
                &rows,
                "an IN subquery selecting several columns is not supported yet",
            )?;
            in_list_expr(
                fold_subqueries(expr, outer, catalog, current_db, ctx)?,
                list,
                *not,
            )
        }
        Expr::CompareSubquery {
            op,
            left,
            all,
            subquery,
        } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let list = subquery_value_list(
                &rows,
                "an ANY/ALL subquery selecting several columns is not supported yet",
            )?;
            any_all_expr(
                *op,
                fold_subqueries(left, outer, catalog, current_db, ctx)?,
                *all,
                list,
            )
        }
        // Walk the forms the expression rewriter itself supports; anything
        // else is returned unchanged and fails to rewrite as it already does.
        Expr::Paren(inner) => Expr::Paren(Box::new(fold_subqueries(
            inner, outer, catalog, current_db, ctx,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(fold_subqueries(inner, outer, catalog, current_db, ctx)?),
        ),
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(fold_subqueries(lhs, outer, catalog, current_db, ctx)?),
            Box::new(fold_subqueries(rhs, outer, catalog, current_db, ctx)?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(fold_subqueries(expr, outer, catalog, current_db, ctx)?),
            target: *target,
            not: *not,
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(fold_subqueries(expr, outer, catalog, current_db, ctx)?),
            list: list
                .iter()
                .map(|item| fold_subqueries(item, outer, catalog, current_db, ctx))
                .collect::<Result<_, _>>()?,
            not: *not,
        },
        other => other.clone(),
    })
}

/// Runs a subquery against the catalog, rejecting the correlated case.
///
/// A correlated subquery references a column of the OUTER query, which this
/// resolver cannot see -- so it fails to resolve here and the error surfaces
/// rather than the subquery being evaluated against the wrong scope.
fn run_subquery(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let tidb_ast::QueryStmt::Select(_) = query else {
        return Err(DriverError::Unsupported(
            "set-operation subqueries are not supported yet",
        ));
    };
    let tidb_ast::QueryStmt::Select(select) = query else {
        unreachable!("the set-operation case is rejected above")
    };
    run_select_stmt(select, catalog, current_db, ctx).map(|(_, rows)| rows)
}

/// Go turns a subquery's result `Datum` into an `expression.Constant`; the
/// same value has to travel back through the AST here, so it becomes the
/// literal that parses to it.
/// A byte string as a literal expression: readable text stays a string, and
/// anything that is not UTF-8 becomes a hex literal so no byte is lost.
pub(crate) fn bytes_to_literal(bytes: &[u8]) -> tidb_ast::Expr {
    match std::str::from_utf8(bytes) {
        Ok(text) => tidb_ast::Expr::String(text.to_owned()),
        Err(_) => tidb_ast::Expr::Hex(hex_digits(bytes)),
    }
}

/// The lowercase, even-length hex digits an `Expr::Hex` carries.
fn hex_digits(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn datum_to_literal(value: &Datum) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match value {
        Datum::Null => Expr::Null,
        Datum::Int(v) => {
            // A negative literal is a unary minus over a positive one, which
            // is how the parser itself represents it.
            if *v < 0 {
                Expr::Unary(
                    tidb_ast::UnaryOp::Minus,
                    Box::new(Expr::Int(v.unsigned_abs().to_string())),
                )
            } else {
                Expr::Int(v.to_string())
            }
        }
        Datum::UInt(v) => Expr::Int(v.to_string()),
        Datum::Real(v) => Expr::Float(*v),
        Datum::Decimal(d) => Expr::Decimal(d.to_string()),
        // A byte string that is not UTF-8 becomes a hex literal, which is
        // lossless where a lossy string conversion would corrupt it.
        Datum::String(s) => bytes_to_literal(s.bytes()),
        Datum::Bytes(b) => bytes_to_literal(b),
        Datum::BinaryLiteral(literal) | Datum::Bit(literal) => {
            Expr::Hex(hex_digits(literal.as_bytes()))
        }
        _ => {
            return Err(DriverError::Unsupported(
                "this subquery result kind is not supported yet",
            ))
        }
    })
}

/// Go `ranger`'s point pair for one comparison, before points become ranges.
///
/// Go builds a sorted point list and folds it into ranges; for the
/// single-column access this covers, each comparison yields exactly one range,
/// which is that fold already applied.
fn range_for_comparison(op: tidb_ast::BinaryOp, value: Datum) -> Option<IndexRange> {
    use tidb_ast::BinaryOp;
    // Go's builder starts every ordinary comparison at MinNotNull rather than
    // at NULL, which is what makes a NULL value satisfy no comparison.
    Some(match op {
        BinaryOp::Eq => IndexRange {
            low: vec![value.clone()],
            high: vec![value],
            low_exclusive: false,
            high_exclusive: false,
        },
        BinaryOp::Lt => IndexRange {
            low: vec![Datum::MinNotNull],
            high: vec![value],
            low_exclusive: false,
            high_exclusive: true,
        },
        BinaryOp::Le => IndexRange {
            low: vec![Datum::MinNotNull],
            high: vec![value],
            low_exclusive: false,
            high_exclusive: false,
        },
        BinaryOp::Gt => IndexRange {
            low: vec![value],
            high: vec![Datum::MaxValue],
            low_exclusive: true,
            high_exclusive: false,
        },
        BinaryOp::Ge => IndexRange {
            low: vec![value],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        },
        _ => return None,
    })
}

/// The index access path for a `WHERE`, when one applies.
///
/// Go's `DetachCondAndBuildRangeForIndex` splits a predicate into access
/// conditions, which become index ranges, and filter conditions, which stay
/// above the read. This builds ranges for the conditions on one index's
/// leading column and leaves the whole `WHERE` in the pipeline, so the filter
/// half is applied by the selection rather than dropped.
///
/// DEFERRED (documented): multi-column ranges (Go extends the prefix while
/// equalities pin leading columns), `IN` lists and `BETWEEN` as ranges, `OR`
/// unions across ranges, and cost-based choice among several usable indexes --
/// this takes the first index whose leading column the `WHERE` constrains.
pub(crate) fn try_index_ranges(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Option<(i64, Vec<IndexRange>)> {
    let where_clause = select.where_clause.as_ref()?;
    let mut conditions = Vec::new();
    collect_conjuncts(where_clause, &mut conditions);

    for index in table.indexes() {
        // Only the leading column can be constrained without the multi-column
        // range builder.
        let leading = &columns[*index.column_offsets.first()?].0;
        let mut ranges: Vec<IndexRange> = Vec::new();
        for condition in &conditions {
            let tidb_ast::Expr::Binary(op, lhs, rhs) = condition else {
                continue;
            };
            // Go accepts the constant on either side, flipping the operator
            // when the column is on the right.
            let (op, value) = match (&**lhs, &**rhs) {
                (tidb_ast::Expr::Column(path), other)
                    if path
                        .last()
                        .is_some_and(|name| name.eq_ignore_ascii_case(leading)) =>
                {
                    (*op, other)
                }
                (other, tidb_ast::Expr::Column(path))
                    if path
                        .last()
                        .is_some_and(|name| name.eq_ignore_ascii_case(leading)) =>
                {
                    (flip_comparison(*op)?, other)
                }
                _ => continue,
            };
            let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(value, &NoResolver)
            else {
                continue;
            };
            let Ok(value) = constant.eval() else {
                continue;
            };
            // A NULL constant makes every comparison unknown, so no row
            // qualifies; Go represents that as an empty range set.
            if value == Datum::Null {
                return Some((index.id, Vec::new()));
            }
            if let Some(range) = range_for_comparison(op, value) {
                ranges.push(range);
            }
        }
        if ranges.is_empty() {
            continue;
        }
        // Several conditions on the same column intersect.
        let combined = ranges
            .into_iter()
            .reduce(intersect_ranges)
            .expect("at least one range");
        return Some((index.id, vec![combined]));
    }
    None
}

/// Flattens an `AND` chain into its conjuncts.
fn collect_conjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_conjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) => {
            collect_conjuncts(lhs, out);
            collect_conjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// The operator with its operands swapped, so `5 < a` reads as `a > 5`.
fn flip_comparison(op: tidb_ast::BinaryOp) -> Option<tidb_ast::BinaryOp> {
    use tidb_ast::BinaryOp;
    Some(match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Le => BinaryOp::Ge,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Ge => BinaryOp::Le,
        _ => return None,
    })
}

/// The intersection of two ranges over the same column, which is what several
/// conditions on that column mean together.
fn intersect_ranges(left: IndexRange, right: IndexRange) -> IndexRange {
    let (low, low_exclusive) = match compare_bounds(&left.low, &right.low) {
        std::cmp::Ordering::Greater => (left.low, left.low_exclusive),
        std::cmp::Ordering::Less => (right.low, right.low_exclusive),
        // Equal bounds: the exclusive one wins, being the tighter.
        std::cmp::Ordering::Equal => (left.low, left.low_exclusive || right.low_exclusive),
    };
    let (high, high_exclusive) = match compare_bounds(&left.high, &right.high) {
        std::cmp::Ordering::Less => (left.high, left.high_exclusive),
        std::cmp::Ordering::Greater => (right.high, right.high_exclusive),
        std::cmp::Ordering::Equal => (left.high, left.high_exclusive || right.high_exclusive),
    };
    IndexRange {
        low,
        high,
        low_exclusive,
        high_exclusive,
    }
}

/// Orders two single-datum bounds, with `MinNotNull` below and `MaxValue`
/// above every ordinary value.
fn compare_bounds(left: &[Datum], right: &[Datum]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (Some(left), Some(right)) = (left.first(), right.first()) else {
        return Ordering::Equal;
    };
    let rank = |value: &Datum| match value {
        Datum::MinNotNull => 0,
        Datum::MaxValue => 2,
        _ => 1,
    };
    match rank(left).cmp(&rank(right)) {
        Ordering::Equal => {}
        other => return other,
    }
    if rank(left) != 1 {
        return Ordering::Equal;
    }
    tidb_expr::compare_datums(left, right).unwrap_or(Ordering::Equal)
}

/// The single TiKV-backed table a `FROM` names, when it names exactly one.
/// A point get applies only to that shape (Go `getSingleTableNameAndAlias`).
pub(crate) fn single_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let join = from.as_ref()?;
    if join.right.is_some() {
        return None;
    }
    let JoinNode::Table(table_ref) = &join.left else {
        return None;
    };
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        // A view stores no rows, so there is no point get to try.
        TableEntry::Mem(_) | TableEntry::View(_) => None,
    }
}

/// Go `tryWhereIn2BatchPointGet`: a single-table `SELECT` whose whole `WHERE`
/// is `column IN (constants)` over the handle or a single-column unique index
/// reads those rows directly instead of scanning.
///
/// Go rejects the fast plan when `ORDER BY`, `GROUP BY`, `LIMIT`, `HAVING`,
/// `DISTINCT` or a window spec is present, when the `IN` is negated, and when
/// its list is empty. The handle path applies when the table's primary key IS
/// the handle and the column names it; otherwise a unique index whose only
/// column it is.
///
/// DEFERRED (documented): Go's row form, `(a, b) IN ((1, 2), (3, 4))`, which
/// needs multi-column key lookup.
pub(crate) fn try_batch_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Result<Option<Vec<TableHandle>>, DriverError> {
    if select.having.is_some()
        || !select.order_by.is_empty()
        || !select.group_by.is_empty()
        || select.limit.is_some()
        || select.distinct
    {
        return Ok(None);
    }
    let Some(where_clause) = &select.where_clause else {
        return Ok(None);
    };
    // The WHERE must be exactly the IN, as Go requires a PatternInExpr.
    let tidb_ast::Expr::In { expr, list, not } = where_clause else {
        return Ok(None);
    };
    if *not || list.is_empty() {
        return Ok(None);
    }
    let tidb_ast::Expr::Column(path) = &**expr else {
        return Ok(None);
    };
    let Some(name) = path.last() else {
        return Ok(None);
    };

    // Every list element must be a constant, or this is not a point plan.
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(item, &NoResolver) else {
            return Ok(None);
        };
        let Ok(value) = constant.eval() else {
            return Ok(None);
        };
        values.push(value);
    }

    // The handle path.
    if let Some(offset) = table.pk_handle_offset() {
        if columns[offset].0.eq_ignore_ascii_case(name) {
            let mut handles = Vec::with_capacity(values.len());
            for value in &values {
                match value {
                    Datum::Int(v) => handles.push(TableHandle::Int(*v)),
                    Datum::UInt(v) => handles.push(TableHandle::Int(*v as i64)),
                    // A non-integer constant names no integer handle, so it
                    // simply matches nothing.
                    _ => {}
                }
            }
            return Ok(Some(handles));
        }
    }

    // The unique-index path.
    let mut table = table.clone();
    for index in table.indexes().to_vec() {
        if !index.unique || index.column_offsets.len() != 1 {
            continue;
        }
        if !columns[index.column_offsets[0]]
            .0
            .eq_ignore_ascii_case(name)
        {
            continue;
        }
        let mut handles = Vec::new();
        for value in &values {
            if let Some(handle) = table
                .lookup_unique(index.id, std::slice::from_ref(value))
                .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?
            {
                handles.push(handle);
            }
        }
        return Ok(Some(handles));
    }
    Ok(None)
}

/// One `column = constant` equality from a `WHERE`, Go's `nameValuePair`.
struct NameValuePair {
    column: String,
    value: Datum,
}

/// Go `getNameValuePairs`: flattens a `WHERE` that is a conjunction of
/// `column = constant` equalities into pairs, returning `None` for any other
/// shape.
///
/// Go accepts the constant on either side of the `=`, and recurses only
/// through `AND`; anything else (an `OR`, a comparison, a function call)
/// makes the statement ineligible for a point get, which is what returning
/// `None` means here.
fn name_value_pairs(expr: &tidb_ast::Expr, pairs: &mut Vec<NameValuePair>) -> bool {
    use tidb_ast::{BinaryOp, Expr};
    match expr {
        Expr::Paren(inner) => name_value_pairs(inner, pairs),
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            name_value_pairs(lhs, pairs) && name_value_pairs(rhs, pairs)
        }
        Expr::Binary(BinaryOp::Eq, lhs, rhs) => {
            let (column, value) = match (&**lhs, &**rhs) {
                (Expr::Column(path), other) => (path, other),
                (other, Expr::Column(path)) => (path, other),
                _ => return false,
            };
            let Some(name) = column.last() else {
                return false;
            };
            // Only a literal qualifies; anything needing evaluation against a
            // row is not a point-get key.
            let Ok(value) = rewrite_expr_resolved(value, &NoResolver) else {
                return false;
            };
            let Expression::Constant(constant) = value else {
                return false;
            };
            let Ok(value) = constant.eval() else {
                return false;
            };
            pairs.push(NameValuePair {
                column: name.clone(),
                value,
            });
            true
        }
        _ => false,
    }
}

/// The row a point get reads, when the statement qualifies for one.
///
/// Go `TryFastPlan`/`tryPointGetPlan`: a single-table `SELECT` with no
/// `HAVING` and no `ORDER BY`, whose `WHERE` is a conjunction of equalities
/// that pins either the handle or every column of a unique index, reads one
/// row directly instead of scanning. `LIMIT` is allowed only when it cannot
/// remove the row (`count > 0` and `offset == 0`), matching Go's check.
///
/// Returns `Ok(None)` when the statement does not qualify, so the caller
/// falls back to the ordinary scan.
pub(crate) fn try_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Result<Option<Option<TableHandle>>, DriverError> {
    if select.having.is_some() || !select.order_by.is_empty() || !select.group_by.is_empty() {
        return Ok(None);
    }
    if let Some(limit) = &select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        if count == 0 || offset > 0 {
            return Ok(None);
        }
    }
    let Some(where_clause) = &select.where_clause else {
        return Ok(None);
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs) || pairs.is_empty() {
        return Ok(None);
    }

    // The handle path: the primary key pinned by exactly one equality, which
    // is Go's `len(pairs) == 1` condition on the handle pair.
    if let Some(handle_offset) = table.pk_handle_offset() {
        let handle_column = &columns[handle_offset].0;
        if pairs.len() == 1 && pairs[0].column.eq_ignore_ascii_case(handle_column) {
            return Ok(Some(match &pairs[0].value {
                Datum::Int(value) => Some(TableHandle::Int(*value)),
                Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                // A non-integer constant cannot name an integer handle, so no
                // row matches rather than the plan being wrong.
                _ => None,
            }));
        }
    }

    // The unique-index path: every column of some unique index is pinned.
    let mut table = table.clone();
    for index in table.indexes().to_vec() {
        if !index.unique {
            continue;
        }
        let mut values = Vec::with_capacity(index.column_offsets.len());
        for offset in &index.column_offsets {
            let name = &columns[*offset].0;
            let Some(pair) = pairs
                .iter()
                .find(|pair| pair.column.eq_ignore_ascii_case(name))
            else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
        }
        if values.len() != index.column_offsets.len() {
            continue;
        }
        let handle = table
            .lookup_unique(index.id, &values)
            .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?;
        return Ok(Some(handle));
    }
    Ok(None)
}

/// One table in a query's `FROM`: the name a qualifier must match (its alias
/// when it has one, as in Go's `TableSource`), its columns, and the offset of
/// its first column in the joined row.
#[derive(Clone, Debug)]
pub(crate) struct FromTable {
    pub(crate) name: String,
    /// The schema the table lives in, when a `db.t.column` reference may name
    /// it. `None` for a source that cannot be schema-qualified: an aliased
    /// table (MySQL's alias replaces the whole path) or a synthetic scope.
    pub(crate) database: Option<String>,
    pub(crate) columns: Vec<(String, FieldType)>,
    pub(crate) offset: usize,
}

/// The joined `FROM` scope: every table's columns concatenated left to right,
/// which is the row layout [`JoinExec`] produces.
#[derive(Clone, Debug, Default)]
pub(crate) struct FromScope {
    pub(crate) tables: Vec<FromTable>,
}

impl FromScope {
    /// Every column of the scope in row order.
    pub(crate) fn column_list(&self) -> Vec<(String, FieldType)> {
        self.tables
            .iter()
            .flat_map(|t| t.columns.iter().cloned())
            .collect()
    }

    pub(crate) fn width(&self) -> usize {
        self.tables.iter().map(|t| t.columns.len()).sum()
    }
}

/// Resolves a column reference against the joined `FROM` scope.
///
/// A qualified `t.a` binds to table `t`'s column; an unqualified `a` binds to
/// the one table that has such a column, and is rejected as ambiguous when
/// several do -- MySQL's `ERROR 1052 (23000): Column 'a' in field list is
/// ambiguous`, which Go raises from `expression.buildColumn`.
struct ScopeResolver<'a> {
    scope: &'a FromScope,
}

/// A resolver over `scope`, for the modules that build their own expressions.
pub(crate) fn scope_resolver(scope: &FromScope) -> impl ColumnResolver + '_ {
    ScopeResolver { scope }
}

impl ColumnResolver for ScopeResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (schema, qualifier, name) = match path {
            [name] => (None, None, name),
            [table, name] => (None, Some(table), name),
            // `db.t.a` is how a view's stored definition names its columns.
            [schema, table, name] => (Some(schema), Some(table), name),
            _ => return None,
        };
        let mut found: Option<(usize, FieldType)> = None;
        for table in &self.scope.tables {
            if let Some(q) = qualifier {
                if !q.eq_ignore_ascii_case(&table.name) {
                    continue;
                }
            }
            if let Some(schema) = schema {
                // An aliased or synthetic source carries no schema, so a
                // schema-qualified reference cannot name it.
                match &table.database {
                    Some(db) if db.eq_ignore_ascii_case(schema) => {}
                    _ => continue,
                }
            }
            for (i, (candidate, ft)) in table.columns.iter().enumerate() {
                if candidate.eq_ignore_ascii_case(name) {
                    if found.is_some() {
                        // Ambiguous across tables: MySQL errors rather than
                        // picking one.
                        return None;
                    }
                    found = Some((table.offset + i, ft.clone()));
                }
            }
        }
        let (index, ft) = found?;
        Some((index, ft, (index + 1) as i64))
    }
}

/// Builds the `FROM` scope and the executor that produces its rows.
///
/// Go's `buildJoin` builds a left-deep tree of `LogicalJoin`s over the
/// `FROM` list; this walks the same tree, so `a JOIN b JOIN c` nests as
/// `(a JOIN b) JOIN c` and the row layout is `a`'s columns, then `b`'s, then
/// `c`'s.
///
/// DEFERRED (documented): derived tables, `USING`, `NATURAL`, and
/// `STRAIGHT_JOIN`'s ordering guarantee.
fn build_from(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    match node {
        JoinNode::Table(table_ref) => {
            // A `db.t` reference resolves in that schema; a bare `t` resolves
            // in the session's current one (Go's name resolution).
            let (database, name) = split_table_path(&table_ref.name, current_db)?;
            let entry = catalog
                .get_in(database, name)
                .ok_or(DriverError::Unsupported("table not found in catalog"))?;
            // A table alias replaces the name for qualification, as in Go.
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            if let TableEntry::View(view) = entry {
                return build_view_source(
                    view,
                    database,
                    name,
                    visible,
                    table_ref.alias.is_none(),
                    catalog,
                    ctx,
                );
            }
            let columns = entry.column_list();
            let schema_columns: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let schema = Schema::new(schema_columns);
            let exec: Box<dyn Executor> = match entry {
                TableEntry::Mem(mem) => Box::new(MemTableSourceExec::new(
                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                    mem.rows.clone(),
                )),
                TableEntry::Kv(kv) => Box::new(TableScanExec::new(
                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                    kv.clone(),
                )),
                // Handled above, before the columns were taken.
                TableEntry::View(_) => unreachable!("views take the branch above"),
            };
            let scope = FromScope {
                tables: vec![FromTable {
                    name: visible,
                    // An alias replaces the whole path, so `db.t.col` no
                    // longer names the table once it is aliased.
                    database: table_ref.alias.is_none().then(|| database.to_owned()),
                    columns,
                    offset: 0,
                }],
            };
            Ok((exec, scope))
        }
        JoinNode::Join(join) => build_join(join, catalog, current_db, ctx),
        JoinNode::Derived { .. } => Err(DriverError::Unsupported(
            "derived tables are not supported yet",
        )),
    }
}

/// How deep a view may nest before the reference is called invalid. A view
/// whose body reads itself (which `CREATE OR REPLACE` can build) would
/// otherwise recurse forever.
///
/// DIVERGENCE (documented): MySQL caps nesting at 61 and reports
/// `ER_VIEW_RECURSIVE` (1462); this reports `ErrViewInvalid` (1356), the same
/// error the other broken-view cases report.
const MAX_VIEW_DEPTH: usize = 32;

thread_local! {
    /// How many view bodies the current statement is inside.
    static VIEW_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Decrements the view-nesting depth however the body's evaluation ends.
struct ViewDepthGuard;

impl ViewDepthGuard {
    /// Enters one view body, refusing to go past [`MAX_VIEW_DEPTH`].
    fn enter(qualified: &str) -> Result<ViewDepthGuard, DriverError> {
        VIEW_DEPTH.with(|depth| {
            if depth.get() >= MAX_VIEW_DEPTH {
                return Err(DriverError::Schema(SchemaErrorKind::ViewInvalid(
                    qualified.to_owned(),
                )));
            }
            depth.set(depth.get() + 1);
            Ok(ViewDepthGuard)
        })
    }
}

impl Drop for ViewDepthGuard {
    fn drop(&mut self) {
        VIEW_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

/// Runs a view's stored `SELECT` and presents its rows as a `FROM` source.
///
/// Go rewrites the reference into a derived table over the view's plan; the
/// rows here are materialized instead, which is the same result for a reader
/// (the outer `WHERE`, joins and `ORDER BY` all apply to the view's output
/// either way) and differs only in that nothing is pushed into the view.
///
/// The body's own failure is Go's `ErrViewInvalid`: the definition ran once
/// already, when the view was created, so anything that stops it running now
/// is a schema change underneath it.
fn build_view_source(
    view: &ViewDef,
    database: &str,
    name: &str,
    visible: String,
    alias_free: bool,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    let qualified = format!("{database}.{name}");
    let _guard = ViewDepthGuard::enter(&qualified)?;
    let invalid = || DriverError::Schema(SchemaErrorKind::ViewInvalid(qualified.clone()));
    // The definition is stored schema-qualified, so it resolves in the view's
    // own schema rather than the reader's.
    let (body_columns, rows) =
        run_select_meta_in(&view.select_sql, catalog, database, ctx).map_err(|_| invalid())?;
    if body_columns.len() != view.columns.len() {
        return Err(invalid());
    }
    // The view's own column names win over the body's, which is what a
    // `CREATE VIEW v (a2) AS SELECT a ...` column list means.
    let columns: Vec<(String, FieldType)> = view
        .columns
        .iter()
        .zip(&body_columns)
        .map(|((name, _), (_, ft))| (name.clone(), ft.clone()))
        .collect();
    let schema_columns: Vec<Column> = columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let exec: Box<dyn Executor> = Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
        rows,
    ));
    let scope = FromScope {
        tables: vec![FromTable {
            name: visible,
            database: alias_free.then(|| database.to_owned()),
            columns,
            offset: 0,
        }],
    };
    Ok((exec, scope))
}

/// Builds one join node (or passes through the single-table wrapper).
fn build_join(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    let (left_exec, left_scope) = build_from(&join.left, catalog, current_db, ctx)?;
    let Some(right_node) = &join.right else {
        // The single-table wrapper the parser always produces.
        return Ok((left_exec, left_scope));
    };
    if join.natural || !join.using.is_empty() {
        return Err(DriverError::Unsupported(
            "NATURAL and USING joins are not supported yet",
        ));
    }
    let (right_exec, right_scope) = build_from(right_node, catalog, current_db, ctx)?;

    // The joined scope: the right tables' columns follow the left's.
    let left_width = left_scope.width();
    let mut scope = left_scope;
    for table in right_scope.tables {
        scope.tables.push(FromTable {
            name: table.name,
            database: table.database,
            columns: table.columns,
            offset: table.offset + left_width,
        });
    }

    let column_list = scope.column_list();
    let schema_columns: Vec<Column> = column_list
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let meta = ExecutorMeta::new(Schema::new(schema_columns), 6, INIT_CAP, MAX_CHUNK_SIZE);

    let conditions = match &join.on {
        Some(expr) => {
            let resolver = ScopeResolver { scope: &scope };
            vec![rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?]
        }
        None => Vec::new(),
    };
    let kind = match join.tp {
        tidb_ast::JoinType::Cross => JoinKind::Inner,
        tidb_ast::JoinType::Left => JoinKind::Left,
        tidb_ast::JoinType::Right => JoinKind::Right,
    };
    let exec: Box<dyn Executor> = Box::new(JoinExec::new(
        meta,
        kind,
        conditions,
        left_exec,
        right_exec,
        ctx.clone(),
    ));
    Ok((exec, scope))
}

/// The table a single-table `UPDATE`/`DELETE` targets.
pub(crate) fn single_table_name(
    table_ref: &tidb_ast::TableRef,
    current_db: &str,
) -> Result<(String, String), DriverError> {
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    Ok((database.to_owned(), name.to_owned()))
}

/// The value an omitted column takes, following Go `GetColDefaultValue` and
/// `getColDefaultValueFromNil`: the stored `DEFAULT` when one was written, or
/// NULL for a nullable column; a NOT NULL column with no default is Go's
/// `ErrNoDefaultForField` under strict mode.
///
/// DEFERRED (documented): non-strict mode, where Go warns and writes the
/// type's zero value instead of failing. This seed always behaves as strict
/// mode, which is TiDB's default sql_mode.
fn column_default(
    meta: &[(Option<Datum>, bool, String)],
    offset: usize,
) -> Result<Datum, DriverError> {
    let (default_value, not_null, name) = &meta[offset];
    match default_value {
        Some(value) => Ok(value.clone()),
        None if *not_null => Err(DriverError::NoDefaultForField(name.clone())),
        None => Ok(Datum::Null),
    }
}

/// Whether the column at `offset` carries Go's `NotNullFlag`.
fn column_is_not_null(meta: &[(Option<Datum>, bool, String)], offset: usize) -> bool {
    meta[offset].1
}

/// Runs a single-table `UPDATE`, returning MySQL's affected-row count.
///
/// Go `executor.UpdateExec` + `updateRecord`: each row the `WHERE` selects is
/// re-evaluated with the `SET` assignments applied, and a row is written back
/// only when a column actually changed. The affected-row count is the number
/// of CHANGED rows, not the number matched -- an unchanged row is "touched"
/// instead, and only a client that negotiated `CLIENT_FOUND_ROWS` sees it
/// counted (that capability is not modelled here, so the count is always the
/// changed-row count).
///
/// Assignments are evaluated against the row's ORIGINAL values, left to right,
/// with each assignment seeing the effects of the previous ones -- Go's
/// `composeNewRow` order.
///
/// DEFERRED (documented): multi-table UPDATE, `ORDER BY`/`LIMIT` tails,
/// `IGNORE`, generated and `ON UPDATE CURRENT_TIMESTAMP` columns,
/// and the handle-changed path (a row whose primary-key handle column is
/// assigned is deleted and re-inserted in Go; this seed rejects it).
pub fn run_update_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_update_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_update_on`] resolving unqualified names in `current_db`.
pub fn run_update_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let update = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Update(update) => update,
            _ => return Err(DriverError::Unsupported("only UPDATE is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only UPDATE is supported here")),
    };
    // A `RETURNING` clause is parsed and silently ignored, matching Go: the
    // planner and executor never read `UpdateStmt.Returning`.
    if update.ignore {
        return Err(DriverError::Unsupported(
            "UPDATE IGNORE is not supported yet",
        ));
    }
    let table_ref = match &update.kind {
        tidb_ast::UpdateKind::Single(table_ref) => table_ref,
        tidb_ast::UpdateKind::Multi { .. } => {
            return Err(DriverError::Unsupported(
                "multi-table UPDATE is not supported yet",
            ))
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let column_list = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?
        .column_list();

    // SET targets, as offsets into the row.
    let mut assignments = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let column = assignment
            .col
            .last()
            .ok_or(DriverError::Unsupported("empty assignment target"))?;
        let offset = column_list
            .iter()
            .position(|(candidate, _)| candidate.eq_ignore_ascii_case(column))
            .ok_or(DriverError::Unsupported("unknown column in SET"))?;
        assignments.push((offset, assignment.value.clone()));
    }

    let resolver = TableResolver {
        table_name: &name,
        columns: &column_list,
    };
    let predicate = match &update.where_clause {
        Some(expr) => Some(
            rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ),
        None => None,
    };
    let mut set_exprs = Vec::with_capacity(assignments.len());
    for (offset, value) in &assignments {
        set_exprs.push((
            *offset,
            rewrite_expr_resolved(value, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ));
    }

    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let column_names: Vec<String> = column_list.iter().map(|(name, _)| name.clone()).collect();
    let row_limit = dml_row_limit(&update.limit)?;
    let entry = catalog
        .get_mut_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?;

    let mut changed = 0u64;
    match entry {
        // Go's planner rejects an UPDATE whose target is a view.
        TableEntry::View(_) => return Err(DriverError::TableNotUpdatable(name.clone())),
        TableEntry::Mem(mem) => {
            let mut updates = Vec::new();
            for (index, row) in mem.rows.iter().enumerate() {
                if let Some(new_row) = compute_updated_row(
                    row,
                    &field_types,
                    &column_names,
                    &predicate,
                    &set_exprs,
                    ctx,
                )? {
                    updates.push((index, new_row));
                }
            }
            changed = updates.len() as u64;
            for (index, new_row) in updates {
                mem.rows[index] = new_row;
            }
        }
        TableEntry::Kv(kv) => {
            let mut rows = kv
                .scan_rows_with_handles()
                .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
            order_rows_for_dml(&mut rows, &update.order_by, &field_types, &resolver, ctx)?;
            for (handle, row) in rows {
                if row_limit.is_some_and(|cap| changed >= cap) {
                    break;
                }
                if let Some(new_row) = compute_updated_row(
                    &row,
                    &field_types,
                    &column_names,
                    &predicate,
                    &set_exprs,
                    ctx,
                )? {
                    kv.update_row(&handle, &new_row).map_err(|e| match e {
                        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                            DriverError::DuplicateEntry { value, key }
                        }
                        other => DriverError::Parse(format!("row encode failed: {other:?}")),
                    })?;
                    changed += 1;
                }
            }
        }
    }
    Ok(changed)
}

/// Applies the `SET` assignments to one row, returning the new row only when
/// the `WHERE` selected it AND a column actually changed (Go's `changed` flag).
fn compute_updated_row(
    row: &[Datum],
    field_types: &[FieldType],
    column_names: &[String],
    predicate: &Option<Expression>,
    set_exprs: &[(usize, Expression)],
    ctx: &crate::StmtContext,
) -> Result<Option<Vec<Datum>>, DriverError> {
    let chunk = row_chunk(row, field_types)?;
    if let Some(predicate) = predicate {
        let selected = predicate
            .eval(ctx, chunk.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        if !datum_is_true(&selected) {
            return Ok(None);
        }
    }
    let mut new_row = row.to_vec();
    for (offset, expr) in set_exprs {
        // Go evaluates each assignment over the row as the previous
        // assignments left it, so `SET a = 1, b = a` sees the new `a`.
        let source = row_chunk(&new_row, field_types)?;
        let value = expr
            .eval(ctx, source.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        // Go casts an assigned value to its column's type here too, which is
        // what stores `SET d = 9.87654` in a DECIMAL(10,3) column as 9.877.
        new_row[*offset] =
            cast_value_for_column(value, &field_types[*offset], &column_names[*offset], 0, ctx)?;
    }
    if new_row == row {
        // Go counts this row as touched, not affected.
        return Ok(None);
    }
    Ok(Some(new_row))
}

/// Runs a single-table `DELETE`, returning the number of removed rows.
///
/// Go `executor.DeleteExec`: every row the `WHERE` selects is removed, and the
/// affected-row count is simply that count.
///
/// DEFERRED (documented): multi-table DELETE, `ORDER BY`/`LIMIT` tails,
/// `IGNORE`. A `RETURNING` clause is parsed and silently ignored, matching
/// Go, where the planner and executor never read `DeleteStmt.Returning`.
pub fn run_delete_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_delete_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_delete_on`] resolving unqualified names in `current_db`.
pub fn run_delete_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let delete = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Delete(delete) => delete,
            _ => return Err(DriverError::Unsupported("only DELETE is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only DELETE is supported here")),
    };
    if delete.ignore || delete.quick {
        return Err(DriverError::Unsupported(
            "only plain DELETE FROM t [WHERE ...] is supported",
        ));
    }
    let table_ref = match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => table_ref,
        tidb_ast::DeleteKind::Multi { .. } => {
            return Err(DriverError::Unsupported(
                "multi-table DELETE is not supported yet",
            ))
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let column_list = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?
        .column_list();
    let resolver = TableResolver {
        table_name: &name,
        columns: &column_list,
    };
    let predicate = match &delete.where_clause {
        Some(expr) => Some(
            rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ),
        None => None,
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let row_limit = dml_row_limit(&delete.limit)?;
    let entry = catalog
        .get_mut_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?;

    let mut deleted = 0u64;
    match entry {
        TableEntry::View(_) => return Err(DriverError::DeleteViewUnsupported(name.clone())),
        TableEntry::Mem(mem) => {
            let mut kept = Vec::with_capacity(mem.rows.len());
            for row in std::mem::take(&mut mem.rows) {
                if row_is_selected(&row, &field_types, &predicate, ctx)? {
                    deleted += 1;
                } else {
                    kept.push(row);
                }
            }
            mem.rows = kept;
        }
        TableEntry::Kv(kv) => {
            let mut rows = kv
                .scan_rows_with_handles()
                .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
            order_rows_for_dml(&mut rows, &delete.order_by, &field_types, &resolver, ctx)?;
            for (handle, row) in rows {
                // Go's LIMIT caps the rows DELETED, not the rows examined.
                if row_limit.is_some_and(|cap| deleted >= cap) {
                    break;
                }
                if row_is_selected(&row, &field_types, &predicate, ctx)? {
                    kv.delete_row(&handle)
                        .map_err(|e| DriverError::Parse(format!("row delete failed: {e:?}")))?;
                    deleted += 1;
                }
            }
        }
    }
    Ok(deleted)
}

/// Whether the `WHERE` predicate (absent = every row) selects this row.
fn row_is_selected(
    row: &[Datum],
    field_types: &[FieldType],
    predicate: &Option<Expression>,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    let chunk = row_chunk(row, field_types)?;
    let selected = predicate
        .eval(ctx, chunk.get_row(0))
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    Ok(datum_is_true(&selected))
}

/// A one-row chunk holding `row`, so an expression can be evaluated over it.
pub(crate) fn row_chunk(
    row: &[Datum],
    field_types: &[FieldType],
) -> Result<tidb_chunk::chunk::Chunk, DriverError> {
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(field_types, 1);
    for (i, value) in row.iter().enumerate() {
        chunk.append_datum(i, value);
    }
    Ok(chunk)
}

/// Go's `WHERE` truth test: NULL and zero are false.
fn datum_is_true(value: &Datum) -> bool {
    match value {
        Datum::Null => false,
        Datum::Int(v) => *v != 0,
        Datum::UInt(v) => *v != 0,
        Datum::Real(v) => *v != 0.0,
        other => !matches!(other, Datum::Null),
    }
}

/// One `GROUPING(c1, ..., cn)` call hoisted into an aggregation output column.
///
/// Go computes `GROUPING` from the `gid` column Expand attaches to every
/// replicated row; this seed's rollup runs one aggregation pass per grouping
/// set, so the pass itself already knows which columns are rolled up and the
/// bitmask is filled straight into the output row.
#[derive(Clone, Debug)]
struct GroupingSpec {
    /// The aggregation output column this call's value is written into.
    out_index: usize,
    /// Each argument's position in the `GROUP BY` list, in argument order.
    /// The LEFTMOST argument owns the HIGHEST bit (captured from real TiDB:
    /// with `GROUP BY a, b WITH ROLLUP`, the `b`-only subtotal row reports
    /// `GROUPING(a,b) = 1` and `GROUPING(b,a) = 2`).
    group_positions: Vec<usize>,
}

impl GroupingSpec {
    /// The bitmask this call reports for a pass that groups by the first `k`
    /// `GROUP BY` expressions, i.e. one where positions `k..` are rolled up.
    fn mask_for_prefix(&self, k: usize) -> u64 {
        let width = self.group_positions.len();
        self.group_positions
            .iter()
            .enumerate()
            .filter(|(_, &position)| position >= k)
            .map(|(arg, _)| 1u64 << (width - 1 - arg))
            .sum()
    }
}

/// The `GROUPING(...)` arguments when `expr` IS such a call, else `None`.
fn grouping_call_args(expr: &tidb_ast::Expr) -> Option<&[tidb_ast::Expr]> {
    match expr {
        tidb_ast::Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("grouping") => {
            Some(args)
        }
        _ => None,
    }
}

/// Whether `expr` mentions `GROUPING()` anywhere the aggregate path can reach
/// it. The recursion covers the same shapes [`substitute_aggregates`] walks;
/// a `GROUPING` buried in a shape neither one descends into is not detected
/// and simply evaluates as an unknown function, as it does today.
fn expr_has_grouping(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    if grouping_call_args(expr).is_some() {
        return true;
    }
    match expr {
        Expr::Paren(inner) | Expr::Unary(_, inner) => expr_has_grouping(inner),
        Expr::Binary(_, lhs, rhs) => expr_has_grouping(lhs) || expr_has_grouping(rhs),
        Expr::Func { args, .. } => args.iter().any(expr_has_grouping),
        _ => false,
    }
}

/// Whether the statement writes `GROUPING()` in any clause the aggregate path
/// evaluates.
fn select_has_grouping(select: &tidb_ast::SelectStmt) -> bool {
    select.fields.fields().iter().any(|field| match field {
        SelectField::Expr { expr, .. } => expr_has_grouping(expr),
        SelectField::Wildcard { .. } => false,
    }) || select.having.as_ref().is_some_and(expr_has_grouping)
        || select
            .order_by
            .iter()
            .any(|item| expr_has_grouping(&item.expr))
}

/// The output type Go gives a `GROUPING()` column: `BIGINT UNSIGNED`, flen 20,
/// with the binary flag (captured from real TiDB: `tp=8 flag=160 flen=20`).
fn grouping_result_type() -> FieldType {
    let mut ftype = FieldType::new(FieldTypeCode::LongLong);
    ftype.add_flags(FieldTypeFlags::UNSIGNED | FieldTypeFlags::BINARY);
    ftype.set_flen(20);
    ftype
}

/// Resolves each `GROUPING()` argument to its position in the `GROUP BY` list.
///
/// Go rejects an argument that is not grouped with `ErrFieldInGroupingNotGroupBy`
/// (3602), naming the argument's 0-based position.
fn grouping_arg_positions(
    args: &[tidb_ast::Expr],
    group_by_names: &[String],
) -> Result<Vec<usize>, DriverError> {
    let mut positions = Vec::with_capacity(args.len());
    for (arg, expr) in args.iter().enumerate() {
        let tidb_ast::Expr::Column(path) = expr else {
            return Err(DriverError::FieldInGroupingNotGroupBy(arg));
        };
        let name = path.last().cloned().unwrap_or_default();
        let position = group_by_names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(&name))
            .ok_or(DriverError::FieldInGroupingNotGroupBy(arg))?;
        positions.push(position);
    }
    Ok(positions)
}

/// Adds a `GROUPING()` call as an aggregation output column and returns that
/// column's name.
///
/// The column is a placeholder as far as the aggregation is concerned -- a
/// `FIRST_ROW` over the constant `0`, so the column exists and every group
/// produces exactly one value -- and [`run_rollup_aggregate`] overwrites it
/// with the per-grouping-set bitmask. Repeating the same call text reuses the
/// column already added, as the aggregate path does for a repeated aggregate.
fn add_grouping_column(
    args: &[tidb_ast::Expr],
    display: String,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_names: &[String],
) -> Result<String, DriverError> {
    if let Some(index) = names
        .iter()
        .position(|name| name.eq_ignore_ascii_case(&display))
    {
        if grouping_specs.iter().any(|spec| spec.out_index == index) {
            return Ok(display);
        }
    }
    let group_positions = grouping_arg_positions(args, group_by_names)?;
    let placeholder = Expression::Constant(tidb_expr::constant::Constant::new(
        Datum::Int(0),
        FieldType::new(FieldTypeCode::LongLong),
    ));
    agg_funcs.push(AggFunc {
        kind: AggKind::FirstRow,
        arg: Some(placeholder),
        extra_args: Vec::new(),
        distinct: false,
        order_by: Vec::new(),
    });
    grouping_specs.push(GroupingSpec {
        out_index: names.len(),
        group_positions,
    });
    names.push(display.clone());
    types.push(grouping_result_type());
    Ok(display)
}

/// Where one select field of an aggregate query reads its value from.
enum OutputSlot {
    /// An aggregation output column, by index.
    Agg(usize),
    /// An expression over the aggregation's (+ Apply's) output columns, by
    /// index into `post_agg_exprs` -- a select field that CONTAINS a
    /// correlated subquery alongside aggregates/columns, e.g.
    /// `SUM(v) + (SELECT ...)`.
    Expr(usize),
    /// The column the n-th window call appends above the aggregation.
    Window(usize),
}

/// Extracts the one correlated subquery in a post-aggregation expression (a
/// select field, `HAVING`, or an `ORDER BY` item), hoists any aggregate calls
/// left in the remainder into `agg_funcs`/`names`/`types`, and returns the
/// resulting expression: aggregates and grouped columns become output column
/// references (Go's `havingWindowAndOrderbyExprResolver`), and the subquery
/// becomes a `__apply_N` placeholder column reference that the caller's
/// Apply (built once every correlated subquery in the statement is known)
/// makes real. `EXISTS`, `IN` and `ANY`/`ALL` ride the same placeholder,
/// because the Apply appends whatever [`run_correlated_subquery`] folds.
///
/// Returns `(expr, true)` when a correlated subquery was found and hoisted,
/// `(expr, false)` otherwise (uncorrelated, or no subquery at all).
#[allow(clippy::too_many_arguments)]
fn extract_and_hoist_subquery(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    applies: &mut Vec<(CorrelatedSubquery, String, FieldType)>,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_names: &[String],
    resolver: &ScopeResolver<'_>,
    ctx: &crate::StmtContext,
) -> Result<(tidb_ast::Expr, bool), DriverError> {
    // No subquery anywhere in the expression, so there is nothing to hoist
    // out of the way of a per-group Apply: the caller decides how (or
    // whether) to run the aggregate hoist itself, exactly as it did before
    // this function existed.
    if !expr_has_subquery(expr) {
        return Ok((expr.clone(), false));
    }
    let index = applies.len();
    let mut found = None;
    let rewritten =
        extract_correlated_subquery(expr, outer, catalog, current_db, index, &mut found, ctx)?;
    let Some(correlated) = found else {
        // Uncorrelated, or no subquery reachable through this expression
        // shape: left for the caller / the fold pass / the rewriter's own
        // error.
        return Ok((rewritten, false));
    };
    let value_type = if matches!(correlated.kind, SubqueryKind::Scalar) {
        subquery_result_type(&correlated, catalog, current_db, ctx)
            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
    } else {
        FieldType::new(FieldTypeCode::LongLong)
    };
    let hoisted = substitute_aggregates(
        &rewritten,
        agg_funcs,
        names,
        types,
        grouping_specs,
        group_by_names,
        resolver,
    )?;
    applies.push((correlated, format!("__apply_{index}"), value_type));
    Ok((hoisted, true))
}

/// Runs an aggregate `SELECT` (`GROUP BY` and/or aggregate select fields)
/// through [`HashAggExec`].
///
/// Faithful scope (deferred items documented): `COUNT`/`SUM` (Go models
/// `COUNT(*)` as the literal-`1` argument, which counts every row identically);
/// any non-aggregate select field becomes a `FIRST_ROW` carrier (Go's planner
/// does the same; `ONLY_FULL_GROUP_BY` validation is deferred); `DISTINCT`
/// and other aggregate functions are rejected as unsupported. `WITH ROLLUP`
/// runs through [`run_rollup_aggregate`] (plain-column grouping only).
/// `HAVING` and `ORDER BY` run over the aggregation's output, as in Go: an
/// aggregate appearing only in those clauses is appended as a hidden output
/// column and trimmed by a final projection. `GROUPING()` rides the same
/// hidden-column path ([`add_grouping_column`]) but is filled in by the
/// rollup pass rather than aggregated.
fn run_aggregate_select(
    select: &tidb_ast::SelectStmt,
    from_source: Option<Box<dyn Executor>>,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    // The grouped column names, which GROUPING() arguments resolve against and
    // which HAVING/ORDER BY may reference even when the select list does not
    // project them.
    let group_by_names: Vec<String> = select
        .group_by
        .iter()
        .filter_map(|item| match &item.expr {
            tidb_ast::Expr::Column(path) => path.last().cloned(),
            _ => None,
        })
        .collect();

    // Fields -> aggregate functions (+ output names/types).
    let mut agg_funcs: Vec<AggFunc> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    let mut types: Vec<FieldType> = Vec::new();
    let mut grouping_specs: Vec<GroupingSpec> = Vec::new();

    // Window functions over a grouped query compute over the aggregation's
    // OUTPUT rows (Go plans Aggregation -> Selection(HAVING) -> Window ->
    // Sort), so every expression inside a window call -- `RANK() OVER (ORDER
    // BY SUM(v))` -- is hoisted into the aggregation first and the call is
    // left reading that output column. The display names below still come
    // from the ORIGINAL field text, so the column is named as written.
    let hoisted;
    let mut window_calls = Vec::new();
    let select = if crate::window::select_has_window(select) {
        if select.rollup {
            return Err(DriverError::Unsupported(
                "a window function combined with GROUP BY ... WITH ROLLUP is not supported yet",
            ));
        }
        // `ORDER BY <window alias>` names a value the window stage computes,
        // not an aggregation output column, so the alias is resolved to its
        // window expression BEFORE hoisting -- the hoist then leaves the same
        // computed column behind in both places.
        let mut aliased = select.clone();
        for item in &mut aliased.order_by {
            let tidb_ast::Expr::Column(path) = &item.expr else {
                continue;
            };
            let [name] = path.as_slice() else { continue };
            let projected = select.fields.fields().iter().find_map(|field| match field {
                SelectField::Expr {
                    expr,
                    alias: Some(alias),
                } if alias.eq_ignore_ascii_case(name)
                    && !crate::window::windows_in(expr).is_empty() =>
                {
                    Some(expr.clone())
                }
                _ => None,
            });
            if let Some(expr) = projected {
                item.expr = expr;
            }
        }
        let select = &aliased;
        let mut hoist_funcs = Vec::new();
        let mut hoist_names = Vec::new();
        let mut hoist_types = Vec::new();
        let mut hoist_specs = Vec::new();
        let (calls, rewritten) = crate::window::hoist_windows(select, |expr| {
            substitute_aggregates(
                expr,
                &mut hoist_funcs,
                &mut hoist_names,
                &mut hoist_types,
                &mut hoist_specs,
                &group_by_names,
                resolver,
            )
        })?;
        agg_funcs = hoist_funcs;
        names = hoist_names;
        types = hoist_types;
        grouping_specs = hoist_specs;
        window_calls = calls;
        hoisted = rewritten;
        &hoisted
    } else {
        select
    };
    // Where each select field's value comes from, in field order: an
    // aggregation output column, or the column an Apply appends above the
    // aggregation for a correlated subquery.
    let mut slots: Vec<OutputSlot> = Vec::new();
    let mut applies: Vec<(CorrelatedSubquery, String, FieldType)> = Vec::new();
    // The hoisted expression for every select field a correlated subquery
    // reaches into (see `OutputSlot::Expr`), in the order they were found.
    let mut post_agg_exprs: Vec<tidb_ast::Expr> = Vec::new();
    // The name a select field forces onto its output column when the column
    // it reads is SHARED with another field (a hoisted window value, or a
    // grouped column the window stage already carried out).
    let mut slot_names: Vec<Option<String>> = Vec::new();
    for field in select.fields.fields() {
        let SelectField::Expr { expr, alias } = field else {
            return Err(DriverError::Unsupported(
                "`*` is not supported in an aggregate SELECT",
            ));
        };
        let display = alias.clone().unwrap_or_else(|| expr.restore());
        // A hoisted window call: its value is appended above the aggregation,
        // so the field reads that column rather than any aggregate.
        if let Some(index) = hoisted_window_index(expr) {
            slots.push(OutputSlot::Window(index));
            slot_names.push(Some(display));
            continue;
        }
        // A grouped column the hoisting already carried out of the
        // aggregation is REUSED rather than carried twice: two columns of the
        // same name in the window stage's scope would be ambiguous there.
        if !window_calls.is_empty() {
            if let tidb_ast::Expr::Column(path) = expr {
                let name = path.last().cloned().unwrap_or_default();
                if let Some(index) = names
                    .iter()
                    .position(|have| have.eq_ignore_ascii_case(&name))
                {
                    slots.push(OutputSlot::Agg(index));
                    slot_names.push(Some(alias.clone().unwrap_or(name)));
                    continue;
                }
            }
        }
        if expr_has_hoisted_window(expr) {
            // Go computes a larger expression over the projection ABOVE the
            // window operator; this path has no such projection, so only a
            // bare window field is supported over a grouped query.
            return Err(DriverError::Unsupported(
                "a window function nested inside a larger select expression is not \
                 supported over a grouped query",
            ));
        }
        // A correlated subquery in an aggregate select list reads the GROUPED
        // value, so it runs once per OUTPUT row rather than per source row --
        // Go's Apply sits above the aggregation for the same reason. It may
        // sit inside a larger expression (`SUM(v) + (SELECT ...)`); the
        // aggregates around it are hoisted the same way HAVING's are.
        let (hoisted, found) = extract_and_hoist_subquery(
            expr,
            resolver.scope,
            catalog,
            current_db,
            &mut applies,
            &mut agg_funcs,
            &mut names,
            &mut types,
            &mut grouping_specs,
            &group_by_names,
            resolver,
            ctx,
        )?;
        if found {
            slots.push(OutputSlot::Expr(post_agg_exprs.len()));
            slot_names.push(None);
            post_agg_exprs.push(hoisted);
            continue;
        }
        slots.push(OutputSlot::Agg(names.len()));
        slot_names.push(None);
        match expr {
            // Both aggregate shapes lower through the same builder, which
            // knows GROUP_CONCAT's separator and DISTINCT.
            tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. } => {
                let (func, ftype) = build_agg_func(expr, resolver)?;
                agg_funcs.push(func);
                names.push(display);
                types.push(ftype);
            }
            // GROUPING() is not an aggregate: it reads the grouping set the
            // output row came from, so it becomes an output column the rollup
            // pass fills in rather than an expression over the row.
            other if grouping_call_args(other).is_some() => {
                let args = grouping_call_args(other).unwrap_or_default();
                add_grouping_column(
                    args,
                    display,
                    &mut agg_funcs,
                    &mut names,
                    &mut types,
                    &mut grouping_specs,
                    &group_by_names,
                )?;
            }
            other if expr_has_grouping(other) => {
                // Go evaluates `GROUPING(a) + 1` over the projection above the
                // aggregation; this seed has no such projection for select
                // fields, so only a bare GROUPING() field is supported.
                return Err(DriverError::Unsupported(
                    "GROUPING() nested inside a larger select expression is not supported yet",
                ));
            }
            other => {
                // A plain field in an aggregate query rides FIRST_ROW.
                let rewritten = rewrite_expr_resolved(other, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                let t = rewritten
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                agg_funcs.push(AggFunc {
                    kind: AggKind::FirstRow,
                    arg: Some(rewritten),
                    extra_args: Vec::new(),
                    distinct: false,
                    order_by: Vec::new(),
                });
                names.push(match other {
                    tidb_ast::Expr::Column(path) => {
                        path.last().cloned().unwrap_or_else(|| other.restore())
                    }
                    _ => display,
                });
                types.push(t);
            }
        }
    }

    // HAVING / ORDER BY: a correlated subquery is hoisted the same way a
    // select field's is (Apply placeholder + aggregate hoisting); whatever
    // aggregates remain become aggregation output columns.
    let having_expr = match &select.having {
        Some(having) => {
            let (expr, found) = extract_and_hoist_subquery(
                having,
                resolver.scope,
                catalog,
                current_db,
                &mut applies,
                &mut agg_funcs,
                &mut names,
                &mut types,
                &mut grouping_specs,
                &group_by_names,
                resolver,
                ctx,
            )?;
            // A found subquery's remainder is already hoisted; otherwise
            // (no subquery at all, or an uncorrelated one left for the fold
            // pass) HAVING's aggregates still need hoisting, exactly as
            // before a subquery could appear here at all.
            let expr = if found {
                expr
            } else {
                substitute_aggregates(
                    &expr,
                    &mut agg_funcs,
                    &mut names,
                    &mut types,
                    &mut grouping_specs,
                    &group_by_names,
                    resolver,
                )?
            };
            Some(expr)
        }
        None => None,
    };
    let mut order_by_exprs = Vec::with_capacity(select.order_by.len());
    for item in &select.order_by {
        let (expr, found) = extract_and_hoist_subquery(
            &item.expr,
            resolver.scope,
            catalog,
            current_db,
            &mut applies,
            &mut agg_funcs,
            &mut names,
            &mut types,
            &mut grouping_specs,
            &group_by_names,
            resolver,
            ctx,
        )?;
        let expr = if found {
            expr
        } else {
            substitute_aggregates(
                &expr,
                &mut agg_funcs,
                &mut names,
                &mut types,
                &mut grouping_specs,
                &group_by_names,
                resolver,
            )?
        };
        order_by_exprs.push((expr, item.desc));
    }

    // An Apply binds its correlated columns from the AGGREGATION's output row,
    // so every column such a subquery reads must be carried out of the
    // aggregation. A grouped column the select list does not project rides the
    // same hidden FIRST_ROW carrier HAVING's aggregates use. This runs after
    // every clause has been walked, so it covers select-field, HAVING and
    // ORDER BY subqueries in one pass.
    for (correlated, _, _) in &applies {
        for path in &correlated.columns {
            let Some(name) = path.last() else { continue };
            if names.iter().any(|have| have.eq_ignore_ascii_case(name)) {
                continue;
            }
            let carrier = rewrite_expr_resolved(&tidb_ast::Expr::Column(path.clone()), resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            let ftype = carrier
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            agg_funcs.push(AggFunc {
                kind: AggKind::FirstRow,
                arg: Some(carrier),
                extra_args: Vec::new(),
                distinct: false,
                order_by: Vec::new(),
            });
            names.push(name.clone());
            types.push(ftype);
        }
    }

    // GROUP BY expressions (legacy ASC/DESC direction ignored, as in MySQL 8).
    let mut group_by = Vec::with_capacity(select.group_by.len());
    for item in &select.group_by {
        group_by.push(
            rewrite_expr_resolved(&item.expr, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }

    // Source (+ WHERE), as in the plain path.
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match from_source {
        Some(exec) => {
            let schema = exec.schema().clone();
            (exec, schema)
        }
        None => (
            Box::new(TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
                1,
            )),
            Schema::new(vec![]),
        ),
    };
    if let Some(predicate) = &select.where_clause {
        let pred = rewrite_expr_resolved(predicate, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        source = Box::new(SelectionExec::new(
            ExecutorMeta::new(source_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![pred],
            source,
            ctx.clone(),
        ));
    }

    // The aggregation output schema.
    let out_columns: Vec<Column> = types
        .iter()
        .enumerate()
        .map(|(i, ft)| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let out_schema = Schema::new(out_columns);

    let mut root: Box<dyn Executor> = if select.rollup {
        run_rollup_aggregate(
            source,
            &group_by,
            &agg_funcs,
            &out_schema,
            &types,
            &grouping_specs,
            ctx,
        )?
    } else {
        Box::new(HashAggExec::new(
            ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
            group_by,
            agg_funcs,
            source,
            ctx.clone(),
        ))
    };

    // Every correlated subquery found above (select fields, HAVING, ORDER BY)
    // becomes an Apply over the aggregation's output rows here, BEFORE HAVING
    // filters and ORDER BY sorts: the outer row is the GROUP row, so each
    // subquery sees the grouped value and runs once per group rather than
    // once per source row, and HAVING/ORDER BY can then read the appended
    // column like any other aggregation output.
    for (correlated, display, value_type) in applies {
        let outer_scope = FromScope {
            tables: vec![FromTable {
                name: String::new(),
                database: None,
                columns: names.iter().cloned().zip(types.iter().cloned()).collect(),
                offset: 0,
            }],
        };
        types.push(value_type);
        names.push(display);
        let columns: Vec<Column> = types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        // The callback outlives this borrow of the catalog, so it owns a
        // snapshot (see ApplyExec::new).
        let inner_catalog = catalog.clone();
        let inner_db = current_db.to_owned();
        let inner_ctx = ctx.clone();
        let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
            run_correlated_subquery(
                &correlated,
                values,
                &outer_scope,
                &inner_catalog,
                &inner_db,
                &inner_ctx,
            )
            .map_err(|e| match e {
                DriverError::Exec(exec) => exec,
                DriverError::SubqueryReturnsMoreThanOneRow => {
                    ExecError::SubqueryReturnsMoreThanOneRow
                }
                other => ExecError::Unsupported(driver_error_text(&other)),
            })
        });
        root = Box::new(crate::apply::ApplyExec::new(
            ExecutorMeta::new(Schema::new(columns), 7, INIT_CAP, MAX_CHUNK_SIZE),
            root,
            runner,
        ));
    }

    // HAVING filters the aggregation's (+ Applies') output rows (Go's
    // Selection above the Aggregation), and ORDER BY sorts them. Built after
    // the Applies above, so both clauses can read a `__apply_N` column by
    // name exactly like an aggregate output.
    let mut agg_resolver = AggOutputResolver {
        names: names.clone(),
        types: types.clone(),
    };
    let mut out_schema = root.schema().clone();
    if let Some(having) = &having_expr {
        let predicate = rewrite_expr_resolved(having, &agg_resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        root = Box::new(SelectionExec::new(
            ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
            vec![predicate],
            root,
            ctx.clone(),
        ));
    }
    // The window stage sits between HAVING and ORDER BY, exactly where Go
    // plans it: the rows it sees are the surviving GROUP rows (with any
    // Apply-appended subquery columns), and the sort below then orders the
    // already-computed window values.
    let window_base = names.len();
    if !window_calls.is_empty() {
        let scope = FromScope {
            tables: vec![FromTable {
                name: String::new(),
                database: None,
                columns: names.iter().cloned().zip(types.iter().cloned()).collect(),
                offset: 0,
            }],
        };
        let rows = drain_executor_rows(root, &types)?;
        let (rows, scope_with_windows) =
            crate::window::compute_windows(&window_calls, rows, &scope, ctx)?;
        // The synthetic `__window_<i>` names are kept here so the ORDER BY /
        // HAVING rewriting resolves them; the final projection puts the
        // field's own written text back on the visible column.
        for (name, field_type) in scope_with_windows
            .column_list()
            .into_iter()
            .skip(window_base)
        {
            names.push(name);
            types.push(field_type);
        }
        let columns: Vec<Column> = types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        out_schema = Schema::new(columns);
        root = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(out_schema.clone(), 0, INIT_CAP, MAX_CHUNK_SIZE),
            rows,
        ));
        // ORDER BY resolves against the WIDENED output, so an `ORDER BY` over
        // a window value reads the computed column.
        agg_resolver = AggOutputResolver {
            names: names.clone(),
            types: types.clone(),
        };
    }
    if !order_by_exprs.is_empty() {
        let mut by_items = Vec::with_capacity(order_by_exprs.len());
        for (expr, desc) in &order_by_exprs {
            by_items.push(SortByItem {
                expr: rewrite_expr_resolved(expr, &agg_resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
                desc: *desc,
            });
        }
        root = Box::new(SortExec::new(
            ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
            by_items,
            root,
            ctx.clone(),
        ));
    }
    if let Some(limit) = &select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        let limit_schema = root.schema().clone();
        root = Box::new(LimitExec::new(
            ExecutorMeta::new(limit_schema, 4, INIT_CAP, MAX_CHUNK_SIZE),
            offset,
            count,
            root,
        ));
    }

    // The select list's own columns, in field order: the aggregates and
    // carriers HAVING/ORDER BY needed but nothing selected are trimmed here,
    // and a select field that hoisted a correlated subquery is evaluated as
    // the full expression (Go's final projection over the aggregation's
    // schema, generalized from a plain column read to `rewrite_expr_resolved`
    // so `SUM(v) + (SELECT ...)`-shaped fields can be more than one column).
    // A window column always needs the projection, if only to put the
    // field's written text back on it in place of the synthetic name.
    let has_expr_slot = slots.iter().any(|slot| matches!(slot, OutputSlot::Expr(_)));
    if has_expr_slot || !window_calls.is_empty() {
        let visible: Vec<Expression> = slots
            .iter()
            .map(|slot| match slot {
                OutputSlot::Agg(index) => {
                    let mut col = Column::new((*index + 1) as i64, types[*index].clone());
                    col.index = *index as i64;
                    Ok(Expression::Column(col))
                }
                OutputSlot::Window(k) => {
                    let index = window_base + k;
                    let mut col = Column::new((index + 1) as i64, types[index].clone());
                    col.index = index as i64;
                    Ok(Expression::Column(col))
                }
                OutputSlot::Expr(index) => {
                    rewrite_expr_resolved(&post_agg_exprs[*index], &agg_resolver)
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))
                }
            })
            .collect::<Result<_, DriverError>>()?;
        let visible_schema: Vec<Column> = visible
            .iter()
            .enumerate()
            .map(|(out, expr)| {
                let mut col = Column::new(
                    (out + 1) as i64,
                    expr.static_type()
                        .cloned()
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
                );
                col.index = out as i64;
                col
            })
            .collect();
        let field_names: Vec<String> = select
            .fields
            .fields()
            .iter()
            .map(|field| match field {
                SelectField::Expr { expr, alias } => {
                    alias.clone().unwrap_or_else(|| expr.restore())
                }
                _ => String::new(),
            })
            .collect();
        let field_types: Vec<FieldType> = visible_schema
            .iter()
            .map(|c| {
                c.ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
            })
            .collect();
        root = Box::new(ProjectionExec::new(
            ExecutorMeta::new(Schema::new(visible_schema), 5, INIT_CAP, MAX_CHUNK_SIZE),
            visible,
            root,
            ctx.clone(),
        ));
        names = field_names;
        types = field_types;
    } else {
        let sources: Vec<usize> = slots
            .iter()
            .map(|slot| match slot {
                OutputSlot::Agg(index) => *index,
                OutputSlot::Window(k) => window_base + k,
                OutputSlot::Expr(_) => unreachable!("no Expr slot when !has_expr_slot"),
            })
            .collect();
        if !sources.iter().copied().eq(0..types.len()) {
            let visible: Vec<Expression> = sources
                .iter()
                .map(|&i| {
                    let mut col = Column::new((i + 1) as i64, types[i].clone());
                    col.index = i as i64;
                    Expression::Column(col)
                })
                .collect();
            let visible_schema: Vec<Column> = sources
                .iter()
                .enumerate()
                .map(|(out, &i)| {
                    let mut col = Column::new((out + 1) as i64, types[i].clone());
                    col.index = out as i64;
                    col
                })
                .collect();
            root = Box::new(ProjectionExec::new(
                ExecutorMeta::new(Schema::new(visible_schema), 5, INIT_CAP, MAX_CHUNK_SIZE),
                visible,
                root,
                ctx.clone(),
            ));
            names = slot_names
                .iter()
                .zip(&sources)
                .map(|(forced, &i)| forced.clone().unwrap_or_else(|| names[i].clone()))
                .collect();
            types = sources.iter().map(|&i| types[i].clone()).collect();
        }
    }
    let ret_types: Vec<FieldType> = types.clone();

    // `SELECT DISTINCT` over an aggregate result deduplicates the output
    // rows, the same buildDistinct step the plain path applies.
    if select.distinct {
        let columns: Vec<Column> = ret_types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        let schema = Schema::new(columns);
        root = Box::new(distinct_over(root, &schema, ctx));
    }

    root.open()?;
    let mut req = root.new_chunk();
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    loop {
        root.next(&mut req)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            let row = req.get_row(r);
            let values = ret_types
                .iter()
                .enumerate()
                .map(|(c, ft)| row.get_datum(c, ft))
                .collect();
            rows.push(values);
        }
    }
    root.close()?;
    Ok((names.into_iter().zip(ret_types).collect(), rows))
}

/// Runs `GROUP BY g1..gn WITH ROLLUP` by materializing the source rows once
/// and aggregating every grouping-set prefix `(g1..gk)`, `k = n..0`, over
/// them -- logically what Go's Expand operator does by replicating each input
/// row once per grouping set. The rolled-up columns are NULLed in the
/// materialized SOURCE rows, so every expression over them (the `FIRST_ROW`
/// carriers, `a+1`, a `HAVING` reference) evaluates against NULL exactly as
/// it does over Expand's replicated rows; a genuinely-NULL data value and a
/// rollup NULL are then indistinguishable in the output, as in TiDB (captured
/// from real TiDB: `a=1` rows `(b=1,c=10)`/`(b=NULL,c=20)` yield both
/// `[1 NULL 20]` and the subtotal `[1 NULL 30]`). `GROUPING()` is what tells
/// the two apart, and each pass fills its `grouping_specs` columns with the
/// bitmask for the grouping set that pass computes.
///
/// Row order: Go's hash aggregation over Expand output emits rollup rows in a
/// NONDETERMINISTIC order (verified against real TiDB -- the order changes
/// across runs), so only the row multiset is contractual and `ORDER BY` is the
/// only ordering guarantee. This tier emits full groups first (first-seen
/// order), then each shorter prefix's subtotals, then the grand total. An
/// empty source yields no rows at all -- not even the grand total -- because
/// Expand replicates zero rows (unlike a scalar aggregate).
fn run_rollup_aggregate(
    source: Box<dyn Executor>,
    group_by: &[Expression],
    agg_funcs: &[AggFunc],
    out_schema: &Schema,
    out_types: &[FieldType],
    grouping_specs: &[GroupingSpec],
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    // Each rolled-up position must be a plain column so it can be NULLed in
    // the materialized source rows (Go's Expand projects grouping expressions
    // into dedicated columns; that generality is deferred).
    let mut group_cols = Vec::with_capacity(group_by.len());
    for expr in group_by {
        let Expression::Column(col) = expr else {
            return Err(DriverError::Unsupported(
                "WITH ROLLUP over a non-column GROUP BY expression is not supported yet",
            ));
        };
        group_cols.push(
            usize::try_from(col.index).map_err(|_| {
                DriverError::Parse("GROUP BY column has no source index".to_string())
            })?,
        );
    }

    // Materialize the source once; every prefix pass replays these rows.
    let source_schema = source.schema().clone();
    let source_types = source.ret_field_types().to_vec();
    let rows = drain_executor_rows(source, &source_types)?;

    let mut out_rows: Vec<Vec<Datum>> = Vec::new();
    if !rows.is_empty() {
        for k in (0..=group_cols.len()).rev() {
            let mut pass_rows = rows.clone();
            for row in &mut pass_rows {
                for &idx in &group_cols[k..] {
                    row[idx] = Datum::Null;
                }
            }
            let pass_source = Box::new(MemTableSourceExec::new(
                ExecutorMeta::new(source_schema.clone(), 1, INIT_CAP, MAX_CHUNK_SIZE),
                pass_rows,
            ));
            let agg = HashAggExec::new(
                ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
                group_by[..k].to_vec(),
                agg_funcs.to_vec(),
                pass_source,
                ctx.clone(),
            );
            // This pass rolls up positions `k..`, which IS the grouping bit
            // each GROUPING() call reports -- the one thing that distinguishes
            // a subtotal's NULL from a data NULL.
            let mut pass_out = drain_executor_rows(Box::new(agg), out_types)?;
            for spec in grouping_specs {
                let mask = Datum::UInt(spec.mask_for_prefix(k));
                for row in &mut pass_out {
                    row[spec.out_index] = mask.clone();
                }
            }
            out_rows.extend(pass_out);
        }
    }
    Ok(Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        out_rows,
    )))
}

/// Opens `exec`, drains every row as datums of `types`, and closes it.
fn drain_executor_rows(
    mut exec: Box<dyn Executor>,
    types: &[FieldType],
) -> Result<Vec<Vec<Datum>>, DriverError> {
    exec.open()?;
    let mut rows = Vec::new();
    let mut req = exec.new_chunk();
    loop {
        exec.next(&mut req)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            let row = req.get_row(r);
            rows.push(
                types
                    .iter()
                    .enumerate()
                    .map(|(c, ft)| row.get_datum(c, ft))
                    .collect(),
            );
        }
    }
    exec.close()?;
    Ok(rows)
}

/// Go `buildDistinct`: an aggregation grouping by every column of `schema`,
/// carrying each one through a `FIRST_ROW` aggregate.
///
/// The hash aggregation emits groups in first-seen order, so a sort below it
/// still orders the deduplicated rows -- the first row of each group is the
/// one the sort put first.
fn distinct_over(
    child: Box<dyn Executor>,
    schema: &Schema,
    ctx: &crate::StmtContext,
) -> HashAggExec<crate::StmtContext> {
    let group_by: Vec<Expression> = schema
        .columns
        .iter()
        .map(|column| Expression::Column(column.clone()))
        .collect();
    let agg_funcs: Vec<AggFunc> = group_by
        .iter()
        .map(|column| AggFunc::new(AggKind::FirstRow, Some(column.clone())))
        .collect();
    HashAggExec::new(
        ExecutorMeta::new(schema.clone(), 5, INIT_CAP, MAX_CHUNK_SIZE),
        group_by,
        agg_funcs,
        child,
        ctx.clone(),
    )
}

/// Evaluates a `LIMIT` bound, which must be a non-negative integer literal.
pub(crate) fn eval_limit_bound(expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
    match expr {
        tidb_ast::Expr::Int(text) => text
            .parse::<u64>()
            .map_err(|_| DriverError::Unsupported("LIMIT bound must be a non-negative integer")),
        _ => Err(DriverError::Unsupported(
            "LIMIT bound must be an integer literal",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_constant_arithmetic() {
        assert_eq!(
            run_select("SELECT 1 + 1").unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select("SELECT 1 + 1, 2 * 3").unwrap(),
            vec![vec![Datum::Int(2), Datum::Int(6)]]
        );
        assert_eq!(
            run_select("SELECT 2 * 3 - 1").unwrap(),
            vec![vec![Datum::Int(5)]]
        );
    }

    #[test]
    fn select_with_where() {
        // A true predicate keeps the row.
        assert_eq!(
            run_select("SELECT 42 WHERE 1 = 1").unwrap(),
            vec![vec![Datum::Int(42)]]
        );
        // A false predicate yields no rows.
        assert_eq!(
            run_select("SELECT 42 WHERE 1 = 0").unwrap(),
            Vec::<Vec<Datum>>::new()
        );
    }

    #[test]
    fn limit_and_order_by_wire_up() {
        // LIMIT truncates / zeroes the single row.
        assert_eq!(
            run_select("SELECT 42 LIMIT 1").unwrap(),
            vec![vec![Datum::Int(42)]]
        );
        assert_eq!(
            run_select("SELECT 42 LIMIT 0").unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select("SELECT 42 LIMIT 1, 1").unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // ORDER BY over the single dual row passes through the sort.
        assert_eq!(
            run_select("SELECT 42 ORDER BY 1 DESC").unwrap(),
            vec![vec![Datum::Int(42)]]
        );
    }

    #[test]
    fn unknown_table_is_rejected() {
        assert!(matches!(
            run_select("SELECT a FROM missing"),
            Err(DriverError::Unsupported(_))
        ));
    }

    fn test_catalog() -> Catalog {
        use tidb_datatype::FieldTypeCode;
        let mut catalog = Catalog::default();
        catalog.register(
            "t",
            MemTable {
                columns: vec![
                    ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                    ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ],
                rows: vec![
                    vec![Datum::Int(1), Datum::Int(30)],
                    vec![Datum::Int(2), Datum::Int(20)],
                    vec![Datum::Int(3), Datum::Int(10)],
                ],
            },
        );
        catalog
    }

    #[test]
    fn select_from_table() {
        let catalog = test_catalog();
        // Column projection.
        assert_eq!(
            run_select_on(
                "SELECT a FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)]
            ]
        );
        // Wildcard, qualified column, and an expression over columns.
        assert_eq!(
            run_select_on(
                "SELECT * FROM t WHERE t.a > 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(3), Datum::Int(10)],
            ]
        );
        assert_eq!(
            run_select_on(
                "SELECT a + b FROM t WHERE a = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(22)]]
        );
    }

    #[test]
    fn insert_then_select_round_trip() {
        let mut catalog = test_catalog();
        // Full-row insert.
        assert_eq!(
            run_insert_on(
                "INSERT INTO t VALUES (4, 40), (5, 50)",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            2
        );
        // Column-list insert: unspecified column fills with NULL.
        assert_eq!(
            run_insert_on(
                "INSERT INTO t (a) VALUES (6)",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM t WHERE a > 3 ORDER BY a",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(4), Datum::Int(40)],
                vec![Datum::Int(5), Datum::Int(50)],
                vec![Datum::Int(6), Datum::Null],
            ]
        );
        // Arity mismatch and unknown table are rejected.
        assert!(run_insert_on(
            "INSERT INTO t (a) VALUES (1, 2)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        assert!(run_insert_on(
            "INSERT INTO missing VALUES (1)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }

    /// The deployment-ladder proof: INSERT and SELECT round-trip through a
    /// table whose rows are genuine TiKV-format bytes (record keys + v2 row
    /// values), not a value matrix.
    #[test]
    fn sql_round_trips_through_real_tikv_bytes() {
        use crate::kv_table::{KvColumn, KvTable};
        use tidb_datatype::FieldTypeCode;
        let mut catalog = Catalog::default();
        catalog.register_kv(
            "kt",
            KvTable::new(
                77,
                vec![
                    KvColumn {
                        name: "a".to_owned(),
                        id: 1,
                        field_type: FieldType::new(FieldTypeCode::LongLong),
                        default_value: None,
                        // A column present at CREATE TABLE has no pre-existing rows.
                        origin_default: None,
                    },
                    KvColumn {
                        name: "b".to_owned(),
                        id: 2,
                        field_type: FieldType::new(FieldTypeCode::LongLong),
                        default_value: None,
                        // A column present at CREATE TABLE has no pre-existing rows.
                        origin_default: None,
                    },
                ],
            ),
        );

        assert_eq!(
            run_insert_on(
                "INSERT INTO kt VALUES (1, 10), (2, 20), (3, 30)",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            3
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM kt WHERE a > 1 ORDER BY b DESC",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(3), Datum::Int(30)],
                vec![Datum::Int(2), Datum::Int(20)],
            ]
        );
        assert_eq!(
            run_select_on(
                "SELECT a + b FROM kt WHERE a = 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(11)]]
        );
    }

    #[test]
    fn aggregate_selects() {
        let catalog = test_catalog();
        // Global aggregates: rows (1,30),(2,20),(3,10).
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*), SUM(a) FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            // SUM is a DECIMAL in MySQL even over a BIGINT column.
            vec![vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(6))
            ]]
        );
        // GROUP BY with a carried key column, WHERE below the agg.
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM t WHERE b >= 20 GROUP BY a",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(1)],
                vec![Datum::Int(2), Datum::Int(1)],
            ]
        );
        // Empty-input rules through SQL: global agg over no rows -> one row.
        assert_eq!(
            run_select_on(
                "SELECT COUNT(a) FROM t WHERE a > 100",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(0)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM t WHERE a > 100 GROUP BY a",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // MIN/MAX over the shared datum ordering.
        assert_eq!(
            run_select_on(
                "SELECT MIN(a), MAX(b) FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1), Datum::Int(30)]]
        );
        // AVG over integers is DECIMAL, scaled by div_precision_increment.
        assert_eq!(
            run_select_on(
                "SELECT AVG(a) FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
                "2.0000"
            ))]]
        );
        // DISTINCT folds repeated inputs once per group: a is 1,2,3 while the
        // constant 1 collapses to a single counted value.
        assert_eq!(
            run_select_on(
                "SELECT COUNT(DISTINCT a), COUNT(DISTINCT 1) FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3), Datum::Int(1)]]
        );
        // An all-NULL / empty group is NULL for MIN/MAX and AVG, as in Go.
        assert_eq!(
            run_select_on(
                "SELECT MIN(a), MAX(a), AVG(a) FROM t WHERE a > 100",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Null, Datum::Null, Datum::Null]]
        );
    }

    /// HAVING filters aggregate output rows, ORDER BY sorts them, and an
    /// aggregate that appears only in those clauses is computed as a hidden
    /// column and trimmed from the result (Go's resolveHavingAndOrderBy plus
    /// the final projection).
    #[test]
    fn aggregate_having_and_order_by() {
        let mut catalog = test_catalog();
        crate::run_create_table_on("CREATE TABLE g (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO g VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // HAVING over an aggregate that IS in the select list.
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(3), Datum::Int(2)],
            ]
        );
        // HAVING over an aggregate that is NOT selected: one output column.
        assert_eq!(
            run_select_on(
                "SELECT a FROM g GROUP BY a HAVING SUM(b) > 15",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // ORDER BY an aggregate that is not selected, descending.
        assert_eq!(
            run_select_on(
                "SELECT a FROM g GROUP BY a ORDER BY SUM(b) DESC",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(3)],
                vec![Datum::Int(2)]
            ]
        );
        // HAVING and ORDER BY together, with LIMIT applied after both.
        assert_eq!(
            run_select_on(
                "SELECT a, SUM(b) FROM g GROUP BY a HAVING COUNT(*) > 1 ORDER BY SUM(b) LIMIT 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(15))
            ]]
        );
        // ORDER BY a selected alias.
        assert_eq!(
            run_select_on(
                "SELECT a, SUM(b) AS total FROM g GROUP BY a ORDER BY total",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(5))
                ],
                vec![
                    Datum::Int(3),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(15))
                ],
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(30))
                ],
            ]
        );
        // A grouped column that is not selected is still visible to HAVING
        // and ORDER BY (Go carries it as a hidden FIRST_ROW column).
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*) FROM g GROUP BY a HAVING a > 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
        // A global aggregate's HAVING filters the single group.
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*) FROM g HAVING COUNT(*) > 100",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
    }

    /// UPDATE and DELETE over both table backings, including MySQL's
    /// affected-row rule: an UPDATE counts CHANGED rows, so a row whose new
    /// values equal its old ones is touched but not affected.
    #[test]
    fn update_and_delete_rows() {
        for kv in [false, true] {
            let mut catalog = Catalog::default();
            if kv {
                crate::run_create_table_on("CREATE TABLE w (a BIGINT, b BIGINT)", &mut catalog)
                    .unwrap();
            } else {
                catalog.register(
                    "w",
                    MemTable {
                        columns: vec![
                            ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                            ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                        ],
                        rows: vec![],
                    },
                );
            }
            run_insert_on(
                "INSERT INTO w VALUES (1, 10), (2, 20), (3, 30)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();

            // WHERE-selected update, counting only changed rows.
            assert_eq!(
                run_update_on(
                    "UPDATE w SET b = b + 1 WHERE a >= 2",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                2,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on(
                    "SELECT a, b FROM w",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![
                    vec![Datum::Int(1), Datum::Int(10)],
                    vec![Datum::Int(2), Datum::Int(21)],
                    vec![Datum::Int(3), Datum::Int(31)],
                ],
                "kv={kv}"
            );

            // A no-op update matches rows but changes none: MySQL reports 0.
            assert_eq!(
                run_update_on(
                    "UPDATE w SET b = b WHERE a = 1",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                0,
                "kv={kv}"
            );

            // Later assignments see earlier ones, as in Go's composeNewRow.
            assert_eq!(
                run_update_on(
                    "UPDATE w SET a = 7, b = a WHERE a = 1",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                1,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on(
                    "SELECT a, b FROM w WHERE a = 7",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![vec![Datum::Int(7), Datum::Int(7)]],
                "kv={kv}"
            );

            // A WHERE-less UPDATE touches every row.
            assert_eq!(
                run_update_on(
                    "UPDATE w SET b = 0",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                3,
                "kv={kv}"
            );

            // DELETE removes the selected rows and reports their count.
            assert_eq!(
                run_delete_on(
                    "DELETE FROM w WHERE a >= 3",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                2,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on(
                    "SELECT a FROM w",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![vec![Datum::Int(2)]],
                "kv={kv}"
            );

            // A WHERE-less DELETE empties the table, and re-inserting works
            // after it (the store is genuinely empty, not just filtered).
            assert_eq!(
                run_delete_on(
                    "DELETE FROM w",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                1,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on(
                    "SELECT a FROM w",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                Vec::<Vec<Datum>>::new(),
                "kv={kv}"
            );
            run_insert_on(
                "INSERT INTO w VALUES (9, 9)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
            assert_eq!(
                run_select_on(
                    "SELECT a FROM w",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![vec![Datum::Int(9)]],
                "kv={kv}"
            );

            // ORDER BY and LIMIT are supported now (see the session's
            // `insert_select_and_ordered_dml`); an unknown SET column and
            // the IGNORE form still fail closed.
            assert!(run_update_on(
                "UPDATE w SET zzz = 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .is_err());
            assert!(run_update_on(
                "UPDATE IGNORE w SET a = 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .is_err());
        }
    }

    /// Two-table joins: inner, left/right outer with NULL padding, the
    /// ON-vs-WHERE distinction, qualified and ambiguous column references,
    /// wildcard expansion, and a three-table left-deep chain.
    #[test]
    fn joins() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE l (id BIGINT, v BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE r (id BIGINT, w BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO r VALUES (1, 100), (3, 300), (3, 301)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // INNER JOIN: only matches, and a left row matching twice emits twice.
        assert_eq!(
            run_select_on(
                "SELECT l.id, l.v, r.w FROM l JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(10), Datum::Int(100)],
                vec![Datum::Int(3), Datum::Int(30), Datum::Int(300)],
                vec![Datum::Int(3), Datum::Int(30), Datum::Int(301)],
            ]
        );

        // LEFT JOIN pads the unmatched left row with NULLs.
        assert_eq!(
            run_select_on(
                "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(100)],
                vec![Datum::Int(2), Datum::Null],
                vec![Datum::Int(3), Datum::Int(300)],
                vec![Datum::Int(3), Datum::Int(301)],
            ]
        );

        // The ON/WHERE distinction: filtering the padded rows is an anti-join.
        assert_eq!(
            run_select_on(
                "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.id IS NULL",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        // A condition in ON does NOT drop the left row; it only stops matching.
        assert_eq!(
            run_select_on(
                "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id AND r.w > 200",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Null],
                vec![Datum::Int(2), Datum::Null],
                vec![Datum::Int(3), Datum::Int(300)],
                vec![Datum::Int(3), Datum::Int(301)],
            ]
        );

        // RIGHT JOIN keeps every right row, padding the left side.
        assert_eq!(
            run_select_on(
                "SELECT l.v, r.id FROM l RIGHT JOIN r ON l.id = r.id AND l.v > 100",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Null, Datum::Int(1)],
                vec![Datum::Null, Datum::Int(3)],
                vec![Datum::Null, Datum::Int(3)],
            ]
        );

        // A comma join with no ON is a Cartesian product.
        assert_eq!(
            run_select_on(
                "SELECT l.id FROM l, r",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            9
        );

        // `*` expands across both tables in FROM order; `t.*` over one.
        assert_eq!(
            run_select_on(
                "SELECT * FROM l JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .first()
            .unwrap()
            .len(),
            4
        );
        assert_eq!(
            run_select_on(
                "SELECT r.* FROM l JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .first()
            .unwrap()
            .len(),
            2
        );

        // An unqualified column present in both tables is ambiguous, as in
        // MySQL; one present in only one table resolves.
        assert!(run_select_on(
            "SELECT id FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        assert_eq!(
            run_select_on(
                "SELECT v, w FROM l JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );

        // An alias replaces the table name for qualification.
        assert_eq!(
            run_select_on(
                "SELECT a.id FROM l AS a JOIN r AS b ON a.id = b.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );

        // A three-table left-deep chain, and an aggregate over a join.
        crate::run_create_table_on("CREATE TABLE m (id BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO m VALUES (3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*) FROM l JOIN r ON l.id = r.id JOIN m ON m.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );

        // Unsupported join shapes fail closed.
        assert!(run_select_on(
            "SELECT * FROM l NATURAL JOIN r",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        assert!(run_select_on(
            "SELECT * FROM l JOIN r USING (id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }

    /// Uncorrelated subqueries are evaluated and folded into literals, the way
    /// Go's handleScalarSubquery does for the non-Apply case.
    #[test]
    fn subqueries() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE s (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE u (a BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (2), (3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A scalar subquery in the select list and in a predicate.
        assert_eq!(
            run_select_on(
                "SELECT (SELECT MAX(b) FROM s)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(30)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE b = (SELECT MAX(b) FROM s)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // No rows is NULL, as Go's buildMaxOneRow leaves it.
        assert_eq!(
            run_select_on(
                "SELECT (SELECT a FROM s WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Null]]
        );
        // More than one row is Go's ER_SUBQUERY_NO_1_ROW.
        assert!(matches!(
            run_select_on(
                "SELECT (SELECT a FROM s)",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::SubqueryReturnsMoreThanOneRow)
        ));

        // IN / NOT IN over a subquery.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a IN (SELECT a FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // An empty IN subquery matches nothing, and NOT IN over it matches all.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a IN (SELECT a FROM u WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );

        // EXISTS / NOT EXISTS fold to 1 / 0.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE EXISTS (SELECT 1 FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE NOT EXISTS (SELECT 1 FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // A subquery in HAVING, over the aggregate path.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s GROUP BY a HAVING SUM(b) > (SELECT MIN(b) FROM s)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );

        // ANY is the OR chain over the folded values, ALL the AND chain:
        // `a > ANY (2, 3)` holds only for 3, and `a > ALL (2, 3)` for nothing.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a > ANY (SELECT a FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a > ALL (SELECT a FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // An empty inner result: ALL is vacuously true, ANY is false.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a > ALL (SELECT a FROM u WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)]
            ]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a > ANY (SELECT a FROM u WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
    }

    /// A correlated subquery becomes an Apply: the inner query re-runs once
    /// per outer row with the outer row's values bound, which is Go's
    /// NestedLoopApplyExec loop.
    #[test]
    fn correlated_subqueries() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE o (id BIGINT, v BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE i (id BIGINT, w BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO o VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO i VALUES (1, 10), (2, 5), (2, 25), (4, 40)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // Scalar: each outer row compares against its own inner maximum.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v = (SELECT MAX(w) FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // id 2's inner rows are 5 and 25, so its max is 25 and 20 < 25 holds;
        // id 1 compares 10 < 10, which does not.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v < (SELECT MAX(w) FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        // An outer row whose inner query returns nothing compares against
        // NULL, so the predicate is unknown and the row drops -- id 3 has no
        // matching inner rows.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE (SELECT MAX(w) FROM i WHERE i.id = o.id) IS NULL",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // Correlated EXISTS / NOT EXISTS, the semi- and anti-join shapes.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // An unqualified inner reference to an outer column still correlates.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.w = v)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );

        // A correlated subquery returning several rows is still the 1242 case,
        // raised from inside the apply loop and reported as the same error the
        // folded path reports.
        assert!(matches!(
            run_select_on(
                "SELECT id FROM o WHERE v = (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::SubqueryReturnsMoreThanOneRow)
        ));

        // Correlated IN / NOT IN and ANY / ALL: the same Apply, folding this
        // outer row's inner result into the three-valued answer. id 3's inner
        // result is EMPTY, which is why NOT IN and ALL keep it.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v IN (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v NOT IN (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v > ANY (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v > ALL (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
    }

    /// A correlated subquery nested inside a larger aggregate-path
    /// expression: arithmetic over an aggregate in the select list, and a
    /// comparison against an aggregate in `HAVING`. The Apply sits above the
    /// aggregation (Go's plan shape), so the subquery sees the GROUPED value
    /// and runs once per group.
    ///
    /// Every result here was cross-checked against a
    /// `testkit.CreateMockStore` capture of real TiDB on the same schema.
    #[test]
    fn grouped_correlated_subqueries() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE t (g BIGINT, v BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE s (k BIGINT, x BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO t VALUES (1, 10), (1, 20), (2, 5), (3, 100), (NULL, 7)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO s VALUES (1, 1), (1, 2), (2, 3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A correlated scalar subquery combined with an aggregate by
        // arithmetic in the select list: SUM(v) is the group's own total,
        // (SELECT COUNT...) reads how many `s` rows share the group's key.
        assert_eq!(
            run_select_on(
                "SELECT g, SUM(v) + (SELECT COUNT(*) FROM s WHERE s.k = t.g) \
                 FROM t GROUP BY g ORDER BY g",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![
                    Datum::Null,
                    Datum::Decimal(tidb_datatype::Decimal::from_int(7))
                ],
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(32))
                ],
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(6))
                ],
                vec![
                    Datum::Int(3),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(100))
                ],
            ]
        );

        // A correlated scalar subquery compared against an aggregate in
        // HAVING: only groups whose SUM(v) beats the correlated average
        // survive.
        assert_eq!(
            run_select_on(
                "SELECT g FROM t GROUP BY g \
                 HAVING SUM(v) > (SELECT AVG(x) FROM s WHERE s.k = t.g) \
                 ORDER BY g",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );

        // The same HAVING subquery, ANDed with a plain grouped-column
        // predicate -- both conjuncts must be readable off the same
        // post-Apply row.
        assert_eq!(
            run_select_on(
                "SELECT g, SUM(v) FROM t GROUP BY g \
                 HAVING SUM(v) > (SELECT COUNT(*) FROM s WHERE s.k = t.g) AND g IS NOT NULL \
                 ORDER BY g",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(30))
                ],
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(5))
                ],
                vec![
                    Datum::Int(3),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(100))
                ],
            ]
        );

        // HAVING a correlated subquery against a bare (unaggregated) GROUP
        // BY column, with a NULL group in the mix.
        assert_eq!(
            run_select_on(
                "SELECT g, COUNT(*) FROM t GROUP BY g \
                 HAVING (SELECT COUNT(*) FROM s WHERE s.k = g) >= 0 \
                 ORDER BY g",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Null, Datum::Int(1)],
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(2), Datum::Int(1)],
                vec![Datum::Int(3), Datum::Int(1)],
            ]
        );

        // DEFERRED (documented, not silently wrong): a correlated subquery
        // inside an AGGREGATE'S OWN ARGUMENT needs a per-SOURCE-ROW Apply
        // below the aggregation, not the per-GROUP Apply above it this
        // driver builds -- refused precisely rather than mis-evaluated.
        assert!(matches!(
            run_select_on(
                "SELECT g, SUM((SELECT COUNT(*) FROM s WHERE s.k = g)) FROM t GROUP BY g",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::Unsupported(_))
        ));
        assert!(matches!(
            run_select_on(
                "SELECT g, SUM(CASE WHEN EXISTS(SELECT 1 FROM s WHERE s.k = t.g) THEN v ELSE 0 END) \
                 FROM t GROUP BY g",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::Exec(_))
        ));

        // DEFERRED: a HAVING clause referencing a non-grouped, non-aggregated
        // column stays ONLY_FULL_GROUP_BY-refused even with a correlated
        // subquery alongside it -- the subquery does not launder the column
        // reference.
        assert!(matches!(
            run_select_on(
                "SELECT g, SUM(v) FROM t GROUP BY g \
                 HAVING v > (SELECT AVG(x) FROM s WHERE s.k = t.g)",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::Unsupported(_))
        ));

        // DEFERRED: two-level nesting (a correlated subquery whose own body
        // contains a subquery correlated to ITS outer scope) is refused
        // rather than mis-evaluated.
        assert!(run_select_on(
            "SELECT g, (SELECT COUNT(*) FROM s WHERE s.k = t.g \
             AND s.x > (SELECT AVG(x) FROM s s2 WHERE s2.k = s.k)) FROM t GROUP BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }

    /// A single-column integer PRIMARY KEY becomes the row handle (Go's
    /// TableInfo.PKIsHandle), so the key value addresses the row and a repeat
    /// is ErrDupEntry. Transcreated from Go's own duplicate-key behavior in
    /// pkg/table/tables `AddRecord`.
    #[test]
    fn integer_primary_key_is_the_row_handle() {
        for ddl in [
            "CREATE TABLE p (id BIGINT PRIMARY KEY, v BIGINT)",
            "CREATE TABLE p (id BIGINT, v BIGINT, PRIMARY KEY (id))",
        ] {
            let mut catalog = Catalog::default();
            crate::run_create_table_on(ddl, &mut catalog).unwrap();
            run_insert_on(
                "INSERT INTO p VALUES (10, 100), (20, 200)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();

            // The rows come back in handle order, which is the key's order --
            // not insertion order, because the handle IS the primary key.
            run_insert_on(
                "INSERT INTO p VALUES (5, 50)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
            assert_eq!(
                run_select_on(
                    "SELECT id FROM p",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![
                    vec![Datum::Int(5)],
                    vec![Datum::Int(10)],
                    vec![Datum::Int(20)],
                ],
                "{ddl}"
            );

            // A repeated key is Go's ErrDupEntry.
            assert!(
                matches!(
                    run_insert_on(
                        "INSERT INTO p VALUES (10, 999)",
                        &mut catalog,
                        &crate::StmtContext::for_query()
                    ),
                    Err(DriverError::DuplicateEntry { .. })
                ),
                "{ddl}"
            );
            // The failed insert left the original row untouched.
            assert_eq!(
                run_select_on(
                    "SELECT v FROM p WHERE id = 10",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![vec![Datum::Int(100)]],
                "{ddl}"
            );
            // A negative key works too: the key codec sign-flips handles.
            run_insert_on(
                "INSERT INTO p VALUES (-1, 1)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
            assert_eq!(
                run_select_on(
                    "SELECT id FROM p",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap()
                .len(),
                4,
                "{ddl}"
            );
        }
    }

    /// Without a primary key the handle is the allocated row id, so repeated
    /// values are fine -- the table is a heap, as in Go with _tidb_rowid.
    #[test]
    fn without_a_primary_key_rows_repeat_freely() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE h (a BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO h VALUES (1), (1), (1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a FROM h",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );
    }

    /// Constraint shapes that need tiers this seed lacks are rejected rather
    /// than silently dropped, so a table never claims what it cannot enforce.
    #[test]
    fn unsupported_constraints_are_rejected() {
        let mut catalog = Catalog::default();
        for ddl in [
            // Two primary keys is not a table.
            "CREATE TABLE c (a BIGINT PRIMARY KEY, b BIGINT PRIMARY KEY)",
            // A prefix-length primary key needs prefix index support.
            "CREATE TABLE c (a VARCHAR(10), PRIMARY KEY (a(3)))",
        ] {
            assert!(
                crate::run_create_table_on(ddl, &mut catalog).is_err(),
                "{ddl} should be rejected"
            );
        }
    }

    /// A non-integer primary key is not a handle -- Go only sets PKIsHandle
    /// for a single integer column -- so the table keeps allocating row ids
    /// and enforces the key through a unique index instead.
    #[test]
    fn a_non_integer_primary_key_is_enforced_by_its_index() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE s (k VARCHAR(10) PRIMARY KEY)", &mut catalog)
            .unwrap();
        run_insert_on(
            "INSERT INTO s VALUES ('a'), ('b')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT k FROM s",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            2
        );
        // The duplicate is now caught by the index, as in real TiDB.
        assert!(matches!(
            run_insert_on(
                "INSERT INTO s VALUES ('a')",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::DuplicateEntry { .. })
        ));
    }

    /// The text of a string datum, however the codec chose to represent it.
    fn datum_text_for_test(value: &Datum) -> String {
        match value {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
            other => panic!("expected a string datum, got {other:?}"),
        }
    }

    /// UNIQUE indexes are enforced on every write path, and MySQL's rule that
    /// a unique index permits any number of NULLs is Go's `distinct` flag:
    /// an entry with a NULL indexed value is stored the non-distinct way and
    /// never collides.
    #[test]
    fn unique_indexes() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE u (id BIGINT PRIMARY KEY, email VARCHAR(32) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (1, 'a@x', 10), (2, 'b@x', 20)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A repeated unique value is rejected, naming the index.
        match run_insert_on(
            "INSERT INTO u VALUES (3, 'a@x', 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        ) {
            Err(DriverError::DuplicateEntry { value, key }) => {
                assert_eq!(value, "a@x");
                // Captured from TiDB: the key is qualified table.index, as in
                // "Duplicate entry 'a' for key 'm.code'".
                assert_eq!(key, "u.email");
            }
            other => panic!("expected a duplicate-entry error, got {other:?}"),
        }
        // The rejected insert wrote nothing.
        assert_eq!(
            run_select_on(
                "SELECT id FROM u",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            2
        );

        // UPDATE is checked too, and a rejected update leaves the row alone.
        assert!(matches!(
            run_update_on(
                "UPDATE u SET email = 'a@x' WHERE id = 2",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::DuplicateEntry { .. })
        ));
        assert_eq!(
            datum_text_for_test(
                &run_select_on(
                    "SELECT email FROM u WHERE id = 2",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap()[0][0]
            ),
            "b@x"
        );
        // An update that frees a value lets another row take it.
        run_update_on(
            "UPDATE u SET email = 'c@x' WHERE id = 1",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (4, 'a@x', 40)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // DELETE frees the value as well.
        run_delete_on(
            "DELETE FROM u WHERE id = 4",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (5, 'a@x', 50)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // MySQL permits many NULLs in a unique index.
        run_insert_on(
            "INSERT INTO u VALUES (6, NULL, 60)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (7, NULL, 70)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT id FROM u",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            5
        );
    }

    /// A non-unique index accepts repeats: its key carries the handle, so two
    /// rows with the same value are two entries (Go's non-distinct path).
    #[test]
    fn a_non_unique_index_accepts_repeats() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE n (id BIGINT PRIMARY KEY, tag VARCHAR(8), KEY tag_idx (tag))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO n VALUES (1, 'x'), (2, 'x'), (3, 'y')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT id FROM n",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );
    }

    /// A unique index stores the handle as its value, which is what makes a
    /// unique-key lookup a point read (Go's PointGetPlan on a unique key).
    #[test]
    fn a_unique_index_entry_points_at_its_row() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE k (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO k VALUES (7, 'abc'), (8, 'def')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("k") else {
            panic!("expected a kv table");
        };
        let mut table = table.clone();
        let index_id = table
            .indexes()
            .iter()
            .find(|index| index.name == "code")
            .expect("the unique index exists")
            .id;
        assert_eq!(
            table
                .lookup_unique(index_id, &[Datum::Bytes(b"abc".to_vec())])
                .unwrap(),
            Some(TableHandle::Int(7)),
            "the entry carries the row's handle"
        );
        assert_eq!(
            table
                .lookup_unique(index_id, &[Datum::Bytes(b"nope".to_vec())])
                .unwrap(),
            None
        );
    }

    /// Go's TryFastPlan: a single-table SELECT whose WHERE pins the handle or
    /// a whole unique index reads one row instead of scanning. The results
    /// must be identical to the scan in every case, including the cases that
    /// do NOT qualify and fall back.
    #[test]
    fn point_get_plans() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE g (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO g VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // Handle point get.
        assert_eq!(
            run_select_on(
                "SELECT v FROM g WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(20)]]
        );
        // A handle that does not exist reads nothing.
        assert_eq!(
            run_select_on(
                "SELECT v FROM g WHERE id = 99",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // Unique-index point get, through the entry's stored handle.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE code = 'c'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE code = 'zz'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // The WHERE stays in the pipeline, so an extra condition still
        // filters: the point get narrows the source, it does not replace the
        // filter.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 2 AND v = 20",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 2 AND v = 999",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // Shapes that do not qualify fall back to the scan and stay correct.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE v = 30",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]],
            "a non-key column is not a point get"
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id > 1 ORDER BY id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]],
            "a range is not a point get"
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 1 OR id = 3",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(3)]],
            "Go recurses only through AND, so OR is not a point get"
        );
        // Go rejects the fast plan when ORDER BY or HAVING is present, or when
        // LIMIT could remove the row; the answers stay right either way.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 2 LIMIT 1 OFFSET 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 2 ORDER BY id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );

        // A non-integer constant cannot name an integer handle: no row, not a
        // wrong row.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 'x'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // A point get sees writes, including the row a DELETE removed.
        run_update_on(
            "UPDATE g SET v = 99 WHERE id = 2",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT v FROM g WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(99)]]
        );
        run_delete_on(
            "DELETE FROM g WHERE id = 2",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT v FROM g WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE code = 'b'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new(),
            "the deleted row's index entry is gone too"
        );
    }

    /// The results above would be right even if the fast plan never fired, so
    /// this asserts the DECISION: which shapes Go's tryPointGetPlan accepts
    /// and which it rejects.
    #[test]
    fn point_get_is_chosen_only_for_the_shapes_go_accepts() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE d (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO d VALUES (1, 'a', 10)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("d") else {
            panic!("expected a kv table");
        };
        let columns = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.field_type.clone()))
            .collect::<Vec<_>>();

        let decides = |sql: &str| {
            let stmt = tidb_parser::parse(sql).unwrap();
            let Stmt::Query(query) = &stmt else {
                panic!("not a query")
            };
            let QueryStmt::Select(select) = &**query else {
                panic!("not a select")
            };
            try_point_get(select, table, &columns).unwrap()
        };

        // Accepted: the handle, and a whole unique index.
        assert_eq!(
            decides("SELECT v FROM d WHERE id = 1"),
            Some(Some(TableHandle::Int(1)))
        );
        assert_eq!(
            decides("SELECT v FROM d WHERE 1 = id"),
            Some(Some(TableHandle::Int(1)))
        );
        assert_eq!(
            decides("SELECT v FROM d WHERE code = 'a'"),
            Some(Some(TableHandle::Int(1)))
        );
        // The handle path does not probe: it hands the plan the handle the
        // constant names, and the row read finds nothing. The index path does
        // probe, because the handle only exists in an index entry.
        assert_eq!(
            decides("SELECT v FROM d WHERE id = 7"),
            Some(Some(TableHandle::Int(7)))
        );
        assert_eq!(decides("SELECT v FROM d WHERE code = 'z'"), Some(None));
        // The index path allows extra pairs beyond the key.
        assert_eq!(
            decides("SELECT v FROM d WHERE code = 'a' AND v = 10"),
            Some(Some(TableHandle::Int(1)))
        );

        // Rejected, so the scan runs: Go requires the handle pair to be the
        // ONLY pair, a conjunction of equalities, no ORDER BY or HAVING, and
        // a LIMIT that cannot drop the row.
        assert_eq!(decides("SELECT v FROM d WHERE id = 1 AND v = 10"), None);
        assert_eq!(decides("SELECT v FROM d WHERE v = 10"), None);
        assert_eq!(decides("SELECT v FROM d WHERE id > 1"), None);
        assert_eq!(decides("SELECT v FROM d WHERE id = 1 OR id = 2"), None);
        assert_eq!(decides("SELECT v FROM d WHERE id = 1 ORDER BY v"), None);
        assert_eq!(decides("SELECT v FROM d WHERE id = 1 LIMIT 0"), None);
        assert_eq!(
            decides("SELECT v FROM d WHERE id = 1 LIMIT 1 OFFSET 1"),
            None
        );
        assert_eq!(decides("SELECT v FROM d"), None);
    }

    /// Index range scans: a comparison on an indexed column reads the rows the
    /// index covers instead of scanning the table, with Go's range semantics.
    #[test]
    fn index_range_scans() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE r (id BIGINT PRIMARY KEY, score BIGINT, KEY score_idx (score))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO r VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let ids = |sql: &str, catalog: &Catalog| {
            let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(value) => value,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect();
            got.sort_unstable();
            got
        };

        assert_eq!(
            ids("SELECT id FROM r WHERE score > 15", &catalog),
            vec![2, 3, 4]
        );
        assert_eq!(
            ids("SELECT id FROM r WHERE score >= 20", &catalog),
            vec![2, 3, 4]
        );
        assert_eq!(
            ids("SELECT id FROM r WHERE score < 30", &catalog),
            vec![1, 2, 4]
        );
        assert_eq!(ids("SELECT id FROM r WHERE score <= 10", &catalog), vec![1]);
        assert_eq!(
            ids("SELECT id FROM r WHERE score = 20", &catalog),
            vec![2, 4]
        );
        // The constant may sit on the left, with the operator flipped.
        assert_eq!(
            ids("SELECT id FROM r WHERE 15 < score", &catalog),
            vec![2, 3, 4]
        );

        // Several conditions on the column intersect into one range.
        assert_eq!(
            ids("SELECT id FROM r WHERE score > 10 AND score < 30", &catalog),
            vec![2, 4]
        );
        assert_eq!(
            ids(
                "SELECT id FROM r WHERE score >= 20 AND score <= 20",
                &catalog
            ),
            vec![2, 4]
        );

        // Go's ranges start at MinNotNull, so a NULL satisfies no comparison
        // -- row 5 never appears, and IS NULL still finds it through the scan.
        assert_eq!(
            ids("SELECT id FROM r WHERE score > -100", &catalog),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            ids("SELECT id FROM r WHERE score IS NULL", &catalog),
            vec![5]
        );

        // A condition the ranges do not consume still filters, because the
        // WHERE stays above the read.
        assert_eq!(
            ids("SELECT id FROM r WHERE score > 15 AND id = 3", &catalog),
            vec![3]
        );

        // Writes are visible to a later range scan, including through the
        // index entries a DELETE removed.
        run_update_on(
            "UPDATE r SET score = 99 WHERE id = 1",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(ids("SELECT id FROM r WHERE score > 50", &catalog), vec![1]);
        run_delete_on(
            "DELETE FROM r WHERE id = 1",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            ids("SELECT id FROM r WHERE score > 50", &catalog),
            Vec::<i64>::new()
        );
    }

    /// A range scan over a UNIQUE index reads its handles out of the entry
    /// VALUES, not the key, so this covers the other half of the entry format
    /// -- including the NULL entries a unique index stores non-distinctly.
    #[test]
    fn index_range_scan_over_a_unique_index() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE u2 (id BIGINT PRIMARY KEY, code BIGINT UNIQUE)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u2 VALUES (1, 100), (2, 200), (3, 300), (4, NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let mut ids: Vec<Datum> = run_select_on(
            "SELECT id FROM u2 WHERE code >= 200",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap()
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
        ids.sort_by_key(|value| match value {
            Datum::Int(v) => *v,
            other => panic!("expected an int, got {other:?}"),
        });
        assert_eq!(ids, vec![Datum::Int(2), Datum::Int(3)]);
        // The NULL row is reachable, just never through a comparison.
        assert_eq!(
            run_select_on(
                "SELECT id FROM u2 WHERE code IS NULL",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(4)]]
        );
    }

    /// The answers above would be right even from a full scan, so this asserts
    /// the DECISION and the ranges themselves.
    #[test]
    fn index_ranges_are_built_the_way_go_builds_them() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE q (id BIGINT PRIMARY KEY, score BIGINT, note VARCHAR(8), KEY s (score))",
            &mut catalog,
        )
        .unwrap();
        let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("q") else {
            panic!("expected a kv table");
        };
        let columns = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.field_type.clone()))
            .collect::<Vec<_>>();
        let ranges = |sql: &str| {
            let stmt = tidb_parser::parse(sql).unwrap();
            let Stmt::Query(query) = &stmt else {
                panic!("not a query")
            };
            let QueryStmt::Select(select) = &**query else {
                panic!("not a select")
            };
            try_index_ranges(select, table, &columns)
        };

        // Go: GT is (v, MaxValue], LT is [MinNotNull, v).
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > 5"),
            Some((
                1,
                vec![IndexRange {
                    low: vec![Datum::Int(5)],
                    high: vec![Datum::MaxValue],
                    low_exclusive: true,
                    high_exclusive: false,
                }]
            ))
        );
        assert_eq!(
            ranges("SELECT id FROM q WHERE score < 5"),
            Some((
                1,
                vec![IndexRange {
                    low: vec![Datum::MinNotNull],
                    high: vec![Datum::Int(5)],
                    low_exclusive: false,
                    high_exclusive: true,
                }]
            ))
        );
        // An intersection keeps the tighter end of each side.
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > 5 AND score <= 9"),
            Some((
                1,
                vec![IndexRange {
                    low: vec![Datum::Int(5)],
                    high: vec![Datum::Int(9)],
                    low_exclusive: true,
                    high_exclusive: false,
                }]
            ))
        );
        // A NULL constant matches nothing, which Go represents as no ranges.
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > NULL"),
            Some((1, vec![]))
        );

        // No usable index: an unindexed column, no WHERE, or an OR.
        assert_eq!(ranges("SELECT id FROM q WHERE note = 'x'"), None);
        assert_eq!(ranges("SELECT id FROM q"), None);
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > 1 OR score < 0"),
            None
        );
    }

    /// Column defaults and the NOT NULL rules, following Go's fillColValue
    /// and CheckNotNull: an omitted column takes its DEFAULT, an omitted NOT
    /// NULL column with no DEFAULT is ErrNoDefaultForField, and an explicit
    /// NULL into a NOT NULL column is the different ErrColumnCantNull.
    #[test]
    fn column_defaults_and_not_null() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE d (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, \
             w BIGINT DEFAULT 7, s VARCHAR(4) DEFAULT 'zz', plain BIGINT)",
            &mut catalog,
        )
        .unwrap();

        // Omitted columns take their defaults; a nullable one with no DEFAULT
        // is NULL.
        run_insert_on(
            "INSERT INTO d (id, n) VALUES (1, 5)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let row = &run_select_on(
            "SELECT w, s, plain FROM d WHERE id = 1",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap()[0];
        assert_eq!(row[0], Datum::Int(7));
        assert_eq!(datum_text_for_test(&row[1]), "zz");
        assert_eq!(row[2], Datum::Null);

        // An explicit value overrides the default.
        run_insert_on(
            "INSERT INTO d (id, n, w) VALUES (2, 5, 100)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT w FROM d WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(100)]]
        );

        // An omitted NOT NULL column with no default is 1364.
        assert!(matches!(
            run_insert_on("INSERT INTO d (id) VALUES (3)", &mut catalog, &crate::StmtContext::for_query()),
            Err(DriverError::NoDefaultForField(name)) if name == "n"
        ));
        // An explicit NULL into that column is the other error, 1048.
        assert!(matches!(
            run_insert_on("INSERT INTO d (id, n) VALUES (3, NULL)", &mut catalog, &crate::StmtContext::for_query()),
            Err(DriverError::ColumnCannotBeNull(name)) if name == "n"
        ));
        // A NULL into a nullable column is fine.
        run_insert_on(
            "INSERT INTO d (id, n, plain) VALUES (3, 5, NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A DEFAULT NULL column is not the same as no DEFAULT: it is
        // omittable even when the column is otherwise unconstrained.
        crate::run_create_table_on(
            "CREATE TABLE e (id BIGINT PRIMARY KEY, v BIGINT DEFAULT NULL)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO e (id) VALUES (1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT v FROM e",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Null]]
        );

        // A primary key is NOT NULL, so omitting it is 1364 as well.
        assert!(matches!(
            run_insert_on(
                "INSERT INTO e (v) VALUES (1)",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::NoDefaultForField(_))
        ));

        // An AUTO_INCREMENT column supplies its own value, so omitting it is
        // never the missing-default case (see the auto_increment test).
        crate::run_create_table_on("CREATE TABLE f (a BIGINT AUTO_INCREMENT)", &mut catalog)
            .unwrap();
        run_insert_on(
            "INSERT INTO f () VALUES ()",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .or_else(|_| {
            run_insert_on(
                "INSERT INTO f VALUES (NULL)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
        })
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a FROM f",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // A generated column is still rejected rather than ignored.
        assert!(crate::run_create_table_on(
            "CREATE TABLE g2 (a BIGINT, b BIGINT GENERATED ALWAYS AS (a+1) VIRTUAL)",
            &mut catalog
        )
        .is_err());
    }

    /// A primary key that is not a single integer column becomes a clustered
    /// COMMON handle: its encoding IS the row key, so rows scan in key order,
    /// the columns live in the key rather than the value, and a repeat is a
    /// duplicate (Go's IsCommonHandle path in addRecord).
    #[test]
    fn clustered_common_handle() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE c (k VARCHAR(8) PRIMARY KEY, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO c VALUES ('b', 2), ('a', 1), ('c', 3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // Key order, not insertion order -- the key IS the primary key.
        assert_eq!(
            run_select_on(
                "SELECT k, v FROM c",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .into_iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect::<Vec<_>>(),
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
        );
        // The key column round-trips even though the value omits it.
        assert_eq!(
            run_select_on(
                "SELECT v FROM c WHERE k = 'b'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        // A repeated key is a duplicate.
        assert!(matches!(
            run_insert_on(
                "INSERT INTO c VALUES ('a', 9)",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::DuplicateEntry { .. })
        ));

        // Writes address the row through its clustered key.
        run_update_on(
            "UPDATE c SET v = 20 WHERE k = 'b'",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT v FROM c WHERE k = 'b'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(20)]]
        );
        run_delete_on(
            "DELETE FROM c WHERE k = 'a'",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT k FROM c",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            2
        );
        // The freed key can be inserted again.
        run_insert_on(
            "INSERT INTO c VALUES ('a', 1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A multi-column primary key is a clustered common handle too.
        crate::run_create_table_on(
            "CREATE TABLE m (a BIGINT, b VARCHAR(4), v BIGINT, PRIMARY KEY (a, b))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO m VALUES (1, 'y', 10), (1, 'x', 20), (2, 'a', 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM m",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .into_iter()
            .map(|row| format!("{:?}/{}", row[0], datum_text_for_test(&row[1])))
            .collect::<Vec<_>>(),
            vec![
                "Int(1)/x".to_owned(),
                "Int(1)/y".to_owned(),
                "Int(2)/a".to_owned()
            ]
        );
        // Only the whole key must be unique; a repeated leading column is fine.
        assert!(matches!(
            run_insert_on(
                "INSERT INTO m VALUES (1, 'x', 99)",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::DuplicateEntry { .. })
        ));
        run_insert_on(
            "INSERT INTO m VALUES (1, 'z', 40)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A secondary index over a clustered table stores the common handle
        // and still resolves to its row.
        crate::run_create_table_on(
            "CREATE TABLE s (k VARCHAR(4) PRIMARY KEY, tag BIGINT, KEY tag_idx (tag))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO s VALUES ('p', 1), ('q', 2)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT k FROM s WHERE tag >= 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .into_iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect::<Vec<_>>(),
            vec!["q".to_owned()]
        );
    }

    /// AUTO_INCREMENT, checked against behavior captured from real TiDB:
    /// inserting 1,2 then an explicit 100 rebases the allocator, so the next
    /// rows are 101, 102, 103 -- NULL and 0 both allocate.
    #[test]
    fn auto_increment() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE a1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a1 (v) VALUES (10), (20)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a1 VALUES (100, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a1 (v) VALUES (40)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a1 VALUES (NULL, 50), (0, 60)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // Captured from TiDB: [[1 10] [2 20] [100 30] [101 40] [102 50] [103 60]]
        assert_eq!(
            run_select_on(
                "SELECT id, v FROM a1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(100), Datum::Int(30)],
                vec![Datum::Int(101), Datum::Int(40)],
                vec![Datum::Int(102), Datum::Int(50)],
                vec![Datum::Int(103), Datum::Int(60)],
            ]
        );

        // TiDB does NOT require the auto column to be a key -- captured, and
        // unlike MySQL, which raises 1075 for it.
        crate::run_create_table_on(
            "CREATE TABLE bad (a BIGINT AUTO_INCREMENT, b BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO bad (b) VALUES (1), (2)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a FROM bad",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );

        // A second auto column is Go's 1075, and a non-integer one is its
        // "Incorrect column specifier" -- both captured from TiDB.
        assert!(matches!(
            crate::run_create_table_on(
                "CREATE TABLE two (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT AUTO_INCREMENT)",
                &mut catalog
            ),
            Err(DriverError::WrongAutoKey)
        ));
        assert!(matches!(
            crate::run_create_table_on(
                "CREATE TABLE strk (a VARCHAR(4) AUTO_INCREMENT PRIMARY KEY)",
                &mut catalog
            ),
            Err(DriverError::WrongColumnSpecifier(_))
        ));
    }

    /// Go's tryWhereIn2BatchPointGet: `col IN (constants)` over the handle or
    /// a single-column unique index reads those rows directly. Results must
    /// match the scan in every case, including the shapes Go rejects.
    #[test]
    fn batch_point_get() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE b (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO b VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let ids = |sql: &str, catalog: &Catalog| {
            let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(value) => value,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect();
            got.sort_unstable();
            got
        };

        // Handle path, including a value that matches nothing.
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (1, 3)", &catalog),
            vec![1, 3]
        );
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (3, 99)", &catalog),
            vec![3]
        );
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (99)", &catalog),
            Vec::<i64>::new()
        );
        // Unique-index path.
        assert_eq!(
            ids("SELECT id FROM b WHERE code IN ('a', 'c')", &catalog),
            vec![1, 3]
        );

        // Shapes Go rejects fall back to the scan and stay correct: NOT IN,
        // a non-key column, and an IN with anything else in the WHERE.
        assert_eq!(
            ids("SELECT id FROM b WHERE id NOT IN (1, 3)", &catalog),
            vec![2]
        );
        assert_eq!(
            ids("SELECT id FROM b WHERE v IN (20, 30)", &catalog),
            vec![2, 3]
        );
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (1, 3) AND v = 30", &catalog),
            vec![3]
        );
        // Go also rejects it with ORDER BY, LIMIT or DISTINCT present.
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (3, 1) ORDER BY id", &catalog),
            vec![1, 3]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM b WHERE id IN (1, 2, 3) LIMIT 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            2
        );
    }

    /// The answers above would be right from a scan too, so this asserts the
    /// DECISION: which shapes Go's batch point get claims.
    #[test]
    fn batch_point_get_is_chosen_only_for_the_shapes_go_accepts() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE bd (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO bd VALUES (1, 'a', 10)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("bd") else {
            panic!("expected a kv table");
        };
        let columns = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.field_type.clone()))
            .collect::<Vec<_>>();
        let decides = |sql: &str| {
            let stmt = tidb_parser::parse(sql).unwrap();
            let Stmt::Query(query) = &stmt else {
                panic!("not a query")
            };
            let QueryStmt::Select(select) = &**query else {
                panic!("not a select")
            };
            try_batch_point_get(select, table, &columns).unwrap()
        };

        assert_eq!(
            decides("SELECT v FROM bd WHERE id IN (1, 2)"),
            Some(vec![TableHandle::Int(1), TableHandle::Int(2)]),
            "the handle path does not probe, as the single point get does not"
        );
        assert_eq!(
            decides("SELECT v FROM bd WHERE code IN ('a', 'zz')"),
            Some(vec![TableHandle::Int(1)]),
            "the index path probes, so a missing key yields no handle"
        );
        // Rejected shapes.
        assert_eq!(decides("SELECT v FROM bd WHERE id NOT IN (1)"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE v IN (1)"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) AND v = 1"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) ORDER BY v"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) LIMIT 1"), None);
        assert_eq!(decides("SELECT DISTINCT v FROM bd WHERE id IN (1)"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE id = 1"), None);
    }

    /// SELECT DISTINCT deduplicates the projected rows, which Go builds as an
    /// aggregation grouping by every projected column with FIRST_ROW
    /// aggregates. The plain path silently returned duplicates before.
    #[test]
    fn select_distinct() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE d2 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO d2 VALUES (1, 1), (1, 2), (1, 1), (2, 2)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a FROM d2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
        // Every projected column takes part, so (1,1) collapses but (1,2)
        // stays.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a, b FROM d2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(1)],
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(2), Datum::Int(2)],
            ]
        );
        // Without DISTINCT every row survives.
        assert_eq!(
            run_select_on(
                "SELECT a FROM d2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            4
        );

        // DISTINCT applies to the projected expression, not the source rows.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a + b FROM d2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(2)],
                vec![Datum::Int(3)],
                vec![Datum::Int(4)]
            ]
        );

        // The dedup emits groups in first-seen order, so a sort below it still
        // orders the surviving rows.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a FROM d2 ORDER BY a DESC",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(1)]]
        );
        // LIMIT applies after the dedup.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a FROM d2 LIMIT 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // A WHERE below it still filters.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a FROM d2 WHERE b = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );

        // Over an aggregate result, DISTINCT deduplicates the output rows.
        crate::run_create_table_on("CREATE TABLE g3 (k BIGINT, v BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO g3 VALUES (1, 5), (2, 5), (3, 9)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT SUM(v) FROM g3 GROUP BY k",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Decimal(tidb_datatype::Decimal::from_int(5))],
                vec![Datum::Decimal(tidb_datatype::Decimal::from_int(9))],
            ]
        );
    }

    /// Non-recursive CTEs: each is materialized in written order and then
    /// resolves like an ordinary table, which is the shape Go's buildWith
    /// plans. The previous behavior was an "unknown table" error.
    #[test]
    fn common_table_expressions() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE c1 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO c1 VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        assert_eq!(
            run_select_on(
                "WITH c AS (SELECT a FROM c1 WHERE a > 1) SELECT a FROM c",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );
        // The outer query filters, orders and aggregates the CTE like a table.
        assert_eq!(
            run_select_on(
                "WITH c AS (SELECT a, b FROM c1) SELECT SUM(b) FROM c WHERE a >= 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(50))]]
        );
        // A column list renames the CTE's columns.
        assert_eq!(
            run_select_on(
                "WITH c (x) AS (SELECT a FROM c1 WHERE a = 3) SELECT x FROM c",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
        // A later CTE may read an earlier one, which is why they are
        // materialized in written order.
        assert_eq!(
            run_select_on(
                "WITH c AS (SELECT a FROM c1 WHERE a > 1), d AS (SELECT a FROM c WHERE a > 2) \
                 SELECT a FROM d",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
        // A CTE and a real table join.
        assert_eq!(
            run_select_on(
                "WITH c AS (SELECT a FROM c1 WHERE a = 2) SELECT c1.b FROM c JOIN c1 ON c.a = c1.a",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(20)]]
        );
        // A CTE shadows a real table of the same name, as in SQL.
        assert_eq!(
            run_select_on(
                "WITH c1 AS (SELECT 9 AS a) SELECT a FROM c1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(9)]]
        );

        // WITH RECURSIVE is rejected rather than run as if it were plain,
        // which would silently return only the seed rows.
        assert!(run_select_on(
            "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) \
             SELECT n FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        // A mismatched column list is an error, not a silent rename of some.
        assert!(run_select_on(
            "WITH c (x, y) AS (SELECT a FROM c1) SELECT x FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }

    /// Set operations, checked against results captured from a running TiDB
    /// for the same data: u1 = 1,2,2,3 and u2 = 2,3,4.
    #[test]
    fn set_operations() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE u1 (a BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE u2 (a BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO u1 VALUES (1), (2), (2), (3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u2 VALUES (2), (3), (4)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let sorted = |sql: &str, catalog: &Catalog| {
            let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(value) => value,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect();
            got.sort_unstable();
            got
        };
        let listed = |sql: &str, catalog: &Catalog| {
            run_select_on(sql, catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(value) => value,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect::<Vec<_>>()
        };

        // Captured: UNION dedups (TiDB returned 4,1,2,3 in hash order, so the
        // comparison sorts); UNION ALL concatenates in term order.
        assert_eq!(
            sorted("SELECT a FROM u1 UNION SELECT a FROM u2", &catalog),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            listed("SELECT a FROM u1 UNION ALL SELECT a FROM u2", &catalog),
            vec![1, 2, 2, 3, 2, 3, 4],
            "captured: UNION ALL keeps duplicates and term order"
        );
        // Captured: EXCEPT -> [1], INTERSECT -> [2, 3] (hash order).
        assert_eq!(
            listed("SELECT a FROM u1 EXCEPT SELECT a FROM u2", &catalog),
            vec![1]
        );
        assert_eq!(
            sorted("SELECT a FROM u1 INTERSECT SELECT a FROM u2", &catalog),
            vec![2, 3]
        );
        // The ALL forms keep multiplicity: u1 has 2 twice, u2 once.
        assert_eq!(
            listed("SELECT a FROM u1 INTERSECT ALL SELECT a FROM u2", &catalog),
            vec![2, 3]
        );
        assert_eq!(
            listed("SELECT a FROM u1 EXCEPT ALL SELECT a FROM u2", &catalog),
            vec![1, 2],
            "one of the two 2s survives EXCEPT ALL"
        );

        // A statement-level ORDER BY and LIMIT apply to the folded result.
        // Captured: ... ORDER BY a DESC -> 4,3,2,1.
        assert_eq!(
            listed(
                "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a DESC",
                &catalog
            ),
            vec![4, 3, 2, 1]
        );
        assert_eq!(
            listed(
                "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a LIMIT 2",
                &catalog
            ),
            vec![1, 2]
        );

        // Three terms fold left to right.
        assert_eq!(
            sorted(
                "SELECT a FROM u1 UNION SELECT a FROM u2 UNION SELECT 9",
                &catalog
            ),
            vec![1, 2, 3, 4, 9]
        );
        // A CTE prefix belongs to the whole statement.
        assert_eq!(
            sorted(
                "WITH c AS (SELECT a FROM u1 WHERE a = 3) \
                 SELECT a FROM c UNION SELECT a FROM u2",
                &catalog
            ),
            vec![2, 3, 4]
        );

        // Captured: a term of a different width is 1222.
        assert!(matches!(
            run_select_on(
                "SELECT a FROM u1 UNION SELECT a, a FROM u2",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::WrongNumberOfColumnsInSelect)
        ));
    }

    #[test]
    fn select_from_table_order_limit() {
        let catalog = test_catalog();
        // ORDER BY a column that is not projected (sort runs below projection).
        assert_eq!(
            run_select_on(
                "SELECT a FROM t ORDER BY b",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(2)],
                vec![Datum::Int(1)]
            ]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM t ORDER BY b DESC LIMIT 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
    }
}

/// The MySQL error a driver failure becomes on the wire, which is also what
/// `SHOW WARNINGS` reports for a failed statement.
///
/// Go attaches the code, the SQLSTATE and the rendered message to the error
/// itself (`terror.Error`), so every surface that reports an error -- the
/// protocol, `SHOW WARNINGS`, the log -- reads the same three fields. This
/// keeps that single source of truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MysqlError {
    /// MySQL error number.
    pub code: u16,
    /// Five-byte SQLSTATE.
    pub state: [u8; 5],
    /// The rendered message.
    pub message: String,
}

impl MysqlError {
    fn new(code: u16, state: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            code,
            state,
            message: message.into(),
        }
    }

    /// Go's catch-all `ER_UNKNOWN_ERROR` (1105), whose SQLSTATE is HY000.
    fn unknown(message: impl Into<String>) -> Self {
        Self::new(1105, *b"HY000", message)
    }
}

/// MySQL `ER_PARSE_ERROR`.
const ER_PARSE_ERROR: u16 = 1064;
/// TiDB `ErrWriteConflict`.
const ER_WRITE_CONFLICT: u16 = 9007;
/// MySQL `ER_UNKNOWN_SYSTEM_VARIABLE`.
const ER_UNKNOWN_SYSTEM_VARIABLE: u16 = 1193;
/// MySQL `ER_INCORRECT_GLOBAL_LOCAL_VAR`.
const ER_INCORRECT_GLOBAL_LOCAL_VAR: u16 = 1238;
/// MySQL `ER_SUBQUERY_NO_1_ROW`.
const ER_SUBQUERY_NO_1_ROW: u16 = 1242;
/// MySQL `ER_DB_CREATE_EXISTS`.
const ER_DB_CREATE_EXISTS: u16 = 1007;
/// MySQL `ER_NO_DB_ERROR`.
const ER_NO_DB_ERROR: u16 = 1046;
/// MySQL `ER_BAD_DB_ERROR`.
const ER_BAD_DB_ERROR: u16 = 1049;

impl DriverError {
    /// The code, SQLSTATE and message this failure reports.
    #[must_use]
    pub fn to_mysql_error(self) -> MysqlError {
        match self {
        DriverError::Parse(message) => MysqlError::new(
            ER_PARSE_ERROR,
            *b"42000",
            format!("You have an error in your SQL syntax: {message}"),
        ),
        DriverError::Unsupported(message) => MysqlError::unknown(message),
        // The `json` error class carries TiDB's own code (3140 malformed
        // document, 3143 malformed path, ...), which applications branch on.
        // Every other eval error is still a porting boundary, not SQL-visible
        // behavior, so it stays the generic unknown-error code.
        DriverError::Exec(ExecError::Eval(crate::EvalError::Json(error))) => {
            MysqlError::new(error.code(), *b"HY000", error.message())
        }
        DriverError::Exec(error) => MysqlError::unknown(format!("{error:?}")),
        DriverError::Txn(crate::TxnErrorKind::WriteConflict) => {
            MysqlError::new(
                ER_WRITE_CONFLICT,
                *b"HY000",
                "Write conflict, please retry the transaction".to_owned(),
            )
        }
        // Go: "The used SELECT statements have a different number of columns".
        DriverError::WrongNumberOfColumnsInSelect => MysqlError::new(
            1222,
            *b"21000",
            "The used SELECT statements have a different number of columns".to_owned(),
        ),
        // Go: "Incorrect table definition; there can be only one auto column
        // and it must be defined as a key".
        DriverError::WrongAutoKey => MysqlError::new(
            1075,
            *b"42000",
            "Incorrect table definition; there can be only one auto column and it must be defined as a key".to_owned(),
        ),
        // Go: "Incorrect column specifier for column '%-.192s'".
        DriverError::WrongColumnSpecifier(name) => MysqlError::new(
            1063,
            *b"42000",
            format!("Incorrect column specifier for column '{name}'"),
        ),
        // Go: "Column '%-.192s' cannot be null".
        DriverError::ColumnCannotBeNull(name) => {
            MysqlError::new(1048, *b"23000", format!("Column '{name}' cannot be null"))
        }
        // Go: "Field '%-.192s' doesn't have a default value".
        DriverError::NoDefaultForField(name) => MysqlError::new(
            1364,
            *b"HY000",
            format!("Field '{name}' doesn't have a default value"),
        ),
        // Go: "Duplicate entry '%-.64s' for key '%-.192s'".
        DriverError::DuplicateEntry { value, key } => MysqlError::new(
            1062,
            *b"23000",
            format!("Duplicate entry '{value}' for key '{key}'"),
        ),
        // Go: "Duplicate key name '%-.192s'".
        DriverError::DuplicateKeyName(name) => {
            MysqlError::new(1061, *b"42000", format!("Duplicate key name '{name}'"))
        }
        // Go: "index %s doesn't exist" -- 1091's index-specific message.
        DriverError::UnknownIndex(name) => {
            MysqlError::new(1091, *b"42000", format!("index {name} doesn't exist"))
        }
        // Go: "Duplicate column name '%-.192s'".
        DriverError::DuplicateColumnName(name) => {
            MysqlError::new(1060, *b"42S21", format!("Duplicate column name '{name}'"))
        }
        // Go: "Can't DROP '%-.192s'; check that column/key exists".
        DriverError::UnknownColumnInAlter(name) => MysqlError::new(
            1091,
            *b"42000",
            format!("Can't DROP '{name}'; check that column/key exists"),
        ),
        // Go: "can't drop only column %s in table %s".
        DriverError::CannotDropOnlyColumn { column, table } => MysqlError::new(
            1090,
            *b"42000",
            format!("can't drop only column {column} in table {table}"),
        ),
        // TiDB: "can't drop column %s with composite index covered or Primary
        // Key covered now".
        DriverError::CannotDropColumnWithCompositeIndex(name) => MysqlError::new(
            8200,
            *b"HY000",
            format!(
                "can't drop column {name} with composite index covered or Primary Key covered now"
            ),
        ),
        // Go: "function %s has only noop implementation in tidb now, use
        // tidb_enable_noop_functions to enable these functions" (1235).
        DriverError::FunctionsNoopImpl(clause) => MysqlError::new(
            1235,
            *b"42000",
            format!(
                "function {clause} has only noop implementation in tidb now, use \
                 tidb_enable_noop_functions to enable these functions"
            ),
        ),
        // TiDB: "Unsupported modify column: %s".
        DriverError::UnsupportedModifyColumn(reason) => MysqlError::new(
            8200,
            *b"HY000",
            format!("Unsupported modify column: {reason}"),
        ),
        // Go: "Incorrect arguments to EXECUTE".
        DriverError::WrongParamCount => MysqlError::new(
            1210,
            *b"HY000",
            "Incorrect arguments to EXECUTE".to_owned(),
        ),
        // Go: "Incorrect arguments to %s".
        DriverError::WrongArguments(function) => MysqlError::new(
            1210,
            *b"HY000",
            format!("Incorrect arguments to {function}"),
        ),
        // Go: "You cannot use the window function '%s' in this context.'"
        // (the trailing quote is in Go's own message text).
        DriverError::WindowInvalidWindowFuncUse(name) => MysqlError::new(
            3593,
            *b"HY000",
            format!("You cannot use the window function '{name}' in this context.'"),
        ),
        // Go: "Window name '%s' is not defined."
        DriverError::WindowNoSuchWindow(name) => MysqlError::new(
            3579,
            *b"HY000",
            format!("Window name '{name}' is not defined."),
        ),
        // Go: "There is a circularity in the window dependency graph."
        DriverError::WindowCircularity => MysqlError::new(
            3580,
            *b"HY000",
            "There is a circularity in the window dependency graph.".to_owned(),
        ),
        // Go: "A window which depends on another cannot define partitioning."
        DriverError::WindowNoChildPartitioning => MysqlError::new(
            3581,
            *b"HY000",
            "A window which depends on another cannot define partitioning.".to_owned(),
        ),
        // Go: "Window '%s' cannot inherit '%s' since both contain an ORDER BY
        // clause." -- an inline `OVER (w ORDER BY ...)` has no name of its
        // own, which Go reports as `<unnamed window>`.
        DriverError::WindowNoRedefineOrderBy(base) => MysqlError::new(
            3583,
            *b"HY000",
            format!(
                "Window '<unnamed window>' cannot inherit '{base}' since both contain an \
                 ORDER BY clause."
            ),
        ),
        // Go: "Window '%s': frame start or end is negative, NULL or of
        // non-integral type" -- an inline `OVER (...)` is `<unnamed window>`.
        DriverError::WindowFrameIllegal => MysqlError::new(
            3586,
            *b"HY000",
            "Window '<unnamed window>': frame start or end is negative, NULL or of \
             non-integral type"
                .to_owned(),
        ),
        // Go: "Window '%s' with RANGE N PRECEDING/FOLLOWING frame requires
        // exactly one ORDER BY expression, of numeric or temporal type".
        DriverError::WindowRangeFrameOrderType => MysqlError::new(
            3587,
            *b"HY000",
            "Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires \
             exactly one ORDER BY expression, of numeric or temporal type"
                .to_owned(),
        ),
        // Go: "Window '%s' with RANGE frame has ORDER BY expression of
        // datetime type. Only INTERVAL bound value allowed."
        DriverError::WindowRangeFrameTemporalType => MysqlError::new(
            3588,
            *b"HY000",
            "Window '<unnamed window>' with RANGE frame has ORDER BY expression of \
             datetime type. Only INTERVAL bound value allowed."
                .to_owned(),
        ),
        // Go: "Window '%s' with RANGE frame has ORDER BY expression of
        // numeric type, INTERVAL bound value not allowed."
        DriverError::WindowRangeFrameNumericType => MysqlError::new(
            3589,
            *b"HY000",
            "Window '<unnamed window>' with RANGE frame has ORDER BY expression of \
             numeric type, INTERVAL bound value not allowed."
                .to_owned(),
        ),
        // Go: "Invalid use of group function".
        DriverError::InvalidGroupFuncUse => MysqlError::new(
            1111,
            *b"HY000",
            "Invalid use of group function".to_owned(),
        ),
        // Go: "Argument #%d of GROUPING function is not in GROUP BY".
        DriverError::FieldInGroupingNotGroupBy(position) => MysqlError::new(
            3602,
            *b"HY000",
            format!("Argument #{position} of GROUPING function is not in GROUP BY"),
        ),
        // Go: "Unknown column '%-.192s' in '%-.192s'".
        DriverError::UnknownColumnInTable { column, table } => MysqlError::new(
            1054,
            *b"42S22",
            format!("Unknown column '{column}' in '{table}'"),
        ),
        // Go: "BLOB/TEXT column '%-.192s' used in key specification without a
        // key length".
        DriverError::BlobKeyWithoutLength(column) => MysqlError::new(
            1170,
            *b"42000",
            format!("BLOB/TEXT column '{column}' used in key specification without a key length"),
        ),
        // Go: "Truncated incorrect %-.32s value: '%-.128s'".
        DriverError::TruncatedIncorrectValue { kind, value } => MysqlError::new(
            1292,
            *b"22007",
            format!("Truncated incorrect {kind} value: '{value}'"),
        ),
        // Go: "Data truncated for column '%s', value is '%s'".
        DriverError::DataTruncatedValue { column, value } => MysqlError::new(
            1265,
            *b"01000",
            format!("Data truncated for column '{column}', value is '{value}'"),
        ),
        // Go: "Data truncated for column '%s' at row %d".
        DriverError::DataTruncatedAtRow { column, row } => MysqlError::new(
            1265,
            *b"01000",
            format!("Data truncated for column '{column}' at row {row}"),
        ),
        // TiDB: "Unsupported drop integer primary key".
        DriverError::UnsupportedDropIntegerPrimaryKey => MysqlError::new(
            8200,
            *b"HY000",
            "Unsupported drop integer primary key".to_owned(),
        ),
        // Go: "Table '%-.192s' already exists".
        DriverError::Schema(crate::SchemaErrorKind::TableExists(name)) => {
            MysqlError::new(1050, *b"42S01", format!("Table '{name}' already exists"))
        }
        // Go: "Unknown table '%-.129s'" -- DROP TABLE's own code, distinct
        // from the 1146 a read of a missing table reports.
        DriverError::Schema(crate::SchemaErrorKind::BadTable(name)) => {
            MysqlError::new(1051, *b"42S02", format!("Unknown table '{name}'"))
        }
        // Go: "Table '%-.192s' doesn't exist".
        DriverError::Schema(crate::SchemaErrorKind::UnknownTable(name)) => {
            MysqlError::new(1146, *b"42S02", format!("Table '{name}' doesn't exist"))
        }
        // Go: "'%-.192s.%-.192s' is not %s".
        DriverError::Schema(crate::SchemaErrorKind::NotView(name)) => {
            MysqlError::new(1347, *b"HY000", format!("'{name}' is not VIEW"))
        }
        // Go: "View '%-.192s.%-.192s' references invalid table(s) ...".
        DriverError::Schema(crate::SchemaErrorKind::ViewInvalid(name)) => MysqlError::new(
            1356,
            *b"HY000",
            format!(
                "View '{name}' references invalid table(s) or column(s) or function(s) or \
                 definer/invoker of view lack rights to use them"
            ),
        ),
        // Go raises this one as a plain error, so it carries 1105.
        DriverError::InsertIntoViewUnsupported(name) => MysqlError::new(
            1105,
            *b"HY000",
            format!("insert into view {name} is not supported now"),
        ),
        DriverError::DeleteViewUnsupported(name) => MysqlError::new(
            1105,
            *b"HY000",
            format!("delete view {name} is not supported now"),
        ),
        // Go: "In definition of view, derived table or common table
        // expression, SELECT list and column names list have different column
        // counts".
        DriverError::ViewWrongList => MysqlError::new(
            1353,
            *b"HY000",
            "In definition of view, derived table or common table expression, SELECT list and \
             column names list have different column counts"
                .to_owned(),
        ),
        // Go: "The target table %-.100s of the %s is not updatable".
        DriverError::TableNotUpdatable(name) => MysqlError::new(
            1288,
            *b"HY000",
            format!("The target table {name} of the UPDATE is not updatable"),
        ),
        // Go `ErrCannotUser` (1396): "Operation %s failed for %.256s", quoted
        // `'user'@'host'` for CREATE USER.
        DriverError::CreateUserAlreadyExists { user, host } => MysqlError::new(
            1396,
            *b"HY000",
            format!("Operation CREATE USER failed for '{user}'@'{host}'"),
        ),
        // Go `ErrCannotUser` (1396): DROP USER prints every failed account
        // through `auth.UserIdentity.String`, unquoted `user@host`, joined
        // by commas.
        DriverError::DropUserMissing { accounts } => MysqlError::new(
            1396,
            *b"HY000",
            format!("Operation DROP USER failed for {accounts}"),
        ),
        // Go: `errors.Errorf("Unknown user: %s", user)` in `RevokeExec.Next`.
        DriverError::RevokeUnknownUser { user, host } => {
            MysqlError::unknown(format!("Unknown user: {user}@{host}"))
        }
        // Go `ErrCantCreateUserWithGrant` (1410).
        DriverError::GrantToUnknownUser => MysqlError::new(
            1410,
            *b"HY000",
            "You are not allowed to create a user with GRANT".to_owned(),
        ),
        // Go `ErrDynamicPrivilegeNotRegistered` (3929).
        DriverError::DynamicPrivilegeNotRegistered(name) => MysqlError::new(
            3929,
            *b"HY000",
            format!("Dynamic privilege '{name}' is not registered with the server."),
        ),
        // Go `ErrNonexistingGrant` (1141).
        DriverError::NonexistingGrant { user, host } => MysqlError::new(
            1141,
            *b"42000",
            format!("There is no such grant defined for user '{user}' on host '{host}'"),
        ),
        // Go `ErrWrongUsage` (1221), `grantDBLevel`'s global-only-privilege
        // check.
        DriverError::DbGrantGlobalOnlyPriv => MysqlError::new(
            1221,
            *b"HY000",
            "Incorrect usage of DB GRANT and GLOBAL PRIVILEGES".to_owned(),
        ),
        // Go `ErrIllegalGrantForTable` (1144).
        DriverError::IllegalGrantForTable => MysqlError::new(
            1144,
            *b"42000",
            "Illegal GRANT/REVOKE command; please consult the manual to see which privileges \
             can be used"
                .to_owned(),
        ),
        // Go: `errors.Errorf("There is no such grant defined for user '%s' \
        // on host '%s' on database %s", ...)` in `RevokeExec.revokeOneUser`.
        DriverError::RevokeNoDbGrant {
            user,
            host,
            database,
        } => MysqlError::unknown(format!(
            "There is no such grant defined for user '{user}' on host '{host}' on database \
             {database}"
        )),
        // Go: the TABLE-scope analogue of `RevokeNoDbGrant`.
        DriverError::RevokeNoTableGrant {
            user,
            host,
            database,
            table,
        } => MysqlError::unknown(format!(
            "There is no such grant defined for user '{user}' on host '{host}' on table \
             {database}.{table}"
        )),
        // Go: "Unknown database '%-.192s'".
        DriverError::Schema(crate::SchemaErrorKind::UnknownDatabase(
            name,
        )) => MysqlError::new(
            ER_BAD_DB_ERROR,
            *b"42000",
            format!("Unknown database '{name}'"),
        ),
        // Go: "Can't create database '%-.192s'; database exists".
        DriverError::Schema(crate::SchemaErrorKind::DatabaseExists(
            name,
        )) => MysqlError::new(
            ER_DB_CREATE_EXISTS,
            *b"HY000",
            format!("Can't create database '{name}'; database exists"),
        ),
        // Go: "No database selected".
        DriverError::Schema(crate::SchemaErrorKind::NoDatabaseSelected) => {
            MysqlError::new(ER_NO_DB_ERROR, *b"3D000", "No database selected".to_owned())
        }
        // Go: "Incorrect argument type to variable '%-.64s'".
        DriverError::Var(crate::VarErrorKind::WrongTypeForVar(name)) => {
            MysqlError::new(
                1232,
                *b"42000",
                format!("Incorrect argument type to variable '{name}'"),
            )
        }
        // Go: "Variable '%-.64s' can't be set to the value of '%-.200s'".
        DriverError::Var(crate::VarErrorKind::WrongValueForVar(
            name,
            value,
        )) => MysqlError::new(
            1231,
            *b"42000",
            format!("Variable '{name}' can't be set to the value of '{value}'"),
        ),
        // Go: "Unknown system variable '%-.64s'".
        DriverError::Var(crate::VarErrorKind::UnknownSystemVariable(
            name,
        )) => MysqlError::new(
            ER_UNKNOWN_SYSTEM_VARIABLE,
            *b"HY000",
            format!("Unknown system variable '{name}'"),
        ),
        // Go: "Variable '%-.192s' is a %s variable".
        DriverError::Var(crate::VarErrorKind::ReadOnlyVariable(name)) => {
            MysqlError::new(
                ER_INCORRECT_GLOBAL_LOCAL_VAR,
                *b"HY000",
                format!("Variable '{name}' is a read only variable"),
            )
        }
        DriverError::SubqueryReturnsMoreThanOneRow => MysqlError::new(
            ER_SUBQUERY_NO_1_ROW,
            *b"21000",
            "Subquery returns more than 1 row".to_owned(),
        ),
        // Go: "Unknown column '%-.192s' in '%-.192s'".
        DriverError::UnknownColumnInClause { column, clause } => MysqlError::new(
            1054,
            *b"42S22",
            format!("Unknown column '{column}' in '{clause}'"),
        ),
        // Go: "Invalid default value for '%-.192s'".
        DriverError::InvalidDefault(column) => MysqlError::new(
            1067,
            *b"42000",
            format!("Invalid default value for '{column}'"),
        ),
        // Go: "Data too long for column '%s' at row %d".
        DriverError::DataTooLong { column, row } => MysqlError::new(
            1406,
            *b"22001",
            format!("Data too long for column '{column}' at row {row}"),
        ),
        // Go: "Out of range value for column '%s' at row %d".
        DriverError::DataOutOfRange { column, row } => MysqlError::new(
            1264,
            *b"22003",
            format!("Out of range value for column '{column}' at row {row}"),
        ),
        // Go: "Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d".
        DriverError::IncorrectValue {
            type_name,
            value,
            column,
            row,
        } => MysqlError::new(
            1366,
            *b"HY000",
            format!("Incorrect {type_name} value: '{value}' for column '{column}' at row {row}"),
        ),
        DriverError::CatalogPoisoned => {
            MysqlError::unknown("the shared catalog is unusable after a failed statement")
        }
        }
    }
}
