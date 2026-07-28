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

//! Driver failures and their MySQL wire representation.
//!
//! [`DriverError`] is the one failure type every driver entry point returns,
//! and [`DriverError::to_mysql_error`] renders it into the code / SQLSTATE /
//! message triple Go attaches to a `terror.Error` -- the single source of
//! truth that the protocol, `SHOW WARNINGS` and the log all read.

use crate::executor::ExecError;

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
    /// Go `ErrJSONUsedAsKey` (3152): a JSON column in an index.
    JsonUsedInKey(String),
    /// Go `ErrBlobCantHaveDefault` (1101): a JSON column's default.
    BlobCantHaveDefault(String),
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
    /// extending window's own name (`<unnamed window>` for an inline `OVER
    /// (w ORDER BY ...)`) and the base's.
    WindowNoRedefineOrderBy {
        /// The window doing the extending, as Go reports it.
        window: String,
        /// The base window it may not inherit from.
        base: String,
    },
    /// Go `plannererrors.ErrWindowNoInheritFrame` (3582): a named window that
    /// defines a frame may not be referenced by another window, carrying its
    /// name.
    WindowNoInheritFrame(String),
    /// Go `plannererrors.ErrNotSupportedYet` (1235) as the window builder
    /// raises it, carrying the feature text Go names.
    NotSupportedYet(&'static str),
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
    /// Go `plannererrors.ErrWrongGroupField` (1056): a `GROUP BY` position
    /// resolves to an aggregate or window-function select field, which
    /// cannot itself be grouped on.
    WrongGroupField(String),
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
    /// Go `types.ErrJSONDocumentNULLKey` (3158): `JSON_OBJECTAGG` evaluated a
    /// NULL member name.
    JsonDocumentNullKey,
    /// Go `typeInfer4ApproxPercentile`'s plain errors (no error class, so
    /// 1105), carrying the message text Go writes.
    ApproxPercentileArgument(&'static str),
    /// Go `typeInfer4ApproxPercentile`: `Percentage value %d is out of range
    /// [1, 100]`, carrying the integer Go printed.
    PercentageOutOfRange(i64),
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
    /// and the body's select list have different widths. A derived table's
    /// own `(c1, c2)` alias column list reports the SAME error when it does
    /// not match the subquery's width (captured).
    ViewWrongList,
    /// Go `plannererrors.ErrInvalidLateralJoin` (3809): a `LATERAL` derived
    /// table in a join shape `buildLateralJoin` refuses. The payload is Go's
    /// own reason text, which the message interpolates.
    InvalidLateralJoin(&'static str),
    /// Go `plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or
    /// CONNECTION_ADMIN")` (1227): `KILL` of a connection logged in as a
    /// DIFFERENT user than the caller, without SUPER (or the dynamic
    /// CONNECTION_ADMIN privilege, not modelled in this tier). Killing one's
    /// own connection is always allowed regardless of privilege.
    KillAccessDenied,
    /// Go `ErrDerivedMustHaveAlias` (1248): a derived table was written
    /// without an alias. Captured from Go for both a plain `SELECT` and a
    /// view body.
    DerivedMustHaveAlias,
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
    /// Go `ErrCannotUser` (1396): `ALTER USER ... IDENTIFIED BY` named an
    /// account that does not exist, without `IF EXISTS`. Quoted
    /// `'user'@'host'`, like CREATE USER's form and unlike DROP USER's.
    AlterUserMissing {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrCannotUser` (1396) for `RENAME USER`, whose message carries a
    /// trailing reason clause rather than just the account
    /// (captured: `... failed for nosuch@% TO x@% old did not exist`, and
    /// `... new did exist` when the target identity is taken).
    RenameUserFailed {
        /// The source account username.
        old_user: String,
        /// The source account host.
        old_host: String,
        /// The target account username.
        new_user: String,
        /// The target account host.
        new_host: String,
        /// Whether the SOURCE account was missing (as opposed to the target
        /// identity already existing), which selects the reason clause.
        old_missing: bool,
    },
    /// Go `ErrPasswordNoMatch` (1133): `SET PASSWORD FOR` named an account
    /// with no `mysql.user` row. `SET PASSWORD` does NOT reuse
    /// `ErrCannotUser` (captured).
    SetPasswordNoMatchingRow,
    /// Go `ErrPluginIsNotLoaded` (1524): `CREATE`/`ALTER USER ... IDENTIFIED
    /// WITH <plugin>` named a plugin that is neither one of Go's built-in
    /// `CREATE USER`-accepted plugins (`mysql_native_password`,
    /// `caching_sha2_password`, `tidb_sm3_password`, `auth_socket`,
    /// `tidb_auth_token`, `authentication_ldap_simple`,
    /// `authentication_ldap_sasl`) nor a registered extension auth plugin
    /// (which this tier has none of).
    PluginIsNotLoaded {
        /// The unrecognized plugin name, as written.
        plugin: String,
    },
    /// Go `ErrPasswordFormat` (1827): an `IDENTIFIED WITH <plugin> AS
    /// '<hash>'` credential is not shaped like that plugin's stored
    /// `authentication_string` -- captured: `mysql_native_password` needs
    /// exactly `*` + 40 hex digits, `caching_sha2_password`/
    /// `tidb_sm3_password` need exactly 70 bytes, and `tidb_auth_token`'s
    /// `AS` form is refused outright (Go's `encodedPassword` has no case for
    /// it, so it falls to the same `default: return "", false`).
    PasswordFormat,
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
    /// Go `ErrCannotUser` (1396) raised by a ROLE statement. Each of them
    /// names its own operation and formats the offending identity the way
    /// that statement's Go code does, so both travel together rather than
    /// being guessed at render time (all captured):
    /// `CREATE ROLE` quotes `'r'@'h'`, `DROP ROLE`/`GRANT ROLE`/`REVOKE ROLE`
    /// print an account bare as `u@h`, and `REVOKE ROLE` prints a missing
    /// ROLE backtick-quoted as ``\`r\`@\`h\`` (`auth.RoleIdentity.String`).
    CannotUserRole {
        /// The operation name the message reports, e.g. `CREATE ROLE`.
        operation: &'static str,
        /// The already-formatted identity (or comma-joined identities).
        target: String,
    },
    /// Go `ErrGrantRole` (3523): `GRANT <role> TO ...` named a role that has
    /// no account row at all.
    GrantUnknownRole {
        /// The role name.
        role: String,
        /// The role host.
        host: String,
    },
    /// Go `ErrRoleNotGranted` (3530): `SET ROLE`/`SET DEFAULT ROLE` named a
    /// role that is not granted DIRECTLY to the account. A role held only
    /// indirectly (granted to a role the account holds) lands here too --
    /// activation never walks the graph.
    RoleNotGranted {
        /// The role name.
        role: String,
        /// The role host.
        role_host: String,
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrDynamicPrivilegeNotRegistered` (3929): a `GRANT`/`REVOKE`
    /// privilege name is not one of the standard static privileges and is
    /// not a registered dynamic privilege either.
    DynamicPrivilegeNotRegistered(String),
    /// Go `exeerrors.ErrIllegalPrivilegeLevel` (3619): a DYNAMIC privilege
    /// was named at DATABASE or TABLE scope, which Go rejects before it
    /// checks whether the privilege is registered at all. `GRANT` names the
    /// offending privilege; `REVOKE` names every dynamic privilege in the
    /// statement, comma-joined.
    IllegalPrivilegeLevel(String),
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
            ExecError::JsonDocumentNullKey => DriverError::JsonDocumentNullKey,
            other => DriverError::Exec(other),
        }
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
        DriverError::WindowNoRedefineOrderBy { window, base } => MysqlError::new(
            3583,
            *b"HY000",
            format!(
                "Window '{window}' cannot inherit '{base}' since both contain an \
                 ORDER BY clause."
            ),
        ),
        // Go: "Window '%s' has a frame definition, so cannot be referenced by
        // another window."
        DriverError::WindowNoInheritFrame(base) => MysqlError::new(
            3582,
            *b"HY000",
            format!(
                "Window '{base}' has a frame definition, so cannot be referenced by \
                 another window."
            ),
        ),
        // Go: "This version of TiDB doesn't yet support '%s'".
        DriverError::NotSupportedYet(feature) => MysqlError::new(
            1235,
            *b"42000",
            format!("This version of TiDB doesn't yet support '{feature}'"),
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
        // Go: "JSON column '%-.192s' cannot be used in key specification."
        DriverError::JsonUsedInKey(column) => MysqlError::new(
            3152,
            *b"42000",
            format!("JSON column '{column}' cannot be used in key specification."),
        ),
        // Go: "BLOB/TEXT/JSON column '%-.192s' can't have a default value".
        DriverError::BlobCantHaveDefault(column) => MysqlError::new(
            1101,
            *b"42000",
            format!("BLOB/TEXT/JSON column '{column}' can't have a default value"),
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
        // Go: "JSON documents may not contain NULL member names."
        DriverError::JsonDocumentNullKey => MysqlError::new(
            3158,
            *b"22032",
            "JSON documents may not contain NULL member names.".to_owned(),
        ),
        // Go raises these with a bare `errors.New`/`fmt.Errorf`, so they carry
        // no error class and reach the client as 1105.
        DriverError::ApproxPercentileArgument(message) => {
            MysqlError::new(1105, *b"HY000", (*message).to_owned())
        }
        DriverError::PercentageOutOfRange(percent) => MysqlError::new(
            1105,
            *b"HY000",
            format!("Percentage value {percent} is out of range [1, 100]"),
        ),
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
        // Go `ErrInvalidLateralJoin`: "Invalid use of LATERAL: %s".
        DriverError::InvalidLateralJoin(reason) => MysqlError::new(
            3809,
            *b"HY000",
            format!("Invalid use of LATERAL: {reason}"),
        ),
        // Go: "Every derived table must have its own alias".
        DriverError::DerivedMustHaveAlias => MysqlError::new(
            1248,
            *b"42000",
            "Every derived table must have its own alias".to_owned(),
        ),
        // Go: "The target table %-.100s of the %s is not updatable".
        DriverError::TableNotUpdatable(name) => MysqlError::new(
            1288,
            *b"HY000",
            format!("The target table {name} of the UPDATE is not updatable"),
        ),
        // Go `ErrSpecificAccessDenied` (1227), `planbuilder.go`'s
        // `*ast.KillStmt` case.
        DriverError::KillAccessDenied => MysqlError::new(
            1227,
            *b"42000",
            "Access denied; you need (at least one of) the SUPER or CONNECTION_ADMIN \
             privilege(s) for this operation"
                .to_owned(),
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
        // Go `ErrCannotUser` (1396) for ALTER USER, quoted like CREATE USER.
        DriverError::AlterUserMissing { user, host } => MysqlError::new(
            1396,
            *b"HY000",
            format!("Operation ALTER USER failed for '{user}'@'{host}'"),
        ),
        // Go `ErrCannotUser` (1396) for RENAME USER: unquoted `user@host` on
        // both sides plus the reason clause.
        DriverError::RenameUserFailed {
            old_user,
            old_host,
            new_user,
            new_host,
            old_missing,
        } => MysqlError::new(
            1396,
            *b"HY000",
            format!(
                "Operation RENAME USER failed for {old_user}@{old_host} TO \
                 {new_user}@{new_host} {}",
                if old_missing {
                    "old did not exist"
                } else {
                    "new did exist"
                }
            ),
        ),
        // Go `ErrPasswordNoMatch` (1133).
        DriverError::SetPasswordNoMatchingRow => MysqlError::new(
            1133,
            *b"42000",
            "Can't find any matching row in the user table".to_owned(),
        ),
        // Go `ErrPluginIsNotLoaded` (1524).
        DriverError::PluginIsNotLoaded { plugin } => MysqlError::new(
            1524,
            *b"HY000",
            format!("Plugin '{plugin}' is not loaded"),
        ),
        // Go `ErrPasswordFormat` (1827).
        DriverError::PasswordFormat => MysqlError::new(
            1827,
            *b"HY000",
            "The password hash doesn't have the expected format. Check if the correct \
             password algorithm is being used with the PASSWORD() function."
                .to_owned(),
        ),
        // Go: `errors.Errorf("Unknown user: %s", user)` in `RevokeExec.Next`.
        DriverError::RevokeUnknownUser { user, host } => {
            MysqlError::unknown(format!("Unknown user: {user}@{host}"))
        }
        // Go `ErrCannotUser` (1396) for every ROLE statement; the caller
        // already formatted the identity the way that statement does.
        DriverError::CannotUserRole { operation, target } => MysqlError::new(
            1396,
            *b"HY000",
            format!("Operation {operation} failed for {target}"),
        ),
        // Go `ErrGrantRole` (3523).
        DriverError::GrantUnknownRole { role, host } => MysqlError::new(
            3523,
            *b"HY000",
            format!("Unknown authorization ID `{role}`@`{host}`"),
        ),
        // Go `ErrRoleNotGranted` (3530): the role is backtick-quoted
        // (`RoleIdentity.String`) and the account bare (`UserIdentity.String`).
        DriverError::RoleNotGranted {
            role,
            role_host,
            user,
            host,
        } => MysqlError::new(
            3530,
            *b"HY000",
            format!("`{role}`@`{role_host}` is not granted to {user}@{host}"),
        ),
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
        // Go `ErrIllegalPrivilegeLevel` (3619).
        DriverError::IllegalPrivilegeLevel(names) => MysqlError::new(
            3619,
            *b"HY000",
            format!("Illegal privilege level specified for {names}"),
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
        // Go: "Can't group on '%-.192s'".
        DriverError::WrongGroupField(field) => MysqlError::new(
            1056,
            *b"42000",
            format!("Can't group on '{field}'"),
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
