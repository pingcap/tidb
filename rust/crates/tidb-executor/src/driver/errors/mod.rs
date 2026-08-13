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

mod driver_error;
mod exec;
mod schema;
mod txn;
mod var;

pub use driver_error::DriverError;
pub use schema::SchemaErrorKind;
pub use txn::TxnErrorKind;
pub use var::VarErrorKind;

impl From<ExecError> for DriverError {
    fn from(err: ExecError) -> Self {
        match err {
            // The same statement-level error whichever layer raised it, so
            // callers match one variant.
            ExecError::SubqueryReturnsMoreThanOneRow => DriverError::SubqueryReturnsMoreThanOneRow,
            ExecError::MemoryExceedForQuery { conn_id } => {
                DriverError::MemoryExceedForQuery { conn_id }
            }
            ExecError::JsonDocumentNullKey => DriverError::JsonDocumentNullKey,
            ExecError::InvalidJsonCharset { charset } => {
                DriverError::InvalidJsonCharset { charset }
            }
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

    /// The SQLSTATE a code carries, rather than one supplied beside it.
    ///
    /// Go never writes the two down together: `NewErr` looks the code up in
    /// `MySQLState` and falls back to `HY000`
    /// (`pkg/parser/mysql/error.go:40-57`). Spelling the state out at the
    /// raise site is what lets a code and its state disagree -- `1365` was
    /// reaching clients as `HY000` where TiDB sends `22012` -- and a derived
    /// state cannot drift. Every entry in the table is five bytes, so this is
    /// total.
    fn coded(code: u16, message: impl Into<String>) -> Self {
        let mut state = [0u8; 5];
        state.copy_from_slice(tidb_error::mysql::mysql_state(code).as_bytes());
        Self::new(code, state, message)
    }

    /// Go's catch-all `ER_UNKNOWN_ERROR` (1105), whose SQLSTATE is HY000.
    fn unknown(message: impl Into<String>) -> Self {
        Self::new(1105, *b"HY000", message)
    }
}

/// MySQL `ER_PARSE_ERROR`.
const ER_PARSE_ERROR: u16 = 1064;
/// TiDB `ErrWriteConflict`.
const ER_WRITE_CONFLICT: u16 = tidb_error::tidb::errcode::ErrWriteConflict;
/// TiDB `ErrRegionUnavailable`.
const ER_REGION_UNAVAILABLE: u16 = tidb_error::tidb::errcode::ErrRegionUnavailable;
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
        // Every execution and evaluation failure, one arm each, in `exec`.
        DriverError::Exec(error) => exec::to_mysql_error(error),
        DriverError::Txn(crate::TxnErrorKind::WriteConflict) => {
            MysqlError::new(
                ER_WRITE_CONFLICT,
                *b"HY000",
                "Write conflict, please retry the transaction".to_owned(),
            )
        }
        DriverError::Txn(crate::TxnErrorKind::RegionUnavailable) => MysqlError::new(
            ER_REGION_UNAVAILABLE,
            *b"HY000",
            tidb_error::tidb::errname::ErrRegionUnavailable.raw.to_owned(),
        ),
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
        // Go: "Incorrect column name '%-.100s'".
        DriverError::WrongColumnName(name) => MysqlError::new(
            1166,
            *b"42000",
            format!("Incorrect column name '{name}'"),
        ),
        // Go: "A primary key index cannot be invisible".
        DriverError::PrimaryKeyCantBeInvisible => MysqlError::new(
            3522,
            *b"HY000",
            "A primary key index cannot be invisible".to_owned(),
        ),
        // Go: "Key '%-.192s' doesn't exist in table '%-.192s'".
        DriverError::KeyNotExists { key, table } => MysqlError::new(
            1176,
            *b"42000",
            format!("Key '{key}' doesn't exist in table '{table}'"),
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
        // Go: "Incorrect foreign key definition for '%-.192s': %s".
        DriverError::WrongFkDef { name, reason } => MysqlError::new(
            1239,
            *b"42000",
            format!("Incorrect foreign key definition for '{name}': {reason}"),
        ),
        // Go: "Cannot add or update a child row: a foreign key constraint
        // fails (%.192s)".
        DriverError::ForeignKeyNoReferencedRow { table, constraint } => MysqlError::new(
            1452,
            *b"23000",
            format!(
                "Cannot add or update a child row: a foreign key constraint fails ({table}, {constraint})"
            ),
        ),
        // Go: "Cannot delete or update a parent row: a foreign key
        // constraint fails (%.192s)".
        DriverError::ForeignKeyRowIsReferenced { table, constraint } => MysqlError::new(
            1451,
            *b"23000",
            format!(
                "Cannot delete or update a parent row: a foreign key constraint fails ({table}, {constraint})"
            ),
        ),
        // Go: "Foreign key '%s' uses virtual column '%s' which is not
        // supported.". Captured via `gorun`: `[schema:3733]`.
        DriverError::ForeignKeyUsesVirtualColumn {
            foreign_key,
            column,
        } => MysqlError::new(
            3733,
            *b"HY000",
            format!("Foreign key '{foreign_key}' uses virtual column '{column}' which is not supported."),
        ),
        // Go: "Cannot define foreign key with %s clause on a generated
        // column.". Captured via `gorun`: `[ddl:3104]`.
        DriverError::WrongFkOptionForGeneratedColumn { clause } => MysqlError::new(
            3104,
            *b"HY000",
            format!("Cannot define foreign key with {clause} clause on a generated column."),
        ),
        // Go: "Cannot drop index '%-.192s': needed in a foreign key
        // constraint". Captured via `gorun`: `[ddl:1553]`.
        DriverError::DropIndexNeededInForeignKey(index) => MysqlError::new(
            1553,
            *b"HY000",
            format!("Cannot drop index '{index}': needed in a foreign key constraint"),
        ),
        // Go: "Referencing column '%s' and referenced column '%s' in foreign
        // key constraint '%s' are incompatible.". Captured: `[ddl:3780]`.
        DriverError::FkIncompatibleColumns {
            referencing,
            referenced,
            constraint,
        } => MysqlError::new(
            3780,
            *b"HY000",
            format!(
                "Referencing column '{referencing}' and referenced column '{referenced}' in \
                 foreign key constraint '{constraint}' are incompatible."
            ),
        ),
        // Go: "Cannot change column '%-.192s': used in a foreign key
        // constraint '%-.192s'". Captured: `[ddl:1832]`.
        DriverError::ForeignKeyColumnCannotChange { column, constraint } => MysqlError::new(
            1832,
            *b"HY000",
            format!("Cannot change column '{column}': used in a foreign key constraint '{constraint}'"),
        ),
        // Go: "Cannot change column '%-.192s': used in a foreign key
        // constraint '%-.192s' of table '%-.192s'". Captured: `[ddl:1833]`.
        DriverError::ForeignKeyColumnCannotChangeChild {
            column,
            constraint,
            child_table,
        } => MysqlError::new(
            1833,
            *b"HY000",
            format!(
                "Cannot change column '{column}': used in a foreign key constraint \
                 '{constraint}' of table '{child_table}'"
            ),
        ),
        // Go: "Duplicate foreign key constraint name '%s'".
        DriverError::FkDupName(name) => MysqlError::new(
            1826,
            *b"HY000",
            format!("Duplicate foreign key constraint name '{name}'"),
        ),
        // Go: "Foreign key cascade delete/update exceeds max depth of %v.".
        DriverError::ForeignKeyCascadeTooDeep => MysqlError::new(
            3008,
            *b"HY000",
            "Foreign key cascade delete/update exceeds max depth of 15.".to_owned(),
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
        // Go: "Multiple primary key defined".
        DriverError::MultiplePrimaryKey => {
            MysqlError::new(1068, *b"42000", "Multiple primary key defined".to_owned())
        }
        // Go: "Too-big precision %d specified for '%-.192s'. Maximum is %d."
        DriverError::TooBigPrecision {
            precision,
            column,
            maximum,
        } => MysqlError::new(
            1426,
            *b"42000",
            format!("Too-big precision {precision} specified for '{column}'. Maximum is {maximum}."),
        ),
        // Go: "For float(M,D), double(M,D) or decimal(M,D), M must be >= D
        // (column '%s')."
        DriverError::MBiggerThanD(column) => MysqlError::new(
            1427,
            *b"42000",
            format!(
                "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '{column}')."
            ),
        ),
        // Go: "Column '%-.100s' has duplicated value '%-.64s' in %s".
        DriverError::DuplicatedValueInType {
            column,
            value,
            type_name,
        } => MysqlError::new(
            1291,
            *b"HY000",
            format!("Column '{column}' has duplicated value '{value}' in {type_name}"),
        ),
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
        // TiDB: "Unsupported modify column: change from original type %v to
        // %v is currently unsupported yet" (the message's INNER wrap; see the
        // variant doc for the double-wrap Go's caller adds on top).
        DriverError::UnsupportedModifyColumnType { from, to } => MysqlError::new(
            8200,
            *b"HY000",
            format!(
                "Unsupported modify column: change from original type {from} to {to} is \
                 currently unsupported yet"
            ),
        ),
        // Go `ErrSpDoesNotExist`: "%s %s does not exist", which
        // `executeReleaseSavepoint` and the ROLLBACK TO path both fill in
        // with the literal "SAVEPOINT" and the name.
        DriverError::SavepointNotExists(name) => MysqlError::new(
            1305,
            *b"42000",
            format!("SAVEPOINT {name} does not exist"),
        ),
        // Go `ClassAdmin.NewStd(errno.ErrAdminCheckTable)`: 8003, HY000. The
        // detail is Go's own "table count %d != index(%s) count %d".
        DriverError::AdminCheckTable(detail) => MysqlError::new(8003, *b"HY000", detail),
        // Go `ClassAdmin.NewStd(errno.ErrDataInconsistent)`: 8223, HY000.
        DriverError::DataInconsistent(detail) => MysqlError::new(8223, *b"HY000", detail),
        DriverError::DataInconsistentMismatchIndex(detail) => {
            MysqlError::new(8134, *b"HY000", detail)
        }
        // Captured from TiDB: both `EXECUTE stmt` with a marker left unbound
        // and `EXECUTE stmt USING @a` with no marker report
        // `[planner:8112]Wrong parameter count`, not 1210 -- the check is
        // `planCachePreprocess`'s step 1, shared by the SQL-level EXECUTE and
        // the binary protocol.
        DriverError::WrongParamCount => {
            MysqlError::new(8112, *b"HY000", "Wrong parameter count".to_owned())
        }
        // Go: "Prepared statement not found".
        DriverError::PreparedStmtNotFound => {
            MysqlError::new(8111, *b"HY000", "Prepared statement not found".to_owned())
        }
        // Go: "Can not prepare multiple statements".
        DriverError::PrepareMulti => MysqlError::new(
            8115,
            *b"HY000",
            "Can not prepare multiple statements".to_owned(),
        ),
        DriverError::UnsupportedPreparedStatement => MysqlError::new(
            1295,
            *b"HY000",
            "This command is not supported in the prepared statement protocol yet".to_owned(),
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
        // Go: "Window '%s': frame start cannot be UNBOUNDED FOLLOWING." --
        // `checkOriginWindowSpec`'s FIRST rule, so it outranks every other
        // frame complaint including a malformed end offset.
        DriverError::WindowFrameStartIllegal => MysqlError::new(
            3584,
            *b"HY000",
            "Window '<unnamed window>': frame start cannot be UNBOUNDED FOLLOWING.".to_owned(),
        ),
        // Go: "Window '%s': frame end cannot be UNBOUNDED PRECEDING."
        DriverError::WindowFrameEndIllegal => MysqlError::new(
            3585,
            *b"HY000",
            "Window '<unnamed window>': frame end cannot be UNBOUNDED PRECEDING.".to_owned(),
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
        // Go: "Incorrect prefix key; ...". The message names nothing, which
        // is why the variant carries nothing.
        DriverError::IncorrectPrefixKey => MysqlError::new(
            1089,
            *b"HY000",
            "Incorrect prefix key; the used key part isn't a string, the used length is longer \
             than the key part, or the storage engine doesn't support unique prefix keys"
                .to_owned(),
        ),
        // Go: "Key part '%-.192s' length cannot be 0".
        DriverError::KeyPart0(column) => MysqlError::new(
            1391,
            *b"HY000",
            format!("Key part '{column}' length cannot be 0"),
        ),
        // Go: "The used storage engine can't index column '%-.192s'".
        DriverError::WrongKeyColumn(column) => MysqlError::new(
            1167,
            *b"42000",
            format!("The used storage engine can't index column '{column}'"),
        ),
        // Go: "Specified key was too long (%d bytes); max key length is %d
        // bytes".
        DriverError::TooLongKey { length, max } => MysqlError::new(
            1071,
            *b"42000",
            format!("Specified key was too long ({length} bytes); max key length is {max} bytes"),
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
        // Go: "Data Too Long, field len %d, data len %d".
        DriverError::DataTooLongRaw {
            field_len,
            data_len,
        } => MysqlError::new(
            1406,
            *b"22001",
            format!("Data Too Long, field len {field_len}, data len {data_len}"),
        ),
        // Go's message TEMPLATE, unformatted -- see the variant's doc.
        DriverError::DataTruncatedUnformatted => MysqlError::new(
            1265,
            *b"01000",
            "Data truncated for column '%s' at row %d".to_owned(),
        ),
        // Go: "Incorrect %-.32s value: '%-.128s'".
        DriverError::IncorrectValueRaw { type_name, value } => MysqlError::new(
            1292,
            *b"22007",
            format!("Incorrect {type_name} value: '{value}'"),
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
        // Go: "Error on rename of '%-.210s' to '%-.210s' (errno: %d - %s)",
        // whose nested errno is the fixed 168 `ExtractTblInfos` passes.
        DriverError::Schema(crate::SchemaErrorKind::RenameTargetDatabaseMissing {
            from,
            to,
            database,
        }) => MysqlError::new(
            1025,
            *b"HY000",
            format!(
                "Error on rename of '{from}' to '{to}' (errno: 168 - Database `{database}` doesn't exist)"
            ),
        ),
        DriverError::Schema(crate::SchemaErrorKind::UnknownSequence(name)) => {
            MysqlError::new(4139, *b"HY000", format!("Unknown SEQUENCE: '{name}'"))
        }
        DriverError::Schema(crate::SchemaErrorKind::SequenceValuesConflicting(name)) => {
            MysqlError::new(
                4136,
                *b"HY000",
                format!("Sequence '{name}' values are conflicting"),
            )
        }
        // Go: "'%-.192s.%-.192s' is not %s".
        DriverError::Schema(crate::SchemaErrorKind::WrongObject { name, expected }) => {
            MysqlError::new(1347, *b"HY000", format!("'{name}' is not {expected}"))
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
        // Go `exeerrors.ErrMemoryExceedForQuery`. The text is NOT retyped
        // here: it is rendered from the ported error catalog, the same way
        // Go's `SQLKiller.getKillError` renders it, so the wire message can
        // only drift if the catalog does. Captured from Go:
        //   [executor:8175]Your query has been cancelled due to exceeding the
        //   allowed memory limit for a single SQL query. Please try narrowing
        //   your query scope or increase the tidb_mem_quota_query limit and
        //   try again.[conn=1]
        DriverError::MemoryExceedForQuery { conn_id } => {
            let sql_error = crate::mem_quota::memory_exceed_for_query(conn_id);
            let mut state = *b"HY000";
            state.copy_from_slice(sql_error.state.as_bytes());
            MysqlError::new(sql_error.code, state, sql_error.message)
        }
        // Go raises this one as a plain error, so it carries 1105.
        // Go: "JSON documents may not contain NULL member names."
        DriverError::JsonDocumentNullKey => MysqlError::new(
            3158,
            *b"22032",
            "JSON documents may not contain NULL member names.".to_owned(),
        ),
        // Go: "Cannot create a JSON value from a string with CHARACTER SET
        // '%s'." (`types.ErrInvalidJSONCharset`, captured verbatim from a
        // BINARY-charset `JSON_OBJECTAGG` key).
        DriverError::InvalidJsonCharset { charset } => MysqlError::new(
            3144,
            *b"22032",
            format!("Cannot create a JSON value from a string with CHARACTER SET '{charset}'."),
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
        DriverError::CheckConstraintNotExists(name) => MysqlError::new(
            3940,
            *b"HY000",
            format!("Constraint '{name}' does not exist."),
        ),
        // Go's own wording, including the colon with no following space.
        DriverError::BindingHintedSqlMismatch { origin, hinted } => MysqlError::new(
            1105,
            *b"HY000",
            format!(
                "hinted sql and origin sql don't match when hinted sql erase the hint info, \
                 after erase hint info, originSQL:{origin}, hintedSQL:{hinted}"
            ),
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
        DriverError::InsertIntoSequenceUnsupported(name) => MysqlError::new(
            1105,
            *b"HY000",
            format!("insert into sequence {name} is not supported now"),
        ),
        DriverError::DeleteSequenceUnsupported(name) => MysqlError::new(
            1105,
            *b"HY000",
            format!("delete sequence {name} is not supported now"),
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
        DriverError::CteRecursiveRequiresUnion(name) => MysqlError::new(
            3573,
            *b"HY000",
            format!("Recursive Common Table Expression '{name}' should contain a UNION"),
        ),
        DriverError::CteRecursiveRequiresNonRecursiveFirst(name) => MysqlError::new(
            3574,
            *b"HY000",
            format!(
                "Recursive Common Table Expression '{name}' should have one or more \
                 non-recursive query blocks followed by one or more recursive ones"
            ),
        ),
        DriverError::CteRecursiveForbidsAggregation(name) => MysqlError::new(
            3575,
            *b"HY000",
            format!(
                "Recursive Common Table Expression '{name}' can contain neither aggregation \
                 nor window functions in recursive query block"
            ),
        ),
        DriverError::CteRecursiveForbiddenJoinOrder(name) => MysqlError::new(
            3577,
            *b"HY000",
            format!(
                "In recursive query block of Recursive Common Table Expression '{name}', the \
                 recursive table must be referenced only once, and not in any subquery"
            ),
        ),
        DriverError::CteMaxRecursionDepth(rounds) => MysqlError::new(
            3636,
            *b"HY000",
            format!(
                "Recursive query aborted after {rounds} iterations. Try increasing \
                 @@cte_max_recursion_depth to a larger value"
            ),
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
        // Go `ErrSpecificAccessDenied` (1227), the general form.
        DriverError::SpecificAccessDenied(privileges) => MysqlError::new(
            1227,
            *b"42000",
            format!(
                "Access denied; you need (at least one of) the {privileges} \
                 privilege(s) for this operation"
            ),
        ),
        // Go `ErrDBaccessDenied` (1044).
        DriverError::DbAccessDenied {
            user,
            host,
            database,
        } => MysqlError::new(
            1044,
            *b"42000",
            format!("Access denied for user '{user}'@'{host}' to database '{database}'"),
        ),
        // Go `ErrTableaccessDenied` (1142).
        DriverError::TableAccessDenied {
            privilege,
            user,
            host,
            table,
        } => MysqlError::new(
            1142,
            *b"42000",
            format!("{privilege} command denied to user '{user}'@'{host}' for table '{table}'"),
        ),
        // Go `ErrPrivilegeCheckFail` (8121). Go's message deliberately
        // begins lowercase.
        DriverError::PrivilegeCheckFail(privilege) => MysqlError::new(
            8121,
            *b"HY000",
            format!("privilege check for '{privilege}' fail"),
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
        // Go `types.ErrWrongValue2` (1525) with the `DAY` unit name, the
        // error `loadOptions` raises for a zero or > 65535 interval.
        DriverError::PasswordExpireIntervalOutOfRange { days } => MysqlError::new(
            1525,
            *b"HY000",
            format!("Incorrect DAY value: '{days}'"),
        ),
        // Go `errno.ErrMustChangePassword` (1820), the sandbox-mode gate.
        DriverError::MustChangePassword => MysqlError::new(
            1820,
            *b"HY000",
            "You must reset your password using ALTER USER statement before executing this statement"
                .to_owned(),
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
        // Go `variable.ErrNotValidPassword` (1819).
        DriverError::NotValidPassword { reason } => MysqlError::new(
            1819,
            *b"HY000",
            format!(
                "Your password does not satisfy the current policy requirements ({reason})"
            ),
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
        // Go `ErrCantCreateUserWithGrant` (1410), SQLSTATE 42000.
        DriverError::GrantToUnknownUser => MysqlError::coded(
            1410,
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
        // Go `ErrWrongUsage` (1221), `GrantExec.Next`'s column-list check.
        DriverError::ColumnGrantNonColumnPriv => MysqlError::new(
            1221,
            *b"HY000",
            "Incorrect usage of COLUMN GRANT and NON-COLUMN PRIVILEGES".to_owned(),
        ),
        // Go: `errors.Errorf("Unknown column: %s", ...)`.
        DriverError::UnknownGrantColumn(column) => {
            MysqlError::unknown(format!("Unknown column: {column}"))
        }
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
        // Go `ErrLocalVariable` (1228): "Variable '%-.64s' is a SESSION
        // variable and can't be used with SET GLOBAL".
        DriverError::Var(crate::VarErrorKind::SessionOnlyVariable(name)) => MysqlError::new(
            1228,
            *b"HY000",
            format!("Variable '{name}' is a SESSION variable and can't be used with SET GLOBAL"),
        ),
        // Go `ErrGlobalVariable` (1229): "Variable '%-.64s' is a GLOBAL
        // variable and should be set with SET GLOBAL".
        DriverError::Var(crate::VarErrorKind::GlobalOnlyVariable(name)) => MysqlError::new(
            1229,
            *b"HY000",
            format!("Variable '{name}' is a GLOBAL variable and should be set with SET GLOBAL"),
        ),
        // Go `ErrIncorrectGlobalLocalVar` (1238), read side: "Variable
        // '%-.192s' is a SESSION variable".
        DriverError::Var(crate::VarErrorKind::NoGlobalCopy(name)) => MysqlError::new(
            ER_INCORRECT_GLOBAL_LOCAL_VAR,
            *b"HY000",
            format!("Variable '{name}' is a SESSION variable"),
        ),
        // Go `ErrIncorrectScope`: "Variable '%-.192s' is a %s variable".
        DriverError::Var(crate::VarErrorKind::IncorrectScope(name, allowed)) => MysqlError::new(
            ER_INCORRECT_GLOBAL_LOCAL_VAR,
            *b"HY000",
            format!("Variable '{name}' is a {allowed} variable"),
        ),
        // Go `ErrSpecificAccessDenied` (1227): `SET GLOBAL` without SUPER or
        // SYSTEM_VARIABLES_ADMIN.
        DriverError::Var(crate::VarErrorKind::SetGlobalAccessDenied) => MysqlError::new(
            1227,
            *b"42000",
            "Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN \
             privilege(s) for this operation"
                .to_owned(),
        ),
        // A Validation closure's own `errors.Errorf`: no code, so 1105.
        DriverError::Var(crate::VarErrorKind::ValidationRefused(message)) => {
            MysqlError::new(1105, *b"HY000", message.clone())
        }
        // Go `ErrUnsupportedIsolationLevel` (8048).
        DriverError::Var(crate::VarErrorKind::UnsupportedIsolationLevel(level)) => {
            MysqlError::new(
                8048,
                *b"HY000",
                format!(
                    "The isolation level '{level}' is not supported. Set \
                     tidb_skip_isolation_level_check=1 to skip this error"
                ),
            )
        }
        // Go `ErrReadOnly` (1621): "%s variable '%s' is read-only. Use SET %s
        // to assign the value".
        DriverError::Var(crate::VarErrorKind::SessionScopeIsReadOnly(name)) => MysqlError::new(
            1621,
            *b"HY000",
            format!("SESSION variable '{name}' is read-only. Use SET GLOBAL to assign the value"),
        ),
        DriverError::SubqueryReturnsMoreThanOneRow => MysqlError::new(
            ER_SUBQUERY_NO_1_ROW,
            *b"21000",
            "Subquery returns more than 1 row".to_owned(),
        ),
        // Go: "Reference '%-.64s' not supported (%s)".
        DriverError::IllegalReference { name, reason } => MysqlError::new(
            1247,
            *b"42S22",
            format!("Reference '{name}' not supported ({reason})"),
        ),
        // Go: "Column '%-.192s' in %-.192s is ambiguous".
        DriverError::AmbiguousColumnInClause { column, clause } => MysqlError::new(
            1052,
            *b"23000",
            format!("Column '{column}' in {clause} is ambiguous"),
        ),
        // Go: "Unknown column '%-.192s' in '%-.192s'".
        DriverError::UnknownColumnInClause { column, clause } => MysqlError::new(
            1054,
            *b"42S22",
            format!("Unknown column '{column}' in '{clause}'"),
        ),
        // Go: "The value specified for generated column '%s' in table '%s' is
        // not allowed."
        DriverError::BadGeneratedColumn { column, table } => MysqlError::new(
            3105,
            *b"HY000",
            format!(
                "The value specified for generated column '{column}' in table '{table}' is not allowed."
            ),
        ),
        // Go: "Generated column can refer only to generated columns defined
        // prior to it."
        DriverError::GeneratedColumnNonPrior => MysqlError::new(
            3107,
            *b"HY000",
            "Generated column can refer only to generated columns defined prior to it.".to_owned(),
        ),
        // The `CREATE TABLE ... PARTITION BY` refusals, each errno and
        // wording captured from real TiDB through a mock-store session. See
        // `crate::ddl::table_partition`.
        DriverError::PartitionWrongExprInFunc => MysqlError::new(
            1486,
            *b"HY000",
            "Constant, random or timezone-dependent expressions in (sub)partitioning function are \
             not allowed"
                .to_owned(),
        ),
        DriverError::PartitionFuncWrongType => MysqlError::new(
            1491,
            *b"HY000",
            "The PARTITION function returns the wrong type".to_owned(),
        ),
        DriverError::PartitionTooMany => MysqlError::new(
            1499,
            *b"HY000",
            "Too many partitions (including subpartitions) were defined".to_owned(),
        ),
        DriverError::PartitionSubpartition => MysqlError::new(
            1500,
            *b"HY000",
            "It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for \
             subpartitioning"
                .to_owned(),
        ),
        DriverError::PartitionUniqueKeyNeedAllFields(kind) => MysqlError::new(
            1503,
            *b"HY000",
            format!("A {kind} must include all columns in the table's partitioning function"),
        ),
        DriverError::PartitionNoParts(what) => MysqlError::new(
            1504,
            *b"HY000",
            format!("Number of {what} = 0 is not an allowed value"),
        ),
        DriverError::PartitionSameName(name) => MysqlError::new(
            1517,
            *b"HY000",
            format!("Duplicate partition name {name}"),
        ),
        DriverError::PartitionFunctionNotAllowed => MysqlError::new(
            1564,
            *b"HY000",
            "This partition function is not allowed".to_owned(),
        ),
        DriverError::PartitionFieldTypeNotAllowed(column) => MysqlError::new(
            1659,
            *b"HY000",
            format!("Field '{column}' is of a not allowed type for this type of partitioning"),
        ),
        DriverError::PartitionGlobalIndexNeeded(index) => MysqlError::new(
            8264,
            *b"HY000",
            format!(
                "Global Index is needed for index '{index}', since the unique index is not \
                 including all partitioning columns, and GLOBAL is not given as IndexOption"
            ),
        ),
        DriverError::PartitionWrongValues { method, clause } => MysqlError::new(
            1480,
            *b"HY000",
            format!("Only {method} PARTITIONING can use {clause} in partition definition"),
        ),
        DriverError::PartitionMaxValueNotLast => MysqlError::new(
            1481,
            *b"HY000",
            "MAXVALUE can only be used in last partition definition".to_owned(),
        ),
        DriverError::PartitionsMustBeDefined(method) => MysqlError::new(
            1492,
            *b"HY000",
            format!("For {method} partitions each partition must be defined"),
        ),
        DriverError::PartitionRangeNotIncreasing => MysqlError::new(
            1493,
            *b"HY000",
            "VALUES LESS THAN value must be strictly increasing for each partition".to_owned(),
        ),
        DriverError::PartitionDuplicateListValue => MysqlError::new(
            1495,
            *b"HY000",
            "Multiple definition of same constant in list partitioning".to_owned(),
        ),
        DriverError::NoPartitionForValue(value) => MysqlError::new(
            1526,
            *b"HY000",
            format!("Table has no partition for value {value}"),
        ),
        DriverError::PartitionConstDomain => MysqlError::new(
            1563,
            *b"HY000",
            "Partition constant is out of partition function domain".to_owned(),
        ),
        DriverError::UnknownPartition { partition, table } => MysqlError::new(
            1735,
            *b"HY000",
            format!("Unknown partition '{partition}' in table '{table}'"),
        ),
        DriverError::RowDoesNotMatchGivenPartitionSet => MysqlError::new(
            1748,
            *b"HY000",
            "Found a row not matching the given partition set".to_owned(),
        ),
        DriverError::PartitionValuesNotInt(partition) => MysqlError::new(
            1697,
            *b"HY000",
            format!("VALUES value for partition '{partition}' must have type INT"),
        ),
        // The expression-index refusals, each errno and wording captured from
        // `gorun`. See `crate::expression_index`'s module doc for the script.
        DriverError::WrongKeyColumnFunctionalIndex(expr) => MysqlError::new(
            3761,
            *b"HY000",
            format!("The used storage engine cannot index the expression '{expr}'"),
        ),
        DriverError::FunctionalIndexOnJson => MysqlError::new(
            3753,
            *b"HY000",
            "Cannot create an expression index on a function that returns a JSON or GEOMETRY value"
                .to_owned(),
        ),
        DriverError::FunctionalIndexOnBlob => MysqlError::new(
            3757,
            *b"HY000",
            "Cannot create an expression index on an expression that returns a BLOB or TEXT. \
             Please consider using CAST"
                .to_owned(),
        ),
        DriverError::FunctionalIndexOnField => MysqlError::new(
            3762,
            *b"HY000",
            "Expression index on a column is not supported. Consider using a regular index instead"
                .to_owned(),
        ),
        DriverError::FunctionalIndexFunctionNotAllowed(index) => MysqlError::new(
            3758,
            *b"HY000",
            format!("Expression of expression index '{index}' contains a disallowed function"),
        ),
        DriverError::FunctionalIndexRowValue(index) => MysqlError::new(
            3800,
            *b"HY000",
            format!("Expression of expression index '{index}' cannot refer to a row value"),
        ),
        DriverError::ExpressionIndexCanNotRefer(index) => MysqlError::new(
            3754,
            *b"HY000",
            format!("Expression index '{index}' cannot refer to an auto-increment column"),
        ),
        DriverError::UnsafeFunctionInExpressionIndex => MysqlError::new(
            8200,
            *b"HY000",
            "Unsupported creating expression index containing unsafe functions without \
             allow-expression-index in config"
                .to_owned(),
        ),
        DriverError::WrongParamCountToNativeFct(name) => MysqlError::new(
            1582,
            *b"42000",
            format!("Incorrect parameter count in the call to native function '{name}'"),
        ),
        // CAPTURED from TiDB: `alter table fi add index idx((a+b))` then any of
        // `drop column a` / `change column a z int` / `rename column a to z`
        // gives 3837 / HY000 / "Column 'a' has an expression index dependency
        // and cannot be dropped or renamed" -- the wording below, confirmed.
        DriverError::DependentByFunctionalIndex(column) => MysqlError::new(
            3837,
            *b"HY000",
            format!(
                "Column '{column}' has an expression index dependency and cannot be dropped or \
                 renamed"
            ),
        ),
        // Go `ErrDependentByGeneratedColumn`: the message has a trailing
        // period, unlike its two siblings here -- that is TiDB's own wording
        // (`errname` catalog), not a slip.
        DriverError::DependentByGeneratedColumn(column) => MysqlError::new(
            3108,
            *b"HY000",
            format!("Column '{column}' has a generated column dependency."),
        ),
        DriverError::DependentByPartitionFunctional(column) => MysqlError::new(
            3855,
            *b"HY000",
            format!(
                "Column '{column}' has a partitioning function dependency and cannot be dropped \
                 or renamed"
            ),
        ),
        DriverError::TooLongIdent(ident) => MysqlError::new(
            1059,
            *b"42000",
            format!("Identifier name '{ident}' is too long"),
        ),
        DriverError::TableCommentTooLong(table) => MysqlError::new(
            1628,
            *b"HY000",
            format!("Comment for table '{table}' is too long (max = 2048)"),
        ),
        // Go: "'%s' is not supported for generated columns."
        DriverError::UnsupportedOnGeneratedColumn(reason) => MysqlError::new(
            3106,
            *b"HY000",
            format!("'{reason}' is not supported for generated columns."),
        ),
        // Go: "Default value expression of column '%s' contains a disallowed
        // function: `%s`."
        DriverError::DefaultFunctionNotAllowed(column, function) => MysqlError::new(
            3770,
            *b"HY000",
            format!(
                "Default value expression of column '{column}' contains a disallowed function: \
                 `{function}`."
            ),
        ),
        // Go: "Unknown table '%-.192s' in %-.32s".
        DriverError::UnknownTableInMultiDelete(table) => MysqlError::new(
            1109,
            *b"42S02",
            format!("Unknown table '{table}' in MULTI DELETE"),
        ),
        // Go: "The target table %-.100s of the %s is not updatable".
        // 1288 is absent from Go's `mysql.MySQLState`, so it carries the
        // default state.
        DriverError::NonUpdatableTable { table, statement } => MysqlError::new(
            1288,
            *b"HY000",
            format!("The target table {table} of the {statement} is not updatable"),
        ),
        // Go: "Can't group on '%-.192s'".
        DriverError::WrongGroupField(field) => MysqlError::new(
            1056,
            *b"42000",
            format!("Can't group on '{field}'"),
        ),
        DriverError::FieldNotInGroupBy { position, clause, column } => MysqlError::new(
            1055,
            *b"42000",
            format!(
                "Expression #{position} of {clause} is not in GROUP BY clause and contains \
                 nonaggregated column '{column}' which is not functionally dependent on columns \
                 in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by"
            ),
        ),
        DriverError::AggregateOrderNonAggQuery { position } => MysqlError::new(
            3029,
            *b"HY000",
            format!(
                "Expression #{position} of ORDER BY contains aggregate function and applies to \
                 the result of a non-aggregated query"
            ),
        ),
        DriverError::FieldNotInAggregatedQuery { position, column } => MysqlError::new(
            8123,
            *b"HY000",
            format!(
                "In aggregated query without GROUP BY, expression #{position} of SELECT list \
                 contains nonaggregated column '{column}'; this is incompatible with \
                 sql_mode=only_full_group_by"
            ),
        ),
        DriverError::FieldInOrderNotSelect { position, column } => MysqlError::new(
            3065,
            *b"HY000",
            format!(
                "Expression #{position} of ORDER BY clause is not in SELECT list, references \
                 column '{column}' which is not in SELECT list; this is incompatible with DISTINCT"
            ),
        ),
        DriverError::AggregateInOrderNotSelect { position } => MysqlError::new(
            3066,
            *b"HY000",
            format!(
                "Expression #{position} of ORDER BY clause is not in SELECT list, contains \
                 aggregate function; this is incompatible with DISTINCT"
            ),
        ),
        // Go: "Invalid default value for '%-.192s'".
        DriverError::InvalidDefault(column) => MysqlError::new(
            1067,
            *b"42000",
            format!("Invalid default value for '{column}'"),
        ),
        DriverError::FieldGetDefaultFailed(column) => MysqlError::coded(
            8038,
            format!("Field '{column}' get default value fail"),
        ),
        DriverError::PrimaryCantHaveNull => MysqlError::new(
            1171,
            *b"42000",
            "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, \
             use UNIQUE instead"
                .to_owned(),
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
        // Go `types.overflow`: "constant %v overflows %s".
        DriverError::ConstantOverflows { value, type_name } => MysqlError::new(
            1690,
            *b"22003",
            format!("constant {value} overflows {type_name}"),
        ),
        // Go: "Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d".
        DriverError::IncorrectValue {
            type_name,
            value,
            column,
            row,
        } => {
            // Go `table.CastValue` converts an invalid character group into
            // the same 1366 error BEFORE an INSERT/UPDATE caller can append a
            // row.  Row zero is the driver representation of that raw form;
            // every completed write supplies a one-based row as before.
            let message = if row == 0 && type_name == "string" {
                format!("Incorrect string value '{value}' for column '{column}'")
            } else {
                format!(
                    "Incorrect {type_name} value: '{value}' for column '{column}' at row {row}"
                )
            };
            MysqlError::new(1366, *b"HY000", message)
        }
        // Go: `types.ErrWrongValue` completed by `completeInsertErr`.
        DriverError::IncorrectTemporalValue {
            type_name,
            value,
            column,
            row,
        } => MysqlError::new(
            1292,
            *b"22007",
            format!("Incorrect {type_name} value: '{value}' for column '{column}' at row {row}"),
        ),
        // Go: "Failed to read auto-increment value from storage engine",
        // which is what an exhausted allocator reports.
        DriverError::AutoincReadFailed => MysqlError::new(
            1467,
            *b"HY000",
            "Failed to read auto-increment value from storage engine".to_owned(),
        ),
        DriverError::AutoIdUnavailable(detail) => MysqlError::unknown(detail),
        DriverError::CatalogPoisoned => {
            MysqlError::unknown("the shared catalog is unusable after a failed statement")
        }
        }
    }
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.clone().to_mysql_error().message)
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;

    #[test]
    fn raw_incorrect_string_value_matches_table_cast_value() {
        // pkg/table/column_test.go::TestCastValue expects the raw table error,
        // before a statement-level caller has attached a row number.
        let error = DriverError::IncorrectValue {
            type_name: "string".to_owned(),
            value: "\\x81".to_owned(),
            column: String::new(),
            row: 0,
        }
        .to_mysql_error();

        assert_eq!(error.code, 1366);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(
            error.message,
            "Incorrect string value '\\x81' for column ''"
        );
    }

    #[test]
    fn region_unavailable_uses_tidb_error_code() {
        let error = DriverError::Txn(TxnErrorKind::RegionUnavailable).to_mysql_error();
        assert_eq!(error.code, tidb_error::tidb::errcode::ErrRegionUnavailable);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(error.message, "Region is unavailable");
    }

    #[test]
    fn display_matches_the_mysql_diagnostic() {
        let error = DriverError::unsupported("an unsupported source operation");
        assert_eq!(error.to_string(), error.clone().to_mysql_error().message);
    }
}
