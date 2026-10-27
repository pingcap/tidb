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
//!
//! Go passes plain `error` values up from `pkg/kv`, `pkg/table` and
//! `pkg/ddl` and renders them with `terror.ToSQLError` at the wire; here the
//! lower layers return their own errors (`StorageError`, `KvTableError`,
//! `ColumnError`, `CastError`, `KillStop`, `MysqlError`) and the conversions
//! below lift them into the executor's. The Go error prototypes are
//! `tidb_error`'s and `tidb_util::dbterror`'s.

use tidb_hack::GoToLower;

mod driver_error;
mod exec;
mod schema;
mod txn;
mod var;

pub use driver_error::DriverError;
pub use exec::coprocessor_evaluation_error;
pub use schema::SchemaErrorKind;
pub use txn::TxnErrorKind;
pub use var::VarError;

mod exec_error;

pub use exec_error::ExecError;
pub use tidb_error::mysql::MysqlError;

/// An error raised with its Go identity already rendered (Go returns it as a
/// plain `error`): the statement reports it unchanged.
impl From<MysqlError> for DriverError {
    fn from(error: MysqlError) -> Self {
        DriverError::Mysql(error)
    }
}

impl From<ExecError> for DriverError {
    fn from(err: ExecError) -> Self {
        match err {
            // The same statement-level error whichever layer raised it, so
            // callers match one variant.
            ExecError::SubqueryReturnsMoreThanOneRow => DriverError::SubqueryReturnsMoreThanOneRow,
            ExecError::CteMaxRecursionDepth(rounds) => DriverError::CteMaxRecursionDepth(rounds),
            ExecError::MemoryExceedForQuery { conn_id } => {
                DriverError::MemoryExceedForQuery { conn_id }
            }
            ExecError::JsonDocumentNullKey => DriverError::JsonDocumentNullKey,
            ExecError::Retrieval(error) => *error,
            // go's charFunctionClass.getFunction BUILD failure: a statement-
            // level build error, not an evaluation event -- the warning buffer
            // records the 1105 row beside the argument casts' truncation
            // warnings (oracle: CHAR('中文测试', 'ünïcödé')).
            ExecError::Eval(tidb_expr::EvalError::Unsupported(message))
                if message.starts_with("Unknown charset ") =>
            {
                DriverError::Unsupported(message.into())
            }
            ExecError::InvalidJsonCharset { charset } => {
                DriverError::InvalidJsonCharset { charset }
            }
            other => DriverError::Exec(other),
        }
    }
}


/// MySQL `ER_PARSE_ERROR`.
const ER_PARSE_ERROR: u16 = 1064;
/// TiDB `ErrWriteConflict`.
const ER_WRITE_CONFLICT: u16 = tidb_error::tidb::errcode::ErrWriteConflict;
/// Go `kv.TxnRetryableMark`, which clients use to identify replayable commits.
/// `pkg/kv/error.go` warns that this compatibility marker must not change.
const TXN_RETRYABLE_MARK: &str = "[try again later]";
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
        DriverError::Mysql(error) => error,
        // Go `util.SyntaxError` (`pkg/util/misc.go:172`):
        // `ErrParse.GenWithStackByArgs(SyntaxErrorPrefix, err.Error())` over
        // ErrParse's `"%s %s"` — the prefix sentence, one space, then the
        // parser's own positional text.
        DriverError::Parse(message) => MysqlError::terror(
            &tidb_error::catalog::parser::ERR_PARSE,
            format!(
                "You have an error in your SQL syntax; check the manual that corresponds \
                 to your TiDB version for the right syntax to use {message}"
            ),
        ),
        DriverError::ParseCoded { proto, message } => MysqlError::terror(proto, message),
        DriverError::DdlCoded { proto, message } => MysqlError::terror(proto, message),
        DriverError::Unsupported(message) => MysqlError::unknown(message),
        DriverError::UnknownExplainFormat(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_UNKNOWN_EXPLAIN_FORMAT,
            format!("Unknown EXPLAIN format name: '{name}'"),
        ),
        DriverError::UnknownThreadId(id) => {
            MysqlError::terror(
                &tidb_error::plannererrors::ERR_NO_SUCH_THREAD,
                format!("Unknown thread id: {id}"),
            )
        }
        DriverError::NotSupportedWithSem(statement) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_NOT_SUPPORTED_WITH_SEM,
            format!(
                "Feature '{statement}' is not supported when security enhanced mode is enabled"
            ),
        ),
        DriverError::TableOptionUnionUnsupported => MysqlError::terror(
            &tidb_util::dbterror::ERR_TABLE_OPTION_UNION_UNSUPPORTED,
            tidb_error::tidb::errname::ErrTableOptionUnionUnsupported
                .raw
                .to_owned(),
        ),
        DriverError::TableOptionInsertMethodUnsupported => MysqlError::terror(
            &tidb_util::dbterror::ERR_TABLE_OPTION_INSERT_METHOD_UNSUPPORTED,
            tidb_error::tidb::errname::ErrTableOptionInsertMethodUnsupported
                .raw
                .to_owned(),
        ),
        DriverError::UnknownStorageEngine(engine) => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNKNOWN_ENGINE,
            format!("Unknown storage engine '{engine}'"),
        ),
        DriverError::OperationOnCachedTable(operation) => MysqlError::terror(
            &tidb_util::dbterror::ERR_OPT_ON_CACHE_TABLE,
            format!("'{operation}' is unsupported on cache tables."),
        ),
        // Go `dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs(op)` over
        // `` `%s` is unsupported on temporary tables. `` -- the backquotes
        // are part of the FORMAT, not decoration added here.
        DriverError::OptOnTemporaryTable(operation) => MysqlError::terror(
            // The DDL executor (`pkg/ddl/executor.go`, `create_table.go`)
            // raises its own `dbterror` definition for these operations; the
            // planner's `preprocess` raises `plannererrors`'.
            match operation {
                "placement" | "pre split regions" | "set TiFlash replica" | "masking policy" => {
                    &tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE
                }
                _ => &tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE,
            },
            format!("`{operation}` is unsupported on temporary tables."),
        ),
        DriverError::UnsupportedOnCommitPreserve => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ON_COMMIT_PRESERVE,
            "TiDB doesn't support ON COMMIT PRESERVE ROWS for now",
        ),
        DriverError::PartitionNoTemporary => MysqlError::terror(
            &tidb_util::dbterror::ERR_PARTITION_NO_TEMPORARY,
            tidb_error::tidb::errname::ErrPartitionNoTemporary.raw,
        ),
        DriverError::DropTableOnTemporaryTable => MysqlError::terror(
            &tidb_error::plannererrors::ERR_DROP_TABLE_ON_TEMPORARY_TABLE,
            tidb_error::tidb::errname::ErrDropTableOnTemporaryTable.raw,
        ),
        DriverError::CheckPartialIndexWithoutFastCheck => MysqlError::terror(
            &tidb_error::catalog::executor::ERR_CHECK_PARTIAL_INDEX_WITHOUT_FAST_CHECK,
            tidb_error::tidb::errname::ErrCheckPartialIndexWithoutFastCheck.raw,
        ),
        DriverError::ViewSelectVariable => MysqlError::terror(
            &tidb_util::dbterror::ERR_VIEW_SELECT_VARIABLE,
            tidb_error::tidb::errname::ErrViewSelectVariable.raw,
        ),
        DriverError::ViewSelectTemporaryTable(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_VIEW_SELECT_TEMPORARY_TABLE,
            format!("View's SELECT refers to a temporary table '{name}'"),
        ),
        DriverError::TempTableNotAllowedWithTTL => MysqlError::terror(
            &tidb_util::dbterror::ERR_TEMP_TABLE_NOT_ALLOWED_WITH_TTL,
            tidb_error::tidb::errname::ErrTempTableNotAllowedWithTTL.raw,
        ),
        // Go: "Unknown column '%-.192s' in '%-.192s'" with the TTL clause name.
        DriverError::UnknownColumnInTtlConfig(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_BAD_FIELD,
            format!("Unknown column '{column}' in 'TTL config'"),
        ),
        // Go: "Field '%-.192s' is of a not supported type for TTL config,
        // expect DATETIME, DATE or TIMESTAMP".
        DriverError::UnsupportedColumnInTtlConfig(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_COLUMN_IN_TTL_CONFIG,
            format!(
                "Field '{column}' is of a not supported type for TTL config, \
                 expect DATETIME, DATE or TIMESTAMP"
            ),
        ),
        // Go: "Cannot drop column '%-.192s': needed in TTL config".
        DriverError::TtlColumnCannotDrop(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_TTL_COLUMN_CANNOT_DROP,
            format!("Cannot drop column '{column}': needed in TTL config"),
        ),
        // Go: "Cannot set %s on a table without TTL config".
        DriverError::SetTtlOptionForNonTtlTable(option) => MysqlError::terror(
            &tidb_util::dbterror::ERR_SET_TTL_OPTION_FOR_NON_TTL_TABLE,
            format!("Cannot set {option} on a table without TTL config"),
        ),
        // Go: "Unsupported clustered primary key type FLOAT/DOUBLE for TTL".
        DriverError::UnsupportedPrimaryKeyTypeWithTtl => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_PRIMARY_KEY_TYPE_WITH_TTL,
            tidb_error::tidb::errname::ErrUnsupportedPrimaryKeyTypeWithTTL.raw,
        ),
        // Go: "Set TTL for a table referenced by foreign key is not allowed".
        DriverError::TtlReferencedByForeignKey => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK,
            tidb_error::tidb::errname::ErrUnsupportedTTLReferencedByFK.raw,
        ),
        DriverError::UnsupportedLocalTempTableDDL(statement) => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_CACHE_FOR_SYS_TABLE,
            format!("TiDB doesn't support {statement} for local temporary table"),
        ),
        DriverError::UnsupportedAlterCacheForSystemTable => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_CACHE_FOR_SYS_TABLE,
            "ALTER table cache for tables in system database is currently unsupported",
        ),
        DriverError::TableLocked {
            table,
            lock_type,
            holder,
        } => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_TABLE_LOCKED,
            format!("Table '{table}' was locked in {} by {holder}", lock_type.sql()),
        ),
        DriverError::TableNotLockedForWrite(table) => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE,
            format!("Table '{table}' was locked with a READ lock and can't be updated"),
        ),
        DriverError::TableNotLocked(table) => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_TABLE_NOT_LOCKED,
            format!("Table '{table}' was not locked with LOCK TABLES"),
        ),
        DriverError::LockOrActiveTransaction => MysqlError::terror(
            &tidb_error::catalog::table::ERR_LOCK_OR_ACTIVE_TRANSACTION,
            tidb_error::tidb::errname::ErrLockOrActiveTransaction.raw,
        ),
        DriverError::UnsupportedOp => MysqlError::terror(
            &tidb_error::catalog::table::ERR_UNSUPPORTED_OP,
            tidb_error::tidb::errname::ErrUnsupportedOp.raw,
        ),
        DriverError::NonuniqTable(table) => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_NONUNIQ_TABLE,
            format!("Not unique table/alias: '{table}'"),
        ),
        DriverError::FuncNotEnabled(statement, switch) => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_FUNC_NOT_ENABLED,
            format!(
                "{statement} is not supported. To enable this experimental feature, set \
                 '{switch}' in the configuration file."
            ),
        ),
        // Every execution and evaluation failure, one arm each, in `exec`.
        DriverError::Exec(error) => exec::to_mysql_error(error),
        DriverError::Txn(crate::TxnErrorKind::WriteConflict) => {
            MysqlError::terror(
                &tidb_error::catalog::kv::ERR_WRITE_CONFLICT,
                format!("Write conflict, please retry the transaction {TXN_RETRYABLE_MARK}"),
            )
        }
        DriverError::Txn(crate::TxnErrorKind::AsOf(cause)) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_AS_OF,
            format!("invalid as of timestamp: {cause}"),
        ),
        DriverError::Txn(crate::TxnErrorKind::RegionUnavailable) => MysqlError::terror(
            &tidb_error::catalog::tikv::ERR_REGION_UNAVAILABLE,
            tidb_error::tidb::errname::ErrRegionUnavailable.raw.to_owned(),
        ),
        // Go: "The used SELECT statements have a different number of columns".
        DriverError::WrongNumberOfColumnsInSelect => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            "The used SELECT statements have a different number of columns".to_owned(),
        ),
        DriverError::WrongUsage { first, second } => MysqlError::terror(
            // Go raises `ErrWrongUsage` from three packages: the grants
            // (`exeerrors`), the DDL executor's OR REPLACE / IF NOT EXISTS
            // pairing (`dbterror`) and the planner (UNION ... INTO / LIMIT /
            // ORDER BY, `plannererrors`); the first operand names which.
            match first {
                "COLUMN GRANT" | "DB GRANT" | "GLOBAL GRANT" => {
                    &tidb_util::dbterror::exeerrors::ERR_WRONG_USAGE
                }
                "OR REPLACE" => &tidb_util::dbterror::ERR_WRONG_USAGE,
                _ => &tidb_error::plannererrors::ERR_WRONG_USAGE,
            },
            format!("Incorrect usage of {first} and {second}"),
        ),
        // Go: "Incorrect table definition; there can be only one auto column
        // and it must be defined as a key".
        DriverError::WrongAutoKey => MysqlError::terror(
            &tidb_error::catalog::autoid::ERR_WRONG_AUTO_KEY,
            "Incorrect table definition; there can be only one auto column and it must be defined as a key".to_owned(),
        ),
        // Go preprocess's plain `errors.Errorf`, which reaches the wire as
        // 1105, not `ErrWrongFieldSpec`'s 1063.
        DriverError::WrongColumnSpecifier(name) => MysqlError::unknown(
            format!("Incorrect column specifier for column '{name}'"),
        ),
        DriverError::InvalidOnUpdate(name) => MysqlError::terror(
            &tidb_util::dbterror::ERR_INVALID_ON_UPDATE,
            format!("Invalid ON UPDATE clause for '{name}' column"),
        ),
        // Go: "Incorrect column name '%-.100s'".
        DriverError::WrongColumnName(name) => MysqlError::terror(
            &tidb_util::dbterror::ERR_WRONG_COLUMN_NAME,
            format!("Incorrect column name '{name}'"),
        ),
        // Go: "A primary key index cannot be invisible".
        DriverError::PrimaryKeyCantBeInvisible => MysqlError::terror(
            &tidb_util::dbterror::ERR_PK_INDEX_CANT_BE_INVISIBLE,
            "A primary key index cannot be invisible".to_owned(),
        ),
        // Go: "Key '%-.192s' doesn't exist in table '%-.192s'".
        DriverError::KeyNotExists { key, table } => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_KEY_NOT_EXISTS,
            format!("Key '{key}' doesn't exist in table '{table}'"),
        ),
        // Go: "Column '%-.192s' cannot be null".
        DriverError::ColumnCannotBeNull(name) => tidb_table::column::err_column_cant_null(&name),
        DriverError::NoDefaultForField(name) => tidb_table::column::err_no_default_value(&name),
        // Go: "Incorrect foreign key definition for '%-.192s': %s".
        DriverError::WrongFkDef { name, reason } => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NOT_MATCH,
            format!("Incorrect foreign key definition for '{name}': {reason}"),
        ),
        // Go `ErrForeignKeyColumnNotNull`.
        DriverError::ForeignKeyColumnNotNull {
            column,
            constraint,
        } => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL,
            format!(
                "Column '{column}' cannot be NOT NULL: needed in a foreign key constraint '{constraint}' SET NULL"
            ),
        ),
        // Go `ErrForeignKeyNoIndexInParent`.
        DriverError::ForeignKeyNoIndexInParent { constraint, table } => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT,
            format!(
                "Failed to add the foreign key constraint. Missing index for constraint '{constraint}' in the referenced table '{table}'"
            ),
        ),
        // Go `infoschema.ErrForeignKeyOnPartitioned`.
        DriverError::ForeignKeyOnPartitioned => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED,
            "Foreign key clause is not yet supported in conjunction with partitioning".to_owned(),
        ),
        // Go: "Cannot add or update a child row: a foreign key constraint
        // fails (%.192s)".
        DriverError::ForeignKeyNoReferencedRow { table, constraint } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_NO_REFERENCED_ROW2,
            format!(
                "Cannot add or update a child row: a foreign key constraint fails ({table}, {constraint})"
            ),
        ),
        // Go: "Cannot delete or update a parent row: a foreign key
        // constraint fails (%.192s)".
        DriverError::ForeignKeyRowIsReferenced { table, constraint } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_ROW_IS_REFERENCED2,
            format!(
                "Cannot delete or update a parent row: a foreign key constraint fails ({table}, {constraint})"
            ),
        ),
        // Go's ErrTruncateIllegalForeignKey is the TRUNCATE TABLE diagnostic;
        // DROP TABLE has the distinct ErrForeignKeyCannotDrop below.
        DriverError::ForeignKeyTableReferenced { detail } => MysqlError::terror(
            &tidb_util::dbterror::ERR_TRUNCATE_ILLEGAL_FOREIGN_KEY,
            format!(
                "Cannot truncate a table referenced in a foreign key constraint ({detail})"
            ),
        ),
        DriverError::ForeignKeyTableCannotDrop {
            parent_table,
            constraint,
            child_table,
        } => MysqlError::terror(
            &tidb_util::dbterror::ERR_FOREIGN_KEY_CANNOT_DROP_PARENT,
            format!(
                "Cannot drop table '{parent_table}' referenced by a foreign key constraint '{constraint}' on table '{child_table}'."
            ),
        ),
        // Go `ErrForeignKeyCannotDrop`: DROP DATABASE reports the parent and
        // child table names in a distinct 3730 diagnostic.
        DriverError::ForeignKeyDatabaseReferenced {
            parent_table,
            constraint,
            child_table,
        } => MysqlError::terror(
            &tidb_util::dbterror::ERR_FOREIGN_KEY_CANNOT_DROP_PARENT,
            format!(
                "Cannot drop table '{parent_table}' referenced by a foreign key constraint '{constraint}' on table '{child_table}'."
            ),
        ),
        // Go: "Foreign key '%s' uses virtual column '%s' which is not
        // supported.". Captured via `gorun`: `[schema:3733]`.
        DriverError::ForeignKeyUsesVirtualColumn {
            foreign_key,
            column,
        } => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN,
            format!("Foreign key '{foreign_key}' uses virtual column '{column}' which is not supported."),
        ),
        // Go: "Cannot define foreign key with %s clause on a generated
        // column.". Captured via `gorun`: `[ddl:3104]`.
        DriverError::WrongFkOptionForGeneratedColumn { clause } => MysqlError::terror(
            &tidb_util::dbterror::ERR_WRONG_FK_OPTION_FOR_GENERATED_COLUMN,
            format!("Cannot define foreign key with {clause} clause on a generated column."),
        ),
        // Go: "Cannot drop index '%-.192s': needed in a foreign key
        // constraint". Captured via `gorun`: `[ddl:1553]`.
        DriverError::DropIndexNeededInForeignKey(index) => MysqlError::terror(
            &tidb_util::dbterror::ERR_DROP_INDEX_NEEDED_IN_FOREIGN_KEY,
            format!("Cannot drop index '{index}': needed in a foreign key constraint"),
        ),
        // Go: "Referencing column '%s' and referenced column '%s' in foreign
        // key constraint '%s' are incompatible.". Captured: `[ddl:3780]`.
        DriverError::FkIncompatibleColumns {
            referencing,
            referenced,
            constraint,
        } => MysqlError::terror(
            &tidb_util::dbterror::ERR_FK_INCOMPATIBLE_COLUMNS,
            format!(
                "Referencing column '{referencing}' and referenced column '{referenced}' in \
                 foreign key constraint '{constraint}' are incompatible."
            ),
        ),
        // Go: "Cannot change column '%-.192s': used in a foreign key
        // constraint '%-.192s'". Captured: `[ddl:1832]`.
        DriverError::ForeignKeyColumnCannotChange { column, constraint } => MysqlError::terror(
            &tidb_util::dbterror::ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE,
            format!("Cannot change column '{column}': used in a foreign key constraint '{constraint}'"),
        ),
        // Go: "Cannot change column '%-.192s': used in a foreign key
        // constraint '%-.192s' of table '%-.192s'". Captured: `[ddl:1833]`.
        DriverError::ForeignKeyColumnCannotChangeChild {
            column,
            constraint,
            child_table,
        } => MysqlError::terror(
            &tidb_util::dbterror::ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE_CHILD,
            format!(
                "Cannot change column '{column}': used in a foreign key constraint \
                 '{constraint}' of table '{child_table}'"
            ),
        ),
        // Go: "Duplicate foreign key constraint name '%s'".
        DriverError::FkDupName(name) => MysqlError::terror(
            &tidb_util::dbterror::ERR_FK_DUP_NAME,
            format!("Duplicate foreign key constraint name '{name}'"),
        ),
        // Go: "Foreign key cascade delete/update exceeds max depth of %v.".
        DriverError::ForeignKeyCascadeTooDeep => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_FOREIGN_KEY_CASCADE_DEPTH_EXCEEDED,
            "Foreign key cascade delete/update exceeds max depth of 15.".to_owned(),
        ),
        // Go: "Duplicate entry '%-.64s' for key '%-.192s'".
        DriverError::DuplicateEntry { value, key } => MysqlError::terror(
            &tidb_error::catalog::kv::ERR_KEY_EXISTS,
            format!("Duplicate entry '{value}' for key '{key}'"),
        ),
        // Go: "Duplicate key name '%-.192s'".
        DriverError::DuplicateKeyName(name) => {
            MysqlError::terror(
                &tidb_util::dbterror::ERR_DUP_KEY_NAME,
                format!("Duplicate key name '{name}'"),
            )
        }
        // Go: "index %s doesn't exist" -- 1091's index-specific message.
        DriverError::UnknownIndex(name) => {
            MysqlError::terror(
                &tidb_util::dbterror::ERR_CANT_DROP_FIELD_OR_KEY,
                format!("index {name} doesn't exist"),
            )
        }
        // Go: "Column count doesn't match value count at row %d" (1136).
        DriverError::WrongValueCountOnRow { row } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW,
            format!("Column count doesn't match value count at row {row}"),
        ),
        // Go: "Multiple primary key defined".
        DriverError::MultiplePrimaryKey => {
            MysqlError::terror(
                &tidb_error::catalog::infoschema::ERR_MULTIPLE_PRI_KEY,
                "Multiple primary key defined".to_owned(),
            )
        }
        // Go: "Too-big precision %d specified for '%-.192s'. Maximum is %d."
        DriverError::TooBigPrecision {
            precision,
            column,
            maximum,
        } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TOO_BIG_PRECISION,
            format!("Too-big precision {precision} specified for '{column}'. Maximum is {maximum}."),
        ),
        // Go: "For float(M,D), double(M,D) or decimal(M,D), M must be >= D
        // (column '%s')."
        DriverError::MBiggerThanD(column) => MysqlError::terror(
            &tidb_error::catalog::types::ERR_M_BIGGER_THAN_D,
            format!(
                "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '{column}')."
            ),
        ),
        // Go: "Column '%-.100s' has duplicated value '%-.64s' in %s".
        DriverError::DuplicatedValueInType {
            column,
            value,
            type_name,
        } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_DUPLICATED_VALUE_IN_TYPE,
            format!("Column '{column}' has duplicated value '{value}' in {type_name}"),
        ),
        // Go: "Too long enumeration/set value for column %s.".
        DriverError::TooLongEnumSetValue { column } => MysqlError::terror(
            &tidb_util::dbterror::ERR_TOO_LONG_VALUE_FOR_TYPE,
            format!("Too long enumeration/set value for column {column}."),
        ),
        // Go: ErrUnsupportedShardRowIDBits -> "Unsupported shard_row_id_bits
        // for table with primary key as row id" (8200).
        DriverError::UnsupportedShardRowIdBits => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_SHARD_ROW_ID_BITS,
            "Unsupported shard_row_id_bits for table with primary key as row id".to_string(),
        ),
        // Go: "Display width out of range for column '%-.192s' (max = %d)".
        DriverError::TooBigDisplayWidth { column, maximum } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TOO_BIG_DISPLAY_WIDTH,
            format!(
                "Display width out of range for column '{column}' (max = {maximum})"
            ),
        ),
        // Go: "Column length too big for column '%-.192s' (max = %d); use
        // BLOB or TEXT instead".
        DriverError::TooBigFieldLength { column, maximum } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TOO_BIG_FIELD_LENGTH,
            format!(
                "Column length too big for column '{column}' (max = {maximum}); use BLOB or TEXT instead"
            ),
        ),
        // Go: "Invalid size for column '%s'.".
        DriverError::InvalidFieldSize { column } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_INVALID_FIELD_SIZE,
            format!("Invalid size for column '{column}'."),
        ),
        // Go: "Too big scale %d specified for column '%-.192s'. Maximum is %d.".
        DriverError::TooBigScale {
            scale,
            column,
            maximum,
        } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TOO_BIG_SCALE,
            format!(
                "Too big scale {scale} specified for column '{column}'. Maximum is {maximum}."
            ),
        ),
        // Go: "Too many strings for column %-.192s and SET".
        DriverError::TooManySetMembers { column } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TOO_BIG_SET,
            format!("Too many strings for column '{column}' and SET"),
        ),
        // Go: "Illegal %s '%-.192s' value found during parsing".
        DriverError::IllegalValueForType { type_name, value } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_ILLEGAL_VALUE_FOR_TYPE,
            format!("Illegal {type_name} '{value}' value found during parsing"),
        ),
        // Go: "Incorrect column specifier for column '%-.192s'".
        DriverError::WrongFieldSpec { column } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_WRONG_FIELD_SPEC,
            format!("Incorrect column specifier for column '{column}'"),
        ),
        // Go: "Duplicate column name '%-.192s'".
        DriverError::DuplicateColumnName(name) => {
            MysqlError::terror(
                &tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS,
                format!("Duplicate column name '{name}'"),
            )
        }
        // Go: "Can't DROP '%-.192s'; check that column/key exists".
        DriverError::UnknownColumnInAlter(name) => MysqlError::terror(
            &tidb_util::dbterror::ERR_CANT_DROP_FIELD_OR_KEY,
            format!("Can't DROP '{name}'; check that column/key exists"),
        ),
        // Go: "can't drop only column %s in table %s".
        DriverError::CannotDropOnlyColumn { column, table } => MysqlError::terror(
            &tidb_util::dbterror::ERR_CANT_REMOVE_ALL_FIELDS,
            format!("can't drop only column {column} in table {table}"),
        ),
        // TiDB: "can't drop column %s with composite index covered or Primary
        // Key covered now".
        DriverError::CannotDropColumnWithCompositeIndex(name) => MysqlError::terror(
            &tidb_util::dbterror::ERR_CANT_DROP_COL_WITH_INDEX,
            format!(
                "can't drop column {name} with composite index covered or Primary Key covered now"
            ),
        ),
        // Go: "function %s has only noop implementation in tidb now, use
        // tidb_enable_noop_functions to enable these functions" (1235).
        DriverError::FunctionsNoopImpl(clause)
        | DriverError::Var(crate::VarError::FunctionsNoopImpl(clause)) => MysqlError::terror(
            &tidb_error::catalog::expression::ERR_FUNCTIONS_NOOP_IMPL,
            format!(
                "function {clause} has only noop implementation in tidb now, use \
                 tidb_enable_noop_functions to enable these functions"
            ),
        ),
        // TiDB: "Unsupported modify column: %s".
        DriverError::UnsupportedModifyColumn(reason) => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN,
            format!("Unsupported modify column: {reason}"),
        ),
        // TiDB: "Unsupported modify column: change from original type %v to
        // %v is currently unsupported yet" (the message's INNER wrap; see the
        // variant doc for the double-wrap Go's caller adds on top).
        DriverError::UnsupportedModifyColumnType { from, to } => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN,
            format!(
                "Unsupported modify column: change from original type {from} to {to} is \
                 currently unsupported yet"
            ),
        ),
        // Go `ErrSpDoesNotExist`: "%s %s does not exist", which
        // `executeReleaseSavepoint` and the ROLLBACK TO path both fill in
        // with the literal "SAVEPOINT" and the name.
        DriverError::SavepointNotExists(name) => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_SAVEPOINT_NOT_EXISTS,
            format!("SAVEPOINT {name} does not exist"),
        ),
        // Go `ClassAdmin.NewStd(errno.ErrAdminCheckTable)`: 8003, HY000. The
        // detail is Go's own "table count %d != index(%s) count %d".
        DriverError::AdminCheckTable(detail) => MysqlError::terror(
            &tidb_error::catalog::admin::ERR_ADMIN_CHECK_TABLE,
            detail,
        ),
        // Go `ClassAdmin.NewStd(errno.ErrDataInconsistent)`: 8223, HY000.
        DriverError::DataInconsistent(detail) => MysqlError::terror(
            &tidb_error::catalog::consistency::ERR_ADMIN_CHECK_INCONSISTENT,
            detail,
        ),
        DriverError::DataInconsistentMismatchIndex(detail) => {
            MysqlError::terror(
                &tidb_error::catalog::consistency::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO,
                detail,
            )
        }
        // Captured from TiDB: both `EXECUTE stmt` with a marker left unbound
        // and `EXECUTE stmt USING @a` with no marker report
        // `[planner:8112]Wrong parameter count`, not 1210 -- the check is
        // `planCachePreprocess`'s step 1, shared by the SQL-level EXECUTE and
        // the binary protocol.
        DriverError::WrongParamCount => {
            MysqlError::terror(
                &tidb_error::plannererrors::ERR_WRONG_PARAM_COUNT,
                "Wrong parameter count".to_owned(),
            )
        }
        // Go: "Prepared statement not found".
        DriverError::PreparedStmtNotFound => {
            MysqlError::terror(
                &tidb_error::plannererrors::ERR_STMT_NOT_FOUND,
                "Prepared statement not found".to_owned(),
            )
        }
        // Go: "Can not prepare multiple statements".
        DriverError::PrepareMulti => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_PREPARE_MULTI,
            "Can not prepare multiple statements".to_owned(),
        ),
        DriverError::UnsupportedPreparedStatement => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_UNSUPPORTED_PS,
            "This command is not supported in the prepared statement protocol yet".to_owned(),
        ),
        // Go: "Incorrect arguments to %s".
        DriverError::WrongArguments(function) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WRONG_ARGUMENTS,
            format!("Incorrect arguments to {function}"),
        ),
        // Go: "You cannot use the window function '%s' in this context.'"
        // (the trailing quote is in Go's own message text).
        DriverError::WindowInvalidWindowFuncUse(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_INVALID_WINDOW_FUNC_USE,
            format!("You cannot use the window function '{name}' in this context.'"),
        ),
        // Go: "Window name '%s' is not defined."
        DriverError::WindowNoSuchWindow(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_NO_SUCH_WINDOW,
            format!("Window name '{name}' is not defined."),
        ),
        // Go: "There is a circularity in the window dependency graph."
        DriverError::WindowCircularity => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_CIRCULARITY_IN_WINDOW_GRAPH,
            "There is a circularity in the window dependency graph.".to_owned(),
        ),
        // Go: "A window which depends on another cannot define partitioning."
        DriverError::WindowNoChildPartitioning => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_NO_CHILD_PARTITIONING,
            "A window which depends on another cannot define partitioning.".to_owned(),
        ),
        // Go: "Window '%s' cannot inherit '%s' since both contain an ORDER BY
        // clause." -- an inline `OVER (w ORDER BY ...)` has no name of its
        // own, which Go reports as `<unnamed window>`.
        DriverError::WindowNoRedefineOrderBy { window, base } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_NO_REDEFINE_ORDER_BY,
            format!(
                "Window '{window}' cannot inherit '{base}' since both contain an \
                 ORDER BY clause."
            ),
        ),
        // Go: "Window '%s' has a frame definition, so cannot be referenced by
        // another window."
        DriverError::WindowNoInheritFrame(base) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_NO_INHERENT_FRAME,
            format!(
                "Window '{base}' has a frame definition, so cannot be referenced by \
                 another window."
            ),
        ),
        // Go: "This version of TiDB doesn't yet support '%s'".
        DriverError::NotSupportedYet(feature) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_NOT_SUPPORTED_YET,
            format!("This version of TiDB doesn't yet support '{feature}'"),
        ),
        // Go: "Window '%s': frame start or end is negative, NULL or of
        // non-integral type" -- an inline `OVER (...)` is `<unnamed window>`.
        DriverError::WindowFrameIllegal => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_FRAME_ILLEGAL,
            "Window '<unnamed window>': frame start or end is negative, NULL or of \
             non-integral type"
                .to_owned(),
        ),
        // Go: "Window '%s': frame start cannot be UNBOUNDED FOLLOWING." --
        // `checkOriginWindowSpec`'s FIRST rule, so it outranks every other
        // frame complaint including a malformed end offset.
        DriverError::WindowFrameStartIllegal => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_FRAME_START_ILLEGAL,
            "Window '<unnamed window>': frame start cannot be UNBOUNDED FOLLOWING.".to_owned(),
        ),
        // Go: "Window '%s': frame end cannot be UNBOUNDED PRECEDING."
        DriverError::WindowFrameEndIllegal => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_FRAME_END_ILLEGAL,
            "Window '<unnamed window>': frame end cannot be UNBOUNDED PRECEDING.".to_owned(),
        ),
        // Go: "Window '%s' with RANGE N PRECEDING/FOLLOWING frame requires
        // exactly one ORDER BY expression, of numeric or temporal type".
        DriverError::WindowRangeFrameOrderType => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_RANGE_FRAME_ORDER_TYPE,
            "Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires \
             exactly one ORDER BY expression, of numeric or temporal type"
                .to_owned(),
        ),
        // Go: "Window '%s' with RANGE frame has ORDER BY expression of
        // datetime type. Only INTERVAL bound value allowed."
        DriverError::WindowRangeFrameTemporalType => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_RANGE_FRAME_TEMPORAL_TYPE,
            "Window '<unnamed window>' with RANGE frame has ORDER BY expression of \
             datetime type. Only INTERVAL bound value allowed."
                .to_owned(),
        ),
        // Go: "Window '%s' with RANGE frame has ORDER BY expression of
        // numeric type, INTERVAL bound value not allowed."
        DriverError::WindowRangeFrameNumericType => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WINDOW_RANGE_FRAME_NUMERIC_TYPE,
            "Window '<unnamed window>' with RANGE frame has ORDER BY expression of \
             numeric type, INTERVAL bound value not allowed."
                .to_owned(),
        ),
        // Go: "Invalid use of group function".
        DriverError::InvalidGroupFuncUse => MysqlError::terror(
            &tidb_error::plannererrors::ERR_INVALID_GROUP_FUNC_USE,
            "Invalid use of group function".to_owned(),
        ),
        // Go: "Argument #%d of GROUPING function is not in GROUP BY"
        // (`resolveGroupingFuncArgsInGroupBy`, the eval-facing rewrite arm).
        DriverError::FieldInGroupingNotGroupBy(position) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_FIELD_IN_GROUPING_NOT_GROUP_BY,
            format!("Argument #{position} of GROUPING function is not in GROUP BY"),
        ),
        // Go: "Argument %s of GROUPING function is not in GROUP BY" fed
        // `strconv.Itoa(offset+1)` (`checkOnlyFullGroupByWithGroupClause`'s
        // rollup arm), so this spelling carries NO `#`.
        DriverError::FieldInGroupingNotGroupByArg { position } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_FIELD_IN_GROUPING_NOT_GROUP_BY,
            format!("Argument {position} of GROUPING function is not in GROUP BY"),
        ),
        // Go: "Unknown column '%-.192s' in '%-.192s'".
        DriverError::UnknownColumnInTable { column, table } => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_COLUMN_NOT_EXISTS,
            format!("Unknown column '{}' in '{table}'", column.go_to_lower()),
        ),
        // Go `ErrNoReferencedTable`.
        DriverError::ForeignKeyReferencedTableMissing(table) => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT,
            format!("Failed to open the referenced table '{table}'"),
        ),
        // Go `ErrForeignKeyNoColumn`.
        DriverError::ForeignKeyReferencedColumnMissing {
            column,
            constraint,
            table,
        } => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT,
            format!(
                "Failed to add the foreign key constraint. Missing column '{column}' for constraint '{constraint}' in the referenced table '{table}'"
            ),
        ),
        // Go `ErrKeyColumnDoesNotExits`.
        DriverError::ForeignKeyChildColumnMissing(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS,
            format!("Key column '{column}' doesn't exist in table"),
        ),
        // Go: "BLOB/TEXT column '%-.192s' used in key specification without a
        // key length".
        DriverError::BlobKeyWithoutLength(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_BLOB_KEY_WITHOUT_LENGTH,
            format!("BLOB/TEXT column '{column}' used in key specification without a key length"),
        ),
        // Go: "Incorrect prefix key; ...". The message names nothing, which
        // is why the variant carries nothing.
        DriverError::IncorrectPrefixKey => MysqlError::terror(
            &tidb_util::dbterror::ERR_INCORRECT_PREFIX_KEY,
            "Incorrect prefix key; the used key part isn't a string, the used length is longer \
             than the key part, or the storage engine doesn't support unique prefix keys"
                .to_owned(),
        ),
        // Go: "Key part '%-.192s' length cannot be 0".
        DriverError::KeyPart0(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_KEY_PART0,
            format!("Key part '{column}' length cannot be 0"),
        ),
        // Go: "The used storage engine can't index column '%-.192s'".
        DriverError::WrongKeyColumn(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_WRONG_KEY_COLUMN,
            format!("The used storage engine can't index column '{column}'"),
        ),
        // Go: "Specified key was too long (%d bytes); max key length is %d
        // bytes".
        DriverError::TooLongKey { length, max } => MysqlError::terror(
            &tidb_util::dbterror::ERR_TOO_LONG_KEY,
            format!("Specified key was too long ({length} bytes); max key length is {max} bytes"),
        ),
        // Go: "JSON column '%-.192s' cannot be used in key specification."
        DriverError::JsonUsedInKey(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_JSON_USED_AS_KEY,
            format!("JSON column '{column}' cannot be used in key specification."),
        ),
        // Go: "BLOB/TEXT/JSON column '%-.192s' can't have a default value".
        DriverError::BlobCantHaveDefault(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_BLOB_CANT_HAVE_DEFAULT,
            format!("BLOB/TEXT/JSON column '{column}' can't have a default value"),
        ),
        // Go: "Truncated incorrect %-.32s value: '%-.128s'".
        DriverError::TruncatedIncorrectValue { kind, value } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TRUNCATED_WRONG_VAL,
            format!("Truncated incorrect {kind} value: '{value}'"),
        ),
        // Go: "Data Too Long, field len %d, data len %d".
        DriverError::DataTooLongRaw {
            field_len,
            data_len,
        } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_DATA_TOO_LONG,
            format!("Data Too Long, field len {field_len}, data len {data_len}"),
        ),
        // Go's message TEMPLATE, unformatted -- see the variant's doc.
        DriverError::DataTruncatedUnformatted => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TRUNCATED,
            "Data truncated for column '%s' at row %d".to_owned(),
        ),
        // Go: "Incorrect %-.32s value: '%-.128s'".
        DriverError::IncorrectValueRaw { type_name, value } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_WRONG_VALUE,
            format!("Incorrect {type_name} value: '{value}'"),
        ),
        // Go: "Data truncated for column '%s', value is '%s'".
        DriverError::DataTruncatedValue { column, value } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TRUNCATED,
            format!("Data truncated for column '{column}', value is '{value}'"),
        ),
        // Go: "Invalid use of NULL value".
        DriverError::PasswordExpireAnonymousUser => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_PASSWORD_EXPIRE_ANONYMOUS_USER,
            "The password for anonymous user cannot be expired.".to_owned(),
        ),
        DriverError::InvalidUseOfNull => {
            MysqlError::terror(
                &tidb_util::dbterror::ERR_INVALID_USE_OF_NULL,
                "Invalid use of NULL value".to_owned(),
            )
        }
        // Go: "Data truncated for column '%s' at row %d".
        DriverError::DataTruncatedAtRow { column, row } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TRUNCATED,
            format!("Data truncated for column '{column}' at row {row}"),
        ),
        // TiDB: "Unsupported drop integer primary key".
        DriverError::UnsupportedDropIntegerPrimaryKey => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_PK_HANDLE,
            "Unsupported drop integer primary key".to_owned(),
        ),
        // Go: "Table '%-.192s' already exists".
        DriverError::Schema(crate::SchemaErrorKind::TableExists(name)) => {
            MysqlError::terror(
                &tidb_error::catalog::infoschema::ERR_TABLE_EXISTS,
                format!("Table '{name}' already exists"),
            )
        }
        DriverError::Schema(crate::SchemaErrorKind::WrongTableName(name)) => {
            MysqlError::terror(
                &tidb_util::dbterror::ERR_WRONG_TABLE_NAME,
                format!("Incorrect table name '{name}'"),
            )
        }
        // Go: "Unknown table '%-.129s'" -- DROP TABLE's own code, distinct
        // from the 1146 a read of a missing table reports.
        DriverError::Schema(crate::SchemaErrorKind::BadTable(name)) => {
            MysqlError::terror(
                &tidb_error::catalog::infoschema::ERR_TABLE_DROP_EXISTS,
                format!("Unknown table '{name}'"),
            )
        }
        // Go: "Table '%-.192s' doesn't exist".
        DriverError::Schema(crate::SchemaErrorKind::UnknownTable(name)) => {
            MysqlError::terror(
                &tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS,
                format!("Table '{name}' doesn't exist"),
            )
        }
        // Go: "Error on rename of '%-.210s' to '%-.210s' (errno: %d - %s)",
        // whose nested errno is the fixed 168 `ExtractTblInfos` passes.
        DriverError::Schema(crate::SchemaErrorKind::RenameTargetDatabaseMissing {
            from,
            to,
            database,
        }) => MysqlError::terror(
            &tidb_util::dbterror::ERR_ERROR_ON_RENAME,
            format!(
                "Error on rename of '{from}' to '{to}' (errno: 168 - Database `{database}` doesn't exist)"
            ),
        ),
        DriverError::Schema(crate::SchemaErrorKind::UnknownSequence(name)) => {
            MysqlError::terror(
                &tidb_error::catalog::infoschema::ERR_SEQUENCE_DROP_EXISTS,
                format!("Unknown SEQUENCE: '{name}'"),
            )
        }
        DriverError::Schema(crate::SchemaErrorKind::SequenceValuesConflicting(name)) => {
            MysqlError::terror(
                &tidb_util::dbterror::ERR_SEQUENCE_INVALID_DATA,
                format!("Sequence '{name}' values are conflicting"),
            )
        }
        // Go: "'%-.192s.%-.192s' is not %s".
        DriverError::Schema(crate::SchemaErrorKind::WrongObject { name, expected }) => {
            MysqlError::terror(
                &tidb_error::catalog::infoschema::ERR_WRONG_OBJECT,
                format!("'{name}' is not {expected}"),
            )
        }
        // Go: "View '%-.192s.%-.192s' references invalid table(s) ...".
        DriverError::Schema(crate::SchemaErrorKind::ViewInvalid(name)) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_VIEW_INVALID,
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
            let sql_error = tidb_util::sqlkiller::memory_exceed_for_query(conn_id);
            let mut state = *b"HY000";
            state.copy_from_slice(sql_error.state.as_bytes());
            MysqlError::from_parts(sql_error.code, state, sql_error.message)
        }
        // Go raises this one as a plain error, so it carries 1105.
        // Go: "JSON documents may not contain NULL member names."
        DriverError::JsonDocumentNullKey => MysqlError::terror(
            &tidb_error::catalog::types::ERR_JSON_DOCUMENT_NULL_KEY,
            "JSON documents may not contain NULL member names.".to_owned(),
        ),
        // Go: "Cannot create a JSON value from a string with CHARACTER SET
        // '%s'." (`types.ErrInvalidJSONCharset`, captured verbatim from a
        // BINARY-charset `JSON_OBJECTAGG` key).
        DriverError::InvalidJsonCharset { charset } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_INVALID_JSON_CHARSET,
            format!("Cannot create a JSON value from a string with CHARACTER SET '{charset}'."),
        ),
        // Go raises these with a bare `errors.New`/`fmt.Errorf`, so they carry
        // no error class and reach the client as 1105.
        DriverError::ApproxPercentileArgument(message) => {
            MysqlError::unknown((*message).to_owned())
        }
        DriverError::SequenceUnsupportedTableOption(option) => MysqlError::terror(
            &tidb_util::dbterror::ERR_SEQUENCE_UNSUPPORTED_TABLE_OPTION,
            format!("Unsupported sequence table-option {option}"),
        ),
        DriverError::PercentageOutOfRange(percent) => MysqlError::unknown(
            format!("Percentage value {percent} is out of range [1, 100]"),
        ),
        DriverError::CheckConstraintNotExists(name) => MysqlError::terror(
            &tidb_util::dbterror::ERR_CONSTRAINT_NOT_FOUND,
            format!("Constraint '{name}' does not exist."),
        ),
        DriverError::CheckConstraintViolated(name) => MysqlError::terror(
            &tidb_error::catalog::table::ERR_CHECK_CONSTRAINT_VIOLATED,
            format!("Check constraint '{name}' is violated."),
        ),
        // Go's own wording, including the colon with no following space.
        DriverError::BindingHintedSqlMismatch { origin, hinted } => MysqlError::unknown(format!(
                "hinted sql and origin sql don't match when hinted sql erase the hint info, \
                 after erase hint info, originSQL:{origin}, hintedSQL:{hinted}"
            ),
        ),
        DriverError::InsertIntoViewUnsupported(name) => MysqlError::unknown(
            format!("insert into view {name} is not supported now"),
        ),
        DriverError::DeleteViewUnsupported(name) => MysqlError::unknown(
            format!("delete view {name} is not supported now"),
        ),
        DriverError::InsertIntoSequenceUnsupported(name) => MysqlError::unknown(
            format!("insert into sequence {name} is not supported now"),
        ),
        DriverError::DeleteSequenceUnsupported(name) => MysqlError::unknown(
            format!("delete sequence {name} is not supported now"),
        ),
        // Go: "In definition of view, derived table or common table
        // expression, SELECT list and column names list have different column
        // counts".
        DriverError::ViewWrongList => MysqlError::terror(
            &tidb_util::dbterror::ERR_VIEW_WRONG_LIST,
            "In definition of view, derived table or common table expression, SELECT list and \
             column names list have different column counts"
                .to_owned(),
        ),
        DriverError::CteRecursiveRequiresUnion(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_CTERECURSIVE_REQUIRES_UNION,
            format!("Recursive Common Table Expression '{name}' should contain a UNION"),
        ),
        DriverError::CteRecursiveRequiresNonRecursiveFirst(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_CTERECURSIVE_REQUIRES_NON_RECURSIVE_FIRST,
            format!(
                "Recursive Common Table Expression '{name}' should have one or more \
                 non-recursive query blocks followed by one or more recursive ones"
            ),
        ),
        DriverError::CteRecursiveForbidsAggregation(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_CTERECURSIVE_FORBIDS_AGGREGATION,
            format!(
                "Recursive Common Table Expression '{name}' can contain neither aggregation \
                 nor window functions in recursive query block"
            ),
        ),
        DriverError::CteRecursiveForbiddenJoinOrder(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_INVALID_REQUIRES_SINGLE_REFERENCE,
            format!(
                "In recursive query block of Recursive Common Table Expression '{name}', the \
                 recursive table must be referenced only once, and not in any subquery"
            ),
        ),
        DriverError::CteMaxRecursionDepth(rounds) => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_CTE_MAX_RECURSION_DEPTH,
            format!(
                "Recursive query aborted after {rounds} iterations. Try increasing \
                 @@cte_max_recursion_depth to a larger value"
            ),
        ),
        // Go `ErrInvalidLateralJoin`: "Invalid use of LATERAL: %s".
        DriverError::InvalidLateralJoin(reason) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_INVALID_LATERAL_JOIN,
            format!("Invalid use of LATERAL: {reason}"),
        ),
        // Go: "Every derived table must have its own alias".
        DriverError::DerivedMustHaveAlias => MysqlError::terror(
            &tidb_util::dbterror::ERR_DERIVED_MUST_HAVE_ALIAS,
            "Every derived table must have its own alias".to_owned(),
        ),
        // Go: "The target table %-.100s of the %s is not updatable".
        DriverError::TableNotUpdatable(name) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_NON_UPDATABLE_TABLE,
            format!("The target table {name} of the UPDATE is not updatable"),
        ),
        // Go `ErrSpecificAccessDenied` (1227), the general form.
        DriverError::SpecificAccessDenied(privileges) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED,
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
        } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_DBACCESS_DENIED,
            format!("Access denied for user '{user}'@'{host}' to database '{database}'"),
        ),
        // Go `ErrTableaccessDenied` (1142).
        DriverError::TableAccessDenied {
            privilege,
            user,
            host,
            table,
        } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_TABLEACCESS_DENIED,
            format!("{privilege} command denied to user '{user}'@'{host}' for table '{table}'"),
        ),
        // Go `ErrPrivilegeCheckFail` (8121). Go's message deliberately
        // begins lowercase.
        DriverError::PrivilegeCheckFail(privilege) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_PRIVILEGE_CHECK_FAIL,
            format!("privilege check for '{privilege}' fail"),
        ),
        // Go `ErrCannotUser` (1396): "Operation %s failed for %.256s", quoted
        // `'user'@'host'` for CREATE USER.
        DriverError::CreateUserAlreadyExists { user, host } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_CANNOT_USER,
            format!("Operation CREATE USER failed for '{user}'@'{host}'"),
        ),
        // Go `ErrCannotUser` (1396): DROP USER prints every failed account
        // through `auth.UserIdentity.String`, unquoted `user@host`, joined
        // by commas.
        DriverError::DropUserMissing { accounts } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_CANNOT_USER,
            format!("Operation DROP USER failed for {accounts}"),
        ),
        // Go `ErrCannotUser` (1396) for ALTER USER, quoted like CREATE USER.
        DriverError::AlterUserMissing { user, host } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_CANNOT_USER,
            format!("Operation ALTER USER failed for '{user}'@'{host}'"),
        ),
        DriverError::AlterUserFailed(accounts) => {
            MysqlError::terror(
                &tidb_util::dbterror::exeerrors::ERR_CANNOT_USER,
                format!("Operation ALTER USER failed for {accounts}"),
            )
        }
        // Go `types.ErrWrongValue2` (1525) with the `DAY` unit name, the
        // error `loadOptions` raises for a zero or > 65535 interval.
        DriverError::PasswordExpireIntervalOutOfRange { days } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_WRONG_VALUE2,
            format!("Incorrect DAY value: '{days}'"),
        ),
        // Go `errno.ErrMustChangePassword` (1820), the sandbox-mode gate.
        DriverError::MustChangePassword => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_MUST_CHANGE_PASSWORD,
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
        } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_CANNOT_USER,
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
        DriverError::PasswordInHistory { user, host } => {
            let message = tidb_error::tidb::errname::ErrExistsInHistoryPassword;
            let error = tidb_error::mysql::SqlError::new_f(
                tidb_error::tidb::errcode::ErrExistsInHistoryPassword,
                message.raw,
                message.redact_arg_pos,
                &[
                    tidb_error::mysql::FormatArg::from(user.as_str()),
                    tidb_error::mysql::FormatArg::from(host.as_str()),
                ],
            );
            MysqlError::terror(
                &tidb_util::dbterror::exeerrors::ERR_EXISTS_IN_HISTORY_PASSWORD,
                error.message,
            )
        },
        // Go `ErrPasswordNoMatch` (1133).
        DriverError::SetPasswordNoMatchingRow => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_PASSWORD_NO_MATCH,
            "Can't find any matching row in the user table".to_owned(),
        ),
        // Go's `errors.Errorf` fallback (1105, ER_UNKNOWN_ERROR) -- the
        // dual-password plugin-capability refusal has no dedicated errno.
        DriverError::DualPasswordUnsupportedForPlugin { plugin } => MysqlError::unknown(
            format!("Dual password is not supported for users authenticating with plugin '{plugin}'"),
        ),
        // Go `ErrSecondPasswordCannotBeEmpty` (3878), message template from
        // `errno/errname.go` with the `%-.64s` width specifiers applied to
        // names far below the truncation width.
        DriverError::SecondPasswordCannotBeEmpty { user, host } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_SECOND_PASSWORD_CANNOT_BE_EMPTY,
            format!(
                "Empty password can not be retained as second password for user \
                 '{user}'@'{host}'."
            ),
        ),
        // Go `ErrPasswordCannotBeRetainedOnPluginChange` (3894).
        DriverError::PasswordCannotBeRetainedOnPluginChange { user, host } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_PASSWORD_CANNOT_BE_RETAINED_ON_PLUGIN_CHANGE,
            format!(
                "Current password can not be retained for user '{user}'@'{host}' because \
                 authentication plugin is being changed."
            ),
        ),
        // Go `ErrCurrentPasswordCannotBeRetained` (3895).
        DriverError::CurrentPasswordCannotBeRetained { user, host } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_CURRENT_PASSWORD_CANNOT_BE_RETAINED,
            format!(
                "Current password can not be retained for user '{user}'@'{host}' because new \
                 password is empty."
            ),
        ),
        // Go `ErrPluginIsNotLoaded` (1524).
        DriverError::PluginIsNotLoaded { plugin } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_PLUGIN_IS_NOT_LOADED,
            format!("Plugin '{plugin}' is not loaded"),
        ),
        // Go `ErrPasswordFormat` (1827).
        DriverError::PasswordFormat => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_PASSWORD_FORMAT,
            "The password hash doesn't have the expected format. Check if the correct \
             password algorithm is being used with the PASSWORD() function."
                .to_owned(),
        ),
        // Go `variable.ErrNotValidPassword` (1819).
        DriverError::NotValidPassword { reason } => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_NOT_VALID_PASSWORD,
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
        DriverError::CannotUserRole { operation, target } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_CANNOT_USER,
            format!("Operation {operation} failed for {target}"),
        ),
        // Go `ErrGrantRole` (3523).
        DriverError::GrantUnknownRole { role, host } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_GRANT_ROLE,
            format!("Unknown authorization ID `{role}`@`{host}`"),
        ),
        // Go `ErrRoleNotGranted` (3530): the role is backtick-quoted
        // (`RoleIdentity.String`) and the account bare (`UserIdentity.String`).
        DriverError::RoleNotGranted {
            role,
            role_host,
            user,
            host,
        } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_ROLE_NOT_GRANTED,
            format!("`{role}`@`{role_host}` is not granted to {user}@{host}"),
        ),
        // Go `ErrCantCreateUserWithGrant` (1410), SQLSTATE 42000.
        DriverError::GrantToUnknownUser => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_CANT_CREATE_USER_WITH_GRANT,
            "You are not allowed to create a user with GRANT".to_owned(),
        ),
        // Go `ErrDynamicPrivilegeNotRegistered` (3929).
        DriverError::DynamicPrivilegeNotRegistered(name) => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_DYNAMIC_PRIVILEGE_NOT_REGISTERED,
            format!("Dynamic privilege '{name}' is not registered with the server."),
        ),
        // Go `ErrIllegalPrivilegeLevel` (3619).
        DriverError::IllegalPrivilegeLevel(names) => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_ILLEGAL_PRIVILEGE_LEVEL,
            format!("Illegal privilege level specified for {names}"),
        ),
        // Go `ErrNonexistingGrant` (1141).
        DriverError::NonexistingGrant { user, host } => MysqlError::terror(
            &tidb_error::catalog::privileges::ERR_NONEXISTING_GRANT,
            format!("There is no such grant defined for user '{user}' on host '{host}'"),
        ),
        // Go `ErrIllegalGrantForTable` (1144).
        DriverError::IllegalGrantForTable => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_ILLEGAL_GRANT_FOR_TABLE,
            "Illegal GRANT/REVOKE command; please consult the manual to see which privileges \
             can be used"
                .to_owned(),
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
        )) => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS,
            format!("Unknown database '{name}'"),
        ),
        // Go: "Can't create database '%-.192s'; database exists".
        DriverError::Schema(crate::SchemaErrorKind::DatabaseExists(
            name,
        )) => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_DATABASE_EXISTS,
            format!("Can't create database '{name}'; database exists"),
        ),
        // Go: "No database selected".
        DriverError::Schema(crate::SchemaErrorKind::NoDatabaseSelected) => {
            MysqlError::terror(
                &tidb_error::plannererrors::ERR_NO_DB,
                "No database selected".to_owned(),
            )
        }
        // Go: "Incorrect argument type to variable '%-.64s'".
        DriverError::Var(crate::VarError::WrongTypeForVar(name)) => {
            MysqlError::terror(
                &tidb_error::catalog::variable::ERR_WRONG_TYPE_FOR_VAR,
                format!("Incorrect argument type to variable '{name}'"),
            )
        }
        // Go: "Variable '%-.64s' can't be set to the value of '%-.200s'".
        DriverError::Var(crate::VarError::WrongValueForVar(
            name,
            value,
        )) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_WRONG_VALUE_FOR_VAR,
            format!("Variable '{name}' can't be set to the value of '{value}'"),
        ),
        DriverError::Var(crate::VarError::SqlError(error)) => {
            MysqlError::from_sql_error(error.clone())
        }
        // Go: "Unknown system variable '%-.64s'".
        DriverError::Var(crate::VarError::UnknownSystemVariable(
            name,
        )) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_UNKNOWN_SYSTEM_VAR,
            format!("Unknown system variable '{name}'"),
        ),
        // Go `ErrVariableNoLongerSupported` (8136): a removed option is
        // accepted by SET for compatibility, but reads explain the removal
        // instead of returning dummy data.
        DriverError::Var(crate::VarError::RemovedSystemVariable {
            name,
            reason,
        }) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_VARIABLE_NO_LONGER_SUPPORTED,
            format!("option '{name}' is no longer supported. Reason: {reason}"),
        ),
        // Go: "Variable '%-.192s' is a %s variable".
        DriverError::Var(crate::VarError::ReadOnlyVariable(name)) => {
            MysqlError::terror(
                &tidb_error::catalog::variable::ERR_INCORRECT_SCOPE,
                format!("Variable '{name}' is a read only variable"),
            )
        }
        // Go `ErrLocalVariable` (1228): "Variable '%-.64s' is a SESSION
        // variable and can't be used with SET GLOBAL".
        DriverError::Var(crate::VarError::SessionOnlyVariable(name)) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_LOCAL_VARIABLE,
            format!("Variable '{name}' is a SESSION variable and can't be used with SET GLOBAL"),
        ),
        // Go `ErrGlobalVariable` (1229): "Variable '%-.64s' is a GLOBAL
        // variable and should be set with SET GLOBAL".
        DriverError::Var(crate::VarError::GlobalOnlyVariable(name)) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_GLOBAL_VARIABLE,
            format!("Variable '{name}' is a GLOBAL variable and should be set with SET GLOBAL"),
        ),
        // Go `ErrIncorrectGlobalLocalVar` (1238), read side: "Variable
        // '%-.192s' is a SESSION variable".
        DriverError::Var(crate::VarError::NoGlobalCopy(name)) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_INCORRECT_SCOPE,
            format!("Variable '{name}' is a SESSION variable"),
        ),
        // Go `ErrIncorrectScope`: "Variable '%-.192s' is a %s variable".
        DriverError::Var(crate::VarError::IncorrectScope(name, allowed)) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_INCORRECT_SCOPE,
            format!("Variable '{name}' is a {allowed} variable"),
        ),
        // A Validation closure's own `errors.Errorf`: no code, so 1105.
        DriverError::Var(crate::VarError::ValidationRefused(message)) => {
            MysqlError::unknown(message.clone())
        }
        // Go `ErrUnsupportedIsolationLevel` (8048).
        DriverError::Var(crate::VarError::UnsupportedIsolationLevel(level)) => {
            MysqlError::terror(
                &tidb_error::catalog::variable::ERR_UNSUPPORTED_ISOLATION_LEVEL,
                format!(
                    "The isolation level '{level}' is not supported. Set \
                     tidb_skip_isolation_level_check=1 to skip this error"
                ),
            )
        }
        // Go `ErrReadOnly` (1621): "%s variable '%s' is read-only. Use SET %s
        // to assign the value".
        DriverError::Var(crate::VarError::SessionScopeIsReadOnly(name)) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_READ_ONLY,
            format!("SESSION variable '{name}' is read-only. Use SET GLOBAL to assign the value"),
        ),
        DriverError::SnapshotTooOld(safe_point_time) => MysqlError::terror(
            &tidb_error::catalog::variable::ERR_SNAPSHOT_TOO_OLD,
            format!("snapshot is older than GC safe point {safe_point_time}"),
        ),
        DriverError::SubqueryReturnsMoreThanOneRow => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_SUBQUERY_MORE_THAN1_ROW,
            "Subquery returns more than 1 row".to_owned(),
        ),
        DriverError::MemTableRetrievalNeeded => MysqlError::unknown(
            "a memory table was read without its rows being retrieved".to_owned(),
        ),
        DriverError::SelectIntoMoreThanOneRow => MysqlError::sql_error(
            1172,
            "Result consisted of more than one row".to_owned(),
        ),
        DriverError::SelectIntoColumnMismatch => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            "The used SELECT statements have a different number of columns".to_owned(),
        ),
        // Go: "Reference '%-.64s' not supported (%s)".
        DriverError::IllegalReference { name, reason } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_ILLEGAL_REFERENCE,
            format!("Reference '{name}' not supported ({reason})"),
        ),
        // Go: "Column '%-.192s' in %-.192s is ambiguous".
        DriverError::AmbiguousColumnInClause { column, clause } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_AMBIGUOUS,
            format!("Column '{column}' in {clause} is ambiguous"),
        ),
        // Go: "Unknown column '%-.192s' in '%-.192s'".
        DriverError::UnknownColumnInClause { column, clause } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_UNKNOWN_COLUMN,
            format!("Unknown column '{}' in '{clause}'", column.go_to_lower()),
        ),
        // Go: "The value specified for generated column '%s' in table '%s' is
        // not allowed."
        DriverError::BadGeneratedColumn { column, table } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_BAD_GENERATED_COLUMN,
            format!(
                "The value specified for generated column '{column}' in table '{table}' is not allowed."
            ),
        ),
        DriverError::GeneratedColumnFunctionNotAllowed(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED,
            format!(
                "Expression of generated column '{column}' contains a disallowed function."
            ),
        ),
        // Go: "Generated column can refer only to generated columns defined
        // prior to it."
        DriverError::GeneratedColumnNonPrior => MysqlError::terror(
            &tidb_util::dbterror::ERR_GENERATED_COLUMN_NON_PRIOR,
            "Generated column can refer only to generated columns defined prior to it.".to_owned(),
        ),
        // The `CREATE TABLE ... PARTITION BY` refusals, each errno and
        // wording captured from real TiDB through a mock-store session. See
        // the executor's `ddl::table_partition`.
        DriverError::PartitionWrongExprInFunc => MysqlError::terror(
            &tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC,
            "Constant, random or timezone-dependent expressions in (sub)partitioning function are \
             not allowed"
                .to_owned(),
        ),
        DriverError::PartitionFuncWrongType => MysqlError::terror(
            &tidb_util::dbterror::ERR_PARTITION_FUNC_NOT_ALLOWED,
            "The PARTITION function returns the wrong type".to_owned(),
        ),
        DriverError::PartitionDuplicateField(field) => MysqlError::terror(
            &tidb_util::dbterror::ERR_SAME_NAME_PARTITION_FIELD,
            format!("Duplicate partition field name '{field}'"),
        ),
        DriverError::PartitionColumnValueWrongType => MysqlError::terror(
            &tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE,
            "Partition column values of incorrect type".to_owned(),
        ),
        DriverError::PartitionColumnList => MysqlError::terror(
            &tidb_error::catalog::ast::ERR_PARTITION_COLUMN_LIST,
            "Inconsistency in usage of column lists for partitioning".to_owned(),
        ),
        DriverError::PartitionTooMany => MysqlError::terror(
            &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS,
            "Too many partitions (including subpartitions) were defined".to_owned(),
        ),
        DriverError::PartitionSubpartition => MysqlError::terror(
            &tidb_error::catalog::ast::ERR_SUBPARTITION,
            "It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for \
             subpartitioning"
                .to_owned(),
        ),
        DriverError::PartitionManagementOnNonpartitioned => MysqlError::terror(
            &tidb_util::dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED,
            "Partition management on a not partitioned table is not possible".to_owned(),
        ),
        DriverError::PartitionDropNonexistent => MysqlError::terror(
            &tidb_util::dbterror::ERR_DROP_PARTITION_NON_EXISTENT,
            "Error in list of partitions to DROP".to_owned(),
        ),
        DriverError::PartitionDropLast => MysqlError::terror(
            &tidb_util::dbterror::ERR_DROP_LAST_PARTITION,
            "Cannot remove all partitions, use DROP TABLE instead".to_owned(),
        ),
        DriverError::PartitionOnlyRangeList(operation) => MysqlError::terror(
            &tidb_util::dbterror::ERR_ONLY_ON_RANGE_LIST_PARTITION,
            format!("{operation} PARTITION can only be used on RANGE/LIST partitions"),
        ),
        // Go: "COALESCE PARTITION can only be used on HASH/KEY partitions".
        DriverError::CoalesceOnlyOnHashPartition => MysqlError::terror(
            &tidb_util::dbterror::ERR_COALESCE_ONLY_ON_HASH_PARTITION,
            "COALESCE PARTITION can only be used on HASH/KEY partitions",
        ),
        // Go: "At least one partition must be coalesced".
        DriverError::CoalescePartitionNoPartition => MysqlError::terror(
            &tidb_error::catalog::ast::ERR_COALESCE_PARTITION_NO_PARTITION,
            "At least one partition must be coalesced",
        ),
        DriverError::PartitionUniqueKeyNeedAllFields(kind) => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF,
            format!("A {kind} must include all columns in the table's partitioning function"),
        ),
        // Go: "Comment for table partition '%-.64s' is too long (max = %d)"
        // (`errno/errname.go:765`), raised only under STRICT mode.
        DriverError::PartitionCommentTooLong { name, limit } => MysqlError::terror(
            &tidb_util::dbterror::ERR_TOO_LONG_TABLE_PARTITION_COMMENT,
            format!("Comment for table partition '{name}' is too long (max = {limit})"),
        ),
        DriverError::PartitionConstDomain => MysqlError::terror(
            &tidb_util::dbterror::ERR_PARTITION_CONST_DOMAIN,
            "Partition constant is out of partition function domain".to_owned(),
        ),
        DriverError::PartitionMetadataUnknown => MysqlError::terror(
            &tidb_error::catalog::table::ERR_UNKNOWN_PARTITION,
            // Verbatim, placeholders and all: Go raises the bare error.
            "Unknown partition '%-.64s' in table '%-.64s'".to_owned(),
        ),
        DriverError::PartitionMetadataIncomplete => MysqlError::unknown("Table partition metadata not correct, neither partition expression or list of \
             partition columns"
                .to_owned(),
        ),
        DriverError::PartitionNoParts(what) => MysqlError::terror(
            &tidb_error::catalog::ast::ERR_NO_PARTS,
            format!("Number of {what} = 0 is not an allowed value"),
        ),
        DriverError::PartitionSameName(name) => MysqlError::terror(
            &tidb_util::dbterror::ERR_SAME_NAME_PARTITION,
            format!("Duplicate partition name {name}"),
        ),
        DriverError::PartitionFunctionNotAllowed => MysqlError::terror(
            &tidb_util::dbterror::ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED,
            "This partition function is not allowed".to_owned(),
        ),
        DriverError::PartitionNullInValuesLessThan => MysqlError::terror(
            &tidb_util::dbterror::ERR_NULL_IN_VALUES_LESS_THAN,
            "Not allowed to use NULL value in VALUES LESS THAN".to_owned(),
        ),
        DriverError::PartitionFieldTypeNotAllowed(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION,
            format!("Field '{column}' is of a not allowed type for this type of partitioning"),
        ),
        DriverError::PlacementPolicyExists(name) => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_EXISTS,
            format!("Placement policy '{name}' already exists"),
        ),
        DriverError::PlacementPolicyNotExists(name) => MysqlError::terror(
            &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS,
            format!("Unknown placement policy '{name}'"),
        ),
        DriverError::PlacementPolicyInUse(name) => MysqlError::terror(
            &tidb_util::dbterror::ERR_PLACEMENT_POLICY_IN_USE,
            format!("Placement policy '{name}' is still in use"),
        ),
        DriverError::PartitionFieldNotFound => MysqlError::terror(
            &tidb_util::dbterror::ERR_FIELD_NOT_FOUND_PART,
            "Field in list of fields for partition function not found in table".to_owned(),
        ),
        DriverError::PartitionGlobalIndexNeeded(index) => MysqlError::terror(
            &tidb_util::dbterror::ERR_GLOBAL_INDEX_NOT_EXPLICITLY_SET,
            format!(
                "Global Index is needed for index '{index}', since the unique index is not \
                 including all partitioning columns, and GLOBAL is not given as IndexOption"
            ),
        ),
        DriverError::PartitionWrongValues { method, clause } => MysqlError::terror(
            &tidb_error::catalog::ast::ERR_PARTITION_WRONG_VALUES,
            format!("Only {method} PARTITIONING can use {clause} in partition definition"),
        ),
        DriverError::PartitionMaxValueNotLast => MysqlError::terror(
            &tidb_util::dbterror::ERR_PARTITION_MAXVALUE,
            "MAXVALUE can only be used in last partition definition".to_owned(),
        ),
        DriverError::PartitionsMustBeDefined(method) => MysqlError::terror(
            &tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED,
            format!("For {method} partitions each partition must be defined"),
        ),
        DriverError::PartitionRangeNotIncreasing => MysqlError::terror(
            &tidb_util::dbterror::ERR_RANGE_NOT_INCREASING,
            "VALUES LESS THAN value must be strictly increasing for each partition".to_owned(),
        ),
        DriverError::PartitionDuplicateListValue => MysqlError::terror(
            &tidb_util::dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART,
            "Multiple definition of same constant in list partitioning".to_owned(),
        ),
        DriverError::NoPartitionForValue(value) => MysqlError::terror(
            &tidb_error::catalog::table::ERR_NO_PARTITION_FOR_GIVEN_VALUE,
            format!("Table has no partition for value {value}"),
        ),
        DriverError::UnknownPartition { partition, table } => MysqlError::terror(
            &tidb_error::catalog::table::ERR_UNKNOWN_PARTITION,
            format!("Unknown partition '{partition}' in table '{table}'"),
        ),
        DriverError::PartitionClauseOnNonpartitioned => MysqlError::terror(
            &tidb_error::plannererrors::ERR_PARTITION_CLAUSE_ON_NONPARTITIONED,
            "PARTITION () clause on non partitioned table".to_owned(),
        ),
        DriverError::RowDoesNotMatchGivenPartitionSet => MysqlError::terror(
            &tidb_error::catalog::table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET,
            "Found a row not matching the given partition set".to_owned(),
        ),
        DriverError::PartitionValuesNotInt(partition) => MysqlError::terror(
            &tidb_util::dbterror::ERR_VALUES_IS_NOT_INT_TYPE,
            format!("VALUES value for partition '{partition}' must have type INT"),
        ),
        // The expression-index refusals, each errno and wording captured from
        // `gorun`. See the executor's `expression_index`'s module doc for the script.
        DriverError::WrongKeyColumnFunctionalIndex(expr) => MysqlError::terror(
            &tidb_util::dbterror::ERR_WRONG_KEY_COLUMN_FUNCTIONAL_INDEX,
            format!("The used storage engine cannot index the expression '{expr}'"),
        ),
        DriverError::FunctionalIndexOnJson => MysqlError::terror(
            &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION,
            "Cannot create an expression index on a function that returns a JSON or GEOMETRY value"
                .to_owned(),
        ),
        DriverError::FunctionalIndexOnBlob => MysqlError::terror(
            &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_BLOB,
            "Cannot create an expression index on an expression that returns a BLOB or TEXT. \
             Please consider using CAST"
                .to_owned(),
        ),
        DriverError::FunctionalIndexOnField => MysqlError::terror(
            &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_FIELD,
            "Expression index on a column is not supported. Consider using a regular index instead"
                .to_owned(),
        ),
        DriverError::FunctionalIndexFunctionNotAllowed(index) => MysqlError::terror(
            &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_FUNCTION_IS_NOT_ALLOWED,
            format!("Expression of expression index '{index}' contains a disallowed function"),
        ),
        DriverError::FunctionalIndexRowValue(index) => MysqlError::terror(
            &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ROW_VALUE_IS_NOT_ALLOWED,
            format!("Expression of expression index '{index}' cannot refer to a row value"),
        ),
        DriverError::GeneratedColumnRowValue(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_GENERATED_COLUMN_ROW_VALUE_IS_NOT_ALLOWED,
            format!("Expression of generated column '{column}' cannot refer to a row value"),
        ),
        DriverError::ExpressionIndexCanNotRefer(index) => MysqlError::terror(
            &tidb_util::dbterror::ERR_EXPRESSION_INDEX_CAN_NOT_REFER,
            format!("Expression index '{index}' cannot refer to an auto-increment column"),
        ),
        DriverError::UnsafeFunctionInExpressionIndex => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_EXPRESSION_INDEX,
            "Unsupported creating expression index containing unsafe functions without \
             allow-expression-index in config"
                .to_owned(),
        ),
        DriverError::WrongParamCountToNativeFct(name) => MysqlError::terror(
            &tidb_error::catalog::expression::ERR_INCORRECT_PARAMETER_COUNT,
            format!("Incorrect parameter count in the call to native function '{name}'"),
        ),
        // CAPTURED from TiDB: `alter table fi add index idx((a+b))` then any of
        // `drop column a` / `change column a z int` / `rename column a to z`
        // gives 3837 / HY000 / "Column 'a' has an expression index dependency
        // and cannot be dropped or renamed" -- the wording below, confirmed.
        DriverError::DependentByFunctionalIndex(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
            format!(
                "Column '{column}' has an expression index dependency and cannot be dropped or \
                 renamed"
            ),
        ),
        // Go `ErrDependentByGeneratedColumn`: the message has a trailing
        // period, unlike its two siblings here -- that is TiDB's own wording
        // (`errname` catalog), not a slip.
        DriverError::DependentByGeneratedColumn(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_DEPENDENT_BY_GENERATED_COLUMN,
            format!("Column '{column}' has a generated column dependency."),
        ),
        DriverError::DependentByPartitionFunctional(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_DEPENDENT_BY_PARTITION_FUNCTIONAL,
            format!(
                "Column '{column}' has a partitioning function dependency and cannot be dropped \
                 or renamed"
            ),
        ),
        DriverError::TooLongIdent(ident) => MysqlError::terror(
            &tidb_util::dbterror::ERR_TOO_LONG_IDENT,
            format!("Identifier name '{ident}' is too long"),
        ),
        // Go: "'%s' is not supported for generated columns."
        DriverError::UnsupportedOnGeneratedColumn(reason) => MysqlError::terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
            format!("'{reason}' is not supported for generated columns."),
        ),
        // Go: "Default value expression of column '%s' contains a disallowed
        // function: `%s`."
        DriverError::DefaultFunctionNotAllowed(column, function) => MysqlError::terror(
            &tidb_util::dbterror::ERR_DEF_VAL_GENERATED_NAMED_FUNCTION_IS_NOT_ALLOWED,
            format!(
                "Default value expression of column '{column}' contains a disallowed function: \
                 `{function}`."
            ),
        ),
        DriverError::UnsupportedSequenceDefaultType(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_COLUMN_TYPE_UNSUPPORTED_NEXT_VALUE,
            format!("Unsupported sequence default value for column type '{column}'"),
        ),
        DriverError::AddColumnSequenceDefault(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_ADD_COLUMN_WITH_SEQUENCE_AS_DEFAULT,
            format!("Unsupported using sequence as default value in add column '{column}'"),
        ),
        DriverError::BinlogUnsafeSystemFunction => MysqlError::terror(
            &tidb_util::dbterror::ERR_BINLOG_UNSAFE_SYSTEM_FUNCTION,
            "Statement is unsafe because it uses a system function that may return a different value on the slave".to_owned(),
        ),
        // Go: "Unknown table '%-.192s' in %-.32s".
        DriverError::UnknownTableInMultiDelete(table) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_UNKNOWN_TABLE,
            format!("Unknown table '{table}' in MULTI DELETE"),
        ),
        // Go: "The target table %-.100s of the %s is not updatable".
        // 1288 is absent from Go's `mysql.MySQLState`, so it carries the
        // default state.
        DriverError::NonUpdatableTable { table, statement } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_NON_UPDATABLE_TABLE,
            format!("The target table {table} of the {statement} is not updatable"),
        ),
        // Go: "Can't group on '%-.192s'".
        DriverError::WrongGroupField(field) => MysqlError::terror(
            &tidb_error::plannererrors::ERR_WRONG_GROUP_FIELD,
            format!("Can't group on '{field}'"),
        ),
        DriverError::FieldNotInGroupBy { position, clause, column } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_FIELD_NOT_IN_GROUP_BY,
            format!(
                "Expression #{position} of {clause} is not in GROUP BY clause and contains \
                 nonaggregated column '{column}' which is not functionally dependent on columns \
                 in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by"
            ),
        ),
        DriverError::AggregateOrderNonAggQuery { position } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_AGGREGATE_ORDER_NON_AGG_QUERY,
            format!(
                "Expression #{position} of ORDER BY contains aggregate function and applies to \
                 the result of a non-aggregated query"
            ),
        ),
        DriverError::FieldNotInAggregatedQuery { position, column } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_MIX_OF_GROUP_FUNC_AND_FIELDS,
            format!(
                "In aggregated query without GROUP BY, expression #{position} of SELECT list \
                 contains nonaggregated column '{column}'; this is incompatible with \
                 sql_mode=only_full_group_by"
            ),
        ),
        DriverError::FieldInOrderNotSelect { position, column } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_FIELD_IN_ORDER_NOT_SELECT,
            format!(
                "Expression #{position} of ORDER BY clause is not in SELECT list, references \
                 column '{column}' which is not in SELECT list; this is incompatible with DISTINCT"
            ),
        ),
        DriverError::AggregateInOrderNotSelect { position } => MysqlError::terror(
            &tidb_error::plannererrors::ERR_AGGREGATE_IN_ORDER_NOT_SELECT,
            format!(
                "Expression #{position} of ORDER BY clause is not in SELECT list, contains \
                 aggregate function; this is incompatible with DISTINCT"
            ),
        ),
        // Go: "Invalid default value for '%-.192s'".
        DriverError::InvalidDefault(column) => MysqlError::terror(
            &tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE,
            format!("Invalid default value for '{column}'"),
        ),
        DriverError::FieldGetDefaultFailed(column) => MysqlError::terror(
            &tidb_error::catalog::table::ERR_GET_DEFAULT_FAILED,
            format!("Field '{column}' get default value fail"),
        ),
        DriverError::PrimaryCantHaveNull => MysqlError::terror(
            &tidb_util::dbterror::ERR_PRIMARY_CANT_HAVE_NULL,
            "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, \
             use UNIQUE instead"
                .to_owned(),
        ),
        // Go: "Data too long for column '%s' at row %d".
        DriverError::DataTooLong { column, row } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_DATA_TOO_LONG,
            format!("Data too long for column '{column}' at row {row}"),
        ),
        // Go: "Out of range value for column '%s' at row %d".
        DriverError::DataOutOfRange { column, row } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_WARN_DATA_OUT_OF_RANGE,
            format!("Out of range value for column '{column}' at row {row}"),
        ),
        // Go `types.overflow`: "constant %v overflows %s".
        DriverError::ConstantOverflows { value, type_name } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_OVERFLOW,
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
            MysqlError::terror(
                &tidb_error::catalog::table::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                message,
            )
        }
        // Go: `types.ErrWrongValue` completed by `completeInsertErr`.
        DriverError::IncorrectTemporalValue {
            type_name,
            value,
            column,
            row,
        } => MysqlError::terror(
            &tidb_util::dbterror::exeerrors::ERR_TRUNCATE_WRONG_INSERT_VALUE,
            format!("Incorrect {type_name} value: '{value}' for column '{column}' at row {row}"),
        ),
        DriverError::TimestampInDSTTransition { value, timezone } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TIMESTAMP_IN_DST_TRANSITION,
            format!(
                "Timestamp is not valid, since it is in Daylight Saving Time transition '{value}' for time zone '{timezone}'"
            ),
        ),
        // Go: "Failed to read auto-increment value from storage engine",
        // which is what an exhausted allocator reports.
        DriverError::AutoincReadFailed => MysqlError::terror(
            &tidb_error::catalog::autoid::ERR_AUTOINC_READ_FAILED,
            "Failed to read auto-increment value from storage engine".to_owned(),
        ),
        DriverError::AutoRandReadFailed => MysqlError::terror(
            &tidb_error::catalog::autoid::ERR_AUTO_RAND_READ_FAILED,
            "Failed to read auto-random value from storage engine".to_owned(),
        ),
        DriverError::AutoIdUnavailable(detail) => MysqlError::unknown(detail),
        DriverError::InvalidAutoRandom(reason) => MysqlError::terror(
            &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM,
            format!("Invalid auto random: {reason}"),
        ),
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

    #[test]
    fn write_conflict_preserves_go_retryable_marker() {
        let error = DriverError::Txn(TxnErrorKind::WriteConflict).to_mysql_error();
        assert_eq!(error.code, ER_WRITE_CONFLICT);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(
            error.message,
            "Write conflict, please retry the transaction [try again later]"
        );
    }
}

/// A variable error as the driver error the wire layer renders.
impl From<tidb_variable::VarError> for DriverError {
    fn from(error: tidb_variable::VarError) -> Self {
        DriverError::Var(error)
    }
}

pub use tidb_util::sqlkiller::memory_exceed_for_query;

// The lower layers' errors, lifted into the executor's.

use tidb_kv::mem_quota::KillStop;
use tidb_kv::storage::StorageError;
use tidb_table::column::{CastError, ColumnError};
use tidb_table::generated_column::GenerationError;
use tidb_table::kv_table::KvTableError;

/// The SQL layer's view of a storage failure. A retryable route error is
/// Go's `ErrRegionUnavailable`; any other non-SQL failure is a 1105 carrying
/// the `Display` text verbatim.
impl From<StorageError> for DriverError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Sql(error) => Self::Mysql(error),
            StorageError::Retryable(_) => Self::Txn(TxnErrorKind::RegionUnavailable),
            other => Self::Exec(ExecError::Internal(other.to_string().into())),
        }
    }
}

impl From<StorageError> for ExecError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Sql(error) => Self::Mysql(error),
            other => Self::unsupported(other.to_string()),
        }
    }
}

impl From<KvTableError> for ExecError {
    fn from(error: KvTableError) -> Self {
        match error {
            KvTableError::Storage(error) => error.into(),
            KvTableError::ColumnCast(error) => Self::Mysql(error),
            KvTableError::Generation {
                eval: Some(error), ..
            }
            | KvTableError::PartitionEval(error) => Self::Eval(error),
            // The variant name reaches the message: keep the names stable.
            other => Self::unsupported(format!("table bytes failed to decode: {other:?}")),
        }
    }
}

/// A rendered cast error is the driver's `Mysql`; a typed evaluation error
/// stays `Exec(Eval)`.
impl From<CastError> for DriverError {
    fn from(error: CastError) -> Self {
        ColumnError::from(error).into()
    }
}

impl From<ColumnError> for DriverError {
    fn from(error: ColumnError) -> Self {
        match error {
            ColumnError::ColumnCannotBeNull(column) => Self::ColumnCannotBeNull(column),
            ColumnError::NoDefaultForField(field) => Self::NoDefaultForField(field),
            ColumnError::Mysql(error) => Self::Mysql(error),
            ColumnError::Eval(error) => Self::Exec(ExecError::Eval(error)),
            ColumnError::Parse(message) => Self::Parse(message),
        }
    }
}

impl From<GenerationError> for DriverError {
    fn from(error: GenerationError) -> Self {
        ColumnError::from(error).into()
    }
}

/// Go `SQLKiller.HandleSignal`'s two outcomes as the executor's errors, 1:1.
impl From<KillStop> for ExecError {
    fn from(stop: KillStop) -> Self {
        match stop {
            KillStop::MemoryExceedForQuery { conn_id } => Self::MemoryExceedForQuery { conn_id },
            KillStop::Killed(error) => Self::Killed(error),
        }
    }
}

impl From<KillStop> for DriverError {
    fn from(stop: KillStop) -> Self {
        ExecError::from(stop).into()
    }
}

#[cfg(test)]
mod lower_layer_parity_tests;
#[cfg(test)]
mod ddl_build_parity_tests;
