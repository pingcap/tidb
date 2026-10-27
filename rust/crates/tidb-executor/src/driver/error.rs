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
mod var;

pub use driver_error::DriverError;
pub use exec::coprocessor_evaluation_error;
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

/// A Go `*terror.Error` raised as is (`return ErrX`, `ErrX.GenWithStackByArgs(...)`).
impl From<tidb_error::terror::TerrorError> for DriverError {
    fn from(error: tidb_error::terror::TerrorError) -> Self {
        DriverError::Mysql(MysqlError::from_terror(&error))
    }
}

impl From<ExecError> for DriverError {
    fn from(err: ExecError) -> Self {
        match err {
            // The same statement-level error whichever layer raised it, so
            // callers match one variant.
            ExecError::SubqueryReturnsMoreThanOneRow => DriverError::from(
                tidb_util::dbterror::exeerrors::ERR_SUBQUERY_MORE_THAN1_ROW
                    .generate_with_stack_by_args(&[]),
            ),
            ExecError::CteMaxRecursionDepth(rounds) => DriverError::from(
                tidb_util::dbterror::exeerrors::ERR_CTE_MAX_RECURSION_DEPTH
                    .generate_with_stack_by_args(&[rounds.into()]),
            ),
            ExecError::MemoryExceedForQuery { conn_id } => {
                DriverError::from(tidb_util::dbterror::exeerrors::ERR_MEMORY_EXCEED_FOR_QUERY.generate_with_stack_by_args(&[(conn_id).into()]))
            }
            ExecError::JsonDocumentNullKey => DriverError::from(
                tidb_error::catalog::types::ERR_JSON_DOCUMENT_NULL_KEY
                    .generate_with_stack_by_args(&[]),
            ),
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
            ExecError::InvalidJsonCharset { charset } => DriverError::from(
                tidb_error::catalog::types::ERR_INVALID_JSON_CHARSET
                    .generate_with_stack_by_args(&[charset.into()]),
            ),
            other => DriverError::Exec(other),
        }
    }
}

impl DriverError {
    /// The code, SQLSTATE and message this failure reports.
    #[must_use]
    pub fn to_mysql_error(self) -> MysqlError {
        match self {
        DriverError::Mysql(error) => error,

        DriverError::Unsupported(message) => MysqlError::unknown(message),

        // Every execution and evaluation failure, one arm each, in `exec`.
        DriverError::Exec(error) => exec::to_mysql_error(error),

        // Go: "function %s has only noop implementation in tidb now, use
        // tidb_enable_noop_functions to enable these functions" (1235).
        DriverError::Var(crate::VarError::FunctionsNoopImpl(clause)) => MysqlError::terror(
            &tidb_error::catalog::expression::ERR_FUNCTIONS_NOOP_IMPL,
            format!(
                "function {clause} has only noop implementation in tidb now, use \
                 tidb_enable_noop_functions to enable these functions"
            ),
        ),
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

        DriverError::MemTableRetrievalNeeded => MysqlError::unknown(
            "a memory table was read without its rows being retrieved".to_owned(),
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
    /// MySQL `ER_PARSE_ERROR`.
    const ER_PARSE_ERROR: u16 = 1064;
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

    #[test]
    fn raw_incorrect_string_value_matches_table_cast_value() {
        // pkg/table/column_test.go::TestCastValue expects the raw table error,
        // before a statement-level caller has attached a row number.
        let error = tidb_table::column::incorrect_value("string", "\\x81", "", 0);

        assert_eq!(error.code, 1366);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(
            error.message,
            "Incorrect string value '\\x81' for column ''"
        );
    }

    #[test]
    fn region_unavailable_uses_tidb_error_code() {
        let error = DriverError::from(tidb_error::catalog::tikv::ERR_REGION_UNAVAILABLE.generate_with_stack_by_args(&[])).to_mysql_error();
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
            StorageError::Retryable(_) => tidb_error::catalog::tikv::ERR_REGION_UNAVAILABLE
                .generate_with_stack_by_args(&[])
                .into(),
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
            ColumnError::ColumnCannotBeNull(column) => tidb_error::catalog::table::ERR_COLUMN_CANT_NULL
                .generate_with_stack_by_args(&[column.into()])
                .into(),
            ColumnError::NoDefaultForField(field) => tidb_error::catalog::table::ERR_NO_DEFAULT_VALUE
                .generate_with_stack_by_args(&[field.into()])
                .into(),
            ColumnError::Mysql(error) => Self::Mysql(error),
            ColumnError::Eval(error) => Self::Exec(ExecError::Eval(error)),
            ColumnError::Parse(message) => tidb_util::misc::syntax_error(message).into(),
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
