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
//! `tidb_error`'s and `tidb_dbterror`'s.


mod driver_error;
mod exec;
mod var;

pub use driver_error::DriverError;
pub use exec::coprocessor_evaluation_error;
pub use var::VarError;

mod exec_error;

pub use exec_error::ExecError;
use tidb_terror::MysqlError;

/// An error raised with its Go identity already rendered (Go returns it as a
/// plain `error`): the statement reports it unchanged.
impl From<MysqlError> for DriverError {
    fn from(error: MysqlError) -> Self {
        DriverError::Mysql(error)
    }
}

/// A Go `*terror.Error` raised as is (`return ErrX`, `ErrX.GenWithStackByArgs(...)`).
impl From<tidb_terror::TerrorError> for DriverError {
    fn from(error: tidb_terror::TerrorError) -> Self {
        DriverError::Mysql(MysqlError::from_terror(&error))
    }
}

impl From<ExecError> for DriverError {
    fn from(err: ExecError) -> Self {
        match err {
            // Go's error, raised by the executor, reported unchanged.
            ExecError::Mysql(error) => DriverError::Mysql(error),
            ExecError::MemoryExceedForQuery { conn_id } => DriverError::from(
                tidb_dbterror::exeerrors::ERR_MEMORY_EXCEED_FOR_QUERY
                    .generate_with_stack_by_args(&[conn_id.into()]),
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

    #[test]
    fn region_unavailable_uses_tidb_error_code() {
        let error = DriverError::from(tidb_txnkv::tikv_errors::ERR_REGION_UNAVAILABLE.generate_with_stack_by_args(&[])).to_mysql_error();
        assert_eq!(error.code, tidb_errno::errcode::ErrRegionUnavailable);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(error.message, "Region is unavailable");
    }

    #[test]
    fn display_matches_the_mysql_diagnostic() {
        let error = DriverError::unsupported("an unsupported source operation");
        assert_eq!(error.to_string(), error.clone().to_mysql_error().message);
    }
}


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
            StorageError::Retryable(_) => tidb_txnkv::tikv_errors::ERR_REGION_UNAVAILABLE
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
            KvTableError::Mysql(error) => Self::Mysql(error),
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
            ColumnError::ColumnCannotBeNull(column) => tidb_table::table_errors::ERR_COLUMN_CANT_NULL
                .generate_with_stack_by_args(&[column.into()])
                .into(),
            ColumnError::NoDefaultForField(field) => tidb_table::table_errors::ERR_NO_DEFAULT_VALUE
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
mod go_error_identity_tests;
