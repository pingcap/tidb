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

//! Executor error vocabulary shared by every execution domain.

use tidb_expr::EvalError;

/// Why a statement could not be executed by this minimal executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    /// The SQL text could not be parsed as exactly one statement.
    Parse {
        /// Parser-provided description of the syntax failure.
        message: String,
        /// Byte offset at which parsing stopped or failed.
        offset: usize,
    },
    /// Not a `SELECT` or set operation.
    NotSelect,
    /// The query reads a real table (has a `FROM` other than `DUAL`).
    RequiresTable,
    /// A `*` wildcard, which needs a table to expand.
    Wildcard,
    /// A construct outside this executor's subset (named in the payload).
    Unsupported(&'static str),
    /// The protocol framing or query payload could not be decoded before SQL
    /// execution began.
    Protocol(String),
    /// A referenced table does not exist.
    UnknownTable(String),
    /// `ROLLBACK TO`/`RELEASE SAVEPOINT` named a savepoint that doesn't
    /// exist — either never defined, already released, already rolled
    /// past, or (a real MySQL rule, confirmed via `gorun`) simply outside
    /// any explicit transaction (`SAVEPOINT` there is a no-op that
    /// records nothing, so any LATER reference to it is unknown too).
    UnknownSavepoint(String),
    /// A `USING` join names a column that isn't in both sides.
    UnknownColumn(String),
    /// A table-local primary, unique, or ordinary index name is already in
    /// use. TiDB keeps those names in one case-insensitive namespace.
    DuplicateIndex(String),
    /// An `INSERT` conflicts with an existing `PRIMARY KEY` or `UNIQUE`
    /// value and does not supply `IGNORE`, `REPLACE`, or an `ON DUPLICATE
    /// KEY UPDATE` clause. This is a data error, not an unsupported syntax
    /// boundary.
    DuplicateKey,
    /// A shared-cluster autocommit statement exhausted its session retry
    /// budget after its local catalog snapshot became stale before publish.
    WriteConflict,
    /// `ONLY_FULL_GROUP_BY` (real MySQL/TiDB's default `sql_mode`): a
    /// non-aggregated column in the select list, `HAVING`, or `ORDER BY`
    /// of a `GROUP BY`/aggregate query that is not itself one of the
    /// `GROUP BY` expressions — see `crate::aggregate::check_group_by_scope`.
    UngroupedColumn(String),
    /// An `INSERT` row has a column count the table does not match.
    ColumnCountMismatch,
    /// A value written to a `VARCHAR(n)` column exceeds `n` characters and
    /// the excess is not all trailing spaces (which would truncate
    /// silently instead) — real TiDB's own `Data too long for column`
    /// error under the default strict `sql_mode` (confirmed via `gorun`).
    /// The payload is the column name.
    DataTooLong(String),
    /// A numeric value written to a `DECIMAL(p,s)` column overflows its
    /// integer-digit budget (`p - s` digits) even AFTER rounding to scale
    /// `s` — real TiDB's own `Out of range value for column` error
    /// (confirmed via `gorun`: `123.4` into `DECIMAL(4,2)` errors, and so
    /// does `99.995` since it rounds to `100.00`). A genuinely different
    /// error from [`ExecError::DataTooLong`] (which MySQL uses for
    /// string/bit width), so it gets its own variant. The payload is the
    /// column name.
    OutOfRange(String),
    /// A row's `FOREIGN KEY` columns (all non-`NULL`) match no row in the
    /// referenced table.
    ForeignKeyViolation,
    /// An expression outside the evaluator's domain.
    Eval(EvalError),
}

impl From<EvalError> for ExecError {
    fn from(e: EvalError) -> Self {
        ExecError::Eval(e)
    }
}

impl From<tidb_parser::ParseError> for ExecError {
    fn from(error: tidb_parser::ParseError) -> Self {
        ExecError::Parse {
            message: error.message,
            offset: error.offset,
        }
    }
}
