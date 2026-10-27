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

//! Go `pkg/table/table.go`'s `Err*` family: what encoding, decoding or
//! writing a table's bytes can fail with.

use tidb_kv::storage::StorageError;
use tidb_error::mysql::MysqlError;

/// A failure while encoding or decoding table bytes.
#[derive(Debug)]
pub enum KvTableError {
    /// The error value Go's table layer raises (`ErrKeyExists`,
    /// `ErrNoPartitionForGivenValue`, `ErrCheckConstraintViolated`, a failed
    /// column cast, ...), as Go raises it.
    Mysql(MysqlError),
    /// A row failed to encode.
    Encode(String),
    /// A generated column's expression could not be evaluated for a row.
    /// DDL admits only expressions this tier can build, so this is a
    /// value-domain failure (an overflow, say), not an unported form.
    Generation {
        /// The generated column.
        column: String,
        /// The evaluation failure.
        detail: String,
        /// The evaluation failure itself, when it carries a MySQL code the
        /// statement has to report (1365 for a zero divisor under
        /// `ERROR_FOR_DIVISION_BY_ZERO`).
        eval: Option<tidb_expr::EvalError>,
    },
    /// A persisted CHECK expression could not be built or evaluated.
    CheckConstraint {
        /// Constraint name.
        name: String,
        /// Build/evaluation detail.
        detail: String,
        /// The evaluation error when evaluation, rather than metadata build,
        /// failed.
        eval: Option<tidb_expr::EvalError>,
    },
    /// A stored value failed to decode.
    Decode(String),
    /// The partition expression could not be evaluated over the row; the
    /// evaluation failure carries the MySQL code the statement reports (1365
    /// for a zero divisor under `ERROR_FOR_DIVISION_BY_ZERO`).
    PartitionEval(tidb_expr::EvalError),
    /// The storage layer refused a read or write.
    Storage(StorageError),
}

impl From<StorageError> for KvTableError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl KvTableError {
    /// The error a row write raises, as Go's table layer raises it: the
    /// `kv.ErrKeyExists`, `table.ErrNoPartitionForGivenValue`,
    /// `table.ErrCheckConstraintViolated`, ... terrors the statement reports.
    /// A failure with no SQL identity of its own keeps the parse-error
    /// wrapping the write path has always reported it under.
    #[must_use]
    pub fn into_mysql_error(self) -> MysqlError {
        match self {
            KvTableError::Storage(StorageError::Sql(error)) | KvTableError::Mysql(error) => error,
            KvTableError::Storage(StorageError::Retryable(_)) => MysqlError::terror(
                &tidb_error::catalog::tikv::ERR_REGION_UNAVAILABLE,
                tidb_error::tidb::errname::ErrRegionUnavailable.raw.to_owned(),
            ),
            KvTableError::Storage(other) => MysqlError::unknown(other.to_string()),
            KvTableError::Generation {
                eval: Some(eval), ..
            }
            | KvTableError::CheckConstraint {
                eval: Some(eval), ..
            }
            | KvTableError::PartitionEval(eval) => eval.to_mysql_error(),
            other => MysqlError::terror(
                &tidb_error::catalog::parser::ERR_PARSE,
                format!(
                    "You have an error in your SQL syntax; check the manual that corresponds \
                     to your TiDB version for the right syntax to use row encode failed: {other:?}"
                ),
            ),
        }
    }
}
