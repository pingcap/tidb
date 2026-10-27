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
    /// A column cast failed with its original MySQL diagnostic.
    ColumnCast(MysqlError),
    /// A row failed to encode.
    Encode(String),
    /// Go `ErrDupKeyName` (1061).
    DuplicateKeyName(String),
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
    /// Go `table.ErrCheckConstraintViolated` (3819).
    CheckConstraintViolated(String),
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
    /// Go `ErrDupEntry` (1062): a row with this primary key already exists.
    DuplicateEntry {
        /// The rejected key value, as MySQL prints it.
        value: String,
        /// The violated key's name; Go names the clustered one PRIMARY.
        key: String,
    },
    /// Go `ErrTruncatedWrongValue` (1292): a stored value is not a valid
    /// number for the column's new numeric type.
    TruncatedIncorrectValue {
        /// The numeric domain Go names in the message.
        kind: &'static str,
        /// The value it could not read.
        value: String,
    },
    /// Go `ErrTruncatedWrongValueForField` (1265) with the value form: a
    /// stored value does not fit the column's new type.
    DataTruncatedValue {
        /// The column being modified.
        column: String,
        /// The value that does not fit.
        value: String,
    },
    /// Go's plain VECTOR conversion error. Vector dimension failures are not
    /// retitled as a generic truncation while a column is reorganized.
    Vector(String),
    /// Go `dbterror.ErrInvalidUseOfNull` (1138): a stored NULL is rejected
    /// while changing a column to `NOT NULL`.
    InvalidUseOfNull,
    /// Go `table.ErrNoPartitionForGivenValue` (1526): the row's partition
    /// value falls outside every partition. HASH cannot produce it; RANGE and
    /// LIST can (captured: `insert into r2 values (20,0)` on a RANGE table
    /// with no `MAXVALUE` partition is `Table has no partition for value 20`).
    NoPartitionForValue(String),
    /// Go `table.ErrRowDoesNotMatchGivenPartitionSet` (1748): an
    /// `INSERT ... PARTITION (...)` row routed to a partition the statement
    /// did not name.
    RowDoesNotMatchGivenPartitionSet,
    /// Go `types.ErrOverflow` (1690) raised by `locateHashPartition`'s
    /// `ConvertTo(TypeLonglong)`: the row's partition value has no signed
    /// reading, so the write fails instead of routing by a clamped one.
    PartitionValueOverflowsBigint(String),
    /// Go `autoid.ErrAutoincReadFailed` (1467) raised while allocating the
    /// row's `_tidb_rowid`, which shares the AUTO_INCREMENT counter.
    AutoIdExhausted,
    /// A stored value failed to decode.
    Decode(String),
    /// The partition expression could not be evaluated over the row; the
    /// evaluation failure carries the MySQL code the statement reports (1365
    /// for a zero divisor under `ERROR_FOR_DIVISION_BY_ZERO`).
    PartitionEval(tidb_expr::EvalError),
    /// The storage layer refused a read or write.
    Storage(StorageError),
    /// TiDB `ErrOptOnCacheTable` (8242).
    CacheTableUnsupported(&'static str),
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
            KvTableError::Storage(StorageError::Sql(error)) | KvTableError::ColumnCast(error) => {
                error
            }
            KvTableError::Storage(StorageError::Retryable(_)) => MysqlError::terror(
                &tidb_error::catalog::tikv::ERR_REGION_UNAVAILABLE,
                tidb_error::tidb::errname::ErrRegionUnavailable.raw.to_owned(),
            ),
            KvTableError::Storage(other) => MysqlError::unknown(other.to_string()),
            KvTableError::DuplicateEntry { value, key } => MysqlError::terror(
                &tidb_error::catalog::kv::ERR_KEY_EXISTS,
                format!("Duplicate entry '{value}' for key '{key}'"),
            ),
            KvTableError::Generation {
                eval: Some(eval), ..
            }
            | KvTableError::CheckConstraint {
                eval: Some(eval), ..
            }
            | KvTableError::PartitionEval(eval) => eval.to_mysql_error(),
            KvTableError::CheckConstraintViolated(name) => MysqlError::terror(
                &tidb_error::catalog::table::ERR_CHECK_CONSTRAINT_VIOLATED,
                format!("Check constraint '{name}' is violated."),
            ),
            KvTableError::NoPartitionForValue(value) => MysqlError::terror(
                &tidb_error::catalog::table::ERR_NO_PARTITION_FOR_GIVEN_VALUE,
                format!("Table has no partition for value {value}"),
            ),
            KvTableError::RowDoesNotMatchGivenPartitionSet => MysqlError::terror(
                &tidb_error::catalog::table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET,
                "Found a row not matching the given partition set".to_owned(),
            ),
            // Go's `ConvertTo` error out of `locateHashPartition`.
            KvTableError::PartitionValueOverflowsBigint(value) => MysqlError::terror(
                &tidb_error::catalog::types::ERR_OVERFLOW,
                format!("constant {value} overflows bigint"),
            ),
            KvTableError::AutoIdExhausted => MysqlError::terror(
                &tidb_error::catalog::autoid::ERR_AUTOINC_READ_FAILED,
                "Failed to read auto-increment value from storage engine".to_owned(),
            ),
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
