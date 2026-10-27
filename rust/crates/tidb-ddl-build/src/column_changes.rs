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

//! The column-change helpers the cluster MODIFY / CHANGE COLUMN path shares:
//! Go `ModifyColumn`'s qualifier check and the client error of a failed data
//! step.

use tidb_error::mysql::MysqlError;
use tidb_hack::GoToLower;

/// Go `ModifyColumn`'s check of a qualified column name (`db.t.c`): its
/// schema and table must be the altered table's.
pub fn check_column_qualifier(
    qualifier: &[String],
    database: &str,
    table: &str,
) -> Result<(), MysqlError> {
    check_column_qualifier_part(qualifier, 2, database, true)?;
    check_column_qualifier_part(qualifier, 1, table, false)
}

/// One part of a column qualifier: the schema (`from_end == 2`, written as
/// `db.t.c`) or the table (`from_end == 1`), compared case-insensitively
/// with the altered table's, `ErrWrongDBName` / `ErrWrongTableName` when it
/// names another.
fn check_column_qualifier_part(
    qualifier: &[String],
    from_end: usize,
    expected: &str,
    schema: bool,
) -> Result<(), MysqlError> {
    let written = match (qualifier, from_end) {
        ([schema, _], 2) => schema.as_str(),
        ([table], 1) | ([_, table], 1) => table.as_str(),
        _ => return Ok(()),
    };
    if written.is_empty() || written.go_to_lower() == expected.go_to_lower() {
        return Ok(());
    }
    Err(if schema {
        MysqlError::terror(
            &tidb_util::dbterror::ERR_WRONG_DB_NAME,
            format!("Incorrect database name '{written}'"),
        )
    } else {
        MysqlError::terror(
            &tidb_util::dbterror::ERR_WRONG_TABLE_NAME,
            format!("Incorrect table name '{written}'"),
        )
    })
}

/// The client error for a failed MODIFY / CHANGE COLUMN data step: Go's own
/// code and text for a value the new type cannot hold, a NULL under NOT NULL,
/// or a duplicate the rewritten entries collide on.
pub fn modify_column_error(error: tidb_table::kv_table::KvTableError) -> MysqlError {
    use tidb_table::kv_table::KvTableError;
    match error {
        KvTableError::TruncatedIncorrectValue { kind, value } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TRUNCATED_WRONG_VAL,
            format!("Truncated incorrect {kind} value: '{value}'"),
        ),
        KvTableError::DataTruncatedValue { column, value } => MysqlError::terror(
            &tidb_error::catalog::types::ERR_TRUNCATED,
            format!("Data truncated for column '{column}', value is '{value}'"),
        ),
        KvTableError::InvalidUseOfNull => MysqlError::terror(
            &tidb_util::dbterror::ERR_INVALID_USE_OF_NULL,
            "Invalid use of NULL value".to_owned(),
        ),
        KvTableError::Vector(message) => crate::errors::unsupported(message),
        KvTableError::DuplicateEntry { value, key } => MysqlError::terror(
            &tidb_error::catalog::kv::ERR_KEY_EXISTS,
            format!("Duplicate entry '{value}' for key '{key}'"),
        ),
        // Go `updateColumnWorker.reformatErrors`' text and code.
        KvTableError::ColumnCast(error) => error,
        other => crate::errors::parse_error(format!("column modification failed: {other:?}")),
    }
}
