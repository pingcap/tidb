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

//! Go `ToTiDBErr` over the optimistic transaction coordinator's failures.

use std::fmt;

use tidb_txnkv::transaction::{OptimisticCoordinatorError, TransactionCause};

use crate::pessimistic_lock_error::{transaction_cause_to_sql_error, LockSqlError};

/// Preserve registered snapshot failures; other failures before a commit
/// verdict retain the existing generic transaction diagnostic.
pub fn coordinator_sql_error(error: OptimisticCoordinatorError) -> LockSqlError {
    match error {
        OptimisticCoordinatorError::Storage(error) => LockSqlError {
            code: error.mysql_code().as_u16(),
            state: *b"HY000",
            message: error.to_string(),
            write_conflict: None,
            identity: None,
        },
        OptimisticCoordinatorError::SnapshotBackoff { kind, detail } => {
            transaction_cause_to_sql_error(&TransactionCause::BackoffExhausted { kind, detail })
        }
        // Go `ToTiDBErr`: client-go's `ErrTxnAbortedByGC` becomes TiDB's
        // `ErrTxnAbortedByGC` (9006) and `ErrPDServerTimeout` keeps its own.
        OptimisticCoordinatorError::Visibility(error) => {
            match tidb_txnkv::to_tidb_driver_error(&error.into_storage_driver_error()) {
                tidb_txnkv::ConvertedDriverError::Terror(converted) => {
                    LockSqlError::from(tidb_error::mysql::MysqlError::from_terror(&converted))
                }
                other => engine_sql_error(other.to_string()),
            }
        }
        other => engine_sql_error(other.to_string()),
    }
}

/// A failure Go reports as a plain `transaction failed` error.
pub fn engine_sql_error(detail: impl fmt::Display) -> LockSqlError {
    LockSqlError::from(tidb_error::mysql::MysqlError::unknown(format!(
        "transaction failed: {detail}"
    )))
}

