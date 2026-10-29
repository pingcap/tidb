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

//! The executor's rendering of an index backfill's failure; the index
//! builder itself is Go `pkg/ddl/index.go`'s, in `tidb_ddl_build::index`.

use super::DriverError;

/// Preserve the source SQL diagnostic for local and cluster index backfill.
pub fn index_backfill_error(error: tidb_table::kv_table::KvTableError) -> DriverError {
    match error {
        tidb_table::kv_table::KvTableError::Storage(error) => error.into(),
        tidb_table::kv_table::KvTableError::Mysql(error) => {
            DriverError::Exec(crate::ExecError::Mysql(error))
        }
        // The backfill computes each entry's value from the row, so a
        // generated column that cannot be evaluated fails the DDL with
        // its own code -- Go answers 1365 for
        // `ALTER TABLE t ADD INDEX ((100/a))` over a row with `a = 0`.
        tidb_table::kv_table::KvTableError::Generation {
            eval: Some(eval), ..
        } => DriverError::Exec(crate::ExecError::Eval(eval)),
        // Every other row-write failure keeps the code a DML write gives it
        // (a REORGANIZE row no new partition takes is Go's 1526).
        other => crate::driver::kv_write_error(other),
    }
}

