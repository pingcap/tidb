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

//! Shared execution boundaries for physical table-reference clauses.
//!
//! Go validates partition selection while building table plans
//! (`pkg/planner/core/planbuilder.go`, including
//! `ErrPartitionClauseOnNonpartitioned`), plans snapshot reads from the
//! table's `AS OF` expression in that same builder, and constructs
//! `TABLESAMPLE` execution in `pkg/executor/builder.go`. The seed has only
//! non-partitioned, single-version, in-memory rows, so all three capabilities
//! are rejected here before either SELECT or DML reaches table data.

use tidb_ast::{Expr, TableSample};

use crate::error::ExecError;

/// Rejects `PARTITION (...)`: every seed table is non-partitioned.
pub(crate) fn check_no_partition(partitions: &[String]) -> Result<(), ExecError> {
    if partitions.is_empty() {
        Ok(())
    } else {
        Err(ExecError::Unsupported(
            "PARTITION clause on non-partitioned table",
        ))
    }
}

/// Rejects `TABLESAMPLE`: an in-memory row vector has no TiKV region sample.
pub(crate) fn check_no_table_sample(sample: &Option<TableSample>) -> Result<(), ExecError> {
    if sample.is_none() {
        Ok(())
    } else {
        Err(ExecError::Unsupported("TABLESAMPLE clause"))
    }
}

/// Rejects historical reads: the seed has no MVCC versions to select from.
pub(crate) fn check_no_as_of(as_of: &Option<Box<Expr>>) -> Result<(), ExecError> {
    if as_of.is_none() {
        Ok(())
    } else {
        Err(ExecError::Unsupported("AS OF TIMESTAMP clause"))
    }
}
