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

//! `CREATE TABLE ... PARTITION BY`: the admission gate.
//!
//! Mirrors Go `pkg/ddl/partition.go`'s `buildTablePartitionInfo`, which is the
//! function that turns an `ast.PartitionOptions` into the `model.PartitionInfo`
//! a real TiDB stores on the table. This tier has no `PartitionInfo`, no
//! per-partition physical table ids, and no partition pruning, so it builds
//! NOTHING from that clause -- and therefore it must not accept it.
//!
//! # Why this is a refusal and not a silent discard
//!
//! There IS a shape of `CREATE TABLE ... PARTITION BY` that real TiDB accepts
//! while building an ordinary unpartitioned table: `buildTablePartitionInfo`
//! leaves `enable == false`, warns
//! `Unsupported partition type %v, treat as normal table`, and returns with
//! `tbInfo.Partition` unset. That branch is why "accept and ignore" can look
//! defensible. It is not, because the branch is unreachable for every method
//! the grammar admits today: RANGE and LIST set `enable = true`
//! unconditionally, and HASH/KEY set it in both arms of their own `if`
//! (`Tp == Hash || len(ColumnNames) != 0`, else `Tp == Key && len == 0`).
//! Reaching the warning needs a method that is none of the four, and
//! `PartitionTypeSystemTime` -- the only other value -- never survives
//! validation. So for every statement a user can write, Go builds a REAL
//! partitioned table.
//!
//! Captured from real TiDB (`zz_partcap`, a mock-store session; see the unit's
//! report for the raw transcript): `create table h1 (a int, b int) partition
//! by hash(a) partitions 4` succeeds with NO warning, and `SHOW CREATE TABLE
//! h1` restores the clause verbatim as
//! ``PARTITION BY HASH (`a`) PARTITIONS 4``. Building an unpartitioned table
//! for that statement is a silent wrong answer: `SHOW CREATE TABLE` loses the
//! clause, `information_schema.partitions` has no rows,
//! `SELECT ... PARTITION (p0)` cannot mean anything, and a RANGE table accepts
//! rows no partition covers instead of raising 1526.
//!
//! Refusing keeps this node's answer honest and matches every other
//! partition-aware path already in the tier: `ALTER TABLE`'s partition actions
//! refuse, `INSERT`/`SELECT ... PARTITION (...)` refuse (see
//! [`tidb_ast::InsertStmt::partitions`]), and `tidb_exec`'s own
//! `build_table_info` already refused `PARTITION BY` -- this executor's
//! `CREATE TABLE` path was the one hole left.
//!
//! # This is a tripwire, not a dead end
//!
//! [`refuse_table_partitioning`] is the single place the refusal lives. When
//! partitioning is implemented, this function is deleted and the tests that
//! pin it (`tests_partition` in `tidb-session`) flip from asserting a refusal
//! to asserting the captured `SHOW CREATE TABLE` text, which those tests
//! already carry verbatim.

use tidb_ast::CreateTableStmt;

use crate::DriverError;

/// The refusal a `CREATE TABLE` carrying `PARTITION BY` gets, named by the
/// partition method it wrote.
///
/// `Ok(())` for a statement with no partitioning clause, which is the whole
/// of what this tier can execute faithfully.
///
/// # Errors
///
/// [`DriverError::UnsupportedKind`] when the statement carries any
/// `PARTITION BY` clause.
pub fn refuse_table_partitioning(create: &CreateTableStmt) -> Result<(), DriverError> {
    let Some(partitioning) = &create.partitioning else {
        return Ok(());
    };
    // The method name is Go's own spelling, so the refusal reads like the
    // clause the user wrote rather than like a Rust variant.
    let method = partitioning.method.kind.sql();
    Err(DriverError::UnsupportedKind(format!(
        "CREATE TABLE ... PARTITION BY {method} is not supported by this node: \
         it stores no partition metadata, so accepting the clause would build \
         an ordinary unpartitioned table"
    )))
}
