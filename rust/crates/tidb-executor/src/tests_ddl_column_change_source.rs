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

#![allow(missing_docs)]

//! GO PORT of `pkg/ddl/column_change_test.go` (items 45-47 of the
//! pkg/ddl.part1 slice, read from `origin/master`).
//!
//! The Go file drives the on-line schema-change state machine: each statement
//! runs as a DDL job whose per-state behavior is inspected through the
//! `afterWaitSchemaSynced`/`beforeRunOneJobStep` failpoint hooks
//! (`checkAddWriteOnly`, `checkAddPublic`, `checkJobWithHistory`,
//! `testCheckJobDone`). This tier has no job state machine, so the
//! state-machine halves stay gaps. What the transcreated
//! `pkg/ddl/column.go` `AddColumn`/`DropColumn` lowering CAN pin is the
//! observable end state of the same statements, and the running test below
//! pins exactly those.

use crate::ddl::{run_alter_table_in, run_create_table_on};
use crate::driver::{run_insert_on, run_select_on};
use crate::{Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

/// Reads back `SELECT <sql>` as datum rows through the wired engine.
fn rows(catalog: &Catalog, sql: &str) -> Vec<Vec<tidb_datatype::Datum>> {
    run_select_on(sql, catalog, &ctx()).unwrap()
}

/// GO PORT of `pkg/ddl/column_change_test.go:41 TestColumnAdd`.
///
/// Go (driving the write-only/public state transitions via failpoint hooks)
/// pins three statements against `t (c1 int, c2 int)` holding one row
/// `(1, 2)`:
/// 1. `alter table t add column c3 int default 3` — the pre-existing row
///    reads `3` for the new column (checkAddPublic asserts the filled
///    default on the old record);
/// 2. `alter table t drop column c3` — the column disappears and the row is
///    back to two values;
/// 3. `alter table t add column c3 int` (no default) — the column comes back
///    and pre-existing rows read NULL.
///
/// The job-history and per-state halves of the Go test (the failpoint
/// inspections) have no counterpart here — go-parity-gap — but each of the
/// three end states above is asserted exactly.
#[test]
fn column_add_default_fill_then_drop_then_null_fill() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (c1 INT, c2 INT)", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t VALUES (1, 2)", &mut catalog, &ctx()).unwrap();

    // 1. Add column with default value: the pre-existing row reads 3.
    run_alter_table_in(
        "ALTER TABLE t ADD COLUMN c3 INT DEFAULT 3",
        &mut catalog,
        crate::driver::DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        rows(&catalog, "SELECT * FROM t"),
        vec![vec![
            tidb_datatype::Datum::Int(1),
            tidb_datatype::Datum::Int(2),
            tidb_datatype::Datum::Int(3),
        ]],
        "add column with DEFAULT must fill the pre-existing row (column_change_test.go:60)"
    );

    // 2. Drop the column again.
    run_alter_table_in(
        "ALTER TABLE t DROP COLUMN c3",
        &mut catalog,
        crate::driver::DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        rows(&catalog, "SELECT * FROM t"),
        vec![vec![
            tidb_datatype::Datum::Int(1),
            tidb_datatype::Datum::Int(2),
        ]],
        "drop column must remove it from the readable schema (column_change_test.go:87)"
    );

    // 3. Add the column back with no default: pre-existing rows read NULL.
    run_alter_table_in(
        "ALTER TABLE t ADD COLUMN c3 INT",
        &mut catalog,
        crate::driver::DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        rows(&catalog, "SELECT * FROM t"),
        vec![vec![
            tidb_datatype::Datum::Int(1),
            tidb_datatype::Datum::Int(2),
            tidb_datatype::Datum::Null,
        ]],
        "add column without DEFAULT must read NULL for pre-existing rows (column_change_test.go:110)"
    );
}

/// GO PORT of `pkg/ddl/column_change_test.go:124
/// TestModifyAutoRandColumnWithMetaKeyChanged`.
///
/// Go pins that `alter table t modify column a bigint AUTO_RANDOM(10)` on an
/// `AUTO_RANDOM(5)` primary key (column_change_test.go:135, MustExec — Go
/// accepts it) survives three injected meta-key changes (auto-random id
/// increments between retries) and ends with
/// `TableInfo.AutoRandomBits == 10` (column_change_test.go:168).
#[test]
#[ignore = "go-parity-gap: this tier's MODIFY COLUMN refuses the AUTO_RANDOM option (only NULL/NOT NULL/DEFAULT/AUTO_INCREMENT are handled), so the AutoRandomBits change has no Rust counterpart, and the injected meta-key retry loop (beforeRunOneJobStep) has no job pipeline to run in"]
fn modify_auto_rand_column_with_meta_key_changed() {}

/// GO PORT of `pkg/ddl/column_change_test.go:418 TestIssue40135`.
///
/// Go pins that during a `MODIFY COLUMN` job on a hash-partitioned table, a
/// concurrent `ALTER TABLE ... ADD COLUMN` waits (SetWaitTimeWhenErrorOccurred)
/// instead of failing with a schema-change conflict, and the modify
/// eventually syncs.
#[test]
#[ignore = "go-parity-gap: needs the DDL job queue's wait-when-error-occurred coordination between concurrent jobs; not transcreated in this tier"]
fn issue_40135() {}
