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

//! Ports of `pkg/ddl/tests/fastcreatetable/fastcreatetable_test.go` (part12
//! items 704-706 of `pkg/ddl`'s `func Test*`/`func Benchmark*` declarations
//! sorted by file and line; item 707, `tests/fastcreatetable/
//! main_test.go:27::TestMain`, is the package harness with no assertions
//! and is recorded as skipped in the batch receipt), read from
//! `origin/master`.
//!
//! `tidb_enable_fast_create_table` merges consecutive CREATE TABLE
//! submissions into one `ActionCreateTables` batch job (Go
//! `pkg/ddl/tests/fastcreatetable/fastcreatetable_test.go:98`
//! `TestMergedJob`). The flag itself, the mock server that sets it, and the
//! job-merging machinery are outside this tier; the LIFECYCLE contract the
//! fast path must preserve — create/duplicate-create/truncate/drop/rename/
//! drop-database/recreate — is carried by the tier's serialized runners and
//! is what the running test below pins. Nothing is approximated.

use tidb_executor::ddl::{self, CreateTableSettings};
use tidb_executor::Catalog;
use tidb_executor::StmtContext;

// --- TestDDL (pkg/ddl/tests/fastcreatetable/fastcreatetable_test.go:59) ---
//
// Go, with `tidb_enable_fast_create_table=ON` through a mock server
// connection: `create database db`; `create table db.tb1(id int)` and
// `db.tb2`; re-creating `db.tb1` fails
// `[schema:1050]Table 'db.tb1' already exists`; `truncate table db.tb1`;
// `drop table db.tb1`; `rename table db.tb2 to db.tb3`; `drop database db`;
// then create the database and both tables AGAIN — the fast path must
// preserve the whole table lifecycle.
//
// The serialized port runs the identical statement sequence through the
// tier's runners and asserts the identical outcomes; the "fast" flag and
// the mock server/conn plumbing are not carriers here (see the ignored
// TestSwitchFastCreateTable port).
#[test]
fn fast_path_table_lifecycle_round_trips() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();

    catalog.create_database("db");
    ddl::run_create_table_in(
        "create table db.tb1(id int)",
        &mut catalog,
        "db",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table db.tb2(id int)",
        &mut catalog,
        "db",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // Go: create table twice → [schema:1050]Table 'db.tb1' already exists.
    let error = ddl::run_create_table_in(
        "create table db.tb1(id int)",
        &mut catalog,
        "db",
        CreateTableSettings::default(),
        &ctx,
    )
    .expect_err("Go: [schema:1050]Table 'db.tb1' already exists");
    let mysql = error.clone().to_mysql_error();
    assert_eq!(mysql.code, 1050);
    assert_eq!(mysql.message, "Table 'db.tb1' already exists");

    // Truncate, drop, rename — all must succeed under the fast path.
    ddl::run_truncate_table_in("truncate table db.tb1", &mut catalog, "db", ctx.sql_mode()).unwrap();
    ddl::run_drop_table_in(
        "drop table db.tb1",
        &mut catalog,
        "db",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    ddl::run_rename_table_in(
        "rename table db.tb2 to db.tb3",
        &mut catalog,
        "db",
        ctx.sql_mode(),
    )
    .unwrap();
    assert!(catalog.drop_database("db"));

    // Create again: a fresh database of the same name takes both tables.
    catalog.create_database("db");
    ddl::run_create_table_in(
        "create table db.tb1(id int)",
        &mut catalog,
        "db",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table db.tb2(id int)",
        &mut catalog,
        "db",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    assert!(catalog.contains_in("db", "tb1"));
    assert!(catalog.contains_in("db", "tb2"));
}

// --- TestSwitchFastCreateTable
//     (pkg/ddl/tests/fastcreatetable/fastcreatetable_test.go:31) ---
//
// Go, through a mock SERVER connection: `show global variables like
// 'tidb_enable_fast_create_table'` reads ON, `set global ... =0` flips it
// to OFF, and `set global ...='wrong'` fails
// `[variable:1231]Variable 'tidb_enable_fast_create_table' can't be set to
// the value of 'wrong'`.
//
// go-parity-gap: the sysvar DEFINITION lives in tidb-session's catalog
// (`rust/crates/tidb-session/src/sysvar/catalog/sql_behavior.rs:244`), but
// this tier has no variable surface (no SHOW VARIABLES, no SET GLOBAL) and
// no mock-server connection to drive one with.
#[test]
#[ignore = "go-parity-gap: no SHOW/SET GLOBAL variable surface in this tier"]
fn the_fast_create_table_switch_validates_and_persists() {
    // Contract (fastcreatetable_test.go:31-56): the default reads ON; 0
    // sets OFF; 'wrong' fails [variable:1231] with Go's exact message.
}

// --- TestMergedJob (pkg/ddl/tests/fastcreatetable/fastcreatetable_test.go:98)
//
// Go gates the scheduler behind `beforeLoadAndDeliverJobs`, submits three
// rounds of concurrent CREATE TABLEs and requires the queue to MERGE the
// same-shape ones: round one leaves exactly 1 job; rounds two/three merge
// into `ActionCreateTables` batch entries (`gotJobs[1].Type` /
// `gotJobs[2].Type`), the merged failures fail TOGETHER (AUTO_INCREMENT 1000
// duplicate against the first t1) and the merged successes land together;
// afterwards `insert into test.t1(c) values(1)` reads back `100 1` — the
// auto-id base survived the failed merge.
//
// go-parity-gap: no job queue, no GetAllDDLJobs, no merging, and no
// failpoint gates in this tier.
#[test]
#[ignore = "go-parity-gap: fast-create-table job merging and the job queue are not transcreated"]
fn merged_jobs_fail_and_succeed_together() {
    // Contract (fastcreatetable_test.go:98-155): queue lengths 1/2/3 with
    // CreateTables batches at [1] and [2]; the failed merge leaves t1
    // creatable with its AUTO_INCREMENT 100 base intact (insert reads
    // "100 1").
}
