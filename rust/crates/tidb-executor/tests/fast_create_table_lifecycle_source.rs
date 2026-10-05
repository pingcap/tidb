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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

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
