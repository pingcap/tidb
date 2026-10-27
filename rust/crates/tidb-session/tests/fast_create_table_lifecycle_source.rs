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

use crate::support::testkit::TestKit;

// --- TestDDL (pkg/ddl/tests/fastcreatetable/fastcreatetable_test.go:59) ---
//
// Go, with `tidb_enable_fast_create_table=ON` through a mock server
// connection: `create database db`; `create table db.tb1(id int)` and
// `db.tb2`; re-creating `db.tb1` fails
// `[schema:1050]Table 'db.tb1' already exists`; `truncate table db.tb1`;
// `drop table db.tb1`; `rename table db.tb2 to db.tb3`; `drop database db`;
// then create the database and both tables AGAIN -- the fast path must
// preserve the whole table lifecycle.
//
// The port runs the identical statement sequence; the mock server/conn
// plumbing is not a carrier here (see the ignored TestSwitchFastCreateTable
// port).
#[test]
fn fast_path_table_lifecycle_round_trips() {
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_enable_fast_create_table=ON");

    tk.must_exec("create database db");
    // Create Table
    tk.must_exec("create table db.tb1(id int)");
    tk.must_exec("create table db.tb2(id int)");
    // create table twice
    let error = tk.must_get_err_code("create table db.tb1(id int)", 1050);
    assert_eq!(error.message, "Table 'db.tb1' already exists");

    // Truncate Table
    tk.must_exec("truncate table db.tb1");
    // Drop Table
    tk.must_exec("drop table db.tb1");
    // Rename Table
    tk.must_exec("rename table db.tb2 to db.tb3");
    // Drop Database
    tk.must_exec("drop database db");

    // create again
    tk.must_exec("create database db");
    // Create Table
    tk.must_exec("create table db.tb1(id int)");
    tk.must_exec("create table db.tb2(id int)");
    let schema = tk.info_schema();
    assert!(schema.contains_in("db", "tb1"));
    assert!(schema.contains_in("db", "tb2"));
}
