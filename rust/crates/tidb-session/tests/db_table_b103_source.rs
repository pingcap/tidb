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

//! Runnable final-state table behavior derived from pinned Go
//! `pkg/ddl/db_table_test.go` tests.

use crate::support::testkit::TestKit;

// --- TestDropTables (pkg/ddl/db_table_test.go:817) ---

#[test]
fn drop_tables_if_exists_and_partial_missing_table() {
    let mut tk = TestKit::new();
    tk.must_exec("create table b103_drop_one (a int)");
    tk.must_exec("drop table if exists b103_missing, b103_drop_one");
    // Go observes a dropped table with `show create table` (ErrNoSuchTable).
    tk.must_get_err_code("show create table b103_drop_one", 1146);
    tk.must_get_err_code("show create table b103_missing", 1146);
}
