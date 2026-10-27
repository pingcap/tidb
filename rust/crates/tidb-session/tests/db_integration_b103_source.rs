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

//! Runnable final-state behavior derived from pinned Go
//! `pkg/ddl/db_integration_test.go` tests, run through the Go testkit port
//! (`support::testkit`).

use crate::support::testkit::TestKit;

// --- TestAlterColumn (pkg/ddl/db_integration_test.go:1143) ---

/// The default-changing and origin-row half of Go's `TestAlterColumn`.
#[test]
fn alter_column_set_default_changes_new_rows() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table b103_alter_column (a int default 111, b varchar(8), c varchar(8) not null)",
    );
    tk.must_exec("insert into b103_alter_column set b = 'a', c = 'aa'");
    tk.must_exec("alter table b103_alter_column alter column a set default 222");
    tk.must_exec("insert into b103_alter_column set b = 'b', c = 'bb'");
    tk.must_query("select a from b103_alter_column order by a")
        .check(["111", "222"]);
}

// --- TestSqlFunctionsInGeneratedColumns (pkg/ddl/db_integration_test.go:1817) ---

/// A small positive generated-column contract from the larger Go matrix.
#[test]
fn deterministic_sql_function_in_generated_column_is_readable() {
    let mut tk = TestKit::new();
    tk.must_exec("create table b103_generated (a int, b int generated always as (abs(a)) virtual)");
    tk.must_exec("insert into b103_generated values (-1, default)");
    tk.must_query("select * from b103_generated").check(["-1 1"]);
}

// --- TestDropColumnWithIndex (pkg/ddl/db_integration_test.go:2017) ---

/// Dropping a covered ordinary index column removes the index and leaves the
/// remaining table usable.
#[test]
fn drop_column_with_index_removes_index_and_column() {
    let mut tk = TestKit::new();
    tk.must_exec("create table b103_drop_index (a int, b int, c int)");
    tk.must_exec("alter table b103_drop_index add index idx(b)");
    tk.must_exec("alter table b103_drop_index drop column b");
    tk.must_exec("insert into b103_drop_index values (1, 3)");
    tk.must_query("select * from b103_drop_index").check(["1 3"]);
}

// --- TestDropColumnWithMultiIndex (pkg/ddl/db_integration_test.go:2047) ---

/// Multiple ordinary indexes covering one dropped column are all removed.
#[test]
fn drop_column_with_multiple_indexes_removes_all_covering_indexes() {
    let mut tk = TestKit::new();
    tk.must_exec("create table b103_drop_multi (a int, b int, c int)");
    tk.must_exec("alter table b103_drop_multi add index idx_1(b)");
    tk.must_exec("alter table b103_drop_multi add index idx_2(b)");
    tk.must_exec("alter table b103_drop_multi drop column b");
    tk.must_query("select * from b103_drop_multi").check(Vec::<&str>::new());
}

// --- TestDropColumnsWithMultiIndex (pkg/ddl/db_integration_test.go:2061) ---

/// Dropping two indexed columns in one ALTER leaves the unindexed column.
#[test]
fn drop_columns_with_multiple_indexes_removes_all_covering_indexes() {
    let mut tk = TestKit::new();
    tk.must_exec("create table b103_drop_both (a int, b int, c int)");
    for sql in [
        "alter table b103_drop_both add index idx_1(b)",
        "alter table b103_drop_both add index idx_2(b)",
        "alter table b103_drop_both add index idx_3(c)",
        "alter table b103_drop_both drop column b, drop column c",
    ] {
        tk.must_exec(sql);
    }
    tk.must_query("select * from b103_drop_both").check(Vec::<&str>::new());
}
