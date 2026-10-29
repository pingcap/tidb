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

//! Runnable final-state rename behavior derived from pinned Go
//! `pkg/ddl/db_rename_test.go` tests.

use crate::support::testkit::TestKit;
use tidb_infoschema::TableEntry;

fn create(tk: &mut TestKit, database: &str, table: &str) {
    tk.must_exec(&format!("create table {database}.{table} (a int, b int)"));
}

// The common statement-level behavior used by TestRenameTable2 and
// TestAlterTableRenameTable (Go `renameTableTest`, db_rename_test.go:78).
fn rename_round_trip(rename_sql: &str) {
    let mut tk = TestKit::new();
    tk.must_exec("create database test1");
    create(&mut tk, "test", "b103_rename");
    tk.must_exec("insert into b103_rename values (1, 1), (2, 2)");
    let rename_keyword = if rename_sql == "alter table" {
        "rename to"
    } else {
        "to"
    };
    tk.must_exec(&format!(
        "{rename_sql} test.b103_rename {rename_keyword} test1.b103_renamed"
    ));
    // Go `is.TableByName` / `is.TableExists` on `domain.GetDomain(ctx).InfoSchema()`.
    let is = tk.info_schema();
    assert!(!is.contains_in("test", "b103_rename"));
    assert!(is.contains_in("test1", "b103_renamed"));
    tk.must_query("select * from test1.b103_renamed order by a")
        .check(["1 1", "2 2"]);
    tk.must_exec("use test1");
    tk.must_exec(&format!(
        "{rename_sql} test1.b103_renamed {rename_keyword} test1.b103_renamed_again"
    ));
    assert!(tk.info_schema().contains_in("test1", "b103_renamed_again"));
}

// --- TestRenameTable2 (pkg/ddl/db_rename_test.go:68) ---

#[test]
fn rename_table_moves_table_across_and_within_databases() {
    rename_round_trip("rename table");
}

// --- TestAlterTableRenameTable (pkg/ddl/db_rename_test.go:73) ---

#[test]
fn alter_table_rename_to_moves_table_across_and_within_databases() {
    rename_round_trip("alter table");
}

// --- TestRenameMultiTables (pkg/ddl/db_rename_test.go:175) ---

#[test]
fn rename_multiple_tables_preserves_rows_and_names() {
    let mut tk = TestKit::new();
    tk.must_exec("create database test1");
    for table in ["b103_multi_a", "b103_multi_b"] {
        create(&mut tk, "test", table);
        tk.must_exec(&format!("insert into {table} values (1, 1), (2, 2)"));
    }
    tk.must_exec("use test1");
    tk.must_exec(
        "rename table test.b103_multi_a to test1.b103_multi_a, test.b103_multi_b to test1.b103_multi_b",
    );
    for table in ["b103_multi_a", "b103_multi_b"] {
        let is = tk.info_schema();
        assert!(is.contains_in("test1", table));
        assert!(!is.contains_in("test", table));
        tk.must_query(&format!("select count(*) from test1.{table}"))
            .check(["2"]);
    }
}

// --- TestRenameMultiTablesIssue47064 (pkg/ddl/db_rename_test.go:286) ---

#[test]
fn rename_multiple_tables_keeps_their_columns() {
    let mut tk = TestKit::new();
    tk.must_exec("create database test1");
    create(&mut tk, "test", "b103_issue47064_a");
    create(&mut tk, "test", "b103_issue47064_b");
    tk.must_exec("use test1");
    tk.must_exec(
        "rename table test.b103_issue47064_a to test1.b103_issue47064_a, test.b103_issue47064_b to test1.b103_issue47064_b",
    );
    for table in ["b103_issue47064_a", "b103_issue47064_b"] {
        let TableEntry::Kv(table) = tk.get_table_by_name("test1", table) else {
            panic!("renamed table is not a KV table");
        };
        assert_eq!(table.visible_columns().len(), 2);
    }
}
