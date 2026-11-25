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

/// `(table id, revision, foreign-key referred tables)` of `db.name`.
fn rename_revision_info(tk: &crate::support::testkit::TestKit, db: &str, name: &str) -> (i64, u64, Vec<String>) {
    match tk.get_table_by_name(db, name) {
        tidb_infoschema::TableEntry::Kv(table) => {
            let meta = table.meta();
            let meta = meta.read();
            let refs = meta.foreign_keys.iter_deref().map(|fk| fk.read().ref_table.lowercase().to_owned()).collect();
            (meta.id, meta.revision, refs)
        }
        _ => panic!("{db}.{name} is a stored table"),
    }
}

/// Go master `TestRenameTableRevision` (#73233): a rename keeps the table ID
/// and advances its revision, so cached plans bound to the old name-to-ID
/// mapping are invalidated. The failed-rename half drives Go's
/// `renameTableErr` failpoint, which Rust's rename has no site for.
#[test]
fn rename_table_revision() {
    for enable_fk in [true, false] {
        let mut tk = crate::support::testkit::TestKit::new();
        tk.must_exec(&format!("set global tidb_enable_foreign_key = {enable_fk}"));
        tk.must_exec("create database rename_revision");
        tk.must_exec("use test");
        let check_rename = |tk: &mut crate::support::testkit::TestKit, sql: &str, old: (&str, &str), new: (&str, &str)| {
            let before = rename_revision_info(tk, old.0, old.1);
            tk.must_exec(sql);
            let after = rename_revision_info(tk, new.0, new.1);
            assert_eq!(before.0, after.0, "{sql}");
            assert!(after.1 > before.1, "{sql}: {} -> {}", before.1, after.1);
        };
        tk.must_exec("create table t1(a int primary key)");
        tk.must_exec("create table t2(a int primary key)");
        tk.must_exec("insert into t1 values(1)");
        tk.must_exec("insert into t2 values(2)");
        check_rename(&mut tk, "rename table t1 to tmp", ("test", "t1"), ("test", "tmp"));
        check_rename(&mut tk, "rename table t2 to t1", ("test", "t2"), ("test", "t1"));
        check_rename(&mut tk, "rename table tmp to t2", ("test", "tmp"), ("test", "t2"));
        tk.must_query("select t1.a,t2.a from t1 join t2").check(["2 1"]);
        check_rename(&mut tk, "alter table t1 rename to tmp", ("test", "t1"), ("test", "tmp"));
        check_rename(&mut tk, "rename table tmp to rename_revision.tmp", ("test", "tmp"), ("rename_revision", "tmp"));
        check_rename(&mut tk, "rename table rename_revision.tmp to test.t1", ("rename_revision", "tmp"), ("test", "t1"));
        let (before1, before2) = (rename_revision_info(&tk, "test", "t1"), rename_revision_info(&tk, "test", "t2"));
        tk.must_exec("rename table t1 to tmp, t2 to t1, tmp to t2");
        let (after1, after2) = (rename_revision_info(&tk, "test", "t2"), rename_revision_info(&tk, "test", "t1"));
        assert_eq!((before1.0, before2.0), (after1.0, after2.0));
        assert!(after1.1 > before1.1 && after2.1 > before2.1);
        tk.must_query("select t1.a,t2.a from t1 join t2").check(["1 2"]);
        tk.must_exec("set global tidb_enable_foreign_key = on");
    }
}

/// Go master `TestRenameTableRevisionWithForeignKeys` (#73233): the renamed
/// parent and its referring child both advance their revisions.
#[test]
fn rename_table_revision_with_foreign_keys() {
    let mut tk = crate::support::testkit::TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table parent(a int primary key, b int, foreign key(b) references parent(a))");
    tk.must_exec("create table child(a int primary key, b int, foreign key(b) references parent(a))");
    let (parent, child) = (rename_revision_info(&tk, "test", "parent"), rename_revision_info(&tk, "test", "child"));
    tk.must_exec("rename table parent to renamed_parent");
    let (new_parent, new_child) = (rename_revision_info(&tk, "test", "renamed_parent"), rename_revision_info(&tk, "test", "child"));
    assert_eq!(parent.0, new_parent.0);
    assert!(new_parent.1 > parent.1 && new_child.1 > child.1);
    assert_eq!(new_parent.2, ["renamed_parent"]);
    assert_eq!(new_child.2, ["renamed_parent"]);
    let (parent, child) = (new_parent, new_child);
    tk.must_exec("rename table child to renamed_child, renamed_parent to parent");
    let (new_parent, new_child) = (rename_revision_info(&tk, "test", "parent"), rename_revision_info(&tk, "test", "renamed_child"));
    assert_eq!((parent.0, child.0), (new_parent.0, new_child.0));
    assert!(new_parent.1 > parent.1 && new_child.1 > child.1);
    assert_eq!(new_parent.2, ["parent"]);
    assert_eq!(new_child.2, ["parent"]);
}
