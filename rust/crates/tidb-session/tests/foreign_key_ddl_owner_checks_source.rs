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


//! Each test opens its Go owner in `pkg/ddl/foreign_key_test.go`; Go's
//! settings are its `set` statements, its errors are the statements' wire
//! errors, and its metadata reads are `external.GetTableByName`.

use crate::support::testkit::TestKit;
use tidb_infoschema::TableEntry;
use tidb_table::kv_table::FkAction;
use tidb_table::kv_table::KvTable;

/// Go `external.GetTableByName(t, tk, db, name).Meta()`: the storage-backed
/// table as the domain's current schema holds it.
fn kv_table(tk: &TestKit, database: &str, name: &str) -> KvTable {
    match tk.get_table_by_name(database, name) {
        TableEntry::Kv(table) => (*table).clone(),
        _ => panic!("expected a storage-backed table {database}.{name}"),
    }
}

// --- TestForeignKey (pkg/ddl/foreign_key_test.go:110) ---
//
// Go submits `ActionAddForeignKey` for `c1_fk` over (c1) referencing
// t2(c1) with ON DELETE CASCADE / ON UPDATE SET NULL, requires the job to
// finish and the constraint to read back public from the table meta, then
// drops it again and requires the constraint gone. The serialized form runs
// the same two statements through SQL and reads the constraint back from the
// table meta.
#[test]
fn foreign_key_add_then_drop_constraint_round_trip() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (c1 int, c2 int)");
    tk.must_exec("create table t2 (c1 int, c2 int, key i1(c1))");

    tk.must_exec(
        "alter table t add constraint c1_fk foreign key (c1) references t2 (c1) \
         on delete cascade on update set null",
    );

    let table = kv_table(&tk, "test", "t");
    let keys = table.foreign_keys();
    assert_eq!(keys.len(), 1, "one public constraint after add");
    let foreign_key = &keys[0];
    assert_eq!(foreign_key.name, "c1_fk");
    assert_eq!(foreign_key.cols, vec!["c1".to_owned()]);
    assert_eq!(foreign_key.ref_table, "t2");
    assert_eq!(foreign_key.ref_cols, vec!["c1".to_owned()]);
    // Go passes ast.ReferOptionCascade (delete) and ast.ReferOptionSetNull
    // (update); the ported meta carries the same two actions.
    assert!(matches!(foreign_key.on_delete, FkAction::Cascade));
    assert!(matches!(foreign_key.on_update, FkAction::SetNull));
    // Go CreateForeignKey auto-creates a supporting index when none covers
    // the referencing columns.
    assert!(
        table
            .indexes()
            .iter()
            .any(|index| index.column_offsets.contains(&0)),
        "c1 must be covered by some index for the constraint to hold"
    );

    tk.must_exec("alter table t drop foreign key c1_fk");
    let table = kv_table(&tk, "test", "t");
    assert!(
        table.foreign_keys().is_empty(),
        "constraint gone after drop"
    );
}

// --- TestTruncateOrDropTableWithForeignKeyReferred2
//     (pkg/ddl/foreign_key_test.go:209) ---
//
// Go races `truncate table t1` (and then `drop table t1`) against the commit
// of `create table t2 (... foreign key fk(b) references t1(id))` and requires
// the loser to see
// `[ddl:1701]Cannot truncate a table referenced in a foreign key constraint
// (`test`.`t2` CONSTRAINT `fk`)` — Go reuses ErrTruncateIllegalForeignKey for
// both the truncate and the drop of a referenced table
// (pkg/ddl/foreign_key.go:404-427).
//
// The synchronous owner now uses Go's 1701 TRUNCATE and 3730 DROP diagnostics;
// the schema-state race itself remains outside this tier.
#[test]
fn truncate_or_drop_referenced_table_reports_go_errnos() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, a int)");
    tk.must_exec("create table t2 (a int, b int, foreign key fk(b) references t1(id))");

    let error = tk.must_get_err_code("truncate table t1", 1701);
    assert_eq!(
        error.message,
        "Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk`)"
    );

    let error = tk.must_get_err_code("drop table t1", 3730);
    assert_eq!(
        error.message,
        "Cannot drop table 't1' referenced by a foreign key constraint 'fk' on table 't2'."
    );
}

// --- TestDropIndexNeededInForeignKey2 (pkg/ddl/foreign_key_test.go:261) ---
//
// Go creates t2 with `index idx1(b)`, `index idx2(b)` and a foreign key over
// (b), drops idx1, and requires the racing `drop index idx2` to fail with
// `[ddl:1553]Cannot drop index 'idx2': needed in a foreign key constraint`
// (the constraint survives on idx2 alone). The serialized form drops idx1
// first (which must succeed — idx2 still covers b), then requires the idx2
// drop to be refused with Go's errno.
#[test]
fn drop_index_needed_in_foreign_key_reports_1553_for_the_last_cover() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, b int)");
    tk.must_exec(
        "create table t2 (a int, b int, index idx1 (b), index idx2 (b), \
         foreign key (b) references t1(id))",
    );

    // With idx1 gone the constraint still has idx2, so this drop is legal.
    tk.must_exec("alter table t2 drop index idx1");

    let error = tk.must_get_err_code("alter table t2 drop index idx2", 1553);
    assert_eq!(
        error.message,
        "Cannot drop index 'idx2': needed in a foreign key constraint"
    );
}

// --- TestDropDatabaseWithForeignKeyReferred2
//     (pkg/ddl/foreign_key_test.go:295) ---
//
// Go drops `database test` while `test2.t3` still declares a constraint
// against `test.t2`, and requires
// `[ddl:3730]Cannot drop table 't2' referenced by a foreign key constraint
// 'fk_b' on table 't3'.` (the owner-side referral check walking every table
// the database would remove); once `test2.t3` is gone the drop succeeds.
// The schema-state race itself remains outside this tier.
#[test]
fn drop_database_with_foreign_key_referred_reports_3730() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, b int, index(b))");
    tk.must_exec("create table t2 (id int key, b int, foreign key fk_b(b) references t1(id))");
    tk.must_exec("create database test2");
    tk.must_exec(
        "create table test2.t3 (id int key, b int, foreign key fk_b(b) references test.t2(id))",
    );

    let error = tk.must_get_err_code("drop database test", 3730);
    assert_eq!(
        error.message,
        "Cannot drop table 't2' referenced by a foreign key constraint 'fk_b' on table 't3'."
    );
    tk.must_exec("drop table test2.t3");
    tk.must_exec("drop database test");
}

// Go TestAddForeignKey2's concurrent index-removal/owner-recheck remains an
// online-DDL obligation. Its old serial surrogate incorrectly expected ADD
// to reject a missing index; Go CreateForeignKey auto-creates that index.
// Public rejection/rollback/cascade coverage now runs through the session
// owner in tidb-session/tests/fk_global_policy.rs, not a direct DML runner.

// --- TestFix59705 (pkg/ddl/foreign_key_test.go:445) ---
//
// With `foreign_key_checks=off` (for the whole test), Go lets
// `child(pid_test)` declare a constraint against the NOT-YET-EXISTING
// `parent(pid)` (that is the bug's setup). Then:
//   * changing the column toward the referenced name while the parent is
//     missing fails `[schema:1146]Table 'test.parent' doesn't exist`;
//   * after `parent` exists, `change column pid_test pid varchar(10)` fails
//     `[ddl:3780]... 'pid' and referenced column 'pid' ... are
//     incompatible` (int -> varchar across a constraint);
//   * `change column pid_test pid int` succeeds and `show create table`
//     prints the constraint with its `KEY fk_1 (pid)` support index.
//
#[test]
fn fix_59705_change_column_toward_a_missing_parent_reports_1146() {
    let mut tk = TestKit::new();
    tk.must_exec("set foreign_key_checks=off");
    tk.must_exec(
        "create table child (id int, pid_test int, foreign key (pid_test) references parent(pid))",
    );

    let error = tk.must_get_err_code("alter table child change column pid_test pid varchar(10);", 1146);
    assert_eq!(error.message, "Table 'test.parent' doesn't exist");
}

// The remaining legs of Go's TestFix59705, against a parent that exists: the
// int -> varchar move across the constraint is 3780, the same-typed rename
// succeeds, and the constraint plus its `fk_1` support index survive on the
// renamed column.
#[test]
fn fix_59705_change_column_across_a_constraint() {
    let mut tk = TestKit::new();
    tk.must_exec("set foreign_key_checks=off");
    tk.must_exec(
        "create table child (id int, pid_test int, foreign key (pid_test) references parent(pid))",
    );

    // The parent is now created (the state Go's failure message describes);
    // the child's constraint was stored unchecked above.
    tk.must_exec("create table parent(pid int primary key)");

    // int -> varchar across the constraint: 3780.
    tk.must_get_err_code("alter table child change column pid_test pid varchar(10);", 3780);

    // Same-typed rename: accepted.
    tk.must_exec("alter table child change column pid_test pid int");

    let table = kv_table(&tk, "test", "child");
    let keys = table.foreign_keys();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].name, "fk_1");
    assert_eq!(keys[0].cols, vec!["pid".to_owned()]);
    // Go: `show create table child` prints the support index and the
    // constraint over the renamed column.
    let create = tk.must_query("show create table child");
    create.check_contain("KEY `fk_1` (`pid`)");
    create.check_contain("CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `parent` (`pid`)");
    tk.must_exec("admin check table child");
}
