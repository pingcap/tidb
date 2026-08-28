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

//! Ports of the `pkg/ddl/foreign_key_test.go` family (part6 items 323–330 of
//! the package's `func Test*`/`func Benchmark*` declarations, sorted by file
//! and line), read from `origin/master`.
//!
//! The Go tests drive two racing sessions against the online-DDL job queue
//! (`beforeRunOneJobStep` parks one job while the other session's statement
//! lands mid-schema-state), then assert the error the loser must see. This
//! tier has no job queue and no schema states, so each port pins the
//! serialized contract the race ultimately depends on — the errno the
//! constraint machinery must produce once the competing DDL has landed — and
//! every divergence found while porting is written in the test's comment
//! rather than papered over.

use tidb_datatype::Datum;
use tidb_executor::driver::Catalog;
use tidb_executor::{
    admin_check, ddl, run_delete_on, run_insert_on, run_select_on, FkAction, KvTable, RowDecodeContext, StmtContext,
    TableEntry,
};

/// The text of a datum, however the codec chose to represent it.
fn datum_text(value: &Datum) -> String {
    match value {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Int(i) => i.to_string(),
        Datum::UInt(u) => u.to_string(),
        other => panic!("unexpected datum {other:?}"),
    }
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(datum_text).collect())
        .collect()
}

/// The storage-backed table a test just built.
fn kv_table(catalog: &Catalog, database: &str, name: &str) -> KvTable {
    match catalog.table_in(database, name) {
        Some(TableEntry::Kv(table)) => table.clone(),
        _ => panic!("expected a storage-backed table {database}.{name}"),
    }
}

/// The errno a failed statement reports.
fn err_code(error: &tidb_executor::DriverError) -> u16 {
    error.clone().to_mysql_error().code
}

/// The message a failed statement reports.
fn err_message(error: &tidb_executor::DriverError) -> String {
    error.clone().to_mysql_error().message
}

// --- TestForeignKey (pkg/ddl/foreign_key_test.go:110) ---
//
// Go submits `ActionAddForeignKey` for `c1_fk` over (c1) referencing
// t2(c1) with ON DELETE CASCADE / ON UPDATE SET NULL, requires the job to
// finish and the constraint to read back public from the table meta, then
// drops it again and requires the constraint gone. The serialized form runs
// the same two statements through the ALTER runner and reads the constraint
// back from the storage-backed meta.
#[test]
fn foreign_key_add_then_drop_constraint_round_trip() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t (c1 int, c2 int)",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (c1 int, c2 int, key i1(c1))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    ddl::run_alter_table_in(
        "alter table t add constraint c1_fk foreign key (c1) references t2 (c1) \
         on delete cascade on update set null",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();

    let table = kv_table(&catalog, "test", "t");
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
    // The constraint's index support: Go's ALTER path refuses a constraint
    // whose columns no index covers; the port carries an auto-created key
    // named after the constraint instead (divergence, see the
    // add_foreign_key_missing_index documentary below).
    assert!(
        table
            .indexes()
            .iter()
            .any(|index| index.column_offsets.contains(&0)),
        "c1 must be covered by some index for the constraint to hold"
    );

    ddl::run_alter_table_in(
        "alter table t drop foreign key c1_fk",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let table = kv_table(&catalog, "test", "t");
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
// go-parity-gap: this tier's truncate (`ddl/table_lifecycle.rs:192`
// run_truncate_table_in) performs no foreign-key referral check at all, and
// its drop check (`foreign_key.rs:926` check_drop_tables) renders Go's
// 1451-shaped parent-row text instead of 1701, so the Go errno is not
// reproducible here.
#[test]
#[ignore = "go-parity-gap: no 1701 truncate/drop referral check (run_truncate_table_in has none; check_drop_tables renders 1451 text)"]
fn truncate_or_drop_referenced_table_reports_1701() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int key, a int)",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int, foreign key fk(b) references t1(id))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    let error =
        ddl::run_truncate_table_in("truncate table t1", &mut catalog, "test", ctx.sql_mode())
            .expect_err("Go: [ddl:1701]Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk`)");
    assert_eq!(err_code(&error), 1701);
    assert_eq!(
        err_message(&error),
        "Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk`)"
    );

    let error = ddl::run_drop_table_in(
        "drop table t1",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .expect_err("Go: the same 1701 for the DROP of a referenced table");
    assert_eq!(err_code(&error), 1701);
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
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int)",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int, index idx1 (b), index idx2 (b), \
         foreign key (b) references t1(id))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // With idx1 gone the constraint still has idx2, so this drop is legal.
    ddl::run_alter_table_in(
        "alter table t2 drop index idx1",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();

    let error = ddl::run_alter_table_in(
        "alter table t2 drop index idx2",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("the constraint's last covering index may not be dropped");
    assert_eq!(err_code(&error), 1553);
    assert_eq!(
        err_message(&error),
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
// the database would remove).
//
// go-parity-gap: this tier's drop-database runner has no owner-side referral
// check, so the 3730 the Go owner produces is not reproducible here.
#[test]
#[ignore = "go-parity-gap: drop-database performs no 3730 referral check across the dropped schema"]
fn drop_database_with_foreign_key_referred_reports_3730() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index(b))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (id int key, b int, foreign key fk_b(b) references t1(id))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    catalog.create_database("test2");
    ddl::run_create_table_in(
        "create table t3 (id int key, b int, foreign key fk_b(b) references test.t2(id))",
        &mut catalog,
        "test2",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // `Catalog::drop_database` reports success without a referral check, so
    // the owner-side 3730 cannot fire here.
    assert!(catalog.drop_database("test"));
}

// --- TestAddForeignKey2 (pkg/ddl/foreign_key_test.go:334) ---
//
// Go races `alter table t2 add foreign key (b) references t1(id)` against a
// concurrent `alter table t2 drop index b`, and requires the add to fail with
// `Failed to add the foreign key constraint. Missing index for 'fk_1' foreign
// key columns in the table 't2'` — the owner-side validation
// (pkg/ddl/foreign_key.go:664-666 checkAddForeignKeyValidInOwner) refuses an
// ADD whose referencing columns no index covers.
//
// go-parity-gap: this tier's ADD FOREIGN KEY auto-creates the covering index
// (`ddl/alter_table.rs:914` add_foreign_key_action mirrors the CREATE TABLE
// arm instead), so the Go refusal is not reproducible here.
#[test]
#[ignore = "go-parity-gap: ALTER ADD FOREIGN KEY auto-creates the covering index where Go refuses with the missing-index error"]
fn add_foreign_key_missing_index_is_refused() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index(b))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (id int key, b int, index(b))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in("alter table t2 drop index b", &mut catalog, "test", &ctx).unwrap();

    let error = ddl::run_alter_table_in(
        "alter table t2 add foreign key (b) references t1(id)",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err(
        "Go: Failed to add the foreign key constraint. Missing index for 'fk_1' \
         foreign key columns in the table 't2'",
    );
    assert!(
        err_message(&error).contains("Failed to add the foreign key constraint"),
        "{:?}",
        err_message(&error)
    );
}

// --- TestAddForeignKey3 (pkg/ddl/foreign_key_test.go:365) ---
//
// Go adds `foreign key (id) references t1(id) on delete cascade` over
// populated tables and, while the job sits in StateWriteOnly and StateWrite-
// Reorganization, probes `insert into t2 values (10, 10)` (orphan child) and
// `delete from t1 where id = 1`: the insert must fail planner:1452 naming
// `fk_1` at BOTH states, and the delete must fail planner:1451 — the
// write-only constraint RESTRICTS the parent mutation without yet being
// allowed to cascade. The serialized port below pins the public-constraint
// behavior instead (no schema states exist here): the orphan insert is
// refused exactly as Go refuses it, and the parent delete now CASCADES,
// removing the child row with it — Go's public-state contract for the same
// constraint.
#[test]
fn foreign_key_enforces_child_side_and_cascades_once_public() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index(b))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (id int, b int, index(id), index(b))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    run_insert_on("insert into t1 values (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx).unwrap();
    run_insert_on("insert into t2 values (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx).unwrap();

    ddl::run_alter_table_in(
        "alter table t2 add foreign key (id) references t1(id) on delete cascade",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();

    let error = run_insert_on("insert into t2 values (10, 10)", &mut catalog, &ctx)
        .expect_err("orphan child row must be refused");
    assert_eq!(err_code(&error), 1452);
    // Go appends the rendered actions, `... REFERENCES `t1` (`id`) ON DELETE
    // CASCADE` (planner:1452); this tier renders the constraint WITHOUT the
    // action suffix, so the assertion pins the shared prefix and the missing
    // suffix is a captured rendering divergence.
    assert!(err_message(&error).starts_with(
        "Cannot add or update a child row: a foreign key constraint fails \
         (`test`.`t2`, CONSTRAINT `fk_1` FOREIGN KEY (`id`) REFERENCES `t1` (`id`)"
    ));

    // The write-only-state 1451 halves of Go's probes need the schema-state
    // machinery; the public constraint cascades the same delete.
    run_delete_on("delete from t1 where id = 1", &mut catalog, &ctx)
        .expect("a PUBLIC on-delete-cascade constraint removes the child rows with the parent");
    let rows = run_select_on("select * from t1 order by id", &mut catalog, &ctx).unwrap();
    assert_eq!(rows_text(&rows), vec![vec!["2", "2"], vec!["3", "3"]]);
    let rows = run_select_on("select * from t2 order by id", &mut catalog, &ctx).unwrap();
    assert_eq!(rows_text(&rows), vec![vec!["2", "2"], vec!["3", "3"]]);
}

// The write-only halves of Go's TestAddForeignKey3 (pkg/ddl/foreign_key_test.go:401-410):
// at StateWriteOnly and StateWriteReorganization the half-born constraint
// RESTRICTS the parent-side delete with planner:1451 instead of cascading,
// and both tables keep every row.
//
// go-parity-gap: schema states do not exist in this tier, so a constraint is
// either absent or fully public; the restricting half-state is not
// reproducible.
#[test]
#[ignore = "go-parity-gap: the write-only state restricts a parent delete with 1451 instead of cascading; no schema states here"]
fn add_foreign_key_write_only_state_restricts_parent_delete() {
    // Contract (pkg/ddl/foreign_key_test.go:403-410): `delete from t1 where
    // id = 1` fails [planner:1451]Cannot delete or update a parent row: a
    // foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk_1` FOREIGN
    // KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE) at both write
    // states, and `select * from t1/t2 order by id` keep 1/2/3.
}

// --- TestForeignKeyInWriteOnlyMode (pkg/ddl/foreign_key_test.go:407) ---
//
// Go creates `child (... foreign key (pid) references parent(id) on delete
// cascade)` and, from a session holding the OLD schema while the job sits in
// StateDeleteOnly, requires every DML against `child` to fail with
// `Table 'test.child' doesn't exist` — the not-yet-public table is invisible
// to the other session, which is precisely what the state machine buys.
//
// go-parity-gap: schema states and the job queue that drives them do not
// exist in this tier, so the invisible-table window cannot be reproduced.
#[test]
#[ignore = "go-parity-gap: DeleteOnly schema-state visibility needs the DDL job queue"]
fn foreign_key_in_write_only_mode_hides_the_table() {
    // Contract (pkg/ddl/foreign_key_test.go:422-436): insert / update /
    // delete / joined delete against a DeleteOnly child all report
    // "Table 'test.child' doesn't exist".
}

// --- TestFix59705 (pkg/ddl/foreign_key_test.go:445) ---
//
// With `foreign_key_checks=off`, Go lets `child(pid_test)` declare a
// constraint against the NOT-YET-EXISTING `parent(pid)` (that is the bug's
// setup). Then:
//   * renaming the column toward the referenced name while the parent is
//     missing fails `[schema:1146]Table 'test.parent' doesn't exist`;
//   * after `parent` exists, `change column pid_test pid varchar(10)` fails
//     `[ddl:3780]... 'pid' and referenced column 'pid' ... are
//     incompatible` (int -> varchar across a constraint);
//   * `change column pid_test pid int` succeeds and `show create table`
//     prints the constraint with its `KEY fk_1 (pid)` support index.
//
// go-parity-gap: the missing-parent leg of the Go test cannot be pinned —
// Go's job resolves the constraint's referenced table at CHANGE COLUMN time
// and reports 1146 when it is gone, while this tier skips the FK type check
// for a parent it cannot find (`foreign_key.rs:651` acceptable_column_change
// never sees a related type), so the rename below succeeds here.
#[test]
#[ignore = "go-parity-gap: CHANGE COLUMN does not resolve the constraint's referenced table, so Go's 1146 for a missing parent is not reproducible"]
fn fix_59705_change_column_toward_a_missing_parent_reports_1146() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    let off = StmtContext::for_query().with_foreign_key_checks(false);
    ddl::run_create_table_in(
        "create table child (id int, pid_test int, foreign key (pid_test) references parent(pid))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings {
            foreign_key_checks: false,
            ..ddl::CreateTableSettings::default()
        },
        &off,
    )
    .unwrap();

    let error = ddl::run_alter_table_in(
        "alter table child change column pid_test pid varchar(10);",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go: [schema:1146]Table 'test.parent' doesn't exist");
    assert_eq!(err_code(&error), 1146);
}

// The remaining legs of Go's TestFix59705, against a parent that exists: the
// int -> varchar move across the constraint is 3780, the same-typed rename
// succeeds, and the constraint plus its `fk_1` support index survive on the
// renamed column.
#[test]
fn fix_59705_change_column_across_a_constraint() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    let off = StmtContext::for_query().with_foreign_key_checks(false);
    ddl::run_create_table_in(
        "create table child (id int, pid_test int, foreign key (pid_test) references parent(pid))",
        &mut catalog,
        "test",
        ddl::CreateTableSettings {
            foreign_key_checks: false,
            ..ddl::CreateTableSettings::default()
        },
        &off,
    )
    .unwrap();

    // The parent is now created (the state Go's failure message describes);
    // the child's constraint was stored unchecked above.
    ddl::run_create_table_in(
        "create table parent(pid int primary key)",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // int -> varchar across the constraint: 3780.
    let error = ddl::run_alter_table_in(
        "alter table child change column pid_test pid varchar(10);",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go: [ddl:3780]Referencing column 'pid' and referenced column 'pid' in foreign key constraint 'fk_1' are incompatible.");
    assert_eq!(err_code(&error), 3780);

    // Same-typed rename: accepted.
    ddl::run_alter_table_in(
        "alter table child change column pid_test pid int",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();

    let table = kv_table(&catalog, "test", "child");
    let keys = table.foreign_keys();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].name, "fk_1");
    assert_eq!(keys[0].cols, vec!["pid".to_owned()]);
    assert!(table.indexes().iter().any(|index| index.name == "fk_1"));
    let mut table = table;
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx)).unwrap();
}
