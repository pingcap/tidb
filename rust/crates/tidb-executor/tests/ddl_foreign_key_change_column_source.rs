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

//! GO PORT of `pkg/ddl/foreign_key_test.go:445 TestFix59705`
//! (`pkg/ddl.part6` batch b105).
//!
//! The Go test pins the `CHANGE COLUMN` rules Go's
//! `checkModifyColumnWithForeignKeyConstraint` (pkg/ddl/foreign_key.go:301)
//! enforces over a constraint that was stored UNRESOLVED because
//! `@@foreign_key_checks` was OFF at create time (a child created before its
//! parent). The statement sequence:
//!
//! 1. `create table child (id int, pid_test int, foreign key (pid_test)
//!    references parent(pid))` with the checks OFF -- succeeds even though
//!    `parent` does not exist; Go's `buildFKInfo` stores the constraint as
//!    written and `addIndexForForeignKey` (pkg/ddl/create_table.go:1728)
//!    auto-adds the implicit `KEY fk_1 (pid_test)`;
//! 2. `alter table child change column pid_test pid varchar(10)` while the
//!    parent is STILL missing -- Go refuses with 1146 "Table 'test.parent'
//!    doesn't exist" (foreign_key.go:311-313 propagates the
//!    `TableByName` error). See the `#[ignore]` gap test below: this tier's
//!    `check_modify_column` cannot ask a missing parent and skips instead.
//! 3. `create table parent(pid int primary key)`;
//! 4. the same `CHANGE ... varchar(10)` now refuses with 3780
//!    "Referencing column 'pid' and referenced column 'pid' in foreign key
//!    constraint 'fk_1' are incompatible" (foreign_key.go:315-317,
//!    `ErrFKIncompatibleColumns`);
//! 5. `change column pid_test pid int` succeeds: the type now matches the
//!    referenced column, and Go's early return
//!    (foreign_key.go:303-305) skips the whole check when type, `Flen` and
//!    `Decimal` are unchanged.

use tidb_executor::{
    run_alter_table_in, run_create_table_in, Catalog, CreateTableSettings, DriverError,
    StmtContext,
};

/// A catalog holding Go's `child` (created with the checks OFF, parent still
/// missing) and, from step 3 on, `parent` itself.
fn catalog_with_child_and_parent() -> Catalog {
    let mut catalog = Catalog::default();
    // Step 1: checks OFF -- Go's `set @@foreign_key_checks=0` session.
    let checks_off = CreateTableSettings {
        foreign_key_checks: false,
        ..CreateTableSettings::default()
    };
    run_create_table_in(
        "create table child (id int, pid_test int, foreign key (pid_test) references parent(pid))",
        &mut catalog,
        "test",
        checks_off,
        &StmtContext::for_query(),
    )
    .expect("a child referencing a missing parent is created with the checks off");
    // Step 3.
    run_create_table_in(
        "create table parent(pid int primary key)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .expect("parent creates with the checks on");
    catalog
}

/// Steps 1 + 2'(stored shape): the constraint is stored exactly as written
/// while the parent is missing, and the implicit `KEY fk_1` exists over the
/// referencing column -- Go's `addIndexForForeignKey`
/// (pkg/ddl/create_table.go:1728-1762) runs in `buildTableInfo` regardless of
/// `@@foreign_key_checks`, because only the REFERENCE resolution is skipped.
#[test]
fn fix59705_checks_off_create_stores_the_constraint_as_written() {
    let catalog = {
        let mut catalog = Catalog::default();
        let checks_off = CreateTableSettings {
            foreign_key_checks: false,
            ..CreateTableSettings::default()
        };
        run_create_table_in(
            "create table child (id int, pid_test int, foreign key (pid_test) references parent(pid))",
            &mut catalog,
            "test",
            checks_off,
            &StmtContext::for_query(),
        )
        .unwrap();
        catalog
    };
    let Some(tidb_executor::TableEntry::Kv(child)) = catalog.table_in("test", "child") else {
        panic!("child is a TiKV-backed table");
    };
    let foreign_keys = child.foreign_keys();
    assert_eq!(foreign_keys.len(), 1, "one stored constraint");
    let fk = &foreign_keys[0];
    assert_eq!(fk.name, "fk_1", "Go names it fk_1 (create_table.go:1449)");
    assert_eq!(fk.cols, vec!["pid_test".to_owned()]);
    assert_eq!(fk.ref_schema, "test");
    assert_eq!(fk.ref_table, "parent");
    assert_eq!(fk.ref_cols, vec!["pid".to_owned()]);
    // The implicit index over the referencing columns, named after the
    // constraint (create_table.go:1754-1757: no covering index ->
    // `idxName := fk.Name`).
    let implicit = child
        .indexes()
        .iter()
        .find(|index| index.name == "fk_1")
        .expect("the implicit KEY fk_1 exists");
    assert_eq!(
        implicit
            .column_offsets
            .iter()
            .map(|offset| child.columns[*offset].name.clone())
            .collect::<Vec<_>>(),
        vec!["pid_test".to_owned()],
    );
}

/// Steps 3-5 replayed to their Go outcome on a FRESH catalog each time (the
/// missing-parent refusal of step 2 is a named gap in this tier -- see the
/// ignored test below -- so the refused statement is skipped rather than
/// replayed): `CHANGE ... varchar(10)` is 3780, `CHANGE ... int` succeeds and
/// leaves the renamed column plus the constraint and its implicit key
/// pointing at the NEW name, exactly what Go's final `show create table`
/// asserts.
#[test]
fn fix59705_change_column_type_rules_match_source() {
    // Step 4: varchar(10) over an int referenced column is 3780. Go:
    // `[ddl:3780]Referencing column 'pid' and referenced column 'pid' in
    // foreign key constraint 'fk_1' are incompatible.`
    {
        let mut catalog = catalog_with_child_and_parent();
        let error = run_alter_table_in(
            "alter table child change column pid_test pid varchar(10)",
            &mut catalog,
            "test",
            &StmtContext::for_query(),
        )
        .expect_err("the type move is refused");
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 3780, "Go's ErrFKIncompatibleColumns");
        // The statement was refused: nothing moved.
        let Some(tidb_executor::TableEntry::Kv(child)) = catalog.table_in("test", "child") else {
            panic!("child is a TiKV-backed table");
        };
        assert!(
            child.columns.iter().any(|column| column.name == "pid_test"),
            "the column keeps its old name after a refusal"
        );
    }
    // Step 5: the same rename onto the SAME type succeeds, and the final
    // shape is Go's `show create table child` row: `pid int`, `KEY fk_1
    // (pid)`, `CONSTRAINT fk_1 FOREIGN KEY (pid) REFERENCES parent (pid)`.
    {
        let mut catalog = catalog_with_child_and_parent();
        run_alter_table_in(
            "alter table child change column pid_test pid int",
            &mut catalog,
            "test",
            &StmtContext::for_query(),
        )
        .expect("the same-type rename is accepted");
        let Some(tidb_executor::TableEntry::Kv(child)) = catalog.table_in("test", "child") else {
            panic!("child is a TiKV-backed table");
        };
        let names: Vec<_> = child.columns.iter().map(|column| column.name.clone()).collect();
        assert_eq!(names, vec!["id".to_owned(), "pid".to_owned()]);
        let fk = &child.foreign_keys()[0];
        assert_eq!(fk.cols, vec!["pid".to_owned()]);
        assert_eq!(fk.ref_cols, vec!["pid".to_owned()]);
        assert_eq!(fk.ref_table, "parent");
        let implicit = child
            .indexes()
            .iter()
            .find(|index| index.name == "fk_1")
            .expect("the implicit KEY survives the rename");
        assert_eq!(
            implicit
                .column_offsets
                .iter()
                .map(|offset| child.columns[*offset].name.clone())
                .collect::<Vec<_>>(),
            vec!["pid".to_owned()],
        );
    }
}

/// GO PORT of `pkg/ddl/foreign_key_test.go:453` (step 2 of TestFix59705).
///
/// Re-derived contract: with `parent` still missing, `alter table child
/// change column pid_test pid varchar(10)` must refuse with
/// `[schema:1146]Table 'test.parent' doesn't exist`, because
/// `checkModifyColumnWithForeignKeyConstraint` propagates the
/// `is.TableByName` error for a missing REFERENCED table
/// (pkg/ddl/foreign_key.go:311-313).
#[test]
#[ignore = "go-parity-gap: check_modify_column cannot ask a missing parent and `continue`s instead of raising Go's 1146 (crates/tidb-executor/src/foreign_key.rs, declared-keys arm)"]
fn fix59705_change_against_a_missing_parent_is_1146() {
    let mut catalog = {
        let mut catalog = Catalog::default();
        let checks_off = CreateTableSettings {
            foreign_key_checks: false,
            ..CreateTableSettings::default()
        };
        run_create_table_in(
            "create table child (id int, pid_test int, foreign key (pid_test) references parent(pid))",
            &mut catalog,
            "test",
            checks_off,
            &StmtContext::for_query(),
        )
        .unwrap();
        catalog
    };
    let error = run_alter_table_in(
        "alter table child change column pid_test pid varchar(10)",
        &mut catalog,
        "test",
        &StmtContext::for_query(),
    )
    .expect_err("Go refuses with 1146 while the parent is missing");
    assert_eq!(error.to_mysql_error().code, 1146);
}

/// GO PORT of `pkg/ddl/foreign_key_test.go:462` (step 4's message TEXT).
///
/// Go names the NEW column in the 3780 message:
/// `ErrFKIncompatibleColumns.GenWithStackByArgs(newCol.Name, fkInfo.RefCols[i],
/// fkInfo.Name)` (pkg/ddl/foreign_key.go:315-317, called from
/// pkg/ddl/modify_column.go:1912 where `newCol.Name` is the post-CHANGE name
/// `pid`), so the captured message reads "Referencing column 'pid' ...".
#[test]
#[ignore = "go-parity-gap: check_modify_column reports the OLD name (`pid_test`) as the referencing column instead of Go's post-CHANGE name (`pid`), foreign_key.go:315 vs foreign_key.rs declared-keys arm"]
fn fix59705_3780_message_names_the_new_column() {
    let mut catalog = catalog_with_child_and_parent();
    let error = run_alter_table_in(
        "alter table child change column pid_test pid varchar(10)",
        &mut catalog,
        "test",
        &StmtContext::for_query(),
    )
    .expect_err("3780 either way");
    match error {
        DriverError::FkIncompatibleColumns {
            referencing,
            referenced,
            constraint,
        } => {
            // Go's spellings (foreign_key.go:315-317).
            assert_eq!(referencing, "pid");
            assert_eq!(referenced, "pid");
            assert_eq!(constraint, "fk_1");
        }
        other => panic!("expected FkIncompatibleColumns, got {other:?}"),
    }
}
