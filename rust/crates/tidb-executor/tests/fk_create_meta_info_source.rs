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

//! Ports of the `pkg/ddl/tests/fk/foreign_key_test.go` meta family (part12
//! items 708-710, 715, 716 of `pkg/ddl`'s `func Test*`/`func Benchmark*`
//! declarations sorted by file and line), read from `origin/master`.
//!
//! Go creates FK-bearing tables through SQL under
//! `@@global.tidb_enable_foreign_key=1`, then reads the CHILD's
//! `TableInfo.ForeignKeys` (`model.FKInfo`: id, name, cols, ref schema/
//! table/cols, actions, State public, Version 1) and the PARENT's
//! `infoschema.GetTableReferredForeignKeys` (`model.ReferredFKInfo`:
//! Cols, ChildSchema, ChildTable, ChildFKName) back from the infoschema,
//! plus the auto-created supporting index and `FKInfo.String`.
//!
//! This tier carries the same two halves: the child constraint on
//! `KvTable::foreign_keys` and — because nothing caches a referred-FK
//! registry — the parent half computed the way Go's runtime checks compute
//! it (`rust/crates/tidb-executor/src/foreign_key.rs:175`, Go
//! `buildFKCheckForReferredFK`): a catalog-wide scan for constraints whose
//! `ref_schema`/`ref_table` name the parent. The tests below re-derive the
//! parent half with the same scan over the catalog's public enumeration.
//! Not carried (each noted where the Go assertion names it): the per-FK
//! numeric `ID` and `State`/`Version` fields (the tier's constraints are
//! always public), the `@@global.tidb_enable_foreign_key` switch (the tier
//! has no global variable surface; the per-statement `foreign_key_checks`
//! is the equivalent control), and `FKInfo.String`'s exact rendering.

use tidb_executor::ddl::{self, CreateTableSettings};
use tidb_executor::{Catalog, FkAction, StmtContext, TableEntry};

/// The foreign keys of a storage-backed table.
fn foreign_keys_of(catalog: &Catalog, db: &str, table: &str) -> Vec<tidb_executor::KvForeignKey> {
    match catalog.table_in(db, table) {
        Some(TableEntry::Kv(table)) => table.foreign_keys().to_vec(),
        other => panic!("expected a storage-backed table {db}.{table}, got {other:?}"),
    }
}

/// The indexes of a storage-backed table, as (name, first column) pairs.
fn index_names(catalog: &Catalog, db: &str, table: &str) -> Vec<String> {
    match catalog.table_in(db, table) {
        Some(TableEntry::Kv(table)) => table.indexes().iter().map(|index| index.name.clone()).collect(),
        other => panic!("expected a storage-backed table {db}.{table}, got {other:?}"),
    }
}

/// Go `getTableInfoReferredForeignKeys`: every `(child_db, child_table,
/// constraint)` in the catalog whose constraint names `db.table` as its
/// referenced table. Go's registry is populated at DDL time; this tier's
/// equivalent (the one its own FK checks use) resolves on demand, so the
/// same query is a scan over the catalog's enumeration — which is sorted,
/// while Go's list is registration-ordered. Every assertion below only
/// orders children whose two orders coincide, or is order-agnostic.
fn referred_foreign_keys(
    catalog: &Catalog,
    db: &str,
    table: &str,
) -> Vec<(String, String, tidb_executor::KvForeignKey)> {
    let mut found = Vec::new();
    for child_db in catalog.database_names() {
        let Some(tables) = catalog.table_names(&child_db) else {
            continue;
        };
        for child_table in tables {
            let Some(TableEntry::Kv(child)) = catalog.table_in(&child_db, &child_table) else {
                continue;
            };
            for foreign_key in child.foreign_keys() {
                if foreign_key.ref_schema.eq_ignore_ascii_case(db)
                    && foreign_key.ref_table.eq_ignore_ascii_case(table)
                {
                    found.push((child_db.clone(), child_table.clone(), foreign_key.clone()));
                }
            }
        }
    }
    found
}

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

// --- TestCreateTableWithForeignKeyMetaInfo
//     (pkg/ddl/tests/fk/foreign_key_test.go:40) ---
//
// Go creates test.t1 (parent), then test2.t2 with
// `foreign key fk_b(b) references test.t1(id) ON UPDATE RESTRICT ON DELETE
// CASCADE`, and requires:
//   * t1: zero declared ForeignKeys, exactly one referred entry
//     {Cols:[id], ChildSchema:test2, ChildTable:t2, ChildFKName:fk_b};
//   * t2: zero referred entries, exactly one declared FK
//     {fk_b, [b] -> test.t1 [id], ON DELETE CASCADE (Go action 2), ON
//     UPDATE RESTRICT (Go action 1), State public, Version 1};
//   * t2's supporting index fk_b auto-created (t2 has no other index);
//   * t3 (referencing t2 with its own idx_b) reuses idx_b and adds none;
//   * t5 self-referencing through an UNNAMED constraint gets fk_1 (the
//     MaxForeignKeyID counter) and an auto-created fk_1 index;
//   * dropping test2 clears every referred entry that pointed into it.
//
// The FKInfo.String rendering legs and the ID/State/Version numeric fields
// have no carrier here (see the module doc).
#[test]
fn create_table_fk_meta_lands_on_both_sides() {
    let mut catalog = Catalog::default();
    let ctx = ctx();
    let off = StmtContext::for_query().with_foreign_key_checks(false);
    ddl::run_create_table_in(
        "create table t1 (id int key, a int, b int as (a) virtual)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    catalog.create_database("test2");
    ddl::run_create_table_in(
        "create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id) \
         ON UPDATE RESTRICT ON DELETE CASCADE)",
        &mut catalog,
        "test2",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // The parent: no declared FKs, one referred entry naming the child.
    assert!(foreign_keys_of(&catalog, "test", "t1").is_empty(), "Go: len(tb1Info.ForeignKeys) == 0");
    let referred = referred_foreign_keys(&catalog, "test", "t1");
    assert_eq!(referred.len(), 1, "Go: GetTableReferredForeignKeys(test, t1) == 1");
    let (child_db, child_table, foreign_key) = &referred[0];
    assert_eq!(child_db, "test2");
    assert_eq!(child_table, "t2");
    assert_eq!(foreign_key.name, "fk_b");
    // Go's ReferredFKInfo.Cols is the PARENT's referenced columns.
    assert_eq!(foreign_key.ref_cols, vec!["id".to_owned()]);

    // The child: one declared FK with both actions, no referred entries.
    let keys = foreign_keys_of(&catalog, "test2", "t2");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].name, "fk_b");
    assert_eq!(keys[0].cols, vec!["b".to_owned()]);
    assert_eq!(keys[0].ref_schema, "test");
    assert_eq!(keys[0].ref_table, "t1");
    assert_eq!(keys[0].ref_cols, vec!["id".to_owned()]);
    assert!(matches!(keys[0].on_delete, FkAction::Cascade), "Go: OnDelete == ast.ReferOptionCascade (2)");
    assert!(matches!(keys[0].on_update, FkAction::Restrict), "Go: OnUpdate == ast.ReferOptionRestrict (1)");
    assert!(referred_foreign_keys(&catalog, "test2", "t2").is_empty());

    // Auto-created supporting index: Go requires len(tb2Info.Indices) == 1
    // named fk_b.
    assert_eq!(index_names(&catalog, "test2", "t2"), vec!["fk_b".to_owned()]);

    // t3 references t2 over an EXISTING idx_b: no extra index is added and
    // t2 gains the referred entry.
    ddl::run_create_table_in(
        "create table t3 (id int, b int, index idx_b(b), foreign key fk_b(b) references t2(id) \
         ON UPDATE SET NULL ON DELETE NO ACTION)",
        &mut catalog,
        "test2",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    assert_eq!(referred_foreign_keys(&catalog, "test2", "t2").len(), 1);
    let t3_keys = foreign_keys_of(&catalog, "test2", "t3");
    assert_eq!(t3_keys.len(), 1);
    assert_eq!(t3_keys[0].name, "fk_b");
    assert_eq!(t3_keys[0].ref_schema, "test2");
    assert_eq!(t3_keys[0].ref_table, "t2");
    assert!(matches!(t3_keys[0].on_delete, FkAction::NoAction), "Go: ON DELETE NO ACTION");
    assert!(matches!(t3_keys[0].on_update, FkAction::SetNull), "Go: ON UPDATE SET NULL");
    assert_eq!(index_names(&catalog, "test2", "t3"), vec!["idx_b".to_owned()], "Go: idx_b reused, none added");

    // t5: an UNNAMED self-referencing constraint is fk_1 (Go
    // `fk_{MaxForeignKeyID+1}`), with the auto-created fk_1 index.
    // DOCUMENTED DIVERGENCE: Go accepts this CREATE with
    // foreign_key_checks ON — its owner resolves the reference against the
    // table's own new TableInfo — while this tier's parent lookup cannot see
    // the table being registered yet and answers 1146, so the create runs
    // with the checks off here (the same divergence the error-matrix gap
    // test records).
    ddl::run_create_table_in(
        "create table t5 (id int key, a int, b int, foreign key (a) references t5(id))",
        &mut catalog,
        "test2",
        CreateTableSettings {
            foreign_key_checks: false,
            ..CreateTableSettings::default()
        },
        &off,
    )
    .unwrap();
    let t5_keys = foreign_keys_of(&catalog, "test2", "t5");
    assert_eq!(t5_keys.len(), 1);
    assert_eq!(t5_keys[0].name, "fk_1");
    assert_eq!(t5_keys[0].ref_schema, "test2");
    assert_eq!(t5_keys[0].ref_table, "t5");
    assert_eq!(referred_foreign_keys(&catalog, "test2", "t5").len(), 1);
    assert_eq!(index_names(&catalog, "test2", "t5"), vec!["fk_1".to_owned()]);

    // Dropping the child database clears every referred entry into it
    // (Go's final legs after `set @@global.tidb_enable_foreign_key=0` and
    // `drop database test2`).
    assert!(catalog.drop_database("test2"));
    assert!(referred_foreign_keys(&catalog, "test2", "t2").is_empty());
    assert!(referred_foreign_keys(&catalog, "test2", "t5").is_empty());
}

// --- TestCreateTableWithForeignKeyMetaInfo2
//     (pkg/ddl/tests/fk/foreign_key_test.go:164) ---
//
// Go creates the CHILD first — with `@@foreign_key_checks=0`, so the
// constraint against the not-yet-existing test.t1 stores unchecked — then
// creates the parent and a third table with TWO constraints; requires the
// declared/referred split on all three tables (t1 referred by t2 and t3,
// t2 referred by t3), both of t3's constraints, and t3's single
// auto-created fk_a index while t2's fk_b one exists.
#[test]
fn child_first_fk_meta_resolves_once_the_parent_lands() {
    let mut catalog = Catalog::default();
    let ctx = ctx();
    let off = StmtContext::for_query().with_foreign_key_checks(false);
    catalog.create_database("test2");
    ddl::run_create_table_in(
        "create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id) \
         ON UPDATE RESTRICT ON DELETE CASCADE)",
        &mut catalog,
        "test2",
        CreateTableSettings {
            foreign_key_checks: false,
            ..CreateTableSettings::default()
        },
        &off,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t1 (id int key, a int, b int as (a) virtual)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // t1 is referred by t2 exactly once; t2 declares one FK.
    let referred = referred_foreign_keys(&catalog, "test", "t1");
    assert_eq!(referred.len(), 1);
    assert_eq!(referred[0].0, "test2");
    assert_eq!(referred[0].1, "t2");
    assert_eq!(referred[0].2.name, "fk_b");
    assert!(foreign_keys_of(&catalog, "test", "t1").is_empty());
    assert_eq!(foreign_keys_of(&catalog, "test2", "t2").len(), 1);
    assert_eq!(index_names(&catalog, "test2", "t2"), vec!["fk_b".to_owned()]);

    // t3 carries fk_a (→ t1) and fk_a2 (→ t2); only fk_a needs its own
    // auto-created index (fk_a2's `a` is already covered by it).
    ddl::run_create_table_in(
        "create table t3 (id int key, a int, foreign key fk_a(a) references test.t1(id) \
         ON DELETE CASCADE ON UPDATE RESTRICT, foreign key fk_a2(a) references test2.t2(id))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    let referred_t1 = referred_foreign_keys(&catalog, "test", "t1");
    assert_eq!(referred_t1.len(), 2, "Go: t1 referred by t2 AND t3");
    assert!(referred_t1.iter().any(|(db, table, fk)| (db.as_str(), table.as_str()) == ("test", "t3") && fk.name == "fk_a"));
    assert!(referred_t1.iter().any(|(db, table, fk)| (db.as_str(), table.as_str()) == ("test2", "t2") && fk.name == "fk_b"));
    let t3_keys = foreign_keys_of(&catalog, "test", "t3");
    assert_eq!(t3_keys.len(), 2);
    assert_eq!(t3_keys[0].name, "fk_a");
    assert_eq!(t3_keys[0].ref_table, "t1");
    assert!(matches!(t3_keys[0].on_delete, FkAction::Cascade));
    assert!(matches!(t3_keys[0].on_update, FkAction::Restrict));
    assert_eq!(t3_keys[1].name, "fk_a2");
    assert_eq!(t3_keys[1].ref_schema, "test2");
    assert_eq!(t3_keys[1].ref_table, "t2");
    assert!(matches!(t3_keys[1].on_delete, FkAction::NoOption));
    assert_eq!(index_names(&catalog, "test", "t3"), vec!["fk_a".to_owned()], "Go: exactly one auto-created index");
}

// --- TestCreateTableWithForeignKeyMetaInfo3
//     (pkg/ddl/tests/fk/foreign_key_test.go:294) ---
//
// Go creates t1 then t2/t3/t4 all referencing it, snapshots the referred
// list, drops t3, re-creates the shape as t5, and requires the SNAPSHOT to
// still read t2/t3/t4 in order — the referred list is immutable once
// captured (Go reads it BEFORE the drop).
#[test]
fn the_captured_referred_list_is_immune_to_later_ddl() {
    let mut catalog = Catalog::default();
    let ctx = ctx();
    ddl::run_create_table_in(
        "create table t1 (id int key, a int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    for name in ["t2", "t3", "t4"] {
        ddl::run_create_table_in(
            &format!("create table {name} (id int key, b int, foreign key fk_b(b) references test.t1(id))"),
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        )
        .unwrap();
    }
    // The snapshot: children t2, t3, t4. (The tier's scan enumerates sorted;
    // Go's registry is insertion-ordered — the two orders coincide for
    // t2/t3/t4.)
    let snapshot = referred_foreign_keys(&catalog, "test", "t1");
    assert_eq!(
        snapshot.iter().map(|(_, table, _)| table.clone()).collect::<Vec<_>>(),
        vec!["t2", "t3", "t4"],
        "Go: tb1ReferredFKs children are t2, t3, t4 in order"
    );

    // Dropping t3 and re-creating as t5 does not mutate the snapshot.
    ddl::run_drop_table_in(
        "drop table test.t3",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t5 (id int key, b int, foreign key fk_b(b) references test.t1(id))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    assert_eq!(
        snapshot.iter().map(|(_, table, _)| table.clone()).collect::<Vec<_>>(),
        vec!["t2", "t3", "t4"],
        "Go: the captured list still reads t2/t3/t4 after t3 was dropped"
    );
    // The LIVE list now names t2, t4, t5.
    let live = referred_foreign_keys(&catalog, "test", "t1");
    assert_eq!(live.len(), 3);
    assert!(live.iter().all(|(_, table, _)| ["t2", "t4", "t5"].contains(&table.as_str())));

    // Go (pkg/ddl/tests/fk/foreign_key_test.go:328): `show create table`
    // for t3/t5's shapes is asserted there; this tier has no SHOW CREATE
    // renderer (documented, not approximated).
}

// --- TestDropChildTableForeignKeyMetaInfo
//     (pkg/ddl/tests/fk/foreign_key_test.go:754) ---
//
// Go: t1 self-references via a named `fk` constraint — referred == 1;
// `drop table t1` clears it to 0. Then t1 (plain) ← t2 with `fk`: referred
// == 1; `drop table t2` (the CHILD) clears the parent's referred list to 0.
// DOCUMENTED DIVERGENCE on the create: Go accepts the self-referencing
// CREATE with foreign_key_checks ON (its owner resolves the reference
// against the table being created); this tier answers 1146, so the create
// runs with the checks off (see the error-matrix gap test).
#[test]
fn dropping_either_side_clears_the_referred_entries() {
    let mut catalog = Catalog::default();
    let ctx = ctx();
    let off = StmtContext::for_query().with_foreign_key_checks(false);
    ddl::run_create_table_in(
        "create table t1 (id int key, a int, b int, CONSTRAINT fk foreign key (a) references t1(id))",
        &mut catalog,
        "test",
        CreateTableSettings {
            foreign_key_checks: false,
            ..CreateTableSettings::default()
        },
        &off,
    )
    .unwrap();
    assert_eq!(referred_foreign_keys(&catalog, "test", "t1").len(), 1);
    ddl::run_drop_table_in(
        "drop table t1",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    assert!(referred_foreign_keys(&catalog, "test", "t1").is_empty());

    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int, foreign key fk (a) references t1(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    assert_eq!(referred_foreign_keys(&catalog, "test", "t1").len(), 1);
    ddl::run_drop_table_in(
        "drop table t2",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    assert!(referred_foreign_keys(&catalog, "test", "t1").is_empty());
}

// --- TestDropForeignKeyMetaInfo (pkg/ddl/tests/fk/foreign_key_test.go:775)
//
// Go: t1 with a named self-referencing `fk` — `alter table t1 drop foreign
// key fk` empties BOTH the declared ForeignKeys and the referred list.
// Then t1 ← t2 (`fk` over t1(b)): `alter table t2 drop foreign key fk`
// clears t1's referred list and t2's declared ForeignKeys.
// DOCUMENTED DIVERGENCE on the create: Go accepts the self-referencing
// CREATE with foreign_key_checks ON; this tier answers 1146, so the create
// runs with the checks off (see the error-matrix gap test).
#[test]
fn drop_foreign_key_clears_both_meta_halves() {
    let mut catalog = Catalog::default();
    let ctx = ctx();
    let off = StmtContext::for_query().with_foreign_key_checks(false);
    ddl::run_create_table_in(
        "create table t1 (id int key, a int, b int, CONSTRAINT fk foreign key (a) references t1(id))",
        &mut catalog,
        "test",
        CreateTableSettings {
            foreign_key_checks: false,
            ..CreateTableSettings::default()
        },
        &off,
    )
    .unwrap();
    assert_eq!(referred_foreign_keys(&catalog, "test", "t1").len(), 1);
    ddl::run_alter_table_in("alter table t1 drop foreign key fk", &mut catalog, "test", &ctx).unwrap();
    assert!(foreign_keys_of(&catalog, "test", "t1").is_empty());
    assert!(referred_foreign_keys(&catalog, "test", "t1").is_empty());

    ddl::run_drop_table_in(
        "drop table t1",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int, foreign key fk (a) references t1(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    assert_eq!(referred_foreign_keys(&catalog, "test", "t1").len(), 1);
    ddl::run_alter_table_in("alter table t2 drop foreign key fk", &mut catalog, "test", &ctx).unwrap();
    assert!(referred_foreign_keys(&catalog, "test", "t1").is_empty());
    assert!(foreign_keys_of(&catalog, "test", "t2").is_empty());
}
