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

//! Ports of the remaining `pkg/ddl/tests/fk/foreign_key_test.go` family
//! (part12 items 711-713, 717-720 of `pkg/ddl`'s `func Test*`/`func
//! Benchmark*` declarations sorted by file and line), read from
//! `origin/master`: the ALTER-side FK surface, the rename/truncate meta
//! maintenance, and the two privilege checks. Go drives these through SQL
//! under `@@global.tidb_enable_foreign_key=1`; that switch has no carrier
//! here (the per-statement `foreign_key_checks` is the equivalent control),
//! and every divergence found is written in the test's comment rather than
//! papered over.

use tidb_datatype::Datum;
use tidb_executor::ddl::{self, CreateTableSettings};
use tidb_executor::{admin_check, run_delete_on, run_insert_on, run_select_on, Catalog, KvForeignKey, RowDecodeContext, StmtContext, TableEntry};

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

/// Go `getTableInfoReferredForeignKeys`: the constraints in the catalog that
/// name `db.table` as their referenced table (computed on demand here; see
/// the sibling `fk_create_meta_info_source` module doc).
fn referred_foreign_keys(catalog: &Catalog, db: &str, table: &str) -> Vec<(String, String, KvForeignKey)> {
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

fn declared(catalog: &Catalog, db: &str, table: &str) -> Vec<KvForeignKey> {
    match catalog.table_in(db, table) {
        Some(TableEntry::Kv(table)) => table.foreign_keys().to_vec(),
        other => panic!("expected a storage-backed table {db}.{table}, got {other:?}"),
    }
}

// --- TestAddForeignKey (pkg/ddl/tests/fk/foreign_key_test.go:1077) ---
//
// Go's ALTER-side ladder, re-derived from the Go assertions:
//   * `alter table t2 add foreign key (b) references t1(id)` (parent side
//     covered by the clustered primary key) succeeds and bumps
//     MaxForeignKeyID to 1;
//   * the same against a parent column with NO index fails
//     `infoschema.ErrForeignKeyNoIndexInParent` — the separate gap test
//     below;
//   * after the parent gains `index(b)`, the constraint lands;
//   * a constraint referencing the table ITSELF fails
//     `infoschema.ErrCannotAddForeign` — also in the gap test below;
//   * `add constraint fk foreign key (b) references t1(b)` auto-creates the
//     support index named fk, and the planner reads t2 through it;
//   * adding THREE constraints in ONE statement lands all of them plus the
//     shared idx_c, in order, all public, and every orphan insert fails
//     `[planner:1452]ErrNoReferencedRow2`;
//   * the SAME multi-add with one bad referenced column adds NOTHING
//     (atomicity — gap test below);
//   * the circular-dependency ADD fails
//     `[ddl:1452]Cannot add or update a child row: ...` and leaves neither
//     table's meta touched;
//   * the auto-create-index arm's failure leaves the meta untouched too.
//
// The EXPLAIN/`use index` legs read Go's plan tree (`IndexReader` over
// `index:fk`); this tier's SELECT reads the same rows through the same
// index and asserts the row content.
#[test]
fn alter_add_foreign_key_lands_constraints_and_enforces_child_side() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (id int key, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in("alter table t2 add index(b)", &mut catalog, "test", &ctx).unwrap();
    ddl::run_alter_table_in(
        "alter table t2 add foreign key (b) references t1(id)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    // Go: require.Equal(t, int64(1), tbl2Info.MaxForeignKeyID) — the tier's
    // counter is private; the NEXT unnamed constraint's computed name is its
    // public echo (`fk_{MaxForeignKeyID+1}`).
    let next = match catalog.table_in("test", "t2") {
        Some(TableEntry::Kv(table)) => table.next_foreign_key_name(),
        _ => panic!("expected a storage-backed table"),
    };
    assert_eq!(next, "fk_2", "Go: MaxForeignKeyID == 1 after the first add");

    // Go: `alter table t2 add foreign key (b) references t1(b);` fails with
    // ErrForeignKeyNoIndexInParent while t1 has no index on b — the tier has
    // no parent-side index check (divergence, see the ignored test below).

    // After the parent gains its index, the constraint lands.
    ddl::run_alter_table_in("alter table t1 add index(b)", &mut catalog, "test", &ctx).unwrap();
    ddl::run_alter_table_in(
        "alter table t2 add foreign key (b) references t1(b)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert_eq!(declared(&catalog, "test", "t2").len(), 2);

    // Auto-create index for a named constraint, then read through it.
    ddl::run_drop_table_in(
        "drop table if exists t1, t2",
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
        "create table t2 (id int key, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t2 add constraint fk foreign key (b) references t1(b)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let table = match catalog.table_in("test", "t2") {
        Some(TableEntry::Kv(table)) => table,
        _ => panic!("expected a storage-backed table"),
    };
    assert_eq!(table.indexes().len(), 1, "Go: len(tbl2Info.Indices) == 1");
    assert_eq!(table.indexes()[0].name, "fk");
    let rows = run_select_on("select b from t2 use index(fk)", &mut catalog, &ctx).unwrap();
    assert!(rows.is_empty(), "Go: MustQuery('select b from t2 use index(fk)') is empty");

    // Three constraints in ONE statement, with their shared index.
    ddl::run_alter_table_in(
        "alter table t2 add column c int, add column d int, add column e int",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t2 add index idx_c(c, d, e)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t2 add constraint fk_c foreign key (c) references t1(b), \
         add constraint fk_d foreign key (d) references t1(b), \
         add constraint fk_e foreign key (e) references t1(b)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let table = match catalog.table_in("test", "t2") {
        Some(TableEntry::Kv(table)) => table,
        _ => panic!("expected a storage-backed table"),
    };
    assert_eq!(table.indexes().len(), 4, "Go: len(tbl2Info.Indices) == 4");
    let index_names: Vec<String> = table.indexes().iter().map(|index| index.name.clone()).collect();
    assert_eq!(index_names, vec!["fk", "idx_c", "fk_d", "fk_e"], "Go's index order");
    let names: Vec<String> = table.foreign_keys().iter().map(|key| key.name.clone()).collect();
    assert_eq!(names, vec!["fk", "fk_c", "fk_d", "fk_e"], "Go's FK order");

    // Every orphan insert fails [planner:1452] — the parent has no rows.
    for (id, column) in [("1", "b"), ("2", "c"), ("3", "d"), ("4", "e")] {
        let error = run_insert_on(
            &format!("insert into t2 (id, {column}) values ({id}, 1)"),
            &mut catalog,
            &ctx,
        )
        .expect_err("Go: plannererrors.ErrNoReferencedRow2");
        assert_eq!(error.clone().to_mysql_error().code, 1452, "insert ({id}, {column})");
    }

    // Circular dependency: t1.a=1 has no match in t2, so the ADD fails with
    // 1452 and NEITHER table's meta moves. (Go's message appends `ON DELETE
    // CASCADE`; this tier renders the constraint without the action suffix
    // — the captured rendering divergence from b105.)
    ddl::run_drop_table_in(
        "drop table if exists t1, t2",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t1 (id int key, a int, index(a))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (id int key, a int, foreign key fk(a) references t1(id) ON DELETE CASCADE)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    run_insert_on("insert into t1 values (1, 1)", &mut catalog, &ctx).unwrap();
    let error = ddl::run_alter_table_in(
        "alter table t1 add foreign key fk(a) references t2(id) ON DELETE CASCADE",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go: [ddl:1452]Cannot add or update a child row");
    assert_eq!(error.clone().to_mysql_error().code, 1452);
    assert!(declared(&catalog, "test", "t1").is_empty(), "Go: len(tbl1Info.ForeignKeys) == 0");
    assert!(referred_foreign_keys(&catalog, "test", "t2").is_empty(), "Go: no referred FKs on t2");

    // The auto-create-index arm's failure: same refusal, same untouched
    // meta — this time the child has NO support index to offer.
    ddl::run_drop_table_in(
        "drop table if exists t1, t2",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t1 (id int key, a int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (id int key)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    run_insert_on("insert into t1 values (1, 1)", &mut catalog, &ctx).unwrap();
    let error = ddl::run_alter_table_in(
        "alter table t1 add foreign key fk(a) references t2(id) ON DELETE CASCADE",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go: [ddl:1452] with the auto-created index rolled back too");
    assert_eq!(error.clone().to_mysql_error().code, 1452);
    assert!(declared(&catalog, "test", "t1").is_empty());
    assert!(referred_foreign_keys(&catalog, "test", "t2").is_empty());

    // Go's SHOW CREATE assertions after each leg read Go's renderer; this
    // tier has none (documented, not approximated).
}

// The parent-index and self-reference rows of Go's TestAddForeignKey
// (pkg/ddl/tests/fk/foreign_key_test.go:1101, :1106):
//   * `alter table t2 add foreign key (b) references t1(b)` while t1 has NO
//     index on b fails `infoschema.ErrForeignKeyNoIndexInParent`
//     ([schema:1822], mysql/errcode.go:842);
//   * `alter table t2 add foreign key (b) references t2(b)` fails
//     `infoschema.ErrCannotAddForeign` ([schema:1215]).
//
// The Rust ALTER owner now checks the same parent-side index and
// same-column self-reference rules before staging metadata.
#[test]
fn alter_add_foreign_key_refuses_missing_parent_index_and_self_reference() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (id int key, b int, index(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    let error = ddl::run_alter_table_in(
        "alter table t2 add foreign key (b) references t1(b)",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go: parent b has no covering index");
    let mysql_error = error.to_mysql_error();
    assert_eq!(mysql_error.code, 1822);
    assert_eq!(
        mysql_error.message,
        "Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't1'"
    );
    assert!(declared(&catalog, "test", "t2").is_empty());

    ddl::run_alter_table_in("alter table t1 add index(b)", &mut catalog, "test", &ctx).unwrap();
    ddl::run_alter_table_in(
        "alter table t2 add foreign key (b) references t1(b)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();

    let error = ddl::run_alter_table_in(
        "alter table t2 add foreign key (b) references t2(b)",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go: same-column self-reference is not supported");
    let mysql_error = error.to_mysql_error();
    assert_eq!(mysql_error.code, 1215);
    assert_eq!(mysql_error.message, "Cannot add foreign key constraint");
    assert_eq!(declared(&catalog, "test", "t2").len(), 1);
}

// The multi-add atomicity rows of Go's TestAddForeignKey
// (pkg/ddl/tests/fk/foreign_key_test.go:1145-1153): one ALTER adding
// fk_c/fk_d/fk_e where fk_e names `t1(unknown_col)` fails with
// `infoschema.ErrForeignKeyNoColumnInParent` and leaves the table with ZERO
// constraints; and `alter table t2 drop index idx_c, add constraint fk_c
// foreign key (c) references t1(b)` fails `ErrDropIndexNeededInForeignKey`
// (1553) because the drop would strand the add.
//
// go-parity-gap (documented divergence): this tier's ALTER applies its
// actions in source order WITHOUT staging — fk_c and fk_d land before fk_e
// fails, and the drop-then-add pair drops first (nothing to strand yet) —
// so neither the atomic rollback nor the 1553 is reproducible.
#[test]
#[ignore = "go-parity-gap: multi-action ALTER is not staged, so Go's add-rollback and 1553 diverge"]
fn a_failed_multi_add_leaves_no_constraint_behind() {
    // Contract (foreign_key_test.go:1145-1153): the bad fk_e rolls back
    // fk_c and fk_d (0 constraints left); drop+add in one statement is 1553.
}

// The partial-index rows of Go's TestAddForeignKey
// (pkg/ddl/tests/fk/foreign_key_test.go:1162-1221): an `index idx_b(b)
// where c is not null` (partial, therefore UNSAFE) does not serve a new
// constraint — TiDB auto-creates `fk_b` beside it, `delete from t1` then
// fails `[planner:1451]ErrRowIsReferenced2`, and dropping the unsafe index
// strands the constraint (1553 on `fk_b`); an `index idx_b(b) where b is
// not null` (IS NOT NULL on the KEY column — safe) DOES serve it, and an
// IS NOT NULL partial index over a REFERENCED column accepts the
// constraint with 1452 for missing keys.
//
// go-parity-gap: this tier's index meta has no partial/WHERE dimension
// (`KvIndex` carries no predicate), so the safe/unsafe distinction — and
// therefore each of these assertions — is not reproducible.
#[test]
#[ignore = "go-parity-gap: partial (WHERE) index predicates do not exist in this tier's index meta"]
fn partial_index_safety_rules_match_go() {
    // Contract (foreign_key_test.go:1162-1221): unsafe partial index →
    // auto-created fk_b beside it, parent delete 1451, drop-of-support 1553;
    // safe IS NOT NULL partial index serves both child and parent sides.
}

// --- TestRenameColumnWithForeignKeyMetaInfo
//     (pkg/ddl/tests/fk/foreign_key_test.go:990), CHANGE COLUMN legs ---
//
// Go renames constraint columns and requires the constraint meta to follow
// (`updateFKInfoWhenModifyColumn`):
//   * t1 self-referencing `fk(a) references t1(id)`:
//     `change id kid int` moves the REFERENCED side to `kid`;
//   * t2 referencing t1(b): `change a aa int` moves the REFERENCING side to
//     `aa` while `b` stays referenced.
//
// The RENAME COLUMN legs of the same Go test (which this file records as a
// gap below) assert the same maintenance; here only CHANGE COLUMN is
// ported, because the tier refuses RENAME COLUMN on FK-participating tables.
// The Go self-ref create is accepted with foreign_key_checks off — Go
// accepts it with checks ON (its create resolves the table's own new
// TableInfo), which is the divergence recorded at the CREATE runner.
#[test]
fn change_column_renames_follow_the_constraint_meta() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    let off = StmtContext::for_query().with_foreign_key_checks(false);
    ddl::run_create_table_in(
        "create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))",
        &mut catalog,
        "test",
        CreateTableSettings {
            foreign_key_checks: false,
            ..CreateTableSettings::default()
        },
        &off,
    )
    .unwrap();
    ddl::run_alter_table_in("alter table t1 change id kid int", &mut catalog, "test", &ctx).unwrap();
    let keys = declared(&catalog, "test", "t1");
    assert_eq!(keys.len(), 1);
    // Go: RefCols and the parent's referred Cols both follow the rename.
    assert_eq!(keys[0].ref_cols, vec!["kid".to_owned()], "Go: RefCols follow the rename");
    let referred = referred_foreign_keys(&catalog, "test", "t1");
    assert_eq!(referred.len(), 1);
    assert_eq!(referred[0].2.ref_cols, vec!["kid".to_owned()], "Go: ReferredFKInfo.Cols follow the rename");

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
        "create table t2 (a int, b int, foreign key fk(a) references t1(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in("alter table t2 change a aa int", &mut catalog, "test", &ctx).unwrap();
    let referred = referred_foreign_keys(&catalog, "test", "t1");
    assert_eq!(referred.len(), 1);
    // Go: the parent's referred Cols are the REFERENCED columns ('b'), and
    // the child's declared Cols follow the rename to 'aa'.
    assert_eq!(referred[0].2.ref_cols, vec!["b".to_owned()], "Go: parent's referred Cols stay 'b'");
    let keys = declared(&catalog, "test", "t2");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].cols, vec!["aa".to_owned()], "Go: child's Cols follow the rename");
    assert_eq!(keys[0].ref_cols, vec!["b".to_owned()]);
}

// The RENAME COLUMN legs of Go's TestRenameColumnWithForeignKeyMetaInfo
// (pkg/ddl/tests/fk/foreign_key_test.go:993, :1029, :1059, :1092):
// `alter table t1 rename column a to aa` (on a self-referencing table),
// `alter table t2 rename column a to aa` (on a child), and the two-column
// finale where both constraints follow `rename b to bb` with the SHOW
// CREATE output printing KEY fk_1 (aa) / KEY fk_2 (bb).
//
// go-parity-gap (documented divergence): the tier REFUSES RENAME COLUMN on
// any FK-participating table ("changing the columns or name of a table
// involved in a FOREIGN KEY is not supported yet",
// `ddl/alter_table.rs:106-122`) where Go accepts it and rewrites the
// constraint meta — the refusal is a Go-leg behavior this tier does not
// have, so the maintenance the Go test asserts is not reachable.
#[test]
#[ignore = "go-parity-gap: RENAME COLUMN on an FK-participating table is refused instead of rewriting the meta"]
fn rename_column_follows_the_constraint_meta() {
    // Contract (foreign_key_test.go:990-1092): after every rename, the
    // constraint's Cols/RefCols (and the parent's referred Cols) carry the
    // NEW names, and show create table prints the moved index parts.
}

// Go's fourth pass drop (`pkg/ddl/tests/fk/foreign_key_test.go:920`, pass
// shape 1): `alter table t2 drop index idxb` where `idxb (b)` is the
// constraint's cover and `b` is ALSO the child's clustered primary handle —
// Go ALLOWS it because the handle keeps answering the constraint's lookups
// (Go `checkIndexNeededInForeignKey`'s PKIsHandle escape applies to the
// declared-FK branch too, pkg/ddl/foreign_key.go:459-464 via :476).
#[test]
fn dropping_the_child_handle_cover_is_allowed() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index idxb (b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int key, index idxa (a), index idxb (b), \
         foreign key fk_b(b) references t1(id))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in("alter table t2 drop index idxb", &mut catalog, "test", &ctx)
        .expect("Go: the clustered handle still covers the child constraint");
    let Some(TableEntry::Kv(table)) = catalog.table_in("test", "t2") else {
        panic!("expected a storage-backed table");
    };
    assert!(table
        .indexes()
        .iter()
        .all(|index| !index.name.eq_ignore_ascii_case("idxb")));
    assert_eq!(table.foreign_keys().len(), 1);
}

// --- TestCreateTableWithForeignKeyPrivilegeCheck
//     (pkg/ddl/tests/fk/foreign_key_test.go:313) ---
//
// Go grants `create` only and requires `create table t2 (... references
// t1(id))` to fail `[planner:1142]REFERENCES command denied to user
// 'u1'@'%' for table 't1'`; `grant references on test.t1` then lets it
// through, and the second constraint against an UNGRANTED t3 fails the
// same way before `grant references on test.t3` unlocks the statement.
//
// go-parity-gap: this tier has no privilege/auth carrier (no user
// identities, no grant table) — the same gap recorded for the sequence
// privilege rows in b110.
#[test]
#[ignore = "go-parity-gap: no auth carrier; Go's 1142 REFERENCES denial is not reproducible"]
fn create_table_foreign_key_requires_references_privilege() {
    // Contract (foreign_key_test.go:313-338): [planner:1142] per ungranted
    // parent, cleared per `grant references`.
}

// --- TestAlterTableWithForeignKeyPrivilegeCheck
//     (pkg/ddl/tests/fk/foreign_key_test.go:340) ---
//
// Go requires `alter table t2 add foreign key (a) references t1 (id) on
// update cascade` to fail `[planner:1142]REFERENCES command denied to user
// 'u1'@'%' for table 't1'` under create+alter-only grants, and to succeed
// after `grant references on test.t1`.
// go-parity-gap: no privilege/auth carrier.
#[test]
#[ignore = "go-parity-gap: no auth carrier; Go's 1142 REFERENCES denial is not reproducible"]
fn alter_table_add_foreign_key_requires_references_privilege() {
    // Contract (foreign_key_test.go:340-356): the denial before the grant,
    // success after it.
}

// --- TestRenameTableWithForeignKeyMetaInfo
//     (pkg/ddl/tests/fk/foreign_key_test.go:358) ---
//
// Go renames FK-bearing tables across schemas and requires the constraint
// meta to follow: `rename table test.t1 to test2.t2` rewrites the
// self-referencing constraint's RefSchema/RefTable to test2/t2 (and the
// schema diff carries zero AffectedOpts); renaming the PARENT t1→test3.tt1
// rewrites the child's RefSchema/RefTable and the SHOW CREATE text.
//
// go-parity-gap (documented divergence): the tier REFUSES any RENAME of an
// FK-participating table ("renaming a table involved in a FOREIGN KEY is
// not supported yet", `ddl/table_lifecycle.rs:134-139`) where Go accepts it
// and rewrites the constraint's stored reference — the meta rewrite Go
// asserts is not reachable. The SHOW CREATE legs additionally need Go's
// renderer, which this tier lacks.
#[test]
#[ignore = "go-parity-gap: RENAME of an FK-participating table is refused; Go rewrites the constraint's reference"]
fn rename_table_rewrites_the_constraint_reference() {
    // Contract (foreign_key_test.go:358-474): after each rename the
    // constraint's RefSchema/RefTable and the referred entries name the NEW
    // location, the schema diff has zero AffectedOpts, and show create
    // prints the new reference.
}

// --- TestTruncateOrDropTableWithForeignKeyReferred
//     (pkg/ddl/tests/fk/foreign_key_test.go:801) ---
//
// Go requires, for three parent/child shapes (including the prefix-index
// one):
//   * `truncate table t1` fails
//     `[ddl:1701]Cannot truncate a table referenced in a foreign key
//     constraint (`test`.`t2` CONSTRAINT `fk_b`)`;
//   * `drop table t1` fails
//     `[ddl:3730]Cannot drop table 't1' referenced by a foreign key
//     constraint 'fk_b' on table 't2'.`;
//   * both succeed with `@@foreign_key_checks=0`.
//
// go-parity-gap (documented divergence): the tier's truncate performs NO
// referral check at all (`ddl/table_lifecycle.rs:192`), and its drop check
// answers the child-side 1451 text ("Cannot delete or update a parent row:
// a foreign key constraint fails (...)") where Go answers 3730 — neither
// Go errno is reproducible (the b105 receipt recorded the same divergence
// for the pkg/ddl-level sibling test).
#[test]
#[ignore = "go-parity-gap: truncate has no referral check; drop renders 1451 text where Go uses 3730"]
fn truncate_or_drop_of_a_referenced_table_reports_go_errnos() {
    // Contract (foreign_key_test.go:801-887): 1701 per truncate, 3730 per
    // drop, both cleared by foreign_key_checks=0.
}

// --- TestDropIndexNeededInForeignKey
//     (pkg/ddl/tests/fk/foreign_key_test.go:889) ---
//
// Go requires, for two covering-index shapes:
//   * `alter table t1 drop index idx` (the constraint's last cover on the
//     PARENT) fails `[ddl:1553]Cannot drop index 'idx': needed in a foreign
//     key constraint`;
//   * `alter table t2 drop index idx` (the CHILD's cover) fails the same;
//   * BOTH refusals hold even with `@@foreign_key_checks=0` — the switch
//     does not unlock index drops;
//   * the pass shapes drop freely once another index covers the column.
//
// The serialized port asserts the same refusals under both switch states —
// the tier's `check_index_needed` runs unconditionally
// (`ddl/indexes.rs:436-438`), matching Go's switch-blindness.
#[test]
fn drop_index_needed_by_a_foreign_key_is_refused_regardless_of_checks() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();

    // Go case 1: single-column covers on both sides.
    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index idx (b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int, index idx (b), foreign key fk_b(b) references t1(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    for drop in ["alter table t1 drop index idx", "alter table t2 drop index idx"] {
        for checks in [false, true] {
            let statement_ctx = StmtContext::for_query().with_foreign_key_checks(checks);
            let error = ddl::run_alter_table_in(drop, &mut catalog, "test", &statement_ctx)
                .expect_err("Go: [ddl:1553]Cannot drop index 'idx': needed in a foreign key constraint");
            let mysql = error.clone().to_mysql_error();
            assert_eq!(mysql.code, 1553, "{drop} with checks={checks}");
            assert_eq!(
                mysql.message,
                "Cannot drop index 'idx': needed in a foreign key constraint",
                "{drop} with checks={checks}"
            );
        }
    }

    // Go case 2: composite covers (t1's idx (id, b), t2's idx (b, a)) —
    // still the constraint's last cover on each side.
    ddl::run_drop_table_in(
        "drop table if exists t2, t1",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t1 (id int, b int, index idx (id, b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int, index idx (b, a), foreign key fk_b(b) references t1(id))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    for drop in ["alter table t1 drop index idx", "alter table t2 drop index idx"] {
        let error = ddl::run_alter_table_in(drop, &mut catalog, "test", &ctx)
            .expect_err("Go: [ddl:1553] on the composite cover");
        assert_eq!(error.clone().to_mysql_error().code, 1553, "{drop}");
    }

    // Go's pass shape 1: the parent's cover is replacable by its primary
    // key, and the child's non-needed index drops freely. The FOURTH drop of
    // Go's shape — `alter table t2 drop index idxb`, the constraint's own
    // cover whose column IS the child's clustered primary handle — is covered
    // by the focused regression above.
    ddl::run_drop_table_in(
        "drop table if exists t2, t1",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index idxb (b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int key, index idxa (a), index idxb (b), \
         foreign key fk_b(b) references t1(id))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    for drop in ["alter table t1 drop index idxb", "alter table t2 drop index idxa"] {
        ddl::run_alter_table_in(drop, &mut catalog, "test", &ctx)
            .unwrap_or_else(|error| panic!("{drop} must pass: {error:?}"));
    }

    // The second pass shape: re-adding a cover re-opens the drops.
    ddl::run_drop_table_in(
        "drop table if exists t2, t1",
        &mut catalog,
        "test",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t1 (id int key, b int, index idxb (b), unique index idx(b, id))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table t2 (a int, b int key, index idx (b, a), index idxb (b), index idxab(a, b), \
         foreign key fk_b(b) references t1(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    for alter in [
        "alter table t1 drop index idxb",
        "alter table t1 add index idxb (b)",
        "alter table t1 drop index idx",
        "alter table t2 drop index idx",
        "alter table t2 add index idx (b, a)",
        "alter table t2 drop index idxb",
        "alter table t2 drop index idxab",
    ] {
        ddl::run_alter_table_in(alter, &mut catalog, "test", &ctx)
            .unwrap_or_else(|error| panic!("{alter} must pass: {error:?}"));
    }

    // Go admin-checks each shape; the tier's checker over the survivors.
    let Some(TableEntry::Kv(table)) = catalog.table_mut_in("test", "t2") else {
        panic!("expected a storage-backed table");
    };
    admin_check::check_table(table, None, &RowDecodeContext::for_query(&ctx)).unwrap();
    run_delete_on("delete from t2", &mut catalog, &ctx).unwrap();
}
